// Copyright 2026 Cloudflare, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![warn(clippy::all)]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! QUIC transport abstractions and integration points for Pingora.
//!
//! This crate intentionally owns the QUIC-facing boundary without leaking QUIC
//! implementation details into the generic UDP transport layer. Concrete
//! transport integration is added behind feature flags in later milestones.

use async_trait::async_trait;
use log::{debug, info, warn};
use parking_lot::Mutex;
use pingora_core::protocols::l4::datagram::{Datagram, DatagramFlowKey, UdpListener};
use pingora_core::protocols::l4::socket::SocketAddr;
use pingora_error::{Error, ErrorType, Result};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::{mpsc, Mutex as AsyncMutex};
#[cfg(feature = "tokio-quiche")]
use tokio_quiche::http3::driver::{ClientH3Controller, ClientH3Event, NewClientRequest};

/// Certificate formats supported by QUIC transport backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QuicCertificateKind {
    /// Standard X.509 certificate and private key pair.
    X509,
    /// Raw public key certificate material.
    RawPublicKey,
}

/// TLS credentials used by QUIC transport backends.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QuicTlsCertificate {
    /// Path to the certificate file.
    pub cert_path: String,
    /// Path to the private key file.
    pub private_key_path: String,
    /// Certificate format expected by the QUIC backend.
    pub kind: QuicCertificateKind,
}

impl QuicTlsCertificate {
    /// Create TLS credentials for a QUIC listener or connector.
    pub fn new(
        cert_path: impl Into<String>,
        private_key_path: impl Into<String>,
        kind: QuicCertificateKind,
    ) -> Self {
        Self {
            cert_path: cert_path.into(),
            private_key_path: private_key_path.into(),
            kind,
        }
    }
}

/// Listener configuration for downstream QUIC traffic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuicListenerConfig {
    /// Stable logical name for the listener.
    pub name: Arc<str>,
    /// UDP socket address used for downstream QUIC traffic.
    pub listen_addr: SocketAddr,
    /// ALPN values accepted by the listener.
    pub alpn_protocols: Vec<Vec<u8>>,
    /// Transport idle timeout advertised during handshake.
    pub max_idle_timeout: Duration,
    /// Initial concurrent bidirectional stream budget.
    pub max_concurrent_bidi_streams: u64,
    /// Maximum UDP datagram size to receive for this listener.
    pub max_datagram_size: usize,
    /// TLS credentials required by real QUIC server backends.
    pub tls_certificate: Option<QuicTlsCertificate>,
}

impl QuicListenerConfig {
    /// Create a baseline listener configuration.
    pub fn new(name: impl Into<Arc<str>>, listen_addr: SocketAddr) -> Self {
        Self {
            name: name.into(),
            listen_addr,
            alpn_protocols: Vec::new(),
            max_idle_timeout: Duration::from_secs(30),
            max_concurrent_bidi_streams: 128,
            max_datagram_size: 1350,
            tls_certificate: None,
        }
    }

    /// Override the ALPN values accepted by this listener.
    pub fn with_alpn_protocols(mut self, alpn_protocols: Vec<Vec<u8>>) -> Self {
        self.alpn_protocols = alpn_protocols;
        self
    }

    /// Override the advertised idle timeout.
    pub fn with_max_idle_timeout(mut self, max_idle_timeout: Duration) -> Self {
        self.max_idle_timeout = max_idle_timeout;
        self
    }

    /// Override the bidirectional stream limit.
    pub fn with_max_concurrent_bidi_streams(mut self, max_concurrent_bidi_streams: u64) -> Self {
        self.max_concurrent_bidi_streams = max_concurrent_bidi_streams;
        self
    }

    /// Override the datagram receive buffer size.
    pub fn with_max_datagram_size(mut self, max_datagram_size: usize) -> Self {
        self.max_datagram_size = max_datagram_size;
        self
    }

    /// Override the TLS credentials used by real QUIC server backends.
    pub fn with_tls_certificate(mut self, tls_certificate: Option<QuicTlsCertificate>) -> Self {
        self.tls_certificate = tls_certificate;
        self
    }

    /// Validate invariants for downstream QUIC listeners.
    pub fn validate(&self) -> Result<()> {
        validate_inet_addr(&self.listen_addr, "QUIC listener")?;
        validate_alpn(&self.alpn_protocols, "QUIC listener")?;
        if self.max_concurrent_bidi_streams == 0 {
            return Error::e_explain(
                ErrorType::InternalError,
                "QUIC listener requires at least one bidirectional stream",
            );
        }
        if self.max_datagram_size == 0 {
            return Error::e_explain(
                ErrorType::InternalError,
                "QUIC listener requires a non-zero datagram buffer size",
            );
        }
        Ok(())
    }
}

/// Connector configuration for upstream QUIC traffic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuicConnectorConfig {
    /// Stable logical name for the connector or service using it.
    pub name: Arc<str>,
    /// Remote UDP address of the upstream QUIC endpoint.
    pub peer_addr: SocketAddr,
    /// Optional local UDP address to bind before creating the upstream session.
    pub local_bind_addr: Option<SocketAddr>,
    /// TLS server name to present during QUIC handshake, when applicable.
    pub server_name: Option<String>,
    /// ALPN values offered by the connector.
    pub alpn_protocols: Vec<Vec<u8>>,
    /// Handshake timeout budget.
    pub connect_timeout: Duration,
    /// Maximum idle time before a pooled upstream session expires.
    pub idle_timeout: Duration,
    /// Optional client certificate used for mTLS-capable QUIC backends.
    pub client_certificate: Option<QuicTlsCertificate>,
    /// Whether the backend should verify the peer certificate.
    pub verify_peer: bool,
}

impl QuicConnectorConfig {
    /// Create a baseline upstream QUIC connector configuration.
    pub fn new(name: impl Into<Arc<str>>, peer_addr: SocketAddr) -> Self {
        Self {
            name: name.into(),
            peer_addr,
            local_bind_addr: None,
            server_name: None,
            alpn_protocols: Vec::new(),
            connect_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(30),
            client_certificate: None,
            verify_peer: false,
        }
    }

    /// Override the local UDP bind address.
    pub fn with_local_bind_addr(mut self, local_bind_addr: Option<SocketAddr>) -> Self {
        self.local_bind_addr = local_bind_addr;
        self
    }

    /// Override the TLS server name used for the QUIC handshake.
    pub fn with_server_name(mut self, server_name: Option<String>) -> Self {
        self.server_name = server_name;
        self
    }

    /// Override the ALPN values offered by this connector.
    pub fn with_alpn_protocols(mut self, alpn_protocols: Vec<Vec<u8>>) -> Self {
        self.alpn_protocols = alpn_protocols;
        self
    }

    /// Override the handshake timeout budget.
    pub fn with_connect_timeout(mut self, connect_timeout: Duration) -> Self {
        self.connect_timeout = connect_timeout;
        self
    }

    /// Override the idle timeout used by the upstream session pool.
    pub fn with_idle_timeout(mut self, idle_timeout: Duration) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    /// Override the client certificate used for mTLS-capable QUIC backends.
    pub fn with_client_certificate(
        mut self,
        client_certificate: Option<QuicTlsCertificate>,
    ) -> Self {
        self.client_certificate = client_certificate;
        self
    }

    /// Override whether the QUIC backend verifies the peer certificate.
    pub fn with_verify_peer(mut self, verify_peer: bool) -> Self {
        self.verify_peer = verify_peer;
        self
    }

    /// Validate invariants for upstream QUIC connectors.
    pub fn validate(&self) -> Result<()> {
        validate_inet_addr(&self.peer_addr, "QUIC connector")?;
        if let Some(local_bind_addr) = &self.local_bind_addr {
            validate_inet_addr(local_bind_addr, "QUIC connector local bind")?;
        }
        validate_alpn(&self.alpn_protocols, "QUIC connector")?;
        if self.connect_timeout.is_zero() {
            return Error::e_explain(
                ErrorType::ConnectTimedout,
                "QUIC connector requires a non-zero connect timeout",
            );
        }
        if self.idle_timeout.is_zero() {
            return Error::e_explain(
                ErrorType::InternalError,
                "QUIC connector requires a non-zero idle timeout",
            );
        }
        if self
            .server_name
            .as_ref()
            .is_some_and(|server_name| server_name.is_empty())
        {
            return Error::e_explain(
                ErrorType::InternalError,
                "QUIC connector server_name cannot be empty",
            );
        }
        Ok(())
    }
}

/// Metadata surfaced for an established QUIC transport session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuicConnectionMeta {
    /// Local UDP address bound for the transport session.
    pub local_addr: SocketAddr,
    /// Remote peer UDP address for the transport session.
    pub peer_addr: SocketAddr,
    /// Negotiated ALPN, when available.
    pub alpn_protocol: Option<Vec<u8>>,
    /// TLS server name or authority associated with the session, when known.
    pub server_name: Option<String>,
    /// Whether the session resumed prior transport state.
    pub resumed: bool,
}

/// Stream direction used by transport-level shutdown commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuicStreamDirection {
    /// Shutdown the read side of the QUIC stream.
    Read,
    /// Shutdown the write side of the QUIC stream.
    Write,
}

/// Transport-level QUIC stream lifecycle event, independent from HTTP semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuicStreamEvent {
    /// A stream was first observed on this QUIC session.
    Opened { stream_id: u64 },
    /// Stream payload bytes were received.
    Data {
        stream_id: u64,
        data: Vec<u8>,
        fin: bool,
    },
    /// The peer finished sending data on the stream.
    Finished { stream_id: u64 },
    /// The peer reset or stopped the stream.
    Reset { stream_id: u64, error_code: u64 },
    /// The stream is writable for locally queued data.
    Writable { stream_id: u64 },
}

#[derive(Debug)]
#[cfg_attr(not(feature = "tokio-quiche"), allow(dead_code))]
enum QuicStreamCommand {
    Send {
        stream_id: u64,
        data: Vec<u8>,
        fin: bool,
    },
    Shutdown {
        stream_id: u64,
        direction: QuicStreamDirection,
        error_code: u64,
    },
}

#[derive(Debug)]
#[cfg_attr(not(feature = "tokio-quiche"), allow(dead_code))]
struct QuicStreamController {
    cmd_tx: mpsc::UnboundedSender<QuicStreamCommand>,
    event_rx: AsyncMutex<mpsc::UnboundedReceiver<QuicStreamEvent>>,
}

/// Handle for transport-level QUIC stream operations on a live session.
#[derive(Debug, Clone)]
pub struct QuicStreamHandle {
    controller: Arc<QuicStreamController>,
}

impl QuicStreamHandle {
    #[cfg_attr(not(feature = "tokio-quiche"), allow(dead_code))]
    fn new(controller: Arc<QuicStreamController>) -> Self {
        Self { controller }
    }

    async fn recv_stream_event(&self) -> Option<QuicStreamEvent> {
        self.controller.recv_stream_event().await
    }

    fn send_stream_data(&self, stream_id: u64, data: Vec<u8>, fin: bool) -> Result<()> {
        self.controller.send_stream_data(stream_id, data, fin)
    }

    fn shutdown_stream(
        &self,
        stream_id: u64,
        direction: QuicStreamDirection,
        error_code: u64,
    ) -> Result<()> {
        self.controller
            .shutdown_stream(stream_id, direction, error_code)
    }
}

impl QuicStreamController {
    #[cfg_attr(not(feature = "tokio-quiche"), allow(dead_code))]
    fn new(
        cmd_tx: mpsc::UnboundedSender<QuicStreamCommand>,
        event_rx: mpsc::UnboundedReceiver<QuicStreamEvent>,
    ) -> Self {
        Self {
            cmd_tx,
            event_rx: AsyncMutex::new(event_rx),
        }
    }

    async fn recv_stream_event(&self) -> Option<QuicStreamEvent> {
        self.event_rx.lock().await.recv().await
    }

    fn send_stream_data(&self, stream_id: u64, data: Vec<u8>, fin: bool) -> Result<()> {
        self.cmd_tx
            .send(QuicStreamCommand::Send {
                stream_id,
                data,
                fin,
            })
            .map_err(|_| {
                Error::explain(
                    ErrorType::ConnectionClosed,
                    "QUIC session closed before stream data could be queued",
                )
            })
    }

    fn shutdown_stream(
        &self,
        stream_id: u64,
        direction: QuicStreamDirection,
        error_code: u64,
    ) -> Result<()> {
        self.cmd_tx
            .send(QuicStreamCommand::Shutdown {
                stream_id,
                direction,
                error_code,
            })
            .map_err(|_| {
                Error::explain(
                    ErrorType::ConnectionClosed,
                    "QUIC session closed before stream shutdown could be queued",
                )
            })
    }
}

/// A transport-oriented representation of a QUIC upstream destination.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QuicUpstreamDestination {
    /// Stable logical name for the service using this destination.
    pub name: Arc<str>,
    /// Remote UDP address for the QUIC upstream.
    pub peer_addr: SocketAddr,
    /// Optional local UDP bind address.
    pub local_bind_addr: Option<SocketAddr>,
    /// TLS server name for the handshake, when applicable.
    pub server_name: Option<String>,
    /// ALPN values offered to the upstream.
    pub alpn_protocols: Vec<Vec<u8>>,
    /// Maximum idle time before a pooled upstream session expires.
    pub idle_timeout: Duration,
    /// Whether the upstream backend verifies the peer certificate.
    pub verify_peer: bool,
}

impl From<&QuicConnectorConfig> for QuicUpstreamDestination {
    fn from(config: &QuicConnectorConfig) -> Self {
        Self {
            name: config.name.clone(),
            peer_addr: config.peer_addr.clone(),
            local_bind_addr: config.local_bind_addr.clone(),
            server_name: config.server_name.clone(),
            alpn_protocols: config.alpn_protocols.clone(),
            idle_timeout: config.idle_timeout,
            verify_peer: config.verify_peer,
        }
    }
}

/// Transport-level state for a tracked downstream QUIC session.
#[derive(Debug, Clone)]
pub struct QuicDownstreamSession {
    /// Listener-scoped flow key that identifies the session path.
    pub flow_key: DatagramFlowKey,
    /// Transport metadata for the QUIC session.
    pub meta: QuicConnectionMeta,
    /// Time when the session was first observed.
    pub established_at: Instant,
    /// Time when the session last received traffic.
    pub last_seen: Instant,
    /// Number of datagrams observed on this session.
    pub packets_received: u64,
    /// Stream lifecycle controller for real QUIC backends, when available.
    #[doc(hidden)]
    pub stream_handle: Option<QuicStreamHandle>,
}

impl QuicDownstreamSession {
    /// Receive the next QUIC stream event for this session, if supported.
    pub async fn recv_stream_event(&self) -> Option<QuicStreamEvent> {
        let handle = self.stream_handle.as_ref()?;
        handle.recv_stream_event().await
    }

    /// Queue data to be sent on a QUIC stream for this session.
    pub fn send_stream_data(&self, stream_id: u64, data: Vec<u8>, fin: bool) -> Result<()> {
        self.stream_handle
            .as_ref()
            .ok_or_else(|| {
                Error::explain(
                    ErrorType::InternalError,
                    "QUIC stream sending is unavailable on this session backend",
                )
            })?
            .send_stream_data(stream_id, data, fin)
    }

    /// Queue a stream shutdown for this session.
    pub fn shutdown_stream(
        &self,
        stream_id: u64,
        direction: QuicStreamDirection,
        error_code: u64,
    ) -> Result<()> {
        self.stream_handle
            .as_ref()
            .ok_or_else(|| {
                Error::explain(
                    ErrorType::InternalError,
                    "QUIC stream shutdown is unavailable on this session backend",
                )
            })?
            .shutdown_stream(stream_id, direction, error_code)
    }
}

/// Event emitted by the downstream listener for a received QUIC datagram.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuicSessionEvent {
    /// A new downstream session was accepted.
    Accepted,
    /// A previously known downstream session received another datagram.
    Reused,
}

/// QUIC transport backend used by a listener or connector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuicBackend {
    /// Pingora's placeholder UDP-backed QUIC model.
    Noop,
    /// `tokio-quiche` backed QUIC transport.
    TokioQuiche,
}

/// A datagram associated with a downstream QUIC session.
#[derive(Debug, Clone)]
pub struct QuicIncomingDatagram {
    /// The session this packet belongs to.
    pub session: QuicDownstreamSession,
    /// The received UDP datagram.
    pub datagram: Datagram,
    /// Whether the session was newly accepted or reused.
    pub event: QuicSessionEvent,
}

/// Downstream session close event surfaced by real QUIC backends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuicDownstreamCloseEvent {
    /// Listener-scoped flow key for the closed session.
    pub flow_key: DatagramFlowKey,
    /// Local UDP address of the closed session.
    pub local_addr: SocketAddr,
    /// Remote peer address of the closed session.
    pub peer_addr: SocketAddr,
    /// QUIC backend that emitted this event.
    pub backend: QuicBackend,
    /// Human-readable close reason.
    pub reason: String,
}

/// Snapshot of lifecycle counters for a downstream QUIC listener.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QuicListenerStats {
    /// Number of newly accepted downstream sessions.
    pub accepted_sessions: u64,
    /// Number of packets received for existing sessions.
    pub reused_sessions: u64,
    /// Number of expired sessions removed from tracking.
    pub expired_sessions: u64,
    /// Number of dropped datagrams.
    pub dropped_datagrams: u64,
    /// Number of datagrams consumed by the listener.
    pub received_datagrams: u64,
    /// Number of downstream handshakes that failed before session acceptance.
    pub handshake_failures: u64,
    /// Number of accepted downstream sessions later observed as closed.
    pub closed_sessions: u64,
}

#[derive(Debug, Default)]
struct QuicListenerStatsInner {
    accepted_sessions: AtomicU64,
    reused_sessions: AtomicU64,
    expired_sessions: AtomicU64,
    dropped_datagrams: AtomicU64,
    received_datagrams: AtomicU64,
    handshake_failures: AtomicU64,
    closed_sessions: AtomicU64,
}

impl QuicListenerStatsInner {
    fn snapshot(&self) -> QuicListenerStats {
        QuicListenerStats {
            accepted_sessions: self.accepted_sessions.load(Ordering::Relaxed),
            reused_sessions: self.reused_sessions.load(Ordering::Relaxed),
            expired_sessions: self.expired_sessions.load(Ordering::Relaxed),
            dropped_datagrams: self.dropped_datagrams.load(Ordering::Relaxed),
            received_datagrams: self.received_datagrams.load(Ordering::Relaxed),
            handshake_failures: self.handshake_failures.load(Ordering::Relaxed),
            closed_sessions: self.closed_sessions.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of lifecycle counters for a QUIC upstream connector.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QuicConnectorStats {
    /// Number of upstream handshake attempts started.
    pub handshake_attempts: u64,
    /// Number of upstream sessions established successfully.
    pub established_sessions: u64,
    /// Number of upstream handshakes that timed out.
    pub handshake_timeouts: u64,
    /// Number of upstream handshake failures excluding timeout.
    pub handshake_failures: u64,
}

#[derive(Debug, Default)]
struct QuicConnectorStatsInner {
    handshake_attempts: AtomicU64,
    established_sessions: AtomicU64,
    handshake_timeouts: AtomicU64,
    handshake_failures: AtomicU64,
}

impl QuicConnectorStatsInner {
    fn snapshot(&self) -> QuicConnectorStats {
        QuicConnectorStats {
            handshake_attempts: self.handshake_attempts.load(Ordering::Relaxed),
            established_sessions: self.established_sessions.load(Ordering::Relaxed),
            handshake_timeouts: self.handshake_timeouts.load(Ordering::Relaxed),
            handshake_failures: self.handshake_failures.load(Ordering::Relaxed),
        }
    }
}

/// A downstream QUIC listener backed by Pingora's generic UDP transport.
#[derive(Debug)]
pub struct QuicDownstreamListener {
    config: QuicListenerConfig,
    listener: UdpListener,
    sessions: Mutex<HashMap<DatagramFlowKey, QuicDownstreamSession>>,
    stats: QuicListenerStatsInner,
}

impl QuicDownstreamListener {
    /// Bind a UDP listener and wrap it as a QUIC downstream listener.
    pub async fn bind(config: QuicListenerConfig) -> Result<Self> {
        let addr = config.listen_addr.to_string();
        let listener = UdpListener::bind(&addr)
            .await
            .map_err(|e| Error::because(ErrorType::BindError, "binding QUIC UDP listener", e))?;
        Self::from_udp_listener(config, listener)
    }

    /// Wrap an existing generic UDP listener with QUIC session tracking.
    pub fn from_udp_listener(config: QuicListenerConfig, listener: UdpListener) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            listener,
            sessions: Mutex::new(HashMap::new()),
            stats: QuicListenerStatsInner::default(),
        })
    }

    /// Borrow the configuration used for this listener.
    pub fn config(&self) -> &QuicListenerConfig {
        &self.config
    }

    /// Return the transport backend used by this listener.
    pub fn backend(&self) -> QuicBackend {
        QuicBackend::Noop
    }

    /// Return the bound local address of the underlying UDP listener.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Return a snapshot of lifecycle counters for this listener.
    pub fn stats(&self) -> QuicListenerStats {
        self.stats.snapshot()
    }

    /// Return the number of tracked downstream sessions.
    pub fn tracked_sessions(&self) -> usize {
        self.sessions.lock().len()
    }

    /// Receive and classify the next QUIC datagram from the UDP transport.
    pub async fn accept(&self) -> Result<QuicIncomingDatagram> {
        loop {
            let datagram = self
                .listener
                .recv_datagram(self.config.max_datagram_size)
                .await
                .map_err(|e| {
                    Error::because(ErrorType::ReadError, "reading QUIC UDP datagram", e)
                })?;

            if let Some(incoming) = self.observe_datagram(datagram) {
                return Ok(incoming);
            }
        }
    }

    fn observe_datagram(&self, datagram: Datagram) -> Option<QuicIncomingDatagram> {
        if datagram.is_truncated() {
            self.stats.dropped_datagrams.fetch_add(1, Ordering::Relaxed);
            warn!(
                "dropping potentially truncated QUIC datagram for listener {} from {}",
                self.config.name, datagram.meta.peer_addr
            );
            return None;
        }

        self.stats
            .received_datagrams
            .fetch_add(1, Ordering::Relaxed);
        let flow_key = datagram.meta.flow_key(self.config.name.clone());
        let now = Instant::now();
        let mut sessions = self.sessions.lock();
        self.cleanup_expired_locked(&mut sessions, now);

        if let Some(session) = sessions.get_mut(&flow_key) {
            session.last_seen = now;
            session.packets_received += 1;
            self.stats.reused_sessions.fetch_add(1, Ordering::Relaxed);
            debug!(
                "reused downstream QUIC session {} for peer {}",
                self.config.name, datagram.meta.peer_addr
            );
            return Some(QuicIncomingDatagram {
                session: session.clone(),
                datagram,
                event: QuicSessionEvent::Reused,
            });
        }

        let session = QuicDownstreamSession {
            flow_key: flow_key.clone(),
            meta: QuicConnectionMeta {
                local_addr: datagram.meta.local_addr.clone(),
                peer_addr: datagram.meta.peer_addr.clone(),
                alpn_protocol: self.config.alpn_protocols.first().cloned(),
                server_name: None,
                resumed: false,
            },
            established_at: now,
            last_seen: now,
            packets_received: 1,
            stream_handle: None,
        };
        sessions.insert(flow_key, session.clone());
        self.stats.accepted_sessions.fetch_add(1, Ordering::Relaxed);
        info!(
            "accepted downstream QUIC session {} from {}",
            self.config.name, session.meta.peer_addr
        );

        Some(QuicIncomingDatagram {
            session,
            datagram,
            event: QuicSessionEvent::Accepted,
        })
    }

    fn cleanup_expired_locked(
        &self,
        sessions: &mut HashMap<DatagramFlowKey, QuicDownstreamSession>,
        now: Instant,
    ) {
        let before = sessions.len();
        sessions.retain(|_, session| {
            now.duration_since(session.last_seen) < self.config.max_idle_timeout
        });
        let removed = before - sessions.len();
        if removed > 0 {
            self.stats
                .expired_sessions
                .fetch_add(removed as u64, Ordering::Relaxed);
            debug!(
                "expired {} downstream QUIC sessions on listener {}",
                removed, self.config.name
            );
        }
    }
}

/// A lightweight upstream connector handle owned by the QUIC adapter.
#[derive(Debug)]
pub struct QuicConnectorHandle {
    config: QuicConnectorConfig,
    backend: QuicBackend,
    stats: QuicConnectorStatsInner,
}

impl QuicConnectorHandle {
    /// Create a connector handle from validated configuration.
    pub fn new(config: QuicConnectorConfig) -> Result<Self> {
        Self::new_with_backend(config, QuicBackend::Noop)
    }

    /// Create a connector handle from validated configuration and backend identity.
    pub fn new_with_backend(config: QuicConnectorConfig, backend: QuicBackend) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            backend,
            stats: QuicConnectorStatsInner::default(),
        })
    }

    /// Borrow the configuration used to create the handle.
    pub fn config(&self) -> &QuicConnectorConfig {
        &self.config
    }

    /// Return the transport backend used by this connector.
    pub fn backend(&self) -> QuicBackend {
        self.backend
    }

    /// Return a snapshot of connector lifecycle counters.
    pub fn stats(&self) -> QuicConnectorStats {
        self.stats.snapshot()
    }

    /// Return the transport-oriented upstream destination derived from this connector.
    pub fn destination(&self) -> QuicUpstreamDestination {
        QuicUpstreamDestination::from(&self.config)
    }

    /// Establish an upstream QUIC transport session.
    pub async fn establish(&self) -> Result<QuicUpstreamSession> {
        self.establish_with_handshake_delay(Duration::ZERO).await
    }

    async fn establish_with_handshake_delay(
        &self,
        handshake_delay: Duration,
    ) -> Result<QuicUpstreamSession> {
        #[cfg(feature = "tokio-quiche")]
        if self.backend == QuicBackend::TokioQuiche {
            return establish_tokio_quiche_upstream(&self.config, &self.stats).await;
        }

        if let Err(error) = self.config.validate() {
            self.stats
                .handshake_failures
                .fetch_add(1, Ordering::Relaxed);
            warn!(
                "invalid QUIC upstream connector {} for peer {}: {}",
                self.config.name, self.config.peer_addr, error
            );
            return Err(error);
        }
        self.stats
            .handshake_attempts
            .fetch_add(1, Ordering::Relaxed);
        debug!(
            "starting QUIC upstream handshake {} to {} with timeout {:?}",
            self.config.name, self.config.peer_addr, self.config.connect_timeout
        );
        tokio::time::timeout(self.config.connect_timeout, async {
            if !handshake_delay.is_zero() {
                tokio::time::sleep(handshake_delay).await;
            } else {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| {
            self.stats
                .handshake_timeouts
                .fetch_add(1, Ordering::Relaxed);
            warn!(
                "QUIC upstream handshake {} to {} timed out after {:?}",
                self.config.name, self.config.peer_addr, self.config.connect_timeout
            );
            Error::explain(
                ErrorType::ConnectTimedout,
                "QUIC upstream handshake timed out before session establishment",
            )
        })?;

        let destination = self.destination();
        let local_addr = destination
            .local_bind_addr
            .clone()
            .unwrap_or_else(|| default_local_addr_for(&destination.peer_addr));
        let established_at = Instant::now();
        self.stats
            .established_sessions
            .fetch_add(1, Ordering::Relaxed);
        info!(
            "established QUIC upstream session {} to {} with ALPN {:?}",
            self.config.name,
            self.config.peer_addr,
            self.config
                .alpn_protocols
                .first()
                .map(|protocol| String::from_utf8_lossy(protocol).into_owned())
        );

        Ok(QuicUpstreamSession {
            destination,
            meta: QuicConnectionMeta {
                local_addr,
                peer_addr: self.config.peer_addr.clone(),
                alpn_protocol: self.config.alpn_protocols.first().cloned(),
                server_name: self.config.server_name.clone(),
                resumed: false,
            },
            established_at,
            connect_timeout: self.config.connect_timeout,
            idle_timeout: self.config.idle_timeout,
            last_used_at: established_at,
            reuse_count: 0,
            stream_handle: None,
        })
    }
}

/// A transport-level QUIC upstream session.
#[derive(Debug, Clone)]
pub struct QuicUpstreamSession {
    /// The upstream destination used to create the session.
    pub destination: QuicUpstreamDestination,
    /// Transport metadata for the established upstream QUIC session.
    pub meta: QuicConnectionMeta,
    /// Time when the upstream session was established.
    pub established_at: Instant,
    /// Timeout budget that governed the handshake.
    pub connect_timeout: Duration,
    /// Maximum idle time before the session should be expired from a reuse pool.
    pub idle_timeout: Duration,
    /// Last time this session was checked out or released.
    pub last_used_at: Instant,
    /// Number of times this session has been reused after the initial establishment.
    pub reuse_count: u64,
    /// Stream lifecycle controller for real QUIC backends, when available.
    #[doc(hidden)]
    pub stream_handle: Option<QuicStreamHandle>,
}

impl QuicUpstreamSession {
    /// Receive the next QUIC stream event for this session, if supported.
    pub async fn recv_stream_event(&self) -> Option<QuicStreamEvent> {
        let handle = self.stream_handle.as_ref()?;
        handle.recv_stream_event().await
    }

    /// Queue data to be sent on a QUIC stream for this session.
    pub fn send_stream_data(&self, stream_id: u64, data: Vec<u8>, fin: bool) -> Result<()> {
        self.stream_handle
            .as_ref()
            .ok_or_else(|| {
                Error::explain(
                    ErrorType::InternalError,
                    "QUIC stream sending is unavailable on this session backend",
                )
            })?
            .send_stream_data(stream_id, data, fin)
    }

    /// Queue a stream shutdown for this session.
    pub fn shutdown_stream(
        &self,
        stream_id: u64,
        direction: QuicStreamDirection,
        error_code: u64,
    ) -> Result<()> {
        self.stream_handle
            .as_ref()
            .ok_or_else(|| {
                Error::explain(
                    ErrorType::InternalError,
                    "QUIC stream shutdown is unavailable on this session backend",
                )
            })?
            .shutdown_stream(stream_id, direction, error_code)
    }
}

/// An HTTP/3-capable upstream session built on top of a live QUIC session.
#[cfg(feature = "tokio-quiche")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio-quiche")))]
#[derive(Clone)]
pub struct Http3UpstreamSession {
    quic: QuicUpstreamSession,
    controller: Arc<AsyncMutex<ClientH3Controller>>,
}

#[cfg(feature = "tokio-quiche")]
impl Http3UpstreamSession {
    fn new(quic: QuicUpstreamSession, controller: ClientH3Controller) -> Self {
        Self {
            quic,
            controller: Arc::new(AsyncMutex::new(controller)),
        }
    }

    /// Return transport metadata for the underlying QUIC session.
    pub fn meta(&self) -> &QuicConnectionMeta {
        &self.quic.meta
    }

    /// Return the logical upstream destination for the session.
    pub fn destination(&self) -> &QuicUpstreamDestination {
        &self.quic.destination
    }

    /// Return how many times this session has been reused from a pool.
    pub fn reuse_count(&self) -> u64 {
        self.quic.reuse_count
    }

    /// Return whether this session was resumed from a pool checkout.
    pub fn resumed(&self) -> bool {
        self.quic.meta.resumed
    }

    fn mark_checked_out(&mut self, now: Instant) {
        self.quic.last_used_at = now;
        self.quic.reuse_count += 1;
        self.quic.meta.resumed = true;
    }

    fn mark_released(&mut self) {
        self.quic.last_used_at = Instant::now();
    }

    fn is_alive(&self, now: Instant) -> bool {
        now.duration_since(self.quic.last_used_at) < self.quic.idle_timeout
    }

    /// Send a client-side HTTP/3 request command on this session.
    pub async fn send_request(&self, request: NewClientRequest) -> Result<()> {
        self.controller
            .lock()
            .await
            .request_sender()
            .send(request)
            .map_err(|_| {
                Error::explain(
                    ErrorType::ConnectionClosed,
                    "HTTP/3 upstream controller closed before request could be sent",
                )
            })
    }

    /// Receive the next HTTP/3 client event from this session.
    pub async fn recv_event(&self) -> Option<ClientH3Event> {
        self.controller
            .lock()
            .await
            .event_receiver_mut()
            .recv()
            .await
    }
}

/// Snapshot of lifecycle counters for the QUIC upstream session pool.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QuicUpstreamPoolStats {
    /// Number of fresh sessions established for checkout.
    pub established_sessions: u64,
    /// Number of pooled sessions reused for checkout.
    pub reused_sessions: u64,
    /// Number of sessions released back to the pool.
    pub released_sessions: u64,
    /// Number of idle sessions expired from the pool.
    pub expired_sessions: u64,
}

#[derive(Debug, Default)]
struct QuicUpstreamPoolStatsInner {
    established_sessions: AtomicU64,
    reused_sessions: AtomicU64,
    released_sessions: AtomicU64,
    expired_sessions: AtomicU64,
}

impl QuicUpstreamPoolStatsInner {
    fn snapshot(&self) -> QuicUpstreamPoolStats {
        QuicUpstreamPoolStats {
            established_sessions: self.established_sessions.load(Ordering::Relaxed),
            reused_sessions: self.reused_sessions.load(Ordering::Relaxed),
            released_sessions: self.released_sessions.load(Ordering::Relaxed),
            expired_sessions: self.expired_sessions.load(Ordering::Relaxed),
        }
    }
}

/// A QUIC-specific pool for upstream sessions, kept separate from TCP connection pools.
#[derive(Debug, Default)]
pub struct QuicUpstreamPool {
    sessions: Mutex<HashMap<u64, Vec<QuicUpstreamSession>>>,
    stats: QuicUpstreamPoolStatsInner,
}

impl QuicUpstreamPool {
    /// Create an empty QUIC upstream session pool.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return a snapshot of pool lifecycle counters.
    pub fn stats(&self) -> QuicUpstreamPoolStats {
        self.stats.snapshot()
    }

    /// Return the number of pooled sessions currently retained.
    pub fn pooled_sessions(&self) -> usize {
        self.sessions.lock().values().map(std::vec::Vec::len).sum()
    }

    /// Obtain a pooled session if one is still alive, otherwise establish a new one.
    pub async fn checkout(
        &self,
        connector: &QuicConnectorHandle,
    ) -> Result<(QuicUpstreamSession, bool)> {
        let destination = connector.destination();
        let key = upstream_pool_key(&destination);
        let now = Instant::now();
        let (mut reused_session, expired) = {
            let mut sessions = self.sessions.lock();
            let mut expired = 0u64;
            let mut reused_session = None;

            if let Some(pool) = sessions.get_mut(&key) {
                pool.retain(|session| {
                    let alive = now.duration_since(session.last_used_at) < session.idle_timeout;
                    if !alive {
                        expired += 1;
                    }
                    alive
                });

                reused_session = pool.pop();
                if pool.is_empty() {
                    sessions.remove(&key);
                }
            }

            (reused_session, expired)
        };

        if expired > 0 {
            self.stats
                .expired_sessions
                .fetch_add(expired, Ordering::Relaxed);
        }

        if let Some(mut session) = reused_session.take() {
            session.last_used_at = now;
            session.reuse_count += 1;
            session.meta.resumed = true;
            self.stats.reused_sessions.fetch_add(1, Ordering::Relaxed);
            debug!(
                "reused QUIC upstream session {} to {}",
                destination.name, destination.peer_addr
            );
            return Ok((session, true));
        }

        let session = connector.establish().await?;
        self.stats
            .established_sessions
            .fetch_add(1, Ordering::Relaxed);
        Ok((session, false))
    }

    /// Return an upstream session to the QUIC pool for future reuse.
    pub fn release(&self, mut session: QuicUpstreamSession) {
        let key = upstream_pool_key(&session.destination);
        session.last_used_at = Instant::now();
        let destination = session.destination.clone();
        self.sessions.lock().entry(key).or_default().push(session);
        self.stats.released_sessions.fetch_add(1, Ordering::Relaxed);
        debug!(
            "released QUIC upstream session {} to {} back to pool",
            destination.name, destination.peer_addr
        );
    }

    /// Remove expired upstream sessions from the pool and return how many were removed.
    pub fn prune_expired(&self) -> usize {
        let now = Instant::now();
        let mut sessions = self.sessions.lock();
        let mut expired = 0usize;

        sessions.retain(|_, pool| {
            pool.retain(|session| {
                let alive = now.duration_since(session.last_used_at) < session.idle_timeout;
                if !alive {
                    expired += 1;
                }
                alive
            });
            !pool.is_empty()
        });

        if expired > 0 {
            self.stats
                .expired_sessions
                .fetch_add(expired as u64, Ordering::Relaxed);
        }
        expired
    }
}

/// A pool of reusable upstream HTTP/3 sessions backed by QUIC.
#[cfg(feature = "tokio-quiche")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio-quiche")))]
#[derive(Default)]
pub struct Http3UpstreamPool {
    sessions: Mutex<HashMap<u64, Vec<Http3UpstreamSession>>>,
    stats: QuicUpstreamPoolStatsInner,
}

#[cfg(feature = "tokio-quiche")]
impl Http3UpstreamPool {
    /// Create an empty HTTP/3 upstream session pool.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return pool lifecycle counters.
    pub fn stats(&self) -> QuicUpstreamPoolStats {
        self.stats.snapshot()
    }

    /// Return the number of pooled HTTP/3 sessions currently retained.
    pub fn pooled_sessions(&self) -> usize {
        self.sessions.lock().values().map(std::vec::Vec::len).sum()
    }

    /// Obtain a pooled HTTP/3 session if one is still alive, otherwise establish a new one.
    pub async fn checkout(
        &self,
        connector: &QuicConnectorConfig,
    ) -> Result<(Http3UpstreamSession, bool)> {
        let destination = QuicUpstreamDestination::from(connector);
        let key = upstream_pool_key(&destination);
        let now = Instant::now();
        let (mut reused_session, expired) = {
            let mut sessions = self.sessions.lock();
            let mut expired = 0u64;
            let mut reused_session = None;

            if let Some(pool) = sessions.get_mut(&key) {
                pool.retain(|session| {
                    let alive = session.is_alive(now);
                    if !alive {
                        expired += 1;
                    }
                    alive
                });

                reused_session = pool.pop();
                if pool.is_empty() {
                    sessions.remove(&key);
                }
            }

            (reused_session, expired)
        };

        if expired > 0 {
            self.stats
                .expired_sessions
                .fetch_add(expired, Ordering::Relaxed);
            observe_quic_event("h3_pool", &destination.name, "session_expired");
        }

        if let Some(mut session) = reused_session.take() {
            session.mark_checked_out(now);
            self.stats.reused_sessions.fetch_add(1, Ordering::Relaxed);
            observe_quic_event("h3_pool", &destination.name, "session_reused");
            return Ok((session, true));
        }

        #[cfg(feature = "tokio-quiche")]
        let session =
            tokio_quiche_adapter::establish_http3_upstream(connector, &self.stats).await?;

        self.stats
            .established_sessions
            .fetch_add(1, Ordering::Relaxed);
        observe_quic_event("h3_pool", &destination.name, "session_established");
        Ok((session, false))
    }

    /// Return an upstream HTTP/3 session to the pool.
    pub fn release(&self, mut session: Http3UpstreamSession) {
        let key = upstream_pool_key(session.destination());
        let destination = session.destination().clone();
        session.mark_released();
        self.sessions.lock().entry(key).or_default().push(session);
        self.stats.released_sessions.fetch_add(1, Ordering::Relaxed);
        observe_quic_event("h3_pool", &destination.name, "session_released");
    }

    /// Remove expired HTTP/3 sessions from the pool and return how many were removed.
    pub fn prune_expired(&self) -> usize {
        let now = Instant::now();
        let mut sessions = self.sessions.lock();
        let mut expired = 0usize;

        sessions.retain(|_, pool| {
            pool.retain(|session| {
                let alive = session.is_alive(now);
                if !alive {
                    expired += 1;
                    observe_quic_event("h3_pool", &session.destination().name, "session_expired");
                }
                alive
            });
            !pool.is_empty()
        });

        if expired > 0 {
            self.stats
                .expired_sessions
                .fetch_add(expired as u64, Ordering::Relaxed);
        }
        expired
    }
}

/// The transport boundary Pingora expects from a QUIC implementation.
#[async_trait]
pub trait QuicTransport: Send + Sync {
    /// Opaque downstream listener type provided by the transport backend.
    type Listener: Send + Sync;
    /// Opaque upstream connector type provided by the transport backend.
    type Connector: Send + Sync;

    /// Bind a downstream QUIC listener.
    async fn bind_listener(&self, config: QuicListenerConfig) -> Result<Self::Listener>;

    /// Create an upstream QUIC connector.
    async fn connect(&self, config: QuicConnectorConfig) -> Result<Self::Connector>;
}

/// Placeholder transport used until a real QUIC backend is wired in.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopQuicTransport;

#[async_trait]
impl QuicTransport for NoopQuicTransport {
    type Listener = QuicDownstreamListener;
    type Connector = QuicConnectorHandle;

    async fn bind_listener(&self, config: QuicListenerConfig) -> Result<Self::Listener> {
        QuicDownstreamListener::bind(config).await
    }

    async fn connect(&self, config: QuicConnectorConfig) -> Result<Self::Connector> {
        QuicConnectorHandle::new(config)
    }
}

#[cfg(feature = "tokio-quiche")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio-quiche")))]
pub mod tokio_quiche_adapter {
    use super::{
        default_local_addr_for, observe_quic_event, Http3UpstreamSession, QuicBackend,
        QuicCertificateKind, QuicConnectionMeta, QuicConnectorConfig, QuicConnectorHandle,
        QuicDownstreamCloseEvent, QuicDownstreamSession, QuicListenerConfig, QuicListenerStats,
        QuicListenerStatsInner, QuicStreamCommand, QuicStreamController, QuicStreamDirection,
        QuicStreamEvent, QuicStreamHandle, QuicTlsCertificate, QuicTransport, QuicUpstreamSession,
        Result,
    };
    use async_trait::async_trait;
    use parking_lot::Mutex;
    use pingora_core::protocols::l4::datagram::DatagramMeta;
    use pingora_core::protocols::l4::socket::SocketAddr;
    use pingora_error::{Error, ErrorType};
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::future::pending;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::net::UdpSocket;
    use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};
    use tokio::time::timeout;
    use tokio_quiche::http3::driver::ClientH3Driver;
    use tokio_quiche::http3::settings::Http3Settings;
    use tokio_quiche::metrics::DefaultMetrics;
    use tokio_quiche::quic::connect_with_config;
    use tokio_quiche::settings::{
        CertificateKind, ConnectionParams, Hooks, QuicSettings, TlsCertificatePaths,
    };
    use tokio_quiche::socket::Socket as ConnectedSocket;
    use tokio_stream::StreamExt;

    #[derive(Debug, Clone, Default)]
    struct NegotiatedMetadata {
        alpn_protocol: Option<Vec<u8>>,
        server_name: Option<String>,
    }

    struct StreamRuntime {
        event_tx: mpsc::UnboundedSender<QuicStreamEvent>,
        cmd_rx: mpsc::UnboundedReceiver<QuicStreamCommand>,
        pending_commands: VecDeque<QuicStreamCommand>,
        seen_streams: HashSet<u64>,
        writable_streams: HashSet<u64>,
        buffer: Vec<u8>,
    }

    impl StreamRuntime {
        fn new(
            cmd_rx: mpsc::UnboundedReceiver<QuicStreamCommand>,
            event_tx: mpsc::UnboundedSender<QuicStreamEvent>,
            max_datagram_size: usize,
        ) -> Self {
            Self {
                event_tx,
                cmd_rx,
                pending_commands: VecDeque::new(),
                seen_streams: HashSet::new(),
                writable_streams: HashSet::new(),
                buffer: vec![0; max_datagram_size.max(4096)],
            }
        }

        fn emit_read_events(
            &mut self,
            qconn: &mut tokio_quiche::quiche::Connection,
        ) -> tokio_quiche::QuicResult<()> {
            for stream_id in qconn.readable() {
                if self.seen_streams.insert(stream_id) {
                    let _ = self.event_tx.send(QuicStreamEvent::Opened { stream_id });
                }

                loop {
                    match qconn.stream_recv(stream_id, &mut self.buffer) {
                        Ok((read, fin)) => {
                            let _ = self.event_tx.send(QuicStreamEvent::Data {
                                stream_id,
                                data: self.buffer[..read].to_vec(),
                                fin,
                            });
                            if fin || qconn.stream_finished(stream_id) {
                                let _ = self.event_tx.send(QuicStreamEvent::Finished { stream_id });
                            }
                        }
                        Err(tokio_quiche::quiche::Error::Done) => break,
                        Err(tokio_quiche::quiche::Error::StreamReset(error_code)) => {
                            let _ = self.event_tx.send(QuicStreamEvent::Reset {
                                stream_id,
                                error_code,
                            });
                            break;
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
            }

            Ok(())
        }

        fn enqueue_writable_events(&mut self, qconn: &mut tokio_quiche::quiche::Connection) {
            for stream_id in qconn.writable() {
                if self.writable_streams.insert(stream_id) {
                    let _ = self.event_tx.send(QuicStreamEvent::Writable { stream_id });
                }
            }
        }

        fn queue_command(&mut self, command: QuicStreamCommand) {
            self.pending_commands.push_back(command);
        }

        fn drain_pending_commands(
            &mut self,
            qconn: &mut tokio_quiche::quiche::Connection,
        ) -> tokio_quiche::QuicResult<()> {
            while let Some(command) = self.pending_commands.pop_front() {
                match command {
                    QuicStreamCommand::Send {
                        stream_id,
                        data,
                        fin,
                    } => match qconn.stream_send(stream_id, &data, fin) {
                        Ok(_) => {
                            self.seen_streams.insert(stream_id);
                            self.writable_streams.remove(&stream_id);
                        }
                        Err(tokio_quiche::quiche::Error::Done) => {
                            self.pending_commands.push_front(QuicStreamCommand::Send {
                                stream_id,
                                data,
                                fin,
                            });
                            break;
                        }
                        Err(tokio_quiche::quiche::Error::StreamStopped(error_code)) => {
                            let _ = self.event_tx.send(QuicStreamEvent::Reset {
                                stream_id,
                                error_code,
                            });
                        }
                        Err(error) => return Err(error.into()),
                    },
                    QuicStreamCommand::Shutdown {
                        stream_id,
                        direction,
                        error_code,
                    } => {
                        let direction = match direction {
                            QuicStreamDirection::Read => tokio_quiche::quiche::Shutdown::Read,
                            QuicStreamDirection::Write => tokio_quiche::quiche::Shutdown::Write,
                        };
                        match qconn.stream_shutdown(stream_id, direction, error_code) {
                            Ok(()) | Err(tokio_quiche::quiche::Error::Done) => {}
                            Err(error) => return Err(error.into()),
                        }
                    }
                }
            }

            Ok(())
        }
    }

    struct TokioQuicheSessionApp {
        flow_key: pingora_core::protocols::l4::datagram::DatagramFlowKey,
        local_addr: SocketAddr,
        peer_addr: SocketAddr,
        negotiated: Arc<Mutex<NegotiatedMetadata>>,
        close_tx: mpsc::UnboundedSender<QuicDownstreamCloseEvent>,
        handshake_tx: Option<oneshot::Sender<()>>,
        runtime: StreamRuntime,
    }

    impl TokioQuicheSessionApp {
        #[allow(clippy::too_many_arguments)]
        fn new(
            flow_key: pingora_core::protocols::l4::datagram::DatagramFlowKey,
            local_addr: SocketAddr,
            peer_addr: SocketAddr,
            negotiated: Arc<Mutex<NegotiatedMetadata>>,
            close_tx: mpsc::UnboundedSender<QuicDownstreamCloseEvent>,
            handshake_tx: Option<oneshot::Sender<()>>,
            cmd_rx: mpsc::UnboundedReceiver<QuicStreamCommand>,
            event_tx: mpsc::UnboundedSender<QuicStreamEvent>,
            max_datagram_size: usize,
        ) -> Self {
            Self {
                flow_key,
                local_addr,
                peer_addr,
                negotiated,
                close_tx,
                handshake_tx,
                runtime: StreamRuntime::new(cmd_rx, event_tx, max_datagram_size),
            }
        }
    }

    impl tokio_quiche::ApplicationOverQuic for TokioQuicheSessionApp {
        fn on_conn_established(
            &mut self,
            qconn: &mut tokio_quiche::quiche::Connection,
            _handshake_info: &tokio_quiche::quic::HandshakeInfo,
        ) -> tokio_quiche::QuicResult<()> {
            let alpn =
                (!qconn.application_proto().is_empty()).then(|| qconn.application_proto().to_vec());
            let server_name = qconn.server_name().map(str::to_string);
            *self.negotiated.lock() = NegotiatedMetadata {
                alpn_protocol: alpn,
                server_name,
            };
            if let Some(handshake_tx) = self.handshake_tx.take() {
                let _ = handshake_tx.send(());
            }
            Ok(())
        }

        fn should_act(&self) -> bool {
            true
        }

        fn buffer(&mut self) -> &mut [u8] {
            &mut self.runtime.buffer
        }

        async fn wait_for_data(
            &mut self,
            _qconn: &mut tokio_quiche::quiche::Connection,
        ) -> tokio_quiche::QuicResult<()> {
            match self.runtime.cmd_rx.recv().await {
                Some(command) => {
                    self.runtime.queue_command(command);
                    Ok(())
                }
                None => {
                    pending::<()>().await;
                    Ok(())
                }
            }
        }

        fn process_reads(
            &mut self,
            qconn: &mut tokio_quiche::quiche::Connection,
        ) -> tokio_quiche::QuicResult<()> {
            self.runtime.emit_read_events(qconn)
        }

        fn process_writes(
            &mut self,
            qconn: &mut tokio_quiche::quiche::Connection,
        ) -> tokio_quiche::QuicResult<()> {
            while let Ok(command) = self.runtime.cmd_rx.try_recv() {
                self.runtime.queue_command(command);
            }
            self.runtime.drain_pending_commands(qconn)?;
            self.runtime.enqueue_writable_events(qconn);
            Ok(())
        }

        fn on_conn_close<M: tokio_quiche::metrics::Metrics>(
            &mut self,
            _qconn: &mut tokio_quiche::quiche::Connection,
            _metrics: &M,
            connection_result: &tokio_quiche::QuicResult<()>,
        ) {
            let reason = match connection_result {
                Ok(()) => "closed".to_string(),
                Err(error) => error.to_string(),
            };
            let _ = self.close_tx.send(QuicDownstreamCloseEvent {
                flow_key: self.flow_key.clone(),
                local_addr: self.local_addr.clone(),
                peer_addr: self.peer_addr.clone(),
                backend: QuicBackend::TokioQuiche,
                reason,
            });
        }
    }

    /// A downstream QUIC listener backed by `tokio-quiche`.
    pub struct TokioQuicheDownstreamListener {
        config: QuicListenerConfig,
        local_addr: SocketAddr,
        accept_stream: AsyncMutex<tokio_quiche::QuicConnectionStream<DefaultMetrics>>,
        sessions: Mutex<
            HashMap<pingora_core::protocols::l4::datagram::DatagramFlowKey, QuicDownstreamSession>,
        >,
        stats: QuicListenerStatsInner,
        close_rx: AsyncMutex<mpsc::UnboundedReceiver<QuicDownstreamCloseEvent>>,
        close_tx: mpsc::UnboundedSender<QuicDownstreamCloseEvent>,
    }

    impl TokioQuicheDownstreamListener {
        fn new(
            config: QuicListenerConfig,
            local_addr: SocketAddr,
            accept_stream: tokio_quiche::QuicConnectionStream<DefaultMetrics>,
        ) -> Self {
            let (close_tx, close_rx) = mpsc::unbounded_channel();
            Self {
                config,
                local_addr,
                accept_stream: AsyncMutex::new(accept_stream),
                sessions: Mutex::new(HashMap::new()),
                stats: QuicListenerStatsInner::default(),
                close_rx: AsyncMutex::new(close_rx),
                close_tx,
            }
        }

        /// Borrow the configuration used for this listener.
        pub fn config(&self) -> &QuicListenerConfig {
            &self.config
        }

        /// Return the transport backend used by this listener.
        pub fn backend(&self) -> QuicBackend {
            QuicBackend::TokioQuiche
        }

        /// Return the bound local address for this listener.
        pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
            Ok(self.local_addr.clone())
        }

        /// Return a snapshot of listener lifecycle counters.
        pub fn stats(&self) -> QuicListenerStats {
            self.stats.snapshot()
        }

        /// Return the number of currently tracked downstream sessions.
        pub fn tracked_sessions(&self) -> usize {
            self.sessions.lock().len()
        }

        /// Accept and complete a downstream QUIC handshake.
        pub async fn accept_session(&self) -> Result<QuicDownstreamSession> {
            let mut accept_stream = self.accept_stream.lock().await;
            let next = accept_stream.next().await;
            drop(accept_stream);

            let Some(initial_result) = next else {
                return Error::e_explain(
                    ErrorType::ConnectionClosed,
                    "tokio-quiche listener closed before accepting a session",
                );
            };

            let initial = match initial_result {
                Ok(initial) => initial,
                Err(error) => {
                    self.stats
                        .handshake_failures
                        .fetch_add(1, Ordering::Relaxed);
                    observe_quic_event("listener", &self.config.name, "handshake_failed");
                    return Err(Error::because(
                        ErrorType::ReadError,
                        "accepting tokio-quiche initial",
                        error,
                    ));
                }
            };

            let local_addr = SocketAddr::Inet(initial.local_addr());
            let peer_addr = SocketAddr::Inet(initial.peer_addr());
            let flow_key = DatagramMeta {
                local_addr: local_addr.clone(),
                peer_addr: peer_addr.clone(),
            }
            .flow_key(self.config.name.clone());
            let negotiated = Arc::new(Mutex::new(NegotiatedMetadata::default()));
            let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
            let (event_tx, event_rx) = mpsc::unbounded_channel();
            let (handshake_tx, handshake_rx) = oneshot::channel();
            let stream_handle =
                QuicStreamHandle::new(Arc::new(QuicStreamController::new(cmd_tx, event_rx)));
            let app = TokioQuicheSessionApp::new(
                flow_key.clone(),
                local_addr.clone(),
                peer_addr.clone(),
                Arc::clone(&negotiated),
                self.close_tx.clone(),
                Some(handshake_tx),
                cmd_rx,
                event_tx,
                self.config.max_datagram_size,
            );

            let handshake_started = Instant::now();
            let (connection, worker) = match initial.handshake(app).await {
                Ok(result) => result,
                Err(error) => {
                    self.stats
                        .handshake_failures
                        .fetch_add(1, Ordering::Relaxed);
                    observe_quic_event("listener", &self.config.name, "handshake_failed");
                    return Err(Error::because(
                        ErrorType::TLSHandshakeFailure,
                        "completing tokio-quiche downstream handshake",
                        error,
                    ));
                }
            };

            tokio_quiche::InitialQuicConnection::resume(worker);
            let _ = handshake_rx.await;

            let negotiated = negotiated.lock().clone();
            let now = Instant::now();
            let session = QuicDownstreamSession {
                flow_key: flow_key.clone(),
                meta: QuicConnectionMeta {
                    local_addr,
                    peer_addr,
                    alpn_protocol: negotiated.alpn_protocol,
                    server_name: negotiated.server_name,
                    resumed: false,
                },
                established_at: handshake_started,
                last_seen: now,
                packets_received: 0,
                stream_handle: Some(stream_handle),
            };

            self.sessions.lock().insert(flow_key, session.clone());
            self.stats.accepted_sessions.fetch_add(1, Ordering::Relaxed);
            observe_quic_event("listener", &self.config.name, "session_accepted");
            log::info!(
                "accepted tokio-quiche downstream session {} from {} to {}",
                self.config.name,
                connection.peer_addr(),
                connection.local_addr()
            );
            Ok(session)
        }

        /// Wait for the next close event emitted by the QUIC backend.
        pub async fn recv_close_event(&self) -> Option<QuicDownstreamCloseEvent> {
            let mut close_rx = self.close_rx.lock().await;
            let event = close_rx.recv().await?;
            drop(close_rx);

            if self.sessions.lock().remove(&event.flow_key).is_some() {
                self.stats.closed_sessions.fetch_add(1, Ordering::Relaxed);
                observe_quic_event("listener", &self.config.name, "session_closed");
            }

            Some(event)
        }
    }

    /// Feature-gated QUIC transport adapter backed by `tokio-quiche`.
    #[derive(Debug, Default, Clone, Copy)]
    pub struct TokioQuicheTransport;

    impl TokioQuicheTransport {
        /// Create a new adapter backed by `tokio-quiche`.
        pub fn new() -> Self {
            Self
        }

        /// Return the backend label for observability and debugging.
        pub fn backend_name(&self) -> &'static str {
            "tokio-quiche"
        }

        fn build_listener_params<'a>(
            &self,
            config: &'a QuicListenerConfig,
        ) -> Result<ConnectionParams<'a>> {
            config.validate()?;
            validate_tokio_quiche_tls(config.tls_certificate.as_ref(), "QUIC listener")?;
            let tls = config
                .tls_certificate
                .as_ref()
                .expect("validated tokio-quiche listener TLS");
            let mut settings = QuicSettings::default();
            settings.alpn = config.alpn_protocols.clone();
            settings.max_idle_timeout = Some(config.max_idle_timeout);
            settings.initial_max_streams_bidi = config.max_concurrent_bidi_streams;
            settings.max_recv_udp_payload_size = config.max_datagram_size;
            settings.max_send_udp_payload_size = config.max_datagram_size;
            settings.disable_client_ip_validation = true;
            Ok(ConnectionParams::new_server(
                settings,
                tls_certificate_paths(tls),
                Hooks::default(),
            ))
        }

        fn build_connector_params<'a>(
            &self,
            config: &'a QuicConnectorConfig,
        ) -> Result<ConnectionParams<'a>> {
            config.validate()?;
            let mut settings = QuicSettings::default();
            settings.alpn = config.alpn_protocols.clone();
            settings.max_idle_timeout = Some(config.idle_timeout);
            settings.verify_peer = config.verify_peer;
            let tls = config
                .client_certificate
                .as_ref()
                .map(tls_certificate_paths);
            Ok(ConnectionParams::new_client(
                settings,
                tls,
                Hooks::default(),
            ))
        }
    }

    pub(crate) async fn establish_upstream(
        config: &QuicConnectorConfig,
        stats: &super::QuicConnectorStatsInner,
    ) -> Result<QuicUpstreamSession> {
        let transport = TokioQuicheTransport::new();
        let params = transport.build_connector_params(config)?;
        let bind_addr = config
            .local_bind_addr
            .clone()
            .unwrap_or_else(|| default_local_addr_for(&config.peer_addr));
        let local_addr = *bind_addr
            .as_inet()
            .expect("validated inet socket address for tokio-quiche bind");
        let peer_addr = *config
            .peer_addr
            .as_inet()
            .expect("validated inet socket address for tokio-quiche peer");
        let socket = UdpSocket::bind(local_addr).await.map_err(|error| {
            Error::because(
                ErrorType::ConnectError,
                "binding tokio-quiche upstream UDP socket",
                error,
            )
        })?;
        socket.connect(peer_addr).await.map_err(|error| {
            Error::because(
                ErrorType::ConnectError,
                "connecting tokio-quiche upstream UDP socket",
                error,
            )
        })?;
        let socket = ConnectedSocket::try_from(socket).map_err(|error| {
            Error::because(
                ErrorType::ConnectError,
                "wrapping tokio-quiche upstream socket",
                error,
            )
        })?;

        let negotiated = Arc::new(Mutex::new(NegotiatedMetadata::default()));
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (close_tx, _close_rx) = mpsc::unbounded_channel();
        let (handshake_tx, handshake_rx) = oneshot::channel();
        let stream_handle =
            QuicStreamHandle::new(Arc::new(QuicStreamController::new(cmd_tx, event_rx)));
        let app = TokioQuicheSessionApp::new(
            DatagramMeta {
                local_addr: bind_addr.clone(),
                peer_addr: config.peer_addr.clone(),
            }
            .flow_key(config.name.clone()),
            bind_addr.clone(),
            config.peer_addr.clone(),
            Arc::clone(&negotiated),
            close_tx,
            Some(handshake_tx),
            cmd_rx,
            event_tx,
            1350,
        );

        stats.handshake_attempts.fetch_add(1, Ordering::Relaxed);
        observe_quic_event("connector", &config.name, "handshake_started");
        let connection = timeout(
            config.connect_timeout,
            connect_with_config(socket, config.server_name.as_deref(), &params, app),
        )
        .await
        .map_err(|_| {
            stats.handshake_timeouts.fetch_add(1, Ordering::Relaxed);
            observe_quic_event("connector", &config.name, "handshake_timeout");
            Error::explain(
                ErrorType::ConnectTimedout,
                "tokio-quiche upstream handshake timed out before session establishment",
            )
        })?
        .map_err(|error| {
            stats.handshake_failures.fetch_add(1, Ordering::Relaxed);
            observe_quic_event("connector", &config.name, "handshake_failed");
            Error::because(
                ErrorType::TLSHandshakeFailure,
                "establishing tokio-quiche upstream session",
                error,
            )
        })?;

        let _ = handshake_rx.await;
        let negotiated = negotiated.lock().clone();
        let established_at = Instant::now();
        stats.established_sessions.fetch_add(1, Ordering::Relaxed);
        observe_quic_event("connector", &config.name, "handshake_established");

        Ok(QuicUpstreamSession {
            destination: super::QuicUpstreamDestination::from(config),
            meta: QuicConnectionMeta {
                local_addr: SocketAddr::Inet(connection.local_addr()),
                peer_addr: SocketAddr::Inet(connection.peer_addr()),
                alpn_protocol: negotiated.alpn_protocol,
                server_name: negotiated
                    .server_name
                    .or_else(|| config.server_name.clone()),
                resumed: false,
            },
            established_at,
            connect_timeout: config.connect_timeout,
            idle_timeout: config.idle_timeout,
            last_used_at: established_at,
            reuse_count: 0,
            stream_handle: Some(stream_handle),
        })
    }

    pub(crate) async fn establish_http3_upstream(
        config: &QuicConnectorConfig,
        _stats: &super::QuicUpstreamPoolStatsInner,
    ) -> Result<Http3UpstreamSession> {
        let transport = TokioQuicheTransport::new();
        let params = transport.build_connector_params(config)?;
        let bind_addr = config
            .local_bind_addr
            .clone()
            .unwrap_or_else(|| default_local_addr_for(&config.peer_addr));
        let local_addr = *bind_addr
            .as_inet()
            .expect("validated inet socket address for tokio-quiche bind");
        let peer_addr = *config
            .peer_addr
            .as_inet()
            .expect("validated inet socket address for tokio-quiche peer");
        let socket = UdpSocket::bind(local_addr).await.map_err(|error| {
            Error::because(
                ErrorType::ConnectError,
                "binding tokio-quiche HTTP/3 upstream UDP socket",
                error,
            )
        })?;
        socket.connect(peer_addr).await.map_err(|error| {
            Error::because(
                ErrorType::ConnectError,
                "connecting tokio-quiche HTTP/3 upstream UDP socket",
                error,
            )
        })?;
        let socket = ConnectedSocket::try_from(socket).map_err(|error| {
            Error::because(
                ErrorType::ConnectError,
                "wrapping tokio-quiche HTTP/3 upstream socket",
                error,
            )
        })?;

        let (driver, controller) = ClientH3Driver::new(Http3Settings::default());
        observe_quic_event("h3_pool", &config.name, "session_establish_started");
        let connection = timeout(
            config.connect_timeout,
            connect_with_config(socket, config.server_name.as_deref(), &params, driver),
        )
        .await
        .map_err(|_| {
            observe_quic_event("h3_pool", &config.name, "session_establish_timeout");
            Error::explain(
                ErrorType::ConnectTimedout,
                "tokio-quiche HTTP/3 upstream handshake timed out before session establishment",
            )
        })?
        .map_err(|error| {
            observe_quic_event("h3_pool", &config.name, "session_establish_failed");
            Error::because(
                ErrorType::TLSHandshakeFailure,
                "establishing tokio-quiche HTTP/3 upstream session",
                error,
            )
        })?;

        let established_at = Instant::now();
        let session = QuicUpstreamSession {
            destination: super::QuicUpstreamDestination::from(config),
            meta: QuicConnectionMeta {
                local_addr: SocketAddr::Inet(connection.local_addr()),
                peer_addr: SocketAddr::Inet(connection.peer_addr()),
                alpn_protocol: config.alpn_protocols.first().cloned(),
                server_name: config.server_name.clone(),
                resumed: false,
            },
            established_at,
            connect_timeout: config.connect_timeout,
            idle_timeout: config.idle_timeout,
            last_used_at: established_at,
            reuse_count: 0,
            stream_handle: None,
        };
        observe_quic_event("h3_pool", &config.name, "session_established");
        Ok(Http3UpstreamSession::new(session, controller))
    }

    #[async_trait]
    impl QuicTransport for TokioQuicheTransport {
        type Listener = TokioQuicheDownstreamListener;
        type Connector = QuicConnectorHandle;

        async fn bind_listener(&self, config: QuicListenerConfig) -> Result<Self::Listener> {
            let params = self.build_listener_params(&config)?;
            let socket = UdpSocket::bind(config.listen_addr.to_string())
                .await
                .map_err(|error| {
                    Error::because(
                        ErrorType::BindError,
                        "binding tokio-quiche downstream UDP listener",
                        error,
                    )
                })?;
            let local_addr = SocketAddr::Inet(socket.local_addr().map_err(|error| {
                Error::because(
                    ErrorType::BindError,
                    "reading tokio-quiche listener local address",
                    error,
                )
            })?);
            let mut listeners =
                tokio_quiche::listen([socket], params, DefaultMetrics).map_err(|error| {
                    Error::because(
                        ErrorType::BindError,
                        "starting tokio-quiche listener",
                        error,
                    )
                })?;
            let accept_stream = listeners.remove(0);
            Ok(TokioQuicheDownstreamListener::new(
                config,
                local_addr,
                accept_stream,
            ))
        }

        async fn connect(&self, config: QuicConnectorConfig) -> Result<Self::Connector> {
            let _ = self.build_connector_params(&config)?;
            QuicConnectorHandle::new_with_backend(config, QuicBackend::TokioQuiche)
        }
    }

    fn tls_certificate_paths(tls: &QuicTlsCertificate) -> TlsCertificatePaths<'_> {
        TlsCertificatePaths {
            cert: &tls.cert_path,
            private_key: &tls.private_key_path,
            kind: match tls.kind {
                QuicCertificateKind::X509 => CertificateKind::X509,
                QuicCertificateKind::RawPublicKey => CertificateKind::RawPublicKey,
            },
        }
    }

    fn validate_tokio_quiche_tls(tls: Option<&QuicTlsCertificate>, role: &str) -> Result<()> {
        let Some(tls) = tls else {
            return Error::e_explain(
                ErrorType::InternalError,
                format!("{role} requires TLS credentials for tokio-quiche"),
            );
        };

        if tls.cert_path.is_empty() || tls.private_key_path.is_empty() {
            return Error::e_explain(
                ErrorType::InternalError,
                format!("{role} requires non-empty TLS certificate paths"),
            );
        }

        Ok(())
    }
}

#[cfg(feature = "tokio-quiche")]
async fn establish_tokio_quiche_upstream(
    config: &QuicConnectorConfig,
    stats: &QuicConnectorStatsInner,
) -> Result<QuicUpstreamSession> {
    tokio_quiche_adapter::establish_upstream(config, stats).await
}

fn validate_inet_addr(addr: &SocketAddr, role: &str) -> Result<()> {
    if addr.as_inet().is_none() {
        return Error::e_explain(
            ErrorType::InternalError,
            format!("{role} requires an inet UDP socket address"),
        );
    }
    Ok(())
}

fn validate_alpn(protocols: &[Vec<u8>], role: &str) -> Result<()> {
    if protocols.is_empty() {
        return Error::e_explain(
            ErrorType::InternalError,
            format!("{role} requires at least one ALPN protocol"),
        );
    }

    if protocols.iter().any(|protocol| protocol.is_empty()) {
        return Error::e_explain(
            ErrorType::InternalError,
            format!("{role} contains an empty ALPN protocol"),
        );
    }

    Ok(())
}

fn default_local_addr_for(peer_addr: &SocketAddr) -> SocketAddr {
    match peer_addr.as_inet().expect("validated inet socket address") {
        std::net::SocketAddr::V4(_) => SocketAddr::Inet("0.0.0.0:0".parse().unwrap()),
        std::net::SocketAddr::V6(_) => SocketAddr::Inet("[::]:0".parse().unwrap()),
    }
}

fn upstream_pool_key(destination: &QuicUpstreamDestination) -> u64 {
    let mut hasher = DefaultHasher::new();
    destination.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::{
        NoopQuicTransport, QuicBackend, QuicConnectionMeta, QuicConnectorConfig,
        QuicDownstreamListener, QuicListenerConfig, QuicSessionEvent, QuicTransport,
    };
    #[cfg(feature = "tokio-quiche")]
    use super::{QuicCertificateKind, QuicTlsCertificate};
    use pingora_core::protocols::l4::datagram::{Datagram, DatagramMeta, UdpListener};
    use pingora_core::protocols::l4::socket::SocketAddr;
    use pingora_error::ErrorType;
    #[cfg(feature = "tokio-quiche")]
    use std::sync::Arc;
    use std::time::Duration;

    fn downstream_listener_config(listen_addr: SocketAddr) -> QuicListenerConfig {
        let mut config = QuicListenerConfig::new("h3-listener", listen_addr);
        config.alpn_protocols.push(b"h3".to_vec());
        config
    }

    #[cfg(feature = "tokio-quiche")]
    fn downstream_listener_tls() -> QuicTlsCertificate {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../pingora-core/tests/certs");
        QuicTlsCertificate::new(
            root.join("server.crt").display().to_string(),
            root.join("server.key").display().to_string(),
            QuicCertificateKind::X509,
        )
    }

    #[test]
    fn listener_config_requires_alpn_and_inet_addr() {
        let mut config = QuicListenerConfig::new(
            "h3-listener",
            SocketAddr::Inet("127.0.0.1:4433".parse().unwrap()),
        );
        assert!(config.validate().is_err());

        config.alpn_protocols.push(b"h3".to_vec());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn connector_config_rejects_empty_server_name() {
        let mut config = QuicConnectorConfig::new(
            "origin-h3",
            SocketAddr::Inet("127.0.0.1:8443".parse().unwrap()),
        );
        config.alpn_protocols.push(b"h3".to_vec());
        config.server_name = Some(String::new());

        assert!(config.validate().is_err());
    }

    #[test]
    fn connector_config_rejects_zero_timeout() {
        let mut config = QuicConnectorConfig::new(
            "origin-h3",
            SocketAddr::Inet("127.0.0.1:8443".parse().unwrap()),
        );
        config.alpn_protocols.push(b"h3".to_vec());
        config.connect_timeout = Duration::ZERO;

        assert!(config.validate().is_err());
    }

    #[tokio::test]
    async fn noop_transport_builds_listener_and_connector_handles() {
        let transport = NoopQuicTransport;
        let listener = downstream_listener_config(SocketAddr::Inet("127.0.0.1:0".parse().unwrap()));

        let mut connector = QuicConnectorConfig::new(
            "origin-h3",
            SocketAddr::Inet("127.0.0.1:8443".parse().unwrap()),
        );
        connector.alpn_protocols.push(b"h3".to_vec());
        connector.server_name = Some("example.com".to_string());

        let listener_handle = transport.bind_listener(listener.clone()).await.unwrap();
        let connector_handle = transport.connect(connector.clone()).await.unwrap();

        assert_eq!(listener_handle.config().name, listener.name);
        assert!(listener_handle.local_addr().unwrap().as_inet().is_some());
        assert_eq!(connector_handle.config(), &connector);
        assert_eq!(
            connector_handle.destination().peer_addr,
            connector.peer_addr
        );
    }

    #[tokio::test]
    async fn connector_handle_establishes_upstream_session() {
        let mut config = QuicConnectorConfig::new(
            "origin-h3",
            SocketAddr::Inet("127.0.0.1:8443".parse().unwrap()),
        );
        config.alpn_protocols.push(b"h3".to_vec());
        config.server_name = Some("example.com".to_string());

        let connector = super::QuicConnectorHandle::new(config.clone()).unwrap();
        let session = connector.establish().await.unwrap();

        assert_eq!(session.destination.peer_addr, config.peer_addr);
        assert_eq!(session.meta.server_name.as_deref(), Some("example.com"));
        assert_eq!(session.meta.alpn_protocol.as_deref(), Some(&b"h3"[..]));
        assert_eq!(session.connect_timeout, config.connect_timeout);
        let stats = connector.stats();
        assert_eq!(stats.handshake_attempts, 1);
        assert_eq!(stats.established_sessions, 1);
        assert_eq!(stats.handshake_timeouts, 0);
    }

    #[tokio::test]
    async fn connector_handle_times_out_slow_handshakes() {
        let mut config = QuicConnectorConfig::new(
            "origin-h3",
            SocketAddr::Inet("127.0.0.1:8443".parse().unwrap()),
        );
        config.alpn_protocols.push(b"h3".to_vec());
        config.connect_timeout = Duration::from_millis(10);

        let connector = super::QuicConnectorHandle::new(config).unwrap();
        let error = connector
            .establish_with_handshake_delay(Duration::from_millis(20))
            .await
            .unwrap_err();

        assert_eq!(error.etype, ErrorType::ConnectTimedout);
        let stats = connector.stats();
        assert_eq!(stats.handshake_attempts, 1);
        assert_eq!(stats.established_sessions, 0);
        assert_eq!(stats.handshake_timeouts, 1);
    }

    #[tokio::test]
    async fn upstream_pool_reuses_released_sessions() {
        let mut config = QuicConnectorConfig::new(
            "origin-h3",
            SocketAddr::Inet("127.0.0.1:8443".parse().unwrap()),
        );
        config.alpn_protocols.push(b"h3".to_vec());
        config.server_name = Some("example.com".to_string());
        config.idle_timeout = Duration::from_secs(30);

        let connector = super::QuicConnectorHandle::new(config).unwrap();
        let pool = super::QuicUpstreamPool::new();

        let (session, reused) = pool.checkout(&connector).await.unwrap();
        assert!(!reused);
        assert_eq!(session.reuse_count, 0);
        pool.release(session);

        let (session, reused) = pool.checkout(&connector).await.unwrap();
        assert!(reused);
        assert_eq!(session.reuse_count, 1);
        assert!(session.meta.resumed);
        assert_eq!(pool.pooled_sessions(), 0);

        let stats = pool.stats();
        assert_eq!(stats.established_sessions, 1);
        assert_eq!(stats.reused_sessions, 1);
        assert_eq!(stats.released_sessions, 1);
    }

    #[tokio::test]
    async fn upstream_pool_expires_idle_sessions() {
        let mut config = QuicConnectorConfig::new(
            "origin-h3",
            SocketAddr::Inet("127.0.0.1:8443".parse().unwrap()),
        );
        config.alpn_protocols.push(b"h3".to_vec());
        config.idle_timeout = Duration::from_millis(10);

        let connector = super::QuicConnectorHandle::new(config).unwrap();
        let pool = super::QuicUpstreamPool::new();

        let (session, _) = pool.checkout(&connector).await.unwrap();
        pool.release(session);
        std::thread::sleep(Duration::from_millis(20));

        assert_eq!(pool.prune_expired(), 1);
        assert_eq!(pool.pooled_sessions(), 0);
        assert_eq!(pool.stats().expired_sessions, 1);
    }

    #[test]
    fn connector_handle_tracks_invalid_configuration_failures() {
        let mut config = QuicConnectorConfig::new(
            "origin-h3",
            SocketAddr::Inet("127.0.0.1:8443".parse().unwrap()),
        );
        config.alpn_protocols.push(b"h3".to_vec());
        config.connect_timeout = Duration::ZERO;

        let connector = super::QuicConnectorHandle {
            config,
            backend: QuicBackend::Noop,
            stats: super::QuicConnectorStatsInner::default(),
        };
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let error = runtime
            .block_on(connector.establish_with_handshake_delay(Duration::ZERO))
            .unwrap_err();

        assert_eq!(error.etype, ErrorType::ConnectTimedout);
        let stats = connector.stats();
        assert_eq!(stats.handshake_attempts, 0);
        assert_eq!(stats.handshake_failures, 1);
    }

    #[tokio::test]
    async fn downstream_listener_tracks_new_and_existing_sessions() {
        let udp = UdpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = udp.local_addr().unwrap();
        let listener =
            QuicDownstreamListener::from_udp_listener(downstream_listener_config(local_addr), udp)
                .unwrap();

        let first = listener
            .observe_datagram(Datagram::new(
                DatagramMeta {
                    local_addr: listener.local_addr().unwrap(),
                    peer_addr: SocketAddr::Inet("127.0.0.1:50000".parse().unwrap()),
                },
                b"client hello".to_vec(),
            ))
            .unwrap();
        let second = listener
            .observe_datagram(Datagram::new(
                DatagramMeta {
                    local_addr: listener.local_addr().unwrap(),
                    peer_addr: SocketAddr::Inet("127.0.0.1:50000".parse().unwrap()),
                },
                b"ack".to_vec(),
            ))
            .unwrap();

        assert_eq!(first.event, QuicSessionEvent::Accepted);
        assert_eq!(second.event, QuicSessionEvent::Reused);
        assert_eq!(second.session.packets_received, 2);
        assert_eq!(listener.tracked_sessions(), 1);

        let stats = listener.stats();
        assert_eq!(stats.accepted_sessions, 1);
        assert_eq!(stats.reused_sessions, 1);
        assert_eq!(stats.received_datagrams, 2);
    }

    #[tokio::test]
    async fn downstream_listener_tracks_dropped_truncated_datagrams() {
        let udp = UdpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = udp.local_addr().unwrap();
        let listener =
            QuicDownstreamListener::from_udp_listener(downstream_listener_config(local_addr), udp)
                .unwrap();

        assert!(listener
            .observe_datagram(Datagram::new_with_truncation(
                DatagramMeta {
                    local_addr: listener.local_addr().unwrap(),
                    peer_addr: SocketAddr::Inet("127.0.0.1:50000".parse().unwrap()),
                },
                b"bad".to_vec(),
                true,
            ))
            .is_none());

        let stats = listener.stats();
        assert_eq!(stats.dropped_datagrams, 1);
        assert_eq!(stats.received_datagrams, 0);
    }

    #[tokio::test]
    async fn downstream_listener_expires_idle_sessions() {
        let udp = UdpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = udp.local_addr().unwrap();
        let mut config = downstream_listener_config(local_addr);
        config.max_idle_timeout = Duration::from_millis(10);
        let listener = QuicDownstreamListener::from_udp_listener(config, udp).unwrap();

        let first = listener
            .observe_datagram(Datagram::new(
                DatagramMeta {
                    local_addr: listener.local_addr().unwrap(),
                    peer_addr: SocketAddr::Inet("127.0.0.1:50000".parse().unwrap()),
                },
                b"client hello".to_vec(),
            ))
            .unwrap();
        std::thread::sleep(Duration::from_millis(20));
        let second = listener
            .observe_datagram(Datagram::new(
                DatagramMeta {
                    local_addr: listener.local_addr().unwrap(),
                    peer_addr: SocketAddr::Inet("127.0.0.1:50000".parse().unwrap()),
                },
                b"client hello again".to_vec(),
            ))
            .unwrap();

        assert_eq!(first.event, QuicSessionEvent::Accepted);
        assert_eq!(second.event, QuicSessionEvent::Accepted);
        assert_eq!(listener.stats().expired_sessions, 1);
    }

    #[test]
    fn connection_meta_captures_transport_identity() {
        let meta = QuicConnectionMeta {
            local_addr: SocketAddr::Inet("127.0.0.1:4433".parse().unwrap()),
            peer_addr: SocketAddr::Inet("127.0.0.1:50000".parse().unwrap()),
            alpn_protocol: Some(b"h3".to_vec()),
            server_name: Some("example.com".to_string()),
            resumed: true,
        };

        assert_eq!(meta.server_name.as_deref(), Some("example.com"));
        assert_eq!(meta.alpn_protocol.as_deref(), Some(&b"h3"[..]));
        assert!(meta.resumed);
    }

    #[cfg(feature = "tokio-quiche")]
    #[tokio::test]
    async fn tokio_quiche_adapter_initializes() {
        use super::tokio_quiche_adapter::TokioQuicheTransport;

        let transport = TokioQuicheTransport::new();
        assert_eq!(transport.backend_name(), "tokio-quiche");
        let listener = downstream_listener_config(SocketAddr::Inet("127.0.0.1:0".parse().unwrap()))
            .with_tls_certificate(Some(downstream_listener_tls()));

        let handle = match transport.bind_listener(listener).await {
            Ok(handle) => handle,
            Err(error)
                if error.etype() == &ErrorType::BindError
                    && error.cause.as_ref().is_some_and(|cause| {
                        cause
                            .downcast_ref::<std::io::Error>()
                            .is_some_and(|io| io.kind() == std::io::ErrorKind::PermissionDenied)
                    }) =>
            {
                return;
            }
            Err(error) => panic!("failed to bind tokio-quiche listener: {error:?}"),
        };
        assert_eq!(handle.config().name.as_ref(), "h3-listener");
        assert_eq!(handle.backend(), QuicBackend::TokioQuiche);
    }

    #[cfg(feature = "tokio-quiche")]
    #[tokio::test]
    async fn tokio_quiche_transport_marks_connector_backend() {
        use super::tokio_quiche_adapter::TokioQuicheTransport;

        let transport = TokioQuicheTransport::new();
        let connector = QuicConnectorConfig::new(
            "origin-h3",
            SocketAddr::Inet("127.0.0.1:8443".parse().unwrap()),
        )
        .with_alpn_protocols(vec![b"h3".to_vec()]);

        let handle = transport.connect(connector).await.unwrap();
        assert_eq!(handle.backend(), QuicBackend::TokioQuiche);
    }

    #[cfg(feature = "tokio-quiche")]
    #[tokio::test]
    async fn tokio_quiche_listener_accepts_real_handshake() {
        use super::tokio_quiche_adapter::TokioQuicheTransport;
        use std::future::pending;
        use tokio::time::{timeout, Duration as TokioDuration};
        use tokio_quiche::quic::connect_with_config;
        use tokio_quiche::settings::{ConnectionParams, Hooks, QuicSettings};
        use tokio_quiche::socket::Socket;

        struct TestClientApp {
            buffer: Vec<u8>,
        }

        impl Default for TestClientApp {
            fn default() -> Self {
                Self {
                    buffer: vec![0; 1350],
                }
            }
        }

        impl tokio_quiche::ApplicationOverQuic for TestClientApp {
            fn on_conn_established(
                &mut self,
                _qconn: &mut tokio_quiche::quiche::Connection,
                _handshake_info: &tokio_quiche::quic::HandshakeInfo,
            ) -> tokio_quiche::QuicResult<()> {
                Ok(())
            }

            fn should_act(&self) -> bool {
                false
            }

            fn buffer(&mut self) -> &mut [u8] {
                &mut self.buffer
            }

            async fn wait_for_data(
                &mut self,
                _qconn: &mut tokio_quiche::quiche::Connection,
            ) -> tokio_quiche::QuicResult<()> {
                pending::<()>().await;
                Ok(())
            }

            fn process_reads(
                &mut self,
                _qconn: &mut tokio_quiche::quiche::Connection,
            ) -> tokio_quiche::QuicResult<()> {
                Ok(())
            }

            fn process_writes(
                &mut self,
                _qconn: &mut tokio_quiche::quiche::Connection,
            ) -> tokio_quiche::QuicResult<()> {
                Ok(())
            }
        }

        let transport = TokioQuicheTransport::new();
        let config = downstream_listener_config(SocketAddr::Inet("127.0.0.1:0".parse().unwrap()))
            .with_tls_certificate(Some(downstream_listener_tls()));
        let listener = match transport.bind_listener(config).await {
            Ok(listener) => listener,
            Err(error)
                if error.etype() == &ErrorType::BindError
                    && error.cause.as_ref().is_some_and(|cause| {
                        cause
                            .downcast_ref::<std::io::Error>()
                            .is_some_and(|io| io.kind() == std::io::ErrorKind::PermissionDenied)
                    }) =>
            {
                return;
            }
            Err(error) => panic!("failed to bind tokio-quiche listener: {error:?}"),
        };
        let server_addr = *listener.local_addr().unwrap().as_inet().unwrap();

        let client_task = tokio::spawn(async move {
            let client_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            client_socket.connect(server_addr).await.unwrap();
            let socket = Socket::try_from(client_socket).unwrap();
            let mut settings = QuicSettings::default();
            settings.alpn = vec![b"h3".to_vec()];
            settings.verify_peer = false;
            settings.max_idle_timeout = Some(Duration::from_secs(30));
            let params = ConnectionParams::new_client(settings, None, Hooks::default());
            connect_with_config(socket, Some("localhost"), &params, TestClientApp::default())
                .await
                .unwrap()
        });

        let session = timeout(TokioDuration::from_secs(2), listener.accept_session())
            .await
            .unwrap()
            .unwrap();
        let _client_conn = client_task.await.unwrap();

        assert_eq!(session.meta.alpn_protocol.as_deref(), Some(&b"h3"[..]));
        assert_eq!(listener.tracked_sessions(), 1);
        let stats = listener.stats();
        assert_eq!(stats.accepted_sessions, 1);
        assert_eq!(stats.handshake_failures, 0);
    }

    #[cfg(feature = "tokio-quiche")]
    #[tokio::test]
    async fn tokio_quiche_upstream_session_exchanges_stream_events() {
        use super::tokio_quiche_adapter::TokioQuicheTransport;
        use tokio::time::{timeout, Duration as TokioDuration};

        async fn recv_until_data(session: &super::QuicDownstreamSession) -> (u64, Vec<u8>, bool) {
            loop {
                let event = timeout(TokioDuration::from_secs(2), session.recv_stream_event())
                    .await
                    .unwrap()
                    .unwrap();
                if let super::QuicStreamEvent::Data {
                    stream_id,
                    data,
                    fin,
                } = event
                {
                    return (stream_id, data, fin);
                }
            }
        }

        async fn recv_upstream_until_data(
            session: &super::QuicUpstreamSession,
        ) -> (u64, Vec<u8>, bool) {
            loop {
                let event = timeout(TokioDuration::from_secs(2), session.recv_stream_event())
                    .await
                    .unwrap()
                    .unwrap();
                if let super::QuicStreamEvent::Data {
                    stream_id,
                    data,
                    fin,
                } = event
                {
                    return (stream_id, data, fin);
                }
            }
        }

        let transport = TokioQuicheTransport::new();
        let listener = match transport
            .bind_listener(
                downstream_listener_config(SocketAddr::Inet("127.0.0.1:0".parse().unwrap()))
                    .with_tls_certificate(Some(downstream_listener_tls())),
            )
            .await
        {
            Ok(listener) => Arc::new(listener),
            Err(error)
                if error.etype() == &ErrorType::BindError
                    && error.cause.as_ref().is_some_and(|cause| {
                        cause
                            .downcast_ref::<std::io::Error>()
                            .is_some_and(|io| io.kind() == std::io::ErrorKind::PermissionDenied)
                    }) =>
            {
                return;
            }
            Err(error) => panic!("failed to bind tokio-quiche listener: {error:?}"),
        };
        let server_addr = listener.local_addr().unwrap();
        let listener_task = {
            let listener = Arc::clone(&listener);
            tokio::spawn(async move {
                let session = listener.accept_session().await.unwrap();
                let (stream_id, data, fin) = recv_until_data(&session).await;
                assert_eq!(stream_id, 0);
                assert_eq!(data, b"ping".to_vec());
                assert!(fin);
                session
                    .send_stream_data(stream_id, b"pong".to_vec(), true)
                    .unwrap();
                session
            })
        };

        let connector = transport
            .connect(
                QuicConnectorConfig::new("origin-h3", server_addr)
                    .with_alpn_protocols(vec![b"h3".to_vec()])
                    .with_server_name(Some("localhost".to_string())),
            )
            .await
            .unwrap();
        let upstream = connector.establish().await.unwrap();
        upstream
            .send_stream_data(0, b"ping".to_vec(), true)
            .unwrap();
        let (stream_id, data, fin) = recv_upstream_until_data(&upstream).await;

        assert_eq!(stream_id, 0);
        assert_eq!(data, b"pong".to_vec());
        assert!(fin);
        let server_session = listener_task.await.unwrap();
        assert!(server_session.stream_handle.is_some());
        assert!(upstream.stream_handle.is_some());
    }

    #[cfg(feature = "tokio-quiche")]
    #[tokio::test]
    async fn tokio_quiche_upstream_pool_reuses_real_sessions() {
        use super::tokio_quiche_adapter::TokioQuicheTransport;

        let transport = TokioQuicheTransport::new();
        let listener = match transport
            .bind_listener(
                downstream_listener_config(SocketAddr::Inet("127.0.0.1:0".parse().unwrap()))
                    .with_tls_certificate(Some(downstream_listener_tls())),
            )
            .await
        {
            Ok(listener) => listener,
            Err(error)
                if error.etype() == &ErrorType::BindError
                    && error.cause.as_ref().is_some_and(|cause| {
                        cause
                            .downcast_ref::<std::io::Error>()
                            .is_some_and(|io| io.kind() == std::io::ErrorKind::PermissionDenied)
                    }) =>
            {
                return;
            }
            Err(error) => panic!("failed to bind tokio-quiche listener: {error:?}"),
        };
        let connector = transport
            .connect(
                QuicConnectorConfig::new("origin-h3", listener.local_addr().unwrap())
                    .with_alpn_protocols(vec![b"h3".to_vec()])
                    .with_server_name(Some("localhost".to_string())),
            )
            .await
            .unwrap();
        let pool = super::QuicUpstreamPool::new();

        let accept_task = tokio::spawn(async move {
            let _ = listener.accept_session().await.unwrap();
        });

        let (session, reused) = pool.checkout(&connector).await.unwrap();
        assert!(!reused);
        pool.release(session);

        let (session, reused) = pool.checkout(&connector).await.unwrap();
        assert!(reused);
        assert!(session.meta.resumed);
        assert!(session.stream_handle.is_some());
        accept_task.await.unwrap();
    }
}
