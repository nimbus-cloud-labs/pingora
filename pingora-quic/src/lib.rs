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
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

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
        }
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
        }
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

/// A transport-oriented representation of a QUIC upstream destination.
#[derive(Debug, Clone, PartialEq, Eq)]
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
}

impl From<&QuicConnectorConfig> for QuicUpstreamDestination {
    fn from(config: &QuicConnectorConfig) -> Self {
        Self {
            name: config.name.clone(),
            peer_addr: config.peer_addr.clone(),
            local_bind_addr: config.local_bind_addr.clone(),
            server_name: config.server_name.clone(),
            alpn_protocols: config.alpn_protocols.clone(),
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
}

/// Event emitted by the downstream listener for a received QUIC datagram.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuicSessionEvent {
    /// A new downstream session was accepted.
    Accepted,
    /// A previously known downstream session received another datagram.
    Reused,
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
}

#[derive(Debug, Default)]
struct QuicListenerStatsInner {
    accepted_sessions: AtomicU64,
    reused_sessions: AtomicU64,
    expired_sessions: AtomicU64,
    dropped_datagrams: AtomicU64,
    received_datagrams: AtomicU64,
}

impl QuicListenerStatsInner {
    fn snapshot(&self) -> QuicListenerStats {
        QuicListenerStats {
            accepted_sessions: self.accepted_sessions.load(Ordering::Relaxed),
            reused_sessions: self.reused_sessions.load(Ordering::Relaxed),
            expired_sessions: self.expired_sessions.load(Ordering::Relaxed),
            dropped_datagrams: self.dropped_datagrams.load(Ordering::Relaxed),
            received_datagrams: self.received_datagrams.load(Ordering::Relaxed),
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
    stats: QuicConnectorStatsInner,
}

impl QuicConnectorHandle {
    /// Create a connector handle from validated configuration.
    pub fn new(config: QuicConnectorConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            stats: QuicConnectorStatsInner::default(),
        })
    }

    /// Borrow the configuration used to create the handle.
    pub fn config(&self) -> &QuicConnectorConfig {
        &self.config
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
        })
    }
}

/// A transport-level QUIC upstream session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuicUpstreamSession {
    /// The upstream destination used to create the session.
    pub destination: QuicUpstreamDestination,
    /// Transport metadata for the established upstream QUIC session.
    pub meta: QuicConnectionMeta,
    /// Time when the upstream session was established.
    pub established_at: Instant,
    /// Timeout budget that governed the handshake.
    pub connect_timeout: Duration,
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
        QuicConnectorConfig, QuicConnectorHandle, QuicDownstreamListener, QuicListenerConfig,
        QuicTransport, Result,
    };
    use async_trait::async_trait;

    /// Feature-gated QUIC transport adapter backed by `tokio-quiche`.
    #[derive(Debug)]
    pub struct TokioQuicheTransport {
        params: tokio_quiche::ConnectionParams<'static>,
    }

    impl TokioQuicheTransport {
        /// Create a new adapter with default `tokio-quiche` connection parameters.
        pub fn new() -> Self {
            Self {
                params: tokio_quiche::ConnectionParams::default(),
            }
        }

        /// Return the backend label for observability and debugging.
        pub fn backend_name(&self) -> &'static str {
            let _ = &self.params;
            "tokio-quiche"
        }
    }

    impl Default for TokioQuicheTransport {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
    impl QuicTransport for TokioQuicheTransport {
        type Listener = QuicDownstreamListener;
        type Connector = QuicConnectorHandle;

        async fn bind_listener(&self, config: QuicListenerConfig) -> Result<Self::Listener> {
            let _ = &self.params;
            QuicDownstreamListener::bind(config).await
        }

        async fn connect(&self, config: QuicConnectorConfig) -> Result<Self::Connector> {
            let _ = &self.params;
            QuicConnectorHandle::new(config)
        }
    }
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

#[cfg(test)]
mod tests {
    use super::{
        NoopQuicTransport, QuicConnectionMeta, QuicConnectorConfig, QuicDownstreamListener,
        QuicListenerConfig, QuicSessionEvent, QuicTransport,
    };
    use pingora_core::protocols::l4::datagram::{Datagram, DatagramMeta, UdpListener};
    use pingora_core::protocols::l4::socket::SocketAddr;
    use pingora_error::ErrorType;
    use std::time::Duration;

    fn downstream_listener_config(listen_addr: SocketAddr) -> QuicListenerConfig {
        let mut config = QuicListenerConfig::new("h3-listener", listen_addr);
        config.alpn_protocols.push(b"h3".to_vec());
        config
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
        let listener = downstream_listener_config(SocketAddr::Inet("127.0.0.1:0".parse().unwrap()));

        let handle = transport.bind_listener(listener).await.unwrap();
        assert_eq!(handle.config().name.as_ref(), "h3-listener");
    }
}
