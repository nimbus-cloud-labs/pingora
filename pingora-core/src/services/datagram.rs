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

//! Datagram listening service support.

use async_trait::async_trait;
use log::{debug, error, info};
use parking_lot::Mutex;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::protocols::l4::datagram::{Datagram, UdpListener};
use crate::protocols::l4::socket::SocketAddr;
#[cfg(unix)]
use crate::server::ListenFds;
use crate::server::ShutdownWatch;
use crate::services::Service as ServiceTrait;
use crate::upstreams::peer::UdpPeer;
use crate::upstreams::udp::{
    UdpFlowInsert, UdpFlowLookup, UdpFlowTable, UdpPeerSet, UdpSelectionMode,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DatagramMetricEvent {
    Received,
    Sent,
    RecvError,
    SendError,
    IgnoredUpstream,
    FlowRemap,
    FlowExpired,
    FlowCleanup,
    FlowTableFull,
    Truncated,
    Dropped,
}

/// A point-in-time snapshot of UDP service counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DatagramServiceStatsSnapshot {
    pub received: u64,
    pub sent: u64,
    pub recv_error: u64,
    pub send_error: u64,
    pub ignored_upstream: u64,
    pub flow_remap: u64,
    pub flow_expired: u64,
    pub flow_cleanup: u64,
    pub flow_table_full: u64,
    pub truncated: u64,
    pub dropped: u64,
}

/// Service-level UDP counters stored independently from any metrics backend.
#[derive(Debug, Default)]
pub struct DatagramServiceStats {
    received: AtomicU64,
    sent: AtomicU64,
    recv_error: AtomicU64,
    send_error: AtomicU64,
    ignored_upstream: AtomicU64,
    flow_remap: AtomicU64,
    flow_expired: AtomicU64,
    flow_cleanup: AtomicU64,
    flow_table_full: AtomicU64,
    truncated: AtomicU64,
    dropped: AtomicU64,
}

impl DatagramServiceStats {
    fn record(&self, event: DatagramMetricEvent) {
        let counter = match event {
            DatagramMetricEvent::Received => &self.received,
            DatagramMetricEvent::Sent => &self.sent,
            DatagramMetricEvent::RecvError => &self.recv_error,
            DatagramMetricEvent::SendError => &self.send_error,
            DatagramMetricEvent::IgnoredUpstream => &self.ignored_upstream,
            DatagramMetricEvent::FlowRemap => &self.flow_remap,
            DatagramMetricEvent::FlowExpired => &self.flow_expired,
            DatagramMetricEvent::FlowCleanup => &self.flow_cleanup,
            DatagramMetricEvent::FlowTableFull => &self.flow_table_full,
            DatagramMetricEvent::Truncated => &self.truncated,
            DatagramMetricEvent::Dropped => &self.dropped,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Return a snapshot of the current UDP service counters.
    pub fn snapshot(&self) -> DatagramServiceStatsSnapshot {
        DatagramServiceStatsSnapshot {
            received: self.received.load(Ordering::Relaxed),
            sent: self.sent.load(Ordering::Relaxed),
            recv_error: self.recv_error.load(Ordering::Relaxed),
            send_error: self.send_error.load(Ordering::Relaxed),
            ignored_upstream: self.ignored_upstream.load(Ordering::Relaxed),
            flow_remap: self.flow_remap.load(Ordering::Relaxed),
            flow_expired: self.flow_expired.load(Ordering::Relaxed),
            flow_cleanup: self.flow_cleanup.load(Ordering::Relaxed),
            flow_table_full: self.flow_table_full.load(Ordering::Relaxed),
            truncated: self.truncated.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
        }
    }
}

/// Application logic for UDP datagram services.
#[async_trait]
pub trait DatagramApp: Send + Sync {
    /// Handle a single received datagram.
    async fn process_new(
        &self,
        datagram: Datagram,
        responder: &DatagramResponder,
        shutdown: &ShutdownWatch,
    );

    /// Cleanup hook invoked when the service stops.
    async fn cleanup(&self) {}
}

/// Helper used by UDP services to send datagrams back to peers.
#[derive(Clone, Debug)]
pub struct DatagramResponder {
    service_name: Arc<str>,
    listener: Arc<UdpListener>,
    stats: Arc<DatagramServiceStats>,
}

impl DatagramResponder {
    fn new(
        service_name: Arc<str>,
        listener: Arc<UdpListener>,
        stats: Arc<DatagramServiceStats>,
    ) -> Self {
        Self {
            service_name,
            listener,
            stats,
        }
    }

    /// Return the local address of the underlying UDP listener.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Return the service name associated with this responder.
    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    fn record_event(&self, event: DatagramMetricEvent) {
        self.stats.record(event);
    }

    /// Send a datagram to the peer encoded in the packet metadata.
    pub async fn send_datagram(&self, datagram: &Datagram) -> std::io::Result<usize> {
        self.send_to(datagram.payload(), &datagram.meta.peer_addr)
            .await
    }

    /// Send a payload to an explicit socket address.
    pub async fn send_to(&self, payload: &[u8], addr: &SocketAddr) -> std::io::Result<usize> {
        match self.listener.send_to(payload, addr).await {
            Ok(size) => {
                self.record_event(DatagramMetricEvent::Sent);
                debug!(
                    "sent UDP datagram on service {} to {} ({} bytes)",
                    self.service_name, addr, size
                );
                Ok(size)
            }
            Err(e) => {
                self.record_event(DatagramMetricEvent::SendError);
                error!(
                    "UDP send failed on service {} to {}: {}",
                    self.service_name, addr, e
                );
                Err(e)
            }
        }
    }
}

/// A UDP forwarding application backed by upstream selection and flow affinity.
#[derive(Debug)]
pub struct UdpLoadBalancer {
    peers: Arc<UdpPeerSet>,
    selection_mode: UdpSelectionMode,
    flow_table: Mutex<UdpFlowTable>,
    cleanup_interval: Option<Duration>,
    last_cleanup: Mutex<Option<std::time::Instant>>,
}

/// Service-level limits and behavior for UDP load balancing.
///
/// Forwarding remains best-effort and unbuffered; overload is handled by dropping
/// new flows once the table reaches capacity.
#[derive(Debug, Clone)]
pub struct UdpLoadBalancerOptions {
    /// Idle timeout for tracked flow affinity.
    pub idle_timeout: Duration,
    /// Maximum number of concurrent tracked flows.
    pub max_tracked_flows: usize,
    /// Periodic cleanup cadence for expired flows. `None` keeps lazy expiration only.
    pub cleanup_interval: Option<Duration>,
}

impl UdpLoadBalancerOptions {
    /// Create options with the given idle timeout and conservative defaults.
    pub fn new(idle_timeout: Duration) -> Self {
        Self {
            idle_timeout,
            max_tracked_flows: 65_536,
            cleanup_interval: None,
        }
    }

    /// Override the maximum number of tracked flows.
    pub fn with_max_tracked_flows(mut self, max_tracked_flows: usize) -> Self {
        self.max_tracked_flows = max_tracked_flows;
        self
    }

    /// Override the periodic cleanup cadence for expired flows.
    pub fn with_cleanup_interval(mut self, cleanup_interval: Option<Duration>) -> Self {
        self.cleanup_interval = cleanup_interval;
        self
    }
}

impl UdpLoadBalancer {
    /// Create a new UDP load balancer with the given peers, selection mode, and idle timeout.
    pub fn new(
        peers: Vec<UdpPeer>,
        selection_mode: UdpSelectionMode,
        idle_timeout: Duration,
    ) -> Self {
        Self::new_with_options(
            peers,
            selection_mode,
            UdpLoadBalancerOptions::new(idle_timeout),
        )
    }

    /// Create a new UDP load balancer with explicit service-level options.
    pub fn new_with_options(
        peers: Vec<UdpPeer>,
        selection_mode: UdpSelectionMode,
        options: UdpLoadBalancerOptions,
    ) -> Self {
        Self {
            peers: Arc::new(UdpPeerSet::new(peers)),
            selection_mode,
            flow_table: Mutex::new(UdpFlowTable::new(
                options.idle_timeout,
                options.max_tracked_flows,
            )),
            cleanup_interval: options.cleanup_interval,
            last_cleanup: Mutex::new(None),
        }
    }

    fn maybe_cleanup_expired(&self, responder: &DatagramResponder) {
        let Some(cleanup_interval) = self.cleanup_interval else {
            return;
        };

        let now = std::time::Instant::now();
        {
            let last_cleanup = self.last_cleanup.lock();
            if last_cleanup
                .as_ref()
                .is_some_and(|previous| now.duration_since(*previous) < cleanup_interval)
            {
                return;
            }
        }

        let removed = self.flow_table.lock().cleanup_expired();
        let mut last_cleanup = self.last_cleanup.lock();
        *last_cleanup = Some(now);
        if removed > 0 {
            responder.record_event(DatagramMetricEvent::FlowCleanup);
            debug!(
                "UDP load balancer {} cleaned up {} expired flows",
                responder.service_name(),
                removed
            );
        }
    }

    fn select_peer_for_datagram(
        &self,
        datagram: &Datagram,
        responder: &DatagramResponder,
    ) -> Option<UdpPeer> {
        self.maybe_cleanup_expired(responder);

        if self.peers.is_empty() {
            return None;
        }

        // Responses from upstream peers are recognized but not forwarded back yet.
        if self.peers.contains_addr(&datagram.meta.peer_addr) {
            responder.record_event(DatagramMetricEvent::IgnoredUpstream);
            debug!(
                "UDP load balancer {} ignored upstream datagram from {}",
                responder.service_name(),
                datagram.meta.peer_addr
            );
            return None;
        }

        let flow_key = datagram
            .meta
            .flow_key(Arc::<str>::from(responder.service_name()));
        let mut flow_table = self.flow_table.lock();

        match flow_table.lookup(&flow_key) {
            UdpFlowLookup::Active(entry) => {
                if self.peers.is_enabled(entry.peer.address()) {
                    return Some(entry.peer.clone());
                }
                let invalid_peer = entry.peer.address().clone();
                let removed = flow_table.invalidate_peer(&invalid_peer);
                responder.record_event(DatagramMetricEvent::FlowRemap);
                debug!(
                    "UDP load balancer {} remapping flow from disabled peer {} (removed {} entries)",
                    responder.service_name(),
                    invalid_peer,
                    removed
                );
            }
            UdpFlowLookup::Expired => {
                responder.record_event(DatagramMetricEvent::FlowExpired);
                debug!(
                    "UDP load balancer {} expired idle flow",
                    responder.service_name()
                );
            }
            UdpFlowLookup::Missing => {}
        }

        let peer = self.peers.select(self.selection_mode, &flow_key)?.clone();
        match flow_table.upsert(flow_key, peer.clone()) {
            UdpFlowInsert::Inserted | UdpFlowInsert::Replaced => {}
            UdpFlowInsert::TableFull => {
                responder.record_event(DatagramMetricEvent::FlowTableFull);
                debug!(
                    "UDP load balancer {} dropped new flow because the flow table is full",
                    responder.service_name()
                );
                return None;
            }
        }
        Some(peer)
    }

    /// Enable or disable a backend address for future selection.
    pub fn set_peer_enabled(&self, addr: &SocketAddr, enabled: bool) -> bool {
        self.peers.set_enabled(addr, enabled)
    }

    #[cfg(test)]
    fn is_peer_enabled(&self, addr: &SocketAddr) -> bool {
        self.peers.is_enabled(addr)
    }

    #[cfg(test)]
    fn tracked_flows(&self) -> usize {
        self.flow_table.lock().len()
    }

    #[cfg(test)]
    fn max_tracked_flows(&self) -> usize {
        self.flow_table.lock().max_entries()
    }

    #[cfg(test)]
    fn flow_idle_timeout(&self) -> Duration {
        self.flow_table.lock().idle_timeout()
    }
}

#[async_trait]
impl DatagramApp for UdpLoadBalancer {
    async fn process_new(
        &self,
        datagram: Datagram,
        responder: &DatagramResponder,
        _shutdown: &ShutdownWatch,
    ) {
        let Some(peer) = self.select_peer_for_datagram(&datagram, responder) else {
            responder.record_event(DatagramMetricEvent::Dropped);
            debug!(
                "UDP load balancer {} dropped datagram from {}",
                responder.service_name(),
                datagram.meta.peer_addr
            );
            return;
        };

        if let Err(e) = responder.send_to(datagram.payload(), peer.address()).await {
            error!(
                "UDP load balancer {} failed forwarding {} bytes from {} to {}: {}",
                responder.service_name(),
                datagram.payload().len(),
                datagram.meta.peer_addr,
                peer.address(),
                e
            );
        }
    }
}

/// A UDP listening service integrated with Pingora's server lifecycle.
pub struct Service<A> {
    name: String,
    listen_addrs: Vec<String>,
    app_logic: Option<A>,
    stats: Arc<DatagramServiceStats>,
    /// The number of preferred threads. `None` to follow global setting.
    pub threads: Option<usize>,
    /// Maximum datagram size to allocate per receive.
    ///
    /// Datagram payloads that fill the entire receive buffer are treated as
    /// truncated and dropped conservatively.
    pub max_datagram_size: usize,
}

impl<A> Service<A> {
    /// Create a new datagram service with the given application logic.
    pub fn new(name: String, app_logic: A) -> Self {
        Self {
            name,
            listen_addrs: Vec::new(),
            app_logic: Some(app_logic),
            stats: Arc::new(DatagramServiceStats::default()),
            threads: None,
            max_datagram_size: u16::MAX as usize,
        }
    }

    /// Add a UDP listening address to this service.
    pub fn add_udp(&mut self, addr: &str) {
        self.listen_addrs.push(addr.to_string());
    }

    /// Override the preferred number of worker threads for this service.
    pub fn set_threads(&mut self, threads: Option<usize>) {
        self.threads = threads;
    }

    /// Override the receive buffer size used for incoming datagrams.
    pub fn set_max_datagram_size(&mut self, max_datagram_size: usize) {
        self.max_datagram_size = max_datagram_size;
    }

    /// Return the configured UDP listening addresses.
    pub fn endpoints(&self) -> &[String] {
        &self.listen_addrs
    }

    /// Return shared UDP counters for this service.
    pub fn stats(&self) -> Arc<DatagramServiceStats> {
        self.stats.clone()
    }
}

impl<A: DatagramApp + Send + Sync + 'static> Service<A> {
    #[cfg(unix)]
    async fn build_listener(
        addr: &str,
        fds: Option<ListenFds>,
    ) -> pingora_error::Result<UdpListener> {
        use pingora_error::{ErrorType::BindError, OrErr};

        if let Some(fds_table) = fds {
            if let Some(fd) = fds_table.lock().get(addr).copied() {
                return UdpListener::from_raw_fd(fd)
                    .or_err_with(BindError, || format!("Failed to reuse UDP listener {addr}"));
            }

            let listener = UdpListener::bind(addr)
                .await
                .or_err_with(BindError, || format!("Failed to bind UDP listener {addr}"))?;
            fds_table.lock().add(addr.to_string(), listener.as_raw_fd());
            return Ok(listener);
        }

        UdpListener::bind(addr)
            .await
            .or_err_with(BindError, || format!("Failed to bind UDP listener {addr}"))
    }

    #[cfg(windows)]
    async fn build_listener(addr: &str) -> pingora_error::Result<UdpListener> {
        use pingora_error::{ErrorType::BindError, OrErr};

        UdpListener::bind(addr)
            .await
            .or_err_with(BindError, || format!("Failed to bind UDP listener {addr}"))
    }

    async fn run_endpoint(
        service_name: Arc<str>,
        addr: String,
        listener: Arc<UdpListener>,
        app_logic: Arc<A>,
        stats: Arc<DatagramServiceStats>,
        max_datagram_size: usize,
        mut shutdown: ShutdownWatch,
    ) {
        let responder =
            DatagramResponder::new(service_name.clone(), listener.clone(), stats.clone());

        loop {
            let next = tokio::select! {
                packet = listener.recv_datagram(max_datagram_size) => packet,
                shutdown_signal = shutdown.changed() => {
                    match shutdown_signal {
                        Ok(()) if *shutdown.borrow() => {
                            info!(
                                "Shutting down UDP listener {} for service {}",
                                addr, service_name
                            );
                            break;
                        }
                        Ok(()) => continue,
                        Err(e) => {
                            error!(
                                "UDP shutdown signal error on {} for service {}: {}",
                                addr, service_name, e
                            );
                            break;
                        }
                    }
                }
            };

            match next {
                Ok(datagram) => {
                    if datagram.is_truncated() {
                        stats.record(DatagramMetricEvent::Truncated);
                        debug!(
                            "dropped potentially truncated UDP datagram on service {} via {} from {} ({} bytes, buffer {})",
                            service_name,
                            addr,
                            datagram.meta.peer_addr,
                            datagram.payload().len(),
                            max_datagram_size
                        );
                        continue;
                    }

                    stats.record(DatagramMetricEvent::Received);
                    let app = app_logic.clone();
                    let responder = responder.clone();
                    let shutdown = shutdown.clone();
                    debug!(
                        "received UDP datagram on service {} via {} from {} ({} bytes)",
                        service_name,
                        addr,
                        datagram.meta.peer_addr,
                        datagram.payload().len()
                    );
                    pingora_runtime::current_handle().spawn(async move {
                        app.process_new(datagram, &responder, &shutdown).await;
                    });
                }
                Err(e) => {
                    stats.record(DatagramMetricEvent::RecvError);
                    error!(
                        "UDP recv failed on service {} via {}: {}",
                        service_name, addr, e
                    );
                }
            }
        }
    }
}

#[async_trait]
impl<A: DatagramApp + Send + Sync + 'static> ServiceTrait for Service<A> {
    async fn start_service(
        &mut self,
        #[cfg(unix)] fds: Option<ListenFds>,
        shutdown: ShutdownWatch,
        listeners_per_fd: usize,
    ) {
        let runtime = pingora_runtime::current_handle();
        let app_logic = Arc::new(
            self.app_logic
                .take()
                .expect("can only start_service() once"),
        );
        let service_name: Arc<str> = Arc::from(self.name.clone());
        let stats = self.stats.clone();

        let mut handlers = Vec::new();

        for addr in self.listen_addrs.iter() {
            #[cfg(unix)]
            let listener = Self::build_listener(addr, fds.clone())
                .await
                .expect("Failed to build UDP listener");

            #[cfg(windows)]
            let listener = Self::build_listener(addr)
                .await
                .expect("Failed to build UDP listener");

            info!("UDP service {} listening on {}", self.name, addr);
            let listener = Arc::new(listener);

            for _ in 0..listeners_per_fd {
                let shutdown = shutdown.clone();
                let app_logic = app_logic.clone();
                let listener = listener.clone();
                let addr = addr.clone();
                let stats = stats.clone();
                let max_datagram_size = self.max_datagram_size;
                let service_name = service_name.clone();

                handlers.push(runtime.spawn(async move {
                    Self::run_endpoint(
                        service_name,
                        addr,
                        listener,
                        app_logic,
                        stats,
                        max_datagram_size,
                        shutdown,
                    )
                    .await;
                }));
            }
        }

        futures::future::join_all(handlers).await;
        app_logic.cleanup().await;
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn threads(&self) -> Option<usize> {
        self.threads
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DatagramApp, DatagramResponder, DatagramServiceStats, Service, UdpLoadBalancer,
        UdpLoadBalancerOptions,
    };
    use crate::protocols::l4::datagram::UdpListener;
    use crate::protocols::l4::datagram::{Datagram, DatagramMeta};
    use crate::protocols::l4::socket::SocketAddr;
    use crate::server::ShutdownWatch;
    use crate::services::Service as ServiceTrait;
    use crate::upstreams::peer::UdpPeer;
    use crate::upstreams::udp::UdpSelectionMode;
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::time::Duration as StdDuration;
    use tokio::net::UdpSocket;
    use tokio::sync::watch;
    use tokio::time::{timeout, Duration};

    struct EchoApp;

    #[async_trait]
    impl DatagramApp for EchoApp {
        async fn process_new(
            &self,
            datagram: Datagram,
            responder: &DatagramResponder,
            _shutdown: &ShutdownWatch,
        ) {
            let _ = responder.send_datagram(&datagram).await;
        }
    }

    fn test_responder(name: &str) -> DatagramResponder {
        let listener = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .unwrap()
            .block_on(UdpListener::bind("127.0.0.1:0"))
            .unwrap();
        DatagramResponder::new(
            Arc::from(name),
            Arc::new(listener),
            Arc::new(DatagramServiceStats::default()),
        )
    }

    #[test]
    fn datagram_service_tracks_udp_endpoints() {
        let mut service = Service::new("udp".to_string(), ());
        service.add_udp("127.0.0.1:9000");
        service.add_udp("127.0.0.1:9001");

        assert_eq!(
            service.endpoints(),
            &["127.0.0.1:9000".to_string(), "127.0.0.1:9001".to_string()]
        );
    }

    #[test]
    fn udp_load_balancer_tracks_and_reuses_flow_selection() {
        let app = UdpLoadBalancer::new(
            vec![
                UdpPeer::new("127.0.0.1:5300"),
                UdpPeer::new("127.0.0.1:5301"),
            ],
            UdpSelectionMode::FlowHash,
            StdDuration::from_secs(30),
        );
        let datagram = Datagram::new(
            DatagramMeta {
                local_addr: SocketAddr::Inet("127.0.0.1:7000".parse().unwrap()),
                peer_addr: SocketAddr::Inet("127.0.0.1:50000".parse().unwrap()),
            },
            b"hello".to_vec(),
        );
        let responder = test_responder("udp-lb");

        let first = app
            .select_peer_for_datagram(&datagram, &responder)
            .unwrap()
            .address()
            .clone();
        let second = app
            .select_peer_for_datagram(&datagram, &responder)
            .unwrap()
            .address()
            .clone();

        assert_eq!(first, second);
        assert_eq!(app.tracked_flows(), 1);
    }

    #[test]
    fn udp_load_balancer_recognizes_backend_responses() {
        let app = UdpLoadBalancer::new(
            vec![UdpPeer::new("127.0.0.1:5300")],
            UdpSelectionMode::FlowHash,
            StdDuration::from_secs(30),
        );
        let backend_datagram = Datagram::new(
            DatagramMeta {
                local_addr: SocketAddr::Inet("127.0.0.1:7000".parse().unwrap()),
                peer_addr: SocketAddr::Inet("127.0.0.1:5300".parse().unwrap()),
            },
            b"response".to_vec(),
        );
        let responder = test_responder("udp-lb");

        assert!(app
            .select_peer_for_datagram(&backend_datagram, &responder)
            .is_none());
        assert_eq!(app.tracked_flows(), 0);
    }

    #[test]
    fn udp_load_balancer_skips_disabled_peers_and_remaps_flows() {
        let app = UdpLoadBalancer::new(
            vec![
                UdpPeer::new("127.0.0.1:5300"),
                UdpPeer::new("127.0.0.1:5301"),
            ],
            UdpSelectionMode::RoundRobin,
            StdDuration::from_secs(30),
        );
        let datagram = Datagram::new(
            DatagramMeta {
                local_addr: SocketAddr::Inet("127.0.0.1:7000".parse().unwrap()),
                peer_addr: SocketAddr::Inet("127.0.0.1:50000".parse().unwrap()),
            },
            b"hello".to_vec(),
        );
        let responder = test_responder("udp-lb");

        let first = app
            .select_peer_for_datagram(&datagram, &responder)
            .unwrap()
            .address()
            .clone();
        assert!(app.set_peer_enabled(&first, false));
        assert!(!app.is_peer_enabled(&first));

        let second = app
            .select_peer_for_datagram(&datagram, &responder)
            .unwrap()
            .address()
            .clone();

        assert_ne!(first, second);
        assert_eq!(app.tracked_flows(), 1);
    }

    #[test]
    fn udp_load_balancer_drops_new_flows_when_table_is_full() {
        let app = UdpLoadBalancer::new_with_options(
            vec![
                UdpPeer::new("127.0.0.1:5300"),
                UdpPeer::new("127.0.0.1:5301"),
            ],
            UdpSelectionMode::FlowHash,
            UdpLoadBalancerOptions {
                idle_timeout: StdDuration::from_secs(30),
                max_tracked_flows: 1,
                cleanup_interval: None,
            },
        );
        let flow_a = Datagram::new(
            DatagramMeta {
                local_addr: SocketAddr::Inet("127.0.0.1:7000".parse().unwrap()),
                peer_addr: SocketAddr::Inet("127.0.0.1:50000".parse().unwrap()),
            },
            b"first".to_vec(),
        );
        let flow_b = Datagram::new(
            DatagramMeta {
                local_addr: SocketAddr::Inet("127.0.0.1:7000".parse().unwrap()),
                peer_addr: SocketAddr::Inet("127.0.0.1:50001".parse().unwrap()),
            },
            b"second".to_vec(),
        );
        let responder = test_responder("udp-lb");

        assert_eq!(app.max_tracked_flows(), 1);
        assert!(app.select_peer_for_datagram(&flow_a, &responder).is_some());
        assert!(app.select_peer_for_datagram(&flow_b, &responder).is_none());
        assert_eq!(app.tracked_flows(), 1);
    }

    #[test]
    fn udp_load_balancer_supports_custom_idle_timeout_and_cleanup_cadence() {
        let app = UdpLoadBalancer::new_with_options(
            vec![UdpPeer::new("127.0.0.1:5300")],
            UdpSelectionMode::FlowHash,
            UdpLoadBalancerOptions {
                idle_timeout: StdDuration::from_millis(10),
                max_tracked_flows: 8,
                cleanup_interval: Some(StdDuration::from_millis(5)),
            },
        );
        let flow_a = Datagram::new(
            DatagramMeta {
                local_addr: SocketAddr::Inet("127.0.0.1:7000".parse().unwrap()),
                peer_addr: SocketAddr::Inet("127.0.0.1:50000".parse().unwrap()),
            },
            b"first".to_vec(),
        );
        let flow_b = Datagram::new(
            DatagramMeta {
                local_addr: SocketAddr::Inet("127.0.0.1:7000".parse().unwrap()),
                peer_addr: SocketAddr::Inet("127.0.0.1:50001".parse().unwrap()),
            },
            b"second".to_vec(),
        );
        let responder = test_responder("udp-lb");

        assert_eq!(app.flow_idle_timeout(), StdDuration::from_millis(10));
        assert!(app.select_peer_for_datagram(&flow_a, &responder).is_some());
        assert_eq!(app.tracked_flows(), 1);

        std::thread::sleep(StdDuration::from_millis(20));

        assert!(app.select_peer_for_datagram(&flow_b, &responder).is_some());
        assert_eq!(app.tracked_flows(), 1);
    }

    #[tokio::test]
    async fn datagram_service_echoes_and_shuts_down() {
        let port = std::net::UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let listen_addr = format!("127.0.0.1:{port}");

        let mut service = Service::new("udp-echo".to_string(), EchoApp);
        service.add_udp(&listen_addr);

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let service_handle = tokio::spawn(async move {
            #[cfg(unix)]
            ServiceTrait::start_service(&mut service, None, shutdown_rx, 1).await;
            #[cfg(windows)]
            ServiceTrait::start_service(&mut service, shutdown_rx, 1).await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client.send_to(b"ping", &listen_addr).await.unwrap();

        let mut buf = [0; 32];
        let (size, _) = timeout(Duration::from_secs(1), client.recv_from(&mut buf))
            .await
            .expect("timed out waiting for UDP echo")
            .unwrap();
        assert_eq!(&buf[..size], b"ping");

        shutdown_tx.send(true).unwrap();
        timeout(Duration::from_secs(1), service_handle)
            .await
            .expect("timed out waiting for UDP service shutdown")
            .unwrap();
    }

    #[tokio::test]
    async fn datagram_service_drops_potentially_truncated_packets() {
        let port = std::net::UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let listen_addr = format!("127.0.0.1:{port}");

        let mut service = Service::new("udp-echo".to_string(), EchoApp);
        service.add_udp(&listen_addr);
        service.max_datagram_size = 4;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let service_handle = tokio::spawn(async move {
            #[cfg(unix)]
            ServiceTrait::start_service(&mut service, None, shutdown_rx, 1).await;
            #[cfg(windows)]
            ServiceTrait::start_service(&mut service, shutdown_rx, 1).await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client.send_to(b"oversized", &listen_addr).await.unwrap();

        let mut buf = [0; 32];
        let received = timeout(Duration::from_millis(200), client.recv_from(&mut buf)).await;
        assert!(received.is_err(), "oversized datagram should be dropped");

        shutdown_tx.send(true).unwrap();
        timeout(Duration::from_secs(1), service_handle)
            .await
            .expect("timed out waiting for UDP service shutdown")
            .unwrap();
    }

    #[async_trait]
    impl DatagramApp for () {
        async fn process_new(
            &self,
            _datagram: Datagram,
            _responder: &DatagramResponder,
            _shutdown: &ShutdownWatch,
        ) {
        }
    }
}
