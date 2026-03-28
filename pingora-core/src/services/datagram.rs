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
#[cfg(feature = "prometheus")]
use once_cell::sync::Lazy;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
use std::sync::Arc;

use crate::protocols::l4::datagram::{Datagram, UdpListener};
use crate::protocols::l4::socket::SocketAddr;
#[cfg(unix)]
use crate::server::ListenFds;
use crate::server::ShutdownWatch;
use crate::services::Service as ServiceTrait;

#[cfg(feature = "prometheus")]
use prometheus::{register_int_counter_vec, IntCounterVec};

#[cfg(feature = "prometheus")]
static UDP_DATAGRAM_COUNTERS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pingora_udp_datagrams_total",
        "Count of UDP datagram transport events by service and event type.",
        &["service", "event"]
    )
    .expect("failed to register pingora_udp_datagrams_total")
});

fn observe_udp_event(_service: &str, _event: &str) {
    #[cfg(feature = "prometheus")]
    UDP_DATAGRAM_COUNTERS
        .with_label_values(&[_service, _event])
        .inc();
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
}

impl DatagramResponder {
    fn new(service_name: Arc<str>, listener: Arc<UdpListener>) -> Self {
        Self {
            service_name,
            listener,
        }
    }

    /// Return the local address of the underlying UDP listener.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
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
                observe_udp_event(&self.service_name, "sent");
                debug!(
                    "sent UDP datagram on service {} to {} ({} bytes)",
                    self.service_name, addr, size
                );
                Ok(size)
            }
            Err(e) => {
                observe_udp_event(&self.service_name, "send_error");
                error!(
                    "UDP send failed on service {} to {}: {}",
                    self.service_name, addr, e
                );
                Err(e)
            }
        }
    }
}

/// A UDP listening service integrated with Pingora's server lifecycle.
pub struct Service<A> {
    name: String,
    listen_addrs: Vec<String>,
    app_logic: Option<A>,
    /// The number of preferred threads. `None` to follow global setting.
    pub threads: Option<usize>,
    /// Maximum datagram size to allocate per receive.
    pub max_datagram_size: usize,
}

impl<A> Service<A> {
    /// Create a new datagram service with the given application logic.
    pub fn new(name: String, app_logic: A) -> Self {
        Self {
            name,
            listen_addrs: Vec::new(),
            app_logic: Some(app_logic),
            threads: None,
            max_datagram_size: u16::MAX as usize,
        }
    }

    /// Add a UDP listening address to this service.
    pub fn add_udp(&mut self, addr: &str) {
        self.listen_addrs.push(addr.to_string());
    }

    /// Return the configured UDP listening addresses.
    pub fn endpoints(&self) -> &[String] {
        &self.listen_addrs
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
            let mut table = fds_table.lock().await;
            if let Some(fd) = table.get(addr) {
                return UdpListener::from_raw_fd(*fd)
                    .or_err_with(BindError, || format!("Failed to reuse UDP listener {addr}"));
            }

            let listener = UdpListener::bind(addr)
                .await
                .or_err_with(BindError, || format!("Failed to bind UDP listener {addr}"))?;
            table.add(addr.to_string(), listener.as_raw_fd());
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
        max_datagram_size: usize,
        mut shutdown: ShutdownWatch,
    ) {
        let responder = DatagramResponder::new(service_name.clone(), listener.clone());

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
                    observe_udp_event(&service_name, "received");
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
                    observe_udp_event(&service_name, "recv_error");
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
                let max_datagram_size = self.max_datagram_size;
                let service_name = service_name.clone();

                handlers.push(runtime.spawn(async move {
                    Self::run_endpoint(
                        service_name,
                        addr,
                        listener,
                        app_logic,
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
    use super::{DatagramApp, DatagramResponder, Service};
    use crate::protocols::l4::datagram::Datagram;
    use crate::server::ShutdownWatch;
    use crate::services::Service as ServiceTrait;
    use async_trait::async_trait;
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
