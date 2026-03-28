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

use async_trait::async_trait;
use pingora_core::server::configuration::Opt;
use pingora_core::server::{Server, ShutdownWatch};
use pingora_core::services::datagram::{DatagramApp, DatagramResponder, Service};
use pingora_core::{protocols::l4::datagram::Datagram, Result};

struct UdpEcho;

#[async_trait]
impl DatagramApp for UdpEcho {
    async fn process_new(
        &self,
        datagram: Datagram,
        responder: &DatagramResponder,
        _shutdown: &ShutdownWatch,
    ) {
        let _ = responder.send_datagram(&datagram).await;
    }
}

fn main() -> Result<()> {
    let mut server = Server::new(Some(Opt::default()))?;
    server.bootstrap();

    let mut udp = Service::new("UDP echo".to_string(), UdpEcho);
    udp.add_udp("127.0.0.1:6180");

    server.add_service(udp);
    server.run_forever();
}
