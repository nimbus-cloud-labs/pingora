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

use pingora_core::server::configuration::Opt;
use pingora_core::server::Server;
use pingora_core::services::datagram::{Service, UdpLoadBalancer, UdpLoadBalancerOptions};
use pingora_core::upstreams::peer::UdpPeer;
use pingora_core::upstreams::udp::UdpSelectionMode;
use pingora_core::Result;
use std::time::Duration;

fn main() -> Result<()> {
    let mut server = Server::new(Some(Opt::default()))?;
    server.bootstrap();

    let upstreams = vec![
        UdpPeer::new_with_weight("127.0.0.1:5300", 2),
        UdpPeer::new_with_weight("127.0.0.1:5301", 1),
    ];

    let app = UdpLoadBalancer::new_with_options(
        upstreams,
        UdpSelectionMode::FlowHash,
        UdpLoadBalancerOptions::new(Duration::from_secs(30))
            .with_max_tracked_flows(65_536)
            .with_cleanup_interval(Some(Duration::from_secs(5))),
    );

    let mut udp = Service::new("UDP load balancer".to_string(), app);
    udp.add_udp("127.0.0.1:6181");
    udp.set_max_datagram_size(1400);

    server.add_service(udp);
    server.run_forever();
}
