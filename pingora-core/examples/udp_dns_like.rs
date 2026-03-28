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

//! This example shows how to route DNS-like UDP request traffic toward upstream servers.
//!
//! The UDP load balancer now keeps a per-flow upstream socket so backend responses
//! can be routed back to the original client generically.

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
        UdpPeer::new("127.0.0.1:5353"),
        UdpPeer::new("127.0.0.1:5354"),
    ];

    let app = UdpLoadBalancer::new_with_options(
        upstreams,
        UdpSelectionMode::FlowHash,
        UdpLoadBalancerOptions::new(Duration::from_secs(10))
            .with_max_tracked_flows(16_384)
            .with_cleanup_interval(Some(Duration::from_secs(2))),
    );

    let mut udp = Service::new("UDP dns-like router".to_string(), app);
    udp.add_udp("127.0.0.1:6190");
    udp.set_max_datagram_size(1232);

    server.add_service(udp);
    server.run_forever();
}
