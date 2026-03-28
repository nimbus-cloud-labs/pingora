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

use pingora_core::protocols::l4::datagram::DatagramFlowKey;
use pingora_core::protocols::l4::socket::SocketAddr;
use pingora_core::upstreams::peer::UdpPeer;
use pingora_core::upstreams::udp::{UdpFlowLookup, UdpFlowTable, UdpPeerSet, UdpSelectionMode};
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

const FLOW_COUNT: usize = 50_000;

fn socket_addr(port: u16) -> SocketAddr {
    SocketAddr::Inet(format!("127.0.0.1:{port}").parse().unwrap())
}

fn flow_key(index: usize) -> DatagramFlowKey {
    DatagramFlowKey {
        listener_id: Arc::<str>::from("udp-bench"),
        local_addr: socket_addr(5300),
        peer_addr: socket_addr(40_000 + (index % 20_000) as u16),
    }
}

fn main() {
    let peers = vec![
        UdpPeer::new_with_weight("127.0.0.1:6300", 2),
        UdpPeer::new_with_weight("127.0.0.1:6301", 1),
        UdpPeer::new_with_weight("127.0.0.1:6302", 1),
    ];
    let peer_set = UdpPeerSet::new(peers);
    let mut flow_table = UdpFlowTable::new(Duration::from_secs(30), FLOW_COUNT * 2);

    let start = Instant::now();
    for index in 0..FLOW_COUNT {
        let key = flow_key(index);
        let peer = peer_set
            .select(UdpSelectionMode::FlowHash, &key)
            .expect("bench peer");
        black_box(flow_table.upsert(key, peer.clone()));
    }
    let upsert_elapsed = start.elapsed();

    let start = Instant::now();
    for index in 0..FLOW_COUNT {
        let result = flow_table.lookup(&flow_key(index));
        match result {
            UdpFlowLookup::Active(entry) => {
                black_box(entry.peer.address());
            }
            UdpFlowLookup::Expired | UdpFlowLookup::Missing => unreachable!("bench flow missing"),
        }
    }
    let lookup_elapsed = start.elapsed();

    let start = Instant::now();
    black_box(flow_table.cleanup_expired());
    let cleanup_elapsed = start.elapsed();

    println!(
        "udp_flow_table upsert: {:?} total, {:?} avg",
        upsert_elapsed,
        upsert_elapsed / FLOW_COUNT as u32
    );
    println!(
        "udp_flow_table lookup: {:?} total, {:?} avg",
        lookup_elapsed,
        lookup_elapsed / FLOW_COUNT as u32
    );
    println!("udp_flow_table cleanup: {:?} total", cleanup_elapsed);
}
