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

use pingora_core::protocols::l4::datagram::{Datagram, DatagramFlowKey, DatagramMeta};
use pingora_core::protocols::l4::socket::SocketAddr;
use pingora_core::upstreams::peer::{Http3Peer, HttpPeer, HttpUpstreamTransport};
use pingora_http::Method;
use pingora_proxy::{Http3AcceptedStream, Http3ProxyBridge, Http3UpstreamExecutor};
use pingora_quic::{
    QuicConnectionMeta, QuicDownstreamSession, QuicIncomingDatagram, QuicSessionEvent,
};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

const ITERATIONS: usize = 50_000;

fn socket_addr(port: u16) -> SocketAddr {
    SocketAddr::Inet(format!("127.0.0.1:{port}").parse().unwrap())
}

fn accepted_stream(event: QuicSessionEvent, flow_id: usize, stream_id: u64) -> Http3AcceptedStream {
    let local_addr = socket_addr(4433);
    let peer_addr = socket_addr(45_000 + (flow_id % 10_000) as u16);

    Http3AcceptedStream {
        transport: QuicIncomingDatagram {
            session: QuicDownstreamSession {
                flow_key: DatagramFlowKey {
                    listener_id: Arc::<str>::from("h3-bench"),
                    local_addr: local_addr.clone(),
                    peer_addr: peer_addr.clone(),
                },
                meta: QuicConnectionMeta {
                    local_addr: local_addr.clone(),
                    peer_addr: peer_addr.clone(),
                    alpn_protocol: Some(b"h3".to_vec()),
                    server_name: Some("example.com".to_string()),
                    resumed: false,
                },
                established_at: Instant::now(),
                last_seen: Instant::now(),
                packets_received: 1,
                stream_handle: None,
            },
            datagram: Datagram::new(
                DatagramMeta {
                    local_addr,
                    peer_addr,
                },
                b"headers".to_vec(),
            ),
            event,
        },
        stream_id,
        method: Method::GET,
        path: b"/bench".to_vec(),
        authority: Some("example.com".to_string()),
        headers: vec![("x-bench".to_string(), "1".to_string())],
    }
}

fn main() {
    let bridge = Http3ProxyBridge::new();
    let executor = Http3UpstreamExecutor::new();

    let start = Instant::now();
    for index in 0..ITERATIONS {
        let request = bridge
            .accept_stream(accepted_stream(
                QuicSessionEvent::Accepted,
                index,
                index as u64,
            ))
            .unwrap();
        black_box(request.request_header.version);
    }
    let accepted_elapsed = start.elapsed();

    let bridge = Http3ProxyBridge::new();
    let start = Instant::now();
    for index in 0..ITERATIONS {
        let event = if index == 0 {
            QuicSessionEvent::Accepted
        } else {
            QuicSessionEvent::Reused
        };
        let request = bridge
            .accept_stream(accepted_stream(event, 0, index as u64))
            .unwrap();
        black_box(request.session.requests_seen);
    }
    let reused_elapsed = start.elapsed();

    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let peer = Http3Peer::new(("127.0.0.1", 8443), "example.com".to_string());
        let config = executor.connector_config(&peer);
        black_box(config.alpn_protocols.clone());
        black_box(config.server_name.clone());
    }
    let connector_elapsed = start.elapsed();

    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let selected = executor
            .select_upstream(
                HttpUpstreamTransport::Http3,
                Box::new(HttpPeer::new(
                    ("127.0.0.1", 8080),
                    false,
                    "example.com".to_string(),
                )),
                Some(Box::new(Http3Peer::new(
                    ("127.0.0.1", 8443),
                    "example.com".to_string(),
                ))),
            )
            .unwrap();
        black_box(selected.transport());
    }
    let selection_elapsed = start.elapsed();

    println!(
        "http3_bridge accepted-streams: {:?} total, {:?} avg",
        accepted_elapsed,
        accepted_elapsed / ITERATIONS as u32
    );
    println!(
        "http3_bridge reused-session: {:?} total, {:?} avg",
        reused_elapsed,
        reused_elapsed / ITERATIONS as u32
    );
    println!(
        "http3_bridge upstream-connector-config: {:?} total, {:?} avg",
        connector_elapsed,
        connector_elapsed / ITERATIONS as u32
    );
    println!(
        "http3_bridge upstream-selection: {:?} total, {:?} avg",
        selection_elapsed,
        selection_elapsed / ITERATIONS as u32
    );
}
