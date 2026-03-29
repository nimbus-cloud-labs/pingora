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

use pingora_core::protocols::l4::socket::SocketAddr;
use pingora_quic::{NoopQuicTransport, QuicConnectorConfig, QuicTransport, QuicUpstreamPool};
use std::hint::black_box;
use std::time::{Duration, Instant};

const ITERATIONS: usize = 20_000;

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async {
        let transport = NoopQuicTransport;
        let mut config = QuicConnectorConfig::new(
            "bench-upstream",
            SocketAddr::Inet("127.0.0.1:4433".parse().unwrap()),
        );
        config.server_name = Some("example.com".to_string());
        config.connect_timeout = Duration::from_secs(1);
        config.idle_timeout = Duration::from_secs(30);

        let connector = transport.connect(config).await.unwrap();
        let pool = QuicUpstreamPool::new();

        let start = Instant::now();
        for _ in 0..ITERATIONS {
            let session = connector.establish().await.unwrap();
            black_box(session.meta.peer_addr.clone());
        }
        let establish_elapsed = start.elapsed();

        let (session, _) = pool.checkout(&connector).await.unwrap();
        pool.release(session);

        let start = Instant::now();
        for _ in 0..ITERATIONS {
            let (session, reused) = pool.checkout(&connector).await.unwrap();
            black_box(reused);
            black_box(session.reuse_count);
            pool.release(session);
        }
        let reuse_elapsed = start.elapsed();

        let mut expired = connector.establish().await.unwrap();
        expired.last_used_at = Instant::now() - expired.idle_timeout - Duration::from_secs(1);
        pool.release(expired);

        let start = Instant::now();
        let expired = black_box(pool.prune_expired());
        let prune_elapsed = start.elapsed();

        println!(
            "quic_upstream_pool establish: {:?} total, {:?} avg",
            establish_elapsed,
            establish_elapsed / ITERATIONS as u32
        );
        println!(
            "quic_upstream_pool reuse: {:?} total, {:?} avg",
            reuse_elapsed,
            reuse_elapsed / ITERATIONS as u32
        );
        println!(
            "quic_upstream_pool prune_expired: {:?} total, removed {} sessions",
            prune_elapsed, expired
        );
    });
}
