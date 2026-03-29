# Prometheus

The [`pingora-prometheus`](https://docs.rs/pingora-prometheus) crate provides a
Prometheus HTTP metrics server for scraping.

## Adding the Dependency

Add `pingora-prometheus` to your `Cargo.toml`:

```toml
pingora-prometheus = "0.9.0"
```

## Setting up a Prometheus Metrics Endpoint

```rust
    ...
    let mut prometheus_service_http = pingora_prometheus::prometheus_http_service();
    prometheus_service_http.add_tcp("0.0.0.0:1234");
    my_server.add_service(prometheus_service_http);
    my_server.run_forever();
```

The simplest way to use it is to have [static metrics](https://docs.rs/prometheus/latest/prometheus/#static-metrics).

```rust
static MY_COUNTER: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!("my_counter", "my counter").unwrap()
});

```

This static metric will automatically appear in the Prometheus metric endpoint.

## UDP Metrics

UDP services track counters through `pingora_core::services::datagram::Service::stats()`.

If you want those counters in a Prometheus endpoint, bridge the returned snapshot
into your own `prometheus` metrics alongside the rest of your application metrics.

```rust,ignore
use once_cell::sync::Lazy;
use pingora_core::services::background::{background_service, BackgroundService};
use pingora_core::server::ShutdownWatch;
use pingora_core::services::datagram::Service as DatagramService;
use pingora_prometheus::prometheus::{register_int_gauge, IntGauge};
use std::sync::Arc;
use std::time::Duration;

static UDP_RECEIVED: Lazy<IntGauge> =
    Lazy::new(|| register_int_gauge!("udp_received", "UDP datagrams received").unwrap());
static UDP_DROPPED: Lazy<IntGauge> =
    Lazy::new(|| register_int_gauge!("udp_dropped", "UDP datagrams dropped").unwrap());

struct UdpMetricsBridge {
    stats: Arc<pingora_core::services::datagram::DatagramServiceStats>,
}

#[async_trait::async_trait]
impl BackgroundService for UdpMetricsBridge {
    async fn start(&self, mut shutdown: ShutdownWatch) {
        loop {
            let snapshot = self.stats.snapshot();
            UDP_RECEIVED.set(snapshot.received as i64);
            UDP_DROPPED.set(snapshot.dropped as i64);

            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
    }
}

let udp_service: DatagramService<MyUdpApp> = /* build your UDP service */;
let udp_stats = udp_service.stats();
server.add_service(udp_service);
server.add_service(background_service(
    "udp metrics bridge",
    UdpMetricsBridge { stats: udp_stats },
));
```

The same pattern applies to QUIC transport stats via `QuicDownstreamListener::stats()` and
`QuicConnectorHandle::stats()`.

See [UDP services](udp.md) for the current event labels and how to interpret
`flow_table_full`, `truncated`, `flow_remap`, `recv_error`, and `send_error`.

## QUIC Metrics

With `quic` enabled, Pingora exposes QUIC transport lifecycle stats through:

- `QuicDownstreamListener::stats()`
- `QuicConnectorHandle::stats()`

These stats cover the same transport lifecycle signals, including:

- downstream datagrams received or dropped
- downstream sessions accepted, reused, or expired
- upstream handshakes attempted, established, timed out, or failed

If you want them in a Prometheus endpoint, bridge those snapshots into application metrics
the same way as the UDP example above.
- `session_reused`
- `session_expired`
- `session_established`
- `session_released`
- `handshake_started`
- `handshake_established`
- `handshake_timeout`

For the pooled upstream HTTP/3 path, the most relevant `h3_pool` events are:

- `session_establish_started`
- `session_established`
- `session_establish_timeout`
- `session_establish_failed`
- `session_reused`
- `session_released`
- `session_expired`

See [HTTP/3 and QUIC](http3.md) for the current transport boundary and limitations.
