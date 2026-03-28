# UDP Services

Pingora's UDP path is designed for datagram-aware forwarding and load balancing.
It is intentionally conservative today:

- flow affinity is tracked with a bounded flow table
- forwarding is best-effort and unbuffered
- datagrams that fill the receive buffer are treated as truncated and dropped
- IP fragmentation is not reconstructed in user space
- the current UDP load balancer forwards client-to-upstream traffic first; backend-to-client response routing is still incomplete

## Operational Defaults

The main knobs live in `pingora_core::services::datagram::UdpLoadBalancerOptions`
and `pingora_core::services::datagram::Service`.

- `idle_timeout`: how long a flow mapping stays active without traffic
- `max_tracked_flows`: hard cap for affinity state
- `cleanup_interval`: optional periodic cleanup cadence; `None` keeps lazy expiration only
- `max_datagram_size`: receive buffer size per packet

Reasonable starting points:

- generic UDP forwarding: `idle_timeout = 30s`, `max_tracked_flows = 65536`, `cleanup_interval = 5s`, `max_datagram_size = 1400`
- DNS-like traffic: `idle_timeout = 10s`, `max_tracked_flows = 16384`, `cleanup_interval = 2s`, `max_datagram_size = 1232`

These values match the shipped examples in [udp_lb.rs](/home/alekitto/projects/pingora/pingora-core/examples/udp_lb.rs) and [udp_dns_like.rs](/home/alekitto/projects/pingora/pingora-core/examples/udp_dns_like.rs).

## Sizing Guidance

`max_tracked_flows` bounds memory and protects the service under pressure. When the
table is full, Pingora drops new flows instead of queueing them internally.

Use a shorter `idle_timeout` when:

- clients churn quickly
- affinity is not critical
- memory retention matters more than stickiness

Use a longer `idle_timeout` when:

- the protocol expects backend affinity across bursts
- request spacing is irregular
- you can afford more retained state

`cleanup_interval` is optional. `None` means expired flows are removed lazily on
lookup. A small periodic cleanup interval keeps memory flatter at the cost of more
churn work on the hot path.

## Socket and MTU Expectations

The implementation is Linux-first. Plan deployments and tuning around Linux socket
behavior unless you have validated another target explicitly.

Choose `max_datagram_size` below your expected path MTU when possible. Pingora does
not attempt to reconstruct fragmented IP datagrams. If a packet fills the receive
buffer exactly, Pingora treats it as potentially truncated and drops it rather than
forwarding ambiguous payload bytes.

## Observability

UDP services maintain per-service counters via `Service::stats().snapshot()`.

Current `event` labels include:

- `received`
- `sent`
- `recv_error`
- `send_error`
- `ignored_upstream`
- `flow_remap`
- `flow_expired`
- `flow_cleanup`
- `flow_table_full`
- `truncated`
- `dropped`

Useful interpretations:

- rising `flow_table_full` means `max_tracked_flows` is too small or flow churn is too high
- rising `truncated` usually means `max_datagram_size` is too small for the workload
- rising `flow_remap` means backends are being disabled or churn is invalidating affinity
- rising `recv_error` or `send_error` points to transport/socket issues

## Example Configuration

```rust
use pingora_core::services::datagram::{Service, UdpLoadBalancer, UdpLoadBalancerOptions};
use pingora_core::upstreams::peer::UdpPeer;
use pingora_core::upstreams::udp::UdpSelectionMode;
use std::time::Duration;

let app = UdpLoadBalancer::new_with_options(
    vec![
        UdpPeer::new_with_weight("127.0.0.1:5300", 2),
        UdpPeer::new_with_weight("127.0.0.1:5301", 1),
    ],
    UdpSelectionMode::FlowHash,
    UdpLoadBalancerOptions::new(Duration::from_secs(30))
        .with_max_tracked_flows(65_536)
        .with_cleanup_interval(Some(Duration::from_secs(5))),
);

let mut udp = Service::new("UDP load balancer".to_string(), app);
udp.add_udp("127.0.0.1:6181");
udp.set_max_datagram_size(1400);
```
