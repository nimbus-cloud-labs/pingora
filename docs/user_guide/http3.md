# HTTP/3 and QUIC

Pingora now has a feature-gated QUIC and HTTP/3 foundation, but it is not yet a
fully production-ready end-to-end HTTP/3 proxy.

What exists today:

- downstream HTTP/3 negotiation and `alt-svc` advertisement
- a QUIC transport boundary in `pingora-quic`
- downstream HTTP/3 request bridging into Pingora's request model
- upstream HTTP/3 peer selection, QUIC session pooling, and retry classification

What is still incomplete:

- full wire-level HTTP/3 proxying across all request and response paths
- mature interoperability validation
- production-hardened operational guidance across all QUIC backends

## Feature Gates

If you use the top-level crate, enable `http3`. It also enables `quic`.

```toml
pingora = { version = "0.8.0", features = ["http3"] }
```

If you work at lower layers, use:

- `pingora-quic` for transport primitives
- `pingora-proxy` with `http3` for proxy-facing HTTP/3 helpers

## Downstream HTTP/3

`pingora-proxy` exposes `Http3Negotiation` to make downstream advertisement
explicit.

```rust
use pingora::proxy::Http3Negotiation;

let negotiation = Http3Negotiation {
    enabled: true,
    advertised_port: 443,
    max_age: Some(86_400),
};
```

When enabled, the preferred downstream order is:

1. HTTP/3
2. HTTP/2
3. HTTP/1.1

When disabled, Pingora falls back to:

1. HTTP/2
2. HTTP/1.1

Current downstream compatibility is intentionally conservative:

- generic request and response phases remain reusable
- HTTP/1.x `Connection` and `Upgrade` semantics are rejected for HTTP/3 requests
- use `Http3CompatibilityReport` if you need an explicit view of supported phases

## Upstream HTTP/3

For upstream selection, Pingora exposes `Http3Peer` alongside the existing
stream-oriented `HttpPeer`.

Important fields on `Http3Peer` and `Http3PeerOptions`:

- UDP destination address
- authority / server name
- optional local bind address
- connect timeout
- idle timeout for QUIC session reuse
- ALPN values, defaulting to `h3`

For example:

```rust
use pingora::upstreams::peer::Http3Peer;
use std::time::Duration;

let mut peer = Http3Peer::new(("127.0.0.1", 8443), "example.com".to_string());
peer.options = peer
    .options
    .clone()
    .with_connect_timeout(Some(Duration::from_secs(2)))
    .with_idle_timeout(Some(Duration::from_secs(30)));
```

The upstream transport choice is explicit via `HttpUpstreamTransport`.

## QUIC Session Reuse

Pingora keeps QUIC upstream reuse separate from TCP connection pools.

`pingora-quic::QuicUpstreamPool` currently provides:

- checkout of reusable upstream QUIC sessions
- idle expiration
- explicit release back into the pool
- pool-level observability for establish, reuse, release, and expiration events

This keeps QUIC lifecycle rules visible instead of implicitly reusing the TCP
pooling model.

## Example

See [http3_proxy.rs](/home/alekitto/projects/pingora/pingora-proxy/examples/http3_proxy.rs)
for a minimal example that combines:

- downstream `alt-svc` advertisement
- explicit `HttpUpstreamTransport::Http3` selection
- `Http3Peer` construction for upstream requests
- `Http3RetryClassifier` policy setup

## Retry and Failover

HTTP/3 upstream retries are modeled explicitly with:

- `Http3RetryClassifier`
- `Http3RetryPolicy`
- `Http3RetryDecision`

Current semantics:

- connect-time QUIC failures may retry another backend or fall back to HTTP/2
- mid-request transport failures are only retryable when the request is known to
  be replay-safe
- mid-request HTTP/3 failures default to a fresh QUIC session on the same logical
  peer instead of silently remapping to another backend

See [Handling failures and failover](failover.md) for the generic retry model.

## Metrics

The QUIC transport layer keeps lifecycle counters through:

- `QuicDownstreamListener::stats()`
- `QuicConnectorHandle::stats()`

If you want those values in Prometheus, bridge the snapshots into application metrics
using the same pattern shown in [Prometheus](prom.md).
- `pool`

See [Prometheus](prom.md) for the generic metrics setup.

## Current Boundaries

This page documents the current implementation, not the eventual goal.

Today, the safe way to think about HTTP/3 support in Pingora is:

- QUIC transport boundary: implemented
- downstream negotiation and request bridge: implemented
- upstream HTTP/3 peer and session lifecycle: implemented
- fully integrated end-to-end HTTP/3 proxy path: still in progress
