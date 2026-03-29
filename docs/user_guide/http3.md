# HTTP/3 and QUIC

Pingora now has a feature-gated QUIC and HTTP/3 foundation, but it is not yet a
fully production-ready end-to-end HTTP/3 proxy.

## Support Status

Treat the current surface like this:

- Supported foundation:
  - UDP forwarding path
  - real QUIC transport boundary on the selected backend path
  - downstream HTTP/3 request/response bridge
  - upstream HTTP/3 request execution against controlled origins
- Experimental:
  - external interoperability beyond the documented matrix
  - production operational tuning across all environments
- Intentionally unsupported today:
  - upstream request trailers
  - upstream response trailers
  - downstream request trailers
  - extension-heavy HTTP/3 paths outside the current request/response model

What exists today:

- downstream HTTP/3 negotiation and `alt-svc` advertisement
- a QUIC transport boundary in `pingora-quic`
- downstream HTTP/3 request, body, and response bridging into Pingora's request model
- upstream HTTP/3 peer selection, pooled session execution, and retry classification

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
- downstream request bodies are surfaced as explicit HTTP/3 body chunks
- downstream response headers, body bytes, and response trailers can be written back on real HTTP/3 streams
- downstream request trailers are not surfaced by the current backend and must be treated as unsupported
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

`pingora-proxy` also exposes `Http3UpstreamExecutor`, which currently provides:

- `Http3Peer` to real `QuicConnectorConfig` mapping
- pooled HTTP/3 session checkout and release
- real request header and request body execution toward an origin
- real response header and response body mapping back into Pingora types
- retry/fallback outcomes based on `Http3RetryClassifier`

Current upstream HTTP/3 limits are still conservative:

- request bodies are supported as buffered body chunks
- response bodies are surfaced as buffered body chunks
- upstream request trailers are not modeled yet
- upstream response trailers are not surfaced by the current boundary and must be treated as unsupported

See [docs/codex/http3-interop-matrix.md](/home/alekitto/projects/pingora/docs/codex/http3-interop-matrix.md)
for the current external-peer validation plan.

## QUIC Session Reuse

Pingora keeps QUIC upstream reuse separate from TCP connection pools.

`pingora-quic::QuicUpstreamPool` currently provides:

- checkout of reusable upstream QUIC sessions
- idle expiration
- explicit release back into the pool
- pool-level observability for establish, reuse, release, and expiration events

This keeps QUIC lifecycle rules visible instead of implicitly reusing the TCP
pooling model.

For real upstream HTTP/3 requests, `pingora-quic::Http3UpstreamPool` builds on
the same idea but keeps the HTTP/3 controller and QUIC session together for
pooled reuse.

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
- `h3_pool`

See [Prometheus](prom.md) for the generic metrics setup.

## Current Boundaries

This page documents the current implementation, not the eventual goal.

Today, the safe way to think about HTTP/3 support in Pingora is:

- QUIC transport boundary: implemented
- downstream negotiation and request bridge: implemented
- upstream HTTP/3 peer selection, pooled execution, and retry outcome wiring: implemented
- fully integrated end-to-end HTTP/3 proxy path: still in progress
