# UDP and HTTP/3 Validation Notes

This note tracks the intentionally narrow validation surface added for the UDP,
QUIC, and HTTP/3 milestones.

For the current external-peer validation plan, see
[HTTP/3 Interoperability Matrix](http3-interop-matrix.md).

## Benchmarks

The current benchmark set is deliberately small and focused on the new hot paths:

- `cargo bench -p pingora-core --bench udp_flow_table`
- `cargo bench -p pingora-quic --bench upstream_pool`
- `cargo bench -p pingora-proxy --bench http3_bridge --features http3`

What they cover:

- UDP flow selection and flow-table lookup/update behavior
- QUIC upstream establish vs pooled reuse behavior
- QUIC upstream prune/retirement behavior
- HTTP/3 request bridging from accepted QUIC streams into Pingora request headers
- HTTP/3 upstream peer-to-connector mapping, pooled request execution, and retry outcome wiring
- bidirectional UDP request/response routing is covered by service-layer runtime tests
- tokio-quiche runtime handshake and basic QUIC stream exchange are covered by
  feature-gated `pingora-quic` tests

## Build Validation Matrix

The current implementation has been kept build-validated across these feature sets:

- `cargo test -p pingora-quic --lib --no-run`
- `cargo test -p pingora-quic --lib --no-run --features tokio-quiche`
- `cargo test -p pingora-proxy --lib --no-run --features http3`
- `cargo test -p pingora --lib --no-run --features quic,http3`
- `cargo check -p pingora-proxy --example http3_proxy --features http3`
- `cargo test -p pingora-proxy --features http3 upstream_executor_`

The UDP service layer also contains runtime tests for:

- backend response routing to the original client
- dropping backend responses after flow expiration

The QUIC crate now also contains feature-gated runtime tests for:

- downstream tokio-quiche handshake acceptance
- upstream tokio-quiche session establishment
- basic bidirectional QUIC stream event delivery
- pooled reuse of real upstream QUIC sessions

The proxy crate now also contains runtime tests for:

- real upstream HTTP/3 request/response round trips against a controlled origin
- connect-failure retry/fallback classification on the real executor path
- pooled reuse of upstream HTTP/3 sessions across sequential requests

Those tests are logically part of the validation surface, but may still be
blocked in restricted sandboxes that deny local UDP listener bind or
`connect()` on ephemeral sockets. The upstream HTTP/3 runtime tests now
short-circuit on `PermissionDenied` in those environments instead of reporting
false negatives.

This is intentionally a build and API-surface validation matrix, not a claim of
full runtime interoperability.

## Known Gaps

Still intentionally out of scope for the current milestone:

- runtime interop tests against external HTTP/3 servers
- cross-platform QUIC socket validation outside the current Linux-first path
- comparative benchmarking across multiple QUIC backends
- full end-to-end downstream-to-upstream HTTP/3 proxy latency measurements

These gaps are recorded here so future work can add them without confusing the
current milestone with production completeness.
