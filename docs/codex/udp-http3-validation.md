# UDP and HTTP/3 Validation Notes

This note tracks the intentionally narrow validation surface added for the UDP,
QUIC, and HTTP/3 milestones.

## Benchmarks

The current benchmark set is deliberately small and focused on the new hot paths:

- `cargo bench -p pingora-core --bench udp_flow_table`
- `cargo bench -p pingora-quic --bench upstream_pool`
- `cargo bench -p pingora-proxy --bench http3_bridge --features http3`

What they cover:

- UDP flow selection and flow-table lookup/update behavior
- QUIC upstream establish vs pooled reuse behavior
- HTTP/3 request bridging from accepted QUIC streams into Pingora request headers

## Build Validation Matrix

The current implementation has been kept build-validated across these feature sets:

- `cargo test -p pingora-quic --lib --no-run`
- `cargo test -p pingora-quic --lib --no-run --features tokio-quiche`
- `cargo test -p pingora-proxy --lib --no-run --features http3`
- `cargo test -p pingora --lib --no-run --features http3`
- `cargo check -p pingora-proxy --example http3_proxy --features http3`

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
