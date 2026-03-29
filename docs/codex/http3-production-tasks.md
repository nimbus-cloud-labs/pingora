# HTTP/3 Production Readiness Task Breakdown

This document expands the production-readiness roadmap into concrete work items.

Completed items should be marked with the `[Implemented]` prefix.

## Milestone 13: Interoperability Validation

### [Implemented] Task 13.1: Define the external client and origin test matrix

Summary: choose the real peer matrix that Pingora must pass before the HTTP/3
stack can be considered production-ready.

Implementation steps:

- extend the matrix in `docs/codex/http3-interop-matrix.md`
- split scenarios into downstream, upstream, and mixed-mode deployments
- record exact client and origin versions used for validation
- define which failures are blockers and which are acceptable unsupported cases

Relevant code and docs:

- `docs/codex/http3-interop-matrix.md`
- `docs/user_guide/http3.md`
- `pingora-proxy/src/proxy_h3.rs`
- `pingora-quic/src/lib.rs`

### [Implemented] Task 13.2: Validate downstream HTTP/3 against real clients

Summary: prove that Pingora behaves correctly as an HTTP/3 ingress with
external client implementations.

Implementation steps:

- run the downstream path against `curl --http3` and `h3i`
- verify request headers, bodies, resets, and negotiation behavior
- record any peer-specific quirks and decide whether to fix or document them
- add integration notes to the interop matrix

Relevant code and docs:

- `pingora-proxy/src/proxy_h3.rs`
- `pingora-proxy/examples/http3_proxy.rs`
- `docs/codex/http3-interop-matrix.md`

### Task 13.3: Validate upstream HTTP/3 against real origins

Summary: prove that Pingora can forward requests to real HTTP/3 origins without
relying only on controlled fixtures.

Implementation steps:

- test the upstream executor against non-test origins or reproducible external
  servers
- validate response headers, body framing, and stream completion behavior
- verify QUIC session reuse and origin-specific edge cases
- classify failures into implementation bug, origin quirk, or unsupported case

Relevant code and docs:

- `pingora-proxy/src/proxy_h3.rs`
- `pingora-core/src/upstreams/peer.rs`
- `pingora-quic/src/lib.rs`
- `docs/codex/http3-interop-matrix.md`

### Task 13.4: Validate mixed-mode ingress and origin routing

Summary: verify the main deployment shape where Pingora terminates HTTP/3 and
forwards to HTTP/1.1, HTTP/2, and HTTP/3 backends.

Implementation steps:

- exercise one downstream HTTP/3 ingress against upstream HTTP/1.1 backends
- repeat for upstream HTTP/2 and HTTP/3 backends
- validate header mapping, body streaming, retry boundaries, and fallback rules
- record any protocol-specific caveats in the docs

Relevant code and docs:

- `pingora-proxy/src/proxy_trait.rs`
- `pingora-proxy/src/proxy_h3.rs`
- `docs/user_guide/http3.md`

## Milestone 14: Failure-Mode Hardening

### Task 14.1: Add explicit failure-mode tests for connect and handshake errors

Summary: validate retry-safe behavior before any request state is committed.

Implementation steps:

- add tests for QUIC connect timeout, handshake failure, and early close
- confirm retry classification remains conservative and predictable
- verify pool state is not leaked after failed connection attempts

Relevant code and docs:

- `pingora-quic/src/lib.rs`
- `pingora-proxy/src/proxy_h3.rs`
- `docs/user_guide/failover.md`

### Task 14.2: Harden mid-stream failure handling

Summary: verify the request lifecycle remains correct when failures happen after
stream creation or after part of the body is transferred.

Implementation steps:

- add tests for upstream reset during request body upload
- add tests for downstream disconnect during response body streaming
- verify that pool reuse, fallback, and retry logic do not cross unsafe
  boundaries
- audit stream cleanup for partial transfers

Relevant code and docs:

- `pingora-proxy/src/proxy_h3.rs`
- `pingora-quic/src/lib.rs`

### Task 14.3: Document retry, failover, and fallback support boundaries

Summary: make the decision rules visible so operators and contributors know what
is expected to happen under failure.

Implementation steps:

- refine the failover guide with HTTP/3-specific examples
- document which failures are retryable, fallback-only, or terminal
- align docs with the exact behavior implemented in the classifier

Relevant code and docs:

- `docs/user_guide/failover.md`
- `docs/user_guide/http3.md`
- `pingora-proxy/src/proxy_h3.rs`

## Milestone 15: Streaming and Load Validation

### Task 15.1: Add large-body streaming validation

Summary: confirm the HTTP/3 path behaves correctly for large payloads without
assuming convenient chunk boundaries.

Implementation steps:

- add tests for large downstream request bodies
- add tests for large upstream response bodies
- verify chunk aggregation, end-of-stream handling, and cleanup behavior

Relevant code and docs:

- `pingora-proxy/src/proxy_h3.rs`
- `pingora-proxy/benches/http3_bridge.rs`

### Task 15.2: Add concurrent-stream and pool-pressure validation

Summary: validate the QUIC and HTTP/3 reuse model under higher concurrency.

Implementation steps:

- add multi-stream tests for one QUIC session
- stress upstream session reuse under concurrent requests
- validate pool pruning and eviction while traffic is active
- record expected invariants for stream and session counts

Relevant code and docs:

- `pingora-quic/src/lib.rs`
- `pingora-quic/benches/upstream_pool.rs`
- `pingora-proxy/src/proxy_h3.rs`

### Task 15.3: Add soak-style validation notes and scripts

Summary: make repeated validation practical instead of relying only on ad-hoc
 local runs.

Implementation steps:

- document representative long-running scenarios
- record which examples or test harnesses to run for soak validation
- capture expected metrics and failure signals during those runs

Relevant code and docs:

- `docs/codex/udp-http3-validation.md`
- `docs/codex/http3-production-review.md`
- `pingora-proxy/examples/http3_proxy.rs`

## Milestone 16: Observability and Operations

### Task 16.1: Expand QUIC and HTTP/3 metrics for production debugging

Summary: export the signals needed to understand pool, stream, retry, timeout,
and fallback behavior in live systems.

Implementation steps:

- review existing counters and identify gaps
- add metrics for session reuse, stream resets, pool pruning, and fallback
  outcomes
- keep label cardinality bounded and deployment-friendly

Relevant code and docs:

- `pingora-quic/src/lib.rs`
- `pingora-proxy/src/proxy_h3.rs`
- `docs/user_guide/prom.md`

### Task 16.2: Improve operational logging and troubleshooting notes

Summary: make logs and docs useful when HTTP/3 incidents occur in production.

Implementation steps:

- improve log messages around connect, handshake, reset, retry, and fallback
- add a troubleshooting section to the HTTP/3 guide
- document the main failure signatures and likely causes

Relevant code and docs:

- `pingora-quic/src/lib.rs`
- `pingora-proxy/src/proxy_h3.rs`
- `docs/user_guide/http3.md`

### Task 16.3: Document deployment tuning guidance

Summary: publish the minimum operational guidance required to run the stack
without guessing at limits and timeouts.

Implementation steps:

- document recommended timeout and pool settings
- document feature combinations for QUIC and HTTP/3 deployments
- explain current platform assumptions and known caveats

Relevant code and docs:

- `docs/user_guide/http3.md`
- `docs/user_guide/udp.md`
- `pingora-quic/src/lib.rs`
- `pingora-core/src/services/datagram.rs`

## Milestone 17: Support Boundary and Release Readiness

### Task 17.1: Make support status explicit in user-facing docs

Summary: separate what is supported from what remains experimental or
unsupported.

Implementation steps:

- update the HTTP/3 guide to include a support boundary section
- list unsupported or partial features explicitly
- align wording across docs so there is one consistent status line

Relevant code and docs:

- `docs/user_guide/http3.md`
- `docs/codex/http3-production-review.md`

### Task 17.2: Add release-facing examples for supported topologies

Summary: make it easy to validate the supported deployment shapes by example.

Implementation steps:

- refine examples for HTTP/3 ingress to HTTP/1.1, HTTP/2, and HTTP/3 upstreams
- document which examples are recommended for validation and demos
- keep unsupported scenarios out of the "happy path" examples

Relevant code and docs:

- `pingora-proxy/examples/http3_proxy.rs`
- `pingora-proxy/examples/gateway.rs`
- `docs/user_guide/http3.md`

### Task 17.3: Define the release gate for HTTP/3 support

Summary: write down the exact checks and evidence required before changing the
support status.

Implementation steps:

- define the required interop runs
- define the required test, bench, and soak evidence
- define the documentation updates needed before release
- record the final support decision in the production review note

Relevant code and docs:

- `docs/codex/http3-production-review.md`
- `docs/codex/http3-production-roadmap.md`
- `docs/codex/udp-http3-validation.md`
