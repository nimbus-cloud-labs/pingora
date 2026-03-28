# UDP and HTTP/3 Later Milestones Task Breakdown

This document turns the remaining roadmap into concrete implementation tasks for the later milestones:

- Milestone 2: UDP upstreams and load balancing
- Milestone 3: operational hardening for UDP
- Milestone 4: QUIC transport foundation
- Milestone 5: downstream HTTP/3 support
- Milestone 6: upstream HTTP/3 support
- Milestone 7: production readiness and ecosystem fit

## Milestone 2

### [Implemented] Task 2.1

Concise, user-visible summary of the fix

Introduce a first UDP upstream peer type that models datagram destinations without inheriting TCP or TLS semantics.

Step-by-step, self-contained instructions for implementing the change.

1. Add a UDP-specific upstream type such as `UdpPeer` in the upstream layer.
2. Reuse only the parts of the current peer model that still make sense for UDP, such as destination address, bind hints, DSCP, and lightweight metadata.
3. Keep TLS, ALPN, stream reuse, and connection-specific behavior out of the new type.
4. Define a small options struct for UDP-specific transport hints if needed.
5. Add unit tests that verify construction, equality, and any helper behavior used by the balancing layer.

Provide just enough context for the assignee to pinpoint the code.

- Existing upstream model: `pingora-core/src/upstreams/peer.rs`
- Existing UDP design note: `docs/codex/udp-upstream-model.md`
- Implemented UDP upstream peer type: `pingora-core/src/upstreams/peer.rs`

### [Implemented] Task 2.2

Concise, user-visible summary of the fix

Add a UDP backend set and baseline selection layer for datagram services.

Step-by-step, self-contained instructions for implementing the change.

1. Define how a UDP service will hold and update its available upstream backends.
2. Reuse the existing load-balancing crate concepts where possible, but avoid assuming HTTP or TCP upstream behavior.
3. Implement at least round robin and deterministic hash-based selection for UDP backends.
4. Define how the selection input uses `DatagramFlowKey`.
5. Add tests that prove stable backend choice for the same flow key and fair distribution for round robin.

Provide just enough context for the assignee to pinpoint the code.

- Existing balancing crate: `pingora-load-balancing`
- Existing datagram flow key: `pingora-core/src/protocols/l4/datagram.rs`
- Implemented UDP backend selection layer: `pingora-core/src/upstreams/udp.rs`

### [Implemented] Task 2.3

Concise, user-visible summary of the fix

Introduce the first UDP flow table so upstream selection remains stable across packets in the same flow.

Step-by-step, self-contained instructions for implementing the change.

1. Add a flow-mapping table keyed by `DatagramFlowKey`.
2. Store the selected backend and the minimum metadata needed for refresh and remap.
3. Refresh flow activity on each packet.
4. Expire flows after configurable idle timeout.
5. Add tests for insertion, lookup, refresh, expiration, and remap after backend invalidation.

Provide just enough context for the assignee to pinpoint the code.

- Existing flow-key model: `pingora-core/src/protocols/l4/datagram.rs`
- Existing balancing model note: `docs/codex/udp-balancing-model.md`
- Implemented UDP flow table: `pingora-core/src/upstreams/udp.rs`

### [Implemented] Task 2.4

Concise, user-visible summary of the fix

Wire the UDP service layer to upstream selection so received datagrams can be forwarded to chosen backends.

Step-by-step, self-contained instructions for implementing the change.

1. Extend the datagram service path to support forwarding instead of only local echo behavior.
2. Connect packet receipt to flow lookup and backend selection.
3. Define how outbound forwarding uses the chosen upstream destination.
4. Preserve the original peer metadata needed to route responses back correctly later.
5. Add tests or harnesses that validate selection and forwarding decisions at the service layer.

Provide just enough context for the assignee to pinpoint the code.

- Datagram service entrypoint: `pingora-core/src/services/datagram.rs`
- UDP listener and packet model: `pingora-core/src/protocols/l4/datagram.rs`
- Implemented UDP forwarding app: `pingora-core/src/services/datagram.rs`

### [Implemented] Task 2.5

Concise, user-visible summary of the fix

Add the first health and availability model for UDP backends.

Step-by-step, self-contained instructions for implementing the change.

1. Define how UDP backends are marked enabled, disabled, or temporarily unavailable.
2. Reuse the existing backend-health concepts where practical.
3. Ensure new flow selection skips unavailable backends.
4. Define how an existing flow remaps when its backend is no longer valid.
5. Add tests that cover backend disablement and flow reselection.

Provide just enough context for the assignee to pinpoint the code.

- Existing health-check patterns: `pingora-load-balancing`
- Existing UDP upstream model note: `docs/codex/udp-upstream-model.md`
- Implemented UDP backend availability and remap: `pingora-core/src/upstreams/udp.rs` and `pingora-core/src/services/datagram.rs`

### [Implemented] Task 2.6

Concise, user-visible summary of the fix

Add first user-facing UDP forwarding examples for generic UDP and DNS-like traffic.

Step-by-step, self-contained instructions for implementing the change.

1. Add an example that forwards UDP datagrams to one of several backends.
2. Add a second example or test shape tailored to a DNS-like request-response pattern.
3. Keep examples small and focused on the datagram and balancing API.
4. Document configuration assumptions and limits.
5. Update the milestone notes if example feedback changes API expectations.

Provide just enough context for the assignee to pinpoint the code.

- Existing example layout: `pingora-core/examples`
- Existing UDP echo example: `pingora-core/examples/udp_echo.rs`
- UDP load-balancer example: `pingora-core/examples/udp_lb.rs`
- DNS-like UDP routing example: `pingora-core/examples/udp_dns_like.rs`

## Milestone 3

### [Implemented] Task 3.1

Concise, user-visible summary of the fix

Add explicit packet drop accounting and flow lifecycle observability to the UDP path.

Step-by-step, self-contained instructions for implementing the change.

1. Extend the UDP metrics surface with drop and expiration counters.
2. Log why packets are dropped or remapped.
3. Record flow expiration, backend invalidation, and forwarding errors.
4. Keep the metric names transport-oriented and stable.
5. Add tests for observability bookkeeping where possible.

Provide just enough context for the assignee to pinpoint the code.

- UDP service observability: `pingora-core/src/services/datagram.rs`
- Planned flow table from Milestone 2
- Implemented flow lifecycle observability: `pingora-core/src/services/datagram.rs` and `pingora-core/src/upstreams/udp.rs`

### [Implemented] Task 3.2

Concise, user-visible summary of the fix

Introduce flow-table limits and backpressure policies so UDP load balancing remains bounded under pressure.

Step-by-step, self-contained instructions for implementing the change.

1. Add configuration for maximum flow-table size.
2. Define what happens when the table is full.
3. Define buffering limits or no-buffer policies for outbound forwarding.
4. Make overload behavior observable.
5. Add tests for limit enforcement and failure behavior.

Provide just enough context for the assignee to pinpoint the code.

- UDP flow state implementation from Milestone 2
- Datagram service layer: `pingora-core/src/services/datagram.rs`
- Implemented bounded flow table and overload policy: `pingora-core/src/upstreams/udp.rs` and `pingora-core/src/services/datagram.rs`

### [Implemented] Task 3.3

Concise, user-visible summary of the fix

Make UDP timeout and expiration behavior configurable at the service level.

Step-by-step, self-contained instructions for implementing the change.

1. Add service-level configuration for flow idle timeout.
2. Add cleanup cadence or lazy-expiration policy settings where needed.
3. Document the operational tradeoff between memory retention and affinity stability.
4. Ensure the defaults are conservative for generic UDP proxying.
5. Add tests that verify timeout behavior under different settings.

Provide just enough context for the assignee to pinpoint the code.

- Datagram service module: `pingora-core/src/services/datagram.rs`
- Flow table implementation from Milestone 2
- Implemented timeout and cleanup options: `pingora-core/src/services/datagram.rs`

### [Implemented] Task 3.4

Concise, user-visible summary of the fix

Handle oversized packets and fragmentation-related edge cases safely in the UDP forwarding path.

Step-by-step, self-contained instructions for implementing the change.

1. Define the first behavior for packets larger than the configured receive buffer.
2. Ensure truncation or rejection behavior is explicit and observable.
3. Document fragmentation assumptions and what Pingora does not attempt to reconstruct.
4. Add tests that validate oversized-packet handling.
5. Update examples or docs to explain the limits clearly.

Provide just enough context for the assignee to pinpoint the code.

- Datagram buffer model: `pingora-core/src/protocols/l4/datagram.rs`
- UDP service layer: `pingora-core/src/services/datagram.rs`
- Implemented truncation handling and drop behavior: `pingora-core/src/protocols/l4/datagram.rs` and `pingora-core/src/services/datagram.rs`

### [Implemented] Task 3.5

Concise, user-visible summary of the fix

Document deployment expectations and socket tuning guidance for UDP load balancing.

Step-by-step, self-contained instructions for implementing the change.

1. Add user-facing docs for UDP service sizing, flow timeouts, and socket buffer considerations.
2. Document Linux-first assumptions where they still apply.
3. Explain how to observe drops, send errors, and flow churn.
4. Include example configurations that match the shipped examples.
5. Keep the docs aligned with the implemented metric names and service options.

Provide just enough context for the assignee to pinpoint the code.

- Existing docs area: `docs`
- Existing Prometheus docs: `docs/user_guide/prom.md`
- Implemented UDP deployment guide: `docs/user_guide/udp.md`

## Milestone 4

### [Implemented] Task 4.1

Concise, user-visible summary of the fix

Create the dedicated `pingora-quic` crate and wire it into the workspace behind feature gates.

Step-by-step, self-contained instructions for implementing the change.

1. Add a new workspace crate for QUIC transport support.
2. Keep its dependency boundary aligned with the QUIC integration design note.
3. Add feature gating so QUIC remains optional.
4. Decide the initial public API surface for downstream listener and connector integration.
5. Add compile-only smoke checks for the new crate.

Provide just enough context for the assignee to pinpoint the code.

- Workspace manifest: `Cargo.toml`
- QUIC strategy note: `docs/codex/quic-integration-strategy.md`
- Implemented workspace crate and feature gates: `pingora-quic`, `Cargo.toml`, and `pingora/Cargo.toml`

### [Implemented] Task 4.2

Concise, user-visible summary of the fix

Integrate the chosen QUIC library inside `pingora-quic` without leaking it into the generic UDP layer.

Step-by-step, self-contained instructions for implementing the change.

1. Add the initial `tokio-quiche` integration behind the new crate boundary.
2. Keep the adapter code local to `pingora-quic`.
3. Define the minimum listener and connector abstractions Pingora needs from QUIC.
4. Keep HTTP/3-specific code out of this layer.
5. Add tests or smoke checks that prove the transport adapter builds and initializes.

Provide just enough context for the assignee to pinpoint the code.

- New QUIC crate from Task 4.1
- QUIC strategy note: `docs/codex/quic-integration-strategy.md`
- Implemented `tokio-quiche` adapter boundary: `pingora-quic/src/lib.rs`

### [Implemented] Task 4.3

Concise, user-visible summary of the fix

Add downstream QUIC listener support on top of the UDP transport layer.

Step-by-step, self-contained instructions for implementing the change.

1. Define how QUIC listeners consume UDP packet I/O from the generic transport layer.
2. Add downstream connection acceptance and connection metadata handling.
3. Keep the boundary transport-oriented rather than HTTP-oriented.
4. Surface connection lifecycle events for logging and metrics.
5. Add focused tests for basic downstream QUIC session establishment.

Provide just enough context for the assignee to pinpoint the code.

- UDP service and transport primitives from Milestones 1-3
- `pingora-quic` crate from Milestone 4
- Implemented downstream QUIC listener and session tracking: `pingora-quic/src/lib.rs`

### [Implemented] Task 4.4

Concise, user-visible summary of the fix

Add upstream QUIC connector support for QUIC-backed destinations.

Step-by-step, self-contained instructions for implementing the change.

1. Define how a QUIC upstream destination is represented.
2. Add a connector path for establishing QUIC upstream sessions.
3. Define timeout and error behavior at the transport layer.
4. Keep the connector API independent from HTTP request semantics.
5. Add tests or compile-time smoke checks for upstream connector creation.

Provide just enough context for the assignee to pinpoint the code.

- Existing connector patterns: `pingora-core/src/connectors`
- New QUIC crate from Milestone 4
- Implemented upstream QUIC destination and connector session path: `pingora-quic/src/lib.rs`

### [Implemented] Task 4.5

Concise, user-visible summary of the fix

Add first QUIC observability and transport-level error reporting.

Step-by-step, self-contained instructions for implementing the change.

1. Define lifecycle stats and logs for handshakes, transport failures, and session tracking.
2. Keep them transport-level, not HTTP-level.
3. Surface enough metadata to debug handshake and stream establishment problems.
4. Keep optional QUIC builds healthy without relying on a Prometheus-specific feature gate.
5. Add tests where practical for bookkeeping behavior.

Provide just enough context for the assignee to pinpoint the code.

- `pingora-quic` crate from Milestone 4
- Existing UDP observability patterns in `pingora-core`
- Implemented QUIC transport lifecycle stats and error reporting: `pingora-quic/src/lib.rs`

## Milestone 5

### [Implemented] Task 5.1

Concise, user-visible summary of the fix

Add downstream HTTP/3 session handling on top of the QUIC transport layer.

Step-by-step, self-contained instructions for implementing the change.

1. Introduce an HTTP/3-aware downstream path in `pingora-proxy`.
2. Map accepted QUIC streams into HTTP/3 request handling.
3. Preserve the proxy phase model where possible.
4. Identify and isolate assumptions that are specific to HTTP/1.x or HTTP/2.
5. Add focused tests for a minimal HTTP/3 downstream request lifecycle.

Provide just enough context for the assignee to pinpoint the code.

- Proxy crate: `pingora-proxy`
- QUIC crate from Milestone 4
- Implemented HTTP/3 downstream bridge: `pingora-proxy/src/proxy_h3.rs`

### [Implemented] Task 5.2

Concise, user-visible summary of the fix

Define compatibility behavior for existing filters and phases under HTTP/3.

Step-by-step, self-contained instructions for implementing the change.

1. Audit existing proxy phases and filters for assumptions tied to HTTP/1.x or HTTP/2.
2. Document which filters work unchanged and which need adaptation.
3. Add compatibility shims or explicit unsupported cases where necessary.
4. Add tests that validate representative filters under HTTP/3.
5. Keep the semantics understandable for users writing custom proxy logic.

Provide just enough context for the assignee to pinpoint the code.

- Proxy phase system: `pingora-proxy`
- Existing guide on phases and filters: `docs/user_guide/phase.md`
- Implemented HTTP/3 compatibility bridge and explicit unsupported cases: `pingora-proxy/src/proxy_h3.rs`

### [Implemented] Task 5.3

Concise, user-visible summary of the fix

Add protocol negotiation across HTTP/1.1, HTTP/2, and HTTP/3 for downstream traffic.

Step-by-step, self-contained instructions for implementing the change.

1. Define feature-gated configuration for enabling downstream HTTP/3.
2. Wire ALPN and related protocol advertisement behavior through the server stack.
3. Keep fallback behavior predictable when HTTP/3 is disabled or unavailable.
4. Update response behavior such as `alt-svc` handling when HTTP/3 is enabled.
5. Add tests for negotiation and fallback behavior.

Provide just enough context for the assignee to pinpoint the code.

- Existing examples that strip `alt-svc`: `pingora-proxy/examples/gateway.rs`
- Existing docs mentioning lack of h3: `docs/user_guide/modify_filter.md`
- Implemented HTTP/3 negotiation policy and `alt-svc` advertisement helpers: `pingora-proxy/src/proxy_h3.rs`

## Milestone 6

### [Implemented] Task 6.1

Concise, user-visible summary of the fix

Introduce HTTP/3-capable upstream peer configuration and connector selection.

Step-by-step, self-contained instructions for implementing the change.

1. Define how an upstream can request HTTP/3 transport.
2. Add an upstream selection path that chooses QUIC-backed transport where configured.
3. Keep fallback to HTTP/2 or HTTP/1.1 explicit and configurable.
4. Ensure configuration remains readable rather than overloading the TCP peer model.
5. Add tests that validate upstream transport choice.

Provide just enough context for the assignee to pinpoint the code.

- Existing upstream peer models in `pingora-core`
- Proxy upstream selection in `pingora-proxy`
- Implemented `Http3Peer` and transport selection model: `pingora-core/src/upstreams/peer.rs` and `pingora-proxy/src/proxy_h3.rs`

### [Implemented] Task 6.2

Concise, user-visible summary of the fix

Define connection reuse and lifecycle behavior for HTTP/3 upstream sessions.

Step-by-step, self-contained instructions for implementing the change.

1. Decide what pooling or reuse means for QUIC-backed upstream sessions.
2. Keep the implementation distinct from TCP connection pools where necessary.
3. Define idle timeout and reuse rules for upstream HTTP/3 sessions.
4. Document how reuse interacts with load balancing and failover.
5. Add tests for reuse or session retention behavior.

Provide just enough context for the assignee to pinpoint the code.

- Existing pooling code: `pingora-pool`
- Existing proxy and upstream connection behavior in `pingora-proxy`
- Implemented QUIC upstream session pool, idle reuse, and expiration behavior: `pingora-quic/src/lib.rs`

### [Implemented] Task 6.3

Concise, user-visible summary of the fix

Define retry, failover, and timeout semantics for HTTP/3 upstream requests.

Step-by-step, self-contained instructions for implementing the change.

1. Audit the current retry and failover model for stream-based upstreams.
2. Define equivalent behavior for QUIC-backed upstream requests.
3. Clarify which failures are retryable and when protocol fallback is allowed.
4. Ensure load-balancer decisions remain understandable under transport failure.
5. Add tests for representative retry and failover cases.

Provide just enough context for the assignee to pinpoint the code.

- Existing failover docs: `docs/user_guide/failover.md`
- Existing proxy retry behavior in `pingora-proxy`
- Implemented HTTP/3 retry/failover classifier and policy model: `pingora-proxy/src/proxy_h3.rs`

## Milestone 7

### [Implemented] Task 7.1

Concise, user-visible summary of the fix

Complete the user guide coverage for UDP load balancing and HTTP/3.

Step-by-step, self-contained instructions for implementing the change.

1. Add user guide pages for UDP service setup and UDP load balancing.
2. Add user guide pages for downstream and upstream HTTP/3 support.
3. Document feature gates, limitations, and operational guidance.
4. Cross-link examples and metrics documentation.
5. Keep the docs aligned with the current implementation rather than aspirational behavior.

Provide just enough context for the assignee to pinpoint the code.

- Documentation root: `docs`
- Existing user guide index: `docs/user_guide/index.md`
- Implemented HTTP/3 user guide coverage: `docs/user_guide/http3.md`

### [Implemented] Task 7.2

Concise, user-visible summary of the fix

Add representative examples for UDP load balancers and HTTP/3 proxy deployments.

Step-by-step, self-contained instructions for implementing the change.

1. Add a UDP load-balancer example using multiple backends.
2. Add an HTTP/3-enabled proxy example.
3. Keep examples intentionally small and focused.
4. Validate that example configs match the final public APIs.
5. Update docs to point at the examples.

Provide just enough context for the assignee to pinpoint the code.

- Example directories: `pingora-core/examples`, `pingora/examples`, and `pingora-proxy/examples`
- Implemented HTTP/3 foundation example: `pingora-proxy/examples/http3_proxy.rs`

### [Implemented] Task 7.3

Concise, user-visible summary of the fix

Add benchmarks and interoperability validation for UDP and HTTP/3 paths.

Step-by-step, self-contained instructions for implementing the change.

1. Add focused benchmarks for UDP forwarding and flow-table behavior.
2. Add focused benchmarks for QUIC and HTTP/3 request handling.
3. Add interoperability checks across supported runtime environments and transport backends.
4. Keep the benchmark scope narrow enough to remain maintainable.
5. Document any known gaps that remain out of scope.

Provide just enough context for the assignee to pinpoint the code.

- Existing benchmark directories across the workspace
- New UDP and QUIC crates introduced by earlier milestones
- Implemented focused benches and validation note: `pingora-core/benches/udp_flow_table.rs`, `pingora-quic/benches/upstream_pool.rs`, `pingora-proxy/benches/http3_bridge.rs`, and `docs/codex/udp-http3-validation.md`

### [Implemented] Task 7.4

Concise, user-visible summary of the fix

Review the configuration surface and stabilize the new protocol options.

Step-by-step, self-contained instructions for implementing the change.

1. Audit the new UDP, QUIC, and HTTP/3 configuration knobs added across earlier milestones.
2. Remove or rename options that are too experimental or redundant.
3. Ensure defaults are safe and documented.
4. Align examples and docs with the stabilized configuration model.
5. Record any intentionally unstable areas for future work.

Provide just enough context for the assignee to pinpoint the code.

- Server configuration module: `pingora-core/src/server/configuration`
- Docs and examples added in previous milestones
- Implemented builder-style config helpers and aligned examples/docs: `pingora-core/src/services/datagram.rs`, `pingora-quic/src/lib.rs`, and `pingora-core/src/upstreams/peer.rs`

## Suggested delivery order

1. Task 2.1
2. Task 2.2
3. Task 2.3
4. Task 2.4
5. Task 2.5
6. Task 2.6
7. Task 3.1
8. Task 3.2
9. Task 3.3
10. Task 3.4
11. Task 3.5
12. Task 4.1
13. Task 4.2
14. Task 4.3
15. Task 4.4
16. Task 4.5
17. Task 5.1
18. Task 5.2
19. Task 5.3
20. Task 6.1
21. Task 6.2
22. Task 6.3
23. Task 7.1
24. Task 7.2
25. Task 7.3
26. Task 7.4
