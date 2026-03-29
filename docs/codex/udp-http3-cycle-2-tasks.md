# UDP, QUIC, and HTTP/3 Cycle 2 Task Breakdown

This document turns the second roadmap into concrete implementation tasks for the next cycle:

- Milestone 8: bidirectional UDP forwarding
- Milestone 9: real QUIC transport integration
- Milestone 10: downstream HTTP/3 execution path
- Milestone 11: upstream HTTP/3 execution path
- Milestone 12: interoperability and production closure

## Milestone 8

### [Implemented] Task 8.1

Concise, user-visible summary of the fix

Define the reverse-path routing model for backend-to-client UDP responses.

Step-by-step, self-contained instructions for implementing the change.

1. Decide how a backend response is matched to an existing client-side flow.
2. Define what metadata must be retained when the client datagram is first forwarded.
3. Clarify how listener identity, client address, backend address, and upstream socket identity interact.
4. Document how unknown backend responses are handled.
5. Add a short design note or implementation comment that makes the reverse-path invariants explicit.

Provide just enough context for the assignee to pinpoint the code.

- Current UDP flow tracking: `pingora-core/src/upstreams/udp.rs`
- Current forwarding logic: `pingora-core/src/services/datagram.rs`
- Cycle 2 roadmap: `docs/codex/udp-http3-cycle-2-roadmap.md`
- Implemented reverse-path model with per-flow upstream sockets: `pingora-core/src/services/datagram.rs`

### [Implemented] Task 8.2

Concise, user-visible summary of the fix

Introduce upstream UDP socket management for real forwarding services.

Step-by-step, self-contained instructions for implementing the change.

1. Decide whether upstream forwarding uses a shared socket, per-listener socket set, or per-flow socket ownership.
2. Add the minimal UDP socket abstraction needed for backend sends and receives.
3. Keep this path separate from the downstream listener API.
4. Define shutdown and cleanup behavior for upstream sockets.
5. Add tests for bind, send, and receive behavior on the upstream side.

Provide just enough context for the assignee to pinpoint the code.

- Existing UDP listener primitive: `pingora-core/src/protocols/l4/datagram.rs`
- Existing datagram service path: `pingora-core/src/services/datagram.rs`
- Implemented upstream per-flow socket runtime: `pingora-core/src/services/datagram.rs`

### [Implemented] Task 8.3

Concise, user-visible summary of the fix

Wire backend-to-client UDP response routing into the load balancer.

Step-by-step, self-contained instructions for implementing the change.

1. Extend the UDP load balancer to recognize datagrams arriving from known backends.
2. Resolve the corresponding client flow and route the payload back to the original client.
3. Define what happens when the flow is expired, missing, or remapped.
4. Ensure metrics distinguish client-to-backend forwarding from backend-to-client forwarding.
5. Add tests that validate response routing, unknown backend handling, and expired-flow behavior.

Provide just enough context for the assignee to pinpoint the code.

- UDP load balancer app: `pingora-core/src/services/datagram.rs`
- Flow table and peer set: `pingora-core/src/upstreams/udp.rs`
- Implemented backend-to-client response routing: `pingora-core/src/services/datagram.rs`

### [Implemented] Task 8.4

Concise, user-visible summary of the fix

Make bidirectional UDP flow cleanup and remap behavior correct.

Step-by-step, self-contained instructions for implementing the change.

1. Revisit idle expiration now that both request and response traffic refresh flow state.
2. Define how peer disablement or remap affects reverse-path routing.
3. Ensure stale backend mappings cannot leak responses to the wrong client flow.
4. Add observability for reverse-path drops and response-side flow misses.
5. Add tests that cover expiration, remap, and backend disablement after forward path traffic exists.

Provide just enough context for the assignee to pinpoint the code.

- Current flow lifecycle logic: `pingora-core/src/upstreams/udp.rs`
- Current UDP metrics surface: `pingora-core/src/services/datagram.rs`
- Implemented reverse-path cleanup and socket retirement: `pingora-core/src/upstreams/udp.rs` and `pingora-core/src/services/datagram.rs`

### [Implemented] Task 8.5

Concise, user-visible summary of the fix

Add end-to-end request/response validation for UDP forwarding.

Step-by-step, self-contained instructions for implementing the change.

1. Add an integration-style test that starts a UDP service, a fake backend, and a client.
2. Validate client-to-backend forwarding and backend-to-client response routing.
3. Add at least one failure-mode test for expired or unknown flows.
4. Update the DNS-like and generic UDP examples if the public API changes.
5. Document any remaining operational limitations after bidirectional forwarding lands.

Provide just enough context for the assignee to pinpoint the code.

- Existing UDP examples: `pingora-core/examples/udp_lb.rs` and `pingora-core/examples/udp_dns_like.rs`
- Existing UDP tests in the service layer: `pingora-core/src/services/datagram.rs`
- Implemented end-to-end reverse-path tests and example/docs updates: `pingora-core/src/services/datagram.rs`, `pingora-core/examples/udp_dns_like.rs`, and `docs/user_guide/udp.md`

## Milestone 9

### [Implemented] Task 9.1

Concise, user-visible summary of the fix

Replace the placeholder QUIC adapter assumptions with a real backend integration plan.

Step-by-step, self-contained instructions for implementing the change.

1. Audit the current `NoopQuicTransport` and `TokioQuicheTransport` boundary.
2. Decide which parts remain generic and which parts move into the real backend adapter.
3. Define how downstream UDP packets reach the real QUIC state machine.
4. Define how upstream connector configuration maps onto the backend.
5. Record any backend limitations or feature mismatches up front.

Provide just enough context for the assignee to pinpoint the code.

- Current QUIC crate: `pingora-quic/src/lib.rs`
- QUIC strategy note: `docs/codex/quic-integration-strategy.md`

### [Implemented] Task 9.2

Concise, user-visible summary of the fix

Implement real downstream QUIC listener behavior on the selected backend.

Step-by-step, self-contained instructions for implementing the change.

1. Route incoming UDP datagrams into the backend QUIC listener/session machinery.
2. Surface real handshake acceptance, session reuse, and close/error states.
3. Keep the public Pingora listener API transport-oriented.
4. Ensure metrics and logs reflect real backend session behavior.
5. Add tests that validate handshake success and downstream session lifecycle.

Provide just enough context for the assignee to pinpoint the code.

- Current downstream QUIC listener: `pingora-quic/src/lib.rs`
- Existing UDP datagram primitives: `pingora-core/src/protocols/l4/datagram.rs`

### [Implemented] Task 9.3

Concise, user-visible summary of the fix

Implement real upstream QUIC session establishment on the selected backend.

Step-by-step, self-contained instructions for implementing the change.

1. Replace synthetic establish behavior with backend-driven QUIC connection setup.
2. Ensure connect timeout and idle timeout semantics survive the adapter boundary.
3. Surface negotiated metadata such as ALPN and server name from the real session.
4. Ensure upstream session pooling interacts correctly with real backend handles.
5. Add tests for successful connect, timeout, and pooled reuse.

Provide just enough context for the assignee to pinpoint the code.

- Current connector and pool logic: `pingora-quic/src/lib.rs`
- Current upstream HTTP/3 peer model: `pingora-core/src/upstreams/peer.rs`

### [Implemented] Task 9.4

Concise, user-visible summary of the fix

Surface real QUIC stream lifecycle to higher layers.

Step-by-step, self-contained instructions for implementing the change.

1. Define how accepted streams are represented once the backend is real.
2. Surface stream open, finish, reset, and close/error conditions.
3. Keep the boundary independent from HTTP semantics.
4. Clarify ownership rules between the QUIC crate and the proxy crate.
5. Add tests that validate stream lifecycle transitions.

Provide just enough context for the assignee to pinpoint the code.

- Current downstream bridge assumptions: `pingora-proxy/src/proxy_h3.rs`
- Current QUIC listener/session types: `pingora-quic/src/lib.rs`

### [Implemented] Task 9.5

Concise, user-visible summary of the fix

Add real QUIC integration tests and operational notes.

Step-by-step, self-contained instructions for implementing the change.

1. Add integration coverage for downstream handshake, upstream connect, and basic stream acceptance.
2. Validate failure paths such as timeout, close, or backend error propagation.
3. Update the validation note with what is now real rather than placeholder.
4. Record any platform or backend restrictions.
5. Keep tests focused enough to remain maintainable.

Provide just enough context for the assignee to pinpoint the code.

- Validation note: `docs/codex/udp-http3-validation.md`
- QUIC crate: `pingora-quic`

## Milestone 10

### [Implemented] Task 10.1

Concise, user-visible summary of the fix

Map real QUIC streams into downstream HTTP/3 request handling.

Step-by-step, self-contained instructions for implementing the change.

1. Replace the synthetic accepted-stream assumptions with real stream inputs from `pingora-quic`.
2. Decode the minimum request metadata needed to enter the proxy phase model.
3. Keep the bridge explicit about unsupported HTTP/3 semantics.
4. Ensure request identity remains tied to the QUIC session and stream ID.
5. Add tests that validate request mapping on real stream inputs.

Provide just enough context for the assignee to pinpoint the code.

- Current HTTP/3 bridge: `pingora-proxy/src/proxy_h3.rs`
- Real HTTP/3 request mapping implemented on `IncomingH3Headers`: `pingora-proxy/src/proxy_h3.rs`

### [Implemented] Task 10.2

Concise, user-visible summary of the fix

Add downstream HTTP/3 body and trailer handling.

Step-by-step, self-contained instructions for implementing the change.

1. Define how request body chunks arrive from the QUIC/HTTP3 layer.
2. Integrate body streaming with the existing proxy request-body filters.
3. Define trailer handling for downstream HTTP/3 requests.
4. Ensure backpressure and stream-end semantics remain correct.
5. Add tests for request bodies and trailers.

Provide just enough context for the assignee to pinpoint the code.

- Existing request body handling paths in `pingora-proxy`
- Implemented request-body bridge via `Http3BodyReader`: `pingora-proxy/src/proxy_h3.rs`

### [Implemented] Task 10.3

Concise, user-visible summary of the fix

Add downstream HTTP/3 response writeback on real streams.

Step-by-step, self-contained instructions for implementing the change.

1. Define how response headers and body bytes are written back onto HTTP/3 streams.
2. Support stream completion and error propagation.
3. Handle response trailers if supported by the implementation boundary.
4. Define how downstream write failures are surfaced into proxy error handling.
5. Add tests that validate response bodies and error behavior.

Provide just enough context for the assignee to pinpoint the code.

- Existing downstream response machinery in `pingora-proxy`
- Implemented HTTP/3 response writeback via `Http3ResponseWriter`: `pingora-proxy/src/proxy_h3.rs`

### [Implemented] Task 10.4

Concise, user-visible summary of the fix

Validate phase and filter behavior on real downstream HTTP/3 requests.

Step-by-step, self-contained instructions for implementing the change.

1. Revisit the current compatibility report now that real stream execution exists.
2. Confirm which phases remain transport-agnostic in practice.
3. Make any unsupported semantics explicit rather than silently degraded.
4. Update docs to reflect the real downstream HTTP/3 behavior.
5. Add targeted tests for filter execution under HTTP/3.

Provide just enough context for the assignee to pinpoint the code.

- Compatibility model: `pingora-proxy/src/proxy_h3.rs`
- Updated phase docs: `docs/user_guide/phase.md`

### [Implemented] Task 10.5

Concise, user-visible summary of the fix

Add downstream HTTP/3 integration tests with fallback behavior.

Step-by-step, self-contained instructions for implementing the change.

1. Add tests that exercise HTTP/3 request/response handling through the proxy stack.
2. Validate body handling and error behavior.
3. Validate that fallback to HTTP/2 or HTTP/1.1 remains explicit and predictable.
4. Update examples or negotiation docs if the behavior changed.
5. Record any remaining downstream HTTP/3 gaps.

Provide just enough context for the assignee to pinpoint the code.

- Negotiation model: `pingora-proxy/src/proxy_h3.rs`
- Downstream HTTP/3 tests and docs: `pingora-proxy/src/proxy_h3.rs` and `docs/user_guide/http3.md`

## Milestone 11

### [Implemented] Task 11.1

Concise, user-visible summary of the fix

Map `Http3Peer` and transport selection onto real upstream QUIC sessions.

Step-by-step, self-contained instructions for implementing the change.

1. Connect `Http3Peer` to the real QUIC connector path from Milestone 9.
2. Ensure authority, ALPN, connect timeout, and idle timeout are all consumed by the real path.
3. Keep the stream-based upstream path unchanged for HTTP/1.1 and HTTP/2.
4. Validate upstream transport selection remains readable and explicit.
5. Add tests that validate real connector configuration for HTTP/3 peers.

Provide just enough context for the assignee to pinpoint the code.

- Upstream peer model: `pingora-core/src/upstreams/peer.rs`
- Upstream selection hooks: `pingora-proxy/src/proxy_trait.rs`
- Implemented peer-to-connector mapping and transport selection: `pingora-proxy/src/proxy_h3.rs` and `pingora-quic/src/lib.rs`

### [Implemented] Task 11.2

Concise, user-visible summary of the fix

Implement real HTTP/3 request execution toward upstream origins.

Step-by-step, self-contained instructions for implementing the change.

1. Open real HTTP/3 request streams on pooled or fresh upstream QUIC sessions.
2. Send request headers and request bodies to the origin.
3. Read response headers, response body, and trailers from the origin.
4. Define how stream-end and error conditions map into proxy behavior.
5. Add tests that validate round-trip upstream HTTP/3 request execution.

Provide just enough context for the assignee to pinpoint the code.

- QUIC connector and pool: `pingora-quic/src/lib.rs`
- Implemented upstream HTTP/3 request executor: `pingora-proxy/src/proxy_h3.rs`

### [Implemented] Task 11.3

Concise, user-visible summary of the fix

Integrate real retry, failover, and fallback behavior into the upstream HTTP/3 path.

Step-by-step, self-contained instructions for implementing the change.

1. Connect `Http3RetryClassifier` decisions to the actual upstream request lifecycle.
2. Define which failures occur before request bytes are committed and which occur after.
3. Implement protocol fallback when allowed by policy.
4. Ensure backend remap decisions stay understandable under retry.
5. Add tests that validate retryable connect failures, non-retryable mid-request failures, and configured fallback behavior.

Provide just enough context for the assignee to pinpoint the code.

- Retry classifier: `pingora-proxy/src/proxy_h3.rs`
- Real retry/fallback outcome wiring: `pingora-proxy/src/proxy_h3.rs`

### [Implemented] Task 11.4

Concise, user-visible summary of the fix

Validate pooling and reuse behavior for real upstream HTTP/3 sessions and streams.

Step-by-step, self-contained instructions for implementing the change.

1. Confirm the current QUIC session pool remains correct with real upstream traffic.
2. Define how multiple upstream HTTP/3 streams share a session.
3. Clarify when a session must be evicted rather than reused.
4. Add observability for stream reuse and session retirement where needed.
5. Add tests for reuse, eviction, and fresh-session fallback.

Provide just enough context for the assignee to pinpoint the code.

- Implemented HTTP/3 upstream pool: `pingora-quic/src/lib.rs`
- Current upstream HTTP/3 docs: `docs/user_guide/http3.md`

### [Implemented] Task 11.5

Concise, user-visible summary of the fix

Add end-to-end upstream HTTP/3 proxy integration tests.

Step-by-step, self-contained instructions for implementing the change.

1. Add tests that proxy through Pingora to a real or controlled HTTP/3 origin.
2. Validate request/response headers, bodies, trailers, and failure behavior.
3. Validate protocol fallback when configured.
4. Update validation notes with the achieved coverage.
5. Record any remaining unsupported upstream behaviors explicitly.

Provide just enough context for the assignee to pinpoint the code.

- Validation note: `docs/codex/udp-http3-validation.md`
- Runtime executor tests and docs: `pingora-proxy/src/proxy_h3.rs` and `docs/user_guide/http3.md`

## Milestone 12

### Task 12.1

Concise, user-visible summary of the fix

Add interoperability checks against external HTTP/3 peers.

Step-by-step, self-contained instructions for implementing the change.

1. Select a small interoperability matrix of clients and origins worth validating.
2. Add repeatable checks for downstream and upstream HTTP/3 behavior.
3. Keep the matrix small enough to maintain in CI or manual validation notes.
4. Record exact gaps that remain unsupported.
5. Update docs with the tested support boundary.

Provide just enough context for the assignee to pinpoint the code.

- Validation note: `docs/codex/udp-http3-validation.md`
- HTTP/3 guide: `docs/user_guide/http3.md`

### Task 12.2

Concise, user-visible summary of the fix

Expand benchmark coverage for the now-real transport and proxy paths.

Step-by-step, self-contained instructions for implementing the change.

1. Add or refine benchmarks for bidirectional UDP forwarding.
2. Add or refine benchmarks for real QUIC session lifecycle behavior.
3. Add or refine benchmarks for downstream and upstream HTTP/3 request paths.
4. Keep benchmarks narrow and interpretable.
5. Update validation notes with what the benchmarks actually measure.

Provide just enough context for the assignee to pinpoint the code.

- Existing Cycle 1 benchmarks: `pingora-core/benches`, `pingora-quic/benches`, and `pingora-proxy/benches`
- Validation note: `docs/codex/udp-http3-validation.md`

### Task 12.3

Concise, user-visible summary of the fix

Tighten observability and operational guidance using the real execution path.

Step-by-step, self-contained instructions for implementing the change.

1. Review metrics and logs now that the path is end-to-end.
2. Add missing counters or labels only where they clarify real operations.
3. Update docs for troubleshooting UDP, QUIC, and HTTP/3 request path failures.
4. Keep the metrics surface stable and not overgrown.
5. Add examples or snippets for common debugging workflows.

Provide just enough context for the assignee to pinpoint the code.

- Prometheus docs: `docs/user_guide/prom.md`
- UDP guide: `docs/user_guide/udp.md`
- HTTP/3 guide: `docs/user_guide/http3.md`

### Task 12.4

Concise, user-visible summary of the fix

Stabilize the support boundary and document what is still experimental.

Step-by-step, self-contained instructions for implementing the change.

1. Review the full transport and proxy surface introduced across both cycles.
2. Mark what is supported, experimental, or intentionally unsupported.
3. Remove or rename any configuration that remained too provisional.
4. Update examples and docs to match the stabilized surface.
5. Record the next set of non-goals for any future cycle.

Provide just enough context for the assignee to pinpoint the code.

- Cycle 2 roadmap: `docs/codex/udp-http3-cycle-2-roadmap.md`
- Current docs and examples under `docs` and the workspace example directories

## Suggested delivery order

1. Task 8.1
2. Task 8.2
3. Task 8.3
4. Task 8.4
5. Task 8.5
6. Task 9.1
7. Task 9.2
8. Task 9.3
9. Task 9.4
10. Task 9.5
11. Task 10.1
12. Task 10.2
13. Task 10.3
14. Task 10.4
15. Task 10.5
16. Task 11.1
17. Task 11.2
18. Task 11.3
19. Task 11.4
20. Task 11.5
21. Task 12.1
22. Task 12.2
23. Task 12.3
24. Task 12.4
