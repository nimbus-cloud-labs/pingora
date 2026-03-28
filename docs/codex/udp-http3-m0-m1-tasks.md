# UDP and HTTP/3 Early Milestones Task Breakdown

This document turns the roadmap into concrete implementation tasks for the first two milestones:

- Milestone 0: design and constraints
- Milestone 1: core UDP transport support

## Milestone 0

### [Implemented] Task 0.1

Concise, user-visible summary of the fix

Define the datagram transport architecture and keep it separate from the existing stream-based transport path.

Step-by-step, self-contained instructions for implementing the change.

1. Write a short design note that explains why UDP must not be added to `pingora_core::protocols::l4::stream::Stream` or `pingora_core::protocols::l4::listener::Listener`.
2. Define the proposed datagram building blocks: listener, packet metadata, flow key, upstream selection hook, and service interface.
3. Document which existing APIs remain stream-only and which new APIs will become transport-agnostic later, if ever.
4. Record the invariants that the UDP path must preserve: no fake connection semantics, explicit packet boundaries, and no implicit TLS assumptions.
5. Add acceptance criteria for Milestone 1 so future code review can validate whether the transport boundary stayed clean.

Provide just enough context for the assignee to pinpoint the code.

- Existing stream listener abstraction: `pingora-core/src/protocols/l4/listener.rs`
- Existing stream wrapper: `pingora-core/src/protocols/l4/stream.rs`
- Current listener stack assembly: `pingora-core/src/listeners/mod.rs`
- Design note produced: `docs/codex/udp-transport-boundary.md`

### [Implemented] Task 0.2

Concise, user-visible summary of the fix

Choose the first UDP balancing model and make the tradeoffs explicit before code lands.

Step-by-step, self-contained instructions for implementing the change.

1. Evaluate three models for the first implementation: stateless forwarding, per-flow tracking, and hybrid flow-aware forwarding.
2. Pick the initial model and document why it is the best fit for Pingora's first UDP release.
3. Define what identifies a UDP flow in Pingora, such as 4-tuple or 5-tuple plus listener context.
4. Define how affinity should behave for repeat datagrams from the same client.
5. Define how flow expiration should work and what metadata must be tracked.

Provide just enough context for the assignee to pinpoint the code.

- Existing load balancing crate for reference patterns: `pingora-load-balancing`
- Existing upstream peer model: `pingora-core/src/upstreams/peer.rs`
- Design note produced: `docs/codex/udp-balancing-model.md`

### [Implemented] Task 0.3

Concise, user-visible summary of the fix

Define the UDP upstream model and decide how much of the current peer API can be reused cleanly.

Step-by-step, self-contained instructions for implementing the change.

1. Review `Peer` and related upstream configuration to identify stream-specific assumptions.
2. List which fields and hooks can be reused for UDP peers and which ones require a new UDP-specific peer type.
3. Decide whether UDP support should introduce a parallel peer trait, a sibling config type, or a layered extension on top of the current peer model.
4. Document how health status, temporary failures, and address families should work for UDP upstreams.
5. Add a migration note describing how HTTP/3 and QUIC will consume this model later.

Provide just enough context for the assignee to pinpoint the code.

- Upstream peer API: `pingora-core/src/upstreams/peer.rs`
- Existing L4 connector logic: `pingora-core/src/connectors/l4.rs`
- Design note produced: `docs/codex/udp-upstream-model.md`

### [Implemented] Task 0.4

Concise, user-visible summary of the fix

Select the QUIC integration strategy early enough to avoid refactoring the UDP layer twice.

Step-by-step, self-contained instructions for implementing the change.

1. Decide whether QUIC support should live inside an existing crate such as `pingora-core` or in a dedicated new crate.
2. Document how the future QUIC layer will consume UDP packet I/O without forcing QUIC-specific types into the generic UDP balancing path.
3. Define the expected boundary between UDP transport, QUIC transport, and HTTP/3 proxy logic.
4. Document any TLS backend constraints that will affect QUIC later, even if they are not implemented in Milestone 1.
5. Add a short note on testing implications so the first UDP APIs are shaped with QUIC in mind.

Provide just enough context for the assignee to pinpoint the code.

- Existing TLS and protocol boundaries: `pingora-core/src/protocols`
- Existing connector and listener stack: `pingora-core/src/connectors` and `pingora-core/src/listeners`
- Design note produced: `docs/codex/quic-integration-strategy.md`

## Milestone 1

### [Implemented] Task 1.1

Concise, user-visible summary of the fix

Introduce a UDP listener abstraction alongside the current TCP and Unix listener path.

Step-by-step, self-contained instructions for implementing the change.

1. Add a datagram listener type under `pingora_core::protocols::l4` that wraps `tokio::net::UdpSocket` or an equivalent socket abstraction.
2. Keep it separate from the existing `Listener` enum so stream and datagram semantics do not leak into each other.
3. Expose the minimal operations needed for packet receive and send.
4. Add socket metadata capture needed by a proxy or load balancer, including local and remote addresses.
5. Add unit tests that prove the listener can bind, receive a packet, and send a response.

Provide just enough context for the assignee to pinpoint the code.

- Current stream listener enum: `pingora-core/src/protocols/l4/listener.rs`
- Current listener endpoint machinery: `pingora-core/src/listeners/l4.rs`
- Implemented UDP listener module: `pingora-core/src/protocols/l4/datagram.rs`

### [Implemented] Task 1.2

Concise, user-visible summary of the fix

Create a UDP packet and metadata model that Pingora services can process without pretending packets are streams.

Step-by-step, self-contained instructions for implementing the change.

1. Add a datagram type that contains payload bytes plus source and destination socket metadata.
2. Define helper types for packet buffers, peer addressing, and flow lookup keys.
3. Ensure the datagram API keeps packet boundaries explicit and does not expose stream-like read or write semantics.
4. Decide where buffer ownership lives so high-throughput forwarding paths can avoid unnecessary copies later.
5. Add tests that validate metadata extraction and flow key derivation.

Provide just enough context for the assignee to pinpoint the code.

- Existing socket and digest utilities: `pingora-core/src/protocols/l4/socket.rs`
- Existing protocol digest types: `pingora-core/src/protocols/digest.rs`
- Implemented datagram model: `pingora-core/src/protocols/l4/datagram.rs`

### [Implemented] Task 1.3

Concise, user-visible summary of the fix

Add a datagram service model so UDP workloads can run inside the existing server lifecycle.

Step-by-step, self-contained instructions for implementing the change.

1. Design a service API for UDP that fits Pingora's service startup and shutdown model.
2. Reuse existing service dependency and readiness concepts where they apply, but avoid forcing HTTP-specific assumptions into the interface.
3. Define the receive loop contract, packet dispatch contract, and shutdown behavior.
4. Integrate the datagram service with server startup so a UDP service can be registered and run beside existing listening services.
5. Add a minimal example service that echoes or forwards UDP packets.

Provide just enough context for the assignee to pinpoint the code.

- Core service traits and lifecycle: `pingora-core/src/services/mod.rs`
- Existing listening service support: `pingora-core/src/services/listening.rs`
- Server assembly: `pingora-core/src/server/mod.rs`
- Implemented datagram service module: `pingora-core/src/services/datagram.rs`
- Example UDP service: `pingora-core/examples/udp_echo.rs`

### [Implemented] Task 1.4

Concise, user-visible summary of the fix

Add basic observability for UDP transport events from the first code drop.

Step-by-step, self-contained instructions for implementing the change.

1. Define counters or logging points for received packets, sent packets, receive errors, send errors, and dropped packets.
2. Add transport-level debug logging that helps trace listener bind, packet receive, route decision, and response send paths.
3. Ensure observability hooks are transport-oriented and do not assume HTTP request semantics.
4. Keep the metrics surface small at first so it can evolve without churn.
5. Add tests where practical and document any gaps that need integration coverage later.

Provide just enough context for the assignee to pinpoint the code.

- Existing Prometheus-related service docs and code patterns: `docs/user_guide/prom.md`
- Listener and service code paths to instrument: `pingora-core/src/listeners` and `pingora-core/src/services`
- Implemented UDP service observability: `pingora-core/src/services/datagram.rs`

### [Implemented] Task 1.5

Concise, user-visible summary of the fix

Add the first end-to-end UDP transport example and validation tests.

Step-by-step, self-contained instructions for implementing the change.

1. Create a small example that binds a UDP endpoint and forwards or echoes packets through the new datagram service path.
2. Add integration-style tests that exercise bind, receive, dispatch, send, and shutdown behavior.
3. Verify the example and tests cover both success and basic error cases.
4. Document any platform-specific assumptions such as Linux-first behavior.
5. Update the roadmap or milestone notes if implementation feedback changes the next steps.

Provide just enough context for the assignee to pinpoint the code.

- Existing examples layout for reference: `pingora/examples` and `pingora-proxy/examples`
- Existing tests around listener and service behavior: `pingora-core/src/listeners` and `pingora-core/src/services`
- Example UDP service: `pingora-core/examples/udp_echo.rs`
- End-to-end datagram service test: `pingora-core/src/services/datagram.rs`

## Suggested delivery order

1. Task 0.1
2. Task 0.2
3. Task 0.3
4. Task 0.4
5. Task 1.1
6. Task 1.2
7. Task 1.3
8. Task 1.4
9. Task 1.5
