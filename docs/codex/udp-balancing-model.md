# UDP Balancing Model Design Note

## Summary

The first UDP load-balancing implementation in Pingora should use a hybrid flow-aware model:

- packets are processed individually
- upstream selection is performed per flow, not per packet
- flow state is explicit, bounded, and expires on idle timeout

This is the best first step because it preserves packet semantics while still providing the routing stability needed by most real UDP protocols and by future QUIC support.

## Evaluated models

### Stateless per-packet forwarding

In this model, each incoming packet is independently balanced to an upstream peer with no retained flow state.

Advantages:

- minimal state management
- simple implementation
- low memory overhead

Disadvantages:

- repeated packets from the same client may hit different upstreams
- response routing becomes awkward for protocols that assume conversational affinity
- unsuitable as a foundation for QUIC, which requires stable endpoint association
- operational behavior becomes harder to reason about under retries or packet reordering

Conclusion:

Useful for a narrow class of fire-and-forget UDP workloads, but too weak as Pingora's first general UDP balancing model.

### Fully stateful per-flow forwarding

In this model, every client flow is tracked and pinned to a chosen upstream until the flow expires.

Advantages:

- stable routing for conversational UDP protocols
- straightforward response mapping
- naturally aligns with future QUIC transport requirements

Disadvantages:

- requires a flow table from the first implementation
- introduces eviction and expiration policy design immediately
- adds memory pressure and lifecycle complexity

Conclusion:

Strong model, but too rigid if taken literally for every UDP workload from day one.

### Hybrid flow-aware forwarding

In this model, packets are still first-class datagrams, but the balancing layer derives a flow key and optionally reuses or creates a flow mapping for routing stability.

Advantages:

- preserves explicit packet handling
- provides stable upstream selection for repeated traffic from the same client flow
- keeps the implementation compatible with future QUIC and HTTP/3 work
- allows bounded, explicit flow state instead of pretending UDP is connection-oriented

Disadvantages:

- more complex than stateless forwarding
- still requires expiration and flow-table sizing decisions

Conclusion:

This is the recommended first model for Pingora.

## Recommended first model

Pingora should implement hybrid flow-aware UDP load balancing first.

That means:

- the transport layer receives and sends individual datagrams
- the balancing layer derives a flow key from packet metadata
- if a live flow mapping exists, it is reused
- otherwise a backend is selected and a new flow mapping is created
- mappings expire after inactivity

This keeps the datagram API honest while giving users predictable routing behavior.

## Flow identity

The first UDP flow key should be derived from:

- listener identity
- client source address and port
- local destination address and port

This is effectively a listener-scoped 4-tuple view of the traffic. It is sufficient for the first implementation because the upstream peer is selected by Pingora, so the inbound flow identity does not need to include the upstream address to find the mapping.

### Why listener identity matters

The same client may send packets to different local UDP listeners or virtual services. Listener identity must therefore participate in the flow key so mappings do not leak across services that share the same process.

### Why the upstream address is not part of the lookup key

On inbound packet receipt, the upstream peer is not known yet. The lookup key must therefore be derivable before backend selection. The mapping value can store the chosen upstream backend separately.

## Affinity policy

For the first implementation, affinity should behave as follows:

- the first packet of a new flow selects a backend using the configured balancing algorithm
- subsequent packets for the same live flow reuse the same backend
- if the mapped backend is no longer available, the flow is reselected and remapped
- if no flow state exists, the packet is treated as the start of a new flow

This gives stable routing while still allowing backend changes when health or availability changes.

## Expiration policy

The first implementation should use idle expiration rather than hard session lifetimes.

Recommended initial behavior:

- each flow entry stores last-seen activity time
- receiving a packet for a flow refreshes its idle timer
- expired entries are removed lazily on lookup and periodically in background cleanup
- the timeout should be configurable, with a conservative default suitable for generic UDP proxying

This is a better fit than fixed lifetimes because UDP workloads vary widely and many protocols only need affinity during bursts of activity.

## Flow table state

The first flow table should store only the minimum metadata needed for correctness:

- flow key
- selected upstream backend identifier
- last activity timestamp
- listener or service context needed for safe response routing

Optional derived metrics such as packet counts can be added later without being required for correctness.

## Behavior when backends change

The first model should define simple and predictable remapping rules:

- healthy existing mappings remain valid while the backend remains enabled
- if a mapped backend is removed or marked unavailable, the next packet for that flow triggers reselection
- remapping should create a fresh activity window

This keeps behavior understandable and aligns with existing Pingora load-balancing expectations.

## Why this model fits future QUIC and HTTP/3

QUIC requires stable routing of packets that belong to the same logical transport conversation.

The proposed hybrid model helps because:

- it already treats packet routing stability as a first-class concern
- it establishes an explicit flow table abstraction
- it avoids encoding fake stream semantics into the UDP layer
- it can later evolve to support richer QUIC-specific connection identifiers without redesigning the basic UDP balancing path

## Acceptance criteria for the first implementation

The first UDP balancing implementation should satisfy these criteria:

- repeated packets from the same live flow are routed to the same backend
- the balancing API still operates on datagrams, not fake connections
- flow mappings expire after configurable inactivity
- backend removal or health changes trigger safe remapping
- the chosen model remains compatible with a future QUIC layer

## Follow-up

The next design task should define the UDP upstream model and decide how much of the current `Peer` abstraction can be reused without leaking stream-only behavior into the datagram path.
