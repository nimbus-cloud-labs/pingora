# UDP Transport Boundary Design Note

## Summary

UDP support in Pingora should be introduced as a separate datagram transport stack, not by extending the existing stream-oriented `Stream` and `Listener` abstractions.

This keeps the architecture honest:

- stream transports keep connection semantics
- datagram transports keep packet semantics
- QUIC and HTTP/3 can later build on a clean UDP foundation instead of inheriting TCP-shaped APIs

## Why UDP must not be added to `Stream`

`pingora_core::protocols::l4::stream::Stream` is fundamentally connection-oriented.

Today it assumes:

- a single accepted connection with a stable peer identity
- `AsyncRead` and `AsyncWrite` semantics
- stream-level shutdown behavior
- socket options such as keepalive and `TCP_NODELAY`
- timing and digest behavior tied to a connection lifecycle

UDP does not fit those assumptions:

- packets are discrete and must preserve message boundaries
- there is no accepted per-client socket in the TCP sense
- read and write operations are address-sensitive
- keepalive, half-close, and stream shutdown semantics do not apply
- a single socket may serve traffic for many client flows concurrently

If UDP were forced into `Stream`, the result would be an API full of exceptions, optional fields, and transport-specific branching. That would make both the TCP path and the future UDP path harder to reason about.

## Why UDP must not be added to `Listener`

`pingora_core::protocols::l4::listener::Listener` is built around `accept()`, which returns a newly accepted stream-like connection.

That model works for TCP and Unix domain sockets because:

- the listener creates new per-connection work items
- peer metadata is attached to a single accepted stream
- downstream service code can run a connection handshake and then process bytes

UDP does not have an `accept()` phase. A UDP socket receives packets directly, and any notion of a "flow" is derived in user space from packet metadata rather than created by the operating system as a connection object.

Adding UDP to `Listener` would force one of two bad outcomes:

- fake accepted connections that do not correspond to kernel transport behavior
- an enum with incompatible execution models hidden behind one type

Both would make the service layer less explicit and harder to extend for QUIC.

## Proposed building blocks

The first UDP implementation should introduce separate datagram primitives beside the existing stream stack.

### Datagram listener

A UDP listener abstraction should:

- own a bound UDP socket
- receive packets from arbitrary remote peers
- send packets to explicit remote addresses
- expose local addressing information needed for forwarding logic

This type should not implement stream traits and should not expose `accept()`.

### Datagram packet metadata

A packet metadata model should carry at least:

- local listener address
- remote peer address
- payload length
- optional receive timestamp or transport digest information where supported

The metadata model should be usable by both UDP load balancing and future QUIC packet handling.

### Flow key

Pingora should define an explicit flow key type for UDP workloads.

The exact contents can be finalized in the next design task, but the type should be designed for:

- affinity decisions
- flow table lookup
- idle expiration
- response routing

The key must be derived from packet metadata, not from a fake accepted connection object.

### Upstream selection hook

UDP load balancing needs a transport-level upstream selection hook that can:

- inspect packet metadata
- inspect the derived flow key
- select an upstream peer
- optionally reuse an existing flow mapping

This hook should remain transport-level and must not assume HTTP semantics.

### Datagram service interface

Pingora needs a datagram service model that fits the current server lifecycle but keeps packet handling explicit.

The first interface should support:

- startup and graceful shutdown through the existing server model
- a receive loop over inbound packets
- packet dispatch to routing or forwarding logic
- explicit send behavior with destination addressing

This service model can live beside the existing listening services rather than replacing them.

## APIs that remain stream-only

The following areas should remain stream-only for now:

- `pingora_core::protocols::l4::stream::Stream`
- `pingora_core::protocols::l4::listener::Listener`
- TLS server handshake flow in `pingora_core::listeners`
- HTTP/1.x and HTTP/2 proxy execution in `pingora-proxy`
- stream connectors and socket tuning paths that assume TCP behavior

Keeping these APIs stream-only avoids premature generalization.

## APIs that may become transport-aware later

Some higher-level areas may eventually need transport-aware extension points:

- service registration and lifecycle integration
- observability and transport metrics
- upstream selection and balancing
- address and socket digest helpers

These should evolve through new sibling APIs or narrow shared traits, not by collapsing stream and datagram types into one abstraction too early.

## Invariants for the UDP path

Any UDP transport implementation in Pingora should preserve these invariants:

1. No fake connection semantics.
2. Packet boundaries remain explicit.
3. Source and destination addresses remain first-class data.
4. No implicit TLS assumptions are embedded in generic UDP transport code.
5. Flow tracking, if added, is explicit user-space state rather than hidden transport behavior.
6. Stream-only socket options and stream-only lifecycle operations are not exposed on datagram types.

## Acceptance criteria for Milestone 1

Milestone 1 should be considered architecturally successful only if all of the following are true:

- a UDP listener type exists without changing the meaning of `Stream`
- UDP receive and send paths do not depend on `AsyncRead` or `AsyncWrite`
- UDP packet handling uses explicit metadata and destination addressing
- the service lifecycle can host UDP processing without pretending packets are accepted connections
- stream listeners, TLS listeners, and HTTP proxying remain readable without UDP-specific branching spread through their core APIs

## Follow-up

The next design steps should define:

- the first UDP flow model
- the UDP upstream peer model
- the boundary between generic UDP transport and future QUIC support
