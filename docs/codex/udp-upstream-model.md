# UDP Upstream Model Design Note

## Summary

Pingora should introduce a UDP-specific upstream peer model instead of trying to reuse the current `Peer` trait directly.

Some concepts from the current peer system remain useful:

- remote address
- optional local bind preferences
- DSCP and similar socket-level hints
- tracing or metadata hooks

However, the existing `Peer` abstraction is fundamentally stream-, TCP-, and TLS-oriented. Reusing it directly for UDP would leak the wrong semantics into the new datagram path.

## Why the current `Peer` abstraction does not fit UDP cleanly

The current `Peer` model in `pingora-core/src/upstreams/peer.rs` assumes a connection establishment workflow.

It includes stream-specific or TLS-specific concerns such as:

- `tls()`
- `sni()`
- certificate validation
- ALPN
- connection timeout and total connection timeout
- idle timeout for reusable connections
- TCP keepalive
- TCP receive buffer tuning
- TCP fast open
- proxy connect behavior
- connection reuse hashing

These concepts are sensible for TCP and HTTP upstreams, but they do not describe a UDP destination accurately.

If UDP peers were forced into the same trait, one of two things would happen:

- most methods would become meaningless no-ops
- the trait would grow transport-specific caveats that make it harder to understand and maintain

Neither outcome is desirable.

## Recommended model

Introduce a UDP-specific peer type and, if needed, a UDP-specific peer trait.

Recommended shape:

- `UdpPeer` for a concrete upstream destination plus UDP-specific options
- optional `UdpPeerOptions` for transport hints and policy
- a narrow `UdpUpstream` trait only if polymorphism is actually needed during implementation

This should exist beside the current `Peer` model rather than beneath it.

## What should be reused from the current model

The following concepts are still useful and should inform the UDP model:

### Remote address

A UDP upstream still needs a destination address.

The existing `SocketAddr` type is reusable for this purpose, with the caveat that the first UDP implementation should likely support only Internet UDP sockets and not Unix datagram sockets unless there is a concrete need.

### Local bind intent

The idea behind `BindTo` remains valid for UDP:

- selecting a source address
- potentially selecting a source port strategy

The implementation details may differ, but the concept itself is reusable.

### DSCP and packet-level socket hints

Transport-level options such as DSCP still make sense for UDP sockets and should remain available where supported.

### Tracing and opaque metadata

Hooks for observability or lightweight annotations remain useful, as long as they are not tied to connection-establishment events.

## What should not be reused directly

The following parts of `Peer` should not be carried into the first UDP upstream model:

- TLS and SNI configuration
- ALPN
- certificate and hostname verification
- reusable stream connection hashes
- idle timeout semantics for pooled connections
- TCP keepalive
- TCP fast open
- upstream TCP socket tweak hooks
- CONNECT proxy behavior
- connection reuse and FD matching behavior

These features belong to the stream/TLS side of Pingora.

## Proposed `UdpPeer` responsibilities

The first UDP upstream type should cover only what the datagram path needs.

Recommended responsibilities:

- destination socket address
- optional local bind preferences
- optional DSCP or socket buffer preferences, where supported
- optional weight or backend identity metadata for balancing
- optional opaque extensions for user metadata

The type should not imply a connected socket or a persistent session.

## Health and availability model

UDP upstream health should be tracked at the backend level, not inferred from transport connection state.

For the first implementation:

- the load-balancing layer should treat UDP backends as enabled or disabled
- health may initially be driven by external configuration or simple active checks
- transport send success alone should not be treated as proof of backend health

This keeps the model aligned with how UDP actually behaves.

## Temporary failure handling

The UDP path should support temporary backend unavailability without pretending that connections failed.

Recommended first behavior:

- when a backend is marked unavailable, new flow selection should skip it
- an existing flow mapped to an unavailable backend should be remapped on the next packet
- packet send errors may contribute to error metrics or future health heuristics, but should not automatically redefine transport semantics

## Address family support

The first UDP upstream implementation should support IPv4 and IPv6 Internet sockets.

Unix datagram sockets should be considered optional future work unless they become necessary for a concrete Pingora use case.

This keeps the first release focused and avoids multiplying transport variants before the main UDP path is stable.

## Relationship to the load balancer

The UDP peer model should integrate cleanly with Pingora's existing load-balancing concepts, but not by pretending a UDP backend is the same thing as an HTTP/TCP upstream.

A good first direction is:

- reuse backend discovery and weighting ideas
- allow a UDP backend set to feed UDP-specific selection logic
- keep the actual send path and flow mapping UDP-specific

This approach captures the reusable parts of the existing ecosystem without inheriting stream-only assumptions.

## Relationship to future QUIC and HTTP/3 support

This model helps future QUIC work because:

- QUIC still needs remote UDP destinations
- the UDP peer model can supply socket and routing policy without embedding HTTP semantics
- QUIC-specific transport state can live above `UdpPeer` instead of inside the generic UDP balancing layer

That separation keeps the generic UDP path useful for non-QUIC workloads while still making it a suitable substrate for HTTP/3 later.

## Recommendation

For the first implementation:

- create a dedicated UDP upstream peer type
- reuse only the address and a small subset of transport hints from the current peer model
- do not make `UdpPeer` implement the existing `Peer` trait
- keep stream and datagram upstream semantics separate until there is a clear, narrow shared abstraction worth extracting

## Follow-up

The next design task should select the QUIC integration strategy and define the crate and API boundary between generic UDP transport, QUIC transport, and HTTP/3 proxy logic.
