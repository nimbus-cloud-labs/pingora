# QUIC Integration Strategy Design Note

## Summary

QUIC support should be introduced in a dedicated crate rather than implemented directly inside `pingora-core` or `pingora-proxy`.

Recommended direction:

- generic UDP transport support lives in `pingora-core`
- QUIC transport logic lives in a dedicated crate
- HTTP/3 support lives above QUIC in the proxy layer

This keeps transport responsibilities separated and avoids mixing packet I/O, QUIC state machines, and HTTP proxy semantics in the same crate.

## Recommendation

Introduce QUIC in a new workspace crate, tentatively named `pingora-quic`.

That crate should:

- depend on the generic UDP transport facilities exposed by `pingora-core`
- expose QUIC listener and connector integration points
- own QUIC-specific transport state and lifecycle
- avoid embedding HTTP/3 proxy behavior directly

`pingora-proxy` can then add HTTP/3 support on top of `pingora-quic`.

For the initial implementation, `pingora-quic` should prefer integrating `tokio-quiche` rather than reimplementing QUIC on top of Pingora's own UDP primitives.

The internal crate boundary should still remain narrow enough that Pingora can switch to lower-level `quiche` integration later if `tokio-quiche` proves too opinionated for Pingora's needs.

## Library choice

### Preferred initial choice: `tokio-quiche`

`tokio-quiche` is the best first implementation choice for Pingora's QUIC layer.

Reasons:

- it is built on top of `quiche`, so it inherits a mature QUIC core rather than introducing a new transport implementation
- it already integrates with Tokio, which matches Pingora's async runtime model closely
- Cloudflare describes it as battle-tested and already used for high-volume HTTP/3 production workloads
- it should reduce time-to-first-working-HTTP/3 compared with building and validating a custom QUIC runtime integration from scratch

Recommended use:

- keep `tokio-quiche` contained inside `pingora-quic`
- do not let `tokio-quiche` define Pingora's generic UDP transport model
- adapt it behind Pingora-owned traits or module boundaries where practical

### Secondary option: direct `quiche` integration

`quiche` remains the fallback option if Pingora needs lower-level control than `tokio-quiche` exposes.

Reasons to prefer direct `quiche` later might include:

- tighter integration with Pingora's UDP service model
- custom timer or event-loop behavior
- finer control over packet I/O and scheduling
- requirements that are awkward to express through the Tokio-oriented wrapper layer

However, that should be treated as a second-stage optimization or escape hatch, not the first implementation strategy.

### Not recommended: reimplement QUIC in Pingora

Pingora should not attempt to implement QUIC itself on top of raw UDP primitives.

That would require independently building and validating:

- handshake behavior
- loss recovery and congestion control
- packet protection
- stream lifecycle management
- transport parameters
- connection migration and identifier handling
- HTTP/3-facing transport semantics

The cost and risk are too high relative to the value, especially when mature Cloudflare-maintained QUIC implementations already exist.

## Why a dedicated crate is the right boundary

### Why not put QUIC entirely in `pingora-core`

`pingora-core` is already the home for foundational transport and protocol abstractions. It should own generic UDP listener and datagram primitives.

However, QUIC is not a thin extension of UDP. It introduces:

- handshake state
- encryption state
- multiplexed streams
- flow control
- transport parameters
- connection identifiers
- QUIC-specific timers and recovery behavior

Putting all of that directly into `pingora-core` would blur the line between generic transport foundations and a specific higher-level transport protocol.

### Why not put QUIC directly in `pingora-proxy`

QUIC is not just an HTTP/3 detail.

HTTP/3 depends on QUIC, but QUIC itself is a transport layer with its own lifecycle and operational concerns. If QUIC lived only inside `pingora-proxy`, the repository would end up coupling HTTP semantics too tightly to transport concerns that should remain reusable and independently testable.

## Proposed crate boundary

### `pingora-core`

`pingora-core` should own:

- UDP listener abstractions
- datagram packet and metadata types
- UDP service lifecycle integration
- UDP upstream peer definitions or generic transport-facing peer traits
- transport-level observability hooks

`pingora-core` should not own:

- QUIC stream state machines
- QUIC connection lifecycle logic
- HTTP/3 request/response translation

### `pingora-quic`

`pingora-quic` should own:

- downstream QUIC listener integration
- upstream QUIC connector integration
- QUIC connection objects
- stream open and accept APIs
- transport parameter handling
- QUIC-specific timers, state transitions, and connection identifiers
- QUIC observability primitives

`pingora-quic` should expose APIs that are transport-oriented, not HTTP-specific.

### `pingora-proxy`

`pingora-proxy` should own:

- HTTP/3 request and response handling
- mapping HTTP/3 exchanges into Pingora's proxy phases and filters
- downstream protocol negotiation behavior
- upstream HTTP/3 peer behavior and fallback policies
- `alt-svc` behavior once HTTP/3 is supported

## Dependency direction

The intended dependency direction should be:

- `pingora-core` -> generic transport foundations
- `pingora-quic` -> depends on `pingora-core`
- `pingora-proxy` -> depends on `pingora-core` and optionally `pingora-quic`
- `pingora` -> re-exports or feature-gates the public surface as needed

This avoids circularity and keeps HTTP/3 optional until the feature is mature.

## API boundary between UDP and QUIC

The UDP layer should expose only what QUIC actually needs from packet transport.

That means:

- bind and receive datagrams
- send datagrams to explicit remote addresses
- surface local and remote packet metadata
- provide enough control for socket options and listener lifecycle

The UDP layer should not:

- know about QUIC streams
- know about connection identifiers
- know about TLS handshakes
- know about HTTP/3

QUIC should consume UDP packet I/O as a transport substrate, not by extending the UDP layer with QUIC-specific concepts.

## API boundary between QUIC and HTTP/3

The QUIC layer should expose:

- connection establishment
- stream accept and open operations
- connection metadata
- transport errors
- shutdown and timeout behavior

The QUIC layer should not expose:

- HTTP request parsing rules
- header translation logic
- proxy filter semantics
- gateway-specific routing behavior

Those belong in `pingora-proxy`.

## TLS backend implications

QUIC requires TLS 1.3 semantics integrated into the QUIC stack, which is materially different from the stream TLS backends already used by Pingora for TCP.

Implications for the design:

- existing stream TLS crates should not be assumed to drop into QUIC unchanged
- `tokio-quiche` and `quiche` bring their own QUIC/TLS integration expectations, so Pingora should treat QUIC TLS compatibility as a separate feature matrix from existing stream TLS support
- QUIC support may initially be constrained to a subset of Pingora's existing TLS feature matrix
- the QUIC crate should own compatibility decisions and feature gating rather than forcing them into generic UDP transport code

This is another reason to keep QUIC in its own crate.

## Testing implications

The chosen boundary should make testing incremental:

- `pingora-core` tests generic UDP transport behavior
- `pingora-quic` tests QUIC connection and stream behavior independently of HTTP
- `pingora-proxy` tests HTTP/3 proxy semantics on top of QUIC

This reduces integration risk and keeps failures easier to localize.

## Initial feature-gating recommendation

QUIC and HTTP/3 should both be optional at first.

Suggested shape:

- a new `quic` feature for the QUIC transport crate and any dependent integration points
- an `http3` feature in higher-level crates that depends on QUIC support

That allows Pingora to land UDP support without committing the whole workspace to QUIC immediately.

## Migration path

The expected order of implementation should be:

1. generic UDP support in `pingora-core`
2. dedicated `pingora-quic` crate
3. downstream QUIC support
4. downstream HTTP/3 in `pingora-proxy`
5. upstream QUIC and HTTP/3 support

This order keeps each layer focused and avoids refactoring the same boundaries repeatedly.

## Recommendation

For the first QUIC design milestone:

- plan for a dedicated `pingora-quic` crate
- prefer `tokio-quiche` as the initial QUIC implementation inside that crate
- keep the internal adapter boundary narrow enough to fall back to direct `quiche` integration if needed
- keep UDP transport generic and QUIC-free in `pingora-core`
- keep HTTP/3 logic out of the QUIC crate
- let `pingora-proxy` consume QUIC as a transport capability rather than owning QUIC itself
