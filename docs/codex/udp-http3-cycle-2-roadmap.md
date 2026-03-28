# UDP, QUIC, and HTTP/3 Cycle 2 Roadmap

## Purpose

The first roadmap is complete. It established:

- UDP transport and service foundations
- UDP upstream selection and flow tracking
- operational hardening for the first UDP path
- a QUIC transport boundary
- downstream and upstream HTTP/3 foundation types
- documentation, examples, and focused validation

This second cycle is about closing the gap between those foundations and a
fully usable end-to-end transport and proxy stack.

## What is done already

The completed work from Cycle 1 should now be treated as stable foundation:

- UDP listeners, datagram model, and UDP service lifecycle
- UDP upstream peer model, peer selection, and bounded flow table
- first QUIC listener/connector abstractions and upstream session pool
- downstream HTTP/3 bridge and upstream HTTP/3 peer selection
- initial retry/failover classification for HTTP/3

Cycle 2 should build on those pieces instead of redesigning them.

## Remaining gaps

The current code still has four major gaps:

1. UDP forwarding is not yet bidirectional in the general case.
2. QUIC transport is not yet backed by a real operational adapter path.
3. Downstream HTTP/3 is bridged into request state, but not yet wired as a full
   request/response proxy path.
4. Upstream HTTP/3 has peer and lifecycle modeling, but not full request/response
   execution against real origins.

## Delivery order

The recommended order remains transport-first:

1. finish UDP request/response forwarding
2. make QUIC transport real
3. make downstream HTTP/3 real
4. make upstream HTTP/3 real

This avoids implementing HTTP/3 semantics on top of placeholder transport paths.

## Milestone 8: Bidirectional UDP forwarding

Outcome:
Complete the generic UDP forwarding path so Pingora can behave like a real UDP
load balancer rather than only a request-side router.

Features:

- add backend-to-client response routing
- introduce upstream UDP socket management appropriate for forwarding services
- define how backend replies map back onto tracked client flows
- make cleanup and idle expiration correct for both directions
- add end-to-end tests for request/response datagram forwarding

Expected result:

- a UDP service can receive datagrams from a client, forward them upstream, and
  route the corresponding backend responses back to the original client

## Milestone 9: Real QUIC transport integration

Outcome:
Replace the placeholder transport assumptions with a real QUIC adapter path.

Features:

- wire `pingora-quic` to a real backend, starting with `tokio-quiche`
- implement downstream handshake and session tracking on real QUIC connections
- implement upstream QUIC session establishment on the real adapter
- define stream acceptance, session close, and error propagation behavior
- add integration tests that validate QUIC handshake and stream lifecycle

Expected result:

- `pingora-quic` owns real transport behavior rather than only abstract boundaries

## Milestone 10: Downstream HTTP/3 execution path

Outcome:
Turn the downstream HTTP/3 bridge into a real request/response proxy path.

Features:

- map real QUIC streams into HTTP/3 request handling
- integrate body streaming, trailers, stream resets, and response writeback
- define how existing phases and filters execute for real HTTP/3 streams
- keep HTTP/1.1 and HTTP/2 fallback behavior explicit and testable
- add downstream HTTP/3 integration tests with request and response bodies

Expected result:

- Pingora can accept real HTTP/3 requests and serve them through the proxy phase model

## Milestone 11: Upstream HTTP/3 execution path

Outcome:
Turn upstream HTTP/3 selection and pooling into real origin communication.

Features:

- open real upstream QUIC sessions using `Http3Peer`
- create HTTP/3 request streams toward upstream origins
- support request body send, response header read, response body read, and trailers
- connect retry/failover policy to the real upstream request lifecycle
- validate fallback to HTTP/2 or HTTP/1.1 where configured

Expected result:

- Pingora can proxy to a real HTTP/3 upstream, not only model one

## Milestone 12: Interoperability and production closure

Outcome:
Harden the end-to-end path against real environments and external peers.

Features:

- interoperability checks against real HTTP/3 servers and clients
- benchmark request path behavior for UDP forwarding, QUIC session lifecycle, and HTTP/3 stream handling
- tighten observability and operational docs using real execution data
- record unsupported extensions and production caveats explicitly
- decide what is ready to be documented as supported versus still experimental

Expected result:

- the new transport stack is no longer just internally coherent, but externally validated

## Acceptance criteria for Cycle 2

Cycle 2 should be considered complete when all of the following are true:

- UDP forwarding works in both directions
- QUIC transport runs on a real backend path
- downstream HTTP/3 can serve real request and response traffic
- upstream HTTP/3 can talk to real origins
- fallback, retry, and timeout behavior are exercised by integration tests
- docs describe the real support boundary instead of the planned one

## Scope guardrails

Cycle 2 should still avoid unnecessary expansion.

Keep out of scope unless they become required by implementation:

- full QUIC connection migration support
- every HTTP/3 extension frame and optional feature
- multi-backend QUIC abstraction work beyond what Pingora actually needs
- protocol-generic datagram platform work unrelated to Pingora's proxy/runtime goals
