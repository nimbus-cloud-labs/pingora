# UDP Load Balancing and HTTP/3 Roadmap

## Purpose

This document describes a practical plan to add two major capabilities that Pingora does not support today:

- UDP load balancing
- HTTP/3 support

The intent is to define an implementation direction that fits the current Pingora architecture instead of layering these features on top of HTTP/1 and HTTP/2 abstractions that were designed around stream-oriented transports.

## Current state

Pingora is currently built around stream transports:

- listeners accept TCP and Unix domain socket connections
- L4 streams wrap `TcpStream` and `UnixStream`
- HTTP proxying is implemented for HTTP/1.x and HTTP/2
- examples and documentation explicitly remove `alt-svc` because HTTP/3 is not supported yet

This has one important consequence: HTTP/3 is not an isolated feature. It depends on QUIC, and QUIC depends on a first-class UDP transport model.

## Goal

Add UDP load balancing first, then use that transport foundation to introduce QUIC and HTTP/3 in incremental, testable stages.

This ordering keeps the changes coherent:

- UDP support becomes useful on its own
- QUIC is built on a transport model that already exists in the codebase
- HTTP/3 can reuse the QUIC foundation instead of bypassing the existing listener and service layers

## Non-goals for the first phase

The initial phases should not attempt to solve every transport problem at once.

Out of scope for the first iterations:

- generic datagram APIs for every protocol family
- full transparent L4 proxying for arbitrary UDP protocols
- connection migration support across all deployment modes
- a complete replacement of the existing TCP listener and stream model
- every possible HTTP/3 extension and optimization from day one

## Architectural overview

### 1. Keep stream and datagram transports separate

Pingora's current abstractions are strongly stream-based. Trying to fold UDP into the existing `Stream` and `Listener` types would create awkward APIs and spread protocol-specific branching across the codebase.

The better direction is to add a separate datagram path:

- UDP listener abstraction
- UDP socket/session abstraction
- UDP service entrypoints
- UDP upstream selection and balancing primitives

This allows the existing TCP, TLS, HTTP/1, and HTTP/2 layers to remain stable while a new transport family is introduced beside them.

### 2. Treat UDP load balancing as the transport foundation

UDP load balancing should be delivered as a standalone capability before QUIC and HTTP/3.

That work should establish:

- how inbound datagrams are received
- how client identity is defined for stickiness and balancing
- how upstream peers are selected
- how response datagrams are mapped back to the original client
- how timeout, connection tracking, and telemetry behave for datagram traffic

### 3. Build QUIC as a transport subsystem, not as an HTTP feature

QUIC should sit between the UDP foundation and HTTP/3:

- UDP provides packet I/O
- QUIC provides connection lifecycle, encryption, streams, and transport behavior
- HTTP/3 runs on QUIC streams

This layering reduces coupling and makes it possible to expose QUIC support later to non-HTTP use cases if desired.

### 4. Add HTTP/3 through the proxy layer only after QUIC is stable

Once QUIC primitives exist, the proxy layer can grow HTTP/3 support:

- downstream HTTP/3 termination
- upstream HTTP/3 connections
- ALPN and protocol negotiation
- graceful fallback to HTTP/2 and HTTP/1.1
- correct `alt-svc` behavior

At that point Pingora can become a multi-protocol edge and proxy platform rather than a TCP-only HTTP proxy framework.

## Capability roadmap

### Milestone 0: Design and constraints

Outcome:
Define the target architecture, choose the transport boundaries, and avoid early design debt.

Features:

- document the separation between stream and datagram transport stacks
- choose whether UDP load balancing is initially stateless, flow-aware, or hybrid
- define the upstream peer model for UDP workloads
- decide how health checks apply to UDP peers
- choose the QUIC implementation strategy and crate boundary

Notes:

- This milestone should produce design notes, trait sketches, and acceptance criteria.
- No user-facing protocol support is required yet.

### Milestone 1: Core UDP transport support

Outcome:
Introduce first-class UDP transport primitives to Pingora.

Features:

- add a UDP listener abstraction beside the existing TCP and Unix listeners
- add UDP socket wrappers and metadata extraction
- capture source and destination addressing information needed for proxying
- define a datagram service model for receive, route, send, and timeout handling
- add metrics and logging hooks for datagram traffic

Risks addressed:

- avoids contaminating stream-only APIs with datagram semantics
- provides the minimum transport layer needed by both UDP balancing and QUIC

### Milestone 2: UDP upstreams and load balancing

Outcome:
Deliver usable UDP proxy and load-balancing support.

Features:

- define UDP upstream peer types and balancing interfaces
- implement baseline balancing strategies such as round robin and hash-based selection
- add optional client affinity based on 5-tuple or reduced flow keys
- implement per-flow state with expiration
- support upstream health and temporary failure handling
- add examples for DNS-like and generic UDP forwarding scenarios

Expected result:

- Pingora can receive UDP datagrams and forward them to one of several upstream peers with predictable balancing behavior

### Milestone 3: Operational hardening for UDP

Outcome:
Make UDP balancing production-usable.

Features:

- better observability for packet drops, flow expiration, and upstream errors
- backpressure and buffering policies
- configurable idle timeouts and state table limits
- safe handling of oversized packets and fragmentation-related edge cases
- documentation for deployment expectations and kernel/socket tuning

Expected result:

- UDP balancing is stable enough to stand on its own as a supported feature set

### Milestone 4: QUIC transport foundation

Outcome:
Introduce QUIC as a transport subsystem built on the UDP layer.

Features:

- downstream QUIC listener integration
- upstream QUIC connector integration
- TLS 1.3 and ALPN support required by QUIC
- QUIC connection lifecycle management
- stream multiplexing and flow control integration
- basic observability for handshakes, connection state, and transport errors

Notes:

- This milestone should stop at transport readiness.
- HTTP semantics should remain out of scope until QUIC is stable.

### Milestone 5: Downstream HTTP/3 support

Outcome:
Allow Pingora to accept HTTP/3 traffic from clients.

Features:

- map HTTP/3 requests into the existing proxy processing model where practical
- support request and response header handling over HTTP/3
- define behavior for filters and phases that assume HTTP/1.x or HTTP/2 semantics
- enable protocol negotiation across HTTP/1.1, HTTP/2, and HTTP/3
- stop stripping `alt-svc` when HTTP/3 is enabled and configured

Expected result:

- Pingora can terminate HTTP/3 downstream connections and process requests through its proxy stack

### Milestone 6: Upstream HTTP/3 support

Outcome:
Allow Pingora to proxy traffic to HTTP/3 upstreams.

Features:

- add upstream HTTP/3 peer configuration
- implement connection pooling or reuse rules appropriate for QUIC
- support fallback from HTTP/3 to HTTP/2 or HTTP/1.1 where configured
- define retry, failover, and timeout semantics for QUIC-backed upstream requests
- extend load-balancing components to consider HTTP/3-specific connection behavior

Expected result:

- Pingora can operate as an end-to-end HTTP/3 proxy, not only as an HTTP/3 terminator

### Milestone 7: Production readiness and ecosystem fit

Outcome:
Bring the new protocols in line with the rest of Pingora's operational model.

Features:

- complete user guide coverage for UDP balancing and HTTP/3
- examples for edge proxy, gateway, and load balancer deployments
- benchmarks for UDP and HTTP/3 paths
- interoperability testing across TLS backends and supported runtime environments
- configuration model review for new protocol options

Expected result:

- UDP load balancing and HTTP/3 feel like native Pingora capabilities, not bolt-on features

## Recommended implementation order

1. Design the datagram transport boundary.
2. Land core UDP transport support.
3. Deliver standalone UDP load balancing.
4. Harden UDP operations and observability.
5. Build QUIC transport support.
6. Add downstream HTTP/3.
7. Add upstream HTTP/3.
8. Finish documentation, examples, and production hardening.

## Why this order is recommended

- It respects the current architecture, which is stream-first today.
- It creates intermediate value early by delivering UDP balancing before HTTP/3.
- It reduces the risk of coupling HTTP/3 directly to ad hoc UDP code.
- It keeps each stage testable and easier to review.

## Success criteria

The roadmap is complete when Pingora can:

- load balance UDP traffic as a supported first-class capability
- terminate HTTP/3 downstream connections
- proxy to HTTP/3 upstreams
- negotiate cleanly across HTTP/1.1, HTTP/2, and HTTP/3
- document and operate these features with the same quality bar as the existing TCP and HTTP support
