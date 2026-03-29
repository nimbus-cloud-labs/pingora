# HTTP/3 Production Readiness Roadmap

This roadmap describes the work required to move the current UDP, QUIC, and
HTTP/3 stack from "coherent and usable" to "production-ready with an explicit
support boundary."

It starts from the current end state of Cycle 2:

- bidirectional UDP forwarding exists
- QUIC is integrated on a real backend path
- downstream HTTP/3 is implemented on a real transport
- upstream HTTP/3 executes real requests against controlled origins

The goal of this roadmap is not to redesign the stack. The goal is to harden,
validate, document, and narrow the remaining risk.

## Milestone 13: Interoperability Validation

Objective: prove the stack behaves correctly against a documented matrix of
external peers.

Focus areas:

- downstream HTTP/3 against real clients
- upstream HTTP/3 against real origins
- mixed deployments where Pingora terminates HTTP/3 and forwards to HTTP/1.1 or
  HTTP/2 upstreams
- protocol negotiation behavior, including `alt-svc` and fallback boundaries

Success criteria:

- a documented interop matrix has been executed
- failures are classified as bug, unsupported behavior, or environment issue
- unstable cases are either fixed or documented as unsupported

## Milestone 14: Failure-Mode Hardening

Objective: make retry, failover, timeout, and close behavior safe and
predictable under real errors.

Focus areas:

- connect-time failure vs mid-request failure handling
- downstream stream reset and early close behavior
- upstream close, timeout, and partial body failure behavior
- backend remap and fallback policy under repeated failures

Success criteria:

- failure-mode tests exist for the main request paths
- retry and fallback rules are documented and validated
- no known ambiguous ownership or lifecycle transitions remain in hot paths

## Milestone 15: Streaming and Load Validation

Objective: validate body streaming, concurrency, and pool behavior under
pressure.

Focus areas:

- large request and response bodies
- many concurrent streams per QUIC session
- slow producer and slow consumer paths
- pool reuse, saturation, and eviction under sustained load
- UDP and QUIC memory growth during long-running scenarios

Success criteria:

- load and soak scenarios exist for representative paths
- performance baselines are recorded for critical benchmarks
- regressions can be detected by rerunning documented validation steps

## Milestone 16: Observability and Operations

Objective: make the stack operable in production and debuggable during
incidents.

Focus areas:

- metrics coverage for session, stream, pool, retry, timeout, and fallback
  events
- log messages that are useful during live debugging
- troubleshooting guidance for common failure classes
- configuration guidance for timeouts, limits, and feature selection

Success criteria:

- operators can tell why requests failed, retried, or fell back
- dashboards and troubleshooting notes can be built from the exported signals
- configuration guidance exists for common deployment shapes

## Milestone 17: Support Boundary and Release Readiness

Objective: define exactly what is supported, what is experimental, and what is
not yet supported.

Focus areas:

- trailers support boundary
- HTTP/3 feature subset and known limitations
- platform assumptions
- compatibility guarantees for ingress and upstream scenarios
- release notes and user-facing documentation

Success criteria:

- support status is explicit in docs
- known limitations are not hidden in tests or code comments
- release-facing documentation matches the actual implementation

## Recommended Execution Order

Work should proceed in this order:

1. Interoperability validation
2. Failure-mode hardening
3. Streaming and load validation
4. Observability and operations
5. Support boundary and release readiness

This order keeps the next cycle evidence-driven. It avoids over-tuning the
system before we know which behaviors actually fail under realistic peers and
traffic.
