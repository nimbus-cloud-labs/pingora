# HTTP/3 Production Readiness Review

This note records the current weak points of the UDP, QUIC, and HTTP/3 stack
after Cycle 2.

It is intentionally practical. The goal is to identify where the branch is
already structurally solid, where the remaining risk lives, and what would be
required before calling the stack production-ready.

## Executive Summary

The current branch has a coherent end-to-end transport and proxy path:

- UDP forwarding is implemented in both directions
- QUIC runs on a real backend path
- downstream HTTP/3 is real, not only modeled
- upstream HTTP/3 executes real requests against controlled origins

The remaining risk is not primarily architectural. The main concerns are:

- interoperability breadth
- semantics under less convenient framing and failure conditions
- operational hardening under load and in mixed environments
- support-boundary clarity for features that remain partial or unsupported

## What Looks Solid

These areas now appear relatively strong from an implementation perspective:

- the transport layering between UDP, QUIC, and HTTP/3
- the separation between downstream and upstream responsibilities
- explicit retry and fallback modeling instead of hidden protocol switching
- explicit pool ownership for QUIC and HTTP/3 reuse
- roadmap and validation artifacts that now describe the real code rather than
  the planned one

This matters because the next cycle should not redesign these foundations. It
should harden and validate them.

## Primary Fragility Areas

### Interoperability

The stack is validated well against controlled peers and internal fixtures, but
not yet against a broad external matrix of HTTP/3 clients and origins.

This is the largest remaining production risk.

Examples of what still needs more evidence:

- differences between `curl --http3`, `h3i`, and browser-derived client stacks
- behavior against non-test HTTP/3 origins
- stricter peers that are less forgiving about edge-case framing or timing

### Semantics Under Arbitrary Framing

The implementation now handles real body chunking, but this remains an area to
review carefully.

Questions that deserve explicit validation:

- whether filters or tests accidentally assume a convenient chunk layout
- whether resets and early closes leave partial request or response state behind
- whether fallback or retry decisions can be triggered after too much request
  state has already been committed

### Retry, Failover, and Fallback Boundaries

The retry classifier is now connected to the real lifecycle, which is a major
improvement. But production readiness depends on proving that the current
decision boundaries are correct under real failure modes.

In particular:

- connect-time failure behavior must remain clearly retry-safe
- mid-request failure behavior must remain conservative
- fallback from HTTP/3 to HTTP/2 or HTTP/1.1 must never hide semantic
  mismatches
- backend remap rules must remain understandable during repeated failures

### Streaming and Backpressure

The current path supports real request and response bodies, but the stack still
deserves review for long-lived or high-volume streaming cases.

The main concern is not whether streaming exists, but whether it behaves well
under:

- large bodies
- many concurrent streams
- slow readers and slow writers
- partial resets and half-closed stream states

### Operational Hardening

The code now has meaningful observability, but there is still a gap between
"events exist" and "operators can debug incidents quickly."

Areas still likely to need work:

- timeout tuning guidance
- pool retention and eviction visibility
- production troubleshooting playbooks
- support expectations across Linux-first vs broader environments

## Unsupported or Partial Areas

These should continue to be treated as explicit support-boundary items:

- downstream request trailers
- upstream request trailers
- upstream response trailers
- extension-heavy HTTP/3 behavior outside the current request/response path
- broad QUIC migration scenarios

These are not necessarily bugs. They are production-readiness decisions that
must remain visible.

## Suggested Review Order

If the branch is reviewed like a PR, this is the order that should produce the
highest signal:

1. Possible bugs in stream, pool, and lifecycle handling
2. Semantic regressions relative to the HTTP/1.1 and HTTP/2 paths
3. Production-readiness gaps in retry, fallback, and observability
4. The exact boundary between supported, experimental, and unsupported behavior

## Exit Criteria for "Production-Ready"

The stack should not be called production-ready until all of the following are
true:

- external interoperability has been validated against a documented matrix
- retry and fallback behavior has been validated under controlled failure modes
- load-oriented and soak-style validation exists for the hot paths
- support-boundary docs are explicit and match reality
- operators have enough metrics and troubleshooting guidance to debug the path
  in production

## Non-Goals for the Next Cycle

The next production-hardening cycle should stay focused.

Avoid treating these as required unless validation proves otherwise:

- redesigning the transport layering
- adding every optional HTTP/3 extension
- broad QUIC backend abstraction work
- cross-platform portability work unrelated to real production blockers
