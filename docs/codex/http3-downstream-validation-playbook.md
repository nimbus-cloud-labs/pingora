# HTTP/3 Downstream Validation Playbook

This playbook defines how to execute `Task 13.2`: validating Pingora as a real
HTTP/3 ingress against external client implementations.

It is intentionally operational. The goal is to make downstream interoperability
validation repeatable and easy to record in the interop matrix.

## Goal

Validate that Pingora can:

- negotiate HTTP/3 with real clients
- serve request and response headers correctly
- handle non-empty request bodies
- stream multi-frame response bodies correctly
- advertise and honor fallback boundaries explicitly

This playbook covers only downstream validation. Upstream and mixed-mode runs
remain separate tasks.

## Recommended Test Topology

Use this topology for the first validation round:

1. start a simple upstream origin that Pingora can proxy to
2. start the Pingora HTTP/3 example with HTTP/3 enabled
3. run multiple real downstream clients against the Pingora ingress
4. record results in the interoperability matrix

The first downstream-only pass was executed against a temporary local HTTP/3
validation harness used during development.

## Environment Requirements

Run this outside restricted sandboxes.

Minimum requirements:

- local UDP sockets must work normally
- the machine must allow QUIC traffic on the chosen local port
- a recent `curl` build with HTTP/3 support must be available
- `h3i` must be installed or otherwise available

Record the exact versions used for every run.

## Suggested Local Setup

### Upstream Origin

Start a simple local origin first. The downstream interoperability task does
not require an HTTP/3 origin.

Any reproducible local origin is acceptable for the first pass as long as it:

- returns a known response body
- accepts a POST request with a small body
- is stable enough to separate ingress bugs from origin instability

### Pingora Ingress

The initial downstream validation was executed against a temporary development
harness, not a stable user-facing example.

For future reruns of `Task 13.2`, prefer a dedicated integration harness or a
real Pingora deployment shape instead of a repository example created only for
one validation cycle.

## Required Client Runs

### Run 1: `curl --http3`

Purpose:

- negotiation
- request and response validation
- body forwarding
- visible fallback behavior

Suggested checks:

```bash
curl -k --http3 -v https://localhost:9443/about
curl -k --http3 -v -d 'pingora-http3' https://localhost:9443/echo
```

Record:

- whether HTTP/3 was negotiated
- whether `alt-svc` was visible when expected
- status code
- response body correctness
- any handshake or transport warnings

### Run 2: `h3i`

Purpose:

- lower-level stream and header validation
- better visibility into stream lifecycle quirks

Suggested checks:

- request a simple path with no body
- request a path with a non-empty body
- inspect stream open, close, and error behavior

Record:

- header correctness
- stream completion behavior
- any close or reset anomalies

## Required Behaviors to Validate

Each downstream client should validate all of the following:

- HTTP/3 negotiation succeeds when enabled
- fallback remains explicit when HTTP/3 is unavailable or disabled
- request headers arrive correctly
- a small POST body is forwarded correctly
- response headers are correct
- response body may arrive in multiple frames and is still correct after full
  aggregation
- no hidden HTTP/1.x-only semantics such as `Connection` or `Upgrade` are
  exposed on the HTTP/3 path

## Result Classification

For each run, classify the result as:

- `Pass`
- `Bug`
- `Unsupported`
- `Environment`

Use the definitions in `docs/codex/http3-interop-matrix.md`.

Do not classify a run as `Pass` if:

- UDP or QUIC was blocked by the environment
- the client did not actually negotiate HTTP/3
- the origin or ingress setup was known to be misconfigured

## Recording Template

Copy this template into the matrix notes for each downstream run:

```text
Date:
Pingora commit:
Rust toolchain:
Scenario: downstream
Ingress example or binary:
Origin:
Origin version:
Client:
Client version:
OS / kernel:
Request path:
Request body:
Negotiated protocol:
Observed alt-svc:
Result classification:
Notes:
```

## Exit Condition for Task 13.2

`Task 13.2` should be marked `[Implemented]` only after:

- `curl --http3` has passed
- `h3i` has passed
- failures have been classified and documented
- the interoperability matrix has been updated with the recorded runs
