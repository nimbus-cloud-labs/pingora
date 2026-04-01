# HTTP/3 Upstream Validation Playbook

This playbook defines how to execute `Task 13.3`: validating Pingora's real
HTTP/3 upstream path against origins outside the narrow controlled-fixture
environment used by crate tests.

It is intentionally practical. The goal is to make upstream interoperability
validation repeatable and to prevent "it worked once against our own fixture"
from being mistaken for production evidence.

## Goal

Validate that Pingora can:

- map `Http3Peer` into a real QUIC connector configuration
- establish or reuse real upstream HTTP/3 sessions
- send request headers and optional request bodies to an origin
- read response headers, response bodies, and stream completion correctly
- classify connect-time failure and retry/fallback behavior conservatively

This playbook covers only the upstream side. Mixed-mode ingress-to-origin
deployments remain part of `Task 13.4`.

## Recommended Origin Categories

The first upstream validation round should cover these categories:

- controlled origin outside the direct test path
- reproducible external origin or reproducible standalone HTTP/3 server
- one failure-oriented target used to confirm connect-time fallback behavior

Do not treat only the in-crate `tokio-quiche` fixture as sufficient evidence for
this task.

## Environment Requirements

Run this outside restricted sandboxes.

Minimum requirements:

- local UDP sockets and outbound UDP must work normally
- the machine must allow reaching the chosen HTTP/3 origin
- the environment must make peer certificate handling explicit
- exact origin versions must be recorded when possible

## Validation Modes

### Mode 1: Controlled but Separate Origin

Purpose:

- validate the upstream executor against a reproducible origin that is not only
  the in-test fixture path
- keep debugging straightforward before moving to third-party peers

Suggested setup:

- start a standalone reproducible HTTP/3 origin
- point a Pingora proxy instance at that origin using `Http3Peer`
- exercise a small set of known requests

### Mode 2: External or Third-Party Origin

Purpose:

- validate that Pingora's upstream HTTP/3 path interoperates with a peer it does
  not control

Suggested setup:

- use a reproducible public or internally managed origin with HTTP/3 enabled
- ensure the origin's software and version are known
- record certificate and trust expectations

Recommended first target:

- `https://www.google.com/`

Rationale:

- it is a well-known public origin with widely deployed HTTP/3 support
- it is more suitable than `example.org` for an initial external upstream check
- it gives a pragmatic first signal that Pingora interoperates with a
  production-grade external peer

Current status:

- initial external run completed successfully against `GET /robots.txt`
- initial failure-oriented run completed successfully against `www.google.com:444`
- this is currently treated as sufficient evidence to close `Task 13.3`

Caveat:

- `www.google.com` is useful as a first interoperability check, not as the only
  upstream target for the milestone
- origin-specific behavior such as redirects, caching, and transport policy
  should not be over-generalized

### Mode 3: Failure-Oriented Origin

Purpose:

- validate fallback and retry classification under realistic connect-time
  failures

Suggested setup:

- point Pingora at an origin that is unreachable, misconfigured, or explicitly
  not accepting HTTP/3
- verify that the resulting decision is `Retry`, `Fallback`, or `Fail`
  according to the configured classifier and transport policy

Suggested first run:

```bash
PINGORA_HTTP3_UPSTREAM_HOST=www.google.com \
PINGORA_HTTP3_UPSTREAM_PORT=444 \
RUST_LOG=info,pingora_quic=debug,pingora_proxy=debug \
cargo run -p pingora-proxy --example http3_proxy --features http3
```

Then:

```bash
curl -v http://127.0.0.1:6191/robots.txt -H 'Host: www.google.com'
```

This should produce a non-success upstream decision and exercise the configured
HTTP/3 retry or fallback behavior without depending on DNS failure or a random
external outage.

## Required Behaviors to Validate

Each upstream validation round should cover all of the following:

- request headers arrive correctly at the origin
- optional request body forwarding works correctly
- response headers are mapped correctly
- response bodies are fully readable and stream completion is correct
- pooled upstream session reuse behaves as expected across sequential requests
- connect-time failure behavior is classified correctly

At least one run should also observe:

- QUIC session reuse across more than one request
- a response body that is not trivial or empty
- a connect failure that produces a non-success `Http3UpstreamOutcome`

## Recording Template

For each upstream run, record:

```text
Date:
Pingora commit:
Rust toolchain:
Scenario: upstream
Pingora binary or harness:
Origin:
Origin version:
Origin certificate expectations:
Operating system:
Request path:
Request body:
Session reuse observed:
Negotiated protocol:
Result classification:
Notes:
```

## Exit Condition for Task 13.3

`Task 13.3` should be marked `[Implemented]` after:

- at least one external or third-party origin has passed
- one failure-oriented run has been recorded
- the interoperability matrix has been updated with the recorded results

A controlled but separate origin remains useful as a follow-up interoperability
check, but it is no longer treated as a blocking requirement for this task.
