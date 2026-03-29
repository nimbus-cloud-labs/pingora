# HTTP/3 Interoperability Matrix

This note defines the external interoperability matrix that Pingora must pass
before the HTTP/3 stack can be treated as production-ready.

It is intentionally narrower than "all HTTP/3 peers." The purpose is to create
a repeatable release gate with explicit scope, exact peers, and documented
outcomes.

## Scope

The current matrix is designed to validate:

- downstream HTTP/3 negotiation and request handling
- upstream HTTP/3 request execution toward both controlled and external origins
- mixed-mode routing where Pingora terminates HTTP/3 and forwards to
  HTTP/1.1, HTTP/2, or HTTP/3 upstreams
- explicit fallback behavior when HTTP/3 is unavailable
- the current unsupported boundary, especially trailers and extension-heavy
  paths

The matrix is not a claim that every HTTP/3 extension or peer is supported.
Anything outside this matrix must remain explicitly documented as unsupported,
experimental, or unvalidated.

## Release-Gate Rule

The HTTP/3 path should not be declared production-ready until:

- all required matrix entries have been exercised
- each failure has been classified as `Bug`, `Unsupported`, or `Environment`
- no required entry remains in `Bug` status
- all `Unsupported` outcomes are reflected in user-facing docs

## Required Peer Matrix

### Downstream Clients

These clients are required because they exercise meaningfully different HTTP/3
stacks.

| Category | Peer | Version Policy | Purpose |
| --- | --- | --- | --- |
| CLI | `curl --http3` | Record exact `curl` and `ngtcp2` or `quiche` backend versions | Basic request and response validation, negotiation, and `alt-svc` behavior |
| Debug client | `h3i` | Record exact build or release tag | Lower-level header, stream, and close debugging |
| Browser-oriented stack | Chrome or Chromium | Record exact browser version | Browser-like negotiation and ingress behavior |

### Upstream Origins

These origins are required because self-consistency alone is not enough.

| Category | Peer | Version Policy | Purpose |
| --- | --- | --- | --- |
| Controlled origin | Pingora or `tokio-quiche` test origin | Record exact Pingora commit or backend crate version | Deterministic regression checks |
| External origin | Public or reproducible third-party HTTP/3 origin | Record exact origin software and version when known | Validate non-fixture interoperability |
| Mixed upstream | HTTP/1.1 and HTTP/2 origins | Record exact server software and version | Validate protocol switching after downstream HTTP/3 termination |

## Required Scenarios

### Downstream Scenarios

Each downstream client must validate:

- HTTP/3 negotiation success when enabled
- explicit `alt-svc` behavior where applicable
- request headers and a non-empty request body
- response headers and a chunked or multi-frame response body
- explicit fallback when HTTP/3 is disabled or unavailable

### Upstream Scenarios

Each upstream origin category must validate:

- `Http3Peer` mapping into a connector config
- session establishment or session reuse
- request headers and optional request body forwarding
- response headers, response body, and stream completion
- retry and fallback behavior on connect-time failure

### Mixed-Mode Scenarios

Pingora must be validated in these deployment shapes:

| Downstream | Upstream | Purpose |
| --- | --- | --- |
| HTTP/3 | HTTP/1.1 | Main compatibility path for legacy origins |
| HTTP/3 | HTTP/2 | Mixed modern deployment path |
| HTTP/3 | HTTP/3 | End-to-end QUIC and HTTP/3 path |

Each mixed-mode scenario must validate:

- header mapping, including pseudo-header translation boundaries
- request and response body forwarding
- retry and fallback behavior that remains semantically conservative
- protocol-specific caveats that need to be called out in docs

## Result Classification

Every run must classify the outcome as one of:

- `Pass`: expected behavior observed
- `Bug`: Pingora behavior is incorrect and must be fixed before release
- `Unsupported`: behavior is outside the support boundary and must be
  documented explicitly
- `Environment`: the run is invalid because the environment or peer setup is
  not trustworthy

`Bug` is release-blocking for required entries.

`Unsupported` is acceptable only if:

- the unsupported behavior is already outside the intended support boundary
- the limitation is documented in `docs/user_guide/http3.md`

`Environment` is never treated as evidence of support. The run must be repeated
in a valid environment.

## Version Recording Policy

Every recorded run must capture exact versions when possible.

At minimum record:

- date
- Pingora commit
- Rust toolchain
- client name and version
- origin name and version
- operating system and kernel when relevant
- QUIC or HTTP/3 backend details when exposed by the peer

Do not record results as "tested with curl" or "tested with Chrome" without
exact versions.

## Known Unsupported or Experimental Areas

The matrix should record these areas as unsupported or unvalidated unless and
until dedicated work changes the support boundary:

- downstream request trailers
- upstream request trailers
- upstream response trailers
- HTTP/3 extensions beyond the current request/response path
- migration-heavy QUIC scenarios
- cross-backend QUIC comparisons

## Manual Recording Template

For each interop run, record:

- date
- Pingora commit
- scenario category: downstream, upstream, or mixed-mode
- client
- client version
- origin
- origin version
- operating system
- protocol path validated
- result classification
- notes on peer quirks or unsupported behavior

This keeps the support boundary explicit and prevents one-off successful tests
from turning into undocumented support claims.
