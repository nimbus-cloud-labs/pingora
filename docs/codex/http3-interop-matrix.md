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

Use `docs/codex/http3-downstream-validation-playbook.md` as the execution guide
for downstream client runs and
`docs/codex/http3-upstream-validation-playbook.md` for upstream origin runs.

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

### Upstream Origins

These origins are required because self-consistency alone is not enough.

| Category | Peer | Version Policy | Purpose |
| --- | --- | --- | --- |
| Controlled origin | Pingora or `tokio-quiche` test origin | Record exact Pingora commit or backend crate version | Deterministic regression checks |
| External origin | Public or reproducible third-party HTTP/3 origin, starting with `https://www.google.com/` | Record exact origin software and version when known | Validate non-fixture interoperability |
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

## Recorded Runs

### 2026-03-29: Downstream `curl --http3-only`

- Pingora commit: local working tree after `585cfee`
- Scenario category: downstream
- Ingress example: temporary downstream HTTP/3 validation harness used during development
- Origin: example-local response path
- Origin version: N/A
- Client: `curl`
- Client version: `8.2.1-DEV`
- Operating system: local developer environment
- Protocol path validated: `client HTTP/3 -> Pingora downstream HTTP/3 ingress`
- Request path: `GET /about/`
- Request body: none
- Negotiated protocol: HTTP/3
- Observed alt-svc: not applicable for `--http3-only` direct validation
- Result classification: `Pass`
- Notes: response was `HTTP/3 200` with body `path=/about/ body=`

### 2026-03-29: Downstream `curl --http3-only` POST body echo

- Pingora commit: local working tree after `585cfee`
- Scenario category: downstream
- Ingress example: temporary downstream HTTP/3 validation harness used during development
- Origin: example-local response path
- Origin version: N/A
- Client: `curl`
- Client version: `8.2.1-DEV`
- Operating system: local developer environment
- Protocol path validated: `client HTTP/3 -> Pingora downstream HTTP/3 ingress`
- Request path: `POST /echo`
- Request body: `pingora-http3`
- Negotiated protocol: HTTP/3
- Observed alt-svc: not applicable for `--http3-only` direct validation
- Result classification: `Pass`
- Notes: response body matched request body exactly

### 2026-03-29: Downstream `h3i`

- Pingora commit: local working tree after `585cfee`
- Scenario category: downstream
- Ingress example: temporary downstream HTTP/3 validation harness used during development
- Origin: example-local response path
- Origin version: N/A
- Client: `h3i`
- Client version: `0.6.0`
- Operating system: local developer environment
- Protocol path validated: `client HTTP/3 -> Pingora downstream HTTP/3 ingress`
- Request path: `GET /about`
- Request body: none
- Negotiated protocol: HTTP/3
- Observed alt-svc: not applicable for direct H3 validation
- Result classification: `Pass`
- Notes: `h3i` received `:status: 200`, `server: pingora-http3-ingress-example`, `content-type: text/plain; charset=utf-8`, and one `DATA` frame of length `30`; connection later idled out without an application-level failure

### 2026-03-29: Upstream `www.google.com`

- Pingora commit: local working tree after `5008689`
- Scenario category: upstream
- Pingora binary or harness: temporary local proxy harness based on `pingora-proxy/examples/http3_proxy.rs`
- Origin: `https://www.google.com/`
- Origin version: unknown public Google production deployment
- Origin certificate expectations: public CA-trusted certificate for `www.google.com`
- Operating system: local developer environment
- Request path: `GET /robots.txt`
- Request body: none
- Session reuse observed: not yet validated in this run
- Negotiated protocol: upstream HTTP/3, downstream HTTP/1.1
- Result classification: `Pass`
- Notes: the proxy returned `HTTP/1.1 200 OK` with the real `robots.txt` payload from Google, preserved upstream response headers such as `content-type: text/plain`, and kept the local proxy-added `Server: Http3AwareProxy` header

### 2026-03-29: Upstream failure-oriented run against `www.google.com:444`

- Pingora commit: local working tree after `5008689`
- Scenario category: upstream
- Pingora binary or harness: temporary local proxy harness based on `pingora-proxy/examples/http3_proxy.rs`
- Origin: `www.google.com:444`
- Origin version: unknown public Google production deployment on an intentionally wrong port
- Origin certificate expectations: not reached successfully because the target port was intentionally invalid for the test
- Operating system: local developer environment
- Request path: `GET /robots.txt`
- Request body: none
- Session reuse observed: not applicable
- Negotiated protocol: upstream HTTP/3 attempt, downstream HTTP/1.1
- Result classification: `Pass`
- Notes: the proxy returned `HTTP/1.1 502 Bad Gateway`, which is the expected outcome for this failure-oriented connect-time validation run

### 2026-04-01: Standard service listener `HTTP/3 downstream -> HTTP/3 upstream` GET

- Pingora commit: local working tree after the `13.5` listener integration fixes
- Scenario category: mixed-mode
- Pingora binary or harness: `pingora-proxy/examples/http3_proxy.rs`
- Downstream client: `curl --http3-only`
- Downstream client version: local developer environment version
- Origin: `https://www.google.com/`
- Origin version: unknown public Google production deployment
- Operating system: local developer environment
- Request path: `GET /robots.txt`
- Request body: none
- Negotiated protocol: downstream HTTP/3, upstream HTTP/3
- Result classification: `Pass`
- Notes: the standard `http_proxy_service().add_http3(...)` path completed the
  full round trip locally; debug logs confirmed the upstream HTTP/3 response was
  received, written into the subrequest-backed proxy lifecycle, and terminated
  cleanly on the downstream HTTP/3 stream

### 2026-04-01: Standard service listener local HTTP/3 POST body echo

- Pingora commit: local working tree after the `13.5` listener integration fixes
- Scenario category: mixed-mode
- Pingora binary or harness: `pingora-proxy/examples/http3_proxy.rs`
- Downstream client: `curl --http3-only`
- Origin: local standard service listener path
- Operating system: local developer environment
- Request path: `POST /echo`
- Request body: non-empty downstream body
- Negotiated protocol: downstream HTTP/3
- Result classification: `Pass`
- Notes: the standard live listener path accepted a downstream HTTP/3 request
  body and the example-local `/echo` route returned the same payload, which
  confirms body handling works on the standard listener path without falling
  back to the earlier temporary harness

### 2026-03-31: Mixed-mode `HTTP/3 -> HTTP/1.1`

- Pingora commit: local working tree after `5008689`
- Scenario category: mixed-mode
- Validation shape: in-crate integration test using the real downstream HTTP/3 bridge and the real stream-based upstream connector
- Downstream path: `client HTTP/3 stream -> Http3ProxyBridge`
- Upstream origin: local HTTP/1.1 origin started inside `pingora-proxy/src/proxy_h3.rs` test
- Origin version: local test origin
- Operating system: local developer environment
- Request path: `POST /echo`
- Request body: `ping` + `pong`
- Protocol path validated: `downstream HTTP/3 -> upstream HTTP/1.1`
- Result classification: `Pass`
- Notes: the mixed-mode path returned a complete downstream HTTP/3 response body `/echo|pingpong`; this validates the bridge plus upstream execution path, not yet a live QUIC listener wired into `http_proxy_service()`

### 2026-03-31: Mixed-mode `HTTP/3 -> HTTP/2`

- Pingora commit: local working tree after `5008689`
- Scenario category: mixed-mode
- Validation shape: in-crate integration test using the real downstream HTTP/3 bridge and the real stream-based upstream connector
- Downstream path: `client HTTP/3 stream -> Http3ProxyBridge`
- Upstream origin: local HTTP/2 origin started inside `pingora-proxy/src/proxy_h3.rs` test
- Origin version: local test origin
- Operating system: local developer environment
- Request path: `POST /echo`
- Request body: `ping` + `pong`
- Protocol path validated: `downstream HTTP/3 -> upstream HTTP/2`
- Result classification: `Pass`
- Notes: the mixed-mode path returned a complete downstream HTTP/3 response body `/echo|pingpong`; this validates the bridge plus upstream execution path, not yet a live QUIC listener wired into `http_proxy_service()`

### 2026-03-31: Mixed-mode `HTTP/3 -> HTTP/3`

- Pingora commit: local working tree after `5008689`
- Scenario category: mixed-mode
- Validation shape: in-crate integration test using the real downstream HTTP/3 bridge and the real HTTP/3 upstream executor
- Downstream path: `client HTTP/3 stream -> Http3ProxyBridge`
- Upstream origin: local HTTP/3 origin started inside `pingora-proxy/src/proxy_h3.rs` test
- Origin version: local `tokio-quiche` test origin
- Operating system: local developer environment
- Request path: `POST /echo`
- Request body: `ping` + `pong`
- Protocol path validated: `downstream HTTP/3 -> upstream HTTP/3`
- Result classification: `Pass`
- Notes: the mixed-mode path returned a complete downstream HTTP/3 response body `/echo|pingpong`; this validates the bridge plus upstream execution path, not yet a live QUIC listener wired into `http_proxy_service()`
