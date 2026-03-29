# HTTP/3 Interoperability Matrix

This note records the intentionally small interoperability matrix for the
current UDP, QUIC, and HTTP/3 support boundary.

It is not a claim that every HTTP/3 peer is supported. It is a repeatable set
of checks that covers the most important edges of the current implementation.

## Scope

The current matrix is designed to validate:

- downstream HTTP/3 negotiation and request handling
- upstream HTTP/3 request execution toward a controlled origin
- explicit fallback behavior when HTTP/3 is unavailable
- the current unsupported boundary, especially trailers and extension-heavy
  paths

## Downstream Clients

Validate at least one client from each category:

- `curl --http3`
  - checks basic request/response handling and `alt-svc`
- `h3i`
  - checks lower-level HTTP/3 behavior and is useful when debugging headers,
    stream lifecycle, or handshake issues

## Upstream Origins

Validate at least one origin from each category:

- controlled `tokio-quiche`-based origin used by crate tests
  - validates Pingora's current upstream HTTP/3 executor against the same
    backend family used by the QUIC adapter
- external HTTP/3-capable origin in a manual environment
  - validates that Pingora's request path is not only self-consistent but
    interoperable with a non-test peer

## Minimum Checks

For downstream validation:

- negotiate HTTP/3 or advertise `alt-svc` explicitly
- accept a request header and body
- return response headers and body
- verify that HTTP/1.1 fallback remains explicit when HTTP/3 is not enabled

For upstream validation:

- map `Http3Peer` into a real connector config
- establish or reuse an HTTP/3 session
- send request headers and optional body
- read response headers and body
- validate retry/fallback decisions on connect failure

## Known Unsupported or Experimental Areas

The current matrix should record failures or untested behavior in these areas
rather than treating them as regressions:

- upstream request trailers
- upstream response trailers
- HTTP/3 extensions beyond the current request/response path
- migration-heavy QUIC scenarios
- cross-backend QUIC comparisons

## Manual Recording Template

For each manual interop run, record:

- date
- Pingora commit
- client
- origin
- protocol path validated
- result
- any unsupported behavior encountered

This keeps the support boundary explicit and prevents one-off successful tests
from turning into undocumented support claims.
