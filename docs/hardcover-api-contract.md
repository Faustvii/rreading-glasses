# Hardcover API response contract

Research date: 2026-11-14. One authorized read-only API request was made after the initial source investigation.

## Observed daily exhaustion response

A single `query { me { id } }` request returned HTTP 429.
The server supplied the following headers. No credentials were saved in this document.

```http
Date: Sat, 14 Nov 2026 18:43:43 GMT
RateLimit: "daily";r=0;t=18977
RateLimit-Policy: "Free";q=60;w=60;burst=10, "daily";q=5000;w=86400
Retry-After: 18977
X-RateLimit-Daily-Limit: 5000
X-RateLimit-Daily-Remaining: 0
X-RateLimit-Daily-Reset: 1794700800
X-RateLimit-Limit: 10
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 1794681823
```

```json
{"error":"Too Many Requests","message":"API rate limit exceeded for tier 'Free'. Try again in 18977 seconds."}
```

The daily reset is November 15, 2026 at 00:00 UTC, or 01:00 in Copenhagen.
This response confirms the reset boundary for this incident, not a permanent API guarantee.
The generic legacy reset equals the response time and does not describe the daily wait.
Use `Retry-After` or the daily reset rather than the generic legacy reset for this response.

## Evidence and limits

The first-party Hardcover SDK is the strongest source located. Its API surface
spec records the gateway's behavior as of 2026-09-28. Findings below are verified
against SDK commit `9e5b90f3e76ed16ff00973c2088fbe0917a86be6`, not against a captured
response from this deployment. The supplied screenshot independently confirms
Free: 5,000 daily requests, 60/minute refill, burst capacity 10.

Sources (pinned):

- [API surface: limits and error shapes](https://github.com/hardcoverapp/hardcover-sdk/blob/9e5b90f3e76ed16ff00973c2088fbe0917a86be6/spec/01-api-surface.md#limits)
- [Rate header parser](https://github.com/hardcoverapp/hardcover-sdk/blob/9e5b90f3e76ed16ff00973c2088fbe0917a86be6/packages/typescript/src/core/rate-limit.ts)
- [Response classification](https://github.com/hardcoverapp/hardcover-sdk/blob/9e5b90f3e76ed16ff00973c2088fbe0917a86be6/packages/typescript/src/core/responses.ts)
- [Client retry contract](https://github.com/hardcoverapp/hardcover-sdk/blob/9e5b90f3e76ed16ff00973c2088fbe0917a86be6/spec/03-sdk-design.md#request-lifecycle)

## Rate limits and headers

The API surface spec describes a per-user token bucket and daily cap. Overrides
can change the tier defaults. Request cost is the number of top-level fields,
with fragments expanded and a minimum cost of one. Free defaults are burst 10,
refill 1 token/second, daily 5,000. Do not assume each batch costs one refill token.

The parser documents these modern response headers:

```http
RateLimit-Policy: "Free";q=60;w=60;burst=10, "daily";q=5000;w=86400
RateLimit: "Free";r=9;t=1, "daily";r=4999;t=86000
Retry-After: 3
```

`q` is quota, `w` the policy window in seconds, `burst` bucket capacity, `r`
remaining allowance, and `t` seconds until reset. The modern reset values are
relative seconds, not timestamps. Once the daily allowance is exhausted, the
SDK's parser comment says the tier entry is omitted from `RateLimit`; retain the
policy information rather than assuming the missing tier means unrestricted.

When modern headers are absent, the SDK falls back to:

| Header | Interpretation in official SDK |
|---|---|
| `X-RateLimit-Limit` | Burst limit |
| `X-RateLimit-Remaining` | Burst remaining |
| `X-RateLimit-Reset` | Unix epoch seconds |
| `X-RateLimit-Daily-Limit` | Daily limit |
| `X-RateLimit-Daily-Remaining` | Daily remaining |
| `X-RateLimit-Daily-Reset` | Unix epoch seconds |

The SDK classifies a 429 as daily exhaustion when daily remaining equals zero.
It prefers numeric `Retry-After` seconds; otherwise it uses the exhausted bucket's
reset seconds, then a one-second fallback. It retries burst 429s but does not
retry daily-cap 429s inside the request. This is SDK policy, not proof that all
responses always contain every header.

## HTTP and GraphQL response handling

The following follows the first-party API spec and response classifier:

| Response | Meaning / shape | Planned handling implication |
|---|---|---|
| 429 | Burst depletion or daily cap; spec body `{error: "Too Many Requests", message}` | Inspect headers to distinguish windows and preserve reset time; body alone cannot distinguish them |
| 403 + `errors: ["request_exceeds_capacity"]` | Request field cost exceeds bucket capacity | Split/reduce batch; waiting cannot fix the same request |
| 403 + `error: "top_level_limit_exceeded"` | Also classified as request too large by SDK | Treat as capacity failure, not transient throttling |
| 401 | Authentication failure | Fail without transient retry |
| 403 + `error: "insufficient_scope"` | Missing scopes, space-separated `scope` field | Fail without transient retry |
| 403 + `error: "unsupported_operation"` | Unsupported operation | Fail without transient retry |
| Other 403 | Unclassified forbidden response | Preserve status and safe error details; do not assume capacity |
| 400 | Invalid request | Fail without transient retry |
| 408 / 5xx / network errors | SDK retries read queries with bounded backoff | Keep separate from rate-limit reset logic |
| HTTP 200 + GraphQL `errors` | GraphQL failure, potentially partial `data` | Preserve per-field result/error handling |
| HTTP 200 + `data.<action>.error` or `.errors` | Action failure | Success status alone does not prove operation success |

The observed local logs contain 429 bodies `{"data":null}`, unlike the documented
rate-limit envelope. Therefore status and headers must remain authoritative;
error-body parsing must tolerate absent or different message fields.

## Still requiring direct verification

- Whether the daily counter charges HTTP requests or field-cost units. The UI
  calls them requests, while the API spec defines field cost without explicitly
  assigning separate daily accounting semantics. Do not assert a daily batching
  saving without a before/after response counter measurement.
- Whether every daily reset uses the same boundary. Use the server's reset value instead of hardcoding midnight UTC.
- Whether all unsuccessful attempts consume daily allowance.

A single read-only request using the already exhausted key can establish the
actual response status, allowlisted rate headers, and redacted body. It should
not log request headers, token, or complete environment/configuration contents.

## Fixture corroboration (not a live observation)

The official SDK's [daily-not-retried fixture](https://github.com/hardcoverapp/hardcover-sdk/blob/9e5b90f3e76ed16ff00973c2088fbe0917a86be6/shared/conformance/cases.json)
constructs HTTP 429 with `Retry-After: 40000`,
`RateLimit: "daily";r=0;t=40000`, and body `{"error":"Too Many Requests"}`.
It expects exactly one request despite `maxRetries: 3`. This confirms intended
SDK behavior and a daily-header example, not that production emitted those
headers for this incident.

The [shared error taxonomy](https://github.com/hardcoverapp/hardcover-sdk/blob/9e5b90f3e76ed16ff00973c2088fbe0917a86be6/shared/errors.json)
identifies `top_level_limit_exceeded` as the older gateway's flat-limit code;
current oversized requests use `errors[0] == "request_exceeds_capacity"`.
