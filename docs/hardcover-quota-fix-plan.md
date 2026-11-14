# Hardcover quota fix plan

Status: implemented core fix on `fix/hardcover-quota-recovery`. See [operational behavior](hardcover-quota-handling.md) for the delivered scope.

The implementation disables new speculative catalogue warming. It does not add an opt-in warming budget or replace historical cache entries.
Header-based configuration replaces hardcoded refill assumptions after the first response. Scheduling coordinates one process.
The supplied daily 429 response is captured in the companion investigation. A successful live-library smoke test remains an operational rollout step.

The fix must control API use, preserve complete cached catalogues, and resume interrupted work safely.
It will use an independent design rather than merge either upstream crawl proposal.
The companion [API contract investigation](hardcover-api-contract.md) records external evidence and remaining unknowns.

## Current evidence

The supplied log contains 5,000 records across approximately 33 minutes on November 14, 2026.
Aggregate analysis found 719 upstream 429 attempts and 104 batches reaching their fifth failed attempt.
Nineteen author pagination failures also reported catalogue completion within this sample.
The log does not contain response headers or establish which work consumed the daily allowance.

The retry loop in `internal/graphql.go` matches error strings and retries outside the batch flush cadence.
It also waits after its last attempt.
The Hardcover transport in `cmd/rghc/main.go` does not capture response headers.
The handler recognizes `statusErr`, but the observed HTTP error text does not match the GraphQL status parser.

The author iterator in `internal/hardcover.go` ends on failure without returning an error.
The controller then clears the persisted refresh state.
Incidental author lookups can also schedule full catalogue refreshes through the same path as explicit requests.

## 1. Preserve the API response contract

Add a Hardcover-specific HTTP transport that observes responses before the GraphQL library reduces them to errors.
Preserve HTTP status, recognized error code, retry time, and documented limit headers in a typed error.
Keep GraphQL field errors separate from HTTP failures, including errors returned with HTTP 200.
Do not classify HTTP failures by searching arbitrary strings for status numbers.

Parse `RateLimit-Policy` and `RateLimit`, including their separate `daily` entry.
Their reset parameter `t` uses relative seconds.
Support legacy `X-RateLimit-*` headers when modern headers are absent.
Legacy reset headers use Unix epoch seconds.
Use daily remaining equal to zero to identify known daily exhaustion.
Prefer valid `Retry-After`, then the relevant bucket reset.
Retain known capacity when daily exhaustion omits the minute entry.

Use an allowlist when recording diagnostic headers.
Record no authorization headers, API keys, or complete request bodies.
Capture one ordinary response and one naturally occurring 429 if a configured test environment permits this.
Do not deliberately exhaust an API allowance.
Mark daily reset behavior as unconfirmed until supported by first-party evidence or an observed response.

## 2. Centralize request scheduling

Use one scheduler for the Hardcover client and its key.
Route initial attempts and retries through that scheduler.
Charge token cost by actual top-level GraphQL fields, rather than HTTP request count or subscriber count alone.
Keep every batch within the configured bucket capacity.
Use server headers to update known capacity and refill information when their meaning is established.

Maintain a shared earliest-next-attempt time after throttling.
Parse supported retry headers according to their documented units and format.
Use bounded exponential backoff with jitter only when no valid server retry time exists.
Do not wait after the final attempt.
Do not retain an interactive request until a daily reset.

Bound pending work and remove canceled subscribers before dispatch.
Let request cancellation stop waiting without blocking other subscribers in a shared batch.
Prioritize interactive work over catalogue warming.
Retain configuration for plan limits rather than assume every key uses the free plan.

The first implementation coordinates one process.
If several processes share a key, they need shared coordination or distinct allocations.
The deployment topology must be established before claiming a global quota guarantee.

## 3. Classify failures and client responses

| Upstream outcome | Internal action | Proposed uncached client response |
| --- | --- | --- |
| HTTP 200 with valid data | Deliver data and update limits | Normal success |
| HTTP 200 with GraphQL errors | Classify each affected field and preserve successful siblings | Existing domain status where valid, otherwise 502 |
| HTTP 429, daily remaining zero | Defer until the daily retry time, without retrying inside the request | 429 with a known `Retry-After` |
| Other HTTP 429 | Apply shared cooldown and bounded retries for short waits | 429 with a known `Retry-After` |
| HTTP 403 with `request_exceeds_capacity` or `top_level_limit_exceeded` | Split an oversized read batch within capacity, with bounded attempts | 502 if the server cannot repair its own batch |
| HTTP 401 or other permission failure | Stop automatic retries and report an operator error | 502, without exposing credentials or implying client authentication failure |
| HTTP 400 or query validation failure | Stop automatic retries | 502 unless caused by invalid client input |
| HTTP 500, 502, 503, or 504 | Retry read operations within a bounded budget through the scheduler | 503 after exhaustion, or 504 for an upstream timeout |
| Caller cancellation | Stop waiting and remove unused queued work | End the canceled request |

These are proposed downstream semantics, not a claim about every Hardcover response.
Serve usable cached data before choosing an error response.
Preserve the existing catalogue while its refresh is pending or interrupted.
Make sure that the target Readarr or Bookshelf client honors the selected retry response.

## 4. Make refresh completion explicit

Change catalogue iteration to return errors, for example through `iter.Seq2[int64, error]`.
Update Goodreads and mocks when changing the shared getter interface.
Only mark a refresh complete after successful enumeration and required processing.
On throttling or transient failure, preserve the last usable catalogue and persist a delayed retry.
Represent queued, running, deferred, and complete states explicitly.

Persist retry timing for long cooldowns so a restart does not immediately repeat exhausted requests.
Keep completeness and catalogue data in one persisted record, or use an atomic update with equivalent lifetime guarantees.
Use one coordination mechanism for incidental and explicit loads of the same author.
Test against production persistence behavior, not only the no-op substitute.

## 5. Bound catalogue expansion

Separate resolving author metadata from requesting a complete catalogue.
An incidental author can supply relationships without scheduling another full refresh.
A later explicit request must promote that same author to a full refresh exactly once.
Default speculative full-catalogue warming to disabled.
If enabled, give warming an explicit work budget and lower priority than requested authors.

Do not remove cached catalogues or persisted refreshes in bulk.
Classify old pending records before resuming them under the new scheduling rules.
Existing partial entries without a completeness record need a conservative migration policy.
Avoid promoting every historical cache entry to immediate work during that migration.

## 6. Validate and observe

Use a fake clock and a local HTTP test server for deterministic timing tests.
Cover weighted token use, retries through the limiter, and batch capacity enforcement.
Cover retry headers, missing or malformed headers, long cooldowns, and recovery after reset.
Cover GraphQL partial errors, cancellation, bounded queues, and interactive priority.
Cover pagination failure, restart recovery, and persistence of an incomplete catalogue.
Cover incidental-to-explicit promotion and concurrent updates without catalogue loss.

Count actual HTTP attempts separately from logical queries and batches.
Expose attempted fields, status codes, retries, cooldown reason, and next permitted attempt.
Expose refresh completion, deferred refreshes, queue depth, and speculative work separately.
Record daily remaining allowance only when the API supplies a trustworthy value.
Measure whether the daily counter charges HTTP attempts or field units before claiming that batching saves daily allowance.

Deliver this in stages: response handling, scheduler, refresh recovery, then crawl policy.
Run focused tests and race tests at each stage.
Run the existing suite, with a local Postgres service for persistence coverage, before release.
Use a small real library for the final smoke test and compare actual upstream usage with the account dashboard.
