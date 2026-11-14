# Hardcover API usage

The Hardcover client charges each top-level GraphQL field against a shared token bucket.
It starts with the free-plan defaults of ten tokens and one token per second.
Response headers update the capacity, refill rate, and remaining allowance.
Retries use the same bucket as initial requests.

The client reads modern `RateLimit` and `RateLimit-Policy` headers, with support for legacy `X-RateLimit-*` headers.
When the daily allowance reaches zero, the client stops upstream requests until the server's retry time.
It saves that deadline in the existing database, under a hash of the API credential.
The saved state contains no API credential.
A restart with the same credential retains the cooldown.

An uncached request during cooldown returns HTTP 429 and `Retry-After` in seconds.
Usable cached author data remains available during an interrupted refresh.
Interactive book, work, and series requests can also use retained expired data when upstream fetching fails.
Background refreshes propagate those failures so stale data cannot count as a successful refresh.

The client splits oversized read batches after a recognized capacity error.
It does not retry authentication or permission failures inside a request.
A background author refresh with such a failure remains pending for recovery after configuration repair and restart.
Other transient failures receive bounded retries.
Long waits return to callers rather than keeping their HTTP requests open until the daily reset.

Author relationships load metadata without starting another full catalogue refresh.
An explicit author request promotes incidental metadata to a full catalogue refresh.
Failed pagination preserves the previous snapshot and schedules a persisted retry deadline.
Existing pending refreshes resume on restart without deleting cached catalogues.
New incidental full-catalogue warming is disabled.

`RG_HC_BATCH_SIZE` sets the collection batch size.
`RG_HC_FLUSH_MS` sets the collection interval, not the token refill rate.
Increasing either value does not bypass the field-based limiter.
Pending GraphQL batches are bounded, and queued interactive requests take priority over background batches.
An attempt already in progress finishes before the next batch starts.

The existing GraphQL metrics now include `http_attempts`, `attempted_fields`, `http_retries`, and `http_status_<code>` counter types.
The existing gauge exposes `upstream_retry_at` as a Unix timestamp and `daily_remaining` when the API supplies that value.
Logical query counters remain separate from actual HTTP attempt counters.
Logs record cooldown deadlines and deferred author refreshes without credentials.

Scheduling coordinates one process.
Several processes sharing a key can still compete for the same allowance.
Server response headers limit that competition, but they do not provide a shared local reservation.
Previously cached author entries retain their existing expiry policy.
The service cannot infer which historical entries were incomplete before this fix.

The [API investigation](hardcover-api-contract.md) records the observed daily 429 response and first-party references.
Daily accounting units remain unconfirmed, so the client does not estimate daily allowance from batch field counts.
