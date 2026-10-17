# 68. Request-path Store calls have a client-side deadline

Date: 2026-10-17

## Status

Accepted

## Context

On 2026-10-17 a production `POST /ns/{namespace}/v1/responses` was held for
222 s before it was answered 503 (#567). It never reached credential lease or
upstream dispatch. The first Store call after authentication
(`namespace_resolve` on Postgres) ran on a pooled session with no client-side
bound:

- `connect_timeout` bounds only the TCP connect, not TLS, startup or session
  setup.
- `statement_timeout` is enforced by the server, so it cannot fire when the
  socket is half-open.
- An idle session is only checked with `is_closed()`. With the default 2 h
  keepalive and no `tcp_user_timeout`, a dead peer is noticed only when kernel
  TCP retransmission gives up.

The availability stance was effectively "wait for the Store as long as the
socket allows". A request could hold a client connection for minutes, and it
failed only later and ambiguously.

## Decision

Request-path Store operations (`namespace_resolve`, `namespace_read`,
`budget_admit`) run under one deadline that covers session acquire and query.
It is set by `[storage] request_timeout_ms`:

- The default is 3000. The value must be between 1 and 60000 inclusive.
- On timeout the session is discarded and the call fails as
  `StoreError::Unavailable`. That surfaces as the existing typed
  `store_unavailable` 503, or as the configured budget `on_unavailable` policy.
- The stalled time is recorded in the phase that stalled
  (`axond.store.acquire_wait` or `axond.store.query_duration`) with
  `outcome=error`, and logged with a stable `reason` and no driver text.

Postgres connects are also bounded end to end by the connect timeout. TCP
keepalive and `tcp_user_timeout` defaults are tightened unless the DSN sets
them.

SQLite ignores the key. A dispatched blocking SQLite call cannot be cancelled,
so its existing dispatch-slot wait and `busy_timeout` remain its bound. The
TypeScript gateway applies the same key and refusals.

Operations off the request path (settlement, usage, management routes) keep
their existing bounds.

### State tier

No change. The decision applies to the Tier 2 Postgres Store and is a no-op at
Tier 0 (SQLite / config-only). It does not raise the tier of any deployment.

## Consequences

- A stalled or half-open Postgres session now fails one request in about 3 s
  instead of minutes, and the session is replaced.
- A healthy Store whose request-path call genuinely takes more than 3 s now
  gets 503s where it used to wait. Operators with a slow Store raise
  `request_timeout_ms` (up to 60 s). This is a new configuration key and a new
  default at upgrade: release-impact is a behavior change of the default, with
  no schema or permission change.
- The availability coupling to the Store is unchanged in kind: a Store outage
  was already a 503. The change is how quickly that 503 is returned.
