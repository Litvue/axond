# 64. Charge actuals after the response

Date: 2026-09-04

## Status

Accepted

Supersedes the reserve-before-dispatch hold in
[ADR 0063](./0063-stateful-only-namespaced-gateway.md) and
[ADR 0010](./0010-shared-budget-backends-and-charging-policy.md).

Charging measured consumption (including cancelled or failed requests) from
ADR 0010 remains.

## Context

ADR 0010 and ADR 0063 held a priced estimate on the Store before upstream
dispatch so concurrent in-flight requests could not overshoot the cap. That
write is a transaction on every inference request. The estimate is also
wrong: output, cache, and tool tokens are unknown, so the conservative
allowance over-reserves.

The latency of that hold, and the work of making the estimate honest, are
not worth an exact concurrent cap.

## Decision

The Store ledger does not write a hold on the inference path.

- **Admit** is a read: deny `429 budget_exceeded` when there is no active
  budget or `spent >= limit`. Capture `(period, incarnation)`.
- **Dispatch** with nothing reserved.
- **Charge** after the response (success, upstream error, cancel, or drop):
  add measured spend to `spent` iff the namespace still exists and
  incarnation matches. Wrong incarnation (DELETE + recreate) is a no-op.
- **GET remaining** is `max(0, limit - spent)`. `reserved_microdollars`
  stays on the wire as `0`.
- Concurrent admits while `spent < limit` can all pass; charges can take
  `spent` past `limit`. The next admit then denies. Overshoot is accepted.
- `max_request_microdollars` remains an estimate ceiling at admission. It
  is not a hold.
- Reservation tables stay in the schema (forward-only). Code does not write
  holds.
- `[budget] backend` remains withdrawn (ADR 0063).
  `reservation_ttl_seconds` is unused on the live Store path.

Request-path Store access is one namespace+admit join, then after the
response a single spent increment. Usage append stays off the path.

## Consequences

- Inference no longer takes a pre-dispatch Store write.
- Exact concurrent caps are not a product claim.
- A replica that dies after dispatch and before charge under-records spend.
  There is no TTL hold to reclaim.
- Charge is not idempotent by request id: two concurrent charges both apply.
- A PUT that lowers the limit below `spent` is still accepted; later admits
  deny once `spent >= limit`.
