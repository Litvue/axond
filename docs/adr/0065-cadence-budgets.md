# 65. Cadence budgets: the gateway rolls monthly periods itself

Date: 2026-09-05

## Status

Accepted

Amends the “Active period (chosen)” rule in
[ADR 0063](./0063-stateful-only-namespaced-gateway.md). The `(namespace,
period)` ledger, the `PUT`/`GET /api/v1/namespaces/{ns}/budgets/{period}`
routes, and the period-free inference contract are unchanged.

## Context

ADR 0063 made `period` a caller-chosen opaque key: a successful
`PUT …/budgets/{period}` marks that period active, and a new billing period
exists only once a caller PUTs a new key. For a monthly plan every consumer has
to run a job that PUTs `YYYY-MM` before the first request of the month, and a
late or failed job either keeps charging last month's period or, if the old row
is exhausted, denies everything with `429 budget_exceeded` until someone acts
([#454](https://github.com/Litvue/axond/issues/454)). The gateway already
knows the clock; it should not need a caller to tell it the month.

## Decision

A namespace may carry one **cadence budget**:

```text
PUT /api/v1/namespaces/{ns}/budget
{ "cadence": "monthly" | "fixed", "limit_microdollars": <int>,
  "timezone": <IANA name, default "UTC">, "period": <fixed only> }
GET /api/v1/namespaces/{ns}/budget
→ { namespace, cadence, limit_microdollars, timezone, period,
    spent_microdollars, reserved_microdollars, remaining_microdollars, active }
```

**Monthly.** The active period is derived, not chosen. At admission the Store
reads the clock, converts it to the budget's timezone and uses `YYYY-MM` as the
period key. If no `(ns, YYYY-MM)` spend row exists yet it is created with the
cadence limit and `spent = 0` inside the same admission (`INSERT … ON CONFLICT
DO NOTHING`, so two concurrent first requests of a month create exactly one
row). The first request of a new month therefore starts a fresh period with no
caller action; last month's row is untouched and stays readable through
`GET …/budgets/{YYYY-MM}` and `GET …/usage?period=YYYY-MM`. The steady state is
a read: only the first request of a period writes.

A `PUT …/budget` with a new monthly limit rewrites the *current* period's row
limit (spend preserved) and every later period is created with the new limit.
Earlier periods keep whatever limit they closed with. Timezone changes take
effect at the next admission; a period key that already exists is reused.

**Fixed.** `cadence: "fixed"` keeps the ADR 0063 behaviour and is what a
namespace without a cadence row is treated as. `PUT …/budget` with `"fixed"`
and a `period` is `PUT …/budgets/{period}`; without a `period` it re-limits the
current active period and is `400 bad_request` when there is none.

**Precedence.** While a `monthly` cadence row exists the
`axond_store_budget_active` marker is ignored by admission: a cadence budget
wins. `PUT …/budgets/{period}` still works — it writes the spend row and the
active marker, so switching back with `PUT …/budget {"cadence":"fixed"}`
resumes from it — and a PUT on the current `YYYY-MM` key overrides that
month's limit. `GET …/budgets/{period}.active` reports whether admission
charges that period right now under whichever rule applies.

`GET …/budget` on a namespace with no cadence row synthesizes the `fixed` view
of the active period (`timezone: "UTC"`) and is `404 unknown_budget` when
there is no active period either. `GET …/budget` never writes: for a monthly
budget whose current period has not been touched yet it reports the cadence
limit with `spent = 0`.

Timezones are IANA names validated against a tz database bundled into the
binary (`jiff`, `tzdb-bundle-always`), so the distroless image needs no
`/usr/share/zoneinfo`.

### State tier

Tier 2, unchanged from ADR 0063: one more table in the same Store
(`axond_store_budget_cadence`, SQLite and Postgres, `create_table` or
`ops/postgres/store_budget_cadence_v1.sql`). No Redis, no new dependency for
any existing deployment; a namespace without a cadence row behaves exactly as
before.

## Consequences

- Litvue and other consumers drop the monthly ensure-period job; one
  `PUT …/budget` at workspace create is enough.
- The period is now the gateway's clock in the budget's timezone. Replicas
  with skewed clocks can disagree near midnight on the first of the month;
  both keys are valid periods and both rows are charged correctly, but a
  request may land in the “wrong” month by the skew.
- Historical periods remain `YYYY-MM` strings, which fit the existing
  `[A-Za-z0-9._-]` period charset; nothing downstream needs to change.
- `reserved_microdollars` stays `0` on both budget responses (ADR 0064).
- A `rolling_days` cadence is out of scope; the clean rollover-by-key model
  does not extend to sliding windows.
