## Summary

A cap per `(namespace, subject)` bounds nothing when the caller can choose `sub`
(minted tokens can), so this adds an optional second scope:

```toml
[budget]
backend = "redis"            # or "postgres"
limit_microdollars = 10_000_000
namespace_limit_microdollars = 100_000_000   # everything the namespace spends
```

Omitted, nothing changes — enforcement stays per-subject only, on the same keys
and rows as before. Set, it is accepted only on the backends that can enforce it
*exactly* across replicas: `none` and `in-memory` (and zero) are rejected at
boot, because a cap that is per-replica but *called* namespace-wide reads as a
guarantee it isn't.

Admission requires both caps to fit, and the two scopes are **one logical
reservation** — one id, recorded in both and settled out of both by the same Lua
script (Redis) or transaction (Postgres), so neither scope can be charged or held
without the other. `on_unavailable` still applies to the whole composite
operation (`deny` → `503`, `allow` → served with nothing held anywhere), and a
denial by either cap keeps the existing `429 budget_exceeded` contract; the new
`axond.budget.namespace_denials` counter is what tells an operator *which* cap is
spent. Release, cancellation, failover, and partial streaming settlement are
unchanged: the route path still holds and settles exactly one `Reservation`.

### Redis: a new layout, so a real migration

v1 keys are tagged `{namespace|subject}`, which puts each subject of a namespace
in a different cluster slot — a script spanning both scopes could not run. v2
tags all four keys with `{namespace}` alone:

```
<prefix>:v2:{ns}:subject:<sub>:spent | :reservations
<prefix>:v2:{ns}:namespace:spent     | :reservations
```

The layouts share no keys, so flipping the config would read zero spend
everywhere. Hence `axond budget migrate-redis` (fleet stopped), which carries
each subject counter forward with `if carried > current then SET`— never
lowering one, so a re-run or a resumed run cannot reset a ledger — sums namespace
totals from the subject ledgers, and stamps `<prefix>:layout = v2`. Boot is
fenced in both directions: with the cap set it refuses to start un-migrated *or*
while any v1 key still exists (i.e. while a v1 binary is still writing, which
would have the two layouts each enforcing a share of the traffic); once migrated
it refuses to start *without* the cap.

### Postgres: additive DDL, namespace-then-subject locking

`ops/postgres/budget_v2.sql` is new rather than an edit to the shipped v1 file:
an `axond_budget_namespace` table, a `(namespace, expires_at)` index for
namespace-wide reservation cleanup, and a backfill that seeds each namespace
total from the subject rows already there (`ON CONFLICT DO NOTHING`, so it is
idempotent and does not clobber totals the request path has moved on from).
Custom `table` names substitute as `<table>_namespace`, and `create_table = true`
applies both files. A namespace with spend but no backfilled row fails boot, so
the cap cannot silently start from zero.

Both transactions take the namespace row first and the subject row second, so a
reserve and a settlement on one namespace cannot deadlock:

```
BEGIN
  INSERT namespace row IF NOT EXISTS; SELECT ... FOR UPDATE   -- namespace first
  INSERT subject row  IF NOT EXISTS; SELECT ... FOR UPDATE    -- subject second
  DELETE expired reservations WHERE namespace = $1            -- whole namespace
  SUM held FILTER (subject = $2), SUM held                    -- both scopes
  ROLLBACK if either cap is short                             -- nothing written
  INSERT one reservation row
COMMIT
```

### Not free

A namespace cap is a hot spot by construction: every subject contends on one
spend row/counter and every reserve scans that namespace's live reservations.
Documented in `docs/deployment.md` and ADR 0010 rather than hidden, and it is
why the cap is opt-in. "Exact" continues to mean settled spend plus live
reservations under the existing estimate/TTL semantics, not provider billing.

Docs: configuration, deployment (both migration runbooks, and the no-mixed-
binaries rule), observability, the minted-token guide, `axond.example.toml`, and
ADRs 0010 (amendment), 0016, 0022 (its recorded blocker is now closable).

Verified with `just check` plus the full suite against real Redis and Postgres
(`AXOND_TEST_REQUIRE_SERVICES=1`), including two-replica contention, expiry
reclaim across subjects, release/partial settlement, migration carry-forward, and
both boot fences.

Closes #126
