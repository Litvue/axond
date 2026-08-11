-- Axond budget schema, version 2 — the namespace-wide spend cap
-- (docs/adr/0010-shared-budget-backends-and-charging-policy.md).
--
-- Additive on top of `budget_v1.sql`, which is unchanged: v1 keeps the
-- per-`(namespace, subject)` spend and reservation tables, and v2 adds the
-- namespace totals `namespace_limit_microdollars` is enforced against. Apply v1
-- first, then this file, once per database before enabling
-- `namespace_limit_microdollars` under `[budget]`:
--
--     psql "$AXOND_BUDGET_POSTGRES_DSN" -f ops/postgres/budget_v1.sql
--     psql "$AXOND_BUDGET_POSTGRES_DSN" -f ops/postgres/budget_v2.sql
--
-- The gateway can apply both itself with `create_table = true`, which runs the
-- same statements (with the table names substituted). Never edit this file in
-- place once it has been applied: a change to the row shape is a new
-- `budget_v<N>.sql`.
--
-- Applying it is safe on a live v1 database and does not reset anything: the
-- backfill at the bottom seeds each namespace's total from the subject rows
-- already there, so enabling the cap does not hand every tenant a fresh budget.
-- It is also idempotent — a namespace row that already exists is left alone,
-- because from then on the gateway maintains it on the request path.

-- Settled spend for a whole namespace: every subject in it, combined. A separate
-- table rather than a view, because the row is what the gateway locks (and locks
-- *first*, before the subject row, so a reserve and a settlement on one
-- namespace can never deadlock against each other).
CREATE TABLE IF NOT EXISTS axond_budget_namespace (
    namespace          text        PRIMARY KEY,
    -- Cumulative measured spend, in micro-dollars. Integer, so no float drift.
    spent_microdollars bigint      NOT NULL DEFAULT 0,
    updated_at         timestamptz NOT NULL DEFAULT now()
);

-- A namespace-scoped reserve reclaims the expired holds of *every* subject in
-- the namespace, which this index serves. The v1 `(namespace, subject)` index
-- cannot: the cleanup predicate is namespace plus deadline.
CREATE INDEX IF NOT EXISTS axond_budget_reservation_namespace_expires_idx
    ON axond_budget_reservation (namespace, expires_at);

-- Seed namespace totals from the spend already recorded per subject. `DO
-- NOTHING` keeps a re-run (or the gateway's own `create_table = true` boot path)
-- from overwriting totals the request path has since moved on from.
INSERT INTO axond_budget_namespace (namespace, spent_microdollars)
SELECT namespace, SUM(spent_microdollars)::bigint
FROM axond_budget
GROUP BY namespace
ON CONFLICT (namespace) DO NOTHING;
