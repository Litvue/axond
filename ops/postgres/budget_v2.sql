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
-- **Stop and drain the fleet first.** This file is not a live migration: the
-- backfill below reads the subject rows to seed each namespace total, so a
-- settlement committing between that read and the fence would be counted
-- against the subject but not the namespace, leaving a total that is quietly
-- short for the rest of the deployment's life — and no boot check can see that,
-- because the row exists and merely holds too small a number. The whole file
-- therefore runs as one transaction that takes an `EXCLUSIVE` lock on the spend
-- table, so a v1 writer cannot commit inside the window: it blocks, and once
-- this transaction commits the fence below rejects it outright. The lock makes
-- the window safe; stopping the fleet is what keeps that window from being a
-- production outage.
--
-- Re-running it is safe: the namespace row of a namespace that already has one
-- is left alone, because from then on the gateway maintains it on the request
-- path.

BEGIN;

-- Block v1 writers for the duration: `EXCLUSIVE` conflicts with the row locks a
-- settlement takes, so the backfill's read cannot race a commit it will not see.
LOCK TABLE axond_budget IN EXCLUSIVE MODE;

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

-- The fence. Once a database enforces a namespace cap, a replica configured
-- *without* `namespace_limit_microdollars` must not write to it: it would charge
-- the subject row and leave the namespace total untouched, so the namespace cap
-- would under-count for good and the tenant would get more than its ceiling —
-- exactly the bypass the cap exists to prevent. Documentation cannot enforce
-- that (nor can a boot check catch a replica started before this file was
-- applied), so the database refuses the write instead.
--
-- A cap-aware gateway announces itself once per connection with
--   SET axond.budget_namespace_cap = 'on'
-- which is a claim about the *binary and its configuration*, not about the row:
-- only a replica that maintains the namespace total sets it.
CREATE OR REPLACE FUNCTION axond_budget_namespace_fence() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF current_setting('axond.budget_namespace_cap', true) IS DISTINCT FROM 'on' THEN
        RAISE EXCEPTION
            'axond: this database enforces a namespace spend cap, but the session writing %'
            ' did not declare namespace-cap support, so its spend would never reach the'
            ' namespace total', TG_TABLE_NAME
            USING HINT =
                'Set namespace_limit_microdollars under [budget] on every replica sharing this'
                ' database (that is what makes the gateway maintain the namespace total), or'
                ' drop the axond_budget_namespace_fence triggers to return to per-subject-only'
                ' enforcement.';
    END IF;
    RETURN NEW;
END;
$$;

-- Both the write that records spend and the write that holds an estimate, so a
-- mis-configured replica is stopped before it admits traffic rather than at the
-- settlement after it.
DROP TRIGGER IF EXISTS axond_budget_namespace_fence ON axond_budget;
CREATE TRIGGER axond_budget_namespace_fence
    BEFORE INSERT OR UPDATE ON axond_budget
    FOR EACH ROW EXECUTE FUNCTION axond_budget_namespace_fence();

DROP TRIGGER IF EXISTS axond_budget_namespace_fence ON axond_budget_reservation;
CREATE TRIGGER axond_budget_namespace_fence
    BEFORE INSERT ON axond_budget_reservation
    FOR EACH ROW EXECUTE FUNCTION axond_budget_namespace_fence();

-- Seed namespace totals from the spend already recorded per subject. `DO
-- NOTHING` keeps a re-run (or the gateway's own `create_table = true` boot path)
-- from overwriting totals the request path has since moved on from. Under the
-- lock taken above, and with the fence already installed in this transaction,
-- this sum cannot miss a settlement.
INSERT INTO axond_budget_namespace (namespace, spent_microdollars)
SELECT namespace, SUM(spent_microdollars)::bigint
FROM axond_budget
GROUP BY namespace
ON CONFLICT (namespace) DO NOTHING;

COMMIT;
