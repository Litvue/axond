-- Axond Store budget ledger (ADR 0063 / ADR 0010).
--
-- Applied by the gateway when `[storage] backend = "postgres"` and
-- `create_table = true`. Operators who migrate out of band apply this file
-- once per database:
--
--     psql "$AXOND_STORAGE_DSN" -f ops/postgres/store_budget_v1.sql
--
-- Spend is cumulative per `(namespace, period)`. Reservations are short-lived
-- holds; a reserve reclaims expired ones for that key before it decides.
-- The spend row is the lock (`SELECT ... FOR UPDATE`) that serializes
-- admissions across replicas.

CREATE TABLE IF NOT EXISTS axond_budget (
    namespace           text        NOT NULL,
    period              text        NOT NULL,
    limit_microdollars  bigint      NOT NULL,
    spent_microdollars  bigint      NOT NULL DEFAULT 0,
    PRIMARY KEY (namespace, period)
);

CREATE TABLE IF NOT EXISTS axond_budget_active (
    namespace text PRIMARY KEY NOT NULL,
    period    text NOT NULL
);

CREATE TABLE IF NOT EXISTS axond_budget_reservation (
    id                  text        PRIMARY KEY,
    namespace           text        NOT NULL,
    period              text        NOT NULL,
    amount_microdollars bigint      NOT NULL,
    expires_at          timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS axond_budget_reservation_scope_idx
    ON axond_budget_reservation (namespace, period, expires_at);
