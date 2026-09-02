-- Axond Store budget ledger (ADR 0063 / ADR 0010).
--
-- Applied by the gateway when `[storage] backend = "postgres"` and
-- `create_table = true`. Operators who migrate out of band apply this file
-- once per database:
--
--     psql "$AXOND_STORAGE_DSN" -f ops/postgres/store_budget_v1.sql
--
-- Tables are `axond_store_budget*` so they never collide with the withdrawn
-- `[budget] backend = "postgres"` ledger (`axond_budget` /
-- `axond_budget_reservation` from budget_v1.sql: PK `(namespace, subject)`,
-- no period). A database that already has those leftover tables keeps them;
-- spend is not migrated (subject vs period). The gateway still creates these
-- tables and boots.
--
-- An earlier draft of this file used the withdrawn names with a `period`
-- column. Connect may RENAME those leftover relations to `axond_store_budget*`
-- before probing, including when this file has already created empty new
-- tables and the draft still has spend (empty new relations are dropped
-- first). Incomplete dest tables with rows are a boot error, not a partial
-- rename. That needs table-rename privilege; migration-only roles should run
-- the rename out of band before boot.
--
-- Spend is cumulative per `(namespace, period)`. Reservations are short-lived
-- holds; a reserve reclaims expired ones for that key before it decides.
-- The spend row is the lock (`SELECT ... FOR UPDATE`) that serializes
-- admissions across replicas.

CREATE TABLE IF NOT EXISTS axond_store_budget (
    namespace           text        NOT NULL,
    period              text        NOT NULL,
    limit_microdollars  bigint      NOT NULL,
    spent_microdollars  bigint      NOT NULL DEFAULT 0,
    PRIMARY KEY (namespace, period)
);

CREATE TABLE IF NOT EXISTS axond_store_budget_active (
    namespace text PRIMARY KEY NOT NULL,
    period    text NOT NULL
);

CREATE TABLE IF NOT EXISTS axond_store_budget_reservation (
    id                  text        PRIMARY KEY,
    namespace           text        NOT NULL,
    period              text        NOT NULL,
    amount_microdollars bigint      NOT NULL,
    expires_at          timestamptz NOT NULL,
    -- Generation of `axond_namespace_incarnation.n` at reserve. Delete bumps
    -- that counter and keeps this row so settle cannot charge a later id.
    incarnation         integer     NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS axond_store_budget_reservation_scope_idx
    ON axond_store_budget_reservation (namespace, period, expires_at);

-- CREATE TABLE IF NOT EXISTS does not add columns to an existing relation.
ALTER TABLE axond_store_budget_reservation
    ADD COLUMN IF NOT EXISTS incarnation integer NOT NULL DEFAULT 1;
