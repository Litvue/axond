-- Namespace incarnation for Store DELETE (ADR 0063).
--
-- Applied by the gateway when `[storage] backend = "postgres"` and
-- `create_table = true`, after `store_budget_v1.sql`. Operators who
-- migrate out of band apply this file once per database:
--
--     psql "$AXOND_STORAGE_DSN" -f ops/postgres/store_namespace_incarnation_v1.sql
--
-- Delete bumps `n` and keeps reservation rows so a late settle cannot
-- charge a later generation of the same id. Expired holds become a
-- compact (id, incarnation) tombstone for the same reason.
-- `create_table = false` only probes these objects and the reservation
-- `incarnation` column; it does not CREATE or ALTER.

CREATE TABLE IF NOT EXISTS axond_namespace_incarnation (
    id text PRIMARY KEY NOT NULL,
    n  bigint NOT NULL
);

ALTER TABLE axond_store_budget_reservation
    ADD COLUMN IF NOT EXISTS incarnation bigint NOT NULL DEFAULT 1;

CREATE TABLE IF NOT EXISTS axond_store_budget_reservation_tombstone (
    id          text PRIMARY KEY NOT NULL,
    incarnation bigint NOT NULL
);
