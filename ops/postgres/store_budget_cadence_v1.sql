-- Axond Store cadence budget policy (ADR 0063).
--
-- Applied by the gateway when `[storage] backend = "postgres"` and
-- `create_table = true`. Operators who migrate out of band apply this file
-- once per database:
--
--     psql "$AXOND_STORAGE_DSN" -f ops/postgres/store_budget_cadence_v1.sql
--
-- A cadence policy selects the effective billing period for admission. Monthly
-- spend rows are inserted lazily into axond_store_budget on the first request
-- of each period; the policy row stores the namespace-level cadence limit and
-- timezone.

CREATE TABLE IF NOT EXISTS axond_store_budget_cadence (
    namespace           text        PRIMARY KEY NOT NULL,
    cadence             text        NOT NULL,
    limit_microdollars  bigint      NOT NULL,
    timezone            text        NOT NULL DEFAULT 'UTC'
);
