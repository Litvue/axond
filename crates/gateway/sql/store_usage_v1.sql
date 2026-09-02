-- Axond Store usage index (ADR 0063).
--
-- Applied by the gateway when `[storage] backend = "postgres"` and
-- `create_table = true`. Operators who migrate out of band apply this file
-- once per database:
--
--     psql "$AXOND_STORAGE_DSN" -f ops/postgres/store_usage_v1.sql
--
-- This is the management-API index for `GET /api/v1/namespaces/{ns}/usage`,
-- not the operator-facing warehouse table in usage_v2.sql. `request_id` is
-- the idempotency key so at-least-once delivery does not double-count.

CREATE TABLE IF NOT EXISTS axond_store_usage (
    request_id          text        PRIMARY KEY,
    namespace           text        NOT NULL,
    period              text,
    model               text        NOT NULL,
    status              text        NOT NULL,
    cost_microdollars   bigint
);

CREATE INDEX IF NOT EXISTS axond_store_usage_ns_period_idx
    ON axond_store_usage (namespace, period);
