-- Axond Store provider model discovery cache (ADR 0063).
--
-- Applied by the gateway when `[storage] backend = "postgres"` and
-- `create_table = true`. Operators who migrate out of band apply this file
-- once per database:
--
--     psql "$AXOND_STORAGE_DSN" -f ops/postgres/store_provider_models_v1.sql
--
-- One row per configured `[[provider]] id`. `models` is the upstream
-- `GET /models` `data` array (OpenAI-compatible; Anthropic/xAI use the same
-- shape). `fetched_at` is RFC3339 of the last successful fetch; NULL if none
-- has succeeded. `stale` is true when the last attempt failed: the last-good
-- `models` (or an empty array) are still returned.
--
-- Azure Foundry's data-plane listing omits deployments; this table still
-- stores whatever `GET /models` returned.
--
-- Refresh is a background timer in `serve`, never the inference path.

CREATE TABLE IF NOT EXISTS axond_store_provider_models (
    provider    text PRIMARY KEY NOT NULL,
    fetched_at  text,
    stale       boolean NOT NULL,
    models      jsonb NOT NULL
);
