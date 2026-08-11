-- Axond usage schema, version 2 (docs/usage-schema.md).
--
-- Apply after usage_v1.sql and usage_v1_001_add_signer_kid.sql for an existing
-- table, or use this file for a fresh installation before enabling a
-- kind = "postgres" usage sink:
--
--     psql "$AXOND_USAGE_POSTGRES_DSN" -f ops/postgres/usage_v2.sql
--
-- Version 2 makes the cache counters part of the canonical usage record and
-- defines input_tokens as the non-cached prompt remainder.

CREATE TABLE IF NOT EXISTS axond_usage (
    id                 bigserial PRIMARY KEY,
    schema_version     integer     NOT NULL,
    request_id         text        NOT NULL,
    trace_id           text,
    namespace          text        NOT NULL,
    subject            text        NOT NULL,
    signer_kid         text,
    model              text        NOT NULL,
    target_provider    text        NOT NULL,
    target_model      text        NOT NULL,
    credential_source  text        NOT NULL,
    credential_id      text        NOT NULL,
    status             text        NOT NULL,
    input_tokens       bigint      NOT NULL,
    output_tokens      bigint      NOT NULL,
    reasoning_tokens   bigint,
    cache_read_tokens  bigint,
    cache_write_tokens bigint,
    cost_microdollars  bigint      NOT NULL,
    catalog_version    bigint      NOT NULL,
    latency_ms         bigint      NOT NULL,
    attempts           bigint      NOT NULL,
    started_at         timestamptz NOT NULL,
    recorded_at        timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS axond_usage_recorded_at_idx
    ON axond_usage (recorded_at DESC);

CREATE INDEX IF NOT EXISTS axond_usage_namespace_recorded_at_idx
    ON axond_usage (namespace, recorded_at DESC);

CREATE INDEX IF NOT EXISTS axond_usage_request_id_idx
    ON axond_usage (request_id);
