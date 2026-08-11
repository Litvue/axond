-- Axond usage schema, version 1 (docs/adr/0009-durable-usage-sinks.md).
--
-- Apply once per database before enabling a `kind = "postgres"` usage sink:
--
--     psql "$AXOND_USAGE_POSTGRES_DSN" -f crates/gateway/sql/usage_v1.sql
--
-- The gateway can apply this itself with `create_table = true`, which runs the
-- same statements (with the table name substituted). Never edit this file in
-- place once it has been applied: a change to the row shape is a new
-- `usage_v<N>.sql` plus a bump of `schema_version`.
--
-- Every row carries `schema_version`, so a table may hold rows written by more
-- than one gateway version. Readers filter or branch on it rather than assuming.
--
-- Rows are written at least once: a batch whose commit outcome is unknown is
-- retried, so a reader that must be exact deduplicates on
-- `(request_id, recorded_at)`. `request_id` is unique per process, not globally,
-- so it is indexed but not constrained.

CREATE TABLE IF NOT EXISTS axond_usage (
    id                 bigserial PRIMARY KEY,
    -- Version of this row's shape; 1 for every column below.
    schema_version     integer     NOT NULL,
    request_id         text        NOT NULL,
    -- W3C trace id of the caller's trace, when the request was traced.
    trace_id           text,
    namespace          text        NOT NULL,
    subject            text        NOT NULL,
    -- Alias the caller asked for, then the provider and concrete model that served it.
    model              text        NOT NULL,
    target_provider    text        NOT NULL,
    target_model       text        NOT NULL,
    -- platform | byok, and the non-secret label of the credential in the pool.
    credential_source  text        NOT NULL,
    credential_id      text        NOT NULL,
    -- ok | upstream_error | client_cancelled | partial | rejected
    status             text        NOT NULL,
    input_tokens       bigint      NOT NULL,
    output_tokens      bigint      NOT NULL,
    -- Reserved: the canonical record does not carry these yet, so they are
    -- nullable and left NULL. Populating them later is not a version bump.
    reasoning_tokens   bigint,
    cache_read_tokens  bigint,
    cache_write_tokens bigint,
    cost_microdollars  bigint      NOT NULL,
    catalog_version    bigint      NOT NULL,
    latency_ms         bigint      NOT NULL,
    -- Upstream target attempts across the alias's targets; retry count is one
    -- less, and 1 means the first target served.
    attempts           bigint      NOT NULL,
    -- `recorded_at` is when the gateway settled the request; `started_at` is
    -- that instant minus `latency_ms`. Neither includes the sink's own batching
    -- delay.
    started_at         timestamptz NOT NULL,
    recorded_at        timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS axond_usage_recorded_at_idx
    ON axond_usage (recorded_at DESC);

CREATE INDEX IF NOT EXISTS axond_usage_namespace_recorded_at_idx
    ON axond_usage (namespace, recorded_at DESC);

CREATE INDEX IF NOT EXISTS axond_usage_request_id_idx
    ON axond_usage (request_id);
