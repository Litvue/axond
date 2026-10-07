/**
 * Shipped usage DDL. The bytes match `ops/postgres/usage_v*.sql`. A test fails
 * when the two copies drift. The compiled binary and the container image do
 * not carry the `ops/` tree, so the writer embeds the text.
 */

export const USAGE_V2_SQL = `-- Axond usage schema, version 2 (docs/usage-schema.md).
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
`;

export const USAGE_ADDITIVE_SQL = [
  `-- Additive usage schema migration for the v1 row shape.
--
-- Fresh installations apply usage_v1.sql first, then this file and any later
-- usage_v1_<sequence>_<name>.sql files in filename order. Existing
-- installations apply this file before deploying a gateway that writes
-- signer_kid. Replace axond_usage below when the usage sink uses a custom table.
--
-- This column is nullable and does not change UsageRecord::SCHEMA_VERSION.
ALTER TABLE axond_usage
    ADD COLUMN IF NOT EXISTS signer_kid text;
`,
  `-- Additive usage schema migration for the v2 row shape.
--
-- Fresh installations apply usage_v2.sql first, then this file and any later
-- usage_v2_<sequence>_<name>.sql files in filename order. Existing
-- installations apply this file before deploying a gateway that writes the
-- price-book identity. Replace axond_usage below when the usage sink uses a
-- custom table.
--
-- These columns name the immutable pricing a row was charged against: the
-- approved price-book resource version, the checksum of its body, and the
-- catalogue content it was approved against (docs/adr/0056-request-path-pricing.md).
-- They are NULL for a request the file configuration priced, and for every row
-- written before they existed.
--
-- The columns are nullable and do not change UsageRecord::SCHEMA_VERSION.
ALTER TABLE axond_usage
    ADD COLUMN IF NOT EXISTS price_book text,
    ADD COLUMN IF NOT EXISTS price_book_checksum text,
    ADD COLUMN IF NOT EXISTS price_catalog text;
`,
  `-- Additive usage schema migration for the v2 row shape.
--
-- Fresh installations apply usage_v2.sql first, then every usage_v2_<sequence>
-- file in filename order. Existing installations apply this file before
-- deploying a gateway that records unpriced-allow traffic with a NULL cost.
-- Replace axond_usage below when the usage sink uses a custom table.
--
-- Unpriced models admitted with \`unpriced_models = allow\` record
-- cost_microdollars as NULL. Priced rows still write an integer.
ALTER TABLE axond_usage
    ALTER COLUMN cost_microdollars DROP NOT NULL;
`,
  `-- Additive usage schema migration for the v2 row shape.
--
-- Fresh installations apply usage_v2.sql first, then every usage_v2_<sequence>
-- file in filename order. Existing installations apply this file before
-- deploying a gateway that records the active budget period at admission.
-- Replace axond_usage below when the usage sink uses a custom table.
--
-- \`period\` is the namespace's active budget period at admission (ADR 0063).
-- It is nullable and does not change UsageRecord::SCHEMA_VERSION.
ALTER TABLE axond_usage
    ADD COLUMN IF NOT EXISTS period text;
`,
] as const;

const ADDITIVE_COLUMNS: readonly (readonly [string, string])[] = [
  ["signer_kid", "usage_v1_001_add_signer_kid.sql"],
  ["price_book", "usage_v2_001_add_price_identity.sql"],
  ["price_book_checksum", "usage_v2_001_add_price_identity.sql"],
  ["price_catalog", "usage_v2_001_add_price_identity.sql"],
  ["period", "usage_v2_003_add_period.sql"],
];

const DEFAULT_TABLE = "axond_usage";
const INDEX_PLACEHOLDER = "\u0001index_prefix\u0001";

/** The shipped DDL with the configured table substituted in. */
export function usageSchemaDdl(table: string): string {
  const parts = [retarget(USAGE_V2_SQL, table)];
  for (const additive of USAGE_ADDITIVE_SQL) {
    parts.push(retarget(additive, table));
  }
  return parts.join("\n");
}

function retarget(ddl: string, table: string): string {
  const indexPrefix = table.split(".").pop() ?? table;
  return ddl
    .replaceAll(`${DEFAULT_TABLE}_`, INDEX_PLACEHOLDER)
    .replaceAll(DEFAULT_TABLE, table)
    .replaceAll(INDEX_PLACEHOLDER, `${indexPrefix}_`);
}

/**
 * Boot refusal when the table is behind this writer. Column order in the
 * message follows the bound columns; the files follow migration order.
 */
export function usageMigrationGap(missing: readonly string[]): string | null {
  if (missing.length === 0) {
    return null;
  }
  const files: string[] = [];
  for (const [column, file] of ADDITIVE_COLUMNS) {
    if (missing.includes(column) && !files.includes(file)) {
      files.push(file);
    }
  }
  const remedy =
    files.length === 0
      ? "recreate it from ops/postgres/usage_v2.sql"
      : `apply ops/postgres/${files.join(", then ops/postgres/")} before deploying this writer`;
  return `usage table is missing column(s) ${missing.join(", ")}: ${remedy}`;
}

export const USAGE_NOT_NULL_COST =
  "usage table column cost_microdollars is still NOT NULL: apply ops/postgres/usage_v2_002_nullable_cost.sql before deploying this writer";
