import { GatewayFailure } from "./errors.ts";

import { validateGlob } from "./glob.ts";
import {
  asRecord,
  WITHDRAWN_SECTIONS,
  OVERRIDE_KEYS,
  envMark,
  ENV_WHOLE,
  ENV_MERGED,
  ENV_ELEMENT,
  type SectionShape,
  SECTIONS_BEFORE_SERVER,
  rejectSectionShapes,
  POSITIONAL_BEFORE_SERVER,
  positionalFields,
  configLoad,
  envFound,
  ENV_LOC,
  isFloatToken,
  foundPhrase,
  realizeSequenceElements,
  topLevelAssignment,
  nthArrayToken,
  positionalValue,
  POSITIONAL_AFTER_SERVER,
  structFromSequence,
  USAGE_INDEX_FIELDS,
  sectionFieldLiteral,
  DEFAULT_TRANSPORT,
  type PositionalField,
  projectNestedStructs,
  coerceEnvInt,
  positionalInt,
  envMarks,
  type EnvLeaf,
  assignmentValue,
  rustFloatText,
  U32_MAX,
  TARGET_UINT_MAX,
  markEnv,
  type SecretReader,
  figmentEnvValue,
  arrayElements,
  type FigmentScalar,
  scalarToken,
} from "./config-values.ts";
import { I64_MAX, I64_MIN } from "./config-toml.ts";

export function validatePriceBook(parsed: Record<string, unknown>, providers: { id: string }[]): void {
  for (const entry of asArray(parsed["price"])) {
    const row = asRecord(entry) ?? {};
    const provider = typeof row["provider"] === "string" ? row["provider"] : "";
    const model = typeof row["model"] === "string" ? row["model"] : "";
    if (provider.trim().length === 0 || model.trim().length === 0) {
      throw configError("`[[price]]` requires a non-empty `provider` and `model` glob");
    }
    if (!providers.some((item) => item.id === provider)) {
      throw configError("`[[price]]` references undefined provider `" + provider + "`");
    }
    const stars = [...model].filter((char) => char === "*").length;
    const star = model.indexOf("*");
    const valid = stars === 0 || model === "*" || (stars === 1 && (star === 0 || star === model.length - 1));
    if (!valid || model.length === 0) {
      throw configError(
        "`[[price]]` model glob `" + model + "` is invalid: use an exact id, `prefix*`, `*suffix`, or `*`",
      );
    }
  }
}

export function validateBlocklist(parsed: Record<string, unknown>): void {
  const blocklistRaw = asRecord(parsed["blocklist"]);
  for (const pattern of asArray(blocklistRaw?.["models"])) {
    if (typeof pattern !== "string") {
      throw configError("blocklist models must be strings");
    }
    validateGlob(pattern);
  }
}

export const CATALOG_REFRESH_INTERVAL_SECONDS = 21_600;

export const CATALOG_REFRESH_TIMEOUT_SECONDS = 60;

export const CATALOG_RETRY_INITIAL_SECONDS = 60;

export const CATALOG_RETRY_MAX_SECONDS = 3_600;

export const CATALOG_CONNECT_TIMEOUT_MS = 10_000;

export const CATALOG_OPERATION_TIMEOUT_MS = 30_000;

export const CATALOG_MAX_PAYLOAD_BYTES = 64 * 1024 * 1024;

export function rejectWithdrawn(parsed: Record<string, unknown>): void {
  const present = WITHDRAWN_SECTIONS.filter((key) => parsed[key] !== undefined).map((key) => `\`${key}\``);
  if (present.length === 0) {
    return;
  }
  const verb = present.length === 1 ? "is" : "are";
  throw configError(
    `${present.join(", ")} ${verb} withdrawn (ADR 0063): remove it. Axond is a store-backed gateway with one static \`[[gateway_key]]\`.`,
  );
}

export function rejectCollisions(parsed: Record<string, unknown>): void {
  const names: string[] = [];
  for (const entry of asArray(parsed["credential"])) {
    const env = asRecord(entry)?.["env"];
    if (typeof env === "string") {
      names.push(env);
    }
  }
  for (const entry of asArray(parsed["gateway_key"])) {
    const env = asRecord(entry)?.["env"];
    if (typeof env === "string") {
      names.push(env);
    }
  }
  const dsn = asRecord(parsed["storage"])?.["dsn_env"];
  if (typeof dsn === "string") {
    names.push(dsn);
  }
  for (const name of names) {
    const field = name.startsWith("AXOND_") ? name.slice("AXOND_".length).toLowerCase() : null;
    if (field !== null && (OVERRIDE_KEYS as readonly string[]).includes(field)) {
      throw configError(
        `a secret names the env var \`${name}\`, which the \`AXOND_\` override layer reads as the \`${field}\` config key rather than as a reference. Name the variable outside the \`AXOND_<section>\` shape`,
      );
    }
  }
}

function replacedByEnv(row: object): boolean {
  return (
    envMark(row, ENV_WHOLE) !== undefined ||
    envMark(row, ENV_MERGED) !== undefined ||
    envMark(row, ENV_ELEMENT) !== undefined
  );
}

export function configError(message: string): GatewayFailure {
  return new GatewayFailure("bad_request", 400, message);
}

export function asArray(value: unknown): unknown[] {
  if (value === undefined) {
    return [];
  }
  return Array.isArray(value) ? value : [value];
}

export function stringField(row: Record<string, unknown>, key: string): string {
  const value = row[key];
  if (typeof value !== "string" || value.length === 0) {
    throw configError(`missing \`${key}\``);
  }
  return value;
}

export function asUint(value: number | bigint): bigint {
  return typeof value === "bigint" ? value : BigInt(value);
}

/** Keys Figment visits after `server`. A bad bind beats these. */
export const SECTIONS_AFTER_SERVER: ReadonlyArray<readonly [string, SectionShape]> = [
  ["shutdown", { form: "struct", name: "Shutdown" }],
  ["storage", { form: "struct", name: "StorageConfig" }],
  ["transport", { form: "struct", name: "Transport" }],
  ["usage_journal", { form: "struct", name: "UsageJournalConfig" }],
  ["usage_sink", { form: "seq", element: "UsageSinkConfigWire" }],
];

/**
 * Figment finishes one top-level key before the next. A `failover = [1.5]`
 * fill must not hide an earlier admission float, blocklist string, or
 * credential weight.
 */
function visitBeforeServer(toml: string, parsed: Record<string, unknown>, key: string): void {
  const shape = SECTIONS_BEFORE_SERVER.find((entry) => entry[0] === key);
  if (shape) {
    rejectSectionShapes(toml, parsed, [shape]);
  }
  const positional = POSITIONAL_BEFORE_SERVER.find((entry) => entry[0] === key);
  if (positional) {
    projectPositional(toml, parsed, [positional]);
  }
}

export function rejectExtractTypes(toml: string, parsed: Record<string, unknown>): void {
  // Figment visits top-level keys in sorted order and finishes each one,
  // including a sequence-to-struct fill, before the next key.
  visitBeforeServer(toml, parsed, "admission");
  const admission = asRecord(parsed["admission"]) ?? {};
  realizePositionalEnv(admission, positionalFields("admission") ?? []);
  for (const [key, expected, fallback] of [
    ["max_in_flight", "usize", 1024],
    ["max_in_flight_per_tenant", "usize", 0],
    ["max_in_flight_settlements", "usize", 64],
    ["max_in_flight_streams", "usize", 512],
    ["max_output_tokens", "u64", 200_000],
    ["max_pending_settlements", "usize", 0],
    ["max_prompt_tokens", "u64", 1_000_000],
    ["max_request_bytes", "usize", 2 * 1024 * 1024],
    ["max_stream_bytes", "u64", 64 * 1024 * 1024],
    ["max_stream_duration_ms", "u64", 3_600_000],
    ["max_tenants", "usize", 1024],
    ["queue_capacity", "usize", 0],
    ["queue_wait_ms", "u64", 0],
    ["settlement_queue_wait_ms", "u64", 10_000],
    ["settlement_timeout_ms", "u64", 10_000],
  ] as const) {
    readTypedInt(toml, "admission", admission, key, expected, fallback);
  }
  visitBeforeServer(toml, parsed, "blocklist");
  const blocklist = asRecord(parsed["blocklist"]);
  if (blocklist) {
    rejectBlocklistExtract(toml, blocklist);
  }
  visitBeforeServer(toml, parsed, "catalog");
  const catalog = asRecord(parsed["catalog"]);
  if (catalog) {
    rejectCatalogExtract(toml, catalog);
  }
  visitBeforeServer(toml, parsed, "credential");
  asArray(parsed["credential"]).forEach((entry, index) => {
    rejectEnvElement(parsed["credential"], index, "Credential");
    const row = asRecord(entry);
    if (!row) {
      return;
    }
    for (const key of ["env", "id", "namespace", "provider"] as const) {
      readEntryString(toml, "credential", index, row, key);
    }
    readTypedInt(
      toml,
      "credential",
      row,
      "weight",
      "u32",
      1,
      `default.credential.${index}.weight`,
      arrayEntryLiteral(toml, "credential", index, "weight"),
    );
    // Required keys are checked after present keys, in declaration order.
    for (const key of ["namespace", "provider"] as const) {
      if (!(key in row)) {
        missingField(row, key, `default.credential.${index}`);
      }
    }
  });
  visitBeforeServer(toml, parsed, "credential_pool");
  const pool = asRecord(parsed["credential_pool"]) ?? {};
  realizePositionalEnv(pool, positionalFields("credential_pool") ?? []);
  readTypedInt(toml, "credential_pool", pool, "cooldown_seconds", "u64", 30);
  readTypedInt(toml, "credential_pool", pool, "failure_threshold", "u32", 2);
  readVariant(toml, "credential_pool", pool, "strategy", ["round-robin", "weighted"], "SelectionStrategy");
  visitBeforeServer(toml, parsed, "discovery");
  const discovery = asRecord(parsed["discovery"]) ?? {};
  realizePositionalEnv(discovery, positionalFields("discovery") ?? []);
  readTypedInt(toml, "discovery", discovery, "refresh_interval_seconds", "u64", 300);
  visitBeforeServer(toml, parsed, "failover");
  const failover = asRecord(parsed["failover"]) ?? {};
  realizePositionalEnv(failover, positionalFields("failover") ?? []);
  readTypedInt(toml, "failover", failover, "cooldown_seconds", "u64", 30);
  readTypedInt(toml, "failover", failover, "failure_threshold", "u32", 3);
  readTypedInt(toml, "failover", failover, "max_attempts", "u32", 3);
  readTypedInt(toml, "failover", failover, "overall_timeout_ms", "u64", 30_000);
  visitBeforeServer(toml, parsed, "gateway_key");
  asArray(parsed["gateway_key"]).forEach((entry, index) => {
    rejectEnvElement(parsed["gateway_key"], index, "GatewayKey");
    const row = asRecord(entry);
    if (!row) {
      return;
    }
    for (const key of ["env", "file", "namespace"] as const) {
      readEntryString(toml, "gateway_key", index, row, key);
    }
    if (!("namespace" in row)) {
      missingField(row, "namespace", `default.gateway_key.${index}`);
    }
  });
  visitBeforeServer(toml, parsed, "namespace");
  asArray(parsed["namespace"]).forEach((entry, index) => {
    rejectEnvElement(parsed["namespace"], index, "Namespace");
    const row = asRecord(entry);
    if (!row) {
      return;
    }
    readEntryBool(toml, "namespace", index, row, "allow_platform_fallback");
    readEntryBool(toml, "namespace", index, row, "default");
    readEntryString(toml, "namespace", index, row, "id");
    if (!("id" in row)) {
      missingField(row, "id", `default.namespace.${index}`);
    }
  });
  visitBeforeServer(toml, parsed, "price");
  asArray(parsed["price"]).forEach((entry, index) => {
    rejectEnvElement(parsed["price"], index, "PriceRule");
    const row = asRecord(entry);
    if (!row) {
      return;
    }
    rejectPriceExtract(toml, index, row);
  });
  visitBeforeServer(toml, parsed, "provider");
  asArray(parsed["provider"]).forEach((entry, index) => {
    rejectEnvElement(parsed["provider"], index, "Provider");
    const row = asRecord(entry);
    if (!row) {
      return;
    }
    rejectProviderExtract(toml, index, row);
  });
}

/** `[catalog]` keys sort `bootstrap` before `create_table` before `source`. */
function rejectCatalogExtract(toml: string, row: Record<string, unknown>): void {
  realizePositionalEnv(row, positionalFields("catalog") ?? []);
  readVariant(toml, "catalog", row, "bootstrap", ["empty", "seed"], "CatalogBootstrap");
  readTypedInt(toml, "catalog", row, "connect_timeout_ms", "u64", CATALOG_CONNECT_TIMEOUT_MS);
  readBool(toml, "catalog", row, "create_table");
  readString(toml, "catalog", row, "dsn_env");
  readTypedInt(toml, "catalog", row, "max_payload_bytes", "usize", CATALOG_MAX_PAYLOAD_BYTES);
  readTypedInt(toml, "catalog", row, "operation_timeout_ms", "u64", CATALOG_OPERATION_TIMEOUT_MS);
  readTypedInt(toml, "catalog", row, "refresh_interval_seconds", "u64", CATALOG_REFRESH_INTERVAL_SECONDS);
  readTypedInt(toml, "catalog", row, "refresh_timeout_seconds", "u64", CATALOG_REFRESH_TIMEOUT_SECONDS);
  readTypedInt(toml, "catalog", row, "retry_initial_seconds", "u64", CATALOG_RETRY_INITIAL_SECONDS);
  readTypedInt(toml, "catalog", row, "retry_max_seconds", "u64", CATALOG_RETRY_MAX_SECONDS);
  readString(toml, "catalog", row, "schema");
  readVariant(toml, "catalog", row, "source", ["none", "models-dev", "seed"], "CatalogSourceBackend");
  readString(toml, "catalog", row, "source_url");
  readVariant(toml, "catalog", row, "store", ["in-memory", "postgres"], "CatalogStoreBackend");
}

function readEntryString(
  toml: string,
  section: string,
  index: number,
  row: Record<string, unknown>,
  key: string,
): void {
  const marked = envMark(row, key);
  if (marked) {
    if (marked.scalar.kind === "string") {
      row[key] = marked.scalar.text;
      return;
    }
    throw configLoad(
      `invalid type: found ${envFound(marked.scalar)}, expected a string for key "${marked.key}"${ENV_LOC}`,
    );
  }
  if (!(key in row)) {
    return;
  }
  const literal = arrayEntryLiteral(toml, section, index, key);
  const value = row[key];
  if (typeof value === "string" && (literal === null || !isFloatToken(literal))) {
    return;
  }
  throw configLoad(
    `invalid type: found ${foundPhrase(value, literal)}, expected a string for key "default.${section}.${index}.${key}"`,
  );
}

function readEntryBool(
  toml: string,
  section: string,
  index: number,
  row: Record<string, unknown>,
  key: string,
): void {
  const marked = envMark(row, key);
  if (marked) {
    if (marked.scalar.kind === "bool") {
      row[key] = marked.scalar.value;
      return;
    }
    throw configLoad(
      `invalid type: found ${envFound(marked.scalar)}, expected a boolean for key "${marked.key}"${ENV_LOC}`,
    );
  }
  if (!(key in row)) {
    return;
  }
  const literal = arrayEntryLiteral(toml, section, index, key);
  const value = row[key];
  if (typeof value === "boolean" && (literal === null || !isFloatToken(literal))) {
    return;
  }
  throw configLoad(
    `invalid type: found ${foundPhrase(value, literal)}, expected a boolean for key "default.${section}.${index}.${key}"`,
  );
}

/**
 * Figment stores tables in a `BTreeMap`, so serde visits keys in sorted order.
 * `server` is before `shutdown`, `storage`, and `transport`. Inside a table,
 * the same order interleaves `deny_unknown_fields` with typed fields.
 */
export function rejectAfterServerExtract(toml: string, parsed: Record<string, unknown>): void {
  extractStruct(toml, parsed, "shutdown", "Shutdown", (row) => rejectShutdownExtract(toml, row));
  extractStruct(toml, parsed, "storage", "StorageConfig", (row) => rejectStorageExtract(toml, row));
  extractStruct(toml, parsed, "transport", "Transport", (row) => rejectTransportExtract(toml, row));
  extractStruct(toml, parsed, "usage_journal", "UsageJournalConfig", (row) => rejectUsageJournalExtract(toml, row));
  realizeSequenceElements(toml, parsed, "usage_sink");
  if (Array.isArray(parsed["usage_sink"])) {
    const literal = topLevelAssignment(toml, "usage_sink");
    parsed["usage_sink"].forEach((entry, index) => {
      rejectEnvElement(parsed["usage_sink"], index, "UsageSinkConfigWire");
      const row = asRecord(entry);
      if (!row) {
        const token = literal === null ? null : nthArrayToken(literal, index);
        throw configLoad(
          `invalid type: found ${foundPhrase(entry, token)}, expected struct UsageSinkConfigWire for key "default.usage_sink.${index}"`,
        );
      }
      rejectUsageSinkExtract(toml, index, row);
    });
  }
}

/** Journal keys are visited even when `backend = "none"` leaves the section inert. */
function rejectUsageJournalExtract(toml: string, row: Record<string, unknown>): void {
  realizePositionalEnv(row, positionalFields("usage_journal") ?? []);
  readVariant(toml, "usage_journal", row, "backend", ["none", "postgres"], "UsageJournalBackend");
  readVariant(toml, "usage_journal", row, "capacity_policy", ["refuse", "drop-oldest"], "UsageCapacityPolicy");
  readTypedInt(toml, "usage_journal", row, "claim_batch", "usize", 1);
  readTypedInt(toml, "usage_journal", row, "connect_timeout_ms", "u64", 5_000);
  readTypedInt(toml, "usage_journal", row, "connections", "usize", 8);
  readString(toml, "usage_journal", row, "consumer");
  readBool(toml, "usage_journal", row, "create_schema");
  readString(toml, "usage_journal", row, "dsn_env");
  readTypedInt(toml, "usage_journal", row, "lease_seconds", "u64", 30);
  readTypedInt(toml, "usage_journal", row, "max_delivery_attempts", "u32", 1);
  readTypedInt(toml, "usage_journal", row, "max_events", "u64", 1);
  readVariant(toml, "usage_journal", row, "on_undurable", ["refuse", "serve"], "UndurablePolicy");
  readTypedInt(toml, "usage_journal", row, "operation_timeout_ms", "u64", 5_000);
  readTypedInt(toml, "usage_journal", row, "poll_interval_ms", "u64", 1_000);
  readTypedInt(toml, "usage_journal", row, "retain_acknowledged_seconds", "u64", 1);
  readString(toml, "usage_journal", row, "schema");
}

/** Sink keys sort `buffer_capacity` before `create_table` before `kind`. */
function rejectUsageSinkExtract(toml: string, index: number, row: Record<string, unknown>): void {
  if ("buffer_capacity" in row) {
    readEnvOrPositionalInt(
      row,
      "buffer_capacity",
      "usize",
      replacedByEnv(row) ? null : arrayEntryLiteral(toml, "usage_sink", index, "buffer_capacity"),
      `default.usage_sink.${index}.buffer_capacity`,
    );
  }
  readEntryBool(toml, "usage_sink", index, row, "create_table");
  readEntryString(toml, "usage_sink", index, row, "dsn_env");
  if ("flush_interval_ms" in row) {
    readEnvOrPositionalInt(
      row,
      "flush_interval_ms",
      "u64",
      replacedByEnv(row) ? null : arrayEntryLiteral(toml, "usage_sink", index, "flush_interval_ms"),
      `default.usage_sink.${index}.flush_interval_ms`,
    );
  }
  readEntryVariant(toml, "usage_sink", index, row, "kind", ["stdout", "postgres", "otlp"], "UsageSinkKind");
  if ("max_batch" in row) {
    readEnvOrPositionalInt(
      row,
      "max_batch",
      "usize",
      replacedByEnv(row) ? null : arrayEntryLiteral(toml, "usage_sink", index, "max_batch"),
      `default.usage_sink.${index}.max_batch`,
    );
  }
  readEntryString(toml, "usage_sink", index, row, "table");
  if (!("kind" in row)) {
    missingField(row, "kind", `default.usage_sink.${index}`);
  }
}

/** `[blocklist] models` is a sequence of strings, visited before `catalog`. */
function rejectBlocklistExtract(toml: string, row: Record<string, unknown>): void {
  const marked = envMark(row, "models");
  if (marked) {
    if (marked.scalar.kind !== "sequence") {
      throw configLoad(
        `invalid type: found ${envFound(marked.scalar)}, expected a sequence for key "${marked.key}"${ENV_LOC}`,
      );
    }
    const items = marked.scalar.items ?? [];
    items.forEach((item, index) => {
      if (item.kind === "string") {
        return;
      }
      throw configLoad(
        `invalid type: found ${envFound(item)}, expected a string for key "${marked.key}.${index}"${ENV_LOC}`,
      );
    });
    row["models"] = items.map((item) => (item.kind === "string" ? item.text : ""));
    return;
  }
  if (!("models" in row)) {
    return;
  }
  positionalValue(
    row["models"],
    { name: "models", kind: "strings" },
    sectionAssignment(toml, "blocklist", "models"),
    "default.blocklist.models",
  );
}

/**
 * Named price keys are visited before the flattened `ModelPrice` integers.
 * A flattened integer that fails reports `default.price.N`, not the field.
 */
function rejectPriceExtract(toml: string, index: number, row: Record<string, unknown>): void {
  readEntryString(toml, "price", index, row, "model");
  readEntryString(toml, "price", index, row, "provider");
  if (!("provider" in row)) {
    missingField(row, "provider", `default.price.${index}`);
  }
  if (!("model" in row)) {
    missingField(row, "model", `default.price.${index}`);
  }
  for (const key of [
    "cache_read_microdollars_per_million",
    "cache_write_microdollars_per_million",
    "input_microdollars_per_million",
    "output_microdollars_per_million",
    "reasoning_microdollars_per_million",
  ]) {
    if (!(key in row)) {
      continue;
    }
    readEnvOrPositionalInt(
      row,
      key,
      "u64",
      replacedByEnv(row) ? null : arrayEntryLiteral(toml, "price", index, key),
      `default.price.${index}`,
    );
  }
  for (const key of ["input_microdollars_per_million", "output_microdollars_per_million"]) {
    if (!(key in row)) {
      missingField(row, key, `default.price.${index}`);
    }
  }
}

/** Provider keys sort `base_url`, `id`, `kind`, then `unpriced_models`. */
function rejectProviderExtract(toml: string, index: number, row: Record<string, unknown>): void {
  readEntryString(toml, "provider", index, row, "base_url");
  readEntryString(toml, "provider", index, row, "id");
  readEntryVariant(
    toml,
    "provider",
    index,
    row,
    "kind",
    ["openai", "anthropic", "openai-compatible"],
    "ProviderKind",
  );
  readEntryVariant(toml, "provider", index, row, "unpriced_models", ["deny", "allow"], "UnpricedModels");
  for (const key of ["id", "kind", "base_url"] as const) {
    if (!(key in row)) {
      missingField(row, key, `default.provider.${index}`);
    }
  }
}

function readEntryVariant(
  toml: string,
  section: string,
  index: number,
  row: Record<string, unknown>,
  key: string,
  variants: readonly string[],
  enumName: string,
): void {
  const marked = envMark(row, key);
  if (marked) {
    if (marked.scalar.kind === "string") {
      row[key] = marked.scalar.text;
      if (variants.includes(marked.scalar.text)) {
        return;
      }
      const list =
        variants.length === 2
          ? `\`${variants[0]}\` or \`${variants[1]}\``
          : `one of ${variants.map((item) => `\`${item}\``).join(", ")}`;
      throw configLoad(
        `unknown variant: found \`${marked.scalar.text}\`, expected \`${list}\` for key "${marked.key}"${ENV_LOC}`,
      );
    }
    throw configLoad(
      `invalid type: found ${envFound(marked.scalar)}, expected enum ${enumName} for key "${marked.key}"${ENV_LOC}`,
    );
  }
  if (!(key in row)) {
    return;
  }
  const literal = arrayEntryLiteral(toml, section, index, key);
  const value = row[key];
  const figmentKey = `default.${section}.${index}.${key}`;
  if (typeof value === "string" && (literal === null || !isFloatToken(literal))) {
    if (variants.includes(value)) {
      return;
    }
    const list =
      variants.length === 2
        ? `\`${variants[0]}\` or \`${variants[1]}\``
        : `one of ${variants.map((item) => `\`${item}\``).join(", ")}`;
    throw configLoad(`unknown variant: found \`${value}\`, expected \`${list}\` for key "${figmentKey}"`);
  }
  throw configLoad(
    `invalid type: found ${foundPhrase(value, literal)}, expected enum ${enumName} for key "${figmentKey}"`,
  );
}

function extractStruct(
  toml: string,
  parsed: Record<string, unknown>,
  key: string,
  structName: string,
  visit: (row: Record<string, unknown>) => void,
): void {
  if (!Object.hasOwn(parsed, key) || parsed[key] === undefined) {
    return;
  }
  if (Array.isArray(parsed[key])) {
    const fields = POSITIONAL_AFTER_SERVER.find((entry) => entry[0] === key)?.[1];
    if (fields) {
      projectPositional(toml, parsed, [[key, fields]]);
    }
    return;
  }
  const record = asRecord(parsed[key]);
  if (!record) {
    const literal = topLevelAssignment(toml, key);
    throw configLoad(
      `invalid type: found ${foundPhrase(parsed[key], literal)}, expected struct ${structName} for key "default.${key}"`,
    );
  }
  visit(record);
}

/**
 * Storage keys sort `backend`, `create_table`, `dsn_env`, `on_unavailable`,
 * `path`, then `usage_index`. A float on `create_table` is reported before a
 * bad `path`.
 */
function rejectStorageExtract(toml: string, storage: Record<string, unknown>): void {
  realizePositionalEnv(storage, positionalFields("storage") ?? []);
  readVariant(toml, "storage", storage, "backend", ["sqlite", "postgres"], "StorageBackend");
  readBool(toml, "storage", storage, "create_table");
  readString(toml, "storage", storage, "dsn_env");
  readVariant(toml, "storage", storage, "on_unavailable", ["deny", "allow"], "StoreUnavailable");
  readString(toml, "storage", storage, "path");
  if (Array.isArray(storage["usage_index"])) {
    storage["usage_index"] = structFromSequence(
      storage["usage_index"] as unknown[],
      USAGE_INDEX_FIELDS,
      sectionFieldLiteral(toml, "storage", "usage_index"),
      "default.storage.usage_index",
    );
  }
  const index = asRecord(storage["usage_index"]);
  if (index) {
    readUsageIndexInt(toml, index, "buffer_capacity", "usize", 1024n);
    readUsageIndexInt(toml, index, "flush_interval_ms", "u64", 50n);
    readUsageIndexInt(toml, index, "max_batch", "usize", 256n);
  }
}

/** Transport keys are visited in sorted order, after `shutdown`. */
function rejectTransportExtract(toml: string, row: Record<string, unknown>): void {
  realizePositionalEnv(row, positionalFields("transport") ?? []);
  const fields = [
    ["buffered_body_timeout_ms", DEFAULT_TRANSPORT.bufferedBodyTimeoutMs],
    ["connect_timeout_ms", DEFAULT_TRANSPORT.connectTimeoutMs ?? 5_000],
    ["max_error_bytes", DEFAULT_TRANSPORT.maxErrorBytes ?? 64 * 1024],
    ["max_response_bytes", DEFAULT_TRANSPORT.maxResponseBytes],
    ["response_header_timeout_ms", DEFAULT_TRANSPORT.responseHeaderTimeoutMs],
    ["stream_idle_timeout_ms", DEFAULT_TRANSPORT.streamIdleTimeoutMs],
    ["stream_terminal_grace_ms", DEFAULT_TRANSPORT.streamTerminalGraceMs ?? 1_000],
  ] as const;
  for (const [key, fallback] of fields) {
    readTypedInt(toml, "transport", row, key, "u64", fallback);
  }
}

/** `[shutdown]` walks sorted keys, so an earlier unknown name wins over a later float. */
function rejectShutdownExtract(toml: string, row: Record<string, unknown>): void {
  realizePositionalEnv(row, positionalFields("shutdown") ?? []);
  for (const field of Object.keys(row).sort()) {
    if (field === "deadline_ms") {
      readTypedInt(toml, "shutdown", row, "deadline_ms", "u64", 15_000);
      continue;
    }
    if (field === "drain_grace_ms") {
      readTypedInt(toml, "shutdown", row, "drain_grace_ms", "u64", 5_000);
      continue;
    }
    if (field === "flush_timeout_ms") {
      readTypedInt(toml, "shutdown", row, "flush_timeout_ms", "u64", 5_000);
      continue;
    }
    const marked = envMark(row, field);
    const figmentKey = marked ? marked.key : `default.shutdown.${field}`;
    const loc = marked ? ENV_LOC : "";
    throw configLoad(
      `unknown field: found \`${field}\`, expected \`one of \`drain_grace_ms\`, \`deadline_ms\`, \`flush_timeout_ms\`\` for key "${figmentKey}"${loc}`,
    );
  }
}

function readString(toml: string, section: string, row: Record<string, unknown>, key: string): void {
  const marked = envMark(row, key);
  if (marked) {
    if (marked.scalar.kind === "string") {
      row[key] = marked.scalar.text;
      return;
    }
    throw configLoad(
      `invalid type: found ${envFound(marked.scalar)}, expected a string for key "${marked.key}"${ENV_LOC}`,
    );
  }
  if (!(key in row)) {
    return;
  }
  const value = row[key];
  const literal = replacedByEnv(row) ? null : sectionFieldLiteral(toml, section, key);
  if (typeof value === "string" && (literal === null || !isFloatToken(literal))) {
    return;
  }
  throw configLoad(
    `invalid type: found ${foundPhrase(value, literal)}, expected a string for key "default.${section}.${key}"`,
  );
}

function readBool(toml: string, section: string, row: Record<string, unknown>, key: string): void {
  const marked = envMark(row, key);
  if (marked) {
    if (marked.scalar.kind === "bool") {
      row[key] = marked.scalar.value;
      return;
    }
    throw configLoad(
      `invalid type: found ${envFound(marked.scalar)}, expected a boolean for key "${marked.key}"${ENV_LOC}`,
    );
  }
  if (!(key in row)) {
    return;
  }
  const value = row[key];
  const literal = replacedByEnv(row) ? null : sectionFieldLiteral(toml, section, key);
  if (typeof value === "boolean" && (literal === null || !isFloatToken(literal))) {
    return;
  }
  throw configLoad(
    `invalid type: found ${foundPhrase(value, literal)}, expected a boolean for key "default.${section}.${key}"`,
  );
}

export function projectPositional(
  toml: string,
  parsed: Record<string, unknown>,
  sections: ReadonlyArray<readonly [string, readonly PositionalField[]]>,
): void {
  for (const [key, fields] of sections) {
    if (!Object.hasOwn(parsed, key) || parsed[key] === undefined) {
      continue;
    }
    const value = parsed[key];
    if (Array.isArray(value)) {
      parsed[key] = structFromSequence(value, fields, topLevelAssignment(toml, key), `default.${key}`);
      continue;
    }
    const record = asRecord(value);
    if (record) {
      projectNestedStructs(toml, key, record, fields);
    }
  }
}

function missingField(row: Record<string, unknown>, field: string, fallback: string): never {
  const marked = envMark(row, ENV_ELEMENT);
  if (marked) {
    throw configLoad(`missing field \`${field}\` for key "${marked.key}"${ENV_LOC}`);
  }
  throw configLoad(`missing field \`${field}\` for key "${fallback}"`);
}

function rejectEnvElement(list: unknown, index: number, structName: string): void {
  if (!Array.isArray(list)) {
    return;
  }
  const marked = envMark(list, String(index));
  if (!marked) {
    return;
  }
  throw configLoad(
    `invalid type: found ${envFound(marked.scalar)}, expected struct ${structName} for key "${marked.key}"${ENV_LOC}`,
  );
}

function readEnvOrPositionalInt(
  row: Record<string, unknown>,
  key: string,
  expected: "u32" | "u64" | "usize",
  token: string | null,
  figmentKey: string,
): void {
  const marked = envMark(row, key);
  if (marked) {
    const integer = coerceEnvInt(marked, expected);
    row[key] = integer <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(integer) : integer;
    return;
  }
  positionalInt(row[key], token, expected, figmentKey);
}

/** A struct filled from an env sequence reports `SECTION.INDEX` in declaration order. */
function realizePositionalEnv(row: Record<string, unknown>, fields: readonly PositionalField[]): void {
  if (!envMark(row, ENV_WHOLE)) {
    return;
  }
  for (const field of fields) {
    if (field.kind === "struct") {
      const nested = asRecord(row[field.name]);
      if (nested && envMark(nested, ENV_WHOLE)) {
        realizePositionalEnv(nested, field.fields);
      }
    }
    const marked = envMark(row, field.name);
    if (!marked) {
      continue;
    }
    coercePositionalMark(row, field, marked);
    envMarks.get(row)?.delete(field.name);
  }
}

function coercePositionalMark(row: Record<string, unknown>, field: PositionalField, marked: EnvLeaf): void {
  const fail = (expected: string): never => {
    throw configLoad(
      `invalid type: found ${envFound(marked.scalar)}, expected ${expected} for key "${marked.key}"${ENV_LOC}`,
    );
  };
  if (field.kind === "int") {
    const integer = coerceEnvInt(marked, field.expected);
    row[field.name] = integer <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(integer) : integer;
    return;
  }
  if (field.kind === "string") {
    if (marked.scalar.kind !== "string") {
      fail("a string");
    }
    row[field.name] = marked.scalar.text;
    return;
  }
  if (field.kind === "bool") {
    if (marked.scalar.kind !== "bool") {
      fail("a boolean");
    }
    row[field.name] = marked.scalar.value;
    return;
  }
  if (field.kind === "enum") {
    if (marked.scalar.kind !== "string") {
      fail(`enum ${field.enumName}`);
    }
    row[field.name] = marked.scalar.text;
    if (!field.variants.includes(marked.scalar.text)) {
      const list =
        field.variants.length === 2
          ? `\`${field.variants[0]}\` or \`${field.variants[1]}\``
          : `one of ${field.variants.map((item) => `\`${item}\``).join(", ")}`;
      throw configLoad(
        `unknown variant: found \`${marked.scalar.text}\`, expected \`${list}\` for key "${marked.key}"${ENV_LOC}`,
      );
    }
    return;
  }
  if (field.kind === "strings") {
    if (marked.scalar.kind !== "sequence") {
      fail("a sequence");
    }
    const items = marked.scalar.items ?? [];
    items.forEach((item, index) => {
      if (item.kind === "string") {
        return;
      }
      throw configLoad(
        `invalid type: found ${envFound(item)}, expected a string for key "${marked.key}.${index}"${ENV_LOC}`,
      );
    });
    row[field.name] = items.map((item) => (item.kind === "string" ? item.text : ""));
    return;
  }
  fail(`struct ${field.structName}`);
}

export function readVariant(
  toml: string,
  section: string,
  row: Record<string, unknown>,
  key: string,
  variants: readonly string[],
  enumName: string,
): void {
  const marked = envMark(row, key);
  if (marked) {
    if (marked.scalar.kind === "string") {
      row[key] = marked.scalar.text;
      if (variants.includes(marked.scalar.text)) {
        return;
      }
      const list =
        variants.length === 2
          ? `\`${variants[0]}\` or \`${variants[1]}\``
          : `one of ${variants.map((item) => `\`${item}\``).join(", ")}`;
      throw configLoad(
        `unknown variant: found \`${marked.scalar.text}\`, expected \`${list}\` for key "${marked.key}"${ENV_LOC}`,
      );
    }
    throw configLoad(
      `invalid type: found ${envFound(marked.scalar)}, expected enum ${enumName} for key "${marked.key}"${ENV_LOC}`,
    );
  }
  if (!(key in row)) {
    return;
  }
  const literal = replacedByEnv(row) ? null : sectionFieldLiteral(toml, section, key);
  const value = row[key];
  const figmentKey = `default.${section}.${key}`;
  if (typeof value === "string" && (literal === null || !isFloatToken(literal))) {
    if (variants.includes(value)) {
      return;
    }
    const list =
      variants.length === 2
        ? `\`${variants[0]}\` or \`${variants[1]}\``
        : `one of ${variants.map((item) => `\`${item}\``).join(", ")}`;
    throw configLoad(`unknown variant: found \`${value}\`, expected \`${list}\` for key "${figmentKey}"`);
  }
  throw configLoad(
    `invalid type: found ${foundPhrase(value, literal)}, expected enum ${enumName} for key "${figmentKey}"`,
  );
}

function arrayEntryLiteral(toml: string, section: string, index: number, key: string): string | null {
  let entry = -1;
  let inEntry = false;
  let found: string | null = null;
  const header = new RegExp(`^\\[\\[${section}\\]\\]\\s*(?:#.*)?$`);
  for (const line of toml.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (header.test(trimmed)) {
      entry += 1;
      inEntry = entry === index;
      continue;
    }
    if (/^\s*\[/.test(line)) {
      inEntry = false;
    }
    if (!inEntry) {
      continue;
    }
    const assigned = assignmentValue(line, key);
    if (assigned !== null) {
      found = assigned;
    }
  }
  return found;
}

/**
 * A value serde would reject while extracting a `u32` or `u64`. Zero stays a
 * number so the caller can report the Rust bound. A u64 above 2^53 stays a
 * bigint: Figment already accepted it, and the only later bound is the
 * caller's `at least 1` check.
 */
export function readTypedInt(
  toml: string,
  section: string,
  row: Record<string, unknown>,
  key: string,
  expected: "u32" | "u64" | "usize",
  fallback: number,
  figmentKey = `default.${section}.${key}`,
  literal: string | null | undefined = undefined,
): number | bigint {
  const marked = envMark(row, key);
  if (marked) {
    const integer = coerceEnvInt(marked, expected);
    const stored = integer <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(integer) : integer;
    row[key] = stored;
    return stored;
  }
  const token = replacedByEnv(row) ? null : literal === undefined ? sectionFieldLiteral(toml, section, key) : literal;
  if (token !== null && isFloatToken(token)) {
    throw configLoad(
      `invalid type: found float \`${rustFloatText(token)}\`, expected ${expected} for key "${figmentKey}"`,
    );
  }
  if (!(key in row)) {
    return fallback;
  }
  const value = row[key];
  const typeError = (found: string): never => {
    throw configLoad(`invalid type: found ${found}, expected ${expected} for key "${figmentKey}"`);
  };
  if (typeof value === "string") {
    typeError(`string ${JSON.stringify(value)}`);
  }
  if (typeof value === "boolean") {
    typeError(`bool ${value}`);
  }
  if (Array.isArray(value)) {
    typeError("sequence");
  }
  if (value !== null && typeof value === "object") {
    typeError("map");
  }
  let integer: bigint;
  if (typeof value === "bigint") {
    integer = value;
  } else if (typeof value === "number" && Number.isInteger(value)) {
    integer = BigInt(value);
  } else if (typeof value === "number") {
    typeError(`float \`${value}\``);
  } else {
    typeError("sequence");
  }
  const max = expected === "u32" ? U32_MAX : TARGET_UINT_MAX;
  if (integer < 0n || (expected === "u32" && integer > U32_MAX)) {
    throw configLoad(`invalid value signed int \`${integer}\`, expected ${expected} for key "${figmentKey}"`);
  }
  if (integer > max) {
    throw configLoad("number too large to fit in target type");
  }
  return integer <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(integer) : integer;
}

function sectionAssignment(toml: string, section: string, key: string): string | null {
  let inSection = false;
  let found: string | null = null;
  const header = new RegExp(`^\\[${section}\\]\\s*(?:#.*)?$`);
  for (const line of toml.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (header.test(trimmed)) {
      inSection = true;
      continue;
    }
    if (inSection && /^\s*\[/.test(line)) {
      inSection = false;
    }
    if (!inSection) {
      continue;
    }
    const assigned = assignmentValue(line, key);
    if (assigned !== null) {
      found = assigned;
    }
  }
  return found;
}

/** Tokio `Semaphore::MAX_PERMITS` (`usize::MAX >> 3` on 64-bit). */
const MAX_USAGE_INDEX_BUFFER = 2305843009213693951n;

const MAX_USAGE_INDEX_BATCH = 4096n;

const MAX_USAGE_INDEX_FLUSH_MS = 86400000n;

/**
 * `[storage.usage_index]` is checked before the backend path. A value serde
 * would reject is a config-load error. A parsed integer outside the Rust
 * bounds is `invalid config`.
 */
export function validateUsageIndex(toml: string, storageRaw: Record<string, unknown>): void {
  const index = asRecord(storageRaw["usage_index"]) ?? {};
  if (!asRecord(storageRaw["usage_index"]) && replacedByEnv(storageRaw)) {
    markEnv(index, ENV_WHOLE, { key: "STORAGE", scalar: { kind: "map" } });
  }
  const nested = asRecord(storageRaw["usage_index"]);
  if (nested) {
    realizePositionalEnv(nested, USAGE_INDEX_FIELDS);
  }
  const buffer = readUsageIndexInt(toml, index, "buffer_capacity", "usize", 1024n);
  const batch = readUsageIndexInt(toml, index, "max_batch", "usize", 256n);
  const flush = readUsageIndexInt(toml, index, "flush_interval_ms", "u64", 50n);
  if (buffer === 0n) {
    throw configError("`[storage.usage_index]` buffer_capacity must be at least 1");
  }
  if (buffer > MAX_USAGE_INDEX_BUFFER) {
    throw configError(
      "`[storage.usage_index]` buffer_capacity (" + buffer + ") must not exceed " + MAX_USAGE_INDEX_BUFFER,
    );
  }
  if (batch === 0n) {
    throw configError("`[storage.usage_index]` max_batch must be at least 1");
  }
  if (batch > MAX_USAGE_INDEX_BATCH) {
    throw configError("`[storage.usage_index]` max_batch (" + batch + ") must not exceed " + MAX_USAGE_INDEX_BATCH);
  }
  if (batch > buffer) {
    throw configError(
      "`[storage.usage_index]` max_batch (" + batch + ") must not exceed buffer_capacity (" + buffer + ")",
    );
  }
  if (flush > MAX_USAGE_INDEX_FLUSH_MS) {
    throw configError(
      "`[storage.usage_index]` flush_interval_ms (" + flush + ") must not exceed " + MAX_USAGE_INDEX_FLUSH_MS + " (24h)",
    );
  }
}

function readUsageIndexInt(
  toml: string,
  index: Record<string, unknown>,
  key: string,
  expected: "usize" | "u64",
  fallback: bigint,
): bigint {
  const marked = envMark(index, key);
  if (marked) {
    const integer = coerceEnvInt(marked, expected);
    index[key] = integer <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(integer) : integer;
    return integer;
  }
  const literal = replacedByEnv(index) ? null : usageIndexLiteral(toml, key);
  if (literal !== null && isFloatToken(literal)) {
    throw configLoad(
      `invalid type: found float \`${rustFloatText(literal)}\`, expected ${expected} for key "default.storage.usage_index.${key}"`,
    );
  }
  if (!(key in index)) {
    return fallback;
  }
  const value = index[key];
  if (typeof value === "string") {
    throw configLoad(
      `invalid type: found string ${JSON.stringify(value)}, expected ${expected} for key "default.storage.usage_index.${key}"`,
    );
  }
  if (typeof value === "boolean") {
    throw configLoad(
      `invalid type: found bool ${value}, expected ${expected} for key "default.storage.usage_index.${key}"`,
    );
  }
  if (Array.isArray(value)) {
    throw configLoad(`invalid type: found sequence, expected ${expected} for key "default.storage.usage_index.${key}"`);
  }
  if (value !== null && typeof value === "object") {
    throw configLoad(`invalid type: found map, expected ${expected} for key "default.storage.usage_index.${key}"`);
  }
  let integer: bigint;
  if (typeof value === "bigint") {
    integer = value;
  } else if (typeof value === "number" && Number.isInteger(value)) {
    integer = BigInt(value);
  } else if (typeof value === "number") {
    throw configLoad(
      `invalid type: found float \`${value}\`, expected ${expected} for key "default.storage.usage_index.${key}"`,
    );
  } else {
    throw configLoad(`invalid type: found sequence, expected ${expected} for key "default.storage.usage_index.${key}"`);
  }
  if (integer < 0n) {
    throw configLoad(
      `invalid value signed int \`${integer}\`, expected ${expected} for key "default.storage.usage_index.${key}"`,
    );
  }
  if (integer > TARGET_UINT_MAX) {
    throw configLoad("number too large to fit in target type");
  }
  return integer;
}

const BIND_KEY = 'default.server.bind';

const BIND_ENV_KEY = "SERVER.BIND";

const BIND_ENV_LOC = " in `AXOND_` environment variable(s)";

/**
 * `server.bind` is a `SocketAddr`. Figment rejects it while extracting, before
 * withdrawn sections and before the store opens. `AXOND_SERVER__BIND` is the
 * env provider, whose key and source differ from the file.
 */
export function readServerBind(toml: string, parsed: Record<string, unknown>, secrets: SecretReader): string {
  const override = serverBindOverride(secrets);
  if (override !== null) {
    return finishBind(figmentEnvValue(override), BIND_ENV_KEY, BIND_ENV_LOC);
  }
  const marked = envMark(parsed, "server");
  if (marked && marked.scalar.kind !== "map" && marked.scalar.kind !== "sequence") {
    throw configLoad(
      `invalid type: found ${envFound(marked.scalar)}, expected struct Server for key "${marked.key}"${ENV_LOC}`,
    );
  }
  if (!Object.hasOwn(parsed, "server") || parsed["server"] === undefined) {
    return "0.0.0.0:8080";
  }
  const server = parsed["server"];
  if (Array.isArray(server)) {
    return bindFromServerArray(toml, server);
  }
  const record = asRecord(server);
  if (!record) {
    const literal = topLevelAssignment(toml, "server");
    throw configLoad(
      `invalid type: found ${foundPhrase(server, literal)}, expected struct Server for key "default.server"`,
    );
  }
  const bindMark = envMark(record, "bind");
  if (bindMark) {
    return finishBind(bindMark.scalar, bindMark.key, ENV_LOC);
  }
  if (!("bind" in record)) {
    return "0.0.0.0:8080";
  }
  return finishBind(tomlBindValue(toml, record["bind"]), BIND_KEY, "");
}

function bindFromServerArray(toml: string, server: unknown[]): string {
  if (server.length === 0) {
    return "0.0.0.0:8080";
  }
  const rhs = topLevelAssignment(toml, "server");
  const token = rhs === null ? null : firstArrayToken(rhs);
  const head = server[0];
  if (typeof head === "string" && (token === null || !isFloatToken(token))) {
    return finishBind({ kind: "string", text: head }, "default.server.0", "");
  }
  return finishBind(scalarFromToken(head, token), "default.server.0", "");
}

function firstArrayToken(rhs: string): string | null {
  return arrayElements(rhs)[0] ?? null;
}

function scalarFromToken(value: unknown, token: string | null): FigmentScalar {
  if (token !== null && isFloatToken(token)) {
    return { kind: "float", text: rustFloatText(token) };
  }
  if (typeof value === "boolean") {
    return { kind: "bool", value };
  }
  if (Array.isArray(value)) {
    return { kind: "sequence" };
  }
  if (value !== null && typeof value === "object") {
    return { kind: "map" };
  }
  if (typeof value === "number" && !Number.isInteger(value)) {
    return { kind: "float", text: String(value) };
  }
  if (typeof value === "bigint" || typeof value === "number") {
    const integer = typeof value === "bigint" ? value : BigInt(value);
    if (integer > I64_MAX || integer < I64_MIN) {
      throw configLoad("number too large to fit in target type");
    }
    return { kind: "int", text: integer.toString() };
  }
  return { kind: "sequence" };
}

function serverBindOverride(secrets: SecretReader): string | null {
  let found: string | null = null;
  for (const [name, value] of secrets.entries()) {
    if (value === undefined || !name.startsWith("AXOND_")) {
      continue;
    }
    const parts = name.slice(6).toLowerCase().split("__");
    if (parts.length === 2 && parts[0] === "server" && parts[1] === "bind") {
      found = value;
    }
  }
  return found;
}

function finishBind(value: FigmentScalar, key: string, loc: string): string {
  const head = `expected socket address for key "${key}"${loc}`;
  switch (value.kind) {
    case "string": {
      const parsed = parseSocketAddr(value.text);
      if (parsed === null) {
        throw configLoad(`invalid socket address syntax for key "${key}"${loc}`);
      }
      return parsed;
    }
    case "bool":
      throw configLoad(`invalid type: found bool ${value.value}, ${head}`);
    case "float":
      throw configLoad(`invalid type: found float \`${value.text}\`, ${head}`);
    case "uint":
      throw configLoad(`invalid type: found unsigned int \`${value.text}\`, ${head}`);
    case "int":
      throw configLoad(`invalid type: found signed int \`${value.text}\`, ${head}`);
    case "sequence":
      throw configLoad(`invalid type: found sequence, ${head}`);
    case "map":
      throw configLoad(`invalid type: found map, ${head}`);
  }
}

function tomlBindValue(toml: string, value: unknown): FigmentScalar {
  const literal = bindLiteral(toml);
  if (literal !== null && isFloatToken(literal)) {
    return { kind: "float", text: rustFloatText(literal) };
  }
  if (typeof value === "string") {
    return { kind: "string", text: value };
  }
  if (typeof value === "boolean") {
    return { kind: "bool", value };
  }
  if (Array.isArray(value)) {
    return { kind: "sequence" };
  }
  if (value !== null && typeof value === "object") {
    return { kind: "map" };
  }
  if (typeof value === "number" && !Number.isInteger(value)) {
    const text = Object.is(value, Infinity)
      ? "inf"
      : Object.is(value, -Infinity)
        ? "-inf"
        : Number.isNaN(value)
          ? "NaN"
          : String(value);
    return { kind: "float", text };
  }
  if (typeof value === "bigint" || typeof value === "number") {
    const integer = typeof value === "bigint" ? value : BigInt(value);
    if (integer > I64_MAX || integer < I64_MIN) {
      throw configLoad("number too large to fit in target type");
    }
    return { kind: "int", text: integer.toString() };
  }
  return { kind: "sequence" };
}

function bindLiteral(toml: string): string | null {
  let inServer = false;
  let found: string | null = null;
  for (const line of toml.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (/^\[server\]\s*(?:#.*)?$/.test(trimmed)) {
      inServer = true;
      continue;
    }
    if (inServer && /^\s*\[/.test(line)) {
      inServer = false;
    }
    const dotted = assignmentValue(line, "server.bind");
    if (dotted !== null) {
      found = scalarToken(dotted);
      continue;
    }
    if (!inServer && !/^\s*server\s*=/.test(line)) {
      continue;
    }
    const inline = /(?:^|[{,]\s*)(?:"bind"|'bind'|bind)\s*=\s*([\s\S]*)$/.exec(trimmed);
    if (inline) {
      found = scalarToken(inline[1] ?? "");
    }
  }
  return found;
}

function parseSocketAddr(text: string): string | null {
  if (text.startsWith("[")) {
    const end = text.indexOf("]");
    if (end <= 1 || text[end + 1] !== ":") {
      return null;
    }
    const host = text.slice(1, end);
    const port = parsePort(text.slice(end + 2));
    if (port === null || !isIpv6(host)) {
      return null;
    }
    return `[${host}]:${port}`;
  }
  const colon = text.lastIndexOf(":");
  if (colon <= 0) {
    return null;
  }
  const host = text.slice(0, colon);
  const port = parsePort(text.slice(colon + 1));
  if (port === null || !isIpv4(host)) {
    return null;
  }
  return `${host}:${port}`;
}

function parsePort(text: string): number | null {
  if (!/^\d{1,10}$/.test(text)) {
    return null;
  }
  const port = Number(text);
  if (!Number.isInteger(port) || port > 65535) {
    return null;
  }
  return port;
}

function isIpv4(host: string): boolean {
  const parts = host.split(".");
  if (parts.length !== 4) {
    return false;
  }
  return parts.every((part) => /^(?:0|[1-9]\d{0,2})$/.test(part) && Number(part) <= 255);
}

function isIpv6(host: string): boolean {
  if (host.includes(".") && (!host.slice(host.lastIndexOf(":") + 1).includes(".") || host.slice(0, host.lastIndexOf(":")).includes("."))) return false;
  if (host.length === 0 || host.includes("%")) {
    return false;
  }
  const halves = host.split("::");
  if (halves.length > 2) {
    return false;
  }
  const expand = (side: string): number | null => {
    if (side.length === 0) {
      return 0;
    }
    const bits = side.split(":");
    if (bits.some((bit) => bit.length === 0)) {
      return null;
    }
    const last = bits[bits.length - 1]!;
    if (last.includes(".")) {
      if (!isIpv4(last)) {
        return null;
      }
      bits.pop();
      if (bits.some((bit) => !/^[0-9a-fA-F]{1,4}$/.test(bit))) {
        return null;
      }
      return bits.length + 2;
    }
    if (bits.some((bit) => !/^[0-9a-fA-F]{1,4}$/.test(bit))) {
      return null;
    }
    return bits.length;
  };
  if (halves.length === 1) {
    return expand(halves[0]!) === 8;
  }
  const left = expand(halves[0]!);
  const right = expand(halves[1]!);
  if (left === null || right === null) {
    return false;
  }
  return left + right < 8;
}

function usageIndexLiteral(toml: string, key: string): string | null {
  let inSection = false;
  let found: string | null = null;
  for (const line of toml.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (/^\[storage\.usage_index\]\s*(?:#.*)?$/.test(trimmed)) {
      inSection = true;
      continue;
    }
    if (inSection && /^\s*\[/.test(line)) {
      inSection = false;
    }
    const names = inSection ? [key, `"${key}"`, `'${key}'`] : [`storage.usage_index.${key}`];
    for (const name of names) {
      const assigned = assignmentValue(line, name);
      if (assigned !== null) {
        found = assigned;
      }
    }
  }
  return found;
}
