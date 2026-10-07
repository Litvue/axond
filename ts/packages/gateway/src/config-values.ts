import type { CredentialConfig, PriceRule, ProviderConfig, TransportLimits } from "@axond/sdk";

import { type AdmissionLimits } from "./admission.ts";

import { GatewayFailure } from "./errors.ts";

import { U64_MAX, I64_MIN } from "./config-toml.ts";

export const WITHDRAWN_SECTIONS = [
  "admin_breakglass",
  "admin_oidc",
  "budget",
  "control_plane",
  "convergence",
  "core_middleware",
  "gateway_minting",
  "gateway_token",
  "gateway_token_epoch",
  "gateway_verifier",
  "mode",
  "model",
  "rate_limit",
  "reload",
  "revocation",
  "secret_store",
] as const;

export const OVERRIDE_KEYS = [
  "server",
  "storage",
  "namespace",
  "provider",
  "price",
  "blocklist",
  "credential",
  "credential_pool",
  "failover",
  "transport",
  "shutdown",
  "gateway_key",
  "usage_sink",
  "usage_journal",
  "admission",
  "catalog",
  "discovery",
] as const;

export interface LoadedConfig {
  bind: string;
  storage: {
    backend: "sqlite" | "postgres";
    path?: string;
    /** Env var name from `dsn_env`. The value is resolved separately. */
    dsnEnv?: string;
    /** Set when that env var is present and non-empty. */
    dsn?: string;
    createTable: boolean;
    onUnavailable: "deny" | "allow";
  };
  namespaces: { id: string; default: boolean; allowPlatformFallback: boolean }[];
  providers: ProviderConfig[];
  credentials: CredentialConfig[];
  gatewayKey: string;
  /** Env var name or file path. Usage records use this as the subject. */
  gatewayKeySubject: string;
  gatewayKeySource: "env" | "file";
  gatewayKeyNamespace: string;
  defaultNamespace: string;
  prices: PriceRule[];
  blocklist: string[];
  transport: TransportLimits;
  discoveryIntervalSeconds: number;
  shutdown: { drainGraceMs: number; deadlineMs: number; flushTimeoutMs: number };
  catalog:
    | { source: "none" }
    | { source: "models-dev" | "seed"; sourceUrl: string | null };
  extensionsDir: string | null;
  maxRequestBytes: number;
  /** `0` disables the ceiling. */
  maxPromptTokens: number;
  /** `0` disables the ceiling. */
  maxOutputTokens: number;
  /** `0` disables the total stream lifetime. */
  maxStreamDurationMs: number;
  /** `0` disables the relayed-byte ceiling. */
  maxStreamBytes: number;
  admission: AdmissionLimits;
  credentialPool: {
    strategy: "round-robin" | "weighted";
    failureThreshold: number;
    cooldownSeconds: number;
  };
  /**
   * Configured usage destinations. Empty means the stdout default: the CLI
   * writes one JSON line per record and opens nothing else.
   */
  usageSinks: UsageSinkConfig[];
}

/** One `[[usage_sink]]` entry. Batching fields apply to `postgres` only. */
export interface UsageSinkConfig {
  kind: "stdout" | "postgres" | "otlp";
  /** Env var name holding the Postgres DSN. Null for the other kinds. */
  dsnEnv: string | null;
  /** Destination table, including an optional schema qualifier. */
  table: string;
  createTable: boolean;
  bufferCapacity: number;
  maxBatch: number;
  /** True when the file set `max_batch`, so a value above the buffer fails boot. */
  maxBatchExplicit: boolean;
  flushIntervalMs: number;
}

export const DEFAULT_USAGE_TABLE = "axond_usage";

export const DEFAULT_USAGE_BUFFER = 10_000;

export const DEFAULT_USAGE_BATCH = 500;

export const DEFAULT_USAGE_FLUSH_MS = 1_000;

/** Rows one flush writes. An omitted `max_batch` is clamped to the buffer. */
export function usageBatchSize(sink: UsageSinkConfig): number {
  return Math.min(sink.maxBatch, sink.bufferCapacity);
}

export interface SecretReader {
  env(name: string): string | undefined;
  file(path: string): Promise<string>;
  entries(): Iterable<[string, string | undefined]>;
}

export const DEFAULT_TRANSPORT: TransportLimits = {
  responseHeaderTimeoutMs: 30_000,
  bufferedBodyTimeoutMs: 30_000,
  streamIdleTimeoutMs: 120_000,
  maxResponseBytes: 32 * 1024 * 1024,
  maxErrorBytes: 64 * 1024,
  connectTimeoutMs: 5_000,
  streamTerminalGraceMs: 1_000,
  overallTimeoutMs: 30_000,
  maxAttempts: 3,
};

export const ENV_LOC = " in `AXOND_` environment variable(s)";

export const ENV_WHOLE = "*";

export const ENV_MERGED = "@";

export const ENV_ELEMENT = "";

const SEQUENCE_OVERRIDE_KEYS = [
  "credential",
  "gateway_key",
  "namespace",
  "price",
  "provider",
  "usage_sink",
] as const;

const FLATTENED_PRICE_FIELDS = new Set([
  "cache_read_microdollars_per_million",
  "cache_write_microdollars_per_million",
  "input_microdollars_per_million",
  "output_microdollars_per_million",
  "reasoning_microdollars_per_million",
]);

export type EnvLeaf = { key: string; scalar: FigmentScalar };

export const envMarks = new WeakMap<object, Map<string, EnvLeaf>>();

export function markEnv(row: object, field: string, leaf: EnvLeaf): void {
  let marks = envMarks.get(row);
  if (!marks) {
    marks = new Map();
    envMarks.set(row, marks);
  }
  marks.set(field, leaf);
}

function unmarkEnv(row: object, field: string): void {
  envMarks.get(row)?.delete(field);
}

export function envMark(row: object, field: string): EnvLeaf | undefined {
  return envMarks.get(row)?.get(field);
}

export function envFound(scalar: FigmentScalar): string {
  switch (scalar.kind) {
    case "string":
      return `string ${JSON.stringify(scalar.text)}`;
    case "bool":
      return `bool ${scalar.value}`;
    case "float":
      return `float \`${scalar.text}\``;
    case "uint":
      return `unsigned int \`${scalar.text}\``;
    case "int":
      return `signed int \`${scalar.text}\``;
    case "sequence":
      return "sequence";
    case "map":
      return "map";
  }
}

export function coerceEnvInt(leaf: EnvLeaf, expected: "u32" | "u64" | "usize"): bigint {
  const scalar = leaf.scalar;
  const max = expected === "u32" ? U32_MAX : TARGET_UINT_MAX;
  if (scalar.kind === "int") {
    const integer = BigInt(scalar.text);
    if (integer < 0n || (expected === "u32" && integer > max)) {
      throw configLoad(
        `invalid value signed int \`${scalar.text}\`, expected ${expected} for key "${leaf.key}"${ENV_LOC}`,
      );
    }
    return integer;
  }
  if (scalar.kind === "uint") {
    const integer = BigInt(scalar.text);
    if (integer > max) {
      throw configLoad(
        `invalid value unsigned int \`${scalar.text}\`, expected ${expected} for key "${leaf.key}"${ENV_LOC}`,
      );
    }
    return integer;
  }
  throw configLoad(
    `invalid type: found ${envFound(scalar)}, expected ${expected} for key "${leaf.key}"${ENV_LOC}`,
  );
}

function jsFromEnvScalar(scalar: FigmentScalar): unknown {
  switch (scalar.kind) {
    case "string":
      return scalar.text;
    case "bool":
      return scalar.value;
    case "float":
      return Number(scalar.text);
    case "uint":
    case "int": {
      const integer = BigInt(scalar.text);
      if (integer >= BigInt(Number.MIN_SAFE_INTEGER) && integer <= BigInt(Number.MAX_SAFE_INTEGER)) {
        return Number(integer);
      }
      return integer;
    }
    case "sequence":
      return (scalar.items ?? []).map((item) => jsFromEnvScalar(item));
    case "map":
      return {};
  }
}

function assignEnvLeaf(
  root: Record<string, unknown>,
  parts: string[],
  scalar: FigmentScalar,
  keyPath: string[],
): void {
  if (scalar.kind === "map") {
    let cursor = root;
    for (const part of parts) {
      const row = asRecord(cursor[part]) ?? {};
      cursor[part] = row;
      cursor = row;
    }
    for (const entry of scalar.entries ?? []) {
      assignEnvLeaf(cursor, [entry.key], entry.value, [...keyPath, entry.key.toUpperCase()]);
    }
    return;
  }
  let cursor = root;
  for (let index = 0; index < parts.length - 1; index += 1) {
    const part = parts[index]!;
    const row = asRecord(cursor[part]) ?? {};
    cursor[part] = row;
    cursor = row;
  }
  const leaf = parts[parts.length - 1]!;
  cursor[leaf] = jsFromEnvScalar(scalar);
  markEnv(cursor, leaf, { key: keyPath.join("."), scalar });
}

/**
 * Figment's `AXOND_` env provider parses values with TOML-like syntax and
 * merges them over the file. A type error cites the upper-case key in the
 * environment, and a later file error does not hide an earlier env value.
 */
export function applyEnvOverrides(parsed: Record<string, unknown>, secrets: SecretReader): void {
  for (const [name, value] of secrets.entries()) {
    if (value === undefined || !name.toLowerCase().startsWith("axond_")) {
      continue;
    }
    const parts = name.slice(6).toLowerCase().split("__").filter((part) => part.length > 0);
    const head = parts[0];
    if (!head || !(OVERRIDE_KEYS as readonly string[]).includes(head)) {
      continue;
    }
    const scalar = figmentEnvValue(value);
    // A nested `__` path into an array section builds a map. Figment then
    // reports `expected a sequence` at the section, whatever the leaf was.
    if ((SEQUENCE_OVERRIDE_KEYS as readonly string[]).includes(head) && !(parts.length === 1 && scalar.kind === "sequence")) {
      const found: FigmentScalar = parts.length > 1 || scalar.kind === "map" ? { kind: "map" } : scalar;
      parsed[head] = found.kind === "map" ? {} : jsFromEnvScalar(found);
      markEnv(parsed, head, { key: head.toUpperCase(), scalar: found });
      continue;
    }
    if (parts.length === 1 && scalar.kind === "sequence") {
      applyEnvSequence(parsed, head, scalar);
      continue;
    }
    // A dict merged over an array replaces that array. A sequence fill is an
    // array, so a later leaf starts from an empty table.
    const existing = parsed[head];
    if (
      existing !== null &&
      typeof existing === "object" &&
      envMark(existing, ENV_WHOLE)?.scalar.kind === "sequence"
    ) {
      parsed[head] = {};
    }
    assignEnvLeaf(parsed, parts, scalar, parts.map((part) => part.toUpperCase()));
  }
}

/**
 * A sequence merged over a section replaces that section. Structs fill in
 * declaration order and cite `SECTION.INDEX`. Arrays of structs cite
 * `SECTION.INDEX` for a non-struct element and `SECTION.INDEX.FIELD` for a
 * named field. Flattened price integers keep the parent key.
 */
function applyEnvSequence(parsed: Record<string, unknown>, head: string, scalar: FigmentScalar): void {
  // A later array replaces an earlier nested map, including its shape mark.
  unmarkEnv(parsed, head);
  const items = scalar.items ?? [];
  const sectionKey = head.toUpperCase();
  if (head === "server") {
    const record: Record<string, unknown> = {};
    markEnv(record, ENV_WHOLE, { key: sectionKey, scalar });
    const item = items[0];
    if (item) {
      record["bind"] = jsFromEnvScalar(item);
      markEnv(record, "bind", { key: `${sectionKey}.0`, scalar: item });
    }
    parsed[head] = record;
    return;
  }
  const fields = positionalFields(head);
  if (fields) {
    const record: Record<string, unknown> = {};
    markEnv(record, ENV_WHOLE, { key: sectionKey, scalar });
    const count = Math.min(items.length, fields.length);
    for (let index = 0; index < count; index += 1) {
      placePositional(record, fields[index]!, items[index]!, `${sectionKey}.${index}`);
    }
    parsed[head] = record;
    return;
  }
  const rows: unknown[] = [];
  items.forEach((item, index) => {
    const leafKey = `${sectionKey}.${index}`;
    if (item.kind === "map") {
      const row: Record<string, unknown> = {};
      markEnv(row, ENV_ELEMENT, { key: leafKey, scalar: item });
      for (const entry of item.entries ?? []) {
        const keyPath =
          head === "price" && FLATTENED_PRICE_FIELDS.has(entry.key) ? [leafKey] : [leafKey, entry.key.toUpperCase()];
        assignEnvLeaf(row, [entry.key], entry.value, keyPath);
      }
      rows.push(row);
      return;
    }
    rows.push(jsFromEnvScalar(item));
  });
  items.forEach((item, index) => {
    if (item.kind !== "map") {
      markEnv(rows, String(index), { key: `${sectionKey}.${index}`, scalar: item });
    }
  });
  parsed[head] = rows;
}

export function positionalFields(head: string): readonly PositionalField[] | null {
  for (const table of [POSITIONAL_BEFORE_SERVER, POSITIONAL_AFTER_SERVER]) {
    const found = table.find((entry) => entry[0] === head);
    if (found) {
      return found[1];
    }
  }
  return null;
}

function placePositional(
  record: Record<string, unknown>,
  field: PositionalField,
  item: FigmentScalar,
  key: string,
): void {
  if (field.kind === "struct" && item.kind === "map") {
    const nested: Record<string, unknown> = {};
    markEnv(nested, ENV_MERGED, { key, scalar: item });
    for (const entry of item.entries ?? []) {
      assignEnvLeaf(nested, [entry.key], entry.value, [key, entry.key.toUpperCase()]);
    }
    record[field.name] = nested;
    return;
  }
  if (field.kind === "struct" && item.kind === "sequence") {
    const nested: Record<string, unknown> = {};
    markEnv(nested, ENV_WHOLE, { key, scalar: item });
    const nestedItems = item.items ?? [];
    const count = Math.min(nestedItems.length, field.fields.length);
    for (let index = 0; index < count; index += 1) {
      placePositional(nested, field.fields[index]!, nestedItems[index]!, `${key}.${index}`);
    }
    record[field.name] = nested;
    return;
  }
  record[field.name] = jsFromEnvScalar(item);
  markEnv(record, field.name, { key, scalar: item });
}

export function asRecord(value: unknown): Record<string, unknown> | null {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return null;
}

export const U32_MAX = 4294967295n;

/**
 * Figment extracts the document before `validate`. A float, string, or enum
 * miss on an earlier key is reported before a later zero bound, a missing
 * store, or a credential. Keys are visited in sorted order.
 */
export type SectionShape = { form: "struct"; name: string } | { form: "seq"; element: string };

/** Keys Figment visits before `server`. A scalar here beats a bad bind. */
export const SECTIONS_BEFORE_SERVER: ReadonlyArray<readonly [string, SectionShape]> = [
  ["admission", { form: "struct", name: "AdmissionConfigWire" }],
  ["blocklist", { form: "struct", name: "BlocklistConfig" }],
  ["catalog", { form: "struct", name: "CatalogConfig" }],
  ["credential", { form: "seq", element: "Credential" }],
  ["credential_pool", { form: "struct", name: "CredentialPool" }],
  ["discovery", { form: "struct", name: "DiscoveryConfig" }],
  ["failover", { form: "struct", name: "Failover" }],
  ["gateway_key", { form: "seq", element: "GatewayKey" }],
  ["namespace", { form: "seq", element: "Namespace" }],
  ["price", { form: "seq", element: "PriceRule" }],
  ["provider", { form: "seq", element: "Provider" }],
];

export function rejectSectionShapes(
  toml: string,
  parsed: Record<string, unknown>,
  sections: ReadonlyArray<readonly [string, SectionShape]>,
): void {
  for (const [key, shape] of sections) {
    if (!Object.hasOwn(parsed, key) || parsed[key] === undefined) {
      continue;
    }
    const value = parsed[key];
    const marked = envMark(parsed, key);
    if (marked) {
      const structShaped =
        shape.form === "struct" &&
        (marked.scalar.kind === "map" || marked.scalar.kind === "sequence" || asRecord(value) !== null || Array.isArray(value));
      if (!structShaped) {
        const expected = shape.form === "struct" ? `struct ${shape.name}` : "a sequence";
        throw configLoad(
          `invalid type: found ${envFound(marked.scalar)}, expected ${expected} for key "${marked.key}"${ENV_LOC}`,
        );
      }
    }
    const literal = topLevelAssignment(toml, key);
    if (shape.form === "struct") {
      if (asRecord(value) || Array.isArray(value)) {
        continue;
      }
      throw configLoad(
        `invalid type: found ${foundPhrase(value, literal)}, expected struct ${shape.name} for key "default.${key}"`,
      );
    }
    if (Array.isArray(value)) {
      realizeSequenceElements(toml, parsed, key);
      value.forEach((entry, index) => {
        if (envMark(value, String(index)) || asRecord(entry)) {
          return;
        }
        const token = literal === null ? null : nthArrayToken(literal, index);
        throw configLoad(
          `invalid type: found ${foundPhrase(entry, token)}, expected struct ${shape.element} for key "default.${key}.${index}"`,
        );
      });
      continue;
    }
    throw configLoad(
      `invalid type: found ${foundPhrase(value, literal)}, expected a sequence for key "default.${key}"`,
    );
  }
}

export function nthArrayToken(rhs: string, index: number): string | null {
  return arrayElements(rhs)[index] ?? null;
}

/**
 * Serde fills a struct from a sequence in declaration order and ignores extra
 * elements. A bad element is reported at `default.{section}.{index}` during
 * extract, before a later zero bound.
 */
export type PositionalField =
  | { name: string; kind: "int"; expected: "u32" | "u64" | "usize" }
  | { name: string; kind: "string" }
  | { name: string; kind: "bool" }
  | { name: string; kind: "enum"; enumName: string; variants: readonly string[] }
  | { name: string; kind: "strings" }
  | { name: string; kind: "struct"; structName: string; fields: readonly PositionalField[] };

/**
 * One element of `[[namespace]]` and the other sequence sections, in
 * declaration order. `defaults[i]` is a `#[serde(default)]` field: a short
 * sequence leaves it unset, and the first field without one is `invalid length`.
 * `[[price]]` is absent because `#[serde(flatten)]` cannot be filled from a
 * sequence, so that element stays `expected struct PriceRule`.
 */
type ElementSpec = {
  name: string;
  fields: readonly PositionalField[];
  defaults: readonly boolean[];
};

const NAMESPACE_ELEMENT: ElementSpec = {
  name: "Namespace",
  fields: [
    { name: "id", kind: "string" },
    { name: "default", kind: "bool" },
    { name: "allow_platform_fallback", kind: "bool" },
  ],
  defaults: [false, true, true],
};

const PROVIDER_ELEMENT: ElementSpec = {
  name: "Provider",
  fields: [
    { name: "id", kind: "string" },
    { name: "kind", kind: "enum", enumName: "ProviderKind", variants: ["openai", "anthropic", "openai-compatible"] },
    { name: "base_url", kind: "string" },
    { name: "unpriced_models", kind: "enum", enumName: "UnpricedModels", variants: ["deny", "allow"] },
  ],
  defaults: [false, false, false, true],
};

const CREDENTIAL_ELEMENT: ElementSpec = {
  name: "Credential",
  fields: [
    { name: "namespace", kind: "string" },
    { name: "provider", kind: "string" },
    { name: "env", kind: "string" },
    { name: "id", kind: "string" },
    { name: "weight", kind: "int", expected: "u32" },
  ],
  defaults: [false, false, true, true, true],
};

const GATEWAY_KEY_ELEMENT: ElementSpec = {
  name: "GatewayKey",
  fields: [
    { name: "env", kind: "string" },
    { name: "file", kind: "string" },
    { name: "namespace", kind: "string" },
  ],
  defaults: [true, true, false],
};

const USAGE_SINK_ELEMENT: ElementSpec = {
  name: "UsageSinkConfigWire",
  fields: [
    { name: "kind", kind: "enum", enumName: "UsageSinkKind", variants: ["stdout", "postgres", "otlp"] },
    { name: "dsn_env", kind: "string" },
    { name: "table", kind: "string" },
    { name: "create_table", kind: "bool" },
    { name: "buffer_capacity", kind: "int", expected: "usize" },
    { name: "max_batch", kind: "int", expected: "usize" },
    { name: "flush_interval_ms", kind: "int", expected: "u64" },
  ],
  defaults: [false, true, true, true, true, true, true],
};

const SEQUENCE_ELEMENT_SPECS: Readonly<Record<string, ElementSpec>> = {
  namespace: NAMESPACE_ELEMENT,
  provider: PROVIDER_ELEMENT,
  credential: CREDENTIAL_ELEMENT,
  gateway_key: GATEWAY_KEY_ELEMENT,
  usage_sink: USAGE_SINK_ELEMENT,
};

export const USAGE_INDEX_FIELDS: readonly PositionalField[] = [
  { name: "buffer_capacity", kind: "int", expected: "usize" },
  { name: "max_batch", kind: "int", expected: "usize" },
  { name: "flush_interval_ms", kind: "int", expected: "u64" },
];

export const POSITIONAL_BEFORE_SERVER: ReadonlyArray<readonly [string, readonly PositionalField[]]> = [
  [
    "admission",
    [
      { name: "max_request_bytes", kind: "int", expected: "usize" },
      { name: "max_in_flight", kind: "int", expected: "usize" },
      { name: "max_in_flight_streams", kind: "int", expected: "usize" },
      { name: "max_in_flight_per_tenant", kind: "int", expected: "usize" },
      { name: "max_tenants", kind: "int", expected: "usize" },
      { name: "queue_capacity", kind: "int", expected: "usize" },
      { name: "queue_wait_ms", kind: "int", expected: "u64" },
      { name: "max_stream_duration_ms", kind: "int", expected: "u64" },
      { name: "max_prompt_tokens", kind: "int", expected: "u64" },
      { name: "max_output_tokens", kind: "int", expected: "u64" },
      { name: "max_stream_bytes", kind: "int", expected: "u64" },
      { name: "max_pending_settlements", kind: "int", expected: "usize" },
      { name: "max_in_flight_settlements", kind: "int", expected: "usize" },
      { name: "settlement_queue_wait_ms", kind: "int", expected: "u64" },
      { name: "settlement_timeout_ms", kind: "int", expected: "u64" },
    ],
  ],
  ["blocklist", [{ name: "models", kind: "strings" }]],
  [
    "catalog",
    [
      { name: "source", kind: "enum", enumName: "CatalogSourceBackend", variants: ["none", "models-dev", "seed"] },
      { name: "store", kind: "enum", enumName: "CatalogStoreBackend", variants: ["in-memory", "postgres"] },
      { name: "source_url", kind: "string" },
      { name: "dsn_env", kind: "string" },
      { name: "schema", kind: "string" },
      { name: "create_table", kind: "bool" },
      { name: "refresh_interval_seconds", kind: "int", expected: "u64" },
      { name: "refresh_timeout_seconds", kind: "int", expected: "u64" },
      { name: "retry_initial_seconds", kind: "int", expected: "u64" },
      { name: "retry_max_seconds", kind: "int", expected: "u64" },
      { name: "bootstrap", kind: "enum", enumName: "CatalogBootstrap", variants: ["empty", "seed"] },
      { name: "max_payload_bytes", kind: "int", expected: "usize" },
      { name: "connect_timeout_ms", kind: "int", expected: "u64" },
      { name: "operation_timeout_ms", kind: "int", expected: "u64" },
    ],
  ],
  [
    "credential_pool",
    [
      { name: "strategy", kind: "enum", enumName: "SelectionStrategy", variants: ["round-robin", "weighted"] },
      { name: "failure_threshold", kind: "int", expected: "u32" },
      { name: "cooldown_seconds", kind: "int", expected: "u64" },
    ],
  ],
  ["discovery", [{ name: "refresh_interval_seconds", kind: "int", expected: "u64" }]],
  [
    "failover",
    [
      { name: "max_attempts", kind: "int", expected: "u32" },
      { name: "overall_timeout_ms", kind: "int", expected: "u64" },
      { name: "failure_threshold", kind: "int", expected: "u32" },
      { name: "cooldown_seconds", kind: "int", expected: "u64" },
    ],
  ],
];

export const POSITIONAL_AFTER_SERVER: ReadonlyArray<readonly [string, readonly PositionalField[]]> = [
  [
    "shutdown",
    [
      { name: "drain_grace_ms", kind: "int", expected: "u64" },
      { name: "deadline_ms", kind: "int", expected: "u64" },
      { name: "flush_timeout_ms", kind: "int", expected: "u64" },
    ],
  ],
  [
    "storage",
    [
      { name: "backend", kind: "enum", enumName: "StorageBackend", variants: ["sqlite", "postgres"] },
      { name: "path", kind: "string" },
      { name: "dsn_env", kind: "string" },
      { name: "on_unavailable", kind: "enum", enumName: "StoreUnavailable", variants: ["deny", "allow"] },
      { name: "create_table", kind: "bool" },
      { name: "usage_index", kind: "struct", structName: "UsageIndexConfig", fields: USAGE_INDEX_FIELDS },
    ],
  ],
  [
    "transport",
    [
      { name: "connect_timeout_ms", kind: "int", expected: "u64" },
      { name: "response_header_timeout_ms", kind: "int", expected: "u64" },
      { name: "buffered_body_timeout_ms", kind: "int", expected: "u64" },
      { name: "stream_idle_timeout_ms", kind: "int", expected: "u64" },
      { name: "stream_terminal_grace_ms", kind: "int", expected: "u64" },
      { name: "max_response_bytes", kind: "int", expected: "u64" },
      { name: "max_error_bytes", kind: "int", expected: "u64" },
    ],
  ],
  [
    "usage_journal",
    [
      { name: "backend", kind: "enum", enumName: "UsageJournalBackend", variants: ["none", "postgres"] },
      { name: "dsn_env", kind: "string" },
      { name: "schema", kind: "string" },
      { name: "create_schema", kind: "bool" },
      { name: "consumer", kind: "string" },
      { name: "max_events", kind: "int", expected: "u64" },
      { name: "max_delivery_attempts", kind: "int", expected: "u32" },
      { name: "retain_acknowledged_seconds", kind: "int", expected: "u64" },
      { name: "capacity_policy", kind: "enum", enumName: "UsageCapacityPolicy", variants: ["refuse", "drop-oldest"] },
      { name: "on_undurable", kind: "enum", enumName: "UndurablePolicy", variants: ["refuse", "serve"] },
      { name: "operation_timeout_ms", kind: "int", expected: "u64" },
      { name: "connect_timeout_ms", kind: "int", expected: "u64" },
      { name: "connections", kind: "int", expected: "usize" },
      { name: "claim_batch", kind: "int", expected: "usize" },
      { name: "lease_seconds", kind: "int", expected: "u64" },
      { name: "poll_interval_ms", kind: "int", expected: "u64" },
    ],
  ],
];

export function projectNestedStructs(
  toml: string,
  section: string,
  record: Record<string, unknown>,
  fields: readonly PositionalField[],
): void {
  for (const field of fields) {
    if (field.kind !== "struct" || !Array.isArray(record[field.name])) {
      continue;
    }
    record[field.name] = structFromSequence(
      record[field.name] as unknown[],
      field.fields,
      sectionFieldLiteral(toml, section, field.name),
      `default.${section}.${field.name}`,
    );
  }
}

/**
 * A `[[namespace]]`-style element written as an array fills that struct in
 * declaration order. A short array keeps defaulted fields unset. `[[price]]`
 * is not filled: flatten refuses the sequence as `expected struct PriceRule`.
 */
export function realizeSequenceElements(toml: string, parsed: Record<string, unknown>, key: string): void {
  const spec = SEQUENCE_ELEMENT_SPECS[key];
  const value = parsed[key];
  if (!spec || !Array.isArray(value)) {
    return;
  }
  const literal = topLevelAssignment(toml, key);
  value.forEach((entry, index) => {
    const marked = envMark(value, String(index));
    if (marked?.scalar.kind === "sequence") {
      value[index] = recordFromEnvSequence(marked.scalar, spec, marked.key);
      unmarkEnv(value, String(index));
      return;
    }
    if (marked || asRecord(entry) || !Array.isArray(entry)) {
      return;
    }
    const token = literal === null ? null : nthArrayToken(literal, index);
    value[index] = recordFromSequence(entry, token === null ? [] : arrayElements(token), spec, `default.${key}.${index}`);
  });
}

function recordFromSequence(
  values: unknown[],
  tokens: string[],
  spec: ElementSpec,
  keyPrefix: string,
): Record<string, unknown> {
  const record: Record<string, unknown> = {};
  spec.fields.forEach((field, index) => {
    if (index < values.length) {
      record[field.name] = positionalValue(values[index], field, tokens[index] ?? null, `${keyPrefix}.${index}`);
      return;
    }
    if (!spec.defaults[index]) {
      throw configLoad(
        `invalid length ${index}, expected struct ${spec.name} with ${spec.fields.length} elements for key "${keyPrefix}"`,
      );
    }
  });
  return record;
}

function recordFromEnvSequence(scalar: FigmentScalar, spec: ElementSpec, keyPrefix: string): Record<string, unknown> {
  const items = scalar.items ?? [];
  const record: Record<string, unknown> = {};
  spec.fields.forEach((field, index) => {
    const item = items[index];
    if (item) {
      record[field.name] = valueFromEnvField(item, field, `${keyPrefix}.${index}`);
      return;
    }
    if (!spec.defaults[index]) {
      throw configLoad(
        `invalid length ${index}, expected struct ${spec.name} with ${spec.fields.length} elements for key "${keyPrefix}"${ENV_LOC}`,
      );
    }
  });
  return record;
}

function valueFromEnvField(scalar: FigmentScalar, field: PositionalField, key: string): unknown {
  const loc = ` for key "${key}"${ENV_LOC}`;
  if (field.kind === "string") {
    if (scalar.kind === "string") {
      return scalar.text;
    }
    throw configLoad(`invalid type: found ${envFound(scalar)}, expected a string${loc}`);
  }
  if (field.kind === "bool") {
    if (scalar.kind === "bool") {
      return scalar.value;
    }
    throw configLoad(`invalid type: found ${envFound(scalar)}, expected a boolean${loc}`);
  }
  if (field.kind === "int") {
    const integer = coerceEnvInt({ key, scalar }, field.expected);
    return integer <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(integer) : integer;
  }
  if (field.kind === "enum") {
    if (scalar.kind === "float") {
      throw configLoad(`invalid type: found float \`${scalar.text}\`, expected enum ${field.enumName}${loc}`);
    }
    if (scalar.kind === "string") {
      if (field.variants.includes(scalar.text)) {
        return scalar.text;
      }
      const list =
        field.variants.length === 2
          ? `\`${field.variants[0]}\` or \`${field.variants[1]}\``
          : `one of ${field.variants.map((item) => `\`${item}\``).join(", ")}`;
      throw configLoad(`unknown variant: found \`${scalar.text}\`, expected \`${list}\`${loc}`);
    }
    throw configLoad(`invalid type: found ${envFound(scalar)}, expected enum ${field.enumName}${loc}`);
  }
  throw configLoad(`invalid type: found ${envFound(scalar)}, expected a sequence${loc}`);
}

export function structFromSequence(
  values: unknown[],
  fields: readonly PositionalField[],
  literal: string | null,
  keyPrefix: string,
): Record<string, unknown> {
  const tokens = literal === null ? [] : arrayElements(literal);
  const record: Record<string, unknown> = {};
  const count = Math.min(values.length, fields.length);
  for (let index = 0; index < count; index += 1) {
    const field = fields[index];
    if (!field) {
      break;
    }
    record[field.name] = positionalValue(values[index], field, tokens[index] ?? null, `${keyPrefix}.${index}`);
  }
  return record;
}

export function positionalValue(
  value: unknown,
  field: PositionalField,
  token: string | null,
  figmentKey: string,
): unknown {
  if (field.kind === "int") {
    return positionalInt(value, token, field.expected, figmentKey);
  }
  if (field.kind === "string") {
    if (typeof value === "string" && (token === null || !isFloatToken(token))) {
      return value;
    }
    throw configLoad(`invalid type: found ${foundPhrase(value, token)}, expected a string for key "${figmentKey}"`);
  }
  if (field.kind === "bool") {
    if (typeof value === "boolean" && (token === null || !isFloatToken(token))) {
      return value;
    }
    throw configLoad(`invalid type: found ${foundPhrase(value, token)}, expected a boolean for key "${figmentKey}"`);
  }
  if (field.kind === "strings") {
    if (!Array.isArray(value)) {
      throw configLoad(`invalid type: found ${foundPhrase(value, token)}, expected a sequence for key "${figmentKey}"`);
    }
    const nested = token === null ? [] : arrayElements(token);
    value.forEach((entry, index) => {
      const entryToken = nested[index] ?? null;
      if (typeof entry === "string" && (entryToken === null || !isFloatToken(entryToken))) {
        return;
      }
      throw configLoad(
        `invalid type: found ${foundPhrase(entry, entryToken)}, expected a string for key "${figmentKey}.${index}"`,
      );
    });
    return value;
  }
  if (field.kind === "enum") {
    if (token !== null && isFloatToken(token)) {
      throw configLoad(
        `invalid type: found float \`${rustFloatText(token)}\`, expected enum ${field.enumName} for key "${figmentKey}"`,
      );
    }
    if (typeof value === "string") {
      if (field.variants.includes(value)) {
        return value;
      }
      const list =
        field.variants.length === 2
          ? `\`${field.variants[0]}\` or \`${field.variants[1]}\``
          : `one of ${field.variants.map((item) => `\`${item}\``).join(", ")}`;
      throw configLoad(`unknown variant: found \`${value}\`, expected \`${list}\` for key "${figmentKey}"`);
    }
    throw configLoad(
      `invalid type: found ${foundPhrase(value, token)}, expected enum ${field.enumName} for key "${figmentKey}"`,
    );
  }
  if (Array.isArray(value)) {
    return structFromSequence(value, field.fields, token, figmentKey);
  }
  const record = asRecord(value);
  if (record) {
    for (const nested of field.fields) {
      if (nested.kind !== "int" || !(nested.name in record)) {
        continue;
      }
      positionalInt(record[nested.name], null, nested.expected, `${figmentKey}.${nested.name}`);
    }
    return record;
  }
  throw configLoad(
    `invalid type: found ${foundPhrase(value, token)}, expected struct ${field.structName} for key "${figmentKey}"`,
  );
}

export function positionalInt(
  value: unknown,
  token: string | null,
  expected: "u32" | "u64" | "usize",
  figmentKey: string,
): number | bigint {
  if (token !== null && isFloatToken(token)) {
    throw configLoad(
      `invalid type: found float \`${rustFloatText(token)}\`, expected ${expected} for key "${figmentKey}"`,
    );
  }
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
  return typeof value === "bigint" ? value : Number(integer);
}

export function foundPhrase(value: unknown, literal: string | null): string {
  if (literal !== null && isFloatToken(literal)) {
    return `float \`${rustFloatText(literal)}\``;
  }
  if (typeof value === "string") {
    return `string ${JSON.stringify(value)}`;
  }
  if (typeof value === "boolean") {
    return `bool ${value}`;
  }
  if (Array.isArray(value)) {
    return "sequence";
  }
  if (value !== null && typeof value === "object") {
    return "map";
  }
  if (typeof value === "number" && !Number.isInteger(value)) {
    return `float \`${value}\``;
  }
  if (typeof value === "bigint" || typeof value === "number") {
    const integer = typeof value === "bigint" ? value : BigInt(value);
    return `signed int \`${integer}\``;
  }
  return "sequence";
}

export function sectionFieldLiteral(toml: string, section: string, key: string): string | null {
  let inSection = false;
  let found: string | null = null;
  const header = new RegExp(`^\\[${section}\\]\\s*(?:#.*)?$`);
  const inline = new RegExp(`(?:^|[{,]\\s*)(?:"${key}"|'${key}'|${key})\\s*=\\s*([\\s\\S]*)$`);
  for (const line of toml.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (header.test(trimmed)) {
      inSection = true;
      continue;
    }
    if (inSection && /^\s*\[/.test(line)) {
      inSection = false;
    }
    const dotted = assignmentValue(line, `${section}.${key}`);
    if (dotted !== null) {
      found = scalarToken(dotted);
      continue;
    }
    if (!inSection) {
      continue;
    }
    const match = inline.exec(trimmed);
    if (match) {
      found = scalarToken(match[1] ?? "");
    }
  }
  return found;
}

export const TARGET_UINT_MAX = 18446744073709551615n;

export function configLoad(message: string): GatewayFailure {
  return new GatewayFailure("bad_request", 400, `config: ${message}`);
}

export function topLevelAssignment(toml: string, key: string): string | null {
  let found: string | null = null;
  for (const line of toml.split(/\r?\n/)) {
    if (/^\s*\[/.test(line)) {
      break;
    }
    const assigned = assignmentValue(line, key);
    if (assigned !== null) {
      found = assigned;
    }
  }
  return found;
}

export function arrayElements(rhs: string): string[] {
  const text = rhs.trim();
  if (!text.startsWith("[")) {
    return [];
  }
  const elements: string[] = [];
  let index = 1;
  while (index < text.length) {
    while (index < text.length && " \t,\n\r".includes(text[index] ?? "")) {
      index += 1;
    }
    if (index >= text.length || text[index] === "]") {
      break;
    }
    const start = index;
    index = skipTomlValue(text, index);
    elements.push(text.slice(start, index).trim());
  }
  return elements;
}

function skipTomlValue(text: string, index: number): number {
  const opener = text[index];
  if (opener === '"' || opener === "'") {
    const quote = opener;
    index += 1;
    while (index < text.length) {
      if (text[index] === "\\") {
        index += 2;
        continue;
      }
      if (text[index] === quote) {
        return index + 1;
      }
      index += 1;
    }
    return index;
  }
  if (opener === "[" || opener === "{") {
    let depth = 0;
    while (index < text.length) {
      const char = text[index];
      if (char === '"' || char === "'") {
        index = skipTomlValue(text, index);
        continue;
      }
      if (char === "[" || char === "{") {
        depth += 1;
      } else if (char === "]" || char === "}") {
        depth -= 1;
        index += 1;
        if (depth === 0) {
          return index;
        }
        continue;
      }
      index += 1;
    }
    return index;
  }
  while (index < text.length && text[index] !== "," && text[index] !== "]" && text[index] !== "}") {
    index += 1;
  }
  return index;
}

export type FigmentScalar =
  | { kind: "string"; text: string }
  | { kind: "bool"; value: boolean }
  | { kind: "float"; text: string }
  | { kind: "uint"; text: string }
  | { kind: "int"; text: string }
  | { kind: "sequence"; items?: FigmentScalar[] }
  | { kind: "map"; entries?: { key: string; value: FigmentScalar }[] };

export function scalarToken(raw: string): string {
  const text = raw.trim();
  if (text.startsWith('"') || text.startsWith("'")) {
    return tomlRhs(text);
  }
  const cut = text.search(/[#},]/);
  return (cut === -1 ? text : text.slice(0, cut)).trim();
}

export function figmentEnvValue(raw: string): FigmentScalar {
  const parsed = parseEnvAt(raw, 0);
  if (!parsed || skipAscii(raw, parsed.end) !== raw.length) {
    return { kind: "string", text: raw };
  }
  return parsed.value;
}

function parseEnvAt(raw: string, index: number): { value: FigmentScalar; end: number } | null {
  let cursor = skipAscii(raw, index);
  if (cursor >= raw.length) {
    return null;
  }
  if (raw.startsWith("true", cursor) || raw.startsWith("false", cursor)) {
    const word = raw.startsWith("true", cursor) ? "true" : "false";
    return { value: { kind: "bool", value: word === "true" }, end: skipAscii(raw, cursor + word.length) };
  }
  const head = raw[cursor];
  if (head === "[") {
    return parseEnvArray(raw, cursor);
  }
  if (head === "{") {
    return parseEnvDict(raw, cursor);
  }
  if (head === '"') {
    const quoted = parseEnvString(raw, cursor);
    return quoted === null ? null : { value: { kind: "string", text: quoted.text }, end: skipAscii(raw, quoted.end) };
  }
  if (head === "'") {
    if (raw[cursor + 2] !== "'") {
      return null;
    }
    return { value: { kind: "string", text: raw[cursor + 1] ?? "" }, end: skipAscii(raw, cursor + 3) };
  }
  const start = cursor;
  while (cursor < raw.length && !",{}[]".includes(raw[cursor]!)) {
    cursor += 1;
  }
  return { value: classifyEnvToken(raw.slice(start, cursor).trim()), end: skipAscii(raw, cursor) };
}

function parseEnvArray(raw: string, index: number): { value: FigmentScalar; end: number } | null {
  const items: FigmentScalar[] = [];
  let cursor = skipAscii(raw, index + 1);
  if (raw[cursor] === "]") {
    return { value: { kind: "sequence", items }, end: skipAscii(raw, cursor + 1) };
  }
  while (cursor < raw.length) {
    const item = parseEnvAt(raw, cursor);
    if (!item) {
      return null;
    }
    items.push(item.value);
    cursor = skipAscii(raw, item.end);
    if (raw[cursor] === ",") {
      cursor = skipAscii(raw, cursor + 1);
      continue;
    }
    if (raw[cursor] === "]") {
      return { value: { kind: "sequence", items }, end: skipAscii(raw, cursor + 1) };
    }
    return null;
  }
  return null;
}

function parseEnvDict(raw: string, index: number): { value: FigmentScalar; end: number } | null {
  const entries: { key: string; value: FigmentScalar }[] = [];
  let cursor = skipAscii(raw, index + 1);
  if (raw[cursor] === "}") {
    return { value: { kind: "map", entries }, end: skipAscii(raw, cursor + 1) };
  }
  while (cursor < raw.length) {
    const keyStart = skipAscii(raw, cursor);
    let key: string;
    let keyEnd: number;
    if (raw[keyStart] === '"') {
      const quoted = parseEnvString(raw, keyStart);
      if (!quoted) {
        return null;
      }
      key = quoted.text;
      keyEnd = skipAscii(raw, quoted.end);
    } else {
      let end = keyStart;
      while (end < raw.length && /[A-Za-z0-9_-]/.test(raw[end]!)) {
        end += 1;
      }
      if (end === keyStart) {
        return null;
      }
      key = raw.slice(keyStart, end);
      keyEnd = skipAscii(raw, end);
    }
    if (raw[keyEnd] !== "=") {
      return null;
    }
    const item = parseEnvAt(raw, keyEnd + 1);
    if (!item) {
      return null;
    }
    entries.push({ key, value: item.value });
    cursor = skipAscii(raw, item.end);
    if (raw[cursor] === ",") {
      cursor = skipAscii(raw, cursor + 1);
      continue;
    }
    if (raw[cursor] === "}") {
      return { value: { kind: "map", entries }, end: skipAscii(raw, cursor + 1) };
    }
    return null;
  }
  return null;
}

function parseEnvString(raw: string, index: number): { text: string; end: number } | null {
  let cursor = index + 1;
  let text = "";
  while (cursor < raw.length) {
    const code = raw.codePointAt(cursor);
    if (code === undefined) {
      return null;
    }
    if (code === 0x5c) {
      const next = raw[cursor + 1];
      if (next === undefined) {
        return null;
      }
      const simple: Record<string, string> = {
        '"': '"',
        "\\": "\\",
        b: "\u0008",
        f: "\u000c",
        n: "\n",
        r: "\r",
        t: "\t",
      };
      if (next in simple) {
        text += simple[next];
        cursor += 2;
        continue;
      }
      if (next === "u" || next === "U") {
        const len = next === "u" ? 4 : 8;
        const hex = raw.slice(cursor + 2, cursor + 2 + len);
        if (!/^[0-9A-Fa-f]+$/.test(hex) || hex.length !== len) {
          return null;
        }
        const point = Number.parseInt(hex, 16);
        if (point > 0x10ffff || (point >= 0xd800 && point <= 0xdfff)) {
          return null;
        }
        text += String.fromCodePoint(point);
        cursor += 2 + len;
        continue;
      }
      return null;
    }
    if (code === 0x22) {
      return { text, end: cursor + 1 };
    }
    // Figment's string escape rejects controls other than tab, and DEL.
    if (code !== 0x09 && (code < 0x20 || code === 0x7f)) {
      return null;
    }
    text += String.fromCodePoint(code);
    cursor += code > 0xffff ? 2 : 1;
  }
  return null;
}

function classifyEnvToken(token: string): FigmentScalar {
  if (token.includes(".")) {
    const float = Number(token);
    if (Number.isFinite(float) && /^[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?$/.test(token)) {
      return { kind: "float", text: String(float) };
    }
  }
  const uint = rustUint(token);
  if (uint !== null) {
    return { kind: "uint", text: uint };
  }
  const signed = rustSigned(token);
  if (signed !== null) {
    return { kind: "int", text: signed };
  }
  return { kind: "string", text: token };
}

function rustUint(token: string): string | null {
  if (!/^\+?\d+$/.test(token)) {
    return null;
  }
  const digits = token.replace(/^\+/, "").replace(/^0+(?=\d)/, "") || "0";
  if (BigInt(digits) > U64_MAX) {
    return null;
  }
  return digits;
}

function rustSigned(token: string): string | null {
  if (!/^-\d+$/.test(token)) {
    return null;
  }
  const integer = BigInt(token);
  if (integer < I64_MIN) {
    return null;
  }
  return integer.toString();
}

function skipAscii(text: string, index: number): number {
  let cursor = index;
  while (cursor < text.length && /[ \t\n\r\f]/.test(text[cursor]!)) {
    cursor += 1;
  }
  return cursor;
}

export function assignmentValue(line: string, key: string): string | null {
  const match = new RegExp(`^\\s*${key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\s*=\\s*(.*)$`).exec(line);
  if (!match) {
    return null;
  }
  return tomlRhs(match[1] ?? "");
}

function tomlRhs(raw: string): string {
  const text = raw.trim();
  if (text.startsWith('"')) {
    let index = 1;
    while (index < text.length) {
      if (text[index] === "\\") {
        index += 2;
        continue;
      }
      if (text[index] === '"') {
        return text.slice(0, index + 1);
      }
      index += 1;
    }
    return text;
  }
  if (text.startsWith("'")) {
    const end = text.indexOf("'", 1);
    return end === -1 ? text : text.slice(0, end + 1);
  }
  const hash = text.indexOf("#");
  return (hash === -1 ? text : text.slice(0, hash)).trim();
}

export function isFloatToken(token: string): boolean {
  return (
    /^[+-]?(?:inf|nan)$/i.test(token) ||
    /^[+-]?(?:\d[\d_]*)?\.\d[\d_]*(?:[eE][+-]?\d[\d_]*)?$/.test(token) ||
    /^[+-]?\d[\d_]*[eE][+-]?\d[\d_]*$/.test(token) ||
    /^[+-]?\d[\d_]*\.$/.test(token)
  );
}

export function rustFloatText(token: string): string {
  const lower = token.toLowerCase().replace(/_/g, "");
  if (lower === "inf" || lower === "+inf") {
    return "inf";
  }
  if (lower === "-inf") {
    return "-inf";
  }
  if (lower === "nan" || lower === "+nan" || lower === "-nan") {
    return "NaN";
  }
  return String(Number(lower));
}
