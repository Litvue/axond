import { parse } from "smol-toml";

import type { CredentialConfig, PriceRule, ProviderConfig, TransportLimits } from "@axond/sdk";

import { clampStreams, defaultPending, validateAdmission, type AdmissionLimits } from "./admission.ts";

import { GatewayFailure } from "./errors.ts";
import { validateGlob } from "./glob.ts";

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

const OVERRIDE_KEYS = [
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

const DEFAULT_USAGE_TABLE = "axond_usage";
const DEFAULT_USAGE_BUFFER = 10_000;
const DEFAULT_USAGE_BATCH = 500;
const DEFAULT_USAGE_FLUSH_MS = 1_000;

/** Rows one flush writes. An omitted `max_batch` is clamped to the buffer. */
export function usageBatchSize(sink: UsageSinkConfig): number {
  return Math.min(sink.maxBatch, sink.bufferCapacity);
}

export interface SecretReader {
  env(name: string): string | undefined;
  file(path: string): Promise<string>;
  entries(): Iterable<[string, string | undefined]>;
}

const DEFAULT_TRANSPORT: TransportLimits = {
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

/**
 * Load post-#499 axond.toml. Withdrawn sections fail the boot by name.
 * Secret values are never copied into an error.
 */
export async function loadConfig(toml: string, secrets: SecretReader): Promise<LoadedConfig> {
  let parsed: Record<string, unknown>;
  try {
    parsed = parse(toml) as Record<string, unknown>;
  } catch (error) {
    throw new GatewayFailure("bad_request", 400, `config: ${error instanceof Error ? error.message : "unreadable toml"}`);
  }
  applyEnvOverrides(parsed, secrets);
  rejectWithdrawn(parsed);
  rejectCollisions(parsed);
  rejectUsageJournal(parsed);

  const server = asRecord(parsed["server"]) ?? {};
  const bind = typeof server["bind"] === "string" ? server["bind"] : "0.0.0.0:8080";
  const storageRaw = asRecord(parsed["storage"]);
  if (!storageRaw) {
    throw configError(
      '`[storage]` is required (ADR 0063): set `backend = "sqlite"` with `path`, or `backend = "postgres"` with `dsn_env`',
    );
  }
  const backend = storageRaw["backend"] === "postgres" ? "postgres" : storageRaw["backend"] === "sqlite" || storageRaw["backend"] === undefined ? "sqlite" : null;
  if (backend === null) {
    throw configError("`[storage] backend` must be `sqlite` or `postgres`");
  }
  const createTable = storageRaw["create_table"] !== false;
  const stance = storageRaw["on_unavailable"];
  const onUnavailable = stance === undefined || stance === "deny" ? "deny" : stance === "allow" ? "allow" : null;
  if (onUnavailable === null) {
    throw configError("`[storage] on_unavailable` must be `deny` or `allow`");
  }
  let storage: LoadedConfig["storage"];
  if (backend === "sqlite") {
    const path = typeof storageRaw["path"] === "string" ? storageRaw["path"] : "";
    if (path.length === 0) {
      throw configError('`[storage] path` is required when `backend = "sqlite"`');
    }
    storage = { backend, path, createTable, onUnavailable };
  } else {
    const dsnEnv = typeof storageRaw["dsn_env"] === "string" ? storageRaw["dsn_env"] : "";
    if (dsnEnv.length === 0) {
      throw configError('`[storage] dsn_env` is required when `backend = "postgres"`');
    }
    const dsn = secrets.env(dsnEnv);
    if (dsn === undefined || dsn.length === 0) {
      throw configError(`\`[storage] dsn_env\` names \`${dsnEnv}\`, which is unset`);
    }
    storage = { backend, dsn, createTable, onUnavailable };
  }

  const namespaces = asArray(parsed["namespace"]).map((entry) => {
    const row = asRecord(entry) ?? {};
    if (typeof row["id"] !== "string") {
      throw configError("namespace `id` must be a string");
    }
    const id = row["id"];
    return {
      id,
      default: row["default"] === true,
      allowPlatformFallback: row["allow_platform_fallback"] === true,
    };
  });
  const defaults = namespaces.filter((namespace) => namespace.default).length;
  if (defaults !== 1) {
    throw configError(`exactly one namespace must set \`default = true\` (found ${defaults})`);
  }
  const defaultNamespace = namespaces.find((namespace) => namespace.default)!.id;

  const providers: ProviderConfig[] = asArray(parsed["provider"]).map((entry) => {
    const row = asRecord(entry) ?? {};
    const id = stringField(row, "id");
    const kind = stringField(row, "kind");
    if (kind !== "openai" && kind !== "openai-compatible" && kind !== "anthropic") {
      throw configError(`provider \`${id}\` kind \`${kind}\` is not supported`);
    }
    const unpriced = row["unpriced_models"];
    return {
      id,
      kind,
      baseUrl: stringField(row, "base_url"),
      unpricedModels: unpriced === "allow" ? "allow" : "deny",
    };
  });

  const credentials: CredentialConfig[] = [];
  for (const entry of asArray(parsed["credential"])) {
    const row = asRecord(entry) ?? {};
    const namespace = stringField(row, "namespace");
    const provider = stringField(row, "provider");
    const envName = typeof row["env"] === "string" ? row["env"] : "";
    if (envName.length === 0) {
      throw configError(`credential for \`${namespace}\`/\`${provider}\` must declare \`env\``);
    }
    const secret = secrets.env(envName);
    if (secret === undefined || secret.length === 0) {
      throw configError(`credential env \`${envName}\` is unset`);
    }
    const weight = numberField(row, "weight", 1);
    if (!Number.isInteger(weight) || weight < 1) {
      throw configError("credential weight must be at least 1");
    }
    const explicitId = typeof row["id"] === "string" && row["id"].length > 0;
    credentials.push({
      namespace,
      provider,
      secret,
      id: explicitId ? String(row["id"]) : envName,
      ...(explicitId ? {} : { explicitId: false as const }),
      weight,
    });
  }
  const poolRaw = asRecord(parsed["credential_pool"]) ?? {};
  const strategyRaw = poolRaw["strategy"];
  if (strategyRaw !== undefined && strategyRaw !== "round-robin" && strategyRaw !== "weighted") {
    throw configError("`[credential_pool] strategy` must be `round-robin` or `weighted`");
  }
  const failureThreshold = numberField(poolRaw, "failure_threshold", 2);
  if (!Number.isInteger(failureThreshold) || failureThreshold < 1) {
    throw configError("credential_pool.failure_threshold must be at least 1");
  }
  const cooldownSeconds = numberField(poolRaw, "cooldown_seconds", 30);
  if (!Number.isInteger(cooldownSeconds) || cooldownSeconds < 1) {
    throw configError("credential_pool.cooldown_seconds must be at least 1");
  }
  const credentialPool: LoadedConfig["credentialPool"] = {
    strategy: strategyRaw === "weighted" ? "weighted" : "round-robin",
    failureThreshold,
    cooldownSeconds,
  };

  const keys = asArray(parsed["gateway_key"]);
  if (keys.length === 0) {
    throw configError(
      "exactly one `[[gateway_key]]` is required: inbound authentication fails closed and there is no keyless mode",
    );
  }
  if (keys.length > 1) {
    throw configError(
      "`[[gateway_key]]` as a per-namespace list is withdrawn (ADR 0063): declare exactly one deployment-wide static key",
    );
  }
  const key = asRecord(keys[0]) ?? {};
  const keyNamespace = typeof key["namespace"] === "string" ? key["namespace"] : "";
  if (!namespaces.some((namespace) => namespace.id === keyNamespace)) {
    throw configError(`gateway_key references undefined namespace \`${keyNamespace}\``);
  }
  const envName = typeof key["env"] === "string" && key["env"].trim().length > 0 ? key["env"] : null;
  const fileName = typeof key["file"] === "string" && key["file"].trim().length > 0 ? key["file"] : null;
  if ((envName === null) === (fileName === null)) {
    throw configError(
      `gateway_key for namespace \`${keyNamespace}\` must declare exactly one non-empty source (\`env\` or \`file\`)`,
    );
  }
  let gatewayKey: string;
  const gatewayKeySubject = envName ?? fileName!;
  if (envName !== null) {
    const value = secrets.env(envName);
    if (value === undefined || value.length === 0) {
      throw configError(`gateway_key env \`${envName}\` is unset`);
    }
    gatewayKey = value;
  } else {
    try {
      gatewayKey = await secrets.file(fileName!);
    } catch (error) {
      const message = error instanceof Error ? error.message : "unreadable";
      throw configError(`gateway_key file could not be read (${message})`);
    }
    if (gatewayKey.length === 0) {
      throw configError("gateway_key file is empty");
    }
  }

  const prices: PriceRule[] = asArray(parsed["price"]).map((entry) => {
    const row = asRecord(entry) ?? {};
    const model = stringField(row, "model");
    validateGlob(model);
    return {
      provider: stringField(row, "provider"),
      model,
      inputMicrodollarsPerMillion: bigField(row, "input_microdollars_per_million"),
      outputMicrodollarsPerMillion: bigField(row, "output_microdollars_per_million"),
    };
  });

  const blocklistRaw = asRecord(parsed["blocklist"]);
  const blocklist = asArray(blocklistRaw?.["models"]).map((pattern) => {
    if (typeof pattern !== "string") {
      throw configError("blocklist models must be strings");
    }
    validateGlob(pattern);
    return pattern;
  });

  const transportRaw = asRecord(parsed["transport"]) ?? {};
  const maxResponseBytes = numberField(transportRaw, "max_response_bytes", DEFAULT_TRANSPORT.maxResponseBytes);
  if (!Number.isInteger(maxResponseBytes) || maxResponseBytes < 1) {
    throw configError("transport.max_response_bytes must be at least 1");
  }
  const maxErrorBytes = numberField(transportRaw, "max_error_bytes", DEFAULT_TRANSPORT.maxErrorBytes ?? 64 * 1024);
  if (!Number.isInteger(maxErrorBytes) || maxErrorBytes < 1) {
    throw configError("transport.max_error_bytes must be at least 1");
  }
  if (maxErrorBytes > maxResponseBytes) {
    throw configError(
      "transport.max_error_bytes must not exceed transport.max_response_bytes: an error body is a response body",
    );
  }
  const failoverRaw = asRecord(parsed["failover"]) ?? {};
  const transport: TransportLimits = {
    responseHeaderTimeoutMs: boundedMillis(transportRaw, "response_header_timeout_ms", DEFAULT_TRANSPORT.responseHeaderTimeoutMs),
    bufferedBodyTimeoutMs: boundedMillis(transportRaw, "buffered_body_timeout_ms", DEFAULT_TRANSPORT.bufferedBodyTimeoutMs),
    streamIdleTimeoutMs: boundedMillis(transportRaw, "stream_idle_timeout_ms", DEFAULT_TRANSPORT.streamIdleTimeoutMs),
    connectTimeoutMs: boundedMillis(transportRaw, "connect_timeout_ms", DEFAULT_TRANSPORT.connectTimeoutMs ?? 5_000),
    streamTerminalGraceMs: boundedMillis(
      transportRaw,
      "stream_terminal_grace_ms",
      DEFAULT_TRANSPORT.streamTerminalGraceMs ?? 1_000,
    ),
    maxResponseBytes,
    maxErrorBytes,
    overallTimeoutMs: boundedMillis(
      failoverRaw,
      "overall_timeout_ms",
      DEFAULT_TRANSPORT.overallTimeoutMs ?? 30_000,
      "failover.overall_timeout_ms",
    ),
    maxAttempts: positiveInteger(
      failoverRaw,
      "max_attempts",
      DEFAULT_TRANSPORT.maxAttempts ?? 3,
      "failover.max_attempts",
    ),
  };
  const admissionRaw = asRecord(parsed["admission"]) ?? {};
  const maxRequestBytes = numberField(admissionRaw, "max_request_bytes", 2 * 1024 * 1024);
  if (!Number.isInteger(maxRequestBytes) || maxRequestBytes < 1) {
    throw configError("admission.max_request_bytes must be at least 1");
  }
  const maxPromptTokens = numberField(admissionRaw, "max_prompt_tokens", 1_000_000);
  if (!Number.isInteger(maxPromptTokens) || maxPromptTokens < 0) {
    throw configError("admission.max_prompt_tokens must be an integer of at least 0");
  }
  const maxOutputTokens = numberField(admissionRaw, "max_output_tokens", 200_000);
  if (!Number.isInteger(maxOutputTokens) || maxOutputTokens < 0) {
    throw configError("admission.max_output_tokens must be an integer of at least 0");
  }
  const maxStreamDurationMs = numberField(admissionRaw, "max_stream_duration_ms", 3_600_000);
  if (!Number.isInteger(maxStreamDurationMs) || maxStreamDurationMs < 0) {
    throw configError("admission.max_stream_duration_ms must be an integer of at least 0");
  }
  const maxStreamBytes = numberField(admissionRaw, "max_stream_bytes", 64 * 1024 * 1024);
  if (!Number.isInteger(maxStreamBytes) || maxStreamBytes < 0) {
    throw configError("admission.max_stream_bytes must be an integer of at least 0");
  }
  const admission = loadAdmission(admissionRaw);
  try {
    validateAdmission(admission);
  } catch (error) {
    throw configError(error instanceof Error ? error.message : "invalid admission");
  }
  const discovery = asRecord(parsed["discovery"]) ?? {};
  const discoveryIntervalSeconds = numberField(discovery, "refresh_interval_seconds", 300);
  if (discoveryIntervalSeconds < 1) {
    throw configError("discovery.refresh_interval_seconds must be at least 1");
  }

  const shutdown = loadShutdown(asRecord(parsed["shutdown"]) ?? {});

  const extensions = asRecord(parsed["extensions"]);
  const extensionsDir = typeof extensions?.["dir"] === "string" ? extensions["dir"] : null;

  const catalogRaw = asRecord(parsed["catalog"]) ?? {};
  const catalogSource = catalogRaw["source"];
  let catalog: LoadedConfig["catalog"] = { source: "none" };
  if (catalogSource === "models-dev" || catalogSource === "models_dev") {
    const sourceUrl = typeof catalogRaw["source_url"] === "string" ? catalogRaw["source_url"] : null;
    if (sourceUrl !== null) {
      assertHttpsCatalog(sourceUrl);
    }
    catalog = { source: "models-dev", sourceUrl };
  } else if (catalogSource === "seed") {
    catalog = { source: "seed", sourceUrl: null };
  } else if (catalogSource !== undefined && catalogSource !== "none") {
    throw configError("`[catalog] source` must be `none`, `models-dev`, or `seed`");
  }

  return {
    bind,
    storage,
    namespaces,
    providers,
    credentials,
    gatewayKey,
    gatewayKeySubject,
    gatewayKeyNamespace: keyNamespace,
    defaultNamespace,
    prices,
    blocklist,
    transport,
    discoveryIntervalSeconds,
    shutdown,
    catalog,
    extensionsDir,
    maxRequestBytes,
    maxPromptTokens,
    maxOutputTokens,
    maxStreamDurationMs,
    maxStreamBytes,
    admission,
    credentialPool,
    usageSinks: loadUsageSinks(parsed["usage_sink"]),
  };
}

function loadAdmission(row: Record<string, unknown>): AdmissionLimits {
  const maxInFlight = nonNegative(row, "max_in_flight", 1024, "admission.max_in_flight");
  const streams = clampStreams(
    maxInFlight,
    Object.hasOwn(row, "max_in_flight_streams")
      ? nonNegative(row, "max_in_flight_streams", 512, "admission.max_in_flight_streams")
      : undefined,
  );
  const queueCapacity = nonNegative(row, "queue_capacity", 0, "admission.queue_capacity");
  const queueWaitMs = nonNegative(row, "queue_wait_ms", 0, "admission.queue_wait_ms");
  const pendingExplicit = Object.hasOwn(row, "max_pending_settlements");
  const maxPendingSettlements = pendingExplicit
    ? nonNegative(row, "max_pending_settlements", 0, "admission.max_pending_settlements")
    : defaultPending(maxInFlight);
  const maxInFlightSettlements = nonNegative(
    row,
    "max_in_flight_settlements",
    64,
    "admission.max_in_flight_settlements",
  );
  const settlementQueueWaitMs = nonNegative(
    row,
    "settlement_queue_wait_ms",
    10_000,
    "admission.settlement_queue_wait_ms",
  );
  const settlementTimeoutMs = nonNegative(row, "settlement_timeout_ms", 10_000, "admission.settlement_timeout_ms");
  return {
    maxInFlight,
    maxInFlightStreams: streams.value,
    streamsExplicit: streams.explicit,
    queueCapacity,
    queueWaitMs,
    maxPendingSettlements,
    pendingExplicit,
    maxInFlightSettlements,
    settlementQueueWaitMs,
    settlementTimeoutMs,
  };
}

function nonNegative(row: Record<string, unknown>, key: string, fallback: number, label: string): number {
  const value = numberField(row, key, fallback);
  if (!Number.isSafeInteger(value) || value < 0) {
    throw configError(`${label} must be an integer of at least 0`);
  }
  return value;
}

function loadShutdown(row: Record<string, unknown>): LoadedConfig["shutdown"] {
  const drainGraceMs = numberField(row, "drain_grace_ms", 5_000);
  const deadlineMs = numberField(row, "deadline_ms", 15_000);
  const flushTimeoutMs = numberField(row, "flush_timeout_ms", 5_000);
  if (!Number.isInteger(drainGraceMs) || drainGraceMs < 0) {
    throw configError("shutdown.drain_grace_ms must be an integer of at least 0");
  }
  for (const [field, value] of [
    ["deadline_ms", deadlineMs],
    ["flush_timeout_ms", flushTimeoutMs],
  ] as const) {
    if (!Number.isInteger(value) || value < 1) {
      throw configError(`shutdown.${field} must be at least 1: shutdown waits are bounded`);
    }
  }
  return { drainGraceMs, deadlineMs, flushTimeoutMs };
}

function assertHttpsCatalog(sourceUrl: string): void {
  let url: URL;
  try {
    url = new URL(sourceUrl);
  } catch {
    throw configError("catalog source_url is not a URL");
  }
  if (url.protocol !== "https:") {
    throw configError("a catalogue source url must be https");
  }
  if (url.username.length > 0 || url.password.length > 0) {
    throw configError("a catalogue source url must have a host without credentials");
  }
}

function rejectUsageJournal(parsed: Record<string, unknown>): void {
  const journal = asRecord(parsed["usage_journal"]);
  if (!journal) {
    return;
  }
  const backend = journal["backend"];
  if (backend === undefined || backend === "none") {
    return;
  }
  if (backend === "postgres") {
    throw configError('`[usage_journal] backend = "postgres"` is not built (ADR 0049)');
  }
  throw configError('`[usage_journal] backend` must be `none` or `postgres`');
}

function loadUsageSinks(value: unknown): UsageSinkConfig[] {
  return asArray(value).map((entry) => {
    const row = asRecord(entry);
    if (!row) {
      throw configError("`[[usage_sink]]` must be a table");
    }
    const kind = row["kind"];
    if (kind !== "stdout" && kind !== "postgres" && kind !== "otlp") {
      throw configError(
        `usage_sink kind: unknown variant \`${String(kind)}\`, expected \`stdout\`, \`postgres\`, or \`otlp\``,
      );
    }
    const sink: UsageSinkConfig = {
      kind,
      dsnEnv: null,
      table: typeof row["table"] === "string" && row["table"].length > 0 ? row["table"] : DEFAULT_USAGE_TABLE,
      createTable: false,
      bufferCapacity: DEFAULT_USAGE_BUFFER,
      maxBatch: DEFAULT_USAGE_BATCH,
      maxBatchExplicit: Object.hasOwn(row, "max_batch"),
      flushIntervalMs: DEFAULT_USAGE_FLUSH_MS,
    };
    if (kind !== "postgres") {
      return sink;
    }
    const createTable = row["create_table"];
    if (createTable !== undefined && typeof createTable !== "boolean") {
      throw configError("usage_sink `postgres`: `create_table` must be a boolean");
    }
    sink.createTable = createTable === true;
    sink.bufferCapacity = usagePositiveInt(row, "buffer_capacity", DEFAULT_USAGE_BUFFER);
    if (sink.maxBatchExplicit) {
      sink.maxBatch = usagePositiveInt(row, "max_batch", DEFAULT_USAGE_BATCH);
      if (sink.maxBatch > sink.bufferCapacity) {
        throw configError(
          `usage_sink \`postgres\`: max_batch (${sink.maxBatch}) must not exceed buffer_capacity (${sink.bufferCapacity})`,
        );
      }
    }
    sink.flushIntervalMs = usagePositiveInt(row, "flush_interval_ms", DEFAULT_USAGE_FLUSH_MS);
    const dsnEnv = typeof row["dsn_env"] === "string" ? row["dsn_env"].trim() : "";
    if (dsnEnv.length === 0) {
      throw configError("usage_sink `postgres`: `dsn_env` must name the env var holding the connection string");
    }
    sink.dsnEnv = dsnEnv;
    const tableError = usageTableError(sink.table);
    if (tableError) {
      throw configError(`usage_sink \`postgres\`: ${tableError}`);
    }
    return sink;
  });
}

function usagePositiveInt(row: Record<string, unknown>, key: string, fallback: number): number {
  if (!Object.hasOwn(row, key)) {
    return fallback;
  }
  const value = row[key];
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw configError(`usage_sink \`postgres\`: ${key} must be an integer`);
  }
  if (value < 1) {
    throw configError(`usage_sink \`postgres\`: ${key} must be at least 1`);
  }
  return value;
}

/** Same identifier rules as the Rust usage sink. The name is interpolated into SQL. */
export function usageTableError(table: string): string | null {
  const parts = table.split(".");
  if (parts.length > 2) {
    return `\`${table}\` is not a valid table name: at most one schema qualifier`;
  }
  for (const part of parts) {
    const valid =
      part.length > 0 &&
      part.length <= 63 &&
      /^[a-z_]/.test(part) &&
      [...part].every((char) => /[a-z0-9_]/.test(char));
    if (!valid) {
      return `\`${table}\` is not a valid table name: use lowercase letters, digits, and underscores`;
    }
  }
  return null;
}

function rejectWithdrawn(parsed: Record<string, unknown>): void {
  const present = WITHDRAWN_SECTIONS.filter((key) => parsed[key] !== undefined).map((key) => `\`${key}\``);
  if (present.length === 0) {
    return;
  }
  const verb = present.length === 1 ? "is" : "are";
  throw configError(
    `${present.join(", ")} ${verb} withdrawn (ADR 0063): remove it. Axond is a store-backed gateway with one static \`[[gateway_key]]\`.`,
  );
}

function rejectCollisions(parsed: Record<string, unknown>): void {
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

function applyEnvOverrides(parsed: Record<string, unknown>, secrets: SecretReader): void {
  const bag = secrets.entries();
  for (const [name, value] of bag) {
    if (!name.startsWith("AXOND_") || value === undefined) {
      continue;
    }
    const field = name.slice("AXOND_".length).toLowerCase();
    if ((OVERRIDE_KEYS as readonly string[]).includes(field)) {
      continue;
    }
    const parts = field.split("__").filter((part) => part.length > 0);
    if (parts.length < 2) {
      continue;
    }
    const [head, ...rest] = parts;
    if (!(OVERRIDE_KEYS as readonly string[]).includes(head!)) {
      continue;
    }
    let cursor = parsed[head!] as Record<string, unknown> | undefined;
    if (!cursor || typeof cursor !== "object" || Array.isArray(cursor)) {
      cursor = {};
      parsed[head!] = cursor;
    }
    let target = cursor;
    for (let index = 0; index < rest.length - 1; index += 1) {
      const key = rest[index]!;
      const next = target[key];
      if (!next || typeof next !== "object" || Array.isArray(next)) {
        target[key] = {};
      }
      target = target[key] as Record<string, unknown>;
    }
    target[rest[rest.length - 1]!] = value;
  }
}

function configError(message: string): GatewayFailure {
  return new GatewayFailure("bad_request", 400, message);
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return null;
}

function asArray(value: unknown): unknown[] {
  if (value === undefined) {
    return [];
  }
  return Array.isArray(value) ? value : [value];
}

function stringField(row: Record<string, unknown>, key: string): string {
  const value = row[key];
  if (typeof value !== "string" || value.length === 0) {
    throw configError(`missing \`${key}\``);
  }
  return value;
}

function bigField(row: Record<string, unknown>, key: string): bigint {
  const value = row[key];
  if (typeof value === "number" && Number.isFinite(value)) {
    return BigInt(Math.trunc(value));
  }
  if (typeof value === "bigint") {
    return value;
  }
  if (typeof value === "string" && /^[0-9]+$/.test(value)) {
    return BigInt(value);
  }
  throw configError(`\`${key}\` must be an integer`);
}

function boundedMillis(
  row: Record<string, unknown>,
  key: string,
  fallback: number,
  label = `transport.${key}`,
): number {
  const value = numberField(row, key, fallback);
  if (!Number.isInteger(value) || value < 1) {
    throw configError(`${label} must be at least 1`);
  }
  return value;
}

function positiveInteger(
  row: Record<string, unknown>,
  key: string,
  fallback: number,
  label: string,
): number {
  const value = numberField(row, key, fallback);
  if (!Number.isInteger(value) || value < 1) {
    throw configError(`${label} must be an integer of at least 1`);
  }
  return value;
}

function numberField(row: Record<string, unknown>, key: string, fallback: number): number {
  const value = row[key];
  if (value === undefined) {
    return fallback;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  throw configError(`\`${key}\` must be an integer`);
}

export function envSecretReader(
  env: Record<string, string | undefined>,
  readFile: (path: string) => Promise<string>,
): SecretReader {
  return {
    env: (name) => env[name],
    file: readFile,
    entries: () => Object.entries(env),
  };
}
