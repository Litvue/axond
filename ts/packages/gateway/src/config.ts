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
export async function loadConfig(
  toml: string,
  secrets: SecretReader,
  options?: { resolveSecrets?: boolean },
): Promise<LoadedConfig> {
  let parsed: Record<string, unknown>;
  try {
    parsed = parse(toml, { integersAsBigInt: "asNeeded" }) as Record<string, unknown>;
  } catch (error) {
    throw new GatewayFailure("bad_request", 400, `config: ${error instanceof Error ? error.message : "unreadable toml"}`);
  }
  applyEnvOverrides(parsed, secrets);
  rejectExtractTypes(toml, parsed);
  const bind = readServerBind(toml, parsed, secrets);
  rejectSectionShapes(toml, parsed, SECTIONS_AFTER_SERVER);
  projectPositional(toml, parsed, POSITIONAL_AFTER_SERVER);
  rejectWithdrawn(parsed);
  rejectCollisions(parsed);
  rejectUsageJournal(parsed);

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
  validateUsageIndex(toml, storageRaw);
  let storage: LoadedConfig["storage"];
  if (backend === "sqlite") {
    const path = typeof storageRaw["path"] === "string" ? storageRaw["path"] : "";
    if (path.trim().length === 0) {
      throw configError("`[storage]` sqlite requires a non-empty `path`");
    }
    if (path.trim() === ":memory:") {
      throw configError("`[storage]` sqlite `:memory:` is not durable; use a file path");
    }
    const dsnEnv = typeof storageRaw["dsn_env"] === "string" ? storageRaw["dsn_env"] : "";
    if (dsnEnv.trim().length > 0) {
      throw configError('`[storage]` sqlite ignores `dsn_env`; omit it or use backend = "postgres"');
    }
    storage = { backend, path, createTable, onUnavailable };
  } else {
    const dsnEnv = typeof storageRaw["dsn_env"] === "string" ? storageRaw["dsn_env"] : "";
    if (dsnEnv.trim().length === 0) {
      throw configError("`[storage]` postgres requires a non-empty `dsn_env`");
    }
    const path = typeof storageRaw["path"] === "string" ? storageRaw["path"] : "";
    if (path.trim().length > 0) {
      throw configError('`[storage]` postgres ignores `path`; omit it or use backend = "sqlite"');
    }
    const dsn = secrets.env(dsnEnv);
    storage = {
      backend,
      dsnEnv,
      ...(dsn !== undefined && dsn.length > 0 ? { dsn } : {}),
      createTable,
      onUnavailable,
    };
  }
  const discoveryEarly = asRecord(parsed["discovery"]) ?? {};
  const discoveryIntervalSeconds = readTypedInt(
    toml,
    "discovery",
    discoveryEarly,
    "refresh_interval_seconds",
    "u64",
    300,
  );
  if (discoveryIntervalSeconds < 1) {
    throw configError("discovery.refresh_interval_seconds must be at least 1");
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

  validatePriceBook(parsed, providers);
  validateBlocklist(parsed);
  const poolRaw = asRecord(parsed["credential_pool"]) ?? {};
  readVariant(toml, "credential_pool", poolRaw, "strategy", ["round-robin", "weighted"], "SelectionStrategy");
  const strategyRaw = poolRaw["strategy"];
  const failureThreshold = atLeastOne(toml, "credential_pool", poolRaw, "failure_threshold", "u32", 2);
  const cooldownSeconds = atLeastOne(toml, "credential_pool", poolRaw, "cooldown_seconds", "u64", 30);
  const credentialPool: LoadedConfig["credentialPool"] = {
    strategy: strategyRaw === "weighted" ? "weighted" : "round-robin",
    failureThreshold,
    cooldownSeconds,
  };
  const failoverRaw = asRecord(parsed["failover"]) ?? {};
  const maxAttempts = atLeastOne(toml, "failover", failoverRaw, "max_attempts", "u32", 3);
  const overallTimeoutMs = atLeastOne(toml, "failover", failoverRaw, "overall_timeout_ms", "u64", 30_000);
  const targetFailures = atLeastOne(toml, "failover", failoverRaw, "failure_threshold", "u32", 3);
  const targetCooldown = atLeastOne(toml, "failover", failoverRaw, "cooldown_seconds", "u64", 30);
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
  const transportRaw = asRecord(parsed["transport"]) ?? {};
  const connectTimeoutMs = atLeastOne(
    toml,
    "transport",
    transportRaw,
    "connect_timeout_ms",
    "u64",
    DEFAULT_TRANSPORT.connectTimeoutMs ?? 5_000,
  );
  const responseHeaderTimeoutMs = atLeastOne(
    toml,
    "transport",
    transportRaw,
    "response_header_timeout_ms",
    "u64",
    DEFAULT_TRANSPORT.responseHeaderTimeoutMs,
  );
  const bufferedBodyTimeoutMs = atLeastOne(
    toml,
    "transport",
    transportRaw,
    "buffered_body_timeout_ms",
    "u64",
    DEFAULT_TRANSPORT.bufferedBodyTimeoutMs,
  );
  const streamIdleTimeoutMs = atLeastOne(
    toml,
    "transport",
    transportRaw,
    "stream_idle_timeout_ms",
    "u64",
    DEFAULT_TRANSPORT.streamIdleTimeoutMs,
  );
  const streamTerminalGraceMs = atLeastOne(
    toml,
    "transport",
    transportRaw,
    "stream_terminal_grace_ms",
    "u64",
    DEFAULT_TRANSPORT.streamTerminalGraceMs ?? 1_000,
  );
  const maxResponseBytes = atLeastOne(
    toml,
    "transport",
    transportRaw,
    "max_response_bytes",
    "u64",
    DEFAULT_TRANSPORT.maxResponseBytes,
  );
  const maxErrorBytes = atLeastOne(
    toml,
    "transport",
    transportRaw,
    "max_error_bytes",
    "u64",
    DEFAULT_TRANSPORT.maxErrorBytes ?? 64 * 1024,
  );
  if (maxErrorBytes > maxResponseBytes) {
    throw configError(
      "transport.max_error_bytes must not exceed transport.max_response_bytes: an error body is a response body",
    );
  }
  const transport: TransportLimits = {
    responseHeaderTimeoutMs,
    bufferedBodyTimeoutMs,
    streamIdleTimeoutMs,
    connectTimeoutMs,
    streamTerminalGraceMs,
    maxResponseBytes,
    maxErrorBytes,
    overallTimeoutMs,
    maxAttempts,
  };
  const shutdown = loadShutdown(toml, asRecord(parsed["shutdown"]) ?? {});
  const catalog = validateCatalog(toml, asRecord(parsed["catalog"]) ?? {});

  const credentials: CredentialConfig[] = [];
  const credentialLabels = new Map<string, string[]>();
  for (const entry of asArray(parsed["credential"])) {
    const row = asRecord(entry) ?? {};
    const namespace = stringField(row, "namespace");
    const provider = stringField(row, "provider");
    const envName = typeof row["env"] === "string" ? row["env"] : "";
    if (envName.trim().length === 0) {
      throw configError(`credential for namespace \`${namespace}\` provider \`${provider}\` has an empty \`env\``);
    }
    const weight = numberField(row, "weight", 1);
    const idField = row["id"];
    const label = typeof idField === "string" ? idField : envName;
    if (weight === 0) {
      throw configError(`credential \`${label}\` has weight 0; remove it instead`);
    }
    if (!Number.isInteger(weight) || weight < 1) {
      throw configError("credential weight must be at least 1");
    }
    const poolKey = `${namespace}\0${provider}`;
    const seen = credentialLabels.get(poolKey) ?? [];
    if (seen.includes(label)) {
      throw configError(
        `duplicate credential id \`${label}\` for namespace \`${namespace}\` provider \`${provider}\``,
      );
    }
    seen.push(label);
    credentialLabels.set(poolKey, seen);
    if (!namespaces.some((item) => item.id === namespace)) {
      throw configError(`credential references undefined namespace \`${namespace}\``);
    }
    if (!providers.some((item) => item.id === provider)) {
      throw configError(`credential references undefined provider \`${provider}\``);
    }
    const explicitId = typeof idField === "string" && idField.length > 0;
    credentials.push({
      namespace,
      provider,
      secret: "",
      env: envName,
      id: label,
      ...(explicitId ? {} : { explicitId: false as const }),
      weight,
    });
  }

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
  const envRaw = typeof key["env"] === "string" ? key["env"] : "";
  const fileRaw = typeof key["file"] === "string" ? key["file"] : "";
  const envSet = envRaw.trim().length > 0;
  const fileSet = fileRaw.trim().length > 0;
  if (envSet && fileSet) {
    throw configError(
      `gateway_key for namespace \`${keyNamespace}\` declares both \`env\` and \`file\`; exactly one source is permitted`,
    );
  }
  if (!envSet && !fileSet) {
    throw configError(
      `gateway_key for namespace \`${keyNamespace}\` must declare exactly one non-empty source (\`env\` or \`file\`)`,
    );
  }
  const gatewayKeySubject = envSet ? envRaw : fileRaw;
  const gatewayKeySource = envSet ? "env" : "file";
  if (!namespaces.some((namespace) => namespace.id === keyNamespace)) {
    throw configError(`gateway_key \`${gatewayKeySubject}\` references undefined namespace \`${keyNamespace}\``);
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

  const extensions = asRecord(parsed["extensions"]);
  const extensionsDir = typeof extensions?.["dir"] === "string" ? extensions["dir"] : null;

  const loaded: LoadedConfig = {
    bind,
    storage,
    namespaces,
    providers,
    credentials,
    gatewayKey: "",
    gatewayKeySubject,
    gatewayKeySource,
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
  if (options?.resolveSecrets !== false) {
    await resolveConfigSecrets(loaded, secrets);
  }
  return loaded;
}

/**
 * Fill credential secrets and the gateway key. Structural graph errors stay in
 * `loadConfig`. A missing value is the Rust snapshot error, wrapped the way
 * `main` wraps it: `config resolution failed: …`.
 */
export async function resolveConfigSecrets(config: LoadedConfig, secrets: SecretReader): Promise<void> {
  for (const credential of config.credentials) {
    const envName = credential.env ?? "";
    const value = secrets.env(envName);
    if (value === undefined || value.length === 0) {
      throw new Error(
        "config resolution failed: credential `" +
          credential.id +
          "` for namespace `" +
          credential.namespace +
          "` provider `" +
          credential.provider +
          "` references env var `" +
          envName +
          "`, which is unset or empty",
      );
    }
    credential.secret = value;
  }
  const namespace = config.gatewayKeyNamespace;
  const subject = config.gatewayKeySubject;
  if (config.gatewayKeySource === "env") {
    const value = secrets.env(subject);
    if (value === undefined || value.length === 0) {
      throw new Error(
        "config resolution failed: gateway_key for namespace `" +
          namespace +
          "` references env var `" +
          subject +
          "`, which is unset or empty",
      );
    }
    config.gatewayKey = value;
    return;
  }
  let text: string;
  try {
    text = await secrets.file(subject);
  } catch (error) {
    throw gatewayKeyReadError(namespace, subject, error);
  }
  if (text.length === 0) {
    throw new Error(
      "config resolution failed: gateway_key for namespace `" + namespace + "` file `" + subject + "` is empty",
    );
  }
  config.gatewayKey = text;
}

function gatewayKeyReadError(namespace: string, path: string, error: unknown): Error {
  const code = error && typeof error === "object" && "code" in error ? String((error as { code?: unknown }).code) : "";
  if (code === "INVALID_UTF8") {
    return new Error(
      "config resolution failed: gateway_key for namespace `" + namespace + "` file `" + path + "` is not valid UTF-8",
    );
  }
  const mapped = ioKind(code);
  const kind = mapped?.kind ?? "other error";
  const detail = mapped?.error ?? "unknown error";
  return new Error(
    "config resolution failed: gateway_key for namespace `" +
      namespace +
      "` file `" +
      path +
      "` failed (" +
      kind +
      "): " +
      detail,
  );
}

/** `std::io::ErrorKind` display plus the `fs::read` os-error string. */
function ioKind(code: string): { kind: string; error: string } | null {
  switch (code) {
    case "ENOENT":
      return { kind: "entity not found", error: "No such file or directory (os error 2)" };
    case "EISDIR":
      return { kind: "is a directory", error: "Is a directory (os error 21)" };
    case "EACCES":
      return { kind: "permission denied", error: "Permission denied (os error 13)" };
    default:
      return null;
  }
}

function validatePriceBook(parsed: Record<string, unknown>, providers: { id: string }[]): void {
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

function validateBlocklist(parsed: Record<string, unknown>): void {
  const blocklistRaw = asRecord(parsed["blocklist"]);
  for (const pattern of asArray(blocklistRaw?.["models"])) {
    if (typeof pattern !== "string") {
      throw configError("blocklist models must be strings");
    }
    validateGlob(pattern);
  }
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

function loadShutdown(toml: string, row: Record<string, unknown>): LoadedConfig["shutdown"] {
  const drainGraceMs = readTypedInt(toml, "shutdown", row, "drain_grace_ms", "u64", 5_000);
  const deadlineMs = readTypedInt(toml, "shutdown", row, "deadline_ms", "u64", 15_000);
  const flushTimeoutMs = readTypedInt(toml, "shutdown", row, "flush_timeout_ms", "u64", 5_000);
  for (const [field, value] of [
    ["deadline_ms", deadlineMs],
    ["flush_timeout_ms", flushTimeoutMs],
  ] as const) {
    if (value < 1) {
      throw configError(`shutdown.${field} must be at least 1: shutdown waits are bounded`);
    }
  }
  return { drainGraceMs, deadlineMs, flushTimeoutMs };
}

const MODELS_DEV_CATALOG_URL = "https://models.dev/catalog.json";
const CATALOG_REFRESH_INTERVAL_SECONDS = 21_600;
const CATALOG_REFRESH_TIMEOUT_SECONDS = 60;
const CATALOG_RETRY_INITIAL_SECONDS = 60;
const CATALOG_RETRY_MAX_SECONDS = 3_600;
const CATALOG_CONNECT_TIMEOUT_MS = 10_000;
const CATALOG_OPERATION_TIMEOUT_MS = 30_000;
const CATALOG_MAX_PAYLOAD_BYTES = 64 * 1024 * 1024;

/**
 * Enabled catalogue imports are refused with the Rust sentences. A disabled
 * section is not checked: fields left at their defaults describe an import
 * that will never run.
 */
function validateCatalog(toml: string, row: Record<string, unknown>): LoadedConfig["catalog"] {
  readVariant(toml, "catalog", row, "source", ["none", "models-dev", "seed"], "CatalogSourceBackend");
  const sourceRaw = row["source"];
  const source = sourceRaw === "models-dev" ? "models-dev" : sourceRaw === "seed" ? "seed" : "none";
  if (source === "none") {
    return { source: "none" };
  }
  for (const [field, fallback] of [
    ["refresh_interval_seconds", CATALOG_REFRESH_INTERVAL_SECONDS],
    ["refresh_timeout_seconds", CATALOG_REFRESH_TIMEOUT_SECONDS],
    ["retry_initial_seconds", CATALOG_RETRY_INITIAL_SECONDS],
    ["retry_max_seconds", CATALOG_RETRY_MAX_SECONDS],
    ["connect_timeout_ms", CATALOG_CONNECT_TIMEOUT_MS],
    ["operation_timeout_ms", CATALOG_OPERATION_TIMEOUT_MS],
  ] as const) {
    catalogInt(toml, row, field, fallback, "u64");
  }
  catalogInt(toml, row, "max_payload_bytes", CATALOG_MAX_PAYLOAD_BYTES, "usize");
  const interval = catalogInt(toml, row, "refresh_interval_seconds", CATALOG_REFRESH_INTERVAL_SECONDS, "u64");
  const timeout = catalogInt(toml, row, "refresh_timeout_seconds", CATALOG_REFRESH_TIMEOUT_SECONDS, "u64");
  const initial = catalogInt(toml, row, "retry_initial_seconds", CATALOG_RETRY_INITIAL_SECONDS, "u64");
  const max = catalogInt(toml, row, "retry_max_seconds", CATALOG_RETRY_MAX_SECONDS, "u64");
  if (timeout > interval) {
    throw configError(
      `catalog: catalogue refresh timeout (${timeout}s) must not exceed the interval (${interval}s)`,
    );
  }
  if (max < initial) {
    throw configError(`catalog: backoff.max (${max}s) must be at least backoff.initial (${initial}s)`);
  }
  if (max > interval) {
    throw configError(
      `catalog: catalogue retry ceiling (${max}s) must not exceed the refresh interval (${interval}s): a refusing deployment would refresh less often than a healthy one`,
    );
  }
  const sourceUrl = Object.hasOwn(row, "source_url") && typeof row["source_url"] === "string" ? row["source_url"] : null;
  if (source !== "models-dev") {
    if (Object.hasOwn(row, "source_url")) {
      throw configError(`catalog \`${source}\`: \`source_url\` applies only to \`models-dev\``);
    }
    return { source: "seed", sourceUrl: null };
  }
  const url = sourceUrl ?? MODELS_DEV_CATALOG_URL;
  assertCatalogUrl(url);
  return { source: "models-dev", sourceUrl: url };
}

function catalogInt(
  toml: string,
  row: Record<string, unknown>,
  key: string,
  fallback: number,
  expected: "u64" | "usize",
): number {
  const value = readTypedInt(toml, "catalog", row, key, expected, fallback);
  if (value < 1) {
    throw configError(`catalog.${key} must be at least 1`);
  }
  return value;
}

function assertCatalogUrl(sourceUrl: string): void {
  const parsed = rustUrl(sourceUrl);
  if (parsed.error !== null) {
    throw configError(`catalog.source_url is not a valid URL: ${parsed.error}`);
  }
  if (parsed.scheme !== "https") {
    throw configError(
      `catalog.source_url \`${sourceUrl}\` must be \`https://\`: imported metadata is read for pricing and enablement decisions, so a source that can be substituted in transit is refused rather than trusted`,
    );
  }
  if (!parsed.hasAuthority) {
    throw configError("catalog.source_url must name an HTTPS host");
  }
  if (parsed.username.length > 0 || parsed.password) {
    throw configError("catalog.source_url must not contain embedded credentials");
  }
  const afterScheme = sourceUrl.split("://")[1] ?? sourceUrl;
  const path = afterScheme.split(/[?#]/, 1)[0] ?? afterScheme;
  if (!path.endsWith("/catalog.json")) {
    throw configError(
      "catalog.source_url: `" +
        excerptLocated(sourceUrl) +
        "` is not a supported models.dev document; only `/catalog.json` is (`api.json` and `models.json` have different shapes)",
    );
  }
}

function rustUrl(value: string): { error: string | null; scheme: string; hasAuthority: boolean; username: string; password: boolean } {
  const schemeMatch = /^([a-zA-Z][a-zA-Z0-9+.-]*):\/\/(.*)$/s.exec(value);
  if (!schemeMatch) {
    return { error: "relative URL without a base", scheme: "", hasAuthority: false, username: "", password: false };
  }
  const scheme = schemeMatch[1]!.toLowerCase();
  const rest = schemeMatch[2] ?? "";
  if (rest.length === 0) {
    return { error: "empty host", scheme, hasAuthority: false, username: "", password: false };
  }
  const authority = rest.split(/[/?#]/, 1)[0] ?? "";
  const hasAuthority = authority.length > 0;
  let username = "";
  let password = false;
  const at = authority.lastIndexOf("@");
  if (at !== -1) {
    const userinfo = authority.slice(0, at);
    const colon = userinfo.indexOf(":");
    if (colon === -1) {
      username = userinfo;
    } else {
      username = userinfo.slice(0, colon);
      password = true;
    }
  }
  return { error: null, scheme, hasAuthority, username, password };
}

function excerptLocated(value: string): string {
  const bytes = Buffer.from(value);
  if (bytes.length <= 128) {
    return value;
  }
  let head = 96;
  while (head > 0 && (bytes[head]! & 0xc0) === 0x80) {
    head -= 1;
  }
  let tail = bytes.length - 32;
  while (tail < bytes.length && (bytes[tail]! & 0xc0) === 0x80) {
    tail += 1;
  }
  return `${bytes.subarray(0, head).toString("utf8")}… (${bytes.length} bytes) …${bytes.subarray(tail).toString("utf8")}`;
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

function atLeastOne(
  toml: string,
  section: string,
  row: Record<string, unknown>,
  key: string,
  expected: "u32" | "u64",
  fallback: number,
): number {
  const value = readTypedInt(toml, section, row, key, expected, fallback);
  if (value < 1) {
    throw configError(`${section}.${key} must be at least 1`);
  }
  return value;
}

const U32_MAX = 4294967295n;

/**
 * Figment extracts the document before `validate`. A float, string, or enum
 * miss on an earlier key is reported before a later zero bound, a missing
 * store, or a credential. Keys are visited in sorted order.
 */
type SectionShape = { form: "struct"; name: string } | { form: "seq"; element: string };

/** Keys Figment visits before `server`. A scalar here beats a bad bind. */
const SECTIONS_BEFORE_SERVER: ReadonlyArray<readonly [string, SectionShape]> = [
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

/** Keys Figment visits after `server`. A bad bind beats these. */
const SECTIONS_AFTER_SERVER: ReadonlyArray<readonly [string, SectionShape]> = [
  ["shutdown", { form: "struct", name: "Shutdown" }],
  ["storage", { form: "struct", name: "StorageConfig" }],
  ["transport", { form: "struct", name: "Transport" }],
  ["usage_journal", { form: "struct", name: "UsageJournalConfig" }],
  ["usage_sink", { form: "seq", element: "UsageSinkConfigWire" }],
];

function rejectExtractTypes(toml: string, parsed: Record<string, unknown>): void {
  rejectSectionShapes(toml, parsed, SECTIONS_BEFORE_SERVER);
  projectPositional(toml, parsed, POSITIONAL_BEFORE_SERVER);
  const admission = asRecord(parsed["admission"]) ?? {};
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
  const catalog = asRecord(parsed["catalog"]) ?? {};
  for (const [key, expected, fallback] of [
    ["connect_timeout_ms", "u64", CATALOG_CONNECT_TIMEOUT_MS],
    ["max_payload_bytes", "usize", CATALOG_MAX_PAYLOAD_BYTES],
    ["operation_timeout_ms", "u64", CATALOG_OPERATION_TIMEOUT_MS],
    ["refresh_interval_seconds", "u64", CATALOG_REFRESH_INTERVAL_SECONDS],
    ["refresh_timeout_seconds", "u64", CATALOG_REFRESH_TIMEOUT_SECONDS],
    ["retry_initial_seconds", "u64", CATALOG_RETRY_INITIAL_SECONDS],
    ["retry_max_seconds", "u64", CATALOG_RETRY_MAX_SECONDS],
  ] as const) {
    readTypedInt(toml, "catalog", catalog, key, expected, fallback);
  }
  readVariant(toml, "catalog", catalog, "source", ["none", "models-dev", "seed"], "CatalogSourceBackend");
  asArray(parsed["credential"]).forEach((entry, index) => {
    const row = asRecord(entry);
    if (!row) {
      return;
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
  });
  const pool = asRecord(parsed["credential_pool"]) ?? {};
  readTypedInt(toml, "credential_pool", pool, "cooldown_seconds", "u64", 30);
  readTypedInt(toml, "credential_pool", pool, "failure_threshold", "u32", 2);
  readVariant(toml, "credential_pool", pool, "strategy", ["round-robin", "weighted"], "SelectionStrategy");
  const discovery = asRecord(parsed["discovery"]) ?? {};
  readTypedInt(toml, "discovery", discovery, "refresh_interval_seconds", "u64", 300);
  const failover = asRecord(parsed["failover"]) ?? {};
  readTypedInt(toml, "failover", failover, "cooldown_seconds", "u64", 30);
  readTypedInt(toml, "failover", failover, "failure_threshold", "u32", 3);
  readTypedInt(toml, "failover", failover, "max_attempts", "u32", 3);
  readTypedInt(toml, "failover", failover, "overall_timeout_ms", "u64", 30_000);
}

function rejectSectionShapes(
  toml: string,
  parsed: Record<string, unknown>,
  sections: ReadonlyArray<readonly [string, SectionShape]>,
): void {
  for (const [key, shape] of sections) {
    if (!Object.hasOwn(parsed, key) || parsed[key] === undefined) {
      continue;
    }
    const value = parsed[key];
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
      value.forEach((entry, index) => {
        if (asRecord(entry)) {
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

function nthArrayToken(rhs: string, index: number): string | null {
  return arrayElements(rhs)[index] ?? null;
}

/**
 * Serde fills a struct from a sequence in declaration order and ignores extra
 * elements. A bad element is reported at `default.{section}.{index}` during
 * extract, before a later zero bound.
 */
type PositionalField =
  | { name: string; kind: "int"; expected: "u32" | "u64" | "usize" }
  | { name: string; kind: "string" }
  | { name: string; kind: "bool" }
  | { name: string; kind: "enum"; enumName: string; variants: readonly string[] }
  | { name: string; kind: "strings" }
  | { name: string; kind: "struct"; structName: string; fields: readonly PositionalField[] };

const USAGE_INDEX_FIELDS: readonly PositionalField[] = [
  { name: "buffer_capacity", kind: "int", expected: "usize" },
  { name: "max_batch", kind: "int", expected: "usize" },
  { name: "flush_interval_ms", kind: "int", expected: "u64" },
];

const POSITIONAL_BEFORE_SERVER: ReadonlyArray<readonly [string, readonly PositionalField[]]> = [
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

const POSITIONAL_AFTER_SERVER: ReadonlyArray<readonly [string, readonly PositionalField[]]> = [
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

function projectPositional(
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

function projectNestedStructs(
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

function structFromSequence(
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

function positionalValue(
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

function positionalInt(
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

function readVariant(
  toml: string,
  section: string,
  row: Record<string, unknown>,
  key: string,
  variants: readonly string[],
  enumName: string,
): void {
  if (!(key in row)) {
    return;
  }
  const literal = sectionFieldLiteral(toml, section, key);
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

function foundPhrase(value: unknown, literal: string | null): string {
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
 * number so the caller can report the Rust bound.
 */
function readTypedInt(
  toml: string,
  section: string,
  row: Record<string, unknown>,
  key: string,
  expected: "u32" | "u64" | "usize",
  fallback: number,
  figmentKey = `default.${section}.${key}`,
  literal: string | null | undefined = undefined,
): number {
  const token = literal === undefined ? sectionFieldLiteral(toml, section, key) : literal;
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
  if (integer > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw configError(`\`${key}\` must be an integer`);
  }
  return Number(integer);
}

function sectionFieldLiteral(toml: string, section: string, key: string): string | null {
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

function numberField(row: Record<string, unknown>, key: string, fallback: number): number {
  const value = row[key];
  if (value === undefined) {
    return fallback;
  }
  if (typeof value === "bigint") {
    if (value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER)) {
      return Number(value);
    }
    throw configError(`\`${key}\` must be an integer`);
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  throw configError(`\`${key}\` must be an integer`);
}

/** Tokio `Semaphore::MAX_PERMITS` (`usize::MAX >> 3` on 64-bit). */
const MAX_USAGE_INDEX_BUFFER = 2305843009213693951n;
const MAX_USAGE_INDEX_BATCH = 4096n;
const MAX_USAGE_INDEX_FLUSH_MS = 86400000n;
const TARGET_UINT_MAX = 18446744073709551615n;

/**
 * `[storage.usage_index]` is checked before the backend path. A value serde
 * would reject is a config-load error. A parsed integer outside the Rust
 * bounds is `invalid config`.
 */
function validateUsageIndex(toml: string, storageRaw: Record<string, unknown>): void {
  const index = asRecord(storageRaw["usage_index"]) ?? {};
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
  const literal = usageIndexLiteral(toml, key);
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

function configLoad(message: string): GatewayFailure {
  return new GatewayFailure("bad_request", 400, `config: ${message}`);
}

const BIND_KEY = 'default.server.bind';
const BIND_ENV_KEY = "SERVER.BIND";
const BIND_ENV_LOC = " in `AXOND_` environment variable(s)";
const I64_MAX = 9223372036854775807n;
const I64_MIN = -9223372036854775808n;
const U64_MAX = 18446744073709551615n;

/**
 * `server.bind` is a `SocketAddr`. Figment rejects it while extracting, before
 * withdrawn sections and before the store opens. `AXOND_SERVER__BIND` is the
 * env provider, whose key and source differ from the file.
 */
function readServerBind(toml: string, parsed: Record<string, unknown>, secrets: SecretReader): string {
  const override = serverBindOverride(secrets);
  if (override !== null) {
    return finishBind(figmentEnvValue(override), BIND_ENV_KEY, BIND_ENV_LOC);
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

function topLevelAssignment(toml: string, key: string): string | null {
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

function firstArrayToken(rhs: string): string | null {
  return arrayElements(rhs)[0] ?? null;
}

function arrayElements(rhs: string): string[] {
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
    if (value === undefined || !name.toLowerCase().startsWith("axond_")) {
      continue;
    }
    const parts = name.slice(6).toLowerCase().split("__");
    if (parts.length === 2 && parts[0] === "server" && parts[1] === "bind") {
      found = value;
    }
  }
  return found;
}

type FigmentScalar =
  | { kind: "string"; text: string }
  | { kind: "bool"; value: boolean }
  | { kind: "float"; text: string }
  | { kind: "uint"; text: string }
  | { kind: "int"; text: string }
  | { kind: "sequence" }
  | { kind: "map" };

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

function scalarToken(raw: string): string {
  const text = raw.trim();
  if (text.startsWith('"') || text.startsWith("'")) {
    return tomlRhs(text);
  }
  const cut = text.search(/[#},]/);
  return (cut === -1 ? text : text.slice(0, cut)).trim();
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

function figmentEnvValue(raw: string): FigmentScalar {
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
  let cursor = skipAscii(raw, index + 1);
  if (raw[cursor] === "]") {
    return { value: { kind: "sequence" }, end: skipAscii(raw, cursor + 1) };
  }
  while (cursor < raw.length) {
    const item = parseEnvAt(raw, cursor);
    if (!item) {
      return null;
    }
    cursor = skipAscii(raw, item.end);
    if (raw[cursor] === ",") {
      cursor = skipAscii(raw, cursor + 1);
      continue;
    }
    if (raw[cursor] === "]") {
      return { value: { kind: "sequence" }, end: skipAscii(raw, cursor + 1) };
    }
    return null;
  }
  return null;
}

function parseEnvDict(raw: string, index: number): { value: FigmentScalar; end: number } | null {
  let cursor = skipAscii(raw, index + 1);
  if (raw[cursor] === "}") {
    return { value: { kind: "map" }, end: skipAscii(raw, cursor + 1) };
  }
  while (cursor < raw.length) {
    const keyEnd = readEnvKey(raw, cursor);
    if (keyEnd === null || raw[keyEnd] !== "=") {
      return null;
    }
    const item = parseEnvAt(raw, keyEnd + 1);
    if (!item) {
      return null;
    }
    cursor = skipAscii(raw, item.end);
    if (raw[cursor] === ",") {
      cursor = skipAscii(raw, cursor + 1);
      continue;
    }
    if (raw[cursor] === "}") {
      return { value: { kind: "map" }, end: skipAscii(raw, cursor + 1) };
    }
    return null;
  }
  return null;
}

function readEnvKey(raw: string, index: number): number | null {
  let cursor = skipAscii(raw, index);
  if (raw[cursor] === '"') {
    return parseEnvString(raw, cursor)?.end ?? null;
  }
  const start = cursor;
  while (cursor < raw.length && /[A-Za-z0-9_-]/.test(raw[cursor]!)) {
    cursor += 1;
  }
  if (cursor === start) {
    return null;
  }
  return skipAscii(raw, cursor);
}

function parseEnvString(raw: string, index: number): { text: string; end: number } | null {
  let cursor = index + 1;
  let text = "";
  while (cursor < raw.length) {
    const char = raw[cursor]!;
    if (char === "\\") {
      const next = raw[cursor + 1];
      const mapped: Record<string, string> = { n: "\n", t: "\t", r: "\r", "\\": "\\", '"': '"', "0": "\0" };
      if (next === undefined || !(next in mapped)) {
        return null;
      }
      text += mapped[next];
      cursor += 2;
      continue;
    }
    if (char === '"') {
      return { text, end: cursor + 1 };
    }
    text += char;
    cursor += 1;
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

function assignmentValue(line: string, key: string): string | null {
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

function isFloatToken(token: string): boolean {
  return (
    /^[+-]?(?:inf|nan)$/i.test(token) ||
    /^[+-]?(?:\d[\d_]*)?\.\d[\d_]*(?:[eE][+-]?\d[\d_]*)?$/.test(token) ||
    /^[+-]?\d[\d_]*[eE][+-]?\d[\d_]*$/.test(token) ||
    /^[+-]?\d[\d_]*\.$/.test(token)
  );
}

function rustFloatText(token: string): string {
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
