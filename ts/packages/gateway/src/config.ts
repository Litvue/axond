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
  const discoveryIntervalSeconds = numberField(discoveryEarly, "refresh_interval_seconds", 300);
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
  const failoverEarly = asRecord(parsed["failover"]) ?? {};
  const targetFailures = numberField(failoverEarly, "failure_threshold", 3);
  if (!Number.isInteger(targetFailures) || targetFailures < 1) {
    throw configError("failover.failure_threshold must be at least 1");
  }
  const targetCooldown = numberField(failoverEarly, "cooldown_seconds", 30);
  if (!Number.isInteger(targetCooldown) || targetCooldown < 1) {
    throw configError("failover.cooldown_seconds must be at least 1");
  }
  const catalog = validateCatalog(asRecord(parsed["catalog"]) ?? {});

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

  const shutdown = loadShutdown(asRecord(parsed["shutdown"]) ?? {});

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
function validateCatalog(row: Record<string, unknown>): LoadedConfig["catalog"] {
  const sourceRaw = row["source"];
  const source =
    sourceRaw === "models-dev" || sourceRaw === "models_dev"
      ? "models-dev"
      : sourceRaw === "seed"
        ? "seed"
        : sourceRaw === undefined || sourceRaw === "none"
          ? "none"
          : null;
  if (source === null) {
    throw configError("`[catalog] source` must be `none`, `models-dev`, or `seed`");
  }
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
    catalogInt(row, field, fallback);
  }
  catalogInt(row, "max_payload_bytes", CATALOG_MAX_PAYLOAD_BYTES);
  const interval = catalogInt(row, "refresh_interval_seconds", CATALOG_REFRESH_INTERVAL_SECONDS);
  const timeout = catalogInt(row, "refresh_timeout_seconds", CATALOG_REFRESH_TIMEOUT_SECONDS);
  const initial = catalogInt(row, "retry_initial_seconds", CATALOG_RETRY_INITIAL_SECONDS);
  const max = catalogInt(row, "retry_max_seconds", CATALOG_RETRY_MAX_SECONDS);
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

function catalogInt(row: Record<string, unknown>, key: string, fallback: number): number {
  const value = numberField(row, key, fallback);
  if (Number.isInteger(value) && value < 1) {
    throw configError(`catalog.${key} must be at least 1`);
  }
  return Number.isInteger(value) ? value : fallback;
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
