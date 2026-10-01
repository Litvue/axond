import { parse } from "smol-toml";

import type { CredentialConfig, PriceRule, ProviderConfig, TransportLimits } from "@axond/sdk";

import {
  ADMISSION_MAX_PERMITS,
  clampStreams,
  validateAdmission,
  type AdmissionLimits,
} from "./admission.ts";

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
  const leaps: number[] = [];
  const range = scanTomlDocument(toml, 0, leaps).hit;
  if (range) {
    throw configLoad(formatTomlIntegerRange(toml, range.index, range.message));
  }
  let parsed: Record<string, unknown>;
  try {
    parsed = parse(tomlLeapSecondsAs59(toml, leaps), { integersAsBigInt: "asNeeded" }) as Record<string, unknown>;
  } catch (error) {
    throw new GatewayFailure("bad_request", 400, `config: ${error instanceof Error ? error.message : "unreadable toml"}`);
  }
  applyEnvOverrides(parsed, secrets);
  rejectExtractTypes(toml, parsed);
  const bind = readServerBind(toml, parsed, secrets);
  rejectAfterServerExtract(toml, parsed);
  rejectSectionShapes(toml, parsed, SECTIONS_AFTER_SERVER);
  projectPositional(toml, parsed, POSITIONAL_AFTER_SERVER);
  rejectWithdrawn(parsed);
  rejectCollisions(parsed);

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
  if (asUint(discoveryIntervalSeconds) < 1n) {
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
  const cooldownSeconds = timerCount(
    atLeastOne(toml, "credential_pool", poolRaw, "cooldown_seconds", "u64", 30),
    1000,
  );
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
  const maxRequestBytesRaw = admissionInteger(admissionRaw, "max_request_bytes", 2 * 1024 * 1024);
  if (asUint(maxRequestBytesRaw) < 1n) {
    throw configError("admission.max_request_bytes must be at least 1");
  }
  const maxRequestBytes = runtimeCount(maxRequestBytesRaw);
  const maxPromptTokens = runtimeCount(admissionAtLeastZero(admissionRaw, "max_prompt_tokens", 1_000_000));
  const maxOutputTokens = runtimeCount(admissionAtLeastZero(admissionRaw, "max_output_tokens", 200_000));
  const maxStreamDurationMs = timerCount(admissionAtLeastZero(admissionRaw, "max_stream_duration_ms", 3_600_000));
  const maxStreamBytes = runtimeCount(admissionAtLeastZero(admissionRaw, "max_stream_bytes", 64 * 1024 * 1024));
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
  if (asUint(maxErrorBytes) > asUint(maxResponseBytes)) {
    throw configError(
      "transport.max_error_bytes must not exceed transport.max_response_bytes: an error body is a response body",
    );
  }
  const transport: TransportLimits = {
    responseHeaderTimeoutMs: timerCount(responseHeaderTimeoutMs),
    bufferedBodyTimeoutMs: timerCount(bufferedBodyTimeoutMs),
    streamIdleTimeoutMs: timerCount(streamIdleTimeoutMs),
    connectTimeoutMs: timerCount(connectTimeoutMs),
    streamTerminalGraceMs: timerCount(streamTerminalGraceMs),
    maxResponseBytes: runtimeCount(maxResponseBytes),
    maxErrorBytes: runtimeCount(maxErrorBytes),
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
    discoveryIntervalSeconds: timerCount(discoveryIntervalSeconds, 1000),
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
  // Rust reaches an enabled journal only after storage, admission, credentials,
  // gateway keys, and usage sinks. The section is not built, so a file that
  // passed those checks still fails here.
  rejectUsageJournal(parsed);
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
  const maxInFlight = admissionAtLeastZero(row, "max_in_flight", 1024);
  // A defaulted tenant ceiling that reaches the global one is turned off.
  // A written ceiling above that global one, or any tenant ceiling with
  // max_tenants at 0, is refused here. The values stay off AdmissionLimits.
  const perTenantExplicit = Object.hasOwn(row, "max_in_flight_per_tenant");
  const perTenantWritten = perTenantExplicit
    ? admissionAtLeastZero(row, "max_in_flight_per_tenant", 0)
    : null;
  const maxTenants = admissionAtLeastZero(row, "max_tenants", 1024);
  const defaultPerTenant = 256n;
  const perTenant =
    perTenantWritten !== null
      ? perTenantWritten
      : asUint(maxInFlight) > 0n && defaultPerTenant >= asUint(maxInFlight)
        ? 0
        : 256;
  if (asUint(perTenant) > 0n && asUint(maxTenants) === 0n) {
    throw configError(
      "admission.max_tenants must be at least 1 when max_in_flight_per_tenant is set",
    );
  }
  if (asUint(maxInFlight) > 0n && perTenantExplicit && asUint(perTenant) > asUint(maxInFlight)) {
    throw configError(
      `admission.max_in_flight_per_tenant (${asUint(perTenant)}) must not exceed admission.max_in_flight (${asUint(maxInFlight)}): a per-tenant ceiling above the global one cannot isolate a tenant`,
    );
  }
  const streamsExplicit = Object.hasOwn(row, "max_in_flight_streams");
  const maxInFlightStreams = streamsExplicit
    ? admissionAtLeastZero(row, "max_in_flight_streams", 512)
    : clampStreams(runtimeCount(maxInFlight)).value;
  const queueCapacity = admissionAtLeastZero(row, "queue_capacity", 0);
  const queueWaitMs = admissionAtLeastZero(row, "queue_wait_ms", 0);
  const pendingExplicit = Object.hasOwn(row, "max_pending_settlements");
  const maxPendingSettlements = pendingExplicit
    ? admissionAtLeastZero(row, "max_pending_settlements", 0)
    : defaultPendingExact(maxInFlight);
  const maxInFlightSettlements = admissionAtLeastZero(row, "max_in_flight_settlements", 64);
  const settlementQueueWaitMs = admissionAtLeastZero(row, "settlement_queue_wait_ms", 10_000);
  const settlementTimeoutMs = admissionAtLeastZero(row, "settlement_timeout_ms", 10_000);
  if (
    asUint(maxInFlight) > 0n &&
    streamsExplicit &&
    asUint(maxInFlightStreams) > 0n &&
    asUint(maxInFlightStreams) > asUint(maxInFlight)
  ) {
    throw configError(
      `admission.max_in_flight_streams (${asUint(maxInFlightStreams)}) must not exceed admission.max_in_flight (${asUint(maxInFlight)}): a stream is an in-flight request`,
    );
  }
  for (const [field, value] of [
    ["admission.max_in_flight", maxInFlight],
    ["admission.max_in_flight_streams", maxInFlightStreams],
    ["admission.queue_capacity", queueCapacity],
    ["admission.max_pending_settlements", maxPendingSettlements],
    ["admission.max_in_flight_settlements", maxInFlightSettlements],
  ] as const) {
    if (asUint(value) > ADMISSION_MAX_PERMITS) {
      throw configError(
        `${field} (${asUint(value)}) must not exceed ${ADMISSION_MAX_PERMITS}: a larger ceiling is not a bound this process can hold`,
      );
    }
  }
  if ((asUint(queueCapacity) === 0n) !== (asUint(queueWaitMs) === 0n)) {
    throw configError(
      "admission.queue_capacity and admission.queue_wait_ms must be set together: a queue without a wait bound is unbounded latency, and a wait without a queue is never used",
    );
  }
  if (asUint(queueCapacity) > 0n && asUint(maxInFlight) === 0n) {
    throw configError(
      "admission.queue_capacity requires admission.max_in_flight: nothing queues when the global ceiling is off",
    );
  }
  if (
    asUint(maxInFlight) > 0n &&
    pendingExplicit &&
    asUint(maxPendingSettlements) > 0n &&
    asUint(maxPendingSettlements) < asUint(maxInFlight)
  ) {
    throw configError(
      `admission.max_pending_settlements (${asUint(maxPendingSettlements)}) must be at least admission.max_in_flight (${asUint(maxInFlight)}): every admitted request reserves one settlement`,
    );
  }
  const narrowedInFlight = runtimeCount(maxInFlight);
  const streams = clampStreams(
    narrowedInFlight,
    streamsExplicit ? runtimeCount(maxInFlightStreams) : undefined,
  );
  return {
    maxInFlight: narrowedInFlight,
    maxInFlightStreams: streams.value,
    streamsExplicit: streams.explicit,
    queueCapacity: runtimeCount(queueCapacity),
    queueWaitMs: timerCount(queueWaitMs),
    maxPendingSettlements: runtimeCount(maxPendingSettlements),
    pendingExplicit,
    maxInFlightSettlements: runtimeCount(maxInFlightSettlements),
    settlementQueueWaitMs: timerCount(settlementQueueWaitMs),
    settlementTimeoutMs: timerCount(settlementTimeoutMs),
  };
}

/** Four settlements per admitted request, capped at the semaphore ceiling. */
function defaultPendingExact(maxInFlight: number | bigint): number | bigint {
  const base = asUint(maxInFlight) > 0n ? asUint(maxInFlight) : 1024n;
  const scaled = base > ADMISSION_MAX_PERMITS / 4n ? ADMISSION_MAX_PERMITS : base * 4n;
  return scaled <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(scaled) : scaled;
}

/**
 * An admission integer Figment already extracted. A u64 or usize above 2^53
 * stays exact so the semaphore ceiling can name every digit.
 */
function admissionInteger(row: Record<string, unknown>, key: string, fallback: number): number | bigint {
  const value = row[key];
  if (value === undefined) {
    return fallback;
  }
  if (typeof value === "bigint") {
    if (value < 0n) {
      throw configError(`admission.${key} must be an integer of at least 0`);
    }
    return value;
  }
  if (typeof value === "number" && Number.isInteger(value)) {
    if (value < 0) {
      throw configError(`admission.${key} must be an integer of at least 0`);
    }
    return value;
  }
  throw configError(`admission.${key} must be an integer of at least 0`);
}

function admissionAtLeastZero(row: Record<string, unknown>, key: string, fallback: number): number | bigint {
  return admissionInteger(row, key, fallback);
}

function loadShutdown(toml: string, row: Record<string, unknown>): LoadedConfig["shutdown"] {
  const drainGraceMs = readTypedInt(toml, "shutdown", row, "drain_grace_ms", "u64", 5_000);
  const deadlineMs = readTypedInt(toml, "shutdown", row, "deadline_ms", "u64", 15_000);
  const flushTimeoutMs = readTypedInt(toml, "shutdown", row, "flush_timeout_ms", "u64", 5_000);
  for (const [field, value] of [
    ["deadline_ms", deadlineMs],
    ["flush_timeout_ms", flushTimeoutMs],
  ] as const) {
    if (asUint(value) < 1n) {
      throw configError(`shutdown.${field} must be at least 1: shutdown waits are bounded`);
    }
  }
  return {
    drainGraceMs: timerCount(drainGraceMs),
    deadlineMs: timerCount(deadlineMs),
    flushTimeoutMs: timerCount(flushTimeoutMs),
  };
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
  if (asUint(timeout) > asUint(interval)) {
    throw configError(
      `catalog: catalogue refresh timeout (${timeout}s) must not exceed the interval (${interval}s)`,
    );
  }
  if (asUint(max) < asUint(initial)) {
    throw configError(`catalog: backoff.max (${max}s) must be at least backoff.initial (${initial}s)`);
  }
  if (asUint(max) > asUint(interval)) {
    throw configError(
      `catalog: catalogue retry ceiling (${max}s) must not exceed the refresh interval (${interval}s): a refusing deployment would refresh less often than a healthy one`,
    );
  }
  const sourceUrl = Object.hasOwn(row, "source_url") && typeof row["source_url"] === "string" ? row["source_url"] : null;
  if (source !== "models-dev") {
    if (Object.hasOwn(row, "source_url")) {
      throw configError(`catalog \`${source}\`: \`source_url\` applies only to \`models-dev\``);
    }
    rejectCatalogRetention(row);
    return { source: "seed", sourceUrl: null };
  }
  const url = sourceUrl ?? MODELS_DEV_CATALOG_URL;
  assertCatalogUrl(url);
  rejectCatalogRetention(row);
  return { source: "models-dev", sourceUrl: url };
}

/** A Postgres catalogue store needs a DSN name, then an unqualified schema. */
function rejectCatalogRetention(row: Record<string, unknown>): void {
  if (row["store"] !== "postgres") {
    return;
  }
  const dsnEnv = typeof row["dsn_env"] === "string" ? row["dsn_env"].trim() : "";
  if (dsnEnv.length === 0) {
    throw configError(
      "catalog `postgres`: `dsn_env` must name the env var holding the connection string",
    );
  }
  if (typeof row["schema"] !== "string") {
    return;
  }
  const schema = row["schema"];
  const tableError = usageTableError(schema);
  if (tableError) {
    throw configError("`catalog.schema`: " + tableError);
  }
  if (schema.includes(".")) {
    throw configError(
      "`catalog.schema` must be a single unqualified schema name: it names the search path, not a table",
    );
  }
}

function catalogInt(
  toml: string,
  row: Record<string, unknown>,
  key: string,
  fallback: number,
  expected: "u64" | "usize",
): number | bigint {
  const value = readTypedInt(toml, "catalog", row, key, expected, fallback);
  if (asUint(value) < 1n) {
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
    const bufferExact = usagePositiveInt(row, "buffer_capacity", DEFAULT_USAGE_BUFFER);
    if (sink.maxBatchExplicit) {
      const batchExact = usagePositiveInt(row, "max_batch", DEFAULT_USAGE_BATCH);
      if (asUint(batchExact) > asUint(bufferExact)) {
        throw configError(
          `usage_sink \`postgres\`: max_batch (${batchExact}) must not exceed buffer_capacity (${bufferExact})`,
        );
      }
      sink.maxBatch = runtimeCount(batchExact);
    }
    sink.bufferCapacity = runtimeCount(bufferExact);
    sink.flushIntervalMs = timerCount(usagePositiveInt(row, "flush_interval_ms", DEFAULT_USAGE_FLUSH_MS));
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

function usagePositiveInt(row: Record<string, unknown>, key: string, fallback: number): number | bigint {
  if (!Object.hasOwn(row, key)) {
    return fallback;
  }
  const integer = usageInteger(row[key], key);
  if (integer < 1n) {
    throw configError(`usage_sink \`postgres\`: ${key} must be at least 1`);
  }
  return integer <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(integer) : integer;
}

/** Figment already accepted a usize or u64. Keep every digit for the Rust bound. */
function usageInteger(value: unknown, key: string): bigint {
  if (typeof value === "bigint") {
    if (value < 0n) {
      throw configError(`usage_sink \`postgres\`: ${key} must be an integer`);
    }
    return value;
  }
  if (typeof value === "number" && Number.isSafeInteger(value)) {
    if (value < 0) {
      throw configError(`usage_sink \`postgres\`: ${key} must be an integer`);
    }
    return BigInt(value);
  }
  throw configError(`usage_sink \`postgres\`: ${key} must be an integer`);
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

const ENV_LOC = " in `AXOND_` environment variable(s)";
const ENV_WHOLE = "*";
const ENV_MERGED = "@";
const ENV_ELEMENT = "";
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

type EnvLeaf = { key: string; scalar: FigmentScalar };

const envMarks = new WeakMap<object, Map<string, EnvLeaf>>();

function markEnv(row: object, field: string, leaf: EnvLeaf): void {
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

function envMark(row: object, field: string): EnvLeaf | undefined {
  return envMarks.get(row)?.get(field);
}

function envFound(scalar: FigmentScalar): string {
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

function coerceEnvInt(leaf: EnvLeaf, expected: "u32" | "u64" | "usize"): bigint {
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
function applyEnvOverrides(parsed: Record<string, unknown>, secrets: SecretReader): void {
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

function positionalFields(head: string): readonly PositionalField[] | null {
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

function replacedByEnv(row: object): boolean {
  return (
    envMark(row, ENV_WHOLE) !== undefined ||
    envMark(row, ENV_MERGED) !== undefined ||
    envMark(row, ENV_ELEMENT) !== undefined
  );
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
): number | bigint {
  const value = readTypedInt(toml, section, row, key, expected, fallback);
  if (asUint(value) < 1n) {
    throw configError(`${section}.${key} must be at least 1`);
  }
  return value;
}

function asUint(value: number | bigint): bigint {
  return typeof value === "bigint" ? value : BigInt(value);
}

/** A counter held in a JS number. Values above 2^53 keep the largest safe integer. */
function runtimeCount(value: number | bigint): number {
  if (typeof value !== "bigint" || value <= BigInt(Number.MAX_SAFE_INTEGER)) {
    return typeof value === "bigint" ? Number(value) : value;
  }
  return Number.MAX_SAFE_INTEGER;
}

/**
 * A duration a `setTimeout` or `setInterval` will actually wait. `scale` is
 * the multiplier applied to the stored unit (seconds become milliseconds).
 * A number already in range is unchanged. A u64 past 2^53 is capped at the
 * largest delay those timers accept, so the wait does not collapse to 1ms.
 */
function timerCount(value: number | bigint, scale = 1): number {
  const max = BigInt(Math.floor(2_147_483_647 / scale));
  if (typeof value === "bigint" && value > BigInt(Number.MAX_SAFE_INTEGER)) {
    return Number(value > max ? max : value);
  }
  return runtimeCount(value);
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

function rejectExtractTypes(toml: string, parsed: Record<string, unknown>): void {
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
function rejectAfterServerExtract(toml: string, parsed: Record<string, unknown>): void {
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

/**
 * A `[[namespace]]`-style element written as an array fills that struct in
 * declaration order. A short array keeps defaulted fields unset. `[[price]]`
 * is not filled: flatten refuses the sequence as `expected struct PriceRule`.
 */
function realizeSequenceElements(toml: string, parsed: Record<string, unknown>, key: string): void {
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
 * number so the caller can report the Rust bound. A u64 above 2^53 stays a
 * bigint: Figment already accepted it, and the only later bound is the
 * caller's `at least 1` check.
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
 * Figment fails the document in the parser, before extract. A file integer
 * outside `i64` is `number too large` or `number too small`. An inline table
 * stays on one line: a trailing comma, a newline, or a comment is
 * `invalid inline table`. A basic string `\x` with two hex digits, or `\e`,
 * is `invalid escape sequence` at the character after the escape letter.
 * `\a`, `\q`, `\x` without two hex digits, and a backslash before a newline
 * use that diagram. A short `\u` or `\U`, or a code point that is a surrogate
 * or above U+10FFFF, is
 * `invalid unicode 4-digit hex code` or `invalid unicode 8-digit hex code`
 * at the character after `u` or `U`. A complete hex sequence that is out of
 * range also says `value is out of range`. A decimal integer that is only
 * `0` (optional sign) stops there: a following digit, `_`, or radix letter
 * is `expected newline, `#`` at that character. A local time `07:32:00`
 * and a four-digit year are not that diagram. Inside an array that is
 * `invalid array` / `expected `]``. Inside an inline table it is the inline
 * closer. `0.5`, `0e1`, and `0x10` still parse. A decimal float that
 * parses as positive infinity is `invalid floating-point number` at the
 * start of that number. `-1e309`, `1e308`, `1e-400`, and `inf` still parse.
 * A calendar day that month does not have is `invalid date-time` and
 * `value is out of range` on the day. `2024-02-29` and `1900-02-28` still parse.
 * A time whose seconds are `60` is a leap second and still parses. An hour
 * past 23 is the container's newline diagram on `:` or `T`. A minute past
 * 59 or a second past 60 is `invalid time` or `invalid date-time`, and
 * `value is out of range`. An offset hour past 23 is `invalid time offset`.
 * An underscore in a number must sit between digits. `1_`, `1__2`, and
 * `1_e2` are `invalid integer` and `expected digit` at the character after
 * the underscore. `0x_1` is `invalid hexadecimal integer` on that
 * underscore, and `0x1_` adds `expected digit`. Octal and binary use their
 * own labels. A fraction `1.0_` is `invalid floating-point number` and
 * `expected digit, digit`. An exponent `1e1_` is `expected digit`. `1e_`
 * is the float label only. `1_000`, `0x1_0`, `1.0_1`, and `1e1_0` still parse.
 */
function formatTomlIntegerRange(source: string, index: number, message: string): string {
  const line = source.slice(0, index).split("\n").length - 1;
  const lineStart = source.lastIndexOf("\n", Math.max(0, index - 1)) + 1;
  const lineEnd = source.indexOf("\n", index);
  const content = source.slice(lineStart, lineEnd === -1 ? source.length : lineEnd);
  const column = [...source.slice(lineStart, index)].length;
  const lineNum = line + 1;
  const gutter = String(lineNum).length;
  const pad = " ".repeat(gutter + 1);
  return (
    `TOML parse error at line ${lineNum}, column ${column + 1}\n` +
    `${pad}|\n` +
    `${lineNum} | ${content}\n` +
    `${pad}|${" ".repeat(column + 1)}^\n` +
    `${message}\n`
  );
}

type TomlScan = { end: number; hit: { index: number; message: string } | null; bail: boolean; token?: number };

type TomlScope = "document" | "inline";

interface TomlTable {
  kind: "table";
  implicit: boolean;
  dotted: boolean;
  items: Map<string, TomlNode>;
}

type TomlNode = { kind: "value"; typeName: string } | { kind: "inline" } | { kind: "aot"; last: TomlTable } | TomlTable;

function newTomlTable(implicit: boolean, dotted: boolean): TomlTable {
  return { kind: "table", implicit, dotted, items: new Map() };
}

function duplicateTomlMessage(key: string, table: readonly string[] | null): string {
  if (table === null) {
    return `duplicate key \`${key}\``;
  }
  if (table.length === 0) {
    return `duplicate key \`${key}\` in document root`;
  }
  return `duplicate key \`${key}\` in table \`${table.join(".")}\``;
}

function extendTomlMessage(path: readonly string[], actual: string): string {
  return `dotted key \`${path.join(".")}\` attempted to extend non-table type (${actual})`;
}

/** Walk a dotted prefix. An error string is Figment's message, without a header label. */
function descendToml(
  table: TomlTable,
  path: readonly string[],
  dotted: boolean,
  scope: TomlScope,
): TomlTable | string {
  let current = table;
  for (let i = 0; i < path.length; i += 1) {
    const key = path[i] ?? "";
    const existing = current.items.get(key);
    if (!existing) {
      const created = newTomlTable(true, dotted);
      current.items.set(key, created);
      current = created;
      continue;
    }
    if (existing.kind === "aot") {
      current = existing.last;
      continue;
    }
    if (existing.kind === "value" || (existing.kind === "inline" && scope === "document")) {
      const typeName = existing.kind === "value" ? existing.typeName : "inline table";
      return extendTomlMessage(path.slice(0, i + 1), typeName);
    }
    const implicit = existing.kind === "table" ? existing.implicit : false;
    if (dotted && !implicit) {
      return duplicateTomlMessage(key, null);
    }
    if (existing.kind !== "table") {
      return duplicateTomlMessage(key, null);
    }
    current = existing;
  }
  return current;
}

function defineTomlKey(
  table: TomlTable,
  tablePath: readonly string[],
  segments: readonly string[],
  value: TomlNode,
  scope: TomlScope,
): string | null {
  if (segments.length === 0) {
    return null;
  }
  const leaf = segments[segments.length - 1] ?? "";
  const prefix = segments.slice(0, -1);
  const landed = descendToml(table, prefix, true, scope);
  if (typeof landed === "string") {
    return landed;
  }
  if (landed.dotted === (prefix.length === 0)) {
    return duplicateTomlMessage(leaf, null);
  }
  if (landed.items.has(leaf)) {
    return duplicateTomlMessage(leaf, scope === "inline" ? null : tablePath);
  }
  landed.items.set(leaf, value);
  return null;
}

function defineTomlHeader(
  root: TomlTable,
  segments: readonly string[],
  array: boolean,
): { table: TomlTable } | { message: string } {
  const leaf = segments[segments.length - 1] ?? "";
  const prefix = segments.slice(0, -1);
  const parent = descendToml(root, prefix, false, "document");
  if (typeof parent === "string") {
    return { message: parent };
  }
  const existing = parent.items.get(leaf);
  if (array) {
    if (!existing) {
      const last = newTomlTable(false, false);
      parent.items.set(leaf, { kind: "aot", last });
      return { table: last };
    }
    if (existing.kind === "aot") {
      const last = newTomlTable(false, false);
      existing.last = last;
      return { table: last };
    }
    return { message: duplicateTomlMessage(leaf, prefix) };
  }
  if (existing?.kind === "table" && existing.implicit && !existing.dotted) {
    existing.implicit = false;
    existing.dotted = false;
    return { table: existing };
  }
  if (existing) {
    return { message: duplicateTomlMessage(leaf, prefix) };
  }
  const created = newTomlTable(false, false);
  parent.items.set(leaf, created);
  return { table: created };
}

function tomlDefinedValue(source: string, start: number, end: number): TomlNode {
  const char = source[start] ?? "";
  if (char === '"' || char === "'") {
    return { kind: "value", typeName: "string" };
  }
  if (char === "{") {
    return { kind: "inline" };
  }
  if (char === "[") {
    return { kind: "value", typeName: "array" };
  }
  if (char === "t" || char === "f") {
    return { kind: "value", typeName: "boolean" };
  }
  const slice = source.slice(start, end);
  if (/^[+-]?(?:inf|nan)$/.test(slice)) {
    return { kind: "value", typeName: "float" };
  }
  if (isTomlDateOrTime(source, start)) {
    return { kind: "value", typeName: "datetime" };
  }
  const body = char === "+" || char === "-" ? start + 1 : start;
  if (source.startsWith("0x", body) || source.startsWith("0o", body) || source.startsWith("0b", body)) {
    return { kind: "value", typeName: "integer" };
  }
  if (/[.eE]/.test(slice)) {
    return { kind: "value", typeName: "float" };
  }
  return { kind: "value", typeName: "integer" };
}

function scanTomlDocument(source: string, index: number, leaps: number[] = []): TomlScan {
  const root = newTomlTable(false, false);
  let current = root;
  let currentPath: string[] = [];
  let cursor = index;
  while (cursor < source.length) {
    const leading = scanTomlTrivia(source, cursor);
    if (leading.hit) {
      return leading;
    }
    cursor = leading.end;
    if (cursor >= source.length) {
      break;
    }
    if (source[cursor] === "[") {
      const at = cursor;
      const segments: string[] = [];
      const header = skipTomlHeader(source, cursor, segments);
      if (header.hit || header.bail) {
        return header;
      }
      const defined = defineTomlHeader(root, segments, source.startsWith("[[", at));
      if ("message" in defined) {
        return {
          end: at,
          hit: { index: at, message: `invalid table header\n${defined.message}` },
          bail: false,
        };
      }
      current = defined.table;
      currentPath = segments.slice();
      cursor = header.end;
      continue;
    }
    const at = cursor;
    const segments: string[] = [];
    const key = skipTomlKey(source, cursor, segments);
    if (key.hit || key.bail) {
      return key;
    }
    const between = scanTomlTrivia(source, key.end);
    if (between.hit) {
      return between;
    }
    cursor = between.end;
    if (source[cursor] !== "=") {
      return { end: cursor, hit: null, bail: true };
    }
    const value = scanTomlValue(source, cursor + 1, "document", leaps);
    if (value.hit || value.bail) {
      return value;
    }
    const failure = defineTomlKey(
      current,
      currentPath,
      segments,
      tomlDefinedValue(source, value.token ?? cursor + 1, value.end),
      "document",
    );
    if (failure) {
      return { end: at, hit: { index: at, message: failure }, bail: false };
    }
    const tail = documentLineTail(source, value.end);
    if (tail) {
      return tail;
    }
    cursor = value.end;
  }
  return { end: cursor, hit: null, bail: false };
}

type TomlContainer = "document" | "array" | "inline";

const DOCUMENT_AFTER_VALUE = "expected newline, `#`";
const ARRAY_AFTER_VALUE = "invalid array\nexpected `]`";
const STRING_VALUE = "invalid string\nexpected `\"`, `'`";
const LEADING_FLOAT = "invalid floating-point number\nexpected leading digit";
const LEADING_INTEGER = "invalid integer\nexpected leading digit";

/** After a value, Figment allows spaces, a comment, and the end of the line. */
function documentLineTail(source: string, index: number): TomlScan | null {
  const cursor = skipInlineWs(source, index);
  const char = source[cursor] ?? "";
  if (char === "" || char === "\n" || char === "#") {
    return null;
  }
  if (char === "\r" && source[cursor + 1] === "\n") {
    return null;
  }
  return { end: cursor, hit: { index: cursor, message: DOCUMENT_AFTER_VALUE }, bail: false };
}

function afterZeroMessage(container: TomlContainer): string {
  if (container === "array") {
    return ARRAY_AFTER_VALUE;
  }
  if (container === "inline") {
    return INLINE_TABLE_MESSAGE;
  }
  return DOCUMENT_AFTER_VALUE;
}

/** A finished `0` ends before trivia, a comment, or this container's closer. */
function isCompletedZeroBoundary(char: string, container: TomlContainer): boolean {
  if (char === "" || char === " " || char === "\t" || char === "\n" || char === "\r" || char === "#") {
    return true;
  }
  if (container === "array") {
    return char === "," || char === "]";
  }
  if (container === "inline") {
    return char === "," || char === "}";
  }
  return false;
}

function tagTomlValue(scan: TomlScan, token: number): TomlScan {
  if (scan.hit || scan.bail) {
    return scan;
  }
  return { ...scan, token };
}

function scanTomlValue(source: string, index: number, container: TomlContainer, leaps: number[]): TomlScan {
  const leading = scanTomlTrivia(source, index);
  if (leading.hit) {
    return leading;
  }
  const cursor = leading.end;
  if (cursor >= source.length) {
    return { end: cursor, hit: null, bail: true };
  }
  const char = source[cursor];
  if (char === '"' || char === "'") {
    return tagTomlValue(scanTomlString(source, cursor), cursor);
  }
  if (char === "{") {
    return tagTomlValue(scanTomlInline(source, cursor, leaps), cursor);
  }
  if (char === "[") {
    return tagTomlValue(scanTomlArray(source, cursor, leaps), cursor);
  }
  const word = tomlWord(source, cursor);
  const keywordHead = char === "t" ? "true" : char === "f" ? "false" : char === "i" ? "inf" : char === "n" ? "nan" : "";
  if (keywordHead) {
    if (word.startsWith(keywordHead)) {
      return { end: cursor + keywordHead.length, hit: null, bail: false, token: cursor };
    }
    // A lowercase t, f, i, or n commits. Anything short of the keyword is a string.
    return { end: cursor, hit: { index: cursor, message: STRING_VALUE }, bail: false };
  }
  if ((char === "+" || char === "-") && (source.startsWith("inf", cursor + 1) || source.startsWith("nan", cursor + 1))) {
    const end = cursor + 1 + (source.startsWith("inf", cursor + 1) ? 3 : 3);
    return { end, hit: null, bail: false, token: cursor };
  }
  if (char === "+" || char === "-" || isTomlDigit(char)) {
    return tagTomlValue(scanTomlNumber(source, cursor, container, leaps), cursor);
  }
  if (char === "." || char === "_") {
    if (container === "array") {
      return { end: cursor, hit: { index: cursor, message: ARRAY_AFTER_VALUE }, bail: false };
    }
    return {
      end: cursor,
      hit: { index: cursor, message: char === "." ? LEADING_FLOAT : LEADING_INTEGER },
      bail: false,
    };
  }
  if (container === "array") {
    return { end: cursor, hit: { index: cursor, message: ARRAY_AFTER_VALUE }, bail: false };
  }
  return { end: cursor, hit: { index: cursor, message: STRING_VALUE }, bail: false };
}

function scanTomlArray(source: string, index: number, leaps: number[]): TomlScan {
  let cursor = index + 1;
  for (;;) {
    const leading = scanTomlTrivia(source, cursor);
    if (leading.hit) {
      return leading;
    }
    cursor = leading.end;
    if (source[cursor] === "]") {
      return { end: cursor + 1, hit: null, bail: false };
    }
    const value = scanTomlValue(source, cursor, "array", leaps);
    if (value.hit || value.bail) {
      return value;
    }
    const between = scanTomlTrivia(source, value.end);
    if (between.hit) {
      return between;
    }
    cursor = between.end;
    if (source[cursor] === ",") {
      cursor += 1;
      continue;
    }
    if (source[cursor] === "]") {
      return { end: cursor + 1, hit: null, bail: false };
    }
    return { end: cursor, hit: { index: cursor, message: ARRAY_AFTER_VALUE }, bail: false };
  }
}

const INLINE_TABLE_MESSAGE = "invalid inline table\nexpected `}`";

/** Inline tables take spaces and tabs. A newline or comment ends the table. */
function skipInlineWs(source: string, index: number): number {
  let cursor = index;
  while (source[cursor] === " " || source[cursor] === "\t") {
    cursor += 1;
  }
  return cursor;
}

function canStartInlineKey(source: string, index: number): boolean {
  const char = source[index] ?? "";
  return char === '"' || char === "'" || /[A-Za-z0-9_-]/.test(char);
}

function inlineTableHit(index: number): TomlScan {
  return { end: index, hit: { index, message: INLINE_TABLE_MESSAGE }, bail: false };
}

function isInlineBreak(source: string, index: number): boolean {
  const char = source[index] ?? "";
  return char === "\n" || char === "\r" || char === "#";
}

/**
 * TOML 1.0 inline tables have no trailing comma and no newline. Figment
 * reports `invalid inline table` at the comma, or at the newline or `#`
 * when that token is where `}` was required. A comma followed by a key
 * continues. Arrays keep their own trailing commas.
 */
function scanTomlInline(source: string, index: number, leaps: number[]): TomlScan {
  const root = newTomlTable(false, false);
  const mark = index + 1;
  let pending: string | null = null;
  let cursor = index + 1;
  for (;;) {
    cursor = skipInlineWs(source, cursor);
    if (cursor >= source.length) {
      if (pending) {
        return { end: mark, hit: { index: mark, message: pending }, bail: false };
      }
      return { end: cursor, hit: null, bail: true };
    }
    if (source[cursor] === "}") {
      if (pending) {
        return { end: mark, hit: { index: mark, message: pending }, bail: false };
      }
      return { end: cursor + 1, hit: null, bail: false };
    }
    if (source[cursor] === "," || isInlineBreak(source, cursor) || !canStartInlineKey(source, cursor)) {
      if (pending) {
        return { end: mark, hit: { index: mark, message: pending }, bail: false };
      }
      return inlineTableHit(cursor);
    }
    const segments: string[] = [];
    const keyEnd = scanDottedKey(source, cursor, "equals", segments);
    if (keyEnd.hit || keyEnd.bail) {
      return keyEnd;
    }
    cursor = skipInlineWs(source, keyEnd.end);
    if (source[cursor] !== "=") {
      return { end: cursor, hit: null, bail: true };
    }
    const value = scanInlineTableValue(source, cursor + 1, leaps);
    if (value.hit || value.bail) {
      return value;
    }
    const failure = defineTomlKey(
      root,
      [],
      segments,
      tomlDefinedValue(source, value.token ?? keyEnd.end, value.end),
      "inline",
    );
    if (failure && pending === null) {
      pending = failure;
    }
    cursor = skipInlineWs(source, value.end);
    if (source[cursor] === ",") {
      const after = skipInlineWs(source, cursor + 1);
      if (canStartInlineKey(source, after)) {
        cursor = after;
        continue;
      }
      if (pending) {
        return { end: mark, hit: { index: mark, message: pending }, bail: false };
      }
      return inlineTableHit(cursor);
    }
    if (source[cursor] === "}") {
      if (pending) {
        return { end: mark, hit: { index: mark, message: pending }, bail: false };
      }
      return { end: cursor + 1, hit: null, bail: false };
    }
    if (isInlineBreak(source, cursor) || cursor >= source.length) {
      if (pending) {
        return { end: mark, hit: { index: mark, message: pending }, bail: false };
      }
      if (isInlineBreak(source, cursor)) {
        return inlineTableHit(cursor);
      }
      return { end: cursor, hit: null, bail: true };
    }
    if (pending) {
      return { end: mark, hit: { index: mark, message: pending }, bail: false };
    }
    return inlineTableHit(cursor);
  }
}

function scanInlineTableValue(source: string, index: number, leaps: number[]): TomlScan {
  const cursor = skipInlineWs(source, index);
  if (cursor >= source.length || isInlineBreak(source, cursor) || source[cursor] === "}" || source[cursor] === ",") {
    return { end: cursor, hit: null, bail: true };
  }
  return scanTomlValue(source, cursor, "inline", leaps);
}

const INTEGER_LABEL = "invalid integer";
const INTEGER_DIGIT = "invalid integer\nexpected digit";
const FLOAT_LABEL = "invalid floating-point number";
const FLOAT_DIGIT = "invalid floating-point number\nexpected digit";
const FLOAT_FRAC_DIGIT = "invalid floating-point number\nexpected digit, digit";

function radixIntegerLabel(base: number): string {
  if (base === 16) {
    return "invalid hexadecimal integer";
  }
  if (base === 8) {
    return "invalid octal integer";
  }
  return "invalid binary integer";
}

/**
 * `index` is a digit. A later `_` must be followed by another digit.
 * The caret sits on the character that was supposed to be that digit.
 */
function walkGroupedDigits(
  source: string,
  index: number,
  digit: (char: string) => boolean,
  underscoreMessage: string,
): { end: number; hit: { index: number; message: string } | null } {
  let cursor = index + 1;
  while (cursor < source.length) {
    const char = source[cursor] ?? "";
    if (digit(char)) {
      cursor += 1;
      continue;
    }
    if (char === "_") {
      const after = cursor + 1;
      if (digit(source[after] ?? "")) {
        cursor = after + 1;
        continue;
      }
      return { end: after, hit: { index: after, message: underscoreMessage } };
    }
    break;
  }
  return { end: cursor, hit: null };
}

function scanTomlNumber(source: string, index: number, container: TomlContainer, leaps: number[]): TomlScan {
  const head = source[index] ?? "";
  if (source.startsWith("0x", index) || source.startsWith("0o", index) || source.startsWith("0b", index)) {
    const base = source[index + 1] === "x" ? 16 : source[index + 1] === "o" ? 8 : 2;
    const label = radixIntegerLabel(base);
    const digit = (char: string) => isRadixDigit(char, base);
    const prefix = index + 2;
    if (!digit(source[prefix] ?? "")) {
      return { end: prefix, hit: { index: prefix, message: label }, bail: false };
    }
    const walked = walkGroupedDigits(source, prefix, digit, `${label}\nexpected digit`);
    if (walked.hit) {
      return { end: walked.end, hit: walked.hit, bail: false };
    }
    const raw = source.slice(prefix, walked.end).replaceAll("_", "");
    const integer = BigInt(base === 16 ? `0x${raw}` : base === 8 ? `0o${raw}` : `0b${raw}`);
    if (integer > I64_MAX) {
      return { end: walked.end, hit: { index, message: "number too large to fit in target type" }, bail: false };
    }
    return { end: walked.end, hit: null, bail: false };
  }
  let cursor = index;
  let signed = false;
  // Figment's integer label keeps the missing digit. An array backtracks to `]`.
  if (head === "+" || head === "-") {
    signed = true;
    cursor += 1;
    if (!isTomlDigit(source[cursor] ?? "")) {
      if (container === "array") {
        return { end: index, hit: { index, message: ARRAY_AFTER_VALUE }, bail: false };
      }
      return { end: cursor, hit: { index: cursor, message: INTEGER_LABEL }, bail: false };
    }
  }
  const digitsAt = cursor;
  if (source[cursor] === "0") {
    const next = source[cursor + 1] ?? "";
    // A sign cannot start a date. `+07:32:00` is the integer `+0`.
    const dateLike = !signed && isTomlDateOrTime(source, cursor);
    if (next !== "." && next !== "e" && next !== "E" && !dateLike) {
      if (isCompletedZeroBoundary(next, container)) {
        return { end: cursor + 1, hit: null, bail: false };
      }
      return { end: cursor + 1, hit: { index: cursor + 1, message: afterZeroMessage(container) }, bail: false };
    }
  }
  const grouped = walkGroupedDigits(source, cursor, isTomlDigit, INTEGER_DIGIT);
  if (grouped.hit) {
    return { end: grouped.end, hit: grouped.hit, bail: false };
  }
  cursor = grouped.end;
  const next = source[cursor] ?? "";
  if (next === "." || next === "e" || next === "E") {
    return scanDecimalFloat(source, index, cursor);
  }
  // A date is four digits then `-`. A time is two digits then `:`. A sign is an integer.
  if (!signed && continuesTomlDateOrTime(source, digitsAt, cursor, next)) {
    if (next === "-") {
      return scanTomlCalendar(source, digitsAt, leaps);
    }
    const hour = Number(source.slice(digitsAt, cursor));
    if (hour <= 23) {
      return scanTomlLocalTime(source, digitsAt, leaps);
    }
  }
  const raw = source.slice(index, cursor).replaceAll("_", "");
  let integer: bigint;
  try {
    integer = BigInt(raw);
  } catch {
    return { end: index, hit: null, bail: true };
  }
  if (integer > I64_MAX || integer < I64_MIN) {
    return {
      end: cursor,
      hit: {
        index,
        message: integer < I64_MIN ? "number too small to fit in target type" : "number too large to fit in target type",
      },
      bail: false,
    };
  }
  return { end: cursor, hit: null, bail: false };
}

function scanDecimalFloat(source: string, numberStart: number, cursor: number): TomlScan {
  let at = cursor;
  if (source[at] === ".") {
    const first = at + 1;
    if (!isTomlDigit(source[first] ?? "")) {
      return { end: first, hit: { index: first, message: FLOAT_DIGIT }, bail: false };
    }
    const walked = walkGroupedDigits(source, first, isTomlDigit, FLOAT_FRAC_DIGIT);
    if (walked.hit) {
      return { end: walked.end, hit: walked.hit, bail: false };
    }
    at = walked.end;
  }
  if (source[at] === "e" || source[at] === "E") {
    at += 1;
    if (source[at] === "+" || source[at] === "-") {
      at += 1;
    }
    if (!isTomlDigit(source[at] ?? "")) {
      return { end: at, hit: { index: at, message: FLOAT_LABEL }, bail: false };
    }
    const walked = walkGroupedDigits(source, at, isTomlDigit, FLOAT_DIGIT);
    if (walked.hit) {
      return { end: walked.end, hit: walked.hit, bail: false };
    }
    at = walked.end;
  }
  const value = Number(source.slice(numberStart, at).replaceAll("_", ""));
  if (value === Number.POSITIVE_INFINITY) {
    return { end: at, hit: { index: numberStart, message: FLOAT_LABEL }, bail: false };
  }
  return { end: at, hit: null, bail: false };
}

function isRadixDigit(char: string, base: number): boolean {
  if (base === 16) {
    return /[0-9a-fA-F]/.test(char);
  }
  if (base === 8) {
    return /[0-7]/.test(char);
  }
  return char === "0" || char === "1";
}

/**
 * smol-toml reads a time through `Date`, which rejects second 60. Figment
 * accepts that leap second. The two digits are read as 59 so the document
 * parses. No config field is a date-time, so the loaded value is unused.
 */
function tomlLeapSecondsAs59(source: string, leaps: readonly number[]): string {
  if (leaps.length === 0) {
    return source;
  }
  let out = source;
  for (const index of [...leaps].reverse()) {
    if (out.slice(index, index + 2) !== "60") {
      continue;
    }
    out = `${out.slice(0, index)}59${out.slice(index + 2)}`;
  }
  return out;
}

function leapSecondAt(source: string, index: number, end: number): number | null {
  const token = source.slice(index, end);
  const match = /(?:^|[Tt ])(\d{2}):(\d{2}):60/.exec(token);
  if (match === null) {
    return null;
  }
  const hour = Number(match[1]);
  const minute = Number(match[2]);
  if (hour > 23 || minute > 59) {
    return null;
  }
  return index + match.index + match[0].length - 2;
}

const TIME_RANGE = "invalid time\nvalue is out of range";
const OFFSET_RANGE = "invalid time offset\nvalue is out of range";

function isTomlDateOrTime(source: string, index: number): boolean {
  const head = source.slice(index, index + 2);
  if (/^\d\d$/.test(head) && source[index + 2] === ":") {
    return true;
  }
  return /^\d{4}-\d{2}-\d{2}/.test(source.slice(index, index + 10));
}

/** Figment tries a date only for four digits before `-`, or two digits before `:`. */
function continuesTomlDateOrTime(source: string, digitsAt: number, cursor: number, next: string): boolean {
  const digits = source.slice(digitsAt, cursor);
  if (next === "-" && /^\d{4}$/.test(digits)) {
    return true;
  }
  return next === ":" && /^\d{2}$/.test(digits);
}

const DATE_LABEL = "invalid date-time";
const DATE_OUT_OF_RANGE = "invalid date-time\nvalue is out of range";
const OFFSET_LABEL = "invalid time offset";

function tomlTwoDigits(source: string, index: number): number | null {
  const head = source.slice(index, index + 2);
  if (!/^\d{2}$/.test(head)) {
    return null;
  }
  return Number(head);
}

/**
 * Figment cuts a date after `YYYY-`. A missing month, dash, or day is
 * `invalid date-time` on that character. A `T` that is not a finished time
 * stays outside the date, so the container reports it.
 */
function scanTomlCalendar(source: string, yearAt: number, leaps: number[]): TomlScan {
  const year = Number(source.slice(yearAt, yearAt + 4));
  let cursor = yearAt + 5;
  const monthAt = cursor;
  const month = tomlTwoDigits(source, cursor);
  if (month === null) {
    return { end: cursor, hit: { index: cursor, message: DATE_LABEL }, bail: false };
  }
  if (month < 1 || month > 12) {
    return { end: monthAt, hit: { index: monthAt, message: DATE_OUT_OF_RANGE }, bail: false };
  }
  cursor += 2;
  if (source[cursor] !== "-") {
    return { end: cursor, hit: { index: cursor, message: DATE_LABEL }, bail: false };
  }
  cursor += 1;
  const dayAt = cursor;
  const day = tomlTwoDigits(source, cursor);
  if (day === null) {
    return { end: cursor, hit: { index: cursor, message: DATE_LABEL }, bail: false };
  }
  const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const shortMonth = month === 4 || month === 6 || month === 9 || month === 11;
  const maxDay = month === 2 ? (leapYear ? 29 : 28) : shortMonth ? 30 : 31;
  if (day < 1 || day > maxDay) {
    return { end: dayAt, hit: { index: dayAt, message: DATE_OUT_OF_RANGE }, bail: false };
  }
  cursor += 2;
  const suffix = scanTomlDateSuffix(source, cursor);
  if (suffix.hit) {
    return suffix;
  }
  const leap = leapSecondAt(source, yearAt, suffix.end);
  if (leap !== null) {
    leaps.push(leap);
  }
  return suffix;
}

/** A time attaches only after its hour and colon. Otherwise the date already ended. */
function scanTomlDateSuffix(source: string, dateEnd: number): TomlScan {
  const delim = source[dateEnd] ?? "";
  if (delim !== "T" && delim !== "t" && delim !== " ") {
    return { end: dateEnd, hit: null, bail: false };
  }
  const timeAt = dateEnd + 1;
  const hour = tomlTwoDigits(source, timeAt);
  if (hour === null || hour > 23 || source[timeAt + 2] !== ":") {
    return { end: dateEnd, hit: null, bail: false };
  }
  let cursor = timeAt + 3;
  const minuteAt = cursor;
  const minute = tomlTwoDigits(source, cursor);
  if (minute === null) {
    return { end: cursor, hit: { index: cursor, message: DATE_LABEL }, bail: false };
  }
  if (minute > 59) {
    return { end: minuteAt, hit: { index: minuteAt, message: DATE_OUT_OF_RANGE }, bail: false };
  }
  cursor += 2;
  if (source[cursor] !== ":") {
    return { end: cursor, hit: { index: cursor, message: DATE_LABEL }, bail: false };
  }
  cursor += 1;
  const secondAt = cursor;
  const second = tomlTwoDigits(source, cursor);
  if (second === null) {
    return { end: cursor, hit: { index: cursor, message: DATE_LABEL }, bail: false };
  }
  if (second > 60) {
    return { end: secondAt, hit: { index: secondAt, message: DATE_OUT_OF_RANGE }, bail: false };
  }
  cursor += 2;
  if (source[cursor] === "." && isTomlDigit(source[cursor + 1] ?? "")) {
    cursor += 2;
    while (isTomlDigit(source[cursor] ?? "")) {
      cursor += 1;
    }
  }
  const offset = source[cursor] ?? "";
  if (offset === "Z" || offset === "z") {
    return { end: cursor + 1, hit: null, bail: false };
  }
  if (offset === "+" || offset === "-") {
    return scanTomlOffset(source, cursor);
  }
  return { end: cursor, hit: null, bail: false };
}

function scanTomlOffset(source: string, index: number): TomlScan {
  const hourAt = index + 1;
  const hour = tomlTwoDigits(source, hourAt);
  if (hour === null) {
    return { end: hourAt, hit: { index: hourAt, message: OFFSET_LABEL }, bail: false };
  }
  if (hour > 23) {
    return { end: hourAt, hit: { index: hourAt, message: OFFSET_RANGE }, bail: false };
  }
  let cursor = hourAt + 2;
  if (source[cursor] !== ":") {
    return { end: cursor, hit: { index: cursor, message: OFFSET_LABEL }, bail: false };
  }
  cursor += 1;
  const minuteAt = cursor;
  const minute = tomlTwoDigits(source, cursor);
  if (minute === null) {
    return { end: cursor, hit: { index: cursor, message: OFFSET_LABEL }, bail: false };
  }
  if (minute > 59) {
    return { end: minuteAt, hit: { index: minuteAt, message: OFFSET_RANGE }, bail: false };
  }
  return { end: cursor + 2, hit: null, bail: false };
}

const TIME_LABEL = "invalid time";

/** The colon after a legal hour commits the rest of a local time. */
function scanTomlLocalTime(source: string, hourAt: number, leaps: number[]): TomlScan {
  let cursor = hourAt + 3;
  const minuteAt = cursor;
  const minute = tomlTwoDigits(source, cursor);
  if (minute === null) {
    return { end: cursor, hit: { index: cursor, message: TIME_LABEL }, bail: false };
  }
  if (minute > 59) {
    return { end: minuteAt, hit: { index: minuteAt, message: TIME_RANGE }, bail: false };
  }
  cursor += 2;
  if (source[cursor] !== ":") {
    return { end: cursor, hit: { index: cursor, message: TIME_LABEL }, bail: false };
  }
  cursor += 1;
  const secondAt = cursor;
  const second = tomlTwoDigits(source, cursor);
  if (second === null) {
    return { end: cursor, hit: { index: cursor, message: TIME_LABEL }, bail: false };
  }
  if (second > 60) {
    return { end: secondAt, hit: { index: secondAt, message: TIME_RANGE }, bail: false };
  }
  cursor += 2;
  if (source[cursor] === "." && isTomlDigit(source[cursor + 1] ?? "")) {
    cursor += 2;
    while (isTomlDigit(source[cursor] ?? "")) {
      cursor += 1;
    }
  }
  const leap = leapSecondAt(source, hourAt, cursor);
  if (leap !== null) {
    leaps.push(leap);
  }
  return { end: cursor, hit: null, bail: false };
}

function isTomlCommentChar(char: string): boolean {
  const code = char.codePointAt(0) ?? 0;
  return code === 0x09 || (code >= 0x20 && code <= 0x7e) || code >= 0x80;
}

/** Spaces, tabs, newlines, and comments. A control character in a comment is an error with an empty message. */
function scanTomlTrivia(source: string, index: number): TomlScan {
  let cursor = index;
  for (;;) {
    while (cursor < source.length && " \t\r\n".includes(source[cursor] ?? "")) {
      cursor += 1;
    }
    if (source[cursor] !== "#") {
      return { end: cursor, hit: null, bail: false };
    }
    cursor += 1;
    while (cursor < source.length && source[cursor] !== "\n") {
      const char = source[cursor] ?? "";
      if (char === "\r" && source[cursor + 1] === "\n") {
        break;
      }
      if (!isTomlCommentChar(char)) {
        return { end: cursor, hit: { index: cursor, message: "" }, bail: false };
      }
      cursor += 1;
    }
  }
}

const KEY_EQUALS = "expected `.`, `=`";
const INVALID_KEY = "invalid key";
const HEADER_STD = "invalid table header\nexpected `.`, `]`";
const HEADER_ARRAY = "invalid table header\nexpected `.`, `]]`";
const HEADER_TRAIL = "invalid table header\nexpected newline, `#`";

type KeyStop = "equals" | "header-std" | "header-array";

/**
 * A dotted key allows spaces and tabs around `.`. A `.` that is not followed
 * by another segment is reported on that dot. A key that never starts is
 * `invalid key`, except an inline table, which still wants `}`.
 */
function scanDottedKey(source: string, index: number, stop: KeyStop, segments: string[]): TomlScan {
  let cursor = index;
  for (;;) {
    let segmentEnd: number;
    if (source[cursor] === '"' || source[cursor] === "'") {
      const scanned = scanTomlString(source, cursor, false);
      if (scanned.hit || scanned.bail) {
        return scanned;
      }
      segmentEnd = scanned.end;
      segments.push(decodeTomlKey(source, cursor, segmentEnd));
    } else {
      segmentEnd = cursor;
      while (/[A-Za-z0-9_-]/.test(source[segmentEnd] ?? "")) {
        segmentEnd += 1;
      }
      segments.push(source.slice(cursor, segmentEnd));
    }
    const after = skipInlineWs(source, segmentEnd);
    if (source[after] === ".") {
      const next = skipInlineWs(source, after + 1);
      if (canStartInlineKey(source, next)) {
        cursor = next;
        continue;
      }
      return { end: after, hit: { index: after, message: keyStopMessage(stop) }, bail: false };
    }
    return finishKeyStop(source, after, stop);
  }
}

function decodeTomlKey(source: string, start: number, end: number): string {
  const quote = source[start] ?? "";
  if (quote !== '"' && quote !== "'") {
    return source.slice(start, end);
  }
  if (quote === "'") {
    return source.slice(start + 1, end - 1);
  }
  let cursor = start + 1;
  const stop = end - 1;
  let out = "";
  while (cursor < stop) {
    if (source[cursor] !== "\\") {
      out += source[cursor] ?? "";
      cursor += 1;
      continue;
    }
    const esc = source[cursor + 1] ?? "";
    const simple: Record<string, string> = {
      b: "\b",
      f: "\f",
      n: "\n",
      r: "\r",
      t: "\t",
      '"': '"',
      "\\": "\\",
    };
    const decoded = simple[esc];
    if (decoded !== undefined) {
      out += decoded;
      cursor += 2;
      continue;
    }
    if (esc === "u" || esc === "U") {
      const width = esc === "u" ? 4 : 8;
      out += String.fromCodePoint(Number.parseInt(source.slice(cursor + 2, cursor + 2 + width), 16));
      cursor += 2 + width;
      continue;
    }
    out += source[cursor] ?? "";
    cursor += 1;
  }
  return out;
}

function keyStopMessage(stop: KeyStop): string {
  if (stop === "header-std") {
    return HEADER_STD;
  }
  if (stop === "header-array") {
    return HEADER_ARRAY;
  }
  return KEY_EQUALS;
}

function finishKeyStop(source: string, cursor: number, stop: KeyStop): TomlScan {
  if (stop === "equals") {
    if (source[cursor] === "=") {
      return { end: cursor, hit: null, bail: false };
    }
    return { end: cursor, hit: { index: cursor, message: KEY_EQUALS }, bail: false };
  }
  if (stop === "header-array") {
    if (source.startsWith("]]", cursor)) {
      return finishHeaderLine(source, cursor + 2);
    }
    return { end: cursor, hit: { index: cursor, message: HEADER_ARRAY }, bail: false };
  }
  if (source[cursor] === "]") {
    return finishHeaderLine(source, cursor + 1);
  }
  return { end: cursor, hit: { index: cursor, message: HEADER_STD }, bail: false };
}

function finishHeaderLine(source: string, cursor: number): TomlScan {
  const after = skipInlineWs(source, cursor);
  const char = source[after] ?? "";
  if (char === "" || char === "\n" || char === "\r" || char === "#") {
    return { end: cursor, hit: null, bail: false };
  }
  return { end: after, hit: { index: after, message: HEADER_TRAIL }, bail: false };
}

function skipTomlHeader(source: string, index: number, segments: string[]): TomlScan {
  const array = source.startsWith("[[", index);
  let cursor = skipInlineWs(source, index + (array ? 2 : 1));
  if (!canStartInlineKey(source, cursor)) {
    return { end: cursor, hit: { index: cursor, message: INVALID_KEY }, bail: false };
  }
  return scanDottedKey(source, cursor, array ? "header-array" : "header-std", segments);
}

function skipTomlKey(source: string, index: number, segments: string[]): TomlScan {
  const cursor = skipInlineWs(source, index);
  if (!canStartInlineKey(source, cursor)) {
    return { end: cursor, hit: { index: cursor, message: INVALID_KEY }, bail: false };
  }
  return scanDottedKey(source, cursor, "equals", segments);
}

const ESCAPE_SEQUENCE_MESSAGE = "invalid escape sequence\nexpected `b`, `f`, `n`, `r`, `t`, `u`, `U`, `\\`, `\"`";

function isTomlHex(char: string): boolean {
  return (char >= "0" && char <= "9") || (char >= "A" && char <= "F") || (char >= "a" && char <= "f");
}

function stringLabel(multiline: boolean, literal: boolean): string {
  if (multiline && literal) {
    return "invalid multiline literal string";
  }
  if (multiline) {
    return "invalid multiline basic string";
  }
  if (literal) {
    return "invalid literal string";
  }
  return "invalid basic string";
}

/** Tab, printable ASCII other than the delimiter and `\`, and non-ASCII. */
function isTomlStringChar(char: string, literal: boolean): boolean {
  const code = char.codePointAt(0) ?? 0;
  if (code === 0x09 || code >= 0x80) {
    return true;
  }
  if (literal) {
    return (code >= 0x20 && code <= 0x26) || (code >= 0x28 && code <= 0x7e);
  }
  return code === 0x20 || code === 0x21 || (code >= 0x23 && code <= 0x5b) || (code >= 0x5d && code <= 0x7e);
}

/**
 * Figment rejects `\xHH` and `\e` in a basic string, and a `\u` or `\U`
 * that is short or names a surrogate or a code point above U+10FFFF. The
 * caret is the character after the escape letter. Literal strings keep the
 * backslash. A raw control character or newline is `invalid basic string`
 * or `invalid literal string` on that character. Multiline strings still
 * contain newlines.
 */
function scanTomlString(source: string, index: number, allowMultiline = true): TomlScan {
  const multiline = allowMultiline && (source.startsWith('"""', index) || source.startsWith("'''", index));
  const quote = multiline ? source.slice(index, index + 3) : (source[index] ?? "");
  if (quote !== '"' && quote !== "'" && quote !== '"""' && quote !== "'''") {
    return { end: index, hit: null, bail: true };
  }
  const literal = quote.startsWith("'");
  const label = stringLabel(multiline, literal);
  let cursor = index + quote.length;
  if (multiline && source[cursor] === "\r" && source[cursor + 1] === "\n") {
    cursor += 2;
  } else if (multiline && source[cursor] === "\n") {
    cursor += 1;
  }
  while (cursor < source.length) {
    const char = source[cursor] ?? "";
    if (!literal && char === "\\") {
      const escaped = scanBasicEscape(source, cursor + 1, multiline);
      if (escaped.hit || escaped.bail) {
        return escaped;
      }
      cursor = escaped.end;
      continue;
    }
    if (source.startsWith(quote, cursor)) {
      return { end: cursor + quote.length, hit: null, bail: false };
    }
    if (multiline && (char === "\n" || (char === "\r" && source[cursor + 1] === "\n"))) {
      cursor += char === "\r" ? 2 : 1;
      continue;
    }
    if (multiline && char === quote[0]) {
      cursor += 1;
      continue;
    }
    if (!isTomlStringChar(char, literal)) {
      return { end: cursor, hit: { index: cursor, message: label }, bail: false };
    }
    cursor += 1;
  }
  return { end: cursor, hit: { index: cursor, message: label }, bail: false };
}

function scanBasicEscape(source: string, index: number, multiline: boolean): TomlScan {
  const char = source[index] ?? "";
  if (char === "b" || char === "f" || char === "n" || char === "r" || char === "t" || char === '"' || char === "\\") {
    return { end: index + 1, hit: null, bail: false };
  }
  if (char === "u" || char === "U") {
    const width = char === "u" ? 4 : 8;
    const label = `invalid unicode ${width}-digit hex code`;
    let hex = "";
    for (let offset = 1; offset <= width; offset += 1) {
      const digit = source[index + offset] ?? "";
      if (!isTomlHex(digit)) {
        return { end: index, hit: { index: index + 1, message: label }, bail: false };
      }
      hex += digit;
    }
    const code = Number.parseInt(hex, 16);
    if (code > 0x10ffff || (code >= 0xd800 && code <= 0xdfff)) {
      return { end: index, hit: { index: index + 1, message: `${label}\nvalue is out of range` }, bail: false };
    }
    return { end: index + 1 + width, hit: null, bail: false };
  }
  if (multiline && (char === " " || char === "\t" || char === "\n" || char === "\r")) {
    let cursor = index;
    while (source[cursor] === " " || source[cursor] === "\t") {
      cursor += 1;
    }
    if (source[cursor] !== "\n" && source[cursor] !== "\r") {
      return escapeSequenceHit(index);
    }
    if (source[cursor] === "\r") {
      cursor += 1;
    }
    if (source[cursor] === "\n") {
      cursor += 1;
    }
    while (source[cursor] === " " || source[cursor] === "\t" || source[cursor] === "\n" || source[cursor] === "\r") {
      cursor += 1;
    }
    return { end: cursor, hit: null, bail: false };
  }
  // `dispatch` consumes the bad letter, so the caret sits on the next character.
  return escapeSequenceHit(index);
}

function escapeSequenceHit(index: number): TomlScan {
  return { end: index, hit: { index: index + 1, message: ESCAPE_SEQUENCE_MESSAGE }, bail: false };
}

function tomlWord(source: string, index: number): string {
  let cursor = index;
  while (/[A-Za-z]/.test(source[cursor] ?? "")) {
    cursor += 1;
  }
  return source.slice(index, cursor);
}

function isTomlDigit(char: string): boolean {
  return char >= "0" && char <= "9";
}

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
  | { kind: "sequence"; items?: FigmentScalar[] }
  | { kind: "map"; entries?: { key: string; value: FigmentScalar }[] };

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
