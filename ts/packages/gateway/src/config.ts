import { parse } from "smol-toml";

import type { CredentialConfig, PriceRule, ProviderConfig, TransportLimits } from "@axond/sdk";

import { ADMISSION_MAX_PERMITS, clampStreams, validateAdmission, type AdmissionLimits } from "./admission.ts";

import { GatewayFailure } from "./errors.ts";

import { validateGlob } from "./glob.ts";
import {
  type SecretReader,
  type LoadedConfig,
  configLoad,
  applyEnvOverrides,
  rejectSectionShapes,
  POSITIONAL_AFTER_SERVER,
  asRecord,
  DEFAULT_TRANSPORT,
  type UsageSinkConfig,
  DEFAULT_USAGE_TABLE,
  DEFAULT_USAGE_BUFFER,
  DEFAULT_USAGE_BATCH,
  DEFAULT_USAGE_FLUSH_MS,
} from "./config-values.ts";
import { scanTomlDocument, formatTomlIntegerRange, tomlLeapSecondsAs59 } from "./config-toml.ts";
import {
  rejectExtractTypes,
  readServerBind,
  rejectAfterServerExtract,
  SECTIONS_AFTER_SERVER,
  projectPositional,
  rejectWithdrawn,
  rejectCollisions,
  configError,
  validateUsageIndex,
  readTypedInt,
  asUint,
  asArray,
  stringField,
  validatePriceBook,
  validateBlocklist,
  readVariant,
  CATALOG_REFRESH_INTERVAL_SECONDS,
  CATALOG_REFRESH_TIMEOUT_SECONDS,
  CATALOG_RETRY_INITIAL_SECONDS,
  CATALOG_RETRY_MAX_SECONDS,
  CATALOG_CONNECT_TIMEOUT_MS,
  CATALOG_OPERATION_TIMEOUT_MS,
  CATALOG_MAX_PAYLOAD_BYTES,
} from "./config-shapes.ts";
export { WITHDRAWN_SECTIONS } from "./config-values.ts";
export { type LoadedConfig} from "./config-values.ts";
export { type UsageSinkConfig} from "./config-values.ts";
export { usageBatchSize } from "./config-values.ts";
export { type SecretReader} from "./config-values.ts";

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
