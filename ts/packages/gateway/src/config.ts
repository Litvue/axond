import { parse } from "smol-toml";

import type { CredentialConfig, PriceRule, ProviderConfig, TransportLimits } from "@axond/sdk";

import { GatewayFailure } from "./errors.ts";
import { validateGlob } from "./glob.ts";
import { parseNamespaceId } from "./namespace.ts";

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
  storage: { backend: "sqlite" | "postgres"; path?: string; dsn?: string; createTable: boolean };
  namespaces: { id: string; default: boolean; allowPlatformFallback: boolean }[];
  providers: ProviderConfig[];
  credentials: CredentialConfig[];
  gatewayKey: string;
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
  let storage: LoadedConfig["storage"];
  if (backend === "sqlite") {
    const path = typeof storageRaw["path"] === "string" ? storageRaw["path"] : "";
    if (path.length === 0) {
      throw configError('`[storage] path` is required when `backend = "sqlite"`');
    }
    storage = { backend, path, createTable };
  } else {
    const dsnEnv = typeof storageRaw["dsn_env"] === "string" ? storageRaw["dsn_env"] : "";
    if (dsnEnv.length === 0) {
      throw configError('`[storage] dsn_env` is required when `backend = "postgres"`');
    }
    const dsn = secrets.env(dsnEnv);
    if (dsn === undefined || dsn.length === 0) {
      throw configError(`\`[storage] dsn_env\` names \`${dsnEnv}\`, which is unset`);
    }
    storage = { backend, dsn, createTable };
  }

  const namespaces = asArray(parsed["namespace"]).map((entry) => {
    const row = asRecord(entry) ?? {};
    const id = typeof row["id"] === "string" ? row["id"] : "";
    parseNamespaceId(id);
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
    credentials.push({
      namespace,
      provider,
      secret,
      id: typeof row["id"] === "string" && row["id"].length > 0 ? row["id"] : envName,
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
  if (envName !== null) {
    const value = secrets.env(envName);
    if (value === undefined || value.length === 0) {
      throw configError(`gateway_key env \`${envName}\` is unset`);
    }
    gatewayKey = value;
  } else {
    try {
      gatewayKey = (await secrets.file(fileName!)).replace(/\r?\n$/, "");
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
  const transport: TransportLimits = {
    responseHeaderTimeoutMs: numberField(transportRaw, "response_header_timeout_ms", DEFAULT_TRANSPORT.responseHeaderTimeoutMs),
    bufferedBodyTimeoutMs: numberField(transportRaw, "buffered_body_timeout_ms", DEFAULT_TRANSPORT.bufferedBodyTimeoutMs),
    streamIdleTimeoutMs: numberField(transportRaw, "stream_idle_timeout_ms", DEFAULT_TRANSPORT.streamIdleTimeoutMs),
    maxResponseBytes: DEFAULT_TRANSPORT.maxResponseBytes,
  };
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
    gatewayKeyNamespace: keyNamespace,
    defaultNamespace,
    prices,
    blocklist,
    transport,
    discoveryIntervalSeconds,
    shutdown,
    catalog,
    extensionsDir,
  };
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
