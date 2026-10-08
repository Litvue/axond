import { Client } from "pg";

import { createAdmission, createAxond, createMetrics, defaultAdmission, resolveTelemetry, usageLine } from "@axond/gateway";
import { StoreFailure } from "../../gateway/src/errors.ts";
import { rateLimitExtension } from "@axond/rate-limit";
import type { AxondExtension, CredentialConfig, PriceRule, ProviderConfig, Store } from "@axond/sdk";

import { discoverOnce, noteCatalogRefusal, type CatalogMetrics } from "../../cli/src/discovery.ts";
import {
  applyPostgresMigrationOn,
  applyPostgresSchema,
  closePgClient,
  createPostgresStore,
  holdPgClient,
  hyperdriveClientOptions,
  queryPgClient,
} from "../../cli/src/postgres-store.ts";

export interface WorkerEnv {
  HYPERDRIVE: {
    connectionString: string;
    host?: string;
    port?: number;
    user?: string;
    password?: string;
    database?: string;
  };
  GATEWAY_KEY: string;
  PROVIDERS_JSON: string;
  /** Price rules. Absent means models stay unpriced and a successful chat does not move spent. */
  PRICES_JSON?: string;
  OTEL_EXPORTER_OTLP_ENDPOINT?: string;
  OTEL_EXPORTER_OTLP_PROTOCOL?: string;
  AXOND_INSTANCE_ID?: string;
  CREDENTIALS_JSON?: string;
  CATALOG_SOURCE?: string;
  CATALOG_SOURCE_URL?: string;
}

interface WaitContext {
  waitUntil(promise: Promise<unknown>): void;
}

/**
 * Worker entry. Extensions are static imports, so the Worker bundler includes
 * them. The process binary loads the same contract from `AXOND_EXTENSIONS_DIR`
 * at startup instead. Hyperdrive supplies the Postgres connection string;
 * each call opens one client and closes it, with no session-level SET.
 * A socket reset is listened for, so it cannot take down the isolate. The
 * client is closed before the error returns, including when the handshake
 * never emits close and when the peer stays open after Terminate.
 * Extension migrations lock `axond_schema_lock` with a row update Hyperdrive can run.
 * A Hyperdrive role with read and write grants and no CREATE skips DDL when
 * the schema is already present, and names a missing table or column without
 * sending the script.
 * A failed schema apply is not kept, so the next request can retry. A
 * successful apply stays for the isolate. A request that arrives while the
 * script is running waits before it connects, so the isolate holds one
 * Hyperdrive client until that script commits.
 * `PRICES_JSON` is the price list a chat uses when it settles.
 * One gateway lives for the isolate, so a parked credential stays parked.
 * Settlement is bound to the request that owns it.
 */
let metrics: ReturnType<typeof createMetrics> | undefined;
let admission = createAdmission(defaultAdmission());
const handlers = new Map<string, ReturnType<typeof createHandler>>();

/**
 * The client response stays `store is unavailable`. A schema message we wrote
 * is logged. A driver message is not, so a DSN cannot reach the log.
 */
export function rethrowSchemaFailure(error: unknown): never {
  const message = error instanceof Error ? error.message : "";
  if (message.startsWith("postgres schema is missing ") || /^extension migration \S+ failed$/.test(message)) {
    console.log(JSON.stringify({ msg: "schema_unavailable", detail: message }));
  }
  throw new StoreFailure();
}

/**
 * The caller that applies the schema already holds `open()`'s client and
 * reuses it for the request. Anyone else waits until that apply finishes,
 * then opens a client of their own. `open` closes a client it fails to
 * connect and then throws. A prepare failure is closed here before it
 * propagates, so the next attempt can connect again.
 */
export async function openSchemaClient<T>(
  schema: { run(apply: () => Promise<void>): Promise<void> },
  open: () => Promise<T>,
  prepare: (client: T) => Promise<void>,
  close: (client: T) => Promise<void>,
): Promise<T> {
  let owned: { client: T } | undefined;
  await schema.run(async () => {
    const client = await open();
    try {
      await prepare(client);
    } catch (error) {
      await close(client);
      throw error;
    }
    owned = { client };
  });
  if (owned) {
    return owned.client;
  }
  return open();
}

/**
 * One in-flight attempt. A rejection is dropped so the next caller can try
 * again. A success stays, including for callers that arrived while it ran.
 */
export function schemaAttempt(): { run(apply: () => Promise<void>): Promise<void> } {
  let pending: Promise<void> | null = null;
  return {
    async run(apply) {
      if (pending === null) {
        const attempt = apply().then(
          () => undefined,
          (error: unknown) => {
            if (pending === attempt) {
              pending = null;
            }
            throw error;
          },
        );
        pending = attempt;
      }
      await pending;
    },
  };
}

export function createHandler(env: WorkerEnv, storeOverride?: Store) {
  metrics ??= createMetrics([env.GATEWAY_KEY]);
  const schema = schemaAttempt();
  const extensions = [rateLimitExtension({ limit: 60, windowMs: 60_000, mode: "isolate" })];
  const store = storeOverride ?? createPostgresStore(async () => {
    // A silent accept and a silent statement use Hyperdrive's 15s and 60s
    // limits. Structured fields keep a password that `connectionString` cannot
    // carry. See `hyperdriveClientOptions`. Callers that arrive during the
    // schema script wait in `openSchemaClient` before they connect.
    let client: Client;
    try {
      client = await openSchemaClient(
        schema,
        async () => {
          const opened = holdPgClient(new Client(hyperdriveClientOptions(env.HYPERDRIVE)));
          try {
            await opened.connect();
          } catch (error) {
            await closePgClient(opened);
            throw error;
          }
          return opened;
        },
        (opened) => prepareWorkerSchema(opened, extensions),
        (opened) => closePgClient(opened),
      );
    } catch (error) {
      rethrowSchemaFailure(error);
    }
    return {
      client: {
        query: async (sql: string, params?: readonly unknown[]) => {
          const result = await queryPgClient(client, sql, params ? [...params] : []);
          return { rows: result.rows as Record<string, unknown>[], rowCount: result.rowCount };
        },
      },
      release: () => closePgClient(client),
    };
  }, metrics);
  const providers = JSON.parse(env.PROVIDERS_JSON) as ProviderConfig[];
  const credentials = JSON.parse(env.CREDENTIALS_JSON ?? "[]") as CredentialConfig[];
  const telemetry = resolveTelemetry({
    endpoint: env.OTEL_EXPORTER_OTLP_ENDPOINT,
    protocol: env.OTEL_EXPORTER_OTLP_PROTOCOL,
    instanceId: env.AXOND_INSTANCE_ID,
  });
  const waits = new WeakMap<Request, WaitContext>();
  const app = createAxond({
    store,
    gatewayKey: env.GATEWAY_KEY,
    defaultNamespace: "platform",
    providers,
    credentials,
    prices: workerPrices(env.PRICES_JSON),
    extensions,
    waitUntil: (promise, request) => {
      waits.get(request)?.waitUntil(promise);
    },
    admissionControl: admission,
    metrics,
    telemetry: telemetry ?? undefined,
    onLog: (record) => {
      console.log(JSON.stringify(record));
    },
    onUsage: (record) => {
      console.log(usageLine(record));
    },
  });
  return {
    scheduled(ctx: WaitContext, fetchImpl?: typeof fetch): void {
      ctx.waitUntil(runScheduledDiscovery(env, store, fetchImpl, metrics));
    },
    fetch(request: Request, ctx: WaitContext): Response | Promise<Response> {
      waits.set(request, ctx);
      return app.fetch(request);
    },
  };
}

/** One gateway per env on this isolate. A new credential list or price list builds another. */
export function handlerFor(env: WorkerEnv) {
  const key = [
    env.HYPERDRIVE.connectionString,
    env.HYPERDRIVE.host ?? "",
    String(env.HYPERDRIVE.port ?? ""),
    env.HYPERDRIVE.user ?? "",
    env.HYPERDRIVE.password ?? "",
    env.HYPERDRIVE.database ?? "",
    env.GATEWAY_KEY,
    env.PROVIDERS_JSON,
    env.PRICES_JSON ?? "",
    env.CREDENTIALS_JSON ?? "",
    env.OTEL_EXPORTER_OTLP_ENDPOINT ?? "",
    env.OTEL_EXPORTER_OTLP_PROTOCOL ?? "",
    env.AXOND_INSTANCE_ID ?? "",
    env.CATALOG_SOURCE ?? "",
    env.CATALOG_SOURCE_URL ?? "",
  ].join("\0");
  const existing = handlers.get(key);
  if (existing) {
    return existing;
  }
  const created = createHandler(env);
  handlers.set(key, created);
  return created;
}

export default {
  fetch(request: Request, env: WorkerEnv, ctx: WaitContext): Promise<Response> {
    return Promise.resolve(handlerFor(env).fetch(request, ctx));
  },
  scheduled(_event: unknown, env: WorkerEnv, ctx: WaitContext): void {
    handlerFor(env).scheduled(ctx);
  },
};

async function prepareWorkerSchema(client: Client, extensions: readonly AxondExtension[]): Promise<void> {
  const executor = {
    query: async (sql: string, params?: readonly unknown[]) => {
      const result = await queryPgClient(client, sql, params);
      const row = Array.isArray(result) ? result[result.length - 1] : result;
      return { rows: (row?.rows ?? []) as Record<string, unknown>[], rowCount: row?.rowCount ?? null };
    },
  };
  await applyPostgresSchema(executor);
  for (const extension of extensions) {
    for (const [index, sql] of (extension.migrations ?? []).entries()) {
      await applyPostgresMigrationOn(executor, `${extension.name}:${index}`, sql);
    }
  }
}

function workerPrices(raw: string | undefined): PriceRule[] {
  const rows = JSON.parse(raw && raw.length > 0 ? raw : "[]") as {
    provider: string;
    model: string;
    inputMicrodollarsPerMillion: number | string;
    outputMicrodollarsPerMillion: number | string;
  }[];
  return rows.map((row) => ({
    provider: row.provider,
    model: row.model,
    inputMicrodollarsPerMillion: BigInt(row.inputMicrodollarsPerMillion),
    outputMicrodollarsPerMillion: BigInt(row.outputMicrodollarsPerMillion),
  }));
}

function workerCatalog(env: WorkerEnv): { source: "none" | "models-dev" | "seed"; sourceUrl: string | null } {
  const source = env.CATALOG_SOURCE === "models-dev" || env.CATALOG_SOURCE === "seed" ? env.CATALOG_SOURCE : "none";
  return { source, sourceUrl: env.CATALOG_SOURCE_URL ?? (source === "models-dev" ? "https://models.dev/catalog.json" : null) };
}

/**
 * The CLI config loader admits only an `https` URL whose path ends in
 * `/catalog.json`. `api.json` and `models.json` are different documents.
 * A Worker env var skips that loader, so the cron checks the same shape
 * and does not fetch a document it would store under the wrong keys.
 */
function supportedModelsDevCatalogUrl(sourceUrl: string): boolean {
  try { const url = new URL(sourceUrl); if (!url.hostname || url.username || url.password) return false; } catch { return false; }
  const match = /^([a-zA-Z][a-zA-Z0-9+.-]*):\/\/(.*)$/s.exec(sourceUrl);
  if (!match || match[1]!.toLowerCase() !== "https") {
    return false;
  }
  const rest = match[2] ?? "";
  if (rest.length === 0) {
    return false;
  }
  const authority = rest.split(/[/?#]/, 1)[0] ?? "";
  if (authority.length === 0 || authority.includes("@")) {
    return false;
  }
  const path = rest.split(/[?#]/, 1)[0] ?? rest;
  return path.endsWith("/catalog.json");
}

function discoveryCatalog(env: WorkerEnv): { catalog: ReturnType<typeof workerCatalog>; refused: boolean } {
  const catalog = workerCatalog(env);
  if (catalog.source === "models-dev" && !supportedModelsDevCatalogUrl(catalog.sourceUrl ?? "")) {
    return { catalog: { source: "none", sourceUrl: null }, refused: true };
  }
  return { catalog, refused: false };
}

/** Run one discovery pass against a store the caller already opened. */
export function discoverOnSchedule(
  env: WorkerEnv,
  store: Store,
  ctx: WaitContext,
  fetchImpl?: typeof fetch,
  metrics?: CatalogMetrics,
): void {
  ctx.waitUntil(runScheduledDiscovery(env, store, fetchImpl, metrics));
}

async function runScheduledDiscovery(
  env: WorkerEnv,
  store: Store,
  fetchImpl?: typeof fetch,
  metrics?: CatalogMetrics,
): Promise<void> {
  const providers = JSON.parse(env.PROVIDERS_JSON) as ProviderConfig[];
  const credentials = JSON.parse(env.CREDENTIALS_JSON ?? "[]") as CredentialConfig[];
  const { catalog, refused } = discoveryCatalog(env);
  if (refused) {
    await noteCatalogRefusal({ store, metrics, onLog: (record) => console.log(JSON.stringify(record)) }, "unsupported_endpoint");
  }
  return discoverOnce({
    store,
    providers,
    credentials,
    catalog,
    fetchImpl,
    metrics,
    onLog: (record) => {
      console.log(JSON.stringify(record));
    },
  });
}
