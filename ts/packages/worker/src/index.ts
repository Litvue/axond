import { Client } from "pg";

import { createAdmission, createAxond, createMetrics, defaultAdmission, resolveTelemetry, usageLine } from "@axond/gateway";
import { rateLimitExtension } from "@axond/rate-limit";
import type { AxondExtension, CredentialConfig, PriceRule, ProviderConfig, Store } from "@axond/sdk";

import { discoverOnce, type CatalogMetrics } from "../../cli/src/discovery.ts";
import { applyPostgresMigrationOn, createPostgresStore, POSTGRES_SCHEMA } from "../../cli/src/postgres-store.ts";

export interface WorkerEnv {
  HYPERDRIVE: { connectionString: string };
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
 * Extension migrations lock `axond_schema_lock` with a row update Hyperdrive can run.
 * A failed schema apply is not kept, so the next request can retry. A
 * successful apply stays for the isolate.
 * `PRICES_JSON` is the price list a chat uses when it settles.
 * One gateway lives for the isolate, so a parked credential stays parked.
 * Settlement is bound to the request that owns it.
 */
let metrics: ReturnType<typeof createMetrics> | undefined;
let admission = createAdmission(defaultAdmission());
const handlers = new Map<string, ReturnType<typeof createHandler>>();

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
    const client = new Client({ connectionString: env.HYPERDRIVE.connectionString });
    await client.connect();
    try {
      await schema.run(() => prepareWorkerSchema(client, extensions));
    } catch (error) {
      await client.end().catch(() => undefined);
      throw error;
    }
    return {
      client: {
        query: async (sql: string, params?: readonly unknown[]) => {
          const result = await client.query(sql, params ? [...(params as unknown[])] : []);
          return { rows: result.rows as Record<string, unknown>[], rowCount: result.rowCount };
        },
      },
      release: () => client.end(),
    };
  }, metrics);
  const providers = JSON.parse(env.PROVIDERS_JSON) as ProviderConfig[];
  const credentials = JSON.parse(env.CREDENTIALS_JSON ?? "[]") as CredentialConfig[];
  const catalog = workerCatalog(env);
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
      ctx.waitUntil(discoverOnce({
        store,
        providers,
        credentials,
        catalog,
        fetchImpl,
        metrics,
        onLog: (record) => {
          console.log(JSON.stringify(record));
        },
      }));
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
  await client.query(POSTGRES_SCHEMA);
  const executor = {
    query: async (sql: string, params?: readonly unknown[]) => {
      const result = params === undefined ? await client.query(sql) : await client.query(sql, [...params]);
      const row = Array.isArray(result) ? result[result.length - 1] : result;
      return { rows: (row?.rows ?? []) as Record<string, unknown>[], rowCount: row?.rowCount ?? null };
    },
  };
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
  return { source, sourceUrl: env.CATALOG_SOURCE_URL ?? null };
}

/** Run one discovery pass against a store the caller already opened. */
export function discoverOnSchedule(
  env: WorkerEnv,
  store: Store,
  ctx: WaitContext,
  fetchImpl?: typeof fetch,
  metrics?: CatalogMetrics,
): void {
  const providers = JSON.parse(env.PROVIDERS_JSON) as ProviderConfig[];
  const credentials = JSON.parse(env.CREDENTIALS_JSON ?? "[]") as CredentialConfig[];
  ctx.waitUntil(
    discoverOnce({
      store,
      providers,
      credentials,
      catalog: workerCatalog(env),
      fetchImpl,
      metrics,
      onLog: (record) => {
        console.log(JSON.stringify(record));
      },
    }),
  );
}
