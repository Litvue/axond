import { Client } from "pg";

import { createAdmission, createAxond, createMetrics, defaultAdmission, resolveTelemetry } from "@axond/gateway";
import { rateLimitExtension } from "@axond/rate-limit";
import type { AxondExtension, CredentialConfig, ProviderConfig, Store } from "@axond/sdk";

import { discoverOnce, type CatalogMetrics } from "../../cli/src/discovery.ts";
import { applyPostgresMigrationOn, createPostgresStore, POSTGRES_SCHEMA } from "../../cli/src/postgres-store.ts";

export interface WorkerEnv {
  HYPERDRIVE: { connectionString: string };
  GATEWAY_KEY: string;
  PROVIDERS_JSON: string;
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
 */
let metrics: ReturnType<typeof createMetrics> | undefined;
let admission = createAdmission(defaultAdmission());

export function createHandler(env: WorkerEnv, storeOverride?: Store) {
  metrics ??= createMetrics([env.GATEWAY_KEY]);
  let schema: Promise<void> | null = null;
  const extensions = [rateLimitExtension({ limit: 60, windowMs: 60_000, mode: "isolate" })];
  const store = storeOverride ?? createPostgresStore(async () => {
    const client = new Client({ connectionString: env.HYPERDRIVE.connectionString });
    await client.connect();
    if (schema === null) {
      schema = prepareWorkerSchema(client, extensions);
    }
    await schema;
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
      const app = createAxond({
        store,
        gatewayKey: env.GATEWAY_KEY,
        defaultNamespace: "platform",
        providers,
        credentials,
        extensions,
        waitUntil: (promise) => ctx.waitUntil(promise),
        admissionControl: admission,
        metrics,
        telemetry: telemetry ?? undefined,
        onLog: (record) => {
          console.log(JSON.stringify(record));
        },
      });
      return app.fetch(request);
    },
  };
}

export default {
  fetch(request: Request, env: WorkerEnv, ctx: WaitContext): Promise<Response> {
    return Promise.resolve(createHandler(env).fetch(request, ctx));
  },
  scheduled(_event: unknown, env: WorkerEnv, ctx: WaitContext): void {
    createHandler(env).scheduled(ctx);
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
