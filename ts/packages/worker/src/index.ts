import { Client } from "pg";

import { createAxond, createMetrics } from "@axond/gateway";
import { rateLimitExtension } from "@axond/rate-limit";
import type { ProviderConfig } from "@axond/sdk";

import { createPostgresStore, POSTGRES_SCHEMA } from "../../cli/src/postgres-store.ts";

export interface WorkerEnv {
  HYPERDRIVE: { connectionString: string };
  GATEWAY_KEY: string;
  PROVIDERS_JSON: string;
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

export function createHandler(env: WorkerEnv) {
  metrics ??= createMetrics([env.GATEWAY_KEY]);
  let schema: Promise<void> | null = null;
  const store = createPostgresStore(async () => {
    const client = new Client({ connectionString: env.HYPERDRIVE.connectionString });
    await client.connect();
    if (schema === null) {
      schema = client.query(POSTGRES_SCHEMA).then(() => undefined);
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
  });
  const providers = JSON.parse(env.PROVIDERS_JSON) as ProviderConfig[];
  return {
    fetch(request: Request, ctx: WaitContext): Response | Promise<Response> {
      const app = createAxond({
        store,
        gatewayKey: env.GATEWAY_KEY,
        defaultNamespace: "platform",
        providers,
        extensions: [rateLimitExtension({ limit: 60, windowMs: 60_000, mode: "isolate" })],
        waitUntil: (promise) => ctx.waitUntil(promise),
        metrics,
      });
      return app.fetch(request);
    },
  };
}

export default {
  fetch(request: Request, env: WorkerEnv, ctx: WaitContext): Promise<Response> {
    return createHandler(env).fetch(request, ctx);
  },
};
