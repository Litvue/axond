import pg from "pg";

import type {
  BudgetLedger,
  BudgetPolicyWrite,
  NamespaceWrite,
  ProviderModelCache,
  SettleInput,
  SqlValue,
  Store,
} from "@axond/sdk";

import { FIXED_CADENCE_NEEDS_PERIOD, GatewayFailure, StoreFailure } from "../../gateway/src/errors.ts";
import { encodeAttrs } from "../../gateway/src/strict-json.ts";
import { budgetPolicyFromLedger, foldUsageSummary, saturateMicrodollars } from "../../gateway/src/memory-store.ts";
import { monthlyPeriod } from "../../gateway/src/namespace.ts";
import {
  recordConnectionDiscarded,
  recordConnectionOpened,
  recordStoreCall,
  type StoreMetrics,
  type StoreOperation,
} from "../../gateway/src/store-metrics.ts";

export interface SqlExecutor {
  query(sql: string, params?: readonly SqlValue[]): Promise<{ rows: Record<string, unknown>[]; rowCount: number | null }>;
}

const I64_MAX = "9223372036854775807";

/**
 * A socket reset emits `error` on the client. With no listener, Node throws
 * that event and the process exits. The query promise still rejects, and the
 * caller closes the client. The listener does not record the driver text.
 */
export function holdPgClient(client: pg.Client): pg.Client {
  client.on("error", () => undefined);
  return client;
}

/**
 * One connection per call. Hyperdrive pools on the far side, so Workers should
 * pass a connector that opens `pg.Client` against the Hyperdrive string and
 * closes it in `release`. Do not issue session-level SET.
 */
export function createPostgresStore(
  connect: () => Promise<{ client: SqlExecutor; release: () => Promise<void> }>,
  metrics?: StoreMetrics,
): Store {
  async function withClient<T>(operation: StoreOperation | null, fn: (client: SqlExecutor) => Promise<T>): Promise<T> {
    const called = Date.now();
    let opened: { client: SqlExecutor; release: () => Promise<void> };
    try {
      opened = await connect();
    } catch (error) {
      recordStoreCall(metrics, "postgres", operation, Date.now() - called, null, "error");
      if (error instanceof GatewayFailure) {
        throw error;
      }
      throw new StoreFailure();
    }
    recordConnectionOpened(metrics);
    const acquired = Date.now();
    try {
      const value = await fn(opened.client);
      recordStoreCall(metrics, "postgres", operation, acquired - called, Date.now() - acquired, "ok");
      return value;
    } catch (error) {
      recordStoreCall(metrics, "postgres", operation, acquired - called, Date.now() - acquired, "error");
      if (error instanceof GatewayFailure || error instanceof StoreFailure) {
        throw error;
      }
      throw new StoreFailure();
    } finally {
      await opened.release().catch(() => undefined);
      recordConnectionDiscarded(metrics);
    }
  }

  return {
    async query(sql, params = []) {
      const text = postgresQueryText(sql, params.length);
      return withClient(null, async (client) => {
        const result = await client.query(text, params);
        return { rows: result.rows };
      });
    },
    async resolveNamespace(id, nowMs) {
      return withClient("namespace_resolve", async (client) => {
        const found = await client.query(
          `SELECT n.id, n.attrs, n.blocklist, n.allow_platform_fallback, n.from_config,
                  c.cadence, c.limit_microdollars AS cadence_limit, c.timezone,
                  a.period AS active_period, b.limit_microdollars, b.spent_microdollars, i.n
           FROM public.axond_namespace n
           LEFT JOIN public.axond_store_budget_cadence c ON c.namespace = n.id
           LEFT JOIN public.axond_store_budget_active a ON a.namespace = n.id
           LEFT JOIN public.axond_store_budget b ON b.namespace = a.namespace AND b.period = a.period
           LEFT JOIN public.axond_namespace_incarnation i ON i.id = n.id
           WHERE n.id = $1`,
          [id],
        );
        const row = found.rows[0];
        if (!row) {
          return null;
        }
        const record = namespaceFrom(row);
        const incarnation = row["n"] == null ? 1n : BigInt(String(row["n"]));
        if (row["cadence"] === "monthly") {
          const period = monthlyPeriod(nowMs, String(row["timezone"]));
          const existing = await client.query(
            "SELECT limit_microdollars, spent_microdollars FROM public.axond_store_budget WHERE namespace = $1 AND period = $2",
            [id, period],
          );
          if (existing.rows[0]) {
            return admittedBudget(record, period, existing.rows[0], incarnation);
          }
          return withTransaction(client, async (client) => {
            await lockNamespace(client, id);
            const still = await client.query("SELECT id FROM public.axond_namespace WHERE id = $1", [id]);
            if (!still.rows[0]) {
              return null;
            }
            await client.query(
              `INSERT INTO public.axond_store_budget (namespace, period, limit_microdollars, spent_microdollars)
               VALUES ($1, $2, $3, 0)
               ON CONFLICT (namespace, period) DO NOTHING`,
              [id, period, row["cadence_limit"]],
            );
            const created = await client.query(
              "SELECT limit_microdollars, spent_microdollars FROM public.axond_store_budget WHERE namespace = $1 AND period = $2",
              [id, period],
            );
            if (!created.rows[0]) {
              return { record, period, limit: null, spent: null, incarnation, admitted: false };
            }
            return admittedBudget(record, period, created.rows[0], incarnation);
          });
        }
        const period = row["active_period"] == null ? null : String(row["active_period"]);
        if (period === null || row["limit_microdollars"] == null) {
          return { record, period, limit: null, spent: null, incarnation, admitted: false };
        }
        return admittedBudget(record, period, row, incarnation);
      });
    },
    async putNamespace(record) {
      return withClient("namespace_write", (client) => withTransaction(client, async (client) => {
        await lockNamespace(client, record.id);
        const result = await client.query(
          `INSERT INTO public.axond_namespace (id, attrs, blocklist, allow_platform_fallback, from_config)
           VALUES ($1, $2::jsonb, $3::jsonb, $4, $5)
           ON CONFLICT (id) DO NOTHING`,
          [
            record.id,
            encodeAttrs(record.attrs),
            record.blocklist === null ? null : JSON.stringify(record.blocklist),
            record.allowPlatformFallback,
            record.fromConfig,
          ],
        );
        return (result.rowCount ?? 0) > 0 ? "created" : "exists";
      }));
    },
    async adoptConfigNamespace(id, allowPlatformFallback) {
      await withClient("namespace_write", async (client) => {
        await client.query(
          `INSERT INTO public.axond_namespace (id, attrs, blocklist, allow_platform_fallback, from_config)
           VALUES ($1, $2::jsonb, NULL, $3, true)
           ON CONFLICT (id) DO UPDATE SET
             allow_platform_fallback = EXCLUDED.allow_platform_fallback,
             from_config = true`,
          [id, encodeAttrs({}), allowPlatformFallback],
        );
      });
    },
    async releaseConfigNamespace(id) {
      await withClient("namespace_write", async (client) => {
        await client.query("UPDATE public.axond_namespace SET from_config = false WHERE id = $1", [id]);
      });
    },
    async getNamespace(id) {
      return withClient("namespace_read", async (client) => {
        const result = await client.query("SELECT * FROM public.axond_namespace WHERE id = $1", [id]);
        return result.rows[0] ? namespaceFrom(result.rows[0]) : null;
      });
    },
    async updateNamespace(id, attrs, blocklist) {
      return withClient("namespace_write", async (client) => {
        const result = await client.query(
          `UPDATE public.axond_namespace SET attrs = $2::jsonb, blocklist = $3::jsonb WHERE id = $1 RETURNING *`,
          [id, encodeAttrs(attrs), blocklist === null ? null : JSON.stringify(blocklist)],
        );
        return result.rows[0] ? namespaceFrom(result.rows[0]) : null;
      });
    },
    async deleteNamespace(id) {
      return withClient("namespace_write", (client) => withTransaction(client, async (client) => {
        await lockNamespace(client, id);
        const deleted = await client.query("DELETE FROM public.axond_namespace WHERE id = $1", [id]);
        if ((deleted.rowCount ?? 0) === 0) {
          return false;
        }
        await client.query("DELETE FROM public.axond_store_budget WHERE namespace = $1", [id]);
        await client.query("DELETE FROM public.axond_store_budget_active WHERE namespace = $1", [id]);
        await client.query("DELETE FROM public.axond_store_budget_cadence WHERE namespace = $1", [id]);
        await client.query(
          `INSERT INTO public.axond_namespace_incarnation (id, n) VALUES ($1, 2)
           ON CONFLICT (id) DO UPDATE SET n = public.axond_namespace_incarnation.n + 1`,
          [id],
        );
        return true;
      }));
    },
    async listNamespaces(cursor, limit) {
      return withClient("namespace_read", async (client) => {
        const result = await client.query(
          "SELECT * FROM public.axond_namespace WHERE ($1::text IS NULL OR id > $1) ORDER BY id LIMIT $2",
          [cursor, limit + 1],
        );
        const page = result.rows.slice(0, limit).map(namespaceFrom);
        const nextCursor = result.rows.length > limit ? page[page.length - 1]!.id : null;
        return { data: page, nextCursor };
      });
    },
    async putBudget(namespace, period, limit, nowMs = Date.now()) {
      return withClient("budget_write", (client) => withTransaction(client, async (client) => {
        await lockNamespace(client, namespace);
        const known = await client.query("SELECT id FROM public.axond_namespace WHERE id = $1", [namespace]);
        if (!known.rows[0]) {
          throw new GatewayFailure("unknown_namespace", 404, "unknown namespace");
        }
        await client.query(
          `INSERT INTO public.axond_store_budget (namespace, period, limit_microdollars, spent_microdollars)
           VALUES ($1, $2, $3::bigint, 0)
           ON CONFLICT (namespace, period) DO UPDATE SET limit_microdollars = EXCLUDED.limit_microdollars`,
          [namespace, period, limit.toString()],
        );
        await client.query(
          `INSERT INTO public.axond_store_budget_active (namespace, period) VALUES ($1, $2)
           ON CONFLICT (namespace) DO UPDATE SET period = EXCLUDED.period`,
          [namespace, period],
        );
        return (await readBudget(client, namespace, period, nowMs))!;
      }));
    },
    async getBudget(namespace, period, nowMs = Date.now()) {
      return withClient("budget_read", async (client) => {
        const known = await client.query("SELECT id FROM public.axond_namespace WHERE id = $1", [namespace]);
        if (!known.rows[0]) {
          throw new GatewayFailure("unknown_namespace", 404, "unknown namespace");
        }
        return readBudget(client, namespace, period, nowMs);
      });
    },
    async putBudgetPolicy(input: BudgetPolicyWrite) {
      return withClient("budget_write", (client) => withTransaction(client, async (client) => {
        await lockNamespace(client, input.namespace);
        const known = await client.query("SELECT id FROM public.axond_namespace WHERE id = $1", [input.namespace]);
        if (!known.rows[0]) {
          throw new GatewayFailure("unknown_namespace", 404, "unknown namespace");
        }
        let period = input.period;
        if (input.cadence === "monthly") {
          period = monthlyPeriod(input.nowMs, input.timezone);
        } else if (!period) {
          const active = await client.query("SELECT period FROM public.axond_store_budget_active WHERE namespace = $1", [
            input.namespace,
          ]);
          period = active.rows[0] ? String(active.rows[0]["period"]) : null;
          if (!period) {
            throw new GatewayFailure("bad_request", 400, FIXED_CADENCE_NEEDS_PERIOD);
          }
        }
        await client.query(
          `INSERT INTO public.axond_store_budget_cadence (namespace, cadence, limit_microdollars, timezone)
           VALUES ($1, $2, $3::bigint, $4)
           ON CONFLICT (namespace) DO UPDATE SET cadence = EXCLUDED.cadence, limit_microdollars = EXCLUDED.limit_microdollars, timezone = EXCLUDED.timezone`,
          [input.namespace, input.cadence, input.limit.toString(), input.timezone],
        );
        await client.query(
          `INSERT INTO public.axond_store_budget (namespace, period, limit_microdollars, spent_microdollars)
           VALUES ($1, $2, $3::bigint, 0)
           ON CONFLICT (namespace, period) DO UPDATE SET limit_microdollars = EXCLUDED.limit_microdollars`,
          [input.namespace, period, input.limit.toString()],
        );
        if (input.cadence === "fixed") {
          await client.query(
            `INSERT INTO public.axond_store_budget_active (namespace, period) VALUES ($1, $2)
             ON CONFLICT (namespace) DO UPDATE SET period = EXCLUDED.period`,
            [input.namespace, period],
          );
        }
        return (await readPolicy(client, input.namespace, input.nowMs))!;
      }));
    },
    async getBudgetPolicy(namespace, nowMs = Date.now()) {
      return withClient("budget_read", async (client) => {
        const known = await client.query("SELECT id FROM public.axond_namespace WHERE id = $1", [namespace]);
        if (!known.rows[0]) {
          throw new GatewayFailure("unknown_namespace", 404, "unknown namespace");
        }
        return readPolicy(client, namespace, nowMs);
      });
    },
    settle(input) {
      return withClient("budget_charge", (client) => settlePostgres(client, input));
    },
    async summarizeUsage(namespace, period) {
      return withClient("usage_summary", async (client) => {
        const result = await client.query(
          `SELECT model, status, cost_microdollars::text AS cost
           FROM public.axond_store_usage WHERE namespace = $1 AND period = $2`,
          [namespace, period],
        );
        return foldUsageSummary(
          result.rows.map((row) => ({
            model: String(row["model"]),
            status: String(row["status"]),
            cost: row["cost"] === null ? null : BigInt(String(row["cost"])),
          })),
        );
      });
    },
    async listProviderModels() {
      return withClient("provider_models", async (client) => {
        const result = await client.query("SELECT * FROM public.axond_store_provider_models", []);
        return result.rows.map(modelFrom);
      });
    },
    async getProviderModels(provider) {
      return withClient("provider_models", async (client) => {
        const result = await client.query("SELECT * FROM public.axond_store_provider_models WHERE provider = $1", [provider]);
        return result.rows[0] ? modelFrom(result.rows[0]) : null;
      });
    },
    async upsertProviderModels(row) {
      return withClient("provider_models", async (client) => {
        await client.query(
          `INSERT INTO public.axond_store_provider_models (provider, fetched_at, stale, models, source)
           VALUES ($1, $2, $3, $4::jsonb, $5)
           ON CONFLICT (provider) DO UPDATE SET fetched_at = EXCLUDED.fetched_at, stale = EXCLUDED.stale, models = EXCLUDED.models, source = EXCLUDED.source
           WHERE public.axond_store_provider_models.source IS NOT DISTINCT FROM EXCLUDED.source
              OR public.axond_store_provider_models.stale = true`,
          [row.provider, row.fetchedAt, row.stale, JSON.stringify(row.data), row.source],
        );
      });
    },
    async markProviderModelsStale(provider) {
      return withClient("provider_models", async (client) => {
        await client.query(
          `INSERT INTO public.axond_store_provider_models (provider, fetched_at, stale, models, source)
           VALUES ($1, NULL, true, '[]'::jsonb, NULL)
           ON CONFLICT (provider) DO UPDATE SET stale = true`,
          [provider],
        );
      });
    },
    async noteCatalogRefusal() {
      return withClient(null, async (client) => {
        const result = await client.query(
          `INSERT INTO public.axond_catalog_streak (singleton, consecutive_refusals) VALUES ('catalog', 1)
           ON CONFLICT (singleton) DO UPDATE SET consecutive_refusals = public.axond_catalog_streak.consecutive_refusals + 1
           RETURNING consecutive_refusals`,
          [],
        );
        return Number(result.rows[0]?.["consecutive_refusals"] ?? 1);
      });
    },
    async resetCatalogStreak() {
      return withClient(null, async (client) => {
        await client.query(
          `INSERT INTO public.axond_catalog_streak (singleton, consecutive_refusals) VALUES ('catalog', 0)
           ON CONFLICT (singleton) DO UPDATE SET consecutive_refusals = 0`,
          [],
        );
      });
    },
  };
}

/**
 * One origin connection for the whole body. Hyperdrive pins a connection only
 * inside a transaction, so a namespace admit or a delete would otherwise split
 * across pooled connections.
 */
/**
 * Serialize create, delete, and budget writes for one id. Hyperdrive rejects
 * `pg_advisory_xact_lock`. The upsert locks this row until the surrounding
 * transaction ends, including when the namespace row is already gone, so a
 * budget write cannot insert a ledger for a namespace a delete removed.
 */
async function lockNamespace(client: SqlExecutor, id: string): Promise<void> {
  await client.query(
    "INSERT INTO public.axond_namespace_lock (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id",
    [id],
  );
}

async function withTransaction<T>(client: SqlExecutor, fn: (client: SqlExecutor) => Promise<T>): Promise<T> {
  await client.query("BEGIN");
  try {
    const value = await fn(client);
    await client.query("COMMIT");
    return value;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  }
}

export async function settlePostgres(client: SqlExecutor, input: SettleInput): Promise<{ charged: boolean }> {
  if (input.cost === null || input.period === null) {
    const inserted = await client.query(
      `INSERT INTO public.axond_store_usage (request_id, namespace, period, model, status, cost_microdollars)
       VALUES ($1, $2, $3, $4, $5, NULL)
       ON CONFLICT (request_id) DO NOTHING`,
      [input.requestId, input.namespace, input.period, input.model, input.status],
    );
    return { charged: false && (inserted.rowCount ?? 0) >= 0 };
  }
  const result = await client.query(
    `WITH ins AS (
       INSERT INTO public.axond_store_usage (request_id, namespace, period, model, status, cost_microdollars, recorded_at)
       VALUES ($1, $2, $3, $4, $5, $6::bigint, now())
       ON CONFLICT (request_id) DO NOTHING
       RETURNING 1
     ),
     upd AS (
       UPDATE public.axond_store_budget
       SET spent_microdollars = CASE
         WHEN spent_microdollars >= ${I64_MAX}::bigint - $6::bigint THEN ${I64_MAX}::bigint
         ELSE spent_microdollars + $6::bigint END
       WHERE namespace = $2 AND period = $3
         AND EXISTS (SELECT 1 FROM ins)
         AND EXISTS (SELECT 1 FROM public.axond_namespace WHERE id = $2)
         AND COALESCE((SELECT n FROM public.axond_namespace_incarnation WHERE id = $2), 1) = $7::bigint
       RETURNING 1
     )
     SELECT (SELECT COUNT(*) FROM ins) AS inserted, (SELECT COUNT(*) FROM upd) AS charged`,
    [input.requestId, input.namespace, input.period, input.model, input.status, saturateMicrodollars(input.cost).toString(), input.incarnation.toString()],
  );
  return { charged: Number(result.rows[0]?.["charged"] ?? 0) === 1 };
}

export const POSTGRES_SCHEMA = `
CREATE TABLE IF NOT EXISTS public.axond_namespace (
    id TEXT PRIMARY KEY NOT NULL,
    attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
    blocklist JSONB,
    allow_platform_fallback boolean NOT NULL DEFAULT false,
    from_config boolean NOT NULL DEFAULT false
);
ALTER TABLE public.axond_namespace ADD COLUMN IF NOT EXISTS allow_platform_fallback boolean NOT NULL DEFAULT false;
ALTER TABLE public.axond_namespace ADD COLUMN IF NOT EXISTS from_config boolean NOT NULL DEFAULT false;
CREATE TABLE IF NOT EXISTS public.axond_namespace_incarnation (
    id text PRIMARY KEY NOT NULL,
    n bigint NOT NULL
);
CREATE TABLE IF NOT EXISTS public.axond_namespace_lock (
    id text PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS public.axond_store_budget (
    namespace text NOT NULL,
    period text NOT NULL,
    limit_microdollars bigint NOT NULL,
    spent_microdollars bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (namespace, period)
);
CREATE TABLE IF NOT EXISTS public.axond_store_budget_active (
    namespace text PRIMARY KEY NOT NULL,
    period text NOT NULL
);
CREATE TABLE IF NOT EXISTS public.axond_store_budget_cadence (
    namespace text PRIMARY KEY NOT NULL,
    cadence text NOT NULL,
    limit_microdollars bigint NOT NULL,
    timezone text NOT NULL DEFAULT 'UTC',
    period text
);
CREATE TABLE IF NOT EXISTS public.axond_store_usage (
    request_id text PRIMARY KEY,
    namespace text NOT NULL,
    period text,
    model text NOT NULL,
    status text NOT NULL,
    cost_microdollars bigint,
    recorded_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS public.axond_store_provider_models (
    provider text PRIMARY KEY NOT NULL,
    fetched_at text,
    stale boolean NOT NULL,
    models jsonb NOT NULL,
    source text
);
CREATE TABLE IF NOT EXISTS public.axond_catalog_streak (
    singleton text PRIMARY KEY NOT NULL,
    consecutive_refusals integer NOT NULL
);
CREATE TABLE IF NOT EXISTS public.axond_schema_migrations (
    id text PRIMARY KEY NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS public.axond_schema_lock (
    id integer PRIMARY KEY CHECK (id = 1)
);
INSERT INTO public.axond_schema_lock (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
`;

async function readPolicy(client: SqlExecutor, namespace: string, nowMs: number) {
  const policy = await client.query(
    "SELECT cadence, limit_microdollars, timezone FROM public.axond_store_budget_cadence WHERE namespace = $1",
    [namespace],
  );
  const row = policy.rows[0];
  if (!row) {
    const active = await client.query(
      `SELECT a.period AS period, b.limit_microdollars AS limit_microdollars, b.spent_microdollars AS spent_microdollars
       FROM public.axond_store_budget_active a
       JOIN public.axond_store_budget b ON b.namespace = a.namespace AND b.period = a.period
       WHERE a.namespace = $1`,
      [namespace],
    );
    const legacy = active.rows[0];
    if (!legacy) {
      return null;
    }
    return budgetPolicyFromLedger({
      namespace,
      cadence: "fixed",
      timezone: "UTC",
      period: String(legacy["period"]),
      limit: BigInt(String(legacy["limit_microdollars"])),
      spent: BigInt(String(legacy["spent_microdollars"])),
    });
  }
  const cadence = row["cadence"] === "monthly" ? "monthly" : "fixed";
  const timezone = String(row["timezone"]);
  let period = "";
  if (cadence === "monthly") {
    period = monthlyPeriod(nowMs, timezone);
  } else {
    const active = await client.query("SELECT period FROM public.axond_store_budget_active WHERE namespace = $1", [namespace]);
    period = active.rows[0] ? String(active.rows[0]["period"]) : "";
  }
  const budget = period ? await readBudget(client, namespace, period, nowMs) : null;
  return budgetPolicyFromLedger({
    namespace,
    cadence,
    timezone,
    period,
    limit: budget?.limit ?? BigInt(String(row["limit_microdollars"])),
    spent: budget?.spent ?? 0n,
  });
}

async function readBudget(client: SqlExecutor, namespace: string, period: string, nowMs = Date.now()): Promise<BudgetLedger | null> {
  const result = await client.query(
    "SELECT limit_microdollars, spent_microdollars FROM public.axond_store_budget WHERE namespace = $1 AND period = $2",
    [namespace, period],
  );
  const row = result.rows[0];
  if (!row) {
    return null;
  }
  const policy = await client.query("SELECT cadence, timezone FROM public.axond_store_budget_cadence WHERE namespace = $1", [namespace]);
  const cadence = policy.rows[0];
  const active =
    cadence?.["cadence"] === "monthly"
      ? monthlyPeriod(nowMs, String(cadence["timezone"])) === period
      : (await client.query("SELECT period FROM public.axond_store_budget_active WHERE namespace = $1", [namespace])).rows[0]?.["period"] ===
        period;
  return {
    namespace,
    period,
    limit: BigInt(String(row["limit_microdollars"])),
    spent: BigInt(String(row["spent_microdollars"])),
    active,
  };
}

function admittedBudget(
  record: NamespaceWrite,
  period: string,
  row: Record<string, unknown>,
  incarnation: bigint,
): { record: NamespaceWrite; period: string; limit: bigint; spent: bigint; incarnation: bigint; admitted: boolean } {
  const limit = BigInt(String(row["limit_microdollars"]));
  const spent = BigInt(String(row["spent_microdollars"]));
  return { record, period, limit, spent, incarnation, admitted: spent < limit };
}

function namespaceFrom(row: Record<string, unknown>): NamespaceWrite {
  return {
    id: String(row["id"]),
    attrs: (row["attrs"] ?? {}) as Record<string, unknown>,
    blocklist: (row["blocklist"] as string[] | null) ?? null,
    allowPlatformFallback: Boolean(row["allow_platform_fallback"]),
    fromConfig: Boolean(row["from_config"]),
  };
}

/**
 * Apply `POSTGRES_SCHEMA`. A role with only `pg_read_all_data` and
 * `pg_write_all_data` cannot `CREATE`, and `CREATE TABLE IF NOT EXISTS`
 * still fails when the tables are already there. Existing tables are enough
 * only when `public.axond_namespace` already has the TypeScript columns. A missing
 * table or column is named, and the driver text is not.
 */
export async function applyPostgresSchema(client: SqlExecutor): Promise<void> {
  try {
    await client.query(POSTGRES_SCHEMA);
  } catch (error) {
    if (!postgresInsufficientPrivilege(error)) {
      throw error;
    }
    const tables = postgresSchemaTables();
    const missingTables = await missingPostgresTables(client, tables);
    const missingColumns = missingTables.includes("axond_namespace")
      ? []
      : await missingPostgresColumns(client, NAMESPACE_COLUMNS);
    const missing = [...missingTables, ...missingColumns];
    if (missing.length > 0) {
      throw new Error(`postgres schema is missing ${missing.join(", ")}`);
    }
  }
}

const NAMESPACE_COLUMNS: readonly (readonly [string, string])[] = [
  ["axond_namespace", "allow_platform_fallback"],
  ["axond_namespace", "from_config"],
];

export function postgresSchemaTables(sql = POSTGRES_SCHEMA): string[] {
  return [...sql.matchAll(/CREATE TABLE IF NOT EXISTS\s+(?:public\.)?([a-zA-Z_][a-zA-Z0-9_]*)/g)].map((match) => match[1]!);
}

function postgresInsufficientPrivilege(error: unknown): boolean {
  return typeof error === "object" && error !== null && "code" in error && (error as { code?: unknown }).code === "42501";
}

async function missingPostgresTables(client: SqlExecutor, tables: readonly string[]): Promise<string[]> {
  const missing: string[] = [];
  for (const table of tables) {
    const found = await client.query("SELECT to_regclass($1) AS name", [`public.${table}`]);
    if (found.rows[0]?.["name"] == null) {
      missing.push(table);
    }
  }
  return missing;
}

async function missingPostgresColumns(
  client: SqlExecutor,
  columns: readonly (readonly [string, string])[],
): Promise<string[]> {
  const missing: string[] = [];
  for (const [table, column] of columns) {
    const found = await client.query(
      `SELECT 1 AS ok FROM pg_attribute
       WHERE attrelid = to_regclass($1) AND attname = $2 AND attnum > 0 AND NOT attisdropped`,
      [`public.${table}`, column],
    );
    if (!found.rows[0]) {
      missing.push(`${table}.${column}`);
    }
  }
  return missing;
}

function existingCreateTables(sql: string): string[] | null {
  const parts = sql.split(";").map((part) => part.trim()).filter((part) => part.length > 0);
  const names: string[] = [];
  for (const part of parts) {
    const match = /^CREATE TABLE IF NOT EXISTS\s+(?:public\.)?([a-zA-Z_][a-zA-Z0-9_]*)\b/i.exec(part);
    if (!match?.[1]) {
      return null;
    }
    names.push(match[1]);
  }
  return names.length > 0 ? names : null;
}

/**
 * Apply one extension migration if its id is not already recorded.
 * A second call leaves the database unchanged. A failed statement is rolled
 * back and is not recorded. The thrown error names the id and omits the
 * driver text. The applier locks `public.axond_schema_lock` with a row update.
 * Hyperdrive does not support advisory locks.
 */
export async function applyPostgresMigration(dsn: string, id: string, sql: string): Promise<void> {
  const client = holdPgClient(new pg.Client({ connectionString: dsn }));
  try {
    await client.connect();
    await applyPostgresMigrationOn(
      {
        query: async (statement, params) => {
          const result = params === undefined ? await client.query(statement) : await client.query(statement, [...params]);
          const row = Array.isArray(result) ? result[result.length - 1] : result;
          return { rows: (row?.rows ?? []) as Record<string, unknown>[], rowCount: row?.rowCount ?? null };
        },
      },
      id,
      sql,
    );
  } catch (error) {
    if (error instanceof Error && error.message === `extension migration ${id} failed`) {
      throw error;
    }
    throw new Error(`extension migration ${id} failed`);
  } finally {
    await client.end().catch(() => undefined);
  }
}

/**
 * Run one migration on an open client. The caller owns connect and close.
 * `INSERT ... ON CONFLICT DO UPDATE` locks the singleton row until commit, so
 * two appliers cannot both run `sql`. A write is the lock: Hyperdrive rejects
 * `pg_advisory_xact_lock` and can cache a bare `SELECT`.
 */
export async function applyPostgresMigrationOn(client: SqlExecutor, id: string, sql: string): Promise<void> {
  try {
    await client.query("BEGIN");
    const lock = await client.query(
      "INSERT INTO public.axond_schema_lock (id) VALUES (1) ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id RETURNING id",
    );
    if (lock.rows.length !== 1) {
      throw new Error("schema lock row is missing");
    }
    const existing = await client.query("SELECT id FROM public.axond_schema_migrations WHERE id = $1", [id]);
    if (existing.rows.length === 0) {
      await client.query(sql);
      await client.query("INSERT INTO public.axond_schema_migrations (id) VALUES ($1)", [id]);
    }
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    const tables = existingCreateTables(sql);
    if (postgresInsufficientPrivilege(error) && tables && (await missingPostgresTables(client, tables)).length === 0) {
      try {
        await recordPostgresMigration(client, id);
        return;
      } catch {
        throw new Error(`extension migration ${id} failed`);
      }
    }
    throw new Error(`extension migration ${id} failed`);
  }
}

async function recordPostgresMigration(client: SqlExecutor, id: string): Promise<void> {
  await client.query("BEGIN");
  try {
    const lock = await client.query(
      "INSERT INTO public.axond_schema_lock (id) VALUES (1) ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id RETURNING id",
    );
    if (lock.rows.length !== 1) {
      throw new Error("schema lock row is missing");
    }
    const existing = await client.query("SELECT id FROM public.axond_schema_migrations WHERE id = $1", [id]);
    if (existing.rows.length === 0) {
      await client.query("INSERT INTO public.axond_schema_migrations (id) VALUES ($1)", [id]);
    }
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  }
}

/**
 * Extension SQL uses `?` binds, which SQLite accepts. Postgres numbers them
 * `$1`, `$2`. A `?` inside a quote, a comment, or a dollar quote stays. `$1`
 * text with no `?` is already Postgres and is left as written. A `?` count
 * that does not match the arguments is a store failure before a connection opens.
 */
export function postgresQueryText(sql: string, paramCount: number): string {
  if (paramCount === 0) {
    return sql;
  }
  const bound = bindPostgresPlaceholders(sql);
  if (bound.count !== 0 && bound.count !== paramCount) {
    throw new StoreFailure();
  }
  return bound.count === paramCount ? bound.sql : sql;
}

function bindPostgresPlaceholders(sql: string): { sql: string; count: number } {
  let count = 0;
  let out = "";
  let i = 0;
  while (i < sql.length) {
    const ch = sql[i]!;
    if (ch === "'" || ch === '"') {
      const end = scanSqlQuote(sql, i, ch);
      out += sql.slice(i, end);
      i = end;
      continue;
    }
    if (ch === "-" && sql[i + 1] === "-") {
      const newline = sql.indexOf("\n", i);
      const end = newline === -1 ? sql.length : newline;
      out += sql.slice(i, end);
      i = end;
      continue;
    }
    if (ch === "/" && sql[i + 1] === "*") {
      const close = sql.indexOf("*/", i + 2);
      const end = close === -1 ? sql.length : close + 2;
      out += sql.slice(i, end);
      i = end;
      continue;
    }
    if (ch === "$") {
      const end = scanDollarQuote(sql, i);
      if (end > i) {
        out += sql.slice(i, end);
        i = end;
        continue;
      }
    }
    if (ch === "?") {
      count += 1;
      out += `$${count}`;
      i += 1;
      continue;
    }
    out += ch;
    i += 1;
  }
  return { sql: out, count };
}

function scanSqlQuote(sql: string, start: number, quote: string): number {
  let i = start + 1;
  while (i < sql.length) {
    if (sql[i] === quote) {
      if (sql[i + 1] === quote) {
        i += 2;
        continue;
      }
      return i + 1;
    }
    i += 1;
  }
  return sql.length;
}

/** `$tag$...$tag$` or `$$...$$`. `$1` is a bind, not a quote. */
function scanDollarQuote(sql: string, start: number): number {
  const next = sql[start + 1];
  if (next === undefined || /\d/.test(next)) {
    return start;
  }
  let i = start + 1;
  if (next !== "$") {
    if (!/[A-Za-z_]/.test(next)) {
      return start;
    }
    i += 1;
    while (i < sql.length && /[A-Za-z0-9_]/.test(sql[i]!)) {
      i += 1;
    }
    if (sql[i] !== "$") {
      return start;
    }
  }
  const tag = sql.slice(start, i + 1);
  const close = sql.indexOf(tag, i + 1);
  return close === -1 ? sql.length : close + tag.length;
}

function modelFrom(row: Record<string, unknown>): ProviderModelCache {
  return {
    provider: String(row["provider"]),
    fetchedAt: row["fetched_at"] === null || row["fetched_at"] === undefined ? null : String(row["fetched_at"]),
    stale: Boolean(row["stale"]),
    data: (row["models"] as unknown[]) ?? [],
    source: row["source"] === null || row["source"] === undefined ? null : String(row["source"]),
  };
}
