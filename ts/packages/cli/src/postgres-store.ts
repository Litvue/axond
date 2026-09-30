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

import { GatewayFailure, StoreFailure } from "../../gateway/src/errors.ts";
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
      return withClient(null, async (client) => {
        const result = await client.query(sql, params);
        return { rows: result.rows };
      });
    },
    async resolveNamespace(id, nowMs) {
      return withClient("namespace_resolve", async (client) => {
        const found = await client.query(
          "SELECT id, attrs, blocklist, allow_platform_fallback, from_config FROM axond_namespace WHERE id = $1",
          [id],
        );
        const row = found.rows[0];
        if (!row) {
          return null;
        }
        const record = namespaceFrom(row);
        const policy = await client.query(
          "SELECT cadence, limit_microdollars, timezone FROM axond_store_budget_cadence WHERE namespace = $1",
          [id],
        );
        let period: string | null = null;
        if (policy.rows[0]?.["cadence"] === "monthly") {
          period = monthlyPeriod(nowMs, String(policy.rows[0]["timezone"]));
          await client.query(
            `INSERT INTO axond_store_budget (namespace, period, limit_microdollars, spent_microdollars)
             VALUES ($1, $2, $3, 0)
             ON CONFLICT (namespace, period) DO NOTHING`,
            [id, period, policy.rows[0]["limit_microdollars"]],
          );
          await client.query(
            `INSERT INTO axond_store_budget_active (namespace, period) VALUES ($1, $2)
             ON CONFLICT (namespace) DO UPDATE SET period = EXCLUDED.period`,
            [id, period],
          );
        } else {
          const active = await client.query("SELECT period FROM axond_store_budget_active WHERE namespace = $1", [id]);
          period = active.rows[0] ? String(active.rows[0]["period"]) : null;
        }
        const incarnation = await client.query("SELECT n FROM axond_namespace_incarnation WHERE id = $1", [id]);
        const n = incarnation.rows[0] ? BigInt(String(incarnation.rows[0]["n"])) : 1n;
        if (!period) {
          return { record, period: null, limit: null, spent: null, incarnation: n, admitted: false };
        }
        const budget = await client.query(
          "SELECT limit_microdollars, spent_microdollars FROM axond_store_budget WHERE namespace = $1 AND period = $2",
          [id, period],
        );
        if (!budget.rows[0]) {
          return { record, period, limit: null, spent: null, incarnation: n, admitted: false };
        }
        const limit = BigInt(String(budget.rows[0]["limit_microdollars"]));
        const spent = BigInt(String(budget.rows[0]["spent_microdollars"]));
        return { record, period, limit, spent, incarnation: n, admitted: spent < limit };
      });
    },
    async putNamespace(record) {
      return withClient("namespace_write", async (client) => {
        const result = await client.query(
          `INSERT INTO axond_namespace (id, attrs, blocklist, allow_platform_fallback, from_config)
           VALUES ($1, $2::jsonb, $3::jsonb, $4, $5)
           ON CONFLICT (id) DO NOTHING`,
          [
            record.id,
            JSON.stringify(record.attrs),
            record.blocklist === null ? null : JSON.stringify(record.blocklist),
            record.allowPlatformFallback,
            record.fromConfig,
          ],
        );
        return (result.rowCount ?? 0) > 0 ? "created" : "exists";
      });
    },
    async getNamespace(id) {
      return withClient("namespace_read", async (client) => {
        const result = await client.query("SELECT * FROM axond_namespace WHERE id = $1", [id]);
        return result.rows[0] ? namespaceFrom(result.rows[0]) : null;
      });
    },
    async updateNamespace(id, attrs, blocklist) {
      return withClient("namespace_write", async (client) => {
        const result = await client.query(
          `UPDATE axond_namespace SET attrs = $2::jsonb, blocklist = $3::jsonb WHERE id = $1 RETURNING *`,
          [id, JSON.stringify(attrs), blocklist === null ? null : JSON.stringify(blocklist)],
        );
        return result.rows[0] ? namespaceFrom(result.rows[0]) : null;
      });
    },
    async deleteNamespace(id) {
      return withClient("namespace_write", async (client) => {
        const deleted = await client.query("DELETE FROM axond_namespace WHERE id = $1", [id]);
        if ((deleted.rowCount ?? 0) === 0) {
          return false;
        }
        await client.query("DELETE FROM axond_store_budget WHERE namespace = $1", [id]);
        await client.query("DELETE FROM axond_store_budget_active WHERE namespace = $1", [id]);
        await client.query("DELETE FROM axond_store_budget_cadence WHERE namespace = $1", [id]);
        await client.query(
          `INSERT INTO axond_namespace_incarnation (id, n) VALUES ($1, 2)
           ON CONFLICT (id) DO UPDATE SET n = axond_namespace_incarnation.n + 1`,
          [id],
        );
        return true;
      });
    },
    async listNamespaces(cursor, limit) {
      return withClient("namespace_read", async (client) => {
        const result = await client.query(
          "SELECT * FROM axond_namespace WHERE ($1::text IS NULL OR id > $1) ORDER BY id LIMIT $2",
          [cursor, limit + 1],
        );
        const page = result.rows.slice(0, limit).map(namespaceFrom);
        const nextCursor = result.rows.length > limit ? page[page.length - 1]!.id : null;
        return { data: page, nextCursor };
      });
    },
    async putBudget(namespace, period, limit) {
      return withClient("budget_write", async (client) => {
        const known = await client.query("SELECT id FROM axond_namespace WHERE id = $1", [namespace]);
        if (!known.rows[0]) {
          throw new GatewayFailure("unknown_namespace", 404, "unknown namespace");
        }
        await client.query(
          `INSERT INTO axond_store_budget (namespace, period, limit_microdollars, spent_microdollars)
           VALUES ($1, $2, $3::bigint, 0)
           ON CONFLICT (namespace, period) DO UPDATE SET limit_microdollars = EXCLUDED.limit_microdollars`,
          [namespace, period, limit.toString()],
        );
        await client.query(
          `INSERT INTO axond_store_budget_active (namespace, period) VALUES ($1, $2)
           ON CONFLICT (namespace) DO UPDATE SET period = EXCLUDED.period`,
          [namespace, period],
        );
        return (await readBudget(client, namespace, period))!;
      });
    },
    async getBudget(namespace, period) {
      return withClient("budget_read", async (client) => {
        const known = await client.query("SELECT id FROM axond_namespace WHERE id = $1", [namespace]);
        if (!known.rows[0]) {
          throw new GatewayFailure("unknown_namespace", 404, "unknown namespace");
        }
        return readBudget(client, namespace, period);
      });
    },
    async putBudgetPolicy(input: BudgetPolicyWrite) {
      return withClient("budget_write", async (client) => {
        const known = await client.query("SELECT id FROM axond_namespace WHERE id = $1", [input.namespace]);
        if (!known.rows[0]) {
          throw new GatewayFailure("unknown_namespace", 404, "unknown namespace");
        }
        const period = input.cadence === "monthly" ? monthlyPeriod(input.nowMs, input.timezone) : input.period;
        if (!period) {
          throw new GatewayFailure("bad_request", 400, 'period is required for cadence "fixed"');
        }
        await client.query(
          `INSERT INTO axond_store_budget_cadence (namespace, cadence, limit_microdollars, timezone)
           VALUES ($1, $2, $3::bigint, $4)
           ON CONFLICT (namespace) DO UPDATE SET cadence = EXCLUDED.cadence, limit_microdollars = EXCLUDED.limit_microdollars, timezone = EXCLUDED.timezone`,
          [input.namespace, input.cadence, input.limit.toString(), input.timezone],
        );
        await client.query(
          `INSERT INTO axond_store_budget (namespace, period, limit_microdollars) VALUES ($1, $2, $3::bigint)
           ON CONFLICT (namespace, period) DO UPDATE SET limit_microdollars = EXCLUDED.limit_microdollars`,
          [input.namespace, period, input.limit.toString()],
        );
        await client.query(
          `INSERT INTO axond_store_budget_active (namespace, period) VALUES ($1, $2)
           ON CONFLICT (namespace) DO UPDATE SET period = EXCLUDED.period`,
          [input.namespace, period],
        );
        const budget = (await readBudget(client, input.namespace, period))!;
        return {
          namespace: input.namespace,
          cadence: input.cadence,
          limit_microdollars: Number(input.limit),
          timezone: input.timezone,
          period,
          spent_microdollars: Number(budget.spent),
          reserved_microdollars: 0,
          remaining_microdollars: Number(budget.limit - budget.spent),
          active: true,
        };
      });
    },
    async getBudgetPolicy(namespace) {
      return withClient("budget_read", async (client) => {
        const policy = await client.query("SELECT * FROM axond_store_budget_cadence WHERE namespace = $1", [namespace]);
        if (!policy.rows[0]) {
          const known = await client.query("SELECT id FROM axond_namespace WHERE id = $1", [namespace]);
          if (!known.rows[0]) {
            throw new GatewayFailure("unknown_namespace", 404, "unknown namespace");
          }
          return null;
        }
        const active = await client.query("SELECT period FROM axond_store_budget_active WHERE namespace = $1", [namespace]);
        const period = String(active.rows[0]?.["period"] ?? "");
        const budget = await readBudget(client, namespace, period);
        return {
          namespace,
          cadence: policy.rows[0]["cadence"] === "monthly" ? "monthly" : "fixed",
          limit_microdollars: Number(policy.rows[0]["limit_microdollars"]),
          timezone: String(policy.rows[0]["timezone"]),
          period,
          spent_microdollars: Number(budget?.spent ?? 0n),
          reserved_microdollars: 0,
          remaining_microdollars: Number((budget?.limit ?? 0n) - (budget?.spent ?? 0n)),
          active: true,
        };
      });
    },
    settle(input) {
      return withClient("budget_charge", (client) => settlePostgres(client, input));
    },
    async summarizeUsage(namespace, period) {
      return withClient("usage_summary", async (client) => {
        const result = await client.query(
          `SELECT model, status, COUNT(*)::int AS count, COALESCE(SUM(cost_microdollars), 0) AS cost
           FROM axond_store_usage WHERE namespace = $1 AND period = $2 GROUP BY model, status`,
          [namespace, period],
        );
        return result.rows.map((row) => ({
          model: String(row["model"]),
          status: String(row["status"]),
          count: Number(row["count"]),
          cost_microdollars: Number(row["cost"]),
        }));
      });
    },
    async listProviderModels() {
      return withClient("provider_models", async (client) => {
        const result = await client.query("SELECT * FROM axond_store_provider_models", []);
        return result.rows.map(modelFrom);
      });
    },
    async getProviderModels(provider) {
      return withClient("provider_models", async (client) => {
        const result = await client.query("SELECT * FROM axond_store_provider_models WHERE provider = $1", [provider]);
        return result.rows[0] ? modelFrom(result.rows[0]) : null;
      });
    },
    async upsertProviderModels(row) {
      return withClient("provider_models", async (client) => {
        await client.query(
          `INSERT INTO axond_store_provider_models (provider, fetched_at, stale, models, source)
           VALUES ($1, $2, $3, $4::jsonb, $5)
           ON CONFLICT (provider) DO UPDATE SET fetched_at = EXCLUDED.fetched_at, stale = EXCLUDED.stale, models = EXCLUDED.models, source = EXCLUDED.source
           WHERE axond_store_provider_models.source IS NOT DISTINCT FROM EXCLUDED.source
              OR axond_store_provider_models.stale = true`,
          [row.provider, row.fetchedAt, row.stale, JSON.stringify(row.data), row.source],
        );
      });
    },
    async markProviderModelsStale(provider) {
      return withClient("provider_models", async (client) => {
        await client.query(
          `INSERT INTO axond_store_provider_models (provider, fetched_at, stale, models, source)
           VALUES ($1, NULL, true, '[]'::jsonb, NULL)
           ON CONFLICT (provider) DO UPDATE SET stale = true`,
          [provider],
        );
      });
    },
    async noteCatalogRefusal() {
      return withClient(null, async (client) => {
        const result = await client.query(
          `INSERT INTO axond_catalog_streak (singleton, consecutive_refusals) VALUES ('catalog', 1)
           ON CONFLICT (singleton) DO UPDATE SET consecutive_refusals = axond_catalog_streak.consecutive_refusals + 1
           RETURNING consecutive_refusals`,
          [],
        );
        return Number(result.rows[0]?.["consecutive_refusals"] ?? 1);
      });
    },
    async resetCatalogStreak() {
      return withClient(null, async (client) => {
        await client.query(
          `INSERT INTO axond_catalog_streak (singleton, consecutive_refusals) VALUES ('catalog', 0)
           ON CONFLICT (singleton) DO UPDATE SET consecutive_refusals = 0`,
          [],
        );
      });
    },
  };
}

export async function settlePostgres(client: SqlExecutor, input: SettleInput): Promise<{ charged: boolean }> {
  if (input.cost === null || input.period === null) {
    const inserted = await client.query(
      `INSERT INTO axond_store_usage (request_id, namespace, period, model, status, cost_microdollars)
       VALUES ($1, $2, $3, $4, $5, NULL)
       ON CONFLICT (request_id) DO NOTHING`,
      [input.requestId, input.namespace, input.period, input.model, input.status],
    );
    return { charged: false && (inserted.rowCount ?? 0) >= 0 };
  }
  const result = await client.query(
    `WITH ins AS (
       INSERT INTO axond_store_usage (request_id, namespace, period, model, status, cost_microdollars, recorded_at)
       VALUES ($1, $2, $3, $4, $5, $6::bigint, now())
       ON CONFLICT (request_id) DO NOTHING
       RETURNING 1
     ),
     upd AS (
       UPDATE axond_store_budget
       SET spent_microdollars = CASE
         WHEN spent_microdollars >= ${I64_MAX}::bigint - $6::bigint THEN ${I64_MAX}::bigint
         ELSE spent_microdollars + $6::bigint END
       WHERE namespace = $2 AND period = $3
         AND EXISTS (SELECT 1 FROM ins)
         AND EXISTS (SELECT 1 FROM axond_namespace WHERE id = $2)
         AND COALESCE((SELECT n FROM axond_namespace_incarnation WHERE id = $2), 1) = $7::bigint
       RETURNING 1
     )
     SELECT (SELECT COUNT(*) FROM ins) AS inserted, (SELECT COUNT(*) FROM upd) AS charged`,
    [input.requestId, input.namespace, input.period, input.model, input.status, input.cost.toString(), input.incarnation.toString()],
  );
  return { charged: Number(result.rows[0]?.["charged"] ?? 0) === 1 };
}

export const POSTGRES_SCHEMA = `
CREATE TABLE IF NOT EXISTS axond_namespace (
    id TEXT PRIMARY KEY NOT NULL,
    attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
    blocklist JSONB,
    allow_platform_fallback boolean NOT NULL DEFAULT false,
    from_config boolean NOT NULL DEFAULT false
);
CREATE TABLE IF NOT EXISTS axond_namespace_incarnation (
    id text PRIMARY KEY NOT NULL,
    n bigint NOT NULL
);
CREATE TABLE IF NOT EXISTS axond_store_budget (
    namespace text NOT NULL,
    period text NOT NULL,
    limit_microdollars bigint NOT NULL,
    spent_microdollars bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (namespace, period)
);
CREATE TABLE IF NOT EXISTS axond_store_budget_active (
    namespace text PRIMARY KEY NOT NULL,
    period text NOT NULL
);
CREATE TABLE IF NOT EXISTS axond_store_budget_cadence (
    namespace text PRIMARY KEY NOT NULL,
    cadence text NOT NULL,
    limit_microdollars bigint NOT NULL,
    timezone text NOT NULL DEFAULT 'UTC',
    period text
);
CREATE TABLE IF NOT EXISTS axond_store_usage (
    request_id text PRIMARY KEY,
    namespace text NOT NULL,
    period text,
    model text NOT NULL,
    status text NOT NULL,
    cost_microdollars bigint,
    recorded_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS axond_store_provider_models (
    provider text PRIMARY KEY NOT NULL,
    fetched_at text,
    stale boolean NOT NULL,
    models jsonb NOT NULL,
    source text
);
CREATE TABLE IF NOT EXISTS axond_catalog_streak (
    singleton text PRIMARY KEY NOT NULL,
    consecutive_refusals integer NOT NULL
);
CREATE TABLE IF NOT EXISTS axond_schema_migrations (
    id text PRIMARY KEY NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
);
`;

async function readBudget(client: SqlExecutor, namespace: string, period: string): Promise<BudgetLedger | null> {
  const result = await client.query(
    "SELECT limit_microdollars, spent_microdollars FROM axond_store_budget WHERE namespace = $1 AND period = $2",
    [namespace, period],
  );
  const row = result.rows[0];
  if (!row) {
    return null;
  }
  const active = await client.query("SELECT period FROM axond_store_budget_active WHERE namespace = $1", [namespace]);
  return {
    namespace,
    period,
    limit: BigInt(String(row["limit_microdollars"])),
    spent: BigInt(String(row["spent_microdollars"])),
    active: active.rows[0]?.["period"] === period,
  };
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
 * Apply one extension migration if its id is not already recorded.
 * A second call leaves the database unchanged. A failed statement is rolled
 * back and is not recorded. The thrown error names the id and omits the
 * driver text.
 */
export async function applyPostgresMigration(dsn: string, id: string, sql: string): Promise<void> {
  const client = new pg.Client({ connectionString: dsn });
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

/** Run one migration on an open client. The caller owns connect and close. */
export async function applyPostgresMigrationOn(client: SqlExecutor, id: string, sql: string): Promise<void> {
  try {
    await client.query("BEGIN");
    await client.query("SELECT pg_advisory_xact_lock(hashtext($1::text)::bigint)", [id]);
    const existing = await client.query("SELECT id FROM axond_schema_migrations WHERE id = $1", [id]);
    if (existing.rows.length === 0) {
      await client.query(sql);
      await client.query("INSERT INTO axond_schema_migrations (id) VALUES ($1)", [id]);
    }
    await client.query("COMMIT");
  } catch {
    await client.query("ROLLBACK").catch(() => undefined);
    throw new Error(`extension migration ${id} failed`);
  }
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
