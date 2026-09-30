import { DatabaseSync } from "node:sqlite";

import type {
  BudgetLedger,
  BudgetPolicy,
  BudgetPolicyWrite,
  NamespaceWrite,
  ProviderModelCache,
  QueryResult,
  ResolvedNamespace,
  SettleInput,
  SqlValue,
  Store,
} from "@axond/sdk";

import { FIXED_CADENCE_NEEDS_PERIOD, GatewayFailure, StoreFailure } from "../../gateway/src/errors.ts";
import { encodeAttrs, serdeValue } from "../../gateway/src/strict-json.ts";
import { budgetPolicyFromLedger, foldUsageSummary, saturateMicrodollars } from "../../gateway/src/memory-store.ts";
import { monthlyPeriod } from "../../gateway/src/namespace.ts";
import { recordStoreCall, type StoreMetrics, type StoreOperation } from "../../gateway/src/store-metrics.ts";

const SCHEMA = `
CREATE TABLE IF NOT EXISTS axond_namespace (
    id TEXT PRIMARY KEY NOT NULL,
    attrs TEXT NOT NULL DEFAULT '{}',
    blocklist TEXT,
    allow_platform_fallback INTEGER NOT NULL DEFAULT 0,
    from_config INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS axond_namespace_incarnation (
    id TEXT PRIMARY KEY NOT NULL,
    n INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS axond_store_budget (
    namespace TEXT NOT NULL,
    period TEXT NOT NULL,
    limit_microdollars INTEGER NOT NULL,
    spent_microdollars INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (namespace, period)
);
CREATE TABLE IF NOT EXISTS axond_store_budget_active (
    namespace TEXT PRIMARY KEY NOT NULL,
    period TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS axond_store_budget_cadence (
    namespace TEXT PRIMARY KEY NOT NULL,
    cadence TEXT NOT NULL,
    limit_microdollars INTEGER NOT NULL,
    timezone TEXT NOT NULL DEFAULT 'UTC',
    period TEXT
);
CREATE TABLE IF NOT EXISTS axond_store_usage (
    request_id TEXT PRIMARY KEY NOT NULL,
    namespace TEXT NOT NULL,
    period TEXT,
    model TEXT NOT NULL,
    status TEXT NOT NULL,
    cost_microdollars INTEGER,
    recorded_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER))
);
CREATE INDEX IF NOT EXISTS axond_store_usage_ns_period
    ON axond_store_usage (namespace, period);
CREATE TABLE IF NOT EXISTS axond_store_provider_models (
    provider TEXT PRIMARY KEY NOT NULL,
    fetched_at TEXT,
    stale INTEGER NOT NULL,
    models TEXT NOT NULL,
    source TEXT
);
CREATE TABLE IF NOT EXISTS axond_catalog_streak (
    singleton TEXT PRIMARY KEY NOT NULL,
    consecutive_refusals INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS axond_schema_migrations (
    id TEXT PRIMARY KEY NOT NULL,
    applied_at INTEGER NOT NULL
);
`;

const I64_MAX = 9223372036854775807n;

export function openSqliteStore(path: string, metrics?: StoreMetrics): Store {
  const db = new DatabaseSync(path);
  db.exec("PRAGMA journal_mode=WAL");
  db.exec("PRAGMA busy_timeout=5000");
  db.exec(SCHEMA);
  let chain: Promise<unknown> = Promise.resolve();
  const lock = <T>(operation: StoreOperation | null, fn: () => T): Promise<T> => {
    const called = Date.now();
    const run = chain.then(() => {
      const acquired = Date.now();
      try {
        const value = fn();
        recordStoreCall(metrics, "sqlite", operation, acquired - called, Date.now() - acquired, "ok");
        return value;
      } catch (error) {
        recordStoreCall(metrics, "sqlite", operation, acquired - called, Date.now() - acquired, "error");
        if (error instanceof GatewayFailure || error instanceof StoreFailure) {
          throw error;
        }
        throw new StoreFailure();
      }
    });
    chain = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  };

  const store: Store = {
    query(sql, params = []) {
      return lock(null, () => ({ rows: all(db, sql, params) }));
    },
    resolveNamespace(id, nowMs) {
      return lock("namespace_resolve", () => {
        const record = readNamespace(db, id);
        if (!record) {
          return null;
        }
        const policy = one(db, "SELECT cadence, limit_microdollars, timezone, period FROM axond_store_budget_cadence WHERE namespace = ?", [id]);
        let period: string | null = null;
        if (policy && policy["cadence"] === "monthly") {
          period = monthlyPeriod(nowMs, String(policy["timezone"]));
          db.prepare(
            `INSERT INTO axond_store_budget (namespace, period, limit_microdollars, spent_microdollars)
             VALUES (?, ?, ?, 0)
             ON CONFLICT(namespace, period) DO NOTHING`,
          ).run(id, period, policy["limit_microdollars"]);
        } else {
          const active = one(db, "SELECT period FROM axond_store_budget_active WHERE namespace = ?", [id]);
          period = active ? String(active["period"]) : null;
        }
        const incarnationRow = one(db, "SELECT n FROM axond_namespace_incarnation WHERE id = ?", [id]);
        const incarnation = incarnationRow ? BigInt(String(incarnationRow["n"])) : 1n;
        if (!period) {
          return { record, period: null, limit: null, spent: null, incarnation, admitted: false } satisfies ResolvedNamespace;
        }
        const budget = one(
          db,
          "SELECT limit_microdollars, spent_microdollars FROM axond_store_budget WHERE namespace = ? AND period = ?",
          [id, period],
        );
        if (!budget) {
          return { record, period, limit: null, spent: null, incarnation, admitted: false };
        }
        const limit = BigInt(String(budget["limit_microdollars"]));
        const spent = BigInt(String(budget["spent_microdollars"]));
        return { record, period, limit, spent, incarnation, admitted: spent < limit };
      });
    },
    putNamespace(record) {
      return lock("namespace_write", () => {
        const exists = one(db, "SELECT id FROM axond_namespace WHERE id = ?", [record.id]);
        if (exists) {
          return "exists" as const;
        }
        db.prepare(
          "INSERT INTO axond_namespace (id, attrs, blocklist, allow_platform_fallback, from_config) VALUES (?, ?, ?, ?, ?)",
        ).run(
          record.id,
          encodeAttrs(record.attrs),
          record.blocklist === null ? null : JSON.stringify(record.blocklist),
          record.allowPlatformFallback ? 1 : 0,
          record.fromConfig ? 1 : 0,
        );
        return "created" as const;
      });
    },
    getNamespace(id) {
      return lock("namespace_read", () => readNamespace(db, id));
    },
    updateNamespace(id, attrs, blocklist) {
      return lock("namespace_write", () => {
        const current = readNamespace(db, id);
        if (!current) {
          return null;
        }
        db.prepare("UPDATE axond_namespace SET attrs = ?, blocklist = ? WHERE id = ?").run(
          encodeAttrs(attrs),
          blocklist === null ? null : JSON.stringify(blocklist),
          id,
        );
        return readNamespace(db, id);
      });
    },
    deleteNamespace(id) {
      return lock("namespace_write", () => {
        const result = db.prepare("DELETE FROM axond_namespace WHERE id = ?").run(id);
        if (Number(result.changes) === 0) {
          return false;
        }
        db.prepare("DELETE FROM axond_store_budget WHERE namespace = ?").run(id);
        db.prepare("DELETE FROM axond_store_budget_active WHERE namespace = ?").run(id);
        db.prepare("DELETE FROM axond_store_budget_cadence WHERE namespace = ?").run(id);
        db.prepare(
          `INSERT INTO axond_namespace_incarnation (id, n) VALUES (?, 2)
           ON CONFLICT(id) DO UPDATE SET n = n + 1`,
        ).run(id);
        return true;
      });
    },
    listNamespaces(cursor, limit) {
      return lock("namespace_read", () => {
        const rows = all(
          db,
          "SELECT id FROM axond_namespace WHERE (? IS NULL OR id > ?) ORDER BY id LIMIT ?",
          [cursor, cursor, limit + 1],
        );
        const page = rows.slice(0, limit);
        const nextCursor = rows.length > limit ? String(page[page.length - 1]!["id"]) : null;
        return {
          data: page.map((row) => readNamespace(db, String(row["id"]))!),
          nextCursor,
        };
      });
    },
    putBudget(namespace, period, limit, nowMs = Date.now()) {
      return lock("budget_write", () => {
        if (!readNamespace(db, namespace)) {
          throw new GatewayFailure("unknown_namespace", 404, "unknown namespace");
        }
        db.prepare(
          `INSERT INTO axond_store_budget (namespace, period, limit_microdollars, spent_microdollars)
           VALUES (?, ?, ?, 0)
           ON CONFLICT(namespace, period) DO UPDATE SET limit_microdollars = excluded.limit_microdollars`,
        ).run(namespace, period, limit.toString());
        db.prepare(
          `INSERT INTO axond_store_budget_active (namespace, period) VALUES (?, ?)
           ON CONFLICT(namespace) DO UPDATE SET period = excluded.period`,
        ).run(namespace, period);
        return readBudget(db, namespace, period, nowMs)!;
      });
    },
    getBudget(namespace, period, nowMs = Date.now()) {
      return lock("budget_read", () => {
        if (!readNamespace(db, namespace)) {
          throw new GatewayFailure("unknown_namespace", 404, "unknown namespace");
        }
        return readBudget(db, namespace, period, nowMs);
      });
    },
    putBudgetPolicy(input) {
      return lock("budget_write", () => writePolicy(db, input));
    },
    getBudgetPolicy(namespace, nowMs = Date.now()) {
      return lock("budget_read", () => {
        if (!readNamespace(db, namespace)) {
          throw new GatewayFailure("unknown_namespace", 404, "unknown namespace");
        }
        return readPolicy(db, namespace, nowMs);
      });
    },
    settle(input) {
      return lock("budget_charge", () => settleSqlite(db, input));
    },
    summarizeUsage(namespace, period) {
      return lock("usage_summary", () => {
        if (!readNamespace(db, namespace)) {
          throw new GatewayFailure("unknown_namespace", 404, "unknown namespace");
        }
        return foldUsageSummary(
          all(
            db,
            `SELECT model, status, CAST(cost_microdollars AS TEXT) AS cost
             FROM axond_store_usage WHERE namespace = ? AND period = ?`,
            [namespace, period],
          ).map((row) => ({
            model: String(row["model"]),
            status: String(row["status"]),
            cost: row["cost"] === null ? null : BigInt(String(row["cost"])),
          })),
        );
      });
    },
    listProviderModels() {
      return lock("provider_models", () => all(db, "SELECT * FROM axond_store_provider_models", []).map(modelRow));
    },
    getProviderModels(provider) {
      return lock("provider_models", () => {
        const row = one(db, "SELECT * FROM axond_store_provider_models WHERE provider = ?", [provider]);
        return row ? modelRow(row) : null;
      });
    },
    upsertProviderModels(row) {
      return lock("provider_models", () => {
        db.prepare(
          `INSERT INTO axond_store_provider_models (provider, fetched_at, stale, models, source)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(provider) DO UPDATE SET fetched_at = excluded.fetched_at, stale = excluded.stale, models = excluded.models, source = excluded.source
           WHERE axond_store_provider_models.source IS NOT DISTINCT FROM excluded.source
              OR axond_store_provider_models.stale = 1`,
        ).run(row.provider, row.fetchedAt, row.stale ? 1 : 0, JSON.stringify(row.data), row.source);
      });
    },
    markProviderModelsStale(provider) {
      return lock("provider_models", () => {
        const current = one(db, "SELECT provider FROM axond_store_provider_models WHERE provider = ?", [provider]);
        if (!current) {
          db.prepare(
            "INSERT INTO axond_store_provider_models (provider, fetched_at, stale, models, source) VALUES (?, NULL, 1, '[]', NULL)",
          ).run(provider);
          return;
        }
        db.prepare("UPDATE axond_store_provider_models SET stale = 1 WHERE provider = ?").run(provider);
      });
    },
    noteCatalogRefusal() {
      return lock(null, () => {
        const row = one(
          db,
          `INSERT INTO axond_catalog_streak (singleton, consecutive_refusals) VALUES ('catalog', 1)
           ON CONFLICT(singleton) DO UPDATE SET consecutive_refusals = consecutive_refusals + 1
           RETURNING consecutive_refusals`,
          [],
        );
        return Number(row?.["consecutive_refusals"] ?? 1);
      });
    },
    resetCatalogStreak() {
      return lock(null, () => {
        db.prepare(
          `INSERT INTO axond_catalog_streak (singleton, consecutive_refusals) VALUES ('catalog', 0)
           ON CONFLICT(singleton) DO UPDATE SET consecutive_refusals = 0`,
        ).run();
      });
    },
  };
  return store;
}

export function applyMigration(dbPath: string, id: string, sql: string): void {
  const db = new DatabaseSync(dbPath);
  db.exec(SCHEMA);
  const existing = db.prepare("SELECT id FROM axond_schema_migrations WHERE id = ?").get(id);
  if (existing) {
    db.close();
    return;
  }
  db.exec("BEGIN");
  try {
    db.exec(sql);
    db.prepare("INSERT INTO axond_schema_migrations (id, applied_at) VALUES (?, ?)").run(id, Date.now());
    db.exec("COMMIT");
  } catch (error) {
    db.exec("ROLLBACK");
    throw error;
  } finally {
    db.close();
  }
}

function settleSqlite(db: DatabaseSync, input: SettleInput): { charged: boolean } {
  const cost = input.cost === null ? null : saturateMicrodollars(input.cost);
  db.exec("BEGIN IMMEDIATE");
  try {
    const inserted = db
      .prepare(
        `INSERT INTO axond_store_usage (request_id, namespace, period, model, status, cost_microdollars)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(request_id) DO NOTHING`,
      )
      .run(
        input.requestId,
        input.namespace,
        input.period,
        input.model,
        input.status,
        cost === null ? null : cost.toString(),
      );
    if (Number(inserted.changes) !== 1 || cost === null || input.period === null) {
      db.exec("COMMIT");
      return { charged: false };
    }
    const result = db
      .prepare(
        `UPDATE axond_store_budget
         SET spent_microdollars = CASE
           WHEN spent_microdollars >= ? - ? THEN ?
           ELSE spent_microdollars + ?
         END
         WHERE namespace = ? AND period = ?
           AND EXISTS (SELECT 1 FROM axond_namespace WHERE id = ?)
           AND COALESCE((SELECT n FROM axond_namespace_incarnation WHERE id = ?), 1) = ?`,
      )
      .run(
        I64_MAX,
        cost,
        I64_MAX,
        cost,
        input.namespace,
        input.period,
        input.namespace,
        input.namespace,
        input.incarnation,
      );
    db.exec("COMMIT");
    return { charged: Number(result.changes) === 1 };
  } catch (error) {
    db.exec("ROLLBACK");
    throw error;
  }
}

function writePolicy(db: DatabaseSync, input: BudgetPolicyWrite): BudgetPolicy {
  if (!readNamespace(db, input.namespace)) {
    throw new GatewayFailure("unknown_namespace", 404, "unknown namespace");
  }
  let period = input.period;
  if (input.cadence === "monthly") {
    period = monthlyPeriod(input.nowMs, input.timezone);
  } else if (!period) {
    const active = one(db, "SELECT period FROM axond_store_budget_active WHERE namespace = ?", [input.namespace]);
    period = active ? String(active["period"]) : null;
    if (!period) {
      throw new GatewayFailure("bad_request", 400, FIXED_CADENCE_NEEDS_PERIOD);
    }
  }
  db.prepare(
    `INSERT INTO axond_store_budget_cadence (namespace, cadence, limit_microdollars, timezone, period)
     VALUES (?, ?, ?, ?, ?)
     ON CONFLICT(namespace) DO UPDATE SET cadence = excluded.cadence, limit_microdollars = excluded.limit_microdollars, timezone = excluded.timezone, period = excluded.period`,
  ).run(input.namespace, input.cadence, input.limit.toString(), input.timezone, input.cadence === "fixed" ? period : null);
  db.prepare(
    `INSERT INTO axond_store_budget (namespace, period, limit_microdollars, spent_microdollars)
     VALUES (?, ?, ?, 0)
     ON CONFLICT(namespace, period) DO UPDATE SET limit_microdollars = excluded.limit_microdollars`,
  ).run(input.namespace, period, input.limit.toString());
  if (input.cadence === "fixed") {
    db.prepare(
      `INSERT INTO axond_store_budget_active (namespace, period) VALUES (?, ?)
       ON CONFLICT(namespace) DO UPDATE SET period = excluded.period`,
    ).run(input.namespace, period);
  }
  return readPolicy(db, input.namespace, input.nowMs)!;
}

function readPolicy(db: DatabaseSync, namespace: string, nowMs: number): BudgetPolicy | null {
  const policy = one(db, "SELECT cadence, limit_microdollars, timezone FROM axond_store_budget_cadence WHERE namespace = ?", [
    namespace,
  ]);
  if (!policy) {
    const active = one(
      db,
      `SELECT a.period AS period, b.limit_microdollars AS limit_microdollars, b.spent_microdollars AS spent_microdollars
       FROM axond_store_budget_active a
       JOIN axond_store_budget b ON b.namespace = a.namespace AND b.period = a.period
       WHERE a.namespace = ?`,
      [namespace],
    );
    if (!active) {
      return null;
    }
    return budgetPolicyFromLedger({
      namespace,
      cadence: "fixed",
      timezone: "UTC",
      period: String(active["period"]),
      limit: BigInt(String(active["limit_microdollars"])),
      spent: BigInt(String(active["spent_microdollars"])),
    });
  }
  const cadence = policy["cadence"] === "monthly" ? "monthly" : "fixed";
  const timezone = String(policy["timezone"]);
  const period =
    cadence === "monthly"
      ? monthlyPeriod(nowMs, timezone)
      : String(one(db, "SELECT period FROM axond_store_budget_active WHERE namespace = ?", [namespace])?.["period"] ?? "");
  const budget = period ? readBudget(db, namespace, period, nowMs) : null;
  return budgetPolicyFromLedger({
    namespace,
    cadence,
    timezone,
    period,
    limit: budget?.limit ?? BigInt(String(policy["limit_microdollars"])),
    spent: budget?.spent ?? 0n,
  });
}

function readBudget(db: DatabaseSync, namespace: string, period: string, nowMs = Date.now()): BudgetLedger | null {
  const row = one(
    db,
    "SELECT limit_microdollars, spent_microdollars FROM axond_store_budget WHERE namespace = ? AND period = ?",
    [namespace, period],
  );
  if (!row) {
    return null;
  }
  const policy = one(db, "SELECT cadence, timezone FROM axond_store_budget_cadence WHERE namespace = ?", [namespace]);
  const active =
    policy?.["cadence"] === "monthly"
      ? monthlyPeriod(nowMs, String(policy["timezone"])) === period
      : one(db, "SELECT period FROM axond_store_budget_active WHERE namespace = ?", [namespace])?.["period"] === period;
  return {
    namespace,
    period,
    limit: BigInt(String(row["limit_microdollars"])),
    spent: BigInt(String(row["spent_microdollars"])),
    active,
  };
}

function storedAttrs(text: string): Record<string, unknown> {
  JSON.parse(text);
  return serdeValue(text) as Record<string, unknown>;
}

function readNamespace(db: DatabaseSync, id: string): NamespaceWrite | null {
  const row = one(db, "SELECT * FROM axond_namespace WHERE id = ?", [id]);
  if (!row) {
    return null;
  }
  return {
    id: String(row["id"]),
    attrs: storedAttrs(String(row["attrs"])),
    blocklist: row["blocklist"] === null ? null : (JSON.parse(String(row["blocklist"])) as string[]),
    allowPlatformFallback: Number(row["allow_platform_fallback"]) === 1,
    fromConfig: Number(row["from_config"]) === 1,
  };
}

function modelRow(row: Record<string, unknown>): ProviderModelCache {
  return {
    provider: String(row["provider"]),
    fetchedAt: row["fetched_at"] === null ? null : String(row["fetched_at"]),
    stale: Number(row["stale"]) === 1,
    data: JSON.parse(String(row["models"])) as unknown[],
    source: row["source"] === null ? null : String(row["source"]),
  };
}

function one(db: DatabaseSync, sql: string, params: readonly SqlValue[]): Record<string, unknown> | null {
  const row = db.prepare(sql).get(...params.map(bind));
  return row ? (row as Record<string, unknown>) : null;
}

function all(db: DatabaseSync, sql: string, params: readonly SqlValue[]): Record<string, unknown>[] {
  return db.prepare(sql).all(...params.map(bind)) as Record<string, unknown>[];
}

function bind(value: SqlValue): string | number | bigint | null {
  return typeof value === "bigint" ? value.toString() : value;
}

export type { QueryResult };
