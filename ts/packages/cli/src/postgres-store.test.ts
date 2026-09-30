import assert from "node:assert/strict";
import test from "node:test";

import pg from "pg";

import { StoreFailure } from "../../gateway/src/errors.ts";
import { createMetrics } from "../../gateway/src/metrics.ts";
import { applyPostgresMigration, createPostgresStore, POSTGRES_SCHEMA } from "./postgres-store.ts";
import type { Store } from "@axond/sdk";

const dsn = process.env["AXOND_TEST_POSTGRES"];

async function connect(): Promise<{ client: pg.Client; release: () => Promise<void> }> {
  const client = new pg.Client({ connectionString: dsn });
  await client.connect();
  return {
    client,
    release: () => client.end(),
  };
}

function store(): Store {
  return createPostgresStore(async () => {
    const opened = await connect();
    return {
      client: {
        query: async (sql, params) => {
          const result = await opened.client.query(sql, params ? [...params] : []);
          return { rows: result.rows as Record<string, unknown>[], rowCount: result.rowCount };
        },
      },
      release: opened.release,
    };
  });
}

async function reset(): Promise<void> {
  const opened = await connect();
  await opened.client.query(POSTGRES_SCHEMA);
  await opened.client.query(`
    TRUNCATE axond_store_usage, axond_store_budget, axond_store_budget_active,
      axond_store_budget_cadence, axond_namespace_incarnation, axond_namespace,
      axond_store_provider_models, axond_catalog_streak, axond_schema_migrations
  `);
  await opened.release();
}

test("a postgres connection error drops the driver message", async () => {
  const db = createPostgresStore(async () => {
    throw new Error("password=super-secret-value");
  });
  await assert.rejects(() => db.getNamespace("platform"), (error: unknown) => {
    assert.ok(error instanceof StoreFailure);
    assert.equal(error.message.includes("super-secret-value"), false);
    return true;
  });
});

test("postgres store counts one session per call and omits a failed connect", async () => {
  const metrics = createMetrics();
  const store = createPostgresStore(async () => {
    return {
      client: {
        query: async () => ({ rows: [], rowCount: 0 }),
      },
      release: async () => undefined,
    };
  }, metrics);
  await store.query("select 1");
  assert.equal(
    metrics.points.some((point) => point.name === "axond.store.operations"),
    false,
  );
  const opened = metrics.points.find((point) => point.name === "axond.store.connections_opened");
  const discarded = metrics.points.find((point) => point.name === "axond.store.connections_discarded");
  assert.equal(opened?.value, 1);
  assert.equal(opened?.attributes["axond.store.backend"], "postgres");
  assert.equal(discarded?.value, 1);
  assert.equal(await store.getNamespace("platform"), null);
  const read = metrics.points.find(
    (point) => point.name === "axond.store.operations" && point.attributes["axond.store.operation"] === "namespace_read",
  );
  const duration = metrics.points.find(
    (point) => point.name === "axond.store.query_duration" && point.attributes["axond.store.operation"] === "namespace_read",
  );
  assert.equal(read?.value, 1);
  assert.equal(read?.attributes["axond.store.outcome"], "ok");
  assert.equal(read?.attributes["axond.store.backend"], "postgres");
  assert.ok(duration && duration.value >= 0);
  assert.equal(opened?.value, 2);
  assert.equal(discarded?.value, 2);

  const failing = createMetrics();
  const down = createPostgresStore(async () => {
    throw new Error("password=sk-live-secret");
  }, failing);
  await assert.rejects(() => down.getNamespace("platform"), (error: unknown) => error instanceof StoreFailure);
  const failed = failing.points.find((point) => point.name === "axond.store.operations");
  assert.equal(failed?.value, 1);
  assert.equal(failed?.attributes["axond.store.outcome"], "error");
  assert.equal(failed?.attributes["axond.store.operation"], "namespace_read");
  assert.equal(
    failing.points.some((point) => point.name === "axond.store.query_duration"),
    false,
  );
  assert.equal(
    failing.points.some((point) => point.name === "axond.store.connections_opened"),
    false,
  );
  assert.equal(
    failing.points.some((point) => point.name === "axond.store.connections_discarded"),
    false,
  );
  assert.equal(JSON.stringify(failing.points).includes("sk-live-secret"), false);
  assert.equal(JSON.stringify(metrics.points).includes("sk-live-secret"), false);
});

test("postgres 16 charges one request_id once and ignores a stale incarnation", { skip: !dsn }, async () => {
  await reset();
  const db = store();
  await db.putNamespace({
    id: "platform",
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: true,
  });
  await db.putBudget("platform", "compat", 1_000_000n);
  const same = Array.from({ length: 10 }, () =>
    db.settle({
      requestId: "same",
      namespace: "platform",
      period: "compat",
      model: "fake-openai/gpt",
      status: "ok",
      cost: 1000n,
      incarnation: 1n,
    }),
  );
  const distinct = Array.from({ length: 10 }, (_, index) =>
    db.settle({
      requestId: `id-${index}`,
      namespace: "platform",
      period: "compat",
      model: "fake-openai/gpt",
      status: "ok",
      cost: 1000n,
      incarnation: 1n,
    }),
  );
  const results = await Promise.all([...same, ...distinct]);
  assert.equal(results.filter((result) => result.charged).length, 11);
  const budget = await db.getBudget("platform", "compat");
  assert.equal(budget?.spent, 11_000n);

  assert.equal(await db.deleteNamespace("platform"), true);
  await db.putNamespace({
    id: "platform",
    attrs: {},
    blocklist: null,
    allowPlatformFallback: true,
    fromConfig: false,
  });
  await db.putBudget("platform", "compat", 1_000_000n);
  const late = await db.settle({
    requestId: "late",
    namespace: "platform",
    period: "compat",
    model: "fake-openai/gpt",
    status: "ok",
    cost: 1000n,
    incarnation: 1n,
  });
  assert.equal(late.charged, false);
  assert.equal((await db.getBudget("platform", "compat"))?.spent, 0n);
});

test("a cached namespace read serves a deleted row; the live store does not", { skip: !dsn }, async () => {
  await reset();
  const db = store();
  await db.putNamespace({
    id: "platform",
    attrs: { team: "core" },
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: true,
  });
  const cache = new Map<string, Record<string, unknown>[]>();
  const cached = createPostgresStore(async () => {
    const opened = await connect();
    return {
      client: {
        query: async (sql, params) => {
          const key = `${sql}\0${JSON.stringify(params ?? [])}`;
          if (sql.trim().startsWith("SELECT") && cache.has(key)) {
            return { rows: cache.get(key)!, rowCount: cache.get(key)!.length };
          }
          const result = await opened.client.query(sql, params ? [...params] : []);
          const rows = result.rows as Record<string, unknown>[];
          if (sql.trim().startsWith("SELECT")) {
            cache.set(key, rows);
          }
          return { rows, rowCount: result.rowCount };
        },
      },
      release: opened.release,
    };
  });
  assert.equal((await cached.getNamespace("platform"))?.id, "platform");
  assert.equal(await db.deleteNamespace("platform"), true);
  assert.equal((await cached.getNamespace("platform"))?.id, "platform");
  assert.equal(await db.getNamespace("platform"), null);
});

test("applying the postgres schema twice is idempotent", { skip: !dsn }, async () => {
  const opened = await connect();
  await opened.client.query(POSTGRES_SCHEMA);
  await opened.client.query(POSTGRES_SCHEMA);
  await opened.release();
});

test("postgres_extension_migration_applies_once_and_omits_the_driver_text", { skip: !dsn }, async () => {
  await reset();
  const secret = "sk-migration-sentinel";
  const id = "demo:0";
  await applyPostgresMigration(dsn!, id, "CREATE TABLE axond_ext_demo_note (id text primary key)");
  await applyPostgresMigration(dsn!, id, "CREATE TABLE axond_ext_demo_note (id text primary key)");
  const opened = await connect();
  try {
    const rows = await opened.client.query("SELECT id FROM axond_schema_migrations WHERE id = $1", [id]);
    assert.equal(rows.rows.length, 1);
    const table = await opened.client.query("SELECT to_regclass('axond_ext_demo_note') AS name");
    assert.equal(table.rows[0]?.["name"], "axond_ext_demo_note");
  } finally {
    await opened.client.query("DROP TABLE IF EXISTS axond_ext_demo_note");
    await opened.release();
  }
  const bad = `SELECT '${secret}'::int`;
  await assert.rejects(() => applyPostgresMigration(dsn!, "demo:1", bad), (error: unknown) => {
    assert.ok(error instanceof Error);
    assert.equal(error.message, "extension migration demo:1 failed");
    assert.equal(error.message.includes(secret), false);
    assert.equal(error.message.includes(dsn!), false);
    return true;
  });
  const check = await connect();
  try {
    const missing = await check.client.query("SELECT id FROM axond_schema_migrations WHERE id = $1", ["demo:1"]);
    assert.equal(missing.rows.length, 0);
  } finally {
    await check.release();
  }
});

test("postgres provider models keep the last payload and reject a different fresh source", { skip: !dsn }, async () => {
  await reset();
  const db = store();
  await db.upsertProviderModels({
    provider: "openai",
    fetchedAt: "2026-09-29T00:00:00Z",
    stale: false,
    data: [{ id: "gpt-test" }],
    source: "https://api.openai.com/v1",
  });
  await db.upsertProviderModels({
    provider: "openai",
    fetchedAt: "2026-09-29T00:01:00Z",
    stale: false,
    data: [{ id: "other" }],
    source: "https://example.invalid/v1",
  });
  const kept = await db.getProviderModels("openai");
  assert.deepEqual(kept?.data, [{ id: "gpt-test" }]);
  await db.markProviderModelsStale("openai");
  const stale = await db.getProviderModels("openai");
  assert.equal(stale?.stale, true);
  assert.deepEqual(stale?.data, [{ id: "gpt-test" }]);
  await db.upsertProviderModels({
    provider: "openai",
    fetchedAt: "2026-09-29T00:02:00Z",
    stale: false,
    data: [{ id: "other" }],
    source: "https://example.invalid/v1",
  });
  const replaced = await db.getProviderModels("openai");
  assert.equal(replaced?.stale, false);
  assert.deepEqual(replaced?.data, [{ id: "other" }]);
});

test("a catalogue refusal streak survives a new postgres store", { skip: !dsn }, async () => {
  await reset();
  assert.equal(await store().noteCatalogRefusal(), 1);
  assert.equal(await store().noteCatalogRefusal(), 2);
  await store().resetCatalogStreak();
  assert.equal(await store().noteCatalogRefusal(), 1);
});
