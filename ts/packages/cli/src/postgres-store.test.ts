import assert from "node:assert/strict";
import test from "node:test";

import pg from "pg";

import { createPostgresStore, POSTGRES_SCHEMA } from "./postgres-store.ts";
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
      axond_store_provider_models, axond_schema_migrations
  `);
  await opened.release();
}

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
