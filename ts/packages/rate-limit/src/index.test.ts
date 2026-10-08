import assert from "node:assert/strict";
import test from "node:test";

import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import pg from "pg";

import { createAxond } from "@axond/gateway";
import { applyPostgresMigration, createPostgresStore, POSTGRES_SCHEMA } from "../../cli/src/postgres-store.ts";
import { applyMigration, openSqliteStore } from "../../cli/src/sqlite-store.ts";
import { createMemoryStore } from "../../gateway/src/memory-store.ts";

import { rateLimitExtension, resetIsolateCounters } from "./index.ts";

test("isolate mode returns 429 after the configured limit", async () => {
  resetIsolateCounters();
  const store = createMemoryStore();
  await store.putNamespace({
    id: "platform",
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: true,
  });
  const app = createAxond({
    store,
    gatewayKey: "k",
    defaultNamespace: "platform",
    providers: [],
    extensions: [rateLimitExtension({ limit: 1, windowMs: 60_000 })],
  });
  const first = await app.request("http://127.0.0.1/ns/platform/v1/models", {
    headers: { authorization: "Bearer k" },
  });
  assert.equal(first.status, 200);
  const second = await app.request("http://127.0.0.1/ns/platform/v1/models", {
    headers: { authorization: "Bearer k" },
  });
  assert.equal(second.status, 429);
  assert.equal((await second.json()).error.type, "rate_limited");
});

test("store mode on postgres saturates one namespace and leaves another its capacity", { skip: !process.env["AXOND_TEST_POSTGRES"] }, async () => {
  const dsn = process.env["AXOND_TEST_POSTGRES"]!;
  const extension = rateLimitExtension({ limit: 1, windowMs: 60_000, mode: "store" });
  const setup = new pg.Client({ connectionString: dsn });
  await setup.connect();
  try {
    await setup.query(POSTGRES_SCHEMA);
    await setup.query("DROP TABLE IF EXISTS axond_ext_ratelimit_window");
    await setup.query("DELETE FROM axond_schema_migrations WHERE id = 'ratelimit:0'");
  } finally {
    await setup.end();
  }
  await applyPostgresMigration(dsn, "ratelimit:0", extension.migrations![0]!);
  const store = createPostgresStore(async () => {
    const client = new pg.Client({ connectionString: dsn });
    await client.connect();
    return {
      client: {
        query: async (sql, params) => {
          const result = await client.query(sql, params ? [...params] : []);
          return { rows: result.rows as Record<string, unknown>[], rowCount: result.rowCount };
        },
      },
      release: () => client.end(),
    };
  });
  try {
    for (const id of ["rl-platform", "rl-tenant", "rl-race"]) {
      await store.putNamespace({
        id,
        attrs: {},
        blocklist: null,
        allowPlatformFallback: false,
        fromConfig: true,
      });
    }
    const app = createAxond({
      store,
      gatewayKey: "k",
      defaultNamespace: "rl-platform",
      providers: [],
      extensions: [extension],
    });
    const call = (namespace: string) =>
      app.request(`http://127.0.0.1/ns/${namespace}/v1/models`, { headers: { authorization: "Bearer k" } });
    assert.equal((await call("rl-platform")).status, 200);
    assert.equal((await call("rl-platform")).status, 429);
    assert.equal((await call("rl-tenant")).status, 200);
    const raced = await Promise.all(Array.from({ length: 8 }, () => call("rl-race")));
    assert.equal(raced.filter((response) => response.status === 200).length, 1);
    assert.equal(raced.filter((response) => response.status === 429).length, 7);
    const held = await store.query("SELECT count FROM axond_ext_ratelimit_window WHERE namespace = ?", ["rl-race"]);
    assert.equal(Number(held.rows[0]?.["count"]), 1);
  } finally {
    const cleanup = new pg.Client({ connectionString: dsn });
    await cleanup.connect();
    await cleanup.query("DROP TABLE IF EXISTS axond_ext_ratelimit_window");
    await cleanup.query("DELETE FROM axond_namespace WHERE id IN ('rl-platform', 'rl-tenant', 'rl-race')");
    await cleanup.end();
  }
});

test("store mode saturates one namespace and leaves another its capacity", async () => {
  const dir = await mkdtemp(join(tmpdir(), "axond-rl-"));
  const path = join(dir, "axond.sqlite");
  try {
    const extension = rateLimitExtension({ limit: 1, windowMs: 60_000, mode: "store" });
    applyMigration(path, "ratelimit:0", extension.migrations![0]!);
    const store = openSqliteStore(path);
    for (const id of ["platform", "tenant"]) {
      await store.putNamespace({
        id,
        attrs: {},
        blocklist: null,
        allowPlatformFallback: false,
        fromConfig: true,
      });
    }
    const app = createAxond({
      store,
      gatewayKey: "k",
      defaultNamespace: "platform",
      providers: [],
      extensions: [extension],
    });
    const call = (namespace: string) =>
      app.request(`http://127.0.0.1/ns/${namespace}/v1/models`, { headers: { authorization: "Bearer k" } });
    assert.equal((await call("platform")).status, 200);
    assert.equal((await call("platform")).status, 429);
    assert.equal((await call("tenant")).status, 200);
    const held = await store.query("SELECT count FROM axond_ext_ratelimit_window WHERE namespace = ?", ["platform"]);
    assert.equal(Number(held.rows[0]?.["count"]), 1);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("independent limiter instances and zero allowances remain independent", async () => {
  const store = createMemoryStore();
  await store.putNamespace({ id: "platform", attrs: {}, blocklist: null, allowPlatformFallback: false, fromConfig: false });
  const build = (limit: number) => createAxond({ store, gatewayKey: "k", defaultNamespace: "platform", providers: [], extensions: [rateLimitExtension({ limit, windowMs: 60000 })] });
  const get = (app: ReturnType<typeof build>) => app.request("http://localhost/ns/platform/v1/models", { headers: { authorization: "Bearer k" } });
  const one = build(1); const another = build(1);
  assert.equal((await get(one)).status, 200); assert.equal((await get(another)).status, 200);
  assert.equal((await get(build(0))).status, 429);
  assert.throws(() => rateLimitExtension({ limit: -1, windowMs: 10 }));
  assert.throws(() => rateLimitExtension({ limit: 1, windowMs: 0 }));
});
