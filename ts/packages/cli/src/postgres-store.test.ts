import assert from "node:assert/strict";
import test from "node:test";

import pg from "pg";

import { StoreFailure } from "../../gateway/src/errors.ts";
import { createMetrics } from "../../gateway/src/metrics.ts";
import {
  applyPostgresMigration,
  applyPostgresMigrationOn,
  applyPostgresSchema,
  createPostgresStore,
  POSTGRES_SCHEMA,
  postgresQueryText,
  postgresSchemaTables,
} from "./postgres-store.ts";
import { seedConfigNamespaces } from "./seed-namespaces.ts";
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

test("the postgres schema seeds a row lock and does not take an advisory lock", () => {
  assert.equal(POSTGRES_SCHEMA.includes("CREATE TABLE IF NOT EXISTS axond_schema_lock"), true);
  assert.equal(POSTGRES_SCHEMA.includes("INSERT INTO axond_schema_lock (id) VALUES (1) ON CONFLICT (id) DO NOTHING"), true);
  assert.equal(POSTGRES_SCHEMA.toLowerCase().includes("pg_advisory"), false);
});

test("an extension migration locks a row and skips an advisory lock", async () => {
  const seen: string[] = [];
  await applyPostgresMigrationOn(
    {
      async query(sql) {
        seen.push(sql);
        if (sql.includes("FROM axond_schema_migrations")) {
          return { rows: [], rowCount: 0 };
        }
        return { rows: [{ id: 1 }], rowCount: 1 };
      },
    },
    "demo:0",
    "CREATE TABLE axond_ext_demo (id int)",
  );
  assert.equal(seen.some((sql) => sql.toLowerCase().includes("pg_advisory")), false);
  assert.equal(seen.some((sql) => sql.includes("INSERT INTO axond_schema_lock") && sql.includes("DO UPDATE")), true);
  assert.equal(seen.includes("CREATE TABLE axond_ext_demo (id int)"), true);
  assert.equal(seen.at(-1), "COMMIT");
});

test("postgres query text numbers question-mark binds and keeps quotes", () => {
  assert.equal(
    postgresQueryText(
      "SELECT count FROM axond_ext_ratelimit_window WHERE namespace = ? AND bucket = ? AND note = '?'",
      2,
    ),
    "SELECT count FROM axond_ext_ratelimit_window WHERE namespace = $1 AND bucket = $2 AND note = '?'",
  );
  assert.equal(postgresQueryText("SELECT $1::text -- ?\n/* ? */", 1), "SELECT $1::text -- ?\n/* ? */");
  assert.equal(postgresQueryText("SELECT $q$?$q$", 1), "SELECT $q$?$q$");
  assert.equal(postgresQueryText("SELECT $$ ? $$", 1), "SELECT $$ ? $$");
  assert.throws(() => postgresQueryText("SELECT ?", 2), (error: unknown) => error instanceof StoreFailure);
});

test("postgres store query sends numbered binds to the driver", async () => {
  let seen = "";
  let params: readonly unknown[] = [];
  const db = createPostgresStore(async () => ({
    client: {
      async query(sql, bound) {
        seen = sql;
        params = bound ?? [];
        return { rows: [{ bucket: "7" }], rowCount: 1 };
      },
    },
    release: async () => undefined,
  }));
  const rows = await db.query("SELECT bucket FROM axond_ext_bind_probe WHERE namespace = ?", ["platform"]);
  assert.equal(seen, "SELECT bucket FROM axond_ext_bind_probe WHERE namespace = $1");
  assert.deepEqual(params, ["platform"]);
  assert.equal(rows.rows[0]?.["bucket"], "7");
});

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

test("a write role keeps an existing postgres schema and names a missing table", async () => {
  const tables = postgresSchemaTables();
  assert.equal(tables.includes("axond_namespace"), true);
  assert.equal(tables.includes("axond_schema_lock"), true);
  await applyPostgresSchema({
    async query(sql, params) {
      if (sql === POSTGRES_SCHEMA) {
        const error = new Error("permission denied for schema public password=secret");
        (error as { code?: string }).code = "42501";
        throw error;
      }
      return { rows: [{ name: String(params?.[0]) }], rowCount: 1 };
    },
  });
  await assert.rejects(
    () =>
      applyPostgresSchema({
        async query(sql, params) {
          if (sql === POSTGRES_SCHEMA) {
            const error = new Error("permission denied password=secret");
            (error as { code?: string }).code = "42501";
            throw error;
          }
          const name = String(params?.[0]);
          return { rows: [{ name: name === "axond_schema_lock" ? null : name }], rowCount: 1 };
        },
      }),
    (error: unknown) => {
      assert.ok(error instanceof Error);
      assert.equal(error.message, "postgres schema is missing axond_schema_lock");
      assert.equal(error.message.includes("password=secret"), false);
      return true;
    },
  );
});

test("a restricted role uses a schema an owner already applied", { skip: !dsn }, async (t) => {
  const admin = await connect();
  const role = await admin.client.query("SELECT rolsuper FROM pg_roles WHERE rolname = current_user");
  if (role.rows[0]?.["rolsuper"] !== true) {
    await admin.release();
    t.skip("the test role cannot create a restricted role");
    return;
  }
  const password = `pw_${crypto.randomUUID().replaceAll("-", "")}`;
  await admin.client.query(`
    DO $$ BEGIN
      CREATE ROLE axond_rw LOGIN PASSWORD '${password}' NOSUPERUSER NOCREATEDB NOCREATEROLE;
    EXCEPTION WHEN duplicate_object THEN NULL;
    END $$;
  `);
  await admin.client.query(`ALTER ROLE axond_rw PASSWORD '${password}'`);
  const exists = await admin.client.query("SELECT 1 FROM pg_database WHERE datname = 'axond_rw'");
  if (exists.rows.length === 0) {
    await admin.client.query("CREATE DATABASE axond_rw OWNER CURRENT_USER");
  }
  await admin.release();
  const ownerUrl = new URL(dsn!);
  ownerUrl.pathname = "/axond_rw";
  const owner = new pg.Client({ connectionString: ownerUrl.toString() });
  await owner.connect();
  await owner.query("REVOKE CREATE ON SCHEMA public FROM PUBLIC");
  await owner.query("GRANT USAGE ON SCHEMA public TO axond_rw");
  await owner.query("GRANT pg_read_all_data, pg_write_all_data TO axond_rw");
  await applyPostgresSchema(sqlExecutor(owner));
  await owner.query("DROP TABLE IF EXISTS axond_ext_priv_probe");
  await owner.query("DELETE FROM axond_schema_migrations WHERE id = 'priv:0'");
  await owner.query("CREATE TABLE axond_ext_priv_probe (id text primary key)");
  await owner.end();
  const restrictedUrl = new URL(ownerUrl.toString());
  restrictedUrl.username = "axond_rw";
  restrictedUrl.password = password;
  const restricted = new pg.Client({ connectionString: restrictedUrl.toString() });
  await restricted.connect();
  try {
    await applyPostgresSchema(sqlExecutor(restricted));
    await applyPostgresMigrationOn(
      sqlExecutor(restricted),
      "priv:0",
      "CREATE TABLE IF NOT EXISTS axond_ext_priv_probe (id text primary key)",
    );
    const recorded = await restricted.query("SELECT id FROM axond_schema_migrations WHERE id = 'priv:0'");
    assert.equal(recorded.rows.length, 1);
    await ownerConnectDropLock(ownerUrl.toString());
    await assert.rejects(() => applyPostgresSchema(sqlExecutor(restricted)), (error: unknown) => {
      assert.ok(error instanceof Error);
      assert.equal(error.message.includes("axond_schema_lock"), true);
      assert.equal(error.message.includes(password), false);
      return true;
    });
  } finally {
    await restricted.end();
    const restore = new pg.Client({ connectionString: ownerUrl.toString() });
    await restore.connect();
    await applyPostgresSchema(sqlExecutor(restore));
    await restore.query("DROP TABLE IF EXISTS axond_ext_priv_probe");
    await restore.end();
  }
});

function sqlExecutor(client: pg.Client) {
  return {
    async query(sql: string, params?: readonly unknown[]) {
      const result = params === undefined ? await client.query(sql) : await client.query(sql, [...params]);
      const row = Array.isArray(result) ? result[result.length - 1] : result;
      return { rows: (row?.rows ?? []) as Record<string, unknown>[], rowCount: row?.rowCount ?? null };
    },
  };
}

async function ownerConnectDropLock(connectionString: string): Promise<void> {
  const owner = new pg.Client({ connectionString });
  await owner.connect();
  await owner.query("DROP TABLE axond_schema_lock");
  await owner.end();
}

test("applying the postgres schema twice is idempotent", { skip: !dsn }, async () => {
  const opened = await connect();
  await opened.client.query(POSTGRES_SCHEMA);
  await opened.client.query(POSTGRES_SCHEMA);
  await opened.release();
});

test("postgres_extension_query_with_question_marks_reads_the_row", { skip: !dsn }, async () => {
  await reset();
  const db = store();
  await db.query(
    "CREATE TABLE IF NOT EXISTS axond_ext_bind_probe (namespace text NOT NULL, bucket text NOT NULL)",
  );
  try {
    await db.query("INSERT INTO axond_ext_bind_probe (namespace, bucket) VALUES (?, ?)", ["platform", "7"]);
    const found = await db.query(
      "SELECT bucket FROM axond_ext_bind_probe WHERE namespace = ? AND '?' = '?'",
      ["platform"],
    );
    assert.equal(found.rows[0]?.["bucket"], "7");
  } finally {
    const opened = await connect();
    await opened.client.query("DROP TABLE IF EXISTS axond_ext_bind_probe");
    await opened.release();
  }
});

test("postgres_extension_migration_serializes_on_a_row_lock", { skip: !dsn }, async () => {
  await reset();
  const id = "demo:race";
  const sql = "CREATE TABLE axond_ext_lock_race (id integer primary key)";
  await Promise.all([applyPostgresMigration(dsn!, id, sql), applyPostgresMigration(dsn!, id, sql)]);
  const opened = await connect();
  try {
    const rows = await opened.client.query("SELECT id FROM axond_schema_migrations WHERE id = $1", [id]);
    assert.equal(rows.rows.length, 1);
    const table = await opened.client.query("SELECT to_regclass('axond_ext_lock_race') AS name");
    assert.equal(table.rows[0]?.["name"], "axond_ext_lock_race");
  } finally {
    await opened.client.query("DROP TABLE IF EXISTS axond_ext_lock_race");
    await opened.release();
  }
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

test("config_seed_tracks_file_fallback_on_postgres", { skip: !dsn }, async () => {
  await reset();
  const db = store();
  await db.putNamespace({
    id: "wsp_ok",
    attrs: { org: "acme" },
    blocklist: ["gpt*"],
    allowPlatformFallback: false,
    fromConfig: true,
  });
  await db.putNamespace({
    id: "left_file",
    attrs: { keep: true },
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: true,
  });
  await seedConfigNamespaces(db, [{ id: "wsp_ok", allowPlatformFallback: true }]);
  const kept = await db.getNamespace("wsp_ok");
  assert.equal(kept?.allowPlatformFallback, true);
  assert.equal(kept?.fromConfig, true);
  assert.deepEqual(kept?.attrs, { org: "acme" });
  assert.deepEqual(kept?.blocklist, ["gpt*"]);
  const released = await db.getNamespace("left_file");
  assert.equal(released?.fromConfig, false);
  assert.deepEqual(released?.attrs, { keep: true });
});

test("a catalogue refusal streak survives a new postgres store", { skip: !dsn }, async () => {
  await reset();
  assert.equal(await store().noteCatalogRefusal(), 1);
  assert.equal(await store().noteCatalogRefusal(), 2);
  await store().resetCatalogStreak();
  assert.equal(await store().noteCatalogRefusal(), 1);
});
