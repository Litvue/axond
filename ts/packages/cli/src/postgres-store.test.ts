import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createServer } from "node:net";
import test from "node:test";

import pg from "pg";

import { GatewayFailure, StoreFailure } from "../../gateway/src/errors.ts";
import { createMetrics } from "../../gateway/src/metrics.ts";

import {
  applyPostgresMigration,
  applyPostgresMigrationOn,
  applyPostgresSchema,
  closePgClient,
  createPostgresStore,
  holdPgClient,
  POSTGRES_SCHEMA,
  postgresClientOptions,
  postgresQueryText,
  postgresSchemaTables,
  queryPgClient,
} from "./postgres-store.ts";
import { seedConfigNamespaces } from "./seed-namespaces.ts";
import type { Store } from "@axond/sdk";

const dsn = process.env["AXOND_TEST_POSTGRES"];

async function connect(): Promise<{ client: pg.Client; release: () => Promise<void> }> {
  const client = holdPgClient(new pg.Client({ connectionString: dsn }));
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

function postgresAuthReady(): Buffer {
  const auth = Buffer.alloc(9);
  auth.write("R", 0, "ascii");
  auth.writeInt32BE(8, 1);
  auth.writeInt32BE(0, 5);
  const ready = Buffer.alloc(6);
  ready.write("Z", 0, "ascii");
  ready.writeInt32BE(5, 1);
  ready.write("I", 5, "ascii");
  return Buffer.concat([auth, ready]);
}

function postgresSelectDone(): Buffer {
  const tag = Buffer.from("SELECT 1\0");
  const complete = Buffer.alloc(5 + tag.length);
  complete.write("C", 0, "ascii");
  complete.writeInt32BE(4 + tag.length, 1);
  tag.copy(complete, 5);
  const ready = Buffer.alloc(6);
  ready.write("Z", 0, "ascii");
  ready.writeInt32BE(5, 1);
  ready.write("I", 5, "ascii");
  return Buffer.concat([complete, ready]);
}

test("closePgClient returns when the peer closes and when the socket stays silent", async () => {
  const closed = await closeAgainstPeer(true);
  assert.equal(closed.ms < 500, true, `peer close took ${closed.ms}ms`);
  const silent = await closeAgainstPeer(false, true);
  assert.equal(silent.ms >= 800, true, `silent socket returned in ${silent.ms}ms`);
  assert.equal(silent.ms < 2_000, true, `silent socket took ${silent.ms}ms`);
});

function closeAgainstPeer(closeOnTerminate: boolean, silenceSocket = false): Promise<{ ms: number }> {
  let peer: import("node:net").Socket | undefined;
  const server = createServer((socket) => {
    peer = socket;
    socket.on("error", () => undefined);
    let started = false;
    socket.on("data", (chunk) => {
      if (!started) {
        started = true;
        socket.write(postgresAuthReady());
        return;
      }
      if (chunk.includes(0x58)) {
        if (closeOnTerminate) {
          socket.end();
        }
        return;
      }
      if (chunk.includes(0x51)) {
        socket.write(postgresSelectDone());
      }
    });
  });
  return new Promise((resolve, reject) => {
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close();
        reject(new Error("no port"));
        return;
      }
      const client = holdPgClient(new pg.Client({
        connectionString: `postgres://axond:socket-secret@127.0.0.1:${address.port}/axond`,
        connectionTimeoutMillis: 2_000,
      }));
      void (async () => {
        const startedAt = Date.now();
        try {
          await client.connect();
          await client.query("SELECT 1");
          const stream = (client as pg.Client & {
            connection: { stream: NodeJS.WritableStream & { destroy: () => void } };
          }).connection.stream;
          const destroySocket = stream.destroy.bind(stream);
          if (silenceSocket) {
            stream.write = ((_data, encoding, callback) => {
              const done = typeof encoding === "function" ? encoding : callback;
              done?.();
              return true;
            }) as typeof stream.write;
            stream.end = (() => stream) as typeof stream.end;
            stream.destroy = (() => stream) as typeof stream.destroy;
          }
          await closePgClient(client);
          if (silenceSocket) {
            destroySocket();
          }
          resolve({ ms: Date.now() - startedAt });
        } catch (error) {
          reject(error);
        } finally {
          peer?.destroy();
          server.close();
        }
      })();
    });
  });
}

test("postgres clients use the connect and statement limits", async () => {
  const options = postgresClientOptions("postgres://axond:socket-secret@127.0.0.1:1/axond");
  assert.equal(options.connectionTimeoutMillis, 15_000);
  assert.equal(options.query_timeout, 60_000);
  assert.equal(options.connectionString.includes("socket-secret"), true);
  const main = await readFile(new URL("./main.ts", import.meta.url), "utf8");
  assert.match(main, /new Client\(postgresClientOptions\(dsn\)\)/);
  assert.match(main, /queryPgClient\(/);
  assert.match(main, /closePgClient\(/);
  const migration = await readFile(new URL("./postgres-store.ts", import.meta.url), "utf8");
  assert.match(migration, /new pg\.Client\(postgresClientOptions\(dsn\)\)/);
});

test("queryPgClient drops the socket when a statement times out", async () => {
  let destroyed = 0;
  const client = {
    connection: { stream: { destroy() { destroyed += 1; } } },
    async query() {
      throw new Error("Query read timeout");
    },
  } as unknown as pg.Client;
  await assert.rejects(() => queryPgClient(client, "SELECT 1"), /Query read timeout/);
  await assert.rejects(
    () => queryPgClient(client, "SELECT $1", [1]),
    /Query read timeout/,
  );
  assert.equal(destroyed, 2);
  const other = {
    connection: { stream: { destroy() { destroyed += 1; } } },
    async query() {
      throw new Error("reset");
    },
  } as unknown as pg.Client;
  await assert.rejects(() => queryPgClient(other, "SELECT 1"), /reset/);
  assert.equal(destroyed, 2);
});

test("a silent postgres accept fails at the connect limit", { timeout: 25_000 }, async () => {
  const peers: import("node:net").Socket[] = [];
  const server = createServer((socket) => {
    peers.push(socket);
    socket.on("error", () => undefined);
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });
  const address = server.address();
  if (!address || typeof address === "string") {
    server.close();
    throw new Error("no port");
  }
  const client = holdPgClient(new pg.Client(postgresClientOptions(
    `postgres://axond:socket-secret@127.0.0.1:${address.port}/axond`,
  )));
  const started = Date.now();
  try {
    const outcome = await Promise.race([
      client.connect().then(() => "ok" as const, (error: unknown) => error),
      new Promise<"hung">((resolve) => setTimeout(() => resolve("hung"), 20_000)),
    ]);
    const ms = Date.now() - started;
    assert.notEqual(outcome, "hung");
    assert.notEqual(outcome, "ok");
    assert.equal(outcome instanceof Error, true);
    assert.equal(String(outcome).includes("socket-secret"), false);
    assert.equal(ms >= 12_000, true, `connect returned in ${ms}ms`);
    assert.equal(ms < 20_000, true, `connect took ${ms}ms`);
  } finally {
    await closePgClient(client);
    for (const peer of peers) {
      peer.destroy();
    }
    server.close();
  }
});

test("a dropped postgres socket does not crash the process", async (t) => {
  if (!dsn) {
    t.skip("AXOND_TEST_POSTGRES is unset");
    return;
  }
  const killer = holdPgClient(new pg.Client({ connectionString: dsn }));
  const victim = holdPgClient(new pg.Client({ connectionString: dsn }));
  await killer.connect();
  await victim.connect();
  try {
    const pid = (await victim.query("SELECT pg_backend_pid() AS pid")).rows[0]?.["pid"];
    await killer.query("SELECT pg_terminate_backend($1)", [pid]);
    await new Promise((resolve) => setTimeout(resolve, 200));
    await assert.rejects(() => victim.query("SELECT 1"));
  } finally {
    await victim.end().catch(() => undefined);
    await killer.end().catch(() => undefined);
  }
});

test("a parameterized query reports a socket reset that never emits close", async (t) => {
  if (!dsn) {
    t.skip("AXOND_TEST_POSTGRES is unset");
    return;
  }
  // pg 8.23.0 set connection._ending on every extended-protocol Sync, so a
  // later ECONNRESET was ignored. A Workers socket can report that reset
  // without emitting close. The query then waited out query_timeout.
  const client = holdPgClient(new pg.Client({
    connectionString: dsn,
    query_timeout: 2_500,
  }));
  await client.connect();
  const stream = client.connection.stream as {
    destroy: (error?: Error) => void;
    emit: (event: string, ...args: unknown[]) => boolean;
  };
  const emit = stream.emit.bind(stream);
  try {
    await client.query("SELECT $1::int AS n", [1]);
    stream.emit = (event: string, ...args: unknown[]) => {
      if (event === "close" || event === "end") {
        return false;
      }
      return emit(event, ...args);
    };
    const started = Date.now();
    const pending = client.query("SELECT pg_sleep(8), $1::int AS n", [2]);
    setTimeout(() => {
      const error = new Error("reset") as Error & { code?: string };
      error.code = "ECONNRESET";
      stream.destroy(error);
    }, 40);
    await assert.rejects(pending, (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", "reset");
      return true;
    });
    assert.equal(Date.now() - started < 1_000, true);
  } finally {
    stream.emit = emit;
    stream.destroy();
    await closePgClient(client);
  }
});

test("the postgres schema seeds a row lock and does not take an advisory lock", () => {
  assert.equal(POSTGRES_SCHEMA.includes("CREATE TABLE IF NOT EXISTS public.axond_schema_lock"), true);
  assert.equal(POSTGRES_SCHEMA.includes("INSERT INTO public.axond_schema_lock (id) VALUES (1) ON CONFLICT (id) DO NOTHING"), true);
  assert.equal(POSTGRES_SCHEMA.toLowerCase().includes("pg_advisory"), false);
  assert.equal(
    POSTGRES_SCHEMA.includes("ALTER TABLE public.axond_namespace ADD COLUMN IF NOT EXISTS allow_platform_fallback boolean NOT NULL DEFAULT false"),
    true,
  );
  assert.equal(
    POSTGRES_SCHEMA.includes("ALTER TABLE public.axond_namespace ADD COLUMN IF NOT EXISTS from_config boolean NOT NULL DEFAULT false"),
    true,
  );
});

test("an extension migration locks a row and skips an advisory lock", async () => {
  const seen: string[] = [];
  await applyPostgresMigrationOn(
    {
      async query(sql) {
        seen.push(sql);
        if (sql.includes("axond_schema_migrations")) {
          return { rows: [], rowCount: 0 };
        }
        return { rows: [{ id: 1 }], rowCount: 1 };
      },
    },
    "demo:0",
    "CREATE TABLE axond_ext_demo (id int)",
  );
  assert.equal(seen.some((sql) => sql.toLowerCase().includes("pg_advisory")), false);
  assert.equal(seen.some((sql) => sql.includes("INSERT INTO public.axond_schema_lock") && sql.includes("DO UPDATE")), true);
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

test("an unknown budget write rolls back and stays a gateway error", async () => {
  const seen: string[] = [];
  const db = createPostgresStore(async () => ({
    client: {
      async query(sql: string) {
        seen.push(sql);
        return { rows: [], rowCount: 0 };
      },
    },
    release: async () => undefined,
  }));
  await assert.rejects(() => db.putBudget("missing", "compat", 1n), (error: unknown) => {
    assert.ok(error instanceof GatewayFailure);
    assert.equal(error.type, "unknown_namespace");
    return true;
  });
  assert.deepEqual(seen, [
    "BEGIN",
    "INSERT INTO public.axond_namespace_lock (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id",
    "SELECT id FROM public.axond_namespace WHERE id = $1",
    "ROLLBACK",
  ]);
});

test("a failed namespace delete rolls the row back", async () => {
  const seen: string[] = [];
  const db = createPostgresStore(async () => ({
    client: {
      async query(sql: string) {
        seen.push(sql);
        if (sql.startsWith("DELETE FROM public.axond_namespace ")) {
          return { rows: [], rowCount: 1 };
        }
        if (sql.includes("axond_namespace_incarnation")) {
          throw new Error("password=secret incarnation refused");
        }
        return { rows: [], rowCount: 0 };
      },
    },
    release: async () => undefined,
  }));
  await assert.rejects(() => db.deleteNamespace("platform"), (error: unknown) => {
    assert.ok(error instanceof StoreFailure);
    assert.equal(error.message.includes("password=secret"), false);
    return true;
  });
  assert.equal(seen[0], "BEGIN");
  assert.equal(seen.at(-1), "ROLLBACK");
  assert.equal(seen.includes("COMMIT"), false);
});

test("postgres rolls back a namespace delete when the incarnation bump fails", { skip: !dsn }, async () => {
  await reset();
  const db = store();
  const id = "txn-rollback";
  await db.putNamespace({
    id,
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: false,
  });
  await db.putBudget(id, "compat", 5n);
  const opened = await connect();
  try {
    await opened.client.query("DROP TRIGGER IF EXISTS axond_test_fail_incarnation ON axond_namespace_incarnation");
    await opened.client.query(`
      CREATE OR REPLACE FUNCTION axond_test_fail_incarnation() RETURNS trigger AS $$
      BEGIN
        IF NEW.id = 'txn-rollback' THEN
          RAISE EXCEPTION 'incarnation refused';
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql
    `);
    await opened.client.query(`
      CREATE TRIGGER axond_test_fail_incarnation
      BEFORE INSERT OR UPDATE ON axond_namespace_incarnation
      FOR EACH ROW EXECUTE FUNCTION axond_test_fail_incarnation()
    `);
    await assert.rejects(() => db.deleteNamespace(id), (error: unknown) => {
      assert.ok(error instanceof StoreFailure);
      assert.equal(error.message.includes("incarnation refused"), false);
      return true;
    });
    assert.equal((await db.getNamespace(id))?.id, id);
    assert.equal((await db.getBudget(id, "compat"))?.limit, 5n);
  } finally {
    await opened.client.query("DROP TRIGGER IF EXISTS axond_test_fail_incarnation ON axond_namespace_incarnation");
    await opened.client.query("DROP FUNCTION IF EXISTS axond_test_fail_incarnation()");
    await opened.release();
  }
});

test("a budget write waits on the namespace lock and does not orphan a ledger", { skip: !dsn }, async () => {
  await reset();
  const db = store();
  const id = "lock-race";
  await db.putNamespace({
    id,
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: false,
  });
  const holder = await connect();
  const look = await connect();
  try {
    await holder.client.query("BEGIN");
    await holder.client.query(
      "INSERT INTO axond_namespace_lock (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id",
      [id],
    );
    const holderPid = await holder.client.query("SELECT pg_backend_pid() AS pid");
    const pid = holderPid.rows[0]?.["pid"];
    let failure: unknown;
    const write = db.putBudget(id, "compat", 5n).then(
      () => undefined,
      (error: unknown) => {
        failure = error;
      },
    );
    const started = Date.now();
    let waiting = false;
    while (Date.now() - started < 2000) {
      const locks = await look.client.query(
        "SELECT count(*)::int AS n FROM pg_stat_activity WHERE $1::int = ANY (pg_blocking_pids(pid))",
        [pid],
      );
      if (Number(locks.rows[0]?.["n"] ?? 0) > 0) {
        waiting = true;
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    assert.equal(waiting, true);
    assert.equal(failure, undefined);
    const mid = await look.client.query("SELECT 1 FROM axond_store_budget WHERE namespace = $1", [id]);
    assert.equal(mid.rowCount, 0);
    await holder.client.query("DELETE FROM axond_namespace WHERE id = $1", [id]);
    await holder.client.query("COMMIT");
    await write;
    assert.ok(failure instanceof GatewayFailure);
    const left = await look.client.query("SELECT 1 FROM axond_store_budget WHERE namespace = $1", [id]);
    assert.equal(left.rowCount, 0);
  } finally {
    await holder.client.query("ROLLBACK").catch(() => undefined);
    await holder.release();
    await look.release();
  }
});

test("a monthly admit locks only while creating the period row", { skip: !dsn }, async () => {
  await reset();
  const db = store();
  const id = "month-lock";
  const march = Date.parse("2026-03-15T12:00:00Z");
  const april = Date.parse("2026-04-15T12:00:00Z");
  await db.putNamespace({
    id,
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: false,
  });
  await db.putBudgetPolicy({
    namespace: id,
    cadence: "monthly",
    limit: 100n,
    timezone: "UTC",
    period: null,
    nowMs: march,
  });
  const holder = await connect();
  const look = await connect();
  try {
    await holder.client.query("BEGIN");
    await holder.client.query(
      "INSERT INTO axond_namespace_lock (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id",
      [id],
    );
    const holderPid = await holder.client.query("SELECT pg_backend_pid() AS pid");
    const pid = holderPid.rows[0]?.["pid"];
    const open = await Promise.race([
      db.resolveNamespace(id, march),
      new Promise<null>((resolve) => setTimeout(() => resolve(null), 500)),
    ]);
    assert.ok(open);
    assert.equal(open.period, "2026-03");
    assert.equal(open.admitted, true);
    assert.equal(open.spent, 0n);
    let failure: unknown;
    const write = db.resolveNamespace(id, april).then(
      (resolved) => resolved,
      (error: unknown) => {
        failure = error;
        return null;
      },
    );
    const started = Date.now();
    let waiting = false;
    while (Date.now() - started < 2000) {
      const locks = await look.client.query(
        "SELECT count(*)::int AS n FROM pg_stat_activity WHERE $1::int = ANY (pg_blocking_pids(pid))",
        [pid],
      );
      if (Number(locks.rows[0]?.["n"] ?? 0) > 0) {
        waiting = true;
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    assert.equal(waiting, true);
    assert.equal(failure, undefined);
    await holder.client.query("COMMIT");
    const created = await write;
    assert.equal(created?.period, "2026-04");
    assert.equal(created?.admitted, true);
    assert.equal(created?.limit, 100n);
    assert.equal(created?.spent, 0n);
  } finally {
    await holder.client.query("ROLLBACK").catch(() => undefined);
    await holder.release();
    await look.release();
  }
});

test("a write role keeps an existing postgres schema and names a missing table", async () => {
  const tables = postgresSchemaTables();
  assert.equal(tables.includes("axond_namespace"), true);
  assert.equal(tables.includes("axond_namespace_lock"), true);
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
          return { rows: [{ name: name === "public.axond_schema_lock" ? null : name }], rowCount: 1 };
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

test("an owner apply adds columns a Rust namespace table omitted", { skip: !dsn }, async (t) => {
  const admin = await connect();
  const role = await admin.client.query("SELECT rolsuper FROM pg_roles WHERE rolname = current_user");
  if (role.rows[0]?.["rolsuper"] !== true) {
    await admin.release();
    t.skip("the test role cannot create a database");
    return;
  }
  const name = `axond_up_${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;
  const url = new URL(dsn!);
  url.pathname = `/${name}`;
  const upgradeDsn = url.toString();
  await admin.client.query(`CREATE DATABASE ${name}`);
  await admin.release();
  const opened = new pg.Client({ connectionString: upgradeDsn });
  await opened.connect();
  try {
    await opened.query(
      `CREATE TABLE axond_namespace (
         id TEXT PRIMARY KEY NOT NULL,
         attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
         blocklist JSONB
       )`,
    );
    await opened.query("INSERT INTO axond_namespace (id, attrs) VALUES ('legacy', '{\"org\":\"acme\"}'::jsonb)");
    await applyPostgresSchema({
      query: async (sql, params) => {
        const result = params === undefined ? await opened.query(sql) : await opened.query(sql, [...params]);
        const row = Array.isArray(result) ? result[result.length - 1] : result;
        return { rows: (row?.rows ?? []) as Record<string, unknown>[], rowCount: row?.rowCount ?? null };
      },
    });
    const columns = await opened.query(
      "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'axond_namespace'",
    );
    const names = columns.rows.map((row) => String(row["column_name"]));
    assert.equal(names.includes("allow_platform_fallback"), true);
    assert.equal(names.includes("from_config"), true);
    const lock = await opened.query("SELECT to_regclass('axond_namespace_lock') AS name");
    assert.equal(lock.rows[0]?.["name"], "axond_namespace_lock");
    const kept = await opened.query("SELECT attrs->>'org' AS org FROM axond_namespace WHERE id = 'legacy'");
    assert.equal(kept.rows[0]?.["org"], "acme");
    const store = createPostgresStore(async () => {
      const client = new pg.Client({ connectionString: upgradeDsn });
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
    const found = await store.getNamespace("legacy");
    assert.equal(found?.id, "legacy");
    assert.equal(found?.allowPlatformFallback, false);
    assert.equal(found?.fromConfig, false);
    assert.equal(found?.attrs["org"], "acme");
  } finally {
    await opened.end().catch(() => undefined);
    const drop = await connect();
    await drop.client.query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()", [
      name,
    ]);
    await drop.client.query(`DROP DATABASE ${name}`);
    await drop.release();
  }
});

test("a role schema does not hide store tables from another role", { skip: !dsn }, async (t) => {
  const admin = await connect();
  const role = await admin.client.query("SELECT rolsuper FROM pg_roles WHERE rolname = current_user");
  if (role.rows[0]?.["rolsuper"] !== true) {
    await admin.release();
    t.skip("the test role cannot create a database");
    return;
  }
  const suffix = crypto.randomUUID().replaceAll("-", "").slice(0, 12);
  const dbName = `axond_sp_${suffix}`;
  const ownerName = `axond_spo_${suffix}`;
  const appName = `axond_spa_${suffix}`;
  const ownerPassword = `pw_${suffix}`;
  const appPassword = `pw_${suffix}a`;
  await admin.client.query(`CREATE ROLE ${ownerName} LOGIN PASSWORD '${ownerPassword}'`);
  await admin.client.query(
    `CREATE ROLE ${appName} LOGIN PASSWORD '${appPassword}' NOSUPERUSER NOCREATEDB NOCREATEROLE`,
  );
  await admin.client.query(`CREATE DATABASE ${dbName} OWNER ${ownerName}`);
  await admin.release();
  const adminUrl = new URL(dsn!);
  adminUrl.pathname = `/${dbName}`;
  const setup = new pg.Client({ connectionString: adminUrl.toString() });
  await setup.connect();
  try {
    await setup.query(`CREATE SCHEMA ${ownerName} AUTHORIZATION ${ownerName}`);
    await setup.query(`CREATE SCHEMA ${appName} AUTHORIZATION ${appName}`);
    await setup.query(`ALTER ROLE ${ownerName} IN DATABASE ${dbName} SET search_path TO ${ownerName}, public`);
    await setup.query(`ALTER ROLE ${appName} IN DATABASE ${dbName} SET search_path TO ${appName}, public`);
    await setup.query("REVOKE CREATE ON SCHEMA public FROM PUBLIC");
    await setup.query(`GRANT USAGE ON SCHEMA public TO ${appName}`);
    await setup.query(`GRANT pg_read_all_data, pg_write_all_data TO ${appName}`);
  } finally {
    await setup.end();
  }
  const ownerUrl = new URL(adminUrl.toString());
  ownerUrl.username = ownerName;
  ownerUrl.password = ownerPassword;
  const owner = new pg.Client({ connectionString: ownerUrl.toString() });
  await owner.connect();
  try {
    const path = await owner.query("SHOW search_path");
    assert.equal(path.rows[0]?.["search_path"], `${ownerName}, public`);
    await applyPostgresSchema(sqlExecutor(owner));
    const placed = await owner.query(
      "SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = 'axond_namespace'",
    );
    assert.deepEqual(
      placed.rows.map((row) => row["nspname"]),
      ["public"],
    );
    const appUrl = new URL(ownerUrl.toString());
    appUrl.username = appName;
    appUrl.password = appPassword;
    const app = createPostgresStore(async () => {
      const client = new pg.Client({ connectionString: appUrl.toString() });
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
    const created = await app.putNamespace({
      id: "visible",
      attrs: { org: "acme" },
      blocklist: null,
      allowPlatformFallback: false,
      fromConfig: false,
    });
    assert.equal(created, "created");
    assert.equal((await app.getNamespace("visible"))?.attrs["org"], "acme");
  } finally {
    await owner.end().catch(() => undefined);
    const drop = await connect();
    await drop.client.query(
      "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
      [dbName],
    );
    await drop.client.query(`DROP DATABASE ${dbName}`);
    await drop.client.query(`REVOKE pg_read_all_data, pg_write_all_data FROM ${appName}`);
    await drop.client.query(`DROP ROLE ${appName}`);
    await drop.client.query(`DROP ROLE ${ownerName}`);
    await drop.release();
  }
});

test("a write role names namespace columns a Rust table omitted", async () => {
  await assert.rejects(
    () =>
      applyPostgresSchema({
        async query(sql, params) {
          if (sql === POSTGRES_SCHEMA) {
            const error = new Error("permission denied password=secret");
            (error as { code?: string }).code = "42501";
            throw error;
          }
          if (sql.includes("pg_attribute")) {
            return { rows: [], rowCount: 0 };
          }
          return { rows: [{ name: String(params?.[0]) }], rowCount: 1 };
        },
      }),
    (error: unknown) => {
      assert.ok(error instanceof Error);
      assert.equal(
        error.message,
        "postgres schema is missing axond_namespace.allow_platform_fallback, axond_namespace.from_config",
      );
      assert.equal(error.message.includes("password=secret"), false);
      return true;
    },
  );
});

test("a restricted role names columns a Rust namespace table omitted", { skip: !dsn }, async (t) => {
  const admin = await connect();
  const role = await admin.client.query("SELECT rolsuper FROM pg_roles WHERE rolname = current_user");
  if (role.rows[0]?.["rolsuper"] !== true) {
    await admin.release();
    t.skip("the test role cannot create a database");
    return;
  }
  const suffix = crypto.randomUUID().replaceAll("-", "").slice(0, 12);
  const dbName = `axond_up_${suffix}`;
  const roleName = `axond_upw_${suffix}`;
  const password = `pw_${suffix}`;
  const ownerUrl = new URL(dsn!);
  ownerUrl.pathname = `/${dbName}`;
  await admin.client.query(`CREATE DATABASE ${dbName}`);
  await admin.client.query(`CREATE ROLE ${roleName} LOGIN PASSWORD '${password}' NOSUPERUSER NOCREATEDB NOCREATEROLE`);
  await admin.release();
  const owner = new pg.Client({ connectionString: ownerUrl.toString() });
  await owner.connect();
  try {
    await owner.query("REVOKE CREATE ON SCHEMA public FROM PUBLIC");
    await owner.query(`GRANT USAGE ON SCHEMA public TO ${roleName}`);
    await owner.query(`GRANT pg_read_all_data, pg_write_all_data TO ${roleName}`);
    await applyPostgresSchema(sqlExecutor(owner));
    await owner.query("ALTER TABLE axond_namespace DROP COLUMN allow_platform_fallback");
    await owner.query("ALTER TABLE axond_namespace DROP COLUMN from_config");
    const restrictedUrl = new URL(ownerUrl.toString());
    restrictedUrl.username = roleName;
    restrictedUrl.password = password;
    const restricted = new pg.Client({ connectionString: restrictedUrl.toString() });
    await restricted.connect();
    try {
      await assert.rejects(() => applyPostgresSchema(sqlExecutor(restricted)), (error: unknown) => {
        assert.ok(error instanceof Error);
        assert.equal(
          error.message,
          "postgres schema is missing axond_namespace.allow_platform_fallback, axond_namespace.from_config",
        );
        assert.equal(error.message.includes(password), false);
        return true;
      });
    } finally {
      await restricted.end();
    }
  } finally {
    await owner.end().catch(() => undefined);
    const drop = await connect();
    await drop.client.query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()", [
      dbName,
    ]);
    await drop.client.query(`DROP DATABASE ${dbName}`);
    await drop.client.query(`REVOKE pg_read_all_data, pg_write_all_data FROM ${roleName}`);
    await drop.client.query(`DROP ROLE ${roleName}`);
    await drop.release();
  }
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
    await admin.client.query("CREATE DATABASE axond_rw");
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
