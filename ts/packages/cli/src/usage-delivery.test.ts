import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { fileURLToPath } from "node:url";

import pg from "pg";

import { loadConfig, envSecretReader } from "../../gateway/src/config.ts";
import { createMetrics } from "../../gateway/src/metrics.ts";

import { createBufferedUsageSink, insertSql, openUsageDelivery, rowValues } from "./usage-delivery.ts";
import { USAGE_ADDITIVE_SQL, USAGE_V2_SQL, usageSchemaDdl } from "./usage-sql.ts";

const dsn = process.env["AXOND_TEST_POSTGRES"];

function sample(requestId = "req_usage") {
  return {
    schemaVersion: 2,
    requestId,
    traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
    namespace: "acme",
    period: "2026-09",
    subject: "GW_KEY",
    model: "openai/gpt-test",
    targetProvider: "openai",
    targetModel: "gpt-test",
    credentialSource: "platform",
    credentialId: "openai-primary",
    status: "ok",
    inputTokens: 4n,
    outputTokens: 1n,
    reasoningTokens: 0n,
    cacheReadTokens: 2n,
    cacheWriteTokens: 0n,
    costMicrodollars: 5n,
    catalogVersion: 0,
    priceBook: null,
    priceBookChecksum: null,
    priceCatalog: null,
    signerKid: null,
    latencyMs: 7,
    attempts: 1,
  };
}

test("embedded usage ddl matches ops/postgres", async () => {
  const root = fileURLToPath(new URL("../../../../ops/postgres/", import.meta.url));
  assert.equal(await readFile(`${root}usage_v2.sql`, "utf8"), USAGE_V2_SQL);
  const files = [
    "usage_v1_001_add_signer_kid.sql",
    "usage_v2_001_add_price_identity.sql",
    "usage_v2_002_nullable_cost.sql",
    "usage_v2_003_add_period.sql",
  ];
  for (const [index, file] of files.entries()) {
    assert.equal(await readFile(`${root}${file}`, "utf8"), USAGE_ADDITIVE_SQL[index]);
  }
  const ddl = usageSchemaDdl("billing.axond_usage");
  assert.match(ddl, /ON billing\.axond_usage \(recorded_at DESC\)/);
  assert.match(ddl, /axond_usage_recorded_at_idx/);
  assert.equal(ddl.includes("billing.axond_usage_recorded_at_idx"), false);
  assert.match(insertSql("axond_usage", 1), /ON CONFLICT DO NOTHING/);
  assert.equal(rowValues({ record: sample(), observedAt: new Date("2026-09-30T00:00:00Z") }).length, 26);
});

test("a full usage buffer drops the record instead of waiting", async () => {
  const metrics = createMetrics([]);
  const logs = [];
  const written = [];
  let release = () => {};
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  const sink = createBufferedUsageSink({
    capacity: 1,
    maxBatch: 500,
    flushIntervalMs: 60_000,
    metrics,
    onLog: (record) => logs.push(record),
    insert: async (rows) => {
      await gate;
      written.push(rows[0].record.requestId);
    },
  });
  sink.write(sample("first"), new Date());
  sink.write(sample("second"), new Date());
  assert.equal(sink.dropped, 1);
  assert.deepEqual(logs, [{ msg: "usage_dropped", sink: "postgres", reason: "buffer_full", dropped: 1 }]);
  release();
  assert.equal(await sink.flush(1_000), true);
  assert.deepEqual(written, ["first"]);
  assert.deepEqual(logs, [
    { msg: "usage_dropped", sink: "postgres", reason: "buffer_full", dropped: 1 },
    { msg: "usage_flush", sink: "postgres", outcome: "flushed", records: 1 },
  ]);
  const dropped = metrics.points.find((point) => point.name === "axond.usage.records_dropped");
  assert.equal(dropped?.attributes["axond.drop_reason"], "buffer_full");
});

test("usage_flush_names_the_outcome_and_omits_the_driver_text", async () => {
  const secret = "dsn-secret-sentinel";
  const metrics = createMetrics([]);
  const logs = [];
  const rejected = createBufferedUsageSink({
    capacity: 10,
    maxBatch: 500,
    flushIntervalMs: 60_000,
    metrics,
    onLog: (record) => logs.push(record),
    insert: async () => {
      throw new Error(`connect ${secret} failed`);
    },
  });
  rejected.write(sample("bad"), new Date());
  assert.equal(await rejected.flush(1_000), false);
  assert.deepEqual(logs, [
    { msg: "usage_dropped", sink: "postgres", reason: "sink_error", records: 1 },
    { msg: "usage_flush", sink: "postgres", outcome: "failed", records: 1 },
  ]);
  assert.equal(JSON.stringify(logs).includes(secret), false);

  const lateLogs = [];
  const closed = createBufferedUsageSink({
    capacity: 10,
    maxBatch: 500,
    flushIntervalMs: 60_000,
    metrics: createMetrics([]),
    onLog: (record) => lateLogs.push(record),
    insert: async () => undefined,
  });
  assert.equal(await closed.flush(1_000), true);
  closed.write(sample("late"), new Date());
  assert.deepEqual(lateLogs, [
    { msg: "usage_flush", sink: "postgres", outcome: "flushed", records: 0 },
    { msg: "usage_dropped", sink: "postgres", reason: "shutdown", dropped: 1 },
  ]);

  let release = () => {};
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  const hungLogs = [];
  const hung = createBufferedUsageSink({
    capacity: 10,
    maxBatch: 500,
    flushIntervalMs: 60_000,
    metrics: createMetrics([]),
    onLog: (record) => hungLogs.push(record),
    insert: async () => {
      await gate;
    },
  });
  hung.write(sample("held"), new Date());
  assert.equal(await hung.flush(30), false);
  assert.deepEqual(hungLogs, [{ msg: "usage_flush", sink: "postgres", outcome: "timeout", abandoned: 1 }]);
  release();
});

test("usage_sink_replaces_stdout_and_postgres_inserts_one_row", async () => {
  const metrics = createMetrics([]);
  const lines = [];
  const sinkLogs = [];
  const stdoutOnly = await openUsageDelivery({
    sinks: [],
    env: {},
    telemetry: null,
    metrics,
    onLog: (record) => {
      sinkLogs.push(record);
    },
    writeStdout: (line) => {
      lines.push(line);
    },
  });
  stdoutOnly.write(sample("req_stdout"));
  assert.match(lines[0], /"request_id":"req_stdout"/);
  assert.match(lines[0], /"input_tokens":4/);
  assert.equal(await stdoutOnly.flush(100), true);
  assert.deepEqual(sinkLogs, [{ msg: "usage_flush", sink: "stdout", outcome: "flushed", records: 0 }]);

  const posts = [];
  const stdout = [];
  const otlp = await openUsageDelivery({
      sinks: [
        {
          kind: "otlp",
          dsnEnv: null,
          table: "axond_usage",
          createTable: false,
          bufferCapacity: 10_000,
          maxBatch: 500,
          maxBatchExplicit: false,
          flushIntervalMs: 1_000,
        },
      ],
      env: {},
      telemetry: { endpoint: "http://127.0.0.1:9" },
      metrics,
      onLog: () => undefined,
      writeStdout: (line) => {
        stdout.push(line);
      },
      fetchImpl: async (url, init) => {
        posts.push({ url: String(url), body: JSON.parse(String(init?.body)) });
        return new Response(null, { status: 200 });
      },
    });
    otlp.write({ ...sample(), costMicrodollars: null, period: null });
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(stdout.some((line) => line.includes("req_usage")), false);
    assert.equal(posts[0]?.url, "http://127.0.0.1:9/v1/logs");
    const attributes = posts[0]!.body.resourceLogs[0]!.scopeLogs[0]!.logRecords[0]!.attributes;
    assert.equal(posts[0]!.body.resourceLogs[0]!.scopeLogs[0]!.logRecords[0]!.eventName, "axond.usage");
    assert.equal(attributes.some((item) => item.key === "axond.cost_microdollars"), false);
    assert.equal(attributes.some((item) => item.key === "axond.period"), false);
    await otlp.flush(100);

  await assert.rejects(
    () =>
      openUsageDelivery({
        sinks: [
          {
            kind: "otlp",
            dsnEnv: null,
            table: "axond_usage",
            createTable: false,
            bufferCapacity: 1,
            maxBatch: 1,
            maxBatchExplicit: false,
            flushIntervalMs: 1,
          },
        ],
        env: {},
        telemetry: null,
        metrics,
        onLog: () => undefined,
      }),
    /OTLP export is off/,
  );

  if (!dsn) {
    return;
  }
  const table = `usage_it_${Date.now()}`;
  const delivery = await openUsageDelivery({
    sinks: [
      {
        kind: "postgres",
        dsnEnv: "AXOND_TEST_POSTGRES",
        table,
        createTable: true,
        bufferCapacity: 10,
        maxBatch: 10,
        maxBatchExplicit: true,
        flushIntervalMs: 60_000,
      },
    ],
    env: { AXOND_TEST_POSTGRES: dsn },
    telemetry: null,
    metrics,
    onLog: () => undefined,
  });
  const record = { ...sample("req_pg"), costMicrodollars: null };
  delivery.write(record);
  await delivery.flush(5_000);
  const client = new pg.Client({ connectionString: dsn });
  await client.connect();
  try {
    const rows = await client.query(`SELECT request_id, input_tokens, cost_microdollars, period FROM ${table}`);
    assert.equal(rows.rowCount, 1);
    assert.equal(rows.rows[0]!.request_id, "req_pg");
    assert.equal(String(rows.rows[0]!.input_tokens), "4");
    assert.equal(rows.rows[0]!.cost_microdollars, null);
    assert.equal(rows.rows[0]!.period, "2026-09");
    await client.query(`DROP TABLE ${table}`);
  } finally {
    await client.end();
  }

  const stale = `usage_stale_${Date.now()}`;
  const setup = new pg.Client({ connectionString: dsn });
  await setup.connect();
  await setup.query(`CREATE TABLE ${stale} (id bigserial PRIMARY KEY, cost_microdollars bigint NOT NULL)`);
  await setup.end();
  await assert.rejects(
    () =>
      openUsageDelivery({
        sinks: [
          {
            kind: "postgres",
            dsnEnv: "AXOND_TEST_POSTGRES",
            table: stale,
            createTable: false,
            bufferCapacity: 10,
            maxBatch: 10,
            maxBatchExplicit: true,
            flushIntervalMs: 1_000,
          },
        ],
        env: { AXOND_TEST_POSTGRES: dsn },
        telemetry: null,
        metrics,
        onLog: () => undefined,
      }),
    /usage table is missing column/,
  );
  const cleanup = new pg.Client({ connectionString: dsn });
  await cleanup.connect();
  await cleanup.query(`DROP TABLE ${stale}`);
  await cleanup.end();
});

test("loadConfig still accepts a postgres sink without opening it", async () => {
  const loaded = await loadConfig(
    `
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "/tmp/axond.sqlite"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
[[usage_sink]]
kind = "postgres"
dsn_env = "AXOND_TEST_POSTGRES"
`,
    envSecretReader({ GW_KEY: "k" }, async () => ""),
  );
  assert.equal(loaded.usageSinks[0]!.kind, "postgres");
  await assert.rejects(
    () =>
      openUsageDelivery({
        sinks: loaded.usageSinks,
        env: {},
        telemetry: null,
        metrics: createMetrics([]),
        onLog: () => undefined,
      }),
    /AXOND_TEST_POSTGRES` is unset or empty/,
  );
});

test("a postgres usage sink uses the statement limit", async () => {
  const source = await readFile(new URL("./usage-delivery.ts", import.meta.url), "utf8");
  assert.match(source, /postgresClientOptions\(dsn\)/);
  assert.match(source, /application_name: "axond"/);
  assert.match(source, /queryPgClient\(/);
  assert.match(source, /closePgClient\(/);
  assert.match(source, /POSTGRES_TRANSACTION_LIMITS/);
  assert.match(source, /POSTGRES_LOCAL_LIMITS/);
  assert.equal(source.includes('queryPgClient(client, "BEGIN")'), false);
  assert.equal(source.includes("client.query("), false);
  assert.equal(source.includes("client.end("), false);
});

test("a usage insert blocked on the table ends before the query limit", { skip: !dsn }, async () => {
  const table = `usage_lock_${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;
  const logs: unknown[] = [];
  const delivery = await openUsageDelivery({
    sinks: [
      {
        kind: "postgres",
        dsnEnv: "AXOND_TEST_POSTGRES",
        table,
        createTable: true,
        bufferCapacity: 10,
        maxBatch: 10,
        maxBatchExplicit: true,
        flushIntervalMs: 60_000,
      },
    ],
    env: { AXOND_TEST_POSTGRES: dsn },
    telemetry: null,
    metrics: createMetrics([]),
    onLog: (record) => logs.push(record),
  });
  const holder = new pg.Client({ connectionString: dsn });
  await holder.connect();
  try {
    await holder.query("BEGIN");
    await holder.query(`LOCK TABLE ${table} IN ACCESS EXCLUSIVE MODE`);
    delivery.write(sample("req_lock"));
    const started = Date.now();
    const flushed = await delivery.flush(12_000);
    const elapsed = Date.now() - started;
    assert.equal(flushed, false);
    assert.ok(elapsed < 8_000, `usage insert still open after ${elapsed}ms`);
    assert.equal(JSON.stringify(logs).includes("lock timeout"), false);
  } finally {
    await holder.query("ROLLBACK").catch(() => undefined);
    await holder.end().catch(() => undefined);
    const drop = new pg.Client({ connectionString: dsn });
    await drop.connect();
    await drop.query(
      "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND query LIKE $1",
      [`%${table}%`],
    );
    await drop.query(`DROP TABLE IF EXISTS ${table}`);
    await drop.end();
  }
});

test("a usage schema apply blocked on the table ends before the query limit", { skip: !dsn }, async () => {
  const table = `usage_ddl_${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;
  const setup = new pg.Client({ connectionString: dsn });
  await setup.connect();
  await setup.query(usageSchemaDdl(table));
  await setup.query("BEGIN");
  await setup.query(`LOCK TABLE ${table} IN ACCESS SHARE MODE`);
  const started = Date.now();
  const opening = openUsageDelivery({
    sinks: [
      {
        kind: "postgres",
        dsnEnv: "AXOND_TEST_POSTGRES",
        table,
        createTable: true,
        bufferCapacity: 10,
        maxBatch: 10,
        maxBatchExplicit: true,
        flushIntervalMs: 60_000,
      },
    ],
    env: { AXOND_TEST_POSTGRES: dsn },
    telemetry: null,
    metrics: createMetrics([]),
    onLog: () => undefined,
  });
  const result = await Promise.race([
    opening.then(
      () => "ok",
      (error: unknown) => error,
    ),
    new Promise<string>((resolve) => setTimeout(() => resolve("pending"), 8_000)),
  ]);
  const elapsed = Date.now() - started;
  try {
    assert.ok(result instanceof Error, `usage schema still open after ${elapsed}ms`);
    assert.equal((result as { code?: string }).code, "55P03");
    assert.ok(elapsed < 8_000);
  } finally {
    await setup.query("ROLLBACK").catch(() => undefined);
    await setup.end().catch(() => undefined);
    await opening.catch(() => undefined);
    const drop = new pg.Client({ connectionString: dsn });
    await drop.connect();
    await drop.query(
      "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND query LIKE $1",
      [`%${table}%`],
    );
    await drop.query(`DROP TABLE IF EXISTS ${table}`);
    await drop.end();
  }
});


test("scheduled batches still consume buffer capacity and failures make flush fail", async () => {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  let inserts = 0;
  const sink = createBufferedUsageSink({ capacity: 1, maxBatch: 1, flushIntervalMs: 1000,
    metrics: createMetrics(), onLog() {}, insert: async () => { inserts++; await gate; throw new Error("database unavailable"); } });
  for (let i = 0; i < 100; i++) sink.write(sample(String(i)), new Date());
  assert.equal(sink.dropped, 99);
  release();
  assert.equal(await sink.flush(1000), false);
  assert.equal(inserts, 1);
});

test("database close shares the flush deadline", async () => {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const sink = createBufferedUsageSink({ capacity: 1, maxBatch: 1, flushIntervalMs: 1000,
    metrics: createMetrics(), onLog() {}, insert: async () => {}, close: () => gate });
  const start = Date.now();
  assert.equal(await sink.flush(30), false);
  assert.ok(Date.now() - start < 500);
  release();
});
