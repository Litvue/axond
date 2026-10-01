import assert from "node:assert/strict";
import { createServer } from "node:http";
import { describe, test } from "node:test";
import pg from "pg";
import { unstable_dev } from "wrangler";

const dsn = process.env.AXOND_TEST_POSTGRES;
const key = "test-inbound-key";

describe("workerd hyperdrive", { concurrency: 1 }, () => {
test("workerd serves the gateway through a local Hyperdrive binding", { skip: !dsn }, async () => {
  process.env.CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE = dsn;
  const worker = await unstable_dev(new URL("./index.ts", import.meta.url).pathname, {
    config: new URL("../wrangler.toml", import.meta.url).pathname,
    local: true,
    ip: "127.0.0.1",
    vars: { GATEWAY_KEY: key, PROVIDERS_JSON: "[]" },
    logLevel: "error",
    experimental: { disableExperimentalWarning: true, disableDevRegistry: true },
  });
  const id = `workerd-${Date.now()}`;
  try {
    const health = await worker.fetch("http://127.0.0.1/healthz");
    assert.equal(health.status, 200);
    assert.equal(await health.text(), "ok");
    const created = await worker.fetch("http://127.0.0.1/api/v1/namespaces", {
      method: "POST",
      headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
      body: JSON.stringify({ id }),
    });
    const createdBody = await created.text();
    assert.equal(created.status, 201, createdBody);
    const listed = await worker.fetch("http://127.0.0.1/api/v1/namespaces", {
      headers: { authorization: `Bearer ${key}` },
    });
    const listedBody = await listed.text();
    assert.equal(listed.status, 200, listedBody);
    const body = JSON.parse(listedBody) as { data: { id: string }[] };
    assert.equal(body.data.some((row) => row.id === id), true);
  } finally {
    await worker.stop();
  }
});

test("workerd_hyperdrive_charges_one_request_id_once", { skip: !dsn }, async () => {
  const upstream = await new Promise<{ url: string; close: () => Promise<void> }>((resolve) => {
    const server = createServer((req, res) => {
      req.resume();
      req.on("end", () => {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(
          '{"id":"chatcmpl-workerd","choices":[{"message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}',
        );
      });
    });
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        throw new Error("no port");
      }
      resolve({
        url: `http://127.0.0.1:${address.port}`,
        close: () =>
          new Promise((done) => {
            server.closeAllConnections();
            server.close(() => done());
          }),
      });
    });
  });
  process.env.CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE = dsn;
  const id = `wd${Date.now()}`;
  const worker = await unstable_dev(new URL("./index.ts", import.meta.url).pathname, {
    config: new URL("../wrangler.toml", import.meta.url).pathname,
    local: true,
    ip: "127.0.0.1",
    vars: {
      GATEWAY_KEY: key,
      PROVIDERS_JSON: JSON.stringify([
        { id: "fake-openai", kind: "openai", baseUrl: upstream.url, unpricedModels: "allow" },
      ]),
      CREDENTIALS_JSON: JSON.stringify([
        { namespace: id, provider: "fake-openai", secret: "sk-workerd-secret", id: "plat" },
      ]),
      PRICES_JSON: JSON.stringify([
        {
          provider: "fake-openai",
          model: "*",
          inputMicrodollarsPerMillion: 1_000_000,
          outputMicrodollarsPerMillion: 1_000_000,
        },
      ]),
    },
    logLevel: "error",
    experimental: { disableExperimentalWarning: true, disableDevRegistry: true },
  });
  const headers = { authorization: `Bearer ${key}`, "content-type": "application/json" };
  try {
    const created = await worker.fetch("http://127.0.0.1/api/v1/namespaces", {
      method: "POST",
      headers,
      body: JSON.stringify({ id }),
    });
    assert.equal(created.status, 201, await created.text());
    const budget = await worker.fetch(`http://127.0.0.1/api/v1/namespaces/${id}/budgets/compat`, {
      method: "PUT",
      headers,
      body: JSON.stringify({ limit_microdollars: 1_000_000 }),
    });
    assert.equal(budget.status, 200, await budget.text());
    const chat = () =>
      worker.fetch(`http://127.0.0.1/ns/${id}/v1/chat/completions`, {
        method: "POST",
        headers: { ...headers, "x-request-id": `workerd-once-${id}` },
        body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: "hi" }] }),
      });
    const first = await chat();
    assert.equal(first.status, 200, await first.text());
    const second = await chat();
    assert.equal(second.status, 200, await second.text());
    let spent = -1;
    for (let attempt = 0; attempt < 20; attempt += 1) {
      const row = await worker.fetch(`http://127.0.0.1/api/v1/namespaces/${id}/budgets/compat`, {
        headers: { authorization: `Bearer ${key}` },
      });
      const rowBody = await row.text();
      assert.equal(row.status, 200, rowBody);
      spent = (JSON.parse(rowBody) as { spent_microdollars: number }).spent_microdollars;
      if (spent === 2) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    assert.equal(spent, 2);
    const usage = await worker.fetch(`http://127.0.0.1/api/v1/namespaces/${id}/usage?period=compat`, {
      headers: { authorization: `Bearer ${key}` },
    });
    const usageBody = await usage.text();
    assert.equal(usage.status, 200, usageBody);
    const summary = JSON.parse(usageBody) as { data: { count: number; cost_microdollars: number }[] };
    assert.equal(summary.data.length, 1);
    assert.equal(summary.data[0]?.count, 1);
    assert.equal(summary.data[0]?.cost_microdollars, 2);
    assert.equal(usageBody.includes("sk-workerd-secret"), false);
  } finally {
    await worker.stop();
    await upstream.close();
  }
});

test("workerd_hyperdrive_logs_missing_namespace_columns", { skip: !dsn }, async (t) => {
  const admin = new pg.Client({ connectionString: dsn });
  await admin.connect();
  const role = await admin.query("SELECT rolsuper FROM pg_roles WHERE rolname = current_user");
  if (role.rows[0]?.["rolsuper"] !== true) {
    await admin.end();
    t.skip("the test role cannot create a database");
    return;
  }
  const suffix = crypto.randomUUID().replaceAll("-", "").slice(0, 12);
  const dbName = `axond_wd_${suffix}`;
  const roleName = `axond_wdw_${suffix}`;
  const password = `pw_${suffix}`;
  await admin.query(`CREATE DATABASE ${dbName}`);
  await admin.query(`CREATE ROLE ${roleName} LOGIN PASSWORD '${password}' NOSUPERUSER NOCREATEDB NOCREATEROLE`);
  await admin.end();
  const ownerUrl = new URL(dsn!);
  ownerUrl.pathname = `/${dbName}`;
  const owner = new pg.Client({ connectionString: ownerUrl.toString() });
  await owner.connect();
  const restrictedUrl = new URL(ownerUrl.toString());
  restrictedUrl.username = roleName;
  restrictedUrl.password = password;
  const lines: string[] = [];
  const write = process.stdout.write;
  const writeError = process.stderr.write;
  process.stdout.write = ((chunk: string | Uint8Array, encoding?: BufferEncoding, callback?: (error?: Error | null) => void) => {
    lines.push(String(chunk));
    return write.call(process.stdout, chunk, encoding, callback);
  }) as typeof process.stdout.write;
  process.stderr.write = ((chunk: string | Uint8Array, encoding?: BufferEncoding, callback?: (error?: Error | null) => void) => {
    lines.push(String(chunk));
    return writeError.call(process.stderr, chunk, encoding, callback);
  }) as typeof process.stderr.write;
  let worker: Awaited<ReturnType<typeof unstable_dev>> | undefined;
  try {
    await owner.query("REVOKE CREATE ON SCHEMA public FROM PUBLIC");
    await owner.query(`GRANT USAGE ON SCHEMA public TO ${roleName}`);
    await owner.query(`GRANT pg_read_all_data, pg_write_all_data TO ${roleName}`);
    await owner.query(`
      CREATE TABLE axond_namespace (
        id TEXT PRIMARY KEY NOT NULL,
        attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
        blocklist JSONB
      )
    `);
    const { applyPostgresSchema } = await import("../../cli/src/postgres-store.ts");
    await applyPostgresSchema({
      query: async (sql, params) => {
        const result = params === undefined ? await owner.query(sql) : await owner.query(sql, [...params]);
        const row = Array.isArray(result) ? result[result.length - 1] : result;
        return { rows: (row?.rows ?? []) as Record<string, unknown>[], rowCount: row?.rowCount ?? null };
      },
    });
    await owner.query("ALTER TABLE axond_namespace DROP COLUMN allow_platform_fallback");
    await owner.query("ALTER TABLE axond_namespace DROP COLUMN from_config");
    await owner.end();
    process.env.CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE = restrictedUrl.toString();
    worker = await unstable_dev(new URL("./index.ts", import.meta.url).pathname, {
      config: new URL("../wrangler.toml", import.meta.url).pathname,
      local: true,
      ip: "127.0.0.1",
      vars: { GATEWAY_KEY: key, PROVIDERS_JSON: "[]" },
      logLevel: "log",
      experimental: { disableExperimentalWarning: true, disableDevRegistry: true },
    });
    const response = await worker.fetch("http://127.0.0.1/api/v1/namespaces", {
      headers: { authorization: `Bearer ${key}` },
    });
    const body = await response.text();
    assert.equal(response.status, 503, body);
    assert.equal(body.includes("store is unavailable"), true);
    assert.equal(body.includes("allow_platform_fallback"), false);
    assert.equal(body.includes(password), false);
    const logs = lines.join("");
    assert.equal(logs.includes("schema_unavailable"), true, logs);
    assert.equal(logs.includes("axond_namespace.allow_platform_fallback"), true, logs);
    assert.equal(logs.includes("axond_namespace.from_config"), true, logs);
    assert.equal(logs.includes(password), false);
  } finally {
    process.stdout.write = write;
    process.stderr.write = writeError;
    await worker?.stop();
    await owner.end().catch(() => undefined);
    const drop = new pg.Client({ connectionString: dsn });
    await drop.connect();
    await drop.query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()", [dbName]);
    await drop.query(`DROP DATABASE IF EXISTS ${dbName}`);
    await drop.query(`REVOKE pg_read_all_data, pg_write_all_data FROM ${roleName}`).catch(() => undefined);
    await drop.query(`DROP ROLE IF EXISTS ${roleName}`);
    await drop.end();
  }
});

test("workerd_hyperdrive_serves_a_role_that_cannot_create", { skip: !dsn }, async (t) => {
  const admin = new pg.Client({ connectionString: dsn });
  await admin.connect();
  const role = await admin.query("SELECT rolsuper FROM pg_roles WHERE rolname = current_user");
  if (role.rows[0]?.["rolsuper"] !== true) {
    await admin.end();
    t.skip("the test role cannot create a database");
    return;
  }
  const suffix = crypto.randomUUID().replaceAll("-", "").slice(0, 12);
  const dbName = `axond_wrok_${suffix}`;
  const roleName = `axond_wrokr_${suffix}`;
  const password = `pw_${suffix}`;
  const lines: string[] = [];
  const write = process.stdout.write;
  const writeError = process.stderr.write;
  process.stdout.write = ((chunk: string | Uint8Array, encoding?: BufferEncoding, callback?: (error?: Error | null) => void) => {
    lines.push(String(chunk));
    return write.call(process.stdout, chunk, encoding, callback);
  }) as typeof process.stdout.write;
  process.stderr.write = ((chunk: string | Uint8Array, encoding?: BufferEncoding, callback?: (error?: Error | null) => void) => {
    lines.push(String(chunk));
    return writeError.call(process.stderr, chunk, encoding, callback);
  }) as typeof process.stderr.write;
  let worker: Awaited<ReturnType<typeof unstable_dev>> | undefined;
  let owner: pg.Client | undefined;
  try {
    await admin.query(`CREATE DATABASE ${dbName}`);
    await admin.query(`CREATE ROLE ${roleName} LOGIN PASSWORD '${password}' NOSUPERUSER NOCREATEDB NOCREATEROLE`);
    const ownerUrl = new URL(dsn!);
    ownerUrl.pathname = `/${dbName}`;
    owner = new pg.Client({ connectionString: ownerUrl.toString() });
    await owner.connect();
    await owner.query("REVOKE CREATE ON SCHEMA public FROM PUBLIC");
    await owner.query(`GRANT USAGE ON SCHEMA public TO ${roleName}`);
    await owner.query(`GRANT pg_read_all_data, pg_write_all_data TO ${roleName}`);
    const { applyPostgresSchema } = await import("../../cli/src/postgres-store.ts");
    await applyPostgresSchema({
      query: async (sql, params) => {
        const result = params === undefined ? await owner!.query(sql) : await owner!.query(sql, [...params]);
        const row = Array.isArray(result) ? result[result.length - 1] : result;
        return { rows: (row?.rows ?? []) as Record<string, unknown>[], rowCount: row?.rowCount ?? null };
      },
    });
    await owner.end();
    owner = undefined;
    const restrictedUrl = new URL(ownerUrl.toString());
    restrictedUrl.username = roleName;
    restrictedUrl.password = password;
    process.env.CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE = restrictedUrl.toString();
    worker = await unstable_dev(new URL("./index.ts", import.meta.url).pathname, {
      config: new URL("../wrangler.toml", import.meta.url).pathname,
      local: true,
      ip: "127.0.0.1",
      vars: { GATEWAY_KEY: key, PROVIDERS_JSON: "[]" },
      logLevel: "log",
      experimental: { disableExperimentalWarning: true, disableDevRegistry: true },
    });
    const headers = { authorization: `Bearer ${key}`, "content-type": "application/json" };
    const id = `ps${suffix}`;
    const created = await worker.fetch("http://127.0.0.1/api/v1/namespaces", {
      method: "POST",
      headers,
      body: JSON.stringify({ id }),
    });
    const createdBody = await created.text();
    assert.equal(created.status, 201, createdBody);
    assert.equal(createdBody.includes(password), false);
    const listed = await worker.fetch("http://127.0.0.1/api/v1/namespaces", {
      headers: { authorization: `Bearer ${key}` },
    });
    const listedBody = await listed.text();
    assert.equal(listed.status, 200, listedBody);
    const body = JSON.parse(listedBody) as { data: { id: string }[] };
    assert.equal(body.data.some((row) => row.id === id), true);
    assert.equal(listedBody.includes(password), false);
    const logs = lines.join("");
    assert.equal(logs.includes(password), false);
    assert.equal(logs.includes("schema_unavailable"), false, logs);
  } finally {
    process.stdout.write = write;
    process.stderr.write = writeError;
    await worker?.stop();
    await owner?.end().catch(() => undefined);
    await admin.end().catch(() => undefined);
    const drop = new pg.Client({ connectionString: dsn });
    await drop.connect();
    await drop.query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()", [dbName]);
    await drop.query(`DROP DATABASE IF EXISTS ${dbName}`);
    await drop.query(`REVOKE pg_read_all_data, pg_write_all_data FROM ${roleName}`).catch(() => undefined);
    await drop.query(`DROP ROLE IF EXISTS ${roleName}`);
    await drop.end();
  }
});
});
