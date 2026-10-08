/** Qualify runtime handoff on a fresh, disposable local PostgreSQL database.
 * AXOND_CONFORMANCE_DSN must select an empty database named axond_conformance_*.
 * node --experimental-strip-types scripts/postgres-rollback.ts --rust PATH --ts PATH --report PATH
 */
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import pg from "pg";
import { boot, call, putBudgets, stop, upstream } from "./shadow-compare.ts";

function argument(name: string): string {
  const i = process.argv.indexOf(name);
  assert(i >= 0 && process.argv[i + 1], `${name} is required`);
  return resolve(process.argv[i + 1]!);
}
const rust = argument("--rust"), ts = argument("--ts"), reportPath = argument("--report");
const dsn = process.env["AXOND_CONFORMANCE_DSN"];
assert(dsn, "AXOND_CONFORMANCE_DSN is required");
const address = new URL(dsn);
assert(["127.0.0.1", "localhost"].includes(address.hostname) && /^\/axond_conformance_[a-z0-9_]+$/.test(address.pathname), "use an explicitly disposable local conformance database");
const client = new pg.Client({ connectionString: dsn });
await client.connect();
const tables = await client.query("SELECT tablename FROM pg_tables WHERE schemaname = 'public'");
assert.equal(tables.rowCount, 0, "database must be empty; this script never resets existing data");
const fake = await upstream();
const root = await mkdtemp(join(tmpdir(), "axond-pg-rollback-"));
const results: Array<Record<string, unknown>> = [];
const report: Record<string, unknown> = { started_at: new Date().toISOString(), results, limitations: ["Local PostgreSQL 16 and a synthetic least-privilege application role; production role grants still require staging qualification"] };
const gates: Array<Awaited<ReturnType<typeof boot>>> = [];
const role = `axond_conformance_role_${Date.now()}`;
let roleCreated = false;
function customize(text: string): string {
  return text.replace('backend = "sqlite"', 'backend = "postgres"\ndsn_env = "AXOND_CONFORMANCE_DSN"\ncreate_table = true').replace(/^path = .*\n/m, "");
}
async function state() {
  const budgets = (await client.query("SELECT namespace, period, limit_microdollars::text, spent_microdollars::text FROM axond_store_budget ORDER BY namespace, period")).rows;
  const usage = (await client.query("SELECT request_id, namespace, period, model, status, cost_microdollars::text FROM axond_store_usage ORDER BY request_id")).rows;
  const namespaces = (await client.query("SELECT id, attrs FROM axond_namespace ORDER BY id")).rows;
  return JSON.stringify({ budgets, usage, namespaces });
}
async function settled(count: number) {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    const rows = (await client.query("SELECT count(*)::int AS n FROM axond_store_usage")).rows;
    if (rows[0].n === count) return;
    await new Promise((wake) => setTimeout(wake, 25));
  }
  throw new Error("Postgres settlement did not reach the expected row count");
}
try {
  let previous: string | undefined;
  let count = 0;
  for (const [program, name] of [[rust, "released-rust"], [ts, "typescript"], [rust, "rollback-rust"]] as const) {
    const gate = await boot(program, root, fake.url, customize); gates.push(gate);
    if (previous) results.push({ name: `${name}-preserves-existing-state`, pass: await state() === previous });
    else {
      await putBudgets(gate.base);
      const created = await call(gate.base, { name: "persist", method: "POST", path: "/api/v1/namespaces", body: '{"id":"persist","attrs":{"owner":"rollback"}}', expectStatus: 201 }, "persist");
      assert.equal(created.status, 201, "create durable API state before runtime handoff");
    }
    const response = await call(gate.base, { name, method: "POST", path: "/ns/platform/v1/chat/completions", body: '{"model":"fake-openai/fixture-chat","messages":[]}', expectStatus: 200 }, name);
    assert.equal(response.status, 200, `${name} must serve an inference request`);
    await settled(++count);
    await stop(gate.child);
    previous = await state();
    const snapshot = JSON.parse(previous);
    assert.equal(snapshot.usage.length, count);
    assert(snapshot.usage.every((row: Record<string, unknown>) => row.status === "ok" && row.cost_microdollars === "117"));
    assert.equal(snapshot.budgets.find((row: Record<string, unknown>) => row.namespace === "platform").spent_microdollars, String(117 * count));
    results.push({ name: `${name}-settles-on-shared-database`, pass: true, usage_rows: count, spent_microdollars: String(117 * count) });
  }
  // Apply schema as owner, then run without DDL privileges and with create_table=false.
  await client.query(`CREATE ROLE ${role} LOGIN PASSWORD 'local-conformance-only' NOSUPERUSER NOCREATEDB NOCREATEROLE`); roleCreated = true;
  await client.query(`GRANT CONNECT ON DATABASE ${address.pathname.slice(1)} TO ${role}`);
  await client.query(`GRANT USAGE ON SCHEMA public TO ${role}`);
  await client.query(`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO ${role}`);
  const restricted = new URL(dsn); restricted.username = role; restricted.password = "local-conformance-only";
  process.env["AXOND_CONFORMANCE_DSN"] = restricted.href;
  for (const [program, name] of [[ts, "restricted-typescript"], [rust, "restricted-rust-rollback"]] as const) {
    const gate = await boot(program, root, fake.url, (text) => customize(text).replace("create_table = true", "create_table = false")); gates.push(gate);
    results.push({ name: `${name}-preserves-existing-state`, pass: await state() === previous });
    const response = await call(gate.base, { name, method: "POST", path: "/ns/platform/v1/chat/completions", body: '{"model":"fake-openai/fixture-chat","messages":[]}', expectStatus: 200 }, name);
    assert.equal(response.status, 200, `${name} must serve without DDL privileges`);
    await settled(++count); await stop(gate.child); previous = await state();
    const snapshot = JSON.parse(previous);
    assert.equal(snapshot.budgets.find((row: Record<string, unknown>) => row.namespace === "platform").spent_microdollars, String(117 * count));
    results.push({ name: `${name}-settles-without-ddl`, pass: true, usage_rows: count });
  }
  report.final_state = JSON.parse(previous!);
} catch (error) { report.error = error instanceof Error ? error.message : String(error); }
finally {
  const cleanup = await Promise.allSettled(gates.map((gate) => stop(gate.child)));
  if (cleanup.some((r) => r.status === "rejected")) report.cleanup_error = "gateway cleanup failed";
  process.env["AXOND_CONFORMANCE_DSN"] = dsn;
  if (roleCreated) { await client.query(`DROP OWNED BY ${role}`); await client.query(`DROP ROLE ${role}`); }
  await client.end(); fake.close(); await rm(root, { recursive: true, force: true });
  report.completed_at = new Date().toISOString();
  report.pass = !report.error && !report.cleanup_error && results.length === 9 && results.every((result) => result.pass === true);
  await writeFile(reportPath, JSON.stringify(report, null, 2) + "\n");
}
process.stdout.write(`${report.pass ? "passed" : "FAILED"} PostgreSQL runtime handoff and restricted-role rollback\n`);
if (!report.pass) process.exitCode = 1;
