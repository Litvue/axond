/** Compiled-binary fault, recovery, and same-database rollback qualification.
 * node --experimental-strip-types scripts/release-conformance.ts
 *   --rust /path/to/released/axond --ts /path/to/compiled/axond --report /path/to/report.json
 * Uses placeholder keys, local deterministic upstreams, and disposable SQLite files.
 */
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { once } from "node:events";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { AddressInfo } from "node:net";
import { boot, call, chargeRows, durableState, putBudgets, stop } from "./shadow-compare.ts";
import { samePayload } from "./parity.ts";

function argument(name: string): string {
  const index = process.argv.indexOf(name);
  assert(index >= 0 && process.argv[index + 1], `${name} is required`);
  return resolve(process.argv[index + 1]!);
}
const rust = argument("--rust"), ts = argument("--ts"), reportPath = argument("--report");
const KEY = "test-inbound-key";
const root = await mkdtemp(join(tmpdir(), "axond-release-conformance-"));
const fixtures = resolve(new URL("../../tests/fixtures/", import.meta.url).pathname);
const chat = await readFile(join(fixtures, "openai/chat_completion.json"));
const chatStream = await readFile(join(fixtures, "openai/chat_completion.sse"));
const results: Array<Record<string, unknown>> = [];
const report: Record<string, unknown> = {
  started_at: new Date().toISOString(),
  binaries: await Promise.all([rust, ts].map(async (path) => ({ path, sha256: createHash("sha256").update(await readFile(path)).digest("hex") }))),
  results,
  limits: ["SQLite only; PostgreSQL roles are qualified separately", "Crash test kills after confirmed durable settlement; no zero-loss claim for unacknowledged events", "Fixture comparison does not authorize production rollout"],
};
const gates: Array<Awaited<ReturnType<typeof boot>>> = [];
let rotationAttempts: string[] = [];
const upstream = createServer((req, res) => {
  const chunks: Buffer[] = [];
  req.on("data", (chunk: Buffer) => chunks.push(chunk));
  req.on("end", () => {
    if (req.method === "GET") { res.writeHead(200, { "content-type": "application/json" }); res.end('{"data":[]}'); return; }
    const { model, stream } = JSON.parse(Buffer.concat(chunks).toString());
    const timers: ReturnType<typeof setTimeout>[] = [];
    res.on("close", () => timers.forEach(clearTimeout));
    if (/^error-/.test(model)) {
      res.writeHead(Number(model.slice(6)), { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { type: "fixture_error", message: "deterministic provider failure" } }));
    } else if (model === "header-stall") {
      timers.push(setTimeout(() => res.end(chat), 2_000));
    } else if (model === "body-stall") {
      res.writeHead(200, { "content-type": "application/json" }); res.write("{");
    } else if (model === "idle-stream" || model === "cancel-stream") {
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.write('data: {"choices":[{"index":0,"delta":{"content":"start"},"finish_reason":null}]}\n\n');
    } else if (model === "truncated-native") {
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.end('event: response.created\ndata: {"type":"response.created","response":{"id":"fixture","status":"in_progress"}}\n\n');
    } else if (model === "rotate" && rotationAttempts.push(req.headers.authorization === "Bearer upstream-openai" ? "first" : "second") === 1) {
      res.writeHead(429, { "content-type": "application/json" });
      res.end('{"error":{"type":"rate_limit_error","message":"fixture rate limit"}}');
    } else if (model === "slow-consumer") {
      res.writeHead(200, { "content-type": "text/event-stream" });
      // Enough data to create downstream pressure; each upstream interval stays below the idle bound.
      let index = 0;
      const next = () => {
        if (res.destroyed) return;
        if (index++ === 32) { res.end(chatStream); return; }
        res.write(`data: ${JSON.stringify({ choices: [{ index: 0, delta: { content: "x".repeat(65536) }, finish_reason: null }] })}\n\n`);
        timers.push(setTimeout(next, 10));
      };
      next();
    } else {
      res.writeHead(200, { "content-type": stream ? "text/event-stream" : "application/json" });
      res.end(stream ? chatStream : chat);
    }
  });
});
await new Promise<void>((ready) => upstream.listen(0, "127.0.0.1", ready));
const url = `http://127.0.0.1:${(upstream.address() as AddressInfo).port}`;
function customize(text: string): string {
  return text.replace("[shutdown]", `[transport]\nresponse_header_timeout_ms = 300\nbuffered_body_timeout_ms = 300\nstream_idle_timeout_ms = 1000\n\n[credential_pool]\nfailure_threshold = 2\ncooldown_seconds = 1\n\n[shutdown]`)
    .replace("[[gateway_key]]", `[[credential]]\nnamespace = "platform"\nprovider = "fake-openai"\nenv = "GW_FAKE_ANTHROPIC_KEY"\nid = "second"\n\n[[gateway_key]]`);
}
async function launch(program: string, name: string) {
  const directory = join(root, name);
  await mkdir(directory, { recursive: true });
  const gate = await boot(program, directory, url, customize);
  gates.push(gate);
  return gate;
}
async function settled(sqlite: string, count: number) {
  const deadline = Date.now() + 5_000;
  while (chargeRows(sqlite).length < count && Date.now() < deadline) await new Promise((wake) => setTimeout(wake, 25));
  assert.equal(chargeRows(sqlite).length, count, "usage must settle exactly once per request");
}
function compare(name: string, left: Awaited<ReturnType<typeof call>>, right: Awaited<ReturnType<typeof call>>, expectedStatus?: number) {
  const pass = left.status === right.status && (expectedStatus === undefined || left.status === expectedStatus)
    && left.headers === right.headers && samePayload(left.body, right.body);
  results.push({ name, pass, expected_status: expectedStatus,
    rust: { status: left.status, headers: left.headers, body: left.body.toString() },
    typescript: { status: right.status, headers: right.headers, body: right.body.toString() } });
  process.stdout.write(`${pass ? "match" : "DIFFERENCE"} ${name} rust=${left.status} ts=${right.status}\n`);
}
try {
  const left = await launch(rust, "rust"), right = await launch(ts, "ts");
  await putBudgets(left.base); await putBudgets(right.base);
  let count = 0;
  for (const [model, stream, expected] of [
    ["error-400", false, 400], ["error-401", false, 502], ["error-403", false, 502],
    ["error-404", false, 502], ["error-429", false, 502], ["error-500", false, 502],
    ["header-stall", false, 504], ["body-stall", false, 504], ["idle-stream", true, 200],
    ["truncated-native", true, 200], ["rotate", false, 200],
  ] as const) {
    const item = { name: model, method: "POST", path: model === "truncated-native" ? "/ns/platform/v1/responses" : "/ns/platform/v1/chat/completions",
      body: JSON.stringify({ model: `fake-openai/${model}`, messages: [{ role: "user", content: "fixture" }], stream }), expectStatus: expected };
    const responses = [];
    const attempts = [];
    for (const gate of [left, right]) {
      rotationAttempts = [];
      responses.push(await call(gate.base, item, model));
      if (model === "rotate") {
        assert.equal(rotationAttempts.length, 2, "a provider 429 must cause exactly one alternate credential attempt");
        assert.notEqual(rotationAttempts[0], rotationAttempts[1], "rotation must change credentials");
        attempts.push([...rotationAttempts]);
      }
    }
    compare(model, responses[0]!, responses[1]!, expected);
    if (model === "rotate") results.push({ name: "credential-rotation-attempts", pass: true, attempts });
    count++;
    await settled(left.sqlite, count); await settled(right.sqlite, count);
  }
  const item = { name: "concurrent", method: "POST", path: "/ns/platform/v1/chat/completions",
    body: JSON.stringify({ model: "fake-openai/concurrent", messages: [{ role: "user", content: "fixture" }] }), expectStatus: 200 };
  const [a, b] = await Promise.all([left, right].map((gate) => Promise.all(Array.from({ length: 16 }, (_, i) => call(gate.base, item, `concurrent-${i}`)))));
  for (let i = 0; i < a!.length; i++) compare(`concurrent-${i}`, a![i]!, b![i]!, 200);
  count += 16;
  await settled(left.sqlite, count); await settled(right.sqlite, count);
  for (const model of ["cancel-stream", "slow-consumer"]) {
    const observations = [];
    const payloads: Buffer[] = [];
    for (const gate of [left, right]) {
      const controller = new AbortController();
      const response = await fetch(`${gate.base}/ns/platform/v1/chat/completions`, { method: "POST", headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json", "x-request-id": model },
        body: JSON.stringify({ model: `fake-openai/${model}`, stream: true, messages: [] }), signal: controller.signal });
      assert.equal(response.status, 200);
      const reader = response.body!.getReader();
      const first = await reader.read();
      assert(first.value?.length);
      if (model === "cancel-stream") { controller.abort(); await reader.cancel().catch(() => {}); observations.push("cancelled after content"); }
      else {
        await new Promise((wake) => setTimeout(wake, 1_500));
        const data = [Buffer.from(first.value!)];
        while (true) { const part = await reader.read(); if (part.done) break; data.push(Buffer.from(part.value)); }
        const bytes = Buffer.concat(data);
        assert(bytes.includes(Buffer.from("[DONE]")), "slow consumer must receive the terminal event");
        payloads.push(bytes);
        observations.push(createHash("sha256").update(bytes).digest("hex"));
      }
    }
    const pass = model === "slow-consumer" ? samePayload(payloads[0]!, payloads[1]!) : observations[0] === observations[1];
    results.push({ name: model, pass, observations, comparison: model === "slow-consumer" ? "SSE event order and payloads, allowing JSON key order and whitespace" : "cancelled after observed content; settlement compared below" });
    count++;
    await settled(left.sqlite, count); await settled(right.sqlite, count);
  }
  const before = [left, right].map((gate) => durableState(gate.sqlite));
  results.push({ name: "fault-usage-and-budget", pass: before[0] === before[1], rust: JSON.parse(before[0]!), typescript: JSON.parse(before[1]!) });
  for (const gate of [left, right]) { const exited = once(gate.child, "exit"); gate.child.kill("SIGKILL"); await exited; }
  for (const [program, name, index] of [[rust, "rust", 0], [ts, "ts", 1]] as const) {
    const gate = await launch(program, name);
    results.push({ name: `${name}-kill-recovery`, pass: durableState(gate.sqlite) === before[index] });
    await stop(gate.child);
  }
  // Switch each existing database to the other runtime, then back. No schema reset.
  for (const [program, name, index] of [[ts, "rust", 0], [rust, "ts", 1], [rust, "rust", 0], [ts, "ts", 1]] as const) {
    const gate = await launch(program, name);
    const budget = await call(gate.base, { name: "handoff", method: "GET", path: "/api/v1/namespaces/platform/budgets/compat", expectStatus: 200 }, "handoff");
    results.push({ name: `${name}-database-on-${program === ts ? "typescript" : "rust"}`, pass: budget.status === 200 && durableState(gate.sqlite) === before[index] });
    await stop(gate.child);
  }
} catch (error) {
  report.error = error instanceof Error ? error.stack : String(error);
  process.stderr.write(`${report.error}\n`);
} finally {
  const cleanup = await Promise.allSettled(gates.map((gate) => stop(gate.child)));
  for (const outcome of cleanup) if (outcome.status === "rejected") report.cleanup_error = String(outcome.reason);
  upstream.closeAllConnections(); upstream.close();
  report.completed_at = new Date().toISOString();
  report.pass = !report.error && !report.cleanup_error && results.length > 0 && results.every((result) => result.pass === true);
  await writeFile(reportPath, JSON.stringify(report, null, 2) + "\n");
  await rm(root, { recursive: true, force: true });
}
if (!report.pass) process.exitCode = 1;
