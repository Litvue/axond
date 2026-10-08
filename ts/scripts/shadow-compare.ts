/**
 * Send the same fixture traffic through the Rust binary and the TypeScript
 * process and compare status, body, and the charged usage rows.
 *
 *   node --experimental-strip-types scripts/shadow-compare.ts \
 *     --rust ../../target/debug/axond --ts ../bin/axond
 */
import { spawn, type ChildProcess } from "node:child_process";
import { samePayload } from "./parity.ts";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { createServer, type ServerResponse } from "node:http";
import { createServer as createNet } from "node:net";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { AddressInfo } from "node:net";

const repo = resolve(new URL("../..", import.meta.url).pathname);
const fixtures = join(repo, "tests/fixtures");

interface Case {
  name: string;
  method: string;
  path: string;
  body?: string;
  headers?: Record<string, string>;
  expectStatus: number;
  anonymous?: boolean;
  byteFaithful?: boolean;
}

const KEY = "test-inbound-key";

function cases(): Case[] {
  const chat = JSON.stringify({
    model: "fake-openai/fixture-chat",
    messages: [{ role: "user", content: "What is the capital of France?" }],
  });
  const chatStream = JSON.stringify({
    model: "fake-openai/fixture-chat",
    stream: true,
    messages: [{ role: "user", content: "What is the capital of France?" }],
  });
  const embeddings = JSON.stringify({ model: "fake-openai/fixture-embeddings", input: "hello" });
  const responses = JSON.stringify({
    model: "fake-openai/fixture-responses",
    input: "What is the capital of France?",
  });
  const responsesStream = JSON.stringify({
    model: "fake-openai/fixture-responses",
    stream: true,
    input: "What is the capital of France?",
  });
  const messages = JSON.stringify({
    model: "fake-anthropic/fixture-messages",
    max_tokens: 1024,
    messages: [{ role: "user", content: "Weather in Paris?" }],
  });
  const messagesStream = JSON.stringify({
    model: "fake-anthropic/fixture-messages",
    max_tokens: 1024,
    stream: true,
    messages: [{ role: "user", content: "Weather in Paris?" }],
  });
  return [
    { name: "healthz", method: "GET", path: "/healthz", headers: {}, expectStatus: 200 },
    { name: "models", method: "GET", path: "/ns/platform/v1/models", expectStatus: 200 },
    { name: "chat", method: "POST", path: "/ns/platform/v1/chat/completions", body: chat, expectStatus: 200 },
    { name: "chat-stream", method: "POST", path: "/ns/platform/v1/chat/completions", body: chatStream, expectStatus: 200 },
    { name: "embeddings", method: "POST", path: "/ns/platform/v1/embeddings", body: embeddings, expectStatus: 200 },
    { name: "responses", method: "POST", path: "/ns/platform/v1/responses", body: responses, expectStatus: 200 },
    { byteFaithful: true, name: "responses-stream", method: "POST", path: "/ns/platform/v1/responses", body: responsesStream, expectStatus: 200 },
    {
      expectStatus: 200,
      name: "messages",
      method: "POST",
      path: "/ns/platform/v1/messages",
      body: messages,
      headers: { "x-api-key": KEY, "anthropic-version": "2023-06-01" },
    },
    {
      expectStatus: 200,
      byteFaithful: true,
      name: "messages-stream",
      method: "POST",
      path: "/ns/platform/v1/messages",
      body: messagesStream,
      headers: { "x-api-key": KEY, "anthropic-version": "2023-06-01" },
    },
    {
      name: "unprefixed",
      method: "POST",
      path: "/ns/platform/v1/chat/completions",
      body: JSON.stringify({ model: "gpt-test", messages: [] }),
      expectStatus: 400,
    },
    { name: "unknown-namespace", method: "GET", path: "/ns/ghost/v1/models", expectStatus: 404 },
    { name: "encoded-namespace", method: "GET", path: "/ns/%70latform/v1/models", expectStatus: 400 },
    { name: "readyz", method: "GET", path: "/readyz", expectStatus: 200 },
    { name: "unauthenticated", method: "GET", path: "/ns/platform/v1/models", anonymous: true, expectStatus: 401 },
    { name: "auth-before-namespace", method: "GET", path: "/ns/ghost/v1/models", headers: { authorization: "Bearer wrong" }, expectStatus: 401 },
    { name: "minted-token-refused", method: "GET", path: "/ns/platform/v1/models", headers: { authorization: "Bearer axt1.invalid" }, expectStatus: 401 },
    { name: "management-auth", method: "GET", path: "/api/v1/namespaces", anonymous: true, expectStatus: 401 },
    { name: "unmounted-v1", method: "GET", path: "/v1/models", expectStatus: 404 },
    { name: "unmounted-admin", method: "GET", path: "/admin/v1/status", expectStatus: 404 },
    { name: "credentials", method: "GET", path: "/ns/platform/v1/credentials", expectStatus: 200 },
    { name: "credentials-duplicate-query", method: "GET", path: "/ns/platform/v1/credentials?namespaces=all&namespaces=all", expectStatus: 400 },
    { name: "invalid-json", method: "POST", path: "/ns/platform/v1/chat/completions", body: "{", expectStatus: 400 },
    { name: "null-model", method: "POST", path: "/ns/platform/v1/chat/completions", body: "null", expectStatus: 400 },
    { name: "wrong-content-type", method: "POST", path: "/ns/platform/v1/chat/completions", body: chat, headers: { "content-type": "text/plain" }, expectStatus: 415 },
    { name: "namespace-list", method: "GET", path: "/api/v1/namespaces?limit=1", expectStatus: 200 },
    { name: "namespace-create", method: "POST", path: "/api/v1/namespaces", body: '{"id":"audit","attrs":{"owner":"regression"}}', expectStatus: 201 },
    { name: "namespace-conflict", method: "POST", path: "/api/v1/namespaces", body: '{"id":"audit","attrs":{}}', expectStatus: 409 },
    { name: "namespace-read", method: "GET", path: "/api/v1/namespaces/audit", expectStatus: 200 },
    { name: "namespace-update", method: "PUT", path: "/api/v1/namespaces/audit", body: '{"attrs":{"owner":"updated"},"blocklist":["fake-openai/*"]}', expectStatus: 200 },
    { name: "namespace-budget", method: "PUT", path: "/api/v1/namespaces/audit/budgets/compat", body: '{"limit_microdollars":1000000}', expectStatus: 200 },
    { name: "blocked-model", method: "POST", path: "/ns/audit/v1/chat/completions", body: chat, expectStatus: 400 },
    { name: "namespace-delete", method: "DELETE", path: "/api/v1/namespaces/audit", expectStatus: 204 },
    { name: "namespace-delete-again", method: "DELETE", path: "/api/v1/namespaces/audit", expectStatus: 204 },
    { name: "namespace-deleted", method: "GET", path: "/api/v1/namespaces/audit", expectStatus: 404 },
    { name: "config-namespace-delete", method: "DELETE", path: "/api/v1/namespaces/platform", expectStatus: 409 },
    { name: "budget-read", method: "GET", path: "/api/v1/namespaces/platform/budgets/compat", expectStatus: 200 },
    { name: "usage-summary", method: "GET", path: "/api/v1/namespaces/platform/usage?period=compat", expectStatus: 200 },
    { name: "usage-period-required", method: "GET", path: "/api/v1/namespaces/platform/usage", expectStatus: 400 },
    { name: "budget-unknown-field", method: "PUT", path: "/api/v1/namespaces/platform/budgets/compat", body: '{"limit_microdollars":1,"extra":true}', expectStatus: 400 },
    { name: "budget-exhaust", method: "PUT", path: "/api/v1/namespaces/platform/budgets/compat", body: '{"limit_microdollars":0}', expectStatus: 200 },
    { name: "budget-denies-inference", method: "POST", path: "/ns/platform/v1/chat/completions", body: chat, expectStatus: 429 },
    { name: "credentials-all", method: "GET", path: "/ns/platform/v1/credentials?namespaces=all", expectStatus: 200 },
    { name: "provider-models", method: "GET", path: "/api/v1/providers/fake-openai/models", expectStatus: 200 },
    { name: "provider-models-all", method: "GET", path: "/api/v1/providers/models", expectStatus: 200 },
    { name: "fixed-policy-put", method: "PUT", path: "/api/v1/namespaces/tenant/budget", body: '{"cadence":"fixed","period":"compat","limit_microdollars":1000000}', expectStatus: 200 },
    { name: "fixed-policy-get", method: "GET", path: "/api/v1/namespaces/tenant/budget", expectStatus: 200 },
    { name: "monthly-policy-put", method: "PUT", path: "/api/v1/namespaces/tenant/budget", body: '{"cadence":"monthly","timezone":"America/New_York","limit_microdollars":1000000}', expectStatus: 200 },
    { name: "monthly-policy-get", method: "GET", path: "/api/v1/namespaces/tenant/budget", expectStatus: 200 },
    { name: "persistent-namespace", method: "POST", path: "/api/v1/namespaces", body: '{"id":"persist","attrs":{"owner":"restart"}}', expectStatus: 201 },

  ];
}

async function fixture(name: string): Promise<Buffer> {
  return readFile(join(fixtures, name));
}

async function upstream(): Promise<{ url: string; close: () => void }> {
  const buffered = new Map<string, Buffer>([
    ["/chat/completions", await fixture("openai/chat_completion.json")],
    ["/embeddings", await fixture("openai/embeddings.json")],
    ["/responses", await fixture("openai/responses.json")],
    ["/messages", await fixture("anthropic/message_thinking_tool_use.json")],
  ]);
  const streamed = new Map<string, Buffer>([
    ["/chat/completions", await fixture("openai/chat_completion.sse")],
    ["/responses", await fixture("openai/responses.sse")],
    ["/messages", await fixture("anthropic/message_thinking_tool_use.sse")],
  ]);
  const server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (chunk: Buffer) => chunks.push(chunk));
    req.on("end", () => {
      let stream = false;
      try {
        stream = JSON.parse(Buffer.concat(chunks).toString("utf8")).stream === true;
      } catch {
        stream = false;
      }
      const path = (req.url ?? "").split("?")[0] ?? "";
      const body = stream ? streamed.get(path) : buffered.get(path);
      if (!body) {
        res.writeHead(404);
        res.end();
        return;
      }
      writeChunks(res, body, stream ? "text/event-stream" : "application/json");
    });
  });
  await new Promise<void>((ready) => server.listen(0, "127.0.0.1", () => ready()));
  const address = server.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${address.port}`,
    close: () => {
      server.closeAllConnections();
      server.close();
    },
  };
}

function writeChunks(res: ServerResponse, body: Buffer, contentType: string): void {
  res.writeHead(200, { "content-type": contentType });
  let offset = 0;
  const step = () => {
    if (offset >= body.length) {
      res.end();
      return;
    }
    const next = body.subarray(offset, offset + 5);
    offset += 5;
    res.write(next, () => step());
  };
  step();
}

function config(bind: string, sqlite: string, baseUrl: string): string {
  return `
[server]
bind = "${bind}"

[shutdown]
drain_grace_ms = 50
deadline_ms = 3000
flush_timeout_ms = 1000

[storage]
backend = "sqlite"
path = "${sqlite}"

[[namespace]]
id = "platform"
default = true

[[namespace]]
id = "tenant"

[[provider]]
id = "fake-openai"
kind = "openai"
base_url = "${baseUrl}"

[[provider]]
id = "fake-anthropic"
kind = "anthropic"
base_url = "${baseUrl}"

[[credential]]
namespace = "platform"
provider = "fake-openai"
env = "GW_FAKE_OPENAI_KEY"

[[credential]]
namespace = "platform"
provider = "fake-anthropic"
env = "GW_FAKE_ANTHROPIC_KEY"

[[gateway_key]]
env = "GW_INBOUND_KEY"
namespace = "platform"

[[price]]
provider = "fake-openai"
model = "*"
input_microdollars_per_million = 2500000
output_microdollars_per_million = 10000000

[[price]]
provider = "fake-anthropic"
model = "*"
input_microdollars_per_million = 2500000
output_microdollars_per_million = 10000000
`;
}

async function freePort(): Promise<number> {
  const probe = createNet();
  await new Promise<void>((ready) => probe.listen(0, "127.0.0.1", () => ready()));
  const { port } = probe.address() as AddressInfo;
  await new Promise<void>((closed) => probe.close(() => closed()));
  return port;
}

async function boot(program: string, directory: string, baseUrl: string): Promise<{ base: string; sqlite: string; child: ChildProcess }> {
  const port = await freePort();
  const bind = `127.0.0.1:${port}`;
  const sqlite = join(directory, "axond.sqlite");
  const configPath = join(directory, "axond.toml");
  await writeFile(configPath, config(bind, sqlite, baseUrl));
  const child = spawn(program, {
    env: {
      ...process.env,
      AXOND_CONFIG: configPath,
      GW_INBOUND_KEY: KEY,
      GW_FAKE_OPENAI_KEY: "upstream-openai",
      GW_FAKE_ANTHROPIC_KEY: "upstream-anthropic",
      RUST_LOG: "error",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stderr = "";
  let spawnError: Error | undefined;
  child.on("error", (error) => { spawnError = error; });
  child.stderr?.on("data", (chunk: Buffer) => {
    stderr += chunk.toString();
  });
  const base = `http://${bind}`;
  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline) {
    if (spawnError) throw spawnError;
    if (child.exitCode !== null) {
      throw new Error(`${program} exited ${child.exitCode}: ${stderr}`);
    }
    try {
      const response = await fetch(`${base}/healthz`, { signal: AbortSignal.timeout(1000) });
      if (response.ok) {
        await response.arrayBuffer();
        return { base, sqlite, child };
      }
    } catch {
      // not up yet
    }
    await new Promise((wake) => setTimeout(wake, 40));
  }
  child.kill("SIGKILL");
  throw new Error(`${program} did not become healthy: ${stderr}`);
}

async function putBudgets(base: string): Promise<void> {
  for (const namespace of ["platform", "tenant"]) {
    const response = await fetch(`${base}/api/v1/namespaces/${namespace}/budgets/compat`, {
      method: "PUT",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ limit_microdollars: 1_000_000_000_000 }),
    });
    if (!response.ok) {
      throw new Error(`budget ${namespace} on ${base}: ${response.status} ${await response.text()}`);
    }
  }
}

function normalizeContentType(value: string | null): string | null {
  if (value === null) return null;
  const [media, ...parameters] = value.toLowerCase().split(";").map((part) => part.trim());
  // JSON is UTF-8 with or without an explicit charset; Bun adds that parameter.
  const kept = parameters.filter((part) => !(media === "application/json" && part === "charset=utf-8"));
  return [media, ...kept.sort()].join("; ");
}

async function call(base: string, item: Case, requestId: string): Promise<{ status: number; body: Buffer; headers: string }> {
  const headers: Record<string, string> = {
    "x-request-id": requestId,
    ...(item.body ? { "content-type": "application/json" } : {}),
    ...(item.headers ?? { authorization: `Bearer ${KEY}` }),
  };
  if (item.anonymous) {
    delete headers.authorization;
  } else if (item.headers && !item.headers.authorization && !item.headers["x-api-key"]) {
    headers.authorization = `Bearer ${KEY}`;
  }
  const response = await fetch(`${base}${item.path}`, { method: item.method, headers, body: item.body, signal: AbortSignal.timeout(10000) });
  const body = Buffer.from(await response.arrayBuffer());
  const contractHeaders = ["content-type", "cache-control", "retry-after", "allow"]
    .map((name) => [name, name === "content-type"
      ? (body.length === 0 && (response.status === 204 || response.status === 404)
        ? null : normalizeContentType(response.headers.get(name)))
      : response.headers.get(name)]);
  return { status: response.status, body, headers: JSON.stringify(contractHeaders) };
}

function chargeRows(sqlite: string): string[] {
  const db = new DatabaseSync(sqlite, { readOnly: true });
  const rows = db
    .prepare(
      `SELECT namespace, period, model, status, CAST(cost_microdollars AS TEXT) AS cost
       FROM axond_store_usage ORDER BY namespace, period, model, status, cost`,
    )
    .all() as Array<{ namespace: string; period: string | null; model: string; status: string; cost: number | bigint | null }>;
  db.close();
  return rows.map((row) => JSON.stringify([row.namespace, row.period, row.model, row.status, row.cost === null ? null : String(row.cost)]));
}

async function waitForUsage(sqlite: string, expected: number): Promise<void> {
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    const count = chargeRows(sqlite).length;
    if (count === expected) return;
    if (count > expected) throw new Error(`unexpected usage count: ${count}, expected ${expected}`);
    await new Promise((wake) => setTimeout(wake, 25));
  }
  throw new Error(`settlement did not produce ${expected} usage rows`);
}

function durableState(sqlite: string): string {
  const db = new DatabaseSync(sqlite, { readOnly: true });
  try {
    const budgets = db.prepare(`SELECT namespace, period, limit_microdollars, spent_microdollars
      FROM axond_store_budget ORDER BY namespace, period`).all();
    const cadence = db.prepare(`SELECT namespace, cadence, limit_microdollars, timezone
      FROM axond_store_budget_cadence ORDER BY namespace`).all();
    const active = db.prepare(`SELECT namespace, period FROM axond_store_budget_active ORDER BY namespace`).all();
    return JSON.stringify({ budgets, active, cadence, usage: chargeRows(sqlite) });
  } finally {
    db.close();
  }
}

async function stop(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  await new Promise<void>((resolve, reject) => {
    const deadline = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new Error("gateway did not stop within 5 seconds"));
    }, 5000);
    child.once("exit", () => { clearTimeout(deadline); resolve(); });
    child.kill("SIGTERM");
  });
}

async function main(): Promise<void> {
  const rust = process.argv.includes("--rust")
    ? resolve(process.argv[process.argv.indexOf("--rust") + 1] ?? "")
    : join(repo, "target/debug/axond");
  const ts = process.argv.includes("--ts")
    ? resolve(process.argv[process.argv.indexOf("--ts") + 1] ?? "")
    : join(repo, "ts/bin/axond");
  const fake = await upstream();
  const root = join(tmpdir(), `axond-shadow-${Date.now()}`);
  await mkdir(join(root, "rust"), { recursive: true });
  await mkdir(join(root, "ts"), { recursive: true });
  const gates: Array<Awaited<ReturnType<typeof boot>>> = [];
  const diffs: string[] = [];
  try {
    const rustGate = await boot(rust, join(root, "rust"), fake.url);
    gates.push(rustGate);
    const tsGate = await boot(ts, join(root, "ts"), fake.url);
    gates.push(tsGate);
    await putBudgets(rustGate.base);
    await putBudgets(tsGate.base);
    let index = 0;
    for (const item of cases()) {
      index += 1;
      if (item.name === "budget-read") {
        await waitForUsage(rustGate.sqlite, 7);
        await waitForUsage(tsGate.sqlite, 7);
      }
      const requestId = `shadow-${item.name}-${index}`;
      const left = await call(rustGate.base, item, requestId);
      const right = await call(tsGate.base, item, requestId);
      if (left.status !== item.expectStatus || right.status !== item.expectStatus || left.headers !== right.headers || !(item.byteFaithful ? left.body.equals(right.body) : samePayload(left.body, right.body))) {
        diffs.push(
          `${item.name} expected ${item.expectStatus}: headers rust ${left.headers} ts ${right.headers}; rust ${left.status} ${left.body.toString("utf8").slice(0, 240)} | ts ${right.status} ${right.body.toString("utf8").slice(0, 240)}`,
        );
      } else if (left.body.equals(right.body)) {
        process.stdout.write(`match ${item.name} ${left.status} ${left.body.length}b\n`);
      } else {
        process.stdout.write(`semantic ${item.name} ${left.status} rust ${left.body.length}b ts ${right.body.length}b\n`);
      }
    }
    await waitForUsage(rustGate.sqlite, 7);
    await waitForUsage(tsGate.sqlite, 7);
    const rustUsage = chargeRows(rustGate.sqlite);
    const tsUsage = chargeRows(tsGate.sqlite);
    if (rustUsage.join("\n") !== tsUsage.join("\n")) {
      diffs.push(`usage rust:\n${rustUsage.join("\n")}\nts:\n${tsUsage.join("\n")}`);
    } else {
      process.stdout.write(`match usage ${rustUsage.length} rows\n`);
    }

    const before = gates.map((gate) => durableState(gate.sqlite));
    if (before[0] !== before[1]) diffs.push(`durable state differs: rust ${before[0]} | ts ${before[1]}`);
    await Promise.all(gates.map((gate) => stop(gate.child)));
    for (const [program, directory] of [[rust, "rust"], [ts, "ts"]] as const) {
      const gate = await boot(program, join(root, directory), fake.url);
      gates.push(gate);
      const persisted = await call(gate.base, {
        name: "restart-namespace", method: "GET", path: "/api/v1/namespaces/persist", expectStatus: 200,
      }, "restart-read");
      if (persisted.status !== 200 || !samePayload(persisted.body, Buffer.from('{"id":"persist","attrs":{"owner":"restart"}}'))) {
        diffs.push(`${directory}: namespace did not survive restart`);
      }
      if (durableState(gate.sqlite) !== before[directory === "rust" ? 0 : 1]) {
        diffs.push(`${directory}: budget or usage changed across restart`);
      }
    }
    process.stdout.write("checked restart: namespace, budgets, and usage\n");
  } finally {
    const stopped = await Promise.allSettled(gates.map((gate) => stop(gate.child)));
    for (const result of stopped) {
      if (result.status === "rejected") diffs.push(`cleanup: ${String(result.reason)}`);
    }
    fake.close();
    await rm(root, { recursive: true, force: true });
  }
  if (diffs.length > 0) {
    process.stderr.write(`${diffs.join("\n\n")}\n`);
    process.exit(1);
  }
  process.stdout.write("shadow compare: rust and typescript responses and charges match\n");
}

main().catch((error: unknown) => {
  process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
  process.exit(1);
});
