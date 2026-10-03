/**
 * Send the same fixture traffic through the Rust binary and the TypeScript
 * process and compare status, body, and the charged usage rows.
 *
 *   node --experimental-strip-types scripts/shadow-compare.ts \
 *     --rust ../../target/debug/axond --ts ../bin/axond
 */
import { spawn, type ChildProcess } from "node:child_process";
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
  expectStatus?: number;
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
    { name: "healthz", method: "GET", path: "/healthz", headers: {} },
    { name: "models", method: "GET", path: "/ns/platform/v1/models" },
    { name: "chat", method: "POST", path: "/ns/platform/v1/chat/completions", body: chat },
    { name: "chat-stream", method: "POST", path: "/ns/platform/v1/chat/completions", body: chatStream },
    { name: "embeddings", method: "POST", path: "/ns/platform/v1/embeddings", body: embeddings },
    { name: "responses", method: "POST", path: "/ns/platform/v1/responses", body: responses },
    { name: "responses-stream", method: "POST", path: "/ns/platform/v1/responses", body: responsesStream },
    {
      name: "messages",
      method: "POST",
      path: "/ns/platform/v1/messages",
      body: messages,
      headers: { "x-api-key": KEY, "anthropic-version": "2023-06-01" },
    },
    {
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
  child.stderr?.on("data", (chunk: Buffer) => {
    stderr += chunk.toString();
  });
  const base = `http://${bind}`;
  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline) {
    if (child.exitCode !== null) {
      throw new Error(`${program} exited ${child.exitCode}: ${stderr}`);
    }
    try {
      const response = await fetch(`${base}/healthz`);
      if (response.ok) {
        await response.arrayBuffer();
        return { base, sqlite, child };
      }
    } catch {
      // not up yet
    }
    await new Promise((wake) => setTimeout(wake, 40));
  }
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

async function call(base: string, item: Case, requestId: string): Promise<{ status: number; body: Buffer }> {
  const headers: Record<string, string> = {
    "x-request-id": requestId,
    ...(item.body ? { "content-type": "application/json" } : {}),
    ...(item.headers ?? { authorization: `Bearer ${KEY}` }),
  };
  if (item.headers && !item.headers.authorization && !item.headers["x-api-key"]) {
    headers.authorization = `Bearer ${KEY}`;
  }
  const response = await fetch(`${base}${item.path}`, { method: item.method, headers, body: item.body });
  return { status: response.status, body: Buffer.from(await response.arrayBuffer()) };
}

/**
 * Rust re-encodes some JSON (sorted object keys, compact SSE data). The
 * TypeScript gateway relays upstream bytes. Compare the parsed value.
 * Request ids differ because each process mints its own, so charges compare
 * model, status, and cost.
 */
function samePayload(left: Buffer, right: Buffer): boolean {
  if (left.equals(right)) {
    return true;
  }
  const a = left.toString("utf8");
  const b = right.toString("utf8");
  if (a.startsWith("{") || a.startsWith("[")) {
    try {
      return canonical(JSON.parse(a)) === canonical(JSON.parse(b));
    } catch {
      return false;
    }
  }
  return canonicalSse(a) === canonicalSse(b);
}

function canonicalSse(text: string): string {
  return text
    .split(/\n\n/)
    .map((frame) =>
      frame
        .split("\n")
        .map((line) => {
          if (!line.startsWith("data:")) {
            return line;
          }
          const data = line.slice(5).replace(/^ /, "");
          if (data === "[DONE]" || data.length === 0) {
            return line;
          }
          try {
            return `data: ${canonical(JSON.parse(data))}`;
          } catch {
            return line;
          }
        })
        .join("\n"),
    )
    .join("\n\n");
}

function canonical(value: unknown): string {
  return JSON.stringify(sortKeys(value));
}

function sortKeys(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(sortKeys);
  }
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    const sorted: Record<string, unknown> = {};
    for (const key of Object.keys(record).sort()) {
      sorted[key] = sortKeys(record[key]);
    }
    return sorted;
  }
  return value;
}

function chargeRows(sqlite: string): string[] {
  const db = new DatabaseSync(sqlite, { readOnly: true });
  const rows = db
    .prepare(
      `SELECT model, status, COALESCE(cost_microdollars, -1) AS cost
       FROM axond_store_usage ORDER BY model, status, cost`,
    )
    .all() as Array<{ model: string; status: string; cost: number | bigint }>;
  db.close();
  return rows.map((row) => `${row.model} ${row.status} ${row.cost}`);
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
  const rustGate = await boot(rust, join(root, "rust"), fake.url);
  const tsGate = await boot(ts, join(root, "ts"), fake.url);
  const diffs: string[] = [];
  try {
    await putBudgets(rustGate.base);
    await putBudgets(tsGate.base);
    let index = 0;
    for (const item of cases()) {
      index += 1;
      const requestId = `shadow-${item.name}-${index}`;
      const left = await call(rustGate.base, item, requestId);
      const right = await call(tsGate.base, item, requestId);
      if (left.status !== right.status || !samePayload(left.body, right.body)) {
        diffs.push(
          `${item.name}: rust ${left.status} ${left.body.toString("utf8").slice(0, 240)} | ts ${right.status} ${right.body.toString("utf8").slice(0, 240)}`,
        );
      } else if (left.body.equals(right.body)) {
        process.stdout.write(`match ${item.name} ${left.status} ${left.body.length}b\n`);
      } else {
        process.stdout.write(`semantic ${item.name} ${left.status} rust ${left.body.length}b ts ${right.body.length}b\n`);
      }
    }
    await new Promise((wake) => setTimeout(wake, 200));
    const rustUsage = chargeRows(rustGate.sqlite);
    const tsUsage = chargeRows(tsGate.sqlite);
    if (rustUsage.join("\n") !== tsUsage.join("\n")) {
      diffs.push(`usage rust:\n${rustUsage.join("\n")}\nts:\n${tsUsage.join("\n")}`);
    } else {
      process.stdout.write(`match usage ${rustUsage.length} rows\n`);
    }
  } finally {
    rustGate.child.kill("SIGTERM");
    tsGate.child.kill("SIGTERM");
    await new Promise((wake) => setTimeout(wake, 300));
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
