import assert from "node:assert/strict";
import { createServer } from "node:http";
import test from "node:test";
import { Agent } from "undici";

import { createAxond } from "./app.ts";
import { createMemoryStore } from "./memory-store.ts";
import { createMetrics } from "./metrics.ts";
import type { Store } from "@axond/sdk";

const KEY = "test-inbound-key";

async function seeded(): Promise<Store> {
  const store = createMemoryStore();
  await store.putNamespace({
    id: "platform",
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: true,
  });
  await store.putNamespace({
    id: "tenant",
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: false,
  });
  await store.putBudget("platform", "compat", 1_000_000_000n);
  await store.putBudget("tenant", "compat", 1_000_000_000n);
  return store;
}

function listen(handler: (req: import("node:http").IncomingMessage, res: import("node:http").ServerResponse) => void) {
  const server = createServer(handler);
  return new Promise<{ url: string; close: () => void }>((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        throw new Error("no port");
      }
      resolve({
        url: `http://127.0.0.1:${address.port}`,
        close: () => {
          server.closeAllConnections();
          server.close();
        },
      });
    });
  });
}

test("unsupported_extension_api_version_is_refused_at_mount", async () => {
  const store = await seeded();
  assert.throws(
    () =>
      createAxond({
        store,
        gatewayKey: KEY,
        defaultNamespace: "platform",
        providers: [],
        extensions: [
          {
            name: "future",
            apiVersion: 2 as 1,
            stage: "pre-auth",
            async middleware(_c, next) {
              await next();
            },
          },
        ],
      }),
    /apiVersion 2 is not supported/,
  );
});

test("extension_migration_outside_its_prefix_is_refused", async () => {
  const store = await seeded();
  assert.throws(
    () =>
      createAxond({
        store,
        gatewayKey: KEY,
        defaultNamespace: "platform",
        providers: [],
        extensions: [
          {
            name: "probe",
            apiVersion: 1,
            stage: "pre-auth",
            migrations: ["CREATE TABLE axond_namespace_shadow (id TEXT)"],
            async middleware(_c, next) {
              await next();
            },
          },
        ],
      }),
    /outside `axond_ext_probe_`/,
  );
});

test("a cancelled stream still settles the delivered request", async () => {
  const store = await seeded();
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.write('data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n');
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "upstream", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 2_500_000n,
        outputMicrodollarsPerMillion: 10_000_000n,
      },
    ],
  });
  const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
    body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, messages: [] }),
  });
  assert.equal(response.status, 200);
  const reader = response.body!.getReader();
  await reader.read();
  await reader.cancel();
  let rows: { count: number }[] = [];
  for (let attempt = 0; attempt < 20; attempt += 1) {
    rows = await store.summarizeUsage("platform", "compat");
    if (rows.length > 0) {
      break;
    }
    await new Promise((wake) => setTimeout(wake, 10));
  }
  assert.equal(rows.reduce((sum, row) => sum + row.count, 0), 1);
  upstream.close();
});

test("a provider 500 stays on that credential and a 429 rotates", async () => {
  const store = await seeded();
  const seen: string[] = [];
  let mode: "down" | "limited" = "down";
  const upstream = await listen((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (chunk: Buffer) => chunks.push(chunk));
    req.on("end", () => {
      const authorization = req.headers.authorization ?? "";
      seen.push(authorization);
      if (authorization.includes("bad-key") && mode === "down") {
        res.writeHead(500, { "content-type": "application/json" });
        res.end('{"error":{"message":"down"}}');
        return;
      }
      if (authorization.includes("bad-key")) {
        res.writeHead(429, { "content-type": "application/json" });
        res.end('{"error":{"message":"slow down"}}');
        return;
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end('{"usage":{"prompt_tokens":1,"completion_tokens":1}}');
    });
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [
      { namespace: "platform", provider: "fake-openai", secret: "bad-key", id: "bad" },
      { namespace: "platform", provider: "fake-openai", secret: "good-key", id: "good" },
    ],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
  });
  const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
  const down = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers,
    body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [] }),
  });
  assert.equal(down.status, 502);
  assert.equal((await down.json()).error.type, "provider_dependency_failed");
  assert.deepEqual(seen, ["Bearer bad-key"]);
  const rotated = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers,
    body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [] }),
  });
  assert.equal(rotated.status, 200);
  await rotated.text();
  mode = "limited";
  const limited = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers,
    body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [] }),
  });
  assert.equal(limited.status, 200);
  assert.deepEqual(seen, ["Bearer bad-key", "Bearer good-key", "Bearer bad-key", "Bearer good-key"]);
  upstream.close();
});

test("two rate limits park the credential and a success clears the streak", async () => {
  const store = await seeded();
  let statusCode = 429;
  const upstream = await listen((_req, res) => {
    res.writeHead(statusCode, { "content-type": "application/json" });
    res.end(
      statusCode === 200
        ? '{"usage":{"prompt_tokens":1,"completion_tokens":1}}'
        : '{"error":{"message":"PROMPT_SENTINEL"}}',
    );
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [
      { namespace: "platform", provider: "fake-openai", secret: "sk-park-secret", id: "only" },
      { namespace: "tenant", provider: "fake-openai", secret: "sk-tenant-secret", id: "tenant-key" },
    ],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
  });
  const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
  const once = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers,
    body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: "PROMPT_SENTINEL" }] }),
  });
  assert.equal(once.status, 502);
  await once.text();
  const early = await app.request("http://127.0.0.1/ns/platform/v1/credentials", { headers });
  assert.equal((await early.json()).data[0].state, "healthy");
  statusCode = 200;
  const served = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers,
    body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [] }),
  });
  assert.equal(served.status, 200);
  await served.text();
  statusCode = 429;
  for (let attempt = 0; attempt < 2; attempt += 1) {
    const failed = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers,
      body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: "PROMPT_SENTINEL" }] }),
    });
    assert.equal(failed.status, 502);
    await failed.text();
  }
  const status = await app.request("http://127.0.0.1/ns/platform/v1/credentials", { headers });
  assert.equal(status.status, 200);
  const body = await status.json();
  assert.equal(body.observed, "replica");
  assert.equal(body.data[0].credential_id, "only");
  assert.equal(body.data[0].state, "parked");
  const encoded = JSON.stringify(body);
  assert.equal(encoded.includes("sk-park-secret"), false);
  assert.equal(encoded.includes("PROMPT_SENTINEL"), false);
  const denied = await app.request("http://127.0.0.1/ns/tenant/v1/credentials?namespaces=all", { headers });
  assert.equal(denied.status, 403);
  assert.equal((await denied.json()).error.type, "token_scope_insufficient");
  upstream.close();
});

test("a responses continuation refuses a parked first credential", async () => {
  const store = await seeded();
  let hits = 0;
  const upstream = await listen((_req, res) => {
    hits += 1;
    res.writeHead(429, { "content-type": "application/json" });
    res.end('{"error":{"message":"down"}}');
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [
      { namespace: "platform", provider: "fake-openai", secret: "first-secret", id: "first" },
      { namespace: "platform", provider: "fake-openai", secret: "second-secret", id: "second" },
    ],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
  });
  const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
  for (let attempt = 0; attempt < 2; attempt += 1) {
    const failed = await app.request("http://127.0.0.1/ns/platform/v1/responses", {
      method: "POST",
      headers,
      body: JSON.stringify({ model: "fake-openai/gpt-test", input: "hello" }),
    });
    assert.equal(failed.status, 502);
    await failed.text();
  }
  assert.equal(hits, 2);
  const continued = await app.request("http://127.0.0.1/ns/platform/v1/responses", {
    method: "POST",
    headers,
    body: JSON.stringify({ model: "fake-openai/gpt-test", previous_response_id: "resp_prior", input: "again" }),
  });
  assert.equal(continued.status, 503);
  const body = await continued.json();
  assert.equal(body.error.type, "continuation_affinity_unavailable");
  assert.equal(body.error.message, "continuation affinity unavailable for Responses target `fake-openai/gpt-test`");
  assert.equal(hits, 2);
  const initial = await app.request("http://127.0.0.1/ns/platform/v1/responses", {
    method: "POST",
    headers,
    body: JSON.stringify({ model: "fake-openai/gpt-test", input: "fresh" }),
  });
  assert.equal(initial.status, 502);
  await initial.text();
  assert.equal(hits, 3);
  upstream.close();
});

test("a cooled credential is one probe and then parked again", async () => {
  const store = await seeded();
  let now = 1_000_000;
  const seen: string[] = [];
  const upstream = await listen((req, res) => {
    const authorization = req.headers.authorization ?? "";
    seen.push(authorization);
    if (authorization.includes("first-secret")) {
      res.writeHead(429, { "content-type": "application/json" });
      res.end('{"error":{"message":"slow down"}}');
      return;
    }
    res.writeHead(200, { "content-type": "application/json" });
    res.end('{"usage":{"prompt_tokens":1,"completion_tokens":1}}');
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    clock: () => now,
    credentialPool: { failureThreshold: 1, cooldownMs: 1_000 },
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [
      { namespace: "platform", provider: "fake-openai", secret: "first-secret", id: "first", weight: 1 },
      { namespace: "platform", provider: "fake-openai", secret: "second-secret", id: "second", weight: 1 },
    ],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
  });
  const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
  const chat = () =>
    app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers,
      body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [] }),
    });
  const parked = await chat();
  assert.equal(parked.status, 200);
  await parked.text();
  const during = await app.request("http://127.0.0.1/ns/platform/v1/credentials", { headers });
  const duringBody = await during.json();
  assert.equal(duringBody.data.find((row: { credential_id: string }) => row.credential_id === "first").state, "parked");
  now += 1_000;
  const cooled = await app.request("http://127.0.0.1/ns/platform/v1/credentials", { headers });
  const cooledBody = await cooled.json();
  assert.equal(cooledBody.data.find((row: { credential_id: string }) => row.credential_id === "first").state, "probe");
  seen.length = 0;
  const probed = await chat();
  assert.equal(probed.status, 200);
  await probed.text();
  assert.deepEqual(seen, ["Bearer first-secret", "Bearer second-secret"]);
  const rearmed = await app.request("http://127.0.0.1/ns/platform/v1/credentials", { headers });
  const rearmedBody = await rearmed.json();
  assert.equal(rearmedBody.data.find((row: { credential_id: string }) => row.credential_id === "first").state, "parked");
  seen.length = 0;
  const skipped = await chat();
  assert.equal(skipped.status, 200);
  await skipped.text();
  assert.deepEqual(seen, ["Bearer second-secret"]);
  upstream.close();
});

test("weighted selection follows the credential weights", async () => {
  const store = await seeded();
  const seen: string[] = [];
  const upstream = await listen((req, res) => {
    seen.push(req.headers.authorization ?? "");
    res.writeHead(200, { "content-type": "application/json" });
    res.end('{"usage":{"prompt_tokens":1,"completion_tokens":1}}');
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    credentialPool: { strategy: "weighted" },
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [
      { namespace: "platform", provider: "fake-openai", secret: "light-secret", id: "light", weight: 1 },
      { namespace: "platform", provider: "fake-openai", secret: "heavy-secret", id: "heavy", weight: 3 },
    ],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
  });
  const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
  for (let attempt = 0; attempt < 4; attempt += 1) {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers,
      body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [] }),
    });
    assert.equal(response.status, 200);
    await response.text();
  }
  assert.deepEqual(seen, [
    "Bearer light-secret",
    "Bearer heavy-secret",
    "Bearer heavy-secret",
    "Bearer heavy-secret",
  ]);
  upstream.close();
});

test("a header timeout is upstream_timeout", async () => {
  const store = await seeded();
  const upstream = await listen(() => {
    // Never writes a response.
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "upstream", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
    transport: {
      responseHeaderTimeoutMs: 80,
      bufferedBodyTimeoutMs: 80,
      streamIdleTimeoutMs: 80,
      maxResponseBytes: 1024,
    },
  });
  const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
    body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [] }),
  });
  assert.equal(response.status, 504);
  const body = await response.json();
  assert.equal(body.error.type, "upstream_timeout");
  assert.match(body.error.message, /exceeded its 80ms bound/);
  upstream.close();
});

test("a tighter failover budget ends the header wait", async () => {
  const store = await seeded();
  const metrics = createMetrics([]);
  const upstream = await listen(() => {
    // Never writes a response.
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    metrics,
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "upstream", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
    transport: {
      responseHeaderTimeoutMs: 5_000,
      bufferedBodyTimeoutMs: 5_000,
      streamIdleTimeoutMs: 5_000,
      maxResponseBytes: 1024,
      overallTimeoutMs: 50,
    },
  });
  const started = Date.now();
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [] }),
    });
    assert.equal(response.status, 504);
    const body = await response.json();
    assert.equal(body.error.type, "upstream_timeout");
    assert.match(body.error.message, /left of the request's failover budget/);
    assert.ok(Date.now() - started < 1_000);
    const point = metrics.points?.find((item) => item.name === "axond.upstream.timeouts");
    assert.equal(point?.attributes["axond.timeout"], "response_headers");
    assert.equal(point?.attributes["axond.timeout.bound"], "walk_budget");
  } finally {
    upstream.close();
  }
});

test("a spent failover budget does not open the next credential", async () => {
  const store = await seeded();
  let now = 1_000;
  let hits = 0;
  const upstream = await listen((_req, res) => {
    hits += 1;
    now = 1_050;
    res.writeHead(429, { "content-type": "application/json" });
    res.end('{"error":{"message":"slow down"}}');
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    clock: () => now,
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [
      { namespace: "platform", provider: "fake-openai", secret: "bad-key", id: "bad" },
      { namespace: "platform", provider: "fake-openai", secret: "good-key", id: "good" },
    ],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
    transport: {
      responseHeaderTimeoutMs: 5_000,
      bufferedBodyTimeoutMs: 5_000,
      streamIdleTimeoutMs: 5_000,
      maxResponseBytes: 1024,
      overallTimeoutMs: 40,
    },
  });
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [] }),
    });
    assert.equal(response.status, 504);
    const body = await response.json();
    assert.equal(body.error.type, "upstream_timeout");
    assert.match(body.error.message, /failover budget was spent before this attempt was dispatched/);
    assert.equal(hits, 1);
  } finally {
    upstream.close();
  }
});

test("post-terminal stream grace closes an open body", async () => {
  const store = await seeded();
  const completed =
    'event: response.completed\ndata: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":3,"output_tokens":1}}}\n\n';
  const tail = 'event: provider.extension\ndata: {"type":"provider.extension"}\n\n';
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.write(completed);
    setTimeout(() => {
      res.write(tail);
    }, 20).unref?.();
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "upstream", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
    transport: {
      responseHeaderTimeoutMs: 1_000,
      bufferedBodyTimeoutMs: 1_000,
      streamIdleTimeoutMs: 5_000,
      streamTerminalGraceMs: 80,
      maxResponseBytes: 4096,
    },
  });
  const started = Date.now();
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/responses", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, input: "hi" }),
    });
    assert.equal(response.status, 200);
    assert.equal(await response.text(), completed + tail);
    assert.ok(Date.now() - started < 1_000);
  } finally {
    upstream.close();
  }
});

test("a connect timeout is upstream_timeout and hides the address", async () => {
  const store = await seeded();
  const agent = new Agent({ connectTimeout: 50, connect: { autoSelectFamily: false } });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    upstreamDispatcher: agent,
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: "http://192.0.2.1:81" }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "upstream", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
    transport: {
      connectTimeoutMs: 50,
      responseHeaderTimeoutMs: 3_000,
      bufferedBodyTimeoutMs: 1_000,
      streamIdleTimeoutMs: 1_000,
      maxResponseBytes: 1024,
      overallTimeoutMs: 10_000,
    },
  });
  const started = Date.now();
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [] }),
    });
    assert.equal(response.status, 504);
    const body = await response.json();
    assert.equal(body.error.type, "upstream_timeout");
    assert.match(body.error.message, /connecting to the provider exceeded its 50ms bound/);
    assert.equal(body.error.message.includes("192.0.2.1"), false);
    assert.ok(Date.now() - started < 3_000);
  } finally {
    await agent.close();
  }
});

test("an oversized provider error is truncated and keeps the provider status", async () => {
  const store = await seeded();
  const prefix = '{"error":{"message":"VISIBLE"}}';
  const upstream = await listen((_req, res) => {
    res.writeHead(500, { "content-type": "application/json" });
    res.end(`${prefix}${"HIDDEN_TAIL_SENTINEL".repeat(20)}`);
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "upstream", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
    transport: {
      responseHeaderTimeoutMs: 1000,
      bufferedBodyTimeoutMs: 1000,
      streamIdleTimeoutMs: 1000,
      maxResponseBytes: 4096,
      maxErrorBytes: prefix.length,
    },
  });
  const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
    body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [] }),
  });
  assert.equal(response.status, 502);
  const text = await response.text();
  assert.equal(text.includes("HIDDEN_TAIL_SENTINEL"), false);
  assert.deepEqual(JSON.parse(text), {
    error: { type: "provider_dependency_failed", message: "VISIBLE" },
  });
  upstream.close();
});

test("a stalled provider error body still returns the provider status", async () => {
  const store = await seeded();
  const upstream = await listen((_req, res) => {
    res.writeHead(429, { "content-type": "application/json" });
    res.flushHeaders();
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "upstream", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
    transport: {
      responseHeaderTimeoutMs: 1000,
      bufferedBodyTimeoutMs: 80,
      streamIdleTimeoutMs: 1000,
      maxResponseBytes: 4096,
      maxErrorBytes: 64,
    },
  });
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [] }),
    });
    assert.equal(response.status, 502);
    assert.deepEqual(await response.json(), {
      error: { type: "provider_dependency_failed", message: "upstream request failed" },
    });
  } finally {
    upstream.close();
  }
});

test("management routes match the compatibility contract", async () => {
  const store = await seeded();
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    configNamespaces: ["platform"],
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: "http://127.0.0.1:9" }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "upstream", id: "openai" }],
  });
  const auth = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
  assert.equal((await app.request("http://127.0.0.1/healthz")).status, 200);
  assert.equal(await (await app.request("http://127.0.0.1/readyz")).text(), "ready");
  const spec = await app.request("http://127.0.0.1/api/v1/openapi.json", { headers: auth });
  assert.equal(spec.status, 200);
  assert.equal((await spec.json()).openapi, "3.1.0");

  const created = await app.request("http://127.0.0.1/api/v1/namespaces", {
    method: "POST",
    headers: auth,
    body: JSON.stringify({ id: "extra", attrs: { team: "a" } }),
  });
  assert.equal(created.status, 201);
  const listed = await (await app.request("http://127.0.0.1/api/v1/namespaces", { headers: auth })).json();
  assert.equal(listed.data.map((row: { id: string }) => row.id).includes("extra"), true);
  const replaced = await app.request("http://127.0.0.1/api/v1/namespaces/extra", {
    method: "PUT",
    headers: auth,
    body: JSON.stringify({ attrs: { team: "b" }, blocklist: ["secret*"] }),
  });
  assert.equal(replaced.status, 200);
  const budget = await app.request("http://127.0.0.1/api/v1/namespaces/extra/budgets/compat", {
    method: "PUT",
    headers: auth,
    body: JSON.stringify({ limit_microdollars: 50 }),
  });
  assert.equal(budget.status, 200);
  assert.equal((await budget.json()).limit_microdollars, 50);
  const policy = await app.request("http://127.0.0.1/api/v1/namespaces/extra/budget", {
    method: "PUT",
    headers: auth,
    body: JSON.stringify({ cadence: "monthly", limit_microdollars: 80, timezone: "UTC" }),
  });
  assert.equal(policy.status, 200);
  const missingPeriod = await app.request("http://127.0.0.1/api/v1/namespaces/extra/usage", { headers: auth });
  assert.equal(missingPeriod.status, 400);
  const usage = await app.request("http://127.0.0.1/api/v1/namespaces/extra/usage?period=compat", { headers: auth });
  assert.equal(usage.status, 200);
  assert.deepEqual((await usage.json()).data, []);
  assert.equal((await app.request("http://127.0.0.1/api/v1/providers/models", { headers: auth })).status, 200);
  assert.equal((await app.request("http://127.0.0.1/api/v1/providers/fake-openai/models", { headers: auth })).status, 200);
  const credentials = await app.request("http://127.0.0.1/ns/platform/v1/credentials", { headers: auth });
  assert.equal(credentials.status, 200);
  assert.equal((await credentials.json()).data[0].credential_id, "openai");

  const removed = await app.request("http://127.0.0.1/api/v1/namespaces/extra", { method: "DELETE", headers: auth });
  assert.equal(removed.status, 204);
  const again = await app.request("http://127.0.0.1/api/v1/namespaces/extra", { method: "DELETE", headers: auth });
  assert.equal(again.status, 204);
  const configRow = await app.request("http://127.0.0.1/api/v1/namespaces/platform", { method: "DELETE", headers: auth });
  assert.equal(configRow.status, 409);
});
