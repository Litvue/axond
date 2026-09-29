import assert from "node:assert/strict";
import { createServer } from "node:http";
import test from "node:test";
import { Agent } from "undici";

import { createAdmission, defaultAdmission } from "./admission.ts";
import { createAxond } from "./app.ts";
import { isRateLimitPayload, targetAttemptCap } from "./dispatch.ts";
import { createMemoryStore } from "./memory-store.ts";
import { createMetrics } from "./metrics.ts";
import { usageEvent } from "./usage.ts";
import type { AxondOptions, Store, UsageRecord } from "@axond/sdk";

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

test("extension_metrics_stay_on_the_extension_prefix", async () => {
  const store = await seeded();
  const metrics = createMetrics([KEY]);
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    metrics,
    extensions: [
      {
        name: "counter",
        apiVersion: 1,
        stage: "pre-auth",
        async middleware(c) {
          c.var.axond.metrics.record("axond.ext.counter.hits", 1, { token: KEY });
          c.var.axond.metrics.set("axond.ext.counter.gauge", 3, { route: "list" });
          c.var.axond.metrics.record(`axond.ext.counter.${KEY}`, 1);
          assert.throws(
            () => c.var.axond.metrics.record("axond.request.count", 1),
            /axond\.ext\./,
          );
          for (let index = 0; index < 250; index += 1) {
            c.var.axond.metrics.record("axond.ext.counter.bucket", 1, { n: String(index) });
          }
          return new Response("ok");
        },
      },
    ],
  });
  const response = await app.request("http://127.0.0.1/api/v1/namespaces");
  assert.equal(await response.text(), "ok");
  const hit = metrics.points.find((point) => point.name === "axond.ext.counter.hits");
  assert.ok(hit);
  assert.equal(hit.attributes.token, undefined);
  const gauge = metrics.points.find((point) => point.name === "axond.ext.counter.gauge");
  assert.equal(gauge?.value, 3);
  assert.equal(metrics.points.some((point) => point.name.includes(KEY)), false);
  assert.ok(metrics.points.length <= 200);
  assert.equal(
    metrics.points.some((point) => point.name === "axond.ext.counter.bucket" && point.attributes.n === "0"),
    true,
  );
  assert.equal(metrics.points.some((point) => point.attributes.n === "249"), false);
});

test("a cancelled stream still settles the delivered request", async () => {
  const store = await seeded();
  const before = (await store.getBudget("platform", "compat"))!;
  const records: UsageRecord[] = [];
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.write('data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n');
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "sk-live-secret", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 2_500_000n,
        outputMicrodollarsPerMillion: 10_000_000n,
      },
    ],
    onUsage: (record) => {
      records.push(record);
    },
  });
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, messages: [] }),
    });
    assert.equal(response.status, 200);
    const reader = response.body!.getReader();
    await reader.read();
    await reader.cancel();
    for (let attempt = 0; attempt < 20 && records.length === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(records.length, 1);
    const record = records[0]!;
    assert.equal(record.status, "client_cancelled");
    assert.equal(record.credentialId, "one");
    assert.equal(record.outputTokens, 1n);
    assert.equal(record.inputTokens > 0n, true);
    assert.equal(record.costMicrodollars !== null && record.costMicrodollars > 0n, true);
    const event = JSON.stringify(usageEvent(record));
    assert.equal(event.includes("sk-live-secret"), false);
    assert.equal(event.includes('"status":"client_cancelled"'), true);
    const after = (await store.getBudget("platform", "compat"))!;
    assert.equal(after.spent - before.spent, record.costMicrodollars);
    const rows = await store.summarizeUsage("platform", "compat");
    assert.equal(rows.length, 1);
    assert.equal(rows[0]!.status, "client_cancelled");
  } finally {
    upstream.close();
  }
});

test("a stalled stream settles upstream_error for the text already relayed", async () => {
  const store = await seeded();
  const before = (await store.getBudget("platform", "compat"))!;
  const records: UsageRecord[] = [];
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.write('data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n');
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    transport: { responseHeaderTimeoutMs: 30_000, bufferedBodyTimeoutMs: 30_000, streamIdleTimeoutMs: 80, maxResponseBytes: 1024 * 1024 },
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "sk-live-secret", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 2_500_000n,
        outputMicrodollarsPerMillion: 10_000_000n,
      },
    ],
    onUsage: (record) => {
      records.push(record);
    },
  });
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, messages: [] }),
    });
    assert.equal(response.status, 200);
    const text = await response.text();
    const prefix = 'data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n';
    assert.equal(text.startsWith(prefix), true);
    assert.equal(text.includes('event: error\ndata: {"error":{"type":"upstream_stream_error"'), true);
    assert.equal(text.includes("data: [DONE]\n\n"), true);
    assert.equal(text.includes("sk-live-secret"), false);
    for (let attempt = 0; attempt < 20 && records.length === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(records.length, 1);
    const record = records[0]!;
    assert.equal(record.status, "upstream_error");
    assert.equal(record.credentialId, "one");
    assert.equal(record.outputTokens, 1n);
    assert.equal(record.inputTokens > 0n, true);
    assert.equal(record.costMicrodollars !== null && record.costMicrodollars > 0n, true);
    assert.equal(JSON.stringify(usageEvent(record)).includes("sk-live-secret"), false);
    const after = (await store.getBudget("platform", "compat"))!;
    assert.equal(after.spent - before.spent, record.costMicrodollars);
  } finally {
    upstream.close();
  }
});

test("a stream that outlives its duration settles upstream_error for the text already relayed", async () => {
  const store = await seeded();
  const before = (await store.getBudget("platform", "compat"))!;
  const records: UsageRecord[] = [];
  const metrics = createMetrics();
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.write('data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n');
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    maxStreamDurationMs: 80,
    transport: {
      responseHeaderTimeoutMs: 30_000,
      bufferedBodyTimeoutMs: 30_000,
      streamIdleTimeoutMs: 5_000,
      streamTerminalGraceMs: 5_000,
      maxResponseBytes: 1024 * 1024,
    },
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "sk-live-secret", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 2_500_000n,
        outputMicrodollarsPerMillion: 10_000_000n,
      },
    ],
    metrics,
    onUsage: (record) => {
      records.push(record);
    },
  });
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, messages: [] }),
    });
    assert.equal(response.status, 200);
    const text = await response.text();
    const prefix = 'data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n';
    assert.equal(text.startsWith(prefix), true);
    assert.equal(text.includes("stream exceeded the gateway's maximum stream duration"), true);
    assert.equal(text.includes('event: error\ndata: {"error":{"type":"upstream_stream_error"'), true);
    assert.equal(text.includes("data: [DONE]\n\n"), true);
    assert.equal(text.includes("sk-live-secret"), false);
    assert.equal(text.includes("waiting for the next provider stream chunk"), false);
    for (let attempt = 0; attempt < 20 && records.length === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(records.length, 1);
    const record = records[0]!;
    assert.equal(record.status, "upstream_error");
    assert.equal(record.credentialId, "one");
    assert.equal(record.outputTokens, 1n);
    assert.equal(record.costMicrodollars !== null && record.costMicrodollars > 0n, true);
    const after = (await store.getBudget("platform", "compat"))!;
    assert.equal(after.spent - before.spent, record.costMicrodollars);
    assert.equal(metrics.points.some((point) => point.name === "axond.upstream.timeouts"), false);
    const ttft = metrics.points.find((point) => point.name === "axond.request.time_to_first_token");
    assert.equal(ttft?.attributes["axond.status"], "upstream_error");
    assert.ok(ttft && ttft.value >= 0);
    const providerTtft = metrics.points.find((point) => point.name === "axond.upstream.time_to_first_token");
    assert.equal(providerTtft?.attributes["axond.target.provider"], "fake-openai");
    assert.equal(providerTtft?.attributes["axond.target.model"], "gpt-test");
    assert.equal(providerTtft?.attributes["axond.status"], undefined);
    const errors = metrics.points.find((point) => point.name === "axond.upstream.errors");
    assert.equal(errors?.value, 1);
    assert.equal(errors?.attributes["axond.status"], "upstream_error");
    assert.equal(errors?.attributes["axond.credential_source"], "platform");
    assert.equal(JSON.stringify(metrics.points).includes("sk-live-secret"), false);
  } finally {
    upstream.close();
  }
});

test("a stream duration after the terminal event closes as ok", async () => {
  const store = await seeded();
  const records: UsageRecord[] = [];
  const metrics = createMetrics();
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.write('data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n');
    res.write("data: [DONE]\n\n");
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    maxStreamDurationMs: 200,
    transport: {
      responseHeaderTimeoutMs: 30_000,
      bufferedBodyTimeoutMs: 30_000,
      streamIdleTimeoutMs: 5_000,
      streamTerminalGraceMs: 5_000,
      maxResponseBytes: 1024 * 1024,
    },
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "sk-live-secret", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 2_500_000n,
        outputMicrodollarsPerMillion: 10_000_000n,
      },
    ],
    metrics,
    onUsage: (record) => {
      records.push(record);
    },
  });
  try {
    const started = Date.now();
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, messages: [] }),
    });
    assert.equal(response.status, 200);
    const text = await response.text();
    assert.equal(Date.now() - started < 1_500, true);
    assert.equal(text.includes('data: {"choices":[{"delta":{"content":"Hi"}}]}'), true);
    assert.equal(text.includes("data: [DONE]\n\n"), true);
    assert.equal(text.includes("upstream_stream_error"), false);
    assert.equal(text.includes("stream exceeded"), false);
    for (let attempt = 0; attempt < 20 && records.length === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(records.length, 1);
    assert.equal(records[0]!.status, "ok");
    assert.equal(records[0]!.outputTokens, 1n);
    assert.equal(metrics.points.some((point) => point.name === "axond.upstream.timeouts"), false);
  } finally {
    upstream.close();
  }
});

test("a stream chunk past the byte cap is dropped and does not charge", async () => {
  const store = await seeded();
  const before = (await store.getBudget("platform", "compat"))!;
  const records: UsageRecord[] = [];
  const payload = 'data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n';
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.write(payload);
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    maxStreamBytes: 8,
    transport: {
      responseHeaderTimeoutMs: 30_000,
      bufferedBodyTimeoutMs: 30_000,
      streamIdleTimeoutMs: 5_000,
      maxResponseBytes: 1024 * 1024,
    },
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "sk-live-secret", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 2_500_000n,
        outputMicrodollarsPerMillion: 10_000_000n,
      },
    ],
    onUsage: (record) => {
      records.push(record);
    },
  });
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, messages: [] }),
    });
    assert.equal(response.status, 200);
    const text = await response.text();
    assert.equal(text.includes("Hi"), false);
    assert.equal(text.includes(payload), false);
    assert.equal(text.includes("stream exceeded the gateway's maximum stream size"), true);
    assert.equal(text.includes('event: error\ndata: {"error":{"type":"upstream_stream_error"'), true);
    assert.equal(text.includes("data: [DONE]\n\n"), true);
    assert.equal(text.includes("sk-live-secret"), false);
    for (let attempt = 0; attempt < 20 && records.length === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(records.length, 1);
    const record = records[0]!;
    assert.equal(record.status, "upstream_error");
    assert.equal(record.outputTokens, 0n);
    assert.equal(record.inputTokens, 0n);
    assert.equal(record.costMicrodollars, 0n);
    const after = (await store.getBudget("platform", "compat"))!;
    assert.equal(after.spent, before.spent);
  } finally {
    upstream.close();
  }
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

test("a target attempt cap of one still walks every credential", async () => {
  assert.equal(targetAttemptCap(false, undefined), 3);
  assert.equal(targetAttemptCap(false, 1), 1);
  assert.equal(targetAttemptCap(true, 9), 1);
  const store = await seeded();
  const seen: string[] = [];
  const upstream = await listen((req, res) => {
    seen.push(req.headers.authorization ?? "");
    res.writeHead(429, { "content-type": "application/json" });
    res.end('{"error":{"message":"slow down"}}');
  });
  const transport = {
    responseHeaderTimeoutMs: 5_000,
    bufferedBodyTimeoutMs: 5_000,
    streamIdleTimeoutMs: 5_000,
    maxResponseBytes: 1024,
    maxAttempts: 1,
  };
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [
      { namespace: "platform", provider: "fake-openai", secret: "key-one", id: "one" },
      { namespace: "platform", provider: "fake-openai", secret: "key-two", id: "two" },
    ],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
    transport,
  });
  const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
  const limited = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers,
    body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [] }),
  });
  assert.equal(limited.status, 502);
  assert.equal((await limited.json()).error.type, "provider_dependency_failed");
  assert.deepEqual(seen, ["Bearer key-one", "Bearer key-two"]);
  const refused = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "key-one", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
    transport: { ...transport, maxAttempts: 0 },
  });
  const before = seen.length;
  const rejected = await refused.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers,
    body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [] }),
  });
  assert.equal(rejected.status, 400);
  assert.equal((await rejected.json()).error.type, "bad_request");
  assert.equal(seen.length, before);
  upstream.close();
});

test("a provider failure records upstream_error and does not charge", async () => {
  const store = await seeded();
  const before = (await store.getBudget("platform", "compat"))!;
  const upstream = await listen((_req, res) => {
    res.writeHead(500, { "content-type": "application/json" });
    res.end('{"error":{"message":"down"}}');
  });
  const records: UsageRecord[] = [];
  const metrics = createMetrics(["sk-live-secret"]);
  try {
    const app = createAxond({
      store,
      gatewayKey: KEY,
      defaultNamespace: "platform",
      providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url, unpricedModels: "allow" }],
      credentials: [{ namespace: "platform", provider: "fake-openai", secret: "sk-live-secret", id: "cred-1" }],
      metrics,
      onUsage: (record) => {
        records.push(record);
      },
    });
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: "PROMPT_SENTINEL" }] }),
    });
    assert.equal(response.status, 502);
    const body = await response.json();
    assert.equal(body.error.type, "provider_dependency_failed");
    assert.equal(JSON.stringify(body).includes("PROMPT_SENTINEL"), false);
    assert.equal(JSON.stringify(body).includes("sk-live-secret"), false);
    for (let attempt = 0; attempt < 20 && records.length === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(records.length, 1);
    const record = records[0]!;
    assert.equal(record.status, "upstream_error");
    assert.equal(record.credentialId, "cred-1");
    assert.equal(record.credentialSource, "platform");
    assert.equal(record.costMicrodollars, 0n);
    assert.equal(record.inputTokens, 0n);
    assert.equal(record.outputTokens, 0n);
    assert.equal(record.cacheReadTokens, 0n);
    assert.equal(record.cacheWriteTokens, 0n);
    assert.equal(record.attempts, 1);
    assert.equal(record.latencyMs >= 0, true);
    const event = JSON.stringify(usageEvent(record));
    assert.equal(event.includes("sk-live-secret"), false);
    assert.equal(event.includes("PROMPT_SENTINEL"), false);
    assert.equal(event.includes('"status":"upstream_error"'), true);
    assert.equal(event.includes('"cost_microdollars":"0"'), true);
    const after = (await store.getBudget("platform", "compat"))!;
    assert.equal(after.spent, before.spent);
    const summary = await store.summarizeUsage("platform", "compat");
    assert.equal(summary.length, 1);
    assert.equal(summary[0]!.status, "upstream_error");
    assert.equal(summary[0]!.cost_microdollars, 0);
    const errors = metrics.points.find((point) => point.name === "axond.upstream.errors");
    assert.equal(errors?.value, 1);
    assert.equal(errors?.attributes["axond.status"], "upstream_error");
    assert.equal(metrics.points.some((point) => point.name === "axond.request.time_to_first_token"), false);
    assert.equal(metrics.points.some((point) => point.name === "axond.upstream.time_to_first_token"), false);
    assert.equal(JSON.stringify(metrics.points).includes("sk-live-secret"), false);
  } finally {
    upstream.close();
  }
});

test("a split stream records request ttft before the provider event is decoded", async () => {
  const store = await seeded();
  const metrics = createMetrics(["sk-live-secret"]);
  let releaseRest: () => void = () => undefined;
  const rest = new Promise<void>((resolve) => {
    releaseRest = resolve;
  });
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.flushHeaders();
    res.write('data: {"choi');
    void rest.then(() => {
      res.write('ces":[{"delta":{"content":"Hi"}}]}\n\n');
      res.write("data: [DONE]\n\n");
      res.end();
    });
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "sk-live-secret", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1_000_000n,
        outputMicrodollarsPerMillion: 1_000_000n,
      },
    ],
    metrics,
  });
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, messages: [] }),
    });
    assert.equal(response.status, 200);
    const reader = response.body!.getReader();
    const first = await reader.read();
    assert.equal(first.done, false);
    assert.equal(metrics.points.some((point) => point.name === "axond.upstream.time_to_first_token"), false);
    await new Promise((wake) => setTimeout(wake, 40));
    releaseRest();
    const chunks: Uint8Array[] = [first.value!];
    for (;;) {
      const next = await reader.read();
      if (next.done) {
        break;
      }
      chunks.push(next.value!);
    }
    const text = new TextDecoder().decode(concatChunks(chunks));
    assert.equal(text.includes('{"choices":[{"delta":{"content":"Hi"}}]}'), true);
    for (let attempt = 0; attempt < 20; attempt += 1) {
      if (metrics.points.some((point) => point.name === "axond.request.time_to_first_token")) {
        break;
      }
      await new Promise((wake) => setTimeout(wake, 10));
    }
    const requestTtft = metrics.points.find((point) => point.name === "axond.request.time_to_first_token");
    const providerTtft = metrics.points.find((point) => point.name === "axond.upstream.time_to_first_token");
    assert.equal(requestTtft?.attributes["axond.status"], "ok");
    assert.equal(providerTtft?.attributes["axond.target.provider"], "fake-openai");
    assert.equal(providerTtft?.attributes["axond.target.model"], "gpt-test");
    assert.ok(requestTtft && providerTtft && providerTtft.value >= requestTtft.value + 20);
    assert.equal(metrics.points.some((point) => point.name === "axond.upstream.errors"), false);
    assert.equal(JSON.stringify(metrics.points).includes("sk-live-secret"), false);
  } finally {
    releaseRest();
    upstream.close();
  }
});

function concatChunks(chunks: Uint8Array[]): Uint8Array {
  const size = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
  const merged = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.length;
  }
  return merged;
}

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

test("a transport error after the terminal event keeps the completed body", async () => {
  const store = await seeded();
  const records: UsageRecord[] = [];
  const metrics = createMetrics();
  const completed =
    'event: response.completed\ndata: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":3,"output_tokens":1}}}\n\n';
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.write(completed, () => {
      // Next turn: Bun drops the body if the socket resets inside this callback.
      setTimeout(() => res.destroy(), 0);
    });
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    metrics,
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "sk-live-secret", id: "one" }],
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
      streamTerminalGraceMs: 1_000,
      maxResponseBytes: 4096,
    },
    onUsage: (record) => {
      records.push(record);
    },
  });
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/responses", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, input: "hi" }),
    });
    assert.equal(response.status, 200);
    const text = await response.text();
    assert.equal(text, completed);
    assert.equal(text.includes("upstream_stream_error"), false);
    assert.equal(text.includes("sk-live-secret"), false);
    for (let attempt = 0; attempt < 20 && records.length === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(records.length, 1);
    assert.equal(records[0]!.status, "ok");
    assert.equal(records[0]!.outputTokens, 1n);
    assert.equal(metrics.points.some((point) => point.name === "axond.upstream.errors"), false);
    assert.equal(JSON.stringify(metrics.points).includes("sk-live-secret"), false);
  } finally {
    upstream.close();
  }
});

test("an incomplete tail after the terminal event is relayed through eof", async () => {
  const store = await seeded();
  const records: UsageRecord[] = [];
  const terminal =
    'event: response.completed\ndata: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":3,"output_tokens":1}}}\n\n';
  const wire = Buffer.concat([
    Buffer.from(terminal),
    Buffer.from("event: provider.extension\ndata: "),
    Buffer.from([0xf0]),
  ]);
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end(wire);
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "sk-live-secret", id: "one" }],
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
      streamTerminalGraceMs: 1_000,
      maxResponseBytes: 4096,
    },
    onUsage: (record) => {
      records.push(record);
    },
  });
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/responses", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, input: "hi" }),
    });
    assert.equal(response.status, 200);
    const body = Buffer.from(await response.arrayBuffer());
    assert.deepEqual(body, wire);
    assert.equal(body.includes(Buffer.from("sk-live-secret")), false);
    for (let attempt = 0; attempt < 20 && records.length === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(records.length, 1);
    assert.equal(records[0]!.status, "ok");
    assert.equal(records[0]!.outputTokens, 1n);
  } finally {
    upstream.close();
  }
});

const MESSAGES_TERMINAL = [
  'event: message_start\ndata: {"type":"message_start","message":{"usage":{"input_tokens":3,"output_tokens":0}}}\n\n',
  'event: message_delta\ndata: {"type":"message_delta","usage":{"output_tokens":1}}\n\n',
  'event: message_stop\ndata: {"type":"message_stop"}\n\n',
].join("");

function messagesApp(
  store: Store,
  baseUrl: string,
  records: UsageRecord[],
  metrics?: ReturnType<typeof createMetrics>,
) {
  return createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    metrics,
    providers: [{ id: "fake-anthropic", kind: "anthropic", baseUrl }],
    credentials: [{ namespace: "platform", provider: "fake-anthropic", secret: "sk-live-secret", id: "one" }],
    prices: [
      {
        provider: "fake-anthropic",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
    transport: {
      responseHeaderTimeoutMs: 1_000,
      bufferedBodyTimeoutMs: 1_000,
      streamIdleTimeoutMs: 5_000,
      streamTerminalGraceMs: 1_000,
      maxResponseBytes: 4096,
    },
    onUsage: (record) => {
      records.push(record);
    },
  });
}

test("a transport error after message_stop keeps the completed body", async () => {
  const store = await seeded();
  const records: UsageRecord[] = [];
  const metrics = createMetrics();
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.write(MESSAGES_TERMINAL, () => {
      // Next turn: Bun drops the body if the socket resets inside this callback.
      setTimeout(() => res.destroy(), 0);
    });
  });
  const app = messagesApp(store, upstream.url, records, metrics);
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/messages", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({
        model: "fake-anthropic/claude-test",
        stream: true,
        messages: [],
        max_tokens: 16,
      }),
    });
    assert.equal(response.status, 200);
    const text = await response.text();
    assert.equal(text, MESSAGES_TERMINAL);
    assert.equal(text.includes("upstream_stream_error"), false);
    assert.equal(text.includes("sk-live-secret"), false);
    for (let attempt = 0; attempt < 20 && records.length === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(records.length, 1);
    assert.equal(records[0]!.status, "ok");
    assert.equal(records[0]!.inputTokens, 3n);
    assert.equal(records[0]!.outputTokens, 1n);
    assert.equal(metrics.points.some((point) => point.name === "axond.upstream.errors"), false);
    assert.equal(JSON.stringify(metrics.points).includes("sk-live-secret"), false);
  } finally {
    upstream.close();
  }
});

test("an incomplete tail after message_stop is relayed through eof", async () => {
  const store = await seeded();
  const records: UsageRecord[] = [];
  const wire = Buffer.concat([
    Buffer.from(MESSAGES_TERMINAL),
    Buffer.from("event: provider.extension\ndata: "),
    Buffer.from([0xf0]),
  ]);
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end(wire);
  });
  const app = messagesApp(store, upstream.url, records);
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/messages", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({
        model: "fake-anthropic/claude-test",
        stream: true,
        messages: [],
        max_tokens: 16,
      }),
    });
    assert.equal(response.status, 200);
    const body = Buffer.from(await response.arrayBuffer());
    assert.deepEqual(body, wire);
    assert.equal(body.includes(Buffer.from("sk-live-secret")), false);
    for (let attempt = 0; attempt < 20 && records.length === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(records.length, 1);
    assert.equal(records[0]!.status, "ok");
    assert.equal(records[0]!.outputTokens, 1n);
  } finally {
    upstream.close();
  }
});

test(
  "a connect timeout is upstream_timeout and hides the address",
  { skip: process.versions.bun !== undefined && "Bun fetch does not enforce connect_timeout_ms" },
  async () => {
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

test("isRateLimitPayload matches explicit provider markers", () => {
  assert.equal(isRateLimitPayload('{"error":{"type":"rate_limit_exceeded"}}'), true);
  assert.equal(isRateLimitPayload('{"error":{"code":"rate_limit"}}'), true);
  assert.equal(isRateLimitPayload('{"type":"error","status":429}'), true);
  assert.equal(isRateLimitPayload('{"error":{"code":429}}'), true);
  assert.equal(isRateLimitPayload('{"choices":[{"delta":{"content":"a"}}]}'), false);
  assert.equal(isRateLimitPayload('{"error":{"message":"other"}}'), false);
  assert.equal(isRateLimitPayload("not json"), false);
});

function poolApp(store: Store, url: string, extra: Partial<AxondOptions> = {}) {
  return createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: url }],
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
    ...extra,
  });
}

const CHAT_HEADERS = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };

test("an openai chat rate limit before content rotates and is not forwarded", async () => {
  const store = await seeded();
  const seen: string[] = [];
  let settlements = 0;
  let servedCredential = "";
  const upstream = await listen((req, res) => {
    const authorization = req.headers.authorization ?? "";
    seen.push(authorization);
    res.writeHead(200, { "content-type": "text/event-stream" });
    if (authorization.includes("bad-key")) {
      res.end('data: {"error":{"type":"rate_limit_exceeded"}}\n\n');
      return;
    }
    res.end('data: {"choices":[{"delta":{"content":"b"}}]}\n\ndata: [DONE]\n\n');
  });
  const app = poolApp(store, upstream.url, {
    credentialPool: { failureThreshold: 1, cooldownMs: 30_000 },
    onUsage: (record) => {
      settlements += 1;
      servedCredential = `${record.credentialSource}:${record.credentialId}:${record.attempts}:${record.period}`;
      assert.equal(record.latencyMs >= 0, true);
      assert.equal(record.traceId, null);
    },
  });
  const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: CHAT_HEADERS,
    body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, messages: [] }),
  });
  assert.equal(response.status, 200);
  const body = await response.text();
  assert.match(body, /"content":"b"/);
  assert.match(body, /\[DONE\]/);
  assert.equal(body.includes("rate_limit_exceeded"), false);
  assert.deepEqual(seen, ["Bearer bad-key", "Bearer good-key"]);
  assert.equal(settlements, 1);
  assert.equal(servedCredential, "platform:good:1:compat");
  const status = await app.request("http://127.0.0.1/ns/platform/v1/credentials", { headers: CHAT_HEADERS });
  const rows = (await status.json()).data as { credential_id: string; state: string }[];
  assert.equal(rows.find((row) => row.credential_id === "bad")?.state, "parked");
  assert.equal(rows.find((row) => row.credential_id === "good")?.state, "healthy");
  upstream.close();
});

test("a split rate-limit frame before content rotates without leaking the prefix", async () => {
  const store = await seeded();
  const seen: string[] = [];
  const upstream = await listen((req, res) => {
    const authorization = req.headers.authorization ?? "";
    seen.push(authorization);
    res.writeHead(200, { "content-type": "text/event-stream" });
    if (authorization.includes("bad-key")) {
      res.write('data: {"error":');
      setTimeout(() => {
        res.end('{"type":"rate_limit_exceeded"}}\n\n');
      }, 40);
      return;
    }
    res.end('data: {"choices":[{"delta":{"content":"b"}}]}\n\ndata: [DONE]\n\n');
  });
  const app = poolApp(store, upstream.url);
  const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: CHAT_HEADERS,
    body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, messages: [] }),
  });
  assert.equal(response.status, 200);
  const body = await response.text();
  assert.match(body, /"content":"b"/);
  assert.equal(body.includes("rate_limit_exceeded"), false);
  assert.equal(body.includes('data: {"error":'), false);
  assert.deepEqual(seen, ["Bearer bad-key", "Bearer good-key"]);
  const status = await app.request("http://127.0.0.1/ns/platform/v1/credentials", { headers: CHAT_HEADERS });
  const rows = (await status.json()).data as { credential_id: string; state: string }[];
  assert.equal(rows.find((row) => row.credential_id === "bad")?.state, "healthy");
  upstream.close();
});

test("a rate limit after chat content stays on that stream", async () => {
  const store = await seeded();
  const seen: string[] = [];
  const upstream = await listen((req, res) => {
    seen.push(req.headers.authorization ?? "");
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end('data: {"choices":[{"delta":{"content":"a"}}]}\n\ndata: {"error":{"type":"rate_limit_exceeded"}}\n\n');
  });
  const app = poolApp(store, upstream.url, {
    credentialPool: { failureThreshold: 1, cooldownMs: 30_000 },
  });
  const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: CHAT_HEADERS,
    body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, messages: [] }),
  });
  assert.equal(response.status, 200);
  const body = await response.text();
  assert.match(body, /"content":"a"/);
  assert.match(body, /rate_limit_exceeded/);
  assert.deepEqual(seen, ["Bearer bad-key"]);
  const status = await app.request("http://127.0.0.1/ns/platform/v1/credentials", { headers: CHAT_HEADERS });
  const rows = (await status.json()).data as { credential_id: string; state: string }[];
  assert.equal(rows.find((row) => row.credential_id === "bad")?.state, "parked");
  upstream.close();
});

test("a rate limit after the chat terminal frame does not park the credential", async () => {
  const store = await seeded();
  const seen: string[] = [];
  const upstream = await listen((req, res) => {
    seen.push(req.headers.authorization ?? "");
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end('data: {"choices":[{"delta":{"content":"a"}}]}\n\ndata: [DONE]\n\ndata: {"error":{"type":"rate_limit_exceeded"}}\n\n');
  });
  const app = poolApp(store, upstream.url, {
    credentialPool: { failureThreshold: 1, cooldownMs: 30_000 },
  });
  const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: CHAT_HEADERS,
    body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, messages: [] }),
  });
  assert.equal(response.status, 200);
  const body = await response.text();
  assert.match(body, /"content":"a"/);
  assert.match(body, /\[DONE\]/);
  assert.match(body, /rate_limit_exceeded/);
  assert.deepEqual(seen, ["Bearer bad-key"]);
  const status = await app.request("http://127.0.0.1/ns/platform/v1/credentials", { headers: CHAT_HEADERS });
  const rows = (await status.json()).data as { credential_id: string; state: string }[];
  assert.equal(rows.find((row) => row.credential_id === "bad")?.state, "healthy");
  upstream.close();
});

test("a responses stream does not rotate on a rate-limit event", async () => {
  const store = await seeded();
  const seen: string[] = [];
  const upstream = await listen((req, res) => {
    seen.push(req.headers.authorization ?? "");
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end('data: {"type":"error","error":{"type":"rate_limit_exceeded"}}\n\n');
  });
  const app = poolApp(store, upstream.url, {
    credentialPool: { failureThreshold: 1, cooldownMs: 30_000 },
  });
  const response = await app.request("http://127.0.0.1/ns/platform/v1/responses", {
    method: "POST",
    headers: CHAT_HEADERS,
    body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, input: "hi" }),
  });
  assert.equal(response.status, 200);
  const body = await response.text();
  assert.match(body, /rate_limit_exceeded/);
  assert.deepEqual(seen, ["Bearer bad-key"]);
  const status = await app.request("http://127.0.0.1/ns/platform/v1/credentials", { headers: CHAT_HEADERS });
  const rows = (await status.json()).data as { credential_id: string; state: string }[];
  assert.equal(rows.find((row) => row.credential_id === "bad")?.state, "parked");
  upstream.close();
});

test("a native messages stream does not rotate on a rate-limit event", async () => {
  const store = await seeded();
  const seen: string[] = [];
  const upstream = await listen((req, res) => {
    const key = req.headers["x-api-key"];
    seen.push(typeof key === "string" ? key : "");
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end('event: error\ndata: {"type":"error","error":{"type":"rate_limit_error"}}\n\n');
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-anthropic", kind: "anthropic", baseUrl: upstream.url }],
    credentials: [
      { namespace: "platform", provider: "fake-anthropic", secret: "bad-key", id: "bad" },
      { namespace: "platform", provider: "fake-anthropic", secret: "good-key", id: "good" },
    ],
    credentialPool: { failureThreshold: 1, cooldownMs: 30_000 },
    prices: [
      {
        provider: "fake-anthropic",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
  });
  const response = await app.request("http://127.0.0.1/ns/platform/v1/messages", {
    method: "POST",
    headers: CHAT_HEADERS,
    body: JSON.stringify({ model: "fake-anthropic/claude-test", stream: true, messages: [], max_tokens: 16 }),
  });
  assert.equal(response.status, 200);
  const body = await response.text();
  assert.match(body, /rate_limit_error/);
  assert.deepEqual(seen, ["bad-key"]);
  const status = await app.request("http://127.0.0.1/ns/platform/v1/credentials", { headers: CHAT_HEADERS });
  const rows = (await status.json()).data as { credential_id: string; state: string }[];
  assert.equal(rows.find((row) => row.credential_id === "bad")?.state, "parked");
  upstream.close();
});

function estimateCost(body: Record<string, unknown>, embeddings = false): bigint {
  const input = BigInt(Math.floor(new TextEncoder().encode(JSON.stringify(body)).length / 4));
  const output = embeddings ? 0n : BigInt(typeof body["max_tokens"] === "number" ? body["max_tokens"] : 1024);
  return input + output;
}

test("a spend cap refuses the estimate before the provider is called", async () => {
  const store = await seeded();
  let hits = 0;
  const upstream = await listen((_req, res) => {
    hits += 1;
    res.writeHead(200, { "content-type": "application/json" });
    res.end('{"usage":{"prompt_tokens":1,"completion_tokens":1}}');
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
        inputMicrodollarsPerMillion: 1_000_000n,
        outputMicrodollarsPerMillion: 1_000_000n,
      },
    ],
    extensions: [
      {
        name: "cap",
        apiVersion: 1,
        stage: "pre-auth",
        async middleware(c, next) {
          const cap = c.req.header("x-test-cap");
          if (cap) {
            c.get("axond").spendCapMicrodollars = BigInt(cap);
          }
          await next();
        },
      },
    ],
  });
  const payload = { model: "fake-openai/gpt-test", messages: [{ role: "user", content: "PROMPT_SENTINEL" }], max_tokens: 4 };
  const estimated = estimateCost(payload);
  const refused = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: { ...CHAT_HEADERS, "x-test-cap": String(estimated - 1n) },
    body: JSON.stringify(payload),
  });
  assert.equal(refused.status, 403);
  const error = await refused.json();
  assert.equal(error.error.type, "request_cost_ceiling_exceeded");
  assert.equal(
    error.error.message,
    `request cost ceiling exceeded for model \`fake-openai/gpt-test\`: estimated ${estimated} microdollars exceeds the per-request ceiling of ${estimated - 1n} microdollars`,
  );
  assert.equal(JSON.stringify(error).includes("PROMPT_SENTINEL"), false);
  assert.equal(hits, 0);
  const allowed = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: { ...CHAT_HEADERS, "x-test-cap": String(estimated) },
    body: JSON.stringify(payload),
  });
  assert.equal(allowed.status, 200);
  await allowed.text();
  assert.equal(hits, 1);
  const embedding = { model: "fake-openai/gpt-test", input: "PROMPT_SENTINEL", max_tokens: 100_000 };
  const embeddingCost = estimateCost(embedding, true);
  const embedded = await app.request("http://127.0.0.1/ns/platform/v1/embeddings", {
    method: "POST",
    headers: { ...CHAT_HEADERS, "x-test-cap": String(embeddingCost) },
    body: JSON.stringify(embedding),
  });
  assert.equal(embedded.status, 200);
  await embedded.text();
  assert.equal(hits, 2);
  upstream.close();
});

test("a saturated replica sheds before the provider and a stream slot is separate", async () => {
  const store = await seeded();
  let hits = 0;
  let releaseUpstream: () => void = () => undefined;
  const upstream = await listen((_req, res) => {
    hits += 1;
    if (hits === 1) {
      void new Promise<void>((resolve) => {
        releaseUpstream = () => {
          releaseUpstream = () => undefined;
          res.writeHead(200, { "content-type": "application/json" });
          res.end('{"usage":{"prompt_tokens":1,"completion_tokens":1}}');
          resolve();
        };
      });
      return;
    }
    res.writeHead(200, { "content-type": "application/json" });
    res.end('{"usage":{"prompt_tokens":1,"completion_tokens":1}}');
  });
  const metrics = createMetrics(["sk-live-secret"]);
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    maxInFlight: 1,
    maxInFlightStreams: 1,
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "sk-live-secret", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1_000_000n,
        outputMicrodollarsPerMillion: 1_000_000n,
      },
    ],
    metrics,
  });
  const payload = JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: "hi" }] });
  try {
    const first = app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: CHAT_HEADERS,
      body: payload,
    });
    for (let attempt = 0; attempt < 50 && hits === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(hits, 1);
    const anonymous = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: payload,
    });
    assert.equal(anonymous.status, 401);
    await anonymous.text();
    const shed = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: CHAT_HEADERS,
      body: payload,
    });
    assert.equal(shed.status, 503);
    assert.equal(shed.headers.get("retry-after"), "1");
    const body = await shed.json() as { error: { type: string; message: string } };
    assert.equal(body.error.type, "gateway_overloaded");
    assert.equal(body.error.message.includes("sk-live-secret"), false);
    assert.equal(hits, 1);
    const rejection = metrics.points.find((point) => point.name === "axond.admission.rejections");
    assert.equal(rejection?.attributes["axond.error.type"], "gateway_overloaded");
    assert.equal(rejection?.attributes["axond.admission.resource"], "request");
    releaseUpstream();
    const served = await first;
    assert.equal(served.status, 200);
    await served.text();
    const again = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: CHAT_HEADERS,
      body: payload,
    });
    assert.equal(again.status, 200);
    await again.text();
    assert.equal(hits, 2);
  } finally {
    releaseUpstream();
    upstream.close();
  }
});

test("one stream slot sheds the next stream and still serves a buffered request", async () => {
  const store = await seeded();
  let hits = 0;
  const upstream = await listen((req, res) => {
    hits += 1;
    const chunks: Buffer[] = [];
    req.on("data", (chunk: Buffer) => chunks.push(chunk));
    req.on("end", () => {
      const body = Buffer.concat(chunks).toString("utf8");
      if (body.includes('"stream":true')) {
        res.writeHead(200, { "content-type": "text/event-stream" });
        res.write('data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n');
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
    maxInFlight: 4,
    maxInFlightStreams: 1,
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "sk-live-secret", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1_000_000n,
        outputMicrodollarsPerMillion: 1_000_000n,
      },
    ],
  });
  const streamBody = JSON.stringify({
    model: "fake-openai/gpt-test",
    messages: [{ role: "user", content: "hi" }],
    stream: true,
  });
  const bufferedBody = JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: "hi" }] });
  try {
    const first = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: CHAT_HEADERS,
      body: streamBody,
    });
    assert.equal(first.status, 200);
    const shed = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: CHAT_HEADERS,
      body: streamBody,
    });
    assert.equal(shed.status, 503);
    assert.equal(shed.headers.get("retry-after"), "1");
    const shedBody = await shed.json() as { error: { type: string; message: string } };
    assert.equal(shedBody.error.type, "stream_capacity_exhausted");
    assert.equal(shedBody.error.message.includes("sk-live-secret"), false);
    assert.equal(hits, 1);
    const buffered = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: CHAT_HEADERS,
      body: bufferedBody,
    });
    assert.equal(buffered.status, 200);
    await buffered.text();
    assert.equal(hits, 2);
    await first.body?.cancel();
    const after = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: CHAT_HEADERS,
      body: streamBody,
    });
    assert.equal(after.status, 200);
    await after.body?.cancel();
  } finally {
    upstream.close();
  }
});

test("a full admission queue sheds immediately and an expired wait is typed", async () => {
  const store = await seeded();
  let hits = 0;
  let releaseUpstream: () => void = () => undefined;
  const upstream = await listen((_req, res) => {
    hits += 1;
    if (hits === 1) {
      void new Promise<void>((resolve) => {
        releaseUpstream = () => {
          releaseUpstream = () => undefined;
          res.writeHead(200, { "content-type": "application/json" });
          res.end('{"usage":{"prompt_tokens":1,"completion_tokens":1}}');
          resolve();
        };
      });
      return;
    }
    res.writeHead(200, { "content-type": "application/json" });
    res.end('{"usage":{"prompt_tokens":1,"completion_tokens":1}}');
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    maxInFlight: 1,
    admissionQueueCapacity: 1,
    admissionQueueWaitMs: 400,
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "sk-live-secret", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1_000_000n,
        outputMicrodollarsPerMillion: 1_000_000n,
      },
    ],
  });
  const payload = JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: "hi" }] });
  try {
    const first = app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: CHAT_HEADERS,
      body: payload,
    });
    for (let attempt = 0; attempt < 50 && hits === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(hits, 1);
    const queued = app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: CHAT_HEADERS,
      body: payload,
    });
    await new Promise((wake) => setTimeout(wake, 40));
    const full = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: CHAT_HEADERS,
      body: payload,
    });
    assert.equal(full.status, 503);
    const fullBody = await full.json() as { error: { type: string } };
    assert.equal(fullBody.error.type, "admission_queue_full");
    assert.equal(full.headers.get("retry-after"), "1");
    assert.equal(hits, 1);
    const expired = await queued;
    assert.equal(expired.status, 503);
    const expiredBody = await expired.json() as { error: { type: string; message: string } };
    assert.equal(expiredBody.error.type, "admission_queue_timeout");
    assert.equal(expiredBody.error.message.includes("sk-live-secret"), false);
    releaseUpstream();
    const served = await first;
    assert.equal(served.status, 200);
    await served.text();
  } finally {
    releaseUpstream();
    upstream.close();
  }
});

test("unsettled charges shed the next request before the provider", async () => {
  const store = await seeded();
  let hits = 0;
  let releaseSettle: () => void = () => undefined;
  const gate = new Promise<void>((resolve) => {
    releaseSettle = resolve;
  });
  const original = store.settle.bind(store);
  store.settle = async (input) => {
    await gate;
    return original(input);
  };
  const upstream = await listen((_req, res) => {
    hits += 1;
    res.writeHead(200, { "content-type": "application/json" });
    res.end('{"usage":{"prompt_tokens":1,"completion_tokens":1}}');
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    maxInFlight: 1,
    maxPendingSettlements: 1,
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "sk-live-secret", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1_000_000n,
        outputMicrodollarsPerMillion: 1_000_000n,
      },
    ],
  });
  const payload = JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: "hi" }] });
  try {
    const first = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: CHAT_HEADERS,
      body: payload,
    });
    assert.equal(first.status, 200);
    await first.text();
    const shed = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: CHAT_HEADERS,
      body: payload,
    });
    assert.equal(shed.status, 503);
    assert.equal(shed.headers.get("retry-after"), "1");
    const body = await shed.json() as { error: { type: string; message: string } };
    assert.equal(body.error.type, "settlement_capacity_exhausted");
    assert.equal(body.error.message.includes("sk-live-secret"), false);
    assert.equal(hits, 1);
    releaseSettle();
    for (let attempt = 0; attempt < 20; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    const again = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: CHAT_HEADERS,
      body: payload,
    });
    assert.equal(again.status, 200);
    await again.text();
    assert.equal(hits, 2);
  } finally {
    releaseSettle();
    upstream.close();
  }
});

test("a charge that misses the settlement execution queue is dropped", async () => {
  const store = await seeded();
  const before = (await store.getBudget("platform", "compat"))!;
  let entered = 0;
  let releaseSettle: () => void = () => undefined;
  const gate = new Promise<void>((resolve) => {
    releaseSettle = resolve;
  });
  const original = store.settle.bind(store);
  store.settle = async (input) => {
    entered += 1;
    await gate;
    return original(input);
  };
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "application/json" });
    res.end('{"usage":{"prompt_tokens":4,"completion_tokens":4}}');
  });
  const metrics = createMetrics(["sk-live-secret"]);
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    maxInFlight: 4,
    maxPendingSettlements: 4,
    maxInFlightSettlements: 1,
    settlementQueueWaitMs: 80,
    settlementTimeoutMs: 0,
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "sk-live-secret", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1_000_000n,
        outputMicrodollarsPerMillion: 1_000_000n,
      },
    ],
    metrics,
  });
  const payload = JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: "hi" }] });
  try {
    const first = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: CHAT_HEADERS,
      body: payload,
    });
    assert.equal(first.status, 200);
    await first.text();
    for (let attempt = 0; attempt < 50 && entered === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(entered, 1);
    const second = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: CHAT_HEADERS,
      body: payload,
    });
    assert.equal(second.status, 200);
    await second.text();
    await new Promise((wake) => setTimeout(wake, 150));
    assert.equal(entered, 1);
    const mid = (await store.getBudget("platform", "compat"))!;
    assert.equal(mid.spent, before.spent);
    assert.equal((await store.summarizeUsage("platform", "compat")).length, 0);
    const dropped = metrics.points.find(
      (point) =>
        point.name === "axond.settlement.failures" && point.attributes["axond.settlement.reason"] === "queue_timeout",
    );
    assert.equal(dropped?.value, 1);
    assert.equal(JSON.stringify(metrics.points).includes("sk-live-secret"), false);
    releaseSettle();
    let after = mid;
    for (let attempt = 0; attempt < 30; attempt += 1) {
      after = (await store.getBudget("platform", "compat"))!;
      if (after.spent !== before.spent) {
        break;
      }
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(after.spent - before.spent, 8n);
    assert.equal(entered, 1);
    const rows = await store.summarizeUsage("platform", "compat");
    assert.equal(rows.length, 1);
    assert.equal(rows[0]?.count, 1);
    const held = metrics.points.find(
      (point) =>
        point.name === "axond.admission.in_flight" && point.attributes["axond.admission.resource"] === "settlement",
    );
    assert.equal(held?.value, 0);
  } finally {
    releaseSettle();
    upstream.close();
  }
});

test("a settlement that outlives its deadline still records the charge", async () => {
  const store = await seeded();
  const before = (await store.getBudget("platform", "compat"))!;
  const original = store.settle.bind(store);
  store.settle = async (input) => {
    await new Promise((wake) => setTimeout(wake, 80));
    return original(input);
  };
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "application/json" });
    res.end('{"usage":{"prompt_tokens":4,"completion_tokens":4}}');
  });
  const metrics = createMetrics(["sk-live-secret"]);
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    maxInFlight: 4,
    maxInFlightSettlements: 1,
    settlementTimeoutMs: 30,
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "sk-live-secret", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1_000_000n,
        outputMicrodollarsPerMillion: 1_000_000n,
      },
    ],
    metrics,
  });
  const payload = JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: "hi" }] });
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: CHAT_HEADERS,
      body: payload,
    });
    assert.equal(response.status, 200);
    await response.text();
    let after = before;
    for (let attempt = 0; attempt < 40; attempt += 1) {
      after = (await store.getBudget("platform", "compat"))!;
      if (after.spent !== before.spent) {
        break;
      }
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(after.spent - before.spent, 8n);
    const timed = metrics.points.find(
      (point) =>
        point.name === "axond.settlement.failures" &&
        point.attributes["axond.settlement.reason"] === "execution_timeout",
    );
    assert.equal(timed?.value, 1);
    assert.equal(JSON.stringify(metrics.points).includes("sk-live-secret"), false);
    const executing = metrics.points.find(
      (point) =>
        point.name === "axond.settlement.in_flight" && point.attributes["axond.settlement.stage"] === "executing",
    );
    assert.equal(executing?.value, 0);
  } finally {
    upstream.close();
  }
});

test("an admitted request holds a reserved settlement until the charge is spawned", async () => {
  const admission = createAdmission({
    ...defaultAdmission(),
    maxInFlight: 4,
    maxPendingSettlements: 4,
  });
  const metrics = createMetrics(["sk-live-secret"]);
  const store = await seeded();
  let releaseUpstream: () => void = () => undefined;
  const upstream = await listen((_req, res) => {
    void new Promise<void>((resolve) => {
      releaseUpstream = () => {
        releaseUpstream = () => undefined;
        res.writeHead(200, { "content-type": "application/json" });
        res.end('{"usage":{"prompt_tokens":1,"completion_tokens":1}}');
        resolve();
      };
    });
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    admissionControl: admission,
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "sk-live-secret", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1_000_000n,
        outputMicrodollarsPerMillion: 1_000_000n,
      },
    ],
    metrics,
  });
  const payload = JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: "hi" }] });
  const reserved = () =>
    metrics.points.find(
      (point) =>
        point.name === "axond.settlement.in_flight" && point.attributes["axond.settlement.stage"] === "reserved",
    );
  try {
    const pending = app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: CHAT_HEADERS,
      body: payload,
    });
    for (let attempt = 0; attempt < 50 && (reserved()?.value ?? 0) !== 1; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(reserved()?.value, 1);
    assert.equal(admission.inFlightRequests(), 1);
    const anonymous = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: payload,
    });
    assert.equal(anonymous.status, 401);
    await anonymous.text();
    assert.equal(reserved()?.value, 1);
    assert.equal(JSON.stringify(metrics.points).includes("sk-live-secret"), false);
    releaseUpstream();
    const response = await pending;
    assert.equal(response.status, 200);
    await response.text();
    for (let attempt = 0; attempt < 30 && (reserved()?.value ?? 1) !== 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(reserved()?.value, 0);
    assert.equal(admission.inFlightRequests(), 0);
  } finally {
    releaseUpstream();
    upstream.close();
  }
});

test("a spawned charge's age climbs until the charge finishes", async () => {
  const admission = createAdmission({
    ...defaultAdmission(),
    maxInFlight: 4,
    maxPendingSettlements: 4,
    settlementTimeoutMs: 0,
  });
  const metrics = createMetrics(["sk-live-secret"]);
  const store = await seeded();
  let releaseSettle: () => void = () => undefined;
  const gate = new Promise<void>((resolve) => {
    releaseSettle = resolve;
  });
  const original = store.settle.bind(store);
  store.settle = async (input) => {
    await gate;
    return original(input);
  };
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "application/json" });
    res.end('{"usage":{"prompt_tokens":4,"completion_tokens":4}}');
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    admissionControl: admission,
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "sk-live-secret", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1_000_000n,
        outputMicrodollarsPerMillion: 1_000_000n,
      },
    ],
    metrics,
  });
  const payload = JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: "hi" }] });
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: CHAT_HEADERS,
      body: payload,
    });
    assert.equal(response.status, 200);
    await response.text();
    for (let attempt = 0; attempt < 50 && admission.oldestPendingAgeMs() === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    const first = admission.oldestPendingAgeMs();
    assert.ok(first >= 0);
    assert.ok(admission.oldestPendingAgeMs() >= first);
    await new Promise((wake) => setTimeout(wake, 40));
    const later = admission.oldestPendingAgeMs();
    assert.ok(later > first, `age ${later} did not pass ${first}`);
    admission.observeAge(metrics);
    const aged = metrics.points.find((point) => point.name === "axond.settlement.oldest_pending_age");
    assert.ok((aged?.value ?? 0) >= 30);
    assert.equal(JSON.stringify(metrics.points).includes("sk-live-secret"), false);
    const waiting = admission.awaitIdle(30);
    const started = Date.now();
    const backlog = await waiting;
    assert.ok(Date.now() - started >= 20);
    assert.equal(backlog.spawned, 1);
    assert.ok(backlog.oldestAgeMs >= 40);
    releaseSettle();
    const idle = await admission.awaitIdle(500);
    assert.equal(idle.spawned, 0);
    assert.equal(idle.oldestAgeMs, 0);
    const cleared = metrics.points.find((point) => point.name === "axond.settlement.oldest_pending_age");
    assert.equal(cleared?.value, 0);
  } finally {
    releaseSettle();
    upstream.close();
  }
});
