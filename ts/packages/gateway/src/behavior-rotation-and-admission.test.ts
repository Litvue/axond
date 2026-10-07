import assert from "node:assert/strict";

import { createServer } from "node:http";

import { createServer as createNetServer } from "node:net";

import test from "node:test";

import { Agent } from "undici";


import { createAdmission, defaultAdmission } from "./admission.ts";

import { createAxond } from "./app.ts";

import { callUpstream, chatRateLimitFailure, classifyUpstream, failoverDeadline, isRateLimitPayload, targetAttemptCap, transportFailureReason } from "./dispatch.ts";

import { StoreFailure } from "./errors.ts";

import { createMemoryStore } from "./memory-store.ts";

import { createMetrics } from "./metrics.ts";

import { usageEvent, usageLine } from "./usage.ts";

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


const MINTED_REQUEST_ID = /^req_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;


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


const MESSAGES_TERMINAL = [
  'event: message_start\ndata: {"type":"message_start","message":{"usage":{"input_tokens":3,"output_tokens":0}}}\n\n',
  'event: message_delta\ndata: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}\n\n',
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


const CHAT_DONE = 'data: {"choices":[{"delta":{"content":"Hi"}}]}\n\ndata: [DONE]\n\n';


function chatDoneApp(store: Store, baseUrl: string, records: UsageRecord[], metrics?: ReturnType<typeof createMetrics>) {
  return createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    metrics,
    providers: [{ id: "fake-openai", kind: "openai", baseUrl }],
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
}


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


function estimateCost(body: Record<string, unknown>, embeddings = false): bigint {
  const input = BigInt(Math.floor(new TextEncoder().encode(JSON.stringify(body)).length / 4));
  const output = embeddings ? 0n : BigInt(typeof body["max_tokens"] === "number" ? body["max_tokens"] : 1024);
  return input + output;
}


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


test("chatRateLimitFailure uses the provider message and bounds it", () => {
  assert.equal(
    chatRateLimitFailure('{"error":{"type":"rate_limit_exceeded"}}'),
    "provider stream was rate limited: OpenAI stream rate limited",
  );
  assert.equal(
    chatRateLimitFailure('{"error":{"type":"rate_limit_exceeded","message":"slow down"}}'),
    "provider stream was rate limited: slow down",
  );
  assert.equal(
    chatRateLimitFailure('{"error":{"message":""}}'),
    "provider stream was rate limited: ",
  );
  const bounded = chatRateLimitFailure(JSON.stringify({ error: { message: "€".repeat(4096) } }));
  const prefix = "provider stream was rate limited: ";
  const marker = "… [truncated]";
  assert.equal(bounded.startsWith(prefix), true);
  assert.equal(bounded.endsWith(marker), true);
  const kept = bounded.slice(prefix.length, -marker.length);
  assert.equal(kept.length > 0 && [...kept].every((character) => character === "€"), true);
  assert.ok(
    new TextEncoder().encode(bounded).length
      <= new TextEncoder().encode(prefix).length + 4096 + new TextEncoder().encode(marker).length,
  );
});


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


test("a chat rate limit before content on one credential fails the stream", async () => {
  const store = await seeded();
  const seen: string[] = [];
  const records: UsageRecord[] = [];
  const upstreamBytes = 'data: {"error":{"type":"rate_limit_exceeded","message":"slow down"}}\n\n';
  const upstream = await listen((req, res) => {
    seen.push(req.headers.authorization ?? "");
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end(upstreamBytes);
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "only-key", id: "only" }],
    credentialPool: { failureThreshold: 1, cooldownMs: 30_000 },
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
    onUsage: (record) => {
      records.push(record);
    },
  });
  const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: CHAT_HEADERS,
    body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, messages: [] }),
  });
  assert.equal(response.status, 200);
  const body = await response.text();
  assert.equal(body.startsWith(upstreamBytes), true);
  assert.match(body, /provider stream was rate limited: slow down/);
  assert.match(body, /data: \[DONE\]/);
  assert.equal(body.includes("only-key"), false);
  assert.equal(body.includes(KEY), false);
  assert.deepEqual(seen, ["Bearer only-key"]);
  for (let attempt = 0; attempt < 20 && records.length === 0; attempt += 1) {
    await new Promise((wake) => setTimeout(wake, 10));
  }
  assert.equal(records.length, 1);
  assert.equal(records[0]!.status, "upstream_error");
  const status = await app.request("http://127.0.0.1/ns/platform/v1/credentials", { headers: CHAT_HEADERS });
  const rows = (await status.json()).data as { credential_id: string; state: string }[];
  assert.equal(rows.find((row) => row.credential_id === "only")?.state, "parked");
  upstream.close();
});


test("a chat rate limit with no credential left fails the stream", async () => {
  const store = await seeded();
  const seen: string[] = [];
  const records: UsageRecord[] = [];
  const lastBytes = 'data: {"error":{"type":"rate_limit_exceeded","message":"last key"}}\n\n';
  const upstream = await listen((req, res) => {
    const authorization = req.headers.authorization ?? "";
    seen.push(authorization);
    res.writeHead(200, { "content-type": "text/event-stream" });
    if (authorization.includes("bad-key")) {
      res.end('data: {"error":{"type":"rate_limit_exceeded","message":"first key"}}\n\n');
      return;
    }
    res.end(lastBytes);
  });
  const app = poolApp(store, upstream.url, {
    credentialPool: { failureThreshold: 1, cooldownMs: 30_000 },
    onUsage: (record) => {
      records.push(record);
    },
  });
  const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: CHAT_HEADERS,
    body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, messages: [] }),
  });
  assert.equal(response.status, 200);
  const body = await response.text();
  assert.equal(body.startsWith(lastBytes), true);
  assert.equal(body.includes("first key"), false);
  assert.match(body, /provider stream was rate limited: last key/);
  assert.match(body, /data: \[DONE\]/);
  assert.equal(body.includes("bad-key"), false);
  assert.equal(body.includes("good-key"), false);
  assert.equal(body.includes(KEY), false);
  assert.deepEqual(seen, ["Bearer bad-key", "Bearer good-key"]);
  for (let attempt = 0; attempt < 20 && records.length === 0; attempt += 1) {
    await new Promise((wake) => setTimeout(wake, 10));
  }
  assert.equal(records.length, 1);
  assert.equal(records[0]!.status, "upstream_error");
  const status = await app.request("http://127.0.0.1/ns/platform/v1/credentials", { headers: CHAT_HEADERS });
  const rows = (await status.json()).data as { credential_id: string; state: string }[];
  assert.equal(rows.find((row) => row.credential_id === "bad")?.state, "parked");
  assert.equal(rows.find((row) => row.credential_id === "good")?.state, "parked");
  upstream.close();
});


test("a rate limit after chat content stays on that stream", async () => {
  const store = await seeded();
  const seen: string[] = [];
  const records: UsageRecord[] = [];
  const upstreamBytes = 'data: {"choices":[{"delta":{"content":"a"}}]}\n\ndata: {"error":{"type":"rate_limit_exceeded"}}\n\n';
  const upstream = await listen((req, res) => {
    seen.push(req.headers.authorization ?? "");
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end(upstreamBytes);
  });
  const app = poolApp(store, upstream.url, {
    credentialPool: { failureThreshold: 1, cooldownMs: 30_000 },
    onUsage: (record) => {
      records.push(record);
    },
  });
  const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: CHAT_HEADERS,
    body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, messages: [] }),
  });
  assert.equal(response.status, 200);
  const body = await response.text();
  assert.equal(body.startsWith(upstreamBytes), true);
  assert.match(body, /provider stream was rate limited: OpenAI stream rate limited/);
  assert.match(body, /data: \[DONE\]/);
  assert.equal(body.includes("bad-key"), false);
  assert.equal(body.includes(KEY), false);
  assert.deepEqual(seen, ["Bearer bad-key"]);
  for (let attempt = 0; attempt < 20 && records.length === 0; attempt += 1) {
    await new Promise((wake) => setTimeout(wake, 10));
  }
  assert.equal(records.length, 1);
  assert.equal(records[0]!.status, "upstream_error");
  const status = await app.request("http://127.0.0.1/ns/platform/v1/credentials", { headers: CHAT_HEADERS });
  const rows = (await status.json()).data as { credential_id: string; state: string }[];
  assert.equal(rows.find((row) => row.credential_id === "bad")?.state, "parked");
  assert.equal(rows.find((row) => row.credential_id === "good")?.state, "healthy");
  upstream.close();
});


test("a chat rate limit after content uses the provider message", async () => {
  const store = await seeded();
  const seen: string[] = [];
  const records: UsageRecord[] = [];
  const upstream = await listen((req, res) => {
    seen.push(req.headers.authorization ?? "");
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.write('data: {"choices":[{"delta":{"content":"a"}}]}\n\n');
    setTimeout(() => {
      res.end('data: {"error":{"type":"rate_limit_exceeded","message":"slow down"}}\n\n');
    }, 40);
  });
  const app = poolApp(store, upstream.url, {
    credentialPool: { failureThreshold: 1, cooldownMs: 30_000 },
    onUsage: (record) => {
      records.push(record);
    },
  });
  const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: CHAT_HEADERS,
    body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, messages: [] }),
  });
  assert.equal(response.status, 200);
  const body = await response.text();
  assert.match(body, /"content":"a"/);
  assert.match(body, /slow down/);
  assert.match(body, /provider stream was rate limited: slow down/);
  assert.equal(body.includes("bad-key"), false);
  assert.deepEqual(seen, ["Bearer bad-key"]);
  for (let attempt = 0; attempt < 20 && records.length === 0; attempt += 1) {
    await new Promise((wake) => setTimeout(wake, 10));
  }
  assert.equal(records.length, 1);
  assert.equal(records[0]!.status, "upstream_error");
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
  assert.equal(body.includes("provider stream was rate limited"), false);
  assert.equal(body.includes("upstream_stream_error"), false);
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
  assert.match(body, /provider stream was rate limited: OpenAI stream rate limited/);
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
  assert.match(body, /provider stream was rate limited: Anthropic stream rate limited/);
  assert.deepEqual(seen, ["bad-key"]);
  const status = await app.request("http://127.0.0.1/ns/platform/v1/credentials", { headers: CHAT_HEADERS });
  const rows = (await status.json()).data as { credential_id: string; state: string }[];
  assert.equal(rows.find((row) => row.credential_id === "bad")?.state, "parked");
  upstream.close();
});


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
