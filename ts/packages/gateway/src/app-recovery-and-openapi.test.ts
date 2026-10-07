import assert from "node:assert/strict";

import { createServer } from "node:http";

import { once } from "node:events";

import test from "node:test";

import { Hono } from "hono";


import { createAxond } from "./app.ts";

import { StoreFailure } from "./errors.ts";

import { forceChatIncludeUsage, rewriteTopLevelModel } from "./body.ts";

import { loadConfig, envSecretReader } from "./config.ts";

import { createMemoryStore } from "./memory-store.ts";

import { createMetrics } from "./metrics.ts";

import { scopeStore } from "./scoped-store.ts";

import { costMicrodollars } from "./pricing.ts";

import type { Store } from "@axond/sdk";


const KEY = "test-inbound-key";


async function gateway(
  metrics?: ReturnType<typeof createMetrics>,
  telemetry?: { endpoint: string; instanceId?: string },
  responseBody?: string,
  onLog?: (record: {
    msg: "request";
    status?: string;
    input_tokens?: string;
    [key: string]: unknown;
  }) => void,
) {
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
    fromConfig: true,
  });
  await store.putBudget("platform", "compat", 1_000_000_000_000n);
  await store.putBudget("tenant", "compat", 1_000_000_000_000n);
  const upstream = await listenUpstream(responseBody);
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    configNamespaces: ["platform", "tenant"],
    providers: [
      { id: "fake-openai", kind: "openai", baseUrl: upstream.url },
      { id: "fake-anthropic", kind: "anthropic", baseUrl: upstream.url },
    ],
    credentials: [
      { namespace: "platform", provider: "fake-openai", secret: "upstream-openai", id: "openai" },
      { namespace: "platform", provider: "fake-anthropic", secret: "upstream-anthropic", id: "anthropic" },
    ],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 2_500_000n,
        outputMicrodollarsPerMillion: 10_000_000n,
      },
      {
        provider: "fake-anthropic",
        model: "*",
        inputMicrodollarsPerMillion: 2_500_000n,
        outputMicrodollarsPerMillion: 10_000_000n,
      },
    ],
    rawPath: (c) => c.req.header("x-axond-raw-path") ?? new URL(c.req.url).pathname,
    metrics,
    telemetry,
    onLog,
  });
  return { app, store, upstream };
}


async function listenUpstream(responseBody?: string): Promise<{
  url: string;
  requests: { path: string; authorization: string; body: string; traceparent: string }[];
  close: () => void;
}> {
  const requests: { path: string; authorization: string; body: string; traceparent: string }[] = [];
  const server = createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) {
      chunks.push(chunk as Buffer);
    }
    const body = Buffer.concat(chunks).toString("utf8");
    requests.push({
      path: req.url ?? "",
      authorization: req.headers.authorization ?? req.headers["x-api-key"]?.toString() ?? "",
      body,
      traceparent: req.headers.traceparent?.toString() ?? "",
    });
    const payload = {
      id: "chatcmpl-test",
      choices: [{ message: { role: "assistant", content: "The capital of France is Paris." } }],
      usage: { prompt_tokens: 12, completion_tokens: 7 },
    };
    res.writeHead(200, { "content-type": "application/json" });
    res.end(responseBody ?? JSON.stringify(payload));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("no port");
  }
  return {
    url: `http://127.0.0.1:${address.port}`,
    requests,
    close: () => server.close(),
  };
}


test("a down budget store is budget_unavailable and a down management store is store_unavailable", async () => {
  const store = createMemoryStore();
  await store.putNamespace({
    id: "platform",
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: true,
  });
  await store.putBudget("platform", "compat", 1_000_000n);
  const down = new Proxy(store, {
    get(target, prop, receiver) {
      if (prop === "resolveNamespace" || prop === "listNamespaces") {
        return async () => {
          throw new StoreFailure();
        };
      }
      const value = Reflect.get(target, prop, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  const app = createAxond({
    store: down,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: "http://127.0.0.1:9" }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "upstream-secret", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
  });
  const chat = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
    body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [] }),
  });
  assert.equal(chat.status, 503);
  assert.deepEqual(await chat.json(), {
    error: { type: "budget_unavailable", message: "budget store is unavailable" },
  });
  const models = await app.request("http://127.0.0.1/ns/platform/v1/models", {
    headers: { authorization: `Bearer ${KEY}` },
  });
  assert.equal(models.status, 503);
  assert.deepEqual(await models.json(), {
    error: { type: "store_unavailable", message: "store is unavailable" },
  });
  const listed = await app.request("http://127.0.0.1/api/v1/namespaces", {
    headers: { authorization: `Bearer ${KEY}` },
  });
  assert.equal(listed.status, 503);
  assert.deepEqual(await listed.json(), {
    error: { type: "store_unavailable", message: "store is unavailable" },
  });
});


test("allow serves a chat when the budget read fails and does not charge", async () => {
  const store = createMemoryStore();
  await store.putNamespace({
    id: "platform",
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: true,
  });
  await store.putBudget("platform", "compat", 1_000_000n);
  const down = new Proxy(store, {
    get(target, prop, receiver) {
      if (prop === "resolveNamespace") {
        return async () => {
          throw new StoreFailure();
        };
      }
      const value = Reflect.get(target, prop, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  const upstream = await listenUpstream();
  const app = createAxond({
    store: down,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    onStoreUnavailable: "allow",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "upstream-openai", id: "one" }],
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
    body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: "hi" }] }),
  });
  assert.equal(response.status, 200);
  await response.json();
  await new Promise((resolve) => setTimeout(resolve, 20));
  const budget = await store.getBudget("platform", "compat");
  assert.equal(budget?.spent, 0n);
  upstream.close();
});


test("budget_store_log_names_the_stance_and_omits_the_driver_text", async () => {
  const driver = "password=secret host=db.internal:5432/axond ECONNREFUSED";
  const prompt = "PROMPT_SENTINEL";
  const logs: { msg: string; stance?: string; request_id?: string }[] = [];
  const store = createMemoryStore();
  await store.putNamespace({
    id: "platform",
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: true,
  });
  await store.putBudget("platform", "compat", 1_000_000n);
  const down = new Proxy(store, {
    get(target, prop, receiver) {
      if (prop === "resolveNamespace") {
        return async () => {
          const failure = new StoreFailure();
          failure.message = driver;
          throw failure;
        };
      }
      const value = Reflect.get(target, prop, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  const prices = [
    {
      provider: "fake-openai",
      model: "*",
      inputMicrodollarsPerMillion: 2_500_000n,
      outputMicrodollarsPerMillion: 10_000_000n,
    },
  ];
  const denied = createAxond({
    store: down,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: "http://127.0.0.1:9" }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "upstream-secret", id: "one" }],
    prices,
    onLog: (record) => {
      logs.push(record);
    },
  });
  const chat = await denied.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
    body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: prompt }] }),
  });
  assert.equal(chat.status, 503);
  const chatBody = await chat.text();
  assert.equal(JSON.parse(chatBody).error.type, "budget_unavailable");
  assert.equal(chatBody.includes(driver), false);
  assert.equal(chatBody.includes(prompt), false);
  const models = await denied.request("http://127.0.0.1/ns/platform/v1/models", {
    headers: { authorization: `Bearer ${KEY}` },
  });
  assert.equal(models.status, 503);
  assert.equal((await models.json()).error.type, "store_unavailable");
  const deniedBudget = logs.filter((record) => record.msg === "budget_unavailable");
  assert.equal(deniedBudget.length, 1);
  assert.equal(deniedBudget[0]?.stance, "deny");
  assert.equal(typeof deniedBudget[0]?.request_id, "string");
  assert.equal((deniedBudget[0]?.request_id ?? "").length > 0, true);

  const upstream = await listenUpstream();
  const allowed = createAxond({
    store: down,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    onStoreUnavailable: "allow",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "upstream-secret", id: "one" }],
    prices,
    onLog: (record) => {
      logs.push(record);
    },
  });
  const served = await allowed.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
    body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: prompt }] }),
  });
  assert.equal(served.status, 200);
  await served.json();
  await new Promise((resolve) => setTimeout(resolve, 20));
  const budget = await store.getBudget("platform", "compat");
  assert.equal(budget?.spent, 0n);
  const allowedBudget = logs.filter((record) => record.msg === "budget_unavailable" && record.stance === "allow");
  assert.equal(allowedBudget.length, 1);
  assert.equal(typeof allowedBudget[0]?.request_id, "string");
  assert.equal(allowedBudget[0]?.request_id === deniedBudget[0]?.request_id, false);
  const encoded = JSON.stringify(logs);
  assert.equal(encoded.includes(driver), false);
  assert.equal(encoded.includes("db.internal"), false);
  assert.equal(encoded.includes(prompt), false);
  assert.equal(encoded.includes(KEY), false);
  assert.equal(encoded.includes("upstream-secret"), false);
  assert.equal(encoded.includes("ECONNREFUSED"), false);
  upstream.close();
});


test("closed admission returns draining before authentication", async () => {
  const store = createMemoryStore();
  await store.putNamespace({
    id: "platform",
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: true,
  });
  let serving = true;
  let admitting = true;
  const metrics = createMetrics();
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    configNamespaces: ["platform"],
    providers: [],
    serving: () => serving,
    admitting: () => admitting,
    metrics,
  });
  serving = false;
  const ready = await app.request("http://127.0.0.1/readyz");
  assert.equal(ready.status, 503);
  assert.equal(await ready.text(), "draining");
  const health = await app.request("http://127.0.0.1/healthz");
  assert.equal(health.status, 200);
  assert.equal(await health.text(), "ok");
  const admitted = await app.request("http://127.0.0.1/api/v1/namespaces", {
    headers: { authorization: `Bearer ${KEY}` },
  });
  assert.equal(admitted.status, 200);
  admitting = false;
  const refused = await app.request("http://127.0.0.1/api/v1/namespaces");
  assert.equal(refused.status, 503);
  assert.equal(refused.headers.get("retry-after"), "0");
  assert.deepEqual(await refused.json(), {
    error: {
      type: "draining",
      message: "the gateway is shutting down and is no longer accepting requests",
    },
  });
  const inference = await app.request("http://127.0.0.1/ns/platform/v1/models", {
    headers: { authorization: `Bearer ${KEY}` },
  });
  assert.equal(inference.status, 503);
  const still = await app.request("http://127.0.0.1/healthz");
  assert.equal(await still.text(), "ok");
  const rejected = metrics.points.find((point) => point.name === "axond.shutdown.rejected_requests");
  assert.equal(rejected?.value, 2);
});


test("withdrawn routes stay unmounted", async () => {
  const { app, upstream } = await gateway();
  const routes = [
    ["POST", "/v1/chat/completions"],
    ["POST", "/v1/messages"],
    ["POST", "/v1/embeddings"],
    ["GET", "/v1/models"],
    ["GET", "/v1/credentials"],
    ["POST", "/v1/responses"],
    ["POST", "/v1/tokens"],
    ["GET", "/admin/v1/status"],
    ["GET", "/admin/v1/catalogue"],
    ["POST", "/admin/v1/bindings"],
    ["GET", "/namespaces/platform/v1/models"],
    ["GET", "/namespaces/platform/v1/credentials"],
    ["POST", "/namespaces/platform/v1/chat/completions"],
    ["POST", "/namespaces/platform/v1/messages"],
    ["POST", "/namespaces/platform/v1/embeddings"],
    ["POST", "/namespaces/platform/v1/responses"],
    ["GET", "/namespaces/%70latform/v1/models"],
  ] as const;
  try {
    for (const [method, path] of routes) {
      const response = await app.request(`http://127.0.0.1${path}`, {
        method,
        headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
        body: method === "GET" ? undefined : "{}",
      });
      assert.equal(response.status, 404, path);
      if (path.startsWith("/api/v1/") || path.startsWith("/ns/")) {
        assert.deepEqual(await response.json(), { error: { type: "not_found", message: "not found" } });
      } else {
        assert.equal(await response.text(), "");
      }
    }
    assert.equal(upstream.requests.length, 0);
  } finally {
    upstream.close();
  }
});


test("untrusted_extension_query_without_namespace_is_refused", async () => {
  const rows = [{ id: "platform" }, { id: "tenant" }];
  const store = {
    async query(sql: string, params: readonly unknown[] = []) {
      assert.match(sql, /axond_namespace/);
      return { rows: params.includes("platform") ? rows : rows };
    },
  } as unknown as Store;
  const scoped = scopeStore(store, "platform");
  await assert.rejects(() => scoped.query("SELECT * FROM axond_namespace"), (error: unknown) => {
    assert.equal((error as { status?: number }).status, 403);
    return true;
  });
  const visible = await scoped.query("SELECT * FROM axond_namespace WHERE id = ?", ["platform"]);
  assert.deepEqual(visible.rows, [{ id: "platform" }]);
});


test("a request records catalogue metrics without the gateway key", async () => {
  const store = createMemoryStore();
  const metrics = createMetrics(["sk-test-secret"]);
  const app = createAxond({
    store,
    gatewayKey: "sk-test-secret",
    defaultNamespace: "platform",
    providers: [],
    metrics,
  });
  const health = await app.request("http://127.0.0.1/healthz");
  assert.equal(health.status, 200);
  const secretPath = await app.request("http://127.0.0.1/sk-test-secret");
  assert.equal(secretPath.status, 404);
  const requests = metrics.points.filter((point) => point.name === "axond.http.server.requests");
  assert.deepEqual(
    requests.map((point) => point.attributes),
    [
      { "http.request.method": "GET", "http.route": "/healthz", "http.response.status_code": "200" },
      { "http.request.method": "GET", "http.route": "/other", "http.response.status_code": "404" },
    ],
  );
  assert.equal(JSON.stringify(metrics.points).includes("sk-test-secret"), false);
  assert.ok(metrics.points.some((point) => point.name === "axond.http.server.duration"));
  const again = await app.request("http://127.0.0.1/healthz");
  assert.equal(again.status, 200);
  assert.equal(requests[0]!.value, 2);
});


test("metrics drop secret and content sentinels", () => {
  const metrics = createMetrics(["sk-test-secret", "PROMPT_SENTINEL"]);
  metrics.record("axond.request.count", 1, {
    namespace: "platform",
    prompt: "PROMPT_SENTINEL hello",
    credential: "sk-test-secret",
  });
  assert.deepEqual(metrics.points[0]!.attributes, { namespace: "platform" });
});


test("provider_model_cache_follows_the_rust_source", async () => {
  const { app, store, upstream } = await gateway();
  const auth = { authorization: `Bearer ${KEY}` };
  const empty = await app.request("http://127.0.0.1/api/v1/providers/fake-openai/models", { headers: auth });
  assert.equal(empty.status, 200);
  assert.deepEqual(await empty.json(), { provider: "fake-openai", stale: true, data: [] });

  await store.upsertProviderModels({
    provider: "fake-openai",
    fetchedAt: "2026-09-30T00:00:00Z",
    stale: false,
    data: [{ id: "gpt-test" }],
    source: upstream.url,
  });
  const listed = await app.request("http://127.0.0.1/ns/platform/v1/models", { headers: auth });
  assert.equal(listed.status, 200);
  assert.deepEqual(await listed.json(), {
    object: "list",
    data: [{ id: "fake-openai/gpt-test", object: "model" }],
  });
  const fresh = await app.request("http://127.0.0.1/api/v1/providers/fake-openai/models", { headers: auth });
  assert.deepEqual(await fresh.json(), {
    provider: "fake-openai",
    fetched_at: "2026-09-30T00:00:00Z",
    stale: false,
    data: [{ id: "gpt-test" }],
  });

  await store.markProviderModelsStale("fake-openai");
  const stillListed = await app.request("http://127.0.0.1/ns/platform/v1/models", { headers: auth });
  assert.deepEqual(await stillListed.json(), {
    object: "list",
    data: [{ id: "fake-openai/gpt-test", object: "model" }],
  });
  const marked = await app.request("http://127.0.0.1/api/v1/providers/fake-openai/models", { headers: auth });
  assert.deepEqual(await marked.json(), {
    provider: "fake-openai",
    fetched_at: "2026-09-30T00:00:00Z",
    stale: true,
    data: [{ id: "gpt-test" }],
  });

  await store.upsertProviderModels({
    provider: "fake-openai",
    fetchedAt: "2026-09-30T00:00:01Z",
    stale: false,
    data: [{ id: "moved" }],
    source: "https://other.example/v1",
  });
  await store.upsertProviderModels({
    provider: "fake-anthropic",
    fetchedAt: "2026-09-30T00:00:02Z",
    stale: false,
    data: [{ id: "legacy" }],
    source: null,
  });
  const omitted = await app.request("http://127.0.0.1/ns/platform/v1/models", { headers: auth });
  assert.deepEqual(await omitted.json(), { object: "list", data: [] });
  const foreign = await app.request("http://127.0.0.1/api/v1/providers/fake-openai/models", { headers: auth });
  assert.deepEqual(await foreign.json(), {
    provider: "fake-openai",
    fetched_at: "2026-09-30T00:00:01Z",
    stale: true,
    data: [{ id: "moved" }],
  });
  const missingSource = await app.request("http://127.0.0.1/api/v1/providers/fake-anthropic/models", { headers: auth });
  assert.deepEqual(await missingSource.json(), {
    provider: "fake-anthropic",
    fetched_at: "2026-09-30T00:00:02Z",
    stale: true,
    data: [{ id: "legacy" }],
  });
  upstream.close();
});


test("openapi is 3.1 and lists the management routes", async () => {
  const { app, upstream } = await gateway();
  const response = await app.request("http://127.0.0.1/api/v1/openapi.json", {
    headers: { authorization: `Bearer ${KEY}` },
  });
  assert.equal(response.status, 200);
  const spec = await response.json();
  assert.equal(spec.openapi, "3.1.0");
  assert.ok(spec.paths["/api/v1/namespaces/{ns}/usage"].get.parameters[0].required);
  upstream.close();
});
