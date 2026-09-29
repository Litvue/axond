import assert from "node:assert/strict";
import { createServer } from "node:http";
import { once } from "node:events";
import test from "node:test";
import { Hono } from "hono";

import { createAxond } from "./app.ts";
import { rewriteTopLevelModel } from "./body.ts";
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
  const upstream = await listenUpstream();
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
  });
  return { app, store, upstream };
}

test("a host Hono app mounts the gateway and keeps its own route", async () => {
  const store = createMemoryStore();
  const gateway = createAxond({
    store,
    gatewayKey: KEY,
    extensions: [
      {
        name: "banner",
        apiVersion: 1,
        stage: "pre-auth",
        async middleware() {
          return new Response("extended");
        },
      },
    ],
  });
  const host = new Hono();
  host.get("/host", (c) => c.text("host"));
  host.route("/", gateway);
  const health = await host.request("http://127.0.0.1/healthz");
  assert.equal(health.status, 200);
  assert.equal(await health.text(), "ok");
  const own = await host.request("http://127.0.0.1/host");
  assert.equal(own.status, 200);
  assert.equal(await own.text(), "host");
  const extended = await host.request("http://127.0.0.1/api/v1/namespaces");
  assert.equal(await extended.text(), "extended");
});

test("rewrite keeps duplicate keys and large integers", () => {
  const raw = new TextEncoder().encode('{"model":"fake-openai/chat","n":9007199254740993,"a":1,"a":2}');
  const next = new TextDecoder().decode(rewriteTopLevelModel(raw, "chat"));
  assert.equal(next, '{"model":"chat","n":9007199254740993,"a":1,"a":2}');
});

test("a buffered chat completion rewrites the model and forwards the provider credential", async () => {
  const { app, store, upstream } = await gateway();
  const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: {
      authorization: `Bearer ${KEY}`,
      "content-type": "application/json",
      "x-request-id": "req-chat-1",
    },
    body: JSON.stringify({
      model: "fake-openai/gpt-test",
      messages: [{ role: "user", content: "What is the capital of France?" }],
    }),
  });
  assert.equal(response.status, 200);
  const body = await response.json();
  assert.equal(body.choices[0].message.content, "The capital of France is Paris.");
  const sent = upstream.requests.at(-1)!;
  assert.equal(sent.path, "/chat/completions");
  assert.equal(sent.authorization, "Bearer upstream-openai");
  assert.equal(JSON.parse(sent.body).model, "gpt-test");
  await new Promise((resolve) => setTimeout(resolve, 20));
  const summary = await store.summarizeUsage("platform", "compat");
  assert.equal(summary.length, 1);
  assert.equal(summary[0]!.count, 1);
  assert.equal(summary[0]!.cost_microdollars, 100);
  upstream.close();
});

test("a settled chat records request duration without the prompt", async () => {
  const metrics = createMetrics([KEY]);
  const { app, upstream } = await gateway(metrics);
  const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: {
      authorization: `Bearer ${KEY}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model: "fake-openai/gpt-test",
      messages: [{ role: "user", content: "PROMPT_SENTINEL" }],
    }),
  });
  assert.equal(response.status, 200);
  await response.text();
  await new Promise((resolve) => setTimeout(resolve, 20));
  const count = metrics.points.find((point) => point.name === "axond.request.count");
  assert.equal(count?.value, 1);
  assert.equal(count?.attributes["axond.namespace"], "platform");
  assert.equal(count?.attributes["axond.status"], "ok");
  assert.equal(count?.attributes["gen_ai.request.model"], "fake-openai/gpt-test");
  const duration = metrics.points.find((point) => point.name === "axond.request.duration");
  assert.ok(duration && duration.value >= 0);
  const cost = metrics.points.find((point) => point.name === "axond.cost.microdollars");
  assert.equal(cost?.value, 100);
  const encoded = JSON.stringify(metrics.points);
  assert.equal(encoded.includes(KEY), false);
  assert.equal(encoded.includes("PROMPT_SENTINEL"), false);
  upstream.close();
});

test("otlp json joins traceparent and omits the prompt", async () => {
  const received: { url: string; body: string }[] = [];
  const collector = createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) {
      chunks.push(chunk as Buffer);
    }
    received.push({ url: req.url ?? "", body: Buffer.concat(chunks).toString("utf8") });
    res.writeHead(200);
    res.end();
  });
  collector.listen(0, "127.0.0.1");
  await once(collector, "listening");
  const address = collector.address();
  if (!address || typeof address === "string") {
    throw new Error("no collector port");
  }
  const metrics = createMetrics([KEY, "upstream-openai"]);
  const { app, upstream } = await gateway(metrics, {
    endpoint: `http://127.0.0.1:${address.port}`,
    instanceId: "axond-replica-a",
  });
  const inbound = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: {
        authorization: `Bearer ${KEY}`,
        "content-type": "application/json",
        traceparent: inbound,
      },
      body: JSON.stringify({
        model: "fake-openai/gpt-test",
        messages: [{ role: "user", content: "PROMPT_SENTINEL" }],
      }),
    });
    assert.equal(response.status, 200);
    await response.text();
    const deadline = Date.now() + 2_000;
    while (received.length < 2 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    const traces = received.find((item) => item.url === "/v1/traces");
    const metricsBody = received.find((item) => item.url === "/v1/metrics");
    assert.ok(traces);
    assert.ok(metricsBody);
    const spans = JSON.parse(traces.body).resourceSpans[0].scopeSpans[0].spans as {
      name: string;
      traceId: string;
      spanId: string;
      parentSpanId?: string;
      attributes: { key: string; value: { stringValue: string } }[];
    }[];
    const span = spans.find((item) => item.name === "http.server.request");
    const attempt = spans.find((item) => item.name === "axond.upstream.attempt");
    assert.ok(span);
    assert.ok(attempt);
    assert.equal(span.traceId, "4bf92f3577b34da6a3ce929d0e0e4736");
    assert.equal(span.parentSpanId, "00f067aa0ba902b7");
    assert.equal(span.name, "http.server.request");
    assert.equal(attempt.traceId, span.traceId);
    assert.equal(attempt.parentSpanId, span.spanId);
    assert.equal(attempt.attributes.find((item) => item.key === "axond.target.provider")?.value.stringValue, "fake-openai");
    assert.equal(attempt.attributes.find((item) => item.key === "axond.status")?.value.stringValue, "ok");
    assert.equal(upstream.requests[0]!.traceparent, `00-${span.traceId}-${span.spanId}-01`);
    const exported = `${traces.body}\n${metricsBody.body}`;
    assert.equal(exported.includes(KEY), false);
    assert.equal(exported.includes("PROMPT_SENTINEL"), false);
    assert.equal(exported.includes("upstream-openai"), false);
    assert.equal(exported.includes("axond-replica-a"), true);
    assert.equal(JSON.parse(metricsBody.body).resourceMetrics[0].scopeMetrics[0].metrics.some(
      (metric: { name: string }) => metric.name === "axond.http.server.requests",
    ), true);
  } finally {
    upstream.close();
    collector.closeAllConnections();
    collector.close();
  }
});

test("unknown_gateway_key_is_rejected_before_namespace_lookup", async () => {
  const { app, upstream } = await gateway();
  const response = await app.request("http://127.0.0.1/ns/ghost/v1/models", {
    headers: { authorization: "Bearer nope" },
  });
  assert.equal(response.status, 401);
  assert.deepEqual(await response.json(), { error: { type: "unauthorized", message: "unauthorized" } });
  assert.equal(upstream.requests.length, 0);
  upstream.close();
});

test("noncanonical_namespace_path_is_invalid_after_authentication", async () => {
  const { app, upstream } = await gateway();
  const response = await app.request("http://127.0.0.1/ns/%70latform/v1/models", {
    headers: {
      authorization: `Bearer ${KEY}`,
      "x-axond-raw-path": "/ns/%70latform/v1/models",
    },
  });
  const body = await response.json();
  assert.equal(response.status, 400);
  assert.deepEqual(body, { error: { type: "invalid_namespace", message: "namespace identifier is invalid" } });
  assert.equal(JSON.stringify(body).includes("%70latform"), false);
  upstream.close();
});

test("an absent namespace is unknown and an unprefixed model is refused", async () => {
  const { app, upstream } = await gateway();
  const missing = await app.request("http://127.0.0.1/ns/ghost/v1/models", {
    headers: { authorization: `Bearer ${KEY}` },
  });
  assert.equal(missing.status, 404);
  assert.deepEqual(await missing.json(), { error: { type: "unknown_namespace", message: "unknown namespace" } });
  const unprefixed = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
    body: JSON.stringify({ model: "gpt-test", messages: [] }),
  });
  assert.equal(unprefixed.status, 400);
  assert.equal((await unprefixed.json()).error.type, "model_unprefixed");
  assert.equal(upstream.requests.length, 0);
  upstream.close();
});

test("ten settlements of one request_id charge once", async () => {
  const store = createMemoryStore();
  await store.putNamespace({ id: "platform", attrs: {}, blocklist: null, allowPlatformFallback: false, fromConfig: true });
  await store.putBudget("platform", "compat", 1_000_000n);
  const input = {
    requestId: "same",
    namespace: "platform",
    period: "compat",
    model: "fake-openai/gpt-test",
    status: "ok",
    cost: 1000n,
    incarnation: 1n,
  };
  const results = await Promise.all(Array.from({ length: 10 }, () => store.settle(input)));
  assert.equal(results.filter((result) => result.charged).length, 1);
  const distinct = await Promise.all(
    Array.from({ length: 10 }, (_, index) => store.settle({ ...input, requestId: `id-${index}` })),
  );
  assert.equal(distinct.filter((result) => result.charged).length, 10);
  const budget = await store.getBudget("platform", "compat");
  assert.equal(budget?.spent, 11_000n);
});

test("pricing truncates a partial microdollar", () => {
  const cost = costMicrodollars(
    {
      provider: "p",
      model: "*",
      inputMicrodollarsPerMillion: 2_000n,
      outputMicrodollarsPerMillion: 4_000n,
    },
    { inputTokens: 499n, outputTokens: 0n, reasoningTokens: 0n, cacheReadTokens: 0n, cacheWriteTokens: 0n },
  );
  assert.equal(cost, 0n);
});

test("withdrawn config sections fail boot by name and secrets stay out of the error", async () => {
  const toml = `
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
[gateway_token]
audience = "test"
[rate_limit]
backend = "redis"
`;
  await assert.rejects(
    () => loadConfig(toml, envSecretReader({ GW_KEY: "super-secret-value" }, async () => "")),
    (error: unknown) => {
      const message = error instanceof Error ? error.message : "";
      assert.match(message, /`gateway_token`/);
      assert.match(message, /`rate_limit`/);
      assert.match(message, /withdrawn \(ADR 0063\)/);
      assert.equal(message.includes("super-secret-value"), false);
      return true;
    },
  );
});

test("shutdown bounds default to the rust values and reject an unbounded wait", async () => {
  const toml = `
[storage]
backend = "sqlite"
path = "/tmp/axond.sqlite"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`;
  const loaded = await loadConfig(toml, envSecretReader({ GW_KEY: "k" }, async () => ""));
  assert.deepEqual(loaded.shutdown, { drainGraceMs: 5_000, deadlineMs: 15_000, flushTimeoutMs: 5_000 });
  await assert.rejects(
    () =>
      loadConfig(
        `${toml}\n[shutdown]\ndeadline_ms = 0\n`,
        envSecretReader({ GW_KEY: "k" }, async () => ""),
      ),
    (error: unknown) => {
      assert.match(error instanceof Error ? error.message : "", /shutdown\.deadline_ms must be at least 1/);
      return true;
    },
  );
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

async function listenUpstream(): Promise<{
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
    res.end(JSON.stringify(payload));
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
