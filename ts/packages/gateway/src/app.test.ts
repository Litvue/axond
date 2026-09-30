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

test("chat stream usage splice keeps unrelated bytes", () => {
  const encode = (value: string) => new TextEncoder().encode(value);
  const decode = (value: Uint8Array) => new TextDecoder().decode(value);
  assert.equal(
    decode(
      forceChatIncludeUsage(
        encode(
          '{"z":1,"model":"gpt-test","n":9007199254740993,"a":1,"a":2,"stream":true,"messages":[{"stream_options":{"include_usage":false}}]}',
        ),
      ),
    ),
    '{"z":1,"model":"gpt-test","n":9007199254740993,"a":1,"a":2,"stream":true,"messages":[{"stream_options":{"include_usage":false}}],"stream_options":{"include_usage":true}}',
  );
  assert.equal(
    decode(
      forceChatIncludeUsage(
        encode(
          '{"stream":true,"n":9007199254740993,"stream_options":{"future_option":"keep","nested":{"include_usage":false},"include_usage":false}}',
        ),
      ),
    ),
    '{"stream":true,"n":9007199254740993,"stream_options":{"future_option":"keep","nested":{"include_usage":false},"include_usage":true}}',
  );
  assert.equal(
    decode(forceChatIncludeUsage(encode('{"stream":false,"stream":true,"stream_options":[]}'))),
    '{"stream":false,"stream":true,"stream_options":{"include_usage":true}}',
  );
  assert.equal(
    decode(forceChatIncludeUsage(encode('{"stream":true,"stream":false,"n":9007199254740993}'))),
    '{"stream":true,"stream":false,"n":9007199254740993}',
  );
  const already = '{"stream":true,"stream_options":{"include_usage":true,"future_option":"keep"}}';
  assert.equal(decode(forceChatIncludeUsage(encode(already))), already);
});

test("a buffered chat forwards duplicate keys, field order, and integers above 2^53", async () => {
  const { app, upstream } = await gateway();
  const raw = '{"z":1,"model": "fake-openai/gpt-test","n":9007199254740993,"a":1,"a":2}';
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: raw,
    });
    assert.equal(response.status, 200);
    await response.text();
    const sent = upstream.requests.at(-1)!;
    assert.equal(sent.body, '{"z":1,"model": "gpt-test","n":9007199254740993,"a":1,"a":2}');
    assert.equal(sent.authorization, "Bearer upstream-openai");
    const parsed = JSON.parse(sent.body) as { n: number; a: number };
    assert.equal(parsed.n, 9007199254740992);
    assert.equal(parsed.a, 2);
  } finally {
    upstream.close();
  }
});

test("a buffered chat returns the provider bytes, including duplicate keys and integers above 2^53", async () => {
  const raw =
    '{"z":1,"id":"chatcmpl-test","n":9007199254740993,"a":1,"a":2,"choices":[{"message":{"role":"assistant","content":"ok"}}]}';
  const { app, upstream } = await gateway(undefined, undefined, raw);
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: "hi" }] }),
    });
    assert.equal(response.status, 200);
    assert.equal(response.headers.get("content-type"), "application/json");
    const text = await response.text();
    assert.equal(text, raw);
    const parsed = JSON.parse(text) as { n: number; a: number };
    assert.equal(parsed.n, 9007199254740992);
    assert.equal(parsed.a, 2);
  } finally {
    upstream.close();
  }
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
  const ttft = metrics.points.find((point) => point.name === "axond.request.time_to_first_token");
  assert.ok(ttft && ttft.value >= 0);
  assert.equal(ttft?.attributes["axond.status"], "ok");
  assert.equal(ttft?.attributes["axond.credential_source"], "platform");
  assert.equal(metrics.points.some((point) => point.name === "axond.upstream.errors"), false);
  assert.equal(metrics.points.some((point) => point.name === "axond.upstream.time_to_first_token"), false);
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
    assert.equal(span.attributes.find((item) => item.key === "axond.subject")?.value.stringValue, "gateway-key");
    assert.equal(span.attributes.find((item) => item.key === "axond.target.provider")?.value.stringValue, "fake-openai");
    assert.equal(span.attributes.find((item) => item.key === "axond.target.model")?.value.stringValue, "gpt-test");
    assert.equal(span.attributes.find((item) => item.key === "axond.credential_source")?.value.stringValue, "platform");
    assert.equal(span.attributes.find((item) => item.key === "axond.status")?.value.stringValue, "ok");
    assert.equal(span.attributes.find((item) => item.key === "axond.retry_count")?.value.stringValue, "0");
    assert.equal(span.attributes.find((item) => item.key === "gen_ai.usage.input_tokens")?.value.stringValue, "12");
    assert.equal(span.attributes.find((item) => item.key === "gen_ai.usage.output_tokens")?.value.stringValue, "7");
    assert.equal(span.attributes.find((item) => item.key === "axond.cost_microdollars")?.value.stringValue, "100");
    assert.ok(span.attributes.find((item) => item.key === "axond.latency_ms"));
    assert.ok(span.attributes.find((item) => item.key === "axond.ttft_ms"));
    assert.equal(attempt.traceId, span.traceId);
    assert.equal(attempt.parentSpanId, span.spanId);
    assert.equal(attempt.attributes.find((item) => item.key === "axond.target.provider")?.value.stringValue, "fake-openai");
    assert.equal(attempt.attributes.find((item) => item.key === "axond.status")?.value.stringValue, "ok");
    assert.equal(
      attempt.attributes.find((item) => item.key === "axond.ttft_ms")?.value.stringValue,
      attempt.attributes.find((item) => item.key === "axond.latency_ms")?.value.stringValue,
    );
    const lease = spans.find((item) => item.name === "axond.credential.lease");
    assert.ok(lease);
    assert.equal(lease.traceId, span.traceId);
    assert.equal(lease.parentSpanId, attempt.spanId);
    assert.equal(lease.attributes.find((item) => item.key === "axond.credential.id")?.value.stringValue, "openai");
    assert.equal(lease.attributes.find((item) => item.key === "axond.credential_source")?.value.stringValue, "platform");
    assert.equal(lease.attributes.find((item) => item.key === "axond.credential.index")?.value.stringValue, "0");
    assert.equal(lease.attributes.find((item) => item.key === "axond.status")?.value.stringValue, "served");
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

test("request_log_carries_server_span_fields_and_omits_content", async () => {
  const logs: {
    msg: string;
    request_id: string;
    trace_id: string;
    span_id: string;
    http_method: string;
    http_route: string;
    status_code: number;
    duration_ms: number;
    namespace: string;
    subject: string;
    model: string;
    target_provider?: string;
    target_model?: string;
    credential_source?: string;
    status?: string;
    retry_count?: number;
    input_tokens?: string;
    cache_read_tokens?: string;
    cache_write_tokens?: string;
    output_tokens?: string;
    cost_microdollars?: string | null;
    latency_ms?: number;
    ttft_ms?: number;
  }[] = [];
  const completion = JSON.stringify({
    id: "chatcmpl-test",
    choices: [{ message: { role: "assistant", content: "COMPLETION_SENTINEL" } }],
    usage: { prompt_tokens: 12, completion_tokens: 7 },
  });
  const { app, upstream } = await gateway(undefined, undefined, completion, (record) => {
    logs.push(record as (typeof logs)[number]);
  });
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: {
        authorization: `Bearer ${KEY}`,
        "content-type": "application/json",
        traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
      },
      body: JSON.stringify({
        model: "fake-openai/gpt-test",
        messages: [{ role: "user", content: "PROMPT_SENTINEL" }],
      }),
    });
    assert.equal(response.status, 200);
    assert.equal(await response.text(), completion);
    assert.equal(logs.length, 1);
    const line = logs[0]!;
    assert.equal(line.msg, "request");
    assert.equal(line.http_method, "POST");
    assert.equal(line.http_route, "/ns/{namespace}/v1/chat/completions");
    assert.equal(line.status_code, 200);
    assert.equal(line.namespace, "platform");
    assert.equal(line.subject, "gateway-key");
    assert.equal(line.model, "fake-openai/gpt-test");
    assert.equal(line.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
    assert.match(line.span_id, /^[0-9a-f]{16}$/);
    assert.equal(line.target_provider, "fake-openai");
    assert.equal(line.target_model, "gpt-test");
    assert.equal(line.credential_source, "platform");
    assert.equal(line.status, "ok");
    assert.equal(line.retry_count, 0);
    assert.equal(line.input_tokens, "12");
    assert.equal(line.output_tokens, "7");
    assert.equal(line.cache_read_tokens, "0");
    assert.equal(line.cache_write_tokens, "0");
    assert.equal(line.cost_microdollars, "100");
    assert.equal(typeof line.latency_ms, "number");
    assert.equal(typeof line.ttft_ms, "number");
    assert.ok(line.request_id.length > 0);
    const encoded = JSON.stringify(line);
    assert.equal(encoded.includes("PROMPT_SENTINEL"), false);
    assert.equal(encoded.includes("COMPLETION_SENTINEL"), false);
    assert.equal(encoded.includes(KEY), false);
    assert.equal(encoded.includes("upstream-openai"), false);
  } finally {
    upstream.close();
  }

  const streamLogs: typeof logs = [];
  const sse = 'data: {"choices":[{"delta":{"content":"COMPLETION_SENTINEL"}}]}\n\ndata: [DONE]\n\n';
  const streamUpstream = await new Promise<{ url: string; close: () => void }>((resolve) => {
    const server = createServer((req, res) => {
      req.resume();
      req.on("end", () => {
        res.writeHead(200, { "content-type": "text/event-stream" });
        res.end(sse);
      });
    });
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
  const store = createMemoryStore();
  await store.putNamespace({
    id: "platform",
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: true,
  });
  await store.putBudget("platform", "compat", 1_000_000_000_000n);
  const streamApp = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: streamUpstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "upstream-openai", id: "openai" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 2_500_000n,
        outputMicrodollarsPerMillion: 10_000_000n,
      },
    ],
    onLog: (record) => {
      streamLogs.push(record);
    },
  });
  try {
    const response = await streamApp.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({
        model: "fake-openai/gpt-test",
        stream: true,
        messages: [{ role: "user", content: "PROMPT_SENTINEL" }],
      }),
    });
    assert.equal(response.status, 200);
    assert.equal(streamLogs.length, 1);
    const opened = streamLogs[0]!;
    assert.equal(opened.target_provider, "fake-openai");
    assert.equal(opened.target_model, "gpt-test");
    assert.equal(opened.credential_source, "platform");
    assert.equal(opened.status, undefined);
    assert.equal(opened.input_tokens, undefined);
    assert.equal(opened.cost_microdollars, undefined);
    const body = await response.text();
    assert.equal(body.includes("COMPLETION_SENTINEL"), true);
    const encoded = JSON.stringify(opened);
    assert.equal(encoded.includes("PROMPT_SENTINEL"), false);
    assert.equal(encoded.includes("COMPLETION_SENTINEL"), false);
    assert.equal(encoded.includes(KEY), false);
    assert.equal(encoded.includes("upstream-openai"), false);
  } finally {
    streamUpstream.close();
  }
});

test("credential_lease_spans_follow_the_pool_walk", async () => {
  const traces: string[] = [];
  const collector = createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) {
      chunks.push(chunk as Buffer);
    }
    if ((req.url ?? "") === "/v1/traces") {
      traces.push(Buffer.concat(chunks).toString("utf8"));
    }
    res.writeHead(200);
    res.end();
  });
  collector.listen(0, "127.0.0.1");
  await once(collector, "listening");
  const collectorAddress = collector.address();
  if (!collectorAddress || typeof collectorAddress === "string") {
    throw new Error("no collector port");
  }
  const seen: string[] = [];
  let mode: "rotate" | "down" = "rotate";
  const upstream = createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) {
      chunks.push(chunk as Buffer);
    }
    await Buffer.concat(chunks);
    const authorization = req.headers.authorization ?? "";
    seen.push(authorization);
    if (authorization.includes("bad-key") || mode === "down") {
      const status = mode === "down" ? 500 : 429;
      const message = mode === "down" ? "slow down good-key" : "slow down";
      res.writeHead(status, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { message } }));
      return;
    }
    res.writeHead(200, { "content-type": "application/json" });
    res.end('{"usage":{"prompt_tokens":1,"completion_tokens":1}}');
  });
  upstream.listen(0, "127.0.0.1");
  await once(upstream, "listening");
  const upstreamAddress = upstream.address();
  if (!upstreamAddress || typeof upstreamAddress === "string") {
    throw new Error("no upstream port");
  }
  const store = createMemoryStore();
  await store.putNamespace({
    id: "platform",
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: true,
  });
  await store.putBudget("platform", "compat", 1_000_000_000_000n);
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    configNamespaces: ["platform"],
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: `http://127.0.0.1:${upstreamAddress.port}` }],
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
    credentialPool: { failureThreshold: 1, cooldownMs: 60_000 },
    telemetry: { endpoint: `http://127.0.0.1:${collectorAddress.port}` },
  });
  const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
  const body = JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: "PROMPT_SENTINEL" }] });
  type Span = {
    name: string;
    spanId: string;
    parentSpanId?: string;
    attributes: { key: string; value: { stringValue: string } }[];
  };
  const attr = (span: Span, key: string) => span.attributes.find((item) => item.key === key)?.value.stringValue;
  const waitTraces = async (count: number) => {
    const deadline = Date.now() + 2_000;
    while (traces.length < count && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.equal(traces.length >= count, true);
  };
  try {
    const rotated = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers,
      body,
    });
    assert.equal(rotated.status, 200);
    await rotated.text();
    await waitTraces(1);
    const first = JSON.parse(traces[0]!).resourceSpans[0].scopeSpans[0].spans as Span[];
    const firstServer = first.find((span) => span.name === "http.server.request");
    const firstAttempts = first.filter((span) => span.name === "axond.upstream.attempt");
    const firstLeases = first.filter((span) => span.name === "axond.credential.lease");
    assert.ok(firstServer);
    assert.equal(firstAttempts.length, 2);
    assert.equal(firstLeases.length, 2);
    const limited = firstLeases.find((span) => attr(span, "axond.credential.id") === "bad");
    const served = firstLeases.find((span) => attr(span, "axond.credential.id") === "good");
    assert.ok(limited);
    assert.ok(served);
    assert.equal(attr(limited, "axond.status"), "rate_limited");
    assert.equal(attr(limited, "axond.credential.index"), "0");
    assert.equal(attr(limited, "axond.credential_source"), "platform");
    assert.equal(attr(firstAttempts[0]!, "axond.upstream.status"), "429");
    assert.equal(attr(firstAttempts[0]!, "axond.upstream.message"), "slow down");
    assert.equal(attr(served, "axond.status"), "served");
    assert.equal(attr(served, "axond.credential.index"), "1");
    assert.equal(limited.parentSpanId, firstAttempts[0]!.spanId);
    assert.equal(served.parentSpanId, firstAttempts[1]!.spanId);
    assert.equal(firstAttempts[0]!.parentSpanId, firstServer.spanId);
    assert.equal(firstAttempts[1]!.parentSpanId, firstServer.spanId);
    assert.equal(attr(firstAttempts[0]!, "axond.status"), "error");
    assert.equal(attr(firstAttempts[1]!, "axond.status"), "ok");

    const skipped = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers,
      body,
    });
    assert.equal(skipped.status, 200);
    await skipped.text();
    await waitTraces(2);
    const second = JSON.parse(traces[1]!).resourceSpans[0].scopeSpans[0].spans as Span[];
    const secondAttempts = second.filter((span) => span.name === "axond.upstream.attempt");
    const secondLeases = second.filter((span) => span.name === "axond.credential.lease");
    assert.equal(secondAttempts.length, 1);
    assert.equal(secondLeases.length, 2);
    const parked = secondLeases.find((span) => attr(span, "axond.status") === "parked");
    const stillServed = secondLeases.find((span) => attr(span, "axond.status") === "served");
    assert.ok(parked);
    assert.ok(stillServed);
    assert.equal(attr(parked, "axond.credential.id"), "bad");
    assert.equal(attr(parked, "axond.credential.index"), "0");
    assert.equal(attr(stillServed, "axond.credential.id"), "good");
    assert.equal(attr(stillServed, "axond.credential.index"), "1");
    assert.equal(parked.parentSpanId, secondAttempts[0]!.spanId);
    assert.equal(stillServed.parentSpanId, secondAttempts[0]!.spanId);
    assert.deepEqual(seen, ["Bearer bad-key", "Bearer good-key", "Bearer good-key"]);

    mode = "down";
    const down = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers,
      body,
    });
    assert.equal(down.status, 502);
    await down.text();
    await waitTraces(3);
    const third = JSON.parse(traces[2]!).resourceSpans[0].scopeSpans[0].spans as Span[];
    const failed = third.find(
      (span) => span.name === "axond.credential.lease" && attr(span, "axond.credential.id") === "good",
    );
    assert.ok(failed);
    assert.equal(attr(failed, "axond.status"), "error");
    assert.equal(attr(failed, "axond.credential.index"), "1");
    const failedAttempt = third.find((span) => span.name === "axond.upstream.attempt");
    assert.ok(failedAttempt);
    assert.equal(attr(failedAttempt, "axond.upstream.status"), "500");
    assert.equal(attr(failedAttempt, "axond.upstream.message"), "slow down [REDACTED]");
    assert.equal(attr(failedAttempt, "axond.timeout"), undefined);
    const exported = traces.join("\n");
    assert.equal(exported.includes(KEY), false);
    assert.equal(exported.includes("bad-key"), false);
    assert.equal(exported.includes("good-key"), false);
    assert.equal(exported.includes("PROMPT_SENTINEL"), false);
  } finally {
    upstream.close();
    collector.closeAllConnections();
    collector.close();
  }
});

test("attempt_span_records_a_header_timeout", async () => {
  const traces: string[] = [];
  const collector = createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) {
      chunks.push(chunk as Buffer);
    }
    if ((req.url ?? "") === "/v1/traces") {
      traces.push(Buffer.concat(chunks).toString("utf8"));
    }
    res.writeHead(200);
    res.end();
  });
  collector.listen(0, "127.0.0.1");
  await once(collector, "listening");
  const collectorAddress = collector.address();
  if (!collectorAddress || typeof collectorAddress === "string") {
    throw new Error("no collector port");
  }
  const upstream = createServer(() => {
    // Never writes a response.
  });
  upstream.listen(0, "127.0.0.1");
  await once(upstream, "listening");
  const upstreamAddress = upstream.address();
  if (!upstreamAddress || typeof upstreamAddress === "string") {
    throw new Error("no upstream port");
  }
  const store = createMemoryStore();
  await store.putNamespace({
    id: "platform",
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: true,
  });
  await store.putBudget("platform", "compat", 1_000_000_000_000n);
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: `http://127.0.0.1:${upstreamAddress.port}` }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "upstream-openai", id: "openai" }],
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
      bufferedBodyTimeoutMs: 5_000,
      streamIdleTimeoutMs: 5_000,
      maxResponseBytes: 1024,
    },
    telemetry: { endpoint: `http://127.0.0.1:${collectorAddress.port}` },
  });
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [] }),
    });
    assert.equal(response.status, 504);
    await response.text();
    const deadline = Date.now() + 2_000;
    while (traces.length < 1 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.equal(traces.length >= 1, true);
    const spans = JSON.parse(traces[0]!).resourceSpans[0].scopeSpans[0].spans as {
      name: string;
      attributes: { key: string; value: { stringValue: string } }[];
    }[];
    const attempt = spans.find((span) => span.name === "axond.upstream.attempt");
    assert.ok(attempt);
    const attr = (key: string) => attempt.attributes.find((item) => item.key === key)?.value.stringValue;
    assert.equal(attr("axond.timeout"), "response_headers");
    assert.equal(attr("axond.timeout.bound"), "phase");
    assert.equal(attr("axond.status"), "error");
    assert.equal(attr("axond.upstream.status"), undefined);
    assert.equal(traces[0]!.includes("upstream-openai"), false);
    assert.equal(traces[0]!.includes(KEY), false);
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

test("empty_model_segments_match_the_rust_split", async () => {
  const { app, upstream } = await gateway();
  const post = (model: string) => app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
    body: JSON.stringify({ model, messages: [] }),
  });
  const leading = await post("/gpt-test");
  assert.equal(leading.status, 400);
  assert.deepEqual(await leading.json(), {
    error: { type: "unknown_provider", message: "unknown provider ``" },
  });
  const onlySlash = await post("/");
  assert.equal(onlySlash.status, 400);
  assert.deepEqual(await onlySlash.json(), {
    error: { type: "unknown_provider", message: "unknown provider ``" },
  });
  const trailing = await post("fake-openai/");
  assert.equal(trailing.status, 400);
  assert.deepEqual(await trailing.json(), {
    error: { type: "bad_request", message: "model id after `/` must not be empty" },
  });
  assert.equal(upstream.requests.length, 0);
  upstream.close();
});

test("json_null_is_missing_model_and_invalid_json_names_the_body", async () => {
  const { app, upstream } = await gateway();
  const post = (body: BodyInit) => app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
    body,
  });
  for (const body of ["null", "[]", "true", "0", "{}"]) {
    const response = await post(body);
    assert.equal(response.status, 400, body);
    assert.deepEqual(await response.json(), {
      error: { type: "bad_request", message: "missing `model`" },
    });
  }
  const invalid = await post("{");
  assert.equal(invalid.status, 400);
  assert.deepEqual(await invalid.json(), {
    error: { type: "bad_request", message: "request body is not valid JSON" },
  });
  const utf8 = await post(new Uint8Array([0xff, 0xfe]));
  assert.equal(utf8.status, 400);
  assert.deepEqual(await utf8.json(), {
    error: { type: "bad_request", message: "request body is not valid JSON" },
  });
  assert.equal(upstream.requests.length, 0);
  upstream.close();
});

test("management_json_rejects_unknown_fields_and_out_of_range_limits", async () => {
  const { app, upstream } = await gateway();
  try {
  const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
  const call = (path: string, method: string, body?: string) =>
    app.request(`http://127.0.0.1${path}`, { method, headers, ...(body === undefined ? {} : { body }) });
  const created = await call("/api/v1/namespaces", "POST", JSON.stringify({ id: "wsp_x", attrs: { org: "acme" } }));
  assert.equal(created.status, 201);
  const typo = await call("/api/v1/namespaces/wsp_x", "PUT", JSON.stringify({ attr: { org: "typo" } }));
  assert.equal(typo.status, 400);
  assert.deepEqual(await typo.json(), {
    error: {
      type: "bad_request",
      message: "Failed to deserialize the JSON body into the target type: attr: unknown field `attr`, expected `attrs` or `blocklist` at line 1 column 7",
    },
  });
  const kept = await call("/api/v1/namespaces/wsp_x", "GET");
  assert.equal(kept.status, 200);
  assert.equal((await kept.json()).attrs.org, "acme");
  const extra = await call("/api/v1/namespaces", "POST", '{"id":"wsp_x","extra":1}');
  assert.equal(extra.status, 400);
  assert.deepEqual(await extra.json(), {
    error: {
      type: "bad_request",
      message: "Failed to deserialize the JSON body into the target type: extra: unknown field `extra`, expected one of `id`, `attrs`, `blocklist` at line 1 column 21",
    },
  });
  const missing = await call("/api/v1/namespaces", "POST", "{}");
  assert.equal(missing.status, 400);
  assert.deepEqual(await missing.json(), {
    error: {
      type: "bad_request",
      message: "Failed to deserialize the JSON body into the target type: missing field `id` at line 1 column 2",
    },
  });
  const spaced = await call("/api/v1/namespaces/wsp_x", "PUT", "{\n  \"attr\": 1\n}");
  assert.equal(spaced.status, 400);
  assert.match((await spaced.json()).error.message, /at line 2 column 8$/);
  const stringLimit = await call("/api/v1/namespaces/wsp_x/budgets/2026-09", "PUT", '{"limit_microdollars":"10"}');
  assert.equal(stringLimit.status, 400);
  assert.deepEqual(await stringLimit.json(), {
    error: {
      type: "bad_request",
      message: 'Failed to deserialize the JSON body into the target type: limit_microdollars: invalid type: string "10", expected u64 at line 1 column 26',
    },
  });
  const floatLimit = await call("/api/v1/namespaces/wsp_x/budgets/2026-09", "PUT", '{"limit_microdollars":1.5}');
  assert.equal(floatLimit.status, 400);
  assert.deepEqual(await floatLimit.json(), {
    error: {
      type: "bad_request",
      message: "Failed to deserialize the JSON body into the target type: limit_microdollars: invalid type: floating point `1.5`, expected u64 at line 1 column 25",
    },
  });
  const over = await call("/api/v1/namespaces/wsp_x/budgets/2026-09", "PUT", '{"limit_microdollars":9223372036854775808}');
  assert.equal(over.status, 400);
  assert.deepEqual(await over.json(), {
    error: { type: "bad_request", message: "microdollar amount exceeds the store integer range" },
  });
  const exact = await call("/api/v1/namespaces/wsp_x/budgets/2026-09", "PUT", '{"limit_microdollars":9223372036854775807}');
  assert.equal(exact.status, 200);
  assert.equal((await exact.json()).limit_microdollars, "9223372036854775807");
  const weekly = await call("/api/v1/namespaces/wsp_x/budget", "PUT", '{"cadence":"weekly","limit_microdollars":1}');
  assert.equal(weekly.status, 400);
  assert.deepEqual(await weekly.json(), {
    error: {
      type: "bad_request",
      message: "Failed to deserialize the JSON body into the target type: cadence: unknown variant `weekly`, expected `monthly` or `fixed` at line 1 column 19",
    },
  });
  const absent = await call("/api/v1/namespaces/not-created", "GET");
  assert.equal(absent.status, 404);
  } finally {
    upstream.close();
  }
});

test("management_query_matches_the_rust_deserializer", async () => {
  const { app, upstream } = await gateway();
  try {
    const get = (path: string) => app.request(`http://127.0.0.1${path}`, {
      headers: { authorization: `Bearer ${KEY}` },
    });
    const bad = async (path: string, message: string) => {
      const response = await get(path);
      assert.equal(response.status, 400, path);
      assert.deepEqual(await response.json(), { error: { type: "bad_request", message } });
    };
    const digit = "Failed to deserialize query string: limit: invalid digit found in string";
    await bad("/api/v1/namespaces?limit=abc", digit);
    await bad("/api/v1/namespaces?limit=", "Failed to deserialize query string: limit: cannot parse integer from empty string");
    await bad("/api/v1/namespaces?limit", "Failed to deserialize query string: limit: cannot parse integer from empty string");
    await bad("/api/v1/namespaces?limit=1.5", digit);
    await bad("/api/v1/namespaces?limit=-1", digit);
    await bad("/api/v1/namespaces?limit=+10", digit);
    await bad("/api/v1/namespaces?limit=1e2", digit);
    await bad("/api/v1/namespaces?limit=%201", digit);
    await bad("/api/v1/namespaces?limit=1%20", digit);
    await bad("/api/v1/namespaces?limit=abc&limit=1", digit);
    await bad("/api/v1/namespaces?limit=4294967296", "Failed to deserialize query string: limit: number too large to fit in target type");
    await bad("/api/v1/namespaces?limit=0", "`limit` must be between 1 and 1000");
    await bad("/api/v1/namespaces?limit=1001", "`limit` must be between 1 and 1000");
    await bad("/api/v1/namespaces?limit=4294967295", "`limit` must be between 1 and 1000");
    await bad("/api/v1/namespaces?limit=1&limit=abc", "Failed to deserialize query string: duplicate field `limit`");
    await bad("/api/v1/namespaces?cursor=a&cursor=b", "Failed to deserialize query string: duplicate field `cursor`");
    await bad("/api/v1/namespaces/platform/usage?period=a&period=b", "Failed to deserialize query string: duplicate field `period`");
    await bad("/api/v1/namespaces/platform/usage?period=bad/period", "period must be 1–128 characters of [A-Za-z0-9._-]");
    await bad("/api/v1/namespaces/platform/usage", "`period` is required");
    await bad("/api/v1/namespaces/platform/usage?period=", "`period` is required");

    const padded = await get("/api/v1/namespaces?limit=010&foo=1");
    assert.equal(padded.status, 200);
    const paddedBody = await padded.json();
    assert.deepEqual(paddedBody.data.map((row: { id: string }) => row.id), ["platform", "tenant"]);
    assert.equal(paddedBody.next_cursor, undefined);

    const page = await get("/api/v1/namespaces?limit=1");
    assert.equal(page.status, 200);
    const pageBody = await page.json();
    assert.deepEqual(pageBody.data.map((row: { id: string }) => row.id), ["platform"]);
    assert.equal(pageBody.next_cursor, "platform");

    const rest = await get("/api/v1/namespaces?cursor=platform");
    assert.equal(rest.status, 200);
    assert.deepEqual((await rest.json()).data.map((row: { id: string }) => row.id), ["tenant"]);

    const spaced = await get("/api/v1/namespaces?cursor=a+b&limit=10");
    assert.equal(spaced.status, 200);
    assert.deepEqual((await spaced.json()).data.map((row: { id: string }) => row.id), ["platform", "tenant"]);

    const usage = await get("/api/v1/namespaces/platform/usage?period=compat&extra=1");
    assert.equal(usage.status, 200);
    assert.equal((await usage.json()).period, "compat");
  } finally {
    upstream.close();
  }
});

test("management_json_matches_serde_syntax_and_null_attrs", async () => {
  const { app, upstream } = await gateway();
  try {
    const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
    const call = (path: string, method: string, body?: string) =>
      app.request(`http://127.0.0.1${path}`, { method, headers, ...(body === undefined ? {} : { body }) });
    const bad = async (path: string, method: string, body: string, message: string) => {
      const response = await call(path, method, body);
      assert.equal(response.status, 400, `${method} ${path} ${body}`);
      assert.deepEqual(await response.json(), { error: { type: "bad_request", message } });
    };
    const parse = "Failed to parse the request body as JSON";
    const data = "Failed to deserialize the JSON body into the target type";
    await bad("/api/v1/namespaces", "POST", "", `${parse}: EOF while parsing a value at line 1 column 0`);
    await bad("/api/v1/namespaces", "POST", " ", `${parse}: EOF while parsing a value at line 1 column 1`);
    await bad("/api/v1/namespaces", "POST", "null", `${data}: invalid type: null, expected struct CreateBody at line 1 column 4`);
    await bad("/api/v1/namespaces", "POST", "true", `${data}: invalid type: boolean \`true\`, expected struct CreateBody at line 1 column 4`);
    await bad("/api/v1/namespaces", "POST", "0", `${data}: invalid type: integer \`0\`, expected struct CreateBody at line 1 column 1`);
    await bad("/api/v1/namespaces", "POST", "1.5", `${data}: invalid type: floating point \`1.5\`, expected struct CreateBody at line 1 column 3`);
    await bad("/api/v1/namespaces", "POST", '"x"', `${data}: invalid type: string "x", expected struct CreateBody at line 1 column 3`);
    await bad("/api/v1/namespaces", "POST", "{", `${parse}: EOF while parsing an object at line 1 column 1`);
    await bad("/api/v1/namespaces", "POST", "{]", `${parse}: key must be a string at line 1 column 2`);
    await bad("/api/v1/namespaces", "POST", '{"id":"a",}', `${parse}: trailing comma at line 1 column 11`);
    await bad("/api/v1/namespaces", "POST", '{"id":"a"', `${parse}: EOF while parsing an object at line 1 column 9`);
    await bad("/api/v1/namespaces", "POST", '{"id":}', `${parse}: id: expected value at line 1 column 7`);
    await bad("/api/v1/namespaces", "POST", '{"id":', `${parse}: id: EOF while parsing a value at line 1 column 6`);
    await bad("/api/v1/namespaces", "POST", '"', `${parse}: EOF while parsing a string at line 1 column 1`);
    await bad("/api/v1/namespaces", "POST", "[", `${parse}: EOF while parsing a list at line 1 column 1`);
    await bad("/api/v1/namespaces", "POST", "[]", `${data}: invalid length 0, expected struct CreateBody with 3 elements at line 1 column 2`);
    await bad("/api/v1/namespaces/wsp_x/budgets/2026-09", "PUT", "[]", `${data}: invalid length 0, expected struct PutBudgetBody with 1 element at line 1 column 2`);
    await bad("/api/v1/namespaces/wsp_x/budget", "PUT", '["monthly"]', `${data}: invalid length 1, expected struct PutBudgetPolicyBody with 4 elements at line 1 column 11`);
    await bad("/api/v1/namespaces/wsp_x", "PUT", "[{}, [], 1]", `${parse}: trailing characters at line 1 column 10`);
    await bad("/api/v1/namespaces", "POST", '["wsp_x", {}, [], 1]', `${parse}: trailing characters at line 1 column 19`);

    const created = await call("/api/v1/namespaces", "POST", '{"id":"wsp_null","attrs":null}');
    assert.equal(created.status, 201);
    assert.deepEqual((await created.json()).attrs, {});
    const scalar = await call("/api/v1/namespaces/wsp_null", "PUT", '{"attrs":1}');
    assert.equal(scalar.status, 200);
    assert.equal((await scalar.json()).attrs, 1);
    const text = await call("/api/v1/namespaces/wsp_null", "PUT", '{"attrs":"no"}');
    assert.equal(text.status, 200);
    assert.equal((await text.json()).attrs, "no");
    const list = await call("/api/v1/namespaces/wsp_null", "PUT", '{"attrs":[true]}');
    assert.equal(list.status, 200);
    assert.deepEqual((await list.json()).attrs, [true]);
    const reset = await call("/api/v1/namespaces/wsp_null", "PUT", "[]");
    assert.equal(reset.status, 200);
    assert.deepEqual((await reset.json()).attrs, {});
    const kept = await call("/api/v1/namespaces/wsp_null", "GET");
    assert.equal(kept.status, 200);
    assert.deepEqual((await kept.json()).attrs, {});
    const positional = await call("/api/v1/namespaces", "POST", '["wsp_pos"]');
    assert.equal(positional.status, 201);
    const positionalBody = await positional.json();
    assert.equal(positionalBody.id, "wsp_pos");
    assert.deepEqual(positionalBody.attrs, {});
    const budget = await call("/api/v1/namespaces/wsp_null/budgets/2026-09", "PUT", "[3]");
    assert.equal(budget.status, 200);
    assert.equal((await budget.json()).limit_microdollars, 3);
    const policy = await call("/api/v1/namespaces/wsp_null/budget", "PUT", '["monthly", 4]');
    assert.equal(policy.status, 200);
    const policyBody = await policy.json();
    assert.equal(policyBody.cadence, "monthly");
    assert.equal(policyBody.limit_microdollars, 4);
  } finally {
    upstream.close();
  }
});

test("management_json_rejects_duplicate_fields", async () => {
  const { app, upstream } = await gateway();
  try {
    const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
    const call = (path: string, method: string, body?: string) =>
      app.request(`http://127.0.0.1${path}`, { method, headers, ...(body === undefined ? {} : { body }) });
    const bad = async (path: string, method: string, body: string, message: string) => {
      const response = await call(path, method, body);
      assert.equal(response.status, 400, `${method} ${path} ${body}`);
      assert.deepEqual(await response.json(), { error: { type: "bad_request", message } });
    };
    const data = "Failed to deserialize the JSON body into the target type";
    const parse = "Failed to parse the request body as JSON";
    await bad("/api/v1/namespaces", "POST", '{"id":"a","id":"b"}', `${data}: duplicate field \`id\` at line 1 column 14`);
    await bad("/api/v1/namespaces", "POST", '{"id":"a","id":}', `${data}: duplicate field \`id\` at line 1 column 14`);
    await bad("/api/v1/namespaces", "POST", '{"id" : "a", "id" : "b"}', `${data}: duplicate field \`id\` at line 1 column 18`);
    await bad("/api/v1/namespaces", "POST", '{"id":"a","id"}', `${data}: duplicate field \`id\` at line 1 column 15`);
    await bad("/api/v1/namespaces", "POST", '{"id":"a","id" }', `${data}: duplicate field \`id\` at line 1 column 16`);
    await bad("/api/v1/namespaces", "POST", '{"id":"a","id" : 1}', `${data}: duplicate field \`id\` at line 1 column 15`);
    await bad("/api/v1/namespaces", "POST", '{"id":"a","id":\n"b"}', `${data}: duplicate field \`id\` at line 1 column 14`);
    await bad("/api/v1/namespaces", "POST", '{"extra" 1}', `${data}: extra: unknown field \`extra\`, expected one of \`id\`, \`attrs\`, \`blocklist\` at line 1 column 9`);
    await bad("/api/v1/namespaces", "POST", '{"id" 1}', `${parse}: expected \`:\` at line 1 column 7`);
    await bad("/api/v1/namespaces", "POST", '{"id":"', `${parse}: id: EOF while parsing a string at line 1 column 7`);
    const missing = await call("/api/v1/namespaces/wsp_dup", "GET");
    assert.equal(missing.status, 404);

    const created = await call("/api/v1/namespaces", "POST", '{"id":"wsp_dup","attrs":{"org":"acme"}}');
    assert.equal(created.status, 201);
    await bad("/api/v1/namespaces/wsp_dup", "PUT", '{"attrs":1,"attrs":2}', `${data}: duplicate field \`attrs\` at line 1 column 18`);
    await bad("/api/v1/namespaces/wsp_dup", "PUT", '{"nope"}', `${data}: nope: unknown field \`nope\`, expected \`attrs\` or \`blocklist\` at line 1 column 8`);
    await bad("/api/v1/namespaces/wsp_dup", "PUT", '{"nope" }', `${data}: nope: unknown field \`nope\`, expected \`attrs\` or \`blocklist\` at line 1 column 9`);
    const kept = await call("/api/v1/namespaces/wsp_dup", "GET");
    assert.equal(kept.status, 200);
    assert.equal((await kept.json()).attrs.org, "acme");

    const nested = await call("/api/v1/namespaces", "POST", '{"id":"wsp_nest","attrs":{"k":"a","k":"b"}}');
    assert.equal(nested.status, 201);
    assert.equal((await nested.json()).attrs.k, "b");

    const budget = await call("/api/v1/namespaces/wsp_dup/budgets/2026-09", "PUT", '{"limit_microdollars":7}');
    assert.equal(budget.status, 200);
    await bad(
      "/api/v1/namespaces/wsp_dup/budgets/2026-09",
      "PUT",
      '{"limit_microdollars":1,"limit_microdollars":2}',
      `${data}: duplicate field \`limit_microdollars\` at line 1 column 44`,
    );
    const budgetKept = await call("/api/v1/namespaces/wsp_dup/budgets/2026-09", "GET");
    assert.equal(budgetKept.status, 200);
    assert.equal((await budgetKept.json()).limit_microdollars, 7);
    await bad(
      "/api/v1/namespaces/wsp_dup/budget",
      "PUT",
      '{"cadence":"monthly","cadence":"fixed","limit_microdollars":1}',
      `${data}: duplicate field \`cadence\` at line 1 column 30`,
    );
  } finally {
    upstream.close();
  }
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

test("file_gateway_key_keeps_exact_bytes", async () => {
  const path = "/run/secrets/axond-gateway-key";
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
file = "${path}"
namespace = "platform"
`;
  const loaded = await loadConfig(toml, envSecretReader({}, async () => "secret\n"));
  assert.equal(loaded.gatewayKey, "secret\n");
  assert.equal(loaded.gatewayKeySubject, path);
  const fromEnv = await loadConfig(
    toml.replace(`file = "${path}"`, 'env = "GW_KEY"'),
    envSecretReader({ GW_KEY: "env-secret" }, async () => ""),
  );
  assert.equal(fromEnv.gatewayKey, "env-secret");
  assert.equal(fromEnv.gatewayKeySubject, "GW_KEY");
  await assert.rejects(
    () => loadConfig(toml, envSecretReader({}, async () => "")),
    (error: unknown) => {
      const message = error instanceof Error ? error.message : "";
      assert.match(message, /empty/);
      assert.equal(message.includes("secret"), false);
      return true;
    },
  );
});

test("gateway_key_subject_is_the_source_label", async () => {
  const store = createMemoryStore();
  await store.putNamespace({
    id: "platform",
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: true,
  });
  const logs: { msg: string; subject?: string }[] = [];
  const app = createAxond({
    store,
    gatewayKey: "sk-subject-sentinel",
    gatewayKeySubject: "GW_INBOUND_KEY",
    defaultNamespace: "platform",
    providers: [],
    onLog: (record) => {
      logs.push(record);
    },
  });
  const response = await app.request("http://127.0.0.1/api/v1/namespaces", {
    headers: { authorization: "Bearer sk-subject-sentinel" },
  });
  assert.equal(response.status, 200);
  await response.text();
  const line = logs.find((entry) => entry.msg === "request");
  assert.ok(line);
  assert.equal(line.subject, "GW_INBOUND_KEY");
  assert.equal(JSON.stringify(logs).includes("sk-subject-sentinel"), false);
});

test("namespace_attrs_and_blocklist_match_the_rust_limits", async () => {
  const store = createMemoryStore();
  await store.putNamespace({
    id: "platform",
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: true,
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [],
  });
  const post = (body: unknown) =>
    app.request("http://127.0.0.1/api/v1/namespaces", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  const utf8 = (value: unknown) => new TextEncoder().encode(JSON.stringify(value)).length;
  const wideAttrs = { n: "é".repeat(2045) };
  assert.ok(JSON.stringify(wideAttrs).length <= 4096);
  assert.ok(utf8(wideAttrs) > 4096);
  const wide = await post({ id: "wide-attrs", attrs: wideAttrs });
  assert.equal(wide.status, 400);
  assert.deepEqual(await wide.json(), {
    error: { type: "bad_request", message: "attrs exceeds 4096 byte limit" },
  });
  const fittingAttrs = { n: "é".repeat(2044) };
  assert.equal(utf8(fittingAttrs), 4096);
  const fitting = await post({ id: "fits-attrs", attrs: fittingAttrs });
  assert.equal(fitting.status, 201);

  const many = await post({ id: "many-globs", blocklist: Array.from({ length: 65 }, () => "foo*bar") });
  assert.equal(many.status, 400);
  assert.deepEqual(await many.json(), {
    error: { type: "bad_request", message: "namespace blocklist exceeds 64 entries" },
  });
  const hugePatterns = Array.from({ length: 64 }, () => "a".repeat(61));
  assert.ok(utf8(hugePatterns) > 4096);
  const huge = await post({ id: "huge-globs", blocklist: hugePatterns });
  assert.equal(huge.status, 400);
  assert.deepEqual(await huge.json(), {
    error: { type: "bad_request", message: "namespace blocklist exceeds 4 KiB" },
  });
  const sizedBeforeGlob = ["a*a" + "b".repeat(4090)];
  assert.ok(utf8(sizedBeforeGlob) > 4096);
  const sized = await post({ id: "sized-glob", blocklist: sizedBeforeGlob });
  assert.equal(sized.status, 400);
  assert.deepEqual(await sized.json(), {
    error: { type: "bad_request", message: "namespace blocklist exceeds 4 KiB" },
  });
  const atSize = Array.from({ length: 64 }, () => "a".repeat(60));
  assert.ok(utf8(atSize) <= 4096);
  const held = await post({ id: "held-globs", blocklist: atSize });
  assert.equal(held.status, 201);

  const middle = await post({ id: "bad-glob", blocklist: ["foo*bar"] });
  assert.equal(middle.status, 400);
  assert.deepEqual(await middle.json(), {
    error: {
      type: "bad_request",
      message: "blocklist glob `foo*bar` is invalid: use an exact id, `prefix*`, `*suffix`, or `*`",
    },
  });
  const empty = await post({ id: "empty-glob", blocklist: [""] });
  assert.equal(empty.status, 400);
  assert.deepEqual(await empty.json(), {
    error: {
      type: "bad_request",
      message: "blocklist glob `` is invalid: use an exact id, `prefix*`, `*suffix`, or `*`",
    },
  });
  const created = await post({ id: "ok-globs", blocklist: ["gpt-4o", "claude-*", "*-latest", "*"] });
  assert.equal(created.status, 201);
  const replaced = await app.request("http://127.0.0.1/api/v1/namespaces/ok-globs", {
    method: "PUT",
    headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
    body: JSON.stringify({ blocklist: ["*middle*"] }),
  });
  assert.equal(replaced.status, 400);
  assert.deepEqual(await replaced.json(), {
    error: {
      type: "bad_request",
      message: "blocklist glob `*middle*` is invalid: use an exact id, `prefix*`, `*suffix`, or `*`",
    },
  });
  const kept = await app.request("http://127.0.0.1/api/v1/namespaces/ok-globs", {
    headers: { authorization: `Bearer ${KEY}` },
  });
  assert.equal(kept.status, 200);
  assert.deepEqual((await kept.json()).blocklist, ["gpt-4o", "claude-*", "*-latest", "*"]);
});

test("namespace_create_uses_the_rust_identifier_messages", async () => {
  const store = createMemoryStore();
  await store.putNamespace({
    id: "platform",
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: true,
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [],
  });
  const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
  const post = (body: string) =>
    app.request("http://127.0.0.1/api/v1/namespaces", { method: "POST", headers, body });
  const refused = async (body: string, message: string) => {
    const response = await post(body);
    assert.equal(response.status, 400);
    const payload = await response.json();
    assert.deepEqual(payload, { error: { type: "bad_request", message } });
    assert.equal(JSON.stringify(payload).includes("é"), false);
    assert.equal(JSON.stringify(payload).includes("cafe"), false);
  };
  const empty = "a namespace identifier must not be empty";
  const long = "a namespace identifier is over the 128-byte limit";
  const character =
    "a namespace identifier contains a character outside ASCII letters, digits, `.`, `-`, and `_`";
  const boundary = "a namespace identifier must start and end with an ASCII letter or digit";
  await refused('{"id":""}', empty);
  await refused(`{"id":"${"a".repeat(129)}"}`, long);
  await refused(`{"id":"${"é".repeat(65)}"}`, long);
  await refused(`{"id":"${"é".repeat(64)}"}`, character);
  await refused('{"id":"a b"}', character);
  await refused('{"id":"café"}', character);
  await refused('{"id":"-"}', boundary);
  await refused('{"id":"a_"}', boundary);
  await refused('{"id":"_a"}', boundary);
  await refused('{"id":"","attrs":{"org":"acme"}}', empty);
  const longest = await post(`{"id":"${"a".repeat(128)}"}`);
  assert.equal(longest.status, 201);
  const shaped = await post('{"id":"Acme_01-prod"}');
  assert.equal(shaped.status, 201);
  const absent = await app.request("http://127.0.0.1/api/v1/namespaces/-bad", {
    headers: { authorization: `Bearer ${KEY}` },
  });
  assert.equal(absent.status, 404);
  assert.deepEqual(await absent.json(), {
    error: { type: "unknown_namespace", message: "unknown namespace" },
  });
  const removed = await app.request("http://127.0.0.1/api/v1/namespaces/-bad", {
    method: "DELETE",
    headers: { authorization: `Bearer ${KEY}` },
  });
  assert.equal(removed.status, 204);
  const periodFirst = await app.request("http://127.0.0.1/api/v1/namespaces/-bad/usage", {
    headers: { authorization: `Bearer ${KEY}` },
  });
  assert.equal(periodFirst.status, 400);
  assert.deepEqual(await periodFirst.json(), {
    error: { type: "bad_request", message: "`period` is required" },
  });
  const unknownUsage = await app.request("http://127.0.0.1/api/v1/namespaces/-bad/usage?period=compat", {
    headers: { authorization: `Bearer ${KEY}` },
  });
  assert.equal(unknownUsage.status, 404);
  assert.deepEqual(await unknownUsage.json(), {
    error: { type: "unknown_namespace", message: "unknown namespace" },
  });
  const unknownBudget = await app.request("http://127.0.0.1/api/v1/namespaces/-bad/budgets/2026-09", {
    method: "PUT",
    headers,
    body: '{"limit_microdollars":1}',
  });
  assert.equal(unknownBudget.status, 404);
  assert.deepEqual(await unknownBudget.json(), {
    error: { type: "unknown_namespace", message: "unknown namespace" },
  });
  const inference = await app.request("http://127.0.0.1/ns/-bad/v1/models", {
    headers: { authorization: `Bearer ${KEY}` },
  });
  assert.equal(inference.status, 400);
  assert.deepEqual(await inference.json(), {
    error: { type: "invalid_namespace", message: "namespace identifier is invalid" },
  });
  const listed = await app.request("http://127.0.0.1/api/v1/namespaces", {
    headers: { authorization: `Bearer ${KEY}` },
  });
  const ids = (await listed.json()).data.map((row: { id: string }) => row.id);
  assert.deepEqual(ids.filter((id: string) => id === "" || id === "-bad" || id.includes("é")), []);
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
  assert.equal(loaded.transport.maxResponseBytes, 32 * 1024 * 1024);
  assert.equal(loaded.transport.maxErrorBytes, 64 * 1024);
  assert.equal(loaded.transport.connectTimeoutMs, 5_000);
  assert.equal(loaded.transport.streamTerminalGraceMs, 1_000);
  assert.equal(loaded.transport.overallTimeoutMs, 30_000);
  assert.equal(loaded.transport.maxAttempts, 3);
  assert.equal(loaded.admission.maxInFlight, 1024);
  assert.equal(loaded.admission.maxInFlightStreams, 512);
  assert.equal(loaded.admission.queueCapacity, 0);
  assert.equal(loaded.admission.queueWaitMs, 0);
  assert.equal(loaded.admission.maxPendingSettlements, 4096);
  assert.equal(loaded.admission.maxInFlightSettlements, 64);
  assert.equal(loaded.admission.settlementQueueWaitMs, 10_000);
  assert.equal(loaded.admission.settlementTimeoutMs, 10_000);
  assert.equal(loaded.maxRequestBytes, 2 * 1024 * 1024);
  assert.equal(loaded.maxPromptTokens, 1_000_000);
  assert.equal(loaded.maxOutputTokens, 200_000);
  assert.equal(loaded.maxStreamDurationMs, 3_600_000);
  assert.equal(loaded.maxStreamBytes, 64 * 1024 * 1024);
  assert.deepEqual(loaded.credentialPool, { strategy: "round-robin", failureThreshold: 2, cooldownSeconds: 30 });
  assert.equal(loaded.storage.onUnavailable, "deny");
  const allowed = await loadConfig(
    toml.replace('path = "/tmp/axond.sqlite"', 'path = "/tmp/axond.sqlite"\non_unavailable = "allow"'),
    envSecretReader({ GW_KEY: "k" }, async () => ""),
  );
  assert.equal(allowed.storage.onUnavailable, "allow");
  await assert.rejects(
    () =>
      loadConfig(
        toml.replace('path = "/tmp/axond.sqlite"', 'path = "/tmp/axond.sqlite"\non_unavailable = "sometimes"'),
        envSecretReader({ GW_KEY: "k" }, async () => ""),
      ),
    (error: unknown) => {
      assert.match(error instanceof Error ? error.message : "", /on_unavailable/);
      return true;
    },
  );
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
  await assert.rejects(
    () =>
      loadConfig(
        `${toml}\n[transport]\nconnect_timeout_ms = 0\n`,
        envSecretReader({ GW_KEY: "k" }, async () => ""),
      ),
    (error: unknown) => {
      assert.match(error instanceof Error ? error.message : "", /transport\.connect_timeout_ms must be at least 1/);
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(
        `${toml}\n[failover]\noverall_timeout_ms = 0\n`,
        envSecretReader({ GW_KEY: "k" }, async () => ""),
      ),
    (error: unknown) => {
      assert.match(error instanceof Error ? error.message : "", /failover\.overall_timeout_ms must be at least 1/);
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(
        `${toml}\n[failover]\nmax_attempts = 0\n`,
        envSecretReader({ GW_KEY: "k" }, async () => ""),
      ),
    (error: unknown) => {
      assert.match(error instanceof Error ? error.message : "", /failover\.max_attempts must be an integer of at least 1/);
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(
        `${toml}\n[failover]\nmax_attempts = 1.5\n`,
        envSecretReader({ GW_KEY: "k" }, async () => ""),
      ),
    (error: unknown) => {
      assert.match(error instanceof Error ? error.message : "", /failover\.max_attempts must be an integer of at least 1/);
      return true;
    },
  );
});

test("admission ceilings load from toml and reject a contradictory queue", async () => {
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
[admission]
max_in_flight = 16
`;
  const loaded = await loadConfig(toml, envSecretReader({ GW_KEY: "k" }, async () => ""));
  assert.equal(loaded.admission.maxInFlight, 16);
  assert.equal(loaded.admission.maxInFlightStreams, 16);
  assert.equal(loaded.admission.streamsExplicit, false);
  assert.equal(loaded.admission.maxPendingSettlements, 64);
  assert.equal(loaded.admission.pendingExplicit, false);
  assert.equal(loaded.admission.maxInFlightSettlements, 64);
  assert.equal(loaded.admission.settlementQueueWaitMs, 10_000);
  assert.equal(loaded.admission.settlementTimeoutMs, 10_000);
  const written = await loadConfig(
    `${toml}max_in_flight_streams = 4\nqueue_capacity = 2\nqueue_wait_ms = 50\nmax_pending_settlements = 16\n`,
    envSecretReader({ GW_KEY: "k" }, async () => ""),
  );
  assert.equal(written.admission.maxInFlightStreams, 4);
  assert.equal(written.admission.streamsExplicit, true);
  assert.equal(written.admission.queueCapacity, 2);
  assert.equal(written.admission.queueWaitMs, 50);
  assert.equal(written.admission.maxPendingSettlements, 16);
  await assert.rejects(
    () =>
      loadConfig(
        `${toml}max_in_flight_streams = 32\n`,
        envSecretReader({ GW_KEY: "k" }, async () => ""),
      ),
    (error: unknown) => {
      assert.match(
        error instanceof Error ? error.message : "",
        /admission\.max_in_flight_streams \(32\) must not exceed admission\.max_in_flight \(16\)/,
      );
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(
        `${toml}queue_capacity = 2\n`,
        envSecretReader({ GW_KEY: "k" }, async () => ""),
      ),
    (error: unknown) => {
      assert.match(error instanceof Error ? error.message : "", /must be set together/);
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(
        toml.replace("max_in_flight = 16", "max_in_flight = 0\nqueue_capacity = 1\nqueue_wait_ms = 10"),
        envSecretReader({ GW_KEY: "k" }, async () => ""),
      ),
    (error: unknown) => {
      assert.match(error instanceof Error ? error.message : "", /queue_capacity requires admission\.max_in_flight/);
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(
        `${toml}max_pending_settlements = 4\n`,
        envSecretReader({ GW_KEY: "k" }, async () => ""),
      ),
    (error: unknown) => {
      assert.match(
        error instanceof Error ? error.message : "",
        /max_pending_settlements \(4\) must be at least admission\.max_in_flight \(16\)/,
      );
      return true;
    },
  );
  const disabled = await loadConfig(
    `${toml}max_in_flight_settlements = 0\nsettlement_queue_wait_ms = 0\nsettlement_timeout_ms = 0\n`,
    envSecretReader({ GW_KEY: "k" }, async () => ""),
  );
  assert.equal(disabled.admission.maxInFlightSettlements, 0);
  assert.equal(disabled.admission.settlementQueueWaitMs, 0);
  assert.equal(disabled.admission.settlementTimeoutMs, 0);
  await assert.rejects(
    () =>
      loadConfig(
        `${toml}max_in_flight_settlements = 1.5\n`,
        envSecretReader({ GW_KEY: "k" }, async () => ""),
      ),
    (error: unknown) => {
      assert.match(
        error instanceof Error ? error.message : "",
        /admission\.max_in_flight_settlements must be an integer of at least 0/,
      );
      return true;
    },
  );
});

test("credential pool threshold, cooldown, and weight load from toml", async () => {
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
[[provider]]
id = "fake-openai"
kind = "openai"
base_url = "http://127.0.0.1:9"
[[credential]]
namespace = "platform"
provider = "fake-openai"
env = "OPENAI_KEY"
id = "light"
weight = 1
[[credential]]
namespace = "platform"
provider = "fake-openai"
env = "OPENAI_KEY"
id = "heavy"
weight = 3
[credential_pool]
strategy = "weighted"
failure_threshold = 1
cooldown_seconds = 5
[failover]
max_attempts = 1
`;
  const loaded = await loadConfig(toml, envSecretReader({ GW_KEY: "k", OPENAI_KEY: "sk" }, async () => ""));
  assert.deepEqual(loaded.credentialPool, { strategy: "weighted", failureThreshold: 1, cooldownSeconds: 5 });
  assert.equal(loaded.transport.maxAttempts, 1);
  assert.deepEqual(
    loaded.credentials.map((credential) => credential.weight),
    [1, 3],
  );
  assert.equal(loaded.credentials[0]?.explicitId, undefined);
  const derived = await loadConfig(
    toml.replace('id = "light"\n', ""),
    envSecretReader({ GW_KEY: "k", OPENAI_KEY: "sk" }, async () => ""),
  );
  assert.equal(derived.credentials[0]?.id, "OPENAI_KEY");
  assert.equal(derived.credentials[0]?.explicitId, false);
  assert.equal(derived.credentials[1]?.explicitId, undefined);
  await assert.rejects(
    () =>
      loadConfig(
        toml.replace("failure_threshold = 1", "failure_threshold = 0"),
        envSecretReader({ GW_KEY: "k", OPENAI_KEY: "sk" }, async () => ""),
      ),
    (error: unknown) => {
      assert.match(error instanceof Error ? error.message : "", /credential_pool\.failure_threshold must be at least 1/);
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(
        toml.replace("cooldown_seconds = 5", "cooldown_seconds = 0"),
        envSecretReader({ GW_KEY: "k", OPENAI_KEY: "sk" }, async () => ""),
      ),
    (error: unknown) => {
      assert.match(error instanceof Error ? error.message : "", /credential_pool\.cooldown_seconds must be at least 1/);
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(
        toml.replace("weight = 3", "weight = 0"),
        envSecretReader({ GW_KEY: "k", OPENAI_KEY: "sk" }, async () => ""),
      ),
    (error: unknown) => {
      assert.match(error instanceof Error ? error.message : "", /credential weight must be at least 1/);
      return true;
    },
  );
});

test("transport and admission byte limits load from toml", async () => {
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
[transport]
max_response_bytes = 4096
max_error_bytes = 128
[admission]
max_request_bytes = 64
`;
  const loaded = await loadConfig(toml, envSecretReader({ GW_KEY: "k" }, async () => ""));
  assert.equal(loaded.transport.maxResponseBytes, 4096);
  assert.equal(loaded.transport.maxErrorBytes, 128);
  assert.equal(loaded.maxRequestBytes, 64);
  await assert.rejects(
    () =>
      loadConfig(
        toml.replace("max_response_bytes = 4096", "max_response_bytes = 0"),
        envSecretReader({ GW_KEY: "k" }, async () => ""),
      ),
    (error: unknown) => {
      assert.match(error instanceof Error ? error.message : "", /transport\.max_response_bytes must be at least 1/);
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(
        toml.replace("max_error_bytes = 128", "max_error_bytes = 0"),
        envSecretReader({ GW_KEY: "k" }, async () => ""),
      ),
    (error: unknown) => {
      assert.match(error instanceof Error ? error.message : "", /transport\.max_error_bytes must be at least 1/);
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(
        toml.replace("max_error_bytes = 128", "max_error_bytes = 8192"),
        envSecretReader({ GW_KEY: "k" }, async () => ""),
      ),
    (error: unknown) => {
      assert.match(
        error instanceof Error ? error.message : "",
        /transport\.max_error_bytes must not exceed transport\.max_response_bytes/,
      );
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(
        toml.replace("max_request_bytes = 64", "max_request_bytes = 0"),
        envSecretReader({ GW_KEY: "k" }, async () => ""),
      ),
    (error: unknown) => {
      assert.match(error instanceof Error ? error.message : "", /admission\.max_request_bytes must be at least 1/);
      return true;
    },
  );
  const disabled = await loadConfig(
    toml.replace("max_request_bytes = 64", "max_request_bytes = 64\nmax_prompt_tokens = 0\nmax_output_tokens = 0"),
    envSecretReader({ GW_KEY: "k" }, async () => ""),
  );
  assert.equal(disabled.maxPromptTokens, 0);
  assert.equal(disabled.maxOutputTokens, 0);
  await assert.rejects(
    () =>
      loadConfig(
        toml.replace("max_request_bytes = 64", "max_request_bytes = 64\nmax_prompt_tokens = -1"),
        envSecretReader({ GW_KEY: "k" }, async () => ""),
      ),
    (error: unknown) => {
      assert.match(error instanceof Error ? error.message : "", /admission\.max_prompt_tokens must be an integer of at least 0/);
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(
        toml.replace("max_request_bytes = 64", "max_request_bytes = 64\nmax_output_tokens = 1.5"),
        envSecretReader({ GW_KEY: "k" }, async () => ""),
      ),
    (error: unknown) => {
      assert.match(error instanceof Error ? error.message : "", /admission\.max_output_tokens must be an integer of at least 0/);
      return true;
    },
  );
  const streamsOff = await loadConfig(
    toml.replace(
      "max_request_bytes = 64",
      "max_request_bytes = 64\nmax_stream_duration_ms = 0\nmax_stream_bytes = 0",
    ),
    envSecretReader({ GW_KEY: "k" }, async () => ""),
  );
  assert.equal(streamsOff.maxStreamDurationMs, 0);
  assert.equal(streamsOff.maxStreamBytes, 0);
  await assert.rejects(
    () =>
      loadConfig(
        toml.replace("max_request_bytes = 64", "max_request_bytes = 64\nmax_stream_duration_ms = -1"),
        envSecretReader({ GW_KEY: "k" }, async () => ""),
      ),
    (error: unknown) => {
      assert.match(
        error instanceof Error ? error.message : "",
        /admission\.max_stream_duration_ms must be an integer of at least 0/,
      );
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(
        toml.replace("max_request_bytes = 64", "max_request_bytes = 64\nmax_stream_bytes = 1.5"),
        envSecretReader({ GW_KEY: "k" }, async () => ""),
      ),
    (error: unknown) => {
      assert.match(
        error instanceof Error ? error.message : "",
        /admission\.max_stream_bytes must be an integer of at least 0/,
      );
      return true;
    },
  );
});

test("an oversized request is 413 and the body is not echoed", async () => {
  const store = createMemoryStore();
  await store.putNamespace({
    id: "platform",
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: true,
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    maxRequestBytes: 32,
    providers: [],
  });
  const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
    body: JSON.stringify({ model: "fake-openai/gpt-test", note: "BODY_SENTINEL" }),
  });
  assert.equal(response.status, 413);
  const text = await response.text();
  assert.equal(text.includes("BODY_SENTINEL"), false);
  assert.deepEqual(JSON.parse(text), {
    error: { type: "request_too_large", message: "request body exceeds the configured inbound limit" },
  });
});

test("prompt and output ceilings refuse before dispatch and do not echo the request", async () => {
  const store = createMemoryStore();
  await store.putNamespace({
    id: "platform",
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: true,
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    maxPromptTokens: 64,
    maxOutputTokens: 16,
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
  const chat = (body: unknown) =>
    app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify(body),
    });

  const prompt = await chat({
    model: "fake-openai/gpt-test",
    messages: [{ role: "user", content: "sensitive ".repeat(32) }],
  });
  assert.equal(prompt.status, 413);
  const promptText = await prompt.text();
  assert.equal(promptText.includes("sensitive"), false);
  assert.deepEqual(JSON.parse(promptText), {
    error: { type: "prompt_too_large", message: "prompt exceeds the configured limit of 64 tokens" },
  });

  const output = await chat({
    model: "fake-openai/gpt-test",
    messages: [],
    max_tokens: 8,
    max_completion_tokens: 4096,
  });
  assert.equal(output.status, 400);
  assert.deepEqual(await output.json(), {
    error: {
      type: "output_limit_exceeded",
      message: "requested output of 4096 tokens exceeds the configured limit of 16 tokens",
    },
  });

  const spelled = await chat({
    model: "fake-openai/gpt-test",
    messages: [],
    max_tokens: "many",
    max_output_tokens: 64,
  });
  assert.equal(spelled.status, 400);
  assert.deepEqual(await spelled.json(), {
    error: {
      type: "output_limit_exceeded",
      message: "requested output of 64 tokens exceeds the configured limit of 16 tokens",
    },
  });

  const atCeiling = await chat({
    model: "fake-openai/gpt-test",
    messages: [],
    max_tokens: 16,
  });
  assert.equal(atCeiling.status, 429);

  const negative = await chat({
    model: "fake-openai/gpt-test",
    messages: [],
    max_tokens: -1,
  });
  assert.equal(negative.status, 429);

  const multibyte = {
    model: "fake-openai/gpt-test",
    messages: [{ role: "user", content: "é".repeat(40) }],
  };
  const encoded = JSON.stringify(multibyte);
  const utf8Tokens = Math.floor(new TextEncoder().encode(encoded).length / 4);
  const utf16Tokens = Math.floor(encoded.length / 4);
  assert.ok(utf8Tokens > utf16Tokens);
  const bytes = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    maxPromptTokens: utf16Tokens,
    maxOutputTokens: 0,
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: "http://127.0.0.1:9" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
  });
  const wide = await bytes.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
    body: encoded,
  });
  assert.equal(wide.status, 413);
  const wideText = await wide.text();
  assert.equal(wideText.includes("é"), false);
  assert.equal(JSON.parse(wideText).error.type, "prompt_too_large");

  const off = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    maxPromptTokens: 0,
    maxOutputTokens: 0,
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: "http://127.0.0.1:9" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
  });
  const disabled = await off.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
    body: JSON.stringify({
      model: "fake-openai/gpt-test",
      messages: [{ role: "user", content: "sensitive ".repeat(32) }],
      max_tokens: 4096,
    }),
  });
  assert.equal(disabled.status, 429);
  const disabledText = await disabled.text();
  assert.equal(disabledText.includes("sensitive"), false);
});

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
      assert.deepEqual(await response.json(), { error: { type: "not_found", message: "not found" } });
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
