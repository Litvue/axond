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


import { KEY, gateway, listenUpstream } from "./app-test-fixtures.ts";

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
  const deadline = Date.now() + 2000;
  let summary = await store.summarizeUsage("platform", "compat");
  while (summary.length === 0 && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 5));
    summary = await store.summarizeUsage("platform", "compat");
  }
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
    error: { type: "bad_request", message: "bad request: model id after `/` must not be empty" },
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
      error: { type: "bad_request", message: "bad request: missing `model`" },
    });
  }
  const invalid = await post("{");
  assert.equal(invalid.status, 400);
  assert.deepEqual(await invalid.json(), {
    error: { type: "bad_request", message: "bad request: request body is not valid JSON" },
  });
  const utf8 = await post(new Uint8Array([0xff, 0xfe]));
  assert.equal(utf8.status, 400);
  assert.deepEqual(await utf8.json(), {
    error: { type: "bad_request", message: "bad request: request body is not valid JSON" },
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
      message: "bad request: Failed to deserialize the JSON body into the target type: attr: unknown field `attr`, expected `attrs` or `blocklist` at line 1 column 7",
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
      message: "bad request: Failed to deserialize the JSON body into the target type: extra: unknown field `extra`, expected one of `id`, `attrs`, `blocklist` at line 1 column 21",
    },
  });
  const missing = await call("/api/v1/namespaces", "POST", "{}");
  assert.equal(missing.status, 400);
  assert.deepEqual(await missing.json(), {
    error: {
      type: "bad_request",
      message: "bad request: Failed to deserialize the JSON body into the target type: missing field `id` at line 1 column 2",
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
      message: 'bad request: Failed to deserialize the JSON body into the target type: limit_microdollars: invalid type: string "10", expected u64 at line 1 column 26',
    },
  });
  const floatLimit = await call("/api/v1/namespaces/wsp_x/budgets/2026-09", "PUT", '{"limit_microdollars":1.5}');
  assert.equal(floatLimit.status, 400);
  assert.deepEqual(await floatLimit.json(), {
    error: {
      type: "bad_request",
      message: "bad request: Failed to deserialize the JSON body into the target type: limit_microdollars: invalid type: floating point `1.5`, expected u64 at line 1 column 25",
    },
  });
  const over = await call("/api/v1/namespaces/wsp_x/budgets/2026-09", "PUT", '{"limit_microdollars":9223372036854775808}');
  assert.equal(over.status, 400);
  assert.deepEqual(await over.json(), {
    error: { type: "bad_request", message: "bad request: microdollar amount exceeds the store integer range" },
  });
  const exact = await call("/api/v1/namespaces/wsp_x/budgets/2026-09", "PUT", '{"limit_microdollars":9223372036854775807}');
  assert.equal(exact.status, 200);
  assert.equal(
    await exact.text(),
    '{"namespace":"wsp_x","period":"2026-09","limit_microdollars":9223372036854775807,"spent_microdollars":0,"reserved_microdollars":0,"remaining_microdollars":9223372036854775807,"active":true}',
  );
  const weekly = await call("/api/v1/namespaces/wsp_x/budget", "PUT", '{"cadence":"weekly","limit_microdollars":1}');
  assert.equal(weekly.status, 400);
  assert.deepEqual(await weekly.json(), {
    error: {
      type: "bad_request",
      message: "bad request: Failed to deserialize the JSON body into the target type: cadence: unknown variant `weekly`, expected `monthly` or `fixed` at line 1 column 19",
    },
  });
  const absent = await call("/api/v1/namespaces/not-created", "GET");
  assert.equal(absent.status, 404);
  } finally {
    upstream.close();
  }
});


test("budget_amounts_match_serde_numbers", async () => {
  const { app, upstream } = await gateway();
  try {
    const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
    const call = (path: string, method: string, body?: string) =>
      app.request(`http://127.0.0.1${path}`, { method, headers, ...(body === undefined ? {} : { body }) });
    assert.equal((await call("/api/v1/namespaces", "POST", '{"id":"wsp_amt"}')).status, 201);
    const cap = "9223372036854775807";
    const ledger =
      `{"namespace":"wsp_amt","period":"2026-09","limit_microdollars":${cap},` +
      `"spent_microdollars":0,"reserved_microdollars":0,"remaining_microdollars":${cap},"active":true}`;
    const put = await call("/api/v1/namespaces/wsp_amt/budgets/2026-09", "PUT", `{"limit_microdollars":${cap}}`);
    assert.equal(put.status, 200);
    assert.equal(await put.text(), ledger);
    const got = await call("/api/v1/namespaces/wsp_amt/budgets/2026-09", "GET");
    assert.equal(got.status, 200);
    assert.equal(await got.text(), ledger);
    const policyBody =
      `{"namespace":"wsp_amt","cadence":"fixed","limit_microdollars":${cap},"timezone":"UTC",` +
      `"period":"2026-09","spent_microdollars":0,"reserved_microdollars":0,"remaining_microdollars":${cap},"active":true}`;
    const policy = await call(
      "/api/v1/namespaces/wsp_amt/budget",
      "PUT",
      `{"cadence":"fixed","limit_microdollars":${cap},"period":"2026-09","timezone":"UTC"}`,
    );
    assert.equal(policy.status, 200);
    assert.equal(await policy.text(), policyBody);
    const policyGet = await call("/api/v1/namespaces/wsp_amt/budget", "GET");
    assert.equal(policyGet.status, 200);
    assert.equal(await policyGet.text(), policyBody);
    const small = await call("/api/v1/namespaces/wsp_amt/budgets/2026-08", "PUT", '{"limit_microdollars":3}');
    assert.equal(small.status, 200);
    assert.equal(
      await small.text(),
      '{"namespace":"wsp_amt","period":"2026-08","limit_microdollars":3,"spent_microdollars":0,"reserved_microdollars":0,"remaining_microdollars":3,"active":true}',
    );
  } finally {
    upstream.close();
  }
});

test("cumulative metric snapshots are bounded per gateway while traces remain per request", async () => {
  const tasks: Promise<void>[] = []; const signals: string[] = []; const metrics = createMetrics();
  const app = createAxond({ store: createMemoryStore(), gatewayKey: KEY, defaultNamespace: "platform", providers: [], metrics,
    telemetry: { endpoint: "http://collector", fetch: async (url) => { signals.push(String(url)); return new Response(null, { status: 200 }); } },
    onBackground: (task) => { tasks.push(task); } });
  for (let i = 0; i < 3; i++) { assert.equal((await app.request("http://localhost/healthz")).status, 200); await Promise.all(tasks); }
  assert.equal(signals.filter((url) => url.endsWith("/v1/traces")).length, 3);
  assert.equal(signals.filter((url) => url.endsWith("/v1/metrics")).length, 1);
});


test("failed metric exports retry and shared recorders do not suppress other collectors", async () => {
  const metrics = createMetrics(), tasks: Promise<void>[] = []; const posts: string[] = []; let failed = false;
  const build = (endpoint: string) => createAxond({ store: createMemoryStore(), gatewayKey: KEY, defaultNamespace: "platform", providers: [], metrics,
    telemetry: { endpoint, fetch: async url => { const signal = String(url); posts.push(signal); if (signal.endsWith("/metrics") && !failed) { failed = true; return new Response(null, { status: 503 }); } return new Response(null, { status: 200 }); } },
    onBackground: task => { tasks.push(task); } });
  const one = build("http://collector-one"), two = build("http://collector-two");
  for (const app of [one, one, two]) { await app.request("http://localhost/healthz"); await Promise.all(tasks); }
  assert.equal(posts.filter(url => url === "http://collector-one/v1/metrics").length, 2);
  assert.equal(posts.filter(url => url === "http://collector-two/v1/metrics").length, 1);
});
