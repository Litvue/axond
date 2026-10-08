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


import { KEY, seeded, listen, MINTED_REQUEST_ID, concatChunks, MESSAGES_TERMINAL, messagesApp, CHAT_DONE, chatDoneApp, poolApp, CHAT_HEADERS, estimateCost } from "./behavior-test-fixtures.ts";

test("provider_refusals_keep_their_class_and_export_bounded_attempt_diagnostics", async () => {
  const secret = "sk-refusal-sentinel";
  const marker = "… [truncated]";
  const cases = [
    { status: 400, message: "input_schema does not support oneOf", http: 400, type: "invalid_request" },
    { status: 422, message: "invalid field", http: 400, type: "invalid_request" },
    { status: 401, message: "invalid provider credentials", http: 502, type: "invalid_request" },
    { status: 403, message: "provider access denied", http: 502, type: "invalid_request" },
    { status: 400, message: "context window exceeded", http: 400, type: "context_window_exceeded" },
    { status: 404, message: "requested model not found", http: 502, type: "model_unavailable" },
    { status: 429, message: "quota exhausted", http: 502, type: "provider_dependency_failed" },
    { status: 503, message: "provider overloaded", http: 502, type: "provider_dependency_failed" },
  ];
  const routes = [
    {
      path: "/ns/platform/v1/chat/completions",
      body: (stream: boolean) => ({ model: "fake-openai/gpt-test", stream, messages: [{ role: "user", content: "hello" }] }),
    },
    {
      path: "/ns/platform/v1/responses",
      body: (stream: boolean) => ({ model: "fake-openai/gpt-test", stream, input: "hello" }),
    },
    {
      path: "/ns/platform/v1/messages",
      body: (stream: boolean) => ({
        model: "fake-anthropic/claude-test",
        stream,
        max_tokens: 16,
        messages: [{ role: "user", content: "hello" }],
      }),
    },
  ];
  let current = cases[0]!;
  let hits = 0;
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
  const collectorAddress = await new Promise<import("node:net").AddressInfo>((resolve) => {
    collector.listen(0, "127.0.0.1", () => {
      const address = collector.address();
      if (!address || typeof address === "string") {
        throw new Error("no port");
      }
      resolve(address);
    });
  });
  const upstream = await listen((_req, res) => {
    hits += 1;
    const diagnostic = `${current.message}; rejected key ${secret}; ${"界".repeat(1800)}`;
    res.writeHead(current.status, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: { message: diagnostic } }));
  });
  const store = await seeded();
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [
      { id: "fake-openai", kind: "openai", baseUrl: upstream.url },
      { id: "fake-anthropic", kind: "anthropic", baseUrl: upstream.url },
    ],
    credentials: [
      { namespace: "platform", provider: "fake-openai", secret, id: "openai" },
      { namespace: "platform", provider: "fake-anthropic", secret, id: "anthropic" },
    ],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
      {
        provider: "fake-anthropic",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
    telemetry: { endpoint: `http://127.0.0.1:${collectorAddress.port}` },
  });
  const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
  try {
    let expectedTraces = 0;
    for (const route of routes) {
      for (const stream of [false, true]) {
        for (const item of cases) {
          current = item;
          hits = 0;
          const response = await app.request(`http://127.0.0.1${route.path}`, {
            method: "POST",
            headers,
            body: JSON.stringify(route.body(stream)),
          });
          const text = await response.text();
          const label = `${route.path} stream=${stream} upstream=${item.status}`;
          assert.equal(response.status, item.http, label);
          const body = JSON.parse(text) as { error: { type: string; message: string } };
          assert.equal(body.error.type, item.type, label);
          assert.equal(body.error.message.startsWith(item.message), true, label);
          assert.equal(body.error.message.includes("[REDACTED]"), true, label);
          assert.equal(body.error.message.endsWith(marker), true, label);
          assert.equal(text.includes(secret), false, label);
          assert.equal(hits, 1, label);
          expectedTraces += 1;
          const deadline = Date.now() + 2_000;
          while (traces.length < expectedTraces && Date.now() < deadline) {
            await new Promise((resolve) => setTimeout(resolve, 10));
          }
          assert.equal(traces.length >= expectedTraces, true, label);
          const spans = JSON.parse(traces[expectedTraces - 1]!).resourceSpans[0].scopeSpans[0].spans as {
            name: string;
            status?: { code?: number };
            attributes: { key: string; value: { stringValue?: string } }[];
          }[];
          const attempts = spans.filter((span) => span.name === "axond.upstream.attempt");
          assert.equal(attempts.length, 1, label);
          const attempt = attempts[0]!;
          assert.equal(attempt.status?.code, 2, label);
          const attr = (key: string) => attempt.attributes.find((entry) => entry.key === key)?.value.stringValue;
          assert.equal(attr("axond.status"), "error", label);
          assert.equal(attr("axond.upstream.status"), String(item.status), label);
          const recorded = attr("axond.upstream.message") ?? "";
          assert.equal(recorded.startsWith(item.message), true, label);
          assert.equal(recorded.includes("[REDACTED]"), true, label);
          assert.equal(recorded.includes(marker), false, label);
          assert.ok(new TextEncoder().encode(recorded).length <= 4096, label);
          assert.equal(traces[expectedTraces - 1]!.includes(secret), false, label);
        }
      }
    }
  } finally {
    upstream.close();
    collector.closeAllConnections();
    collector.close();
  }
});


test("malformed_responses_controls_never_reach_the_provider", async () => {
  const secret = "malformed-control@example.com";
  let hits = 0;
  const upstream = await listen((_req, res) => {
    hits += 1;
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ id: "resp_ok", output: [], usage: { input_tokens: 1, output_tokens: 1 } }));
  });
  const store = await seeded();
  const logs: unknown[] = [];
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "sk-control", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
    onLog: (record) => {
      logs.push(record);
    },
  });
  const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
  const refused: Array<{ path: string; body: unknown; message: string }> = [
    {
      path: "/ns/platform/v1/responses",
      body: { model: "fake-openai/gpt-test", input: "ordinary", stream: secret },
      message: "`stream` must be a boolean when present",
    },
    {
      path: "/ns/platform/v1/chat/completions",
      body: { model: "fake-openai/gpt-test", messages: [], previous_response_id: { value: secret } },
      message: "`previous_response_id` must be a string or null when present",
    },
    {
      path: "/ns/platform/v1/responses",
      body: { model: "fake-openai/gpt-test", input: "ordinary", stream: true, previous_response_id: { value: secret } },
      message: "`previous_response_id` must be a string or null when present",
    },
    {
      path: "/ns/platform/v1/embeddings",
      body: { model: "fake-openai/gpt-test", input: "hello", stream: true },
      message: "/v1/embeddings does not support streaming",
    },
  ];
  try {
    for (const item of refused) {
      hits = 0;
      const response = await app.request(`http://127.0.0.1${item.path}`, {
        method: "POST",
        headers,
        body: JSON.stringify(item.body),
      });
      const text = await response.text();
      assert.equal(response.status, 400, item.path);
      const body = JSON.parse(text) as { error: { type: string; message: string } };
      assert.equal(body.error.type, "bad_request", item.path);
      assert.equal(body.error.message, `bad request: ${item.message}`, item.path);
      assert.equal(text.includes(secret), false, item.path);
      assert.equal(hits, 0, item.path);
    }
    for (const body of [
      { model: "fake-openai/gpt-test", input: "ordinary", stream: false, previous_response_id: null },
      { model: "fake-openai/gpt-test", input: "ordinary", previous_response_id: "resp_1" },
    ]) {
      const before = hits;
      const response = await app.request("http://127.0.0.1/ns/platform/v1/responses", {
        method: "POST",
        headers,
        body: JSON.stringify(body),
      });
      assert.equal(response.status, 200);
      await response.text();
      assert.equal(hits, before + 1);
    }
    const encoded = JSON.stringify(logs);
    assert.equal(encoded.includes(secret), false);
    assert.equal(encoded.includes(KEY), false);
  } finally {
    upstream.close();
  }
});


test("chat_stream_forces_include_usage_and_keeps_other_bytes", async () => {
  const seen: string[] = [];
  const upstream = await listen(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) {
      chunks.push(chunk as Buffer);
    }
    seen.push(Buffer.concat(chunks).toString("utf8"));
    const path = req.url ?? "";
    res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
    if (path.endsWith("/messages")) {
      res.end('event: message_stop\ndata: {"type":"message_stop","usage":{"input_tokens":1,"output_tokens":1}}\n\n');
      return;
    }
    if (path.endsWith("/responses")) {
      res.end(
        'event: response.completed\ndata: {"type":"response.completed","response":{"id":"resp_1","usage":{"input_tokens":1,"output_tokens":1}}}\n\n',
      );
      return;
    }
    res.end('data: {"choices":[{"delta":{"content":"ok"}}]}\n\ndata: [DONE]\n\n');
  });
  const store = await seeded();
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [
      { id: "fake-openai", kind: "openai", baseUrl: upstream.url },
      { id: "fake-anthropic", kind: "anthropic", baseUrl: upstream.url },
    ],
    credentials: [
      { namespace: "platform", provider: "fake-openai", secret: "sk-usage", id: "one" },
      { namespace: "platform", provider: "fake-anthropic", secret: "sk-usage-anthropic", id: "two" },
    ],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
      {
        provider: "fake-anthropic",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
  });
  const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
  try {
    const chat = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers,
      body: '{"z":1,"model": "fake-openai/gpt-test","n":9007199254740993,"a":1,"a":2,"stream":true,"stream_options":{"future_option":"keep","include_usage":false}}',
    });
    assert.equal(chat.status, 200);
    await chat.text();
    assert.equal(
      seen[0],
      '{"z":1,"model": "gpt-test","n":9007199254740993,"a":1,"a":2,"stream":true,"stream_options":{"future_option":"keep","include_usage":true}}',
    );
    const responses = await app.request("http://127.0.0.1/ns/platform/v1/responses", {
      method: "POST",
      headers,
      body: '{"model":"fake-openai/gpt-test","input":"ordinary","stream":true,"n":9007199254740993}',
    });
    assert.equal(responses.status, 200);
    await responses.text();
    assert.equal(
      seen[1],
      '{"model":"gpt-test","input":"ordinary","stream":true,"n":9007199254740993}',
    );
    const messages = await app.request("http://127.0.0.1/ns/platform/v1/messages", {
      method: "POST",
      headers,
      body: '{"model":"fake-anthropic/claude","messages":[],"stream":true,"max_tokens":16}',
    });
    assert.equal(messages.status, 200);
    await messages.text();
    assert.equal(seen[2], '{"model":"claude","messages":[],"stream":true,"max_tokens":16}');
    assert.equal(seen.some((body) => body.includes("sk-usage")), false);
    assert.equal(seen.some((body) => body.includes(KEY)), false);
  } finally {
    upstream.close();
  }
});


test("messages_wire_headers_default_the_version_and_keep_the_caller_pin", async () => {
  const store = await seeded();
  const seen: Array<{
    version?: string;
    beta?: string;
    accept?: string;
    apiKey?: string;
    authorization?: string;
  }> = [];
  const upstream = await listen((req, res) => {
    const one = (name: string): string | undefined => {
      const value = req.headers[name];
      return Array.isArray(value) ? value[0] : value;
    };
    seen.push({
      version: one("anthropic-version"),
      beta: one("anthropic-beta"),
      accept: one("accept"),
      apiKey: one("x-api-key"),
      authorization: one("authorization"),
    });
    res.writeHead(200, { "content-type": "application/json" });
    res.end('{"id":"ok","usage":{"input_tokens":1,"output_tokens":1,"prompt_tokens":1,"completion_tokens":1}}');
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [
      { id: "fake-anthropic", kind: "anthropic", baseUrl: upstream.url },
      { id: "fake-openai", kind: "openai", baseUrl: upstream.url },
    ],
    credentials: [
      { namespace: "platform", provider: "fake-anthropic", secret: "sk-wire-anthropic", id: "anthropic" },
      { namespace: "platform", provider: "fake-openai", secret: "sk-wire-openai", id: "openai" },
    ],
    prices: [
      {
        provider: "fake-anthropic",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
  });
  const base = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
  const messages = JSON.stringify({
    model: "fake-anthropic/claude-test",
    max_tokens: 8,
    messages: [],
  });
  try {
    const omitted = await app.request("http://127.0.0.1/ns/platform/v1/messages", {
      method: "POST",
      headers: base,
      body: messages,
    });
    assert.equal(omitted.status, 200);
    await omitted.text();
    assert.equal(seen[0]?.version, "2023-06-01");
    assert.equal(seen[0]?.beta, undefined);
    assert.equal(seen[0]?.apiKey, "sk-wire-anthropic");
    assert.equal(seen[0]?.authorization, undefined);
    assert.notEqual(seen[0]?.accept, "text/plain");

    const pinned = await app.request("http://127.0.0.1/ns/platform/v1/messages", {
      method: "POST",
      headers: {
        ...base,
        "anthropic-version": "2099-01-01",
        "anthropic-beta": "thinking-2025",
        accept: "text/plain",
      },
      body: messages,
    });
    assert.equal(pinned.status, 200);
    await pinned.text();
    assert.equal(seen[1]?.version, "2099-01-01");
    assert.equal(seen[1]?.beta, "thinking-2025");
    assert.equal(seen[1]?.apiKey, "sk-wire-anthropic");
    assert.equal(seen[1]?.authorization, undefined);
    assert.notEqual(seen[1]?.accept, "text/plain");

    const blank = await app.request("http://127.0.0.1/ns/platform/v1/messages", {
      method: "POST",
      headers: { ...base, "anthropic-version": "", "anthropic-beta": "" },
      body: messages,
    });
    assert.equal(blank.status, 200);
    await blank.text();
    assert.equal(seen[2]?.version, "");
    assert.equal(seen[2]?.beta, "");

    const chat = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: {
        ...base,
        "anthropic-version": "2099-01-01",
        "anthropic-beta": "thinking-2025",
        accept: "text/plain",
      },
      body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [] }),
    });
    assert.equal(chat.status, 200);
    await chat.text();
    assert.equal(seen[3]?.version, undefined);
    assert.equal(seen[3]?.beta, undefined);
    assert.equal(seen[3]?.authorization, "Bearer sk-wire-openai");
    assert.equal(seen[3]?.apiKey, undefined);
    assert.notEqual(seen[3]?.accept, "text/plain");
    assert.equal(seen.some((headers) => headers.authorization?.includes(KEY)), false);
    assert.equal(seen.some((headers) => headers.apiKey === KEY), false);
  } finally {
    upstream.close();
  }
});


test("responses_sequence_ends_an_invalid_stream", async () => {
  const store = await seeded();
  const records: UsageRecord[] = [];
  const upstreamBytes = 'event: response.completed\ndata: {"type":"response.completed","response":{"id":"resp_1"}}\n\n';
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end(upstreamBytes);
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "sk-responses-sequence", id: "one" }],
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
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/responses", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, input: "hi" }),
    });
    assert.equal(response.status, 200);
    const text = await response.text();
    assert.equal(text.startsWith(upstreamBytes), true);
    assert.match(text, /provider stream was invalid: response.completed is missing status=completed/);
    assert.equal(text.includes("sk-responses-sequence"), false);
    assert.equal(text.includes(KEY), false);
    for (let attempt = 0; attempt < 20 && records.length === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(records.length, 1);
    assert.equal(records[0]!.status, "upstream_error");
    const status = await app.request("http://127.0.0.1/ns/platform/v1/credentials", {
      headers: { authorization: `Bearer ${KEY}` },
    });
    const rows = (await status.json()).data as { credential_id: string; state: string }[];
    assert.equal(rows.find((row) => row.credential_id === "one")?.state, "healthy");
  } finally {
    upstream.close();
  }
});


test("native_messages_sequence_ends_an_invalid_stream", async () => {
  const store = await seeded();
  const records: UsageRecord[] = [];
  const upstreamBytes = 'event: message_stop\ndata: {"type":"message_stop"}\n\n';
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end(upstreamBytes);
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-anthropic", kind: "anthropic", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-anthropic", secret: "sk-sequence", id: "one" }],
    prices: [
      {
        provider: "fake-anthropic",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
    onUsage: (record) => {
      records.push(record);
    },
  });
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
    assert.equal(text.startsWith(upstreamBytes), true);
    assert.match(
      text,
      /provider stream was invalid: native Messages message_stop arrived before a complete message sequence/,
    );
    assert.equal(text.includes("sk-sequence"), false);
    assert.equal(text.includes(KEY), false);
    for (let attempt = 0; attempt < 20 && records.length === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(records.length, 1);
    assert.equal(records[0]!.status, "upstream_error");
    const status = await app.request("http://127.0.0.1/ns/platform/v1/credentials", {
      headers: { authorization: `Bearer ${KEY}` },
    });
    const rows = (await status.json()).data as { credential_id: string; state: string }[];
    assert.equal(rows.find((row) => row.credential_id === "one")?.state, "healthy");
  } finally {
    upstream.close();
  }
});


test("a u64 overall timeout above 2^53 still dispatches", async () => {
  const budget = 18446744073709551615n;
  assert.equal(failoverDeadline(1_000, 30_000), 31_000);
  assert.equal(failoverDeadline(1_000, budget), 1_000 + 2_147_483_647);
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ id: "ok", usage: { prompt_tokens: 1, completion_tokens: 1 } }));
  });
  try {
    const result = await callUpstream({
      url: upstream.url,
      headers: new Headers({ authorization: "Bearer sk-test" }),
      body: new TextEncoder().encode("{}"),
      transport: {
        responseHeaderTimeoutMs: 30_000,
        bufferedBodyTimeoutMs: 30_000,
        streamIdleTimeoutMs: 30_000,
        maxResponseBytes: 1024 * 1024,
        overallTimeoutMs: budget,
      },
      stream: false,
      route: "chat",
      onUsage: () => {},
    });
    assert.equal(result.response.status, 200);
  } finally {
    upstream.close();
  }
});
