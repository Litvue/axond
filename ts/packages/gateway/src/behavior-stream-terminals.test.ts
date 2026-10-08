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

test("post-terminal stream grace closes an open body", async () => {
  const store = await seeded();
  const completed =
    'event: response.completed\ndata: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":3,"output_tokens":1}}}\n\n';
  const tail = 'event: provider.extension\ndata: {"type":"provider.extension"}\n\n';
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.write(completed + tail);
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


test("post-terminal grace closes a hanging body and releases the stream slot", async () => {
  const store = await seeded();
  const records: UsageRecord[] = [];
  const completed =
    'event: response.completed\ndata: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":3,"output_tokens":1}}}\n\n';
  let hits = 0;
  const upstream = await listen((_req, res) => {
    hits += 1;
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.write(completed);
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    maxInFlight: 4,
    maxInFlightStreams: 1,
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
    onUsage: (record) => {
      records.push(record);
    },
  });
  const payload = JSON.stringify({ model: "fake-openai/gpt-test", stream: true, input: "hi" });
  const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
  try {
    const started = Date.now();
    const first = await app.request("http://127.0.0.1/ns/platform/v1/responses", {
      method: "POST",
      headers,
      body: payload,
    });
    assert.equal(first.status, 200);
    assert.equal(hits, 1);
    const shed = await app.request("http://127.0.0.1/ns/platform/v1/responses", {
      method: "POST",
      headers,
      body: payload,
    });
    assert.equal(shed.status, 503);
    assert.equal(shed.headers.get("retry-after"), "1");
    const shedBody = await shed.json() as { error: { type: string } };
    assert.equal(shedBody.error.type, "stream_capacity_exhausted");
    assert.equal(hits, 1);
    const text = await first.text();
    assert.equal(text, completed);
    assert.equal(text.includes("upstream_stream_error"), false);
    assert.ok(Date.now() - started < 1_000);
    for (let attempt = 0; attempt < 20 && records.length === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(records[0]?.status, "ok");
    assert.equal(records[0]?.outputTokens, 1n);
    const replacement = await app.request("http://127.0.0.1/ns/platform/v1/responses", {
      method: "POST",
      headers,
      body: payload,
    });
    assert.equal(replacement.status, 200);
    assert.equal(await replacement.text(), completed);
    assert.equal(hits, 2);
    assert.equal(records.at(-1)?.status, "ok");
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


test("native_messages_usage_folds_message_delta_counters", async () => {
  const store = await seeded();
  const records: UsageRecord[] = [];
  const wire = [
    'event: message_start\ndata: {"type":"message_start","message":{"usage":{"input_tokens":12,"output_tokens":0,"cache_read_input_tokens":3,"cache_creation_input_tokens":2}}}\n\n',
    'event: message_delta\ndata: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":9,"reasoning_tokens":2}}\n\n',
    'event: message_stop\ndata: {"type":"message_stop"}\n\n',
  ].join("");
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
    assert.equal(await response.text(), wire);
    for (let attempt = 0; attempt < 20 && records.length === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(records.length, 1);
    assert.equal(records[0]!.status, "ok");
    assert.equal(records[0]!.inputTokens, 12n);
    assert.equal(records[0]!.outputTokens, 9n);
    assert.equal(records[0]!.reasoningTokens, 2n);
    assert.equal(records[0]!.cacheReadTokens, 3n);
    assert.equal(records[0]!.cacheWriteTokens, 2n);
    const rendered = JSON.stringify(records[0], (_key, value) => typeof value === "bigint" ? value.toString() : value);
    assert.equal(rendered.includes("sk-live-secret"), false);
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


test("a transport error after [DONE] keeps the completed body", async () => {
  const store = await seeded();
  const records: UsageRecord[] = [];
  const metrics = createMetrics();
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.write(CHAT_DONE, () => {
      // Next turn: Bun drops the body if the socket resets inside this callback.
      setTimeout(() => res.destroy(), 0);
    });
  });
  const app = chatDoneApp(store, upstream.url, records, metrics);
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, messages: [] }),
    });
    assert.equal(response.status, 200);
    const text = await response.text();
    assert.equal(text, CHAT_DONE);
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


test("an incomplete tail after [DONE] is relayed through eof", async () => {
  const store = await seeded();
  const records: UsageRecord[] = [];
  const wire = Buffer.concat([
    Buffer.from(CHAT_DONE),
    Buffer.from("data: "),
    Buffer.from([0xf0]),
  ]);
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end(wire);
  });
  const app = chatDoneApp(store, upstream.url, records);
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, messages: [] }),
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


test("chat_eof_names_an_incomplete_event_and_a_split_character", async () => {
  const store = await seeded();
  const records: UsageRecord[] = [];
  const incompleteBytes = 'data: {"choices":[{"delta":{"content":"a"}}]}\n\ndata: {"choices"';
  const incomplete = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end(incompleteBytes);
  });
  const splitBytes = Buffer.concat([
    Buffer.from('data: {"choices":[{"delta":{"content":"a"}}]}\n\n'),
    Buffer.from([0xf0]),
  ]);
  const split = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end(splitBytes);
  });
  const messagesBytes = Buffer.concat([
    Buffer.from('event: ping\ndata: {"type":"ping"}\n\n'),
    Buffer.from([0xf0]),
  ]);
  const messages = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end(messagesBytes);
  });
  try {
    const chatApp = chatDoneApp(store, incomplete.url, records);
    const chat = await chatApp.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, messages: [] }),
    });
    assert.equal(chat.status, 200);
    const chatBody = await chat.text();
    assert.equal(chatBody.startsWith(incompleteBytes), true);
    assert.match(chatBody, /stream ended with an incomplete SSE event/);
    assert.equal(chatBody.includes("sk-live-secret"), false);
    assert.equal(chatBody.includes(KEY), false);
    for (let attempt = 0; attempt < 20 && records.length === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(records.length, 1);
    assert.equal(records[0]!.status, "upstream_error");

    records.length = 0;
    const splitApp = chatDoneApp(store, split.url, records);
    const splitResponse = await splitApp.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, messages: [] }),
    });
    assert.equal(splitResponse.status, 200);
    const splitBody = Buffer.from(await splitResponse.arrayBuffer());
    assert.equal(splitBody.subarray(0, splitBytes.length).equals(splitBytes), true);
    assert.equal(splitBody.includes(Buffer.from("stream ended mid-character")), true);
    assert.equal(splitBody.includes(Buffer.from("sk-live-secret")), false);
    for (let attempt = 0; attempt < 20 && records.length === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(records.length, 1);
    assert.equal(records[0]!.status, "upstream_error");

    records.length = 0;
    const messagesAppLive = messagesApp(store, messages.url, records);
    const native = await messagesAppLive.request("http://127.0.0.1/ns/platform/v1/messages", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-anthropic/claude-test", stream: true, messages: [], max_tokens: 16 }),
    });
    assert.equal(native.status, 200);
    const nativeBody = Buffer.from(await native.arrayBuffer());
    assert.equal(nativeBody.subarray(0, messagesBytes.length).equals(messagesBytes), true);
    assert.equal(nativeBody.includes(Buffer.from("stream ended mid-character")), true);
    assert.equal(nativeBody.includes(Buffer.from("sk-live-secret")), false);
    for (let attempt = 0; attempt < 20 && records.length === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(records.length, 1);
    assert.equal(records[0]!.status, "upstream_error");
  } finally {
    incomplete.close();
    split.close();
    messages.close();
  }
});


test("chat_invalid_json_fails_the_stream_and_keeps_the_credential", async () => {
  const store = await seeded();
  const records: UsageRecord[] = [];
  const seen: string[] = [];
  const broken = [
    'data: {"choices":[{"delta":{"content":"partial answer"}}]}\n\n',
    "data: {not json}\n\n",
    'data: {"error":{"type":"rate_limit_exceeded","message":"slow down"}}\n\n',
  ].join("");
  const tail = `${CHAT_DONE}data: {not json}\n\n`;
  const comment = 'data: {"choices":[{"delta":{"content":"a"}}]}\n\n: comment\n\n';
  const empty = "data:\n\n";
  let phase: "broken" | "tail" | "comment" | "empty" = "broken";
  const upstream = await listen((req, res) => {
    seen.push(req.headers.authorization ?? "");
    res.writeHead(200, { "content-type": "text/event-stream" });
    if (phase === "broken") {
      res.end(broken);
      return;
    }
    if (phase === "tail") {
      res.end(tail);
      return;
    }
    if (phase === "comment") {
      res.end(comment);
      return;
    }
    res.end(empty);
  });
  const app = poolApp(store, upstream.url, {
    credentialPool: { failureThreshold: 1, cooldownMs: 30_000 },
    onUsage: (record) => {
      records.push(record);
    },
  });
  const chat = (body: string) => app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: CHAT_HEADERS,
    body,
  });
  const payload = JSON.stringify({ model: "fake-openai/gpt-test", stream: true, messages: [] });
  const waitUsage = async () => {
    for (let attempt = 0; attempt < 20 && records.length === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
  };
  let invalid = "provider stream was invalid: invalid JSON";
  try {
    JSON.parse("{not json}");
  } catch (error) {
    invalid = `provider stream was invalid: ${error instanceof Error ? error.message : "invalid JSON"}`;
  }
  try {
    const first = await chat(payload);
    assert.equal(first.status, 200);
    const firstBody = await first.text();
    assert.equal(firstBody.startsWith(broken), true);
    assert.equal(firstBody.includes(invalid), true);
    assert.equal(firstBody.includes("data: [DONE]\n\n"), true);
    assert.equal(firstBody.includes("provider stream was rate limited"), false);
    assert.equal(firstBody.includes("bad-key"), false);
    assert.equal(firstBody.includes("good-key"), false);
    assert.equal(firstBody.includes(KEY), false);
    await waitUsage();
    assert.equal(records.length, 1);
    assert.equal(records[0]!.status, "upstream_error");
    assert.equal(records[0]!.outputTokens, 4n);

    assert.deepEqual(seen, ["Bearer bad-key"]);
    records.length = 0;
    const second = await chat(payload);
    assert.equal(second.status, 200);
    assert.equal((await second.text()).includes(invalid), true);
    await waitUsage();
    assert.equal(records.length, 1);
    assert.equal(records[0]!.status, "upstream_error");
    records.length = 0;
    const third = await chat(payload);
    assert.equal(third.status, 200);
    await third.text();
    await waitUsage();
    assert.deepEqual(seen, ["Bearer bad-key", "Bearer good-key", "Bearer bad-key"]);
    const status = await app.request("http://127.0.0.1/ns/platform/v1/credentials", { headers: CHAT_HEADERS });
    const rows = (await status.json()).data as { credential_id: string; state: string }[];
    assert.equal(rows.find((row) => row.credential_id === "bad")?.state, "healthy");
    assert.equal(rows.find((row) => row.credential_id === "good")?.state, "healthy");

    records.length = 0;
    phase = "tail";
    const afterDone = await chat(payload);
    assert.equal(afterDone.status, 200);
    const afterBody = await afterDone.text();
    assert.equal(afterBody, tail);
    assert.equal(afterBody.includes("upstream_stream_error"), false);
    await waitUsage();
    assert.equal(records.length, 1);
    assert.equal(records[0]!.status, "ok");
    assert.equal(records[0]!.outputTokens, 1n);

    records.length = 0;
    phase = "comment";
    const commented = await chat(payload);
    assert.equal(commented.status, 200);
    assert.equal(await commented.text(), comment);
    await waitUsage();
    assert.equal(records.length, 1);
    assert.equal(records[0]!.status, "ok");
    assert.equal(records[0]!.outputTokens, 1n);

    records.length = 0;
    phase = "empty";
    const blank = await chat(payload);
    assert.equal(blank.status, 200);
    const blankBody = await blank.text();
    assert.equal(blankBody.startsWith(empty), true);
    assert.equal(blankBody.includes("provider stream was invalid:"), true);
    assert.equal(blankBody.includes("bad-key"), false);
    await waitUsage();
    assert.equal(records.length, 1);
    assert.equal(records[0]!.status, "upstream_error");
    assert.equal(records[0]!.outputTokens, 0n);
    assert.equal(records[0]!.inputTokens, 0n);
  } finally {
    upstream.close();
  }
});


test("sse_buffer_limit_fails_an_unterminated_event_and_keeps_a_finished_stream", async () => {
  const limit = 1024 * 1024;
  const over = "x".repeat(limit + 1);
  const exact = "y".repeat(limit);
  const frame = 'data: {"choices":[{"delta":{"content":"a"}}]}\n\n';
  const chunk = frame.repeat(Math.ceil(700_000 / frame.length));
  assert.ok(Buffer.byteLength(chunk) < limit);
  assert.ok(Buffer.byteLength(chunk) * 2 > limit);
  const euros = "€".repeat(400_000);
  assert.ok(euros.length < limit);
  assert.ok(Buffer.byteLength(euros) > limit);

  const store = await seeded();
  const records: UsageRecord[] = [];
  const overUp = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end(over);
  });
  const exactUp = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end(exact);
  });
  const euroUp = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end(euros);
  });
  let releaseMany: () => void = () => undefined;
  const manyGate = new Promise<void>((resolve) => {
    releaseMany = resolve;
  });
  const manyUp = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.flushHeaders();
    res.write(chunk);
    void manyGate.then(() => {
      res.write(chunk);
      res.write("data: [DONE]\n\n");
      res.end();
    });
  });
  let releaseTail: () => void = () => undefined;
  const tailGate = new Promise<void>((resolve) => {
    releaseTail = resolve;
  });
  const tail = "z".repeat(limit + 1);
  const tailUp = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.flushHeaders();
    res.write(CHAT_DONE);
    void tailGate.then(() => {
      res.end(tail);
    });
  });
  const messagesUp = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end(over);
  });
  const seen: string[] = [];
  const heldUp = await listen((req, res) => {
    seen.push(req.headers.authorization ?? "");
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end(over);
  });

  const chatBody = JSON.stringify({ model: "fake-openai/gpt-test", stream: true, messages: [] });
  const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
  const waitUsage = async () => {
    for (let attempt = 0; attempt < 20 && records.length === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
  };
  const readGated = async (response: Response, ready: (got: number) => boolean, release: () => void) => {
    const reader = response.body!.getReader();
    const parts: Uint8Array[] = [];
    let got = 0;
    try {
      for (;;) {
        const part = await reader.read();
        if (part.done) {
          break;
        }
        parts.push(part.value);
        got += part.value.byteLength;
        if (ready(got)) {
          release();
        }
      }
    } finally {
      release();
    }
    return Buffer.concat(parts);
  };

  try {
    const overApp = chatDoneApp(store, overUp.url, records);
    const overResponse = await overApp.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers,
      body: chatBody,
    });
    assert.equal(overResponse.status, 200);
    const overBody = Buffer.from(await overResponse.arrayBuffer());
    assert.equal(overBody.subarray(0, over.length).toString(), over);
    assert.equal(overBody.includes(Buffer.from("SSE buffer exceeded 1048576 bytes")), true);
    assert.equal(overBody.includes(Buffer.from("data: [DONE]\n\n")), true);
    assert.equal(overBody.includes(Buffer.from("sk-live-secret")), false);
    assert.equal(overBody.includes(Buffer.from(KEY)), false);
    await waitUsage();
    assert.equal(records.length, 1);
    assert.equal(records[0]!.status, "upstream_error");

    records.length = 0;
    const exactApp = chatDoneApp(store, exactUp.url, records);
    const exactResponse = await exactApp.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers,
      body: chatBody,
    });
    assert.equal(exactResponse.status, 200);
    const exactBody = Buffer.from(await exactResponse.arrayBuffer());
    assert.equal(exactBody.subarray(0, exact.length).toString(), exact);
    assert.equal(exactBody.includes(Buffer.from("SSE buffer exceeded")), false);
    assert.equal(exactBody.includes(Buffer.from("stream ended with an incomplete SSE event")), true);
    await waitUsage();
    assert.equal(records.length, 1);
    assert.equal(records[0]!.status, "upstream_error");

    records.length = 0;
    const euroApp = chatDoneApp(store, euroUp.url, records);
    const euroResponse = await euroApp.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers,
      body: chatBody,
    });
    assert.equal(euroResponse.status, 200);
    const euroBody = Buffer.from(await euroResponse.arrayBuffer());
    const euroMark = euroBody.indexOf("event: error\n");
    assert.ok(euroMark > limit);
    assert.equal(euroBody.subarray(0, euroMark).equals(Buffer.from(euros).subarray(0, euroMark)), true);
    assert.equal(euroBody.includes(Buffer.from("SSE buffer exceeded 1048576 bytes")), true);
    await waitUsage();
    assert.equal(records.length, 1);
    assert.equal(records[0]!.status, "upstream_error");

    records.length = 0;
    const manyApp = chatDoneApp(store, manyUp.url, records);
    const manyResponse = await manyApp.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers,
      body: chatBody,
    });
    assert.equal(manyResponse.status, 200);
    const manyBody = await readGated(manyResponse, (got) => got >= Buffer.byteLength(chunk), releaseMany);
    assert.equal(manyBody.includes(Buffer.from(chunk)), true);
    assert.equal(manyBody.includes(Buffer.from(`${chunk}${chunk}`)), true);
    assert.equal(manyBody.includes(Buffer.from("data: [DONE]\n\n")), true);
    assert.equal(manyBody.includes(Buffer.from("SSE buffer exceeded")), false);
    assert.equal(manyBody.includes(Buffer.from("upstream_stream_error")), false);
    await waitUsage();
    assert.equal(records.length, 1);
    assert.equal(records[0]!.status, "ok");

    records.length = 0;
    const tailApp = chatDoneApp(store, tailUp.url, records);
    const tailResponse = await tailApp.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers,
      body: chatBody,
    });
    assert.equal(tailResponse.status, 200);
    const tailBody = await readGated(tailResponse, (got) => got >= Buffer.byteLength(CHAT_DONE), releaseTail);
    assert.equal(tailBody.subarray(0, Buffer.byteLength(CHAT_DONE)).toString(), CHAT_DONE);
    assert.equal(tailBody.includes(tail), true);
    assert.equal(tailBody.includes(Buffer.from("SSE buffer exceeded")), false);
    assert.equal(tailBody.includes(Buffer.from("upstream_stream_error")), false);
    await waitUsage();
    assert.equal(records.length, 1);
    assert.equal(records[0]!.status, "ok");
    assert.equal(records[0]!.outputTokens, 1n);

    records.length = 0;
    const nativeApp = messagesApp(store, messagesUp.url, records);
    const native = await nativeApp.request("http://127.0.0.1/ns/platform/v1/messages", {
      method: "POST",
      headers,
      body: JSON.stringify({ model: "fake-anthropic/claude-test", stream: true, messages: [], max_tokens: 16 }),
    });
    assert.equal(native.status, 200);
    const nativeBody = Buffer.from(await native.arrayBuffer());
    assert.equal(nativeBody.subarray(0, over.length).toString(), over);
    assert.equal(nativeBody.includes(Buffer.from("SSE buffer exceeded 1048576 bytes")), true);
    assert.equal(nativeBody.includes(Buffer.from("data: [DONE]")), false);
    assert.equal(nativeBody.includes(Buffer.from("sk-live-secret")), false);
    await waitUsage();
    assert.equal(records.length, 1);
    assert.equal(records[0]!.status, "upstream_error");

    const heldApp = poolApp(store, heldUp.url, {
      credentialPool: { failureThreshold: 1, cooldownMs: 30_000 },
    });
    const held = await heldApp.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers,
      body: chatBody,
    });
    assert.equal(held.status, 200);
    const heldBody = Buffer.from(await held.arrayBuffer());
    assert.equal(heldBody.subarray(0, over.length).toString(), over);
    assert.equal(heldBody.includes(Buffer.from("SSE buffer exceeded 1048576 bytes")), true);
    assert.equal(heldBody.includes(Buffer.from("bad-key")), false);
    assert.equal(heldBody.includes(Buffer.from("good-key")), false);
    assert.deepEqual(seen, ["Bearer bad-key"]);
  } finally {
    releaseMany();
    releaseTail();
    overUp.close();
    exactUp.close();
    euroUp.close();
    manyUp.close();
    tailUp.close();
    messagesUp.close();
    heldUp.close();
  }
});


test(
  "a connect timeout is upstream_timeout and hides the address",
  { skip: process.versions.bun !== undefined && "Bun fetch does not enforce connect_timeout_ms" },
  async () => {
  const store = await seeded();
  // A local peer accepts TCP but never completes the TLS handshake. This
  // exercises the real connect timer without depending on Internet routing.
  const sockets = new Set<import("node:net").Socket>();
  const silent = createNetServer((socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    socket.on("error", () => undefined);
  });
  await new Promise<void>((resolve) => silent.listen(0, "127.0.0.1", () => resolve()));
  const address = silent.address();
  assert.ok(address && typeof address !== "string");
  const agent = new Agent({ connectTimeout: 50, connect: { autoSelectFamily: false } });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    upstreamDispatcher: agent,
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: `https://127.0.0.1:${address.port}` }],
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
    assert.equal(body.error.message.includes("127.0.0.1"), false);
    assert.ok(Date.now() - started < 3_000);
  } finally {
    await agent.close();
    for (const socket of sockets) socket.destroy();
    await new Promise<void>((resolve) => silent.close(() => resolve()));
  }
});
