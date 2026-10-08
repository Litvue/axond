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

test("terminal_remain_log_names_the_bound_and_omits_the_secret", async () => {
  const store = await seeded();
  const logs: { msg: string; bound?: string; grace_ms?: number; provider?: string; model?: string; request_id?: string }[] = [];
  const records: UsageRecord[] = [];
  const completed =
    'event: response.completed\ndata: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":3,"output_tokens":1}}}\n\n';
  const doneFrame = 'data: {"choices":[{"delta":{"content":"Hi"}}]}\n\ndata: [DONE]\n\n';
  let mode: "grace" | "duration" | "eof" = "grace";
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    if (mode === "grace") {
      res.write(completed);
      return;
    }
    res.write(doneFrame);
    if (mode === "eof") {
      res.end();
    }
  });
  const shared = {
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai" as const, baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "sk-live-secret", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
    onLog: (record: { msg: string; bound?: string; grace_ms?: number; provider?: string; model?: string; request_id?: string }) => {
      logs.push(record);
    },
    onUsage: (record: UsageRecord) => {
      records.push(record);
    },
  };
  const graceApp = createAxond({
    ...shared,
    transport: {
      responseHeaderTimeoutMs: 1_000,
      bufferedBodyTimeoutMs: 1_000,
      streamIdleTimeoutMs: 5_000,
      streamTerminalGraceMs: 80,
      maxResponseBytes: 4096,
    },
  });
  const durationApp = createAxond({
    ...shared,
    maxStreamDurationMs: 200,
    transport: {
      responseHeaderTimeoutMs: 1_000,
      bufferedBodyTimeoutMs: 1_000,
      streamIdleTimeoutMs: 5_000,
      streamTerminalGraceMs: 5_000,
      maxResponseBytes: 4096,
    },
  });
  const eofApp = createAxond({
    ...shared,
    transport: {
      responseHeaderTimeoutMs: 1_000,
      bufferedBodyTimeoutMs: 1_000,
      streamIdleTimeoutMs: 5_000,
      streamTerminalGraceMs: 800,
      maxResponseBytes: 4096,
    },
  });
  try {
    const graceResponse = await graceApp.request("http://127.0.0.1/ns/platform/v1/responses", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, input: "PROMPT_SENTINEL" }),
    });
    assert.equal(graceResponse.status, 200);
    const graceText = await graceResponse.text();
    assert.equal(graceText, completed);
    assert.equal(graceText.includes("upstream_stream_error"), false);
    const grace = logs.find((line) => line.msg === "terminal_remain" && line.bound === "grace");
    assert.ok(grace);
    assert.equal(grace.grace_ms, 80);
    assert.equal(grace.provider, "fake-openai");
    assert.equal(grace.model, "gpt-test");
    assert.equal(typeof grace.request_id, "string");
    assert.equal((grace.request_id ?? "").length > 0, true);
    mode = "duration";
    const durationResponse = await durationApp.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({
        model: "fake-openai/gpt-test",
        stream: true,
        messages: [{ role: "user", content: "PROMPT_SENTINEL" }],
      }),
    });
    assert.equal(durationResponse.status, 200);
    const durationText = await durationResponse.text();
    assert.equal(durationText.includes("data: [DONE]\n\n"), true);
    assert.equal(durationText.includes("upstream_stream_error"), false);
    assert.equal(durationText.includes("stream exceeded"), false);
    const duration = logs.find((line) => line.msg === "terminal_remain" && line.bound === "duration");
    assert.ok(duration);
    assert.equal(duration.grace_ms, undefined);
    assert.equal(duration.provider, "fake-openai");
    assert.equal(duration.model, "gpt-test");
    assert.equal(duration.request_id === grace.request_id, false);
    mode = "eof";
    const before = logs.filter((line) => line.msg === "terminal_remain").length;
    const started = Date.now();
    const eofResponse = await eofApp.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({
        model: "fake-openai/gpt-test",
        stream: true,
        messages: [{ role: "user", content: "PROMPT_SENTINEL" }],
      }),
    });
    assert.equal(eofResponse.status, 200);
    assert.equal((await eofResponse.text()).includes("data: [DONE]\n\n"), true);
    assert.ok(Date.now() - started < 500);
    assert.equal(logs.filter((line) => line.msg === "terminal_remain").length, before);
    for (let attempt = 0; attempt < 30 && records.length < 2; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(records.filter((record) => record.status === "ok").length >= 2, true);
    const encoded = JSON.stringify(logs);
    assert.equal(encoded.includes("sk-live-secret"), false);
    assert.equal(encoded.includes(KEY), false);
    assert.equal(encoded.includes("PROMPT_SENTINEL"), false);
    assert.equal(encoded.includes(upstream.url), false);
  } finally {
    upstream.close();
  }
});


test("transport_failure_reason_is_a_bounded_class", () => {
  const refused = Object.assign(new Error("fetch failed"), {
    cause: Object.assign(new Error("connect ECONNREFUSED 127.0.0.1:9"), { code: "ECONNREFUSED" }),
  });
  assert.equal(transportFailureReason(refused), "refused");
  assert.equal(transportFailureReason(Object.assign(new Error("Unable to connect. Is the computer able to access the url?"), { code: "ConnectionRefused" })), "refused");
  assert.equal(transportFailureReason(Object.assign(new Error("getaddrinfo ENOTFOUND"), { code: "ENOTFOUND" })), "dns");
  assert.equal(transportFailureReason(Object.assign(new Error("read ECONNRESET"), { code: "ECONNRESET" })), "reset");
  assert.equal(transportFailureReason(new Error("certificate has expired")), "tls");
  assert.equal(transportFailureReason(new Error("mystery")), "other");
});


test("upstream_transport_log_names_the_reason_and_omits_the_address", async () => {
  const store = await seeded();
  const refusedUrl = await new Promise<string>((resolve, reject) => {
    const server = createNetServer();
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        reject(new Error("no port"));
        return;
      }
      const url = `http://127.0.0.1:${address.port}`;
      server.close(() => resolve(url));
    });
  });
  const logs: { msg: string; phase?: string; reason?: string; provider?: string; model?: string }[] = [];
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: refusedUrl }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "upstream-openai", id: "one" }],
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
  const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
    body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: "PROMPT_SENTINEL" }] }),
  });
  assert.equal(response.status, 502);
  const body = await response.json() as { error: { type: string; message: string } };
  assert.equal(body.error.type, "upstream_transport");
  assert.equal(body.error.message, "upstream transport failure");
  const line = logs.find((entry) => entry.msg === "upstream_transport");
  assert.ok(line);
  assert.equal(line.phase, "request");
  assert.equal(line.reason, "refused");
  assert.equal(line.provider, "fake-openai");
  assert.equal(line.model, "gpt-test");
  const encoded = JSON.stringify(logs);
  assert.equal(encoded.includes(refusedUrl), false);
  assert.equal(encoded.includes("127.0.0.1"), false);
  assert.equal(encoded.includes("upstream-openai"), false);
  assert.equal(encoded.includes(KEY), false);
  assert.equal(encoded.includes("PROMPT_SENTINEL"), false);
  assert.equal(encoded.includes("ECONNREFUSED"), false);
});


test("open_stream_transport_log_names_the_phase_and_omits_the_address", async () => {
  const store = await seeded();
  const logs: { msg: string; phase?: string; reason?: string; committed?: boolean }[] = [];
  const records: UsageRecord[] = [];
  let calls = 0;
  const upstream = await listen((_req, res) => {
    calls += 1;
    res.writeHead(200, { "content-type": "text/event-stream" });
    if (calls === 1) {
      res.write('data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n', () => {
        setTimeout(() => res.destroy(), 0);
      });
      return;
    }
    res.write('data: {"choices":[{"delta":{"content":"Hi"}}]}\n\ndata: [DONE]\n\n', () => {
      setTimeout(() => res.destroy(), 0);
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
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
    transport: {
      responseHeaderTimeoutMs: 30_000,
      bufferedBodyTimeoutMs: 30_000,
      streamIdleTimeoutMs: 5_000,
      streamTerminalGraceMs: 1_000,
      maxResponseBytes: 1024 * 1024,
    },
    onLog: (record) => {
      logs.push(record);
    },
    onUsage: (record) => {
      records.push(record);
    },
  });
  try {
    const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
    const open = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers,
      body: JSON.stringify({
        model: "fake-openai/gpt-test",
        stream: true,
        messages: [{ role: "user", content: "PROMPT_SENTINEL" }],
      }),
    });
    assert.equal(open.status, 200);
    const openText = await open.text();
    assert.equal(openText.includes("Hi"), true);
    assert.equal(openText.includes("upstream stream failed"), true);
    const reset = logs.find((entry) => entry.msg === "upstream_transport" && entry.phase === "stream");
    assert.ok(reset);
    assert.equal(reset.reason, "reset");
    assert.equal(reset.committed, true);
    const done = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers,
      body: JSON.stringify({
        model: "fake-openai/gpt-test",
        stream: true,
        messages: [{ role: "user", content: "PROMPT_SENTINEL" }],
      }),
    });
    assert.equal(done.status, 200);
    const doneText = await done.text();
    assert.equal(doneText.includes("data: [DONE]"), true);
    assert.equal(doneText.includes("upstream_stream_error"), false);
    const closing = logs.find((entry) => entry.msg === "upstream_transport" && entry.phase === "closing");
    assert.ok(closing);
    assert.equal(closing.reason, "reset");
    for (let attempt = 0; attempt < 20 && records.length < 2; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(records.length, 2);
    assert.equal(records[0]!.status, "upstream_error");
    assert.equal(records[1]!.status, "ok");
    const encoded = JSON.stringify(logs);
    assert.equal(encoded.includes(upstream.url), false);
    assert.equal(encoded.includes("127.0.0.1"), false);
    assert.equal(encoded.includes("sk-live-secret"), false);
    assert.equal(encoded.includes(KEY), false);
    assert.equal(encoded.includes("PROMPT_SENTINEL"), false);
    assert.equal(encoded.includes("ECONNRESET"), false);
  } finally {
    upstream.close();
  }
});


test("stream_byte_limit_log_names_the_size_cap", async () => {
  const store = await seeded();
  const logs: { msg: string; limit?: string; provider?: string; model?: string }[] = [];
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.write('data: {"choices":[{"delta":{"content":"COMPLETION_SENTINEL"}}]}\n\n');
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
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
    onLog: (record) => {
      logs.push(record);
    },
  });
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({
        model: "fake-openai/gpt-test",
        stream: true,
        messages: [{ role: "user", content: "PROMPT_SENTINEL" }],
      }),
    });
    assert.equal(response.status, 200);
    const text = await response.text();
    assert.equal(text.includes("COMPLETION_SENTINEL"), false);
    assert.equal(text.includes("stream exceeded the gateway's maximum stream size"), true);
    const limit = logs.find((line) => line.msg === "stream_limit");
    assert.ok(limit);
    assert.equal(limit.limit, "bytes");
    assert.equal(limit.provider, "fake-openai");
    assert.equal(limit.model, "gpt-test");
    const encoded = JSON.stringify(logs);
    assert.equal(encoded.includes("sk-live-secret"), false);
    assert.equal(encoded.includes(KEY), false);
    assert.equal(encoded.includes("PROMPT_SENTINEL"), false);
    assert.equal(encoded.includes("COMPLETION_SENTINEL"), false);
    assert.equal(encoded.includes(upstream.url), false);
  } finally {
    upstream.close();
  }
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
    assert.equal(event.includes('"cost_microdollars":0'), true);
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

test("keepalives do not reset the upstream idle deadline", async () => {
  const upstream = await listen((_req, res) => { res.writeHead(200, { "content-type": "text/event-stream" }); res.write('data: {"choices":[{"delta":{"content":"hi"}}]}\n\n'); });
  try {
    const started = Date.now();
    const opened = await callUpstream({ url: `${upstream.url}/chat/completions`, headers: new Headers(), body: new TextEncoder().encode("{}"), stream: true, route: "chat", onUsage() {},
      transport: { responseHeaderTimeoutMs: 1000, bufferedBodyTimeoutMs: 1000, streamIdleTimeoutMs: 16000, streamTerminalGraceMs: 1000, maxResponseBytes: 4096 } });
    const wire = await opened.response.text();
    assert.match(wire, /16000ms bound/);
    assert.ok(Date.now() - started < 22000);
    assert.equal((wire.match(/: keepalive/g) ?? []).length, 1);
  } finally { upstream.close(); }
});
