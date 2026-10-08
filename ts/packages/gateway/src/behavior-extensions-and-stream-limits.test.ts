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


test("usage_event_copies_admission_attrs", async () => {
  const store = await seeded();
  const records: UsageRecord[] = [];
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "application/json" });
    res.end('{"usage":{"prompt_tokens":1,"completion_tokens":1}}');
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
    onUsage: (record) => {
      records.push(record);
    },
  });
  const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
  try {
    const created = await app.request("http://127.0.0.1/api/v1/namespaces", {
      method: "POST",
      headers,
      body: '{"id":"wsp_attrs","attrs":{"z":1,"a":"acme","n":1.0}}',
    });
    assert.equal(created.status, 201);
    await store.putBudget("wsp_attrs", "compat", 1_000_000n);
    const chat = await app.request("http://127.0.0.1/ns/wsp_attrs/v1/chat/completions", {
      method: "POST",
      headers,
      body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: "PROMPT_SENTINEL" }] }),
    });
    assert.equal(chat.status, 200);
    await chat.text();
    const replaced = await app.request("http://127.0.0.1/api/v1/namespaces/wsp_attrs", {
      method: "PUT",
      headers,
      body: '{"attrs":{"org":"later"}}',
    });
    assert.equal(replaced.status, 200);
    for (let attempt = 0; attempt < 20 && records.length === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(records.length, 1);
    const line = usageLine(records[0]!);
    assert.equal(
      line.includes('"namespace":"wsp_attrs","attrs":{"a":"acme","n":1.0,"z":1},"period":"compat"'),
      true,
    );
    assert.equal(line.includes("later"), false);
    assert.equal(line.includes("sk-live-secret"), false);
    assert.equal(line.includes(KEY), false);
    assert.equal(line.includes("PROMPT_SENTINEL"), false);
    const platform = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers,
      body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [] }),
    });
    assert.equal(platform.status, 200);
    await platform.text();
    for (let attempt = 0; attempt < 20 && records.length < 2; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(records.length, 2);
    assert.equal(usageLine(records[1]!).includes('"namespace":"platform","attrs":{},"period":"compat"'), true);
  } finally {
    upstream.close();
  }
});


test("usage_event_omits_null_price_identity", async () => {
  const store = await seeded();
  const records: UsageRecord[] = [];
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "application/json" });
    res.end('{"usage":{"prompt_tokens":1,"completion_tokens":1}}');
  });
  const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
  const prices = [
    {
      provider: "fake-openai",
      model: "*",
      inputMicrodollarsPerMillion: 1n,
      outputMicrodollarsPerMillion: 1n,
    },
  ];
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "sk-live-secret", id: "one" }],
    prices,
    onUsage: (record) => {
      records.push(record);
    },
  });
  try {
    const chat = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers,
      body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [] }),
    });
    assert.equal(chat.status, 200);
    await chat.text();
    for (let attempt = 0; attempt < 20 && records.length === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(records.length, 1);
    const held = usageLine(records[0]!);
    assert.equal(held.includes('"namespace":"platform","attrs":{},"period":"compat"'), true);
    assert.equal(held.includes('"signer_kid"'), false);
    assert.equal(held.includes('"price_book"'), false);
    assert.equal(held.includes('"price_book_checksum"'), false);
    assert.equal(held.includes('"price_catalog"'), false);

    const unpriced = createAxond({
      store,
      gatewayKey: KEY,
      defaultNamespace: "platform",
      providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url, unpricedModels: "allow" }],
      credentials: [{ namespace: "platform", provider: "fake-openai", secret: "sk-live-secret", id: "one" }],
      onUsage: (record) => {
        records.push(record);
      },
    });
    const open = await unpriced.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { ...headers, "x-request-id": "unpriced-cost" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [] }),
    });
    assert.equal(open.status, 200);
    await open.text();
    for (let attempt = 0; attempt < 20 && records.length < 2; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(records.length, 2);
    assert.equal(records[1]!.costMicrodollars, null);
    const unpricedLine = usageLine(records[1]!);
    assert.equal(unpricedLine.includes('"cost_microdollars":null'), true);
    assert.equal(unpricedLine.includes('"period":"compat"'), true);
    assert.equal(unpricedLine.includes('"price_catalog"'), false);

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
    const allowed = createAxond({
      store: down,
      gatewayKey: KEY,
      defaultNamespace: "platform",
      onStoreUnavailable: "allow",
      providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
      credentials: [{ namespace: "platform", provider: "fake-openai", secret: "sk-live-secret", id: "one" }],
      prices,
      onUsage: (record) => {
        records.push(record);
      },
    });
    const unheld = await allowed.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { ...headers, "x-request-id": "unheld-period" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [] }),
    });
    assert.equal(unheld.status, 200);
    await unheld.text();
    for (let attempt = 0; attempt < 20 && records.length < 3; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(records.length, 3);
    assert.equal(records[2]!.period, null);
    const line = usageLine(records[2]!);
    assert.equal(line.includes('"period"'), false);
    assert.equal(line.includes('"namespace":"platform","attrs":{},"subject":'), true);
    assert.equal(line.includes('"signer_kid"'), false);
    assert.equal(line.includes('"price_book"'), false);
  } finally {
    upstream.close();
  }
});


test("minted_request_id_is_a_uuid7", async () => {
  const store = await seeded();
  const records: UsageRecord[] = [];
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "application/json" });
    res.end('{"usage":{"prompt_tokens":1,"completion_tokens":1}}');
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
    onUsage: (record) => {
      records.push(record);
    },
  });
  const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
  const chat = async (requestId?: string) => {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: requestId === undefined ? headers : { ...headers, "x-request-id": requestId },
      body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [] }),
    });
    assert.equal(response.status, 200);
    await response.text();
  };
  try {
    await chat();
    await chat();
    await chat("worker-price-once");
    await chat("bad id");
    await chat("a".repeat(129));
    await chat("b".repeat(128));
    for (let attempt = 0; attempt < 20 && records.length < 6; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.equal(records.length, 6);
    const [first, second, kept, spaced, tooLong, maxLength] = records;
    assert.match(first!.requestId, MINTED_REQUEST_ID);
    assert.equal(first!.requestId.length, 40);
    assert.match(second!.requestId, MINTED_REQUEST_ID);
    assert.equal(first!.requestId < second!.requestId, true);
    assert.equal(kept!.requestId, "worker-price-once");
    assert.match(spaced!.requestId, MINTED_REQUEST_ID);
    assert.match(tooLong!.requestId, MINTED_REQUEST_ID);
    assert.equal(maxLength!.requestId, "b".repeat(128));
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


test("credential_rate_limit_log_names_the_id_and_omits_the_secret", async () => {
  const store = await seeded();
  const logs: { msg: string; provider?: string; credential_id?: string; request_id?: string }[] = [];
  const upstream = await listen((req, res) => {
    const authorization = req.headers.authorization ?? "";
    if (authorization.includes("bad-key")) {
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
        messages: [{ role: "user", content: "PROMPT_SENTINEL" }],
      }),
    });
    assert.equal(response.status, 200);
    await response.text();
    const rotated = logs.filter((line) => line.msg === "credential_rate_limited");
    assert.equal(rotated.length, 1);
    assert.equal(rotated[0]!.provider, "fake-openai");
    assert.equal(rotated[0]!.credential_id, "bad");
    assert.ok((rotated[0]!.request_id ?? "").length > 0);
    const encoded = JSON.stringify(logs);
    assert.equal(encoded.includes("bad-key"), false);
    assert.equal(encoded.includes("good-key"), false);
    assert.equal(encoded.includes(KEY), false);
    assert.equal(encoded.includes("PROMPT_SENTINEL"), false);
  } finally {
    upstream.close();
  }
});


test("upstream_timeout_log_names_the_phase_and_omits_the_address", async () => {
  const store = await seeded();
  const logs: { msg: string; timeout?: string; bound?: string; provider?: string; model?: string }[] = [];
  const upstream = await listen(() => {
    // Holds the socket open so the header budget expires.
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "upstream-openai", id: "one" }],
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
    onLog: (record) => {
      logs.push(record);
    },
  });
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: "PROMPT_SENTINEL" }] }),
    });
    assert.equal(response.status, 504);
    const timeout = logs.find((line) => line.msg === "upstream_timeout");
    assert.ok(timeout);
    assert.equal(timeout.provider, "fake-openai");
    assert.equal(timeout.model, "gpt-test");
    assert.equal(timeout.timeout, "response_headers");
    assert.equal(timeout.bound, "phase");
    const encoded = JSON.stringify(logs);
    assert.equal(encoded.includes("upstream-openai"), false);
    assert.equal(encoded.includes(KEY), false);
    assert.equal(encoded.includes("PROMPT_SENTINEL"), false);
    assert.equal(encoded.includes(upstream.url), false);
  } finally {
    upstream.close();
  }
});


test("stream_limit_log_names_the_duration_cap", async () => {
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
    assert.equal(text.includes("COMPLETION_SENTINEL"), true);
    const limit = logs.find((line) => line.msg === "stream_limit");
    assert.ok(limit);
    assert.equal(limit.limit, "duration");
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

test("extension stages run in order and unwind after dispatch", async () => {
  const events: string[] = [];
  const app = createAxond({ store: await seeded(), gatewayKey: KEY, defaultNamespace: "platform", providers: [],
    extensions: ["pre-dispatch", "post-auth", "pre-auth"].map((stage) => ({ name: stage, apiVersion: 1 as const, stage: stage as "pre-auth" | "post-auth" | "pre-dispatch",
      async middleware(_c: any, next: () => Promise<void>) { events.push(stage); await next(); events.push(`${stage}:done`); } })) });
  assert.equal((await app.request("http://localhost/ns/platform/v1/models", { headers: { authorization: `Bearer ${KEY}` } })).status, 200);
  assert.deepEqual(events, ["pre-auth", "post-auth", "pre-dispatch", "pre-dispatch:done", "post-auth:done", "pre-auth:done"]);
});
