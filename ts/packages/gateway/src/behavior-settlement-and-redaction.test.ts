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


test("settlement_failure_log_names_the_reason_and_omits_the_prompt", async () => {
  const logs: { msg: string; reason?: string; waited_ms?: number; request_id?: string }[] = [];
  const store = await seeded();
  let releaseSettle: () => void = () => undefined;
  const gate = new Promise<void>((resolve) => {
    releaseSettle = resolve;
  });
  const original = store.settle.bind(store);
  let entered = 0;
  store.settle = async (input) => {
    entered += 1;
    if (entered === 1) {
      await gate;
    }
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
    onLog: (record) => {
      logs.push(record);
    },
  });
  const payload = JSON.stringify({
    model: "fake-openai/gpt-test",
    messages: [{ role: "user", content: "PROMPT_SENTINEL" }],
  });
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
    const dropped = logs.filter((line) => line.msg === "settlement_failure" && line.reason === "queue_timeout");
    assert.equal(dropped.length, 1);
    assert.ok((dropped[0]!.waited_ms ?? 0) >= 80);
    assert.ok((dropped[0]!.request_id ?? "").length > 0);
    releaseSettle();
    const slow = await seeded();
    const before = (await slow.getBudget("platform", "compat"))!;
    const slowOriginal = slow.settle.bind(slow);
    slow.settle = async (input) => {
      await new Promise((wake) => setTimeout(wake, 80));
      return slowOriginal(input);
    };
    const slowUpstream = await listen((_req, res) => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end('{"usage":{"prompt_tokens":4,"completion_tokens":4}}');
    });
    const slowApp = createAxond({
      store: slow,
      gatewayKey: KEY,
      defaultNamespace: "platform",
      maxInFlight: 4,
      maxInFlightSettlements: 1,
      settlementTimeoutMs: 30,
      providers: [{ id: "fake-openai", kind: "openai", baseUrl: slowUpstream.url }],
      credentials: [{ namespace: "platform", provider: "fake-openai", secret: "sk-live-secret", id: "one" }],
      prices: [
        {
          provider: "fake-openai",
          model: "*",
          inputMicrodollarsPerMillion: 1_000_000n,
          outputMicrodollarsPerMillion: 1_000_000n,
        },
      ],
      onLog: (record) => {
        logs.push(record);
      },
    });
    try {
      const response = await slowApp.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
        method: "POST",
        headers: CHAT_HEADERS,
        body: payload,
      });
      assert.equal(response.status, 200);
      await response.text();
      let after = before;
      for (let attempt = 0; attempt < 40; attempt += 1) {
        after = (await slow.getBudget("platform", "compat"))!;
        if (after.spent !== before.spent) {
          break;
        }
        await new Promise((wake) => setTimeout(wake, 10));
      }
      assert.equal(after.spent - before.spent, 8n);
      const timed = logs.filter((line) => line.msg === "settlement_failure" && line.reason === "execution_timeout");
      assert.equal(timed.length, 1);
      assert.ok((timed[0]!.waited_ms ?? 0) >= 30);
      assert.ok((timed[0]!.request_id ?? "").length > 0);
      const encoded = JSON.stringify(logs);
      assert.equal(encoded.includes("PROMPT_SENTINEL"), false);
      assert.equal(encoded.includes("sk-live-secret"), false);
      assert.equal(encoded.includes(KEY), false);
      assert.equal(encoded.includes(slowUpstream.url), false);
      assert.equal(encoded.includes(upstream.url), false);
    } finally {
      slowUpstream.close();
    }
  } finally {
    releaseSettle();
    upstream.close();
  }
});


test("charge_failure_log_names_the_reason_and_omits_the_driver_text", async () => {
  const driver = "password=secret host=db.internal:5432/axond ECONNREFUSED";
  const logs: { msg: string; reason?: string; waited_ms?: number; request_id?: string }[] = [];
  const usage: UsageRecord[] = [];
  const store = await seeded();
  const before = (await store.getBudget("platform", "compat"))!;
  store.settle = async () => {
    const failure = new StoreFailure();
    failure.message = driver;
    throw failure;
  };
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "application/json" });
    res.end('{"usage":{"prompt_tokens":4,"completion_tokens":4}}');
  });
  const metrics = createMetrics(["sk-live-secret", KEY]);
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
    onLog: (record) => {
      logs.push(record);
    },
    onUsage: (record) => {
      usage.push(record);
    },
  });
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: CHAT_HEADERS,
      body: JSON.stringify({
        model: "fake-openai/gpt-test",
        messages: [{ role: "user", content: "PROMPT_SENTINEL" }],
      }),
    });
    assert.equal(response.status, 200);
    await response.text();
    let failed: (typeof logs)[number] | undefined;
    for (let attempt = 0; attempt < 40; attempt += 1) {
      failed = logs.find((line) => line.msg === "settlement_failure" && line.reason === "charge_failed");
      if (failed) {
        break;
      }
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.ok(failed);
    assert.equal(failed.waited_ms, undefined);
    assert.ok((failed.request_id ?? "").length > 0);
    const after = (await store.getBudget("platform", "compat"))!;
    assert.equal(after.spent, before.spent);
    assert.equal(usage.length, 0);
    const point = metrics.points.find(
      (item) => item.name === "axond.settlement.failures" && item.attributes["axond.settlement.reason"] === "charge_failed",
    );
    assert.equal(point?.value, 1);
    const encoded = JSON.stringify(logs);
    assert.equal(encoded.includes(driver), false);
    assert.equal(encoded.includes("db.internal"), false);
    assert.equal(encoded.includes("ECONNREFUSED"), false);
    assert.equal(encoded.includes("PROMPT_SENTINEL"), false);
    assert.equal(encoded.includes("sk-live-secret"), false);
    assert.equal(encoded.includes(KEY), false);
    assert.equal(encoded.includes(upstream.url), false);
  } finally {
    upstream.close();
  }
});


test("settlement_panic_log_names_the_reason_and_omits_the_thrown_text", async () => {
  const thrown = "password=secret host=db.internal:5432/axond settlement blew up";
  const logs: { msg: string; reason?: string; waited_ms?: number; request_id?: string }[] = [];
  const usage: UsageRecord[] = [];
  const store = await seeded();
  const before = (await store.getBudget("platform", "compat"))!;
  store.settle = async () => {
    throw new Error(thrown);
  };
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "application/json" });
    res.end('{"usage":{"prompt_tokens":4,"completion_tokens":4}}');
  });
  const metrics = createMetrics(["sk-live-secret", KEY]);
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
    onLog: (record) => {
      logs.push(record);
    },
    onUsage: (record) => {
      usage.push(record);
    },
  });
  try {
    const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: CHAT_HEADERS,
      body: JSON.stringify({
        model: "fake-openai/gpt-test",
        messages: [{ role: "user", content: "PROMPT_SENTINEL" }],
      }),
    });
    assert.equal(response.status, 200);
    await response.text();
    let failed: (typeof logs)[number] | undefined;
    for (let attempt = 0; attempt < 40; attempt += 1) {
      failed = logs.find((line) => line.msg === "settlement_failure" && line.reason === "panicked");
      if (failed) {
        break;
      }
      await new Promise((wake) => setTimeout(wake, 10));
    }
    assert.ok(failed);
    assert.equal(failed.waited_ms, undefined);
    assert.ok((failed.request_id ?? "").length > 0);
    const after = (await store.getBudget("platform", "compat"))!;
    assert.equal(after.spent, before.spent);
    assert.equal(usage.length, 0);
    const point = metrics.points.find(
      (item) => item.name === "axond.settlement.failures" && item.attributes["axond.settlement.reason"] === "panicked",
    );
    assert.equal(point?.value, 1);
    const chargeFailed = metrics.points.find(
      (item) => item.name === "axond.settlement.failures" && item.attributes["axond.settlement.reason"] === "charge_failed",
    );
    assert.equal(chargeFailed, undefined);
    const encoded = JSON.stringify(logs);
    assert.equal(encoded.includes(thrown), false);
    assert.equal(encoded.includes("db.internal"), false);
    assert.equal(encoded.includes("password=secret"), false);
    assert.equal(encoded.includes("PROMPT_SENTINEL"), false);
    assert.equal(encoded.includes("sk-live-secret"), false);
    assert.equal(encoded.includes(KEY), false);
    assert.equal(encoded.includes(upstream.url), false);
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
  let released = false;
  let finishUpstream: (() => void) | null = null;
  const releaseUpstream = () => {
    released = true;
    finishUpstream?.();
    finishUpstream = null;
  };
  const upstream = await listen((_req, res) => {
    const finish = () => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end('{"usage":{"prompt_tokens":1,"completion_tokens":1}}');
    };
    if (released) {
      finish();
      return;
    }
    finishUpstream = finish;
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


test("upstream_redirect_is_not_followed_and_omits_the_secret", async () => {
  const secret = "sk-redirect-sentinel";
  let targetHits = 0;
  const target = await listen((_req, res) => {
    targetHits += 1;
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ id: secret, choices: [{ message: { content: secret } }] }));
  });
  const moved = await listen((req, res) => {
    assert.equal(req.headers.authorization, `Bearer ${secret}`);
    res.writeHead(302, { location: `${target.url}/landed/${secret}` });
    res.end();
  });
  const store = await seeded();
  const logs: unknown[] = [];
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: moved.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret, id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 2_500_000n,
        outputMicrodollarsPerMillion: 10_000_000n,
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
    const text = await response.text();
    assert.equal(response.status, 400);
    const body = JSON.parse(text) as { error: { type: string; message: string } };
    assert.equal(body.error.type, "invalid_request");
    assert.equal(body.error.message, "invalid provider request: ");
    assert.equal(targetHits, 0);
    assert.equal(text.includes(secret), false);
    assert.equal(text.includes(target.url), false);
    const encoded = JSON.stringify(logs);
    assert.equal(encoded.includes(secret), false);
    assert.equal(encoded.includes(target.url), false);
    assert.equal(encoded.includes("PROMPT_SENTINEL"), false);
  } finally {
    moved.close();
    target.close();
  }
});


test("provider_error_replaces_the_echoed_credential", async () => {
  const openaiSecret = "sk-echo-openai";
  const anthropicSecret = "sk-echo-anthropic";
  const upstream = await listen((req, res) => {
    const messages = (req.url ?? "").includes("/messages");
    const secret = messages ? anthropicSecret : openaiSecret;
    assert.equal(messages ? req.headers["x-api-key"] : req.headers.authorization, messages ? secret : `Bearer ${secret}`);
    res.writeHead(401, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: { message: `rejected key ${secret}` } }));
  });
  const store = await seeded();
  const logs: unknown[] = [];
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [
      { id: "fake-openai", kind: "openai", baseUrl: upstream.url },
      { id: "fake-anthropic", kind: "anthropic", baseUrl: upstream.url },
    ],
    credentials: [
      { namespace: "platform", provider: "fake-openai", secret: openaiSecret, id: "openai" },
      { namespace: "platform", provider: "fake-anthropic", secret: anthropicSecret, id: "anthropic" },
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
    onLog: (record) => {
      logs.push(record);
    },
  });
  try {
    const chat = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({
        model: "fake-openai/gpt-test",
        messages: [{ role: "user", content: "PROMPT_SENTINEL" }],
      }),
    });
    const chatText = await chat.text();
    assert.equal(chat.status, 502);
    const chatBody = JSON.parse(chatText) as { error: { type: string; message: string } };
    assert.equal(chatBody.error.type, "invalid_request");
    assert.equal(chatBody.error.message, "invalid provider request: rejected key [REDACTED]");
    assert.equal(chatText.includes(openaiSecret), false);
    const messages = await app.request("http://127.0.0.1/ns/platform/v1/messages", {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({
        model: "fake-anthropic/claude-test",
        max_tokens: 16,
        messages: [{ role: "user", content: "PROMPT_SENTINEL" }],
      }),
    });
    const messageText = await messages.text();
    assert.equal(messages.status, 502);
    const messageBody = JSON.parse(messageText) as { error: { type: string; message: string } };
    assert.equal(messageBody.error.type, "invalid_request");
    assert.equal(messageBody.error.message, "invalid provider request: rejected key [REDACTED]");
    assert.equal(messageText.includes(anthropicSecret), false);
    const encoded = JSON.stringify(logs);
    assert.equal(encoded.includes(openaiSecret), false);
    assert.equal(encoded.includes(anthropicSecret), false);
    assert.equal(encoded.includes(KEY), false);
    assert.equal(encoded.includes("PROMPT_SENTINEL"), false);
  } finally {
    upstream.close();
  }
});


test("provider_diagnostics_keep_context_limits_and_a_bounded_message", async () => {
  const marker = "… [truncated]";
  const markerBytes = new TextEncoder().encode(marker).length;
  const cases: Array<{ status: number; body: string; http: number; type: string; message: string; rateLimited: boolean }> = [
    {
      status: 400,
      body: JSON.stringify({ error: { code: "context_length_exceeded", message: "too long" } }),
      http: 400,
      type: "context_window_exceeded",
      message: "too long",
      rateLimited: false,
    },
    {
      status: 400,
      body: JSON.stringify({ error: { message: "prompt is too long: 250000 tokens" } }),
      http: 400,
      type: "context_window_exceeded",
      message: "prompt is too long: 250000 tokens",
      rateLimited: false,
    },
    {
      status: 400,
      body: JSON.stringify({ message: "input exceeds the maximum number of tokens" }),
      http: 400,
      type: "context_window_exceeded",
      message: "input exceeds the maximum number of tokens",
      rateLimited: false,
    },
    {
      status: 401,
      body: JSON.stringify({ error: { code: "context_length_exceeded", message: "too long" } }),
      http: 502,
      type: "context_window_exceeded",
      message: "too long",
      rateLimited: false,
    },
    {
      status: 429,
      body: JSON.stringify({ error: { message: "context window exceeded" } }),
      http: 400,
      type: "context_window_exceeded",
      message: "context window exceeded",
      rateLimited: false,
    },
    {
      status: 500,
      body: "prompt is too long",
      http: 400,
      type: "context_window_exceeded",
      message: "prompt is too long",
      rateLimited: false,
    },
    {
      status: 500,
      body: "upstream unavailable",
      http: 502,
      type: "provider_dependency_failed",
      message: "upstream unavailable",
      rateLimited: false,
    },
    {
      status: 302,
      body: "",
      http: 400,
      type: "invalid_request",
      message: "",
      rateLimited: false,
    },
  ];
  for (const item of cases) {
    const failure = classifyUpstream(item.status, item.body);
    assert.equal(failure.status, item.http, item.body);
    assert.equal(failure.type, item.type, item.body);
    assert.equal(failure.message, item.message, item.body);
    assert.equal(failure.rateLimited, item.rateLimited, item.body);
    assert.equal(failure.message.endsWith(marker), false, item.body);
  }
  const plain = classifyUpstream(500, "x".repeat(4 * 4096));
  assert.equal(plain.type, "provider_dependency_failed");
  assert.equal(plain.message.endsWith(marker), true);
  assert.ok(new TextEncoder().encode(plain.message).length <= 4096 + markerBytes);
  const nested = classifyUpstream(500, JSON.stringify({ error: { message: "y".repeat(64 * 1024) } }));
  assert.equal(nested.message.endsWith(marker), true);
  assert.ok(new TextEncoder().encode(nested.message).length <= 4096 + markerBytes);
  assert.equal(nested.message.includes("y".repeat(5000)), false);
  const euros = classifyUpstream(500, "€".repeat(4096));
  assert.equal(euros.message.endsWith(marker), true);
  assert.ok(euros.message.slice(0, -marker.length).split("").every((character) => character === "€"));
  assert.ok(new TextEncoder().encode(euros.message).length <= 4096 + markerBytes);

  const secret = "sk-context-sentinel";
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
  let hits = 0;
  const upstream = await listen((_req, res) => {
    hits += 1;
    if (hits === 1) {
      res.writeHead(400, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { code: "context_length_exceeded", message: `too long ${secret}` } }));
      return;
    }
    res.writeHead(401, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: { message: `rejected key ${secret}; ${"y".repeat(6000)}` } }));
  });
  const store = await seeded();
  const logs: unknown[] = [];
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret, id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
    telemetry: { endpoint: `http://127.0.0.1:${collectorAddress.port}` },
    onLog: (record) => {
      logs.push(record);
    },
  });
  const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
  const chat = (content: string) =>
    app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers,
      body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content }] }),
    });
  try {
    const context = await chat("PROMPT_SENTINEL");
    const contextText = await context.text();
    assert.equal(context.status, 400);
    const contextBody = JSON.parse(contextText) as { error: { type: string; message: string } };
    assert.equal(contextBody.error.type, "context_window_exceeded");
    assert.equal(contextBody.error.message, "context window exceeded: too long [REDACTED]");
    assert.equal(contextText.includes(secret), false);

    const refused = await chat("PROMPT_SENTINEL");
    const refusedText = await refused.text();
    assert.equal(refused.status, 502);
    const refusedBody = JSON.parse(refusedText) as { error: { type: string; message: string } };
    assert.equal(refusedBody.error.type, "invalid_request");
    assert.equal(refusedBody.error.message.startsWith("invalid provider request: rejected key [REDACTED]"), true);
    assert.equal(refusedBody.error.message.endsWith(marker), true);
    assert.ok(new TextEncoder().encode(refusedBody.error.message).length <= 4096 + markerBytes + "invalid provider request: ".length);
    assert.equal(refusedText.includes(secret), false);
    assert.equal(refusedBody.error.message.includes("y".repeat(5000)), false);

    const deadline = Date.now() + 2_000;
    while (traces.length < 2 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.equal(traces.length >= 2, true);
    const spans = JSON.parse(traces[1]!).resourceSpans[0].scopeSpans[0].spans as {
      name: string;
      attributes: { key: string; value: { stringValue: string } }[];
    }[];
    const attempt = spans.find((span) => span.name === "axond.upstream.attempt");
    assert.ok(attempt);
    const recorded = attempt.attributes.find((item) => item.key === "axond.upstream.message")?.value.stringValue ?? "";
    assert.equal(recorded.startsWith("rejected key [REDACTED]"), true);
    assert.equal(recorded.endsWith(marker), false);
    assert.ok(new TextEncoder().encode(recorded).length <= 4096);
    assert.ok(new TextEncoder().encode(recorded).length > 512);
    const exported = `${traces.join("\n")}\n${JSON.stringify(logs)}`;
    assert.equal(exported.includes(secret), false);
    assert.equal(exported.includes(KEY), false);
    assert.equal(exported.includes("PROMPT_SENTINEL"), false);
  } finally {
    upstream.close();
    collector.closeAllConnections();
    collector.close();
  }
});
