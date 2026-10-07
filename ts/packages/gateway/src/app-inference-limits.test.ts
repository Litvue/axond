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
      assert.equal(
        error instanceof Error ? error.message : "",
        "config resolution failed: gateway_key for namespace `platform` file `/run/secrets/axond-gateway-key` is empty",
      );
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
    error: { type: "bad_request", message: "bad request: attrs exceeds 4096 byte limit" },
  });
  const fittingAttrs = { n: "é".repeat(2044) };
  assert.equal(utf8(fittingAttrs), 4096);
  const fitting = await post({ id: "fits-attrs", attrs: fittingAttrs });
  assert.equal(fitting.status, 201);

  const many = await post({ id: "many-globs", blocklist: Array.from({ length: 65 }, () => "foo*bar") });
  assert.equal(many.status, 400);
  assert.deepEqual(await many.json(), {
    error: { type: "bad_request", message: "bad request: namespace blocklist exceeds 64 entries" },
  });
  const hugePatterns = Array.from({ length: 64 }, () => "a".repeat(61));
  assert.ok(utf8(hugePatterns) > 4096);
  const huge = await post({ id: "huge-globs", blocklist: hugePatterns });
  assert.equal(huge.status, 400);
  assert.deepEqual(await huge.json(), {
    error: { type: "bad_request", message: "bad request: namespace blocklist exceeds 4 KiB" },
  });
  const sizedBeforeGlob = ["a*a" + "b".repeat(4090)];
  assert.ok(utf8(sizedBeforeGlob) > 4096);
  const sized = await post({ id: "sized-glob", blocklist: sizedBeforeGlob });
  assert.equal(sized.status, 400);
  assert.deepEqual(await sized.json(), {
    error: { type: "bad_request", message: "bad request: namespace blocklist exceeds 4 KiB" },
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
      message: "bad request: blocklist glob `foo*bar` is invalid: use an exact id, `prefix*`, `*suffix`, or `*`",
    },
  });
  const empty = await post({ id: "empty-glob", blocklist: [""] });
  assert.equal(empty.status, 400);
  assert.deepEqual(await empty.json(), {
    error: {
      type: "bad_request",
      message: "bad request: blocklist glob `` is invalid: use an exact id, `prefix*`, `*suffix`, or `*`",
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
      message: "bad request: blocklist glob `*middle*` is invalid: use an exact id, `prefix*`, `*suffix`, or `*`",
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
    assert.deepEqual(payload, { error: { type: "bad_request", message: `bad request: ${message}` } });
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
    error: { type: "bad_request", message: "bad request: `period` is required" },
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


test("json_content_type_matches_axum", async () => {
  const { app, upstream } = await gateway();
  try {
    const post = (contentType: string | null, path: string, body: string) =>
      app.request(`http://127.0.0.1${path}`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${KEY}`,
          ...(contentType === null ? {} : { "content-type": contentType }),
        },
        body,
      });
    const media = {
      error: {
        type: "unsupported_media_type",
        message: "expected a `content-type: application/json` request",
      },
    };
    const suffix = await post("application/cloudevents+json", "/ns/platform/v1/chat/completions", "{}");
    assert.equal(suffix.status, 400);
    assert.deepEqual(await suffix.json(), { error: { type: "bad_request", message: "bad request: missing `model`" } });
    const charset = await post("Application/Json; Charset=UTF-8", "/ns/platform/v1/chat/completions", "{}");
    assert.equal(charset.status, 400);
    assert.deepEqual(await charset.json(), { error: { type: "bad_request", message: "bad request: missing `model`" } });
    const textJson = await post("text/json", "/ns/platform/v1/chat/completions", "{}");
    assert.equal(textJson.status, 415);
    assert.deepEqual(await textJson.json(), media);
    const embedded = await post("fooapplication/json", "/ns/platform/v1/chat/completions", "{}");
    assert.equal(embedded.status, 415);
    assert.deepEqual(await embedded.json(), media);
    const jsonFoo = await post("application/jsonfoo", "/api/v1/namespaces", '{"id":"wsp_ct"}');
    assert.equal(jsonFoo.status, 415);
    assert.deepEqual(await jsonFoo.json(), media);
    const created = await post("application/vnd.api+json", "/api/v1/namespaces", '{"id":"wsp_ct"}');
    assert.equal(created.status, 201);
    const absent = await post(null, "/api/v1/namespaces", '{"id":"wsp_missing"}');
    assert.equal(absent.status, 415);
    assert.deepEqual(await absent.json(), media);
    assert.equal(upstream.requests.length, 0);
  } finally {
    upstream.close();
  }
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
      assert.equal(error instanceof Error ? error.message : "", "failover.max_attempts must be at least 1");
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
      assert.equal(
        error instanceof Error ? error.message : "",
        'config: invalid type: found float `1.5`, expected u32 for key "default.failover.max_attempts"',
      );
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
      assert.equal(
        error instanceof Error ? error.message : "",
        'config: invalid type: found float `1.5`, expected usize for key "default.admission.max_in_flight_settlements"',
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
      assert.equal(
        error instanceof Error ? error.message : "",
        "credential `heavy` has weight 0; remove it instead",
      );
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
      assert.equal(
        error instanceof Error ? error.message : "",
        'config: invalid value signed int `-1`, expected u64 for key "default.admission.max_prompt_tokens"',
      );
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
      assert.equal(
        error instanceof Error ? error.message : "",
        'config: invalid type: found float `1.5`, expected u64 for key "default.admission.max_output_tokens"',
      );
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
      assert.equal(
        error instanceof Error ? error.message : "",
        'config: invalid value signed int `-1`, expected u64 for key "default.admission.max_stream_duration_ms"',
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
      assert.equal(
        error instanceof Error ? error.message : "",
        'config: invalid type: found float `1.5`, expected u64 for key "default.admission.max_stream_bytes"',
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
