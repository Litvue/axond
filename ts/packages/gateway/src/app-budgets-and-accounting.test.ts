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


test("budget_policy_follows_the_cadence_period", async () => {
  let now = Date.parse("2026-09-30T12:00:00Z");
  const store = createMemoryStore();
  const app = createAxond({
    store,
    providers: [],
    gatewayKey: KEY,
    defaultNamespace: "platform",
    clock: () => now,
  });
  const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
  const call = (path: string, method: string, body?: string) =>
    app.request(`http://127.0.0.1${path}`, { method, headers, ...(body === undefined ? {} : { body }) });
  assert.equal((await call("/api/v1/namespaces", "POST", '{"id":"wsp_roll"}')).status, 201);
  assert.equal((await call("/api/v1/namespaces", "POST", '{"id":"wsp_resume"}')).status, 201);
  assert.equal((await call("/api/v1/namespaces", "POST", '{"id":"wsp_need"}')).status, 201);
  assert.equal((await call("/api/v1/namespaces", "POST", '{"id":"wsp_legacy"}')).status, 201);
  const monthly = await call(
    "/api/v1/namespaces/wsp_roll/budget",
    "PUT",
    '{"cadence":"monthly","limit_microdollars":1000,"timezone":"UTC"}',
  );
  assert.equal(monthly.status, 200);
  assert.equal(
    await monthly.text(),
    '{"namespace":"wsp_roll","cadence":"monthly","limit_microdollars":1000,"timezone":"UTC","period":"2026-09","spent_microdollars":0,"reserved_microdollars":0,"remaining_microdollars":1000,"active":true}',
  );
  await store.settle({
    requestId: "r1",
    namespace: "wsp_roll",
    period: "2026-09",
    model: "m",
    status: "ok",
    cost: 40n,
    incarnation: 1n,
  });
  const spent = await call("/api/v1/namespaces/wsp_roll/budget", "GET");
  const spentBody =
    '{"namespace":"wsp_roll","cadence":"monthly","limit_microdollars":1000,"timezone":"UTC","period":"2026-09","spent_microdollars":40,"reserved_microdollars":0,"remaining_microdollars":960,"active":true}';
  assert.equal(await spent.text(), spentBody);
  const other = await call("/api/v1/namespaces/wsp_roll/budgets/legacy", "PUT", '{"limit_microdollars":7}');
  assert.equal(other.status, 200);
  assert.equal(
    await other.text(),
    '{"namespace":"wsp_roll","period":"legacy","limit_microdollars":7,"spent_microdollars":0,"reserved_microdollars":0,"remaining_microdollars":7,"active":false}',
  );
  const still = await call("/api/v1/namespaces/wsp_roll/budget", "GET");
  assert.equal(await still.text(), spentBody);
  const current = await call("/api/v1/namespaces/wsp_roll/budgets/2026-09", "GET");
  assert.equal((await current.json()).active, true);
  const lowered = await call("/api/v1/namespaces/wsp_roll/budgets/2026-09", "PUT", '{"limit_microdollars":250}');
  assert.equal(lowered.status, 200);
  assert.equal((await lowered.json()).active, true);
  const ledgerLimit = await (await call("/api/v1/namespaces/wsp_roll/budget", "GET")).json();
  assert.equal(ledgerLimit.limit_microdollars, 250);
  assert.equal(ledgerLimit.spent_microdollars, 40);
  assert.equal(ledgerLimit.remaining_microdollars, 210);
  now = Date.parse("2026-10-01T00:00:00Z");
  const rolled = await call("/api/v1/namespaces/wsp_roll/budget", "GET");
  assert.equal(
    await rolled.text(),
    '{"namespace":"wsp_roll","cadence":"monthly","limit_microdollars":1000,"timezone":"UTC","period":"2026-10","spent_microdollars":0,"reserved_microdollars":0,"remaining_microdollars":1000,"active":true}',
  );
  assert.equal((await call("/api/v1/namespaces/wsp_roll/budgets/2026-10", "GET")).status, 404);
  assert.equal((await (await call("/api/v1/namespaces/wsp_roll/budgets/2026-09", "GET")).json()).active, false);
  assert.equal(
    (await call("/api/v1/namespaces/wsp_resume/budget", "PUT", '{"cadence":"monthly","limit_microdollars":1000,"timezone":"UTC"}')).status,
    200,
  );
  assert.equal((await call("/api/v1/namespaces/wsp_resume/budgets/legacy", "PUT", '{"limit_microdollars":7}')).status, 200);
  await store.resolveNamespace("wsp_resume", now);
  const resumed = await call(
    "/api/v1/namespaces/wsp_resume/budget",
    "PUT",
    '{"cadence":"fixed","limit_microdollars":9}',
  );
  assert.equal(resumed.status, 200);
  assert.equal(
    await resumed.text(),
    '{"namespace":"wsp_resume","cadence":"fixed","limit_microdollars":9,"timezone":"UTC","period":"legacy","spent_microdollars":0,"reserved_microdollars":0,"remaining_microdollars":9,"active":true}',
  );
  const needsPeriod = await call(
    "/api/v1/namespaces/wsp_need/budget",
    "PUT",
    '{"cadence":"monthly","limit_microdollars":1,"timezone":"UTC"}',
  );
  assert.equal(needsPeriod.status, 200);
  const missing = await call("/api/v1/namespaces/wsp_need/budget", "PUT", '{"cadence":"fixed","limit_microdollars":1}');
  assert.equal(missing.status, 400);
  assert.deepEqual(await missing.json(), {
    error: { type: "bad_request", message: "bad request: fixed cadence needs a period: the namespace has no active period" },
  });
  assert.equal((await call("/api/v1/namespaces/wsp_legacy/budgets/legacy", "PUT", '{"limit_microdollars":11}')).status, 200);
  const synthesized = await call("/api/v1/namespaces/wsp_legacy/budget", "GET");
  assert.equal(
    await synthesized.text(),
    '{"namespace":"wsp_legacy","cadence":"fixed","limit_microdollars":11,"timezone":"UTC","period":"legacy","spent_microdollars":0,"reserved_microdollars":0,"remaining_microdollars":11,"active":true}',
  );
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
      assert.deepEqual(await response.json(), { error: { type: "bad_request", message: `bad request: ${message}` } });
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
      assert.deepEqual(await response.json(), { error: { type: "bad_request", message: `bad request: ${message}` } });
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
      assert.deepEqual(await response.json(), { error: { type: "bad_request", message: `bad request: ${message}` } });
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


test("management_json_string_escapes_match_serde", async () => {
  const { app, upstream } = await gateway();
  try {
    const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
    const call = (path: string, method: string, body?: string) =>
      app.request(`http://127.0.0.1${path}`, { method, headers, ...(body === undefined ? {} : { body }) });
    const bad = async (path: string, method: string, body: string, message: string) => {
      const response = await call(path, method, body);
      assert.equal(response.status, 400, `${method} ${path} ${JSON.stringify(body)}`);
      assert.deepEqual(await response.json(), { error: { type: "bad_request", message: `bad request: ${message}` } });
    };
    const parse = "Failed to parse the request body as JSON";
    const data = "Failed to deserialize the JSON body into the target type";
    const control = "control character (\\u0000-\\u001F) found while parsing a string";
    await bad("/api/v1/namespaces", "POST", '{"id":"\\q"}', `${parse}: id: invalid escape at line 1 column 9`);
    await bad("/api/v1/namespaces", "POST", '{"id":"\\', `${parse}: id: EOF while parsing a string at line 1 column 8`);
    await bad("/api/v1/namespaces", "POST", '{"id":"a\u0001b"}', `${parse}: id: ${control} at line 1 column 9`);
    await bad("/api/v1/namespaces", "POST", '{"id":"a\nb"}', `${parse}: id: ${control} at line 2 column 0`);
    await bad("/api/v1/namespaces", "POST", '{"id":"\\u12XY"}', `${parse}: id: invalid escape at line 1 column 13`);
    await bad("/api/v1/namespaces", "POST", '{"id":"\\u12"}', `${parse}: id: invalid escape at line 1 column 13`);
    await bad("/api/v1/namespaces", "POST", '{"id":"\\u12', `${parse}: id: EOF while parsing a string at line 1 column 11`);
    await bad("/api/v1/namespaces", "POST", '{"id":"\\uDFFF"}', `${parse}: id: lone leading surrogate in hex escape at line 1 column 13`);
    await bad("/api/v1/namespaces", "POST", '{"id":"\\uD800"}', `${parse}: id: unexpected end of hex escape at line 1 column 14`);
    await bad("/api/v1/namespaces", "POST", '{"id":"\\uD800x"}', `${parse}: id: unexpected end of hex escape at line 1 column 14`);
    await bad("/api/v1/namespaces", "POST", '{"id":"\\uD800\\uD800"}', `${parse}: id: lone leading surrogate in hex escape at line 1 column 19`);
    await bad("/api/v1/namespaces", "POST", '{"\\q":1}', `${parse}: invalid escape at line 1 column 4`);
    await bad("/api/v1/namespaces", "POST", '{"id":"é\\q"}', `${parse}: id: invalid escape at line 1 column 11`);
    await bad(
      "/api/v1/namespaces",
      "POST",
      '{"id":"a","attrs":{"k":"\\q"}}',
      `${parse}: attrs.k: invalid escape at line 1 column 26`,
    );
    await bad(
      "/api/v1/namespaces",
      "POST",
      '{"id":"a","attrs":["\\q"]}',
      `${parse}: attrs[0]: invalid escape at line 1 column 22`,
    );
    await bad(
      "/api/v1/namespaces",
      "POST",
      '{"id":"a","attrs":{"a":[{"b":"\\q"}]}}',
      `${parse}: attrs.a[0].b: invalid escape at line 1 column 32`,
    );
    await bad(
      "/api/v1/namespaces",
      "POST",
      '{"id":"a","attrs":{"\\q":1}}',
      `${parse}: attrs.?: invalid escape at line 1 column 22`,
    );
    await bad(
      "/api/v1/namespaces",
      "POST",
      '{"id":"a","attrs":{"a.b":"\\q"}}',
      `${parse}: attrs.a.b: invalid escape at line 1 column 28`,
    );
    await bad(
      "/api/v1/namespaces",
      "POST",
      '{"id":"a","blocklist":["ok","\\q"]}',
      `${parse}: blocklist[1]: invalid escape at line 1 column 31`,
    );
    await bad(
      "/api/v1/namespaces",
      "POST",
      '{"id":"a","blocklist":"\\q"}',
      `${parse}: blocklist: invalid escape at line 1 column 25`,
    );
    await bad(
      "/api/v1/namespaces",
      "POST",
      '["\\q"]',
      `${parse}: [0]: invalid escape at line 1 column 4`,
    );
    await bad(
      "/api/v1/namespaces",
      "POST",
      '["ok", {}, ["\\q"]]',
      `${parse}: [2][0]: invalid escape at line 1 column 15`,
    );
    await bad(
      "/api/v1/namespaces/wsp_esc/budgets/2026-09",
      "PUT",
      '{"limit_microdollars":"\\q"}',
      `${parse}: limit_microdollars: invalid escape at line 1 column 25`,
    );
    await bad(
      "/api/v1/namespaces/wsp_esc/budgets/2026-09",
      "PUT",
      '{"limit_microdollars":{"a":"\\q"}}',
      `${data}: limit_microdollars: invalid type: map, expected u64 at line 1 column 22`,
    );
    await bad(
      "/api/v1/namespaces",
      "POST",
      '{"id":{"a":"\\q"}}',
      `${data}: id: invalid type: map, expected a string at line 1 column 6`,
    );
    await bad(
      "/api/v1/namespaces/wsp_esc/budget",
      "PUT",
      '{"cadence":"\\q","limit_microdollars":1}',
      `${parse}: cadence: invalid escape at line 1 column 14`,
    );
    await bad(
      "/api/v1/namespaces",
      "POST",
      '{"id":"A\\nB"}',
      "a namespace identifier contains a character outside ASCII letters, digits, `.`, `-`, and `_`",
    );
    const created = await call("/api/v1/namespaces", "POST", '{"id":"\\u0041"}');
    assert.equal(created.status, 201);
    assert.equal((await created.json()).id, "A");
    const stored = await call(
      "/api/v1/namespaces",
      "POST",
      '{"id":"wsp_esc","attrs":"\\uD800\\uDC00"}',
    );
    assert.equal(stored.status, 201);
    assert.equal((await stored.json()).attrs, String.fromCodePoint(0x10000));
    const newline = await call("/api/v1/namespaces/wsp_esc", "PUT", '{"attrs":"a\\nb"}');
    assert.equal(newline.status, 200);
    assert.equal((await newline.json()).attrs, "a\nb");
    const missing = await call("/api/v1/namespaces/not-created", "GET");
    assert.equal(missing.status, 404);
  } finally {
    upstream.close();
  }
});


test("management_json_cadence_unit_enum_matches_serde", async () => {
  const { app, upstream } = await gateway();
  try {
    const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
    const call = (path: string, method: string, body?: string) =>
      app.request(`http://127.0.0.1${path}`, { method, headers, ...(body === undefined ? {} : { body }) });
    const data = "Failed to deserialize the JSON body into the target type";
    const parse = "Failed to parse the request body as JSON";
    const created = await call("/api/v1/namespaces", "POST", '{"id":"wsp_cad"}');
    assert.equal(created.status, 201);
    const bad = async (body: string, message: string) => {
      const response = await call("/api/v1/namespaces/wsp_cad/budget", "PUT", body);
      assert.equal(response.status, 400);
      assert.deepEqual(await response.json(), { error: { type: "bad_request", message: `bad request: ${message}` } });
    };
    const monthly = await call(
      "/api/v1/namespaces/wsp_cad/budget",
      "PUT",
      '{"cadence":{"monthly":null},"limit_microdollars":1}',
    );
    assert.equal(monthly.status, 200);
    assert.equal((await monthly.json()).cadence, "monthly");
    const spaced = await call(
      "/api/v1/namespaces/wsp_cad/budget",
      "PUT",
      '{"cadence": { "fixed" : null },"limit_microdollars":1,"period":"2026-09"}',
    );
    assert.equal(spaced.status, 200);
    assert.equal((await spaced.json()).cadence, "fixed");
    const decoded = await call(
      "/api/v1/namespaces/wsp_cad/budget",
      "PUT",
      '{"cadence":{"mon\\u0074hly":null},"limit_microdollars":1}',
    );
    assert.equal(decoded.status, 200);
    assert.equal((await decoded.json()).cadence, "monthly");
    const positional = await call("/api/v1/namespaces/wsp_cad/budget", "PUT", '[{"monthly":null},1]');
    assert.equal(positional.status, 200);
    assert.equal((await positional.json()).cadence, "monthly");
    const fixedOnly = await call(
      "/api/v1/namespaces/wsp_cad/budget",
      "PUT",
      '{"cadence":{"fixed":null},"limit_microdollars":1}',
    );
    assert.equal(fixedOnly.status, 200);
    const resumed = await fixedOnly.json();
    assert.equal(resumed.cadence, "fixed");
    assert.equal(resumed.period, "2026-09");
    assert.equal(resumed.limit_microdollars, 1);
    await bad(
      '{"cadence":{"a":"\\q"},"limit_microdollars":1}',
      `${data}: cadence: unknown variant \`a\`, expected \`monthly\` or \`fixed\` at line 1 column 15`,
    );
    await bad(
      '{"cadence":{"weekly":null},"limit_microdollars":1}',
      `${data}: cadence: unknown variant \`weekly\`, expected \`monthly\` or \`fixed\` at line 1 column 20`,
    );
    await bad(
      '{"cadence":{"weekly"}}',
      `${data}: cadence: unknown variant \`weekly\`, expected \`monthly\` or \`fixed\` at line 1 column 20`,
    );
    await bad(
      '{"cadence":{"":null},"limit_microdollars":1}',
      `${data}: cadence: unknown variant \`\`, expected \`monthly\` or \`fixed\` at line 1 column 14`,
    );
    await bad(
      '{"cadence":{"\\u0061":null},"limit_microdollars":1}',
      `${data}: cadence: unknown variant \`a\`, expected \`monthly\` or \`fixed\` at line 1 column 20`,
    );
    await bad(
      '{"cadence":{"monthly":"\\q"},"limit_microdollars":1}',
      `${parse}: cadence.monthly: invalid escape at line 1 column 25`,
    );
    await bad(
      '{"cadence":{"monthly":"\\uD800"},"limit_microdollars":1}',
      `${parse}: cadence.monthly: unexpected end of hex escape at line 1 column 30`,
    );
    await bad(
      '{"cadence":{"fixed":[]},"limit_microdollars":1}',
      `${data}: cadence.fixed: invalid type: sequence, expected unit at line 1 column 20`,
    );
    await bad(
      '{"cadence":{"monthly":{}},"limit_microdollars":1}',
      `${data}: cadence.monthly: invalid type: map, expected unit at line 1 column 22`,
    );
    await bad(
      '{"cadence":{"monthly":"no"},"limit_microdollars":1}',
      `${data}: cadence.monthly: invalid type: string "no", expected unit at line 1 column 26`,
    );
    await bad(
      '{"cadence":{"monthly":"a\\"b"},"limit_microdollars":1}',
      `${data}: cadence.monthly: invalid type: string "a\\"b", expected unit at line 1 column 28`,
    );
    await bad(
      '{"cadence":{"monthly":true},"limit_microdollars":1}',
      `${data}: cadence.monthly: invalid type: boolean \`true\`, expected unit at line 1 column 26`,
    );
    await bad(
      '{"cadence":{"monthly":1},"limit_microdollars":1}',
      `${data}: cadence.monthly: invalid type: integer \`1\`, expected unit at line 1 column 23`,
    );
    await bad(
      '{"cadence":{"monthly":-1},"limit_microdollars":1}',
      `${data}: cadence.monthly: invalid type: integer \`-1\`, expected unit at line 1 column 24`,
    );
    await bad(
      '{"cadence":{"monthly":1.5},"limit_microdollars":1}',
      `${data}: cadence.monthly: invalid type: floating point \`1.5\`, expected unit at line 1 column 25`,
    );
    await bad(
      '{"cadence":{"monthly":1e2},"limit_microdollars":1}',
      `${data}: cadence.monthly: invalid type: floating point \`100.0\`, expected unit at line 1 column 25`,
    );
    await bad(
      '{"cadence":{"monthly":null,"fixed":null},"limit_microdollars":1}',
      `${parse}: cadence: expected value at line 1 column 26`,
    );
    await bad(
      '{"cadence":{"monthly":null ,"fixed":null},"limit_microdollars":1}',
      `${parse}: cadence: expected value at line 1 column 27`,
    );
    await bad(
      '{"cadence":{"monthly":null,},"limit_microdollars":1}',
      `${parse}: cadence: expected value at line 1 column 26`,
    );
    await bad('{"cadence":1,"limit_microdollars":1}', `${parse}: cadence: expected value at line 1 column 12`);
    await bad('{"cadence":null,"limit_microdollars":1}', `${parse}: cadence: expected value at line 1 column 12`);
    await bad('{"cadence":[],"limit_microdollars":1}', `${parse}: cadence: expected value at line 1 column 12`);
    await bad('{"cadence":{},"limit_microdollars":1}', `${parse}: cadence: expected value at line 1 column 13`);
    await bad('{"cadence":{1:null},"limit_microdollars":1}', `${parse}: cadence: key must be a string at line 1 column 13`);
    await bad('{"cadence":{"monthly"},"limit_microdollars":1}', `${parse}: cadence: expected \`:\` at line 1 column 22`);
    await bad('{"cadence":{"\\q":null},"limit_microdollars":1}', `${parse}: cadence: invalid escape at line 1 column 15`);
    await bad('{"cadence":{"monthly":', `${parse}: cadence.monthly: EOF while parsing a value at line 1 column 22`);
    await bad('{"cadence":{', `${parse}: cadence: EOF while parsing an object at line 1 column 12`);
    await bad(
      '{"cadence":{"monthly":1e},"limit_microdollars":1}',
      `${parse}: cadence.monthly: invalid number at line 1 column 25`,
    );
    await bad(
      '{"cadence":{"monthly":nul},"limit_microdollars":1}',
      `${parse}: cadence.monthly: expected ident at line 1 column 26`,
    );
    await bad(
      '{"cadence":{"monthly":},"limit_microdollars":1}',
      `${parse}: cadence.monthly: expected value at line 1 column 23`,
    );
    await bad(
      '[{"a":"\\q"},1]',
      `${data}: [0]: unknown variant \`a\`, expected \`monthly\` or \`fixed\` at line 1 column 5`,
    );
    await bad(
      '[{"fixed":[]},1]',
      `${data}: [0].fixed: invalid type: sequence, expected unit at line 1 column 10`,
    );
    await bad('[{"monthly":null,"fixed":null},1]', `${parse}: [0]: expected value at line 1 column 16`);
    const kept = await call("/api/v1/namespaces/wsp_cad/budget", "GET");
    assert.equal(kept.status, 200);
    const keptBody = await kept.json();
    assert.equal(keptBody.cadence, "fixed");
    assert.equal(keptBody.period, "2026-09");
  } finally {
    upstream.close();
  }
});


test("management_json_invalid_utf8_matches_serde", async () => {
  const { app, upstream } = await gateway();
  try {
    const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
    const call = (path: string, method: string, body?: Uint8Array) =>
      app.request(`http://127.0.0.1${path}`, { method, headers, ...(body === undefined ? {} : { body }) });
    const parse = "Failed to parse the request body as JSON";
    const data = "Failed to deserialize the JSON body into the target type";
    const utf8 = (...parts: Array<string | number>) => {
      const out: number[] = [];
      for (const part of parts) {
        if (typeof part === "number") {
          out.push(part);
        } else {
          for (const byte of new TextEncoder().encode(part)) {
            out.push(byte);
          }
        }
      }
      return Uint8Array.from(out);
    };
    const bad = async (path: string, method: string, body: Uint8Array, message: string) => {
      const response = await call(path, method, body);
      assert.equal(response.status, 400, `${method} ${path} ${message}`);
      assert.deepEqual(await response.json(), { error: { type: "bad_request", message: `bad request: ${message}` } });
    };
    const unicode = "invalid unicode code point";
    const control = "control character (\\u0000-\\u001F) found while parsing a string";
    const created = await call("/api/v1/namespaces", "POST", utf8('{"id":"wsp_utf"}'));
    assert.equal(created.status, 201);
    await bad("/api/v1/namespaces", "POST", utf8('{"id":"caf', 0xff, '"}'), `${parse}: id: ${unicode} at line 1 column 12`);
    await bad("/api/v1/namespaces", "POST", utf8('{"id":"caf', 0xff), `${parse}: id: EOF while parsing a string at line 1 column 11`);
    await bad("/api/v1/namespaces", "POST", utf8('{"id":"', 0xc3, '"}'), `${parse}: id: ${unicode} at line 1 column 9`);
    await bad("/api/v1/namespaces", "POST", utf8('{"id":"', 0xc0, 0x80, '"}'), `${parse}: id: ${unicode} at line 1 column 10`);
    await bad("/api/v1/namespaces", "POST", utf8('{"id":"', 0xc3, 0xa9, 0xff, '"}'), `${parse}: id: ${unicode} at line 1 column 11`);
    await bad("/api/v1/namespaces", "POST", utf8('{"id":"', 0xed, 0xa0, 0x80, '"}'), `${parse}: id: ${unicode} at line 1 column 11`);
    await bad("/api/v1/namespaces", "POST", utf8('{"id":"\\n', 0xff, '"}'), `${parse}: id: ${unicode} at line 1 column 11`);
    await bad("/api/v1/namespaces", "POST", utf8('{"id":"', 0xff, '"}'), `${parse}: id: ${unicode} at line 1 column 9`);
    await bad("/api/v1/namespaces", "POST", utf8('["', 0xff, '"]'), `${parse}: [0]: ${unicode} at line 1 column 4`);
    await bad("/api/v1/namespaces", "POST", utf8('{"', 0xff, '":1}'), `${parse}: ${unicode} at line 1 column 4`);
    await bad(
      "/api/v1/namespaces",
      "POST",
      utf8('{"id":"a","blocklist":["ok","', 0xff, '"]}'),
      `${parse}: blocklist[1]: ${unicode} at line 1 column 31`,
    );
    await bad(
      "/api/v1/namespaces",
      "POST",
      utf8('{"id":"a","attrs":{"k":"x', 0xff, '"}}'),
      `${parse}: attrs.k: ${unicode} at line 1 column 27`,
    );
    await bad(
      "/api/v1/namespaces",
      "POST",
      utf8('{"id":"a","attrs":{"', 0xff, '":1}}'),
      `${parse}: attrs.?: ${unicode} at line 1 column 22`,
    );
    await bad(
      "/api/v1/namespaces",
      "POST",
      utf8('{"id":"a",\n"attrs":"', 0xff, '"}'),
      `${parse}: attrs: ${unicode} at line 2 column 11`,
    );
    await bad("/api/v1/namespaces", "POST", utf8('{"id":"\\q', 0xff, '"}'), `${parse}: id: invalid escape at line 1 column 9`);
    await bad("/api/v1/namespaces", "POST", utf8('{"id":"', 0xff, 0x01, '"}'), `${parse}: id: ${control} at line 1 column 9`);
    await bad("/api/v1/namespaces", "POST", utf8('{"id":"\\u12', 0xff, 'Y"}'), `${parse}: id: invalid escape at line 1 column 13`);
    await bad(
      "/api/v1/namespaces",
      "POST",
      utf8('{"id":{"a":"', 0xff, '"}}'),
      `${data}: id: invalid type: map, expected a string at line 1 column 6`,
    );
    await bad(
      "/api/v1/namespaces",
      "POST",
      utf8('{"id":"a","nope":"', 0xff, '"}'),
      `${data}: nope: unknown field \`nope\`, expected one of \`id\`, \`attrs\`, \`blocklist\` at line 1 column 16`,
    );
    await bad("/api/v1/namespaces", "POST", utf8("{", 0xff, '"id":"a"}'), `${parse}: key must be a string at line 1 column 2`);
    await bad("/api/v1/namespaces", "POST", utf8('{"id":"a"', 0xff, "}"), `${parse}: expected \`,\` or \`}\` at line 1 column 10`);
    await bad("/api/v1/namespaces", "POST", utf8('{"id":"a"x}'), `${parse}: expected \`,\` or \`}\` at line 1 column 10`);
    await bad("/api/v1/namespaces", "POST", utf8('{"id":"a"}', 0xff), `${parse}: trailing characters at line 1 column 11`);
    await bad("/api/v1/namespaces", "POST", utf8(" ", 0xff), `${parse}: expected value at line 1 column 2`);
    await bad("/api/v1/namespaces", "POST", utf8(0xef, 0xbb, 0xbf, '{"id":"a"}'), `${parse}: expected value at line 1 column 1`);
    await bad("/api/v1/namespaces", "POST", utf8('{"id":', 0xff, "}"), `${parse}: id: expected value at line 1 column 7`);
    await bad("/api/v1/namespaces", "POST", utf8('{"id":x}'), `${parse}: id: expected value at line 1 column 7`);
    await bad(
      "/api/v1/namespaces",
      "POST",
      utf8('{"id":"', 0xc3, 0xa9, '"}'),
      "a namespace identifier contains a character outside ASCII letters, digits, `.`, `-`, and `_`",
    );
    await bad(
      "/api/v1/namespaces",
      "POST",
      utf8('{"id":"a","attrs":{"k":1', 0xff, "}}"),
      `${parse}: attrs.?: expected \`,\` or \`}\` at line 1 column 25`,
    );
    await bad(
      "/api/v1/namespaces",
      "POST",
      utf8('{"id":"a","attrs":[', 0xff, "]}"),
      `${parse}: attrs[0]: expected value at line 1 column 20`,
    );
    await bad(
      "/api/v1/namespaces",
      "POST",
      utf8('{"id":"a","attrs":[1', 0xff, "]}"),
      `${parse}: attrs: expected \`,\` or \`]\` at line 1 column 21`,
    );
    await bad(
      "/api/v1/namespaces/wsp_utf/budgets/2026-09",
      "PUT",
      utf8('{"limit_microdollars":1', 0xff, "}"),
      `${parse}: expected \`,\` or \`}\` at line 1 column 24`,
    );
    await bad(
      "/api/v1/namespaces/wsp_utf/budgets/2026-09",
      "PUT",
      utf8('{"limit_microdollars":', 0xff, "}"),
      `${parse}: limit_microdollars: expected value at line 1 column 23`,
    );
    await bad(
      "/api/v1/namespaces/wsp_utf/budget",
      "PUT",
      utf8('{"cadence":"', 0xff, '","limit_microdollars":1}'),
      `${parse}: cadence: ${unicode} at line 1 column 14`,
    );
    await bad(
      "/api/v1/namespaces/wsp_utf/budget",
      "PUT",
      utf8('{"cadence":{"monthly":', 0xff, '},"limit_microdollars":1}'),
      `${parse}: cadence.monthly: expected value at line 1 column 23`,
    );
    const stored = await call("/api/v1/namespaces/wsp_utf", "PUT", utf8('{"attrs":"', 0xef, 0xbf, 0xbd, '"}'));
    assert.equal(stored.status, 200);
    assert.equal((await stored.json()).attrs, "\uFFFD");
    const list = await call("/api/v1/namespaces", "GET");
    assert.equal(list.status, 200);
    const ids = ((await list.json()).data as Array<{ id: string }>).map((row) => row.id);
    assert.equal(ids.includes("wsp_utf"), true);
    assert.equal(ids.some((id) => id.includes("\uFFFD") || id.includes("caf")), false);
    const kept = await call("/api/v1/namespaces/wsp_utf", "GET");
    assert.equal(kept.status, 200);
    assert.equal((await kept.json()).attrs, "\uFFFD");
  } finally {
    upstream.close();
  }
});


test("management_json_attrs_match_serde_reserialization", async () => {
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
  const call = (path: string, method: string, body?: string) =>
    app.request(`http://127.0.0.1${path}`, { method, headers, ...(body === undefined ? {} : { body }) });
  const parse = "Failed to parse the request body as JSON";
  const created = await call("/api/v1/namespaces", "POST", '{"id":"wsp_ser","attrs":{"z":1,"a":2}}');
  assert.equal(created.status, 201);
  assert.equal(await created.text(), '{"id":"wsp_ser","attrs":{"a":2,"z":1}}');
  const put = async (body: string, attrs: string) => {
    const response = await call("/api/v1/namespaces/wsp_ser", "PUT", body);
    assert.equal(response.status, 200, body);
    const text = await response.text();
    assert.equal(text, `{"id":"wsp_ser","attrs":${attrs}}`, body);
    const again = await call("/api/v1/namespaces/wsp_ser", "GET");
    assert.equal(again.status, 200);
    assert.equal(await again.text(), text);
  };
  await put('{"attrs":{"n":9007199254740993}}', '{"n":9007199254740993}');
  await put('{"attrs":{"n":1.0}}', '{"n":1.0}');
  await put('{"attrs":{"n":1e2}}', '{"n":100.0}');
  await put('{"attrs":1.0}', "1.0");
  await put('{"attrs":9007199254740993}', "9007199254740993");
  await put('{"attrs":{"b":1,"a":{"z":1,"m":2}}}', '{"a":{"m":2,"z":1},"b":1}');
  await put('{"attrs":{"n":-0}}', '{"n":-0.0}');
  await put('{"attrs":{"n":18446744073709551615}}', '{"n":18446744073709551615}');
  await put('{"attrs":{"n":18446744073709551616}}', '{"n":1.8446744073709552e+19}');
  await put('{"attrs":{"n":-9223372036854775809}}', '{"n":-9.223372036854776e+18}');
  await put('{"attrs":{"n":1E21}}', '{"n":1e+21}');
  await put('{"attrs":{"k":"a","k":"b"}}', '{"k":"b"}');
  await put('{"attrs":{"é":1,"a":2}}', '{"a":2,"é":1}');
  await put('{"attrs":1e-400}', "0.0");
  await put('{"attrs":1e308}', "1e+308");
  await put('{"attrs":[1.0,9007199254740993]}', "[1.0,9007199254740993]");
  await put('{"attrs":{"\\uFFFD":1,"😀":2}}', '{"\uFFFD":1,"😀":2}');
  const bad = async (path: string, method: string, body: string, message: string) => {
    const response = await call(path, method, body);
    assert.equal(response.status, 400, body);
    assert.deepEqual(await response.json(), { error: { type: "bad_request", message: `bad request: ${message}` } });
  };
  await bad(
    "/api/v1/namespaces",
    "POST",
    '{"id":"a","attrs":1e309}',
    `${parse}: attrs: number out of range at line 1 column 23`,
  );
  await bad(
    "/api/v1/namespaces",
    "POST",
    '{"id":"a","attrs":{"n":1e309}}',
    `${parse}: attrs.n: number out of range at line 1 column 28`,
  );
  await bad(
    "/api/v1/namespaces",
    "POST",
    '{"id":"a","attrs":-1e309}',
    `${parse}: attrs: number out of range at line 1 column 24`,
  );
  await bad("/api/v1/namespaces", "POST", '{"id":1e309}', `${parse}: id: number out of range at line 1 column 11`);
  await bad(
    "/api/v1/namespaces/wsp_ser/budgets/2026-09",
    "PUT",
    '{"limit_microdollars":1e309}',
    `${parse}: limit_microdollars: number out of range at line 1 column 27`,
  );
  await bad(
    "/api/v1/namespaces/wsp_ser/budget",
    "PUT",
    '{"cadence":{"monthly":1e309},"limit_microdollars":1}',
    `${parse}: cadence.monthly: number out of range at line 1 column 27`,
  );
  const missing = await call("/api/v1/namespaces/a", "GET");
  assert.equal(missing.status, 404);
  const kept = await call("/api/v1/namespaces/wsp_ser", "GET");
  assert.equal(kept.status, 200);
  assert.equal((await kept.text()).includes('"attrs":{"\uFFFD":1,"😀":2}'), true);
});


test("management_json_trailing_characters_match_serde", async () => {
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
  const utf8 = (...parts: Array<string | number>) => {
    const out: number[] = [];
    for (const part of parts) {
      if (typeof part === "number") {
        out.push(part);
      } else {
        for (const byte of new TextEncoder().encode(part)) {
          out.push(byte);
        }
      }
    }
    return Uint8Array.from(out);
  };
  const call = (path: string, method: string, body?: Uint8Array) =>
    app.request(`http://127.0.0.1${path}`, { method, headers, ...(body === undefined ? {} : { body }) });
  const parse = "Failed to parse the request body as JSON: trailing characters";
  const created = await call("/api/v1/namespaces", "POST", utf8('{"id":"wsp_trail","attrs":{"z":1}} \n'));
  assert.equal(created.status, 201);
  assert.equal(await created.text(), '{"id":"wsp_trail","attrs":{"z":1}}');
  const bad = async (path: string, method: string, body: Uint8Array, message: string) => {
    const response = await call(path, method, body);
    assert.equal(response.status, 400);
    assert.deepEqual(await response.json(), { error: { type: "bad_request", message: `bad request: ${message}` } });
  };
  await bad("/api/v1/namespaces", "POST", utf8('{"id":"a"}x'), `${parse} at line 1 column 11`);
  await bad("/api/v1/namespaces", "POST", utf8('{"id":"a"}é'), `${parse} at line 1 column 11`);
  await bad("/api/v1/namespaces", "POST", utf8('{"id":"a"}€'), `${parse} at line 1 column 11`);
  await bad("/api/v1/namespaces", "POST", utf8('{"id":"a"}😀'), `${parse} at line 1 column 11`);
  await bad("/api/v1/namespaces", "POST", utf8('{"id":"a"} é'), `${parse} at line 1 column 12`);
  await bad("/api/v1/namespaces", "POST", utf8('{"id":"a"}', 0xc2, 0xa0), `${parse} at line 1 column 11`);
  await bad("/api/v1/namespaces", "POST", utf8('{"id":"a"}', 0xff), `${parse} at line 1 column 11`);
  await bad("/api/v1/namespaces", "POST", utf8('{"id":"a"}éx'), `${parse} at line 1 column 11`);
  await bad("/api/v1/namespaces", "POST", utf8('{"id":"a"}\nx'), `${parse} at line 2 column 1`);
  await bad("/api/v1/namespaces", "POST", utf8('{"id":"a"}\n é'), `${parse} at line 2 column 2`);
  await bad("/api/v1/namespaces", "POST", utf8('{"id":"a"}\r\nx'), `${parse} at line 2 column 1`);
  await bad("/api/v1/namespaces", "POST", utf8('["wsp_x", {}, []]é'), `${parse} at line 1 column 18`);
  await bad("/api/v1/namespaces/wsp_trail", "PUT", utf8("{}x"), `${parse} at line 1 column 3`);
  await bad("/api/v1/namespaces/wsp_trail", "PUT", utf8("{}é"), `${parse} at line 1 column 3`);
  await bad(
    "/api/v1/namespaces/wsp_trail/budgets/2026-09",
    "PUT",
    utf8('{"limit_microdollars":1}é'),
    `${parse} at line 1 column 25`,
  );
  await bad(
    "/api/v1/namespaces/wsp_trail/budget",
    "PUT",
    utf8('{"cadence":"monthly","limit_microdollars":1}😀'),
    `${parse} at line 1 column 45`,
  );
  const missing = await call("/api/v1/namespaces/a", "GET");
  assert.equal(missing.status, 404);
  const kept = await call("/api/v1/namespaces/wsp_trail", "GET");
  assert.equal(kept.status, 200);
  assert.equal(await kept.text(), '{"id":"wsp_trail","attrs":{"z":1}}');
  const budget = await call("/api/v1/namespaces/wsp_trail/budgets/2026-09", "GET");
  assert.equal(budget.status, 404);
});


test("usage_summary_matches_the_rust_grouping", async () => {
  const store = createMemoryStore();
  await store.putNamespace({
    id: "wsp_usage",
    attrs: {},
    blocklist: null,
    allowPlatformFallback: true,
    fromConfig: false,
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "wsp_usage",
  });
  const headers = { authorization: `Bearer ${KEY}` };
  const settle = (requestId: string, model: string, status: string, cost: bigint | null, period = "p") =>
    store.settle({
      requestId,
      namespace: "wsp_usage",
      period,
      model,
      status,
      cost,
      incarnation: 1n,
    });
  const half = 9223372036854775807n / 2n + 1n;
  await settle("r1", "b/m", "ok", 15n);
  await settle("r2", "b/m", "upstream_error", 1n);
  await settle("r3", "a/m", "ok", null);
  await settle("r4", "a/m", "ok", 7n);
  await settle("r1", "b/m", "ok", 99n);
  await settle("r5", "😀", "ok", 1n);
  await settle("r6", "\uFFFF", "ok", 2n);
  await settle("r7", "c/m", "ok", half);
  await settle("r8", "c/m", "ok", half);
  await settle("r9", "d/m", "ok", 9223372036854775807n + 1n);
  await settle("r10", "b/m", "ok", 99n, "other");
  const response = await app.request("http://127.0.0.1/api/v1/namespaces/wsp_usage/usage?period=p", { headers });
  assert.equal(response.status, 200);
  assert.equal(
    await response.text(),
    '{"namespace":"wsp_usage","period":"p","data":[' +
      '{"model":"a/m","status":"ok","count":2,"cost_microdollars":7},' +
      '{"model":"b/m","status":"ok","count":1,"cost_microdollars":15},' +
      '{"model":"b/m","status":"upstream_error","count":1,"cost_microdollars":1},' +
      '{"model":"c/m","status":"ok","count":2,"cost_microdollars":9223372036854775807},' +
      '{"model":"d/m","status":"ok","count":1,"cost_microdollars":9223372036854775807},' +
      '{"model":"\uFFFF","status":"ok","count":1,"cost_microdollars":2},' +
      '{"model":"😀","status":"ok","count":1,"cost_microdollars":1}]}',
  );
  const other = await app.request("http://127.0.0.1/api/v1/namespaces/wsp_usage/usage?period=other", { headers });
  assert.equal(other.status, 200);
  assert.equal(
    await other.text(),
    '{"namespace":"wsp_usage","period":"other","data":[{"model":"b/m","status":"ok","count":1,"cost_microdollars":99}]}',
  );
  const empty = await app.request("http://127.0.0.1/api/v1/namespaces/wsp_usage/usage?period=unused", { headers });
  assert.equal(empty.status, 200);
  assert.equal(await empty.text(), '{"namespace":"wsp_usage","period":"unused","data":[]}');
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
