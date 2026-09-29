import assert from "node:assert/strict";
import { createServer } from "node:http";
import test from "node:test";

import { createAxond } from "./app.ts";
import { createMemoryStore } from "./memory-store.ts";
import type { Store } from "@axond/sdk";

const KEY = "test-inbound-key";

async function seeded(): Promise<Store> {
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
    fromConfig: false,
  });
  await store.putBudget("platform", "compat", 1_000_000_000n);
  await store.putBudget("tenant", "compat", 1_000_000_000n);
  return store;
}

function listen(handler: (req: import("node:http").IncomingMessage, res: import("node:http").ServerResponse) => void) {
  const server = createServer(handler);
  return new Promise<{ url: string; close: () => void }>((resolve) => {
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
}

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

test("a cancelled stream still settles the delivered request", async () => {
  const store = await seeded();
  const upstream = await listen((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.write('data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n');
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
        inputMicrodollarsPerMillion: 2_500_000n,
        outputMicrodollarsPerMillion: 10_000_000n,
      },
    ],
  });
  const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
    body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, messages: [] }),
  });
  assert.equal(response.status, 200);
  const reader = response.body!.getReader();
  await reader.read();
  await reader.cancel();
  let rows: { count: number }[] = [];
  for (let attempt = 0; attempt < 20; attempt += 1) {
    rows = await store.summarizeUsage("platform", "compat");
    if (rows.length > 0) {
      break;
    }
    await new Promise((wake) => setTimeout(wake, 10));
  }
  assert.equal(rows.reduce((sum, row) => sum + row.count, 0), 1);
  upstream.close();
});

test("a provider 500 fails over to the next credential", async () => {
  const store = await seeded();
  const seen: string[] = [];
  const upstream = await listen((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (chunk: Buffer) => chunks.push(chunk));
    req.on("end", () => {
      const authorization = req.headers.authorization ?? "";
      seen.push(authorization);
      if (authorization.includes("bad-key")) {
        res.writeHead(500, { "content-type": "application/json" });
        res.end('{"error":{"message":"down"}}');
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
  const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
    body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [] }),
  });
  assert.equal(response.status, 200);
  assert.deepEqual(seen, ["Bearer bad-key", "Bearer good-key"]);
  upstream.close();
});

test("three provider failures park the credential on the replica status", async () => {
  const store = await seeded();
  const upstream = await listen((_req, res) => {
    res.writeHead(500, { "content-type": "application/json" });
    res.end('{"error":{"message":"PROMPT_SENTINEL"}}');
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
  for (let attempt = 0; attempt < 3; attempt += 1) {
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
  assert.match(body.error.message, /80ms/);
  upstream.close();
});

test("management routes match the compatibility contract", async () => {
  const store = await seeded();
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    configNamespaces: ["platform"],
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: "http://127.0.0.1:9" }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "upstream", id: "openai" }],
  });
  const auth = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
  assert.equal((await app.request("http://127.0.0.1/healthz")).status, 200);
  assert.equal(await (await app.request("http://127.0.0.1/readyz")).text(), "ready");
  const spec = await app.request("http://127.0.0.1/api/v1/openapi.json", { headers: auth });
  assert.equal(spec.status, 200);
  assert.equal((await spec.json()).openapi, "3.1.0");

  const created = await app.request("http://127.0.0.1/api/v1/namespaces", {
    method: "POST",
    headers: auth,
    body: JSON.stringify({ id: "extra", attrs: { team: "a" } }),
  });
  assert.equal(created.status, 201);
  const listed = await (await app.request("http://127.0.0.1/api/v1/namespaces", { headers: auth })).json();
  assert.equal(listed.data.map((row: { id: string }) => row.id).includes("extra"), true);
  const replaced = await app.request("http://127.0.0.1/api/v1/namespaces/extra", {
    method: "PUT",
    headers: auth,
    body: JSON.stringify({ attrs: { team: "b" }, blocklist: ["secret*"] }),
  });
  assert.equal(replaced.status, 200);
  const budget = await app.request("http://127.0.0.1/api/v1/namespaces/extra/budgets/compat", {
    method: "PUT",
    headers: auth,
    body: JSON.stringify({ limit_microdollars: 50 }),
  });
  assert.equal(budget.status, 200);
  assert.equal((await budget.json()).limit_microdollars, 50);
  const policy = await app.request("http://127.0.0.1/api/v1/namespaces/extra/budget", {
    method: "PUT",
    headers: auth,
    body: JSON.stringify({ cadence: "monthly", limit_microdollars: 80, timezone: "UTC" }),
  });
  assert.equal(policy.status, 200);
  const missingPeriod = await app.request("http://127.0.0.1/api/v1/namespaces/extra/usage", { headers: auth });
  assert.equal(missingPeriod.status, 400);
  const usage = await app.request("http://127.0.0.1/api/v1/namespaces/extra/usage?period=compat", { headers: auth });
  assert.equal(usage.status, 200);
  assert.deepEqual((await usage.json()).data, []);
  assert.equal((await app.request("http://127.0.0.1/api/v1/providers/models", { headers: auth })).status, 200);
  assert.equal((await app.request("http://127.0.0.1/api/v1/providers/fake-openai/models", { headers: auth })).status, 200);
  const credentials = await app.request("http://127.0.0.1/ns/platform/v1/credentials", { headers: auth });
  assert.equal(credentials.status, 200);
  assert.equal((await credentials.json()).data[0].credential_id, "openai");

  const removed = await app.request("http://127.0.0.1/api/v1/namespaces/extra", { method: "DELETE", headers: auth });
  assert.equal(removed.status, 204);
  const again = await app.request("http://127.0.0.1/api/v1/namespaces/extra", { method: "DELETE", headers: auth });
  assert.equal(again.status, 204);
  const configRow = await app.request("http://127.0.0.1/api/v1/namespaces/platform", { method: "DELETE", headers: auth });
  assert.equal(configRow.status, 409);
});
