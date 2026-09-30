import assert from "node:assert/strict";
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { createMemoryStore } from "../../gateway/src/memory-store.ts";

import { createHandler, discoverOnSchedule } from "./index.ts";

test("the worker handler is a static bundle of the gateway and an extension", async () => {
  const source = await readFile(new URL("./index.ts", import.meta.url), "utf8");
  const wrangler = await readFile(new URL("../wrangler.toml", import.meta.url), "utf8");
  assert.match(source, /from "@axond\/rate-limit"/);
  assert.match(source, /waitUntil/);
  assert.match(source, /discoverOnce/);
  assert.match(source, /scheduled/);
  assert.match(wrangler, /crons = \["\*\/5 \* \* \* \*"\]/);
  assert.equal(source.includes("node:"), false);
  const handler = createHandler({
    HYPERDRIVE: { connectionString: "postgres://example" },
    GATEWAY_KEY: "k",
    PROVIDERS_JSON: "[]",
  });
  assert.equal(typeof handler.fetch, "function");
  assert.equal(typeof handler.scheduled, "function");
});

test("scheduled discovery keeps the last catalogue when the provider is down", async () => {
  const store = createMemoryStore();
  await store.upsertProviderModels({
    provider: "fake-openai",
    fetchedAt: "2026-09-29T00:00:00Z",
    stale: false,
    data: [{ id: "gpt-test" }],
    source: "http://upstream",
  });
  let waited: Promise<unknown> = Promise.resolve();
  discoverOnSchedule(
    {
      HYPERDRIVE: { connectionString: "postgres://example" },
      GATEWAY_KEY: "k",
      PROVIDERS_JSON: JSON.stringify([{ id: "fake-openai", kind: "openai", baseUrl: "http://upstream" }]),
      CREDENTIALS_JSON: JSON.stringify([{ namespace: "platform", provider: "fake-openai", secret: "s", id: "s" }]),
      CATALOG_SOURCE: "models-dev",
      CATALOG_SOURCE_URL: "https://example.test/models.json",
    },
    store,
    {
      waitUntil(promise) {
        waited = promise;
      },
    },
    async () => {
      throw new Error("upstream down");
    },
  );
  await waited;
  const row = await store.getProviderModels("fake-openai");
  assert.equal(row?.stale, true);
  assert.deepEqual(row?.data, [{ id: "gpt-test" }]);
  const catalog = await store.getProviderModels("catalog");
  assert.equal(catalog?.stale, true);
  assert.deepEqual(catalog?.data, []);
});

test("worker_request_path_uses_credentials_json", async () => {
  const upstream = await new Promise<{ url: string; authorization: () => string; close: () => void }>((resolve) => {
    let authorization = "";
    const server = createServer((req, res) => {
      authorization = req.headers.authorization ?? "";
      req.resume();
      req.on("end", () => {
        res.writeHead(200, { "content-type": "application/json" });
        res.end('{"id":"chatcmpl-worker","choices":[{"message":{"role":"assistant","content":"ok"}}]}');
      });
    });
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        throw new Error("no port");
      }
      resolve({
        url: `http://127.0.0.1:${address.port}`,
        authorization: () => authorization,
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
  await store.putBudget("platform", "compat", 1_000_000n);
  const handler = createHandler(
    {
      HYPERDRIVE: { connectionString: "postgres://example" },
      GATEWAY_KEY: "k",
      PROVIDERS_JSON: JSON.stringify([
        { id: "fake-openai", kind: "openai", baseUrl: upstream.url, unpricedModels: "allow" },
      ]),
      CREDENTIALS_JSON: JSON.stringify([
        { namespace: "platform", provider: "fake-openai", secret: "sk-worker-secret", id: "plat" },
      ]),
    },
    store,
  );
  const wait = { waitUntil() {} };
  try {
    const listed = await handler.fetch(new Request("http://127.0.0.1/ns/platform/v1/credentials", {
      headers: { authorization: "Bearer k" },
    }), wait);
    const listedBody = await listed.text();
    assert.equal(listed.status, 200, listedBody);
    assert.equal(listedBody.includes("sk-worker-secret"), false);
    const rows = JSON.parse(listedBody) as { data: { credential_id?: string; source: string; state: string }[] };
    assert.equal(rows.data.length, 1);
    assert.equal(rows.data[0]?.credential_id, "plat");
    assert.equal(rows.data[0]?.source, "platform");
    assert.equal(rows.data[0]?.state, "healthy");
    const chat = await handler.fetch(new Request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: "Bearer k", "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: "hi" }] }),
    }), wait);
    const chatBody = await chat.text();
    assert.equal(chat.status, 200, chatBody);
    assert.equal(chatBody.includes("sk-worker-secret"), false);
    assert.equal(upstream.authorization(), "Bearer sk-worker-secret");
  } finally {
    upstream.close();
  }
});
