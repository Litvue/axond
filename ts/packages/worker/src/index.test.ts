import assert from "node:assert/strict";
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
