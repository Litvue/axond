import assert from "node:assert/strict";
import test from "node:test";

import { createMemoryStore } from "../../gateway/src/memory-store.ts";

import { discoverOnce } from "./discovery.ts";

test("discovery stores provider models and a catalogue document", async () => {
  const store = createMemoryStore();
  const calls: string[] = [];
  await discoverOnce({
    store,
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: "http://upstream" }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "s", id: "s" }],
    catalog: { source: "models-dev", sourceUrl: "https://example.test/models.json" },
    fetchImpl: async (url) => {
      calls.push(String(url));
      if (String(url).endsWith("/models")) {
        return new Response(JSON.stringify({ data: [{ id: "gpt-test" }] }), { status: 200 });
      }
      return new Response(JSON.stringify({ "openai/gpt-test": { id: "gpt-test" } }), { status: 200 });
    },
  });
  const provider = await store.getProviderModels("fake-openai");
  assert.equal(provider?.stale, false);
  assert.deepEqual(provider?.data, [{ id: "gpt-test" }]);
  const catalog = await store.getProviderModels("catalog");
  assert.deepEqual(catalog?.data, [{ id: "openai/gpt-test" }]);
  assert.deepEqual(calls, ["http://upstream/models", "https://example.test/models.json"]);
});

test("a failed discovery keeps the last catalogue and a fresh row rejects a different source", async () => {
  const store = createMemoryStore();
  await store.upsertProviderModels({
    provider: "fake-openai",
    fetchedAt: "2026-09-29T00:00:00Z",
    stale: false,
    data: [{ id: "gpt-test" }],
    source: "http://upstream",
  });
  await store.upsertProviderModels({
    provider: "fake-openai",
    fetchedAt: "2026-09-29T00:01:00Z",
    stale: false,
    data: [{ id: "other" }],
    source: "http://elsewhere",
  });
  const kept = await store.getProviderModels("fake-openai");
  assert.deepEqual(kept?.data, [{ id: "gpt-test" }]);
  await discoverOnce({
    store,
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: "http://upstream" }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "s", id: "s" }],
    catalog: { source: "none" },
    fetchImpl: async () => {
      throw new Error("upstream down");
    },
  });
  const stale = await store.getProviderModels("fake-openai");
  assert.equal(stale?.stale, true);
  assert.deepEqual(stale?.data, [{ id: "gpt-test" }]);
  assert.equal(stale?.source, "http://upstream");
  await store.upsertProviderModels({
    provider: "fake-openai",
    fetchedAt: "2026-09-29T00:02:00Z",
    stale: false,
    data: [{ id: "other" }],
    source: "http://elsewhere",
  });
  const replaced = await store.getProviderModels("fake-openai");
  assert.deepEqual(replaced?.data, [{ id: "other" }]);
  assert.equal(replaced?.stale, false);
});
