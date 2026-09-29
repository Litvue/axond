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
