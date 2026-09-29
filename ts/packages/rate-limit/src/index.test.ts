import assert from "node:assert/strict";
import test from "node:test";

import { createAxond } from "@axond/gateway";
import { createMemoryStore } from "../../gateway/src/memory-store.ts";

import { rateLimitExtension, resetIsolateCounters } from "./index.ts";

test("isolate mode returns 429 after the configured limit", async () => {
  resetIsolateCounters();
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
    gatewayKey: "k",
    defaultNamespace: "platform",
    providers: [],
    extensions: [rateLimitExtension({ limit: 1, windowMs: 60_000 })],
  });
  const first = await app.request("http://127.0.0.1/ns/platform/v1/models", {
    headers: { authorization: "Bearer k" },
  });
  assert.equal(first.status, 200);
  const second = await app.request("http://127.0.0.1/ns/platform/v1/models", {
    headers: { authorization: "Bearer k" },
  });
  assert.equal(second.status, 429);
  assert.equal((await second.json()).error.type, "rate_limited");
});
