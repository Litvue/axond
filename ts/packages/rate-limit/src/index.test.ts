import assert from "node:assert/strict";
import test from "node:test";

import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createAxond } from "@axond/gateway";
import { applyMigration, openSqliteStore } from "../../cli/src/sqlite-store.ts";
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

test("store mode saturates one namespace and leaves another its capacity", async () => {
  const dir = await mkdtemp(join(tmpdir(), "axond-rl-"));
  const path = join(dir, "axond.sqlite");
  try {
    const extension = rateLimitExtension({ limit: 1, windowMs: 60_000, mode: "store" });
    applyMigration(path, "ratelimit:0", extension.migrations![0]!);
    const store = openSqliteStore(path);
    for (const id of ["platform", "tenant"]) {
      await store.putNamespace({
        id,
        attrs: {},
        blocklist: null,
        allowPlatformFallback: false,
        fromConfig: true,
      });
    }
    const app = createAxond({
      store,
      gatewayKey: "k",
      defaultNamespace: "platform",
      providers: [],
      extensions: [extension],
    });
    const call = (namespace: string) =>
      app.request(`http://127.0.0.1/ns/${namespace}/v1/models`, { headers: { authorization: "Bearer k" } });
    assert.equal((await call("platform")).status, 200);
    assert.equal((await call("platform")).status, 429);
    assert.equal((await call("tenant")).status, 200);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
