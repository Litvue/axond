import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { createMemoryStore } from "../../gateway/src/memory-store.ts";
import { createMetrics } from "../../gateway/src/metrics.ts";

import { discoverOnce } from "./discovery.ts";
import type { ProviderDiscoveryLog } from "@axond/sdk";
import { openSqliteStore } from "./sqlite-store.ts";

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

test("a catalogue refusal names a bounded reason and omits the source url", async () => {
  const store = createMemoryStore();
  const metrics = createMetrics();
  const secret = "sk-live-secret";
  const catalog = { source: "models-dev" as const, sourceUrl: `https://example.test/${secret}.json` };
  await discoverOnce({
    store,
    providers: [],
    credentials: [],
    catalog,
    metrics,
    fetchImpl: async () => {
      throw new Error(secret);
    },
  });
  await discoverOnce({
    store,
    providers: [],
    credentials: [],
    catalog,
    metrics,
    fetchImpl: async () => new Response("no", { status: 401 }),
  });
  const reason = (name: string) =>
    metrics.points.find(
      (point) => point.name === "axond.catalog.refusals" && point.attributes["axond.catalog.reason"] === name,
    );
  assert.equal(reason("unreachable")?.value, 1);
  assert.equal(reason("denied")?.value, 1);
  assert.equal(metrics.points.find((point) => point.name === "axond.catalog.consecutive_refusals")?.value, 2);
  assert.equal(
    metrics.points.some((point) => point.name === "axond.catalog.active_age"),
    false,
  );
  assert.equal(JSON.stringify(metrics.points).includes(secret), false);

  await store.upsertProviderModels({
    provider: "catalog",
    fetchedAt: new Date(Date.now() - 5_000).toISOString(),
    stale: false,
    data: [{ id: "kept" }],
    source: "https://example.test/models.json",
  });
  await discoverOnce({
    store,
    providers: [],
    credentials: [],
    catalog: { source: "models-dev", sourceUrl: "https://example.test/models.json" },
    metrics,
    fetchImpl: async () => new Response("not-json", { status: 200 }),
  });
  const age = metrics.points.find((point) => point.name === "axond.catalog.active_age");
  assert.ok(age && age.value >= 4_000);
  assert.equal(reason("not_json")?.value, 1);
  assert.equal(metrics.points.find((point) => point.name === "axond.catalog.consecutive_refusals")?.value, 3);
  const kept = await store.getProviderModels("catalog");
  assert.equal(kept?.stale, true);
  assert.deepEqual(kept?.data, [{ id: "kept" }]);

  await discoverOnce({
    store,
    providers: [],
    credentials: [],
    catalog: { source: "models-dev", sourceUrl: "https://example.test/models.json" },
    metrics,
    fetchImpl: async () => new Response(JSON.stringify({ "openai/gpt-test": { id: "gpt-test" } }), { status: 200 }),
  });
  assert.equal(metrics.points.find((point) => point.name === "axond.catalog.consecutive_refusals")?.value, 0);
  const admittedAge = metrics.points.find((point) => point.name === "axond.catalog.active_age");
  assert.ok(admittedAge && admittedAge.value >= 0 && admittedAge.value < 5_000);
  const admitted = await store.getProviderModels("catalog");
  assert.equal(admitted?.stale, false);
  assert.deepEqual(admitted?.data, [{ id: "openai/gpt-test" }]);

  await discoverOnce({
    store,
    providers: [],
    credentials: [],
    catalog: { source: "models-dev", sourceUrl: "https://example.test/models.json" },
    metrics,
    fetchImpl: async () => new Response("missing", { status: 404 }),
  });
  await discoverOnce({
    store,
    providers: [],
    credentials: [],
    catalog: { source: "models-dev", sourceUrl: "https://example.test/models.json" },
    metrics,
    fetchImpl: async () => new Response("down", { status: 500 }),
  });
  assert.equal(reason("unsupported_endpoint")?.value, 1);
  assert.equal(reason("unreachable")?.value, 2);
  assert.equal(metrics.points.find((point) => point.name === "axond.catalog.consecutive_refusals")?.value, 2);
  assert.equal(JSON.stringify(metrics.points).includes(secret), false);
});

test("catalogue_import_log_names_the_reason_and_omits_the_url", async () => {
  const store = createMemoryStore();
  const logs: { msg: string; outcome: string; reason: string; consecutive_refusals: number }[] = [];
  const secret = "sk-live-secret";
  const sourceUrl = `https://example.test/${secret}.json`;
  await discoverOnce({
    store,
    providers: [],
    credentials: [],
    catalog: { source: "models-dev", sourceUrl },
    onLog: (record) => {
      logs.push(record);
    },
    fetchImpl: async () => {
      throw new Error(secret);
    },
  });
  await discoverOnce({
    store,
    providers: [],
    credentials: [],
    catalog: { source: "models-dev", sourceUrl },
    onLog: (record) => {
      logs.push(record);
    },
    fetchImpl: async () => new Response(JSON.stringify({ "openai/gpt-test": { id: "gpt-test" } }), { status: 200 }),
  });
  assert.equal(logs[0]!.msg, "catalogue_import");
  assert.equal(logs[0]!.outcome, "refused");
  assert.equal(logs[0]!.reason, "unreachable");
  assert.equal(logs[0]!.consecutive_refusals, 1);
  assert.equal(logs[1]!.outcome, "admitted");
  assert.equal(logs[1]!.reason, "");
  assert.equal(logs[1]!.consecutive_refusals, 0);
  const encoded = JSON.stringify(logs);
  assert.equal(encoded.includes(secret), false);
  assert.equal(encoded.includes(sourceUrl), false);
  assert.equal(encoded.includes("example.test"), false);
});

test("catalogue refusals survive a new store object on the same database", async () => {
  const dir = await mkdtemp(join(tmpdir(), "axond-streak-"));
  const path = join(dir, "axond.sqlite");
  const metrics = createMetrics();
  const catalog = { source: "models-dev" as const, sourceUrl: "https://example.test/models.json" };
  const fail = async () => {
    throw new Error("down");
  };
  try {
    await discoverOnce({
      store: openSqliteStore(path),
      providers: [],
      credentials: [],
      catalog,
      metrics,
      fetchImpl: fail,
    });
    await discoverOnce({
      store: openSqliteStore(path),
      providers: [],
      credentials: [],
      catalog,
      metrics,
      fetchImpl: async () => new Response("no", { status: 403 }),
    });
    assert.equal(metrics.points.find((point) => point.name === "axond.catalog.consecutive_refusals")?.value, 2);
    await discoverOnce({
      store: openSqliteStore(path),
      providers: [],
      credentials: [],
      catalog,
      metrics,
      fetchImpl: async () => new Response(JSON.stringify({ "openai/gpt-test": {} }), { status: 200 }),
    });
    assert.equal(metrics.points.find((point) => point.name === "axond.catalog.consecutive_refusals")?.value, 0);
    await discoverOnce({
      store: openSqliteStore(path),
      providers: [],
      credentials: [],
      catalog,
      metrics,
      fetchImpl: fail,
    });
    assert.equal(metrics.points.find((point) => point.name === "axond.catalog.consecutive_refusals")?.value, 1);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("provider_discovery_log_names_the_provider_and_omits_the_secret", async () => {
  const store = createMemoryStore();
  await store.upsertProviderModels({
    provider: "fake-openai",
    fetchedAt: "2026-09-29T00:00:00Z",
    stale: false,
    data: [{ id: "gpt-test" }],
    source: "http://upstream",
  });
  const secret = "sk-discovery-sentinel";
  const baseUrl = `http://127.0.0.1:9/${secret}`;
  const logs: ProviderDiscoveryLog[] = [];
  const note = (record: { msg: string }) => {
    if (record.msg === "provider_discovery") {
      logs.push(record as ProviderDiscoveryLog);
    }
  };
  let calls = 0;
  await discoverOnce({
    store,
    providers: [
      { id: "fake-openai", kind: "openai", baseUrl },
      { id: "missing-cred", kind: "openai", baseUrl },
    ],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret, id: "cred-1" }],
    catalog: { source: "none" },
    onLog: note,
    fetchImpl: async () => {
      calls += 1;
      throw new Error(`connect ${secret} ${baseUrl}`);
    },
  });
  assert.equal(calls, 1);
  assert.deepEqual(
    logs.map((line) => ({ provider: line.provider, reason: line.reason })),
    [
      { provider: "fake-openai", reason: "unreachable" },
      { provider: "missing-cred", reason: "no_credential" },
    ],
  );
  const stale = await store.getProviderModels("fake-openai");
  assert.equal(stale?.stale, true);
  assert.deepEqual(stale?.data, [{ id: "gpt-test" }]);
  assert.equal(await store.getProviderModels("missing-cred"), null);
  assert.equal(JSON.stringify(logs).includes(secret), false);
  assert.equal(JSON.stringify(logs).includes("127.0.0.1"), false);

  logs.length = 0;
  await discoverOnce({
    store,
    providers: [{ id: "fake-openai", kind: "openai", baseUrl }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret, id: "cred-1" }],
    catalog: { source: "none" },
    onLog: note,
    fetchImpl: async () => new Response(secret, { status: 401 }),
  });
  assert.equal(logs.length, 1);
  assert.equal(logs[0]?.reason, "denied");
  assert.equal(JSON.stringify(logs).includes(secret), false);

  logs.length = 0;
  await discoverOnce({
    store,
    providers: [{ id: "fake-openai", kind: "openai", baseUrl }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret, id: "cred-1" }],
    catalog: { source: "none" },
    onLog: note,
    fetchImpl: async () => new Response(secret, { status: 200 }),
  });
  assert.equal(logs.length, 1);
  assert.equal(logs[0]?.reason, "not_json");
  assert.equal(JSON.stringify(logs).includes(secret), false);
  const kept = await store.getProviderModels("fake-openai");
  assert.equal(kept?.stale, true);
  assert.deepEqual(kept?.data, [{ id: "gpt-test" }]);

  const retained: ProviderDiscoveryLog[] = [];
  const base = createMemoryStore();
  await discoverOnce({
    store: {
      ...base,
      upsertProviderModels: async () => {
        throw new Error(`dsn postgres://axond:${secret}@127.0.0.1/axond`);
      },
    },
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: "http://upstream" }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret, id: "cred-1" }],
    catalog: { source: "none" },
    onLog: (record) => {
      if (record.msg === "provider_discovery") {
        retained.push(record);
      }
    },
    fetchImpl: async () => new Response(JSON.stringify({ data: [{ id: "gpt-test" }] }), { status: 200 }),
  });
  assert.deepEqual(
    retained.map((line) => line.reason),
    ["not_retained"],
  );
  const encoded = JSON.stringify(retained);
  assert.equal(encoded.includes(secret), false);
  assert.equal(encoded.includes("postgres"), false);
  assert.equal(encoded.includes("127.0.0.1"), false);

  logs.length = 0;
  await discoverOnce({
    store,
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: "http://upstream" }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret, id: "cred-1" }],
    catalog: { source: "none" },
    onLog: note,
    fetchImpl: async () => new Response(JSON.stringify({ data: [{ id: "gpt-test" }] }), { status: 200 }),
  });
  assert.equal(logs.length, 0);
  const fresh = await store.getProviderModels("fake-openai");
  assert.equal(fresh?.stale, false);
  assert.deepEqual(fresh?.data, [{ id: "gpt-test" }]);
});
