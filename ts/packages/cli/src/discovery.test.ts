import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:http";
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
  const secret = "sk-discovery-sentinel";
  const baseUrl = `http://127.0.0.1:9/${secret}`;
  await store.upsertProviderModels({
    provider: "fake-openai",
    fetchedAt: "2026-09-29T00:00:00Z",
    stale: false,
    data: [{ id: "gpt-test" }],
    source: baseUrl,
  });
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

test("provider_listing_walks_the_pool_and_omits_the_secret", async () => {
  const secret = "sk-listing-sentinel";
  const store = createMemoryStore();
  await store.upsertProviderModels({
    provider: "fake-openai",
    fetchedAt: "2026-09-29T00:00:00Z",
    stale: false,
    data: [{ id: "old" }],
    source: "http://old.example/v1",
  });
  const logs: ProviderDiscoveryLog[] = [];
  const urls: string[] = [];
  const keys: string[] = [];
  await discoverOnce({
    store,
    replaceForeignSource: true,
    providers: [{ id: "fake-openai", kind: "anthropic", baseUrl: "http://upstream" }],
    credentials: [
      { namespace: "zeta", provider: "fake-openai", secret: "tenant-zeta", id: "zeta" },
      { namespace: "platform", provider: "fake-openai", secret, id: "bad" },
      { namespace: "platform", provider: "fake-openai", secret: "sk-second", id: "good" },
      { namespace: "alpha", provider: "fake-openai", secret: "tenant-alpha", id: "alpha" },
    ],
    catalog: { source: "none" },
    onLog: (record) => {
      if (record.msg === "provider_discovery") {
        logs.push(record);
      }
    },
    fetchImpl: async (url, init) => {
      urls.push(String(url));
      const key = new Headers(init?.headers).get("x-api-key") ?? "";
      keys.push(key);
      if (key === secret) {
        return new Response(secret, { status: 401 });
      }
      if (!String(url).includes("after_id")) {
        return new Response(
          JSON.stringify({
            data: [{ id: "claude-3" }, { object: "model" }, { id: "" }],
            has_more: true,
            last_id: "claude-3 opus",
          }),
          { status: 200 },
        );
      }
      return new Response(JSON.stringify({ data: [{ id: "claude-4" }, { id: 4 }], has_more: false }), { status: 200 });
    },
  });
  assert.deepEqual(logs, []);
  assert.deepEqual(keys, [secret, "sk-second", "sk-second"]);
  assert.deepEqual(urls, [
    "http://upstream/models",
    "http://upstream/models",
    "http://upstream/models?after_id=claude-3%20opus",
  ]);
  const replaced = await store.getProviderModels("fake-openai");
  assert.equal(replaced?.stale, false);
  assert.equal(replaced?.source, "http://upstream");
  assert.deepEqual(replaced?.data, [{ id: "claude-3" }, { id: "" }, { id: "claude-4" }]);

  urls.length = 0;
  logs.length = 0;
  await store.markProviderModelsStale("fake-openai");
  await store.upsertProviderModels({
    provider: "fake-openai",
    fetchedAt: "2026-09-29T00:01:00Z",
    stale: false,
    data: [{ id: "kept" }],
    source: "http://foreign.example/v1",
  });
  await discoverOnce({
    store,
    replaceForeignSource: false,
    providers: [{ id: "fake-openai", kind: "anthropic", baseUrl: "http://upstream" }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret, id: "bad" }],
    catalog: { source: "none" },
    onLog: (record) => {
      if (record.msg === "provider_discovery") {
        logs.push(record);
      }
    },
    fetchImpl: async (url) => {
      urls.push(String(url));
      return new Response(JSON.stringify({ data: [{ id: "should-not-store" }] }), { status: 200 });
    },
  });
  assert.deepEqual(urls, []);
  assert.deepEqual(logs, []);
  const kept = await store.getProviderModels("fake-openai");
  assert.equal(kept?.stale, false);
  assert.deepEqual(kept?.data, [{ id: "kept" }]);
  assert.equal(kept?.source, "http://foreign.example/v1");

  logs.length = 0;
  let pages = 0;
  await discoverOnce({
    store,
    replaceForeignSource: true,
    providers: [{ id: "fake-openai", kind: "anthropic", baseUrl: "http://foreign.example/v1" }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret, id: "bad" }],
    catalog: { source: "none" },
    onLog: (record) => {
      if (record.msg === "provider_discovery") {
        logs.push(record);
      }
    },
    fetchImpl: async () => {
      pages += 1;
      return new Response(JSON.stringify({ data: [{ id: `m${pages}` }], has_more: true, last_id: `m${pages}` }), {
        status: 200,
      });
    },
  });
  assert.equal(pages, 20);
  assert.deepEqual(
    logs.map((line) => line.reason),
    ["page_bound"],
  );
  const bounded = await store.getProviderModels("fake-openai");
  assert.equal(bounded?.stale, true);
  assert.deepEqual(bounded?.data, [{ id: "kept" }]);
  assert.equal(JSON.stringify(logs).includes(secret), false);

  logs.length = 0;
  await discoverOnce({
    store,
    replaceForeignSource: true,
    providers: [{ id: "tenant-only", kind: "openai", baseUrl: "http://tenant" }],
    credentials: [
      { namespace: "zeta", provider: "tenant-only", secret: "zeta-secret", id: "zeta" },
      { namespace: "alpha", provider: "tenant-only", secret: "alpha-secret", id: "alpha" },
    ],
    catalog: { source: "none" },
    onLog: (record) => {
      if (record.msg === "provider_discovery") {
        logs.push(record);
      }
    },
    fetchImpl: async (_url, init) => {
      const key = new Headers(init?.headers).get("authorization") ?? "";
      assert.equal(key, "Bearer alpha-secret");
      return new Response(JSON.stringify({ data: [{ id: "t" }, { name: "drop" }] }), { status: 200 });
    },
  });
  assert.deepEqual(logs, []);
  assert.deepEqual((await store.getProviderModels("tenant-only"))?.data, [{ id: "t" }]);

  logs.length = 0;
  await discoverOnce({
    store,
    replaceForeignSource: true,
    providers: [{ id: "broken-page", kind: "anthropic", baseUrl: `http://upstream/${secret}` }],
    credentials: [{ namespace: "platform", provider: "broken-page", secret, id: "bad" }],
    catalog: { source: "none" },
    onLog: (record) => {
      if (record.msg === "provider_discovery") {
        logs.push(record);
      }
    },
    fetchImpl: async () => new Response(JSON.stringify({ data: [{ id: "x" }], has_more: true }), { status: 200 }),
  });
  assert.deepEqual(
    logs.map((line) => line.reason),
    ["not_json"],
  );
  assert.equal(JSON.stringify(logs).includes(secret), false);
  assert.equal(JSON.stringify(logs).includes("upstream"), false);
  const broken = await store.getProviderModels("broken-page");
  assert.equal(broken?.stale, true);
  assert.deepEqual(broken?.data, []);
});

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

test("catalogue_and_listing_redirects_are_not_followed_and_omit_the_secret", async () => {
  const secret = "sk-redirect-sentinel";
  let targetHits = 0;
  const target = await listen((_req, res) => {
    targetHits += 1;
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ data: [{ id: secret }] }));
  });
  const moved = await listen((_req, res) => {
    res.writeHead(302, { location: `${target.url}/landed/${secret}` });
    res.end();
  });
  const store = createMemoryStore();
  await store.upsertProviderModels({
    provider: "catalog",
    fetchedAt: "2026-09-29T00:00:00Z",
    stale: false,
    data: [{ id: "kept-catalog" }],
    source: `${moved.url}/catalog.json`,
  });
  await store.upsertProviderModels({
    provider: "fake-openai",
    fetchedAt: "2026-09-29T00:00:00Z",
    stale: false,
    data: [{ id: "kept-models" }],
    source: moved.url,
  });
  const logs: Array<{ msg: string; reason?: string; outcome?: string }> = [];
  try {
    await discoverOnce({
      store,
      replaceForeignSource: true,
      providers: [{ id: "fake-openai", kind: "openai", baseUrl: moved.url }],
      credentials: [{ namespace: "platform", provider: "fake-openai", secret, id: "one" }],
      catalog: { source: "models-dev", sourceUrl: `${moved.url}/catalog.json` },
      onLog: (record) => {
        logs.push(record);
      },
    });
    assert.equal(targetHits, 0);
    assert.deepEqual(
      logs.map((line) => (line.msg === "provider_discovery" ? line.reason : `${line.outcome}:${line.reason}`)),
      ["unreachable", "refused:unsupported_endpoint"],
    );
    const models = await store.getProviderModels("fake-openai");
    assert.equal(models?.stale, true);
    assert.deepEqual(models?.data, [{ id: "kept-models" }]);
    assert.equal(models?.source, moved.url);
    const catalog = await store.getProviderModels("catalog");
    assert.equal(catalog?.stale, true);
    assert.deepEqual(catalog?.data, [{ id: "kept-catalog" }]);
    assert.equal(catalog?.source, `${moved.url}/catalog.json`);
    const encoded = JSON.stringify(logs);
    assert.equal(encoded.includes(secret), false);
    assert.equal(encoded.includes(target.url), false);
  } finally {
    moved.close();
    target.close();
  }
});

test("discovery treats a provider body that never arrives as unreachable", async () => {
  const server = createServer((req, res) => {
    req.resume();
    res.writeHead(200, { "content-type": "application/json" });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
  const address = server.address();
  if (!address || typeof address === "string") {
    server.close();
    throw new Error("no discovery port");
  }
  const store = createMemoryStore();
  const logs: ProviderDiscoveryLog[] = [];
  const secret = "discovery-body-secret";
  const started = Date.now();
  try {
    await Promise.race([
      discoverOnce({
        store,
        providers: [{ id: "fake-openai", kind: "openai", baseUrl: `http://127.0.0.1:${address.port}` }],
        credentials: [{ namespace: "platform", provider: "fake-openai", secret, id: "one" }],
        catalog: { source: "none" },
        onLog: (record) => {
          if (record.msg === "provider_discovery") {
            logs.push(record);
          }
        },
      }),
      new Promise<never>((_resolve, reject) => setTimeout(() => reject(new Error("discovery hung")), 35_000)),
    ]);
  } finally {
    server.closeAllConnections();
    server.close();
  }
  assert.equal(logs.length, 1);
  assert.equal(logs[0]?.reason, "unreachable");
  assert.equal(JSON.stringify(logs).includes(secret), false);
  const elapsed = Date.now() - started;
  assert.equal(elapsed >= 25_000, true);
  assert.equal(elapsed < 35_000, true);
});
