import assert from "node:assert/strict";
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import test from "node:test";

import pg from "pg";

import { createMemoryStore } from "../../gateway/src/memory-store.ts";

import { StoreFailure } from "../../gateway/src/errors.ts";

import { createHandler, discoverOnSchedule, handlerFor, rethrowSchemaFailure, schemaAttempt } from "./index.ts";

test("worker_schema_attempt_retries_after_rejection", async () => {
  const gate = schemaAttempt();
  let calls = 0;
  await assert.rejects(
    () => gate.run(async () => {
      calls += 1;
      throw new Error("pool");
    }),
    /pool/,
  );
  let release = (): void => {};
  const started = new Promise<void>((resolve) => {
    release = resolve;
  });
  const first = gate.run(async () => {
    calls += 1;
    await started;
  });
  const second = gate.run(async () => {
    calls += 1;
  });
  assert.equal(calls, 2);
  release();
  await Promise.all([first, second]);
  await gate.run(async () => {
    calls += 1;
  });
  assert.equal(calls, 2);
});

test("a rejected Hyperdrive login closes the client and hides the password", async () => {
  const original = pg.Client.prototype.end;
  let ended = 0;
  pg.Client.prototype.end = function (this: pg.Client, callback?: () => void) {
    ended += 1;
    return original.call(this, callback);
  };
  try {
    const handler = createHandler({
      HYPERDRIVE: { connectionString: "postgres://axond:socket-secret@127.0.0.1:1/axond" },
      GATEWAY_KEY: "k",
      PROVIDERS_JSON: "[]",
    });
    const response = await handler.fetch(
      new Request("http://127.0.0.1/api/v1/namespaces", { headers: { authorization: "Bearer k" } }),
      { waitUntil() {} },
    );
    const body = await response.text();
    assert.equal(response.status, 503, body);
    assert.equal(body.includes("socket-secret"), false);
    assert.equal(body.includes("ECONNREFUSED"), false);
    assert.equal(ended, 1);
  } finally {
    pg.Client.prototype.end = original;
  }
});

test("worker_schema_failure_logs_a_missing_column_and_hides_the_driver", () => {
  const lines: string[] = [];
  const original = console.log;
  console.log = (...args: unknown[]) => {
    lines.push(args.map((part) => String(part)).join(" "));
  };
  try {
    assert.throws(
      () => rethrowSchemaFailure(new Error("postgres schema is missing axond_namespace.allow_platform_fallback, axond_namespace.from_config")),
      (error: unknown) => error instanceof StoreFailure && error.message === "store is unavailable",
    );
    assert.equal(
      lines.includes(
        JSON.stringify({
          msg: "schema_unavailable",
          detail: "postgres schema is missing axond_namespace.allow_platform_fallback, axond_namespace.from_config",
        }),
      ),
      true,
    );
    lines.length = 0;
    assert.throws(
      () => rethrowSchemaFailure(new Error("password=secret")),
      (error: unknown) => error instanceof StoreFailure,
    );
    assert.equal(lines.join("\n").includes("password=secret"), false);
    assert.equal(lines.join("\n").includes("schema_unavailable"), false);
  } finally {
    console.log = original;
  }
});

test("the worker handler is a static bundle of the gateway and an extension", async () => {
  const source = await readFile(new URL("./index.ts", import.meta.url), "utf8");
  const wrangler = await readFile(new URL("../wrangler.toml", import.meta.url), "utf8");
  assert.match(source, /from "@axond\/rate-limit"/);
  assert.match(source, /holdPgClient\(new Client/);
  assert.match(source, /connectionTimeoutMillis: 15_000/);
  assert.match(source, /query_timeout: 60_000/);
  assert.match(source, /closePgClient\(client\)/);
  assert.match(source, /waitUntil/);
  assert.match(source, /discoverOnce/);
  assert.match(source, /scheduled/);
  assert.match(wrangler, /crons = \["\*\/5 \* \* \* \*"\]/);
  assert.match(wrangler, /https:\/\/models\.dev\/catalog\.json/);
  assert.equal(wrangler.includes("api.json"), false);
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
      CATALOG_SOURCE_URL: "https://example.test/catalog.json",
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

test("a worker catalogue url other than catalog.json is not fetched", async () => {
  const store = createMemoryStore();
  let fetched = 0;
  const lines: string[] = [];
  const original = console.log;
  let waited: Promise<unknown> = Promise.resolve();
  console.log = (...args: unknown[]) => {
    lines.push(args.map((part) => String(part)).join(" "));
  };
  try {
    discoverOnSchedule(
      {
        HYPERDRIVE: { connectionString: "postgres://example" },
        GATEWAY_KEY: "k",
        PROVIDERS_JSON: "[]",
        CATALOG_SOURCE: "models-dev",
        CATALOG_SOURCE_URL: "https://models.dev/api.json",
      },
      store,
      {
        waitUntil(promise) {
          waited = promise;
        },
      },
      async () => {
        fetched += 1;
        return new Response(JSON.stringify({ openai: { id: "gpt-test" } }), { status: 200 });
      },
    );
  } finally {
    console.log = original;
  }
  await waited;
  assert.equal(fetched, 0);
  const body = lines.join("\n");
  assert.equal(body.includes("unsupported_endpoint"), true);
  assert.equal(body.includes("api.json"), false);
  assert.equal(body.includes("models.dev"), false);
  assert.equal(await store.getProviderModels("catalog"), null);
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

test("worker_credential_pool_survives_the_next_request", async () => {
  let hits = 0;
  const upstream = await listen((req, res) => {
    hits += 1;
    req.resume();
    req.on("end", () => {
      res.writeHead(429, { "content-type": "application/json" });
      res.end('{"error":{"message":"slow down"}}');
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
  const env = {
    HYPERDRIVE: { connectionString: "postgres://example" },
    GATEWAY_KEY: "k",
    PROVIDERS_JSON: JSON.stringify([
      { id: "fake-openai", kind: "openai", baseUrl: upstream.url, unpricedModels: "allow" },
    ]),
    CREDENTIALS_JSON: JSON.stringify([
      { namespace: "platform", provider: "fake-openai", secret: "sk-worker-secret", id: "plat" },
    ]),
  };
  const handler = createHandler(env, store);
  const wait = { waitUntil() {} };
  const chat = () =>
    handler.fetch(
      new Request("http://127.0.0.1/ns/platform/v1/chat/completions", {
        method: "POST",
        headers: { authorization: "Bearer k", "content-type": "application/json" },
        body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: "hi" }] }),
      }),
      wait,
    );
  try {
    const first = await chat();
    assert.equal(first.status, 502, await first.text());
    const second = await chat();
    assert.equal(second.status, 502, await second.text());
    const listed = await handler.fetch(
      new Request("http://127.0.0.1/ns/platform/v1/credentials", { headers: { authorization: "Bearer k" } }),
      wait,
    );
    const listedBody = await listed.text();
    assert.equal(listed.status, 200, listedBody);
    assert.equal(listedBody.includes("sk-worker-secret"), false);
    const rows = JSON.parse(listedBody) as { data: { credential_id?: string; state: string }[] };
    assert.equal(rows.data[0]?.credential_id, "plat");
    assert.equal(rows.data[0]?.state, "parked");
    assert.equal(hits, 2);
    const fresh = createHandler(env, store);
    const again = await fresh.fetch(
      new Request("http://127.0.0.1/ns/platform/v1/credentials", { headers: { authorization: "Bearer k" } }),
      wait,
    );
    const againBody = await again.text();
    const freshRows = JSON.parse(againBody) as { data: { state: string }[] };
    assert.equal(freshRows.data[0]?.state, "healthy");
  } finally {
    upstream.close();
  }
});

test("worker_wait_until_stays_on_the_request", async () => {
  let started = 0;
  let release = () => {};
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  let both = () => {};
  const opened = new Promise<void>((resolve) => {
    both = resolve;
  });
  const upstream = await listen((req, res) => {
    started += 1;
    if (started === 2) {
      both();
    }
    req.resume();
    void gate.then(() => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end('{"id":"chatcmpl-worker","choices":[{"message":{"role":"assistant","content":"ok"}}]}');
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
  const buckets: Promise<unknown>[][] = [[], []];
  const ctx = (index: number) => ({
    waitUntil(promise: Promise<unknown>) {
      buckets[index]!.push(promise);
    },
  });
  const request = () =>
    new Request("http://127.0.0.1/ns/platform/v1/chat/completions", {
      method: "POST",
      headers: { authorization: "Bearer k", "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: "hi" }] }),
    });
  try {
    const first = handler.fetch(request(), ctx(0));
    const second = handler.fetch(request(), ctx(1));
    await opened;
    release();
    const [one, two] = await Promise.all([first, second]);
    assert.equal(one.status, 200, await one.text());
    assert.equal(two.status, 200, await two.text());
    assert.equal(buckets[0]!.length, 1);
    assert.equal(buckets[1]!.length, 1);
    await Promise.all([...buckets[0]!, ...buckets[1]!]);
  } finally {
    upstream.close();
  }
});

test("worker_handler_for_reuses_one_gateway", () => {
  const env = {
    HYPERDRIVE: { connectionString: "postgres://example" },
    GATEWAY_KEY: "k",
    PROVIDERS_JSON: "[]",
  };
  assert.equal(handlerFor(env), handlerFor({ ...env }));
  assert.notEqual(handlerFor(env), handlerFor({ ...env, CREDENTIALS_JSON: "[]" }));
  assert.notEqual(handlerFor(env), handlerFor({ ...env, PRICES_JSON: "[]" }));
});

test("worker_price_charges_one_request_id_once", async () => {
  const upstream = await listen((req, res) => {
    req.resume();
    req.on("end", () => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
        '{"id":"chatcmpl-worker","choices":[{"message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}',
      );
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
      PRICES_JSON: JSON.stringify([
        {
          provider: "fake-openai",
          model: "*",
          inputMicrodollarsPerMillion: 1_000_000,
          outputMicrodollarsPerMillion: 1_000_000,
        },
      ]),
    },
    store,
  );
  const pending: Promise<unknown>[] = [];
  const wait = {
    waitUntil(promise: Promise<unknown>) {
      pending.push(promise);
    },
  };
  const chat = () =>
    handler.fetch(
      new Request("http://127.0.0.1/ns/platform/v1/chat/completions", {
        method: "POST",
        headers: {
          authorization: "Bearer k",
          "content-type": "application/json",
          "x-request-id": "worker-price-once",
        },
        body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: "hi" }] }),
      }),
      wait,
    );
  const lines: string[] = [];
  const original = console.log;
  console.log = (...args: unknown[]) => {
    lines.push(args.map((part) => String(part)).join(" "));
  };
  try {
    const first = await chat();
    assert.equal(first.status, 200, await first.text());
    const second = await chat();
    assert.equal(second.status, 200, await second.text());
    await Promise.all(pending);
    const budget = await store.getBudget("platform", "compat");
    assert.equal(budget?.spent, 2n);
    const summary = await store.summarizeUsage("platform", "compat");
    assert.equal(summary.length, 1);
    assert.equal(summary[0]?.count, 1);
    assert.equal(summary[0]?.cost_microdollars, 2);
    const usage = lines
      .map((line) => {
        try {
          return JSON.parse(line) as { schema_version?: number; request_id?: string; input_tokens?: unknown; output_tokens?: unknown; cost_microdollars?: unknown; period?: unknown };
        } catch {
          return null;
        }
      })
      .filter((row) => row?.schema_version === 2 && row.request_id === "worker-price-once");
    assert.equal(usage.length, 2);
    assert.equal(usage[0]?.input_tokens, 1);
    assert.equal(usage[0]?.output_tokens, 1);
    assert.equal(usage[0]?.cost_microdollars, 2);
    assert.equal(usage[0]?.period, "compat");
    assert.equal(JSON.stringify(usage[0]).includes('"input_tokens":"'), false);
  } finally {
    console.log = original;
    upstream.close();
  }
});

test("worker_template_enables_request_signal", async () => {
  const toml = await readFile(new URL("../wrangler.toml", import.meta.url), "utf8");
  assert.match(toml, /enable_request_signal/);
});

test("worker_hyperdrive_create_comment_keeps_sslmode_out_of_the_url", async () => {
  const toml = await readFile(new URL("../wrangler.toml", import.meta.url), "utf8");
  const command = toml
    .split("\n")
    .filter((line) => line.includes("connection-string") || line.includes("caching-disabled"))
    .join("\n");
  assert.match(command, /--caching-disabled/);
  assert.equal(command.includes("sslmode="), false);
  assert.match(command, /:5432\//);
});

test("worker_request_abort_settles_client_cancelled", async () => {
  const upstream = await listen((req, res) => {
    req.resume();
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.write('data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n');
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
      PRICES_JSON: JSON.stringify([
        {
          provider: "fake-openai",
          model: "*",
          inputMicrodollarsPerMillion: 1_000_000,
          outputMicrodollarsPerMillion: 1_000_000,
        },
      ]),
    },
    store,
  );
  const pending: Promise<unknown>[] = [];
  const wait = {
    waitUntil(promise: Promise<unknown>) {
      pending.push(promise);
    },
  };
  const controller = new AbortController();
  try {
    const response = await handler.fetch(
      new Request("http://127.0.0.1/ns/platform/v1/chat/completions", {
        method: "POST",
        headers: { authorization: "Bearer k", "content-type": "application/json" },
        body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, messages: [{ role: "user", content: "hi" }] }),
        signal: controller.signal,
      }),
      wait,
    );
    assert.equal(response.status, 200);
    const reader = response.body!.getReader();
    const first = await reader.read();
    assert.equal(first.done, false);
    controller.abort();
    await Promise.all(pending);
    const summary = await store.summarizeUsage("platform", "compat");
    assert.equal(summary.length, 1);
    assert.equal(summary[0]?.status, "client_cancelled");
    assert.equal(Number(summary[0]?.cost_microdollars) > 0, true);
    assert.equal(JSON.stringify(summary).includes("sk-worker-secret"), false);
  } finally {
    upstream.close();
  }
});

test("worker_response_cancel_settles_client_cancelled", async () => {
  const upstream = await listen((req, res) => {
    req.resume();
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.write('data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n');
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
      PRICES_JSON: JSON.stringify([
        {
          provider: "fake-openai",
          model: "*",
          inputMicrodollarsPerMillion: 1_000_000,
          outputMicrodollarsPerMillion: 1_000_000,
        },
      ]),
    },
    store,
  );
  const pending: Promise<unknown>[] = [];
  const wait = {
    waitUntil(promise: Promise<unknown>) {
      pending.push(promise);
    },
  };
  try {
    const response = await handler.fetch(
      new Request("http://127.0.0.1/ns/platform/v1/chat/completions", {
        method: "POST",
        headers: { authorization: "Bearer k", "content-type": "application/json" },
        body: JSON.stringify({ model: "fake-openai/gpt-test", stream: true, messages: [{ role: "user", content: "hi" }] }),
      }),
      wait,
    );
    assert.equal(response.status, 200);
    const reader = response.body!.getReader();
    const first = await reader.read();
    assert.equal(first.done, false);
    await reader.cancel();
    assert.equal(pending.length > 0, true);
    await Promise.all(pending);
    const summary = await store.summarizeUsage("platform", "compat");
    assert.equal(summary.length, 1);
    assert.equal(summary[0]?.status, "client_cancelled");
    assert.equal(Number(summary[0]?.cost_microdollars) > 0, true);
    assert.equal(JSON.stringify(summary).includes("sk-worker-secret"), false);
  } finally {
    upstream.close();
  }
});

function listen(
  onRequest: (req: import("node:http").IncomingMessage, res: import("node:http").ServerResponse) => void,
): Promise<{ url: string; close: () => void }> {
  return new Promise((resolve) => {
    const server = createServer(onRequest);
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
