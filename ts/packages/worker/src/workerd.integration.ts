import assert from "node:assert/strict";
import { createServer } from "node:http";
import test from "node:test";
import { unstable_dev } from "wrangler";

const dsn = process.env.AXOND_TEST_POSTGRES;
const key = "test-inbound-key";

test("workerd serves the gateway through a local Hyperdrive binding", { skip: !dsn }, async () => {
  process.env.CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE = dsn;
  const worker = await unstable_dev(new URL("./index.ts", import.meta.url).pathname, {
    config: new URL("../wrangler.toml", import.meta.url).pathname,
    local: true,
    ip: "127.0.0.1",
    vars: { GATEWAY_KEY: key, PROVIDERS_JSON: "[]" },
    logLevel: "error",
    experimental: { disableExperimentalWarning: true, disableDevRegistry: true },
  });
  const id = `workerd-${Date.now()}`;
  try {
    const health = await worker.fetch("http://127.0.0.1/healthz");
    assert.equal(health.status, 200);
    assert.equal(await health.text(), "ok");
    const created = await worker.fetch("http://127.0.0.1/api/v1/namespaces", {
      method: "POST",
      headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
      body: JSON.stringify({ id }),
    });
    const createdBody = await created.text();
    assert.equal(created.status, 201, createdBody);
    const listed = await worker.fetch("http://127.0.0.1/api/v1/namespaces", {
      headers: { authorization: `Bearer ${key}` },
    });
    const listedBody = await listed.text();
    assert.equal(listed.status, 200, listedBody);
    const body = JSON.parse(listedBody) as { data: { id: string }[] };
    assert.equal(body.data.some((row) => row.id === id), true);
  } finally {
    await worker.stop();
  }
});

test("workerd_hyperdrive_charges_one_request_id_once", { skip: !dsn }, async () => {
  const upstream = await new Promise<{ url: string; close: () => Promise<void> }>((resolve) => {
    const server = createServer((req, res) => {
      req.resume();
      req.on("end", () => {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(
          '{"id":"chatcmpl-workerd","choices":[{"message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}',
        );
      });
    });
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        throw new Error("no port");
      }
      resolve({
        url: `http://127.0.0.1:${address.port}`,
        close: () =>
          new Promise((done) => {
            server.closeAllConnections();
            server.close(() => done());
          }),
      });
    });
  });
  process.env.CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE = dsn;
  const id = `wd${Date.now()}`;
  const worker = await unstable_dev(new URL("./index.ts", import.meta.url).pathname, {
    config: new URL("../wrangler.toml", import.meta.url).pathname,
    local: true,
    ip: "127.0.0.1",
    vars: {
      GATEWAY_KEY: key,
      PROVIDERS_JSON: JSON.stringify([
        { id: "fake-openai", kind: "openai", baseUrl: upstream.url, unpricedModels: "allow" },
      ]),
      CREDENTIALS_JSON: JSON.stringify([
        { namespace: id, provider: "fake-openai", secret: "sk-workerd-secret", id: "plat" },
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
    logLevel: "error",
    experimental: { disableExperimentalWarning: true, disableDevRegistry: true },
  });
  const headers = { authorization: `Bearer ${key}`, "content-type": "application/json" };
  try {
    const created = await worker.fetch("http://127.0.0.1/api/v1/namespaces", {
      method: "POST",
      headers,
      body: JSON.stringify({ id }),
    });
    assert.equal(created.status, 201, await created.text());
    const budget = await worker.fetch(`http://127.0.0.1/api/v1/namespaces/${id}/budgets/compat`, {
      method: "PUT",
      headers,
      body: JSON.stringify({ limit_microdollars: 1_000_000 }),
    });
    assert.equal(budget.status, 200, await budget.text());
    const chat = () =>
      worker.fetch(`http://127.0.0.1/ns/${id}/v1/chat/completions`, {
        method: "POST",
        headers: { ...headers, "x-request-id": `workerd-once-${id}` },
        body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: "hi" }] }),
      });
    const first = await chat();
    assert.equal(first.status, 200, await first.text());
    const second = await chat();
    assert.equal(second.status, 200, await second.text());
    let spent = -1;
    for (let attempt = 0; attempt < 20; attempt += 1) {
      const row = await worker.fetch(`http://127.0.0.1/api/v1/namespaces/${id}/budgets/compat`, {
        headers: { authorization: `Bearer ${key}` },
      });
      const rowBody = await row.text();
      assert.equal(row.status, 200, rowBody);
      spent = (JSON.parse(rowBody) as { spent_microdollars: number }).spent_microdollars;
      if (spent === 2) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    assert.equal(spent, 2);
    const usage = await worker.fetch(`http://127.0.0.1/api/v1/namespaces/${id}/usage?period=compat`, {
      headers: { authorization: `Bearer ${key}` },
    });
    const usageBody = await usage.text();
    assert.equal(usage.status, 200, usageBody);
    const summary = JSON.parse(usageBody) as { data: { count: number; cost_microdollars: number }[] };
    assert.equal(summary.data.length, 1);
    assert.equal(summary.data[0]?.count, 1);
    assert.equal(summary.data[0]?.cost_microdollars, 2);
    assert.equal(usageBody.includes("sk-workerd-secret"), false);
  } finally {
    await worker.stop();
    await upstream.close();
  }
});
