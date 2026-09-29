import assert from "node:assert/strict";
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
