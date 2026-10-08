import assert from "node:assert/strict";
import test from "node:test";

import { createAxond } from "./app.ts";
import { createMemoryStore } from "./memory-store.ts";

test("exporter posts are handed to onBackground", async () => {
  const tasks: Promise<void>[] = [];
  let release = () => {};
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const endpoint = "http://collector.example/otlp";
  const app = createAxond({
    store: createMemoryStore(),
    gatewayKey: "local-key",
    defaultNamespace: "platform",
    telemetry: {
      endpoint,
      fetch: async () => {
        await gate;
        return new Response(null, { status: 200 });
      },
    },
    onBackground: (task) => {
      tasks.push(task);
    },
  });
  const response = await app.request("http://127.0.0.1/healthz");
  assert.equal(response.status, 200);
  assert.equal(await response.text(), "ok");
  assert.equal(tasks.length, 1);
  release();
  await tasks[0];
});
