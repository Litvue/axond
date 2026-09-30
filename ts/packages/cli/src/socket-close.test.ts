import assert from "node:assert/strict";
import { createServer, request as httpRequest } from "node:http";
import type { IncomingMessage, ServerResponse } from "node:http";
import test from "node:test";

import { getRequestListener } from "@hono/node-server";
import type { UsageRecord } from "@axond/sdk";

import { createAxond } from "../../gateway/src/app.ts";
import { createMemoryStore } from "../../gateway/src/memory-store.ts";
import { usageEvent } from "../../gateway/src/usage.ts";

const KEY = "test-inbound-key";

function listen(
  onRequest: (req: IncomingMessage, res: ServerResponse) => void,
): Promise<{ url: string; close: () => void }> {
  const server = createServer(onRequest);
  return new Promise((resolve) => {
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

test("node_socket_close_settles_client_cancelled", async () => {
  const store = createMemoryStore();
  await store.putNamespace({
    id: "platform",
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: true,
  });
  await store.putBudget("platform", "compat", 1_000_000_000n);
  const before = (await store.getBudget("platform", "compat"))!;
  const records: UsageRecord[] = [];
  const upstream = await listen((req, res) => {
    req.resume();
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.write('data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n');
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "sk-live-secret", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 2_500_000n,
        outputMicrodollarsPerMillion: 10_000_000n,
      },
    ],
    rawPath: (c) => c.req.header("x-axond-raw-path") ?? new URL(c.req.url).pathname,
    onUsage: (record) => {
      records.push(record);
    },
  });
  const listener = getRequestListener(app.fetch);
  const gateway = await listen((req, res) => {
    const raw = (req.url ?? "/").split("?")[0] ?? "/";
    delete req.headers["x-axond-raw-path"];
    req.headers["x-axond-raw-path"] = raw;
    listener(req, res);
  });
  try {
    const body = JSON.stringify({ model: "fake-openai/gpt-test", stream: true, messages: [] });
    const url = new URL(`${gateway.url}/ns/platform/v1/chat/completions`);
    await new Promise<void>((resolve, reject) => {
      let settled = false;
      const finish = (error?: Error) => {
        if (settled) {
          return;
        }
        settled = true;
        if (error) {
          reject(error);
          return;
        }
        resolve();
      };
      const req = httpRequest(
        {
          hostname: url.hostname,
          port: url.port,
          path: `${url.pathname}${url.search}`,
          method: "POST",
          headers: {
            authorization: `Bearer ${KEY}`,
            "content-type": "application/json",
            "content-length": Buffer.byteLength(body),
          },
        },
        (res) => {
          if (res.statusCode !== 200) {
            res.resume();
            finish(new Error(`status ${res.statusCode}`));
            return;
          }
          res.once("data", () => {
            req.destroy();
            finish();
          });
          res.on("error", () => undefined);
        },
      );
      req.on("error", (error: NodeJS.ErrnoException) => {
        if (settled && (error.code === "ECONNRESET" || error.code === "EPIPE")) {
          return;
        }
        finish(error);
      });
      req.setTimeout(5_000, () => {
        req.destroy();
        finish(new Error("socket close timed out"));
      });
      req.write(body);
      req.end();
    });
    for (let attempt = 0; attempt < 50 && records.length === 0; attempt += 1) {
      await new Promise((wake) => setTimeout(wake, 20));
    }
    assert.equal(records.length, 1);
    const record = records[0]!;
    assert.equal(record.status, "client_cancelled");
    assert.equal(record.credentialId, "one");
    assert.equal(record.outputTokens, 1n);
    assert.equal(record.inputTokens > 0n, true);
    assert.equal(record.costMicrodollars !== null && record.costMicrodollars > 0n, true);
    const event = JSON.stringify(usageEvent(record));
    assert.equal(event.includes("sk-live-secret"), false);
    assert.equal(event.includes('"status":"client_cancelled"'), true);
    const after = (await store.getBudget("platform", "compat"))!;
    assert.equal(after.spent - before.spent, record.costMicrodollars);
    const rows = await store.summarizeUsage("platform", "compat");
    assert.equal(rows.length, 1);
    assert.equal(rows[0]!.status, "client_cancelled");
  } finally {
    gateway.close();
    upstream.close();
  }
});
