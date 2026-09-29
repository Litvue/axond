import assert from "node:assert/strict";
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { createAxond } from "./app.ts";
import { createMemoryStore } from "./memory-store.ts";

const KEY = "test-inbound-key";
const ROOT = new URL("../../../../tests/fixtures/", import.meta.url);

async function fixture(name: string): Promise<Uint8Array> {
  return new Uint8Array(await readFile(new URL(name, ROOT)));
}

function chunked(bytes: Uint8Array, size: number): Promise<{ url: string; close: () => void }> {
  return new Promise((resolve) => {
    const server = createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (chunk: Buffer) => chunks.push(chunk));
      req.on("end", () => {
        res.writeHead(200, { "content-type": req.url?.endsWith(".json") ? "application/json" : "text/event-stream" });
        let offset = 0;
        const write = () => {
          if (offset >= bytes.length) {
            res.end();
            return;
          }
          const next = bytes.subarray(offset, offset + size);
          offset += size;
          res.write(Buffer.from(next), () => write());
        };
        write();
      });
    });
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

async function boot(baseUrl: string, kind: "openai" | "anthropic") {
  const store = createMemoryStore();
  await store.putNamespace({
    id: "platform",
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: true,
  });
  await store.putBudget("platform", "compat", 1_000_000_000_000n);
  const provider = kind === "openai" ? "fake-openai" : "fake-anthropic";
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    providers: [{ id: provider, kind, baseUrl }],
    credentials: [{ namespace: "platform", provider, secret: "upstream", id: "one" }],
    prices: [
      {
        provider,
        model: "*",
        inputMicrodollarsPerMillion: 2_500_000n,
        outputMicrodollarsPerMillion: 10_000_000n,
      },
    ],
  });
  return app;
}

for (const [name, route, model, kind] of [
  ["openai/chat_completion.sse", "/ns/platform/v1/chat/completions", "fake-openai/gpt-test", "openai"],
  ["openai/responses.sse", "/ns/platform/v1/responses", "fake-openai/gpt-test", "openai"],
  ["anthropic/message_thinking_tool_use.sse", "/ns/platform/v1/messages", "fake-anthropic/claude-test", "anthropic"],
] as const) {
  test(`relays ${name} byte-for-byte across 5-byte chunks`, async () => {
    const bytes = await fixture(name);
    const upstream = await chunked(bytes, 5);
    const app = await boot(upstream.url, kind);
    const response = await app.request(`http://127.0.0.1${route}`, {
      method: "POST",
      headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
      body: JSON.stringify({ model, stream: true, messages: [{ role: "user", content: "hi" }] }),
    });
    assert.equal(response.status, 200);
    const got = new Uint8Array(await response.arrayBuffer());
    assert.deepEqual(got, bytes);
    upstream.close();
  });
}

test("a buffered fixture is relayed without a keepalive comment", async () => {
  const bytes = await fixture("openai/chat_completion.json");
  const upstream = await chunked(bytes, 3);
  const app = await boot(upstream.url, "openai");
  const response = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
    body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [] }),
  });
  const got = new Uint8Array(await response.arrayBuffer());
  assert.deepEqual(got, bytes);
  assert.equal(new TextDecoder().decode(got).includes(": keepalive"), false);
  upstream.close();
});
