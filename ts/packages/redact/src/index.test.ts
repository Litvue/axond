import assert from "node:assert/strict";
import { createServer } from "node:http";
import test from "node:test";

import { createAxond } from "@axond/gateway";
import { createMemoryStore } from "../../gateway/src/memory-store.ts";

import { redactExtension } from "./index.ts";

test("a rule redacts across an SSE split and a miss keeps the upstream bytes", async () => {
  const seen: string[] = [];
  const server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (chunk: Buffer) => chunks.push(chunk));
    req.on("end", () => {
      seen.push(Buffer.concat(chunks).toString("utf8"));
      const stream = new URL(req.url ?? "", "http://local").searchParams;
      void stream;
      if (req.url === "/chat/completions" && chunks.length >= 0 && Buffer.concat(chunks).toString("utf8").includes('"stream":true')) {
        res.writeHead(200, { "content-type": "text/event-stream" });
        res.write('data: {"delta":"sk-li');
        res.end('ve"}\n\n');
        return;
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end('{"usage":{"prompt_tokens":1,"completion_tokens":1}}');
    });
  });
  await new Promise<void>((ready) => server.listen(0, "127.0.0.1", () => ready()));
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("no port");
  }
  const store = createMemoryStore();
  await store.putNamespace({
    id: "platform",
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: true,
  });
  await store.putBudget("platform", "compat", 1_000_000n);
  const app = createAxond({
    store,
    gatewayKey: "k",
    defaultNamespace: "platform",
    providers: [{ id: "fake-openai", kind: "openai", baseUrl: `http://127.0.0.1:${address.port}` }],
    credentials: [{ namespace: "platform", provider: "fake-openai", secret: "upstream", id: "one" }],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 1n,
        outputMicrodollarsPerMillion: 1n,
      },
    ],
    extensions: [redactExtension([{ pattern: "sk-live", replacement: "[redacted]" }])],
  });
  const clean = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: { authorization: "Bearer k", "content-type": "application/json" },
    body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: "hello" }] }),
  });
  assert.equal(clean.status, 200);
  assert.equal(seen[0]?.includes("hello"), true);
  assert.equal(seen[0]?.includes("sk-live"), false);

  const streamed = await app.request("http://127.0.0.1/ns/platform/v1/chat/completions", {
    method: "POST",
    headers: { authorization: "Bearer k", "content-type": "application/json" },
    body: JSON.stringify({
      model: "fake-openai/gpt-test",
      stream: true,
      messages: [{ role: "user", content: "secret sk-live please" }],
    }),
  });
  const text = await streamed.text();
  assert.equal(text.includes("sk-live"), false);
  assert.equal(text.includes("[redacted]"), true);
  assert.equal(seen[1]?.includes("sk-live"), false);
  server.closeAllConnections();
  server.close();
});