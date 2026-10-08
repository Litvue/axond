import assert from "node:assert/strict";
import test from "node:test";
import { createAxond } from "./app.ts";
import { KEY, seeded, listen } from "./behavior-test-fixtures.ts";
import type { UsageRecord } from "@axond/sdk";

for (const [extra, expectedInput] of [[false, 14n], [true, 29n]] as const) {
  test(`partial stream estimates exclude provider rewrites and retain extension changes: extra=${extra}`, async () => {
    const records: UsageRecord[] = [];
    const background: Promise<unknown>[] = [];
    let sent: Record<string, unknown> | undefined;
    const upstream = await listen(async (req, res) => {
      const chunks = [];
      for await (const chunk of req) chunks.push(chunk);
      sent = JSON.parse(Buffer.concat(chunks).toString());
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.end('data: {"choices":[{"index":0,"delta":{"content":"Hi"},"finish_reason":null}]}\n\n');
    });
    const app = createAxond({
      store: await seeded(), gatewayKey: KEY, defaultNamespace: "platform",
      providers: [{ id: "fake-openai", kind: "openai", baseUrl: upstream.url, unpricedModels: "allow" }],
      credentials: [{ namespace: "platform", provider: "fake-openai", secret: "fixture" }],
      onUsage: (record) => { records.push(record); }, onBackground: (task) => { background.push(task); },
      extensions: extra ? [{ name: "extra", apiVersion: 1, stage: "pre-dispatch", async middleware(c, next) {
        const body = c.get("axond").body;
        body.setJson({ ...await body.json<Record<string, unknown>>(), messages: [{ role: "user", content: "x".repeat(32) }] });
        await next();
      } }] : [],
    });
    try {
      const response = await app.request("http://localhost/ns/platform/v1/chat/completions", {
        method: "POST", headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
        body: '{"model":"fake-openai/fixture","stream":true,"messages":[]}',
      });
      await response.text();
      await Promise.all(background);
      assert.equal(sent?.model, "fixture");
      assert.deepEqual(sent?.stream_options, { include_usage: true });
      assert.deepEqual(sent?.messages, extra ? [{ role: "user", content: "x".repeat(32) }] : []);
      assert.equal(records.length, 1);
      assert.equal(records[0]!.inputTokens, expectedInput);
    } finally { upstream.close(); }
  });
}
