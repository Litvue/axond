import assert from "node:assert/strict";
import test from "node:test";
import { ByteRequestBody, readBoundedBody } from "./body.ts";
import { readStrictObject, serdeValue } from "./strict-json.ts";

test("concurrent body readers share the source and replacements retain the prepared model", async () => {
  const body = new ByteRequestBody(new Request("http://localhost", { method: "POST", body: '{"model":"provider/model","messages":[]}' }));
  const [raw, parsed] = await Promise.all([body.raw(), body.json()]);
  assert.equal(new TextDecoder().decode(raw), JSON.stringify(parsed));
  body.setModel("model"); body.setJson({ model: "provider/model", prompt: "hidden" });
  assert.equal((await body.json<{ model: string }>()).model, "model");
  assert.equal(JSON.parse(new TextDecoder().decode(body.outgoing())).model, "model");
});

test("chunked body limits cancel before EOF rather than buffering the whole request", async () => {
  let cancelled = false; let reads = 0;
  const source = new ReadableStream({ pull(c) { reads++; c.enqueue(new Uint8Array(8)); }, cancel() { cancelled = true; } });
  const request = new Request("http://localhost", { method: "POST", body: source, duplex: "half" } as RequestInit);
  await assert.rejects(readBoundedBody(request, 12), (error: any) => error.status === 413);
  assert.equal(cancelled, true); assert.ok(reads < 5);
});

test("strict management JSON rejects deep nesting and positional trailing commas", () => {
  const fields = [{ name: "attrs", kind: "any" as const }];
  const encode = (s: string) => new TextEncoder().encode(s);
  assert.throws(() => readStrictObject(encode('{"attrs":' + '['.repeat(200) + '0' + ']'.repeat(200) + '}'), fields, "Body"), /recursion limit/);
  assert.throws(() => readStrictObject(encode('[{},]'), fields, "Body"), /trailing comma/);
  assert.equal(serdeValue("null"), null);
});
