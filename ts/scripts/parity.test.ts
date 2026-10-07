import assert from "node:assert/strict";
import test from "node:test";
import { samePayload } from "./parity.ts";

const compare = (a: string, b: string) => samePayload(Buffer.from(a), Buffer.from(b));

test("parity keeps large integer changes visible despite key reordering", () => {
  assert.equal(compare('{"n":9007199254740992,"ok":true}', '{"ok":true,"n":9007199254740993}'), false);
  assert.equal(compare('{"n":9007199254740993,"ok":true}', '{"ok":true,"n":9007199254740993}'), true);
});

test("parity accepts JSON spacing but preserves errors, nulls, and array order", () => {
  assert.equal(compare('{"a":1,"b":2}', '{ "b": 2, "a": 1 }'), true);
  assert.equal(compare('{"cost":null}', '{"cost":0}'), false);
  assert.equal(compare('[1,2]', '[2,1]'), false);
  assert.equal(compare('{"error":{"type":"bad_request","message":"a"}}', '{"error":{"message":"b","type":"bad_request"}}'), false);
});

test("parity preserves SSE event order, terminal events, and large counters", () => {
  const a = 'event: delta\ndata: {"n":9007199254740993,"text":"a"}\n\ndata: [DONE]\n\n';
  assert.equal(compare(a, 'event: delta\ndata: { "text": "a", "n": 9007199254740993 }\n\ndata: [DONE]\n\n'), true);
  assert.equal(compare(a, a.replace("9007199254740993", "9007199254740992")), false);
  assert.equal(compare(a, a.replace("event: delta", "event: done")), false);
  assert.equal(compare(a, a.replace("data: [DONE]\n\n", "")), false);
});
