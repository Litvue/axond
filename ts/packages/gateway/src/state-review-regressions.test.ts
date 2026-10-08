import assert from "node:assert/strict";
import test from "node:test";
import { createMemoryStore } from "./memory-store.ts";

test("namespace records cannot mutate the store through ingress or returned aliases", async () => {
  const store = createMemoryStore();
  const attrs = { nested: { value: "original" } }; const blocklist = ["blocked"];
  await store.putNamespace({ id: "one", attrs, blocklist, allowPlatformFallback: false, fromConfig: false });
  attrs.nested.value = "changed"; blocklist.push("changed");
  const read = (await store.getNamespace("one"))!;
  (read.attrs as typeof attrs).nested.value = "changed"; read.blocklist!.push("changed");
  const resolved = (await store.resolveNamespace("one", Date.now()))!;
  (resolved.record.attrs as typeof attrs).nested.value = "changed";
  const latest = (await store.listNamespaces(null, 10)).data[0]!;
  assert.deepEqual(latest.attrs, { nested: { value: "original" } }); assert.deepEqual(latest.blocklist, ["blocked"]);
});
