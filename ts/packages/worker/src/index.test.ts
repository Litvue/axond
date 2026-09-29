import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { createHandler } from "./index.ts";

test("the worker handler is a static bundle of the gateway and an extension", async () => {
  const source = await readFile(new URL("./index.ts", import.meta.url), "utf8");
  assert.match(source, /from "@axond\/rate-limit"/);
  assert.match(source, /waitUntil/);
  assert.equal(source.includes("node:"), false);
  const handler = createHandler({
    HYPERDRIVE: { connectionString: "postgres://example" },
    GATEWAY_KEY: "k",
    PROVIDERS_JSON: "[]",
  });
  assert.equal(typeof handler.fetch, "function");
});
