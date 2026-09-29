import assert from "node:assert/strict";
import test from "node:test";

import { compilePattern } from "./pattern.ts";

test("a nested quantifier is rejected", () => {
  assert.throws(() => compilePattern("(a+)+"), /nested quantifiers/);
});

test("an empty match is rejected", () => {
  assert.throws(() => compilePattern("a*"), /empty matches/);
});

test("a literal is replaced by the caller", () => {
  const match = compilePattern("sk-test");
  assert.deepEqual(match("key sk-test end"), [{ start: 4, end: 11 }]);
});
