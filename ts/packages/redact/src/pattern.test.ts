import assert from "node:assert/strict";
import test from "node:test";

import { compilePattern } from "./pattern.ts";

test("nested_quantifier_redaction_pattern_is_rejected", () => {
  assert.throws(() => compilePattern("(a+)+"), /nested quantifiers/);
});

test("an empty match is rejected", () => {
  assert.throws(() => compilePattern("a*"), /empty matches/);
});

test("a literal is replaced by the caller", () => {
  const match = compilePattern("sk-test");
  assert.deepEqual(match("key sk-test end"), [{ start: 4, end: 11 }]);
});

test("accepted ambiguous quantifiers remain bounded on a repetitive miss", () => {
  assert.deepEqual(compilePattern("a+a+b")("a".repeat(10000)), []);
  assert.throws(() => compilePattern("(ab)+"), /grouping/);
  assert.throws(() => compilePattern("a{100}"), /quantifier/);
});
