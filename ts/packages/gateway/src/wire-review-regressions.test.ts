import assert from "node:assert/strict";
import test from "node:test";
import { createResponsesSequence } from "./responses-sequence.ts";
import { createNativeMessagesSequence } from "./native-messages.ts";
import { costMicrodollars } from "./pricing.ts";
import { createMetrics } from "./metrics.ts";
import { metricPayload, postOtlp } from "./otel.ts";
import { scopeStore } from "./scoped-store.ts";
import type { Store } from "@axond/sdk";

test("pricing saturates the public u64 domain", () => {
  const max = (1n << 64n) - 1n;
  assert.equal(costMicrodollars({ provider: "p", model: "*", inputMicrodollarsPerMillion: max, outputMicrodollarsPerMillion: max }, { inputTokens: max, outputTokens: max, reasoningTokens: 0n, cacheReadTokens: 0n, cacheWriteTokens: 0n }), max);
});

test("OTLP respects instrument kinds and strips sensitive extension labels", async () => {
  const metrics = createMetrics();
  metrics.record("axond.admission.queue.depth", 2);
  metrics.record("axond.admission.queue.depth", 4);
  metrics.record("axond.settlement.in_flight", 1);
  metrics.record("axond.settlement.in_flight", -1);
  metrics.set("axond.catalog.active_age", 50);
  metrics.record("axond.ext.test", 1, { prompt: "private", api_key: "private", "axond.namespace": "safe" });
  const exported = (metricPayload(metrics.points, {}, Date.now()) as any).resourceMetrics[0].scopeMetrics[0].metrics;
  assert.equal(exported[0].histogram.dataPoints[0].count, "2");
  assert.equal(exported[1].sum.isMonotonic, false);
  assert.equal(exported[2].gauge.dataPoints[0].asDouble, 50);
  assert.equal(JSON.stringify(exported).includes("private"), false);
  await assert.rejects(postOtlp("http://collector", "metrics", {}, async () => new Response("unavailable", { status: 503 })), /HTTP 503/);
});

test("scoped SQL refuses projected leaks and writes before contacting the backend", async () => {
  let calls = 0;
  const store = { async query() { calls++; return { rows: [] }; } } as unknown as Store;
  const scoped = scopeStore(store, "one");
  for (const query of ["SELECT attrs FROM axond_namespace WHERE id = 'two' AND ? = ?", "DELETE FROM axond_namespace WHERE id = ?", "SELECT attrs FROM axond_namespace WHERE id <> ?"]) {
    await assert.rejects(scoped.query(query, ["one"]));
  }
  assert.equal(calls, 0);
  await scoped.query("SELECT attrs FROM axond_namespace WHERE id = ?", ["one"]);
  assert.equal(calls, 1);
});


test("complete coalesced SSE frames do not consume the incomplete-event limit", () => {
  const wire = (": " + "x".repeat(1024) + "\n\n").repeat(1100);
  for (const decoder of [createResponsesSequence(() => false), createNativeMessagesSequence(() => false)]) {
    assert.equal(decoder.push(wire), null); assert.equal(decoder.finish(), null);
    assert.match(decoder.push("data: " + "x".repeat(1024 * 1024)), /exceeded/);
  }
});

test("extension wait and first-token observations retain histograms", () => {
  const metrics = createMetrics();
  for (const name of ["axond.ext.queue_wait", "axond.ext.time_to_first_token"]) {
    metrics.record(name, 1); metrics.record(name, 4);
    const point = metrics.points.find(row => row.name === name)!;
    assert.equal(point.observations, 2); assert.equal(point.min, 1); assert.equal(point.max, 4);
  }
});
