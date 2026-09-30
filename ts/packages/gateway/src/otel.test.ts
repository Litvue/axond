import assert from "node:assert/strict";
import test from "node:test";

import { beginTrace, formatTraceparent, metricPayload, resolveTelemetry, signalUrl, tracePayload, usageLogPayload } from "./otel.ts";
import type { UsageRecord } from "@axond/sdk";

test("resolveTelemetry is off without an endpoint and rejects other protocols", () => {
  assert.equal(resolveTelemetry({}), null);
  assert.equal(resolveTelemetry({ endpoint: "  ", instanceId: "replica-a" }), null);
  assert.deepEqual(resolveTelemetry({ endpoint: "http://collector:4318" }), { endpoint: "http://collector:4318" });
  assert.deepEqual(resolveTelemetry({ endpoint: "http://collector:4318", protocol: "http/json", instanceId: "replica-a" }), {
    endpoint: "http://collector:4318",
    instanceId: "replica-a",
  });
  assert.throws(() => resolveTelemetry({ endpoint: "http://collector:4318", protocol: "grpc" }), /OTLP\/HTTP JSON/);
  assert.throws(() => resolveTelemetry({ endpoint: "http://collector:4318", protocol: "http/protobuf" }), /OTLP\/HTTP JSON/);
  assert.throws(() => resolveTelemetry({ endpoint: "collector:4318" }), /http:\/\/ or https:\/\//);
  assert.throws(() => resolveTelemetry({ instanceId: "has space" }), /ASCII letters/);
});

test("signalUrl appends the requested signal", () => {
  assert.equal(signalUrl("http://collector:4318", "traces"), "http://collector:4318/v1/traces");
  assert.equal(signalUrl("http://collector:4318/v1/metrics/", "traces"), "http://collector:4318/v1/traces");
});

test("beginTrace joins an inbound traceparent", () => {
  const inbound = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
  const trace = beginTrace(inbound);
  assert.equal(trace.traceId, "4bf92f3577b34da6a3ce929d0e0e4736");
  assert.equal(trace.parentSpanId, "00f067aa0ba902b7");
  assert.notEqual(trace.spanId, "00f067aa0ba902b7");
  assert.equal(formatTraceparent(trace), `00-${trace.traceId}-${trace.spanId}-01`);
  assert.equal(beginTrace("00-00000000000000000000000000000000-00f067aa0ba902b7-01").parentSpanId, undefined);
  const span = tracePayload(
    [{ name: "http.server.request", trace, startMs: 1, endMs: 2, attributes: { "http.route": "/healthz" }, error: false }],
    { "service.name": "axond" },
  ) as { resourceSpans: { scopeSpans: { spans: { traceId: string; parentSpanId: string }[] }[] }[] };
  assert.equal(span.resourceSpans[0]!.scopeSpans[0]!.spans[0]!.traceId, trace.traceId);
  const metrics = metricPayload(
    [{ name: "axond.http.server.duration", value: 5, attributes: {}, observations: 1, min: 5, max: 5 }],
    { "service.name": "axond" },
    2,
  ) as { resourceMetrics: { scopeMetrics: { metrics: { histogram: { dataPoints: { sum: number }[] } }[] }[] }[] };
  assert.equal(metrics.resourceMetrics[0]!.scopeMetrics[0]!.metrics[0]!.histogram.dataPoints[0]!.sum, 5);
});

test("usage log omits a null cost and keeps a zero", () => {
  const record = sampleUsage();
  const priced = usageLogPayload(record, { "service.name": "axond" }, 5) as LogBody;
  const log = priced.resourceLogs[0]!.scopeLogs[0]!.logRecords[0]!;
  assert.equal(log.eventName, "axond.usage");
  assert.equal(log.traceId, record.traceId);
  assert.equal(log.attributes.find((item) => item.key === "axond.cost_microdollars")?.value.intValue, "5");
  assert.equal(log.attributes.find((item) => item.key === "axond.signer_kid"), undefined);
  assert.equal(log.attributes.find((item) => item.key === "attrs"), undefined);
  const unpriced = usageLogPayload({ ...record, costMicrodollars: null, traceId: null, period: null }, { "service.name": "axond" }, 5) as LogBody;
  const bare = unpriced.resourceLogs[0]!.scopeLogs[0]!.logRecords[0]!;
  assert.equal(bare.traceId, undefined);
  assert.equal(bare.attributes.find((item) => item.key === "axond.cost_microdollars"), undefined);
  assert.equal(bare.attributes.find((item) => item.key === "axond.period"), undefined);
  assert.equal(bare.attributes.find((item) => item.key === "axond.cost_microdollars"), undefined);
  const zero = usageLogPayload({ ...record, costMicrodollars: 0n }, { "service.name": "axond" }, 5) as LogBody;
  assert.equal(
    zero.resourceLogs[0]!.scopeLogs[0]!.logRecords[0]!.attributes.find((item) => item.key === "axond.cost_microdollars")?.value.intValue,
    "0",
  );
  const wide = usageLogPayload({ ...record, inputTokens: 9223372036854775808n }, { "service.name": "axond" }, 5) as LogBody;
  assert.equal(
    wide.resourceLogs[0]!.scopeLogs[0]!.logRecords[0]!.attributes.find((item) => item.key === "gen_ai.usage.input_tokens")?.value.intValue,
    "9223372036854775807",
  );
});

interface LogBody {
  resourceLogs: {
    scopeLogs: {
      logRecords: {
        eventName: string;
        traceId?: string;
        attributes: { key: string; value: { intValue?: string; stringValue?: string } }[];
      }[];
    }[];
  }[];
}

function sampleUsage(): UsageRecord {
  return {
    schemaVersion: 2,
    requestId: "req_usage",
    traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
    namespace: "acme",
    period: "2026-09",
    subject: "GW_KEY",
    model: "openai/gpt-test",
    targetProvider: "openai",
    targetModel: "gpt-test",
    credentialSource: "platform",
    credentialId: "openai-primary",
    status: "ok",
    inputTokens: 4n,
    outputTokens: 1n,
    reasoningTokens: 0n,
    cacheReadTokens: 2n,
    cacheWriteTokens: 0n,
    costMicrodollars: 5n,
    catalogVersion: 0,
    priceBook: null,
    priceBookChecksum: null,
    priceCatalog: null,
    signerKid: null,
    latencyMs: 7,
    attempts: 1,
  };
}
