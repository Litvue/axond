import assert from "node:assert/strict";
import test from "node:test";

import { beginTrace, formatTraceparent, metricPayload, resolveTelemetry, signalUrl, tracePayload } from "./otel.ts";

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
