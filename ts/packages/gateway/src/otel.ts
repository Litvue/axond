/**
 * OTLP/HTTP JSON export. One `fetch` per signal at the end of a request, with
 * no timer thread and no exporter object kept across requests. The Rust
 * process speaks `http/protobuf`; this process speaks `application/json` on
 * the same `/v1/traces` and `/v1/metrics` paths.
 */

const SIGNALS = ["traces", "metrics", "logs"] as const;

export interface TelemetryTarget {
  endpoint: string;
  instanceId?: string;
}

export function resolveTelemetry(input: {
  endpoint?: string | null;
  protocol?: string | null;
  instanceId?: string | null;
}): TelemetryTarget | null {
  const instanceId = validateInstanceId(input.instanceId);
  const endpoint = blank(input.endpoint);
  if (!endpoint) {
    return null;
  }
  if (!endpoint.startsWith("http://") && !endpoint.startsWith("https://")) {
    throw new Error("OTEL_EXPORTER_OTLP_ENDPOINT must be an http:// or https:// URL");
  }
  const protocol = blank(input.protocol);
  if (protocol && protocol !== "http/json") {
    throw new Error(
      `OTEL_EXPORTER_OTLP_PROTOCOL=\`${protocol}\` is unsupported: the TypeScript gateway exports OTLP/HTTP JSON`,
    );
  }
  return instanceId ? { endpoint, instanceId } : { endpoint };
}

export function signalUrl(endpoint: string, signal: "traces" | "metrics" | "logs"): string {
  let base = endpoint.trim().replace(/\/+$/, "");
  for (const name of SIGNALS) {
    const suffix = `/v1/${name}`;
    if (base.endsWith(suffix)) {
      base = base.slice(0, -suffix.length);
    }
  }
  return `${base.replace(/\/+$/, "")}/v1/${signal}`;
}

export interface TraceContext {
  traceId: string;
  spanId: string;
  parentSpanId?: string;
}

export function beginTrace(header: string | undefined): TraceContext {
  const parent = parseTraceparent(header);
  if (!parent) {
    return { traceId: randomHex(16), spanId: randomHex(8) };
  }
  return { traceId: parent.traceId, spanId: randomHex(8), parentSpanId: parent.spanId };
}

export function formatTraceparent(trace: TraceContext): string {
  return `00-${trace.traceId}-${trace.spanId}-01`;
}

export function parseTraceparent(header: string | undefined): { traceId: string; spanId: string } | null {
  if (!header) {
    return null;
  }
  const match = /^00-([0-9a-f]{32})-([0-9a-f]{16})-[0-9a-f]{2}$/i.exec(header.trim());
  if (!match) {
    return null;
  }
  const traceId = match[1]!.toLowerCase();
  const spanId = match[2]!.toLowerCase();
  if (/^0+$/.test(traceId) || /^0+$/.test(spanId)) {
    return null;
  }
  return { traceId, spanId };
}

export interface ExportedSpan {
  name: string;
  trace: TraceContext;
  startMs: number;
  endMs: number;
  attributes: Record<string, string>;
  error: boolean;
}

export function tracePayload(spans: readonly ExportedSpan[], resource: Record<string, string>): unknown {
  return {
    resourceSpans: [
      {
        resource: { attributes: keyValues(resource) },
        scopeSpans: [
          {
            scope: { name: "axond" },
            spans: spans.map((span) => ({
              traceId: span.trace.traceId,
              spanId: span.trace.spanId,
              ...(span.trace.parentSpanId ? { parentSpanId: span.trace.parentSpanId } : {}),
              name: span.name,
              kind: 2,
              startTimeUnixNano: unixNano(span.startMs),
              endTimeUnixNano: unixNano(span.endMs),
              attributes: keyValues(span.attributes),
              status: { code: span.error ? 2 : 1 },
            })),
          },
        ],
      },
    ],
  };
}

export interface ExportedPoint {
  name: string;
  value: number;
  attributes: Record<string, string>;
  observations?: number;
  min?: number;
  max?: number;
}

export function metricPayload(points: readonly ExportedPoint[], resource: Record<string, string>, timeMs: number): unknown {
  return {
    resourceMetrics: [
      {
        resource: { attributes: keyValues(resource) },
        scopeMetrics: [
          {
            scope: { name: "axond" },
            metrics: points.map((point) =>
              isHistogram(point.name) ? histogramMetric(point, timeMs) : sumMetric(point, timeMs),
            ),
          },
        ],
      },
    ],
  };
}

export async function postOtlp(
  endpoint: string,
  signal: "traces" | "metrics",
  body: unknown,
  fetchImpl: typeof fetch = fetch,
): Promise<void> {
  const response = await fetchImpl(signalUrl(endpoint, signal), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  await response.arrayBuffer();
}

export function resourceAttributes(instanceId?: string): Record<string, string> {
  const attributes: Record<string, string> = { "service.name": "axond" };
  if (instanceId) {
    attributes["service.instance.id"] = instanceId;
  }
  return attributes;
}

export function isHistogram(name: string): boolean {
  return name.endsWith(".duration") || name.includes("time_to_first_token") || name.endsWith("_wait") || name.endsWith(".wait");
}

function sumMetric(point: ExportedPoint, timeMs: number): unknown {
  return {
    name: point.name,
    sum: {
      aggregationTemporality: 2,
      isMonotonic: true,
      dataPoints: [
        {
          asInt: String(Math.trunc(point.value)),
          timeUnixNano: unixNano(timeMs),
          attributes: keyValues(point.attributes),
        },
      ],
    },
  };
}

function histogramMetric(point: ExportedPoint, timeMs: number): unknown {
  const count = point.observations ?? 1;
  return {
    name: point.name,
    histogram: {
      aggregationTemporality: 2,
      dataPoints: [
        {
          count: String(count),
          sum: point.value,
          min: point.min ?? point.value,
          max: point.max ?? point.value,
          bucketCounts: [String(count)],
          explicitBounds: [],
          timeUnixNano: unixNano(timeMs),
          attributes: keyValues(point.attributes),
        },
      ],
    },
  };
}

function keyValues(attributes: Record<string, string>): { key: string; value: { stringValue: string } }[] {
  return Object.entries(attributes).map(([key, value]) => ({ key, value: { stringValue: value } }));
}

function unixNano(ms: number): string {
  return (BigInt(Math.max(0, Math.trunc(ms))) * 1_000_000n).toString();
}

function blank(value: string | null | undefined): string | null {
  if (value === undefined || value === null) {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length === 0 ? null : trimmed;
}

function validateInstanceId(value: string | null | undefined): string | undefined {
  const instanceId = blank(value);
  if (!instanceId) {
    return undefined;
  }
  if (instanceId.length > 128) {
    throw new Error("AXOND_INSTANCE_ID must be at most 128 bytes");
  }
  if (![...instanceId].every((char) => /[A-Za-z0-9._-]/.test(char))) {
    throw new Error("AXOND_INSTANCE_ID may contain only ASCII letters, digits, `.`, `_`, and `-`");
  }
  return instanceId;
}

function randomHex(bytes: number): string {
  const buffer = new Uint8Array(bytes);
  crypto.getRandomValues(buffer);
  let hex = "";
  for (const byte of buffer) {
    hex += byte.toString(16).padStart(2, "0");
  }
  if (/^0+$/.test(hex)) {
    return randomHex(bytes);
  }
  return hex;
}
