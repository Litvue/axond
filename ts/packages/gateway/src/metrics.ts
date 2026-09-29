/** Metric names that remain after ADR 0063. Dashboards and alerts match these strings. */
export const METRIC_NAMES = [
  "axond.http.server.requests",
  "axond.http.server.duration",
  "axond.request.count",
  "axond.request.duration",
  "axond.request.time_to_first_token",
  "axond.tokens.input",
  "axond.tokens.cache_read",
  "axond.tokens.cache_write",
  "axond.tokens.output",
  "axond.cost.microdollars",
  "axond.upstream.errors",
  "axond.upstream.timeouts",
  "axond.upstream.time_to_first_token",
  "axond.upstream.circuit_state",
  "axond.usage.records_written",
  "axond.usage.records_dropped",
  "axond.usage.flushes",
  "axond.usage.index.appends",
  "axond.usage.index.batches",
  "axond.usage.index.batch_size",
  "axond.usage.index.queue_age",
  "axond.settlement.in_flight",
  "axond.settlement.failures",
  "axond.store.operations",
  "axond.store.query_duration",
  "axond.admission.in_flight",
  "axond.admission.rejections",
] as const;

export interface MetricPoint {
  name: string;
  value: number;
  attributes: Record<string, string>;
}

const MAX_LABEL = 64;
const MAX_CARDINALITY = 200;

/**
 * In-process recorder. Attribute values that contain a secret or a content
 * sentinel are dropped. Extension metrics share the cardinality ceiling.
 */
export function createMetrics(secrets: readonly string[] = []) {
  const points: MetricPoint[] = [];
  const seen = new Set<string>();
  return {
    points,
    record(name: string, value: number, attributes: Record<string, string> = {}) {
      if (!(METRIC_NAMES as readonly string[]).includes(name) && !name.startsWith("axond.ext.")) {
        throw new Error(`metric ${name} is not in the catalogue`);
      }
      const safe: Record<string, string> = {};
      for (const [key, raw] of Object.entries(attributes)) {
        if (raw.length > MAX_LABEL) {
          continue;
        }
        if (secrets.some((secret) => secret.length > 0 && raw.includes(secret))) {
          continue;
        }
        safe[key] = raw;
      }
      const signature = `${name}\0${JSON.stringify(safe)}`;
      if (!seen.has(signature) && seen.size >= MAX_CARDINALITY) {
        return;
      }
      seen.add(signature);
      points.push({ name, value, attributes: safe });
    },
  };
}
