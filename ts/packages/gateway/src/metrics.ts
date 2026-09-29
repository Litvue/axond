/** Metric names that remain after ADR 0063. Dashboards and alerts match these strings. */
/** Names from `crates/gateway/src/telemetry/catalog.rs` that dashboards and alerts match. */
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
  "axond.usage.journal.appends",
  "axond.usage.journal.deliveries",
  "axond.usage.journal.quarantined",
  "axond.usage.journal.undeliverable",
  "axond.usage.journal.lost",
  "axond.usage.journal.depth",
  "axond.usage.journal.in_flight",
  "axond.usage.journal.quarantined_events",
  "axond.usage.journal.oldest_pending_age",
  "axond.usage.journal.capacity",
  "axond.usage.index.appends",
  "axond.usage.index.batches",
  "axond.usage.index.batch_size",
  "axond.usage.index.queue_age",
  "axond.usage.index.queue.depth",
  "axond.usage.index.queue.wait",
  "axond.shutdown.phase",
  "axond.shutdown.rejected_requests",
  "axond.shutdown.abandoned_requests",
  "axond.shutdown.abandoned_settlements",
  "axond.shutdown.abandoned_index",
  "axond.settlement.in_flight",
  "axond.settlement.queue_wait",
  "axond.settlement.oldest_pending_age",
  "axond.settlement.failures",
  "axond.admission.queue.depth",
  "axond.admission.in_flight",
  "axond.admission.rejections",
  "axond.store.acquire_wait",
  "axond.store.query_duration",
  "axond.store.operations",
  "axond.store.connections_opened",
  "axond.store.connections_reused",
  "axond.store.connections_discarded",
  "axond.store.pool.sessions",
  "axond.catalog.refusals",
  "axond.catalog.active_age",
  "axond.catalog.consecutive_refusals",
] as const;

export interface MetricPoint {
  name: string;
  value: number;
  attributes: Record<string, string>;
  observations?: number;
  min?: number;
  max?: number;
}

const HISTOGRAM = /\.duration$|time_to_first_token|_wait$|\.wait$/;

export function sanitizeAttributes(
  attributes: Record<string, string>,
  secrets: readonly string[],
): Record<string, string> {
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
  return safe;
}

const MAX_LABEL = 64;
const MAX_CARDINALITY = 200;

function nameHidesSecret(name: string, secrets: readonly string[]): boolean {
  return secrets.some((secret) => secret.length > 0 && name.includes(secret));
}

/**
 * In-process recorder. Attribute values that contain a secret or a content
 * sentinel are dropped. Extension metrics share the cardinality ceiling.
 */
export function createMetrics(secrets: readonly string[] = []) {
  const points: MetricPoint[] = [];
  const series = new Map<string, MetricPoint>();
  return {
    points,
    record(name: string, value: number, attributes: Record<string, string> = {}) {
      if (nameHidesSecret(name, secrets)) {
        return;
      }
      if (!(METRIC_NAMES as readonly string[]).includes(name) && !name.startsWith("axond.ext.")) {
        throw new Error(`metric ${name} is not in the catalogue`);
      }
      const safe = sanitizeAttributes(attributes, secrets);
      const signature = `${name}\0${JSON.stringify(safe)}`;
      const histogram = HISTOGRAM.test(name);
      const existing = series.get(signature);
      if (existing) {
        if (histogram) {
          existing.min = Math.min(existing.min ?? existing.value, value);
          existing.max = Math.max(existing.max ?? existing.value, value);
          existing.observations = (existing.observations ?? 1) + 1;
        }
        existing.value += value;
        return;
      }
      if (series.size >= MAX_CARDINALITY) {
        return;
      }
      const point: MetricPoint = histogram
        ? { name, value, attributes: safe, observations: 1, min: value, max: value }
        : { name, value, attributes: safe };
      series.set(signature, point);
      points.push(point);
    },
    set(name: string, value: number, attributes: Record<string, string> = {}) {
      if (nameHidesSecret(name, secrets)) {
        return;
      }
      if (!(METRIC_NAMES as readonly string[]).includes(name) && !name.startsWith("axond.ext.")) {
        throw new Error(`metric ${name} is not in the catalogue`);
      }
      const safe = sanitizeAttributes(attributes, secrets);
      const signature = `${name}\0${JSON.stringify(safe)}`;
      const existing = series.get(signature);
      if (existing) {
        existing.value = value;
        return;
      }
      if (series.size >= MAX_CARDINALITY) {
        return;
      }
      const point: MetricPoint = { name, value, attributes: safe };
      series.set(signature, point);
      points.push(point);
    },
  };
}
