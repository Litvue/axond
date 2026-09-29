/**
 * Store timing series from `crates/gateway/src/telemetry/catalog.rs`.
 * Labels stay inside the closed backend, operation, and outcome vocabularies.
 */

export interface StoreMetrics {
  record(name: string, value: number, attributes?: Record<string, string>): void;
}

/** Catalogue values of `axond.store.operation`. */
export type StoreOperation =
  | "namespace_resolve"
  | "namespace_read"
  | "namespace_write"
  | "budget_read"
  | "budget_write"
  | "budget_admit"
  | "budget_charge"
  | "usage_append"
  | "usage_summary"
  | "provider_models";

export function recordStoreCall(
  metrics: StoreMetrics | undefined,
  backend: "sqlite" | "postgres",
  operation: StoreOperation | null,
  acquireWaitMs: number,
  executionMs: number | null,
  outcome: "ok" | "error" | "saturated",
): void {
  if (!metrics || operation === null) {
    return;
  }
  const dimensions = {
    "axond.store.backend": backend,
    "axond.store.operation": operation,
  };
  metrics.record("axond.store.acquire_wait", Math.max(0, acquireWaitMs), { ...dimensions });
  if (executionMs !== null) {
    metrics.record("axond.store.query_duration", Math.max(0, executionMs), { ...dimensions });
  }
  metrics.record("axond.store.operations", 1, {
    ...dimensions,
    "axond.store.outcome": outcome,
  });
}

export function recordConnectionOpened(metrics: StoreMetrics | undefined): void {
  metrics?.record("axond.store.connections_opened", 1, { "axond.store.backend": "postgres" });
}

export function recordConnectionDiscarded(metrics: StoreMetrics | undefined): void {
  metrics?.record("axond.store.connections_discarded", 1, { "axond.store.backend": "postgres" });
}
