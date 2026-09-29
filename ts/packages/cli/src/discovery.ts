import type { CredentialConfig, ProviderConfig, Store } from "@axond/sdk";

export interface CatalogConfig {
  source: "none" | "models-dev" | "seed";
  sourceUrl?: string | null;
}

/** Closed `axond.catalog.reason` values this importer can actually observe. */
export type CatalogRefusalReason = "unreachable" | "denied" | "unsupported_endpoint" | "not_json" | "not_retained";

export interface CatalogMetrics {
  record(name: string, value: number, attributes?: Record<string, string>): void;
  set(name: string, value: number, attributes?: Record<string, string>): void;
}

/** One store is one catalogue holder. The count is not durable across processes. */
const catalogStreaks = new WeakMap<object, number>();

/**
 * Refresh provider `/models` caches and, when configured, a catalogue URL.
 * Failures mark the row stale and leave the previous payload in place.
 */
export async function discoverOnce(input: {
  store: Store;
  providers: readonly ProviderConfig[];
  credentials: readonly CredentialConfig[];
  catalog: CatalogConfig;
  fetchImpl?: typeof fetch;
  metrics?: CatalogMetrics;
}): Promise<void> {
  const fetchImpl = input.fetchImpl ?? fetch;
  for (const provider of input.providers) {
    const credential = input.credentials.find((item) => item.provider === provider.id);
    if (!credential) {
      continue;
    }
    const headers = new Headers();
    if (provider.kind === "anthropic") {
      headers.set("x-api-key", credential.secret);
      headers.set("anthropic-version", "2023-06-01");
    } else {
      headers.set("authorization", `Bearer ${credential.secret}`);
    }
    try {
      const response = await fetchImpl(`${provider.baseUrl.replace(/\/$/, "")}/models`, { headers });
      if (!response.ok) {
        await input.store.markProviderModelsStale(provider.id);
        continue;
      }
      const body = (await response.json()) as { data?: unknown };
      const data = Array.isArray(body.data) ? body.data : [];
      await input.store.upsertProviderModels({
        provider: provider.id,
        fetchedAt: new Date().toISOString(),
        stale: false,
        data,
        source: provider.baseUrl,
      });
    } catch {
      await input.store.markProviderModelsStale(provider.id);
    }
  }
  if (input.catalog.source === "models-dev" && input.catalog.sourceUrl) {
    await refreshCatalog(input, fetchImpl, input.catalog.sourceUrl);
  }
}

async function refreshCatalog(
  input: { store: Store; metrics?: CatalogMetrics },
  fetchImpl: typeof fetch,
  sourceUrl: string,
): Promise<void> {
  let response: Response;
  try {
    response = await fetchImpl(sourceUrl);
  } catch {
    await input.store.markProviderModelsStale("catalog").catch(() => undefined);
    await noteCatalogRefusal(input, "unreachable");
    return;
  }
  if (!response.ok) {
    await input.store.markProviderModelsStale("catalog").catch(() => undefined);
    await noteCatalogRefusal(input, catalogStatusReason(response.status));
    return;
  }
  let body: unknown;
  try {
    body = await response.json();
  } catch {
    await input.store.markProviderModelsStale("catalog").catch(() => undefined);
    await noteCatalogRefusal(input, "not_json");
    return;
  }
  const fetchedAt = new Date().toISOString();
  try {
    await input.store.upsertProviderModels({
      provider: "catalog",
      fetchedAt,
      stale: false,
      data: catalogModels(body),
      source: sourceUrl,
    });
  } catch {
    await noteCatalogRefusal(input, "not_retained");
    return;
  }
  catalogStreaks.set(input.store, 0);
  input.metrics?.set("axond.catalog.consecutive_refusals", 0);
  const age = Date.now() - Date.parse(fetchedAt);
  if (Number.isFinite(age)) {
    input.metrics?.set("axond.catalog.active_age", Math.max(0, age));
  }
}

function catalogStatusReason(status: number): CatalogRefusalReason {
  if (status === 401 || status === 403) {
    return "denied";
  }
  if (status >= 300 && status <= 499 && status !== 408 && status !== 429) {
    return "unsupported_endpoint";
  }
  return "unreachable";
}

async function noteCatalogRefusal(
  input: { store: Store; metrics?: CatalogMetrics },
  reason: CatalogRefusalReason,
): Promise<void> {
  const next = (catalogStreaks.get(input.store) ?? 0) + 1;
  catalogStreaks.set(input.store, next);
  input.metrics?.record("axond.catalog.refusals", 1, { "axond.catalog.reason": reason });
  input.metrics?.set("axond.catalog.consecutive_refusals", next);
  try {
    const row = await input.store.getProviderModels("catalog");
    if (!row?.fetchedAt) {
      return;
    }
    const age = Date.now() - Date.parse(row.fetchedAt);
    if (Number.isFinite(age)) {
      input.metrics?.set("axond.catalog.active_age", Math.max(0, age));
    }
  } catch {
    // A store that cannot be read leaves the age unpublished.
  }
}

function catalogModels(body: unknown): unknown[] {
  if (Array.isArray(body)) {
    return body;
  }
  if (body && typeof body === "object" && Array.isArray((body as { data?: unknown }).data)) {
    return (body as { data: unknown[] }).data;
  }
  if (body && typeof body === "object") {
    return Object.keys(body as Record<string, unknown>).map((id) => ({ id }));
  }
  return [];
}

/** Background loop. The timer does not keep a CLI process alive. */
export function startDiscovery(input: {
  store: Store;
  providers: readonly ProviderConfig[];
  credentials: readonly CredentialConfig[];
  catalog: CatalogConfig;
  intervalSeconds: number;
  metrics?: CatalogMetrics;
}): () => void {
  const run = () => {
    void discoverOnce(input);
  };
  const timer = setInterval(run, input.intervalSeconds * 1000);
  timer.unref?.();
  return () => clearInterval(timer);
}
