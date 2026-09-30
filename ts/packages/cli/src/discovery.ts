import type {
  CatalogueImportLog,
  CredentialConfig,
  ProviderConfig,
  ProviderDiscoveryLog,
  Store,
} from "@axond/sdk";

type DiscoveryLog = CatalogueImportLog | ProviderDiscoveryLog;

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

/** Used only when the durable streak write fails. */
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
  onLog?: (record: DiscoveryLog) => void;
}): Promise<void> {
  const fetchImpl = input.fetchImpl ?? fetch;
  for (const provider of input.providers) {
    await refreshProvider(input, fetchImpl, provider);
  }
  if (input.catalog.source === "models-dev" && input.catalog.sourceUrl) {
    await refreshCatalog(input, fetchImpl, input.catalog.sourceUrl);
  }
}

async function refreshProvider(
  input: {
    store: Store;
    credentials: readonly CredentialConfig[];
    onLog?: (record: DiscoveryLog) => void;
  },
  fetchImpl: typeof fetch,
  provider: ProviderConfig,
): Promise<void> {
  const credential = input.credentials.find((item) => item.provider === provider.id);
  if (!credential) {
    noteProviderDiscovery(input.onLog, provider.id, "no_credential");
    try {
      const existing = await input.store.getProviderModels(provider.id);
      if (existing) {
        await input.store.markProviderModelsStale(provider.id);
      }
    } catch {
      noteProviderDiscovery(input.onLog, provider.id, "not_retained");
    }
    return;
  }
  const headers = new Headers();
  if (provider.kind === "anthropic") {
    headers.set("x-api-key", credential.secret);
    headers.set("anthropic-version", "2023-06-01");
  } else {
    headers.set("authorization", `Bearer ${credential.secret}`);
  }
  const url = `${provider.baseUrl.replace(/\/$/, "")}/models`;
  let response: Response;
  try {
    response = await fetchImpl(url, { headers });
  } catch {
    noteProviderDiscovery(input.onLog, provider.id, "unreachable");
    await markProviderStale(input, provider.id);
    return;
  }
  if (!response.ok) {
    const reason = response.status === 401 || response.status === 403 ? "denied" : "unreachable";
    noteProviderDiscovery(input.onLog, provider.id, reason);
    await markProviderStale(input, provider.id);
    return;
  }
  let body: unknown;
  try {
    body = await response.json();
  } catch {
    noteProviderDiscovery(input.onLog, provider.id, "not_json");
    await markProviderStale(input, provider.id);
    return;
  }
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    noteProviderDiscovery(input.onLog, provider.id, "not_json");
    await markProviderStale(input, provider.id);
    return;
  }
  const data = Array.isArray((body as { data?: unknown }).data) ? (body as { data: unknown[] }).data : [];
  try {
    await input.store.upsertProviderModels({
      provider: provider.id,
      fetchedAt: new Date().toISOString(),
      stale: false,
      data,
      source: provider.baseUrl,
    });
  } catch {
    noteProviderDiscovery(input.onLog, provider.id, "not_retained");
    await markProviderStale(input, provider.id);
  }
}

function noteProviderDiscovery(
  onLog: ((record: DiscoveryLog) => void) | undefined,
  provider: string,
  reason: ProviderDiscoveryLog["reason"],
): void {
  onLog?.({ msg: "provider_discovery", provider, reason });
}

async function markProviderStale(
  input: { store: Store; onLog?: (record: DiscoveryLog) => void },
  provider: string,
): Promise<void> {
  try {
    await input.store.markProviderModelsStale(provider);
  } catch {
    noteProviderDiscovery(input.onLog, provider, "not_retained");
  }
}

async function refreshCatalog(
  input: { store: Store; metrics?: CatalogMetrics; onLog?: (record: DiscoveryLog) => void },
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
  const reset = await input.store.resetCatalogStreak().then(
    () => 0,
    () => {
      catalogStreaks.set(input.store, 0);
      return 0;
    },
  );
  catalogStreaks.set(input.store, reset);
  input.metrics?.set("axond.catalog.consecutive_refusals", reset);
  input.onLog?.({ msg: "catalogue_import", outcome: "admitted", reason: "", consecutive_refusals: reset });
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
  input: { store: Store; metrics?: CatalogMetrics; onLog?: (record: DiscoveryLog) => void },
  reason: CatalogRefusalReason,
): Promise<void> {
  let next: number;
  try {
    next = await input.store.noteCatalogRefusal();
  } catch {
    next = (catalogStreaks.get(input.store) ?? 0) + 1;
  }
  catalogStreaks.set(input.store, next);
  input.metrics?.record("axond.catalog.refusals", 1, { "axond.catalog.reason": reason });
  input.onLog?.({ msg: "catalogue_import", outcome: "refused", reason, consecutive_refusals: next });
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
  onLog?: (record: DiscoveryLog) => void;
}): () => void {
  const run = () => {
    void discoverOnce(input);
  };
  const timer = setInterval(run, input.intervalSeconds * 1000);
  timer.unref?.();
  return () => clearInterval(timer);
}
