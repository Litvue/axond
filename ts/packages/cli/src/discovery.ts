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

/** Anthropic's default page is 20; twenty pages is the hard ceiling. */
const MAX_MODEL_PAGES = 20;

/**
 * The first round in this process may replace a row fetched from another
 * base URL. Later rounds leave a fresh foreign row alone.
 */
let replaceForeignSource = true;

/**
 * Refresh provider `/models` caches and, when configured, a catalogue URL.
 * Failures mark the row stale and leave the previous payload in place.
 */
export async function discoverOnce(input: {
  store: Store;
  providers: readonly ProviderConfig[];
  credentials: readonly CredentialConfig[];
  catalog: CatalogConfig;
  /** Namespace whose pool is tried first. Defaults to `platform`. */
  platformNamespace?: string;
  /**
   * When true, a row fetched from another base URL is marked stale so this
   * round can replace it. When omitted, only the first round in the process does.
   */
  replaceForeignSource?: boolean;
  fetchImpl?: typeof fetch;
  metrics?: CatalogMetrics;
  onLog?: (record: DiscoveryLog) => void;
}): Promise<void> {
  const fetchImpl = input.fetchImpl ?? fetch;
  const allowReplace = input.replaceForeignSource ?? replaceForeignSource;
  replaceForeignSource = false;
  for (const provider of input.providers) {
    await refreshProvider(input, fetchImpl, provider, allowReplace);
  }
  if (input.catalog.source === "models-dev" && input.catalog.sourceUrl) {
    await refreshCatalog(input, fetchImpl, input.catalog.sourceUrl);
  }
}

type ListingReason = "unreachable" | "denied" | "not_json" | "page_bound";

function discoveryCredentials(
  credentials: readonly CredentialConfig[],
  providerId: string,
  platformNamespace: string,
): CredentialConfig[] {
  const forProvider = credentials.filter((item) => item.provider === providerId);
  const platform = forProvider.filter((item) => item.namespace === platformNamespace);
  if (platform.length > 0) {
    return platform;
  }
  const namespaces = [...new Set(forProvider.map((item) => item.namespace))].sort();
  const first = namespaces[0];
  if (!first) {
    return [];
  }
  return forProvider.filter((item) => item.namespace === first);
}

async function refreshProvider(
  input: {
    store: Store;
    credentials: readonly CredentialConfig[];
    platformNamespace?: string;
    onLog?: (record: DiscoveryLog) => void;
  },
  fetchImpl: typeof fetch,
  provider: ProviderConfig,
  allowReplace: boolean,
): Promise<void> {
  const credentials = discoveryCredentials(input.credentials, provider.id, input.platformNamespace ?? "platform");
  if (credentials.length === 0) {
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
  if (!(await prepareForeignSource(input, provider, allowReplace))) {
    return;
  }
  let last: ListingReason | null = null;
  for (const credential of credentials) {
    const listing = await fetchListing(fetchImpl, provider, credential);
    if (!listing.ok) {
      last = listing.reason;
      continue;
    }
    try {
      await input.store.upsertProviderModels({
        provider: provider.id,
        fetchedAt: new Date().toISOString(),
        stale: false,
        data: listing.data,
        source: provider.baseUrl,
      });
    } catch {
      noteProviderDiscovery(input.onLog, provider.id, "not_retained");
      await markProviderStale(input, provider.id);
    }
    return;
  }
  if (last) {
    noteProviderDiscovery(input.onLog, provider.id, last);
    await markProviderStale(input, provider.id);
  }
}

/** False when this round must leave a fresh foreign row untouched. */
async function prepareForeignSource(
  input: { store: Store; onLog?: (record: DiscoveryLog) => void },
  provider: ProviderConfig,
  allowReplace: boolean,
): Promise<boolean> {
  let existing;
  try {
    existing = await input.store.getProviderModels(provider.id);
  } catch {
    return true;
  }
  if (!existing || existing.stale || existing.source === null || existing.source === provider.baseUrl) {
    return true;
  }
  if (!allowReplace) {
    return false;
  }
  try {
    await input.store.markProviderModelsStale(provider.id);
  } catch {
    noteProviderDiscovery(input.onLog, provider.id, "not_retained");
    return false;
  }
  return true;
}

async function fetchListing(
  fetchImpl: typeof fetch,
  provider: ProviderConfig,
  credential: CredentialConfig,
): Promise<{ ok: true; data: unknown[] } | { ok: false; reason: ListingReason }> {
  const headers = new Headers();
  if (provider.kind === "anthropic") {
    headers.set("x-api-key", credential.secret);
    headers.set("anthropic-version", "2023-06-01");
  } else {
    headers.set("authorization", `Bearer ${credential.secret}`);
  }
  const base = provider.baseUrl.replace(/\/$/, "");
  const data: unknown[] = [];
  let after: string | null = null;
  for (let page = 0; page < MAX_MODEL_PAGES; page += 1) {
    const url = after === null ? `${base}/models` : `${base}/models?after_id=${encodeQueryComponent(after)}`;
    let response: Response;
    try {
      response = await fetchImpl(url, { headers });
    } catch {
      return { ok: false, reason: "unreachable" };
    }
    if (!response.ok) {
      return { ok: false, reason: response.status === 401 || response.status === 403 ? "denied" : "unreachable" };
    }
    let body: unknown;
    try {
      body = await response.json();
    } catch {
      return { ok: false, reason: "not_json" };
    }
    const parsed = parsePage(body);
    if (!parsed) {
      return { ok: false, reason: "not_json" };
    }
    data.push(...parsed.data);
    if (parsed.nextAfter === null) {
      return { ok: true, data };
    }
    if (page + 1 === MAX_MODEL_PAGES) {
      return { ok: false, reason: "page_bound" };
    }
    after = parsed.nextAfter;
  }
  return { ok: false, reason: "page_bound" };
}

function parsePage(body: unknown): { data: unknown[]; nextAfter: string | null } | null {
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    return null;
  }
  const record = body as { data?: unknown; has_more?: unknown; last_id?: unknown };
  if (!Array.isArray(record.data)) {
    return null;
  }
  const data = record.data.filter(
    (item) => item !== null && typeof item === "object" && !Array.isArray(item) && typeof (item as { id?: unknown }).id === "string",
  );
  if (record.has_more !== true) {
    return { data, nextAfter: null };
  }
  if (typeof record.last_id !== "string" || record.last_id.length === 0) {
    return null;
  }
  return { data, nextAfter: record.last_id };
}

function encodeQueryComponent(value: string): string {
  let out = "";
  for (const byte of new TextEncoder().encode(value)) {
    const unreserved =
      (byte >= 0x41 && byte <= 0x5a) ||
      (byte >= 0x61 && byte <= 0x7a) ||
      (byte >= 0x30 && byte <= 0x39) ||
      byte === 0x2d ||
      byte === 0x5f ||
      byte === 0x2e ||
      byte === 0x7e;
    out += unreserved ? String.fromCharCode(byte) : `%${byte.toString(16).toUpperCase().padStart(2, "0")}`;
  }
  return out;
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
  platformNamespace?: string;
  replaceForeignSource?: boolean;
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
