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
 * A provider or catalogue that accepts the socket and never finishes the body
 * would hold the Worker cron until the runtime's wall limit. 30s matches the
 * request path's budget for provider response headers.
 */
const DISCOVERY_FETCH_TIMEOUT_MS = 30_000;

function discoveryAbort(): { signal: AbortSignal; done: () => void } {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), DISCOVERY_FETCH_TIMEOUT_MS);
  return { signal: controller.signal, done: () => clearTimeout(timer) };
}

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
   * round can replace it. Otherwise a fresh foreign-source row is retained.
   */
  replaceForeignSource?: boolean;
  fetchImpl?: typeof fetch;
  metrics?: CatalogMetrics;
  onLog?: (record: DiscoveryLog) => void;
}): Promise<void> {
  const fetchImpl = input.fetchImpl ?? fetch;
  const allowReplace = input.replaceForeignSource === true;
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
    const deadline = discoveryAbort();
    let response: Response;
    try {
      try {
        response = await fetchImpl(url, { headers, redirect: "manual", signal: deadline.signal });
      } catch {
        return { ok: false, reason: "unreachable" };
      }
      if (!response.ok) {
        await response.body?.cancel().catch(() => undefined);
        return { ok: false, reason: response.status === 401 || response.status === 403 ? "denied" : "unreachable" };
      }
      let body: unknown;
      try {
        body = await response.json();
      } catch (error) {
        return { ok: false, reason: deadline.signal.aborted || !(error instanceof SyntaxError) ? "unreachable" : "not_json" };
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
    } finally {
      deadline.done();
    }
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
  const deadline = discoveryAbort();
  let response: Response;
  let body: unknown;
  try {
    try {
      response = await fetchImpl(sourceUrl, { redirect: "manual", signal: deadline.signal });
    } catch {
      await input.store.markProviderModelsStale("catalog").catch(() => undefined);
      await noteCatalogRefusal(input, "unreachable");
      return;
    }
    if (!response.ok) {
      await response.body?.cancel().catch(() => undefined);
      await input.store.markProviderModelsStale("catalog").catch(() => undefined);
      await noteCatalogRefusal(input, catalogStatusReason(response.status));
      return;
    }
    try {
      body = await response.json();
    } catch (error) {
      await input.store.markProviderModelsStale("catalog").catch(() => undefined);
      await noteCatalogRefusal(input, deadline.signal.aborted || !(error instanceof SyntaxError) ? "unreachable" : "not_json");
      return;
    }
  } finally {
    deadline.done();
  }
  let data: unknown[];
  try { data = catalogModels(body); } catch { await noteCatalogRefusal(input, "not_json"); return; }
  const fetchedAt = new Date().toISOString();
  try {
    await input.store.upsertProviderModels({
      provider: "catalog",
      fetchedAt,
      stale: false,
      data,
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

export async function noteCatalogRefusal(
  input: { store: Store; metrics?: CatalogMetrics; onLog?: (record: DiscoveryLog) => void },
  reason: CatalogRefusalReason,
): Promise<void> {
  await input.store.markProviderModelsStale("catalog").catch(() => undefined);
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
  if (body !== null && typeof body === "object" && !Array.isArray(body) && "models" in body && "providers" in body) {
    const catalog = body as { models: unknown; providers: unknown };
    if (catalog.models === null || typeof catalog.models !== "object" || Array.isArray(catalog.models) || catalog.providers === null || typeof catalog.providers !== "object" || Array.isArray(catalog.providers)) throw new Error("invalid catalogue schema");
    const entries = Object.entries(catalog.models);
    if (!entries.every(([id, row]) => /^[^/]+\/[^/]+$/.test(id) && row !== null && typeof row === "object" && !Array.isArray(row) && typeof (row as { id?: unknown }).id === "string" && (row as { id: string }).id === id)) throw new Error("invalid catalogue schema");
    if (!Object.values(catalog.providers).every(row => row !== null && typeof row === "object" && !Array.isArray(row))) throw new Error("invalid catalogue schema");
    return entries.map(([, row]) => row);
  }

  const models = Array.isArray(body) ? body : body && typeof body === "object" && Array.isArray((body as { data?: unknown }).data) ? (body as { data: unknown[] }).data : null;
  if (models) {
    if (models.every((row) => row !== null && typeof row === "object" && typeof (row as { id?: unknown }).id === "string")) return models;
    throw new Error("invalid catalogue schema");
  }
  if (body !== null && typeof body === "object" && Object.entries(body).every(([id, row]) => /^[^/]+\/[^/]+$/.test(id) && row !== null && typeof row === "object" && !Array.isArray(row))) {
    return Object.keys(body).map((id) => ({ id }));
  }
  if (body !== null && typeof body === "object" && Object.values(body).every((row) => row !== null && typeof row === "object" && !Array.isArray(row) && typeof (row as { models?: unknown }).models === "object" && (row as { models?: unknown }).models !== null)) {
    return Object.keys(body).map((id) => ({ id }));
  }
  throw new Error("invalid catalogue schema");
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
  let running = false;
  let stopped = false;
  let initial = true;
  const run = () => {
    if (running || stopped) return;
    running = true;
    const replaceForeignSource = input.replaceForeignSource ?? (initial && input.providers.length > 0);
    if (input.providers.length > 0) initial = false;
    void discoverOnce({ ...input, replaceForeignSource }).catch(() => undefined).finally(() => { running = false; });
  };
  run();
  const timer = setInterval(run, input.intervalSeconds * 1000);
  timer.unref?.();
  return () => { stopped = true; clearInterval(timer); };
}
