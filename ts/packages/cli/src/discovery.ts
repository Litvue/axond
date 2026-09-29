import type { CredentialConfig, ProviderConfig, Store } from "@axond/sdk";

export interface CatalogConfig {
  source: "none" | "models-dev" | "seed";
  sourceUrl?: string | null;
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
  fetchImpl?: typeof fetch;
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
    try {
      const response = await fetchImpl(input.catalog.sourceUrl);
      if (!response.ok) {
        await input.store.markProviderModelsStale("catalog");
        return;
      }
      const body: unknown = await response.json();
      await input.store.upsertProviderModels({
        provider: "catalog",
        fetchedAt: new Date().toISOString(),
        stale: false,
        data: catalogModels(body),
        source: input.catalog.sourceUrl,
      });
    } catch {
      await input.store.markProviderModelsStale("catalog");
    }
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
}): () => void {
  const run = () => {
    void discoverOnce(input);
  };
  const timer = setInterval(run, input.intervalSeconds * 1000);
  timer.unref?.();
  return () => clearInterval(timer);
}
