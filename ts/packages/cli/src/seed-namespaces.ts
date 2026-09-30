import type { Store } from "@axond/sdk";

import { namespaceIdMessage } from "../../gateway/src/namespace.ts";

/**
 * Insert TOML `[[namespace]]` rows. An id the identifier rules refuse is left
 * out of the store, matching `seed_config_namespaces`. A restart applies the
 * file's current `allow_platform_fallback` and leaves attrs in place. A row
 * that left the file inherits platform credentials.
 */
export async function seedConfigNamespaces(
  store: Store,
  namespaces: readonly { id: string; allowPlatformFallback: boolean }[],
): Promise<void> {
  const configured = new Set<string>();
  for (const namespace of namespaces) {
    if (namespaceIdMessage(namespace.id) !== null) {
      continue;
    }
    configured.add(namespace.id);
    await store.adoptConfigNamespace(namespace.id, namespace.allowPlatformFallback);
  }
  let cursor: string | null = null;
  do {
    const page = await store.listNamespaces(cursor, 1000);
    for (const row of page.data) {
      if (row.fromConfig && !configured.has(row.id)) {
        await store.releaseConfigNamespace(row.id);
      }
    }
    cursor = page.nextCursor;
  } while (cursor !== null);
}
