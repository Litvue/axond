import type { Store } from "@axond/sdk";

import { namespaceIdMessage } from "../../gateway/src/namespace.ts";

/**
 * Insert TOML `[[namespace]]` rows. An id the identifier rules refuse is left
 * out of the store, matching `seed_config_namespaces`. A duplicate insert is
 * ignored so a restart against an existing file still boots.
 */
export async function seedConfigNamespaces(
  store: Store,
  namespaces: readonly { id: string; allowPlatformFallback: boolean }[],
): Promise<void> {
  for (const namespace of namespaces) {
    if (namespaceIdMessage(namespace.id) !== null) {
      continue;
    }
    await store.putNamespace({
      id: namespace.id,
      attrs: {},
      blocklist: null,
      allowPlatformFallback: namespace.allowPlatformFallback,
      fromConfig: true,
    });
  }
}
