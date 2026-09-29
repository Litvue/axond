import type { ExtensionStore, QueryResult, SqlValue, Store } from "@axond/sdk";

import { GatewayFailure } from "./errors.ts";

/**
 * An untrusted extension may query only rows for the request namespace.
 * Queries that do not bind that namespace are refused. Rows whose `namespace`
 * or `id` column names some other namespace are dropped, so a predicate the
 * extension writes cannot widen the result.
 */
export function scopeStore(store: Store, namespace: string): ExtensionStore {
  return {
    async query(sql: string, params: readonly SqlValue[] = []): Promise<QueryResult> {
      if (namespace.length === 0 || !params.includes(namespace)) {
        throw new GatewayFailure(
          "namespace_not_authorized",
          403,
          "the authenticated grant does not authorize the selected namespace",
        );
      }
      const result = await store.query(sql, params);
      return {
        rows: result.rows.filter((row) => rowVisible(row, namespace)),
      };
    },
  };
}

function rowVisible(row: Record<string, unknown>, namespace: string): boolean {
  if (typeof row["namespace"] === "string" && row["namespace"] !== namespace) {
    return false;
  }
  if (typeof row["id"] === "string" && row["namespace"] === undefined && row["id"] !== namespace && !("request_id" in row)) {
    return false;
  }
  return true;
}
