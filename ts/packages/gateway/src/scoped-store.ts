import type { ExtensionStore, QueryResult, SqlValue, Store } from "@axond/sdk";

import { GatewayFailure } from "./errors.ts";

/**
 * An untrusted extension may query only rows for the request namespace.
 * Only a simple SELECT from the namespace table or extension tables with an
 * exact namespace predicate is supported; arbitrary SQL requires trusted code.
 * Rows whose `namespace`
 * or `id` column names some other namespace are dropped, so a predicate the
 * extension writes cannot widen the result.
 */
export function scopeStore(store: Store, namespace: string): ExtensionStore {
  return {
    async query(sql: string, params: readonly SqlValue[] = []): Promise<QueryResult> {
      const match = /^\s*SELECT\s+(\*|[a-z_][a-z0-9_]*(?:\s*,\s*[a-z_][a-z0-9_]*)*)\s+FROM\s+(axond_namespace|axond_ext_[a-z0-9_]+)\s+WHERE\s+(id|namespace)\s*=\s*(\?|\$1)\s*;?\s*$/i.exec(sql);
      const table = match?.[2]?.toLowerCase();
      const column = match?.[3]?.toLowerCase();
      if (namespace.length === 0 || !match || params.length !== 1 || params[0] !== namespace ||
          column !== (table === "axond_namespace" ? "id" : "namespace")) {
        throw new GatewayFailure("namespace_not_authorized", 403,
          "the authenticated grant does not authorize the selected namespace");
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
