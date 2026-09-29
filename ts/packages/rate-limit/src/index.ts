import type { AxondExtension, SqlValue } from "@axond/sdk";

export interface RateLimitOptions {
  /** Requests allowed per window. */
  limit: number;
  windowMs: number;
  /** `isolate` is a soft per-process counter. `store` is a strict shared counter. */
  mode?: "isolate" | "store";
}

const counters = new Map<string, { count: number; resetAt: number }>();

/**
 * pre-dispatch limiter. Isolate mode is per process and can over-admit across
 * replicas. Store mode updates an extension-owned table and is as consistent
 * as that table's transaction.
 */
export function rateLimitExtension(options: RateLimitOptions): AxondExtension {
  const mode = options.mode ?? "isolate";
  return {
    name: "ratelimit",
    apiVersion: 1,
    stage: "pre-dispatch",
    trusted: mode === "store",
    migrations:
      mode === "store"
        ? [
            `CREATE TABLE IF NOT EXISTS axond_ext_ratelimit_window (
              namespace TEXT NOT NULL,
              bucket TEXT NOT NULL,
              count INTEGER NOT NULL,
              PRIMARY KEY (namespace, bucket)
            )`,
          ]
        : [],
    async middleware(c, next) {
      const axond = c.get("axond");
      const namespace = axond.namespace?.id ?? "anonymous";
      const allowed =
        mode === "store" ? await storeAllow(axond.store, namespace, options) : isolateAllow(namespace, options);
      if (!allowed) {
        return c.json({ error: { type: "rate_limited", message: "rate limit exceeded" } }, 429);
      }
      await next();
    },
  };
}

function isolateAllow(namespace: string, options: RateLimitOptions): boolean {
  const now = Date.now();
  const slot = counters.get(namespace);
  if (!slot || slot.resetAt <= now) {
    counters.set(namespace, { count: 1, resetAt: now + options.windowMs });
    return true;
  }
  if (slot.count >= options.limit) {
    return false;
  }
  slot.count += 1;
  return true;
}

async function storeAllow(
  store: { query(sql: string, params?: readonly SqlValue[]): Promise<{ rows: Record<string, unknown>[] }> },
  namespace: string,
  options: RateLimitOptions,
): Promise<boolean> {
  const bucket = String(Math.floor(Date.now() / options.windowMs));
  const existing = await store.query(
    "SELECT count FROM axond_ext_ratelimit_window WHERE namespace = ? AND bucket = ?",
    [namespace, bucket],
  );
  const count = existing.rows[0] ? Number(existing.rows[0]["count"]) : 0;
  if (count >= options.limit) {
    return false;
  }
  if (count === 0) {
    await store.query(
      "INSERT INTO axond_ext_ratelimit_window (namespace, bucket, count) VALUES (?, ?, 1)",
      [namespace, bucket],
    );
  } else {
    await store.query(
      "UPDATE axond_ext_ratelimit_window SET count = count + 1 WHERE namespace = ? AND bucket = ?",
      [namespace, bucket],
    );
  }
  return true;
}

/** Test helper. Isolate counters survive the process, so tests reset them. */
export function resetIsolateCounters(): void {
  counters.clear();
}
