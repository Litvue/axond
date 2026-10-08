import type { AxondExtension, SqlValue } from "@axond/sdk";

export interface RateLimitOptions {
  /** Requests allowed per window. */
  limit: number;
  windowMs: number;
  /** `isolate` is a soft per-process counter. `store` is a strict shared counter. */
  mode?: "isolate" | "store";
}

type Counters = Map<string, { count: number; resetAt: number }>;
let resetGeneration = 0;

/**
 * pre-dispatch limiter. Isolate mode is per process and can over-admit across
 * replicas. Store mode takes one slot with a single upsert, so two Postgres
 * connections cannot both admit the last request.
 */
export function rateLimitExtension(options: RateLimitOptions): AxondExtension {
  if (!Number.isSafeInteger(options.limit) || options.limit < 0 || !Number.isSafeInteger(options.windowMs) || options.windowMs <= 0) {
    throw new Error("rate limit requires a nonnegative integer limit and positive integer windowMs");
  }
  const counters: Counters = new Map();
  let generation = resetGeneration;
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
      if (generation !== resetGeneration) { counters.clear(); generation = resetGeneration; }
      const axond = c.get("axond");
      const namespace = axond.namespace?.id ?? "anonymous";
      const allowed =
        options.limit > 0 && (mode === "store" ? await storeAllow(axond.store, namespace, options) : isolateAllow(counters, namespace, options));
      if (!allowed) {
        return c.json({ error: { type: "rate_limited", message: "rate limit exceeded" } }, 429);
      }
      await next();
    },
  };
}

function isolateAllow(counters: Counters, namespace: string, options: RateLimitOptions): boolean {
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
  await store.query("DELETE FROM axond_ext_ratelimit_window WHERE namespace = ? AND CAST(bucket AS BIGINT) < CAST(? AS BIGINT)", [namespace, bucket]);
  const taken = await store.query(
    `INSERT INTO axond_ext_ratelimit_window (namespace, bucket, count)
     VALUES (?, ?, 1)
     ON CONFLICT (namespace, bucket) DO UPDATE
     SET count = axond_ext_ratelimit_window.count + 1
     WHERE axond_ext_ratelimit_window.count < ?
     RETURNING count`,
    [namespace, bucket, options.limit],
  );
  return taken.rows.length > 0;
}

/** Test helper. Isolate counters survive the process, so tests reset them. */
export function resetIsolateCounters(): void {
  resetGeneration += 1;
}
