# Deploying Axond from TypeScript: investigation

Date: 2026-09-28. Status: investigation, no decision recorded.

Axond can run inside an existing Hono API, and it can run on a Cloudflare Worker against PlanetScale Postgres through Hyperdrive. It cannot do either as the current Rust binary. The workable route is a TypeScript reimplementation of the ADR 0063 product shape, written against Web-standard APIs so one codebase serves Workers, Bun, and Node. Whole-gateway WASM is a bad fit. WASM for `gateway-core` alone is feasible but buys little. Cloudflare Containers can host the unmodified Rust binary behind a Worker today, but that path cannot use Hyperdrive. The single-binary goal survives through `bun build --compile` (a 95 MiB executable in my test, with SQLite via `bun:sqlite`).

Everything marked **verified** below was executed in this session (rustc 1.97.1, wrangler 4.143.0 with `workerd`, Postgres 16.13, Bun 1.3.11). Cloudflare limits come from Cloudflare's own docs as read by a research pass on 2026-09-28. Anything not fetched from an official page is flagged **unverified**.

## 1. What Axond is at runtime

Facts from the source tree at `2bf9398`.

| Item | Value |
| --- | --- |
| Rust lines, all crates including tests | 229,859 |
| `gateway-core` (runtime-neutral adapters) | 8,946 lines, of which `guardrail.rs` is 4,349 |
| `gateway-transport` | 1,066 lines |
| Request-path core of `crates/gateway` | roughly 25,000 to 30,000 lines |
| Compiled but not driven by the serving path | roughly 40,000 lines (`desired_state`, `convergence`, `availability`, `backends/control_plane`, `backends/secrets`, `budget/{redis,postgres}`, `ops`) |

ADR 0063 withdrew a lot, but the code is mid-refactor. `main.rs` still declares every module. Config still accepts `[rate_limit]` with Redis, `[revocation]`, and the catalogue. `axt1.` tokens are rejected with 401 (`routes/auth.rs:113`). A port targets ADR 0063 and `docs/compatibility.md`, not the module list.

### Production HTTP surface

`/healthz`, `/readyz`, `POST /ns/{ns}/v1/{chat/completions,messages,embeddings,responses}`, `GET /ns/{ns}/v1/{models,credentials}`, and `/api/v1/*` for namespaces, budgets, usage, provider models, and OpenAPI. One static key authenticates everything (`Authorization: Bearer` or `x-api-key`).

### One inference request today

1. Auth by static key, then `Store::resolve_namespace`. This is one SQL join that returns the namespace row and the budget admit decision. In Rust it runs as `BEGIN`, `SELECT`, `COMMIT`, which is 3 sequential round trips (inference from tokio-postgres behavior, `store/postgres.rs:1045-1060`).
2. Parse the body, split `provider/model` on the first `/`, check blocklist globs, look up price.
3. Admit if `spent < limit`. No hold is placed (ADR 0064).
4. Pick a credential from the pool, call upstream, relay the stream. Usage is read from the stream tail.
5. After the response, one `UPDATE axond_store_budget SET spent = spent + $1` charges actual cost (`store/postgres.rs:1699-1726`). Usage rows go to an in-process queue and a batched `INSERT ... ON CONFLICT DO NOTHING`.

Nothing spans an upstream call in a transaction. That is the single most important compatibility fact for a pooled serverless database.

### Cross-request in-process state

| State | Correctness or optimization |
| --- | --- |
| Circuit breakers, credential health, rotation cursor | Optimization only |
| Admission semaphores, in-memory rate limiter | Protective, per replica by design |
| Settlement queue, usage queue | Correctness-sensitive: spend or usage is lost if the process dies first. Already an accepted property (ADR 0064, ADR 0009) |
| Config snapshot | Immutable per process |

No namespace or budget cache exists. Every request reads the Store.

## 2. Probe results

| # | Probe | Result |
| --- | --- | --- |
| P1 | `cargo build -p gateway-core --target wasm32-unknown-unknown` | Fails until `getrandom` gets its `js` feature (pulled in by `ring`). With it, builds. `clang` is required for `ring`. |
| P2 | Size of wasm containing regex, `DeterministicGuardrail::compile`, and `SseDecoder` (opt-level z, LTO) | 971 KB raw, 315 KB gzip |
| P3 | Load that module in `workerd` under Hono and call it | Runs. Guardrail compiled and the SSE decoder parsed. |
| P4 | `CircuitBreaker::allow` in wasm | Traps with `RuntimeError: unreachable`. `std::time::Instant::now()` is unsupported on `wasm32-unknown-unknown`. |
| P5 | Apply the shipped Store DDL (`ops/postgres/store_*.sql` plus inline `axond_namespace`) to Postgres 16 | Applies unmodified. |
| P6 | Real `resolve_namespace` join through `pg` in `workerd` via a Hyperdrive binding | Works in one autocommit query. 10 to 19 ms locally. |
| P7 | 20 concurrent isolated clients each run the real charge `UPDATE` | Final `spent` is exactly 20,000. No lost update. |
| P8 | `pg_advisory_xact_lock` plus `INSERT ... ON CONFLICT DO NOTHING` in one transaction, run twice | Inserts 1 row, then 0. |
| P9 | 100-row batched usage insert, run twice | 100, then 0. |
| P10 | `ctx.waitUntil` write after the response | Response returned first, row landed. |
| P11 | Streamed SSE through Hono in `workerd` | Chunks arrive incrementally. |
| P12 | Single statement that inserts usage and charges only if the insert won (section 4) | 10 concurrent calls with one `request_id` charge once. 10 distinct ids charge 10 times. Total 11,000. |
| P13 | `bun build --compile` of a Hono plus `bun:sqlite` app | 94.7 MiB executable, runs. |

Limits of these probes. Wrangler's local Hyperdrive is a direct TCP proxy. It does not exercise transaction pooling, query caching, or edge latency. Nothing here ran on Cloudflare's network, and no PlanetScale instance was contacted.

## 3. Deployment options

| Option | What runs | Hyperdrive | Work | Gives up |
| --- | --- | --- | --- | --- |
| A. Reverse proxy to the Rust binary | Existing container or VM. Hono does `app.all('/ns/*', proxyToAxond)` | No (binary uses `tokio-postgres` directly) | Days | A second deployment. Not on Workers. |
| B. Rust binary in Cloudflare Containers behind a Worker | Container as a Durable Object, Worker routes to it | Unconfirmed from inside a container (**unverified**). Container likely connects to PlanetScale directly. | Days | Not a Worker. Container billing and cold starts (cold start **unverified**). |
| C. TypeScript rewrite as a Hono app or library | Worker, Bun, or Node | Yes | 8,000 to 12,000 lines of TS (my estimate, see section 5) | A second implementation to keep in step with Rust. |
| D. `gateway-core` as WASM called from TS | TS glue plus a 315 KB gzip module | Yes | Small, but replaces little | Needs a patched `circuit.rs` and a `clang` build step. The module carries its own `ring` HMAC, so it stays synchronous. |
| E. Whole gateway as WASM (`workers-rs`) | Worker | Via `worker` crate sockets, unproven | Near a rewrite of the 25k to 30k line core | Threads, `tokio`, `rusqlite`, OTLP threads, `Instant`, and `axum` all need replacing. Highest risk. |
| F. Rust as a native addon (`napi-rs`) in Node or Bun | Node or Bun process | No, and not on Workers | Medium | Native builds per platform. (**Unverified**, not tested.) |

**Option A** is the fastest route to "inside my existing server" and needs no Axond change. **Option C** is the only route that reaches Worker plus Hyperdrive with Axond's semantics intact. I recommend C, with A as a bridge while it is built.

**Option D** is not worth doing first. In `gateway-core`, only the guardrail (about 2,400 production lines) depends on Rust regex semantics. The rest of the crate is small and ports directly. Section 5 covers the guardrail decision.

**Option E** is a bad idea. It pays the full cost of a rewrite (adapting all I/O) and keeps the Rust build, with worse debuggability.

## 4. Recommended shape: Worker, Hyperdrive, PlanetScale

### Packaging

Export a factory, not a server:

```ts
// @axond/gateway
export interface AxondOptions {
  store: Store;                          // Postgres first, SQLite second
  providers: ProviderConfig[];           // id, kind, baseUrl, credentials
  gatewayKey: string | ((c: Context) => string);
  waitUntil?: (p: Promise<unknown>) => void; // c.executionCtx.waitUntil on Workers
}
export function createAxond(opts: AxondOptions): Hono;

// Existing API
app.route('/', createAxond({ store, providers, gatewayKey: c.env.AXOND_KEY }));
```

Use only `fetch`, `Request`, `Response`, `ReadableStream`, and `crypto.subtle`. Keep `node:` imports out of the core so the same package runs on Workers, Bun, and Node. Two hosting shapes on Cloudflare:

- **Same Worker.** `app.route('/', createAxond(...))` inside the existing Hono Worker.
- **Separate Worker behind a Service Binding.** The API Worker calls `env.AXOND.fetch(req)`. There is no public route, which matches ADR 0063's "private network, one static key" model. My read: this is the better default, because a deploy of Axond cannot break the main API.

### Per-request flow and round trips

| Step | Rust today | TS on Workers |
| --- | --- | --- |
| Resolve namespace and admit | `BEGIN`, `SELECT`, `COMMIT` (3 RTT) | One autocommit `SELECT` (1 RTT). P6 confirms the query runs as-is. |
| Upstream call and stream | reqwest | `fetch` with an `AbortController` for the header, body, and idle timeouts |
| Charge and usage after response | `UPDATE` (1 RTT), plus a queued batch `INSERT` | One statement in `ctx.waitUntil` (1 RTT), below |
| Total DB round trips | 4 sequential, plus a background insert | 2 |

Combined, idempotent settlement (verified in P12):

```sql
WITH ins AS (
  INSERT INTO axond_store_usage
    (request_id, namespace, period, model, status, cost_microdollars, recorded_at)
  VALUES ($1, $2, $3, $4, 'ok', $5::bigint, now())
  ON CONFLICT (request_id) DO NOTHING
  RETURNING 1
)
UPDATE axond_store_budget
SET spent_microdollars = CASE
  WHEN spent_microdollars >= 9223372036854775807 - $5::bigint THEN 9223372036854775807
  ELSE spent_microdollars + $5::bigint END
WHERE namespace = $2 AND period = $3
  AND EXISTS (SELECT 1 FROM ins)
  AND EXISTS (SELECT 1 FROM axond_namespace WHERE id = $2)
  AND COALESCE((SELECT n FROM axond_namespace_incarnation WHERE id = $2), 1) = $6::bigint
```

This fixes a documented weakness. ADR 0064 says the Rust charge is not idempotent, so a retried settlement double-charges. Gating the charge on winning the usage insert makes it exactly-once per `request_id`. It changes `axond_store_usage` from a best-effort index into the charge's idempotency key, so a usage row must exist for every charge. Tradeoff: a usage insert failure now also skips the charge, which errs toward under-billing, the same direction as today's failure mode.

### Hyperdrive and PlanetScale settings

- **Disable Hyperdrive query caching** on the config that serves `resolve_namespace`. Cloudflare caches read queries by default (max age 60 s), and writes do not invalidate the cache. A cached `SELECT` would serve a deleted namespace and a stale `spent` for up to a minute. Create the config with `--caching-disabled`. A second config with caching on is fine for the provider-models listing.
- **One `pg` client per request.** Workers cannot share I/O objects across requests. Hyperdrive pools on the far side, so the connect is cheap.
- **Use unnamed parameterized queries.** `pg` does this by default. Do not set `lock_timeout` or `statement_timeout` with session `SET`, as `store/postgres.rs:152` does. Under transaction pooling the setting does not stick. Use `ALTER ROLE ... SET statement_timeout` or `SET LOCAL` inside a transaction.
- **Schema-qualify table names** rather than relying on `search_path` (the journal and usage sink do rely on it).
- **Advisory locks are safe** because Axond uses only `pg_advisory_xact_lock` (`store/postgres.rs:1597-1602`), scoped to one transaction. LISTEN/NOTIFY appears only in the withdrawn control plane.
- **Port choice (opinion, unverified).** Point Hyperdrive at PlanetScale's direct port 5432, not the PgBouncer port 6432, so two poolers do not stack.
- **Role.** Cloudflare's guide lists `pg_read_all_data` as the minimum for read-only use. Axond writes, so the role needs write grants.
- **Migrations** run out of band on a direct connection (`psql -f`). P5 shows the DDL is idempotent and applies unmodified.
- **Numbers.** `pg` returns `bigint` as strings. Microdollars fit a JS `Number` up to about $9 billion (2^53), but pass values as strings or `BigInt` to keep the SQL's saturation semantics.

### Replacing the background machinery

| Rust component | Replacement |
| --- | --- |
| Usage index worker | None. Written in the settlement statement. |
| Provider model discovery loop | Cron Trigger writing `axond_store_provider_models`. Its source compare-and-set logic ports as SQL. |
| Catalogue import (models.dev) | Cron Trigger, or a build-time snapshot. |
| Status refresher, reload watcher, graceful drain | Not needed. |
| Billing-grade usage journal (ADR 0049, opt-in) | A Cloudflare Queue with `request_id` as the dedup key, or a Cron consumer running the same `SKIP LOCKED` claim transaction. Skip unless billing-grade delivery is required. |
| Circuit breakers, credential cooldown, rotation | Per isolate, in module scope. Safe because they only optimize. |
| Admission and per-tenant load shedding | Cloudflare absorbs concurrency. Per-tenant fairness is lost. Add a Rate Limiting binding or Durable Object only if needed. |

## 5. Port scope and cost

Portable with little risk: route table, model parse, pricing math, `SseDecoder` (445 lines), usage decoders, circuit breaker, failover policy, credential pool, and namespace, budget, and usage SQL. About 4,000 Rust lines of `gateway-core` outside the guardrail port directly.

My estimate is 8,000 to 12,000 lines of TypeScript for the ADR 0063 surface with Postgres and SQLite stores, versus 25,000 to 30,000 Rust lines. This is an inference from module sizes, not a measurement. Most Rust volume is boot, config, telemetry, shutdown, and admission machinery that does not apply to a Worker.

### Hard parts

1. **The guardrail.** `axond.redact` compiles user regexes with `regex_syntax`, rejects empty-match patterns via `minimum_len`, strips captures, merges all rules into one leftmost-first alternation, and depends on the Rust regex engine's linear-time guarantee. JS `RegExp` backtracks (ReDoS), has different Unicode and `\b` behavior, and cannot re-render a parsed pattern. It also does streamed token restore across event boundaries (4096 carries, 1 MiB of keys). Three choices: (a) ship without it, (b) accept a restricted pattern dialect validated in TS, (c) load the guardrail as the 315 KB gzip WASM module from P2. My read: (a) first, then (c), because it is the only option that keeps regex semantics exact.
2. **Byte-faithful passthrough.** Axond rewrites only `model`. `JSON.parse` collapses duplicate keys and loses integers above 2^53. The strict stream parser in Rust rejects duplicate keys (`stream.rs:214-296`). Rewrite `model` with a targeted scan of the raw body instead of a parse and re-serialize.
3. **Settlement on client disconnect.** Rust settles in `Drop`. On Workers this must come from `TransformStream` `cancel` and `flush`, with the charge in `ctx.waitUntil`. Whether Workers reliably signals cancel on client disconnect is **unverified**. Test it on the real edge.
4. **Timeouts.** `fetch` has no separate connect or idle timeout. Build header, body, and per-chunk idle bounds with `AbortController` and timers.
5. **Clock.** `Instant::now()` traps in wasm (P4). In TS use `Date.now()`, which Workers freezes during pure CPU work. Cooldown windows therefore advance only across I/O, which is acceptable.

## 6. Cloudflare constraints that matter

From Cloudflare's docs (research pass, 2026-09-28). Re-verify before committing.

| Constraint | Value | Consequence |
| --- | --- | --- |
| CPU time, Paid | 30 s default, up to 5 min | Streaming is bound by CPU, not wall time. Byte piping is cheap. JSON parse per event is not. |
| CPU time, Free | 10 ms | Too low. Budget for the $5/month Paid plan. |
| Wall time for HTTP | Unbounded while the client stays connected | Long SSE streams are allowed. |
| `ctx.waitUntil` | Up to 30 s after response or disconnect | Enough for one settlement statement. Not enough for a slow outbox flush. |
| Memory | 128 MB per isolate, shared by concurrent requests | Do not buffer large bodies. Rust buffers up to 32 MiB. |
| Subrequests | 10,000 default on Paid | Not a constraint. |
| Simultaneous open connections | 6 waiting for headers | Watch this under a burst on one isolate. **Unverified** whether DB sockets count. |
| Startup | 1 s global scope | Lazy-init anything heavy, including the WASM module. |
| Hyperdrive, Free | 100,000 queries per day | Two queries per request supports about 50,000 requests per day. Use Paid. |
| Hyperdrive queries | Counted per statement, including `BEGIN` and `COMMIT` | Autocommit statements are cheaper. |
| Bundle size | Docs conflict: 64 MiB uncompressed with no compressed cap, versus older 3 MB and 10 MB compressed limits | Confirm before relying on the WASM module. |
| Proxy read timeout | 100 s on Free and Pro (**unverified**, non-official source) | Send SSE keepalive comments every 15 to 30 s. |

## 7. Single-binary and existing-server deployments

- **Bun or Node beside the existing API.** The same package runs with a real connection pool, `setInterval` for background work, and no `waitUntil` limits. This is simpler than Workers, and it is the right host if the existing API already runs on Bun or Node.
- **Single binary.** `bun build --compile` produced a 94.7 MiB executable with an embedded SQLite (P13). This keeps the original goal, at a size cost versus a stripped Rust binary. I did not build the Rust release binary, so I have no size comparison. Deno compile and Node SEA were not tested.
- **SQLite on Workers.** D1 is an alternative store with SQLite semantics and no Hyperdrive. The charge `UPDATE` works there. It does not fit the stated PlanetScale goal, so I did not pursue it.

## 8. Reusing the Rust repo as an oracle

`tests/compat` (Python SDKs), `tests/compat-ts` (the vendors' Node SDKs), and `tests/fixtures` boot a real process from `AXOND_BIN` against a fake upstream and assert wire behavior. A TS port that runs as a process on Bun can target the same lanes through a small launcher that translates the generated TOML config. That gives byte-level conformance evidence without writing new suites. Workers-specific behavior (disconnect, `waitUntil`, Hyperdrive) still needs its own tests.

## 9. Risks

| Risk | Likelihood (my read) | Mitigation |
| --- | --- | --- |
| Two implementations drift | High | Conformance lanes in CI, and a decision on whether Rust stays supported |
| Worker disconnect handling loses charges | Medium | Real-edge test before launch. Idempotent settlement (P12) makes retries safe. |
| Hyperdrive caching serves stale budget or a deleted namespace | Certain if left at default | Caching-disabled config |
| Passthrough fidelity regressions from `JSON.parse` | Medium | Targeted `model` rewrite, fixture-based tests |
| Guardrail parity | Only if guardrail is used | Defer, or WASM |
| Hyperdrive pooling surprises not visible in local tests | Low to medium | Staging against PlanetScale before any commitment |

## 10. Recommended plan

1. **Now, no port.** Run the Rust binary behind your Hono API with a proxy route (option A) to get Litvue traffic flowing.
2. **Spike, about one week.** Build the smallest TS slice: auth, `resolve_namespace`, `POST /ns/{ns}/v1/chat/completions` and `/messages` with streaming passthrough, and combined settlement. Deploy to a Worker against a PlanetScale branch through a caching-disabled Hyperdrive config. Answer the three unverified questions (disconnect cancel, connection counting, real round-trip latency) with measurements.
3. **Decide** with those numbers whether to complete the port (management API, budgets, cadence, models listing, credentials, SQLite store) or stay on Rust with option B.
4. **Complete** with the `tests/compat*` lanes as the gate. Record the result as an ADR that supersedes the single-binary framing in ADR 0063 for the TS distribution.

## Open questions

1. Is the guardrail (`axond.redact`) in use for Litvue, or can it ship later?
2. Does Litvue's API already run on Cloudflare Workers, or on Bun or Node? That decides between the same-Worker, Service Binding, and same-process shapes.
3. Should the Rust implementation remain a supported product after a TS port, or become a reference and conformance oracle?
4. Is billing-grade usage delivery (the ADR 0049 journal) required, or is the idempotent usage row in `axond_store_usage` enough?
5. Does the SQLite store need to ship in the TS package, or is Postgres the only supported store for TS?
6. Are per-tenant admission limits required, given that Workers cannot enforce them without a Durable Object or Rate Limiting binding?

## Sources

- Workers limits and pricing: <https://developers.cloudflare.com/workers/platform/limits/>, <https://developers.cloudflare.com/workers/platform/pricing/>
- Hyperdrive pooling, caching, limits, and pricing: <https://developers.cloudflare.com/hyperdrive/concepts/connection-pooling/>, <https://developers.cloudflare.com/hyperdrive/concepts/query-caching/>, <https://developers.cloudflare.com/hyperdrive/platform/limits/>, <https://developers.cloudflare.com/hyperdrive/platform/pricing/>
- Hyperdrive with PlanetScale Postgres: <https://developers.cloudflare.com/hyperdrive/examples/connect-to-postgres/postgres-database-providers/planetscale-postgres/>, <https://planetscale.com/docs/connect/cloudflare>
- Cloudflare Containers GA and pricing: <https://developers.cloudflare.com/changelog/post/2026-04-13-containers-sandbox-ga/>, <https://developers.cloudflare.com/containers/pricing/>
- WASM and Rust on Workers: <https://developers.cloudflare.com/workers/runtime-apis/webassembly/>, <https://developers.cloudflare.com/workers/languages/rust/>
- Bun executables: <https://bun.com/docs/bundler/executables>
- Hono streaming: <https://hono.dev/docs/helpers/streaming>
- Repo: `docs/adr/0063-stateful-only-namespaced-gateway.md`, `docs/adr/0064-charge-actuals-after-response.md`, `crates/gateway/src/store/postgres.rs`, `crates/gateway-core/src/guardrail.rs`, `docs/compatibility.md`
