# Axond in TypeScript: investigation and direction

Date: 2026-09-28, updated 2026-09-29. Status: direction chosen by the maintainer, not yet recorded as an ADR.

The direction is a TypeScript gateway built on Hono, written against Web-standard APIs so one codebase serves Cloudflare Workers, Bun, Node, and a compiled single binary. Every extension is a Hono middleware, so extending the gateway means writing a `.ts` file instead of recompiling Rust. A factory function (`createAxond`) doubles as an SDK: anyone can mount the gateway in their own Hono app, deploy it to a Worker, or containerize it. The Worker path runs against PlanetScale Postgres through Hyperdrive and works without changing the SQL. The single-binary goal survives through `bun build --compile` (94.7 MiB, or 99.3 MB, in my test), and I verified that such a binary loads an extension `.ts` file from disk at runtime without a rebuild. Whole-gateway WASM is a bad fit, and the WASM guardrail idea is dropped because the guardrail turned out to be unreachable in production. The hard problems are now the extension contract (stability, trust, and byte-faithful streaming) rather than the port itself.

## What changed since the first version

- **Direction.** The first version compared options. The maintainer chose the TypeScript and Hono route, with middleware as the only extension mechanism. Sections 4 and 11 are new or rewritten around that.
- **Deletions accepted.** [PR 499](https://github.com/Litvue/axond/pull/499) removes the code ADR 0063 withdrew and code no production path reaches. Section 1 now shows the tree before and after it.
- **Correction: the guardrail was never reachable.** The first version treated `axond.redact` as a live feature and the hardest part of the port. Its config fields were `#[serde(skip)]`, so no operator could enable it. PR 499 deletes it. Redaction becomes an optional extension (section 4.7).
- **Correction: withdrawn config was ignored, not rejected.** The first version said config "still accepts" withdrawn sections. More precisely, `Config` did not deny unknown keys, so a leftover `[rate_limit]` was silently dropped. PR 499 rejects all 16 withdrawn sections at boot.
- **New evidence (P14).** A compiled Bun binary imports an external `.ts` middleware at runtime.

Facts marked **verified** were executed in this session (rustc 1.97.1, wrangler 4.143.0 with `workerd`, Postgres 16.13, Bun 1.3.11). Cloudflare limits come from Cloudflare's docs as read by a research pass on 2026-09-28. Anything not fetched from an official page is flagged **unverified**.

## 1. What Axond is at runtime

| Item | `main` (`2bf9398`) | After PR 499 |
| --- | --- | --- |
| Rust lines, all crates including tests | 229,859 | 88,268 |
| `crates/gateway` (including tests) | not measured | 83,040 |
| `crates/gateway/src` | not measured | 62,337 |
| `gateway-core` | 8,946, of which `guardrail.rs` is 4,349 | 4,162 |
| `gateway-transport` | 1,066 | 1,066 |
| Compiled but not driven by the serving path | roughly 40,000 lines | none |
| Withdrawn config sections | silently ignored | rejected at boot |

A port targets ADR 0063 and `docs/compatibility.md`, not the module list.

### Production HTTP surface

`/healthz`, `/readyz`, `POST /ns/{ns}/v1/{chat/completions,messages,embeddings,responses}`, `GET /ns/{ns}/v1/{models,credentials}`, and `/api/v1/*` for namespaces, budgets, usage, provider models, and OpenAPI. One static key authenticates everything (`Authorization: Bearer` or `x-api-key`).

### One inference request today

1. Auth by static key, then `Store::resolve_namespace`. This is one SQL join that returns the namespace row and the budget admit decision. In Rust it runs as `BEGIN`, `SELECT`, `COMMIT`, which is 3 sequential round trips (inference from tokio-postgres behavior, `store/postgres.rs:1045-1060` on `main`).
2. Parse the body, split `provider/model` on the first `/`, check blocklist globs, look up price.
3. Admit if `spent < limit`. No hold is placed (ADR 0064).
4. Pick a credential from the pool, call upstream, relay the stream. Usage is read from the stream tail.
5. After the response, one `UPDATE axond_store_budget SET spent = spent + $1` charges actual cost (`store/postgres.rs:1699-1726` on `main`). Usage rows go to an in-process queue and a batched `INSERT ... ON CONFLICT DO NOTHING`.

Nothing spans an upstream call in a transaction. That is the single most important compatibility fact for a pooled serverless database.

### Cross-request in-process state

| State | Correctness or optimization |
| --- | --- |
| Circuit breakers, credential health, rotation cursor | Optimization only |
| Admission semaphores | Protective, per replica by design |
| Settlement queue, usage queue | Correctness-sensitive: spend or usage is lost if the process dies first. Already an accepted property (ADR 0064, ADR 0009) |
| Config snapshot | Immutable per process (hot reload is removed) |

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
| P12 | Single statement that inserts usage and charges only if the insert won (section 5) | 10 concurrent calls with one `request_id` charge once. 10 distinct ids charge 10 times. Total 11,000. |
| P13 | `bun build --compile` of a Hono plus `bun:sqlite` app | 94.7 MiB executable, runs. |
| P14 | `bun build --compile` of a Hono app that `import()`s a middleware `.ts` file by path at startup. Run it, edit the file, run the same binary again. | Middleware loads from disk. The edit takes effect with no rebuild (header changed from `v1` to `v2-edited-after-compile`). 99.3 MB executable. |

P1 to P4 and P2 to P3 test the WASM route that section 3 no longer pursues. They are kept as evidence. P14's extension imported only Hono types, which are erased at load. An extension that imports npm packages at runtime needs those packages to resolve from disk or be exported by the host. That case is **unverified**.

Limits of these probes. Wrangler's local Hyperdrive is a direct TCP proxy. It does not exercise transaction pooling, query caching, or edge latency. Nothing here ran on Cloudflare's network, and no PlanetScale instance was contacted.

## 3. Deployment options

| Option | What runs | Hyperdrive | Work | Gives up |
| --- | --- | --- | --- | --- |
| A. Reverse proxy to the Rust binary | Existing container or VM. Hono does `app.all('/ns/*', proxyToAxond)` | No (binary uses `tokio-postgres` directly) | Days | A second deployment. Not on Workers. No extensions. |
| B. Rust binary in Cloudflare Containers behind a Worker | Container as a Durable Object, Worker routes to it | Unconfirmed from inside a container (**unverified**) | Days | Not a Worker. No extensions. |
| **C. TypeScript on Hono (chosen)** | Worker, Bun, Node, or a compiled binary | Yes | 8,000 to 12,000 lines of TS for the core (my estimate, section 8), plus the extension SDK | A second implementation until Rust is retired (open question 1) |
| D. `gateway-core` as WASM called from TS | TS glue plus a 315 KB gzip module | Yes | Small, but replaces little | Dropped. It existed to keep the guardrail's regex semantics, and the guardrail is gone. |
| E. Whole gateway as WASM (`workers-rs`) | Worker | Via `worker` crate sockets, unproven | Near a rewrite | Dropped. It keeps the Rust build and still cannot host extensions. |
| F. Rust as a native addon (`napi-rs`) | Node or Bun | No, and not on Workers | Medium | Dropped (**unverified**, not tested). |

Options A and B keep the recompile-per-extension problem the maintainer is trying to remove, so they serve only as bridges. Option C is the only route that reaches Worker plus Hyperdrive with Axond's semantics intact and gives extensions a home.

## 4. Target architecture

### 4.1 Packaging

Export a factory, not a server:

```ts
// @axond/gateway
export interface AxondOptions {
  store: Store;                                  // Postgres first, SQLite second
  providers: ProviderConfig[] | (() => Promise<ProviderConfig[]>);
  gatewayKey: string | ((c: Context) => string);
  extensions?: AxondExtension[];
  waitUntil?: (p: Promise<unknown>) => void;     // c.executionCtx.waitUntil on Workers
}
export function createAxond(opts: AxondOptions): Hono<AxondEnv>;

// Existing API
app.route('/', createAxond({ store, providers, gatewayKey: c.env.AXOND_KEY, extensions }));
```

Use only `fetch`, `Request`, `Response`, `ReadableStream`, and `crypto.subtle`. Keep `node:` imports out of the core so the same package runs on Workers, Bun, and Node. Two hosting shapes on Cloudflare:

- **Same Worker.** `app.route('/', createAxond(...))` inside the existing Hono Worker.
- **Separate Worker behind a Service Binding.** The API Worker calls `env.AXOND.fetch(req)`. There is no public route, which matches ADR 0063's "private network, one static key" model. My read: this is the better default, because a deploy of Axond cannot break the main API.

### 4.2 The extension contract

An extension is a Hono middleware plus optional routes and store migrations. The core owns the order of stages. Extensions attach to a named stage, and within a stage they run in the order given.

```ts
// @axond/sdk
export type Stage = 'pre-auth' | 'post-auth' | 'pre-dispatch';

export interface AxondEnv {
  Bindings: Record<string, unknown>;
  Variables: { axond: AxondContext };
}

export interface AxondContext {
  requestId: string;
  route: 'chat' | 'messages' | 'embeddings' | 'responses';
  subject?: string;                              // set by auth (post-auth onward)
  namespace?: NamespaceRecord;                   // set by namespace resolution (post-auth onward)
  target?: { provider: string; model: string };  // set after the model id is parsed (pre-dispatch)
  body: RequestBody;
  onSettle(fn: (s: Settlement) => Promise<void>): void; // runs inside waitUntil, after the charge
}

export interface RequestBody {
  raw(): Promise<Uint8Array>;                    // untouched bytes
  json<T = unknown>(): Promise<T>;               // read-only parse, does not mark the body modified
  setModel(model: string): void;                 // targeted rewrite, stays byte-faithful otherwise
  setJson(value: unknown): void;                 // full replace, opts out of byte-fidelity
}

export interface AxondExtension {
  name: string;
  apiVersion: 1;                                 // section 4.6
  stage: Stage;
  middleware: MiddlewareHandler<AxondEnv>;
  routes?: Hono<AxondEnv>;                       // e.g. a token-minting endpoint
  migrations?: string[];                         // SQL, idempotent, tables prefixed `axond_ext_<name>_`
}
```

Response transforms are ordinary post-`await next()` code on `c.res`. The core wraps upstream streams in a `ReadableStream`, and an extension that rewrites the response replaces `c.res.body` with a transformed stream. Section 8 covers why the core must stay byte-faithful unless an extension opts out.

A second extension point is not middleware. Anything shaped like the removed budget backends is a `Store` implementation passed as `opts.store`. The `Store` interface is therefore also part of the public contract.

### 4.3 Where each stage sits

```
request
  → pre-auth extensions        (no identity yet; cheap rejection, request-id, CORS, JWT verification)
  → static-key auth            (core)
  → resolve namespace + admit  (core, 1 SQL round trip)
  → post-auth extensions       (subject and namespace known; scope narrowing, revocation lookup)
  → parse model, blocklist, price (core; sets target)
  → pre-dispatch extensions    (target known; request transforms, rate limits, redaction)
  → credential pick, upstream call, stream relay (core)
  → response-side code in extensions (after `await next()`)
  → charge + usage (core, in waitUntil), then onSettle hooks
```

### 4.4 Distribution

| Target | How extensions load | Status |
| --- | --- | --- |
| Cloudflare Worker | Bundled at build time. Workers cannot import arbitrary files at runtime. | Follows from Workers' model (**unverified** for this codebase) |
| Compiled binary (`bun build --compile`) | The binary `import()`s `.ts` files from a configured directory at startup. Editing a file needs a restart, not a rebuild (P14). | **Verified** for a type-only import. Extensions with npm dependencies are **unverified**. |
| Container or Node process | Same as the binary, or bundled. | Not tested |
| Library inside an existing Hono app | The host app passes `extensions: [...]` as ordinary imports. | Follows from the API |

The "drop a file in a directory" model is therefore a binary and container feature. On Workers it becomes "add an import and redeploy". The SDK should treat bundling as the primary path and file loading as one host's convenience.

### 4.5 Trust and isolation

Extensions run in the gateway process with access to credentials, every namespace's rows, and the raw request and response bytes. The Rust binary avoided this by having no extensions. After this change, tenant isolation is only as strong as the least-trusted extension. That is acceptable for extensions the operator writes or reviews. It is not acceptable for third-party code without a further boundary (for example, running untrusted extensions as separate Workers behind a dispatch layer, which I did not investigate). Treat "who may author extensions" as a decision, not a default (open question 3).

### 4.6 Contract stability

The SDK is a public API in a way the compiled Rust gateway never was. `AxondContext`, `RequestBody`, `Stage`, the stage order, and the `Store` interface are all compatibility promises. Version them from the first release (`apiVersion` above), keep the surface small, and decide the deprecation policy before publishing. Adding a stage later is cheap. Changing when an existing stage runs is a breaking change for every extension.

### 4.7 Removed features as extensions

Each item ADR 0063 withdrew maps to an extension shape. Two need SDK primitives that do not exist yet.

| Removed feature | Extension shape | Needs |
| --- | --- | --- |
| Minted tokens, verifiers, epochs | `pre-auth` middleware that verifies a token with `crypto.subtle` or `hono/jwt` and narrows `subject` and `namespace`. A `routes` entry serves the minting endpoint. | Nothing new |
| Token revocation | `post-auth` lookup against an extension-owned table | `migrations` and a `store.query` for extensions |
| Rate limiting | `pre-dispatch` middleware | Shared state across isolates: an extension-owned table, Durable Object, or Redis. Per-isolate counters are fine for a soft limit. |
| Per-key scope, alias globs, per-request spend caps | `post-auth` or `pre-dispatch` checks against `target` and the namespace record | Nothing new |
| Content redaction (`axond.redact`) | `pre-dispatch` request rewrite plus a response stream transform | A linear-time regex engine. JS `RegExp` backtracks, so untrusted patterns need a safe-regex library or an RE2 binding. |
| Config hot reload | Not middleware. Pass `providers` as an async function, or have the host restart. | Nothing new |
| Redis or Postgres budget backends | A `Store` implementation, not middleware | The stable `Store` interface |
| Per-tenant admission fairness | `pre-dispatch` middleware, or a Durable Object on Workers | Shared state, as with rate limiting |

## 5. Per-request flow and round trips

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

## 6. Hyperdrive and PlanetScale settings

- **Disable Hyperdrive query caching** on the config that serves `resolve_namespace`. Cloudflare caches read queries by default (max age 60 s), and writes do not invalidate the cache. A cached `SELECT` would serve a deleted namespace and a stale `spent` for up to a minute. Create the config with `--caching-disabled`. A second config with caching on is fine for the provider-models listing.
- **One `pg` client per request.** Workers cannot share I/O objects across requests. Hyperdrive pools on the far side, so the connect is cheap.
- **Use unnamed parameterized queries.** `pg` does this by default. Do not set `lock_timeout` or `statement_timeout` with session `SET`, as `store/postgres.rs:152` does. Under transaction pooling the setting does not stick. Use `ALTER ROLE ... SET statement_timeout` or `SET LOCAL` inside a transaction.
- **Schema-qualify table names** rather than relying on `search_path` (the journal and usage sink do rely on it).
- **Advisory locks are safe** because Axond uses only `pg_advisory_xact_lock` (`store/postgres.rs:1597-1602`), scoped to one transaction. LISTEN/NOTIFY appeared only in the withdrawn control plane.
- **Port choice (opinion, unverified).** Point Hyperdrive at PlanetScale's direct port 5432, not the PgBouncer port 6432, so two poolers do not stack.
- **Role.** Cloudflare's guide lists `pg_read_all_data` as the minimum for read-only use. Axond writes, so the role needs write grants.
- **Migrations** run out of band on a direct connection (`psql -f`). P5 shows the DDL is idempotent and applies unmodified. Extension migrations (section 4.2) run the same way.
- **Numbers.** `pg` returns `bigint` as strings. Microdollars fit a JS `Number` up to about $9 billion (2^53), but pass values as strings or `BigInt` to keep the SQL's saturation semantics.

## 7. Replacing the background machinery

| Rust component | Replacement |
| --- | --- |
| Usage index worker | None. Written in the settlement statement. |
| Provider model discovery loop | Cron Trigger writing `axond_store_provider_models`. Its source compare-and-set logic ports as SQL. |
| Catalogue import (models.dev) | Cron Trigger, or a build-time snapshot. |
| Status refresher, graceful drain | Not needed. |
| Billing-grade usage journal (ADR 0049, opt-in) | A Cloudflare Queue with `request_id` as the dedup key, or a Cron consumer running the same `SKIP LOCKED` claim transaction. Skip unless billing-grade delivery is required. |
| Circuit breakers, credential cooldown, rotation | Per isolate, in module scope. Safe because they only optimize. |
| Admission and per-tenant load shedding | Cloudflare absorbs concurrency. Per-tenant fairness is lost unless an extension adds it (section 4.7). |

## 8. Port scope and cost

Portable with little risk: route table, model parse, pricing math, `SseDecoder` (445 lines), usage decoders, circuit breaker, failover policy, credential pool, and namespace, budget, and usage SQL. After PR 499, `gateway-core` is 4,162 lines and ports directly.

My estimate is 8,000 to 12,000 lines of TypeScript for the ADR 0063 surface with Postgres and SQLite stores, versus 62,337 Rust lines in `crates/gateway/src` after PR 499 (about 25,000 to 30,000 of those on the request path, by my earlier read). This is an inference from module sizes, not a measurement. Most Rust volume is boot, config, telemetry, shutdown, and admission machinery that does not apply to a Worker. The extension SDK is extra: a few hundred lines for the stage runner and the types, plus its tests.

### Hard parts

1. **Middleware versus byte-faithful passthrough.** Axond rewrites only `model` and otherwise relays bytes. `JSON.parse` collapses duplicate keys and loses integers above 2^53, and the strict stream parser in Rust rejects duplicate keys (`stream.rs:214-296` on `main`). Middleware that needs the body wants a parsed object. The design in section 4.2 resolves this by making the raw bytes the default, `json()` read-only, and `setModel` a targeted scan of the raw body. Only `setJson` gives up fidelity, and it does so visibly. Seven byte-faithful streaming tests were restored during the cleanup. They are the parity bar for the port.
2. **Streaming response transforms in middleware.** An extension that rewrites SSE (redaction, for example) must not break event boundaries or delay chunks. The core's stream wrapper needs a documented helper for transforming events, so each extension does not reimplement the SSE decoder.
3. **Settlement on client disconnect.** Rust settles in `Drop`. On Workers this must come from `TransformStream` `cancel` and `flush`, with the charge in `ctx.waitUntil`. Whether Workers reliably signals cancel on client disconnect is **unverified**. Test it on the real edge.
4. **Timeouts.** `fetch` has no separate connect or idle timeout. Build header, body, and per-chunk idle bounds with `AbortController` and timers.
5. **Clock.** In TS use `Date.now()`, which Workers freezes during pure CPU work. Cooldown windows therefore advance only across I/O, which is acceptable.

## 9. Cloudflare constraints that matter

From Cloudflare's docs (research pass, 2026-09-28). Re-verify before committing.

| Constraint | Value | Consequence |
| --- | --- | --- |
| CPU time, Paid | 30 s default, up to 5 min | Streaming is bound by CPU, not wall time. Byte piping is cheap. JSON parse per event is not, and neither is a redaction extension. |
| CPU time, Free | 10 ms | Too low. Budget for the $5/month Paid plan. |
| Wall time for HTTP | Unbounded while the client stays connected | Long SSE streams are allowed. |
| `ctx.waitUntil` | Up to 30 s after response or disconnect | Enough for one settlement statement. Not enough for a slow outbox flush, and `onSettle` hooks share this budget. |
| Memory | 128 MB per isolate, shared by concurrent requests | Do not buffer large bodies. Rust buffers up to 32 MiB. Extensions that call `body.raw()` buffer. |
| Subrequests | 10,000 default on Paid | Not a constraint. |
| Simultaneous open connections | 6 waiting for headers | Watch this under a burst on one isolate. **Unverified** whether DB sockets count. |
| Startup | 1 s global scope | Lazy-init anything heavy, including extension setup. |
| Hyperdrive, Free | 100,000 queries per day | Two queries per request supports about 50,000 requests per day. Use Paid. |
| Hyperdrive queries | Counted per statement, including `BEGIN` and `COMMIT` | Autocommit statements are cheaper. Extension queries count too. |
| Bundle size | Docs conflict: 64 MiB uncompressed with no compressed cap, versus older 3 MB and 10 MB compressed limits | Confirm before bundling many extensions. |
| Proxy read timeout | 100 s on Free and Pro (**unverified**, non-official source) | Send SSE keepalive comments every 15 to 30 s. |

## 10. Single-binary and existing-server deployments

- **Bun or Node beside the existing API.** The same package runs with a real connection pool, `setInterval` for background work, and no `waitUntil` limits. This is simpler than Workers, and it is the right host if the existing API already runs on Bun or Node.
- **Single binary.** `bun build --compile` produced a 94.7 MiB (99.3 MB) executable with an embedded SQLite (P13). It loads extension files from a directory at startup, so operators add behavior without a compiler (P14). This keeps the original goal, at a size cost versus a stripped Rust binary. I did not build the Rust release binary, so I have no size comparison. Deno compile and Node SEA were not tested.
- **SQLite on Workers.** D1 is an alternative store with SQLite semantics and no Hyperdrive. The charge `UPDATE` works there. It does not fit the stated PlanetScale goal, so I did not pursue it.

## 11. Reusing the Rust repo as an oracle

`tests/compat` (Python SDKs), `tests/compat-ts` (the vendors' Node SDKs), and `tests/fixtures` boot a real process from `AXOND_BIN` against a fake upstream and assert wire behavior. A TS port that runs as a process on Bun can target the same lanes through a small launcher that translates the generated TOML config. That gives byte-level conformance evidence without writing new suites. Workers-specific behavior (disconnect, `waitUntil`, Hyperdrive) still needs its own tests. If Rust is retired (open question 1), keep the compat lanes and the byte-faithful streaming fixtures, since they define the contract the SDK must preserve.

## 12. Risks

| Risk | Likelihood (my read) | Mitigation |
| --- | --- | --- |
| Extension contract changes after release and breaks extensions | High if unplanned | `apiVersion`, small surface, deprecation policy before publishing (section 4.6) |
| An extension leaks credentials or crosses namespaces | Depends on who writes extensions | Decide authorship and trust up front (section 4.5). Review first-party extensions like core code. |
| Extensions break byte-faithful streaming | Medium | Raw-by-default body, a tested SSE transform helper, and the restored streaming fixtures as a CI gate |
| Two implementations drift | High while both ship, low once Rust retires | Conformance lanes in CI, and a decision on Rust's future |
| Worker disconnect handling loses charges | Medium | Real-edge test before launch. Idempotent settlement (P12) makes retries safe. |
| Hyperdrive caching serves stale budget or a deleted namespace | Certain if left at default | Caching-disabled config |
| Hyperdrive pooling surprises not visible in local tests | Low to medium | Staging against PlanetScale before any commitment |
| Binary size and startup regress against Rust | Certain for size, unmeasured for startup | Accept the size (99.3 MB measured). Measure startup during the spike. |

## 13. Recommended plan

1. **Merge PR 499 first.** It gives the port a clean baseline: the surface to match is what remains.
2. **Now, no port.** Run the Rust binary behind your Hono API with a proxy route (option A) to keep Litvue traffic flowing.
3. **Spike, about one week.** Build the smallest TS slice: auth, `resolve_namespace`, `POST /ns/{ns}/v1/chat/completions` and `/messages` with streaming passthrough, and combined settlement. Add the extension seam: the stage runner plus three reference extensions, (a) a `pre-dispatch` rate limiter, (b) a response-stream transform that preserves SSE chunk boundaries, and (c) a file-loaded extension with an npm dependency inside a compiled binary. Deploy the core to a Worker against a PlanetScale branch through a caching-disabled Hyperdrive config. Answer the unverified questions with measurements: disconnect cancel, connection counting, real round-trip latency, and extension dependencies in the binary.
4. **Confirm** with those numbers, then complete the port: management API, budgets, cadence, models listing, credentials, SQLite store.
5. **Gate** with the `tests/compat*` lanes. Record the result as an ADR that supersedes the single-binary Rust framing in ADR 0063, and states the extension contract, its versioning, and who may author extensions.

## 14. Spike results (2026-09-29)

Recorded while landing the TypeScript gateway. [ADR 0066](../adr/0066-typescript-hono-extension-contract.md) is the decision record.

| Question | Result |
| --- | --- |
| Worker + Hyperdrive + PlanetScale | **No-go for a live PlanetScale claim.** `wrangler deploy --dry-run` bundled `ts/packages/worker` (322.90 KiB gzip 70.51 KiB) with a Hyperdrive binding and the rate-limit extension imported statically. This environment has no Cloudflare or PlanetScale credentials, so disconnect latency on the edge, connection counting, and a PlanetScale round trip were not measured. |
| Worker runtime, local Hyperdrive | **Go against Postgres 16.** `wrangler` 4.143.1 `unstable_dev` (workerd) with `CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE` served `GET /healthz` as `ok` and `POST /api/v1/namespaces` as 201, then listed that namespace. The binding was Hyperdrive's local proxy, not a Cloudflare Hyperdrive config and not PlanetScale. |
| TypeScript container | **Go locally.** `podman build` of `ts/Dockerfile` produced `localhost/axond-ts:local` (235 MB, image `5613090b9bda`). The container served `GET /healthz` as `ok` and `GET /api/v1/namespaces` as the configured `platform` namespace from SQLite. The root `Dockerfile` remains the Rust release image. |
| Idempotent charge | **Go on Postgres 16.** Ten concurrent settlements of one `request_id` plus ten distinct ids charged 11 times (`spent = 11000`). A settle carrying the pre-delete incarnation charged nothing after delete and recreate. The same case passes on SQLite. A wrapper that caches `SELECT`s still returned a deleted namespace; the live store returned none. Hyperdrive caching stays disabled for that reason. PlanetScale was not in the run. |
| Compiled binary | **Go.** Bun 1.4.2 `bun build --compile` produced an 81,696,224-byte executable (`sha256 281e2f7574c91d080f2ef504bab786e2fed57384d5d428d810f9526ebf847dc8`). A successful boot reached `/healthz` in 71 ms (`ok`) at 43,276 kB RSS and loaded a `.ts` extension from `AXOND_EXTENSIONS_DIR` without a rebuild (`loaded`). `apiVersion` 2 exited 1 before listen: `extension future.ts apiVersion 2 is not supported (want 1)`. The local Rust binary next to it is a debug build of 412,428,264 bytes. |
| Signed binary | **Go for an ephemeral key.** cosign 2.5.2 signed that same-sized rebuild (`sha256 92297bdd7ea87d8225e3a78368911fc5928733ee8a201812affe1082ee788092`) with `--tlog-upload=false`, and `verify-blob` printed `Verified OK`. The private key was discarded. Pull-request CI repeats that check. A `v*` tag job signs keyless with GitHub OIDC; that tag path was not executed in this environment. |
| Extension npm dependency | **Bundle the extension.** A compiled binary that `import()`s a `.ts` file cannot resolve that file's `node_modules` (`Cannot find package 'smol-toml'`). `bun build probe.ts --outfile probe.js` embeds the dependency, and the same binary then served the parsed value `7`. The Node wrapper still resolves packages from the extension directory. |
| Extension seam | Promoted. Stages are `pre-auth`, `post-auth`, `pre-dispatch`, with response transforms after `await next()` and `onSettle` after the charge. Reference packages: `@axond/rate-limit`, `@axond/redact`, `@axond/tokens`. |
| Shadow against the Rust binary | **Go for the fixture suite.** `ts/scripts/shadow-compare.ts` compared status, parsed bodies, and charged `(model, status, cost)` for health, models, chat, embeddings, responses, and messages, buffered and streamed, plus the unprefixed, unknown-namespace, and encoded-namespace errors. Seven usage rows matched. Rust re-encodes some JSON; TypeScript keeps the fixture bytes. Request ids are minted per process. This is not a production traffic sample. |
| Rollback to the Rust binary | **Exercised locally.** The compiled TypeScript binary created namespace `rollback-tenant` (`201`). That process was stopped and `target/debug/axond` was started on the same TOML and SQLite file. Rust answered `/healthz` `ok`, listed `platform` and `rollback-tenant`, and `GET /ns/platform/v1/models` returned `200` with an empty list. That is a process switch on one machine, not a production cutover. |

## Open questions

Answers are in [ADR 0066](../adr/0066-typescript-hono-extension-contract.md).

1. Should the Rust implementation remain a supported product after the TS gateway ships, or become a reference and conformance oracle? **Oracle for this 0.x line.** The crates stay. A later minor can make the TypeScript build the release binary.
2. Does Litvue's API already run on Cloudflare Workers, or on Bun or Node? **Not in this repository.** The recommended shape is a separate Worker behind a service binding. Same-process `createAxond` when the API is Bun or Node. The interim proxy of the Rust binary is a Litvue API change (#501), not an Axond route.
3. Who may author extensions: **operators and first-party maintainers**, reviewed like core. No third-party isolation boundary.
4. What shared-state primitive does the SDK give extensions? **Extension-owned Postgres or SQLite tables** via `Store.query`. No Durable Objects or Redis dependency.
5. Is billing-grade usage delivery (the ADR 0049 journal) required? **No.** The idempotent `axond_store_usage` row is the charge key. A failed insert skips the charge (under-billing).
6. Does the SQLite store need to ship? **Yes**, for Bun, Node, and the compiled binary. Postgres is the Worker and highly available store.
7. Should per-tenant admission limits ship in core? **No.** The rate-limit extension owns that. Core does not emit `tenant_concurrency_exceeded`.

## Sources

- Workers limits and pricing: <https://developers.cloudflare.com/workers/platform/limits/>, <https://developers.cloudflare.com/workers/platform/pricing/>
- Hyperdrive pooling, caching, limits, and pricing: <https://developers.cloudflare.com/hyperdrive/concepts/connection-pooling/>, <https://developers.cloudflare.com/hyperdrive/concepts/query-caching/>, <https://developers.cloudflare.com/hyperdrive/platform/limits/>, <https://developers.cloudflare.com/hyperdrive/platform/pricing/>
- Hyperdrive with PlanetScale Postgres: <https://developers.cloudflare.com/hyperdrive/examples/connect-to-postgres/postgres-database-providers/planetscale-postgres/>, <https://planetscale.com/docs/connect/cloudflare>
- Cloudflare Containers GA and pricing: <https://developers.cloudflare.com/changelog/post/2026-04-13-containers-sandbox-ga/>, <https://developers.cloudflare.com/containers/pricing/>
- WASM and Rust on Workers: <https://developers.cloudflare.com/workers/runtime-apis/webassembly/>, <https://developers.cloudflare.com/workers/languages/rust/>
- Bun executables: <https://bun.com/docs/bundler/executables>
- Hono streaming and middleware: <https://hono.dev/docs/helpers/streaming>, <https://hono.dev/docs/guides/middleware>
- Repo: `docs/adr/0063-stateful-only-namespaced-gateway.md`, `docs/adr/0064-charge-actuals-after-response.md`, `crates/gateway/src/store/postgres.rs`, `docs/compatibility.md`. `crates/gateway-core/src/guardrail.rs` exists on `main` and is removed by PR 499.
