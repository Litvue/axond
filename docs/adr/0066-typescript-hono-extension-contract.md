# 66. TypeScript gateway, extension contract, and the Rust implementation

Date: 2026-09-29

## Status

Accepted

## Context

Axond's product shape after [ADR 0063](./0063-stateful-only-namespaced-gateway.md) is one static gateway key, namespace URLs, and a SQLite or Postgres store. The deployment investigation concluded that a Hono application written against Web standards can serve that shape on Node, Bun, a compiled binary, and a Cloudflare Worker, and that extensions should be Hono middleware instead of a Rust rebuild.

The Rust process is the implementation operators run today. Retiring it in the same change as the port would delete the conformance oracle the compat lanes already trust.

## Decision

### 1. Rust stays the conformance oracle for this 0.x line

The crates under `crates/` stay. They are the oracle for `tests/compat` and `tests/compat-ts` until a later minor makes the TypeScript build the release binary and the release image. This ADR does not delete Rust, and it does not retarget the existing Dockerfile or release-please config. New product behavior lands in `ts/` first. A TypeScript-only pull request does not need the Rust CI lanes.

### 2. One host-agnostic library

`createAxond` returns a Hono app. `@axond/sdk` and `@axond/gateway` do not import `node:`. The recommended Litvue deployment is a separate Worker behind a service binding. When the API process is Bun or Node, the same factory runs in-process. The Litvue proxy that still fronts the Rust binary is outside this repository; it is not an Axond change.

### 3. Who may author an extension

Extensions are operator and first-party code, reviewed like the gateway. There is no third-party in-process isolation boundary. `apiVersion` is `1`. Adding a stage is compatible. Changing when an existing stage runs is breaking and needs a new `apiVersion`.

Stages, in order: `pre-auth`, static gateway key (unless a pre-auth extension set `authenticated`), namespace resolution, `post-auth`, admission, `pre-dispatch`, upstream. A returned `Response` ends the pipeline. Code after `await next()` may replace `c.res`.

An untrusted extension receives a store that requires the request namespace on every query and drops rows for any other namespace. A trusted extension receives the process store. Migrations may create only tables prefixed `axond_ext_<name>_`.

On a binary or Node process, `AXOND_EXTENSIONS_DIR` (or `[extensions] dir`) loads `.ts`, `.js`, and `.mjs` files at startup. Adding a file does not require rebuilding the binary; the process is restarted. On a Worker, extensions are static imports and the bundler includes them.

### 4. Shared extension state

Extension-owned Postgres or SQLite tables, reached through `Store.query`, are the shared state. The SDK does not depend on Durable Objects or Redis. Isolate-local maps are a documented soft limit (the rate-limit extension's `isolate` mode).

### 5. Settlement stays an idempotent usage row

[ADR 0049](./0049-billing-grade-usage-outbox.md)'s billing journal is not required. The charge key is one `axond_store_usage` insert per `request_id`. The budget update runs only when that insert wins, the cost is non-null, the namespace still exists, and the admit-time incarnation matches. A lost usage insert under-bills. That is the accepted tradeoff: exactly-once per `request_id`, not a second journal. This amends [ADR 0064](./0064-charge-actuals-after-response.md) by naming the insert as the idempotency boundary.

### 6. Two stores, one interface

SQLite ships for Bun, Node, and the compiled binary. Postgres ships for Workers (Hyperdrive, caching disabled for namespace and budget reads) and for other highly available deployments. Both implement `Store`. The TypeScript Postgres schema adds `allow_platform_fallback` and `from_config` on `axond_namespace`, and `axond_namespace_lock` so create, delete, and budget writes can serialize without `pg_advisory_xact_lock` (Hyperdrive rejects advisory locks). Those objects are not in the shipped `ops/postgres/*.sql` files, which remain the Rust oracle's DDL.

### 7. Per-tenant admission is not core

Core does not emit `tenant_concurrency_exceeded`. A rate-limit extension is the admission control. That is a parity gap with the Rust per-replica limiter, recorded in the [parity contract](../design/typescript-parity.md).

### State tier

Tier 2 for the gateway store (SQLite or Postgres), unchanged from ADR 0063. Extensions do not add a new required backend.

## Consequences

- Compat lanes can target `AXOND_BIN=ts/bin/axond` without a new suite.
- A Worker deploy still needs an operator's Hyperdrive and PlanetScale credentials. This repository records the template and the SQL; it does not claim a live edge run.
- Minted `axt1.` tokens, redaction, and rate limits return as extensions. The Rust binary still rejects `axt1.` with `401`, which remains the operator-facing contract until the TypeScript artifact ships.
