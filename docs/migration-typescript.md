# Moving a process to the TypeScript gateway

The release artifact for this 0.x line is still the Rust binary. [ADR 0066](./adr/0066-typescript-hono-extension-contract.md) keeps those crates as the conformance oracle. This page is how an operator runs the TypeScript process on the same config and store, and how to switch back.

## Config

`axond.toml` is the same file. The TypeScript loader reads the sections the post-ADR 0063 gateway still uses: `server`, `storage`, `namespace`, `provider`, `credential`, `credential_pool`, `gateway_key`, `price`, `transport`, `failover`, `admission`, `shutdown`, and `catalog`.

These sections fail boot, by name, and the error does not echo secret values:

`admin_breakglass`, `admin_oidc`, `budget`, `control_plane`, `convergence`, `core_middleware`, `gateway_minting`, `gateway_token`, `gateway_token_epoch`, `gateway_verifier`, `mode`, `model`, `rate_limit`, `reload`, `revocation`, `secret_store`.

Declare one deployment-wide `[[gateway_key]]`. A per-namespace list is withdrawn. Scalar overrides use `AXOND_<section>__<key>` (for example `AXOND_SERVER__BIND`). A secret's env var must not use that shape, or boot treats it as a config key.

## Store

There is no usage-row migration. SQLite and Postgres keep `axond_store_usage`, and one `request_id` is still the charge key. Point `[storage] path` or `dsn_env` at the existing database.

The TypeScript process adds two columns on `axond_namespace`, `allow_platform_fallback` and `from_config`, and tables `axond_catalog_streak`, `axond_namespace_lock`, and `axond_schema_lock`. Those are not in the Rust `ops/postgres` scripts. Rust does not read `axond_catalog_streak` or the lock tables. Opening the same SQLite file from the Rust binary leaves the extra objects in place.

A role that can `CREATE` adds the two columns to an existing `axond_namespace` and creates the missing tables. A second boot does not rebuild existing rows. A role that cannot `CREATE` leaves an existing schema in place and names a missing table or a missing namespace column. On a Worker that sentence is the log line `schema_unavailable`, and the HTTP response stays `store_unavailable`. Extension migrations run at process start on SQLite and on Postgres. Each statement is recorded in `axond_schema_migrations` and a second boot skips that id. A failed statement is rolled back and is not recorded. The error names the migration id and omits the driver text.

## What core no longer does

| Removed from core | How to get it back |
| --- | --- |
| Rate limiting and per-tenant admission | `@axond/rate-limit`. Isolate mode counts in one process. Store mode counts in `axond_ext_ratelimit_window`. |
| Redaction | `@axond/redact` rewrites request JSON and SSE frames. |
| Minted `axt1.` tokens, revocation, and epoch | `@axond/tokens`. Core still rejects an `axt1.` credential that no extension accepts. |
| Redis budget backend | Not an SDK dependency. The store is SQLite or Postgres. |
| Hot reload | Withdrawn. Extensions load at process start. Restart to pick up a new file. |
| Billing-grade usage journal | Not built. The usage insert is the charge. |

On a Node process or a compiled binary, put extension files in `AXOND_EXTENSIONS_DIR` or `[extensions] dir`. A `.ts` file loads without rebuilding the binary. A Worker bundles the extensions it imports; it does not read that directory.

## Rollback

Stop the TypeScript process. Start the Rust binary with the same `AXOND_CONFIG` and the same SQLite file. `/healthz` stays a liveness check on both. Namespaces created through `POST /api/v1/namespaces` remain in the store.

That switch was exercised on this host. The TypeScript process created `streak-rollback` (`201`) on a fresh SQLite file that also held `axond_catalog_streak` with `consecutive_refusals` 2. After that process stopped, `target/debug/axond` on the same file answered `/healthz` `ok`, listed `platform` and `streak-rollback`, and returned `200` `{"data":[],"object":"list"}` from `GET /ns/platform/v1/models`. Rust stderr was empty.

The root `Dockerfile`, Kubernetes, Azure, and Compose manifests stay on the Rust image. Switching a deployment to `ts/Dockerfile` or the Worker template is a separate change, and it is not what this 0.x line ships as the release artifact.

A shadow compare of the fixture suite is `ts/scripts/shadow-compare.ts`. It checks status, parsed JSON, and charged `(model, status, cost)`. It is not a production traffic sample. Live Hyperdrive against PlanetScale is not part of this repository's test environment.
