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

A role that can `CREATE` adds the two columns to an existing `axond_namespace` and creates the missing tables. A second boot does not rebuild existing rows. A role that cannot `CREATE` leaves an existing schema in place and names a missing table or a missing namespace column. On a Worker that sentence is the log line `schema_unavailable`, and the HTTP response stays `store_unavailable`. After an owner has applied the schema, that same role serves namespace create and list. The Worker log does not record `schema_unavailable` for that request. Extension migrations run at process start on SQLite and on Postgres. Each statement is recorded in `axond_schema_migrations` and a second boot skips that id. A failed statement is rolled back and is not recorded. The error names the migration id and omits the driver text.

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

## Running on Litvue's dev config (#563)

### Image

`ghcr.io/litvue/axond-typescript` is built from `ts/Dockerfile` as a Bun-compiled
binary on `gcr.io/distroless/base-debian12:nonroot`. The image runs `/axond` as
the non-root user and listens on port `8080`. Its working directory is
`/var/lib/axond`, writable by `nonroot`; mount a volume there to persist a
relative SQLite path. The **TypeScript image** workflow is its only publisher:
a manual dispatch publishes `sha-<short>`, and a `v*` tag publishes that SHA
tag plus the semver `<version>` tag. Deploy by the digest printed in the
workflow run summary. The Rust image
`ghcr.io/litvue/axond` and release-please workflow remain unchanged.

### Configuration differences

| Setting | Rust 0.6 dev | TypeScript gateway | Action |
| --- | --- | --- | --- |
| `[reload] watch = false` | The Rust main also refuses this withdrawn section (ADR 0063). | Boot fails because `reload` is withdrawn. | Delete `[reload]`. |
| `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf` | Litvue's Rust exporter uses OTLP/HTTP protobuf. | Boot fails; the TypeScript exporter supports OTLP/HTTP JSON only. | Set `http/json`. |
| `OTEL_EXPORTER_OTLP_{TRACES,METRICS}_HEADERS` | Rust applies signal-specific headers before generic headers and percent-decodes values. | They were previously ignored, so Axiom exports returned 401. The TypeScript gateway now reads them with the same precedence and decoding behavior. | Keep the signal-specific headers. |

Axiom accepts OTLP/HTTP JSON on `/v1/traces`, `/v1/metrics`, and `/v1/logs`.
On 2026-10-16, empty JSON exports returned `200 {"partialSuccess":{}}` on all
three routes; malformed JSON returned `400`, and missing authentication
returned `401`.

### Accepted unchanged dev configuration

The TypeScript gateway accepts these remaining sections and keys from Litvue's
dev config. For the local boot, the test copy also redirected `server.bind` and
provider `base_url` values to loopback; all other listed TOML values were kept
unchanged after removing `[reload]`:

| Section | Accepted keys and values |
| --- | --- |
| `[server]` | `bind = "0.0.0.0:8080"` |
| `[shutdown]` | `drain_grace_ms = 5000`, `deadline_ms = 15000`, `flush_timeout_ms = 5000` |
| `[[namespace]]` | `id = "platform"`, `default = true` |
| `[[provider]]` | Keys: `id`, `kind`, `base_url`. `openai`: `kind = "openai"`, `https://api.openai.com/v1`; `anthropic` and `anthropic-covered`: `kind = "anthropic"`, `https://api.anthropic.com/v1`; `xai`: `kind = "openai"`, `https://api.x.ai/v1`. |
| `[[credential]]` | Keys: `namespace`, `provider`, `env`, `id`. `openai-primary` → `openai`, `GW_PLATFORM_OPENAI_API_KEY`; `anthropic-primary` → `anthropic`, `GW_PLATFORM_ANTHROPIC_API_KEY`; `anthropic-covered-primary` → `anthropic-covered`, `GW_PLATFORM_ANTHROPIC_COVERED_API_KEY`; `xai-primary` → `xai`, `GW_PLATFORM_XAI_API_KEY`. All use `namespace = "platform"`. |
| `[[gateway_key]]` | Keys: `env`, `namespace`; values `namespace = "platform"`, `env = "GW_INBOUND_PLATFORM_KEY"`. |
| `[storage]` | `backend = "postgres"`, `dsn_env = "GW_STORE_DSN"`, `create_table = true`, `on_unavailable = "deny"` |
| `[catalog]` | `source = "models-dev"`, `store = "postgres"`, `dsn_env = "GW_STORE_DSN"`, `create_table = true` |
| `[[price]]` | Keys: `provider`, `model`, `input_microdollars_per_million`, `output_microdollars_per_million` (all 11 entries). |
| `[failover]` | `overall_timeout_ms = 120000` |
| `[transport]` | `response_header_timeout_ms = 120000`, `buffered_body_timeout_ms = 120000` |
| `[admission]` | `max_request_bytes = 8388608`, `max_in_flight = 32`, `max_in_flight_streams = 16`, `max_in_flight_per_tenant = 0`, `max_tenants = 256`, `queue_capacity = 0`, `queue_wait_ms = 0`, `max_stream_duration_ms = 230000`, `max_stream_bytes = 33554432` |

The environment variables used by the dev config are `GW_STORE_DSN`,
`GW_PLATFORM_OPENAI_API_KEY`, `GW_PLATFORM_ANTHROPIC_API_KEY`,
`GW_PLATFORM_ANTHROPIC_COVERED_API_KEY`, `GW_PLATFORM_XAI_API_KEY`,
`GW_INBOUND_PLATFORM_KEY`, and `OTEL_EXPORTER_OTLP_ENDPOINT`.

Use a separate database from the Rust dev deployment: the TypeScript process
adds its own store columns and tables described in [Store](#store).
For a Postgres dev rollback, stop the TypeScript app and route traffic back to
the Rust deployment; the TypeScript app never touched the Rust database. Usage
and budget rows written by TypeScript remain in its separate database and are
not carried back.

### Boot check

The local run used a test copy with `[reload]` removed, a loopback bind, provider
URLs redirected to the fake upstream, and the OTLP endpoint pointed at the local
stub with `http/json`. These loopback values were test-only; the provider URLs
above are unchanged for deployment. The compiled binary was started with:

```sh
AXOND_CONFIG=./axond.dev-loopback.toml ./axond
```

The probes returned:

```text
$ curl -fsS http://127.0.0.1:18563/healthz
ok
$ curl -fsS http://127.0.0.1:18563/readyz
ready
```

The authenticated models route returned `200` (`data: []` in the test store);
the unauthenticated route returned `401`. The test Postgres store had no
`platform` budget policy (`GET /api/v1/namespaces/platform/budget` returned
`404 unknown_budget`), so the test created a monthly policy through that
management endpoint before sending one chat request. The fake upstream returned
`200`; the TOML file was not changed for this setup. A fresh store needs an
active budget policy before it will admit inference. The OTLP stub recorded
authorization and dataset headers on both `/v1/traces` and `/v1/metrics`.
The same config was mounted read-only into `axond-typescript:local` with
`docker run --network host`; the image returned `200 ok` from `/healthz` and
`200 ready` from `/readyz`.

## Rollback

Stop the TypeScript process. Start the Rust binary with the same `AXOND_CONFIG` and the same SQLite file. `/healthz` stays a liveness check on both. Namespaces created through `POST /api/v1/namespaces` remain in the store.

That switch was exercised on this host. The TypeScript process created `streak-rollback` (`201`) on a fresh SQLite file that also held `axond_catalog_streak` with `consecutive_refusals` 2. After that process stopped, `target/debug/axond` on the same file answered `/healthz` `ok`, listed `platform` and `streak-rollback`, and returned `200` `{"data":[],"object":"list"}` from `GET /ns/platform/v1/models`. Rust stderr was empty.

The root `Dockerfile`, Kubernetes, Azure, and Compose manifests stay on the Rust image. The TypeScript image is published separately by the **TypeScript image** workflow and does not change the Rust release artifact.

A shadow compare of the fixture suite is `ts/scripts/shadow-compare.ts`. It checks status, parsed JSON, and charged `(model, status, cost)`. It is not a production traffic sample. A Worker Hyperdrive config points at PlanetScale's direct port 5432 with caching disabled. The template passes `--origin-host`, `--origin-port`, `--origin-scheme`, `--database`, `--origin-user`, and `--origin-password`, because a connection-string password containing `/`, `#`, `?`, or `%` is rejected by Wrangler's URL parser. The default TLS mode is `require` (TLS, WebPKI), and `--sslmode verify-full` also needs `--ca-certificate-id`. Port 6432 is PgBouncer, and Hyperdrive is already the pooler. Live Hyperdrive against PlanetScale is not part of this repository's test environment.
