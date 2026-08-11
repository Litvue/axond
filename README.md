# Axond

A stateless, single-binary, self-hosted **AI gateway**. Point your agents at
Axond instead of at provider APIs directly, and get one place to hold
provider keys, route model names, meter usage, and emit telemetry.

> **Status: beta.** Everything below is implemented and covered by tests:
> OpenAI-compatible and native Anthropic paths (buffered and streamed), ordered
> failover, credential pools, OTLP telemetry, durable usage sinks, shared
> budgets, and config hot-reload. What is supported, what is deferred, and what
> may change is written down in the
> [compatibility contract](./docs/compatibility.md); readiness is assessed in
> [`RELEASE.md`](./RELEASE.md).

## Why

Teams building on LLMs end up scattering provider keys across services, hard-coding
`provider/model` strings, and bolting on usage tracking per app. Axond
centralizes that:

- **One place for keys.** Provider credentials live in the gateway's config as
  environment-variable references — never in your app code.
- **Bring-your-own-key namespaces.** Serve your own customers' keys under
  isolated namespaces alongside your platform keys, with explicit (opt-in)
  fallback.
- **Model aliases, not topology.** Callers send `gpt-4o`; the gateway resolves it
  to a concrete provider + deployment. Change providers without touching callers.
- **Usage + budgets behind traits.** Raw usage records fan out to pluggable sinks
  (stdout by default; batched Postgres and OTLP opt-in); spend budgets —
  denominated in currency, computed from per-model pricing — are checked on a
  separate, request-path store.
- **Telemetry first.** One canonical usage record per request drives sinks,
  budgets, spans, and metrics from the same source of truth.

## Design principles

- **Runs stateless; scales stateful only when you ask it to.** The binary boots
  with zero external dependencies. Every stateful feature (durable usage,
  cross-replica budgets) is opt-in behind a trait — nothing silently drags a
  datastore onto the default path.
- **Passthrough-first.** The caller's wire shape must already match the target:
  the body is forwarded and only the `model` field is rewritten. A
  wire-incompatible target is rejected up front (`400 unsupported_wire`) rather
  than translated; cross-provider translation is deferred past beta.
- **Fail at boot, not at request time.** The whole config graph is validated on
  startup: undefined alias targets, empty target lists, and dangling
  namespace/provider references refuse to start.
- **Every route always exists.** Unavailable features return typed errors
  explaining themselves — never a bare 404 that's indistinguishable from a wrong
  `base_url`.
- **Credentials are write-only.** No endpoint ever returns a key; only presence
  is observable.
- **Inbound auth fails closed.** Every request that reaches a provider presents a
  configured gateway key; there is no keyless / anonymous mode.

## State tiers

State tiers describe the state backends axond itself depends on, not provider
egress: the gateway obviously calls upstream providers over the network at
Tier 0.

| Tier | What it buys | What it costs |
| --- | --- | --- |
| **0 — config-only** | Namespaces, providers, aliases, prices, credentials, pools, failover, reload, gateway keys, minted-token verification and issuance epochs, stdout usage, local budgets, and health probes. | No datastore. In-memory health and budgets are per replica; the default is no budget. This is the hermetic [`ops/tier0-gate.sh`](./ops/tier0-gate.sh) CI posture proved on every PR by [ADR 0018](./docs/adr/0018-tier-0-hermetic-boot-gate.md). |
| **1 — Redis** | Exact shared budgets and inbound in-flight rate limiting. | Redis availability is part of selected admission paths; `on_unavailable = "deny"` (the default) returns `503` (`budget_unavailable` or `rate_limit_unavailable`). |
| **2 — Postgres** | Durable usage rows and shared Postgres-backed budgets; a future store-owned caller/key lifecycle. | A Postgres role, ordered migrations, backup/restore ownership, and boot-time DSN resolution. |

Tier 1 ships `[budget] backend = "redis"` and `[rate_limit] backend = "redis"`
for exact shared admission. Precise per-token revocation remains a future
declaration. Minted claims require `jti`, but today's
revocation ladder is short TTLs, killing a `kid`, and rotation. An OTLP usage
sink is Tier 0 state (no datastore, nothing to migrate), but not hermetic: it
adds a collector dependency at boot, so it is excluded from the hermetic Tier
0 CI lane.

**One dimension, one owner.** Namespaces, providers, aliases, prices, and
provider credentials are permanently config-owned and reload through ADR 0011.
Only callers and keys may ever become store-owned at Tier 2; nothing is ever
defined in both. No database may override namespace provider access, an
alias's target, a price, or the credential pool. Even at Tier 2, a token
verifier intersects token claims with config-owned namespace authority (ADR
0016). See [ADR 0017](./docs/adr/0017-state-tiers-and-optional-backends.md)
and [ADR 0018](./docs/adr/0018-tier-0-hermetic-boot-gate.md).

## Quick start

For a five-minute Docker Compose deployment (including a stateful Redis and
Postgres variant), see the [deployment guide](./docs/deployment.md#5-minute-quickstart).

```bash
cp ops/compose/env.example .env
docker compose up -d --build
curl http://localhost:8080/healthz
```

The first Compose build compiles the static musl release and can take several
minutes. To call the authenticated catalogue:

```bash
curl -H "Authorization: Bearer quickstart-platform-key" \
  http://localhost:8080/v1/models
docker compose down -v
```

Keep `.env` until after teardown: Compose validates required variables before
running `down`. To run the smoke helper, tear down this stack first; it uses
the same host port. If another local stack owns port 8080, use
`AXOND_QUICKSTART_SMOKE_PORT=18080 just quickstart-smoke`.

For a source-based configuration path, use the full annotated reference:

```bash
cp axond.example.toml axond.toml      # edit providers/models/namespaces

# Secrets by env, referenced from config. Every credential the config declares
# must resolve, so either set all of these or delete the entries you don't want.
export GW_PLATFORM_OPENAI_API_KEY=sk-...
export GW_PLATFORM_OPENAI_API_KEY_OVERFLOW=sk-...   # second key in the openai pool
export GW_PLATFORM_ANTHROPIC_API_KEY=sk-ant-...
export GW_PLATFORM_AZURE_OPENAI_API_KEY=...         # gpt-4o's failover target
export GW_ACME_OPENAI_API_KEY=sk-...                # the example's BYOK namespace

# Inbound keys authenticate callers. At least one is required — there is no
# keyless mode, and a declared key whose env var is unset is a boot error.
export GW_INBOUND_PLATFORM_KEY=local-dev-token
export GW_INBOUND_ACME_KEY=acme-token

cargo run -p axond                    # or: just run
```

The same binary also provides offline token minting and Ed25519 key generation.
`keygen` never loads the gateway config. `mint` uses an explicitly supplied
`--config` when requested; an `AXOND_CONFIG` value is only an ambient aid.

```bash
# Generate a public verifier key and write the base64 PKCS#8 private key to a
# new 0600 file on Unix. On non-Unix platforms, restrict inherited permissions
# manually. The command prints only the public key and TOML snippet.
axond keygen --private-key ./acme-signing.key \
  --kid acme-2026-08 --env GW_VERIFY_ACME_2026_08 \
  --namespace acme --max-ttl 15m

# Signing material is read by name from the environment, never from argv.
export GW_SIGN_ACME="$(cat ./acme-signing.key)"
axond mint --kid acme-2026-08 --alg EdDSA --key-env GW_SIGN_ACME \
  --namespace acme --subject agent-1 --ttl 10m \
  --audience acme-production
```

`mint` prints only the `axt1.` token to stdout. It always enforces the 24-hour
policy ceiling; when a matching verifier is available in `--config` (or
`AXOND_CONFIG`), it also enforces that verifier's `max_ttl` and namespace
permission and defaults the audience from `[gateway_token]`. An unloadable
explicit config fails; an unloadable ambient `AXOND_CONFIG` produces a warning
on stderr and minting continues with only the policy ceiling. Without a usable
config, a token above the verifier's configured `max_ttl` can be minted but is
rejected by the gateway.
The minter emits only claims enforced by the current verifier.

For the complete signer setup, claim contract, rotation runbook, revocation
ladder, and delegation guidance, see
[`docs/minted-token-guide.md`](./docs/minted-token-guide.md).

```bash
curl localhost:8080/v1/chat/completions \
  -H "authorization: Bearer $GW_INBOUND_PLATFORM_KEY" \
  -H 'content-type: application/json' \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}'
```

Point any OpenAI-compatible SDK at `http://localhost:8080/v1` as its base URL,
with the gateway key as its API key.

### Native provider routes

An Anthropic client works the same way — point it at the same base URL and send
Anthropic's own wire to `/v1/messages`:

```bash
curl localhost:8080/v1/messages \
  -H "x-api-key: $GW_INBOUND_PLATFORM_KEY" \
  -H 'content-type: application/json' \
  -d '{"model":"claude-sonnet","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}'
```

The body is forwarded to the provider untouched but for `model`, streaming
included, so signed thinking blocks and tool-use blocks survive intact (verbatim
bytes on a stream; re-serialized values, same signatures, when buffered) —
and aliasing, failover, credential pools, budgets, and usage records apply exactly
as they do on `/v1/chat/completions`. `/v1/embeddings` is served the same way for
OpenAI-family targets, billed on input tokens only. Every route requires an alias
whose targets — including its failover targets — speak that route's wire: an
OpenAI-only alias on `/v1/messages`, or an Anthropic-native one on
`/v1/chat/completions`, is a `400 unsupported_wire` before anything is reserved or
dispatched. `/v1/responses` is deferred past beta and returns a typed
`501`. See [ADR 0012](./docs/adr/0012-native-provider-routes.md).

## Configuration

TOML owns all structure; the environment owns secrets (referenced by name) and
may override scalars. See [`axond.example.toml`](./axond.example.toml) for
the annotated surface: `server`, `namespace`, `provider`, `credential`,
`credential_pool`, `gateway_key`, `model`, and `usage_sink`.

Every env var a `[[credential]]` or `[[gateway_key]]` references must be set and
non-empty at boot, or the gateway refuses to start.

### Inbound authentication

```toml
[[gateway_key]]
env = "GW_INBOUND_PLATFORM_KEY"   # the secret lives in the env; `env` is its NAME
namespace = "platform"            # the namespace this caller is served under
```

Callers present the key as `Authorization: Bearer <token>` or `x-api-key: <token>`
(what an Anthropic SDK sends) — the same key table either way. Usage records
attribute the caller as the env var's *name*; a secret's value is never logged.

**Authentication fails closed and there is no keyless / anonymous mode:**

- at least one `[[gateway_key]]` is required, or the gateway refuses to boot;
- a declared key whose env var is unset or empty is a **fatal** boot error naming
  that env var and namespace — never a silently dropped key;
- two keys may not resolve to the same secret: whose namespace the caller gets
  would be ambiguous, so that too refuses to boot;
- an empty key table can never mean "allow all": a request without a configured
  key is `401`;
- a reload (`SIGHUP` / `[reload] watch`) runs the same validation, so a candidate
  with an unresolvable key is rejected and the running config keeps serving —
  rotate by declaring the new key alongside the old, reloading, then dropping the
  old one.

Minted tokens are additive to this static-key path. Configure a deployment
audience and a verifier whose public key is referenced by environment-variable
name:

```toml
[gateway_token]
audience = "acme-production"

[[gateway_verifier]]
kid = "acme-2026-08"
alg = "EdDSA"
env = "GW_VERIFY_ACME_2026_08"
namespaces = ["acme"]
max_ttl = "15m"
```

To revoke older minted tokens for a namespace on reload, add an issuance
epoch; a per-subject entry overrides the namespace-wide value for that subject:

```toml
[[gateway_token_epoch]]
namespace = "acme"
min_iat = "2026-08-10T12:00:00Z"
```

The verifier accepts `axt1.` compact JWS credentials alongside static keys.
`axond keygen` keeps the private PKCS#8 key in a new file and prints only the
public key plus this snippet; `axond mint` reads signing material by env-var
name and prints only the token. Ed25519 base64 whitespace is trimmed on both
sides because mounted secrets may preserve the generated file's trailing
newline; HS256 secrets are opaque bytes and are not trimmed. `scope` is enforced
as a narrowing route capability and can be emitted with repeatable `--scope`
flags. The `aliases` claim is enforced at dispatch and in the `/v1/models` view,
and is emitted by `axond mint --alias`. `max_request_microdollars` is enforced
at admission time against the pre-dispatch estimate and can be emitted by
`axond mint`. An explicit `--audience` must still match the configured
`[gateway_token]` audience.

Keep at least one static `[[gateway_key]]` for breakglass. Minted identity
verification is Tier 0 and adds no runtime datastore dependency. See the
[minted-token guide](./docs/minted-token-guide.md) for rotation (including the
same-`kid` key-material reload trap) and the honest Tier 1 revocation boundary.

Every route that dispatches to a provider (`/v1/chat/completions`, `/v1/messages`,
`/v1/embeddings`) authenticates, and so does `/v1/models` — it answers only for a
configured gateway key and lists the aliases scoped to the caller's namespace (an
alias whose targets the caller holds no credential for is not disclosed). Only the
liveness probes `/healthz` and `/readyz` answer without a credential.

See [ADR 0013](./docs/adr/0013-inbound-auth-fails-closed.md).

### Credential pools (Tier 0)

Several `[[credential]]` entries may name the same `(namespace, provider)` pair;
together they are that pair's pool. Each entry takes an optional `id` (the
attribution label — defaults to the env-var *name*, never the secret) and a
`weight`.

```toml
[credential_pool]
strategy = "weighted"        # or "round-robin" (default)
failure_threshold = 2        # 429s that park one credential
cooldown_seconds = 30        # wait before a half-open probe

[[credential]]
namespace = "platform"
provider = "openai"
env = "GW_PLATFORM_OPENAI_API_KEY"
id = "openai-primary"
weight = 3

[[credential]]
namespace = "platform"
provider = "openai"
env = "GW_PLATFORM_OPENAI_API_KEY_OVERFLOW"
id = "openai-overflow"
weight = 1
```

A credential that answers `429` (rate limit / exhausted quota) is skipped and the
request retries the **same** target with the next credential in the pool —
credential rotation, not target failover. Repeated 429s park that credential
alone; the target stays available to every other key. The credential that served
a request is attributed on its usage record as `credential_id`. Pools never cross
a namespace boundary: a BYOK namespace uses its own pool, or the whole platform
pool when it opts into `allow_platform_fallback`. See
[ADR 0006](./docs/adr/0006-credential-pools-per-namespace-provider.md).

## Failover across targets

An alias's `targets` are tried in configured order. Credential-pool dispatch is
the *inner* loop (rotate keys within one target); target failover is the *outer*
loop: a **retryable** upstream failure — a 5xx, a transport error, a fully
rate-limited pool, an unavailable model — advances to the next target, while a
4xx-class error that would just fail again is returned as-is. Each target carries
its own in-memory, per-replica circuit breaker: consecutive target-scoped
failures trip it, a tripped target is skipped for a cooldown, then re-offered as a
single half-open probe that closes on success. A pool-wide `429` is
credential-scoped and never trips the target's breaker. The walk is bounded by
both `failover.max_attempts` and `failover.overall_timeout_ms` so failover cannot
amplify latency without limit. Streaming can fail over only while opening the
upstream; once the relay emits its first byte a mid-stream failure is a terminal
`error` event. The serving target and total attempt count land on the usage
record and on the per-attempt spans. See
[ADR 0008](./docs/adr/0008-target-failover-and-circuit-scope.md).

## Usage sinks (Tier 0 by default; Tier 2 for Postgres)

Every terminated request — including failures, cancellations, and partial
streams — produces one canonical usage record. With no `[[usage_sink]]`
configured the record is written as one JSON line on stdout and the process
touches no datastore. Durable destinations are opt-in:

```toml
[[usage_sink]]
kind = "postgres"
dsn_env = "AXOND_USAGE_POSTGRES_DSN"   # the DSN is a secret: referenced, never inlined
create_table = true                   # or apply usage_v1.sql and additive migrations yourself

[[usage_sink]]
kind = "otlp"                         # usage as OTel log records, on the existing exporter
```

A `kind = "otlp"` sink is Tier 0 state but not hermetic: it requires the OTLP
collector at boot and is excluded from the hermetic Tier 0 CI lane. A
`kind = "postgres"` sink is Tier 2.

A configured sink connects at boot, so a bad DSN or an unreachable database
refuses to start rather than dropping records later. From then on the sink is off
the request path: records are buffered per sink and written in batches, and a
slow or failing destination **drops with a count** rather than stalling a
request. Drops are visible on `axond.usage.records_dropped{sink,reason}` and
writes on `axond.usage.records_written` — alert on the former.

The Postgres row shape is a published, versioned interface: see
[`docs/usage-schema.md`](./docs/usage-schema.md) for the columns, the change
policy, and the delivery guarantees, and
[ADR 0009](./docs/adr/0009-durable-usage-sinks.md) for why. Tinybird and
ClickHouse fit the same seam and are deferred to post-beta.

## Config hot-reload (Tier 0)

Provider, model, namespace, credential, and gateway-key config is replaceable
without a restart, so onboarding a BYOK customer is an edit and a signal:

```bash
export ACME_OPENAI_KEY=sk-...          # must reach the gateway's own process env
$EDITOR axond.toml                     # add [[namespace]], [[credential]], aliases
kill -HUP "$(pidof axond)"
```

The candidate config goes through the **full boot-time validation**, so a reload
is the boot gate applied again: any error — bad TOML, an alias pointing at an
undefined provider, a declared credential whose env var is unset — rejects the
candidate and the **previous config keeps serving**. The process environment is
re-read on every reload, which makes a newly-referenced credential resolve when
that variable is already present in the gateway process. A running process
cannot gain a new environment variable: minted verifier rotation must provision
the new variable before a restart, then start with both verifier entries before
removing the old one. See [#86](https://github.com/Litvue/axond/issues/86).

A successful reload publishes one atomic snapshot. Each request takes that
snapshot once and holds it for its whole life (streams included), so a reload
never half-applies and nothing in flight is dropped.

Watching the config file is opt-in:

```toml
[reload]
watch = true              # reload when the file's contents change
poll_interval_ms = 2_000  # floor 100
```

Reloads are observable: an `axond.config.reload` span, a counter
`axond.config.reloads{trigger,outcome}`, and a gauge `axond.config.generation`
(0 at boot, +1 per applied reload) — a replica that missed a reload shows up as a
generation skew. The applied log line carries an added/removed diff of
namespaces, providers, aliases, credential labels, and gateway-key env-var names.

`[server] bind` and `[[usage_sink]]` changes are reported with a warning and
otherwise ignored: the socket is already bound and sinks own live connections, so
both still need a restart. See
[ADR 0011](./docs/adr/0011-config-hot-reload.md).

## Telemetry

Off by default: with no OTLP endpoint the process only writes JSON logs to
stdout, and no exporter, tracer, meter, or propagator is installed. Point it at
a collector to turn on traces and metrics:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318   # OTLP/HTTP only
```

Every request produces one `http.server.request` span with a child
`axond.upstream.attempt` span per upstream call, carrying the alias, resolved
target, namespace, `credential_source`, status, retry count, tokens, cost, and
time-to-first-token. An inbound `traceparent` is joined rather than replaced, and
the context is injected into the upstream request, so a caller's trace runs
end-to-end. Metrics are `axond.http.server.*` (route/status) plus
`axond.request.*` and `axond.upstream.*` (namespace/alias/target/status). Spans
never carry credentials, prompts, or completions. See
[ADR 0007](./docs/adr/0007-telemetry-model.md).

## Architecture

```
crates/
  gateway-core        I/O-free provider adapters, wire translation, routing
                      primitives (circuit breaker, failover, catalog/pricing).
                      No HTTP client, no runtime, no config, no secrets.
  gateway-transport   The HTTP half: pooled reqwest client, credential
                      injection, timeouts.
  gateway             The binary: config, namespaced credential resolution,
                      routes, UsageSink + BudgetStore wiring.
```

The split keeps the hard-won provider-wire logic testable in isolation and
runtime-neutral.

## Roadmap

- [x] Streaming (SSE) relay end-to-end (see [ADR 0005](./docs/adr/0005-streaming-relay.md))
- [x] Ordered failover across targets + per-target circuit health (see [ADR 0008](./docs/adr/0008-target-failover-and-circuit-scope.md))
- [x] OpenTelemetry traces (per-upstream-attempt spans, TTFT), metrics, logs
- [x] Usage sinks: Postgres (batched, versioned schema) + OTLP; Tinybird / ClickHouse post-beta
- [x] Budget backends (Tier 1 / Tier 2): shared Redis / Postgres, held reservations, partial charging (see [ADR 0010](./docs/adr/0010-shared-budget-backends-and-charging-policy.md))
- [x] Exact cross-replica inbound in-flight rate limiting via Redis leases (Tier 1)
- [x] Native Anthropic `/v1/messages` passthrough + `/v1/embeddings` (see [ADR 0012](./docs/adr/0012-native-provider-routes.md)); `/v1/responses` deferred post-beta (typed `501`)
- [x] Multiple credentials per provider (pooling, weighted, skip-on-429)
- [x] Config hot-reload (SIGHUP / watched files) for zero-restart BYOK onboarding (see [ADR 0011](./docs/adr/0011-config-hot-reload.md))
- [x] Provider-SDK compatibility + record/replay + SSE soak tests (see [ADR 0014](./docs/adr/0014-compatibility-and-soak-harness.md))

See [`docs/adr`](./docs/adr) for the decisions behind these.

## Documentation

- [Deployment guide](./docs/deployment.md) — static binary, signed image, env,
  health/readiness, rotation.
- [Configuration reference](./docs/configuration.md) — every section, key, and
  default.
- [Minted identity guide](./docs/minted-token-guide.md) — signer setup, claims,
  rotation, delegation, and revocation boundaries.
- [Observability and runbook](./docs/observability.md) — OTel setup, metrics,
  failure modes.
- [Compatibility contract](./docs/compatibility.md) — supported routes and
  providers, deferrals, the `0.x` stability promise.
- [Usage schema](./docs/usage-schema.md) — the versioned usage row.
- [Security review](./docs/security-review-2026-08-05.md) and
  [release readiness](./RELEASE.md).

## Development

Toolchain is pinned in [`rust-toolchain.toml`](./rust-toolchain.toml). The checks
that CI enforces:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo doc --workspace --no-deps --all-features --locked   # RUSTDOCFLAGS=-D warnings
cargo deny --locked --all-features check                  # advisories, licenses, sources
bash ops/docker-smoke.sh "$(docker build -q .)"           # image boots and serves /healthz
```

`cargo test` includes the black-box suites in [`crates/gateway/tests`](./crates/gateway/tests):
a real `axond` process against a fake upstream replaying the committed wire
fixtures in [`tests/fixtures`](./tests/fixtures), plus a short SSE soak. Two
heavier lanes run outside it:

```bash
just compat   # the vendors' own Python SDKs against a real axond (its own CI lane)
just soak     # the long soak: hundreds of concurrent streams (weekly / on demand)
```

Everything is offline: no provider account, no key, no network. See
[ADR 0014](./docs/adr/0014-compatibility-and-soak-harness.md).

## Releases

Releases are automated with [release-please](https://github.com/googleapis/release-please)
off [Conventional Commits](https://www.conventionalcommits.org/) — the same
pipeline shape as the sibling `actord` and `custodian` repos:

- Merges to `main` keep a release PR open; merging it tags `v<major>.<minor>.<patch>`,
  updates [`CHANGELOG.md`](./CHANGELOG.md), and bumps the workspace version.
- Cutting a release builds, per tagged commit: cross-platform binaries
  (`x86_64` gnu + static musl Linux, `aarch64` macOS, `x86_64` Windows) with
  SHA-256 checksums, and a signed + SBOM-attested OCI image at
  `ghcr.io/litvue/axond`. All artifacts carry SLSA build provenance; the image
  is signed keylessly with cosign and verified before the release completes.
- PR titles are gated to Conventional Commits, and a daily job re-audits the
  released `main` against new advisories.

The release PR is authored by the org-wide release GitHub App
(`RELEASE_PLEASE_APP_ID` / `RELEASE_PLEASE_APP_PRIVATE_KEY`, the same
organization config `actord` and `custodian` use) so that it triggers CI; if the
repo is outside the App's scope the pipeline falls back to `GITHUB_TOKEN` and the
release PR is not CI-validated until it merges.

## License

Dual-licensed under either of [Apache-2.0](./LICENSE-APACHE) or
[MIT](./LICENSE-MIT) at your option. Contributions are accepted under the same
terms per the Apache-2.0 §5 inbound=outbound convention.
