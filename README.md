# Axond

A stateless, single-binary, self-hosted **AI gateway**. Point your agents at
Axond instead of at provider APIs directly, and get one place to hold
provider keys, route model names, meter usage, and emit telemetry.

> **Status: early scaffold.** The architecture, config surface, and the
> core ↔ transport seam are in place, with a working OpenAI-compatible path,
> buffered and streamed, ordered failover across targets, and OTLP telemetry.
> The Postgres/Tinybird/Redis backends are on the roadmap below.

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
- **Passthrough-first.** When the caller's wire shape already matches the target,
  the body is forwarded and only the `model` field is rewritten. Cross-provider
  translation happens only when routing requires it.
- **Fail at boot, not at request time.** The whole config graph is validated on
  startup: undefined alias targets, empty target lists, and dangling
  namespace/provider references refuse to start.
- **Every route always exists.** Unavailable features return typed errors
  explaining themselves — never a bare 404 that's indistinguishable from a wrong
  `base_url`.
- **Credentials are write-only.** No endpoint ever returns a key; only presence
  is observable.

## Quick start

```bash
cp axond.example.toml axond.toml      # edit providers/models/namespaces

# Secrets by env, referenced from config. Every credential the config declares
# must resolve, so either set all of these or delete the entries you don't want.
export GW_PLATFORM_OPENAI_API_KEY=sk-...
export GW_PLATFORM_OPENAI_API_KEY_OVERFLOW=sk-...   # second key in the openai pool
export GW_PLATFORM_ANTHROPIC_API_KEY=sk-ant-...
export GW_PLATFORM_AZURE_OPENAI_API_KEY=...         # gpt-4o's failover target
export GW_ACME_OPENAI_API_KEY=sk-...                # the example's BYOK namespace

cargo run -p axond                    # or: just run
```

```bash
curl localhost:8080/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}'
```

Point any OpenAI-compatible SDK at `http://localhost:8080/v1` as its base URL.

### Native provider routes

An Anthropic client works the same way — point it at the same base URL and send
Anthropic's own wire to `/v1/messages`:

```bash
curl localhost:8080/v1/messages \
  -H 'content-type: application/json' \
  -d '{"model":"claude-sonnet","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}'
```

The body is forwarded to the provider untouched but for `model`, streaming
included, so signed thinking blocks and tool-use blocks survive intact (verbatim
bytes on a stream; re-serialized values, same signatures, when buffered) —
and aliasing, failover, credential pools, budgets, and usage records apply exactly
as they do on `/v1/chat/completions`. `/v1/embeddings` is served the same way for
OpenAI-family targets, billed on input tokens only. A native alias must resolve to
targets that speak that wire: an OpenAI-only alias on `/v1/messages` is a
`400 unsupported_wire`. `/v1/responses` is deferred past beta and returns a typed
`501`. See [ADR 0012](./docs/adr/0012-native-provider-routes.md).

## Configuration

TOML owns all structure; the environment owns secrets (referenced by name) and
may override scalars. See [`axond.example.toml`](./axond.example.toml) for
the annotated surface: `server`, `namespace`, `provider`, `credential`,
`credential_pool`, `gateway_key`, `model`, and `usage_sink`.

Every env var a `[[credential]]` references must be set and non-empty at boot, or
the gateway refuses to start.

### Credential pools

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

## Usage sinks

Every terminated request — including failures, cancellations, and partial
streams — produces one canonical usage record. With no `[[usage_sink]]`
configured the record is written as one JSON line on stdout and the process
touches no datastore. Durable destinations are opt-in:

```toml
[[usage_sink]]
kind = "postgres"
dsn_env = "AXOND_USAGE_POSTGRES_DSN"   # the DSN is a secret: referenced, never inlined
create_table = true                   # or apply ops/postgres/usage_v1.sql yourself

[[usage_sink]]
kind = "otlp"                         # usage as OTel log records, on the existing exporter
```

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

## Config hot-reload

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
re-read on every reload, which is what makes a newly-referenced credential
resolve; it must be set on the gateway's process, not just in your shell.

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
- [x] Budget backends: shared Redis / Postgres, held reservations, partial charging (see [ADR 0010](./docs/adr/0010-shared-budget-backends-and-charging-policy.md))
- [x] Native Anthropic `/v1/messages` passthrough + `/v1/embeddings` (see [ADR 0012](./docs/adr/0012-native-provider-routes.md)); `/v1/responses` deferred post-beta (typed `501`)
- [x] Multiple credentials per provider (pooling, weighted, skip-on-429)
- [x] Config hot-reload (SIGHUP / watched files) for zero-restart BYOK onboarding (see [ADR 0011](./docs/adr/0011-config-hot-reload.md))
- [ ] Provider-SDK compatibility + record/replay + SSE soak tests

See [`docs/adr`](./docs/adr) for the decisions behind these.

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
