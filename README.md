# Axond

A stateless, single-binary, self-hosted **AI gateway**. Point your agents at
Axond instead of at provider APIs directly, and get one place to hold
provider keys, route model names, meter usage, and emit telemetry.

> **Status: early scaffold.** The architecture, config surface, and the
> core ↔ transport seam are in place, with a working OpenAI-compatible path,
> buffered and streamed. Cross-provider failover, OTel export, and the Postgres/Tinybird/Redis backends are on the roadmap below.

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
  (stdout today; Postgres/Tinybird/ClickHouse/OTLP planned); spend budgets —
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
export GW_PLATFORM_OPENAI_API_KEY=sk-...     # secrets by env, referenced from config
cargo run -p axond                        # or: just run
```

```bash
curl localhost:8080/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}'
```

Point any OpenAI-compatible SDK at `http://localhost:8080/v1` as its base URL.

## Configuration

TOML owns all structure; the environment owns secrets (referenced by name) and
may override scalars. See [`axond.example.toml`](./axond.example.toml) for
the annotated surface: `server`, `namespace`, `provider`, `credential`,
`gateway_key`, and `model`.

## Architecture

```
crates/
  gateway-core        I/O-free provider adapters, wire translation, routing
                      primitives (circuit breaker, failover, catalog/pricing).
                      No HTTP client, no runtime, no config, no secrets.
  gateway-transport   The HTTP half: pooled reqwest client, credential
                      injection, timeouts (retries/failover next).
  gateway             The binary: config, namespaced credential resolution,
                      routes, UsageSink + BudgetStore wiring.
```

The split keeps the hard-won provider-wire logic testable in isolation and
runtime-neutral.

## Roadmap

- [x] Streaming (SSE) relay end-to-end (see [ADR 0005](./docs/adr/0005-streaming-relay.md))
- [ ] Ordered failover across targets + per-target circuit health
- [ ] OpenTelemetry traces (per-upstream-attempt spans, TTFT), metrics, logs
- [ ] Usage sinks: Postgres, Tinybird, ClickHouse, OTLP
- [ ] Budget backends: in-memory (present) → Redis / Postgres, reserve-then-reconcile
- [ ] Native Anthropic `/v1/messages` passthrough; `/v1/embeddings`, `/v1/responses`
- [ ] Multiple credentials per provider (pooling, weighted, skip-on-429)
- [ ] Config hot-reload (SIGHUP / watched files) for zero-restart BYOK onboarding
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
