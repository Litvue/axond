# Deployment guide

Axond is one stateless process. It reads a TOML file for structure, the
environment for secrets, and — unless you opt into a datastore-backed feature —
touches nothing else. Scaling out is running more replicas behind a load
balancer; there is no leader, no local state, and nothing to migrate.

For what each config key means, see the [configuration reference](./configuration.md).
For what to watch once it is running, see the [observability runbook](./observability.md).

## What you need before you start

- A config file. Copy [`axond.example.toml`](../axond.example.toml) and edit it.
- Every environment variable that file references, set on the **gateway's own
  process** — provider credentials (`[[credential]] env`), inbound gateway keys
  (`[[gateway_key]] env`), and any sink/budget DSN (`dsn_env`). A declared
  reference that is unset or empty is a **fatal boot error**, by design: the
  gateway fails at boot rather than at request time.
- At least one `[[gateway_key]]`. Inbound authentication fails closed and there
  is no keyless mode ([ADR 0013](./adr/0013-inbound-auth-fails-closed.md)).

## Minimal working config

```toml
[server]
bind = "0.0.0.0:8080"

[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "https://api.openai.com/v1"

[[credential]]
namespace = "platform"
provider = "openai"
env = "GW_PLATFORM_OPENAI_API_KEY"

[[gateway_key]]
env = "GW_INBOUND_PLATFORM_KEY"
namespace = "platform"

[[model]]
name = "gpt-4o"
targets = [
  { provider = "openai", model = "gpt-4o", price = { input_microdollars_per_million = 2_500_000, output_microdollars_per_million = 10_000_000 } },
]
```

That is the whole floor: one namespace marked `default`, one provider, one
credential, one inbound key, and one priced alias. Everything else —
credential pools, failover tuning, budgets, usage sinks, hot-reload — has a
default and may be omitted.

## Running the static binary

Release tags publish per-target archives with SHA-256 sidecars, SPDX SBOMs, and
build provenance attestations (`x86_64-unknown-linux-gnu`,
`x86_64-unknown-linux-musl`, `aarch64-apple-darwin`, `x86_64-pc-windows-msvc`).

```bash
tag=v0.0.1                       # the release you are installing
target=x86_64-unknown-linux-musl

gh release download "$tag" --repo Litvue/axond \
  --pattern "axond-*-${target}.tar.gz*"
sha256sum -c "axond-${tag#v}-${target}.tar.gz.sha256"
gh attestation verify "axond-${tag#v}-${target}.tar.gz" --repo Litvue/axond
tar -xzf "axond-${tag#v}-${target}.tar.gz"
```

The musl archive is a static-PIE binary: no libc, no shell, nothing to install.
Run it with the config path in the environment:

```bash
export AXOND_CONFIG=/etc/axond/axond.toml
export GW_PLATFORM_OPENAI_API_KEY=sk-...
export GW_INBOUND_PLATFORM_KEY=...
./axond
```

`AXOND_CONFIG` defaults to `axond.toml` in the working directory.

The same binary has offline-only `keygen` and `mint` subcommands. They dispatch
before telemetry or gateway configuration is loaded. `keygen` never needs a
serving config. `mint` uses an explicitly supplied `--config` when requested;
an `AXOND_CONFIG` value is only an ambient aid for inferring a matching
verifier's algorithm, audience, namespace permission, and `max_ttl`. Signing
material is always read from the environment. On Unix, `keygen` writes the
base64 PKCS#8 private key to a new `0600` file; on non-Unix platforms it warns
that permissions are inherited and must be restricted manually. It prints only
the base64 raw public key plus a verifier snippet; `mint` prints only the
`axt1.` token to stdout.

```bash
axond keygen --private-key ./acme-signing.key \
  --kid acme-2026-08 --env GW_VERIFY_ACME_2026_08 \
  --namespace acme --max-ttl 15m
export GW_SIGN_ACME="$(cat ./acme-signing.key)"
axond mint --kid acme-2026-08 --alg EdDSA --key-env GW_SIGN_ACME \
  --namespace acme --subject agent-1 --ttl 10m \
  --audience acme-production
```

`mint` always enforces the 24-hour policy ceiling. An unloadable explicit config
is fatal; an unloadable ambient `AXOND_CONFIG` produces a warning on stderr and
minting continues with only the policy ceiling. Without a usable matching
verifier config, it cannot know that verifier's configured `max_ttl`; such a
token may be minted and is rejected at gateway verification if it exceeds the
configured bound.

Under systemd, put the secrets in an `EnvironmentFile` rather than the unit, and
keep `Restart=on-failure`: a boot failure means the config or the environment is
wrong, and restarting will (correctly) keep failing until it is fixed.

```ini
[Service]
ExecStart=/usr/local/bin/axond
Environment=AXOND_CONFIG=/etc/axond/axond.toml
EnvironmentFile=/etc/axond/axond.env
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
```

`systemctl reload axond` then re-reads the config file and the process
environment without dropping a connection (see [hot-reload](#hot-reload-and-rotation)).

## Running the container image

`ghcr.io/litvue/axond` is a distroless image holding the same static binary and
nothing else — no shell, no package manager — and it runs as `nonroot`. It is
signed keylessly with cosign and carries SLSA provenance plus an SPDX SBOM
attestation. Verify before you run it:

```bash
image=ghcr.io/litvue/axond:0.0.1
digest=$(crane digest "$image")   # or read the *.digest asset on the release

cosign verify \
  --certificate-identity-regexp '^https://github\.com/Litvue/axond/\.github/workflows/release-please\.yml@(refs/heads/main|refs/tags/v[0-9]+\.[0-9]+\.[0-9]+)$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  "ghcr.io/litvue/axond@${digest}"

gh attestation verify "oci://ghcr.io/litvue/axond@${digest}" \
  --repo Litvue/axond --predicate-type https://slsa.dev/provenance/v1
```

Tags are the bare version (`0.0.1`) and `sha-<short-sha>`; there is no `latest`.
Deploy by digest.

The image ships no config, so mount one in:

```bash
docker run --rm -p 8080:8080 \
  -e AXOND_CONFIG=/etc/axond/axond.toml \
  -e GW_PLATFORM_OPENAI_API_KEY=sk-... \
  -e GW_INBOUND_PLATFORM_KEY=... \
  -v "$PWD/axond.toml:/etc/axond/axond.toml:ro" \
  "ghcr.io/litvue/axond@${digest}"
```

[`ops/docker-smoke.sh`](../ops/docker-smoke.sh) does exactly this against the
example config and probes `/healthz`; it runs in CI on every PR and again
against the published image before it is signed.

On Kubernetes: mount the config from a ConfigMap, take every secret from a
`Secret` via `envFrom`, and — because the process is stateless — scale the
Deployment freely. Only the in-memory pieces are per-replica (circuit breakers,
credential health, `backend = "in-memory"` budgets); shared budgets need Redis
or Postgres.

## Environment variables

| Variable | Required | Meaning |
| --- | --- | --- |
| `AXOND_CONFIG` | no | Path to the TOML config. Defaults to `axond.toml`. |
| `GW_*` / any name you choose | yes | The secrets your config references by name. The names above are the example's; the config decides them. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | no | OTLP/HTTP collector, e.g. `http://collector:4318`. Unset means logs only — no exporter, tracer, meter, or propagator is installed. |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | no | Only `http/protobuf` is supported. Any other value (including `grpc`) is a boot error rather than a silent no-op. |
| `OTEL_EXPORTER_OTLP_HEADERS` | no | Standard OTel exporter headers (auth for a hosted collector). |
| `RUST_LOG` | no | Log filter, standard `tracing` syntax. Defaults to `info,axond=info`. |
| `AXOND_<SECTION>__<KEY>` | no | Overrides a config scalar, e.g. `AXOND_SERVER__BIND=0.0.0.0:9090`. Structure still belongs in TOML. |

Secrets are always referenced **by name** from the config and read from the
process environment. Nothing in the config file is ever a secret value.

## Health and readiness

| Endpoint | Auth | Answer |
| --- | --- | --- |
| `GET /healthz` | none | `ok` once the process is serving. |
| `GET /readyz` | none | `ready` once the process is serving. |
| `GET /v1/models` | gateway key | The alias catalogue (names only), scoped to the caller's namespace — only aliases whose targets the caller holds a credential for. |

Minted callers may additionally carry repeatable `--scope` capabilities
(`chat`, `messages`, `embeddings`, or `models`). Scope only narrows the derived
namespace authority; denial is `403 token_scope_insufficient`. Static keys and
scope-less tokens are unaffected. `/v1/responses` remains a typed `501`.

Both probes report process liveness. `/readyz` does **not** currently probe the
usage sink, the budget store, or any provider — it answers `ready` whenever the
listener is up. That is deliberate rather than aspirational: the dependencies it
could check are all validated at boot, so a process that is listening has
already proven them once. Use `/healthz` for liveness and `/readyz` for
readiness gates today, and alert on the request-path metrics in the
[runbook](./observability.md) for dependency health.

Boot ordering matters for rollouts: config validation, credential resolution,
usage-sink connection, and budget-store connection all happen **before** the
socket is bound. A replica that answers `/healthz` has a valid config and live
connections to whatever datastores it was told to use.

## Telemetry

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318   # OTLP/HTTP only
```

Setting it installs the tracer, meter, logger, and W3C propagator with
`service.name = axond`. Leaving it unset is a fully supported posture: the
process writes JSON logs to stdout and exports nothing. See the
[observability runbook](./observability.md).

## Hot-reload and rotation

`SIGHUP` re-runs the whole boot validation against the current file **and the
current process environment**, and publishes the result atomically. A candidate
that fails validation is rejected and the running config keeps serving. Set
`[reload] watch = true` to additionally reload when the file's contents change
(a ConfigMap swap, for instance).

Rotate an inbound key or a provider credential without downtime by declaring the
new one alongside the old, reloading, moving callers over, then dropping the old
one and reloading again. Two gateway keys may not hold the same secret, so the
two must be distinct values.

`[server] bind` and `[[usage_sink]]` changes are logged with a warning and
otherwise ignored — the socket is already bound and sinks own live connections.
Both need a restart.

## Sizing and stateful opt-ins

Choose the state tier before sizing the deployment. Tiers describe the state
backends axond itself depends on, not provider egress: upstream provider calls
still use the network at Tier 0. Tier 0 is config-only and has no datastore;
credential health, circuits, and in-memory budgets are per replica. The
hermetic CI gate in [ADR 0018](./adr/0018-tier-0-hermetic-boot-gate.md) proves
Tier 0 boot-and-serve on every PR.

Tier 1 adds Redis for shared budget enforcement and exact inbound in-flight
rate limiting. Redis availability becomes part of the selected admission path;
the default `on_unavailable = "deny"` fails closed with `503 budget_unavailable`
or `503 rate_limit_unavailable`. Precise per-token revocation remains future.

Tier 2 adds Postgres-backed durable usage and shared budgets. It requires
database roles, boot-time DSN resolution, ordered migrations, and explicit
backup/restore ownership. A Postgres dependency is therefore part of startup,
and migrations must precede a binary that writes a new usage schema. The
one-owner rule is described in the [configuration reference](./configuration.md#state-tiers):
namespaces, providers, aliases, prices, and provider credentials remain
config-owned; only callers and keys may become store-owned.

| Tier | Want | Turn on | Costs |
| --- | --- | --- | --- |
| 0 | Default operation, local health, failover, reload, stdout usage, or an approximate per-replica spend cap | Omit `[[usage_sink]]` and `[budget]`, or use `[budget] backend = "in-memory"` | No datastore; a fleet has one in-memory budget per replica. |
| 0* | Usage in your trace backend | `[[usage_sink]] kind = "otlp"` | Requires `OTEL_EXPORTER_OTLP_ENDPOINT` at boot; Tier 0 state but not hermetic, so it is outside the Tier 0 CI lane. |
| 1 | A spend cap across replicas | `[budget] backend = "redis"` | Redis availability couples budgeted admission to Redis; `deny` fails closed. |
| 2 | Durable usage rows | `[[usage_sink]] kind = "postgres"` | A Postgres role, the [`usage_v1.sql`](../ops/postgres/usage_v1.sql) table, ordered additive migrations, and backup/restore ownership. |
| 2 | A spend cap across replicas | `[budget] backend = "postgres"` | Postgres availability and the [`budget_v1.sql`](../ops/postgres/budget_v1.sql) tables. |

For upgrades, apply each additive `usage_v1_<sequence>_<name>.sql` migration in
filename order before deploying a gateway that writes its new column. Otherwise
the sink fails those batches and drops usage rows, surfaced as `sink_error` on
the dropped-records metric, until the migrations are applied.

A `[budget]` backend of `in-memory` enforces the cap **per replica**, so a fleet
of N replicas admits up to N caps. Use a shared backend for a real fleet-wide
cap.
