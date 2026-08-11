# Deployment guide

Axond is one stateless process. It reads a TOML file for structure, the
environment for secrets, and — unless you opt into a datastore-backed feature —
touches nothing else. Scaling out is running more replicas behind a load
balancer; there is no leader, no local state, and nothing to migrate.

For what each config key means, see the [configuration reference](./configuration.md).
For what to watch once it is running, see the [observability runbook](./observability.md).

## 5-minute quickstart

This path builds the local distroless image from the Dockerfile rather than
pulling a registry image. The first build compiles the static musl release and
can take several minutes; later starts reuse the cached image.

The example inbound keys are public values for local use only. Replace them
before exposing the gateway beyond your own machine.

```bash
git clone https://github.com/Litvue/axond.git
cd axond
cp ops/compose/env.example .env
docker compose up -d --build
curl http://localhost:8080/healthz
curl -H 'Authorization: Bearer quickstart-platform-key' \
  http://localhost:8080/v1/models
curl -X POST http://localhost:8080/v1/chat/completions \
  -w '\nHTTP %{http_code}\n' \
  -H 'Authorization: Bearer quickstart-platform-key' \
  -H 'content-type: application/json' \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"Say hello in one word."}]}'
docker compose down -v
```

The health probe returns `ok`, and the authenticated catalogue lists the
aliases available to the platform namespace:

```text
ok
{"data":[{"id":"gpt-4o","object":"model","owned_by":"axond"},{"id":"claude-sonnet","object":"model","owned_by":"axond"},{"id":"text-embedding-3-small","object":"model","owned_by":"axond"}],"object":"list"}
```

With the committed placeholder key, the chat request returns HTTP `502` and
an example of the following typed provider error:

```text
{"error":{"message":"invalid provider request: Incorrect API key provided: placehol**********-key. You can find your API key at https://platform.openai.com/account/api-keys.","type":"invalid_request"}}
HTTP 502
```

A real key in `GW_PLATFORM_OPENAI_API_KEY` returns the provider's normal HTTP
`200` chat-completion response.

The exact provider message varies with network and provider responses; an
air-gapped run returns a typed `upstream_transport` error instead. Keep `.env`
until after `docker compose down -v`, because required-variable interpolation
runs before every Compose command. To run `just quickstart-smoke`, tear down
the quickstart first because the smoke uses the same host port. If port 8080
is occupied by another local stack, use
`AXOND_QUICKSTART_SMOKE_PORT=18080 just quickstart-smoke`.

To try the stateful variant, select the Tier 1 Redis budget/rate-limit backends
and the Tier 2 Postgres durable usage sink:

```bash
export AXOND_QUICKSTART_CONFIG=./ops/compose/axond.stateful.toml
docker compose \
  -f docker-compose.yml -f docker-compose.stateful.yml \
  --profile stateful up -d --build
curl -X POST http://localhost:8080/v1/chat/completions \
  -w '\nHTTP %{http_code}\n' \
  -H 'Authorization: Bearer quickstart-platform-key' \
  -H 'content-type: application/json' \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"Say hello in one word."}]}'
```

The stateful configuration creates the Postgres usage table at boot. Redis and
Postgres are required dependencies in this path; admission fails closed when
Redis is unavailable, and usage rows are durable in Postgres.

After the chat request, the usage sink batches rows. Poll for the durable Tier
2 usage row with:

```bash
for attempt in $(seq 1 12); do
  count="$(docker compose -f docker-compose.yml -f docker-compose.stateful.yml \
    --profile stateful exec -T postgres psql -U postgres -d axond -Atc \
    "select count(*) from axond_usage;")"
  if [ "$count" = 1 ]; then
    printf '%s\n' "$count"
    break
  fi
  sleep 1
done
test "$count" = 1
```

Observed output after the placeholder chat request:

```text
1
```

Keep `.env` in place for this query and for teardown. When finished:

```bash
docker compose -f docker-compose.yml -f docker-compose.stateful.yml \
  --profile stateful down -v
```

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
| `GET /v1/credentials` | gateway key | Replica-local credential labels and circuit state for the caller's namespace; `?namespaces=all` is the operator view and needs a static key in the default namespace. |

Minted callers may additionally carry repeatable `--scope` capabilities
(`chat`, `messages`, `embeddings`, `responses`, `models`, `credentials`, or
`credentials:all`). Scope only narrows the derived namespace authority;
scope-less principals retain their own-namespace credential view. A scoped
token needs `credentials` for the route. `/v1/responses` requires the
`responses` capability for scoped callers.

The all-namespaces credential view is reached with a scope-less static
`[[gateway_key]]` in the configured default namespace — in practice the
breakglass key — and not with a token:

```sh
curl -sS -H "Authorization: Bearer $GW_INBOUND_PLATFORM_KEY" \
  'http://localhost:8080/v1/credentials?namespaces=all'
```

A static key in a tenant namespace, and every minted token, gets
`403 token_scope_insufficient` naming `credentials:all` — including a token
that carries that claim, which `POST /v1/tokens` refuses to mint anyway. So an
operator who needs the fleet-wide credential view keeps a default-namespace
static key rather than trying to mint one
([ADR 0021](./adr/0021-credential-status-endpoint.md)).

Credential status is Tier 0, in-memory, and per replica: `observed: "replica"`
is not a fleet-wide health view. Presence is represented by an entry (boot
resolves configured credentials or fails), and credential ids are attribution
labels, never secrets. The default env-derived `credential_id` is omitted for
platform entries shown through tenant fallback; explicit ids remain visible.

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
| 2 | Durable usage rows | `[[usage_sink]] kind = "postgres"` | A Postgres role, the [`usage_v2.sql`](../ops/postgres/usage_v2.sql) table, ordered additive migrations, and backup/restore ownership. |
| 2 | A spend cap across replicas | `[budget] backend = "postgres"` | Postgres availability and the [`budget_v1.sql`](../ops/postgres/budget_v1.sql) tables. |
| 1 or 2 | A cap on a whole namespace, not just each subject | `[budget] namespace_limit_microdollars` on a `redis` or `postgres` budget | A one-time migration with the fleet stopped for either backend (below), and contention on one key or row per namespace. |

For upgrades, apply each additive `usage_v1_<sequence>_<name>.sql` migration in
filename order before deploying a gateway that writes its new column. Otherwise
the sink fails those batches and drops usage rows, surfaced as `sink_error` on
the dropped-records metric, until the migrations are applied.

A `[budget]` backend of `in-memory` enforces the cap **per replica**, so a fleet
of N replicas admits up to N caps. Use a shared backend for a real fleet-wide
cap.

### Enabling a namespace-wide cap

`namespace_limit_microdollars` caps everything a namespace spends across all its
subjects, which is what bounds a namespace whose holder can mint fresh subjects
(see the [minted-token guide](./minted-token-guide.md)). Only `redis` and
`postgres` can enforce it exactly across replicas, so the other backends reject
it at boot. Turning it on is a **migration**, not a config flip, because the
accumulated spend has to be carried into the new shape:

**Redis.** Stop every replica, then run the migration and start the fleet with
the cap set:

```bash
axond budget migrate-redis --config /etc/axond/axond.toml
```

Do not run old and new binaries at the same time. A version without namespace-cap
support writes the previous key layout, so the two would each enforce a share of
the traffic. The gateway fences this rather than trusting the runbook: with the
cap set it refuses to boot until the migration marker exists and refuses to boot
while any old-layout key remains; once migrated, it refuses to boot *without* the
cap, because the old keys no longer hold the spend.

Re-running the migration is safe, and is the repair for a replica that slipped
through: spend is claimed out of an old counter atomically and added to the new
one at most once, so an interrupted run loses nothing, a repeat run adds nothing
twice, and spend a stray old replica recorded *after* the first run is carried
over rather than discarded. The report line names the amount, so a non-zero carry
on a re-run is the signal that something was still writing the old layout.

An interrupted run is fenced rather than papered over, in both of the ways it can
stop. The migration marks `<key_prefix>:layout` as `v2-migrating` before it moves
anything and writes `v2` only once every subject is across, so a run that carried
some subjects and then failed on a later one is visible even though it leaves
nothing else behind — a carried subject has no old key — and no configuration will
serve a `v2-migrating` prefix, since the ledger is then split between the layouts.
Spend taken off an old counter but not yet added to the new one sits in a
`:migration_pending` claim, which the cap-enabled boot check refuses as well. A
gateway *without* the cap reads the marker and nothing else, so enabling this
feature costs an un-migrated deployment one `GET` at boot and no keyspace scan.
Re-running the migration resumes where it stopped and clears both states; that is
all it takes.

The migration attributes old keys by resolving their `{namespace|subject}` tag
against the namespaces in your config. Neither half of that tag was escaped, so
`{team|west|abc}` could mean namespace `team` or `team|west`; rather than guess,
the migration stops with the offending key named, having moved and deleted
nothing. That happens when a namespace has been removed from the config while its
spend is still in Redis (add it back, or delete the key), or when one namespace id
is a prefix of another such that both could own the key (rename, or migrate under
a separate `key_prefix`).

In-flight reservations are not carried over, which is the other reason to stop
traffic first.

Turning the cap back off is deliberately not a config flip either, and there is
no un-migrate command: the old keys are gone, so a cap-less binary would read
zero spend for every subject and hand each one a fresh budget — which is why it
refuses to boot against migrated state. Dropping the cap therefore means
accepting a spend reset, which you do explicitly: stop the fleet and either
delete `<key_prefix>:v2:*` together with `<key_prefix>:layout`, or move the
deployment to a new `key_prefix`. Note that the layout marker is per
`key_prefix`, so two deployments sharing one Redis and one prefix cannot disagree
about the cap; give them separate prefixes if they need to.

**Postgres.** Stop and drain the fleet here too, then apply
[`budget_v2.sql`](../ops/postgres/budget_v2.sql) on top of `budget_v1.sql` — it is
additive, and its backfill seeds each namespace total from the subject rows
already present:

```bash
psql "$AXOND_BUDGET_POSTGRES_DSN" -f ops/postgres/budget_v2.sql
```

It is not a live migration. The backfill sums the subject rows, so a settlement
that commits after that sum would be counted against its subject and never
against its namespace, leaving a total that is permanently short — and no boot
check can detect that, because the row exists and merely holds too small a
number. The file therefore runs as one transaction holding an `EXCLUSIVE` lock on
the spend table, which blocks a v1 settlement rather than losing it; stopping the
fleet is what keeps that lock from being an outage. Re-running it is idempotent,
and `create_table = true` applies the same statements at boot.

Mixed configurations are fenced by the database, not by this runbook. The file
installs a trigger that rejects spend and reservation writes from any session that
has not declared namespace-cap support, which a cap-aware gateway does once per
connection. A replica still configured without `namespace_limit_microdollars` —
including a binary too old to have a boot check — therefore cannot charge a
subject while leaving the namespace total behind; its writes fail loudly instead.
Such a replica also refuses to boot, naming the fence, and a cap-enabled replica
refuses to boot if the fence is missing from either table or a namespace has spend
but no backfilled row, so the cap can neither begin from zero nor be quietly
bypassed. To return to per-subject-only enforcement, stop the fleet and drop the
two `<table>_namespace_fence` triggers.

The declaration the fence looks for is sent once per connection *and* again, as
`SET LOCAL`, inside every transaction that reserves or settles. So a pooler in
transaction mode cannot route a write to a backend where the declaration never
ran — which would otherwise have booted cleanly and then had every reserve and
settlement rejected by the fence. `create_table = true` also
re-applies the DDL on boot, but skips the v2 file once the schema is installed and
backfilled, so a restart does not re-take the `EXCLUSIVE` lock or re-run the
aggregate against a live fleet.

Both backends then concentrate a namespace's traffic on one hot spot — one spend
row in Postgres, one counter and reservation hash in Redis — and every reserve
scans that namespace's live reservations. Size for that when a single namespace
carries the bulk of the fleet's requests.
