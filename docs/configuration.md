# Configuration reference

Every section, key, and default the gateway reads today, cross-checked against
[`crates/gateway/src/config.rs`](../crates/gateway/src/config.rs).
[`axond.example.toml`](../axond.example.toml) is the same surface with prose;
this is the lookup table.

Axond is a **store-backed inference gateway**
([ADR 0063](./adr/0063-stateful-only-namespaced-gateway.md)). There is no
stateless/config-only product, no in-memory or Redis budget tier, and no
`/admin/v1` control plane. Historical ADRs that described those shapes remain
on disk as records; this page is the live contract.

Two rules hold everywhere:

- **TOML owns process structure, and secrets are referenced rather than
  inlined.** A key, DSN, or token is referenced by an environment-variable name
  or, for gateway key material, a file path. No config key takes material
  inline.
- **Fail at boot, not at request time.** The whole graph is validated before the
  socket is bound, and again on every reload. Anything listed as "rejected"
  below is a boot error (or, on reload, is rejected while the previous config
  keeps serving).

Loading order: the TOML file (`AXOND_CONFIG`, default `axond.toml`), then
`AXOND_`-prefixed environment variables layered on top with `__` as the section
separator (`AXOND_SERVER__BIND=0.0.0.0:9090`). The override layer is for scalars
in containerized deploys; structure belongs in the file.

TOML plus env holds bind, providers, credentials, **exactly one**
`[[gateway_key]]`, `[storage]`, the deployment price-book and blocklist, and
process bounds (`[admission]`, `[transport]`, `[shutdown]`, telemetry).
Namespaces, period budgets, and usage also live in the Store: file
`[[namespace]]` rows are seeded at boot; further namespaces are created through
`/api/v1`. Changing process config is a restart or a reload of the keys that
reload supports.

## State tiers

The live product is one Store. SQLite WAL is the single-replica
implementation; Postgres is the HA implementation. Boot requires `[storage]`; a
missing or unreachable store is a boot failure.

| Config section | Persistence |
| --- | --- |
| `[storage]` | **Required.** SQLite WAL or Postgres. Namespaces, period budgets, usage index, discovery cache. |
| `[server]`, `[[provider]]`, `[[price]]`, `[blocklist]`, `[[credential]]` | Process config. |
| `[[namespace]]` | Seeded into the Store at boot; runtime create/update/delete is `/api/v1`. |
| `[credential_pool]`, `[failover]` | In-memory, per replica. Alias-level failover is gone; credential-pool rotation inside one provider remains. |
| `[transport]`, `[admission]`, `[shutdown]` | Process-level bounds. |
| `[[gateway_key]]` | **Exactly one** deployment-wide static key. |
| `[reload]` | Re-reads the config file, referenced key-material files, and process environment. Not a live channel for `[storage]`. |
| `[[usage_sink]]` omitted or `kind = "stdout"` | One JSON line on stdout. |
| `[[usage_sink]] kind = "otlp"` | Process-local export; a collector is a boot-time dependency. |
| `[[usage_sink]] kind = "postgres"` | Durable usage rows (optional sink; the Store already has a usage index). |
| `[usage_journal]` omitted or `backend = "none"` | Telemetry-grade usage delivery. |
| `[usage_journal] backend = "postgres"` | Durable usage outbox on the request path. |
| `[budget]` | Hold TTL only. `backend = "redis"\|"postgres"\|"in-memory"` is a boot error. Caps are `PUT /api/v1/namespaces/{ns}/budgets/{period}`. |
| `[rate_limit]` | Optional per-replica or Redis in-flight limiter. Not a budget backend. |
| `/healthz`, `/readyz` | Unauthenticated liveness / readiness. |

`[[model]]` is a boot error. Callers send `provider-id/model-id`. There is no
config hot-reload of a model table.

## Operating mode

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `mode` | withdrawn | omitted | Any presence is a boot error, including `mode = "stateless"` and `mode = "stateful"`. Axond is store-backed; there is no mode matrix. Remove the key. |

Omitting `mode` is the only valid configuration. The two-mode product in
[ADR 0027](./adr/0027-stateless-and-stateful-operating-modes.md) is superseded
by [ADR 0063](./adr/0063-stateful-only-namespaced-gateway.md).

### Stateful bootstrap

These sections still parse far enough for diagnostics to name them, then fail
validation. They are not a supported deployment. The parser-accepted field
names are listed so an old file's error is readable; do not copy them into a
new config. [`axond.stateful.example.toml`](../axond.stateful.example.toml) is
the withdrawn file with prose — it is not a runnable bootstrap.

A referenced env var must also stay clear of the `AXOND_<section>` shape,
because `AXOND_`-prefixed variables are the override layer: `AXOND_ADMIN_BREAKGLASS`
would be merged as the `admin_breakglass` *key* rather than resolved as a
reference. Such a name is rejected at validation, naming the variable and the
key it collides with. Live examples use the `GW_` prefix for secret-bearing
variables.

Presence of `[control_plane]`, `[secret_store]`, `[[admin_breakglass]]`,
`[admin_oidc]`, or `[convergence]` is a boot error: Axond has no control-plane
mode and does not serve `/admin/v1`.

#### `[control_plane]`

Withdrawn. Presence is a boot error.

| Key | Type | Default | Meaning (historical) |
| --- | --- | --- | --- |
| `backend` | `object-storage` \| `postgres` | `postgres` | Durable control-plane implementation. Redis and memory were refused. |
| `dsn_env` | string | — | **Postgres only.** Name of the env var holding the control-plane connection string. |
| `schema` | string | connection default | **Postgres only.** PostgreSQL journal schema. A single unqualified identifier. |
| `migrate` | boolean | `false` | **Postgres only.** Whether a booting process may apply pending migrations. |
| `environment_id` | string | — | **Object storage only.** Stable environment object-key segment. |
| `container_url` | absolute URL | — | **Object storage only.** Credential-free container URL. |
| `authentication` | `workload-identity` | — | **Object storage only.** Adapter workload-identity chain. |
| `max_object_bytes` | integer | `16777216` | **Object storage only.** Absolute object ceiling. |
| `max_read_bytes` | integer | `min(16777216, max_object_bytes)` | **Object storage only.** Streaming read ceiling. |
| `max_write_bytes` | integer | `min(16777216, max_object_bytes)` | **Object storage only.** Conditional-write ceiling. |
| `allow_loopback_http` | boolean | `false` | **Object storage only.** Insecure development/Azurite escape hatch. |
| `connect_timeout_ms` | integer | `5000` | Bound on establishing a backend connection. `0` was rejected. |
| `operation_timeout_ms` | integer | `30000` | Bound on one control-plane operation. `0` was rejected. |

Historical object-storage shape (does not boot):

```toml
mode = "stateful"

[control_plane]
backend = "object-storage"
environment_id = "prod-us-east"
container_url = "https://axondstate.blob.core.windows.net/control-plane"
authentication = "workload-identity"

[[admin_breakglass]]
env = "GW_ADMIN_BREAKGLASS"
```

#### `[secret_store]` (legacy PostgreSQL control plane)

Withdrawn. Presence is a boot error.

| Key | Type | Default | Meaning (historical) |
| --- | --- | --- | --- |
| `backend` | `postgres` | `postgres` | Which store held wrapped material. |
| `dsn_env` | string | `[control_plane] dsn_env` | Name of the env var holding the store's connection string. |
| `kek_env` | string | — | Name of the env var holding the key-encryption key. |
| `kek_file` | string | — | Path to a file holding the key-encryption key. |
| `schema` | string | connection default | PostgreSQL schema `axond_secret` lived in. |
| `create_table` | boolean | `true` | Whether boot applied `secret_store_v1.sql`. |

Exactly one of `kek_env` and `kek_file` had to be non-empty.

#### `[convergence]`

Withdrawn with the control plane. Presence of a non-default section is a boot
error.

| Key | Type | Default | Meaning (historical) |
| --- | --- | --- | --- |
| `cache_path` | path | unset | Per-replica signed desired-state cache. |
| `cache_key_env` | string | unset | Environment-variable name containing the cache HMAC key. |

#### `[[admin_breakglass]]`

Withdrawn. Presence is a boot error. `/admin/v1` is unmounted.

| Key | Type | Default | Meaning (historical) |
| --- | --- | --- | --- |
| `env` | string | — | Name of the env var holding the credential. |
| `file` | string | — | Path to a file holding the credential. |
| `id` | string | the source reference | Non-secret attribution label for audit events. |

Exactly one of `env` and `file` had to be non-empty.

#### `[admin_oidc]`

Withdrawn. Presence is a boot error.

| Key | Type | Default | Meaning (historical) |
| --- | --- | --- | --- |
| `issuer` | string | — | Exact `iss` claim accepted. |
| `audience` | string | — | Required `aud` value. |
| `jwks_url` | string | — | JWKS endpoint. |

## `[server]`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `bind` | socket address | `0.0.0.0:8080` | Listening address. Changing it needs a restart; a reload warns and ignores it. |

## `[storage]` — required (ADR 0063)

The durable namespace store. Boot refuses a config without this section. A
reload that changes `backend`, `path`, or `dsn_env` is reported and ignored
until restart — the live `Store` is opened once.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `backend` | `sqlite` \| `postgres` | `sqlite` | SQLite WAL for a single replica; Postgres for HA. |
| `path` | string | — | SQLite file path. Required for `sqlite`. `:memory:` is refused by `Config::load`. |
| `dsn_env` | env-var name | — | Postgres DSN environment variable. Required for `postgres`. The DSN's `sslmode` selects TLS. |
| `create_table` | bool | `true` | Apply namespace and `axond_store_budget*` DDL at connect. Postgres deployments that migrate out of band set `false`. |
| `on_unavailable` | `deny` \| `allow` | `deny` | When the Store cannot reserve: `deny` answers `503 budget_unavailable`; `allow` serves without a hold. |

SQLite rejects a set `dsn_env`; Postgres rejects a set `path`.

Postgres Store budget tables are `axond_store_budget`,
`axond_store_budget_active`, and `axond_store_budget_reservation`
(`ops/postgres/store_budget_v1.sql`). They do not reuse the withdrawn
`[budget] backend = "postgres"` names (`axond_budget*`). A database that
already has those leftover tables keeps them; spend is not migrated (subject
vs period). Connect still creates the Store tables and boots. An earlier
draft of the Store DDL that used `axond_budget` with a `period` column is
renamed at connect (needs table-rename privilege; migration-only roles
should run the rename out of band before boot).

The Store also holds the management usage index (`axond_store_usage`) that
`GET /api/v1/namespaces/{ns}/usage` reads. Litvue reads current-period
summaries. The gateway does not auto-prune this table: operators delete rows
they no longer need, for example

```sql
DELETE FROM axond_store_usage WHERE recorded_at < now() - interval '90 days';
```

or drop rows whose `period` is no longer billed. See
[`ops/postgres/store_usage_v1.sql`](../ops/postgres/store_usage_v1.sql).
SQLite stores `recorded_at` as unix seconds; prune by `period` there, or
`WHERE recorded_at < strftime('%s','now') - 90*24*60*60`.

## `[shutdown]` — Tier 0

Bounds on the `SIGTERM`/`SIGINT` sequence
([ADR 0029](./adr/0029-bounded-termination.md)). All three are read when the
signal arrives and enforced as one snapshot, so a reload applies to the
termination that follows it.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `drain_grace_ms` | integer | `5000` | How long `/readyz` fails while the replica *keeps* admitting work, so a load balancer can remove it before anything is refused. `0` closes admission immediately — only safe when something else already drained the endpoint. |
| `deadline_ms` | integer | `15000` | How long requests admitted before the close have to finish. Anything still open is cut: its response body ends in an error and a stream settles as `client_cancelled` up to the last relayed token. Rejected at `0`. |
| `flush_timeout_ms` | integer | `5000` | Bound on the whole post-serving sequence: settling cut responses, flushing buffered usage sinks, and flushing telemetry exporters. Settling gets at most half of it so a request that cannot end cannot starve the flush; anything still unsettled then is counted as abandoned. Records that cannot be written are counted as `shutdown` drops. Rejected at `0`. |

`/healthz` answers `ok` throughout; only `/readyz` reports the drain, because a
terminating replica is not an unhealthy one. Worst-case termination is the sum
of the three, so the supervisor's stopping timeout
(`terminationGracePeriodSeconds`, `TimeoutStopSec`, `docker stop -t`) must
exceed it or a `SIGKILL` lands mid-flush.

A second termination signal *during the drain window* skips the rest of it and
closes admission at once. With `drain_grace_ms = 0` there is no window to skip:
the first signal closes admission. Once admission is closed — by either route —
further signals are logged and otherwise ignored, because the deadline and the
flush budget already bound what is left and honoring one there would kill the
process mid-flush, discarding the usage records the sequence exists to write.

## `[[namespace]]`

The tenancy boundary: which credential pool a caller's requests draw from.
File rows are seeded into the Store at boot. Runtime create, replace, list, and
delete is `/api/v1/namespaces` (same static key). `id` is 1–128 characters,
`[A-Za-z0-9._-]+`, case-sensitive, immutable.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `id` | string | — | Namespace name, referenced by credentials, gateway keys, and usage records. |
| `default` | bool | `false` | The fallback namespace. **Exactly one** file namespace must set it; zero or two is rejected. |
| `allow_platform_fallback` | bool | `false` | When this namespace has no credential for a provider, may it use the `platform` namespace's pool? Off means BYOK means BYOK. |

Unknown namespace on an inference or management path: typed `404
unknown_namespace` (same body for never-existed and deleted).

## `[[provider]]`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `id` | string | — | Provider name, referenced by credentials and the `provider-id/` prefix of a request model. |
| `kind` | `openai` \| `anthropic` \| `openai-compatible` | — | Which wire the endpoint speaks. Decides which routes can serve it — see the [compatibility contract](./compatibility.md). |
| `base_url` | string | — | Endpoint root, e.g. `https://api.openai.com/v1`. **Path only**: the route's path is appended by string concatenation, so a query string or fragment here would swallow it. Never put a secret in it either — see the [security review](./security-review-2026-08-05.md#4-finding-transport-errors-echoed-the-upstream-url--fixed-here). |
| `unpriced_models` | `allow` \| `deny` | `deny` | What to do when no `[[price]]` rule matches. `deny` is `400 unpriced_model` before dispatch. `allow` dispatches and records `cost_microdollars` NULL. |

## `[blocklist]`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `models` | array of glob | `[]` | Deployment-wide denials (exact, `prefix*`, `*suffix`, or `*`). |

Effective denials are the union of this list and the namespace's optional
`blocklist`. A hit is `400 model_blocked` and is not sent upstream. Globs match
the full `provider-id/model-id` or the bare model id.

## `[[price]]` — deployment price-book

Not a routing table. First matching `(provider, model-id glob)` in file order
wins; put exact ids before globs. Callers send `provider-id/model-id`; Axond
forwards the bare id. A leftover `[[model]]` table is a boot error that names
the old alias and the prefix form.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `provider` | string | — | A defined `[[provider]] id`. |
| `model` | string | — | Glob against the bare upstream model id. |
| `input_microdollars_per_million` | integer | — | Prompt-token price. |
| `output_microdollars_per_million` | integer | — | Completion-token price. Unused by `/v1/embeddings`. |

Wire compatibility is the provider `kind`: `/v1/messages` to an `openai`
provider is `400 unsupported_wire`. There is no alias-level failover; credential
pool rotation inside one provider remains. `GET /ns/{ns}/v1/models` lists the
cached `provider-id/model-id` ids, minus the effective blocklist.

## `[discovery]` — cached upstream model listings

Background refresh of each `[[provider]]`'s OpenAI-compatible `GET /models`.
Not on the inference path. `GET /api/v1/providers/{id}/models` and
`GET /api/v1/providers/models` read the cache (`fetched_at`, `stale`).

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `refresh_interval_seconds` | integer | `300` | Seconds between refresh rounds. Read after each round, so a reload takes effect without a restart. The first round runs at boot; an empty provider set retries with short backoff instead of waiting the full interval. `0` is rejected. |

## `[[credential]]` — outbound provider keys

Explicit `(namespace, provider) → env var` bindings, never inferred from names.
Several entries for the same pair form that pair's **pool**
([ADR 0006](./adr/0006-credential-pools-per-namespace-provider.md)). Put the
key in a secret store and inject the named variable; do not inline it. On Azure
Container Apps that store is Key Vault — see
[the production guide](./deployment/azure-container-apps.md).

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `namespace` | string | — | A defined namespace. Undefined is rejected. |
| `provider` | string | — | A defined provider. Undefined is rejected. |
| `env` | string | — | *Name* of the environment variable holding the key. Empty is rejected; unset or empty **at boot** is a fatal error naming the variable. |
| `id` | string | the `env` name | Non-secret attribution label; lands on usage records as `credential_id`. Duplicates within one pool are rejected. |
| `weight` | integer | `1` | Share of pool traffic under the `weighted` strategy. `0` is rejected — remove the entry instead. |

The label is caller-visible in the owning namespace and in the operator's
`?namespaces=all` view (see `[[gateway_key]]` below for who reaches it).
A fallback tenant sees the platform credential and its
state, but the default `env`-derived label is omitted; an explicitly configured
`id` remains visible. This keeps operator environment naming private while
making deliberate credential labels actionable.

In the JSON response, `credential_id` is therefore optional: it is omitted for
fallback platform entries without an explicit `id`.

## `[credential_pool]` — Tier 0, in-memory per replica

Pool-wide policy for every `(namespace, provider)` pair that binds more than one
credential.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `strategy` | `round-robin` \| `weighted` | `round-robin` | How the first credential of a request is picked; the rest follow in rotation. |
| `failure_threshold` | integer | `2` | Consecutive credential-scoped failures (429 / quota) that park one credential. Must be ≥ 1. |
| `cooldown_seconds` | integer | `30` | How long a parked credential waits before a single half-open probe. Must be ≥ 1. |

`/v1/responses` is exempt: every Responses request uses the **first** configured
credential of the pool regardless of `strategy` or parked state, because a
response id is scoped to the key that created it (ADR 0023).

Parking is per credential, never per target: a rate-limited key is skipped while
the same target keeps serving every other key. This applies to streamed opens;
an OpenAI-normalized stream can rotate on an explicit rate-limit event before
content is emitted. Native streams do not rotate after relay bytes begin.

## `[failover]` — Tier 0, in-memory per replica

The outer loop around pool dispatch: an alias's targets, in order
([ADR 0008](./adr/0008-target-failover-and-circuit-scope.md)).

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `max_attempts` | integer | `3` | Upper bound on target attempts for one request; the retry count is one less. Must be ≥ 1. |
| `overall_timeout_ms` | integer | `30000` | Wall-clock budget for the whole walk. No further target is attempted once spent. Must be ≥ 1. |
| `failure_threshold` | integer | `3` | Consecutive target-scoped failures that trip a target's circuit. Distinct from the credential threshold. Must be ≥ 1. |
| `cooldown_seconds` | integer | `30` | How long a tripped target is skipped before a half-open probe. Must be ≥ 1. |

`max_attempts` does not apply to `/v1/responses`, which always attempts exactly
one target. Its circuit is still recorded and observed: a tripped first target
makes Responses requests fail rather than move on.

Circuits are in-memory and per replica
([ADR 0008](./adr/0008-target-failover-and-circuit-scope.md)). Alias-level
failover is gone ([ADR 0063](./adr/0063-stateful-only-namespaced-gateway.md));
this section still bounds credential-pool circuits inside one provider.

`overall_timeout_ms` is authoritative for everything that happens *before* a
response is being served: connecting, waiting for response headers, reading a
buffered body, and opening a stream — including a credential rotation's open. An
in-flight attempt is cancelled when it is spent, so a request cannot outlive the
budget by having started just inside it. Once a stream is open the budget stops
applying, because a long answer is not a stalled one: from there
`transport.stream_idle_timeout_ms` governs each wait for the next chunk. Once a
byte-faithful route observes its semantic terminal event,
`transport.stream_terminal_grace_ms` is the tighter fixed close bound.

## `[transport]` — per-phase upstream bounds, Tier 0

Bounds on one upstream call. Each phase is separate because they fail for
different reasons: connecting is egress or DNS, no headers is an overloaded
provider, a silent open socket is a half-dead connection. Every bound must be
≥ 1 — zero is not "unbounded", it is a gateway that cannot call anything
([ADR 0028](./adr/0028-transport-phase-bounds.md)).

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `connect_timeout_ms` | integer | `5000` | Bound on establishing the TCP + TLS connection to a provider. |
| `response_header_timeout_ms` | integer | `30000` | Bound on waiting for response headers (time to first byte) after dispatch. For a non-streamed call this covers the whole completion — see below. |
| `buffered_body_timeout_ms` | integer | `30000` | Bound on reading a whole buffered response body once headers arrived. |
| `stream_idle_timeout_ms` | integer | `120000` | Bound on waiting for the *next* chunk of an already-open stream. Not a stream lifetime: a productive stream may run for as long as it keeps producing. |
| `stream_terminal_grace_ms` | integer | `1000` | Fixed grace for trailing byte-faithful provider extension bytes after the semantic terminal event. It does not reset on chunks; an earlier silent-body idle timeout can still close the transport first. |
| `max_response_bytes` | integer | `33554432` | Largest buffered response body that will be read. A larger one is refused as `upstream_body_too_large` rather than buffered. |
| `max_error_bytes` | integer | `65536` | Largest provider *error* body read for diagnostics. A larger one is truncated, so the provider's own status still reaches the caller. Must not exceed `max_response_bytes`. |

The tighter of a phase bound and the remaining `failover.overall_timeout_ms`
governs each phase, and the caller-visible error names which one fired:
`upstream_timeout` (`504`) for a bound, `upstream_body_too_large` (`502`) for a
byte bound. Neither message names the provider endpoint.

The error names the phase that was waiting, and `axond.timeout.bound` records
which bound ended the wait (`phase` or `walk_budget`). A stalled phase is the
target's own failure and counts towards its circuit either way: a target that
accepted a request and produced nothing in the time it was given is evidence
about the target, not about the gateway's budget. Only `overall` — the walk's
budget already spent before the attempt was dispatched, so no target was called —
is excluded from target health.

The header and buffered-body defaults are therefore *not* tighter than the
default walk budget. A non-streamed provider call sends no headers until the
completion exists, so those two bounds are the model's thinking time rather than
a liveness signal, and a tighter default would refuse answers the walk still had
time for; `failover.overall_timeout_ms` is what keeps them finite in practice.
Tighten them below the walk budget only when you want to cap a single attempt so
later targets get a turn.

`connect_timeout_ms` configures the shared pooled HTTP client, so the whole
section is read at boot: a reload validates a changed `[transport]` and warns
that a restart is needed to apply it, exactly as `[server] bind` behaves.

## `[admission]` — request bounds and load shedding, Tier 0

`[transport]` bounds what a provider may do to the gateway; this section bounds
what callers may do to it. Two questions: how large one request may be, and how
many may be in flight at once.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `max_request_bytes` | integer | `2097152` | Largest request body accepted. The router refuses an oversized inbound body before buffering; request middleware output is measured again and refused before provider dispatch if expansion crosses the same ceiling. Must be ≥ 1. |
| `max_prompt_tokens` | integer | `1000000` | Largest estimated input size a request may carry. `0` disables. Only binds below `max_request_bytes` / 4 — see below. |
| `max_output_tokens` | integer | `200000` | Largest output allowance a request may ask for (`max_tokens`, `max_completion_tokens`, or `max_output_tokens`). Refused, not clamped, so the caller is never silently given a different request than it sent. `0` disables. |
| `max_in_flight` | integer | `1024` | Concurrent requests this replica admits. `0` disables. |
| `max_in_flight_streams` | integer | `512` | Of those, how many may be streams. A stream holds a socket and a relay task for as long as the answer lasts, so it is the scarcer resource. Must not exceed `max_in_flight` when you write both — see below. `0` disables. |
| `max_in_flight_per_tenant` | integer | `256` | Concurrent requests one namespace may hold, so one tenant cannot take the whole replica. Must not exceed `max_in_flight` when you write both — see below. `0` disables. In a single-namespace deployment this, not `max_in_flight`, is the effective ceiling — see below. |
| `max_tenants` | integer | `1024` | Distinct namespaces tracked concurrently. Bounds the admission table itself. |
| `queue_capacity` | integer | `0` | Requests that may wait for capacity instead of being refused. `0` refuses immediately. |
| `queue_wait_ms` | integer | `0` | How long a queued request waits before it is shed. Must be set together with `queue_capacity`, and queueing requires a finite `max_in_flight`. |
| `max_stream_duration_ms` | integer | `3600000` | Total lifetime of one stream, however productive. Distinct from `transport.stream_idle_timeout_ms`, which bounds silence: this is the bound on a stream that never stops talking. Applies to a stream the caller is draining — see below. `0` disables. |
| `max_stream_bytes` | integer | `67108864` | Raw upstream bytes one stream may relay before it is ended. `0` disables this configured ceiling. Reconstructed output from response-mutating middleware, and streams held for policy validation, still have a 64 MiB rendered-output safety ceiling. Ordinary and block-only OpenAI re-emission remains unlimited when this is `0`. |

Except for `max_request_bytes`, `0` means "this ceiling is off".

Lowering `max_in_flight` alone is enough. The two sub-ceilings default below the
shipped global one, so a config that only writes `max_in_flight = 16` would
otherwise leave a 256-request tenant ceiling above a 16-request process. An unset
sub-ceiling therefore follows a lowered `max_in_flight`:

- `max_in_flight_streams` is clamped down to it;
- `max_in_flight_per_tenant` is turned **off**, because a tenant ceiling equal to
  the global one isolates nothing and would shed at the same point with the wrong
  verdict — the tenant gate never queues and answers `429`. Shedding then happens
  at the global gate, which honors `queue_capacity` and answers `503`.

Write the numbers yourself and they are taken literally, including a
`max_in_flight_per_tenant` below a lowered `max_in_flight` (isolation you asked
for) and a `0` (the ceiling off). A written sub-ceiling *above* a written
`max_in_flight` is a boot error, because there is no obvious way to reconcile two
numbers an operator chose.

The two input bounds are related, and the body bound wins ties. The estimate
`max_prompt_tokens` is compared against is the serialized request divided by four
bytes per token, and the router has already refused anything over
`max_request_bytes`, so a prompt ceiling above `max_request_bytes` / 4 can never
be reached: at the shipped defaults `max_request_bytes` alone refuses at roughly
525 000 estimated tokens, well under the `1000000` ceiling. That pairing is
deliberate — the shipped prompt ceiling refuses what no provider would serve,
and the body ceiling is the operative input bound. Lower `max_prompt_tokens`
below `max_request_bytes` / 4 if you want a prompt-shaped refusal
(`413 prompt_too_large`) rather than a size-shaped one (`413 request_too_large`),
or raise `max_request_bytes` to make the token ceiling the binding one.

Three limits of these bounds are worth knowing before you tune them:

- **A single-namespace deployment sheds at `max_in_flight_per_tenant`, not at
  `max_in_flight`.** With one namespace serving all traffic the per-tenant
  ceiling is the operative one, and it answers `429 tenant_concurrency_exceeded`
  — which reads as a caller problem. Raise it to `max_in_flight`, or set it to
  `0`, when one namespace is the whole deployment.
- **`max_stream_duration_ms` and `max_stream_bytes` bound a stream the caller is
  draining.** They are evaluated as the relay is polled, and a caller that stops
  reading stops the polling, so a deliberately non-reading client can hold a
  stream slot past its lifetime. Front axond with a proxy that enforces a
  write/response timeout if untrusted clients can do that.
- **`max_in_flight` bounds requests reaching a provider, not bodies in memory.**
  Shedding happens after the body has been read and parsed, so
  `max_request_bytes` is what bounds the pre-admission phase. Size the process's
  memory against `max_request_bytes` times the connection concurrency your
  ingress allows, not against `max_in_flight` alone.

### Where shedding happens

Admission is taken *after* authentication and *before* the rate-limit store, the
budget reservation, and the provider call. Unauthenticated traffic therefore
cannot consume capacity (authentication stays fail-closed), and an overloaded
replica spends no round trips to say no. Each answer is typed and stable:

| Status | `error.type` | Cause |
| --- | --- | --- |
| `429` | `tenant_concurrency_exceeded` | The caller's namespace is at `max_in_flight_per_tenant`. The caller's own traffic is the cause, so it is a `429`. |
| `503` | `gateway_overloaded` | The replica is at `max_in_flight`. |
| `503` | `stream_capacity_exhausted` | No stream slot free. |
| `503` | `admission_queue_full` | The queue is at `queue_capacity`. |
| `503` | `admission_queue_timeout` | Queued, then `queue_wait_ms` elapsed. |
| `503` | `admission_tenant_capacity_exhausted` | More distinct namespaces in flight than `max_tenants`. |
| `413` | `request_too_large` / `prompt_too_large` | A per-request size bound. |
| `400` | `output_limit_exceeded` | A requested output allowance above the ceiling. |

Shed classes that a retry can plausibly clear carry `Retry-After: 1`, an honest
lower bound rather than a guess at when capacity returns. Tenant-table
exhaustion does not: nothing the caller can wait for will change it.

A tenant refused at its own ceiling never takes a global slot or a queue slot, so
a saturated tenant cannot crowd out a quiet one. Permits are released
synchronously on drop, which covers a returned handler, a provider failure, a
cancelled request, an abandoned queue waiter, and — because the permit is moved
into the relay's accounting — a stream that completes, is cancelled mid-answer,
or is cut off by its own duration or byte bound.

### Queueing or refusing

`queue_capacity = 0` is the default: a saturated replica answers immediately,
which is something a caller can act on. Queueing trades that for latency the
caller cannot see, and is worth it only for short bursts where a brief wait beats
a retry. Set `queue_capacity` and `queue_wait_ms` together; a queue with no wait
bound is a queue that hides an outage.

### Sizing across replicas

Every ceiling here is per replica and in process memory. A fleet of *N*
replicas admits *N* × `max_in_flight` requests, and a tenant behind a
round-robin load balancer gets *N* × `max_in_flight_per_tenant`. Size these
from what one process can hold — sockets, relay tasks, buffered bodies — and
use `[rate_limit]` for a per-subject in-flight bound. Period spend caps live
on the Store, not in this section.

The ceilings own semaphores built at boot, so a reload validates a changed
`[admission]` and warns that a restart is needed to apply it, exactly as
`[transport]` behaves
([ADR 0030](./adr/0030-request-bounds-and-load-shedding.md)).

## `[[gateway_key]]` — inbound authentication (required)

Exactly one deployment-wide static key authenticates **both** `/api/v1` and
`/ns/...` inference ([ADR 0063](./adr/0063-stateful-only-namespaced-gateway.md)).
Zero entries or two-or-more entries are boot errors. A second key as a
per-namespace list is withdrawn.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `env` | string | — | *Name* of the environment variable holding the inbound token. Exactly one of `env` and `file` must be non-empty. |
| `file` | string | — | Path to a UTF-8 file holding the inbound token. Exactly one of `file` and `env` must be non-empty; the file is re-read on every reload. |
| `namespace` | string | — | Must name a defined `[[namespace]]`. The key itself is deployment-wide; this field is the seed namespace used for credential-status authority. |
| `can_mint` | boolean | `false` | Withdrawn with in-gateway minting. Leave `false`. |

Exactly one source (`env` or `file`) is permitted; both declared or
neither declared is a config error. File contents are read without trimming:
static gateway-key secrets are exact bytes, so do not leave a trailing newline
(`printf %s 'secret' > /run/secrets/axond-gateway-key`). On Unix, a
group/other-readable file produces a warning. A trailing newline makes a
file-backed static key unusable because HTTP headers cannot carry it. The resolved material is never
logged; usage subjects use the env name or file path. Switching an existing
key from `env` to `file` changes that subject, so in-flight or accumulated
budget ledgers keyed by the old subject do not carry over. An absolute secret
mount path is emitted as written and may therefore expose tenant names in
usage sinks.

Reload fingerprints are salted per process: they are comparable only within
one process lifetime and show that material changed at this reload; they are
not a stable identifier for a key.

Callers present the token as `Authorization: Bearer <token>` or
`x-api-key: <token>`. Minted `axt1.` tokens are `401` and are not issued.
The usage record's `subject` is the env var's *name*
([ADR 0013](./adr/0013-inbound-auth-fails-closed.md)).

A key whose `namespace` is the file default namespace may use
`GET /ns/{ns}/v1/credentials?namespaces=all` and see every namespace.

## `[gateway_minting]` — withdrawn (ADR 0063)

`POST /v1/tokens` is unmounted. Do not enable this section. Field names below
are the parser surface only.

This section is absent by default; its presence registers `POST /v1/tokens`.
It may be paired with a static `[[gateway_key]]` with `can_mint = true`; if no
such key is enabled, the route remains present but rejects every caller.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `kid` | string | — | Existing verifier key identifier used for the minted token. |
| `env` / `file` | string | — | Exactly one signing-material source; resolved at boot and reload. |
| `max_ttl` | duration | verifier `max_ttl` | Issuance ceiling, never above the matching verifier's ceiling and at most 24h. When omitted, it tracks the verifier's `max_ttl`; raising that verifier ceiling also raises the issuance ceiling. |
| `scope` | array of string | — | Optional capability ceiling. When configured, omitted requests inherit it; when absent, omitted scope uses the ordinary capability posture. The operator-only `credentials:all` is rejected here and is never issued by `POST /v1/tokens`. |
| `aliases` | array of string | — | Optional alias-pattern ceiling. When configured, omitted requests inherit it; when absent, `*` is permitted, but alias dispatch still narrows it to aliases the namespace can already reach. |
| `max_request_microdollars` | u64 | — | Optional per-request ceiling. Omitted requests inherit it. |

The route is registered only at boot. Enabling minting on reload is reported
but requires a restart; removing it takes effect immediately and returns a
typed 404. Key material and ceilings otherwise reload normally. Enabling this
feature means every replica with minting enabled holds signing material: a
compromised replica can forge tokens. EdDSA otherwise supports verification-only
replicas with only public key material; HS256 never had that property because
every verifier already holds the forging secret. Keep offline `axond mint` as
the default, prefer EdDSA when verification-only replicas matter, and consider
a separately deployed minting replica set with short `max_ttl`.

## `[gateway_token]` — withdrawn minted-token policy (ADR 0063)

Minted inbound identity is withdrawn. `axt1.` presentations are `401`. This
section is documented so an old file's keys are still named.

This section is optional when the gateway uses only static gateway keys. It is
required when any `[[gateway_verifier]]` is declared: the verifier needs one
deployment-wide audience to validate the `aud` claim.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `audience` | string | — | Audience accepted by every configured minted token verifier. It must be present and non-empty when verifiers are declared. |

The audience is config-owned and is applied to every verifier. A token with a
different audience is rejected. The value is not a secret and is written
directly in TOML.

## `[[gateway_verifier]]` — withdrawn minted-token verification (ADR 0063)

Do not configure verifiers. Production authentication does not verify `axt1.`
tokens. Historical field names:

Verifiers were additive to the required static gateway keys. They resolved
`axt1.` compact JWS credentials without a per-caller registry or a runtime
datastore. See the withdrawn [minted identity guide](./minted-token-guide.md).

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `kid` | string | — | JWS key identifier. Required, non-empty, and unique across verifier entries. |
| `alg` | `EdDSA` \| `HS256` | — | Signature algorithm. Required and authoritative for this verifier. |
| `env` | string | — | *Name* of the environment variable holding the public Ed25519 key or opaque HS256 secret. Exactly one of `env` and `file` must be non-empty; the referenced variable must be set and non-empty at boot and reload. |
| `file` | string | — | Path to a UTF-8 file holding the public Ed25519 key or opaque HS256 secret. Exactly one of `file` and `env` must be non-empty; the file is re-read at boot and reload. |
| `namespaces` | array of string | — | Namespaces this signer may place in the token's `ns` claim. Required and non-empty; every namespace must be declared by `[[namespace]]`. |
| `max_ttl` | duration | — | Maximum `exp - iat` lifetime accepted for this verifier. Required, at least `1s`, and no more than `24h`. |

The verifier's `kid` must be present in the JWS header and its `alg` must match
the configured algorithm. Ed25519 values from either source are standard-base64
raw 32-byte public keys; surrounding whitespace is trimmed, so a trailing
newline is accepted. HS256 values are opaque exact bytes, are not trimmed, and
must be at least 32 bytes: a trailing newline changes the secret and must not be
written accidentally (`printf %s 'secret' > /run/secrets/verifier`). Empty,
absent, unreadable, or non-UTF-8 files are rejected at boot or reload, leaving
the previous running snapshot in place on reload. File permissions are checked
on Unix and group/other-readable files produce a warning.

The gateway validates the verifier's namespace set, lifetime, audience, and
signature on every token. At least one static `[[gateway_key]]` remains
mandatory as breakglass access. See the [minted identity guide](./minted-token-guide.md)
for the new-`kid` rotation procedure and the Tier 0/Tier 1 revocation boundary.

An optional `aliases` claim narrows the aliases the token may use. It is an array
of strings, matched as a case-sensitive union: a string without `*` is an exact
alias; one `*` at the end is a prefix match (`foo*`); one at the beginning is a
suffix match (`*foo`); and bare `*` matches every alias. Empty strings, a `*`
in the middle, or more than one `*` are invalid and reject the request with
`403`. An empty array permits no aliases, and a claim that is present but not an
array of strings — including `null` — is invalid. The claim can only narrow the
namespace's existing authority: it never adds aliases the namespace cannot
already reach. Static gateway keys remain unrestricted.

The check runs before the alias is looked up, so a disallowed alias returns `403`
whether or not it is configured and regardless of whether the endpoint supports
the target's wire protocol.

## `[[gateway_token_epoch]]` — withdrawn minted-token epochs (ADR 0063)

An issuance epoch invalidated minted tokens whose `iat` is earlier than the
configured instant. Minted tokens are no longer a supported inbound identity.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `namespace` | string | — | Declared namespace whose minted tokens are affected. Required. |
| `subject` | string | omitted | Optional subject-specific override. If present, this entry is the only epoch used for that subject; otherwise the namespace-wide entry applies. |
| `min_iat` | integer or RFC 3339 UTC string | — | Earliest accepted token issuance time, as Unix seconds or a timestamp such as `2026-08-10T12:00:00Z`. Required. |

Entries must use declared namespaces and may not duplicate a
`(namespace, subject)` pair. A namespace-wide epoch cannot spare one subject;
use a per-subject entry with an earlier epoch when that exception is needed.
Epochs affect minted `axt1.` tokens only. Static `[[gateway_key]]` credentials
remain valid.

## `[reload]` — Tier 0

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `watch` | bool | `false` | Also reload when the config file's contents change. `SIGHUP` always reloads regardless. |
| `poll_interval_ms` | integer | `2000` | How often the watcher compares contents. Below `100` is rejected. |

A reload re-runs the full boot validation against the current file, current
process environment, and referenced key-material files; a bad candidate is
rejected and the running config keeps serving. Replacing file contents in place
or via an atomic rename is therefore reload-reachable without a process
restart. `[[namespace]]` changes are reloadable and appear in the reported
namespace delta, but the namespace count used for in-memory budget retention
floors is captured at boot and does not resize until restart. `[server] bind`,
`[transport]`, `[admission]`, `[[usage_sink]]`, `[usage_journal]`, `[budget]`,
`[rate_limit]`, `[revocation]`, and `[catalog]` changes warn and are ignored
until restart;
this includes `limit_microdollars` ([ADR 0011](./adr/0011-config-hot-reload.md)).
The catalogue candidate is still fully validated, but the running snapshot
keeps the boot-time `[catalog]` settings so it cannot claim that a new source,
store, or refresh schedule is active when the importer task is still using the
old one. Restart after changing `[catalog]`; the applied reload log includes
`catalog_changed = true` and a restart warning.
The same log entry sets `restart_required = true` for catalogue and other
boot-owned changes; `changed` only reports live serving state applied by the
reload.

Stateful mode is the exception to this file-reload contract. Because a file
reload has no control-plane projection compiler, SIGHUP and file-watch reloads
are refused rather than replacing the active or pending revision with the
keyless bootstrap configuration. Restart for process-local bootstrap changes;
publish durable resource changes through `/admin/v1`.

## `[[usage_sink]]` — Tier 0 by default; Tier 2 for `postgres`

Omit the section entirely for the default: one JSON line per record on stdout,
no datastore ([ADR 0009](./adr/0009-durable-usage-sinks.md)). The row shape is a
versioned interface — see [`docs/usage-schema.md`](./usage-schema.md).

`kind = "stdout"` is Tier 0. `kind = "otlp"` is Tier 0 state (no datastore,
nothing to migrate), but not hermetic: it adds a collector dependency at boot
and is excluded from the hermetic Tier 0 CI lane. `kind = "postgres"` is Tier 2
and requires the Postgres role, `ops/postgres/usage_v2.sql`, ordered additive
migrations, and backup/restore ownership.

| Key | Type | Default | Applies to | Meaning |
| --- | --- | --- | --- | --- |
| `kind` | `stdout` \| `postgres` \| `otlp` | — | all | Destination. Declare several; each buffers independently. |
| `dsn_env` | string | — | `postgres` | *Name* of the env var holding the connection string. Required and non-empty for `postgres`. `sslmode=require` in the DSN turns on TLS (rustls + webpki roots). |
| `table` | string | `axond_usage` | `postgres` | Destination table; `schema.table` allowed. Validated as an identifier, so it cannot carry SQL. |
| `create_table` | bool | `false` | `postgres` | Apply the shipped DDL at boot. Off because most deployments give the gateway's role no DDL rights. |
| `buffer_capacity` | integer | `10000` | `postgres` | Records buffered before the fan-out drops. Must be ≥ 1. Not used when the journal is enabled. |
| `max_batch` | integer | `500` | `postgres` | Records accumulated before a flush. Must be ≥ 1 and no greater than `buffer_capacity`; the sink splits large batches across statements as needed. Not used when the journal is enabled. |
| `flush_interval_ms` | integer | `1000` | `postgres` | How long a partial batch waits. Must be ≥ 1. Not used when the journal is enabled. |

`max_batch` was previously capped by the INSERT parameter budget, which moved
whenever a column was added; it is now bounded by `buffer_capacity` instead, so
the cap no longer shifts under a schema change. A config setting `max_batch`
above its sink's `buffer_capacity` booted before this release and is now a boot
error: lower `max_batch`, or raise `buffer_capacity` to match.
When `max_batch` is omitted, the default of `500` is clamped to
`buffer_capacity` instead, so a smaller operator-configured buffer remains a
valid upgrade path and runs with the smaller effective batch size.

`kind = "otlp"` emits usage as OTel log records on the exporter telemetry
already installed, so it needs `OTEL_EXPORTER_OTLP_ENDPOINT`; the SDK's batch
processor does its buffering and the batching keys above do not apply.

A configured sink connects **at boot**, so a bad DSN refuses to start rather
than dropping records later. Afterwards the sink is off the request path and a
stalled destination drops with a count rather than delaying a request.

That last sentence is the telemetry-grade guarantee, and it is why a sink alone
cannot be a billing source: a record it accepted is not a record it wrote. See
[`[usage_journal]`](#usage_journal--billing-grade-usage-delivery-opt-in-tier-2)
for the mode that makes a record durable before the request is answered.

## `[usage_journal]` — billing-grade usage delivery (opt-in, Tier 2)

Omit the section for the default: telemetry-grade delivery, no outbox, no
datastore, byte-for-byte the behaviour that shipped before this section existed.

With `backend = "postgres"`, every settled usage event is appended to a durable
outbox **before the request is answered**, and a bounded delivery worker replays
it into the configured `[[usage_sink]]`s until they acknowledge it — at-least-once,
replayed after a restart, deduplicated by the consumer on `request_id`
([ADR 0049](./adr/0049-billing-grade-usage-outbox.md)). The operator guide is
[`docs/operations/usage-outbox.md`](./operations/usage-outbox.md); read it before
enabling this, because it adds a failure mode to the request path.

In billing-grade mode the sinks are written through synchronously by the worker
instead of buffering, since the worker acknowledges on their answer. At least one
sink is therefore required.

`kind = "otlp"` is not one the worker can acknowledge on — the OTel batch
processor confirms nothing, so an acknowledgement taken from it would forget an
event no collector ever received. It is not rejected either, because exporting
usage telemetry and storing it durably are things one deployment reasonably does
at once. An OTLP sink declared beside a storing one keeps exporting: the worker
writes it after a destination that *can* answer has accepted the events, once per
delivery pass, and a failed export is logged rather than retried, since the event
is already delivered. What is refused is a journal whose destinations are *all*
OTLP, because then an acknowledgement rests on nothing.

That moves buffering out of the sink and into the outbox, so a sink's
`buffer_capacity`, `max_batch`, and `flush_interval_ms` stop applying —
`claim_batch` and `poll_interval_ms` below replace them, and a replica that had
set them is told at boot which ones no longer do anything. `usage.records_written`
is then emitted by the delivery worker for records a destination accepted, and
`usage.records_dropped` no longer counts a failed write, because a failed write is
retried from the outbox rather than lost. The full contract is in [the operator
guide](./operations/usage-outbox.md#what-enabling-it-changes-about-your-sinks).

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `backend` | `none` \| `postgres` | `none` | `none` keeps telemetry-grade delivery. `postgres` is the durable outbox and requires `ops/postgres/usage_outbox_v1.sql`. |
| `dsn_env` | string | — | *Name* of the env var holding the outbox connection string. Required and non-empty for `postgres`. `sslmode=require` in the DSN turns on TLS. |
| `schema` | string | — | Schema the outbox tables live in. One unqualified identifier: it is interpolated into `SET search_path`, so it is validated and cannot carry SQL. |
| `create_schema` | bool | `false` | Apply the shipped outbox DDL at boot. Off, because most deployments give the gateway's role no DDL rights. |
| `consumer` | string | `billing` | Name the delivery state is kept under. Stable across restarts: renaming it replays everything still retained into the destinations as first deliveries, and leaves the old name registered — [delete the retired consumer row](./operations/usage-outbox.md#recovery), or retention stops pruning and the outbox fills. |
| `max_events` | integer | `1000000` | Events the outbox holds before `capacity_policy` applies. Must be ≥ 1. |
| `max_delivery_attempts` | integer | `8` | Attempts one event gets before it is quarantined as poison. Only a refusal the destination attributes to that event spends an attempt, so a destination-wide outage does not exhaust it ([usage outbox](./operations/usage-outbox.md#recovery)). Must be ≥ 1. |
| `retain_acknowledged_seconds` | integer | `86400` | How long an acknowledged event is kept, counted from when the request was observed rather than from its acknowledgement, because the window it has to cover is the caller's retry horizon and that starts at the request. Must exceed the longest retry horizon a caller has: pruning forgets the idempotency key, so a later retry of the same request would append a second copy. An event delivered long after a delivery outage is therefore prunable sooner than one delivered promptly. |
| `capacity_policy` | `refuse` \| `drop-oldest` | `refuse` | What a full outbox does. `refuse` is the only policy that keeps the billing-grade promise; `drop-oldest` discards the oldest undelivered event and counts it durably. |
| `on_undurable` | `refuse` \| `serve` | `refuse` | What a request does when its event could not be journaled. `refuse` answers `503 usage_not_durable`; `serve` answers anyway and counts the event as lost. |
| `operation_timeout_ms` | integer | `5000` | Bound on the append a request waits for, and on every other outbox operation. Must be ≥ 1. |
| `connect_timeout_ms` | integer | `5000` | Bound on connecting. Must be ≥ 1. |
| `connections` | integer | `8` | Connections held open. One is reserved for the delivery worker's claims, so the rest bound how many appends a replica can have in flight — a claim waiting on a slow destination cannot hold a connection a request needs. Must be ≥ 2, and no more than the share of Postgres `max_connections` this replica may hold. |
| `claim_batch` | integer | `256` | Events one claim takes. Must be ≥ 1. |
| `lease_seconds` | integer | `30` | How long a claimed batch stays invisible to other claimants. Must exceed the slowest write the destinations do, and it is how long recovery from a dead worker takes. |
| `poll_interval_ms` | integer | `250` | How long the worker waits after finding nothing to deliver. Must be ≥ 1. |

The outbox connects and checks its tables **at boot**, so a bad DSN, a missing
schema, or a role without the right grants refuses to start rather than failing
every request afterwards. A replica logs the mode it is running in:

```text
INFO usage delivery mode=billing_grade durable=true journal=postgres on_undurable=refuse
```

`capacity_policy = "drop-oldest"` and `on_undurable = "serve"` each trade
accounting for availability, and each is counted
(`axond.usage.journal.lost`). A configuration that sets either one logs a warning
at boot saying so.

## `[budget]` — hold TTL (ADR 0063)

Spend caps live on the Store:
`PUT /api/v1/namespaces/{ns}/budgets/{period}` sets `{limit_microdollars}` and
marks that period active for admission. The ledger is `(namespace, period)`
spent + reserved, not Redis, in-memory, or a `[budget] backend`.
`backend = "redis"`, `"postgres"`, and `"in-memory"` are boot errors.

Outage stance is `[storage].on_unavailable` (`deny` → `503 budget_unavailable`,
`allow` → serve without a hold). A namespace with no budget row is
`429 budget_exceeded`.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `reservation_ttl_seconds` | integer | `300` | How long a hold survives a replica that died mid-request. Should exceed the longest expected request. Zero is rejected. |

Enforcement holds a priced estimate before dispatch and settles it against
measured spend afterwards, so concurrent requests cannot collectively overshoot.
A cancelled or failed request is charged what it actually consumed — not the
estimate, and not always zero.

Settlement happens **exactly once** per admitted request — on completion,
upstream failure, client cancellation, or a dropped handler — and charges and
releases in one atomic operation, so a request can never be charged without its
hold being freed or vice versa. It is never retried: a settlement the store
rejects leaves the hold to lapse at `reservation_ttl_seconds`, which the next
reserve reclaims. That TTL is therefore the upper bound on how long a failed
settlement can hold budget out of circulation, which is why it should exceed the
longest expected request rather than be set generously.

A successful PUT marks that period as the namespace's active period. Inference
does not carry a period. Plan change is PUT the same period with a new limit;
a new billing period is PUT a new period key.

## `[rate_limit]` — opt-in inbound concurrency enforcement

Omit this section for the Tier 0 default: `NoLimit` has zero state and no
network dependency. The in-memory backend limits concurrent requests per
`(namespace, subject)` and is per-replica and approximate. `backend = "redis"`
is Tier 1 and enforces exact fleet-wide in-flight concurrency using expiring
leases; it is not an RPM/token-bucket limiter.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `backend` | `none` \| `in-memory` \| `redis` | `none` | Selects no-op, per-replica in-memory, or exact shared Redis leases. |
| `max_in_flight_per_subject` | integer | `16` | Maximum concurrent dispatches for one authenticated caller. Must be nonzero when enabled. |
| `max_subjects` | integer | `10000` | Maximum retained caller keys in the in-memory map. Must be nonzero when enabled. |
| `dsn_env` | string | — | Name of the env var holding the Redis URL. If omitted, a Redis budget's `dsn_env` is reused explicitly. |
| `key_prefix` | string | `axond:rate_limit` | Redis key namespace. |
| `on_unavailable` | `deny` \| `allow` | `deny` | Redis outage policy. `deny` fails closed with `503 rate_limit_unavailable`; `allow` admits unenforced and warns. |
| `lease_ttl_seconds` | integer | `300` | Redis lease lifetime and crash-safety backstop. Must be ≥ 1 for Redis. |
| `timeout_ms` | integer | `250` | Bounded Redis acquire/release operation timeout. Must be ≥ 1 for Redis. |
| `connect_timeout_ms` | integer | `5000` | Bounded Redis connection setup and boot-time `PING` timeout. Must be ≥ 1 for Redis. |

When `max_subjects` is reached, a new caller is refused rather than silently
admitted without a limit; zero-in-flight entries are evicted on permit drop, so
the map retains only active callers.

Redis connects and PINGs at boot. A Redis limiter's lease is released when its
permit drops; if the process or Redis is unavailable, the TTL reclaims it.

## `[core_middleware]` — accounting ownership migration gate

The fixed rate-limit and budget stages default to response-lifetime middleware
ownership. The backend, numeric limits, refusal envelope, charging policy, and
acquisition order do not change. Ownership lifetime does: the permit and
reservation are stored beside configurable middleware state and follow that
owner into a buffered response or streaming accounting. In `middleware` mode,
a buffered response keeps its rate-limit permit until the response body reaches
EOF or is dropped, rather than releasing it when the handler returns. Slow
response consumers can therefore occupy a subject's concurrency ceiling longer
and cause more `429 rate_limited` responses at the same configured limit.
`legacy` preserves the former buffered permit-release timing.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `accounting` | `middleware` \| `legacy` | `middleware` | `middleware` uses the ADR 0060 response-lifetime owner. `legacy` restores the previous straight-line permit and reservation guards as an operational rollback without changing the binary. A reload binds the selection to new requests through their captured snapshot; in-flight requests keep the mode they started with. |

The legacy mode is a migration escape hatch, not a different accounting
contract. Qualification runs both modes against the same refusal, settlement,
failure, and cancellation expectations. Operators should return to
`middleware` after diagnosing a rollback because subsequent fixed-core stages
build on that ownership model.

## `[revocation]` — withdrawn minted-token denylist (ADR 0063)

Minted tokens are not verified, so a revocation denylist is unused. Omit this
section. Historical keys:

| Key | Type | Default | Applies to | Meaning |
| --- | --- | --- | --- | --- |
| `backend` | `none` \| `redis` \| `postgres` | `none` | all | Selects the denylist backend. |
| `dsn_env` | string | — | `redis`, `postgres` | Env-var name for the connection string; omitted Redis references reuse a Redis budget's `dsn_env`. |
| `key_prefix` | string | `axond:revocation` | `redis` | Prefix for `<prefix>:{<jti>}` keys. |
| `table` | string | `axond_revocation` | `postgres` | Revocation table, validated as an identifier. |
| `create_table` | bool | `false` | `postgres` | Apply the shipped versioned DDL at boot. |
| `on_unavailable` | `deny` \| `allow` | `deny` | shared | For outages after boot, fail closed with `503 revocation_unavailable`, or explicitly admit and warn. `allow` is an explicit fail-open opt-in: during a store-wide recovery window or invoke-cap exhaustion, every revoked JTI is admitted, not just the triggering request. The backend connects and PINGs/`SELECT`s before the listener binds, so an unreachable store aborts startup for either value. |
| `timeout_ms` | integer | `250` | shared | Maximum time a request waits for the operation; the owned Redis operation may continue under a longer liveness budget, and must be nonzero. |
| `connect_timeout_ms` | integer | `5000` | shared | Bounded connection/PING timeout; must be nonzero. |

For Redis, a liveness-budget expiry retires the shared connection generation.
Until the replacement connection is published, all revocation checks use the
configured unavailable policy, so the default `deny` produces a `503` window
for all minted-token traffic rather than only failing the triggering operation.
With `allow`, that same window admits all revoked JTIs. Invoke-cap exhaustion
also applies the policy store-wide.
The separate request-wait and liveness budgets keep ordinary slowness from
triggering that generation-wide recovery path.

## `[catalog]` — imported model metadata (opt-in, Tier 2 when retained)

Omit this section for the default: nothing is imported, no HTTP client is built,
and no connection is opened. Enabled, a background task imports the
[models.dev](https://models.dev) catalogue, retains each accepted snapshot with
its provenance, and keeps a last-known-good active
([ADR 0043](./adr/0043-catalogue-source-imports.md),
[ADR 0051](./adr/0051-durable-catalogue-snapshots-and-refresh-orchestration.md)).

Imports are observational metadata only: they never activate a model, change a
tenant's enablements, or settle a price. Nothing on the inference path reads the
source or the store — a request cannot cause a fetch, and a fetch cannot delay a
request.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `source` | `none` \| `models-dev` \| `seed` | `none` | Selects no import, models.dev over HTTPS, or the bundled offline excerpt. |
| `source_url` | string | `https://models.dev/catalog.json` | The document a `models-dev` import fetches; must be a hosted `https://` URL without embedded credentials, is validated at boot against the adapter, and is rejected for other sources. |
| `store` | `in-memory` \| `postgres` | `in-memory` | Where accepted snapshots are retained. `in-memory` is a development store and is refused in stateful mode, which loses every snapshot and its provenance on restart. |
| `dsn_env` | string | — | Name of the env var holding the Postgres connection string. Inherits `[control_plane] dsn_env` when omitted. The value never appears in config or in a log line. |
| `schema` | string | — | Schema qualifying the catalogue tables, validated as an identifier. |
| `create_table` | bool | `true` | Apply the shipped versioned catalogue DDL at boot. |
| `refresh_interval_seconds` | integer | `21600` | Scheduled cadence. Must be nonzero. |
| `refresh_timeout_seconds` | integer | `60` | One bound covering the fetch and the retention of a single import. Must be nonzero. |
| `retry_initial_seconds` | integer | `60` | First delay after a refused import; doubles per consecutive refusal. Must be nonzero. |
| `retry_max_seconds` | integer | `3600` | Backoff ceiling. Must be nonzero and at least `retry_initial_seconds`. |
| `bootstrap` | `empty` \| `seed` | `empty` | What an empty store starts from. `seed` admits the bundled excerpt so an offline deployment has a catalogue before its first successful fetch. |
| `max_payload_bytes` | integer | `67108864` | Maximum upstream document accepted; a larger body is refused without being read whole. Must be nonzero. |
| `connect_timeout_ms` | integer | `10000` | Bounded Postgres connection setup. Must be nonzero. |
| `operation_timeout_ms` | integer | `30000` | Bounded Postgres statement timeout. Must be nonzero. |

An HTTPS mirror is a supported `source_url`; a plaintext, hostless, or credential-
carrying one is refused at boot. Hosts are deliberately not allowlisted: an
operator-controlled HTTPS mirror may be deployment-local or air-gapped. The URL
must still name the exact supported `/catalog.json` document.
Imported metadata is what an operator browses (`axond admin catalog browse`)
before `axond admin model apply`. An import still never enables, aliases, or
prices anything, so a document anyone on the path could substitute is not a
source this gateway will trust. Redirects are not followed either: the configured URL is the
provenance every snapshot records, so a `3xx` is a bounded refusal naming its
status rather than an import of whatever the answer pointed at.

Refreshes are conditional: the stored ETag and `Last-Modified` are sent back, and
a `304` confirms the active snapshot's freshness without producing new content.
Identical normalized content is idempotent — reformatted or reordered upstream
bytes admit as unchanged rather than as a new revision.

A malformed, oversized, schema-drifting, or unreachable import is *refused*: the
active snapshot keeps serving, the refusal is counted, and the next attempt is
delayed by the backoff. Freshness, the active content digest, and the bounded
refusal reason are readable by an operator on
[`/status`](./observability.md); the response carries no URL, payload, upstream
error text, or tenant-specific data, and a tenant-scoped caller sees no
catalogue at all.

## Telemetry

There is no telemetry section: telemetry is environment-only, exactly as in any
other OTel service. See the [deployment guide](./deployment.md#environment-variables)
for the variables and the [runbook](./observability.md) for what is emitted.

## Stability

The config surface follows the `0.x` compatibility policy in
[ADR 0015](./adr/0015-zero-dot-x-compatibility-policy.md) and the
[compatibility contract](./compatibility.md): additive keys with defaults in a
patch release; a rename or a changed default is a minor bump with the change
noted in [`CHANGELOG.md`](../CHANGELOG.md).
