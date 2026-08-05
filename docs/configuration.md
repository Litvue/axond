# Configuration reference

Every section, key, and default the gateway reads today, cross-checked against
[`crates/gateway/src/config.rs`](../crates/gateway/src/config.rs).
[`axond.example.toml`](../axond.example.toml) is the same surface with prose;
this is the lookup table.

Two rules hold everywhere:

- **TOML owns structure, the environment owns secrets.** A key, DSN, or token is
  always referenced by the *name* of an environment variable (`env`, `dsn_env`)
  and read from the process environment. No config key takes a secret value.
- **Fail at boot, not at request time.** The whole graph is validated before the
  socket is bound, and again on every reload. Anything listed as "rejected"
  below refuses to start (or, on reload, is rejected while the previous config
  keeps serving).

Loading order: the TOML file (`AXOND_CONFIG`, default `axond.toml`), then
`AXOND_`-prefixed environment variables layered on top with `__` as the section
separator (`AXOND_SERVER__BIND=0.0.0.0:9090`). The override layer is for scalars
in containerized deploys; structure belongs in the file.

## `[server]`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `bind` | socket address | `0.0.0.0:8080` | Listening address. Changing it needs a restart; a reload warns and ignores it. |

## `[[namespace]]`

The tenancy boundary: which credential pool a caller's requests draw from.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `id` | string | — | Namespace name, referenced by credentials, gateway keys, and usage records. |
| `default` | bool | `false` | The fallback namespace. **Exactly one** namespace must set it; zero or two is rejected. |
| `allow_platform_fallback` | bool | `false` | When this namespace has no credential for a provider, may it use the `platform` namespace's pool? Off means BYOK means BYOK. |

## `[[provider]]`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `id` | string | — | Provider name, referenced by targets and credentials. |
| `kind` | `openai` \| `anthropic` \| `openai-compatible` | — | Which wire the endpoint speaks. Decides which routes can serve it — see the [compatibility contract](./compatibility.md). |
| `base_url` | string | — | Endpoint root, e.g. `https://api.openai.com/v1`. **Path only**: the route's path is appended by string concatenation, so a query string or fragment here would swallow it. Never put a secret in it either — see the [security review](./security-review-2026-08-05.md#4-finding-transport-errors-echoed-the-upstream-url--fixed-here). |

## `[[model]]` — aliases and pricing

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `name` | string | — | The name callers send (`gpt-4o`). Also what `/v1/models` lists. |
| `targets` | array of target | — | Concrete destinations, tried **in order** on a retryable failure. An empty list is rejected. |

Each target:

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `provider` | string | — | A defined `[[provider]] id`. An undefined reference is rejected. |
| `model` | string | — | The upstream model / deployment id sent to the provider. |
| `price.input_microdollars_per_million` | integer | — | Prompt-token price. Required. |
| `price.output_microdollars_per_million` | integer | — | Completion-token price. Required; unused by `/v1/embeddings`, which bills input only. |

Pricing is mandatory because budgets are denominated in currency: an unpriced
target could not be charged, so it fails to parse.

## `[[credential]]` — outbound provider keys

Explicit `(namespace, provider) → env var` bindings, never inferred from names.
Several entries for the same pair form that pair's **pool**
([ADR 0006](./adr/0006-credential-pools-per-namespace-provider.md)).

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `namespace` | string | — | A defined namespace. Undefined is rejected. |
| `provider` | string | — | A defined provider. Undefined is rejected. |
| `env` | string | — | *Name* of the environment variable holding the key. Empty is rejected; unset or empty **at boot** is a fatal error naming the variable. |
| `id` | string | the `env` name | Non-secret attribution label; lands on usage records as `credential_id`. Duplicates within one pool are rejected. |
| `weight` | integer | `1` | Share of pool traffic under the `weighted` strategy. `0` is rejected — remove the entry instead. |

## `[credential_pool]`

Pool-wide policy for every `(namespace, provider)` pair that binds more than one
credential.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `strategy` | `round-robin` \| `weighted` | `round-robin` | How the first credential of a request is picked; the rest follow in rotation. |
| `failure_threshold` | integer | `2` | Consecutive credential-scoped failures (429 / quota) that park one credential. Must be ≥ 1. |
| `cooldown_seconds` | integer | `30` | How long a parked credential waits before a single half-open probe. Must be ≥ 1. |

Parking is per credential, never per target: a rate-limited key is skipped while
the same target keeps serving every other key.

## `[failover]`

The outer loop around pool dispatch: an alias's targets, in order
([ADR 0008](./adr/0008-target-failover-and-circuit-scope.md)).

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `max_attempts` | integer | `3` | Upper bound on target attempts for one request; the retry count is one less. Must be ≥ 1. |
| `overall_timeout_ms` | integer | `30000` | Wall-clock budget for the whole walk. No further target is attempted once spent. Must be ≥ 1. |
| `failure_threshold` | integer | `3` | Consecutive target-scoped failures that trip a target's circuit. Distinct from the credential threshold. Must be ≥ 1. |
| `cooldown_seconds` | integer | `30` | How long a tripped target is skipped before a half-open probe. Must be ≥ 1. |

Circuits are in-memory and per replica, consistent with running stateless
([ADR 0002](./adr/0002-stateless-by-default-stateful-by-opt-in.md)).

## `[[gateway_key]]` — inbound authentication (required)

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `env` | string | — | *Name* of the environment variable holding the inbound token. Empty is rejected; unset or empty at boot is fatal. |
| `namespace` | string | — | Namespace the bearer is served under. Undefined is rejected. |

At least one entry is required: a config with none describes a gateway nobody
could call, so it refuses to boot. Two keys may not resolve to the **same**
secret — the caller's namespace would be ambiguous. Callers present the token as
`Authorization: Bearer <token>` or `x-api-key: <token>`; both read the same
table. The usage record's `subject` is the env var's *name*
([ADR 0013](./adr/0013-inbound-auth-fails-closed.md)).

## `[reload]`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `watch` | bool | `false` | Also reload when the config file's contents change. `SIGHUP` always reloads regardless. |
| `poll_interval_ms` | integer | `2000` | How often the watcher compares contents. Below `100` is rejected. |

A reload re-runs the full boot validation against the current file **and the
current process environment**; a bad candidate is rejected and the running
config keeps serving. `[server] bind` and `[[usage_sink]]` changes warn and are
ignored ([ADR 0011](./adr/0011-config-hot-reload.md)).

## `[[usage_sink]]` — opt-in, datastore for `postgres`

Omit the section entirely for the default: one JSON line per record on stdout,
no datastore ([ADR 0009](./adr/0009-durable-usage-sinks.md)). The row shape is a
versioned interface — see [`docs/usage-schema.md`](./usage-schema.md).

| Key | Type | Default | Applies to | Meaning |
| --- | --- | --- | --- | --- |
| `kind` | `stdout` \| `postgres` \| `otlp` | — | all | Destination. Declare several; each buffers independently. |
| `dsn_env` | string | — | `postgres` | *Name* of the env var holding the connection string. Required and non-empty for `postgres`. `sslmode=require` in the DSN turns on TLS (rustls + webpki roots). |
| `table` | string | `axond_usage` | `postgres` | Destination table; `schema.table` allowed. Validated as an identifier, so it cannot carry SQL. |
| `create_table` | bool | `false` | `postgres` | Apply the shipped DDL at boot. Off because most deployments give the gateway's role no DDL rights. |
| `buffer_capacity` | integer | `10000` | `postgres` | Records buffered before the fan-out drops. Must be ≥ 1. |
| `max_batch` | integer | `500` | `postgres` | Rows per statement. Must be between 1 and the statement parameter budget. |
| `flush_interval_ms` | integer | `1000` | `postgres` | How long a partial batch waits. Must be ≥ 1. |

`kind = "otlp"` emits usage as OTel log records on the exporter telemetry
already installed, so it needs `OTEL_EXPORTER_OTLP_ENDPOINT`; the SDK's batch
processor does its buffering and the batching keys above do not apply.

A configured sink connects **at boot**, so a bad DSN refuses to start rather
than dropping records later. Afterwards the sink is off the request path and a
stalled destination drops with a count rather than delaying a request.

## `[budget]` — opt-in, datastore for `redis` / `postgres`

Omit the section for the default: no cap, no datastore
([ADR 0010](./adr/0010-shared-budget-backends-and-charging-policy.md)). The cap
is per `(namespace, subject)` — that is, per gateway key — in micro-dollars.

| Key | Type | Default | Applies to | Meaning |
| --- | --- | --- | --- | --- |
| `backend` | `none` \| `in-memory` \| `redis` \| `postgres` | `none` | all | `in-memory` holds state per replica, so a fleet of N enforces N caps; `redis` and `postgres` share one cap atomically. |
| `limit_microdollars` | integer | `0` | every backend but `none` | The cap. `10_000_000` µ$ = $10. Zero would deny everything, so it is rejected. |
| `on_unavailable` | `deny` \| `allow` | `deny` | shared backends | What to do when the store cannot be reached. `deny` answers `503 budget_unavailable`; `allow` serves unenforced and warns. |
| `dsn_env` | string | — | `redis`, `postgres` | *Name* of the env var holding the connection string (`redis://`/`rediss://`, or a libpq DSN). Required and non-empty. |
| `table` | string | `axond_budget` | `postgres` | Base table; reservations live in `<table>_reservation`. Validated as an identifier. |
| `create_table` | bool | `false` | `postgres` | Apply the shipped DDL at boot. |
| `key_prefix` | string | `axond:budget` | `redis` | Key namespace for budget state. |
| `reservation_ttl_seconds` | integer | `300` | every backend but `none` | How long a hold survives a replica that died mid-request. Should exceed the longest expected request. Zero is rejected. |

Enforcement holds a priced estimate before dispatch and settles it against
measured spend afterwards, so concurrent requests cannot collectively overshoot.
A cancelled or failed request is charged what it actually consumed — not the
estimate, and not always zero.

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
