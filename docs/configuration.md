# Configuration reference

Every section, key, and default the gateway reads today, cross-checked against
[`crates/gateway/src/config.rs`](../crates/gateway/src/config.rs).
[`axond.example.toml`](../axond.example.toml) is the same surface with prose;
this is the lookup table.

Two rules hold everywhere:

- **TOML owns structure, and secrets are referenced rather than inlined.** A
  key, DSN, or token is referenced by an environment-variable name or, for
  gateway key material, a file path. No config key takes material inline.
- **Fail at boot, not at request time.** The whole graph is validated before the
  socket is bound, and again on every reload. Anything listed as "rejected"
  below refuses to start (or, on reload, is rejected while the previous config
  keeps serving).

Loading order: the TOML file (`AXOND_CONFIG`, default `axond.toml`), then
`AXOND_`-prefixed environment variables layered on top with `__` as the section
separator (`AXOND_SERVER__BIND=0.0.0.0:9090`). The override layer is for scalars
in containerized deploys; structure belongs in the file.

## State tiers

State tiers describe the state backends axond itself depends on, not provider
egress: upstream provider calls still use the network at Tier 0.

| Config section | State tier |
| --- | --- |
| `[server]`, `[[namespace]]`, `[[provider]]`, `[[model]]`, `[[credential]]` | Tier 0: config-only. |
| `[credential_pool]`, `[failover]` | Tier 0: in-memory, per replica. |
| `[[gateway_key]]`, `[gateway_token]`, `[[gateway_verifier]]`, offline `keygen`/`mint` | Tier 0: config, referenced files, and environment only. |
| `[reload]` | Tier 0: reload reads the config file, referenced key-material files, and process environment. |
| `[[usage_sink]]` omitted or `kind = "stdout"` | Tier 0: one JSON line on stdout. |
| `[[usage_sink]] kind = "otlp"` | Tier 0 state, but not hermetic: a collector is a boot-time dependency, so this is outside the hermetic Tier 0 CI lane. |
| `[[usage_sink]] kind = "postgres"` | Tier 2: durable usage rows. |
| `[budget] backend = "none"` or `"in-memory"` | Tier 0; in-memory state is per replica and approximate. |
| `[budget] backend = "redis"` | Tier 1: the only shipped Tier 1 feature today. |
| `[budget] backend = "postgres"` | Tier 2: shared caps. |
| `/healthz`, `/readyz` | Tier 0. |

Namespaces, providers, aliases, prices, and provider credentials are
permanently config-owned and reload through ADR 0011. Only callers and keys may
ever become store-owned at Tier 2; nothing is defined in both. A database may
not override namespace provider access, an alias's target, a price, or the
credential pool. Even at Tier 2, a token verifier intersects token claims with
config-owned namespace authority (ADR 0016). See
[ADR 0017](./adr/0017-state-tiers-and-optional-backends.md) and the hermetic
[Tier 0 gate](./adr/0018-tier-0-hermetic-boot-gate.md).

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
| `name` | string | — | The name callers send (`gpt-4o`). Also what `/v1/models` lists, for callers whose namespace holds a credential for one of its targets. |
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

## `[credential_pool]` — Tier 0, in-memory per replica

Pool-wide policy for every `(namespace, provider)` pair that binds more than one
credential.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `strategy` | `round-robin` \| `weighted` | `round-robin` | How the first credential of a request is picked; the rest follow in rotation. |
| `failure_threshold` | integer | `2` | Consecutive credential-scoped failures (429 / quota) that park one credential. Must be ≥ 1. |
| `cooldown_seconds` | integer | `30` | How long a parked credential waits before a single half-open probe. Must be ≥ 1. |

Parking is per credential, never per target: a rate-limited key is skipped while
the same target keeps serving every other key.

## `[failover]` — Tier 0, in-memory per replica

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

## `[[gateway_key]]` — inbound authentication (required, Tier 0)

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `env` | string | — | *Name* of the environment variable holding the inbound token. Exactly one of `env` and `file` must be non-empty. |
| `file` | string | — | Path to a UTF-8 file holding the inbound token. Exactly one of `file` and `env` must be non-empty; the file is re-read on every reload. |
| `namespace` | string | — | Namespace the bearer is served under. Undefined is rejected. |

Exactly one source (`env` or `file`) is permitted per entry; both declared or
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

At least one entry is required: a config with none describes a gateway nobody
could call, so it refuses to boot. Two keys may not resolve to the **same**
secret — the caller's namespace would be ambiguous. Callers present the token as
`Authorization: Bearer <token>` or `x-api-key: <token>`; both read the same
table. The usage record's `subject` is the env var's *name*
([ADR 0013](./adr/0013-inbound-auth-fails-closed.md)).

## `[gateway_token]` — minted-token deployment policy (Tier 0)

This section is optional when the gateway uses only static gateway keys. It is
required when any `[[gateway_verifier]]` is declared: the verifier needs one
deployment-wide audience to validate the `aud` claim.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `audience` | string | — | Audience accepted by every configured minted token verifier. It must be present and non-empty when verifiers are declared. |

The audience is config-owned and is applied to every verifier. A token with a
different audience is rejected. The value is not a secret and is written
directly in TOML.

## `[[gateway_verifier]]` — minted-token verification (optional, Tier 0)

Verifiers are additive to the required static gateway keys. They resolve
`axt1.` compact JWS credentials without a per-caller registry or a runtime
datastore. For the operator setup, rotation, claims, and revocation runbook,
see the [minted identity guide](./minted-token-guide.md).

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

## `[reload]` — Tier 0

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `watch` | bool | `false` | Also reload when the config file's contents change. `SIGHUP` always reloads regardless. |
| `poll_interval_ms` | integer | `2000` | How often the watcher compares contents. Below `100` is rejected. |

A reload re-runs the full boot validation against the current file, current
process environment, and referenced key-material files; a bad candidate is
rejected and the running config keeps serving. Replacing file contents in place
or via an atomic rename is therefore reload-reachable without a process
restart. `[server] bind`, `[[usage_sink]]`, and `[budget]`
changes warn and are ignored until restart; this includes
`limit_microdollars` ([ADR 0011](./adr/0011-config-hot-reload.md)).

## `[[usage_sink]]` — Tier 0 by default; Tier 2 for `postgres`

Omit the section entirely for the default: one JSON line per record on stdout,
no datastore ([ADR 0009](./adr/0009-durable-usage-sinks.md)). The row shape is a
versioned interface — see [`docs/usage-schema.md`](./usage-schema.md).

`kind = "stdout"` is Tier 0. `kind = "otlp"` is Tier 0 state (no datastore,
nothing to migrate), but not hermetic: it adds a collector dependency at boot
and is excluded from the hermetic Tier 0 CI lane. `kind = "postgres"` is Tier 2
and requires the Postgres role, `ops/postgres/usage_v1.sql`, ordered additive
migrations, and backup/restore ownership.

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

## `[budget]` — opt-in budget enforcement (Tier 0, 1, or 2 by backend)

Omit the section for the default: no cap, no datastore
([ADR 0010](./adr/0010-shared-budget-backends-and-charging-policy.md)). The cap
is per `(namespace, subject)` — that is, per gateway key — in micro-dollars.

`backend = "none"` and `"in-memory"` are Tier 0; in-memory enforcement is
per-replica and approximate. `backend = "redis"` is Tier 1 and is the only
shipped Tier 1 feature today; with the default `on_unavailable = "deny"`, an
unavailable Redis answers `503 budget_unavailable`. `backend = "postgres"` is
Tier 2 and shares the cap through Postgres.

| Key | Type | Default | Applies to | Meaning |
| --- | --- | --- | --- | --- |
| `backend` | `none` \| `in-memory` \| `redis` \| `postgres` | `none` | all | `in-memory` holds state per replica, so a fleet of N enforces N caps; `redis` and `postgres` share one cap atomically. |
| `limit_microdollars` | integer | `0` | every backend but `none` | The cap. `10_000_000` µ$ = $10. Zero would deny everything, so it is rejected. |
| `on_unavailable` | `deny` \| `allow` | `deny` | `in-memory`, `redis`, `postgres` | What to do when the budget cannot enforce the cap. `deny` answers `503 budget_unavailable`; `allow` serves unenforced and warns. |
| `dsn_env` | string | — | `redis`, `postgres` | *Name* of the env var holding the connection string (`redis://`/`rediss://`, or a libpq DSN). Required and non-empty. |
| `table` | string | `axond_budget` | `postgres` | Base table; reservations live in `<table>_reservation`. Validated as an identifier. |
| `create_table` | bool | `false` | `postgres` | Apply the shipped DDL at boot. |
| `key_prefix` | string | `axond:budget` | `redis` | Key namespace for budget state. |
| `reservation_ttl_seconds` | integer | `300` | every backend but `none` | How long a hold survives a replica that died mid-request. Should exceed the longest expected request. Zero is rejected. |
| `idle_ttl_seconds` | integer | `3600` | `in-memory` | Idle time before an unheld ledger may be pruned when `max_subjects` is reached. In-memory state is per-replica and approximate; zero is rejected. |
| `max_subjects` | integer | `10000` | `in-memory` | Maximum retained `(namespace, subject)` ledgers. When full, unheld idle ledgers are pruned lazily; zero is rejected. Use Redis for exact caps. |

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
