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
| `[transport]` | Tier 0: process-level bounds on provider egress. |
| `[[gateway_key]]`, `[gateway_token]`, `[[gateway_verifier]]`, `[[gateway_token_epoch]]`, offline `keygen`/`mint` | Tier 0: config, referenced files, and environment only. |
| `[reload]` | Tier 0: reload reads the config file, referenced key-material files, and process environment. |
| `[[usage_sink]]` omitted or `kind = "stdout"` | Tier 0: one JSON line on stdout. |
| `[[usage_sink]] kind = "otlp"` | Tier 0 state, but not hermetic: a collector is a boot-time dependency, so this is outside the hermetic Tier 0 CI lane. |
| `[[usage_sink]] kind = "postgres"` | Tier 2: durable usage rows. |
| `[budget] backend = "none"` or `"in-memory"` | Tier 0; in-memory state is per replica and approximate. |
| `[budget] backend = "redis"`, `[rate_limit] backend = "redis"`, or `[revocation] backend = "redis"` | Tier 1: exact shared admission and precise token revocation through Redis. |
| `[rate_limit] backend = "none"` or `"in-memory"` | Tier 0; in-memory state is per replica and approximate. |
| `[budget] backend = "postgres"` | Tier 2: shared caps. |
| `[revocation] backend = "postgres"` | Tier 2: durable precise token revocation. |
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
| `name` | string | — | The name callers send (`gpt-4o`). Also what `/v1/models` lists, for callers whose namespace holds a credential for one of its targets. Credential labels are separately exposed by the scoped, replica-local `/v1/credentials` status view. |
| `targets` | array of target | — | Concrete destinations, tried **in order** on a retryable failure. All targets must use one provider wire family: OpenAI (`openai` or `openai-compatible`) or Anthropic. An empty list is rejected. |

Each target:

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `provider` | string | — | A defined `[[provider]] id`. An undefined reference is rejected. |
| `model` | string | — | The upstream model / deployment id sent to the provider. |
| `price.input_microdollars_per_million` | integer | — | Prompt-token price. Required. |
| `price.output_microdollars_per_million` | integer | — | Completion-token price. Required; unused by `/v1/embeddings`, which bills input only. |

Pricing is mandatory because budgets are denominated in currency: an unpriced
target could not be charged, so it fails to parse.

An alias cannot fail over between OpenAI-shaped and Anthropic targets because no
single route can serve both wires. Such a cross-family alias is rejected at boot
and on reload; a request that uses an alias from one family on the wrong route
still receives the typed `400 unsupported_wire` error.

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

Circuits are in-memory and per replica, consistent with running stateless
([ADR 0002](./adr/0002-stateless-by-default-stateful-by-opt-in.md)).

`overall_timeout_ms` is authoritative for everything that happens *before* a
response is being served: connecting, waiting for response headers, reading a
buffered body, and opening a stream — including a credential rotation's open. An
in-flight attempt is cancelled when it is spent, so a request cannot outlive the
budget by having started just inside it. Once a stream is open the budget stops
applying, because a long answer is not a stalled one: from there
`transport.stream_idle_timeout_ms` governs each wait for the next chunk.

## `[transport]` — per-phase upstream bounds, Tier 0

Bounds on one upstream call. Each phase is separate because they fail for
different reasons: connecting is egress or DNS, no headers is an overloaded
provider, a silent open socket is a half-dead connection. Every bound must be
≥ 1 — zero is not "unbounded", it is a gateway that cannot call anything
([ADR 0027](./adr/0027-transport-phase-bounds.md)).

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `connect_timeout_ms` | integer | `5000` | Bound on establishing the TCP + TLS connection to a provider. |
| `response_header_timeout_ms` | integer | `30000` | Bound on waiting for response headers (time to first byte) after dispatch. |
| `buffered_body_timeout_ms` | integer | `30000` | Bound on reading a whole buffered response body once headers arrived. |
| `stream_idle_timeout_ms` | integer | `120000` | Bound on waiting for the *next* chunk of an already-open stream. Not a stream lifetime: a productive stream may run for as long as it keeps producing. |
| `max_response_bytes` | integer | `33554432` | Largest buffered response body that will be read. A larger one is refused as `upstream_body_too_large` rather than buffered. |
| `max_error_bytes` | integer | `65536` | Largest provider *error* body read for diagnostics. A larger one is truncated, so the provider's own status still reaches the caller. Must not exceed `max_response_bytes`. |

The tighter of a phase bound and the remaining `failover.overall_timeout_ms`
governs each phase, and the caller-visible error names which one fired:
`upstream_timeout` (`504`) for a bound, `upstream_body_too_large` (`502`) for a
byte bound. Neither message names the provider endpoint.

`connect_timeout_ms` configures the shared pooled HTTP client, so the whole
section is read at boot: a reload validates a changed `[transport]` and warns
that a restart is needed to apply it, exactly as `[server] bind` behaves.

## `[[gateway_key]]` — inbound authentication (required, Tier 0)

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `env` | string | — | *Name* of the environment variable holding the inbound token. Exactly one of `env` and `file` must be non-empty. |
| `file` | string | — | Path to a UTF-8 file holding the inbound token. Exactly one of `file` and `env` must be non-empty; the file is re-read on every reload. |
| `namespace` | string | — | Namespace the bearer is served under. Undefined is rejected. |
| `can_mint` | boolean | `false` | Authorizes this static key to use the opt-in in-gateway minting endpoint. |

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

A key's `namespace` also decides its authority over the operator credential
view: an entry in the default namespace may use
`GET /v1/credentials?namespaces=all` and see every namespace, because an
operator placed that secret in the config. An entry in a tenant namespace keeps
its own-namespace view only, and no minted token can reach the operator view
even with a `credentials:all` claim
([ADR 0021](./adr/0021-credential-status-endpoint.md)). Keeping one
default-namespace key is therefore what makes the fleet-wide credential view
reachable at all.

Treat the default namespace as the operator's own namespace and its keys as
breakglass: a static key placed there is an operator credential, so anyone
holding it can enumerate every namespace's credential labels and circuit state.
Serve applications from their own `[[namespace]]` with their own key, or from
minted tokens, rather than handing out the default-namespace key.

## `[gateway_minting]` — in-gateway token issuance (optional, Tier 0)

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

## `[[gateway_token_epoch]]` — minted-token issuance revocation (optional)

An issuance epoch invalidates minted tokens whose `iat` is earlier than the
configured instant. It is applied when the config is reloaded, so changing an
epoch and sending `SIGHUP` revokes matching tokens without a restart.

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
`[transport]`, `[[usage_sink]]`, `[budget]`, `[rate_limit]`, and `[revocation]`
changes warn and are ignored until restart; this includes
`limit_microdollars` ([ADR 0011](./adr/0011-config-hot-reload.md)).

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
| `buffer_capacity` | integer | `10000` | `postgres` | Records buffered before the fan-out drops. Must be ≥ 1. |
| `max_batch` | integer | `500` | `postgres` | Records accumulated before a flush. Must be ≥ 1 and no greater than `buffer_capacity`; the sink splits large batches across statements as needed. |
| `flush_interval_ms` | integer | `1000` | `postgres` | How long a partial batch waits. Must be ≥ 1. |

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

## `[budget]` — opt-in budget enforcement (Tier 0, 1, or 2 by backend)

Omit the section for the default: no cap, no datastore
([ADR 0010](./adr/0010-shared-budget-backends-and-charging-policy.md)). The cap
is per `(namespace, subject)` — that is, per gateway key — in micro-dollars.
`namespace_limit_microdollars` adds an optional second cap on everything a
*namespace* spends, which is what bounds a namespace whose holder can mint fresh
subjects; omit it and enforcement is per-subject only, exactly as before.

`backend = "none"` and `"in-memory"` are Tier 0; in-memory enforcement is
per-replica and approximate. `backend = "redis"` is Tier 1; with the default
`on_unavailable = "deny"`, an unavailable Redis answers `503 budget_unavailable`.
`backend = "postgres"` is
Tier 2 and shares the cap through Postgres.

| Key | Type | Default | Applies to | Meaning |
| --- | --- | --- | --- | --- |
| `backend` | `none` \| `in-memory` \| `redis` \| `postgres` | `none` | all | `in-memory` holds state per replica, so a fleet of N enforces N caps; `redis` and `postgres` share one cap atomically. |
| `limit_microdollars` | integer | `0` | every backend but `none` | The cap. `10_000_000` µ$ = $10. Zero would deny everything, so it is rejected. |
| `namespace_limit_microdollars` | integer | unset | `redis`, `postgres` | An additional cap on the whole namespace — every subject in it combined. Unset means per-subject-only enforcement. Only the shared backends can enforce it *exactly*, so `none` and `in-memory` reject it at boot; zero is rejected. Both backends need a one-time migration first (below). |
| `on_unavailable` | `deny` \| `allow` | `deny` | `in-memory`, `redis`, `postgres` | What to do when the budget cannot enforce the cap. `deny` answers `503 budget_unavailable`; `allow` serves unenforced and warns. |
| `dsn_env` | string | — | `redis`, `postgres` | *Name* of the env var holding the connection string (`redis://`/`rediss://`, or a libpq DSN). Required and non-empty. |
| `table` | string | `axond_budget` | `postgres` | Base table; reservations live in `<table>_reservation`. Validated as an identifier. |
| `create_table` | bool | `false` | `postgres` | Apply the shipped DDL at boot. |
| `key_prefix` | string | `axond:budget` | `redis` | Key namespace for budget state. |
| `reservation_ttl_seconds` | integer | `300` | every backend but `none` | How long a hold survives a replica that died mid-request. Should exceed the longest expected request. Zero is rejected. |
| `idle_ttl_seconds` | integer | `3600` | `in-memory` | Idle time before an unheld ledger may be pruned when `max_subjects` is reached. In-memory state is per-replica and approximate; zero is rejected. |
| `max_subjects` | integer | `10000` | `in-memory` | Maximum retained `(namespace, subject)` ledgers. Configured namespaces derive an equal guaranteed floor (`max_subjects / namespace_count`, minimum 1); a namespace may use headroom only when it is not reserved for another configured namespace's unmet floor. Eviction is same-namespace-only and lazy. If no namespaces are configured, or the ceiling is smaller than their count, the previous global behavior is retained. The namespace count is captured at boot, so this and `max_subjects` require a restart; exact cross-replica retention and enforcement still needs Redis. Zero is rejected. |

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

### The namespace cap

With `namespace_limit_microdollars` set, a request is admitted only if it fits
*both* caps, and the reservation is one logical hold recorded in both scopes
atomically — Redis in one Lua script, Postgres in one transaction. Settlement
charges both or neither. A denial by either cap is the same
`429 budget_exceeded` response the subject cap already returned; the
`axond.budget.namespace_denials` counter is what distinguishes them
([observability](./observability.md)). `on_unavailable` still applies to the
whole operation: `deny` answers `503 budget_unavailable` and `allow` serves the
request with nothing held in either scope, so one scope is never enforced while
the other is not.

"Exact" means settled spend plus live reservations, under the same estimate and
reservation-TTL semantics as the subject cap — not provider billing. A namespace
cap also concentrates load: every subject in a namespace contends on one spend
row (Postgres) or one counter and reservation hash (Redis), so a very large
namespace pays for that exactness in contention on its hottest key.

**Redis** needs a one-time migration, because the namespace cap uses a
cluster-safe key layout tagged by namespace rather than by
`(namespace, subject)`. Stop every replica, then:

```console
$ axond budget migrate-redis --config axond.toml
```

It carries accumulated spend forward — claimed out of each v1 counter and added
to the v2 ones at most once, so an interrupted run loses nothing, a re-run adds
nothing twice, and spend a stray v1 replica wrote *after* the first run is added
rather than dropped — sums namespace totals from it, and stamps a layout marker.
A legacy key whose `{namespace|subject}` tag matches no configured namespace, or
more than one, stops the migration before anything is moved, deleted, or stamped
rather than guessing where the namespace ends. A gateway with
the cap set refuses to boot until that marker exists, refuses to boot while any
v1 key remains — that is, while a version without namespace-cap support is still
writing — and, once migrated, refuses to boot *without* the cap, since the v1
keys no longer hold the spend. Do not run mixed binaries during the migration:
the two layouts would each enforce a share of the traffic. Reservation state is
not carried over, so migrate with traffic stopped.

**Postgres** needs `ops/postgres/budget_v2.sql`, which is additive on top of
`budget_v1.sql`: a namespace spend table, an index for namespace-wide
reservation cleanup, and a backfill that seeds each namespace's total from the
subject rows already there. Custom `table` names are substituted as usual
(`<table>_namespace`), and `create_table = true` applies both files. Apply it with
the fleet stopped and drained: the backfill's sum would otherwise miss a
settlement committing behind it, leaving a namespace total permanently short (the
file takes an `EXCLUSIVE` lock so such a settlement blocks rather than being
lost). It also installs a fence trigger, so a replica configured *without* the
namespace cap can neither boot against that database nor write to it — its spend
would never reach the namespace total. The gateway refuses to boot if the fence is
missing, or if a namespace with spend has no backfilled row, so the cap cannot
start from zero or be bypassed by an old configuration.

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

## `[revocation]` — opt-in precise minted-token revocation

Omit this section for Tier 0: no denylist is consulted. Redis is Tier 1 and
Postgres is the durable alternative. Expired rows and keys are harmless
leftovers and are not removed on the request path.

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
