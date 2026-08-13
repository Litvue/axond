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
| `[admission]` | Tier 0: in-memory per-replica request bounds and load shedding. |
| `[[gateway_key]]`, `[gateway_token]`, `[[gateway_verifier]]`, `[[gateway_token_epoch]]`, offline `keygen`/`mint` | Tier 0: config, referenced files, and environment only. |
| `[reload]` | Tier 0: reload reads the config file, referenced key-material files, and process environment. |
| `[[usage_sink]]` omitted or `kind = "stdout"` | Tier 0: one JSON line on stdout. |
| `[[usage_sink]] kind = "otlp"` | Tier 0 state, but not hermetic: a collector is a boot-time dependency, so this is outside the hermetic Tier 0 CI lane. |
| `[[usage_sink]] kind = "postgres"` | Tier 2: durable usage rows. |
| `[usage_journal]` omitted or `backend = "none"` | Tier 0: telemetry-grade usage delivery, exactly as before. |
| `[usage_journal] backend = "postgres"` | Tier 2: a durable usage outbox on the request path. |
| `[budget] backend = "none"` or `"in-memory"` | Tier 0; in-memory state is per replica and approximate. |
| `[budget] backend = "redis"`, `[rate_limit] backend = "redis"`, or `[revocation] backend = "redis"` | Tier 1: exact shared admission and precise token revocation through Redis. |
| `[rate_limit] backend = "none"` or `"in-memory"` | Tier 0; in-memory state is per replica and approximate. |
| `[budget] backend = "postgres"` | Tier 2: shared caps. |
| `[revocation] backend = "postgres"` | Tier 2: durable precise token revocation. |
| `[shutdown]` | Tier 0: process-level bounds on termination. |
| `/healthz`, `/readyz` | Tier 0. |

Namespaces, providers, aliases, prices, and provider credentials are
config-owned and reload through ADR 0011. Only callers and keys may ever become
store-owned at Tier 2; nothing is defined in both. A database may not override
namespace provider access, an alias's target, a price, or the credential pool.
Even at Tier 2, a token verifier intersects token claims with config-owned
namespace authority (ADR 0016). See
[ADR 0017](./adr/0017-state-tiers-and-optional-backends.md) and the hermetic
[Tier 0 gate](./adr/0018-tier-0-hermetic-boot-gate.md).

## Operating mode

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `mode` | `stateless` \| `stateful` | `stateless` | Which authority owns durable resources. Any other value is rejected. |

Everything else in this reference describes the **stateless** mode: TOML plus
the environment and files it references own every resource. It is the default,
so no existing configuration needs the key and none of its behaviour is
tightened by having one — omitting `mode` and writing `mode = "stateless"` are
the same configuration.

[ADR 0027](./adr/0027-stateless-and-stateful-operating-modes.md) adds an opt-in
`mode = "stateful"`, in which tenants, projects, identities, providers, provider
credentials, catalogues, prices, aliases, and policy are owned by a durable
Postgres control plane and administered through `/admin/v1`, while inference
still serves one immutable in-memory snapshot. The mode is process-wide and
exclusive: there is no per-resource migration state, and therefore no merge
policy between a file and a database. It is a bootstrap property, so a reload
cannot switch a serving process between modes — that needs a restart.

**A stateful replica administers today and serves inference later.** It boots,
opens the control plane, and serves `/admin/v1` — see
[administering a stateful deployment](./operations/admin-api.md) — but a
published revision cannot be compiled into a runtime snapshot yet, so `/readyz`
stays `503` and every `/v1` route answers `503 inference_unavailable` rather than
an empty configuration. Serve inference from `mode = "stateless"` until
[revision convergence](./operations/revision-convergence.md) ships, and read the
ADR's ownership and failure matrices before planning a deployment.

### Stateful bootstrap

The whole file a stateful replica reads is `mode`, `[server]`, `[transport]`,
`[admission]`, `[reload]`, telemetry (`[[usage_sink]]`, plus the environment-only
OTLP settings), the three sections below, and *backend selection with DSN references*
for the opt-in `[budget]`, `[rate_limit]`, and `[revocation]` backends.
[`axond.stateful.example.toml`](../axond.stateful.example.toml) is that file with
prose.

Two symmetrical rejections happen before the socket is bound, and again on every
reload:

- Any stateful-owned section in a stateful file — `[[namespace]]`,
  `[[provider]]`, `[[model]]`, `[[credential]]`, `[credential_pool]`,
  `[failover]`, `[[gateway_key]]`, `[[gateway_verifier]]`, `[gateway_minting]`,
  `[gateway_token]`, `[[gateway_token_epoch]]` — is rejected, and so is any
  *policy value* under `[budget]` or `[rate_limit]` (`limit_microdollars`,
  `namespace_limit_microdollars`, `max_in_flight_per_subject`, the TTLs, and
  `max_subjects`). Bootstrap owns connectivity to those backends; the control
  plane owns their limits. Every offending section is named in one error.
- Any stateful bootstrap section in a stateless file — `[control_plane]`,
  `[secret_store]`, `[[admin_breakglass]]` — is rejected, since stateless mode
  never reads it. That is almost always a missing `mode = "stateful"`.

Every value below is a *reference*: an environment-variable name or a file path.
Nothing here connects to Postgres, reads a key, or resolves a DSN, and
diagnostics name the reference rather than its value.

A referenced env var must also stay clear of the `AXOND_<section>` shape, because
`AXOND_`-prefixed variables are the override layer described at the top of this
reference: `AXOND_ADMIN_BREAKGLASS` would be merged as the `admin_breakglass`
*key* rather than resolved as a reference, so exporting it would fail config load
and put the credential in the error. Such a name is rejected at validation,
naming the variable and the key it collides with. The examples use the `GW_`
prefix for secret-bearing variables.

#### `[control_plane]`

Required in stateful mode. Initial cold boot needs the control plane: a replica
with no snapshot fails readiness rather than serving partial state.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `dsn_env` | string | — | Name of the env var holding the control-plane Postgres connection string. Required and non-empty. |
| `schema` | string | connection default | PostgreSQL schema the journal lives in. A single unqualified identifier: it becomes `SET search_path`, so a qualified name like `a.b` and anything that is not an identifier are rejected at load. Omit to use whatever the DSN's own `search_path` selects. |
| `migrate` | boolean | `false` | Whether a *booting replica* may apply pending migrations. Off by default: the safe order is one `axond migrate apply` before any replica starts, so a rollout cannot have one replica migrating a database the others are already reading. A replica checks the schema either way and refuses to serve one it does not recognise. |
| `connect_timeout_ms` | integer | `5000` | Bound on establishing a control-plane connection. `0` is rejected. |
| `operation_timeout_ms` | integer | `30000` | Bound on one control-plane operation, including a migration transaction. Higher than the connect bound because applying DDL to a large database is slower than opening a socket. `0` is rejected. |

`migrate` governs boot only. `axond migrate apply` is an operator asking for a
migration explicitly, so it applies pending migrations whatever this key says;
`axond check preflight` and `axond migrate status` never write regardless of it.
See [the control-plane journal](operations/control-plane-journal.md#operator-commands).

#### `[secret_store]`

Required in stateful mode. Tenant provider credentials are stored wrapped and
unwrapped only while a snapshot is compiled; a request never unwraps a secret.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `backend` | `postgres` | `postgres` | Which store holds wrapped material. Encrypted Postgres is the first implementation ADR 0027 approves. |
| `dsn_env` | string | `[control_plane] dsn_env` | Name of the env var holding the store's connection string. Omit to reuse the control plane's own reference, which is the common single-database deployment. |
| `kek_env` | string | — | Name of the env var holding the key-encryption key. |
| `kek_file` | string | — | Path to a file holding the key-encryption key. |
| `schema` | string | connection default | PostgreSQL schema `axond_secret` lives in. A single unqualified identifier, on the same rules as `[control_plane] schema`, because it becomes `SET search_path`. |
| `create_table` | boolean | `true` | Whether a booting replica may apply the shipped `secret_store_v1.sql`. On by default, unlike `[control_plane] migrate`: the DDL is one `CREATE TABLE IF NOT EXISTS`, not a migration ledger a rollout can race on. An operator who applies it out of band turns this off and gets a refusal at boot instead of a schema change. |

Exactly one of `kek_env` and `kek_file` must be non-empty; zero or both is
rejected.

The key is 32 bytes, base64-encoded (padded or not; surrounding whitespace and a
trailing newline are tolerated, so a file written by `openssl rand -base64 32 >
kek` works as-is). Anything else is refused at boot, naming the reference and the
reason and never the material:

```bash
openssl rand -base64 32 > /etc/axond/secret-store.kek   # chmod 0400, root-owned
```

Material is sealed under a fresh per-version data key, that key is sealed under
this KEK, and only the sealed bytes reach the database — so a dump, a backup, or
a stolen replica of the store discloses nothing without the KEK, which is not in
the database. Rotating the KEK is therefore not a config edit on its own: rows
sealed under the previous key stop unwrapping, and the material has to be
restaged under the new one. Timeouts are inherited from `[control_plane]`, since
encrypted Postgres is normally the same database and two independent sets of
bounds for one server is a knob with no decision behind it. See
[ADR 0039](./adr/0039-envelope-encrypted-secret-store-and-snapshot-time-resolution.md).

#### `[[admin_breakglass]]`

Exactly one is required in stateful mode. Human `/admin/v1` identity is OIDC;
this static credential is what remains when the identity provider is down or the
control plane rejected the last change. A second one would make an audited
operator action ambiguous, so two are rejected.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `env` | string | — | Name of the env var holding the credential. |
| `file` | string | — | Path to a file holding the credential. |
| `id` | string | the source reference | Non-secret attribution label for audit events. |

Exactly one of `env` and `file` must be non-empty; zero or both is rejected.

## `[server]`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `bind` | socket address | `0.0.0.0:8080` | Listening address. Changing it needs a restart; a reload warns and ignores it. |

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

`/v1/responses` does not use the target order at all: every Responses request,
initial or continuation, is served only by the **first** target so a
provider-stored response id stays reachable without gateway state. An alias
whose first target is low-availability is a poor Responses alias, and reordering
`targets` strands response ids created under the previous order
([ADR 0023](./adr/0023-openai-responses-passthrough.md)).

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
([ADR 0028](./adr/0028-transport-phase-bounds.md)).

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `connect_timeout_ms` | integer | `5000` | Bound on establishing the TCP + TLS connection to a provider. |
| `response_header_timeout_ms` | integer | `30000` | Bound on waiting for response headers (time to first byte) after dispatch. For a non-streamed call this covers the whole completion — see below. |
| `buffered_body_timeout_ms` | integer | `30000` | Bound on reading a whole buffered response body once headers arrived. |
| `stream_idle_timeout_ms` | integer | `120000` | Bound on waiting for the *next* chunk of an already-open stream. Not a stream lifetime: a productive stream may run for as long as it keeps producing. |
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
| `max_request_bytes` | integer | `2097152` | Largest inbound request body accepted. Enforced by the router before the body is buffered, so an oversized request is refused rather than read. Must be ≥ 1. |
| `max_prompt_tokens` | integer | `1000000` | Largest estimated input size a request may carry. `0` disables. Only binds below `max_request_bytes` / 4 — see below. |
| `max_output_tokens` | integer | `200000` | Largest output allowance a request may ask for (`max_tokens`, `max_completion_tokens`, or `max_output_tokens`). Refused, not clamped, so the caller is never silently given a different request than it sent. `0` disables. |
| `max_in_flight` | integer | `1024` | Concurrent requests this replica admits. `0` disables. |
| `max_in_flight_streams` | integer | `512` | Of those, how many may be streams. A stream holds a socket and a relay task for as long as the answer lasts, so it is the scarcer resource. Must not exceed `max_in_flight` when you write both — see below. `0` disables. |
| `max_in_flight_per_tenant` | integer | `256` | Concurrent requests one namespace may hold, so one tenant cannot take the whole replica. Must not exceed `max_in_flight` when you write both — see below. `0` disables. In a single-namespace deployment this, not `max_in_flight`, is the effective ceiling — see below. |
| `max_tenants` | integer | `1024` | Distinct namespaces tracked concurrently. Bounds the admission table itself. |
| `queue_capacity` | integer | `0` | Requests that may wait for capacity instead of being refused. `0` refuses immediately. |
| `queue_wait_ms` | integer | `0` | How long a queued request waits before it is shed. Must be set together with `queue_capacity`, and queueing requires a finite `max_in_flight`. |
| `max_stream_duration_ms` | integer | `3600000` | Total lifetime of one stream, however productive. Distinct from `transport.stream_idle_timeout_ms`, which bounds silence: this is the bound on a stream that never stops talking. Applies to a stream the caller is draining — see below. `0` disables. |
| `max_stream_bytes` | integer | `67108864` | Bytes one stream may relay before it is ended. `0` disables. |

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

Every ceiling here is per replica and in process memory. The gateway is stateless
(Tier 0), so a fleet of *N* replicas admits *N* × `max_in_flight` requests, and a
tenant behind a round-robin load balancer gets *N* × `max_in_flight_per_tenant`.
Size these from what one process can hold — sockets, relay tasks, buffered bodies
— and use the shared-store `[rate_limit]` for the fleet-wide bound on a subject's
request *rate*, which is a different question from concurrency. Fleet-wide
admission policy is left to the stateful-policy work in #150; this section is the
per-replica floor it will build on.

The ceilings own semaphores built at boot, so a reload validates a changed
`[admission]` and warns that a restart is needed to apply it, exactly as
`[transport]` behaves
([ADR 0030](./adr/0030-request-bounds-and-load-shedding.md)).

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
`[transport]`, `[admission]`, `[[usage_sink]]`, `[usage_journal]`, `[budget]`,
`[rate_limit]`, and
`[revocation]` changes warn and are ignored until restart; this includes
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
| `retain_acknowledged_seconds` | integer | `86400` | How long an acknowledged event is kept. Must exceed the longest retry horizon a caller has: pruning forgets the idempotency key, so a later retry of the same request would append a second copy. |
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
INFO usage delivery mode=billing-grade durable=true journal=postgres on_undurable=refuse
```

`capacity_policy = "drop-oldest"` and `on_undurable = "serve"` each trade
accounting for availability, and each is counted
(`axond.usage.journal.lost`). A configuration that sets either one logs a warning
at boot saying so.

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
