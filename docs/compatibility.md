# Supported providers and compatibility contract

What axond serves at beta, what it deliberately does not, and what you may
depend on not changing under you. The versioning rules behind this document are
[ADR 0015](./adr/0015-zero-dot-x-compatibility-policy.md).

Status: **beta (`0.x`)**. The interfaces below are stable in the sense that
breaking them is a deliberate, documented act — not that they are frozen.

## Routes

| Route | Status | Wire | Streaming |
| --- | --- | --- | --- |
| `POST /v1/chat/completions` | **supported** | OpenAI chat completions | yes (`stream: true`) |
| `POST /v1/messages` | **supported** | Anthropic Messages, native | yes |
| `POST /v1/embeddings` | **supported** | OpenAI embeddings | n/a |
| `GET /v1/models` | **supported** | the alias catalogue, gated + namespace-scoped | n/a |
| `GET /v1/credentials` | **supported** | replica-local credential labels and circuit state, scoped | n/a |
| `GET /healthz`, `GET /readyz` | **supported** | liveness / readiness text | n/a |
| `POST /v1/responses` | **supported** | OpenAI Responses, native passthrough | yes |

Scoped minted callers receive a typed `403 token_scope_insufficient` when their
scope does not include a route capability or the namespace cannot serve it.
Static gateway keys and scope-less tokens retain their existing route behavior,
including the own-namespace `/v1/credentials` view. The all-namespaces
credential view (`?namespaces=all`) follows direct operator authority: a
scope-less static `[[gateway_key]]` in the configured default namespace is
admitted, while a static key in a tenant namespace and every minted token —
including one carrying a `credentials:all` claim — receive
`403 token_scope_insufficient`. A scoped token also needs `credentials` for the
route. `credentials:all` remains unmintable through `POST /v1/tokens`.

Responses is forwarded natively with only `model` rewritten and streaming is
byte-faithful. **Every** `/v1/responses` request — initial calls as well as ones
carrying a `previous_response_id` — considers only its alias's first configured
target and first configured credential, and never fails over or rotates
credentials. That is what lets a continuation reach the provider that stored the
response without gateway state; the trade-off is that the Responses route has no
failover, so a first-target outage or an exhausted first key is returned to the
caller. Only a request with a non-empty `previous_response_id` reports
`continuation_affinity_unavailable`; a pinned initial request that cannot use
its target or key reports the ordinary routing, credential, or upstream error.
`/v1/chat/completions`, `/v1/messages`, and `/v1/embeddings` keep full failover
and credential rotation over the same aliases.

## Providers

`[[provider]] kind` decides which routes a target can serve. axond is
**passthrough-first**: the caller's body is forwarded with only the `model`
field rewritten, so the caller and the target must already speak the same wire.

| `kind` | Serves | Examples |
| --- | --- | --- |
| `openai` | `/v1/chat/completions`, `/v1/embeddings` | OpenAI |
| `openai-compatible` | `/v1/chat/completions`, `/v1/embeddings` | Azure OpenAI, vLLM, Together, any OpenAI-shaped endpoint |
| `anthropic` | `/v1/messages` | Anthropic |

**Cross-provider translation is explicitly deferred.** There is no path that
converts an OpenAI chat request into an Anthropic Messages request, and none is
planned for beta. An alias whose target — *including any failover target* —
cannot speak the route's wire is rejected up front with
`400 unsupported_wire`, before a budget hold or a dispatch
([ADR 0012](./adr/0012-native-provider-routes.md), and the same guard on
`/v1/chat/completions`).

What passthrough buys: Anthropic signed thinking blocks and tool-use blocks
survive intact — verbatim bytes on a stream, re-serialized values with the same
signatures when buffered — because nothing rewrites them.

## Clients

Point any OpenAI-compatible or Anthropic SDK at the gateway's base URL with a
gateway key as its API key. Both `Authorization: Bearer <token>` and
`x-api-key: <token>` are accepted, so an Anthropic SDK's default works unchanged.

Compatibility is enforced by CI, not asserted: a required lane drives a real
`axond` process with the vendors' own Python SDKs — `openai==2.50.0` and
`anthropic==0.120.0`, pinned exactly — against committed wire fixtures, with no
provider account and no network
([ADR 0014](./adr/0014-compatibility-and-soak-harness.md)).

## Stability promises

### The config surface

[`docs/configuration.md`](./configuration.md) is the reference; the file it
describes is a public interface. Within `0.x`:

- **A patch release will not** remove or rename a key or section, tighten
  validation on a config that used to boot, or change a documented default.
- **A patch release may** add a new key or section that has a default, add a new
  enum variant, or relax validation.
- **A minor release may** rename or remove a key, change a default, or make
  previously-tolerated config a boot error. Every such change is listed in
  [`CHANGELOG.md`](../CHANGELOG.md) with the migration.

Practically: the config that boots on `0.x.y` boots on `0.x.(y+1)`.

**Operating modes do not change that promise.**
[ADR 0027](./adr/0027-stateless-and-stateful-operating-modes.md) accepts an
opt-in stateful mode whose resources live in a Postgres control plane rather
than TOML. The mode key is optional and defaults to `stateless`, so it is an
additive change under the rules above: no existing key is renamed or removed, no
default changes, and stateless validation is not tightened. Stateful mode is a
deliberate operator choice with its own bootstrap surface, and configuration
valid in one mode is not expected to be valid in the other.

### The usage schema

`UsageRecord::SCHEMA_VERSION = 2`, with the row shape in
[`ops/postgres/usage_v2.sql`](../ops/postgres/usage_v2.sql) and the field-level
contract in [`docs/usage-schema.md`](./usage-schema.md). It lands in *your*
tables, so it is treated as an API and is versioned independently of the
gateway's own version:

- Adding a nullable column, populating a reserved one, or adding a `status`
  value is **not** a bump.
- Populating a reserved column while changing the meaning of an existing field
  is still a bump; version 2 separates cached prompt tokens from
  `input_tokens`.
- Removing or renaming a column, making one `NOT NULL`, changing a unit, or
  redefining an existing vocabulary value **is** a bump: a new
  `ops/postgres/usage_v<N>.sql` alongside the old one, and a bump of
  `SCHEMA_VERSION`. Shipped DDL is never edited in place.
- One table may hold rows from several gateway versions. Read `schema_version`;
  do not assume a deploy timeline.

The budget schema ([`ops/postgres/budget_v1.sql`](../ops/postgres/budget_v1.sql))
follows the same rule, but it is gateway-internal state rather than a reporting
interface — read it at your own risk.

### The HTTP surface

- Request and response bodies on the supported routes are the **provider's**,
  not ours. What changes there is what the provider changed.
- Gateway-originated errors are `{"error": {"type": …, "message": …}}`. The
  `type` vocabulary is stable within `0.x`: values may be added, and an existing
  one will not be redefined or given a different HTTP status without a minor
  bump. The `message` text is diagnostic and may change at any time — do not
  parse it.
- Status codes for the documented failure modes (see the
  [runbook](./observability.md#failure-modes)) are part of the contract:
  notably `429 budget_exceeded` (the tenant is over cap) versus
  `503 budget_unavailable` (the gateway's own dependency is down).

### Telemetry

Metric and span names, and the `axond.*` attribute keys, are stable within `0.x`
in the same additive sense. They are an operational interface — dashboards break
loudly — so a rename or meaning change is a minor bump and a changelog entry.
The `axond.tokens.input` metric now reports only the non-cached prompt
remainder. Cache-read and cache-write tokens are reported separately as
`axond.tokens.cache_read` and `axond.tokens.cache_write`; operators should
account for all three counters when comparing prompt volume across the schema
version 2 transition.

### What is explicitly *not* promised at beta

- **`1.0` compatibility.** `1.0` is reserved for a real API commitment. Nothing
  here promises a `0.x` → `1.0` migration will be free.
- **In-memory behaviour across replicas.** Circuit state, credential health, and
  `backend = "in-memory"` budgets are per replica by design. Their exact
  thresholds and recovery timing may be tuned in any release.
- **Pricing catalogue values.** Prices come from your config, not from us.
- **Deferred features arriving on a date.** Cross-provider translation and
  further usage sinks (Tinybird, ClickHouse) remain post-beta with no committed
  schedule.
- **The stateful control plane.** The design is accepted in
  [ADR 0027](./adr/0027-stateless-and-stateful-operating-modes.md), but no
  stateful mode, `/admin/v1` route, or durable schema ships yet. Nothing about
  its surface is under the `0.x` config or HTTP promise until it exists.
