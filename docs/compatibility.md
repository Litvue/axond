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

**Operating modes do not change that promise.** `mode` selects which authority
owns durable resources ([ADR 0027]): the default `stateless` is TOML as it is
documented today, and the opt-in `stateful` bootstrap points at a Postgres
control plane. Adding the key was additive under the rules above — nothing is
renamed or removed and no default changed, so omitting `mode` still means exactly
what it always meant.

With one deliberate exception, stated rather than glossed: a stateless file that
already contained `[control_plane]`, `[secret_store]`, or `[[admin_breakglass]]`
used to boot, because unknown sections are tolerated, and is now a boot error.
That is a tightening under the rules above. It is accepted because those names
had no meaning before this release, so such a file was either hand-written
against an unimplemented surface or a `mode = "stateful"` line short of what its
author intended — and silently ignoring a control-plane reference is exactly the
ambiguity the mode boundary exists to remove. The diagnostic names the section
and the missing `mode`. No key that ever had a meaning became stricter.

Stateful mode is a deliberate operator choice with its own bootstrap surface,
and configuration valid in one mode is *not* expected to be valid in the other:
each mode rejects the other's sections at boot rather than merging them. The
stateful bootstrap surface is not under the `0.x` promise until the control plane
it bootstraps exists; see [the reference](./configuration.md#operating-mode).

[ADR 0027]: ./adr/0027-stateless-and-stateful-operating-modes.md

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
  `503 budget_unavailable` (the gateway's own dependency is down),
  `429 tenant_concurrency_exceeded` (this tenant is at its own concurrency
  ceiling) versus `503 gateway_overloaded` (the replica itself is saturated), and
  `504 upstream_timeout` (a transport bound fired before anything could be
  served) versus `502 upstream_body_too_large` (a buffered provider body was
  refused rather than held in memory). Both are new `type` values under the
  additive rule above: a call that previously hung until the caller gave up now
  terminates at a bound, which is a behaviour change rather than a status
  change, since there was no earlier response to reclassify.

### The published Rust API

Three crates are published: `gateway-core` and `gateway-transport` are libraries
with a public Rust API, and `axond` is the binary — its compatibility surface is
the config, HTTP, and telemetry contracts above, not Rust items. `axond` does
carry a library target, but it is empty outside `--cfg fuzzing`: it exists so the
out-of-tree [fuzz project](./security/fuzzing.md) can link the parsers under
test, exports nothing to any other consumer, and is therefore excluded from the
compatibility gate rather than promised.

The library API follows Cargo's `0.x` rules, and mechanically: a required CI lane
runs [`cargo-semver-checks`](https://github.com/obi1kenobi/cargo-semver-checks)
against the versions already on crates.io, so removing or renaming a public item,
changing a signature, or adding a variant to an exhaustive public enum fails the
build rather than a downstream `cargo update`. Additive change — a new item, a new
method, a new `#[non_exhaustive]` variant — is a patch.

An intentional break is a minor bump plus a reviewed entry in
[`ops/api-compat-overrides.toml`](../ops/api-compat-overrides.toml) naming the
crate, the published baseline, and the review; the process is in the
[release runbook](./maintainers/releasing.md#public-api-compatibility). There is
no blanket allow list, and an override stops applying as soon as the next release
moves the baseline.

`gateway-core` and `gateway-transport` are published so the gateway can be
assembled from its parts, but they are *the gateway's* internals: the promise
above is about not breaking you silently, not a commitment to a stable embedding
API before `1.0`.

### The Rust version floor (MSRV)

The minimum supported Rust version is **1.97**, declared once as
`rust-version` in `[workspace.package]` and inherited by every published crate,
so `cargo add gateway-core` on 1.97.0 resolves and builds.

The floor and the toolchain this repository builds with are deliberately
different things:

| Declaration | Value | Why |
| --- | --- | --- |
| `rust-version` (`Cargo.toml`) | `1.97` | the floor consumers may rely on; enforced by Cargo for them |
| `rust-toolchain.toml` | `1.97.1` | one pinned patch for reproducible `rustfmt`/`clippy` results |
| `FROM rust:` (`Dockerfile`) | `1.97` | the release image builds on the floor's minor |
| CI lanes | `1.97.1`, plus one `1.97.0` MSRV lane | the stable lane keeps its pin; the floor is proved separately |

`ops/msrv-gate.sh` is the enforcement: it reads the floor from `Cargo.toml`,
refuses a pinned toolchain older than it, refuses a `Dockerfile` or a crate
manifest that drifts from it, and then builds the workspace — `--locked`, all
features, all targets — on the first patch of that minor. A dependency bump that
quietly raises its own MSRV therefore fails in CI rather than in a consumer's
build.

Raising the floor is a **minor** bump with a changelog entry, treated like any
other break in this document, and is done for a reason that is written down —
not merely because a newer compiler exists.

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
- **The stateful control plane.** `mode = "stateful"` bootstrap configuration
  parses and validates ([ADR 0027]), and a stateful process then refuses to
  start: no `/admin/v1` route, durable schema, or snapshot compiler ships yet.
  Nothing about that surface is under the `0.x` config or HTTP promise until it
  exists.

## Supported releases and who owns each matrix

Four matrices decide what "supported" means, and each has one owner file so a
claim here cannot drift from what CI and the release actually do:

| Matrix | Owner (source of truth) | Exercised by |
| --- | --- | --- |
| Supported versions for fixes | [`SECURITY.md`](../SECURITY.md) — latest `0.x` release plus the immediately previous minor, security fixes only | the release/backport process |
| Release targets | the `binaries` matrix in [`release-please.yml`](../.github/workflows/release-please.yml): `x86_64-unknown-linux-gnu`, `x86_64-unknown-linux-musl`, `aarch64-apple-darwin`, `x86_64-pc-windows-msvc`, plus the `linux/amd64` image | the release workflow; on every change the `binary-smoke` matrix boots each target on a runner of its own platform, the musl `static-binary` lane adds the Tier 0 network-denial gate, and `docker-smoke` covers the image |
| Provider-SDK compatibility | [`tests/compat/requirements.in`](../tests/compat/requirements.in) (exact pins, hash-locked in `requirements.txt`) | the required `sdk-compat` lane against committed fixtures ([ADR 0014](./adr/0014-compatibility-and-soak-harness.md)) |
| Rust floor and published API | `rust-version` in [`Cargo.toml`](../Cargo.toml); [`ops/api-compat-overrides.toml`](../ops/api-compat-overrides.toml) for accepted breaks | the required `msrv` and `api-compat` lanes |

Adding a target, an SDK, or a supported version means editing the owner file
above; this document describes the policy and does not restate the values it
cannot enforce. It does enforce that the three lists agree: `ops/check-docs.py`
fails if a released target has no `binary-smoke` lane, if a smoked target is not
published, or if either is missing from this document. So a new target arrives
with its smoke coverage or not at all.

Every released target is booted and served, not merely compiled. On each change
and again at the tag, for the exact binary that is archived,
[`ops/binary-smoke.py`](../ops/binary-smoke.py) asserts that `/healthz` and
`/readyz` answer unauthenticated, that `/v1/models` requires a gateway key and
lists the configured alias, that an unknown model is refused as `unknown_model`,
and that one chat completion completes against a local fixture upstream. Linux
musl is held to more: [`ops/tier0-gate.sh`](../ops/tier0-gate.sh) boots it inside
a network namespace that denies egress and DNS, which is why a datastore or
outbound dependency added to the default path fails there first. That gate is
Linux-only by construction, so macOS and Windows get the portable subset.

What is *not* covered: the hermetic Tier 0 gate applies to
`x86_64-unknown-linux-musl` alone, the smoke exercises one buffered
fixture request rather than streaming or a real provider, and only the Python
SDKs are exercised end to end.
