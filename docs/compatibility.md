# Supported providers and compatibility contract

What axond serves at beta, what it deliberately does not, and what you may
depend on not changing under you. The versioning rules behind this document are
[ADR 0015](./adr/0015-zero-dot-x-compatibility-policy.md).

Status: **beta (`0.x`)**. The interfaces below are stable in the sense that
breaking them is a deliberate, documented act — not that they are frozen.

## Routes

Inference is namespace-prefixed. An SDK `baseURL` is `/ns/{ns}/v1` (OpenAI) or
`/ns/{ns}` (Anthropic, which appends `/v1/messages`). Unprefixed `/v1/...` is
not served. Management is `/api/v1`. `/admin/v1` is unmounted.

| Route | Status | Wire | Streaming |
| --- | --- | --- | --- |
| `POST /ns/{ns}/v1/chat/completions` | **supported** | OpenAI chat completions | yes (`stream: true`) |
| `POST /ns/{ns}/v1/messages` | **supported** | Anthropic Messages, native | yes |
| `POST /ns/{ns}/v1/embeddings` | **supported** | OpenAI embeddings | n/a |
| `GET /ns/{ns}/v1/models` | **supported** | cached `provider-id/model-id` listings, minus the namespace blocklist | n/a |
| `GET /ns/{ns}/v1/credentials` | **supported** | replica-local credential labels and circuit state, scoped | n/a |
| `POST /ns/{ns}/v1/responses` | **supported** | OpenAI Responses, native passthrough | yes |
| `GET /healthz`, `GET /readyz` | **supported** | liveness / readiness text | n/a |
| `GET /api/v1/openapi.json` | **supported** | OpenAPI 3.1 of the management API | n/a |
| `POST /api/v1/namespaces` | **supported** | create `{id, attrs, blocklist?}` | n/a |
| `GET /api/v1/namespaces` | **supported** | list, cursor-paginated | n/a |
| `GET`/`PUT`/`DELETE /api/v1/namespaces/{ns}` | **supported** | read / replace attrs / idempotent delete | n/a |
| `PUT`/`GET /api/v1/namespaces/{ns}/budgets/{period}` | **supported** | period cap; active period for admission (fixed cadence) | n/a |
| `PUT`/`GET /api/v1/namespaces/{ns}/budget` | **supported** | cadence budget: `monthly` derives `YYYY-MM` in the budget's timezone and wins over the active-period marker; `fixed` keeps the chosen-period rule ([ADR 0065](./adr/0065-cadence-budgets.md)) | n/a |
| `GET /api/v1/namespaces/{ns}/usage` | **supported** | summary by model and status (`period` required) | n/a |
| `GET /api/v1/providers/{id}/models` | **supported** | cached upstream discovery | n/a |
| `GET /api/v1/providers/models` | **supported** | fan-out of the same | n/a |
| `POST /v1/chat/completions` | unmounted | same wire as `/ns/{ns}/v1/chat/completions` | — |
| `POST /v1/messages` | unmounted | same wire as `/ns/{ns}/v1/messages` | — |
| `POST /v1/embeddings` | unmounted | same wire as `/ns/{ns}/v1/embeddings` | — |
| `GET /v1/models` | unmounted | same wire as `/ns/{ns}/v1/models` | — |
| `GET /v1/credentials` | unmounted | same wire as `/ns/{ns}/v1/credentials` | — |
| `POST /v1/responses` | unmounted | same wire as `/ns/{ns}/v1/responses` | — |
| `POST /v1/tokens` | unmounted | minted-token issuance withdrawn (ADR 0063) | — |
| `GET /admin/v1/status` | unmounted | replica diagnostic withdrawn from production `serve()` | — |
| `GET /admin/v1/catalogue` | unmounted | control-plane catalogue withdrawn | — |
| `POST /admin/v1/bindings` | unmounted | `axond admin model apply` withdrawn | — |

The one static `[[gateway_key]]` authenticates every `/api/v1` and `/ns/...`
route. Minted `axt1.` tokens are `401`. The all-namespaces credential view
(`?namespaces=all`) is admitted only for that key when its configured
`namespace` is the file default namespace.

`GET /admin/v1/status` is unmounted in production `serve()` ([ADR 0063](./adr/0063-stateful-only-namespaced-gateway.md),
[#438](https://github.com/Litvue/axond/pull/438)). A `diagnostic_router` helper
still exists for withdrawn-tree tests; it is not composed into the listening
app. Ask `/readyz` for load-balancer readiness and logs/metrics for Store
health.

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
`/ns/{ns}/v1/chat/completions`, `/ns/{ns}/v1/messages`, and
`/ns/{ns}/v1/embeddings` keep credential-pool rotation inside one provider.
Alias-level failover is gone ([ADR 0063](./adr/0063-stateful-only-namespaced-gateway.md)).

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
planned for beta. A `provider-id/model-id` whose provider `kind` cannot speak
the route's wire is rejected up front with `400 unsupported_wire`, before a
budget hold or a dispatch
([ADR 0012](./adr/0012-native-provider-routes.md)).

What passthrough buys: Anthropic signed thinking blocks and tool-use blocks
survive intact — verbatim bytes on a stream, re-serialized values with the same
signatures when buffered — because nothing rewrites them.

## Clients

Point any OpenAI-compatible SDK at `/ns/{namespace}/v1` and any Anthropic SDK
at `/ns/{namespace}` with the deployment gateway key as its API key. Both
`Authorization: Bearer <token>` and `x-api-key: <token>` are accepted, so an
Anthropic SDK's default works unchanged. The request `model` is
`provider-id/model-id`.

Compatibility is enforced by CI, not asserted: two required lanes drive a real
`axond` process with the vendors' own SDKs against committed wire fixtures, with
no provider account and no network
([ADR 0014](./adr/0014-compatibility-and-soak-harness.md)).

| Runtime | SDKs | Lane |
| --- | --- | --- |
| Python 3.12 | `openai==2.50.0`, `anthropic==0.120.0` | [`tests/compat`](../tests/compat) |
| Node 22 | `openai@7.4.0`, `@anthropic-ai/sdk@0.115.0` | [`tests/compat-ts`](../tests/compat-ts) |

Every version above is pinned exactly, including the Node runtime, so a failure
means a *release* changed the wire rather than that a range floated. The
TypeScript lane is compiled with `tsc --strict` before it runs, so it also holds
the gateway to what the SDKs' own type definitions describe — including those
definitions themselves, since `skipLibCheck` is deliberately off: a release whose
shipped `.d.ts` no longer type-checks is a compatibility fact about that release.
Those two failures are not the same claim, so the lane's build labels which one
broke — declarations, before any request was made, or a call the SDKs' types
reject — and [`tests/compat-ts/README.md`](../tests/compat-ts/README.md) says
what each calls for.

Both lanes cover buffered and streamed chat, Responses, embeddings, native
Anthropic Messages with thinking and tool-use blocks, the `/ns/{ns}/v1/models`
catalogue, rejection of an unknown gateway key, and — the property no unit test
can see — that the credential reaching the upstream is the *provider's*, never
the caller's gateway key.

**Go is deliberately not covered.** A third runtime would re-assert the same
wire; the case for one is a generated management client, which is not an Axond
crate.

## Supported platforms

Released artifacts cover these targets. Anything else builds from source; a
target is added by a release, never by an unpublished build.

| Platform | Rust target | Release archive | OCI platform |
| --- | --- | --- | --- |
| Linux x86-64, glibc | `x86_64-unknown-linux-gnu` | `.tar.gz` | — |
| Linux x86-64, static musl | `x86_64-unknown-linux-musl` | `.tar.gz` | `linux/amd64` |
| Linux ARM64, glibc | `aarch64-unknown-linux-gnu` | `.tar.gz` | — |
| Linux ARM64, static musl | `aarch64-unknown-linux-musl` | `.tar.gz` | `linux/arm64` |
| macOS Apple Silicon | `aarch64-apple-darwin` | `.tar.gz` | — |
| Windows x86-64 | `x86_64-pc-windows-msvc` | `.zip` | — |

Archives are named `axond-<version>-<target><extension>`. Every target in this
table has a `binary-smoke` lane on a runner of its own platform.

`ghcr.io/litvue/axond:<version>` and `:sha-<short>` resolve to a
multi-architecture index containing `linux/amd64` and `linux/arm64`, so one
pinned digest deploys on either architecture, resolving the native child rather
than emulating one ([architecture
selection](./deployment/docker-compose.md#architecture-selection)). Releases up to
and including 0.3.17 published a single `linux/amd64` image. Single-platform
references remain available as `:<version>-amd64` and `:<version>-arm64`. There is no `latest`
tag, and adding one is not planned.

Every archive carries a SHA-256 sidecar, an SPDX SBOM, and provenance/SBOM
attestations. Every published image manifest — each single-platform image and
the multi-architecture index — is smoke-tested on its own architecture before it
is signed, and carries a keyless signature and a provenance attestation. The
index reaches its `<version>` and `sha-<short>` tags only after being booted by
digest on both architectures.

SBOM attestations are per-architecture, on the single-platform children: an
index has no filesystem of its own, so its SBOM would only ever be one child's
relabelled. To audit what you run, resolve the child digest for your platform
from the index and verify that digest's SBOM attestation, or use the
per-architecture SPDX asset on the release.

Archive signing is deliberately not part of this: archives are verified through
their checksum and GitHub attestations, and keyless signatures are an image-only
mechanism here.

Dropping a supported platform is a minor-release, changelog-listed act under the
same rules as the config surface below.

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
[ADR 0031]: ./adr/0031-bounded-status-contract.md

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

### Desired-state schemas and approved price books

Stateful mode's desired state is versioned by schema identifier per body, not by
one revision format. A price book declares `axond.price-book.v2`
([ADR 0046](./adr/0046-approved-price-books.md)), and a replica reads only the
schemas its build knows:

`axond.price-book.v1` remains readable as a legacy shape but is no longer
written. It has no `catalog_version`, so a request charged from a retained v1
book reports the compatibility value `catalog_version = 0` until the book is
republished as v2.

- Adding an optional field to a body **is** a schema bump. Bodies are read
  strictly — an unknown schema, an unknown field, a missing field, a wrong type,
  a currency, unit, precedence, or approval state this build does not know, or a
  rate it cannot bill is **refused**, never partially applied.
- A refusal of that kind is reported as an **incompatibility**, not as
  corruption, and the replica keeps serving the revision it already converged
  onto — including the pricing that revision carried. So a rolling upgrade in
  either direction is safe: a replica running an older build refuses the newer
  build's revision under a named reason and continues at its previous prices,
  rather than serving a revision it half understands or falling back to no
  pricing.
- Rolling *back* is republication of a prior revision. Price books are immutable
  per version, so no rollback rewrites a historical rate, and the price book a
  request was billed against is identified in the snapshot that served it by
  reference, canonical checksum, catalogue content id, and effective interval.
- What is **not** promised: nothing forces a replica to be able to read a body a
  newer build wrote. Mixed-version fleets converge at the oldest build's schema
  support, and staleness is the visible signal.

A configuration reload cannot change any of that either: `axond.toml` describes
no price book, so a reload keeps the pricing the replica already serves on the
snapshot it publishes. Approved pricing changes when a revision says so.

Approved pricing is separate from imported catalogue metadata by construction:
observed models.dev rates are metadata and never activate a billed rate, and a
target no approved book names carries *no* price in the snapshot rather than a
zero one — an unpriced model is not a free one.

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
  notably `429 budget_exceeded` (`spent >= limit`, or no budget row; in-flight
  requests are not reserved) versus
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

crates.io publishes **`axond` only** — the gateway binary. Its compatibility
surface is the config, HTTP, and telemetry contracts above, not Rust items.
`gateway-core` and `gateway-transport` remain unpublished git workspace members
so the in-repo test graph can still `cargo test -p gateway-core`; they are not a
library product and must not be `cargo add`-ed. Versions already on the registry
are not yanked.

`axond` does carry a library target, but it is empty outside `--cfg fuzzing`: it
exists so the out-of-tree [fuzz project](./security/fuzzing.md) can link the
parsers under test, exports nothing to any other consumer, and is therefore
excluded from the compatibility gate rather than promised.

A required CI lane still runs [`ops/api-compat.py`](../ops/api-compat.py): an
empty published-library set is success. If a workspace member is later marked
publishable and exports a library, that lane compares it with
[`cargo-semver-checks`](https://github.com/obi1kenobi/cargo-semver-checks)
against the version already on crates.io. An intentional break of such an API is
a minor bump plus a reviewed entry in
[`ops/api-compat-overrides.toml`](../ops/api-compat-overrides.toml); the process
is in the [release runbook](./maintainers/releasing.md#public-api-compatibility).

### The Rust version floor (MSRV)

The minimum supported Rust version is **1.97**, declared once as
`rust-version` in `[workspace.package]` and inherited by every workspace crate,
so `cargo install axond` on 1.97.0 resolves and builds.

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
- **The withdrawn control plane.** `mode`, `[control_plane]`, `[secret_store]`,
  `[[admin_breakglass]]`, and `/admin/v1` are boot errors or unmounted
  ([ADR 0063](./adr/0063-stateful-only-namespaced-gateway.md)). They are not
  under the `0.x` HTTP promise. Historical pages remain under
  [operations](./operations/admin-api.md).

### Namespaced inference (ADR 0063)

Canonical inference is `/ns/{namespace}/v1/...`. [ADR 0062](./adr/0062-blob-backed-flat-namespace-control-plane.md)
proposed `/namespaces/{namespace}` and is superseded. That longer prefix is not
served. Historical spelling for the blob-control-plane draft:

```text
/namespaces/{namespace}/v1/chat/completions
/namespaces/{namespace}/v1/responses
/namespaces/{namespace}/v1/embeddings
/namespaces/{namespace}/v1/models
/namespaces/{namespace}/v1/messages
/namespaces/{namespace}/v1/credentials
/namespaces/{namespace}/v1/tokens
```

Live clients use `/ns/{namespace}/v1` (OpenAI) or `/ns/{namespace}` (Anthropic).
Redirects are not a migration mechanism for authenticated or streaming `POST`
requests. Unprefixed `/v1/*` and `/namespaces/{namespace}` are unmounted. One
static key authenticates every namespace path; minted `axt1` tokens are `401`.

## Supported releases and who owns each matrix

Four matrices decide what "supported" means, and each has one owner file so a
claim here cannot drift from what CI and the release actually do:

| Matrix | Owner (source of truth) | Exercised by |
| --- | --- | --- |
| Supported versions for fixes | [`SECURITY.md`](../SECURITY.md) — latest `0.x` release plus the immediately previous minor, security fixes only | the release/backport process |
| Release targets | the `binaries` matrix in [`release-please.yml`](../.github/workflows/release-please.yml), listed under [supported platforms](#supported-platforms) above: six archive targets plus the `linux/amd64` + `linux/arm64` image index | the release workflow; on every change the `binary-smoke` matrix boots each target on a runner of its own platform, the musl `static-binary` lane adds the Tier 0 network-denial gate, and `docker-smoke` covers the image |
| Provider-SDK compatibility | [`tests/compat/requirements.in`](../tests/compat/requirements.in) (exact pins, hash-locked in `requirements.txt`) and [`tests/compat-ts/package.json`](../tests/compat-ts/package.json) with [`.nvmrc`](../tests/compat-ts/.nvmrc) (exact pins, hash-locked in `package-lock.json`) | the required `sdk-compat` and `sdk-compat-ts` lanes against committed fixtures ([ADR 0014](./adr/0014-compatibility-and-soak-harness.md)) |
| Rust floor and published API | `rust-version` in [`Cargo.toml`](../Cargo.toml); [`ops/api-compat.py`](../ops/api-compat.py) (empty published-library set is success) | the required `msrv` and `api-compat` lanes |

Adding a target, an SDK, or a supported version means editing the owner file
above; this document describes the policy and restates a value only where a gate
holds the restatement to its owner. `ops/check-docs.py` is that gate:

- a released target with no `binary-smoke` lane fails, as does a smoked target
  that is never published, or either missing from the platform table above — so a
  new target arrives with its smoke coverage or not at all;
- every SDK version and runtime in the [client matrix](#clients) must be the one
  its owner file pins and the lane installs, so a pin bump that stops here is a
  failing gate rather than a compatibility claim about a release CI no longer
  exercises;
- the MSRV section must name the floor `Cargo.toml` declares and the toolchain
  pinned in `rust-toolchain.toml`.

Every released target is booted and served, not merely compiled. On each change
and again at the tag, for the exact binary that is archived,
[`ops/binary-smoke.py`](../ops/binary-smoke.py) asserts that `/healthz` and
`/readyz` answer unauthenticated, that `/ns/{ns}/v1/models` requires a gateway
key and lists cached `provider-id/model-id` ids, that an unprefixed model is
refused as `model_unprefixed`, and that one chat completion completes against a
local fixture upstream. Each
Linux archive runs it on a runner of its own architecture, so an ARM64 archive is
booted on ARM64 rather than emulated. Linux musl is held to more:
[`ops/tier0-gate.sh`](../ops/tier0-gate.sh) boots it inside a network namespace
that denies egress and DNS, which is why a datastore or outbound dependency added
to the default path fails there first. That gate is Linux-only by construction,
so macOS and Windows get the portable subset.

What is *not* covered: the hermetic Tier 0 gate applies to
`x86_64-unknown-linux-musl` alone on every change — every Linux archive passes
through it at release time, but there with the namespace treated as best-effort
([ADR 0018](./adr/0018-tier-0-hermetic-boot-gate.md)) — the smoke exercises one
buffered fixture request rather than streaming or a real provider, and the SDKs
exercised end to end are the Python and Node ones above — no Go SDK, and no other
runtime.
