# 63. Stateful-only, namespace-scoped gateway

Date: 2026-09-02

## Status

Accepted

Budget reserve-before-dispatch is superseded by
[ADR 0064](./0064-charge-actuals-after-response.md): the Store ledger charges
actual spend after the response and does not hold an estimate on the inference
path.

Settled with the maintainer on 2026-09-02. The session brief named this
record `0061`; [ADR 0061](./0061-authentication-remains-an-outer-boundary.md)
and [ADR 0062](./0062-blob-backed-flat-namespace-control-plane.md) already
occupy those numbers, so this is 0063.

Supersedes:

- [ADR 0002](./0002-stateless-by-default-stateful-by-opt-in.md)
- [ADR 0017](./0017-state-tiers-and-optional-backends.md)
- [ADR 0027](./0027-stateless-and-stateful-operating-modes.md)
- [ADR 0036](./0036-typed-policy-documents-generations-and-transitions.md)
- [ADR 0042](./0042-model-enablement-and-alias-contracts.md)
- [ADR 0050](./0050-runtime-policy-activation.md)
- [ADR 0058](./0058-tenant-owned-alias-names-and-the-management-catalogue.md)
- [P0: one operator model](../design/p0-one-operator-model.md)

Also supersedes the blob-backed control plane, grant model, `/namespaces/`
spelling, and namespace-owned model/alias graph of
[ADR 0062](./0062-blob-backed-flat-namespace-control-plane.md). Namespace as
the tenant unit and “the path selects the namespace” are absorbed here.

Withdraws, without deleting those files: minted inbound identity
([ADR 0016](./0016-minted-inbound-identity-and-principal-stores.md),
[ADR 0022](./0022-opt-in-gateway-token-minting.md)); config hot-reload of
routing and model tables ([ADR 0011](./0011-config-hot-reload.md)); the
hermetic no-datastore boot as a product promise
([ADR 0018](./0018-tier-0-hermetic-boot-gate.md)); catalogue import, pinned
offerings, approved books, and enablement identity
([ADR 0043](./0043-catalogue-source-imports.md),
[ADR 0046](./0046-approved-price-books.md),
[ADR 0047](./0047-callable-offering-identity.md),
[ADR 0051](./0051-durable-catalogue-snapshots-and-refresh-orchestration.md),
[ADR 0054](./0054-resolving-pinned-catalogue-offerings.md),
[ADR 0055](./0055-catalogue-imports-in-a-running-deployment.md),
[ADR 0056](./0056-request-path-pricing.md),
[ADR 0059](./0059-effective-dated-pricing-activation.md)).

This is the v1.0 product shape. It is a breaking change under
[ADR 0015](./0015-zero-dot-x-compatibility-policy.md).

## Context

Axond grew a two-mode product: a config-only default (ADR 0002 / 0017) and an
opt-in stateful control plane (ADR 0027) that owns tenants, projects, aliases,
enablements, price books, minted tokens, and a four-resource publication
graph. The P0 design tried to hide that graph behind one operator document.
ADR 0062 then flattened tenancy to namespaces and pointed the control plane
at object storage.

Litvue is the primary consumer. It already authenticates users, owns
workspaces, and will own the user-facing model catalog. What it needs from
Axond is narrower than what those ADRs built:

- a durable place to create a workspace-shaped namespace, cap its spend, and
  delete it;
- an SDK-compatible inference URL that already contains the workspace;
- one static key on a private network;
- usage records with cost, so Litvue does not keep an AI spend ledger.

The mode matrix, minted tokens, alias failover, and configured model catalog
are costs Litvue does not want to operate. This record deletes them.

## Decision

Axond is a **stateful inference gateway**. There is no stateless mode, no
in-memory or Redis budget or rate-limit tier, and no config hot-reload of
models. One `Store` trait is the storage abstraction. SQLite (WAL) is the
single-replica implementation; Postgres is the HA implementation. Boot
requires a store; a missing or unreachable store is a boot failure.

### Deployment config

TOML plus env holds only:

| Surface | Contents |
| --- | --- |
| `[server]` | bind, process-local serving |
| `[[provider]]` | `id`, `kind`, `base_url`, credentials, `unpriced_models = allow \| deny` |
| inbound auth | **one** static API key (env or file reference) |
| `[storage]` | `sqlite` (path) or `postgres` (DSN reference) |
| default blocklist | deployment-wide model-id glob patterns |
| `[catalog]` | imported models.dev metadata; admitted `cost` is the default charging source |
| optional `[[price]]` | fallback rates for offerings the catalogue does not list |
| `[admission]`, `[transport]`, `[shutdown]`, telemetry / usage-sink transport | unchanged process bounds and export |

Changing any of that is a restart. Namespaces, budgets, and usage are not in
the file.

`[[model]]`, `[[namespace]]`, `[[gateway_key]]` (plural), `[gateway_token]`,
`[[gateway_verifier]]`, `[gateway_minting]`, `[[gateway_token_epoch]]`,
`mode`, `[control_plane]`, `[secret_store]`, `[reload]` as a live channel,
`[budget]` Redis/in-memory/none backends, and `[rate_limit]` as a shared
tier are rejected at boot.

### Auth

One deployment-wide static API key authenticates **both** `/api/v1` and
`/ns/...` inference. Present as `Authorization: Bearer` or `x-api-key`.
Wrong or missing key is `401`. Minted `axt1.` tokens are not verified and
are not issued; those routes are unmounted.

Axond runs on a private network. The caller is trusted and is the tenant
isolation boundary: it maps its workspaces to namespace ids. Axond does not
mint per-tenant keys, does not speak OIDC, and does not host roles.

Authentication remains the outer identity-sensitive refusal
([ADR 0061](./0061-authentication-remains-an-outer-boundary.md),
[ADR 0013](./0013-inbound-auth-fails-closed.md)). `/healthz` and `/readyz`
stay unauthenticated.

### Namespaces

A namespace is an API-created resource and the tenant unit.

```text
id      caller-chosen, URL-safe, path-unique (e.g. wsp_…)
attrs   opaque JSON, copied onto usage records at admission
blocklist?   optional extra model-id globs
credentials? optional per-namespace provider credentials (BYOK; later)
```

`id` is 1–128 characters, `[A-Za-z0-9._-]+`, case-sensitive, immutable, not
parsed for hierarchy. `attrs` is capped at 4 KiB.

Count may reach tens of thousands. Credential pools
([ADR 0006](./0006-credential-pools-per-namespace-provider.md)) and circuit
breakers ([ADR 0008](./0008-target-failover-and-circuit-scope.md)) are
**lazy per namespace** (and, for circuits, per observed `(provider, model)`).
Boot does not preload namespace rows, pools, or breakers.

Unknown namespace on an inference or management path: typed `404
unknown_namespace` (same body for “never existed” and “deleted”; do not
enumerate).

### Inference routes

Namespace-prefixed so an SDK `baseURL` stays native:

```text
/ns/{ns}/v1/chat/completions
/ns/{ns}/v1/responses
/ns/{ns}/v1/messages
/ns/{ns}/v1/embeddings
/ns/{ns}/v1/models
```

Byte-faithful passthrough per wire
([ADR 0012](./0012-native-provider-routes.md),
[ADR 0005](./0005-streaming-relay.md),
[ADR 0023](./0023-openai-responses-passthrough.md)). No cross-wire
translation. Wire compatibility is the provider’s `kind`:
`/v1/messages` to an `openai` provider is `400 unsupported_wire`.

Unprefixed `/v1/...` inference is not served. No `x-axond-namespace`
header, no namespace in the JSON body.

### Models are not configured

The request `model` is `<provider-id>/<model-id>`. Axond splits on the
**first** `/`, selects the configured provider by id, and forwards the bare
id. Unknown provider prefix: `400 unknown_provider`. Missing `/`:
`400 model_unprefixed`. No `default_provider`.

Unknown upstream ids are forwarded; the upstream’s response is the answer.
Blocklists are glob patterns. Effective denials are the **union** of the
deployment default and the namespace’s optional list (a namespace can only
add). A blocked id is `400 model_blocked` and is not sent upstream.

Alias-level failover is removed. Ordered fallback across models is the
caller’s job. Credential-pool rotation inside one provider remains.

### Pricing

Charging uses **actual tokens × rates from the imported models.dev snapshot**
the request started under. Operator setup is `[[provider]]` plus `[catalog]`
import; a per-model TOML price book is not required for models.dev-covered
ids. The `[[provider]] id` must match the models.dev provider key.

Optional `[[price]]` is fallback for custom / unlisted offerings (vLLM, Azure
deployment ids). When both a snapshot `cost` and a `[[price]]` row exist, the
snapshot wins.

Per-provider `unpriced_models = allow | deny` applies when neither source
covers the id:

- `deny` → `400 unpriced_model` before dispatch;
- `allow` → dispatch; usage `cost_microdollars` is NULL.

The inference path does not fetch models.dev. A later catalogue admit cannot
reprice an in-flight request. Rates are micro-dollars per million tokens.

This supersedes the earlier “deployment `[[price]]` book is the rate source”
wording in this ADR for models.dev-covered offerings. Historical ADRs
0043–0056 remain records of approved price books; they are not the live
operator contract.

### Budgets

Per namespace per period:

```text
PUT /api/v1/namespaces/{ns}/budgets/{period}
{ "limit_microdollars": <int> }
```

`period` is a caller-chosen opaque string (1–128, `[A-Za-z0-9._-]+`).
Axond keeps `spent` per `(ns, period)`. Setting a limit **never**
resets spend. A PUT that lowers the limit below `spent` is accepted;
later admits fail once `spent >= limit`. Concurrent in-flight requests
can overshoot; see [ADR 0064](./0064-charge-actuals-after-response.md).

**Active period (chosen):** a successful PUT marks that period as the
namespace’s active period for admission. Inference does not carry the
period (no header, no body field) so the SDK contract stays `baseURL` +
static key. Plan change = PUT the same period with a new limit. New
billing period = PUT a new period key, which switches admission; the old
period’s spend is retained and still GET-able.

A namespace with **no** budget row yet is fail-closed:
`429 budget_exceeded` (nothing was published to spend against). Litvue
PUTs on workspace create.

```text
GET /api/v1/namespaces/{ns}/budgets/{period}
→ { limit_microdollars, spent_microdollars, reserved_microdollars,
    remaining_microdollars, active }
```

`remaining = max(0, limit - spent)`. `reserved_microdollars` is always `0`
(the field stays on the wire).

Denial when `spent >= limit` (or no budget row): `429 budget_exceeded`.
Store unavailable: `503 budget_unavailable`. Deployment
`[storage].on_unavailable = deny | allow` (default `deny`).

The ledger lives in the `Store`, not in Redis.

### Management API

All under `/api/v1`, same static key. OpenAPI 3.1 generated from the code
(utoipa or equivalent) and served at `/api/v1/openapi.json`. That document
is the integration surface; Litvue generates a TypeScript client from it
(the client itself is not an Axond crate).

| Method | Path | Role |
| --- | --- | --- |
| `POST` | `/api/v1/namespaces` | create `{id, attrs, blocklist?}` |
| `GET` | `/api/v1/namespaces` | list, cursor-paginated (default 100, max 1000) |
| `GET` | `/api/v1/namespaces/{ns}` | read |
| `PUT` | `/api/v1/namespaces/{ns}` | replace `attrs` / `blocklist`; id immutable |
| `DELETE` | `/api/v1/namespaces/{ns}` | idempotent remove (own slice) |
| `PUT`/`GET` | `/api/v1/namespaces/{ns}/budgets/{period}` | as above |
| `GET` | `/api/v1/namespaces/{ns}/usage?period=` | summary by `model` and `status` for that period (`period` required) |
| `GET` | `/api/v1/providers/{id}/models` | upstream discovery, cached |
| `GET` | `/api/v1/providers/models` | fan-out of the same |

`/admin/v1` is unmounted.

Discovery is cached, stamped `fetched_at` and `stale` (true when the last
upstream fetch failed). It is not on the inference path. Until the
discovery slice, `GET /ns/{ns}/v1/models` may return an empty list; after
it, that route lists cached, blocklist-filtered ids in
`provider-id/model-id` form for SDK compatibility. Litvue’s user-facing
catalog stays in Litvue.

### Usage schema

Kept, plus:

| Column | Change |
| --- | --- |
| `namespace` | already present; now the API namespace id |
| `attrs` | new, nullable JSON; copy of the namespace `attrs` at admission |
| `period` | new, nullable text; active period at admission |

Both additions are nullable, so not a schema-version bump
([usage schema](../usage-schema.md)). `subject` for the static key remains
the env-var name or file path.

### Kept unchanged

Request path (admission bounds, streaming relay, credential pools, circuit
breakers, telemetry names, typed inference errors that still apply).
Passthrough. Fail-closed auth placement. Usage at-least-once delivery.

### Deleted

- Stateless mode and the Tier 0 / 1 / 2 matrix as a product.
- In-memory and Redis budget and inbound rate-limit backends.
- Minted tokens, verifiers, epochs, `/v1/tokens`.
- `[[model]]` aliases, enablements, catalogue pins, `/admin/v1` bindings.
- Alias-level failover.
- Config hot-reload as a live channel for routing or models.
- Qualification harnesses whose only job is the removed tier / mode
  matrix: `qualification/recovery`, `qualification/rollout`,
  `qualification/stateful-endurance`, and the ADR 0018 “no datastore”
  gate as stated. Keep the harnesses that prove the request path
  ([ADR 0014](./0014-compatibility-and-soak-harness.md) black-box/soak,
  [ADR 0033](./0033-capacity-qualification-harness.md) capacity,
  [ADR 0040](./0040-endurance-qualification-harness.md) endurance,
  [ADR 0048](./0048-fault-qualification-harness.md) provider/transport
  faults), retargeted at `/ns/{ns}/v1` and SQLite or Postgres.

CI boots against a temp SQLite file. That is the single-replica gate, not
“no datastore”.

### Storage

Replaces the template’s “State tier” section: **there are no tiers**.

The `Store` is required. It owns namespace rows, budget ledgers
(`spent`, limit, active period), usage, and the discovery
cache. Implementations:

- **SQLite WAL** — one process, one file. Two processes on one file are
  unsupported.
- **Postgres** — HA, many replicas, one database.

No Redis, no blob control plane, no in-memory store, no “none”. Forward-only
migrations remain ([ADR 0032](./0032-operator-preflight-and-forward-only-migrations.md)).

Request-path store access is one namespace+admit join, then after the
response a spent increment. Usage append stays off the request path
(ADR 0009). Discovery refresh is background.

## Consumer contract (Litvue)

```text
workspace create  → POST /api/v1/namespaces {id: wsp_x, attrs: {org, plan}}
                    then PUT .../budgets/{period}
plan / period     → PUT  /api/v1/namespaces/{ns}/budgets/{period}
workspace delete  → DELETE /api/v1/namespaces/{ns}
SDK               → baseURL = ${gateway}/ns/${workspaceId}/v1
                    Authorization: the static key
                    model: provider-id/model-id
capacity meter    → GET  .../budgets/{period}
429               → Litvue capacity message
ledger            → Axond usage + budget GET; Litvue does not keep one
client            → generated TypeScript from /api/v1/openapi.json
```

## Chosen resolutions

Gaps the 2026-09-02 decisions did not name. These are closed here, not
reopened:

| Gap | Choice |
| --- | --- |
| ADR number | 0063; 0061/0062 already used |
| Unprefixed model id | `400 model_unprefixed`; no `default_provider` |
| Blocklist compose | union of deployment default and namespace extras |
| Price-book home | deployment TOML; restart to change; not an `/api/v1` resource |
| Price-book match | first matching glob in file order |
| Which period inference charges | last successful budget PUT becomes the namespace’s active period |
| No budget row | `429 budget_exceeded` (fail closed) |
| Usage `period` / `attrs` | additive nullable columns |
| `GET .../usage?period=` | `period` query required |
| Lowered limit < spent | accepted; later admits deny once spent >= limit |
| DELETE semantics | idempotent; in-flight finish; new inference `404 unknown_namespace`; usage rows retained; live budget row removed |
| `/v1/models` | empty until discovery; then cached prefixed ids, blocklist applied |
| TS client | Litvue generates it from the spec Axond serves |
| BYOK | field exists on the namespace resource; implementation is later |
| SQLite + replicas | unsupported; Postgres is the HA path |
| Inbound RPM/concurrency Redis | removed; per-replica `[admission]` remains |
| 0062 `/namespaces/` spelling | replaced by `/ns/` |
| Dual-mount unprefixed `/v1` | no |

## Implementation slices

Ordered so the request path does not regress. Each slice is independently
shippable; tests that hit inference move with the first slice that changes
their URL or body.

Tracked as [epic #424](https://github.com/Litvue/axond/issues/424).

1. **[#425](https://github.com/Litvue/axond/issues/425) Store + namespaces + prefixed routes + static key.** Required
   `Store` (SQLite WAL, Postgres). `POST`/`GET`/`PUT`/`list` namespaces.
   Inference only at `/ns/{ns}/v1/...`. One static key. Minted-token
   surface unmounted. Pools and breakers lazy. Usage `attrs`. Existing
   `[[model]]` routing may still resolve the `model` field in this slice
   so completions keep working.
2. **[#423](https://github.com/Litvue/axond/issues/423) Providers-only routing.** Drop `[[model]]`. Split
   `provider-id/model-id`. Wire by `kind`. Blocklists. Deployment
   price-book. `unpriced_models`. Remove alias failover. Discovery is
   **not** in this slice.
3. **[#426](https://github.com/Litvue/axond/issues/426) Budgets.** `PUT`/`GET` per `(ns, period)`; active-period rule;
   admit (spent-vs-limit) / charge actuals ([ADR 0064](./0064-charge-actuals-after-response.md));
   `429 budget_exceeded` / `503 budget_unavailable`; usage `period`.
4. **[#428](https://github.com/Litvue/axond/issues/428) OpenAPI.** utoipa (or equivalent) 3.1 spec at
   `/api/v1/openapi.json` covering every `/api/v1` route then mounted,
   including usage summary.
5. **[#429](https://github.com/Litvue/axond/issues/429) DELETE namespace.** Idempotent delete; 404 on later inference;
   usage retained.
6. **[#430](https://github.com/Litvue/axond/issues/430) Provider model discovery.** Cached `GET /api/v1/providers/{id}/models`
   and fan-out `GET /api/v1/providers/models`; `fetched_at` / `stale`;
   namespaced `/v1/models` lists the cache.
7. **[#427](https://github.com/Litvue/axond/issues/427) Retire tier-matrix qualification.** Delete recovery / rollout /
   stateful-endurance harnesses and the no-datastore gate; keep
   request-path evidence; boot CI against SQLite. #427 may land in
   parallel with 2–6 once #425 is in.

## Consequences

- Operational floor is “a SQLite file or Postgres”, not “a binary and a
  TOML”. Single-replica remains cheap; HA is one database, not Redis plus
  a control plane plus object storage.
- Litvue can treat Axond as a workspace-scoped proxy with a capacity
  meter. It does not import aliases, pins, or minted tokens.
- Tens of thousands of namespaces are a store and cache problem, not a
  boot problem. A pathological first-request miss is a store round-trip
  and a pool init, not a fleet-wide preload.
- Losing the store loses budget enforcement and namespace lookup. Default
  `on_unavailable = deny` makes that a `503`, not silent free traffic.
  Warm inference without a store is not a claim this ADR makes.
- Removing alias failover pushes model fallback into Litvue. Provider
  outages are pool rotation and the circuit breaker only.
- v1.0 is a clean break with 0.x config and `/admin/v1`. There is no
  mixed-mode migration. Cutover is: new config, new URLs, new key story.
- The ADR template’s “State tier” section is obsolete for work that
  follows this record; say which `Store` implementation the feature
  needs, and that it does not add a second one.

## Alternatives considered

**Keep stateless as default, stateful as opt-in (ADR 0002/0027).**
Rejected. Litvue will not run the stateless product, and the matrix is
what made the operator model unusable.

**Blob-backed revision journal (ADR 0062).** Rejected for v1.0. Namespaces
and budget ledgers want transactions and `SELECT FOR UPDATE`, not
compare-and-swap of a desired-state graph. Object storage may return later
as a usage sink, not as the `Store`.

**Minted per-namespace tokens.** Rejected. The caller is the isolation
boundary; a second identity system inside Axond duplicates Litvue.

**Period on every inference request.** Rejected. It would break “set
`baseURL` and go” for ordinary SDKs.

**Uncapped until first PUT.** Rejected. Fail-closed matches
`on_unavailable = deny` and Litvue’s create-then-PUT sequence.

**Keep `/admin/v1` as an expert escape hatch.** Rejected. One management
tree under `/api/v1` with a generated spec.

## References

- Litvue consumer contract, maintainer session 2026-09-02
- [Issue #423](https://github.com/Litvue/axond/issues/423) (providers-only
  routing; discovery and per-subject budgets in that issue are superseded
  by this record)
- [usage schema](../usage-schema.md), [configuration](../configuration.md)
- ADRs 0005, 0006, 0008, 0009, 0010, 0012, 0013, 0014, 0015, 0023, 0028,
  0030, 0032, 0060, 0061 (remain in force as cited)
