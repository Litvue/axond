# 27. Stateless and stateful operating modes

Date: 2026-08-12

## Status

Accepted

Supersedes the permanent-config-ownership rule of
[ADR 0017](./0017-state-tiers-and-optional-backends.md) ("One dimension, one
owner") and its "no runtime control plane" deferral. ADR 0017's state tiers,
per-feature tier declarations, and the hermetic Tier 0 gate
([ADR 0018](./0018-tier-0-hermetic-boot-gate.md)) remain in force.

Implemented by #162 (mode and bootstrap parsing), #163 (responsibility-specific
backend contracts), #141 (revisioned `ControlPlaneStore`), and #142 (revision
reconciliation into runtime snapshots). This ADR is the settled design those
slices consume; it introduces no code, schema, or public trait.

## Context

ADR 0002 makes a zero-state boot the product, and ADR 0017 hardened that with
explicit tiers plus a rule that namespaces, providers, aliases, prices, and
provider credentials are *permanently* config-owned. That rule exists to
prevent split-brain authority, and it did its job while Axond was a
single-tenant, file-configured proxy.

It also forecloses a real product: a multi-tenant deployment where tenants,
projects, provider credentials, model catalogues, prices, and policies are
administered through an API and stored durably, without a human editing TOML
and restarting a fleet. ADR 0017 deferred that as "a runtime control plane"
and left the door open at Tier 2 for callers/keys only.

Reopening that door needs answers to the questions that make control planes
fail:

- Does the request path now depend on a database? (If yes, the Tier 0 pitch and
  the availability story are both gone.)
- Which authority owns each dimension when both a file and a database exist?
- What happens to a running fleet when Postgres, Redis, the secret store, or
  the model-metadata source is unavailable?
- How does an existing stateless deployment upgrade without changing anything?

ADR 0017's tier vocabulary answers "what backends does a deployment depend on".
It does not answer "who owns a resource's definition". Those are different
questions, and conflating them is why the ownership rule had to be absolute.

## Decision

**Axond has exactly two operating modes, selected by one bootstrap key.**

```toml
mode = "stateless"   # default; omitting the key means stateless
```

`mode` is the only switch. There is no per-dimension, per-namespace, or
per-resource migration flag, and no mode in which some tenants are file-owned
while others are database-owned within the same process.

### Mode definitions

**Stateless mode is the backward-compatible default.** Omitting `mode` selects
it. TOML plus the environment variables and files it references remains the
authority for every resource, exactly as documented in
[configuration.md](../configuration.md) and reloaded under
[ADR 0011](./0011-config-hot-reload.md). ADR 0017's tiers still describe the
optional Redis/Postgres backends a stateless deployment may select for budgets,
rate limiting, revocation, and durable usage; selecting them does not make the
deployment stateful in the sense of this ADR.

The one carve-out ADR 0017 already grants stands: a Tier 2 store-backed
principal layer may own *caller and key lifecycle* for the credential shapes it
declares, under the shape-ownership, TTL-cache, and fail-closed rules of
[ADR 0016](./0016-minted-inbound-identity-and-principal-stores.md). That is
caller/key lifecycle only — namespaces, providers, aliases, prices, and provider
credentials stay config-owned in stateless mode — and it is a different
mechanism from stateful mode, where identities are compiled into the snapshot
instead of resolved per request. **Stateless mode is otherwise unchanged by this
ADR**, including its Tier 0 hermetic guarantee.

**Stateful mode moves durable-resource ownership to the control plane.**
Bootstrap TOML shrinks to the things a process needs before it can read
anything else:

| Bootstrap section | Purpose in stateful mode |
| --- | --- |
| `mode` | Selects stateful. |
| `[server]` | Listener and process-local serving parameters. |
| telemetry (`OTEL_*` environment plus `[[usage_sink]]` transport settings) | Where traces, metrics, logs, and usage rows go. |
| Postgres control-plane connectivity | DSN *reference* for the control-plane database. |
| secret-store and KEK settings | Which `SecretStore` implementation, and the key-encryption-key reference used to unwrap tenant secrets. |
| static breakglass operator credential | The mandatory `/admin/v1` breakglass identity, referenced the way `[[gateway_key]]` already is. |
| hot-state backend connectivity | DSN *references* for the opt-in Tier 1/Tier 2 budget, rate-limit, and revocation backends a deployment selects. |

Bootstrap owns *connectivity* to the opt-in enforcement backends; the control
plane owns their *policy values*. So in stateful mode a `[budget]` or
`[rate_limit]` section may carry only backend selection and DSN references, and
carrying limits, windows, or scopes there is the same boot error as any other
stateful-owned resource. Deployments that select no such backend keep the
process-local behaviour ADR 0017 already defines.

Nothing else. Tenants, projects, identities, providers, provider credentials,
model catalogues, prices, aliases, and policies are **not** expressible in
stateful bootstrap TOML.

### Split-brain ownership is rejected by design, not reconciled

**A resource class has exactly one authority in a given mode, and the mode is
process-wide.** Stateful mode does not merge, overlay, prefer, or fall back
between TOML and durable resources: presence of a stateful-owned section in a
`mode = "stateful"` configuration is a **boot error before the listener
binds**, and the same rejection applies on reload while the previous
configuration keeps serving.

This replaces ADR 0017's mechanism (permanent file ownership) while preserving
its property (one authority per dimension). The property was never about files;
it was about never having to write a merge policy for two disagreeing sources
of truth.

There is deliberately **no** partial or per-resource migration mode. A
"stateless for aliases, stateful for credentials" deployment would need exactly
the merge semantics this ADR refuses to define.

### The request path reads one immutable snapshot

**Ordinary inference reads a single immutable in-memory snapshot and never
queries the control plane.** A request resolves its inbound identity,
namespace/tenant authority, alias, targets, credential pool, prices, and
policies from the snapshot it captured when it started, and it keeps that
snapshot for its whole lifetime — including buffered responses, streaming
relays, and the entire failover walk. A revision published mid-request never
changes a request already in flight (ADR 0011's `ArcSwap` publication and
[ADR 0005](./0005-streaming-relay.md)'s relay both already behave this way).

Control-plane availability and data-plane availability are therefore separate
properties:

- **Data-plane availability** is the ability to serve inference from an active
  snapshot. In both modes it depends on the process, the snapshot, and the
  upstream provider — plus any *separately selected* Tier 1/Tier 2 admission
  backend (budget, rate limit, revocation) the deployment turned on.
- **Control-plane availability** is the ability to *read or change* durable
  desired state: administrative APIs, revision publication, and convergence of
  new or restarting replicas. It depends on Postgres and the secret store.

A control-plane outage degrades change and cold start. It does not degrade
inference on replicas that already hold a snapshot.

### Every request-path database access, named

This is the exhaustive list of operations that may touch Postgres or Redis
while an inference request is in flight. Anything not on this list is a bug in
the implementation slices, not an undocumented extension of it.

| Operation | Backend | Mode | Status |
| --- | --- | --- | --- |
| Exact shared spend cap check/reserve/reconcile | Redis or Postgres | Both | Existing, opt-in ([ADR 0010](./0010-shared-budget-backends-and-charging-policy.md)) |
| Exact fleet-wide in-flight rate-limit lease | Redis | Both | Existing, opt-in (ADR 0017) |
| Precise minted-token `jti` revocation check | Redis or Postgres | Both | Existing, opt-in (ADR 0017 amendment) |
| Durable usage row write | Postgres | Both | Existing, opt-in, **off** the request path (buffered sink) |
| Store-backed caller/key principal resolution for a declared credential shape | Redis or Postgres | **Stateless only** | Permitted but unimplemented (ADR 0016/0017); cached, fail-closed. Stateful mode resolves identities from the snapshot instead |

Explicitly **not** on the request path in stateful mode:

- inbound key/token/OIDC principal lookup — resolved from the snapshot;
- tenant, project, namespace, or policy resolution;
- alias and target resolution, including every step of the failover walk;
- provider credential selection and pool health;
- catalogue lookup (`/v1/models`) and price lookup;
- revision, manifest, or audit reads.

Secret material follows the same rule: a snapshot is only publishable once
every credential reference it needs is **already resolved into memory**. A
request never unwraps a secret, never calls the secret store, and never reads a
KEK. Secret resolution happens during snapshot compilation, which is control
plane work.

Consequently, `/healthz` and `/readyz` stay Tier 0 in both modes: readiness
reflects whether the replica holds an active snapshot, not whether Postgres is
reachable.

### Durable state is immutable revisions of versioned resources

**A durable revision is an immutable manifest that references versioned
resources**; it does not embed their mutable current state. Publishing a change
creates a new resource version and a new manifest referencing it. Manifests and
resource versions are never edited in place, so:

- any retained revision hydrates deterministically into a complete candidate;
- "what was serving at 14:00" is answerable exactly;
- rollback is publishing an earlier manifest, not reverse-engineering a diff;
- a replica reports desired, loaded, and active revision numbers independently.

Compilation of a candidate revision reuses the **same whole-graph validation**
stateless boot and reload already perform. A candidate that fails validation,
reference resolution, or secret resolution is rejected atomically and the
previous revision keeps serving; there is no partial publication.

#### Initial cold boot in stateful mode requires Postgres

A process starting in stateful mode with no snapshot **must** reach the control
plane. It fails to become ready otherwise, and it fails loudly rather than
serving an empty or partial configuration — *unless* it holds a signed
last-known-good snapshot of a revision it previously served, which #142 lands
alongside reconciliation and which relaxes this requirement to "reach the
control plane or restore an authenticated local cache".

The cache is not a general fallback. It is consulted for exactly one failure,
the control plane being unreachable, because that is the only failure where
cached state is the better answer: a desired revision that exists but does not
compile stays a fatal boot failure, since booting an older cached revision
would silently serve state an administrator already replaced. It is
authenticated before it is interpreted and re-verified through the domain's
integrity checks afterwards, so an edited cache refuses to boot rather than
becoming desired state. See
[revision convergence](../operations/revision-convergence.md).

The asymmetry that remains is deliberate: a running replica must survive a
control-plane outage, and a brand-new replica can only do so if a sibling left
it a signed snapshot to start from.

### Administrative surface

Administrative APIs live under **`/admin/v1`**, disjoint from the inference
routes, and never share a credential type with them.

- **Human identity is OIDC.** Operators and tenant administrators authenticate
  as humans through an external identity provider; Axond does not own passwords
  or sessions.
- **A static breakglass operator credential is mandatory**, referenced from
  bootstrap the way `[[gateway_key]]` already is. It exists precisely for
  "OIDC is down" and "the control plane rejected the last change", which is the
  same reasoning ADR 0017 used to keep an always-present config breakglass key.
- Inference credentials (static gateway keys, minted `axt1.` tokens per
  [ADR 0016](./0016-minted-inbound-identity-and-principal-stores.md)) grant no
  administrative authority, and administrative identity grants no inference
  authority.
- Every mutation is written transactionally with its audit event and accepts an
  idempotency key, so retries are safe.

`/admin/v1` is control plane. Its unavailability is a control-plane outage, and
`503` there says nothing about inference health.

### Identifiers

**Durable resources carry a UUIDv7 internal identifier plus a tenant-scoped
human-readable slug.** UUIDv7 is the stable, never-reused primary reference:
time-ordered for index locality, generatable without coordination, and safe to
put in manifests, audit events, and foreign references. The slug is what humans
and API callers type; it is unique within its tenant scope, and it may be
renamed without invalidating references.

Usage records and telemetry keep the human-readable namespace/tenant identity
they already emit so the
[usage schema](../usage-schema.md) contract is unaffected; the UUID is
additional, not a replacement.

### Secrets

**Encrypted Postgres is the first `SecretStore` implementation.** Tenant
provider credentials are stored wrapped under a KEK referenced from bootstrap,
and the plaintext exists only in memory, only inside a compiled snapshot.
Secrets are represented by opaque references everywhere else: manifests,
diagnostics, audit events, `/admin/v1` responses, logs, and `Debug` output name
a reference, never a value — the rule stateless mode already enforces for
`env`-referenced keys.

External secret managers (Vault, cloud KMS/secret managers) remain future
adapters behind the same contract. Choosing encrypted Postgres first avoids
making a second infrastructure dependency mandatory in the first stateful
release.

### Model metadata versus tenant intent

**Metadata may refresh automatically; enablement and pricing are explicit
acts.**

- Axond may refresh model metadata from models.dev in the background and store
  new or changed *catalogue metadata* without human action.
- A refresh never enables a model for a tenant, never changes which alias
  targets exist, and never activates a price.
- Enabling a model for a tenant and activating a price are explicit
  administrative mutations that produce a new revision and an audit event.

A metadata refresh that silently changed billing or availability would make an
upstream catalogue edit a production change. ADR 0002 already denominates
budgets in currency derived from configured prices; automatic price activation
would break the promise that budget and billing never disagree.

The catalogue source is therefore never on the request path and never a boot
dependency: a stale or unreachable models.dev is a background-refresh failure.

### Redis is hot state only

**Redis is never a durable control-plane store.** It holds exactly what ADR
0017 already gave it — exact shared budget counters and reservations, in-flight
rate-limit leases, precise revocation entries — and it must not be selectable
as an implementation of durable control-plane state. Its data model is
loss-tolerant hot state with expiry semantics; durable desired state needs
transactions, migrations, backup/restore, and referential integrity.

Losing Redis loses hot enforcement precision. Losing durable state loses the
deployment. Those must not be the same store.

### Responsibility-specific backends, no universal `StateBackend`

**Backend selection is per responsibility.** The contracts (internal to the
gateway crate; #163 scaffolds them) are:

| Contract | Responsibility | Permitted implementations | Path |
| --- | --- | --- | --- |
| `ControlPlaneStore` | Durable desired state: revisions, manifests, resources, audit | Postgres | Control plane only |
| `SecretStore` | Wrapped secret material and unwrapping | Encrypted Postgres (first); external managers later | Snapshot compilation only |
| `CatalogSource` | Model metadata ingestion | models.dev (first) | Background only |
| `BudgetStore` | Spend caps | none / in-memory / Redis / Postgres | Request path (opt-in) |
| `RateLimiter` | Inbound admission | `NoLimit` / in-memory / Redis | Request path (opt-in) |
| `RevocationStore` | Precise `jti` revocation | none / Redis / Postgres | Request path (opt-in) |
| `UsageSink` | Durable usage rows | stdout / OTLP / Postgres | Off the request path |

A single universal `StateBackend` trait is rejected. It would imply that any
backend can serve any responsibility — which is exactly how Redis would end up
holding durable state — and it would force one error taxonomy, one availability
policy, and one consistency model onto seams that legitimately differ:
`BudgetStore` needs millisecond request-path reads with a fail-closed policy;
`ControlPlaneStore` needs transactional multi-row writes with optimistic
concurrency and is allowed to be slow. Keeping them separate also keeps each
responsibility's `on_unavailable` policy independently reviewable.

The contracts stay internal to the gateway crate in this phase: no new
published workspace crate, and no public trait in `gateway-core` or
`gateway-transport` until an implementation has proven the shape. Their scaffolded
shapes, path declarations, capability set, and error categories are documented in
[backend responsibility boundaries](../maintainers/backend-contracts.md).

### State tier

Stateless mode is unchanged: **Tier 0** by default, with Tier 1/Tier 2 opt-ins
exactly as ADR 0017 describes. Stateful mode is **Tier 2**: Postgres is a
control-plane and cold-boot dependency, and the secret store is required for
snapshot compilation. Selecting stateful mode is the operator's explicit act, so
no existing deployment's tier is raised.

## State ownership matrix

"Stateless" is today's behavior. "Stateful" is what this ADR settles. Authority
is exclusive: nothing appears in TOML and durable state simultaneously within
one mode.

| Dimension | Stateless owner | Stateful owner | Durability | On the request path? |
| --- | --- | --- | --- | --- |
| Bootstrap (`mode`, `[server]`, telemetry, control-plane DSN reference, secret-store/KEK settings, static breakglass credential) | TOML + env/files | TOML + env/files | Process-local file | No (read at boot/reload) |
| Tenants and projects | TOML `[[namespace]]` | `ControlPlaneStore` | Postgres revision | No — resolved from snapshot |
| Human/administrative identities | n/a (no admin API) | OIDC issuer + `ControlPlaneStore` bindings; static breakglass in bootstrap | Postgres + IdP | No — `/admin/v1` only |
| Inference identities (static keys, minted signing/verification policy, epochs) | TOML `[[gateway_key]]`, `[gateway_minting]`, `[gateway_token]`, `[[gateway_verifier]]`, `[[gateway_token_epoch]]` (plus an optional ADR 0016 store-backed layer for caller/key lifecycle) | `ControlPlaneStore` (static breakglass stays in bootstrap) | Postgres revision | Stateless: yes, if a store-backed principal layer is selected. Stateful: no — snapshot; opt-in `RevocationStore` check is separate |
| Providers and endpoints | TOML `[[provider]]` | `ControlPlaneStore` | Postgres revision | No — snapshot |
| Provider credentials (BYOK) | TOML `[[credential]]` → env var | `ControlPlaneStore` reference + `SecretStore` material | Postgres revision + wrapped secret | No — resolved into snapshot before publication |
| Model catalogue metadata | TOML `[[model]]` | `CatalogSource` ingestion into `ControlPlaneStore` | Postgres | No — background refresh |
| Tenant model enablement | TOML `[[model]]` | `ControlPlaneStore`, explicit mutation | Postgres revision | No — snapshot |
| Prices | TOML target `price.*` | `ControlPlaneStore`, explicit activation | Postgres revision | No — snapshot |
| Aliases and targets | TOML `[[model]] targets` | `ControlPlaneStore` | Postgres revision | No — snapshot |
| Policies (failover, credential-pool strategy, budget/rate-limit policy values) | TOML `[failover]`, `[credential_pool]`, `[budget]`, `[rate_limit]` | `ControlPlaneStore` | Postgres revision | No — snapshot |
| Hot enforcement state (budget counters/reservations, rate-limit leases, revocation entries) | Selected backend (Tier 0/1/2) | Selected backend (Tier 0/1/2) | Redis/Postgres, expiry-bounded | **Yes**, when opted in |
| Hot enforcement backend selection and connectivity | TOML `[budget]`, `[rate_limit]`, `[revocation]` | TOML bootstrap (references only; policy values are control-plane owned) | Process-local file | No (read at boot/reload) |
| Circuit and credential health | Process memory | Process memory | None (per replica) | Yes, in-process only |
| Usage records | `UsageSink` | `UsageSink` | stdout/OTLP/Postgres | No — buffered, off path |
| Audit events | n/a | `ControlPlaneStore`, same transaction as the mutation | Postgres | No |
| Change trigger | TOML `[reload]` file watch / `SIGHUP` (ADR 0011) | Revision convergence (#142); `[reload]` has no stateful-owned resources to reload | Process-local | No |
| Active runtime snapshot | Compiled from TOML | Compiled from a revision | Process memory (`ArcSwap`) | Yes — the only thing the request path reads |

## Failure and outage matrix

| Dependency | Unavailable while replicas hold a snapshot | Unavailable at cold start (stateful) | Change/administration | Notes |
| --- | --- | --- | --- | --- |
| Postgres (control plane) | Inference continues on the active snapshot; convergence stalls; revision lag grows and is exported | Replica fails readiness; it does not serve partial state | `/admin/v1` mutations fail; `503` | Data-plane availability is preserved by design |
| Postgres (budget/revocation/usage backends) | Existing per-feature `on_unavailable` policy applies (default `deny`, typed `503`); usage buffers then drops with metrics | Boot-time connectivity is validated as today | Unchanged | Independent from control-plane connectivity, even if the same server |
| Redis (hot state) | Selected feature's `on_unavailable` policy applies; default `deny` yields `503 budget_unavailable` / `rate_limit_unavailable` / `revocation_unavailable`; precision is lost, durable state is not | Boot-time PING is validated as today | Unaffected | Redis loss never loses desired state |
| Secret store / KEK | Active snapshot keeps its already-resolved material and serves | Cold boot cannot compile a snapshot; replica stays unready | Mutations touching secrets fail | Failure surfaces as candidate rejection, never as a request-time secret fetch |
| Catalogue source (models.dev) | No effect on inference | No effect: catalogue metadata is read from Postgres, not upstream | Refresh fails with metrics and a stale-metadata age signal; explicit enablement/pricing still work on stored metadata | Never a boot or request dependency |
| Provider discovery / upstream provider | Existing failover walk and circuit behavior ([ADR 0008](./0008-target-failover-and-circuit-scope.md)) | Unchanged | Unchanged | Provider egress was never Tier 0's concern |
| A rejected candidate revision (malformed, dangling reference, unresolvable secret) | Previous revision keeps serving on every replica; rejection reason is exported | Cold start fails rather than serving partial state | Publication is refused atomically | No partial publication anywhere |
| OIDC provider | No effect on inference | No effect | Human administration fails; the static breakglass operator credential remains | The reason breakglass is mandatory, not optional |

## Compatibility and migration posture

- **Every configuration valid today remains valid, unchanged, and stateless.**
  `mode` is optional and defaults to `stateless`; no existing deployment adds a
  key, a datastore, or a migration to upgrade.
- Under the [`0.x` policy](../compatibility.md#the-config-surface) this is an
  additive change: a new key with a default. Stateless validation is not
  tightened.
- The Tier 0 hermetic gate (ADR 0018) continues to run the stateless default,
  network-free. Stateful mode gets its own integration coverage and never
  enters the hermetic lane.
- **Stateful mode is opt-in, all-at-once, and per process.** The migration path
  is: stand up Postgres and the secret store, import the resources currently in
  TOML through `/admin/v1`, publish a revision, then start replicas with
  `mode = "stateful"`. There is no mixed-authority intermediate state, so
  cutover is a deployment change, not a config merge.
- Rolling a fleet from stateless to stateful with both modes live temporarily is
  a *deployment* choice: each replica serves its own authority consistently, and
  operators must accept that the two populations may serve different resource
  definitions during the roll. Axond does not attempt to reconcile them.
- Rollback from stateful to stateless is redeploying the previous TOML. Nothing
  in stateless mode reads durable state, so a rollback cannot half-apply.
- The [usage schema](../usage-schema.md) and gateway-originated error
  vocabularies are unchanged by this ADR. `/admin/v1` introduces no new
  inference-route behavior.

## Implementation dependency map

```
#140 (tracker)
 └── #161  this ADR — settled ownership, failure semantics, request-path rules
      ├── #162  mode + bootstrap parsing/validation
      │           consumes: mode definitions, bootstrap section list,
      │                     split-brain rejection, compatibility posture
      ├── #163  responsibility-specific backend contracts
      │           consumes: contract table, no-universal-StateBackend,
      │                     request-path vs control-plane split,
      │                     opaque secret references
      └── #141  revisioned ControlPlaneStore on Postgres  (needs #162, #163)
                  consumes: immutable manifests over versioned resources,
                            UUIDv7 + tenant-scoped slugs, transactional
                            audit, idempotency keys, deterministic hydration
                            └── #142  revision → snapshot reconciliation
                                        consumes: one-snapshot-per-request,
                                                  atomic publication, keep
                                                  previous on failure, cold-boot
                                                  requires Postgres (and later
                                                  relaxes it via signed
                                                  last-known-good)
```

Decisions each downstream slice may **not** reopen: the default mode; the
bootstrap section list; process-wide exclusive authority; the request path's
freedom from control-plane queries; manifest immutability; `/admin/v1` with
OIDC plus mandatory static breakglass; UUIDv7 plus tenant-scoped slugs;
encrypted-Postgres `SecretStore` first; automatic metadata refresh with
explicit enablement and price activation; Redis as hot state only; and the
absence of a universal `StateBackend`.

Deliberately left open for those slices: exact TOML key names and section
shapes within the approved bootstrap set (#162); precise error and capability
enum variants (#163); and DDL, index, and concurrency-control details (#141).
Settled by #142: polling as the correctness mechanism with notifications as a
latency optimization, per-replica desired/loaded/active reporting with lag as
the alertable signal, bounded exponential retry, and the authenticated
last-known-good cache format.

## Consequences

- Axond can be administered as a multi-tenant product without giving up the
  single-binary, file-configured deployment, because the two modes are separate
  products of one binary rather than a spectrum.
- Inference availability stops depending on control-plane availability, which is
  the property most self-hosted gateways lose when they add a database. The cost
  is that a stateful change is only as fast as revision convergence, and a
  replica can knowingly serve a stale-but-valid revision.
- ADR 0017's absolute file-ownership rule is gone, so its split-brain protection
  now rests on mode exclusivity and boot-time rejection. That protection is only
  as strong as the validation in #162, which is why mixed authority must fail
  before the listener binds.
- Refusing a per-resource migration mode makes some adoptions harder: a large
  TOML deployment must import everything before cutting over. The alternative is
  a merge policy, and no merge policy survives contact with a disagreement.
- Immutable manifests over versioned resources cost storage and add a garbage
  and retention question, and they make audit, rollback, and "what was serving"
  cheap and exact.
- A control-plane outage no longer blocks scale-out for replicas that hold a
  signed last-known-good snapshot, at the cost of a new deployment-wide signing
  secret and of replicas that may knowingly start on state older than desired.
  A replica with no cache still cannot cold-boot during an outage, so operators
  must still size and monitor for that.
- `/admin/v1` is a new attack surface with a mandatory static breakglass
  credential — a long-lived secret whose rotation and storage become an
  operational duty, in exchange for not being locked out by an IdP outage.
- Encrypted Postgres for secrets keeps the dependency count down but makes the
  KEK the highest-value secret in the deployment: its loss is unrecoverable for
  wrapped material, and its rotation is a real procedure.
- Explicit model enablement and price activation mean tenants do not get new
  models "for free" when models.dev changes, so operators must actively curate
  the catalogue. That is the price of never letting an upstream edit change
  billing.
- Seven responsibility-specific contracts instead of one universal backend
  multiplies the test matrix and the number of `on_unavailable` policies to
  review, and it is what keeps Redis structurally unable to become the
  system of record.
