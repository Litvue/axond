# Revision convergence

> **Withdrawn ([ADR 0063](../adr/0063-stateful-only-namespaced-gateway.md)).** Control-plane revision convergence is gone. Historical record; do not follow as a runbook.

In stateful mode, a change is *published* to the control plane and then
*converges* onto every replica. This page is the operator's view of that second
half: how fast it is, what a replica reports about itself, what happens when it
cannot converge, and how a replica boots while PostgreSQL is unavailable.

Stateful serving uses the convergence layer
([#142](https://github.com/Litvue/axond/issues/142)) over the revision journal
([#165](https://github.com/Litvue/axond/issues/165)). The runtime binds its
listener before the first bounded bootstrap attempt, then publishes only
complete projected snapshots. Read [ADR 0027](../adr/0027-stateless-and-stateful-operating-modes.md)
for the mode as a whole.

## How a change reaches a replica

1. An administrator publishes a revision. The journal advances one head pointer.
2. Every replica polls the head, independently. A PostgreSQL notification, when
   one arrives, only makes a replica poll sooner.
3. A replica that sees a head it is not serving hydrates the **whole** revision,
   projects it onto its configuration, runs the same whole-graph validation boot
   runs, and resolves every secret the result needs.
4. If all of that succeeds, including durable workload-principal projection,
   the replica swaps in the new snapshot atomically and the next request is
   served from it.
5. Once an active snapshot exists, if any later candidate fails, the replica
   keeps serving what it already had and reports why. A bootstrap with no
   complete revision remains unready and fails closed.

When the published snapshot carries effective-dated pricing, the reconciler also
arms a timer for `PricingSnapshot::effective().ends()`. At that boundary it
re-runs the same compile/admit/publish path against the current durable revision,
even if no administrator has published a new revision and no request arrives.
The schedule is derived from the snapshot after each successful publication, so
an `effective_until` boundary can restore a baseline rule or make a target
ineligible according to the approved-book rules. A failed boundary refresh keeps
the prior snapshot and retries with the normal bounded backoff. On restart,
bootstrap resolves the durable book at the current instant and reconstructs the
next timer; no derived timer state is persisted.

Two consequences worth internalising:

- **Replicas converge independently, so a fleet is briefly mixed.** Two replicas
  can serve different revisions for the length of one convergence. There is no
  fleet-wide barrier, and adding one would mean an unavailable replica could
  block everyone else's rollout.
- **A request never straddles a revision.** A request resolves its alias, its
  credential, and its circuit against the snapshot it started with, and a stream
  relays to completion against that same snapshot. A revision published
  mid-request does not change that request's routing, pricing, or authorisation.

## Convergence targets

| Situation | Time to serve the new revision |
| --- | --- |
| Notification delivered | Compile time (milliseconds), no poll wait |
| Notification lost or disabled | Up to one poll interval, plus compile time |
| Effective-dated pricing boundary | At the boundary, plus compile time; the scheduler is off the request path |
| Control plane unreachable after an active snapshot | Not until it returns; the previous revision keeps serving |
| Revision refused | Never, until the revision is fixed or replaced |

Polling is the mechanism that makes convergence *correct*; notifications only
make it fast. A PostgreSQL `NOTIFY` delivered while a replica is reconnecting is
gone for good, so a replica that waited for one could sit on a stale snapshot
indefinitely. Disabling notifications costs latency, never convergence.

The **convergence target** is the divergence an operator is willing to treat as
normal. Divergence lasting longer than the target is an incident, not rollout in
progress, and that is the threshold to alert on — not the poll interval, and not
a revision-identifier comparison, which is noisy by design during a rollout.

## What a replica reports

Each replica reports three revision identifiers, and they answer different
questions:

| Reported | Means |
| --- | --- |
| `desired` | What the control plane says should be serving. The same on every healthy replica. |
| `loaded` | The newest revision this replica hydrated *and* compiled successfully. |
| `active` | What requests are actually served from. The only one that describes behaviour. |

Read them together:

- `desired == loaded == active` — converged.
- `desired != loaded` — this replica cannot *accept* the desired revision.
  Something refused it; the reported reason says which stage.
- `loaded != active` — a candidate compiled but is not serving. Transient by
  construction, since publication follows compilation immediately.

Alongside them:

- **lag** — how long `desired` has differed from `active`. This is the alerting
  signal. One second of lag is convergence working; ten minutes is an incident.
- **reason** — the stage that refused the last candidate: `unavailable`,
  `incompatible`, `corrupt`, `not_found`, `projection`, `validation`, `secret`,
  `snapshot`, `pricing`, or `clock` — or, when the candidate compiled but its
  policy was refused before it could be enforced, `unsupported`, `migration`,
  `refused`, `withdrawn`, or `ungoverned` ([policy
  activation](./policy-activation.md#classification-live-drain-migration-refused)).
- **source** — `control-plane` or `last-known-good`. A replica reporting
  `last-known-good` booted from its cache and may be serving something older
  than desired.
- **generation** — the local snapshot counter, incremented on every publication,
  which request logs correlate against.

### Metrics

| Metric | Use |
| --- | --- |
| `axond.revision.lag` (ms) | Alert when it exceeds the convergence target |
| `axond.revision.converged` | `1` converged, `0` diverged; alert on a *fleet* where this stays 0 for one replica |
| `axond.revision.rejections` (by `reason`) | Which stage is refusing, and whether it is the store or the state |
| `axond.revision.consecutive_failures` | Backoff depth; a rising value is a replica that keeps failing |
| `axond.revision.convergence_duration` (ms) | How long an accepted revision takes to compile and publish |
| `axond.revision.attempts` (by `trigger`, `outcome`) | Whether convergence is being driven by notifications, polls, or an effective-dated pricing boundary (`pricing-boundary`) |
| `axond.revision.desired_at` / `axond.revision.active_at` | Publication timestamps embedded in the revision identifiers, for comparing replicas |
| `axond.revision.last_known_good` (by `outcome`) | Cache exports, export failures, cold-boot restores, and a cache this build cannot read (`incompatible`) |
| `axond.config.generation` | Which snapshot generation a replica serves |

Alert on **lag above the convergence target**, and on
`axond.revision.last_known_good{outcome="restored"}`, which means a replica
started without reaching the control plane.

A refusal this build cannot read is its own label rather than a bucket shared with
damage: `axond.revision.rejections{reason="incompatible"}`, and `schema_incompatible`
in a status response. Alert on it separately from `corrupt` — the action is a
deployment, not a restore.

## Resource body schemas

A published resource carries a **body**: a record whose meaning is fixed by an
explicit schema identifier stored inside it, alongside the resource's identity,
scope, and slug. Seven schemas exist today:

| Schema | Resource | Fields |
| --- | --- | --- |
| `axond.tenant.v1` | a deployment tenant | `schema`, `tenant_id`, `display_name` |
| `axond.project.v1` | a tenant-owned project | `schema`, `project_id`, `tenant_id`, `display_name` |
| `axond.provider-credential.v1` | a tenant's or project's credential for one provider | `schema`, `credential_id`, `tenant_id`, `project_id` (a project's only), `provider_id`, `display_name`, `secret_id`, `secret_version`, `lifecycle` |
| `axond.policy.v1` | the policy of a tenant or a project | `schema`, `tenant_id`, `project_id` (project documents only), `epoch`, `budget_limit_microdollars`, `namespace_budget_limit_microdollars` (optional), `reservation_ttl_seconds`, `max_in_flight_per_subject`, `lease_ttl_seconds`, `minimum_token_epoch`, `content_middleware` (optional ordered list; `axond.redact` carries `guardrail.key_env` and ordered `rules`), `buffered_response_routes` (optional normalized set: `messages`, `responses`) |
| `axond.model-enablement.v1` | a tenant's or project's permission to use one catalogue offering | `schema`, `enablement_id`, `tenant_id`, `project_id` (a project's only), `offering_id`, `catalog_snapshot`, `wire_family`, `state`, `observed_price` (optional), `approved_price` (optional) |
| `axond.model-alias.v1` | a project-scoped name for an ordered list of enablements | `schema`, `alias_id`, `tenant_id`, `project_id`, `wire_family`, `state`, `targets` |
| `axond.price-book.v2` | the deployment's approved price book | `schema`, `catalog_content_id`, `catalog_version`, `currency`, `unit`, `approval`, `rules` |

Six rules hold for every body schema, present and future:

- **The identifier is inside the checksummed body.** A replica reads the schema
  before it reads anything else, so a revision cannot be interpreted under a
  schema it did not declare.
- **An unknown schema, or an unknown field, is a refusal.** A body declaring
  `axond.tenant.v2`, or an extra field a newer release added, is rejected rather
  than read partially — as `projection` when a *candidate* is compiled, and as
  `incompatible` when a *stored* revision is hydrated. An older replica keeps
  serving the revision it already had instead of serving a half-understood newer
  one. Roll the replica forward, or publish a revision the deployed version
  understands.
- **A body written before its schema was typed is the same refusal, not
  corruption.** Revisions published before `axond.tenant.v1` and
  `axond.project.v1` existed carry tenant and project bodies with no `schema`
  field. This build does not read them, and it does not guess: hydrating one is
  `incompatible`, and nothing untyped is ever accepted *as* a typed tenancy
  resource. Storage is intact, so there is nothing to repair — republish the
  affected tenants and projects from a build that writes typed bodies, and the
  fleet converges onto the new revision. Older revisions in the journal stay
  unreadable to this build by design; they remain in the journal as history. One
  exception is written down rather than inferred: an **alias** body with no
  `schema` field is *skipped* by the model rules instead of refused, because
  untyped alias rows exist in revisions already stored and refusing one would
  stop an existing revision from hydrating on upgrade. Such a row is neither
  validated nor refused — republish it from this build to have it checked. A
  model *enablement* has no such history, so an untyped enablement is
  `incompatible` like every other untyped body
  ([ADR 0042](../adr/0042-model-enablement-and-alias-contracts.md)). The
  exception is the *absence* of the field and nothing else: an alias body that
  carries a `schema` whose value is not text is read strictly and refused, so a
  damaged marker cannot skip the alias rules unreported. Reads accommodate rows
  already in the journal; the slice that gains an authoring path is where
  refusing to *write* a new untyped alias belongs.
- **A `schema` marker that is present and is not an identifier is damage, in
  every schema.** Absence of the field is a body an older release wrote, and an
  identifier this build does not know is a body a *newer* release wrote — both
  `incompatible`, storage intact. A marker that is present and is not text is
  neither: no release has ever written one, so it is reported as `corrupt`, and
  the alert names the repair ("restore the row or republish the resource rather
  than changing build"). The rule is the shared reader's, so it is the same for a
  tenant, a project, a provider, a credential, a policy, a price book, an
  enablement, and an alias — an operator is never told to roll a fleet forward
  over a row a rewrite damaged.
- **A sub-record is part of its schema.** `observed_price` and `approved_price`
  in an enablement, and each entry of an alias's `targets`, define their own field
  sets (`input_micros_per_million`/`output_micros_per_million`,
  `price_id`/`version`, `enablement_id`/`version`). A key a newer release added
  inside one is an unknown field — named by its path, as
  `approved_price.effective_from` — and is the same `incompatible` refusal an
  extra top-level field is, rather than a value read past and dropped. A value
  that is missing or wrongly typed inside a sub-record is named by its path too.
- **A body that declares a schema this build reads, and then is not one, is
  damage.** Past the identifier the field set is known, so a `v1` body missing a
  `v1` field, or carrying one whose type changed, is reported as `corrupt` and not
  as `incompatible`: nothing about a release skew can produce it, and the operator
  is pointed at storage rather than away from it. (A *display name* this build will
  not take is the exception, and is `incompatible`: validation rules can tighten
  within one schema — this build refuses an invisible byte-order mark an earlier
  one accepted.) A body that is not an inline record, or that sits under a kind it
  does not match, is damage for the same reason: every release that has written a
  tenancy body wrote an inline record, so no skew produces a scalar or a blob there.
- **A change to a field's presence or meaning is a new identifier.** `v1` bodies
  never change shape, so a checksum computed by one release is computed the same
  way by every release that accepts it. Adding a field, renaming one, or changing
  what one means all produce `axond.<kind>.v2`.

Identity in a body is bound to its envelope rather than merely carried by it: a
tenant body's `tenant_id` *is* its resource identity, a project's `tenant_id` *is*
the tenant it is scoped to, and a mismatch is refused at publication and again at
hydration. Human-readable slugs live on the envelope, so renaming a tenant leaves
every body and every reference untouched.

What a revision is *not* required to carry is a tenant row for everything scoped
to a tenant. A project must name a tenant the same revision declares — nothing can
name that project's namespace otherwise, and this build never writes a project
without its tenant, so a project whose tenant row is gone is damage rather than a
release skew. Other tenant-scoped resources are deliberately not held to it: a
credential or an alias published before tenancy bodies existed may carry a tenant
no row describes, and adding that requirement now would make revisions already in
the journal stop hydrating on upgrade. Such a scope is *unroutable*, which the
boundary that routes reports, rather than unreadable.

### Provider credentials name material without holding it

A credential body carries an **opaque, exactly-versioned reference** to secret
material — `secret_id` (`sct_…`) plus `secret_version` — and never the material,
nor a fingerprint, prefix, or length of it. Plaintext lives in the secret store,
is unwrapped only while a snapshot is compiled, and is never in the journal, a
manifest, an audit event, or a log line ([ADR
0034](../adr/0034-typed-provider-credentials-and-secret-lifecycle.md)).

Five things follow, and each is worth knowing before you rotate a key:

- **A revision pins one exact version.** Rotating stages a *new* version of the
  same secret and publishes a new version of the credential; the revision that
  pinned the old version keeps pinning it, so a rollback restores the material as
  well as the manifest. A rotation therefore does not take effect until the
  credential naming it is published and activated.
- **A rotation that must not interrupt service is authored as two credentials.**
  One credential resource names one version, so repointing the serving credential
  is the cut-over, not the overlap. Stage the new version under a *second*
  credential beside the serving one, prove it by compiling a candidate against it,
  then publish one revision that activates the new credential and withdraws
  (`disabled` or `revoked`) the old one. Activating the new version while the old
  one is still `active` is refused, and publishing only the repointed body leaves
  the revision with no active version until a further move — which is exactly why
  the overlap is two rows.
- **`lifecycle` says what may be done with the material**, and moves
  deterministically: `staged` (loaded, resolvable, so a candidate can be compiled
  against it) → `active` (in service) ⇄ `disabled` (withdrawn, reversible); any of
  those → `revoked` (never returns to service) → `tombstoned` (the stored bytes
  are destroyed). Repeating a state an entry already has is a no-op, so
  republishing the same desired state is not a conflict. Only `staged` and
  `active` resolve.
- **Ownership is exact.** The body's `tenant_id`/`project_id` *are* the
  resource's scope, and one secret has exactly one owner: a project's credential
  never resolves its tenant's material, and a credential naming another owner's
  secret is refused at publication and again at hydration. A credential
  authenticates only to a provider its owner can reach — its own scope, or its
  tenant's — so a sibling project's or another tenant's provider is refused.
- **Two credentials cannot disagree about one secret.** Two references to the same
  version must declare the same `lifecycle`, and at most one version of a secret
  is `active`, so which key authorizes a request never depends on iteration order.
  Two credentials naming the *same* active version are fine — one key, used for two
  providers, is unambiguous.

A `provider_id` naming a resource the same revision does not declare is *not*
refused, for the reason given above: such a reference is unroutable rather than
unreadable. Two classes of refusal follow from that, and they page an operator
differently:

| Refusal | Classified | What it means |
| --- | --- | --- |
| no `schema` field, an unknown `schema`, an unknown field, an unknown `lifecycle` | `incompatible` | storage is intact; run or roll forward to a build that reads the body |
| contradictory readable rows — one secret with two owners, an unreachable provider, two states for one version, two active versions | invalid, reported `Corrupt` | real repair work on stored rows |
| a price book stating an approved rate this build cannot bill exactly, an approver kind or field it does not know, a citation its name rules refuse, or a scope it does not serve | `incompatible` | storage is intact; roll forward, or approve a rate this build states |
| a price book whose rules contradict each other — two rules of one precedence covering one instant, an empty interval, a rate no `ApprovedRate` could have written | invalid, reported `Corrupt` | the body was rewritten underneath the gateway |

A credential body written before this schema existed — no `schema` field — is
therefore `incompatible` rather than corrupt, as an untyped tenant or project body
is; republish the affected credentials from a build that writes typed bodies, and
a `lifecycle` identifier this build does not know is the same refusal, so a newer
release may add a state without older replicas reporting damage. The second class
cannot be produced by publishing through the gateway — the same rules run before a
revision is stored — so a stored revision that hits one was written out of band.

### Policy documents

A policy document is the **complete** policy of one tenant or one project: what it
may spend, how much it may have in flight, and the token epoch below which a token
is refused, plus the ordered content middleware selected for its namespaces. It
is scoped to what it governs and written under that object's
identity, so "the policy of project `core`" is one durable resource whose
successive versions are successive revisions of the same document ([ADR
0036](../adr/0036-typed-policy-documents-generations-and-transitions.md)).

- **Nothing is merged, across revisions or across scopes.** Reading policy takes
  one whole document or none of it. A field absent from a newer document is not
  inherited from an older one, and a project's document does not inherit its
  tenant's field by field: what governs a scope is the document published *for* it
  if there is one, otherwise its tenant's document, selected whole. An absent
  optional field is therefore a complete statement — "this scope has no scope-wide
  cap" — rather than an omission to be filled in. A scope with no document at all
  has no published policy, and the bootstrap file's limits stand.
- **A generation is a scope and an `epoch`, plus the revision that published it.**
  The epoch is advanced by the operator when a document's content changes; the
  revision id says which publication carried it. No part identifies a generation
  alone: two publications can carry one epoch (a restored backup, a forked control
  plane), a revision id says which publication but not whether the change was
  material, and an epoch counts within its own scope rather than across scopes. A
  generation therefore also carries a digest of the document's content, which is
  what separates those two cases.
- **A document restated by a later revision is the same policy, not a fork.** A
  revision is whole desired state, so every revision republishes every policy
  document it carries: changing an unrelated resource hands out a generation with a
  new revision id for a document whose epoch and content never moved. That is the
  ordinary case and it is adoptable — a replica follows the fleet onto the revision
  now serving it, and a writer holding either publication is admitted, because both
  enforce one policy. The same epoch stating *different* content is the fork, and
  stays refused.
- **A change the epoch does not carry is refused.** Publishing different content
  under the same epoch, or moving the epoch backwards, is refused rather than
  applied — otherwise two documents would claim one generation and no replica could
  tell a stale writer from a current one.
- **Content middleware is compiled with the namespace snapshot.** Each entry
  names an in-process implementation, request/response scope, failure posture,
  and invocation bound. Unknown implementations or combinations this binary
  cannot serve reject the candidate whole; the active snapshot remains serving.
  `axond.redact` additionally requires fail-closed posture, valid rules, and a
  canonical padded-base64 32-byte key resolved from `guardrail.key_env` at
  compile time. Its compiled key is namespace-separated, so identical content
  does not produce a cross-namespace correlation token.
  A project document replaces its tenant document whole, so its chain cannot
  leak into a sibling namespace.
- **A change is classified by what activating it would require.** `live` (safe on
  the next request: looser limits, longer TTLs, a higher token floor, or a
  republication that changes nothing), `drain` (safe once what was admitted under
  the old document has finished: tighter caps, lower ceilings, shorter TTLs),
  `migration-required` (enforcement changes shape rather than its numbers: turning a
  scope-wide cap on or off changes the keys a shared ledger composes), and `refused`
  (a document for another scope, an epoch that does not carry its change, or a token
  floor lowered so that revoked tokens would work again). A publication is as
  disruptive as its worst field.
- **Stale writers fail closed.** A writer is admitted only when it holds the policy
  the fence is enforcing — the active epoch and the active content, whichever
  revision carried it. An older epoch, an epoch this replica has not adopted, the
  same epoch stating different content, and any generation of another scope all
  deny; refusing anything but the enforced policy is what makes an unknown
  generation deny instead of enforcing something nobody serves. Adoption is
  monotonic in what is enforced: onto a higher epoch of the same scope, or onto the
  active document as a later revision restates it, never onto a different document.
- **What bootstrap owns stays in `axond.toml`.** Which backend enforces a limit,
  the DSN it connects with, the table or key prefix it lays state out under, and the
  stance to take when that store is unreachable (`on_unavailable`) are not policy
  and are not publishable: a document that could flip an unavailable store to
  `allow` would turn a policy publication into a way to switch enforcement off.
  Naming one of those fields in a policy body is its own refusal, reported as damage
  rather than as a release skew, because no future schema adds them.
- **A value below a bound is a skew; a value outside the range is damage.** Bounds
  are rules rather than shape, and rules tighten within one schema identifier — the
  same exception a display name is in tenancy — so a stored counter below a minimum
  this build enforces is `incompatible` and storage is intact: run a build that
  reads it, or republish the document. A value that could never have been written —
  negative, or past what these fields count in — is `corrupt`, as is a body that
  contradicts its own identity or scope.

The stateful compiler now enforces the projected policy in the same immutable
snapshot as routing, credentials, and pricing. The control plane still owns the
document and the request path never reads it directly; a candidate is rejected
before publication if its policy or any other projection is incomplete.

### Model enablements pin the catalogue they were approved against

An enablement body names an offering by an **opaque derived identity**
(`offering_id`, `off_…`) rather than by the provider/model strings an upstream
published, and pins the **catalogue snapshot** it was approved against; the
resource depends on the blob declaring that snapshot, so a revision cannot pin a
snapshot it does not carry ([ADR
0042](../adr/0042-model-enablement-and-alias-contracts.md)). The pin must resolve
to a `CatalogModel` dependency whose body is a blob of kind `CatalogSnapshot`
with a matching digest — an unresolvable pin is an **invalid** revision, not a
compatibility skew, and a revision whose enablements have lost the catalogue they
were approved against does not converge.

Five things follow, and each is worth knowing before you approve a model:

- **A catalogue refresh does not widen an entitlement.** A refresh mints a new
  snapshot; the revision that pinned the old one keeps pinning it. Re-approve by
  publishing enablements against the new snapshot.
- **An offering keeps one spelling.** The identity is derived under a pinned
  canonical encoding, so an upstream re-spelling a model's display strings does
  not silently orphan an entitlement — and does not make it match something else.
- **An observed price is not an approved price.** `observed_price` is what a
  catalogue publishes, recorded so an operator can see it, and inert: nothing
  bills against it and nothing promotes it. Only `approved_price` — a price
  resource and an exact version of it — is billable.
- **A project row shadows its tenant's row for the same offering**, including a
  `disabled` project row, which is how one project is denied what its tenant
  allows. Withdrawal is a state, not a deletion: a disabled row is retained and
  versioned.
- **Identity, owner, offering, snapshot, and wire family never change across
  versions of one enablement.** A different offering or a different snapshot is a
  different resource.

An alias body is project-scoped, and its `targets` list is *ordered*: the order
is the preference order, which is why it lives in the body rather than in the
resource's `depends_on` set. Every target must name an enablement the same
revision declares, in the alias's own project or its tenant, and every target
must agree with the alias's `wire_family`
([ADR 0020](../adr/0020-alias-wire-family-validation.md)). A duplicate, dangling,
cross-tenant, sibling-project, or wrong-scope target is refused at publication
and again at hydration, and an alias name is unique within a project while the
same name may exist in another project or tenant. Both bodies move only between
`enabled` and `disabled`, in either direction and idempotently; a state
identifier this build does not know is `incompatible`, so a newer release may add
one without older replicas reporting damage.

There is one lifecycle compatibility exception for typed model bodies. Older
writers could leave an enabled alias pointing at a disabled enablement. The
reader tolerates that exact legacy shape during hydration, catalogue projection,
and rollback so retained revisions remain readable. `DesiredState` candidate
validation rejects it for a newly authored or modified alias, while a
one-resource repair can carry other unchanged legacy aliases from its base;
store-side publication reuses that base context. A repair publishes a
disabled/cleared alias or retargets it to an enabled target. Restack strips a
target only on an actual enabled-to-disabled transition, and when that removes
the last target the alias's disabled resource version is carried in the same
revision and semantic diff, which is the resource-level audit record for the
incidental retirement.

### How a project becomes a namespace

The runtime's tenancy boundary is the namespace: keys bind to one, credential
pools are per `(namespace, provider)`, and usage is accounted against it. A
project *is* that boundary made durable, so each published project projects to
exactly one namespace.

Its projected id is **tenant-qualified**: `acme/core`, not `core`. A project slug
is unique within its tenant and only within it — two tenants may each have a
`core` — while a namespace id is deployment-wide. Qualifying is what keeps two
tenants' projects out of one budget, one credential pool, and one key binding, and
`/` is not a legal slug character, so the qualified form decomposes exactly one
way. A projected namespace whose id a bootstrap namespace already claims is
refused rather than merged.

A qualified id is a *name*, and both halves of it are renameable, so it is not
what the namespace **is**. A projected namespace also carries the tenant and
project ids it was made from, and those never change: renaming `acme` to
`acme-inc` renames what callers say and what an operator reads in a label, and
moves nothing that was accounted. Per-namespace durable state — budgets,
credential pools, gateway-key bindings — therefore keys on that identity and not
on the name, and a rename is a rename rather than a delete plus a create. A
file-declared namespace has no such identity: its id is immutable for the same
reason the file is, and it keeps keying on the id.

Every compiled configuration's namespace ids are also held to a shape a file's
never were: one slug, or two joined by `/`, and never repeated. A file may
legitimately declare the same id twice (it means one namespace), but a *generated*
id nobody reviewed may not — a duplicate would put two tenants' budgets,
credentials, and keys on one name. The projection is wired into `serve`, so
every consumer of the qualified id — metric and trace labels, Redis and
Postgres key composition, and gateway-key bindings — must preserve that identity
and the separator rules. The compile and integration tests cover those consumers
against a separator no `axond.toml` could have produced.

What projection does *not* touch is everything the local file owns: listener,
transport bounds, admission, telemetry, and datastore connectivity. Stateful
projection now also derives durable provider connections, project namespaces,
active credentials, inbound workload principals, and catalogue-backed aliases.
The catalogue payload is hydrated from the retained store during compilation;
requests receive only concrete provider/model strings and an explicit catalogue
binding. See [Which pool a credential
authenticates](#which-pool-a-credential-authenticates). The bootstrap's default
namespace stays the default, and a projected project starts with no platform
fallback: it borrows no other namespace's credentials.

**No published project is ever made the deployment default.** A request that names
no namespace is served by whatever the file made default, and publishing a project
does not move that target — promoting one, even when it is the only one, would let
an unrelated publication silently redirect unnamed traffic. A bootstrap that
An empty stateful bootstrap receives the deterministic platform default; a file
that declares namespaces but omits a default is still refused because projection
cannot guess which file-owned namespace should receive unnamed traffic.

Project-scoped workload keys are projected from their durable digests and bind to
the qualified project namespace. Tenant-scoped workloads and human identities are
not treated as inference keys, so a revision with no recoverable project caller
principal remains fail-closed. A typed model revision also needs its exact
catalogue payload retained and an effective approved price-book entry for every
callable target. Missing catalogue, provider, principal, or pricing evidence is
a projection refusal, not an anonymous or free-serving fallback. The compiler
path is wired; the stateful integration matrix publishes a complete typed
revision and proves HTTP inference against a controlled upstream. Fleet-level
qualification remains a separate evidence gate.

A stateless deployment is unaffected by all of this. Tenants and projects are
published, never declared in `axond.toml`, and a stateless config's namespace ids
are exactly the ids the file wrote.

### Which pool a credential authenticates

A published credential is a reference; a compiled snapshot is where it becomes a
key a provider call presents. Compiling a candidate resolves every version its
credentials pin, and each **active** credential becomes one entry in the pool of
`(namespace, provider)`:

- **which namespace** — a project's credential serves that project's namespace. A
  *tenant's* credential is a default for every namespace of that tenant, and a
  project's own credential for the same provider replaces it there rather than
  being tried after it. A credential whose owner has no projected namespace (a
  tenant with no projects, a suspended one) is logged by reference and skipped: it
  is not a reason to refuse the revision.
- **which provider** — a durable provider connection is projected from desired
  state before credentials are pooled. Its runtime id is the readable slug when
  unique, or a deterministic tenant/project-qualified id when tenants reuse a
  slug. The provider's endpoint and wire family come from the same durable body;
  a credential for an absent connection, or one whose wire family disagrees,
  refuses the candidate with reason `projection` rather than publishing a
  namespace with no key or presenting a key to the wrong account.
- **staged is not serving.** Staged material resolves — that is how you prove it
  before traffic reaches it — but only `active` material is pooled. `disabled`,
  `revoked`, and `tombstoned` credentials are absent from the next snapshot's
  pools.
- **withdrawing a project's own key empties its pool; it does not fall back to
  the tenant's.** A project credential holds its `(namespace, provider)` pair from
  the moment it is more than a preparation until it is deleted, so disabling or
  revoking it makes calls
  for that provider fail with no credential rather than quietly moving that
  traffic onto the tenant's key — which would bill another account and make a
  different key the one a leak implicates. *Deleting* (`tombstoned`) the
  credential releases the pair, and the tenant's default serves it again: falling
  back is something an operator states, not something a withdrawal implies. A
  `staged` project credential holds nothing — the tenant's default keeps serving
  until the new key is activated, so preparing a key never interrupts traffic.
  Withdrawing a key *out of* staging (staged, then disabled or revoked, the way a
  key that leaked before activation is handled) does hold the pair, because
  pulling a key for cause is a reason to stop calling that provider rather than to
  move that traffic onto an account nobody nominated. Delete it to hand the pool
  back to the tenant default.
- **a pool entry names a version, never a value.** Pool status, usage records,
  logs, and metrics carry the credential's slug; the material is held as a
  reference-counted, zeroizing lease that nothing renders.

#### Rotating a key without a redeploy

Nothing below restarts a replica, and nothing below is on the request path.

1. Store the new material as a new version of the secret and publish it as a
   *second* credential resource beside the serving one, `staged`.
2. Watch `revision.active` reach that revision on every replica. A replica that
   cannot unwrap the new version refuses the candidate and keeps serving its last
   known good snapshot, with reason `secret` — fix the store, do not roll back.
3. Publish one revision that moves the new credential to `active` and the old one
   to `disabled` (reversible) or `revoked` (not). The next snapshot's pool holds
   the new key.
4. Requests already in flight — buffered or streaming — finish on the snapshot
   they started on, so they keep using the *old* key. The old material stays
   unwrapped for exactly as long as some snapshot still references it, and is
   zeroized when the last one is dropped.
5. Rolling back is publishing the previous revision: it pinned the previous
   version, so the material comes back with the manifest — unless the old version
   was `revoked`, which never returns to service, or `tombstoned`, whose bytes are
   gone. Roll back with `disabled` if you want the option; use `revoked` when the
   key is compromised.

A replica that boots cold while the secret store is unreachable **refuses to
start**, even from a valid signed last-known-good cache: a cache holds references,
not material, and starting without keys would serve nothing. A replica already
running keeps serving.

#### Stateless deployments are unchanged

A `[[credential]]` in `axond.toml` still names an `env` var and still reads its
material from the boot environment, with the same boot-time refusal when the var
is unset or empty. Projected and file-declared credentials share one pool
implementation and one leasing path, and an entry names exactly one source of
material: an env var, or a secret version. Neither ever names both.

## When a replica will not converge

The refusal reason is the triage key.

- **`unavailable`** — PostgreSQL is unreachable. The replica keeps serving,
  retries with exponential backoff, and converges by itself when the database
  returns. No action beyond fixing the database; lag will grow until it does. See
  [during an outage](#during-a-control-plane-outage).
- **`validation`** — the desired revision does not describe a servable
  configuration: an alias pointing at a provider nobody defines, no default
  namespace, a duplicate identifier. This is the same gate a file-configured boot
  applies, so the message is the same one a bad `axond.toml` produces. **Every**
  replica will refuse it. Publish a corrected revision; the fleet keeps serving
  the previous one meanwhile.
- **`secret`** or **`snapshot`** — the revision is fine but material it references
  could not be resolved: a missing environment variable, an unreadable key file,
  a secret the store does not hold. Messages name the *reference*, never the
  value. Frequently replica-specific — one replica missing an environment
  variable while its siblings converge looks exactly like this. `secret`
  specifically means a *typed* credential's exact version did not resolve through
  the secret store while the candidate was compiled: the version is withdrawn
  (`disabled`, `revoked`, or `tombstoned`), belongs to another owner, was never
  staged, is sealed under a KEK this deployment no longer has, or the store is
  down. A credential whose own body records the withdrawal is *skipped* rather
  than resolved, so the revision that withdraws material still compiles; the
  rejection above is the disagreement — a body that says `active` over a store row
  that says otherwise. Material a serving snapshot already holds is unaffected —
  the replica keeps serving it, because a candidate is compiled in full before
  anything is published
  ([ADR 0039](../adr/0039-envelope-encrypted-secret-store-and-snapshot-time-resolution.md)).
  A *booting* replica is stricter than a serving one here, and deliberately: an
  unreachable control plane falls back to the last-known-good cache, but an
  unreachable secret store fails the boot outright, because the cached revision
  needs the same material the live one does and a replica that started without it
  would serve nothing. Treat a secret store as a boot dependency of a stateful
  replica, like the control plane's database — scale-out waits on it. A boot that
  cannot prepare the store's schema refuses *permanently* only for a `SQLSTATE`
  an operator has to clear (class `42` access/undefined-object apart from the
  duplicate-object codes, `3F` invalid schema name), and its message names the
  grant or the DDL to apply. A server that
  is starting up, out of connections, deadlocked, or racing a sibling replica's
  `CREATE TABLE IF NOT EXISTS` (`23505`, `42P07`, `42710`) stays retryable, so a
  whole fleet booting at once does not turn a hiccup into a permanent refusal.
  The boot *connection* is classified the same way — a wrong role or password
  (`28*`) or an absent database (`3D*`) refuses and names the `dsn_env` string to
  fix. Reconnections during the life of a serving replica are not: the same codes
  arrive during a credential rotation the deployment is halfway through, and a
  replica already serving should wait rather than strand itself. `25006`
  (read-only transaction) is retryable on purpose, at boot and on reconnect
  alike: a `dsn_env` pointed at a hot standby says it, but so does a primary
  mid-demotion and a pooler routing to a replica during a failover, and a
  transient failover must never permanently refuse a replica.

  The misconfiguration is separated out by a boot preflight instead. Once
  connected, boot asks the server `pg_is_in_recovery()` and whether the session
  is read-only, and logs a warning naming the endpoint before any statement
  fails. If a later `25006` does arrive, that answer is attached to the
  (retryable) outage: *in recovery* means the `dsn_env` names a standby and has
  to be repointed at the primary unless a failover is under way, and *not in
  recovery but read-only* points at `default_transaction_read_only` on the role
  or the database, or at the pooler's routing. A server that accepted writes at
  the preflight and refuses them now is being demoted, so the outage carries no
  diagnostic and simply retries. A standing misconfiguration therefore repeats
  its diagnostic in every retried outage under the `secret` reason — check there
  before suspecting the store is down.
- **`projection`** — a candidate this build cannot project: a resource body it
  does not read, or a bootstrap that is missing something projection may not
  supply for it (today, a default namespace). Roll the replica forward, publish a
  revision the deployed version understands, or fix the bootstrap file.
- **`incompatible`** — a *stored* revision this build cannot read: a schema from a
  newer release, an unknown field, or a body written before that resource's schema
  was typed. Distinct from `corrupt` on purpose — storage is intact and there is
  nothing to repair, so this is an upgrade or a republication, never a database
  investigation. The replica keeps serving its last known good revision and does
  not retry into a different answer. During a rolling upgrade, expect it on
  replicas still running the older build. A body that *declares* a schema this
  build reads and then is not one is `corrupt` instead — see the rules above.
- **A price book this build cannot bill** — an approved rate finer than a
  micro-dollar, a rate for usage the gateway does not meter, a context tier, or an
  approval citation its display-name rules refuse is
  read as release skew and reported as **`incompatible`**; contradictory rules —
  two of one precedence covering one instant, an empty interval — or a negative or
  overflowing stored rate is damage and reported as **`corrupt`**. Both are
  decided when the revision is read, so triage them with the two bullets above
  and not as a pricing-specific reason. **Every** replica of this build refuses
  the revision, and the fleet keeps serving the previously published pricing
  along with its routing; approve a corrected book. See
  [ADR 0046](../adr/0046-approved-price-books.md) for why none of these are
  rounded or partially applied.
- **`pricing`** (reported as `pricing_rejected` on `/status`) — the same refusal
  reached one stage later, when the book is resolved into billable rates. Reading
  a revision already rejects every book this build cannot bill, so this reason is
  a guard against those two stages disagreeing rather than something a bad book
  produces: treat it as a bug report, and triage the book itself as above.
- **`clock`** — this replica's clock is not on the effective-dating timeline
  (before the Unix epoch, or beyond the range an instant represents), so a price
  book cannot be resolved at "now" without inventing an instant. Replica-specific
  and a host problem, reported as `clock_unsynchronised` on `/status`: fix time
  synchronisation.
- **`corrupt`** / **`not_found`** — the journal itself does not add up. Retrying
  will not clear it; see
  [when a revision will not load](./control-plane-journal.md#when-a-revision-will-not-load).

A refused candidate is never partially applied. Fetch, hydration, validation,
compilation, and secret resolution all complete before anything is published, so
a replica that refuses a revision is serving exactly what it served before —
same aliases, same credentials, same circuits.

## Retry pacing

A failing replica retries on exponential backoff: doubling from a sub-second
first retry to a ceiling of tens of seconds, then holding at the ceiling. It does
not stop retrying, and it does not tighten under load. The point is that an
extended outage costs a bounded number of attempts per replica — the alternative,
retrying at the poll interval, turns a recovering database into a thundering herd
from every replica at once.

Backoff clears on the first success.

## During a control-plane outage

A **new** replica normally compiles the durable head. For the legacy state model,
or for credential-free flat-v2 state, the encrypted compiled-serving sibling of
the signed last-known-good cache can restore the last admitted snapshot when the
control plane is unavailable. Credential-bearing flat-v2 state is deliberately
ineligible for compiled-cache persistence and cold restoration until an
authenticated monotonic revision/tombstone floor exists. The signed desired-state
file alone is insufficient because it contains references, not usable credential
material; refusing it also prevents an older authentic revision from reviving a
credential withdrawn by a newer revision.

### The signed last-known-good cache

When enabled by the durable StatefulSet/PVC deployment, every replica writes
the revision it just published to a local file, and
a replica that boots while the control plane is unreachable may restore that
file instead of failing to start. The current Recreate overlay intentionally
does not enable this path.

What to know about it operationally:

- **It is authenticated, not just serialised.** The file carries an HMAC over its
  contents, verified before a single field is interpreted. Editing the file by
  hand produces a refusal to boot, not a gateway serving hand-written desired
  state. Its signing key is deployment-wide material to provision like any other
  secret; replicas sharing it can read each other's caches.
- **A restored revision is re-verified.** Checksums, scope rules, and references
  are re-checked after the signature passes, so a cache is not a way to smuggle
  in state the journal would refuse.
- **The signed desired-state file holds no secrets.** Bodies are resource
  envelopes and credential references. The separate `.serving` sibling is an
  authenticated AES-GCM envelope containing only the already-admitted material
  needed for cold recovery; it is not readable without the cache key and is
  written only after publication. This build does not write that sibling for a
  credential-bearing flat-v2 snapshot, and refuses an authentic legacy sibling
  that contains flat-v2 credential material. Credential-free flat-v2 snapshots
  remain eligible.
- **It may be stale.** A replica reports `source = last-known-good` exactly so
  this is visible. Once the control plane returns, the replica converges to
  desired state normally and stops reporting the cache as its source.
- **A cache is a cache, not a fallback for bad state.** It is consulted for the
  two refusals where cached state is the better answer: the control plane being
  *unreachable*, and a desired revision this build cannot *read* (`incompatible`,
  the mixed-version case above). Both leave storage intact and neither is repaired
  by a replica refusing to start — a replica added mid-rollout that would not boot
  withdraws capacity exactly when a rollback needs it added. Corruption, a revision
  past this build's bounds, and a revision that exists but does not compile remain
  rejected rather than being restored from cache: booting an older cached revision
  instead would hide damage, or silently serve state an operator already replaced.
  The initial bootstrap attempt returns a typed refusal, while the post-listener
  convergence task remains alive and retries the control plane with bounded
  backoff; it never turns the invalid candidate into a cache or empty snapshot.

  A replica that boots this way reports `source = last-known-good` and keeps
  reporting `incompatible` for the revision it will not read, so the mixed-version
  state is visible rather than papered over. Roll it forward, as above.

  A rollback that reuses the volume can find a cache the *newer* build exported.
  It is authentic and intact, and this build still cannot read it, so the boot
  refusal names the version skew rather than the cache file: the action is to roll
  the replica forward or repave the volume, not to hunt a disk fault. A cache that
  fails its signature, or holds rows that do not add up, is still reported as the
  cache's own failure. The compiled-serving envelope increments its layout byte
  whenever its payload gains a field, and policy payloads reject unknown keys, so
  a downgrade cannot silently discard a guardrail registration. Guardrail key
  material is not cached; a non-secret namespace-key fingerprint binds the
  encrypted record to the value that compiled it, and a changed value behind the
  same `key_env` reference refuses recovery instead of changing placeholder
  identity under the cached revision.

  The forward edge is equally deliberate for the v0.4.0 guardrail layout: a new
  binary does not cold-restore a pre-v4 compiled-serving record. Keep the control
  plane and SecretStore reachable while each upgraded ordinal starts, compiles
  the admitted revision, and atomically writes its own v4 record; verify that
  publication before advancing the rollout. Do not replace the whole fleet
  during a control-plane outage and assume pre-upgrade PVC caches are a recovery
  path. After every replica has written v4, the ordinary outage/cold-start drill
  applies again.
- **An unwritable cache is a warning, not an outage.** A replica whose disk is
  full keeps serving and logs once; what it loses is the ability to cold-boot
  during an outage.

Without a cache — or with one that fails its signature — a replica that cannot
reach the control plane, or cannot read the revision it finds, refuses to become
ready. It never serves an empty or partial configuration while reporting itself
healthy.

## Related

- [Control-plane revision journal](./control-plane-journal.md) — what is stored,
  and why a revision will not load.
- [ADR 0027](../adr/0027-stateless-and-stateful-operating-modes.md) — the mode,
  its authority rules, and the request path's independence from the control
  plane.
- [ADR 0011](../adr/0011-config-hot-reload.md) — the immutable-snapshot
  publication this converges through, and the one-snapshot-per-request rule it
  inherits.
- [Observability and runbook](../observability.md) — traces, metrics, and boot
  failures.
