# Revision convergence

In stateful mode, a change is *published* to the control plane and then
*converges* onto every replica. This page is the operator's view of that second
half: how fast it is, what a replica reports about itself, what happens when it
cannot converge, and how a replica boots while PostgreSQL is unavailable.

Stateful mode is still being assembled: this is the convergence layer
([#142](https://github.com/Litvue/axond/issues/142)), built on the revision
journal ([#165](https://github.com/Litvue/axond/issues/165)). The loop, its
telemetry, and the signed cache are complete and tested, but `serve` does not yet
construct them. Tenants and projects have durable schemas and a projection
([#191](https://github.com/Litvue/axond/issues/191)); the remaining resource
*bodies* (providers, credentials, catalogue models, prices, policies) belong to
the slices that own those schemas. Read
[ADR 0027](../adr/0027-stateless-and-stateful-operating-modes.md) for the mode as
a whole.

## How a change reaches a replica

1. An administrator publishes a revision. The journal advances one head pointer.
2. Every replica polls the head, independently. A PostgreSQL notification, when
   one arrives, only makes a replica poll sooner.
3. A replica that sees a head it is not serving hydrates the **whole** revision,
   projects it onto its configuration, runs the same whole-graph validation boot
   runs, and resolves every secret the result needs.
4. If all of that succeeds, the replica swaps in the new snapshot atomically and
   the next request is served from it.
5. If any of it fails, the replica keeps serving what it already had and reports
   why.

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
| Control plane unreachable | Not until it returns; the previous revision keeps serving |
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
  or `snapshot`.
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
| `axond.revision.attempts` (by `trigger`, `outcome`) | Whether convergence is being driven by notifications or by polls |
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
scope, and slug. Six schemas exist today:

| Schema | Resource | Fields |
| --- | --- | --- |
| `axond.tenant.v1` | a deployment tenant | `schema`, `tenant_id`, `display_name` |
| `axond.project.v1` | a tenant-owned project | `schema`, `project_id`, `tenant_id`, `display_name` |
| `axond.provider-credential.v1` | a tenant's or project's credential for one provider | `schema`, `credential_id`, `tenant_id`, `project_id` (a project's only), `provider_id`, `display_name`, `secret_id`, `secret_version`, `lifecycle` |
| `axond.policy.v1` | the policy of a tenant or a project | `schema`, `tenant_id`, `project_id` (project documents only), `epoch`, `budget_limit_microdollars`, `namespace_budget_limit_microdollars` (optional), `reservation_ttl_seconds`, `max_in_flight_per_subject`, `lease_ttl_seconds`, `minimum_token_epoch` |
| `axond.model-enablement.v1` | a tenant's or project's permission to use one catalogue offering | `schema`, `enablement_id`, `tenant_id`, `project_id` (a project's only), `offering_id`, `catalog_snapshot`, `wire_family`, `state`, `observed_price` (optional), `approved_price` (optional) |
| `axond.model-alias.v1` | a project-scoped name for an ordered list of enablements | `schema`, `alias_id`, `tenant_id`, `project_id`, `wire_family`, `state`, `targets` |

Five rules hold for every body schema, present and future:

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
  ([ADR 0038](../adr/0038-model-enablement-and-alias-contracts.md)).
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
is refused. It is scoped to what it governs and written under that object's
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

Nothing enforces a document yet: no request path reads one, and no store writes
one. This is the contract a later activation slice binds to, and the
classification above states what activating a change would require of a fleet
rather than performing it.

### Model enablements pin the catalogue they were approved against

An enablement body names an offering by an **opaque derived identity**
(`offering_id`, `off_…`) rather than by the provider/model strings an upstream
published, and pins the **catalogue snapshot** it was approved against; the
resource depends on the blob declaring that snapshot, so a revision cannot pin a
snapshot it does not carry ([ADR
0038](../adr/0038-model-enablement-and-alias-contracts.md)). The pin must resolve
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
credentials, and keys on one name. What that gate does not cover is `/` where an
id is *used* rather than declared: before the runtime slice wires this projection
into `serve`, metric and trace label values, Redis and Postgres key composition,
and gateway-key bindings all have to be checked against a separator no
`axond.toml` could have produced.

What projection does *not* touch is everything the local file owns: listener,
transport bounds, admission, telemetry, datastore connectivity, and — until their
own slices land — providers, credentials, models, and prices. The bootstrap's
default namespace stays the default, and a projected project starts with no
platform fallback: it borrows no other namespace's credentials.

**No published project is ever made the deployment default.** A request that names
no namespace is served by whatever the file made default, and publishing a project
does not move that target — promoting one, even when it is the only one, would let
an unrelated publication silently redirect unnamed traffic. A bootstrap that
declares no default namespace is therefore refused with reason `projection`, and
the message says so. Since `[[namespace]]` is a control-plane-owned section that a
stateful file may not declare, that is the shape a stateful bootstrap has today:
**stateful serving stays gated until the runtime slice that selects a default from
desired state lands.** Nothing in `serve` constructs this projection yet, so the
refusal is a design boundary rather than an outage.

A stateless deployment is unaffected by all of this. Tenants and projects are
published, never declared in `axond.toml`, and a stateless config's namespace ids
are exactly the ids the file wrote.

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

A **running** replica is unaffected in the only way that matters: it keeps
serving inference from its active snapshot. It cannot learn about new revisions,
so its lag grows and its rejection reason reads `unavailable`, and it converges
without intervention once PostgreSQL returns.

A **new** replica has nothing to serve, and this is where the signed
last-known-good cache matters.

### The signed last-known-good cache

Every replica writes the revision it just published to a local file, and a
replica that boots while the control plane is unreachable may restore that file
instead of failing to start. This is what keeps a database incident from also
freezing fleet size — otherwise an outage during a traffic spike means no
scale-out and no replacement of failed replicas.

What to know about it operationally:

- **It is authenticated, not just serialised.** The file carries an HMAC over its
  contents, verified before a single field is interpreted. Editing the file by
  hand produces a refusal to boot, not a gateway serving hand-written desired
  state. Its signing key is deployment-wide material to provision like any other
  secret; replicas sharing it can read each other's caches.
- **A restored revision is re-verified.** Checksums, scope rules, and references
  are re-checked after the signature passes, so a cache is not a way to smuggle
  in state the journal would refuse.
- **It holds no secrets.** Bodies are resource envelopes; every credential is a
  *reference* resolved through the secret store at compile time.
- **It may be stale.** A replica reports `source = last-known-good` exactly so
  this is visible. Once the control plane returns, the replica converges to
  desired state normally and stops reporting the cache as its source.
- **A cache is a cache, not a fallback for bad state.** It is consulted for the
  two refusals where cached state is the better answer: the control plane being
  *unreachable*, and a desired revision this build cannot *read* (`incompatible`,
  the mixed-version case above). Both leave storage intact and neither is repaired
  by a replica refusing to start — a replica added mid-rollout that would not boot
  withdraws capacity exactly when a rollback needs it added. Corruption, a revision
  past this build's bounds, and a revision that exists but does not compile are all
  fatal at boot: booting an older cached revision instead would hide damage, or
  silently serve state an operator already replaced.

  A replica that boots this way reports `source = last-known-good` and keeps
  reporting `incompatible` for the revision it will not read, so the mixed-version
  state is visible rather than papered over. Roll it forward, as above.

  A rollback that reuses the volume can find a cache the *newer* build exported.
  It is authentic and intact, and this build still cannot read it, so the boot
  refusal names the version skew rather than the cache file: the action is to roll
  the replica forward or repave the volume, not to hunt a disk fault. A cache that
  fails its signature, or holds rows that do not add up, is still reported as the
  cache's own failure.
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
