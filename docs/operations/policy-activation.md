# Policy activation

In stateful mode, a tenant's or project's limits — spend cap, reservation TTL,
concurrency ceiling, lease TTL, token floor — are published as a
[policy document](../adr/0036-typed-policy-documents-generations-and-transitions.md)
and take effect on every replica without a redeploy. This page is the operator's
view of the second half of that: what a replica does with a published document,
which changes it will activate, what happens to requests already in flight, how
to roll back, and what to check before publishing.

Policy activation rides the [revision convergence](./revision-convergence.md)
loop, so everything that page says about polling, mixed fleets, and outages
applies here unchanged. The design is
[ADR 0041](../adr/0041-runtime-policy-activation.md).

> Stateless deployments are unaffected in every respect. With no control plane
> there are no revisions and no documents: `axond.toml` is the policy, forever.
> Nothing on this page applies, and nothing about boot or the request path
> changes.

## Before you publish: preflight

Run through this list once per deployment, and again whenever the bootstrap file
changes. A document that fails any of it is **refused at activation**, before
anything is served — but the refusal is nicer to read before a rollout than
during one.

| Check | Why | How |
| --- | --- | --- |
| `[budget] backend` is `redis` or `postgres` | A published spend cap is a fleet-wide statement; `in-memory` and `none` enforce a separate copy per replica, or nothing | `axond preflight` / read the bootstrap file |
| `[rate_limit] backend` is `redis` | Every document states a concurrency ceiling — `axond.policy.v1` has no spend-only form — and that needs leases every replica shares | as above |
| `[budget] namespace_scope` matches the document | Whether ledger keys carry a scope-wide cap is a **durable layout** fact, not a value | see [Migrating the layout](#migrating-the-layout) |
| Every cap the document states is at least `1` | A cap of zero denies every request for the scope, which is not a bound on spending but a closed scope wearing one — the bootstrap file refuses it for the same reason, so a published document may not express what an authored one may not. Close a scope through tenancy (withdraw the projection, revoke its credentials), not through a limit | the revision is refused with `validation`, naming `budget_limit_microdollars` or `namespace_budget_limit_microdollars` |
| Every projected namespace is governed | A namespace no document governs has **no enforceable cap**, and its requests are denied | publish a tenant-level document as the floor |
| The budget backend's DSN is **not** the control plane's | The revision journal is not request-path state | compare `[budget] dsn_env` with `[control_plane] dsn_env` |
| Every replica runs a build that reads `axond.policy.v1` | A build that cannot read the body refuses the whole revision as skew | roll the binary out first |

### The control plane is not a hot path

The control plane's PostgreSQL holds the revision journal. The budget store's
PostgreSQL backend is a **separate, explicitly configured** DSN with its own
ledger tables and its own migrations. Nothing derives one from the other, and
enabling stateful mode never turns the journal into request-path state. Pointing
both at one instance is a deliberate capacity decision about one database — make
it explicitly, or not at all.

Which datastore to select for what:

| Responsibility | Redis | PostgreSQL | In-memory / none |
| --- | --- | --- | --- |
| Spend caps (budget ledger) | yes | yes | not for a published cap |
| Concurrency ceilings (leases) | yes | no | not for a published ceiling |
| Revocation | yes | — | per-replica only |
| Revision journal (control plane) | no | yes (its own instance) | no |

## What a replica does with a published document

1. The revision is hydrated and projected. Each namespace a project projects to
   is governed by the document published for that project, or — whole, never
   field by field — its tenant's.
2. Before anything is published, the replica **plans** the activation: can these
   backends enforce it, does it fit the layout this process booted on, and is the
   move from what is active a move this model performs.
3. If the plan is refused, the candidate is dropped whole. The replica keeps the
   configuration *and* the policy it already had, and reports the refusal like
   any other rejected revision.
4. If it is accepted, the new view is installed with one atomic store. The next
   admission reads the new limits; nothing already admitted is disturbed.

Values are read **per admission** from the active view. A publication never
rebuilds a store, reconnects a pool, or changes a DSN, key prefix, table, or the
`on_unavailable` stance — those stay bootstrap-owned and unpublishable. The caps
a request is checked against and the generation stamped on its hold come from a
single read of that view, so a publication landing mid-admission can never grant
one document's limits under another's name.

### A revision that would leave a namespace ungoverned is refused

In a stateful deployment, a namespace with no document has **no enforceable
cap**, and a request for it would be denied — a `503` — *regardless* of
`on_unavailable`. That stance is deliberate:

- `on_unavailable = "allow"` answers the question "the store that holds the
  limits is unreachable — admit anyway?". There the store is fine; what is
  missing is the limit itself.
- An unenforced cap and an infinite one are indistinguishable to a caller, and
  only one of them is what an operator published. Admitting would spend real
  money against a cap nobody set.

But a replica does not *get* there by activating a revision. A candidate that
would serve a namespace no document governs is refused before publication —
reason `ungoverned`, the namespace named — so projecting a namespace before its
tenant's floor exists costs you the revision, not the namespace's traffic; the
last known good policy keeps serving. Publish a tenant-level document as the
floor **before** projecting namespaces to it; the preflight table's "every
projected namespace is governed" row is that check.

Every such denial is counted on `axond.policy.unenforceable_denials`
(`axond.policy.condition = ungoverned`, or `layout` when the published cap and
the store's key layout disagree), once per request; the log line explaining it
is sampled — once per condition, backend and namespace, then
at most once a minute for as long as it lasts — so a namespace that stays
ungoverned under load produces an operator signal rather than a log flood.

A stateful bootstrap cannot declare a namespace at all (`[[namespace]]` is a
boot error under `mode = "stateful"`), so there is no file-governed namespace to
exempt: in stateful mode every namespace is governed by a published document, or
the revision that introduced it is refused. That is also why `[budget]
namespace_scope = true` — the layout whose *value* the file may not state —
cannot strand a namespace against half-described caps.

## Classification: live, drain, migration, refused

| Class | What it covers | What happens |
| --- | --- | --- |
| `live` | Looser caps, longer TTLs, a higher token floor, a republication that changes nothing | Activates. New admissions use the new values |
| `drain` | Tighter caps, shorter TTLs | Activates. New admissions use the new values; **what is already admitted keeps its own terms** until it finishes |
| `migration-required` | Turning a scope-wide cap on or off | Refused. See [Migrating the layout](#migrating-the-layout) |
| `refused` | A regressed epoch, changed content under an unchanged epoch, another scope's document, a lowered token floor (**including across a handover**), a document these backends cannot enforce, withdrawing a document from a namespace still served, or serving a namespace no document governs | Refused. Nothing changes |

A publication is as disruptive as its worst field.

### Handing a namespace between scopes

A project publishing its own document over its tenant's — or deleting it so the
tenant's applies again — is a **handover**, not a withdrawal: the namespace never
stops being governed, so it is not refused. It is classified against the document
it displaces rather than against the (absent) history of the scope that is new,
so a project capping itself below its tenant is reported as a `drain` and its
tenant's outstanding holds keep their terms. Both directions are classified this
way, and both are reported: dropping a project's document for a tighter tenant
one drains too, so `draining` is a complete picture before a stop-the-fleet
migration.

Only the epochs are not compared across a handover: an epoch orders one scope's
own publications, and a project's first is not behind its tenant's tenth. The
*values* are compared in full, so the refusals still hold — in particular a
handover cannot lower a namespace's `minimum_token_epoch`, which would restore
tokens an operator revoked by changing nothing but which document states the
floor.

### Why a drain is safe

Every reservation and every shared lease is stamped with the **generation** that
admitted it, and the replica counts outstanding holds per generation. A tightened
document therefore cannot shorten a lease someone is holding or reprice a
settlement in flight: those finish on the terms they were granted, and only then
does the superseded generation's count reach zero.

A drain is bounded by *work*, not by a timer. A long-lived stream keeps its
generation alive, and during a drain two generations are enforced at once — by
design.

The count is taken *before* the store call that admits, and released if that
call denies, so it can briefly name a request that is about to be turned away
but never misses one that was admitted. Zero therefore means what a migration
needs it to mean: nothing is running under the superseded document.

## Rolling back

**Roll back by publishing the old values forward: a new document, a higher epoch,
yesterday's numbers.** It is classified like any other change (usually a drain,
since it lowers something), and the holds taken under the higher caps finish on
the terms they were granted.

**Do not roll the epoch backwards, and do not repoint the fleet at an older
revision to undo a limits change.** Both are refused, and for the same reason:
one epoch would name two different documents across a partly rolled fleet, and
the fence could no longer tell a stale writer from a current one. The refusal
names the generation it is enforcing.

## Mixed-version and forked writers

During a rollout a fleet is briefly mixed, which is normal: each replica converges
independently and each request keeps the snapshot and generation it started with.

What is *not* normal is two publications claiming one epoch with different
content — a restored backup, or a second control plane. That is a fork, and it
fails closed in both directions: neither replica adopts the other's document, and
the refusal names it as a fork rather than as staleness. To recover, decide which
control plane is authoritative, stop the other, and publish the intended document
under an epoch higher than either.

## Migrating the layout

Whether the budget store's keys carry a scope-wide (namespace-wide) cap is a
durable property of the ledgers, so turning it on or off is a migration and not a
publication:

1. Publish a document *without* the change and let the fleet converge onto it.
2. Stop the fleet.
3. Migrate the budget ledgers: `axond budget migrate-redis --config PATH` for
   Redis, or apply `ops/postgres/budget_v2.sql` for Postgres. (Not `axond
   migrate apply` — that is the control-plane schema, not these ledgers.) The
   command wants the bootstrap the fleet will *start* on, so set
   `[budget] namespace_scope = true` before running it.
4. Restart on that bootstrap.
5. Publish the document that states (or omits) `namespace_budget_limit_microdollars`.

Until step 5, the replica refuses the document and keeps enforcing what it has,
which is the intended behaviour: half-enforcing a scope-wide cap against keys
laid out without one would leave ledgers accumulating against nothing.

## Revocation floors

A document's `minimum_token_epoch` is a floor, and it only ever **raises** the
token epochs a replica enforces — both the namespace-wide entry and any more
specific per-subject entries in the bootstrap file. A stale file entry therefore
cannot undercut a published revocation. Lowering a floor is refused, because it
would make already-revoked tokens work again; issue new credentials instead.

## When things go wrong

| Symptom | Meaning | Action |
| --- | --- | --- |
| Revisions rejected with `unsupported` | The bootstrap backends cannot enforce a fleet-wide cap | Select `redis`/`postgres` for budget and `redis` for rate limiting, then restart |
| Revisions rejected with `migration` | The document and the ledger layout disagree | [Migrate the layout](#migrating-the-layout) |
| Revisions rejected with `refused` | An epoch regression, a fork, or an unsafe field change | Read the refusal; publish forward under a higher epoch |
| Revisions rejected with `withdrawn` | A document was removed from a namespace still being served | Delete the namespace, or publish a document for it |
| Revisions rejected with `ungoverned` | The revision projects a namespace no document governs | Publish a tenant-level document as the floor, then project the namespace |
| `axond.policy.unenforceable_denials{condition="ungoverned"}` climbing | No document governs that namespace — only reachable from a bootstrap gap, not from a publication | Publish a tenant-level document |
| `axond.policy.unenforceable_denials{condition="layout"}` climbing | The published cap and the key layout this process booted on disagree | [Migrate the layout](#migrating-the-layout) |
| A tightening never finishes draining | Long-lived requests still hold the old generation | Wait, or drain the replica |
| Control plane unreachable | Last-known-good policy keeps being enforced | Nothing; convergence resumes on its own |
