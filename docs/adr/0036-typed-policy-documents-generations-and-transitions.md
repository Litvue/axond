# 36. Typed policy documents, generations, and transition classification

Date: 2026-08-13

## Status

Accepted

Types the limits half of the stateful mode chosen in
[ADR 0027](./0027-stateless-and-stateful-operating-modes.md), on the tenant and
project scopes the desired-state tenancy bodies already define, and alongside
[ADR 0034](./0034-typed-provider-credentials-and-secret-lifecycle.md), which
types the credential half.

## Context

Stateful mode lets an operator publish what a tenant or a project may spend and
hold: a budget cap, a concurrency ceiling, a reservation and lease TTL, and a
token floor. Until now those values existed only in `axond.toml`, and the
published resource that was meant to carry them had an untyped body. Four
questions had no answer in the domain:

- **What does one publication mean?** With a bag of settings, a newer revision
  that omits a field could plausibly mean "leave the old value alone" or "there
  is no such limit". The two readings differ in what a request is allowed to do,
  and nothing in the body said which one applied.
- **Does a project inherit its tenant's limits field by field?** Per-field
  inheritance makes the effective policy of a scope a document no operator ever
  wrote, and therefore one no operator can read back and check.
- **Which publication is a replica enforcing?** A cap that is not enforced
  identically across a fleet is not a cap. Replicas need to tell a writer holding
  the policy they serve from one holding something else — including two
  publications that claim the same version.
- **Is a limit change safe to apply mid-flight?** Raising a cap and lowering one
  are not the same operation, and turning a scope-wide cap on changes the keys a
  shared ledger composes rather than just a number in it.

None of these can be answered on the request path: the request path is where the
answers are *used*. They have to hold at publication and again when a replica
hydrates a stored revision, or the two boundaries disagree about the same rows.

## Decision

A tenant's or project's policy is the typed, versioned record
`axond.policy.v1` — one **complete document** per scope:

```
schema, tenant_id, project_id?, epoch,
budget_limit_microdollars, namespace_budget_limit_microdollars?,
reservation_ttl_seconds, max_in_flight_per_subject, lease_ttl_seconds,
minimum_token_epoch
```

- **A document is complete, and nothing is merged.** Reading policy takes one
  whole document or none of it — never fields from two revisions, and never a
  project's field beside its tenant's. What governs a scope is the document
  published *for* it if there is one, otherwise its tenant's document, selected
  whole. An absent optional field is therefore a complete statement ("this scope
  has no scope-wide cap") rather than an omission to fill in, and a scope with no
  document has no published policy at all, leaving the bootstrap file's limits.
- **A generation is a scope, an epoch, the revision that carried it, and the
  content it states.** The operator advances `epoch` when content changes; the
  revision id is provenance. No part identifies a publication alone: two
  publications can carry one epoch (a restored backup, a forked control plane), a
  revision id says which publication but not whether the change was material, and
  an epoch counts within its own scope, so a higher one from another tenant orders
  nothing. Because a revision is whole desired state, every revision restates every
  policy document it carries, so a generation also carries a digest of the
  document's content — which is what separates the ordinary carry-forward from a
  fork.
- **Stale writers fail closed, and carry-forward is not staleness.** A writer is
  admitted only when it holds the policy the fence is enforcing: the active epoch
  and the active content, whichever revision carried it. An older epoch, an epoch
  this replica has not adopted, the same epoch stating *different* content, and any
  generation of another scope all deny — the same posture as an unreachable budget store
  (`on_unavailable = "deny"`): an unenforceable cap must not silently admit.
  Adoption is monotonic in what is enforced: onto a higher epoch of the same scope,
  or onto the active document as a later revision restates it, never onto a
  different document. A fence that could be walked onto another scope's document
  would then deny every writer the scope actually has.
- **A change is classified by what activating it would require.** `live` (looser
  limits, longer TTLs, a higher token floor, or a republication that changes
  nothing), `drain` (tighter caps, shorter TTLs — safe once what was admitted
  under the old document has finished), `migration-required` (turning a scope-wide
  cap on or off, which changes the keys a shared ledger composes), and `refused`
  (a document for another scope, changed content that does not advance the epoch,
  a regressed epoch, or a token floor lowered so revoked tokens would work again).
  A publication is as disruptive as its worst field.
- **What bootstrap owns may never be published.** Which backend enforces a limit,
  the DSN it connects with, the table or key prefix it lays state out under, and
  the stance when that store is unreachable are not policy: a document that could
  flip `on_unavailable` to `allow` would turn a policy publication into a way to
  switch enforcement off. Those field names are a typed refusal in the reader
  rather than an unknown field, and reported as damage rather than as a release
  skew, because no future schema adds them.
- **Compatibility follows the rule tenancy states.** A schema identifier this
  build does not read, a field a newer release added, and a body with no
  identifier at all are release skew, and storage is intact. A refusal *inside* a
  body that declared `axond.policy.v1` points at storage — with the one exception
  tenancy already makes for a display name: a *bound* is a rule rather than a
  shape, and rules tighten within one identifier, so a stored counter below a
  minimum this build enforces is skew. A value that could never have been written
  is damage.

Nothing is activated. No request path reads a document, no store writes one, and
no snapshot is compiled from one: the transition classification states what
activating a change would require of a fleet rather than performing it.

### State tier

Tier 0. This slice is domain types, publication-time validation, and
classification: it selects no backend, opens no connection, and runs nothing at
boot. A stateless deployment publishes no revisions and therefore reads no policy
bodies, and its limits keep coming from `axond.toml` through the existing budget
and concurrency configuration. The durable stores these documents will one day
drive stay where [ADR 0027](./0027-stateless-and-stateful-operating-modes.md) put
them (Tier 1 Redis, Tier 2 Postgres), and no existing deployment's tier is
raised.

## Consequences

**An effective policy is always a document somebody wrote.** An operator can read
back exactly what governs a scope, and a reviewer can diff two publications. The
cost is that publishing a single field means republishing the document that
contains it, and that a project wanting its tenant's limits plus one change must
state all of them.

**A material change costs an epoch.** Changed content that does not advance the
epoch is refused rather than applied, so an operator cannot edit limits in place
without saying that they changed. In exchange, replicas have an order they can
fence on that does not depend on comparing revision ids.

**Fencing survives unrelated publications.** Because a generation carries a
content digest, a revision that changes some other resource does not fence out
every writer holding the previous publication of an unchanged policy, and a
replica can follow the fleet onto the new revision. Two publications that state
different policies under one epoch still deny in both directions.

**Tightening a bound is a rolling upgrade, not a repair.** Classifying a
below-minimum value as a compatibility refusal means a replica that cannot read a
stored document says storage is intact and keeps serving what it holds. The risk
this accepts is that a genuinely rewritten counter reads as skew; the identity,
scope, and shape checks that no release skew can produce stay damage.

**Bootstrap keeps the failure stance.** Because `on_unavailable`, the backend, and
the storage layout are unpublishable, no revision can turn a fail-closed limit
into a fail-open one, and reviewing that stance stays a review of the file it
lives in.

**Security review outcome.** Trigger 2 (tenant and namespace scoping) fires: the
document carries budget and concurrency limits and a token floor, keyed by tenant
and project. It is held by identity binding (a body's `tenant_id`/`project_id`
*is* its resource identity and must match its envelope's scope), by
whole-document selection with no cross-scope inheritance, and by the fail-closed
fence — with
`a_body_is_bound_to_the_envelope_that_carries_it`,
`a_published_document_may_not_name_a_field_bootstrap_owns`,
`an_effective_policy_is_one_whole_document_never_a_merge_of_two`,
`a_writer_of_any_generation_but_the_active_one_fails_closed`, and
`an_unchanged_document_carried_into_a_later_revision_stays_the_same_policy`
holding those properties. Trigger 1 is *not* fired: `minimum_token_epoch` is a
published number that no code reads yet, and revocation enforcement is untouched.
Trigger 3 is not fired: the body has no field material could travel in. Trigger 5
is not fired: no migration, table, or telemetry change. No `expose_secret` call
site is added, and no request path changes.

## Alternatives considered

- **Mergeable settings with per-field inheritance.** Convenient to author, and it
  makes the effective policy of a scope a document nobody wrote: an omission and
  a deliberate "no cap" become indistinguishable, and a rollback restores a
  combination that was never reviewed.
- **A content checksum instead of an epoch.** A checksum tells you two documents
  differ but not which one an operator published later, so a fence built on it
  cannot order two publications — which is exactly what a fence needs.
- **A revision id alone as the generation.** It says which publication carried a
  document, but not whether the change was material, and ordering revision ids
  makes a storage detail into a policy decision.
- **Fencing on order rather than on the enforced policy.** Admitting anything
  "not older" lets a writer enforce whatever it can claim is newer, which is not
  fencing. Refusing anything but the policy this replica serves is what makes an
  unknown generation deny.
- **Publishing the backend and the unavailable stance.** It would make a
  deployment configurable from one place, and it would also make "switch
  enforcement off" a publishable change, reviewed as a limits edit.
