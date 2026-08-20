# 42. Snapshot-pinned model enablements and ordered project aliases

Date: 2026-08-13

## Status

Accepted, partially superseded by
[ADR 0062](./0062-blob-backed-flat-namespace-control-plane.md).

Pinned offering identity, ordered aliases, wire-family validation, and atomic
snapshot activation remain in force. Tenant defaults and project overrides are
replaced by one complete namespace-owned model and alias view.

Types the entitlement half of the stateful mode chosen in
[ADR 0027](./0027-stateless-and-stateful-operating-modes.md), inside the tenancy
bodies of
[revision convergence](../operations/revision-convergence.md#resource-body-schemas),
and keeps the wire-family rule of
[ADR 0020](./0020-alias-wire-family-validation.md) as the constraint an alias's
targets are held to.

## Context

Stateful mode gives a deployment a durable answer to *which models may a tenant
use, and under which names*. Two resources record it — a model enablement and an
alias — and until now both bodies were untyped records. Four questions had no
answer in the domain:

- **What is a tenant enabled for?** A catalogue offering was named by whatever
  provider/model strings an upstream published at import time. Those strings are
  an upstream's editorial choice: a refresh renames or re-spells them, and an
  entitlement written against the old spelling silently stops matching, or worse,
  starts matching something else.
- **Which catalogue is that?** Catalogue contents change without human action.
  An enablement that names an offering but not the catalogue it was approved
  against means "whatever the catalogue says now", so an upstream edit changes
  what a published revision permits.
- **What does a price in a catalogue mean?** An upstream publishes rates. An
  operator approves rates. Carrying both in one field makes an upstream edit a
  billing change.
- **Which target does an alias mean first?** An alias names more than one
  enablement, and failover order is the whole point of naming more than one. The
  resource envelope's `depends_on` is a set, so order cannot live there.

None of this can be pushed to the request path: a request resolves an alias, it
does not decide who may use what. The answers have to hold at publication and
again when a replica hydrates a stored revision, or the two boundaries disagree
about the same rows.

## Decision

An enablement's body is the typed record `axond.model-enablement.v1` and an
alias's is `axond.model-alias.v1`. Both bind their declared identity and owner to
the envelope that carries them, the way tenancy and credential bodies do.

```
axond.model-enablement.v1: schema, enablement_id, tenant_id, project_id?,
                           offering_id, catalog_snapshot, wire_family, state,
                           observed_price?, approved_price?
axond.model-alias.v1:      schema, alias_id, tenant_id, project_id,
                           wire_family, state, targets[]
```

- **An offering identity is opaque, derived, and stable.** `OfferingId`
  (`off_…`) is the digest of the canonical provider/model identity under a
  *pinned* serializer version, not under whatever encoding a future build
  defaults to. So the same offering keeps one spelling across catalogue
  refreshes, an upstream's display changes do not re-spell an entitlement, and
  nothing about the identity discloses or depends on catalogue internals this
  slice does not own.
- **An enablement pins the catalogue it was approved against.** The body carries
  a `catalog_snapshot` checksum, and the resource depends on the blob that
  declares it. A refresh mints a *new* snapshot; the revision that pinned the old
  one keeps pinning it, so a rollback restores the catalogue facts along with the
  manifest, and an entitlement never widens because an upstream published more.
  The pin resolves *structurally*: the dependency must be a `CatalogModel` whose
  body is a blob of kind `CatalogSnapshot` with a matching digest. This is a
  requirement on any future storage path — hydration must reconstruct a catalogue
  resource with its blob kind and digest intact rather than rematerializing the
  body inline, because an enablement whose pin no longer resolves is refused as
  invalid, not tolerated as skew. A revision whose enablements have lost the
  catalogue they were approved against must not converge.
- **An observed price is not an approved price.** `observed_price` records what a
  catalogue publishes and is inert: nothing bills against it and no conversion
  promotes it. Only `approved_price` — an exactly-versioned reference to a price
  resource — is billable, so an upstream edit cannot change what a deployment
  charges.
- **A tenant default is a row; a project override is another row.** An
  enablement is owned by a tenant or by one of its projects, matching its
  envelope scope, and a project row shadows its tenant's for the same offering —
  including a *disabled* project row, which is how a project is denied something
  its tenant allows. Withdrawal is a state, not a deletion: a disabled row is
  retained and versioned, so history stays answerable.
- **An alias is project-scoped, ordered, and one wire family.** `targets` is a
  list, and the list order *is* the preference order. Every target names an
  enablement in the alias's own project or its tenant, and every target agrees
  with the alias's declared `wire_family` — the ADR 0020 rule, enforced where the
  contract is authored rather than where a request is served. A duplicate,
  dangling, cross-tenant, sibling-project, or wrong-scope target is refused, and
  an active alias must retain a non-empty, reachable target list and all targets
  must share its wire family. A newly authored enabled alias may not target a
  disabled enablement; a disabled alias may retain historical targets or clear
  them in the same revision that withdraws the name. The retained row is
  lifecycle history, not an active graph. An alias name is unique within a
  project while the same name may exist in another project or tenant.
- **Lifecycle is enabled ↔ disabled, and identity is immutable.** Both
  transitions are permitted and idempotent; a transition to a state a body does
  not know is a typed refusal. Across versions of one resource, identity, owner,
  wire family, and — for an enablement — offering and pinned snapshot never
  change: a new offering or a new snapshot is a new resource, not an edit.
- **A sub-record is held to its schema too.** `observed_price`,
  `approved_price`, and each alias target define their own field sets, so a key a
  newer release added inside one is an unknown field rather than a value read past
  and dropped. Every refusal inside a sub-record names its path —
  `approved_price.effective_from`, `targets.weight` — so an operator is told which
  value to fix and which of two `version` fields is meant.

### State tier

Tier 0 (config-only). This slice adds no store, no route, and no boot step: the
bodies are read at publication and at hydration by code that already runs, and
Tier 1 and Tier 2 deployments read the same rules. Nothing here raises the tier
of an existing deployment.

## Consequences

Entitlement becomes answerable from a revision alone: what a tenant may use, in
which catalogue, at which approved rate, under which name, in which order. A
rollback restores all of it together, because every part is pinned rather than
resolved at read time.

The cost is authoring weight. Enabling one model for one project is a row per
scope rather than a flag, a re-approval when a catalogue snapshot is replaced,
and a separate approved-price resource before anything bills. Ordered targets
mean an alias edit that reorders is a new version an operator must publish, not a
runtime setting.

Two compatibility positions differ deliberately, and the difference is history
rather than taste:

- An **alias** body with no `schema` field is skipped rather than refused.
  Untyped alias rows exist in revisions already in the journal, and refusing one
  would stop an existing revision from hydrating on upgrade. Such a row is
  neither validated nor refused by these rules, which
  [revision convergence](../operations/revision-convergence.md#resource-body-schemas)
  states as the one exception to the untyped-body rule. The exception is keyed on
  the field being *absent*: a body that carries a `schema` which is not text is
  refused, because a damaged marker is not an older release's writing and reading
  it as one would let the row skip the scope, target, reach, and wire-family
  rules with nothing reported. It is refused as `ModelError::DamagedSchema`, and
  that refusal is *not* a compatibility one: no release wrote a marker that is
  not an identifier, so the row is damaged storage and the action is to restore
  it or republish the resource, not to roll a build forward. That classification
  is the shared reader's rather than this slice's: every body schema — tenants,
  projects, providers, credentials, policies, price books, and these two — has
  the same `DamagedSchema` refusal, so one operator-facing answer covers a marker
  a rewrite damaged wherever it is found. These rules run
  wherever a revision is read, so
  they hold at publication as well as at hydration; refusing to *author* a new
  untyped alias belongs to the slice that writes these bodies, which keeps the
  accommodation limited to rows already in the journal.
- An **enablement** body with no `schema` field is refused as `incompatible`. No
  release ever wrote one, so there is no upgrade to accommodate, and skipping it
  would be an entitlement hole rather than a compatibility allowance: a row
  nothing reads is also a row nothing binds to a scope, pins to a snapshot, or
  holds to one enablement per offering.

### Amendment — legacy disabled targets (2026-08-14)

PR #344 closes the lifecycle gap without making retained history unreadable. A
legacy published revision may contain an enabled alias targeting an enablement
that is already disabled; hydration, catalogue reads, and rollback tolerate
that exact shape so the revision remains readable and reversible. The strict
candidate/publication path rejects it for newly authored or modified aliases,
while a one-resource repair may carry untouched legacy aliases from its base;
the normal repair is to retarget or retire the alias being changed before
publishing further entitlement changes. Store-side revalidation carries that
same base context, and rollback branches on its mutation kind. Restack removes
a target only for a genuine enabled-to-disabled transition, not when an
already-disabled enablement is republished. When the last target disappears
during that transition, the alias's disabled version is part of the same
complete revision and semantic diff, so its retirement remains auditable without
creating a second mutation event.

Nothing in this slice is reachable from a request. Routing, `/v1/models`, admin
handlers, catalogue polling, and persistence continue to work as they did; they
become consumers of these bodies in later work, and a consumer that reads them
inherits the checks rather than restating them.
