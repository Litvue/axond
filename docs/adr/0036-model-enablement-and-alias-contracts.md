# 36. Snapshot-pinned model enablements and ordered project aliases

Date: 2026-08-13

## Status

Accepted

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
  an alias name is unique within a project while the same name may exist in
  another project or tenant.
- **Lifecycle is enabled ↔ disabled, and identity is immutable.** Both
  transitions are permitted and idempotent; a transition to a state a body does
  not know is a typed refusal. Across versions of one resource, identity, owner,
  wire family, and — for an enablement — offering and pinned snapshot never
  change: a new offering or a new snapshot is a new resource, not an edit.

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
  states as the one exception to the untyped-body rule.
- An **enablement** body with no `schema` field is refused as `incompatible`. No
  release ever wrote one, so there is no upgrade to accommodate, and skipping it
  would be an entitlement hole rather than a compatibility allowance: a row
  nothing reads is also a row nothing binds to a scope, pins to a snapshot, or
  holds to one enablement per offering.

Nothing in this slice is reachable from a request. Routing, `/v1/models`, admin
handlers, catalogue polling, and persistence continue to work as they did; they
become consumers of these bodies in later work, and a consumer that reads them
inherits the checks rather than restating them.
