# 54. Resolving pinned catalogue offerings

Date: 2026-08-13

## Status

Accepted

Numbered 54 because 53 is taken by request-path pricing (#147, PR #318), which
was written first and is unrelated to this decision; the sequence is contiguous
once that lands, and no other open change claims either number.

Extends [ADR 0047](./0047-callable-offering-identity.md), which keys an imported
catalogue by `CallableId`, and [ADR 0043](./0043-catalogue-source-imports.md),
which identifies a snapshot. This decision covers only how the two keys are held
together. It changes nothing about parsing, import, retention, refresh pacing, or
what any surface serves.

## Context

An enablement (ADR 0042) names a `CatalogOffering`: an opaque `OfferingId` and
the digest of the snapshot payload that identity was read from. It deliberately
carries nothing else — no provider endpoint, no published id, no limits, no
observed price — because those are catalogue facts, and copying them into
desired state would let an approved enablement and the catalogue drift apart.

So every consumer of an enablement has to go back to a catalogue to learn
anything, and the two keys do not meet:

- `OfferingId::of` digests the provider and the **neutral** model id, so it names
  "this provider's offering of this model".
- `ModelProjection` keys by `CallableId`: the provider and the **exact published
  id** a caller may send (ADR 0047, deliberately, because a provider may publish
  one model under several callable ids).

Neither key is derivable from the other by string manipulation, and the mapping
is one-to-many in exactly the provider-local alias case ADR 0047 exists to
preserve. Before this decision, the only code bridging them was
`RefreshImpact`, which derives the set of published `OfferingId`s to answer one
operator question — "what did this refresh stop publishing?" — and returns no
offering. Every other consumer (#149 tenant enablement and catalogue browsing,
#147 pricing, and request construction) would have had to re-derive the same
mapping, each with its own answer for the alias case.

## Decision

One map, `backends::catalog_pins::PinnedCatalog`, keys a single snapshot by the
identities enablements pin, and answers exactly four things.

**A resolution is always against content in hand.** `PinnedCatalog::of` takes the
content and the digest of the payload it was parsed from, and a pin naming a
different digest is answered `Resolution::OtherSnapshot` rather than looked up.
An enablement approved against yesterday's catalogue does not silently start
resolving through today's: the endpoint, limits, and observed price an operator
approved are the ones in the snapshot they read, and admitting new content does
not move a pin (ADR 0051).

**A pin reaching several callable ids is `Resolution::Ambiguous`, carrying all of
them.** Picking the first alias would send a request to an id the operator never
saw, and the choice would change when an upstream reordered or renamed its
aliases. Which id to call is an enablement decision, owned by #149, so the map
returns every candidate and refuses to make it.

**A pin the snapshot does not publish is `Resolution::Withdrawn`** — an
observation about the catalogue, never a withdrawal of the enablement.

Withdrawal *observation* is deliberately independent of the pin: an enablement
still pinned to an older payload is exactly the one nobody has looked at since
the upstream dropped its offering, so `PinnedCatalog::withdrawn_from` reports it
like `RefreshImpact` does rather than staying silent until an operator
republishes. Routing stays pinned all the same — `resolve` answers
`OtherSnapshot` for such a pin whether or not the offering is still published —
so the two states are distinguished where a request is served, not where an
operator is told.

**Identity derivation is total or the map is refused.** Construction derives
every offering identity once, so a lookup is a search rather than a
re-derivation, and a catalogue containing an offering whose identity cannot be
derived fails as a whole (`PinError::Underivable`) instead of answering
`Withdrawn` for an offering it does publish.

The map is I/O-free and borrows the content it was built over: it holds no store,
no client, and no runtime handle, and it cannot outlive or drift from the
catalogue that answered. `RefreshImpact` keeps its shape, and
`PinnedCatalog::withdrawn_from` is tested to agree with it, so the operator
report and the resolver cannot disagree about what an upstream stopped
publishing.

Nothing here enables, activates, prices, or withdraws anything.

### State tier

Tier 0. `PinnedCatalog` is a pure projection over already-loaded content and adds
no store, no schema, and no migration. It is usable identically in a Tier 0
config-only deployment, over a seed snapshot, and in a Tier 2 deployment over a
snapshot retained in Postgres (ADR 0051). It does not raise the tier of any
existing deployment.

## Consequences

The seam a runtime projection needs is now one type with one derivation, and the
alias and stale-pin cases are decided once rather than per consumer. What
remains, and is deliberately out of scope here:

- **Resolving a pin against the snapshot it names, rather than the active one.**
  `Resolution::OtherSnapshot` is honest about the gap: answering it requires
  reading that retained snapshot, which is
  `CatalogStore::retained(content_id)` — keyed by `CatalogContentId`, while a pin
  carries the **raw payload** digest. Nothing today maps one to the other. Either
  the store gains a lookup by raw digest, or an activation records the pair; that
  choice belongs to the slice that first needs a superseded snapshot (#149).
- **Where the map is built.** It must be compiled off the request path, from a
  snapshot already hydrated at boot or by a refresh, and published inside the
  immutable runtime snapshot, so that request handling reads a compiled map and
  never parses a payload or queries the control plane (ADR 0050,
  `backends` module contract). `PinnedCatalog` borrows its content, so whatever
  owns the runtime snapshot must own the content it is keyed over.
- **Choosing among ambiguous candidates, and pricing.** #149 and #147
  respectively.
