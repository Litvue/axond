# 41. Callable offering identity: keying the normalized model projection

Date: 2026-08-12

## Status

Accepted

Extends [ADR 0040](./0040-catalogue-source-imports.md), which settled how a
catalogue is imported and identified. This decision covers how the imported
catalogue is *keyed* for consumers, and changes nothing about parsing, import
semantics, or the three snapshot identities.

## Context

ADR 0040 files an offering under the model it is an offering of — the
provider-neutral, authored id (`xiaomi/mimo-v2-flash`) — and keeps the
provider's own string in `published_model_id`. That filing answers "who offers
this model?" and is the reason a provider publishing one model under two
callable ids contributes two offerings of one model rather than two models.

It does not answer the question every later slice asks. Issue #237: what a
caller may request is a `(provider, published id)` pair, and neither half of it
is a key on its own.

- `qiniu-ai` publishes both `mimo-v2-flash` and `xiaomi/mimo-v2-flash`. Both are
  separately callable, so a projection that keys by `(model, provider)` has to
  drop one, merge them, or pick one arbitrarily — and each of those makes a
  request that works today stop working, or bills against the wrong terms.
- The same published id (`mimo-v2-flash`) is published by a first-party provider
  and by an aggregator. Keying by published id alone merges two offerings with
  different prices and different endpoints.
- Conversely, two providers naming one model differently
  (`mimo-v2-flash` and `xiaomi/mimo-v2-flash`) must be recognizable as the same
  model, or a caller cannot fail over between them.

`CatalogDiff` has the matching gap: it compares offerings per
`(model, provider)`, so a provider that renames the id callers must send reports
as a metadata change, and a provider adding an alias reports an
`OfferingAdded` that does not say *which id* appeared. An operator reading
either cannot tell which requests broke.

## Decision

The normalized catalogue keeps one filing and gains one projection of it, and
the two identities stay separate types.

**A callable offering is identified by `CallableId` = provider + exact published
model id.** The published id is kept verbatim, case included, and is not
re-typed as a `ModelId` in the key: what makes it meaningful is the provider
that publishes it. `ModelProjection` holds one entry per catalogue offering,
sorted by `CallableId`, so a projection has exactly as many entries as the
catalogue has offerings — nothing is dropped, nothing is merged, and adding or
removing an alias cannot change how a sibling is filed.

**Neutral/authored model identity stays separate and is never rewritten.** Every
`CallableOffering` carries both: the id a request sends, and the `ModelId` it
reaches. `ProjectedModel` groups callable ids under that model identity, so
provider-local aliases (same provider, same model) and cross-provider
equivalents (same model, other providers) are both answerable directly, without
re-deriving them from string shapes.

**One callable id resolves to exactly one model.** `CatalogContent` already
refuses a repeated offering within a model, which is all a source document can
express; the projection refuses the same collision across models
(`ProjectionError::AmbiguousCallable`) because that is where a request has to
resolve to one model. Combined with ADR 0040's refusal of ambiguous tails at
import, no consumer of a projection ever has to guess.

**A projection has its own content identity.** `ProjectionId` is a checksum of
the projection's canonical form — the callable keying included — and is
deliberately distinct from `CatalogContentId`, which names the catalogue. Two
equal projection ids mean the same callable ids resolving to the same models
with the same offering content. `content_id()` is carried alongside so a stored
projection traces back to the catalogue it came from.

**Diffs are classified over callable ids.** `ProjectionDiff` reports `Added`,
`Removed`, `Renamed`, and `Refiled` (the id still works but now names another
model). A rename is its own class, not a removal beside an addition, because the
pair is what a caller has to act on. Renames are only ever looked for within one
`(provider, model)` group, and only between offerings that are the same offering
republished: same price, same endpoint, same stated capabilities, modalities,
limits, lifecycle, and provenance dates. What a rename tolerates is a changed
label — the display name and the last-updated date — because a provider that
renames the id callers send normally relabels the offering in the same breath,
and a label is not a term of service; requiring an identical one would reclassify
most real renames as an unrelated pair. Override provenance and payload pointers
are excluded too, being records of where the facts came from rather than facts of
their own. Anything left over stays an addition or a removal: two ids of one
model that differ in price, limits, or endpoint are not substitutes, and
reporting them as a rename would tell an operator to move traffic onto different
terms.
Facts, prices, and lifecycle are *not* re-reported here — those are
`CatalogDiff`'s classes, and reporting them twice would make a refresh's change
count depend on how many views a caller built. Two predicates separate the two
audiences a diff has: `breaks_requests()` is true when an id a caller sends
stopped working, and `resolves_elsewhere()` when an id keeps working but reaches
a differently identified model, which is what anything keyed by `ModelId` — an
entitlement in #205, a route — has to reconsider.

### State tier

Tier 0 (config-only), unchanged from ADR 0040. This is an I/O-free projection of
content already in hand: it borrows from a `CatalogContent`, reads nothing, and
writes nothing. Nothing here is reachable from the request path, and the parser
and import slice stay inert — no `/v1/models`, no admin handler, no persistence,
no entitlement activation, no provider polling.

## Consequences

**Backward compatibility.** Nothing existing changes shape: `CatalogContent`,
`CatalogModelEntry`, `ProviderOffering`, `CatalogContentId`, and `CatalogDiff`
are untouched, and every consumer of them keeps working unchanged. The
projection is additive and derived, so a deployment that never builds one
behaves exactly as before. Because no catalogue is persisted or served yet,
there is no stored representation to migrate and no wire contract to version:
this decision is made *before* the first consumer, which is the cheapest point
to make it.

**Migration and release.** The projection is derived from content, so a stored
snapshot (a later slice) needs to hold only the snapshot; a projection can be
recomputed, and `ProjectionId` is a cache validator rather than a schema
version. `ProjectionId` is not stable across changes to the projection's
canonical shape — that is what it is for — so anything that stores one must
treat a mismatch as "recompute", never as "the catalogue changed". Release-wise
this is a minor, additive change to a crate that has not yet exposed catalogue
metadata publicly; the first slice that *does* expose it (#149's catalogue APIs,
#205's enablement and aliases) chooses the wire spelling of a callable id, and
this ADR fixes only the domain identity it will spell.

**Two keyings now exist, and they are for different questions.** "Who offers
this model?" reads `CatalogContent`; "what may a caller send?" reads
`ModelProjection`. The cost is that a consumer has to know which question it is
asking, and a future request-path resolver must resolve through the projection
rather than by string-matching a model id. The benefit is that neither question
is answered by a lossy read of the other's key: the alias case that motivated
this — one model, four callable ids, three providers, three prices — is
representable exactly once, and a refresh that renames one of those ids says so.
