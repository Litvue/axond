# 37. Importing a model catalogue: observed rates, snapshot identity, and an offline seed

Date: 2026-08-12

## Status

Accepted

Extends the price denomination of [ADR 0010](./0010-shared-budget-backends-and-charging-policy.md)
with a second, non-billing unit, and adds a package boundary to the ones
[ADR 0025](./0025-crates-io-publication.md) settled.

## Context

The gateway knows what an operator configured and nothing about what providers
publish: which models exist, what they cost, which are deprecated. Issue #146
wants that knowledge, and this decision covers only the first slice of it —
reading a source's catalogue and holding it as an immutable, comparable
snapshot. Nothing here charges anyone, enables anything for a tenant, or runs on
the request path.

Three properties of the source material force the decisions below.

**Published rates are finer than money the gateway charges.** models.dev states
rates per million tokens as JSON decimals, and they go as fine as
`$0.26666667` — a third of a dollar, divided by three, published as eight
decimal places. ADR 0010 denominates *gateway* prices in micro-dollars because
that is the resolution a charge is enforced at; the same unit would round the
observation above at import, and an observation rounded once can never be
rounded correctly again.

**"Is this the catalogue we already hold?" has three different answers.** The
bytes may differ while the meaning does not (whitespace, key order, a
re-serialization upstream); the meaning may differ while the bytes cannot be
compared at all (a mirror, a re-fetch); and the upstream's own answer — `ETag`
and `Last-Modified` — is about the *document*, not about the catalogue in it.
Collapsing these into one version string, which the pre-existing
`CatalogVersion` did, makes a conditional refresh and a content comparison the
same question, and they are not.

**A catalogue import fails long after the process needs one.** The source is a
third party reached over the network at refresh time. A refusal must therefore
leave the previous catalogue serving, and a deployment that has never reached
the network at all must still have a catalogue to show.

## Decision

Catalogue imports are a domain with its own units and its own identity, kept
apart from pricing and from the request path.

**Observed rates are integer nano-dollars per million tokens**
(`ObservedRate`), parsed from the JSON number's own text by exact decimal
arithmetic — never through `f64`, which cannot represent `0.1` and would make a
content checksum depend on rounding. A rate finer than a nano-dollar is refused
rather than rounded, because a rate too fine to represent is an unusable
observation and not a free one. This unit is deliberately *not* ADR 0010's
micro-dollars, and an `ObservedPrice` is deliberately not a `ModelPrice`: it
records what a source published. Converting an observation into a price the
gateway charges is price activation, a later slice, and it is that conversion's
job to choose a rounding an operator has agreed to.

**An imported snapshot carries three separate identities.** The raw payload's
digest and length name the bytes; a canonical checksum over the normalized
catalogue (`CatalogContentId`) names the meaning, and is by construction
independent of retrieval time, source URL, validators, JSON formatting, and map
order; `SourceValidators` holds the upstream's `ETag` and `Last-Modified`
verbatim for conditional refresh. `CatalogVersion` is replaced by these, and a
content id is computed when content is constructed, so content that cannot be
canonicalized is refused at the boundary instead of failing later.

Admission is last-known-good: a snapshot whose content id matches the active one
is `Unchanged`, a differing one is `Updated` with a semantic diff, and a refusal
changes nothing that is already active. The diff classes cover everything the
content id covers — providers, neutral records, offerings, lifecycle,
capabilities, metadata, prices — so an `Updated` can never report "the catalogue
changed" with nothing to show.

**The two id namespaces in the document are joined explicitly.** A source's
neutral index is authored (`openai/gpt-5.5`) while each provider keys offerings
as its own API names them (`gpt-5.5`), so an offering resolves to the neutral
record it is the unauthored tail of at a segment boundary, and a tail matching
two authored records is refused rather than attributed to one of them.
`published_model_id` keeps the provider's own string, which is what a request to
that provider must use. Resolution reads only the key and the neutral index, so a
provider that publishes one model under two callable ids (`qiniu-ai` offers both
`mimo-v2-flash` and `xiaomi/mimo-v2-flash`) contributes two offerings of one
model rather than two models: what identifies an offering is
`(provider, published_model_id)`, and a projection of distinct models is
therefore a projection of the catalogue's models.

**A real excerpt of the source document is compiled into the binary** as an
offline seed and parsed through the same strict adapter as a fetched payload, so
the seed cannot drift from the parser and an air-gapped deployment still has a
catalogue. `crates/gateway`'s published `include` therefore carries
`src/**/*.json`, extending ADR 0025's package boundaries; the seed is an
excerpt, chosen for shape coverage, not a mirror of the upstream document. It
therefore carries a validator over its *own* content (`W/"seed-<content id>"`)
rather than the `ETag` the excerpt was cut from: a validator only means "you
already have this" for content actually held, and echoing the upstream's would
let the first live refresh answer `304` and leave four providers active as if
they were the whole catalogue.

### State tier

Tier 0 (config-only). This slice is domain, parsing, and fixtures: a catalogue
lives in memory, the offline seed is compiled in, and no Redis or Postgres state
is read or written. The tier of an existing deployment is unchanged, and the
default is unchanged: without a configured source, the seed is the catalogue.
Persisting snapshots is a later slice and will declare its own tier.

## Consequences

Two units for money now exist, and they cannot be mixed accidentally:
`ObservedRate` is nano-dollars per million tokens and `ModelPrice` is
micro-dollars, with no conversion between them in this slice. That is the point
— an observation must not silently become a charge — but it does mean price
activation has to state its rounding explicitly, in public, when it arrives.

Three identities are more to carry than one version string, and each has to be
recorded on every snapshot. In exchange, a conditional refresh costs nothing
when the upstream is unchanged, a re-serialized document does not read as a
changed catalogue, and "what changed?" is answerable from two snapshots without
re-reading the payloads.

Strictness has an operational cost: an upstream that renames a status, changes a
field's type, publishes a rate finer than a nano-dollar, or makes an offering's
model ambiguous stops the import instead of guessing. Last-known-good makes that
survivable rather than an outage, but it means catalogue staleness is a real
operational signal and a refusal has to be visible to an operator. Fields the
adapter does not model are ignored, so an upstream *addition* never has that
effect.

That visibility cannot be built here, because this slice does not schedule
anything: the source is background-only and has no caller yet. The contract is
therefore stated for the slice that will drive it, and the domain is shaped to
make it keepable — `admit_result` returns the typed rejection *together with* the
snapshot that stayed active, so a refusal cannot be observed without the stale
thing in hand, and every rejection names a JSON Pointer. Scheduled refresh must
ship with a refusal counter labelled by reason, the active snapshot's
`fetched_at` exported as an age, and an alert when refusals persist across more
than one interval; that work is [issue #241][241]. Until then no catalogue is
refreshed automatically at all, so there is no unattended staleness to miss:
without a configured source the compiled-in seed is the catalogue, and because
metadata is never an entitlement or admission input, a stale catalogue degrades
metadata quality rather than availability.

[241]: https://github.com/Litvue/axond/issues/241

The compiled-in seed adds bytes to the binary and one more thing to keep
plausible as the upstream evolves. It is what makes the catalogue testable
hermetically and available with no network at all, and because it is parsed by
the shipped adapter, a seed that stops being valid fails the build's tests
rather than a deployment.
