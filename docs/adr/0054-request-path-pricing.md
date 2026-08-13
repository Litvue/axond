# 54. Request-path pricing: which rate a request is charged at, and what says so

Date: 2026-08-13

## Status

Accepted

Closes the seam [ADR 0046](./0046-approved-price-books.md) left open — an
approved price book reaches the snapshot, but nothing charged from it — under the
charging denomination of
[ADR 0010](./0010-shared-budget-backends-and-charging-policy.md) and the usage
record contract of [ADR 0049](./0049-billing-grade-usage-outbox.md).

## Context

ADR 0046 makes approved pricing part of the immutable `ConfigSnapshot`: a
resolved set of approved rates, the price book's resource reference and canonical
checksum, and the `CatalogContentId` it was approved against. Nothing reads it.
Every charge is still computed from `Target::price` in `axond.toml`, and every
usage row still reports `catalog_version = 0` — a placeholder from before there
was a version to report (issue #147).

Four constraints shape the answer.

**A price book prices catalogue offerings, not routing names.** A price rule is
keyed by a `PricedTarget`: the catalogue's provider id and the id that provider
publishes a model under. A `[[model]] target` names an operator-chosen
`[[provider]] id` and whatever model string the operator wants to send upstream.
The two vocabularies coincide only by accident, and pricing one provider's
traffic at another's rates is a money error that no test would catch.

**Absence must not become zero.** ADR 0046 models "nothing approved this" as
`None` rather than as a free target, and the acceptance criterion for issue #147
is that such a model stays *discoverable* while being ineligible for
budget-controlled routing. A missing price that resolved to zero would serve the
traffic and bill nobody for it.

**A settlement must name the pricing it was computed from.** "What was this
charged at?" is asked months later, of a row, by someone who cannot re-derive the
answer from whatever is approved by then. Numeric `catalog_version` alone cannot
say which book, which publication of it, or which catalogue it was approved
against.

**A publication must not reprice work already in flight.** Streams last minutes
and hold a reservation across a settlement; a book published mid-stream would
otherwise settle a request at a rate that did not exist when it was admitted.

## Decision

A request resolves the price of every target it could route to *once*, from the
snapshot it is already holding, before it is admitted — and carries that resolved
value through the hold, the attempt walk, the settlement, and the row.

**The binding from a routed target to a catalogue offering is explicit.**
`Target::catalog` is an optional, operator-written `{ provider, model }` pair,
parsed into the pricing domain's own `ProviderId` at boot so a malformed binding
is a startup refusal. It is never inferred from the routing names, for the reason
above. An unbound target is simply not something a book prices.

**Precedence is: an approved book covering a bound target, else the file.** A
deployment with no price book, and a target its book does not bind, are both
charged at `Target::price` exactly as before — file pricing stays authoritative
where no approved rate claims the target, which is what keeps every existing
deployment's behaviour and its `catalog_version = 0` rows unchanged. Where a
bound target *is* covered by an approved book, the book's rate wins and the
configured rate is not consulted: a rate an operator approved is not overridden
by a file a deploy pipeline edits.

**A bound target the snapshot has no approved price for is ineligible, never
free.** `Ineligible::Unpriced` carries the offering, the book, and the book's
approval state — so a draft book, which activates no rates (ADR 0046), reads as
"nothing approved this" rather than as "priced at the draft". That triple is
operator-facing only: `Ineligible::detail` is logged, while `Ineligible::reason`
is the stable redacted string a caller is refused with, because which book a
deployment runs, at which version, and whether it is still a draft are
control-plane facts an unprivileged data-plane caller must not read out of an
error body. A target that is
ineligible is skipped by the failover walk; an alias whose every target is
ineligible refuses the request as `model_not_priced` (503) before admission
capacity or a rate-limit permit is spent. A route that pins its destination (the
Responses affinity pin) cannot fail over past the target it is pinned to, so a
walk that skipped only ineligible candidates reports the same typed
`model_not_priced` refusal rather than the generic "nothing to attempt" request
error — an unpriced pin is a pricing refusal even when a later target of the same
alias is chargeable. The alias remains listed by `/v1/models`
throughout: discoverability is a catalogue question, chargeability is a pricing
one.

**The resolved price is a value the request carries, not a lookup it repeats.**
`RequestPrice` is the rates plus an optional `PriceIdentity` (book reference,
checksum, catalogue content id). It is resolved from `ConfigSnapshot::pricing`
before admission and moved into the served target, the stream context, and the
usage record. A publication swaps what a *later* request's snapshot resolves and
cannot reach a request already holding one; a credential rotation mid-attempt
changes which key is used, never what the work costs. The estimate the budget
hold is taken against and the amount the settlement charges therefore come from
one immutable value.

**A row names its pricing in four columns, three of them nullable.**
`catalog_version` becomes the price book's resource version where a book applies
and keeps its `0` where the file prices the request — the compatibility meaning
of `0` is now "priced by configuration", which is exactly what it has always
meant in practice. `price_book`, `price_book_checksum`, and `price_catalog` are
new, nullable, and absent from a file-priced row, so an existing consumer parses
new rows unchanged. They are added by an additive migration
(`usage_v2_001_add_price_identity.sql`), which the Postgres sink requires to have
been applied: it compares the table's columns against the ones it binds while it
connects and refuses to boot naming the file to apply, so an existing
installation that deploys the writer first fails its deploy rather than dropping
every batch at insert time. The columns are propagated to the OTLP attributes and
the Postgres journal, which reads them as optional so rows written before this
change stay readable. No historical row is rewritten when a price changes: a new
publication writes a new version into new rows and leaves settled ones alone,
which is what makes a price change auditable rather than retroactive.

### State tier

Tier 0 (config-only). Resolution reads the in-memory snapshot; the request path
gains no query, no lock, and no I/O, and a deployment with no price book is
unchanged. The three new usage columns land wherever usage already persists —
Tier 2 deployments run one additive `ALTER TABLE`, and this raises no
deployment's tier because a deployment without the Postgres sink has nothing to
migrate.

## Consequences

Binding a target to a catalogue offering becomes the act that opts it into
approved pricing, and it is deliberate: approving a book changes no charge until
an operator writes the binding. That is two steps where one might be expected,
and it is the cost of never guessing which offering a routing name meant. It also
means an operator can approve a book and diff it against what is being charged
before any traffic moves onto it.

Binding a target the approved book does not price takes it out of service (503)
rather than quietly billing the file's rate. This is the intended failure — an
unapproved price is a budget the deployment cannot enforce — but it makes a
binding typo an availability event, so the refusal names the offering, the book,
and the book's approval state to keep the loop short. An alias with more than one
target degrades instead: unpriced targets are skipped while any priced one can
still serve.

Rows written before this change carry no book identity, and rows from a
file-priced deployment never will. A query that groups by pricing has to treat
`price_book IS NULL` as "configured rates", which is why the numeric column keeps
its old meaning rather than being repurposed.

Resolution is per-request and per-target rather than cached, so the cost is a
`BTreeMap` lookup per target per request against a map sized by the deployment's
priced targets. Caching it on the snapshot would need the alias set and the
pricing to be compiled together; that is worth doing when the alias vocabulary
becomes durable state (#149), not before.

Deployment-wide books are the only scope served (ADR 0046), so per-tenant
negotiated pricing does not reach the request path here even though the envelope
can carry it. A tenant-scoped book still refuses the candidate as an
incompatibility, so a replica that meets one reports skew rather than charging
the baseline to a tenant that negotiated its own.
