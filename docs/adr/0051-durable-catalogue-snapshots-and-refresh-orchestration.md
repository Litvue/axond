# 51. Durable catalogue snapshots, and who decides when to import one

Date: 2026-08-12

## Status

Accepted

Completes the slice [ADR 0043](./0043-catalogue-source-imports.md) deferred: it
imported a catalogue and held it in memory, and named the scheduler that would
drive it as later work. This adds the storage and the driver, under the state
tiers and per-responsibility backend contracts of
[ADR 0027](./0027-stateless-and-stateful-operating-modes.md).

## Context

ADR 0043 leaves a deployment in an odd state. It can import models.dev, refuse a
malformed one without disturbing what is active, and report the refusal — but
only for the life of a process, and only when something calls `refresh`. Nothing
calls it. So the catalogue a running gateway holds is whichever one that replica
last imported, a restart is an amnesia the operator cannot see, and two replicas
of the same deployment can hold two different catalogues while both report
themselves healthy.

Three things make this more than a missing `tokio::spawn`.

**A catalogue is referenced by identity long after it is imported.** A
`CatalogOffering` pins the snapshot an enablement was published against
([ADR 0042](./0042-model-enablement-and-alias-contracts.md)), and an approved
price book names the content it was approved for
([ADR 0046](./0046-approved-price-books.md)). Those pins outlive the import, so
the store has to keep superseded catalogues, not just the current one, and it
has to keep them unchanged.

**Storing normalized content is a second definition of what a catalogue is.**
The obvious design — serialize `CatalogContent` into columns or JSON and read it
back — creates a decoder that is free to drift from the normalizer, silently: a
field the normalizer stopped emitting is a field the decoder happily supplies
from an old row, and nothing compares the two.

**Staleness is a property of the deployment, not of a process.** A refusal
counter that resets on restart is worse than none: an upstream that has been
refusing for a day looks, to a replica that restarted an hour ago, like a
catalogue that has simply not refreshed yet.

## Decision

Two tables, one refresher, and a rule about who may activate anything.

**Storage is checksum-addressed history plus a pointer.**
`axond_catalog_snapshot` holds one row per distinct imported catalogue, keyed by
the `CatalogContentId` of its normalized content and holding the exact bytes
that were accepted, their digest, their length, and the provenance they arrived
with. Nothing updates a row: re-importing an unchanged catalogue conflicts on the
primary key and stores nothing, so idempotence is the table's shape rather than a
check someone remembered to write, and a pin keeps resolving what it pinned.
`axond_catalog_active` is a single row — enforced by a constant primary key, so
two active catalogues are unrepresentable — naming which snapshot is active, what
the source last confirmed about it, and how many refreshes in a row have been
refused. Activation is one transaction over both, because a replica that crashed
between them would either lose an import it admitted or point at bytes it never
wrote.

**Rehydration re-parses; it does not deserialize.** `hydrate` runs the stored
bytes back through the same strict adapter that accepted them and then checks
that the content still carries the identity the row was stored under. There is
no stored form of the normalized domain at all, so there is nothing to drift.
It also makes the interesting failure loud: if this build would normalize
yesterday's bytes differently, boot says so (`HydrationError::Drift`) instead of
serving content nobody can reproduce.

**Retention precedes admission.** `CatalogRefresher` writes an import to the
store before it becomes active in the process, so a store that refuses leaves
the previous catalogue active and counts a `NotRetained` refusal. A replica
cannot serve a catalogue it did not manage to keep, which is what stops the
in-memory catalogue and the deployment's own record of it from diverging.

**Every outcome goes through the last-known-good holder.** Success, `304`, a
refused parse, an upstream outage, a store failure, and a timeout all reach
`LastKnownGoodCatalog::record_refresh`, which is where the refusal run is
counted and where an admitted or confirmed import ends it. The durable counters
move with it, and `LastKnownGoodCatalog::restored` adopts them at boot rather
than replaying them as events, so a restarted replica reports the staleness the
deployment actually has.

**One attempt is bounded; a run of failures is paced.** A refresh is capped by
`RefreshSchedule::timeout` — a hanging upstream costs a refused refresh, not a
scheduler that never runs again — and a refused one is retried on the
deterministic, saturating backoff of ADR 0027's convergence loop. The retry
ceiling is validated to stay at or under the refresh interval, so a failing
deployment can never end up refreshing *less* often than a healthy one.

**A manual refresh is the same import as a scheduled one.** `RefreshTrigger`
changes exactly one behaviour: a scheduled refresh that is not due is skipped and
a manual one is not, because an operator asking during an incident is asking now.
The timeout, the ordering, the counting, and the backoff are identical, so the
path an operator exercises by hand is the path that runs unattended.

**Nothing an upstream says activates anything.** This is ADR 0043's boundary,
now that something drives it automatically: a refresh admits observations. An
enablement keeps its state, its approved price, and the snapshot digest it was
approved against — admitting new content does not move a pin — so a model that
appears upstream is not usable and a rate that changes upstream is not charged.
`RefreshImpact` reports what a newly imported catalogue *would* mean for what
operators enabled (which pins are behind, which enabled offerings the upstream
no longer publishes) and changes none of it.

**The offline seed is a bootstrap, not a claim.** A deployment configured with
`Bootstrap::Seed` and an empty store imports the compiled-in excerpt and retains
it like any other import, aged to the moment it was imported rather than to the
day the fixture was cut. Its provenance stays seed-local (ADR 0043's
`W/"seed-<content id>"`), so no upstream can answer `304` against it and the
first live refresh transfers the real document.

### State tier

Tier 2 (Postgres) when a durable store is configured, and unchanged otherwise:
`InMemoryCatalogStore` keeps the same semantics for a single-replica development
run, and a deployment with no catalogue store configured behaves exactly as it
did under ADR 0043. Nothing here is on the request path — the store declares
`BackendPath::Background`, like the source it retains — so an unavailable
database costs a refused refresh and never an inference request.

## Consequences

A deployment now accumulates catalogues. Every distinct import is a
multi-megabyte row kept forever, because deleting one could break a pin, and
this slice deliberately does not decide a retention policy: the decision needs
to consider which snapshots enablements and price books still name, and getting
it wrong strands an approval. The row count grows with genuine upstream changes
rather than with refresh frequency — an unchanged re-import stores nothing — so
the growth is slow, but it is unbounded and a later slice owes an answer.

Re-parsing on every boot costs a parse of the whole document per replica start,
and it makes a parser change able to refuse a stored catalogue that used to be
fine. Both are deliberate: the parse is milliseconds off the request path, and a
refusal at boot is how a normalization change announces itself instead of
quietly re-identifying content that a price book was approved against.

Refusals now have somewhere to persist, which means an operator can be told the
truth across restarts — and also that a deployment can boot into an
already-alerting state. That is the intended reading: `PERSISTENT_REFUSAL_THRESHOLD`
is about the catalogue, not about the process, and #241's metrics and status
surface project from the same `CatalogReport` either way.

Two behaviours are deliberately still absent here. The refresher is not wired
into `serve`: nothing spawns it and no configuration section selects a store, so
this slice changes no running deployment — [ADR 0053](./0053-catalogue-imports-in-a-running-deployment.md)
adds the `[catalog]` section, the production fetch, and the task that owns the
loop. And `RefreshImpact` is a report with no consumer yet — the surface an operator reads it on is the enablement work of
#146's remaining slices, which is also where a decision to act on a withdrawn
offering belongs.
