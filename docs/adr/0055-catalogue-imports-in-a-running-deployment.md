# 55. Catalogue imports in a running deployment

Date: 2026-08-13

## Status

Accepted

Numbered 55 because 53 is request-path pricing (#147, PR #318) and 54 is
resolving pinned catalogue offerings (#146, PR #319); this decision is the import
side of #146 and is independent of both.

Closes the two behaviours [ADR 0051](./0051-durable-catalogue-snapshots-and-refresh-orchestration.md)
deliberately left absent: nothing configured a source or a store, and nothing
spawned the refresher. The domain, the retention, and the refresh policy are
unchanged by this decision — what is added is the configuration that selects
them, the HTTP client that performs the fetch, and the task that owns the loop.

## Context

Everything a catalogue import needs exists and nothing performs one. `CatalogSource`
and `ModelsDevSource` normalize and identify content, `CatalogStore` retains it
with its provenance, and `CatalogRefresher` decides what an import did — but the
only implementation of `CatalogFetch` lived in a test module, no `[catalog]`
section existed to select a source or a store, and `serve` never constructed any
of it. A shipped binary therefore imported nothing, and the durable snapshots
ADR 0051 designed had no writer.

Three properties make the wiring less obvious than "spawn a task":

An import must not be reachable from a request. models.dev is a third party with
its own latency and outages, and a catalogue is metadata about what *could* be
served rather than an input to serving anything. A handler that could trigger a
fetch — or wait behind one — couples inference availability to an upstream that
has nothing to do with the request.

A refresher cannot have two drivers. It owns the active pointer and the refusal
count, and two callers refreshing concurrently would race over both while
producing no result either could not have produced alone. An operator still needs
to be able to say "now" without waiting out the interval.

An unhappy import must not be able to stop a deployment. Metadata that is stale,
or a stored snapshot this build no longer reproduces, is an operational condition
every request the replica would serve is indifferent to.

## Decision

`[catalog]` selects a source (`none`, `models-dev`, `seed`), a store
(`in-memory`, `postgres`), the schedule and backoff, the bootstrap, and the
payload ceiling. Every bound is checked at boot as the set it is, so a zero
interval, an instant timeout, a backoff ceiling below its first delay, or an
unsupported document is a boot failure rather than a loop that misbehaves six
hours later. The DSN is named, never held: `dsn_env` gives the variable, which is
inherited from `[control_plane]` when omitted, and the connection string reaches
`PostgresCatalogStore::connect` and nothing else.

The default is `source = "none"`: no client is built, no connection is opened,
and no task is spawned. Upgrading acquires no upstream dependency.

`HttpCatalogFetch` is the production fetch, and the same one the tests exercise:
conditional `GET` carrying the stored ETag and `Last-Modified`, `304` as
`NotModified`, an explicit request timeout, and a body read under a ceiling
rather than read whole and then measured.

The source is the URL an operator approved, and the constraints that keep it so:
`source_url` must parse as an HTTPS URL with a host and no embedded credentials,
refused at boot rather than at the first refresh, and redirects are not followed.
Imported metadata is what an operator reads to approve a price or enable a model
later, so a plaintext document anyone on the path may rewrite is not a source
worth trusting, and following a redirect would let the answer choose the document
— and the transport — while the provenance every snapshot records still named the
configured one. A `3xx` is therefore a bounded refusal that names its status and
is counted as a misconfiguration rather than an outage: retrying an unchanged
request cannot fix a URL only an operator can change. An HTTPS mirror stays
supported and hosts are deliberately not allowlisted, because an air-gapped
deployment's mirror is a legitimate source and its hostname is not something this
project can enumerate.

Boot constructs the source, the store, and the refresher before the listener
binds, adopts what is already retained, and publishes the first report. A
background Tokio task then owns the refresher for the life of the process,
sleeping until the refresher says the next import is due — so ordinary cadence
and post-refusal backoff are one mechanism — and is biased toward its stop
signal, because a terminating process must not start an upstream request it will
abandon. Manual refreshes reach that same task through a channel, so a manual
import and a scheduled one take one code path and differ only where they truly
differ: a manual one is never skipped for not being due.

Freshness is published where an operator already looks. The task writes a bounded
`CatalogReport` into a handle the replica holds, and the deployment-scope
`/status` projection reads it: the active content's short digest, its age, the
consecutive refusals, and the last refusal's closed-vocabulary reason. A status
read is a memory read of that handle — it contacts no upstream and queries no
database — and a tenant-scoped caller sees no catalogue at all.

### State tier

Tier 0 by default: the inert `[catalog]` section requires nothing. `store =
"postgres"` is Tier 2 and is what a stateful deployment must select — an
in-memory store there would lose every snapshot and its provenance on restart,
which is the durable contract in name only. A stateless deployment may still
import into the in-memory store for development. No existing deployment's tier
changes: a file that does not configure a catalogue imports none.

## Consequences

A deployment that enables this now depends on models.dev *in the background*.
Availability coupling is one-directional and bounded: a failed fetch is a
counted refusal, the last-known-good catalogue keeps serving, and the next
attempt is delayed by the backoff. Nothing on the request path notices.

Restoration failure is a refusal, not a boot failure. A store that cannot be read
or a retained catalogue this build no longer reproduces leaves the replica
serving with the catalogue reported as refused — which is louder than it sounds,
since two consecutive refusals is the alerting threshold, and quieter than a
fleet that will not start because its metadata is unhappy.

The stop signal fires after serving ends rather than at `SIGTERM`, so an import
in flight during a drain runs until the drain it cannot outlive is over. Nothing
in the shutdown sequence waits on it: the flush budget belongs to spend already
incurred, and abandoning a metadata import costs a refresh, not a record.

Imports remain observational. Admitting a catalogue activates no model, changes
no tenant's enablements, reconciles no availability, and settles no price —
`RefreshImpact` still reports pins and withdrawals to nobody, and the surfaces
that act on them belong to the enablement, pricing, and availability slices.
