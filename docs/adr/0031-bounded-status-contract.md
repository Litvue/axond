# 31. Bounded status contract and canonical metric catalogue

Date: 2026-08-12

## Status

Accepted

Fixes the shape of the health and telemetry surfaces that the stateful slices of
[ADR 0027](./0027-stateless-and-stateful-operating-modes.md) will fill in, and
extends the telemetry model of [ADR 0007](./0007-telemetry-model.md) with a
machine-checked catalogue.

## Context

A stateful axond has dependencies a stateless one does not: a control plane, a
catalogue projection, a secret store, and durable budget, rate-limit, and
revocation stores. Operators will ask the obvious question — *which of them is
this replica actually talking to right now?* — and there is a well-worn wrong
answer: an endpoint that probes every backend when asked. That design makes the
health surface an amplifier (one dashboard refresh becomes six backend calls), it
makes a slow store into a slow health check, it lets an unauthenticated or
tenant-scoped caller drive load against shared infrastructure, and it puts store
latency on a path that must answer while those stores are down.

The redaction question is just as sharp. Everything useful a probe learns is
unsafe by default: a connection string names hosts and passwords, a store error
quotes credentials, a rejected revision names the revision and the field that
failed validation, and a tenant asking about *its* view must not learn what other
tenants exist or how the operator's control plane is configured.

Metrics have the same failure mode one layer down. Cardinality is a correctness
property, not a cost concern: a label keyed by tenant, subject, credential id,
alias, or revision id turns a counter into an unbounded time-series family and,
worse, makes tenancy readable from any scrape endpoint. `axond.request.*` already
carries namespace and model dimensions deliberately, and the distinction between
*a dimension a deployment opts into on a specific instrument* and *a label
attached to everything by default* was documented prose rather than something a
test could enforce.

Both surfaces are cheaper to fix as contracts, before there is an implementation
whose behaviour has to be preserved.

## Decision

**Three distinct health surfaces, with distinct audiences.** `/healthz`
(unauthenticated, "the process is alive") and `/readyz` (unauthenticated, "this
replica is serving a boot-validated snapshot, and `503 draining` once termination
starts") keep their exact current behaviour and stay free of dependency state:
they are what a load balancer and a kubelet poll, and neither may ever fail
because a store is slow. Dependency state belongs to a third surface,
`/admin/v1/status`, which is **authenticated** and requires a dedicated `status`
capability — not `models`, not `credentials`, and not operator-only, so a
deployment can grant it to a monitoring principal without granting anything else.
`status` is the first capability that is not tied to a namespace having a model
route, because a namespace's dependency health is a legitimate question even when
it can invoke nothing.

**Reads are cache reads.** `CachedStatusRegistry::view` is synchronous, takes an
in-memory lock, and has no `async` in its signature — a handler *cannot* probe a
backend, acquire a budget or rate-limit permit, or read revocation state, because
there is no path from the read to any of them. Observations are published by a
separate `StatusRefresher` on its own cadence, each probe bounded by
`probe_timeout` and abandoned (recorded as `timeout`) rather than waited on. Every
answer therefore carries an *age*, and an observation older than
`staleness_budget` reports `degraded`/`stale` rather than either lying about
freshness or failing: a replica serving a valid snapshot through an observation
outage is stale, not down. A component nobody has configured reports `disabled`,
and one that is enabled but never observed reports `unavailable`/`unknown` — the
open direction, never `ok`.

**The response has nowhere to put free text.** Every string in it comes from a
closed vocabulary: the component, the state (`ok`, `degraded`, `unavailable`,
`disabled`), and an optional reason from a fixed list of sixteen codes. Backend
failures and rejected revisions are *mapped* into that list; an unrecognised
input maps to `unknown` rather than becoming a new response value. The
operator-facing detail a probe collects lives on the internal observation, is
logged there, and has no field to be projected into. Tenant scope narrows the
answer twice over: operator-only components (control plane, secret store, usage
sink) are omitted entirely, reasons that would describe the operator's
configuration coarsen to `unavailable`, ages coarsen to whole seconds, and the
revision summary — lag, generation, convergence — is omitted. No response at any
scope carries a revision id, a namespace, a subject, a credential id, or a host.

**The metric catalogue is code.** Every instrument the gateway builds is declared
once, with its kind, unit, and the labels it may carry, and each label is
classified `Closed` (a finite vocabulary, enumerated), `Numeric`, `Route`
(a registered route template), or `Configured` — deployment-defined and therefore
potentially unbounded. `Configured` is legitimate as an explicitly chosen
dimension on a specific instrument and is *refused* as a default label attached
to everything, which is the distinction that was previously only prose. A
forbidden-key list refuses tenant, subject, credential id, alias, model id,
revision id, request id, jti, and anything named like a secret, DSN, URL, error,
message, detail, prompt, or completion. Tests parse `metrics.rs` and fail if an
instrument is built that the catalogue does not declare, if a recorded label is
not declared for the instrument recording it, or if a closed vocabulary is missing
a value the code can emit — so the catalogue cannot drift into documentation.

## Alternatives considered

- **Extend `/readyz` with dependency state.** It is the surface a load balancer
  polls, it is unauthenticated, and it must answer while stores are down.
  Anything that makes readiness depend on a dependency observation makes a store
  outage into a fleet-wide removal from service.
- **Probe on request, with a short cache.** The cache would still be filled by
  the first caller, so a health check would still occasionally cost six backend
  calls and inherit their latency; the amplification would just be intermittent
  and therefore harder to reason about.
- **One free-text `detail` field, redacted by best effort.** Redaction by pattern
  matching is a treadmill: every new backend error format is a new leak, and the
  leak lands on the surface most likely to be scraped into a ticket. A response
  type with no free-text field cannot leak.
- **Reuse `models` or an operator-only authority for status.** Monitoring would
  then require a principal that can also invoke inference or administer the
  deployment; a dedicated capability is the smaller grant.
- **Document metric labels and rely on review.** That is what existed. The
  catalogue's value is that a drifting instrument fails a test rather than a
  reader.

## Not decided here

- No component probe is implemented, no refresher is started, and no route is
  registered: this slice fixes the contract, and the stateful slices that own
  each backend add their probe behind it. The stateless posture is that every
  component is `disabled`, which is exactly what a stateless replica should say.
- Dashboards, alert rules, and multi-replica qualification are out of scope; the
  catalogue is the input they will be written against.

## Consequences

- The status surface can be implemented incrementally, one probe at a time,
  without any slice being able to reintroduce request-path probing: the read API
  makes it unrepresentable.
- Status answers are approximate by construction, and callers must read the age.
  A component that just failed reports `ok` until the next refresh; that is the
  cost of never putting a store on a health path, and the age makes it visible
  rather than implicit.
- The reason vocabulary is a compatibility surface. New codes are additive under
  the [0.x policy](./0015-zero-dot-x-compatibility-policy.md); a caller must
  treat an unrecognised code as opaque rather than exhaustively matching it.
- Adding an instrument or a label now means updating the catalogue in the same
  change, and adding a genuinely unbounded default label is no longer possible
  without deleting a test.
