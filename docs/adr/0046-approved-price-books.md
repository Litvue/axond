# 46. Approved price books: what activates a rate, and what a snapshot must remember

Date: 2026-08-12

## Status

Accepted

Answers the question [ADR 0043](./0043-catalogue-source-imports.md) deferred —
how an observed rate becomes a rate the gateway charges — under the price
denomination of
[ADR 0010](./0010-shared-budget-backends-and-charging-policy.md) and inside the
desired-state revision model of
[ADR 0027](./0027-stateless-and-stateful-operating-modes.md).

## Context

ADR 0043 imports what providers publish and deliberately stops short of
charging anyone: an `ObservedRate` is nano-dollars per million tokens, a
`ModelPrice` is micro-dollars, and there is no conversion between them. That
leaves a gap issue #201 has to close, because a budget cannot be enforced
against a price nobody has agreed to.

Three constraints shape the answer.

**An import is not an approval.** models.dev is a third party the gateway polls.
If an imported rate became the billed rate, an upstream edit would silently
change what every tenant is charged, with no operator in the loop and no record
of who accepted it. A price change is a money change, and money changes need an
approver and an audit trail.

**A published rate can be finer than money the gateway charges.** ADR 0043 keeps
observations at nano-dollar resolution precisely so they are not rounded at
import. The runtime charges in micro-dollars. Something has to state what
happens to `$0.26666667`, in public, once.

**"What was this request billed at?" must be answerable from the snapshot the
request held.** Requests are served from an immutable `ConfigSnapshot`; the
request path reads no database, no models.dev, and nothing mutable. So the
identity of the price book, the catalogue it was approved against, and the
interval its rates were in force over have to be *in* the snapshot, not looked
up later. Numeric `catalog_version` — already replaced by three identities in
ADR 0043 — is not a place to encode this.

## Decision

Approved pricing is a typed, immutable desired-state body (`axond.price-book.v1`,
`ResourceKind::Price`) that is read during compilation and published as part of
the snapshot it prices.

**Approval activates a rate; an import never does.** A price book records an
`Approval` — `draft`, or `approved` with the actor, the instant, and an optional
citation (a change ticket). A draft book activates nothing, and neither does a
book that does not name a target: the snapshot then carries *no price* for it,
which is what a later budget-controlled routing slice reads as ineligible rather
than as free. Observed catalogue prices remain metadata, and the only path from
an observation to a charge is an operator approving a book that states the rate.
`ApprovedRate` is a distinct type from `ObservedRate` for that reason;
`ApprovedRate::approving` is the one place the two meet, and it is a decision
rather than a conversion.

**Conversion to the runtime unit is exact, or it is a refusal.** An approved rate
is nano-dollars per million tokens — the same unit an observation is stated in,
so approving is not a rounding — and converts to `ModelPrice`'s micro-dollars
only when it is a whole number of micro-dollars. A rate that is not is refused
naming the field, not rounded in either direction. The rounding question ADR
0041 left to this slice is therefore answered by not rounding: an operator who
approves `$0.26666667` per million tokens is told the runtime cannot bill it and
approves a rate it can. Negative rates, rates past the unit's range, tiered
schedules, and rates for usage this build does not meter (audio, which no field
of `Usage` counts) are refused the same way, each naming what it refused.

**Rules are effective-dated over half-open intervals, and precedence is
explicit.** A rule covers `[from, until)`, so consecutive rules meet at an
instant without overlapping and the boundary instant belongs to exactly one of
them. Two rules of the *same* precedence in force for one target at one instant
are refused: which rate applies would be undefined. Deliberate overlap is
expressed rather than inferred — an `override` rule supersedes a `baseline` rule
for the interval it covers, which is what makes an operator's negotiated price
auditable as an override instead of an edit to the baseline.

**A pricing snapshot carries identity, not just numbers.** `PricingSnapshot`
records the price book's resource reference, its canonical checksum, the
`CatalogContentId` it was approved against, the approval state, the interval the
resolution is valid over, and the approved prices. It is attached to the
`ConfigSnapshot` the routing config was built into, before publication, so
routing and pricing become visible in one atomic store: no request can observe a
generation whose prices have not arrived. A book this build cannot bill refuses
the whole candidate at compile time, which is what makes "the previous snapshot
keeps serving" true for pricing as it already is for routing.

**Historical books are immutable.** A revision pins resource versions, so
changing a rate is a new version of the book in a new revision, and rollback is
republication of a prior one. The envelope permits price books at deployment,
tenant, and project scope, because a negotiated book belongs to whoever
negotiated it; this slice serves the deployment-wide approved baseline only and
refuses the other two as an *incompatibility*, so a replica that meets a
tenant-scoped book reports version skew rather than corruption.

### State tier

Tier 0 (config-only). A price book is desired state read through the existing
revision path and resolved in memory during compilation; nothing here reads or
writes Redis or Postgres, and nothing is added to the request path. The tier of
an existing deployment is unchanged, and a deployment with no price book keeps
serving exactly as before. Propagating a resolved price into a request receipt
and settling it durably is issue #155 and will declare its own tier.

## Consequences

Approving pricing becomes a deliberate act with an audit trail, and it is more
work than importing: a deployment that has approved nothing has no approved
prices at all. Absence has to stay legible rather than collapse into zero, which
is why the snapshot models a missing price as `None` and not as a free target;
"nothing approved this" is a state operators can reach and must be able to see.

Exact conversion makes some published rates unapprovable as stated. An operator
who wants `$0.26666667` per million tokens must approve a rate the runtime can
bill, and the refusal says so. In exchange, no charge is ever a rounded
approximation of what was approved, and two replicas cannot disagree about a
price because they rounded differently.

Effective dating adds a clock to compilation, and this slice resolves against it
*once* per compilation. Each snapshot carries the interval over which its
resolution remains the answer. The convergence layer arms a control-plane timer
from that interval and recompiles the durable revision at the exact half-open
boundary, including when the control plane is otherwise idle; it never puts a
price-book lookup or clock read on the request path. A host clock that is not on
the timeline refuses the candidate rather than pricing at an invented instant.
See [ADR 0059](./0059-effective-dated-pricing-activation.md) for the scheduler,
restart recovery, and failure semantics.

Pricing rides on the snapshot, so whatever publishes a snapshot decides whether
it has prices — and the file reloader publishes snapshots too. `axond.toml`
describes no price book, so a reload carries the pricing already active onto its
candidate rather than building a snapshot without it: a `SIGHUP` after
convergence must not be how a priced deployment stops being priced. Carrying it
forward is the conservative half of the seam; deciding when convergence
*replaces* pricing, and recompiling at an effective-date boundary, belongs to the
convergence/serve wiring (#142).

Refusing a book refuses the whole revision, including its routing changes. A
typo in a rate therefore blocks an unrelated alias rename published in the same
revision. Revisions are the unit of atomicity, and partially applying one is the
alternative, so this is the trade the revision model already makes; the refusal
names the target and the rate to make it a short loop.
