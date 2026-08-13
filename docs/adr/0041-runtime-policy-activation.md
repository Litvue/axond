# 41. Runtime policy activation, generations, and hold protection

Date: 2026-08-13

## Status

Accepted

Activates the documents typed by
[ADR 0036](./0036-typed-policy-documents-generations-and-transitions.md), on the
namespaces projected by tenancy, through the convergence loop
[ADR 0027](./0027-stateless-and-stateful-operating-modes.md) defines, against the
responsibility-specific backends
[ADR 0010](./0010-shared-budget-backends-and-charging-policy.md) and
[ADR 0017](./0017-state-tiers-and-optional-backends.md) already select.

## Context

ADR 0036 deliberately activated nothing: it typed a policy document, defined a
generation, and classified what a change would require of a fleet. Every value
a request path actually reads — the spend cap, the reservation TTL, the
concurrency ceiling, the lease TTL, the token floor — still came from
`axond.toml`, so changing any of them meant editing a file on every replica and
restarting the fleet.

Turning a published document into enforcement raises questions the typed
document does not answer on its own:

- **Where do the values enter the request path?** The stores that enforce them
  are built at boot, hold connections, and are shared by every request. Rebuilding
  one to change a number would drop pooled connections and, worse, discard the
  in-flight accounting it holds.
- **What happens to what is already admitted?** A reservation was granted under a
  cap and a TTL; a lease was granted under a ceiling and a lease TTL. Applying a
  tighter document to them retroactively would shorten a lease someone is holding
  and change the terms of a settlement already in flight.
- **What does rolling back mean?** An operator who wants yesterday's limits back
  wants the values back. Pointing a fleet at yesterday's *revision* would make one
  epoch name two documents across a partly-rolled fleet, and the fence could no
  longer tell a stale writer from a current one.
- **Which backends may be told to enforce a published cap at all?** A fleet-wide
  cap enforced by a per-replica map is not a cap; it is N caps. And the control
  plane's own Postgres is a durable revision journal that every replica already
  connects to, which makes it exactly the tempting wrong answer for hot state.
- **What must a stateless deployment notice?** Nothing. That is the requirement.

## Decision

A replica holds one `PolicyRuntime`: the policy it is enforcing right now, plus
the holds outstanding under each generation that granted one.

### Values reach the request path by being read, not by being rebuilt

The runtime holds an `ArcSwap<PolicyView>`; the budget store and the rate limiter
hold a `Ceilings` handle onto it and read the caps governing a namespace **per
admission**. A publication is one atomic store of a whole view, so a request
reads one coherent document or the previous one, never a mix of two. Backends,
DSNs, key prefixes, tables, connection pools, and the unavailability stance are
untouched by a publication — they remain bootstrap-owned, exactly as ADR 0036
requires, and no publication reconnects anything.

A namespace no document governs has **no enforceable cap**, and a store that
cannot answer "what is the cap here" denies rather than admitting: an unenforced
cap and an infinite one are indistinguishable to a caller, and only one of them
is something an operator published. A publication is not allowed to create that
state — a candidate serving a namespace no document governs is refused before it
is installed — so the denial belongs to a bootstrap gap, not to a revision. In
stateful mode the bootstrap file cannot declare a namespace at all, so "governed
by a published document" is the only way a namespace is served. The condition
belongs to the view rather than to the request, so it is counted per denial on
`axond.policy.unenforceable_denials` — apart from the unavailable-denial
counters, because the store is healthy and only the limit is missing — and
explained in the log once per condition, backend and namespace, and at most once
a minute thereafter: a bootstrap gap under production traffic must not turn the
log into the outage.

### A hold carries the generation that granted it

Every reservation and every shared concurrency lease is stamped with the
generation active when it was admitted, and the runtime counts outstanding holds
per generation: entered before the store call that admits, released if it denies,
and otherwise exited exactly once at settlement, release, or drop. The count is
deliberately conservative — a publication landing mid-admission may see a hold
for a request about to be turned away, but never misses one that succeeded, so
"no hold names the superseded generation" is the statement a stop-the-fleet
migration can be gated on. Two consequences follow structurally rather than by
convention:

- Terms are not rewritten under a running request. A lease releases on the TTL it
  was granted, whatever the current document says.
- "The drain has finished" is an observable fact — a superseded generation with a
  zero count — rather than a guess about elapsed time.

Bootstrap (stateless) admissions carry no generation, so nothing accounts them.

### Activation is planned before it is applied, at the convergence sink

A candidate revision is projected into a config whose namespaces carry policy
documents (`PolicyProjection` over `TenancyProjection`), and the convergence
sink is asked to *admit* it before anything is published. The plan answers three
questions in order — can these backends enforce it, does it fit the durable
layout this process booted on, and is this a transition this model performs —
and any refusal drops the candidate whole: same snapshot, same policy, an
ordinary rejection with its own reason. A replica therefore never half-applies a
policy, and an outage or a refusal leaves last-known-good enforcement in place.

`live` and `drain` activate; `migration-required` and `refused` do not.

Which document governs a namespace may itself change — a project publishing over
its tenant's, or dropping its own so the tenant's applies again. That is a
handover: the namespace stays governed, so it is not a withdrawal, and it is
classified against the values it displaces so a tightening handover drains rather
than reporting as a scope's first binding — in both directions, so `draining` is
complete. Epochs are not compared across it, because an epoch orders one scope's
own publications; the values are, in full, so a handover cannot lower a token
floor an operator raised to revoke credentials. Changing which document states a
value is not a way to make a refused change performable. A scope hands over every
namespace it governs at once, and the documents taking them over need not strand
the same thing, so the drain reasons reported for it are the union across those
namespaces — sorted and de-duplicated, so two replicas classifying one
publication report the same list.

### Rolling back is publishing the old values forward

A rollback is a new document, a higher epoch, the previous values. It is
classified like any other change — usually a drain, since it lowers something —
and the holds taken under the higher caps finish on the terms they were granted.
Walking the epoch itself backwards is refused, and so is a second document
claiming an epoch this replica already has with different content: that is a
forked writer, and it fails closed in both directions.

### Only backends whose semantics meet the contract may enforce a document

A published spend cap requires Redis or Postgres *selected as the budget
backend*; a published concurrency ceiling requires Redis leases. Both, always:
`axond.policy.v1` makes `max_in_flight_per_subject` and `lease_ttl_seconds`
required (ADR 0036), so there is no spend-only document, and a deployment
without shared leases cannot publish policy at all. Supporting one would mean an
optional concurrency block in the contract — a change to ADR 0036, deliberately
not made here: a policy that silently enforces half of what it states is the
ambiguity the typed contract exists to remove. The in-memory
and no-op backends cannot enforce a fleet-wide statement and are refused with the
bootstrap change named. Whether the budget store's keys carry a scope-wide cap is
a durable layout fact: a document that turns one on or off is refused as a
migration, before publication, rather than half-enforced against keys laid out
for the other shape.

**The control plane's Postgres is never reused as request-path state.** The
budget store's Postgres backend is a separate, explicitly configured DSN with its
own ledger tables and its own migrations; nothing derives a hot-path connection
from `[control_plane]`. Sharing that instance is an operator's deliberate choice
about one database's capacity, never an implicit consequence of enabling
stateful mode.

### Stateless mode is untouched

With no control plane there are no revisions and no documents: the runtime boots
from `axond.toml` and never replaces its view, holds carry no generation, and
every limit is the file's, forever. Stateful values are not expressible in TOML
and TOML values are not publishable, so neither mode can quietly acquire the
other's behaviour.

### State tier

Unchanged. Enforcement of a published document requires the Tier 1 (Redis) or
Tier 2 (Postgres) budget backend and the Tier 1 rate limiter that a deployment
already selected in its bootstrap file; a deployment that has not selected them
is refused a publication rather than silently upgraded, and Tier 0 keeps the
file's limits with no new dependency.

## Consequences

**Limits change without a redeploy, and revocation propagates fleet-wide.** A
published `minimum_token_epoch` raises the configured namespace and per-subject
floors rather than replacing them, so a stale, more specific file entry cannot
undercut a revocation — the direction that must always take effect.

**A drain is bounded by work, not by a timer.** Outstanding holds are counted and
reported per generation, so an operator can see what a tightening is waiting on.
The cost is that a long-lived stream keeps its generation alive, and a fleet
mid-drain has two generations enforced at once — deliberately, since each request
keeps the terms it was granted.

**A refusal is an operator's problem, stated once.** Because activation is
planned before publication, an unsupported backend, a layout mismatch, or a
forked epoch surfaces as one refusal naming the fix, rather than as divergent
enforcement discovered later from usage records.

**Publishing an unenforceable cap is impossible, and publishing an ungoverned
namespace is too.** A namespace with no document is refused spend rather than
granted an infinite budget — and because the revision that would introduce one
is refused first, that denial is not something an operator can publish their way
into. The cost is a stricter ordering: a tenant-level floor has to exist before
its namespaces are projected.

**Security review outcome.** Trigger 1 fires: revocation is now enforced from
published state, since `minimum_token_epoch` raises the token epochs the verifier
applies. It is held by monotonicity — a floor may only raise a configured epoch,
never lower it — with `a_published_revocation_floor_raises_every_epoch_it_covers`
holding that property. Trigger 2 fires: the caps a request is admitted under are
tenant- and project-scoped, held by whole-document selection (ADR 0036) and by
denying any namespace no document governs, with
`shared_settings_read_the_caps_the_runtime_is_publishing_now` and
`a_policy_these_backends_cannot_enforce_is_refused_before_anything_is_published`
holding it. Trigger 5 is not fired: no migration ships, no table changes, and the
scope-wide-cap layout change is refused rather than performed. No `expose_secret`
call site is added, and no credential or secret material is read.

## Alternatives considered

- **Rebuilding the stores on each publication.** Simple to write, and it discards
  connection pools and in-flight accounting on a routine limits edit, turning a
  number change into a reconnect storm.
- **Applying new terms to existing holds.** It makes "what is enforced" a single
  value, and it retroactively shortens leases and reprices settlements that were
  already granted — the failure the generation stamp exists to prevent.
- **Rolling back by repointing the fleet at an older revision.** It reads as the
  obvious rollback, and it makes one epoch name two documents across a partly
  rolled fleet, which is precisely what the fence cannot tolerate.
- **Reusing the control-plane Postgres for budget ledgers.** One less DSN to
  configure, and it couples request-path availability to the revision journal and
  makes a control-plane incident a spend-enforcement incident.
- **Letting an ungoverned namespace fall back to the bootstrap file.** Friendlier
  during a rollout, and it silently serves a deployment's own limits to a tenant
  an operator believes is capped by the control plane. In stateful mode the file
  has no namespaces to fall back to in any case.
- **Publishing the revision and letting the ungoverned namespace 503.** It keeps
  the revision and the enforcement decision separate, and it makes an ordering
  mistake in the control plane cost a tenant's traffic instead of costing the
  operator one refusal they can read and fix.
- **Making the concurrency block optional so a spend-only deployment can publish.**
  It fits the deployment that only wants budgets, and it makes a document's
  meaning depend on which backends the reader booted with — the ambiguity ADR
  0036 removed. If spend-only is wanted, it is a contract change there, not a
  runtime relaxation here.
