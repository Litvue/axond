# 36. Derived availability and discovery evaluation

Date: 2026-08-12

## Status

Accepted

Fixes the availability stance the catalogue, entitlement, enablement, credential,
and discovery slices of [ADR 0027](./0027-stateless-and-stateful-operating-modes.md)
will each contribute a dimension to, and inherits the snapshot and reload
semantics of [ADR 0002](./0002-stateless-by-default-stateful-by-opt-in.md) and
[ADR 0011](./0011-config-hot-reload.md).

## Context

"Is this model available to this tenant?" is about to be asked by several slices
that arrive separately: a catalogue projection, an enablement reference, an
entitlement answer from a provider account, a deployment policy, a discovery
mechanism that lists or probes a provider, and the per-target circuit breaker of
[ADR 0008](./0008-target-failover-and-circuit-scope.md). If each of them answers in its
own vocabulary, the deployment ends up with several disagreeing notions of
availability and a request path that consults whichever one is nearest.

Two failure modes are worth deciding against before there is an implementation to
preserve. The first is **collapsing uncertainty into a decision**. A discovery
listing that broke halfway, a provider that offers no listing mechanism, an
entitlement nobody has established yet, and a policy that could not be evaluated
are all *not knowing*; rendering them as "not available" turns one failed refresh
into a fleet-wide denial of a model a tenant pays for, and rendering them as
"available" routes traffic at a target nobody established exists. The second is
**letting derived evidence become desired-state truth**. Availability changes
continuously, is partly replica-local, and is partly a provider's opinion;
allowing it to write back into the catalogue or a revision would let one replica's
bad afternoon retire a model, and would put a discovery lookup on the inference
path.

Redaction is the third pressure. Everything a discovery probe learns is unsafe by
default — a provider error body, a credential, a DSN, a policy expression — and a
tenant asking about its own targets must not learn how the deployment performs
discovery or how one replica is faring.

## Decision

**Availability is derived, and it is a verdict, not a fact.** An
`AvailabilityIndex` is built from dimensions other slices own, is immutable once
built, and is carried *beside* a `ConfigSnapshot` rather than inside its config:
`ConfigSnapshot::with_availability` consumes the snapshot and cannot add a model,
a namespace, or a credential. Refreshing availability publishes a replacement
index exactly as a reload publishes a replacement snapshot, so no reader observes a
half-updated one. Nothing is written back: the catalogue and desired state decide
what exists, and an index only says what is currently reachable.

**Five states, because three of the situations two states would merge need
different actions.** `available` (definitive, unexpired positive evidence),
`unknown` (no usable evidence either way), `stale` (positive evidence past its
expiry), `denied` (an authority, or complete discovery, said no — stable until
somebody changes it), and `unavailable` (it is not in this deployment's catalogue,
or this replica is skipping it). `stale` is deliberately not a flavour of
`available`: routing to a target whose evidence expired during a discovery outage
is a decision a deployment is entitled to make either way, and reporting it as
`available` both takes that choice away and hides the outage.

**Six independent dimensions, and one fixed ladder.** Catalogue presence,
enablement, entitlement, policy, and runtime health are single-valued; discovery
is the only one with observations, sources, timestamps, and expiry. They stay
separate because each has a different authority, lifetime, and repair — "the
operator has not enabled this", "your provider account is not entitled", and "this
replica's circuit is open" need three different people to act. Evaluation walks
them in a fixed order — catalogue, policy refusal, enablement, policy
indeterminacy, entitlement, an open circuit, discovery, runtime impairment — and
records which rung decided, so the answer names who to go and talk to rather than
only what the answer is. Policy is split across two rungs because a deployment's
refusal outranks a tenant's switch while a deployment's *inability to decide* must
not: every rung that can answer `unknown` sits below enablement. Runtime health is
split for the same reason in the other direction: an open circuit is this replica's
refusal and outranks the evidence, while impairment short of tripping only *lowers*
a positive or uncertain verdict to `unknown` (the breaker would still attempt the
target, [ADR 0008](./0008-target-failover-and-circuit-scope.md)) and leaves a
conclusion the evidence reached standing — local flakiness is no reason to stop
reporting that a complete listing no longer carries the model, still less to make
that target attemptable again.

**Completeness is a separate question from the result, and only a complete
negative may deny.** A `DiscoveryObservation` carries its result, its
completeness, its source, when it was observed and when it expires. `absent` plus
`complete` is the *only* definitive negative; partial, unsupported, and unreliable
coverage is `unknown` with a reason naming which, because a listing that broke
halfway is the most tempting way to deny a tenant a model it pays for. Expiry only
ever moves away from confidence: an expired positive becomes `stale`, and an
expired denial becomes `unknown` rather than continuing to deny, so one listing
taken once cannot outlive every attempt to refresh it.

**A discovery outage costs freshness, not access.** The last definitive positive
observation is retained across non-definitive ones, so an outage degrades to
`available (last_known_good)` and then to `stale`, never straight to `denied`. What
is retained and what is currently held advance independently, so which of two
overlapping probes finishes first cannot change the result: the current slot keeps
the newest look, while retention is judged against a watermark of every *conclusive*
answer the target has ever reached — not the looks still held, because a definitive
negative retains nothing and an inconclusive refresh displaces it from the current
slot. A definitive look that lands after a newer *inconclusive* one therefore still
counts, while one that predates a conclusive answer overturns nothing in either
direction: an older negative does not discredit a later positive, and an older
positive does not resurrect a target a later complete listing dropped. Two looks
bearing the same instant resolve the same way whichever lands first — the negative
holds, because two answers about one instant are not evidence of reachability — and
only a strictly *later* look lowers certainty, so an inconclusive probe sharing the
instant of a complete listing cannot soften a denial into a routable `unknown`.
Declared evidence goes through the same retention and ordering path as an observed
look: a projection handing the builder a whole record has its listing retained for
the outage that follows, has a complete listing which dropped the target discredit
the retained positive, and cannot adopt a look older than the one already held.
Declared evidence is judged against the conclusion the *index* has reached rather
than the one the record carries itself, so a record read out of one index survives
being declared into another and an ordinary refresh reports nothing out of order.

**Uncertainty is routable only where a scope chose it.** `unknown` and `stale` are
not refusals, but routability is a property of the whole verdict:
`Availability::permits_attempt` refuses a verdict decided by `NoRecord`, so an
index that is empty, still loading, or missing a key permits nothing. Every other
`unknown` is one the ladder let past its policy and enablement rungs — that is, one
an operator explicitly enabled. Certainty is ordered and asserted, so no merge of
observations can raise it without new definitive evidence.

**The verdict has nowhere to put free text.** Every field is an enum, a bool, or a
timestamp, and the reason vocabulary is closed, so redaction is the absence of a
field rather than a filter someone can forget to apply. The operator-facing detail
a probe collects lives on the internal observation and has no projection path.
A namespace-scoped verdict additionally coarsens the reasons that describe the
deployment's own machinery — how discovery is performed, how this replica is
faring — to `unspecified` and drops the discovery source; what a tenant keeps is
its own state, whether it rests on last-known-good evidence, and when that
evidence expires.

**A reload re-projects availability or re-derives it; it never inherits it
silently.** `ConfigSnapshot::build` always yields the empty index, and
`with_availability` is consuming, so a rebuild carries evidence forward only by
attaching the outgoing snapshot's handle (`ConfigSnapshot::availability_handle`)
at the call site. That is the point rather than a gap: evidence is derived against a
particular catalogue, credential set, and set of namespaces, and a reload of
[ADR 0011](./0011-config-hot-reload.md) may change any of them, so inheriting an
index would carry verdicts about targets the new config no longer declares. The
reload path chooses, visibly, between re-deriving and re-projecting.

### State tier

Tier 0. The index is an in-memory `BTreeMap` carried beside a snapshot, evaluation
is a pure function over data already in hand — no I/O, no lock, no lookup — and
nothing is persisted or polled. A stateless deployment carries an empty index,
which permits nothing and denies nothing. At Tier 1 and Tier 2 the same contract
is filled by the projections the store-backed slices bring with them, and the
evaluation path does not change: wiring evaluation into admission later cannot turn
an inference request into a catalogue, discovery, Postgres, Redis, or `SecretStore`
read.

## Alternatives considered

- **Two states, available and not.** Merges "we know you cannot", "we do not
  know", and "we knew, a while ago". The three need different operator actions and
  license different routing decisions.
- **One boolean per dimension, ANDed together.** Cheap, and it loses which
  authority objected — the single most useful thing an operator wants from the
  answer — and makes an undecided policy indistinguishable from a denial.
- **Treat a target missing from any listing as absent.** A partial listing is the
  common case during a provider incident; this is how a refresh failure becomes a
  denial for a whole fleet.
- **Store availability in the catalogue or a revision.** Availability is partly
  replica-local and changes continuously; writing it back makes one replica's
  circuit breaker into a durable fact and puts derived state on the desired-state
  path.
- **Let discovery evidence add targets.** Then a provider's listing decides what a
  deployment offers, and a catalogue is advisory. Presence is a precondition here,
  never a consequence.
- **A single free-text reason, redacted by best effort.** Redaction by pattern is a
  treadmill and the leak lands on an operator surface. A type with no free-text
  field cannot leak.

## Not decided here

- No provider is polled, no observation is persisted, no admission or `/v1/models`
  behaviour changes, and readiness does not read an index: this slice fixes the
  contract and constructs nothing at runtime.
- How a deployment routes `unknown` and `stale` — attempt, or refuse — is a
  routing policy, and so is whether an operator may pin a target's availability by
  assertion.
- Where each dimension comes from: the catalogue, enablement, entitlement, and
  credential slices each own their projection into a record.

## Consequences

- The dimensions can land one slice at a time without any of them being able to
  invent a second availability vocabulary, and without a slice being able to make
  availability authoritative over desired state.
- Verdicts are approximate by construction and carry `observed_at`/`expires_at` so
  the approximation is visible. A target that just started failing reports what the
  last observation said until the next one arrives.
- The reason and state vocabularies are a compatibility surface once anything
  projects them into a response; new codes are additive under the
  [0.x policy](./0015-zero-dot-x-compatibility-policy.md), and a caller must treat
  an unrecognised code as opaque.
- Wiring evaluation into admission is a change to what a request may do, so it
  fires the catalogue and model-entitlement trigger of the
  [security review checklist](../security/threat-model-review.md) again on its own
  merits.
