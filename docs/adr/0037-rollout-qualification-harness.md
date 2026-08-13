# 37. Multi-replica rollout qualification and its result artifact

Date: 2026-08-13

## Status

Accepted

Extends the capacity harness of
[ADR 0033](./0033-capacity-qualification-harness.md) from *one replica under
load* to *a fleet being replaced*, and turns the rolling-upgrade and rollback
procedure of [upgrades and rollback](../operations/upgrades.md) into something
that is executed rather than described.

## Context

The shutdown work of the runtime slice proved a great deal about one process:
readiness fails at once on `SIGTERM`, an admitted request finishes, a stream
still open at the deadline is cut and still accounted for, and usage flushes
before exit. Every one of those tests holds a single gateway and drives it
directly.

An upgrade is not a process shutting down. It is a *load balancer* deciding to
stop routing to a replica, on the strength of a readiness probe, while callers
are mid-request — and then a second replica of a different revision picking up
the traffic the first one no longer takes. The properties an operator depends on
during a rollout live in the gap between those two processes: that no caller is
routed to a replica the balancer has already seen drain; that a request in flight
when the signal arrived still gets an answer; that the replacement takes traffic
before the outgoing one stops taking it; that nothing is silently dropped from
the usage ledger on the way out. None of that is observable from inside one
process, and none of it was covered.

The documented procedure also makes two claims about *state* that no test made:
that an ordinary compatible patch rollback is allowed, and that rolling a build
backwards onto a control plane a newer build has migrated is not. The second is
the one that costs money when it is wrong.

The obvious shortcut — a test-side function that picks a base URL — would not
qualify any of this. A chooser cannot be routed to after it has withdrawn a
member, because it has no independent view of readiness to be wrong about, and
it cannot relay a stream that gets cut, because it never held one.

## Decision

A rollout harness that runs a fleet of real `axond` processes behind a real HTTP
load balancer, driven by a committed manifest, producing a machine-readable
result artifact per scenario run.

**The balancer is a real proxy.** `Ingress` is an Axum server on its own
loopback port. It polls `/readyz` on every member, admits a member only after a
probe succeeds, forwards round-robin over the ready set, relays streaming bodies
without buffering them, retries a typed `draining` refusal onto another member,
and answers `503` when no member will take the request. It stamps the replica and
revision it chose onto every response, which is what makes "who served this
request" a recorded fact rather than an inference. Because it holds its own
readiness view, `forwards_after_withdrawal` is a number that *can* be non-zero —
so a run asserting it is zero has asserted something.

**The subjects are real processes at two revisions.** A revision is the (binary,
config) pair a process was started from. The incoming revision serves an alias
the outgoing one has never heard of, so the harness can prove a mixed-version
window is genuinely mixed: the same request is answered `200` by one replica and
`404` by another, at the same moment, through the same balancer. This is the
shape of the mixed-version rule in the upgrade guide. It is *not* a second build:
the artifact records `distinct_binary: false` rather than implying that two
compilations were compared.

**The drain is observed from both sides at once.** A buffered request and a
stream the upstream never ends are pinned directly to the victim replica and
confirmed to have reached the upstream *before* `SIGTERM` is sent, so the drain
is known to have found work in flight rather than hoping it did. The harness then
watches the balancer's withdrawal and the child's exit concurrently, while a full
traffic phase runs against the fleet. What that yields per drain: when readiness
was removed, how much traffic reached the replica after it was removed, whether
the buffered request finished and with what usage status, when the stream was cut
and how many bytes had been relayed, and how many usage records the replica
flushed on its way out.

**Both rollbacks are exercised.** The compatible one is performed: a
previous-revision replica is admitted, a next-revision replica is drained, and
the artifact records that the older build then served real traffic. The
prohibited one is *evaluated* against a real PostgreSQL — a control plane is
migrated, a ledger entry only a newer build could have written is inserted, and
the harness records that this build refuses to serve it and that the refusal
names a newer gateway. Without a database the fence is recorded as
`evaluated: false` with a reason, so a skipped fence can never read as a passing
one.

**The operator gate runs before the fleet does.** `axond check preflight` and
`axond migrate status` are run as subprocesses against the incoming revision's
own config file, in the order the upgrade guide gives, and the rollout refuses
to start if either fails. Their argv and their operator-visible output are kept
in the artifact; no DSN or secret value is.

**The artifact carries the capacity harness's provenance.** Same SHA-256 helpers,
same hardware, toolchain, and source blocks, so a rollout result and a capacity
result from one commit describe the same build with the same digest. On top of
that it carries the loss ledger, the per-phase traffic split by replica, the
drain records, the mixed-version evidence, the migration and rollback evidence,
and a chronological timeline of every rollout event.

**Only environment-independent properties are hard failures.** The gates are:
nothing routed to a withdrawn replica, every offered request answered, no `503`,
one usage record per request with none lost, readiness removed within its bound,
a replacement admitted within its bound, termination inside
`drain_grace_ms + deadline_ms + flush_timeout_ms`, the mixed-version window
genuinely mixed, the pinned buffered request completed, the pinned stream cut
inside the deadline and accounted for as partial, the migration gate passed, and
the compatible rollback serving. Throughput and latency are recorded and never
asserted, for the reason ADR 0033 gives.

**Both tiers are the same code.** The reduced tier runs inside
`cargo test --workspace`. The heavy tier is the identical driver, manifest, and
assertions at a scale that wants a runner to itself: `AXOND_ROLLOUT=1`, run by the
`Rollout` workflow on dispatch and weekly.

### State tier

Tier 0 for the fleet, Tier 2 for the fence. The replicas are config-only
processes needing no service container, so the rollout scenario runs in the
hermetic suite. The forward-only rollback fence needs a real PostgreSQL and is
skipped — visibly, in the artifact — when `AXOND_TEST_POSTGRES_DSN` is unset.

## Consequences

- The rolling-upgrade procedure in the upgrade guide is executable. A change that
  makes a draining replica stay in a balancer's ready set, that drops an admitted
  request when the signal arrives, or that loses a usage record on the way out
  fails on the pull request rather than during someone's deployment.
- The drain bound an orchestrator's `terminationGracePeriodSeconds` is set from is
  measured against a real process on every run, so the documented sum is evidence
  rather than arithmetic.
- The mixed-version claim is proven by capability, not by version string. That
  covers the property that matters — two revisions serving different catalogues
  through one balancer — and does not cover anything that would need two
  compilations, such as a wire-format change between builds.
- The fence is the harness's only stateful dependency, and it is the one piece
  whose absence is easiest to misread. It is therefore recorded explicitly as
  skipped, and the threshold it feeds only passes when it was actually evaluated
  or explicitly not required.
- A rollout run boots several processes and drains them at their real deadlines,
  so it costs wall-clock time proportional to the shutdown bounds, not to the
  traffic. The manifest's bounds are chosen so the reduced tier stays inside a
  normal test run.
- The ingress is a fixture, not a product. It is representative of a
  readiness-driven load balancer and deliberately simple: round-robin, one retry
  onto another member, no outlier ejection, no connection draining of its own. A
  production balancer's own behaviour is not qualified here.
