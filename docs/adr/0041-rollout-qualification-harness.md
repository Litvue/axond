# 41. Multi-replica rollout qualification and its result artifact

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
readiness view, the routing rule has a witness other than the driver.
`forwards_after_withdrawal` is zero by construction — selection reads readiness
and withdrawal under one lock, so a request chosen from a ready member cannot
land in it — and is recorded as the invariant it is. What a run actually asserts
is recomputed from the events instead: the dispatch instants the balancer
logged, compared after the fact with the withdrawal instant it recorded, where
the dispatch instant is taken when the request is actually handed to the
replica rather than when it was selected. A unit test produces exactly that race
— it holds a request at that seam, withdraws the member, and lets it go — so the
gate is known to be capable of failing rather than merely asserted to be.

The zero gate is `dispatches_beyond_drain_grace`, not every dispatch later than
the withdrawal. The two differ by the window the replica itself honours: within
`drain_grace_ms` admission is still open, so a hand-over the scheduler happened
to delay across the withdrawal instant is served exactly as it would be in
production, and gating on it would turn task scheduling at heavy scale into a
red run about nothing. Past the grace the replica refuses work, so a dispatch
there is a balancer that is still routing to a drained member. Both counts and
the worst observed lag are kept in the artifact, so the margin is visible rather
than inferred.

**The subjects are real processes at two revisions.** The promotable heavy lane
downloads and checksum-verifies the retained release and runs it beside the
candidate executable. A revision is the (binary, config) pair a process was
started from. Before candidate-only configuration is enabled, the candidate must
serve behind the fleet using the retained configuration. The incoming revision serves an alias
the outgoing one has never heard of, so the harness can prove a mixed-version
window is genuinely mixed: the same request is answered `200` by one replica and
`404` by another, at the same moment, through the same balancer. This is the
shape of the mixed-version rule in the upgrade guide. The reduced lane reuses the
candidate binary, records `promotable: false`, and cannot cross the promotion
boundary.

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

**Rollback follows the real migration matrix.** On a private PostgreSQL schema,
the retained binary applies and inspects its migrations, the candidate inspects
and applies its migrations, and the retained binary inspects the resulting
ledger. Exact version/name/checksum rows classify the result. An unchanged
layout permits a retained-binary traffic rollback; candidate-added versions must
make the retained binary refuse with the newer-gateway fence. No synthetic
future migration is used. Heavy qualification refuses to start without the
database. Raw artifact schema 5 also binds this matrix to a redacted structured
target: the control-plane environment-variable name, schema, and SHA-256 of the
exact bootstrap file supplied to every command. Promotion compares that target
with each digest-bound revision config, so a valid matrix from another schema
cannot satisfy the rollout gate.

**The operator gate runs before the fleet does.** `axond check preflight` and
`axond migrate status` are run as subprocesses against the incoming revision's
own config file, in the order the upgrade guide gives, and the rollout refuses
to start if either fails. Their argv and their operator-visible output are kept
in the artifact; no DSN or secret value is. That is enforced rather than
assumed: every value a command was given is replaced by the name it came from
before the output is retained, and anything URL-shaped is dropped whole, so a
failure path that echoes its environment cannot turn an uploaded artifact into a
credential.

**The artifact carries the capacity harness's provenance.** Same SHA-256 helpers,
same hardware, toolchain, and source blocks, so a rollout result and a capacity
result from one commit describe the same build with the same digest. On top of
that it carries the loss ledger, the per-phase traffic split by replica, the
drain records, the mixed-version evidence, the migration and rollback evidence,
and a chronological timeline of every rollout event.

**Usage loss is reconciled by exact caller trace identity at every revision.**
The retained v0.3.40 executable only copies inbound W3C trace context into its
usage row when an OpenTelemetry provider is active. The qualification topology
therefore gives every replica its own loopback OTLP/HTTP receiver. This is test
instrumentation, not a production dependency: it preserves the same exact
`(replica, trace_id, status)` join across the retained and candidate binaries.
The artifact records the complete exact-trace replica set, the number of
received trace batches, the replicas whose dedicated receiver decoded a
caller-domain trace, and every such trace. Readiness-only batches cannot prove
replica coverage. A receiver rejects a resource whose `service.instance.id`
does not name its owner. Promotion independently requires the exported
caller-trace set to equal the complete caller trace ledger and reconstructs
every sink join from the raw ledgers. Usage-bearing traces remain separate from
a canonical,
reasoned list of expected typed-drain refusals that deliberately owe no usage
row. Reduced raw diagnostics can additionally expose capability refusals, but
they are not promotable. Promotable drain exemptions are independently reconstructed
from the retained ingress attempt, including the refusing replica and the
replica that accepted the same caller trace. Caller-domain export activity must
remain quiet for five configured batch intervals before that exact snapshot is
serialized and judged; a duplicate caller span resets the window while an
unrelated readiness span does not. An unlisted or delayed extra decoded during
that bounded drain still fails; this is not an unbounded claim about activity
after the configured quiet window. A timeout, malformed export, or receiver
ownership error is serialized with the partial witness and fails the same
identity gate instead of aborting before diagnostics are written. A count-only
or status-multiset fallback is not an accepted proof
because it can hide one lost event behind an unrelated row.

**Only environment-independent properties are hard failures.** The gates are:
nothing routed to a withdrawn replica, every offered request answered, no `503`,
one usage record per caller request with none lost — reconciled replica by
replica, so a duplicate one replica wrote cannot stand in for the record another
lost — readiness removed within its bound,
a replacement admitted within its bound, termination inside
`drain_grace_ms + deadline_ms + flush_timeout_ms`, the mixed-version window
genuinely mixed, the pinned buffered request completed, the pinned stream cut
inside the deadline and accounted for as partial, the migration gate passed, and
the compatible rollback serving. Throughput and latency are recorded and never
asserted, for the reason ADR 0033 gives.

**Both tiers use the same driver but make different claims.** The reduced tier
runs inside `cargo test --workspace` as a same-binary diagnostic. The heavy tier
requires `AXOND_ROLLOUT_PREVIOUS_BINARY`, a distinct digest, and PostgreSQL; only
that tier is promotable.

### State tier

Tier 0 for the reduced diagnostic; Tier 2 for heavy qualification. Heavy runs
require a retained release artifact and real PostgreSQL. Missing either is a
hard refusal, not a skipped passing gate.

## Consequences

- The rolling-upgrade procedure in the upgrade guide is executable. A change that
  makes a draining replica stay in a balancer's ready set, that drops an admitted
  request when the signal arrives, or that loses a usage record on the way out
  fails on the pull request rather than during someone's deployment.
- The drain bound an orchestrator's `terminationGracePeriodSeconds` is set from is
  measured against a real process on every run, so the documented sum is evidence
  rather than arithmetic.
- The mixed-version claim is proven by both executable digest and observable
  capability, while the compatibility phase proves the candidate can consume the
  retained config before candidate-only configuration is enabled.
- The loopback OTLP receiver is part of the qualification topology so a retained
  revision cannot silently weaken exact usage-loss reconciliation. Production
  deployments remain free to configure their own collector or leave telemetry
  export disabled.
- PostgreSQL and a distinct retained binary are promotion prerequisites; their
  absence cannot be represented as a successful heavy result.
- A rollout run boots several processes and drains them at their real deadlines,
  so it costs wall-clock time proportional to the shutdown bounds, not to the
  traffic. The manifest's bounds are chosen so the reduced tier stays inside a
  normal test run.
- The ingress is a fixture, not a product. It is representative of a
  readiness-driven load balancer and deliberately simple: round-robin, one retry
  onto another member, no outlier ejection, no connection draining of its own. A
  production balancer's own behaviour is not qualified here.
