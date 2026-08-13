# 48. Stateful endurance: soaking a deployment, not a process

Date: 2026-08-13

## Status

Accepted

Extends [ADR 0040](./0040-endurance-qualification-harness.md) from *whether one
stateless replica is still the same process after half a day* to *whether a
deployment is still correct after half a day of change*. It keeps that ADR's
manifest-first shape, its bounded-accumulator rules, its provenance rules, and
its refusal to gate on throughput and latency.

## Context

ADR 0040's soak answers a real question about a single Tier 0 process: does it
give back the memory, descriptors, and sockets it took, and is every request
still accounted for exactly once. It deliberately answers it with nothing else
moving — no datastore, no control plane, no second replica, no configuration
change.

Production is the opposite. A deployment that runs for twelve hours is a
deployment whose catalogue was revised, whose credentials were rotated, whose
tenant policy changed, whose provider went slow and then away, whose usage
database was briefly unreachable, and which was restarted a replica at a time
for an unrelated deploy — all while serving. Each of those is separately
qualified elsewhere ([ADR 0037](./0037-recovery-qualification-harness.md),
[ADR 0041](./0041-rollout-qualification-harness.md)), for minutes, in isolation.
The failures this decision is about are the ones that need duration *and* change
together:

- a usage row that is owed by a replica which no longer exists, because the
  restart that replaced it happened between the request and the flush;
- a revision that converged on the replicas that were up when it was published
  and not on the one booted afterwards;
- a tenant that keeps borrowing a pool its policy withdrew, because the process
  that serves it never reloaded;
- a circuit that opens for a declared outage and, cooldown or not, never closes.

The hard part is not driving those events; it is being able to say afterwards
what each one cost. Under a workload that is deliberately failing part of the
time — cancelled callers, upstreams dying mid-stream, upstreams refusing — an
error, a missing row, and a refusal all have at least two plausible causes, and
an artifact that cannot tell them apart is not evidence.

## Decision

A second driver, beside the stateless one, sharing its manifest-first shape and
its accumulator discipline, and adding what a stateful run needs to attribute
what it sees.

**The script is committed as fractions of the run.**
`qualification/stateful-endurance/manifest.toml` places every event — three
revisions, an upstream latency window, an upstream outage, a usage-backend
outage, and a rolling restart — as a fraction of the offered duration, so the
one-minute smoke tier and the twelve-hour soak execute the same script in the
same order. A tier that ran a different script would not be a shorter run of the
same qualification, and a test asserts the two orders are equal.

**Declared faults never overlap, and attribution outlives them.** Two backends
out at once produce losses neither can be charged with, so the manifest's
windows are ordered and disjoint and a test keeps them that way. Each window
extends by `recovery_allowance_ms` for attribution, because the breakers in
front of a dead backend hold their cooldown after it returns; the extended
windows must not overlap either.

**Recovery is measured, not assumed.** For every declared fault the artifact
records `recovered_ms`: the time from lifting the fault to the first request
that settles a usage record afterwards. A window with no such request is treated
as infinite and fails `max_recovery_ms`. Excusing errors inside a window without
this gate would let a deployment that never came back pass.

**Durable loss is excused only by the fleet's own account of it.** The durable
sink drops a batch rather than queueing for a database that is not there
([ADR 0009](./0009-durable-usage-sinks.md)). The harness parses those drop
events out of each process's structured output and keeps them apart from its
bounded scrollback, then charges missing rows against them:
`max_durable_usage_loss_outside_windows = 0`, and loss inside the window is
allowed only as far as the processes reported dropping. Attributing loss by
comparing the harness's clock to a database's would excuse whatever happened to
land near a window.

**A retired replica's accounting stays in the run.** Rolling restarts are part
of the script, so a replaced process's flushed records and drop reports are
drained at retirement and folded into the same aggregates as the live ones.
Otherwise every restart would read as bulk loss.

**Revisions are observed by a request, not by a signal.** A revision converges
when a request sees the new behaviour — the new alias serving, a record
attributed to the rotated credential label, the probe tenant refused — which is
what an operator means by converged, and what a `SIGHUP` alone does not prove.

**The artifact carries labels, never material.** Tenant keys are files under the
run directory, the usage DSN travels as the name of the environment variable the
replicas read, the normalised config replaces ephemeral ports and the key
directory before hashing, and credential evidence is the attribution label.

**A run without a datastore is skipped, not shortened.** Where
`AXOND_TEST_POSTGRES_DSN` is unset the driver declines to run. A stateful
qualification whose stateful half quietly did not happen is worse than no run.

**The dispatched duration moves the soak tier only**, and the artifact records
the effective duration, the manifest duration, and which was used — the same
rule and the same reason as ADR 0040, under its own variable
(`AXOND_STATEFUL_ENDURANCE_DURATION_MS`).

### State tier

Tier 2. The harness boots real replicas against a real PostgreSQL usage sink and
creates an ephemeral schema for each run. It changes no shipped code path and
raises no deployment's tier; it does mean the smoke tier runs only in lanes that
have a database, which is the `Stateful tests` lane in CI.

## Consequences

- The stateless soak stays cheap, portable, and unconditional, and this one
  carries the cost of a datastore. Two drivers is the price of not making every
  contributor's `cargo test` need PostgreSQL.
- Every excuse the run makes for an error or a lost row is backed by something
  the deployment itself emitted — a gate event, a drop report, a recovery
  observation — rather than by a timestamp comparison.
- The smoke tier makes the harness's own regressions visible on every change,
  but it is a correctness run: a minute of traffic measures no envelope.
- The twelve-hour tier has not been dispatched. Until it publishes an artifact
  no long-run stateful envelope is claimed, and the qualification packet keeps
  the endurance slice short of `evidenced`.
