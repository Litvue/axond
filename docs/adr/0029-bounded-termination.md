# 29. Bounded termination: readiness drain, request deadline, and flush budget

Date: 2026-08-12

## Status

Accepted

Gives the streaming relay of [ADR 0005](./0005-streaming-relay.md) and the
buffered usage sinks of [ADR 0009](./0009-durable-usage-sinks.md) a defined
end of life, and completes the per-phase bounding of
[ADR 0028](./0028-transport-phase-bounds.md) at the process boundary.

## Context

A rolling deployment stops a replica by sending `SIGTERM` and waiting a fixed
grace period before `SIGKILL`. Axond had nothing between those two events: the
process ended when its runtime did. That loses two different things.

Traffic. A load balancer removes a replica by *observing* a readiness endpoint,
not by being told, so there is a window between the signal and the routing
change in which requests still arrive. `/readyz` reported nothing about
termination, so those requests were accepted by a process about to disappear and
failed as connection errors.

Money. In-flight streams and buffered usage rows are the gateway's record of
what a caller actually spent. `axum`'s graceful shutdown waits for every
connection with *no bound of its own*, which is precisely the unbounded wait a
grace period resolves with `SIGKILL` — after which nothing flushes at all. So
either the process exits immediately and drops buffered spend, or it waits
forever and gets killed before writing it. Both lose revenue-bearing records,
and neither is observable.

The naive fix — one shutdown timeout — cannot work, because the three waits are
not interchangeable. Time spent waiting for a load balancer must not be charged
to a caller's request, and time spent waiting for a caller's request must not be
charged to the flush that records it.

## Decision

Termination is an explicit, monotonic sequence of process-local phases
(`Serving` → `Draining` → `Closing`), each with its own configured bound in a
new `[shutdown]` section: `drain_grace_ms`, `deadline_ms`, and
`flush_timeout_ms`.

**Readiness drains before admission closes.** The first signal moves the process
to `Draining`, where `/readyz` answers `503 draining` while the replica *keeps
admitting and serving work* for `drain_grace_ms`. Failing readiness and refusing
requests are separate events, deliberately: doing them at once would refuse
exactly the requests routing has not yet stopped sending. A second signal in
that window closes admission at once, for the operator who knows routing has
already caught up; with `drain_grace_ms = 0` there is no window and the first
signal closes admission, so no second signal is needed to make progress. Past
the close, signals are logged and otherwise ignored — the remaining bounds
already cap termination, and honoring one there would kill the process mid-flush
and discard the records the sequence exists to write.

**Liveness never fails.** `/healthz` answers `ok`, unauthenticated, for the
entire sequence. A terminating replica is not a wedged one, and failing liveness
would only earn it the `SIGKILL` this design exists to avoid. Only readiness
reports the drain.

**Admission is a request-lifetime guard, not a middleware span.** The guard is
held by the *response body*, so a streamed or SSE response is still counted as
in flight until its last chunk is sent or the body is dropped. Releasing it when
the handler returned would report a replica as quiesced while it was still
relaying tokens.

**The deadline actively ends work rather than waiting for it.** Requests
admitted before the close get `deadline_ms`. A response that has begun is *told
to end*: the body returns an error, which drops the upstream stream and settles
it through the ordinary cancellation path, so the usage record is written as
`client_cancelled` with spend measured up to the last relayed token. The signal
reaches responses, not handlers — a request still inside its handler (a buffered
completion waiting on an upstream, bounded by `failover.overall_timeout_ms`) has
no body to end yet, so it is bounded only by the deadline and the waits that
follow it, and is cut by process exit with nothing settled if it outlasts them.
Carrying cancellation into the handler path is a follow-up.

Dropping the server future instead of ending bodies would settle nothing at all:
`hyper` serves each connection on its own task, so those connections outlive the
future and are torn down only with the runtime, too late for anything to settle.
A truncated response ends in an error rather than cleanly, because a clean end
would present a partial answer as a complete one.

**One flush budget covers everything after serving.** Inside a single
`flush_timeout_ms`, measured as one absolute deadline: abandoned responses are
given a moment to end, their settlements are awaited, the buffered usage sinks
are flushed in order, and the telemetry exporters flush with whatever remains.
Each step waits only for the time left, so the budget is the total, not a
per-step allowance. Records that cannot be written are counted as `shutdown`
drops (sink failures as `sink_error`), because a lost row must be a number an
operator can alert on rather than a silent gap.

**The bounds are read when the signal arrives, and enforced as one snapshot.**
A reload therefore applies to the next termination, and the deadline and flush
budget are the same values the drain logged — not whatever `[shutdown]` said at
boot.

**Worst-case termination is the sum of the three**, which makes the operator's
obligation checkable: `terminationGracePeriodSeconds` (or `TimeoutStopSec`, or
`stop_grace_period`) must exceed `drain_grace_ms + deadline_ms +
flush_timeout_ms`, and the shipped defaults (5s + 15s + 5s = 25s) fit under the
shipped supervisor grace of 30s. No wait in the sequence is unbounded.

Stateful control-plane behavior is untouched: nothing here reads or writes a
control plane, and no reconciliation, lease, or revision handling changes.

### State tier

Tier 0. The phases, counters, and bounds are process-local; `[shutdown]` is
process-local configuration validated in both operating modes (ADR 0027), and
`/healthz` and `/readyz` remain Tier 0 and unauthenticated. Tier 1 (Redis) and
Tier 2 (Postgres) deployments flush the same sinks they already configured and
keep their existing state choices; no existing deployment's tier is raised.

## Consequences

- A rolling deployment can be made lossless by configuration alone: the drain
  window is what removes the replica from routing before anything is refused.
- Termination takes longer than it used to. A deployment whose supervisor grace
  is below the sum must raise it or lower the bounds; otherwise `SIGKILL` lands
  mid-flush and the phase at which it happened is visible in
  `axond.shutdown.phase`.
- A very long stream can be cut short by `deadline_ms`. That is the trade being
  made — the alternative is an unbounded wait — and the cut is accounted for:
  `client_cancelled` with partial spend, plus
  `axond.shutdown.abandoned_requests`.
- Callers can observe one additional error, `503 draining` with `Retry-After: 0`,
  during the close. It is additive and retryable at another replica.
- Buffered usage loss is now bounded and counted rather than silent, but it is
  not eliminated: a sink that stays unavailable for the whole flush budget still
  drops its rows, as `shutdown` drops.
- Shutdown diagnostics name signals and phases only. No endpoint, DSN, URL, or
  credential appears in a shutdown log line.
