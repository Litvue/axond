# 30. Request bounds and per-replica load shedding

Date: 2026-08-12

## Status

Accepted

Bounds the inbound side of the request path that
[ADR 0028](./0028-transport-phase-bounds.md) bounded on the provider side, and
takes the per-replica half of the admission story that
[ADR 0027](./0027-stateless-and-stateful-operating-modes.md) leaves to the
stateful control plane.

## Context

ADR 0028 bounded what a provider may do to the gateway. Nothing bounded what
callers may do to it. A request body was read to whatever the framework would
accept, an authenticated caller could open as many concurrent requests and
streams as it could afford, and a stream that never stopped producing had no
lifetime at all — only `transport.stream_idle_timeout_ms`, which bounds silence
rather than duration. Every one of those is a way for a single tenant, or an
ordinary traffic surge, to exhaust process memory, sockets, or the runtime, and
the failure mode is the worst kind: the replica degrades for *everyone* instead
of refusing the traffic that caused it.

Existing controls do not cover this. The budget system bounds spend, not
concurrency, and it consults a store. The `[rate_limit]` control bounds one
subject's in-flight requests against a shared store, which is the right shape
for fleet-wide fairness and the wrong shape for self-defence: it is a dependency
call, it is keyed by subject rather than by what the process can hold, and under
`on_unavailable = "allow"` it does not answer at all when the store is down —
exactly when the replica most needs to protect itself.

## Decision

A new `[admission]` section defines two families of bound, both enforced
in-process with no store on the path.

**Per-request bounds** — `max_request_bytes` (enforced by the router before the
body is buffered), `max_prompt_tokens`, `max_output_tokens`,
`max_stream_duration_ms`, and `max_stream_bytes`. `max_stream_duration_ms` is
deliberately distinct from `stream_idle_timeout_ms`: the idle bound cannot end a
stream that keeps talking forever, and a total lifetime can. An output allowance
over the ceiling is *refused*, not clamped, so a caller is never silently served
a different request than it sent.

**Concurrency bounds** — `max_in_flight`, `max_in_flight_streams`,
`max_in_flight_per_tenant`, `max_tenants`, and an optional bounded queue
(`queue_capacity`, `queue_wait_ms`). Streams get their own, tighter ceiling
because a stream holds a socket and a relay task for the length of an answer,
which makes it the scarcer resource. `0` means "this ceiling is off" everywhere
except `max_request_bytes`, where zero would be a gateway that cannot serve.

The gates are taken in the order tenant → global → stream. The tenant gate never
waits, so a tenant at its own ceiling cannot occupy the queue other tenants are
waiting in; the stream gate is taken last, so a stream slot is only ever held by
a request that is about to open a stream rather than by one still queued for
capacity. Because the two sub-ceilings ship *below* the global one, an unset
sub-ceiling follows a lowered `max_in_flight` on load: turning one number down
must not fail boot over a default the operator never wrote. `max_in_flight_streams`
is clamped to it; a defaulted `max_in_flight_per_tenant` that reaches it is turned
off instead, because a tenant ceiling equal to the global one isolates nothing and
would shed at the same point through the gate that neither queues nor answers
`503`. Two written numbers that contradict each other still refuse to boot.

Admission is taken **after authentication and before the rate-limit store, the
budget reservation, and the provider call**. Ordering is the decision, not an
implementation detail: authentication stays fail-closed so unauthenticated
traffic can never consume capacity, and shedding costs no round trip, no
reservation, and no upstream — an overloaded replica must be *cheap* to refuse.

Refusals are typed and carry different meanings, so they carry different
statuses. A tenant at `max_in_flight_per_tenant` is `429
tenant_concurrency_exceeded`: the caller's own traffic caused it and the caller
can fix it. A saturated process is `503` (`gateway_overloaded`,
`stream_capacity_exhausted`, `admission_queue_full`,
`admission_queue_timeout`, `admission_tenant_capacity_exhausted`): the replica
is the problem. Size bounds are `413` (`request_too_large`, `prompt_too_large`)
and an over-large output allowance is `400 output_limit_exceeded`. Classes a
retry can plausibly clear advertise `Retry-After: 1` — an honest lower bound,
not a prediction; tenant-table exhaustion advertises nothing, because no amount
of waiting changes it.

A tenant refused at its own ceiling takes no global slot and no queue slot, so a
saturated tenant cannot crowd out a quiet one. Capacity is held by a permit
released in `Drop`, synchronously, which is what makes every exit path correct
by construction: a returned handler, a provider failure, a cancelled request, an
abandoned queue waiter, and — because the permit is moved into the relay's
accounting alongside the budget hold — a stream that completes, is cancelled
mid-answer, or is cut off by its own duration or byte bound.

`queue_capacity = 0` is the default. Refusing immediately gives a caller
something to act on; a queue converts saturation into latency the caller cannot
see, and is worth it only for bursts short enough that waiting beats retrying. A
queue must therefore be configured with a wait bound, and queueing requires a
finite `max_in_flight` — a queue in front of an unbounded ceiling is not a
control.

Telemetry is deliberately coarse: `axond.admission.in_flight` and
`axond.admission.rejections`, labelled by a closed vocabulary of resources
(`request`, `stream`, `tenant`, `queue`) and the stable error type. Tenant and
subject identity are **not** labels — an admission metric keyed by tenant is an
unbounded-cardinality metric authored by whoever is attacking, which is the same
resource-exhaustion bug one level down. For the same reason no bound's message or
log line echoes a prompt, a body, an output, or a credential.

### State tier

Tier 0. Every ceiling is process-local and in memory; no backend is added to the
request path, and no deployment's tier is raised. That is also the limitation: a
fleet of *N* replicas admits *N* × `max_in_flight`, and one tenant behind a load
balancer gets *N* × `max_in_flight_per_tenant`. Fleet-wide admission policy
belongs to the stateful control plane, so the ceilings live behind one
`AdmissionControl` value on `AppState` with a single `admit` entry point — the
seam a store-backed policy can be introduced behind without touching the request
path. Because the ceilings own semaphores built at boot, `[admission]` is
boot-only: a reload validates it and warns that a restart applies it, exactly as
`[transport]` behaves.

### What these bounds do not cover

Three gaps are known and accepted for this ADR, because closing them changes
either the request pipeline's shape or a default's meaning:

- **The stream bounds need a draining caller.** `max_stream_duration_ms` and
  `max_stream_bytes` are evaluated in the relay, which the server polls only
  when it can write to the caller's socket. A caller that opens a stream and
  then stops reading applies write backpressure, the relay stops being polled,
  and neither bound fires — so that stream's permit and budget hold stay held.
  Enforcing a lifetime independently of polling needs the relay driven by its
  own task rather than by the response body, which is a larger change than this
  ADR takes on.
- **Bodies are buffered and parsed before admission.** Shedding happens in the
  handler, after the JSON extractor has read and parsed the body, so the
  concurrency ceilings bound how many requests reach a provider rather than how
  many bodies are in memory at once. `max_request_bytes` is the only bound on
  that phase. Moving admission ahead of body reading means taking it in a
  middleware layer that does not yet know the caller's namespace.
- **`max_in_flight_per_tenant` is the effective ceiling in a single-namespace
  deployment.** With one namespace serving all traffic, the replica sheds at the
  per-tenant ceiling — and answers `429 tenant_concurrency_exceeded`, which
  points an operator at the caller rather than at the replica. Such deployments
  should raise the per-tenant ceiling to `max_in_flight` or disable it with `0`.

## Consequences

- A single tenant or surge can no longer exhaust the process. Traffic above the
  ceiling is refused with a typed answer instead of degrading every caller.
- Operators inherit finite defaults on upgrade. A deployment that legitimately
  runs more concurrency than the defaults, or accepts larger bodies, must raise
  the relevant bound explicitly and restart — the same upgrade cost ADR 0028
  imposed.
- Callers can observe additional error types. They are additive under the
  [0.x policy](./0015-zero-dot-x-compatibility-policy.md): they classify traffic
  that previously succeeded at the expense of other callers, or hung.
  `429 tenant_concurrency_exceeded` in particular must not be read as the
  existing `429 rate_limit_exceeded`; the two answer different questions.
- The per-replica scope must be part of capacity planning, and it is a real
  sharp edge: autoscaling multiplies these ceilings. `[rate_limit]` remains the
  fleet-wide bound on a subject's request *rate*.
- Bounds are refusals, so a workload that relied on a very large output
  allowance or a multi-hour stream fails where it previously ran. That is the
  intent: an unbounded request is indistinguishable from an abusive one.
