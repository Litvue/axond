# 8. Ordered target failover and per-target circuit scope

Date: 2026-08-04

## Status

Accepted

## Context

An alias maps a caller-facing name to an ordered list of concrete `targets`
(`provider` + `model` + `price`). Until now `/v1/chat/completions` served the
**first target only**; the remaining targets were dead config. The point of an
ordered list is resilience: when the primary provider is degraded, the gateway
should try the next one rather than surface the failure.

Two mechanisms already exist in the tree and must not be conflated:

- **Credential pools** (ADR 0006) rotate *credentials* within a single
  `(namespace, provider)` pair. A `429` parks one key and retries the same
  target with the next; this is `dispatch_over_pool`.
- **`gateway-core` primitives** — the retryability taxonomy
  (`ProviderError::is_retryable` / `affects_provider_health`), a `FailoverPolicy`,
  and a `CircuitBreaker` — exist but were unused on the request path.

The retryability taxonomy already draws the line we need: `429` and `5xx` and
transport errors are retryable and reflect on provider health; a `404`
(`ModelUnavailable`) is retryable but deliberately does *not* reflect on health;
`4xx`-class errors (invalid request, context-window, unsupported) are permanent.
Streaming (ADR 0005) adds a hard constraint: once the SSE relay has emitted a
byte, the response is committed and a mid-stream failure is a terminal `error`
event — it can never be retried against another target.

## Decision

**Failover is the outer loop around credential-pool dispatch.** A new
`dispatch_with_failover` walks the alias's `targets` in order; at each target it
resolves that provider's credential pool and calls the existing
`dispatch_over_pool` unchanged. Pool selection is not rewritten. Native routes
(#7) build on this helper rather than re-implementing the walk. The busiest
shared file, `routes.rs`, keeps the loop as a factored helper instead of an
inline tangle.

**A retryable upstream failure advances to the next target; a permanent one is
returned as-is.** The decision reuses `FailoverPolicy::decide` over the core
taxonomy, so failover and the rest of the system share one definition of
"retryable". A `4xx`-class error stops the walk immediately — retrying it would
only fail again and multiply latency.

**Each target has its own in-memory, per-replica circuit breaker**, reusing
`gateway_core::CircuitBreaker` (not a new implementation). It is keyed by the
target's qualified `provider/model`, held on `AppState`, and — consistent with
running stateless by default (ADR 0002) — is process-local: no shared store, no
cross-replica coordination. Consecutive target-scoped failures trip it; a tripped
target is skipped until a cooldown elapses, then re-offered as a single half-open
probe that closes on success or re-opens on failure.

**Circuit scope is defined explicitly, and it is narrower than "any failure".** A
target's breaker records a failure only when the error reflects on the *target's*
health (`affects_provider_health`) **and** is not credential-scoped:

- `5xx` / transport failure → fail over **and** trip the target's breaker.
- `404` `ModelUnavailable` → fail over, but **do not** trip the breaker (a missing
  deployment is not an unhealthy target).
- A pool-wide `429` (every credential exhausted) → fail over, but **do not** trip
  the target's breaker. `429` is credential-scoped (ADR 0006); letting it open a
  target would take capacity away from other keys and other namespaces.
- `4xx`-class permanent error → no failover, no breaker change.

**The walk is bounded twice.** `failover.max_attempts` caps the number of
upstream target attempts and `failover.overall_timeout_ms` caps the total
wall-clock spent walking, checked before each attempt. Either bound ends the walk
and returns the last real error, so failover cannot amplify a slow request
without limit. Both, plus the per-target `failure_threshold` and
`cooldown_seconds`, are configurable with sane defaults and are rejected at boot
if zero — "fail at boot, not at request time".

**Streaming separates open-time rotation from mid-relay rotation.** HTTP
open-time 429s walk the credential plan on both wires. After opening, only an
OpenAI-normalized stream may rotate on an explicit rate-limit event before
content is emitted; native byte-faithful streams and partially delivered
streams retain terminal-error semantics.

**Attempt and target attribution are recorded.** Each target attempt emits one
`axond.upstream.attempt` child span with an incrementing zero-based
`attempt_index`, and the canonical `UsageRecord` gains an additive `attempts`
field (the retry count is `attempts - 1`) alongside the already-present serving
`target_provider` / `target_model`. Per ADR 0006's precedent, adding a field is
an additive change under the existing `schema_version`. Target circuit
transitions publish the existing per-target circuit-state gauge.

## Consequences

- An alias with a healthy fallback now survives a degraded primary transparently;
  usage rows and traces show which target served and how many attempts it took.
- A flapping target stops being retried on every request once its breaker opens,
  bounding both latency and load on a struggling upstream, and recovers on its own
  via the half-open probe.
- Per-target and per-credential health remain independent: a single hot key never
  opens a target, and an unhealthy target never parks a key.
- The bounds mean a pathological all-targets-failing request is capped rather than
  walking an arbitrarily long list; the caller gets the last upstream error.
- Breaker state is per-replica, so a target may be probed independently on each
  replica. A shared/coordinated breaker is intentionally deferred, consistent with
  the stateless-by-default posture.
- A rotated stream keeps target-attempt accounting separate from lease attempts
  and reconciles its one reservation by charging the prompt once plus carried
  output from each consumed stream attempt.
