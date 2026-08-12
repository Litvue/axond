# 27. Per-phase transport bounds and stream idle timeouts

Date: 2026-08-12

## Status

Accepted

Bounds the upstream call phases of the streaming relay in
[ADR 0005](./0005-streaming-relay.md) and refines how the failover walk in
[ADR 0008](./0008-target-failover-and-circuit-scope.md) spends its overall
deadline.

## Context

`failover.overall_timeout_ms` was checked only *between* attempts, so an
in-flight attempt could outlive it: a target that accepted a connection and then
never sent response headers held the request open indefinitely. Nothing bounded
the wait for headers, the read of a buffered body, or the wait for the next
chunk of an already-open stream, and both success and error bodies were read
whole with `resp.text()`, so a provider's response size dictated the gateway's
memory use.

A single total request timeout cannot serve both shapes of traffic: a buffered
completion should fail fast, while a long, productive stream may legitimately
run far longer than any bound an operator would accept for "time to first
byte". A useful bound must therefore distinguish the phases of an upstream call,
and must distinguish work that can still be retried from work whose bytes the
caller already holds.

## Decision

A new `[transport]` section configures one finite, validated bound per phase:
`connect_timeout_ms`, `response_header_timeout_ms`, `buffered_body_timeout_ms`,
`stream_idle_timeout_ms`, `max_response_bytes`, and `max_error_bytes`. Zero
disables nothing — it is rejected, as is an error bound wider than the body
bound. Defaults are safe rather than generous, and the settings are boot-only
because `connect_timeout_ms` configures the shared pooled HTTP client; a reload
validates them and warns that a restart applies them.

`failover.overall_timeout_ms` is authoritative for every phase that precedes
serving the caller: connecting, waiting for headers, reading a buffered body,
opening a stream, and rotating credentials. Each phase waits for the *tighter*
of its own bound and the deadline's remaining time, so an active call cannot
outlive the walk's budget. Retry and failover remain possible only while that
budget permits another attempt and only before any downstream byte is committed.

Once a stream is open the overall deadline no longer applies: the request is
being served, and cutting a productive stream off at a failover budget would
destroy work that cannot be retried. Every wait for a subsequent chunk is bound
by `stream_idle_timeout_ms` instead, which is what actually distinguishes a
useful stream from a stalled one. A post-byte failure is terminal — the relay
ends the stream in band and never splices a second attempt onto a response the
caller has already begun reading.

Bodies are collected with explicit bounds, and the two bounds mean different
things. A success body over `max_response_bytes` is refused with a typed error
rather than relayed, because the gateway will not buffer unbounded provider
output. A provider error body is diagnostic, so it is *truncated* at
`max_error_bytes`: the provider's status is what the failover policy needs, and
discarding it over a verbose error page would lose information.

Timeouts and body-limit refusals are typed (`Connect`, `ResponseHeaders`,
`BufferedBody`, `StreamIdle`, `Overall`, and oversized body), surface as
`504 upstream_timeout` and `502 upstream_body_too_large`, and are attributed on
the attempt span and the `axond.upstream.timeouts` metric by phase label only —
never a provider URL or credential. A spent overall deadline is the gateway's
own budget, not evidence about a target, so it is excluded from the target
circuit breaker in the same way a pool-wide `429` is (ADR 0006); the per-phase
timeouts remain target-scoped dependency failures.

### State tier

Tier 0. The bounds are process-level configuration for provider egress and
introduce no runtime state or service dependency. Tier 1 (Redis) and Tier 2
(Postgres) deployments read the same settings and keep their existing state
choices; no existing deployment's tier is raised.

## Consequences

- A stalled or silent target now fails within a known bound instead of holding
  a request open, and the overall failover budget is a real limit rather than a
  pre-dispatch check.
- Callers can observe two additional error types. They are additive: they
  classify failures that previously hung or surfaced as generic transport
  errors, and no existing successful response is reclassified.
- Operators inherit finite defaults on upgrade. A deployment that relied on
  unbounded waits or very large buffered responses must raise the relevant
  bound explicitly, and must restart to apply it.
- A productive stream can outlive `failover.overall_timeout_ms`, so that
  setting is not an upper bound on request duration for streaming traffic;
  `stream_idle_timeout_ms` is the bound that governs a stream's liveness.
- Every timeout path still settles its budget reservation and releases its rate
  limit permit exactly once, so bounding a phase cannot leak accounting.
