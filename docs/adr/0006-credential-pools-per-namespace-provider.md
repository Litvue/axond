# 6. Credential pools per namespace and provider

Date: 2026-08-04

## Status

Accepted

## Context

ADR 0003 organizes credentials by namespace and binds each `(namespace,
provider)` pair to one environment variable, while explicitly anticipating
*multiple* credentials per pair. Operators need that for reasons that have
nothing to do with provider topology:

- A single provider key has its own rate limit and quota. Two keys on the same
  provider double the ceiling without changing routing.
- Keys fail independently. One key hitting `429` — or being revoked — should not
  take a provider target out of service for every namespace using it.
- Spend has to stay attributable per key, not just per namespace, or a pooled
  deployment cannot reconcile a provider invoice.

Ordered failover *across targets* and a per-target circuit breaker are a separate
concern (tracked separately, `gateway-core::CircuitBreaker`). Conflating the two
would let one exhausted key open a target that other keys can still serve.

## Decision

**A `(namespace, provider)` pair binds N credentials.** Several `[[credential]]`
entries may name the same pair; together they are that pair's pool. Each entry
keeps its env-var reference (secrets stay write-only) and gains two optional
fields: an `id` label and a `weight`. `id` defaults to the env-var *name*, which
is a reference and not a secret, so a pool is attributable without any config
change.

**Every declared credential must resolve at boot.** A credential whose env var is
unset or empty refuses to start the process, as do a zero `weight` and a
duplicate `id` within a pool. This extends "fail at boot, not at request time" to
the credential graph — previously a missing env var silently produced a pair with
no key and surfaced as a request-time `502`.

**Selection is a rotating cursor, `round-robin` or `weighted`.** Round-robin
starts each request at the next credential; weighted walks a cumulative-weight
ladder, so a credential of weight `n` leads `n` out of every `total_weight`
requests. Both then order the *rest* of the pool by rotation, which is what makes
the skip-on-429 walk deterministic: a request receives an ordered plan of
attempts, not just one key.

**A `429` is attributed to the credential, not the target.** On a rate-limit or
exhausted-quota response the request retries the *same* target with the next
credential in the plan, including streamed opens. An OpenAI-normalized stream
may also rotate on an explicit rate-limit event before content is emitted;
native streams and partially delivered streams remain terminal. The failure
counts against that credential only.
Every other upstream failure (5xx, transport, `404`) is a target-scoped signal
and is left to target failover and the per-target breaker.

**Credential health is a circuit scoped below the target.** After
`failure_threshold` consecutive credential-scoped failures a credential is parked
for `cooldown_seconds`, then re-offered as a half-open probe; a success clears its
history. The probe is single-shot and taking it re-arms the cooldown, so a burst
of concurrent requests cannot all pay a round-trip to a key that is still
known-bad. The probe *leads* the plan rather than trailing it, so the request
holding it actually exercises the key, and a probe failure simply continues the
walk into the healthy credentials. The breaker lives in the gateway crate
next to the pool, deliberately separate from `gateway-core`'s per-target breaker:
a bad key parks that key, and the target stays available to every other key. When
*every* credential in a pool is parked and none is due a probe, the request still
gets the rotation's first choice — health is advisory, so stale bookkeeping must
not manufacture an outage.

**BYOK isolation is decided before selection.** A plan is always drawn from a
single namespace's pool: the requesting namespace's own pool, or — only when
`allow_platform_fallback` is set — the whole platform pool, attributed as
`credential_source = platform`. A pool walk can therefore never mix a customer's
key with a platform key inside one request.

**The serving credential is attributed on the usage record.** `UsageRecord` gains
`credential_id` (the label, never the secret) alongside the existing
`credential_source`, so per-key spend and per-key error rates fall out of the
single canonical record. Skips are logged and represented by lease spans; the
serving credential remains the one attributed on the usage record.

## Consequences

- Adding capacity to a provider is a config + env-var change: another
  `[[credential]]` entry for the same pair.
- `UsageRecord` gains a field. The schema is treated as an API, so this is an
  additive change under the existing `schema_version`.
- Operators must set every env var they reference; a half-configured pool no
  longer boots. Deploy tooling that mounted optional credentials has to declare
  them.
- Because 401/403 lose their status in the current provider-error taxonomy, a
  revoked key trips its credential only once it starts answering `429` (or
  through the target path). Preserving authentication status on `ProviderError`
  so a revoked key parks immediately is a follow-up.
- Per-credential state is per-process. Two replicas rotate independently and park
  independently, which is consistent with running stateless by default (ADR
  0002); a shared credential-health store would be an opt-in stateful backend.
- Read-side exposure of pool health (a credential-status endpoint) is a follow-up
  on the status/readiness surface; the snapshot it needs is already available on
  `Credentials`.
