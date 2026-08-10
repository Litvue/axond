# 17. Explicit state tiers and optional backends

Date: 2026-08-09

## Status

Accepted

## Context

ADR 0002 promises that axond boots and serves with zero external state, while
`UsageSink` and `BudgetStore` demonstrate that stateful capabilities can sit
behind narrow traits. The token and principal-store design in ADR 0016 makes
this seam more important: identity, revocation, rate limiting, budgets, and
audit have different state requirements and should not silently turn a
config-only deployment into a database client.

The project needs an honest tier promise, a mechanical test of the default
promise, and a rule that prevents configuration and a database from becoming
competing authorities for the same object.

## Decision

**Every stateful feature declares its required tier.** The tiers are:

- **Tier 0 — config-only.** No network dependency. Config principals, minted
  token verification, offline minting, `NoLimit`, `NoBudget`, and stdout usage
  remain usable in the single-binary/default deployment.
- **Tier 1 — Redis.** Optional shared state for exact cross-replica budgets,
  inbound rate limits, and precise revocation.
- **Tier 2 — Postgres.** Optional durable audit and self-serve identity,
  including store-owned caller/key lifecycle.

No feature may quietly raise the tier of an existing deployment. Its ADR and
configuration must say which tier it requires and which implementation is
selected at each lower tier.

**CI must boot and serve axond at Tier 0 with no network dependency.** This is
the mechanical enforcement of ADR 0002's currently prose-only rule. The
hermetic black-box lane introduced by ADR 0014 must prove process startup,
liveness, authenticated catalogue/request behavior, and local fake-upstream
serving without Redis, Postgres, or provider access. A feature that makes this
lane require a network service has violated the default-state promise.

### One dimension, one owner

**Namespaces, providers, aliases, prices, and provider credentials are
permanently config-owned.** Callers and keys may be store-owned when a higher
tier is selected. Nothing may be defined in both config and a stateful store.

This rule prevents split-brain authority: a database cannot silently override a
namespace's provider access, an alias's target, a price, or the provider
credential pool. Config reload remains the owner of those dimensions under ADR
0011. A principal store owns only caller/key lifecycle, and the token verifier
still intersects token claims with config-owned namespace authority under ADR
0016.

### What state buys

**Only precise revocation and exact cross-replica inbound rate limiting lack an
adequate stateless answer.** Short TTLs, killing a `kid`, and `min_iat` epochs
cover coarse stateless revocation; minted tokens solve high-cardinality caller
issuance without a registry. Durable usage history is already an opt-in
`UsageSink`, and per-model/hierarchical budgets use the existing budget seam.

**Axond has no inbound rate limiting today.** The only limiting currently
present is reactive, per-replica handling of upstream `429` responses for
provider credentials, as described by ADR 0006. That behavior is not an
inbound RPM, TPM, concurrency, or tenant-throughput limit.

### RateLimiter

**Inbound limiting uses a new `RateLimiter` trait with `NoLimit` as the
default.** The default adds no state and preserves Tier 0. An in-memory
implementation is available for development and single-replica deployments,
but it provides only approximately `limit ÷ replicas` when replicas share a
nominal limit and is not an exact fleet-wide control.

Redis provides the exact Tier 1 implementation using expiring leases for
in-flight concurrency. It reuses the existing `[budget]` DSN and connection
convention rather than creating a second datastore configuration story. If a
configured Redis limiter is unavailable, its documented admission policy is
bounded and fail-closed, consistent with the shared budget precedent in ADR
0010.

### Explicitly rejected or deferred

The following are not part of this state decision:

- **Shared circuit or credential health.** Replicas should not couple on the
  hot path for a transient, self-correcting provider condition. The cheap
  alternative may share short-lived `429` observations, but the circuit
  decision remains local.
- **Response or semantic caching.** Cross-replica value, invalidation, usage
  accounting, and namespace isolation require a separate decision.
- **Streaming resumption or durable requests.** This belongs outside the
  gateway's current scope and is already handled at the application layer.
- **A runtime control plane.** Aliases, routes, prices, namespaces, and
  provider credentials remain file/config owned and reload through ADR 0011.
- **A key registry before a customer needs self-serve keys.** When that need
  exists, a store-backed principal implementation and its administrative
  authentication surface can be added at Tier 2. Minted tokens and offline
  minting come first.

Per-model and hierarchical budget caps remain the existing opt-in
`BudgetStore` concern from ADR 0010, not a reason to add another state tier.

## Consequences

- The single-binary, config-only deployment remains a first-class and tested
  product rather than an aspirational default.
- Tier 1 gives exact shared budgets, rate limits, and revocation, but makes
  Redis availability part of those selected request paths.
- Tier 2 adds durable audit and self-serve identity, but an identity table is
  load-bearing: it requires migrations, backup, restore, and ownership.
- If rate limiting becomes practically mandatory, the single-binary pitch
  weakens because Redis becomes operationally expected. That is a product
  decision, not something to hide behind a default.
- Layered principal resolution, bounded caches, per-layer timeouts, and an
  always-present config breakglass key mitigate availability coupling for
  stateful authentication. They do not make a stateful store disappear.
- The test matrix multiplies per trait and backend. ADR 0014's soak harness
  owns shared-state combinations; unit and black-box suites must still cover
  each default and failure mode.
- The one-owner rule makes some central-control designs harder: a database
  cannot be used as a convenient override for config-owned routing or
  credentials. That constraint is intentional.
- A Tier 0 CI lane must remain hermetic, while Tier 1 and Tier 2 need explicit
  integration coverage for unavailable stores, timeout behavior, fail-closed
  admission, and recovery.

## Open questions

- Settled by issue #64: the `RateLimiter` key is exactly `(namespace, subject)`,
  with no route or alias dimension, and the first policy is in-flight
  concurrency only (no RPM or TPM). Concurrency rejections do not emit
  `Retry-After`, because an upstream request has no honest deadline. Redis
  (#65) will slot behind the same trait without changing key semantics,
  placement, error shape, or the `NoLimit` default.
- The Redis schema and algorithm parameters for `jti` revocation remain open.
- A future self-serve identity product must define its administrative auth
  separately; this ADR does not create a runtime control plane.
- The tier labels should be included in future feature ADRs and deployment
  documentation so operators can choose a tier deliberately.

## Amendment (2026-08-10)

Issue #65 ships the Tier 1 Redis rate limiter. It enforces the existing
`(namespace, subject)` policy as exact fleet-wide **in-flight concurrency**, not
RPM or TPM: each admission creates a lease in one Redis hash and the owned
permit releases that lease on drop. Token-bucket and GCRA algorithms remain
deferred to the future issue that adds a rate-window policy.

The key is
`<key_prefix>:{<namespace>|<subject>}:leases`, with
`key_prefix = "axond:rate_limit"` by default. Hash tags keep each key's script
atomic and cluster-compatible. Each hash field is `lease_id -> expires_at_ms`;
the acquire script removes expired fields, counts live leases, and admits only
when the count is below `max_in_flight_per_subject`. The hash TTL is twice the
lease TTL, and the lease expiry is the crash-safety backstop when a replica
cannot drop its permit.

The limiter reuses the Redis budget's `dsn_env` when its own reference is
omitted, so a single-Redis deployment has one connection-string reference.
Redis connects and PINGs at boot. Each operation has a bounded timeout and the
default `on_unavailable = "deny"` fails closed with
`503 rate_limit_unavailable`; `allow` is an explicit fail-open exception.
When an ambiguous acquire may have created a lease, its compensating release is
fire-and-forget with bounded retries; retries open fresh Redis connections
instead of waiting behind the timed-out shared connection. A process-wide cap
limits how many releases retry at once, so an outage cannot amplify connection
pressure on the failing server: contended attempts are skipped rather than
queued, while the attempt count and deadline bound the effort. Lease expiry
remains the final backstop.

A dropped in-flight response wait can poison a multiplexed connection's reply
alignment, so every shared invoke is guarded structurally: dropping the
invoke retires the generation it used. Permit releases use a separate,
generous bounded timeout derived from the configured admission timeout because
they have no caller waiting on them; ordinary slowness should not retire a
healthy manager, while a real stall still does.
When a response is lost, later replies can be delivered to the wrong waiter;
an unattributable admission result is therefore unknown, not a denial.
The limiter refuses new admissions while a bounded replacement is in flight.
Results from a retired generation are unknown, so the existing unavailable
policy applies rather than trusting an admission or denial.
