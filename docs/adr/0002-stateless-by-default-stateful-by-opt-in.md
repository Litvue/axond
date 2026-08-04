# 2. Stateless by default, stateful by opt-in

Date: 2026-08-03

## Status

Accepted

## Context

Tollgate is distributed as a single self-hosted binary. The competing
self-hosted gateways generally assume a datastore (Postgres and/or Redis) just
to boot, which raises the operational floor for the common case: a company
routing its own traffic through its own keys.

But two features genuinely want state:

- **Usage records** — raw, per-request accounting that must be durable if it is
  to be used for billing or analytics.
- **Usage quotas** — spend/volume caps that, to be *exact* across a replica set,
  need shared state.

A previous iteration of this service put a Redis-backed usage counter on the
default request path. It failed *open* during a Redis outage (silently
disabling all enforcement) and coupled every request to an external system it
did not otherwise need.

## Decision

The binary boots and serves with **zero external state**. Every stateful
feature is opt-in behind a trait:

- `UsageSink` (write path, off the request path): default `StdoutSink` (JSONL).
  Postgres/Tinybird/ClickHouse/OTLP are opt-in implementations.
- `QuotaStore` (read path, on the request path): default `NoQuota`, with an
  in-memory per-replica store available. Redis/Postgres backends for
  cross-replica exact accounting are opt-in.

These are **two separate traits**, deliberately. Usage sinks can be slow,
batched, and eventually consistent (Tinybird is a fine sink). A quota store
must be fast and fresh (Tinybird is *not* a fine quota store — ingestion lag
makes caps meaningless under burst).

Quotas are enforced **reserve-then-reconcile**: token counts are unknown until a
response completes, so we reserve a conservative estimate before dispatch and
reconcile against the actual receipt after. Concurrent in-flight requests can
overshoot, so caps are **soft** unless a backend implements hard reservation.
Whether a quota-store failure fails open or closed is configurable; the default
for platform-owned spend caps is fail-closed.

## Consequences

- The tagline is honest: "runs stateless; scales stateful when you ask it to."
- No feature may quietly add a datastore to the default path; doing so is a bug.
- Multi-replica deployments that need exact quotas must configure a shared
  `QuotaStore`; the in-memory default enforces per-replica ceilings, which is
  documented rather than hidden.
