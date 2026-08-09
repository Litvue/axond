# 18. Hermetic Tier 0 boot-and-serve gate

Date: 2026-08-09

## Status

Accepted

## Context

ADR 0002 makes config-only operation the default, and ADR 0017 names that
deployment Tier 0: no datastore or network dependency at boot or on the
serving path. ADR 0014 already provides a black-box harness and committed
provider fixtures, but its process runs with the host's network privileges.
The Tier 0 promise needs a mechanical CI check rather than a convention that
can silently regress when a new feature starts a client or store.

## Decision

The existing static-musl CI lane runs `ops/tier0-gate.sh` after building the
binary. The script re-executes itself in an unprivileged user and network
namespace using `unshare --user --map-root-user --net --fork`, brings up only
loopback, and fails if the namespace can reach a public TCP endpoint or resolve
public DNS. It also asserts that the usual Redis (6379) and Postgres (5432)
loopback ports are not listening.

Inside that namespace the gate boots a committed Tier 0 configuration, probes
health and readiness, checks inbound authentication and typed unknown-model
errors, and sends a real chat request to the committed-fixture fake upstream on
loopback. The configuration includes token-verifier env resolution so all
startup dependencies are exercised before process exec.

The gate deliberately does **not** claim that loopback is a network boundary.
Loopback is allowed so a local fake upstream can prove the serving path. A
datastore listening on 127.0.0.1 inside the same namespace would remain
reachable; the explicit Redis and Postgres port assertions cover the usual
accidental cases.

This extends the static-musl lane instead of adding another job: the qualified
artifact is already built there, and the namespace gate adds focused runtime
coverage without multiplying CI scheduling and aggregate status. `CI Success`
therefore remains the stable required aggregate.

## Consequences

- CI proves no outbound connectivity, no public DNS, and no usual Redis or
  Postgres dependency for the default boot-and-serve path.
- Failures dump gateway logs and explain that a stateful feature must be
  gated behind an explicitly selected higher tier.
- New stateful features must declare their tier in an ADR and stay off the
  default path unless the deployment opts into that tier.
- Tier 1 and Tier 2 still need their own integration tests for unavailable
  stores, timeouts, fail-closed behavior, and recovery; this gate does not
  qualify those deployments.
