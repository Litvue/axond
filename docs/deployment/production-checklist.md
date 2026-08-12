# Production checklist

Use this checklist before exposing Axond beyond a local development network.

## Artifact and platform

- [ ] Release archive or OCI image is pinned to an explicit version and digest.
- [ ] SHA-256, GitHub provenance, SBOM attestation, and cosign signature are verified.
- [ ] Runtime nodes support the selected artifact architecture; the OCI image is currently `linux/amd64`.
- [ ] No deployment depends on a `latest` tag.
- [ ] Rollback artifact and its compatible configuration are retained.

## Network and requests

- [ ] Inbound TLS terminates at a trusted proxy or load balancer.
- [ ] Axond is reachable only by intended callers.
- [ ] `Authorization`, `x-api-key`, and `traceparent` survive every proxy hop.
- [ ] Proxy buffering is disabled for streamed routes.
- [ ] Idle and total request timeouts allow expected model-generation duration.
- [ ] A real streamed request has been tested through production ingress.
- [ ] Clients retry transport failures without blindly replaying committed streams.

## Configuration and secrets

- [ ] TOML contains references, never secret values.
- [ ] Every declared environment/file reference is present and non-empty.
- [ ] At least one static breakglass gateway key is configured.
- [ ] Gateway keys are unique and mapped to the intended namespaces.
- [ ] Provider `base_url` values contain a path only, with no query, fragment, credentials, or secret.
- [ ] Alias targets stay within one provider wire family.
- [ ] Provider pricing is reviewed; budgets depend on it.
- [ ] Secret and signer rotation has been rehearsed.
- [ ] Config reload rejection is monitored.

## State and scaling

- [ ] Tier 0 per-replica behavior is acceptable, or a shared backend is selected.
- [ ] Redis/Postgres outage policy is an explicit `deny` or `allow` decision.
- [ ] Namespace budget-cap migrations were completed with the fleet stopped.
- [ ] Postgres DDL is applied before writers using the new schema.
- [ ] Redis key prefixes and Postgres tables are isolated between environments.
- [ ] Backup and restore procedures include schema/layout metadata and have been tested.
- [ ] Autoscaling does not accidentally multiply an in-memory budget or rate limit.

## Rollout and shutdown

- [ ] New replicas pass boot validation before receiving traffic.
- [ ] Rollout keeps enough replicas for current concurrency and disruption policy.
- [ ] Load balancer drains on `/readyz` failure, or `shutdown.drain_grace_ms` covers its polling interval.
- [ ] `terminationGracePeriodSeconds` / `TimeoutStopSec` exceed `drain_grace_ms + deadline_ms + flush_timeout_ms`.
- [ ] `shutdown.deadline_ms` reflects how long callers are allowed to hold a stream, since streams open at the deadline are cut.
- [ ] `axond.usage.dropped{reason="shutdown"}` is alerted on: it means records were lost at termination.
- [ ] Mixed-version restrictions in the release's migration notes are respected.

## Observability

- [ ] JSON logs reach the platform log sink.
- [ ] OTLP/HTTP export is configured and reachable when required.
- [ ] Alerts cover request failures, upstream failures, budget/rate-limit/revocation unavailable denials, and usage drops.
- [ ] Namespace budget denials are distinguished from per-subject denials.
- [ ] Credential `parked`/`probe` state can be inspected safely.
- [ ] Dashboard owners understand cache-read/cache-write token accounting.

## Acceptance test

- [ ] `/healthz` returns `ok` without authentication.
- [ ] `/readyz` returns `ready` without authentication, and `503 draining` after `SIGTERM` while `/healthz` still returns `ok`.
- [ ] `/v1/models` returns `401` without a key.
- [ ] `/v1/models` returns the expected namespace-scoped aliases with a key.
- [ ] A real buffered request succeeds through the production ingress.
- [ ] A real streamed request succeeds through the production ingress.
- [ ] A deliberately unknown alias returns typed `404 unknown_model`.
- [ ] A backend outage produces the configured fail-open or fail-closed behavior.
