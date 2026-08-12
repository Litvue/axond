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
- [ ] `[admission]` ceilings are set deliberately, not left at defaults, and sized to what one replica's memory and descriptor limits can hold.
- [ ] `admission.max_request_bytes` matches the largest legitimate caller payload, and the proxy in front does not accept more than the gateway will.
- [ ] The container/unit memory limit exceeds `max_in_flight` x `max_request_bytes` plus steady-state footprint.
- [ ] Queueing is off (`queue_capacity = 0`) unless bursty traffic has been measured, and `queue_wait_ms` is set whenever it is on.
- [ ] Callers handle `429 tenant_concurrency_exceeded` and `503 gateway_overloaded` with backoff, honoring `Retry-After`.
- [ ] Shedding metrics (`axond.admission.rejections`, `axond.admission.in_flight`) are dashboarded and alerted on.

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
- [ ] Per-replica `[admission]` ceilings x replica count is the concurrency the providers and the fleet can actually absorb.

## Rollout and shutdown

- [ ] New replicas pass boot validation before receiving traffic.
- [ ] Rollout keeps enough replicas for current concurrency and disruption policy.
- [ ] Load balancer removes an instance before termination.
- [ ] Operators understand that application-level SIGTERM draining is not implemented yet.
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
- [ ] `/readyz` returns `ready` without authentication.
- [ ] `/v1/models` returns `401` without a key.
- [ ] `/v1/models` returns the expected namespace-scoped aliases with a key.
- [ ] A real buffered request succeeds through the production ingress.
- [ ] A real streamed request succeeds through the production ingress.
- [ ] A deliberately unknown alias returns typed `404 unknown_model`.
- [ ] A backend outage produces the configured fail-open or fail-closed behavior.
