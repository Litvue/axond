# Tenant isolation: current evidence

TypeScript is the sole implementation under
[ADR 0069](../adr/0069-retire-the-rust-gateway.md). The tenant unit is the namespace
selected from the canonical `/ns/{ns}/v1` path after static-key authentication.
The static key is deployment-wide, not a per-tenant authorization credential.

| Layer | Mechanism | Evidence owner under `ts/packages/` |
| --- | --- | --- |
| Authentication | Reject unknown keys before namespace lookup; reject noncanonical paths | `gateway/src/app-routes-and-validation.test.ts` |
| Credentials | Own BYOK pool, explicit platform fallback, non-secret status labels | `gateway/src/query.test.ts` |
| Store | Namespace/period usage grouping; incarnation rejects late settlement after recreation | `gateway/src/app-budgets-and-accounting.test.ts`, `cli/src/sqlite-store.test.ts`, `cli/src/postgres-store.test.ts` |
| Extensions | Untrusted queries require request namespace; optional store rate limiter isolates counters | `gateway/src/app-recovery-and-openapi.test.ts`, `rate-limit/src/index.test.ts` |

See [threat-model triggers](./threat-model-review.md) for exact current test names
and gaps. Credential-status tests are not full inference-dispatch isolation
evidence. Per-tenant concurrency admission is not in core.

The control-plane and minted-token isolation suite was withdrawn under ADR 0063.
Remaining Rust harnesses were removed under ADR 0069. Historical qualification
does not prove these properties for the TS process; Postgres tests require their
configured service and skipped tests are not passing evidence.
