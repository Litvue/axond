# Backend responsibility boundaries

TypeScript is the sole implementation under
[ADR 0069](../adr/0069-retire-the-rust-gateway.md). Operators should read
[Store backends](../deployment/stateful-backends.md).

| Responsibility | Owner under `ts/packages/` | Path |
| --- | --- | --- |
| Namespaces, budgets, usage index, discovery cache | `sdk/src/index.ts` contract; `cli/src/sqlite-store.ts`, `cli/src/postgres-store.ts` | Request path and management |
| Spend admission and settlement | `gateway/src/app.ts`, `gateway/src/dispatch.ts` | Request path; charge tracked through settlement |
| Provider discovery and catalogue refresh | `cli/src/discovery.ts` | Background; retain prior data on refusal |
| External usage delivery | `cli/src/usage-delivery.ts`, `cli/src/usage-sql.ts` | Buffered off the request path |
| Secret resolution | `gateway/src/config.ts`, `cli/src/config-file.ts` | Boot; TOML carries references, not material |

The Store is SQLite or Postgres in the process host; the Worker uses Postgres
through Hyperdrive. The memory implementation is useful for embedding/tests,
not durable production accounting. Store errors are mapped to typed gateway
failures. Budget-store failure denies by default; the configured availability
stance must be reviewed independently of best-effort external usage delivery.

The Store usage insert is the charge. The former Rust billing-grade outbox is
not implemented in TS core. Imported catalogue metadata does not activate a
price book; configured price rules remain authoritative.

There is no universal backend: a durable charge, background discovery and
best-effort sink have different availability and consistency contracts.
New state or secret paths require
[threat-model review](../security/threat-model-review.md), and state-tier changes
need an ADR.
