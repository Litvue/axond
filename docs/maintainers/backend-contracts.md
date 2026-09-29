# Backend responsibility boundaries

Audience: contributors adding or changing a stateful seam. This page maps each
durable or external responsibility to the code that owns it and the path it may
be called from. Operators do not need it; they read
[Store backends](../deployment/stateful-backends.md).

The live product is [ADR 0063](../adr/0063-stateful-only-namespaced-gateway.md):
one Store, SQLite or Postgres, behind `[storage]`. The control plane,
`SecretStore`, the revocation store, the rate limiter, and the Redis and
in-memory budget backends were withdrawn with it.

## There is no universal state backend

Axond selects a backend **per responsibility**. There is deliberately no
`StateBackend` trait that "the database" implements, because the seams differ in
ways a single trait would have to flatten: a spend cap is read while a request is
in flight, in milliseconds, with a fail-closed stance when its store is
unreachable, while a usage row is buffered and batched off the request path and
must never fail a response. One trait would force one error taxonomy, one
availability policy, and one consistency model on both.

## The contracts

| Contract | Owns | Implementations | Callable from | Module |
| --- | --- | --- | --- | --- |
| `Store` | Namespaces, period and cadence budgets, the management usage index, the discovery cache | SQLite / Postgres | Request path and `/api/v1` | `crates/gateway/src/store/` |
| `BudgetStore` | Spend admission and settlement over the Store | Store-backed | Request path | `crates/gateway/src/budget/` |
| `CatalogSource` | Model metadata ingestion | models.dev | Background refresh only | `crates/gateway/src/backends/catalog.rs` |
| `CatalogStore` | Durable retention of imported catalogue snapshots | Postgres / in-memory | Background refresh only | `crates/gateway/src/backends/catalog_store.rs` |
| `UsageSink` | Usage rows | stdout / OTLP / Postgres | Off the request path | `crates/gateway/src/usage/` |

The request-path seams keep their own error enums and their own
`on_unavailable` policies. A common supertrait would make the per-seam
availability decisions harder to review, and those decisions are the ones that
determine whether a store outage returns `503` or silently stops enforcing.

## Request path versus background

- **Request path.** Called while an inference request is in flight: the Store
  lookup of the namespace and the budget admission. An unreachable Store fails
  closed by default (`404 unknown_namespace` vs `503 store_unavailable`, and
  `503 budget_unavailable` under `on_unavailable = "deny"`).
- **Off the request path.** Carries a request's data but cannot fail its
  response: `UsageSink` is buffered and batched. The billing-grade usage outbox
  ([ADR 0049](../adr/0049-billing-grade-usage-outbox.md)) is the opt-in
  exception and says so.
- **Background.** Periodic work with no request or boot dependency:
  `CatalogSource` and `CatalogStore`.

A background contract appearing in a request handler is a bug, not a slow path.

## Error categories

Each contract keeps its own error enum and maps into a shared `FailureCategory`
so retry and surfacing policy can be written once:

| Category | Means | Retry the same operation? |
| --- | --- | --- |
| `Unavailable` | Unreachable or timed out | Yes |
| `Conflict` | A concurrent writer won | No: re-read and rebuild |
| `NotFound` | The referenced thing does not exist | No |
| `Invalid` | Malformed input, dangling reference, violated constraint | No |
| `Denied` | Refused on authorization or policy grounds | No |
| `Corrupt` | Stored data is unreadable (unknown record version) | No: operator alert |

`Corrupt` exists so an unreadable row is never reported as an outage: retrying
cannot help, and an operator has to know.

## Secrets are references

TOML carries an environment-variable name or a file path, never material.
Provider credentials and DSNs are read at boot
(`crates/gateway/src/key_material.rs`, `crates/gateway/src/credentials.rs`) and
held in memory; no store holds secret material. A new `expose_secret` call site
fires [threat-model trigger 3](../security/threat-model-review.md#3-credential-delivery-and-redaction).

## Catalogue metadata is not activation

`CatalogSource::refresh` may store new or changed model metadata without human
action. It never makes a model reachable for a namespace. `CatalogRefresh::Unchanged`
is a first-class answer so "the upstream has nothing new" cannot be confused
with "the upstream now lists no models".

A scheduled refresher drives it
([ADR 0051](../adr/0051-durable-catalogue-snapshots-and-refresh-orchestration.md))
in a deployment that selects a source and a store in `[catalog]`
([ADR 0055](../adr/0055-catalogue-imports-in-a-running-deployment.md)).
`CatalogRefresher` writes an import to `CatalogStore` *before* it becomes
active, so a deployment never serves a catalogue it could not retain; a refusal
of any kind (upstream, parse, storage, timeout) leaves the active catalogue
alone and is counted. Imported models.dev rates are what the request path
charges from, with `[[price]]` as the fallback for unlisted ids
([ADR 0056](../adr/0056-request-path-pricing.md)).

## Adding a responsibility

A new durable responsibility names its path, its permitted backends, its error
enum, and its `on_unavailable` stance in the same change, and a state-tier
change gets an ADR from the [template](../adr/template.md).
