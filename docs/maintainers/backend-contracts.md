# Backend responsibility boundaries

Audience: contributors adding or implementing a stateful seam. This page is the
map between [ADR 0027](../adr/0027-stateless-and-stateful-operating-modes.md)
and the code. Operators do not need it: nothing described here changes how a
deployment is configured today, and no contract on this page has a durable
implementation yet.

## There is no universal state backend

Axond selects a backend **per responsibility**. There is deliberately no
`StateBackend` trait that "the database" implements, because the seams differ in
ways a single trait would have to flatten:

- a spend cap is read while a request is in flight, in milliseconds, with a
  fail-closed stance when its store is unreachable;
- durable desired state is written transactionally, with optimistic
  concurrency and an audit event, and is allowed to be slow.

Collapsing those into one trait would force one error taxonomy, one availability
policy, and one consistency model on both — and it would imply that any backend
can serve any responsibility, which is precisely how Redis ends up holding the
system of record.

## The seven contracts

| Contract | Owns | Permitted implementations | Callable from | Module |
| --- | --- | --- | --- | --- |
| `ControlPlaneStore` | Durable desired state: revisions, manifests, resource versions, audit | Postgres | Control plane only | `crates/gateway/src/backends/control_plane.rs` |
| `SecretStore` | Wrapped secret material and unwrapping | Encrypted Postgres (first), external secret managers later | Snapshot compilation only | `crates/gateway/src/backends/secrets.rs` |
| `CatalogSource` | Model metadata ingestion | models.dev | Background refresh only | `crates/gateway/src/backends/catalog.rs` |
| `BudgetStore` | Spend caps | none / in-memory / Redis / Postgres | Request path (opt-in) | `crates/gateway/src/budget/` |
| `RateLimiter` | Inbound admission | none / in-memory / Redis | Request path (opt-in) | `crates/gateway/src/rate_limit/` |
| `RevocationStore` | Precise minted-token `jti` revocation | none / Redis / Postgres | Request path (opt-in) | `crates/gateway/src/revocation/` |
| `UsageSink` | Durable usage rows | stdout / OTLP / Postgres | Off the request path | `crates/gateway/src/usage/` |

The table is also code — `backends::RESPONSIBILITIES` — and the unit tests in
`backends::tests` assert the invariants against it, so a new contract or a new
permitted implementation cannot quietly break them.

The four request-path seams keep their own modules, their own error enums, and
their own `on_unavailable` policies. They are named here, not merged: a common
supertrait would make the per-seam availability decisions harder to review, and
those decisions are the ones that determine whether a store outage returns `503`
or silently stops enforcing.

## Request path versus control plane

`BackendPath` records where each contract may be called from, and it is the
property the ADR's availability argument rests on:

- `RequestPath` — called while an inference request is in flight. Only the
  opt-in budget, rate-limit, and revocation seams qualify.
- `OffRequestPath` — carries a request's data but cannot fail its response
  (`UsageSink` is buffered and batched).
- `ControlPlane` — administrative reads and writes, revision publication, and
  convergence. Unavailability degrades change and cold start, never inference on
  a replica that already holds a snapshot.
- `SnapshotCompilation` — called only while compiling a candidate revision.
  `SecretStore` lives here: a snapshot is publishable only once every credential
  it needs is already resolved in memory, so a request never unwraps a secret.
- `Background` — periodic maintenance with no request or boot dependency
  (`CatalogSource`).

A `ControlPlane`, `SnapshotCompilation`, or `Background` contract appearing in a
request handler is a bug, not a slow path. The tests enforce the declaration;
review enforces the call sites.

## Redis cannot own durable state

This is structural, not a naming convention:

- `ControlPlaneBackend` has exactly one variant, `Postgres`. Adding a durable
  backend means adding a variant *and* an arm to
  `BackendKind::durable_control_plane`.
- `ControlPlaneBackend::parse` rejects `redis` with its own error arm
  (`UnsupportedControlPlaneBackend::HotStateOnly`) rather than as an unknown
  name, so an operator who tries it is told why the answer is no.
- `SecretBackend` offers encrypted Postgres and external managers only.
- `RESPONSIBILITIES` lists no durable responsibility that permits
  `BackendKind::Redis`, and a test fails if one ever does.

Losing Redis loses hot enforcement precision. Losing durable state loses the
deployment. They must not be the same store.

## Capabilities and error categories

`Capability` is declared by an implementation, never probed by a caller: a caller
that needs transactional audit asks, instead of discovering the answer from a
half-applied write. Every `ControlPlaneStore` must declare
`TransactionalWrites`, `OptimisticConcurrency`, `IdempotentWrites`, and
`TransactionalAudit`; `ChangeNotification` is optional and only decides whether
convergence polls.

Each contract keeps its own error enum and maps into a shared
`FailureCategory` so retry and surfacing policy can be written once:

| Category | Means | Retry the same operation? |
| --- | --- | --- |
| `Unavailable` | Unreachable or timed out | Yes |
| `Conflict` | A concurrent writer won | No — re-read and rebuild |
| `NotFound` | The referenced thing does not exist | No |
| `Invalid` | Malformed input, dangling reference, violated constraint | No |
| `Denied` | Refused on authorization or policy grounds | No |
| `Corrupt` | Stored data is unreadable (decryption failure, unknown record version) | No — operator alert |

`Corrupt` exists so an unwrappable secret or an unreadable revision is never
reported as an outage: retrying cannot help, and an operator has to know.

## Secrets are references everywhere except a compiled snapshot

`SecretRef` is what manifests, audit events, `/admin/v1` responses, logs, and
diagnostics carry. `SecretMaterial` holds plaintext, has no `Display`, no
`Serialize`, and a `Debug` that prints `SecretMaterial(<redacted>)`, so a
holder's derived `Debug` inherits the redaction. The only way to the bytes is
`SecretMaterial::expose`, which is greppable in review. This is the rule
stateless mode already applies to `env`-referenced key material.

## Catalogue metadata is not activation

`CatalogSource::refresh` may store new or changed model metadata without human
action. It never enables a model for a tenant, never changes which alias targets
exist, and never activates a price — `CatalogPrice` is the rate the upstream
*publishes*, and turning it into a billed price is an explicit administrative
mutation. `CatalogRefresh::Unchanged` is a first-class answer so "the upstream
has nothing new" cannot be confused with "the upstream now lists no models",
which would retire every model.

## What is not here yet

These are contracts. Nothing in `backends` is constructed by `serve`, so the
running gateway is the stateless gateway it was: no new boot step, no new
request-path work, no Postgres, and no schema. The contract tests run against
in-memory fakes (`backends::fakes`), which keeps the Tier 0 hermetic gate
hermetic and is why a fake is test-only — an in-memory control plane is not a
selectable backend.

The desired-state domain (UUIDv7 typed ids, tenant-scoped slug rules, canonical
serialization and checksums, resource envelopes, content-addressed blobs) is
defined separately, and the types in `control_plane` are thin placeholders it
refines: a manifest carries resource references and an opaque checksum, and the
store never interprets either. The durable Postgres implementation and revision
reconciliation follow after that.
