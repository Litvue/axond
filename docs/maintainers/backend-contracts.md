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

## The eight contracts

| Contract | Owns | Permitted implementations | Callable from | Module |
| --- | --- | --- | --- | --- |
| `ControlPlaneStore` | Durable desired state: revisions, manifests, resource versions, audit | Postgres | Control plane only | `crates/gateway/src/backends/control_plane.rs` |
| `SecretStore` | Wrapped secret material and unwrapping | Encrypted Postgres (first), external secret managers later | Snapshot compilation only | `crates/gateway/src/backends/secrets.rs` |
| `CatalogSource` | Model metadata ingestion | models.dev | Background refresh only | `crates/gateway/src/backends/catalog.rs` |
| `CatalogStore` | Durable retention of imported catalogue snapshots | Postgres / in-memory | Background refresh only | `crates/gateway/src/backends/catalog_store.rs` |
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
  (`CatalogSource`, `CatalogStore`).

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
  name, so an operator who tries it is told why the answer is no. `parse` is the
  single resolution path — `Deserialize` delegates to it — so a configured value
  and a programmatic lookup accept the same canonical spelling (`postgres`) and
  fail with the same explanation.
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

Idempotency is payload-aware: a repeat of a key carrying the same checksum
replays the revision the first call published, while a repeat carrying a
*different* checksum is refused with
`ControlPlaneError::IdempotencyKeyReused` (an `Invalid`). Replaying the earlier
revision would tell a caller their change landed when it never did.

`Corrupt` exists so an unwrappable secret or an unreadable revision is never
reported as an outage: retrying cannot help, and an operator has to know.

## Secrets are references everywhere except a compiled snapshot

`SecretRef` is what manifests, audit events, `/admin/v1` responses, logs, and
diagnostics carry. `SecretMaterial` holds plaintext, has no `Display`, no
`Serialize`, and a `Debug` that prints `SecretMaterial(<redacted>)`, so a
holder's derived `Debug` inherits the redaction. The only way to the bytes is
`SecretMaterial::expose`, which is greppable in review. This is the rule
stateless mode already applies to `env`-referenced key material.

A reference is a *domain* type, not a store type:
`desired_state::secrets::SecretRef` is what a credential body carries and what
the store is asked about, so there is no second reference shape to keep in step.
Three properties of it are enforced by types rather than by review
([ADR 0034](../adr/0034-typed-provider-credentials-and-secret-lifecycle.md)):

- **Opaque and exact.** `SecretId` (`sct_…`) is deliberately not a `ResourceId`,
  so a credential's id and the id of its material are not interchangeable in a
  call or in text, and every reference carries a one-based `SecretVersion`.
  Rotation mints `SecretRef::rotated`; it never rewrites a version, and a store
  refuses a rotation whose next version is already stored. A credential resource
  names one version, so an admin surface that must not interrupt service stages
  the new version under a second credential and withdraws the old one after the
  cut-over; `ProviderCredentialBody::rotated` alone *is* the cut-over.
- **Owned.** Every `SecretResolver`/`SecretStore` method takes a `SecretOwner`
  (tenant, optionally project), derived from the resource's scope by
  `SecretOwner::from_scope`, so scoping is an argument rather than a check a
  caller may forget. Ownership is exact, not hierarchical, and a store must not
  disclose the existence of another owner's reference.
- **Stateful in a total, deterministic way.** `SecretLifecycle::transition_to`
  returns a permitted move, an idempotent `Unchanged`, or `ForbiddenTransition`
  for every ordered pair of `staged`/`active`/`disabled`/`revoked`/`tombstoned`.
  Only `staged` and `active` resolve; `revoked` never returns to service; a
  tombstone destroys the bytes. A revoked version can still be *rotated from* —
  that mints a fresh `staged` successor, which is how withdrawn material is
  replaced, and leaves the revoked version revoked and unresolvable. Only a
  tombstone refuses rotation.

The trait is split so that resolving cannot mint: `SecretResolver` has `resolve`
and `exists`, and `SecretStore: SecretResolver` adds `stage`, `rotate`,
`transition`, and `describe`. `exists` answers ownership and lifecycle rather than
whether a row is present — withdrawn material answers `false`, so a
pre-publication check cannot approve a version that will never authorize
anything, and `describe` is where a caller asks *why*. It is not a cheap
`resolve`: unwrappability is only provable by unwrapping, so material a rotated or
lost KEK has made unreadable still answers `true`, and compiling a candidate
revision is what proves material. The in-memory fake in `backends::fakes` states
the contract executably and is not a selectable backend. Provider-credential
bodies and the publication rules that cross-check them live in
`desired_state::credentials`; nothing there calls a store.

The one selectable implementation is `backends::secrets::postgres`: envelope-
encrypted rows in PostgreSQL, sealed under a fresh per-version data key which is
itself sealed under the deployment KEK `[secret_store]` references
([ADR 0039](../adr/0039-envelope-encrypted-secret-store-and-snapshot-time-resolution.md)).
Three invariants there are what make the domain above enforceable, and a second
implementation has to reproduce them: a version is a row written once, ownership is
checked against the row's own owner columns on every read (another owner's row
answers as an absent one),
and tombstoning destroys the sealed bytes in the transaction that records it. The
seal binds the scheme, the owner, and the exact reference as associated data, so
ciphertext moved between rows or tenants does not open.

Resolution is reachable from exactly one caller:
`convergence::secrets::SecretMaterialization`, which resolves the exact versions a
candidate revision's resolvable credentials pin, once each, while that candidate is
compiled. It holds a `SecretResolver` rather than a `SecretStore`, so the component
that handles plaintext cannot stage, rotate, or transition anything. Unwrapped
material is owned by the `ConfigSnapshot` compiled against it and shared through a
reference-counted holder registered in a ledger of references and counts, so a
rotation leaves both versions live and the superseded one is zeroized when the last
snapshot holding it — and therefore the last request compiled against it — is gone.
A resolution failure is a refused candidate (`ProjectionError::Secret`, rejection
label `secret`) and the previously published snapshot keeps serving.

## Catalogue metadata is not activation

`CatalogSource::refresh` may store new or changed model metadata without human
action. It never enables a model for a tenant, never changes which alias targets
exist, and never activates a price — `ObservedPrice` is the rate the upstream
*publishes*, and turning it into a billed price is an explicit administrative
mutation. `CatalogRefresh::Unchanged` is a first-class answer so "the upstream
has nothing new" cannot be confused with "the upstream now lists no models",
which would retire every model.

That holds now that something drives refresh on a schedule
([ADR 0051](../adr/0051-durable-catalogue-snapshots-and-refresh-orchestration.md))
in a deployment that selects a source and a store in `[catalog]`
([ADR 0055](../adr/0055-catalogue-imports-in-a-running-deployment.md)).
`CatalogRefresher` writes an import to `CatalogStore` *before* it becomes
active, so a deployment never serves a catalogue it could not retain; a refusal
of any kind — upstream, parse, storage, timeout — leaves the active catalogue
alone and is counted, durably, so staleness survives a restart. What a new
catalogue would mean for existing enablements is a `RefreshImpact` report: which
pins are behind, and which enabled offerings the upstream has stopped
publishing. Acting on either is an administrative mutation, so an operator does
it.

Reading an enablement back against a catalogue goes through `PinnedCatalog`
([ADR 0054](../adr/0054-resolving-pinned-catalogue-offerings.md)), the one place
that maps a pinned `OfferingId` onto the `CallableId` a request would send. It
answers about the snapshot it was built over and nothing else: a pin naming a
different payload digest is `OtherSnapshot` rather than resolved through the
active catalogue, and a provider publishing one model under several callable ids
is `Ambiguous` rather than guessed, because choosing between them is an
enablement decision. It is a projection — no store, no client, no request-path
I/O — and its withdrawal answer is tested to agree with `RefreshImpact` for
unmoved pins as well as current ones, so the operator report and the resolver
derive identity once and report the same withdrawals.

## The desired-state domain the control plane stores

`ControlPlaneStore` is expressed in `crates/gateway/src/desired_state/`, which
holds the domain itself: what a revision *is*, independent of any database. No
SQL type, connection, or Postgres representation appears in it, because #165
(schema and transactions), #166 (hydration), and #142 (runtime publication) all
have to agree on the same rules, and rules half-expressed in DDL cannot be
shared by a second store or a test double.

| Module | Answers |
| --- | --- |
| `ids` | who is who: UUIDv7 typed ids per entity, with `Slug` names kept separate from identity |
| `canonical` | what state hashes to: one versioned encoding, deterministic bytes, SHA-256 |
| `resource` | what a resource is: a generic envelope, versioned references, content-addressed blobs |
| `mutation` | who changed it, under what expectation, and what the audit trail records |
| `revision` | the complete state, the candidate proposing it, the manifest recording it, and the integrity checks a replica verifies |
| `secrets` | what a secret reference is: an opaque exactly-versioned handle, its owner, and its lifecycle |
| `credentials` | what a provider credential is: a typed body pointing at material it never holds, and the rules that cross-check a revision's credentials |

Three properties the rest depends on:

- **Identity is not a name.** A rename changes a `Slug`; ids, manifests, audit
  rows, and references are untouched. Ids are typed per entity, so a `TenantId`
  cannot stand in for a `ProjectId` even though both are 16 bytes, and their text
  forms carry distinct prefixes (`ten_`, `prj_`, `res_`, `rev_`, `mut_`, `aud_`).
- **Revisions are immutable and complete.** Publishing creates new resource
  versions and a new `RevisionManifest`; nothing is edited in place. A revision
  names the whole desired state rather than a diff, so hydration is one load and
  rollback is republication rather than reverse-application. Large immutable
  payloads — a catalogue snapshot — are `BlobRef`s addressed by their SHA-256
  digest, so revisions share one copy instead of duplicating it per manifest.
- **State has exactly one canonical form.** `SerializerVersion` is written into
  the bytes, integers are width- and sign-normalized, floating point has no
  representation at all, and set-like collections are sorted while
  order-significant lists are preserved. Two replicas, two releases, and two
  backends therefore compute the same `Checksum` for the same state, which is
  what makes a checksum comparison a decision rather than a hint.

`LoadedRevision::assemble` is the seam #142 consumes: a manifest and a state are
only paired after the whole-state checksum, every resource's content checksum,
the serializer version, and the blob declarations agree — so a truncated,
partially written, or tampered revision fails to load instead of being published
to the runtime. `desired_state::oracle` is the test-only in-memory
`ControlPlaneStore` that states these behaviours executably; like the other
fakes, it is not a selectable backend.

## The durable control plane is Postgres, and only Postgres

`backends::control_plane::postgres::PostgresControlPlane` is the real
`ControlPlaneStore`: the revision journal in
[`ops/postgres/control_plane_0001_initial.sql`](../../ops/postgres/control_plane_0001_initial.sql),
plus the transactions that keep it consistent. The
[journal runbook](../operations/control-plane-journal.md) is the operator's view;
the boundaries that matter to a maintainer are these.

**A publication is one transaction.** The head row is read `FOR UPDATE`, so
publishers serialize on one row instead of racing to append, and the manifest,
the resource versions, the blob references, the mutation, the audit event, the
idempotency record, and the head advancement commit together. A failure anywhere
rolls all of it back, which is why a refused publication leaves no resource
version and no audit event rather than leaving them to be cleaned up.

**Order is part of the contract.** Validation happens before any durable write;
the caller's retry window is consulted *before* the expected revision, so a retry
of a candidate that has since gone stale replays its own outcome instead of being
told it conflicts with the revision it published; only then is the expectation
checked, and only then is version immutability.

**Replay identity is the state checksum, and nothing else.** Not the request
bytes, not the mutation id, not the actor's attribution. The retry window is
scoped by a digest of the authenticated caller's identity so one administrator's
key cannot replay or block another's, and it expires — deduplication is a window,
not a permanent namespace. That scope is a retry namespace only: it is not an
access-attempt record, and `Mutation`/`AuditEvent` attribution is not reused as
one.

**The domain decides what a stored row means.** SQL constrains structure — id and
checksum shapes, scope ownership, actor attribution, body exclusivity,
referential integrity, the linearity of the chain — but it does not enumerate the
resource or blob vocabularies, because a new `ResourceKind` must not require a
migration. A row naming a kind the build cannot read is corruption reported as
such, not an outage, and `load_revision` returns a `LoadedRevision` only through
`LoadedRevision::assemble`, so an unverifiable revision fails to load.

**Storage is deduplicated, and blobs stay references.** A resource version is
written once and shared by every revision that pins it; the journal holds a
blob's kind, digest, and size, never its payload.

**A read is bounded, and a bound is a refusal.** `control_plane::hydration` owns
the manifest and revision reads, under `ControlPlaneSettings::hydration`: entry
rows, blobs, declared blob bytes, dependency edges, dependency depth, inline body
bytes, and the canonical size of the assembled candidate. Exceeding one is
`ControlPlaneError::TooLarge`, categorized `Denied` — policy an operator raises or
a revision an operator splits, not corruption and not an outage. Two properties
follow from *how* the bounds are applied. Each bounded read asks for one row past
its bound, so an oversized revision is detected rather than silently truncated to
the rows that fit; and the body bounds are server-side predicates
(`octet_length(...) > $2`), so an oversized body is refused without transferring
it. Nothing hydrated before the refusal is returned with it: a partial candidate
is the outcome this module exists to make unrepresentable.

**Hydration is deterministic, and cross-tenant isolation is checked twice.**
Every read is ordered and collects into `BTreeMap`/`BTreeSet`, so a stored
revision round-trips to the checksum it was published with, and a reference whose
version row is missing is named (`IntegrityError::MissingResource`) rather than
dropped from a shorter manifest that would then fail its checksum for reasons
naming no row. A reference-layer query refuses a dependency edge that crosses a
tenant boundary — or that makes deployment-scoped state depend on one tenant's —
before anything is hydrated, and `DesiredState::validate` checks the same rule on
the value that comes back. The dependency walk is iterative and memoized, so the
depth it bounds is not depth on the call stack. Its two refusals are different
answers on purpose: nesting past the bound is `TooLarge`, which an operator can
raise, while a cycle — which the domain refuses at publication, so storage only
holds one if something wrote outside the gateway — is `Corrupt` naming the edge
that closes it, because no bound could ever clear it.

One consequence of canonical encoding is worth stating: an inline body reads back
in canonical order, which need not be the order a caller wrote its map keys in.
It is the same value by the only measure the journal, the manifest, and the
checksum use.

## What is not here yet

Nothing in `backends` is constructed by `serve`, so the running gateway is the
stateless gateway it was: no new boot step, no new request-path work, and no
Postgres on the inference path. A replica serves an immutable snapshot it already
holds, which is what makes a control-plane outage an administrative failure
rather than a serving one. The non-Postgres contract tests still run against
in-memory fakes (`backends::fakes`), which keeps the Tier 0 hermetic gate
hermetic and is why a fake is test-only — an in-memory control plane is not a
selectable backend.

Revision convergence and publication to replicas (#142) follow.
`load_revision` — and `load_desired_revision`, which Postgres answers in a single
transaction so a head read cannot disagree with the hydration following it — is
the seam they read from.
