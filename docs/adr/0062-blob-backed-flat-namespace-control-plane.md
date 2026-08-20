# 62. Blob-backed flat namespace control plane

Date: 2026-08-20

## Status

Accepted; implementation in progress. Typed namespace identity, canonical
namespace-prefixed inference routes, complete flat namespace desired-state
resources, and deployment-scoped single/set/all workload grants now compile
into recoverable serving snapshots. Deployment resources now carry a signed
secret index, static policy compiles without a coordination backend, and exact
shared caps remain explicit. Blob publication/runtime wiring, administrative
trust activation, migration, and topology qualification remain open.

Supersedes the PostgreSQL-only stateful control plane and the durable
tenant/project/workload-principal hierarchy selected by
[ADR 0027](./0027-stateless-and-stateful-operating-modes.md). It also supersedes
ADR 0017's definition of Tier 2 as PostgreSQL specifically: Tier 2 means a
durable external state service, of which object storage and PostgreSQL are
implementations with different capability envelopes.

The following ADR 0027 properties remain in force:

- stateless mode is the default and retains its hermetic Tier 0 guarantee;
- one resource dimension has one authority in one process;
- ordinary inference reads one immutable in-memory snapshot and never reads the
  control plane;
- a candidate activates completely or not at all, and a rejected candidate
  leaves the active snapshot serving;
- control-plane loss degrades administration and cold start, not warm serving.

This ADR changes the target architecture before the PostgreSQL stateful surface
has entered the `0.x` compatibility promise. The implementation and migration
sequence is tracked in
[the namespace control-plane migration plan](../maintainers/namespace-control-plane-migration.md).

## Context

Axond's stateful design made the gateway the owner of tenants, projects,
workload principals, their inheritance, and their durable lifecycle. PostgreSQL
was a defensible implementation for transactional resource mutation, audit,
idempotency, and optimistic concurrency, but that choice made a relational
database a prerequisite for centrally managed inference even though the
request path consumes a compiled snapshot rather than relational rows.

The desired deployment boundary is smaller. Axond is embedded behind a trusted
consumer backend. That consumer already authenticates users and owns tenant,
project, membership, and RBAC semantics. Axond needs one opaque isolation and
configuration key; teaching it the consumer's organization hierarchy duplicates
authority and creates projection and inheritance states that can fail without
improving provider routing.

The inference API has a second constraint. Existing provider SDKs must continue
to call their native paths and bodies. Namespace selection cannot move into a
provider payload, model name, query parameter, or required custom header. It
must be expressible by changing only the SDK's base URL.

The durable mutations are low-frequency and naturally revisioned. They need a
strong exact-key read, immutable object creation, and one compare-and-swap
publication point; they do not require joins or a database read on every
request. Object storage is the smallest common service with those properties.

## Decision

### Namespace is the only Axond ownership boundary

Axond has no durable tenant, project, or workload-principal resource in its
target stateful model. It has two scopes:

- **deployment scope** owns provider profiles, encrypted secret material or
  references, the base catalogue, middleware registrations, administrative
  authentication, and telemetry/usage destinations;
- **namespace scope** owns one complete serving definition: enabled models,
  aliases, provider routing, namespace credential references, policy limits,
  middleware selection, approved pricing, and a coarse token epoch.

A namespace identifier is opaque, caller-chosen, URL-safe, canonically encoded,
and never parsed for hierarchy. A consumer may map a tenant and project to that
identifier; Axond neither stores nor reconstructs that mapping.

Namespace specifications are complete. Administrative templates may reduce
authoring duplication, but publication resolves a template into the namespace
body. Runtime inheritance between deployment, tenant, project, and principal
objects is rejected. A namespace either compiles as a whole or the revision
containing it is refused.

The consumer owns user identity, membership, RBAC, and the choice of namespace.
Axond owns isolation after that choice: routing, credentials, policy, pricing,
and usage attribution are all keyed by the authorized namespace.

### Grants authorize; the path selects

Inbound authentication remains the outer request boundary selected by
[ADR 0061](./0061-authentication-remains-an-outer-boundary.md), but a durable
"principal" is not an Axond business resource. An authenticated caller has a
**grant** that permits one named set of namespaces or all namespaces. The grant
may be represented by a static key, a signed token claim, or a configured
trusted issuer; those mechanisms do not change the namespace resource model.

The URL selects exactly one namespace. The grant is intersected with that path
parameter before Axond parses a provider body, consults a request-path backend,
or dispatches upstream. A missing namespace and a namespace outside the grant
have the same non-enumerating response.

There is no `x-axond-namespace` header. Tokens may carry namespace grants, but
no token claim supplies hidden routing context that can disagree with the URL.
An optional opaque subject may be recorded for usage or local enforcement; it
does not create a managed subject hierarchy or confer namespace access.

### Namespace prefixes preserve provider-native routes

The provider-compatible router is mounted under
`/namespaces/{namespace}`. Removing that prefix from an inference URL produces
the exact native route:

```text
/namespaces/{namespace}/v1/chat/completions -> /v1/chat/completions
/namespaces/{namespace}/v1/responses        -> /v1/responses
/namespaces/{namespace}/v1/embeddings       -> /v1/embeddings
/namespaces/{namespace}/v1/models           -> /v1/models
/namespaces/{namespace}/v1/messages         -> /v1/messages
/namespaces/{namespace}/v1/credentials      -> /v1/credentials
/namespaces/{namespace}/v1/tokens           -> /v1/tokens
```

The outer router extracts and authorizes the namespace. The inner route table,
provider request bodies, response bodies, streaming semantics, and SDK methods
remain unchanged. An OpenAI-compatible client can therefore use a base URL such
as `https://axond.example/namespaces/acme-production/v1`; a client that appends
`/v1` itself uses `https://axond.example/namespaces/acme-production`.

Namespace-scoped administration is mounted under the same outer prefix, for
example `/namespaces/{namespace}/admin/v1/models`, `/aliases`, `/policy`, and
`/pricing`. Deployment-wide provider, secret, trust, revision-history, and
replica-status operations remain explicitly global under `/admin/v1`; liveness,
readiness, and metrics are global operational surfaces as well.

The namespaced routes are the canonical target surface. A temporary legacy
mount may exist only as an explicitly time-bounded migration aid; it cannot be
used by the blob-backed stateful mode, cannot infer among multiple namespaces,
and cannot become a second permanent route authority.

### Object storage is the preferred durable control plane

The preferred stateful control-plane implementation is a provider-neutral
object-store protocol. Azure Blob Storage block blobs are the first target;
Amazon S3 and Google Cloud Storage can implement the same narrow contract.
PostgreSQL may remain an optional compatibility or higher-write backend, but it
is not required for managed inference and does not define the domain model.

The mandatory object-store operations are:

```text
get(key) -> bytes, version
put_if_absent(key, bytes)
replace_if_version(key, bytes, expected_version)
```

The store must provide strong read-after-write for exact keys, atomic object
replacement, an opaque stable version token (for example an ETag or generation),
conditional create/update, bounded operation and object sizes, TLS, and
deployment-scoped authorization. Production adapters must implement native
conditional writes; a lock-object fallback is local-development behavior only.
An acknowledged write must be durable, a failed precondition must leave the
object unchanged, and the opaque concurrency version is never treated as a
content hash.
Listing, deletion, notifications, leases, object versioning, and retention are
useful operational capabilities but are not on the publication or serving
correctness path.

There is one mutable publication object per environment:

```text
environments/{environment}/head.json
```

All revision and resource objects are immutable and content-addressed:

```text
revisions/{revision-digest}/manifest.cbor
resources/namespaces/{resource-digest}.cbor
resources/deployment/{resource-digest}.cbor
secrets/{ciphertext-digest}.bin
```

The head names the active revision digest, monotonic sequence, schema version,
and integrity metadata. A revision manifest names its parent, actor/grant,
mutation summary, resource hashes, and signature. The namespace map is
`namespace identifier -> immutable resource digest`, so changing one namespace
reuses every unchanged resource.

### Publication is immutable upload followed by one CAS

A publisher:

1. reads the current head and records its version token;
2. checks the reachable revision history for the request's idempotency key;
3. hydrates the referenced revision and applies a declarative desired-state
   change with an explicit expected revision;
4. validates the complete candidate and compiles its namespace views;
5. uploads new ciphertext, resource, and revision objects with create-only
   conditions;
6. replaces the head only if its version still matches the one read;
7. after an ambiguous response or failed compare-and-swap, re-reads the head and
   checks idempotency before reporting success or conflict.

The head never references an object that was not successfully written. A crash
before the final compare-and-swap leaves unreachable immutable objects, not a
partial revision. A losing concurrent publisher receives a typed conflict,
re-reads, and rebuilds; it never retries a stale head update as last-writer-wins.

Administrative mutations are declarative. Callers choose stable resource IDs
and send an expected revision and idempotency key. The immutable revision binds
that key to the desired-state checksum: the same key and checksum replay the
original result, while the same key with different content is refused. The
check occurs before stale-head conflict reporting so a lost successful response
is recoverable. The core protocol does not recreate a mutable relational
idempotency table or imperative ID allocator; retained history is the initial
index, and any future compaction must first add immutable idempotency
checkpoints.

Rollback publishes a new revision whose resources reproduce an earlier desired
state. It never rewinds the sequence or edits history. Successful-mutation
audit is inside the signed, parent-linked revision chain and becomes visible at
the same head commit point. Denied operations and secret-material operations
that publish no revision use individually immutable create-only audit objects;
listing those records is administrative behavior, not serving correctness.
Operational retention may additionally protect objects with store-native
versioning or WORM policy.

Garbage collection starts from retained environment heads and walks immutable
references. It deletes unreachable objects only after a grace period longer
than the maximum publisher and reader lifetime. Garbage collection is never
required to publish or serve.

### Replicas compile and serve without storage on the request path

A replica conditionally reads the head, fetches a changed immutable revision,
verifies hashes/signatures/schema, resolves its secrets, and validates the
complete graph. Before activation it confirms that the head version has not
changed; a changed head discards the candidate and retries. It then atomically
swaps the compiled snapshot. Requests capture one namespace view from that
snapshot for their whole lifetime.

Warm replicas keep serving during an object-store outage. Administration and
convergence pause. A cold replica may restore an authenticated local
last-known-good snapshot only when that snapshot contains no flat-v2 provider
credentials. Credential-bearing flat-v2 snapshots deliberately do not cross a
restart until the cache is checked against an authenticated monotonic
revision/tombstone floor; otherwise a stale but authentic cache could resurrect
material revoked by a newer revision whose cache write failed. Without object
storage or an eligible local cache, the replica remains healthy-but-unready and
serves no inference.

The optional local cache is a recovery copy, never the desired-state authority.
It is not an additional external service and may use a VM disk or per-replica
persistent volume. A deployment that does not retain it makes no cold-start
outage claim. This projection slice retains cold recovery for credential-free
flat-v2 state; credential-bearing recovery is a later integration gate, not a
claim inferred from the cache format alone.

### Secrets remain encrypted and off the request path

Secret ciphertext may be an immutable object, but plaintext never appears in a
manifest, audit entry, log, or administrative response. The minimal deployment
receives a bootstrap key from an environment variable or mounted secret and
uses per-version envelope encryption bound to the deployment, namespace owner,
and exact secret reference. A KMS or external secret manager is optional.

The signed deployment resource is authoritative for non-secret secret metadata.
Each index entry binds an owner namespace, exact `SecretRef` and version,
ciphertext digest, and lifecycle state. A namespace credential is valid only
when that exact active entry exists and names the same namespace. Two
credentials in one namespace may share an exact reference; an exact reference
cannot have two index rows, and versions of one secret cannot move between
namespace owners. Resolution receives this complete typed binding rather than
an ownerless reference. Only the validated flat-state index can mint the opaque
request consumed by resolution; raw request construction is unavailable.

The native v2 object is a deterministic canonical-CBOR fixed array containing
only schema `2`, scheme `aes256-kw.aes256-gcm.envelope.v2`, stable KEK id, RFC
3394 wrapped DEK, material nonce, and ciphertext. Environment, namespace, and
exact secret reference are authenticated caller context rather than stored
fields. Binary length-prefixed material AAD binds its purpose, environment id,
`NamespaceId`, secret UUID, version, and KEK id. RFC 3394 AES-256-KW wraps the
fixed 32-byte DEK without a nonce; because AES-KW has no AAD, caller-context
binding is asserted only for the complete object after material authentication.
The environment value is the publication protocol's single `EnvironmentId`,
not a codec-local spelling. Opening consumes an opaque
`AuthenticatedSecretBinding` and checks its indexed ciphertext digest before
selecting a KEK. This crypto slice intentionally provides no production
constructor for that binding: the integration slice must mint it only after a
signed active revision, its content-addressed deployment object, and the exact
deployment secret-index entry have all been verified. Tests and fuzzing alone
have synthetic constructors. The same is true of the distinct create-only
publication binding; its production minting belongs beside immutable publisher
reservation enforcement.
Plaintext is capped at 64 KiB, and the strict decoder rejects alternate CBOR
spellings and oversized objects before allocation. A serving
`BlobSecretOpener` owns only a bounded `KekDecryptRing` and has no sealing API or
publication authority. A publisher-only `BlobSecretSealer` owns one active KEK
and one opaque create-only binding and has no opening API. Up to eight
decrypt-only keys permit rolling rotation, while duplicate ids or aliased raw
key bytes refuse the whole ring atomically. The legacy v1
Postgres envelope is a separate unchanged format and is never guessed from blob
bytes.

Publication must reserve an exact `SecretRef` create-only and report a conflict
if any value already occupies it; changing bytes always requires a new version.
The object-store contract already refuses overwriting an immutable object key,
but the reference index and publisher that enforce this stronger rule are a
follow-up slice and are not claimed by the codec alone.

Staging or rotation creates a new sealed version. Activation, disablement, and
revocation publish namespace or deployment revisions that change references;
they do not mutate ciphertext. Destruction first creates an immutable tombstone
and then cryptographically erases or deletes the sealed object. Rollback
preflight must refuse an older revision whose exact secret version has been
tombstoned.

Secret resolution occurs only while compiling a candidate. The resulting
material is owned and zeroized with the compiled snapshot as required by
[ADR 0039](./0039-envelope-encrypted-secret-store-and-snapshot-time-resolution.md).

### High-frequency coordination stays outside the blob control plane

Object storage does not implement distributed counters or transactional
request journaling. The blob-only deployment supports static namespace policy,
per-replica admission, revisioned token epochs, and usage attribution, but not
an implied exact fleet-wide counter. A namespace therefore carries static
policy independently of an optional exact-enforcement block. Omitting exact
enforcement activates on the blob-only topology. Requesting exact spend and
concurrency caps preserves the existing fail-closed backend and storage-layout
checks and is refused unless configured shared backends enforce every value.

The following remain optional responsibility-specific backends:

- exact fleet-wide budgets, rate limits, and concurrency leases may select
  Redis or PostgreSQL as their existing contracts permit;
- precise high-frequency token revocation may select a revocation backend;
- telemetry and usage continue to use pluggable destinations;
- billing-grade "durable before response" usage requires an acknowledged
  durable journal and is not implemented by the control-plane blobs.

Selecting one of those capabilities raises only that responsibility's tier and
request-path availability coupling. It does not make PostgreSQL or Redis a
requirement of stateful namespace management.

Deployment-scoped administrative trust remains part of the target authority,
but it is security state and cannot be accepted as inert configuration. Until
one snapshot can activate trust for both administrative authentication and
flat-namespace authorization and recover it through LKG atomically, this build
rejects every nonempty durable trust list.

### State tier

This feature is **Tier 2 — durable external state**, implemented by object
storage in the preferred deployment and optionally by PostgreSQL. Tier 2 no
longer means PostgreSQL specifically. Tier 0 remains config-only and hermetic;
Tier 1 remains optional Redis coordination. No existing stateless deployment is
raised from Tier 0, and none gains a blob dependency unless it explicitly
selects stateful mode.

## Consequences

- The minimum managed deployment is Axond plus one object-storage account;
  telemetry, usage, and exact shared enforcement remain explicit opt-ins.
- Tenant/project identity and authorization move to the consumer backend. Axond
  can no longer answer which customer owns a namespace or enforce a cap across
  several consumer-defined tenants or projects without an external mapping or
  enforcement service.
- Namespace isolation becomes easier to reason about: one route parameter, one
  grant check, one complete namespace view, and one usage key replace a
  hierarchy and its inheritance/projection rules.
- Provider SDK compatibility is preserved by changing only the configured base
  URL. The route transition is nevertheless a public HTTP break and must ship
  in a minor release with SDK integration evidence and migration notes.
- Object-store publication has weaker query and multi-row mutation facilities
  than PostgreSQL. Administrative writes may conflict and retry, server-side
  ad-hoc querying is limited, and compaction/garbage collection become explicit
  maintenance work. These costs are accepted because publication is
  low-frequency and serving uses a compiled snapshot.
- A full current PostgreSQL stateful deployment cannot be silently converted.
  Tenant inheritance must be materialized into project namespaces, grants must
  be flattened, and unsupported cross-namespace constraints must be rejected by
  an explicit export/import tool.
- Existing PostgreSQL qualification evidence remains historical evidence for
  that implementation. It cannot close production gates for the blob-backed
  namespace topology. Recovery, rollout, SDK compatibility, corruption,
  conflict, and endurance qualification must target this decision before the
  new stateful mode is called production-ready.
