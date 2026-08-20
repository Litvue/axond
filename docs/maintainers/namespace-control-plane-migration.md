# Namespace and blob control-plane migration plan

Status: accepted architecture; Stage 1 is in progress. Typed namespace identity,
canonical routes, and authorization against existing one-namespace credentials
are implemented. Set/all grant projection and later stages remain open.

Audience: maintainers planning the transition selected by
[ADR 0062](../adr/0062-blob-backed-flat-namespace-control-plane.md). This is not
an operator runbook. The released and current-tree behavior remains documented
by the existing configuration, administration, and deployment guides until a
stage below lands with its tests and migration notes.

## Target boundary

The target managed deployment requires one durable object-storage service.
Axond owns deployment resources and flat namespace resources; the consumer
owns users, tenants, projects, membership, RBAC, and the mapping to an opaque
namespace identifier.

Namespace-scoped provider APIs are mounted without changing their native
suffixes:

```text
/namespaces/{namespace}/v1/chat/completions
/namespaces/{namespace}/v1/responses
/namespaces/{namespace}/v1/embeddings
/namespaces/{namespace}/v1/models
/namespaces/{namespace}/v1/messages
/namespaces/{namespace}/v1/credentials
/namespaces/{namespace}/v1/tokens
```

The path selects one namespace and the authenticated grant authorizes it. No
header, model-name convention, query parameter, or request-body field selects a
namespace.

## Work that is paused

Do not restart the PostgreSQL stateful endurance runs or use their result to
close the production qualification epic for the new topology. Existing
capacity, fault, recovery, rollout, and endurance artifacts remain immutable
historical evidence for the source and topology that produced them. They are
not evidence for a blob-backed namespace control plane.

The current PostgreSQL implementation must remain buildable while the migration
is developed. Do not delete its schemas or mutate shipped DDL in place.

## Stage 1: freeze the contracts in tests

Before adding a storage adapter, add failing contract tests for the public and
storage boundaries:

- table-drive the provider-compatible route suffixes and prove that stripping
  `/namespaces/{namespace}` yields the existing registered path;
- prove namespace extraction and grant authorization run before body parsing,
  convergence disclosure, request-path stores, accounting, and provider
  dispatch;
- return the same response for an absent namespace and a namespace outside the
  grant;
- reject encoded slashes, dot segments, ambiguous percent encodings, Unicode
  aliases, duplicate separators, and overlong namespace identifiers;
- prove telemetry uses the bounded matched-path template rather than a concrete
  namespace label;
- exercise Python and TypeScript OpenAI and Anthropic SDKs with namespaced base
  URLs, including buffered and streamed calls, Responses, embeddings, and
  model listing.

The route implementation should nest the current provider router beneath a
namespace extractor rather than duplicate each route. OpenAI-compatible SDKs
use a base URL ending in `/namespaces/{namespace}/v1`; Anthropic SDKs that append
`/v1/messages` use a base URL ending in `/namespaces/{namespace}`.

Redirects are forbidden for authenticated or streaming `POST` routes. If a
legacy direct alias is needed for one migration window, it must dispatch
directly, be stateless-only and opt-in, name one configured default namespace,
and carry a removal release. The blob-backed stateful mode never serves an
implicit `/v1/*` namespace.

## Stage 2: introduce the object-store protocol

Add a responsibility-specific backend for immutable desired state. Do not add a
universal `StateBackend` trait and do not make object storage a request-path
backend.

The conformance suite must run against a deterministic in-memory fake and every
selectable adapter. It must prove:

- create-only writes have one winner;
- replacement succeeds only for the version read;
- readers see either the complete old object or complete new object;
- stale and concurrent publishers receive `Conflict`;
- a successful head update never references a missing object;
- timeout after an ambiguous write is reconciled by reading, never by blind
  overwrite;
- a repeated idempotency key with the same checksum replays its published
  result, while the same key with different content is refused before stale-head
  conflict handling;
- malformed, hash-mismatched, unknown-schema, and unwrappable records are
  `Corrupt` or `Invalid`, never retried as outages.

Azure Blob Storage block blobs are the first production adapter. The domain
trait must expose only provider-neutral bytes and opaque version tokens so S3
ETags and Google Cloud Storage generations can implement the same contract.
Cloud-specific leases, queues, notifications, and list consistency cannot be
required for correctness.

## Stage 3: replace the durable domain hierarchy

Introduce the target desired-state types without translating at request time:

```text
DeploymentSpec
NamespaceId
NamespaceSpec
NamespaceGrant
BlobRevisionManifest
BlobHead
```

`NamespaceSpec` is complete and contains the effective model enablement,
aliases, routes, policies, middleware, pricing, and credential references for
one namespace. Remove runtime tenant/project inheritance. A template or import
may help author several namespace bodies, but the published representation is
fully materialized.

Keep provider profiles, encrypted secret objects, base catalogue resources,
middleware registrations, and administrative trust at deployment scope. A
namespace pins immutable deployment resource versions; a deployment resource
change does not become half-active in one namespace.

Authentication produces a grant. The path produces the selected namespace.
The authorization step intersects them and places only the authorized path
namespace in request context. Every downstream key—credentials, aliases,
budgets, rate limits, pricing, redaction, telemetry, and usage—must derive from
that context.

## Stage 4: implement publication and convergence

Implement immutable upload followed by one conditional head replacement:

1. read the environment head and version;
2. check retained history for the request's idempotency key and checksum;
3. hydrate the current immutable revision;
4. apply a declarative mutation with an expected revision;
5. validate and compile the complete candidate;
6. create all new immutable objects;
7. conditionally replace the environment head;
8. after failure or an ambiguous response, re-read the head and check
   idempotency before reporting success or a typed conflict.

Required crash and recovery tests cover every boundary between those steps.
Unreachable uploads are garbage, not visible partial state. Garbage collection
starts from retained heads, follows manifests, and deletes only after a grace
period.

Replica convergence conditionally reads the head, verifies all immutable
objects, compiles a snapshot, and swaps it atomically. Warm serving continues
during storage loss. Cold start succeeds from object storage or an authenticated
local last-known-good cache and otherwise remains unready.

Rollback always publishes a new monotonic revision containing earlier resource
content. It never rewinds or edits the head's history.

Successful mutation audit is committed inside the revision. Denied operations
and secret-material operations that produce no revision use immutable
create-only audit objects. Initial retention keeps the complete revision chain;
history compaction is out of scope until immutable revision and idempotency
checkpoints have a separate accepted design.

## Stage 5: migrate administration and secrets

Replace tenant/project/principal mutations with namespace and grant operations.
Namespace-scoped administration is mounted under the same outer namespace
prefix; deployment-scoped provider, secret, trust, history, and status
operations remain explicitly global.

Secret ciphertext becomes an immutable object. Preserve the existing envelope
encryption, associated-data binding, lifecycle, non-disclosure, snapshot-time
resolution, and zeroization properties. The minimal adapter uses a bootstrap
key supplied by environment or mounted secret; external KMS and secret managers
remain optional.

Administrative query behavior must be designed for immutable manifests rather
than emulating arbitrary relational queries. Required list and lookup indexes
are part of the signed revision. Optional diagnostic listing may use the object
store but cannot become serving authority.

## Stage 6: provide a one-way PostgreSQL export/import

Do not dual-write PostgreSQL and object storage. Dual authority would recreate
the split-brain condition the operating-mode boundary forbids.

The migration command must:

1. quiesce PostgreSQL administrative writes at a named source revision;
2. map each existing project to its effective Axond namespace;
3. materialize inherited tenant policy, catalogue enablement, aliases, pricing,
   middleware, providers, and credentials into a complete namespace body;
4. convert workload-principal access into namespace grants;
5. export encrypted secret material without exposing plaintext;
6. preserve the old audit stream as a signed historical archive and start the
   blob revision chain with explicit source provenance;
7. reject constructs that cannot be preserved, including an exact cap spanning
   several namespaces, ambiguous alias ownership, or conflicting namespace
   projections;
8. validate and compile the exported revision before publishing its head.

The new namespace identifier is derived from the stable durable project ID, not
the renameable `tenant-slug/project-slug` runtime label. The exporter emits the
old-project-to-new-namespace mapping that consumers must install before route
cutover; generated IDs are slash-free and canonical.

Existing workload-key rows contain digests rather than recoverable plaintext.
The exporter must either retain a narrowly bounded compatibility verifier for a
documented window or require those credentials to rotate; it cannot claim to
export key material it does not possess. Existing signed last-known-good caches
use the old snapshot schema and must be regenerated after cutover.

Cutover uses one authority at a time. Rollback before any blob mutation may
return to the quiesced PostgreSQL revision. After blob writes begin, returning
to PostgreSQL requires an explicit reverse export or loss acknowledgement; it
must never happen automatically.

## Stage 7: update public compatibility surfaces

This is a minor-release change under the `0.x` policy. The release must include:

- the namespace base-URL migration for every supported SDK;
- the removal schedule or absence of implicit `/v1/*` routes;
- new namespace and grant administration examples;
- typed errors for malformed namespace IDs, conflicts, unavailable object
  storage, corrupt revisions, and unavailable serving snapshots;
- configuration migration from PostgreSQL connectivity to an object-store URL
  and credential source;
- an explicit statement that Redis/PostgreSQL remain optional only for exact
  shared enforcement or billing-grade usage;
- a rollback boundary and the one-way export warning.

Update all raw route users through a shared namespace-aware URL helper rather
than scattered string substitutions. This includes binary/Compose smoke tests,
Tier 0 gates, compatibility fixtures, capacity/fault/rollout/endurance drivers,
examples, alerts, dashboards, and client documentation.

## Stage 8: replace the mandatory deployment topology

Binary/VM, Compose, managed-container, and Kubernetes deployments are all
first-class. The object-store protocol does not require Kubernetes.

Remove the PostgreSQL migration Job and port-5432 egress from the minimal
stateful topology. Configure an object-store endpoint/container/prefix,
workload identity or bounded storage credential, HTTPS egress, operation
timeouts, and payload limits. A normal rolling process or Kubernetes
`Deployment` is sufficient when cold-start survival during a blob outage is not
required. A per-replica disk or StatefulSet/PVC remains an optional recovery
profile for the signed last-known-good cache.

Preflight must verify protocol version, credentials, read/write bounds, and a
disposable native conditional-create/replace sequence before replicas roll. It
must clean up the probe and must not require DDL authority.

## Stage 9: re-qualify the intended topology

The production packet must be re-cut around one frozen candidate and the target
topology. At minimum retain direct evidence for:

- provider SDK route compatibility and namespace non-enumeration;
- single-replica capacity with namespace isolation;
- provider and optional request-path backend faults;
- concurrent publication and stale-writer conflicts;
- publisher crashes before and after every immutable/CAS boundary;
- corrupt, missing, and unknown-version objects;
- warm object-store outage and recovery convergence;
- cold boot with and without a valid signed local cache;
- secret rotation and loss of the bootstrap key;
- multi-replica rollout while namespace and deployment resources change;
- backup, object version recovery, rollback, and region-failover fencing;
- stateless and blob-backed stateful long endurance outside GitHub Actions.

No production closure claim may combine evidence from different source commits,
storage protocols, route authorities, or topology generations.

## Completion gates

The transition is complete only when:

- PostgreSQL is absent from the minimal stateful boot and serving dependency
  graph;
- the only canonical inference routes are namespace-prefixed native routes;
- tenant, project, and workload-principal resources are absent from the target
  public administration model;
- every replica can converge and cold-start under the documented blob/cache
  contract;
- SDK, security, recovery, rollout, and endurance evidence is retained from one
  frozen candidate;
- documentation and deployment examples describe object storage as the
  preferred stateful control plane and label PostgreSQL as optional or legacy.
