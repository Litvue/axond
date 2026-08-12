//! The desired-state domain: what a stateful deployment wants to be serving,
//! expressed without reference to any database.
//!
//! Nothing here knows about SQL, Postgres, connection pools, or wire formats.
//! That is the point: #165 stores these types, #166 hydrates them, #142 compiles
//! a snapshot from them, and all three have to agree on what a revision *is*. If
//! that agreement lived in DDL, the rules would be half-expressed in a schema, and
//! a second store — or a test double — could disagree with it silently. Here they
//! are expressed once, as types and validation, with an executable oracle.
//!
//! # The shape of the domain
//!
//! | Module | Answers |
//! | --- | --- |
//! | [`ids`] | who is who: time-ordered [`Uuid7`] identity, typed per entity, with slugs kept separate from it |
//! | [`canonical`] | what state hashes to: one versioned encoding, deterministic bytes, SHA-256 |
//! | [`resource`] | what a resource is: a generic envelope, versioned references, content-addressed blobs |
//! | [`mutation`] | who changed it, under what expectation, and what the audit trail records |
//! | [`revision`] | the complete state, the candidate that proposes it, the manifest that records it, and the integrity checks that let a replica trust it |
//! | [`tenancy`] | the first two body schemas: what a tenant and a tenant-owned project are, and who owns what |
//!
//! # Three properties everything else rests on
//!
//! **Identity is not a name.** Every entity has a [`Uuid7`]-based typed id, and a
//! separate human [`Slug`]. Renaming a tenant changes its slug and nothing else,
//! so manifests, audit rows, and references survive a rename untouched. The ids
//! are typed per entity, so a [`TenantId`] cannot be passed where a [`ProjectId`]
//! belongs even though both are 16 bytes.
//!
//! **Revisions are immutable and complete.** Publishing state creates new
//! resource versions and a new [`RevisionManifest`]; nothing is edited in place.
//! Each revision names the full desired state, not a diff, so hydrating one is a
//! single load and rollback is republication rather than reverse-application.
//!
//! **State has exactly one canonical form.** [`Canonical`] defines the bytes a
//! revision hashes to, under an explicit [`SerializerVersion`], with no
//! floating-point values and stable ordering for everything set-like. So two
//! replicas, two releases, and two backends compute the same [`Checksum`] for the
//! same state — which is what makes a checksum comparison a *decision* ("do I
//! already hold this?") rather than a hint.
//!
//! # What this module is not
//!
//! It is not wired into the request path. The runtime remains stateless: nothing
//! here is constructed by `serve`, and no snapshot is compiled from a revision
//! yet. It is also not a complete body model: [`tenancy`] is the only schema the
//! domain reads, and identity, provider, catalogue, pricing, and policy bodies
//! remain owned by their own slices.
//!
//! The types are the contract that #165, #166, and #142 build against, and the
//! test-only `oracle` module is the executable statement of how a
//! `ControlPlaneStore` must behave when they do.

pub mod canonical;
pub mod ids;
pub mod mutation;
pub mod resource;
pub mod revision;
pub mod tenancy;

#[cfg(test)]
pub(crate) mod fixtures;
#[cfg(test)]
pub(crate) mod oracle;

// The domain's facade. `allow(unused_imports)` because this is a binary crate:
// a re-export nothing in the tree happens to name yet is still part of the
// contract #165, #166, and #142 build against.
#[allow(unused_imports)]
pub use canonical::{
    Canonical, CanonicalError, CanonicalValue, Checksum, InvalidChecksum, SerializerVersion,
};
#[allow(unused_imports)]
pub use ids::{
    AuditEventId, InvalidId, InvalidSlug, InvalidUuid7, MutationId, ProjectId, ResourceId,
    RevisionId, Slug, TenantId, Uuid7, Uuid7Generator,
};
#[allow(unused_imports)]
pub use mutation::{
    Actor, AuditEvent, ExpectedRevision, IdempotencyKey, InvalidIdempotencyKey, Mutation,
    MutationKind,
};
#[allow(unused_imports)]
pub use resource::{
    BlobError, BlobKind, BlobRef, ResourceBody, ResourceKind, ResourceRef, ResourceScope,
    ResourceVersion, ResourceVersionNumber,
};
#[allow(unused_imports)]
pub use revision::{
    DesiredState, IntegrityError, LoadedRevision, ManifestEntry, RevisionCandidate,
    RevisionManifest, ValidationError,
};
#[allow(unused_imports)]
pub use tenancy::{
    DisplayName, InvalidDisplayName, PROJECT_SCHEMA, Project, ProjectBody, QualifiedProject,
    TENANT_SCHEMA, Tenancy, TenancyError, Tenant, TenantBody,
};
