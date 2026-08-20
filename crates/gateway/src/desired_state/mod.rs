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
//! | [`tenancy`] | the first two body schemas: what a tenant and a tenant-owned project are, who owns what, and where a tenant is in its life |
//! | [`access`] | who may change it: the identity directory, the roles, and the authorization decision a mutation has to carry |
//! | [`policy`] | the complete policy document of a tenant or a project, its generation, and how a change to it may be activated |
//! | [`models`] | what a tenant may use and what a project calls it: typed model enablements and project-scoped aliases |
//! | [`providers`] | where a tenant's traffic goes: the endpoint and dialect of one upstream connection, with no material in it |
//! | [`pricing`] | the approved price book: effective-dated rates a deployment charges, and the pricing identity a snapshot serves under |
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
//! It is not read directly by the request path. Stateful `serve` hydrates and
//! compiles these bodies off the request path into an immutable snapshot; no
//! request consults this domain or its stores. It is also not a complete body
//! model: [`tenancy`], [`access`],
//! [`policy`], [`models`], [`providers`], and [`pricing`] are the schemas the
//! domain reads, and catalogue bodies remain owned by their own slice —
//! [`access::Surface`] names those surfaces so they can be authorized against,
//! which is not the same as authoring them. A policy document is a contract
//! rather than an activation: nothing enforces one, and [`PolicyTransition`]
//! states what enforcing a change *would* require of a fleet.
//!
//! Nor is [`access`] request-path authorization. An inference request is
//! authorized against the snapshot it captured when it started
//! ([`crate::principals`]); a [`access::Role`] is about administering the control
//! plane, and no directory is consulted while a request is in flight.
//!
//! The types are the contract that #165, #166, and #142 build against, and the
//! test-only `oracle` module is the executable statement of how a
//! `ControlPlaneStore` must behave when they do.

pub mod access;
pub mod canonical;
pub mod credentials;
mod environment;
pub mod ids;
pub mod models;
pub mod mutation;
pub mod policy;
pub mod pricing;
pub mod providers;
pub mod publication;
mod publication_auth;
pub mod resource;
pub mod resource_document;
pub mod revision;
mod secret_binding;
pub mod secrets;
pub mod tenancy;

mod record;

#[cfg(test)]
pub(crate) mod fixtures;
#[cfg(test)]
pub(crate) mod oracle;

// The domain's facade. `allow(unused_imports)` because this is a binary crate:
// a re-export nothing in the tree happens to name yet is still part of the
// contract #165, #166, and #142 build against.
#[allow(unused_imports)]
pub use access::{
    AccessDenial, AccessRequest, Action, Authorization, AuthorizationSnapshot, Basis, Caller,
    Credential, Denial, DenialPage, DenialReason, Directory, IDENTITY_SCHEMA, IdentityBody,
    IdentityError, IdentityKind, KeyError, Principal, Role, Surface, WorkloadKey,
};
#[allow(unused_imports)]
pub use canonical::{
    Canonical, CanonicalError, CanonicalValue, Checksum, InvalidChecksum, SerializerVersion,
};
#[allow(unused_imports)]
pub use credentials::{
    CredentialError, Credentials, PROVIDER_CREDENTIAL_SCHEMA, ProviderCredential,
    ProviderCredentialBody,
};
#[allow(unused_imports)]
pub use environment::{EnvironmentId, InvalidEnvironmentId};
#[allow(unused_imports)]
pub use ids::{
    AuditEventId, InvalidId, InvalidSlug, InvalidUuid7, MutationId, PrincipalId, ProjectId,
    ResourceId, RevisionId, SecretId, Slug, TenantId, Uuid7, Uuid7Generator,
};
#[allow(unused_imports)]
pub use models::{
    AliasTarget, ApprovedPrice, CatalogOffering, ForbiddenModelTransition, InvalidOfferingId,
    LifecycleChange, MODEL_ALIAS_SCHEMA, MODEL_ENABLEMENT_SCHEMA, ModelAlias, ModelAliasBody,
    ModelEnablement, ModelEnablementBody, ModelError, ModelInvariant, ModelLifecycle, ModelOwner,
    Models, ObservedPrice, OfferingId, WireFamily,
};
#[allow(unused_imports)]
pub use mutation::{
    Actor, AuditEvent, ExpectedRevision, IdempotencyKey, InvalidActor, InvalidIdempotencyKey,
    Mutation, MutationKind,
};
#[allow(unused_imports)]
pub use policy::{
    BOOTSTRAP_OWNED_FIELDS, BudgetBound, BudgetPolicy, ConcurrencyPolicy,
    ContentMiddlewareRegistration, Fenced, InvalidContentMiddleware, InvalidPolicy, NotAnAdvance,
    Offered, POLICY_SCHEMA, PolicyBody, PolicyContent, PolicyDocument, PolicyEpoch, PolicyError,
    PolicyFence, PolicyGeneration, PolicyScope, PolicySet, PolicySnapshot, PolicyTransition,
    RevocationPolicy, TransitionClass, TransitionReason,
};
#[allow(unused_imports)]
pub use pricing::{
    Approval, ApprovedRate, ApprovedRates, Currency, EffectiveInstant, EffectiveInterval,
    InvalidInstant, InvalidInterval, PRICE_BOOK_SCHEMA, PriceBook, PriceBookBody, PriceBooks,
    PriceOrigin, PriceProvenance, PriceRule, PricedTarget, PricingError, PricingSnapshot,
    RateRejection, RateUnit, RulePrecedence,
};
#[allow(unused_imports)]
pub use providers::{PROVIDER_SCHEMA, Provider, ProviderBody, ProviderError, Providers};
#[allow(unused_imports)]
pub use publication::{
    ActivationReadyRevision, BlobPublication, BlobPublicationError, BlobPublicationRequest,
    ExpectedHead, HeadDocument, IdempotencyHistoryLimit, IdempotencyHistoryStatus, ImmutableObject,
    ImmutableObjectKind, PublicationActorBinding, PublicationAuthorization,
    PublicationGrantBinding, PublicationHeadState, PublicationOutcome, PublicationSequenceGuard,
    VerifiedActiveRevision, VerifiedRevisionManifest,
};
#[allow(unused_imports)]
pub use publication_auth::{
    ED25519_V1_ALGORITHM, InvalidPublicationKeyId, MAX_PUBLICATION_TRUST_KEYS,
    PublicationAuthenticationError, PublicationKeyId, PublicationSignatureAlgorithm,
    PublicationSigner, PublicationTrustStore, TrustedPublicationKey,
};
#[allow(unused_imports)]
pub use resource::{
    BlobError, BlobKind, BlobRef, ResourceBody, ResourceKind, ResourceRef, ResourceScope,
    ResourceVersion, ResourceVersionNumber,
};
#[allow(unused_imports)]
pub use resource_document::{BlobResourceDocument, BlobResourceDocumentError};
#[allow(unused_imports)]
pub use revision::{
    BodySkew, DesiredState, IntegrityError, LoadedRevision, ManifestEntry, RevisionCandidate,
    RevisionManifest, ValidationError,
};
#[allow(unused_imports)]
pub use secret_binding::{AuthenticatedSecretBinding, BlobSecretPublicationBinding};
#[allow(unused_imports)]
pub use secrets::{
    ForbiddenTransition, LifecycleTransition, SecretLifecycle, SecretOwner, SecretRef,
    SecretVersion,
};
#[allow(unused_imports)]
pub use tenancy::{
    DisplayName, InvalidDisplayName, PROJECT_SCHEMA, Project, ProjectBody, QualifiedProject,
    TENANT_LIFECYCLE_SCHEMA, TENANT_SCHEMA, Tenancy, TenancyError, Tenant, TenantBody,
    TenantLifecycle,
};
