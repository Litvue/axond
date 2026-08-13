//! Durable desired state: the [`ControlPlaneStore`] contract.
//!
//! The control plane owns what a stateful deployment *wants* to be serving.
//! Durable state is a chain of **immutable revisions**: publishing a change
//! creates new resource versions and a new manifest referencing them, and
//! nothing is edited in place. That is what makes "what was serving at 14:00"
//! answerable, rollback a matter of publishing an earlier manifest, and
//! hydration of any retained revision deterministic.
//!
//! Two rules shape every method here:
//!
//! - **Nothing on this trait is callable while an inference request is in
//!   flight.** A request reads the immutable snapshot it captured at start and
//!   never queries the control plane, so a `ControlPlaneStore` outage stalls
//!   convergence and administration while replicas keep serving. The contract
//!   is therefore allowed to be slow, and it is declared
//!   [`BackendPath::ControlPlane`](super::BackendPath::ControlPlane).
//! - **Redis cannot implement it.** [`ControlPlaneBackend`] has exactly one
//!   variant, and parsing rejects `redis` with a typed error instead of falling
//!   back, so "Redis is hot state only" is a compile- and boot-time property.
//!
//! ## What this module owns, and what the domain owns
//!
//! This is the *store* contract: which durable implementations exist, how a
//! candidate is published, how conflicts and outages are expressed, and how a
//! retained revision is read back. Everything a revision is *made of* —
//! [`Uuid7`](crate::desired_state::Uuid7)-based typed ids, tenant-scoped slug
//! rules, the canonical serializer and its checksums, resource envelopes,
//! content-addressed blobs, validation — lives in [`crate::desired_state`] and is
//! database-agnostic by construction.
//!
//! The split matters because the two evolve differently: a second durable
//! backend changes this module and nothing in the domain, while a new resource
//! schema changes neither. It is also why the trait's error type distinguishes a
//! caller's invalid candidate ([`ValidationError`]) from storage that no longer
//! adds up ([`IntegrityError`]): the first is a rejected request, the second is
//! an operator alert.

pub mod hydration;
pub mod postgres;
mod rows;
pub mod schema;

use hydration::HydrationLimit;

use async_trait::async_trait;

use super::{BackendFailure, BackendKind, Capabilities, FailureCategory};
use crate::desired_state::{
    AccessDenial, AuditEvent, ExpectedRevision, IdempotencyKey, IntegrityError, LoadedRevision,
    ResourceRef, RevisionCandidate, RevisionId, RevisionManifest, TenantId, ValidationError,
};

/// The durable implementations a deployment may select for the control plane.
///
/// One variant, on purpose. A second durable store is a new variant *and* a new
/// `durable_control_plane` implementation, which is a reviewable change rather
/// than a config string that happened to parse.
///
/// [`ControlPlaneBackend::parse`] is the only resolution path: deserialization
/// delegates to it, so a TOML value and a programmatic lookup accept exactly the
/// same names and produce exactly the same explanation when they do not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ControlPlaneBackend {
    #[default]
    Postgres,
}

impl<'de> serde::Deserialize<'de> for ControlPlaneBackend {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = <std::borrow::Cow<'de, str>>::deserialize(deserializer)?;
        Self::parse(&name).map_err(serde::de::Error::custom)
    }
}

impl ControlPlaneBackend {
    pub const fn kind(self) -> BackendKind {
        match self {
            Self::Postgres => BackendKind::Postgres,
        }
    }

    /// Resolve an operator-supplied backend name.
    ///
    /// `redis` is rejected with its own arm rather than as an unknown name: the
    /// operator asked for something coherent-sounding and must be told why the
    /// answer is no, not that they made a typo.
    pub fn parse(name: &str) -> Result<Self, UnsupportedControlPlaneBackend> {
        match name {
            "postgres" => Ok(Self::Postgres),
            // A near miss on the one durable backend is a typo, not a request
            // for something else.
            "postgresql" | "pg" => Err(UnsupportedControlPlaneBackend::Unknown {
                name: name.to_owned(),
            }),
            "redis" => Err(UnsupportedControlPlaneBackend::HotStateOnly {
                name: "redis".to_owned(),
            }),
            "memory" | "in-memory" => Err(UnsupportedControlPlaneBackend::NotDurable {
                name: name.to_owned(),
            }),
            other => Err(UnsupportedControlPlaneBackend::Unknown {
                name: other.to_owned(),
            }),
        }
    }
}

/// Why a requested control-plane backend cannot be selected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UnsupportedControlPlaneBackend {
    #[error(
        "`{name}` holds loss-tolerant hot state and cannot own durable control-plane state; \
         the only durable control-plane backend is `postgres`"
    )]
    HotStateOnly { name: String },
    #[error("`{name}` is not durable and cannot own control-plane state")]
    NotDurable { name: String },
    #[error("unknown control-plane backend `{name}`; the only durable backend is `postgres`")]
    Unknown { name: String },
}

/// Why a control-plane operation failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ControlPlaneError {
    #[error("control-plane store `{backend}` unavailable: {message}")]
    Unavailable {
        backend: &'static str,
        message: String,
    },
    /// Another writer published first. The caller re-reads and rebuilds; it does
    /// not replay the same candidate.
    #[error("expected {expected} to be current, but the newest is {actual:?}")]
    Conflict {
        expected: ExpectedRevision,
        actual: Option<RevisionId>,
    },
    #[error("revision {0} is not retained")]
    RevisionNotFound(RevisionId),
    /// The candidate is not valid desired state. Typed, because "invalid" is the
    /// answer an administrator has to act on: the variant names the resource and
    /// the rule.
    #[error("invalid candidate revision: {0}")]
    Invalid(#[from] ValidationError),
    /// A resource version already exists with different content. Versions are
    /// immutable, so the caller must publish a new version rather than redefine
    /// one an earlier revision still pins.
    #[error("{reference} is already published with different content; publish a new version")]
    ImmutableResourceVersion { reference: ResourceRef },
    /// The key was already used to publish *different* desired state. Replaying
    /// the earlier revision would tell the caller their change was applied when
    /// it never was, so the write is refused instead.
    #[error(
        "idempotency key `{key}` already published revision {published} with different desired state"
    )]
    IdempotencyKeyReused {
        key: IdempotencyKey,
        published: RevisionId,
    },
    #[error("control-plane store `{backend}` refused the operation: {message}")]
    Denied {
        backend: &'static str,
        message: String,
    },
    /// Two resources claim one name. The projection enforces uniqueness on the
    /// names an operator types — a tenant slug, a project slug within its tenant
    /// — and a candidate that re-uses one already held by a row this deployment
    /// retains cannot be published.
    ///
    /// Separate from [`ControlPlaneError::Denied`] because the two need opposite
    /// answers: a denial is the deployment's own problem, where this is the
    /// caller's, fixed by picking another name or by deleting the resource that
    /// holds this one. It is reported with the name, so the fix does not require
    /// reading a driver message.
    #[error("the {noun} name `{name}` is already held by another {noun}")]
    NameTaken {
        noun: &'static str,
        name: String,
        /// The resource that holds the name, when the projection can name it.
        holder: Option<String>,
    },
    /// A retained revision could not be interpreted. Never masked as
    /// "unavailable": an operator has to know that stored state is unreadable.
    ///
    /// The cause is boxed so the rare unreadable-storage arm does not widen every
    /// `Result` on this trait; [`ControlPlaneError::corrupt`] is the constructor.
    #[error("stored revision {revision} is unreadable: {source}")]
    Corrupt {
        revision: RevisionId,
        source: Box<IntegrityError>,
    },
    /// Stored data that belongs to no single revision could not be interpreted:
    /// the desired-revision pointer, a schema record, an idempotency record. Same
    /// category as [`ControlPlaneError::Corrupt`] and for the same reason —
    /// retrying cannot help and an operator has to know — but there is no
    /// revision to name.
    #[error("control-plane storage is unreadable: {detail}")]
    CorruptStorage { detail: String },
    /// A retained revision this build cannot interpret: a resource body written to
    /// a schema this release does not read, or read *before* that body was typed.
    ///
    /// Not corruption and not an outage. The rows may be perfectly consistent, so
    /// this must not page whoever owns storage integrity; what it needs is a build
    /// that reads the revision, or a revision the deployed build reads. Nothing
    /// partial is returned, and a replica that already holds a snapshot keeps
    /// serving it — the same last-known-good behaviour every other refusal has.
    #[error("stored revision {revision} is not compatible with this build: {source}")]
    Incompatible {
        revision: RevisionId,
        source: Box<IntegrityError>,
    },
    /// A retained revision is larger than this build reads. Not corruption — the
    /// rows may be perfectly consistent — and not an outage: it is a refusal to
    /// spend unbounded memory hydrating storage, and it needs an operator who can
    /// either raise the bound deliberately or split the revision.
    ///
    /// Nothing hydrated so far is returned with it. A bound that yielded the part
    /// it managed to read would be a partial candidate, which is the outcome
    /// [`hydration`] exists to make unrepresentable.
    #[error("stored revision {revision} exceeds what hydration reads: {limit}")]
    TooLarge {
        revision: RevisionId,
        limit: HydrationLimit,
    },
}

impl ControlPlaneError {
    /// Report a retained revision that does not add up.
    pub fn corrupt(revision: RevisionId, source: IntegrityError) -> Self {
        Self::Corrupt {
            revision,
            source: Box::new(source),
        }
    }

    /// Report an integrity failure under the classification the failure itself
    /// carries: [`ControlPlaneError::Incompatible`] for a revision this build
    /// cannot read, [`ControlPlaneError::Corrupt`] for storage that does not add
    /// up.
    ///
    /// Hydration reports through this rather than through
    /// [`ControlPlaneError::corrupt`] so the two never collapse into one alert:
    /// see [`IntegrityError::is_incompatible`].
    pub fn integrity(revision: RevisionId, source: IntegrityError) -> Self {
        if source.is_incompatible() {
            Self::Incompatible {
                revision,
                source: Box::new(source),
            }
        } else {
            Self::corrupt(revision, source)
        }
    }

    /// Refuse a retained revision that exceeds a hydration bound.
    pub fn too_large(revision: RevisionId, limit: HydrationLimit) -> Self {
        Self::TooLarge { revision, limit }
    }
}

impl BackendFailure for ControlPlaneError {
    fn category(&self) -> FailureCategory {
        match self {
            Self::Unavailable { .. } => FailureCategory::Unavailable,
            Self::Conflict { .. } | Self::NameTaken { .. } => FailureCategory::Conflict,
            Self::RevisionNotFound(_) => FailureCategory::NotFound,
            Self::Invalid(_)
            | Self::ImmutableResourceVersion { .. }
            | Self::IdempotencyKeyReused { .. } => FailureCategory::Invalid,
            // A bound is policy, and policy is a refusal a retry cannot clear.
            // An incompatible revision is the same shape of answer: intact
            // storage this build declines to interpret, cleared by a deployment
            // and never by a retry.
            Self::Denied { .. } | Self::TooLarge { .. } | Self::Incompatible { .. } => {
                FailureCategory::Denied
            }
            Self::Corrupt { .. } | Self::CorruptStorage { .. } => FailureCategory::Corrupt,
        }
    }
}

/// Durable desired state, read and written off the inference path.
///
/// An implementation must provide [`Capability::TransactionalWrites`],
/// [`Capability::OptimisticConcurrency`], [`Capability::IdempotentWrites`], and
/// [`Capability::TransactionalAudit`]; [`ChangeNotification`] is optional and
/// only decides whether convergence polls.
///
/// [`ChangeNotification`]: super::Capability::ChangeNotification
///
/// [`Capability::TransactionalWrites`]: super::Capability::TransactionalWrites
/// [`Capability::OptimisticConcurrency`]: super::Capability::OptimisticConcurrency
/// [`Capability::IdempotentWrites`]: super::Capability::IdempotentWrites
/// [`Capability::TransactionalAudit`]: super::Capability::TransactionalAudit
#[async_trait]
pub trait ControlPlaneStore: Send + Sync {
    fn name(&self) -> &'static str;

    fn capabilities(&self) -> Capabilities;

    /// Control-plane reachability, for administrative diagnostics.
    ///
    /// Never consulted by `/readyz`: readiness reflects whether the replica
    /// holds an active snapshot, not whether the control plane is reachable.
    async fn health(&self) -> Result<(), ControlPlaneError>;

    /// The newest published revision, or `None` before the first publication.
    async fn desired_revision(&self) -> Result<Option<RevisionId>, ControlPlaneError>;

    /// A retained revision's manifest: identity, parentage, entries, and
    /// checksum, without hydrating the resource bodies.
    ///
    /// This is the cheap read — "what is desired, and is it what I already
    /// hold?" — that convergence polls with.
    async fn load_manifest(&self, id: RevisionId) -> Result<RevisionManifest, ControlPlaneError>;

    /// Hydrate a retained revision into a complete, verified candidate.
    ///
    /// This is the seam #142 compiles a snapshot from, and the reason it returns
    /// [`LoadedRevision`] rather than a manifest plus a bag of rows: that type
    /// cannot be constructed without passing integrity verification, so a caller
    /// cannot accidentally publish a snapshot compiled from a partially
    /// hydrated revision. A revision is immutable, so a successful load is
    /// repeatable and cacheable forever; a load that does not verify is
    /// [`ControlPlaneError::Corrupt`], never an outage.
    async fn load_revision(&self, id: RevisionId) -> Result<LoadedRevision, ControlPlaneError>;

    /// The desired revision, hydrated — the seam #142 loads from.
    ///
    /// Provided rather than required, because "the head, hydrated" is
    /// [`desired_revision`](Self::desired_revision) followed by
    /// [`load_revision`](Self::load_revision) for any store. A store that can
    /// answer both in one consistent read should override it and do so: a head
    /// read that is not consistent with the hydration following it can report
    /// convergence onto a revision that was never the head, and
    /// [`PostgresControlPlane`] therefore answers this in a single transaction.
    ///
    /// `None` means no revision has been published, which is distinct from a
    /// revision that fails to hydrate: that is an error, never an empty answer.
    ///
    /// [`PostgresControlPlane`]: postgres::PostgresControlPlane
    async fn load_desired_revision(&self) -> Result<Option<LoadedRevision>, ControlPlaneError> {
        match self.desired_revision().await? {
            Some(id) => self.load_revision(id).await.map(Some),
            None => Ok(None),
        }
    }

    /// Publish a candidate as the new newest revision.
    ///
    /// Atomic with its audit event, conditioned on
    /// [`RevisionCandidate::expected`], and idempotent under the candidate's
    /// [`IdempotencyKey`]: a repeat of the same key carrying the same desired
    /// state returns the revision the first call published, and a repeat carrying
    /// different state is refused with
    /// [`ControlPlaneError::IdempotencyKeyReused`] rather than replaying an
    /// outcome the caller did not ask for.
    ///
    /// The store validates the candidate as desired state — that is a domain
    /// rule, and #165's DDL is not where it belongs — but it does not compile a
    /// snapshot: compiling and rejecting routing state is the replica's job
    /// (#142). The store's job is to make the transition all-or-nothing.
    async fn publish_revision(
        &self,
        candidate: RevisionCandidate,
    ) -> Result<RevisionManifest, ControlPlaneError>;

    /// Audit events for a revision, newest-first, for `/admin/v1` reads.
    async fn audit_trail(&self, id: RevisionId) -> Result<Vec<AuditEvent>, ControlPlaneError>;

    /// Record an administrative action that was refused.
    ///
    /// Separate from [`publish_revision`](Self::publish_revision) because a
    /// refusal publishes nothing: there is no revision for an audit event to hang
    /// off, and minting an empty one to hold a refusal would put a revision in the
    /// chain that describes no state. It is still the half of the trail that
    /// matters most — "who tried to reach another tenant, and when" is not
    /// answerable from successful changes.
    ///
    /// The caller is told only that it was forbidden; the reason is recorded here.
    /// Recording is best-effort in exactly one sense: a store that cannot record a
    /// denial must return an error rather than succeed, but a *caller* must still
    /// refuse the request. A denial that cannot be written is not a denial that
    /// becomes a grant.
    async fn record_denial(&self, denial: &AccessDenial) -> Result<(), ControlPlaneError>;

    /// Refused actions against one tenant, newest-first, for `/admin/v1` reads.
    ///
    /// Tenant-scoped rather than global: a tenant administrator reading their own
    /// trail must see attempts against their tenant, and must not see another
    /// tenant's. Deployment-scoped denials — refusals that named no tenant — are
    /// read with `None`, which only a platform-scoped caller may ask for.
    ///
    /// Scope-exact, and every refusal against the scope asked for is returned
    /// whoever attempted it — including the cross-tenant attempt, which is the
    /// event the trail exists for. Filtering the actor here as well would put such
    /// a refusal on no page at all: not the targeted tenant's, by actor; not the
    /// attempting tenant's, by scope; and not the deployment page, which is only
    /// the rows that named no tenant.
    ///
    /// Withholding another tenant's workload identifiers is the row-level-security
    /// policy's job, because the wall is the only layer that hands a pinned session
    /// a row it is not the subject of: it shares the deployment-scoped refusals, so
    /// it withholds the ones another tenant's workload attempted.
    async fn denials(
        &self,
        tenant: Option<TenantId>,
        limit: usize,
    ) -> Result<Vec<AccessDenial>, ControlPlaneError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redis_cannot_be_selected_as_a_control_plane_backend() {
        let error = ControlPlaneBackend::parse("redis").expect_err("redis must be refused");
        assert!(matches!(
            error,
            UnsupportedControlPlaneBackend::HotStateOnly { .. }
        ));
        assert!(error.to_string().contains("hot state"));

        assert!(matches!(
            ControlPlaneBackend::parse("in-memory"),
            Err(UnsupportedControlPlaneBackend::NotDurable { .. })
        ));
        assert!(matches!(
            ControlPlaneBackend::parse("sqlite"),
            Err(UnsupportedControlPlaneBackend::Unknown { .. })
        ));
    }

    #[test]
    fn every_selectable_control_plane_backend_is_durable() {
        let backend = ControlPlaneBackend::parse("postgres").expect("durable backend");
        assert!(backend.kind().durable_control_plane());
        assert_eq!(
            ControlPlaneBackend::default(),
            ControlPlaneBackend::Postgres
        );
    }

    #[test]
    fn deserialization_resolves_through_parse() {
        assert_eq!(
            serde_json::from_str::<ControlPlaneBackend>("\"postgres\"").unwrap(),
            ControlPlaneBackend::Postgres
        );
        // One resolution path, so a configured value is refused with the same
        // explanation a programmatic lookup gets — including for a near miss.
        for name in ["redis", "in-memory", "postgresql", "sqlite"] {
            let refusal = serde_json::from_str::<ControlPlaneBackend>(&format!("\"{name}\""))
                .expect_err("only postgres is a durable control plane")
                .to_string();
            let expected = ControlPlaneBackend::parse(name).unwrap_err().to_string();
            assert!(
                refusal.contains(&expected),
                "`{name}` was refused as `{refusal}` instead of `{expected}`"
            );
        }
    }

    #[test]
    fn a_callers_mistake_and_unreadable_storage_are_different_categories() {
        // The distinction the trait's error type exists to keep: an invalid
        // candidate is a rejected request, unreadable storage is an alert.
        let invalid = ControlPlaneError::Invalid(ValidationError::Empty);
        assert_eq!(invalid.category(), FailureCategory::Invalid);
        assert!(!invalid.retryable());

        let corrupt = ControlPlaneError::corrupt(
            crate::desired_state::RevisionId::new(
                crate::desired_state::Uuid7::from_parts(1, 0, 1).unwrap(),
            ),
            IntegrityError::Invalid(ValidationError::Empty),
        );
        assert_eq!(corrupt.category(), FailureCategory::Corrupt);
        assert!(!corrupt.retryable());
        assert!(corrupt.to_string().contains("unreadable"));

        let outage = ControlPlaneError::Unavailable {
            backend: "postgres",
            message: "connection refused".to_owned(),
        };
        assert_eq!(outage.category(), FailureCategory::Unavailable);
        assert!(outage.retryable());
    }
}
