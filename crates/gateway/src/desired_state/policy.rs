//! Policy bodies: what a tenant or a project *may spend and hold* (#208).
//!
//! [`tenancy`](super::tenancy) gives a tenant and a project a durable identity;
//! this module gives each of them one durable **policy document**: the budget
//! cap, the concurrency ceiling, and the token floor a revision wants enforced.
//! It is the typed contract a later runtime slice activates against, and nothing
//! more — see *what this module deliberately does not do*, below.
//!
//! # A policy document is complete, never merged
//!
//! One document states a scope's policy in full:
//!
//! | Field | Meaning |
//! | --- | --- |
//! | `schema` | `axond.policy.v1` |
//! | `tenant_id` | the owning [`TenantId`] |
//! | `project_id` | the owning [`ProjectId`], absent for a tenant-wide document |
//! | `epoch` | the operator-advanced [`PolicyEpoch`] this content was published under |
//! | `budget_limit_microdollars` | the per-subject cap inside this scope |
//! | `namespace_budget_limit_microdollars` | the scope-wide cap, absent when there is none |
//! | `reservation_ttl_seconds` | how long a budget hold survives a replica that dies mid-request |
//! | `max_in_flight_per_subject` | the concurrency ceiling per subject |
//! | `lease_ttl_seconds` | how long an abandoned concurrency lease stays live |
//! | `minimum_token_epoch` | the mint epoch below which a token is refused |
//!
//! Two rules follow from *complete*, and they are the reason this is a document
//! rather than a bag of settings:
//!
//! - **Fields are never merged across revisions.** Reading policy takes the whole
//!   document of one revision or none of it. There is no "the new revision
//!   changed the cap, keep the old TTL": a revision is whole desired state, so a
//!   field absent from the newer document is not inherited from the older one.
//! - **Fields are never merged across scopes.** A project's policy does not
//!   inherit its tenant's field by field either. [`PolicySet::effective`] selects
//!   *one* document — the project's if it has one, otherwise its tenant's — so an
//!   effective policy is always a document some operator published as a unit and
//!   can read back verbatim. An absent optional field is therefore a complete
//!   statement ("this scope has no scope-wide cap"), never an inheritance.
//!
//! A scope with no document at all has no *published* policy, and the bootstrap
//! file's limits stand. That is the only fallback, and it is a fallback between
//! whole configurations rather than between fields.
//!
//! # Generation: an epoch plus the revision that published it
//!
//! A document declares an `epoch`; the revision that carries it has a
//! [`RevisionId`]. Together they are a [`PolicyGeneration`], and that pair — not
//! either half — is what a writer holds and what a fence compares.
//!
//! The epoch alone is not enough: two publications can carry the same epoch (a
//! forked control plane, a restored backup), and a writer admitted on epoch
//! equality would be enforcing a document nobody currently serves. The revision
//! id alone is not enough either: it says which publication, but not whether the
//! change was a material one, and ordering revision ids is a storage detail
//! rather than a policy decision. The epoch is what an operator advances when the
//! content changes, and [`PolicyTransition`] refuses a material change that does
//! not advance it — which is what makes the epoch a usable order.
//!
//! A generation therefore carries the document's content, digested as a
//! [`PolicyContent`], as well. A
//! revision is whole desired state, so every revision restates every policy
//! document it carries: a revision that changed an unrelated resource still hands
//! out a generation with a new revision id for a document whose epoch and content
//! never moved. That carry-forward is the ordinary case, and it is exactly what
//! distinguishes it from a fork — same epoch, *different* content.
//!
//! # Stale writers fail closed
//!
//! [`PolicyFence`] admits a writer that holds the policy the fence is enforcing —
//! the active epoch and the active content, whichever revision carried it — and
//! refuses every other case: an older epoch, a newer epoch this replica has not
//! adopted, and the same epoch stating a different policy. Refusing a seemingly
//! newer generation is deliberate — a writer that may enforce anything it can
//! claim is newer is not fenced at all — and refusing anything but the enforced
//! policy means an unknown generation denies instead of admitting
//! unenforced. Adoption follows the same rule: it moves onto a higher epoch, and
//! onto the active document as a later revision restates it, never onto a
//! different policy. That is the same posture as an unreachable budget store
//! ([`UnavailablePolicy::Deny`](crate::budget::UnavailablePolicy)): an
//! unenforceable cap must not silently admit.
//!
//! # What this module deliberately does not do
//!
//! **Bootstrap keeps what bootstrap owns.** Which backend enforces a cap, the DSN
//! it connects with, the table or key prefix it lays state out under, and the
//! stance to take when that store cannot be reached are *not* policy. They are
//! local, operator-owned, and reviewed with the file they live in; a published
//! document that could flip `on_unavailable` to `allow` would turn a policy
//! publication into a way to switch enforcement off. Naming one of those fields in
//! a policy body is a typed refusal ([`PolicyError::BootstrapOwned`]) rather than
//! an unknown field, so the boundary is stated in the reader rather than only in
//! prose.
//!
//! **Nothing here is activated.** No request path reads a document, no store
//! writes one, and no snapshot is compiled from one: this slice is the contract,
//! and [`PolicyTransition`] classifies what activating a change would *require*
//! rather than performing it.
//!
//! # Where these rules are enforced
//!
//! [`PolicySet::of`] reads every policy body in a [`DesiredState`], and
//! [`DesiredState::validate`] calls it — so publication and hydration inherit it
//! exactly as they inherit tenancy, with no request path involved. Schema
//! strictness, and the compatibility-versus-damage distinction
//! ([`PolicyError::is_incompatible`]), follow the rules the
//! [tenancy module](super::tenancy) states for every body schema.

use std::collections::BTreeMap;
use std::fmt;

use super::canonical::{Canonical, CanonicalValue, Checksum};
use super::ids::{InvalidId, ProjectId, ResourceId, RevisionId, Slug, TenantId};
use super::resource::{
    ResourceBody, ResourceKind, ResourceRef, ResourceScope, ResourceVersion, ResourceVersionNumber,
};
use super::revision::DesiredState;

/// The policy body schema this build reads and writes.
pub const POLICY_SCHEMA: &str = "axond.policy.v1";

const SCHEMA_FIELD: &str = "schema";
const TENANT_ID_FIELD: &str = "tenant_id";
const PROJECT_ID_FIELD: &str = "project_id";
const EPOCH_FIELD: &str = "epoch";
const BUDGET_LIMIT_FIELD: &str = "budget_limit_microdollars";
const NAMESPACE_BUDGET_LIMIT_FIELD: &str = "namespace_budget_limit_microdollars";
const RESERVATION_TTL_FIELD: &str = "reservation_ttl_seconds";
const MAX_IN_FLIGHT_FIELD: &str = "max_in_flight_per_subject";
const LEASE_TTL_FIELD: &str = "lease_ttl_seconds";
const MINIMUM_TOKEN_EPOCH_FIELD: &str = "minimum_token_epoch";

/// The settings a published document may never name, because the bootstrap file
/// owns them: which backend enforces policy, how it is reached, how it lays state
/// out, and what it does when it cannot be reached.
///
/// Held as data rather than as prose so the reader enforces the boundary and a
/// test can assert on it.
pub const BOOTSTRAP_OWNED_FIELDS: &[&str] = &[
    "backend",
    "create_table",
    "dsn_env",
    "key_prefix",
    "on_unavailable",
    "table",
];

/// Why a policy body, or the set of documents it belongs to, was refused.
///
/// Every arm names the resource it is about, so a refusal an operator reads
/// points at one row rather than at "the revision".
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PolicyError {
    #[error("{reference} is a {} resource, not a {}", found.as_str(), expected.as_str())]
    Kind {
        reference: ResourceRef,
        expected: ResourceKind,
        found: ResourceKind,
    },
    #[error("{reference} is a blob body; a policy record is inline")]
    NotInline { reference: ResourceRef },
    #[error("{reference} is not a record")]
    NotARecord { reference: ResourceRef },
    #[error(
        "{reference} declares schema `{found}`, which this build does not read (expected `{expected}`)"
    )]
    Schema {
        reference: ResourceRef,
        expected: &'static str,
        found: String,
    },
    #[error("{reference} has no `{field}`")]
    MissingField {
        reference: ResourceRef,
        field: &'static str,
    },
    #[error("{reference} carries `{field}`, which `{schema}` does not define")]
    UnknownField {
        reference: ResourceRef,
        schema: &'static str,
        field: String,
    },
    /// A field the bootstrap file owns, named by a published document.
    ///
    /// Its own arm rather than [`UnknownField`](Self::UnknownField): the field is
    /// not one a future schema may add, so the refusal states the boundary
    /// instead of reading as a version skew.
    #[error(
        "{reference} carries `{field}`, which the bootstrap file owns and a published policy may not set"
    )]
    BootstrapOwned {
        reference: ResourceRef,
        field: String,
    },
    #[error("{reference} field `{field}` is not {expected}")]
    FieldType {
        reference: ResourceRef,
        field: &'static str,
        expected: &'static str,
    },
    #[error("{reference} field `{field}` is not an id: {source}")]
    MalformedId {
        reference: ResourceRef,
        field: &'static str,
        #[source]
        source: InvalidId,
    },
    /// The bound is boxed because [`InvalidPolicy`] names a canonical integer,
    /// whose alignment would otherwise pad out every `Result` that can carry a
    /// policy refusal.
    #[error("{reference} field `{field}` is out of range: {source}")]
    FieldRange {
        reference: ResourceRef,
        field: &'static str,
        #[source]
        source: Box<InvalidPolicy>,
    },
    #[error("{reference} carries {declared}, but its resource identity is {identity}")]
    IdentityMismatch {
        reference: ResourceRef,
        declared: String,
        identity: ResourceId,
    },
    #[error("{reference} declares the policy of {declared}, but is scoped to {scoped:?}")]
    ScopeMismatch {
        reference: ResourceRef,
        declared: PolicyScope,
        scoped: ResourceScope,
    },
}

impl PolicyError {
    /// Whether this refusal means *this build cannot read the body*, rather than
    /// *these rows do not agree with each other*.
    ///
    /// The rule is the one [`TenancyError::is_incompatible`] states, applied to
    /// one more schema: a schema identifier this build does not read, a field a
    /// newer release added, or a body with no identifier at all is a release skew
    /// and storage is intact; anything refused *inside* a body that declared
    /// `axond.policy.v1` is a rewrite, and points at storage.
    ///
    /// A bootstrap-owned field is damage rather than a skew on purpose: those
    /// field names are excluded from policy by design, so no future release adds
    /// one, and reporting it as a version skew would send an operator to upgrade
    /// a fleet over a document that should never have been authored.
    ///
    /// A *bound* is the exception, for the reason a display name is tenancy's: the
    /// rules a schema's values are held to can tighten within one identifier, so a
    /// value below a minimum this build enforces may be one an earlier build wrote
    /// and accepted, and storage is intact. A value that is not representable at
    /// all — negative, or past what this build counts in — is damage: every
    /// release has written these fields as unsigned counters, so no tightening
    /// produces one.
    ///
    /// [`TenancyError::is_incompatible`]: super::tenancy::TenancyError::is_incompatible
    pub fn is_incompatible(&self) -> bool {
        match self {
            Self::Schema { .. } | Self::UnknownField { .. } => true,
            // Only the schema identifier itself: its absence is a body written
            // before policy had one at all.
            Self::MissingField { field, .. } | Self::FieldType { field, .. } => {
                *field == SCHEMA_FIELD
            }
            Self::FieldRange { source, .. } => source.is_tightenable(),
            Self::Kind { .. }
            | Self::NotInline { .. }
            | Self::NotARecord { .. }
            | Self::BootstrapOwned { .. }
            | Self::MalformedId { .. }
            | Self::IdentityMismatch { .. }
            | Self::ScopeMismatch { .. } => false,
        }
    }

    /// The resource this refusal is about.
    pub const fn reference(&self) -> ResourceRef {
        match self {
            Self::Kind { reference, .. }
            | Self::NotInline { reference }
            | Self::NotARecord { reference }
            | Self::Schema { reference, .. }
            | Self::MissingField { reference, .. }
            | Self::UnknownField { reference, .. }
            | Self::BootstrapOwned { reference, .. }
            | Self::FieldType { reference, .. }
            | Self::MalformedId { reference, .. }
            | Self::FieldRange { reference, .. }
            | Self::IdentityMismatch { reference, .. }
            | Self::ScopeMismatch { reference, .. } => *reference,
        }
    }
}

/// Why a policy value is not one this build will enforce.
///
/// Checked when a body is *built* as well as when one is read, so an authored
/// document and a stored document obey the same bounds rather than two.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidPolicy {
    #[error("{value} is below the minimum of {min}")]
    TooSmall { value: u64, min: u64 },
    #[error("{value} does not fit the range this build enforces")]
    NotRepresentable { value: i128 },
}

impl InvalidPolicy {
    /// Whether a release could have written this value and had it accepted.
    ///
    /// A minimum is a rule rather than a shape, and rules tighten within one
    /// schema identifier, so a stored value below one is read as a release skew
    /// ([`PolicyError::is_incompatible`]). A value outside the range these fields
    /// are counted in was never writable, so it is damage.
    pub const fn is_tightenable(&self) -> bool {
        match self {
            Self::TooSmall { .. } => true,
            Self::NotRepresentable { .. } => false,
        }
    }
}

/// A policy generation counter an operator advances when a document's content
/// changes.
///
/// Monotonic within a scope, and part of the body rather than derived from it: a
/// checksum tells you two documents differ, while an epoch tells you which one an
/// operator published *later*, which is what a fence needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PolicyEpoch(u64);

impl PolicyEpoch {
    /// The first epoch. Zero is not an epoch, so an unset field cannot read as a
    /// valid one.
    pub const FIRST: Self = Self(1);

    pub const fn new(value: u64) -> Result<Self, InvalidPolicy> {
        if value == 0 {
            return Err(InvalidPolicy::TooSmall { value, min: 1 });
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    /// The next epoch, for the publication that changes a document's content.
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl fmt::Display for PolicyEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Which tenant or project a policy document is the policy *of*.
///
/// The tenancy model of #144/#191 exactly: a tenant, and optionally one of its
/// projects. There is no third level and no policy that belongs to a deployment —
/// deployment-wide limits are the bootstrap file's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PolicyScope {
    Tenant(TenantId),
    Project {
        tenant: TenantId,
        project: ProjectId,
    },
}

impl PolicyScope {
    pub const fn tenant(self) -> TenantId {
        match self {
            Self::Tenant(tenant) | Self::Project { tenant, .. } => tenant,
        }
    }

    pub const fn project(self) -> Option<ProjectId> {
        match self {
            Self::Tenant(_) => None,
            Self::Project { project, .. } => Some(project),
        }
    }

    /// The scope a document at this policy scope lives at on its envelope.
    ///
    /// A policy document is scoped to the thing it governs, so scope and subject
    /// are one fact rather than two that could disagree.
    pub const fn resource_scope(self) -> ResourceScope {
        match self {
            Self::Tenant(tenant) => ResourceScope::Tenant(tenant),
            Self::Project { tenant, project } => ResourceScope::Project { tenant, project },
        }
    }

    /// The resource identity this scope's policy is written under.
    ///
    /// Derived from the governed object, so "the policy of project X" is one
    /// durable resource: a second document for the same scope is a second version
    /// of the same resource, which [`DesiredState::validate`] already refuses
    /// within one revision.
    pub const fn resource_id(self) -> ResourceId {
        match self {
            Self::Tenant(tenant) => ResourceId::new(tenant.uuid()),
            Self::Project { project, .. } => ResourceId::new(project.uuid()),
        }
    }

    /// The scope whose document applies when this one has none: a project's
    /// tenant, and nothing above a tenant.
    pub const fn fallback(self) -> Option<Self> {
        match self {
            Self::Tenant(_) => None,
            Self::Project { tenant, .. } => Some(Self::Tenant(tenant)),
        }
    }
}

impl fmt::Display for PolicyScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tenant(tenant) => write!(f, "the policy of tenant {tenant}"),
            Self::Project { tenant, project } => {
                write!(f, "the policy of project {project} in tenant {tenant}")
            }
        }
    }
}

/// What a scope may spend, and how long an unsettled hold survives.
///
/// What is *not* here: the backend that enforces it, where it stores ledgers, and
/// what it does when it cannot be reached. Those stay in the bootstrap file (see
/// the module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BudgetPolicy {
    subject_limit_microdollars: u64,
    namespace_limit_microdollars: Option<u64>,
    reservation_ttl_seconds: u64,
}

impl BudgetPolicy {
    pub const fn new(
        subject_limit_microdollars: u64,
        namespace_limit_microdollars: Option<u64>,
        reservation_ttl_seconds: u64,
    ) -> Result<Self, InvalidPolicy> {
        if reservation_ttl_seconds == 0 {
            return Err(InvalidPolicy::TooSmall {
                value: reservation_ttl_seconds,
                min: 1,
            });
        }
        Ok(Self {
            subject_limit_microdollars,
            namespace_limit_microdollars,
            reservation_ttl_seconds,
        })
    }

    /// The cap on one `(scope, subject)` pair.
    pub const fn subject_limit_microdollars(&self) -> u64 {
        self.subject_limit_microdollars
    }

    /// The cap on everything the scope spends, or `None` when there is none.
    ///
    /// `None` is a complete statement rather than an omission: this scope has no
    /// scope-wide cap. Turning it on or off changes how a shared store composes
    /// the keys a reservation touches, which is why the transition is
    /// [`TransitionClass::MigrationRequired`] rather than live.
    pub const fn namespace_limit_microdollars(&self) -> Option<u64> {
        self.namespace_limit_microdollars
    }

    pub const fn reservation_ttl_seconds(&self) -> u64 {
        self.reservation_ttl_seconds
    }
}

/// How much a scope may have in flight at once, and how long an abandoned lease
/// stays live.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConcurrencyPolicy {
    max_in_flight_per_subject: u64,
    lease_ttl_seconds: u64,
}

impl ConcurrencyPolicy {
    pub const fn new(
        max_in_flight_per_subject: u64,
        lease_ttl_seconds: u64,
    ) -> Result<Self, InvalidPolicy> {
        if max_in_flight_per_subject == 0 {
            return Err(InvalidPolicy::TooSmall {
                value: max_in_flight_per_subject,
                min: 1,
            });
        }
        if lease_ttl_seconds == 0 {
            return Err(InvalidPolicy::TooSmall {
                value: lease_ttl_seconds,
                min: 1,
            });
        }
        Ok(Self {
            max_in_flight_per_subject,
            lease_ttl_seconds,
        })
    }

    pub const fn max_in_flight_per_subject(&self) -> u64 {
        self.max_in_flight_per_subject
    }

    pub const fn lease_ttl_seconds(&self) -> u64 {
        self.lease_ttl_seconds
    }
}

/// The mint epoch below which a token issued for this scope is refused.
///
/// Revocation states a floor rather than a list, so a document stays a fixed
/// shape and a mass revocation is one advancing integer. Advancing it only ever
/// refuses more, which is why it is a live change; lowering it would un-revoke
/// tokens an operator already revoked, and is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RevocationPolicy {
    minimum_token_epoch: u64,
}

impl RevocationPolicy {
    pub const fn new(minimum_token_epoch: u64) -> Self {
        Self {
            minimum_token_epoch,
        }
    }

    pub const fn minimum_token_epoch(&self) -> u64 {
        self.minimum_token_epoch
    }
}

/// The complete policy of one tenant or project, as a revision carries it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolicyBody {
    scope: PolicyScope,
    epoch: PolicyEpoch,
    budget: BudgetPolicy,
    concurrency: ConcurrencyPolicy,
    revocation: RevocationPolicy,
}

impl PolicyBody {
    /// The schema identifier this body encodes and reads.
    pub const SCHEMA: &'static str = POLICY_SCHEMA;

    const KNOWN_FIELDS: &'static [&'static str] = &[
        TENANT_ID_FIELD,
        PROJECT_ID_FIELD,
        EPOCH_FIELD,
        BUDGET_LIMIT_FIELD,
        NAMESPACE_BUDGET_LIMIT_FIELD,
        RESERVATION_TTL_FIELD,
        MAX_IN_FLIGHT_FIELD,
        LEASE_TTL_FIELD,
        MINIMUM_TOKEN_EPOCH_FIELD,
    ];

    pub const fn new(
        scope: PolicyScope,
        epoch: PolicyEpoch,
        budget: BudgetPolicy,
        concurrency: ConcurrencyPolicy,
        revocation: RevocationPolicy,
    ) -> Self {
        Self {
            scope,
            epoch,
            budget,
            concurrency,
            revocation,
        }
    }

    pub const fn scope(&self) -> PolicyScope {
        self.scope
    }

    pub const fn epoch(&self) -> PolicyEpoch {
        self.epoch
    }

    pub const fn budget(&self) -> &BudgetPolicy {
        &self.budget
    }

    pub const fn concurrency(&self) -> &ConcurrencyPolicy {
        &self.concurrency
    }

    pub const fn revocation(&self) -> &RevocationPolicy {
        &self.revocation
    }

    /// This document's generation, once the revision that publishes it is known.
    ///
    /// A body cannot carry its own revision id — the revision does not exist
    /// until the body is in it — so a generation is formed at the seam that knows
    /// both, and never guessed at inside the body.
    pub fn generation(&self, source: RevisionId) -> PolicyGeneration {
        PolicyGeneration {
            epoch: self.epoch,
            source,
            content: self.content(),
        }
    }

    /// Everything a request would see, without the epoch it was published under.
    ///
    /// What distinguishes "the same document, carried into a later revision" from
    /// "a different document claiming one epoch", which is the distinction
    /// [`PolicyFence`] turns on.
    pub fn content(&self) -> PolicyContent {
        PolicyContent::of(self)
    }

    /// The resource identity this document is written under: the governed
    /// object's.
    pub const fn resource_id(&self) -> ResourceId {
        self.scope.resource_id()
    }

    pub fn body(&self) -> ResourceBody {
        ResourceBody::Inline(self.canonical())
    }

    pub fn version(&self, slug: Slug) -> ResourceVersion {
        self.version_at(slug, ResourceVersionNumber::FIRST)
    }

    pub fn version_at(&self, slug: Slug, version: ResourceVersionNumber) -> ResourceVersion {
        ResourceVersion::new(
            ResourceRef::new(ResourceKind::Policy, self.resource_id(), version),
            self.scope.resource_scope(),
            slug,
            self.body(),
        )
    }

    /// Read a policy resource's body, binding it to its envelope: identity to the
    /// reference, governed scope to the envelope's scope.
    pub fn read(resource: &ResourceVersion) -> Result<Self, PolicyError> {
        let record = Record::open(
            resource,
            ResourceKind::Policy,
            Self::SCHEMA,
            Self::KNOWN_FIELDS,
        )?;
        let tenant = record.tenant()?;
        let scope = match record.optional_project()? {
            Some(project) => PolicyScope::Project { tenant, project },
            None => PolicyScope::Tenant(tenant),
        };
        record.identity(scope, scope.resource_id())?;
        if resource.scope != scope.resource_scope() {
            return Err(PolicyError::ScopeMismatch {
                reference: resource.reference,
                declared: scope,
                scoped: resource.scope.clone(),
            });
        }
        let epoch = record.bounded(EPOCH_FIELD, PolicyEpoch::new)?;
        let budget = BudgetPolicy::new(
            record.integer(BUDGET_LIMIT_FIELD)?,
            record.optional_integer(NAMESPACE_BUDGET_LIMIT_FIELD)?,
            record.integer(RESERVATION_TTL_FIELD)?,
        )
        .map_err(|source| PolicyError::FieldRange {
            reference: resource.reference,
            field: RESERVATION_TTL_FIELD,
            source: Box::new(source),
        })?;
        let max_in_flight = record.integer(MAX_IN_FLIGHT_FIELD)?;
        let lease_ttl = record.integer(LEASE_TTL_FIELD)?;
        let concurrency = ConcurrencyPolicy::new(max_in_flight, lease_ttl).map_err(|source| {
            PolicyError::FieldRange {
                reference: resource.reference,
                // Two fields share one bound, so the refusal names the one that
                // broke it rather than the first one checked.
                field: if max_in_flight == 0 {
                    MAX_IN_FLIGHT_FIELD
                } else {
                    LEASE_TTL_FIELD
                },
                source: Box::new(source),
            }
        })?;
        Ok(Self {
            scope,
            epoch,
            budget,
            concurrency,
            revocation: RevocationPolicy::new(record.integer(MINIMUM_TOKEN_EPOCH_FIELD)?),
        })
    }

    /// How a fleet may move from this document to `next`.
    ///
    /// The classification is a property of the two documents, so publication,
    /// review tooling, and a later activation slice reach the same answer.
    pub fn transition(&self, next: &Self) -> PolicyTransition {
        PolicyTransition::between(self, next)
    }

    /// Whether two documents state the same policy, ignoring the epoch they were
    /// published under.
    ///
    /// The epoch is publication metadata, so a republication that advances only
    /// it changes nothing a request would see.
    fn same_content(&self, other: &Self) -> bool {
        self.content() == other.content()
    }
}

impl Canonical for PolicyBody {
    fn canonical(&self) -> CanonicalValue {
        // Absent rather than zero for an optional field: the canonical encoding
        // has no null, so "no scope-wide cap" is the absence of the key, and a
        // zero cap would mean a cap of zero.
        let mut fields = vec![
            (SCHEMA_FIELD, CanonicalValue::string(Self::SCHEMA)),
            (
                TENANT_ID_FIELD,
                CanonicalValue::string(self.scope.tenant().to_string()),
            ),
            (EPOCH_FIELD, CanonicalValue::integer(self.epoch.get())),
            (
                BUDGET_LIMIT_FIELD,
                CanonicalValue::integer(self.budget.subject_limit_microdollars),
            ),
            (
                RESERVATION_TTL_FIELD,
                CanonicalValue::integer(self.budget.reservation_ttl_seconds),
            ),
            (
                MAX_IN_FLIGHT_FIELD,
                CanonicalValue::integer(self.concurrency.max_in_flight_per_subject),
            ),
            (
                LEASE_TTL_FIELD,
                CanonicalValue::integer(self.concurrency.lease_ttl_seconds),
            ),
            (
                MINIMUM_TOKEN_EPOCH_FIELD,
                CanonicalValue::integer(self.revocation.minimum_token_epoch),
            ),
        ];
        if let Some(project) = self.scope.project() {
            fields.push((
                PROJECT_ID_FIELD,
                CanonicalValue::string(project.to_string()),
            ));
        }
        if let Some(limit) = self.budget.namespace_limit_microdollars {
            fields.push((NAMESPACE_BUDGET_LIMIT_FIELD, CanonicalValue::integer(limit)));
        }
        CanonicalValue::map(fields)
    }
}

/// What a policy document states, digested: the scope it governs and every value
/// it enforces, with no reference to when it was published.
///
/// A revision is whole desired state, so every revision restates every policy
/// document it carries, whether or not that document changed. Comparing content
/// is what lets a carried-forward document be recognized as the same policy
/// rather than as a second one claiming its epoch, and a digest is what lets a
/// [`PolicyGeneration`] carry that comparison without carrying a document.
///
/// Fixed-width and total: every field is length-prefixed or fixed-width, and an
/// absent optional cap is distinguished from any value it could hold, so two
/// documents digest alike only when they state the same policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PolicyContent(Checksum);

impl PolicyContent {
    fn of(body: &PolicyBody) -> Self {
        let mut bytes = Vec::new();
        for text in [
            body.scope.tenant().to_string(),
            body.scope
                .project()
                .map_or_else(String::new, |project| project.to_string()),
        ] {
            bytes.extend_from_slice(&(text.len() as u64).to_be_bytes());
            bytes.extend_from_slice(text.as_bytes());
        }
        for number in [
            body.budget.subject_limit_microdollars,
            u64::from(body.budget.namespace_limit_microdollars.is_some()),
            body.budget.namespace_limit_microdollars.unwrap_or(0),
            body.budget.reservation_ttl_seconds,
            body.concurrency.max_in_flight_per_subject,
            body.concurrency.lease_ttl_seconds,
            body.revocation.minimum_token_epoch,
        ] {
            bytes.extend_from_slice(&number.to_be_bytes());
        }
        Self(Checksum::of(&bytes))
    }

    /// The digest itself, for a caller that stores or logs it.
    pub const fn digest(&self) -> Checksum {
        self.0
    }
}

impl fmt::Display for PolicyContent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The identity of one published policy document: the epoch its content was
/// published under, the policy that content *is*, and the revision that carried
/// it.
///
/// No part identifies a generation alone — see the module docs — so the three are
/// one type rather than fields a caller could compare separately. The revision is
/// provenance: it says which publication a writer read, and is what makes two
/// documents claiming one epoch distinguishable. The content is what says whether
/// those two documents are the same policy or a fork.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PolicyGeneration {
    epoch: PolicyEpoch,
    source: RevisionId,
    content: PolicyContent,
}

impl PolicyGeneration {
    pub const fn new(epoch: PolicyEpoch, source: RevisionId, content: PolicyContent) -> Self {
        Self {
            epoch,
            source,
            content,
        }
    }

    pub const fn epoch(&self) -> PolicyEpoch {
        self.epoch
    }

    /// The revision that published this document.
    pub const fn source(&self) -> RevisionId {
        self.source
    }

    /// The policy this generation enforces, digested.
    pub const fn content(&self) -> PolicyContent {
        self.content
    }

    /// Whether this generation's content was published after `other`'s.
    ///
    /// Strictly by epoch: two generations of one epoch stating *different*
    /// policies are not ordered, they are a fork, and [`PolicyFence`] refuses one
    /// rather than picking a winner.
    pub fn supersedes(&self, other: &Self) -> bool {
        self.epoch > other.epoch
    }

    /// Whether both generations enforce the same policy under the same epoch,
    /// whichever revision carried them.
    ///
    /// True for a document restated verbatim by a later revision — the ordinary
    /// case, since a revision that changes an unrelated resource still republishes
    /// every policy document — and false for two different documents sharing an
    /// epoch, which is the fork a fence must refuse.
    pub fn same_policy(&self, other: &Self) -> bool {
        self.epoch == other.epoch && self.content == other.content
    }

    /// Whether this generation is `other` carried into a different revision:
    /// the same policy under the same epoch, published again.
    pub fn carries_forward(&self, other: &Self) -> bool {
        self.same_policy(other) && self.source != other.source
    }
}

impl fmt::Display for PolicyGeneration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "epoch {} from revision {}", self.epoch, self.source)
    }
}

/// Why a writer was fenced out.
///
/// Three arms rather than one, because they are three different operational
/// stories, and only the first is the ordinary one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Fenced {
    /// The ordinary case: the writer holds a generation the fleet has moved past.
    #[error("writer holds {writer}, which the active {active} has moved past")]
    Stale {
        writer: PolicyGeneration,
        active: PolicyGeneration,
    },
    /// A writer claiming a generation this replica has not adopted. Refused
    /// rather than trusted: a writer that may enforce anything it can claim is
    /// newer than the active generation is not fenced at all.
    #[error("writer holds {writer}, which is ahead of the active {active}")]
    Ahead {
        writer: PolicyGeneration,
        active: PolicyGeneration,
    },
    /// The same epoch stating a *different* policy: two publications claiming one
    /// generation, which a restored backup or a forked control plane produces. A
    /// revision that merely restates the active document is not this — see
    /// [`PolicyGeneration::carries_forward`].
    #[error("writer holds {writer}, which claims the epoch of the active {active}")]
    Forked {
        writer: PolicyGeneration,
        active: PolicyGeneration,
    },
}

/// Why a fence refused to move.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("cannot adopt {next} over the active {active}")]
pub struct NotAnAdvance {
    pub active: PolicyGeneration,
    pub next: PolicyGeneration,
}

/// The generation a scope's policy is currently enforced under, and the gate every
/// writer passes.
///
/// Fail-closed by construction: [`admit`](Self::admit) returns `Ok` only for the
/// policy the fence is actively enforcing, so an older, forked, or newer
/// generation denies. Nothing here writes anything — it is the contract a later
/// activation slice enforces with.
///
/// "The policy it is enforcing" rather than "the revision it came from": a
/// revision restates every policy document it carries, so a revision that changed
/// an unrelated resource hands out a generation with a new
/// [`source`](PolicyGeneration::source) for an unchanged document. Admitting and
/// adopting compare the epoch and the content, and treat the revision as
/// provenance — otherwise the ordinary carry-forward would read as a fork, and a
/// replica could never follow the fleet onto the revision it is serving.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolicyFence {
    active: PolicyGeneration,
}

impl PolicyFence {
    pub const fn new(active: PolicyGeneration) -> Self {
        Self { active }
    }

    pub const fn active(&self) -> PolicyGeneration {
        self.active
    }

    /// Admit a writer holding `writer`, or say why it is fenced out.
    ///
    /// Admits the active generation, and the same policy under the same epoch from
    /// another revision — the same document, carried forward. Everything else
    /// denies, including a generation this replica has not adopted.
    pub fn admit(&self, writer: PolicyGeneration) -> Result<(), Fenced> {
        if writer.same_policy(&self.active) {
            return Ok(());
        }
        let (writer, active) = (writer, self.active);
        if writer.epoch < active.epoch {
            Err(Fenced::Stale { writer, active })
        } else if writer.epoch > active.epoch {
            Err(Fenced::Ahead { writer, active })
        } else {
            Err(Fenced::Forked { writer, active })
        }
    }

    /// Move the fence onto a generation that supersedes the active one, or onto
    /// the active document as a later revision restates it.
    ///
    /// A generation that neither advances the epoch nor carries the active
    /// document forward is refused, so adopting is monotonic in what is
    /// *enforced*: a replica cannot be walked backwards onto an older policy, nor
    /// sideways onto a different policy claiming the active epoch, but it can
    /// follow the fleet onto the revision now serving the same policy.
    pub fn adopt(&mut self, next: PolicyGeneration) -> Result<(), NotAnAdvance> {
        if !next.supersedes(&self.active) && !next.carries_forward(&self.active) {
            return Err(NotAnAdvance {
                active: self.active,
                next,
            });
        }
        self.active = next;
        Ok(())
    }
}

/// What activating a policy change requires of a fleet.
///
/// Ordered by severity, so the class of a change with several effects is the
/// maximum of theirs: a publication is as disruptive as its worst field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TransitionClass {
    /// Safe to enforce on the next request. Nothing already admitted becomes
    /// invalid.
    Live,
    /// Safe once what was admitted under the old document has finished: holds and
    /// leases granted under a looser policy are honoured, and the new one binds
    /// from the next admission.
    Drain,
    /// Enforcement changes shape, not only its numbers: durable state laid out
    /// for the old document has to be reconciled before the new one is enforced.
    MigrationRequired,
    /// Not a transition this model performs. The publication is refused rather
    /// than applied.
    Refused,
}

impl TransitionClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Drain => "drain",
            Self::MigrationRequired => "migration-required",
            Self::Refused => "refused",
        }
    }
}

impl fmt::Display for TransitionClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One reason a transition is classified the way it is.
///
/// A change is described by every reason that applies, so an operator reads *why*
/// a publication needs a drain rather than only that it does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TransitionReason {
    /// A document for a different tenant or project. Not a transition of this
    /// document at all.
    ScopeChanged,
    /// The epoch went backwards.
    EpochRegressed,
    /// The content changed without advancing the epoch, which would leave two
    /// documents claiming one generation and make the fence unable to tell a
    /// stale writer from a current one.
    EpochNotAdvanced,
    /// The epoch advanced with no change to what is enforced.
    Republished,
    BudgetRaised,
    /// A lower cap: what is already held was admitted under the higher one.
    BudgetLowered,
    /// A scope-wide cap where there was none: a shared store starts composing
    /// every reservation over a second key, so existing ledgers have to be
    /// reconciled before the cap means anything.
    ScopeCapEnabled,
    /// A scope-wide cap removed: reservations stop touching the composite key,
    /// and the ledgers it accumulated are left behind.
    ScopeCapDisabled,
    ScopeCapRaised,
    ScopeCapLowered,
    ReservationTtlExtended,
    /// A shorter hold lifetime: holds taken under the longer one would be
    /// reclaimed before the request that owns them settles.
    ReservationTtlShortened,
    ConcurrencyRaised,
    /// A lower ceiling: leases already granted exceed it.
    ConcurrencyLowered,
    LeaseTtlExtended,
    /// A shorter lease lifetime: leases held under the longer one would be
    /// reclaimed while their requests are still in flight.
    LeaseTtlShortened,
    /// The token floor rose: more tokens are refused, which is the fail-closed
    /// direction.
    TokenFloorRaised,
    /// The token floor fell, which would restore tokens an operator revoked.
    TokenFloorLowered,
}

impl TransitionReason {
    /// The class this reason alone implies.
    pub const fn class(self) -> TransitionClass {
        match self {
            Self::ScopeChanged
            | Self::EpochRegressed
            | Self::EpochNotAdvanced
            | Self::TokenFloorLowered => TransitionClass::Refused,
            Self::ScopeCapEnabled | Self::ScopeCapDisabled => TransitionClass::MigrationRequired,
            Self::BudgetLowered
            | Self::ScopeCapLowered
            | Self::ReservationTtlShortened
            | Self::ConcurrencyLowered
            | Self::LeaseTtlShortened => TransitionClass::Drain,
            Self::Republished
            | Self::BudgetRaised
            | Self::ScopeCapRaised
            | Self::ReservationTtlExtended
            | Self::ConcurrencyRaised
            | Self::LeaseTtlExtended
            | Self::TokenFloorRaised => TransitionClass::Live,
        }
    }
}

/// How a fleet may move from one policy document to the next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyTransition {
    class: TransitionClass,
    reasons: Vec<TransitionReason>,
}

impl PolicyTransition {
    /// Classify the move from `from` to `to`.
    ///
    /// The refusing reasons are decided first and short-circuit the rest: a
    /// document for another scope, or one whose epoch does not carry its change,
    /// is not a document to compare field by field.
    fn between(from: &PolicyBody, to: &PolicyBody) -> Self {
        if from.scope != to.scope {
            return Self::of(vec![TransitionReason::ScopeChanged]);
        }
        if to.epoch < from.epoch {
            return Self::of(vec![TransitionReason::EpochRegressed]);
        }
        if to.epoch == from.epoch {
            return if from.same_content(to) {
                // The same document twice: no change to classify.
                Self::of(Vec::new())
            } else {
                Self::of(vec![TransitionReason::EpochNotAdvanced])
            };
        }
        if from.same_content(to) {
            return Self::of(vec![TransitionReason::Republished]);
        }

        let mut reasons = Vec::new();
        let (old, new) = (&from.budget, &to.budget);
        push_ordered(
            &mut reasons,
            old.subject_limit_microdollars,
            new.subject_limit_microdollars,
            TransitionReason::BudgetRaised,
            TransitionReason::BudgetLowered,
        );
        match (
            old.namespace_limit_microdollars,
            new.namespace_limit_microdollars,
        ) {
            (None, Some(_)) => reasons.push(TransitionReason::ScopeCapEnabled),
            (Some(_), None) => reasons.push(TransitionReason::ScopeCapDisabled),
            (Some(old), Some(new)) => push_ordered(
                &mut reasons,
                old,
                new,
                TransitionReason::ScopeCapRaised,
                TransitionReason::ScopeCapLowered,
            ),
            (None, None) => {}
        }
        push_ordered(
            &mut reasons,
            old.reservation_ttl_seconds,
            new.reservation_ttl_seconds,
            TransitionReason::ReservationTtlExtended,
            TransitionReason::ReservationTtlShortened,
        );
        let (old, new) = (&from.concurrency, &to.concurrency);
        push_ordered(
            &mut reasons,
            old.max_in_flight_per_subject,
            new.max_in_flight_per_subject,
            TransitionReason::ConcurrencyRaised,
            TransitionReason::ConcurrencyLowered,
        );
        push_ordered(
            &mut reasons,
            old.lease_ttl_seconds,
            new.lease_ttl_seconds,
            TransitionReason::LeaseTtlExtended,
            TransitionReason::LeaseTtlShortened,
        );
        push_ordered(
            &mut reasons,
            from.revocation.minimum_token_epoch,
            to.revocation.minimum_token_epoch,
            TransitionReason::TokenFloorRaised,
            TransitionReason::TokenFloorLowered,
        );
        Self::of(reasons)
    }

    fn of(mut reasons: Vec<TransitionReason>) -> Self {
        reasons.sort_unstable();
        reasons.dedup();
        let class = reasons
            .iter()
            .map(|reason| reason.class())
            .max()
            .unwrap_or(TransitionClass::Live);
        Self { class, reasons }
    }

    /// The class of the whole change: the most disruptive of its reasons.
    pub const fn class(&self) -> TransitionClass {
        self.class
    }

    /// Every reason that applies, ordered so two replicas report one change the
    /// same way.
    pub fn reasons(&self) -> &[TransitionReason] {
        &self.reasons
    }

    /// Whether this change may be enforced on the next request.
    pub fn is_live(&self) -> bool {
        self.class == TransitionClass::Live
    }

    /// Whether this change is refused rather than applied.
    pub fn is_refused(&self) -> bool {
        self.class == TransitionClass::Refused
    }
}

fn push_ordered(
    reasons: &mut Vec<TransitionReason>,
    old: u64,
    new: u64,
    raised: TransitionReason,
    lowered: TransitionReason,
) {
    if new > old {
        reasons.push(raised);
    } else if new < old {
        reasons.push(lowered);
    }
}

/// A policy document as a revision holds it: its envelope, its name, and its
/// body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyDocument {
    pub reference: ResourceRef,
    pub slug: Slug,
    pub body: PolicyBody,
}

/// Every policy document of one revision, resolved once.
///
/// Built by [`PolicySet::of`], which is the single place policy bodies are
/// interpreted, so publication, hydration, and any later projection reach the
/// same conclusions. Ordering is by scope throughout, so two replicas iterate the
/// same documents in the same order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PolicySet {
    documents: BTreeMap<PolicyScope, PolicyDocument>,
}

impl PolicySet {
    /// Read every policy body in a desired state.
    ///
    /// What is checked here is what no envelope-level rule can see: that each
    /// body is one this build reads, and that it is bound to its envelope —
    /// identity to the reference, governed scope to the envelope's scope.
    ///
    /// What is deliberately *not* checked is that a document's tenant or project
    /// row exists in the same revision. Tenancy already refuses a project-scoped
    /// resource that contradicts a declared project, and requiring the row itself
    /// would add a rule to revisions already published under the older one — the
    /// same reasoning [`Tenancy::of`](super::tenancy::Tenancy::of) states. A
    /// document whose scope names nothing is unroutable at the boundary that
    /// routes, not unreadable here.
    pub fn of(state: &DesiredState) -> Result<Self, PolicyError> {
        let mut set = Self::default();
        for resource in state.resources() {
            if resource.reference.kind != ResourceKind::Policy {
                continue;
            }
            let body = PolicyBody::read(resource)?;
            set.documents.insert(
                body.scope(),
                PolicyDocument {
                    reference: resource.reference,
                    slug: resource.slug.clone(),
                    body,
                },
            );
        }
        Ok(set)
    }

    /// Every document, ordered by scope.
    pub fn documents(&self) -> impl ExactSizeIterator<Item = &PolicyDocument> {
        self.documents.values()
    }

    /// The document published *for* this scope, if any.
    pub fn document(&self, scope: PolicyScope) -> Option<&PolicyDocument> {
        self.documents.get(&scope)
    }

    /// The complete document that governs `scope`: its own, or its tenant's.
    ///
    /// Whole-document selection, never a field-by-field merge — so what governs a
    /// project is always a document an operator published as a unit. A scope with
    /// neither has no published policy, and the bootstrap file's limits stand.
    pub fn effective(&self, scope: PolicyScope) -> Option<&PolicyDocument> {
        self.documents
            .get(&scope)
            .or_else(|| self.documents.get(&scope.fallback()?))
    }

    /// This set as the revision `source` published it.
    pub fn snapshot(&self, source: RevisionId) -> PolicySnapshot {
        PolicySnapshot {
            source,
            documents: self
                .documents
                .iter()
                .map(|(scope, document)| (*scope, document.body))
                .collect(),
        }
    }
}

/// The policy of one revision, with the generation each document is enforced
/// under.
///
/// A snapshot is what a later activation slice holds: complete documents, an
/// explicit generation per scope, and a fence per scope. It is built from a
/// revision that already validated, so there is no reading or refusing left in
/// it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicySnapshot {
    source: RevisionId,
    documents: BTreeMap<PolicyScope, PolicyBody>,
}

impl PolicySnapshot {
    /// The revision every generation in this snapshot came from.
    pub const fn source(&self) -> RevisionId {
        self.source
    }

    /// The complete policy governing `scope`, selected whole.
    pub fn effective(&self, scope: PolicyScope) -> Option<&PolicyBody> {
        self.documents
            .get(&scope)
            .or_else(|| self.documents.get(&scope.fallback()?))
    }

    /// The generation the policy governing `scope` is enforced under.
    ///
    /// The generation of the document that actually governs it: a project with no
    /// document of its own is fenced by its tenant's generation, because that is
    /// the document being enforced.
    pub fn generation(&self, scope: PolicyScope) -> Option<PolicyGeneration> {
        Some(self.effective(scope)?.generation(self.source))
    }

    /// The fence a writer for `scope` must pass.
    pub fn fence(&self, scope: PolicyScope) -> Option<PolicyFence> {
        Some(PolicyFence::new(self.generation(scope)?))
    }

    /// Every scope this snapshot publishes a document for, ordered.
    pub fn scopes(&self) -> impl ExactSizeIterator<Item = PolicyScope> + '_ {
        self.documents.keys().copied()
    }
}

/// A strict reader over one inline policy record.
struct Record<'a> {
    reference: ResourceRef,
    fields: &'a [(String, CanonicalValue)],
}

impl<'a> Record<'a> {
    fn open(
        resource: &'a ResourceVersion,
        kind: ResourceKind,
        schema: &'static str,
        known: &[&str],
    ) -> Result<Self, PolicyError> {
        let reference = resource.reference;
        if reference.kind != kind {
            return Err(PolicyError::Kind {
                reference,
                expected: kind,
                found: reference.kind,
            });
        }
        let ResourceBody::Inline(value) = &resource.body else {
            return Err(PolicyError::NotInline { reference });
        };
        let CanonicalValue::Map(fields) = value else {
            return Err(PolicyError::NotARecord { reference });
        };
        let record = Self { reference, fields };
        let declared = record.string(SCHEMA_FIELD)?;
        if declared != schema {
            return Err(PolicyError::Schema {
                reference,
                expected: schema,
                found: declared.to_owned(),
            });
        }
        // A bootstrap-owned name is refused before the unknown-field rule, so the
        // boundary is reported as itself rather than as a version skew.
        if let Some((field, _)) = fields
            .iter()
            .find(|(field, _)| BOOTSTRAP_OWNED_FIELDS.contains(&field.as_str()))
        {
            return Err(PolicyError::BootstrapOwned {
                reference,
                field: field.clone(),
            });
        }
        if let Some((field, _)) = fields
            .iter()
            .find(|(field, _)| field != SCHEMA_FIELD && !known.contains(&field.as_str()))
        {
            return Err(PolicyError::UnknownField {
                reference,
                schema,
                field: field.clone(),
            });
        }
        Ok(record)
    }

    fn field(&self, field: &'static str) -> Option<&'a CanonicalValue> {
        self.fields
            .iter()
            .find(|(name, _)| name == field)
            .map(|(_, value)| value)
    }

    fn string(&self, field: &'static str) -> Result<&'a str, PolicyError> {
        match self.field(field).ok_or(PolicyError::MissingField {
            reference: self.reference,
            field,
        })? {
            CanonicalValue::String(text) => Ok(text),
            _ => Err(PolicyError::FieldType {
                reference: self.reference,
                field,
                expected: "a string",
            }),
        }
    }

    /// An unsigned integer field, refusing a value this build cannot enforce
    /// rather than saturating it into one it can.
    fn integer(&self, field: &'static str) -> Result<u64, PolicyError> {
        self.optional_integer(field)?
            .ok_or(PolicyError::MissingField {
                reference: self.reference,
                field,
            })
    }

    fn optional_integer(&self, field: &'static str) -> Result<Option<u64>, PolicyError> {
        let Some(value) = self.field(field) else {
            return Ok(None);
        };
        let CanonicalValue::Integer(value) = value else {
            return Err(PolicyError::FieldType {
                reference: self.reference,
                field,
                expected: "an integer",
            });
        };
        u64::try_from(*value)
            .map(Some)
            .map_err(|_| PolicyError::FieldRange {
                reference: self.reference,
                field,
                source: Box::new(InvalidPolicy::NotRepresentable { value: *value }),
            })
    }

    /// An integer field parsed through the same bound an authored value passes.
    fn bounded<T>(
        &self,
        field: &'static str,
        parse: impl FnOnce(u64) -> Result<T, InvalidPolicy>,
    ) -> Result<T, PolicyError> {
        parse(self.integer(field)?).map_err(|source| PolicyError::FieldRange {
            reference: self.reference,
            field,
            source: Box::new(source),
        })
    }

    fn tenant(&self) -> Result<TenantId, PolicyError> {
        TenantId::parse(self.string(TENANT_ID_FIELD)?).map_err(|source| PolicyError::MalformedId {
            reference: self.reference,
            field: TENANT_ID_FIELD,
            source,
        })
    }

    fn optional_project(&self) -> Result<Option<ProjectId>, PolicyError> {
        if self.field(PROJECT_ID_FIELD).is_none() {
            return Ok(None);
        }
        ProjectId::parse(self.string(PROJECT_ID_FIELD)?)
            .map(Some)
            .map_err(|source| PolicyError::MalformedId {
                reference: self.reference,
                field: PROJECT_ID_FIELD,
                source,
            })
    }

    fn identity(
        &self,
        declared: impl fmt::Display,
        identity: ResourceId,
    ) -> Result<(), PolicyError> {
        if self.reference.id == identity {
            Ok(())
        } else {
            Err(PolicyError::IdentityMismatch {
                reference: self.reference,
                declared: declared.to_string(),
                identity: self.reference.id,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::canonical::SerializerVersion;
    use super::super::fixtures::{
        DESIRED_STATE_RESOURCES, candidate, policy_body, project_id, project_policy,
        project_policy_body, revision_id, state, state_with_policy, tenant_id, tenant_policy,
        tenant_policy_body,
    };
    use super::super::mutation::ExpectedRevision;
    use super::super::revision::{
        BodySkew, IntegrityError, LoadedRevision, RevisionManifest, ValidationError,
    };
    use super::*;
    use std::time::SystemTime;

    fn tenant_scope() -> PolicyScope {
        PolicyScope::Tenant(tenant_id(1))
    }

    fn project_scope() -> PolicyScope {
        PolicyScope::Project {
            tenant: tenant_id(1),
            project: project_id(2),
        }
    }

    fn slug() -> Slug {
        Slug::parse("limits").expect("test slug")
    }

    fn with_fields(
        resource: &ResourceVersion,
        edit: impl FnOnce(&mut Vec<(String, CanonicalValue)>),
    ) -> ResourceVersion {
        let ResourceBody::Inline(CanonicalValue::Map(fields)) = &resource.body else {
            panic!("a policy fixture body is an inline record");
        };
        let mut fields = fields.clone();
        edit(&mut fields);
        ResourceVersion {
            body: ResourceBody::Inline(CanonicalValue::Map(fields)),
            ..resource.clone()
        }
    }

    fn set(fields: &mut Vec<(String, CanonicalValue)>, field: &str, value: CanonicalValue) {
        fields.retain(|(name, _)| name != field);
        fields.push((field.to_owned(), value));
    }

    /// A tenant document with one field of the canonical record edited: how a
    /// strictness test names exactly what is wrong with a stored body.
    fn edited(edit: impl FnOnce(&mut Vec<(String, CanonicalValue)>)) -> ResourceVersion {
        with_fields(&tenant_policy(1, 1), edit)
    }

    #[test]
    fn a_document_round_trips_through_its_envelope_and_its_canonical_bytes() {
        let body = tenant_policy_body(1, 1);
        let resource = tenant_policy(1, 1);
        assert_eq!(PolicyBody::read(&resource).unwrap(), body);
        assert_eq!(
            resource.reference.id,
            ResourceId::new(tenant_id(1).uuid()),
            "a tenant's policy is written under the tenant it governs"
        );
        assert_eq!(resource.scope, ResourceScope::Tenant(tenant_id(1)));

        let project = project_policy_body(1, 2, 1);
        let resource = project_policy(1, 2, 1);
        assert_eq!(PolicyBody::read(&resource).unwrap(), project);
        assert_eq!(resource.reference.id, ResourceId::new(project_id(2).uuid()));

        // The bytes are the content's identity: the same document built twice is
        // one checksum, a different document is another, and the schema is inside
        // the bytes rather than beside them.
        assert_eq!(
            body.checksum().unwrap(),
            tenant_policy_body(1, 1).checksum().unwrap()
        );
        assert_ne!(
            body.checksum().unwrap(),
            tenant_policy_body(1, 2).checksum().unwrap(),
            "the epoch is part of the document, so republishing changes its bytes"
        );
        assert_ne!(body.checksum().unwrap(), project.checksum().unwrap());

        let bytes = SerializerVersion::V1.encode(&project.canonical()).unwrap();
        let decoded = SerializerVersion::V1
            .decode(&bytes)
            .expect("a policy body is canonical, so storage returns what it took");
        assert_eq!(
            SerializerVersion::V1.encode(&decoded).unwrap(),
            bytes,
            "the decoded body re-encodes to the bytes storage holds"
        );
        assert_eq!(
            PolicyBody::read(&ResourceVersion {
                body: ResourceBody::Inline(decoded),
                ..resource
            })
            .unwrap(),
            project,
            "and reads back as the same document"
        );
        assert!(
            matches!(
                body.canonical(),
                CanonicalValue::Map(ref fields)
                    if fields.iter().any(|(name, value)|
                        name == SCHEMA_FIELD && *value == CanonicalValue::string(POLICY_SCHEMA))
            ),
            "the schema identifier is inside the checksummed bytes"
        );
    }

    #[test]
    fn an_absent_optional_field_is_a_statement_rather_than_an_omission() {
        let uncapped = tenant_policy_body(1, 1);
        assert_eq!(uncapped.budget().namespace_limit_microdollars(), None);
        let CanonicalValue::Map(fields) = uncapped.canonical() else {
            panic!("a policy body is a record");
        };
        assert!(
            !fields
                .iter()
                .any(|(name, _)| name == NAMESPACE_BUDGET_LIMIT_FIELD),
            "no scope-wide cap is the absence of the key, not a cap of zero"
        );

        let capped = PolicyBody::new(
            tenant_scope(),
            PolicyEpoch::FIRST,
            BudgetPolicy::new(1_000_000, Some(0), 60).unwrap(),
            ConcurrencyPolicy::new(8, 30).unwrap(),
            RevocationPolicy::new(1),
        );
        assert_ne!(
            uncapped.checksum().unwrap(),
            capped.checksum().unwrap(),
            "a scope-wide cap of zero is a different document from no cap at all"
        );
        assert_eq!(
            PolicyBody::read(&capped.version(slug())).unwrap(),
            capped,
            "and it reads back as the cap it is"
        );
    }

    #[test]
    fn a_body_is_bound_to_the_envelope_that_carries_it() {
        // Identity: the document names the object it governs, and the envelope is
        // written under that object's id. A body moved onto another envelope is
        // refused rather than silently governing whatever carried it.
        let moved = ResourceVersion {
            reference: ResourceRef::new(
                ResourceKind::Policy,
                ResourceId::new(project_id(9).uuid()),
                ResourceVersionNumber::FIRST,
            ),
            ..tenant_policy(1, 1)
        };
        assert!(
            matches!(
                PolicyBody::read(&moved),
                Err(PolicyError::IdentityMismatch { .. })
            ),
            "{:?}",
            PolicyBody::read(&moved)
        );

        // Scope: a tenant-wide document at a project's scope would be enforced
        // for a scope it does not describe.
        let rescoped = ResourceVersion {
            scope: ResourceScope::Project {
                tenant: tenant_id(1),
                project: project_id(2),
            },
            ..tenant_policy(1, 1)
        };
        assert_eq!(
            PolicyBody::read(&rescoped),
            Err(PolicyError::ScopeMismatch {
                reference: rescoped.reference,
                declared: tenant_scope(),
                scoped: rescoped.scope.clone(),
            })
        );

        // And a policy body is only ever read off a policy resource.
        let mistyped = ResourceVersion {
            reference: ResourceRef::new(
                ResourceKind::ProviderCredential,
                tenant_policy(1, 1).reference.id,
                ResourceVersionNumber::FIRST,
            ),
            ..tenant_policy(1, 1)
        };
        assert!(matches!(
            PolicyBody::read(&mistyped),
            Err(PolicyError::Kind { .. })
        ));
    }

    #[test]
    fn a_strict_reader_refuses_every_body_it_does_not_fully_understand() {
        let reference = tenant_policy(1, 1).reference;

        assert_eq!(
            PolicyBody::read(&edited(|fields| {
                set(
                    fields,
                    SCHEMA_FIELD,
                    CanonicalValue::string("axond.policy.v2"),
                );
            })),
            Err(PolicyError::Schema {
                reference,
                expected: POLICY_SCHEMA,
                found: "axond.policy.v2".to_owned(),
            })
        );
        assert_eq!(
            PolicyBody::read(&edited(|fields| {
                set(fields, "burst_limit", CanonicalValue::integer(7u32));
            })),
            Err(PolicyError::UnknownField {
                reference,
                schema: POLICY_SCHEMA,
                field: "burst_limit".to_owned(),
            }),
            "a field a newer release added is refused rather than ignored"
        );
        assert_eq!(
            PolicyBody::read(&edited(|fields| {
                fields.retain(|(name, _)| name != EPOCH_FIELD);
            })),
            Err(PolicyError::MissingField {
                reference,
                field: EPOCH_FIELD,
            })
        );
        assert_eq!(
            PolicyBody::read(&edited(|fields| {
                set(fields, EPOCH_FIELD, CanonicalValue::string("4"));
            })),
            Err(PolicyError::FieldType {
                reference,
                field: EPOCH_FIELD,
                expected: "an integer",
            })
        );
        assert_eq!(
            PolicyBody::read(&edited(|fields| {
                set(fields, EPOCH_FIELD, CanonicalValue::integer(0u32));
            })),
            Err(PolicyError::FieldRange {
                reference,
                field: EPOCH_FIELD,
                source: Box::new(InvalidPolicy::TooSmall { value: 0, min: 1 }),
            }),
            "zero is not an epoch, so an unset counter cannot read as a valid one"
        );
        assert_eq!(
            PolicyBody::read(&edited(|fields| {
                set(fields, LEASE_TTL_FIELD, CanonicalValue::integer(-30i32));
            })),
            Err(PolicyError::FieldRange {
                reference,
                field: LEASE_TTL_FIELD,
                source: Box::new(InvalidPolicy::NotRepresentable { value: -30 }),
            }),
            "a value this build cannot enforce is refused, not saturated into one it can"
        );
        assert_eq!(
            PolicyBody::read(&edited(|fields| {
                set(fields, LEASE_TTL_FIELD, CanonicalValue::integer(0u32));
            })),
            Err(PolicyError::FieldRange {
                reference,
                field: LEASE_TTL_FIELD,
                source: Box::new(InvalidPolicy::TooSmall { value: 0, min: 1 }),
            }),
            "a refusal names the field that broke a bound, not the one checked first"
        );
        assert_eq!(
            PolicyBody::read(&edited(|fields| {
                set(fields, MAX_IN_FLIGHT_FIELD, CanonicalValue::integer(0u32));
            })),
            Err(PolicyError::FieldRange {
                reference,
                field: MAX_IN_FLIGHT_FIELD,
                source: Box::new(InvalidPolicy::TooSmall { value: 0, min: 1 }),
            })
        );
        assert!(matches!(
            PolicyBody::read(&edited(|fields| {
                set(
                    fields,
                    TENANT_ID_FIELD,
                    CanonicalValue::string("not-a-uuid"),
                );
            })),
            Err(PolicyError::MalformedId {
                field: TENANT_ID_FIELD,
                ..
            })
        ));
        assert_eq!(
            PolicyBody::read(&ResourceVersion {
                body: ResourceBody::Inline(CanonicalValue::string(POLICY_SCHEMA)),
                ..tenant_policy(1, 1)
            }),
            Err(PolicyError::NotARecord { reference })
        );

        // Every bound an authored document passes is the bound a stored one
        // passes, so the two cannot disagree.
        assert_eq!(
            ConcurrencyPolicy::new(0, 30),
            Err(InvalidPolicy::TooSmall { value: 0, min: 1 })
        );
        assert_eq!(
            BudgetPolicy::new(1, None, 0),
            Err(InvalidPolicy::TooSmall { value: 0, min: 1 })
        );
    }

    #[test]
    fn a_published_document_may_not_name_a_field_bootstrap_owns() {
        let reference = tenant_policy(1, 1).reference;
        for field in BOOTSTRAP_OWNED_FIELDS {
            assert_eq!(
                PolicyBody::read(&edited(|fields| {
                    set(fields, field, CanonicalValue::string("allow"));
                })),
                Err(PolicyError::BootstrapOwned {
                    reference,
                    field: (*field).to_owned(),
                }),
                "`{field}` is the bootstrap file's, and a publication may not set it"
            );
            assert!(
                !PolicyBody::KNOWN_FIELDS.contains(field),
                "`{field}` must not also be a field this schema defines"
            );
        }
        assert!(
            BOOTSTRAP_OWNED_FIELDS.contains(&"on_unavailable")
                && BOOTSTRAP_OWNED_FIELDS.contains(&"backend"),
            "backend identity and the unavailable stance are the two that must never be publishable"
        );
        assert!(
            !PolicyBody::read(&edited(|fields| {
                set(fields, "on_unavailable", CanonicalValue::string("allow"));
            }))
            .expect_err("a bootstrap-owned field is refused")
            .is_incompatible(),
            "a boundary this schema will never cross must not read as a version skew"
        );
    }

    #[test]
    fn a_body_this_build_cannot_read_is_a_skew_and_a_rewritten_one_is_damage() {
        let reference = tenant_policy(1, 1).reference;
        for error in [
            PolicyError::Schema {
                reference,
                expected: POLICY_SCHEMA,
                found: "axond.policy.v2".to_owned(),
            },
            PolicyError::UnknownField {
                reference,
                schema: POLICY_SCHEMA,
                field: "burst_limit".to_owned(),
            },
            PolicyError::MissingField {
                reference,
                field: SCHEMA_FIELD,
            },
        ] {
            assert!(error.is_incompatible(), "{error}");
            assert_eq!(error.reference(), reference);
        }
        for error in [
            PolicyError::MissingField {
                reference,
                field: EPOCH_FIELD,
            },
            PolicyError::FieldType {
                reference,
                field: EPOCH_FIELD,
                expected: "an integer",
            },
            PolicyError::IdentityMismatch {
                reference,
                declared: tenant_scope().to_string(),
                identity: reference.id,
            },
            PolicyError::FieldRange {
                reference,
                field: LEASE_TTL_FIELD,
                source: Box::new(InvalidPolicy::NotRepresentable { value: -30 }),
            },
        ] {
            assert!(
                !error.is_incompatible(),
                "a refusal inside a body that declared this schema points at storage: {error}"
            );
        }

        // A bound is the one rule that can tighten inside a stable schema — as a
        // display name's can in tenancy — so a value below one this build enforces
        // is a release skew rather than a rewritten row.
        let below_a_bound = PolicyError::FieldRange {
            reference,
            field: LEASE_TTL_FIELD,
            source: Box::new(InvalidPolicy::TooSmall { value: 0, min: 1 }),
        };
        assert!(below_a_bound.is_incompatible(), "{below_a_bound}");
        assert!(InvalidPolicy::TooSmall { value: 0, min: 1 }.is_tightenable());
        assert!(!InvalidPolicy::NotRepresentable { value: -30 }.is_tightenable());
    }

    #[test]
    fn publication_and_hydration_read_policy_the_way_they_read_tenancy() {
        let state = state_with_policy();
        state.validate().expect("the fixture documents are valid");
        assert_eq!(
            state.resources().len(),
            DESIRED_STATE_RESOURCES + 2,
            "a document per scope, and no other resource added"
        );

        let mut broken = DesiredState::new();
        for resource in state.resources() {
            let resource = if resource.reference.kind == ResourceKind::Policy
                && resource.scope == ResourceScope::Tenant(tenant_id(1))
            {
                edited(|fields| {
                    set(fields, EPOCH_FIELD, CanonicalValue::integer(0u32));
                })
            } else {
                resource.clone()
            };
            broken.insert(resource).expect("distinct references");
        }
        for blob in state.blobs() {
            broken.declare_blob(*blob);
        }
        assert!(
            matches!(
                broken.validate(),
                Err(ValidationError::Policy(PolicyError::FieldRange { .. }))
            ),
            "{:?}",
            broken.validate()
        );

        // Hydration classifies a body this build cannot read as a compatibility
        // refusal rather than as corruption — the same distinction tenancy makes,
        // reached through the same seam.
        let candidate = candidate(ExpectedRevision::Empty, "policy", state_with_policy());
        let manifest =
            RevisionManifest::of(revision_id(1), None, SystemTime::UNIX_EPOCH, &candidate)
                .expect("the fixture state is publishable");
        let mut newer = DesiredState::new();
        for resource in candidate.state.resources() {
            let resource = if resource.reference.kind == ResourceKind::Policy
                && resource.scope == ResourceScope::Tenant(tenant_id(1))
            {
                edited(|fields| {
                    set(
                        fields,
                        SCHEMA_FIELD,
                        CanonicalValue::string("axond.policy.v2"),
                    );
                })
            } else {
                resource.clone()
            };
            newer.insert(resource).expect("distinct references");
        }
        for blob in candidate.state.blobs() {
            newer.declare_blob(*blob);
        }
        let error = LoadedRevision::assemble(manifest, newer)
            .expect_err("a policy schema from another release must not hydrate");
        assert!(
            matches!(
                error,
                IntegrityError::Incompatible(BodySkew::Policy(PolicyError::Schema { .. }))
            ),
            "{error}"
        );
        assert!(error.is_incompatible());
        assert_eq!(
            match &error {
                IntegrityError::Incompatible(skew) => skew.reference(),
                other => panic!("{other}"),
            },
            tenant_policy(1, 1).reference,
            "and it names the row an operator has to act on"
        );
    }

    #[test]
    fn an_effective_policy_is_one_whole_document_never_a_merge_of_two() {
        // A project document that differs from its tenant's in every field, and
        // that *drops* the tenant's scope-wide cap: if anything were merged, the
        // cap would survive.
        let tenant = PolicyBody::new(
            tenant_scope(),
            PolicyEpoch::FIRST,
            BudgetPolicy::new(9_000_000, Some(50_000_000), 900).unwrap(),
            ConcurrencyPolicy::new(64, 600).unwrap(),
            RevocationPolicy::new(7),
        );
        let project = PolicyBody::new(
            project_scope(),
            PolicyEpoch::FIRST,
            BudgetPolicy::new(1_000, None, 30).unwrap(),
            ConcurrencyPolicy::new(2, 15).unwrap(),
            RevocationPolicy::new(1),
        );
        let mut with_documents = state();
        with_documents
            .insert(tenant.version(slug()))
            .and_then(|state| state.insert(project.version(slug())))
            .expect("one document per scope");
        with_documents.validate().expect("both documents are valid");

        let set = PolicySet::of(&with_documents).unwrap();
        assert_eq!(set.documents().len(), 2);
        assert_eq!(set.document(project_scope()).unwrap().body, project);
        assert_eq!(
            set.effective(project_scope()).unwrap().body,
            project,
            "a scope with its own document is governed by that document verbatim"
        );
        assert_eq!(
            set.effective(project_scope())
                .unwrap()
                .body
                .budget()
                .namespace_limit_microdollars(),
            None,
            "the tenant's scope-wide cap is not inherited field by field"
        );

        // A sibling project with no document of its own takes its tenant's, whole.
        let sibling = PolicyScope::Project {
            tenant: tenant_id(1),
            project: project_id(3),
        };
        assert_eq!(set.document(sibling), None);
        assert_eq!(set.effective(sibling).unwrap().body, tenant);

        // And a scope outside the revision has no published policy at all: the
        // bootstrap file's limits stand, rather than some partial document.
        assert_eq!(set.effective(PolicyScope::Tenant(tenant_id(4))), None);
        assert_eq!(PolicySet::of(&state()).unwrap().documents().len(), 0);
    }

    #[test]
    fn a_generation_is_an_epoch_and_the_revision_that_published_it() {
        let set = PolicySet::of(&state_with_policy()).unwrap();
        let (first, second) = (revision_id(1), revision_id(2));
        let snapshot = set.snapshot(first);
        assert_eq!(snapshot.source(), first);
        assert_eq!(snapshot.scopes().len(), 2);

        let content = tenant_policy_body(1, 1).content();
        let generation = snapshot.generation(tenant_scope()).unwrap();
        assert_eq!(generation.epoch(), PolicyEpoch::FIRST);
        assert_eq!(generation.source(), first);
        assert_eq!(generation.content(), content);
        assert_eq!(
            generation,
            PolicyGeneration::new(PolicyEpoch::FIRST, first, content)
        );

        let carried = set.snapshot(second).generation(tenant_scope()).unwrap();
        assert_ne!(
            generation, carried,
            "one epoch published by two revisions is two generations, not one"
        );
        assert!(
            carried.carries_forward(&generation) && generation.carries_forward(&carried),
            "but a revision that restates an unchanged document carries it forward, \
             rather than forking it"
        );
        assert!(
            carried.same_policy(&generation),
            "so the two generations enforce one policy"
        );

        // A project with no document of its own is fenced by the generation of
        // the document that actually governs it: its tenant's.
        let sibling = PolicyScope::Project {
            tenant: tenant_id(1),
            project: project_id(3),
        };
        assert_eq!(
            snapshot.effective(sibling).unwrap(),
            &tenant_policy_body(1, 1)
        );
        assert_eq!(snapshot.generation(sibling), Some(generation));
        assert_eq!(snapshot.generation(PolicyScope::Tenant(tenant_id(4))), None);
        assert_eq!(snapshot.fence(PolicyScope::Tenant(tenant_id(4))), None);
    }

    #[test]
    fn a_writer_of_any_generation_but_the_active_one_fails_closed() {
        let (first, second) = (revision_id(1), revision_id(2));
        let content = tenant_policy_body(1, 4).content();
        let active = PolicyGeneration::new(PolicyEpoch::new(4).unwrap(), first, content);
        let fence = PolicyFence::new(active);
        assert_eq!(fence.active(), active);
        assert_eq!(fence.admit(active), Ok(()));

        let stale = PolicyGeneration::new(PolicyEpoch::new(3).unwrap(), first, content);
        assert_eq!(
            fence.admit(stale),
            Err(Fenced::Stale {
                writer: stale,
                active
            })
        );

        // A generation this replica has not adopted is refused rather than
        // trusted: a writer that may enforce anything it claims is newer is not
        // fenced at all.
        let ahead = PolicyGeneration::new(PolicyEpoch::new(5).unwrap(), second, content);
        assert_eq!(
            fence.admit(ahead),
            Err(Fenced::Ahead {
                writer: ahead,
                active
            })
        );

        // The same epoch stating a different policy — a restored backup, a forked
        // control plane — is refused rather than resolved in someone's favour.
        let forked = PolicyGeneration::new(
            PolicyEpoch::new(4).unwrap(),
            second,
            PolicyBody::new(
                tenant_scope(),
                PolicyEpoch::new(4).unwrap(),
                BudgetPolicy::new(9_000_000, None, 60).unwrap(),
                ConcurrencyPolicy::new(8, 30).unwrap(),
                RevocationPolicy::new(1),
            )
            .content(),
        );
        assert_eq!(
            fence.admit(forked),
            Err(Fenced::Forked {
                writer: forked,
                active
            })
        );
        assert!(!forked.supersedes(&active) && !active.supersedes(&forked));
        assert!(!forked.carries_forward(&active));

        // Adoption is monotonic, so a replica cannot be walked back onto a
        // generation whose writers it already fenced out.
        let mut fence = PolicyFence::new(active);
        assert_eq!(fence.adopt(ahead), Ok(()));
        assert_eq!(fence.active(), ahead);
        assert_eq!(
            fence.admit(active),
            Err(Fenced::Stale {
                writer: active,
                active: ahead
            })
        );
        assert_eq!(
            fence.adopt(active),
            Err(NotAnAdvance {
                active: ahead,
                next: active
            })
        );
        assert_eq!(
            fence.adopt(forked),
            Err(NotAnAdvance {
                active: ahead,
                next: forked
            })
        );
        assert_eq!(fence.active(), ahead, "a refused adoption changes nothing");
    }

    #[test]
    fn an_unchanged_document_carried_into_a_later_revision_stays_the_same_policy() {
        // The ordinary case: a revision that changes an unrelated resource still
        // restates every policy document, so an unchanged document is republished
        // under a new revision id at the same epoch.
        let set = PolicySet::of(&state_with_policy()).unwrap();
        let (published, carried) = (
            set.snapshot(revision_id(1))
                .generation(tenant_scope())
                .unwrap(),
            set.snapshot(revision_id(2))
                .generation(tenant_scope())
                .unwrap(),
        );

        // A writer holding the earlier publication enforces the policy the fence
        // is enforcing, so it is admitted rather than fenced out as a fork.
        let mut fence = PolicyFence::new(carried);
        assert_eq!(fence.admit(published), Ok(()));
        assert_eq!(PolicyFence::new(published).admit(carried), Ok(()));

        // And a replica can follow the fleet onto the revision now serving it,
        // which strict epoch advance alone would never allow.
        let mut following = PolicyFence::new(published);
        assert_eq!(following.adopt(carried), Ok(()));
        assert_eq!(following.active(), carried);

        // Monotonic in what is enforced, not in the revision: the same document is
        // adoptable in either direction, while a different policy at that epoch is
        // not adoptable in either.
        assert_eq!(fence.adopt(published), Ok(()));
        let forked = PolicyGeneration::new(
            published.epoch(),
            revision_id(3),
            PolicyBody::new(
                tenant_scope(),
                published.epoch(),
                BudgetPolicy::new(9_000_000, None, 60).unwrap(),
                ConcurrencyPolicy::new(8, 30).unwrap(),
                RevocationPolicy::new(1),
            )
            .content(),
        );
        assert_eq!(
            fence.adopt(forked),
            Err(NotAnAdvance {
                active: published,
                next: forked
            })
        );
        assert!(matches!(fence.admit(forked), Err(Fenced::Forked { .. })));

        // The transition model agrees: restating a document changes nothing.
        let body = tenant_policy_body(1, published.epoch().get());
        assert!(body.transition(&body).reasons().is_empty());
        assert_eq!(
            body.content(),
            PolicyBody::new(
                body.scope(),
                body.epoch().next(),
                *body.budget(),
                *body.concurrency(),
                *body.revocation(),
            )
            .content(),
            "advancing only the epoch restates the same policy"
        );

        // An absent optional cap is not a cap of zero, so the two never digest
        // alike and one can never be carried forward as the other.
        let capped = |limit| {
            PolicyBody::new(
                tenant_scope(),
                PolicyEpoch::FIRST,
                BudgetPolicy::new(1_000_000, limit, 60).unwrap(),
                ConcurrencyPolicy::new(8, 30).unwrap(),
                RevocationPolicy::new(1),
            )
            .content()
        };
        assert_ne!(capped(None), capped(Some(0)));
        assert_ne!(capped(Some(1)), capped(Some(2)));
    }

    #[test]
    fn a_transition_is_classified_by_what_activating_it_would_require() {
        let base = policy_body(tenant_scope(), 4);
        let next = |body: PolicyBody| {
            PolicyBody::new(
                body.scope(),
                body.epoch().next(),
                *body.budget(),
                *body.concurrency(),
                *body.revocation(),
            )
        };

        // Republication with no change to what is enforced.
        let republished = next(base);
        assert_eq!(
            base.transition(&republished),
            PolicyTransition {
                class: TransitionClass::Live,
                reasons: vec![TransitionReason::Republished],
            }
        );
        assert!(base.transition(&republished).is_live());
        assert!(base.transition(&base).reasons().is_empty());

        // Looser limits and a higher token floor refuse more, or refuse nothing
        // new: enforceable on the next request.
        let raised = PolicyBody::new(
            tenant_scope(),
            PolicyEpoch::new(5).unwrap(),
            BudgetPolicy::new(2_000_000, None, 120).unwrap(),
            ConcurrencyPolicy::new(16, 60).unwrap(),
            RevocationPolicy::new(2),
        );
        assert_eq!(base.transition(&raised).class(), TransitionClass::Live);
        assert_eq!(
            base.transition(&raised).reasons(),
            [
                TransitionReason::BudgetRaised,
                TransitionReason::ReservationTtlExtended,
                TransitionReason::ConcurrencyRaised,
                TransitionReason::LeaseTtlExtended,
                TransitionReason::TokenFloorRaised,
            ]
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .as_slice(),
            "every reason that applies is reported, in one order"
        );

        // A tighter limit binds what has not been admitted yet, and honours what
        // has: a drain rather than a live change.
        let lowered = PolicyBody::new(
            tenant_scope(),
            PolicyEpoch::new(5).unwrap(),
            BudgetPolicy::new(500_000, None, 30).unwrap(),
            ConcurrencyPolicy::new(4, 15).unwrap(),
            RevocationPolicy::new(1),
        );
        let transition = base.transition(&lowered);
        assert_eq!(transition.class(), TransitionClass::Drain);
        assert!(
            transition
                .reasons()
                .contains(&TransitionReason::BudgetLowered)
        );
        assert!(
            transition
                .reasons()
                .contains(&TransitionReason::ReservationTtlShortened)
        );

        // Turning a scope-wide cap on or off changes the shape of what a shared
        // store keeps, not only its numbers.
        let capped = PolicyBody::new(
            tenant_scope(),
            PolicyEpoch::new(5).unwrap(),
            BudgetPolicy::new(1_000_000, Some(10_000_000), 60).unwrap(),
            *base.concurrency(),
            *base.revocation(),
        );
        assert_eq!(
            base.transition(&capped),
            PolicyTransition {
                class: TransitionClass::MigrationRequired,
                reasons: vec![TransitionReason::ScopeCapEnabled],
            }
        );
        assert_eq!(
            capped.transition(&next(capped)).class(),
            TransitionClass::Live,
            "republishing a capped document is not itself a migration"
        );
        let uncapped = PolicyBody::new(
            capped.scope(),
            capped.epoch().next(),
            BudgetPolicy::new(1_000_000, None, 60).unwrap(),
            *capped.concurrency(),
            *capped.revocation(),
        );
        assert_eq!(
            capped.transition(&uncapped).reasons(),
            [TransitionReason::ScopeCapDisabled],
        );
        assert_eq!(
            capped.transition(&uncapped).class(),
            TransitionClass::MigrationRequired
        );

        // The most disruptive reason decides the class of the whole change.
        let mixed = PolicyBody::new(
            tenant_scope(),
            PolicyEpoch::new(5).unwrap(),
            BudgetPolicy::new(2_000_000, Some(1), 60).unwrap(),
            ConcurrencyPolicy::new(1, 15).unwrap(),
            *base.revocation(),
        );
        let transition = base.transition(&mixed);
        assert_eq!(transition.class(), TransitionClass::MigrationRequired);
        assert!(
            transition
                .reasons()
                .contains(&TransitionReason::BudgetRaised)
        );
        assert!(
            transition
                .reasons()
                .contains(&TransitionReason::ConcurrencyLowered)
        );
        assert!(TransitionClass::Live < TransitionClass::Drain);
        assert!(TransitionClass::Drain < TransitionClass::MigrationRequired);
        assert!(TransitionClass::MigrationRequired < TransitionClass::Refused);
        assert_eq!(
            TransitionClass::MigrationRequired.to_string(),
            "migration-required"
        );
    }

    #[test]
    fn a_change_the_epoch_does_not_carry_is_refused_rather_than_applied() {
        let base = policy_body(tenant_scope(), 4);

        // A material change under the same epoch would leave two documents
        // claiming one generation, and a fence unable to tell them apart.
        let same_epoch = PolicyBody::new(
            tenant_scope(),
            base.epoch(),
            BudgetPolicy::new(2_000_000, None, 60).unwrap(),
            *base.concurrency(),
            *base.revocation(),
        );
        let transition = base.transition(&same_epoch);
        assert!(transition.is_refused());
        assert_eq!(transition.reasons(), [TransitionReason::EpochNotAdvanced]);

        let regressed = PolicyBody::new(
            tenant_scope(),
            PolicyEpoch::new(3).unwrap(),
            *base.budget(),
            *base.concurrency(),
            *base.revocation(),
        );
        assert_eq!(
            base.transition(&regressed).reasons(),
            [TransitionReason::EpochRegressed]
        );

        // Lowering the token floor would restore tokens an operator revoked.
        let unrevoked = PolicyBody::new(
            tenant_scope(),
            base.epoch().next(),
            *base.budget(),
            *base.concurrency(),
            RevocationPolicy::new(base.revocation().minimum_token_epoch() - 1),
        );
        let transition = base.transition(&unrevoked);
        assert!(transition.is_refused());
        assert_eq!(transition.reasons(), [TransitionReason::TokenFloorLowered]);

        // And two documents for different scopes are not a transition at all.
        let elsewhere = policy_body(project_scope(), 5);
        assert_eq!(
            base.transition(&elsewhere).reasons(),
            [TransitionReason::ScopeChanged]
        );
        assert!(base.transition(&elsewhere).is_refused());
    }
}
