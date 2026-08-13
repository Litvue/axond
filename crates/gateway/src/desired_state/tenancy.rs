//! Tenancy bodies: what a tenant and a project *are* inside a revision (#191).
//!
//! [`resource`](super::resource) fixes the envelope and deliberately leaves every
//! body shape to the slice that owns it. This module owns the first two, and they
//! are the two the rest of the durable model hangs off: a deployment-scoped
//! [`TenantBody`], and a tenant-scoped [`ProjectBody`] whose readable name is the
//! project's routing and accounting boundary.
//!
//! # What a body carries, and what it deliberately does not
//!
//! A body carries the durable *identity* of the thing it describes and nothing
//! the envelope already carries:
//!
//! | Field | Tenant | Project |
//! | --- | --- | --- |
//! | `schema` | `axond.tenant.v1` | `axond.project.v1` |
//! | `tenant_id` | its own [`TenantId`] | the owning [`TenantId`] |
//! | `project_id` | — | its own [`ProjectId`] |
//! | `display_name` | operator-facing prose | operator-facing prose |
//!
//! The readable name lives in [`ResourceVersion::slug`] and is *not* repeated
//! here. Duplicating it would create two spellings of one name that a rename
//! could put out of agreement, and slug uniqueness is already enforced per scope
//! and kind by [`DesiredState::validate`].
//!
//! Identity is not repeated either, it is *bound*: a tenant's
//! [`ResourceRef::id`](super::resource::ResourceRef) is its [`TenantId`] and a
//! project's is its [`ProjectId`], both compared on every read. One durable
//! object therefore has one identity, and "the resource row for tenant X" cannot
//! come to mean a different tenant than its body claims.
//!
//! # Schema identifiers and compatibility
//!
//! Each body names its own schema, and reading is strict in both directions:
//!
//! - a body whose `schema` is not the identifier this build reads is refused
//!   ([`TenancyError::Schema`]) — never coerced, and never read field-by-field on
//!   the chance the shapes overlap;
//! - a field this build does not know is refused ([`TenancyError::UnknownField`])
//!   rather than dropped, so a revision published by a newer build cannot be
//!   hydrated into a snapshot that silently ignores half of it.
//!
//! The consequence is the compatibility rule: **any change to a body's field set
//! or field meaning is a new schema identifier**, and a build reads the
//! identifiers it knows. A newer revision is a typed refusal on an older replica
//! (which keeps serving what it has, see [`crate::convergence`]), not a partial
//! interpretation. That is the same reasoning as [`SerializerVersion`] carrying
//! its version in the bytes, one level up.
//!
//! [`SerializerVersion`]: super::canonical::SerializerVersion
//!
//! Strictness and *classification* are separate questions, and only the second
//! one is about blame. A body this build cannot read is refused either way, but
//! it is refused as an incompatibility — see [`TenancyError::is_incompatible`] —
//! and never as storage corruption. That covers both directions of a rolling
//! upgrade: a revision published by a newer release, and a revision published
//! before tenancy bodies were typed at all, whose `Tenant` row carries whatever
//! untyped body the writer of the day put there. Neither is silently coerced into
//! a typed tenancy resource, and neither pages someone to go repair a database
//! that is perfectly intact; both say *this replica cannot read this revision*,
//! and both leave the revision the replica already holds serving.
//!
//! # Where these rules are enforced
//!
//! [`Tenancy::of`] reads every tenancy body in a [`DesiredState`] and resolves
//! the tenancy graph, and [`DesiredState::validate`] calls it. Every existing
//! seam therefore inherits it, with no request path involved:
//!
//! - publication: [`RevisionCandidate::validated_checksum`] validates before a
//!   manifest exists, so an invalid tenancy body is never published;
//! - hydration: [`LoadedRevision::assemble`] re-validates what storage returned,
//!   so a project whose owner was edited underneath it does not hydrate;
//! - projection: [`TenancyProjection`] reads the same view, so no projection has
//!   its own second interpretation of a body.
//!
//! [`RevisionCandidate::validated_checksum`]: super::revision::RevisionCandidate::validated_checksum
//! [`LoadedRevision::assemble`]: super::revision::LoadedRevision::assemble
//! [`TenancyProjection`]: crate::convergence::tenancy::TenancyProjection

use std::collections::BTreeMap;
use std::fmt;

use super::canonical::{Canonical, CanonicalValue};
use super::ids::{InvalidId, ProjectId, ResourceId, Slug, TenantId};
use super::record::{
    BodyError, DISPLAY_NAME_FIELD, DisplayNameError, IdentifiedBody, PROJECT_ID_FIELD, Record,
    SCHEMA_FIELD, TENANT_ID_FIELD,
};
use super::resource::{
    ResourceBody, ResourceKind, ResourceRef, ResourceScope, ResourceVersion, ResourceVersionNumber,
};
use super::revision::DesiredState;

/// The tenant body schema a tenant that has never left [`TenantLifecycle::Active`]
/// is written under, and the only one a build predating lifecycle ever wrote.
pub const TENANT_SCHEMA: &str = "axond.tenant.v1";

/// The tenant body schema a tenant carrying a lifecycle state other than
/// [`TenantLifecycle::Active`] is written under (#144).
///
/// Two identifiers rather than one, and an active tenant keeps being written
/// under [`TENANT_SCHEMA`], so publishing tenants does not require every replica
/// to understand lifecycle first. Disabling a tenant is the moment a revision
/// starts requiring a build that does — which is the honest boundary, because a
/// replica that read a disabled tenant as active would keep serving it.
///
/// Named for the field set it adds rather than `axond.tenant.v2`: a `v2` is the
/// next shape of *the tenant body itself*, and spending that identifier on an
/// additive field set would leave the real successor without a name.
pub const TENANT_LIFECYCLE_SCHEMA: &str = "axond.tenant.lifecycle.v1";

/// The project body schema this build reads and writes.
pub const PROJECT_SCHEMA: &str = "axond.project.v1";

const LIFECYCLE_FIELD: &str = "lifecycle";

/// Why a tenancy body, or the tenancy graph it belongs to, was refused.
///
/// Every arm names the resource it is about, so a refusal an operator reads
/// points at one row rather than at "the revision".
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TenancyError {
    #[error("{reference} is a {} resource, not a {}", found.as_str(), expected.as_str())]
    Kind {
        reference: ResourceRef,
        expected: ResourceKind,
        found: ResourceKind,
    },
    #[error("{reference} is a blob body; a tenancy record is inline")]
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
    #[error("{reference} field `{field}` is not a string")]
    FieldType {
        reference: ResourceRef,
        field: &'static str,
    },
    #[error("{reference} field `{field}` is not an id: {source}")]
    MalformedId {
        reference: ResourceRef,
        field: &'static str,
        #[source]
        source: InvalidId,
    },
    #[error("{reference} field `{field}` is not a display name: {source}")]
    MalformedDisplayName {
        reference: ResourceRef,
        field: &'static str,
        #[source]
        source: InvalidDisplayName,
    },
    #[error("{reference} carries {declared}, but its resource identity is {identity}")]
    IdentityMismatch {
        reference: ResourceRef,
        declared: String,
        identity: ResourceId,
    },
    #[error("{reference} declares owner {declared} but is scoped to {scoped:?}")]
    OwnerMismatch {
        reference: ResourceRef,
        declared: TenantId,
        scoped: Option<TenantId>,
    },
    /// A project whose owning tenant row this revision does not carry.
    ///
    /// Only a *project* is held to this: a typed project body is never written
    /// without its tenant, so its absence is a row that went missing rather than
    /// a revision published before the rule existed. Other tenant-scoped
    /// resources are not, deliberately — see [`Tenancy::of`].
    #[error("{reference} belongs to {tenant}, which this revision does not declare")]
    UnknownTenant {
        reference: ResourceRef,
        tenant: TenantId,
    },
    #[error("{reference} places {project} under {scoped}, but that project belongs to {owner}")]
    ProjectOwnerMismatch {
        reference: ResourceRef,
        project: ProjectId,
        scoped: TenantId,
        owner: TenantId,
    },
    #[error("{reference} field `{field}` is not a set of strings")]
    FieldSet {
        reference: ResourceRef,
        field: &'static str,
    },
    #[error("{reference} field `{field}` is not a checksum")]
    MalformedChecksum {
        reference: ResourceRef,
        field: &'static str,
    },
    /// A closed vocabulary — a lifecycle state, an identity kind, a role — spelled
    /// with a value this build does not know.
    ///
    /// A compatibility refusal: every spelling this release writes is one it
    /// reads, so a value it does not know is a value a *newer* release wrote, and
    /// the answer is to run that release rather than to repair a database.
    #[error("{reference} declares `{value}`, which this build does not read as a {vocabulary}")]
    UnknownVocabulary {
        reference: ResourceRef,
        vocabulary: &'static str,
        value: String,
    },
    #[error("{reference} grants no role; a principal with no grant is not a principal")]
    NoRoles { reference: ResourceRef },
    #[error("{reference} grants `{role}` at {scope}, which is not a scope that role is granted at")]
    RoleScope {
        reference: ResourceRef,
        role: &'static str,
        scope: String,
    },
    #[error("{reference} declares `{value}` in `{field}`, which `{schema}` does not spell there")]
    ValueNotForSchema {
        reference: ResourceRef,
        schema: &'static str,
        field: &'static str,
        value: String,
    },
    #[error("{reference} carries `{field}`, which a {kind} identity does not define")]
    FieldNotForKind {
        reference: ResourceRef,
        field: &'static str,
        kind: &'static str,
    },
    #[error("{reference} and {first} are both the identity of {detail}")]
    DuplicatePrincipal {
        reference: ResourceRef,
        first: ResourceRef,
        detail: String,
    },
    #[error("{reference} and {first} are both authenticated by the key {digest}")]
    DuplicateKey {
        reference: ResourceRef,
        first: ResourceRef,
        digest: String,
    },
    #[error("{reference} belongs to {project}, which this revision does not declare")]
    UnknownProject {
        reference: ResourceRef,
        project: ProjectId,
    },
    #[error("{reference} is a {kind} identity at {scope}, which that kind does not live at")]
    IdentityScope {
        reference: ResourceRef,
        kind: &'static str,
        scope: String,
    },
}

impl TenancyError {
    /// Whether this refusal means *this build cannot read the body*, rather than
    /// *these rows do not agree with each other*.
    ///
    /// The two call for opposite operator actions, so they must not arrive as one
    /// label. A body whose schema identifier, form, or field set is not the one
    /// this release reads — a revision published by a newer build, or a legacy
    /// row written before tenancy bodies were typed — is a *compatibility*
    /// refusal: storage is intact, and the fix is to run a build that reads it or
    /// to publish a revision this one does.
    ///
    /// Five kinds of refusal are *not* compatibility failures:
    ///
    /// - a value this build reads, in a position its schema does not put it: a
    ///   lifecycle body spelling `active`, which the base schema is the encoding
    ///   of. No release accepts it there, so "run a newer build" would send the
    ///   operator after one that cannot exist;
    /// - an identity or ownership contradiction: those rows were readable, this
    ///   build understands both of them, and they disagree;
    /// - a body that is not an inline record, or sits under a kind that does not
    ///   match it. Every build that has ever written a tenancy resource wrote an
    ///   inline record under its own kind, untyped ones included, so no release
    ///   writes a scalar or a blob here; a body replaced with one was rewritten
    ///   underneath the gateway, and a newer field set would still arrive as a
    ///   record and be refused by its identifier;
    /// - a project whose tenant row is absent: a typed project body is only ever
    ///   written alongside its tenant, so no release skew produces one, and the
    ///   only rows *required* to exist are ones this build itself wrote (a
    ///   manifest entry whose row is gone is [`MissingResource`], not this);
    /// - a field missing, mistyped, or unparseable *inside a body whose schema
    ///   this build reads*. Reading takes the schema identifier first, so anything
    ///   past that point declared `axond.tenant.v1` and then failed to be one, and
    ///   a schema identifier is only reused for one field set (see the module
    ///   docs). The likely cause is a body rewritten underneath the gateway, not a
    ///   release skew, so it must not be reported as intact storage. A missing
    ///   `schema` field is the legacy case and stays a compatibility refusal.
    ///
    /// The one exception is a display name, whose *rules* can tighten within one
    /// schema — this build refuses a byte-order mark an earlier one accepted — so
    /// a name this build will not take is a skew rather than damage.
    ///
    /// Either way an operator has real repair work; only the compatibility cases
    /// are told the database is fine.
    ///
    /// [`IntegrityError`](super::revision::IntegrityError) carries the
    /// distinction into hydration, and convergence reports it as its own refusal
    /// reason.
    ///
    /// [`MissingResource`]: super::revision::IntegrityError::MissingResource
    pub fn is_incompatible(&self) -> bool {
        match self {
            Self::Schema { .. } | Self::UnknownField { .. } | Self::MalformedDisplayName { .. } => {
                true
            }
            // Only the schema identifier itself: its absence is a body written
            // before tenancy had one at all.
            Self::MissingField { field, .. } | Self::FieldType { field, .. } => {
                *field == SCHEMA_FIELD
            }
            Self::UnknownVocabulary { .. } => true,
            // And its opposite: a spelling this build reads perfectly well, in a
            // position its schema does not put it. No release will accept it
            // there — that is what the schema split means — so telling an
            // operator to upgrade would send them after a build that cannot
            // exist. The row was rewritten underneath the gateway.
            Self::ValueNotForSchema { .. } => false,
            Self::Kind { .. }
            | Self::NotInline { .. }
            | Self::NotARecord { .. }
            | Self::MalformedId { .. }
            | Self::IdentityMismatch { .. }
            | Self::OwnerMismatch { .. }
            | Self::UnknownTenant { .. }
            | Self::ProjectOwnerMismatch { .. }
            | Self::FieldSet { .. }
            | Self::MalformedChecksum { .. }
            | Self::NoRoles { .. }
            | Self::RoleScope { .. }
            | Self::FieldNotForKind { .. }
            | Self::DuplicatePrincipal { .. }
            | Self::DuplicateKey { .. }
            | Self::UnknownProject { .. }
            | Self::IdentityScope { .. } => false,
        }
    }

    /// The resource this refusal is about.
    ///
    /// Projection reports failures per resource ([`ProjectionError::Body`]), so
    /// the mapping is here rather than repeated at each call site.
    ///
    /// [`ProjectionError::Body`]: crate::convergence::ProjectionError::Body
    pub const fn reference(&self) -> ResourceRef {
        match self {
            Self::Kind { reference, .. }
            | Self::NotInline { reference }
            | Self::NotARecord { reference }
            | Self::Schema { reference, .. }
            | Self::MissingField { reference, .. }
            | Self::UnknownField { reference, .. }
            | Self::FieldType { reference, .. }
            | Self::MalformedId { reference, .. }
            | Self::MalformedDisplayName { reference, .. }
            | Self::IdentityMismatch { reference, .. }
            | Self::OwnerMismatch { reference, .. }
            | Self::UnknownTenant { reference, .. }
            | Self::ProjectOwnerMismatch { reference, .. }
            | Self::FieldSet { reference, .. }
            | Self::MalformedChecksum { reference, .. }
            | Self::UnknownVocabulary { reference, .. }
            | Self::ValueNotForSchema { reference, .. }
            | Self::NoRoles { reference }
            | Self::RoleScope { reference, .. }
            | Self::FieldNotForKind { reference, .. }
            | Self::DuplicatePrincipal { reference, .. }
            | Self::DuplicateKey { reference, .. }
            | Self::IdentityScope { reference, .. }
            | Self::UnknownProject { reference, .. } => *reference,
        }
    }
}

/// Why a display name was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidDisplayName {
    #[error("a display name must not be empty")]
    Empty,
    #[error("a display name of {length} characters is over the {max}-character limit")]
    TooLong { length: usize, max: usize },
    #[error("a display name may not contain the control character {codepoint:#06x}")]
    ControlCharacter { codepoint: u32 },
    #[error("a display name may not contain a byte-order mark")]
    ByteOrderMark,
    #[error("a display name may not begin or end with whitespace")]
    Untrimmed,
}

/// An operator-facing name: prose, not identity, and not a [`Slug`].
///
/// Normalized on the way in rather than at every comparison: leading and
/// trailing whitespace are refused instead of trimmed, so a name has one
/// spelling and one checksum. Everything the canonical encoder cannot represent —
/// control characters and byte-order marks alike — is refused here too, so an
/// unencodable body is a validation error at the admin edge rather than a
/// [`CanonicalError`](super::canonical::CanonicalError) at publication time.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DisplayName(String);

impl DisplayName {
    pub const MAX_LEN: usize = 128;

    pub fn parse(input: &str) -> Result<Self, InvalidDisplayName> {
        if input.is_empty() {
            return Err(InvalidDisplayName::Empty);
        }
        if input.trim() != input {
            return Err(InvalidDisplayName::Untrimmed);
        }
        let length = input.chars().count();
        if length > Self::MAX_LEN {
            return Err(InvalidDisplayName::TooLong {
                length,
                max: Self::MAX_LEN,
            });
        }
        for character in input.chars() {
            // Exactly what the canonical encoder refuses, and refused here so an
            // unencodable name fails validation instead of publication.
            if character == '\u{feff}' {
                return Err(InvalidDisplayName::ByteOrderMark);
            }
            if character.is_control() {
                return Err(InvalidDisplayName::ControlCharacter {
                    codepoint: u32::from(character),
                });
            }
        }
        Ok(Self(input.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DisplayName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The strict reader, with tenancy's refusals in it.
///
/// Every check [`Record`] makes is a check tenancy needs; giving it tenancy's
/// error type is all it takes to inherit them, and a second schema inherits the
/// same ones rather than reimplementing them slightly differently.
impl BodyError for TenancyError {
    fn kind(reference: ResourceRef, expected: ResourceKind, found: ResourceKind) -> Self {
        Self::Kind {
            reference,
            expected,
            found,
        }
    }

    fn not_inline(reference: ResourceRef) -> Self {
        Self::NotInline { reference }
    }

    fn not_a_record(reference: ResourceRef) -> Self {
        Self::NotARecord { reference }
    }

    fn schema(reference: ResourceRef, expected: &'static str, found: String) -> Self {
        Self::Schema {
            reference,
            expected,
            found,
        }
    }

    fn missing_field(reference: ResourceRef, field: &'static str) -> Self {
        Self::MissingField { reference, field }
    }

    fn unknown_field(reference: ResourceRef, schema: &'static str, field: String) -> Self {
        Self::UnknownField {
            reference,
            schema,
            field,
        }
    }

    fn field_type(reference: ResourceRef, field: &'static str) -> Self {
        Self::FieldType { reference, field }
    }

    fn field_set(reference: ResourceRef, field: &'static str) -> Self {
        Self::FieldSet { reference, field }
    }

    fn malformed_checksum(reference: ResourceRef, field: &'static str) -> Self {
        Self::MalformedChecksum { reference, field }
    }
}

impl DisplayNameError for TenancyError {
    fn malformed_display_name(
        reference: ResourceRef,
        field: &'static str,
        source: InvalidDisplayName,
    ) -> Self {
        Self::MalformedDisplayName {
            reference,
            field,
            source,
        }
    }
}

impl IdentifiedBody for TenancyError {
    fn malformed_id(reference: ResourceRef, field: &'static str, source: InvalidId) -> Self {
        Self::MalformedId {
            reference,
            field,
            source,
        }
    }

    fn identity_mismatch(reference: ResourceRef, declared: String, identity: ResourceId) -> Self {
        Self::IdentityMismatch {
            reference,
            declared,
            identity,
        }
    }
}

/// Where a tenant is in its life (#144).
///
/// A transition, never an erasure: disabling or deleting a tenant stops it being
/// served and stops it being administered, and leaves every revision, mutation,
/// audit event, and usage record that named it exactly where it was. "Forget this
/// tenant's data" is a separate, deliberate compliance operation on the rows a
/// retention policy allows to be dropped — not a side effect of an administrator
/// clicking delete, which would take the evidence of what was billed with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum TenantLifecycle {
    /// Served and administered.
    #[default]
    Active,
    /// Not served, still administered: an administrator can read it, re-enable
    /// it, and settle its bill. What a suspension for non-payment is.
    Disabled,
    /// Not served and not administered: a tombstone that keeps the tenant's id
    /// from being reused and keeps its history attributable.
    Deleted,
}

impl TenantLifecycle {
    /// Every state, so a stored spelling resolves exhaustively.
    pub const ALL: &'static [Self] = &[Self::Active, Self::Disabled, Self::Deleted];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Disabled => "disabled",
            Self::Deleted => "deleted",
        }
    }

    /// The state a stored or requested identifier names, or `None` for text no
    /// release wrote.
    pub fn parse(input: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|state| state.as_str() == input)
    }

    /// Whether a request may be served for this tenant's projects.
    pub const fn is_served(self) -> bool {
        matches!(self, Self::Active)
    }

    /// Whether this tenant may still be administered — read, re-enabled, billed.
    pub const fn is_administrable(self) -> bool {
        matches!(self, Self::Active | Self::Disabled)
    }
}

impl fmt::Display for TenantLifecycle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A deployment tenant: the durable isolation boundary every other resource
/// hangs off.
///
/// Deployment-scoped by [`ResourceKind::permits`], because the tenant *is* the
/// boundary: a tenant living inside a tenant would be a hierarchy this model
/// does not have.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantBody {
    tenant: TenantId,
    display_name: DisplayName,
    lifecycle: TenantLifecycle,
}

impl TenantBody {
    /// The schema identifier an active tenant encodes under, and the one a build
    /// predating lifecycle wrote.
    pub const SCHEMA: &'static str = TENANT_SCHEMA;

    /// The schema identifier a tenant that is not active encodes under.
    pub const LIFECYCLE_SCHEMA: &'static str = TENANT_LIFECYCLE_SCHEMA;

    /// Base schema last, because that is the one a refusal names — see
    /// `Record::open_any`.
    const SCHEMAS: &'static [&'static str] = &[TENANT_LIFECYCLE_SCHEMA, TENANT_SCHEMA];

    const KNOWN_FIELDS: &'static [&'static str] =
        &[TENANT_ID_FIELD, DISPLAY_NAME_FIELD, LIFECYCLE_FIELD];

    pub const fn new(tenant: TenantId, display_name: DisplayName) -> Self {
        Self {
            tenant,
            display_name,
            lifecycle: TenantLifecycle::Active,
        }
    }

    /// The same tenant in another lifecycle state.
    ///
    /// A new *version* of the tenant is how a transition is published: identity
    /// is stable, and the revision that disabled it stays readable next to the
    /// one that did not.
    pub fn in_lifecycle(self, lifecycle: TenantLifecycle) -> Self {
        Self { lifecycle, ..self }
    }

    pub const fn tenant(&self) -> TenantId {
        self.tenant
    }

    pub const fn display_name(&self) -> &DisplayName {
        &self.display_name
    }

    pub const fn lifecycle(&self) -> TenantLifecycle {
        self.lifecycle
    }

    /// The resource identity a tenant's versions are written under.
    ///
    /// A tenant has one identity, not an id plus a separate row id.
    pub const fn resource_id(&self) -> ResourceId {
        ResourceId::new(self.tenant.uuid())
    }

    /// This body as a resource body.
    pub fn body(&self) -> ResourceBody {
        ResourceBody::Inline(self.canonical())
    }

    /// The first version of this tenant, named `slug`.
    pub fn version(&self, slug: Slug) -> ResourceVersion {
        self.version_at(slug, ResourceVersionNumber::FIRST)
    }

    /// A specific version of this tenant, for a rename or a body change.
    pub fn version_at(&self, slug: Slug, version: ResourceVersionNumber) -> ResourceVersion {
        ResourceVersion::new(
            ResourceRef::new(ResourceKind::Tenant, self.resource_id(), version),
            ResourceScope::Deployment,
            slug,
            self.body(),
        )
    }

    /// Read a tenant resource's body, binding it to its envelope.
    ///
    /// Either schema is accepted, and the pair is not interchangeable: the base
    /// schema has no lifecycle field and *is* an active tenant, while the
    /// lifecycle schema carries one and may not spell `active` — one state, one
    /// encoding, so a checksum answers "is this the same tenant?" without a
    /// normalization step.
    pub fn read(resource: &ResourceVersion) -> Result<Self, TenancyError> {
        let (record, schema) = Record::<TenancyError>::open_any(
            resource,
            ResourceKind::Tenant,
            Self::SCHEMAS,
            Self::KNOWN_FIELDS,
        )?;
        let tenant = record.tenant()?;
        record.identity(tenant, ResourceId::new(tenant.uuid()))?;
        let lifecycle = if schema == TENANT_SCHEMA {
            if record.optional_string(LIFECYCLE_FIELD)?.is_some() {
                return Err(TenancyError::UnknownField {
                    reference: resource.reference,
                    schema,
                    field: LIFECYCLE_FIELD.to_owned(),
                });
            }
            TenantLifecycle::Active
        } else {
            let declared = record.string(LIFECYCLE_FIELD)?;
            let lifecycle = TenantLifecycle::ALL
                .iter()
                .copied()
                .find(|lifecycle| lifecycle.as_str() == declared)
                .ok_or_else(|| TenancyError::UnknownVocabulary {
                    reference: resource.reference,
                    vocabulary: "tenant lifecycle",
                    value: declared.to_owned(),
                })?;
            // `active` is a state this build reads — under the other schema,
            // which is the encoding it has — so a lifecycle body spelling it is
            // a rewritten row rather than one from a release that spells it
            // differently. Refusing it as an unknown vocabulary would tell the
            // operator to run a newer build, and no newer build will accept it
            // here.
            if lifecycle == TenantLifecycle::Active {
                return Err(TenancyError::ValueNotForSchema {
                    reference: resource.reference,
                    schema,
                    field: LIFECYCLE_FIELD,
                    value: declared.to_owned(),
                });
            }
            lifecycle
        };
        Ok(Self {
            tenant,
            display_name: record.display_name()?,
            lifecycle,
        })
    }
}

impl Canonical for TenantBody {
    fn canonical(&self) -> CanonicalValue {
        let mut fields = vec![
            (
                TENANT_ID_FIELD.to_owned(),
                CanonicalValue::string(self.tenant.to_string()),
            ),
            (
                DISPLAY_NAME_FIELD.to_owned(),
                CanonicalValue::string(self.display_name.as_str()),
            ),
        ];
        let schema = if self.lifecycle == TenantLifecycle::Active {
            Self::SCHEMA
        } else {
            fields.push((
                LIFECYCLE_FIELD.to_owned(),
                CanonicalValue::string(self.lifecycle.as_str()),
            ));
            Self::LIFECYCLE_SCHEMA
        };
        fields.push((SCHEMA_FIELD.to_owned(), CanonicalValue::string(schema)));
        CanonicalValue::map(fields)
    }
}

/// A tenant-owned project: the routing and accounting boundary a request is
/// served under.
///
/// A project is what the running gateway calls a *namespace* (ADR 0003): keys
/// bind to it, credentials are pooled per `(namespace, provider)`, budgets are
/// charged against it, and rate limits are held per tenant of it. This slice does
/// not change that boundary — it gives it a durable identity and an owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectBody {
    project: ProjectId,
    tenant: TenantId,
    display_name: DisplayName,
}

impl ProjectBody {
    /// The schema identifier this body encodes and reads.
    pub const SCHEMA: &'static str = PROJECT_SCHEMA;

    const KNOWN_FIELDS: &'static [&'static str] =
        &[PROJECT_ID_FIELD, TENANT_ID_FIELD, DISPLAY_NAME_FIELD];

    pub const fn new(project: ProjectId, tenant: TenantId, display_name: DisplayName) -> Self {
        Self {
            project,
            tenant,
            display_name,
        }
    }

    pub const fn project(&self) -> ProjectId {
        self.project
    }

    /// The tenant that owns this project. Ownership is durable: a project is
    /// never moved between tenants, because everything charged and authorized
    /// under it was charged and authorized under that tenant.
    pub const fn tenant(&self) -> TenantId {
        self.tenant
    }

    pub const fn display_name(&self) -> &DisplayName {
        &self.display_name
    }

    pub const fn resource_id(&self) -> ResourceId {
        ResourceId::new(self.project.uuid())
    }

    pub fn body(&self) -> ResourceBody {
        ResourceBody::Inline(self.canonical())
    }

    /// The scope a project's versions live at: its owning tenant, and only ever
    /// that one.
    pub const fn scope(&self) -> ResourceScope {
        ResourceScope::Tenant(self.tenant)
    }

    /// The scope a resource *inside* this project lives at.
    pub const fn child_scope(&self) -> ResourceScope {
        ResourceScope::Project {
            tenant: self.tenant,
            project: self.project,
        }
    }

    pub fn version(&self, slug: Slug) -> ResourceVersion {
        self.version_at(slug, ResourceVersionNumber::FIRST)
    }

    pub fn version_at(&self, slug: Slug, version: ResourceVersionNumber) -> ResourceVersion {
        ResourceVersion::new(
            ResourceRef::new(ResourceKind::Project, self.resource_id(), version),
            self.scope(),
            slug,
            self.body(),
        )
    }

    /// Read a project resource's body, binding it to its envelope: identity to
    /// the reference, ownership to the scope.
    pub fn read(resource: &ResourceVersion) -> Result<Self, TenancyError> {
        let record = Record::<TenancyError>::open(
            resource,
            ResourceKind::Project,
            Self::SCHEMA,
            Self::KNOWN_FIELDS,
        )?;
        let project = record.project()?;
        record.identity(project, ResourceId::new(project.uuid()))?;
        let tenant = record.tenant()?;
        if resource.scope != ResourceScope::Tenant(tenant) {
            return Err(TenancyError::OwnerMismatch {
                reference: resource.reference,
                declared: tenant,
                scoped: resource.scope.tenant(),
            });
        }
        Ok(Self {
            project,
            tenant,
            display_name: record.display_name()?,
        })
    }
}

impl Canonical for ProjectBody {
    fn canonical(&self) -> CanonicalValue {
        CanonicalValue::map([
            (SCHEMA_FIELD, CanonicalValue::string(Self::SCHEMA)),
            (
                PROJECT_ID_FIELD,
                CanonicalValue::string(self.project.to_string()),
            ),
            (
                TENANT_ID_FIELD,
                CanonicalValue::string(self.tenant.to_string()),
            ),
            (
                DISPLAY_NAME_FIELD,
                CanonicalValue::string(self.display_name.as_str()),
            ),
        ])
    }
}

/// A tenant as a revision holds it: its envelope, its name, and its body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tenant {
    pub reference: ResourceRef,
    pub slug: Slug,
    pub body: TenantBody,
}

/// A project as a revision holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Project {
    pub reference: ResourceRef,
    pub slug: Slug,
    pub body: ProjectBody,
}

/// The tenancy graph of one revision, resolved once.
///
/// Built by [`Tenancy::of`], which is the single place tenancy bodies are
/// interpreted: publication, hydration, and projection all reach the same
/// conclusions because they all call it. Ordering is by id throughout, so two
/// replicas iterate the same tenants and projects in the same order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Tenancy {
    tenants: BTreeMap<TenantId, Tenant>,
    projects: BTreeMap<ProjectId, Project>,
}

impl Tenancy {
    /// Read and resolve the tenancy of a desired state.
    ///
    /// Three things are checked that no envelope-level rule can see:
    ///
    /// 1. every tenancy body is a body of a schema this build reads, bound to its
    ///    own envelope (identity, and a project's owner);
    /// 2. a project's owning tenant is declared by the same revision — a revision
    ///    is whole desired state, so an owner that is merely assumed to exist is
    ///    a dangling owner;
    /// 3. a project-scoped resource whose project this revision *does* declare
    ///    names that project's actual owner, so a project cannot be read under a
    ///    tenant that does not own it.
    ///
    /// What is deliberately *not* checked is the tenant of every other
    /// tenant-scoped resource. Requiring it would read well — "a revision is whole
    /// desired state" — but it is a rule this build would be adding to revisions
    /// already published under the older one, where a credential or an alias could
    /// legitimately carry a tenant no row described. Hydration runs these same
    /// rules, so such a revision would stop loading on upgrade. The tenancy view
    /// is the wrong place to buy that: nothing here needs the tenant of a
    /// credential, and a scope naming a tenant that does not exist is unroutable
    /// at the boundary that routes, not unreadable. A project is held to its owner
    /// because this build never writes one without it, and because the projection
    /// cannot name a namespace without it.
    pub fn of(state: &DesiredState) -> Result<Self, TenancyError> {
        let mut tenancy = Self::default();
        for resource in state.resources() {
            match resource.reference.kind {
                ResourceKind::Tenant => {
                    let body = TenantBody::read(resource)?;
                    tenancy.tenants.insert(
                        body.tenant(),
                        Tenant {
                            reference: resource.reference,
                            slug: resource.slug.clone(),
                            body,
                        },
                    );
                }
                ResourceKind::Project => {
                    let body = ProjectBody::read(resource)?;
                    tenancy.projects.insert(
                        body.project(),
                        Project {
                            reference: resource.reference,
                            slug: resource.slug.clone(),
                            body,
                        },
                    );
                }
                _ => {}
            }
        }

        for project in tenancy.projects.values() {
            if !tenancy.tenants.contains_key(&project.body.tenant()) {
                return Err(TenancyError::UnknownTenant {
                    reference: project.reference,
                    tenant: project.body.tenant(),
                });
            }
        }

        for resource in state.resources() {
            let ResourceScope::Project { tenant, project } = &resource.scope else {
                continue;
            };
            // A project this revision does not declare is not contradicted by
            // anything, so there is nothing to refuse: what the scope names is
            // then simply unroutable, and the boundary that routes says so.
            let Some(owner) = tenancy
                .projects
                .get(project)
                .map(|project| project.body.tenant())
            else {
                continue;
            };
            if owner != *tenant {
                return Err(TenancyError::ProjectOwnerMismatch {
                    reference: resource.reference,
                    project: *project,
                    scoped: *tenant,
                    owner,
                });
            }
        }
        Ok(tenancy)
    }

    /// Every tenant, ordered by [`TenantId`].
    pub fn tenants(&self) -> impl ExactSizeIterator<Item = &Tenant> {
        self.tenants.values()
    }

    /// Every project, ordered by [`ProjectId`].
    pub fn projects(&self) -> impl ExactSizeIterator<Item = &Project> {
        self.projects.values()
    }

    pub fn tenant(&self, id: TenantId) -> Option<&Tenant> {
        self.tenants.get(&id)
    }

    pub fn project(&self, id: ProjectId) -> Option<&Project> {
        self.projects.get(&id)
    }

    /// A tenant's lifecycle state, or `None` if this revision does not declare
    /// the tenant.
    ///
    /// The two are distinct on purpose: "disabled" is a decision an administrator
    /// made, and "not here" is a tenant this revision never had. A caller that
    /// wants to *serve* a tenant should ask [`Tenancy::is_served`], which folds
    /// the absent case into the same refusal.
    pub fn lifecycle(&self, id: TenantId) -> Option<TenantLifecycle> {
        self.tenants.get(&id).map(|tenant| tenant.body.lifecycle())
    }

    /// Whether requests may be served for a tenant: it is declared here, and it
    /// is [`TenantLifecycle::Active`].
    pub fn is_served(&self, id: TenantId) -> bool {
        self.lifecycle(id).is_some_and(TenantLifecycle::is_served)
    }

    /// One tenant's projects, ordered by [`ProjectId`].
    pub fn projects_of(&self, tenant: TenantId) -> impl Iterator<Item = &Project> {
        self.projects
            .values()
            .filter(move |project| project.body.tenant() == tenant)
    }

    /// The tenant-qualified name of a project, or `None` if the project is not
    /// in this view.
    ///
    /// A project slug is unique within its tenant and *only* within it, so this
    /// is the qualified form a global, flat runtime namespace identifier must be
    /// derived from. The separator is `/`, which a [`Slug`] can never contain, so
    /// the qualified form is unambiguous and reversible.
    pub fn qualified_name(&self, project: ProjectId) -> Option<QualifiedProject> {
        let project = self.projects.get(&project)?;
        let tenant = self.tenants.get(&project.body.tenant())?;
        Some(QualifiedProject {
            tenant: tenant.slug.clone(),
            project: project.slug.clone(),
        })
    }
}

/// A project named the way a deployment-wide identifier has to name it: tenant
/// first.
///
/// Two tenants may both have a project called `core`; nothing that is global to a
/// deployment may treat those as one name. This is the type that stops a
/// tenant-unique slug from being flattened into a global string by accident.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QualifiedProject {
    pub tenant: Slug,
    pub project: Slug,
}

impl QualifiedProject {
    /// The separator between the two slugs: not a legal [`Slug`] character, so
    /// `acme/core` decomposes exactly one way.
    pub const SEPARATOR: char = '/';

    /// Split a qualified name back into its slugs.
    pub fn parse(input: &str) -> Option<Self> {
        let (tenant, project) = input.split_once(Self::SEPARATOR)?;
        Some(Self {
            tenant: Slug::parse(tenant).ok()?,
            project: Slug::parse(project).ok()?,
        })
    }
}

impl fmt::Display for QualifiedProject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}{}", self.tenant, Self::SEPARATOR, self.project)
    }
}

#[cfg(test)]
mod tests {
    use super::super::canonical::{Canonical as _, SerializerVersion};
    use super::super::fixtures::{
        alias, candidate, display_name, project, project_body, project_credential, project_id,
        reference, resource_id, state, tenant, tenant_body, tenant_id,
    };
    use super::super::mutation::ExpectedRevision;
    use super::super::revision::{
        BodySkew, IntegrityError, LoadedRevision, RevisionManifest, ValidationError,
    };
    use super::*;
    use std::time::SystemTime;

    fn tenant_resource() -> ResourceVersion {
        tenant(1, "acme")
    }

    fn project_resource() -> ResourceVersion {
        project(&tenant_id(1), 2, "core")
    }

    /// Rewrite a resource's inline record, which is how a body a caller could
    /// never author — or a newer build's body — is put in front of the reader.
    fn with_fields(
        resource: &ResourceVersion,
        edit: impl FnOnce(&mut Vec<(String, CanonicalValue)>),
    ) -> ResourceVersion {
        let ResourceBody::Inline(CanonicalValue::Map(fields)) = &resource.body else {
            panic!("a tenancy fixture body is an inline record");
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

    #[test]
    fn a_body_round_trips_through_its_envelope_and_its_canonical_bytes() {
        let body = tenant_body(1, "Acme");
        let resource = tenant_resource();
        assert_eq!(TenantBody::read(&resource).unwrap(), body);
        assert_eq!(resource.reference.id, resource_id(1));
        assert_eq!(resource.slug.as_str(), "acme");

        let project = project_body(2, 1, "Core");
        let resource = project_resource();
        assert_eq!(ProjectBody::read(&resource).unwrap(), project);
        assert_eq!(project.tenant(), body.tenant());
        assert_eq!(project.child_scope().tenant(), Some(body.tenant()));

        // The bytes are the identity of the content, so the same body built twice
        // is the same checksum, and the schema is inside them.
        assert_eq!(
            body.checksum().unwrap(),
            tenant_body(1, "Acme").checksum().unwrap()
        );
        assert_ne!(
            body.checksum().unwrap(),
            tenant_body(1, "Globex").checksum().unwrap()
        );
        let bytes = SerializerVersion::V1.encode(&project.canonical()).unwrap();
        let decoded = SerializerVersion::V1
            .decode(&bytes)
            .expect("a tenancy body is canonical, so storage returns what it took");
        assert_eq!(
            SerializerVersion::V1.encode(&decoded).unwrap(),
            bytes,
            "the decoded body re-encodes to the bytes storage holds"
        );
        assert_eq!(
            ProjectBody::read(&ResourceVersion {
                body: ResourceBody::Inline(decoded),
                ..resource
            })
            .unwrap(),
            project,
            "and reads back as the same body"
        );
        assert!(
            String::from_utf8_lossy(&bytes).contains(PROJECT_SCHEMA),
            "the schema identifier is part of the checksummed body"
        );
    }

    #[test]
    fn a_schema_this_build_does_not_read_is_refused_rather_than_guessed_at() {
        let newer = with_fields(&tenant_resource(), |fields| {
            set(fields, "schema", CanonicalValue::string("axond.tenant.v2"));
        });
        assert_eq!(
            TenantBody::read(&newer),
            Err(TenancyError::Schema {
                reference: newer.reference,
                expected: TENANT_SCHEMA,
                found: "axond.tenant.v2".to_owned()
            })
        );

        // A field a newer schema added is a refusal too: reading the fields this
        // build knows and dropping the rest would serve half a revision.
        let extended = with_fields(&project_resource(), |fields| {
            set(fields, "residency", CanonicalValue::string("eu"));
        });
        assert_eq!(
            ProjectBody::read(&extended),
            Err(TenancyError::UnknownField {
                reference: extended.reference,
                schema: PROJECT_SCHEMA,
                field: "residency".to_owned()
            })
        );
    }

    #[test]
    fn a_malformed_body_is_a_typed_refusal_for_every_way_it_can_be_malformed() {
        let resource = tenant_resource();
        let missing = with_fields(&resource, |fields| {
            fields.retain(|(name, _)| name != DISPLAY_NAME_FIELD);
        });
        assert_eq!(
            TenantBody::read(&missing),
            Err(TenancyError::MissingField {
                reference: resource.reference,
                field: DISPLAY_NAME_FIELD
            })
        );

        let wrong_type = with_fields(&resource, |fields| {
            set(fields, TENANT_ID_FIELD, CanonicalValue::integer(7));
        });
        assert_eq!(
            TenantBody::read(&wrong_type),
            Err(TenancyError::FieldType {
                reference: resource.reference,
                field: TENANT_ID_FIELD
            })
        );

        // A project id where a tenant id belongs: the text form is typed, so this
        // is a parse error and not a lookup under the wrong table.
        let mistyped = with_fields(&resource, |fields| {
            set(
                fields,
                TENANT_ID_FIELD,
                CanonicalValue::string(project_id(1).to_string()),
            );
        });
        assert!(matches!(
            TenantBody::read(&mistyped),
            Err(TenancyError::MalformedId {
                field: TENANT_ID_FIELD,
                ..
            })
        ));

        let untrimmed = with_fields(&resource, |fields| {
            set(fields, DISPLAY_NAME_FIELD, CanonicalValue::string(" Acme"));
        });
        assert!(matches!(
            TenantBody::read(&untrimmed),
            Err(TenancyError::MalformedDisplayName {
                source: InvalidDisplayName::Untrimmed,
                ..
            })
        ));

        // The envelope's kind and body form are part of what a body must be.
        let as_project = ResourceVersion {
            reference: reference(ResourceKind::Project, 1),
            ..resource.clone()
        };
        assert!(matches!(
            TenantBody::read(&as_project),
            Err(TenancyError::Kind {
                expected: ResourceKind::Tenant,
                found: ResourceKind::Project,
                ..
            })
        ));
        let not_a_record = ResourceVersion {
            body: ResourceBody::Inline(CanonicalValue::string("acme")),
            ..resource.clone()
        };
        assert_eq!(
            TenantBody::read(&not_a_record),
            Err(TenancyError::NotARecord {
                reference: resource.reference
            })
        );
    }

    #[test]
    fn a_body_that_claims_another_identity_than_its_row_is_refused() {
        // The row for tenant 1 carrying tenant 9's body: two identities for one
        // durable object, which is what binding the body to the reference stops.
        let mismatched = with_fields(&tenant_resource(), |fields| {
            set(
                fields,
                TENANT_ID_FIELD,
                CanonicalValue::string(tenant_id(9).to_string()),
            );
        });
        assert_eq!(
            TenantBody::read(&mismatched),
            Err(TenancyError::IdentityMismatch {
                reference: mismatched.reference,
                declared: tenant_id(9).to_string(),
                identity: resource_id(1)
            })
        );

        let mismatched = with_fields(&project_resource(), |fields| {
            set(
                fields,
                PROJECT_ID_FIELD,
                CanonicalValue::string(project_id(8).to_string()),
            );
        });
        assert!(matches!(
            ProjectBody::read(&mismatched),
            Err(TenancyError::IdentityMismatch { .. })
        ));
    }

    #[test]
    fn a_project_cannot_be_read_under_a_tenant_that_does_not_own_it() {
        // Storage the domain would never have accepted: the scope column says one
        // tenant, the body says another.
        let moved = ResourceVersion {
            scope: ResourceScope::Tenant(tenant_id(9)),
            ..project_resource()
        };
        assert_eq!(
            ProjectBody::read(&moved),
            Err(TenancyError::OwnerMismatch {
                reference: moved.reference,
                declared: tenant_id(1),
                scoped: Some(tenant_id(9))
            })
        );

        let mut state = state();
        state
            .insert(tenant(9, "globex"))
            .expect("a distinct reference");
        let mut relocated = DesiredState::new();
        for resource in state.resources() {
            let resource = if resource.reference.kind == ResourceKind::Project {
                moved.clone()
            } else {
                resource.clone()
            };
            relocated.insert(resource).expect("distinct references");
        }
        for blob in state.blobs() {
            relocated.declare_blob(*blob);
        }
        assert_eq!(
            relocated.validate(),
            Err(ValidationError::Tenancy(TenancyError::OwnerMismatch {
                reference: moved.reference,
                declared: tenant_id(1),
                scoped: Some(tenant_id(9))
            })),
            "an owner edited underneath a project is refused by the domain"
        );
    }

    #[test]
    fn an_invalid_tenancy_body_is_refused_before_a_manifest_exists() {
        let mut state = DesiredState::new();
        let unreadable = with_fields(&tenant_resource(), |fields| {
            set(fields, "schema", CanonicalValue::string("axond.tenant.v2"));
        });
        state.insert(unreadable.clone()).expect("a fresh state");
        let candidate = candidate(ExpectedRevision::Empty, "unreadable", state);
        assert!(matches!(
            candidate.validated_checksum(),
            Err(ValidationError::Tenancy(TenancyError::Schema { .. }))
        ));
        assert!(
            matches!(
                RevisionManifest::of(
                    super::super::fixtures::revision_id(1),
                    None,
                    SystemTime::UNIX_EPOCH,
                    &candidate
                ),
                Err(ValidationError::Tenancy(TenancyError::Schema { .. }))
            ),
            "a body this build cannot read must not become a published revision"
        );
    }

    #[test]
    fn a_hydrated_revision_re_reads_the_bodies_it_was_published_with() {
        let candidate = candidate(ExpectedRevision::Empty, "hydrate", state());
        let manifest = RevisionManifest::of(
            super::super::fixtures::revision_id(1),
            None,
            SystemTime::UNIX_EPOCH,
            &candidate,
        )
        .expect("the fixture state is publishable");
        let loaded = LoadedRevision::assemble(manifest.clone(), candidate.state.clone())
            .expect("the state the manifest describes");
        let tenancy = Tenancy::of(loaded.state()).expect("the fixture tenancy resolves");
        assert_eq!(tenancy.tenants().len(), 1);
        assert_eq!(tenancy.projects().len(), 1);
        assert_eq!(
            tenancy
                .qualified_name(project_id(2))
                .map(|name| name.to_string()),
            Some("acme/core".to_owned())
        );
        assert_eq!(
            tenancy
                .tenant(tenant_id(1))
                .map(|tenant| tenant.slug.as_str()),
            Some("acme")
        );
        assert_eq!(loaded.state().checksum().unwrap(), manifest.checksum);

        // A tenancy body edited in storage does not hydrate, whatever the row's
        // own checksum says.
        let mut edited = DesiredState::new();
        for resource in candidate.state.resources() {
            let resource = if resource.reference.kind == ResourceKind::Project {
                with_fields(resource, |fields| {
                    set(
                        fields,
                        TENANT_ID_FIELD,
                        CanonicalValue::string(tenant_id(9).to_string()),
                    );
                })
            } else {
                resource.clone()
            };
            edited.insert(resource).expect("distinct references");
        }
        for blob in candidate.state.blobs() {
            edited.declare_blob(*blob);
        }
        let error = LoadedRevision::assemble(manifest, edited)
            .expect_err("an edited tenancy body must not hydrate");
        assert_eq!(
            error,
            IntegrityError::Invalid(ValidationError::Tenancy(TenancyError::OwnerMismatch {
                reference: project_resource().reference,
                declared: tenant_id(9),
                scoped: Some(tenant_id(1))
            })),
            "the domain refuses it before any checksum is compared, and names the row"
        );
    }

    /// The two refusals hydration must not conflate. Both are strict; only one is
    /// a reason to go looking at storage.
    #[test]
    fn a_body_this_build_cannot_read_hydrates_as_an_incompatibility_not_corruption() {
        let candidate = candidate(ExpectedRevision::Empty, "hydrate", state());
        let manifest = RevisionManifest::of(
            super::super::fixtures::revision_id(1),
            None,
            SystemTime::UNIX_EPOCH,
            &candidate,
        )
        .expect("the fixture state is publishable");

        // A revision retained from before tenancy bodies were typed: the row is
        // intact, and this build simply does not read it.
        let mut legacy = DesiredState::new();
        for resource in candidate.state.resources() {
            let resource = if resource.reference.kind == ResourceKind::Tenant {
                super::super::fixtures::legacy_tenant(1, "acme")
            } else {
                resource.clone()
            };
            legacy.insert(resource).expect("distinct references");
        }
        for blob in candidate.state.blobs() {
            legacy.declare_blob(*blob);
        }
        let error = LoadedRevision::assemble(manifest.clone(), legacy)
            .expect_err("an untyped tenancy body must not hydrate");
        assert_eq!(
            error,
            IntegrityError::Incompatible(BodySkew::Tenancy(TenancyError::MissingField {
                reference: tenant_resource().reference,
                field: "schema"
            })),
            "a legacy body is a compatibility refusal, and it names the row"
        );
        assert!(error.is_incompatible());
        assert!(
            !error.to_string().contains("unreadable"),
            "intact storage must not be described as unreadable: {error}"
        );

        // A schema identifier from a future release is the same class of refusal,
        // arriving from the other direction.
        let mut newer = DesiredState::new();
        for resource in candidate.state.resources() {
            let resource = if resource.reference.kind == ResourceKind::Tenant {
                with_fields(resource, |fields| {
                    set(fields, "schema", CanonicalValue::string("axond.tenant.v2"));
                })
            } else {
                resource.clone()
            };
            newer.insert(resource).expect("distinct references");
        }
        for blob in candidate.state.blobs() {
            newer.declare_blob(*blob);
        }
        let error =
            LoadedRevision::assemble(manifest, newer).expect_err("a newer schema must not hydrate");
        assert!(
            matches!(
                error,
                IntegrityError::Incompatible(BodySkew::Tenancy(TenancyError::Schema { .. }))
            ),
            "{error}"
        );

        // An ownership contradiction is not a compatibility refusal: those rows were readable and
        // disagree, which is the case that means storage.
        assert!(
            !TenancyError::OwnerMismatch {
                reference: project_resource().reference,
                declared: tenant_id(9),
                scoped: Some(tenant_id(1)),
            }
            .is_incompatible()
        );

        // Nor is a body that declares a schema this build *does* read and then is
        // not one: the identifier is read first, so past that point the field set
        // is known, and a field that has gone missing or changed type is a rewrite
        // rather than a release skew. Telling that operator the database is fine
        // would point them away from the only place to look.
        let mut damaged = DesiredState::new();
        let losing_a_field = with_fields(&tenant_resource(), |fields| {
            fields.retain(|(name, _)| name != DISPLAY_NAME_FIELD);
        });
        damaged.insert(losing_a_field).expect("a fresh state");
        let error = damaged
            .validate()
            .expect_err("a v1 body without a v1 field is not a v1 body");
        assert_eq!(
            error,
            ValidationError::Tenancy(TenancyError::MissingField {
                reference: tenant_resource().reference,
                field: DISPLAY_NAME_FIELD,
            })
        );
        let ValidationError::Tenancy(tenancy) = &error else {
            panic!("expected a tenancy refusal, got {error:?}");
        };
        assert!(
            !tenancy.is_incompatible(),
            "a field lost from a schema this build reads is damage, not a skew"
        );
        assert!(
            !TenancyError::FieldType {
                reference: tenant_resource().reference,
                field: TENANT_ID_FIELD,
            }
            .is_incompatible(),
            "and so is a field whose type changed underneath the gateway"
        );
        assert!(
            TenancyError::MissingField {
                reference: tenant_resource().reference,
                field: SCHEMA_FIELD,
            }
            .is_incompatible(),
            "only the identifier's own absence is the legacy shape"
        );

        // Nor is a body that is no longer a record at all, or one under a kind that
        // does not match it. No release, typed or not, wrote a tenancy body as a
        // scalar or a blob, so the shape itself is the evidence of a rewrite, and a
        // newer field set would still be a record refused by its identifier.
        let mut scalar = DesiredState::new();
        let not_a_record = ResourceVersion {
            body: ResourceBody::Inline(CanonicalValue::String("acme".to_owned())),
            ..tenant_resource()
        };
        scalar.insert(not_a_record).expect("a fresh state");
        let error = scalar
            .validate()
            .expect_err("a tenancy body is a record or it is nothing");
        let ValidationError::Tenancy(tenancy) = &error else {
            panic!("expected a tenancy refusal, got {error:?}");
        };
        assert!(
            !tenancy.is_incompatible(),
            "a body no build ever wrote is damage, and points at storage"
        );
        assert!(
            !TenancyError::NotInline {
                reference: tenant_resource().reference,
            }
            .is_incompatible(),
            "and so is a tenancy record replaced by a blob reference"
        );
        assert!(
            !TenancyError::Kind {
                reference: tenant_resource().reference,
                expected: ResourceKind::Tenant,
                found: ResourceKind::Project,
            }
            .is_incompatible(),
            "and so is a row whose kind and body disagree"
        );
    }

    /// A state this build reads, stored where its schema does not put it, points
    /// at the row rather than at an upgrade.
    ///
    /// `active` under the lifecycle schema is the case: the split exists so one
    /// state has one encoding, so no release will ever accept it there. Reported
    /// as an unknown vocabulary it would read "run a build that understands
    /// this", sending an operator after a build that cannot exist while the
    /// rewritten row stays rewritten.
    #[test]
    fn a_state_stored_under_the_wrong_schema_is_damage_not_a_skew() {
        let rewritten = with_fields(&tenant_resource(), |fields| {
            set(
                fields,
                SCHEMA_FIELD,
                CanonicalValue::string(TenantBody::LIFECYCLE_SCHEMA),
            );
            set(
                fields,
                "lifecycle",
                CanonicalValue::string(TenantLifecycle::Active.as_str()),
            );
        });
        let error = TenantBody::read(&rewritten)
            .expect_err("one state has one encoding, so `active` is not spelled here");
        assert_eq!(
            error,
            TenancyError::ValueNotForSchema {
                reference: rewritten.reference,
                schema: TenantBody::LIFECYCLE_SCHEMA,
                field: "lifecycle",
                value: TenantLifecycle::Active.as_str().to_owned(),
            }
        );
        assert!(
            !error.is_incompatible(),
            "a value this build reads is not a value a newer build wrote"
        );

        // A state no build spells stays a compatibility refusal, which is the
        // distinction this test exists to keep: that one *is* answerable by
        // running the release that wrote it.
        let newer = with_fields(&rewritten, |fields| {
            set(fields, "lifecycle", CanonicalValue::string("archived"));
        });
        let error = TenantBody::read(&newer).expect_err("`archived` is nothing this build reads");
        assert!(
            matches!(error, TenancyError::UnknownVocabulary { .. }) && error.is_incompatible(),
            "an unknown state is a skew: {error}"
        );
    }

    #[test]
    fn a_project_needs_a_tenant_this_revision_declares() {
        let mut orphaned = DesiredState::new();
        let project = project(&tenant_id(9), 2, "core");
        orphaned.insert(project.clone()).expect("a fresh state");
        assert_eq!(
            Tenancy::of(&orphaned),
            Err(TenancyError::UnknownTenant {
                reference: project.reference,
                tenant: tenant_id(9)
            })
        );

        // It is the *project* that is held to this, and it is damage rather than a
        // skew: this build never writes a project body without its tenant, so the
        // row went missing rather than never having been required.
        assert!(
            !TenancyError::UnknownTenant {
                reference: project.reference,
                tenant: tenant_id(9),
            }
            .is_incompatible(),
            "a row this build itself wrote and cannot find is not an upgrade"
        );

        // Nothing else is: a revision published before this rule existed could
        // carry a credential or an alias whose tenant no row described, and it
        // still hydrates — the tenancy view needs no tenant for either, and a scope
        // naming a tenant that does not exist is unroutable at the boundary that
        // routes, not unreadable here.
        let mut stray = DesiredState::new();
        let alias = alias(&tenant_id(9), 4, "fast", &[]);
        stray.insert(alias.clone()).expect("a fresh state");
        stray
            .validate()
            .expect("an older revision's tenant-scoped resource is not made unhydratable");
        let tenancy = Tenancy::of(&stray).expect("nothing tenancy reads is missing");
        assert_eq!(tenancy.tenants().len(), 0);
    }

    #[test]
    fn a_project_scoped_resource_names_its_projects_real_owner() {
        let owner = tenant_id(1);
        let other = tenant_id(9);
        let mut state = state();
        state.insert(tenant(9, "globex")).expect("a distinct id");

        // A credential filed under `globex`'s scope but inside `acme`'s project:
        // the pair is inconsistent even though each half exists.
        let leaked = project_credential(&other, &project_id(2), 21, "leaked");
        let mut mixed = state.clone();
        mixed.insert(leaked.clone()).expect("a distinct reference");
        assert_eq!(
            Tenancy::of(&mixed),
            Err(TenancyError::ProjectOwnerMismatch {
                reference: leaked.reference,
                project: project_id(2),
                scoped: other,
                owner
            })
        );

        // A project this revision does not declare contradicts nothing, so there
        // is nothing here to refuse: what the scope names is unroutable, which the
        // boundary that routes says, and an older revision does not become
        // unhydratable for having said it.
        let dangling = project_credential(&owner, &project_id(77), 22, "dangling");
        let mut missing = state.clone();
        missing
            .insert(dangling.clone())
            .expect("a distinct reference");
        missing
            .validate()
            .expect("a scope naming an undeclared project is unroutable, not unreadable");

        // The consistent pair is valid desired state.
        let inside = project_credential(&owner, &project_id(2), 23, "inside");
        let mut consistent = state;
        consistent
            .insert(inside)
            .expect("a distinct reference")
            .validate()
            .expect("a resource inside its own tenant's project is valid");
    }

    #[test]
    fn a_project_slug_is_unique_per_tenant_and_qualified_beyond_it() {
        // Two tenants may both call a project `core`: the slug is scoped, so the
        // envelope-level uniqueness rule permits it.
        let mut state = state();
        state.insert(tenant(9, "globex")).expect("a distinct id");
        state
            .insert(project(&tenant_id(9), 12, "core"))
            .expect("a distinct reference")
            .validate()
            .expect("a project slug is unique within its tenant, not across tenants");

        let tenancy = Tenancy::of(&state).expect("two tenants, two projects");
        assert_eq!(tenancy.projects().len(), 2);
        assert_eq!(tenancy.projects_of(tenant_id(9)).count(), 1);
        let qualified: Vec<String> = tenancy
            .projects()
            .map(|project| {
                tenancy
                    .qualified_name(project.body.project())
                    .expect("a project's tenant is declared")
                    .to_string()
            })
            .collect();
        assert_eq!(qualified, vec!["acme/core", "globex/core"]);
        assert_eq!(
            QualifiedProject::parse("acme/core").map(|name| name.to_string()),
            Some("acme/core".to_owned()),
            "the qualified form decomposes exactly one way"
        );
        assert_eq!(QualifiedProject::parse("acme"), None);

        // The same slug twice within one tenant is not a tenancy question: the
        // envelope answers it, and it answers it the same way it always has.
        let mut clashing = state;
        clashing
            .insert(project(&tenant_id(1), 13, "core"))
            .expect("a distinct reference");
        assert!(matches!(
            clashing.validate(),
            Err(ValidationError::DuplicateSlug { .. })
        ));
    }

    #[test]
    fn a_display_name_is_prose_and_is_normalized_on_the_way_in() {
        assert_eq!(display_name("Acme Corp").as_str(), "Acme Corp");
        assert_eq!(DisplayName::parse(""), Err(InvalidDisplayName::Empty));
        assert_eq!(
            DisplayName::parse("Acme "),
            Err(InvalidDisplayName::Untrimmed)
        );
        assert_eq!(
            DisplayName::parse("Acme\tCorp"),
            Err(InvalidDisplayName::ControlCharacter { codepoint: 0x09 }),
            "a name with no canonical form is refused here, not at publication"
        );
        let long = "a".repeat(DisplayName::MAX_LEN + 1);
        assert_eq!(
            DisplayName::parse(&long),
            Err(InvalidDisplayName::TooLong {
                length: DisplayName::MAX_LEN + 1,
                max: DisplayName::MAX_LEN
            })
        );

        // A tenant body carrying an unencodable name cannot be built, so the
        // canonical form of any body that exists is obtainable.
        assert!(
            tenant_body(1, "Acme")
                .canonical()
                .to_canonical_bytes()
                .is_ok(),
            "a validated body always has canonical bytes"
        );
    }

    /// A byte-order mark is not a control character, so it needs its own rule to
    /// stay in step with the canonical encoder — which refuses it. Without one,
    /// `DisplayName::parse` would accept a name whose body then has no canonical
    /// bytes, turning a validation error into a publication-time encoding failure.
    #[test]
    fn a_display_name_refuses_a_byte_order_mark_exactly_as_the_encoder_does() {
        for name in ["\u{feff}Acme", "Ac\u{feff}me", "Acme\u{feff}"] {
            assert_eq!(
                DisplayName::parse(name),
                Err(InvalidDisplayName::ByteOrderMark),
                "a mark anywhere in `{name}` is refused, not only a leading one"
            );
            // The same string, encoded: what validation is standing in front of.
            assert_eq!(
                CanonicalValue::string(name).to_canonical_bytes(),
                Err(super::super::canonical::CanonicalError::ByteOrderMark),
                "the two layers agree about `{name}`"
            );
        }
        assert!(
            !'\u{feff}'.is_control(),
            "a mark is refused by its own rule, not by the control-character check"
        );
    }

    #[test]
    fn the_view_is_ordered_by_id_so_two_replicas_read_it_the_same_way() {
        let mut state = state();
        state.insert(tenant(9, "globex")).expect("a distinct id");
        state
            .insert(project(&tenant_id(9), 12, "later"))
            .expect("a distinct reference");
        let tenancy = Tenancy::of(&state).expect("valid tenancy");
        let tenants: Vec<TenantId> = tenancy
            .tenants()
            .map(|tenant| tenant.body.tenant())
            .collect();
        let mut sorted = tenants.clone();
        sorted.sort();
        assert_eq!(tenants, sorted);
        let projects: Vec<ProjectId> = tenancy
            .projects()
            .map(|project| project.body.project())
            .collect();
        let mut sorted = projects.clone();
        sorted.sort();
        assert_eq!(projects, sorted);
        assert_eq!(
            tenancy.project(project_id(12)).map(|p| p.slug.as_str()),
            Some("later")
        );
        assert_eq!(tenancy.project(project_id(77)), None);
    }
}
