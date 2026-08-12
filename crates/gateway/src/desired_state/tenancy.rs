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
use super::resource::{
    ResourceBody, ResourceKind, ResourceRef, ResourceScope, ResourceVersion, ResourceVersionNumber,
};
use super::revision::DesiredState;

/// The tenant body schema this build reads and writes.
pub const TENANT_SCHEMA: &str = "axond.tenant.v1";

/// The project body schema this build reads and writes.
pub const PROJECT_SCHEMA: &str = "axond.project.v1";

const SCHEMA_FIELD: &str = "schema";
const TENANT_ID_FIELD: &str = "tenant_id";
const PROJECT_ID_FIELD: &str = "project_id";
const DISPLAY_NAME_FIELD: &str = "display_name";

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
    #[error("{reference} belongs to {tenant}, which this revision does not declare")]
    UnknownTenant {
        reference: ResourceRef,
        tenant: TenantId,
    },
    #[error("{reference} is scoped to {project}, which this revision does not declare")]
    UnknownProject {
        reference: ResourceRef,
        project: ProjectId,
    },
    #[error("{reference} places {project} under {scoped}, but that project belongs to {owner}")]
    ProjectOwnerMismatch {
        reference: ResourceRef,
        project: ProjectId,
        scoped: TenantId,
        owner: TenantId,
    },
}

impl TenancyError {
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
            | Self::UnknownProject { reference, .. }
            | Self::ProjectOwnerMismatch { reference, .. } => *reference,
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
    #[error("a display name may not begin or end with whitespace")]
    Untrimmed,
}

/// An operator-facing name: prose, not identity, and not a [`Slug`].
///
/// Normalized on the way in rather than at every comparison: leading and
/// trailing whitespace are refused instead of trimmed, so a name has one
/// spelling and one checksum. Control characters are refused here too, so an
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
        if let Some(control) = input.chars().find(|c| c.is_control()) {
            return Err(InvalidDisplayName::ControlCharacter {
                codepoint: control as u32,
            });
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

/// A strict reader over one inline record, shared by both body schemas.
struct Record<'a> {
    reference: ResourceRef,
    fields: &'a [(String, CanonicalValue)],
}

impl<'a> Record<'a> {
    /// Open a resource's body as a record of `schema`, refusing a body of the
    /// wrong kind, form, schema, or field set.
    fn open(
        resource: &'a ResourceVersion,
        kind: ResourceKind,
        schema: &'static str,
        known: &[&str],
    ) -> Result<Self, TenancyError> {
        let reference = resource.reference;
        if reference.kind != kind {
            return Err(TenancyError::Kind {
                reference,
                expected: kind,
                found: reference.kind,
            });
        }
        let ResourceBody::Inline(value) = &resource.body else {
            return Err(TenancyError::NotInline { reference });
        };
        let CanonicalValue::Map(fields) = value else {
            return Err(TenancyError::NotARecord { reference });
        };
        let record = Self { reference, fields };
        let declared = record.string(SCHEMA_FIELD)?;
        if declared != schema {
            return Err(TenancyError::Schema {
                reference,
                expected: schema,
                found: declared.to_owned(),
            });
        }
        if let Some((field, _)) = fields
            .iter()
            .find(|(field, _)| field != SCHEMA_FIELD && !known.contains(&field.as_str()))
        {
            return Err(TenancyError::UnknownField {
                reference,
                schema,
                field: field.clone(),
            });
        }
        Ok(record)
    }

    fn string(&self, field: &'static str) -> Result<&'a str, TenancyError> {
        let value = self
            .fields
            .iter()
            .find(|(name, _)| name == field)
            .map(|(_, value)| value)
            .ok_or(TenancyError::MissingField {
                reference: self.reference,
                field,
            })?;
        match value {
            CanonicalValue::String(text) => Ok(text),
            _ => Err(TenancyError::FieldType {
                reference: self.reference,
                field,
            }),
        }
    }

    fn tenant(&self) -> Result<TenantId, TenancyError> {
        TenantId::parse(self.string(TENANT_ID_FIELD)?).map_err(|source| TenancyError::MalformedId {
            reference: self.reference,
            field: TENANT_ID_FIELD,
            source,
        })
    }

    fn project(&self) -> Result<ProjectId, TenancyError> {
        ProjectId::parse(self.string(PROJECT_ID_FIELD)?).map_err(|source| {
            TenancyError::MalformedId {
                reference: self.reference,
                field: PROJECT_ID_FIELD,
                source,
            }
        })
    }

    fn display_name(&self) -> Result<DisplayName, TenancyError> {
        DisplayName::parse(self.string(DISPLAY_NAME_FIELD)?).map_err(|source| {
            TenancyError::MalformedDisplayName {
                reference: self.reference,
                field: DISPLAY_NAME_FIELD,
                source,
            }
        })
    }

    fn identity(
        &self,
        declared: impl fmt::Display,
        identity: ResourceId,
    ) -> Result<(), TenancyError> {
        if self.reference.id == identity {
            Ok(())
        } else {
            Err(TenancyError::IdentityMismatch {
                reference: self.reference,
                declared: declared.to_string(),
                identity: self.reference.id,
            })
        }
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
}

impl TenantBody {
    /// The schema identifier this body encodes and reads.
    pub const SCHEMA: &'static str = TENANT_SCHEMA;

    const KNOWN_FIELDS: &'static [&'static str] = &[TENANT_ID_FIELD, DISPLAY_NAME_FIELD];

    pub const fn new(tenant: TenantId, display_name: DisplayName) -> Self {
        Self {
            tenant,
            display_name,
        }
    }

    pub const fn tenant(&self) -> TenantId {
        self.tenant
    }

    pub const fn display_name(&self) -> &DisplayName {
        &self.display_name
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
    pub fn read(resource: &ResourceVersion) -> Result<Self, TenancyError> {
        let record = Record::open(
            resource,
            ResourceKind::Tenant,
            Self::SCHEMA,
            Self::KNOWN_FIELDS,
        )?;
        let tenant = record.tenant()?;
        record.identity(tenant, ResourceId::new(tenant.uuid()))?;
        Ok(Self {
            tenant,
            display_name: record.display_name()?,
        })
    }
}

impl Canonical for TenantBody {
    fn canonical(&self) -> CanonicalValue {
        CanonicalValue::map([
            (SCHEMA_FIELD, CanonicalValue::string(Self::SCHEMA)),
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
        let record = Record::open(
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
    /// Four things are checked that no envelope-level rule can see:
    ///
    /// 1. every tenancy body is a body of a schema this build reads, bound to its
    ///    own envelope (identity, and a project's owner);
    /// 2. a project's owning tenant is declared by the same revision — a revision
    ///    is whole desired state, so an owner that is merely assumed to exist is
    ///    a dangling owner;
    /// 3. so is the tenant of anything tenant- or project-scoped, which is what
    ///    makes "resource of a tenant this revision does not have" unpublishable
    ///    rather than merely unroutable;
    /// 4. a project-scoped resource names its project's *actual* owner, so a
    ///    project cannot be read under a tenant that does not own it.
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
            let reference = resource.reference;
            match &resource.scope {
                ResourceScope::Deployment => {}
                ResourceScope::Tenant(tenant) => tenancy.require_tenant(reference, *tenant)?,
                ResourceScope::Project { tenant, project } => {
                    tenancy.require_tenant(reference, *tenant)?;
                    let owner = tenancy
                        .projects
                        .get(project)
                        .ok_or(TenancyError::UnknownProject {
                            reference,
                            project: *project,
                        })?
                        .body
                        .tenant();
                    if owner != *tenant {
                        return Err(TenancyError::ProjectOwnerMismatch {
                            reference,
                            project: *project,
                            scoped: *tenant,
                            owner,
                        });
                    }
                }
            }
        }
        Ok(tenancy)
    }

    fn require_tenant(&self, reference: ResourceRef, tenant: TenantId) -> Result<(), TenancyError> {
        if self.tenants.contains_key(&tenant) {
            Ok(())
        } else {
            Err(TenancyError::UnknownTenant { reference, tenant })
        }
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
        alias, candidate, credential, display_name, project, project_body, project_id, reference,
        resource_id, state, tenant, tenant_body, tenant_id,
    };
    use super::super::mutation::ExpectedRevision;
    use super::super::revision::{
        IntegrityError, LoadedRevision, RevisionManifest, ValidationError,
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

        // And so does anything else that is scoped to one.
        let mut stray = DesiredState::new();
        let alias = alias(&tenant_id(9), 4, "fast", &[]);
        stray.insert(alias.clone()).expect("a fresh state");
        assert_eq!(
            Tenancy::of(&stray),
            Err(TenancyError::UnknownTenant {
                reference: alias.reference,
                tenant: tenant_id(9)
            })
        );
    }

    #[test]
    fn a_project_scoped_resource_names_its_projects_real_owner() {
        let owner = tenant_id(1);
        let other = tenant_id(9);
        let mut state = state();
        state.insert(tenant(9, "globex")).expect("a distinct id");

        // A credential filed under `globex`'s scope but inside `acme`'s project:
        // the pair is inconsistent even though each half exists.
        let leaked = ResourceVersion {
            scope: ResourceScope::Project {
                tenant: other,
                project: project_id(2),
            },
            ..credential(&other, 21, "leaked")
        };
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

        // An undeclared project is named as such rather than reported as a
        // mismatch against nothing.
        let dangling = ResourceVersion {
            scope: ResourceScope::Project {
                tenant: owner,
                project: project_id(77),
            },
            ..credential(&owner, 22, "dangling")
        };
        let mut missing = state.clone();
        missing
            .insert(dangling.clone())
            .expect("a distinct reference");
        assert_eq!(
            Tenancy::of(&missing),
            Err(TenancyError::UnknownProject {
                reference: dangling.reference,
                project: project_id(77)
            })
        );

        // The consistent pair is valid desired state.
        let inside = ResourceVersion {
            scope: ResourceScope::Project {
                tenant: owner,
                project: project_id(2),
            },
            ..credential(&owner, 23, "inside")
        };
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
