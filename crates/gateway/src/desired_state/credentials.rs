//! The provider-credential body: a durable pointer to secret material (#198).
//!
//! A provider credential is the resource an operator authors when a tenant brings
//! its own provider key (ADR 0003). What makes it unusual among resources is what
//! it must *not* contain: the key. So its body carries a
//! [`SecretRef`] — an opaque, exactly-versioned handle
//! — the [`SecretOwner`] that handle belongs to, and the
//! [`SecretLifecycle`] state of that material. The bytes are behind
//! [`SecretStore`](crate::backends::secrets::SecretStore), which nothing in this
//! module calls.
//!
//! # What a body carries
//!
//! | Field | Meaning |
//! | --- | --- |
//! | `schema` | `axond.provider-credential.v1` |
//! | `credential_id` | its own [`ResourceId`], bound to the envelope's |
//! | `tenant_id` | the owning [`TenantId`] |
//! | `project_id` | the owning project, when the credential is a project's |
//! | `provider_id` | the provider resource this credential authenticates to |
//! | `display_name` | operator-facing prose |
//! | `secret_id` | the opaque secret this credential points at |
//! | `secret_version` | *which* version of that secret, exactly |
//! | `lifecycle` | `staged`, `active`, `disabled`, `revoked`, or `tombstoned` |
//!
//! The material is absent, and so is anything derived from it: no fingerprint, no
//! prefix, no length. A body is canonically encoded into a checksum an operator
//! can read in a manifest, and a "harmless" four-character prefix in there would
//! be a disclosure that no later change could take back.
//!
//! # Rotation is a new version, not an edit
//!
//! Material is immutable per version: rotation stages a *new* secret version
//! ([`ProviderCredentialBody::rotated`]) and publishing that body is a new
//! resource version of the credential. A revision therefore pins the exact
//! material it was compiled against, and a rotation cannot retroactively change
//! what an already-published revision meant. Putting the new material in service
//! is a separate, deliberate lifecycle move
//! ([`ProviderCredentialBody::transitioned`]).
//!
//! # What is checked, and where
//!
//! [`Credentials::of`] reads every credential body in a [`DesiredState`] and
//! [`DesiredState::validate`] calls it, so publication and hydration inherit the
//! rules with no request path involved:
//!
//! - **ownership** — a body's owner is its envelope's scope, not a second opinion
//!   about it ([`CredentialError::OwnerMismatch`]);
//! - **cross-tenant and cross-project references** — one secret belongs to one
//!   owner ([`CredentialError::SecretOwnerConflict`]), and a credential
//!   authenticates to a provider its own owner can reach
//!   ([`CredentialError::ForeignProvider`]);
//! - **an unambiguous serving version** — two credentials cannot declare two
//!   different active versions of one secret
//!   ([`CredentialError::AmbiguousActiveSecret`]), and two references to one
//!   version cannot disagree about its state
//!   ([`CredentialError::LifecycleConflict`]).
//!
//! Stateless mode is untouched by all of this: `[[credential]]` material still
//! comes from TOML, `env:`, or `file:` through [`crate::credentials`], which has
//! no [`SecretRef`] in it and no dependency on this module.

use std::collections::BTreeMap;

use super::canonical::{Canonical, CanonicalValue};
use super::ids::{InvalidId, ProjectId, ResourceId, SecretId, Slug, TenantId};
use super::record::{
    BodyError, DISPLAY_NAME_FIELD, DisplayNameError, IdentifiedBody, PROJECT_ID_FIELD, Record,
    SCHEMA_FIELD, TENANT_ID_FIELD,
};
use super::resource::{
    ResourceBody, ResourceKind, ResourceRef, ResourceScope, ResourceVersion, ResourceVersionNumber,
};
use super::revision::DesiredState;
use super::secrets::{
    ForbiddenTransition, LifecycleTransition, SecretLifecycle, SecretOwner, SecretRef,
    SecretVersion,
};
use super::tenancy::{DisplayName, InvalidDisplayName};

/// The provider-credential body schema this build reads and writes.
pub const PROVIDER_CREDENTIAL_SCHEMA: &str = "axond.provider-credential.v1";

const CREDENTIAL_ID_FIELD: &str = "credential_id";
const PROVIDER_ID_FIELD: &str = "provider_id";
const SECRET_ID_FIELD: &str = "secret_id";
const SECRET_VERSION_FIELD: &str = "secret_version";
const LIFECYCLE_FIELD: &str = "lifecycle";

/// Why a provider-credential body, or the set of them in a revision, was refused.
///
/// No arm carries material, because no arm has any: a credential error names
/// references, owners, and states, so every one of these is safe to log verbatim.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CredentialError {
    #[error("{reference} is a {} resource, not a {}", found.as_str(), expected.as_str())]
    Kind {
        reference: ResourceRef,
        expected: ResourceKind,
        found: ResourceKind,
    },
    #[error("{reference} is a blob body; a provider-credential record is inline")]
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
    /// A `schema` that is present and is not text, so the identifier deciding how
    /// to read the rest of the body is itself unreadable. No release wrote one,
    /// so the row is damage rather than another release's writing.
    #[error(
        "{reference} carries a `schema` that is not an identifier, which no release wrote; \
         restore the row or republish the resource rather than changing build"
    )]
    DamagedSchema { reference: ResourceRef },
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
    #[error(
        "{reference} field `{field}` is not the type `{}` defines",
        PROVIDER_CREDENTIAL_SCHEMA
    )]
    FieldType {
        reference: ResourceRef,
        field: &'static str,
    },
    #[error("{reference} field `{field}` {source}")]
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
    #[error("{reference} declares owner {declared}, which is not the scope it is filed under")]
    OwnerMismatch {
        reference: ResourceRef,
        declared: SecretOwner,
    },
    /// Version `0`, which no release ever wrote: material is versioned from one.
    #[error("{reference} names secret version {found}; versions start at 1")]
    SecretVersion { reference: ResourceRef, found: u64 },
    /// A lifecycle identifier this build does not know — a state a newer release
    /// defined, so a compatibility refusal rather than damage.
    #[error("{reference} declares lifecycle `{found}`, which this build does not know")]
    UnknownLifecycle {
        reference: ResourceRef,
        found: String,
    },
    /// One secret, two owners. The reference is opaque, so nothing about the
    /// material itself would reveal that a tenant had been handed another
    /// tenant's key — this is the rule that refuses it.
    #[error("{reference} claims secret {secret}, which {conflicting} claims for a different owner")]
    SecretOwnerConflict {
        reference: ResourceRef,
        secret: SecretId,
        conflicting: ResourceRef,
    },
    /// A credential naming a provider resource its owner cannot reach: another
    /// tenant's provider, or another project's.
    #[error("{reference} authenticates to {provider}, which {owner} cannot reach")]
    ForeignProvider {
        reference: ResourceRef,
        provider: ResourceRef,
        owner: SecretOwner,
    },
    /// A `provider_id` that names something in this revision which is not a
    /// provider.
    #[error("{reference} names provider {provider}, which this revision declares as a {}", found.as_str())]
    NotAProvider {
        reference: ResourceRef,
        provider: ResourceId,
        found: ResourceKind,
    },
    /// Two credentials, one secret version, two states: the material would be
    /// both in service and not, depending on which row was read.
    #[error(
        "{reference} declares {secret} `{state}`, but {conflicting} declares it `{conflicting_state}`"
    )]
    LifecycleConflict {
        reference: ResourceRef,
        secret: SecretRef,
        state: SecretLifecycle,
        conflicting: ResourceRef,
        conflicting_state: SecretLifecycle,
    },
    /// Two versions of one secret, both active: which material a request would
    /// be authorized by would depend on iteration order.
    #[error(
        "{reference} activates {secret}, but {conflicting} already activates another version of it"
    )]
    AmbiguousActiveSecret {
        reference: ResourceRef,
        secret: SecretRef,
        conflicting: ResourceRef,
    },
}

impl CredentialError {
    /// Whether this refusal means *this build cannot read the body*, rather than
    /// *these rows do not agree with each other*.
    ///
    /// The same division [`TenancyError::is_incompatible`] draws, and for the same
    /// reason: a compatibility refusal tells an operator that storage is intact
    /// and the fix is a build or a revision, while everything else is real repair
    /// work. A body declaring a schema, a field, or a *lifecycle state* this
    /// release does not know is the newer-build case; a contradiction between two
    /// readable rows is not.
    ///
    /// [`TenancyError::is_incompatible`]: super::tenancy::TenancyError::is_incompatible
    pub fn is_incompatible(&self) -> bool {
        match self {
            Self::Schema { .. }
            | Self::UnknownField { .. }
            | Self::UnknownLifecycle { .. }
            | Self::MalformedDisplayName { .. } => true,
            // Absence of the schema identifier only: a body written before
            // provider credentials had one at all is another release's writing,
            // while a marker that is present and unreadable is `DamagedSchema`.
            Self::MissingField { field, .. } => *field == SCHEMA_FIELD,
            Self::FieldType { .. } | Self::DamagedSchema { .. } => false,
            Self::Kind { .. }
            | Self::NotInline { .. }
            | Self::NotARecord { .. }
            | Self::MalformedId { .. }
            | Self::IdentityMismatch { .. }
            | Self::OwnerMismatch { .. }
            | Self::SecretVersion { .. }
            | Self::SecretOwnerConflict { .. }
            | Self::ForeignProvider { .. }
            | Self::NotAProvider { .. }
            | Self::LifecycleConflict { .. }
            | Self::AmbiguousActiveSecret { .. } => false,
        }
    }

    /// The resource this refusal is about.
    pub const fn reference(&self) -> ResourceRef {
        match self {
            Self::Kind { reference, .. }
            | Self::NotInline { reference }
            | Self::NotARecord { reference }
            | Self::Schema { reference, .. }
            | Self::DamagedSchema { reference }
            | Self::MissingField { reference, .. }
            | Self::UnknownField { reference, .. }
            | Self::FieldType { reference, .. }
            | Self::MalformedId { reference, .. }
            | Self::MalformedDisplayName { reference, .. }
            | Self::IdentityMismatch { reference, .. }
            | Self::OwnerMismatch { reference, .. }
            | Self::SecretVersion { reference, .. }
            | Self::UnknownLifecycle { reference, .. }
            | Self::SecretOwnerConflict { reference, .. }
            | Self::ForeignProvider { reference, .. }
            | Self::NotAProvider { reference, .. }
            | Self::LifecycleConflict { reference, .. }
            | Self::AmbiguousActiveSecret { reference, .. } => *reference,
        }
    }
}

impl BodyError for CredentialError {
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

    fn damaged_schema(reference: ResourceRef) -> Self {
        Self::DamagedSchema { reference }
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
}

impl DisplayNameError for CredentialError {
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

impl IdentifiedBody for CredentialError {
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

/// A tenant's or project's credential for one provider: an owner, a provider, and
/// an opaque reference to the material that authenticates to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderCredentialBody {
    credential: ResourceId,
    owner: SecretOwner,
    provider: ResourceId,
    display_name: DisplayName,
    secret: SecretRef,
    lifecycle: SecretLifecycle,
}

impl ProviderCredentialBody {
    /// The schema identifier this body encodes and reads.
    pub const SCHEMA: &'static str = PROVIDER_CREDENTIAL_SCHEMA;

    const KNOWN_FIELDS: &'static [&'static str] = &[
        CREDENTIAL_ID_FIELD,
        TENANT_ID_FIELD,
        PROJECT_ID_FIELD,
        PROVIDER_ID_FIELD,
        DISPLAY_NAME_FIELD,
        SECRET_ID_FIELD,
        SECRET_VERSION_FIELD,
        LIFECYCLE_FIELD,
    ];

    /// A newly authored credential, pointing at freshly staged material.
    ///
    /// Staged rather than active on purpose: material is loaded, then proven by
    /// compiling a candidate revision against it, and only then put in service.
    /// Nothing here can be authored straight into the request path.
    pub const fn staged(
        credential: ResourceId,
        owner: SecretOwner,
        provider: ResourceId,
        display_name: DisplayName,
        secret: SecretRef,
    ) -> Self {
        Self {
            credential,
            owner,
            provider,
            display_name,
            secret,
            lifecycle: SecretLifecycle::Staged,
        }
    }

    pub const fn credential(&self) -> ResourceId {
        self.credential
    }

    /// Who owns this credential and, by construction, its material.
    pub const fn owner(&self) -> SecretOwner {
        self.owner
    }

    pub const fn tenant(&self) -> TenantId {
        self.owner.tenant
    }

    pub const fn project(&self) -> Option<ProjectId> {
        self.owner.project
    }

    pub const fn provider(&self) -> ResourceId {
        self.provider
    }

    pub const fn display_name(&self) -> &DisplayName {
        &self.display_name
    }

    /// The exact material this credential authenticates with.
    pub const fn secret(&self) -> SecretRef {
        self.secret
    }

    pub const fn lifecycle(&self) -> SecretLifecycle {
        self.lifecycle
    }

    /// Whether this credential's material may be unwrapped during snapshot
    /// compilation. Lifecycle only — the store still checks ownership, and it is
    /// the store that holds the material.
    pub const fn permits_resolution(&self) -> bool {
        self.lifecycle.permits_resolution()
    }

    /// The same credential, its material moved to `next`.
    ///
    /// Metadata only: no plaintext is read, written, or returned, so a lifecycle
    /// change never has to touch the secret store. Idempotent moves return an
    /// unchanged body, which is what makes republishing the same desired state a
    /// no-op instead of a conflict.
    pub fn transitioned(&self, next: SecretLifecycle) -> Result<Self, ForbiddenTransition> {
        let transition = self.lifecycle.transition_to(next)?;
        Ok(Self {
            lifecycle: transition.state(),
            display_name: self.display_name.clone(),
            ..*self
        })
    }

    /// What [`ProviderCredentialBody::transitioned`] would do, without doing it.
    pub fn transition_to(
        &self,
        next: SecretLifecycle,
    ) -> Result<LifecycleTransition, ForbiddenTransition> {
        self.lifecycle.transition_to(next)
    }

    /// The same credential, pointing at the next version of the same secret.
    ///
    /// The new version is staged: rotation stores material, and putting it in
    /// service is a separate decision. The previous version keeps whatever state
    /// it had, in the revision that named it.
    ///
    /// One resource names one version, so *this* body no longer names the version
    /// it was serving. A rotation that must not interrupt service is therefore two
    /// credential resources — the serving one untouched, a second one staged
    /// against the new version — and the credential the old one names is withdrawn
    /// only after the new one is active. Publishing this body alone is the
    /// deliberate cut-over, not the overlap.
    pub fn rotated(&self) -> Self {
        Self {
            secret: self.secret.rotated(),
            lifecycle: SecretLifecycle::Staged,
            display_name: self.display_name.clone(),
            ..*self
        }
    }

    /// This credential, reauthored from `authored`: the author's provider,
    /// display name, and material, over the lifecycle the credential is
    /// actually in.
    ///
    /// Authoring never *sets* a lifecycle — that is a transition the domain
    /// owns — but naming different material is not a metadata edit: the new
    /// version re-enters [`SecretLifecycle::Staged`], because material is proven
    /// by compiling a candidate against it before it serves.
    pub fn reauthored(&self, authored: Self) -> Self {
        let lifecycle = if authored.secret == self.secret {
            self.lifecycle
        } else {
            SecretLifecycle::Staged
        };
        Self {
            lifecycle,
            ..authored
        }
    }

    /// The resource identity this credential's versions are written under.
    pub const fn resource_id(&self) -> ResourceId {
        self.credential
    }

    pub fn body(&self) -> ResourceBody {
        ResourceBody::Inline(self.canonical())
    }

    /// The scope this credential's versions live at: exactly its owner's.
    pub const fn scope(&self) -> ResourceScope {
        self.owner.scope()
    }

    pub fn version(&self, slug: Slug) -> ResourceVersion {
        self.version_at(slug, ResourceVersionNumber::FIRST)
    }

    pub fn version_at(&self, slug: Slug, version: ResourceVersionNumber) -> ResourceVersion {
        ResourceVersion::new(
            ResourceRef::new(
                ResourceKind::ProviderCredential,
                self.resource_id(),
                version,
            ),
            self.scope(),
            slug,
            self.body(),
        )
    }

    /// Read a provider-credential resource's body, binding it to its envelope:
    /// identity to the reference, ownership to the scope.
    pub fn read(resource: &ResourceVersion) -> Result<Self, CredentialError> {
        let record = Record::<CredentialError>::open(
            resource,
            ResourceKind::ProviderCredential,
            Self::SCHEMA,
            Self::KNOWN_FIELDS,
        )?;
        let credential = record.typed_id(CREDENTIAL_ID_FIELD, ResourceId::parse)?;
        record.identity(credential, credential)?;
        let owner = SecretOwner {
            tenant: record.tenant()?,
            project: record.optional_project()?,
        };
        if resource.scope != owner.scope() {
            return Err(CredentialError::OwnerMismatch {
                reference: resource.reference,
                declared: owner,
            });
        }
        let version = record.integer(SECRET_VERSION_FIELD)?;
        let secret = SecretRef::new(
            record.typed_id(SECRET_ID_FIELD, SecretId::parse)?,
            SecretVersion::new(version).ok_or(CredentialError::SecretVersion {
                reference: resource.reference,
                found: version,
            })?,
        );
        let declared = record.string(LIFECYCLE_FIELD)?;
        let lifecycle =
            SecretLifecycle::parse(declared).ok_or_else(|| CredentialError::UnknownLifecycle {
                reference: resource.reference,
                found: declared.to_owned(),
            })?;
        Ok(Self {
            credential,
            owner,
            provider: record.typed_id(PROVIDER_ID_FIELD, ResourceId::parse)?,
            display_name: record.display_name()?,
            secret,
            lifecycle,
        })
    }
}

impl Canonical for ProviderCredentialBody {
    fn canonical(&self) -> CanonicalValue {
        // `project_id` is absent rather than empty for a tenant-scoped
        // credential: the canonical model has no null, and an empty id would be a
        // second spelling of "none".
        let mut fields = vec![
            (SCHEMA_FIELD, CanonicalValue::string(Self::SCHEMA)),
            (
                CREDENTIAL_ID_FIELD,
                CanonicalValue::string(self.credential.to_string()),
            ),
            (
                TENANT_ID_FIELD,
                CanonicalValue::string(self.owner.tenant.to_string()),
            ),
            (
                PROVIDER_ID_FIELD,
                CanonicalValue::string(self.provider.to_string()),
            ),
            (
                DISPLAY_NAME_FIELD,
                CanonicalValue::string(self.display_name.as_str()),
            ),
            (
                SECRET_ID_FIELD,
                CanonicalValue::string(self.secret.secret.to_string()),
            ),
            (
                SECRET_VERSION_FIELD,
                CanonicalValue::integer(self.secret.version.get()),
            ),
            (
                LIFECYCLE_FIELD,
                CanonicalValue::string(self.lifecycle.as_str()),
            ),
        ];
        if let Some(project) = self.owner.project {
            fields.push((
                PROJECT_ID_FIELD,
                CanonicalValue::string(project.to_string()),
            ));
        }
        CanonicalValue::map(fields)
    }
}

/// A provider credential as a revision holds it: its envelope, its name, and its
/// body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderCredential {
    pub reference: ResourceRef,
    pub slug: Slug,
    pub body: ProviderCredentialBody,
}

/// The credentials of one revision, read once.
///
/// Built by [`Credentials::of`], which is the single place credential bodies are
/// interpreted, so publication and hydration cannot reach different conclusions
/// about the same revision. Ordering is by id, so two replicas iterate the same
/// credentials in the same order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Credentials {
    credentials: BTreeMap<ResourceId, ProviderCredential>,
    owners: BTreeMap<SecretId, (SecretOwner, ResourceRef)>,
}

impl Credentials {
    /// Read and cross-check every provider credential in a desired state.
    ///
    /// Beyond reading each body strictly, four properties are checked that no
    /// envelope-level rule can see, because the envelope cannot see inside a
    /// body:
    ///
    /// 1. one secret belongs to one owner, so a reference cannot carry material
    ///    across a tenant or project boundary;
    /// 2. a credential authenticates to a provider its owner can reach: its own
    ///    scope, or its tenant's, and never another tenant's or a sibling
    ///    project's;
    /// 3. two references to one secret version agree about that version's state;
    /// 4. at most one version of a secret is active, so what a request would be
    ///    authorized by does not depend on iteration order.
    ///
    /// A `provider_id` naming a resource this revision does not declare is *not*
    /// refused, for the reason [`Tenancy::of`] gives about tenant-scoped
    /// resources: a revision published before this rule existed may name one, and
    /// hydration runs these same checks, so requiring it would stop such a
    /// revision from loading on upgrade. What the reference names is then
    /// unresolvable at the boundary that resolves it, which is not the same thing
    /// as unreadable here.
    ///
    /// [`Tenancy::of`]: super::tenancy::Tenancy::of
    pub fn of(state: &DesiredState) -> Result<Self, CredentialError> {
        let mut credentials = Self::default();
        for resource in state.resources() {
            if resource.reference.kind != ResourceKind::ProviderCredential {
                continue;
            }
            let body = ProviderCredentialBody::read(resource)?;
            let owner = body.owner();
            if let Some((claimed_by, conflicting)) =
                credentials.owners.get(&body.secret().secret).copied()
                && claimed_by != owner
            {
                return Err(CredentialError::SecretOwnerConflict {
                    reference: resource.reference,
                    secret: body.secret().secret,
                    conflicting,
                });
            }
            credentials
                .owners
                .insert(body.secret().secret, (owner, resource.reference));
            credentials.credentials.insert(
                body.credential(),
                ProviderCredential {
                    reference: resource.reference,
                    slug: resource.slug.clone(),
                    body,
                },
            );
        }

        credentials.check_providers(state)?;
        credentials.check_lifecycles()?;
        Ok(credentials)
    }

    /// A credential reaches a provider at its own scope, or at its tenant's.
    fn check_providers(&self, state: &DesiredState) -> Result<(), CredentialError> {
        for credential in self.credentials.values() {
            let owner = credential.body.owner();
            let Some(provider) = state
                .resources()
                .find(|resource| resource.reference.id == credential.body.provider())
            else {
                continue;
            };
            if provider.reference.kind != ResourceKind::Provider {
                return Err(CredentialError::NotAProvider {
                    reference: credential.reference,
                    provider: credential.body.provider(),
                    found: provider.reference.kind,
                });
            }
            let reachable = provider.scope == owner.scope()
                || provider.scope == ResourceScope::Tenant(owner.tenant);
            if !reachable {
                return Err(CredentialError::ForeignProvider {
                    reference: credential.reference,
                    provider: provider.reference,
                    owner,
                });
            }
        }
        Ok(())
    }

    /// One state per secret version, and one active version per secret.
    ///
    /// Two credentials naming the *same* active version are not refused, and the
    /// rule is about ambiguity rather than tidiness: one version's material serves
    /// either way, so nothing depends on which row is read. Refusing it would also
    /// make an alias-style arrangement — two provider resources authenticating with
    /// one key — unpublishable for no safety gain.
    fn check_lifecycles(&self) -> Result<(), CredentialError> {
        let mut states: BTreeMap<SecretRef, (SecretLifecycle, ResourceRef)> = BTreeMap::new();
        let mut active: BTreeMap<SecretId, (SecretRef, ResourceRef)> = BTreeMap::new();
        for credential in self.credentials.values() {
            let secret = credential.body.secret();
            let state = credential.body.lifecycle();
            if let Some((declared, conflicting)) = states.get(&secret).copied()
                && declared != state
            {
                return Err(CredentialError::LifecycleConflict {
                    reference: credential.reference,
                    secret,
                    state,
                    conflicting,
                    conflicting_state: declared,
                });
            }
            states.insert(secret, (state, credential.reference));
            if state != SecretLifecycle::Active {
                continue;
            }
            if let Some((conflicting_secret, conflicting)) = active.get(&secret.secret).copied()
                && conflicting_secret != secret
            {
                return Err(CredentialError::AmbiguousActiveSecret {
                    reference: credential.reference,
                    secret,
                    conflicting,
                });
            }
            active.insert(secret.secret, (secret, credential.reference));
        }
        Ok(())
    }

    /// Every credential, ordered by id.
    pub fn all(&self) -> impl ExactSizeIterator<Item = &ProviderCredential> {
        self.credentials.values()
    }

    pub fn len(&self) -> usize {
        self.credentials.len()
    }

    pub fn is_empty(&self) -> bool {
        self.credentials.is_empty()
    }

    pub fn get(&self, credential: ResourceId) -> Option<&ProviderCredential> {
        self.credentials.get(&credential)
    }

    /// The credentials one owner holds.
    pub fn of_owner(&self, owner: SecretOwner) -> impl Iterator<Item = &ProviderCredential> {
        self.credentials
            .values()
            .filter(move |credential| credential.body.owner() == owner)
    }

    /// The credentials whose material is in service.
    pub fn active(&self) -> impl Iterator<Item = &ProviderCredential> {
        self.credentials
            .values()
            .filter(|credential| credential.body.lifecycle() == SecretLifecycle::Active)
    }

    /// Who owns a secret this revision references, if it references it.
    ///
    /// The reverse lookup a resolver needs: given a reference, the owner it must
    /// be resolved as, taken from the revision rather than from the caller.
    pub fn owner_of(&self, secret: SecretId) -> Option<SecretOwner> {
        self.owners.get(&secret).map(|(owner, _)| *owner)
    }

    /// Every exact secret version this revision's credentials pin, with the owner
    /// each must be resolved as.
    ///
    /// What snapshot compilation iterates: a revision is publishable once every
    /// one of these resolves, and a resolution failure is a rejected candidate
    /// rather than a request-time error.
    pub fn required_secrets(&self) -> impl Iterator<Item = (SecretOwner, SecretRef)> {
        self.credentials
            .values()
            .filter(|credential| credential.body.permits_resolution())
            .map(|credential| (credential.body.owner(), credential.body.secret()))
    }
}

#[cfg(test)]
mod tests {
    use super::super::canonical::SerializerVersion;
    use super::super::fixtures::{
        alias, candidate, credential, credential_body, display_name, legacy_credential,
        project_credential, project_id, provider, provider_id, resource_id, revision_id, secret_id,
        secret_ref, secret_ref_at, state, tenant, tenant_id,
    };
    use super::super::ids::Slug;
    use super::super::mutation::ExpectedRevision;
    use super::super::revision::{
        BodySkew, IntegrityError, LoadedRevision, RevisionManifest, ValidationError,
    };
    use super::*;

    /// The material a test must never be able to find in a body, an error, or a
    /// checksummed encoding — because no type here can hold it.
    const PLAINTEXT: &str = "sk-live-do-not-log";

    fn owner() -> SecretOwner {
        SecretOwner::tenant(tenant_id(1))
    }

    fn slug(name: &str) -> Slug {
        Slug::parse(name).expect("fixture slug")
    }

    /// Rewrite a credential's inline record: how a body no caller could author —
    /// or a newer build's body — is put in front of the reader.
    fn with_fields(
        resource: &ResourceVersion,
        edit: impl FnOnce(&mut Vec<(String, CanonicalValue)>),
    ) -> ResourceVersion {
        let ResourceBody::Inline(CanonicalValue::Map(fields)) = &resource.body else {
            panic!("a credential fixture body is an inline record");
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

    /// A state holding `resources` and nothing else, for the cases that are about
    /// credential bodies rather than about a whole revision.
    fn state_of(resources: impl IntoIterator<Item = ResourceVersion>) -> DesiredState {
        let mut state = DesiredState::new();
        for resource in resources {
            state.insert(resource).expect("distinct references");
        }
        state
    }

    #[test]
    fn a_body_round_trips_through_its_envelope_and_its_canonical_bytes() {
        let body = credential_body(&tenant_id(1), 3, "primary");
        let resource = credential(&tenant_id(1), 3, "primary");
        assert_eq!(ProviderCredentialBody::read(&resource).unwrap(), body);
        assert_eq!(resource.reference.kind, ResourceKind::ProviderCredential);
        assert_eq!(resource.reference.id, resource_id(3));
        assert_eq!(resource.scope, ResourceScope::Tenant(tenant_id(1)));
        assert_eq!(body.secret(), secret_ref(3));
        assert_eq!(body.owner(), owner());
        assert_eq!(body.tenant(), tenant_id(1));
        assert_eq!(body.project(), None);
        assert_eq!(body.provider(), provider_id(3));
        assert_eq!(body.resource_id(), body.credential());

        let bytes = SerializerVersion::V1.encode(&body.canonical()).unwrap();
        let decoded = SerializerVersion::V1
            .decode(&bytes)
            .expect("a credential body is canonical, so storage returns what it took");
        assert_eq!(SerializerVersion::V1.encode(&decoded).unwrap(), bytes);
        assert_eq!(
            ProviderCredentialBody::read(&ResourceVersion {
                body: ResourceBody::Inline(decoded),
                ..resource
            })
            .unwrap(),
            body,
            "and reads back as the same body"
        );
        assert!(
            String::from_utf8_lossy(&bytes).contains(PROVIDER_CREDENTIAL_SCHEMA),
            "the schema identifier is part of the checksummed body"
        );

        // A project's credential names its project; a tenant's omits the field
        // rather than carrying an empty one.
        let inner = project_credential(&tenant_id(1), &project_id(2), 4, "inner");
        let inner = ProviderCredentialBody::read(&inner).unwrap();
        assert_eq!(
            inner.owner(),
            SecretOwner::project(tenant_id(1), project_id(2))
        );
        assert_eq!(
            inner.scope(),
            ResourceScope::Project {
                tenant: tenant_id(1),
                project: project_id(2)
            }
        );
        let CanonicalValue::Map(fields) = body.canonical() else {
            panic!("a body is a record");
        };
        assert!(
            !fields.iter().any(|(field, _)| field == PROJECT_ID_FIELD),
            "a tenant-scoped credential has no project field at all"
        );
    }

    /// The point of the whole slice: a body is a *reference*, so there is no field
    /// a plaintext, a fingerprint, or a prefix could travel in.
    #[test]
    fn a_body_carries_a_reference_and_nothing_derived_from_the_material() {
        let body = credential_body(&tenant_id(1), 3, "primary");
        let CanonicalValue::Map(fields) = body.canonical() else {
            panic!("a body is a record");
        };
        let mut names: Vec<&str> = fields.iter().map(|(field, _)| field.as_str()).collect();
        names.sort_unstable();
        assert_eq!(
            names,
            [
                "credential_id",
                "display_name",
                "lifecycle",
                "provider_id",
                "schema",
                "secret_id",
                "secret_version",
                "tenant_id",
            ],
            "a new field here is a new disclosure to review"
        );

        // Every rendering of a body, and of the resource that carries it, is
        // material-free — there being no material in it is why.
        let resource = credential(&tenant_id(1), 3, "primary");
        let bytes = SerializerVersion::V1.encode(&body.canonical()).unwrap();
        for rendered in [
            format!("{body:?}"),
            format!("{resource:?}"),
            String::from_utf8_lossy(&bytes).into_owned(),
        ] {
            assert!(!rendered.contains(PLAINTEXT), "{rendered}");
            assert!(!rendered.contains("sk-"), "{rendered}");
        }
        assert!(
            format!("{body:?}").contains(&secret_id(3).uuid().to_string()),
            "the opaque reference is what a diagnostic prints"
        );
    }

    #[test]
    fn material_is_staged_before_it_is_ever_in_service() {
        let body = credential_body(&tenant_id(1), 3, "primary");
        assert_eq!(body.lifecycle(), SecretLifecycle::Staged);
        assert!(
            body.permits_resolution(),
            "staged material resolves so a candidate can be compiled against it"
        );

        let active = body.transitioned(SecretLifecycle::Active).unwrap();
        assert_eq!(active.lifecycle(), SecretLifecycle::Active);
        assert_eq!(active.secret(), body.secret(), "a state change is metadata");
        assert_eq!(active.credential(), body.credential());
        assert_eq!(active.display_name(), body.display_name());

        // Republishing the same desired state is a no-op, not a conflict.
        assert_eq!(
            active.transition_to(SecretLifecycle::Active).unwrap(),
            LifecycleTransition::Unchanged(SecretLifecycle::Active)
        );
        assert_eq!(
            active.transitioned(SecretLifecycle::Active).unwrap(),
            active
        );

        // Withdrawn material stops resolving without being edited or deleted.
        let disabled = active.transitioned(SecretLifecycle::Disabled).unwrap();
        assert!(!disabled.permits_resolution());
        assert_eq!(
            disabled
                .transitioned(SecretLifecycle::Active)
                .unwrap()
                .lifecycle(),
            SecretLifecycle::Active,
            "disabling is reversible; revoking is not"
        );
        let revoked = active.transitioned(SecretLifecycle::Revoked).unwrap();
        assert_eq!(
            revoked.transitioned(SecretLifecycle::Active),
            Err(ForbiddenTransition {
                from: SecretLifecycle::Revoked,
                to: SecretLifecycle::Active
            })
        );
    }

    #[test]
    fn rotation_pins_a_new_version_instead_of_editing_the_old_one() {
        let first = credential_body(&tenant_id(1), 3, "primary")
            .transitioned(SecretLifecycle::Active)
            .unwrap();
        let second = first.rotated();

        assert_eq!(second.secret(), secret_ref_at(3, 2));
        assert!(second.secret().is_same_secret(first.secret()));
        assert_eq!(
            second.lifecycle(),
            SecretLifecycle::Staged,
            "rotation stores material; putting it in service is a separate move"
        );
        assert_eq!(
            first.secret(),
            secret_ref(3),
            "the published body still pins the material it was compiled against"
        );
        assert_eq!(first.lifecycle(), SecretLifecycle::Active);
        assert_eq!(second.credential(), first.credential());
        assert_eq!(second.owner(), first.owner());

        // A rotated body is a new *resource version* of the same credential, so
        // the revision that pinned version 1 is untouched by it.
        let published = second.version_at(slug("primary"), ResourceVersionNumber::FIRST.next());
        assert_eq!(published.reference.id, first.credential());
        assert_eq!(
            ProviderCredentialBody::read(&published).unwrap().secret(),
            secret_ref_at(3, 2)
        );
    }

    /// One resource names one version, so an operator who must not interrupt
    /// service authors the new material *beside* the serving credential and
    /// withdraws the old one after the cut-over. This is the sequence an admin
    /// surface has to produce; every step of it publishes, and the one step that
    /// would make "which key authorizes this" ambiguous does not.
    #[test]
    fn an_uninterrupted_rotation_is_two_credentials_and_a_deliberate_cut_over() {
        let serving = credential_body(&tenant_id(1), 3, "primary")
            .transitioned(SecretLifecycle::Active)
            .unwrap();
        // Step 1: the new version is staged beside the serving one, under its own
        // credential resource, so nothing stops serving while it is proven.
        let incoming = ProviderCredentialBody::staged(
            resource_id(18),
            owner(),
            provider_id(18),
            display_name("Rotating"),
            serving.secret().rotated(),
        );
        let overlap = state_of([
            serving.version(slug("primary")),
            incoming.version(slug("rotating")),
        ]);
        let credentials = Credentials::of(&overlap).expect("staging beside a serving credential");
        assert_eq!(credentials.active().count(), 1, "one version serves");
        assert_eq!(credentials.of_owner(owner()).count(), 2);

        // Step 2: activating the new version *before* withdrawing the old one is
        // the ambiguity the rules exist to refuse, not a valid overlap.
        let contested = state_of([
            serving.version(slug("primary")),
            incoming
                .transitioned(SecretLifecycle::Active)
                .unwrap()
                .version(slug("rotating")),
        ]);
        assert!(matches!(
            Credentials::of(&contested).expect_err("two active versions of one secret"),
            CredentialError::AmbiguousActiveSecret { .. }
        ));

        // Step 3: the cut-over — the old version is withdrawn in the same revision
        // that puts the new one in service, so no revision has either two active
        // versions or none.
        let cut_over = state_of([
            serving
                .transitioned(SecretLifecycle::Revoked)
                .unwrap()
                .version_at(slug("primary"), ResourceVersionNumber::FIRST.next()),
            incoming
                .transitioned(SecretLifecycle::Active)
                .unwrap()
                .version(slug("rotating")),
        ]);
        let credentials = Credentials::of(&cut_over).expect("a cut-over publishes");
        let mut active = credentials.active();
        assert_eq!(
            active.next().expect("one active credential").body.secret(),
            secret_ref_at(3, 2),
            "the new material serves, and only it"
        );
        assert!(active.next().is_none());

        // Repointing the serving credential instead, which `rotated` does, is the
        // same cut-over in one resource: it publishes, and it leaves the revision
        // with no active version until a further move, which is why an
        // uninterrupted rotation is authored as two.
        let repointed = state_of([serving
            .rotated()
            .version_at(slug("primary"), ResourceVersionNumber::FIRST.next())]);
        assert_eq!(
            Credentials::of(&repointed)
                .expect("repointing publishes")
                .active()
                .count(),
            0
        );
    }

    #[test]
    fn a_body_cannot_declare_an_owner_its_envelope_does_not_place_it_under() {
        // Scope and body disagree: the credential is filed under a project, its
        // body claims the tenant.
        let resource = credential(&tenant_id(1), 3, "primary");
        let misfiled = ResourceVersion {
            scope: ResourceScope::Project {
                tenant: tenant_id(1),
                project: project_id(2),
            },
            ..resource.clone()
        };
        assert_eq!(
            ProviderCredentialBody::read(&misfiled),
            Err(CredentialError::OwnerMismatch {
                reference: misfiled.reference,
                declared: owner()
            })
        );
        // Another tenant's scope, same body: ownership is the envelope's.
        let stolen = ResourceVersion {
            scope: ResourceScope::Tenant(tenant_id(9)),
            ..resource.clone()
        };
        assert!(matches!(
            ProviderCredentialBody::read(&stolen),
            Err(CredentialError::OwnerMismatch { .. })
        ));

        // And a body's declared identity is its envelope's.
        let renamed = with_fields(&resource, |fields| {
            set(
                fields,
                CREDENTIAL_ID_FIELD,
                CanonicalValue::string(resource_id(99).to_string()),
            );
        });
        assert_eq!(
            ProviderCredentialBody::read(&renamed),
            Err(CredentialError::IdentityMismatch {
                reference: renamed.reference,
                declared: resource_id(99).to_string(),
                identity: resource_id(3)
            })
        );
    }

    #[test]
    fn a_body_this_build_cannot_read_is_a_compatibility_refusal_not_damage() {
        let resource = credential(&tenant_id(1), 3, "primary");

        // A newer release's schema, and a field it added: both mean "run a build
        // that reads this", not "storage is damaged".
        let newer = with_fields(&resource, |fields| {
            set(
                fields,
                SCHEMA_FIELD,
                CanonicalValue::string("axond.provider-credential.v2"),
            );
        });
        let error = ProviderCredentialBody::read(&newer).expect_err("a v2 body");
        assert_eq!(
            error,
            CredentialError::Schema {
                reference: newer.reference,
                expected: PROVIDER_CREDENTIAL_SCHEMA,
                found: "axond.provider-credential.v2".to_owned()
            }
        );
        assert!(error.is_incompatible());
        assert_eq!(error.reference(), newer.reference);

        let extended = with_fields(&resource, |fields| {
            set(fields, "rotation_policy", CanonicalValue::string("monthly"));
        });
        assert!(
            ProviderCredentialBody::read(&extended)
                .expect_err("an unknown field")
                .is_incompatible()
        );

        // A lifecycle state a newer release defined is the same class: the body is
        // well-formed, this build just does not know what it says.
        let unknown = with_fields(&resource, |fields| {
            set(
                fields,
                LIFECYCLE_FIELD,
                CanonicalValue::string("quarantined"),
            );
        });
        let error = ProviderCredentialBody::read(&unknown).expect_err("an unknown state");
        assert_eq!(
            error,
            CredentialError::UnknownLifecycle {
                reference: unknown.reference,
                found: "quarantined".to_owned()
            }
        );
        assert!(error.is_incompatible());

        // A marker that is present and is not an identifier is the other side of
        // that boundary: no release wrote one, so it is damage rather than another
        // release's writing, and the operator is sent to storage.
        for marker in [
            CanonicalValue::integer(1),
            CanonicalValue::List(vec![CanonicalValue::string(PROVIDER_CREDENTIAL_SCHEMA)]),
            CanonicalValue::map([(
                SCHEMA_FIELD,
                CanonicalValue::string(PROVIDER_CREDENTIAL_SCHEMA),
            )]),
        ] {
            let damaged = with_fields(&resource, |fields| {
                set(fields, SCHEMA_FIELD, marker.clone());
            });
            let error = ProviderCredentialBody::read(&damaged).expect_err("an unreadable marker");
            assert_eq!(
                error,
                CredentialError::DamagedSchema {
                    reference: damaged.reference
                }
            );
            assert!(!error.is_incompatible(), "{error}");
            assert!(
                error.to_string().contains("restore the row"),
                "the alert must name the repair: {error}"
            );
        }

        // An untyped body a build predating this slice wrote: no schema at all.
        let legacy = legacy_credential(&tenant_id(1), 3, "primary");
        let error = ProviderCredentialBody::read(&legacy).expect_err("an untyped body");
        assert_eq!(
            error,
            CredentialError::MissingField {
                reference: legacy.reference,
                field: SCHEMA_FIELD
            }
        );
        assert!(error.is_incompatible());
    }

    #[test]
    fn a_malformed_body_is_refused_as_malformed_rather_than_as_a_newer_schema() {
        let resource = credential(&tenant_id(1), 3, "primary");
        let cases = [
            with_fields(&resource, |fields| {
                set(fields, SECRET_ID_FIELD, CanonicalValue::string("res_nope"));
            }),
            with_fields(&resource, |fields| {
                set(fields, SECRET_VERSION_FIELD, CanonicalValue::integer(0));
            }),
            with_fields(&resource, |fields| {
                set(fields, SECRET_VERSION_FIELD, CanonicalValue::string("1"));
            }),
            with_fields(&resource, |fields| {
                fields.retain(|(field, _)| field != DISPLAY_NAME_FIELD);
            }),
            ResourceVersion {
                body: ResourceBody::Inline(CanonicalValue::string("primary")),
                ..resource.clone()
            },
            ResourceVersion {
                reference: ResourceRef::new(
                    ResourceKind::Alias,
                    resource_id(3),
                    ResourceVersionNumber::FIRST,
                ),
                ..resource.clone()
            },
        ];
        for case in cases {
            let error = ProviderCredentialBody::read(&case).expect_err("a malformed body");
            assert!(
                !error.is_incompatible(),
                "malformed state is repair work, not a version skew: {error}"
            );
            assert!(!error.to_string().contains(PLAINTEXT));
        }

        // Version zero names itself, so an operator can see what was written.
        let zero = with_fields(&resource, |fields| {
            set(fields, SECRET_VERSION_FIELD, CanonicalValue::integer(0));
        });
        assert_eq!(
            ProviderCredentialBody::read(&zero),
            Err(CredentialError::SecretVersion {
                reference: zero.reference,
                found: 0
            })
        );
    }

    #[test]
    fn one_secret_belongs_to_one_owner() {
        // Two tenants' credentials pointing at the same material: opaque
        // references make this invisible to everything except this rule.
        let mine = credential(&tenant_id(1), 3, "primary");
        let theirs = ProviderCredentialBody::staged(
            resource_id(13),
            SecretOwner::tenant(tenant_id(9)),
            provider_id(13),
            display_name("Borrowed"),
            secret_ref(3),
        )
        .version(slug("borrowed"));
        let error = Credentials::of(&state_of([mine.clone(), theirs.clone()]))
            .expect_err("one secret, two tenants");
        assert_eq!(
            error,
            CredentialError::SecretOwnerConflict {
                reference: theirs.reference,
                secret: secret_id(3),
                conflicting: mine.reference
            }
        );
        assert!(!error.is_incompatible());

        // A project of the *same* tenant is a different owner too: material is
        // owned exactly, not by a hierarchy.
        let inner = ProviderCredentialBody::staged(
            resource_id(14),
            SecretOwner::project(tenant_id(1), project_id(2)),
            provider_id(14),
            display_name("Inner"),
            secret_ref(3),
        )
        .version(slug("inner"));
        assert!(matches!(
            Credentials::of(&state_of([mine.clone(), inner])),
            Err(CredentialError::SecretOwnerConflict { .. })
        ));

        // Two versions of one secret, one owner, is ordinary rotation.
        let rotated = ProviderCredentialBody::staged(
            resource_id(15),
            owner(),
            provider_id(15),
            display_name("Rotated"),
            secret_ref_at(3, 2),
        )
        .version(slug("rotated"));
        let credentials =
            Credentials::of(&state_of([mine, rotated])).expect("one owner, two versions");
        assert_eq!(credentials.len(), 2);
        assert_eq!(credentials.owner_of(secret_id(3)), Some(owner()));
        assert_eq!(credentials.owner_of(secret_id(77)), None);
    }

    #[test]
    fn a_credential_reaches_only_the_providers_its_owner_can_reach() {
        let inner = project_credential(&tenant_id(1), &project_id(2), 4, "inner");

        // Its tenant's provider, and its own project's, are both reachable.
        for scope in [
            ResourceScope::Tenant(tenant_id(1)),
            ResourceScope::Project {
                tenant: tenant_id(1),
                project: project_id(2),
            },
        ] {
            Credentials::of(&state_of([inner.clone(), provider(4, scope, "openai")]))
                .expect("a provider inside the owner's reach");
        }

        // A sibling project's is not, and neither is another tenant's.
        for scope in [
            ResourceScope::Project {
                tenant: tenant_id(1),
                project: project_id(77),
            },
            ResourceScope::Tenant(tenant_id(9)),
        ] {
            let foreign = provider(4, scope, "openai");
            assert_eq!(
                Credentials::of(&state_of([inner.clone(), foreign.clone()])),
                Err(CredentialError::ForeignProvider {
                    reference: inner.reference,
                    provider: foreign.reference,
                    owner: SecretOwner::project(tenant_id(1), project_id(2))
                })
            );
        }

        // A `provider_id` naming something that is not a provider is refused.
        let impostor = alias(&tenant_id(1), 904, "impostor", &[]);
        assert_eq!(
            Credentials::of(&state_of([inner.clone(), impostor])),
            Err(CredentialError::NotAProvider {
                reference: inner.reference,
                provider: provider_id(4),
                found: ResourceKind::Alias
            })
        );

        // A provider this revision does not declare is unresolvable, not
        // unreadable: an older revision must not stop hydrating on upgrade.
        Credentials::of(&state_of([inner]))
            .expect("an absent provider row is resolved elsewhere, or not at all");
    }

    #[test]
    fn one_version_of_a_secret_is_in_service_and_it_is_not_ambiguous() {
        let active = credential_body(&tenant_id(1), 3, "primary")
            .transitioned(SecretLifecycle::Active)
            .unwrap();

        // Two rows, one exact version, two states: the material would be both in
        // service and not, depending on which row was read.
        let staged = ProviderCredentialBody::staged(
            resource_id(16),
            owner(),
            provider_id(16),
            display_name("Shared"),
            secret_ref(3),
        );
        let error = Credentials::of(&state_of([
            active.version(slug("primary")),
            staged.version(slug("shared")),
        ]))
        .expect_err("one version cannot be in two states");
        assert!(matches!(error, CredentialError::LifecycleConflict { .. }));
        assert!(!error.is_incompatible());

        // Two *versions* of one secret, both active: which material authorizes a
        // request would depend on iteration order.
        let second = ProviderCredentialBody::staged(
            resource_id(17),
            owner(),
            provider_id(17),
            display_name("Second"),
            secret_ref_at(3, 2),
        )
        .transitioned(SecretLifecycle::Active)
        .unwrap();
        assert_eq!(
            Credentials::of(&state_of([
                active.version(slug("primary")),
                second.version(slug("second")),
            ])),
            Err(CredentialError::AmbiguousActiveSecret {
                reference: ResourceRef::new(
                    ResourceKind::ProviderCredential,
                    resource_id(17),
                    ResourceVersionNumber::FIRST
                ),
                secret: secret_ref_at(3, 2),
                conflicting: ResourceRef::new(
                    ResourceKind::ProviderCredential,
                    resource_id(3),
                    ResourceVersionNumber::FIRST
                )
            })
        );

        // Rotating is not ambiguous: the new version is staged, the old serves.
        let credentials = Credentials::of(&state_of([
            active.version(slug("primary")),
            second
                .transitioned(SecretLifecycle::Disabled)
                .unwrap()
                .version(slug("second")),
        ]))
        .expect("one active version per secret");
        assert_eq!(credentials.active().count(), 1);
        assert_eq!(credentials.of_owner(owner()).count(), 2);
        assert_eq!(
            credentials
                .of_owner(SecretOwner::tenant(tenant_id(9)))
                .count(),
            0
        );
        // Only material a snapshot may unwrap is required to resolve, so
        // disabling a credential does not make a revision unpublishable.
        assert_eq!(
            credentials.required_secrets().collect::<Vec<_>>(),
            vec![(owner(), secret_ref(3))]
        );
        assert!(
            credentials
                .get(resource_id(3))
                .is_some_and(|credential| credential.slug.as_str() == "primary")
        );

        // Two credentials naming the *same* active version is not ambiguity: one
        // version's material serves either way, so it publishes, and each row is
        // required to resolve as its own owner's.
        let shared = ProviderCredentialBody::staged(
            resource_id(20),
            owner(),
            provider_id(20),
            display_name("Shared"),
            secret_ref(3),
        )
        .transitioned(SecretLifecycle::Active)
        .unwrap();
        let credentials = Credentials::of(&state_of([
            active.version(slug("primary")),
            shared.version(slug("shared")),
        ]))
        .expect("one version, named twice, is unambiguous");
        assert_eq!(credentials.active().count(), 2);
        assert_eq!(
            credentials.required_secrets().collect::<Vec<_>>(),
            vec![(owner(), secret_ref(3)), (owner(), secret_ref(3))]
        );
    }

    #[test]
    fn a_revision_is_refused_before_publication_and_again_on_hydration() {
        // Publication: `validate` reads credential bodies, so a cross-owner
        // reference never reaches storage.
        let mut leaking = state();
        leaking
            .insert(tenant(9, "globex"))
            .and_then(|state| {
                state.insert(
                    ProviderCredentialBody::staged(
                        resource_id(19),
                        SecretOwner::tenant(tenant_id(9)),
                        provider_id(19),
                        display_name("Borrowed"),
                        secret_ref(3),
                    )
                    .version(slug("borrowed")),
                )
            })
            .expect("distinct references");
        let error = leaking
            .validate()
            .expect_err("a cross-tenant secret reference must not publish");
        assert!(matches!(
            error,
            ValidationError::Credential(CredentialError::SecretOwnerConflict { .. })
        ));

        // The valid state publishes, and a project's credential inside its own
        // tenant is valid too.
        let mut valid = state();
        valid
            .insert(project_credential(
                &tenant_id(1),
                &project_id(2),
                4,
                "inner",
            ))
            .expect("a distinct reference")
            .validate()
            .expect("a project's own credential is valid desired state");
        assert_eq!(Credentials::of(&valid).unwrap().len(), 2);

        // Hydration: an untyped credential body from a build predating this slice
        // is a *compatibility* refusal that names the row, not corruption.
        let candidate = candidate(ExpectedRevision::Empty, "hydrate", state());
        let manifest = RevisionManifest::of(
            revision_id(1),
            None,
            std::time::SystemTime::UNIX_EPOCH,
            &candidate,
        )
        .expect("a valid candidate");
        let legacy = legacy_credential(&tenant_id(1), 3, "primary");
        let mut stored = DesiredState::new();
        for resource in candidate.state.resources() {
            let resource = if resource.reference.kind == ResourceKind::ProviderCredential {
                legacy.clone()
            } else {
                resource.clone()
            };
            stored.insert(resource).expect("distinct references");
        }
        for blob in candidate.state.blobs() {
            stored.declare_blob(*blob);
        }
        let error = LoadedRevision::assemble(manifest, stored)
            .expect_err("an untyped credential body must not hydrate");
        assert_eq!(
            error,
            IntegrityError::Incompatible(BodySkew::Credential(CredentialError::MissingField {
                reference: legacy.reference,
                field: SCHEMA_FIELD
            }))
        );
        assert!(error.is_incompatible());
        let IntegrityError::Incompatible(skew) = &error else {
            panic!("an incompatibility");
        };
        assert_eq!(
            skew.reference(),
            legacy.reference,
            "a refusal an operator reads names one row"
        );

        // The other half of the classification: rows this build *can* read that
        // contradict each other are not an upgrade away from working, so they
        // hydrate as invalid desired state rather than as compatibility skew. Only
        // a writer outside the gateway can produce this, because `validate` runs
        // before publication too.
        let mut contradictory = DesiredState::new();
        for resource in candidate.state.resources() {
            contradictory
                .insert(resource.clone())
                .expect("distinct references");
        }
        let borrowed = ProviderCredentialBody::staged(
            resource_id(19),
            SecretOwner::tenant(tenant_id(9)),
            provider_id(19),
            display_name("Borrowed"),
            secret_ref(3),
        )
        .version(slug("borrowed"));
        contradictory
            .insert(tenant(9, "globex"))
            .and_then(|state| state.insert(borrowed))
            .expect("distinct references");
        let manifest = RevisionManifest::of(
            revision_id(1),
            None,
            std::time::SystemTime::UNIX_EPOCH,
            &candidate,
        )
        .expect("a valid candidate");
        let error = LoadedRevision::assemble(manifest, contradictory)
            .expect_err("two owners for one secret is not readable desired state");
        assert!(
            !error.is_incompatible(),
            "a contradiction between readable rows is repair work, not an upgrade"
        );
    }

    /// Nothing a refusal prints could tell a reader anything about material —
    /// there is nothing in the domain that holds any.
    #[test]
    fn no_refusal_can_carry_material() {
        let reference = credential(&tenant_id(1), 3, "primary").reference;
        let errors = [
            CredentialError::OwnerMismatch {
                reference,
                declared: owner(),
            },
            CredentialError::SecretVersion {
                reference,
                found: 0,
            },
            CredentialError::UnknownLifecycle {
                reference,
                found: "quarantined".to_owned(),
            },
            CredentialError::SecretOwnerConflict {
                reference,
                secret: secret_id(3),
                conflicting: reference,
            },
            CredentialError::LifecycleConflict {
                reference,
                secret: secret_ref(3),
                state: SecretLifecycle::Active,
                conflicting: reference,
                conflicting_state: SecretLifecycle::Staged,
            },
            CredentialError::AmbiguousActiveSecret {
                reference,
                secret: secret_ref(3),
                conflicting: reference,
            },
        ];
        for error in errors {
            let rendered = error.to_string();
            assert!(!rendered.contains(PLAINTEXT), "{rendered}");
            assert!(!rendered.contains("sk-"), "{rendered}");
            assert!(!format!("{error:?}").contains(PLAINTEXT));
            assert_eq!(error.reference(), reference);
        }
    }
}
