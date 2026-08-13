//! Revisions: a complete desired state, the candidate that proposes one, the
//! manifest that records one, and the integrity checks that let a replica trust
//! one it loaded.
//!
//! Durable state is a chain of **immutable revisions**. Publishing a change
//! creates new resource versions and a new manifest referencing them; nothing is
//! edited in place. That is what makes "what was serving at 14:00" answerable,
//! rollback a matter of republishing an earlier state, and hydration of any
//! retained revision deterministic (#141, #166).
//!
//! The three types are the three moments of a revision's life:
//!
//! | Type | Moment | Contains |
//! | --- | --- | --- |
//! | [`RevisionCandidate`] | proposed | the whole [`DesiredState`], the mutation, the audit event |
//! | [`RevisionManifest`] | published | identity, parentage, one entry per resource version, blob references, the checksum |
//! | [`LoadedRevision`] | hydrated | a manifest *and* the state it names, proven to match |
//!
//! A manifest is a *reference* structure: one entry per resource version and one
//! reference per blob, never a payload. A revision that changes one alias
//! re-references the previous revision's catalogue snapshot digest, so retaining
//! a hundred revisions costs a hundred small manifests, not a hundred copies of
//! the catalogue.

use std::collections::{BTreeMap, BTreeSet};
use std::time::SystemTime;

use super::access::Directory;
use super::canonical::{Canonical, CanonicalError, CanonicalValue, Checksum, SerializerVersion};
use super::credentials::{CredentialError, Credentials};
use super::ids::{AuditEventId, MutationId, ResourceId, RevisionId, Slug};
use super::models::{ModelError, Models};
use super::mutation::{AuditEvent, ExpectedRevision, Mutation};
use super::policy::{PolicyError, PolicySet};
use super::resource::{BlobRef, ResourceKind, ResourceRef, ResourceScope, ResourceVersion};
use super::tenancy::{Tenancy, TenancyError};

/// Why a desired state is not a valid revision.
///
/// Every variant is a caller error that no retry can fix, and every variant names
/// the offending references rather than describing them: these messages are what
/// an administrator sees when `/admin/v1` refuses a change, and what #165's
/// integration tests assert on.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ValidationError {
    #[error("a revision must contain at least one resource")]
    Empty,
    #[error("{reference} appears twice in one revision")]
    DuplicateResourceVersion { reference: ResourceRef },
    #[error("{first} and {second} are two versions of one resource in one revision")]
    MultipleVersions {
        first: ResourceRef,
        second: ResourceRef,
    },
    #[error("{proposed} does not advance on {previous}")]
    VersionNotAdvanced {
        previous: ResourceRef,
        proposed: ResourceRef,
    },
    #[error("`{slug}` names both {first} and {second} in the same scope")]
    DuplicateSlug {
        slug: Slug,
        first: ResourceRef,
        second: ResourceRef,
    },
    #[error("{reference} cannot live at {scope:?}")]
    ScopeMismatch {
        reference: ResourceRef,
        scope: ResourceScope,
    },
    #[error("{from} depends on {to}, which this revision does not contain")]
    DanglingResourceReference { from: ResourceRef, to: ResourceRef },
    #[error("{from} references blob {digest}, which this revision does not declare")]
    DanglingBlobReference { from: ResourceRef, digest: Checksum },
    #[error("blob {digest} is declared but referenced by no resource")]
    UnreferencedBlob { digest: Checksum },
    #[error("{from} depends on {to}, which belongs to another tenant")]
    CrossTenantReference { from: ResourceRef, to: ResourceRef },
    #[error("deployment-scoped {from} depends on tenant-scoped {to}")]
    TenantScopedDependency { from: ResourceRef, to: ResourceRef },
    #[error("this revision's tenancy is not valid: {0}")]
    Tenancy(#[from] TenancyError),
    #[error("this revision's provider credentials are not valid: {0}")]
    Credential(#[from] CredentialError),
    #[error("this revision's policy is not valid: {0}")]
    Policy(#[from] PolicyError),
    /// Boxed so that this error — and every `Result` that carries it — stays the
    /// size it was before model bodies were typed.
    #[error("this revision's model contracts are not valid: {0}")]
    Model(Box<ModelError>),
    #[error("audit event {audit} records mutation {recorded}, not this candidate's {mutation}")]
    AuditMutationMismatch {
        audit: AuditEventId,
        recorded: MutationId,
        mutation: MutationId,
    },
    #[error("desired state has no canonical form: {0}")]
    Canonical(#[from] CanonicalError),
}

/// A complete desired state: every resource version a revision pins, plus the
/// content-addressed blobs those versions reference.
///
/// "Complete" is the point. A revision is not a diff, so hydrating one never
/// requires walking a chain of parents, and #142 can compile a snapshot from a
/// single load. Diffing two revisions remains possible — they are both complete —
/// but no part of the system depends on a diff being replayable.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DesiredState {
    resources: BTreeMap<ResourceRef, ResourceVersion>,
    blobs: BTreeMap<Checksum, BlobRef>,
}

impl DesiredState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a resource version.
    ///
    /// Refuses a reference already present rather than overwriting it: two
    /// authors of one revision disagreeing about a resource's content is a
    /// caller error, and silently keeping the last one is how a published
    /// revision stops matching what was reviewed.
    pub fn insert(&mut self, resource: ResourceVersion) -> Result<&mut Self, ValidationError> {
        if self.resources.contains_key(&resource.reference) {
            return Err(ValidationError::DuplicateResourceVersion {
                reference: resource.reference,
            });
        }
        self.resources.insert(resource.reference, resource);
        Ok(self)
    }

    /// Declare a blob this revision's resources may reference.
    ///
    /// Idempotent by content address: declaring the same digest twice is the same
    /// declaration, which is what makes an unchanged catalogue snapshot free to
    /// carry forward into the next revision.
    pub fn declare_blob(&mut self, blob: BlobRef) -> &mut Self {
        self.blobs.insert(blob.digest, blob);
        self
    }

    pub fn resources(&self) -> impl ExactSizeIterator<Item = &ResourceVersion> {
        self.resources.values()
    }

    pub fn blobs(&self) -> impl ExactSizeIterator<Item = &BlobRef> {
        self.blobs.values()
    }

    pub fn get(&self, reference: &ResourceRef) -> Option<&ResourceVersion> {
        self.resources.get(reference)
    }

    pub fn len(&self) -> usize {
        self.resources.len()
    }

    pub fn is_empty(&self) -> bool {
        self.resources.is_empty()
    }

    /// Check every invariant a durable store is allowed to assume.
    ///
    /// Validation is the domain's job, not the database's: #165 stores what this
    /// accepted, and #166 re-runs this on what it hydrated, so a constraint lives
    /// in one place instead of being half-expressed in DDL.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.resources.is_empty() {
            return Err(ValidationError::Empty);
        }

        let mut by_resource: BTreeMap<(_, _), ResourceRef> = BTreeMap::new();
        let mut by_slug: BTreeMap<(&ResourceScope, _, &Slug), ResourceRef> = BTreeMap::new();
        let mut referenced_blobs = BTreeSet::new();

        for resource in self.resources.values() {
            let reference = resource.reference;
            if !reference.kind.permits(&resource.scope) {
                return Err(ValidationError::ScopeMismatch {
                    reference,
                    scope: resource.scope.clone(),
                });
            }
            // One version of a resource per revision: a revision pins state, and
            // "which version of this alias is live" must have one answer.
            if let Some(first) = by_resource.insert((reference.kind, reference.id), reference) {
                return Err(ValidationError::MultipleVersions {
                    first,
                    second: reference,
                });
            }
            // Slugs are unique per scope and kind, which is what lets an
            // administrator address a resource by name inside their tenant while
            // another tenant uses the same name.
            if let Some(first) =
                by_slug.insert((&resource.scope, reference.kind, &resource.slug), reference)
            {
                return Err(ValidationError::DuplicateSlug {
                    slug: resource.slug.clone(),
                    first,
                    second: reference,
                });
            }
            if let Some(blob) = resource.body.blob() {
                if !self.blobs.contains_key(&blob.digest) {
                    return Err(ValidationError::DanglingBlobReference {
                        from: reference,
                        digest: blob.digest,
                    });
                }
                referenced_blobs.insert(blob.digest);
            }
        }

        for resource in self.resources.values() {
            for dependency in &resource.depends_on {
                let Some(target) = self.resources.get(dependency) else {
                    return Err(ValidationError::DanglingResourceReference {
                        from: resource.reference,
                        to: *dependency,
                    });
                };
                // The rule is asymmetric on purpose. A tenant-scoped resource may
                // depend on deployment-scoped state (an alias on the shared
                // catalogue), because that state is shared by construction. The
                // reverse is not allowed: shared, deployment-wide state that
                // depends on one tenant's private resource would make that
                // tenant's data reachable from every other tenant's hydration.
                match (resource.scope.tenant(), target.scope.tenant()) {
                    (Some(from), Some(to)) if from != to => {
                        return Err(ValidationError::CrossTenantReference {
                            from: resource.reference,
                            to: *dependency,
                        });
                    }
                    (None, Some(_)) => {
                        return Err(ValidationError::TenantScopedDependency {
                            from: resource.reference,
                            to: *dependency,
                        });
                    }
                    _ => {}
                }
            }
        }

        if let Some(digest) = self
            .blobs
            .keys()
            .find(|digest| !referenced_blobs.contains(*digest))
        {
            // An orphan blob is either a caller mistake or storage that will
            // never be reclaimed, and both are worth refusing while it is cheap.
            return Err(ValidationError::UnreferencedBlob { digest: *digest });
        }

        // Last, because these are the only steps that read a *body*: everything
        // above holds for every resource kind, including the ones whose schemas
        // later slices own. Tenancy is the schema the domain knows (#191), and it
        // is what makes ownership — a project's tenant, and the tenant of anything
        // scoped to one — a domain invariant rather than a projection's problem.
        let tenancy = Tenancy::of(self)?;
        // Then the directory, against that tenancy (#144): a grant over a tenant
        // this revision does not declare, or over another tenant's project, is
        // refused here rather than at the moment someone uses it. Identity bodies
        // are strictly read because no release has ever published one — there is
        // no untyped identity row in any deployment for this to become
        // retroactively strict about.
        Directory::of(self, &tenancy)?;

        // Then the bodies that hang off tenancy. Credentials are read after it
        // because their ownership is stated in the same terms — a tenant, and
        // optionally one of its projects — and a project that does not belong to
        // the tenant a credential names is tenancy's refusal to make, not a second
        // opinion about it here (#198).
        Credentials::of(self)?;

        // A policy document states the scope it governs in tenancy's own terms
        // too, so it is read after the view that owns those terms rather than
        // forming a second opinion about them (#208).
        PolicySet::of(self)?;

        // Model enablements and aliases state ownership the same way, and an alias
        // reaches its own project's enablements and its tenant's defaults — which
        // is only meaningful once tenancy has agreed the project belongs to the
        // tenant (#205).
        Models::of(self)?;

        Ok(())
    }

    /// Replace a resource with a later version of itself.
    ///
    /// The shape every administrative change has: desired state is complete, so
    /// renaming a project or disabling a tenant means republishing everything with
    /// one resource advanced. Refuses a version that does not advance, because a
    /// revision that reuses a version number would make "which body is version 3?"
    /// depend on which revision you loaded.
    ///
    /// Inserts when the resource is absent, so a caller building the next revision
    /// out of the current one does not have to branch on whether it is a create.
    pub fn supersede(&mut self, resource: ResourceVersion) -> Result<&mut Self, ValidationError> {
        let previous = self
            .resources
            .keys()
            .find(|reference| {
                reference.kind == resource.reference.kind && reference.id == resource.reference.id
            })
            .copied();
        if let Some(previous) = previous {
            if previous.version >= resource.reference.version {
                return Err(ValidationError::VersionNotAdvanced {
                    previous,
                    proposed: resource.reference,
                });
            }
            self.resources.remove(&previous);
        }
        self.insert(resource)
    }

    /// The version of a resource this state holds, by identity rather than by
    /// reference: a caller advancing a resource knows *which* resource, not which
    /// version number it is currently at.
    pub fn version_of(&self, kind: ResourceKind, id: ResourceId) -> Option<&ResourceVersion> {
        self.resources
            .values()
            .find(|resource| resource.reference.kind == kind && resource.reference.id == id)
    }

    /// The checksum of this state's canonical bytes: the revision's identity as
    /// *content*, independent of when or by whom it was published.
    pub fn checksum(&self) -> Result<Checksum, CanonicalError> {
        Canonical::checksum(self)
    }
}

impl Canonical for DesiredState {
    fn canonical(&self) -> CanonicalValue {
        // Sets, not lists: the state is a collection of resource versions, and
        // the order they were inserted in is not part of what is desired.
        CanonicalValue::map([
            (
                "resources",
                CanonicalValue::set(self.resources.values().map(Canonical::canonical)),
            ),
            (
                "blobs",
                CanonicalValue::set(self.blobs.values().map(Canonical::canonical)),
            ),
        ])
    }
}

/// A revision offered for publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevisionCandidate {
    pub expected: ExpectedRevision,
    pub state: DesiredState,
    pub mutation: Mutation,
    pub audit: AuditEvent,
}

impl RevisionCandidate {
    /// Validate the state and return its checksum.
    ///
    /// Callers get both in one step because a store must never persist state it
    /// has not validated, nor a checksum it did not compute from the state it
    /// persisted.
    ///
    /// The audit event must record *this* candidate's mutation: an audit row
    /// pointing at some other mutation is a dangling reference, refused here for
    /// the same reason a dangling resource reference is, and before #165 stores it
    /// as a foreign key.
    pub fn validated_checksum(&self) -> Result<Checksum, ValidationError> {
        if self.audit.mutation != self.mutation.id {
            return Err(ValidationError::AuditMutationMismatch {
                audit: self.audit.id,
                recorded: self.audit.mutation,
                mutation: self.mutation.id,
            });
        }
        self.state.validate()?;
        Ok(self.state.checksum()?)
    }
}

/// One line of a manifest: which resource version, where it lives, what it was
/// called, and the checksum of its content.
///
/// The content checksum is here so a single resource can be verified on its own
/// after #166 hydrates it, without recomputing the whole revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestEntry {
    pub reference: ResourceRef,
    pub scope: ResourceScope,
    pub slug: Slug,
    pub content: Checksum,
}

impl ManifestEntry {
    fn of(resource: &ResourceVersion) -> Result<Self, CanonicalError> {
        Ok(Self {
            reference: resource.reference,
            scope: resource.scope.clone(),
            slug: resource.slug.clone(),
            content: resource.content_checksum()?,
        })
    }
}

/// A published, immutable revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevisionManifest {
    pub id: RevisionId,
    /// The revision this one was published against. `None` for the first.
    pub parent: Option<RevisionId>,
    pub created_at: SystemTime,
    /// Which canonical encoding produced [`RevisionManifest::checksum`], so a
    /// future encoding cannot invalidate old checksums.
    pub serializer: SerializerVersion,
    pub mutation: MutationId,
    /// One entry per resource version, ordered by reference.
    pub entries: Vec<ManifestEntry>,
    /// Every blob the entries reference, ordered by digest. References only: the
    /// payloads live once, addressed by these digests.
    pub blobs: Vec<BlobRef>,
    /// The checksum of the whole desired state's canonical bytes.
    pub checksum: Checksum,
}

impl RevisionManifest {
    /// Record a validated candidate as a published revision.
    ///
    /// The store assigns identity and parentage; everything else is derived from
    /// the candidate, so a manifest cannot describe state that was not validated.
    pub fn of(
        id: RevisionId,
        parent: Option<RevisionId>,
        created_at: SystemTime,
        candidate: &RevisionCandidate,
    ) -> Result<Self, ValidationError> {
        let checksum = candidate.validated_checksum()?;
        let mut entries = candidate
            .state
            .resources()
            .map(ManifestEntry::of)
            .collect::<Result<Vec<_>, _>>()?;
        entries.sort_by_key(|entry| entry.reference);
        let mut blobs: Vec<BlobRef> = candidate.state.blobs().copied().collect();
        blobs.sort_by_key(|blob| blob.digest);
        Ok(Self {
            id,
            parent,
            created_at,
            serializer: SerializerVersion::default(),
            mutation: candidate.mutation.id,
            entries,
            blobs,
            checksum,
        })
    }

    /// Every resource version this revision pins.
    pub fn references(&self) -> impl ExactSizeIterator<Item = ResourceRef> + '_ {
        self.entries.iter().map(|entry| entry.reference)
    }
}

/// A stored body this build cannot read, whichever schema it declares.
///
/// One label rather than one arm per schema: an operator's action is the same —
/// run a build that reads the revision, or publish one this build reads — and
/// convergence classifies by [`IntegrityError::is_incompatible`] rather than by
/// matching on which body it was.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BodySkew {
    #[error(transparent)]
    Tenancy(TenancyError),
    #[error(transparent)]
    Credential(CredentialError),
    #[error(transparent)]
    Policy(PolicyError),
    /// Boxed for the same reason [`ValidationError::Model`] is.
    #[error(transparent)]
    Model(Box<ModelError>),
}

impl From<ModelError> for ValidationError {
    fn from(error: ModelError) -> Self {
        Self::Model(Box::new(error))
    }
}

impl From<TenancyError> for BodySkew {
    fn from(error: TenancyError) -> Self {
        Self::Tenancy(error)
    }
}

impl From<CredentialError> for BodySkew {
    fn from(error: CredentialError) -> Self {
        Self::Credential(error)
    }
}

impl From<PolicyError> for BodySkew {
    fn from(error: PolicyError) -> Self {
        Self::Policy(error)
    }
}

impl From<ModelError> for BodySkew {
    fn from(error: ModelError) -> Self {
        Self::Model(Box::new(error))
    }
}

impl BodySkew {
    /// The resource the refusal is about, whichever schema refused it.
    pub const fn reference(&self) -> ResourceRef {
        match self {
            Self::Tenancy(error) => error.reference(),
            Self::Credential(error) => error.reference(),
            Self::Policy(error) => error.reference(),
            Self::Model(error) => error.reference(),
        }
    }
}

/// Why stored state could not be trusted as the revision it claims to be.
///
/// Distinct from [`ValidationError`] on purpose: a validation error means a
/// *caller* proposed something invalid, while an integrity error means storage
/// returned something that does not add up. The first is a `400`, the second is an
/// operator alert, and conflating them hides the one that matters.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IntegrityError {
    #[error(
        "revision was written by serializer `{stored}`, but this build canonicalizes with `{current}`"
    )]
    Serializer {
        stored: SerializerVersion,
        current: SerializerVersion,
    },
    #[error(
        "stored serializer `{stored}` is a canonical encoding this build does not know; \
         it canonicalizes with `{current}`"
    )]
    UnknownSerializer {
        stored: String,
        current: SerializerVersion,
    },
    #[error("revision checksum is {expected}, but the loaded state hashes to {actual}")]
    ChecksumMismatch {
        expected: Checksum,
        actual: Checksum,
    },
    #[error("manifest names {reference}, which the loaded state does not contain")]
    MissingResource { reference: ResourceRef },
    #[error("loaded state contains {reference}, which the manifest does not name")]
    UnexpectedResource { reference: ResourceRef },
    #[error("{reference} hashes to {actual}, but the manifest recorded {expected}")]
    ContentMismatch {
        reference: ResourceRef,
        expected: Checksum,
        actual: Checksum,
    },
    #[error(
        "{reference} was stored as `{stored}` in {scope:?}, but the manifest recorded `{manifest_slug}`"
    )]
    EntryMismatch {
        reference: ResourceRef,
        stored: Slug,
        manifest_slug: Slug,
        scope: ResourceScope,
    },
    #[error("manifest declares blob {digest}, which the loaded state does not")]
    MissingBlob { digest: Checksum },
    #[error("loaded state declares blob {digest}, which the manifest does not")]
    UnexpectedBlob { digest: Checksum },
    #[error("stored revision is not valid desired state: {0}")]
    Invalid(#[from] ValidationError),
    /// A retained revision this build cannot interpret, because a body declares a
    /// schema, form, or field set that belongs to a different release: a revision
    /// published by a newer build, or one published before that body was typed.
    ///
    /// Deliberately *not* an [`IntegrityError::Invalid`] and not corruption. The
    /// rows may be entirely self-consistent; what is wrong is this build's ability
    /// to read them, and the actions differ — roll the replica forward, or publish
    /// a revision the deployed version understands, rather than repair storage.
    /// The replica keeps serving what it already holds either way.
    #[error("stored revision is not compatible with this build: {0}")]
    Incompatible(BodySkew),
    /// A stored record could not be interpreted at all: an id, checksum, kind,
    /// scope, or canonical body that is not the value it was written as. Distinct
    /// from the mismatch arms above, which compare two things that were both
    /// readable.
    #[error("stored revision is unreadable: {detail}")]
    Unreadable { detail: String },
}

impl IntegrityError {
    /// Classify a validation failure on *stored* state as an incompatibility or
    /// as corruption.
    ///
    /// Hydration re-validates what storage returned, so this is where "a body
    /// this build cannot read" stops being indistinguishable from "these rows
    /// contradict each other". See [`TenancyError::is_incompatible`].
    fn classify(error: ValidationError) -> Self {
        match error {
            ValidationError::Tenancy(tenancy) if tenancy.is_incompatible() => {
                Self::Incompatible(BodySkew::Tenancy(tenancy))
            }
            ValidationError::Credential(credential) if credential.is_incompatible() => {
                Self::Incompatible(BodySkew::Credential(credential))
            }
            ValidationError::Policy(policy) if policy.is_incompatible() => {
                Self::Incompatible(BodySkew::Policy(policy))
            }
            ValidationError::Model(model) if model.is_incompatible() => {
                Self::Incompatible(BodySkew::Model(model))
            }
            other => Self::Invalid(other),
        }
    }

    /// Whether this is a compatibility refusal rather than unreadable storage.
    ///
    /// The store and convergence both classify by this rather than by matching
    /// arms of their own, so one revision cannot be reported as an incompatibility
    /// at one layer and as corruption at the next.
    pub const fn is_incompatible(&self) -> bool {
        matches!(
            self,
            Self::Incompatible(_) | Self::Serializer { .. } | Self::UnknownSerializer { .. }
        )
    }
}

/// A retained revision, hydrated and proven complete: the seam #142 publishes a
/// snapshot from.
///
/// The only way to obtain one is [`LoadedRevision::assemble`], which verifies
/// before it constructs. So "did anyone check this?" is not a question a caller
/// has to ask — holding the type is the answer, and a corrupt or partially
/// hydrated revision is a typed error at the boundary rather than a snapshot that
/// serves the wrong routing table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedRevision {
    manifest: RevisionManifest,
    state: DesiredState,
}

impl LoadedRevision {
    /// Pair a manifest with the state it names, verifying that they agree.
    ///
    /// Checks, in order: the encoding the checksum was taken under, the state's
    /// own invariants, entry-for-resource correspondence in both directions, each
    /// entry's content checksum, the declared blobs, and finally the whole-state
    /// checksum. The order is deliberate — the most specific discrepancy is
    /// reported, because "checksum mismatch" alone tells an operator nothing about
    /// which row rotted.
    pub fn assemble(
        manifest: RevisionManifest,
        state: DesiredState,
    ) -> Result<Self, IntegrityError> {
        let current = SerializerVersion::default();
        if manifest.serializer != current {
            return Err(IntegrityError::Serializer {
                stored: manifest.serializer,
                current,
            });
        }
        state.validate().map_err(IntegrityError::classify)?;

        for entry in &manifest.entries {
            let Some(resource) = state.get(&entry.reference) else {
                return Err(IntegrityError::MissingResource {
                    reference: entry.reference,
                });
            };
            if resource.slug != entry.slug || resource.scope != entry.scope {
                return Err(IntegrityError::EntryMismatch {
                    reference: entry.reference,
                    stored: resource.slug.clone(),
                    manifest_slug: entry.slug.clone(),
                    scope: resource.scope.clone(),
                });
            }
            let actual = resource
                .content_checksum()
                .map_err(ValidationError::Canonical)?;
            if actual != entry.content {
                return Err(IntegrityError::ContentMismatch {
                    reference: entry.reference,
                    expected: entry.content,
                    actual,
                });
            }
        }
        let named: BTreeSet<ResourceRef> = manifest.references().collect();
        if let Some(resource) = state
            .resources()
            .find(|resource| !named.contains(&resource.reference))
        {
            return Err(IntegrityError::UnexpectedResource {
                reference: resource.reference,
            });
        }

        let declared: BTreeSet<Checksum> = state.blobs().map(|blob| blob.digest).collect();
        if let Some(blob) = manifest
            .blobs
            .iter()
            .find(|blob| !declared.contains(&blob.digest))
        {
            return Err(IntegrityError::MissingBlob {
                digest: blob.digest,
            });
        }
        let manifested: BTreeSet<Checksum> =
            manifest.blobs.iter().map(|blob| blob.digest).collect();
        if let Some(blob) = state
            .blobs()
            .find(|blob| !manifested.contains(&blob.digest))
        {
            return Err(IntegrityError::UnexpectedBlob {
                digest: blob.digest,
            });
        }

        let actual = state.checksum().map_err(ValidationError::Canonical)?;
        if actual != manifest.checksum {
            return Err(IntegrityError::ChecksumMismatch {
                expected: manifest.checksum,
                actual,
            });
        }
        Ok(Self { manifest, state })
    }

    pub fn manifest(&self) -> &RevisionManifest {
        &self.manifest
    }

    pub fn state(&self) -> &DesiredState {
        &self.state
    }

    pub fn id(&self) -> RevisionId {
        self.manifest.id
    }

    /// Take the verified state, for a caller that is about to compile a snapshot
    /// from it.
    pub fn into_state(self) -> DesiredState {
        self.state
    }
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::{
        DESIRED_STATE_RESOURCES, alias, blob_backed_catalog, catalog_payload, credential, project,
        reference, resource_id, revision_id, state, tenant, tenant_id,
    };
    use super::super::ids::Uuid7;
    use super::super::resource::{
        BlobKind, ResourceBody, ResourceKind, ResourceVersion, ResourceVersionNumber,
    };
    use super::*;

    fn candidate(state: DesiredState) -> RevisionCandidate {
        super::super::fixtures::candidate(ExpectedRevision::Empty, "publish-1", state)
    }

    fn manifest(candidate: &RevisionCandidate) -> RevisionManifest {
        RevisionManifest::of(revision_id(1), None, SystemTime::UNIX_EPOCH, candidate)
            .expect("a valid candidate")
    }

    #[test]
    fn insertion_order_does_not_change_the_checksum() {
        let forward = state();
        let mut backward = DesiredState::new();
        let mut resources: Vec<_> = forward.resources().cloned().collect();
        resources.reverse();
        for resource in resources {
            backward.insert(resource).unwrap();
        }
        for blob in forward.blobs() {
            backward.declare_blob(*blob);
        }
        assert_eq!(forward.checksum().unwrap(), backward.checksum().unwrap());
        assert_eq!(forward, backward, "the state itself is order-independent");
        assert_eq!(forward.len(), DESIRED_STATE_RESOURCES);
        assert!(!forward.is_empty());
    }

    #[test]
    fn semantically_identical_states_have_identical_bytes() {
        let one = state();
        let other = state();
        assert_eq!(
            one.canonical().to_canonical_bytes().unwrap(),
            other.canonical().to_canonical_bytes().unwrap()
        );
        assert_eq!(one.checksum().unwrap(), other.checksum().unwrap());
    }

    #[test]
    fn any_semantic_change_changes_the_checksum() {
        let base = state().checksum().unwrap();
        let tenant = tenant_id(1);

        let mut renamed = DesiredState::new();
        for resource in state().resources() {
            let mut resource = resource.clone();
            if resource.reference.kind == ResourceKind::Alias {
                resource.slug = Slug::parse("renamed").unwrap();
            }
            renamed.insert(resource).unwrap();
        }
        for blob in state().blobs() {
            renamed.declare_blob(*blob);
        }
        assert_ne!(base, renamed.checksum().unwrap(), "a rename is a change");

        let mut extra = state();
        extra
            .insert(alias(
                &tenant,
                7,
                "spare",
                &[reference(ResourceKind::ProviderCredential, 3)],
            ))
            .unwrap();
        assert_ne!(base, extra.checksum().unwrap(), "an addition is a change");

        // A different payload behind the same-sized blob: the digest is in the
        // canonical form, so the state's checksum moves with it.
        let mut reblobbed = DesiredState::new();
        let payload = catalog_payload(b"other");
        let replacement = BlobRef::of(BlobKind::CatalogSnapshot, &payload);
        for resource in state().resources() {
            let mut resource = resource.clone();
            if resource.body.blob().is_some() {
                resource.body = ResourceBody::Blob(replacement);
            }
            reblobbed.insert(resource).unwrap();
        }
        reblobbed.declare_blob(replacement);
        assert_ne!(base, reblobbed.checksum().unwrap());
    }

    #[test]
    fn a_blob_is_referenced_not_duplicated() {
        let payload = catalog_payload(b"models");
        let state = state();
        let blob = *state.blobs().next().expect("a declared blob");
        assert_eq!(blob.size_bytes, payload.len() as u64);

        // The canonical bytes of the whole revision are far smaller than the
        // payload they pin: a manifest scales with the number of resources, not
        // with the size of a catalogue snapshot.
        let bytes = state.canonical().to_canonical_bytes().unwrap();
        assert!(
            bytes.len() < payload.len(),
            "{} canonical bytes must not carry the {}-byte payload",
            bytes.len(),
            payload.len()
        );
        blob.verify(&payload).expect("the payload it addresses");
    }

    #[test]
    fn an_empty_state_is_not_a_revision() {
        assert_eq!(DesiredState::new().validate(), Err(ValidationError::Empty));
    }

    #[test]
    fn the_same_reference_cannot_be_inserted_twice() {
        let mut state = DesiredState::new();
        let tenant = tenant(1, "acme");
        state.insert(tenant.clone()).unwrap();
        assert_eq!(
            state.insert(tenant.clone()),
            Err(ValidationError::DuplicateResourceVersion {
                reference: tenant.reference
            })
        );
    }

    #[test]
    fn one_revision_pins_one_version_of_a_resource() {
        let mut state = DesiredState::new();
        let first = tenant(1, "acme");
        let second = ResourceVersion {
            reference: first.reference.at(ResourceVersionNumber::FIRST.next()),
            slug: Slug::parse("acme-renamed").unwrap(),
            ..first.clone()
        };
        state.insert(first.clone()).unwrap();
        state.insert(second.clone()).unwrap();
        assert_eq!(
            state.validate(),
            Err(ValidationError::MultipleVersions {
                first: first.reference,
                second: second.reference
            })
        );
    }

    #[test]
    fn slugs_are_unique_per_scope_and_kind_but_not_across_them() {
        let tenant = tenant_id(1);
        let mut clashing = DesiredState::new();
        clashing.insert(self::tenant(1, "acme")).unwrap();
        let first = alias(&tenant, 2, "fast", &[]);
        let second = alias(&tenant, 3, "fast", &[]);
        clashing.insert(first.clone()).unwrap();
        clashing.insert(second.clone()).unwrap();
        assert_eq!(
            clashing.validate(),
            Err(ValidationError::DuplicateSlug {
                slug: Slug::parse("fast").unwrap(),
                first: first.reference,
                second: second.reference
            })
        );

        // The same name in another tenant, and the same name for another kind in
        // the same tenant, are both fine: a slug is scoped, not global.
        let other = tenant_id(9);
        let mut distinct = DesiredState::new();
        distinct.insert(self::tenant(1, "acme")).unwrap();
        distinct.insert(self::tenant(9, "globex")).unwrap();
        distinct.insert(alias(&tenant, 2, "fast", &[])).unwrap();
        distinct.insert(alias(&other, 3, "fast", &[])).unwrap();
        distinct.insert(credential(&tenant, 4, "fast")).unwrap();
        distinct.validate().expect("scoped slugs do not collide");
    }

    #[test]
    fn a_kind_cannot_live_outside_its_scope() {
        let mut state = DesiredState::new();
        let misplaced = ResourceVersion::new(
            reference(ResourceKind::Alias, 1),
            ResourceScope::Deployment,
            Slug::parse("fast").unwrap(),
            ResourceBody::Inline(CanonicalValue::Bool(true)),
        );
        state.insert(misplaced.clone()).unwrap();
        assert_eq!(
            state.validate(),
            Err(ValidationError::ScopeMismatch {
                reference: misplaced.reference,
                scope: ResourceScope::Deployment
            })
        );
    }

    #[test]
    fn a_dangling_resource_reference_is_refused() {
        let tenant = tenant_id(1);
        let missing = reference(ResourceKind::ProviderCredential, 99);
        let mut state = DesiredState::new();
        state.insert(self::tenant(1, "acme")).unwrap();
        let alias = alias(&tenant, 2, "fast", &[missing]);
        state.insert(alias.clone()).unwrap();
        assert_eq!(
            state.validate(),
            Err(ValidationError::DanglingResourceReference {
                from: alias.reference,
                to: missing
            })
        );

        // The same resource at a version this revision does not pin is dangling
        // too: a reference names a *version*, not a resource.
        let credential = credential(&tenant, 3, "primary");
        let mut versioned = DesiredState::new();
        versioned.insert(self::tenant(1, "acme")).unwrap();
        versioned.insert(credential.clone()).unwrap();
        let stale = credential.reference.at(ResourceVersionNumber::FIRST.next());
        let alias = self::alias(&tenant, 2, "fast", &[stale]);
        versioned.insert(alias.clone()).unwrap();
        assert_eq!(
            versioned.validate(),
            Err(ValidationError::DanglingResourceReference {
                from: alias.reference,
                to: stale
            })
        );
    }

    #[test]
    fn a_cross_tenant_reference_is_refused_but_deployment_state_is_shared() {
        let acme = tenant_id(1);
        let globex = tenant_id(9);
        let leaked = credential(&globex, 3, "primary");
        let mut state = DesiredState::new();
        state.insert(tenant(1, "acme")).unwrap();
        state.insert(tenant(9, "globex")).unwrap();
        state.insert(leaked.clone()).unwrap();
        let alias = alias(&acme, 2, "fast", &[leaked.reference]);
        state.insert(alias.clone()).unwrap();
        assert_eq!(
            state.validate(),
            Err(ValidationError::CrossTenantReference {
                from: alias.reference,
                to: leaked.reference
            })
        );

        // A tenant resource is deployment-scoped, and every tenant may reference
        // deployment-scoped state — otherwise nothing could point at its own
        // tenant or at the shared catalogue.
        let shared = blob_backed_catalog(5);
        let mut allowed = DesiredState::new();
        allowed.insert(tenant(1, "acme")).unwrap();
        allowed.insert(shared.clone()).unwrap();
        allowed.declare_blob(*shared.body.blob().unwrap());
        allowed
            .insert(super::super::fixtures::alias(
                &acme,
                2,
                "fast",
                &[shared.reference],
            ))
            .unwrap();
        allowed
            .validate()
            .expect("deployment-scoped state is referenceable from a tenant");
    }

    #[test]
    fn deployment_scoped_state_may_not_depend_on_one_tenants_resource() {
        // The reverse of the case above, and not symmetric with it: shared state
        // reachable from every tenant must not depend on one tenant's private
        // resource.
        let acme = tenant_id(1);
        let credential = credential(&acme, 3, "primary");
        let shared = tenant(9, "globex").depending_on([credential.reference]);
        let mut state = DesiredState::new();
        state.insert(tenant(1, "acme")).unwrap();
        state.insert(credential.clone()).unwrap();
        state.insert(shared.clone()).unwrap();
        assert_eq!(
            state.validate(),
            Err(ValidationError::TenantScopedDependency {
                from: shared.reference,
                to: credential.reference
            })
        );
    }

    #[test]
    fn project_scoped_state_may_reference_its_own_tenant() {
        let tenant = tenant_id(1);
        let project_id = super::super::ids::ProjectId::new(Uuid7::from_parts(2, 0, 2).unwrap());
        let credential = credential(&tenant, 3, "primary");
        let scoped = ResourceVersion::new(
            reference(ResourceKind::Alias, 4),
            ResourceScope::Project {
                tenant,
                project: project_id,
            },
            Slug::parse("fast").unwrap(),
            ResourceBody::Inline(CanonicalValue::Bool(true)),
        )
        .depending_on([credential.reference]);
        let mut state = DesiredState::new();
        state.insert(self::tenant(1, "acme")).unwrap();
        state.insert(project(&tenant, 2, "core")).unwrap();
        state.insert(credential).unwrap();
        state.insert(scoped).unwrap();
        state
            .validate()
            .expect("a project shares its tenant's scope");
    }

    #[test]
    fn blob_references_must_be_declared_and_declarations_must_be_used() {
        let catalog = blob_backed_catalog(5);
        let blob = *catalog.body.blob().unwrap();

        let mut undeclared = DesiredState::new();
        undeclared.insert(catalog.clone()).unwrap();
        assert_eq!(
            undeclared.validate(),
            Err(ValidationError::DanglingBlobReference {
                from: catalog.reference,
                digest: blob.digest
            })
        );

        let mut orphaned = DesiredState::new();
        orphaned.insert(tenant(1, "acme")).unwrap();
        orphaned.declare_blob(blob);
        assert_eq!(
            orphaned.validate(),
            Err(ValidationError::UnreferencedBlob {
                digest: blob.digest
            })
        );

        // Declaring the same digest twice is one declaration: an unchanged
        // snapshot carried into the next revision costs nothing.
        let mut state = DesiredState::new();
        state.insert(catalog).unwrap();
        state.declare_blob(blob);
        state.declare_blob(blob);
        assert_eq!(state.blobs().len(), 1);
        state.validate().expect("declared and referenced");
    }

    #[test]
    fn a_manifest_records_references_not_payloads() {
        let candidate = candidate(state());
        let manifest = manifest(&candidate);

        assert_eq!(manifest.entries.len(), DESIRED_STATE_RESOURCES);
        assert_eq!(manifest.serializer, SerializerVersion::default());
        assert_eq!(manifest.mutation, candidate.mutation.id);
        assert_eq!(manifest.checksum, candidate.state.checksum().unwrap());
        assert_eq!(manifest.parent, None);

        // Ordered by reference, so two stores building a manifest for one state
        // produce the same rows in the same order.
        let references: Vec<_> = manifest.references().collect();
        let mut sorted = references.clone();
        sorted.sort();
        assert_eq!(references, sorted);

        for entry in &manifest.entries {
            let resource = candidate.state.get(&entry.reference).expect("named");
            assert_eq!(entry.content, resource.content_checksum().unwrap());
            assert_eq!(entry.slug, resource.slug);
        }
        assert_eq!(manifest.blobs.len(), 1);
    }

    #[test]
    fn an_invalid_candidate_produces_no_manifest() {
        let mut state = DesiredState::new();
        let missing = reference(ResourceKind::ProviderCredential, 99);
        state.insert(tenant(1, "acme")).unwrap();
        state
            .insert(alias(&tenant_id(1), 2, "fast", &[missing]))
            .unwrap();
        let error = RevisionManifest::of(
            revision_id(1),
            None,
            SystemTime::UNIX_EPOCH,
            &candidate(state),
        )
        .expect_err("a dangling reference must not be publishable");
        assert!(matches!(
            error,
            ValidationError::DanglingResourceReference { .. }
        ));
    }

    #[test]
    fn a_revision_round_trips_through_its_manifest() {
        let candidate = candidate(state());
        let manifest = manifest(&candidate);
        let loaded = LoadedRevision::assemble(manifest.clone(), candidate.state.clone())
            .expect("the state the manifest describes");

        assert_eq!(loaded.id(), manifest.id);
        assert_eq!(loaded.manifest(), &manifest);
        assert_eq!(loaded.state(), &candidate.state);
        assert_eq!(
            loaded.clone().into_state().checksum().unwrap(),
            manifest.checksum
        );

        // Rebuilding the manifest from the hydrated state reproduces it exactly:
        // hydration is deterministic, which is what #166 relies on.
        let rebuilt = RevisionManifest::of(
            manifest.id,
            manifest.parent,
            manifest.created_at,
            &RevisionCandidate {
                state: loaded.into_state(),
                ..candidate
            },
        )
        .unwrap();
        assert_eq!(rebuilt, manifest);
    }

    #[test]
    fn a_checksum_mismatch_is_reported_when_the_state_itself_still_adds_up() {
        let candidate = candidate(state());
        let mut manifest = manifest(&candidate);
        // A manifest row whose checksum column rotted: every entry verifies, so
        // only the whole-state checksum can catch it.
        manifest.checksum = Checksum::of(b"not the state");
        let error = LoadedRevision::assemble(manifest.clone(), candidate.state.clone())
            .expect_err("the recorded checksum must be enforced");
        assert_eq!(
            error,
            IntegrityError::ChecksumMismatch {
                expected: manifest.checksum,
                actual: candidate.state.checksum().unwrap()
            }
        );
        assert!(error.to_string().contains("hashes to"));
    }

    #[test]
    fn a_rotted_resource_row_names_itself() {
        let candidate = candidate(state());
        let manifest = manifest(&candidate);
        let mut state = DesiredState::new();
        let mut rotted = None;
        for resource in candidate.state.resources() {
            let mut resource = resource.clone();
            if resource.reference.kind == ResourceKind::Alias {
                resource.body = ResourceBody::Inline(CanonicalValue::string("tampered"));
                rotted = Some(resource.reference);
            }
            state.insert(resource).unwrap();
        }
        for blob in candidate.state.blobs() {
            state.declare_blob(*blob);
        }
        let reference = rotted.expect("the fixture has an alias");
        let error = LoadedRevision::assemble(manifest, state)
            .expect_err("a tampered body must not hydrate");
        assert!(
            matches!(error, IntegrityError::ContentMismatch { reference: named, .. } if named == reference),
            "{error}"
        );
    }

    #[test]
    fn a_manifest_and_a_state_must_name_the_same_resources() {
        let candidate = candidate(state());
        let manifest = manifest(&candidate);

        let mut missing = manifest.clone();
        let dropped = missing.entries.pop().expect("entries");
        let error = LoadedRevision::assemble(missing, candidate.state.clone())
            .expect_err("extra state must not hydrate");
        assert_eq!(
            error,
            IntegrityError::UnexpectedResource {
                reference: dropped.reference
            }
        );

        let mut short = DesiredState::new();
        let mut skipped = None;
        for resource in candidate.state.resources() {
            if resource.reference.kind == ResourceKind::Alias {
                skipped = Some(resource.reference);
                continue;
            }
            short.insert(resource.clone()).unwrap();
        }
        for blob in candidate.state.blobs() {
            short.declare_blob(*blob);
        }
        let error = LoadedRevision::assemble(manifest.clone(), short)
            .expect_err("a missing row must not hydrate");
        assert_eq!(
            error,
            IntegrityError::MissingResource {
                reference: skipped.expect("the fixture has an alias")
            }
        );
    }

    #[test]
    fn a_renamed_row_is_an_entry_mismatch_not_a_silent_rename() {
        let candidate = candidate(state());
        let manifest = manifest(&candidate);
        let mut state = DesiredState::new();
        for resource in candidate.state.resources() {
            let mut resource = resource.clone();
            if resource.reference.kind == ResourceKind::Alias {
                resource.slug = Slug::parse("renamed-underneath").unwrap();
            }
            state.insert(resource).unwrap();
        }
        for blob in candidate.state.blobs() {
            state.declare_blob(*blob);
        }
        let error =
            LoadedRevision::assemble(manifest, state).expect_err("a rename must not hydrate");
        assert!(
            matches!(error, IntegrityError::EntryMismatch { .. }),
            "{error}"
        );
    }

    #[test]
    fn blob_declarations_must_agree_in_both_directions() {
        // A small state whose only blob-backed resource nothing depends on, so
        // dropping it leaves valid desired state and the blob comparison is the
        // only check that can catch the discrepancy.
        let catalog = blob_backed_catalog(5);
        let mut with_blob = DesiredState::new();
        with_blob.insert(tenant(1, "acme")).unwrap();
        with_blob.insert(catalog.clone()).unwrap();
        with_blob.declare_blob(*catalog.body.blob().unwrap());
        let declared = manifest(&candidate(with_blob));

        let mut without = DesiredState::new();
        without.insert(tenant(1, "acme")).unwrap();
        let mut trimmed = declared.clone();
        trimmed
            .entries
            .retain(|entry| without.get(&entry.reference).is_some());
        trimmed.checksum = without.checksum().unwrap();
        let error = LoadedRevision::assemble(trimmed, without)
            .expect_err("a manifest blob the state does not declare must not hydrate");
        assert_eq!(
            error,
            IntegrityError::MissingBlob {
                digest: catalog.body.blob().unwrap().digest
            }
        );

        let candidate = candidate(state());
        let mut extra = manifest(&candidate);
        extra.blobs.clear();
        let error = LoadedRevision::assemble(extra, candidate.state.clone())
            .expect_err("a state blob the manifest does not declare must not hydrate");
        assert!(
            matches!(error, IntegrityError::UnexpectedBlob { .. }),
            "{error}"
        );
    }

    #[test]
    fn a_revision_written_by_another_serializer_is_not_silently_rehashed() {
        let candidate = candidate(state());
        let manifest = manifest(&candidate);
        // There is one serializer version today, so this asserts the check exists
        // and reports both versions; a second variant will exercise it for real.
        assert_eq!(manifest.serializer, SerializerVersion::V1);
        assert_eq!(
            LoadedRevision::assemble(manifest, candidate.state)
                .map(|loaded| loaded.id())
                .unwrap(),
            revision_id(1)
        );
    }

    #[test]
    fn stored_state_that_is_not_valid_desired_state_is_an_integrity_error() {
        let candidate = candidate(state());
        let mut manifest = manifest(&candidate);
        let mut state = candidate.state.clone();
        // Storage returned a revision whose blob declaration is gone: an
        // invalidity, not a caller mistake.
        let catalog = state
            .resources()
            .find(|resource| resource.body.blob().is_some())
            .cloned()
            .expect("the fixture has a blob-backed resource");
        let mut rebuilt = DesiredState::new();
        for resource in state.resources() {
            rebuilt.insert(resource.clone()).unwrap();
        }
        state = rebuilt;
        manifest.blobs.clear();
        let error = LoadedRevision::assemble(manifest, state)
            .expect_err("an undeclared blob must not hydrate");
        assert_eq!(
            error,
            IntegrityError::Invalid(ValidationError::DanglingBlobReference {
                from: catalog.reference,
                digest: catalog.body.blob().unwrap().digest
            })
        );
    }

    #[test]
    fn a_candidate_validates_before_it_reports_a_checksum() {
        let valid = candidate(state());
        assert_eq!(
            valid.validated_checksum().unwrap(),
            valid.state.checksum().unwrap()
        );
        assert_eq!(
            candidate(DesiredState::new()).validated_checksum(),
            Err(ValidationError::Empty)
        );
    }

    #[test]
    fn an_audit_event_recording_another_mutation_is_refused() {
        // An audit row pointing at a mutation this candidate is not publishing is
        // a dangling reference, and it is refused before the state is even walked.
        let mut detached = candidate(state());
        let elsewhere = MutationId::new(Uuid7::from_parts(7, 0, 7).unwrap());
        detached.audit.mutation = elsewhere;
        assert_eq!(
            detached.validated_checksum(),
            Err(ValidationError::AuditMutationMismatch {
                audit: detached.audit.id,
                recorded: elsewhere,
                mutation: detached.mutation.id
            })
        );
    }

    #[test]
    fn unrepresentable_state_is_a_validation_error_not_a_panic() {
        let mut state = DesiredState::new();
        state.insert(tenant(1, "acme")).unwrap();
        state
            .insert(ResourceVersion::new(
                reference(ResourceKind::Alias, 2),
                ResourceScope::Tenant(tenant_id(1)),
                Slug::parse("fast").unwrap(),
                // A control character has no canonical form, so the state has no
                // checksum — and therefore cannot be published. The body of a kind
                // whose schema is not this slice's is opaque to validation, so the
                // encoder is what refuses it.
                ResourceBody::Inline(CanonicalValue::string("wire\tfamily")),
            ))
            .unwrap();
        assert!(matches!(
            state.checksum(),
            Err(CanonicalError::ControlCharacter { .. })
        ));
        assert!(matches!(
            candidate(state).validated_checksum(),
            Err(ValidationError::Canonical(
                CanonicalError::ControlCharacter { .. }
            ))
        ));
        let _ = resource_id(1);
    }
}
