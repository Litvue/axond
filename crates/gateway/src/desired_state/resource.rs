//! Resource envelopes: what a revision is made of.
//!
//! A resource version is an *envelope*, not a schema. It carries identity, the
//! scope it belongs to, its readable name, the versioned resources it depends
//! on, and a body that is either an inline canonical value or a
//! content-addressed [`BlobRef`]. What is inside the body — the catalogue,
//! tenancy, secret, pricing, and policy schemas — is deliberately not fixed
//! here: those land as later slices, and each is a body shape plus its own
//! validation, with no change to this envelope, to the canonical serializer, or
//! to #165's tables.
//!
//! The envelope is what the revision machinery needs, and it is all it needs:
//!
//! - identity that never changes ([`ResourceId`]) versus a name that may
//!   ([`Slug`]), so a rename is not a re-creation;
//! - a monotonic [`ResourceVersionNumber`], so a manifest pins the exact bytes a
//!   revision was compiled against and an older revision keeps pinning the older
//!   ones;
//! - explicit [`ResourceVersion::depends_on`] edges, so "this alias points at a
//!   credential that no longer exists in this revision" is a domain-level
//!   dangling reference rather than something only a schema-aware validator could
//!   notice;
//! - a scope, so a cross-tenant reference is detectable without reading any body.

use std::collections::BTreeSet;

use super::canonical::{Canonical, CanonicalValue, Checksum};
use super::ids::{ProjectId, ResourceId, Slug, TenantId};

/// The classes of durable resource a revision may contain.
///
/// Extended by later slices. A store never interprets a variant beyond storing
/// and returning it; the scope rules below are the only meaning the domain
/// attaches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ResourceKind {
    Deployment,
    Namespace,
    InboundGrant,
    Tenant,
    Project,
    Identity,
    Provider,
    ProviderCredential,
    CatalogModel,
    ModelEnablement,
    Price,
    Alias,
    Policy,
}

impl ResourceKind {
    /// Every kind, so exhaustiveness is testable rather than assumed.
    pub const ALL: &'static [Self] = &[
        Self::Deployment,
        Self::Namespace,
        Self::InboundGrant,
        Self::Tenant,
        Self::Project,
        Self::Identity,
        Self::Provider,
        Self::ProviderCredential,
        Self::CatalogModel,
        Self::ModelEnablement,
        Self::Price,
        Self::Alias,
        Self::Policy,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Deployment => "deployment",
            Self::Namespace => "namespace",
            Self::InboundGrant => "inbound-grant",
            Self::Tenant => "tenant",
            Self::Project => "project",
            Self::Identity => "identity",
            Self::Provider => "provider",
            Self::ProviderCredential => "provider-credential",
            Self::CatalogModel => "catalog-model",
            Self::ModelEnablement => "model-enablement",
            Self::Price => "price",
            Self::Alias => "alias",
            Self::Policy => "policy",
        }
    }

    /// Whether a kind may live at a scope.
    ///
    /// Four rules, and they are the reason scope is on the envelope rather than
    /// inside a body:
    ///
    /// - a tenant and the model catalogue are deployment-wide: the tenant *is*
    ///   the boundary, and catalogue metadata is upstream fact, not tenant state;
    /// - a project belongs to exactly one tenant;
    /// - an identity may live at *any* scope, because a platform administrator is
    ///   an identity that belongs to no tenant: its grants are over the
    ///   deployment, so scoping it into one tenant would either be a lie or make
    ///   the platform role unrepresentable. The narrower scopes are the ordinary
    ///   case — a tenant's administrator, a project's workload — and
    ///   [`Directory`](super::access::Directory) holds each role to the scopes it
    ///   may be granted at;
    /// - a price book is deployment-wide *or* tenant state: the approved baseline
    ///   a deployment charges is one shared decision (#201), while a
    ///   tenant-specific rate is that tenant's. The body schema this build reads
    ///   accepts only the deployment-scoped baseline
    ///   ([`PricingError::ScopeNotSupported`](super::pricing::PricingError)), so the
    ///   envelope permits what the model will hold and the body refuses what the
    ///   routing model cannot yet charge correctly;
    /// - everything else is tenant- or project-scoped, never deployment-wide, so
    ///   no ordinary resource can be authored outside a tenant by accident.
    pub const fn permits(self, scope: &ResourceScope) -> bool {
        match self {
            Self::Deployment
            | Self::Namespace
            | Self::InboundGrant
            | Self::Tenant
            | Self::CatalogModel => {
                matches!(scope, ResourceScope::Deployment)
            }
            Self::Price => matches!(
                scope,
                ResourceScope::Deployment
                    | ResourceScope::Tenant(_)
                    | ResourceScope::Project { .. }
            ),
            Self::Project => matches!(scope, ResourceScope::Tenant(_)),
            Self::Identity => true,
            _ => matches!(
                scope,
                ResourceScope::Tenant(_) | ResourceScope::Project { .. }
            ),
        }
    }
}

impl Canonical for ResourceKind {
    fn canonical(&self) -> CanonicalValue {
        CanonicalValue::string(self.as_str())
    }
}

/// Where a resource lives in the tenancy hierarchy.
///
/// Deployment-wide state and tenant state are different things, and the type
/// says which: a resource cannot be "sort of global".
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ResourceScope {
    /// The deployment as a whole: tenants themselves, and the model catalogue.
    Deployment,
    Tenant(TenantId),
    Project {
        tenant: TenantId,
        project: ProjectId,
    },
}

impl ResourceScope {
    /// The tenant this scope belongs to, if any.
    ///
    /// This is what cross-tenant reference validation compares, so it is one
    /// function rather than a match repeated at every call site.
    pub const fn tenant(&self) -> Option<TenantId> {
        match self {
            Self::Deployment => None,
            Self::Tenant(tenant) | Self::Project { tenant, .. } => Some(*tenant),
        }
    }

    /// Whether this scope encloses `inner`: a grant held here reaches there.
    ///
    /// Containment runs one way only. Deployment encloses every scope, a tenant
    /// encloses itself and its own projects, and a project encloses nothing but
    /// itself — a grant on one project is not a grant on its tenant, or the
    /// narrowest grant would be the widest one.
    pub fn contains(&self, inner: &Self) -> bool {
        match (self, inner) {
            (Self::Deployment, _) => true,
            (Self::Tenant(outer), Self::Tenant(tenant) | Self::Project { tenant, .. }) => {
                outer == tenant
            }
            (Self::Tenant(_) | Self::Project { .. }, Self::Deployment) => false,
            (Self::Project { .. }, Self::Tenant(_)) => false,
            (Self::Project { .. }, Self::Project { .. }) => self == inner,
        }
    }
}

/// Renders a scope the way a refusal has to name it: ids only, narrowest last.
///
/// Ids rather than slugs, because a slug is renameable and a message an operator
/// reads has to point at the row they can look up.
impl std::fmt::Display for ResourceScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Deployment => f.write_str("deployment scope"),
            Self::Tenant(tenant) => write!(f, "{tenant}"),
            Self::Project { tenant, project } => write!(f, "{tenant}/{project}"),
        }
    }
}

impl Canonical for ResourceScope {
    fn canonical(&self) -> CanonicalValue {
        match self {
            Self::Deployment => {
                CanonicalValue::map([("kind", CanonicalValue::string("deployment"))])
            }
            Self::Tenant(tenant) => CanonicalValue::map([
                ("kind", CanonicalValue::string("tenant")),
                ("tenant", CanonicalValue::string(tenant.to_string())),
            ]),
            Self::Project { tenant, project } => CanonicalValue::map([
                ("kind", CanonicalValue::string("project")),
                ("tenant", CanonicalValue::string(tenant.to_string())),
                ("project", CanonicalValue::string(project.to_string())),
            ]),
        }
    }
}

/// A resource's version counter. Starts at 1 and only ever increases, so
/// `(id, version)` names immutable content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResourceVersionNumber(u64);

impl ResourceVersionNumber {
    pub const FIRST: Self = Self(1);

    /// A version number, refusing zero: version 0 would be a resource that
    /// exists but has no content.
    pub const fn new(version: u64) -> Option<Self> {
        if version == 0 {
            None
        } else {
            Some(Self(version))
        }
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl std::fmt::Display for ResourceVersionNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v{}", self.0)
    }
}

/// A typed, versioned reference to one immutable resource version.
///
/// The ordering derive is the domain's ordering: kind, then id, then version.
/// Manifests and canonical bytes both rely on it, which is why it is derived once
/// here rather than re-specified per collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResourceRef {
    pub kind: ResourceKind,
    pub id: ResourceId,
    pub version: ResourceVersionNumber,
}

impl ResourceRef {
    pub const fn new(kind: ResourceKind, id: ResourceId, version: ResourceVersionNumber) -> Self {
        Self { kind, id, version }
    }

    /// The same resource at a different version — what an update produces.
    pub const fn at(self, version: ResourceVersionNumber) -> Self {
        Self { version, ..self }
    }

    /// Whether two references name the same resource, whatever their versions.
    pub fn same_resource(&self, other: &Self) -> bool {
        self.kind == other.kind && self.id == other.id
    }
}

impl std::fmt::Display for ResourceRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}@{}", self.kind.as_str(), self.id, self.version)
    }
}

impl Canonical for ResourceRef {
    fn canonical(&self) -> CanonicalValue {
        CanonicalValue::map([
            ("kind", self.kind.canonical()),
            ("id", CanonicalValue::string(self.id.to_string())),
            ("version", CanonicalValue::integer(self.version.get())),
        ])
    }
}

/// What a content-addressed blob holds.
///
/// Naming the classes keeps a blob self-describing: a manifest entry says "this
/// digest is a catalogue snapshot", so a mismatched or misfiled payload is
/// detectable without parsing it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BlobKind {
    /// A full upstream model-metadata snapshot (models.dev), which is large,
    /// immutable, and shared by every revision that did not change it.
    CatalogSnapshot,
    /// A published price book.
    PriceBook,
    /// A compiled policy bundle.
    PolicyBundle,
}

impl BlobKind {
    /// Every kind, so a stored spelling can be resolved back to a variant
    /// exhaustively rather than by a lookup table a new variant would leave out.
    pub const ALL: &'static [Self] = &[Self::CatalogSnapshot, Self::PriceBook, Self::PolicyBundle];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CatalogSnapshot => "catalog-snapshot",
            Self::PriceBook => "price-book",
            Self::PolicyBundle => "policy-bundle",
        }
    }
}

/// A reference to an immutable payload stored once and addressed by its digest.
///
/// This is how a manifest can pin a multi-megabyte catalogue snapshot without
/// containing it. Content addressing does the deduplication for free: a revision
/// that changes only an alias re-references the same digest, so N revisions of a
/// deployment hold one copy of the snapshot, not N. It also makes the reference
/// self-verifying — [`BlobRef::verify`] is the only way to accept a payload, so a
/// truncated or substituted blob is a typed error rather than state that hydrates
/// into a running snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobRef {
    pub kind: BlobKind,
    pub digest: Checksum,
    pub size_bytes: u64,
}

/// Why a blob payload was not accepted for its reference.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BlobError {
    #[error("blob {expected} is {actual_bytes} bytes, not the referenced {expected_bytes}")]
    Size {
        expected: Checksum,
        expected_bytes: u64,
        actual_bytes: u64,
    },
    #[error("blob payload hashes to {actual}, not the referenced {expected}")]
    Digest {
        expected: Checksum,
        actual: Checksum,
    },
}

impl BlobRef {
    /// The reference for a payload that is in hand.
    pub fn of(kind: BlobKind, payload: &[u8]) -> Self {
        Self {
            kind,
            digest: Checksum::of(payload),
            size_bytes: payload.len() as u64,
        }
    }

    /// Check a payload against this reference.
    ///
    /// Size is checked first only so the error names the cheaper discrepancy
    /// when both are wrong; either failure is a refusal, never a warning.
    pub fn verify(&self, payload: &[u8]) -> Result<(), BlobError> {
        if payload.len() as u64 != self.size_bytes {
            return Err(BlobError::Size {
                expected: self.digest,
                expected_bytes: self.size_bytes,
                actual_bytes: payload.len() as u64,
            });
        }
        let actual = Checksum::of(payload);
        if actual != self.digest {
            return Err(BlobError::Digest {
                expected: self.digest,
                actual,
            });
        }
        Ok(())
    }
}

impl std::fmt::Display for BlobRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{} ({} bytes)",
            self.kind.as_str(),
            self.digest,
            self.size_bytes
        )
    }
}

impl Canonical for BlobRef {
    fn canonical(&self) -> CanonicalValue {
        CanonicalValue::map([
            ("kind", CanonicalValue::string(self.kind.as_str())),
            (
                "digest",
                CanonicalValue::Bytes(self.digest.as_bytes().to_vec()),
            ),
            ("size_bytes", CanonicalValue::integer(self.size_bytes)),
        ])
    }
}

/// A resource version's content: inline, or out of line and content-addressed.
///
/// Small records are inline, because a manifest that referenced everything by
/// digest would need a blob fetch to answer "which aliases exist". Large
/// immutable payloads are blobs, because a manifest that inlined them would
/// duplicate megabytes per revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceBody {
    Inline(CanonicalValue),
    Blob(BlobRef),
}

impl ResourceBody {
    /// The blob this body references, if it is out of line.
    pub const fn blob(&self) -> Option<&BlobRef> {
        match self {
            Self::Inline(_) => None,
            Self::Blob(reference) => Some(reference),
        }
    }
}

impl Canonical for ResourceBody {
    fn canonical(&self) -> CanonicalValue {
        match self {
            // The discriminant participates, so an inline body can never
            // canonicalize to the same bytes as a blob reference.
            Self::Inline(value) => CanonicalValue::map([
                ("form", CanonicalValue::string("inline")),
                ("value", value.clone()),
            ]),
            Self::Blob(reference) => CanonicalValue::map([
                ("form", CanonicalValue::string("blob")),
                ("blob", reference.canonical()),
            ]),
        }
    }
}

/// One immutable version of one resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceVersion {
    pub reference: ResourceRef,
    pub scope: ResourceScope,
    /// The readable name at this version. Renaming produces a new version with a
    /// new slug and the same [`ResourceId`].
    pub slug: Slug,
    pub body: ResourceBody,
    /// The exact resource versions this one requires. A set: dependency order
    /// carries no meaning, and the same edge twice is not two edges.
    pub depends_on: BTreeSet<ResourceRef>,
}

impl ResourceVersion {
    /// A version with no dependencies — the common case for a leaf resource.
    pub fn new(
        reference: ResourceRef,
        scope: ResourceScope,
        slug: Slug,
        body: ResourceBody,
    ) -> Self {
        Self {
            reference,
            scope,
            slug,
            body,
            depends_on: BTreeSet::new(),
        }
    }

    pub fn depending_on(mut self, references: impl IntoIterator<Item = ResourceRef>) -> Self {
        self.depends_on.extend(references);
        self
    }

    /// The identity of this version's content, independent of the revision that
    /// contains it. #165 stores it per row so a hydrated resource can be checked
    /// on its own, not only as part of a whole revision.
    pub fn content_checksum(&self) -> Result<Checksum, super::canonical::CanonicalError> {
        self.checksum()
    }
}

impl Canonical for ResourceVersion {
    fn canonical(&self) -> CanonicalValue {
        CanonicalValue::map([
            ("reference", self.reference.canonical()),
            ("scope", self.scope.canonical()),
            ("slug", CanonicalValue::string(self.slug.as_str())),
            ("body", self.body.canonical()),
            (
                "depends_on",
                CanonicalValue::set(self.depends_on.iter().map(Canonical::canonical)),
            ),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::super::ids::Uuid7;
    use super::*;

    fn resource_id(seed: u64) -> ResourceId {
        ResourceId::new(Uuid7::from_parts(seed, 0, seed).unwrap())
    }

    fn tenant_id(seed: u64) -> TenantId {
        TenantId::new(Uuid7::from_parts(seed, 0, seed).unwrap())
    }

    fn reference(kind: ResourceKind, seed: u64) -> ResourceRef {
        ResourceRef::new(kind, resource_id(seed), ResourceVersionNumber::FIRST)
    }

    #[test]
    fn scope_rules_place_every_kind_exactly_where_it_belongs() {
        let tenant = tenant_id(1);
        let project = ResourceScope::Project {
            tenant,
            project: ProjectId::new(Uuid7::from_parts(2, 0, 2).unwrap()),
        };

        assert!(ResourceKind::Tenant.permits(&ResourceScope::Deployment));
        assert!(ResourceKind::Namespace.permits(&ResourceScope::Deployment));
        assert!(ResourceKind::InboundGrant.permits(&ResourceScope::Deployment));
        assert!(!ResourceKind::Namespace.permits(&ResourceScope::Tenant(tenant)));
        assert!(!ResourceKind::InboundGrant.permits(&project));
        assert!(!ResourceKind::Tenant.permits(&ResourceScope::Tenant(tenant)));
        assert!(ResourceKind::CatalogModel.permits(&ResourceScope::Deployment));
        assert!(ResourceKind::Project.permits(&ResourceScope::Tenant(tenant)));
        assert!(!ResourceKind::Project.permits(&project));
        // An identity is the one kind that lives at every scope: a platform
        // administrator belongs to no tenant, and a project's workload belongs to
        // one project. Which *roles* each may hold is the directory's rule, not
        // the envelope's.
        for scope in [
            ResourceScope::Deployment,
            ResourceScope::Tenant(tenant),
            project.clone(),
        ] {
            assert!(
                ResourceKind::Identity.permits(&scope),
                "identity at {scope}"
            );
        }

        // A price book is the one kind that is legal at every scope: an approved
        // baseline is deployment-wide, and a negotiated book belongs to whoever
        // negotiated it.
        assert!(ResourceKind::Price.permits(&ResourceScope::Deployment));
        assert!(ResourceKind::Price.permits(&ResourceScope::Tenant(tenant)));
        assert!(ResourceKind::Price.permits(&project));

        for kind in ResourceKind::ALL {
            if matches!(
                kind,
                ResourceKind::Deployment
                    | ResourceKind::Namespace
                    | ResourceKind::InboundGrant
                    | ResourceKind::Tenant
                    | ResourceKind::Project
                    | ResourceKind::CatalogModel
                    | ResourceKind::Identity
                    | ResourceKind::Price
            ) {
                continue;
            }
            assert!(
                kind.permits(&ResourceScope::Tenant(tenant)) && kind.permits(&project),
                "{} must be tenant- or project-scoped",
                kind.as_str()
            );
            assert!(
                !kind.permits(&ResourceScope::Deployment),
                "{} must not be deployment-wide",
                kind.as_str()
            );
        }
    }

    #[test]
    fn a_scope_names_its_tenant() {
        let tenant = tenant_id(1);
        assert_eq!(ResourceScope::Deployment.tenant(), None);
        assert_eq!(ResourceScope::Tenant(tenant).tenant(), Some(tenant));
        assert_eq!(
            ResourceScope::Project {
                tenant,
                project: ProjectId::new(Uuid7::from_parts(2, 0, 2).unwrap()),
            }
            .tenant(),
            Some(tenant)
        );
    }

    #[test]
    fn version_numbers_start_at_one_and_only_climb() {
        assert_eq!(ResourceVersionNumber::new(0), None);
        assert_eq!(
            ResourceVersionNumber::new(1),
            Some(ResourceVersionNumber::FIRST)
        );
        assert_eq!(ResourceVersionNumber::FIRST.next().get(), 2);
        assert!(ResourceVersionNumber::FIRST < ResourceVersionNumber::FIRST.next());
        assert_eq!(ResourceVersionNumber::FIRST.to_string(), "v1");
    }

    #[test]
    fn a_reference_names_a_version_and_a_resource() {
        let first = reference(ResourceKind::Alias, 1);
        let second = first.at(ResourceVersionNumber::FIRST.next());
        assert!(first.same_resource(&second));
        assert_ne!(first, second);
        assert!(first < second, "versions of one resource sort in order");
        assert!(!first.same_resource(&reference(ResourceKind::Alias, 2)));
        assert!(!first.same_resource(&ResourceRef {
            kind: ResourceKind::Policy,
            ..first
        }));
        assert_eq!(
            first.to_string(),
            format!("alias/{}@v1", first.id),
            "a reference is greppable in a log line"
        );
    }

    #[test]
    fn references_of_different_kinds_never_canonicalize_alike() {
        let alias = reference(ResourceKind::Alias, 1);
        let policy = ResourceRef {
            kind: ResourceKind::Policy,
            ..alias
        };
        assert_ne!(
            alias.checksum().unwrap(),
            policy.checksum().unwrap(),
            "the kind participates in the canonical form"
        );
    }

    #[test]
    fn dependency_order_does_not_change_a_resource_checksum() {
        let base = ResourceVersion::new(
            reference(ResourceKind::Alias, 1),
            ResourceScope::Tenant(tenant_id(9)),
            Slug::parse("fast").unwrap(),
            ResourceBody::Inline(CanonicalValue::map([(
                "targets",
                CanonicalValue::List(vec![CanonicalValue::string("primary")]),
            )])),
        );
        let ascending = base.clone().depending_on([
            reference(ResourceKind::ProviderCredential, 2),
            reference(ResourceKind::Provider, 3),
        ]);
        let descending = base.clone().depending_on([
            reference(ResourceKind::Provider, 3),
            reference(ResourceKind::ProviderCredential, 2),
        ]);
        assert_eq!(
            ascending.content_checksum().unwrap(),
            descending.content_checksum().unwrap()
        );
        assert_ne!(
            base.content_checksum().unwrap(),
            ascending.content_checksum().unwrap()
        );
    }

    #[test]
    fn renaming_changes_the_content_but_not_the_identity() {
        let reference = reference(ResourceKind::Alias, 1);
        let body = ResourceBody::Inline(CanonicalValue::Bool(true));
        let before = ResourceVersion::new(
            reference,
            ResourceScope::Tenant(tenant_id(9)),
            Slug::parse("fast").unwrap(),
            body.clone(),
        );
        let renamed = ResourceVersion {
            slug: Slug::parse("quick").unwrap(),
            reference: reference.at(ResourceVersionNumber::FIRST.next()),
            ..before.clone()
        };
        assert!(before.reference.same_resource(&renamed.reference));
        assert_ne!(
            before.content_checksum().unwrap(),
            renamed.content_checksum().unwrap()
        );
    }

    #[test]
    fn a_blob_reference_verifies_its_payload() {
        let payload = b"{\"models\":[]}".repeat(64);
        let reference = BlobRef::of(BlobKind::CatalogSnapshot, &payload);
        assert_eq!(reference.size_bytes, payload.len() as u64);
        reference
            .verify(&payload)
            .expect("the payload it addresses");

        // Content addressing deduplicates: the same snapshot in another revision
        // is the same reference, so it is stored once.
        assert_eq!(reference, BlobRef::of(BlobKind::CatalogSnapshot, &payload));
        // ...and the kind is part of the reference, so a payload cannot be
        // reinterpreted as another class of blob.
        assert_ne!(reference, BlobRef::of(BlobKind::PriceBook, &payload));
    }

    #[test]
    fn a_substituted_or_truncated_blob_is_refused() {
        let payload = b"catalogue".to_vec();
        let reference = BlobRef::of(BlobKind::CatalogSnapshot, &payload);

        let truncated = &payload[..payload.len() - 1];
        assert!(matches!(
            reference.verify(truncated),
            Err(BlobError::Size {
                expected_bytes: 9,
                actual_bytes: 8,
                ..
            })
        ));

        let substituted = b"catalogxes".to_vec();
        let error = reference
            .verify(&substituted[..9])
            .expect_err("same length, different bytes");
        assert!(matches!(error, BlobError::Digest { .. }));
        assert!(error.to_string().contains("hashes to"));
    }

    #[test]
    fn an_inline_body_never_canonicalizes_like_a_blob() {
        let payload = b"snapshot".to_vec();
        let blob = BlobRef::of(BlobKind::CatalogSnapshot, &payload);
        let as_blob = ResourceBody::Blob(blob);
        let as_inline = ResourceBody::Inline(blob.canonical());
        assert_ne!(
            as_blob.canonical().checksum().unwrap(),
            as_inline.canonical().checksum().unwrap()
        );
        assert_eq!(as_blob.blob(), Some(&blob));
        assert_eq!(as_inline.blob(), None);
    }
}
