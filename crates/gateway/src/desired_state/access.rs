//! Principals, roles, and administrative authorization (#144).
//!
//! Tenancy says *what belongs to whom* ([`Tenancy`]). This says *who may change
//! it*, and it is the only place that decides:
//!
//! - **Identity** is a durable resource. A human is authenticated by an external
//!   OIDC provider and named by the issuer-scoped pair `(issuer, subject)`; a
//!   workload is Axond's own, named by a [`PrincipalId`] and authenticated by key
//!   material Axond mints. Both are [`IdentityBody`] rows, so an operator sees one
//!   directory rather than two half-models, and both carry their grants in the
//!   revision that declared them — which means an authorization decision is a
//!   pure function of a snapshot rather than a query.
//! - **A grant is a role at a scope**, and the scope is the resource envelope's
//!   scope rather than a field inside the body. A tenant administrator is an
//!   identity scoped to a tenant; a platform administrator is one scoped to the
//!   deployment. There is no "role with a tenant column" that could disagree with
//!   the row it lives on.
//! - **A decision is a value.** [`authorize`](Directory::authorize) returns either
//!   an [`Authorization`] — the only way to build the [`Mutation`] that carries a
//!   change — or a [`Denial`], which renders an [`AccessDenial`] for the audit
//!   trail. A denied administrative action is recorded, which no revision-carried
//!   [`AuditEvent`] could do: a refusal publishes nothing, so it would otherwise
//!   leave no trace at all.
//!
//! # What this is not
//!
//! Not request-path authorization. A chat completion is authorized by
//! [`crate::principals`] against the snapshot it captured at the start of the
//! request, and nothing here is consulted while a request is in flight — no
//! control-plane read, no directory scan (see [`Directory::authenticate_workload`],
//! which says so at the one function whose shape invites it). The two models are
//! deliberately separate: an inbound key's [`Capability`](crate::principals::Capability)
//! set says which *inference surfaces* it may call, and a [`Role`] says which
//! *administrative* surfaces a principal may change. Merging them would make
//! "can call `/v1/chat/completions`" and "can rotate a provider credential" the
//! same kind of statement.
//!
//! # Enumeration
//!
//! A [`Denial`] carries a precise [`DenialReason`] *for the audit trail* and
//! [`Denial::public_reason`] for the caller, which is the same string for every
//! reason. An administrator who is told "no such tenant" for one id and
//! "forbidden" for another has been handed a tenant-existence oracle, and tenant
//! ids are exactly what a cross-tenant attempt needs. Auditors get the detail;
//! callers get one answer.
//!
//! [`Tenancy`]: super::tenancy::Tenancy

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::time::SystemTime;

use ring::rand::{SecureRandom, SystemRandom};

use super::canonical::{Canonical, CanonicalValue, Checksum};
use super::ids::{AuditEventId, MutationId, PrincipalId, ProjectId, ResourceId, Slug, TenantId};
use super::mutation::{Actor, AuditEvent, IdempotencyKey, Mutation, MutationKind};
use super::record::{DISPLAY_NAME_FIELD, Record, SCHEMA_FIELD};
use super::resource::{
    ResourceBody, ResourceKind, ResourceRef, ResourceScope, ResourceVersion, ResourceVersionNumber,
};
use super::revision::DesiredState;
use super::tenancy::{DisplayName, Tenancy, TenancyError};
use crate::principals::constant_time_eq;

/// The identity body schema this build reads and writes.
pub const IDENTITY_SCHEMA: &str = "axond.identity.v1";

const PRINCIPAL_ID_FIELD: &str = "principal_id";
const KIND_FIELD: &str = "identity_kind";
const ROLES_FIELD: &str = "roles";
const ISSUER_FIELD: &str = "issuer";
const SUBJECT_FIELD: &str = "subject";
const KEY_DIGEST_FIELD: &str = "key_digest";

/// An administrative surface a role is granted over.
///
/// Its own closed vocabulary rather than [`ResourceKind`], for two reasons: some
/// surfaces are not resources at all (reading the audit trail, reading a bill),
/// and two resource kinds can be one surface (a catalogue model and a tenant's
/// enablement of it are both "models" to an operator). [`Surface::of`] maps the
/// kinds that do correspond, so a caller holding a [`ResourceRef`] never has to
/// guess.
///
/// Naming a surface here is not authoring its resource body: what a provider,
/// credential, price, policy, or model row *contains* belongs to the slices that
/// own them (#198, #205, #206, #208). This is the vocabulary those slices
/// authorize against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Surface {
    /// The tenants themselves: creating, renaming, disabling, deleting.
    Tenant,
    /// Projects within a tenant — namespaces, in the running gateway's terms.
    Project,
    /// The identity directory: humans, workloads, and their grants.
    Principal,
    /// Provider connections.
    Provider,
    /// Provider credentials and their rotation.
    Credential,
    /// The model catalogue and a tenant's enablement of it.
    Model,
    /// Prices and price books.
    Price,
    /// Aliases: the names a tenant's callers ask for.
    Alias,
    /// Routing, budget, and rate-limit policy.
    Policy,
    /// The audit trail itself. Readable, never writable — by any role, the
    /// platform administrator included, which is why [`Role::actions`] answers
    /// this surface before it consults the role. An administrator who can delete
    /// audit events can delete the record of their own, so altering one stays an
    /// operation for whoever can reach the database, bounded by retention and
    /// backups rather than by a grant.
    AuditTrail,
    /// Usage and spend. Separated from [`Surface::Price`] because reading what a
    /// tenant was charged and deciding what it is charged are different jobs.
    Billing,
}

impl Surface {
    /// Every surface, so an authorization matrix can be asserted exhaustively
    /// rather than sampled.
    pub const ALL: &'static [Self] = &[
        Self::Tenant,
        Self::Project,
        Self::Principal,
        Self::Provider,
        Self::Credential,
        Self::Model,
        Self::Price,
        Self::Alias,
        Self::Policy,
        Self::AuditTrail,
        Self::Billing,
    ];

    /// Read a stored surface back, through the names [`Surface::as_str`] writes.
    pub fn parse(text: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|surface| surface.as_str() == text)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Tenant => "tenant",
            Self::Project => "project",
            Self::Principal => "principal",
            Self::Provider => "provider",
            Self::Credential => "credential",
            Self::Model => "model",
            Self::Price => "price",
            Self::Alias => "alias",
            Self::Policy => "policy",
            Self::AuditTrail => "audit-trail",
            Self::Billing => "billing",
        }
    }

    /// The surface a resource kind is administered through.
    pub const fn of(kind: ResourceKind) -> Self {
        match kind {
            ResourceKind::Tenant => Self::Tenant,
            ResourceKind::Project => Self::Project,
            ResourceKind::Identity => Self::Principal,
            ResourceKind::Provider => Self::Provider,
            ResourceKind::ProviderCredential => Self::Credential,
            ResourceKind::CatalogModel | ResourceKind::ModelEnablement => Self::Model,
            ResourceKind::Price => Self::Price,
            ResourceKind::Alias => Self::Alias,
            ResourceKind::Policy => Self::Policy,
        }
    }
}

impl fmt::Display for Surface {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What is being attempted on a surface.
///
/// [`Action::Rotate`] is not an update: replacing key material is the operation a
/// deployment wants to grant to whoever runs it without also granting "change
/// which provider this points at", and an audit trail wants it spelled out.
/// [`MutationKind`] carries the same distinction into the revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Action {
    Read,
    Create,
    Update,
    Delete,
    Rotate,
}

impl Action {
    /// Every action, so the matrix is testable over the full cross product.
    pub const ALL: &'static [Self] = &[
        Self::Read,
        Self::Create,
        Self::Update,
        Self::Delete,
        Self::Rotate,
    ];

    /// Read a stored action back, through the names [`Action::as_str`] writes.
    pub fn parse(text: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|action| action.as_str() == text)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Create => "create",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::Rotate => "rotate",
        }
    }

    /// Whether this action changes durable state, and therefore has to be
    /// carried by a mutation and an audit event.
    pub const fn is_write(self) -> bool {
        !matches!(self, Self::Read)
    }

    /// The mutation verb this action publishes as.
    ///
    /// `None` for [`Action::Read`], which publishes nothing:
    /// [`MutationKind::Rollback`] has no action of its own because rolling back
    /// is authorized as an update of whatever it restores.
    pub const fn mutation_kind(self) -> Option<MutationKind> {
        match self {
            Self::Read => None,
            Self::Create => Some(MutationKind::Create),
            Self::Update => Some(MutationKind::Update),
            Self::Delete => Some(MutationKind::Delete),
            Self::Rotate => Some(MutationKind::Rotate),
        }
    }
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A role: a named set of (surface, action) permissions, grantable at some scopes
/// and not others.
///
/// Five roles, closed. An operator cannot define a sixth, and that is the point:
/// a deployment with per-tenant custom roles has an authorization model nobody can
/// review, and #144 asks for a matrix that can be stated and tested. New roles
/// arrive as code, in a release, with the matrix test updated in the same commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Role {
    /// Runs the deployment. Every surface, every action, deployment-wide —
    /// including creating and deleting tenants, which no tenant-scoped role can
    /// do. One exception, and it is the deliberate one: writing the audit trail,
    /// because a trail its administrator can edit does not record administrators.
    PlatformAdmin,
    /// Runs one tenant. Everything inside it, except creating or deleting the
    /// tenant itself: a tenant that could delete itself could also delete the
    /// billing boundary it is billed through.
    TenantAdmin,
    /// Day-2 operations: connections, credentials, rotation, aliases, and the
    /// models on offer. Deliberately *not* the identity directory — an operator
    /// who can grant roles can grant themselves one — and not policy, which is
    /// where budgets live.
    Operator,
    /// Reads what was spent and what it costs, and nothing else. Not the audit
    /// trail: finance does not need to know which administrator rotated a key.
    BillingViewer,
    /// Builds against the gateway: reads the models and prices on offer, and owns
    /// the aliases its own code calls. No credentials, in any form — a developer
    /// who can read a credential row does not need it rotated to use it.
    Developer,
}

impl Role {
    /// Every role, so the matrix and its tests cover all of them.
    pub const ALL: &'static [Self] = &[
        Self::PlatformAdmin,
        Self::TenantAdmin,
        Self::Operator,
        Self::BillingViewer,
        Self::Developer,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PlatformAdmin => "platform-admin",
            Self::TenantAdmin => "tenant-admin",
            Self::Operator => "operator",
            Self::BillingViewer => "billing-viewer",
            Self::Developer => "developer",
        }
    }

    /// Resolve a stored spelling, exhaustively over [`Role::ALL`].
    pub fn parse(text: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|role| role.as_str() == text)
    }

    /// Whether this role may be granted at a scope.
    ///
    /// Scope is where the *grant* lives, not where the request goes. Three rules:
    ///
    /// - [`Role::PlatformAdmin`] is deployment-only. A platform administrator
    ///   scoped into a tenant would be a tenant administrator with a misleading
    ///   name;
    /// - [`Role::TenantAdmin`] is tenant-only. Administering one project of a
    ///   tenant is what [`Role::Operator`] and [`Role::Developer`] are for, and a
    ///   project-scoped tenant administrator could create projects beside its own;
    /// - the remaining roles are tenant- or project-scoped, never deployment-wide:
    ///   a deployment-wide operator or developer is a platform administrator
    ///   assembled out of narrower parts.
    pub const fn permits_scope(self, scope: &ResourceScope) -> bool {
        match self {
            Self::PlatformAdmin => matches!(scope, ResourceScope::Deployment),
            Self::TenantAdmin => matches!(scope, ResourceScope::Tenant(_)),
            Self::Operator | Self::BillingViewer | Self::Developer => matches!(
                scope,
                ResourceScope::Tenant(_) | ResourceScope::Project { .. }
            ),
        }
    }

    /// The actions this role holds on a surface — the authorization matrix, in
    /// one place, written out rather than derived from a hierarchy.
    ///
    /// A hierarchy ("tenant admin ⊇ operator ⊇ developer") reads well and hides
    /// exactly the questions a review asks: it makes every widening of a narrow
    /// role a silent widening of every role above it, and it cannot express
    /// [`Role::BillingViewer`], which is not a subset of anything. Written out,
    /// the matrix is greppable and every cell is a decision.
    pub const fn actions(self, surface: Surface) -> &'static [Action] {
        const NONE: &[Action] = &[];
        const READ: &[Action] = &[Action::Read];
        const MANAGE: &[Action] = &[Action::Read, Action::Create, Action::Update, Action::Delete];
        const OPERATE: &[Action] = &[
            Action::Read,
            Action::Create,
            Action::Update,
            Action::Delete,
            Action::Rotate,
        ];
        match (self, surface) {
            // The audit trail is read-only for every role, before any role's own
            // row is consulted — including the platform administrator's. An
            // administrator who can delete audit events can delete the evidence
            // of their own actions, which is the one power a trail exists to
            // deny. Altering one stays a database operation, defended by
            // retention and backups rather than by a grant.
            (Self::BillingViewer | Self::Developer, Surface::AuditTrail) => NONE,
            (_, Surface::AuditTrail) => READ,
            // Runs the deployment: no other cell is narrowed, because the role
            // exists to be the one that is not.
            (Self::PlatformAdmin, _) => Action::ALL,
            // Its own tenant is readable and renameable; its existence is not
            // its own to decide.
            (Self::TenantAdmin, Surface::Tenant) => &[Action::Read, Action::Update],
            (Self::TenantAdmin, Surface::Provider | Surface::Credential) => OPERATE,
            (Self::TenantAdmin, Surface::Billing) => READ,
            (Self::TenantAdmin, _) => MANAGE,
            (Self::Operator, Surface::Provider | Surface::Credential) => OPERATE,
            (Self::Operator, Surface::Model | Surface::Alias) => MANAGE,
            (
                Self::Operator,
                Surface::Tenant
                | Surface::Project
                | Surface::Principal
                | Surface::Price
                | Surface::Policy
                | Surface::Billing,
            ) => READ,
            (
                Self::BillingViewer,
                Surface::Tenant | Surface::Project | Surface::Price | Surface::Billing,
            ) => READ,
            (Self::BillingViewer, _) => NONE,
            (Self::Developer, Surface::Alias) => MANAGE,
            (Self::Developer, Surface::Project | Surface::Model | Surface::Price) => READ,
            (Self::Developer, _) => NONE,
        }
    }

    /// Whether this role holds an action on a surface.
    pub fn permits(self, surface: Surface, action: Action) -> bool {
        self.actions(surface).contains(&action)
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether a principal is a person or a program.
///
/// The distinction is about *who issues the credential*, which is why it is typed
/// rather than conventional: a human is authenticated by an external OIDC
/// provider that Axond does not control, and a workload by key material Axond
/// mints and can revoke. Storing an issuer for a workload, or key material for a
/// human, would put Axond in the business of being an identity provider for
/// people — the thing #144 explicitly does not do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IdentityKind {
    /// A person, authenticated elsewhere.
    Human,
    /// A service account, authenticated by an Axond-minted key.
    Workload,
}

impl IdentityKind {
    pub const ALL: &'static [Self] = &[Self::Human, Self::Workload];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Human => "human",
            Self::Workload => "workload",
        }
    }
}

impl fmt::Display for IdentityKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How a principal proves who it is.
///
/// One field set per kind, in one enum, so a body cannot be half a human: there
/// is no representable identity with an issuer *and* a key digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Credential {
    /// An issuer-scoped OIDC subject. A subject is unique only within its issuer,
    /// so the pair is the identity and neither half is.
    Oidc { issuer: String, subject: String },
    /// The SHA-256 digest of an Axond-minted key, or `None` for a workload whose
    /// key has been revoked and not replaced — which is a workload that can hold
    /// grants and authenticate with nothing, and is how revocation is spelled
    /// without deleting the principal an audit trail refers to.
    ///
    /// A digest, never the key: verification needs only a comparison, so storing
    /// anything reversible would be storing a secret for no purpose. What
    /// [`WorkloadKey::generate`] returns is shown once and then unrecoverable.
    MintedKey { digest: Option<Checksum> },
}

impl Credential {
    pub const fn kind(&self) -> IdentityKind {
        match self {
            Self::Oidc { .. } => IdentityKind::Human,
            Self::MintedKey { .. } => IdentityKind::Workload,
        }
    }
}

/// Key material for a workload principal, held so it is hard to leak.
///
/// No `Clone`, no `Display`, no `Serialize`, and a [`fmt::Debug`] that renders a
/// placeholder: the only way to get the string out is [`WorkloadKey::expose_once`],
/// which consumes the value. The type is therefore the one-time display rule
/// rather than a comment asking callers to observe it — a handler that wants to
/// log a key has to visibly destroy its ability to return it.
pub struct WorkloadKey(String);

impl WorkloadKey {
    /// The prefix a minted workload key carries, so a leaked string is
    /// recognisable in a scanner and cannot be confused with an inbound gateway
    /// key or a minted request token.
    pub const PREFIX: &'static str = "axw1.";

    /// 256 bits, hex-encoded. Long enough that guessing is not a threat model,
    /// and hex rather than base64 so the alphabet holds no character that needs
    /// escaping in a URL, a shell, or a log line.
    const ENTROPY_BYTES: usize = 32;

    /// Mint a new key from the system CSPRNG.
    ///
    /// Fails only if the operating system's randomness is unavailable, which is
    /// not something to paper over with a fallback: a workload key from a
    /// degraded source is worse than no workload key.
    pub fn generate() -> Result<Self, KeyError> {
        let mut bytes = [0u8; Self::ENTROPY_BYTES];
        SystemRandom::new()
            .fill(&mut bytes)
            .map_err(|_| KeyError::Randomness)?;
        let mut text = String::with_capacity(Self::PREFIX.len() + Self::ENTROPY_BYTES * 2);
        text.push_str(Self::PREFIX);
        for byte in bytes {
            text.push_str(&format!("{byte:02x}"));
        }
        Ok(Self(text))
    }

    /// Accept a presented key, checking only its shape.
    ///
    /// Shape is not authentication: this refuses obvious non-keys before any
    /// digest is computed, and [`Directory::authenticate_workload`] is what
    /// decides whose key it is.
    pub fn parse(text: &str) -> Result<Self, KeyError> {
        let digits = text.strip_prefix(Self::PREFIX).ok_or(KeyError::Prefix)?;
        if digits.len() != Self::ENTROPY_BYTES * 2
            || !digits
                .bytes()
                .all(|digit| digit.is_ascii_digit() || (b'a'..=b'f').contains(&digit))
        {
            return Err(KeyError::Shape);
        }
        Ok(Self(text.to_owned()))
    }

    /// The digest that is stored and compared.
    pub fn digest(&self) -> Checksum {
        Checksum::of(self.0.as_bytes())
    }

    /// Surrender the key material, consuming the value.
    ///
    /// The one place a key becomes an ordinary `String`. Called by the handler
    /// that returns a freshly minted key to the administrator who created the
    /// principal, and nowhere else: there is no second read, because nothing
    /// stores the material to read.
    pub fn expose_once(self) -> String {
        self.0
    }

    /// Whether a presented key matches a stored digest, in constant time.
    ///
    /// Constant time because the comparison is over an attacker-supplied value
    /// against a secret-derived one; the digests are equal-length, so this is a
    /// straight `verify_slices_are_equal` rather than a length-leaking `==`.
    pub fn verifies(digest: &Checksum, presented: &str) -> bool {
        let Ok(key) = Self::parse(presented) else {
            return false;
        };
        constant_time_eq(digest.as_bytes(), key.digest().as_bytes())
    }
}

/// Renders a placeholder, never the material: a key reaches a log line only if a
/// caller consumed it with [`WorkloadKey::expose_once`] and logged the result on
/// purpose.
impl fmt::Debug for WorkloadKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WorkloadKey(<redacted>)")
    }
}

/// Why key material was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum KeyError {
    #[error("a workload key must start with `{prefix}`", prefix = WorkloadKey::PREFIX)]
    Prefix,
    #[error("a workload key must be 64 lowercase hex digits")]
    Shape,
    #[error("the system random number generator is unavailable")]
    Randomness,
}

/// A principal: a human or a workload, its grants, and how it authenticates.
///
/// Scope is *not* here — it is on the [`ResourceVersion`] envelope, exactly as it
/// is for every other resource, so a grant cannot claim a tenant the row does not
/// belong to. [`Principal`] is the pair of the two.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityBody {
    principal: PrincipalId,
    display_name: DisplayName,
    credential: Credential,
    roles: BTreeSet<Role>,
}

impl IdentityBody {
    /// The schema identifier this body encodes and reads.
    pub const SCHEMA: &'static str = IDENTITY_SCHEMA;

    const KNOWN_FIELDS: &'static [&'static str] = &[
        PRINCIPAL_ID_FIELD,
        DISPLAY_NAME_FIELD,
        KIND_FIELD,
        ROLES_FIELD,
        ISSUER_FIELD,
        SUBJECT_FIELD,
        KEY_DIGEST_FIELD,
    ];

    /// Build an identity, refusing one with no grant.
    ///
    /// A principal with an empty role set can authenticate and do nothing, which
    /// is not a state an administrator ever means to create; it is what a
    /// half-applied grant update looks like. Deleting the principal, or revoking
    /// its key, is how "this identity is finished" is spelled.
    pub fn new(
        principal: PrincipalId,
        display_name: DisplayName,
        credential: Credential,
        roles: impl IntoIterator<Item = Role>,
    ) -> Result<Self, IdentityError> {
        let roles: BTreeSet<Role> = roles.into_iter().collect();
        if roles.is_empty() {
            return Err(IdentityError::NoRoles);
        }
        Ok(Self {
            principal,
            display_name,
            credential,
            roles,
        })
    }

    pub const fn principal(&self) -> PrincipalId {
        self.principal
    }

    pub const fn display_name(&self) -> &DisplayName {
        &self.display_name
    }

    pub const fn credential(&self) -> &Credential {
        &self.credential
    }

    pub const fn kind(&self) -> IdentityKind {
        self.credential.kind()
    }

    /// The roles granted, ordered by [`Role`] declaration order.
    pub fn roles(&self) -> impl ExactSizeIterator<Item = Role> + '_ {
        self.roles.iter().copied()
    }

    /// The same identity with different key material — a rotation, or a
    /// revocation when `digest` is `None`.
    ///
    /// A no-op for a human: Axond does not hold a person's credential, so there
    /// is nothing here to rotate, and a caller asking to is asking the wrong
    /// system. `Err` rather than a silent ignore, because "rotated" is what the
    /// audit trail would otherwise claim.
    pub fn with_key_digest(self, digest: Option<Checksum>) -> Result<Self, IdentityError> {
        match self.credential {
            Credential::MintedKey { .. } => Ok(Self {
                credential: Credential::MintedKey { digest },
                ..self
            }),
            Credential::Oidc { .. } => Err(IdentityError::NotAWorkload {
                principal: self.principal,
            }),
        }
    }

    /// The same identity with a different grant set.
    pub fn with_roles(self, roles: impl IntoIterator<Item = Role>) -> Result<Self, IdentityError> {
        let roles: BTreeSet<Role> = roles.into_iter().collect();
        if roles.is_empty() {
            return Err(IdentityError::NoRoles);
        }
        Ok(Self { roles, ..self })
    }

    /// The resource identity this principal's versions are written under.
    pub const fn resource_id(&self) -> ResourceId {
        ResourceId::new(self.principal.uuid())
    }

    /// This body as a resource body.
    pub fn body(&self) -> ResourceBody {
        ResourceBody::Inline(self.canonical())
    }

    /// The first version of this identity at `scope`, named `slug`.
    pub fn version(&self, scope: ResourceScope, slug: Slug) -> ResourceVersion {
        self.version_at(scope, slug, ResourceVersionNumber::FIRST)
    }

    /// A specific version, for a rename, a re-grant, or a rotation.
    pub fn version_at(
        &self,
        scope: ResourceScope,
        slug: Slug,
        version: ResourceVersionNumber,
    ) -> ResourceVersion {
        ResourceVersion::new(
            ResourceRef::new(ResourceKind::Identity, self.resource_id(), version),
            scope,
            slug,
            self.body(),
        )
    }

    /// Read an identity resource's body, binding it to its envelope.
    ///
    /// Every refusal names the resource, and the field set is checked against the
    /// *kind*: an issuer on a workload or a key digest on a human is a refusal
    /// rather than an ignored field, because a body nobody reads is a body
    /// nobody notices is wrong.
    pub fn read(resource: &ResourceVersion) -> Result<Self, TenancyError> {
        let record = Record::<TenancyError>::open(
            resource,
            ResourceKind::Identity,
            Self::SCHEMA,
            Self::KNOWN_FIELDS,
        )?;
        let reference = record.reference();
        let principal =
            PrincipalId::parse(record.string(PRINCIPAL_ID_FIELD)?).map_err(|source| {
                TenancyError::MalformedId {
                    reference,
                    field: PRINCIPAL_ID_FIELD,
                    source,
                }
            })?;
        record.identity(principal, ResourceId::new(principal.uuid()))?;

        let declared = record.string(KIND_FIELD)?;
        let kind = IdentityKind::ALL
            .iter()
            .copied()
            .find(|kind| kind.as_str() == declared)
            .ok_or_else(|| TenancyError::UnknownVocabulary {
                reference,
                vocabulary: "identity kind",
                value: declared.to_owned(),
            })?;

        let credential = match kind {
            IdentityKind::Human => {
                Self::refuse_field(&record, KEY_DIGEST_FIELD, kind)?;
                Credential::Oidc {
                    issuer: record.string(ISSUER_FIELD)?.to_owned(),
                    subject: record.string(SUBJECT_FIELD)?.to_owned(),
                }
            }
            IdentityKind::Workload => {
                Self::refuse_field(&record, ISSUER_FIELD, kind)?;
                Self::refuse_field(&record, SUBJECT_FIELD, kind)?;
                Credential::MintedKey {
                    digest: record.optional_checksum(KEY_DIGEST_FIELD)?,
                }
            }
        };

        let mut roles = BTreeSet::new();
        for spelling in record.string_set(ROLES_FIELD)? {
            let role = Role::parse(spelling).ok_or_else(|| TenancyError::UnknownVocabulary {
                reference,
                vocabulary: "role",
                value: spelling.to_owned(),
            })?;
            roles.insert(role);
        }
        if roles.is_empty() {
            return Err(TenancyError::NoRoles { reference });
        }

        Ok(Self {
            principal,
            display_name: record.display_name()?,
            credential,
            roles,
        })
    }

    fn refuse_field(
        record: &Record<'_, TenancyError>,
        field: &'static str,
        kind: IdentityKind,
    ) -> Result<(), TenancyError> {
        if record.optional_string(field)?.is_some() {
            return Err(TenancyError::FieldNotForKind {
                reference: record.reference(),
                field,
                kind: kind.as_str(),
            });
        }
        Ok(())
    }
}

impl Canonical for IdentityBody {
    fn canonical(&self) -> CanonicalValue {
        let mut fields = vec![
            (
                SCHEMA_FIELD.to_owned(),
                CanonicalValue::string(Self::SCHEMA),
            ),
            (
                PRINCIPAL_ID_FIELD.to_owned(),
                CanonicalValue::string(self.principal.to_string()),
            ),
            (
                DISPLAY_NAME_FIELD.to_owned(),
                CanonicalValue::string(self.display_name.as_str()),
            ),
            (
                KIND_FIELD.to_owned(),
                CanonicalValue::string(self.kind().as_str()),
            ),
            (ROLES_FIELD.to_owned(), role_set(&self.roles)),
        ];
        match &self.credential {
            Credential::Oidc { issuer, subject } => {
                fields.push((
                    ISSUER_FIELD.to_owned(),
                    CanonicalValue::string(issuer.clone()),
                ));
                fields.push((
                    SUBJECT_FIELD.to_owned(),
                    CanonicalValue::string(subject.clone()),
                ));
            }
            Credential::MintedKey { digest } => {
                if let Some(digest) = digest {
                    fields.push((
                        KEY_DIGEST_FIELD.to_owned(),
                        CanonicalValue::string(digest.to_string()),
                    ));
                }
            }
        }
        CanonicalValue::map(fields)
    }
}

/// A role set in the order the canonical encoder writes it, so a body compares
/// equal to its own round trip rather than merely hashing the same.
fn role_set(roles: &BTreeSet<Role>) -> CanonicalValue {
    let mut members: Vec<&'static str> = roles.iter().map(|role| role.as_str()).collect();
    // The encoder's order for strings — length first, then bytes.
    members.sort_unstable_by_key(|role| (role.len(), *role));
    CanonicalValue::set(members.into_iter().map(CanonicalValue::string))
}

/// Why an identity could not be built or changed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdentityError {
    #[error("an identity must grant at least one role")]
    NoRoles,
    #[error("{principal} is a human identity, whose credential Axond does not hold")]
    NotAWorkload { principal: PrincipalId },
}

/// A principal as a revision holds it: its envelope, its scope, its name, and its
/// body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub reference: ResourceRef,
    /// Where this principal's grants apply. The envelope's scope, so it cannot
    /// disagree with the row.
    pub scope: ResourceScope,
    pub slug: Slug,
    pub body: IdentityBody,
}

impl Principal {
    /// The tenant this principal belongs to, or `None` for a platform identity.
    pub const fn tenant(&self) -> Option<TenantId> {
        self.scope.tenant()
    }

    /// How an audit trail attributes this principal's changes.
    ///
    /// `None` only for a deployment-scoped workload, which [`Directory::of`]
    /// refuses: a workload's audit attribution *is* its tenant, so a hand-built
    /// one that has none is unattributable rather than attributable to nothing.
    pub fn actor(&self) -> Option<Actor> {
        match self.body.credential() {
            Credential::Oidc { issuer, subject } => Some(Actor::Human {
                issuer: issuer.clone(),
                subject: subject.clone(),
            }),
            Credential::MintedKey { .. } => Some(Actor::Workload {
                tenant: self.scope.tenant()?,
                principal: self.body.principal(),
            }),
        }
    }
}

/// The identity directory of one revision, resolved once.
///
/// Built alongside [`Tenancy`] and validated against it: a principal's scope has
/// to name a tenant and project the same revision declares, each of its roles has
/// to be grantable at that scope, and no two principals may be the same person.
/// Ordering is by [`PrincipalId`], so two replicas iterate identically.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Directory {
    principals: BTreeMap<PrincipalId, Principal>,
    /// `(issuer, subject)` to principal, so a sign-in resolves without a scan and
    /// two rows for one person are refused at resolution time.
    humans: BTreeMap<(String, String), PrincipalId>,
    /// Key digest to workload, for the same reason: one key authenticates at most
    /// one identity, so which roles it carries is a declaration rather than a
    /// consequence of id ordering.
    keys: BTreeMap<Checksum, PrincipalId>,
}

impl Directory {
    /// Read and resolve the identity directory of a desired state.
    ///
    /// Takes the [`Tenancy`] rather than rebuilding it, because the two views have
    /// to agree: a principal scoped to a tenant this revision does not declare is
    /// a grant over nothing, and a principal scoped to a project of another tenant
    /// is a cross-tenant grant — the confused-deputy shape #144 asks to reject in
    /// the service layer as well as in SQL.
    ///
    /// Five refusals, each about one row:
    ///
    /// 1. a body this build does not read, or one bound to the wrong envelope;
    /// 2. a workload at deployment scope: a workload belongs to a tenant, and one
    ///    that belonged to the deployment would be an unattributable service
    ///    account with platform reach;
    /// 3. a scope naming a tenant or project the revision does not declare;
    /// 4. a role granted at a scope it cannot be granted at;
    /// 5. two principals for one `(issuer, subject)`, which would make "what may
    ///    this person do?" depend on which row was read first.
    pub fn of(state: &DesiredState, tenancy: &Tenancy) -> Result<Self, TenancyError> {
        let mut directory = Self::default();
        for resource in state.resources() {
            if resource.reference.kind != ResourceKind::Identity {
                continue;
            }
            let body = IdentityBody::read(resource)?;
            let reference = resource.reference;
            let principal = Principal {
                reference,
                scope: resource.scope.clone(),
                slug: resource.slug.clone(),
                body,
            };

            if principal.body.kind() == IdentityKind::Workload
                && matches!(principal.scope, ResourceScope::Deployment)
            {
                return Err(TenancyError::IdentityScope {
                    reference,
                    kind: IdentityKind::Workload.as_str(),
                    scope: principal.scope.to_string(),
                });
            }
            if let Some(tenant) = principal.tenant()
                && tenancy.tenant(tenant).is_none()
            {
                return Err(TenancyError::UnknownTenant { reference, tenant });
            }
            if let ResourceScope::Project { project, .. } = &principal.scope
                && tenancy.project(*project).is_none()
            {
                return Err(TenancyError::UnknownProject {
                    reference,
                    project: *project,
                });
            }
            for role in principal.body.roles() {
                if !role.permits_scope(&principal.scope) {
                    return Err(TenancyError::RoleScope {
                        reference,
                        role: role.as_str(),
                        scope: principal.scope.to_string(),
                    });
                }
            }
            if let Credential::Oidc { issuer, subject } = principal.body.credential() {
                let key = (issuer.clone(), subject.clone());
                if let Some(first) = directory.humans.get(&key) {
                    let first = directory.principals[first].reference;
                    return Err(TenancyError::DuplicatePrincipal {
                        reference,
                        first,
                        detail: format!("{subject} at {issuer}"),
                    });
                }
                directory.humans.insert(key, principal.body.principal());
            }
            // The same rule for a minted key: two workloads sharing a digest would
            // make `authenticate_workload` return whichever id sorts first, so a
            // key would carry a scope and a role set nobody granted it. SQL holds
            // a unique index on the digest; refusing it here means the revision is
            // invalid rather than unpublishable, and the refusal names the digest
            // rather than a name that did not clash.
            if let Credential::MintedKey {
                digest: Some(digest),
            } = principal.body.credential()
            {
                if let Some(first) = directory.keys.get(digest) {
                    let first = directory.principals[first].reference;
                    return Err(TenancyError::DuplicateKey {
                        reference,
                        first,
                        digest: digest.to_string(),
                    });
                }
                directory.keys.insert(*digest, principal.body.principal());
            }
            directory
                .principals
                .insert(principal.body.principal(), principal);
        }
        Ok(directory)
    }

    /// Every principal, ordered by [`PrincipalId`].
    pub fn principals(&self) -> impl ExactSizeIterator<Item = &Principal> {
        self.principals.values()
    }

    pub fn principal(&self, id: PrincipalId) -> Option<&Principal> {
        self.principals.get(&id)
    }

    /// The principal an OIDC sign-in resolves to.
    pub fn human(&self, issuer: &str, subject: &str) -> Option<&Principal> {
        let id = self.humans.get(&(issuer.to_owned(), subject.to_owned()))?;
        self.principals.get(id)
    }

    /// The workload a presented key authenticates as, if any.
    ///
    /// Linear in the number of workloads and constant-time per comparison, which
    /// is the right trade for an *administrative* authentication: this is called
    /// when an operator or a CI job calls the admin API, not while an inference
    /// request is in flight. The request path authenticates against the snapshot's
    /// inbound keys ([`crate::principals`]) and never reaches this function —
    /// putting a directory scan on a chat completion is exactly the request-path
    /// control-plane lookup the design forbids.
    pub fn authenticate_workload(&self, presented: &str) -> Option<&Principal> {
        self.principals.values().find(|principal| {
            matches!(
                principal.body.credential(),
                Credential::MintedKey { digest: Some(digest) } if WorkloadKey::verifies(digest, presented)
            )
        })
    }

    /// Decide one administrative request.
    ///
    /// The whole decision, in order, with the reason for each refusal:
    ///
    /// 1. **Who is calling.** A [`Caller::Human`] resolves by `(issuer, subject)`;
    ///    a [`Caller::Workload`] resolves by id *and* has to belong to the tenant
    ///    it claims, so a stolen id from another tenant is [`DenialReason::CrossTenant`]
    ///    rather than a lookup that happens to succeed. Breakglass and the gateway
    ///    itself resolve to no principal at all — see below.
    /// 2. **Whether the request's tenant can be administered.** A deleted tenant
    ///    is a tombstone: reading its audit trail is a database operation, not an
    ///    API call. A disabled tenant is administrable, because settling a bill
    ///    and re-enabling are the reasons to disable rather than delete. A
    ///    project-scoped request also has to name a project that tenant owns:
    ///    pairing one's own tenant with a foreign project id is a scope the grant
    ///    test cannot see through.
    /// 3. **Whether the caller's own tenant is administrable**, which is not the
    ///    same question: a principal of a disabled tenant may not act *anywhere*,
    ///    or disabling a tenant would leave its administrators able to keep
    ///    changing it.
    /// 4. **Whether any grant reaches the request**: the grant's scope has to
    ///    contain the request's scope, and the role has to hold the action on the
    ///    surface.
    ///
    /// [`Caller::Breakglass`] is allowed everything and recorded as breakglass,
    /// because a deployment whose only administrator locked themselves out has no
    /// other way back in and an auditor's first question is whether it was used.
    /// [`Caller::System`] is the gateway's own background work: read anywhere, and
    /// write only the two deployment-wide surfaces it owns — the model catalogue
    /// and the price book it refreshes from upstream. It cannot touch a tenant's
    /// state, so a compromised refresher is not a compromised tenant.
    ///
    /// Neither of them skips step 2, and that ordering is the definition rather
    /// than an accident: breakglass and system are *deployment-scoped* recovery.
    /// A request that names a tenant this revision does not declare, or one that
    /// has been deleted, is refused whoever asks — a tombstone is not a tenant
    /// with a stricter door. Recovering from "the tenant is gone" means publishing
    /// a revision that declares it again, at deployment scope, which breakglass
    /// can always do; letting a tenant-scoped call resurrect it would make the
    /// lifecycle advisory and put a deleted tenant's rows back in reach of an API
    /// key rather than of a deliberate, reviewable publish.
    pub fn authorize(
        &self,
        tenancy: &Tenancy,
        caller: &Caller,
        request: AccessRequest,
    ) -> Result<Authorization, Denial> {
        let deny = |reason: DenialReason| {
            Err(Denial {
                actor: caller.actor(),
                request: request.clone(),
                reason,
            })
        };

        if let Some(tenant) = request.scope.tenant() {
            match tenancy.lifecycle(tenant) {
                None => return deny(DenialReason::UnknownTenant),
                Some(lifecycle) if !lifecycle.is_administrable() => {
                    return deny(DenialReason::TenantNotAdministrable);
                }
                Some(_) => {}
            }
        }

        // A project scope has to name a project *of* that tenant, checked here
        // rather than left to the grant test below: `ResourceScope::contains`
        // compares tenants when the grant is tenant-scoped, so a caller pairing
        // its own tenant with another tenant's project id — or with one no revision
        // declares — is inside its grant by that measure. A write would still be
        // refused by the composite foreign key; a read authorized on a
        // contradictory scope has nothing behind it at all.
        if let ResourceScope::Project { tenant, project } = request.scope
            && tenancy.project(project).map(|owned| owned.body.tenant()) != Some(tenant)
        {
            return deny(DenialReason::UnknownProject);
        }

        let principal = match caller {
            Caller::Breakglass => {
                return Ok(Authorization {
                    actor: caller.actor(),
                    request,
                    basis: Basis::Breakglass,
                });
            }
            Caller::System { .. } => {
                return if system_permits(&request) {
                    Ok(Authorization {
                        actor: caller.actor(),
                        request,
                        basis: Basis::System,
                    })
                } else {
                    deny(DenialReason::RoleLacksAction)
                };
            }
            Caller::Human { issuer, subject } => self.human(issuer, subject),
            // The kind is checked as well as the tenant, so the two caller shapes
            // are symmetric: `Caller::Human` resolves through an index that holds
            // only OIDC identities, and a workload claim naming a human's
            // principal id — which is not a secret, it is a published resource id
            // — must not authorize with that human's roles and record the change
            // as that human. Callers are expected to come from
            // [`Directory::authenticate_workload`]; this is the decision point
            // refusing to depend on that.
            Caller::Workload { principal, tenant } => self.principal(*principal).filter(|found| {
                found.body.kind() == IdentityKind::Workload && found.tenant() == Some(*tenant)
            }),
        };
        let Some(principal) = principal else {
            return deny(DenialReason::UnknownPrincipal);
        };

        if let Some(tenant) = principal.tenant()
            && !tenancy
                .lifecycle(tenant)
                .is_some_and(|lifecycle| lifecycle.is_administrable())
        {
            return deny(DenialReason::TenantNotAdministrable);
        }

        if !principal.scope.contains(&request.scope) {
            // A caller reaching another tenant is named as such, because
            // "out of scope" and "another tenant's row" are the same refusal to
            // the caller and very different events to an auditor.
            let crossing = match (principal.tenant(), request.scope.tenant()) {
                (Some(held), Some(wanted)) => held != wanted,
                _ => false,
            };
            return deny(if crossing {
                DenialReason::CrossTenant
            } else {
                DenialReason::OutOfScope
            });
        }

        let role = principal
            .body
            .roles()
            .find(|role| role.permits(request.surface, request.action));
        let Some(role) = role else {
            return deny(DenialReason::RoleLacksAction);
        };
        // Unreachable for a principal out of a resolved directory, which refuses
        // the one shape that has no attribution; a caller that assembled one by
        // hand is refused here rather than granted an unattributable change.
        let Some(actor) = principal.actor() else {
            return deny(DenialReason::UnknownPrincipal);
        };
        Ok(Authorization {
            actor,
            request,
            basis: Basis::Role {
                role,
                principal: principal.body.principal(),
            },
        })
    }
}

/// What the gateway's own background work may do: read anything, and refresh the
/// two deployment-wide catalogues it owns.
fn system_permits(request: &AccessRequest) -> bool {
    if !request.action.is_write() {
        return true;
    }
    matches!(request.scope, ResourceScope::Deployment)
        && matches!(request.surface, Surface::Model | Surface::Price)
        && matches!(request.action, Action::Create | Action::Update)
}

/// Who is asking.
///
/// Distinct from [`Actor`], which is what an audit row records: a caller is a
/// *claim* to be resolved, and the resolution is what produces the actor. The
/// workload variant carries the tenant it claims for exactly that reason — a
/// claim that does not match the directory is a recordable event, and an
/// unresolvable caller still has to be attributable in the denial it causes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Caller {
    /// An OIDC-authenticated person.
    Human { issuer: String, subject: String },
    /// A workload that authenticated with its key, and the tenant it claims.
    Workload {
        tenant: TenantId,
        principal: PrincipalId,
    },
    /// The static bootstrap operator.
    Breakglass,
    /// The gateway itself.
    System { component: String },
}

impl Caller {
    /// How this caller is attributed, whether or not it resolves to a principal.
    pub fn actor(&self) -> Actor {
        match self {
            Self::Human { issuer, subject } => Actor::Human {
                issuer: issuer.clone(),
                subject: subject.clone(),
            },
            Self::Workload { tenant, principal } => Actor::Workload {
                tenant: *tenant,
                principal: *principal,
            },
            Self::Breakglass => Actor::Breakglass,
            Self::System { component } => Actor::System {
                component: component.clone(),
            },
        }
    }
}

/// One administrative request: a verb, a surface, and where.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessRequest {
    pub surface: Surface,
    pub action: Action,
    /// The scope of the thing being touched — a tenant's project, a deployment's
    /// catalogue. Not the caller's scope.
    pub scope: ResourceScope,
}

impl AccessRequest {
    pub fn new(surface: Surface, action: Action, scope: ResourceScope) -> Self {
        Self {
            surface,
            action,
            scope,
        }
    }

    /// The request that changes a specific resource, so a caller cannot describe
    /// a mutation of one kind and authorize another.
    pub fn of(reference: &ResourceRef, action: Action, scope: ResourceScope) -> Self {
        Self::new(Surface::of(reference.kind), action, scope)
    }
}

/// Why an [`Authorization`] was granted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Basis {
    /// A role held by a principal. Both are recorded: the role is why, and the
    /// principal is who.
    Role { role: Role, principal: PrincipalId },
    /// The bootstrap breakglass operator.
    Breakglass,
    /// The gateway's own background work.
    System,
}

/// Proof that a request was authorized.
///
/// Unforgeable outside this module — the fields are private and there is no
/// public constructor — and the only source of a [`Mutation`] or an
/// [`AuditEvent`], so "we forgot to check" is not reachable from a handler that
/// compiles: there is nothing to pass to `publish_revision` without a decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Authorization {
    actor: Actor,
    request: AccessRequest,
    basis: Basis,
}

impl Authorization {
    pub const fn actor(&self) -> &Actor {
        &self.actor
    }

    pub const fn request(&self) -> &AccessRequest {
        &self.request
    }

    pub const fn basis(&self) -> &Basis {
        &self.basis
    }

    /// Whether this decision rests on breakglass, which is what an alert watches
    /// for rather than something buried in an audit summary.
    pub const fn is_breakglass(&self) -> bool {
        matches!(self.basis, Basis::Breakglass)
    }

    /// The mutation this authorization permits.
    ///
    /// `None` for a read: a read publishes no revision, so it has no mutation and
    /// no revision-carried audit event. Reads are recorded where they are served,
    /// which this slice does not add.
    pub fn mutation(
        &self,
        id: MutationId,
        idempotency_key: IdempotencyKey,
        submitted_at: SystemTime,
    ) -> Option<Mutation> {
        Some(Mutation {
            id,
            actor: self.actor.clone(),
            kind: self.request.action.mutation_kind()?,
            scope: self.request.scope.clone(),
            idempotency_key,
            submitted_at,
        })
    }

    /// The audit event for a mutation this authorization permitted.
    ///
    /// `summary` is operator-facing text, and it must not contain credential
    /// material: an audit trail is read by more people than a response is, and it
    /// is kept for longer. Callers build it from ids, slugs, and verbs — the
    /// things already in the row.
    pub fn audit(
        &self,
        id: AuditEventId,
        mutation: MutationId,
        target: Option<ResourceRef>,
        summary: impl Into<String>,
        recorded_at: SystemTime,
    ) -> Option<AuditEvent> {
        Some(AuditEvent {
            id,
            mutation,
            actor: self.actor.clone(),
            kind: self.request.action.mutation_kind()?,
            target,
            summary: summary.into(),
            recorded_at,
        })
    }
}

/// Why a request was refused.
///
/// Precise for the audit trail, and never returned to the caller as-is: see
/// [`Denial::public_reason`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DenialReason {
    /// No principal matched the caller's claim — including a workload id that
    /// exists in another tenant, which is not distinguished here because to the
    /// directory it is simply not that tenant's principal.
    UnknownPrincipal,
    /// The request names a tenant this revision does not declare.
    UnknownTenant,
    /// The request names a project this revision does not declare, or one that
    /// belongs to a tenant other than the one the request paired it with. One
    /// reason for both, because telling a caller which of the two it hit is a
    /// project-existence oracle for every other tenant.
    UnknownProject,
    /// The tenant — the request's, or the caller's own — is deleted.
    TenantNotAdministrable,
    /// The caller holds grants in a different tenant.
    CrossTenant,
    /// The caller's grants are narrower than the request: a project-scoped
    /// principal reaching its tenant, or a tenant-scoped one reaching the
    /// deployment.
    OutOfScope,
    /// The caller is in scope, and no role it holds allows this action on this
    /// surface.
    RoleLacksAction,
}

impl DenialReason {
    pub const ALL: &'static [Self] = &[
        Self::UnknownPrincipal,
        Self::UnknownTenant,
        Self::UnknownProject,
        Self::TenantNotAdministrable,
        Self::CrossTenant,
        Self::OutOfScope,
        Self::RoleLacksAction,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnknownPrincipal => "unknown-principal",
            Self::UnknownTenant => "unknown-tenant",
            Self::UnknownProject => "unknown-project",
            Self::TenantNotAdministrable => "tenant-not-administrable",
            Self::CrossTenant => "cross-tenant",
            Self::OutOfScope => "out-of-scope",
            Self::RoleLacksAction => "role-lacks-action",
        }
    }

    /// Read a stored reason back. Resolves through the same names
    /// [`DenialReason::as_str`] writes, so a durable denial trail cannot come to
    /// mean something else.
    pub fn parse(text: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|reason| reason.as_str() == text)
    }
}

impl fmt::Display for DenialReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A refused request: who, what, where, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Denial {
    actor: Actor,
    request: AccessRequest,
    reason: DenialReason,
}

impl Denial {
    pub const fn actor(&self) -> &Actor {
        &self.actor
    }

    pub const fn request(&self) -> &AccessRequest {
        &self.request
    }

    /// The reason, for the audit trail and for operator logs.
    pub const fn reason(&self) -> DenialReason {
        self.reason
    }

    /// What the caller is told: one string for every reason.
    ///
    /// Deliberately uninformative. "No such tenant" for one id and "forbidden"
    /// for another turns the admin API into an oracle for which tenant ids exist,
    /// and enumerating tenants is the first step of the cross-tenant attempt this
    /// model exists to refuse. The precise reason is recorded in
    /// [`Denial::record`], where the audience is an auditor rather than the
    /// caller.
    pub const fn public_reason(&self) -> &'static str {
        "forbidden"
    }

    /// The audit record for this refusal.
    ///
    /// Denied actions are audited, and they cannot ride on a revision: nothing was
    /// published, so there is no mutation and no revision to hang an
    /// [`AuditEvent`] off. This is the separate record, and it carries no
    /// caller-supplied text at all — every field is an id, an enum, or a
    /// timestamp, so a denial cannot be the thing that writes an attacker's bytes
    /// into the audit trail.
    pub fn record(&self, id: AuditEventId, recorded_at: SystemTime) -> AccessDenial {
        AccessDenial {
            id,
            actor: self.actor.clone(),
            surface: self.request.surface,
            action: self.request.action,
            scope: self.request.scope.clone(),
            reason: self.reason,
            recorded_at,
        }
    }
}

impl fmt::Display for Denial {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} may not {} {} at {}: {}",
            self.actor, self.request.action, self.request.surface, self.request.scope, self.reason
        )
    }
}

/// An audit record of a denied administrative action.
///
/// A first-class durable record rather than a log line, because "who tried to
/// reach another tenant" is the question a tenancy incident starts from, and a log
/// line is not retained, ordered, or tenant-attributed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessDenial {
    pub id: AuditEventId,
    pub actor: Actor,
    pub surface: Surface,
    pub action: Action,
    /// What was reached for. The tenant of this scope is what a tenant-scoped
    /// query of the denial trail filters on, so a tenant's administrators can see
    /// attempts against their own tenant and no others.
    pub scope: ResourceScope,
    pub reason: DenialReason,
    pub recorded_at: SystemTime,
}

impl Canonical for AccessDenial {
    /// Excludes `recorded_at`, as [`AuditEvent`] does: a timestamp is when the
    /// row was written, not what it says, and including it would make two
    /// otherwise identical records incomparable.
    fn canonical(&self) -> CanonicalValue {
        CanonicalValue::map([
            ("id", CanonicalValue::string(self.id.to_string())),
            ("actor", self.actor.canonical()),
            ("surface", CanonicalValue::string(self.surface.as_str())),
            ("action", CanonicalValue::string(self.action.as_str())),
            ("scope", self.scope.canonical()),
            ("reason", CanonicalValue::string(self.reason.as_str())),
        ])
    }
}

/// The tenant a denial is attributed to for a tenant-scoped read, if any.
///
/// A denial whose scope is the deployment belongs to no tenant, and is therefore
/// only visible to a platform administrator.
impl AccessDenial {
    pub const fn tenant(&self) -> Option<TenantId> {
        self.scope.tenant()
    }

    pub fn project(&self) -> Option<ProjectId> {
        match self.scope {
            ResourceScope::Project { project, .. } => Some(project),
            ResourceScope::Deployment | ResourceScope::Tenant(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::super::fixtures::{
        display_name, human, principal_id, project, project_id, state, state_with_directory,
        tenant, tenant_id, workload, workload_key,
    };
    use super::*;
    use crate::desired_state::TenantLifecycle;

    fn directory(state: &DesiredState) -> (Tenancy, Directory) {
        let tenancy = Tenancy::of(state).expect("fixture tenancy is valid");
        let directory = Directory::of(state, &tenancy).expect("fixture directory is valid");
        (tenancy, directory)
    }

    fn request(surface: Surface, action: Action, scope: ResourceScope) -> AccessRequest {
        AccessRequest::new(surface, action, scope)
    }

    fn caller_human(subject: &str) -> Caller {
        Caller::Human {
            issuer: "https://idp.example".to_owned(),
            subject: subject.to_owned(),
        }
    }

    /// The matrix, asserted over the whole cross product rather than sampled: 55
    /// cells per role, and every cell either named by a rule below or absent.
    ///
    /// Written as an independent statement of the intended model — "who may do
    /// what" in prose — so it fails if [`Role::actions`] is edited, rather than
    /// re-deriving the table from the table.
    #[test]
    fn the_authorization_matrix_is_exactly_the_intended_one() {
        for &role in Role::ALL {
            for &surface in Surface::ALL {
                for &action in Action::ALL {
                    // The audit trail is read-only for everyone who holds it at
                    // all, so it is stated once, above the roles.
                    let expected = if surface == Surface::AuditTrail {
                        action == Action::Read
                            && !matches!(role, Role::BillingViewer | Role::Developer)
                    } else {
                        match role {
                            Role::PlatformAdmin => true,
                            Role::TenantAdmin => match surface {
                                Surface::Tenant => {
                                    matches!(action, Action::Read | Action::Update)
                                }
                                Surface::Billing => action == Action::Read,
                                Surface::Provider | Surface::Credential => true,
                                _ => action != Action::Rotate,
                            },
                            Role::Operator => match surface {
                                Surface::Provider | Surface::Credential => true,
                                Surface::Model | Surface::Alias => action != Action::Rotate,
                                _ => action == Action::Read,
                            },
                            Role::BillingViewer => {
                                action == Action::Read
                                    && matches!(
                                        surface,
                                        Surface::Tenant
                                            | Surface::Project
                                            | Surface::Price
                                            | Surface::Billing
                                    )
                            }
                            Role::Developer => match surface {
                                Surface::Alias => action != Action::Rotate,
                                Surface::Project | Surface::Model | Surface::Price => {
                                    action == Action::Read
                                }
                                _ => false,
                            },
                        }
                    };
                    assert_eq!(
                        role.permits(surface, action),
                        expected,
                        "{role} on {surface}/{action}"
                    );
                }
            }
        }
    }

    /// Two properties the matrix must hold whatever its cells say: nobody but a
    /// platform administrator may create or delete a tenant, and no role short of
    /// tenant administrator may touch the identity directory — a role that can
    /// grant roles can grant itself any other cell.
    #[test]
    fn only_a_platform_admin_creates_tenants_and_only_admins_grant_roles() {
        for &role in Role::ALL {
            let platform = role == Role::PlatformAdmin;
            assert_eq!(role.permits(Surface::Tenant, Action::Create), platform);
            assert_eq!(role.permits(Surface::Tenant, Action::Delete), platform);
            let grants = matches!(role, Role::PlatformAdmin | Role::TenantAdmin);
            for &action in Action::ALL {
                if action.is_write() {
                    assert_eq!(
                        role.permits(Surface::Principal, action),
                        grants && action != Action::Rotate || platform,
                        "{role} writing the directory with {action}"
                    );
                }
            }
        }
        // Nothing may write the audit trail, whatever else it holds — including
        // the role that holds everything else.
        for &role in Role::ALL {
            for &action in Action::ALL {
                if action.is_write() {
                    assert!(
                        !role.permits(Surface::AuditTrail, action),
                        "{role} may {action} the audit trail"
                    );
                }
            }
        }
        // And reading it stays a role a deployment grants deliberately: finance
        // and application developers are not auditors.
        for &role in Role::ALL {
            assert_eq!(
                role.permits(Surface::AuditTrail, Action::Read),
                !matches!(role, Role::BillingViewer | Role::Developer),
                "{role} reads the audit trail"
            );
        }
    }

    #[test]
    fn a_role_is_grantable_only_at_the_scopes_it_means() {
        let tenant = tenant_id(1);
        let project = ResourceScope::Project {
            tenant,
            project: project_id(2),
        };
        assert!(Role::PlatformAdmin.permits_scope(&ResourceScope::Deployment));
        assert!(!Role::PlatformAdmin.permits_scope(&ResourceScope::Tenant(tenant)));
        assert!(Role::TenantAdmin.permits_scope(&ResourceScope::Tenant(tenant)));
        assert!(!Role::TenantAdmin.permits_scope(&project));
        assert!(!Role::TenantAdmin.permits_scope(&ResourceScope::Deployment));
        for role in [Role::Operator, Role::BillingViewer, Role::Developer] {
            assert!(role.permits_scope(&ResourceScope::Tenant(tenant)));
            assert!(role.permits_scope(&project));
            assert!(!role.permits_scope(&ResourceScope::Deployment));
        }
    }

    #[test]
    fn every_vocabulary_round_trips_through_its_stored_spelling() {
        for &role in Role::ALL {
            assert_eq!(Role::parse(role.as_str()), Some(role));
        }
        for &surface in Surface::ALL {
            assert_eq!(Surface::parse(surface.as_str()), Some(surface));
        }
        for &action in Action::ALL {
            assert_eq!(Action::parse(action.as_str()), Some(action));
        }
        for &reason in DenialReason::ALL {
            assert_eq!(DenialReason::parse(reason.as_str()), Some(reason));
        }
        assert_eq!(Role::parse("root"), None);
        assert_eq!(Surface::parse("everything"), None);
        // The spellings are distinct, so no two enum members collide in storage.
        let roles: BTreeSet<&str> = Role::ALL.iter().map(|role| role.as_str()).collect();
        assert_eq!(roles.len(), Role::ALL.len());
        let surfaces: BTreeSet<&str> = Surface::ALL
            .iter()
            .map(|surface| surface.as_str())
            .collect();
        assert_eq!(surfaces.len(), Surface::ALL.len());
    }

    #[test]
    fn an_identity_body_round_trips_and_binds_to_its_envelope() {
        for resource in [
            human(
                30,
                "root",
                ResourceScope::Deployment,
                &[Role::PlatformAdmin],
            ),
            workload(
                33,
                "deployer",
                ResourceScope::Tenant(tenant_id(1)),
                &[Role::Operator, Role::Developer],
                Some(&workload_key(0xd0)),
            ),
            workload(
                34,
                "revoked",
                ResourceScope::Tenant(tenant_id(1)),
                &[Role::Operator],
                None,
            ),
        ] {
            let body = IdentityBody::read(&resource).expect("a written identity reads back");
            assert_eq!(body.body(), resource.body);
            assert_eq!(
                ResourceId::new(body.principal().uuid()),
                resource.reference.id
            );
        }
    }

    #[test]
    fn an_identity_cannot_be_half_a_human_and_cannot_hold_no_role() {
        assert_eq!(
            IdentityBody::new(
                principal_id(30),
                display_name("Nobody"),
                Credential::MintedKey { digest: None },
                [],
            ),
            Err(IdentityError::NoRoles)
        );
        let person = IdentityBody::new(
            principal_id(31),
            display_name("Ada"),
            Credential::Oidc {
                issuer: "https://idp.example".to_owned(),
                subject: "ada".to_owned(),
            },
            [Role::TenantAdmin],
        )
        .expect("a granted human");
        assert_eq!(
            person.clone().with_key_digest(None),
            Err(IdentityError::NotAWorkload {
                principal: principal_id(31)
            })
        );
        assert_eq!(person.with_roles([]), Err(IdentityError::NoRoles));
        // A human's body carries no key digest field at all, so there is no
        // representable identity holding both.
        let encoded = format!(
            "{:?}",
            human(31, "ada", ResourceScope::Deployment, &[Role::PlatformAdmin]).body
        );
        assert!(!encoded.contains(KEY_DIGEST_FIELD), "{encoded}");
    }

    #[test]
    fn a_minted_key_is_shown_once_hashed_at_rest_and_verified_in_constant_time() {
        let key = WorkloadKey::generate().expect("system randomness");
        let digest = key.digest();
        let redacted = format!("{key:?}");
        assert_eq!(redacted, "WorkloadKey(<redacted>)");
        let material = key.expose_once();
        assert!(material.starts_with(WorkloadKey::PREFIX));
        assert!(!redacted.contains(&material));
        // The digest is what a body carries, and it is not the material.
        assert!(!digest.to_string().contains(&material));
        assert!(WorkloadKey::verifies(&digest, &material));
        assert!(!WorkloadKey::verifies(&digest, "axw1.not-a-key"));
        let other = WorkloadKey::generate().expect("system randomness");
        assert_ne!(other.digest(), digest);
        assert!(!WorkloadKey::verifies(&digest, &other.expose_once()));
        assert_eq!(WorkloadKey::parse("nope").unwrap_err(), KeyError::Prefix);
        assert_eq!(WorkloadKey::parse("axw1.XYZ").unwrap_err(), KeyError::Shape);
    }

    #[test]
    fn a_workload_authenticates_by_digest_and_a_revoked_one_authenticates_with_nothing() {
        let mut state = state_with_directory();
        state
            .insert(workload(
                34,
                "revoked",
                ResourceScope::Tenant(tenant_id(1)),
                &[Role::Operator],
                None,
            ))
            .expect("a second workload");
        let (_, directory) = directory(&state);
        let found = directory
            .authenticate_workload(&workload_key(0xd0))
            .expect("the digest matches");
        assert_eq!(found.body.principal(), principal_id(33));
        assert!(
            directory
                .authenticate_workload(&workload_key(0xd1))
                .is_none()
        );
        // A key of the wrong shape is refused before any digest is computed.
        assert!(directory.authenticate_workload("axw1.deployer").is_none());
        // The revoked workload still exists, still holds its grant, and cannot
        // authenticate: revocation is not deletion.
        let revoked = directory
            .principal(principal_id(34))
            .expect("still in the directory");
        assert_eq!(
            revoked.body.credential(),
            &Credential::MintedKey { digest: None }
        );
    }

    #[test]
    fn a_directory_refuses_a_cross_tenant_or_unscoped_identity() {
        let tenant = tenant_id(1);
        // A workload with no tenant: nothing could attribute its changes.
        let mut deployment_workload = state();
        deployment_workload
            .insert(workload(
                33,
                "deployer",
                ResourceScope::Deployment,
                &[Role::PlatformAdmin],
                Some(&workload_key(0xd0)),
            ))
            .expect("insertion is not validation");
        let tenancy = Tenancy::of(&deployment_workload).expect("valid tenancy");
        assert!(matches!(
            Directory::of(&deployment_workload, &tenancy),
            Err(TenancyError::IdentityScope { .. })
        ));

        // A principal of a tenant no revision declares.
        let mut unknown_tenant = state();
        unknown_tenant
            .insert(human(
                31,
                "admin",
                ResourceScope::Tenant(tenant_id(99)),
                &[Role::TenantAdmin],
            ))
            .expect("insertion is not validation");
        let tenancy = Tenancy::of(&unknown_tenant).expect("valid tenancy");
        assert!(matches!(
            Directory::of(&unknown_tenant, &tenancy),
            Err(TenancyError::UnknownTenant { .. })
        ));

        // A role granted at a scope it may not be granted at.
        let mut misscoped_role = state();
        misscoped_role
            .insert(human(
                31,
                "admin",
                ResourceScope::Tenant(tenant),
                &[Role::PlatformAdmin],
            ))
            .expect("insertion is not validation");
        let tenancy = Tenancy::of(&misscoped_role).expect("valid tenancy");
        assert!(matches!(
            Directory::of(&misscoped_role, &tenancy),
            Err(TenancyError::RoleScope { .. })
        ));
    }

    #[test]
    fn one_person_is_one_principal() {
        let mut state = state_with_directory();
        // The same `(issuer, subject)` as principal 30, at a different id.
        state
            .insert(human(
                35,
                "root",
                ResourceScope::Deployment,
                &[Role::PlatformAdmin],
            ))
            .expect("insertion is not validation");
        let tenancy = Tenancy::of(&state).expect("valid tenancy");
        assert!(matches!(
            Directory::of(&state, &tenancy),
            Err(TenancyError::DuplicatePrincipal { .. })
        ));
    }

    /// And one key authenticates one workload. Without this the key would resolve
    /// to whichever principal sorted first, carrying a scope and a role set nobody
    /// granted it — and the refusal would come only from SQL, at publication.
    #[test]
    fn one_key_is_one_workload() {
        let mut state = state_with_directory();
        // The digest principal 32 already carries, at a different id and with a
        // role that principal was not granted.
        state
            .insert(workload(
                36,
                "second-runner",
                ResourceScope::Tenant(tenant_id(1)),
                &[Role::TenantAdmin],
                Some(&workload_key(0xd0)),
            ))
            .expect("insertion is not validation");
        let tenancy = Tenancy::of(&state).expect("valid tenancy");
        assert!(matches!(
            Directory::of(&state, &tenancy),
            Err(TenancyError::DuplicateKey { .. })
        ));
    }

    #[test]
    fn scope_containment_is_one_way() {
        let tenant = tenant_id(1);
        let other = tenant_id(11);
        let project = ResourceScope::Project {
            tenant,
            project: project_id(2),
        };
        assert!(ResourceScope::Deployment.contains(&project));
        assert!(ResourceScope::Tenant(tenant).contains(&project));
        assert!(!project.contains(&ResourceScope::Tenant(tenant)));
        assert!(!ResourceScope::Tenant(tenant).contains(&ResourceScope::Deployment));
        assert!(!ResourceScope::Tenant(tenant).contains(&ResourceScope::Tenant(other)));
        assert!(!project.contains(&ResourceScope::Project {
            tenant: other,
            project: project_id(2),
        }));
    }

    #[test]
    fn a_decision_names_who_and_why_and_produces_the_mutation() {
        let state = state_with_directory();
        let (tenancy, directory) = directory(&state);
        let scope = ResourceScope::Tenant(tenant_id(1));
        let granted = directory
            .authorize(
                &tenancy,
                &caller_human("admin"),
                request(Surface::Alias, Action::Create, scope.clone()),
            )
            .expect("a tenant admin creates an alias in its own tenant");
        assert_eq!(
            granted.basis(),
            &Basis::Role {
                role: Role::TenantAdmin,
                principal: principal_id(31),
            }
        );
        assert!(!granted.is_breakglass());
        let mutation = granted
            .mutation(
                MutationId::new(super::super::ids::Uuid7::from_parts(7, 7, 7).expect("parts")),
                IdempotencyKey::parse("create-alias").expect("a key"),
                SystemTime::UNIX_EPOCH,
            )
            .expect("a write has a mutation");
        assert_eq!(mutation.kind, MutationKind::Create);
        assert_eq!(mutation.scope, scope);
        assert_eq!(
            mutation.actor,
            Actor::Human {
                issuer: "https://idp.example".to_owned(),
                subject: "admin".to_owned(),
            }
        );
        // A read publishes nothing, so it has neither.
        let read = directory
            .authorize(
                &tenancy,
                &caller_human("admin"),
                request(Surface::Alias, Action::Read, scope),
            )
            .expect("a tenant admin reads its own aliases");
        assert!(
            read.mutation(
                MutationId::new(super::super::ids::Uuid7::from_parts(7, 7, 8).expect("parts")),
                IdempotencyKey::parse("read").expect("a key"),
                SystemTime::UNIX_EPOCH,
            )
            .is_none()
        );
    }

    #[test]
    fn a_workload_is_attributed_to_its_tenant_and_its_principal() {
        let state = state_with_directory();
        let (tenancy, directory) = directory(&state);
        let tenant = tenant_id(1);
        let granted = directory
            .authorize(
                &tenancy,
                &Caller::Workload {
                    tenant,
                    principal: principal_id(33),
                },
                request(
                    Surface::Credential,
                    Action::Rotate,
                    ResourceScope::Tenant(tenant),
                ),
            )
            .expect("an operator workload rotates its tenant's credential");
        assert_eq!(
            granted.actor(),
            &Actor::Workload {
                tenant,
                principal: principal_id(33),
            }
        );
    }

    /// A workload claim naming a human is not that human.
    ///
    /// A `PrincipalId` is a published resource id rather than a secret, so a
    /// caller path that built `Caller::Workload` from a request-supplied id —
    /// instead of from the principal `authenticate_workload` returned — would
    /// otherwise authorize with a tenant administrator's roles and record the
    /// change as that person.
    #[test]
    fn a_workload_claim_naming_a_human_authorizes_as_nobody() {
        let state = state_with_directory();
        let (tenancy, directory) = directory(&state);
        let tenant = tenant_id(1);
        let denial = directory
            .authorize(
                &tenancy,
                &Caller::Workload {
                    tenant,
                    // The tenant administrator of this very tenant: the claim is
                    // wrong about the kind and about nothing else.
                    principal: principal_id(31),
                },
                request(
                    Surface::Principal,
                    Action::Create,
                    ResourceScope::Tenant(tenant),
                ),
            )
            .expect_err("a human's id does not authenticate a workload");
        assert_eq!(denial.reason(), DenialReason::UnknownPrincipal);
        assert_eq!(denial.public_reason(), "forbidden");
    }

    #[test]
    fn a_caller_of_one_tenant_cannot_reach_another() {
        let mut state = state_with_directory();
        state.insert(tenant(11, "globex")).expect("a second tenant");
        let (tenancy, directory) = directory(&state);
        let denial = directory
            .authorize(
                &tenancy,
                &caller_human("admin"),
                request(
                    Surface::Alias,
                    Action::Read,
                    ResourceScope::Tenant(tenant_id(11)),
                ),
            )
            .expect_err("another tenant is not reachable");
        assert_eq!(denial.reason(), DenialReason::CrossTenant);
        // And the caller is told nothing that distinguishes it from any other
        // refusal, including "that tenant does not exist".
        assert_eq!(denial.public_reason(), "forbidden");
        let unknown = directory
            .authorize(
                &tenancy,
                &caller_human("admin"),
                request(
                    Surface::Alias,
                    Action::Read,
                    ResourceScope::Tenant(tenant_id(98)),
                ),
            )
            .expect_err("a tenant that does not exist is not reachable either");
        assert_eq!(unknown.reason(), DenialReason::UnknownTenant);
        assert_eq!(unknown.public_reason(), denial.public_reason());
    }

    /// A caller pairing its *own* tenant with a project that is not that tenant's:
    /// inside its grant by scope containment, which compares tenants, and a
    /// contradiction the decision has to catch itself.
    #[test]
    fn a_caller_cannot_pair_its_tenant_with_a_project_it_does_not_own() {
        let mut state = state_with_directory();
        state
            .insert(tenant(11, "globex"))
            .and_then(|state| state.insert(project(&tenant_id(11), 12, "edge")))
            .expect("a second tenant with a project of its own");
        let (tenancy, directory) = directory(&state);
        let tenant = tenant_id(1);
        for project in [
            // Another tenant's project, and a project nothing declares.
            project_id(12),
            project_id(97),
        ] {
            let denial = directory
                .authorize(
                    &tenancy,
                    &caller_human("admin"),
                    request(
                        Surface::Alias,
                        Action::Read,
                        ResourceScope::Project { tenant, project },
                    ),
                )
                .expect_err("a project of another tenant is not this tenant's project");
            assert_eq!(denial.reason(), DenialReason::UnknownProject);
            assert_eq!(denial.public_reason(), "forbidden");
        }
        directory
            .authorize(
                &tenancy,
                &caller_human("admin"),
                request(
                    Surface::Alias,
                    Action::Read,
                    ResourceScope::Project {
                        tenant,
                        project: project_id(2),
                    },
                ),
            )
            .expect("its own tenant's project is still reachable");
    }

    #[test]
    fn a_project_scoped_caller_cannot_reach_its_tenant_and_a_narrow_role_cannot_widen() {
        let state = state_with_directory();
        let (tenancy, directory) = directory(&state);
        let tenant = tenant_id(1);
        let denial = directory
            .authorize(
                &tenancy,
                &caller_human("dev"),
                request(
                    Surface::Alias,
                    Action::Create,
                    ResourceScope::Tenant(tenant),
                ),
            )
            .expect_err("a project-scoped developer cannot write its tenant");
        assert_eq!(denial.reason(), DenialReason::OutOfScope);
        let denial = directory
            .authorize(
                &tenancy,
                &caller_human("dev"),
                request(
                    Surface::Credential,
                    Action::Read,
                    ResourceScope::Project {
                        tenant,
                        project: project_id(2),
                    },
                ),
            )
            .expect_err("a developer holds nothing on credentials");
        assert_eq!(denial.reason(), DenialReason::RoleLacksAction);
    }

    #[test]
    fn an_unresolvable_caller_is_refused_without_saying_which_half_was_wrong() {
        let state = state_with_directory();
        let (tenancy, directory) = directory(&state);
        let tenant = tenant_id(1);
        for caller in [
            caller_human("nobody"),
            Caller::Human {
                issuer: "https://other.example".to_owned(),
                subject: "admin".to_owned(),
            },
            // A real principal id, claimed by the wrong tenant.
            Caller::Workload {
                tenant: tenant_id(11),
                principal: principal_id(33),
            },
        ] {
            let denial = directory
                .authorize(
                    &tenancy,
                    &caller,
                    request(Surface::Alias, Action::Read, ResourceScope::Tenant(tenant)),
                )
                .expect_err("no principal matches");
            assert_eq!(denial.reason(), DenialReason::UnknownPrincipal);
        }
    }

    #[test]
    fn a_disabled_tenant_is_administrable_and_a_deleted_one_is_not() {
        let tenant = tenant_id(1);
        for (lifecycle, expected) in [
            (TenantLifecycle::Disabled, None),
            (
                TenantLifecycle::Deleted,
                Some(DenialReason::TenantNotAdministrable),
            ),
        ] {
            let mut state = state_with_directory();
            let body = super::super::fixtures::tenant_body(1, "Acme").in_lifecycle(lifecycle);
            state
                .supersede(body.version_at(
                    Slug::parse("acme").expect("a slug"),
                    ResourceVersionNumber::FIRST.next(),
                ))
                .expect("a later version of the same tenant");
            let (tenancy, directory) = directory(&state);
            let decision = directory.authorize(
                &tenancy,
                &caller_human("admin"),
                request(
                    Surface::Billing,
                    Action::Read,
                    ResourceScope::Tenant(tenant),
                ),
            );
            match expected {
                None => {
                    decision.expect("a disabled tenant is still administrable");
                }
                Some(reason) => {
                    assert_eq!(decision.expect_err("a tombstone").reason(), reason);
                }
            }
        }
    }

    #[test]
    fn breakglass_is_allowed_everything_and_recorded_as_itself() {
        let state = state_with_directory();
        let (tenancy, directory) = directory(&state);
        for &surface in Surface::ALL {
            for &action in Action::ALL {
                let granted = directory
                    .authorize(
                        &tenancy,
                        &Caller::Breakglass,
                        request(surface, action, ResourceScope::Tenant(tenant_id(1))),
                    )
                    .expect("breakglass is the way back in");
                assert!(granted.is_breakglass());
                assert_eq!(granted.actor(), &Actor::Breakglass);
            }
        }
    }

    /// Breakglass is the way back into a *deployment*, not a way into a tenant
    /// that no longer exists: the tenant gate runs before the caller is resolved,
    /// so a tombstoned or undeclared tenant refuses every caller, and recovery is
    /// a deployment-scoped publish that declares the tenant again.
    #[test]
    fn breakglass_and_the_gateway_recover_the_deployment_not_a_deleted_tenant() {
        let tenant = tenant_id(1);
        let mut state = state_with_directory();
        let body =
            super::super::fixtures::tenant_body(1, "Acme").in_lifecycle(TenantLifecycle::Deleted);
        state
            .supersede(body.version_at(
                Slug::parse("acme").expect("a slug"),
                ResourceVersionNumber::FIRST.next(),
            ))
            .expect("a later version of the same tenant");
        let (tenancy, directory) = directory(&state);
        let system = Caller::System {
            component: "catalog-refresh".to_owned(),
        };
        for caller in [Caller::Breakglass, system] {
            for scope in [
                ResourceScope::Tenant(tenant),
                ResourceScope::Tenant(tenant_id(97)),
            ] {
                let denial = directory
                    .authorize(
                        &tenancy,
                        &caller,
                        request(Surface::AuditTrail, Action::Read, scope),
                    )
                    .expect_err("a tombstone and a stranger are both closed");
                assert!(matches!(
                    denial.reason(),
                    DenialReason::TenantNotAdministrable | DenialReason::UnknownTenant
                ));
                assert_eq!(denial.public_reason(), "forbidden");
            }
            // And the recovery path is the deployment one, which stays open.
            directory
                .authorize(
                    &tenancy,
                    &caller,
                    request(Surface::Model, Action::Read, ResourceScope::Deployment),
                )
                .expect("deployment scope is where recovery happens");
        }
    }

    #[test]
    fn the_gateways_own_work_reads_anywhere_and_writes_only_its_catalogues() {
        let state = state_with_directory();
        let (tenancy, directory) = directory(&state);
        let system = Caller::System {
            component: "catalog-refresh".to_owned(),
        };
        for &surface in Surface::ALL {
            directory
                .authorize(
                    &tenancy,
                    &system,
                    request(surface, Action::Read, ResourceScope::Tenant(tenant_id(1))),
                )
                .expect("convergence reads");
        }
        directory
            .authorize(
                &tenancy,
                &system,
                request(Surface::Model, Action::Update, ResourceScope::Deployment),
            )
            .expect("the catalogue is the gateway's own");
        for bad in [
            request(Surface::Alias, Action::Create, ResourceScope::Deployment),
            request(
                Surface::Model,
                Action::Update,
                ResourceScope::Tenant(tenant_id(1)),
            ),
            request(Surface::Model, Action::Delete, ResourceScope::Deployment),
        ] {
            let denial = directory
                .authorize(&tenancy, &system, bad)
                .expect_err("a compromised refresher is not a compromised tenant");
            assert_eq!(denial.reason(), DenialReason::RoleLacksAction);
        }
    }

    #[test]
    fn a_denial_is_recorded_with_its_reason_and_no_caller_supplied_bytes() {
        let state = state_with_directory();
        let (tenancy, directory) = directory(&state);
        let scope = ResourceScope::Tenant(tenant_id(1));
        let denial = directory
            .authorize(
                &tenancy,
                &caller_human("dev"),
                request(Surface::Credential, Action::Rotate, scope.clone()),
            )
            .expect_err("a developer holds nothing on credentials");
        let id = AuditEventId::new(super::super::ids::Uuid7::from_parts(5, 5, 5).expect("parts"));
        let record = denial.record(id, SystemTime::UNIX_EPOCH);
        assert_eq!(record.id, id);
        assert_eq!(record.reason, DenialReason::OutOfScope);
        assert_eq!(record.surface, Surface::Credential);
        assert_eq!(record.action, Action::Rotate);
        assert_eq!(record.tenant(), Some(tenant_id(1)));
        assert_eq!(record.project(), None);
        assert_eq!(
            record.actor,
            Actor::Human {
                issuer: "https://idp.example".to_owned(),
                subject: "dev".to_owned(),
            }
        );
        // Canonical, so two records of the same refusal compare equal whatever
        // order their fields were built in, and the encoding excludes the clock.
        let later = denial.record(
            id,
            SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(60),
        );
        assert_eq!(record.canonical(), later.canonical());
    }

    /// The mode boundary, from the authorization side: #144 adds an administrative
    /// directory to the *stateful* control plane and nothing else. A stateless
    /// deployment has no published revision, so it has no directory — and the
    /// empty directory refuses every caller rather than defaulting open, which is
    /// what would turn a misconfigured stateful boot into an unauthenticated
    /// control plane.
    #[test]
    fn a_deployment_with_no_published_directory_grants_nothing_but_breakglass() {
        let empty = DesiredState::new();
        let tenancy = Tenancy::of(&empty).expect("an empty state has an empty tenancy");
        let directory = Directory::of(&empty, &tenancy).expect("and an empty directory");
        assert_eq!(directory.principals().count(), 0);
        assert!(
            directory
                .authenticate_workload(&workload_key(0xd0))
                .is_none()
        );

        let scope = ResourceScope::Tenant(tenant_id(1));
        for caller in [
            caller_human("root"),
            Caller::Workload {
                tenant: tenant_id(1),
                principal: principal_id(33),
            },
        ] {
            let denial = directory
                .authorize(
                    &tenancy,
                    &caller,
                    request(Surface::Tenant, Action::Read, scope.clone()),
                )
                .expect_err("an empty directory authorizes nobody");
            assert!(matches!(
                denial.reason(),
                DenialReason::UnknownPrincipal | DenialReason::UnknownTenant
            ));
        }

        // The static breakglass operator is the way in, so an empty directory is
        // recoverable rather than a locked door. Deployment scope, because there
        // is no tenant to be an administrator of yet.
        directory
            .authorize(
                &tenancy,
                &Caller::Breakglass,
                request(Surface::Tenant, Action::Create, ResourceScope::Deployment),
            )
            .expect("breakglass creates the first tenant");
    }

    #[test]
    fn a_denial_carries_no_secret_material() {
        let state = state_with_directory();
        let (tenancy, directory) = directory(&state);
        let denial = directory
            .authorize(
                &tenancy,
                &Caller::Workload {
                    tenant: tenant_id(1),
                    principal: principal_id(33),
                },
                request(Surface::Tenant, Action::Delete, ResourceScope::Deployment),
            )
            .expect_err("no tenant-scoped role deletes a tenant");
        let record = denial.record(
            AuditEventId::new(super::super::ids::Uuid7::from_parts(5, 5, 6).expect("parts")),
            SystemTime::UNIX_EPOCH,
        );
        let rendered = format!("{record:?} {denial}");
        assert!(!rendered.contains("axw1."), "{rendered}");
    }
}
