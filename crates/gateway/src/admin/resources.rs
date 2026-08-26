//! The typed request documents `/admin/v1` accepts, and the edits they become.
//!
//! Every mutating administrative route is the same three steps, which is why
//! they share one handler:
//!
//! 1. **Read a typed document.** A request body parses into a resource-specific
//!    struct that names its identities explicitly and refuses unknown fields.
//!    Nothing here reaches a backend, so a malformed body is
//!    [`AdminError::RequestInvalid`] before the control plane is consulted.
//! 2. **Turn it into a [`ResourcePlan`]:** the scope the change is attributed to,
//!    and a [`DesiredStateEdit`] over the *complete* desired state.
//! 3. **Hand both to [`super::service::AdminService::apply`]**, which owns everything else —
//!    mode, authority, preconditions, complete-candidate validation, the diff,
//!    dry-run purity, and one atomic publication.
//!
//! # Why identities are caller-supplied
//!
//! A create names the id it is creating. Minting one here would make a retry of
//! a request whose response was lost build a *different* candidate under the
//! same idempotency key, which the store correctly refuses as a reused key — the
//! retry-safety property the protocol exists to provide would be defeated by the
//! handler. The document a caller writes therefore names every id it creates,
//! and resending that document is what makes a retry the same candidate.
//!
//! # Why an edit rather than a patch
//!
//! Each edit supersedes exactly the resource versions its document describes,
//! against the state the service hydrated. It never removes a resource it was
//! not asked about, and it never reaches a store: a handler that wanted to
//! publish would have nothing to publish with.

use std::sync::Arc;

use gateway_core::{GuardrailRule, MiddlewareFailurePosture, MiddlewareScope};
use serde::Deserialize;
use serde::de::DeserializeOwned;

use super::error::AdminError;
use super::service::DesiredStateEdit;
use crate::backends::catalog::{CatalogContentId, ProviderId};
use crate::desired_state::policy::BufferedResponseRoute;
use crate::desired_state::{
    Actor, AliasTarget, Approval, ApprovedRate, ApprovedRates, BlobKind, BlobRef, BudgetBound,
    BudgetPolicy, CatalogOffering, Checksum, ConcurrencyPolicy, ContentMiddlewareRegistration,
    Credential, DesiredState, DisplayName, EffectiveInstant, EffectiveInterval, IdentityBody,
    IdentityKind, InvalidDisplayName, InvalidId, InvalidSlug, InvalidUuid7, ModelAliasBody,
    ModelEnablementBody, ModelLifecycle, ModelOwner, ObservedPrice, OfferingId, PolicyBody,
    PolicyEpoch, PolicyScope, PriceBookBody, PriceOrigin, PriceProvenance, PriceRule, PricedTarget,
    PrincipalId, ProjectBody, ProjectId, ProviderBody, ProviderCredentialBody, ResourceBody,
    ResourceId, ResourceKind, ResourceRef, ResourceScope, ResourceVersion, ResourceVersionNumber,
    RevocationPolicy, Role, RulePrecedence, SecretId, SecretLifecycle, SecretOwner, SecretRef,
    SecretVersion, Slug, Surface, TenantBody, TenantId, TenantLifecycle, ValidationError,
    WireFamily,
};

/// What a handler contributes to a mutation: where it applies, and what it does.
pub struct ResourcePlan {
    /// The scope the mutation is attributed to and authorized at — the scope of
    /// the resource being changed, not the caller's own reach.
    pub scope: ResourceScope,
    pub edit: Arc<dyn DesiredStateEdit>,
    /// Whether the document retires the resource: puts it into the terminal
    /// state its own lifecycle offers, which is the only kind of deletion this
    /// surface has. Read by the handler to keep `mutation: "delete"` honest in
    /// the audit trail — nothing here removes a resource, so a document that
    /// leaves it in service may not be recorded as a deletion.
    pub retires: bool,
}

impl ResourcePlan {
    fn new<E>(scope: ResourceScope, edit: E) -> Self
    where
        E: Fn(&mut DesiredState) -> Result<(), ValidationError> + Send + Sync + 'static,
    {
        Self {
            scope,
            edit: Arc::new(edit),
            retires: false,
        }
    }

    fn new_with_actor<E>(scope: ResourceScope, edit: E) -> Self
    where
        E: Fn(&mut DesiredState, &Actor) -> Result<(), ValidationError> + Send + Sync + 'static,
    {
        Self {
            scope,
            edit: Arc::new(ActorAwareEdit(edit)),
            retires: false,
        }
    }

    /// The same plan, marked as retiring the resource it publishes.
    #[must_use]
    fn retiring(mut self, retires: bool) -> Self {
        self.retires = retires;
        self
    }
}

struct ActorAwareEdit<F>(F);

impl<F> DesiredStateEdit for ActorAwareEdit<F>
where
    F: Fn(&mut DesiredState, &Actor) -> Result<(), ValidationError> + Send + Sync,
{
    fn edit(&self, state: &mut DesiredState, actor: &Actor) -> Result<(), AdminError> {
        (self.0)(state, actor).map_err(AdminError::from)
    }
}

/// A document one administrative route reads.
pub trait AdminResourceRequest: DeserializeOwned + Send + Sync + 'static {
    /// The schema name a refusal names, so a client learns which document it got
    /// wrong rather than only that it got one wrong.
    const SCHEMA: &'static str;

    /// The surface a refusal of this document is recorded against in the denial
    /// trail, so an investigator filters denials by what was reached for rather
    /// than by which URL was typed.
    const SURFACE: Surface;

    /// Resolve the document into the scope it changes and the edit it performs.
    ///
    /// Every failure here is a caller error about the *document* — an unparsable
    /// id, a display name that is not one, an unknown wire family — never about
    /// state, which this cannot see.
    fn plan(self) -> Result<ResourcePlan, AdminError>;
}

/// The envelope every mutating route shares.
///
/// The resource document is nested rather than flattened so both halves can
/// refuse unknown fields: a typo in a field name must not be read as an omission
/// and published as a change the caller did not describe.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationEnvelope<R> {
    /// The audit summary — why, in the author's words.
    pub summary: String,
    /// What kind of change this is, for the audit trail. Defaults to `update`,
    /// which is the honest default for an upsert: a create says so.
    #[serde(default)]
    pub mutation: MutationKindInput,
    pub resource: R,
}

/// The mutation kinds a caller may declare. Deliberately not
/// [`MutationKind::Rollback`], which is the rollback route's own and not
/// something an upsert may claim.
///
/// [`MutationKind::Rollback`]: crate::desired_state::MutationKind::Rollback
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationKindInput {
    Create,
    #[default]
    Update,
    Delete,
    Rotate,
}

impl MutationKindInput {
    pub const fn kind(self) -> crate::desired_state::MutationKind {
        use crate::desired_state::MutationKind;
        match self {
            Self::Create => MutationKind::Create,
            Self::Update => MutationKind::Update,
            Self::Delete => MutationKind::Delete,
            Self::Rotate => MutationKind::Rotate,
        }
    }
}

/// The body a rollback request carries: which retained revision to republish.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RollbackRequest {
    pub summary: String,
    pub revision: String,
    /// The scope the rollback is attributed to. Republishing a whole revision is
    /// deployment-wide by nature, and a scoped grant cannot authorize it: the
    /// field exists so the refusal is explicit rather than implied.
    #[serde(default)]
    pub tenant: Option<String>,
    #[serde(default)]
    pub project: Option<String>,
}

/// The document `POST /admin/v1/tenants` reads.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantRequest {
    pub tenant: String,
    pub slug: String,
    pub display_name: String,
    #[serde(default)]
    pub lifecycle: Option<String>,
}

impl AdminResourceRequest for TenantRequest {
    const SCHEMA: &'static str = "tenant";

    const SURFACE: Surface = Surface::Tenant;

    fn plan(self) -> Result<ResourcePlan, AdminError> {
        let tenant = tenant_id::<Self>(&self.tenant)?;
        let slug = slug::<Self>(&self.slug)?;
        let display_name = display_name::<Self>(&self.display_name)?;
        let lifecycle = match self.lifecycle.as_deref() {
            None => TenantLifecycle::Active,
            Some(text) => TenantLifecycle::parse(text).ok_or_else(|| {
                unknown::<Self>(
                    "lifecycle",
                    TenantLifecycle::ALL.iter().map(|state| state.as_str()),
                )
            })?,
        };
        // A tenant is deployment-scoped: creating one is not something a
        // tenant-scoped administrator can authorize for themselves.
        Ok(ResourcePlan::new(
            ResourceScope::Deployment,
            move |state: &mut DesiredState| {
                let body = TenantBody::new(tenant, display_name.clone()).in_lifecycle(lifecycle);
                let version = next_version(state, ResourceKind::Tenant, body.resource_id());
                publish(state, body.version_at(slug.clone(), version))?;
                Ok(())
            },
        )
        .retiring(lifecycle == TenantLifecycle::Deleted))
    }
}

/// The document `POST /admin/v1/projects` reads.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectRequest {
    pub project: String,
    pub tenant: String,
    pub slug: String,
    pub display_name: String,
}

impl AdminResourceRequest for ProjectRequest {
    const SCHEMA: &'static str = "project";

    const SURFACE: Surface = Surface::Project;

    fn plan(self) -> Result<ResourcePlan, AdminError> {
        let project = project_id::<Self>(&self.project)?;
        let tenant = tenant_id::<Self>(&self.tenant)?;
        let slug = slug::<Self>(&self.slug)?;
        let display_name = display_name::<Self>(&self.display_name)?;
        let body = ProjectBody::new(project, tenant, display_name);
        Ok(ResourcePlan::new(
            body.scope(),
            move |state: &mut DesiredState| {
                let version = next_version(state, ResourceKind::Project, body.resource_id());
                publish(state, body.version_at(slug.clone(), version))?;
                Ok(())
            },
        ))
    }
}

/// The document `POST /admin/v1/principals` reads.
///
/// Workload material never crosses the control-plane API. The caller presents
/// the digest of a one-time `axw1.` key it minted through its secret-delivery
/// workflow; the revision retains only that digest and the resulting snapshot
/// reconstructs the verifier. Omitting the digest is the typed revocation form.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrincipalRequest {
    pub principal: String,
    pub tenant: String,
    #[serde(default)]
    pub project: Option<String>,
    pub slug: String,
    pub display_name: String,
    /// `workload` is the backwards-compatible default. Human principals must
    /// say `human` explicitly so an issuer-scoped identity is never inferred
    /// from a partially populated document.
    #[serde(default)]
    pub identity_kind: Option<String>,
    #[serde(default)]
    pub issuer: Option<String>,
    #[serde(default)]
    pub subject: Option<String>,
    #[serde(default)]
    pub key_digest: Option<String>,
    pub roles: Vec<String>,
}

impl AdminResourceRequest for PrincipalRequest {
    const SCHEMA: &'static str = "principal";

    const SURFACE: Surface = Surface::Principal;

    fn plan(self) -> Result<ResourcePlan, AdminError> {
        let principal = PrincipalId::parse(&self.principal)
            .map_err(|error| malformed_id::<Self>("principal", PrincipalId::PREFIX, error))?;
        let tenant = tenant_id::<Self>(&self.tenant)?;
        let project = self
            .project
            .as_deref()
            .map(project_id::<Self>)
            .transpose()?;
        let scope = match project {
            Some(project) => ResourceScope::Project { tenant, project },
            None => ResourceScope::Tenant(tenant),
        };
        let slug = slug::<Self>(&self.slug)?;
        let display_name = display_name::<Self>(&self.display_name)?;
        let mut roles = Vec::with_capacity(self.roles.len());
        for role in &self.roles {
            roles.push(Role::parse(role).ok_or_else(|| {
                unknown::<Self>("roles", Role::ALL.iter().map(|role| role.as_str()))
            })?);
        }
        let kind = match self.identity_kind.as_deref().unwrap_or("workload") {
            value if value == IdentityKind::Human.as_str() => IdentityKind::Human,
            value if value == IdentityKind::Workload.as_str() => IdentityKind::Workload,
            _ => {
                return Err(unknown::<Self>(
                    "identity_kind",
                    IdentityKind::ALL.iter().map(|kind| kind.as_str()),
                ));
            }
        };
        let credential = match kind {
            IdentityKind::Human => {
                if self.key_digest.is_some() {
                    return Err(malformed::<Self>(
                        "key_digest",
                        "is not allowed for human identities",
                    ));
                }
                let issuer = self
                    .issuer
                    .as_deref()
                    .filter(|issuer| !issuer.trim().is_empty())
                    .ok_or_else(|| malformed::<Self>("issuer", "is required for human identities"))?
                    .trim()
                    .to_owned();
                let subject = self
                    .subject
                    .as_deref()
                    .filter(|subject| !subject.trim().is_empty())
                    .ok_or_else(|| {
                        malformed::<Self>("subject", "is required for human identities")
                    })?
                    .trim()
                    .to_owned();
                Credential::Oidc { issuer, subject }
            }
            IdentityKind::Workload => {
                if self.issuer.is_some() {
                    return Err(malformed::<Self>(
                        "issuer",
                        "is not allowed for workload identities",
                    ));
                }
                if self.subject.is_some() {
                    return Err(malformed::<Self>(
                        "subject",
                        "is not allowed for workload identities",
                    ));
                }
                let digest = self
                    .key_digest
                    .as_deref()
                    .map(|digest| checksum::<Self>("key_digest", digest))
                    .transpose()?;
                Credential::MintedKey { digest }
            }
        };
        let body = IdentityBody::new(principal, display_name, credential, roles)
            .map_err(|error| malformed::<Self>("roles", &error.to_string()))?;
        Ok(ResourcePlan::new(
            scope.clone(),
            move |state: &mut DesiredState| {
                let version = next_version(state, ResourceKind::Identity, body.resource_id());
                publish(state, body.version_at(scope.clone(), slug.clone(), version))?;
                Ok(())
            },
        ))
    }
}

/// The document `POST /admin/v1/providers` reads.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRequest {
    pub provider: String,
    pub tenant: String,
    #[serde(default)]
    pub project: Option<String>,
    pub slug: String,
    pub display_name: String,
    pub wire_family: String,
    pub endpoint: String,
}

impl AdminResourceRequest for ProviderRequest {
    const SCHEMA: &'static str = "provider";

    const SURFACE: Surface = Surface::Provider;

    fn plan(self) -> Result<ResourcePlan, AdminError> {
        let provider = resource_id::<Self>("provider", &self.provider)?;
        let tenant = tenant_id::<Self>(&self.tenant)?;
        let project = self
            .project
            .as_deref()
            .map(project_id::<Self>)
            .transpose()?;
        let slug = slug::<Self>(&self.slug)?;
        let display_name = display_name::<Self>(&self.display_name)?;
        let wire_family = wire_family::<Self>(&self.wire_family)?;
        let body = ProviderBody::for_tenant(
            provider,
            tenant,
            display_name,
            wire_family,
            self.endpoint.clone(),
        );
        let body = match project {
            Some(project) => body.owned_by_project(project),
            None => body,
        };
        Ok(ResourcePlan::new(
            body.scope(),
            move |state: &mut DesiredState| {
                let version = next_version(state, ResourceKind::Provider, body.resource_id());
                publish(state, body.version_at(slug.clone(), version))?;
                Ok(())
            },
        ))
    }
}

/// The document `POST /admin/v1/credentials` reads.
///
/// The secret is named, never carried: `secret` is a reference into the secret
/// store, and no field on this document accepts material.
///
/// `secret_version` is *unstated* when omitted rather than "version 1": for a
/// credential that already names the same secret it means the version in force,
/// so an edit that only changes a display name cannot silently republish a
/// rotated credential at v1 and re-stage it. A rotation is `rotate: true`, which
/// advances from that same in-force version.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialRequest {
    pub credential: String,
    pub tenant: String,
    #[serde(default)]
    pub project: Option<String>,
    pub provider: String,
    pub slug: String,
    pub display_name: String,
    pub secret: String,
    #[serde(default)]
    pub secret_version: Option<u64>,
    #[serde(default)]
    pub lifecycle: Option<String>,
    #[serde(default)]
    pub rotate: bool,
}

impl AdminResourceRequest for CredentialRequest {
    const SCHEMA: &'static str = "provider-credential";

    const SURFACE: Surface = Surface::Credential;

    fn plan(self) -> Result<ResourcePlan, AdminError> {
        let credential = resource_id::<Self>("credential", &self.credential)?;
        let tenant = tenant_id::<Self>(&self.tenant)?;
        let project = self
            .project
            .as_deref()
            .map(project_id::<Self>)
            .transpose()?;
        let provider = resource_id::<Self>("provider", &self.provider)?;
        let slug = slug::<Self>(&self.slug)?;
        let display_name = display_name::<Self>(&self.display_name)?;
        // `secret` is the one field an operator can paste material into by
        // mistake, so its refusal names the form expected rather than echoing
        // what arrived: an error is rendered into a response, a log line and an
        // audit trail, and a mispasted key must not reach any of them.
        let secret = SecretId::parse(&self.secret).map_err(|error| match error {
            InvalidId::Prefix { .. } => malformed::<Self>(
                "secret",
                &format!("is not a `{}`-prefixed secret id", SecretId::PREFIX),
            ),
            InvalidId::Uuid(uuid) => malformed::<Self>(
                "secret",
                &format!("names a secret id whose uuid {}", uuid_detail(&uuid)),
            ),
        })?;
        // An omitted version is *unstated*, not "the first": for a credential
        // that already exists it means the version in force, resolved against
        // the state below.
        let version = match self.secret_version {
            None => None,
            Some(version) => Some(
                SecretVersion::new(version)
                    .ok_or_else(|| malformed::<Self>("secret_version", "versions start at 1"))?,
            ),
        };
        let lifecycle = match self.lifecycle.as_deref() {
            None => None,
            Some(text) => Some(SecretLifecycle::parse(text).ok_or_else(|| {
                unknown::<Self>(
                    "lifecycle",
                    SecretLifecycle::ALL.iter().map(|state| state.as_str()),
                )
            })?),
        };
        let owner = match project {
            Some(project) => SecretOwner::project(tenant, project),
            None => SecretOwner::tenant(tenant),
        };
        let rotate = self.rotate;
        Ok(
            ResourcePlan::new(owner.scope(), move |state: &mut DesiredState| {
                let reference = state.version_of(ResourceKind::ProviderCredential, credential);
                // A credential that exists moves from what it *is*: the document
                // reauthors its provider, name, and material, but lifecycle is a
                // transition the domain owns, not a field an author overwrites.
                let previous = match reference {
                    Some(resource) => Some(ProviderCredentialBody::read(resource)?),
                    None => None,
                };
                // Every path here works from the material *in force*, never from
                // the version a document happens to spell. A credential serving
                // v5 whose document omits `secret_version` — because a rename is
                // not a statement about material, and `/admin/v1/state` does not
                // publish bodies for an author to read it from — would otherwise
                // be republished at v1 and re-staged: a silent downgrade that
                // takes the credential out of service. The document names the
                // credential, not the version it is at.
                let staged = |material: SecretRef| {
                    ProviderCredentialBody::staged(
                        credential,
                        owner,
                        provider,
                        display_name.clone(),
                        material,
                    )
                };
                // The material the document asks for: the version it states, or
                // the one in force when it states none and names the same secret.
                let in_force = previous.as_ref().map(ProviderCredentialBody::secret);
                let authored = match (version, in_force) {
                    (Some(version), _) => SecretRef::new(secret, version),
                    (None, Some(held)) if held.secret == secret => held,
                    (None, _) => SecretRef::first(secret),
                };
                let body = match (previous, rotate) {
                    (Some(previous), true) => previous.reauthored(staged(authored)).rotated(),
                    (Some(previous), false) => previous.reauthored(staged(authored)),
                    // Nothing to rotate: the first version of a credential is the
                    // material the author named, at the version they named.
                    (None, _) => staged(authored),
                };
                let body = match lifecycle {
                    Some(lifecycle) => body.transitioned(lifecycle)?,
                    None => body,
                };
                let next = next_version(state, ResourceKind::ProviderCredential, credential);
                publish(state, body.version_at(slug.clone(), next))?;
                Ok(())
            })
            .retiring(matches!(
                lifecycle,
                Some(SecretLifecycle::Revoked | SecretLifecycle::Tombstoned)
            )),
        )
    }
}

/// The document `POST /admin/v1/catalogs` reads: the catalogue snapshot a model
/// enablement pins.
///
/// The payload is not carried here — a snapshot is megabytes and lives in the
/// catalogue store — so the document declares its content address and size, and
/// the revision pins that digest.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogRequest {
    pub catalog: String,
    pub slug: String,
    pub digest: String,
    pub size_bytes: u64,
}

impl AdminResourceRequest for CatalogRequest {
    const SCHEMA: &'static str = "catalog-snapshot";

    const SURFACE: Surface = Surface::Model;

    fn plan(self) -> Result<ResourcePlan, AdminError> {
        let catalog = resource_id::<Self>("catalog", &self.catalog)?;
        let slug = slug::<Self>(&self.slug)?;
        let digest = checksum::<Self>("digest", &self.digest)?;
        let blob = BlobRef {
            kind: BlobKind::CatalogSnapshot,
            digest,
            size_bytes: self.size_bytes,
        };
        Ok(ResourcePlan::new(
            ResourceScope::Deployment,
            move |state: &mut DesiredState| {
                refuse_withdrawing_a_pinned_snapshot(state, catalog, digest)?;
                let version = next_version(state, ResourceKind::CatalogModel, catalog);
                state.declare_blob(blob);
                publish(
                    state,
                    ResourceVersion::new(
                        ResourceRef::new(ResourceKind::CatalogModel, catalog, version),
                        ResourceScope::Deployment,
                        slug.clone(),
                        ResourceBody::Blob(blob),
                    ),
                )?;
                // Re-pointing a catalogue row at a new snapshot orphans the old
                // payload's declaration, which is a validation failure no author
                // could otherwise repair.
                state.retain_referenced_blobs();
                Ok(())
            },
        ))
    }
}

/// One effective-dated price rule in a deployment PriceBook.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PriceRuleRequest {
    pub provider: String,
    pub model: String,
    pub precedence: String,
    pub from_millis: u64,
    #[serde(default)]
    pub until_millis: Option<u64>,
    pub input_nano_dollars_per_million: u64,
    pub output_nano_dollars_per_million: u64,
    #[serde(default)]
    pub origin: Option<String>,
    #[serde(default)]
    pub citation: Option<String>,
}

/// The document `POST /admin/v1/prices` reads.
///
/// The request may describe a draft or an approved book, but it cannot name the
/// approver: an approved body records the authenticated administrative actor
/// that actually published it. This is the management seam that turns a typed
/// PriceBook into a serving price rather than accepting caller-supplied audit
/// metadata.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PriceBookRequest {
    pub price_book: String,
    pub slug: String,
    pub catalog: String,
    pub catalog_version: u64,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub approved_at_millis: Option<u64>,
    #[serde(default)]
    pub approval_citation: Option<String>,
    pub rules: Vec<PriceRuleRequest>,
}

impl AdminResourceRequest for PriceBookRequest {
    const SCHEMA: &'static str = "price-book";

    const SURFACE: Surface = Surface::Price;

    fn plan(self) -> Result<ResourcePlan, AdminError> {
        let price_book = resource_id::<Self>("price_book", &self.price_book)?;
        let slug = slug::<Self>(&self.slug)?;
        let catalog = CatalogContentId::from_checksum(checksum::<Self>("catalog", &self.catalog)?);
        let catalog_version = ResourceVersionNumber::new(self.catalog_version)
            .ok_or_else(|| malformed::<Self>("catalog_version", "versions start at 1"))?;
        let approved = match self.state.as_deref() {
            None | Some("draft") => false,
            Some("approved") => true,
            Some(_) => {
                return Err(unknown::<Self>("state", ["draft", "approved"]));
            }
        };
        let approved_at = match (approved, self.approved_at_millis) {
            (true, Some(at)) => Some(EffectiveInstant::from_millis(at)),
            (true, None) => {
                return Err(malformed::<Self>(
                    "approved_at_millis",
                    "is required when state is `approved`",
                ));
            }
            (false, Some(_)) => {
                return Err(malformed::<Self>(
                    "approved_at_millis",
                    "is only accepted when state is `approved`",
                ));
            }
            (false, None) => None,
        };
        let approval_citation = match self.approval_citation {
            Some(citation) if approved => Some(display_name::<Self>(&citation)?),
            Some(_) => {
                return Err(malformed::<Self>(
                    "approval_citation",
                    "is only accepted when state is `approved`",
                ));
            }
            None => None,
        };

        let mut rules = Vec::with_capacity(self.rules.len());
        for (index, request) in self.rules.into_iter().enumerate() {
            let provider = ProviderId::parse(&request.provider).map_err(|_| {
                malformed::<Self>("rules", &format!("entry {index} has an invalid provider"))
            })?;
            if request.model.is_empty()
                || request.model.len() > 256
                || request.model.chars().any(char::is_control)
            {
                return Err(malformed::<Self>(
                    "rules",
                    &format!("entry {index} has an invalid model"),
                ));
            }
            let precedence = RulePrecedence::parse(&request.precedence).ok_or_else(|| {
                malformed::<Self>("rules", &format!("entry {index} has an invalid precedence"))
            })?;
            let effective_from = EffectiveInstant::from_millis(request.from_millis);
            let effective = match request.until_millis {
                Some(until) => {
                    EffectiveInterval::bounded(effective_from, EffectiveInstant::from_millis(until))
                        .map_err(|error| {
                            malformed::<Self>("rules", &format!("entry {index}: {error}"))
                        })?
                }
                None => EffectiveInterval::from(effective_from),
            };
            let origin = match request.origin.as_deref() {
                None => PriceOrigin::Catalogue,
                Some(origin) => PriceOrigin::parse(origin).ok_or_else(|| {
                    malformed::<Self>("rules", &format!("entry {index} has an invalid origin"))
                })?,
            };
            let provenance = match request.citation {
                Some(citation) => PriceProvenance::cited(origin, display_name::<Self>(&citation)?),
                None => PriceProvenance::stated(origin),
            };
            let rates = ApprovedRates::new(
                ApprovedRate::from_nanos(request.input_nano_dollars_per_million),
                ApprovedRate::from_nanos(request.output_nano_dollars_per_million),
            );
            rules.push(
                PriceRule::new(
                    PricedTarget::new(provider, request.model),
                    precedence,
                    effective,
                    rates,
                    provenance,
                )
                .map_err(|error| malformed::<Self>("rules", &format!("entry {index}: {error}")))?,
            );
        }

        Ok(ResourcePlan::new_with_actor(
            ResourceScope::Deployment,
            move |state: &mut DesiredState, actor: &Actor| {
                let approval = match (approved, approved_at) {
                    (true, Some(at)) => Approval::Approved {
                        by: actor.clone(),
                        at,
                        citation: approval_citation.clone(),
                    },
                    (false, None) => Approval::Draft,
                    _ => unreachable!("price approval was validated before the edit"),
                };
                let body = rules.iter().cloned().fold(
                    PriceBookBody::new(catalog, catalog_version, approval),
                    PriceBookBody::with_rule,
                );
                let version = next_version(state, ResourceKind::Price, price_book);
                publish(state, body.version_at(price_book, slug.clone(), version))?;
                Ok(())
            },
        ))
    }
}

/// The document `POST /admin/v1/models` reads: one offering, enabled for one
/// scope, pinned to one catalogue snapshot.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRequest {
    pub enablement: String,
    pub tenant: String,
    #[serde(default)]
    pub project: Option<String>,
    pub slug: String,
    pub offering: String,
    pub catalog: String,
    pub snapshot: String,
    pub wire_family: String,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub observed_input_micros_per_million: Option<u64>,
    #[serde(default)]
    pub observed_output_micros_per_million: Option<u64>,
}

impl AdminResourceRequest for ModelRequest {
    const SCHEMA: &'static str = "model-enablement";

    const SURFACE: Surface = Surface::Model;

    fn plan(self) -> Result<ResourcePlan, AdminError> {
        let enablement = resource_id::<Self>("enablement", &self.enablement)?;
        let tenant = tenant_id::<Self>(&self.tenant)?;
        let project = self
            .project
            .as_deref()
            .map(project_id::<Self>)
            .transpose()?;
        let slug = slug::<Self>(&self.slug)?;
        let offering = offering::<Self>(&self.offering)?;
        let catalog = resource_id::<Self>("catalog", &self.catalog)?;
        let snapshot = checksum::<Self>("snapshot", &self.snapshot)?;
        let wire_family = wire_family::<Self>(&self.wire_family)?;
        let state = match self.state.as_deref() {
            None => ModelLifecycle::Enabled,
            Some(text) => ModelLifecycle::parse(text).ok_or_else(|| {
                unknown::<Self>(
                    "state",
                    ModelLifecycle::ALL.iter().map(|state| state.as_str()),
                )
            })?,
        };
        let observed = match (
            self.observed_input_micros_per_million,
            self.observed_output_micros_per_million,
        ) {
            (Some(input), Some(output)) => Some(ObservedPrice::new(input, output)),
            (None, None) => None,
            // Half a rate is not a rate: recording one side would publish a price
            // nobody wrote.
            _ => {
                return Err(malformed::<Self>(
                    "observed_input_micros_per_million",
                    "an observed price needs both an input and an output rate",
                ));
            }
        };
        let owner = match project {
            Some(project) => ModelOwner::project(tenant, project),
            None => ModelOwner::tenant(tenant),
        };
        let body = ModelEnablementBody::new(
            enablement,
            owner,
            CatalogOffering::new(offering, snapshot),
            wire_family,
        )
        .transitioned(state);
        let body = match observed {
            Some(observed) => body.observing(observed),
            None => body,
        };
        Ok(
            ResourcePlan::new(owner.scope(), move |state: &mut DesiredState| {
                // The catalogue row is depended on at the version the state holds,
                // so an enablement pins the snapshot that is actually published
                // rather than a version the author guessed.
                let pinned = state
                    .version_of(ResourceKind::CatalogModel, catalog)
                    .map_or(
                        ResourceRef::new(
                            ResourceKind::CatalogModel,
                            catalog,
                            ResourceVersionNumber::FIRST,
                        ),
                        |resource| resource.reference,
                    );
                let version = next_version(state, ResourceKind::ModelEnablement, enablement);
                publish(state, body.version_at(slug.clone(), version, pinned))?;
                Ok(())
            })
            .retiring(state == ModelLifecycle::Disabled),
        )
    }
}

/// One target of an alias, at the exact enablement version it was published
/// against.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AliasTargetRequest {
    pub enablement: String,
    #[serde(default)]
    pub version: Option<u64>,
}

/// The document `POST /admin/v1/aliases` reads.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AliasRequest {
    pub alias: String,
    pub tenant: String,
    pub project: String,
    pub slug: String,
    pub wire_family: String,
    #[serde(default)]
    pub state: Option<String>,
    pub targets: Vec<AliasTargetRequest>,
}

impl AdminResourceRequest for AliasRequest {
    const SCHEMA: &'static str = "model-alias";

    const SURFACE: Surface = Surface::Alias;

    fn plan(self) -> Result<ResourcePlan, AdminError> {
        let alias = resource_id::<Self>("alias", &self.alias)?;
        let tenant = tenant_id::<Self>(&self.tenant)?;
        let project = project_id::<Self>(&self.project)?;
        let slug = alias_slug::<Self>(&self.slug)?;
        let wire_family = wire_family::<Self>(&self.wire_family)?;
        let lifecycle = match self.state.as_deref() {
            None => ModelLifecycle::Enabled,
            Some(text) => ModelLifecycle::parse(text).ok_or_else(|| {
                unknown::<Self>(
                    "state",
                    ModelLifecycle::ALL.iter().map(|state| state.as_str()),
                )
            })?,
        };
        // An omitted version is resolved against the enablement the state
        // actually holds, not assumed to be the first: re-posting an alias
        // document after its enablement advanced would otherwise name a version
        // the candidate no longer has, and be refused with nothing pointing at
        // the field to add.
        let mut targets = Vec::with_capacity(self.targets.len());
        for target in &self.targets {
            let enablement = resource_id::<Self>("targets.enablement", &target.enablement)?;
            let version = match target.version {
                None => None,
                Some(version) => {
                    Some(ResourceVersionNumber::new(version).ok_or_else(|| {
                        malformed::<Self>("targets.version", "versions start at 1")
                    })?)
                }
            };
            targets.push((enablement, version));
        }
        Ok(ResourcePlan::new(
            ResourceScope::Project { tenant, project },
            move |state: &mut DesiredState| {
                let resolved = targets
                    .iter()
                    .map(|(enablement, version)| {
                        let version = version.unwrap_or_else(|| {
                            state
                                .version_of(ResourceKind::ModelEnablement, *enablement)
                                .map_or(ResourceVersionNumber::FIRST, |held| held.reference.version)
                        });
                        AliasTarget::new(*enablement, version)
                    })
                    .collect::<Vec<_>>();
                let body = ModelAliasBody::new(alias, tenant, project, wire_family, resolved)
                    .transitioned(lifecycle);
                let version = next_version(state, ResourceKind::Alias, alias);
                publish(state, body.version_at(slug.clone(), version))?;
                Ok(())
            },
        )
        .retiring(lifecycle == ModelLifecycle::Disabled))
    }
}

/// The document `POST /admin/v1/policies` reads: budgets, limits, and revocation
/// for one tenant or project.
///
/// What is deliberately absent is every bootstrap-owned field — which backend
/// holds the ledger, its DSN, its key prefix, what happens when it is
/// unreachable. Those are the process's, not the control plane's, and the policy
/// body refuses them outright.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyRequest {
    pub tenant: String,
    #[serde(default)]
    pub project: Option<String>,
    pub slug: String,
    pub epoch: u64,
    pub subject_limit_microdollars: u64,
    #[serde(default)]
    pub namespace_limit_microdollars: Option<u64>,
    pub reservation_ttl_seconds: u64,
    pub max_in_flight_per_subject: u64,
    pub lease_ttl_seconds: u64,
    #[serde(default)]
    pub minimum_token_epoch: u64,
    /// Ordered content middleware. Core authentication, admission, accounting,
    /// and routing stages are intentionally not expressible here.
    #[serde(default)]
    pub content_middleware: Vec<ContentMiddlewareRequest>,
    /// Streaming surfaces where response-mutating middleware may explicitly
    /// replace byte-faithful passthrough with bounded buffering.
    #[serde(default)]
    pub buffered_response_routes: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentMiddlewareRequest {
    pub id: String,
    pub scopes: Vec<MiddlewareScope>,
    pub failure_posture: MiddlewareFailurePosture,
    pub max_duration_milliseconds: u64,
    #[serde(default)]
    pub guardrail: Option<ContentGuardrailRequest>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentGuardrailRequest {
    pub key_env: String,
    pub rules: Vec<GuardrailRule>,
}

impl AdminResourceRequest for PolicyRequest {
    const SCHEMA: &'static str = "policy";

    const SURFACE: Surface = Surface::Policy;

    fn plan(self) -> Result<ResourcePlan, AdminError> {
        let tenant = tenant_id::<Self>(&self.tenant)?;
        let project = self
            .project
            .as_deref()
            .map(project_id::<Self>)
            .transpose()?;
        let slug = slug::<Self>(&self.slug)?;
        let epoch = PolicyEpoch::new(self.epoch)
            .map_err(|error| malformed::<Self>("epoch", &error.to_string()))?;
        let budget = BudgetPolicy::new(
            self.subject_limit_microdollars,
            self.namespace_limit_microdollars,
            self.reservation_ttl_seconds,
        )
        .map_err(|error| {
            // The three settings share one bound, so the refusal names the one
            // the administrator actually set wrongly.
            let field = match BudgetPolicy::unmet_bound(
                self.subject_limit_microdollars,
                self.namespace_limit_microdollars,
            ) {
                BudgetBound::SubjectLimit => "subject_limit_microdollars",
                BudgetBound::NamespaceLimit => "namespace_limit_microdollars",
                BudgetBound::ReservationTtl => "reservation_ttl_seconds",
            };
            malformed::<Self>(field, &error.to_string())
        })?;
        let concurrency =
            ConcurrencyPolicy::new(self.max_in_flight_per_subject, self.lease_ttl_seconds)
                .map_err(|error| {
                    // Both settings share one bound too, and the request spells
                    // them the way a document does.
                    let field = ConcurrencyPolicy::unmet_bound(self.max_in_flight_per_subject)
                        .document_field();
                    malformed::<Self>(field, &error.to_string())
                })?;
        let revocation = RevocationPolicy::new(self.minimum_token_epoch);
        let scope = match project {
            Some(project) => PolicyScope::Project { tenant, project },
            None => PolicyScope::Tenant(tenant),
        };
        let content_middleware = self
            .content_middleware
            .into_iter()
            .map(|registration| {
                let middleware = ContentMiddlewareRegistration::new(
                    registration.id,
                    registration.scopes,
                    registration.failure_posture,
                    registration.max_duration_milliseconds,
                )
                .map_err(|error| malformed::<Self>("content_middleware", &error.to_string()))?;
                let Some(guardrail) = registration.guardrail else {
                    return Ok(middleware);
                };
                let guardrail = crate::desired_state::policy::ContentGuardrailRegistration::new(
                    guardrail.key_env,
                    guardrail.rules,
                )
                .map_err(|error| malformed::<Self>("content_middleware", &error.to_string()))?;
                middleware
                    .with_guardrail(guardrail)
                    .map_err(|error| malformed::<Self>("content_middleware", &error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let buffered_response_routes = self
            .buffered_response_routes
            .iter()
            .map(|route| {
                BufferedResponseRoute::parse(route).map_err(|error| {
                    malformed::<Self>("buffered_response_routes", &error.to_string())
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let body = PolicyBody::new(scope, epoch, budget, concurrency, revocation)
            .with_content_middleware(content_middleware)
            .map_err(|error| malformed::<Self>("content_middleware", &error.to_string()))?
            .with_buffered_response_routes(buffered_response_routes)
            .map_err(|error| malformed::<Self>("buffered_response_routes", &error.to_string()))?;
        Ok(ResourcePlan::new(
            scope.resource_scope(),
            move |state: &mut DesiredState| {
                let version = next_version(state, ResourceKind::Policy, body.resource_id());
                publish(state, body.version_at(slug.clone(), version))?;
                Ok(())
            },
        ))
    }
}

/// Refuse a catalogue refresh that would take away content an enablement reads
/// its offering from.
///
/// The dependent carry-forward below re-points edges, and an enablement's
/// snapshot is not an edge: it is part of what the enablement *is*, and
/// [`ModelEnablementBody::transition_from`] refuses a version that changes it.
/// Carrying the edge forward while the digest changed would therefore publish an
/// enablement whose pin resolves to nothing, reported as an unpinned snapshot
/// from three resources away. Named here, the refusal says which enablement and
/// which digest, and the documented flow — a new catalogue resource, new
/// enablements against it, the old ones retired — is reachable.
///
/// [`ModelEnablementBody::transition_from`]: crate::desired_state::ModelEnablementBody::transition_from
fn refuse_withdrawing_a_pinned_snapshot(
    state: &DesiredState,
    catalog: ResourceId,
    digest: Checksum,
) -> Result<(), ValidationError> {
    let Some(held) = state.version_of(ResourceKind::CatalogModel, catalog) else {
        return Ok(());
    };
    let withdrawn = match held.body.blob() {
        Some(blob) if blob.digest != digest => blob.digest,
        _ => return Ok(()),
    };
    let catalog = held.reference;
    for resource in state.resources() {
        if resource.reference.kind != ResourceKind::ModelEnablement {
            continue;
        }
        let pinned = ModelEnablementBody::read(resource)
            .map(|body| body.offering().is_pinned_to(withdrawn))
            .unwrap_or(false);
        if pinned {
            return Err(ValidationError::PinnedSnapshotWithdrawn {
                catalog,
                enablement: resource.reference,
                digest: withdrawn,
            });
        }
    }
    Ok(())
}

/// Publish a resource version into the candidate, advancing everything pinned to
/// the version it replaces onto it.
///
/// Dependency edges name an exact version, and one request publishes one
/// resource, so without this an enablement an alias points at could never be
/// changed again: superseding it would dangle the alias, and pointing the alias
/// at a version that does not exist yet dangles too. The edit holds the complete
/// desired state, so the candidate carries the dependents forward itself.
pub(super) fn publish(
    state: &mut DesiredState,
    resource: ResourceVersion,
) -> Result<(), ValidationError> {
    let current = resource.reference;
    let superseded = state.version_of(current.kind, current.id).map(|held| {
        let was_enabled = if current.kind == ResourceKind::ModelEnablement {
            ModelEnablementBody::read(held)
                .ok()
                .is_some_and(|body| body.is_enabled())
        } else {
            false
        };
        (held.reference, was_enabled)
    });
    state.supersede(resource)?;
    match superseded {
        Some((superseded, was_enabled)) => {
            let is_disabling = current.kind == ResourceKind::ModelEnablement
                && was_enabled
                && state
                    .get(&current)
                    .and_then(|resource| ModelEnablementBody::read(resource).ok())
                    .is_some_and(|body| !body.is_enabled());
            restack(state, superseded, current, is_disabling)
        }
        None => Ok(()),
    }
}

/// Re-pin every resource that depended on `superseded` onto `current`, and then
/// whatever depended on those, so one publication leaves no dangling edge.
fn restack(
    state: &mut DesiredState,
    superseded: ResourceRef,
    current: ResourceRef,
    disabling_enablement: bool,
) -> Result<(), ValidationError> {
    let dependents: Vec<ResourceVersion> = state
        .resources()
        .filter(|resource| resource.depends_on.contains(&superseded))
        .cloned()
        .collect();
    for dependent in dependents {
        let previous = dependent.reference;
        let version = previous.version.next();
        let advanced = if previous.kind == ResourceKind::Alias {
            // An alias names its targets in its *body*, so re-pinning it is a
            // retarget rather than an edge rewrite: the edges follow the body.
            let body = ModelAliasBody::read(&dependent)?;
            let targets = body
                .targets()
                .iter()
                .filter_map(|target| {
                    if target.enablement == superseded.id && target.version == superseded.version {
                        if disabling_enablement && body.is_enabled() {
                            None
                        } else {
                            Some(AliasTarget::new(target.enablement, current.version))
                        }
                    } else {
                        Some(*target)
                    }
                })
                .collect::<Vec<_>>();
            let body = body.retargeted(targets);
            let body = if disabling_enablement && body.is_enabled() && body.targets().is_empty() {
                body.transitioned(ModelLifecycle::Disabled)
            } else {
                body
            };
            body.version_at(dependent.slug.clone(), version)
        } else {
            // Everything else pins by edge alone — an enablement's catalogue
            // pin is the version of the row, while the snapshot digest it read
            // the offering from stays what it was published against.
            let mut depends_on = dependent.depends_on.clone();
            depends_on.remove(&superseded);
            depends_on.insert(current);
            ResourceVersion::new(
                ResourceRef::new(previous.kind, previous.id, version),
                dependent.scope.clone(),
                dependent.slug.clone(),
                dependent.body.clone(),
            )
            .depending_on(depends_on)
        };
        let advanced_reference = advanced.reference;
        state.supersede(advanced)?;
        // A dependent carry-forward changes its edge, not its lifecycle. Only
        // the original enabled -> disabled enablement transition may remove a
        // target; republication of an already-disabled enablement must preserve
        // a disabled alias's historical target for rollback/readability.
        restack(state, previous, advanced_reference, false)?;
    }
    Ok(())
}

/// The version a supersede publishes: the first, or one past what is there.
pub(super) fn next_version(
    state: &DesiredState,
    kind: ResourceKind,
    id: ResourceId,
) -> ResourceVersionNumber {
    state
        .version_of(kind, id)
        .map_or(ResourceVersionNumber::FIRST, |resource| {
            resource.reference.version.next()
        })
}

fn resource_id<R: AdminResourceRequest>(
    field: &'static str,
    text: &str,
) -> Result<ResourceId, AdminError> {
    ResourceId::parse(text).map_err(|error| malformed_id::<R>(field, ResourceId::PREFIX, error))
}

fn tenant_id<R: AdminResourceRequest>(text: &str) -> Result<TenantId, AdminError> {
    TenantId::parse(text).map_err(|error| malformed_id::<R>("tenant", TenantId::PREFIX, error))
}

fn project_id<R: AdminResourceRequest>(text: &str) -> Result<ProjectId, AdminError> {
    ProjectId::parse(text).map_err(|error| malformed_id::<R>("project", ProjectId::PREFIX, error))
}

fn slug<R: AdminResourceRequest>(text: &str) -> Result<Slug, AdminError> {
    map_slug::<R>(Slug::parse(text), false)
}

fn alias_slug<R: AdminResourceRequest>(text: &str) -> Result<Slug, AdminError> {
    map_slug::<R>(Slug::parse_alias(text), true)
}

fn map_slug<R: AdminResourceRequest>(
    parsed: Result<Slug, InvalidSlug>,
    alias: bool,
) -> Result<Slug, AdminError> {
    parsed.map_err(|error| {
        let detail = match error {
            InvalidSlug::Empty => "must not be empty".to_owned(),
            InvalidSlug::TooLong { max, .. } => format!("is over the {max}-character limit"),
            InvalidSlug::Character { .. } if alias => {
                "contains a character outside ASCII letters, digits, `.`, `-`, and `_`".to_owned()
            }
            InvalidSlug::Character { .. } => {
                "contains a character outside ASCII letters, digits, `-`, and `_`".to_owned()
            }
            InvalidSlug::Boundary { .. } => "must start and end with a letter or digit".to_owned(),
            InvalidSlug::IdLike { .. } => "looks like an id; ids are not names".to_owned(),
        };
        malformed::<R>("slug", &detail)
    })
}

fn display_name<R: AdminResourceRequest>(text: &str) -> Result<DisplayName, AdminError> {
    DisplayName::parse(text).map_err(|error| {
        let detail = match error {
            InvalidDisplayName::Empty => "must not be empty".to_owned(),
            InvalidDisplayName::TooLong { max, .. } => {
                format!("is over the {max}-character limit")
            }
            InvalidDisplayName::ControlCharacter { .. } => {
                "contains a control character".to_owned()
            }
            InvalidDisplayName::ByteOrderMark => "contains a byte-order mark".to_owned(),
            InvalidDisplayName::Untrimmed => "may not begin or end with whitespace".to_owned(),
        };
        malformed::<R>("display_name", &detail)
    })
}

fn malformed_id<R: AdminResourceRequest>(
    field: &'static str,
    prefix: &'static str,
    error: InvalidId,
) -> AdminError {
    let detail = match error {
        InvalidId::Prefix { .. } => format!("is not a `{prefix}`-prefixed id"),
        InvalidId::Uuid(uuid) => format!("has a uuid that {}", uuid_detail(&uuid)),
    };
    malformed::<R>(field, &detail)
}

/// Why a uuid was refused, said without repeating the text that arrived.
///
/// A prefixed reference whose prefix is right and whose uuid is not is a
/// different mistake from a value of the wrong kind entirely, and an
/// administrator cannot tell a typo from a mispaste if both refusals blame the
/// prefix.
pub(super) fn uuid_detail(error: &InvalidUuid7) -> String {
    match error {
        InvalidUuid7::Shape(_) => "is not a hyphenated 8-4-4-4-12 uuid".to_owned(),
        InvalidUuid7::Digit(_) => {
            "contains a character that is not a lowercase hex digit".to_owned()
        }
        InvalidUuid7::Version { version } => {
            format!("is version {version}, but only version 7 is accepted")
        }
        InvalidUuid7::Variant { variant } => {
            format!("has variant bits {variant:#04b}, but only the RFC 9562 variant is accepted")
        }
        InvalidUuid7::Timestamp { .. } | InvalidUuid7::Sequence { .. } => {
            "is not a version 7 uuid".to_owned()
        }
    }
}

/// A catalogue offering identity, refused by its shape rather than by its text:
/// the field takes a long opaque `off_`-prefixed digest, so a mispaste lands here
/// as plausibly as in a checksum field, and the refusal still separates a wrong
/// prefix from a malformed body.
fn offering<R: AdminResourceRequest>(text: &str) -> Result<OfferingId, AdminError> {
    OfferingId::parse(text).map_err(|error| malformed::<R>("offering", &error.to_string()))
}

/// A digest field is where a mispasted key lands most plausibly of all — it is
/// the one field that legitimately holds a long opaque string — so the refusal
/// names the form expected and never the text that arrived, while still saying
/// whether the algorithm prefix or the digits are at fault.
fn checksum<R: AdminResourceRequest>(
    field: &'static str,
    text: &str,
) -> Result<Checksum, AdminError> {
    Checksum::parse(text).map_err(|error| malformed::<R>(field, &error.to_string()))
}

/// A wire family this build does not speak is a *compatibility* refusal, not a
/// typo: a newer release may know it.
fn wire_family<R: AdminResourceRequest>(text: &str) -> Result<WireFamily, AdminError> {
    WireFamily::parse(text).ok_or_else(|| {
        unknown::<R>(
            "wire_family",
            WireFamily::ALL.iter().map(|family| family.as_str()),
        )
    })
}

fn malformed<R: AdminResourceRequest>(field: &'static str, detail: &str) -> AdminError {
    AdminError::RequestInvalid {
        schema: R::SCHEMA,
        detail: format!("`{field}`: {detail}"),
    }
}

/// A closed-set field refused with the set, not with the value.
///
/// The context an operator needs is what this build accepts, and that is a
/// compile-time list of this build's own constants: bounded, low-cardinality and
/// impossible to fill with caller text. Echoing the arriving value would add
/// nothing an operator cannot read off their own request, and a document that
/// pastes material into `lifecycle` would have it read back.
fn unknown<R: AdminResourceRequest>(
    field: &'static str,
    accepted: impl IntoIterator<Item = &'static str>,
) -> AdminError {
    let accepted = accepted
        .into_iter()
        .map(|value| format!("`{value}`"))
        .collect::<Vec<_>>()
        .join(", ");
    AdminError::RequestInvalid {
        schema: R::SCHEMA,
        detail: format!("`{field}`: is not a value this build knows; it accepts {accepted}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::desired_state::fixtures;

    const PASTED_MATERIAL: &str = "sk-axond-admin-sentinel-51H9xNEVERLOGME";

    fn credential() -> CredentialRequest {
        CredentialRequest {
            credential: fixtures::resource_id(11).to_string(),
            tenant: fixtures::tenant_id(1).to_string(),
            project: None,
            provider: fixtures::resource_id(10).to_string(),
            slug: "openai-primary".to_owned(),
            display_name: "OpenAI primary".to_owned(),
            secret: fixtures::secret_ref(3).secret.to_string(),
            secret_version: Some(1),
            lifecycle: Some("active".to_owned()),
            rotate: false,
        }
    }

    #[test]
    fn approved_price_books_record_the_authenticated_actor() {
        let request = PriceBookRequest {
            price_book: fixtures::resource_id(31).to_string(),
            slug: "deployment-prices".to_owned(),
            catalog: fixtures::catalog_content_id().checksum().to_string(),
            catalog_version: fixtures::catalog_version().get(),
            state: Some("approved".to_owned()),
            approved_at_millis: Some(0),
            approval_citation: Some("CHG-PRICE-1".to_owned()),
            rules: vec![PriceRuleRequest {
                provider: "openai".to_owned(),
                model: "gpt-4o".to_owned(),
                precedence: "baseline".to_owned(),
                from_millis: 0,
                until_millis: None,
                input_nano_dollars_per_million: 1_000_000,
                output_nano_dollars_per_million: 2_000_000,
                origin: Some("catalogue".to_owned()),
                citation: None,
            }],
        };
        let plan = request.plan().expect("the typed price document is valid");
        let actor = fixtures::actor();
        let mut state = DesiredState::new();
        plan.edit
            .edit(&mut state, &actor)
            .expect("the price document publishes");
        let resource = state
            .version_of(ResourceKind::Price, fixtures::resource_id(31))
            .expect("the price resource is present");
        let body = PriceBookBody::read(resource).expect("the published body is typed");
        assert_eq!(body.approval().approver(), Some(&actor));
        assert_eq!(body.rules().len(), 1);
    }

    #[test]
    fn policy_requests_normalize_buffered_routes_and_reject_invalid_sets() {
        let document = serde_json::json!({
            "tenant": fixtures::tenant_id(1).to_string(),
            "slug": "buffered-responses",
            "epoch": 2,
            "subject_limit_microdollars": 1_000_000,
            "reservation_ttl_seconds": 300,
            "max_in_flight_per_subject": 8,
            "lease_ttl_seconds": 60,
            "buffered_response_routes": ["responses", "messages"]
        });
        let request: PolicyRequest = serde_json::from_value(document.clone()).unwrap();
        let plan = request.plan().expect("typed buffering routes are accepted");
        let mut state = fixtures::state();
        plan.edit
            .edit(&mut state, &fixtures::actor())
            .expect("policy publishes");
        let scope = PolicyScope::Tenant(fixtures::tenant_id(1));
        let resource = state
            .version_of(ResourceKind::Policy, scope.resource_id())
            .expect("policy resource is present");
        let body = PolicyBody::read(resource).expect("published policy is typed");
        assert_eq!(
            body.buffered_response_routes(),
            [
                BufferedResponseRoute::Messages,
                BufferedResponseRoute::Responses,
            ]
        );

        let mut duplicate = document.clone();
        duplicate["buffered_response_routes"] = serde_json::json!(["messages", "messages"]);
        let duplicate: PolicyRequest = serde_json::from_value(duplicate).unwrap();
        assert!(duplicate.plan().is_err());

        let mut unknown = document;
        unknown["buffered_response_routes"] = serde_json::json!(["chat_completions"]);
        let unknown: PolicyRequest = serde_json::from_value(unknown).unwrap();
        assert!(unknown.plan().is_err());
    }

    #[test]
    fn principal_documents_publish_project_workload_digests_without_material() {
        let key = fixtures::workload_key(0xd0);
        let plan = PrincipalRequest {
            principal: fixtures::principal_id(33).to_string(),
            tenant: fixtures::tenant_id(1).to_string(),
            project: Some(fixtures::project_id(2).to_string()),
            slug: "inference".to_owned(),
            display_name: "Inference workload".to_owned(),
            identity_kind: None,
            issuer: None,
            subject: None,
            key_digest: Some(Checksum::of(key.as_bytes()).to_string()),
            roles: vec!["developer".to_owned()],
        }
        .plan()
        .expect("the typed principal document is valid");
        let mut state = fixtures::state();
        plan.edit
            .edit(&mut state, &fixtures::actor())
            .expect("the principal document publishes");
        let resource = state
            .version_of(
                ResourceKind::Identity,
                ResourceId::new(fixtures::principal_id(33).uuid()),
            )
            .expect("the principal resource is present");
        let body = IdentityBody::read(resource).expect("the published identity is typed");
        assert_eq!(
            body.credential(),
            &Credential::MintedKey {
                digest: Some(Checksum::of(key.as_bytes()))
            }
        );
    }

    fn refusal(request: CredentialRequest) -> String {
        match request.plan() {
            Ok(_) => panic!("the malformed credential document was accepted"),
            Err(error) => error
                .operator_detail()
                .expect("a request refusal has operator detail")
                .to_owned(),
        }
    }

    #[test]
    fn secret_reference_errors_distinguish_prefix_from_malformed_uuid_without_echoing() {
        let mut wrong_prefix = credential();
        wrong_prefix.secret = PASTED_MATERIAL.to_owned();
        let prefix_detail = refusal(wrong_prefix);
        assert_eq!(
            prefix_detail,
            "`secret`: is not a `sct_`-prefixed secret id"
        );
        assert!(!prefix_detail.contains(PASTED_MATERIAL));

        const MALFORMED_REFERENCE: &str = "sct_not-a-hyphenated-uuid";
        let mut malformed_uuid = credential();
        malformed_uuid.secret = MALFORMED_REFERENCE.to_owned();
        let detail = refusal(malformed_uuid);
        assert_eq!(
            detail,
            "`secret`: names a secret id whose uuid is not a hyphenated 8-4-4-4-12 uuid"
        );
        assert!(!detail.contains(MALFORMED_REFERENCE));
    }

    /// A right-prefix reference is refused for the reason it actually failed —
    /// an administrator cannot tell a typo in the identifier from a value of the
    /// wrong kind if every failure blames the prefix — and no reason repeats the
    /// text that arrived.
    #[test]
    fn a_malformed_uuid_is_refused_for_the_reason_it_failed() {
        const GOOD: &str = "0189f8c1-2a3b-7c4d-8e5f-6a7b8c9d0e1f";
        assert!(
            SecretId::parse(&format!("{}{GOOD}", SecretId::PREFIX)).is_ok(),
            "the case base must be a uuid the parser accepts"
        );
        let version4 = GOOD.replacen("-7c4d-", "-4c4d-", 1);
        let cases = [
            ("0189f8c1", "is not a hyphenated 8-4-4-4-12 uuid"),
            (
                &GOOD.replace('-', "_") as &str,
                "is not a hyphenated 8-4-4-4-12 uuid",
            ),
            (
                &GOOD.to_uppercase(),
                "contains a character that is not a lowercase hex digit",
            ),
            (&version4, "is version 4, but only version 7 is accepted"),
        ];

        for (uuid, reason) in cases {
            let mut request = credential();
            request.secret = format!("{}{uuid}", SecretId::PREFIX);
            let detail = refusal(request);
            assert_eq!(
                detail,
                format!("`secret`: names a secret id whose uuid {reason}"),
            );
            assert!(!detail.contains(uuid), "{uuid} was echoed: {detail}");
        }
    }

    /// The same distinction on the other identifier fields, which share one
    /// non-echoing renderer.
    #[test]
    fn identifier_fields_distinguish_a_wrong_prefix_from_a_malformed_uuid() {
        let mut wrong_prefix = credential();
        wrong_prefix.provider = format!("prv_{}", fixtures::resource_id(10).uuid());
        assert_eq!(
            refusal(wrong_prefix),
            "`provider`: is not a `res_`-prefixed id"
        );

        let mut malformed_uuid = credential();
        malformed_uuid.tenant = format!("{}not-a-uuid", TenantId::PREFIX);
        assert_eq!(
            refusal(malformed_uuid),
            "`tenant`: has a uuid that is not a hyphenated 8-4-4-4-12 uuid"
        );
    }

    #[test]
    fn malformed_document_fields_do_not_echo_pasted_material() {
        let cases = [
            ("credential", format!("{PASTED_MATERIAL}!")),
            ("tenant", format!("{PASTED_MATERIAL}!")),
            ("project", format!("{PASTED_MATERIAL}!")),
            ("provider", format!("{PASTED_MATERIAL}!")),
            ("slug", format!("{PASTED_MATERIAL}!")),
            ("display_name", format!("{PASTED_MATERIAL}\n")),
            ("lifecycle", PASTED_MATERIAL.to_owned()),
        ];

        for (field, value) in cases {
            let mut request = credential();
            match field {
                "credential" => request.credential = value.clone(),
                "tenant" => request.tenant = value.clone(),
                "project" => request.project = Some(value.clone()),
                "provider" => request.provider = value.clone(),
                "slug" => request.slug = value.clone(),
                "display_name" => request.display_name = value.clone(),
                "lifecycle" => request.lifecycle = Some(value.clone()),
                _ => unreachable!("the test case names its field"),
            }
            let detail = refusal(request);
            assert!(
                !detail.contains(&value),
                "{field} validation echoed pasted material: {detail}"
            );
        }
    }

    fn catalog() -> CatalogRequest {
        CatalogRequest {
            catalog: fixtures::resource_id(12).to_string(),
            slug: "models-dev".to_owned(),
            digest: format!("sha256:{}", "a".repeat(64)),
            size_bytes: 4_096,
        }
    }

    fn model() -> ModelRequest {
        ModelRequest {
            enablement: fixtures::resource_id(13).to_string(),
            tenant: fixtures::tenant_id(1).to_string(),
            project: None,
            slug: "gpt-4o".to_owned(),
            offering: format!("off_{}", "b".repeat(64)),
            catalog: fixtures::resource_id(12).to_string(),
            snapshot: format!("sha256:{}", "a".repeat(64)),
            wire_family: "openai-chat".to_owned(),
            state: None,
            observed_input_micros_per_million: None,
            observed_output_micros_per_million: None,
        }
    }

    fn detail_of<R: AdminResourceRequest>(request: R) -> String {
        match request.plan() {
            Ok(_) => panic!("the malformed document was accepted"),
            Err(error) => error
                .operator_detail()
                .expect("a request refusal has operator detail")
                .to_owned(),
        }
    }

    /// A digest and an offering identity are the two document fields that
    /// legitimately hold a long opaque string, so they are where a mispasted key
    /// is least conspicuous. The refusal still separates a wrong prefix from a
    /// malformed body, and neither reason repeats the text.
    #[test]
    fn digest_and_offering_refusals_name_the_form_and_not_the_text() {
        let mut pasted_digest = catalog();
        pasted_digest.digest = PASTED_MATERIAL.to_owned();
        let detail = detail_of(pasted_digest);
        assert_eq!(detail, "`digest`: is not prefixed `sha256:`");
        assert!(!detail.contains(PASTED_MATERIAL));

        let mut short_digest = catalog();
        short_digest.digest = format!("sha256:{PASTED_MATERIAL}");
        let detail = detail_of(short_digest);
        assert_eq!(detail, "`digest`: does not carry 64 lowercase hex digits");
        assert!(!detail.contains(PASTED_MATERIAL));

        let mut pasted_offering = model();
        pasted_offering.offering = PASTED_MATERIAL.to_owned();
        let detail = detail_of(pasted_offering);
        assert_eq!(detail, "`offering`: is not prefixed `off_`");
        assert!(!detail.contains(PASTED_MATERIAL));

        let mut short_offering = model();
        short_offering.offering = format!("off_{PASTED_MATERIAL}");
        let detail = detail_of(short_offering);
        assert_eq!(detail, "`offering`: does not carry 64 lowercase hex digits");
        assert!(!detail.contains(PASTED_MATERIAL));

        let mut pasted_snapshot = model();
        pasted_snapshot.snapshot = PASTED_MATERIAL.to_owned();
        let detail = detail_of(pasted_snapshot);
        assert_eq!(detail, "`snapshot`: is not prefixed `sha256:`");
        assert!(!detail.contains(PASTED_MATERIAL));
    }

    /// A closed set is refused with the set: the value that arrived carries no
    /// information an operator does not already have, and may carry material.
    #[test]
    fn a_closed_set_field_is_refused_with_what_this_build_accepts() {
        let mut unknown_family = model();
        unknown_family.wire_family = PASTED_MATERIAL.to_owned();
        let detail = detail_of(unknown_family);
        assert_eq!(
            detail,
            "`wire_family`: is not a value this build knows; \
             it accepts `openai-chat`, `anthropic-messages`"
        );
        assert!(!detail.contains(PASTED_MATERIAL));

        let mut unknown_state = model();
        unknown_state.state = Some(PASTED_MATERIAL.to_owned());
        let detail = detail_of(unknown_state);
        assert_eq!(
            detail,
            "`state`: is not a value this build knows; it accepts `enabled`, `disabled`"
        );
        assert!(!detail.contains(PASTED_MATERIAL));

        let mut unknown_lifecycle = credential();
        unknown_lifecycle.lifecycle = Some(PASTED_MATERIAL.to_owned());
        let detail = refusal(unknown_lifecycle);
        assert!(
            detail.starts_with("`lifecycle`: is not a value this build knows; it accepts `"),
            "{detail}"
        );
        assert!(!detail.contains(PASTED_MATERIAL));
    }
}
