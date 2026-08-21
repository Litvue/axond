//! Deterministic fixtures the domain and contract tests share.
//!
//! Every id here is built from [`Uuid7::from_parts`], so no test depends on a
//! clock: the same seed is the same id in every run, which is what lets a test
//! assert on exact canonical bytes and exact error payloads.

use std::time::{Duration, SystemTime};

use super::access::{Credential, IdentityBody, Role, WorkloadKey};
use super::canonical::{Canonical, CanonicalValue, Checksum};
use super::credentials::ProviderCredentialBody;
use super::ids::{
    AuditEventId, MutationId, PrincipalId, ProjectId, ResourceId, RevisionId, SecretId, Slug,
    TenantId, Uuid7,
};
use super::models::{
    AliasTarget, ApprovedPrice, CatalogOffering, ModelAliasBody, ModelEnablementBody, ModelOwner,
    ObservedPrice, OfferingId, WireFamily,
};
use super::mutation::{
    Actor, AuditEvent, ExpectedRevision, IdempotencyKey, Mutation, MutationKind,
};
use super::namespaces::{
    DeploymentBody, DeploymentProvider, DeploymentSecretIndexEntry, FlatProviderKind,
    InboundGrantBody, NamespaceBody, NamespaceCredential, NamespacePolicySpec,
};
use super::policy::{
    BudgetPolicy, ConcurrencyPolicy, PolicyBody, PolicyEpoch, PolicyScope, RevocationPolicy,
};
use super::pricing::{
    Approval, ApprovedRate, ApprovedRates, EffectiveInstant, EffectiveInterval, PriceBookBody,
    PriceBooks, PriceOrigin, PriceProvenance, PriceRule, PricedTarget, PricingSnapshot,
    RulePrecedence,
};
use super::resource::{
    BlobKind, BlobRef, ResourceBody, ResourceKind, ResourceRef, ResourceScope, ResourceVersion,
    ResourceVersionNumber,
};
use super::revision::{DesiredState, RevisionCandidate};
use super::secrets::{SecretLifecycle, SecretOwner, SecretRef, SecretVersion};
use super::tenancy::{DisplayName, ProjectBody, TenantBody};
use crate::backends::catalog::{CatalogContentId, ProviderId};
use crate::namespace::{NamespaceGrant, NamespaceId};

/// How many resource versions [`state`] contains.
pub(crate) const DESIRED_STATE_RESOURCES: usize = 5;

fn uuid(seed: u64) -> Uuid7 {
    Uuid7::from_parts(seed, 0, seed).expect("seeds are in range")
}

pub(crate) fn resource_id(seed: u64) -> ResourceId {
    ResourceId::new(uuid(seed))
}

pub(crate) fn tenant_id(seed: u64) -> TenantId {
    TenantId::new(uuid(seed))
}

pub(crate) fn project_id(seed: u64) -> ProjectId {
    ProjectId::new(uuid(seed))
}

pub(crate) fn revision_id(seed: u64) -> RevisionId {
    RevisionId::new(uuid(seed))
}

pub(crate) fn secret_id(seed: u64) -> SecretId {
    SecretId::new(uuid(seed))
}

/// The first version of the secret seeded by `seed`.
pub(crate) fn secret_ref(seed: u64) -> SecretRef {
    SecretRef::first(secret_id(seed))
}

pub(crate) fn secret_ref_at(seed: u64, version: u64) -> SecretRef {
    SecretRef::new(
        secret_id(seed),
        SecretVersion::new(version).expect("fixture secret version"),
    )
}

/// A minimal valid ADR 0062 revision used at hydration/recovery seams.
pub(crate) fn flat_namespace_state() -> DesiredState {
    let deployment =
        DeploymentBody::new(Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new())
            .expect("empty shared deployment authority is valid");
    let deployment = deployment.version(
        resource_id(950),
        Slug::parse("deployment").expect("fixture slug"),
    );
    let namespace = NamespaceBody::new(
        NamespaceId::parse("acme").expect("fixture namespace"),
        true,
        false,
        deployment.reference,
        Vec::new(),
        Vec::new(),
        NamespacePolicySpec {
            epoch: 1,
            exact: None,
            middleware: Vec::new(),
            buffered_response_routes: Vec::new(),
        },
        1,
    )
    .expect("minimal namespace is valid")
    .version(resource_id(951), Slug::parse("acme").expect("fixture slug"));
    let grant = InboundGrantBody::new(
        Checksum::of(b"flat-namespace-grant"),
        NamespaceGrant::all(),
        Some("fixture".to_owned()),
    )
    .expect("fixture grant")
    .version(
        resource_id(952),
        Slug::parse("fixture").expect("fixture slug"),
    );
    let mut state = DesiredState::new();
    state.insert(deployment).expect("deployment is unique");
    state.insert(namespace).expect("namespace is unique");
    state.insert(grant).expect("grant is unique");
    state.validate().expect("flat namespace fixture is valid");
    state
}

/// A flat-v2 revision whose active provider credential resolves through the
/// deployment secret index.
pub(crate) fn flat_namespace_state_with_active_credential() -> DesiredState {
    flat_namespace_state_with_active_credential_digest(Checksum::of(b"fixture-ciphertext"))
}

/// The active-credential fixture with a caller-selected immutable ciphertext
/// address. Blob resolver tests seal a real object first, then use its digest
/// in the authenticated deployment index.
pub(crate) fn flat_namespace_state_with_active_credential_digest(
    ciphertext_digest: Checksum,
) -> DesiredState {
    flat_namespace_credential_state(SecretLifecycle::Active, true, ciphertext_digest)
}

/// The successor to [`flat_namespace_state_with_active_credential`]: the
/// credential is withdrawn and its exact indexed ciphertext is tombstoned.
pub(crate) fn flat_namespace_state_with_tombstoned_credential() -> DesiredState {
    flat_namespace_credential_state(
        SecretLifecycle::Tombstoned,
        false,
        Checksum::of(b"fixture-ciphertext"),
    )
}

fn flat_namespace_credential_state(
    lifecycle: SecretLifecycle,
    include_credential: bool,
    ciphertext_digest: Checksum,
) -> DesiredState {
    let version = if include_credential {
        ResourceVersionNumber::FIRST
    } else {
        ResourceVersionNumber::new(2).expect("fixture resource version")
    };
    let reference = secret_ref(953);
    let deployment_body = DeploymentBody::new(
        vec![DeploymentProvider {
            id: Slug::parse("shared").expect("fixture provider slug"),
            kind: FlatProviderKind::OpenaiCompatible,
            base_url: "https://provider.example/v1".to_owned(),
        }],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![DeploymentSecretIndexEntry::new(
            NamespaceId::parse("acme").expect("fixture namespace"),
            reference,
            ciphertext_digest,
            lifecycle,
        )],
    )
    .expect("fixture deployment is valid");
    let deployment = ResourceVersion::new(
        ResourceRef::new(ResourceKind::Deployment, resource_id(950), version),
        ResourceScope::Deployment,
        Slug::parse("deployment").expect("fixture slug"),
        ResourceBody::Inline(deployment_body.canonical()),
    );
    let credentials = include_credential
        .then(|| NamespaceCredential {
            id: Slug::parse("primary").expect("fixture credential slug"),
            provider: Slug::parse("shared").expect("fixture provider slug"),
            secret: reference,
            weight: 1,
        })
        .into_iter()
        .collect();
    let namespace_body = NamespaceBody::new(
        NamespaceId::parse("acme").expect("fixture namespace"),
        true,
        false,
        deployment.reference,
        credentials,
        Vec::new(),
        NamespacePolicySpec {
            epoch: 1,
            exact: None,
            middleware: Vec::new(),
            buffered_response_routes: Vec::new(),
        },
        1,
    )
    .expect("fixture namespace is valid");
    let namespace = ResourceVersion::new(
        ResourceRef::new(ResourceKind::Namespace, resource_id(951), version),
        ResourceScope::Deployment,
        Slug::parse("acme").expect("fixture slug"),
        ResourceBody::Inline(namespace_body.canonical()),
    )
    .depending_on([deployment.reference]);
    let grant = InboundGrantBody::new(
        Checksum::of(b"flat-namespace-credential-grant"),
        NamespaceGrant::all(),
        Some("fixture".to_owned()),
    )
    .expect("fixture grant")
    .version(
        resource_id(952),
        Slug::parse("fixture").expect("fixture slug"),
    );
    let mut state = DesiredState::new();
    state.insert(deployment).expect("deployment is unique");
    state.insert(namespace).expect("namespace is unique");
    state.insert(grant).expect("grant is unique");
    state.validate().expect("flat credential fixture is valid");
    state
}

/// The namespace row above with a self-consistent newer-schema field.
pub(crate) fn incompatible_flat_namespace() -> ResourceVersion {
    let mut namespace = flat_namespace_state()
        .resources()
        .find(|resource| resource.reference.kind == ResourceKind::Namespace)
        .cloned()
        .expect("fixture contains a namespace");
    let ResourceBody::Inline(CanonicalValue::Map(fields)) = &mut namespace.body else {
        unreachable!("flat namespace fixture body is inline")
    };
    fields.push(("future_enforcement".to_owned(), CanonicalValue::Bool(true)));
    namespace
}

/// The provider a credential seeded by `seed` authenticates to.
///
/// Offset far from the seeds the fixtures use for resources, so a provider id is
/// deliberately *not* declared by [`state`]: a credential naming a provider row
/// this revision does not carry is valid desired state, which is the case the
/// fixtures exercise by default.
pub(crate) fn provider_id(seed: u64) -> ResourceId {
    resource_id(900 + seed)
}

pub(crate) fn reference(kind: ResourceKind, seed: u64) -> ResourceRef {
    ResourceRef::new(kind, resource_id(seed), ResourceVersionNumber::FIRST)
}

fn inline(field: &str, value: &str) -> ResourceBody {
    ResourceBody::Inline(CanonicalValue::map([(
        field,
        CanonicalValue::string(value),
    )]))
}

pub(crate) fn display_name(name: &str) -> DisplayName {
    DisplayName::parse(name).expect("fixture display name")
}

/// The typed body of the tenant `tenant` builds, so a test can assert on the
/// body a resource carries without rebuilding the record by hand.
pub(crate) fn tenant_body(seed: u64, name: &str) -> TenantBody {
    TenantBody::new(tenant_id(seed), display_name(name))
}

pub(crate) fn project_body(seed: u64, tenant: u64, name: &str) -> ProjectBody {
    ProjectBody::new(project_id(seed), tenant_id(tenant), display_name(name))
}

/// A tenant whose id is its resource id, named `slug`, displayed capitalized.
///
/// The seed is the tenant's id *and* its resource id, which is the binding
/// [`TenantBody::read`] enforces: one durable object, one identity.
pub(crate) fn tenant(seed: u64, slug: &str) -> ResourceVersion {
    tenant_body(seed, &capitalize(slug)).version(Slug::parse(slug).expect("fixture slug"))
}

/// The tenant `tenant` builds, as a build that predates typed tenancy bodies
/// would have written it: same envelope, an untyped body carrying no schema.
///
/// What a legacy row in a long-lived deployment looks like, so a test can assert
/// that this build refuses it as an *incompatibility* rather than reading it as a
/// typed tenant or reporting it as corruption.
pub(crate) fn legacy_tenant(seed: u64, slug: &str) -> ResourceVersion {
    ResourceVersion::new(
        ResourceRef::new(
            ResourceKind::Tenant,
            ResourceId::new(tenant_id(seed).uuid()),
            ResourceVersionNumber::FIRST,
        ),
        ResourceScope::Deployment,
        Slug::parse(slug).expect("fixture slug"),
        inline("display_name", &capitalize(slug)),
    )
}

pub(crate) fn project(tenant: &TenantId, seed: u64, slug: &str) -> ResourceVersion {
    ProjectBody::new(project_id(seed), *tenant, display_name(&capitalize(slug)))
        .version(Slug::parse(slug).expect("fixture slug"))
}

/// `acme` displayed as `Acme`: a display name is prose, so a fixture's is not
/// its slug.
fn capitalize(slug: &str) -> String {
    let mut characters = slug.chars();
    match characters.next() {
        None => String::new(),
        Some(first) => first.to_ascii_uppercase().to_string() + characters.as_str(),
    }
}

/// The typed body of the credential `credential` builds: a tenant's credential
/// pointing at the first version of the secret seeded by `seed`.
///
/// No material anywhere, in a fixture least of all: what the body carries is an
/// opaque reference, so a test that prints a fixture prints an id.
pub(crate) fn credential_body(tenant: &TenantId, seed: u64, slug: &str) -> ProviderCredentialBody {
    ProviderCredentialBody::staged(
        resource_id(seed),
        SecretOwner::tenant(*tenant),
        provider_id(seed),
        display_name(&capitalize(slug)),
        secret_ref(seed),
    )
}

/// The policy every policy fixture starts from: one cap, no scope-wide cap, and
/// bounded holds. Varying one field of it is how a transition test states exactly
/// what changed.
pub(crate) fn policy_body(scope: PolicyScope, epoch: u64) -> PolicyBody {
    PolicyBody::new(
        scope,
        PolicyEpoch::new(epoch).expect("fixture epoch"),
        BudgetPolicy::new(1_000_000, None, 60).expect("fixture budget policy"),
        ConcurrencyPolicy::new(8, 30).expect("fixture concurrency policy"),
        RevocationPolicy::new(1),
    )
}

pub(crate) fn tenant_policy_body(tenant: u64, epoch: u64) -> PolicyBody {
    policy_body(PolicyScope::Tenant(tenant_id(tenant)), epoch)
}

pub(crate) fn project_policy_body(tenant: u64, project: u64, epoch: u64) -> PolicyBody {
    policy_body(
        PolicyScope::Project {
            tenant: tenant_id(tenant),
            project: project_id(project),
        },
        epoch,
    )
}

/// A tenant's policy document, at the scope it governs.
pub(crate) fn tenant_policy(tenant: u64, epoch: u64) -> ResourceVersion {
    tenant_policy_body(tenant, epoch).version(Slug::parse("limits").expect("fixture slug"))
}

/// A project's own policy document, which overrides its tenant's as a whole.
pub(crate) fn project_policy(tenant: u64, project: u64, epoch: u64) -> ResourceVersion {
    project_policy_body(tenant, project, epoch)
        .version(Slug::parse("limits").expect("fixture slug"))
}

/// [`state`] plus a tenant policy and its project's own policy: the shape an
/// effective-policy test needs, where one scope has a document of its own and
/// another falls back to its tenant's.
pub(crate) fn state_with_policy() -> DesiredState {
    let mut state = state();
    state
        .insert(tenant_policy(1, 1))
        .and_then(|state| state.insert(project_policy(1, 2, 1)))
        .expect("a policy document per scope is a distinct reference");
    state
}

pub(crate) fn credential(tenant: &TenantId, seed: u64, slug: &str) -> ResourceVersion {
    credential_body(tenant, seed, slug).version(Slug::parse(slug).expect("fixture slug"))
}

/// A credential owned by one of a tenant's projects rather than by the tenant.
pub(crate) fn project_credential(
    tenant: &TenantId,
    project: &ProjectId,
    seed: u64,
    slug: &str,
) -> ResourceVersion {
    ProviderCredentialBody::staged(
        resource_id(seed),
        SecretOwner::project(*tenant, *project),
        provider_id(seed),
        display_name(&capitalize(slug)),
        secret_ref(seed),
    )
    .version(Slug::parse(slug).expect("fixture slug"))
}

/// The credential `credential` builds, as a build predating typed credential
/// bodies wrote it: same envelope, an untyped body carrying no schema.
pub(crate) fn legacy_credential(tenant: &TenantId, seed: u64, slug: &str) -> ResourceVersion {
    ResourceVersion::new(
        reference(ResourceKind::ProviderCredential, seed),
        ResourceScope::Tenant(*tenant),
        Slug::parse(slug).expect("fixture slug"),
        inline("secret_ref", slug),
    )
}

/// The provider resource a credential seeded with `seed` authenticates to,
/// declared at `scope`.
///
/// The scope is the caller's because that is the whole point of the cases this
/// serves: reachable at the owner's own scope or its tenant's, foreign anywhere
/// else.
pub(crate) fn provider(seed: u64, scope: ResourceScope, slug: &str) -> ResourceVersion {
    ResourceVersion::new(
        ResourceRef::new(
            ResourceKind::Provider,
            provider_id(seed),
            ResourceVersionNumber::FIRST,
        ),
        scope,
        Slug::parse(slug).expect("fixture slug"),
        inline("wire_family", "openai-chat"),
    )
}

/// An alias inside a project rather than merely inside its tenant.
pub(crate) fn project_alias(
    tenant: &TenantId,
    project: &ProjectId,
    seed: u64,
    slug: &str,
) -> ResourceVersion {
    ResourceVersion::new(
        reference(ResourceKind::Alias, seed),
        ResourceScope::Project {
            tenant: *tenant,
            project: *project,
        },
        Slug::parse(slug).expect("fixture slug"),
        inline("wire_family", "openai-chat"),
    )
}

pub(crate) fn alias(
    tenant: &TenantId,
    seed: u64,
    slug: &str,
    depends_on: &[ResourceRef],
) -> ResourceVersion {
    ResourceVersion::new(
        reference(ResourceKind::Alias, seed),
        ResourceScope::Tenant(*tenant),
        Slug::parse(slug).expect("fixture slug"),
        inline("wire_family", "openai-chat"),
    )
    .depending_on(depends_on.iter().copied())
}

/// A payload large enough that inlining it into every revision would be the
/// wrong design — which is the point of addressing it by content.
pub(crate) fn catalog_payload(seed: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(16_384);
    payload.extend_from_slice(b"{\"models\":[");
    while payload.len() < 16_384 {
        payload.extend_from_slice(seed);
    }
    payload.extend_from_slice(b"]}");
    payload
}

/// A deployment-scoped catalogue resource whose body is a content-addressed
/// snapshot.
pub(crate) fn blob_backed_catalog(seed: u64) -> ResourceVersion {
    ResourceVersion::new(
        reference(ResourceKind::CatalogModel, seed),
        ResourceScope::Deployment,
        Slug::parse("models-dev").expect("fixture slug"),
        ResourceBody::Blob(BlobRef::of(
            BlobKind::CatalogSnapshot,
            &catalog_payload(b"models"),
        )),
    )
}

/// A second content-addressed catalogue snapshot, with its own digest.
///
/// Exists so a test can distinguish a *total* over declared blobs from a partial
/// one: with a single blob the two are equal, and an assertion on the total
/// proves nothing.
pub(crate) fn second_blob_backed_catalog(seed: u64) -> ResourceVersion {
    ResourceVersion::new(
        reference(ResourceKind::CatalogModel, seed),
        ResourceScope::Deployment,
        Slug::parse("embeddings-dev").expect("fixture slug"),
        ResourceBody::Blob(BlobRef::of(
            BlobKind::CatalogSnapshot,
            &catalog_payload(b"embeddings"),
        )),
    )
}

/// [`state`] plus a second blob-backed catalogue, so the state declares two
/// blobs of different sizes.
pub(crate) fn state_with_two_blobs() -> DesiredState {
    let catalog = second_blob_backed_catalog(6);
    let mut state = state();
    state.declare_blob(*catalog.body.blob().expect("a blob body"));
    state.insert(catalog).expect("a distinct reference");
    state
}

/// The catalogue content a fixture price book was approved against.
///
/// A named checksum rather than a real import: what a book records is the
/// *identity*, and a test asserting that the identity survives a round trip must
/// not depend on the models.dev parser to produce it.
pub(crate) fn catalog_content_id() -> CatalogContentId {
    CatalogContentId::from_checksum(Checksum::of(b"fixture catalogue content"))
}

/// The catalogue resource version a fixture price book was approved against.
/// It intentionally differs from the price-book fixture version so propagation
/// tests cannot pass by accidentally reading the book's version.
pub(crate) fn catalog_version() -> ResourceVersionNumber {
    ResourceVersionNumber::new(3).expect("fixture catalogue version is non-zero")
}

pub(crate) fn priced_target(provider: &str, model: &str) -> PricedTarget {
    PricedTarget::new(
        ProviderId::parse(provider).expect("fixture provider id"),
        model,
    )
}

/// A rule at whole micro-dollars, so it converts, in force from `from`.
pub(crate) fn price_rule(
    target: PricedTarget,
    precedence: RulePrecedence,
    effective: EffectiveInterval,
    input_nanos: u64,
    output_nanos: u64,
) -> PriceRule {
    PriceRule::new(
        target,
        precedence,
        effective,
        ApprovedRates::new(
            ApprovedRate::from_nanos(input_nanos),
            ApprovedRate::from_nanos(output_nanos),
        ),
        PriceProvenance::stated(PriceOrigin::Catalogue),
    )
    .expect("fixture rates convert exactly")
}

/// An approved book pricing `openai/gpt-4o` from the epoch onwards.
pub(crate) fn approved_price_book() -> PriceBookBody {
    PriceBookBody::new(
        catalog_content_id(),
        catalog_version(),
        Approval::Approved {
            by: actor(),
            at: EffectiveInstant::EPOCH,
            citation: Some(display_name("CHG-1")),
        },
    )
    .with_rule(price_rule(
        priced_target("openai", "gpt-4o"),
        RulePrecedence::Baseline,
        EffectiveInterval::from(EffectiveInstant::EPOCH),
        2_500_000,
        10_000_000,
    ))
}

/// A price-book resource version, deployment-scoped as its kind's baseline is.
pub(crate) fn price_book(body: &PriceBookBody, seed: u64, slug: &str) -> ResourceVersion {
    body.version(resource_id(seed), Slug::parse(slug).expect("fixture slug"))
}

/// A retained price book this build cannot bill: its rate is finer than a
/// micro-dollar, which is what a newer release metering sub-micro-dollar prices
/// would leave behind. Written directly rather than through [`PriceBookBody`],
/// because the typed body refuses to construct it.
pub(crate) fn unbillable_price_book(seed: u64, slug: &str) -> ResourceVersion {
    let book = approved_price_book();
    let CanonicalValue::Map(mut fields) = book.canonical() else {
        panic!("a body is a record");
    };
    let Some((_, CanonicalValue::Set(rules))) = fields.iter().find(|(name, _)| name == "rules")
    else {
        panic!("a body carries a rule set");
    };
    let CanonicalValue::Map(mut rule) = rules.first().expect("one rule").clone() else {
        panic!("a rule is a record");
    };
    rule.retain(|(name, _)| name != "rates");
    rule.push((
        "rates".to_owned(),
        CanonicalValue::map([
            ("input", CanonicalValue::integer(1_500)),
            ("output", CanonicalValue::integer(1_000)),
        ]),
    ));
    fields.retain(|(name, _)| name != "rules");
    fields.push((
        "rules".to_owned(),
        CanonicalValue::set([CanonicalValue::map(rule)]),
    ));
    ResourceVersion::new(
        reference(ResourceKind::Price, seed),
        ResourceScope::Deployment,
        Slug::parse(slug).expect("fixture slug"),
        ResourceBody::Inline(CanonicalValue::map(fields)),
    )
}

/// [`state`] plus a price book, which is what a deployment that has approved
/// pricing looks like.
pub(crate) fn state_with_price_book(body: &PriceBookBody) -> DesiredState {
    let mut state = state();
    state
        .insert(price_book(body, 7, "baseline"))
        .expect("a distinct reference");
    state
}

/// The pricing a converged snapshot serves under, resolved from
/// [`approved_price_book`] at the epoch. For tests outside the pricing domain
/// that need a published snapshot's pricing without rebuilding a revision.
pub(crate) fn approved_pricing_snapshot() -> PricingSnapshot {
    PriceBooks::of(&state_with_price_book(&approved_price_book()))
        .expect("the fixture book is servable")
        .snapshot_at(EffectiveInstant::EPOCH)
        .expect("the state holds a book")
}

/// A complete, valid desired state: one tenant, a project, a credential, a
/// blob-backed catalogue snapshot, and an alias depending on both the credential
/// and the catalogue.
pub(crate) fn state() -> DesiredState {
    let tenant_id = tenant_id(1);
    let catalog = blob_backed_catalog(5);
    let credential = credential(&tenant_id, 3, "primary");
    let mut state = DesiredState::new();
    state.declare_blob(*catalog.body.blob().expect("a blob body"));
    state
        .insert(tenant(1, "acme"))
        .and_then(|state| state.insert(project(&tenant_id, 2, "core")))
        .and_then(|state| state.insert(credential.clone()))
        .and_then(|state| state.insert(catalog.clone()))
        .and_then(|state| {
            state.insert(alias(
                &tenant_id,
                4,
                "fast",
                &[credential.reference, catalog.reference],
            ))
        })
        .expect("fixture state is valid");
    state
}

/// [`state`], with its tenant row as a build predating typed tenancy bodies
/// wrote it: what a newer build's storage — or its exported cache — looks like to
/// an older one, so a test can drive the incompatibility path from realistic
/// state rather than from one hand-edited row.
pub(crate) fn state_with_legacy_tenant() -> DesiredState {
    let mut state = DesiredState::new();
    state.declare_blob(*blob_backed_catalog(5).body.blob().expect("a blob body"));
    let credential = credential(&tenant_id(1), 3, "primary");
    let catalog = blob_backed_catalog(5);
    state
        .insert(legacy_tenant(1, "acme"))
        .and_then(|state| state.insert(project(&tenant_id(1), 2, "core")))
        .and_then(|state| state.insert(credential.clone()))
        .and_then(|state| state.insert(catalog.clone()))
        .and_then(|state| {
            state.insert(alias(
                &tenant_id(1),
                4,
                "fast",
                &[credential.reference, catalog.reference],
            ))
        })
        .expect("the envelopes are consistent; only the body is untyped");
    state
}

/// A revision shaped the way a build predating typed tenancy could publish one:
/// tenant- and project-scoped resources with no tenant or project row anywhere.
///
/// The exemption in [`Tenancy::of`](super::Tenancy) exists for exactly this
/// state, so a fixture holds it rather than a comment: nothing here names an
/// owner that contradicts anything, so there is nothing to refuse, and what the
/// project scope names is simply unroutable.
pub(crate) fn state_a_pre_tenancy_build_published() -> DesiredState {
    let owner = tenant_id(1);
    let credential = credential(&owner, 23, "legacy-primary");
    let mut state = DesiredState::new();
    state
        .insert(credential.clone())
        .and_then(|state| state.insert(alias(&owner, 24, "legacy-fast", &[credential.reference])))
        .and_then(|state| state.insert(project_alias(&owner, &project_id(2), 26, "legacy-inner")))
        .expect("a revision without its owner rows is valid desired state");
    state
}

/// [`state`], with its credential rotated onto the next version of its secret.
///
/// What a rotation is as desired state: the same credential identity, a new
/// resource version, and a *different* exact secret reference — which is why
/// publishing this revision has to leave the previous version's material alive
/// for whatever is still serving the previous revision.
pub(crate) fn state_with_rotated_credential() -> DesiredState {
    let tenant_id = tenant_id(1);
    let catalog = blob_backed_catalog(5);
    let credential = credential_body(&tenant_id, 3, "primary")
        .rotated()
        .version_at(
            Slug::parse("primary").expect("fixture slug"),
            ResourceVersionNumber::FIRST.next(),
        );
    let mut state = DesiredState::new();
    state.declare_blob(*catalog.body.blob().expect("a blob body"));
    state
        .insert(tenant(1, "acme"))
        .and_then(|state| state.insert(project(&tenant_id, 2, "core")))
        .and_then(|state| state.insert(credential.clone()))
        .and_then(|state| state.insert(catalog.clone()))
        .and_then(|state| {
            // A new version of the alias too: its dependency now names the
            // credential's new version, and a resource version is immutable.
            state.insert(
                ResourceVersion::new(
                    reference(ResourceKind::Alias, 4).at(ResourceVersionNumber::FIRST.next()),
                    ResourceScope::Tenant(tenant_id),
                    Slug::parse("fast").expect("fixture slug"),
                    inline("wire_family", "openai-chat"),
                )
                .depending_on([credential.reference, catalog.reference]),
            )
        })
        .expect("fixture state is valid");
    state
}

/// A second valid state that shares the catalogue blob with [`state`] — what a
/// revision that changes one alias looks like.
pub(crate) fn state_with_renamed_alias() -> DesiredState {
    let tenant_id = tenant_id(1);
    let catalog = blob_backed_catalog(5);
    let credential = credential(&tenant_id, 3, "primary");
    let mut state = DesiredState::new();
    state.declare_blob(*catalog.body.blob().expect("a blob body"));
    state
        .insert(tenant(1, "acme"))
        .and_then(|state| state.insert(project(&tenant_id, 2, "core")))
        .and_then(|state| state.insert(credential.clone()))
        .and_then(|state| state.insert(catalog.clone()))
        .and_then(|state| {
            // A new version of the same alias: identity is stable, content is not.
            state.insert(
                ResourceVersion::new(
                    reference(ResourceKind::Alias, 4).at(ResourceVersionNumber::FIRST.next()),
                    ResourceScope::Tenant(tenant_id),
                    Slug::parse("quick").expect("fixture slug"),
                    inline("wire_family", "openai-chat"),
                )
                .depending_on([credential.reference, catalog.reference]),
            )
        })
        .expect("fixture state is valid");
    state
}

/// A second tenant's credential, for the isolation cases: two tenants' resources
/// coexist in one revision, and nothing in either may reference the other.
pub(crate) fn other_tenant_credential() -> ResourceVersion {
    credential(&tenant_id(11), 13, "secondary")
}

/// A valid state holding two tenants' resources, with no reference between them.
///
/// The starting point for the isolation tests: a cross-tenant edge is then
/// something storage is *made* to hold, so what is being tested is hydration
/// refusing it rather than the domain refusing to build it.
pub(crate) fn state_with_second_tenant() -> DesiredState {
    let other = other_tenant_credential();
    let mut state = state();
    state
        .insert(tenant(11, "globex"))
        .and_then(|state| state.insert(other.clone()))
        .and_then(|state| state.insert(alias(&tenant_id(11), 14, "steady", &[other.reference])))
        .expect("two tenants that reference nothing of each other's are valid");
    state
}

pub(crate) fn principal_id(seed: u64) -> PrincipalId {
    PrincipalId::new(uuid(seed))
}

/// A human principal: an OIDC subject at one issuer, with the roles given.
///
/// The seed is the principal id *and* the resource id, the binding
/// [`IdentityBody::read`] enforces.
pub(crate) fn human(
    seed: u64,
    subject: &str,
    scope: ResourceScope,
    roles: &[Role],
) -> ResourceVersion {
    identity(
        seed,
        subject,
        scope,
        roles,
        Credential::Oidc {
            issuer: "https://idp.example".to_owned(),
            subject: subject.to_owned(),
        },
    )
}

/// A workload key of the shape [`WorkloadKey::parse`] accepts, seeded so it is
/// the same string in every run.
///
/// Deterministic rather than minted, because a test that asserts on a stored
/// digest cannot use fresh randomness — and because a fixture must not need the
/// system CSPRNG to build a state.
pub(crate) fn workload_key(seed: u8) -> String {
    let mut key = String::from(WorkloadKey::PREFIX);
    for _ in 0..32 {
        key.push_str(&format!("{seed:02x}"));
    }
    key
}

/// A workload principal whose key digest is the digest of `key`, or which has no
/// key at all — a workload whose key was revoked and not replaced.
pub(crate) fn workload(
    seed: u64,
    slug: &str,
    scope: ResourceScope,
    roles: &[Role],
    key: Option<&str>,
) -> ResourceVersion {
    identity(
        seed,
        slug,
        scope,
        roles,
        Credential::MintedKey {
            digest: key.map(|key| Checksum::of(key.as_bytes())),
        },
    )
}

fn identity(
    seed: u64,
    slug: &str,
    scope: ResourceScope,
    roles: &[Role],
    credential: Credential,
) -> ResourceVersion {
    IdentityBody::new(
        principal_id(seed),
        display_name(&capitalize(slug)),
        credential,
        roles.iter().copied(),
    )
    .expect("fixture identities grant a role")
    .version(scope, Slug::parse(slug).expect("fixture slug"))
}

/// [`state`] plus a directory: a platform administrator, a tenant administrator,
/// an operator scoped into one project, and a workload of the tenant.
///
/// One state that every authorization case can be decided against, so a test
/// asserts on the decision rather than on a hand-built directory.
pub(crate) fn state_with_directory() -> DesiredState {
    directory_state(true)
}

/// [`state_with_directory`] with the workload no longer declared: what publishing
/// a revocation looks like, since a revision is the whole directory rather than a
/// change to it.
pub(crate) fn state_with_revoked_workload() -> DesiredState {
    directory_state(false)
}

fn directory_state(with_workload: bool) -> DesiredState {
    let tenant = tenant_id(1);
    let project = project_id(2);
    let mut state = state();
    state
        .insert(human(
            30,
            "root",
            ResourceScope::Deployment,
            &[Role::PlatformAdmin],
        ))
        .and_then(|state| {
            state.insert(human(
                31,
                "admin",
                ResourceScope::Tenant(tenant),
                &[Role::TenantAdmin],
            ))
        })
        .and_then(|state| {
            state.insert(human(
                32,
                "dev",
                ResourceScope::Project { tenant, project },
                &[Role::Developer],
            ))
        })
        .expect("a directory over declared tenants and projects is valid");
    if with_workload {
        state
            .insert(workload(
                33,
                "deployer",
                ResourceScope::Tenant(tenant),
                &[Role::Operator],
                Some(&workload_key(0xd0)),
            ))
            .expect("a workload of a declared tenant is valid");
    }
    state
}

/// Two tenants, each with its own administrator and its own project.
///
/// What an isolation case needs that [`state_with_directory`] cannot give it: a
/// second tenant's principals and grants to *fail* to see. Neither tenant's
/// resources reference the other's, so the state is valid for the same reason
/// [`state_with_second_tenant`] is.
pub(crate) fn two_tenant_directory_state() -> DesiredState {
    let other = tenant_id(11);
    let mut state = state_with_directory();
    state
        .insert(tenant(11, "globex"))
        .and_then(|state| state.insert(project(&other, 12, "core")))
        .and_then(|state| {
            state.insert(human(
                40,
                "their-admin",
                ResourceScope::Tenant(other),
                &[Role::TenantAdmin],
            ))
        })
        .and_then(|state| {
            state.insert(workload(
                41,
                "their-deployer",
                ResourceScope::Tenant(other),
                &[Role::Operator],
                Some(&workload_key(0xe1)),
            ))
        })
        .expect("two tenants that reference nothing of each other's are valid");
    state
}

/// A chain of `depth` aliases, each depending on the next.
///
/// Nesting a hydration bound can be stated against: the chain is linear, so its
/// depth is `depth - 1` edges below the first alias, and no other fixture
/// property varies with it.
pub(crate) fn deep_chain_state(depth: u64) -> DesiredState {
    let owner = tenant_id(1);
    let mut state = DesiredState::new();
    state.insert(tenant(1, "acme")).expect("a fresh state");
    for step in 0..depth {
        let seed = 100 + step;
        let depends_on: Vec<ResourceRef> = if step + 1 < depth {
            vec![reference(ResourceKind::Alias, seed + 1)]
        } else {
            Vec::new()
        };
        state
            .insert(alias(&owner, seed, &format!("step-{step}"), &depends_on))
            .expect("distinct references");
    }
    state
}

/// The identity of an offering a catalogue lists, derived the way an importer
/// derives it: from the provider and model identifiers, never from a display name.
pub(crate) fn offering_id(model: &str) -> OfferingId {
    OfferingId::of("openai", model).expect("fixture identifiers are encodable")
}

/// The snapshot digest [`blob_backed_catalog`] carries, which is what an
/// enablement pins.
pub(crate) fn catalog_snapshot() -> Checksum {
    blob_backed_catalog(5)
        .body
        .blob()
        .expect("a blob body")
        .digest
}

pub(crate) fn catalog_offering(model: &str) -> CatalogOffering {
    CatalogOffering::new(offering_id(model), catalog_snapshot())
}

/// A typed enablement body owned by `owner`, of the offering `model`.
pub(crate) fn enablement_body(seed: u64, owner: ModelOwner, model: &str) -> ModelEnablementBody {
    ModelEnablementBody::new(
        resource_id(seed),
        owner,
        catalog_offering(model),
        WireFamily::OpenaiChat,
    )
}

/// A tenant-default enablement: every project of the tenant sees it unless one
/// overrides it.
pub(crate) fn tenant_enablement(tenant: &TenantId, seed: u64, model: &str) -> ResourceVersion {
    enablement_body(seed, ModelOwner::tenant(*tenant), model).version(
        Slug::parse(model).expect("fixture slug"),
        catalog_reference(),
    )
}

/// A project override of the same offering: the enablement one project gets
/// instead of its tenant's default.
pub(crate) fn project_enablement(
    tenant: &TenantId,
    project: &ProjectId,
    seed: u64,
    model: &str,
) -> ResourceVersion {
    enablement_body(seed, ModelOwner::project(*tenant, *project), model).version(
        Slug::parse(model).expect("fixture slug"),
        catalog_reference(),
    )
}

/// The catalogue resource version every fixture enablement pins.
pub(crate) fn catalog_reference() -> ResourceRef {
    blob_backed_catalog(5).reference
}

/// A typed project alias resolving to `targets`, in the order given.
pub(crate) fn typed_alias(
    tenant: &TenantId,
    project: &ProjectId,
    seed: u64,
    slug: &str,
    targets: &[ResourceRef],
) -> ResourceVersion {
    alias_body(tenant, project, seed, targets).version(Slug::parse(slug).expect("fixture slug"))
}

pub(crate) fn alias_body(
    tenant: &TenantId,
    project: &ProjectId,
    seed: u64,
    targets: &[ResourceRef],
) -> ModelAliasBody {
    ModelAliasBody::new(
        resource_id(seed),
        *tenant,
        *project,
        WireFamily::OpenaiChat,
        targets
            .iter()
            .map(|target| AliasTarget::new(target.id, target.version)),
    )
}

/// A rate an upstream publishes: recorded, and never billed against.
pub(crate) fn observed_price() -> ObservedPrice {
    ObservedPrice::new(2_500_000, 10_000_000)
}

/// A reference to the exact price version an operator approved.
pub(crate) fn approved_price(seed: u64) -> ApprovedPrice {
    ApprovedPrice::version(resource_id(seed), ResourceVersionNumber::FIRST)
}

/// A price resource an approval points at.
///
/// Untyped: the price body schema is the pricing slice's to define (#201), and
/// this slice only needs a `Price` row that exists, is owned, and is referenced.
pub(crate) fn price(tenant: &TenantId, seed: u64, slug: &str) -> ResourceVersion {
    ResourceVersion::new(
        reference(ResourceKind::Price, seed),
        ResourceScope::Tenant(*tenant),
        Slug::parse(slug).expect("fixture slug"),
        inline("micros_per_million", "2500000"),
    )
}

/// A valid state carrying the typed model contracts: a tenant default, a project
/// override of the same offering, and a project alias resolving to both in
/// priority order.
///
/// The catalogue snapshot both enablements pin is declared once and shared, which
/// is the pinning rule stated as state rather than as a comment.
pub(crate) fn state_with_models() -> DesiredState {
    let tenant_id = tenant_id(1);
    let project_id = project_id(2);
    let catalog = blob_backed_catalog(5);
    let default = tenant_enablement(&tenant_id, 30, "gpt-4o");
    let over = project_enablement(&tenant_id, &project_id, 31, "gpt-4o");
    let mut state = DesiredState::new();
    state.declare_blob(*catalog.body.blob().expect("a blob body"));
    state
        .insert(tenant(1, "acme"))
        .and_then(|state| state.insert(project(&tenant_id, 2, "core")))
        .and_then(|state| state.insert(catalog.clone()))
        .and_then(|state| state.insert(default.clone()))
        .and_then(|state| state.insert(over.clone()))
        .and_then(|state| {
            state.insert(typed_alias(
                &tenant_id,
                &project_id,
                32,
                "fast",
                &[over.reference, default.reference],
            ))
        })
        .expect("fixture state is valid");
    state
}

pub(crate) fn actor() -> Actor {
    Actor::Human {
        issuer: "https://idp.example".to_owned(),
        subject: "u-1".to_owned(),
    }
}

/// A candidate carrying `state`, attributed to a human, keyed by `key`.
///
/// The mutation and audit ids are derived from the key so two candidates differ
/// exactly when their key or their state differs.
pub(crate) fn candidate(
    expected: ExpectedRevision,
    key: &str,
    state: DesiredState,
) -> RevisionCandidate {
    let seed = u64::from(key.bytes().fold(0u32, |seed, byte| {
        seed.wrapping_mul(31).wrapping_add(u32::from(byte))
    }));
    let mutation = MutationId::new(uuid(seed));
    RevisionCandidate {
        expected,
        state,
        legacy_aliases: Default::default(),
        mutation: Mutation {
            id: mutation,
            actor: actor(),
            kind: MutationKind::Update,
            scope: ResourceScope::Tenant(tenant_id(1)),
            idempotency_key: IdempotencyKey::parse(key).expect("fixture key"),
            submitted_at: SystemTime::UNIX_EPOCH + Duration::from_secs(seed),
        },
        audit: AuditEvent {
            id: AuditEventId::new(uuid(seed + 1)),
            mutation,
            actor: actor(),
            kind: MutationKind::Update,
            target: Some(reference(ResourceKind::Alias, 4)),
            summary: format!("applied {key}"),
            recorded_at: SystemTime::UNIX_EPOCH + Duration::from_secs(seed),
        },
    }
}
