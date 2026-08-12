//! Deterministic fixtures the domain and contract tests share.
//!
//! Every id here is built from [`Uuid7::from_parts`], so no test depends on a
//! clock: the same seed is the same id in every run, which is what lets a test
//! assert on exact canonical bytes and exact error payloads.

use std::time::{Duration, SystemTime};

use super::canonical::CanonicalValue;
use super::ids::{
    AuditEventId, MutationId, ProjectId, ResourceId, RevisionId, Slug, TenantId, Uuid7,
};
use super::mutation::{
    Actor, AuditEvent, ExpectedRevision, IdempotencyKey, Mutation, MutationKind,
};
use super::resource::{
    BlobKind, BlobRef, ResourceBody, ResourceKind, ResourceRef, ResourceScope, ResourceVersion,
    ResourceVersionNumber,
};
use super::revision::{DesiredState, RevisionCandidate};
use super::tenancy::{DisplayName, ProjectBody, TenantBody};

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

pub(crate) fn credential(tenant: &TenantId, seed: u64, slug: &str) -> ResourceVersion {
    ResourceVersion::new(
        reference(ResourceKind::ProviderCredential, seed),
        ResourceScope::Tenant(*tenant),
        Slug::parse(slug).expect("fixture slug"),
        inline("secret_ref", slug),
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
