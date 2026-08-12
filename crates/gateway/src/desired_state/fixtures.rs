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

pub(crate) fn tenant(seed: u64, slug: &str) -> ResourceVersion {
    ResourceVersion::new(
        reference(ResourceKind::Tenant, seed),
        ResourceScope::Deployment,
        Slug::parse(slug).expect("fixture slug"),
        inline("display_name", slug),
    )
}

pub(crate) fn project(tenant: &TenantId, seed: u64, slug: &str) -> ResourceVersion {
    ResourceVersion::new(
        reference(ResourceKind::Project, seed),
        ResourceScope::Tenant(*tenant),
        Slug::parse(slug).expect("fixture slug"),
        inline("display_name", slug),
    )
}

pub(crate) fn credential(tenant: &TenantId, seed: u64, slug: &str) -> ResourceVersion {
    ResourceVersion::new(
        reference(ResourceKind::ProviderCredential, seed),
        ResourceScope::Tenant(*tenant),
        Slug::parse(slug).expect("fixture slug"),
        inline("secret_ref", slug),
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
