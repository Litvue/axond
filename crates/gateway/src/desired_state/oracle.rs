//! The in-memory contract oracle: what every `ControlPlaneStore` must do.
//!
//! This implementation exists to *define* behaviour, not to be deployed — an
//! in-memory control plane is not a selectable backend, because
//! [`ControlPlaneBackend`](crate::backends::control_plane::ControlPlaneBackend)
//! has no in-memory variant. It is test-only, which also keeps the Tier 0
//! hermetic gate hermetic (ADR 0018).
//!
//! What it is precise about is the part #165 has to reproduce in SQL:
//!
//! - **Revisions are immutable, and so are resource versions.** Resource
//!   versions are stored once, keyed by `(kind, id, version)`, and shared by every
//!   revision that references them; republishing a version with different content
//!   is refused rather than accepted as an update. So a manifest is a reference
//!   structure in storage too, not only in the domain, and a catalogue snapshot
//!   is stored once no matter how many revisions pin it.
//! - **A publication is one critical section.** The manifest, the resource
//!   versions, the blob references, the audit event, and the idempotency record
//!   become visible together or not at all. A mutex is the fake's transaction;
//!   #165's is a transaction.
//! - **Expectation and idempotency are checked in that same section**, in the
//!   order a durable store must use: idempotent replay first (so a retry of a
//!   now-stale candidate replays instead of conflicting), then the expected
//!   revision.
//! - **A load verifies.** Hydration reassembles the state from the stored
//!   versions and returns a [`LoadedRevision`], so anything that does not add up
//!   surfaces as `Corrupt` at the boundary instead of becoming a snapshot.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;

use async_trait::async_trait;

use super::canonical::Checksum;
use super::ids::{RevisionId, Uuid7Generator};
use super::mutation::{AuditEvent, IdempotencyKey};
use super::resource::{ResourceRef, ResourceVersion};
use super::revision::{
    DesiredState, IntegrityError, LoadedRevision, RevisionCandidate, RevisionManifest,
};
use crate::backends::control_plane::{ControlPlaneError, ControlPlaneStore};
use crate::backends::{Capabilities, Capability};

#[derive(Default)]
struct Storage {
    /// Publication order. Also the answer to "which revision is newest".
    order: Vec<RevisionId>,
    manifests: BTreeMap<RevisionId, RevisionManifest>,
    /// Every resource version ever published, shared across revisions.
    versions: BTreeMap<ResourceRef, ResourceVersion>,
    audit: BTreeMap<RevisionId, Vec<AuditEvent>>,
    /// The revision a key published, plus the checksum of the state it
    /// published, so a reused key can be told apart from a retried one.
    ///
    /// One unscoped, never-expiring namespace, which is adequate for a
    /// single-caller test double and is *not* the contract: per-caller scoping
    /// and expiry are required of a durable store, per [`IdempotencyKey`].
    applied: BTreeMap<IdempotencyKey, (RevisionId, Checksum)>,
}

/// A `ControlPlaneStore` whose transaction is a mutex.
pub(crate) struct InMemoryControlPlane {
    ids: Uuid7Generator,
    storage: Mutex<Storage>,
    unavailable: AtomicBool,
}

impl InMemoryControlPlane {
    pub(crate) fn new() -> Self {
        Self {
            ids: Uuid7Generator::new(),
            storage: Mutex::new(Storage::default()),
            unavailable: AtomicBool::new(false),
        }
    }

    pub(crate) fn set_unavailable(&self, unavailable: bool) {
        self.unavailable.store(unavailable, Ordering::Relaxed);
    }

    pub(crate) fn published_revisions(&self) -> usize {
        self.locked().order.len()
    }

    /// How many distinct resource versions storage holds, which is what proves
    /// revisions share versions instead of copying them.
    pub(crate) fn stored_versions(&self) -> usize {
        self.locked().versions.len()
    }

    /// Drop a stored resource version, as a partially restored backup would.
    pub(crate) fn forget_version(&self, reference: &ResourceRef) {
        self.locked().versions.remove(reference);
    }

    /// Rewrite a manifest's recorded checksum, as a corrupted column would.
    pub(crate) fn corrupt_checksum(&self, id: RevisionId, checksum: Checksum) {
        if let Some(manifest) = self.locked().manifests.get_mut(&id) {
            manifest.checksum = checksum;
        }
    }

    fn locked(&self) -> std::sync::MutexGuard<'_, Storage> {
        self.storage.lock().expect("not poisoned")
    }

    fn outage(&self) -> Option<ControlPlaneError> {
        self.unavailable
            .load(Ordering::Relaxed)
            .then(|| ControlPlaneError::Unavailable {
                backend: "in-memory",
                message: "fake control plane is unavailable".to_owned(),
            })
    }
}

#[async_trait]
impl ControlPlaneStore for InMemoryControlPlane {
    fn name(&self) -> &'static str {
        "in-memory"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::new(&[
            Capability::TransactionalWrites,
            Capability::OptimisticConcurrency,
            Capability::IdempotentWrites,
            Capability::TransactionalAudit,
        ])
    }

    async fn health(&self) -> Result<(), ControlPlaneError> {
        match self.outage() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn desired_revision(&self) -> Result<Option<RevisionId>, ControlPlaneError> {
        if let Some(error) = self.outage() {
            return Err(error);
        }
        Ok(self.locked().order.last().copied())
    }

    async fn load_manifest(&self, id: RevisionId) -> Result<RevisionManifest, ControlPlaneError> {
        if let Some(error) = self.outage() {
            return Err(error);
        }
        self.locked()
            .manifests
            .get(&id)
            .cloned()
            .ok_or(ControlPlaneError::RevisionNotFound(id))
    }

    async fn load_revision(&self, id: RevisionId) -> Result<LoadedRevision, ControlPlaneError> {
        if let Some(error) = self.outage() {
            return Err(error);
        }
        let storage = self.locked();
        let manifest = storage
            .manifests
            .get(&id)
            .cloned()
            .ok_or(ControlPlaneError::RevisionNotFound(id))?;

        let mut state = DesiredState::new();
        for blob in &manifest.blobs {
            state.declare_blob(*blob);
        }
        for reference in manifest.references() {
            let version = storage.versions.get(&reference).cloned().ok_or_else(|| {
                ControlPlaneError::corrupt(id, IntegrityError::MissingResource { reference })
            })?;
            state
                .insert(version)
                .map_err(|source| ControlPlaneError::corrupt(id, IntegrityError::from(source)))?;
        }
        LoadedRevision::assemble(manifest, state)
            .map_err(|source| ControlPlaneError::corrupt(id, source))
    }

    async fn publish_revision(
        &self,
        candidate: RevisionCandidate,
    ) -> Result<RevisionManifest, ControlPlaneError> {
        if let Some(error) = self.outage() {
            return Err(error);
        }
        // Validation is domain work and happens before the store commits to
        // anything, so a rejected candidate leaves no trace.
        let checksum = candidate.validated_checksum()?;

        let mut storage = self.locked();

        if let Some((published, applied)) = storage
            .applied
            .get(&candidate.mutation.idempotency_key)
            .copied()
        {
            if applied != checksum {
                return Err(ControlPlaneError::IdempotencyKeyReused {
                    key: candidate.mutation.idempotency_key,
                    published,
                });
            }
            return storage
                .manifests
                .get(&published)
                .cloned()
                .ok_or(ControlPlaneError::RevisionNotFound(published));
        }

        let newest = storage.order.last().copied();
        if !candidate.expected.matches(newest) {
            return Err(ControlPlaneError::Conflict {
                expected: candidate.expected,
                actual: newest,
            });
        }

        // Versions are immutable: a reference that already exists must name
        // byte-identical content, or the caller is redefining state an earlier
        // revision still pins.
        for resource in candidate.state.resources() {
            if let Some(stored) = storage.versions.get(&resource.reference)
                && stored != resource
            {
                return Err(ControlPlaneError::ImmutableResourceVersion {
                    reference: resource.reference,
                });
            }
        }

        let id = RevisionId::new(self.ids.next());
        let manifest = RevisionManifest::of(id, newest, SystemTime::now(), &candidate)?;

        // One critical section: versions, manifest, audit, and the idempotency
        // record become visible together or not at all.
        for resource in candidate.state.resources() {
            storage
                .versions
                .insert(resource.reference, resource.clone());
        }
        storage.manifests.insert(id, manifest.clone());
        storage.order.push(id);
        storage.audit.insert(id, vec![candidate.audit]);
        storage
            .applied
            .insert(candidate.mutation.idempotency_key, (id, checksum));
        Ok(manifest)
    }

    async fn audit_trail(&self, id: RevisionId) -> Result<Vec<AuditEvent>, ControlPlaneError> {
        if let Some(error) = self.outage() {
            return Err(error);
        }
        self.locked()
            .audit
            .get(&id)
            .cloned()
            .ok_or(ControlPlaneError::RevisionNotFound(id))
    }
}

#[cfg(test)]
mod tests {
    use super::super::canonical::CanonicalValue;
    use super::super::fixtures::{
        DESIRED_STATE_RESOURCES, alias, candidate, reference, revision_id, state,
        state_with_renamed_alias, tenant_id,
    };
    use super::super::mutation::{Actor, ExpectedRevision};
    use super::super::resource::{ResourceBody, ResourceKind};
    use super::super::revision::ValidationError;
    use super::*;
    use crate::backends::{BackendFailure, FailureCategory};

    #[tokio::test]
    async fn publication_is_a_chain_of_immutable_revisions() {
        let store = InMemoryControlPlane::new();
        assert_eq!(store.desired_revision().await.unwrap(), None);

        let first = store
            .publish_revision(candidate(ExpectedRevision::Empty, "first", state()))
            .await
            .expect("first publication");
        assert_eq!(first.parent, None);
        assert_eq!(store.desired_revision().await.unwrap(), Some(first.id));

        let second = store
            .publish_revision(candidate(
                ExpectedRevision::Exactly(first.id),
                "second",
                state_with_renamed_alias(),
            ))
            .await
            .expect("second publication");
        assert_eq!(second.parent, Some(first.id));
        assert!(
            second.id > first.id,
            "revision ids are time-ordered, so the chain sorts"
        );
        assert_ne!(second.checksum, first.checksum);

        // The earlier revision is unchanged by the later one, and still hydrates.
        assert_eq!(store.load_manifest(first.id).await.unwrap(), first);
        let loaded = store.load_revision(first.id).await.unwrap();
        assert_eq!(loaded.manifest(), &first);
        assert_eq!(loaded.state().len(), DESIRED_STATE_RESOURCES);
    }

    #[tokio::test]
    async fn revisions_share_resource_versions_and_blobs_instead_of_copying_them() {
        let store = InMemoryControlPlane::new();
        let first = store
            .publish_revision(candidate(ExpectedRevision::Empty, "first", state()))
            .await
            .unwrap();
        assert_eq!(store.stored_versions(), DESIRED_STATE_RESOURCES);

        let second = store
            .publish_revision(candidate(
                ExpectedRevision::Exactly(first.id),
                "second",
                state_with_renamed_alias(),
            ))
            .await
            .unwrap();

        // The second revision changed one resource, so storage grew by exactly
        // one version — the other four, including the blob-backed catalogue, are
        // shared.
        assert_eq!(store.stored_versions(), DESIRED_STATE_RESOURCES + 1);
        assert_eq!(
            first.blobs, second.blobs,
            "the snapshot digest is unchanged"
        );
        assert_eq!(
            store
                .load_revision(first.id)
                .await
                .unwrap()
                .state()
                .blobs()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn a_stale_expected_revision_conflicts_instead_of_overwriting() {
        let store = InMemoryControlPlane::new();
        let first = store
            .publish_revision(candidate(ExpectedRevision::Empty, "first", state()))
            .await
            .unwrap();

        let error = store
            .publish_revision(candidate(
                ExpectedRevision::Empty,
                "racing",
                state_with_renamed_alias(),
            ))
            .await
            .expect_err("a stale expectation must not publish");
        assert_eq!(
            error,
            ControlPlaneError::Conflict {
                expected: ExpectedRevision::Empty,
                actual: Some(first.id),
            }
        );
        assert_eq!(error.category(), FailureCategory::Conflict);
        assert!(
            !error.retryable(),
            "a conflicting write is rebuilt, not replayed"
        );
        assert_eq!(store.desired_revision().await.unwrap(), Some(first.id));
        assert_eq!(store.published_revisions(), 1);

        // Expecting a revision that is not the newest conflicts too, and the
        // error names what is actually current so the caller can re-read it.
        let error = store
            .publish_revision(candidate(
                ExpectedRevision::Exactly(revision_id(1)),
                "guessing",
                state_with_renamed_alias(),
            ))
            .await
            .expect_err("a wrong expectation must not publish");
        assert_eq!(
            error,
            ControlPlaneError::Conflict {
                expected: ExpectedRevision::Exactly(revision_id(1)),
                actual: Some(first.id),
            }
        );
    }

    #[tokio::test]
    async fn a_retried_publication_applies_once() {
        let store = InMemoryControlPlane::new();
        let candidate = candidate(ExpectedRevision::Empty, "first", state());
        let first = store.publish_revision(candidate.clone()).await.unwrap();
        let retried = store
            .publish_revision(candidate)
            .await
            .expect("a retry replays the original outcome");
        assert_eq!(first, retried);
        assert_eq!(store.published_revisions(), 1);
        assert_eq!(store.stored_versions(), DESIRED_STATE_RESOURCES);
    }

    #[tokio::test]
    async fn a_replay_survives_a_moved_expectation() {
        let store = InMemoryControlPlane::new();
        let first = store
            .publish_revision(candidate(ExpectedRevision::Empty, "first", state()))
            .await
            .unwrap();
        store
            .publish_revision(candidate(
                ExpectedRevision::Exactly(first.id),
                "second",
                state_with_renamed_alias(),
            ))
            .await
            .unwrap();

        // The original candidate's expectation is now stale, but its key and
        // desired state are unchanged: a retry replays rather than conflicts,
        // which is what makes a client's retry after a lost response safe.
        let replayed = store
            .publish_revision(candidate(ExpectedRevision::Empty, "first", state()))
            .await
            .expect("an unchanged retry replays its own outcome");
        assert_eq!(replayed, first);
        assert_eq!(store.published_revisions(), 2);
    }

    #[tokio::test]
    async fn a_reused_key_carrying_different_state_is_refused() {
        let store = InMemoryControlPlane::new();
        let first = store
            .publish_revision(candidate(ExpectedRevision::Empty, "first", state()))
            .await
            .unwrap();

        // Same key, different desired state: replaying `first` would report a
        // change that was never published.
        let error = store
            .publish_revision(candidate(
                ExpectedRevision::Exactly(first.id),
                "first",
                state_with_renamed_alias(),
            ))
            .await
            .expect_err("a reused key must not replay a different revision");
        assert!(matches!(
            error,
            ControlPlaneError::IdempotencyKeyReused { published, .. } if published == first.id
        ));
        assert_eq!(error.category(), FailureCategory::Invalid);
        assert!(!error.retryable());
        assert_eq!(store.published_revisions(), 1);
        assert_eq!(store.desired_revision().await.unwrap(), Some(first.id));
    }

    #[tokio::test]
    async fn a_resource_version_cannot_be_redefined() {
        let store = InMemoryControlPlane::new();
        let first = store
            .publish_revision(candidate(ExpectedRevision::Empty, "first", state()))
            .await
            .unwrap();

        // The same alias reference with a different body: an earlier revision
        // still pins that reference, so accepting this would mutate history.
        let tenant = tenant_id(1);
        let mut redefined = DesiredState::new();
        for resource in state().resources() {
            let mut resource = resource.clone();
            if resource.reference.kind == ResourceKind::Alias {
                resource.body = ResourceBody::Inline(CanonicalValue::string("redefined"));
            }
            redefined.insert(resource).unwrap();
        }
        for blob in state().blobs() {
            redefined.declare_blob(*blob);
        }
        let error = store
            .publish_revision(candidate(
                ExpectedRevision::Exactly(first.id),
                "redefine",
                redefined,
            ))
            .await
            .expect_err("a published version is immutable");
        assert!(matches!(
            error,
            ControlPlaneError::ImmutableResourceVersion { reference }
                if reference == reference_of_alias()
        ));
        assert_eq!(error.category(), FailureCategory::Invalid);
        assert_eq!(store.published_revisions(), 1);
        assert_eq!(store.stored_versions(), DESIRED_STATE_RESOURCES);

        // A *new version* of the same resource is the supported way to change it.
        let mut next = state();
        next.insert(alias(&tenant, 7, "spare", &[])).unwrap();
        store
            .publish_revision(candidate(
                ExpectedRevision::Exactly(first.id),
                "add-alias",
                next,
            ))
            .await
            .expect("new versions are how state changes");
    }

    fn reference_of_alias() -> ResourceRef {
        reference(ResourceKind::Alias, 4)
    }

    #[tokio::test]
    async fn an_invalid_candidate_leaves_no_trace() {
        let store = InMemoryControlPlane::new();
        let tenant = tenant_id(1);
        let missing = reference(ResourceKind::ProviderCredential, 99);
        let mut dangling = DesiredState::new();
        dangling
            .insert(super::super::fixtures::tenant(1, "acme"))
            .unwrap();
        dangling
            .insert(alias(&tenant, 2, "fast", &[missing]))
            .unwrap();

        let error = store
            .publish_revision(candidate(ExpectedRevision::Empty, "dangling", dangling))
            .await
            .expect_err("a dangling reference must not publish");
        assert_eq!(error.category(), FailureCategory::Invalid);
        assert!(matches!(
            error,
            ControlPlaneError::Invalid(ValidationError::DanglingResourceReference { .. })
        ));
        assert_eq!(store.desired_revision().await.unwrap(), None);
        assert_eq!(store.published_revisions(), 0);
        assert_eq!(store.stored_versions(), 0);

        let error = store
            .publish_revision(candidate(
                ExpectedRevision::Empty,
                "empty",
                DesiredState::new(),
            ))
            .await
            .expect_err("an empty candidate is invalid");
        assert_eq!(
            error,
            ControlPlaneError::Invalid(ValidationError::Empty),
            "the refusal names the rule, not just `invalid`"
        );
    }

    #[tokio::test]
    async fn audit_is_written_with_the_mutation() {
        let store = InMemoryControlPlane::new();
        let candidate = candidate(ExpectedRevision::Empty, "first", state());
        let expected = candidate.audit.clone();
        let revision = store.publish_revision(candidate).await.unwrap();

        assert_eq!(
            store.audit_trail(revision.id).await.unwrap(),
            vec![expected]
        );
        assert_eq!(
            store.audit_trail(revision.id).await.unwrap()[0].mutation,
            revision.mutation,
            "the audit event names the mutation the manifest records"
        );
        assert!(matches!(
            store.audit_trail(revision_id(1)).await,
            Err(ControlPlaneError::RevisionNotFound(_))
        ));
    }

    #[tokio::test]
    async fn an_audit_actor_round_trips_from_owned_data() {
        let store = InMemoryControlPlane::new();
        // What a durable store has when it reads an audit row back: owned bytes
        // with no static lifetime available to borrow from.
        let read_back = |column: &str| Actor::System {
            component: column.to_string(),
        };
        let mut candidate = candidate(ExpectedRevision::Empty, "refresh", state());
        candidate.audit.actor = read_back(&String::from("catalog-refresh"));

        let revision = store.publish_revision(candidate).await.unwrap();
        let trail = store.audit_trail(revision.id).await.unwrap();
        assert_eq!(trail[0].actor, read_back("catalog-refresh"));
        assert_ne!(trail[0].actor, read_back("someone-else"));
    }

    #[tokio::test]
    async fn a_revision_hydrates_deterministically() {
        let store = InMemoryControlPlane::new();
        let manifest = store
            .publish_revision(candidate(ExpectedRevision::Empty, "first", state()))
            .await
            .unwrap();

        let once = store.load_revision(manifest.id).await.unwrap();
        let twice = store.load_revision(manifest.id).await.unwrap();
        assert_eq!(once, twice, "an immutable revision loads identically");
        assert_eq!(once.state(), &state(), "hydration reproduces the candidate");
        assert_eq!(
            once.state().checksum().unwrap(),
            manifest.checksum,
            "the loaded state hashes to what was published"
        );
    }

    #[tokio::test]
    async fn a_missing_stored_version_is_corruption_not_an_outage() {
        let store = InMemoryControlPlane::new();
        let manifest = store
            .publish_revision(candidate(ExpectedRevision::Empty, "first", state()))
            .await
            .unwrap();

        store.forget_version(&reference_of_alias());
        let error = store
            .load_revision(manifest.id)
            .await
            .expect_err("a manifest entry without its row must not hydrate");
        assert_eq!(
            error,
            ControlPlaneError::corrupt(
                manifest.id,
                IntegrityError::MissingResource {
                    reference: reference_of_alias()
                }
            )
        );
        assert_eq!(error.category(), FailureCategory::Corrupt);
        assert!(
            !error.retryable(),
            "retrying cannot repair unreadable storage"
        );
        // The manifest itself is still readable, so convergence can report the
        // revision it cannot load rather than going silent.
        assert!(store.load_manifest(manifest.id).await.is_ok());
    }

    #[tokio::test]
    async fn a_checksum_mismatch_is_corruption() {
        let store = InMemoryControlPlane::new();
        let manifest = store
            .publish_revision(candidate(ExpectedRevision::Empty, "first", state()))
            .await
            .unwrap();

        store.corrupt_checksum(manifest.id, Checksum::of(b"not the state"));
        let error = store
            .load_revision(manifest.id)
            .await
            .expect_err("a rotted checksum must not hydrate");
        assert!(matches!(
            &error,
            ControlPlaneError::Corrupt { source, .. }
                if matches!(**source, IntegrityError::ChecksumMismatch { .. })
        ));
        assert_eq!(error.category(), FailureCategory::Corrupt);
    }

    #[tokio::test]
    async fn unknown_revisions_and_outages_are_distinguishable() {
        let store = InMemoryControlPlane::new();
        for missing in [
            store.load_manifest(revision_id(7)).await.err(),
            store.load_revision(revision_id(7)).await.err(),
        ] {
            let missing = missing.expect("an unpublished revision is not found");
            assert_eq!(missing.category(), FailureCategory::NotFound);
            assert!(!missing.retryable());
        }

        store.set_unavailable(true);
        let outage = store
            .desired_revision()
            .await
            .expect_err("an unreachable store must not report an empty control plane");
        assert_eq!(outage.category(), FailureCategory::Unavailable);
        assert!(outage.retryable());
        for result in [
            store.health().await.err(),
            store.load_manifest(revision_id(7)).await.err(),
            store.load_revision(revision_id(7)).await.err(),
            store.audit_trail(revision_id(7)).await.err(),
            store
                .publish_revision(candidate(ExpectedRevision::Empty, "first", state()))
                .await
                .err(),
        ] {
            assert_eq!(
                result
                    .expect("every method fails while unreachable")
                    .category(),
                FailureCategory::Unavailable
            );
        }
    }

    #[tokio::test]
    async fn the_store_declares_the_capabilities_publication_relies_on() {
        let store = InMemoryControlPlane::new();
        for capability in [
            Capability::TransactionalWrites,
            Capability::OptimisticConcurrency,
            Capability::IdempotentWrites,
            Capability::TransactionalAudit,
        ] {
            assert!(
                store.capabilities().has(capability),
                "{capability:?} is required of every ControlPlaneStore"
            );
        }
        assert_eq!(store.name(), "in-memory");
        store.health().await.expect("a healthy fake");
    }

    #[tokio::test]
    async fn concurrent_writers_cannot_lose_an_update() {
        let store = std::sync::Arc::new(InMemoryControlPlane::new());
        let first = store
            .publish_revision(candidate(ExpectedRevision::Empty, "first", state()))
            .await
            .unwrap();

        // Two administrators build different changes against the same revision.
        // Exactly one may win; the loser is told to re-read.
        let racers = (0..2).map(|index| {
            let store = std::sync::Arc::clone(&store);
            let candidate = candidate(
                ExpectedRevision::Exactly(first.id),
                if index == 0 { "left" } else { "right" },
                if index == 0 {
                    state_with_renamed_alias()
                } else {
                    let mut state = state();
                    state
                        .insert(alias(&tenant_id(1), 8, "another", &[]))
                        .unwrap();
                    state
                },
            );
            tokio::spawn(async move { store.publish_revision(candidate).await })
        });
        let outcomes = futures::future::join_all(racers).await;
        let (won, lost): (Vec<_>, Vec<_>) = outcomes
            .into_iter()
            .map(|joined| joined.expect("no panic"))
            .partition(Result::is_ok);
        assert_eq!(won.len(), 1, "exactly one writer publishes");
        assert_eq!(lost.len(), 1);
        assert_eq!(
            lost[0].as_ref().unwrap_err().category(),
            FailureCategory::Conflict
        );
        assert_eq!(store.published_revisions(), 2);
    }
}
