//! In-memory fakes the contract tests run against.
//!
//! They exist so the contracts are exercised — publication ordering, conflict
//! detection, idempotent retries, redaction, refresh semantics — without a
//! datastore, keeping the Tier 0 hermetic gate hermetic. They are test-only by
//! construction: a fake `ControlPlaneStore` is not a selectable implementation,
//! because [`ControlPlaneBackend`](super::control_plane::ControlPlaneBackend)
//! has no in-memory variant.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;

use super::catalog::{
    CatalogError, CatalogModelMetadata, CatalogPrice, CatalogRefresh, CatalogSnapshot,
    CatalogSource, CatalogVersion,
};
use super::control_plane::{
    Actor, AuditEvent, ControlPlaneError, ControlPlaneStore, ExpectedRevision, IdempotencyKey,
    ResourceId, ResourceKind, ResourceVersionRef, RevisionCandidate, RevisionChecksum, RevisionId,
    RevisionManifest,
};
use super::secrets::{KekRef, SecretError, SecretMaterial, SecretRef, SecretStore};
use super::{Capabilities, Capability};

/// The audit event the control-plane fixtures publish.
pub(crate) fn audit(action: &str) -> AuditEvent {
    AuditEvent {
        actor: Actor::System { component: "test" },
        action: action.to_owned(),
        summary: format!("{action} applied"),
    }
}

/// A one-resource candidate. `key` is both the idempotency key and the checksum
/// input, so two fixtures differ exactly when their desired state does.
pub(crate) fn candidate(expected: ExpectedRevision, action: &str, key: &str) -> RevisionCandidate {
    RevisionCandidate {
        expected,
        resources: vec![ResourceVersionRef {
            kind: ResourceKind::Tenant,
            id: ResourceId(format!("tenant-{key}")),
            slug: format!("tenant-{key}"),
            version: 1,
        }],
        checksum: RevisionChecksum(format!("sha256:{key}")),
        audit: audit(action),
        idempotency_key: IdempotencyKey(key.to_owned()),
    }
}

#[derive(Default)]
struct ControlPlaneState {
    revisions: Vec<RevisionManifest>,
    audit: HashMap<RevisionId, Vec<AuditEvent>>,
    applied: HashMap<IdempotencyKey, RevisionId>,
}

/// A `ControlPlaneStore` whose transaction is a mutex.
pub(crate) struct InMemoryControlPlane {
    state: Mutex<ControlPlaneState>,
    unavailable: AtomicBool,
}

impl InMemoryControlPlane {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(ControlPlaneState::default()),
            unavailable: AtomicBool::new(false),
        }
    }

    pub(crate) fn set_unavailable(&self, unavailable: bool) {
        self.unavailable.store(unavailable, Ordering::Relaxed);
    }

    pub(crate) fn published_revisions(&self) -> usize {
        self.state.lock().expect("not poisoned").revisions.len()
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
        Ok(self
            .state
            .lock()
            .expect("not poisoned")
            .revisions
            .last()
            .map(|revision| revision.id))
    }

    async fn load_revision(&self, id: RevisionId) -> Result<RevisionManifest, ControlPlaneError> {
        if let Some(error) = self.outage() {
            return Err(error);
        }
        self.state
            .lock()
            .expect("not poisoned")
            .revisions
            .iter()
            .find(|revision| revision.id == id)
            .cloned()
            .ok_or(ControlPlaneError::RevisionNotFound(id))
    }

    async fn publish_revision(
        &self,
        candidate: RevisionCandidate,
    ) -> Result<RevisionManifest, ControlPlaneError> {
        if let Some(error) = self.outage() {
            return Err(error);
        }
        if candidate.resources.is_empty() {
            return Err(ControlPlaneError::Invalid(
                "a revision must reference at least one resource".to_owned(),
            ));
        }

        let mut state = self.state.lock().expect("not poisoned");

        if let Some(existing) = state.applied.get(&candidate.idempotency_key).copied() {
            let manifest = state
                .revisions
                .iter()
                .find(|revision| revision.id == existing)
                .cloned()
                .ok_or(ControlPlaneError::RevisionNotFound(existing))?;
            return Ok(manifest);
        }

        let newest = state.revisions.last().map(|revision| revision.id);
        let expected_matches = match (candidate.expected, newest) {
            (ExpectedRevision::Empty, None) => true,
            (ExpectedRevision::Exactly(expected), Some(actual)) => expected == actual,
            _ => false,
        };
        if !expected_matches {
            return Err(ControlPlaneError::Conflict {
                expected: candidate.expected,
                actual: newest,
            });
        }

        let id = RevisionId(newest.map_or(1, |RevisionId(n)| n + 1));
        let manifest = RevisionManifest {
            id,
            parent: newest,
            created_at: SystemTime::UNIX_EPOCH + Duration::from_secs(id.0),
            resources: candidate.resources,
            checksum: candidate.checksum,
        };
        // One critical section: manifest, audit, and the idempotency record are
        // visible together or not at all.
        state.revisions.push(manifest.clone());
        state.audit.insert(id, vec![candidate.audit]);
        state.applied.insert(candidate.idempotency_key, id);
        Ok(manifest)
    }

    async fn audit_trail(&self, id: RevisionId) -> Result<Vec<AuditEvent>, ControlPlaneError> {
        if let Some(error) = self.outage() {
            return Err(error);
        }
        self.state
            .lock()
            .expect("not poisoned")
            .audit
            .get(&id)
            .cloned()
            .ok_or(ControlPlaneError::RevisionNotFound(id))
    }
}

/// A `SecretStore` that "wraps" material by keeping a KEK label beside it, so a
/// wrong KEK is an unwrap failure rather than a missing row.
pub(crate) struct InMemorySecrets {
    entries: Mutex<HashMap<SecretRef, (String, KekRef)>>,
    kek: Mutex<KekRef>,
    next_id: AtomicUsize,
    unavailable: AtomicBool,
}

impl InMemorySecrets {
    pub(crate) fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            kek: Mutex::new(KekRef("AXOND_KEK".to_owned())),
            next_id: AtomicUsize::new(1),
            unavailable: AtomicBool::new(false),
        }
    }

    pub(crate) fn set_unavailable(&self, unavailable: bool) {
        self.unavailable.store(unavailable, Ordering::Relaxed);
    }

    /// Rotate the KEK out from under stored material, as a lost or replaced key
    /// would.
    pub(crate) fn break_kek(&self) {
        *self.kek.lock().expect("not poisoned") = KekRef("AXOND_KEK_ROTATED".to_owned());
    }

    fn outage(&self) -> Option<SecretError> {
        self.unavailable
            .load(Ordering::Relaxed)
            .then(|| SecretError::Unavailable {
                backend: "in-memory",
                message: "fake secret store is unavailable".to_owned(),
            })
    }
}

#[async_trait]
impl SecretStore for InMemorySecrets {
    fn name(&self) -> &'static str {
        "in-memory"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::new(&[Capability::EnvelopeEncryption])
    }

    async fn store(&self, material: SecretMaterial) -> Result<SecretRef, SecretError> {
        if let Some(error) = self.outage() {
            return Err(error);
        }
        if material.is_empty() {
            return Err(SecretError::Invalid("material is empty".to_owned()));
        }
        let reference = SecretRef {
            id: ResourceId(format!(
                "secret-{}",
                self.next_id.fetch_add(1, Ordering::Relaxed)
            )),
            version: 1,
        };
        let kek = self.kek.lock().expect("not poisoned").clone();
        self.entries
            .lock()
            .expect("not poisoned")
            .insert(reference.clone(), (material.expose().to_owned(), kek));
        Ok(reference)
    }

    async fn rotate(
        &self,
        reference: &SecretRef,
        material: SecretMaterial,
    ) -> Result<SecretRef, SecretError> {
        if let Some(error) = self.outage() {
            return Err(error);
        }
        if material.is_empty() {
            return Err(SecretError::Invalid("material is empty".to_owned()));
        }
        let mut entries = self.entries.lock().expect("not poisoned");
        if !entries.contains_key(reference) {
            return Err(SecretError::NotFound(reference.clone()));
        }
        let rotated = SecretRef {
            id: reference.id.clone(),
            version: reference.version + 1,
        };
        let kek = self.kek.lock().expect("not poisoned").clone();
        entries.insert(rotated.clone(), (material.expose().to_owned(), kek));
        Ok(rotated)
    }

    async fn resolve(&self, reference: &SecretRef) -> Result<SecretMaterial, SecretError> {
        if let Some(error) = self.outage() {
            return Err(error);
        }
        let entries = self.entries.lock().expect("not poisoned");
        let (material, sealed_under) = entries
            .get(reference)
            .ok_or_else(|| SecretError::NotFound(reference.clone()))?;
        let kek = self.kek.lock().expect("not poisoned").clone();
        if *sealed_under != kek {
            return Err(SecretError::Unwrap {
                reference: reference.clone(),
                kek,
            });
        }
        Ok(SecretMaterial::new(material.clone()))
    }

    async fn exists(&self, reference: &SecretRef) -> Result<bool, SecretError> {
        if let Some(error) = self.outage() {
            return Err(error);
        }
        Ok(self
            .entries
            .lock()
            .expect("not poisoned")
            .contains_key(reference))
    }
}

/// A `CatalogSource` serving a fixed model list under a fixed upstream version.
pub(crate) struct InMemoryCatalog {
    version: CatalogVersion,
    models: Vec<CatalogModelMetadata>,
    transfers: AtomicUsize,
    unavailable: AtomicBool,
}

impl InMemoryCatalog {
    pub(crate) fn with_models(models: &[(&str, &str)], version: &str) -> Self {
        Self {
            version: CatalogVersion(version.to_owned()),
            models: models
                .iter()
                .map(|(provider, model)| CatalogModelMetadata {
                    provider: (*provider).to_owned(),
                    model: (*model).to_owned(),
                    display_name: Some((*model).to_owned()),
                    context_window_tokens: Some(128_000),
                    max_output_tokens: Some(16_384),
                    price: Some(CatalogPrice {
                        price: gateway_core::ModelPrice {
                            input_microdollars_per_million: 2_500_000,
                            output_microdollars_per_million: 10_000_000,
                            reasoning_microdollars_per_million: None,
                            cache_read_microdollars_per_million: None,
                            cache_write_microdollars_per_million: None,
                        },
                        published_at: None,
                    }),
                    deprecated: false,
                })
                .collect(),
            transfers: AtomicUsize::new(0),
            unavailable: AtomicBool::new(false),
        }
    }

    pub(crate) fn set_unavailable(&self, unavailable: bool) {
        self.unavailable.store(unavailable, Ordering::Relaxed);
    }

    /// How many refreshes actually transferred metadata.
    pub(crate) fn transfers(&self) -> usize {
        self.transfers.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl CatalogSource for InMemoryCatalog {
    fn name(&self) -> &'static str {
        "in-memory"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::new(&[Capability::IncrementalRefresh, Capability::PriceMetadata])
    }

    async fn refresh(
        &self,
        since: Option<&CatalogVersion>,
    ) -> Result<CatalogRefresh, CatalogError> {
        if self.unavailable.load(Ordering::Relaxed) {
            return Err(CatalogError::Unavailable {
                backend: "in-memory",
                message: "fake catalogue source is unavailable".to_owned(),
            });
        }
        if since == Some(&self.version) {
            return Ok(CatalogRefresh::Unchanged {
                version: Some(self.version.clone()),
            });
        }
        self.transfers.fetch_add(1, Ordering::Relaxed);
        Ok(CatalogRefresh::Updated(CatalogSnapshot {
            retrieved_at: SystemTime::UNIX_EPOCH,
            version: Some(self.version.clone()),
            models: self.models.clone(),
        }))
    }
}
