//! In-memory fakes the contract tests run against.
//!
//! They exist so the contracts are exercised — redaction, refresh semantics,
//! rotation — without a datastore, keeping the Tier 0 hermetic gate hermetic.
//!
//! The `ControlPlaneStore` oracle lives with the domain it publishes, in
//! [`crate::desired_state::oracle`], because its behaviour is defined in terms of
//! revisions and validation rather than of this module's fixtures.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::SystemTime;

use async_trait::async_trait;

use super::catalog::{
    CatalogContent, CatalogError, CatalogModelEntry, CatalogProvider, CatalogRefresh,
    CatalogSnapshot, CatalogSource, JsonPointer, Modality, ModelCapability, ModelFacts, ModelId,
    ModelLimits, ObservedPrice, ObservedRate, PriceRates, ProviderEndpoint, ProviderId,
    ProviderOffering, SchemaVersion, SourceValidators, source_snapshot,
};
use super::secrets::{
    KekRef, SecretDescriptor, SecretError, SecretMaterial, SecretResolver, SecretStore,
};
use super::{Capabilities, Capability};
use crate::desired_state::secrets::{LifecycleTransition, SecretLifecycle, SecretOwner, SecretRef};
use crate::desired_state::{SecretId, Uuid7Generator};

/// One stored secret version: who owns it, what may be done with it, and the
/// material — wrapped, in the sense that the KEK label it was sealed under is
/// kept beside it, so a wrong KEK is an unwrap failure rather than a missing row.
///
/// The material is `None` once the version is tombstoned: destroying it is what
/// tombstoning *is*, and a fake that kept the bytes would let a test pass that a
/// real store would fail.
struct Entry {
    owner: SecretOwner,
    lifecycle: SecretLifecycle,
    material: Option<(String, KekRef)>,
}

/// A `SecretStore` for the contract tests: ownership, lifecycle, and rotation
/// with no datastore.
///
/// Test-only by construction — the module is `#[cfg(test)]`, and no
/// `SecretBackend` names it — so it cannot become a selectable production
/// backend by accident.
pub(crate) struct InMemorySecrets {
    entries: Mutex<HashMap<SecretRef, Entry>>,
    kek: Mutex<KekRef>,
    ids: Uuid7Generator,
    unavailable: AtomicBool,
}

impl InMemorySecrets {
    pub(crate) fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            kek: Mutex::new(KekRef("AXOND_KEK".to_owned())),
            ids: Uuid7Generator::new(),
            unavailable: AtomicBool::new(false),
        }
    }

    /// Store material under an *exact* reference, in an exact state.
    ///
    /// [`SecretStore::stage`] mints its own id, which is right for an
    /// administrative call and useless for a test whose fixture revision already
    /// pins references. This is the only way to put a given version in the store,
    /// and it is why the compilation tests can resolve the same references a
    /// fixture's credential bodies name.
    pub(crate) fn seed(
        &self,
        owner: SecretOwner,
        reference: SecretRef,
        material: &str,
        lifecycle: SecretLifecycle,
    ) {
        let kek = self.kek.lock().expect("not poisoned").clone();
        self.entries.lock().expect("not poisoned").insert(
            reference,
            Entry {
                owner,
                lifecycle,
                // Tombstoned material does not exist, here as in a real store.
                material: (lifecycle != SecretLifecycle::Tombstoned)
                    .then(|| (material.to_owned(), kek)),
            },
        );
    }

    pub(crate) fn set_unavailable(&self, unavailable: bool) {
        self.unavailable.store(unavailable, Ordering::Relaxed);
    }

    /// Rotate the KEK out from under stored material, as a lost or replaced key
    /// would.
    pub(crate) fn break_kek(&self) {
        *self.kek.lock().expect("not poisoned") = KekRef("AXOND_KEK_ROTATED".to_owned());
    }

    /// Whether the store still holds bytes for a version: what a test asserts on
    /// to prove tombstoning destroyed material rather than only relabelling it.
    pub(crate) fn holds_material(&self, reference: &SecretRef) -> bool {
        self.entries
            .lock()
            .expect("not poisoned")
            .get(reference)
            .is_some_and(|entry| entry.material.is_some())
    }

    fn outage(&self) -> Option<SecretError> {
        self.unavailable
            .load(Ordering::Relaxed)
            .then(|| SecretError::Unavailable {
                backend: "in-memory",
                message: "fake secret store is unavailable".to_owned(),
            })
    }

    /// The one place a reference is turned into an entry: unknown and
    /// not-this-owner's are the two ways it fails, in that order, and every
    /// method goes through it so neither check can be skipped in one of them.
    fn describe_locked(
        entries: &HashMap<SecretRef, Entry>,
        owner: SecretOwner,
        reference: &SecretRef,
    ) -> Result<SecretDescriptor, SecretError> {
        let entry = entries
            .get(reference)
            .ok_or(SecretError::NotFound(*reference))?;
        if entry.owner != owner {
            return Err(SecretError::Ownership {
                reference: *reference,
                owner,
            });
        }
        Ok(SecretDescriptor {
            reference: *reference,
            owner: entry.owner,
            lifecycle: entry.lifecycle,
        })
    }
}

#[async_trait]
impl SecretResolver for InMemorySecrets {
    fn name(&self) -> &'static str {
        "in-memory"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::new(&[Capability::EnvelopeEncryption])
    }

    async fn resolve(
        &self,
        owner: SecretOwner,
        reference: &SecretRef,
    ) -> Result<SecretMaterial, SecretError> {
        if let Some(error) = self.outage() {
            return Err(error);
        }
        let entries = self.entries.lock().expect("not poisoned");
        let descriptor = Self::describe_locked(&entries, owner, reference)?;
        if !descriptor.permits_resolution() {
            return Err(SecretError::Lifecycle {
                reference: *reference,
                state: descriptor.lifecycle,
            });
        }
        let entry = entries.get(reference).expect("described above");
        let (material, sealed_under) = entry.material.as_ref().ok_or(SecretError::Lifecycle {
            reference: *reference,
            state: descriptor.lifecycle,
        })?;
        let kek = self.kek.lock().expect("not poisoned").clone();
        if *sealed_under != kek {
            return Err(SecretError::Unwrap {
                reference: *reference,
                kek,
            });
        }
        Ok(SecretMaterial::new(material.clone()))
    }

    async fn exists(&self, owner: SecretOwner, reference: &SecretRef) -> Result<bool, SecretError> {
        if let Some(error) = self.outage() {
            return Err(error);
        }
        let entries = self.entries.lock().expect("not poisoned");
        match Self::describe_locked(&entries, owner, reference) {
            // Withdrawn material answers as material that is not there: the
            // question is what state the material is in, not whether a row exists.
            // Whether it still unwraps is deliberately not asked, because asking
            // means unwrapping.
            Ok(descriptor) => Ok(descriptor.lifecycle.permits_resolution()),
            // A reference somebody else owns answers as one that is not stored:
            // probing must not enumerate another tenant's material.
            Err(SecretError::NotFound(_) | SecretError::Ownership { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }
}

#[async_trait]
impl SecretStore for InMemorySecrets {
    async fn stage(
        &self,
        owner: SecretOwner,
        material: SecretMaterial,
    ) -> Result<SecretDescriptor, SecretError> {
        if let Some(error) = self.outage() {
            return Err(error);
        }
        if material.is_empty() {
            return Err(SecretError::Invalid("material is empty".to_owned()));
        }
        let reference = SecretRef::first(SecretId::new(self.ids.next()));
        let kek = self.kek.lock().expect("not poisoned").clone();
        self.entries.lock().expect("not poisoned").insert(
            reference,
            Entry {
                owner,
                lifecycle: SecretLifecycle::Staged,
                material: Some((material.expose().to_owned(), kek)),
            },
        );
        Ok(SecretDescriptor {
            reference,
            owner,
            lifecycle: SecretLifecycle::Staged,
        })
    }

    async fn rotate(
        &self,
        owner: SecretOwner,
        reference: &SecretRef,
        material: SecretMaterial,
    ) -> Result<SecretDescriptor, SecretError> {
        if let Some(error) = self.outage() {
            return Err(error);
        }
        if material.is_empty() {
            return Err(SecretError::Invalid("material is empty".to_owned()));
        }
        let mut entries = self.entries.lock().expect("not poisoned");
        let current = Self::describe_locked(&entries, owner, reference)?;
        if current.lifecycle.is_terminal() {
            return Err(SecretError::Lifecycle {
                reference: *reference,
                state: current.lifecycle,
            });
        }
        let rotated = reference.rotated();
        // A version is immutable, so rotating twice from one base reference is a
        // stale request rather than a second rotation: overwriting would change
        // what a credential body already pinning `rotated` resolves to.
        if entries.contains_key(&rotated) {
            return Err(SecretError::Invalid(format!("{rotated} already exists")));
        }
        let kek = self.kek.lock().expect("not poisoned").clone();
        entries.insert(
            rotated,
            Entry {
                owner,
                lifecycle: SecretLifecycle::Staged,
                material: Some((material.expose().to_owned(), kek)),
            },
        );
        Ok(SecretDescriptor {
            reference: rotated,
            owner,
            lifecycle: SecretLifecycle::Staged,
        })
    }

    async fn transition(
        &self,
        owner: SecretOwner,
        reference: &SecretRef,
        next: SecretLifecycle,
    ) -> Result<LifecycleTransition, SecretError> {
        if let Some(error) = self.outage() {
            return Err(error);
        }
        let mut entries = self.entries.lock().expect("not poisoned");
        let current = Self::describe_locked(&entries, owner, reference)?;
        let transition =
            current
                .lifecycle
                .transition_to(next)
                .map_err(|source| SecretError::Transition {
                    reference: *reference,
                    source,
                })?;
        let entry = entries.get_mut(reference).expect("described above");
        entry.lifecycle = transition.state();
        if entry.lifecycle == SecretLifecycle::Tombstoned {
            // Tombstoning is the destruction, not a label on material that stays.
            entry.material = None;
        }
        Ok(transition)
    }

    async fn describe(
        &self,
        owner: SecretOwner,
        reference: &SecretRef,
    ) -> Result<SecretDescriptor, SecretError> {
        if let Some(error) = self.outage() {
            return Err(error);
        }
        let entries = self.entries.lock().expect("not poisoned");
        Self::describe_locked(&entries, owner, reference)
    }
}

/// A `CatalogSource` serving a fixed model list under a fixed upstream version.
pub(crate) struct InMemoryCatalog {
    validators: SourceValidators,
    content: CatalogContent,
    transfers: AtomicUsize,
    unavailable: AtomicBool,
}

impl InMemoryCatalog {
    /// A catalogue of `(provider, model)` offerings, served under `etag`.
    pub(crate) fn with_models(models: &[(&str, &str)], etag: &str) -> Self {
        let providers: Vec<CatalogProvider> = models
            .iter()
            .map(|(provider, _)| CatalogProvider {
                id: ProviderId::parse(provider).expect("a canonical fake provider id"),
                display_name: Some((*provider).to_owned()),
                doc_url: None,
                endpoint: ProviderEndpoint::default(),
                env_vars: Vec::new(),
                pointer: JsonPointer::new("").child("providers").child(provider),
            })
            .collect();
        let entries: Vec<CatalogModelEntry> = models
            .iter()
            .map(|(provider, model)| {
                let id = ModelId::parse(model).expect("a canonical fake model id");
                let facts = ModelFacts {
                    display_name: Some((*model).to_owned()),
                    capabilities: [ModelCapability::ToolCall].into_iter().collect(),
                    input_modalities: [Modality::Text].into_iter().collect(),
                    output_modalities: [Modality::Text].into_iter().collect(),
                    limits: ModelLimits {
                        context_tokens: Some(128_000),
                        output_tokens: Some(16_384),
                        ..ModelLimits::default()
                    },
                    ..ModelFacts::default()
                };
                let pointer = JsonPointer::new("")
                    .child("providers")
                    .child(provider)
                    .child("models")
                    .child(model);
                CatalogModelEntry {
                    id: id.clone(),
                    neutral: Some(facts.clone()),
                    offerings: vec![ProviderOffering {
                        provider: ProviderId::parse(provider).expect("a canonical fake id"),
                        model: id,
                        published_model_id: (*model).to_owned(),
                        facts,
                        overrides: Vec::new(),
                        price: Some(ObservedPrice::new(PriceRates::new(
                            ObservedRate::from_nanos(2_500_000_000),
                            ObservedRate::from_nanos(10_000_000_000),
                        ))),
                        endpoint: ProviderEndpoint::default(),
                        pointer,
                    }],
                }
            })
            .collect();
        Self {
            validators: SourceValidators::etag(etag),
            content: CatalogContent::new(providers, entries).expect("a consistent fake catalogue"),
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
        since: Option<&SourceValidators>,
    ) -> Result<CatalogRefresh, CatalogError> {
        if self.unavailable.load(Ordering::Relaxed) {
            return Err(CatalogError::Unavailable {
                backend: "in-memory",
                message: "fake catalogue source is unavailable".to_owned(),
            });
        }
        if since == Some(&self.validators) {
            return Ok(CatalogRefresh::Unchanged {
                validators: self.validators.clone(),
            });
        }
        self.transfers.fetch_add(1, Ordering::Relaxed);
        let source = source_snapshot(
            "memory://catalogue",
            SchemaVersion::MODELS_DEV_CATALOG_V1,
            b"{}",
            &self.content,
            self.validators.clone(),
            SystemTime::UNIX_EPOCH,
        );
        Ok(CatalogRefresh::Updated(CatalogSnapshot {
            source,
            content: self.content.clone(),
        }))
    }
}
