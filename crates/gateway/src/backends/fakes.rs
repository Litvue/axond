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
    CatalogError, CatalogModelMetadata, CatalogPrice, CatalogRefresh, CatalogSnapshot,
    CatalogSource, CatalogVersion,
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
            Ok(_) => Ok(true),
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
