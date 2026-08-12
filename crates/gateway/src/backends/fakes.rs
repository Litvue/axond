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
use super::secrets::{KekRef, SecretError, SecretMaterial, SecretRef, SecretStore};
use super::{Capabilities, Capability};
use crate::desired_state::{ResourceId, Uuid7Generator};

/// A `SecretStore` that "wraps" material by keeping a KEK label beside it, so a
/// wrong KEK is an unwrap failure rather than a missing row.
pub(crate) struct InMemorySecrets {
    entries: Mutex<HashMap<SecretRef, (String, KekRef)>>,
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
            id: ResourceId::new(self.ids.next()),
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
            id: reference.id,
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
