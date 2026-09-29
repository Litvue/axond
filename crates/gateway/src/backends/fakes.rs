//! An in-memory catalogue source the catalogue contract tests run against,
//! without a network.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::SystemTime;

use async_trait::async_trait;

use super::catalog::{
    CatalogContent, CatalogError, CatalogModelEntry, CatalogProvider, CatalogRefresh,
    CatalogSnapshot, CatalogSource, JsonPointer, Modality, ModelCapability, ModelFacts, ModelId,
    ModelLimits, ObservedPrice, ObservedRate, PriceRates, ProviderEndpoint, ProviderId,
    ProviderOffering, RawPayload, SchemaVersion, SourceValidators, source_snapshot,
};
use super::{Capabilities, Capability};

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
            return Err(CatalogError::unavailable(
                "in-memory",
                "fake catalogue source is unavailable".to_owned(),
            ));
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
        Ok(CatalogRefresh::Updated {
            snapshot: Box::new(CatalogSnapshot {
                source,
                content: self.content.clone(),
            }),
            payload: RawPayload::new(&b"{}"[..]),
        })
    }
}
