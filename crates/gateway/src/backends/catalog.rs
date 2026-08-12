//! Model metadata ingestion: the [`CatalogSource`] contract.
//!
//! models.dev is the first source. The contract exists to keep one distinction
//! sharp: **metadata may refresh automatically; enablement and pricing are
//! explicit administrative acts.** A refresh stores new or changed catalogue
//! metadata and nothing else — it never enables a model for a tenant, never
//! changes which alias targets exist, and never activates a price. An upstream
//! catalogue edit must not be able to become a production billing change.
//!
//! So [`CatalogPrice`] is *observed* pricing, not applied pricing: a refresh may
//! record that a model's published rate changed, and only an explicit mutation
//! turns that into a price a request is billed against.
//!
//! The source is [`BackendPath::Background`](super::BackendPath::Background):
//! never on the request path, never a boot dependency. In stateful mode
//! `/v1/models` and price lookups read the snapshot compiled from stored
//! metadata, so an unreachable models.dev is a stale-metadata signal with
//! metrics, not an outage.

use std::time::SystemTime;

use async_trait::async_trait;
use gateway_core::ModelPrice;

use super::{BackendFailure, BackendKind, Capabilities, FailureCategory};

/// The sources a deployment may select for catalogue metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CatalogBackend {
    #[default]
    ModelsDev,
}

impl CatalogBackend {
    pub const fn kind(self) -> BackendKind {
        match self {
            Self::ModelsDev => BackendKind::ModelsDev,
        }
    }
}

/// The upstream's own version token — an ETag, digest, or release id.
///
/// Opaque: it is only ever compared for equality and handed back to the source
/// on the next refresh, so a source can answer [`CatalogRefresh::Unchanged`]
/// without transferring the catalogue again.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CatalogVersion(pub String);

/// Pricing as *published upstream*.
///
/// Reusing [`ModelPrice`] keeps the arithmetic and the micro-dollar denomination
/// identical to configured prices, and it is deliberately not the same thing as
/// an active price: activation is an administrative mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogPrice {
    pub price: ModelPrice,
    /// When the source last changed this price, when it says.
    pub published_at: Option<SystemTime>,
}

/// One model as the upstream catalogue describes it.
///
/// Metadata only: nothing here says a tenant may use the model, which alias
/// targets it, or what it is billed at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogModelMetadata {
    /// The upstream provider id, in the source's vocabulary.
    pub provider: String,
    /// The provider's model or deployment id.
    pub model: String,
    pub display_name: Option<String>,
    pub context_window_tokens: Option<u32>,
    pub max_output_tokens: Option<u32>,
    /// Observed pricing. Recording it never activates it.
    pub price: Option<CatalogPrice>,
    /// The source marks the model as retiring or retired.
    pub deprecated: bool,
}

/// A consistent set of metadata read from the source at one instant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogSnapshot {
    pub retrieved_at: SystemTime,
    pub version: Option<CatalogVersion>,
    pub models: Vec<CatalogModelMetadata>,
}

/// The outcome of a refresh.
///
/// `Unchanged` is a first-class answer rather than an empty snapshot, so a
/// caller can tell "the upstream has nothing new" from "the upstream now lists
/// no models" — the second would silently retire every model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogRefresh {
    Unchanged { version: Option<CatalogVersion> },
    Updated(CatalogSnapshot),
}

/// Why a refresh failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CatalogError {
    #[error("catalogue source `{backend}` unavailable: {message}")]
    Unavailable {
        backend: &'static str,
        message: String,
    },
    #[error("catalogue source `{backend}` returned unusable metadata: {message}")]
    Invalid {
        backend: &'static str,
        message: String,
    },
    #[error("catalogue source `{backend}` refused the request: {message}")]
    Denied {
        backend: &'static str,
        message: String,
    },
}

impl BackendFailure for CatalogError {
    fn category(&self) -> FailureCategory {
        match self {
            Self::Unavailable { .. } => FailureCategory::Unavailable,
            Self::Invalid { .. } => FailureCategory::Invalid,
            Self::Denied { .. } => FailureCategory::Denied,
        }
    }
}

/// Background ingestion of upstream model metadata.
#[async_trait]
pub trait CatalogSource: Send + Sync {
    fn name(&self) -> &'static str;

    fn capabilities(&self) -> Capabilities;

    /// Read metadata, skipping the transfer when `since` still matches the
    /// upstream version.
    ///
    /// A failure is a background-refresh failure: it must leave previously
    /// stored metadata in place, because stale metadata serves requests fine and
    /// an empty catalogue does not.
    async fn refresh(&self, since: Option<&CatalogVersion>)
    -> Result<CatalogRefresh, CatalogError>;
}

#[cfg(test)]
mod tests {
    use super::super::{BackendPath, Capability, fakes::InMemoryCatalog, responsibility};
    use super::*;

    #[tokio::test]
    async fn a_first_refresh_returns_metadata_with_a_version() {
        let source = InMemoryCatalog::with_models(&[("openai", "gpt-4o")], "v1");
        let CatalogRefresh::Updated(snapshot) = source.refresh(None).await.expect("refresh") else {
            panic!("a first refresh has no prior version to match");
        };
        assert_eq!(snapshot.version, Some(CatalogVersion("v1".to_owned())));
        assert_eq!(snapshot.models.len(), 1);
        assert_eq!(snapshot.models[0].model, "gpt-4o");
    }

    #[tokio::test]
    async fn an_unchanged_upstream_is_not_an_empty_catalogue() {
        let source = InMemoryCatalog::with_models(&[("openai", "gpt-4o")], "v1");
        let refreshed = source
            .refresh(Some(&CatalogVersion("v1".to_owned())))
            .await
            .expect("refresh");
        assert_eq!(
            refreshed,
            CatalogRefresh::Unchanged {
                version: Some(CatalogVersion("v1".to_owned()))
            }
        );
        assert_eq!(
            source.transfers(),
            0,
            "an unchanged refresh transfers nothing"
        );
    }

    #[tokio::test]
    async fn a_changed_upstream_transfers_the_new_metadata() {
        let source = InMemoryCatalog::with_models(&[("openai", "gpt-4o")], "v2");
        let CatalogRefresh::Updated(snapshot) = source
            .refresh(Some(&CatalogVersion("v1".to_owned())))
            .await
            .expect("refresh")
        else {
            panic!("a changed version must transfer");
        };
        assert_eq!(snapshot.version, Some(CatalogVersion("v2".to_owned())));
        assert_eq!(source.transfers(), 1);
    }

    #[tokio::test]
    async fn observed_pricing_is_metadata_not_activation() {
        let source = InMemoryCatalog::with_models(&[("openai", "gpt-4o")], "v1");
        let CatalogRefresh::Updated(snapshot) = source.refresh(None).await.unwrap() else {
            panic!("expected metadata");
        };
        let price = snapshot.models[0]
            .price
            .as_ref()
            .expect("the fake publishes a price");
        // The contract carries the observed rate; nothing here can enable a
        // model or bill against it.
        assert!(price.price.input_microdollars_per_million > 0);
        assert!(!snapshot.models[0].deprecated);
    }

    #[tokio::test]
    async fn an_unreachable_source_is_retryable_and_never_a_boot_failure() {
        let source = InMemoryCatalog::with_models(&[("openai", "gpt-4o")], "v1");
        source.set_unavailable(true);
        let error = source.refresh(None).await.expect_err("outage");
        assert_eq!(error.category(), FailureCategory::Unavailable);
        assert!(error.retryable());

        source.set_unavailable(false);
        assert!(matches!(
            source.refresh(None).await,
            Ok(CatalogRefresh::Updated(_))
        ));
    }

    #[tokio::test]
    async fn the_source_is_background_only_and_declares_incremental_refresh() {
        let source = InMemoryCatalog::with_models(&[], "v1");
        assert!(source.capabilities().has(Capability::IncrementalRefresh));
        assert!(source.capabilities().has(Capability::PriceMetadata));

        let responsibility = responsibility("CatalogSource").expect("declared responsibility");
        assert_eq!(responsibility.path, BackendPath::Background);
        assert!(responsibility.permits(CatalogBackend::default().kind()));
        assert!(!responsibility.permits(BackendKind::Redis));
    }
}
