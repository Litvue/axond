//! Operator-authored catalogue snapshots for custom deployment ids.
//!
//! Local (vLLM / Azure deployment id) offerings are stored as models.dev
//! documents so unchanged [`super::catalog_store::hydrate`] can re-parse them.
//! The builder below is the only payload the expander retains for that path:
//! one provider, one offering, `cost` derived from stated micro-dollars.
//! Provenance is `axond://local/{tenant}/{content_id}/catalog.json` so
//! [`super::models_dev::ModelsDevAdapter::new`] accepts the URL. Identity
//! remains the blob digest; [`project()`](crate::convergence::serving::project)
//! distinguishes local vs imported by tenant-scoped `CatalogModel`, not by
//! sniffing the URL.

use std::time::SystemTime;

use gateway_core::ModelPrice;
use serde_json::{Value, json};

use super::catalog::{
    CatalogSnapshot, ObservedPrice, RawPayload, SchemaVersion, SourceValidators, source_snapshot,
};
use super::catalog_pins::{PinnedCatalog, Resolution};
use super::catalog_store::RetainedCatalog;
use super::models_dev::{ModelsDevAdapter, ModelsDevError};
use crate::desired_state::{CatalogOffering, TenantId};

/// Golden local snapshot: one vLLM offering at a stated `$0` price.
pub const GOLDEN_LOCAL: &str = include_str!("fixtures/models_dev/catalog.local.json");

/// Why a local catalogue could not be built or parsed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LocalCatalogError {
    #[error("the local catalogue payload is not valid models.dev input: {source}")]
    Adapter {
        #[source]
        source: ModelsDevError,
    },
    #[error("the local catalogue payload could not be encoded")]
    Encode,
}

/// Typed authoring of a one-offering models.dev document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalCatalogBuilder {
    provider: String,
    provider_name: String,
    model: String,
    display_name: String,
    input_micros: u64,
    output_micros: u64,
    context_tokens: u64,
}

impl LocalCatalogBuilder {
    /// One offering keyed the way the connection publishes it.
    pub fn new(provider: impl Into<String>, model: impl Into<String>) -> Self {
        let provider = provider.into();
        let model = model.into();
        let provider_name = provider.clone();
        let display_name = model.clone();
        Self {
            provider,
            provider_name,
            model,
            display_name,
            input_micros: 0,
            output_micros: 0,
            context_tokens: 8_192,
        }
    }

    /// The design fixture: vLLM Llama 3 70B at `$0`.
    pub fn golden() -> Self {
        Self::new("vllm", "meta-llama-3-70b-instruct")
            .provider_name("vLLM")
            .display_name("Llama 3 70B Instruct")
            .price(0, 0)
    }

    #[must_use]
    pub fn provider_name(mut self, name: impl Into<String>) -> Self {
        self.provider_name = name.into();
        self
    }

    #[must_use]
    pub fn display_name(mut self, name: impl Into<String>) -> Self {
        self.display_name = name.into();
        self
    }

    /// Stated rates in micro-dollars per million tokens. `$0` is approved free.
    #[must_use]
    pub fn price(mut self, input_micros: u64, output_micros: u64) -> Self {
        self.input_micros = input_micros;
        self.output_micros = output_micros;
        self
    }

    #[must_use]
    pub fn context_tokens(mut self, tokens: u64) -> Self {
        self.context_tokens = tokens;
        self
    }

    /// Exact models.dev bytes this builder will retain.
    pub fn payload(&self) -> Result<Vec<u8>, LocalCatalogError> {
        let env = format!("{}_API_KEY", env_prefix(&self.provider));
        let model = model_record(&self.model, &self.display_name, self.context_tokens, None);
        let offered = model_record(
            &self.model,
            &self.display_name,
            self.context_tokens,
            Some((self.input_micros, self.output_micros)),
        );
        let document = json!({
            "models": { self.model.clone(): model },
            "providers": {
                self.provider.clone(): {
                    "id": self.provider,
                    "name": self.provider_name,
                    "env": [env],
                    "models": { self.model.clone(): offered },
                }
            }
        });
        serde_json::to_vec_pretty(&document).map_err(|_| LocalCatalogError::Encode)
    }

    /// Provenance URL hydrate can hand to [`ModelsDevAdapter::new`].
    pub fn source_url(tenant: TenantId, content_id: super::catalog::CatalogContentId) -> String {
        format!("axond://local/{tenant}/{content_id}/catalog.json")
    }

    /// Parse into a snapshot whose URL ends with `/catalog.json`.
    pub fn snapshot(
        &self,
        tenant: TenantId,
        fetched_at: SystemTime,
    ) -> Result<CatalogSnapshot, LocalCatalogError> {
        let bytes = self.payload()?;
        parse_local_payload(&bytes, tenant, fetched_at)
    }

    /// Checksum-addressed record the expander retains. Never activates.
    pub fn retained(
        &self,
        tenant: TenantId,
        fetched_at: SystemTime,
    ) -> Result<RetainedCatalog, LocalCatalogError> {
        let bytes = self.payload()?;
        let snapshot = parse_local_payload(&bytes, tenant, fetched_at)?;
        Ok(RetainedCatalog {
            source: snapshot.source,
            payload: RawPayload::new(bytes),
        })
    }
}

/// Compile a local snapshot's stated `cost` into a file price, when exact.
pub fn model_price_from_cost(price: &ObservedPrice) -> Option<ModelPrice> {
    const PER_MICRO: u64 = 1_000;
    let input = price.base.input.nanos();
    let output = price.base.output.nanos();
    if !input.is_multiple_of(PER_MICRO) || !output.is_multiple_of(PER_MICRO) {
        return None;
    }
    Some(ModelPrice {
        input_microdollars_per_million: input / PER_MICRO,
        output_microdollars_per_million: output / PER_MICRO,
        reasoning_microdollars_per_million: None,
        cache_read_microdollars_per_million: None,
        cache_write_microdollars_per_million: None,
    })
}

/// The file price a tenant-scoped pin would compile, when the snapshot still
/// publishes the offering at an exact micro-dollar cost.
pub fn compiled_local_price(
    snapshot: &CatalogSnapshot,
    offering: CatalogOffering,
) -> Option<ModelPrice> {
    let pinned = PinnedCatalog::of_snapshot(snapshot).ok()?;
    match pinned.resolve(offering) {
        Resolution::Callable(callable) => model_price_from_cost(callable.price()?),
        _ => None,
    }
}

fn parse_local_payload(
    bytes: &[u8],
    tenant: TenantId,
    fetched_at: SystemTime,
) -> Result<CatalogSnapshot, LocalCatalogError> {
    let draft_url = format!("axond://local/{tenant}/catalog.json");
    let adapter = ModelsDevAdapter::new(&draft_url)
        .map_err(|source| LocalCatalogError::Adapter { source })?;
    let mut snapshot = adapter
        .parse(bytes, SourceValidators::default(), fetched_at)
        .map_err(|source| LocalCatalogError::Adapter { source })?;
    let url = LocalCatalogBuilder::source_url(tenant, snapshot.content.content_id());
    snapshot.source = source_snapshot(
        url,
        SchemaVersion::MODELS_DEV_CATALOG_V1,
        bytes,
        &snapshot.content,
        SourceValidators::default(),
        fetched_at,
    );
    Ok(snapshot)
}

fn env_prefix(provider: &str) -> String {
    provider
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn model_record(id: &str, name: &str, context: u64, cost: Option<(u64, u64)>) -> Value {
    let mut record = json!({
        "id": id,
        "name": name,
        "modalities": { "input": ["text"], "output": ["text"] },
        "limit": { "context": context },
    });
    if let Some((input, output)) = cost {
        record["cost"] = json!({
            "input": dollars_value(input),
            "output": dollars_value(output),
        });
    }
    record
}

/// Exact dollars-per-million JSON number: micros / 1_000_000, never through f64.
fn dollars_value(micros: u64) -> Value {
    const MILLION: u64 = 1_000_000;
    if micros.is_multiple_of(MILLION) {
        return Value::Number((micros / MILLION).into());
    }
    let whole = micros / MILLION;
    let mut frac = micros % MILLION;
    let mut digits = 6usize;
    while frac.is_multiple_of(10) && digits > 1 {
        frac /= 10;
        digits -= 1;
    }
    let text = format!("{whole}.{frac:0digits$}");
    serde_json::from_str(&text).expect("a micro-dollar decimal is JSON")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::UNIX_EPOCH;

    use crate::backends::catalog_store::{CatalogStore, InMemoryCatalogStore, Retention, hydrate};
    use crate::desired_state::fixtures;

    fn tenant() -> TenantId {
        fixtures::tenant_id(1)
    }

    #[test]
    fn golden_bytes_are_valid_models_dev_adapter_input() {
        let builder = LocalCatalogBuilder::golden();
        let payload = builder.payload().expect("the golden builder encodes");
        let golden = GOLDEN_LOCAL.trim_end_matches(['\n', '\r']);
        assert_eq!(
            String::from_utf8_lossy(&payload),
            golden,
            "builder bytes must match the committed golden file"
        );
        let parsed: Value = serde_json::from_slice(&payload).expect("json");
        assert_eq!(parsed["providers"]["vllm"]["id"], "vllm");
        assert_eq!(
            parsed["providers"]["vllm"]["models"]["meta-llama-3-70b-instruct"]["cost"]["input"],
            0
        );
        assert_eq!(
            parsed["providers"]["vllm"]["models"]["meta-llama-3-70b-instruct"]["cost"]["output"],
            0
        );
    }

    #[test]
    fn provenance_url_ends_with_catalog_json() {
        let snapshot = LocalCatalogBuilder::golden()
            .snapshot(tenant(), UNIX_EPOCH)
            .expect("golden parses");
        let url = &snapshot.source.source_url;
        assert!(
            url.ends_with("/catalog.json"),
            "hydrate requires this suffix, got {url}"
        );
        assert!(
            url.starts_with(&format!("axond://local/{}/", tenant())),
            "local provenance, got {url}"
        );
        assert_eq!(
            snapshot.source.schema_version,
            SchemaVersion::MODELS_DEV_CATALOG_V1
        );
    }

    #[test]
    fn stated_micros_become_exact_nano_dollar_cost() {
        let snapshot = LocalCatalogBuilder::new("vllm", "local-model")
            .price(2_500_000, 10_000_000)
            .snapshot(tenant(), UNIX_EPOCH)
            .expect("priced local parses");
        let offering = snapshot.content.models()[0].offerings[0]
            .price
            .as_ref()
            .expect("cost is stated");
        assert_eq!(offering.base.input.nanos(), 2_500_000_000);
        assert_eq!(offering.base.output.nanos(), 10_000_000_000);
        let price = model_price_from_cost(offering).expect("exact micros");
        assert_eq!(price.input_microdollars_per_million, 2_500_000);
        assert_eq!(price.output_microdollars_per_million, 10_000_000);
    }

    #[test]
    fn zero_is_approved_free_not_unpublished() {
        let snapshot = LocalCatalogBuilder::golden()
            .snapshot(tenant(), UNIX_EPOCH)
            .expect("zero parses");
        let offering = snapshot.content.models()[0].offerings[0]
            .price
            .as_ref()
            .expect("$0 is a stated cost, not an empty cost object");
        assert_eq!(offering.base.input.nanos(), 0);
        assert_eq!(offering.base.output.nanos(), 0);
        let price = model_price_from_cost(offering).expect("zero converts");
        assert_eq!(price.input_microdollars_per_million, 0);
        assert_eq!(price.output_microdollars_per_million, 0);
    }

    #[tokio::test]
    async fn retain_hydrates_into_a_pinned_catalog() {
        let retained = LocalCatalogBuilder::golden()
            .retained(tenant(), UNIX_EPOCH)
            .expect("retainable");
        let store = InMemoryCatalogStore::new();
        assert_eq!(
            store.retain(&retained).await.expect("retain"),
            Retention::Retained
        );
        let loaded = store
            .retained(retained.content_id())
            .await
            .expect("lookup")
            .expect("held");
        let snapshot = hydrate(&loaded).expect("re-parse");
        assert_eq!(snapshot.source.content_id, retained.content_id());
        assert!(snapshot.source.source_url.ends_with("/catalog.json"));
        let pinned = PinnedCatalog::of_snapshot(&snapshot).expect("keyed");
        let offering = CatalogOffering::new(
            crate::desired_state::OfferingId::of("vllm", "meta-llama-3-70b-instruct").expect("id"),
            snapshot.source.raw.digest,
        );
        assert!(
            matches!(pinned.resolve(offering), Resolution::Callable(_)),
            "the golden offering must pin"
        );
        assert!(store.load().await.expect("load").active.is_none());
    }
}
