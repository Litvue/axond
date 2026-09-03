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

use std::collections::{BTreeMap, HashMap};
use std::time::SystemTime;

use gateway_core::ModelPrice;
use serde::Serialize;
use serde_json::value::RawValue;
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
        let model = model_record(&self.model, &self.display_name, self.context_tokens);
        let offered = LocalOffering {
            cost: LocalCost {
                input: dollars_token(self.input_micros)?,
                output: dollars_token(self.output_micros)?,
            },
            id: self.model.as_str(),
            limit: model["limit"].clone(),
            modalities: model["modalities"].clone(),
            name: self.display_name.as_str(),
        };
        let mut models = BTreeMap::new();
        models.insert(self.model.as_str(), model);
        let mut offerings = BTreeMap::new();
        offerings.insert(self.model.as_str(), offered);
        let mut providers = BTreeMap::new();
        providers.insert(
            self.provider.as_str(),
            LocalProvider {
                env: vec![env],
                id: self.provider.as_str(),
                models: offerings,
                name: self.provider_name.as_str(),
            },
        );
        serde_json::to_vec_pretty(&LocalDocument { models, providers })
            .map_err(|_| LocalCatalogError::Encode)
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

/// Compile a models.dev `cost` into billable micro-dollar rates, when exact.
///
/// Nano-dollars that are not a whole micro-dollar are unpublished: charging a
/// truncated rate would disagree with the observation, and rounding up would
/// overcharge. Optional cache/reasoning rates convert the same way; a stated
/// optional that is not exact makes the whole offering unusable rather than
/// silently falling back to input/output.
pub fn model_price_from_cost(price: &ObservedPrice) -> Option<ModelPrice> {
    Some(ModelPrice {
        input_microdollars_per_million: exact_micros(price.base.input.nanos())?,
        output_microdollars_per_million: exact_micros(price.base.output.nanos())?,
        reasoning_microdollars_per_million: optional_micros(price.base.reasoning)?,
        cache_read_microdollars_per_million: optional_micros(price.base.cache_read)?,
        cache_write_microdollars_per_million: optional_micros(price.base.cache_write)?,
    })
}

const NANOS_PER_MICRO: u64 = 1_000;

fn exact_micros(nanos: u64) -> Option<u64> {
    nanos
        .is_multiple_of(NANOS_PER_MICRO)
        .then_some(nanos / NANOS_PER_MICRO)
}

fn optional_micros(rate: Option<super::catalog::ObservedRate>) -> Option<Option<u64>> {
    match rate {
        None => Some(None),
        Some(rate) => exact_micros(rate.nanos()).map(Some),
    }
}

/// Billable rates compiled from one imported snapshot.
///
/// Keyed by (`[[provider]]` id, the id that provider publishes — the bare
/// model id after the request's first `/`). Offerings with no usable `cost`
/// are omitted; `unpriced_models` decides those at admission.
#[derive(Debug, Clone, Default)]
pub struct CatalogPriceIndex {
    rates: HashMap<String, HashMap<String, ModelPrice>>,
}

impl CatalogPriceIndex {
    pub fn from_snapshot(snapshot: &CatalogSnapshot) -> Self {
        let mut rates: HashMap<String, HashMap<String, ModelPrice>> = HashMap::new();
        for model in snapshot.content.models() {
            for offering in &model.offerings {
                let Some(observed) = offering.price.as_ref() else {
                    continue;
                };
                let Some(price) = model_price_from_cost(observed) else {
                    continue;
                };
                rates
                    .entry(offering.provider.as_str().to_owned())
                    .or_default()
                    .insert(offering.published_model_id.clone(), price);
            }
        }
        Self { rates }
    }

    pub fn price_for(&self, provider: &str, published_model_id: &str) -> Option<ModelPrice> {
        self.rates.get(provider)?.get(published_model_id).copied()
    }
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

/// Models.dev document whose `cost` rates stay raw number tokens.
///
/// Field order matches `serde_json::Value` map order (alphabetical) so
/// [`LocalCatalogBuilder::golden`] bytes stay identical to the committed fixture.
#[derive(Serialize)]
struct LocalDocument<'a> {
    models: BTreeMap<&'a str, Value>,
    providers: BTreeMap<&'a str, LocalProvider<'a>>,
}

#[derive(Serialize)]
struct LocalProvider<'a> {
    env: Vec<String>,
    id: &'a str,
    models: BTreeMap<&'a str, LocalOffering<'a>>,
    name: &'a str,
}

#[derive(Serialize)]
struct LocalOffering<'a> {
    cost: LocalCost,
    id: &'a str,
    limit: Value,
    modalities: Value,
    name: &'a str,
}

#[derive(Serialize)]
struct LocalCost {
    input: Box<RawValue>,
    output: Box<RawValue>,
}

fn model_record(id: &str, name: &str, context: u64) -> Value {
    json!({
        "id": id,
        "name": name,
        "modalities": { "input": ["text"], "output": ["text"] },
        "limit": { "context": context },
    })
}

/// Dollars-per-million as a JSON number token: micros / 1_000_000.
///
/// Integer micros emit an integer token (`10`); otherwise the exact decimal
/// (`0.000001`, `2.5`). The token is never parsed into `Value::Number`.
fn dollars_token(micros: u64) -> Result<Box<RawValue>, LocalCatalogError> {
    const MILLION: u64 = 1_000_000;
    let text = if micros.is_multiple_of(MILLION) {
        (micros / MILLION).to_string()
    } else {
        let whole = micros / MILLION;
        let mut frac = micros % MILLION;
        let mut digits = 6usize;
        while frac.is_multiple_of(10) && digits > 1 {
            frac /= 10;
            digits -= 1;
        }
        format!("{whole}.{frac:0digits$}")
    };
    RawValue::from_string(text).map_err(|_| LocalCatalogError::Encode)
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
        let builder = LocalCatalogBuilder::new("vllm", "local-model").price(2_500_000, 10_000_000);
        let payload = builder.payload().expect("priced local encodes");
        let text = String::from_utf8_lossy(&payload);
        assert!(
            text.contains("\"input\": 2.5"),
            "2.5 must pretty-print as a decimal token, got {text}"
        );
        assert!(
            text.contains("\"output\": 10"),
            "integer micros must pretty-print as a JSON integer, got {text}"
        );
        assert!(
            !text.contains("10.0"),
            "integer micros must not emit a trailing fraction, got {text}"
        );
        let snapshot = builder
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
    fn non_f64_safe_micros_round_trip_as_exact_decimal_tokens() {
        assert_exact_cost_tokens(1, 3, "0.000001", "0.000003");
        assert_exact_cost_tokens(1_000_001, 10_000_000, "1.000001", "10");
    }

    fn assert_exact_cost_tokens(input: u64, output: u64, input_token: &str, output_token: &str) {
        let builder = LocalCatalogBuilder::new("vllm", "local-model").price(input, output);
        let payload = builder.payload().expect("encodes");
        let text = String::from_utf8_lossy(&payload);
        assert!(
            text.contains(&format!("\"input\": {input_token}")),
            "payload must contain {input_token}, got {text}"
        );
        assert!(
            text.contains(&format!("\"output\": {output_token}")),
            "payload must contain {output_token}, got {text}"
        );
        assert!(
            !text.contains("e-") && !text.contains("E-"),
            "cost must not use exponent form, got {text}"
        );

        let snapshot = builder
            .snapshot(tenant(), UNIX_EPOCH)
            .expect("snapshot parses the tokens");
        let offering = CatalogOffering::new(
            crate::desired_state::OfferingId::of("vllm", "local-model").expect("id"),
            snapshot.source.raw.digest,
        );
        let price = compiled_local_price(&snapshot, offering).expect("compiles");
        assert_eq!(price.input_microdollars_per_million, input);
        assert_eq!(price.output_microdollars_per_million, output);

        let retained = builder.retained(tenant(), UNIX_EPOCH).expect("retainable");
        let hydrated = hydrate(&retained).expect("hydrate re-parses the same tokens");
        let offering = CatalogOffering::new(
            crate::desired_state::OfferingId::of("vllm", "local-model").expect("id"),
            hydrated.source.raw.digest,
        );
        let hydrated_price = compiled_local_price(&hydrated, offering).expect("hydrated compiles");
        assert_eq!(hydrated_price, price);
        let stated = hydrated.content.models()[0].offerings[0]
            .price
            .as_ref()
            .expect("cost is stated");
        assert_eq!(model_price_from_cost(stated).expect("exact micros"), price);
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

    fn usage(input_tokens: u64, output_tokens: u64) -> gateway_core::Usage {
        gateway_core::Usage {
            input_tokens,
            output_tokens,
            reasoning_tokens: 0,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
        }
    }

    /// models.dev `cost.input`/`cost.output` of 5/30 USD per million tokens
    /// become 5_000_000/30_000_000 µUSD per million; 1M+1M tokens charge
    /// 35_000_000 µUSD with no `[[price]]` row.
    #[test]
    fn seed_charges_from_models_dev_cost_without_a_price_book() {
        let index = CatalogPriceIndex::from_snapshot(&crate::backends::models_dev::seed_snapshot());
        let rates = index
            .price_for("openai", "gpt-5.5")
            .expect("openai publishes gpt-5.5 with a usable cost");
        assert_eq!(rates.input_microdollars_per_million, 5_000_000);
        assert_eq!(rates.output_microdollars_per_million, 30_000_000);
        assert_eq!(rates.cache_read_microdollars_per_million, Some(500_000));
        assert_eq!(
            rates.cost_microdollars(usage(1_000_000, 1_000_000)),
            35_000_000
        );
        assert!(
            index.price_for("openai", "does-not-exist").is_none(),
            "an offering the snapshot does not price is omitted, not free"
        );
        let gpt_4o = index
            .price_for("openai", "gpt-4o")
            .expect("the seed also prices gpt-4o");
        assert_eq!(gpt_4o.input_microdollars_per_million, 2_500_000);
        assert_eq!(gpt_4o.output_microdollars_per_million, 10_000_000);
    }

    #[test]
    fn empty_cost_is_unpublished_not_indexed() {
        let payload = r#"{
            "models": {
                "openai/unpriced": {
                    "id": "openai/unpriced",
                    "name": "Unpriced",
                    "modalities": {"input": ["text"], "output": ["text"]}
                }
            },
            "providers": {
                "openai": {
                    "id": "openai",
                    "name": "OpenAI",
                    "env": ["OPENAI_API_KEY"],
                    "models": {
                        "unpriced": {
                            "id": "unpriced",
                            "name": "Unpriced",
                            "modalities": {"input": ["text"], "output": ["text"]},
                            "cost": {}
                        }
                    }
                }
            }
        }"#;
        let snapshot = ModelsDevAdapter::default()
            .parse(
                payload.as_bytes(),
                SourceValidators::etag("\"empty-cost\""),
                UNIX_EPOCH,
            )
            .expect("empty cost is unpublished, not a parse error");
        let index = CatalogPriceIndex::from_snapshot(&snapshot);
        assert!(
            index.price_for("openai", "unpriced").is_none(),
            "empty cost must not become a zero charge"
        );
    }

    /// A request copies rates at bind time; a later snapshot cannot reprice it.
    #[test]
    fn a_later_snapshot_does_not_reprice_an_already_bound_request() {
        let opened = CatalogPriceIndex::from_snapshot(
            &LocalCatalogBuilder::new("openai", "gpt-5.5")
                .price(5_000_000, 30_000_000)
                .snapshot(tenant(), UNIX_EPOCH)
                .expect("snapshot A"),
        )
        .price_for("openai", "gpt-5.5")
        .expect("priced");
        let bound = crate::pricing::RequestPrice::configured(opened);

        let after = CatalogPriceIndex::from_snapshot(
            &LocalCatalogBuilder::new("openai", "gpt-5.5")
                .price(4_000_000, 30_000_000)
                .snapshot(tenant(), UNIX_EPOCH)
                .expect("snapshot B"),
        )
        .price_for("openai", "gpt-5.5")
        .expect("still priced");

        assert_eq!(
            bound.cost_microdollars(usage(1_000_000, 0)),
            Some(5_000_000)
        );
        assert_eq!(after.cost_microdollars(usage(1_000_000, 0)), 4_000_000);
        assert_ne!(opened, after);
    }
}
