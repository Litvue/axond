//! Billable rates compiled from an imported models.dev snapshot.
//!
//! The request path prices an offering from the snapshot the importer last
//! admitted ([`super::catalog_runtime::CatalogStatus::price_for`]).

use std::collections::HashMap;

use gateway_core::ModelPrice;

use super::catalog::{CatalogSnapshot, ObservedPrice};

/// Compile a models.dev `cost` into billable micro-dollar rates, when exact.
///
/// Nano-dollars that are not a whole micro-dollar are unpublished: charging a
/// truncated rate would disagree with the observation, and rounding up would
/// overcharge. Optional cache/reasoning rates convert the same way; a stated
/// optional that is not exact makes the whole offering unusable rather than
/// silently falling back to input/output.
///
/// A published schedule with context tiers is unpublished for the same reason:
/// [`ModelPrice`] has no threshold, so compiling only the base rates would bill
/// long-context requests below what the source states. Audio rates are
/// unpublished because [`gateway_core::Usage`] has no audio token field, so
/// compiling the rest would let audio usage escape the charge. Such offerings
/// fall to the `[[price]]` fallback, where an operator can state the rate they
/// accept.
pub fn model_price_from_cost(price: &ObservedPrice) -> Option<ModelPrice> {
    if !price.tiers.is_empty() {
        return None;
    }
    if price.base.input_audio.is_some() || price.base.output_audio.is_some() {
        return None;
    }
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

#[cfg(test)]
mod tests {
    use std::time::UNIX_EPOCH;

    use super::*;
    use crate::backends::catalog::SourceValidators;
    use crate::backends::models_dev::ModelsDevAdapter;

    fn usage(input_tokens: u64, output_tokens: u64) -> gateway_core::Usage {
        gateway_core::Usage {
            input_tokens,
            output_tokens,
            reasoning_tokens: 0,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
        }
    }

    /// models.dev `cost.input`/`cost.output` of 2.5/10 USD per million tokens
    /// become 2_500_000/10_000_000 µUSD per million; 1M+1M tokens charge
    /// 12_500_000 µUSD with no `[[price]]` row.
    #[test]
    fn seed_charges_from_models_dev_cost_without_a_price_book() {
        let index = CatalogPriceIndex::from_snapshot(&crate::backends::models_dev::seed_snapshot());
        let rates = index
            .price_for("openai", "gpt-4o")
            .expect("openai publishes gpt-4o with a usable cost");
        assert_eq!(rates.input_microdollars_per_million, 2_500_000);
        assert_eq!(rates.output_microdollars_per_million, 10_000_000);
        assert_eq!(rates.cache_read_microdollars_per_million, Some(1_250_000));
        assert_eq!(
            rates.cost_microdollars(usage(1_000_000, 1_000_000)),
            12_500_000
        );
        assert!(
            index.price_for("openai", "does-not-exist").is_none(),
            "an offering the snapshot does not price is omitted, not free"
        );
        let azure = index
            .price_for("azure", "gpt-4o")
            .expect("the seed also prices azure/gpt-4o");
        assert_eq!(azure.input_microdollars_per_million, 2_500_000);
        assert_eq!(azure.output_microdollars_per_million, 10_000_000);
    }

    /// The seed publishes `gpt-5.5` with a `context_over_200k` tier at twice
    /// the base rate. A flat [`ModelPrice`] cannot hold that schedule, so the
    /// offering is omitted rather than billed at the base rate for every
    /// context size; a `[[price]]` row is then the operator's rate.
    #[test]
    fn tiered_cost_is_not_flattened_to_its_base_rate() {
        let snapshot = crate::backends::models_dev::seed_snapshot();
        let observed = snapshot
            .content
            .models()
            .iter()
            .flat_map(|model| &model.offerings)
            .find(|offering| {
                offering.provider.as_str() == "openai" && offering.published_model_id == "gpt-5.5"
            })
            .and_then(|offering| offering.price.as_ref())
            .expect("the seed states a cost for openai/gpt-5.5");
        assert!(!observed.tiers.is_empty(), "the fixture must stay tiered");
        assert_eq!(model_price_from_cost(observed), None);
        let index = CatalogPriceIndex::from_snapshot(&snapshot);
        assert!(
            index.price_for("openai", "gpt-5.5").is_none(),
            "a tiered offering must not charge its base rate for long context"
        );
    }

    /// Same fail-closed rule as the approved book: an `input_audio` /
    /// `output_audio` rate has no usage field to bill, so the offering is
    /// omitted rather than charged as text-only.
    #[test]
    fn an_audio_rate_is_not_compiled_because_no_usage_field_would_bill_it() {
        let payload = r#"{
            "models": {
                "openai/gpt-audio": {
                    "id": "openai/gpt-audio",
                    "name": "GPT Audio",
                    "modalities": {"input": ["text", "audio"], "output": ["text"]}
                }
            },
            "providers": {
                "openai": {
                    "id": "openai",
                    "name": "OpenAI",
                    "env": ["OPENAI_API_KEY"],
                    "models": {
                        "gpt-audio": {
                            "id": "gpt-audio",
                            "name": "GPT Audio",
                            "modalities": {"input": ["text", "audio"], "output": ["text"]},
                            "cost": {"input": 5, "output": 20, "input_audio": 40}
                        }
                    }
                }
            }
        }"#;
        let snapshot = ModelsDevAdapter::default()
            .parse(
                payload.as_bytes(),
                SourceValidators::etag("\"audio-cost\""),
                UNIX_EPOCH,
            )
            .expect("audio cost parses");
        let observed = snapshot.content.models()[0].offerings[0]
            .price
            .as_ref()
            .expect("cost is stated");
        assert!(observed.base.input_audio.is_some());
        assert_eq!(model_price_from_cost(observed), None);
        let index = CatalogPriceIndex::from_snapshot(&snapshot);
        assert!(
            index.price_for("openai", "gpt-audio").is_none(),
            "an audio schedule must not charge as text-only"
        );
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
}
