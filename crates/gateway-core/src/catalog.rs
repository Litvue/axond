use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    #[serde(default)]
    pub reasoning_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelPrice {
    pub input_microdollars_per_million: u64,
    pub output_microdollars_per_million: u64,
    #[serde(default)]
    pub reasoning_microdollars_per_million: Option<u64>,
    #[serde(default)]
    pub cache_read_microdollars_per_million: Option<u64>,
    #[serde(default)]
    pub cache_write_microdollars_per_million: Option<u64>,
}

impl ModelPrice {
    /// Cost of a usage record in integer micro-dollars. Reasoning tokens are a
    /// subset of output tokens and are priced separately (falling back to the
    /// output rate); cache reads/writes fall back to the input rate. Saturates
    /// rather than overflowing.
    pub fn cost_microdollars(&self, usage: Usage) -> u64 {
        let billed_output_tokens = usage.output_tokens.saturating_sub(usage.reasoning_tokens);
        let cost = component(usage.input_tokens, self.input_microdollars_per_million)
            .saturating_add(component(
                billed_output_tokens,
                self.output_microdollars_per_million,
            ))
            .saturating_add(component(
                usage.reasoning_tokens,
                self.reasoning_microdollars_per_million
                    .unwrap_or(self.output_microdollars_per_million),
            ))
            .saturating_add(component(
                usage.cache_read_tokens,
                self.cache_read_microdollars_per_million
                    .unwrap_or(self.input_microdollars_per_million),
            ))
            .saturating_add(component(
                usage.cache_write_tokens,
                self.cache_write_microdollars_per_million
                    .unwrap_or(self.input_microdollars_per_million),
            ));
        u64::try_from(cost).unwrap_or(u64::MAX)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogModel {
    pub id: String,
    pub price: ModelPrice,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCatalog {
    pub version: u64,
    pub models: Vec<CatalogModel>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageReceipt {
    pub catalog_version: u64,
    pub model: String,
    pub price: ModelPrice,
    pub usage: Usage,
    pub cost_microdollars: u64,
}

impl UsageReceipt {
    pub fn normalized_cost_microdollars(&self) -> u64 {
        self.cost_microdollars
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CatalogError {
    #[error("duplicate model '{0}' in catalog")]
    DuplicateModel(String),
    #[error("model '{0}' is not priced in catalog")]
    UnknownModel(String),
    #[error("usage cost overflow")]
    CostOverflow,
}

impl ModelCatalog {
    pub fn validate(&self) -> Result<(), CatalogError> {
        let mut seen = HashMap::new();
        for model in &self.models {
            if seen.insert(&model.id, ()).is_some() {
                return Err(CatalogError::DuplicateModel(model.id.clone()));
            }
        }
        Ok(())
    }

    pub fn receipt(&self, model: &str, usage: Usage) -> Result<UsageReceipt, CatalogError> {
        let price = self
            .models
            .iter()
            .find(|entry| entry.id == model)
            .map(|entry| entry.price)
            .ok_or_else(|| CatalogError::UnknownModel(model.to_owned()))?;
        let cost_microdollars = price.cost_microdollars(usage);
        Ok(UsageReceipt {
            catalog_version: self.version,
            model: model.to_owned(),
            price,
            usage,
            cost_microdollars,
        })
    }
}

fn component(tokens: u64, microdollars_per_million: u64) -> u128 {
    u128::from(tokens).saturating_mul(u128::from(microdollars_per_million)) / 1_000_000
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog(version: u64, output_price: u64) -> ModelCatalog {
        ModelCatalog {
            version,
            models: vec![CatalogModel {
                id: "openai/model".into(),
                price: ModelPrice {
                    input_microdollars_per_million: 1_000_000,
                    output_microdollars_per_million: output_price,
                    reasoning_microdollars_per_million: None,
                    cache_read_microdollars_per_million: Some(100_000),
                    cache_write_microdollars_per_million: Some(1_250_000),
                },
            }],
        }
    }

    #[test]
    fn receipt_pins_catalog_version_and_normalizes_cost() {
        let usage = Usage {
            input_tokens: 1_000,
            output_tokens: 500,
            reasoning_tokens: 0,
            cache_read_tokens: 1_000,
            cache_write_tokens: 200,
        };
        let receipt = catalog(7, 2_000_000)
            .receipt("openai/model", usage)
            .unwrap();
        assert_eq!(receipt.catalog_version, 7);
        assert_eq!(receipt.price, catalog(7, 2_000_000).models[0].price);
        assert_eq!(receipt.cost_microdollars, 2_350);
        assert_eq!(receipt.normalized_cost_microdollars(), 2_350);
        assert_eq!(
            catalog(8, 4_000_000)
                .receipt("openai/model", usage)
                .unwrap()
                .cost_microdollars,
            3_350
        );
    }

    #[test]
    fn optional_reasoning_price_replaces_output_price_for_reasoning_subset() {
        let mut catalog = catalog(9, 2_000_000);
        catalog.models[0].price.reasoning_microdollars_per_million = Some(4_000_000);
        let receipt = catalog
            .receipt(
                "openai/model",
                Usage {
                    input_tokens: 0,
                    output_tokens: 1_000,
                    reasoning_tokens: 250,
                    cache_read_tokens: 0,
                    cache_write_tokens: 0,
                },
            )
            .unwrap();
        assert_eq!(receipt.cost_microdollars, 2_500);
        assert_eq!(receipt.catalog_version, 9);
        assert_eq!(
            receipt.price.reasoning_microdollars_per_million,
            Some(4_000_000)
        );
    }
}
