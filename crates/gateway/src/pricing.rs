//! What a request is charged at.
//!
//! Charging after ADR 0063 is [`crate::routes`] `serve`: imported models.dev
//! `cost` from [`crate::backends::catalog_runtime::CatalogStatus`], then an
//! optional `[[price]]` row, then the provider's `unpriced_models` stance.
//!
//! Two rules make that safe for a request already in flight:
//!
//! 1. Resolution copies rates onto [`RequestPrice`] when the request starts, so
//!    a catalogue admitted mid-request cannot change what that request settles
//!    at.
//! 2. A target is priced by one authority at a time — the admitted snapshot
//!    first, then a `[[price]]` fallback — never a merge of the two.

use gateway_core::catalog::{ModelPrice, Usage};

/// The rates one request is charged at.
///
/// Copied into the streaming context and the served target rather than
/// borrowed: settlement can outlive the handler (a stream settles in a detached
/// task), and a charge must not observe anything but the pricing the request
/// started under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestPrice {
    rates: Option<ModelPrice>,
}

impl RequestPrice {
    /// Rates copied at admission: imported models.dev `cost`, or a `[[price]]`
    /// fallback when the snapshot does not cover the offering.
    pub const fn configured(rates: ModelPrice) -> Self {
        Self { rates: Some(rates) }
    }

    /// Unpriced + `allow`: dispatch, record `cost_microdollars` as NULL.
    pub const fn unpriced() -> Self {
        Self { rates: None }
    }

    /// The integer micro-dollar cost of a usage report at these rates, or
    /// `None` when the request was admitted unpriced.
    pub fn cost_microdollars(&self, usage: Usage) -> Option<u64> {
        self.rates.map(|rates| rates.cost_microdollars(usage))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rates(input: u64, output: u64) -> ModelPrice {
        ModelPrice {
            input_microdollars_per_million: input,
            output_microdollars_per_million: output,
            reasoning_microdollars_per_million: None,
            cache_read_microdollars_per_million: None,
            cache_write_microdollars_per_million: None,
        }
    }

    fn usage(input_tokens: u64, output_tokens: u64) -> Usage {
        Usage {
            input_tokens,
            output_tokens,
            reasoning_tokens: 0,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
        }
    }

    /// Every rate reaches the charge, and each bills its own token counter
    /// rather than falling back to input or output.
    #[test]
    fn reasoning_and_cache_rates_bill_their_own_tokens() {
        let price = RequestPrice::configured(ModelPrice {
            reasoning_microdollars_per_million: Some(20_000),
            cache_read_microdollars_per_million: Some(1_000),
            cache_write_microdollars_per_million: Some(3_000),
            ..rates(2_000, 4_000)
        });
        // A million tokens of each. Reasoning is a subset of output, so the
        // output rate bills the remainder — here none of it — and the total is
        // input 2 000 + reasoning 20 000 + cache read 1 000 + cache write 3 000.
        let cost = price.cost_microdollars(Usage {
            input_tokens: 1_000_000,
            output_tokens: 1_000_000,
            reasoning_tokens: 1_000_000,
            cache_read_tokens: 1_000_000,
            cache_write_tokens: 1_000_000,
        });
        assert_eq!(cost, Some(26_000));
    }

    /// Charging is integer micro-dollars throughout: a partial micro-dollar of
    /// consumption is truncated, never rounded up.
    #[test]
    fn a_charge_truncates_the_micro_dollar_it_did_not_reach() {
        let price = RequestPrice::configured(rates(2_000, 4_000));
        assert_eq!(price.cost_microdollars(usage(499, 0)), Some(0));
        assert_eq!(price.cost_microdollars(usage(500, 0)), Some(1));
        assert_eq!(price.cost_microdollars(usage(999, 0)), Some(1));
    }

    #[test]
    fn an_unpriced_request_records_no_cost() {
        assert_eq!(
            RequestPrice::unpriced().cost_microdollars(usage(1_000, 1_000)),
            None
        );
    }
}
