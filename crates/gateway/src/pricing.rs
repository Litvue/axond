//! What a request is charged at, and which approved state said so.
//!
//! The pricing domain ([`crate::desired_state::pricing`]) decides what an
//! operator approved and resolves it into an immutable
//! [`PricingSnapshot`] at compile time. This module is the request path's half of
//! that seam: it maps a routed [`Target`] onto the snapshot's approved rates,
//! answers *ineligible* rather than *free* when nothing approved covers it, and
//! carries the identity of the pricing a charge was computed against all the way
//! to the usage record.
//!
//! Three rules make a publication safe for a request already in flight:
//!
//! 1. Resolution reads the [`ConfigSnapshot`] the request is already holding, so
//!    a book published mid-request cannot change what that request settles at.
//! 2. A target is priced by exactly one authority — the deployment's approved
//!    book when the target names a catalogue offering the book prices, and the
//!    `[[model]]` rates otherwise — never by a merge of the two.
//! 3. A target the approved book *should* price but does not is ineligible: it
//!    stays discoverable and is not routable under a budget (ADR 0056).

use gateway_core::catalog::{ModelPrice, Usage};

use crate::backends::catalog::CatalogContentId;
use crate::config::{Model, Target};
use crate::desired_state::pricing::PricingSnapshot;
use crate::desired_state::{Checksum, ResourceRef};
use crate::state::ConfigSnapshot;

/// The immutable pricing state a charge was computed against.
///
/// Three identities rather than one number: the price-book resource *version*
/// answers "which approved document", its checksum answers "the same document on
/// every replica", and the catalogue content id answers "against which observed
/// catalogue was it approved". A monotonic counter answers none of them, which is
/// why the legacy `catalog_version` placeholder is derived from the version here
/// rather than kept as the whole answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriceIdentity {
    book: ResourceRef,
    checksum: Checksum,
    catalog: CatalogContentId,
}

impl PriceIdentity {
    /// The identity of the pricing a snapshot serves under.
    pub const fn of(pricing: &PricingSnapshot) -> Self {
        Self {
            book: pricing.book(),
            checksum: pricing.checksum(),
            catalog: pricing.catalog(),
        }
    }

    /// The price-book resource version, as a `ResourceRef` renders it:
    /// `price/<resource id>@v<n>`.
    pub fn book(&self) -> String {
        self.book.to_string()
    }

    /// The book's version number on its own, for the numeric field a usage row
    /// has carried since the first schema.
    pub const fn version(&self) -> u64 {
        self.book.version.get()
    }

    /// The checksum of the approved body, `sha256:`-prefixed.
    pub fn checksum(&self) -> String {
        self.checksum.to_string()
    }

    /// The catalogue content the book was approved against.
    pub fn catalog(&self) -> String {
        self.catalog.to_string()
    }
}

/// The rates one request is charged at, and where they came from.
///
/// Copied into the streaming context and the served target rather than borrowed:
/// settlement can outlive the handler's borrow of the snapshot (a stream settles
/// in a detached task), and a charge must not be able to observe anything but the
/// pricing the request started under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestPrice {
    rates: ModelPrice,
    identity: Option<PriceIdentity>,
}

impl RequestPrice {
    /// Rates a `[[model]]` target declared, in a deployment whose revision
    /// published no price book — or whose book prices other offerings.
    pub const fn configured(rates: ModelPrice) -> Self {
        Self {
            rates,
            identity: None,
        }
    }

    /// Rates an approved price book put in force, named by the identity that
    /// approved them.
    pub const fn approved(rates: ModelPrice, identity: PriceIdentity) -> Self {
        Self {
            rates,
            identity: Some(identity),
        }
    }

    /// The integer micro-dollar cost of a usage report at these rates.
    pub fn cost_microdollars(&self, usage: Usage) -> u64 {
        self.rates.cost_microdollars(usage)
    }

    /// The rates themselves. Charging goes through `cost_microdollars`, so this
    /// is for tests that assert *which* rates a target resolved to.
    #[cfg(test)]
    pub const fn rates(&self) -> ModelPrice {
        self.rates
    }

    /// The approved pricing this charge came from, or `None` when the file
    /// config priced it.
    pub const fn identity(&self) -> Option<PriceIdentity> {
        self.identity
    }

    /// The numeric price-book version a usage row records: `0` when no approved
    /// book priced the request, which is exactly what the placeholder meant
    /// before there was a version to record.
    pub const fn catalog_version(&self) -> u64 {
        match &self.identity {
            None => 0,
            Some(identity) => identity.version(),
        }
    }
}

/// Why a target cannot be charged, and therefore cannot be routed.
///
/// One arm, deliberately: a target that names no catalogue offering is not an
/// error — the deployment's book prices offerings, and a target outside that
/// vocabulary keeps the rates its `[[model]]` entry declares. What is refused is
/// a target the book *is* the authority for and does not price, because serving
/// it would either charge rates nobody approved or charge nothing at all.
///
/// The refusal a caller is given is stable and says nothing more: which price
/// book a deployment runs, at which resource version, and whether it is still a
/// draft are control-plane facts an unprivileged caller of the data plane must
/// not learn from an error body. The identity stays in the variant for the
/// operator log, and [`Ineligible::reason`] is what crosses the boundary.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Ineligible {
    #[error("no price is in force for this model")]
    Unpriced {
        provider: String,
        model: String,
        book: String,
        approval: &'static str,
    },
}

impl Ineligible {
    /// The stable, redacted reason a refusal is reported to a caller as. Stable
    /// so a client can branch on it, redacted so it names no internal resource.
    pub const fn reason(&self) -> &'static str {
        match self {
            Self::Unpriced { .. } => "no price is in force for this model",
        }
    }

    /// The same refusal for the operator log: the offering, the price book that
    /// is its authority, and that book's approval state — the three facts that
    /// answer "approve what, where?" and none of which reach a caller.
    pub fn detail(&self) -> String {
        match self {
            Self::Unpriced {
                provider,
                model,
                book,
                approval,
            } => format!(
                "catalogue offering `{provider}`/`{model}` has no approved price in {book} ({approval})"
            ),
        }
    }
}

/// What each of an alias's targets is charged at, resolved once per request.
///
/// Positional: index *i* answers for `model.targets[i]`, so the failover walk can
/// skip an ineligible target without re-resolving anything, and every attempt at
/// one target is priced identically to the estimate the hold was taken from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AliasPrices {
    priced: Vec<Result<RequestPrice, Ineligible>>,
}

impl AliasPrices {
    /// Resolve every target of an alias against a snapshot's approved pricing.
    pub fn resolve(snapshot: &ConfigSnapshot, model: &Model) -> Self {
        Self {
            priced: model
                .targets
                .iter()
                .map(|target| price_of(snapshot.pricing(), target))
                .collect(),
        }
    }

    /// The price for one target position, or `None` when that target is
    /// ineligible.
    pub fn get(&self, index: usize) -> Option<RequestPrice> {
        self.priced
            .get(index)
            .and_then(|priced| priced.as_ref().ok().copied())
    }

    /// The price the hold is estimated from: the first target that can be
    /// charged at all, which is the first target the walk can attempt.
    pub fn estimate(&self) -> Option<RequestPrice> {
        self.priced
            .iter()
            .find_map(|priced| priced.as_ref().ok().copied())
    }

    /// Why one target position cannot be charged. The failover walk reports this
    /// when the target it was pinned to is the ineligible one, so a pinned
    /// destination is refused for the reason it was skipped for rather than as a
    /// walk that found nothing to attempt.
    pub fn ineligible(&self, index: usize) -> Option<&Ineligible> {
        self.priced
            .get(index)
            .and_then(|priced| priced.as_ref().err())
    }

    /// Why the alias cannot be routed, when none of its targets can be charged.
    pub fn refusal(&self) -> Option<&Ineligible> {
        if self.estimate().is_some() {
            return None;
        }
        self.priced.iter().find_map(|priced| priced.as_ref().err())
    }
}

/// Which authority prices one routed target, under one snapshot's pricing.
fn price_of(
    pricing: Option<&PricingSnapshot>,
    target: &Target,
) -> Result<RequestPrice, Ineligible> {
    // No revision published a book: the deployment is priced by its file, which
    // is the only pricing a stateless deployment has ever had.
    let Some(pricing) = pricing else {
        return Ok(RequestPrice::configured(target.price));
    };
    // The book prices catalogue offerings, and a target that names none is
    // outside its vocabulary. Binding is explicit because the operator-chosen
    // `[[provider]] id` and the catalogue's provider id are unrelated namespaces
    // that only coincide by coincidence.
    let Some(catalog) = &target.catalog else {
        return Ok(RequestPrice::configured(target.price));
    };
    match pricing.price(&catalog.provider, &catalog.model) {
        Some(rates) => Ok(RequestPrice::approved(rates, PriceIdentity::of(pricing))),
        // Discoverable, not chargeable: an unapproved or uncovered offering is
        // ineligible for budget-controlled routing rather than free.
        None => Err(Ineligible::Unpriced {
            provider: catalog.provider.to_string(),
            model: catalog.model.clone(),
            book: pricing.book().to_string(),
            approval: pricing.approval().state(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CatalogBinding;
    use crate::desired_state::fixtures;
    use crate::desired_state::ids::Slug;
    use crate::desired_state::pricing::{
        Approval, ApprovedRate, ApprovedRates, EffectiveInstant, EffectiveInterval, PriceBookBody,
        PriceBooks, PriceOrigin, PriceProvenance, PriceRule, RulePrecedence,
    };
    use crate::desired_state::resource::ResourceVersionNumber;

    fn configured() -> ModelPrice {
        ModelPrice {
            input_microdollars_per_million: 7,
            output_microdollars_per_million: 9,
            reasoning_microdollars_per_million: None,
            cache_read_microdollars_per_million: None,
            cache_write_microdollars_per_million: None,
        }
    }

    fn target(catalog: Option<CatalogBinding>) -> Target {
        Target {
            provider: "primary".to_owned(),
            model: "gpt-4o".to_owned(),
            price: configured(),
            catalog,
        }
    }

    fn binding(provider: &str, model: &str) -> CatalogBinding {
        CatalogBinding::new(provider, model).expect("a catalogue binding")
    }

    /// The pricing a deployment serves under when `body` is its price book at
    /// resource version `version`. A version is chosen rather than defaulted
    /// because an amendment and a rollback are both new versions of one book.
    fn pricing(body: &PriceBookBody, version: u64) -> PricingSnapshot {
        let mut state = fixtures::state();
        state
            .insert(body.version_at(
                fixtures::resource_id(7),
                Slug::parse("baseline").expect("fixture slug"),
                ResourceVersionNumber::new(version).expect("a version is not zero"),
            ))
            .expect("a distinct reference");
        PriceBooks::of(&state)
            .expect("the book is servable state")
            .snapshot_at(EffectiveInstant::EPOCH)
            .expect("the state holds a book")
    }

    /// An approved book pricing `openai/gpt-4o` at `input`/`output` nano-dollars
    /// from the epoch onwards.
    fn book(input_nanos: u64, output_nanos: u64) -> PriceBookBody {
        PriceBookBody::new(
            fixtures::catalog_content_id(),
            Approval::Approved {
                by: fixtures::actor(),
                at: EffectiveInstant::EPOCH,
                citation: Some(fixtures::display_name("CHG-1")),
            },
        )
        .with_rule(fixtures::price_rule(
            fixtures::priced_target("openai", "gpt-4o"),
            RulePrecedence::Baseline,
            EffectiveInterval::from(EffectiveInstant::EPOCH),
            input_nanos,
            output_nanos,
        ))
    }

    fn bound() -> Target {
        target(Some(binding("openai", "gpt-4o")))
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

    #[test]
    fn a_deployment_with_no_price_book_is_priced_by_its_file() {
        let resolved = price_of(None, &target(Some(binding("openai", "gpt-4o"))))
            .expect("a file-priced target is chargeable");
        assert_eq!(resolved.rates(), configured());
        assert_eq!(resolved.catalog_version(), 0);
        assert!(resolved.identity().is_none());
    }

    #[test]
    fn a_target_outside_the_books_vocabulary_keeps_its_declared_rates() {
        let pricing = fixtures::approved_pricing_snapshot();
        let resolved =
            price_of(Some(&pricing), &target(None)).expect("an unbound target is chargeable");
        assert_eq!(resolved.rates(), configured());
        assert!(resolved.identity().is_none());
    }

    #[test]
    fn an_approved_book_prices_a_bound_target_and_names_its_identity() {
        let pricing = fixtures::approved_pricing_snapshot();
        let resolved = price_of(Some(&pricing), &target(Some(binding("openai", "gpt-4o"))))
            .expect("an approved target is chargeable");
        assert_eq!(
            resolved.rates(),
            pricing
                .price(&binding("openai", "gpt-4o").provider, "gpt-4o")
                .expect("the fixture book prices it")
        );
        let identity = resolved.identity().expect("the charge names its book");
        assert_eq!(identity.version(), pricing.book().version.get());
        assert_eq!(resolved.catalog_version(), identity.version());
        assert_eq!(identity.catalog(), pricing.catalog().to_string());
        assert_eq!(identity.checksum(), pricing.checksum().to_string());
    }

    #[test]
    fn an_offering_the_book_does_not_price_is_ineligible_rather_than_free() {
        let pricing = fixtures::approved_pricing_snapshot();
        let refusal = price_of(Some(&pricing), &target(Some(binding("openai", "o3"))))
            .expect_err("an unpriced offering cannot be charged");
        let Ineligible::Unpriced { model, .. } = &refusal;
        assert_eq!(model, "o3");
    }

    /// The approval gate at the runtime seam: a draft book resolves to no prices
    /// at all, so a bound target is ineligible rather than priced from a document
    /// nobody approved.
    #[test]
    fn a_draft_book_prices_nothing_it_covers() {
        let body = PriceBookBody::new(fixtures::catalog_content_id(), Approval::Draft).with_rule(
            fixtures::price_rule(
                fixtures::priced_target("openai", "gpt-4o"),
                RulePrecedence::Baseline,
                EffectiveInterval::from(EffectiveInstant::EPOCH),
                2_500_000,
                10_000_000,
            ),
        );
        let pricing = PriceBooks::of(&fixtures::state_with_price_book(&body))
            .expect("a draft book is servable state")
            .snapshot_at(EffectiveInstant::EPOCH)
            .expect("the state holds a book");
        let refusal = price_of(Some(&pricing), &target(Some(binding("openai", "gpt-4o"))))
            .expect_err("a draft book activates no price");
        let Ineligible::Unpriced { approval, .. } = &refusal;
        assert_eq!(*approval, "draft");
    }

    /// What a caller is told is what every caller is told: an unpriced offering
    /// and a draft book refuse identically, and neither refusal names the book, a
    /// resource id, a version, or an approval state. The operator detail keeps
    /// all four, so redaction costs the audit trail nothing.
    #[test]
    fn a_refusal_names_no_price_book_to_the_caller_and_all_of_it_to_the_log() {
        let approved = fixtures::approved_pricing_snapshot();
        let draft_body = PriceBookBody::new(fixtures::catalog_content_id(), Approval::Draft)
            .with_rule(fixtures::price_rule(
                fixtures::priced_target("openai", "gpt-4o"),
                RulePrecedence::Baseline,
                EffectiveInterval::from(EffectiveInstant::EPOCH),
                2_500_000,
                10_000_000,
            ));
        let draft = PriceBooks::of(&fixtures::state_with_price_book(&draft_body))
            .expect("a draft book is servable state")
            .snapshot_at(EffectiveInstant::EPOCH)
            .expect("the state holds a book");

        for (snapshot, model) in [(&approved, "o3"), (&draft, "gpt-4o")] {
            let refusal = price_of(Some(snapshot), &target(Some(binding("openai", model))))
                .expect_err("neither snapshot has an approved price for it");

            // One stable string for both, so a client can branch on it.
            assert_eq!(refusal.reason(), "no price is in force for this model");
            let public = refusal.to_string();
            assert_eq!(public, refusal.reason());
            for leak in [
                snapshot.book().to_string(),
                snapshot.book().id.to_string(),
                snapshot.checksum().to_string(),
                snapshot.catalog().to_string(),
                snapshot.approval().state().to_owned(),
            ] {
                assert!(
                    !public.contains(&leak),
                    "refusal `{public}` discloses `{leak}`"
                );
            }

            // The operator still learns which book to go and approve in.
            let detail = refusal.detail();
            assert!(detail.contains(&snapshot.book().to_string()), "{detail}");
            assert!(detail.contains(snapshot.approval().state()), "{detail}");
            assert!(detail.contains(model), "{detail}");
        }
    }

    /// The rendering `docs/usage-schema.md` promises of the `price_book` column,
    /// pinned: a `ResourceRef`, so `price/<resource id>@v<version>` and not a
    /// `price-book/…` kind that does not exist.
    #[test]
    fn a_charges_price_book_renders_as_the_resource_reference_it_is() {
        let pricing = fixtures::approved_pricing_snapshot();
        let identity = price_of(Some(&pricing), &target(Some(binding("openai", "gpt-4o"))))
            .expect("the fixture book prices it")
            .identity()
            .expect("an approved charge names its book")
            .book();
        assert_eq!(identity, pricing.book().to_string());
        let (kind, version) = identity
            .split_once('/')
            .and_then(|(kind, rest)| rest.split_once('@').map(|(_, version)| (kind, version)))
            .expect("a reference renders as `<kind>/<id>@<version>`");
        assert_eq!(kind, "price");
        assert!(version.starts_with('v'), "{identity}");
        assert!(identity.contains("/res_"), "{identity}");
    }

    /// The acceptance criterion for a request that spans a publication: the
    /// price it opened under is a value it already holds, so the later book
    /// prices later requests and settles nothing about this one.
    #[test]
    fn a_publication_cannot_change_what_an_open_request_settles_at() {
        let opened = price_of(Some(&pricing(&book(2_000_000, 4_000_000), 1)), &bound())
            .expect("the request opened under an approved price");

        // The next revision doubles both rates while the request is in flight.
        let published = pricing(&book(4_000_000, 8_000_000), 2);
        let later = price_of(Some(&published), &bound()).expect("a later request is priced too");

        // 2 000 000 nano-dollars per million tokens is 2 000 micro-dollars.
        assert_eq!(opened.cost_microdollars(usage(1_000_000, 1_000_000)), 6_000);
        assert_eq!(later.cost_microdollars(usage(1_000_000, 1_000_000)), 12_000);
        assert_eq!(opened.catalog_version(), 1);
        assert_eq!(later.catalog_version(), 2);
    }

    /// Rollback is republication, not mutation: the earlier body comes back as a
    /// *new* version, so the rates and the checksum are the ones that were
    /// audited before, and the version says which publication is in force.
    #[test]
    fn a_rollback_republishes_the_earlier_rates_under_a_new_version() {
        let original = book(2_000_000, 4_000_000);
        let amended = book(4_000_000, 8_000_000);

        let before = price_of(Some(&pricing(&original, 1)), &bound()).expect("priced");
        let after = price_of(Some(&pricing(&amended, 2)), &bound()).expect("priced");
        let rolled_back = price_of(Some(&pricing(&original, 3)), &bound()).expect("priced");

        assert_eq!(rolled_back.rates(), before.rates());
        assert_ne!(rolled_back.rates(), after.rates());
        let identity = rolled_back.identity().expect("a rollback names its book");
        assert_eq!(identity.version(), 3);
        assert_eq!(
            identity.checksum(),
            before
                .identity()
                .expect("the original names its book")
                .checksum(),
            "republishing one body must reproduce its checksum, or a diff of the \
             two publications would not show them as the same rates"
        );
    }

    /// Every rate an approved schedule can state reaches the charge, and each
    /// bills its own token counter rather than falling back to input or output.
    #[test]
    fn reasoning_and_cache_rates_bill_their_own_tokens() {
        let rates = ApprovedRates {
            reasoning: Some(ApprovedRate::from_nanos(20_000_000)),
            cache_read: Some(ApprovedRate::from_nanos(1_000_000)),
            cache_write: Some(ApprovedRate::from_nanos(3_000_000)),
            ..ApprovedRates::new(
                ApprovedRate::from_nanos(2_000_000),
                ApprovedRate::from_nanos(4_000_000),
            )
        };
        let body = PriceBookBody::new(
            fixtures::catalog_content_id(),
            Approval::Approved {
                by: fixtures::actor(),
                at: EffectiveInstant::EPOCH,
                citation: Some(fixtures::display_name("CHG-2")),
            },
        )
        .with_rule(
            PriceRule::new(
                fixtures::priced_target("openai", "gpt-4o"),
                RulePrecedence::Baseline,
                EffectiveInterval::from(EffectiveInstant::EPOCH),
                rates,
                PriceProvenance::stated(PriceOrigin::Catalogue),
            )
            .expect("whole micro-dollar rates convert"),
        );
        let resolved = price_of(Some(&pricing(&body, 1)), &bound()).expect("priced");

        // A million tokens of each. Reasoning is a subset of output, so the
        // output rate bills the remainder — here none of it — and the total is
        // input 2 000 + reasoning 20 000 + cache read 1 000 + cache write 3 000.
        let cost = resolved.cost_microdollars(Usage {
            input_tokens: 1_000_000,
            output_tokens: 1_000_000,
            reasoning_tokens: 1_000_000,
            cache_read_tokens: 1_000_000,
            cache_write_tokens: 1_000_000,
        });
        assert_eq!(cost, 26_000);
    }

    /// Charging is integer micro-dollars throughout: a partial micro-dollar of
    /// consumption is truncated, never rounded up, so a charge can never exceed
    /// the rate an operator approved.
    #[test]
    fn a_charge_truncates_the_micro_dollar_it_did_not_reach() {
        let resolved =
            price_of(Some(&pricing(&book(2_000_000, 4_000_000), 1)), &bound()).expect("priced");
        // 2 000 micro-dollars per million tokens: half a micro-dollar of input
        // is one micro-dollar's worth at 500 tokens and nothing below it.
        assert_eq!(resolved.cost_microdollars(usage(499, 0)), 0);
        assert_eq!(resolved.cost_microdollars(usage(500, 0)), 1);
        assert_eq!(resolved.cost_microdollars(usage(999, 0)), 1);
    }

    /// An alias is refused only when *nothing* it could route to is chargeable:
    /// one unpriced target is skipped by the walk, not a refusal for the alias.
    #[test]
    fn an_alias_is_refused_only_when_no_target_of_it_can_be_charged() {
        let snapshot = pricing(&book(2_000_000, 4_000_000), 1);
        let priced = AliasPrices {
            priced: vec![
                price_of(Some(&snapshot), &target(Some(binding("openai", "o3")))),
                price_of(Some(&snapshot), &bound()),
            ],
        };
        assert!(priced.refusal().is_none());
        assert!(priced.get(0).is_none());
        assert_eq!(priced.get(1), priced.estimate());

        let all_unpriced = AliasPrices {
            priced: vec![price_of(
                Some(&snapshot),
                &target(Some(binding("openai", "o3"))),
            )],
        };
        assert!(all_unpriced.estimate().is_none());
        assert!(all_unpriced.refusal().is_some());
    }
}
