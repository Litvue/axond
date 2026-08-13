//! The seam between what an operator enabled and what a catalogue publishes:
//! resolving a [`CatalogOffering`] pin to the offering a request would be built
//! from.
//!
//! An enablement names a pair — the opaque [`OfferingId`] it enables, and the
//! digest of the immutable snapshot *blob* that identity was read from
//! ([`CatalogOffering`]) — and a published revision carries nothing else about
//! the model: no provider endpoint, no published id, no limits, no observed
//! price. Everything a request or an operator surface needs is in the catalogue,
//! keyed the way a catalogue keys it. Something has to hold the two together,
//! and until this module nothing did: [`ModelProjection`] keys offerings by
//! [`CallableId`] (provider + the exact id that provider publishes), while an
//! enablement names an [`OfferingId`] (a digest of provider + the *neutral*
//! model id). Neither key is the other's.
//!
//! [`PinnedCatalog`] is that map, and it is deliberately only a map:
//!
//! | Question | Answer |
//! | --- | --- |
//! | is this pin's snapshot the one in hand? | [`Resolution::OtherSnapshot`] |
//! | does that snapshot still publish the offering? | [`Resolution::Withdrawn`] |
//! | which id would a request send? | [`Resolution::Callable`] |
//! | and if the provider publishes several? | [`Resolution::Ambiguous`] |
//!
//! # Why an ambiguous arm exists rather than a choice
//!
//! [`OfferingId::of`] digests the provider and the *neutral* model id, so it
//! names "this provider's offering of this model" — while a provider may
//! publish one model under several ids a caller can send, which
//! [ADR 0047](https://github.com/Litvue/axond/blob/main/docs/adr/0047-callable-offering-identity.md)
//! records as distinct callable offerings of one model. The mapping is therefore
//! one-to-many in exactly the case where guessing would be worst: picking the
//! first alias would send a request to an id an operator never saw, and the
//! choice would silently change when an upstream reordered or renamed its
//! aliases. So a pin that reaches several callable ids resolves to
//! [`Resolution::Ambiguous`] carrying all of them, and the decision — which is
//! an enablement decision, owned by #149 — is left to the caller that has the
//! authority to make it.
//!
//! # A resolution is about content in hand
//!
//! [`PinnedCatalog::of`] takes the content *and* the digest of the payload it
//! was parsed from, and every resolution is against that pair. A pin naming
//! another snapshot is answered [`Resolution::OtherSnapshot`] rather than looked
//! up: an enablement approved against yesterday's catalogue must not silently
//! start resolving through today's, because the facts an operator approved — the
//! endpoint, the limits, the observed price — are the ones in the snapshot they
//! read. Resolving such a pin means fetching *that* retained snapshot, which is
//! a store read this module does not perform and does not hide: it is I/O-free,
//! borrows the content it was built from, and holds no handle to anything.
//!
//! Nothing here enables, activates, prices, or withdraws anything. It answers
//! what a catalogue says about a pin; what to do about the answer belongs to the
//! enablement (#149) and pricing (#147) slices.

use std::collections::{BTreeMap, BTreeSet};

use super::catalog::{CatalogContent, CatalogContentId, CatalogSnapshot};
use super::catalog_projection::{
    CallableId, CallableOffering, ModelProjection, ProjectionError, ProjectionId,
};
use crate::desired_state::Checksum;
use crate::desired_state::models::{CatalogOffering, ModelEnablementBody, OfferingId};

/// Why a catalogue could not be keyed by the identities enablements pin.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PinError {
    /// The callable view could not be built, so there is nothing to key.
    #[error("the catalogue has no callable projection: {source}")]
    Unprojectable {
        #[source]
        source: ProjectionError,
    },
    /// An offering identity could not be derived, so a pin naming it could
    /// never be matched — and, worse, the same offering would be unresolvable
    /// under a key an admin surface had already published.
    #[error("the offering `{published}` of provider `{provider}` has no derivable identity")]
    Underivable { provider: String, published: String },
}

/// What a catalogue says about one pinned offering.
///
/// Borrowed from the [`PinnedCatalog`], and through it from the content: a
/// resolution cannot outlive, or drift from, the catalogue that answered it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution<'r, 'a> {
    /// The pin resolves to exactly one id a request may send.
    Callable(&'r CallableOffering<'a>),
    /// The provider publishes this model under several callable ids, so the pin
    /// does not name one request. Every candidate, ordered as the projection
    /// orders them.
    Ambiguous { callables: &'r [CallableId] },
    /// This snapshot no longer publishes the offering. An observation about the
    /// catalogue, never a withdrawal of the enablement.
    Withdrawn,
    /// The pin was approved against a different snapshot, whose digest it
    /// carries. Resolving it means reading that retained snapshot.
    OtherSnapshot { pinned: Checksum },
}

impl<'r, 'a> Resolution<'r, 'a> {
    /// The offering, when the pin named exactly one.
    pub const fn callable(&self) -> Option<&'r CallableOffering<'a>> {
        match *self {
            Self::Callable(offering) => Some(offering),
            _ => None,
        }
    }

    /// Whether this catalogue answered about the pin at all, rather than
    /// deferring to another snapshot.
    pub const fn is_about_this_snapshot(&self) -> bool {
        !matches!(self, Self::OtherSnapshot { .. })
    }
}

/// One catalogue snapshot, keyed by the identities enablements pin.
///
/// Construction derives every offering identity once, so a lookup is a binary
/// search rather than a re-derivation, and a catalogue whose identities cannot
/// all be derived is refused as a whole ([`PinError::Underivable`]) instead of
/// answering `Withdrawn` for an offering it does publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PinnedCatalog<'a> {
    snapshot: Checksum,
    projection: ModelProjection<'a>,
    offerings: BTreeMap<OfferingId, Vec<CallableId>>,
}

impl<'a> PinnedCatalog<'a> {
    /// Key `content`, which was parsed from the payload `snapshot` identifies.
    ///
    /// `snapshot` is the raw payload digest rather than the
    /// [`CatalogContentId`] because that is what an enablement pins: an operator
    /// approved the bytes they read.
    pub fn of(content: &'a CatalogContent, snapshot: Checksum) -> Result<Self, PinError> {
        let projection = ModelProjection::project(content)
            .map_err(|source| PinError::Unprojectable { source })?;
        let mut offerings: BTreeMap<OfferingId, Vec<CallableId>> = BTreeMap::new();
        for callable in projection.callables() {
            let offering = callable.offering();
            let id = OfferingId::of(offering.provider.as_str(), offering.model.as_str()).map_err(
                |_| PinError::Underivable {
                    provider: offering.provider.as_str().to_owned(),
                    published: offering.published_model_id.clone(),
                },
            )?;
            offerings.entry(id).or_default().push(callable.id().clone());
        }
        Ok(Self {
            snapshot,
            projection,
            offerings,
        })
    }

    /// Key the content of `snapshot`, taking the pinned digest from the
    /// provenance it was imported with.
    pub fn of_snapshot(snapshot: &'a CatalogSnapshot) -> Result<Self, PinError> {
        Self::of(&snapshot.content, snapshot.source.raw.digest)
    }

    /// The snapshot digest an enablement must pin for this catalogue to answer
    /// about it.
    pub const fn snapshot(&self) -> Checksum {
        self.snapshot
    }

    /// The callable view this was keyed over, for the questions that are asked
    /// by callable id rather than by pin.
    pub const fn projection(&self) -> &ModelProjection<'a> {
        &self.projection
    }

    pub const fn content_id(&self) -> CatalogContentId {
        self.projection.content_id()
    }

    pub const fn projection_id(&self) -> ProjectionId {
        self.projection.projection_id()
    }

    /// Every offering identity this catalogue publishes, ascending.
    pub fn published(&self) -> impl Iterator<Item = OfferingId> {
        self.offerings.keys().copied()
    }

    /// What this catalogue says about `pin`.
    pub fn resolve(&self, pin: CatalogOffering) -> Resolution<'_, 'a> {
        if !pin.is_pinned_to(self.snapshot) {
            return Resolution::OtherSnapshot {
                pinned: pin.snapshot,
            };
        }
        let Some(callables) = self.offerings.get(&pin.offering) else {
            return Resolution::Withdrawn;
        };
        match callables.as_slice() {
            [only] => self
                .projection
                .callable(only)
                .map_or(Resolution::Withdrawn, Resolution::Callable),
            several => Resolution::Ambiguous { callables: several },
        }
    }

    /// The offerings among `enablements` this catalogue no longer publishes.
    ///
    /// Independent of which snapshot a pin names, so this agrees with
    /// [`RefreshImpact`](super::catalog_refresh::RefreshImpact): an enablement
    /// whose offering vanished upstream is worth reporting whether or not its
    /// operator has moved the pin to the current snapshot, and an operator who
    /// was told otherwise would hear about the withdrawal only once they
    /// republished against the catalogue that had already dropped it.
    ///
    /// The distinction between the two states lives in [`Self::resolve`], which
    /// answers [`Resolution::OtherSnapshot`] rather than
    /// [`Resolution::Withdrawn`] for a pin to content this catalogue is not: a
    /// request may not be routed through a snapshot nobody approved, even when
    /// this one still publishes the offering.
    pub fn withdrawn_from<'b>(
        &self,
        enablements: impl IntoIterator<Item = &'b ModelEnablementBody>,
    ) -> BTreeSet<OfferingId> {
        enablements
            .into_iter()
            .map(ModelEnablementBody::offering)
            .map(|pin| pin.offering)
            .filter(|offering| !self.offerings.contains_key(offering))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use super::*;
    use crate::backends::catalog::SourceValidators;
    use crate::backends::catalog_refresh::RefreshImpact;
    use crate::backends::models_dev::ModelsDevAdapter;
    use crate::desired_state::fixtures::{resource_id, tenant_id};
    use crate::desired_state::models::{ModelOwner, WireFamily};

    const IDENTITY: &str = include_str!("fixtures/models_dev/catalog.identity.json");
    const ALIASES: &str = include_str!("fixtures/models_dev/catalog.aliases.json");
    const ALIASES_REPRICED: &str =
        include_str!("fixtures/models_dev/catalog.aliases-repriced.json");

    /// A payload, and the content and digest a published snapshot of it carries.
    fn imported(payload: &str) -> (CatalogContent, Checksum) {
        let content = ModelsDevAdapter::default()
            .parse(
                payload.as_bytes(),
                SourceValidators::etag("\"fixture\""),
                SystemTime::UNIX_EPOCH,
            )
            .expect("the fixture parses")
            .content;
        (content, Checksum::of(payload.as_bytes()))
    }

    fn pin(provider: &str, model: &str, snapshot: Checksum) -> CatalogOffering {
        CatalogOffering::new(
            OfferingId::of(provider, model).expect("a fixture identity is derivable"),
            snapshot,
        )
    }

    fn enablement(sequence: u64, offering: CatalogOffering) -> ModelEnablementBody {
        ModelEnablementBody::new(
            resource_id(sequence),
            ModelOwner::tenant(tenant_id(1)),
            offering,
            WireFamily::OpenaiChat,
        )
    }

    /// The seam: a pin an operator approved becomes the id a request sends, and
    /// the provider it sends it to.
    #[test]
    fn a_pin_resolves_to_the_id_a_request_would_send() {
        let (content, snapshot) = imported(IDENTITY);
        let pinned = PinnedCatalog::of(&content, snapshot).expect("the catalogue is keyable");

        let resolved = pinned.resolve(pin("hpc-ai", "openai/gpt-5.5", snapshot));
        let callable = resolved.callable().expect("the offering is published");
        assert_eq!(callable.published_model_id(), "openai/gpt-5.5");
        assert_eq!(callable.provider().as_str(), "hpc-ai");
        assert!(callable.price().is_some(), "with that provider's own terms");
        assert_eq!(
            pinned.published().count(),
            2,
            "one model published by two providers is two pinnable offerings"
        );
    }

    /// Two providers publishing one model are two pins, and neither resolves
    /// through the other's endpoint or price.
    #[test]
    fn each_provider_of_one_model_is_pinned_separately() {
        let (content, snapshot) = imported(IDENTITY);
        let pinned = PinnedCatalog::of(&content, snapshot).expect("the catalogue is keyable");

        let first = pinned
            .resolve(pin("openai", "openai/gpt-5.5", snapshot))
            .callable()
            .expect("openai publishes it");
        let second = pinned
            .resolve(pin("hpc-ai", "openai/gpt-5.5", snapshot))
            .callable()
            .expect("hpc-ai publishes it too");
        assert_eq!(first.model(), second.model());
        assert_ne!(first.id(), second.id());
        assert_ne!(first.price(), second.price());
    }

    /// A provider publishing one model under two callable ids makes the pin
    /// one-to-many, and choosing between them is not this map's decision.
    #[test]
    fn an_offering_published_under_several_ids_is_ambiguous_rather_than_guessed() {
        let (content, snapshot) = imported(ALIASES);
        let pinned = PinnedCatalog::of(&content, snapshot).expect("the catalogue is keyable");

        let resolved = pinned.resolve(pin("qiniu-ai", "xiaomi/mimo-v2-flash", snapshot));
        let Resolution::Ambiguous { callables } = resolved else {
            panic!("a pin reaching two callable ids must not resolve to one: {resolved:?}");
        };
        assert_eq!(
            callables
                .iter()
                .map(CallableId::published_model_id)
                .collect::<Vec<_>>(),
            vec!["mimo-v2-flash", "xiaomi/mimo-v2-flash"],
            "every candidate, so a caller with the authority to choose can"
        );
        assert!(resolved.callable().is_none());
        assert!(resolved.is_about_this_snapshot());
    }

    #[test]
    fn a_pin_the_catalogue_no_longer_publishes_is_withdrawn() {
        let (content, snapshot) = imported(IDENTITY);
        let pinned = PinnedCatalog::of(&content, snapshot).expect("the catalogue is keyable");

        assert_eq!(
            pinned.resolve(pin("openai", "openai/a-model-that-was-withdrawn", snapshot)),
            Resolution::Withdrawn
        );
        assert_eq!(
            pinned.resolve(pin("a-provider-that-left", "openai/gpt-5.5", snapshot)),
            Resolution::Withdrawn,
            "a pin names a provider's offering, not a model"
        );
    }

    /// The reason a resolution is against content in hand: the facts an operator
    /// approved are the ones in the snapshot they read.
    #[test]
    fn a_pin_approved_against_another_snapshot_is_not_resolved_through_this_one() {
        let (content, snapshot) = imported(ALIASES);
        let (_, repriced) = imported(ALIASES_REPRICED);
        let pinned = PinnedCatalog::of(&content, snapshot).expect("the catalogue is keyable");

        let resolved = pinned.resolve(pin("qiniu-ai", "xiaomi/mimo-v2-flash", repriced));
        assert_eq!(resolved, Resolution::OtherSnapshot { pinned: repriced });
        assert!(!resolved.is_about_this_snapshot());
        assert!(resolved.callable().is_none());
    }

    /// A republished blob is a new pin, but not a new identity: the same
    /// catalogue keys the same way whichever payload it arrived in.
    #[test]
    fn an_identity_is_derived_from_the_catalogue_rather_than_the_payload() {
        let (content, first) = imported(IDENTITY);
        let second = Checksum::of(b"the same catalogue, served again");
        let before = PinnedCatalog::of(&content, first).expect("the catalogue is keyable");
        let after = PinnedCatalog::of(&content, second).expect("the catalogue is keyable");

        assert_eq!(
            before.published().collect::<Vec<_>>(),
            after.published().collect::<Vec<_>>()
        );
        assert_eq!(before.content_id(), after.content_id());
        assert_eq!(before.projection_id(), after.projection_id());
        assert_ne!(before.snapshot(), after.snapshot());
    }

    /// One derivation, so the resolver and the operator-facing refresh report
    /// cannot disagree about what an upstream stopped publishing.
    #[test]
    fn withdrawal_agrees_with_the_impact_a_refresh_reports() {
        let (content, snapshot) = imported(IDENTITY);
        let pinned = PinnedCatalog::of(&content, snapshot).expect("the catalogue is keyable");
        let enablements = [
            enablement(1, pin("openai", "openai/gpt-5.5", snapshot)),
            enablement(2, pin("openai", "openai/gone", snapshot)),
        ];

        let withdrawn = pinned.withdrawn_from(&enablements);
        let impact = RefreshImpact::of(&enablements, &content, snapshot);
        assert_eq!(withdrawn, impact.withdrawn);
        assert_eq!(withdrawn.len(), 1);
    }

    /// Withdrawal observation does not depend on an operator having moved their
    /// pin: an unmoved pin is exactly the one whose offering nobody has looked
    /// at since the upstream dropped it, and the refresh report says so too.
    #[test]
    fn an_offering_pinned_elsewhere_is_still_reported_as_withdrawn() {
        let (content, snapshot) = imported(IDENTITY);
        let pinned = PinnedCatalog::of(&content, snapshot).expect("the catalogue is keyable");
        let older = Checksum::of(b"an older catalogue payload");
        let enablements = [
            enablement(3, pin("openai", "openai/gone", older)),
            enablement(4, pin("openai", "openai/gpt-5.5", older)),
        ];

        let withdrawn = pinned.withdrawn_from(&enablements);
        let impact = RefreshImpact::of(&enablements, &content, snapshot);
        assert_eq!(withdrawn, impact.withdrawn);
        assert_eq!(
            withdrawn,
            [OfferingId::of("openai", "openai/gone").expect("derivable")]
                .into_iter()
                .collect::<BTreeSet<_>>(),
            "the offering this catalogue dropped, and only it"
        );
        assert_eq!(impact.pins_unmoved, 2);
        // And the two states stay distinct where it matters: neither pin may be
        // routed through this catalogue, withdrawn or not.
        assert_eq!(
            pinned.resolve(pin("openai", "openai/gpt-5.5", older)),
            Resolution::OtherSnapshot { pinned: older }
        );
    }
}
