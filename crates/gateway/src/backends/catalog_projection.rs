//! The normalized model projection: every callable offering keyed by the id a
//! request must actually send.
//!
//! [`CatalogContent`] files offerings under the model they are offerings *of* —
//! the neutral, authored identity (`xiaomi/mimo-v2-flash`) — which is the right
//! filing for "who offers this model?" and the wrong one for "what may a caller
//! ask for?". A provider may publish one model under several callable ids
//! (`qiniu-ai` publishes both `mimo-v2-flash` and `xiaomi/mimo-v2-flash`), and
//! two providers may publish the same callable id, so neither the model id nor
//! the published id alone identifies something a caller can request.
//!
//! This module holds the other view, and it keys it the way a request is made:
//!
//! | Identity | Type | What it names |
//! | --- | --- | --- |
//! | callable offering | [`CallableId`] = provider + exact published id | what a caller may ask a provider for |
//! | model | [`ModelId`] on [`ProjectedModel`] | the neutral/authored model those ids are aliases of |
//! | projection | [`ProjectionId`] | the whole projection's content, as one checksum |
//!
//! The two identities stay separate, and every callable offering keeps both:
//! [`CallableOffering::id`] is what a request sends,
//! [`CallableOffering::model`] is what it reaches. Nothing is dropped and
//! nothing is merged — provider-local aliases are distinct
//! [`CallableOffering`]s of one [`ProjectedModel`], and the same published id
//! from two providers is two callable offerings — so a projection has exactly
//! as many entries as the catalogue has offerings.
//!
//! # A projection, not a copy
//!
//! Entries borrow from the [`CatalogContent`] they were projected from, so a
//! projection cannot drift from its catalogue and cannot become a second place
//! offering facts are stored. Its identity ([`ProjectionId`]) is computed once,
//! at construction, over the canonical form of the projection — the callable
//! keying included — so it names *this view* of a catalogue rather than the
//! catalogue: two projections with equal ids present the same callable ids,
//! resolving to the same models, with the same offering content.
//!
//! # What a diff of two projections reports
//!
//! [`ProjectionDiff`] classifies the changes that are invisible to
//! [`CatalogDiff`](super::catalog::CatalogDiff)'s per-`(model, provider)` view:
//! a callable id appearing or disappearing, a provider renaming the id callers
//! must send, and a callable id coming to resolve to a different model. It
//! deliberately does *not* re-report facts, prices, or lifecycle: those are
//! `CatalogDiff`'s classes, and reporting them twice would make a refresh's
//! change count depend on how many views someone happened to build.
//!
//! Nothing here fetches, persists, or serves anything: it is an I/O-free
//! projection of content already in hand, off the request path, and the
//! decisions it rests on are recorded in
//! [ADR 0035](https://github.com/Litvue/axond/blob/main/docs/adr/0035-callable-offering-identity.md).

use std::collections::{BTreeMap, BTreeSet};

use super::catalog::{
    CatalogContent, CatalogContentId, ModelFacts, ModelId, ObservedPrice, ProviderId,
    ProviderOffering,
};
use crate::desired_state::{Canonical, CanonicalError, CanonicalValue, Checksum};

/// The identity of one callable offering: a provider, and the model id that
/// provider publishes it under, exactly as published.
///
/// This is the only key a request can be built from. The published id is kept
/// verbatim — case included — because it is the string the provider's API
/// answers to, and it is *not* a [`ModelId`] here even though the source
/// validates it as one: what makes it meaningful is that a particular provider
/// publishes it, and a bare published id is ambiguous across providers.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CallableId {
    provider: ProviderId,
    published_model_id: String,
}

impl CallableId {
    pub fn new(provider: ProviderId, published_model_id: impl Into<String>) -> Self {
        Self {
            provider,
            published_model_id: published_model_id.into(),
        }
    }

    /// The id this offering is callable by.
    pub fn of(offering: &ProviderOffering) -> Self {
        Self::new(
            offering.provider.clone(),
            offering.published_model_id.clone(),
        )
    }

    pub const fn provider(&self) -> &ProviderId {
        &self.provider
    }

    pub fn published_model_id(&self) -> &str {
        &self.published_model_id
    }
}

impl std::fmt::Display for CallableId {
    /// Space-separated, which is unambiguous: neither a provider id nor a
    /// published model id may contain whitespace, while both may contain `/`
    /// and `:`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.provider, self.published_model_id)
    }
}

impl Canonical for CallableId {
    fn canonical(&self) -> CanonicalValue {
        CanonicalValue::map([
            ("provider", self.provider.canonical()),
            (
                "published_model_id",
                CanonicalValue::string(&self.published_model_id),
            ),
        ])
    }
}

/// The identity of a whole projection.
///
/// Distinct from [`CatalogContentId`] by construction: it is a checksum of the
/// projection's own canonical form, so a projection id can be stored and
/// compared without implying that two equal ids came from byte-identical
/// catalogues, and a change in how offerings are keyed changes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProjectionId(Checksum);

impl ProjectionId {
    pub const fn checksum(self) -> Checksum {
        self.0
    }
}

impl std::fmt::Display for ProjectionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Why a catalogue has no projection.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProjectionError {
    /// One callable id is filed under two models, so a request naming it could
    /// not be resolved to one model.
    ///
    /// [`CatalogContent`] rejects a repeated offering *within* a model, which
    /// is what a source document can express; this is the same rule across
    /// models, and it is the projection's to enforce because the projection is
    /// where a callable id has to be unique.
    #[error("`{callable}` is filed under both model `{first}` and model `{second}`")]
    AmbiguousCallable {
        callable: CallableId,
        first: ModelId,
        second: ModelId,
    },
    /// The projection has no canonical form, so it has no identity to be
    /// compared or stored under.
    #[error("the projection has no canonical form: {source}")]
    Uncanonicalizable {
        #[source]
        source: CanonicalError,
    },
}

/// One callable offering: the id a request sends, and everything the catalogue
/// knows about what it reaches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallableOffering<'a> {
    id: CallableId,
    model: &'a ModelId,
    neutral: Option<&'a ModelFacts>,
    offering: &'a ProviderOffering,
}

impl<'a> CallableOffering<'a> {
    pub const fn id(&self) -> &CallableId {
        &self.id
    }

    /// The neutral/authored model this callable id resolves to — the identity
    /// two providers' offerings of one model share, and the one a provider's
    /// aliases of it share.
    pub const fn model(&self) -> &'a ModelId {
        self.model
    }

    /// The source's provider-neutral record for the model, when it publishes
    /// one. What [`ProviderOffering::overrides`] are measured against.
    pub const fn neutral(&self) -> Option<&'a ModelFacts> {
        self.neutral
    }

    /// The offering as the catalogue holds it, provider-stated facts included.
    pub const fn offering(&self) -> &'a ProviderOffering {
        self.offering
    }

    pub const fn provider(&self) -> &'a ProviderId {
        &self.offering.provider
    }

    /// The id a request to this provider must send.
    pub fn published_model_id(&self) -> &'a str {
        &self.offering.published_model_id
    }

    /// What this provider states about the offering. Provider values win; the
    /// neutral record is the fallback for what the provider leaves unsaid.
    pub const fn facts(&self) -> &'a ModelFacts {
        &self.offering.facts
    }

    pub const fn price(&self) -> Option<&'a ObservedPrice> {
        self.offering.price.as_ref()
    }

    /// Whether this provider publishes the model under the authored id itself,
    /// rather than under a provider-local alias of it.
    pub fn publishes_authored_id(&self) -> bool {
        self.offering.published_model_id == self.model.as_str()
    }

    /// The offering's content, with the id it is published under left out — the
    /// comparison that tells a renamed callable offering from an unrelated one.
    fn content(&self) -> (&ModelFacts, Option<&ObservedPrice>) {
        (&self.offering.facts, self.offering.price.as_ref())
    }
}

impl Canonical for CallableOffering<'_> {
    fn canonical(&self) -> CanonicalValue {
        CanonicalValue::map([
            ("callable", self.id.canonical()),
            ("model", self.model.canonical()),
            ("offering", self.offering.canonical()),
        ])
    }
}

/// One model, and every callable id that reaches it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedModel<'a> {
    id: &'a ModelId,
    neutral: Option<&'a ModelFacts>,
    callables: Vec<CallableId>,
}

impl<'a> ProjectedModel<'a> {
    /// The neutral/authored model identity, which is independent of any
    /// provider-local id.
    pub const fn id(&self) -> &'a ModelId {
        self.id
    }

    /// Whether the source authors a provider-neutral record for this model, or
    /// the model is known only from the offerings of it.
    pub const fn authored(&self) -> bool {
        self.neutral.is_some()
    }

    pub const fn neutral(&self) -> Option<&'a ModelFacts> {
        self.neutral
    }

    /// Every callable id reaching this model, ordered by provider and then by
    /// published id.
    pub fn callables(&self) -> &[CallableId] {
        &self.callables
    }

    /// The providers offering this model, each once, in order.
    pub fn providers(&self) -> impl Iterator<Item = &ProviderId> {
        let mut seen: Option<&ProviderId> = None;
        self.callables.iter().filter_map(move |callable| {
            if seen == Some(callable.provider()) {
                return None;
            }
            seen = Some(callable.provider());
            Some(callable.provider())
        })
    }

    /// The ids this one provider publishes the model under: one, or several
    /// provider-local aliases of it.
    pub fn published_by(&self, provider: &ProviderId) -> impl Iterator<Item = &CallableId> {
        self.callables
            .iter()
            .filter(move |callable| callable.provider() == provider)
    }
}

impl Canonical for ProjectedModel<'_> {
    fn canonical(&self) -> CanonicalValue {
        let mut fields = vec![
            ("model".to_owned(), self.id.canonical()),
            (
                "callables".to_owned(),
                CanonicalValue::List(self.callables.iter().map(Canonical::canonical).collect()),
            ),
        ];
        if let Some(neutral) = self.neutral {
            fields.push(("neutral".to_owned(), neutral.canonical()));
        }
        CanonicalValue::Map(fields)
    }
}

/// Every callable offering a catalogue publishes, keyed by what a request sends.
///
/// Construction sorts, validates that a callable id reaches exactly one model,
/// and computes the projection's identity, so a value in hand is deterministic,
/// unambiguous, and comparable. Ordering is by [`CallableId`] — provider, then
/// published id — and never by traversal order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelProjection<'a> {
    callables: Vec<CallableOffering<'a>>,
    models: Vec<ProjectedModel<'a>>,
    content_id: CatalogContentId,
    id: ProjectionId,
}

impl<'a> ModelProjection<'a> {
    /// Project a catalogue's offerings onto the ids callers may send.
    pub fn project(content: &'a CatalogContent) -> Result<Self, ProjectionError> {
        let mut callables: Vec<CallableOffering<'a>> = Vec::with_capacity(content.offering_count());
        let mut models = Vec::with_capacity(content.models().len());
        for entry in content.models() {
            let mut model_callables = Vec::with_capacity(entry.offerings.len());
            for offering in &entry.offerings {
                let id = CallableId::of(offering);
                model_callables.push(id.clone());
                callables.push(CallableOffering {
                    id,
                    model: &entry.id,
                    neutral: entry.neutral.as_ref(),
                    offering,
                });
            }
            model_callables.sort();
            models.push(ProjectedModel {
                id: &entry.id,
                neutral: entry.neutral.as_ref(),
                callables: model_callables,
            });
        }
        callables.sort_by(|left, right| left.id.cmp(&right.id));
        if let Some(pair) = callables
            .windows(2)
            .find(|pair| pair[0].id == pair[1].id)
            .map(|pair| [&pair[0], &pair[1]])
        {
            return Err(ProjectionError::AmbiguousCallable {
                callable: pair[0].id.clone(),
                first: pair[0].model.clone(),
                second: pair[1].model.clone(),
            });
        }
        models.sort_by(|left, right| left.id.cmp(right.id));
        let id = ProjectionId(
            canonical_projection(&callables, &models)
                .checksum()
                .map_err(|source| ProjectionError::Uncanonicalizable { source })?,
        );
        Ok(Self {
            callables,
            models,
            content_id: content.content_id(),
            id,
        })
    }

    /// Every callable offering, ordered by provider and then by published id.
    pub fn callables(&self) -> &[CallableOffering<'a>] {
        &self.callables
    }

    /// Every model, ordered by its neutral/authored id.
    pub fn models(&self) -> &[ProjectedModel<'a>] {
        &self.models
    }

    pub fn callable_count(&self) -> usize {
        self.callables.len()
    }

    /// The offering a request naming `published` at `provider` would reach.
    pub fn resolve(&self, provider: &ProviderId, published: &str) -> Option<&CallableOffering<'a>> {
        self.callable(&CallableId::new(provider.clone(), published))
    }

    pub fn callable(&self, id: &CallableId) -> Option<&CallableOffering<'a>> {
        let index = self
            .callables
            .binary_search_by(|offering| offering.id.cmp(id))
            .ok()?;
        self.callables.get(index)
    }

    pub fn model(&self, id: &ModelId) -> Option<&ProjectedModel<'a>> {
        let index = self
            .models
            .binary_search_by(|model| model.id.cmp(id))
            .ok()?;
        self.models.get(index)
    }

    /// Every callable offering of one model, across providers.
    pub fn callables_of(&self, model: &ModelId) -> impl Iterator<Item = &CallableOffering<'a>> {
        self.callables
            .iter()
            .filter(move |offering| offering.model == model)
    }

    /// The other ids the *same provider* publishes this offering's model under:
    /// its provider-local aliases of it, this id excluded.
    pub fn local_aliases_of(&self, id: &CallableId) -> impl Iterator<Item = &CallableOffering<'a>> {
        let model = self.callable(id).map(|offering| offering.model);
        self.callables.iter().filter(move |offering| {
            Some(offering.model) == model
                && offering.provider() == id.provider()
                && &offering.id != id
        })
    }

    /// The equivalent offerings *other providers* publish of this offering's
    /// model, whatever they call it.
    pub fn equivalents_of(&self, id: &CallableId) -> impl Iterator<Item = &CallableOffering<'a>> {
        let model = self.callable(id).map(|offering| offering.model);
        self.callables.iter().filter(move |offering| {
            Some(offering.model) == model && offering.provider() != id.provider()
        })
    }

    /// This projection's identity.
    pub const fn projection_id(&self) -> ProjectionId {
        self.id
    }

    /// The identity of the content this is a projection of, so a stored
    /// projection can be traced back to the catalogue it came from.
    pub const fn content_id(&self) -> CatalogContentId {
        self.content_id
    }

    /// How the callable offerings changed from `previous` to this projection.
    pub fn diff(&self, previous: &ModelProjection<'_>) -> ProjectionDiff {
        ProjectionDiff::between(previous, self)
    }
}

impl Canonical for ModelProjection<'_> {
    fn canonical(&self) -> CanonicalValue {
        canonical_projection(&self.callables, &self.models)
    }
}

/// A free function so [`ModelProjection::project`] can canonicalize before it
/// has a projection to ask.
fn canonical_projection(
    callables: &[CallableOffering<'_>],
    models: &[ProjectedModel<'_>],
) -> CanonicalValue {
    CanonicalValue::map([
        (
            "callables",
            CanonicalValue::List(callables.iter().map(Canonical::canonical).collect()),
        ),
        (
            "models",
            CanonicalValue::List(models.iter().map(Canonical::canonical).collect()),
        ),
    ])
}

/// One change to the set of callable offerings.
///
/// Every arm names a callable id, because the question this diff answers is
/// "which requests stopped working, and which ones started?" — and a rename is
/// its own arm rather than a removal beside an addition, since a caller reading
/// the pair has no way to tell the two apart from what a per-model diff reports.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum CallableChange {
    /// A callable id that was not offered before.
    Added { id: CallableId, model: ModelId },
    /// A callable id a request can no longer use.
    Removed { id: CallableId, model: ModelId },
    /// The same offering of the same model, published under a new id: requests
    /// must send `to` where they sent `from`.
    Renamed {
        provider: ProviderId,
        model: ModelId,
        from: String,
        to: String,
    },
    /// The id is unchanged and still callable, but it now resolves to a
    /// different model — the neutral record it is an alias of appeared,
    /// disappeared, or was re-authored upstream.
    Refiled {
        id: CallableId,
        from: ModelId,
        to: ModelId,
    },
}

impl CallableChange {
    /// The model the change is about; for a refiling, the model the id resolves
    /// to now.
    pub const fn model(&self) -> &ModelId {
        match self {
            Self::Added { model, .. }
            | Self::Removed { model, .. }
            | Self::Renamed { model, .. } => model,
            Self::Refiled { to, .. } => to,
        }
    }

    pub const fn provider(&self) -> &ProviderId {
        match self {
            Self::Added { id, .. } | Self::Removed { id, .. } | Self::Refiled { id, .. } => {
                id.provider()
            }
            Self::Renamed { provider, .. } => provider,
        }
    }

    /// The variant's ordering rank, so a diff's order is a property of the
    /// change kinds and not of the traversal that produced them.
    const fn rank(&self) -> u8 {
        match self {
            Self::Added { .. } => 0,
            Self::Removed { .. } => 1,
            Self::Renamed { .. } => 2,
            Self::Refiled { .. } => 3,
        }
    }
}

/// How many changes of each class a projection diff holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ProjectionDiffCounts {
    pub added: usize,
    pub removed: usize,
    pub renamed: usize,
    pub refiled: usize,
}

/// The change in callable offerings between two projections.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProjectionDiff {
    changes: Vec<CallableChange>,
}

impl ProjectionDiff {
    fn between(previous: &ModelProjection<'_>, current: &ModelProjection<'_>) -> Self {
        let mut changes = Vec::new();
        let mut appeared: Vec<&CallableOffering<'_>> = Vec::new();
        let mut withdrawn: Vec<&CallableOffering<'_>> = Vec::new();

        for offering in current.callables() {
            match previous.callable(&offering.id) {
                None => appeared.push(offering),
                Some(before) if before.model != offering.model => {
                    changes.push(CallableChange::Refiled {
                        id: offering.id.clone(),
                        from: before.model.clone(),
                        to: offering.model.clone(),
                    });
                }
                // The id still reaches the model it did. Whether what it
                // reaches changed is `CatalogDiff`'s classification, not this
                // one's.
                Some(_) => {}
            }
        }
        for offering in previous.callables() {
            if current.callable(&offering.id).is_none() {
                withdrawn.push(offering);
            }
        }

        changes.extend(renames(&mut withdrawn, &mut appeared));
        changes.extend(
            withdrawn
                .into_iter()
                .map(|offering| CallableChange::Removed {
                    id: offering.id.clone(),
                    model: offering.model.clone(),
                }),
        );
        changes.extend(appeared.into_iter().map(|offering| CallableChange::Added {
            id: offering.id.clone(),
            model: offering.model.clone(),
        }));

        changes.sort_by(|left, right| {
            left.model()
                .cmp(right.model())
                .then_with(|| left.provider().cmp(right.provider()))
                .then_with(|| left.rank().cmp(&right.rank()))
                .then_with(|| left.cmp(right))
        });
        Self { changes }
    }

    pub fn changes(&self) -> &[CallableChange] {
        &self.changes
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub fn counts(&self) -> ProjectionDiffCounts {
        let mut counts = ProjectionDiffCounts::default();
        for change in &self.changes {
            match change {
                CallableChange::Added { .. } => counts.added += 1,
                CallableChange::Removed { .. } => counts.removed += 1,
                CallableChange::Renamed { .. } => counts.renamed += 1,
                CallableChange::Refiled { .. } => counts.refiled += 1,
            }
        }
        counts
    }

    /// Whether any request that worked against the previous projection has to
    /// be changed to keep working: an id was withdrawn or renamed.
    ///
    /// A refiled id is *not* one of these — it still resolves — but what it
    /// resolves to is now a differently identified model, which is
    /// [`Self::resolves_elsewhere`].
    pub fn breaks_requests(&self) -> bool {
        self.changes.iter().any(|change| {
            matches!(
                change,
                CallableChange::Removed { .. } | CallableChange::Renamed { .. }
            )
        })
    }

    /// Whether any id that keeps working now reaches a different model, which is
    /// what anything keyed by [`ModelId`] — an entitlement, a route — has to
    /// reconsider even though no request has to change.
    pub fn resolves_elsewhere(&self) -> bool {
        self.changes
            .iter()
            .any(|change| matches!(change, CallableChange::Refiled { .. }))
    }
}

/// Pair withdrawn ids with appeared ones that are the same offering under a new
/// id, removing both from the leftovers.
///
/// A rename is only ever looked for within one `(provider, model)` group: an id
/// that reaches a different model, or that a different provider publishes, is a
/// different offering by construction, never a renaming of this one. Within a
/// group, the only pairing is by the offering's content — same facts, same
/// price, different id — because that is the evidence that the *same* offering
/// is now published under a new id, which is what a rename asserts and what a
/// caller acts on by rewriting requests. Two ids of one model whose content
/// differs are a removal and an addition however convenient a pairing would be:
/// telling an operator to send `to` where they sent `from` is wrong when the two
/// are not substitutes.
fn renames(
    withdrawn: &mut Vec<&CallableOffering<'_>>,
    appeared: &mut Vec<&CallableOffering<'_>>,
) -> Vec<CallableChange> {
    /// The indexes into `withdrawn` and into `appeared` of one group's members.
    type Group = (Vec<usize>, Vec<usize>);

    let mut groups: BTreeMap<(&ProviderId, &ModelId), Group> = BTreeMap::new();
    for (index, offering) in withdrawn.iter().enumerate() {
        groups
            .entry((offering.provider(), offering.model))
            .or_default()
            .0
            .push(index);
    }
    for (index, offering) in appeared.iter().enumerate() {
        groups
            .entry((offering.provider(), offering.model))
            .or_default()
            .1
            .push(index);
    }

    let mut changes = Vec::new();
    let mut paired_withdrawn = BTreeSet::new();
    let mut paired_appeared = BTreeSet::new();
    for ((provider, model), (before, mut unpaired_after)) in groups {
        for from in before {
            let Some(position) = unpaired_after
                .iter()
                .position(|to| withdrawn[from].content() == appeared[*to].content())
            else {
                continue;
            };
            let to = unpaired_after.remove(position);
            paired_withdrawn.insert(from);
            paired_appeared.insert(to);
            changes.push(CallableChange::Renamed {
                provider: provider.clone(),
                model: (*model).clone(),
                from: withdrawn[from].published_model_id().to_owned(),
                to: appeared[to].published_model_id().to_owned(),
            });
        }
    }

    let mut index = 0;
    withdrawn.retain(|_| {
        let keep = !paired_withdrawn.contains(&index);
        index += 1;
        keep
    });
    let mut index = 0;
    appeared.retain(|_| {
        let keep = !paired_appeared.contains(&index);
        index += 1;
        keep
    });
    changes
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use super::super::catalog::{
        CatalogModelEntry, CatalogProvider, JsonPointer, Modality, ModelCapability, ModelLimits,
        ObservedRate, PriceRates, ProviderEndpoint, SourceValidators,
    };
    use super::super::models_dev::ModelsDevAdapter;
    use super::*;

    const ALIASES: &str = include_str!("fixtures/models_dev/catalog.aliases.json");
    const CROSS_PROVIDER: &str = include_str!("fixtures/models_dev/catalog.cross-provider.json");
    const CROSS_PROVIDER_RENAMED: &str =
        include_str!("fixtures/models_dev/catalog.cross-provider-renamed.json");
    const CROSS_PROVIDER_SUBSTITUTED: &str =
        include_str!("fixtures/models_dev/catalog.cross-provider-substituted.json");
    const UNAUTHORED: &str = include_str!("fixtures/models_dev/catalog.aliases-unauthored.json");
    const AMBIGUOUS: &str = include_str!("fixtures/models_dev/drift.model-key-ambiguous.json");

    fn content(payload: &str) -> CatalogContent {
        ModelsDevAdapter::default()
            .parse(
                payload.as_bytes(),
                SourceValidators::etag("\"fixture\""),
                SystemTime::UNIX_EPOCH,
            )
            .expect("the fixture parses")
            .content
    }

    fn id(value: &str) -> ModelId {
        ModelId::parse(value).expect("a valid fixture id")
    }

    fn callable(provider: &str, published: &str) -> CallableId {
        CallableId::new(ProviderId::parse(provider).expect("a valid id"), published)
    }

    /// The projection's reason to exist: two ids one provider publishes for one
    /// model are two separately callable offerings, and the model they are
    /// aliases of is named once, separately from either of them.
    #[test]
    fn provider_local_aliases_are_distinct_callable_offerings_of_one_model() {
        let content = content(ALIASES);
        let projection = ModelProjection::project(&content).expect("a projection");
        let authored = id("xiaomi/mimo-v2-flash");

        assert_eq!(projection.callable_count(), 2);
        assert_eq!(projection.models().len(), 1, "one model, under two ids");
        assert_eq!(
            projection
                .callables()
                .iter()
                .map(|offering| offering.id().to_string())
                .collect::<Vec<_>>(),
            vec![
                "qiniu-ai mimo-v2-flash".to_owned(),
                "qiniu-ai xiaomi/mimo-v2-flash".to_owned(),
            ]
        );
        for offering in projection.callables() {
            assert_eq!(
                offering.model(),
                &authored,
                "both ids resolve to the authored identity, which neither of them is a copy of"
            );
        }

        let alias = callable("qiniu-ai", "mimo-v2-flash");
        assert_eq!(
            projection
                .local_aliases_of(&alias)
                .map(|offering| offering.published_model_id())
                .collect::<Vec<_>>(),
            vec!["xiaomi/mimo-v2-flash"],
            "and each knows the other ids that reach the same model"
        );
        assert_eq!(projection.equivalents_of(&alias).count(), 0);
        assert!(
            !projection
                .resolve(&ProviderId::parse("qiniu-ai").expect("id"), "mimo-v2-flash")
                .expect("the alias resolves")
                .publishes_authored_id(),
            "a provider-local alias is not the authored id"
        );
    }

    /// The same published id from two providers is two callable offerings, and
    /// two providers' different ids for one model are equivalents of each other.
    #[test]
    fn cross_provider_aliases_normalize_onto_one_model_without_merging() {
        let content = content(CROSS_PROVIDER);
        let projection = ModelProjection::project(&content).expect("a projection");
        let authored = id("xiaomi/mimo-v2-flash");

        assert_eq!(projection.models().len(), 1);
        let model = projection.model(&authored).expect("the model");
        assert!(model.authored());
        assert_eq!(
            model
                .callables()
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec![
                "openrouter xiaomi/mimo-v2-flash".to_owned(),
                "qiniu-ai mimo-v2-flash".to_owned(),
                "qiniu-ai xiaomi/mimo-v2-flash".to_owned(),
                "xiaomi mimo-v2-flash".to_owned(),
            ],
            "four callable ids, one model, none of them merged into another"
        );
        assert_eq!(
            model
                .providers()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec![
                "openrouter".to_owned(),
                "qiniu-ai".to_owned(),
                "xiaomi".to_owned()
            ]
        );
        assert_eq!(
            model
                .published_by(&ProviderId::parse("qiniu-ai").expect("id"))
                .count(),
            2,
            "one provider's aliases stay grouped under that provider"
        );

        let duplicated = "mimo-v2-flash";
        let qiniu = projection
            .resolve(&ProviderId::parse("qiniu-ai").expect("id"), duplicated)
            .expect("qiniu-ai publishes it");
        let first_party = projection
            .resolve(&ProviderId::parse("xiaomi").expect("id"), duplicated)
            .expect("xiaomi publishes it too");
        assert_ne!(
            qiniu.id(),
            first_party.id(),
            "one published id from two providers is two callable offerings"
        );
        assert_eq!(qiniu.model(), first_party.model());
        assert_ne!(
            qiniu.price(),
            first_party.price(),
            "and each keeps its own provider's terms"
        );
        assert_eq!(
            projection
                .equivalents_of(qiniu.id())
                .map(|offering| offering.id().to_string())
                .collect::<Vec<_>>(),
            vec![
                "openrouter xiaomi/mimo-v2-flash".to_owned(),
                "xiaomi mimo-v2-flash".to_owned()
            ],
            "equivalent offerings elsewhere are answerable without a re-scan"
        );
    }

    /// The projection is a function of the catalogue's content, so a payload
    /// that normalizes to the same content projects to the same identity, and a
    /// callable id changing changes it.
    #[test]
    fn a_projection_identity_covers_the_callable_keying() {
        let projected = content(CROSS_PROVIDER);
        let again = projected.clone();
        let renamed = content(CROSS_PROVIDER_RENAMED);

        let projection = ModelProjection::project(&projected).expect("a projection");
        assert_eq!(
            projection.projection_id(),
            ModelProjection::project(&again)
                .expect("a projection")
                .projection_id(),
            "the same content projects to the same identity"
        );
        assert_eq!(projection.content_id(), projected.content_id());
        assert_ne!(
            projection.projection_id(),
            ModelProjection::project(&renamed)
                .expect("a projection")
                .projection_id(),
            "and a renamed callable id is a different projection"
        );
        assert_ne!(
            projection.projection_id().checksum(),
            projected.content_id().checksum(),
            "a projection names a view of a catalogue, not the catalogue"
        );
    }

    /// A rename is reported as a rename: the pair a caller has to act on, not a
    /// removal and an addition they have to correlate themselves.
    #[test]
    fn a_renamed_callable_id_is_a_rename_and_a_withdrawn_one_a_removal() {
        let before = content(CROSS_PROVIDER);
        let after = content(CROSS_PROVIDER_RENAMED);
        let previous = ModelProjection::project(&before).expect("a projection");
        let current = ModelProjection::project(&after).expect("a projection");

        let diff = current.diff(&previous);
        assert_eq!(
            diff.counts(),
            ProjectionDiffCounts {
                added: 0,
                removed: 1,
                renamed: 1,
                refiled: 0,
            }
        );
        assert!(diff.breaks_requests());
        let model = id("xiaomi/mimo-v2-flash");
        assert_eq!(
            diff.changes(),
            [
                CallableChange::Renamed {
                    provider: ProviderId::parse("openrouter").expect("id"),
                    model: model.clone(),
                    from: "xiaomi/mimo-v2-flash".to_owned(),
                    to: "mimo-v2-flash".to_owned(),
                },
                CallableChange::Removed {
                    id: callable("qiniu-ai", "mimo-v2-flash"),
                    model,
                },
            ],
            "one id is gone and one moved; neither reads as the other"
        );
    }

    /// A withdrawal and an addition in one provider's ids for one model are not
    /// a rename when they are not the same offering: telling an operator to send
    /// the new id where they sent the old one would move traffic onto different
    /// limits at a different price.
    #[test]
    fn an_id_replaced_by_a_differently_priced_one_is_not_a_rename() {
        let before = content(CROSS_PROVIDER);
        let after = content(CROSS_PROVIDER_SUBSTITUTED);
        let previous = ModelProjection::project(&before).expect("a projection");
        let current = ModelProjection::project(&after).expect("a projection");

        let diff = current.diff(&previous);
        assert_eq!(
            diff.counts(),
            ProjectionDiffCounts {
                added: 1,
                removed: 2,
                renamed: 0,
                refiled: 0,
            },
            "same provider, same model, and still not substitutes"
        );
        assert!(diff.breaks_requests());
        assert!(!diff.resolves_elsewhere());
    }

    /// Adding an alias is an addition, and nothing else: the ids that already
    /// worked are not reported as changed because a sibling appeared.
    #[test]
    fn a_new_alias_of_an_offered_model_is_one_addition() {
        let before = content(CROSS_PROVIDER_RENAMED);
        let after = content(CROSS_PROVIDER);
        let previous = ModelProjection::project(&before).expect("a projection");
        let current = ModelProjection::project(&after).expect("a projection");

        let diff = current.diff(&previous);
        assert_eq!(
            diff.counts(),
            ProjectionDiffCounts {
                added: 1,
                removed: 0,
                renamed: 1,
                refiled: 0,
            }
        );
        assert!(
            diff.changes().iter().any(|change| matches!(
                change,
                CallableChange::Added { id, .. } if id == &callable("qiniu-ai", "mimo-v2-flash")
            )),
            "the alias that came back is an addition"
        );
    }

    /// A callable id that keeps working but comes to name a different model is
    /// neither an addition nor a removal, and reporting it as unchanged would
    /// hide that a request now reaches a differently identified model.
    #[test]
    fn a_callable_id_resolving_to_a_new_model_is_refiled() {
        let before = content(UNAUTHORED);
        let after = content(ALIASES);
        let previous = ModelProjection::project(&before).expect("a projection");
        let current = ModelProjection::project(&after).expect("a projection");

        assert_eq!(
            previous.models().len(),
            2,
            "with no neutral record, each published id is its own model"
        );
        assert_eq!(current.models().len(), 1, "the authored record joins them");
        assert_eq!(previous.callable_count(), current.callable_count());

        let diff = current.diff(&previous);
        assert_eq!(
            diff.counts(),
            ProjectionDiffCounts {
                added: 0,
                removed: 0,
                renamed: 0,
                refiled: 1,
            },
            "every id a caller could send still works; the alias now names the authored model"
        );
        assert!(!diff.breaks_requests());
        assert!(
            diff.resolves_elsewhere(),
            "but what the id reaches is now identified differently"
        );
        assert_eq!(
            diff.changes(),
            [CallableChange::Refiled {
                id: callable("qiniu-ai", "mimo-v2-flash"),
                from: id("mimo-v2-flash"),
                to: id("xiaomi/mimo-v2-flash"),
            }],
            "the id that already spelled the authored model is not reported as moved"
        );
    }

    #[test]
    fn an_unchanged_catalogue_projects_to_an_empty_diff() {
        let projected = content(CROSS_PROVIDER);
        let projection = ModelProjection::project(&projected).expect("a projection");
        let diff = projection.diff(&projection);
        assert!(diff.is_empty());
        assert!(!diff.breaks_requests());
    }

    /// An offering whose model cannot be identified is refused at import, so a
    /// projection is never asked to guess which model an ambiguous tail names.
    #[test]
    fn an_ambiguous_provider_key_never_reaches_a_projection() {
        let refused = ModelsDevAdapter::default().parse(
            AMBIGUOUS.as_bytes(),
            SourceValidators::etag("\"fixture\""),
            SystemTime::UNIX_EPOCH,
        );
        assert!(
            refused.is_err(),
            "an ambiguous provider-local key is refused by the import"
        );
    }

    /// The rule a source document cannot break but a caller assembling content
    /// can: one callable id, one model.
    #[test]
    fn one_callable_id_filed_under_two_models_has_no_projection() {
        let provider = ProviderId::parse("qiniu-ai").expect("id");
        let pointer = JsonPointer::new("");
        let facts = ModelFacts {
            display_name: Some("MiMo".to_owned()),
            capabilities: [ModelCapability::ToolCall].into_iter().collect(),
            input_modalities: [Modality::Text].into_iter().collect(),
            output_modalities: [Modality::Text].into_iter().collect(),
            limits: ModelLimits::default(),
            ..ModelFacts::default()
        };
        let entry = |model: &str| CatalogModelEntry {
            id: id(model),
            neutral: None,
            offerings: vec![ProviderOffering {
                provider: provider.clone(),
                model: id(model),
                published_model_id: "mimo-v2-flash".to_owned(),
                facts: facts.clone(),
                overrides: Vec::new(),
                price: Some(ObservedPrice::new(PriceRates::new(
                    ObservedRate::from_nanos(1),
                    ObservedRate::from_nanos(1),
                ))),
                endpoint: ProviderEndpoint::default(),
                pointer: pointer.clone(),
            }],
        };
        let projected = CatalogContent::new(
            vec![CatalogProvider {
                id: provider.clone(),
                display_name: None,
                doc_url: None,
                endpoint: ProviderEndpoint::default(),
                env_vars: Vec::new(),
                pointer: pointer.clone(),
            }],
            vec![entry("mimo-v2-flash"), entry("xiaomi/mimo-v2-flash")],
        )
        .expect("content the domain accepts");

        assert_eq!(
            ModelProjection::project(&projected),
            Err(ProjectionError::AmbiguousCallable {
                callable: callable("qiniu-ai", "mimo-v2-flash"),
                first: id("mimo-v2-flash"),
                second: id("xiaomi/mimo-v2-flash"),
            }),
            "a request naming it could not be resolved, so there is no projection"
        );
    }
}
