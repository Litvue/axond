//! The models.dev catalogue source: one document shape, parsed strictly.
//!
//! # One endpoint, named
//!
//! models.dev publishes several documents, and they are not interchangeable:
//! `api.json` and `models.json` have different shapes from
//! [`MODELS_DEV_CATALOG_URL`]. Only `/catalog.json` is supported, and
//! [`ModelsDevAdapter::new`] refuses any other path rather than trying a parse
//! that would either fail confusingly or — worse — half-succeed. The shape it
//! parses is recorded on every snapshot as
//! [`SchemaVersion::MODELS_DEV_CATALOG_V1`].
//!
//! That document is:
//!
//! ```text
//! { "models":    { "<model id>":    { …provider-neutral metadata… } },
//!   "providers": { "<provider id>": { …provider metadata…,
//!                                     "models": { "<model id>": { …metadata…, "cost": … } } } } }
//! ```
//!
//! so provider-neutral metadata and provider offerings are the upstream's own
//! distinction, and this adapter keeps it: the neutral record lands in
//! [`CatalogModelEntry::neutral`], each offering keeps what its provider states,
//! and every field where the provider contradicts the neutral record is recorded
//! in [`ProviderOffering::overrides`] with a JSON Pointer to the provider's own
//! value. Provider values therefore win by construction, and *why* they won is
//! auditable against the raw payload the snapshot's digest names.
//!
//! The two maps do not share one id namespace, though — the neutral index is
//! authored (`openai/gpt-5.5`) while a provider keys its offerings the way its
//! own API names them (`gpt-5.5`) — so filing an offering under the model it
//! belongs to is [`canonical_model_id`]'s job, and
//! [`ProviderOffering::published_model_id`] keeps the string a request to that
//! provider must actually use.
//!
//! The decisions this module rests on — the observed-rate unit, the three
//! identities, and the compiled-in seed — are recorded in
//! [ADR 0033](https://github.com/Litvue/axond/blob/main/docs/adr/0033-catalogue-source-imports.md).
//!
//! # Strict where a mistake would be silent
//!
//! The rule is: **be tolerant of new information, intolerant of changed
//! meaning.** A field this adapter does not model is ignored, so an upstream
//! addition does not freeze imports. Everything else is refused, with a pointer
//! to the offending location:
//!
//! - a missing required field or a changed type (`"limit": {"context": "272000"}`);
//! - an unrecognized enumerated value — a `status` or a modality — because
//!   flattening one into "available" or dropping it would quietly change what an
//!   operator sees;
//! - a key that disagrees with the `id` inside it, or an id containing something
//!   no provider id contains, since both make one model two or two models one;
//! - a price that is negative, finer than a nano-dollar, out of range, partially
//!   stated, tiered on an unknown threshold, or tiered without the base pair its
//!   tiers qualify — an empty `cost` object is the only one read as unpublished;
//! - text a canonical form cannot hold, since content that cannot be
//!   checksummed has no identity to admit it under. Whitespace is normalized
//!   first (see [`text`]), because upstream publishes trailing tabs and those
//!   carry no meaning.
//!
//! # Prices are parsed exactly, never through a float
//!
//! Upstream states prices as JSON decimals in dollars per million tokens
//! (`0.5`, `12.5`). They are read from the raw JSON text with
//! [`serde_json::value::RawValue`] and converted to integer nano-dollars per
//! million tokens by exact decimal arithmetic — never through `f64`, which cannot
//! represent `0.1` and so would make a checksum depend on rounding. A rate finer
//! than that unit is refused rather than rounded to zero: a rate too fine to
//! represent is an unusable observation, not a free one.
//!
//! The one exception is a value the *upstream* computed in floating point and
//! published as such (`0.049999999999999996`), which is read as the decimal it is
//! the `f64` of; see [`is_binary_artifact`].

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::value::RawValue;

use super::catalog::{
    CatalogContent, CatalogContentError, CatalogError, CatalogModelEntry, CatalogProvider,
    CatalogRefresh, CatalogSnapshot, CatalogSource, ETag, HttpDate, InvalidCatalogId, JsonPointer,
    Modality, ModelCapability, ModelFacts, ModelField, ModelId, ModelLifecycle, ModelLimits,
    ObservedPrice, ObservedRate, PriceRates, PriceTier, PriceTierThreshold, ProviderEndpoint,
    ProviderOffering, SchemaVersion, SourceValidators, source_snapshot,
};
use super::{Capabilities, Capability};

/// The only supported models.dev document.
pub const MODELS_DEV_CATALOG_URL: &str = "https://models.dev/catalog.json";

/// The path every supported source URL must end with.
const SUPPORTED_PATH: &str = "/catalog.json";

/// The name every [`CatalogError`] from this source carries.
const BACKEND: &str = "models.dev";

/// Why a models.dev payload was refused.
///
/// Every arm names a location in the payload, because "the catalogue is invalid"
/// is not an actionable operator message and a refused import means the previous
/// catalogue stays active until someone can act on it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ModelsDevError {
    #[error(
        "`{url}` is not a supported models.dev document; only `{SUPPORTED_PATH}` is \
         (`api.json` and `models.json` have different shapes)"
    )]
    UnsupportedEndpoint { url: String },
    #[error("the payload is not JSON: {message}")]
    NotJson { message: String },
    #[error("the payload is not a models.dev catalogue document: {message}")]
    Schema { message: String },
    #[error("`{pointer}` is keyed `{key}` but its `id` is `{id}`")]
    IdMismatch {
        pointer: JsonPointer,
        key: String,
        id: String,
    },
    #[error("`{pointer}` has an unusable identifier: {source}")]
    Identifier {
        pointer: JsonPointer,
        #[source]
        source: InvalidCatalogId,
    },
    #[error("`{pointer}` has an unrecognized status `{status}`")]
    UnknownStatus {
        pointer: JsonPointer,
        status: String,
    },
    #[error("`{pointer}` has an unrecognized modality `{modality}`")]
    UnknownModality {
        pointer: JsonPointer,
        modality: String,
    },
    #[error("`{pointer}` states a price the gateway cannot represent: {reason}")]
    Price {
        pointer: JsonPointer,
        reason: PriceRejection,
    },
    #[error("`{pointer}` has an unrecognized price tier type `{kind}`")]
    UnknownTierType { pointer: JsonPointer, kind: String },
    #[error("`{pointer}` states two prices for the same tier threshold")]
    DuplicateTier { pointer: JsonPointer },
    #[error("`{pointer}` publishes a price on a provider-neutral record")]
    NeutralPrice { pointer: JsonPointer },
    #[error(
        "`{pointer}` offers `{key}`, which could be any of `{}`",
        candidates.join("`, `")
    )]
    AmbiguousModelKey {
        pointer: JsonPointer,
        key: String,
        candidates: Vec<String>,
    },
    #[error("the payload's catalogue is not usable: {source}")]
    Content {
        #[source]
        source: CatalogContentError,
    },
}

impl ModelsDevError {
    fn identifier(pointer: &JsonPointer, source: InvalidCatalogId) -> Self {
        Self::Identifier {
            pointer: pointer.clone(),
            source,
        }
    }
}

impl From<ModelsDevError> for CatalogError {
    fn from(error: ModelsDevError) -> Self {
        Self::Invalid {
            backend: BACKEND,
            message: error.to_string(),
        }
    }
}

/// Why a published decimal is not a usable [`ObservedRate`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PriceRejection {
    #[error("`{value}` is not a JSON number")]
    NotANumber { value: String },
    #[error("`{value}` is negative")]
    Negative { value: String },
    #[error("`{value}` is finer than one nano-dollar per million tokens")]
    ExcessPrecision { value: String },
    #[error("`{value}` is larger than an observed rate can hold")]
    Overflow { value: String },
    #[error("a price states `{stated}` without `{missing}`")]
    Partial {
        stated: &'static str,
        missing: &'static str,
    },
}

/// Reads and validates the models.dev `/catalog.json` document.
///
/// I/O-free: it turns bytes that are already in hand into a
/// [`CatalogSnapshot`], so parsing is testable against checked-in fixtures and
/// nothing about it can reach the network. Fetching is [`CatalogFetch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelsDevAdapter {
    source_url: String,
}

impl Default for ModelsDevAdapter {
    fn default() -> Self {
        Self {
            source_url: MODELS_DEV_CATALOG_URL.to_owned(),
        }
    }
}

impl ModelsDevAdapter {
    /// An adapter for a `/catalog.json` URL — the public one, or a mirror.
    pub fn new(source_url: impl Into<String>) -> Result<Self, ModelsDevError> {
        let source_url = source_url.into();
        let path = source_url
            .split_once("://")
            .map_or(source_url.as_str(), |(_, rest)| rest);
        let path = path.split(['?', '#']).next().unwrap_or(path);
        if !path.ends_with(SUPPORTED_PATH) {
            return Err(ModelsDevError::UnsupportedEndpoint { url: source_url });
        }
        Ok(Self { source_url })
    }

    pub fn source_url(&self) -> &str {
        &self.source_url
    }

    /// Parse and normalize a payload into a snapshot.
    ///
    /// `fetched_at` and `validators` are provenance: they are recorded, and they
    /// cannot influence [`CatalogSnapshot::content`] or its identity.
    pub fn parse(
        &self,
        payload: &[u8],
        validators: SourceValidators,
        fetched_at: SystemTime,
    ) -> Result<CatalogSnapshot, ModelsDevError> {
        let text = std::str::from_utf8(payload).map_err(|error| ModelsDevError::NotJson {
            message: error.to_string(),
        })?;
        let document: WireCatalog = serde_json::from_str(text).map_err(|error| {
            if error.is_syntax() || error.is_eof() {
                ModelsDevError::NotJson {
                    message: error.to_string(),
                }
            } else {
                ModelsDevError::Schema {
                    message: error.to_string(),
                }
            }
        })?;
        let content = normalize(&document)?;
        let source = source_snapshot(
            self.source_url.clone(),
            SchemaVersion::MODELS_DEV_CATALOG_V1,
            payload,
            &content,
            validators,
            fetched_at,
        );
        Ok(CatalogSnapshot { source, content })
    }
}

/// The document, as it is on the wire.
///
/// Both members are required: a payload without them is one of the other
/// models.dev shapes, and reading it as a catalogue would silently import
/// nothing.
#[derive(Debug, Deserialize)]
struct WireCatalog {
    models: BTreeMap<String, WireModel>,
    providers: BTreeMap<String, WireProvider>,
}

#[derive(Debug, Deserialize)]
struct WireProvider {
    id: String,
    name: String,
    #[serde(default)]
    doc: Option<String>,
    #[serde(default)]
    api: Option<String>,
    #[serde(default)]
    npm: Option<String>,
    #[serde(default)]
    env: Vec<String>,
    models: BTreeMap<String, WireModel>,
}

/// A model record, neutral or offered.
///
/// Unknown fields are ignored on purpose (see the module docs): `benchmarks`,
/// `weights`, `reasoning_options` and whatever upstream adds next are new
/// information, not changed meaning.
#[derive(Debug, Deserialize)]
struct WireModel {
    id: String,
    name: String,
    #[serde(default)]
    family: Option<String>,
    #[serde(default)]
    attachment: Option<bool>,
    #[serde(default)]
    reasoning: Option<bool>,
    #[serde(default)]
    tool_call: Option<bool>,
    #[serde(default)]
    temperature: Option<bool>,
    #[serde(default)]
    structured_output: Option<bool>,
    #[serde(default)]
    interleaved: Option<WireFlag>,
    #[serde(default)]
    open_weights: Option<bool>,
    #[serde(default)]
    experimental: Option<WireFlag>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    knowledge: Option<String>,
    #[serde(default)]
    release_date: Option<String>,
    #[serde(default)]
    last_updated: Option<String>,
    modalities: WireModalities,
    limit: WireLimit,
    #[serde(default)]
    cost: Option<WireCost>,
    #[serde(default)]
    provider: Option<WireModelProvider>,
}

/// A field the upstream states either as a boolean or as an object
/// (`interleaved`, `experimental`).
///
/// What the object *means* differs per field, and the difference matters, so
/// this type only records which form was used and each caller decides:
///
/// - `"interleaved": {"field": "reasoning_content"}` configures the capability
///   it names, so it reads as stated; the configuration is provider-specific
///   detail this slice does not model. Every object form in the live payload is
///   this shape, and 688 offerings use it.
/// - `"experimental": {"modes": {"fast": {…}}}` describes *extra experimental
///   modes* of an otherwise generally-available offering, so it does not state
///   that the offering is experimental. All 39 object-valued `experimental`
///   keys in the live payload are this shape, and none of them state the
///   boolean; reading them as [`ModelCapability::Experimental`] would mark 39
///   GA offerings experimental.
///
/// Anything that is neither a boolean nor an object is still a type error.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum WireFlag {
    Stated(bool),
    Configured(BTreeMap<String, serde_json::Value>),
}

impl WireFlag {
    /// The capability is present, whether stated bare or configured.
    const fn configurable(&self) -> bool {
        match self {
            Self::Stated(stated) => *stated,
            Self::Configured(_) => true,
        }
    }

    /// The capability is present only if the upstream says so outright: an
    /// object here describes something else (see the type docs), and the
    /// modelled fields say nothing about it either way.
    const fn asserted(&self) -> bool {
        match self {
            Self::Stated(stated) => *stated,
            Self::Configured(_) => false,
        }
    }
}

#[derive(Debug, Deserialize)]
struct WireModalities {
    #[serde(default)]
    input: Vec<String>,
    #[serde(default)]
    output: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct WireLimit {
    #[serde(default)]
    context: Option<u64>,
    #[serde(default)]
    input: Option<u64>,
    #[serde(default)]
    output: Option<u64>,
}

/// A per-offering endpoint hint.
#[derive(Debug, Deserialize)]
struct WireModelProvider {
    #[serde(default)]
    api: Option<String>,
    #[serde(default)]
    npm: Option<String>,
    #[serde(default)]
    shape: Option<String>,
}

/// Published rates.
///
/// Every rate is [`RawValue`] — the number's own text — so a decimal is never
/// deserialized into an `f64` on its way to an integer. That is also why neither
/// this struct nor [`WireTier`] uses `#[serde(flatten)]`: flattening buffers the
/// document and would hand back a parsed number instead of its text.
#[derive(Debug, Deserialize)]
struct WireCost {
    #[serde(default)]
    input: Option<Box<RawValue>>,
    #[serde(default)]
    output: Option<Box<RawValue>>,
    #[serde(default)]
    cache_read: Option<Box<RawValue>>,
    #[serde(default)]
    cache_write: Option<Box<RawValue>>,
    #[serde(default)]
    reasoning: Option<Box<RawValue>>,
    #[serde(default)]
    input_audio: Option<Box<RawValue>>,
    #[serde(default)]
    output_audio: Option<Box<RawValue>>,
    #[serde(default)]
    tiers: Vec<WireTier>,
    /// The upstream's older spelling of a single long-context tier.
    #[serde(default)]
    context_over_200k: Option<WireTierRates>,
}

/// A tier: its threshold, and rates as siblings of it.
#[derive(Debug, Deserialize)]
struct WireTier {
    tier: WireTierKey,
    #[serde(default)]
    input: Option<Box<RawValue>>,
    #[serde(default)]
    output: Option<Box<RawValue>>,
    #[serde(default)]
    cache_read: Option<Box<RawValue>>,
    #[serde(default)]
    cache_write: Option<Box<RawValue>>,
    #[serde(default)]
    reasoning: Option<Box<RawValue>>,
    #[serde(default)]
    input_audio: Option<Box<RawValue>>,
    #[serde(default)]
    output_audio: Option<Box<RawValue>>,
}

#[derive(Debug, Deserialize)]
struct WireTierKey {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    size: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct WireTierRates {
    #[serde(default)]
    input: Option<Box<RawValue>>,
    #[serde(default)]
    output: Option<Box<RawValue>>,
    #[serde(default)]
    cache_read: Option<Box<RawValue>>,
    #[serde(default)]
    cache_write: Option<Box<RawValue>>,
    #[serde(default)]
    reasoning: Option<Box<RawValue>>,
    #[serde(default)]
    input_audio: Option<Box<RawValue>>,
    #[serde(default)]
    output_audio: Option<Box<RawValue>>,
}

/// One rate schedule's fields, borrowed from wherever they were stated.
///
/// The upstream states the same seven rates in three places — a cost, a tier, and
/// the legacy `context_over_200k` key — so they are read once, here.
struct WireRates<'a> {
    input: Option<&'a RawValue>,
    output: Option<&'a RawValue>,
    cache_read: Option<&'a RawValue>,
    cache_write: Option<&'a RawValue>,
    reasoning: Option<&'a RawValue>,
    input_audio: Option<&'a RawValue>,
    output_audio: Option<&'a RawValue>,
}

impl WireCost {
    /// Whether the object states nothing beyond the base rates, so an absent
    /// base pair means an absent price rather than a dropped one.
    fn states_only_base_rates(&self) -> bool {
        self.cache_read.is_none()
            && self.cache_write.is_none()
            && self.reasoning.is_none()
            && self.input_audio.is_none()
            && self.output_audio.is_none()
            && self.tiers.is_empty()
            && self.context_over_200k.is_none()
    }

    fn rates(&self) -> WireRates<'_> {
        WireRates {
            input: self.input.as_deref(),
            output: self.output.as_deref(),
            cache_read: self.cache_read.as_deref(),
            cache_write: self.cache_write.as_deref(),
            reasoning: self.reasoning.as_deref(),
            input_audio: self.input_audio.as_deref(),
            output_audio: self.output_audio.as_deref(),
        }
    }
}

impl WireTier {
    fn rates(&self) -> WireRates<'_> {
        WireRates {
            input: self.input.as_deref(),
            output: self.output.as_deref(),
            cache_read: self.cache_read.as_deref(),
            cache_write: self.cache_write.as_deref(),
            reasoning: self.reasoning.as_deref(),
            input_audio: self.input_audio.as_deref(),
            output_audio: self.output_audio.as_deref(),
        }
    }
}

impl WireTierRates {
    fn rates(&self) -> WireRates<'_> {
        WireRates {
            input: self.input.as_deref(),
            output: self.output.as_deref(),
            cache_read: self.cache_read.as_deref(),
            cache_write: self.cache_write.as_deref(),
            reasoning: self.reasoning.as_deref(),
            input_audio: self.input_audio.as_deref(),
            output_audio: self.output_audio.as_deref(),
        }
    }
}

/// The provider-neutral records, keyed as the upstream publishes them.
type NeutralRecords = BTreeMap<ModelId, (ModelFacts, JsonPointer)>;

/// Resolve every key one provider publishes to the id the catalogue files it
/// under.
///
/// Resolution is per provider rather than per key because a provider may publish
/// one model under two callable ids — `qiniu-ai` offers both `mimo-v2-flash` and
/// `xiaomi/mimo-v2-flash` — and an offering is one provider's statement about one
/// model. When two keys would resolve to the same model, only a key that *is*
/// the model's id keeps it; the others stay filed under the id they were
/// published as, which is the id a request to that provider uses. Two published
/// aliases therefore remain two offerings, as upstream states them, rather than
/// one of them being dropped or the import refused.
fn resolve_provider_models<'a>(
    published: &BTreeMap<&'a str, ModelId>,
    neutral: &NeutralRecords,
    pointers: &BTreeMap<&'a str, JsonPointer>,
) -> Result<BTreeMap<&'a str, ModelId>, ModelsDevError> {
    let mut resolved = BTreeMap::new();
    for (key, id) in published {
        let pointer = &pointers[key];
        resolved.insert(*key, canonical_model_id(id, neutral, pointer)?);
    }
    let claimed: BTreeMap<ModelId, usize> =
        resolved.values().fold(BTreeMap::new(), |mut counts, id| {
            *counts.entry(id.clone()).or_default() += 1;
            counts
        });
    for (key, id) in &mut resolved {
        if claimed[id] > 1 && *id != published[key] {
            *id = published[key].clone();
        }
    }
    Ok(resolved)
}

/// Resolve a provider's key for a model to the id the catalogue files it under.
///
/// The two indexes of the document do not share one id namespace: every key of
/// the top-level `models` map is authored (`openai/gpt-5.5`, and all 310 of them
/// in the live document carry an author), while a provider keys its offerings
/// the way *its own API* names them (`gpt-5.5` from `openai`, `openai/gpt-5.5`
/// from an aggregator that republishes the authored id). Joining the two by
/// string equality alone would file one model under two ids — the neutral record
/// under the authored one, the first-party offering under the provider-local one
/// — leaving 1,465 of the live document's offerings without the neutral record
/// they are variations of, and no consumer able to ask "who offers this model?".
///
/// So a key also resolves to a neutral record it is the unauthored tail of, at a
/// segment boundary: `gpt-5.5` is `openai/gpt-5.5` offered by its author.
/// `Qwen/Qwen3-32B` is not `some-author/other/Qwen/Qwen3-32B` unless the segments
/// line up, and a tail that matches two authored records is refused rather than
/// attributed to one of them: an offering whose model cannot be identified is
/// exactly the "changed meaning" this adapter will not guess at. No key in the
/// live document is ambiguous, so nothing upstream publishes today is refused by
/// this rule.
fn canonical_model_id(
    published: &ModelId,
    neutral: &NeutralRecords,
    pointer: &JsonPointer,
) -> Result<ModelId, ModelsDevError> {
    if neutral.contains_key(published) {
        return Ok(published.clone());
    }
    let tail = format!("/{published}");
    let candidates: Vec<&ModelId> = neutral
        .keys()
        .filter(|id| id.as_str().ends_with(&tail))
        .collect();
    match candidates.as_slice() {
        [] => Ok(published.clone()),
        [only] => Ok((*only).clone()),
        many => Err(ModelsDevError::AmbiguousModelKey {
            pointer: pointer.clone(),
            key: published.to_string(),
            candidates: many.iter().map(ToString::to_string).collect(),
        }),
    }
}

fn normalize(document: &WireCatalog) -> Result<CatalogContent, ModelsDevError> {
    let root = JsonPointer::new("");
    let providers_pointer = root.child("providers");
    let models_pointer = root.child("models");

    let mut neutral: NeutralRecords = BTreeMap::new();
    for (key, model) in &document.models {
        let pointer = models_pointer.child(key);
        let id = identifier(key, &pointer)?;
        expect_key(key, &model.id, &pointer)?;
        if model.cost.is_some() {
            return Err(ModelsDevError::NeutralPrice { pointer });
        }
        neutral.insert(id, (facts(model, &pointer)?, pointer));
    }

    let mut providers = Vec::with_capacity(document.providers.len());
    let mut offerings: BTreeMap<ModelId, Vec<ProviderOffering>> = BTreeMap::new();
    for (key, provider) in &document.providers {
        let pointer = providers_pointer.child(key);
        let id = identifier(key, &pointer)?;
        expect_key(key, &provider.id, &pointer)?;
        providers.push(CatalogProvider {
            id: id.clone(),
            display_name: text(Some(&provider.name)),
            doc_url: text(provider.doc.as_deref()),
            endpoint: ProviderEndpoint {
                api_base: text(provider.api.as_deref()),
                client_package: text(provider.npm.as_deref()),
                wire_shape: None,
            },
            env_vars: provider
                .env
                .iter()
                .filter_map(|env| text(Some(env)))
                .collect(),
            pointer: pointer.clone(),
        });

        let offered_pointer = pointer.child("models");
        let mut published_ids = BTreeMap::new();
        let mut pointers = BTreeMap::new();
        for (model_key, model) in &provider.models {
            let model_pointer = offered_pointer.child(model_key);
            let published = identifier(model_key, &model_pointer)?;
            expect_key(model_key, &model.id, &model_pointer)?;
            published_ids.insert(model_key.as_str(), published);
            pointers.insert(model_key.as_str(), model_pointer);
        }
        let resolved = resolve_provider_models(&published_ids, &neutral, &pointers)?;

        for (model_key, model) in &provider.models {
            let model_pointer = pointers[model_key.as_str()].clone();
            let model_id = resolved[model_key.as_str()].clone();
            let endpoint =
                model
                    .provider
                    .as_ref()
                    .map_or_else(ProviderEndpoint::default, |endpoint| ProviderEndpoint {
                        api_base: text(endpoint.api.as_deref()),
                        client_package: text(endpoint.npm.as_deref()),
                        wire_shape: text(endpoint.shape.as_deref()),
                    });
            offerings
                .entry(model_id.clone())
                .or_default()
                .push(ProviderOffering {
                    provider: id.clone(),
                    model: model_id,
                    published_model_id: model.id.clone(),
                    facts: facts(model, &model_pointer)?,
                    overrides: Vec::new(),
                    price: price(model.cost.as_ref(), &model_pointer)?,
                    endpoint,
                    pointer: model_pointer,
                });
        }
    }

    let ids: BTreeSet<ModelId> = neutral.keys().chain(offerings.keys()).cloned().collect();
    let models = ids
        .into_iter()
        .map(|id| {
            let neutral_facts = neutral.get(&id).map(|(facts, _)| facts.clone());
            let mut model_offerings = offerings.remove(&id).unwrap_or_default();
            if let Some(neutral_facts) = &neutral_facts {
                for offering in &mut model_offerings {
                    offering.overrides = offering
                        .facts
                        .differences(neutral_facts)
                        .into_iter()
                        .map(|field| (field, field_pointer(&offering.pointer, field)))
                        .collect();
                }
            }
            CatalogModelEntry {
                id,
                neutral: neutral_facts,
                offerings: model_offerings,
            }
        })
        .collect();

    CatalogContent::new(providers, models).map_err(|source| ModelsDevError::Content { source })
}

/// The payload location a field was read from, so an override points at the
/// provider's own value rather than at the offering as a whole.
fn field_pointer(offering: &JsonPointer, field: ModelField) -> JsonPointer {
    match field {
        ModelField::DisplayName => offering.child("name"),
        ModelField::Family => offering.child("family"),
        ModelField::Capabilities => offering.clone(),
        ModelField::InputModalities => offering.child("modalities").child("input"),
        ModelField::OutputModalities => offering.child("modalities").child("output"),
        ModelField::ContextTokens => offering.child("limit").child("context"),
        ModelField::InputTokens => offering.child("limit").child("input"),
        ModelField::OutputTokens => offering.child("limit").child("output"),
        ModelField::Lifecycle => offering.child("status"),
        ModelField::KnowledgeCutoff => offering.child("knowledge"),
        ModelField::ReleaseDate => offering.child("release_date"),
        ModelField::LastUpdated => offering.child("last_updated"),
        ModelField::Endpoint => offering.child("provider"),
        ModelField::PublishedModelId => offering.child("id"),
    }
}

/// Free text as normalized content holds it.
///
/// Surrounding whitespace is dropped and interior runs of it collapse to one
/// space: the upstream publishes names with trailing tabs (`"DeepSeek V3
/// (Turbo)\t"`), and canonical content holds no control characters. Whitespace
/// that carries no meaning must not be able to change a content identity or
/// register as a metadata diff, and text left empty by it is absent rather than
/// blank.
fn text(value: Option<&str>) -> Option<String> {
    let collapsed = value?.split_whitespace().collect::<Vec<_>>().join(" ");
    (!collapsed.is_empty()).then_some(collapsed)
}

fn identifier(key: &str, pointer: &JsonPointer) -> Result<ModelId, ModelsDevError> {
    ModelId::parse(key).map_err(|source| ModelsDevError::identifier(pointer, source))
}

/// A map key and the `id` inside it must agree: they are two spellings of one
/// identity, and a payload where they differ is one this adapter cannot resolve
/// without guessing which is authoritative.
fn expect_key(key: &str, id: &str, pointer: &JsonPointer) -> Result<(), ModelsDevError> {
    if key == id {
        return Ok(());
    }
    Err(ModelsDevError::IdMismatch {
        pointer: pointer.clone(),
        key: key.to_owned(),
        id: id.to_owned(),
    })
}

fn facts(model: &WireModel, pointer: &JsonPointer) -> Result<ModelFacts, ModelsDevError> {
    let mut capabilities = BTreeSet::new();
    for (stated, capability) in [
        (model.attachment, ModelCapability::Attachment),
        (model.reasoning, ModelCapability::Reasoning),
        (model.tool_call, ModelCapability::ToolCall),
        (model.temperature, ModelCapability::Temperature),
        (model.structured_output, ModelCapability::StructuredOutput),
        (
            model.interleaved.as_ref().map(WireFlag::configurable),
            ModelCapability::Interleaved,
        ),
        (model.open_weights, ModelCapability::OpenWeights),
        (
            model.experimental.as_ref().map(WireFlag::asserted),
            ModelCapability::Experimental,
        ),
    ] {
        if stated == Some(true) {
            capabilities.insert(capability);
        }
    }
    Ok(ModelFacts {
        display_name: text(Some(&model.name)),
        family: text(model.family.as_deref()),
        capabilities,
        input_modalities: modalities(
            &model.modalities.input,
            &pointer.child("modalities").child("input"),
        )?,
        output_modalities: modalities(
            &model.modalities.output,
            &pointer.child("modalities").child("output"),
        )?,
        limits: ModelLimits {
            context_tokens: model.limit.context,
            input_tokens: model.limit.input,
            output_tokens: model.limit.output,
        },
        lifecycle: lifecycle(model.status.as_deref(), &pointer.child("status"))?,
        knowledge_cutoff: text(model.knowledge.as_deref()),
        release_date: text(model.release_date.as_deref()),
        last_updated: text(model.last_updated.as_deref()),
    })
}

fn modalities(
    stated: &[String],
    pointer: &JsonPointer,
) -> Result<BTreeSet<Modality>, ModelsDevError> {
    stated
        .iter()
        .map(|modality| {
            Modality::parse(modality).ok_or_else(|| ModelsDevError::UnknownModality {
                pointer: pointer.clone(),
                modality: modality.clone(),
            })
        })
        .collect()
}

fn lifecycle(
    status: Option<&str>,
    pointer: &JsonPointer,
) -> Result<ModelLifecycle, ModelsDevError> {
    let Some(status) = status else {
        return Ok(ModelLifecycle::Available);
    };
    ModelLifecycle::ALL
        .iter()
        .copied()
        .find(|lifecycle| lifecycle.as_str() == status)
        .ok_or_else(|| ModelsDevError::UnknownStatus {
            pointer: pointer.clone(),
            status: status.to_owned(),
        })
}

fn price(
    cost: Option<&WireCost>,
    pointer: &JsonPointer,
) -> Result<Option<ObservedPrice>, ModelsDevError> {
    let Some(cost) = cost else {
        return Ok(None);
    };
    let pointer = pointer.child("cost");
    let stated = cost.rates();
    if stated.input.is_none() && stated.output.is_none() {
        if cost.states_only_base_rates() {
            // An empty `cost` object is an offering whose price the upstream has
            // not published, not a free one.
            return Ok(None);
        }
        // Tiers or optional rates without the base pair they qualify: reading
        // this as "no published price" would discard rates the payload does
        // state, which is the one thing this adapter never does silently.
        return Err(ModelsDevError::Price {
            pointer,
            reason: PriceRejection::Partial {
                stated: "tiered or optional rates",
                missing: "input and output",
            },
        });
    }
    let base = rates(&stated, &pointer)?;

    let mut tiers = Vec::new();
    for (index, tier) in cost.tiers.iter().enumerate() {
        let tier_pointer = pointer.child("tiers").child(&index.to_string());
        let threshold = match tier.tier.kind.as_str() {
            "context" => PriceTierThreshold::ContextOver {
                tokens: tier.tier.size.ok_or_else(|| ModelsDevError::Schema {
                    message: format!("`{tier_pointer}/tier` states no `size`"),
                })?,
            },
            kind => {
                return Err(ModelsDevError::UnknownTierType {
                    pointer: tier_pointer,
                    kind: kind.to_owned(),
                });
            }
        };
        tiers.push(PriceTier {
            threshold,
            rates: rates(&tier.rates(), &tier_pointer)?,
        });
    }
    if let Some(legacy) = &cost.context_over_200k {
        let tier_pointer = pointer.child("context_over_200k");
        let threshold = PriceTierThreshold::ContextOver {
            tokens: LEGACY_LONG_CONTEXT_TOKENS,
        };
        let legacy = PriceTier {
            threshold,
            rates: rates(&legacy.rates(), &tier_pointer)?,
        };
        // Upstream states this tier twice for most models that have it: once in
        // `tiers`, once under the older key. Two spellings of one tier are the
        // same tier when they agree, and a payload where they disagree is one
        // this adapter cannot resolve without picking a price.
        match tiers.iter().find(|tier| tier.threshold == threshold) {
            Some(stated) if *stated == legacy => {}
            Some(_) => return Err(ModelsDevError::DuplicateTier { pointer }),
            None => tiers.push(legacy),
        }
    }
    tiers.sort_by_key(|tier| tier.threshold);
    if tiers
        .windows(2)
        .any(|pair| pair[0].threshold == pair[1].threshold)
    {
        return Err(ModelsDevError::DuplicateTier { pointer });
    }
    Ok(Some(ObservedPrice { base, tiers }))
}

/// The context size the upstream's `context_over_200k` key names.
const LEGACY_LONG_CONTEXT_TOKENS: u64 = 200_000;

/// A rate schedule, which must state both of the rates every price has.
///
/// A half-stated price is refused rather than defaulted: an offering with an
/// input rate and no output rate would otherwise look like output tokens are
/// free.
fn rates(stated: &WireRates<'_>, pointer: &JsonPointer) -> Result<PriceRates, ModelsDevError> {
    let (Some(input), Some(output)) = (stated.input, stated.output) else {
        let (present, missing) = match (stated.input.is_some(), stated.output.is_some()) {
            (true, _) => ("input", "output"),
            (_, true) => ("output", "input"),
            // A tier or a legacy long-context object may state only optional
            // rates, and naming one of the base rates as present would point an
            // operator at a rate the payload never published.
            _ => ("only optional rates", "input and output"),
        };
        return Err(ModelsDevError::Price {
            pointer: pointer.clone(),
            reason: PriceRejection::Partial {
                stated: present,
                missing,
            },
        });
    };
    Ok(PriceRates {
        input: rate(input, &pointer.child("input"))?,
        output: rate(output, &pointer.child("output"))?,
        cache_read: optional_rate(stated.cache_read, pointer, "cache_read")?,
        cache_write: optional_rate(stated.cache_write, pointer, "cache_write")?,
        reasoning: optional_rate(stated.reasoning, pointer, "reasoning")?,
        input_audio: optional_rate(stated.input_audio, pointer, "input_audio")?,
        output_audio: optional_rate(stated.output_audio, pointer, "output_audio")?,
    })
}

fn optional_rate(
    raw: Option<&RawValue>,
    pointer: &JsonPointer,
    field: &str,
) -> Result<Option<ObservedRate>, ModelsDevError> {
    raw.map(|raw| rate(raw, &pointer.child(field))).transpose()
}

fn rate(raw: &RawValue, pointer: &JsonPointer) -> Result<ObservedRate, ModelsDevError> {
    nano_dollars_per_million(raw.get()).map_err(|reason| ModelsDevError::Price {
        pointer: pointer.clone(),
        reason,
    })
}

/// The digits of a decimal, and the power of ten they are scaled by.
struct Decimal {
    digits: u128,
    exponent: i32,
}

/// Convert a published dollars-per-million-tokens decimal into an integer
/// [`ObservedRate`].
///
/// Exact: the digits are read as an integer and the decimal point is moved by
/// integer arithmetic, so `0.1` is exactly `100_000_000` nano-dollars rather than
/// whatever the nearest `f64` rounds to. A value the unit cannot state is
/// refused, never rounded — with one narrow exception for values the upstream
/// itself computed in floating point (see [`is_binary_artifact`]).
fn nano_dollars_per_million(text: &str) -> Result<ObservedRate, PriceRejection> {
    /// Nano-dollars per dollar: how far the decimal point moves.
    const NANO_DOLLARS_PER_DOLLAR: i32 = 9;

    let decimal = parse_decimal(text)?;
    // JSON's number grammar bounds neither exponent, and a rate whose exponent
    // does not fit the shift is an unusable observation: refusing it costs an
    // import, where overflowing costs the task holding it.
    let shift = decimal
        .exponent
        .checked_add(NANO_DOLLARS_PER_DOLLAR)
        .ok_or_else(|| PriceRejection::Overflow {
            value: text.to_owned(),
        })?;
    let nanos = if shift >= 0 {
        let factor = 10u128
            .checked_pow(u32::try_from(shift).map_err(|_| PriceRejection::Overflow {
                value: text.to_owned(),
            })?)
            .ok_or_else(|| PriceRejection::Overflow {
                value: text.to_owned(),
            })?;
        decimal
            .digits
            .checked_mul(factor)
            .ok_or_else(|| PriceRejection::Overflow {
                value: text.to_owned(),
            })?
    } else {
        let divisor = 10u128.checked_pow(shift.unsigned_abs()).ok_or_else(|| {
            PriceRejection::ExcessPrecision {
                value: text.to_owned(),
            }
        })?;
        let remainder = decimal.digits % divisor;
        let quotient = decimal.digits / divisor;
        match remainder {
            0 => quotient,
            _ if is_binary_artifact(decimal.digits, divisor, remainder) => {
                // `0.049999999999999996` is not a rate stated to eighteen
                // places; it is `0.05` after a round trip through a binary float
                // upstream. Reading it as `0.05` recovers what was published.
                if remainder * 2 >= divisor {
                    quotient + 1
                } else {
                    quotient
                }
            }
            _ => {
                return Err(PriceRejection::ExcessPrecision {
                    value: text.to_owned(),
                });
            }
        }
    };
    u64::try_from(nanos)
        .map(ObservedRate::from_nanos)
        .map_err(|_| PriceRejection::Overflow {
            value: text.to_owned(),
        })
}

/// Read a JSON number's text into digits and an exponent.
///
/// The JSON number grammar, not Rust's: anything else — a quoted string, a
/// leading `+`, an empty fraction — is refused rather than coerced, so a payload
/// that changed a price's type is a rejection and not a zero.
fn parse_decimal(text: &str) -> Result<Decimal, PriceRejection> {
    let not_a_number = || PriceRejection::NotANumber {
        value: text.to_owned(),
    };
    if text.is_empty() {
        return Err(not_a_number());
    }
    if let Some(rest) = text.strip_prefix('-') {
        // Refused as negative only when it really is a number: a quoted string
        // beginning with `-` is a type error, not a sign error.
        return parse_decimal(rest).and(Err(PriceRejection::Negative {
            value: text.to_owned(),
        }));
    }

    let (mantissa, exponent) = match text.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => {
            let stated = exponent.strip_prefix('+').unwrap_or(exponent);
            let exponent = stated.parse::<i32>().map_err(|_| {
                // An exponent JSON allows but no integer holds: a well-formed
                // number, and a rate out of range in whichever direction it
                // points.
                match stated.strip_prefix('-') {
                    Some(magnitude) if all_digits(magnitude) && !magnitude.is_empty() => {
                        PriceRejection::ExcessPrecision {
                            value: text.to_owned(),
                        }
                    }
                    Some(_) => not_a_number(),
                    None if all_digits(stated) && !stated.is_empty() => PriceRejection::Overflow {
                        value: text.to_owned(),
                    },
                    None => not_a_number(),
                }
            })?;
            (mantissa, exponent)
        }
        None => (text, 0),
    };
    let (integer, fraction) = match mantissa.split_once('.') {
        Some((integer, fraction)) => (integer, fraction),
        None => (mantissa, ""),
    };
    if integer.is_empty() || !all_digits(integer) || (mantissa.contains('.') && fraction.is_empty())
    {
        return Err(not_a_number());
    }
    if !all_digits(fraction) {
        return Err(not_a_number());
    }
    let mut digits = String::with_capacity(integer.len() + fraction.len());
    digits.push_str(integer);
    digits.push_str(fraction);
    let digits = digits
        .parse::<u128>()
        .map_err(|_| PriceRejection::Overflow {
            value: text.to_owned(),
        })?;
    let fraction_length = i32::try_from(fraction.len()).map_err(|_| PriceRejection::Overflow {
        value: text.to_owned(),
    })?;
    Ok(Decimal {
        digits,
        // An exponent this far below zero states a rate below any nano-dollar,
        // whatever its digits are.
        exponent: exponent.checked_sub(fraction_length).ok_or_else(|| {
            PriceRejection::ExcessPrecision {
                value: text.to_owned(),
            }
        })?,
    })
}

/// Whether a value's excess precision is a binary floating-point artifact of a
/// representable rate rather than a rate of its own.
///
/// The upstream computes some rates in a language whose numbers are `f64`, so
/// `0.05` is published as `0.049999999999999996`. The distinction from real
/// excess precision is relative distance: an artifact sits about one part in 2⁵³
/// from a representable rate, while a rate someone actually wrote —
/// `0.0000000001` — sits a large fraction of one away. The tolerance is far
/// tighter than any decimal a person would publish and far looser than `f64`
/// rounding, so neither case can be mistaken for the other.
fn is_binary_artifact(digits: u128, divisor: u128, remainder: u128) -> bool {
    /// One part in a trillion: far coarser than `f64`'s ~1-in-9×10¹⁵ resolution,
    /// and far finer than the precision of any published decimal.
    const TOLERANCE: u128 = 1_000_000_000_000;

    let distance = remainder.min(divisor - remainder);
    distance
        .checked_mul(TOLERANCE)
        .is_some_and(|scaled| scaled <= digits)
}

fn all_digits(text: &str) -> bool {
    text.bytes().all(|byte| byte.is_ascii_digit())
}

/// The bundled offline seed: a checked-in models.dev excerpt.
///
/// A deployment with no egress, an air-gapped one, or a test still needs a
/// catalogue to exist. The seed is that catalogue: real upstream data, reviewed
/// in the repository, imported through exactly the same parser and validation as
/// a fetched payload — so a seed that would be refused on the wire is refused
/// here too, and CI notices.
pub const SEED_PAYLOAD: &str = include_str!("fixtures/models_dev/catalog.seed.json");

/// The seed's recorded retrieval time: when the excerpt was taken.
///
/// A constant rather than "now", so importing the seed twice produces identical
/// provenance and a test can assert on it.
pub fn seed_fetched_at() -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(1_786_566_474)
}

/// The upstream validator the excerpt was served with.
fn seed_validators() -> SourceValidators {
    SourceValidators {
        etag: Some(ETag("\"38a27321531a976c916911889525f559\"".to_owned())),
        last_modified: Some(HttpDate("Wed, 12 Aug 2026 20:27:54 GMT".to_owned())),
    }
}

/// Parse the bundled seed.
///
/// Infallible by construction — a malformed seed fails the suite — so callers do
/// not have to decide what to do about a broken constant.
pub fn seed_snapshot() -> CatalogSnapshot {
    ModelsDevAdapter::default()
        .parse(
            SEED_PAYLOAD.as_bytes(),
            seed_validators(),
            seed_fetched_at(),
        )
        .expect("the bundled models.dev seed parses")
}

/// A [`CatalogSource`] that serves the bundled seed and never uses the network.
///
/// This is what "offline operation" means for this slice: a deployment can hold a
/// real catalogue without egress, and the refresh path is exercised end to end in
/// tests without an HTTP server.
#[derive(Debug, Default, Clone, Copy)]
pub struct SeedCatalogSource;

#[async_trait]
impl CatalogSource for SeedCatalogSource {
    fn name(&self) -> &'static str {
        "models.dev-seed"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::new(&[Capability::IncrementalRefresh, Capability::PriceMetadata])
    }

    async fn refresh(
        &self,
        since: Option<&SourceValidators>,
    ) -> Result<CatalogRefresh, CatalogError> {
        let validators = seed_validators();
        if since == Some(&validators) {
            return Ok(CatalogRefresh::Unchanged { validators });
        }
        Ok(CatalogRefresh::Updated(Box::new(seed_snapshot())))
    }
}

/// What one conditional fetch returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchResponse {
    /// The upstream answered `304`: the validators still match, and no payload
    /// was transferred.
    NotModified { validators: SourceValidators },
    Payload {
        bytes: Vec<u8>,
        validators: SourceValidators,
    },
}

/// The largest payload a refresh will hold.
///
/// The real document is a few megabytes, so this is generous by an order of
/// magnitude and still bounded: the source URL is operator-configurable, and a
/// mirror that answers with an endless body must cost a refused refresh rather
/// than the process's memory. Enforced twice on purpose — a [`CatalogFetch`]
/// stops reading at the ceiling, and [`ModelsDevSource`] re-checks what it was
/// handed, so an implementation that forgets cannot make the source unbounded.
pub const MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

/// Why a fetch did not produce a payload.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FetchError {
    #[error("{message}")]
    Transport { message: String },
    #[error("upstream answered HTTP {status}")]
    Status { status: u16 },
    #[error("payload exceeds the {limit}-byte ceiling")]
    TooLarge { limit: usize },
}

impl From<FetchError> for CatalogError {
    fn from(error: FetchError) -> Self {
        match error {
            FetchError::Status { status } if status == 401 || status == 403 => Self::Denied {
                backend: BACKEND,
                message: error.to_string(),
            },
            error => Self::Unavailable {
                backend: BACKEND,
                message: error.to_string(),
            },
        }
    }
}

/// Read a response body without holding more than `limit` bytes of it.
///
/// Streamed and checked as it arrives rather than afterwards: `Response::bytes`
/// allocates the whole body before anyone can object, and a declared
/// `Content-Length` is a claim rather than a bound — so the declaration is
/// refused early when it is already too large, and the chunks are counted
/// regardless of what it said.
pub async fn bounded_body(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, FetchError> {
    let declared = response.content_length();
    if declared.is_some_and(|declared| declared > limit as u64) {
        return Err(FetchError::TooLarge { limit });
    }
    let mut body = Vec::with_capacity(
        declared
            .and_then(|declared| usize::try_from(declared).ok())
            .unwrap_or_default()
            .min(limit),
    );
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| FetchError::Transport {
            message: error.to_string(),
        })?
    {
        if body.len() + chunk.len() > limit {
            return Err(FetchError::TooLarge { limit });
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// How a payload is retrieved.
///
/// Injected rather than hard-wired so the source's conditional-request behaviour
/// is testable against a local server, and so scheduling, backoff, and staleness
/// reporting — which are a later slice — have a seam to attach to instead of a
/// reason to rewrite this one.
///
/// An implementation is expected to stop reading at the ceiling it was given
/// ([`bounded_body`] does); [`ModelsDevSource`] re-checks what it was handed, so
/// one that does not cannot make the source unbounded.
#[async_trait]
pub trait CatalogFetch: Send + Sync {
    async fn get(
        &self,
        url: &str,
        validators: Option<&SourceValidators>,
    ) -> Result<FetchResponse, FetchError>;
}

/// The models.dev source: a conditional fetch, then a strict parse.
///
/// Background use only. Nothing constructs one on the request path, and this
/// slice does not construct one during boot either.
#[derive(Debug)]
pub struct ModelsDevSource<F> {
    adapter: ModelsDevAdapter,
    fetch: F,
    payload_limit: usize,
}

impl<F: CatalogFetch> ModelsDevSource<F> {
    pub const fn new(adapter: ModelsDevAdapter, fetch: F) -> Self {
        Self {
            adapter,
            fetch,
            payload_limit: MAX_PAYLOAD_BYTES,
        }
    }

    /// Hold less than the default ceiling.
    #[must_use]
    pub const fn with_payload_limit(mut self, limit: usize) -> Self {
        self.payload_limit = limit;
        self
    }
}

#[async_trait]
impl<F: CatalogFetch> CatalogSource for ModelsDevSource<F> {
    fn name(&self) -> &'static str {
        BACKEND
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::new(&[Capability::IncrementalRefresh, Capability::PriceMetadata])
    }

    async fn refresh(
        &self,
        since: Option<&SourceValidators>,
    ) -> Result<CatalogRefresh, CatalogError> {
        match self.fetch.get(self.adapter.source_url(), since).await? {
            FetchResponse::NotModified { validators } => {
                Ok(CatalogRefresh::Unchanged { validators })
            }
            FetchResponse::Payload { bytes, validators } => {
                if bytes.len() > self.payload_limit {
                    return Err(FetchError::TooLarge {
                        limit: self.payload_limit,
                    }
                    .into());
                }
                Ok(CatalogRefresh::Updated(Box::new(self.adapter.parse(
                    &bytes,
                    validators,
                    SystemTime::now(),
                )?)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode, header};
    use axum::response::{IntoResponse, Response};
    use axum::routing::get;

    use super::super::catalog::{
        Admission, CatalogChange, LastKnownGoodCatalog, ModelCapability, ProviderId,
    };
    use super::*;

    const IDENTITY: &str = include_str!("fixtures/models_dev/catalog.identity.json");
    const ALIASES: &str = include_str!("fixtures/models_dev/catalog.aliases.json");
    const IDENTITY_REORDERED: &str =
        include_str!("fixtures/models_dev/catalog.identity-reordered.json");

    /// The identity of `catalog.identity.json`'s normalized content.
    ///
    /// A golden value: it pins the canonical encoding as well as the
    /// normalization, so a change to either is a deliberate edit here rather than
    /// a silent change to every stored snapshot's identity.
    const IDENTITY_CONTENT_ID: &str =
        "sha256:4ae07b3da3c559576a5be87dbed8349b766e901f2d2243df6cca4696e514e454";

    fn drift(name: &str) -> &'static str {
        match name {
            "limit-type" => include_str!("fixtures/models_dev/drift.limit-type.json"),
            "unknown-status" => include_str!("fixtures/models_dev/drift.unknown-status.json"),
            "unknown-modality" => include_str!("fixtures/models_dev/drift.unknown-modality.json"),
            "price-precision" => include_str!("fixtures/models_dev/drift.price-precision.json"),
            "price-negative" => include_str!("fixtures/models_dev/drift.price-negative.json"),
            "price-type" => include_str!("fixtures/models_dev/drift.price-type.json"),
            "price-partial" => include_str!("fixtures/models_dev/drift.price-partial.json"),
            "tier-type" => include_str!("fixtures/models_dev/drift.tier-type.json"),
            "tier-duplicate" => include_str!("fixtures/models_dev/drift.tier-duplicate.json"),
            "model-id-mismatch" => include_str!("fixtures/models_dev/drift.model-id-mismatch.json"),
            "model-id-case" => include_str!("fixtures/models_dev/drift.model-id-case.json"),
            "provider-id-mismatch" => {
                include_str!("fixtures/models_dev/drift.provider-id-mismatch.json")
            }
            "neutral-price" => include_str!("fixtures/models_dev/drift.neutral-price.json"),
            "missing-providers" => include_str!("fixtures/models_dev/drift.missing-providers.json"),
            "missing-model-name" => {
                include_str!("fixtures/models_dev/drift.missing-model-name.json")
            }
            "top-level-array" => include_str!("fixtures/models_dev/drift.top-level-array.json"),
            "empty" => include_str!("fixtures/models_dev/drift.empty.json"),
            "not-json" => include_str!("fixtures/models_dev/drift.not-json.json"),
            "control-character" => {
                include_str!("fixtures/models_dev/drift.control-character.json")
            }
            "price-tiers-without-base" => {
                include_str!("fixtures/models_dev/drift.price-tiers-without-base.json")
            }
            "tier-without-base" => {
                include_str!("fixtures/models_dev/drift.tier-without-base.json")
            }
            "model-key-ambiguous" => {
                include_str!("fixtures/models_dev/drift.model-key-ambiguous.json")
            }
            other => panic!("no drift fixture named `{other}`"),
        }
    }

    fn parse(payload: &str) -> Result<CatalogSnapshot, ModelsDevError> {
        ModelsDevAdapter::default().parse(
            payload.as_bytes(),
            SourceValidators::etag("\"fixture\""),
            SystemTime::UNIX_EPOCH,
        )
    }

    #[test]
    fn only_the_catalog_document_is_an_accepted_source() {
        assert_eq!(
            ModelsDevAdapter::new(MODELS_DEV_CATALOG_URL)
                .expect("the supported endpoint")
                .source_url(),
            MODELS_DEV_CATALOG_URL
        );
        assert!(ModelsDevAdapter::new("https://mirror.example/models.dev/catalog.json").is_ok());
        for rejected in [
            "https://models.dev/api.json",
            "https://models.dev/models.json",
            "https://models.dev/",
        ] {
            assert_eq!(
                ModelsDevAdapter::new(rejected),
                Err(ModelsDevError::UnsupportedEndpoint {
                    url: rejected.to_owned()
                }),
                "`{rejected}` is a different document shape"
            );
        }
    }

    #[test]
    fn normalization_is_independent_of_key_order_formatting_and_unknown_fields() {
        let ordered = parse(IDENTITY).expect("fixture parses");
        let reordered = parse(IDENTITY_REORDERED).expect("reordered fixture parses");

        assert_eq!(ordered.content, reordered.content);
        assert_eq!(ordered.source.content_id, reordered.source.content_id);
        assert_ne!(
            ordered.source.raw, reordered.source.raw,
            "the raw payloads differ, and the raw digest says so"
        );
    }

    #[test]
    fn the_content_identity_is_stable_across_releases() {
        let snapshot = parse(IDENTITY).expect("fixture parses");
        assert_eq!(snapshot.source.content_id.to_string(), IDENTITY_CONTENT_ID);
    }

    #[test]
    fn provider_offerings_keep_their_overrides_with_provenance() {
        let snapshot = parse(IDENTITY).expect("fixture parses");
        let id = ModelId::parse("openai/gpt-5.5").expect("id");
        let entry = snapshot.content.model(&id).expect("the fixture's model");
        let neutral = entry.neutral.as_ref().expect("a neutral record");
        assert_eq!(neutral.limits.context_tokens, Some(1_050_000));

        let openai = entry
            .offering(&ProviderId::parse("openai").expect("id"))
            .expect("the first-party offering");
        assert!(
            !openai.has_overrides(),
            "an offering that agrees with the neutral record overrides nothing"
        );

        let aggregator = entry
            .offering(&ProviderId::parse("hpc-ai").expect("id"))
            .expect("the aggregator's offering");
        let overrides: Vec<(&str, &str)> = aggregator
            .overrides
            .iter()
            .map(|(field, pointer)| (field.as_str(), pointer.as_str()))
            .collect();
        assert_eq!(
            overrides,
            vec![
                ("capabilities", "/providers/hpc-ai/models/openai~1gpt-5.5"),
                (
                    "input_modalities",
                    "/providers/hpc-ai/models/openai~1gpt-5.5/modalities/input"
                ),
                (
                    "context_tokens",
                    "/providers/hpc-ai/models/openai~1gpt-5.5/limit/context"
                ),
                (
                    "input_tokens",
                    "/providers/hpc-ai/models/openai~1gpt-5.5/limit/input"
                ),
                (
                    "lifecycle",
                    "/providers/hpc-ai/models/openai~1gpt-5.5/status"
                ),
            ]
        );
        // The provider's own values are what the offering states, so provider
        // metadata takes precedence without a second resolution step.
        assert_eq!(aggregator.facts.limits.context_tokens, Some(272_000));
        assert_eq!(aggregator.facts.lifecycle, ModelLifecycle::Deprecated);
        assert!(!aggregator.facts.input_modalities.contains(&Modality::Pdf));
        assert!(
            !aggregator
                .facts
                .capabilities
                .contains(&ModelCapability::StructuredOutput)
        );
        assert_eq!(
            aggregator.endpoint.api_base.as_deref(),
            Some("https://api.hpc-ai.com/v1")
        );
    }

    #[test]
    fn published_decimals_become_exact_integer_rates() {
        let snapshot = parse(IDENTITY).expect("fixture parses");
        let id = ModelId::parse("openai/gpt-5.5").expect("id");
        let entry = snapshot.content.model(&id).expect("model");
        let price = entry
            .offering(&ProviderId::parse("openai").expect("id"))
            .expect("offering")
            .price
            .as_ref()
            .expect("a published price");
        assert_eq!(price.base.input, ObservedRate::from_nanos(5_000_000_000));
        assert_eq!(price.base.output, ObservedRate::from_nanos(30_000_000_000));
        assert_eq!(
            price.base.cache_read,
            Some(ObservedRate::from_nanos(500_000_000))
        );
        assert!(price.tiers.is_empty());

        let tiered = entry
            .offering(&ProviderId::parse("hpc-ai").expect("id"))
            .expect("offering")
            .price
            .as_ref()
            .expect("a published price");
        assert_eq!(
            tiered.tiers,
            vec![PriceTier {
                threshold: PriceTierThreshold::ContextOver { tokens: 272_000 },
                rates: PriceRates {
                    input: ObservedRate::from_nanos(12_500_000_000),
                    output: ObservedRate::from_nanos(50_000_000_000),
                    ..PriceRates::new(ObservedRate::ZERO, ObservedRate::ZERO)
                },
            }]
        );
    }

    #[test]
    fn decimal_conversion_is_exact_and_refuses_what_it_cannot_state() {
        for (text, nanos) in [
            ("0", 0),
            ("10", 10_000_000_000),
            ("2.5", 2_500_000_000),
            ("0.075", 75_000_000),
            ("0.1", 100_000_000),
            // Finer than the gateway's own micro-dollars, held as published.
            ("0.26666667", 266_666_670),
            ("0.000000001", 1),
            ("1e-9", 1),
            ("1.5e1", 15_000_000_000),
            ("1E+2", 100_000_000_000),
        ] {
            assert_eq!(
                nano_dollars_per_million(text),
                Ok(ObservedRate::from_nanos(nanos)),
                "`{text}` converts exactly"
            );
        }
        // Rates the upstream computed in floating point, recovered rather than
        // refused: these are real values from `catalog.json`.
        for (published, nanos) in [
            ("0.049999999999999996", 50_000_000),
            ("0.09999999999999999", 100_000_000),
            ("2.9000000000000004", 2_900_000_000),
            ("0.12500000000000003", 125_000_000),
        ] {
            assert_eq!(
                nano_dollars_per_million(published),
                Ok(ObservedRate::from_nanos(nanos)),
                "`{published}` is a float artifact of a representable rate"
            );
        }
        for finer in ["0.0000000001", "0.0000000015", "0.1234567891"] {
            assert_eq!(
                nano_dollars_per_million(finer),
                Err(PriceRejection::ExcessPrecision {
                    value: finer.to_owned()
                }),
                "`{finer}` is a rate finer than the gateway represents"
            );
        }
        assert_eq!(
            nano_dollars_per_million("-1"),
            Err(PriceRejection::Negative {
                value: "-1".to_owned()
            })
        );
        assert_eq!(
            nano_dollars_per_million("1e30"),
            Err(PriceRejection::Overflow {
                value: "1e30".to_owned()
            })
        );
        // Exponents JSON's grammar allows and no rate can hold: refused as out
        // of range, never by overflowing the arithmetic that reads them.
        for enormous in ["1e2147483647", "1e2147483648", "1E999999999999999999"] {
            assert_eq!(
                nano_dollars_per_million(enormous),
                Err(PriceRejection::Overflow {
                    value: enormous.to_owned()
                }),
                "`{enormous}` states more dollars than a rate holds"
            );
        }
        for minuscule in ["1.5e-2147483648", "1e-2147483649", "1e-999999999999999999"] {
            assert_eq!(
                nano_dollars_per_million(minuscule),
                Err(PriceRejection::ExcessPrecision {
                    value: minuscule.to_owned()
                }),
                "`{minuscule}` states a rate below any nano-dollar"
            );
        }
        for malformed in [
            "", "\"10\"", "+1", "1.", ".5", "1.2.3", "abc", "null", "1e", "1e-",
        ] {
            assert!(
                matches!(
                    nano_dollars_per_million(malformed),
                    Err(PriceRejection::NotANumber { .. })
                ),
                "`{malformed}` is not a JSON number"
            );
        }
    }

    #[test]
    fn every_drifted_payload_is_refused_with_a_pointer() {
        /// A fixture's name and what refusing it must look like.
        type Expectation = (&'static str, fn(&ModelsDevError) -> bool);

        let expectations: &[Expectation] = &[
            ("not-json", |error| {
                matches!(error, ModelsDevError::NotJson { .. })
            }),
            ("top-level-array", |error| {
                matches!(error, ModelsDevError::Schema { .. })
            }),
            ("missing-providers", |error| {
                matches!(error, ModelsDevError::Schema { .. })
            }),
            ("missing-model-name", |error| {
                matches!(error, ModelsDevError::Schema { .. })
            }),
            ("limit-type", |error| {
                matches!(error, ModelsDevError::Schema { .. })
            }),
            (
                "unknown-status",
                |error| matches!(error, ModelsDevError::UnknownStatus { status, .. } if status == "sunset"),
            ),
            ("unknown-modality", |error| {
                matches!(
                    error,
                    ModelsDevError::UnknownModality { modality, .. } if modality == "telepathy"
                )
            }),
            ("price-precision", |error| {
                matches!(
                    error,
                    ModelsDevError::Price {
                        reason: PriceRejection::ExcessPrecision { .. },
                        ..
                    }
                )
            }),
            ("price-negative", |error| {
                matches!(
                    error,
                    ModelsDevError::Price {
                        reason: PriceRejection::Negative { .. },
                        ..
                    }
                )
            }),
            ("price-type", |error| {
                matches!(
                    error,
                    ModelsDevError::Price {
                        reason: PriceRejection::NotANumber { .. },
                        ..
                    }
                )
            }),
            // Tiers with no base pair are refused rather than read as "no
            // published price", which would drop the rates the payload states.
            ("price-tiers-without-base", |error| {
                matches!(
                    error,
                    ModelsDevError::Price {
                        reason: PriceRejection::Partial {
                            stated: "tiered or optional rates",
                            ..
                        },
                        ..
                    }
                )
            }),
            ("price-partial", |error| {
                matches!(
                    error,
                    ModelsDevError::Price {
                        reason: PriceRejection::Partial { .. },
                        ..
                    }
                )
            }),
            // A tier stating only an optional rate states neither base rate, so
            // the refusal names neither as published.
            ("tier-without-base", |error| {
                matches!(
                    error,
                    ModelsDevError::Price {
                        reason: PriceRejection::Partial {
                            stated: "only optional rates",
                            missing: "input and output",
                        },
                        ..
                    }
                )
            }),
            (
                "tier-type",
                |error| matches!(error, ModelsDevError::UnknownTierType { kind, .. } if kind == "requests"),
            ),
            ("tier-duplicate", |error| {
                matches!(error, ModelsDevError::DuplicateTier { .. })
            }),
            ("model-id-mismatch", |error| {
                matches!(error, ModelsDevError::IdMismatch { .. })
            }),
            // Case is meaning, not noise: a key that differs from its `id` only
            // in case is a mismatch rather than a spelling to normalize.
            ("model-id-case", |error| {
                matches!(error, ModelsDevError::IdMismatch { .. })
            }),
            ("provider-id-mismatch", |error| {
                matches!(error, ModelsDevError::IdMismatch { .. })
            }),
            // A provider-local key that is the tail of two authored records
            // names no single model, so it is refused rather than attributed to
            // whichever record sorts first.
            ("model-key-ambiguous", |error| {
                matches!(
                    error,
                    ModelsDevError::AmbiguousModelKey { key, candidates, .. }
                        if key == "m-1" && candidates == &["alpha/m-1", "beta/m-1"]
                )
            }),
            ("neutral-price", |error| {
                matches!(error, ModelsDevError::NeutralPrice { .. })
            }),
            ("empty", |error| {
                matches!(
                    error,
                    ModelsDevError::Content {
                        source: CatalogContentError::Empty
                    }
                )
            }),
            // Text a canonical form cannot hold is refused, not asserted about:
            // the content would otherwise have no identity, and an import that
            // cannot be identified cannot be admitted.
            ("control-character", |error| {
                matches!(
                    error,
                    ModelsDevError::Content {
                        source: CatalogContentError::Uncanonicalizable { .. }
                    }
                )
            }),
        ];
        for (name, expected) in expectations {
            let error = parse(drift(name)).expect_err("a drifted payload is refused");
            assert!(
                expected(&error),
                "`{name}` produced the wrong error: {error}"
            );
        }
    }

    #[test]
    fn a_refused_payload_cannot_replace_last_known_good_state() {
        let mut catalogue = LastKnownGoodCatalog::new();
        let good = parse(IDENTITY).expect("fixture parses");
        let content_id = good.source.content_id;
        assert_eq!(catalogue.admit(good), Admission::Initial { content_id });

        for name in [
            "not-json",
            "unknown-status",
            "price-precision",
            "empty",
            "control-character",
        ] {
            let (error, active) = catalogue
                .admit_result(parse(drift(name)))
                .expect_err("a drifted payload is refused");
            assert!(!error.to_string().is_empty());
            assert_eq!(
                active.map(|snapshot| snapshot.source.content_id),
                Some(content_id),
                "`{name}` must not disturb the active catalogue"
            );
        }
        assert_eq!(
            catalogue
                .active()
                .map(|snapshot| snapshot.source.content_id),
            Some(content_id)
        );
    }

    #[test]
    fn the_offline_seed_parses_deterministically() {
        let first = seed_snapshot();
        let second = seed_snapshot();
        assert_eq!(first, second);
        assert_eq!(first.source.content_id, second.source.content_id);
        assert_eq!(first.source.fetched_at, seed_fetched_at());
        assert_eq!(first.source.source_url, MODELS_DEV_CATALOG_URL);
        assert_eq!(
            first.source.schema_version,
            SchemaVersion::MODELS_DEV_CATALOG_V1
        );
        assert_eq!(first.source.raw.size_bytes as usize, SEED_PAYLOAD.len());

        // The excerpt keeps the shapes the adapter has to handle.
        let content = &first.content;
        assert_eq!(content.providers().len(), 4);
        assert!(content.offering_count() >= 5);
        let deprecated = content
            .offering(
                &ModelId::parse("gpt-4o").expect("id"),
                &ProviderId::parse("azure").expect("id"),
            )
            .expect("azure offers gpt-4o");
        assert_eq!(deprecated.facts.lifecycle, ModelLifecycle::Deprecated);
        let tiered = content
            .offering(
                &ModelId::parse("openai/gpt-5.5").expect("id"),
                &ProviderId::parse("hpc-ai").expect("id"),
            )
            .expect("hpc-ai offers gpt-5.5");
        assert_eq!(
            tiered
                .price
                .as_ref()
                .expect("a published price")
                .tiers
                .iter()
                .map(|tier| tier.threshold)
                .collect::<Vec<_>>(),
            vec![
                PriceTierThreshold::ContextOver { tokens: 200_000 },
                PriceTierThreshold::ContextOver { tokens: 272_000 },
            ]
        );
    }

    /// The upstream's two indexes use two id namespaces — the neutral index is
    /// authored, a provider keys offerings as its own API names them — so the
    /// seed's `openai/gpt-5.5` and OpenAI's `gpt-5.5` are one model, and filing
    /// them apart would leave the first-party offering with no neutral record
    /// and the model listed twice.
    #[test]
    fn a_provider_local_key_files_under_the_model_it_offers() {
        let content = seed_snapshot().content;
        let id = ModelId::parse("openai/gpt-5.5").expect("id");
        assert!(
            content
                .model(&ModelId::parse("gpt-5.5").expect("id"))
                .is_none(),
            "a provider-local key is not a model of its own"
        );

        let entry = content.model(&id).expect("the authored record");
        assert!(entry.neutral.is_some());
        let offering = content
            .offering(&id, &ProviderId::parse("openai").expect("id"))
            .expect("its author offers it");
        assert_eq!(
            offering.published_model_id, "gpt-5.5",
            "a request to OpenAI must still use OpenAI's own id"
        );
        assert!(
            offering.overrides.is_empty(),
            "and it is compared against the neutral record it agrees with, \
             rather than having none to compare against"
        );
        assert!(
            entry
                .offerings
                .iter()
                .any(|offering| offering.published_model_id == "openai/gpt-5.5"),
            "an aggregator republishing the authored id joins the same entry"
        );
    }

    /// A provider may publish one model under two callable ids, and both are
    /// ids a request can use, so neither the import nor either offering may be
    /// lost to the join that files provider-local keys under authored ones.
    #[test]
    fn two_published_aliases_of_one_model_stay_two_offerings() {
        let content = parse(ALIASES).expect("fixture parses").content;
        let authored = ModelId::parse("xiaomi/mimo-v2-flash").expect("id");
        let alias = ModelId::parse("mimo-v2-flash").expect("id");
        let provider = ProviderId::parse("qiniu-ai").expect("id");

        let joined = content
            .offering(&authored, &provider)
            .expect("the authored id is the model it names");
        assert_eq!(joined.published_model_id, "xiaomi/mimo-v2-flash");
        assert!(
            content
                .model(&authored)
                .and_then(|entry| entry.neutral.as_ref())
                .is_some()
        );

        let kept = content
            .offering(&alias, &provider)
            .expect("the provider's other id is still an offering");
        assert_eq!(
            kept.published_model_id, "mimo-v2-flash",
            "a request may use either published id, so neither is dropped"
        );
        assert_eq!(content.offering_count(), 2);
    }

    /// An object-valued flag says different things for different keys, and the
    /// seed publishes both shapes: `interleaved` configures the capability it
    /// names, while `experimental` describes extra modes of an offering that is
    /// not itself experimental.
    #[test]
    fn an_object_valued_flag_states_the_capability_only_where_it_configures_it() {
        let content = seed_snapshot().content;
        let configured = content
            .offering(
                &ModelId::parse("openai/gpt-5.5").expect("id"),
                &ProviderId::parse("hpc-ai").expect("id"),
            )
            .expect("hpc-ai offers gpt-5.5");
        assert!(
            configured
                .facts
                .capabilities
                .contains(&ModelCapability::Interleaved)
        );

        let modes = content
            .offering(
                &ModelId::parse("openai/gpt-5.5").expect("id"),
                &ProviderId::parse("openai").expect("id"),
            )
            .expect("openai offers gpt-5.5");
        assert!(
            !modes
                .facts
                .capabilities
                .contains(&ModelCapability::Experimental),
            "`experimental: {{ modes: … }}` describes modes, not the offering's status"
        );
        assert!(
            !modes.overrides_field(ModelField::Capabilities),
            "and so it is not an override of the neutral record either"
        );
    }

    #[tokio::test]
    async fn the_seed_source_serves_the_catalogue_without_a_network() {
        let source = SeedCatalogSource;
        let CatalogRefresh::Updated(snapshot) = source.refresh(None).await.expect("refresh") else {
            panic!("a first refresh transfers the seed");
        };
        assert_eq!(
            source.refresh(Some(&snapshot.source.validators)).await,
            Ok(CatalogRefresh::Unchanged {
                validators: snapshot.source.validators.clone()
            })
        );
    }

    #[test]
    fn a_price_only_upstream_edit_is_a_price_diff_and_nothing_else() {
        let before = parse(IDENTITY).expect("fixture parses");
        let repriced = IDENTITY.replace("\"input\": 5,", "\"input\": 4.25,");
        let after = parse(&repriced).expect("the repriced fixture parses");

        assert_ne!(after.source.content_id, before.source.content_id);
        let diff = after.content.diff(&before.content);
        assert!(diff.has_price_changes());
        let counts = diff.counts();
        assert_eq!(counts.prices_changed, 1);
        assert_eq!(counts.metadata_changed, 0);
        assert_eq!(counts.capabilities_changed, 0);
        assert_eq!(counts.lifecycle_changed, 0);
        assert!(matches!(
            diff.changes(),
            [CatalogChange::PriceChanged { to, .. }]
                if to.as_ref().map(|price| price.base.input)
                    == Some(ObservedRate::from_nanos(4_250_000_000))
        ));
    }

    /// A local `catalog.json` that honours `If-None-Match`, so the conditional
    /// refresh path is exercised over real HTTP rather than mocked away.
    #[derive(Clone)]
    struct Upstream {
        etag: String,
        payload: &'static str,
        transfers: Arc<AtomicUsize>,
    }

    async fn serve(State(upstream): State<Upstream>, headers: HeaderMap) -> Response {
        let matched = headers
            .get(header::IF_NONE_MATCH)
            .and_then(|value| value.to_str().ok())
            == Some(upstream.etag.as_str());
        if matched {
            return (
                StatusCode::NOT_MODIFIED,
                [(header::ETAG, upstream.etag.clone())],
            )
                .into_response();
        }
        upstream.transfers.fetch_add(1, Ordering::Relaxed);
        (
            StatusCode::OK,
            [
                (header::ETAG, upstream.etag.clone()),
                (
                    header::LAST_MODIFIED,
                    "Wed, 12 Aug 2026 20:27:54 GMT".to_owned(),
                ),
            ],
            upstream.payload,
        )
            .into_response()
    }

    /// The minimal `reqwest` fetch the test drives the source through.
    struct HttpFetch {
        client: reqwest::Client,
        limit: usize,
    }

    impl HttpFetch {
        fn new() -> Self {
            Self {
                client: reqwest::Client::new(),
                limit: MAX_PAYLOAD_BYTES,
            }
        }

        const fn holding_at_most(mut self, limit: usize) -> Self {
            self.limit = limit;
            self
        }
    }

    #[async_trait]
    impl CatalogFetch for HttpFetch {
        async fn get(
            &self,
            url: &str,
            validators: Option<&SourceValidators>,
        ) -> Result<FetchResponse, FetchError> {
            let mut request = self.client.get(url);
            if let Some(ETag(etag)) = validators.and_then(|validators| validators.etag.as_ref()) {
                request = request.header(reqwest::header::IF_NONE_MATCH, etag);
            }
            if let Some(HttpDate(date)) =
                validators.and_then(|validators| validators.last_modified.as_ref())
            {
                request = request.header(reqwest::header::IF_MODIFIED_SINCE, date);
            }
            let response = request
                .send()
                .await
                .map_err(|error| FetchError::Transport {
                    message: error.to_string(),
                })?;
            let validators = SourceValidators {
                etag: response
                    .headers()
                    .get(reqwest::header::ETAG)
                    .and_then(|value| value.to_str().ok())
                    .map(|value| ETag(value.to_owned())),
                last_modified: response
                    .headers()
                    .get(reqwest::header::LAST_MODIFIED)
                    .and_then(|value| value.to_str().ok())
                    .map(|value| HttpDate(value.to_owned())),
            };
            match response.status().as_u16() {
                304 => Ok(FetchResponse::NotModified { validators }),
                200 => Ok(FetchResponse::Payload {
                    bytes: bounded_body(response, self.limit).await?,
                    validators,
                }),
                status => Err(FetchError::Status { status }),
            }
        }
    }

    /// Serve `payload` from a local `catalog.json`, and count the transfers.
    async fn upstream(payload: &'static str) -> (ModelsDevAdapter, Arc<AtomicUsize>) {
        let transfers = Arc::new(AtomicUsize::new(0));
        let router = axum::Router::new()
            .route("/catalog.json", get(serve))
            .with_state(Upstream {
                etag: "\"identity-1\"".to_owned(),
                payload,
                transfers: Arc::clone(&transfers),
            });
        let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .expect("a free port");
        let address = listener.local_addr().expect("a bound address");
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        let adapter = ModelsDevAdapter::new(format!("http://{address}/catalog.json"))
            .expect("a catalog.json URL");
        (adapter, transfers)
    }

    #[tokio::test]
    async fn a_conditional_refresh_transfers_nothing_when_the_upstream_is_unchanged() {
        let (adapter, transfers) = upstream(IDENTITY).await;
        let source = ModelsDevSource::new(adapter, HttpFetch::new());
        let mut catalogue = LastKnownGoodCatalog::new();

        let CatalogRefresh::Updated(snapshot) = source.refresh(None).await.expect("first refresh")
        else {
            panic!("a first refresh has nothing to be conditional on");
        };
        assert_eq!(
            snapshot.source.validators.etag,
            Some(ETag("\"identity-1\"".to_owned()))
        );
        assert!(snapshot.source.validators.last_modified.is_some());
        let content_id = snapshot.source.content_id;
        catalogue.admit(*snapshot);
        assert_eq!(transfers.load(Ordering::Relaxed), 1);

        let refreshed = source
            .refresh(catalogue.validators())
            .await
            .expect("second refresh");
        assert!(matches!(refreshed, CatalogRefresh::Unchanged { .. }));
        assert_eq!(
            transfers.load(Ordering::Relaxed),
            1,
            "a 304 transfers no payload"
        );
        assert_eq!(
            catalogue
                .active()
                .map(|snapshot| snapshot.source.content_id),
            Some(content_id)
        );
    }

    #[tokio::test]
    async fn an_upstream_outage_is_retryable_and_leaves_the_catalogue_alone() {
        struct Offline;

        #[async_trait]
        impl CatalogFetch for Offline {
            async fn get(
                &self,
                _url: &str,
                _validators: Option<&SourceValidators>,
            ) -> Result<FetchResponse, FetchError> {
                Err(FetchError::Transport {
                    message: "connection refused".to_owned(),
                })
            }
        }

        let source = ModelsDevSource::new(ModelsDevAdapter::default(), Offline);
        let error = source.refresh(None).await.expect_err("an outage");
        assert!(matches!(error, CatalogError::Unavailable { .. }));
        assert_eq!(
            CatalogError::from(FetchError::Status { status: 403 }),
            CatalogError::Denied {
                backend: BACKEND,
                message: "upstream answered HTTP 403".to_owned(),
            }
        );
    }

    /// A configured mirror is not a trusted one: an endless or merely enormous
    /// body has to cost a refused refresh, not the process.
    #[tokio::test]
    async fn an_oversized_payload_is_refused_rather_than_held() {
        let ceiling = IDENTITY.len() - 1;
        let (adapter, transfers) = upstream(IDENTITY).await;
        let source = ModelsDevSource::new(adapter, HttpFetch::new().holding_at_most(ceiling));

        let error = source
            .refresh(None)
            .await
            .expect_err("an oversized payload");
        assert_eq!(
            error,
            CatalogError::Unavailable {
                backend: BACKEND,
                message: format!("payload exceeds the {ceiling}-byte ceiling"),
            }
        );
        assert_eq!(
            transfers.load(Ordering::Relaxed),
            1,
            "the body was served; the point is that it was not kept"
        );

        // And a fetch that ignores the ceiling it was given cannot make the
        // source unbounded: what it hands back is measured too.
        struct Unbounded;

        #[async_trait]
        impl CatalogFetch for Unbounded {
            async fn get(
                &self,
                _url: &str,
                _validators: Option<&SourceValidators>,
            ) -> Result<FetchResponse, FetchError> {
                Ok(FetchResponse::Payload {
                    bytes: IDENTITY.as_bytes().to_vec(),
                    validators: SourceValidators::default(),
                })
            }
        }

        let source = ModelsDevSource::new(ModelsDevAdapter::default(), Unbounded)
            .with_payload_limit(ceiling);
        assert_eq!(
            source.refresh(None).await.expect_err("too large to parse"),
            CatalogError::Unavailable {
                backend: BACKEND,
                message: format!("payload exceeds the {ceiling}-byte ceiling"),
            }
        );
    }
}
