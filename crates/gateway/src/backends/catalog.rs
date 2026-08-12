//! Model metadata ingestion: the [`CatalogSource`] contract and the normalized
//! catalogue domain a source produces.
//!
//! models.dev is the first source ([`super::models_dev`]). The contract exists to
//! keep one distinction sharp: **metadata may refresh automatically; enablement
//! and pricing are explicit administrative acts.** A refresh stores new or
//! changed catalogue metadata and nothing else — it never enables a model for a
//! tenant, never changes which alias targets exist, and never activates a price.
//! An upstream catalogue edit must not be able to become a production billing
//! change.
//!
//! So [`ObservedPrice`] is *observed* pricing, not applied pricing, and it is
//! deliberately its own type rather than [`gateway_core::ModelPrice`]: a value
//! that a request could be billed against must not be constructible by parsing
//! an upstream document. Turning an observation into an effective price is a
//! later, explicit mutation that has to convert deliberately.
//!
//! The source is [`BackendPath::Background`](super::BackendPath::Background):
//! never on the request path, never a boot dependency. In stateful mode
//! `/v1/models` and price lookups read the snapshot compiled from stored
//! metadata, so an unreachable models.dev is a stale-metadata signal with
//! metrics, not an outage.
//!
//! # Three identities, kept apart
//!
//! An imported snapshot carries three independent things, because they answer
//! three different questions and collapsing them loses one of the answers:
//!
//! | Identity | What it is | What it answers |
//! | --- | --- | --- |
//! | [`SourceSnapshot::raw`] | digest and size of the exact payload bytes | "which bytes did we accept?" — auditable, replayable |
//! | [`SourceSnapshot::content_id`] | digest of the *normalized* content's canonical bytes | "is this the catalogue we already hold?" |
//! | [`SourceSnapshot::validators`] | the upstream's `ETag` and `Last-Modified` | "may the next fetch be conditional?" |
//!
//! Only the second is content identity, and it is computed over normalized
//! content alone: retrieval time, source URL, JSON key order, and whitespace
//! cannot change it, while a validator change with identical content is not a new
//! catalogue. The validators are kept as the two fields HTTP actually defines
//! rather than as one opaque version string, because a single string cannot say
//! *which* of them changed, and a conditional request needs both
//! (`If-None-Match` and `If-Modified-Since`) independently.
//!
//! # Rejection never replaces good state
//!
//! [`LastKnownGoodCatalog`] is the seam that makes that structural: an import is
//! admitted through it, a typed parse or validation failure returns without
//! touching the active snapshot, and a successful admission reports the semantic
//! [`CatalogDiff`] against what it replaced.
//!
//! The observed-rate denomination, the three identities and their validator
//! semantics, and the bundled offline seed are recorded in
//! [ADR 0033](https://github.com/Litvue/axond/blob/main/docs/adr/0033-catalogue-source-imports.md).

use std::collections::{BTreeMap, BTreeSet};
use std::time::SystemTime;

use async_trait::async_trait;

use super::{BackendFailure, BackendKind, Capabilities, FailureCategory};
use crate::desired_state::{
    BlobKind, BlobRef, Canonical, CanonicalError, CanonicalValue, Checksum,
};

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

/// The payload shape an accepted import was parsed as.
///
/// models.dev serves several documents with different shapes at different paths;
/// only one is supported, and which one is recorded on every snapshot so a stored
/// import cannot be reinterpreted under a shape it was not parsed as.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaVersion(&'static str);

impl SchemaVersion {
    /// `https://models.dev/catalog.json`: a combined document with a
    /// provider-neutral `models` index and per-provider `providers[].models`
    /// offerings.
    pub const MODELS_DEV_CATALOG_V1: Self = Self("models.dev/catalog.json/v1");

    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

/// An HTTP entity tag, kept in the exact form the upstream sent.
///
/// Opaque: compared for equality and echoed back in `If-None-Match`, never
/// parsed, so a weak tag and a strong tag stay distinguishable.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ETag(pub String);

/// An HTTP `Last-Modified` value, kept verbatim for `If-Modified-Since`.
///
/// Not a [`SystemTime`]: reformatting an HTTP date is how a conditional request
/// stops matching, and the value's only uses are equality and echoing it back.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HttpDate(pub String);

/// What a conditional refresh may send, and what a `304` confirms.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SourceValidators {
    pub etag: Option<ETag>,
    pub last_modified: Option<HttpDate>,
}

impl SourceValidators {
    pub fn etag(etag: impl Into<String>) -> Self {
        Self {
            etag: Some(ETag(etag.into())),
            last_modified: None,
        }
    }

    /// Whether a conditional request can be made at all.
    pub fn is_empty(&self) -> bool {
        self.etag.is_none() && self.last_modified.is_none()
    }
}

/// The identity of normalized catalogue content: the SHA-256 of its canonical
/// bytes.
///
/// Two imports of the same catalogue are the same id whatever the payload's key
/// order, whitespace, retrieval time, or validators were.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CatalogContentId(Checksum);

impl CatalogContentId {
    pub const fn checksum(self) -> Checksum {
        self.0
    }
}

impl std::fmt::Display for CatalogContentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Where one accepted import came from, and what it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSnapshot {
    /// The URL the payload was read from.
    pub source_url: String,
    pub schema_version: SchemaVersion,
    /// The upstream's refresh metadata, for the next conditional request.
    pub validators: SourceValidators,
    pub fetched_at: SystemTime,
    /// The raw payload's content address: digest and byte length of exactly what
    /// was accepted.
    pub raw: BlobRef,
    /// The normalized content's identity.
    pub content_id: CatalogContentId,
}

/// A validated, normalized catalogue read from a source at one instant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogSnapshot {
    pub source: SourceSnapshot,
    pub content: CatalogContent,
}

/// The outcome of a refresh.
///
/// `Unchanged` is a first-class answer rather than an empty snapshot, so a
/// caller can tell "the upstream has nothing new" from "the upstream now lists
/// no models" — the second would silently retire every model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogRefresh {
    Unchanged {
        validators: SourceValidators,
    },
    /// Boxed because a snapshot is a whole catalogue and `Unchanged` — the
    /// common answer — is two optional header values.
    Updated(Box<CatalogSnapshot>),
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
    /// upstream's validators.
    ///
    /// A failure is a background-refresh failure: it must leave previously
    /// stored metadata in place, because stale metadata serves requests fine and
    /// an empty catalogue does not.
    async fn refresh(
        &self,
        since: Option<&SourceValidators>,
    ) -> Result<CatalogRefresh, CatalogError>;
}

/// A provider or model identifier, exactly as the source publishes it.
///
/// Case-sensitive and never rewritten: upstream ids are the strings a provider's
/// API expects (`MiniMax-M1`, `Qwen/Qwen3-32B`), so lowercasing one would produce
/// a model id no provider answers to, and folding case would merge two distinct
/// published models into one entry. Validation is therefore about what an id may
/// contain, not about which spelling of it wins.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CatalogId(String);

/// Why an identifier was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidCatalogId {
    #[error("a catalogue identifier may not be empty")]
    Empty,
    #[error("catalogue identifier `{value}` is longer than {max} bytes")]
    TooLong { value: String, max: usize },
    #[error(
        "catalogue identifier `{value}` contains `{character}`; \
         only ASCII alphanumerics and `-._:/+@~` are accepted"
    )]
    Character { value: String, character: char },
    #[error("catalogue identifier `{value}` has an empty path segment")]
    Segment { value: String },
}

impl CatalogId {
    const MAX_BYTES: usize = 128;

    pub fn parse(value: &str) -> Result<Self, InvalidCatalogId> {
        if value.is_empty() {
            return Err(InvalidCatalogId::Empty);
        }
        if value.len() > Self::MAX_BYTES {
            return Err(InvalidCatalogId::TooLong {
                value: value.to_owned(),
                max: Self::MAX_BYTES,
            });
        }
        for character in value.chars() {
            let permitted = character.is_ascii_alphanumeric()
                || matches!(character, '-' | '.' | '_' | ':' | '/' | '+' | '@' | '~');
            if !permitted {
                return Err(InvalidCatalogId::Character {
                    value: value.to_owned(),
                    character,
                });
            }
        }
        if value.split('/').any(str::is_empty) {
            return Err(InvalidCatalogId::Segment {
                value: value.to_owned(),
            });
        }
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for CatalogId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Canonical for CatalogId {
    fn canonical(&self) -> CanonicalValue {
        CanonicalValue::string(&self.0)
    }
}

/// A provider id, in the source's vocabulary (`openai`, `bedrock`).
pub type ProviderId = CatalogId;
/// A model id, in the source's vocabulary (`gpt-4o`, `anthropic/claude-sonnet-4`).
pub type ModelId = CatalogId;

/// An input or output modality a model accepts or emits.
///
/// A closed set: an unrecognized modality is schema drift the adapter refuses,
/// because "some modality we do not model" must not silently read as "text".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Modality {
    Text,
    Image,
    Audio,
    Video,
    Pdf,
}

impl Modality {
    pub const ALL: &'static [Self] =
        &[Self::Text, Self::Image, Self::Audio, Self::Video, Self::Pdf];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Image => "image",
            Self::Audio => "audio",
            Self::Video => "video",
            Self::Pdf => "pdf",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|modality| modality.as_str() == value)
    }
}

/// A capability the source asserts a model has.
///
/// Presence is the assertion: a capability the source does not state is absent
/// from the set, so "not stated" and "stated false" are the same thing to every
/// consumer, and a capability appearing or disappearing is one diff.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ModelCapability {
    /// Accepts file attachments.
    Attachment,
    /// Exposes reasoning/thinking.
    Reasoning,
    /// Supports tool calls.
    ToolCall,
    /// Accepts a temperature.
    Temperature,
    /// Supports structured/JSON-schema output.
    StructuredOutput,
    /// Supports interleaved reasoning and tool calls.
    Interleaved,
    /// Weights are published.
    OpenWeights,
    /// The offering is marked experimental.
    Experimental,
}

impl ModelCapability {
    pub const ALL: &'static [Self] = &[
        Self::Attachment,
        Self::Reasoning,
        Self::ToolCall,
        Self::Temperature,
        Self::StructuredOutput,
        Self::Interleaved,
        Self::OpenWeights,
        Self::Experimental,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Attachment => "attachment",
            Self::Reasoning => "reasoning",
            Self::ToolCall => "tool-call",
            Self::Temperature => "temperature",
            Self::StructuredOutput => "structured-output",
            Self::Interleaved => "interleaved",
            Self::OpenWeights => "open-weights",
            Self::Experimental => "experimental",
        }
    }
}

/// Where a model sits in its published lifecycle.
///
/// A closed set for the same reason as [`Modality`]: lifecycle drives what an
/// operator is warned about, so an unrecognized status is refused rather than
/// flattened into "available".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum ModelLifecycle {
    #[default]
    Available,
    Alpha,
    Beta,
    Deprecated,
}

impl ModelLifecycle {
    pub const ALL: &'static [Self] = &[Self::Available, Self::Alpha, Self::Beta, Self::Deprecated];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Alpha => "alpha",
            Self::Beta => "beta",
            Self::Deprecated => "deprecated",
        }
    }

    /// Whether the source marks the model as retiring or retired.
    pub const fn deprecated(self) -> bool {
        matches!(self, Self::Deprecated)
    }
}

/// Published token limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ModelLimits {
    pub context_tokens: Option<u64>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}

impl Canonical for ModelLimits {
    fn canonical(&self) -> CanonicalValue {
        let mut fields = Vec::new();
        for (key, value) in [
            ("context_tokens", self.context_tokens),
            ("input_tokens", self.input_tokens),
            ("output_tokens", self.output_tokens),
        ] {
            if let Some(value) = value {
                fields.push((key.to_owned(), CanonicalValue::integer(value)));
            }
        }
        CanonicalValue::Map(fields)
    }
}

/// A published rate, in nano-dollars per million tokens.
///
/// Integer, like every other price in the gateway (ADR 0010): a rate that entered
/// the system as a float could not be checksummed, and rounding it at import
/// would make the observation disagree with the published one.
///
/// Nano-dollars rather than the gateway's micro-dollars because this is what a
/// source *published*, not what the gateway charges: sources state rates as fine
/// as `$0.26666667` per million tokens, which micro-dollars cannot hold, and an
/// observation rounded once at import can never be rounded correctly again.
/// Turning an observation into a billable price is price activation, a later
/// slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObservedRate(u64);

impl ObservedRate {
    pub const ZERO: Self = Self(0);

    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    pub const fn nanos(self) -> u64 {
        self.0
    }
}

impl Canonical for ObservedRate {
    fn canonical(&self) -> CanonicalValue {
        CanonicalValue::integer(self.0)
    }
}

/// One set of published rates: the base rates, or a tier's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriceRates {
    pub input: ObservedRate,
    pub output: ObservedRate,
    pub cache_read: Option<ObservedRate>,
    pub cache_write: Option<ObservedRate>,
    pub reasoning: Option<ObservedRate>,
    pub input_audio: Option<ObservedRate>,
    pub output_audio: Option<ObservedRate>,
}

impl PriceRates {
    /// The two rates every published price states.
    pub const fn new(input: ObservedRate, output: ObservedRate) -> Self {
        Self {
            input,
            output,
            cache_read: None,
            cache_write: None,
            reasoning: None,
            input_audio: None,
            output_audio: None,
        }
    }
}

impl Canonical for PriceRates {
    fn canonical(&self) -> CanonicalValue {
        let mut fields = vec![
            ("input".to_owned(), self.input.canonical()),
            ("output".to_owned(), self.output.canonical()),
        ];
        for (key, rate) in [
            ("cache_read", self.cache_read),
            ("cache_write", self.cache_write),
            ("reasoning", self.reasoning),
            ("input_audio", self.input_audio),
            ("output_audio", self.output_audio),
        ] {
            if let Some(rate) = rate {
                fields.push((key.to_owned(), rate.canonical()));
            }
        }
        CanonicalValue::Map(fields)
    }
}

/// What makes a tier apply.
///
/// One variant today, named rather than left as a bare token count, so a second
/// kind of tier is a new variant instead of a reinterpretation of this one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PriceTierThreshold {
    /// Applies once a request's context exceeds this many tokens.
    ContextOver { tokens: u64 },
}

impl Canonical for PriceTierThreshold {
    fn canonical(&self) -> CanonicalValue {
        match self {
            Self::ContextOver { tokens } => CanonicalValue::map([
                ("type", CanonicalValue::string("context-over")),
                ("tokens", CanonicalValue::integer(*tokens)),
            ]),
        }
    }
}

/// A conditional rate schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriceTier {
    pub threshold: PriceTierThreshold,
    pub rates: PriceRates,
}

impl Canonical for PriceTier {
    fn canonical(&self) -> CanonicalValue {
        CanonicalValue::map([
            ("threshold", self.threshold.canonical()),
            ("rates", self.rates.canonical()),
        ])
    }
}

/// Pricing as *published upstream*, for one provider's offering of a model.
///
/// Deliberately not [`gateway_core::ModelPrice`]: that type is what a request is
/// billed against, and nothing parsed out of an upstream document may be one
/// without an explicit administrative act. Recording an observation never
/// activates it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedPrice {
    pub base: PriceRates,
    /// Tiers, ordered by threshold. Never two tiers for one threshold.
    pub tiers: Vec<PriceTier>,
}

impl ObservedPrice {
    pub fn new(base: PriceRates) -> Self {
        Self {
            base,
            tiers: Vec::new(),
        }
    }
}

impl Canonical for ObservedPrice {
    fn canonical(&self) -> CanonicalValue {
        CanonicalValue::map([
            ("base", self.base.canonical()),
            (
                "tiers",
                CanonicalValue::List(self.tiers.iter().map(Canonical::canonical).collect()),
            ),
        ])
    }
}

/// One metadata field, so an override or a change can name what differs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ModelField {
    DisplayName,
    Family,
    Capabilities,
    InputModalities,
    OutputModalities,
    ContextTokens,
    InputTokens,
    OutputTokens,
    Lifecycle,
    KnowledgeCutoff,
    ReleaseDate,
    LastUpdated,
    /// The offering's own endpoint metadata, when a provider states one.
    Endpoint,
    /// The model id this provider publishes, which is the id a request to it
    /// must use. Only ever a change, never an override: it has no neutral
    /// counterpart to contradict.
    PublishedModelId,
}

impl ModelField {
    pub const ALL: &'static [Self] = &[
        Self::DisplayName,
        Self::Family,
        Self::Capabilities,
        Self::InputModalities,
        Self::OutputModalities,
        Self::ContextTokens,
        Self::InputTokens,
        Self::OutputTokens,
        Self::Lifecycle,
        Self::KnowledgeCutoff,
        Self::ReleaseDate,
        Self::LastUpdated,
        Self::Endpoint,
        Self::PublishedModelId,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DisplayName => "display_name",
            Self::Family => "family",
            Self::Capabilities => "capabilities",
            Self::InputModalities => "input_modalities",
            Self::OutputModalities => "output_modalities",
            Self::ContextTokens => "context_tokens",
            Self::InputTokens => "input_tokens",
            Self::OutputTokens => "output_tokens",
            Self::Lifecycle => "lifecycle",
            Self::KnowledgeCutoff => "knowledge_cutoff",
            Self::ReleaseDate => "release_date",
            Self::LastUpdated => "last_updated",
            Self::Endpoint => "endpoint",
            Self::PublishedModelId => "published_model_id",
        }
    }

    /// Whether a change to this field is a lifecycle change rather than plain
    /// metadata. Kept here so the diff's classification is one function.
    const fn lifecycle(self) -> bool {
        matches!(self, Self::Lifecycle)
    }

    const fn capability(self) -> bool {
        matches!(
            self,
            Self::Capabilities | Self::InputModalities | Self::OutputModalities
        )
    }
}

/// A JSON Pointer (RFC 6901) into the payload a value was read from.
///
/// Provenance is a location in the accepted bytes rather than prose, so an
/// operator asking "why does this offering say 200k context?" can be answered by
/// pointing at the raw snapshot the digest in [`SourceSnapshot::raw`] names.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JsonPointer(String);

impl JsonPointer {
    pub fn new(pointer: impl Into<String>) -> Self {
        Self(pointer.into())
    }

    /// A child pointer, escaping `~` and `/` as RFC 6901 requires.
    pub fn child(&self, token: &str) -> Self {
        let escaped = token.replace('~', "~0").replace('/', "~1");
        Self(format!("{}/{escaped}", self.0))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for JsonPointer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Canonical for JsonPointer {
    fn canonical(&self) -> CanonicalValue {
        CanonicalValue::string(&self.0)
    }
}

/// Provider-neutral metadata about a model, or one provider's statement of it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ModelFacts {
    pub display_name: Option<String>,
    pub family: Option<String>,
    pub capabilities: BTreeSet<ModelCapability>,
    pub input_modalities: BTreeSet<Modality>,
    pub output_modalities: BTreeSet<Modality>,
    pub limits: ModelLimits,
    pub lifecycle: ModelLifecycle,
    pub knowledge_cutoff: Option<String>,
    pub release_date: Option<String>,
    pub last_updated: Option<String>,
}

impl ModelFacts {
    /// The fields on which two statements of the same model differ.
    ///
    /// This is the one comparison behind both override detection (offering
    /// against the neutral record) and diff classification (an offering against
    /// its previous self), so the two can never disagree about what "changed"
    /// means.
    pub fn differences(&self, other: &Self) -> Vec<ModelField> {
        let mut fields = Vec::new();
        let mut differs = |condition: bool, field: ModelField| {
            if condition {
                fields.push(field);
            }
        };
        differs(
            self.display_name != other.display_name,
            ModelField::DisplayName,
        );
        differs(self.family != other.family, ModelField::Family);
        differs(
            self.capabilities != other.capabilities,
            ModelField::Capabilities,
        );
        differs(
            self.input_modalities != other.input_modalities,
            ModelField::InputModalities,
        );
        differs(
            self.output_modalities != other.output_modalities,
            ModelField::OutputModalities,
        );
        differs(
            self.limits.context_tokens != other.limits.context_tokens,
            ModelField::ContextTokens,
        );
        differs(
            self.limits.input_tokens != other.limits.input_tokens,
            ModelField::InputTokens,
        );
        differs(
            self.limits.output_tokens != other.limits.output_tokens,
            ModelField::OutputTokens,
        );
        differs(self.lifecycle != other.lifecycle, ModelField::Lifecycle);
        differs(
            self.knowledge_cutoff != other.knowledge_cutoff,
            ModelField::KnowledgeCutoff,
        );
        differs(
            self.release_date != other.release_date,
            ModelField::ReleaseDate,
        );
        differs(
            self.last_updated != other.last_updated,
            ModelField::LastUpdated,
        );
        fields
    }
}

impl Canonical for ModelFacts {
    fn canonical(&self) -> CanonicalValue {
        let mut fields = vec![
            (
                "capabilities".to_owned(),
                CanonicalValue::set(
                    self.capabilities
                        .iter()
                        .map(|capability| CanonicalValue::string(capability.as_str())),
                ),
            ),
            (
                "input_modalities".to_owned(),
                CanonicalValue::set(
                    self.input_modalities
                        .iter()
                        .map(|modality| CanonicalValue::string(modality.as_str())),
                ),
            ),
            (
                "output_modalities".to_owned(),
                CanonicalValue::set(
                    self.output_modalities
                        .iter()
                        .map(|modality| CanonicalValue::string(modality.as_str())),
                ),
            ),
            ("limits".to_owned(), self.limits.canonical()),
            (
                "lifecycle".to_owned(),
                CanonicalValue::string(self.lifecycle.as_str()),
            ),
        ];
        for (key, value) in [
            ("display_name", self.display_name.as_deref()),
            ("family", self.family.as_deref()),
            ("knowledge_cutoff", self.knowledge_cutoff.as_deref()),
            ("release_date", self.release_date.as_deref()),
            ("last_updated", self.last_updated.as_deref()),
        ] {
            if let Some(value) = value {
                fields.push((key.to_owned(), CanonicalValue::string(value)));
            }
        }
        CanonicalValue::Map(fields)
    }
}

/// How a provider's API is reached, as the source describes it.
///
/// Metadata only: no credential, and nothing here is resolved or contacted by
/// this slice.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProviderEndpoint {
    /// The API base, which may contain `${VAR}` placeholders the source leaves
    /// for an operator to fill.
    pub api_base: Option<String>,
    /// The upstream's client-package hint (`npm`).
    pub client_package: Option<String>,
    /// The wire shape the provider speaks, when the source states one.
    pub wire_shape: Option<String>,
}

impl ProviderEndpoint {
    pub fn is_empty(&self) -> bool {
        self.api_base.is_none() && self.client_package.is_none() && self.wire_shape.is_none()
    }
}

impl Canonical for ProviderEndpoint {
    fn canonical(&self) -> CanonicalValue {
        let mut fields = Vec::new();
        for (key, value) in [
            ("api_base", self.api_base.as_deref()),
            ("client_package", self.client_package.as_deref()),
            ("wire_shape", self.wire_shape.as_deref()),
        ] {
            if let Some(value) = value {
                fields.push((key.to_owned(), CanonicalValue::string(value)));
            }
        }
        CanonicalValue::Map(fields)
    }
}

/// One field of a provider record, so a change can name what differs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProviderField {
    DisplayName,
    DocUrl,
    Endpoint,
    /// The names of the environment variables the source says hold this
    /// provider's credentials, in the order it published them.
    EnvVars,
}

impl ProviderField {
    pub const ALL: &'static [Self] = &[
        Self::DisplayName,
        Self::DocUrl,
        Self::Endpoint,
        Self::EnvVars,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DisplayName => "display_name",
            Self::DocUrl => "doc_url",
            Self::Endpoint => "endpoint",
            Self::EnvVars => "env_vars",
        }
    }
}

/// A provider as the source describes it, without its models.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogProvider {
    pub id: ProviderId,
    pub display_name: Option<String>,
    pub doc_url: Option<String>,
    pub endpoint: ProviderEndpoint,
    /// The environment variables the source says hold this provider's
    /// credentials. Names only — this slice never reads them.
    ///
    /// A list rather than a set, and ordered as published, because upstream uses
    /// the order: `google` lists three interchangeable keys
    /// (`GOOGLE_API_KEY`, `GOOGLE_GENERATIVE_AI_API_KEY`, `GEMINI_API_KEY`) in
    /// the order a client should prefer them, while `amazon-bedrock` lists four
    /// variables that are read together. Credential discovery is a later slice
    /// and will need that order, so a reordered `env` is an upstream edit this
    /// records rather than noise it hides.
    pub env_vars: Vec<String>,
    pub pointer: JsonPointer,
}

impl CatalogProvider {
    /// The fields on which two descriptions of the same provider differ.
    ///
    /// The same role [`ModelFacts::differences`] plays for models: one
    /// comparison, so what the identity covers and what the diff reports cannot
    /// drift apart.
    pub fn differences(&self, other: &Self) -> Vec<ProviderField> {
        let mut fields = Vec::new();
        for (differs, field) in [
            (
                self.display_name != other.display_name,
                ProviderField::DisplayName,
            ),
            (self.doc_url != other.doc_url, ProviderField::DocUrl),
            (self.endpoint != other.endpoint, ProviderField::Endpoint),
            (self.env_vars != other.env_vars, ProviderField::EnvVars),
        ] {
            if differs {
                fields.push(field);
            }
        }
        fields
    }
}

impl Canonical for CatalogProvider {
    fn canonical(&self) -> CanonicalValue {
        let mut fields = vec![
            ("id".to_owned(), self.id.canonical()),
            ("endpoint".to_owned(), self.endpoint.canonical()),
            (
                "env_vars".to_owned(),
                CanonicalValue::List(
                    self.env_vars
                        .iter()
                        .map(CanonicalValue::string)
                        .collect::<Vec<_>>(),
                ),
            ),
        ];
        for (key, value) in [
            ("display_name", self.display_name.as_deref()),
            ("doc_url", self.doc_url.as_deref()),
        ] {
            if let Some(value) = value {
                fields.push((key.to_owned(), CanonicalValue::string(value)));
            }
        }
        CanonicalValue::Map(fields)
    }
}

/// One provider's offering of one model.
///
/// The offering's [`ProviderOffering::facts`] are what the *provider* states, so
/// provider values take precedence by construction: a consumer reads the
/// offering, and the neutral record on [`CatalogModelEntry`] is the fallback for
/// what the provider leaves unsaid. [`ProviderOffering::overrides`] records which
/// fields the provider contradicted, with a pointer into the payload, so an
/// override is auditable rather than inferred by re-comparing later.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderOffering {
    pub provider: ProviderId,
    pub model: ModelId,
    /// The model id exactly as this provider publishes it, which is what a
    /// request to that provider must use.
    pub published_model_id: String,
    pub facts: ModelFacts,
    /// Fields where this provider contradicts the neutral record, sorted, each
    /// with the pointer to the provider's own value.
    pub overrides: Vec<(ModelField, JsonPointer)>,
    /// Observed price. Recording it never activates it.
    pub price: Option<ObservedPrice>,
    pub endpoint: ProviderEndpoint,
    pub pointer: JsonPointer,
}

impl ProviderOffering {
    /// Whether this provider contradicts the neutral record anywhere.
    pub fn has_overrides(&self) -> bool {
        !self.overrides.is_empty()
    }

    pub fn overrides_field(&self, field: ModelField) -> bool {
        self.overrides.iter().any(|(name, _)| *name == field)
    }
}

impl Canonical for ProviderOffering {
    fn canonical(&self) -> CanonicalValue {
        let mut fields = vec![
            ("provider".to_owned(), self.provider.canonical()),
            ("model".to_owned(), self.model.canonical()),
            (
                "published_model_id".to_owned(),
                CanonicalValue::string(&self.published_model_id),
            ),
            ("facts".to_owned(), self.facts.canonical()),
            (
                "overrides".to_owned(),
                CanonicalValue::List(
                    self.overrides
                        .iter()
                        .map(|(field, pointer)| {
                            CanonicalValue::map([
                                ("field", CanonicalValue::string(field.as_str())),
                                ("pointer", pointer.canonical()),
                            ])
                        })
                        .collect(),
                ),
            ),
            ("pointer".to_owned(), self.pointer.canonical()),
        ];
        if let Some(price) = &self.price {
            fields.push(("price".to_owned(), price.canonical()));
        }
        if !self.endpoint.is_empty() {
            fields.push(("endpoint".to_owned(), self.endpoint.canonical()));
        }
        CanonicalValue::Map(fields)
    }
}

/// One model: the provider-neutral record, when the source publishes one, and
/// every provider offering of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogModelEntry {
    pub id: ModelId,
    /// The source's provider-neutral description. `None` when the source
    /// publishes offerings for a model without a neutral record — common, and not
    /// an error: the offerings are still usable metadata.
    pub neutral: Option<ModelFacts>,
    /// Offerings, ordered by provider and then by the id that provider
    /// publishes.
    ///
    /// A provider may offer one model under more than one callable id — the
    /// upstream's `qiniu-ai` publishes both `mimo-v2-flash` and
    /// `xiaomi/mimo-v2-flash` — and each is a separate offering because each is
    /// separately requestable. What identifies an offering within a model is
    /// therefore `(provider, published_model_id)`, not the provider alone.
    pub offerings: Vec<ProviderOffering>,
}

impl CatalogModelEntry {
    /// This provider's offering of the model, or its first by published id when
    /// the provider publishes the model under several.
    pub fn offering(&self, provider: &ProviderId) -> Option<&ProviderOffering> {
        self.offerings
            .iter()
            .find(|offering| &offering.provider == provider)
    }

    /// Every offering this provider publishes of the model, ordered by the id it
    /// publishes them under.
    pub fn offerings_by(&self, provider: &ProviderId) -> impl Iterator<Item = &ProviderOffering> {
        self.offerings
            .iter()
            .filter(move |offering| &offering.provider == provider)
    }

    /// The offering a request naming `published` would reach.
    pub fn offering_published_as(
        &self,
        provider: &ProviderId,
        published: &str,
    ) -> Option<&ProviderOffering> {
        self.offerings.iter().find(|offering| {
            &offering.provider == provider && offering.published_model_id == published
        })
    }
}

impl Canonical for CatalogModelEntry {
    fn canonical(&self) -> CanonicalValue {
        let mut fields = vec![
            ("id".to_owned(), self.id.canonical()),
            (
                "offerings".to_owned(),
                CanonicalValue::List(self.offerings.iter().map(Canonical::canonical).collect()),
            ),
        ];
        if let Some(neutral) = &self.neutral {
            fields.push(("neutral".to_owned(), neutral.canonical()));
        }
        CanonicalValue::Map(fields)
    }
}

/// Why normalized content was not accepted.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CatalogContentError {
    #[error("provider `{provider}` appears twice")]
    DuplicateProvider { provider: ProviderId },
    #[error("model `{model}` appears twice")]
    DuplicateModel { model: ModelId },
    #[error("model `{model}` lists `{provider}`'s `{published}` twice")]
    DuplicateOffering {
        model: ModelId,
        provider: ProviderId,
        published: String,
    },
    #[error("model `{model}` is offered by `{provider}`, which the payload does not describe")]
    UnknownProvider {
        model: ModelId,
        provider: ProviderId,
    },
    #[error("offering `{provider}`/`{published}` is filed under model `{model}`")]
    OfferingModelMismatch {
        model: ModelId,
        provider: ProviderId,
        published: String,
    },
    #[error("the payload describes no models")]
    Empty,
    /// Text a canonical form cannot hold, so the content has no identity.
    ///
    /// A rejection rather than a panic: upstream free text is upstream's to
    /// choose, and a stray control character must cost an import, not the task
    /// running it.
    #[error("the catalogue has no canonical form: {source}")]
    Uncanonicalizable {
        #[source]
        source: CanonicalError,
    },
}

/// A normalized catalogue: providers, and models with their offerings.
///
/// Construction sorts and validates, so a `CatalogContent` value in hand is
/// already deterministic — every collection is in one order, whatever order the
/// payload used — and internally consistent. That is what makes
/// [`CatalogContent::content_id`] an identity rather than a hash of one parser's
/// traversal order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogContent {
    providers: Vec<CatalogProvider>,
    models: Vec<CatalogModelEntry>,
    /// Computed once, at construction, because that is the only place the
    /// canonical form can fail: a value in hand therefore has an identity, and
    /// asking for it cannot fail.
    content_id: CatalogContentId,
}

impl CatalogContent {
    pub fn new(
        providers: Vec<CatalogProvider>,
        models: Vec<CatalogModelEntry>,
    ) -> Result<Self, CatalogContentError> {
        if models.is_empty() {
            return Err(CatalogContentError::Empty);
        }
        let mut providers = providers;
        providers.sort_by(|left, right| left.id.cmp(&right.id));
        if let Some(duplicate) = first_duplicate(providers.iter().map(|provider| &provider.id)) {
            return Err(CatalogContentError::DuplicateProvider {
                provider: duplicate.clone(),
            });
        }
        let known: BTreeSet<&ProviderId> = providers.iter().map(|provider| &provider.id).collect();

        let mut models = models;
        models.sort_by(|left, right| left.id.cmp(&right.id));
        if let Some(duplicate) = first_duplicate(models.iter().map(|model| &model.id)) {
            return Err(CatalogContentError::DuplicateModel {
                model: duplicate.clone(),
            });
        }
        for model in &mut models {
            model.offerings.sort_by(|left, right| {
                left.provider
                    .cmp(&right.provider)
                    .then_with(|| left.published_model_id.cmp(&right.published_model_id))
            });
            if let Some(duplicate) = model.offerings.windows(2).find(|pair| {
                pair[0].provider == pair[1].provider
                    && pair[0].published_model_id == pair[1].published_model_id
            }) {
                return Err(CatalogContentError::DuplicateOffering {
                    model: model.id.clone(),
                    provider: duplicate[0].provider.clone(),
                    published: duplicate[0].published_model_id.clone(),
                });
            }
            for offering in &model.offerings {
                if !known.contains(&offering.provider) {
                    return Err(CatalogContentError::UnknownProvider {
                        model: model.id.clone(),
                        provider: offering.provider.clone(),
                    });
                }
                if offering.model != model.id {
                    return Err(CatalogContentError::OfferingModelMismatch {
                        model: model.id.clone(),
                        provider: offering.provider.clone(),
                        published: offering.published_model_id.clone(),
                    });
                }
            }
        }
        let content_id = CatalogContentId(
            canonical_content(&providers, &models)
                .checksum()
                .map_err(|source| CatalogContentError::Uncanonicalizable { source })?,
        );
        Ok(Self {
            providers,
            models,
            content_id,
        })
    }

    pub fn providers(&self) -> &[CatalogProvider] {
        &self.providers
    }

    pub fn models(&self) -> &[CatalogModelEntry] {
        &self.models
    }

    pub fn provider(&self, id: &ProviderId) -> Option<&CatalogProvider> {
        self.providers.iter().find(|provider| &provider.id == id)
    }

    pub fn model(&self, id: &ModelId) -> Option<&CatalogModelEntry> {
        self.models.iter().find(|model| &model.id == id)
    }

    pub fn offering(&self, model: &ModelId, provider: &ProviderId) -> Option<&ProviderOffering> {
        self.model(model)?.offering(provider)
    }

    /// The number of offerings across every model.
    pub fn offering_count(&self) -> usize {
        self.models.iter().map(|model| model.offerings.len()).sum()
    }

    /// The identity of this content.
    ///
    /// Over the content only: no URL, no fetch time, no validators, so identical
    /// catalogue data imported twice is one identity.
    pub const fn content_id(&self) -> CatalogContentId {
        self.content_id
    }

    /// The semantic change from `previous` to `self`.
    pub fn diff(&self, previous: &Self) -> CatalogDiff {
        CatalogDiff::between(previous, self)
    }
}

impl Canonical for CatalogContent {
    fn canonical(&self) -> CanonicalValue {
        canonical_content(&self.providers, &self.models)
    }
}

/// The canonical form of a catalogue's parts.
///
/// A free function so [`CatalogContent::new`] can canonicalize before it has a
/// `CatalogContent` to ask.
fn canonical_content(
    providers: &[CatalogProvider],
    models: &[CatalogModelEntry],
) -> CanonicalValue {
    CanonicalValue::map([
        (
            "providers",
            CanonicalValue::List(providers.iter().map(Canonical::canonical).collect()),
        ),
        (
            "models",
            CanonicalValue::List(models.iter().map(Canonical::canonical).collect()),
        ),
    ])
}

fn first_duplicate<'a, T: PartialEq>(values: impl Iterator<Item = &'a T> + 'a) -> Option<&'a T> {
    let mut previous: Option<&T> = None;
    for value in values {
        if previous == Some(value) {
            return Some(value);
        }
        previous = Some(value);
    }
    None
}

/// One semantic change between two catalogues.
///
/// Price changes are their own variant, never folded into metadata: an operator
/// reviewing a refresh needs "what got more expensive" answerable without
/// re-diffing, and a price change is the one class of change that must never be
/// applied implicitly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogChange {
    ProviderAdded {
        provider: ProviderId,
    },
    ProviderRemoved {
        provider: ProviderId,
    },
    ProviderChanged {
        provider: ProviderId,
        fields: Vec<ProviderField>,
    },
    ModelAdded {
        model: ModelId,
    },
    ModelRemoved {
        model: ModelId,
    },
    OfferingAdded {
        model: ModelId,
        provider: ProviderId,
    },
    OfferingRemoved {
        model: ModelId,
        provider: ProviderId,
    },
    /// The provider-neutral record changed, gained values, or lost them.
    ///
    /// Reported separately from the offerings: neutral metadata is what an
    /// offering's overrides are measured against, so an operator seeing an
    /// override appear needs to know whether the provider moved or the neutral
    /// record did.
    NeutralChanged {
        model: ModelId,
        fields: Vec<ModelField>,
    },
    NeutralDescribed {
        model: ModelId,
    },
    NeutralDropped {
        model: ModelId,
    },
    LifecycleChanged {
        model: ModelId,
        provider: ProviderId,
        from: ModelLifecycle,
        to: ModelLifecycle,
    },
    CapabilitiesChanged {
        model: ModelId,
        provider: ProviderId,
        fields: Vec<ModelField>,
    },
    MetadataChanged {
        model: ModelId,
        provider: ProviderId,
        fields: Vec<ModelField>,
    },
    /// Boxed because an observed price carries every published rate and tier,
    /// and a diff is mostly changes that carry two ids.
    PriceChanged {
        model: ModelId,
        provider: ProviderId,
        from: Option<Box<ObservedPrice>>,
        to: Option<Box<ObservedPrice>>,
    },
}

impl CatalogChange {
    /// The model a change is about, or `None` for a provider-record change.
    pub fn model(&self) -> Option<&ModelId> {
        match self {
            Self::ProviderAdded { .. }
            | Self::ProviderRemoved { .. }
            | Self::ProviderChanged { .. } => None,
            Self::ModelAdded { model }
            | Self::ModelRemoved { model }
            | Self::NeutralChanged { model, .. }
            | Self::NeutralDescribed { model }
            | Self::NeutralDropped { model }
            | Self::OfferingAdded { model, .. }
            | Self::OfferingRemoved { model, .. }
            | Self::LifecycleChanged { model, .. }
            | Self::CapabilitiesChanged { model, .. }
            | Self::MetadataChanged { model, .. }
            | Self::PriceChanged { model, .. } => Some(model),
        }
    }

    pub fn provider(&self) -> Option<&ProviderId> {
        match self {
            Self::ModelAdded { .. }
            | Self::ModelRemoved { .. }
            | Self::NeutralChanged { .. }
            | Self::NeutralDescribed { .. }
            | Self::NeutralDropped { .. } => None,
            Self::ProviderAdded { provider }
            | Self::ProviderRemoved { provider }
            | Self::ProviderChanged { provider, .. }
            | Self::OfferingAdded { provider, .. }
            | Self::OfferingRemoved { provider, .. }
            | Self::LifecycleChanged { provider, .. }
            | Self::CapabilitiesChanged { provider, .. }
            | Self::MetadataChanged { provider, .. }
            | Self::PriceChanged { provider, .. } => Some(provider),
        }
    }

    /// The variant's ordering rank, so a diff's order is a property of the
    /// change kinds rather than of the traversal that produced them.
    const fn rank(&self) -> u8 {
        match self {
            Self::ProviderAdded { .. } => 0,
            Self::ProviderRemoved { .. } => 1,
            Self::ProviderChanged { .. } => 2,
            Self::ModelAdded { .. } => 3,
            Self::ModelRemoved { .. } => 4,
            Self::NeutralDescribed { .. } => 5,
            Self::NeutralDropped { .. } => 6,
            Self::NeutralChanged { .. } => 7,
            Self::OfferingAdded { .. } => 8,
            Self::OfferingRemoved { .. } => 9,
            Self::LifecycleChanged { .. } => 10,
            Self::CapabilitiesChanged { .. } => 11,
            Self::MetadataChanged { .. } => 12,
            Self::PriceChanged { .. } => 13,
        }
    }
}

/// How many changes of each class a diff holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CatalogDiffCounts {
    pub providers_added: usize,
    pub providers_removed: usize,
    pub providers_changed: usize,
    pub models_added: usize,
    pub models_removed: usize,
    pub offerings_added: usize,
    pub offerings_removed: usize,
    pub neutral_changed: usize,
    pub lifecycle_changed: usize,
    pub capabilities_changed: usize,
    pub metadata_changed: usize,
    pub prices_changed: usize,
}

/// The semantic difference between two catalogues.
///
/// Offerings are compared per `(model, provider)` over what the *provider*
/// states; provider records and neutral records are compared separately and
/// reported as their own classes, so an offering is never reported as changed
/// because something around it moved.
///
/// Everything the content identity covers is compared, which is the property
/// that matters: an [`Admission::Updated`] can never carry an empty diff, so
/// "the catalogue changed" is always answerable with what changed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CatalogDiff {
    changes: Vec<CatalogChange>,
}

impl CatalogDiff {
    fn between(previous: &CatalogContent, current: &CatalogContent) -> Self {
        let mut changes = Vec::new();
        let previous_providers: BTreeMap<&ProviderId, &CatalogProvider> = previous
            .providers()
            .iter()
            .map(|provider| (&provider.id, provider))
            .collect();
        for provider in current.providers() {
            match previous_providers.get(&provider.id) {
                None => changes.push(CatalogChange::ProviderAdded {
                    provider: provider.id.clone(),
                }),
                Some(before) => {
                    let fields = provider.differences(before);
                    if !fields.is_empty() {
                        changes.push(CatalogChange::ProviderChanged {
                            provider: provider.id.clone(),
                            fields,
                        });
                    }
                }
            }
        }
        for provider in previous.providers() {
            if current.provider(&provider.id).is_none() {
                changes.push(CatalogChange::ProviderRemoved {
                    provider: provider.id.clone(),
                });
            }
        }

        let previous_models: BTreeMap<&ModelId, &CatalogModelEntry> = previous
            .models()
            .iter()
            .map(|model| (&model.id, model))
            .collect();
        let current_models: BTreeMap<&ModelId, &CatalogModelEntry> = current
            .models()
            .iter()
            .map(|model| (&model.id, model))
            .collect();

        for (id, model) in &current_models {
            if !previous_models.contains_key(id) {
                changes.push(CatalogChange::ModelAdded {
                    model: (*id).clone(),
                });
                for offering in &model.offerings {
                    changes.push(CatalogChange::OfferingAdded {
                        model: (*id).clone(),
                        provider: offering.provider.clone(),
                    });
                }
            }
        }
        for (id, model) in &previous_models {
            if !current_models.contains_key(id) {
                changes.push(CatalogChange::ModelRemoved {
                    model: (*id).clone(),
                });
                for offering in &model.offerings {
                    changes.push(CatalogChange::OfferingRemoved {
                        model: (*id).clone(),
                        provider: offering.provider.clone(),
                    });
                }
            }
        }
        for (id, model) in &current_models {
            let Some(before) = previous_models.get(id) else {
                continue;
            };
            match (&before.neutral, &model.neutral) {
                (None, Some(_)) => changes.push(CatalogChange::NeutralDescribed {
                    model: (*id).clone(),
                }),
                (Some(_), None) => changes.push(CatalogChange::NeutralDropped {
                    model: (*id).clone(),
                }),
                (Some(was), Some(now)) => {
                    let fields = now.differences(was);
                    if !fields.is_empty() {
                        changes.push(CatalogChange::NeutralChanged {
                            model: (*id).clone(),
                            fields,
                        });
                    }
                }
                (None, None) => {}
            }
            for offering in &model.offerings {
                let Some(previous_offering) = paired(before, model, offering) else {
                    changes.push(CatalogChange::OfferingAdded {
                        model: (*id).clone(),
                        provider: offering.provider.clone(),
                    });
                    continue;
                };
                changes.extend(offering_changes(id, previous_offering, offering));
            }
            for offering in &before.offerings {
                if paired(model, before, offering).is_none() {
                    changes.push(CatalogChange::OfferingRemoved {
                        model: (*id).clone(),
                        provider: offering.provider.clone(),
                    });
                }
            }
        }

        changes.sort_by(|left, right| {
            left.model()
                .cmp(&right.model())
                .then_with(|| left.provider().cmp(&right.provider()))
                .then_with(|| left.rank().cmp(&right.rank()))
        });
        Self { changes }
    }

    pub fn changes(&self) -> &[CatalogChange] {
        &self.changes
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub fn counts(&self) -> CatalogDiffCounts {
        let mut counts = CatalogDiffCounts::default();
        for change in &self.changes {
            match change {
                CatalogChange::ProviderAdded { .. } => counts.providers_added += 1,
                CatalogChange::ProviderRemoved { .. } => counts.providers_removed += 1,
                CatalogChange::ProviderChanged { .. } => counts.providers_changed += 1,
                CatalogChange::NeutralChanged { .. }
                | CatalogChange::NeutralDescribed { .. }
                | CatalogChange::NeutralDropped { .. } => counts.neutral_changed += 1,
                CatalogChange::ModelAdded { .. } => counts.models_added += 1,
                CatalogChange::ModelRemoved { .. } => counts.models_removed += 1,
                CatalogChange::OfferingAdded { .. } => counts.offerings_added += 1,
                CatalogChange::OfferingRemoved { .. } => counts.offerings_removed += 1,
                CatalogChange::LifecycleChanged { .. } => counts.lifecycle_changed += 1,
                CatalogChange::CapabilitiesChanged { .. } => counts.capabilities_changed += 1,
                CatalogChange::MetadataChanged { .. } => counts.metadata_changed += 1,
                CatalogChange::PriceChanged { .. } => counts.prices_changed += 1,
            }
        }
        counts
    }

    /// Whether any published price moved. The question an operator asks first,
    /// and the reason price changes are classified separately.
    pub fn has_price_changes(&self) -> bool {
        self.changes
            .iter()
            .any(|change| matches!(change, CatalogChange::PriceChanged { .. }))
    }
}

/// The offering in `entry` that `offering` is the other revision of.
///
/// A provider that offers the model once is paired by provider alone, so
/// renaming the id callers must send reads as that one offering changing rather
/// than as one disappearing and another arriving. A provider offering the model
/// under several ids has no such single counterpart, so those are paired by the
/// id each is published under.
fn paired<'a>(
    entry: &'a CatalogModelEntry,
    from: &CatalogModelEntry,
    offering: &ProviderOffering,
) -> Option<&'a ProviderOffering> {
    let mut published = entry.offerings_by(&offering.provider);
    let first = published.next()?;
    if published.next().is_none() && from.offerings_by(&offering.provider).count() == 1 {
        return Some(first);
    }
    entry.offering_published_as(&offering.provider, &offering.published_model_id)
}

fn offering_changes(
    model: &ModelId,
    previous: &ProviderOffering,
    current: &ProviderOffering,
) -> Vec<CatalogChange> {
    let mut changes = Vec::new();
    let mut differences = current.facts.differences(&previous.facts);
    if previous.endpoint != current.endpoint {
        differences.push(ModelField::Endpoint);
    }
    // The id a request must send is part of the identity, so a provider moving
    // from an authored key to its own local one has to be reportable: the
    // offering is otherwise unchanged, and an operator would see a changed
    // catalogue with nothing named.
    if previous.published_model_id != current.published_model_id {
        differences.push(ModelField::PublishedModelId);
    }
    if differences.iter().any(|field| field.lifecycle()) {
        changes.push(CatalogChange::LifecycleChanged {
            model: model.clone(),
            provider: current.provider.clone(),
            from: previous.facts.lifecycle,
            to: current.facts.lifecycle,
        });
    }
    let capability_fields: Vec<ModelField> = differences
        .iter()
        .copied()
        .filter(|field| field.capability())
        .collect();
    if !capability_fields.is_empty() {
        changes.push(CatalogChange::CapabilitiesChanged {
            model: model.clone(),
            provider: current.provider.clone(),
            fields: capability_fields,
        });
    }
    let metadata_fields: Vec<ModelField> = differences
        .iter()
        .copied()
        .filter(|field| !field.lifecycle() && !field.capability())
        .collect();
    if !metadata_fields.is_empty() {
        changes.push(CatalogChange::MetadataChanged {
            model: model.clone(),
            provider: current.provider.clone(),
            fields: metadata_fields,
        });
    }
    if previous.price != current.price {
        changes.push(CatalogChange::PriceChanged {
            model: model.clone(),
            provider: current.provider.clone(),
            from: previous.price.clone().map(Box::new),
            to: current.price.clone().map(Box::new),
        });
    }
    changes
}

/// What admitting an import did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admission {
    /// The import's content is the active content. The snapshot's provenance —
    /// validators, fetch time — is refreshed; the catalogue is not.
    Unchanged { content_id: CatalogContentId },
    /// New content became active, replacing what the diff describes.
    Updated {
        content_id: CatalogContentId,
        diff: CatalogDiff,
    },
    /// The first content this deployment has seen. Not an update: there is
    /// nothing to have changed from, so a diff against "nothing" would report
    /// every model as an addition.
    Initial { content_id: CatalogContentId },
}

/// The active catalogue, and the rule that a failed import cannot disturb it.
///
/// Every import goes through [`LastKnownGoodCatalog::admit`] or
/// [`LastKnownGoodCatalog::admit_result`]; there is no other way to make content
/// active, so "a malformed payload cannot replace the active catalogue" is a
/// property of the type rather than of each caller remembering to check.
///
/// # Staleness must not be silent
///
/// A refusal is durable by design — the previously admitted content stays active
/// — which means a *persistent* refusal is a catalogue that has stopped
/// advancing. Nothing in this slice can raise that alarm, because refresh is not
/// scheduled here: the source is
/// [`BackendPath::Background`](super::BackendPath::Background) and is driven by a
/// caller that does not exist yet. So the contract is placed on that caller,
/// and this type is built to make it keepable rather than optional:
/// [`LastKnownGoodCatalog::admit_result`] hands back the typed error *and* the
/// snapshot that stayed active, so a scheduler cannot observe a refusal without
/// also holding the thing that went stale, and every rejection carries a JSON
/// Pointer to the location that caused it.
///
/// Whoever schedules refresh must therefore ship, with it: a refusal counter
/// labelled by reason, the active snapshot's
/// [`SourceSnapshot::fetched_at`] exported as an age, and an alert when refusals
/// persist across more than one interval — tracked in
/// [#241](https://github.com/Litvue/axond/issues/241). Staleness degrades
/// metadata quality only: no enablement, admission, or billing decision reads
/// this snapshot, so a stale catalogue is never an outage.
#[derive(Debug, Default)]
pub struct LastKnownGoodCatalog {
    active: Option<CatalogSnapshot>,
}

impl LastKnownGoodCatalog {
    pub const fn new() -> Self {
        Self { active: None }
    }

    pub fn active(&self) -> Option<&CatalogSnapshot> {
        self.active.as_ref()
    }

    pub fn content(&self) -> Option<&CatalogContent> {
        self.active.as_ref().map(|snapshot| &snapshot.content)
    }

    /// The validators to send on the next conditional refresh.
    pub fn validators(&self) -> Option<&SourceValidators> {
        self.active
            .as_ref()
            .map(|snapshot| &snapshot.source.validators)
    }

    pub fn admit(&mut self, snapshot: CatalogSnapshot) -> Admission {
        let content_id = snapshot.source.content_id;
        let admission = match self.active.as_ref() {
            None => Admission::Initial { content_id },
            Some(active) if active.source.content_id == content_id => {
                Admission::Unchanged { content_id }
            }
            Some(active) => Admission::Updated {
                content_id,
                diff: snapshot.content.diff(&active.content),
            },
        };
        self.active = Some(snapshot);
        admission
    }

    /// Admit a parse result, leaving the active snapshot untouched on failure.
    pub fn admit_result<E>(
        &mut self,
        parsed: Result<CatalogSnapshot, E>,
    ) -> Result<Admission, (E, Option<&CatalogSnapshot>)> {
        match parsed {
            Ok(snapshot) => Ok(self.admit(snapshot)),
            Err(error) => Err((error, self.active.as_ref())),
        }
    }
}

/// Build the provenance for a payload that has been parsed into `content`.
///
/// Kept as one function so the raw digest and the content id are always taken
/// from the same accepted bytes and the same normalized content.
pub fn source_snapshot(
    source_url: impl Into<String>,
    schema_version: SchemaVersion,
    payload: &[u8],
    content: &CatalogContent,
    validators: SourceValidators,
    fetched_at: SystemTime,
) -> SourceSnapshot {
    SourceSnapshot {
        source_url: source_url.into(),
        schema_version,
        validators,
        fetched_at,
        raw: BlobRef::of(BlobKind::CatalogSnapshot, payload),
        content_id: content.content_id(),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::super::{BackendPath, Capability, fakes::InMemoryCatalog, responsibility};
    use super::*;

    fn provider(id: &str) -> CatalogProvider {
        CatalogProvider {
            id: ProviderId::parse(id).expect("fixture id"),
            display_name: Some(id.to_owned()),
            doc_url: None,
            endpoint: ProviderEndpoint::default(),
            env_vars: vec![format!("{}_API_KEY", id.to_uppercase())],
            pointer: JsonPointer::new("").child("providers").child(id),
        }
    }

    fn facts() -> ModelFacts {
        ModelFacts {
            display_name: Some("GPT-4o".to_owned()),
            capabilities: [ModelCapability::ToolCall].into_iter().collect(),
            input_modalities: [Modality::Text].into_iter().collect(),
            output_modalities: [Modality::Text].into_iter().collect(),
            limits: ModelLimits {
                context_tokens: Some(128_000),
                output_tokens: Some(16_384),
                ..ModelLimits::default()
            },
            ..ModelFacts::default()
        }
    }

    fn offering(provider: &str, model: &str, price: Option<ObservedPrice>) -> ProviderOffering {
        ProviderOffering {
            provider: ProviderId::parse(provider).expect("fixture id"),
            model: ModelId::parse(model).expect("fixture id"),
            published_model_id: model.to_owned(),
            facts: facts(),
            overrides: Vec::new(),
            price,
            endpoint: ProviderEndpoint::default(),
            pointer: JsonPointer::new("")
                .child("providers")
                .child(provider)
                .child("models")
                .child(model),
        }
    }

    /// A provider offering one model under two ids has two offerings, so each is
    /// compared against the id it is published under: a price moving on one is
    /// that offering changing, not one offering disappearing and another
    /// arriving.
    #[test]
    fn a_second_published_id_from_one_provider_is_diffed_as_its_own_offering() {
        let mut alias = offering("openai", "gpt-4o", Some(price(1, 2)));
        alias.published_model_id = "gpt-4o-latest".to_owned();
        let mut dearer = alias.clone();
        dearer.price = Some(price(1, 3));
        let before = content(vec![offering("openai", "gpt-4o", Some(price(1, 2))), alias]);
        let after = content(vec![
            offering("openai", "gpt-4o", Some(price(1, 2))),
            dearer,
        ]);

        assert_ne!(before.content_id(), after.content_id());
        let diff = after.diff(&before);
        assert_eq!(diff.counts().prices_changed, 1);
        assert_eq!(diff.counts().offerings_added, 0);
        assert_eq!(diff.counts().offerings_removed, 0);
    }

    fn price(input: u64, output: u64) -> ObservedPrice {
        ObservedPrice::new(PriceRates::new(
            ObservedRate::from_nanos(input),
            ObservedRate::from_nanos(output),
        ))
    }

    fn content(offerings: Vec<ProviderOffering>) -> CatalogContent {
        let mut providers: Vec<CatalogProvider> = offerings
            .iter()
            .map(|offering| provider(offering.provider.as_str()))
            .collect();
        providers.dedup_by(|left, right| left.id == right.id);
        let mut models: BTreeMap<ModelId, CatalogModelEntry> = BTreeMap::new();
        for offering in offerings {
            models
                .entry(offering.model.clone())
                .or_insert_with(|| CatalogModelEntry {
                    id: offering.model.clone(),
                    neutral: Some(facts()),
                    offerings: Vec::new(),
                })
                .offerings
                .push(offering);
        }
        CatalogContent::new(providers, models.into_values().collect()).expect("fixture content")
    }

    fn snapshot(content: CatalogContent, validators: SourceValidators) -> CatalogSnapshot {
        let source = source_snapshot(
            "https://models.dev/catalog.json",
            SchemaVersion::MODELS_DEV_CATALOG_V1,
            b"{}",
            &content,
            validators,
            SystemTime::UNIX_EPOCH,
        );
        CatalogSnapshot { source, content }
    }

    #[test]
    fn identical_content_is_one_identity_whatever_order_it_was_built_in() {
        let forwards = content(vec![
            offering(
                "anthropic",
                "claude-sonnet-4",
                Some(price(3_000_000_000, 15_000_000_000)),
            ),
            offering(
                "openai",
                "gpt-4o",
                Some(price(2_500_000_000, 10_000_000_000)),
            ),
        ]);
        let backwards = content(vec![
            offering(
                "openai",
                "gpt-4o",
                Some(price(2_500_000_000, 10_000_000_000)),
            ),
            offering(
                "anthropic",
                "claude-sonnet-4",
                Some(price(3_000_000_000, 15_000_000_000)),
            ),
        ]);
        assert_eq!(forwards.content_id(), backwards.content_id());
        assert_eq!(forwards, backwards);
    }

    #[test]
    fn retrieval_metadata_does_not_change_content_identity() {
        let content = content(vec![offering("openai", "gpt-4o", None)]);
        let first = snapshot(content.clone(), SourceValidators::etag("\"one\""));
        let second = CatalogSnapshot {
            source: SourceSnapshot {
                fetched_at: SystemTime::UNIX_EPOCH + Duration::from_secs(86_400),
                validators: SourceValidators {
                    etag: Some(ETag("\"two\"".to_owned())),
                    last_modified: Some(HttpDate("Wed, 12 Aug 2026 20:00:00 GMT".to_owned())),
                },
                ..first.source.clone()
            },
            content,
        };
        assert_eq!(first.source.content_id, second.source.content_id);
        assert_ne!(first.source.validators, second.source.validators);
    }

    #[test]
    fn a_price_change_is_classified_apart_from_metadata() {
        let before = content(vec![offering(
            "openai",
            "gpt-4o",
            Some(price(2_500_000_000, 10_000_000_000)),
        )]);
        let after = content(vec![offering(
            "openai",
            "gpt-4o",
            Some(price(2_000_000_000, 10_000_000_000)),
        )]);
        let diff = after.diff(&before);
        assert!(diff.has_price_changes());
        assert_eq!(diff.counts().prices_changed, 1);
        assert_eq!(diff.counts().metadata_changed, 0);
        assert_eq!(diff.counts().capabilities_changed, 0);
        assert!(matches!(
            diff.changes(),
            [CatalogChange::PriceChanged { from, to, .. }]
                if from.as_ref().map(|price| price.base.input.nanos()) == Some(2_500_000_000)
                    && to.as_ref().map(|price| price.base.input.nanos()) == Some(2_000_000_000)
        ));
    }

    #[test]
    fn lifecycle_capability_and_metadata_changes_are_separate_classes() {
        let before = content(vec![offering("openai", "gpt-4o", None)]);
        let mut changed = offering("openai", "gpt-4o", None);
        changed.facts.lifecycle = ModelLifecycle::Deprecated;
        changed
            .facts
            .capabilities
            .insert(ModelCapability::Reasoning);
        changed.facts.limits.context_tokens = Some(200_000);
        let after = content(vec![changed]);

        let counts = after.diff(&before).counts();
        assert_eq!(counts.lifecycle_changed, 1);
        assert_eq!(counts.capabilities_changed, 1);
        assert_eq!(counts.metadata_changed, 1);
        assert_eq!(counts.prices_changed, 0);
    }

    /// A different identity must always come with something to show for it:
    /// "the catalogue changed" and an empty diff cannot both be true.
    #[test]
    fn every_change_the_identity_notices_the_diff_names() {
        let before = content(vec![offering("openai", "gpt-4o", None)]);

        let mut providers = before.providers().to_vec();
        providers[0].env_vars.push("OPENAI_BASE_URL".to_owned());
        let provider_moved = CatalogContent::new(providers, before.models().to_vec())
            .expect("a provider's own metadata changed");

        let mut neutral = before.models().to_vec();
        neutral[0].neutral = None;
        let neutral_dropped = CatalogContent::new(before.providers().to_vec(), neutral)
            .expect("the neutral record went away");

        let mut endpoint = offering("openai", "gpt-4o", None);
        endpoint.endpoint = ProviderEndpoint {
            api_base: Some("https://eu.api.openai.com/v1".to_owned()),
            ..ProviderEndpoint::default()
        };
        let endpoint_moved = content(vec![endpoint]);

        // A provider renaming what callers must send: the offering is otherwise
        // identical, so nothing but this field can report it.
        let mut renamed = offering("openai", "gpt-4o", None);
        renamed.published_model_id = "gpt-4o-2024-11-20".to_owned();
        let republished = content(vec![renamed]);

        for (case, after, expected) in [
            (
                "provider metadata",
                provider_moved,
                CatalogChange::ProviderChanged {
                    provider: ProviderId::parse("openai").expect("fixture id"),
                    fields: vec![ProviderField::EnvVars],
                },
            ),
            (
                "the neutral record",
                neutral_dropped,
                CatalogChange::NeutralDropped {
                    model: ModelId::parse("gpt-4o").expect("fixture id"),
                },
            ),
            (
                "an offering's endpoint",
                endpoint_moved,
                CatalogChange::MetadataChanged {
                    model: ModelId::parse("gpt-4o").expect("fixture id"),
                    provider: ProviderId::parse("openai").expect("fixture id"),
                    fields: vec![ModelField::Endpoint],
                },
            ),
            (
                "the id a request must send",
                republished,
                CatalogChange::MetadataChanged {
                    model: ModelId::parse("gpt-4o").expect("fixture id"),
                    provider: ProviderId::parse("openai").expect("fixture id"),
                    fields: vec![ModelField::PublishedModelId],
                },
            ),
        ] {
            assert_ne!(
                before.content_id(),
                after.content_id(),
                "{case} is part of the identity"
            );
            assert_eq!(after.diff(&before).changes(), [expected], "{case}");
        }
    }

    #[test]
    fn additions_and_removals_name_their_offerings() {
        let before = content(vec![offering("openai", "gpt-4o", None)]);
        let after = content(vec![
            offering("openai", "gpt-4o", None),
            offering("anthropic", "claude-sonnet-4", None),
        ]);
        let diff = after.diff(&before);
        assert_eq!(diff.counts().models_added, 1);
        assert_eq!(diff.counts().offerings_added, 1);
        assert_eq!(diff.counts().models_removed, 0);

        let reversed = before.diff(&after);
        assert_eq!(reversed.counts().models_removed, 1);
        assert_eq!(reversed.counts().offerings_removed, 1);
    }

    #[test]
    fn a_diff_is_ordered_by_model_then_provider_then_kind() {
        let before = content(vec![
            offering("anthropic", "claude-sonnet-4", Some(price(1, 2))),
            offering("openai", "gpt-4o", Some(price(1, 2))),
        ]);
        let mut anthropic = offering("anthropic", "claude-sonnet-4", Some(price(3, 2)));
        anthropic.facts.lifecycle = ModelLifecycle::Deprecated;
        let after = content(vec![
            anthropic,
            offering("openai", "gpt-4o", Some(price(4, 2))),
        ]);

        let diff = after.diff(&before);
        let ordered: Vec<(Option<String>, Option<String>, u8)> = diff
            .changes()
            .iter()
            .map(|change| {
                (
                    change.model().map(ToString::to_string),
                    change.provider().map(ToString::to_string),
                    change.rank(),
                )
            })
            .collect();
        let mut sorted = ordered.clone();
        sorted.sort();
        assert_eq!(ordered, sorted, "changes come out in a stable order");
        assert_eq!(after.diff(&before), after.diff(&before));
    }

    #[test]
    fn content_rejects_a_dangling_or_duplicated_offering() {
        let orphan = CatalogModelEntry {
            id: ModelId::parse("gpt-4o").expect("id"),
            neutral: None,
            offerings: vec![offering("openai", "gpt-4o", None)],
        };
        assert_eq!(
            CatalogContent::new(Vec::new(), vec![orphan.clone()]),
            Err(CatalogContentError::UnknownProvider {
                model: ModelId::parse("gpt-4o").expect("id"),
                provider: ProviderId::parse("openai").expect("id"),
            })
        );

        let doubled = CatalogModelEntry {
            offerings: vec![
                offering("openai", "gpt-4o", None),
                offering("openai", "gpt-4o", None),
            ],
            ..orphan
        };
        assert_eq!(
            CatalogContent::new(vec![provider("openai")], vec![doubled]),
            Err(CatalogContentError::DuplicateOffering {
                model: ModelId::parse("gpt-4o").expect("id"),
                provider: ProviderId::parse("openai").expect("id"),
                published: "gpt-4o".to_owned(),
            })
        );
        assert_eq!(
            CatalogContent::new(vec![provider("openai")], Vec::new()),
            Err(CatalogContentError::Empty)
        );
    }

    #[test]
    fn an_offering_filed_under_the_wrong_model_is_refused() {
        let entry = CatalogModelEntry {
            id: ModelId::parse("gpt-4o-mini").expect("id"),
            neutral: None,
            offerings: vec![offering("openai", "gpt-4o", None)],
        };
        assert_eq!(
            CatalogContent::new(vec![provider("openai")], vec![entry]),
            Err(CatalogContentError::OfferingModelMismatch {
                model: ModelId::parse("gpt-4o-mini").expect("id"),
                provider: ProviderId::parse("openai").expect("id"),
                published: "gpt-4o".to_owned(),
            })
        );
    }

    #[test]
    fn ids_are_taken_as_published_and_validated_not_rewritten() {
        for published in [
            "anthropic/claude-sonnet-4",
            "MiniMax-M1",
            "Qwen/Qwen3-32B",
            "gpt-4o@2024-08-06",
            "accounts/fireworks/models/kimi~k2",
        ] {
            assert_eq!(
                ModelId::parse(published).expect("a published id").as_str(),
                published,
                "an id is stored as the provider publishes it"
            );
        }
        assert_ne!(
            ModelId::parse("MiniMax-M1").expect("id"),
            ModelId::parse("minimax-m1").expect("id"),
            "case distinguishes two published models"
        );
        assert_eq!(
            ModelId::parse("gpt 4o"),
            Err(InvalidCatalogId::Character {
                value: "gpt 4o".to_owned(),
                character: ' ',
            })
        );
        assert_eq!(ModelId::parse(""), Err(InvalidCatalogId::Empty));
        assert_eq!(
            ModelId::parse("openai//gpt-4o"),
            Err(InvalidCatalogId::Segment {
                value: "openai//gpt-4o".to_owned(),
            })
        );
        assert!(matches!(
            ModelId::parse(&"m".repeat(129)),
            Err(InvalidCatalogId::TooLong { max: 128, .. })
        ));
    }

    #[test]
    fn a_rejected_import_leaves_the_active_catalogue_in_place() {
        let mut catalogue = LastKnownGoodCatalog::new();
        let good = snapshot(
            content(vec![offering("openai", "gpt-4o", Some(price(1, 2)))]),
            SourceValidators::etag("\"one\""),
        );
        assert_eq!(
            catalogue.admit(good.clone()),
            Admission::Initial {
                content_id: good.source.content_id
            }
        );

        let rejected: Result<CatalogSnapshot, &str> = Err("schema drift");
        let (error, active) = catalogue
            .admit_result(rejected)
            .expect_err("a drifted payload is refused");
        assert_eq!(error, "schema drift");
        assert_eq!(
            active.map(|snapshot| snapshot.source.content_id),
            Some(good.source.content_id)
        );
        assert_eq!(
            catalogue.content().map(CatalogContent::offering_count),
            Some(1)
        );
        assert_eq!(
            catalogue.validators(),
            Some(&SourceValidators::etag("\"one\""))
        );
    }

    #[test]
    fn admitting_identical_content_is_not_an_update() {
        let mut catalogue = LastKnownGoodCatalog::new();
        let content = content(vec![offering("openai", "gpt-4o", Some(price(1, 2)))]);
        catalogue.admit(snapshot(content.clone(), SourceValidators::etag("\"one\"")));
        let admission =
            catalogue.admit(snapshot(content.clone(), SourceValidators::etag("\"two\"")));
        assert_eq!(
            admission,
            Admission::Unchanged {
                content_id: content.content_id()
            }
        );
        // The provenance still advances: the next conditional request must use
        // the validators the upstream last sent.
        assert_eq!(
            catalogue.validators(),
            Some(&SourceValidators::etag("\"two\""))
        );

        let updated = content_with_price(3);
        let admission = catalogue.admit(snapshot(updated, SourceValidators::default()));
        assert!(matches!(admission, Admission::Updated { diff, .. } if diff.has_price_changes()));
    }

    fn content_with_price(input: u64) -> CatalogContent {
        content(vec![offering("openai", "gpt-4o", Some(price(input, 2)))])
    }

    #[tokio::test]
    async fn a_first_refresh_returns_metadata_with_validators() {
        let source = InMemoryCatalog::with_models(&[("openai", "gpt-4o")], "v1");
        let CatalogRefresh::Updated(snapshot) = source.refresh(None).await.expect("refresh") else {
            panic!("a first refresh has no prior validators to match");
        };
        assert_eq!(snapshot.source.validators, SourceValidators::etag("v1"));
        assert_eq!(snapshot.content.offering_count(), 1);
        assert_eq!(
            snapshot.content.models()[0].id,
            ModelId::parse("gpt-4o").expect("id")
        );
        assert_eq!(
            snapshot.source.schema_version,
            SchemaVersion::MODELS_DEV_CATALOG_V1
        );
    }

    #[tokio::test]
    async fn an_unchanged_upstream_is_not_an_empty_catalogue() {
        let source = InMemoryCatalog::with_models(&[("openai", "gpt-4o")], "v1");
        let refreshed = source
            .refresh(Some(&SourceValidators::etag("v1")))
            .await
            .expect("refresh");
        assert_eq!(
            refreshed,
            CatalogRefresh::Unchanged {
                validators: SourceValidators::etag("v1")
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
            .refresh(Some(&SourceValidators::etag("v1")))
            .await
            .expect("refresh")
        else {
            panic!("changed validators must transfer");
        };
        assert_eq!(snapshot.source.validators, SourceValidators::etag("v2"));
        assert_eq!(source.transfers(), 1);
    }

    #[tokio::test]
    async fn observed_pricing_is_metadata_not_activation() {
        let source = InMemoryCatalog::with_models(&[("openai", "gpt-4o")], "v1");
        let CatalogRefresh::Updated(snapshot) = source.refresh(None).await.unwrap() else {
            panic!("expected metadata");
        };
        let offering = &snapshot.content.models()[0].offerings[0];
        let price = offering.price.as_ref().expect("the fake publishes a price");
        // The contract carries the observed rate; nothing here can enable a
        // model or bill against it.
        assert!(price.base.input.nanos() > 0);
        assert!(!offering.facts.lifecycle.deprecated());
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
        let source = InMemoryCatalog::with_models(&[("openai", "gpt-4o")], "v1");
        assert!(source.capabilities().has(Capability::IncrementalRefresh));
        assert!(source.capabilities().has(Capability::PriceMetadata));

        let responsibility = responsibility("CatalogSource").expect("declared responsibility");
        assert_eq!(responsibility.path, BackendPath::Background);
        assert!(responsibility.permits(CatalogBackend::default().kind()));
        assert!(!responsibility.permits(BackendKind::Redis));
    }
}
