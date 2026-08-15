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
//! [ADR 0043](https://github.com/Litvue/axond/blob/main/docs/adr/0043-catalogue-source-imports.md).
//!
//! # One filing, one projection of it
//!
//! Content here is filed under the model an offering is an offering *of*, which
//! answers "who offers this model?". What a caller may *send* is a provider and
//! that provider's own published id, and a provider may publish one model under
//! several of those, so [`super::catalog_projection`] keys the same offerings by
//! [`CallableId`](super::catalog_projection::CallableId) and classifies diffs
//! over the ids a request uses. Nothing in this module changes for it: the
//! projection borrows, adds no state, and is not a second place facts live.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

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

    /// Take each validator `stated` about content already held, keeping the held
    /// value where the answer states none.
    ///
    /// An unstated validator is not a withdrawn one: intermediaries drop them,
    /// and dropping the held value in turn would leave nothing to ask
    /// conditionally with, turning every later refresh into a full transfer of a
    /// document already in hand.
    pub fn carry_over(&mut self, stated: Self) {
        if let Some(etag) = stated.etag {
            self.etag = Some(etag);
        }
        if let Some(last_modified) = stated.last_modified {
            self.last_modified = Some(last_modified);
        }
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
    /// The identity a stored record names.
    ///
    /// A price book records the catalogue content it was approved against
    /// ([`crate::desired_state::pricing`]), so the identity has to survive a round
    /// trip through a canonical body. Constructing one does not assert that the
    /// content is held — it names it.
    pub const fn from_checksum(checksum: Checksum) -> Self {
        Self(checksum)
    }

    pub const fn checksum(self) -> Checksum {
        self.0
    }

    /// The first [`CONTENT_ID_SHORT_HEX`] hex digits of the digest.
    ///
    /// The form an operator surface may carry. A full digest and a short one are
    /// equally unbounded over a deployment's lifetime — neither may be a metric
    /// label — but a short one is fixed-width, derived from bytes this process
    /// hashed rather than from upstream text, and long enough to match against
    /// the id an import logged.
    pub fn short(self) -> String {
        self.0
            .as_bytes()
            .iter()
            .take(CONTENT_ID_SHORT_HEX / 2)
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }
}

/// The width of [`CatalogContentId::short`], in hex digits.
pub const CONTENT_ID_SHORT_HEX: usize = 16;

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

/// The exact bytes one import accepted, shared rather than copied.
///
/// [`SourceSnapshot::raw`] names these bytes; this is the bytes themselves, and
/// they exist because a digest is not a payload: retaining an import durably
/// means storing what was accepted, and rehydrating a stored catalogue means
/// parsing those same bytes again through the same parser. Carried only from a
/// fetch to whatever retains it — nothing holds it for the life of a snapshot,
/// so an active catalogue costs its normalized content and not a second copy of
/// a multi-megabyte document.
#[derive(Clone, PartialEq, Eq)]
pub struct RawPayload(Arc<[u8]>);

impl RawPayload {
    pub fn new(bytes: impl Into<Arc<[u8]>>) -> Self {
        Self(bytes.into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Length only. A payload is an upstream document of unbounded size, and a
/// derived `Debug` would put the whole of it in a log line the first time one is
/// traced.
impl std::fmt::Debug for RawPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawPayload")
            .field("bytes", &self.0.len())
            .finish()
    }
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
    Updated {
        /// Boxed because a snapshot is a whole catalogue and `Unchanged` — the
        /// common answer — is two optional header values.
        snapshot: Box<CatalogSnapshot>,
        /// The bytes the snapshot was parsed from, so a store can retain the
        /// import itself rather than a rendering of it. Handed over here, at the
        /// one moment they are in hand, because a source that dropped them left
        /// the deployment unable to prove later what it accepted.
        payload: RawPayload,
    },
}

/// Why an import did not become the active catalogue, as a bounded label.
///
/// A refusal is durable — the previously admitted content stays active — so the
/// reason has to survive as far as a metric, and a metric label may not be
/// upstream text. Every arm is therefore a fixed string chosen by the code that
/// classified the failure, never a fragment of the payload: the vocabulary's
/// size is a property of this enum, and the location that caused the refusal
/// travels beside it as a [`JsonPointer`] rather than inside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RefusalReason {
    /// The source could not be reached, or did not answer.
    Unreachable,
    /// The source refused the request on authentication or authorization
    /// grounds.
    Denied,
    /// The payload is larger than a refresh will hold.
    Oversized,
    /// The configured URL is not a document this source can read.
    UnsupportedEndpoint,
    /// The bytes are not JSON.
    NotJson,
    /// JSON, but not this source's document shape.
    Schema,
    /// A record's key disagrees with the `id` it embeds.
    IdMismatch,
    /// An identifier the domain will not hold.
    Identifier,
    /// A lifecycle status this build does not model.
    UnknownStatus,
    /// A modality this build does not model.
    UnknownModality,
    /// A price the gateway cannot represent exactly, or one stated in part.
    Price,
    /// A price tier of an unrecognized kind.
    UnknownTierType,
    /// Two prices for one tier threshold.
    DuplicateTier,
    /// A price published where prices do not belong.
    NeutralPrice,
    /// Free text that normalized content has no canonical form for.
    UncanonicalizableText,
    /// A provider-local model key that resolves to more than one model.
    AmbiguousModelKey,
    /// Normalized content that is not internally consistent.
    Content,
    /// The import parsed, and the store would not retain it. Its own reason
    /// because the payload is blameless: an operator reading `schema` goes and
    /// looks at the upstream document, while this one is the deployment's own
    /// database, and a catalogue admitted in memory but absent from the store
    /// would be a catalogue that silently un-imports itself on restart.
    NotRetained,
    /// The source confirmed content nobody holds: an unchanged answer arrived
    /// where no import has ever succeeded, so there is no error text and no
    /// pointer behind this refusal — only an answer no conditional request
    /// asked for.
    UnsolicitedUnchanged,
    /// Classified as a refusal this vocabulary has no code for. Present so a new
    /// failure mode degrades to a safe label instead of tempting a caller to
    /// pass through the error's text.
    Unknown,
}

/// Every refusal reason, in [`RefusalReason::ALL`] order.
///
/// Duplicated as strings so the metric catalogue can name the vocabulary in a
/// const context; a test asserts the two never drift.
pub const REFUSAL_REASONS: &[&str] = &[
    "unreachable",
    "denied",
    "oversized",
    "unsupported_endpoint",
    "not_json",
    "schema",
    "id_mismatch",
    "identifier",
    "unknown_status",
    "unknown_modality",
    "price",
    "unknown_tier_type",
    "duplicate_tier",
    "neutral_price",
    "uncanonicalizable_text",
    "ambiguous_model_key",
    "content",
    "not_retained",
    "unsolicited_unchanged",
    "unknown",
];

impl RefusalReason {
    pub const ALL: &'static [Self] = &[
        Self::Unreachable,
        Self::Denied,
        Self::Oversized,
        Self::UnsupportedEndpoint,
        Self::NotJson,
        Self::Schema,
        Self::IdMismatch,
        Self::Identifier,
        Self::UnknownStatus,
        Self::UnknownModality,
        Self::Price,
        Self::UnknownTierType,
        Self::DuplicateTier,
        Self::NeutralPrice,
        Self::UncanonicalizableText,
        Self::AmbiguousModelKey,
        Self::Content,
        Self::NotRetained,
        Self::UnsolicitedUnchanged,
        Self::Unknown,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unreachable => "unreachable",
            Self::Denied => "denied",
            Self::Oversized => "oversized",
            Self::UnsupportedEndpoint => "unsupported_endpoint",
            Self::NotJson => "not_json",
            Self::Schema => "schema",
            Self::IdMismatch => "id_mismatch",
            Self::Identifier => "identifier",
            Self::UnknownStatus => "unknown_status",
            Self::UnknownModality => "unknown_modality",
            Self::Price => "price",
            Self::UnknownTierType => "unknown_tier_type",
            Self::DuplicateTier => "duplicate_tier",
            Self::NeutralPrice => "neutral_price",
            Self::UncanonicalizableText => "uncanonicalizable_text",
            Self::AmbiguousModelKey => "ambiguous_model_key",
            Self::Content => "content",
            Self::NotRetained => "not_retained",
            Self::UnsolicitedUnchanged => "unsolicited_unchanged",
            Self::Unknown => "unknown",
        }
    }
}

/// One refused import: the bounded reason, and where in the payload it was
/// decided.
///
/// The two are separated on purpose. The reason is what a counter is labelled
/// by and is bounded by [`RefusalReason`]; the pointer names one location in one
/// upstream document, is unbounded over that document's keys, and is therefore
/// for a log line, an alert body, or an operator surface — never a label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    reason: RefusalReason,
    pointer: Option<JsonPointer>,
}

impl Refusal {
    pub const fn new(reason: RefusalReason) -> Self {
        Self {
            reason,
            pointer: None,
        }
    }

    /// A refusal decided at a location in the payload.
    pub fn at(reason: RefusalReason, pointer: JsonPointer) -> Self {
        Self {
            reason,
            pointer: Some(pointer),
        }
    }

    pub const fn reason(&self) -> RefusalReason {
        self.reason
    }

    /// The location that caused the refusal, when the classifier had one.
    ///
    /// This is what an alert names so an operator can open the raw snapshot at
    /// the offending field instead of re-deriving it from prose.
    pub const fn pointer(&self) -> Option<&JsonPointer> {
        self.pointer.as_ref()
    }
}

/// A failure that can name why it refused an import, in the bounded vocabulary.
///
/// Implemented by every error an import can fail with, so
/// [`LastKnownGoodCatalog::admit_result`] can count a refusal by reason without
/// knowing which layer produced it, and so a new error type has to state its
/// reason rather than arrive as free text.
pub trait Refusable {
    fn refusal(&self) -> Refusal;
}

impl Refusable for CatalogError {
    fn refusal(&self) -> Refusal {
        match self {
            Self::Unavailable { refusal, .. }
            | Self::Invalid { refusal, .. }
            | Self::Denied { refusal, .. }
            | Self::Misconfigured { refusal, .. } => refusal.clone(),
        }
    }
}

/// Why a refresh failed.
///
/// Every arm carries a [`Refusal`] rather than only its message, because the
/// message is the one thing that cannot be counted: a source's typed error is
/// flattened to text at this boundary, and a scheduler holding only text would
/// have to parse it back to label a metric.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CatalogError {
    #[error("catalogue source `{backend}` unavailable: {message}")]
    Unavailable {
        backend: &'static str,
        refusal: Refusal,
        message: String,
    },
    #[error("catalogue source `{backend}` returned unusable metadata: {message}")]
    Invalid {
        backend: &'static str,
        refusal: Refusal,
        message: String,
    },
    #[error("catalogue source `{backend}` refused the request: {message}")]
    Denied {
        backend: &'static str,
        refusal: Refusal,
        message: String,
    },
    /// The source cannot serve a catalogue at all — the configured URL answers
    /// `404`, the document was withdrawn, the request shape is rejected.
    ///
    /// Apart from [`CatalogError::Unavailable`] because retrying cannot fix it
    /// and because an operator reading "upstream is down" would look at the
    /// wrong thing: what is wrong is the configuration.
    #[error("catalogue source `{backend}` cannot serve a catalogue: {message}")]
    Misconfigured {
        backend: &'static str,
        refusal: Refusal,
        message: String,
    },
}

impl CatalogError {
    /// The bounded reason this failure refused an import.
    pub const fn refused_by(&self) -> &Refusal {
        match self {
            Self::Unavailable { refusal, .. }
            | Self::Invalid { refusal, .. }
            | Self::Denied { refusal, .. }
            | Self::Misconfigured { refusal, .. } => refusal,
        }
    }

    /// An unreachable source.
    pub const fn unavailable(backend: &'static str, message: String) -> Self {
        Self::Unavailable {
            backend,
            refusal: Refusal::new(RefusalReason::Unreachable),
            message,
        }
    }
}

impl BackendFailure for CatalogError {
    fn category(&self) -> FailureCategory {
        match self {
            Self::Unavailable { .. } => FailureCategory::Unavailable,
            Self::Invalid { .. } => FailureCategory::Invalid,
            Self::Denied { .. } => FailureCategory::Denied,
            Self::Misconfigured { .. } => FailureCategory::NotFound,
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

/// Upstream text as a refusal may repeat it: itself when it is short, and a
/// bounded head plus its true length when it is not.
///
/// Every string a catalogue refusal quotes came off the wire, where the only
/// bound is the payload ceiling — a map key may be megabytes long. A refusal is
/// written to be read by an operator and is retried on a schedule, so quoting
/// one whole would let an upstream choose how many megabytes of its own text
/// this gateway writes to its logs, on a timer. The excerpt keeps the value
/// recognizable; the JSON Pointer every refusal carries is what locates it
/// exactly.
pub fn excerpt(value: &str) -> String {
    const MAX_BYTES: usize = 96;
    if value.len() <= MAX_BYTES {
        return value.to_owned();
    }
    let mut end = MAX_BYTES;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}… ({} bytes)", &value[..end], value.len())
}

/// Bounded like [`excerpt`], but keeping the tail as well as the head.
///
/// For the two refusals that carry no [`JsonPointer`] — a payload that is not
/// JSON, and one whose shape the schema rejects — the deserializer's message is
/// the only locator there is, and it states the position last (`… at line 112
/// column 33`). Cutting only the head would drop precisely the part worth
/// reading, and the hostile case is the one that needs it: a type error quotes
/// the offending value, so an upstream that files a megabyte of text where a
/// number belongs produces exactly the long message whose location is lost.
pub fn excerpt_located(value: &str) -> String {
    const HEAD_BYTES: usize = 96;
    const TAIL_BYTES: usize = 32;
    if value.len() <= HEAD_BYTES + TAIL_BYTES {
        return value.to_owned();
    }
    let mut head = HEAD_BYTES;
    while !value.is_char_boundary(head) {
        head -= 1;
    }
    let mut tail = value.len() - TAIL_BYTES;
    while !value.is_char_boundary(tail) {
        tail += 1;
    }
    format!(
        "{}… ({} bytes) …{}",
        &value[..head],
        value.len(),
        &value[tail..]
    )
}

/// A list of upstream-derived values as a refusal may repeat it.
///
/// Each element is already an accepted identifier and so is bounded, but their
/// number is not: a document may file thousands of models under one ambiguous
/// key. The same reasoning as [`excerpt`] applies to the list itself.
pub fn excerpt_list(values: &[String]) -> String {
    const MAX_ITEMS: usize = 8;
    let shown = values.len().min(MAX_ITEMS);
    let mut listed = values[..shown]
        .iter()
        .map(|value| format!("`{}`", excerpt(value)))
        .collect::<Vec<_>>()
        .join(", ");
    if values.len() > shown {
        listed.push_str(&format!(" and {} more", values.len() - shown));
    }
    listed
}

/// Why an identifier was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidCatalogId {
    #[error("a catalogue identifier may not be empty")]
    Empty,
    #[error("catalogue identifier `{}` is longer than {max} bytes", excerpt(value))]
    TooLong { value: String, max: usize },
    #[error(
        "catalogue identifier `{}` contains `{character}`; \
         only ASCII alphanumerics and `-._:/+@~` are accepted",
        excerpt(value)
    )]
    Character { value: String, character: char },
    #[error("catalogue identifier `{}` has an empty path segment", excerpt(value))]
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

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|capability| capability.as_str() == value)
    }
}

/// Where a model sits in its published lifecycle.
///
/// A closed set for the same reason as [`Modality`]: lifecycle drives what an
/// operator is warned about, so an unrecognized status is refused rather than
/// flattened into "available".
///
/// Unrelated to [`crate::desired_state::models::ModelLifecycle`], which is
/// whether an operator has *put a resource in service*. This one is what the
/// upstream says about its own model, so a `Deprecated` offering can be
/// perfectly enabled, and a withdrawn enablement can point at an `Available`
/// one.
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

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|lifecycle| lifecycle.as_str() == value)
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
        CanonicalValue::map(fields)
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
        CanonicalValue::map(fields)
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
///
/// Not [`crate::desired_state::models::ObservedPrice`] either, and the two are
/// not interchangeable despite the name: that one is the rate desired state
/// carries, in **micro**-dollars per million tokens, while this one is what a
/// source published, in **nano**-dollars per million tokens and with tiers. A
/// value crossing that boundary is a division by 1,000 that has to round, so it
/// belongs in the slice that performs the crossing — where the rounding
/// direction can be stated — and never in a `From` impl a careless import could
/// apply silently.
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
    ///
    /// A token longer than an identifier may ever be is excerpted: such a token
    /// is a map key no catalogue this gateway accepts can carry, so the pointer
    /// built from it only ever appears inside a refusal, and a refusal an
    /// upstream can size is a way to write megabytes into these logs on a timer.
    pub fn child(&self, token: &str) -> Self {
        let bounded;
        let token = if token.len() > CatalogId::MAX_BYTES {
            bounded = excerpt(token);
            bounded.as_str()
        } else {
            token
        };
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
        CanonicalValue::map(fields)
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
        CanonicalValue::map(fields)
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
        CanonicalValue::map(fields)
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
    /// Over what the provider *states*, and nothing else.
    ///
    /// [`ProviderOffering::pointer`] and [`ProviderOffering::overrides`] are left
    /// out, as [`CatalogProvider`]'s pointer is: one is where a value was read
    /// from, the other is a function of the offering's facts against the neutral
    /// record. Neither is content, and including them would let content differ in
    /// identity while [`CatalogDiff`] — which compares stated values — found
    /// nothing to report.
    fn canonical(&self) -> CanonicalValue {
        let mut fields = vec![
            ("provider".to_owned(), self.provider.canonical()),
            ("model".to_owned(), self.model.canonical()),
            (
                "published_model_id".to_owned(),
                CanonicalValue::string(&self.published_model_id),
            ),
            ("facts".to_owned(), self.facts.canonical()),
        ];
        if let Some(price) = &self.price {
            fields.push(("price".to_owned(), price.canonical()));
        }
        if !self.endpoint.is_empty() {
            fields.push(("endpoint".to_owned(), self.endpoint.canonical()));
        }
        CanonicalValue::map(fields)
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
        CanonicalValue::map(fields)
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
    /// Models, but nothing offering them: no provider, or no offering under any
    /// provider.
    ///
    /// Separate from [`CatalogContentError::Empty`] because it is a different
    /// upstream accident — a document that kept its model records and lost its
    /// providers section — and the same danger: a catalogue nothing can be
    /// routed or priced from must not be admitted over one that can.
    #[error("the payload describes {models} model(s) that no provider offers")]
    Unoffered { models: usize },
    /// Text a canonical form cannot hold, so the content has no identity.
    ///
    /// A rejection rather than a panic: upstream free text is upstream's to
    /// choose, and a stray control character must cost an import, not the task
    /// running it.
    ///
    /// A backstop, and deliberately not the arm an operator should see: this
    /// canonicalizes the whole tree at once and so can only report *what* was
    /// wrong, not where. A source adapter is expected to check free text as it
    /// reads it, while the pointer to the field is in hand — the models.dev
    /// adapter refuses with `UncanonicalizableText` naming the field — leaving
    /// this arm for content assembled by some other means.
    #[error("the catalogue has no canonical form: {source}")]
    Uncanonicalizable {
        #[source]
        source: CanonicalError,
    },
}

/// Every arm is content that is not internally consistent, which is one label:
/// the arm names *which* inconsistency in the message, and the pointer a source
/// adapter would have carried is not available here, since this check runs over
/// assembled content rather than over the payload it came from.
impl Refusable for CatalogContentError {
    fn refusal(&self) -> Refusal {
        Refusal::new(RefusalReason::Content)
    }
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
        // Checked after the references, so a document whose offerings name
        // providers it never described is still told which provider is missing.
        if models.iter().all(|model| model.offerings.is_empty()) {
            return Err(CatalogContentError::Unoffered {
                models: models.len(),
            });
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
    /// A provider began offering the model under `published`.
    ///
    /// Named by the id it is published under, not by its provider alone: that
    /// pair is what identifies an offering (see [`CatalogModelEntry::offerings`]),
    /// so a provider gaining one of two aliases would otherwise be a report
    /// indistinguishable from gaining the other.
    OfferingAdded {
        model: ModelId,
        provider: ProviderId,
        published: String,
    },
    /// A provider stopped offering the model under `published`.
    OfferingRemoved {
        model: ModelId,
        provider: ProviderId,
        published: String,
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
    /// An offering's lifecycle moved.
    ///
    /// Every changed-offering variant names `published` for the same reason
    /// [`Self::OfferingAdded`] does: the offering that changed is
    /// `(provider, published)`, so a provider publishing two aliases of one
    /// model would otherwise report two changes an operator cannot tell apart.
    /// When the published id is itself what changed, the id named is the current
    /// one — the id requests must use from now on — and
    /// [`ModelField::PublishedModelId`] in the accompanying
    /// [`Self::MetadataChanged`] says that is what moved.
    LifecycleChanged {
        model: ModelId,
        provider: ProviderId,
        published: String,
        from: ModelLifecycle,
        to: ModelLifecycle,
    },
    CapabilitiesChanged {
        model: ModelId,
        provider: ProviderId,
        published: String,
        fields: Vec<ModelField>,
    },
    MetadataChanged {
        model: ModelId,
        provider: ProviderId,
        published: String,
        fields: Vec<ModelField>,
    },
    /// Boxed because an observed price carries every published rate and tier,
    /// and a diff is mostly changes that carry two ids.
    PriceChanged {
        model: ModelId,
        provider: ProviderId,
        published: String,
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

    /// The id a request would have used, for the changes that name one.
    pub fn published(&self) -> Option<&str> {
        match self {
            Self::OfferingAdded { published, .. }
            | Self::OfferingRemoved { published, .. }
            | Self::LifecycleChanged { published, .. }
            | Self::CapabilitiesChanged { published, .. }
            | Self::MetadataChanged { published, .. }
            | Self::PriceChanged { published, .. } => Some(published),
            _ => None,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize)]
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
                        published: offering.published_model_id.clone(),
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
                        published: offering.published_model_id.clone(),
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
                        published: offering.published_model_id.clone(),
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
                        published: offering.published_model_id.clone(),
                    });
                }
            }
        }

        changes.sort_by(|left, right| {
            left.model()
                .cmp(&right.model())
                .then_with(|| left.provider().cmp(&right.provider()))
                .then_with(|| left.rank().cmp(&right.rank()))
                .then_with(|| left.published().cmp(&right.published()))
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
            published: current.published_model_id.clone(),
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
            published: current.published_model_id.clone(),
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
            published: current.published_model_id.clone(),
            fields: metadata_fields,
        });
    }
    if previous.price != current.price {
        changes.push(CatalogChange::PriceChanged {
            model: model.clone(),
            provider: current.provider.clone(),
            published: current.published_model_id.clone(),
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

impl Admission {
    /// The content that is now active. Every arm has one: an admission that did
    /// not leave content active is not an admission.
    pub const fn content_id(&self) -> CatalogContentId {
        match self {
            Self::Unchanged { content_id }
            | Self::Updated { content_id, .. }
            | Self::Initial { content_id } => *content_id,
        }
    }
}

/// What a refresh that produced no error did to the catalogue.
///
/// A refresh can fail without an error to log: a source that answers "not
/// modified" where nothing was ever imported has refused the import while
/// reporting success. Naming that outcome, and carrying the [`Refusal`] with it,
/// is what keeps a caller's refusal counter agreeing with the catalogue's own —
/// recording a reason from the error branch alone would count the run without
/// ever naming this reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refreshed {
    /// The refresh advanced the catalogue, or confirmed what was already active.
    Admitted(Admission),
    /// The refresh left the catalogue where it was, for this bounded reason.
    Refused(Refusal),
}

impl Refreshed {
    /// The admission, when the refresh advanced or confirmed the catalogue.
    pub const fn admission(&self) -> Option<&Admission> {
        match self {
            Self::Admitted(admission) => Some(admission),
            Self::Refused(_) => None,
        }
    }

    /// The refusal to record, when the refresh advanced nothing.
    pub const fn refusal(&self) -> Option<&Refusal> {
        match self {
            Self::Admitted(_) => None,
            Self::Refused(refusal) => Some(refusal),
        }
    }
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
/// A scheduler is still not this slice's business, but the *observability* of a
/// refusal no longer waits for one: this type counts consecutive refusals and
/// keeps the last one's bounded [`Refusal`], and [`LastKnownGoodCatalog::report`]
/// projects both — with the active snapshot's [`SourceSnapshot::fetched_at`] as
/// an age and its [`CatalogContentId`] — into a [`CatalogReport`] that a metric
/// exporter, an alert, and the operator status surface all read. For that age to
/// mean "last confirmed current" rather than "last changed", an unchanged answer
/// is recorded through [`LastKnownGoodCatalog::record_unchanged`], which also
/// takes the validators the `304` itself stated. A scheduler can act on the
/// categories too rather than retrying every failure alike:
/// [`CatalogError::Unavailable`] is worth retrying, while
/// [`CatalogError::Misconfigured`], [`CatalogError::Denied`] and
/// [`CatalogError::Invalid`] are not — retrying them only buries an operator's
/// own misconfiguration under "upstream is down".
///
/// Staleness degrades metadata quality only: no enablement, admission, or
/// billing decision reads this snapshot, so a stale catalogue is never an
/// outage, and nothing here is wired into readiness.
#[derive(Debug, Default)]
pub struct LastKnownGoodCatalog {
    active: Option<CatalogSnapshot>,
    consecutive_refusals: u32,
    last_refusal: Option<Refusal>,
    /// Counts from the most recent content-changing import. Kept separately
    /// from the active snapshot because a refusal does not erase the last
    /// successful import's classification.
    last_diff: Option<CatalogDiffCounts>,
}

impl LastKnownGoodCatalog {
    pub const fn new() -> Self {
        Self {
            active: None,
            consecutive_refusals: 0,
            last_refusal: None,
            last_diff: None,
        }
    }

    /// The state a store held, adopted as this process's own.
    ///
    /// Not [`admit`](Self::admit) plus a loop of
    /// [`record_refusal`](Self::record_refusal): the refusal run is a count that
    /// was already established elsewhere, and replaying it as events would both
    /// be a lie about what this process observed and cost one call per refusal
    /// for a deployment that has been failing for a week. Restoring is the one
    /// operation that may set the counters directly, and it is what makes a
    /// restarted replica report the staleness the deployment actually has rather
    /// than a fresh, healthy catalogue that has simply forgotten.
    ///
    /// `active` carries its own confirmation time in `source.fetched_at` — the
    /// store records when the content was last confirmed current, which is what
    /// age means here — so nothing is re-stamped to boot.
    pub fn restored(
        active: Option<CatalogSnapshot>,
        consecutive_refusals: u32,
        last_refusal: Option<Refusal>,
    ) -> Self {
        Self {
            active,
            consecutive_refusals,
            last_refusal,
            last_diff: None,
        }
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

    /// Whether an unchanged answer was actually asked for.
    ///
    /// A `304` is only evidence about held content when a validator went out to
    /// be checked against, so the question is what the request carried — not
    /// what the catalogue happens to hold. Content admitted without validators
    /// (a payload that stated none, the compiled-in seed) leaves nothing to ask
    /// conditionally with, and a caller may also fetch unconditionally while
    /// holding a perfectly good `ETag`; in both cases an answer of "not
    /// modified" confirms nothing, and reading held state instead of
    /// `asked_with` would credit the second case as confirmation.
    ///
    /// The validators such an answer states are discarded along with it, rather
    /// than kept for the next request: their only provenance is the answer being
    /// refused. Recording one would make the following request conditional on a
    /// token an intermediary can keep matching, leaving the catalogue on content
    /// it never received with every signal reading confirmed. A full response
    /// establishes validators legitimately, and this state ends at the first one.
    /// Public because durable state has to answer it too: a store that recorded
    /// a confirmation this holder is about to refuse would be the one place a
    /// deployment's staleness could read healthier than it is.
    pub fn can_confirm_unchanged(&self, asked_with: Option<&SourceValidators>) -> bool {
        self.active.is_some() && asked_with.is_some_and(|validators| !validators.is_empty())
    }

    /// Make `snapshot`'s content active, classifying what that did.
    ///
    /// Classification reads the content's own [`CatalogContent::content_id`],
    /// not the id recorded beside it on the [`SourceSnapshot`]: the provenance is
    /// a plain struct anyone may build, so a snapshot can carry an id that is not
    /// its content's, and admitting content while reporting `Unchanged` is the one
    /// outcome this type exists to prevent. The two agree for anything
    /// [`source_snapshot`] built.
    /// Validators are replaced wholesale only when the content is: a full answer
    /// whose content turns out to be the content already active is the `304`
    /// case with a body, so its validators are carried over the held ones by
    /// [`SourceValidators::carry_over`] rather than replacing them — an
    /// intermediary that serves identical bytes without an `ETag` must not cost
    /// the tag that still describes them.
    ///
    /// The snapshot keeps the `fetched_at` it arrived with, which is a retrieval
    /// time its *source* stated. Age is how long ago this process confirmed the
    /// content, so anything importing on this process's behalf — a scheduler, or a
    /// boot path seeding from
    /// [`seed_snapshot`](super::models_dev::seed_snapshot), whose fixture states
    /// the day it was cut — wants [`admit_as_of`](Self::admit_as_of) instead.
    pub fn admit(&mut self, mut snapshot: CatalogSnapshot) -> Admission {
        let content_id = snapshot.content.content_id();
        let admission = match self.active.as_ref() {
            None => Admission::Initial { content_id },
            Some(active) if active.content.content_id() == content_id => {
                let mut held = active.source.validators.clone();
                held.carry_over(std::mem::take(&mut snapshot.source.validators));
                snapshot.source.validators = held;
                Admission::Unchanged { content_id }
            }
            Some(active) => Admission::Updated {
                content_id,
                diff: snapshot.content.diff(&active.content),
            },
        };
        if let Admission::Updated { diff, .. } = &admission {
            self.last_diff = Some(diff.counts());
        }
        self.active = Some(snapshot);
        self.consecutive_refusals = 0;
        self.last_refusal = None;
        admission
    }

    /// Admit `snapshot` as content this process confirmed at `checked_at`.
    ///
    /// The stamping [`record_refresh`](Self::record_refresh) does, available to
    /// the paths that never see a [`CatalogRefresh`] — boot-time seeding is the
    /// one that exists — so "a freshly imported catalogue reads as fresh" holds
    /// wherever content becomes active, and not only where a refresh drove it.
    pub fn admit_as_of(
        &mut self,
        mut snapshot: CatalogSnapshot,
        checked_at: SystemTime,
    ) -> Admission {
        snapshot.source.fetched_at = checked_at;
        self.admit(snapshot)
    }

    /// Record that the source answered [`CatalogRefresh::Unchanged`], so the
    /// next conditional request carries what it answered with.
    ///
    /// A `304` may state validators that are not the ones sent — a refreshed
    /// `Last-Modified`, or an `ETag` an intermediary rewrote — and they describe
    /// the content already held, so provenance moves while the content and its
    /// identity do not. `checked_at` advances too: an unchanged answer is
    /// evidence the held content is current as of then, which is what an active
    /// snapshot's age means to an operator.
    ///
    /// A validator the answer does not state is *not* a validator withdrawn: a
    /// `304` only SHOULD repeat them and intermediaries drop them, so an
    /// unstated one keeps the held value. Overwriting it with nothing would
    /// leave nothing to ask conditionally with, turning every later refresh into
    /// a full transfer of the whole document.
    ///
    /// Returns whether anything was recorded; there is nothing to move before a
    /// first import.
    pub fn record_unchanged(
        &mut self,
        validators: SourceValidators,
        checked_at: SystemTime,
    ) -> bool {
        let Some(active) = self.active.as_mut() else {
            return false;
        };
        active.source.validators.carry_over(validators);
        active.source.fetched_at = checked_at;
        self.consecutive_refusals = 0;
        self.last_refusal = None;
        true
    }

    /// Admit a parse result, leaving the active snapshot untouched on failure.
    ///
    /// A failure is also *counted* here, by its bounded [`Refusal`], because this
    /// is the one place that sees both the refusal and the run of refusals before
    /// it. The error itself is returned unchanged, so a caller still logs the
    /// typed detail — pointer included — that a metric may not carry.
    pub fn admit_result<E: Refusable>(
        &mut self,
        parsed: Result<CatalogSnapshot, E>,
    ) -> Result<Admission, (E, Option<&CatalogSnapshot>)> {
        match parsed {
            Ok(snapshot) => Ok(self.admit(snapshot)),
            Err(error) => {
                self.record_refusal(error.refusal());
                Err((error, self.active.as_ref()))
            }
        }
    }

    /// Record a whole refresh — the one entry point that keeps the run of
    /// refusals honest whichever way the refresh ended.
    ///
    /// [`admit_result`](Self::admit_result) only sees imports that got as far as
    /// a parse, and a refresh can fail before that (transport, an oversized
    /// body, a URL that serves no catalogue) or succeed without one
    /// ([`CatalogRefresh::Unchanged`]). Routing every outcome through here is
    /// what makes `consecutive_refusals` a property of the catalogue rather than
    /// of a caller's diligence: a fetch failure counts, and a confirmed
    /// unchanged answer ends the run and ages the active snapshot forward.
    ///
    /// `asked_with` is what the request that produced this answer actually
    /// carried — [`validators`](Self::validators) for a conditional refresh,
    /// `None` for an unconditional one — because whether a `304` confirms
    /// anything is a property of the question, not of what happens to be held.
    ///
    /// [`Refreshed::Refused`] is the one odd answer: a `304` nothing asked for.
    /// That is any unchanged answer no validator went out with — before a first
    /// import there is no content to confirm, content held without validators
    /// (a payload that stated none, the compiled-in seed) has nothing to ask
    /// conditionally with, and a caller may simply have fetched unconditionally
    /// while holding a good one, so the answer is not evidence about the held
    /// content in any of those cases. There is nothing to admit
    /// and nothing to age, but the import did not advance the catalogue either,
    /// so it counts as an
    /// [`RefusalReason::UnsolicitedUnchanged`] refusal rather than passing
    /// silently — otherwise an intermediary answering `304` to every
    /// unconditional request would leave the catalogue empty with every signal
    /// at rest. The reason is its own arm because no `CatalogError` was ever
    /// produced, so the runbook's pointer-in-the-log step has nothing to offer
    /// and the label itself has to say why. It is carried in the success value
    /// rather than counted silently so a caller recording a reason from its error
    /// branch alone cannot miss it: every refusal this method counts is also
    /// handed back with a [`Refusal`] to record.
    ///
    /// An admitted snapshot is aged to `checked_at` rather than to the
    /// `fetched_at` its source stated: age means how long ago *this process*
    /// confirmed the content current, and a source may state a retrieval time it
    /// did not perform — the compiled-in seed catalogue states the day it was
    /// cut, which would otherwise read as months stale the moment it is
    /// imported.
    pub fn record_refresh<E: Refusable>(
        &mut self,
        refreshed: Result<CatalogRefresh, E>,
        asked_with: Option<&SourceValidators>,
        checked_at: SystemTime,
    ) -> Result<Refreshed, (E, Option<&CatalogSnapshot>)> {
        match refreshed {
            Ok(CatalogRefresh::Unchanged { validators }) => {
                if !self.can_confirm_unchanged(asked_with) {
                    let refusal = Refusal::new(RefusalReason::UnsolicitedUnchanged);
                    self.record_refusal(refusal.clone());
                    return Ok(Refreshed::Refused(refusal));
                }
                let confirmed = self.record_unchanged(validators, checked_at);
                debug_assert!(confirmed, "a confirmable answer has an active snapshot");
                Ok(Refreshed::Admitted(Admission::Unchanged {
                    content_id: self
                        .active
                        .as_ref()
                        .expect("an unchanged answer was confirmed against an active snapshot")
                        .content
                        .content_id(),
                }))
            }
            Ok(CatalogRefresh::Updated { snapshot, .. }) => {
                Ok(Refreshed::Admitted(self.admit_as_of(*snapshot, checked_at)))
            }
            Err(error) => {
                self.record_refusal(error.refusal());
                Err((error, self.active.as_ref()))
            }
        }
    }

    /// Count a refusal that happened before there was anything to admit — a
    /// transport failure, an oversized body — so "this catalogue has stopped
    /// advancing" does not depend on how far the import got.
    pub fn record_refusal(&mut self, refusal: Refusal) {
        self.consecutive_refusals = self.consecutive_refusals.saturating_add(1);
        self.last_refusal = Some(refusal);
    }

    /// How many imports in a row have been refused. Zero after any admitted or
    /// confirmed-unchanged import.
    pub const fn consecutive_refusals(&self) -> u32 {
        self.consecutive_refusals
    }

    /// The most recent refusal, still holding its pointer for a log line.
    pub const fn last_refusal(&self) -> Option<&Refusal> {
        self.last_refusal.as_ref()
    }

    /// What an operator is told about this catalogue, as of `now`.
    ///
    /// One projection for three consumers — metrics, alerts, the status response
    /// — so a refusal cannot be visible on a dashboard and absent from the
    /// surface an operator reads during the page.
    pub fn report(&self, now: SystemTime) -> CatalogReport {
        CatalogReport {
            active: self.active.as_ref().map(|snapshot| ActiveCatalog {
                content_id: snapshot.content.content_id(),
                fetched_at: snapshot.source.fetched_at,
                age: now
                    .duration_since(snapshot.source.fetched_at)
                    .unwrap_or_default(),
            }),
            consecutive_refusals: self.consecutive_refusals,
            last_refusal: self.last_refusal.as_ref().map(Refusal::reason),
            last_diff: self.last_diff,
        }
    }
}

/// How many consecutive refusals make a catalogue's staleness worth waking
/// someone for.
///
/// Two, not one: a single refused import is an upstream having a bad minute and
/// the previous content is still active, while a second consecutive refusal is
/// the first evidence that the catalogue has stopped advancing across intervals
/// rather than within one. The threshold is a count rather than a duration so it
/// means the same thing whatever cadence a scheduler eventually runs at.
pub const PERSISTENT_REFUSAL_THRESHOLD: u32 = 2;

/// The active snapshot, as an operator sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveCatalog {
    pub content_id: CatalogContentId,
    pub fetched_at: SystemTime,
    /// How long ago the active content was last confirmed current — admitted, or
    /// answered `304`. This is what grows while imports are being refused.
    pub age: Duration,
}

/// What is operationally true about a catalogue: what is active, how old it is,
/// and whether imports are being refused.
///
/// Bounded by construction. The only unbounded value a catalogue could offer —
/// the raw payload, the source URL, an error message, a JSON Pointer — is not
/// here: the identity is a digest this process computed, and the reason is a
/// [`RefusalReason`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogReport {
    /// `None` before a first successful import, which is not a refusal: a
    /// deployment that has never imported has nothing stale.
    pub active: Option<ActiveCatalog>,
    pub consecutive_refusals: u32,
    pub last_refusal: Option<RefusalReason>,
    /// The bounded semantic counts from the most recent content-changing
    /// import. A refusal leaves this intact because the last-known-good
    /// catalogue and its last successful classification remain active.
    pub last_diff: Option<CatalogDiffCounts>,
}

impl CatalogReport {
    /// Whether refusals have persisted across more than one import attempt. The
    /// alert condition, evaluated on data rather than restated by each consumer.
    pub const fn persistent_refusal(&self) -> bool {
        self.consecutive_refusals >= PERSISTENT_REFUSAL_THRESHOLD
    }

    /// The age of the active content, when there is any.
    pub fn active_age(&self) -> Option<Duration> {
        self.active.map(|active| active.age)
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
            capabilities: [
                ModelCapability::ToolCall,
                ModelCapability::Reasoning,
                ModelCapability::Attachment,
            ]
            .into_iter()
            .collect(),
            input_modalities: [Modality::Text, Modality::Image, Modality::Pdf]
                .into_iter()
                .collect(),
            output_modalities: [Modality::Text, Modality::Audio].into_iter().collect(),
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
        assert_eq!(
            diff.changes()[0].published(),
            Some("gpt-4o-latest"),
            "and the report names which of the provider's ids got dearer"
        );
    }

    /// A change to an offering names the id it is published under, so a provider
    /// repricing both of its aliases of one model is two changes an operator can
    /// act on separately rather than two identical-looking reports.
    #[test]
    fn a_change_to_one_alias_is_distinguishable_from_a_change_to_the_other() {
        let mut first = offering("openai", "gpt-4o", Some(price(1, 2)));
        first.published_model_id = "gpt-4o-2024".to_owned();
        let mut second = offering("openai", "gpt-4o", Some(price(1, 2)));
        second.published_model_id = "gpt-4o-latest".to_owned();
        let before = content(vec![first.clone(), second.clone()]);

        let mut first_dearer = first;
        first_dearer.price = Some(price(1, 3));
        let mut second_deprecated = second;
        second_deprecated.facts.lifecycle = ModelLifecycle::Deprecated;
        let after = content(vec![first_dearer, second_deprecated]);

        let diff = after.diff(&before);
        let named: Vec<Option<&str>> = diff
            .changes()
            .iter()
            .map(CatalogChange::published)
            .collect();
        assert_eq!(
            named,
            [Some("gpt-4o-latest"), Some("gpt-4o-2024")],
            "each report names its own callable id"
        );
        assert!(matches!(
            diff.changes()[0],
            CatalogChange::LifecycleChanged { .. }
        ));
        assert!(matches!(
            diff.changes()[1],
            CatalogChange::PriceChanged { .. }
        ));
    }

    /// Content identity is over stated values, so two catalogues differing only
    /// in where their values were read from, or in the derived override record,
    /// are one identity — otherwise an `Admission::Updated` could carry an empty
    /// diff, which is the one thing the identity exists to rule out.
    #[test]
    fn provenance_is_not_content() {
        let stated = offering("openai", "gpt-4o", Some(price(1, 2)));
        let mut elsewhere = stated.clone();
        elsewhere.pointer = JsonPointer::new("").child("somewhere").child("else");
        elsewhere.overrides = vec![(ModelField::DisplayName, JsonPointer::new("/made/up"))];

        let before = content(vec![stated]);
        let after = content(vec![elsewhere]);
        assert_eq!(before.content_id(), after.content_id());
        assert!(after.diff(&before).is_empty());
    }

    /// Every catalogue record is built in the order it encodes in, so a record
    /// held in memory *equals* the same record read back out of storage rather
    /// than merely checksumming the same — otherwise comparing a fresh
    /// catalogue against a stored one would differ on field order alone.
    #[test]
    fn a_catalogue_record_equals_its_own_round_trip() {
        // Several capabilities and modalities, and in an order that is not the
        // order they encode in: a set of one member round-trips whatever the
        // constructor does with it, so it would pin nothing.
        let mut described = offering("openai", "gpt-4o", Some(price(1, 2)));
        described.facts.capabilities = [
            ModelCapability::ToolCall,
            ModelCapability::Attachment,
            ModelCapability::Reasoning,
        ]
        .into_iter()
        .collect();
        described.facts.input_modalities = [Modality::Text, Modality::Image, Modality::Audio]
            .into_iter()
            .collect();
        let content = content(vec![described]);
        let serializer = crate::desired_state::canonical::SerializerVersion::default();
        for record in [
            content.providers()[0].canonical(),
            content.models()[0].canonical(),
            content.models()[0].offerings[0].canonical(),
            content.canonical(),
        ] {
            let bytes = record.to_canonical_bytes().expect("canonical bytes");
            assert_eq!(
                serializer.decode(&bytes).expect("decode"),
                record,
                "a record built here must be the record storage returns"
            );
        }
    }

    /// An offering that arrives or leaves is named by the id a request would
    /// have used: an alias moving beside a sibling alias would otherwise be a
    /// report indistinguishable from the sibling's.
    #[test]
    fn an_offering_that_comes_or_goes_names_the_id_callers_would_have_sent() {
        let held = offering("openai", "gpt-4o", None);
        let mut first = held.clone();
        first.published_model_id = "gpt-4o-latest".to_owned();
        let mut second = held.clone();
        second.published_model_id = "gpt-4o-2024".to_owned();

        let before = content(vec![held.clone()]);
        let after = content(vec![held.clone(), first, second]);
        let model = ModelId::parse("gpt-4o").expect("fixture id");
        let provider = ProviderId::parse("openai").expect("fixture id");

        let added = after.diff(&before);
        assert_eq!(
            added.changes(),
            [
                CatalogChange::OfferingAdded {
                    model: model.clone(),
                    provider: provider.clone(),
                    published: "gpt-4o-2024".to_owned(),
                },
                CatalogChange::OfferingAdded {
                    model: model.clone(),
                    provider: provider.clone(),
                    published: "gpt-4o-latest".to_owned(),
                },
            ],
            "two aliases arriving are two distinguishable changes"
        );

        let removed = before.diff(&after);
        assert_eq!(
            removed.changes(),
            [
                CatalogChange::OfferingRemoved {
                    model: model.clone(),
                    provider: provider.clone(),
                    published: "gpt-4o-2024".to_owned(),
                },
                CatalogChange::OfferingRemoved {
                    model,
                    provider,
                    published: "gpt-4o-latest".to_owned(),
                },
            ],
            "and withdrawing them names which callable id went"
        );
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

    /// A refresh answer carrying `snapshot`, with the bytes the fixture states it
    /// was parsed from.
    fn refreshed(snapshot: CatalogSnapshot) -> CatalogRefresh {
        CatalogRefresh::Updated {
            snapshot: Box::new(snapshot),
            payload: RawPayload::new(&b"{}"[..]),
        }
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

    /// Provenance is a plain struct, so the id recorded beside content is a
    /// claim: admitting different content while reporting `Unchanged` would apply
    /// a catalogue change with nothing for an operator to review.
    #[test]
    fn admission_classifies_the_content_it_admits_not_the_id_beside_it() {
        let mut catalogue = LastKnownGoodCatalog::new();
        let before = content(vec![offering("openai", "gpt-4o", None)]);
        let stale_id = before.content_id();
        assert_eq!(
            catalogue.admit(snapshot(before, SourceValidators::etag("\"one\""))),
            Admission::Initial {
                content_id: stale_id
            }
        );

        let after = content(vec![offering(
            "openai",
            "gpt-4o",
            Some(price(2_000_000_000, 10_000_000_000)),
        )]);
        let content_id = after.content_id();
        let mislabelled = CatalogSnapshot {
            source: SourceSnapshot {
                content_id: stale_id,
                ..snapshot(after.clone(), SourceValidators::etag("\"two\"")).source
            },
            content: after,
        };

        let Admission::Updated {
            content_id: id,
            diff,
        } = catalogue.admit(mislabelled)
        else {
            panic!("content that differs is an update whatever the record beside it says");
        };
        assert_eq!(id, content_id);
        assert!(diff.has_price_changes());
        assert_eq!(
            catalogue
                .report(SystemTime::UNIX_EPOCH)
                .last_diff
                .expect("the report keeps bounded diff counts")
                .prices_changed,
            1
        );
        catalogue.record_refusal(Refusal::new(RefusalReason::Unreachable));
        assert_eq!(
            catalogue
                .report(SystemTime::UNIX_EPOCH)
                .last_diff
                .expect("a refusal keeps the last successful classification")
                .prices_changed,
            1
        );
        assert_eq!(
            catalogue.content().map(CatalogContent::content_id),
            Some(content_id),
            "and the id reported is the id of what became active"
        );
    }

    /// A `304` may answer with validators that are not the ones sent, and they
    /// describe the content already held — so recording them must move
    /// provenance without touching the content or its identity.
    #[test]
    fn an_unchanged_answer_moves_the_validators_without_moving_the_content() {
        let mut catalogue = LastKnownGoodCatalog::new();
        assert!(
            !catalogue.record_unchanged(SourceValidators::etag("\"one\""), SystemTime::UNIX_EPOCH),
            "there is nothing to say is unchanged before a first import"
        );

        let held = content(vec![offering("openai", "gpt-4o", None)]);
        let content_id = held.content_id();
        catalogue.admit(snapshot(held, SourceValidators::etag("\"one\"")));

        let checked_at = SystemTime::UNIX_EPOCH + Duration::from_secs(3_600);
        assert!(catalogue.record_unchanged(SourceValidators::etag("\"two\""), checked_at));
        assert_eq!(
            catalogue.validators(),
            Some(&SourceValidators::etag("\"two\"")),
            "the next conditional request asks with what the source last answered"
        );
        let active = catalogue.active().expect("the content stayed active");
        assert_eq!(active.source.fetched_at, checked_at);
        assert_eq!(
            active.content.content_id(),
            content_id,
            "an unchanged answer is not new content"
        );
        assert_eq!(active.source.content_id, content_id);

        assert!(catalogue.record_unchanged(SourceValidators::default(), checked_at));
        assert_eq!(
            catalogue.validators(),
            Some(&SourceValidators::etag("\"two\"")),
            "an answer that repeats no tag has not withdrawn one: dropping it \
             would leave nothing to ask conditionally with, and every later \
             refresh would transfer the whole document"
        );

        let last_modified = SourceValidators {
            etag: None,
            last_modified: Some(HttpDate("Wed, 21 Oct 2015 07:28:00 GMT".to_owned())),
        };
        assert!(catalogue.record_unchanged(last_modified, checked_at));
        assert_eq!(
            catalogue.validators(),
            Some(&SourceValidators {
                etag: Some(ETag("\"two\"".to_owned())),
                last_modified: Some(HttpDate("Wed, 21 Oct 2015 07:28:00 GMT".to_owned())),
            }),
            "and a partial answer states the one it carries without clearing the other"
        );
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
            .insert(ModelCapability::StructuredOutput);
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
                    published: "gpt-4o".to_owned(),
                    fields: vec![ModelField::Endpoint],
                },
            ),
            (
                "the id a request must send",
                republished,
                CatalogChange::MetadataChanged {
                    model: ModelId::parse("gpt-4o").expect("fixture id"),
                    provider: ProviderId::parse("openai").expect("fixture id"),
                    // The id in effect after the change, which is the one
                    // requests must use from here on.
                    published: "gpt-4o-2024-11-20".to_owned(),
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

    /// A document that kept its model records and lost its providers section is
    /// a catalogue nothing can be routed or priced from. It must be refused, so
    /// that admission cannot hand it the place a working catalogue holds.
    #[test]
    fn a_catalogue_no_provider_offers_is_refused() {
        let neutral = CatalogModelEntry {
            id: ModelId::parse("gpt-4o").expect("id"),
            neutral: None,
            offerings: Vec::new(),
        };
        assert_eq!(
            CatalogContent::new(Vec::new(), vec![neutral.clone()]),
            Err(CatalogContentError::Unoffered { models: 1 })
        );
        assert_eq!(
            CatalogContent::new(vec![provider("openai")], vec![neutral.clone()]),
            Err(CatalogContentError::Unoffered { models: 1 })
        );

        let held = CatalogContent::new(
            vec![provider("openai")],
            vec![CatalogModelEntry {
                offerings: vec![offering("openai", "gpt-4o", None)],
                ..neutral.clone()
            }],
        )
        .expect("an offered catalogue");
        let mut active = LastKnownGoodCatalog::default();
        active.admit(snapshot(held.clone(), SourceValidators::etag("\"one\"")));
        let refused: Result<CatalogSnapshot, CatalogContentError> =
            CatalogContent::new(Vec::new(), vec![neutral])
                .map(|content| snapshot(content, SourceValidators::etag("\"two\"")));
        assert!(active.admit_result(refused).is_err());
        assert_eq!(
            active.active().map(|active| active.content.content_id()),
            Some(held.content_id()),
            "a catalogue no provider offers replaced the last known good one"
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
    fn a_refusal_quotes_a_hostile_identifier_only_in_excerpt() {
        let hostile = "m".repeat(4 * 1024 * 1024);
        let refusal = ModelId::parse(&hostile)
            .expect_err("an over-long id")
            .to_string();

        assert!(
            refusal.len() < 256,
            "a refusal an upstream can size is a log amplifier: {} bytes",
            refusal.len()
        );
        assert!(
            refusal.contains("mmmm") && refusal.contains(&hostile.len().to_string()),
            "the excerpt has to stay recognizable and state the true length: {refusal}"
        );

        let multibyte = format!("{}€", "é".repeat(200));
        let refusal = ModelId::parse(&multibyte)
            .expect_err("an over-long id")
            .to_string();
        assert!(
            refusal.len() < 256 && refusal.contains('é'),
            "an excerpt cuts on a character boundary: {refusal}"
        );

        assert_eq!(
            excerpt("openai/gpt-4o"),
            "openai/gpt-4o",
            "an identifier of a plausible length is quoted whole"
        );

        // A list of accepted identifiers is bounded in its length as well as in
        // each element: how many of them there are is the payload's choice.
        let many: Vec<String> = (0..100_000).map(|index| format!("a{index}/x")).collect();
        let listed = excerpt_list(&many);
        assert!(
            listed.len() < 256 && listed.ends_with("and 99992 more"),
            "a candidate list an upstream can size is the same amplifier: {} bytes",
            listed.len()
        );
        assert_eq!(
            excerpt_list(&["alpha/m-1".to_owned(), "beta/m-1".to_owned()]),
            "`alpha/m-1`, `beta/m-1`",
            "a list an operator can read is quoted whole"
        );
    }

    /// A bound that eats the location is a bound that eats the diagnosis.
    #[test]
    fn a_bounded_message_keeps_the_position_it_ends_with() {
        let located = format!(
            "invalid type: string \"{}\", expected u64 at line 1234 column 12",
            "x".repeat(4 * 1024 * 1024)
        );
        let bounded = excerpt_located(&located);

        assert!(
            bounded.len() < 256,
            "an upstream still cannot size the line: {} bytes",
            bounded.len()
        );
        assert!(
            bounded.starts_with("invalid type: string")
                && bounded.contains(&located.len().to_string()),
            "the head and the true length survive: {bounded}"
        );
        assert!(
            bounded.ends_with("at line 1234 column 12"),
            "the only locator these two refusals carry survives: {bounded}"
        );

        let multibyte = format!("{}€ at line 1 column 9", "é".repeat(400));
        let bounded = excerpt_located(&multibyte);
        assert!(
            bounded.len() < 256 && bounded.ends_with("at line 1 column 9"),
            "both cuts land on a character boundary: {bounded}"
        );

        assert_eq!(
            excerpt_located("expected value at line 1 column 1"),
            "expected value at line 1 column 1",
            "a message an operator can read is quoted whole"
        );
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

        let rejected: Result<CatalogSnapshot, CatalogError> = Err(CatalogError::Invalid {
            backend: "test",
            refusal: Refusal::new(RefusalReason::Schema),
            message: "schema drift".to_owned(),
        });
        let (error, active) = catalogue
            .admit_result(rejected)
            .expect_err("a drifted payload is refused");
        assert_eq!(error.refused_by().reason(), RefusalReason::Schema);
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

        // An answer that states none keeps the held one: it still describes the
        // content that stayed active, and dropping it would make every later
        // refresh transfer the whole document again.
        let admission = catalogue.admit(snapshot(content.clone(), SourceValidators::default()));
        assert_eq!(
            admission,
            Admission::Unchanged {
                content_id: content.content_id()
            }
        );
        assert_eq!(
            catalogue.validators(),
            Some(&SourceValidators::etag("\"two\"")),
            "an intermediary stripping the tag must not cost the tag"
        );

        let updated = content_with_price(3);
        let admission = catalogue.admit(snapshot(updated, SourceValidators::default()));
        assert!(matches!(admission, Admission::Updated { diff, .. } if diff.has_price_changes()));
        // New content, on the other hand, is described by the validators it
        // arrived with and by nothing that came before it.
        assert_eq!(catalogue.validators(), Some(&SourceValidators::default()));
    }

    fn content_with_price(input: u64) -> CatalogContent {
        content(vec![offering("openai", "gpt-4o", Some(price(input, 2)))])
    }

    #[tokio::test]
    async fn a_first_refresh_returns_metadata_with_validators() {
        let source = InMemoryCatalog::with_models(&[("openai", "gpt-4o")], "v1");
        let CatalogRefresh::Updated { snapshot, .. } = source.refresh(None).await.expect("refresh")
        else {
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
        let CatalogRefresh::Updated { snapshot, .. } = source
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
        let CatalogRefresh::Updated { snapshot, .. } = source.refresh(None).await.unwrap() else {
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
            Ok(CatalogRefresh::Updated { .. })
        ));
    }

    /// The vocabulary is duplicated as strings so the metric catalogue can name
    /// it in a const; the duplicate has to stay exactly the enum.
    #[test]
    fn the_refusal_vocabulary_and_its_string_duplicate_agree() {
        let reasons: Vec<&str> = RefusalReason::ALL
            .iter()
            .map(|reason| reason.as_str())
            .collect();
        assert_eq!(REFUSAL_REASONS, reasons.as_slice());
        let unique: BTreeSet<&str> = reasons.iter().copied().collect();
        assert_eq!(unique.len(), reasons.len(), "a reason is named twice");
    }

    /// A short id is what an operator surface may carry: fixed-width, and a
    /// prefix of the digest an import logged rather than a re-derivation of it.
    #[test]
    fn a_short_content_id_is_a_fixed_width_prefix_of_its_digest() {
        let content = content(vec![offering("openai", "gpt-4o", Some(price(1, 2)))]);
        let short = content.content_id().short();
        assert_eq!(short.len(), CONTENT_ID_SHORT_HEX);
        assert!(short.chars().all(|c| c.is_ascii_hexdigit()));
        assert!(content.content_id().checksum().to_string().contains(&short));
        assert_eq!(
            short,
            content.content_id().short(),
            "the same content is the same id"
        );
    }

    /// The point of the whole slice: refusals are counted and named while the
    /// content an operator is serving stays exactly where it was.
    #[test]
    fn consecutive_refusals_accumulate_without_disturbing_what_is_active() {
        let mut catalogue = LastKnownGoodCatalog::new();
        let good = snapshot(
            content(vec![offering("openai", "gpt-4o", Some(price(1, 2)))]),
            SourceValidators::etag("\"one\""),
        );
        catalogue.admit(good.clone());
        let report = catalogue.report(SystemTime::UNIX_EPOCH);
        assert_eq!(report.consecutive_refusals, 0);
        assert!(!report.persistent_refusal());

        catalogue.record_refusal(Refusal::new(RefusalReason::Unreachable));
        let report = catalogue.report(SystemTime::UNIX_EPOCH);
        assert_eq!(report.consecutive_refusals, 1);
        assert_eq!(report.last_refusal, Some(RefusalReason::Unreachable));
        assert!(
            !report.persistent_refusal(),
            "one bad minute upstream is not a page"
        );

        catalogue.record_refusal(Refusal::at(
            RefusalReason::Schema,
            JsonPointer::new("").child("models"),
        ));
        let report = catalogue.report(SystemTime::UNIX_EPOCH);
        assert_eq!(report.consecutive_refusals, PERSISTENT_REFUSAL_THRESHOLD);
        assert_eq!(report.last_refusal, Some(RefusalReason::Schema));
        assert!(
            report.persistent_refusal(),
            "a second refusal is the catalogue no longer advancing"
        );
        assert_eq!(
            report.active.map(|active| active.content_id),
            Some(good.content.content_id()),
            "and none of it changed what is being served"
        );
        assert_eq!(
            catalogue.last_refusal().and_then(Refusal::pointer),
            Some(&JsonPointer::new("").child("models")),
            "the pointer survives for the log line that a metric may not carry"
        );
    }

    /// A run of refusals is a run, not a tally: anything that proves the
    /// catalogue is current again ends it.
    #[test]
    fn a_confirmed_import_ends_the_run_of_refusals() {
        let mut catalogue = LastKnownGoodCatalog::new();
        catalogue.admit(snapshot(
            content(vec![offering("openai", "gpt-4o", Some(price(1, 2)))]),
            SourceValidators::etag("\"one\""),
        ));
        catalogue.record_refusal(Refusal::new(RefusalReason::NotJson));
        catalogue.record_refusal(Refusal::new(RefusalReason::NotJson));
        assert!(
            catalogue
                .report(SystemTime::UNIX_EPOCH)
                .persistent_refusal()
        );

        let later = SystemTime::UNIX_EPOCH + Duration::from_secs(600);
        assert!(catalogue.record_unchanged(SourceValidators::etag("\"one\""), later));
        let report = catalogue.report(later);
        assert_eq!(report.consecutive_refusals, 0);
        assert_eq!(report.last_refusal, None);
        assert_eq!(
            report.active_age(),
            Some(Duration::ZERO),
            "a 304 is evidence the held content is current, not merely unchanged"
        );

        catalogue.record_refusal(Refusal::new(RefusalReason::NotJson));
        catalogue.admit(snapshot(
            content(vec![offering("openai", "gpt-4o", Some(price(1, 3)))]),
            SourceValidators::etag("\"two\""),
        ));
        assert_eq!(
            catalogue.report(later).consecutive_refusals,
            0,
            "an admitted import ends the run too"
        );
    }

    /// Age is the operator's answer to "how far behind is this": it is measured
    /// from the active snapshot's `fetched_at`, and it keeps growing across
    /// refusals rather than being reset by the attempt that failed.
    #[test]
    fn active_age_grows_across_refusals_and_never_runs_backwards() {
        let mut catalogue = LastKnownGoodCatalog::new();
        assert_eq!(
            catalogue.report(SystemTime::UNIX_EPOCH).active_age(),
            None,
            "a deployment that never imported has nothing stale"
        );

        let imported_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let mut fresh = snapshot(
            content(vec![offering("openai", "gpt-4o", Some(price(1, 2)))]),
            SourceValidators::etag("\"one\""),
        );
        fresh.source.fetched_at = imported_at;
        catalogue.admit(fresh);

        catalogue.record_refusal(Refusal::new(RefusalReason::Oversized));
        assert_eq!(
            catalogue
                .report(imported_at + Duration::from_secs(3_600))
                .active_age(),
            Some(Duration::from_secs(3_600))
        );
        assert_eq!(
            catalogue
                .report(imported_at - Duration::from_secs(60))
                .active_age(),
            Some(Duration::ZERO),
            "a clock that stepped backwards reads as fresh, never as negative"
        );
    }

    /// Refusing through the admission boundary counts the refusal for free:
    /// a scheduler cannot observe the error and forget the series.
    #[test]
    fn admitting_a_failure_counts_it_by_its_typed_reason() {
        let mut catalogue = LastKnownGoodCatalog::new();
        let refused: Result<CatalogSnapshot, CatalogError> = Err(CatalogError::Invalid {
            backend: "test",
            refusal: Refusal::at(
                RefusalReason::Price,
                JsonPointer::new("").child("cost").child("input"),
            ),
            message: "https://models.dev/api.json: price is not a number".to_owned(),
        });
        let (error, active) = catalogue
            .admit_result(refused)
            .expect_err("a refused import");
        assert!(active.is_none(), "there was nothing to keep active");
        assert_eq!(error.refused_by().reason(), RefusalReason::Price);

        let report = catalogue.report(SystemTime::UNIX_EPOCH);
        assert_eq!(report.consecutive_refusals, 1);
        assert_eq!(report.last_refusal, Some(RefusalReason::Price));
        assert!(
            !report
                .last_refusal
                .expect("a reason")
                .as_str()
                .contains('/'),
            "the reason is a vocabulary word, never the pointer or the URL beside it"
        );
    }

    /// Every way a refresh can end goes through one entry point, so the run of
    /// refusals cannot depend on a scheduler remembering to count a failure that
    /// never reached a parse, or to credit a `304`.
    #[test]
    fn one_entry_point_counts_a_refresh_however_it_ended() {
        let mut catalogue = LastKnownGoodCatalog::new();
        let imported_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let first = catalogue
            .record_refresh::<CatalogError>(
                Ok(refreshed(snapshot(
                    content(vec![offering("openai", "gpt-4o", Some(price(1, 2)))]),
                    SourceValidators::etag("\"one\""),
                ))),
                None,
                imported_at,
            )
            .expect("an admitted import");
        assert!(matches!(
            first,
            Refreshed::Admitted(Admission::Initial { .. })
        ));

        // A fetch that never produced a document to parse still counts.
        let unreachable = CatalogError::unavailable("test", "connection refused".to_owned());
        let (error, active) = catalogue
            .record_refresh(Err(unreachable), None, imported_at)
            .expect_err("a refused refresh");
        assert_eq!(error.refused_by().reason(), RefusalReason::Unreachable);
        assert!(
            active.is_some(),
            "and the last good catalogue keeps serving through it"
        );
        assert_eq!(catalogue.report(imported_at).consecutive_refusals, 1);

        let checked_at = imported_at + Duration::from_secs(600);
        let asked_with = catalogue.validators().cloned().expect("an active snapshot");
        let confirmed = catalogue
            .record_refresh::<CatalogError>(
                Ok(CatalogRefresh::Unchanged {
                    validators: SourceValidators::etag("\"one\""),
                }),
                Some(&asked_with),
                checked_at,
            )
            .expect("a confirmed answer");
        assert!(matches!(
            confirmed,
            Refreshed::Admitted(Admission::Unchanged { .. })
        ));
        let report = catalogue.report(checked_at);
        assert_eq!(report.consecutive_refusals, 0, "a 304 ends the run");
        assert_eq!(report.active_age(), Some(Duration::ZERO));
    }

    /// An unchanged answer to a request that could not have been conditional
    /// confirms content nobody holds. Nothing becomes active, so the import has
    /// to leave a mark rather than reading as a quiet success.
    #[test]
    fn an_unchanged_answer_with_nothing_held_is_counted_as_a_refusal() {
        let mut catalogue = LastKnownGoodCatalog::new();
        let checked_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let refreshed = catalogue
            .record_refresh::<CatalogError>(
                Ok(CatalogRefresh::Unchanged {
                    validators: SourceValidators::etag("\"one\""),
                }),
                None,
                checked_at,
            )
            .expect("an answer, not an error");
        assert_eq!(refreshed.admission(), None, "nothing was admitted");
        assert_eq!(
            refreshed.refusal().map(Refusal::reason),
            Some(RefusalReason::UnsolicitedUnchanged),
            "and the caller is handed the reason to record, not just a count"
        );
        let report = catalogue.report(checked_at);
        assert_eq!(report.active, None, "nothing became active");
        assert_eq!(report.consecutive_refusals, 1);
        assert_eq!(
            report.last_refusal,
            Some(RefusalReason::UnsolicitedUnchanged),
            "and says so by name, because no error was produced to log"
        );
    }

    /// Holding content is not the same as having asked about it. A payload that
    /// stated no validators leaves nothing to send conditionally, so the `304`
    /// answering the unconditional request that follows is evidence of nothing
    /// and must not age the content forward or end a run of refusals.
    #[test]
    fn an_unchanged_answer_to_content_held_without_validators_is_a_refusal() {
        let mut catalogue = LastKnownGoodCatalog::new();
        let imported_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        catalogue.admit_as_of(
            snapshot(
                content(vec![offering("openai", "gpt-4o", Some(price(1, 2)))]),
                SourceValidators::default(),
            ),
            imported_at,
        );
        assert!(
            catalogue
                .validators()
                .expect("an active snapshot")
                .is_empty(),
            "nothing to make the next request conditional with"
        );
        catalogue.record_refusal(Refusal::new(RefusalReason::Unreachable));

        let checked_at = imported_at + Duration::from_secs(3_600);
        let refreshed = catalogue
            .record_refresh::<CatalogError>(
                Ok(CatalogRefresh::Unchanged {
                    validators: SourceValidators::default(),
                }),
                catalogue.validators().cloned().as_ref(),
                checked_at,
            )
            .expect("an answer, not an error");
        assert_eq!(
            refreshed.refusal().map(Refusal::reason),
            Some(RefusalReason::UnsolicitedUnchanged),
            "an answer to a question nobody asked confirms nothing"
        );
        let report = catalogue.report(checked_at);
        assert_eq!(
            report.active_age(),
            Some(Duration::from_secs(3_600)),
            "so the active content keeps aging"
        );
        assert_eq!(
            report.consecutive_refusals, 2,
            "and the run continues rather than being cleared"
        );
    }

    /// Whether a `304` confirms anything is a property of the request, not of
    /// what the catalogue holds: a refresh made unconditionally while a good
    /// validator is held is still a question nobody asked, so its answer cannot
    /// buy the content a fresh age or clear a run of refusals.
    #[test]
    fn an_unchanged_answer_to_a_request_that_carried_no_validator_is_a_refusal() {
        let mut catalogue = LastKnownGoodCatalog::new();
        let imported_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        catalogue.admit_as_of(
            snapshot(
                content(vec![offering("openai", "gpt-4o", Some(price(1, 2)))]),
                SourceValidators::etag("\"one\""),
            ),
            imported_at,
        );
        assert!(
            !catalogue
                .validators()
                .expect("an active snapshot")
                .is_empty(),
            "the held state alone would have made this confirmable"
        );
        catalogue.record_refusal(Refusal::new(RefusalReason::Unreachable));

        let checked_at = imported_at + Duration::from_secs(3_600);
        let refreshed = catalogue
            .record_refresh::<CatalogError>(
                Ok(CatalogRefresh::Unchanged {
                    validators: SourceValidators::etag("\"one\""),
                }),
                // The refresh went out unconditionally all the same.
                None,
                checked_at,
            )
            .expect("an answer, not an error");
        assert_eq!(
            refreshed.refusal().map(Refusal::reason),
            Some(RefusalReason::UnsolicitedUnchanged),
            "nothing was sent for the source to have checked against"
        );
        let report = catalogue.report(checked_at);
        assert_eq!(
            report.active_age(),
            Some(Duration::from_secs(3_600)),
            "so the content keeps aging"
        );
        assert_eq!(
            report.consecutive_refusals, 2,
            "and the run continues rather than being cleared"
        );
    }

    /// Age is how long ago *this process* confirmed the content, so an admitted
    /// snapshot is aged to the check rather than to a retrieval time its source
    /// stated. The compiled-in seed states the day it was cut, which would
    /// otherwise read as months stale the moment it is imported.
    #[test]
    fn an_admitted_import_is_aged_from_the_check_and_not_from_what_it_claims() {
        let mut catalogue = LastKnownGoodCatalog::new();
        let checked_at = SystemTime::UNIX_EPOCH + Duration::from_secs(86_400);
        // The fixture states UNIX_EPOCH, a day before this import happened.
        let stated = snapshot(
            content(vec![offering("openai", "gpt-4o", Some(price(1, 2)))]),
            SourceValidators::etag("\"one\""),
        );
        assert_eq!(stated.source.fetched_at, SystemTime::UNIX_EPOCH);
        catalogue
            .record_refresh::<CatalogError>(Ok(refreshed(stated)), None, checked_at)
            .expect("an admitted import");

        let report = catalogue.report(checked_at);
        assert_eq!(
            report.active_age(),
            Some(Duration::ZERO),
            "a fresh import is fresh however old the document says it is"
        );
        assert_eq!(
            report.active.expect("an active catalogue").fetched_at,
            checked_at
        );
    }

    /// The same holds for content that never came from a refresh: the bundled
    /// seed states the day it was cut, and a boot path importing it is confirming
    /// content now, not months ago.
    #[test]
    fn a_seeded_import_is_aged_from_the_import_and_not_from_the_fixture() {
        let mut catalogue = LastKnownGoodCatalog::new();
        let booted_at =
            crate::backends::models_dev::seed_fetched_at() + Duration::from_secs(90 * 86_400);
        let seed = crate::backends::models_dev::seed_snapshot();
        assert_eq!(
            seed.source.fetched_at,
            crate::backends::models_dev::seed_fetched_at()
        );

        assert!(matches!(
            catalogue.admit_as_of(seed, booted_at),
            Admission::Initial { .. }
        ));
        assert_eq!(
            catalogue.report(booted_at).active_age(),
            Some(Duration::ZERO),
            "a seed imported now is as current as this process has confirmed anything"
        );
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
