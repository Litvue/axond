//! The approved price book: what a deployment has *decided* to charge, and the
//! immutable pricing identity a snapshot serves under (#201).
//!
//! The catalogue ([`crate::backends::catalog`], #192) records what an upstream
//! *publishes*. That is an observation, and ADR 0042 is explicit that an
//! observation never becomes a charge: turning one into money is an
//! administrative act, and this module is the shape that act takes. A price book
//! is a resource body like any other — inline, canonical, immutable, versioned by
//! the generic envelope — holding effective-dated rates an operator approved,
//! against a named catalogue snapshot.
//!
//! # Four separations the types enforce
//!
//! **Observed is not approved.** An [`ObservedRate`]
//! parsed out of models.dev cannot be an [`ApprovedRate`] by accident: the only
//! path from one to the other is [`ApprovedRate::approving`], which an operator's
//! mutation calls, and the book records *which* catalogue content
//! ([`CatalogContentId`]) it was approved against. Nothing in the import path can
//! reach this module.
//!
//! **Approved is not billable until it converts exactly.** Sources publish rates
//! as fine as a nano-dollar per million tokens; the runtime bills in
//! micro-dollars ([`ModelPrice`], ADR 0010). The conversion is integer and exact,
//! and a rate that micro-dollars cannot state is *refused*
//! ([`RateRejection::ExcessPrecision`]) rather than rounded — a rounded rate is a
//! billing decision nobody made, and it can never be un-rounded afterwards.
//! Rates whose usage the gateway does not meter (audio) and price structures it
//! cannot enforce (context tiers) are refused for the same reason: approving them
//! would promise a schedule the request path would not apply.
//!
//! **Approval is not publication.** A book carries [`Approval`], and a
//! [`Approval::Draft`] book projects no prices at all. Its targets stay
//! discoverable and simply have no approved price, which is what "missing pricing
//! leaves an offering ineligible for budget-controlled routing" means in the
//! type: eligibility is `Some(price)`, never a default.
//!
//! **Precedence is stated, not inferred.** Two rules may cover one target at one
//! instant only when one of them is an [`RulePrecedence::Override`] of the other:
//! the negotiated rate wins over the baseline, explicitly. Two rules of the *same*
//! precedence overlapping in time is a refusal ([`PricingError::OverlappingRules`]),
//! so "which rate applied at 14:00" has exactly one answer and no tie-break rule
//! anyone has to remember.
//!
//! # Effective dating
//!
//! An interval is half-open — `[from, until)` — so consecutive rules meet at an
//! instant without either sharing it or leaving a gap. Instants are integer
//! milliseconds since the Unix epoch ([`EffectiveInstant`]), because a canonical
//! body has no floats and no timezone.
//!
//! Resolution at an instant produces a [`PricingSnapshot`]: the approved targets,
//! the catalogue identity, the book's reference and content checksum, and the
//! maximal interval over which that resolution does not change. The interval is
//! what makes future-dated pricing safe to publish *and* honest: a snapshot says
//! until when it is the answer, so a later slice can recompile when it elapses
//! instead of a request path having to consult a clock and a table.
//!
//! # What is deliberately absent
//!
//! - **Tenant overrides.** [`ResourceKind::Price`] is deployment-scoped in this
//!   slice ([`PricingError::ScopeNotSupported`] refuses a tenant- or
//!   project-scoped price book). A per-tenant price changes which budget a
//!   request is charged against, and the routing model has no representation for
//!   that yet; a book that silently applied to every tenant would be worse than a
//!   refusal. A narrower-scoped `Price` resource that does *not* declare
//!   [`PRICE_BOOK_SCHEMA`] — a tenant's rate row a model entitlement points at
//!   (#207) — is left to the slice that reads it, and activates nothing here.
//! - **Usage propagation and settlement.** Nothing here writes a receipt or
//!   charges a budget (#155). A [`PricingSnapshot`] is a lookup table published
//!   with the routing snapshot, and no request path reads a database, models.dev,
//!   or a mutable price to use it.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use gateway_core::catalog::ModelPrice;

use super::canonical::{Canonical, CanonicalError, CanonicalValue, Checksum, InvalidChecksum};
use super::ids::{ResourceId, Slug};
use super::mutation::{Actor, InvalidActor};
use super::record::{BodyError, DisplayNameError, Record, SCHEMA_FIELD};
use super::resource::{
    ResourceBody, ResourceKind, ResourceRef, ResourceScope, ResourceVersion, ResourceVersionNumber,
};
use super::revision::DesiredState;
use super::tenancy::{DisplayName, InvalidDisplayName};
use crate::backends::catalog::{
    CatalogContentId, InvalidCatalogId, JsonPointer, ObservedRate, ProviderId,
};

/// The schema identifier a price-book body declares and this build reads.
///
/// As with the tenancy bodies, any change to the field set or a field's meaning
/// is a *new* identifier: an unknown schema and an unknown field are both
/// refusals, never partial interpretations (see [`super::tenancy`]).
pub const PRICE_BOOK_SCHEMA: &str = "axond.price-book.v1";

const CATALOG_FIELD: &str = "catalog_content_id";
const CURRENCY_FIELD: &str = "currency";
const UNIT_FIELD: &str = "unit";
const APPROVAL_FIELD: &str = "approval";
const RULES_FIELD: &str = "rules";

const PROVIDER_FIELD: &str = "provider";
const MODEL_FIELD: &str = "published_model_id";
const PRECEDENCE_FIELD: &str = "precedence";
const FROM_FIELD: &str = "effective_from";
const UNTIL_FIELD: &str = "effective_until";
const RATES_FIELD: &str = "rates";
const TIERS_FIELD: &str = "tiers";
const PROVENANCE_FIELD: &str = "provenance";

const STATE_FIELD: &str = "state";
const APPROVED_BY_FIELD: &str = "by";
const APPROVED_AT_FIELD: &str = "at";
const CITATION_FIELD: &str = "citation";

const ORIGIN_FIELD: &str = "origin";
const POINTER_FIELD: &str = "pointer";

const INPUT_FIELD: &str = "input";
const OUTPUT_FIELD: &str = "output";
const REASONING_FIELD: &str = "reasoning";
const CACHE_READ_FIELD: &str = "cache_read";
const CACHE_WRITE_FIELD: &str = "cache_write";
const INPUT_AUDIO_FIELD: &str = "input_audio";
const OUTPUT_AUDIO_FIELD: &str = "output_audio";

const THRESHOLD_FIELD: &str = "threshold";
const TYPE_FIELD: &str = "type";

/// The currency a book's rates are stated in.
///
/// One variant, named rather than assumed: every rate in the runtime is
/// micro-dollars (ADR 0010), so a book stating anything else would need a
/// conversion nobody has authorized, and the type is where that shows up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum Currency {
    #[default]
    Usd,
}

impl Currency {
    pub const ALL: &'static [Self] = &[Self::Usd];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Usd => "USD",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|it| it.as_str() == text)
    }
}

impl fmt::Display for Currency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The unit a book's rates are stated in.
///
/// Nano-dollars per million tokens: the unit the catalogue observes in, so
/// approving exactly what was published needs no arithmetic at the moment of
/// approval, and every loss of precision happens in one place — the conversion to
/// [`ModelPrice`] — where it is refused rather than rounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum RateUnit {
    #[default]
    NanoDollarsPerMillionTokens,
}

impl RateUnit {
    pub const ALL: &'static [Self] = &[Self::NanoDollarsPerMillionTokens];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NanoDollarsPerMillionTokens => "nano-dollars-per-million-tokens",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|it| it.as_str() == text)
    }
}

impl fmt::Display for RateUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A rate an operator approved, in nano-dollars per million tokens.
///
/// Distinct from [`ObservedRate`] despite the identical unit, and that is the
/// point: the two are the same number on either side of a decision, and only the
/// approved one can be converted into something a request is billed against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ApprovedRate(u64);

impl ApprovedRate {
    pub const ZERO: Self = Self(0);

    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    /// Approve an observed rate unchanged.
    ///
    /// The only bridge from the catalogue's observations to a book, and named for
    /// what it is: a `grep` for `approving` finds every place an observation
    /// becomes a candidate charge.
    pub const fn approving(observed: ObservedRate) -> Self {
        Self(observed.nanos())
    }

    pub const fn nanos(self) -> u64 {
        self.0
    }

    /// Micro-dollars per million tokens, exactly or not at all.
    ///
    /// A thousand nano-dollars are one micro-dollar. A remainder means the
    /// approved rate is finer than the runtime's unit, and there is no correct
    /// rounding to apply: rounding down bills less than was approved, rounding up
    /// bills more, and either would be a decision made by this function instead of
    /// by the operator who approved the rate. So it is refused, and the operator
    /// approves a rate the runtime can state.
    fn micro_dollars(self, field: &'static str) -> Result<u64, RateRejection> {
        /// Nano-dollars in a micro-dollar.
        const PER_MICRO: u64 = 1_000;

        if self.0.is_multiple_of(PER_MICRO) {
            Ok(self.0 / PER_MICRO)
        } else {
            Err(RateRejection::ExcessPrecision {
                field,
                nanos: self.0,
            })
        }
    }
}

impl fmt::Display for ApprovedRate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} nano-dollars/Mtok", self.0)
    }
}

impl Canonical for ApprovedRate {
    fn canonical(&self) -> CanonicalValue {
        CanonicalValue::integer(self.0)
    }
}

/// Why an approved rate schedule cannot become a runtime price.
///
/// Every arm is an authoring refusal: the book states something the request path
/// would not apply, and publishing it would promise a charge that never happens.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RateRejection {
    #[error("rate `{field}` is negative ({value})")]
    Negative { field: &'static str, value: i128 },
    #[error(
        "rate `{field}` of {nanos} nano-dollars/Mtok is finer than the micro-dollar the runtime \
         bills in; approve a rate that is a whole number of micro-dollars rather than one that \
         would have to be rounded"
    )]
    ExcessPrecision { field: &'static str, nanos: u64 },
    #[error("rate `{field}` of {value} does not fit an unsigned 64-bit nano-dollar rate")]
    Overflow { field: &'static str, value: i128 },
    #[error(
        "a `{threshold}` price tier cannot be approved: the request path applies one rate \
         schedule per target and would bill the base rate regardless"
    )]
    UnsupportedTier { threshold: String },
    #[error(
        "rate `{field}` cannot be approved: the gateway's usage record has no matching token \
         count, so a request would never be billed for it"
    )]
    UnbillableUsage { field: &'static str },
}

/// One approved rate schedule for one target.
///
/// The optional rates are optional in the same way [`ModelPrice`]'s are: absent
/// means "billed at the rate the runtime falls back to" (reasoning at the output
/// rate, cache reads and writes at the input rate), which is a documented runtime
/// behaviour rather than free tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApprovedRates {
    pub input: ApprovedRate,
    pub output: ApprovedRate,
    pub reasoning: Option<ApprovedRate>,
    pub cache_read: Option<ApprovedRate>,
    pub cache_write: Option<ApprovedRate>,
    /// Stated only so it can be refused: the gateway meters no audio tokens, so
    /// an audio rate is a schedule the request path could not apply. Carried in
    /// the schema because the catalogue observes these rates, and an operator who
    /// approves an observation wholesale must be told *why* it cannot activate.
    pub input_audio: Option<ApprovedRate>,
    pub output_audio: Option<ApprovedRate>,
}

impl ApprovedRates {
    /// The two rates every schedule states.
    pub const fn new(input: ApprovedRate, output: ApprovedRate) -> Self {
        Self {
            input,
            output,
            reasoning: None,
            cache_read: None,
            cache_write: None,
            input_audio: None,
            output_audio: None,
        }
    }

    /// The runtime price this schedule bills as, or why it cannot be one.
    ///
    /// Total and exact: no rounding, no saturation, no default. A schedule that
    /// converts once converts identically on every replica and every release,
    /// which is what lets the conversion happen at publication time and never on
    /// the request path.
    pub fn to_model_price(self) -> Result<ModelPrice, RateRejection> {
        if self.input_audio.is_some() {
            return Err(RateRejection::UnbillableUsage {
                field: INPUT_AUDIO_FIELD,
            });
        }
        if self.output_audio.is_some() {
            return Err(RateRejection::UnbillableUsage {
                field: OUTPUT_AUDIO_FIELD,
            });
        }
        let optional = |rate: Option<ApprovedRate>, field| {
            rate.map(|rate| rate.micro_dollars(field)).transpose()
        };
        Ok(ModelPrice {
            input_microdollars_per_million: self.input.micro_dollars(INPUT_FIELD)?,
            output_microdollars_per_million: self.output.micro_dollars(OUTPUT_FIELD)?,
            reasoning_microdollars_per_million: optional(self.reasoning, REASONING_FIELD)?,
            cache_read_microdollars_per_million: optional(self.cache_read, CACHE_READ_FIELD)?,
            cache_write_microdollars_per_million: optional(self.cache_write, CACHE_WRITE_FIELD)?,
        })
    }
}

impl Canonical for ApprovedRates {
    fn canonical(&self) -> CanonicalValue {
        let mut fields = vec![
            (INPUT_FIELD.to_owned(), self.input.canonical()),
            (OUTPUT_FIELD.to_owned(), self.output.canonical()),
        ];
        for (field, rate) in [
            (REASONING_FIELD, self.reasoning),
            (CACHE_READ_FIELD, self.cache_read),
            (CACHE_WRITE_FIELD, self.cache_write),
            (INPUT_AUDIO_FIELD, self.input_audio),
            (OUTPUT_AUDIO_FIELD, self.output_audio),
        ] {
            if let Some(rate) = rate {
                fields.push((field.to_owned(), rate.canonical()));
            }
        }
        CanonicalValue::map(fields)
    }
}

/// An instant on the effective-dating timeline: integer milliseconds since the
/// Unix epoch.
///
/// Integer rather than [`SystemTime`] in the body because a canonical value has
/// no floats and one spelling per value; milliseconds rather than seconds because
/// a [`Uuid7`](super::ids::Uuid7) is millisecond-stamped and a boundary should be
/// expressible at the resolution the rest of the domain orders by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EffectiveInstant(u64);

/// Why a wall-clock time is not an [`EffectiveInstant`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidInstant {
    #[error("an effective instant cannot precede the Unix epoch")]
    BeforeEpoch,
    #[error("{millis} milliseconds since the epoch does not fit an unsigned 64-bit instant")]
    Overflow { millis: u128 },
}

impl EffectiveInstant {
    /// The beginning of the timeline: what "in force since always" means, and the
    /// lower bound a resolved interval falls back to.
    pub const EPOCH: Self = Self(0);

    pub const fn from_millis(millis: u64) -> Self {
        Self(millis)
    }

    pub const fn millis(self) -> u64 {
        self.0
    }

    /// The instant a wall clock reads.
    ///
    /// Fallible rather than saturating: a clock before the epoch or beyond the
    /// range is a host whose time is wrong, and resolving pricing against a
    /// silently clamped instant would activate a rate schedule nobody dated.
    pub fn of(time: SystemTime) -> Result<Self, InvalidInstant> {
        let millis = time
            .duration_since(UNIX_EPOCH)
            .map_err(|_| InvalidInstant::BeforeEpoch)?
            .as_millis();
        u64::try_from(millis)
            .map(Self)
            .map_err(|_| InvalidInstant::Overflow { millis })
    }

    /// The wall-clock time this instant names, or `None` when the host cannot
    /// represent it.
    ///
    /// Fallible for the same reason [`Self::of`] is, in the other direction: an
    /// instant is read from stored state and may be any `u64` of milliseconds,
    /// which is further ahead than a [`SystemTime`] reaches on some hosts. A
    /// surface rendering a far-future boundary should say it cannot state it, not
    /// take the replica down.
    pub fn to_system_time(self) -> Option<SystemTime> {
        UNIX_EPOCH.checked_add(Duration::from_millis(self.0))
    }
}

impl fmt::Display for EffectiveInstant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}ms", self.0)
    }
}

impl Canonical for EffectiveInstant {
    fn canonical(&self) -> CanonicalValue {
        CanonicalValue::integer(self.0)
    }
}

/// Why an interval is not an interval.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidInterval {
    #[error("an effective interval ending at {until} cannot begin at {from}")]
    Empty {
        from: EffectiveInstant,
        until: EffectiveInstant,
    },
}

/// The half-open interval a rule is in force over: `[from, until)`.
///
/// Half-open so consecutive rules *meet*: a rule until `t` and a rule from `t`
/// leave no instant uncovered and no instant covered twice, and the boundary
/// itself belongs to exactly one of them. A closed interval would make `t`
/// ambiguous, and an open one would leave it unpriced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EffectiveInterval {
    from: EffectiveInstant,
    until: Option<EffectiveInstant>,
}

impl EffectiveInterval {
    /// An interval in force from `from` until further notice.
    pub const fn from(from: EffectiveInstant) -> Self {
        Self { from, until: None }
    }

    /// A bounded interval, refusing one that contains no instant.
    pub const fn bounded(
        from: EffectiveInstant,
        until: EffectiveInstant,
    ) -> Result<Self, InvalidInterval> {
        if until.0 <= from.0 {
            return Err(InvalidInterval::Empty { from, until });
        }
        Ok(Self {
            from,
            until: Some(until),
        })
    }

    pub const fn starts(self) -> EffectiveInstant {
        self.from
    }

    pub const fn ends(self) -> Option<EffectiveInstant> {
        self.until
    }

    /// Whether `at` falls inside the interval, `from` included and `until`
    /// excluded.
    pub const fn contains(self, at: EffectiveInstant) -> bool {
        if at.0 < self.from.0 {
            return false;
        }
        match self.until {
            None => true,
            Some(until) => at.0 < until.0,
        }
    }

    /// Whether two intervals share any instant.
    pub const fn overlaps(self, other: Self) -> bool {
        let starts_before_other_ends = match other.until {
            None => true,
            Some(until) => self.from.0 < until.0,
        };
        let other_starts_before_self_ends = match self.until {
            None => true,
            Some(until) => other.from.0 < until.0,
        };
        starts_before_other_ends && other_starts_before_self_ends
    }
}

impl fmt::Display for EffectiveInterval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.until {
            None => write!(f, "[{}, ∞)", self.from),
            Some(until) => write!(f, "[{}, {until})", self.from),
        }
    }
}

impl Canonical for EffectiveInterval {
    fn canonical(&self) -> CanonicalValue {
        let mut fields = vec![(FROM_FIELD.to_owned(), self.from.canonical())];
        if let Some(until) = self.until {
            fields.push((UNTIL_FIELD.to_owned(), until.canonical()));
        }
        CanonicalValue::map(fields)
    }
}

/// Which of two rules covering one target at one instant applies.
///
/// Stated in the body rather than inferred from how specific a rule looks. A
/// deployment's baseline comes from approving a catalogue observation; an override
/// is a negotiated or contractual rate that must win over it for a while without
/// the baseline having to be edited, split, or re-approved. Two rules of the same
/// precedence covering one instant is a refusal, so precedence is a total order on
/// whatever can legitimately overlap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RulePrecedence {
    /// The deployment's approved baseline.
    Baseline,
    /// An operator override of the baseline, for the interval it covers.
    Override,
}

impl RulePrecedence {
    pub const ALL: &'static [Self] = &[Self::Baseline, Self::Override];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::Override => "override",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|it| it.as_str() == text)
    }
}

impl fmt::Display for RulePrecedence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What a price is attached to: one provider's own name for one model.
///
/// The published id rather than the catalogue's neutral model id, because the
/// published id is what a request to that provider carries
/// ([`ProviderOffering::published_model_id`](crate::backends::catalog::ProviderOffering)),
/// and pricing has to key on the thing the request path already holds.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PricedTarget {
    pub provider: ProviderId,
    pub published_model_id: String,
}

impl PricedTarget {
    pub fn new(provider: ProviderId, published_model_id: impl Into<String>) -> Self {
        Self {
            provider,
            published_model_id: published_model_id.into(),
        }
    }
}

impl fmt::Display for PricedTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.provider.as_str(), self.published_model_id)
    }
}

/// Where an approved rate came from.
///
/// Recorded per rule rather than per book, because a book is normally a baseline
/// approved from an import plus a handful of negotiated overrides, and "why is
/// this number what it is" is asked about one target at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PriceOrigin {
    /// Approved from a catalogue observation, unchanged.
    Catalogue,
    /// A contractual or negotiated rate the upstream does not publish.
    Negotiated,
    /// Stated by an operator for a reason of their own — a promotion, an
    /// internal cross-charge.
    Operator,
}

impl PriceOrigin {
    pub const ALL: &'static [Self] = &[Self::Catalogue, Self::Negotiated, Self::Operator];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Catalogue => "catalogue",
            Self::Negotiated => "negotiated",
            Self::Operator => "operator",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|it| it.as_str() == text)
    }
}

impl fmt::Display for PriceOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The audit trail of one rule: where the number came from, and where to look.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriceProvenance {
    pub origin: PriceOrigin,
    /// For a catalogue origin, the pointer into the imported payload the rate was
    /// read from — the same [`JsonPointer`] the import recorded, so an audit does
    /// not have to re-derive it.
    pub pointer: Option<JsonPointer>,
    /// An operator-facing citation: a contract, a ticket, a change record.
    pub citation: Option<DisplayName>,
}

impl PriceProvenance {
    pub const fn stated(origin: PriceOrigin) -> Self {
        Self {
            origin,
            pointer: None,
            citation: None,
        }
    }

    pub fn cited(origin: PriceOrigin, citation: DisplayName) -> Self {
        Self {
            origin,
            pointer: None,
            citation: Some(citation),
        }
    }
}

impl Canonical for PriceProvenance {
    fn canonical(&self) -> CanonicalValue {
        let mut fields = vec![(
            ORIGIN_FIELD.to_owned(),
            CanonicalValue::string(self.origin.as_str()),
        )];
        if let Some(pointer) = &self.pointer {
            fields.push((POINTER_FIELD.to_owned(), pointer.canonical()));
        }
        if let Some(citation) = &self.citation {
            fields.push((
                CITATION_FIELD.to_owned(),
                CanonicalValue::string(citation.as_str()),
            ));
        }
        CanonicalValue::map(fields)
    }
}

/// Whether a book has been approved, and by whom.
///
/// Two states rather than a boolean plus optional metadata: an approved book
/// without an approver is unconstructible, which is the same reasoning that gives
/// [`Actor`] no "unknown" variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Approval {
    /// Stated but not approved. Projects no prices at all: every target it names
    /// stays unpriced, and unpriced means ineligible rather than free.
    Draft,
    Approved {
        by: Actor,
        at: EffectiveInstant,
        /// The change record, ticket, or contract the approval cites.
        citation: Option<DisplayName>,
    },
}

impl Approval {
    pub const fn is_approved(&self) -> bool {
        matches!(self, Self::Approved { .. })
    }

    /// The stable label a rejection, log line, or metric carries.
    pub const fn state(&self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Approved { .. } => "approved",
        }
    }

    /// Who approved the book, or `None` for a draft.
    pub const fn approver(&self) -> Option<&Actor> {
        match self {
            Self::Draft => None,
            Self::Approved { by, .. } => Some(by),
        }
    }
}

impl Canonical for Approval {
    fn canonical(&self) -> CanonicalValue {
        match self {
            Self::Draft => {
                CanonicalValue::map([(STATE_FIELD, CanonicalValue::string(Self::Draft.state()))])
            }
            Self::Approved { by, at, citation } => {
                let mut fields = vec![
                    (STATE_FIELD.to_owned(), CanonicalValue::string(self.state())),
                    (APPROVED_BY_FIELD.to_owned(), by.canonical()),
                    (APPROVED_AT_FIELD.to_owned(), at.canonical()),
                ];
                if let Some(citation) = citation {
                    fields.push((
                        CITATION_FIELD.to_owned(),
                        CanonicalValue::string(citation.as_str()),
                    ));
                }
                CanonicalValue::map(fields)
            }
        }
    }
}

/// One effective-dated rate schedule for one target.
///
/// Constructed fallibly: a rule holds the [`ModelPrice`] its rates convert to, so
/// a rule that exists is a rule the runtime can bill, and resolving a snapshot
/// cannot fail on arithmetic. The stated rates are kept alongside it, because what
/// was approved and what is billed are different facts and an audit needs both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriceRule {
    target: PricedTarget,
    precedence: RulePrecedence,
    effective: EffectiveInterval,
    rates: ApprovedRates,
    price: ModelPrice,
    provenance: PriceProvenance,
}

impl PriceRule {
    /// A rule, or the reason its rates cannot be billed.
    pub fn new(
        target: PricedTarget,
        precedence: RulePrecedence,
        effective: EffectiveInterval,
        rates: ApprovedRates,
        provenance: PriceProvenance,
    ) -> Result<Self, RateRejection> {
        Ok(Self {
            target,
            precedence,
            effective,
            rates,
            price: rates.to_model_price()?,
            provenance,
        })
    }

    pub const fn target(&self) -> &PricedTarget {
        &self.target
    }

    pub const fn precedence(&self) -> RulePrecedence {
        self.precedence
    }

    pub const fn effective(&self) -> EffectiveInterval {
        self.effective
    }

    pub const fn rates(&self) -> ApprovedRates {
        self.rates
    }

    /// What a request billed under this rule is charged at.
    pub const fn price(&self) -> ModelPrice {
        self.price
    }

    pub const fn provenance(&self) -> &PriceProvenance {
        &self.provenance
    }
}

impl Canonical for PriceRule {
    fn canonical(&self) -> CanonicalValue {
        // The price is *derived* and deliberately not encoded: two spellings of
        // one fact could disagree, and the conversion is exact, so the rates are
        // the fact and the price is what reading them concludes.
        let mut fields = vec![
            (PROVIDER_FIELD.to_owned(), self.target.provider.canonical()),
            (
                MODEL_FIELD.to_owned(),
                CanonicalValue::string(&self.target.published_model_id),
            ),
            (
                PRECEDENCE_FIELD.to_owned(),
                CanonicalValue::string(self.precedence.as_str()),
            ),
            (FROM_FIELD.to_owned(), self.effective.from.canonical()),
            (RATES_FIELD.to_owned(), self.rates.canonical()),
            (PROVENANCE_FIELD.to_owned(), self.provenance.canonical()),
        ];
        if let Some(until) = self.effective.until {
            fields.push((UNTIL_FIELD.to_owned(), until.canonical()));
        }
        CanonicalValue::map(fields)
    }
}

/// The typed body of a price-book resource.
///
/// Immutable, like every resource body: a change is a new
/// [`ResourceVersion`] of the same resource, so
/// "what did we charge in March" is answerable from the revision that was serving
/// in March rather than from an audit log of edits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriceBookBody {
    catalog: CatalogContentId,
    currency: Currency,
    unit: RateUnit,
    approval: Approval,
    rules: Vec<PriceRule>,
}

impl PriceBookBody {
    /// The schema identifier this body encodes and reads.
    pub const SCHEMA: &'static str = PRICE_BOOK_SCHEMA;

    const KNOWN_FIELDS: &'static [&'static str] = &[
        CATALOG_FIELD,
        CURRENCY_FIELD,
        UNIT_FIELD,
        APPROVAL_FIELD,
        RULES_FIELD,
    ];

    const RULE_FIELDS: &'static [&'static str] = &[
        PROVIDER_FIELD,
        MODEL_FIELD,
        PRECEDENCE_FIELD,
        FROM_FIELD,
        UNTIL_FIELD,
        RATES_FIELD,
        TIERS_FIELD,
        PROVENANCE_FIELD,
    ];

    const RATE_FIELDS: &'static [&'static str] = &[
        INPUT_FIELD,
        OUTPUT_FIELD,
        REASONING_FIELD,
        CACHE_READ_FIELD,
        CACHE_WRITE_FIELD,
        INPUT_AUDIO_FIELD,
        OUTPUT_AUDIO_FIELD,
    ];

    const APPROVAL_FIELDS: &'static [&'static str] = &[
        STATE_FIELD,
        APPROVED_BY_FIELD,
        APPROVED_AT_FIELD,
        CITATION_FIELD,
    ];

    const PROVENANCE_FIELDS: &'static [&'static str] =
        &[ORIGIN_FIELD, POINTER_FIELD, CITATION_FIELD];

    /// An empty book against one catalogue snapshot.
    pub const fn new(catalog: CatalogContentId, approval: Approval) -> Self {
        Self {
            catalog,
            currency: Currency::Usd,
            unit: RateUnit::NanoDollarsPerMillionTokens,
            approval,
            rules: Vec::new(),
        }
    }

    /// Add a rule. Consistency between rules is checked where every body is
    /// interpreted — [`PriceBookBody::read`] — so there is one answer and not a
    /// builder's answer plus a reader's.
    #[must_use]
    pub fn with_rule(mut self, rule: PriceRule) -> Self {
        self.rules.push(rule);
        self
    }

    /// The catalogue content this book priced.
    pub const fn catalog(&self) -> CatalogContentId {
        self.catalog
    }

    pub const fn currency(&self) -> Currency {
        self.currency
    }

    pub const fn unit(&self) -> RateUnit {
        self.unit
    }

    pub const fn approval(&self) -> &Approval {
        &self.approval
    }

    pub fn rules(&self) -> &[PriceRule] {
        &self.rules
    }

    pub fn body(&self) -> ResourceBody {
        ResourceBody::Inline(self.canonical())
    }

    /// The first version of this book, at the deployment scope its kind permits.
    ///
    /// The identity is supplied rather than derived from the catalogue content:
    /// a book is a durable object an operator amends — a new version, same
    /// identity — and re-approving the same rates against a refreshed catalogue
    /// snapshot must not silently become a *different* resource.
    pub fn version(&self, id: ResourceId, slug: Slug) -> ResourceVersion {
        self.version_at(id, slug, ResourceVersionNumber::FIRST)
    }

    pub fn version_at(
        &self,
        id: ResourceId,
        slug: Slug,
        version: ResourceVersionNumber,
    ) -> ResourceVersion {
        ResourceVersion::new(
            ResourceRef::new(ResourceKind::Price, id, version),
            ResourceScope::Deployment,
            slug,
            self.body(),
        )
    }

    /// Read a price-book resource's body, strictly.
    ///
    /// Strict in every direction a body can be wrong: the kind, the form, the
    /// schema identifier, the field set, each field's type, each rate's
    /// convertibility, and the consistency of the rules with each other. A book
    /// this build cannot read is refused rather than partially applied, because
    /// half a price book is a billing error.
    pub fn read(resource: &ResourceVersion) -> Result<Self, PricingError> {
        let record = Record::<PricingError>::open(
            resource,
            ResourceKind::Price,
            Self::SCHEMA,
            Self::KNOWN_FIELDS,
        )?;
        let reference = resource.reference;
        let catalog = record.string(CATALOG_FIELD)?;
        let catalog =
            Checksum::parse(catalog).map_err(|source| PricingError::MalformedChecksum {
                reference,
                field: CATALOG_FIELD,
                source,
            })?;
        let currency = record.string(CURRENCY_FIELD)?;
        let currency = Currency::parse(currency).ok_or_else(|| PricingError::UnknownCurrency {
            reference,
            currency: currency.to_owned(),
        })?;
        let unit = record.string(UNIT_FIELD)?;
        let unit = RateUnit::parse(unit).ok_or_else(|| PricingError::UnknownUnit {
            reference,
            unit: unit.to_owned(),
        })?;
        let approval = record.record(Self::SCHEMA, APPROVAL_FIELD, Self::APPROVAL_FIELDS)?;
        let approval = Self::read_approval(&approval)?;

        let mut rules = Vec::new();
        for member in record.set(RULES_FIELD)? {
            let rule = Record::nested(
                reference,
                Self::SCHEMA,
                RULES_FIELD,
                member,
                Self::RULE_FIELDS,
            )?;
            rules.push(Self::read_rule(&rule)?);
        }

        let book = Self {
            catalog: CatalogContentId::from_checksum(catalog),
            currency,
            unit,
            approval,
            rules,
        };
        book.check_rule_consistency(reference)?;
        Ok(book)
    }

    fn read_approval(record: &Record<'_, PricingError>) -> Result<Approval, PricingError> {
        match record.string(STATE_FIELD)? {
            "draft" => {
                // A draft states nothing but its state. Reading one that also
                // names an approver would drop the evidence and re-canonicalize
                // to a checksum the stored bytes do not have.
                for field in [APPROVED_BY_FIELD, APPROVED_AT_FIELD, CITATION_FIELD] {
                    if record.optional_value(field).is_some() {
                        return Err(PricingError::UnknownField {
                            reference: record.reference(),
                            schema: PRICE_BOOK_SCHEMA,
                            field: field.to_owned(),
                        });
                    }
                }
                Ok(Approval::Draft)
            }
            "approved" => Ok(Approval::Approved {
                by: record.actor(APPROVED_BY_FIELD)?,
                at: record.instant(APPROVED_AT_FIELD)?,
                citation: record.optional_display_name(CITATION_FIELD)?,
            }),
            state => Err(PricingError::UnknownApprovalState {
                reference: record.reference(),
                state: state.to_owned(),
            }),
        }
    }

    fn read_rule(record: &Record<'_, PricingError>) -> Result<PriceRule, PricingError> {
        let reference = record.reference();
        let provider = record.catalog_id(PROVIDER_FIELD)?;
        let published_model_id = record.string(MODEL_FIELD)?.to_owned();
        let target = PricedTarget {
            provider,
            published_model_id,
        };
        let precedence = record.string(PRECEDENCE_FIELD)?;
        let precedence =
            RulePrecedence::parse(precedence).ok_or_else(|| PricingError::UnknownPrecedence {
                reference,
                precedence: precedence.to_owned(),
            })?;
        let from = record.instant(FROM_FIELD)?;
        let effective = match record.optional_instant(UNTIL_FIELD)? {
            None => EffectiveInterval::from(from),
            Some(until) => EffectiveInterval::bounded(from, until).map_err(|source| {
                PricingError::InvalidInterval {
                    reference,
                    target: target.to_string(),
                    source,
                }
            })?,
        };
        // Read before the rates, so a tiered schedule is refused as a tier rather
        // than as whatever its base rates happen to be.
        record
            .reject_tiers(TIERS_FIELD)
            .map_err(|source| PricingError::Rate {
                reference,
                target: target.to_string(),
                source,
            })?;
        let rates = record.record(Self::SCHEMA, RATES_FIELD, Self::RATE_FIELDS)?;
        let rates = ApprovedRates {
            input: rates.rate(&target, INPUT_FIELD)?,
            output: rates.rate(&target, OUTPUT_FIELD)?,
            reasoning: rates.optional_rate(&target, REASONING_FIELD)?,
            cache_read: rates.optional_rate(&target, CACHE_READ_FIELD)?,
            cache_write: rates.optional_rate(&target, CACHE_WRITE_FIELD)?,
            input_audio: rates.optional_rate(&target, INPUT_AUDIO_FIELD)?,
            output_audio: rates.optional_rate(&target, OUTPUT_AUDIO_FIELD)?,
        };
        let provenance = record.record(Self::SCHEMA, PROVENANCE_FIELD, Self::PROVENANCE_FIELDS)?;
        let origin = provenance.string(ORIGIN_FIELD)?;
        let provenance = PriceProvenance {
            origin: PriceOrigin::parse(origin).ok_or_else(|| PricingError::UnknownOrigin {
                reference,
                origin: origin.to_owned(),
            })?,
            pointer: provenance
                .optional_string(POINTER_FIELD)?
                .map(JsonPointer::new),
            citation: provenance.optional_display_name(CITATION_FIELD)?,
        };
        PriceRule::new(target.clone(), precedence, effective, rates, provenance).map_err(|source| {
            PricingError::Rate {
                reference,
                target: target.to_string(),
                source,
            }
        })
    }

    /// The one rule that spans rules: at any instant, a target has at most one
    /// rule per precedence.
    ///
    /// Overlap is refused rather than resolved by a tie-break, because every
    /// tie-break anyone could write — later `from` wins, narrower interval wins,
    /// last in the set wins — is a rule an operator would have to know to predict
    /// what they will be charged. Overriding is expressible, explicitly, through
    /// [`RulePrecedence`].
    fn check_rule_consistency(&self, reference: ResourceRef) -> Result<(), PricingError> {
        let mut by_key: BTreeMap<(&PricedTarget, RulePrecedence), Vec<&PriceRule>> =
            BTreeMap::new();
        for rule in &self.rules {
            by_key
                .entry((&rule.target, rule.precedence))
                .or_default()
                .push(rule);
        }
        for ((target, precedence), mut rules) in by_key {
            rules.sort_by_key(|rule| rule.effective);
            for pair in rules.windows(2) {
                if pair[0].effective.overlaps(pair[1].effective) {
                    return Err(PricingError::OverlappingRules {
                        reference,
                        target: target.to_string(),
                        precedence,
                        first: pair[0].effective,
                        second: pair[1].effective,
                    });
                }
            }
        }
        Ok(())
    }

    /// Every instant at which this book's resolution can change.
    ///
    /// Both ends of every interval: a rule starting is a change, and a rule ending
    /// is a change even when nothing replaces it.
    fn boundaries(&self) -> BTreeSet<EffectiveInstant> {
        let mut boundaries = BTreeSet::new();
        for rule in &self.rules {
            boundaries.insert(rule.effective.from);
            if let Some(until) = rule.effective.until {
                boundaries.insert(until);
            }
        }
        boundaries
    }

    /// The interval around `at` over which this book resolves to the same prices.
    ///
    /// The previous boundary at or before `at` (the epoch if there is none) and
    /// the next one after it. A caller holding this knows exactly when the
    /// resolution it holds stops being the answer.
    fn stable_interval(&self, at: EffectiveInstant) -> EffectiveInterval {
        let boundaries = self.boundaries();
        let from = boundaries
            .range(..=at)
            .next_back()
            .copied()
            .unwrap_or(EffectiveInstant::EPOCH);
        // The saturating step means the range can still yield `at` itself at the
        // end of the timeline, where a boundary after `at` does not exist; an
        // interval that would be empty is unbounded instead.
        match boundaries
            .range(EffectiveInstant(at.0.saturating_add(1))..)
            .next()
            .copied()
            .filter(|until| until.0 > from.0)
        {
            None => EffectiveInterval::from(from),
            Some(until) => EffectiveInterval::bounded(from, until)
                .expect("a boundary after `at` is after the boundary at or before it"),
        }
    }

    /// The rules in force at `at`, one per target, highest precedence winning.
    fn in_force(&self, at: EffectiveInstant) -> BTreeMap<&PricedTarget, &PriceRule> {
        let mut resolved: BTreeMap<&PricedTarget, &PriceRule> = BTreeMap::new();
        for rule in &self.rules {
            if !rule.effective.contains(at) {
                continue;
            }
            resolved
                .entry(&rule.target)
                .and_modify(|winner| {
                    if rule.precedence > winner.precedence {
                        *winner = rule;
                    }
                })
                .or_insert(rule);
        }
        resolved
    }
}

impl Canonical for PriceBookBody {
    fn canonical(&self) -> CanonicalValue {
        CanonicalValue::map([
            (SCHEMA_FIELD, CanonicalValue::string(Self::SCHEMA)),
            (
                CATALOG_FIELD,
                CanonicalValue::string(self.catalog.checksum().to_string()),
            ),
            (
                CURRENCY_FIELD,
                CanonicalValue::string(self.currency.as_str()),
            ),
            (UNIT_FIELD, CanonicalValue::string(self.unit.as_str())),
            (APPROVAL_FIELD, self.approval.canonical()),
            (
                // A set: the order rules were authored in carries no meaning, so
                // it must not change the book's checksum.
                RULES_FIELD,
                CanonicalValue::set(self.rules.iter().map(Canonical::canonical)),
            ),
        ])
    }
}

/// Why a price book could not be read, or is not one this build can serve.
///
/// Named references throughout: these are the messages an administrator sees when
/// `/admin/v1` refuses a publication, and what a replica reports when it cannot
/// read a retained revision.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PricingError {
    #[error("{reference} is a {} resource, not a {}", found.as_str(), expected.as_str())]
    Kind {
        reference: ResourceRef,
        expected: ResourceKind,
        found: ResourceKind,
    },
    #[error(
        "{reference} carries a {scope:?}-scoped price book; this build approves pricing for the \
         deployment as a whole, and a tenant-scoped price book would change which budget a \
         request is charged against without the routing model representing it"
    )]
    ScopeNotSupported {
        reference: ResourceRef,
        scope: ResourceScope,
    },
    #[error(
        "{first} and {second} are both deployment price books; one deployment has one approved \
         baseline, so which of two applies would be undefined"
    )]
    MultipleBooks {
        first: ResourceRef,
        second: ResourceRef,
    },
    #[error("{reference} does not carry an inline body")]
    NotInline { reference: ResourceRef },
    #[error("{reference} does not carry a record")]
    NotARecord { reference: ResourceRef },
    #[error("{reference} carries a body no canonical writer could have produced: {source}")]
    Uncanonicalizable {
        reference: ResourceRef,
        #[source]
        source: CanonicalError,
    },
    #[error("{reference} declares schema `{found}`, not `{expected}`")]
    Schema {
        reference: ResourceRef,
        expected: &'static str,
        found: String,
    },
    #[error("{reference} is missing the `{field}` field")]
    MissingField {
        reference: ResourceRef,
        field: &'static str,
    },
    #[error("{reference} carries the field `{field}`, which `{schema}` does not define")]
    UnknownField {
        reference: ResourceRef,
        schema: &'static str,
        field: String,
    },
    #[error("{reference} field `{field}` is not the type `{schema}` defines")]
    FieldType {
        reference: ResourceRef,
        schema: &'static str,
        field: &'static str,
    },
    #[error("{reference} field `{field}` is not a checksum: {source}")]
    MalformedChecksum {
        reference: ResourceRef,
        field: &'static str,
        #[source]
        source: InvalidChecksum,
    },
    #[error("{reference} field `{field}` is not a catalogue identifier: {source}")]
    MalformedId {
        reference: ResourceRef,
        field: &'static str,
        #[source]
        source: InvalidCatalogId,
    },
    #[error("{reference} field `{field}` is not an operator-facing name: {source}")]
    MalformedCitation {
        reference: ResourceRef,
        field: &'static str,
        #[source]
        source: InvalidDisplayName,
    },
    #[error("{reference} field `{field}` does not record an actor: {source}")]
    MalformedActor {
        reference: ResourceRef,
        field: &'static str,
        #[source]
        source: InvalidActor,
    },
    #[error("{reference} states its rates in `{currency}`, which this build does not bill in")]
    UnknownCurrency {
        reference: ResourceRef,
        currency: String,
    },
    #[error("{reference} states its rates in `{unit}`, which this build cannot convert")]
    UnknownUnit {
        reference: ResourceRef,
        unit: String,
    },
    #[error("{reference} records approval state `{state}`, which this build does not know")]
    UnknownApprovalState {
        reference: ResourceRef,
        state: String,
    },
    #[error("{reference} records rule precedence `{precedence}`, which this build does not know")]
    UnknownPrecedence {
        reference: ResourceRef,
        precedence: String,
    },
    #[error("{reference} records price origin `{origin}`, which this build does not know")]
    UnknownOrigin {
        reference: ResourceRef,
        origin: String,
    },
    #[error("{reference} field `{field}` is not an instant on the effective-dating timeline")]
    MalformedInstant {
        reference: ResourceRef,
        field: &'static str,
    },
    #[error("{reference} dates the rule for {target} over an interval that is empty: {source}")]
    InvalidInterval {
        reference: ResourceRef,
        target: String,
        #[source]
        source: InvalidInterval,
    },
    #[error("{reference} cannot bill the approved rate for {target}: {source}")]
    Rate {
        reference: ResourceRef,
        target: String,
        #[source]
        source: RateRejection,
    },
    #[error(
        "{reference} states two {precedence} rules for {target} that are both in force — {first} \
         and {second} — so which rate applies would be undefined; use an `override` rule to \
         supersede a baseline"
    )]
    OverlappingRules {
        reference: ResourceRef,
        target: String,
        precedence: RulePrecedence,
        first: EffectiveInterval,
        second: EffectiveInterval,
    },
}

impl PricingError {
    /// The resource the refusal is about.
    pub const fn reference(&self) -> ResourceRef {
        match self {
            Self::Kind { reference, .. }
            | Self::ScopeNotSupported { reference, .. }
            | Self::MultipleBooks {
                second: reference, ..
            }
            | Self::NotInline { reference }
            | Self::NotARecord { reference }
            | Self::Uncanonicalizable { reference, .. }
            | Self::Schema { reference, .. }
            | Self::MissingField { reference, .. }
            | Self::UnknownField { reference, .. }
            | Self::FieldType { reference, .. }
            | Self::MalformedChecksum { reference, .. }
            | Self::MalformedId { reference, .. }
            | Self::MalformedCitation { reference, .. }
            | Self::MalformedActor { reference, .. }
            | Self::UnknownCurrency { reference, .. }
            | Self::UnknownUnit { reference, .. }
            | Self::UnknownApprovalState { reference, .. }
            | Self::UnknownPrecedence { reference, .. }
            | Self::UnknownOrigin { reference, .. }
            | Self::MalformedInstant { reference, .. }
            | Self::InvalidInterval { reference, .. }
            | Self::Rate { reference, .. }
            | Self::OverlappingRules { reference, .. } => *reference,
        }
    }

    /// Whether this is a release skew rather than state that contradicts itself.
    ///
    /// The same distinction tenancy draws, for the same reason: a replica that
    /// cannot read a retained revision must say *this build cannot read this
    /// revision* — and keep serving what it holds — rather than page someone to
    /// repair an intact database.
    ///
    /// A schema identifier, an unknown field, and an unknown enumerated spelling
    /// are all things a newer release writes and an older one refuses, so they are
    /// skew — including an approver kind or an approver field a later release adds
    /// to [`Actor`], which is an enumerated spelling like any other. A rate this
    /// build cannot *bill* is skew for the same reason: a newer
    /// release may meter usage this one does not, or state a rate in a unit it
    /// bills exactly. A citation this build will not take is skew on the same
    /// grounds as every other body's display names ([`super::tenancy`]): what
    /// counts as an operator-facing name can tighten within one schema, so an
    /// approval note an earlier release wrote is a version mismatch and not a
    /// damaged row. A rate no release could have *written* is not: every writer
    /// encodes an [`ApprovedRate`], which is unsigned and fits its own range, so a
    /// negative or out-of-range rate is a rewritten body. A missing or mistyped
    /// `schema` field is skew for the reason it is in every other body: a body
    /// written before price books declared one. Everything else — a missing or
    /// mistyped field *inside* a schema this build reads, an empty interval, two
    /// rules contradicting each other — is a body that was rewritten underneath
    /// the gateway or written by no release at all.
    ///
    /// An identifier or a checksum this build cannot parse is on the damage side
    /// of that line, and deliberately so: tenancy draws the same line between a
    /// display name and an id ([`super::tenancy::TenancyError::is_incompatible`]).
    /// A citation is prose whose rules tighten; an identifier and a digest are
    /// what a body's *identity* is spelled in, so widening either changes what
    /// the field means, which is a new schema identifier — refused as skew by the
    /// arm above — rather than an older release meeting a wider spelling of the
    /// same schema. A body that cannot be canonically encoded at all
    /// ([`Self::Uncanonicalizable`]) is damage on the same grounds: every writer
    /// encodes through the canonical form, so a body that form rejects is one no
    /// release produced.
    pub fn is_incompatible(&self) -> bool {
        // Only the schema identifier itself, as for every other body: its
        // absence is a body written before price books had one at all.
        if let Self::MissingField { field, .. } | Self::FieldType { field, .. } = self {
            return *field == SCHEMA_FIELD;
        }
        matches!(
            self,
            Self::Schema { .. }
                | Self::UnknownField { .. }
                | Self::UnknownCurrency { .. }
                | Self::UnknownUnit { .. }
                | Self::UnknownApprovalState { .. }
                | Self::UnknownPrecedence { .. }
                | Self::UnknownOrigin { .. }
                | Self::MalformedCitation { .. }
                | Self::MalformedActor {
                    source: InvalidActor::UnknownKind { .. } | InvalidActor::UnknownField { .. },
                    ..
                }
                | Self::Rate {
                    source: RateRejection::ExcessPrecision { .. }
                        | RateRejection::UnsupportedTier { .. }
                        | RateRejection::UnbillableUsage { .. },
                    ..
                }
                | Self::ScopeNotSupported { .. }
        )
    }
}

/// A price book as a revision holds it: its envelope, its name, its body, and the
/// checksum of the body it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriceBook {
    pub reference: ResourceRef,
    pub slug: Slug,
    pub body: PriceBookBody,
    /// The checksum of the body's canonical bytes: the book's identity as
    /// *content*, which is what a snapshot records so "which prices was this
    /// replica serving" is answerable without re-reading the revision.
    pub checksum: Checksum,
}

/// Whether a resource's stored body says it is a price book.
///
/// A declaration, not a reading: a body that names [`PRICE_BOOK_SCHEMA`] is
/// judged by this slice's rules even when it sits at a scope this build cannot
/// bill, so a misplaced book is refused rather than ignored. Anything else is
/// another slice's price row.
fn declares_a_price_book(resource: &ResourceVersion) -> bool {
    let ResourceBody::Inline(CanonicalValue::Map(fields)) = &resource.body else {
        return false;
    };
    fields.iter().any(|(field, value)| {
        field == SCHEMA_FIELD && *value == CanonicalValue::string(PRICE_BOOK_SCHEMA)
    })
}

/// The price books of one revision, resolved once.
///
/// Built by [`PriceBooks::of`], which [`DesiredState::validate`] calls, so
/// publication, hydration, and projection reach the same conclusions about a book
/// rather than each having its own reading.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PriceBooks {
    book: Option<PriceBook>,
}

impl PriceBooks {
    /// Read and resolve the pricing of a desired state.
    ///
    /// Refuses a price book at a scope this slice cannot serve, a second
    /// deployment book, and any body that is not readable as
    /// [`PRICE_BOOK_SCHEMA`]. A revision with no price book at all is valid and
    /// carries no approved prices: pricing is opt-in, and its absence is what
    /// leaves an offering discoverable but ineligible for budget-controlled
    /// routing.
    pub fn of(state: &DesiredState) -> Result<Self, PricingError> {
        let mut books = Self::default();
        for resource in state.resources() {
            if resource.reference.kind != ResourceKind::Price {
                continue;
            }
            // A price resource narrower than the deployment is only this slice's
            // business when it claims to *be* a price book. The model contracts
            // (#207) point a tenant's entitlement at its own approved rate, and
            // those rows are that slice's state read by its own rules; reading
            // them here would refuse a revision this build otherwise serves
            // correctly, and it bills nothing from them either way.
            if resource.scope != ResourceScope::Deployment && !declares_a_price_book(resource) {
                continue;
            }
            // Before the body is read *and* before the scope is judged, so a
            // deployment that declares two books is told *that*, whatever else
            // is wrong with the second one. A second book that is unreadable or
            // sits at a scope this build does not serve would otherwise be
            // reported as skew — "roll the build forward" — for state whose only
            // repair is removing the extra book (see `is_incompatible`).
            if let Some(first) = &books.book {
                return Err(PricingError::MultipleBooks {
                    first: first.reference,
                    second: resource.reference,
                });
            }
            if resource.scope != ResourceScope::Deployment {
                return Err(PricingError::ScopeNotSupported {
                    reference: resource.reference,
                    scope: resource.scope.clone(),
                });
            }
            let body = PriceBookBody::read(resource)?;
            // Over the *stored* body rather than the parse of it: the identity a
            // snapshot publishes has to name the bytes an operator can go and
            // read, so a checksum cannot depend on this build's reading of them.
            let ResourceBody::Inline(stored) = &resource.body else {
                return Err(PricingError::NotInline {
                    reference: resource.reference,
                });
            };
            let checksum = stored
                .checksum()
                .map_err(|source| PricingError::Uncanonicalizable {
                    reference: resource.reference,
                    source,
                })?;
            books.book = Some(PriceBook {
                reference: resource.reference,
                slug: resource.slug.clone(),
                body,
                checksum,
            });
        }
        Ok(books)
    }

    /// The deployment's price book, if it published one.
    pub const fn book(&self) -> Option<&PriceBook> {
        self.book.as_ref()
    }

    /// The immutable pricing context for an instant, or `None` when the
    /// deployment published no book.
    pub fn snapshot_at(&self, at: EffectiveInstant) -> Option<PricingSnapshot> {
        self.book.as_ref().map(|book| PricingSnapshot::of(book, at))
    }
}

/// The pricing a snapshot serves under: immutable, complete, and identified.
///
/// Published with the routing snapshot and never separately (see
/// [`RevisionCompiler`](crate::convergence::compile::RevisionCompiler)), so a
/// request served from a snapshot is priced by exactly the book that snapshot was
/// compiled from. There is no lookup here that touches a database, an upstream, or
/// a clock: resolution happened at compile time, and what remains is a map.
///
/// The identities are carried in full rather than summarized into a number: a
/// receipt slice (#155) has to be able to say *which* catalogue content and
/// *which* price-book version a charge came from, and a monotonic
/// `catalog_version` cannot answer either.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PricingSnapshot {
    book: ResourceRef,
    checksum: Checksum,
    catalog: CatalogContentId,
    approval: Approval,
    effective: EffectiveInterval,
    targets: BTreeMap<PricedTarget, ModelPrice>,
}

impl PricingSnapshot {
    /// Resolve a book at an instant.
    ///
    /// Total: every rule already holds the price it converts to, so there is no
    /// arithmetic left to fail. A draft book resolves to no targets at all — the
    /// approval gate is here, not in the caller.
    pub fn of(book: &PriceBook, at: EffectiveInstant) -> Self {
        let targets = if book.body.approval.is_approved() {
            book.body
                .in_force(at)
                .into_iter()
                .map(|(target, rule)| (target.clone(), rule.price()))
                .collect()
        } else {
            BTreeMap::new()
        };
        Self {
            book: book.reference,
            checksum: book.checksum,
            catalog: book.body.catalog(),
            approval: book.body.approval.clone(),
            effective: book.body.stable_interval(at),
            targets,
        }
    }

    /// The price-book resource version these prices came from — the book's
    /// identity *and* its version number.
    pub const fn book(&self) -> ResourceRef {
        self.book
    }

    /// The checksum of the price-book body, so two replicas can agree that they
    /// are billing from the same book and not merely from the same version
    /// number.
    pub const fn checksum(&self) -> Checksum {
        self.checksum
    }

    /// The catalogue content the book was approved against.
    pub const fn catalog(&self) -> CatalogContentId {
        self.catalog
    }

    pub const fn approval(&self) -> &Approval {
        &self.approval
    }

    pub const fn is_approved(&self) -> bool {
        self.approval.is_approved()
    }

    /// The interval over which this resolution is the answer.
    pub const fn effective(&self) -> EffectiveInterval {
        self.effective
    }

    /// What a target is billed at, or `None` when nothing approved covers it.
    ///
    /// `None` is the eligibility answer, not a zero price: an offering with no
    /// approved price is discoverable and not routable under a budget.
    ///
    /// The provider is the *catalogue's* identifier ([`ProviderId`], the
    /// upstream's own name), not the operator-chosen `[[provider]] id` a routed
    /// [`Target`](crate::config::Target) carries. The two namespaces are
    /// unrelated and coincide only when an operator happens to reuse the
    /// upstream's name, so a request-path consumer (#155) must map a routed
    /// target to its catalogue provider *explicitly*; passing a config id here
    /// asks a question about a provider that does not exist and correctly gets
    /// `None`. The argument type is the guard: a config id is a `String` and
    /// cannot be passed without a deliberate parse.
    pub fn price(&self, provider: &ProviderId, published_model_id: &str) -> Option<ModelPrice> {
        self.targets
            .iter()
            .find(|(target, _)| {
                target.provider == *provider && target.published_model_id == published_model_id
            })
            .map(|(_, price)| *price)
    }

    /// Every approved target, ordered.
    pub fn targets(&self) -> impl ExactSizeIterator<Item = (&PricedTarget, &ModelPrice)> {
        self.targets.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }
}

/// What the shared reader needs a price-book refusal to be able to say.
///
/// Implementing this is what lets price books be read through the same strict
/// reader every other typed body is read through ([`super::record::Record`]),
/// while a refusal an operator reads still names a rate and a target rather than
/// a tenant: the reader decides what is wrong about a body, and this enum decides
/// what that means ([`PricingError::is_incompatible`]).
///
/// [`super::record::IdentifiedBody`] is deliberately not implemented: a price
/// book is deployment-scoped and names no tenant, project, or declared identity,
/// so the id-bearing refusals are unreachable here and are not restated as arms
/// that could never be constructed.
impl BodyError for PricingError {
    fn kind(reference: ResourceRef, expected: ResourceKind, found: ResourceKind) -> Self {
        Self::Kind {
            reference,
            expected,
            found,
        }
    }

    fn not_inline(reference: ResourceRef) -> Self {
        Self::NotInline { reference }
    }

    fn not_a_record(reference: ResourceRef) -> Self {
        Self::NotARecord { reference }
    }

    fn schema(reference: ResourceRef, expected: &'static str, found: String) -> Self {
        Self::Schema {
            reference,
            expected,
            found,
        }
    }

    fn missing_field(reference: ResourceRef, field: &'static str) -> Self {
        Self::MissingField { reference, field }
    }

    fn unknown_field(reference: ResourceRef, schema: &'static str, field: String) -> Self {
        Self::UnknownField {
            reference,
            schema,
            field,
        }
    }

    fn field_type(reference: ResourceRef, field: &'static str) -> Self {
        Self::FieldType {
            reference,
            schema: PRICE_BOOK_SCHEMA,
            field,
        }
    }
}

impl DisplayNameError for PricingError {
    /// The only prose a price book carries is an approval citation, so a name
    /// this build cannot parse is that citation.
    fn malformed_display_name(
        reference: ResourceRef,
        field: &'static str,
        source: InvalidDisplayName,
    ) -> Self {
        Self::MalformedCitation {
            reference,
            field,
            source,
        }
    }
}

/// The fields only a price book has, read on top of the shared reader.
///
/// Everything the shared reader already states — the kind, the form, the schema
/// identifier, the field set, strings, nested records, sets — stays there. What
/// is here is what pricing means by a field: a catalogue identifier, an actor, an
/// instant on the effective-dating timeline, a rate that must be billable, and a
/// tier schedule this build refuses rather than drops.
trait PriceFields<'a> {
    fn catalog_id(&self, field: &'static str) -> Result<ProviderId, PricingError>;
    fn actor(&self, field: &'static str) -> Result<Actor, PricingError>;
    fn instant(&self, field: &'static str) -> Result<EffectiveInstant, PricingError>;
    fn optional_instant(
        &self,
        field: &'static str,
    ) -> Result<Option<EffectiveInstant>, PricingError>;
    fn rate(
        &self,
        target: &PricedTarget,
        field: &'static str,
    ) -> Result<ApprovedRate, PricingError>;
    fn optional_rate(
        &self,
        target: &PricedTarget,
        field: &'static str,
    ) -> Result<Option<ApprovedRate>, PricingError>;
    fn reject_tiers(&self, field: &'static str) -> Result<(), RateRejection>;
}

impl<'a> PriceFields<'a> for Record<'a, PricingError> {
    fn catalog_id(&self, field: &'static str) -> Result<ProviderId, PricingError> {
        ProviderId::parse(self.string(field)?).map_err(|source| PricingError::MalformedId {
            reference: self.reference(),
            field,
            source,
        })
    }

    fn actor(&self, field: &'static str) -> Result<Actor, PricingError> {
        Actor::read(self.value(field)?).map_err(|source| PricingError::MalformedActor {
            reference: self.reference(),
            field,
            source,
        })
    }

    fn instant(&self, field: &'static str) -> Result<EffectiveInstant, PricingError> {
        u64::try_from(self.signed_integer(field)?)
            .map(EffectiveInstant::from_millis)
            .map_err(|_| PricingError::MalformedInstant {
                reference: self.reference(),
                field,
            })
    }

    fn optional_instant(
        &self,
        field: &'static str,
    ) -> Result<Option<EffectiveInstant>, PricingError> {
        match self.optional_value(field) {
            None => Ok(None),
            Some(_) => self.instant(field).map(Some),
        }
    }

    /// A rate: non-negative, and within the unit's range.
    ///
    /// Both refusals are stated as [`RateRejection`]s rather than as field-type
    /// errors, because "this rate is negative" is what an operator has to fix and
    /// "this field is not the type the schema defines" is not.
    fn rate(
        &self,
        target: &PricedTarget,
        field: &'static str,
    ) -> Result<ApprovedRate, PricingError> {
        let value = self.signed_integer(field)?;
        let rejection = |source| PricingError::Rate {
            reference: self.reference(),
            target: target.to_string(),
            source,
        };
        if value < 0 {
            return Err(rejection(RateRejection::Negative { field, value }));
        }
        u64::try_from(value)
            .map(ApprovedRate::from_nanos)
            .map_err(|_| rejection(RateRejection::Overflow { field, value }))
    }

    fn optional_rate(
        &self,
        target: &PricedTarget,
        field: &'static str,
    ) -> Result<Option<ApprovedRate>, PricingError> {
        match self.optional_value(field) {
            None => Ok(None),
            Some(_) => self.rate(target, field).map(Some),
        }
    }

    /// A tier list, refused for whatever it states.
    ///
    /// The field is part of the schema so a tiered schedule is *refused* rather
    /// than dropped: an operator approving a catalogue observation wholesale must
    /// be told the tiers cannot activate, not silently billed the base rate.
    fn reject_tiers(&self, field: &'static str) -> Result<(), RateRejection> {
        let Some(value) = self.optional_value(field) else {
            return Ok(());
        };
        let tiers = match value {
            CanonicalValue::List(tiers) | CanonicalValue::Set(tiers) => tiers,
            _ => {
                return Err(RateRejection::UnsupportedTier {
                    threshold: "unrecognized".to_owned(),
                });
            }
        };
        // An empty list too: the field is stated, this build cannot carry it, and
        // accepting it would be a reading that drops what the store holds.
        Err(RateRejection::UnsupportedTier {
            threshold: tiers
                .first()
                .map_or_else(|| "empty".to_owned(), tier_threshold_name),
        })
    }
}

/// The threshold kind a tier states, for a message an operator can act on.
fn tier_threshold_name(tier: &CanonicalValue) -> String {
    let CanonicalValue::Map(fields) = tier else {
        return "unrecognized".to_owned();
    };
    let Some((_, CanonicalValue::Map(threshold))) =
        fields.iter().find(|(name, _)| name == THRESHOLD_FIELD)
    else {
        return "unrecognized".to_owned();
    };
    match threshold.iter().find(|(name, _)| name == TYPE_FIELD) {
        Some((_, CanonicalValue::String(kind))) => kind.clone(),
        _ => "unrecognized".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::desired_state::fixtures;

    const MICRO: u64 = 1_000;

    fn target() -> PricedTarget {
        fixtures::priced_target("openai", "gpt-4o")
    }

    fn at(millis: u64) -> EffectiveInstant {
        EffectiveInstant::from_millis(millis)
    }

    fn approved() -> Approval {
        Approval::Approved {
            by: fixtures::actor(),
            at: EffectiveInstant::EPOCH,
            citation: None,
        }
    }

    fn book_of(body: &PriceBookBody) -> PriceBook {
        let resource = fixtures::price_book(body, 7, "baseline");
        let mut state = fixtures::state();
        state.insert(resource).expect("a distinct reference");
        PriceBooks::of(&state)
            .expect("the book is readable")
            .book()
            .expect("the state holds a book")
            .clone()
    }

    /// Reading a book a caller built returns the same book, so the round trip is
    /// what a test can assert on rather than the checksum alone.
    #[test]
    fn an_approved_book_round_trips_through_its_canonical_body() {
        let body = fixtures::approved_price_book();
        let read = PriceBookBody::read(&fixtures::price_book(&body, 7, "baseline"))
            .expect("the fixture body is readable");
        assert_eq!(read, body);
        assert_eq!(read.catalog(), fixtures::catalog_content_id());
        assert_eq!(read.currency(), Currency::Usd);
        assert_eq!(read.unit(), RateUnit::NanoDollarsPerMillionTokens);
        assert!(read.approval().is_approved());
    }

    /// The order rules were authored in is not part of what a book *is*, so it
    /// cannot change the identity a snapshot records.
    #[test]
    fn a_books_checksum_does_not_depend_on_the_order_its_rules_were_added_in() {
        let first = fixtures::price_rule(
            target(),
            RulePrecedence::Baseline,
            EffectiveInterval::bounded(EffectiveInstant::EPOCH, at(10)).expect("non-empty"),
            MICRO,
            MICRO,
        );
        let second = fixtures::price_rule(
            target(),
            RulePrecedence::Baseline,
            EffectiveInterval::from(at(10)),
            2 * MICRO,
            2 * MICRO,
        );
        let forwards = PriceBookBody::new(fixtures::catalog_content_id(), approved())
            .with_rule(first.clone())
            .with_rule(second.clone());
        let backwards = PriceBookBody::new(fixtures::catalog_content_id(), approved())
            .with_rule(second)
            .with_rule(first);
        assert_eq!(
            forwards.canonical().checksum().expect("canonical"),
            backwards.canonical().checksum().expect("canonical")
        );
    }

    #[test]
    fn a_rate_at_whole_micro_dollars_converts_exactly() {
        let rates = ApprovedRates {
            reasoning: Some(ApprovedRate::from_nanos(7 * MICRO)),
            cache_read: Some(ApprovedRate::ZERO),
            ..ApprovedRates::new(
                ApprovedRate::from_nanos(2_500_000),
                ApprovedRate::from_nanos(10_000_000),
            )
        };
        let price = rates.to_model_price().expect("whole micro-dollars convert");
        assert_eq!(price.input_microdollars_per_million, 2_500);
        assert_eq!(price.output_microdollars_per_million, 10_000);
        assert_eq!(price.reasoning_microdollars_per_million, Some(7));
        assert_eq!(price.cache_read_microdollars_per_million, Some(0));
        assert_eq!(price.cache_write_microdollars_per_million, None);
    }

    /// The rounding question, answered by refusing to answer it: a rate the
    /// runtime cannot state is not silently rounded in either direction.
    #[test]
    fn a_rate_finer_than_a_micro_dollar_is_refused_rather_than_rounded() {
        let rates = ApprovedRates::new(
            ApprovedRate::from_nanos(1_500),
            ApprovedRate::from_nanos(MICRO),
        );
        assert_eq!(
            rates.to_model_price(),
            Err(RateRejection::ExcessPrecision {
                field: "input",
                nanos: 1_500
            })
        );
        // And one nano-dollar below a whole micro-dollar, so the boundary itself
        // is asserted rather than a value far from it.
        let boundary = ApprovedRates::new(ApprovedRate::from_nanos(999), ApprovedRate::ZERO);
        assert!(matches!(
            boundary.to_model_price(),
            Err(RateRejection::ExcessPrecision { .. })
        ));
    }

    #[test]
    fn an_audio_rate_is_refused_because_no_usage_field_would_bill_it() {
        let rates = ApprovedRates {
            input_audio: Some(ApprovedRate::from_nanos(MICRO)),
            ..ApprovedRates::new(
                ApprovedRate::from_nanos(MICRO),
                ApprovedRate::from_nanos(MICRO),
            )
        };
        assert_eq!(
            rates.to_model_price(),
            Err(RateRejection::UnbillableUsage {
                field: "input_audio"
            })
        );
    }

    /// An observed rate can be approved unchanged, and the number does not move:
    /// approving is a decision, not a conversion.
    #[test]
    fn approving_an_observed_rate_preserves_it_exactly() {
        let observed = ObservedRate::from_nanos(3_000);
        assert_eq!(ApprovedRate::approving(observed).nanos(), observed.nanos());
    }

    /// The half-open boundary, asserted at the instant itself: the rule that ends
    /// at `t` is not in force at `t`, and the rule that begins at `t` is.
    #[test]
    fn an_effective_interval_includes_its_start_and_excludes_its_end() {
        let interval = EffectiveInterval::bounded(at(100), at(200)).expect("non-empty");
        assert!(!interval.contains(at(99)));
        assert!(interval.contains(at(100)));
        assert!(interval.contains(at(199)));
        assert!(!interval.contains(at(200)));
    }

    #[test]
    fn an_interval_that_contains_no_instant_is_refused() {
        assert_eq!(
            EffectiveInterval::bounded(at(100), at(100)),
            Err(InvalidInterval::Empty {
                from: at(100),
                until: at(100)
            })
        );
        assert!(EffectiveInterval::bounded(at(100), at(99)).is_err());
    }

    /// Consecutive rules meet without overlapping, and the price changes exactly
    /// at the boundary instant.
    #[test]
    fn consecutive_rules_hand_over_at_the_boundary_instant() {
        let body = PriceBookBody::new(fixtures::catalog_content_id(), approved())
            .with_rule(fixtures::price_rule(
                target(),
                RulePrecedence::Baseline,
                EffectiveInterval::bounded(EffectiveInstant::EPOCH, at(1_000)).expect("non-empty"),
                MICRO,
                MICRO,
            ))
            .with_rule(fixtures::price_rule(
                target(),
                RulePrecedence::Baseline,
                EffectiveInterval::from(at(1_000)),
                2 * MICRO,
                2 * MICRO,
            ));
        let book = book_of(&body);
        let provider = target().provider;
        let before = PricingSnapshot::of(&book, at(999));
        let on = PricingSnapshot::of(&book, at(1_000));
        assert_eq!(
            before
                .price(&provider, "gpt-4o")
                .expect("priced")
                .input_microdollars_per_million,
            1
        );
        assert_eq!(
            on.price(&provider, "gpt-4o")
                .expect("priced")
                .input_microdollars_per_million,
            2
        );
        // The interval each resolution is valid over is the gap between
        // boundaries, so a caller knows when what it holds stops being the answer.
        assert_eq!(before.effective().starts(), EffectiveInstant::EPOCH);
        assert_eq!(before.effective().ends(), Some(at(1_000)));
        assert_eq!(on.effective(), EffectiveInterval::from(at(1_000)));
    }

    #[test]
    fn two_rules_of_one_precedence_covering_one_instant_are_refused() {
        let body = PriceBookBody::new(fixtures::catalog_content_id(), approved())
            .with_rule(fixtures::price_rule(
                target(),
                RulePrecedence::Baseline,
                EffectiveInterval::bounded(EffectiveInstant::EPOCH, at(1_001)).expect("non-empty"),
                MICRO,
                MICRO,
            ))
            .with_rule(fixtures::price_rule(
                target(),
                RulePrecedence::Baseline,
                EffectiveInterval::from(at(1_000)),
                2 * MICRO,
                2 * MICRO,
            ));
        let error = PriceBookBody::read(&fixtures::price_book(&body, 7, "baseline"))
            .expect_err("overlapping rules of one precedence are refused");
        assert!(matches!(
            error,
            PricingError::OverlappingRules {
                precedence: RulePrecedence::Baseline,
                ..
            }
        ));
        // Contradictory state, not release skew: no build wrote this.
        assert!(!error.is_incompatible());
    }

    /// An override is how overlap is *expressed*: same target, same instant,
    /// stated precedence, and the override's rate is the one billed.
    #[test]
    fn an_override_supersedes_the_baseline_for_the_interval_it_covers() {
        let body = PriceBookBody::new(fixtures::catalog_content_id(), approved())
            .with_rule(fixtures::price_rule(
                target(),
                RulePrecedence::Baseline,
                EffectiveInterval::from(EffectiveInstant::EPOCH),
                10 * MICRO,
                10 * MICRO,
            ))
            .with_rule(fixtures::price_rule(
                target(),
                RulePrecedence::Override,
                EffectiveInterval::bounded(at(500), at(1_500)).expect("non-empty"),
                4 * MICRO,
                4 * MICRO,
            ));
        let book = book_of(&body);
        let provider = target().provider;
        let priced = |instant| {
            PricingSnapshot::of(&book, instant)
                .price(&provider, "gpt-4o")
                .expect("the baseline covers every instant")
                .input_microdollars_per_million
        };
        assert_eq!(priced(at(499)), 10);
        assert_eq!(priced(at(500)), 4);
        assert_eq!(priced(at(1_499)), 4);
        assert_eq!(priced(at(1_500)), 10);
    }

    /// The approval gate: a draft book activates nothing, and its targets are
    /// unpriced rather than free.
    #[test]
    fn a_draft_book_activates_no_prices() {
        let body = PriceBookBody::new(fixtures::catalog_content_id(), Approval::Draft).with_rule(
            fixtures::price_rule(
                target(),
                RulePrecedence::Baseline,
                EffectiveInterval::from(EffectiveInstant::EPOCH),
                MICRO,
                MICRO,
            ),
        );
        let snapshot = PricingSnapshot::of(&book_of(&body), at(5_000));
        assert!(!snapshot.is_approved());
        assert!(snapshot.is_empty());
        assert_eq!(snapshot.price(&target().provider, "gpt-4o"), None);
    }

    /// Eligibility is `Some(price)`: a target the book does not price has no
    /// price, and no default stands in for one.
    #[test]
    fn a_target_the_book_does_not_price_has_no_price() {
        let snapshot = PricingSnapshot::of(&book_of(&fixtures::approved_price_book()), at(1));
        assert!(snapshot.price(&target().provider, "gpt-4o").is_some());
        assert_eq!(snapshot.price(&target().provider, "o3"), None);
        assert_eq!(
            snapshot.price(
                &crate::backends::catalog::ProviderId::parse("anthropic").expect("id"),
                "gpt-4o"
            ),
            None
        );
    }

    /// Approved prices key on the catalogue's provider identity, not on the
    /// `[[provider]] id` an operator picked. A deployment that calls its OpenAI
    /// pool `openai-primary` gets no price for it, which is the honest answer
    /// and not a match: the slice that bills a routed target (#155) owes an
    /// explicit mapping rather than a hopeful string comparison.
    #[test]
    fn a_config_provider_id_is_not_a_catalogue_provider_id() {
        let snapshot = PricingSnapshot::of(&book_of(&fixtures::approved_price_book()), at(1));
        let routed = "openai-primary";
        let routed = ProviderId::parse(routed).expect("a config id can spell a catalogue id");
        assert_eq!(snapshot.price(&routed, "gpt-4o"), None);
        assert!(snapshot.price(&target().provider, "gpt-4o").is_some());
    }

    /// The identities a receipt slice (#155) needs, carried whole rather than
    /// summarized into a version number.
    #[test]
    fn a_snapshot_records_the_book_and_catalogue_it_priced_from() {
        let body = fixtures::approved_price_book();
        let book = book_of(&body);
        let snapshot = PricingSnapshot::of(&book, at(1));
        assert_eq!(snapshot.book(), book.reference);
        assert_eq!(
            snapshot.book(),
            fixtures::price_book(&body, 7, "baseline").reference
        );
        assert_eq!(
            snapshot.checksum(),
            body.canonical().checksum().expect("canonical")
        );
        assert_eq!(snapshot.catalog(), fixtures::catalog_content_id());
        assert_eq!(snapshot.targets().len(), 1);
    }

    /// The published fingerprint names the stored body, not this build's reading
    /// of it, so two replicas reading one row agree on which book they bill from.
    #[test]
    fn the_published_checksum_is_the_stored_bodys_checksum() {
        let resource = fixtures::price_book(&fixtures::approved_price_book(), 7, "baseline");
        let ResourceBody::Inline(stored) = &resource.body else {
            panic!("a price book is inline");
        };
        let stored = stored.checksum().expect("a record has a checksum");
        let mut state = fixtures::state();
        state.insert(resource).expect("a distinct reference");
        let book = PriceBooks::of(&state)
            .expect("the book is readable")
            .book()
            .expect("the state holds a book")
            .clone();
        assert_eq!(book.checksum, stored);
        assert_eq!(
            PricingSnapshot::of(&book, at(1)).checksum(),
            stored,
            "the snapshot publishes the stored book's identity"
        );
    }

    /// Anything the reader would have to drop is refused instead, because the
    /// checksum is over the stored bytes and a dropped field would leave the
    /// published identity naming a body this build cannot reproduce.
    #[test]
    fn a_body_this_build_would_have_to_read_lossily_is_refused() {
        let empty_tiers = read_rule_with(|fields| {
            fields.push((TIERS_FIELD.to_owned(), CanonicalValue::List(Vec::new())));
        })
        .expect_err("an empty tier list is refused");
        let PricingError::Rate {
            source: RateRejection::UnsupportedTier { threshold },
            ..
        } = &empty_tiers
        else {
            panic!("expected a tier rejection, got {empty_tiers}");
        };
        assert_eq!(threshold, "empty");

        // A nested record declares no schema of its own, so a `schema` key inside
        // one is a field this build does not know.
        let nested_schema = read_rule_with(|fields| {
            fields.push((
                SCHEMA_FIELD.to_owned(),
                CanonicalValue::string(PRICE_BOOK_SCHEMA),
            ));
        })
        .expect_err("a nested schema key is refused");
        let PricingError::UnknownField { field, .. } = &nested_schema else {
            panic!("expected an unknown-field refusal, got {nested_schema}");
        };
        assert_eq!(field, SCHEMA_FIELD);
        assert!(nested_schema.is_incompatible());
    }

    /// A revision that holds only a catalogue import carries no approved pricing:
    /// observed prices are metadata, and no path activates them.
    #[test]
    fn a_revision_without_a_price_book_carries_no_approved_pricing() {
        let books = PriceBooks::of(&fixtures::state()).expect("state without a book is valid");
        assert!(books.book().is_none());
        assert!(books.snapshot_at(at(1)).is_none());
    }

    #[test]
    fn a_tenant_scoped_price_book_is_refused() {
        let body = fixtures::approved_price_book();
        let tenant = fixtures::tenant_id(1);
        let resource = ResourceVersion::new(
            fixtures::price_book(&body, 7, "baseline").reference,
            ResourceScope::Tenant(tenant),
            fixtures::price_book(&body, 7, "baseline").slug,
            body.body(),
        );
        let mut state = fixtures::state();
        state.insert(resource).expect("a distinct reference");
        let error = PriceBooks::of(&state).expect_err("a tenant-scoped book is not servable");
        assert!(matches!(error, PricingError::ScopeNotSupported { .. }));
        // A newer release may support it; this one says so rather than reporting
        // corruption.
        assert!(error.is_incompatible());
    }

    /// A tenant's own rate row — a `Price` resource a model entitlement points at
    /// (#207) — is that slice's state, read by its rules and not by this one.
    /// Reading it here would refuse a revision this build serves correctly, and
    /// nothing bills from it either way: the deployment's baseline is the only
    /// thing a snapshot activates.
    #[test]
    fn a_tenant_rate_row_is_not_the_deployments_baseline() {
        let mut state = fixtures::state();
        state
            .insert(fixtures::price(&fixtures::tenant_id(1), 7, "acme-rate"))
            .expect("a distinct reference");

        let books =
            PriceBooks::of(&state).expect("a tenant's rate row is not this slice's to read");
        assert!(books.book().is_none(), "and it prices nothing");
        assert!(books.snapshot_at(at(1)).is_none());
    }

    #[test]
    fn two_deployment_price_books_are_refused() {
        let body = fixtures::approved_price_book();
        let mut state = fixtures::state();
        state
            .insert(fixtures::price_book(&body, 7, "baseline"))
            .and_then(|state| state.insert(fixtures::price_book(&body, 8, "second")))
            .expect("distinct references");
        assert!(matches!(
            PriceBooks::of(&state),
            Err(PricingError::MultipleBooks { .. })
        ));
    }

    /// A duplicate book is reported as a duplicate even when the extra book is
    /// also unreadable, because the two refusals send an operator different
    /// places: skew says roll the build forward, and a deployment that declares
    /// two books is repaired by removing one whatever release reads it.
    #[test]
    fn a_second_book_is_a_duplicate_before_it_is_anything_else() {
        let body = fixtures::approved_price_book();
        let CanonicalValue::Map(mut fields) = body.canonical() else {
            panic!("a body is a record");
        };
        fields.retain(|(name, _)| name != CURRENCY_FIELD);
        fields.push((CURRENCY_FIELD.to_owned(), CanonicalValue::string("EUR")));

        let first = fixtures::price_book(&body, 7, "baseline");
        let second = ResourceVersion::new(
            fixtures::reference(ResourceKind::Price, 8),
            ResourceScope::Deployment,
            first.slug.clone(),
            ResourceBody::Inline(CanonicalValue::map(fields)),
        );
        assert!(
            first.reference < second.reference,
            "the readable book has to be the one visited first for this to test the ordering"
        );

        let mut state = fixtures::state();
        state
            .insert(first)
            .and_then(|state| state.insert(second))
            .expect("distinct references");

        let error = PriceBooks::of(&state).expect_err("two books are refused");
        assert!(
            matches!(error, PricingError::MultipleBooks { .. }),
            "{error}"
        );
        // Contradictory state, not a body a newer release wrote.
        assert!(!error.is_incompatible());
    }

    /// The same ordering for the other refusal a second book can carry: a scope
    /// this build does not serve is skew, and reporting it for the *extra* book
    /// would send an operator to an upgrade that cannot repair state whose only
    /// repair is removing one of the two books.
    #[test]
    fn a_second_book_is_a_duplicate_before_its_scope_is_judged() {
        let body = fixtures::approved_price_book();
        let first = fixtures::price_book(&body, 7, "baseline");
        let second = ResourceVersion::new(
            fixtures::reference(ResourceKind::Price, 8),
            ResourceScope::Tenant(fixtures::tenant_id(1)),
            first.slug.clone(),
            body.body(),
        );
        assert!(
            first.reference < second.reference,
            "the deployment book has to be the one visited first for this to test the ordering"
        );

        let mut state = fixtures::state();
        state
            .insert(first)
            .and_then(|state| state.insert(second))
            .expect("distinct references");

        let error = PriceBooks::of(&state).expect_err("two books are refused");
        assert!(
            matches!(error, PricingError::MultipleBooks { .. }),
            "{error}"
        );
        assert!(!error.is_incompatible());
    }

    /// Every way a body can be a *newer release's* body, and the classification
    /// each one gets. Table-driven because the classification is the contract: a
    /// replica that reports skew as corruption pages the wrong person.
    #[test]
    fn bodies_a_newer_release_could_have_written_are_incompatibilities() {
        let cases: &[(&str, CanonicalValue)] = &[
            (SCHEMA_FIELD, CanonicalValue::string("axond.price-book.v2")),
            (CURRENCY_FIELD, CanonicalValue::string("EUR")),
            (UNIT_FIELD, CanonicalValue::string("pico-dollars")),
        ];
        for (field, value) in cases {
            let error = read_with_field(field, value.clone())
                .err()
                .unwrap_or_else(|| panic!("`{field}` = {value:?} is refused"));
            assert!(error.is_incompatible(), "{field}: {error}");
        }
        let unknown = read_with_field("rebate", CanonicalValue::integer(1))
            .expect_err("an unknown field is refused");
        assert!(matches!(unknown, PricingError::UnknownField { .. }));
        assert!(unknown.is_incompatible());

        // The schema identifier is the one field whose absence — or whose type
        // being wrong — is a body older than the identifier, exactly as it is for
        // the tenancy and credential bodies.
        let untyped = read_without_field(SCHEMA_FIELD).expect_err("an untyped body is refused");
        assert!(matches!(
            untyped,
            PricingError::MissingField {
                field: SCHEMA_FIELD,
                ..
            }
        ));
        assert!(untyped.is_incompatible());

        let mistyped_schema = read_with_field(SCHEMA_FIELD, CanonicalValue::integer(1))
            .expect_err("a non-string schema is refused");
        assert!(matches!(
            mistyped_schema,
            PricingError::FieldType {
                field: SCHEMA_FIELD,
                ..
            }
        ));
        assert!(mistyped_schema.is_incompatible());
    }

    /// And every way a body can be *wrong* rather than newer.
    #[test]
    fn bodies_no_release_would_have_written_are_invalid_state() {
        let missing = read_without_field(CURRENCY_FIELD).expect_err("a missing field is refused");
        assert!(matches!(missing, PricingError::MissingField { .. }));
        assert!(!missing.is_incompatible());

        let mistyped = read_with_field(CURRENCY_FIELD, CanonicalValue::integer(840))
            .expect_err("a mistyped field is refused");
        assert!(matches!(mistyped, PricingError::FieldType { .. }));
        assert!(!mistyped.is_incompatible());

        let not_a_record = PriceBookBody::read(&ResourceVersion::new(
            fixtures::reference(ResourceKind::Price, 7),
            ResourceScope::Deployment,
            fixtures::price_book(&fixtures::approved_price_book(), 7, "baseline").slug,
            ResourceBody::Inline(CanonicalValue::integer(1)),
        ))
        .expect_err("a non-record body is refused");
        assert!(matches!(not_a_record, PricingError::NotARecord { .. }));
        assert!(!not_a_record.is_incompatible());
    }

    /// A body that reads as a record but cannot be encoded is named for what is
    /// wrong with it: the refusal an operator reads has to point at the string
    /// nothing could have written, not at the form of a body that is a record.
    #[test]
    fn a_body_no_writer_could_have_encoded_names_its_encoding() {
        let CanonicalValue::Map(mut fields) = fixtures::approved_price_book().canonical() else {
            panic!("a body is a record");
        };
        let Some((_, CanonicalValue::Set(rules))) =
            fields.iter().find(|(name, _)| name == RULES_FIELD)
        else {
            panic!("a body carries a rule set");
        };
        let CanonicalValue::Map(mut rule) = rules.first().expect("one rule").clone() else {
            panic!("a rule is a record");
        };
        rule.retain(|(name, _)| name != MODEL_FIELD);
        rule.push((
            MODEL_FIELD.to_owned(),
            CanonicalValue::string("gpt-4o\u{1}"),
        ));
        fields.retain(|(name, _)| name != RULES_FIELD);
        fields.push((
            RULES_FIELD.to_owned(),
            CanonicalValue::set([CanonicalValue::map(rule)]),
        ));

        let mut state = fixtures::state();
        state
            .insert(ResourceVersion::new(
                fixtures::reference(ResourceKind::Price, 7),
                ResourceScope::Deployment,
                fixtures::price_book(&fixtures::approved_price_book(), 7, "baseline").slug,
                ResourceBody::Inline(CanonicalValue::map(fields)),
            ))
            .expect("a distinct reference");

        let error = PriceBooks::of(&state).expect_err("a body with no checksum has no identity");
        assert!(
            matches!(
                error,
                PricingError::Uncanonicalizable {
                    source: CanonicalError::ControlCharacter { .. },
                    ..
                }
            ),
            "{error}"
        );
        // No release encodes a control character, so this is a rewritten body
        // rather than one an older build merely cannot read.
        assert!(!error.is_incompatible());
    }

    /// The rules are a *set* on read as well as on write: the fingerprint is taken
    /// over the stored body, and only the set encoding sorts and deduplicates, so
    /// a list would give one book of rules a checksum per ordering.
    #[test]
    fn rules_stated_in_an_order_are_refused_rather_than_fingerprinted() {
        let CanonicalValue::Map(fields) = fixtures::approved_price_book().canonical() else {
            panic!("a body is a record");
        };
        let Some((_, CanonicalValue::Set(rules))) =
            fields.iter().find(|(name, _)| name == RULES_FIELD)
        else {
            panic!("a body carries a rule set");
        };
        let error = read_with_field(RULES_FIELD, CanonicalValue::List(rules.clone()))
            .expect_err("an ordered rule field is refused");
        assert!(
            matches!(
                error,
                PricingError::FieldType {
                    field: RULES_FIELD,
                    ..
                }
            ),
            "{error}"
        );
    }

    /// A rate the runtime cannot bill is skew rather than corruption: a newer
    /// release may meter usage this one does not.
    #[test]
    fn a_rate_this_build_cannot_bill_is_read_as_an_incompatibility() {
        let error = read_rule_with(|fields| {
            fields.push((
                RATES_FIELD.to_owned(),
                CanonicalValue::map([
                    (INPUT_FIELD, CanonicalValue::integer(1_500)),
                    (OUTPUT_FIELD, CanonicalValue::integer(1_000)),
                ]),
            ));
        })
        .expect_err("a rate finer than a micro-dollar is refused");
        assert!(matches!(
            error,
            PricingError::Rate {
                source: RateRejection::ExcessPrecision { .. },
                ..
            }
        ));
        assert!(error.is_incompatible());
    }

    #[test]
    fn a_negative_rate_is_refused() {
        let error = read_rule_with(|fields| {
            fields.push((
                RATES_FIELD.to_owned(),
                CanonicalValue::map([
                    (INPUT_FIELD, CanonicalValue::integer(-1_000)),
                    (OUTPUT_FIELD, CanonicalValue::integer(1_000)),
                ]),
            ));
        })
        .expect_err("a negative rate is refused");
        assert!(matches!(
            error,
            PricingError::Rate {
                source: RateRejection::Negative { .. },
                ..
            }
        ));
        // The operator repairing this by hand needs the model, not just the
        // column: the message names both.
        assert!(error.to_string().contains(&target().to_string()));
        assert!(error.to_string().contains(INPUT_FIELD));
        // No release wrote this: an `ApprovedRate` is unsigned. So it is a
        // rewritten body, not a newer build's rate.
        assert!(!error.is_incompatible());
    }

    #[test]
    fn a_rate_beyond_the_units_range_is_refused_as_an_overflow() {
        let error = read_rule_with(|fields| {
            fields.push((
                RATES_FIELD.to_owned(),
                CanonicalValue::map([
                    (
                        INPUT_FIELD,
                        CanonicalValue::Integer(i128::from(u64::MAX) + 1),
                    ),
                    (OUTPUT_FIELD, CanonicalValue::integer(1_000)),
                ]),
            ));
        })
        .expect_err("a rate past the range is refused");
        assert!(matches!(
            error,
            PricingError::Rate {
                source: RateRejection::Overflow { .. },
                ..
            }
        ));
        assert!(error.to_string().contains(&target().to_string()));
        // Beyond the range every writer encodes in, so likewise corruption
        // rather than skew.
        assert!(!error.is_incompatible());
    }

    /// A tiered schedule is refused *as a tier*, naming the threshold, rather
    /// than silently billed at its base rate.
    #[test]
    fn a_context_tiered_schedule_is_refused_naming_the_threshold() {
        let error = read_rule_with(|fields| {
            fields.push((
                TIERS_FIELD.to_owned(),
                CanonicalValue::List(vec![CanonicalValue::map([(
                    THRESHOLD_FIELD,
                    CanonicalValue::map([
                        (TYPE_FIELD, CanonicalValue::string("context_over")),
                        ("tokens", CanonicalValue::integer(128_000)),
                    ]),
                )])]),
            ));
        })
        .expect_err("a tiered schedule is refused");
        let PricingError::Rate {
            source: RateRejection::UnsupportedTier { threshold },
            ..
        } = &error
        else {
            panic!("expected a tier rejection, got {error}");
        };
        assert_eq!(threshold, "context_over");
        assert!(error.is_incompatible());
    }

    #[test]
    fn unknown_enumerated_spellings_are_incompatibilities() {
        let precedence = read_rule_with(|fields| {
            fields.retain(|(name, _)| name != PRECEDENCE_FIELD);
            fields.push((
                PRECEDENCE_FIELD.to_owned(),
                CanonicalValue::string("contractual"),
            ));
        })
        .expect_err("an unknown precedence is refused");
        assert!(matches!(precedence, PricingError::UnknownPrecedence { .. }));
        assert!(precedence.is_incompatible());

        let state = read_with_field(
            APPROVAL_FIELD,
            CanonicalValue::map([(STATE_FIELD, CanonicalValue::string("countersigned"))]),
        )
        .expect_err("an unknown approval state is refused");
        assert!(matches!(state, PricingError::UnknownApprovalState { .. }));
        assert!(state.is_incompatible());
    }

    /// A citation an earlier release accepted and this one does not is a version
    /// mismatch, exactly as it is for every other body's display names: the row
    /// is intact and nobody should be sent to repair it.
    #[test]
    fn a_citation_this_build_will_not_take_is_read_as_an_incompatibility() {
        let error = read_with_field(
            APPROVAL_FIELD,
            CanonicalValue::map([
                (STATE_FIELD, CanonicalValue::string("approved")),
                (APPROVED_BY_FIELD, fixtures::actor().canonical()),
                (APPROVED_AT_FIELD, CanonicalValue::integer(1)),
                // A name a tightened rule refuses; the citation itself is legible.
                (CITATION_FIELD, CanonicalValue::string(" CHG-1")),
            ]),
        )
        .expect_err("an unreadable citation is refused");
        assert!(matches!(
            error,
            PricingError::MalformedCitation {
                field: CITATION_FIELD,
                ..
            }
        ));
        assert!(
            error.is_incompatible(),
            "a citation rule that tightened is skew, not damage: {error}"
        );
    }

    /// The other half of the same line: what a body's identity is spelled in is
    /// not prose whose rules tighten, so an unparseable catalogue id or checksum
    /// is damage. Widening either spelling changes what the field means, and that
    /// is a new schema identifier — which *is* skew.
    #[test]
    fn an_unreadable_identity_is_read_as_damage_and_not_as_skew() {
        let checksum = read_with_field(CATALOG_FIELD, CanonicalValue::string("sha512:beef"))
            .expect_err("a digest this build does not state is refused");
        assert!(matches!(
            checksum,
            PricingError::MalformedChecksum {
                field: CATALOG_FIELD,
                ..
            }
        ));
        assert!(
            !checksum.is_incompatible(),
            "an identity nothing can verify is damage: {checksum}"
        );

        let provider = read_rule_with(|fields| {
            fields.retain(|(name, _)| name != PROVIDER_FIELD);
            fields.push((PROVIDER_FIELD.to_owned(), CanonicalValue::string("Open AI")));
        })
        .expect_err("an unreadable provider id is refused");
        assert!(matches!(
            provider,
            PricingError::MalformedId {
                field: PROVIDER_FIELD,
                ..
            }
        ));
        assert!(
            !provider.is_incompatible(),
            "a target nothing can route to is damage: {provider}"
        );
    }

    /// A draft carrying approval evidence is a reading this build would have to
    /// drop, so it is refused as skew: the stored bytes name an approver, and a
    /// reduced reading would publish a checksum those bytes do not have.
    #[test]
    fn a_draft_book_carrying_approval_evidence_is_refused() {
        for field in [APPROVED_BY_FIELD, APPROVED_AT_FIELD, CITATION_FIELD] {
            let value = match field {
                APPROVED_BY_FIELD => fixtures::actor().canonical(),
                APPROVED_AT_FIELD => CanonicalValue::integer(1),
                _ => CanonicalValue::string("CHG-1"),
            };
            let error = read_with_field(
                APPROVAL_FIELD,
                CanonicalValue::map([
                    (STATE_FIELD, CanonicalValue::string("draft")),
                    (field, value),
                ]),
            )
            .expect_err("a draft naming an approval is refused");
            let PricingError::UnknownField { field: named, .. } = &error else {
                panic!("expected an unknown-field refusal, got {error}");
            };
            assert_eq!(named, field);
            assert!(error.is_incompatible(), "dropped evidence is skew: {error}");
        }
    }

    /// An approved book without a readable approver is unreadable, so "who
    /// approved this" cannot degrade into "someone".
    #[test]
    fn an_approved_book_with_no_readable_approver_is_refused() {
        let error = read_with_field(
            APPROVAL_FIELD,
            CanonicalValue::map([
                (STATE_FIELD, CanonicalValue::string("approved")),
                (APPROVED_AT_FIELD, CanonicalValue::integer(1)),
            ]),
        )
        .expect_err("an approval without an approver is refused");
        assert!(matches!(
            error,
            PricingError::MissingField { field: "by", .. }
        ));
    }

    /// An approver a later release extends is skew, not damage: the database says
    /// exactly who approved the book, in a spelling this build cannot read.
    #[test]
    fn an_approver_a_newer_release_wrote_is_read_as_an_incompatibility() {
        for approver in [
            CanonicalValue::map([
                ("kind", CanonicalValue::string("human")),
                ("issuer", CanonicalValue::string("https://idp.example")),
                ("subject", CanonicalValue::string("ops@example")),
                // A field a later release adds to the human approver.
                ("assurance", CanonicalValue::string("webauthn")),
            ]),
            // An approver kind a later release adds.
            CanonicalValue::map([("kind", CanonicalValue::string("delegate"))]),
        ] {
            let error = read_with_field(
                APPROVAL_FIELD,
                CanonicalValue::map([
                    (STATE_FIELD, CanonicalValue::string("approved")),
                    (APPROVED_BY_FIELD, approver),
                    (APPROVED_AT_FIELD, CanonicalValue::integer(1)),
                ]),
            )
            .expect_err("an approver this build cannot read is refused");
            assert!(matches!(error, PricingError::MalformedActor { .. }));
            assert!(
                error.is_incompatible(),
                "an approver a newer release wrote is skew, not damage: {error}"
            );
        }
    }

    /// The end of the timeline is an instant like any other: the resolution simply
    /// has no boundary after it, and saying so must not be a panic.
    #[test]
    fn a_resolution_at_the_end_of_the_timeline_has_no_boundary_after_it() {
        let last = EffectiveInstant::from_millis(u64::MAX);
        let body = PriceBookBody::new(fixtures::catalog_content_id(), approved()).with_rule(
            PriceRule::new(
                fixtures::priced_target("openai", "gpt-5.5"),
                RulePrecedence::Baseline,
                EffectiveInterval::bounded(EffectiveInstant::EPOCH, last).expect("non-empty"),
                ApprovedRates::new(
                    ApprovedRate::from_nanos(1_000),
                    ApprovedRate::from_nanos(2_000),
                ),
                PriceProvenance::stated(PriceOrigin::Catalogue),
            )
            .expect("billable"),
        );
        let snapshot = PricingSnapshot::of(&book_of(&body), last);
        assert_eq!(snapshot.effective().starts(), last);
        assert_eq!(snapshot.effective().ends(), None);
        // The rule ends at `last`, so nothing is priced there.
        assert_eq!(snapshot.targets().len(), 0);
    }

    /// A service account is an approver like any other: the audit trail records
    /// one, so a book it approved must read back as the same principal rather than
    /// as a version mismatch.
    #[test]
    fn a_service_account_can_approve_a_book() {
        let by = Actor::Workload {
            tenant: fixtures::tenant_id(1),
            principal: fixtures::principal_id(9),
        };
        let body = PriceBookBody::new(
            fixtures::catalog_content_id(),
            Approval::Approved {
                by: by.clone(),
                at: EffectiveInstant::EPOCH,
                citation: None,
            },
        );
        let read =
            PriceBookBody::read(&fixtures::price_book(&body, 7, "baseline")).expect("readable");
        assert_eq!(read.approval().approver(), Some(&by));
        assert_eq!(read, body, "and the book itself round trips");
    }

    /// A workload approver missing the principal it names is damage, not skew:
    /// this build knows the kind, so the record is one no writer produced.
    #[test]
    fn a_workload_approver_without_its_principal_is_damaged() {
        let error = read_with_field(
            APPROVAL_FIELD,
            CanonicalValue::map([
                (STATE_FIELD, CanonicalValue::string("approved")),
                (
                    APPROVED_BY_FIELD,
                    CanonicalValue::map([
                        ("kind", CanonicalValue::string("workload")),
                        (
                            "tenant",
                            CanonicalValue::string(fixtures::tenant_id(1).to_string()),
                        ),
                    ]),
                ),
                (APPROVED_AT_FIELD, CanonicalValue::integer(1)),
            ]),
        )
        .expect_err("an approver missing its principal is refused");
        assert!(matches!(error, PricingError::MalformedActor { .. }));
        assert!(
            !error.is_incompatible(),
            "a known approver kind missing a field is damage: {error}"
        );
    }

    #[test]
    fn an_approval_records_its_approver_and_citation() {
        let body = fixtures::approved_price_book();
        let read =
            PriceBookBody::read(&fixtures::price_book(&body, 7, "baseline")).expect("readable");
        let Approval::Approved { by, at, citation } = read.approval() else {
            panic!("the fixture book is approved");
        };
        assert_eq!(by, &fixtures::actor());
        assert_eq!(*at, EffectiveInstant::EPOCH);
        assert_eq!(citation.as_ref().map(DisplayName::as_str), Some("CHG-1"));
    }

    /// A book is validated where every revision is, so a rejected price book
    /// cannot be published and a retained one is read the same way on hydration.
    #[test]
    fn a_revision_carrying_an_unbillable_book_does_not_validate() {
        let body = PriceBookBody::new(fixtures::catalog_content_id(), approved());
        let resource = fixtures::price_book(&body, 7, "baseline");
        let mut state = fixtures::state();
        state.insert(resource).expect("a distinct reference");
        // A readable, empty book is valid: it approves nothing.
        state.validate().expect("an empty book is valid");

        let mut broken = fixtures::state();
        broken
            .insert(ResourceVersion::new(
                fixtures::reference(ResourceKind::Price, 7),
                ResourceScope::Deployment,
                fixtures::price_book(&body, 7, "baseline").slug,
                ResourceBody::Inline(CanonicalValue::map([(
                    SCHEMA_FIELD,
                    CanonicalValue::string("axond.price-book.v99"),
                )])),
            ))
            .expect("a distinct reference");
        let error = broken
            .validate()
            .expect_err("an unreadable book is not publishable");
        assert!(error.to_string().contains("price-book"), "{error}");
    }

    /// The instants a wall clock produces, and the two it cannot.
    #[test]
    fn an_instant_off_the_timeline_is_refused_rather_than_clamped() {
        assert_eq!(
            EffectiveInstant::of(SystemTime::UNIX_EPOCH).expect("the epoch is on the timeline"),
            EffectiveInstant::EPOCH
        );
        assert_eq!(
            EffectiveInstant::of(SystemTime::UNIX_EPOCH - Duration::from_millis(1)),
            Err(InvalidInstant::BeforeEpoch)
        );
        let instant = at(1_700_000_000_000);
        assert_eq!(
            EffectiveInstant::of(instant.to_system_time().expect("a representable instant"))
                .expect("round trip"),
            instant
        );
        // A stored instant may be any u64 of milliseconds, which is further ahead
        // than a `SystemTime` reaches on some hosts. How far *this* host reaches is
        // the host's business; that the conversion answers instead of aborting is
        // not, so the far end of the range is asked rather than assumed refused.
        let far = EffectiveInstant::from_millis(u64::MAX);
        if let Some(time) = far.to_system_time() {
            assert_eq!(EffectiveInstant::of(time), Ok(far));
        }
    }

    /// Build a fixture body's canonical record with one field replaced, so a test
    /// asserts on the reader rather than on a hand-written body.
    fn read_with_field(field: &str, value: CanonicalValue) -> Result<PriceBookBody, PricingError> {
        let CanonicalValue::Map(mut fields) = fixtures::approved_price_book().canonical() else {
            panic!("a body is a record");
        };
        fields.retain(|(name, _)| name != field);
        fields.push((field.to_owned(), value));
        read_body(CanonicalValue::map(fields))
    }

    fn read_without_field(field: &str) -> Result<PriceBookBody, PricingError> {
        let CanonicalValue::Map(mut fields) = fixtures::approved_price_book().canonical() else {
            panic!("a body is a record");
        };
        fields.retain(|(name, _)| name != field);
        read_body(CanonicalValue::map(fields))
    }

    /// The fixture book's single rule, mutated.
    fn read_rule_with(
        mutate: impl FnOnce(&mut Vec<(String, CanonicalValue)>),
    ) -> Result<PriceBookBody, PricingError> {
        let CanonicalValue::Map(fields) = fixtures::approved_price_book().canonical() else {
            panic!("a body is a record");
        };
        let Some((_, CanonicalValue::Set(rules))) =
            fields.iter().find(|(name, _)| name == RULES_FIELD)
        else {
            panic!("a body carries a rule set");
        };
        let CanonicalValue::Map(mut rule) = rules.first().expect("one rule").clone() else {
            panic!("a rule is a record");
        };
        mutate(&mut rule);
        // Keep the *last* spelling of each field, so a mutation pushing `rates`
        // replaces the fixture's rather than being shadowed by it.
        let mut seen = std::collections::BTreeSet::new();
        rule.reverse();
        rule.retain(|(name, _)| seen.insert(name.clone()));
        let mut fields = fields;
        fields.retain(|(name, _)| name != RULES_FIELD);
        fields.push((
            RULES_FIELD.to_owned(),
            CanonicalValue::set([CanonicalValue::map(rule)]),
        ));
        read_body(CanonicalValue::map(fields))
    }

    fn read_body(value: CanonicalValue) -> Result<PriceBookBody, PricingError> {
        PriceBookBody::read(&ResourceVersion::new(
            fixtures::reference(ResourceKind::Price, 7),
            ResourceScope::Deployment,
            fixtures::price_book(&fixtures::approved_price_book(), 7, "baseline").slug,
            ResourceBody::Inline(value),
        ))
    }
}
