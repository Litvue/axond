//! The durable usage journal contract (#155): what a zero-silent-loss delivery
//! mode has to guarantee, expressed without reference to any storage engine.
//!
//! Today's sinks are telemetry-grade by construction (ADR 0009): the request
//! path enqueues with a non-blocking `try_send`, and a destination that cannot
//! keep up loses records, counted on `axond.usage.records_dropped`. That is the
//! right default — a usage write is worth less than the request it describes —
//! and nothing here changes it. This module is the contract for operators who
//! bill from usage, for whom a dropped record is a missing invoice line.
//!
//! # What a journal is, and what it is not
//!
//! A [`UsageJournal`] is **not** a [`UsageSink`](super::UsageSink). A sink is a
//! destination: it is handed a record and reports nothing an operator can act on
//! later. A journal is a *log with consumers*: an event is appended, claimed by a
//! named consumer, and only forgotten once that consumer acknowledges it. The
//! two traits stay separate because their contracts are opposites — a sink may
//! drop, a journal may not — and flattening them would make the drop-or-not
//! decision invisible at the call site.
//!
//! # The five operations
//!
//! | Operation | Guarantee |
//! | --- | --- |
//! | [`append`](UsageJournal::append) | An accepted event is durable, or the caller is told it was not. Re-appending the same [`IdempotencyKey`] is [`Appended::AlreadyPresent`], never a second event; re-appending it with *different* content is a [`JournalError::Conflict`] rather than a silent overwrite. |
//! | [`claim`](UsageJournal::claim) | Hands a consumer a bounded batch of unacknowledged events under a lease. Claims respect [`OrderingKey`], so one caller's events are delivered in append order. |
//! | [`ack`](UsageJournal::ack) | Idempotent: an event acknowledged twice is acknowledged once, so a crash between the sink write and the ack is safe to retry. |
//! | [`quarantine`](UsageJournal::quarantine) | A poison event leaves the delivery path explicitly instead of blocking its ordering key forever. |
//! | [`stats`](UsageJournal::stats) | Depth, in-flight count, oldest pending age, quarantine count, and the [`Capacity`] they are bounded by — the numbers an operator alerts on. |
//!
//! # Replay, duplicates, and where idempotency actually lands
//!
//! Delivery is **at least once**, and deliberately so: an exactly-once journal
//! would have to commit the consumer's side effect and its own acknowledgement in
//! one transaction, which is not available for an arbitrary sink. A lease that
//! expires (because the process holding it crashed) makes its events claimable
//! again, so *replay is the normal case*, not the exceptional one.
//!
//! What makes replay safe is that every event carries a stable
//! [`IdempotencyKey`] — the [`RequestId`]'s text form, which is also the
//! `request_id` column of the shipped usage schema. A
//! conforming consumer constrains that value and lets the second write collide.
//! Each *delivery* additionally has its own identity ([`DeliveryId`]), so a
//! redelivery is distinguishable from a first attempt in logs and metrics without
//! being distinguishable as a billable event.
//!
//! # Not enabled here
//!
//! No journal is constructed by `serve`, no configuration selects one, and the
//! settlement order is unchanged: this slice is the contract plus its executable
//! oracle, in the same posture as [`crate::backends`] and
//! [`crate::desired_state`]. The Postgres outbox worker that implements it — and
//! the append-before-settlement ordering that makes
//! [`DeliveryMode::BillingGrade`] true rather than merely named — is the next
//! slice.

#[cfg(test)]
pub(crate) mod oracle;
#[cfg(test)]
mod tests;

use std::fmt;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;

use super::identity::RequestId;
use super::{ObservedRecord, UsageRecord};

/// Which delivery guarantee a deployment has asked for.
///
/// Telemetry-grade is the default and the only mode that exists on the request
/// path today: sinks buffer, and a full buffer drops. Billing-grade names the
/// opt-in mode a journal implementation enables — an accepted request's event is
/// durable before the request is settled — so the two postures have one name each
/// in documentation, metrics, and configuration rather than being inferred from
/// which sinks happen to be configured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeliveryMode {
    /// Best-effort, non-blocking, lossy under overload. The default.
    #[default]
    TelemetryGrade,
    /// Durable append before settlement, replayed until acknowledged.
    BillingGrade,
}

impl DeliveryMode {
    /// Stable, low-cardinality label — the same vocabulary a metric dimension
    /// and the documentation use.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TelemetryGrade => "telemetry_grade",
            Self::BillingGrade => "billing_grade",
        }
    }

    /// Whether an accepted event survives the loss of the process that accepted
    /// it. False for the default mode, which is the honest answer.
    pub fn is_durable(self) -> bool {
        matches!(self, Self::BillingGrade)
    }
}

/// The value a conforming consumer deduplicates on.
///
/// It is the event's [`RequestId`] in text form, which is exactly what lands in
/// the `request_id` column, so "the key the journal promises is unique" and "the
/// column a billing table constrains" are the same string rather than two
/// identifiers that have to be kept in step.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<RequestId> for IdempotencyKey {
    fn from(id: RequestId) -> Self {
        Self(id.to_string())
    }
}

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The unit ordering is promised within: one caller inside one namespace.
///
/// Ordering is per key rather than global on purpose. A global order would make
/// every consumer as slow as its slowest event and would serialize a fleet's
/// deliveries behind one another; a per-caller order is what a billing reader
/// actually needs, because reconciling one subject's spend is a walk over that
/// subject's events.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrderingKey {
    pub namespace: String,
    pub subject: String,
}

impl OrderingKey {
    fn of(record: &UsageRecord) -> Self {
        Self {
            namespace: record.namespace.clone(),
            subject: record.subject.clone(),
        }
    }
}

impl fmt::Display for OrderingKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.namespace, self.subject)
    }
}

/// One accepted usage event: an immutable envelope around the canonical record.
///
/// Immutable in the strong sense — the fields are private and there is no setter,
/// so nothing between acceptance and acknowledgement can alter what was
/// journaled. A correction is a new event, never an edit, because a consumer that
/// already delivered the old content cannot un-deliver it.
#[derive(Debug, Clone, PartialEq)]
pub struct UsageEvent {
    id: RequestId,
    idempotency_key: IdempotencyKey,
    ordering_key: OrderingKey,
    record: UsageRecord,
    observed_at: SystemTime,
}

/// Why a record could not become an event.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidEvent {
    #[error("`{request_id}` is not a usage event identity: {reason}")]
    Identity { request_id: String, reason: String },
}

impl UsageEvent {
    /// Wrap an observed record, taking its identity from the record's own
    /// `request_id`.
    ///
    /// The identity is not minted here: it was minted once when the request was
    /// accepted and is already on the record, in the span, and in the metrics. A
    /// record whose id is not a UUIDv7 — one built by an older writer, or one
    /// that lost its identity in a round trip — is refused rather than given a
    /// fresh id, because a re-identified event is a duplicate a consumer cannot
    /// detect.
    pub fn new(observed: ObservedRecord) -> Result<Self, InvalidEvent> {
        let id =
            RequestId::parse(&observed.record.request_id).map_err(|e| InvalidEvent::Identity {
                request_id: observed.record.request_id.clone(),
                reason: e.to_string(),
            })?;
        Ok(Self {
            id,
            idempotency_key: IdempotencyKey::from(id),
            ordering_key: OrderingKey::of(&observed.record),
            record: observed.record,
            observed_at: observed.observed_at,
        })
    }

    pub fn id(&self) -> RequestId {
        self.id
    }

    pub fn idempotency_key(&self) -> &IdempotencyKey {
        &self.idempotency_key
    }

    pub fn ordering_key(&self) -> &OrderingKey {
        &self.ordering_key
    }

    pub fn record(&self) -> &UsageRecord {
        &self.record
    }

    /// When the fan-out first saw the record — the row's `recorded_at`, carried
    /// through the journal so a replay written days later still says when the
    /// request happened.
    pub fn observed_at(&self) -> SystemTime {
        self.observed_at
    }

    /// The event as the sinks want it, so a journal consumer and the best-effort
    /// path write identical rows.
    pub fn observed(&self) -> ObservedRecord {
        ObservedRecord {
            record: self.record.clone(),
            observed_at: self.observed_at,
        }
    }
}

/// A named consumer of the journal.
///
/// Delivery state is per consumer, so adding a second destination (a warehouse
/// beside the billing table) does not make the first one's acknowledgements
/// ambiguous, and a new consumer's backlog is visible as its own depth.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConsumerId(String);

/// Why a consumer name was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidConsumerId {
    #[error("a consumer name must not be empty")]
    Empty,
    #[error("consumer name `{name}` is over the {max}-character limit")]
    TooLong { name: String, max: usize },
    #[error(
        "consumer name `{name}` contains `{character}`; use lowercase letters, digits, `-`, and `_`"
    )]
    Character { name: String, character: char },
}

impl ConsumerId {
    pub const MAX_LEN: usize = 63;

    /// Parse a consumer name. Narrow and ASCII-only, because the name is a
    /// storage key, a metric dimension, and something an operator types.
    pub fn parse(name: &str) -> Result<Self, InvalidConsumerId> {
        if name.is_empty() {
            return Err(InvalidConsumerId::Empty);
        }
        if name.len() > Self::MAX_LEN {
            return Err(InvalidConsumerId::TooLong {
                name: name.to_owned(),
                max: Self::MAX_LEN,
            });
        }
        if let Some(character) = name
            .chars()
            .find(|c| !(c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '-' || *c == '_'))
        {
            return Err(InvalidConsumerId::Character {
                name: name.to_owned(),
                character,
            });
        }
        Ok(Self(name.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ConsumerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The identity of one delivery attempt: which consumer, which event, which
/// attempt.
///
/// Distinct from the event's identity on purpose. The event id answers "is this
/// the same billable fact?" (and must not change across a replay); the delivery
/// id answers "is this the same attempt?" (and must). A log line or a metric can
/// therefore say a delivery was retried without a consumer concluding that the
/// caller was billed twice.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeliveryId {
    pub consumer: ConsumerId,
    pub event: RequestId,
    /// 1 for the first attempt. A value above 1 is a redelivery.
    pub attempt: u32,
}

impl DeliveryId {
    /// Whether this delivery is a replay of one that was never acknowledged.
    pub fn is_redelivery(&self) -> bool {
        self.attempt > 1
    }
}

impl fmt::Display for DeliveryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}#{}", self.consumer, self.event, self.attempt)
    }
}

/// One claimed event, leased to one consumer until `lease_expires_at`.
///
/// The lease is what makes crash recovery automatic: a consumer that dies
/// mid-delivery stops renewing, the lease expires, and the event is claimable
/// again. There is no unlock call to lose.
#[derive(Debug, Clone, PartialEq)]
pub struct Delivery {
    pub id: DeliveryId,
    pub event: UsageEvent,
    pub lease_expires_at: SystemTime,
}

/// What one [`claim`](UsageJournal::claim) asks for.
///
/// The clock is a parameter rather than read inside the journal so lease expiry
/// is testable — and so a store whose authority on time is the database can pass
/// its own.
#[derive(Debug, Clone, Copy)]
pub struct Claim {
    /// Upper bound on events returned. A claim is always bounded: a consumer
    /// that asked for "everything" would hold a lease it cannot finish.
    pub max_events: usize,
    /// How long the returned events stay invisible to other claimants.
    pub lease: Duration,
    pub now: SystemTime,
}

/// The outcome of an append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Appended {
    /// The event is durable and will be delivered.
    Accepted {
        /// Monotonic position in the journal. Not the event's identity: it is a
        /// storage detail a consumer may use to resume, and it is not comparable
        /// across journals.
        position: u64,
    },
    /// The same idempotency key was already appended, carrying the same content.
    /// The benign case — a retried append after an unknown outcome — and *not*
    /// an error: the caller's intent is already satisfied.
    AlreadyPresent { position: u64 },
}

impl Appended {
    pub fn position(&self) -> u64 {
        match self {
            Self::Accepted { position } | Self::AlreadyPresent { position } => *position,
        }
    }

    /// Whether this append added an event, as opposed to recognising one.
    pub fn is_new(&self) -> bool {
        matches!(self, Self::Accepted { .. })
    }
}

/// Why an event left the delivery path instead of being retried forever.
///
/// A bounded vocabulary, because it is a metric dimension and an operator's
/// first question after "how many?".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoisonReason {
    /// The consumer could not interpret the event: a shape no reader accepts is
    /// not going to become acceptable on the next attempt.
    Malformed,
    /// The destination rejected it permanently (a constraint that is not the
    /// idempotency constraint, a value it will never accept).
    Rejected,
    /// The delivery attempt budget in [`Capacity::max_delivery_attempts`] ran
    /// out. The failure may be transient, but retrying it is now indistinguishable
    /// from blocking every later event for the same ordering key.
    AttemptsExhausted,
}

impl PoisonReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Malformed => "malformed",
            Self::Rejected => "rejected",
            Self::AttemptsExhausted => "attempts_exhausted",
        }
    }
}

/// What a full journal does. Explicit, because "storage is full" is the moment a
/// durability promise is either kept or quietly broken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapacityPolicy {
    /// Refuse the append. The only policy that keeps a billing-grade promise: the
    /// caller is told the event is not durable, and can decide (shed the request,
    /// page someone) rather than discovering the gap in a bill.
    Refuse,
    /// Drop the oldest unacknowledged event to make room. Bounded storage at the
    /// cost of losing the events that were waiting longest — telemetry-grade
    /// behaviour, chosen deliberately rather than by accident.
    DropOldest,
}

impl CapacityPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Refuse => "refuse",
            Self::DropOldest => "drop_oldest",
        }
    }

    /// Whether this policy can lose an accepted event.
    pub fn can_lose_events(self) -> bool {
        matches!(self, Self::DropOldest)
    }
}

/// The bounds a journal is operated under. Metadata rather than configuration in
/// this slice: it is what an implementation reports about itself, so an operator
/// can see the limit next to the depth it is being compared against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capacity {
    /// Unacknowledged events the journal will hold.
    pub max_events: u64,
    /// Attempts one event gets before it is quarantined as poison.
    pub max_delivery_attempts: u32,
    /// How long an acknowledged event is retained before it may be pruned. A
    /// retention window rather than an immediate delete, so a consumer that
    /// double-acknowledges after a restart still finds the event it is talking
    /// about instead of an absence it has to interpret.
    pub retain_acknowledged: Duration,
    pub policy: CapacityPolicy,
}

impl Capacity {
    /// A conservative default for a billing-grade journal: refuse rather than
    /// lose, and give an event a bounded number of attempts.
    pub const BILLING_GRADE: Self = Self {
        max_events: 1_000_000,
        max_delivery_attempts: 8,
        retain_acknowledged: Duration::from_secs(24 * 60 * 60),
        policy: CapacityPolicy::Refuse,
    };
}

/// What one consumer's delivery position looks like right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalStats {
    /// Appended, not acknowledged, not quarantined, not currently leased.
    pub pending: u64,
    /// Claimed under a lease that has not expired.
    pub in_flight: u64,
    /// Set aside as poison, awaiting an operator.
    pub quarantined: u64,
    /// Age of the oldest pending event — the number that says how far behind a
    /// bill is, which a raw depth does not.
    pub oldest_pending_age: Option<Duration>,
    /// Events lost to [`CapacityPolicy::DropOldest`]. Always zero under
    /// [`CapacityPolicy::Refuse`], and the metric that makes a lossy policy's
    /// cost visible.
    pub dropped: u64,
    pub capacity: Capacity,
}

impl JournalStats {
    /// Whether every appended event has been accounted for by a consumer.
    pub fn is_drained(&self) -> bool {
        self.pending == 0 && self.in_flight == 0
    }
}

/// Why a journal operation did not do what was asked.
///
/// Typed, unlike [`SinkFailure`](super::SinkFailure): a sink failure is
/// operational and every caller treats it the same way (count, log, move on),
/// while these are decisions — refuse the request, page an operator, fix a bug —
/// and collapsing them into a string would erase the difference.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JournalError {
    /// The journal is full and its policy is to refuse. The caller has *not*
    /// journaled the event.
    #[error(
        "usage journal is at capacity ({pending} unacknowledged events, limit {}); the event was not journaled",
        capacity.max_events
    )]
    AtCapacity { pending: u64, capacity: Capacity },
    /// The same idempotency key was appended with different content. A bug (a
    /// reused id, a mutated record), never a retry, so it is refused instead of
    /// overwriting a fact a consumer may already have delivered.
    #[error("usage event `{key}` was already journaled with different content")]
    Conflict { key: IdempotencyKey },
    /// The delivery is not outstanding: never claimed, or its lease expired and
    /// the event was claimed by someone else. Acknowledging an already
    /// acknowledged delivery is *not* this error — that case is `Ok`.
    #[error("delivery `{delivery}` is not outstanding")]
    NotOutstanding { delivery: DeliveryId },
    /// The storage engine failed. Operational, and the one variant that carries
    /// only a message.
    #[error("usage journal backend: {0}")]
    Backend(String),
}

/// A durable, replayable log of accepted usage events with per-consumer delivery
/// state.
///
/// The trait is the extension point the Postgres outbox worker implements: an
/// `axond_usage_outbox` table for the events, a per-consumer cursor table for
/// delivery state, `FOR UPDATE SKIP LOCKED` for [`claim`](Self::claim), and a
/// lease column for expiry. Nothing in the signatures assumes that — an
/// implementation over a local write-ahead file satisfies the same contract —
/// but the shapes were chosen so the SQL one needs no additions.
#[async_trait]
pub trait UsageJournal: Send + Sync {
    /// Stable, low-cardinality name — a metric dimension.
    fn name(&self) -> &'static str;

    /// The bounds this journal is operated under.
    fn capacity(&self) -> Capacity;

    /// The guarantee this journal provides. A journal that cannot survive the
    /// loss of its process must say [`DeliveryMode::TelemetryGrade`], so nothing
    /// claims durability it does not have.
    fn mode(&self) -> DeliveryMode;

    /// Append an accepted event, idempotently on its
    /// [`IdempotencyKey`](UsageEvent::idempotency_key).
    async fn append(&self, event: &UsageEvent) -> Result<Appended, JournalError>;

    /// Claim up to `claim.max_events` deliverable events for `consumer`, leasing
    /// them until `claim.now + claim.lease`.
    ///
    /// Implementations must return at most one in-flight event per
    /// [`OrderingKey`], in append order: that is what makes per-caller ordering
    /// hold under concurrent consumers rather than only under a single-threaded
    /// one.
    async fn claim(
        &self,
        consumer: &ConsumerId,
        claim: Claim,
    ) -> Result<Vec<Delivery>, JournalError>;

    /// Acknowledge a delivered event. Idempotent: acknowledging a delivery whose
    /// event this consumer has already acknowledged is `Ok`, because a crash
    /// between the destination write and the acknowledgement must be recoverable
    /// by repeating the acknowledgement.
    async fn ack(&self, delivery: &DeliveryId) -> Result<(), JournalError>;

    /// Set an event aside as poison. It stops being delivered — and stops
    /// blocking its ordering key — and is counted in
    /// [`JournalStats::quarantined`] until an operator deals with it.
    async fn quarantine(
        &self,
        delivery: &DeliveryId,
        reason: PoisonReason,
    ) -> Result<(), JournalError>;

    /// This consumer's depth, in-flight count, oldest pending age, quarantine
    /// count, and the capacity they are bounded by.
    async fn stats(&self, consumer: &ConsumerId) -> Result<JournalStats, JournalError>;
}
