//! The immutable index and the evaluator over it (#206).
//!
//! An [`AvailabilityIndex`] holds one [`AvailabilityRecord`] per
//! [`AvailabilityKey`] — one scope, one target — and answers one question:
//! *given everything derived so far, what is this target's availability at this
//! instant?* It is built once, by an [`AvailabilityIndexBuilder`], and is
//! immutable afterwards. Refreshing availability publishes a replacement, exactly
//! as a config reload publishes a replacement [`ConfigSnapshot`], so no reader can
//! observe a half-updated index and two evaluations at the same instant cannot
//! disagree.
//!
//! [`ConfigSnapshot`]: crate::state::ConfigSnapshot
//!
//! # The precedence ladder
//!
//! Evaluation walks the dimensions in a fixed order and the first one that is not
//! satisfied decides. The order is not arbitrary — it is *severity of authority
//! first, uncertainty last*:
//!
//! 1. **Catalogue presence.** An absent or withdrawn entry means there is nothing
//!    to be entitled to; `unavailable`, and no other dimension is consulted,
//!    because entitlement or discovery evidence about a model this deployment does
//!    not carry is meaningless.
//! 2. **Policy refusal.** A deployment-level refusal outranks everything a tenant
//!    has enabled or is entitled to: `denied`.
//! 3. **Enablement.** The scope's own switch. `denied` when not enabled. Every rung
//!    that can answer `unknown` sits *below* this one — including an undecided
//!    policy, which is why policy is split across rungs 2 and 4 — so "unknown or
//!    stale evidence is routable only under explicit enablement" is structural
//!    rather than a rule a caller has to remember. Checked before entitlement
//!    because a target the scope never asked for should say so, rather than
//!    reporting whatever its account happens to be entitled to.
//! 4. **Policy indeterminacy.** An undecided policy is not a permit: `unknown`.
//! 5. **Entitlement.** The provider account's own answer. `denied` when missing or
//!    revoked, `unknown` when never established.
//! 6. **Runtime health.** Replica-local. An open circuit is `unavailable`, because
//!    this replica is already skipping the target; intermittent failure short of
//!    tripping is `unknown`, because the breaker would still attempt it (ADR 0008)
//!    and refusing it here would deny a fleet a target that mostly works. Placed
//!    above the evidence rung so a flaky target does not report as plainly
//!    `available` — what this replica is living through outranks what a listing
//!    said.
//! 7. **Discovery evidence.** `available`, `stale`, `unknown`, or `denied`, per
//!    [`DiscoveryObservation::verdict`], and `unknown` when nothing has looked
//!    yet — still decided by discovery, which is what distinguishes it from a key
//!    the index does not hold at all.
//!
//! Ties are impossible by construction: exactly one rung decides, and the verdict
//! records which one in [`Availability::decided_by`].
//!
//! # Last-known-good, and the no-silent-upgrade rule
//!
//! The builder is where a discovery outage is survived. Recording an observation
//! ([`AvailabilityIndexBuilder::observe`]) keeps the *last definitive positive*
//! look separately from the current one, and:
//!
//! - an indeterminate, partial, unsupported, or unreliable look replaces the
//!   current observation and **leaves last-known-good intact**, so an outage
//!   degrades a fleet to `available (last_known_good)` and then, at expiry, to
//!   `stale` — never straight to `denied`, and never to `unknown` while the
//!   retained evidence still counts;
//! - a definitive *negative* look clears last-known-good, because a complete
//!   listing that no longer carries the target is precisely the evidence that the
//!   retained positive is wrong;
//! - the current slot always holds the newest look, so an older arrival from a slow
//!   probe cannot rewind it, while what is *retained* is judged against every
//!   conclusive answer this key has ever reached — a watermark
//!   ([`AvailabilityRecord::definitive_at`]) rather than the looks still held,
//!   because a negative retains nothing and an inconclusive refresh displaces it
//!   from the current slot. The two advance independently, which is what makes the
//!   index independent of the order observations arrive in: a definitive look that
//!   lands after a newer *inconclusive* one still counts, while one that predates a
//!   conclusive answer cannot overturn it in either direction — an older negative
//!   does not discredit a later positive, and an older positive does not resurrect
//!   a target a later complete listing dropped. Two looks bearing the same instant
//!   resolve the same way whichever lands first: the negative holds;
//! - nothing ever infers a positive from a non-definitive look. Certainty only
//!   rises when definitive evidence arrives, which is what
//!   [`AvailabilityState::certainty`] makes testable.
//!
//! [`AvailabilityState::certainty`]: super::AvailabilityState::certainty
//!
//! # Not desired-state truth, and not on the request path
//!
//! An index is *derived*. It is projected onto a snapshot
//! ([`ConfigSnapshot::with_availability`]) as a value carried alongside the
//! config, and it never edits a config section: a projection cannot add a model, a
//! namespace, or a credential, so no amount of discovery evidence can enlarge what
//! the deployment declares. The direction of authority is one-way — desired state
//! and the catalogue decide what exists, and an index only says what is currently
//! reachable.
//!
//! Nothing in `serve` constructs one, and no evaluation happens on the request
//! path in this slice: [`evaluate`](AvailabilityIndex::evaluate) is a pure
//! function over data already in hand, with no lookup, no I/O, and no lock, so
//! wiring it in later cannot turn an inference request into a catalogue,
//! discovery, Postgres, Redis, or `SecretStore` read.
//!
//! [`ConfigSnapshot::with_availability`]: crate::state::ConfigSnapshot::with_availability

use std::collections::BTreeMap;
use std::time::SystemTime;

use super::dimensions::{
    CataloguePresence, Enablement, Entitlement, PolicyDecision, RuntimeHealth,
};
use super::discovery::DiscoveryObservation;
use super::refs::{AvailabilityKey, CredentialRef, ScopeRef, TargetRef};
use super::verdict::{Availability, AvailabilityReason, AvailabilityState, DecidedBy};

/// Everything derived about one target in one scope.
///
/// The five single-valued dimensions, the credential the entitlement was decided
/// against, the current discovery observation, and the retained last-known-good
/// one. Defaults are the ignorant ones: nothing is present, nothing is enabled,
/// nothing is entitled, and nothing has been observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvailabilityRecord {
    pub presence: CataloguePresence,
    pub enablement: Enablement,
    pub entitlement: Entitlement,
    pub policy: PolicyDecision,
    pub runtime: RuntimeHealth,
    /// Which credential the entitlement was decided against, when one was. A
    /// reference for correlation; never material, and never projected into a
    /// verdict.
    pub credential: Option<CredentialRef>,
    /// The most recent observation, whatever it said.
    pub discovery: Option<DiscoveryObservation>,
    /// The most recent *definitive positive* observation, retained across
    /// non-definitive ones so a discovery outage does not lose it.
    pub last_known_good: Option<DiscoveryObservation>,
    /// When the newest *definitive* look for this key was observed, whether or
    /// not it is still held anywhere.
    ///
    /// A watermark rather than an observation: a complete listing that dropped
    /// the target retains nothing, and an inconclusive refresh displaces it from
    /// [`discovery`](Self::discovery), so without this the fact that a
    /// conclusive answer was ever reached would be lost and a much older
    /// positive could be adopted afterwards.
    pub definitive_at: Option<SystemTime>,
}

impl Default for AvailabilityRecord {
    fn default() -> Self {
        Self {
            presence: CataloguePresence::Absent,
            enablement: Enablement::NotEnabled,
            entitlement: Entitlement::Unknown,
            policy: PolicyDecision::Indeterminate,
            runtime: RuntimeHealth::Unobserved,
            credential: None,
            discovery: None,
            last_known_good: None,
            definitive_at: None,
        }
    }
}

impl AvailabilityRecord {
    /// A record for a catalogued, enabled, policy-permitted target, with
    /// entitlement, runtime health, and discovery all still unobserved.
    ///
    /// The three authorities a deployment declares, and nothing more. It does
    /// not evaluate to `available`: entitlement keeps its ignorant default, so
    /// the ladder stops at the entitlement rung until the provider account's own
    /// answer is known. A projection that has resolved entitlement sets it —
    /// deliberately not something this constructor assumes, because an entitlement
    /// nobody established is exactly the uncertainty the states exist to report.
    pub fn enabled() -> Self {
        Self {
            presence: CataloguePresence::Present,
            enablement: Enablement::Enabled,
            policy: PolicyDecision::Permitted,
            ..Self::default()
        }
    }

    /// The observation evaluation should read: the current one when it is
    /// definitive, and the retained last-known-good one otherwise.
    ///
    /// Returns whether the observation is the retained one, so a verdict can say
    /// it is resting on last-known-good evidence.
    fn evidence(&self) -> Option<(&DiscoveryObservation, bool)> {
        match (&self.discovery, &self.last_known_good) {
            (Some(current), _) if current.is_definitive() => Some((current, false)),
            (_, Some(retained)) => Some((retained, true)),
            (Some(current), None) => Some((current, false)),
            (None, None) => None,
        }
    }
}

/// An immutable, already-derived index of availability records.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AvailabilityIndex {
    records: BTreeMap<AvailabilityKey, AvailabilityRecord>,
}

impl AvailabilityIndex {
    /// An index that knows nothing. What a snapshot carries until an availability
    /// projection produces one, and deliberately not an index that says
    /// everything is fine.
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn builder() -> AvailabilityIndexBuilder {
        AvailabilityIndexBuilder::default()
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// The record for one key, for an operator-facing dump. Carries the
    /// dimensions, not a verdict.
    pub fn record(&self, key: &AvailabilityKey) -> Option<&AvailabilityRecord> {
        self.records.get(key)
    }

    /// The availability of one target in one scope at `now`.
    ///
    /// Walks the precedence ladder in the module docs. A key the index does not
    /// hold is [`Availability::no_record`] — `unknown`, never available and never
    /// denied.
    pub fn evaluate(&self, key: &AvailabilityKey, now: SystemTime) -> Availability {
        let Some(record) = self.records.get(key) else {
            return Availability::no_record();
        };
        Self::evaluate_record(record, now)
    }

    fn evaluate_record(record: &AvailabilityRecord, now: SystemTime) -> Availability {
        match record.presence {
            CataloguePresence::Absent => {
                return Availability::decided(
                    AvailabilityState::Unavailable,
                    AvailabilityReason::NotInCatalogue,
                    DecidedBy::Catalogue,
                );
            }
            CataloguePresence::Withdrawn => {
                return Availability::decided(
                    AvailabilityState::Unavailable,
                    AvailabilityReason::WithdrawnFromCatalogue,
                    DecidedBy::Catalogue,
                );
            }
            CataloguePresence::Present => {}
        }
        if record.policy == PolicyDecision::Denied {
            return Availability::decided(
                AvailabilityState::Denied,
                AvailabilityReason::PolicyDenied,
                DecidedBy::Policy,
            );
        }
        // Above every rung that can answer `unknown`, so uncertainty is only ever
        // reported for a target a scope switched on.
        if !record.enablement.is_enabled() {
            return Availability::decided(
                AvailabilityState::Denied,
                AvailabilityReason::NotEnabled,
                DecidedBy::Enablement,
            );
        }
        if record.policy == PolicyDecision::Indeterminate {
            return Availability::decided(
                AvailabilityState::Unknown,
                AvailabilityReason::PolicyIndeterminate,
                DecidedBy::Policy,
            );
        }
        match record.entitlement {
            Entitlement::Missing => {
                return Availability::decided(
                    AvailabilityState::Denied,
                    AvailabilityReason::EntitlementMissing,
                    DecidedBy::Entitlement,
                );
            }
            Entitlement::Revoked => {
                return Availability::decided(
                    AvailabilityState::Denied,
                    AvailabilityReason::EntitlementRevoked,
                    DecidedBy::Entitlement,
                );
            }
            Entitlement::Unknown => {
                return Availability::decided(
                    AvailabilityState::Unknown,
                    AvailabilityReason::EntitlementUnknown,
                    DecidedBy::Entitlement,
                );
            }
            Entitlement::Granted => {}
        }
        match record.runtime {
            RuntimeHealth::Unavailable => {
                return Availability::decided(
                    AvailabilityState::Unavailable,
                    AvailabilityReason::RuntimeUnavailable,
                    DecidedBy::Runtime,
                );
            }
            // Not a refusal: an untripped target is still one the breaker would
            // attempt (ADR 0008), so impairment lowers certainty rather than
            // withdrawing the target. Deciding here — rather than falling through to
            // the evidence — keeps the impairment visible instead of reporting a
            // flaky target as plainly available.
            RuntimeHealth::Impaired => {
                return Availability::decided(
                    AvailabilityState::Unknown,
                    AvailabilityReason::RuntimeImpaired,
                    DecidedBy::Runtime,
                );
            }
            RuntimeHealth::Healthy | RuntimeHealth::Unobserved => {}
        }
        match record.evidence() {
            Some((observation, last_known_good)) => observation.verdict(now, last_known_good),
            // Every authority permits and discovery simply has not run yet. Reported
            // as decided by discovery, not as [`Availability::no_record`]: "nobody
            // has looked at a target this scope may use" and "nothing describes this
            // pair" send an operator to different places.
            None => Availability::decided(
                AvailabilityState::Unknown,
                AvailabilityReason::NoEvidence,
                DecidedBy::Discovery,
            ),
        }
    }

    /// Every target in one scope, in target order.
    ///
    /// Scoped by construction: a scope's records are a contiguous run of the map,
    /// and no other scope's record can be reached through this call.
    pub fn evaluate_scope(
        &self,
        scope: &ScopeRef,
        now: SystemTime,
    ) -> Vec<(TargetRef, Availability)> {
        self.records
            .iter()
            .filter(|(key, _)| key.scope == *scope)
            .map(|(key, record)| (key.target.clone(), Self::evaluate_record(record, now)))
            .collect()
    }

    /// Every record, in key order.
    ///
    /// Deterministic: two replicas holding the same records produce the same
    /// sequence, which is what makes an availability dump comparable across a
    /// fleet.
    pub fn evaluate_all(&self, now: SystemTime) -> Vec<(AvailabilityKey, Availability)> {
        self.records
            .iter()
            .map(|(key, record)| (key.clone(), Self::evaluate_record(record, now)))
            .collect()
    }
}

/// Builds an [`AvailabilityIndex`], preserving last-known-good evidence as
/// observations arrive.
#[derive(Debug, Clone, Default)]
pub struct AvailabilityIndexBuilder {
    records: BTreeMap<AvailabilityKey, AvailabilityRecord>,
    /// Observations that were ignored because the index already held a newer one
    /// for that key. Counted rather than dropped silently, so a projection can
    /// report that it is receiving observations out of order.
    superseded: usize,
}

impl AvailabilityIndexBuilder {
    /// Start from the records of an existing index.
    ///
    /// How a refresh keeps last-known-good evidence across an index replacement:
    /// the previous index is immutable, so a refresh reads it into a builder and
    /// publishes a new one rather than editing what readers hold.
    pub fn from_index(index: &AvailabilityIndex) -> Self {
        Self {
            records: index.records.clone(),
            superseded: 0,
        }
    }

    /// Declare the single-valued dimensions for a key, replacing any already
    /// declared and keeping the discovery evidence held for it.
    #[must_use]
    pub fn record(mut self, key: AvailabilityKey, record: AvailabilityRecord) -> Self {
        let entry = self.records.entry(key).or_default();
        let discovery = record.discovery.clone().or_else(|| entry.discovery.clone());
        let last_known_good = record
            .last_known_good
            .clone()
            .or_else(|| entry.last_known_good.clone());
        // The watermark tracks every definitive look this key has seen, so it
        // advances to cover declared evidence rather than being reset by a
        // redeclaration of the dimensions.
        let definitive_at = [
            entry.definitive_at,
            record.definitive_at,
            discovery
                .as_ref()
                .filter(|held| held.is_definitive())
                .map(|held| held.observed_at),
            last_known_good.as_ref().map(|held| held.observed_at),
        ]
        .into_iter()
        .flatten()
        .max();
        *entry = AvailabilityRecord {
            discovery,
            last_known_good,
            definitive_at,
            ..record
        };
        self
    }

    /// Record a discovery observation.
    ///
    /// Retention and the current slot advance independently, which is what makes
    /// the result independent of arrival order: the current slot keeps the newest
    /// observation, while retention is judged against
    /// [`definitive_at`](AvailabilityRecord::definitive_at) — every conclusive
    /// answer this key has ever reached, not merely the ones still held. The rest
    /// of the rules are in the module docs.
    #[must_use]
    pub fn observe(mut self, observation: DiscoveryObservation) -> Self {
        let entry = self.records.entry(observation.key()).or_default();
        // A look that predates a conclusive answer overturns nothing — neither an
        // older negative discrediting a later positive, nor an older positive
        // resurrecting a target a later complete listing dropped — while a slow
        // definitive look that lands after a newer *inconclusive* one still counts,
        // and dropping it would cost the fallback an outage needs.
        let overturns_conclusion = entry.definitive_at.is_none_or(|held| {
            if observation.is_positive() {
                // Strictly newer, so a positive and a complete negative bearing the
                // same instant resolve the same way whichever lands first: the
                // negative holds, because two answers about one instant are not
                // evidence a target is reachable.
                observation.observed_at > held
            } else {
                observation.observed_at >= held
            }
        });
        if observation.is_definitive() && overturns_conclusion {
            // A complete look that no longer carries the target is the one thing
            // that discredits retained positive evidence.
            entry.last_known_good = observation.is_positive().then(|| observation.clone());
            entry.definitive_at = Some(observation.observed_at);
        }
        // The current slot is the newest look, whatever it said, so an older
        // arrival can never become the evidence a verdict reads first — and a
        // positive that failed to overturn a conclusion cannot enter it either,
        // or it would be read as current evidence having been refused retention.
        let newest_held = entry
            .discovery
            .iter()
            .chain(entry.last_known_good.iter())
            .map(|held| held.observed_at)
            .chain(entry.definitive_at)
            .max();
        if !overturns_conclusion || newest_held.is_some_and(|held| held > observation.observed_at) {
            self.superseded += 1;
            return self;
        }
        entry.discovery = Some(observation);
        self
    }

    /// How many observations did not advance the current slot because something
    /// newer was already held. Such an arrival may still have been retained as
    /// last-known-good, if it was the newest positive evidence held.
    pub fn superseded(&self) -> usize {
        self.superseded
    }

    pub fn build(self) -> AvailabilityIndex {
        AvailabilityIndex {
            records: self.records,
        }
    }
}
