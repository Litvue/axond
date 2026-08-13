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
//! 2. **Policy.** A deployment-level refusal outranks everything a tenant has
//!    enabled or is entitled to. `denied` when refused, and `unknown` when the
//!    policy could not be decided — an undecided policy is not a permit.
//! 3. **Enablement.** The scope's own switch. `denied` when not enabled, so
//!    "unknown or stale evidence is routable only under explicit enablement" is
//!    structural: a verdict of `unknown` or `stale` from any later rung is only
//!    reachable past this one. Checked before entitlement because a target the
//!    scope never asked for should say so, rather than reporting whatever its
//!    account happens to be entitled to.
//! 4. **Entitlement.** The provider account's own answer. `denied` when missing or
//!    revoked, `unknown` when never established.
//! 5. **Runtime health.** Replica-local: an open circuit is `unavailable`. Placed
//!    above evidence uncertainty deliberately — an operator who can see requests
//!    failing right now is better served by `unavailable` than by `unknown`, and
//!    both are non-routable-by-default anyway.
//! 6. **Discovery evidence.** `available`, `stale`, `unknown`, or `denied`, per
//!    [`DiscoveryObservation::verdict`].
//! 7. Nothing objected and there is definitive positive evidence: `available`.
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
//! - an observation older than the one already held is ignored, so a late arrival
//!   from a slow probe cannot rewind the index and evaluation does not depend on
//!   the order observations arrive in;
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
        }
    }
}

impl AvailabilityRecord {
    /// A record for a catalogued, enabled, policy-permitted target with no
    /// evidence yet: the shape a projection produces before discovery has run.
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
        match record.policy {
            PolicyDecision::Denied => {
                return Availability::decided(
                    AvailabilityState::Denied,
                    AvailabilityReason::PolicyDenied,
                    DecidedBy::Policy,
                );
            }
            PolicyDecision::Indeterminate => {
                return Availability::decided(
                    AvailabilityState::Unknown,
                    AvailabilityReason::PolicyIndeterminate,
                    DecidedBy::Policy,
                );
            }
            PolicyDecision::Permitted => {}
        }
        if !record.enablement.is_enabled() {
            return Availability::decided(
                AvailabilityState::Denied,
                AvailabilityReason::NotEnabled,
                DecidedBy::Enablement,
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
            RuntimeHealth::Impaired => {
                return Availability::decided(
                    AvailabilityState::Unavailable,
                    AvailabilityReason::RuntimeImpaired,
                    DecidedBy::Runtime,
                );
            }
            RuntimeHealth::Healthy | RuntimeHealth::Unobserved => {}
        }
        match record.evidence() {
            Some((observation, last_known_good)) => observation.verdict(now, last_known_good),
            None => Availability::no_record(),
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
        *entry = AvailabilityRecord {
            discovery,
            last_known_good,
            ..record
        };
        self
    }

    /// Record a discovery observation.
    ///
    /// The last-known-good rules are in the module docs; the short version is that
    /// only definitive evidence changes what is retained, and an out-of-order
    /// arrival changes nothing at all.
    #[must_use]
    pub fn observe(mut self, observation: DiscoveryObservation) -> Self {
        let entry = self.records.entry(observation.key()).or_default();
        if entry
            .discovery
            .as_ref()
            .is_some_and(|held| held.observed_at > observation.observed_at)
        {
            self.superseded += 1;
            return self;
        }
        if observation.is_positive() {
            entry.last_known_good = Some(observation.clone());
        } else if observation.is_definitive() {
            // A complete look that no longer carries the target is the one thing
            // that discredits retained positive evidence.
            entry.last_known_good = None;
        }
        entry.discovery = Some(observation);
        self
    }

    /// How many observations were ignored as older than what was already held.
    pub fn superseded(&self) -> usize {
        self.superseded
    }

    pub fn build(self) -> AvailabilityIndex {
        AvailabilityIndex {
            records: self.records,
        }
    }
}
