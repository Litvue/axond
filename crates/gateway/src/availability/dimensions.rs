//! The dimensions availability is *derived from*, kept separate on purpose
//! (#206).
//!
//! Five independent questions decide whether a tenant may reach an upstream
//! target, and each has its own authority, its own lifetime, and its own repair:
//!
//! | Dimension | Authority | Repair |
//! | --- | --- | --- |
//! | [`CataloguePresence`] | the catalogue (#192/#146) | publish or restore the entry |
//! | [`Enablement`] | the tenant's own enablement (#205/#149) | enable it |
//! | [`Entitlement`] | the provider account behind a credential (#198/#145) | grant or rotate |
//! | [`PolicyDecision`] | the deployment's policy | change the policy |
//! | [`RuntimeHealth`] | this replica's own request outcomes | wait, or fix the upstream |
//!
//! Collapsing them into one tri-state would be smaller and wrong. "The operator
//! has not enabled this model", "your provider account is not entitled to it",
//! and "this replica's circuit for it is open" call for three different actions by
//! three different people, and only the third one heals by itself. They are also
//! *not* commutative with respect to durability: a runtime observation is
//! replica-local evidence with a lifetime of minutes, and a catalogue fact is
//! durable and fleet-wide. So the dimensions stay separate all the way to the
//! evaluator, which combines them under one documented precedence
//! ([`AvailabilityIndex::evaluate`]) and records *which* dimension decided.
//!
//! [`AvailabilityIndex::evaluate`]: super::AvailabilityIndex::evaluate
//!
//! Discovery is the sixth dimension and lives in [`discovery`](super::discovery),
//! because it is the only one with observations, expiry, and a last-known-good
//! history rather than a single current value.

/// Whether the catalogue carries the target at all.
///
/// A durable, fleet-wide fact: what the deployment's catalogue says exists. Not
/// an entitlement — a present entry says the deployment knows the model, not that
/// anyone may call it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CataloguePresence {
    /// The catalogue carries the target.
    Present,
    /// The catalogue carried it and it was retired. Distinguished from
    /// [`Self::Absent`] because an operator's next step differs: a withdrawn
    /// entry was a deliberate decision, and a missing one may be an unpublished
    /// revision.
    Withdrawn,
    /// The catalogue does not carry the target.
    Absent,
}

impl CataloguePresence {
    pub const ALL: &'static [Self] = &[Self::Present, Self::Withdrawn, Self::Absent];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Present => "present",
            Self::Withdrawn => "withdrawn",
            Self::Absent => "absent",
        }
    }
}

/// Whether the scope has been enabled for the target.
///
/// The tenant-facing switch (#205/#149), and the reason unknown or stale evidence
/// can be routable at all: a scope that has explicitly enabled a target has said
/// it accepts the risk of calling one whose discovery evidence is not definitive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Enablement {
    /// Explicitly enabled for this scope.
    Enabled,
    /// Not enabled. The default: enablement is opt-in, never inferred from a
    /// catalogue entry existing.
    NotEnabled,
}

impl Enablement {
    pub const ALL: &'static [Self] = &[Self::Enabled, Self::NotEnabled];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::NotEnabled => "not_enabled",
        }
    }

    pub const fn is_enabled(self) -> bool {
        matches!(self, Self::Enabled)
    }
}

/// Whether the provider account behind the scope's credential may call the
/// target.
///
/// Decided upstream of the gateway and only *observed* here.
/// [`Self::Unknown`] is a first-class value rather than an optimistic default: a
/// credential that has never been exercised against a target says nothing about
/// entitlement, and treating silence as a grant is how a fleet learns about a
/// missing entitlement from a customer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Entitlement {
    /// Observed granted.
    Granted,
    /// Observed withdrawn after having been granted.
    Revoked,
    /// Observed absent: the account does not have it.
    Missing,
    /// Never established.
    Unknown,
}

impl Entitlement {
    pub const ALL: &'static [Self] = &[Self::Granted, Self::Revoked, Self::Missing, Self::Unknown];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Granted => "granted",
            Self::Revoked => "revoked",
            Self::Missing => "missing",
            Self::Unknown => "unknown",
        }
    }
}

/// What the deployment's policy says about the pair.
///
/// [`Self::Indeterminate`] is what a policy engine that could not decide reports —
/// a rule referencing a fact this replica does not hold, for instance. It is
/// deliberately *not* a permit: an undecided policy leaves availability unknown,
/// and unknown is routable only under explicit enablement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PolicyDecision {
    /// Policy permits the pair.
    Permitted,
    /// Policy refuses the pair.
    Denied,
    /// Policy could not be decided.
    Indeterminate,
}

impl PolicyDecision {
    pub const ALL: &'static [Self] = &[Self::Permitted, Self::Denied, Self::Indeterminate];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Permitted => "permitted",
            Self::Denied => "denied",
            Self::Indeterminate => "indeterminate",
        }
    }
}

/// What this replica's own request outcomes last said about the target.
///
/// Replica-local by construction, exactly like the per-target circuit breaker it
/// is derived from (ADR 0008, [`CircuitBreaker`](gateway_core::CircuitBreaker)),
/// and it stays that way: runtime evidence contributes to a *verdict* and never
/// to a durable fact. Nothing here is written back to a catalogue, a discovery
/// observation, or a revision, so a single replica's bad afternoon cannot retire
/// a model for the fleet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RuntimeHealth {
    /// Recent requests succeeded.
    Healthy,
    /// Failing intermittently, but not tripped.
    Impaired,
    /// The circuit is open: this replica is skipping the target.
    Unavailable,
    /// No request has been made from this replica. The default, and not a
    /// judgement either way.
    Unobserved,
}

impl RuntimeHealth {
    pub const ALL: &'static [Self] = &[
        Self::Healthy,
        Self::Impaired,
        Self::Unavailable,
        Self::Unobserved,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Impaired => "impaired",
            Self::Unavailable => "unavailable",
            Self::Unobserved => "unobserved",
        }
    }
}
