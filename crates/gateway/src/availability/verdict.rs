//! The verdict: five states, a bounded reason vocabulary, and the dimension that
//! decided (#206).
//!
//! # Why five states
//!
//! Two would not do. "Available" and "not available" force three genuinely
//! different situations into one bucket — *we know you cannot* , *we do not know*,
//! and *we knew, a while ago* — and the difference decides both what a caller may
//! do and who has to act:
//!
//! | State | Means | Routable |
//! | --- | --- | --- |
//! | [`AvailabilityState::Available`] | definitive positive evidence, unexpired | yes |
//! | [`AvailabilityState::Unknown`] | no usable evidence either way | only under explicit enablement |
//! | [`AvailabilityState::Stale`] | positive evidence that has expired | only under explicit enablement |
//! | [`AvailabilityState::Denied`] | an authority said no, or discovery definitively did | no |
//! | [`AvailabilityState::Unavailable`] | it does not exist here, or this replica cannot reach it | no |
//!
//! `Denied` and `Unavailable` are both refusals and still distinct: a denial is a
//! *decision* — policy, entitlement, enablement, or a complete negative discovery —
//! and is stable until somebody changes it, while `Unavailable` is a fact about
//! this deployment or this replica (no catalogue entry; an open circuit) and can
//! change without anyone deciding anything.
//!
//! `Stale` is not a flavour of `Available`, and that is the point of having it: a
//! deployment may reasonably route to a target whose evidence expired during a
//! discovery outage, and it may equally reasonably refuse to. Reporting it as
//! `Available` takes that choice away and hides the outage.
//!
//! # The reason vocabulary is closed, and that is the redaction
//!
//! Every field of [`Availability`] is an enum, a bool, or a timestamp. There is
//! nowhere to put a provider's error body, a DSN, a credential, a policy
//! expression, or an operator's free text, so redaction is not a filter someone
//! can forget to apply — it is the absence of a field. The operator-facing detail
//! a discovery probe collects rides on
//! [`DiscoveryObservation::detail`](super::DiscoveryObservation::detail), which is
//! logged and never projected into a verdict.
//!
//! Scope is the second half. [`Availability::for_scope`] coarsens the reasons that
//! describe the deployment's own machinery — how discovery is performed, how this
//! replica is faring — to [`AvailabilityReason::Unspecified`] for a
//! namespace-scoped reader, and drops the discovery source. What survives is what
//! the tenant can act on: the state, whether it rests on last-known-good
//! evidence, and when that evidence expires.

use std::time::SystemTime;

use super::discovery::DiscoverySource;
use crate::status::StatusScope;

/// The derived availability of one target in one scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AvailabilityState {
    /// Definitive, unexpired positive evidence, and no authority refusing.
    Available,
    /// No usable evidence either way.
    Unknown,
    /// Positive evidence whose expiry has passed.
    Stale,
    /// An authority refused, or complete discovery definitively found it absent.
    Denied,
    /// It is not in this deployment's catalogue, or this replica cannot reach it.
    Unavailable,
}

impl AvailabilityState {
    pub const ALL: &'static [Self] = &[
        Self::Available,
        Self::Unknown,
        Self::Stale,
        Self::Denied,
        Self::Unavailable,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Unknown => "unknown",
            Self::Stale => "stale",
            Self::Denied => "denied",
            Self::Unavailable => "unavailable",
        }
    }

    /// Whether this state alone refuses an attempt against the target.
    ///
    /// Not the routability question on its own — use
    /// [`Availability::permits_attempt`], which is the whole verdict and knows
    /// whether any dimension was actually consulted. `Unknown` here means only
    /// "this state is not a refusal": an `Unknown` that passed the enablement and
    /// policy rungs is a scope's accepted risk, and an `Unknown` from
    /// [`Availability::no_record`] is ignorance no rung has examined.
    ///
    /// Which of the permitted states a deployment actually routes to is a routing
    /// decision, not this contract's.
    pub const fn permits_attempt(self) -> bool {
        matches!(self, Self::Available | Self::Unknown | Self::Stale)
    }

    /// Whether this state rests on evidence somebody definitively established,
    /// as opposed to the absence or expiry of evidence.
    pub const fn is_definitive(self) -> bool {
        matches!(self, Self::Available | Self::Denied | Self::Unavailable)
    }

    /// How certain this state is, ascending: `Unknown` is the least certain,
    /// then `Stale`, then the three definitive states.
    ///
    /// Exists so "never silently upgraded" is testable rather than only stated:
    /// no merge of observations may raise certainty without new definitive
    /// evidence (see [`AvailabilityIndexBuilder`](super::AvailabilityIndexBuilder)).
    pub const fn certainty(self) -> u8 {
        match self {
            Self::Unknown => 0,
            Self::Stale => 1,
            Self::Available | Self::Denied | Self::Unavailable => 2,
        }
    }
}

/// Which dimension decided the verdict.
///
/// Recorded so an operator reads *who* to go and talk to, and so the precedence
/// ladder is observable rather than inferred from a reason code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DecidedBy {
    Catalogue,
    Enablement,
    Entitlement,
    Policy,
    Discovery,
    Runtime,
    /// Nothing in the index describes this scope and target.
    NoRecord,
    /// A dimension a namespace-scoped reader is not told the name of.
    Undisclosed,
}

impl DecidedBy {
    pub const ALL: &'static [Self] = &[
        Self::Catalogue,
        Self::Enablement,
        Self::Entitlement,
        Self::Policy,
        Self::Discovery,
        Self::Runtime,
        Self::NoRecord,
        Self::Undisclosed,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Catalogue => "catalogue",
            Self::Enablement => "enablement",
            Self::Entitlement => "entitlement",
            Self::Policy => "policy",
            Self::Discovery => "discovery",
            Self::Runtime => "runtime",
            Self::NoRecord => "no_record",
            Self::Undisclosed => "undisclosed",
        }
    }

    /// Whether a namespace-scoped reader may learn that this dimension decided.
    ///
    /// Discovery and runtime describe how the deployment operates — which
    /// mechanism it uses to learn what a provider offers, and how one replica is
    /// faring — so they are reported as [`Self::Undisclosed`] rather than named.
    /// They are not reported as [`Self::NoRecord`] either: "nothing is known" and
    /// "you are not being told which dimension decided" are different statements,
    /// and conflating them would make a coarsened verdict unreadable.
    pub const fn is_tenant_visible(self) -> bool {
        matches!(
            self,
            Self::Catalogue
                | Self::Enablement
                | Self::Entitlement
                | Self::Policy
                | Self::NoRecord
                | Self::Undisclosed
        )
    }
}

/// Why a target is in the state it is.
///
/// Closed, and small on purpose: these codes are response fields and metric label
/// values as well as log content, so an open vocabulary would be an unbounded
/// dimension and — worse — an invitation to pass a provider's error text through.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AvailabilityReason {
    /// Definitive, unexpired positive evidence.
    Observed,
    /// Serving the last positive evidence held, because the current observation
    /// is not usable. The discovery-outage case.
    LastKnownGood,
    /// The catalogue does not carry the target.
    NotInCatalogue,
    /// The catalogue retired the target.
    WithdrawnFromCatalogue,
    /// The scope has not enabled the target.
    NotEnabled,
    /// The provider account is not entitled to the target.
    EntitlementMissing,
    /// The entitlement was withdrawn.
    EntitlementRevoked,
    /// No entitlement has ever been established.
    EntitlementUnknown,
    /// Policy refuses the pair.
    PolicyDenied,
    /// Policy could not be decided.
    PolicyIndeterminate,
    /// Complete discovery found the target absent.
    DiscoveryAbsent,
    /// Discovery covered only part of the provider's surface.
    DiscoveryIncomplete,
    /// The provider offers no discovery mechanism this build can use.
    DiscoveryUnsupported,
    /// Discovery answered, but the answer cannot be trusted.
    DiscoveryUnreliable,
    /// The evidence held has passed its expiry.
    EvidenceExpired,
    /// Nothing is known about this scope and target.
    NoEvidence,
    /// This replica is seeing failures against the target.
    RuntimeImpaired,
    /// This replica's circuit for the target is open.
    RuntimeUnavailable,
    /// The coarse code a namespace-scoped reader receives in place of a reason
    /// that describes the deployment's own machinery.
    Unspecified,
}

impl AvailabilityReason {
    pub const ALL: &'static [Self] = &[
        Self::Observed,
        Self::LastKnownGood,
        Self::NotInCatalogue,
        Self::WithdrawnFromCatalogue,
        Self::NotEnabled,
        Self::EntitlementMissing,
        Self::EntitlementRevoked,
        Self::EntitlementUnknown,
        Self::PolicyDenied,
        Self::PolicyIndeterminate,
        Self::DiscoveryAbsent,
        Self::DiscoveryIncomplete,
        Self::DiscoveryUnsupported,
        Self::DiscoveryUnreliable,
        Self::EvidenceExpired,
        Self::NoEvidence,
        Self::RuntimeImpaired,
        Self::RuntimeUnavailable,
        Self::Unspecified,
    ];

    pub const fn code(self) -> &'static str {
        match self {
            Self::Observed => "observed",
            Self::LastKnownGood => "last_known_good",
            Self::NotInCatalogue => "not_in_catalogue",
            Self::WithdrawnFromCatalogue => "withdrawn_from_catalogue",
            Self::NotEnabled => "not_enabled",
            Self::EntitlementMissing => "entitlement_missing",
            Self::EntitlementRevoked => "entitlement_revoked",
            Self::EntitlementUnknown => "entitlement_unknown",
            Self::PolicyDenied => "policy_denied",
            Self::PolicyIndeterminate => "policy_indeterminate",
            Self::DiscoveryAbsent => "discovery_absent",
            Self::DiscoveryIncomplete => "discovery_incomplete",
            Self::DiscoveryUnsupported => "discovery_unsupported",
            Self::DiscoveryUnreliable => "discovery_unreliable",
            Self::EvidenceExpired => "evidence_expired",
            Self::NoEvidence => "no_evidence",
            Self::RuntimeImpaired => "runtime_impaired",
            Self::RuntimeUnavailable => "runtime_unavailable",
            Self::Unspecified => "unspecified",
        }
    }

    /// Whether a namespace-scoped reader may see this code.
    ///
    /// The tenant-safe codes are the ones about the tenant's own access: what the
    /// catalogue carries, whether the tenant enabled it, whether its account is
    /// entitled, what policy said, and whether the evidence is current. The
    /// operator-only codes describe the deployment's discovery mechanism, its
    /// policy engine's own failures, and one replica's health — a tenant learns
    /// *that* the answer is unknown, which is what it can act on, and not that the
    /// provider's listing endpoint is unreliable this week.
    pub const fn is_tenant_safe(self) -> bool {
        matches!(
            self,
            Self::Observed
                | Self::LastKnownGood
                | Self::NotInCatalogue
                | Self::WithdrawnFromCatalogue
                | Self::NotEnabled
                | Self::EntitlementMissing
                | Self::EntitlementRevoked
                | Self::EntitlementUnknown
                | Self::PolicyDenied
                | Self::EvidenceExpired
                | Self::NoEvidence
                | Self::Unspecified
        )
    }

    /// This code as the given scope may see it.
    pub const fn for_scope(self, scope: StatusScope) -> Self {
        match scope {
            StatusScope::Deployment => self,
            StatusScope::Namespace if self.is_tenant_safe() => self,
            StatusScope::Namespace => Self::Unspecified,
        }
    }
}

/// One derived verdict: the state, why, who decided, and the evidence it rests
/// on.
///
/// Immutable and `Copy`: a verdict is a value computed at an instant from an
/// index, not a handle onto one, so holding it cannot observe a later change and
/// two readers cannot disagree about what a single evaluation said.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Availability {
    pub state: AvailabilityState,
    pub reason: AvailabilityReason,
    pub decided_by: DecidedBy,
    /// When the evidence behind the verdict was observed, when the deciding
    /// dimension has evidence at all.
    pub observed_at: Option<SystemTime>,
    /// When that evidence expires, or expired.
    pub expires_at: Option<SystemTime>,
    /// How the evidence was obtained. Deployment scope only.
    pub source: Option<DiscoverySource>,
    /// Whether the verdict rests on retained last-known-good evidence rather
    /// than on the current observation.
    pub last_known_good: bool,
}

impl Availability {
    /// A verdict with no evidence behind it.
    pub const fn decided(
        state: AvailabilityState,
        reason: AvailabilityReason,
        decided_by: DecidedBy,
    ) -> Self {
        Self {
            state,
            reason,
            decided_by,
            observed_at: None,
            expires_at: None,
            source: None,
            last_known_good: false,
        }
    }

    /// The verdict for a scope and target the index holds no record of.
    ///
    /// `Unknown`, never `Available` and never `Denied`: an index is a cache of
    /// derived evidence and its absence is ignorance, not permission — and not a
    /// refusal either, which would let an index that failed to load deny a whole
    /// fleet.
    pub const fn no_record() -> Self {
        Self::decided(
            AvailabilityState::Unknown,
            AvailabilityReason::NoEvidence,
            DecidedBy::NoRecord,
        )
    }

    /// The same verdict with the evidence a discovery observation carries.
    pub const fn with_evidence(
        mut self,
        observed_at: SystemTime,
        expires_at: Option<SystemTime>,
        source: DiscoverySource,
        last_known_good: bool,
    ) -> Self {
        self.observed_at = Some(observed_at);
        self.expires_at = expires_at;
        self.source = Some(source);
        self.last_known_good = last_known_good;
        self
    }

    /// Whether an attempt against the target is allowed to be made at all.
    ///
    /// Both halves matter. The state must not be a refusal, *and* some dimension
    /// must have decided: a [`DecidedBy::NoRecord`] verdict is `Unknown` because
    /// nothing was consulted — no catalogue entry was checked, no enablement, no
    /// policy — so an index that is empty, still loading, or missing a key must not
    /// be mistaken for a scope that explicitly accepted the risk of routing on
    /// uncertain evidence. Uncertainty is routable where a scope *chose* it, never
    /// by default.
    pub const fn permits_attempt(&self) -> bool {
        self.state.permits_attempt() && !matches!(self.decided_by, DecidedBy::NoRecord)
    }

    /// This verdict as the given scope may see it.
    ///
    /// Deployment scope sees it whole. Namespace scope keeps the state, the
    /// last-known-good flag, and the evidence timestamps — all of which describe
    /// the caller's own access — and loses the discovery source and any reason or
    /// deciding dimension that describes the deployment's machinery.
    pub const fn for_scope(self, scope: StatusScope) -> Self {
        match scope {
            StatusScope::Deployment => self,
            StatusScope::Namespace => Self {
                state: self.state,
                reason: self.reason.for_scope(scope),
                decided_by: if self.decided_by.is_tenant_visible() {
                    self.decided_by
                } else {
                    DecidedBy::Undisclosed
                },
                observed_at: self.observed_at,
                expires_at: self.expires_at,
                source: None,
                last_known_good: self.last_known_good,
            },
        }
    }
}
