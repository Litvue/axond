//! Discovery evidence: what a look at a provider said, how much of it was
//! covered, when it was taken, and how long it counts for (#206).
//!
//! Discovery is the one availability dimension with a *history*. The other five
//! are current values an authority holds; this one is a series of observations
//! that age, expire, and sometimes fail to happen at all — which is exactly why
//! the interesting states of the whole contract (`unknown`, `stale`, and
//! last-known-good) are born here.
//!
//! # Completeness is not the same question as the answer
//!
//! An observation carries two independent things: what it found
//! ([`DiscoveryResult`]) and how much of the provider's surface the look covered
//! ([`DiscoveryCompleteness`]). Folding them together is the mistake this module
//! exists to prevent, because *"absent"* means nothing without *"out of a complete
//! listing"*:
//!
//! | Result | Completeness | Verdict |
//! | --- | --- | --- |
//! | `Present` | `Complete` | available while unexpired, `stale` after |
//! | `Present` | anything else | `unknown` — the target may exist, but this look does not establish it |
//! | `Absent` | `Complete` | `denied`, while unexpired: the one definitive negative |
//! | `Absent` | anything else | `unknown` — a partial listing not mentioning a model is not a denial |
//! | `Indeterminate` | anything | `unknown` |
//!
//! A paginated listing that failed halfway, a provider with no listing endpoint,
//! and a provider whose listing is known to omit deployments are all *partial
//! knowledge*, and a partial listing that does not mention a model is the single
//! most tempting way to deny a tenant access to a model it is paying for. So only
//! a complete negative denies, and everything else is unknown.
//!
//! # Expiry cuts both ways, and never towards more confidence
//!
//! Evidence has an [`expires_at`](DiscoveryObservation::expires_at). Past it:
//!
//! - expired *positive* evidence becomes `stale` — we did know, and a deployment
//!   may still choose to route on it;
//! - expired *negative* evidence becomes `unknown`, not a continuing denial. A
//!   denial rests on a complete look, and an expired look is no longer one;
//!   continuing to deny would let one bad listing, taken once, outlive every
//!   attempt to refresh it.
//!
//! An observation with no expiry never expires, which is the right default for
//! evidence an operator asserted rather than a provider reported.
//!
//! # `detail` is the only free text, and it never leaves
//!
//! A probe learns useful, unsafe things: an HTTP body, a provider error naming an
//! account, occasionally a URL with a query string in it.
//! [`DiscoveryObservation::detail`] is where that goes, for the log line, and
//! [`Availability`] has no field it fits in — so no
//! projection can leak it, because there is nowhere to project it to.

use std::time::SystemTime;

use super::refs::{AvailabilityKey, ScopeRef, TargetRef};
use super::verdict::{Availability, AvailabilityReason, AvailabilityState, DecidedBy};

/// How an observation was obtained.
///
/// Deployment-scoped detail: it names the deployment's own mechanism, so
/// [`Availability::for_scope`] drops it for a namespace-scoped reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DiscoverySource {
    /// The provider's own model listing.
    ProviderListing,
    /// A direct probe of one target.
    ProviderProbe,
    /// A catalogue record published into desired state.
    CatalogueRecord,
    /// An operator's explicit assertion.
    OperatorAssertion,
}

impl DiscoverySource {
    pub const ALL: &'static [Self] = &[
        Self::ProviderListing,
        Self::ProviderProbe,
        Self::CatalogueRecord,
        Self::OperatorAssertion,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProviderListing => "provider_listing",
            Self::ProviderProbe => "provider_probe",
            Self::CatalogueRecord => "catalogue_record",
            Self::OperatorAssertion => "operator_assertion",
        }
    }
}

/// How much of the provider's surface an observation covered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DiscoveryCompleteness {
    /// The whole surface was enumerated, so an absence is meaningful.
    Complete,
    /// Only part of it: a truncated page, a filtered listing, a timeout partway.
    Partial,
    /// The provider offers no mechanism this build can enumerate it with.
    Unsupported,
    /// It answered, but the answer is not trustworthy — a known-lossy listing, a
    /// response that failed a sanity check, a proxy that may have served a cached
    /// body.
    Unreliable,
}

impl DiscoveryCompleteness {
    pub const ALL: &'static [Self] = &[
        Self::Complete,
        Self::Partial,
        Self::Unsupported,
        Self::Unreliable,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Partial => "partial",
            Self::Unsupported => "unsupported",
            Self::Unreliable => "unreliable",
        }
    }

    pub const fn is_complete(self) -> bool {
        matches!(self, Self::Complete)
    }

    /// The reason code an incomplete look reports as.
    const fn reason(self) -> AvailabilityReason {
        match self {
            // Not reachable through `DiscoveryObservation::verdict`, which only
            // asks an incomplete look for its reason; kept total rather than
            // panicking, and `Observed` is what a complete look means.
            Self::Complete => AvailabilityReason::Observed,
            Self::Partial => AvailabilityReason::DiscoveryIncomplete,
            Self::Unsupported => AvailabilityReason::DiscoveryUnsupported,
            Self::Unreliable => AvailabilityReason::DiscoveryUnreliable,
        }
    }
}

/// What an observation found.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DiscoveryResult {
    /// The target was found.
    Present,
    /// The target was not found in what was looked at.
    Absent,
    /// The look did not answer: a transport failure, a refused request, a body
    /// that could not be read.
    Indeterminate,
}

impl DiscoveryResult {
    pub const ALL: &'static [Self] = &[Self::Present, Self::Absent, Self::Indeterminate];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Present => "present",
            Self::Absent => "absent",
            Self::Indeterminate => "indeterminate",
        }
    }
}

/// One look at one target in one scope.
///
/// Scoped, because a listing taken with one tenant's credentials describes that
/// tenant's account and no other: a model absent from tenant A's listing may be
/// present in tenant B's, and an index that shared the observation between them
/// would deny B on A's evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveryObservation {
    pub scope: ScopeRef,
    pub target: TargetRef,
    pub result: DiscoveryResult,
    pub completeness: DiscoveryCompleteness,
    pub source: DiscoverySource,
    pub observed_at: SystemTime,
    /// When this evidence stops counting. `None` never expires.
    pub expires_at: Option<SystemTime>,
    /// Operator-facing detail, for the log line only. Never projected into a
    /// verdict — see the module docs.
    pub detail: Option<String>,
}

impl DiscoveryObservation {
    /// An observation with no expiry and no detail.
    pub fn new(
        scope: ScopeRef,
        target: TargetRef,
        result: DiscoveryResult,
        completeness: DiscoveryCompleteness,
        source: DiscoverySource,
        observed_at: SystemTime,
    ) -> Self {
        Self {
            scope,
            target,
            result,
            completeness,
            source,
            observed_at,
            expires_at: None,
            detail: None,
        }
    }

    /// The same observation, expiring at `expires_at`.
    #[must_use]
    pub fn expiring_at(mut self, expires_at: SystemTime) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// The same observation, carrying operator-facing detail for the log.
    #[must_use]
    pub fn detailed(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    /// The key this observation is filed under.
    pub fn key(&self) -> AvailabilityKey {
        AvailabilityKey::new(self.scope, self.target.clone())
    }

    /// Whether this look establishes something definitively: it answered, and it
    /// covered enough of the surface for the answer to mean what it says.
    pub const fn is_definitive(&self) -> bool {
        self.completeness.is_complete() && !matches!(self.result, DiscoveryResult::Indeterminate)
    }

    /// Whether this look is definitive *positive* evidence — the only kind that
    /// may become a last-known-good state.
    pub const fn is_positive(&self) -> bool {
        self.completeness.is_complete() && matches!(self.result, DiscoveryResult::Present)
    }

    /// Whether the evidence has passed its expiry at `now`.
    ///
    /// Compared with [`SystemTime`] ordering rather than by subtracting, so a
    /// clock that moved backwards produces "not expired" instead of a panic on a
    /// negative duration.
    pub fn is_expired(&self, now: SystemTime) -> bool {
        self.expires_at.is_some_and(|expires_at| now >= expires_at)
    }

    /// This observation's own verdict at `now`, before any other dimension is
    /// considered.
    ///
    /// `last_known_good` marks a verdict derived from retained evidence rather
    /// than from the current look; the caller knows which it is holding, so it is
    /// passed in rather than guessed.
    pub fn verdict(&self, now: SystemTime, last_known_good: bool) -> Availability {
        let expired = self.is_expired(now);
        let (state, reason) = match (self.result, self.completeness.is_complete(), expired) {
            // Partial, unsupported, or untrustworthy coverage: unknown whatever it
            // found, and the reason says which, so an operator can tell "we cannot
            // enumerate this provider" from "this listing broke today".
            (_, false, _) => (AvailabilityState::Unknown, self.completeness.reason()),
            // Definitive positive evidence, still counting.
            (DiscoveryResult::Present, true, false) => (
                AvailabilityState::Available,
                if last_known_good {
                    AvailabilityReason::LastKnownGood
                } else {
                    AvailabilityReason::Observed
                },
            ),
            // We did know. A deployment may still route on it, and it is told
            // that it is doing so.
            (DiscoveryResult::Present, true, true) => (
                AvailabilityState::Stale,
                AvailabilityReason::EvidenceExpired,
            ),
            // The one definitive negative.
            (DiscoveryResult::Absent, true, false) => (
                AvailabilityState::Denied,
                AvailabilityReason::DiscoveryAbsent,
            ),
            // An expired complete negative stops being a denial: the look it
            // rested on is no longer current, and a stale denial is worse than an
            // honest unknown.
            (DiscoveryResult::Absent, true, true) => (
                AvailabilityState::Unknown,
                AvailabilityReason::EvidenceExpired,
            ),
            // A look that covered the surface and still could not answer: not
            // evidence of anything, and reported as an untrustworthy answer rather
            // than as incomplete coverage, which it was not.
            (DiscoveryResult::Indeterminate, true, _) => (
                AvailabilityState::Unknown,
                AvailabilityReason::DiscoveryUnreliable,
            ),
        };
        Availability::decided(state, reason, DecidedBy::Discovery).with_evidence(
            self.observed_at,
            self.expires_at,
            self.source,
            last_known_good,
        )
    }
}
