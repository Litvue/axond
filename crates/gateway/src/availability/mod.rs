//! Derived availability: whether a scope may reach an upstream target right now,
//! evaluated from facts that are owned elsewhere (#206).
//!
//! Availability is the question a caller actually asks — *can I use this model?* —
//! and it is not a field anybody stores. It is derived, every time, from six
//! independent things: what the catalogue carries, what the scope enabled, what
//! the provider account is entitled to, what policy permits, what discovery
//! observed, and how this replica's own requests have been going. This module
//! defines what that derivation *is*, so the slices that will feed it cannot each
//! invent their own answer. The decision it implements is ADR 0038.
//!
//! # The shape of the domain
//!
//! | Module | Answers |
//! | --- | --- |
//! | [`refs`] | what a verdict is about: a tenancy scope, an upstream target, and the credential a decision was made against |
//! | [`dimensions`] | the five single-valued inputs, kept separate because they have five different authorities |
//! | [`discovery`] | the sixth input: observations, how complete they were, when they expire, and what each one establishes |
//! | [`verdict`] | the five states, the closed reason vocabulary, and the dimension that decided |
//! | [`index`] | the immutable index, the precedence ladder over it, and last-known-good retention |
//!
//! # Four properties everything else rests on
//!
//! **Absence of evidence is `unknown`, never `available` and never `denied`.** An
//! index that failed to load, a key nobody has observed, a policy that could not
//! be decided, and a listing that broke halfway all produce
//! [`AvailabilityState::Unknown`]. Defaulting such a case open would route traffic
//! at a target nobody established exists; defaulting it closed would let one
//! failed refresh deny a fleet. `unknown` is routable only where the scope
//! explicitly *chose* it: [`Availability::permits_attempt`] refuses a
//! [`DecidedBy::NoRecord`] verdict, so an index that is empty or missing a key
//! permits nothing, and every other `unknown` is one the [`index`] ladder let past
//! its enablement and policy rungs.
//!
//! **A discovery outage costs freshness, not access.** The last definitive
//! positive observation is retained across non-definitive ones, so an outage
//! degrades to `available (last_known_good)` and then to
//! [`AvailabilityState::Stale`] at expiry. Nothing here can fail a readiness probe
//! or empty a catalogue: the index is a value carried beside a snapshot, and
//! `/readyz` does not read it.
//!
//! **Uncertainty is never silently upgraded.** Only definitive evidence raises
//! certainty. A partial listing does not become a denial, an indeterminate probe
//! does not become an availability, an expired positive becomes `stale` rather
//! than staying `available`, and an expired *negative* becomes `unknown` rather
//! than remaining a denial.
//!
//! **Verdicts are per scope, and bounded by type.** Every record is keyed by
//! [`ScopeRef`], so one tenant's evidence can never decide another's — an observed
//! look is filed under the key it names, and a declared one is refused if it names
//! another — and every
//! field of [`Availability`] is an enum, a bool, or a timestamp — there is nowhere
//! to put a provider error body, a policy expression, a credential, or free text.
//! The one place free text is allowed is
//! [`DiscoveryObservation::detail`], which is for the log line and has no path
//! into a verdict.
//!
//! # What this slice deliberately is not
//!
//! Contract only, in the same sense as [`crate::backends`],
//! [`crate::convergence`], and [`crate::status`]: nothing here is constructed by
//! `serve`.
//!
//! - no provider is polled and no observation is persisted — the discovery
//!   mechanism and its storage are their own slices, and this module only says what
//!   an observation *means*;
//! - no request is enforced against a verdict, `/v1/models` is unchanged, and
//!   readiness is unchanged;
//! - an index is derived and projected onto a [`ConfigSnapshot`] as a value
//!   alongside the config ([`ConfigSnapshot::with_availability`]); it is never
//!   desired-state truth and cannot add a model, namespace, or credential to what
//!   a deployment declares;
//! - [`AvailabilityIndex::evaluate`] is a pure function over data already in hand,
//!   so wiring it into the request path later still performs no catalogue,
//!   discovery, Postgres, Redis, or `SecretStore` lookup per request.
//!
//! [`ConfigSnapshot`]: crate::state::ConfigSnapshot
//! [`ConfigSnapshot::with_availability`]: crate::state::ConfigSnapshot::with_availability

pub mod dimensions;
pub mod discovery;
pub mod index;
pub mod projection;
pub mod refs;
pub mod store;
pub mod verdict;

#[cfg(test)]
mod projection_tests;
#[cfg(test)]
mod tests;

// The availability facade. `allow(unused_imports)` for the same reason the
// desired-state and convergence facades carry it: this is a binary crate, and a
// re-export nothing in the tree names yet is still part of the contract the
// discovery, catalogue, and enforcement slices build against.
#[allow(unused_imports)]
pub use dimensions::{CataloguePresence, Enablement, Entitlement, PolicyDecision, RuntimeHealth};
#[allow(unused_imports)]
pub use discovery::{
    DiscoveryCompleteness, DiscoveryObservation, DiscoveryResult, DiscoverySource,
};
#[allow(unused_imports)]
pub use index::{AvailabilityIndex, AvailabilityIndexBuilder, AvailabilityRecord};
#[allow(unused_imports)]
pub use projection::{
    AvailabilityEvidence, AvailabilityProjection, AvailabilityProjectionError, AvailabilityReader,
    AvailabilityView, Catalogue, CatalogueListing, CredentialReadiness, ProjectedAvailability,
    RuntimeObservations,
};
#[allow(unused_imports)]
pub use refs::{
    AvailabilityKey, CredentialRef, InvalidToken, ModelRef, ProviderRef, ScopeRef, TargetRef, Token,
};
#[allow(unused_imports)]
pub use store::{
    EvidenceClear, EvidenceWrite, ObservationSlot, ObservationStore, StoredObservation,
};
#[allow(unused_imports)]
pub use verdict::{Availability, AvailabilityReason, AvailabilityState, DecidedBy};
