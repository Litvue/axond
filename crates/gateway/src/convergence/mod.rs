//! Stateful revision convergence: making replicas serve desired state without
//! restarting them (#142).
//!
//! A stateful deployment's desired state is a chain of immutable revisions in the
//! control plane (ADR 0027, [`crate::desired_state`]). This module is how a
//! running replica gets from "a new revision was published" to "requests are
//! being served from it", and — just as importantly — what happens when it
//! cannot.
//!
//! # The shape
//!
//! | Module | Answers |
//! | --- | --- |
//! | [`settings`] | how often to look, how long divergence may last, how to pace retries |
//! | [`compile`] | how a hydrated revision becomes a whole runtime snapshot, and every way that fails |
//! | [`credentials`] | how a revision's provider credentials become the pools a provider call leases from |
//! | [`policy`] | which published document governs each projected namespace |
//! | [`reconciler`] | the loop: observe, hydrate, compile, publish, report, back off |
//! | [`status`] | what the replica reports: desired, loaded, active, lag, last refusal |
//! | [`backoff`] | bounded exponential retry pacing |
//! | [`lkg`] | the signed last-known-good cache a replica may cold-boot from |
//!
//! # Four properties, and where each is enforced
//!
//! **One snapshot per request.** Convergence publishes by replacing an
//! [`ArcSwap`](arc_swap::ArcSwap)ped snapshot, and a request loads that pointer
//! once and holds the `Arc` for its lifetime (see [`crate::state`]). A buffered
//! request that started under revision *N* finishes under *N*, and a stream that
//! started under *N* relays to completion under *N*, even though *N+1* became
//! active in between. Nothing in this module can change that, because nothing in
//! it can reach into a published snapshot — publication is a whole-value store.
//!
//! **A refused candidate changes nothing.** [`compile`] builds a candidate from a
//! hydrated revision without ever being given the running snapshot, so fetch,
//! hydration, validation, compilation, and secret-resolution failures cannot
//! half-apply. The reconciler's failure path records a reason and backs off; it
//! has no rollback to perform, because there was no partial application.
//!
//! **Polling is correctness; ordinary notification is latency.** A Postgres
//! notification delivered while a replica reconnects is lost, so convergence
//! never depends on one: [`reconciler::ChangeSignal`] only shortens the wait
//! between polls for durable desired-state changes. Its explicit force-refresh
//! form also re-runs the candidate path for runtime inputs, such as secret
//! lifecycle state, that do not create a new revision.
//!
//! **An outage degrades to staleness, not unavailability.** A replica that cannot
//! reach the control plane keeps serving its active snapshot, retries on a bounded
//! backoff, and reports its lag. A replica that *boots* during an outage may
//! restore the signed [`lkg`] cache, which is authenticated before it is
//! interpreted and re-verified through the domain's integrity checks after.
//!
//! **A published policy is admitted before it is served.** A candidate carries
//! the limits [`policy`] attached to it, and the sink is asked
//! ([`reconciler::SnapshotSink::admit`]) whether this replica's backends and its
//! outstanding holds permit them *before* the snapshot is published
//! ([`crate::policy::PolicyRuntime::plan`]). A refusal is an ordinary rejection
//! with its own reason, and the replica keeps both the configuration and the
//! policy it already had.
//!
//! # Serving boundary
//!
//! `serve` owns the bootstrap snapshot and the reconciler owns every projected
//! replacement. A stateful bootstrap is intentionally keyless and therefore
//! cannot authenticate traffic; only a candidate with a complete projected
//! inbound-key set may become active. A control-plane outage leaves the active
//! immutable snapshot in place, while a cold boot without a valid projection or
//! signed last-known-good cache remains fail-closed until recovery.

pub mod backoff;
pub mod compile;
pub mod credentials;
pub mod lkg;
pub mod namespaces;
pub mod policy;
pub mod pricing;
pub mod principals;
pub mod reconciler;
pub mod secrets;
pub mod serving;
pub mod settings;
pub mod status;
pub mod tenancy;

// The convergence facade. `allow(unused_imports)` for the same reason the
// desired-state facade carries it: this is a binary crate, and a re-export that
// nothing in the tree names yet is still part of the contract the projection
// slices build against.
#[allow(unused_imports)]
pub use backoff::{Backoff, BackoffPolicy, InvalidBackoff};
#[allow(unused_imports)]
pub use compile::{
    CandidateCompiler, CompileError, ProjectionError, RevisionCompiler, RevisionProjection,
};
#[allow(unused_imports)]
pub use credentials::{CredentialProjection, RuntimeProjection};
#[allow(unused_imports)]
pub use lkg::{CachedBlobCandidate, LastKnownGood, LastKnownGoodError};
#[allow(unused_imports)]
pub use namespaces::{FlatNamespaceProjection, StateModelProjection};
#[allow(unused_imports)]
pub use policy::PolicyProjection;
#[allow(unused_imports)]
pub use pricing::{PricingSchedule, PricingScheduleError};
#[allow(unused_imports)]
pub use principals::PrincipalProjection;
#[allow(unused_imports)]
pub use reconciler::{BootstrapError, ChangeSignal, Outcome, Reconciler, SnapshotSink};
#[allow(unused_imports)]
pub use secrets::{MaterialLedger, ResolvedSecrets, RetainedMaterial, SecretMaterialization};
#[allow(unused_imports)]
pub use settings::{ConvergenceSettings, InvalidSettings};
#[allow(unused_imports)]
pub use status::{Clock, Rejection, RevisionReport, RevisionStatus, SnapshotSource, SystemClock};
#[allow(unused_imports)]
pub use tenancy::TenancyProjection;

#[cfg(test)]
mod tests;
