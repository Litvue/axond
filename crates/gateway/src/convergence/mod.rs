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
//! **Polling is correctness; notification is latency.** A Postgres notification
//! delivered while a replica reconnects is lost, so convergence never depends on
//! one: [`reconciler::ChangeSignal`] only shortens the wait between polls.
//!
//! **An outage degrades to staleness, not unavailability.** A replica that cannot
//! reach the control plane keeps serving its active snapshot, retries on a bounded
//! backoff, and reports its lag. A replica that *boots* during an outage may
//! restore the signed [`lkg`] cache, which is authenticated before it is
//! interpreted and re-verified through the domain's integrity checks after.
//!
//! # Not wired to `serve` yet
//!
//! Stateful boot still refuses to start, and deliberately: a revision's resource
//! *bodies* — tenancy, providers, catalogue, pricing, policy — are schemas owned
//! by later slices, so no production [`compile::RevisionProjection`] can exist
//! yet. This module is the convergence machinery and its contract, with the body
//! schemas as the one seam left open; wiring it into `serve` is the projection
//! landing, not a second convergence design.

pub mod backoff;
pub mod compile;
pub mod lkg;
pub mod reconciler;
pub mod settings;
pub mod status;

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
pub use lkg::{LastKnownGood, LastKnownGoodError};
#[allow(unused_imports)]
pub use reconciler::{BootstrapError, ChangeSignal, Outcome, Reconciler, SnapshotSink};
#[allow(unused_imports)]
pub use settings::{ConvergenceSettings, InvalidSettings};
#[allow(unused_imports)]
pub use status::{Clock, Rejection, RevisionReport, RevisionStatus, SnapshotSource, SystemClock};

#[cfg(test)]
mod tests;
