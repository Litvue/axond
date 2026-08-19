//! The multi-replica rollout and rollback harness (#220).
//!
//! Two or more real `axond` processes behind a real load balancer, replaced one
//! at a time by a newer revision and then rolled back, with buffered and
//! streamed traffic in flight throughout — and a machine-readable artifact of
//! what happened, in the order it happened.
//!
//! What it qualifies is the deployment sequence itself: the readiness-driven
//! removal a rolling update depends on, the bounded drain a termination grace
//! period is set from, the accounting a drained replica flushes on its way out,
//! and the two rollbacks — the compatible one an operator may perform, and the
//! one a forward-only migration prohibits.
//!
//! The heavy lane runs a checksum-verified retained release beside the candidate
//! build and requires a real migration matrix before its artifact is promotable.
//! The reduced lane uses one binary for both sides and labels its artifact as a
//! non-promotable diagnostic.

pub mod fleet;
pub mod ingress;
pub mod manifest;
pub mod result;
pub mod run;
mod stateful;

pub use fleet::{Fleet, Revision};
pub use ingress::Ingress;
pub use manifest::{Manifest, Scenario, Tier};
pub use result::RolloutResult;
pub use run::run;
