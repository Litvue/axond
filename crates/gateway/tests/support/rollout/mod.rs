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
//! What it does not qualify: nothing here runs a second *build*. A revision is
//! the (binary, config) pair a process was started from, and the incoming one
//! differs by a capability the outgoing one does not have, which is exactly the
//! shape of the mixed-version rule in `docs/operations/upgrades.md`. The
//! artifact records `distinct_binary: false` rather than implying otherwise.

pub mod fleet;
pub mod ingress;
pub mod manifest;
pub mod result;
pub mod run;

pub use fleet::{Fleet, Revision};
pub use ingress::Ingress;
pub use manifest::{Manifest, Scenario, Tier};
pub use result::RolloutResult;
pub use run::run;
