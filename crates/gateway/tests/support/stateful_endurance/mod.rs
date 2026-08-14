//! The stateful endurance harness: the mixed workload of the endurance slice,
//! offered to a *fleet* whose catalogue, credentials, policy, provider, usage
//! database and processes all change while it runs.
//!
//! The stateless soak answers "does one replica stay healthy under twelve hours
//! of mixed traffic". It cannot answer the questions a deployment actually
//! fails on: whether a revision published under load becomes the one serving,
//! whether accounting survives the database going away and coming back, whether
//! a tenant is still isolated after the policy that isolates it has been
//! rewritten twice, and whether replacing every replica costs a caller
//! anything. Those need durable state, more than one process, and faults that
//! happen to the backends rather than to the requests.
//!
//! Two tiers of the same qualification, as the other harnesses have: a
//! deterministic `smoke` short enough for the ordinary test path, and a `soak`
//! long enough to qualify a release. The script is written as fractions of the
//! offered duration, so the short tier is the same run compressed rather than a
//! different one.

pub mod durable;
pub mod fleet;
pub mod gate;
pub mod manifest;
pub mod result;
pub mod run;

pub use manifest::{DURATION_ENV, Manifest, Profile, Stop, Tier, load};
pub use result::StatefulEnduranceResult;
pub use run::{Dispatch, run, run_with};
