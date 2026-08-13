//! The endurance harness: a long, mixed, reproducible workload offered to a
//! real `axond` process, and the evidence it leaves behind.
//!
//! Capacity answers "how much, right now"; endurance answers "and after twelve
//! hours of it". The two failures it exists to find — a resource that never
//! comes back, and an accounting row that goes missing or arrives twice — are
//! invisible to a run that ends in two minutes, and both are silent in
//! production until they are expensive.
//!
//! The same profile at two tiers: a deterministic `smoke` short enough to run
//! in the ordinary test path, and a `soak` long enough to qualify a release.
//! What differs between them is duration, concurrency, and which drift gates
//! apply; the workload, the plan, and the artifact are the same.

pub mod manifest;
pub mod plan;
pub mod result;
pub mod run;
pub mod sampler;

pub use manifest::{Ending, Manifest, Profile, Tier};
pub use result::EnduranceResult;
pub use run::{DURATION_ENV, requested_duration, run, trend};
