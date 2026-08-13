//! The deterministic capacity harness: a Rust-native driver that offers a
//! committed profile's load to a real `axond` process and a deterministic fake
//! upstream, and writes a machine-readable result artifact (ADR 0033).
//!
//! Nothing here qualifies stateful serving: the profiles run a Tier 0 process
//! with no Redis, no Postgres, and no control plane.

pub mod manifest;
pub mod probe;
pub mod result;
pub mod run;

pub use manifest::{Manifest, Profile, Tier, Workload};
pub use probe::{ResourceReport, Span};
pub use result::CapacityResult;
pub use run::{
    Gauges, Tenant, cancels, crossed_credential_uses, crossed_usage_records,
    expected_cancellations, measured_verdict, memory_verdict, offered_per_tenant,
    offered_to_healthy_backend, output_events, retuned, run, tenants, tuning, untyped_errors,
};
