//! The fault qualification harness: a committed matrix of injected provider,
//! transport, and backend faults, driven against a real `axond` process, with a
//! machine-readable artifact per row (issue #218, under the production
//! qualification programme of #156).
//!
//! It is deliberately *not* the capacity harness. Capacity qualifies a healthy
//! stateless replica and treats an error as a finding; every row here expects a
//! failure and qualifies the shape of it — the classification, the bound that
//! ended it, the retries it cost, the cleanup, the settled charge, the
//! telemetry, and that nothing about the provider or the datastore leaked.

pub mod collector;
pub mod injector;
pub mod manifest;
pub mod result;
pub mod run;

pub use manifest::{Family, Fault, Manifest, Row, Service};
pub use result::FaultResult;
pub use run::{Outcome, run};
