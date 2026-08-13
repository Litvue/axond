//! Secret material never leaves the store, and the lifecycle around it is safe.
//!
//! The contracts under [`crate::backends::secrets`] and
//! [`crate::desired_state::credentials`] each prove a *local* property: a
//! `Debug` impl redacts, a body carries a reference rather than a value, an
//! error names a version. This module family proves the property those local
//! ones exist to add up to, and proves it over the composition rather than over
//! the pieces: material an administrator stages is resolved into a snapshot,
//! served to a provider, recorded, logged, traced and journalled — and appears
//! in none of it.
//!
//! That is a different kind of test, and it is organised accordingly:
//!
//! * [`sweep`] is the detector. It searches for a sentinel in every encoding it
//!   could survive in — base64, hex, case-folded, and any twelve-character
//!   fragment — because a redaction suite is only as strong as its notion of
//!   "contains", and it can assert that material *is* present, so no test here
//!   passes because its sentinel never entered the system.
//! * [`harness`] holds the sentinels, a fake provider that records what it was
//!   authenticated with, and the compiler that resolves desired-state
//!   credentials through a `SecretStore`.
//! * [`lifecycle`] is the runtime: one-time disclosure, rotation overlapping
//!   in-flight requests, a failed resolution keeping the last known good, and
//!   retirement destroying material once nothing references it.
//! * [`request_path`] sweeps everything one served request emits — response,
//!   logs, spans, usage records, status.
//! * [`journal`] sweeps everything a published revision durably leaves behind,
//!   against a real PostgreSQL journal.
//! * [`stateful`] drives the zero-redeploy sequence — stage, activate, serve,
//!   rotate, roll back, revoke — against the *production* secret store, so the
//!   lifecycle is asserted over envelope-encrypted rows and owner-checked reads
//!   rather than over a fake.
//!
//! # Why the durable half is required rather than optional
//!
//! [`journal`] runs against PostgreSQL and is skipped when no DSN is
//! configured, which locally is a convenience and in CI would be a hole: a
//! suite that silently skips is a suite that reports green while the property it
//! guards is unasserted. CI therefore sets `AXOND_TEST_REQUIRE_SERVICES=1`, under
//! which [`crate::test_services`] panics instead of returning `None` — the
//! stateful lane cannot pass by not running these tests.
//! `docs/security/secret-material.md` records the evidence and the surfaces
//! covered.

pub(crate) mod harness;
pub(crate) mod sweep;

mod journal;
mod lifecycle;
mod request_path;
mod stateful;
