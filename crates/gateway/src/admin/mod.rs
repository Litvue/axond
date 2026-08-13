//! The `/admin/v1` protocol and service boundary: how durable state is changed,
//! by whom, and under what guarantees.
//!
//! A stateful deployment has two entirely separate surfaces (ADR 0027). `/v1`
//! serves inference from an immutable snapshot and never queries the control
//! plane. `/admin/v1` — this module — is the only way desired state changes, and
//! every request on it reads or writes the control plane. The separation is the
//! availability argument: a control-plane outage stalls administration and
//! convergence while replicas keep serving.
//!
//! | Module | Owns |
//! | --- | --- |
//! | [`error`] | the typed envelope and its closed vocabulary of stable codes |
//! | [`auth`] | administrative identity: OIDC humans, attributed breakglass, and the disjointness from inference credentials |
//! | [`protocol`] | the mutation preconditions: idempotency key, expected revision, dry run, audit summary |
//! | [`diff`] | the redacted semantic diff between two complete desired states |
//! | [`reads`] | bounded read projections: state, history, audit, convergence |
//! | [`service`] | the one path a mutation takes: mode, authority, read, validate, diff, publish |
//! | [`mod@router`] | the route boundary, its authentication layer, and its precondition layer |
//!
//! # The five properties this slice exists to make structural
//!
//! **Stateless mode answers without a backend.** [`AdminService::stateless`]
//! holds no [`ControlPlaneStore`], so every operation is
//! [`AdminError::StatefulModeRequired`] and there is nothing to touch — not a
//! connection, not a query, not a health check.
//!
//! **Inference credentials carry no administrative authority.** There is no
//! conversion from an inference principal to an [`AdminIdentity`], the two
//! routers layer different authentication, and a presented `axt1.` token or
//! `x-api-key` is refused with [`AdminAuthError::InferenceCredential`] rather
//! than being looked up.
//!
//! **A mutation cannot skip its preconditions.** The idempotency key and the
//! expected revision are parsed by the router's layer for any mutating route and
//! required by the service, and the expected revision is checked against the head
//! before any state is hydrated.
//!
//! **A dry run has no durable effect.** It stops after validating the complete
//! candidate and computing the diff, and never calls `publish_revision`.
//!
//! **Secrets are absent by type.** Resource bodies are never rendered: a diff and
//! a state read describe a body by form, checksum, and — for a blob — digest and
//! size. Backend text stays in [`AdminError::operator_detail`], which is logged
//! and never serialized. Credentials live in a `SecretString` inside
//! [`AdminCredential`], which has no [`Debug`] rendering of its material, and no
//! error variant can hold presented material.
//!
//! # Contract only
//!
//! Nothing here is constructed by `serve`, and [`router::admin_route_specs`] is
//! empty: the resource handlers, their bodies, and CLI parity are #143's, and the
//! durable store this composes over is #140's. What ships is the boundary they
//! land into, plus the contract tests — against the in-memory store oracle and
//! fake authorities — that hold the five properties above.
//!
//! [`AdminCredential`]: auth::AdminCredential
//! [`AdminAuthError::InferenceCredential`]: auth::AdminAuthError::InferenceCredential
//! [`AdminIdentity`]: auth::AdminIdentity
//! [`AdminError::StatefulModeRequired`]: error::AdminError::StatefulModeRequired
//! [`AdminError::operator_detail`]: error::AdminError::operator_detail
//! [`AdminService::stateless`]: service::AdminService::stateless
//! [`ControlPlaneStore`]: crate::backends::control_plane::ControlPlaneStore
//! [`Debug`]: std::fmt::Debug

pub mod auth;
pub mod diff;
pub mod error;
pub mod protocol;
pub mod reads;
pub mod router;
pub mod service;

#[cfg(test)]
mod fakes;
#[cfg(test)]
mod tests;

// The administrative facade. `allow(unused_imports)` for the same reason
// `desired_state` needs it: this is a binary crate, and a re-export that no
// handler names *yet* is still part of the contract #143 builds against.
#[allow(unused_imports)]
pub use auth::{
    AdminAction, AdminAuthError, AdminAuthenticator, AdminAuthorizer, AdminCredential, AdminGrant,
    AdminIdentity, AdminPresented, BreakglassAttribution, InvalidAttribution,
};
#[allow(unused_imports)]
pub use diff::{BlobDelta, ChangeKind, DiffSummary, ResourceDelta, SemanticDiff};
#[allow(unused_imports)]
pub use error::{AdminError, AdminErrorBody, AdminErrorEnvelope};
#[allow(unused_imports)]
pub use protocol::{ADMIN_PREFIX, AuditSummary, MutationPreconditions, MutationRequest, WriteMode};
#[allow(unused_imports)]
pub use reads::{
    AuditPage, AuditRecord, ConvergenceResult, HistoryLimit, HistoryRequest, RevisionPage,
    RevisionRecord, StateView,
};
#[allow(unused_imports)]
pub use router::{AdminApi, AdminRouteSpec, admin_route_specs, router};
#[allow(unused_imports)]
pub use service::{AdminService, DesiredStateEdit, MutationOutcome, MutationResult};
