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
//! One path under the prefix is deliberately not this surface:
//! `GET /admin/v1/status` is the replica diagnostic, registered on the inference
//! router and authenticated with a gateway credential carrying
//! [`Capability::Status`](crate::principals::Capability::Status). It reads
//! this process's cached component states and never the control plane, which is
//! why it answers in both modes and why an inference credential is the right one
//! for it. It borrows only this module's method contract: a wrong method on it
//! answers [`AdminError::MethodNotAllowed`] rather than an empty-bodied 405.
//! Deployments that put a network boundary on the prefix have to route it with
//! the inference listener; see `docs/operations/admin-api.md`.
//!
//! | Module | Owns |
//! | --- | --- |
//! | [`error`] | the typed envelope and its closed vocabulary of stable codes |
//! | [`auth`] | administrative identity: OIDC humans, attributed breakglass, and the disjointness from inference credentials |
//! | [`protocol`] | the mutation preconditions: idempotency key, expected revision, dry run, audit summary |
//! | [`diff`] | the redacted semantic diff between two complete desired states |
//! | [`reads`] | bounded read projections: state, history, audit, convergence |
//! | [`resources`] | the typed request documents, and the edits they become |
//! | [`service`] | the one path a mutation takes: mode, authority, read, validate, diff, publish |
//! | [`handlers`] | the routes themselves: parse, plan, delegate |
//! | [`mod@router`] | the route table, its authentication layer, and its precondition layer |
//! | [`runtime`] | the authorities a running gateway builds, and the surface `serve` mounts |
//! | [`cli`] | `axond admin`: an HTTP client for these routes, with no second way in |
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
//! # One way in
//!
//! A handler holds no store. It parses a document, resolves the scope it
//! changes, and hands an edit to [`service::AdminService`], which is the only
//! code that publishes. `axond admin` speaks the same routes over HTTP rather
//! than reaching into the domain, so the CLI cannot acquire an authority or skip
//! a precondition the API enforces.
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
pub mod cli;
pub mod conditional;
pub mod diff;
pub mod error;
pub mod handlers;
pub mod protocol;
pub mod reads;
pub mod resources;
pub mod router;
pub mod runtime;
pub mod service;

#[cfg(test)]
mod api_tests;
#[cfg(test)]
pub(crate) mod fakes;
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
pub use resources::{
    AdminResourceRequest, AliasRequest, CatalogRequest, CredentialRequest, ModelRequest,
    MutationEnvelope, MutationKindInput, PolicyRequest, ProjectRequest, ProviderRequest,
    ResourcePlan, RollbackRequest, TenantRequest,
};
#[allow(unused_imports)]
pub use router::{AdminApi, AdminRouteSpec, admin_route_specs, router};
#[allow(unused_imports)]
pub use runtime::{BreakglassAuthenticator, BreakglassAuthorizer};
#[allow(unused_imports)]
pub use service::{AdminService, DesiredStateEdit, MutationOutcome, MutationResult};
