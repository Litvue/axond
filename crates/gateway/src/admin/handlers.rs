//! The `/admin/v1` handlers: one generic mutation handler over every resource
//! document, and the read projections.
//!
//! A handler's whole job is to turn a request into a grant, a
//! [`MutationRequest`], and an edit, then hand all three to
//! [`AdminService`](super::service::AdminService). It does not read the control
//! plane, does not publish, and cannot skip a precondition: the preconditions
//! arrive as an extension the router's layer inserted, and the service refuses a
//! mutation whose grant does not match the scope the document names.
//!
//! Authorization happens here rather than in the layer for the reason
//! [`AdminApi::authorize`] gives: the scope is in the body, and the layer cannot
//! see which tenant a document is about without parsing it.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::routing::{MethodRouter, get, post};
use serde::Deserialize;

use super::auth::{AdminAction, AdminIdentity};
use super::error::AdminError;
use super::protocol::{AuditSummary, MutationPreconditions, MutationRequest};
use super::reads::{
    AuditPage, ConvergenceResult, HistoryLimit, HistoryRequest, RevisionPage, StateView,
};
use super::resources::{AdminResourceRequest, MutationEnvelope, RollbackRequest};
use super::router::AdminApi;
use super::service::MutationOutcome;
use crate::desired_state::{MutationKind, ProjectId, ResourceScope, RevisionId, TenantId};

/// The route table's mutating rows, as method routers.
pub(super) fn publish_route<R: AdminResourceRequest>() -> MethodRouter<Arc<AdminApi>> {
    post(publish::<R>)
}

pub(super) fn rollback_route() -> MethodRouter<Arc<AdminApi>> {
    post(rollback)
}

pub(super) fn state_route() -> MethodRouter<Arc<AdminApi>> {
    get(state)
}

pub(super) fn history_route() -> MethodRouter<Arc<AdminApi>> {
    get(history)
}

pub(super) fn audit_route() -> MethodRouter<Arc<AdminApi>> {
    get(audit)
}

pub(super) fn convergence_route() -> MethodRouter<Arc<AdminApi>> {
    get(convergence)
}

/// Publish, or rehearse, one resource document.
///
/// The body is taken as bytes and deserialized here rather than through
/// `Json<T>`, so a malformed document answers in the administrative envelope
/// with [`AdminError::RequestInvalid`] instead of axum's bare `400`: a client
/// branching on `AdminError::CODES` must never meet a body it cannot parse.
async fn publish<R: AdminResourceRequest>(
    State(api): State<Arc<AdminApi>>,
    identity: AdminIdentity,
    preconditions: MutationPreconditions,
    body: axum::body::Bytes,
) -> Result<Json<MutationOutcome>, AdminError> {
    let envelope: MutationEnvelope<R> =
        serde_json::from_slice(&body).map_err(|error| AdminError::RequestInvalid {
            schema: R::SCHEMA,
            detail: error.to_string(),
        })?;
    let summary = AuditSummary::parse(&envelope.summary)?;
    let kind = envelope.mutation.kind();
    let plan = envelope.resource.plan()?;
    let grant = api.authorize(&identity, AdminAction::Publish, &plan.scope)?;
    let request = MutationRequest {
        preconditions,
        kind,
        scope: plan.scope.clone(),
        summary,
    };
    let outcome = api
        .service
        .apply(&grant, &request, plan.edit.as_ref())
        .await?;
    Ok(Json(outcome))
}

/// Republish a retained revision's complete desired state.
async fn rollback(
    State(api): State<Arc<AdminApi>>,
    identity: AdminIdentity,
    preconditions: MutationPreconditions,
    body: axum::body::Bytes,
) -> Result<Json<MutationOutcome>, AdminError> {
    let request: RollbackRequest =
        serde_json::from_slice(&body).map_err(|error| AdminError::RequestInvalid {
            schema: "rollback",
            detail: error.to_string(),
        })?;
    let summary = AuditSummary::parse(&request.summary)?;
    let target =
        RevisionId::parse(&request.revision).map_err(|error| AdminError::RequestInvalid {
            schema: "rollback",
            detail: format!("`revision`: {error}"),
        })?;
    let scope = scope_of(
        "rollback",
        request.tenant.as_deref(),
        request.project.as_deref(),
    )?;
    let grant = api.authorize(&identity, AdminAction::Rollback, &scope)?;
    let mutation = MutationRequest {
        preconditions,
        kind: MutationKind::Rollback,
        scope,
        summary,
    };
    let outcome = api.service.rollback(&grant, &mutation, target).await?;
    Ok(Json(outcome))
}

/// The complete desired state, projected: identities, scopes, checksums, and
/// dependencies, never bodies and never secret material.
async fn state(
    State(api): State<Arc<AdminApi>>,
    identity: AdminIdentity,
) -> Result<Json<StateView>, AdminError> {
    let grant = api.authorize(
        &identity,
        AdminAction::ReadState,
        &ResourceScope::Deployment,
    )?;
    Ok(Json(api.service.desired_state(&grant).await?))
}

/// What a history read may ask for.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryQuery {
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    start: Option<String>,
}

async fn history(
    State(api): State<Arc<AdminApi>>,
    identity: AdminIdentity,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<RevisionPage>, AdminError> {
    let grant = api.authorize(
        &identity,
        AdminAction::ReadHistory,
        &ResourceScope::Deployment,
    )?;
    let limit = match query.limit {
        None => HistoryLimit::default(),
        Some(limit) => HistoryLimit::parse(limit)?,
    };
    let start = match query.start.as_deref() {
        None => None,
        Some(text) => {
            Some(
                RevisionId::parse(text).map_err(|error| AdminError::RequestInvalid {
                    schema: "history",
                    detail: format!("`start`: {error}"),
                })?,
            )
        }
    };
    let page = api
        .service
        .history(&grant, HistoryRequest { limit, start })
        .await?;
    Ok(Json(page))
}

async fn audit(
    State(api): State<Arc<AdminApi>>,
    identity: AdminIdentity,
    Path(revision): Path<String>,
) -> Result<Json<AuditPage>, AdminError> {
    let grant = api.authorize(
        &identity,
        AdminAction::ReadAudit,
        &ResourceScope::Deployment,
    )?;
    let revision = RevisionId::parse(&revision).map_err(|error| AdminError::RequestInvalid {
        schema: "audit",
        detail: format!("`revision`: {error}"),
    })?;
    Ok(Json(api.service.audit(&grant, revision).await?))
}

/// What this replica has converged onto — answered from its own cached report,
/// so it still answers during a control-plane outage.
async fn convergence(
    State(api): State<Arc<AdminApi>>,
    identity: AdminIdentity,
) -> Result<Json<ConvergenceResult>, AdminError> {
    let grant = api.authorize(
        &identity,
        AdminAction::ReadConvergence,
        &ResourceScope::Deployment,
    )?;
    let report = api.convergence_report();
    Ok(Json(api.service.convergence(&grant, &report)?))
}

/// The scope a request names, from an optional tenant and project.
fn scope_of(
    schema: &'static str,
    tenant: Option<&str>,
    project: Option<&str>,
) -> Result<ResourceScope, AdminError> {
    let invalid = |field: &'static str, detail: String| AdminError::RequestInvalid {
        schema,
        detail: format!("`{field}`: {detail}"),
    };
    match (tenant, project) {
        (None, None) => Ok(ResourceScope::Deployment),
        (None, Some(_)) => Err(invalid(
            "project",
            "a project scope must name the tenant that owns it".to_owned(),
        )),
        (Some(tenant), project) => {
            let tenant =
                TenantId::parse(tenant).map_err(|error| invalid("tenant", error.to_string()))?;
            match project {
                None => Ok(ResourceScope::Tenant(tenant)),
                Some(project) => {
                    let project = ProjectId::parse(project)
                        .map_err(|error| invalid("project", error.to_string()))?;
                    Ok(ResourceScope::Project { tenant, project })
                }
            }
        }
    }
}

/// The header-derived extractors the layer inserted, read back as extensions.
///
/// A handler that forgot to declare them would not compile into the route table:
/// the mutating handlers take both, and the layer inserts both for exactly the
/// routes whose action mutates.
mod extractors {
    use super::{AdminError, AdminIdentity, MutationPreconditions};
    use axum::extract::FromRequestParts;
    use axum::http::request::Parts;

    impl<S: Send + Sync> FromRequestParts<S> for AdminIdentity {
        type Rejection = AdminError;

        async fn from_request_parts(
            parts: &mut Parts,
            _state: &S,
        ) -> Result<Self, Self::Rejection> {
            parts
                .extensions
                .get::<Self>()
                .cloned()
                // Unreachable through the router, which authenticates every
                // registered route: a handler reached without an identity is a
                // registration bug, and answering `401` is the safe reading of
                // one.
                .ok_or(AdminError::Unauthenticated(
                    crate::admin::auth::AdminAuthError::MissingCredential,
                ))
        }
    }

    impl<S: Send + Sync> FromRequestParts<S> for MutationPreconditions {
        type Rejection = AdminError;

        async fn from_request_parts(
            parts: &mut Parts,
            _state: &S,
        ) -> Result<Self, Self::Rejection> {
            if let Some(preconditions) = parts.extensions.get::<Self>() {
                return Ok(preconditions.clone());
            }
            // A mutating handler registered under a non-mutating action would
            // otherwise publish without preconditions; parsing them here means it
            // cannot.
            Self::from_headers(&parts.headers)
        }
    }
}
