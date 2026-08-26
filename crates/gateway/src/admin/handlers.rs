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
use std::time::SystemTime;

use axum::Json;
use axum::body::Bytes;
use axum::extract::rejection::{BytesRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::routing::{MethodRouter, get, post};
use serde::Deserialize;

use super::auth::{AdminAction, AdminIdentity};
use super::catalogue::{
    CatalogueFilters, CatalogueRefreshView, CatalogueRequest, CatalogueSource, CatalogueView,
    IMPORTED_QUERY_MIN_CHARS,
};
use super::conditional::Conditional;
use super::error::AdminError;
use super::protocol::{AuditSummary, MutationPreconditions, MutationRequest};
use super::reads::{
    AuditPage, AvailabilityResult, ConvergenceResult, HistoryLimit, HistoryRequest, RevisionPage,
    StateView,
};
use super::resources::{AdminResourceRequest, MutationEnvelope, RollbackRequest, uuid_detail};
use super::router::{ADMIN_MAX_REQUEST_BYTES, AdminApi};
use super::secrets::{
    self, RotateSecretRequest, SecretLifecycleRequest, SecretTransitionView, SecretVersionView,
    SecretVersionsView, StageSecretRequest,
};
use super::service::{AvailabilityAuthority, MutationOutcome};
use crate::desired_state::{
    InvalidId, ModelLifecycle, MutationKind, OfferingId, ProjectId, ResourceScope, RevisionId,
    Surface, TenantId, WireFamily,
};

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

pub(super) fn catalogue_route() -> MethodRouter<Arc<AdminApi>> {
    get(catalogue)
}

pub(super) fn catalogue_refresh_route() -> MethodRouter<Arc<AdminApi>> {
    post(refresh_catalogue)
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

pub(super) fn availability_route() -> MethodRouter<Arc<AdminApi>> {
    get(availability)
}

pub(super) fn stage_secret_route() -> MethodRouter<Arc<AdminApi>> {
    post(stage_secret)
}

pub(super) fn rotate_secret_route() -> MethodRouter<Arc<AdminApi>> {
    post(rotate_secret)
}

pub(super) fn secret_lifecycle_route() -> MethodRouter<Arc<AdminApi>> {
    post(secret_lifecycle)
}

pub(super) fn secret_versions_route() -> MethodRouter<Arc<AdminApi>> {
    get(secret_versions)
}

/// The buffered request body, or the administrative refusal for one that never
/// arrived whole.
///
/// The body is taken as `Result` rather than as [`Bytes`] so that the router's
/// declared limit answers in this surface's envelope: a client branching on
/// [`AdminError::CODES`] would otherwise meet axum's bare `413` on the one
/// response it cannot afford to misread as success.
fn document(
    schema: &'static str,
    body: Result<Bytes, BytesRejection>,
) -> Result<Bytes, AdminError> {
    body.map_err(|rejection| {
        if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
            AdminError::RequestTooLarge {
                limit: ADMIN_MAX_REQUEST_BYTES,
            }
        } else {
            AdminError::RequestInvalid {
                schema,
                detail: rejection.body_text(),
            }
        }
    })
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
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<MutationOutcome>, AdminError> {
    let body = document(R::SCHEMA, body)?;
    let envelope: MutationEnvelope<R> =
        serde_json::from_slice(&body).map_err(|error| AdminError::RequestInvalid {
            schema: R::SCHEMA,
            detail: error.to_string(),
        })?;
    let summary = AuditSummary::parse(&envelope.summary)?;
    let kind = envelope.mutation.kind();
    let plan = envelope.resource.plan()?;
    // Nothing on this surface removes a resource: desired state supersedes
    // versions and retains what history resolves against, so the only deletion
    // there is is a resource's own terminal lifecycle state. A document that
    // leaves the resource in service may not be *recorded* as a deletion — an
    // auditor filtering the trail for `delete` is asking what stopped serving,
    // and a rename wearing that label answers wrongly.
    if kind == MutationKind::Delete && !plan.retires {
        return Err(AdminError::RequestInvalid {
            schema: R::SCHEMA,
            detail: "`mutation: \"delete\"` requires a document that retires the resource: this \
                     surface removes nothing, so state the terminal lifecycle the resource \
                     supports (a tenant `deleted`, a credential `revoked`, an enablement or alias \
                     `disabled`) — or record the change as an update"
                .to_owned(),
        });
    }
    let grant = api
        .authorize(&identity, AdminAction::Publish, R::SURFACE, &plan.scope)
        .await?;
    let request = MutationRequest {
        preconditions,
        kind,
        surface: R::SURFACE,
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
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<MutationOutcome>, AdminError> {
    let body = document("rollback", body)?;
    let request: RollbackRequest =
        serde_json::from_slice(&body).map_err(|error| AdminError::RequestInvalid {
            schema: "rollback",
            detail: error.to_string(),
        })?;
    let summary = AuditSummary::parse(&request.summary)?;
    let target =
        RevisionId::parse(&request.revision).map_err(|error| AdminError::RequestInvalid {
            schema: "rollback",
            detail: format!("`revision`: {}", id_detail(RevisionId::PREFIX, &error)),
        })?;
    let scope = scope_of(
        "rollback",
        request.tenant.as_deref(),
        request.project.as_deref(),
    )?;
    let grant = api
        .authorize(
            &identity,
            AdminAction::Rollback,
            Surface::AuditTrail,
            &scope,
        )
        .await?;
    let mutation = MutationRequest {
        preconditions,
        kind: MutationKind::Rollback,
        surface: Surface::AuditTrail,
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
    headers: HeaderMap,
) -> Result<Conditional<StateView>, AdminError> {
    let grant = api
        .authorize(
            &identity,
            AdminAction::ReadState,
            Surface::AuditTrail,
            &ResourceScope::Deployment,
        )
        .await?;
    Ok(Conditional::new(
        &headers,
        api.service.desired_state(&grant).await?,
    ))
}

/// What a catalogue read may ask for: the scope, and filters over it.
///
/// `tenant` is required, so there is no spelling of this query that asks for
/// every tenant's enablements. Unknown keys are refused rather than ignored.
/// Every accepted filter is parsed into the corresponding catalogue or
/// availability identity before the read is authorized, so a caller that asked
/// to narrow the answer cannot accidentally receive an unfiltered projection.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogueQuery {
    tenant: String,
    #[serde(default)]
    project: Option<String>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    wire_family: Option<String>,
    #[serde(default)]
    offering: Option<String>,
    #[serde(default)]
    billable: Option<bool>,
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    capability: Option<String>,
    #[serde(default)]
    modality: Option<String>,
    #[serde(default)]
    lifecycle: Option<String>,
    #[serde(default)]
    availability: Option<String>,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    q: Option<String>,
}

/// Catalogue queries are deliberately small and finite: the route has one
/// required scope key and twelve optional filters. Bounding the raw query before
/// deserialization keeps malformed input from turning into an unbounded error
/// detail and prevents a client from spending parser work on an unbounded
/// number of repeated keys.
pub(super) const CATALOGUE_MAX_QUERY_BYTES: usize = 2 * 1024;
pub(super) const CATALOGUE_MAX_QUERY_PARAMS: usize = 13;

fn validate_catalogue_query(uri: &Uri) -> Result<(), AdminError> {
    let query = uri.query().unwrap_or_default();
    if query.len() > CATALOGUE_MAX_QUERY_BYTES {
        return Err(AdminError::RequestInvalid {
            schema: "catalogue",
            detail: format!("query exceeds the {CATALOGUE_MAX_QUERY_BYTES}-byte limit"),
        });
    }

    let parameters = query
        .split('&')
        .filter(|parameter| !parameter.is_empty())
        .count();
    if parameters > CATALOGUE_MAX_QUERY_PARAMS {
        return Err(AdminError::RequestInvalid {
            schema: "catalogue",
            detail: format!("query has more than {CATALOGUE_MAX_QUERY_PARAMS} parameters"),
        });
    }

    Ok(())
}

/// One tenant's management catalogue: what it has enabled, what names route to
/// it, and why a model is not routable.
///
/// A scoped read, unlike every other read on this surface: the scope comes from
/// the query and the grant has to cover it, so a tenant-scoped administrator gets
/// its own tenant and nothing else — including no evidence that another tenant's
/// enablements exist.
async fn catalogue(
    State(api): State<Arc<AdminApi>>,
    identity: AdminIdentity,
    headers: HeaderMap,
    uri: Uri,
    query: Result<Query<CatalogueQuery>, QueryRejection>,
) -> Result<Conditional<CatalogueView>, AdminError> {
    const SCHEMA: &str = "catalogue";
    validate_catalogue_query(&uri)?;
    let Query(query) = query.map_err(|rejection| AdminError::RequestInvalid {
        schema: SCHEMA,
        detail: rejection.body_text(),
    })?;
    let invalid = |field: &'static str, detail: String| AdminError::RequestInvalid {
        schema: SCHEMA,
        detail: format!("`{field}`: {detail}"),
    };
    let tenant =
        TenantId::parse(&query.tenant).map_err(|error| invalid("tenant", error.to_string()))?;
    let project = match query.project.as_deref() {
        None => None,
        Some(project) => {
            Some(ProjectId::parse(project).map_err(|error| invalid("project", error.to_string()))?)
        }
    };
    // Parsed, not matched loosely: text no release wrote is a client error rather
    // than an unfiltered listing.
    let state =
        match query.state.as_deref() {
            None => None,
            Some(text) => Some(ModelLifecycle::parse(text).ok_or_else(|| {
                invalid("state", format!("`{text}` is not a model lifecycle state"))
            })?),
        };
    let wire_family = match query.wire_family.as_deref() {
        None => None,
        Some(text) => Some(
            WireFamily::parse(text)
                .ok_or_else(|| invalid("wire_family", format!("`{text}` is not a wire family")))?,
        ),
    };
    let offering = match query.offering.as_deref() {
        None => None,
        Some(text) => {
            Some(OfferingId::parse(text).map_err(|error| invalid("offering", error.to_string()))?)
        }
    };
    let provider = query
        .provider
        .as_deref()
        .map(str::trim)
        .filter(|provider| !provider.is_empty())
        .map(str::to_owned);
    let capability = match query.capability.as_deref() {
        None => None,
        Some(text) => Some(
            crate::backends::catalog::ModelCapability::parse(text).ok_or_else(|| {
                invalid(
                    "capability",
                    format!("`{text}` is not a catalogue capability"),
                )
            })?,
        ),
    };
    let modality =
        match query.modality.as_deref() {
            None => None,
            Some(text) => Some(crate::backends::catalog::Modality::parse(text).ok_or_else(
                || invalid("modality", format!("`{text}` is not a catalogue modality")),
            )?),
        };
    let catalog_lifecycle = match query.lifecycle.as_deref() {
        None => None,
        Some(text) => Some(
            crate::backends::catalog::ModelLifecycle::parse(text).ok_or_else(|| {
                invalid(
                    "lifecycle",
                    format!("`{text}` is not a catalogue lifecycle"),
                )
            })?,
        ),
    };
    let availability = match query.availability.as_deref() {
        None => None,
        Some(text) => Some(
            crate::availability::AvailabilityState::parse(text).ok_or_else(|| {
                invalid(
                    "availability",
                    format!("`{text}` is not an availability state"),
                )
            })?,
        ),
    };
    let source = match query.source.as_deref() {
        None => CatalogueSource::Enabled,
        Some(text) => CatalogueSource::parse(text)
            .ok_or_else(|| invalid("source", format!("`{text}` is not a catalogue source")))?,
    };
    let q = match query.q.as_deref().map(str::trim).filter(|q| !q.is_empty()) {
        None => None,
        Some(text) => {
            if text.chars().count() < IMPORTED_QUERY_MIN_CHARS {
                return Err(invalid(
                    "q",
                    format!("must be at least {IMPORTED_QUERY_MIN_CHARS} characters"),
                ));
            }
            Some(text.to_owned())
        }
    };
    if source == CatalogueSource::Imported && provider.is_none() && q.is_none() {
        return Err(AdminError::RequestInvalid {
            schema: SCHEMA,
            detail: "`source=imported` requires `provider` and/or `q`".to_owned(),
        });
    }
    let request = CatalogueRequest {
        tenant,
        project,
        source,
        filters: CatalogueFilters {
            state,
            wire_family,
            offering,
            billable: query.billable,
            provider,
            capability,
            modality,
            catalog_lifecycle,
            availability,
            q,
        },
    };
    let grant = api
        .authorize(
            &identity,
            AdminAction::ReadState,
            // The surface a denial is recorded against is what was asked for, and
            // this read asks about enablements: an auditor filtering the trail for
            // refused model reads must find it there.
            Surface::Model,
            &request.scope(),
        )
        .await?;
    Ok(Conditional::new(
        &headers,
        api.service
            .model_catalogue_with_context(
                &grant,
                &request,
                api.catalogue.as_deref(),
                api.availability.as_deref(),
                SystemTime::now(),
            )
            .await?,
    ))
}

/// Import now. Same timeout and backoff as the scheduled loop; a refusal leaves
/// last-known-good active and does not publish a revision.
async fn refresh_catalogue(
    State(api): State<Arc<AdminApi>>,
    identity: AdminIdentity,
) -> Result<Json<CatalogueRefreshView>, AdminError> {
    let grant = api
        .authorize(
            &identity,
            AdminAction::RefreshCatalog,
            Surface::Model,
            &ResourceScope::Deployment,
        )
        .await?;
    let Some(handle) = api.catalog_handle.as_ref() else {
        return Err(AdminError::RequestInvalid {
            schema: "catalogue_refresh",
            detail: "catalogue imports are not enabled".to_owned(),
        });
    };
    Ok(Json(api.service.catalogue_refresh(&grant, handle).await?))
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

/// A query string the extractor cannot read is refused in the administrative
/// envelope, for the reason [`publish`] takes raw bytes: axum's own rejection is
/// plain text with no `error.type`, and a client branching on
/// [`AdminError::CODES`] would meet a body it cannot parse.
async fn history(
    State(api): State<Arc<AdminApi>>,
    identity: AdminIdentity,
    headers: HeaderMap,
    query: Result<Query<HistoryQuery>, QueryRejection>,
) -> Result<Conditional<RevisionPage>, AdminError> {
    let Query(query) = query.map_err(|rejection| AdminError::RequestInvalid {
        schema: "history",
        detail: rejection.body_text(),
    })?;
    let grant = api
        .authorize(
            &identity,
            AdminAction::ReadHistory,
            Surface::AuditTrail,
            &ResourceScope::Deployment,
        )
        .await?;
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
                    detail: format!("`start`: {}", id_detail(RevisionId::PREFIX, &error)),
                })?,
            )
        }
    };
    let page = api
        .service
        .history(&grant, HistoryRequest { limit, start })
        .await?;
    Ok(Conditional::new(&headers, page))
}

async fn audit(
    State(api): State<Arc<AdminApi>>,
    identity: AdminIdentity,
    headers: HeaderMap,
    Path(revision): Path<String>,
) -> Result<Conditional<AuditPage>, AdminError> {
    let grant = api
        .authorize(
            &identity,
            AdminAction::ReadAudit,
            Surface::AuditTrail,
            &ResourceScope::Deployment,
        )
        .await?;
    let revision = RevisionId::parse(&revision).map_err(|error| AdminError::RequestInvalid {
        schema: "audit",
        detail: format!("`revision`: {}", id_detail(RevisionId::PREFIX, &error)),
    })?;
    Ok(Conditional::new(
        &headers,
        api.service.audit(&grant, revision).await?,
    ))
}

/// What this replica has converged onto — answered from its own cached report,
/// so it still answers during a control-plane outage.
async fn convergence(
    State(api): State<Arc<AdminApi>>,
    identity: AdminIdentity,
    headers: HeaderMap,
) -> Result<Conditional<ConvergenceResult>, AdminError> {
    let grant = api
        .authorize(
            &identity,
            AdminAction::ReadConvergence,
            Surface::AuditTrail,
            &ResourceScope::Deployment,
        )
        .await?;
    let report = api.convergence_report();
    let result = api.service.convergence(&grant, report.as_ref())?;
    // Validated over the state, not the bytes: `lag_ms` moves every millisecond
    // a replica is behind, and the caller waiting on that is the one this read
    // exists for.
    let identity = result.identity();
    Ok(Conditional::identified_by(&headers, result, &identity))
}

/// What an availability read asks about.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct AvailabilityQuery {
    #[serde(default)]
    tenant: Option<String>,
    #[serde(default)]
    project: Option<String>,
}

/// Store material as a new secret's first version.
///
/// The one request body on this surface that carries material. It is parsed
/// into [`secrets::PresentedMaterial`], which does not render and is moved into a
/// zeroizing [`SecretMaterial`](crate::backends::secrets::SecretMaterial)
/// before anything else sees it — including the refusal for a body that failed
/// to parse, which reports serde's message about the *shape*.
async fn stage_secret(
    State(api): State<Arc<AdminApi>>,
    identity: AdminIdentity,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<SecretVersionView>, AdminError> {
    const SCHEMA: &str = "secret";
    let body = document(SCHEMA, body)?;
    let request: StageSecretRequest = secret_document(SCHEMA, &body)?;
    let owner = secrets::owner_of(SCHEMA, &request.tenant, request.project.as_deref())?;
    let material = secrets::material_of(SCHEMA, request.material)?;
    let grant = api
        .authorize(
            &identity,
            AdminAction::WriteSecrets,
            Surface::Credential,
            &owner.scope(),
        )
        .await?;
    Ok(Json(
        api.service.stage_secret(&grant, owner, material).await?,
    ))
}

/// Store material as the next version of an existing secret.
async fn rotate_secret(
    State(api): State<Arc<AdminApi>>,
    identity: AdminIdentity,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<SecretVersionView>, AdminError> {
    const SCHEMA: &str = "secret_rotation";
    let body = document(SCHEMA, body)?;
    let request: RotateSecretRequest = secret_document(SCHEMA, &body)?;
    let owner = secrets::owner_of(SCHEMA, &request.tenant, request.project.as_deref())?;
    let reference = secrets::reference_of(SCHEMA, &request.reference)?;
    let material = secrets::material_of(SCHEMA, request.material)?;
    let grant = api
        .authorize(
            &identity,
            AdminAction::WriteSecrets,
            Surface::Credential,
            &owner.scope(),
        )
        .await?;
    Ok(Json(
        api.service
            .rotate_secret(&grant, owner, reference, material)
            .await?,
    ))
}

/// Activate, disable, revoke, or destroy one version's material.
async fn secret_lifecycle(
    State(api): State<Arc<AdminApi>>,
    identity: AdminIdentity,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<SecretTransitionView>, AdminError> {
    const SCHEMA: &str = "secret_lifecycle";
    let body = document(SCHEMA, body)?;
    // This body carries no material, but an operator who posts the wrong one to
    // it has: parsed like the routes that do, so the mistake is refused without
    // the mistake being quoted back.
    let request: SecretLifecycleRequest = secret_document(SCHEMA, &body)?;
    let owner = secrets::owner_of(SCHEMA, &request.tenant, request.project.as_deref())?;
    let reference = secrets::reference_of(SCHEMA, &request.reference)?;
    let next = secrets::lifecycle_of(SCHEMA, &request.lifecycle)?;
    let grant = api
        .authorize(
            &identity,
            AdminAction::WriteSecrets,
            Surface::Credential,
            &owner.scope(),
        )
        .await?;
    Ok(Json(
        api.service
            .move_secret(&grant, owner, reference, next)
            .await?,
    ))
}

/// What a versions read names: whose material, and which secret.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SecretVersionsQuery {
    tenant: String,
    #[serde(default)]
    project: Option<String>,
}

/// What this replica derives about one scope's models — answered from the
/// snapshot it is serving and its own circuits, so it survives the control-plane
/// or provider outage that prompted the question.
async fn availability(
    State(api): State<Arc<AdminApi>>,
    identity: AdminIdentity,
    headers: HeaderMap,
    query: Result<Query<AvailabilityQuery>, QueryRejection>,
) -> Result<Conditional<AvailabilityResult>, AdminError> {
    let Query(query) = query.map_err(|rejection| AdminError::RequestInvalid {
        schema: "availability",
        detail: rejection.body_text(),
    })?;
    // Refused before any authority is consulted: an availability read is always
    // about a tenant, so a query that names none is a malformed request rather
    // than an attempt on the deployment. Authorizing it first would answer a
    // tenant-scoped caller's typo with a forbidden — and write it to the denial
    // trail an investigator later has to rule out. The service's own check stays
    // where it is, as the defence in depth it already was.
    if query.tenant.is_none() && query.project.is_none() {
        return Err(AdminError::RequestInvalid {
            schema: "availability",
            detail: "`tenant`: an availability read must name the tenant it asks about".to_owned(),
        });
    }
    let scope = scope_of(
        "availability",
        query.tenant.as_deref(),
        query.project.as_deref(),
    )?;
    let grant = api
        .authorize(
            &identity,
            AdminAction::ReadAvailability,
            Surface::Model,
            &scope,
        )
        .await?;
    // Asked separately from the grant, because the grant answers "may this
    // caller read this tenant" and disclosure turns on "would this caller be
    // trusted with the whole deployment" — and this route's scope is
    // tenant-shaped for every caller, root operator included.
    let authority = AvailabilityAuthority::of(
        api.holds_deployment_authority(&identity, AdminAction::ReadAvailability),
    );
    let result = api.service.availability(
        &grant,
        &scope,
        authority,
        api.availability.as_deref(),
        SystemTime::now(),
    )?;
    // Validated over the bytes, unlike `/convergence`: nothing in this answer
    // moves on its own. A verdict is evaluated against `now`, but it only
    // *changes* when evidence expires or a dimension does — which is the answer
    // changing, exactly what a validator is for. So an operator polling a target
    // through an incident pays for a body when something moved and not otherwise.
    Ok(Conditional::new(&headers, result))
}

/// Every version of one secret, with the state each is in — and no material,
/// because the store has no method that would return any.
///
/// Conditional like the rest of the administrative read surface: an operator
/// watching a staged version reach `active` polls this, and the validator turns
/// the reads between two lifecycle moves into a header comparison. The digest is
/// over a projection of references and states, so it discloses nothing the
/// caller was not already authorized to read.
async fn secret_versions(
    State(api): State<Arc<AdminApi>>,
    identity: AdminIdentity,
    headers: HeaderMap,
    Path(secret): Path<String>,
    query: Result<Query<SecretVersionsQuery>, QueryRejection>,
) -> Result<Conditional<SecretVersionsView>, AdminError> {
    const SCHEMA: &str = "secret_versions";
    let Query(query) = query.map_err(|rejection| AdminError::RequestInvalid {
        schema: SCHEMA,
        detail: rejection.body_text(),
    })?;
    let owner = secrets::owner_of(SCHEMA, &query.tenant, query.project.as_deref())?;
    let secret = secrets::secret_of(SCHEMA, &secret)?;
    let grant = api
        .authorize(
            &identity,
            AdminAction::ReadSecrets,
            Surface::Credential,
            &owner.scope(),
        )
        .await?;
    Ok(Conditional::new(
        &headers,
        api.service.secret_versions(&grant, owner, secret).await?,
    ))
}

/// Parse a body that carries material.
///
/// Separate from the other documents' inline `from_slice` for one reason: serde
/// renders the offending *input* into some of its messages, and this is the only
/// input on the surface that may be a credential. The refusal therefore says
/// which schema rejected it and nothing about what was in it — an administrator
/// debugging a malformed body has the request they sent, and the alternative is
/// a provider key in a log line.
fn secret_document<T: serde::de::DeserializeOwned>(
    schema: &'static str,
    body: &Bytes,
) -> Result<T, AdminError> {
    serde_json::from_slice(body).map_err(|error| AdminError::RequestInvalid {
        schema,
        detail: format!(
            "the document is not a valid `{schema}` request (line {}, column {}); its text is not \
             reported, because it carries material",
            error.line(),
            error.column()
        ),
    })
}

/// The scope a request names, from an optional tenant and project.
pub(super) fn scope_of(
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
            // A scope names ids, and a refusal is rendered into a response and a
            // log line, so it says what form was expected rather than repeating
            // the text a caller sent — the same boundary the document path holds.
            let tenant = TenantId::parse(tenant)
                .map_err(|error| invalid("tenant", id_detail(TenantId::PREFIX, &error)))?;
            match project {
                None => Ok(ResourceScope::Tenant(tenant)),
                Some(project) => {
                    let project = ProjectId::parse(project).map_err(|error| {
                        invalid("project", id_detail(ProjectId::PREFIX, &error))
                    })?;
                    Ok(ResourceScope::Project { tenant, project })
                }
            }
        }
    }
}

/// Why an id a request named was refused, without echoing the text that
/// arrived: a refusal reaches a response body, a log line and an audit trail,
/// and material mispasted where an id belongs must not reach any of them.
fn id_detail(prefix: &'static str, error: &InvalidId) -> String {
    match error {
        InvalidId::Prefix { .. } => format!("is not a `{prefix}`-prefixed id"),
        InvalidId::Uuid(uuid) => format!("has a uuid that {}", uuid_detail(uuid)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PASTED_MATERIAL: &str = "sk-axond-admin-sentinel-51H9xNEVERLOGME";

    fn detail(error: AdminError) -> String {
        error
            .operator_detail()
            .expect("a request refusal has operator detail")
            .to_owned()
    }

    /// A scope the caller spelled wrongly is refused for the reason it failed,
    /// and the text it sent is never rendered back: a refusal reaches a response
    /// body, a log line and a transcript of the session.
    #[test]
    fn a_malformed_scope_id_is_refused_without_echoing_what_arrived() {
        let refusal = detail(
            scope_of("rollback", Some(PASTED_MATERIAL), None).expect_err("a wrong-prefix tenant"),
        );
        assert_eq!(refusal, "`tenant`: is not a `ten_`-prefixed id");

        let malformed = format!("{}not-a-uuid", TenantId::PREFIX);
        let refusal =
            detail(scope_of("rollback", Some(&malformed), None).expect_err("a malformed uuid"));
        assert_eq!(
            refusal,
            "`tenant`: has a uuid that is not a hyphenated 8-4-4-4-12 uuid"
        );
        assert!(!refusal.contains(&malformed));

        let tenant = format!("{}0189f8c1-2a3b-7c4d-8e5f-6a7b8c9d0e1f", TenantId::PREFIX);
        let refusal = detail(
            scope_of("rollback", Some(&tenant), Some(PASTED_MATERIAL))
                .expect_err("a wrong-prefix project"),
        );
        assert_eq!(refusal, "`project`: is not a `prj_`-prefixed id");
        assert!(!refusal.contains(PASTED_MATERIAL));
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
