//! `/api/v1` management surface (ADR 0063).
//!
//! OpenAPI 3.1 is generated from these handlers and served at
//! `GET /api/v1/openapi.json` behind the same static key.

use std::sync::Arc;

use axum::Router;
use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, response::IntoResponse};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi, ToSchema};

use crate::error::GatewayError;
use crate::state::AppState;
use crate::store::{
    BudgetRecord, NamespaceRecord, ProviderModels, Store, StoreError, UsageSummary, validate_attrs,
    validate_namespace_id, validate_period,
};

/// Bound for the whole management request body. Attrs alone are capped at
/// 4 KiB ([`crate::store::MAX_ATTRS_BYTES`]); this leaves room for id and
/// blocklist without letting a single key write multi-megabyte rows into the
/// store.
const MANAGEMENT_MAX_REQUEST_BYTES: usize = 64 * 1024;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/openapi.json", get(openapi_spec))
        .route(
            "/api/v1/namespaces",
            post(create_namespace).get(list_namespaces),
        )
        .route(
            "/api/v1/namespaces/{ns}",
            get(get_namespace)
                .put(put_namespace)
                .delete(delete_namespace),
        )
        .route(
            "/api/v1/namespaces/{ns}/budgets/{period}",
            get(get_budget).put(put_budget),
        )
        .route("/api/v1/namespaces/{ns}/usage", get(get_usage))
        .route("/api/v1/providers/models", get(list_provider_models))
        .route("/api/v1/providers/{id}/models", get(get_provider_models))
        .layer(DefaultBodyLimit::max(MANAGEMENT_MAX_REQUEST_BYTES))
        .with_state(state)
}

/// Compile-time OpenAPI 3.1 document for the mounted `/api/v1` routes.
pub fn openapi_document() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}

struct GatewayKeySecurity;

impl Modify for GatewayKeySecurity {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "gateway_key",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .description(Some(
                        "Deployment-wide static API key (`Authorization: Bearer`).",
                    ))
                    .build(),
            ),
        );
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Axond management API",
        description = "Namespaced gateway management surface (ADR 0063). Inference errors such as `budget_exceeded` and `unpriced_model` are not returned here.",
        version = "1"
    ),
    servers((url = "/", description = "This gateway")),
    tags(
        (name = "spec", description = "Generated OpenAPI document"),
        (name = "namespaces", description = "Namespace CRUD"),
        (name = "budgets", description = "Per-namespace per-period ledger"),
        (name = "usage", description = "Usage summary by model and status"),
        (name = "providers", description = "Cached upstream model listings")
    ),
    paths(
        openapi_spec,
        create_namespace,
        list_namespaces,
        get_namespace,
        put_namespace,
        delete_namespace,
        put_budget,
        get_budget,
        get_usage,
        list_provider_models,
        get_provider_models
    ),
    modifiers(&GatewayKeySecurity),
    security(("gateway_key" = []))
)]
struct ApiDoc;

/// Typed error envelope used on the management surface.
#[derive(Debug, Serialize, ToSchema)]
struct ErrorEnvelope {
    error: TypedError,
}

#[derive(Debug, Serialize, ToSchema)]
struct TypedError {
    /// Stable code: `unknown_namespace`, `unknown_budget`, `unknown_provider`,
    /// `namespace_conflict`, `store_unavailable`, `bad_request`, `unauthorized`,
    /// `request_too_large`, or `unsupported_media_type`.
    #[serde(rename = "type")]
    r#type: String,
    message: String,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
struct CreateBody {
    id: String,
    #[serde(default)]
    attrs: Value,
    #[serde(default)]
    blocklist: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
struct ReplaceBody {
    #[serde(default)]
    attrs: Value,
    #[serde(default)]
    blocklist: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct ListQuery {
    cursor: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct UsageQuery {
    period: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
struct PutBudgetBody {
    limit_microdollars: u64,
}

#[derive(Debug, Serialize, ToSchema)]
struct ListBody {
    data: Vec<NamespaceRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
struct ProviderModelsList {
    data: Vec<ProviderModels>,
}

fn store(state: &AppState) -> Result<&Arc<dyn Store>, GatewayError> {
    state.store().ok_or(GatewayError::StoreUnavailable)
}

fn normalize_attrs(attrs: Value) -> Result<Value, GatewayError> {
    let attrs = if attrs.is_null() {
        Value::Object(Default::default())
    } else {
        attrs
    };
    validate_attrs(&attrs).map_err(|err| GatewayError::BadRequest(err.to_string()))?;
    Ok(attrs)
}

const MAX_BLOCKLIST_BYTES: usize = 4096;
const MAX_BLOCKLIST_ENTRIES: usize = 64;

fn validate_payload(attrs: &Value, blocklist: &Option<Vec<String>>) -> Result<(), GatewayError> {
    validate_attrs(attrs).map_err(|err| GatewayError::BadRequest(err.to_string()))?;
    if let Some(list) = blocklist {
        if list.len() > MAX_BLOCKLIST_ENTRIES {
            return Err(GatewayError::BadRequest(
                "namespace blocklist exceeds 64 entries".into(),
            ));
        }
        if serde_json::to_string(list).unwrap_or_default().len() > MAX_BLOCKLIST_BYTES {
            return Err(GatewayError::BadRequest(
                "namespace blocklist exceeds 4 KiB".into(),
            ));
        }
        for pattern in list {
            crate::config::validate_glob_pattern(pattern).map_err(GatewayError::BadRequest)?;
        }
    }
    Ok(())
}

fn json_body<T>(body: Result<Json<T>, JsonRejection>) -> Result<T, GatewayError> {
    match body {
        Ok(Json(body)) => Ok(body),
        Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            Err(GatewayError::RequestTooLarge)
        }
        Err(JsonRejection::MissingJsonContentType(_)) => Err(GatewayError::UnsupportedMediaType),
        Err(err) => Err(GatewayError::BadRequest(err.body_text())),
    }
}

fn query_value<T>(query: Result<Query<T>, QueryRejection>) -> Result<T, GatewayError> {
    match query {
        Ok(Query(value)) => Ok(value),
        Err(rejection) => Err(GatewayError::BadRequest(rejection.body_text())),
    }
}

fn required_period(period: Option<String>) -> Result<String, GatewayError> {
    let Some(period) = period.filter(|value| !value.is_empty()) else {
        return Err(GatewayError::BadRequest("`period` is required".into()));
    };
    validate_period(&period).map_err(|err| GatewayError::BadRequest(err.to_string()))?;
    Ok(period)
}

/// OpenAPI 3.1 document for the mounted `/api/v1` routes.
#[utoipa::path(
    get,
    path = "/api/v1/openapi.json",
    tag = "spec",
    responses(
        (status = 200, description = "OpenAPI 3.1 document", content_type = "application/json"),
        (status = 401, description = "Missing or wrong gateway key", body = ErrorEnvelope)
    )
)]
async fn openapi_spec() -> Json<utoipa::openapi::OpenApi> {
    Json(openapi_document())
}

/// Create a namespace.
#[utoipa::path(
    post,
    path = "/api/v1/namespaces",
    tag = "namespaces",
    request_body = CreateBody,
    responses(
        (status = 201, description = "Created", body = NamespaceRecord),
        (status = 400, description = "Malformed body or invalid id", body = ErrorEnvelope),
        (status = 401, description = "Missing or wrong gateway key", body = ErrorEnvelope),
        (status = 409, description = "`namespace_conflict`", body = ErrorEnvelope),
        (status = 413, description = "`request_too_large`", body = ErrorEnvelope),
        (status = 415, description = "`unsupported_media_type`", body = ErrorEnvelope),
        (status = 503, description = "`store_unavailable`", body = ErrorEnvelope)
    )
)]
async fn create_namespace(
    State(state): State<AppState>,
    body: Result<Json<CreateBody>, JsonRejection>,
) -> Result<(StatusCode, Json<NamespaceRecord>), GatewayError> {
    let body = json_body(body)?;
    validate_namespace_id(&body.id).map_err(|err| GatewayError::BadRequest(err.to_string()))?;
    validate_payload(&body.attrs, &body.blocklist)?;
    let rec = NamespaceRecord {
        id: body.id,
        attrs: normalize_attrs(body.attrs)?,
        blocklist: body.blocklist,
    };
    match store(&state)?.put_namespace(rec.clone()).await {
        Ok(()) => Ok((StatusCode::CREATED, Json(rec))),
        Err(StoreError::Duplicate(_)) => Err(GatewayError::NamespaceConflict),
        Err(StoreError::Invalid(msg)) => Err(GatewayError::BadRequest(msg)),
        Err(StoreError::Unavailable(_)) => Err(GatewayError::StoreUnavailable),
        Err(StoreError::NotFound(_)) => Err(GatewayError::UnknownNamespace),
    }
}

/// Read a namespace.
#[utoipa::path(
    get,
    path = "/api/v1/namespaces/{ns}",
    tag = "namespaces",
    params(("ns" = String, Path, description = "Namespace id")),
    responses(
        (status = 200, description = "Namespace", body = NamespaceRecord),
        (status = 401, description = "Missing or wrong gateway key", body = ErrorEnvelope),
        (status = 404, description = "`unknown_namespace`", body = ErrorEnvelope),
        (status = 503, description = "`store_unavailable`", body = ErrorEnvelope)
    )
)]
async fn get_namespace(
    State(state): State<AppState>,
    Path(ns): Path<String>,
) -> Result<Json<NamespaceRecord>, GatewayError> {
    match store(&state)?.get_namespace(&ns).await {
        Ok(Some(rec)) => Ok(Json(rec)),
        Ok(None) => Err(GatewayError::UnknownNamespace),
        Err(StoreError::Unavailable(_)) => Err(GatewayError::StoreUnavailable),
        Err(err) => Err(GatewayError::BadRequest(err.to_string())),
    }
}

/// Replace namespace attrs and blocklist. The id is immutable.
#[utoipa::path(
    put,
    path = "/api/v1/namespaces/{ns}",
    tag = "namespaces",
    params(("ns" = String, Path, description = "Namespace id")),
    request_body = ReplaceBody,
    responses(
        (status = 200, description = "Replaced attrs/blocklist", body = NamespaceRecord),
        (status = 400, description = "Malformed body", body = ErrorEnvelope),
        (status = 401, description = "Missing or wrong gateway key", body = ErrorEnvelope),
        (status = 404, description = "`unknown_namespace`", body = ErrorEnvelope),
        (status = 413, description = "`request_too_large`", body = ErrorEnvelope),
        (status = 415, description = "`unsupported_media_type`", body = ErrorEnvelope),
        (status = 503, description = "`store_unavailable`", body = ErrorEnvelope)
    )
)]
async fn put_namespace(
    State(state): State<AppState>,
    Path(ns): Path<String>,
    body: Result<Json<ReplaceBody>, JsonRejection>,
) -> Result<Json<NamespaceRecord>, GatewayError> {
    let body = json_body(body)?;
    validate_payload(&body.attrs, &body.blocklist)?;
    let attrs = normalize_attrs(body.attrs)?;
    match store(&state)?
        .update_namespace(&ns, attrs, body.blocklist)
        .await
    {
        Ok(Some(rec)) => Ok(Json(rec)),
        Ok(None) => Err(GatewayError::UnknownNamespace),
        Err(StoreError::Unavailable(_)) => Err(GatewayError::StoreUnavailable),
        Err(err) => Err(GatewayError::BadRequest(err.to_string())),
    }
}

/// Idempotent remove. Missing id is 204; live budget rows go with it.
#[utoipa::path(
    delete,
    path = "/api/v1/namespaces/{ns}",
    tag = "namespaces",
    params(("ns" = String, Path, description = "Namespace id")),
    responses(
        (status = 204, description = "Deleted, or already absent"),
        (status = 401, description = "Missing or wrong gateway key", body = ErrorEnvelope),
        (status = 409, description = "`namespace_conflict` when `{ns}` is still in the deployment file", body = ErrorEnvelope),
        (status = 503, description = "`store_unavailable`", body = ErrorEnvelope)
    )
)]
async fn delete_namespace(
    State(state): State<AppState>,
    Path(ns): Path<String>,
) -> Result<StatusCode, GatewayError> {
    if state
        .config()
        .config
        .namespace
        .iter()
        .any(|namespace| namespace.id == ns)
    {
        return Err(GatewayError::NamespaceConflict);
    }
    match store(&state)?.delete_namespace(&ns).await {
        Ok(_) => Ok(StatusCode::NO_CONTENT),
        Err(StoreError::Unavailable(_)) => Err(GatewayError::StoreUnavailable),
        Err(err) => Err(GatewayError::BadRequest(err.to_string())),
    }
}

/// Set the period limit and mark it as the namespace's active period.
#[utoipa::path(
    put,
    path = "/api/v1/namespaces/{ns}/budgets/{period}",
    tag = "budgets",
    params(
        ("ns" = String, Path, description = "Namespace id"),
        ("period" = String, Path, description = "Opaque billing period")
    ),
    request_body = PutBudgetBody,
    responses(
        (status = 200, description = "Ledger after the PUT", body = BudgetRecord),
        (status = 400, description = "Malformed body or period", body = ErrorEnvelope),
        (status = 401, description = "Missing or wrong gateway key", body = ErrorEnvelope),
        (status = 404, description = "`unknown_namespace`", body = ErrorEnvelope),
        (status = 413, description = "`request_too_large`", body = ErrorEnvelope),
        (status = 415, description = "`unsupported_media_type`", body = ErrorEnvelope),
        (status = 503, description = "`store_unavailable`", body = ErrorEnvelope)
    )
)]
async fn put_budget(
    State(state): State<AppState>,
    Path((ns, period)): Path<(String, String)>,
    body: Result<Json<PutBudgetBody>, JsonRejection>,
) -> Result<Json<BudgetRecord>, GatewayError> {
    let body = json_body(body)?;
    validate_period(&period).map_err(|err| GatewayError::BadRequest(err.to_string()))?;
    match store(&state)?
        .put_budget(&ns, &period, body.limit_microdollars)
        .await
    {
        Ok(rec) => Ok(Json(rec)),
        Err(StoreError::NotFound(_)) => Err(GatewayError::UnknownNamespace),
        Err(StoreError::Invalid(msg)) => Err(GatewayError::BadRequest(msg)),
        Err(StoreError::Unavailable(_)) => Err(GatewayError::StoreUnavailable),
        Err(StoreError::Duplicate(_)) => Err(GatewayError::NamespaceConflict),
    }
}

/// Read the ledger for a namespace and period.
#[utoipa::path(
    get,
    path = "/api/v1/namespaces/{ns}/budgets/{period}",
    tag = "budgets",
    params(
        ("ns" = String, Path, description = "Namespace id"),
        ("period" = String, Path, description = "Opaque billing period")
    ),
    responses(
        (status = 200, description = "Ledger", body = BudgetRecord),
        (status = 400, description = "Invalid period", body = ErrorEnvelope),
        (status = 401, description = "Missing or wrong gateway key", body = ErrorEnvelope),
        (status = 404, description = "`unknown_namespace` or `unknown_budget`", body = ErrorEnvelope),
        (status = 503, description = "`store_unavailable`", body = ErrorEnvelope)
    )
)]
async fn get_budget(
    State(state): State<AppState>,
    Path((ns, period)): Path<(String, String)>,
) -> Result<Json<BudgetRecord>, GatewayError> {
    validate_period(&period).map_err(|err| GatewayError::BadRequest(err.to_string()))?;
    let store = store(&state)?;
    match store.get_namespace(&ns).await {
        Ok(Some(_)) => {}
        Ok(None) => return Err(GatewayError::UnknownNamespace),
        Err(StoreError::Unavailable(_)) => return Err(GatewayError::StoreUnavailable),
        Err(err) => return Err(GatewayError::BadRequest(err.to_string())),
    }
    match store.get_budget(&ns, &period).await {
        Ok(Some(rec)) => Ok(Json(rec)),
        Ok(None) => Err(GatewayError::UnknownBudget),
        Err(StoreError::Unavailable(_)) => Err(GatewayError::StoreUnavailable),
        Err(err) => Err(GatewayError::BadRequest(err.to_string())),
    }
}

/// List namespaces, cursor-paginated.
#[utoipa::path(
    get,
    path = "/api/v1/namespaces",
    tag = "namespaces",
    params(
        ("cursor" = Option<String>, Query, description = "List cursor from a previous page"),
        ("limit" = Option<u32>, Query, description = "Page size, default 100, max 1000")
    ),
    responses(
        (status = 200, description = "Cursor-paginated namespaces", body = ListBody),
        (status = 400, description = "Invalid `limit`", body = ErrorEnvelope),
        (status = 401, description = "Missing or wrong gateway key", body = ErrorEnvelope),
        (status = 503, description = "`store_unavailable`", body = ErrorEnvelope)
    )
)]
async fn list_namespaces(
    State(state): State<AppState>,
    query: Result<Query<ListQuery>, QueryRejection>,
) -> Result<Json<ListBody>, GatewayError> {
    let query = query_value(query)?;
    let limit = query.limit.unwrap_or(100);
    if !(1..=1000).contains(&limit) {
        return Err(GatewayError::BadRequest(
            "`limit` must be between 1 and 1000".into(),
        ));
    }
    match store(&state)?.list_namespaces(query.cursor, limit).await {
        Ok((data, next_cursor)) => Ok(Json(ListBody { data, next_cursor })),
        Err(StoreError::Unavailable(_)) => Err(GatewayError::StoreUnavailable),
        Err(err) => Err(GatewayError::BadRequest(err.to_string())),
    }
}

/// Summary by model and status for one period. `period` is required.
#[utoipa::path(
    get,
    path = "/api/v1/namespaces/{ns}/usage",
    tag = "usage",
    params(
        ("ns" = String, Path, description = "Namespace id"),
        ("period" = String, Query, description = "Budget period; required")
    ),
    responses(
        (status = 200, description = "Per-model per-status counts and cost totals", body = UsageSummary),
        (status = 400, description = "Missing or invalid `period`", body = ErrorEnvelope),
        (status = 401, description = "Missing or wrong gateway key", body = ErrorEnvelope),
        (status = 404, description = "`unknown_namespace`", body = ErrorEnvelope),
        (status = 503, description = "`store_unavailable`", body = ErrorEnvelope)
    )
)]
async fn get_usage(
    State(state): State<AppState>,
    Path(ns): Path<String>,
    query: Result<Query<UsageQuery>, QueryRejection>,
) -> Result<Json<UsageSummary>, GatewayError> {
    let query = query_value(query)?;
    let period = required_period(query.period)?;
    let store = store(&state)?;
    match store.get_namespace(&ns).await {
        Ok(Some(_)) => {}
        Ok(None) => return Err(GatewayError::UnknownNamespace),
        Err(StoreError::Unavailable(_)) => return Err(GatewayError::StoreUnavailable),
        Err(err) => return Err(GatewayError::BadRequest(err.to_string())),
    }
    match store.summarize_usage(&ns, &period).await {
        Ok(data) => Ok(Json(UsageSummary {
            namespace: ns,
            period,
            data,
        })),
        Err(StoreError::Unavailable(_)) => Err(GatewayError::StoreUnavailable),
        Err(err) => Err(GatewayError::BadRequest(err.to_string())),
    }
}

/// Cached upstream listing for one configured provider.
#[utoipa::path(
    get,
    path = "/api/v1/providers/{id}/models",
    tag = "providers",
    params(("id" = String, Path, description = "Configured provider id")),
    responses(
        (status = 200, description = "Cached listing, possibly stale", body = ProviderModels),
        (status = 400, description = "`unknown_provider`", body = ErrorEnvelope),
        (status = 401, description = "Missing or wrong gateway key", body = ErrorEnvelope),
        (status = 503, description = "`store_unavailable`", body = ErrorEnvelope)
    )
)]
async fn get_provider_models(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ProviderModels>, GatewayError> {
    let snapshot = state.config();
    let Some(provider) = snapshot
        .config
        .provider
        .iter()
        .find(|provider| provider.id == id)
    else {
        return Err(GatewayError::UnknownProvider(id));
    };
    match store(&state)?.get_provider_models(&id).await {
        Ok(Some(row)) => Ok(Json(row.against_source(&provider.base_url))),
        Ok(None) => Ok(Json(ProviderModels::empty_stale(id))),
        Err(StoreError::Unavailable(_)) => Err(GatewayError::StoreUnavailable),
        Err(err) => Err(GatewayError::BadRequest(err.to_string())),
    }
}

/// Fan-out of the cached listing across every configured provider.
#[utoipa::path(
    get,
    path = "/api/v1/providers/models",
    tag = "providers",
    responses(
        (status = 200, description = "Per-provider cached listings", body = ProviderModelsList),
        (status = 401, description = "Missing or wrong gateway key", body = ErrorEnvelope),
        (status = 503, description = "`store_unavailable`", body = ErrorEnvelope)
    )
)]
async fn list_provider_models(
    State(state): State<AppState>,
) -> Result<Json<ProviderModelsList>, GatewayError> {
    let snapshot = state.config();
    let cached = match store(&state)?.list_provider_models().await {
        Ok(rows) => rows,
        Err(StoreError::Unavailable(_)) => return Err(GatewayError::StoreUnavailable),
        Err(err) => return Err(GatewayError::BadRequest(err.to_string())),
    };
    let data = snapshot
        .config
        .provider
        .iter()
        .map(|provider| {
            cached
                .iter()
                .find(|row| row.provider == provider.id)
                .cloned()
                .map(|row| row.against_source(&provider.base_url))
                .unwrap_or_else(|| ProviderModels::empty_stale(provider.id.clone()))
        })
        .collect();
    Ok(Json(ProviderModelsList { data }))
}

impl IntoResponse for StoreError {
    fn into_response(self) -> axum::response::Response {
        GatewayError::from(self).into_response()
    }
}

impl From<StoreError> for GatewayError {
    fn from(err: StoreError) -> Self {
        match err {
            StoreError::Duplicate(_) => Self::NamespaceConflict,
            StoreError::NotFound(_) => Self::UnknownNamespace,
            StoreError::Unavailable(_) => Self::StoreUnavailable,
            StoreError::Invalid(msg) => Self::BadRequest(msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::openapi_document;

    #[test]
    fn openapi_spec_is_31_and_covers_mounted_routes() {
        let spec = serde_json::to_value(openapi_document()).expect("serialize");
        let version = spec["openapi"].as_str().expect("openapi");
        assert!(
            version.starts_with("3.1"),
            "expected OpenAPI 3.1, got {version}"
        );

        let paths = spec["paths"].as_object().expect("paths");
        for path in [
            "/api/v1/openapi.json",
            "/api/v1/namespaces",
            "/api/v1/namespaces/{ns}",
            "/api/v1/namespaces/{ns}/budgets/{period}",
            "/api/v1/namespaces/{ns}/usage",
            "/api/v1/providers/{id}/models",
            "/api/v1/providers/models",
        ] {
            assert!(paths.contains_key(path), "missing {path}");
        }
        assert!(paths["/api/v1/namespaces"].get("post").is_some());
        assert!(paths["/api/v1/namespaces"].get("get").is_some());
        assert!(paths["/api/v1/namespaces/{ns}"].get("get").is_some());
        assert!(paths["/api/v1/namespaces/{ns}"].get("put").is_some());
        assert!(paths["/api/v1/namespaces/{ns}"].get("delete").is_some());
        assert!(
            paths["/api/v1/namespaces/{ns}/budgets/{period}"]["get"]["responses"]
                .get("400")
                .is_some(),
            "invalid period is a typed 400"
        );
        assert!(paths["/api/v1/providers/{id}/models"].get("get").is_some());
        assert!(paths["/api/v1/providers/models"].get("get").is_some());
        assert!(
            paths
                .keys()
                .filter(|path| path.contains("/providers"))
                .all(|path| {
                    *path == "/api/v1/providers/{id}/models" || *path == "/api/v1/providers/models"
                }),
            "only the two discovery routes are mounted: {paths:?}"
        );

        let usage = &paths["/api/v1/namespaces/{ns}/usage"]["get"];
        let params = usage["parameters"].as_array().expect("usage params");
        let period = params
            .iter()
            .find(|param| param["name"] == "period")
            .expect("period param");
        assert_eq!(period["in"], "query");
        assert_eq!(period["required"], true);

        let scheme = &spec["components"]["securitySchemes"]["gateway_key"];
        assert_eq!(scheme["type"], "http");
        assert_eq!(scheme["scheme"], "bearer");

        let models = &spec["components"]["schemas"]["ProviderModels"]["properties"];
        assert!(models.get("source").is_none(), "cache source is internal");

        if let Ok(path) = std::env::var("AXOND_OPENAPI_OUT") {
            let pretty = serde_json::to_vec_pretty(&spec).expect("pretty spec");
            std::fs::write(&path, pretty).unwrap_or_else(|error| {
                panic!("write OpenAPI spec to {path}: {error}");
            });
        }
    }
}
