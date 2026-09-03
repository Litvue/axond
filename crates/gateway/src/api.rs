//! `/api/v1` management surface (ADR 0063). OpenAPI generation is a later slice.

use std::sync::Arc;

use axum::Router;
use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, response::IntoResponse};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::GatewayError;
use crate::state::AppState;
use crate::store::{
    BudgetRecord, NamespaceRecord, Store, StoreError, validate_attrs, validate_namespace_id,
    validate_period,
};

/// Bound for the whole management request body. Attrs alone are capped at
/// 4 KiB ([`crate::store::MAX_ATTRS_BYTES`]); this leaves room for id and
/// blocklist without letting a single key write multi-megabyte rows into the
/// store.
const MANAGEMENT_MAX_REQUEST_BYTES: usize = 64 * 1024;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route(
            "/api/v1/namespaces",
            post(create_namespace).get(list_namespaces),
        )
        .route(
            "/api/v1/namespaces/{ns}",
            get(get_namespace).put(put_namespace),
        )
        .route(
            "/api/v1/namespaces/{ns}/budgets/{period}",
            get(get_budget).put(put_budget),
        )
        .layer(DefaultBodyLimit::max(MANAGEMENT_MAX_REQUEST_BYTES))
        .with_state(state)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateBody {
    id: String,
    #[serde(default)]
    attrs: Value,
    #[serde(default)]
    blocklist: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplaceBody {
    #[serde(default)]
    attrs: Value,
    #[serde(default)]
    blocklist: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    cursor: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PutBudgetBody {
    limit_microdollars: u64,
}

#[derive(Debug, Serialize)]
struct ListBody {
    data: Vec<NamespaceRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
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

async fn list_namespaces(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> Result<Json<ListBody>, GatewayError> {
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
