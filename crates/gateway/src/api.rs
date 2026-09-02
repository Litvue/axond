//! `/api/v1` management surface (ADR 0063). OpenAPI generation is a later slice.

use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, response::IntoResponse};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::GatewayError;
use crate::state::AppState;
use crate::store::{NamespaceRecord, Store, StoreError, validate_namespace_id};

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
        .with_state(state)
}

#[derive(Debug, Deserialize)]
struct CreateBody {
    id: String,
    #[serde(default)]
    attrs: Value,
    #[serde(default)]
    blocklist: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Serialize)]
struct ListBody {
    data: Vec<NamespaceRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
}

fn store(state: &AppState) -> Result<&Arc<dyn Store>, GatewayError> {
    state.store().ok_or(GatewayError::StoreUnavailable)
}

async fn create_namespace(
    State(state): State<AppState>,
    Json(body): Json<CreateBody>,
) -> Result<(StatusCode, Json<NamespaceRecord>), GatewayError> {
    validate_namespace_id(&body.id).map_err(|err| GatewayError::BadRequest(err.to_string()))?;
    let rec = NamespaceRecord {
        id: body.id,
        attrs: if body.attrs.is_null() {
            Value::Object(Default::default())
        } else {
            body.attrs
        },
        blocklist: body.blocklist,
    };
    match store(&state)?.put_namespace(rec.clone()).await {
        Ok(()) => Ok((StatusCode::CREATED, Json(rec))),
        Err(StoreError::Duplicate(_)) => Err(GatewayError::NamespaceConflict),
        Err(StoreError::Invalid(msg)) => Err(GatewayError::BadRequest(msg)),
        Err(StoreError::Unavailable(_)) => Err(GatewayError::StoreUnavailable),
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
    Json(body): Json<ReplaceBody>,
) -> Result<Json<NamespaceRecord>, GatewayError> {
    match store(&state)?
        .update_namespace(&ns, body.attrs, body.blocklist)
        .await
    {
        Ok(Some(rec)) => Ok(Json(rec)),
        Ok(None) => Err(GatewayError::UnknownNamespace),
        Err(StoreError::Unavailable(_)) => Err(GatewayError::StoreUnavailable),
        Err(err) => Err(GatewayError::BadRequest(err.to_string())),
    }
}

async fn list_namespaces(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> Result<Json<ListBody>, GatewayError> {
    let limit = query.limit.unwrap_or(100).min(1000);
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
            StoreError::Unavailable(_) => Self::StoreUnavailable,
            StoreError::Invalid(msg) => Self::BadRequest(msg),
        }
    }
}
