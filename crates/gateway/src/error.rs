//! Typed gateway errors → HTTP responses.
//!
//! Every route always exists and returns a *typed* error explaining its own
//! state (delta B3). We never 404 a whole route behind a kill switch, because
//! a 404 from a proxy is indistinguishable from a wrong `base_url`.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use gateway_core::ProviderError;
use gateway_transport::TransportError;
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("unknown model `{0}`")]
    UnknownModel(String),
    #[error("no credential for provider `{provider}` in namespace `{namespace}`")]
    NoCredential { namespace: String, provider: String },
    #[error("budget exceeded for model `{0}`")]
    BudgetExceeded(String),
    #[error("budget store is unavailable")]
    BudgetUnavailable,
    #[error("unauthorized")]
    Unauthorized,
    #[error("{0} is not implemented yet")]
    NotImplemented(&'static str),
    #[error(transparent)]
    Provider(#[from] ProviderError),
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error("bad request: {0}")]
    BadRequest(String),
}

impl GatewayError {
    fn status(&self) -> StatusCode {
        match self {
            Self::UnknownModel(_) => StatusCode::NOT_FOUND,
            Self::NoCredential { .. } => StatusCode::BAD_GATEWAY,
            Self::BudgetExceeded(_) => StatusCode::TOO_MANY_REQUESTS,
            // Fail-closed: the cap cannot be enforced, so the request is a
            // dependency failure rather than an over-cap caller (ADR 0010).
            Self::BudgetUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Provider(e) => match e {
                ProviderError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
                ProviderError::ContextWindowExceeded(_) => StatusCode::BAD_REQUEST,
                ProviderError::Unsupported(_) => StatusCode::NOT_IMPLEMENTED,
                ProviderError::ModelUnavailable(_) => StatusCode::BAD_GATEWAY,
                ProviderError::Dependency(_) => StatusCode::BAD_GATEWAY,
                ProviderError::InvalidStream(_) => StatusCode::BAD_GATEWAY,
                ProviderError::AllCircuitsOpen(_) => StatusCode::SERVICE_UNAVAILABLE,
            },
            Self::Transport(TransportError::Provider(_)) => StatusCode::BAD_GATEWAY,
            Self::Transport(TransportError::Http(_)) => StatusCode::BAD_GATEWAY,
        }
    }

    fn code(&self) -> &str {
        match self {
            Self::UnknownModel(_) => "unknown_model",
            Self::NoCredential { .. } => "no_credential",
            Self::BudgetExceeded(_) => "budget_exceeded",
            Self::BudgetUnavailable => "budget_unavailable",
            Self::Unauthorized => "unauthorized",
            Self::NotImplemented(_) => "not_implemented",
            Self::BadRequest(_) => "bad_request",
            Self::Provider(e) => e.code(),
            Self::Transport(TransportError::Provider(e)) => e.code(),
            Self::Transport(TransportError::Http(_)) => "upstream_transport",
        }
    }
}

impl IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        let body = json!({
            "error": {
                "type": self.code(),
                "message": self.to_string(),
            }
        });
        (self.status(), Json(body)).into_response()
    }
}
