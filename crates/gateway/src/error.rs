//! Typed gateway errors → HTTP responses.
//!
//! Every route always exists and returns a *typed* error explaining its own
//! state (delta B3). We never 404 a whole route behind a kill switch, because
//! a 404 from a proxy is indistinguishable from a wrong `base_url`.
//!
//! The narrow exception is an opt-in issuance endpoint: when it is not
//! configured, it is not registered at all because absence is the security
//! property there.

use axum::Json;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use gateway_core::ProviderError;
use gateway_transport::TransportError;
use serde_json::json;

use crate::principals::{Capability, TokenVerificationError};

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("unknown model `{0}`")]
    UnknownModel(String),
    #[error("no credential for provider `{provider}` in namespace `{namespace}`")]
    NoCredential { namespace: String, provider: String },
    #[error("budget exceeded for model `{0}`")]
    BudgetExceeded(String),
    #[error(
        "request cost ceiling exceeded for model `{alias}`: estimated {estimated_microdollars} microdollars exceeds the per-request ceiling of {ceiling_microdollars} microdollars"
    )]
    RequestCostCeilingExceeded {
        alias: String,
        estimated_microdollars: u64,
        ceiling_microdollars: u64,
    },
    #[error("budget store is unavailable")]
    BudgetUnavailable,
    #[error("rate-limit store is unavailable")]
    RateLimitUnavailable,
    #[error("revocation store is unavailable")]
    RevocationUnavailable,
    #[error("inbound concurrency limit exceeded")]
    RateLimitExceeded { retry_after_seconds: Option<u64> },
    #[error("unauthorized")]
    Unauthorized,
    #[error("token authentication failed: {0}")]
    TokenUnauthorized(#[source] TokenVerificationError),
    #[error("token authorization failed: {0}")]
    TokenForbidden(#[source] TokenVerificationError),
    #[error("token scope does not authorize `{0}`")]
    ScopeInsufficient(Capability),
    #[error("{0} is not implemented yet")]
    NotImplemented(&'static str),
    #[error(transparent)]
    Provider(#[from] ProviderError),
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("minting is disabled")]
    MintingDisabled,
    #[error("caller is not authorized to mint tokens")]
    MintNotAuthorized,
    #[error("requested claims are not narrower than the minting ceiling")]
    MintClaimsNotNarrowing,
    #[error(
        "minting key `{kid}` has an epoch at {min_iat} that cannot produce a currently valid token"
    )]
    MintEpochNotUsable { kid: String, min_iat: u64 },
    /// A native route reached with an alias whose target cannot speak that wire
    /// shape (an OpenAI-only alias on `/v1/messages`, say). The caller asked for
    /// something the configuration cannot serve, so it is a request error rather
    /// than an upstream failure.
    #[error("model `{alias}` cannot serve {route}: provider `{provider}` does not speak that wire")]
    UnsupportedWire {
        route: &'static str,
        alias: String,
        provider: String,
    },
}

impl GatewayError {
    fn status(&self) -> StatusCode {
        match self {
            Self::UnknownModel(_) => StatusCode::NOT_FOUND,
            Self::NoCredential { .. } => StatusCode::BAD_GATEWAY,
            Self::BudgetExceeded(_) => StatusCode::TOO_MANY_REQUESTS,
            Self::RequestCostCeilingExceeded { .. } => StatusCode::FORBIDDEN,
            // Fail-closed: the cap cannot be enforced, so the request is a
            // dependency failure rather than an over-cap caller (ADR 0010).
            Self::BudgetUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::RateLimitUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::RevocationUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::RateLimitExceeded { .. } => StatusCode::TOO_MANY_REQUESTS,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::TokenUnauthorized(_) => StatusCode::UNAUTHORIZED,
            Self::TokenForbidden(_) => StatusCode::FORBIDDEN,
            Self::ScopeInsufficient(_) => StatusCode::FORBIDDEN,
            Self::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::MintingDisabled => StatusCode::NOT_FOUND,
            Self::MintNotAuthorized
            | Self::MintClaimsNotNarrowing
            | Self::MintEpochNotUsable { .. } => StatusCode::FORBIDDEN,
            Self::UnsupportedWire { .. } => StatusCode::BAD_REQUEST,
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
            Self::RequestCostCeilingExceeded { .. } => "request_cost_ceiling_exceeded",
            Self::BudgetUnavailable => "budget_unavailable",
            Self::RateLimitUnavailable => "rate_limit_unavailable",
            Self::RevocationUnavailable => "revocation_unavailable",
            Self::RateLimitExceeded { .. } => "rate_limited",
            Self::Unauthorized => "unauthorized",
            Self::TokenUnauthorized(error) | Self::TokenForbidden(error) => error.code(),
            Self::ScopeInsufficient(_) => "token_scope_insufficient",
            Self::NotImplemented(_) => "not_implemented",
            Self::BadRequest(_) => "bad_request",
            Self::MintingDisabled => "minting_disabled",
            Self::MintNotAuthorized => "mint_not_authorized",
            Self::MintClaimsNotNarrowing => "mint_claims_not_narrowing",
            Self::MintEpochNotUsable { .. } => "mint_epoch_not_usable",
            Self::UnsupportedWire { .. } => "unsupported_wire",
            Self::Provider(e) => e.code(),
            Self::Transport(TransportError::Provider(e)) => e.code(),
            Self::Transport(TransportError::Http(_)) => "upstream_transport",
        }
    }
}

impl IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        let status = self.status();
        let code = self.code().to_owned();
        let retry_after = match &self {
            Self::RateLimitExceeded {
                retry_after_seconds: Some(seconds),
            } => Some(seconds.to_string()),
            _ => None,
        };
        let message = match &self {
            Self::TokenUnauthorized(_) => "token authentication failed".to_owned(),
            Self::TokenForbidden(_) => "token authorization failed".to_owned(),
            _ => self.to_string(),
        };
        let body = json!({
            "error": {
                "type": code,
                "message": message,
            }
        });
        let mut response = (status, Json(body)).into_response();
        if let Some(seconds) = retry_after
            && let Ok(value) = HeaderValue::from_str(&seconds)
        {
            response
                .headers_mut()
                .insert(axum::http::header::RETRY_AFTER, value);
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    #[test]
    fn token_error_statuses_and_codes_are_distinct() {
        let unauthorized = GatewayError::TokenUnauthorized(TokenVerificationError::Expired);
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(unauthorized.code(), "token_expired");

        let forbidden = GatewayError::TokenForbidden(TokenVerificationError::UnknownNamespace {
            namespace: "ghost".to_owned(),
        });
        assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);
        assert_eq!(forbidden.code(), "token_unknown_namespace");
    }

    #[test]
    fn request_cost_ceiling_and_budget_errors_are_distinct() {
        let ceiling = GatewayError::RequestCostCeilingExceeded {
            alias: "gpt-4o".to_owned(),
            estimated_microdollars: 11,
            ceiling_microdollars: 10,
        };
        assert_eq!(ceiling.status(), StatusCode::FORBIDDEN);
        assert_eq!(ceiling.code(), "request_cost_ceiling_exceeded");

        let budget = GatewayError::BudgetExceeded("gpt-4o".to_owned());
        assert_eq!(budget.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(budget.code(), "budget_exceeded");
        let unavailable = GatewayError::RateLimitUnavailable;
        assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(unavailable.code(), "rate_limit_unavailable");
    }

    #[tokio::test]
    async fn token_error_bodies_do_not_echo_caller_details() {
        let unauthorized = GatewayError::TokenUnauthorized(TokenVerificationError::UnknownKey {
            kid: "caller-kid".to_owned(),
        })
        .into_response();
        let unauthorized_body = unauthorized
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes();
        let unauthorized_body = String::from_utf8(unauthorized_body.to_vec()).unwrap();
        assert!(!unauthorized_body.contains("caller-kid"));

        let forbidden = GatewayError::TokenForbidden(TokenVerificationError::UnknownNamespace {
            namespace: "caller-namespace".to_owned(),
        })
        .into_response();
        let forbidden_body = forbidden
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes();
        let forbidden_body = String::from_utf8(forbidden_body.to_vec()).unwrap();
        assert!(!forbidden_body.contains("caller-namespace"));
    }

    #[tokio::test]
    async fn rate_limit_error_is_typed_429_without_retry_after() {
        let response = GatewayError::RateLimitExceeded {
            retry_after_seconds: None,
        }
        .into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(
            response
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .is_none()
        );
        let body = response
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            serde_json::json!({
                "error": {
                    "type": "rate_limited",
                    "message": "inbound concurrency limit exceeded"
                }
            })
        );
    }

    #[tokio::test]
    async fn scope_error_names_only_the_static_capability() {
        let response = GatewayError::ScopeInsufficient(Capability::Messages).into_response();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["type"], "token_scope_insufficient");
        assert_eq!(
            body["error"]["message"],
            "token scope does not authorize `messages`"
        );
    }
}
