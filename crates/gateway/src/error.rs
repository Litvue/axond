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

use crate::admission::AdmissionRejection;
use crate::principals::Capability;

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("model `{0}` is not prefixed as `provider-id/model-id`")]
    ModelUnprefixed(String),
    #[error("unknown provider `{0}`")]
    UnknownProvider(String),
    #[error("model `{0}` is blocked")]
    ModelBlocked(String),
    #[error("model `{0}` has no price")]
    UnpricedModel(String),
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
    /// A billing-grade deployment could not journal the request's usage event
    /// and is configured to refuse rather than serve a request it cannot bill
    /// for. The upstream work is already done; what is refused is the
    /// *acknowledgement* of a request whose spend would otherwise go unrecorded.
    #[error("usage could not be recorded durably: {reason}")]
    UsageNotDurable { reason: &'static str },
    #[error("continuation affinity unavailable for Responses target `{provider}/{model}`")]
    ContinuationAffinityUnavailable { provider: String, model: String },
    /// Load shed by admission control: the process, the tenant, or the stream
    /// ceiling is full (see [`crate::admission`]). Typed per ceiling so an
    /// operator can tell a saturated replica from one noisy tenant.
    #[error(transparent)]
    Overloaded(#[from] AdmissionRejection),
    /// The inbound body exceeded `admission.max_request_bytes`, or request
    /// middleware expanded a valid inbound body beyond the same ceiling. Wire
    /// oversize is refused before buffering; middleware growth is refused
    /// before provider dispatch.
    #[error("request body exceeds the configured inbound limit")]
    RequestTooLarge,
    /// The request did not declare a JSON content type. Axum's own extractor
    /// answered `415` before the gateway mapped its rejections, and that status
    /// is preserved: a wrong media type is not a malformed body.
    #[error("expected a `content-type: application/json` request")]
    UnsupportedMediaType,
    /// The prompt's estimated token count exceeded
    /// `admission.max_prompt_tokens`. Reports the bound, never the prompt.
    #[error("prompt exceeds the configured limit of {limit_tokens} tokens")]
    PromptTooLarge { limit_tokens: u64 },
    /// The request asked for a larger output allowance than
    /// `admission.max_output_tokens`. Refused rather than clamped.
    #[error(
        "requested output of {requested_tokens} tokens exceeds the configured limit of {limit_tokens} tokens"
    )]
    OutputLimitExceeded {
        requested_tokens: u64,
        limit_tokens: u64,
    },
    #[error("the gateway is shutting down and is no longer accepting requests")]
    Draining,
    #[error("unauthorized")]
    Unauthorized,
    #[error("token scope does not authorize `{0}`")]
    ScopeInsufficient(Capability),
    #[error("namespace identifier is invalid")]
    InvalidNamespace,
    #[error("unknown namespace")]
    UnknownNamespace,
    #[error("unknown budget")]
    UnknownBudget,
    #[error("namespace already exists")]
    NamespaceConflict,
    #[error("store is unavailable")]
    StoreUnavailable,
    #[error("the authenticated grant does not authorize the selected namespace")]
    NamespaceNotAuthorized,
    #[error(transparent)]
    Provider(#[from] ProviderError),
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error("bad request: {0}")]
    BadRequest(String),
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
            Self::ModelUnprefixed(_)
            | Self::UnknownProvider(_)
            | Self::ModelBlocked(_)
            | Self::UnpricedModel(_) => StatusCode::BAD_REQUEST,
            Self::NoCredential { .. } => StatusCode::BAD_GATEWAY,
            Self::BudgetExceeded(_) => StatusCode::TOO_MANY_REQUESTS,
            // The configuration is servable and the caller's request is
            // well-formed; what is missing is an operator's approval, so this is
            // the deployment's state rather than the caller's fault.
            Self::RequestCostCeilingExceeded { .. } => StatusCode::FORBIDDEN,
            // Fail-closed: the cap cannot be enforced, so the request is a
            // dependency failure rather than an over-cap caller (ADR 0010).
            Self::BudgetUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            // Same fail-closed reasoning as the budget store, applied to the
            // usage outbox: billing-grade delivery promised the event is durable
            // before the response is acknowledged, and it is not.
            Self::UsageNotDurable { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Self::ContinuationAffinityUnavailable { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Self::Overloaded(rejection) => {
                if rejection.is_caller_limit() {
                    StatusCode::TOO_MANY_REQUESTS
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            }
            Self::RequestTooLarge | Self::PromptTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            Self::UnsupportedMediaType => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Self::OutputLimitExceeded { .. } => StatusCode::BAD_REQUEST,
            // A policy guardrail denied an authenticated request rather than
            // finding its shape malformed. Keep invalid-request refusals at 400,
            // but expose policy denial with the ordinary authorization status.
            // Retryable elsewhere immediately: this replica is leaving, not
            // failing, and readiness has already said so.
            Self::Draining => StatusCode::SERVICE_UNAVAILABLE,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::ScopeInsufficient(_) => StatusCode::FORBIDDEN,
            Self::InvalidNamespace => StatusCode::BAD_REQUEST,
            Self::UnknownNamespace => StatusCode::NOT_FOUND,
            Self::UnknownBudget => StatusCode::NOT_FOUND,
            Self::NamespaceConflict => StatusCode::CONFLICT,
            Self::StoreUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::NamespaceNotAuthorized => StatusCode::FORBIDDEN,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::UnsupportedWire { .. } => StatusCode::BAD_REQUEST,
            // Provider credentials belong to the operator. The provider parser
            // groups these refusals with invalid requests, but their original
            // HTTP status distinguishes them from caller validation failures.
            Self::Transport(TransportError::Upstream {
                status: 401 | 403, ..
            }) => StatusCode::BAD_GATEWAY,
            Self::Provider(e)
            | Self::Transport(
                TransportError::Provider(e) | TransportError::Upstream { error: e, .. },
            ) => match e {
                ProviderError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
                ProviderError::ContextWindowExceeded(_) => StatusCode::BAD_REQUEST,
                ProviderError::Unsupported(_) => StatusCode::NOT_IMPLEMENTED,
                ProviderError::ModelUnavailable(_) => StatusCode::BAD_GATEWAY,
                ProviderError::Dependency(_) => StatusCode::BAD_GATEWAY,
                ProviderError::InvalidStream(_) => StatusCode::BAD_GATEWAY,
                // Stream decoder rate limits arrive after a 200 response and
                // are relayed in-band; stream-open 429s are Dependency errors.
                // This arm is therefore not an HTTP response path today.
                ProviderError::RateLimitedStream(_) => StatusCode::BAD_GATEWAY,
                ProviderError::AllCircuitsOpen(_) => StatusCode::SERVICE_UNAVAILABLE,
            },
            Self::Transport(TransportError::Http(_)) => StatusCode::BAD_GATEWAY,
            // A bound the gateway itself imposed, not a provider verdict: the
            // upstream never answered in time, which is what 504 means.
            Self::Transport(TransportError::Timeout { .. }) => StatusCode::GATEWAY_TIMEOUT,
            Self::Transport(TransportError::BodyTooLarge { .. }) => StatusCode::BAD_GATEWAY,
        }
    }

    fn code(&self) -> &str {
        match self {
            Self::ModelUnprefixed(_) => "model_unprefixed",
            Self::UnknownProvider(_) => "unknown_provider",
            Self::ModelBlocked(_) => "model_blocked",
            Self::UnpricedModel(_) => "unpriced_model",
            Self::NoCredential { .. } => "no_credential",
            Self::BudgetExceeded(_) => "budget_exceeded",
            Self::RequestCostCeilingExceeded { .. } => "request_cost_ceiling_exceeded",
            Self::BudgetUnavailable => "budget_unavailable",
            Self::UsageNotDurable { .. } => "usage_not_durable",
            Self::ContinuationAffinityUnavailable { .. } => "continuation_affinity_unavailable",
            Self::Overloaded(rejection) => rejection.code(),
            Self::RequestTooLarge => "request_too_large",
            Self::UnsupportedMediaType => "unsupported_media_type",
            Self::PromptTooLarge { .. } => "prompt_too_large",
            Self::OutputLimitExceeded { .. } => "output_limit_exceeded",
            Self::Draining => "draining",
            Self::Unauthorized => "unauthorized",
            Self::ScopeInsufficient(_) => "token_scope_insufficient",
            Self::InvalidNamespace => "invalid_namespace",
            Self::UnknownNamespace => "unknown_namespace",
            Self::UnknownBudget => "unknown_budget",
            Self::NamespaceConflict => "namespace_conflict",
            Self::StoreUnavailable => "store_unavailable",
            Self::NamespaceNotAuthorized => "namespace_not_authorized",
            Self::BadRequest(_) => "bad_request",
            Self::UnsupportedWire { .. } => "unsupported_wire",
            Self::Provider(e) => e.code(),
            Self::Transport(
                TransportError::Provider(e) | TransportError::Upstream { error: e, .. },
            ) => e.code(),
            Self::Transport(TransportError::Http(_)) => "upstream_transport",
            // One code for every phase: the phase is in the message and on the
            // attempt span, so callers get a stable type to match on.
            Self::Transport(TransportError::Timeout { .. }) => "upstream_timeout",
            Self::Transport(TransportError::BodyTooLarge { .. }) => "upstream_body_too_large",
        }
    }
}

/// What a caller is told about a transport failure, on the buffered path and
/// in a stream's in-band terminal event alike.
///
/// `reqwest` renders the endpoint it failed against into its message, and
/// `redact_url` only takes that URL's credential-bearing parts off. The
/// endpoint itself belongs in the operator's logs and on the attempt span,
/// where the full `Display` still goes — not in the caller's answer, which
/// would name a provider host, port, and path the caller never chose. Every
/// other transport failure names its phase and no endpoint (ADR 0028), so it is
/// relayed as it stands.
pub fn transport_caller_message(error: &TransportError) -> String {
    match error {
        TransportError::Http(_) => "upstream transport failure".to_owned(),
        other => other.to_string(),
    }
}

impl IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        let status = self.status();
        let code = self.code().to_owned();
        let retry_after = match &self {
            Self::Overloaded(rejection) => rejection
                .retry_after_seconds()
                .map(|seconds| seconds.to_string()),
            // A rolling deployment replaces the replica, so "try again" is a
            // matter of routing rather than of waiting.
            Self::Draining => Some("0".to_owned()),
            _ => None,
        };
        let message = match &self {
            Self::Transport(error) => transport_caller_message(error),
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
    }

    #[test]
    fn draining_is_a_typed_retryable_unavailable() {
        let draining = GatewayError::Draining;
        assert_eq!(draining.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(draining.code(), "draining");
        let response = draining.into_response();
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("0")
        );
    }

    #[tokio::test]
    async fn tenant_saturation_is_429_and_process_saturation_is_503() {
        let tenant = GatewayError::Overloaded(AdmissionRejection::Tenant);
        assert_eq!(tenant.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(tenant.code(), "tenant_concurrency_exceeded");

        let global = GatewayError::Overloaded(AdmissionRejection::Global).into_response();
        assert_eq!(global.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            global
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .map(|value| value.to_str().expect("ascii").to_owned()),
            Some("1".to_owned())
        );
        let body = global
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["type"], "gateway_overloaded");

        // Tenant-table capacity frees when some other tenant goes idle, which
        // this replica cannot predict, so it advertises no retry window.
        let capacity = GatewayError::Overloaded(AdmissionRejection::TenantCapacity).into_response();
        assert_eq!(capacity.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            capacity
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .is_none()
        );
    }

    #[tokio::test]
    async fn an_oversized_request_is_typed_413_without_echoing_the_body() {
        let response = GatewayError::RequestTooLarge.into_response();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["type"], "request_too_large");
        assert_eq!(
            body["error"]["message"],
            "request body exceeds the configured inbound limit"
        );
    }

    #[tokio::test]
    async fn a_transport_failure_does_not_name_the_endpoint_it_failed_against() {
        let error = GatewayError::Transport(TransportError::Http(
            "error sending request for url (http://provider.internal:9443/v1/chat/completions)"
                .to_owned(),
        ));
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["type"], "upstream_transport");
        assert_eq!(body["error"]["message"], "upstream transport failure");
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
