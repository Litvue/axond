use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencyFailure {
    pub provider: String,
    pub status: Option<u16>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProviderError {
    #[error("invalid provider request: {0}")]
    InvalidRequest(String),
    #[error("context window exceeded: {0}")]
    ContextWindowExceeded(String),
    #[error("surface unsupported by provider: {0}")]
    Unsupported(String),
    #[error("upstream model unavailable")]
    ModelUnavailable(Vec<DependencyFailure>),
    #[error("provider dependency failed")]
    Dependency(Vec<DependencyFailure>),
    #[error("provider stream was invalid: {0}")]
    InvalidStream(String),
    #[error("all provider circuits are open")]
    AllCircuitsOpen(Vec<String>),
}

impl ProviderError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidRequest(_) => "invalid_request",
            Self::ContextWindowExceeded(_) => "context_window_exceeded",
            Self::Unsupported(_) => "unsupported",
            Self::ModelUnavailable(_) => "model_unavailable",
            Self::Dependency(_) => "provider_dependency_failed",
            Self::InvalidStream(_) => "invalid_stream",
            Self::AllCircuitsOpen(_) => "all_provider_circuits_open",
        }
    }

    pub fn from_upstream(provider: impl Into<String>, status: u16, body: &str) -> Self {
        let provider = provider.into();
        let message = extract_message(body);
        if is_context_length_error(body) || is_context_length_error(&message) {
            return Self::ContextWindowExceeded(message);
        }
        let failure = DependencyFailure {
            provider,
            status: Some(status),
            message,
        };
        if status == 404 {
            Self::ModelUnavailable(vec![failure])
        } else if (400..500).contains(&status) && status != 429 {
            Self::InvalidRequest(failure.message)
        } else {
            Self::Dependency(vec![failure])
        }
    }

    pub fn transport(provider: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Dependency(vec![DependencyFailure {
            provider: provider.into(),
            status: None,
            message: message.into(),
        }])
    }

    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Dependency(failures) => {
                !failures.is_empty()
                    && failures.iter().all(|failure| {
                        failure
                            .status
                            .is_none_or(|status| status == 429 || status >= 500)
                    })
            }
            Self::ModelUnavailable(_) => true,
            _ => false,
        }
    }

    pub fn affects_provider_health(&self) -> bool {
        matches!(self, Self::Dependency(_)) && self.is_retryable()
    }
}

fn extract_message(body: &str) -> String {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|value| {
            value
                .pointer("/error/message")
                .or_else(|| value.get("message"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| body.to_owned())
}

fn is_context_length_error(text: &str) -> bool {
    let text = text.to_ascii_lowercase();
    [
        "context_length_exceeded",
        "context length",
        "context window",
        "prompt is too long",
        "prompt too long",
        "maximum number of tokens",
        "too many tokens",
        "maximum prompt length",
    ]
    .iter()
    .any(|signal| text.contains(signal))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_context_limit_signals_without_retrying_or_degrading_health() {
        for body in [
            r#"{"error":{"code":"context_length_exceeded","message":"too long"}}"#,
            r#"{"error":{"message":"prompt is too long: 250000 tokens"}}"#,
            r#"{"message":"input exceeds the maximum number of tokens"}"#,
        ] {
            let error = ProviderError::from_upstream("provider", 400, body);
            assert!(matches!(error, ProviderError::ContextWindowExceeded(_)));
            assert!(!error.is_retryable());
            assert!(!error.affects_provider_health());
        }
    }

    #[test]
    fn rate_limits_and_server_failures_retry_and_affect_health() {
        for status in [429, 500, 502, 503, 599] {
            let error = ProviderError::from_upstream("provider", status, "upstream unavailable");
            assert!(matches!(error, ProviderError::Dependency(_)));
            assert!(error.is_retryable(), "status {status}");
            assert!(error.affects_provider_health(), "status {status}");
        }
        let transport = ProviderError::transport("provider", "timeout");
        assert!(transport.is_retryable());
        assert!(transport.affects_provider_health());
    }

    #[test]
    fn authentication_and_other_client_failures_are_permanent_but_not_unhealthy() {
        for status in [400, 401, 403, 422] {
            let error = ProviderError::from_upstream("provider", status, "invalid request");
            assert!(matches!(error, ProviderError::InvalidRequest(_)));
            assert!(!error.is_retryable(), "status {status}");
            assert!(!error.affects_provider_health(), "status {status}");
        }
    }

    #[test]
    fn missing_model_fails_over_without_marking_provider_unhealthy() {
        let error = ProviderError::from_upstream("foundry", 404, "missing deployment");
        assert!(matches!(error, ProviderError::ModelUnavailable(_)));
        assert!(error.is_retryable());
        assert!(!error.affects_provider_health());
    }
}
