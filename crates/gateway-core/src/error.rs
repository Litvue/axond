use serde::{Deserialize, Serialize};

/// The longest upstream diagnostic a [`ProviderError`] carries, before the
/// truncation marker.
///
/// An upstream failure body is attacker-influenced and arrives over the
/// network, so the diagnostic built from it is bounded *here* rather than only
/// at the edge that read it: a provider error is logged, counted, and rendered
/// into a response, and every one of those is a place an unbounded body would
/// end up. The transport already truncates what it reads (`max_error_bytes`,
/// 64 KiB), which makes this the second bound rather than the only one — and
/// the one that holds for any caller, including a future non-HTTP one.
pub const MAX_DIAGNOSTIC_BYTES: usize = 4 * 1024;

/// Appended to a diagnostic that hit [`MAX_DIAGNOSTIC_BYTES`], so a reader can
/// tell a truncated message from a short one.
pub const DIAGNOSTIC_TRUNCATION_MARKER: &str = "… [truncated]";

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
    #[error("provider stream was rate limited: {0}")]
    RateLimitedStream(String),
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
            Self::RateLimitedStream(_) => "provider_rate_limited",
            Self::AllCircuitsOpen(_) => "all_provider_circuits_open",
        }
    }

    pub fn from_upstream(provider: impl Into<String>, status: u16, body: &str) -> Self {
        let provider = provider.into();
        let message = bounded(extract_message(body));
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

    /// A malformed provider stream, with its diagnostic bounded: the message
    /// comes from the provider's own payload, so it is untrusted input the same
    /// way a failure body is.
    pub fn invalid_stream(message: impl Into<String>) -> Self {
        Self::InvalidStream(bounded(message.into()))
    }

    /// A rate-limited provider stream, with its diagnostic bounded for the same
    /// reason as [`Self::invalid_stream`].
    pub fn rate_limited_stream(message: impl Into<String>) -> Self {
        Self::RateLimitedStream(bounded(message.into()))
    }

    pub fn transport(provider: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Dependency(vec![DependencyFailure {
            provider: provider.into(),
            status: None,
            message: bounded(message.into()),
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

    pub fn is_stream_rate_limited(&self) -> bool {
        matches!(self, Self::RateLimitedStream(_))
    }

    pub fn is_credential_rate_limited(&self) -> bool {
        match self {
            Self::RateLimitedStream(_) => true,
            Self::Dependency(failures) => {
                failures.iter().any(|failure| failure.status == Some(429))
            }
            _ => false,
        }
    }
}

/// Cut a diagnostic down to [`MAX_DIAGNOSTIC_BYTES`] on a character boundary.
///
/// The cut is by bytes rather than characters because what is being bounded is
/// the memory and the log line, not the glyph count.
fn bounded(mut message: String) -> String {
    if message.len() <= MAX_DIAGNOSTIC_BYTES {
        return message;
    }
    let mut cut = MAX_DIAGNOSTIC_BYTES;
    while !message.is_char_boundary(cut) {
        cut -= 1;
    }
    message.truncate(cut);
    message.push_str(DIAGNOSTIC_TRUNCATION_MARKER);
    message
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

/// Recognize only explicit provider rate-limit markers in an SSE JSON payload.
pub fn is_rate_limit_payload(value: &serde_json::Value) -> bool {
    let error = value.get("error");
    let error_shaped =
        error.is_some() || value.get("type").and_then(serde_json::Value::as_str) == Some("error");
    if !error_shaped {
        return false;
    }
    let status_is_429 = [value.get("status"), value.pointer("/error/status")]
        .into_iter()
        .flatten()
        .any(|status| {
            status.as_u64() == Some(429) || status.as_str().is_some_and(|status| status == "429")
        });
    if status_is_429 {
        return true;
    }
    [
        value
            .pointer("/error/type")
            .and_then(serde_json::Value::as_str),
        value.pointer("/error/code").and_then(|code| {
            code.as_str()
                .or_else(|| (code.as_u64() == Some(429)).then_some("429"))
        }),
        value.pointer("/type").and_then(serde_json::Value::as_str),
        value.pointer("/code").and_then(serde_json::Value::as_str),
    ]
    .into_iter()
    .flatten()
    .any(|signal| signal.contains("rate_limit") || signal == "429")
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

    /// A provider that answers a failure with megabytes of HTML must not put
    /// megabytes into a log line, a metric label, or a response body.
    #[test]
    fn upstream_diagnostics_are_bounded_however_large_the_body_is() {
        let body = "x".repeat(4 * MAX_DIAGNOSTIC_BYTES);
        for (status, message) in [
            (
                400,
                diagnostic(&ProviderError::from_upstream("p", 400, &body)),
            ),
            (
                404,
                diagnostic(&ProviderError::from_upstream("p", 404, &body)),
            ),
            (
                503,
                diagnostic(&ProviderError::from_upstream("p", 503, &body)),
            ),
        ] {
            assert!(
                message.len() <= MAX_DIAGNOSTIC_BYTES + DIAGNOSTIC_TRUNCATION_MARKER.len(),
                "status {status} carried a {}-byte diagnostic",
                message.len()
            );
            assert!(
                message.ends_with(DIAGNOSTIC_TRUNCATION_MARKER),
                "status {status}"
            );
        }
        // The JSON path is bounded too: the message a provider nests is as
        // attacker-influenced as the body around it.
        let nested = format!(r#"{{"error":{{"message":"{}"}}}}"#, "y".repeat(64 * 1024));
        let error = ProviderError::from_upstream("p", 500, &nested);
        assert!(
            diagnostic(&error).len() <= MAX_DIAGNOSTIC_BYTES + DIAGNOSTIC_TRUNCATION_MARKER.len()
        );
        // So is a transport diagnostic, which is built from an error string
        // rather than a body but reaches the same places.
        let transport = ProviderError::transport("p", "z".repeat(1024 * 1024));
        assert!(
            diagnostic(&transport).len()
                <= MAX_DIAGNOSTIC_BYTES + DIAGNOSTIC_TRUNCATION_MARKER.len()
        );
    }

    /// Truncation cuts bytes, so it has to land on a character boundary: a
    /// multi-byte character straddling the bound would panic the truncation.
    #[test]
    fn truncation_never_splits_a_character() {
        // Three bytes each, so the 4096-byte bound falls mid-character.
        let body = "€".repeat(MAX_DIAGNOSTIC_BYTES);
        let message = diagnostic(&ProviderError::from_upstream("p", 500, &body));
        assert!(message.len() <= MAX_DIAGNOSTIC_BYTES + DIAGNOSTIC_TRUNCATION_MARKER.len());
        assert!(
            message
                .trim_end_matches(DIAGNOSTIC_TRUNCATION_MARKER)
                .chars()
                .all(|character| character == '€')
        );
    }

    /// A body short enough to keep is kept whole: bounding must not silently
    /// mangle the diagnostics operators actually read.
    #[test]
    fn short_diagnostics_are_untouched() {
        let error = ProviderError::from_upstream("p", 500, "upstream unavailable");
        assert_eq!(diagnostic(&error), "upstream unavailable");
    }

    fn diagnostic(error: &ProviderError) -> String {
        match error {
            ProviderError::InvalidRequest(message)
            | ProviderError::ContextWindowExceeded(message)
            | ProviderError::Unsupported(message)
            | ProviderError::InvalidStream(message)
            | ProviderError::RateLimitedStream(message) => message.clone(),
            ProviderError::ModelUnavailable(failures) | ProviderError::Dependency(failures) => {
                failures
                    .iter()
                    .map(|failure| failure.message.clone())
                    .collect::<Vec<_>>()
                    .join("\n")
            }
            ProviderError::AllCircuitsOpen(providers) => providers.join("\n"),
        }
    }

    #[test]
    fn recognizes_only_explicit_stream_rate_limit_shapes() {
        for body in [
            r#"{"type":"error","error":{"type":"rate_limit_error"}}"#,
            r#"{"error":{"code":"rate_limit_exceeded"}}"#,
            r#"{"error":{"code":429}}"#,
            r#"{"error":{"status":429}}"#,
        ] {
            let value: serde_json::Value = serde_json::from_str(body).unwrap();
            assert!(is_rate_limit_payload(&value), "{body}");
        }
        for body in [
            r#"{"error":{"type":"overloaded_error"}}"#,
            r#"{"error":{"message":"try again later"}}"#,
            r#"{"status":500}"#,
            r#"{"type":"rate_limits.updated","rate_limits":{"requests":10}}"#,
        ] {
            let value: serde_json::Value = serde_json::from_str(body).unwrap();
            assert!(!is_rate_limit_payload(&value), "{body}");
        }
    }
}
