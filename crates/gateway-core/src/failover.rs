use crate::ProviderError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailoverTarget {
    pub provider: String,
    pub model: String,
}

impl FailoverTarget {
    pub fn new(provider: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            model: model.into(),
        }
    }

    pub fn qualified_model(&self) -> String {
        format!("{}/{}", self.provider, self.model)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverDecision {
    TryNext,
    Return,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct FailoverPolicy;

impl FailoverPolicy {
    pub fn decide(&self, error: &ProviderError, has_next: bool) -> FailoverDecision {
        if has_next && error.is_retryable() {
            FailoverDecision::TryNext
        } else {
            FailoverDecision::Return
        }
    }

    pub fn ordered_targets(
        &self,
        targets: impl IntoIterator<Item = FailoverTarget>,
    ) -> Result<Vec<FailoverTarget>, ProviderError> {
        let targets: Vec<_> = targets.into_iter().collect();
        if targets.is_empty() {
            Err(ProviderError::InvalidRequest(
                "at least one provider target is required".into(),
            ))
        } else {
            Ok(targets)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retries_only_retryable_errors_with_remaining_target() {
        let policy = FailoverPolicy;
        assert_eq!(
            policy.decide(&ProviderError::transport("openai", "timeout"), true),
            FailoverDecision::TryNext
        );
        assert_eq!(
            policy.decide(&ProviderError::InvalidRequest("bad".into()), true),
            FailoverDecision::Return
        );
        assert_eq!(
            policy.decide(&ProviderError::transport("openai", "timeout"), false),
            FailoverDecision::Return
        );
    }
}
