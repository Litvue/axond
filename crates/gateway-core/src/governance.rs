use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GovernanceKey {
    pub scope_id: String,
    pub principal_id: String,
    pub model: String,
}

impl GovernanceKey {
    pub fn new(
        scope_id: impl Into<String>,
        principal_id: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            scope_id: scope_id.into(),
            principal_id: principal_id.into(),
            model: model.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GovernanceLimits {
    pub requests_per_window: u32,
    pub tokens_per_window: u64,
    pub window: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    Allowed,
    RequestLimited { retry_after: Duration },
    TokenLimited { retry_after: Duration },
}

#[derive(Debug, Clone, Copy)]
struct Window {
    started: Instant,
    requests: u32,
    tokens: u64,
}

pub struct Governance {
    limits: GovernanceLimits,
    windows: Mutex<HashMap<GovernanceKey, Window>>,
}

impl Governance {
    pub fn new(limits: GovernanceLimits) -> Self {
        Self {
            limits,
            windows: Mutex::new(HashMap::new()),
        }
    }

    pub fn admit(&self, key: &GovernanceKey) -> Admission {
        self.admit_at(key, Instant::now())
    }

    pub fn record_usage(&self, key: &GovernanceKey, tokens: u64) {
        self.record_usage_at(key, tokens, Instant::now());
    }

    pub fn clear(&self, key: &GovernanceKey) {
        self.lock().remove(key);
    }

    fn admit_at(&self, key: &GovernanceKey, now: Instant) -> Admission {
        let mut windows = self.lock();
        let window = window(&mut windows, key, now, self.limits.window);
        let retry_after = self
            .limits
            .window
            .saturating_sub(now.saturating_duration_since(window.started));
        if window.tokens >= self.limits.tokens_per_window {
            return Admission::TokenLimited { retry_after };
        }
        if window.requests >= self.limits.requests_per_window {
            return Admission::RequestLimited { retry_after };
        }
        window.requests = window.requests.saturating_add(1);
        Admission::Allowed
    }

    fn record_usage_at(&self, key: &GovernanceKey, tokens: u64, now: Instant) {
        let mut windows = self.lock();
        let window = window(&mut windows, key, now, self.limits.window);
        window.tokens = window.tokens.saturating_add(tokens);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<GovernanceKey, Window>> {
        self.windows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn window<'a>(
    windows: &'a mut HashMap<GovernanceKey, Window>,
    key: &GovernanceKey,
    now: Instant,
    duration: Duration,
) -> &'a mut Window {
    let current = windows.entry(key.clone()).or_insert(Window {
        started: now,
        requests: 0,
        tokens: 0,
    });
    if now.saturating_duration_since(current.started) >= duration {
        *current = Window {
            started: now,
            requests: 0,
            tokens: 0,
        };
    }
    current
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enforces_requests_and_tokens_per_key() {
        let governance = Governance::new(GovernanceLimits {
            requests_per_window: 1,
            tokens_per_window: 10,
            window: Duration::from_secs(60),
        });
        let key = GovernanceKey::new("scope-a", "principal-a", "openai/model");
        assert_eq!(governance.admit(&key), Admission::Allowed);
        assert!(matches!(
            governance.admit(&key),
            Admission::RequestLimited { .. }
        ));
        governance.clear(&key);
        governance.record_usage(&key, 10);
        assert!(matches!(
            governance.admit(&key),
            Admission::TokenLimited { .. }
        ));
    }
}
