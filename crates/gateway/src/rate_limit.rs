//! Inbound concurrency limiting.
//!
//! Rate limiting is deliberately a separate request-path trait from
//! [`crate::budget::BudgetStore`]. [`NoLimit`] is the Tier 0 default and
//! carries no state or datastore dependency. [`InMemoryRateLimiter`] is
//! bounded and per-replica: with N replicas sharing a nominal limit, each
//! replica admits approximately `limit / N`, rather than enforcing a
//! fleet-wide ceiling.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::config::RateLimitConfig;
use crate::telemetry::metrics;

/// The authenticated caller dimension used by inbound limits.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RateLimitKey {
    pub namespace: String,
    pub subject: String,
}

/// Why a request could not acquire an in-flight permit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RateLimitError {
    #[error("in-flight concurrency limit exceeded")]
    Exceeded,
    #[error("in-memory rate-limit subject capacity exhausted")]
    SubjectCapacityExceeded,
}

/// An owned permit released synchronously when dropped.
pub struct RateLimitPermit {
    release: Option<PermitRelease>,
}

enum PermitRelease {
    NoLimit,
    InMemory {
        state: Arc<InMemoryState>,
        key: RateLimitKey,
    },
}

impl RateLimitPermit {
    fn no_limit() -> Self {
        Self {
            release: Some(PermitRelease::NoLimit),
        }
    }

    fn in_memory(state: Arc<InMemoryState>, key: RateLimitKey) -> Self {
        Self {
            release: Some(PermitRelease::InMemory { state, key }),
        }
    }
}

impl Drop for RateLimitPermit {
    fn drop(&mut self) {
        let Some(PermitRelease::InMemory { state, key }) = self.release.take() else {
            return;
        };
        let mut subjects = state
            .subjects
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(count) = subjects.get_mut(&key) {
            *count -= 1;
            if *count == 0 {
                subjects.remove(&key);
            }
        }
    }
}

#[async_trait]
pub trait RateLimiter: Send + Sync {
    fn name(&self) -> &'static str;
    async fn acquire(&self, key: &RateLimitKey) -> Result<RateLimitPermit, RateLimitError>;
}

/// Always-allow. The default posture when no inbound limit is configured.
pub struct NoLimit;

#[async_trait]
impl RateLimiter for NoLimit {
    fn name(&self) -> &'static str {
        "none"
    }

    async fn acquire(&self, _key: &RateLimitKey) -> Result<RateLimitPermit, RateLimitError> {
        Ok(RateLimitPermit::no_limit())
    }
}

/// Per-replica bounded in-flight concurrency limiter.
pub struct InMemoryRateLimiter {
    limit: usize,
    max_subjects: usize,
    state: Arc<InMemoryState>,
}

struct InMemoryState {
    subjects: Mutex<HashMap<RateLimitKey, usize>>,
}

impl InMemoryRateLimiter {
    pub fn new(limit: usize, max_subjects: usize) -> Self {
        Self {
            limit,
            max_subjects,
            state: Arc::new(InMemoryState {
                subjects: Mutex::new(HashMap::new()),
            }),
        }
    }
}

#[async_trait]
impl RateLimiter for InMemoryRateLimiter {
    fn name(&self) -> &'static str {
        "in-memory"
    }

    async fn acquire(&self, key: &RateLimitKey) -> Result<RateLimitPermit, RateLimitError> {
        let mut subjects = self
            .state
            .subjects
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let count = match subjects.get_mut(key) {
            Some(count) => count,
            None => {
                if subjects.len() >= self.max_subjects {
                    metrics::record_rate_limit_capacity_denial();
                    return Err(RateLimitError::SubjectCapacityExceeded);
                }
                subjects.entry(key.clone()).or_insert(0)
            }
        };
        if *count >= self.limit {
            metrics::record_rate_limit_denial();
            return Err(RateLimitError::Exceeded);
        }
        *count += 1;
        Ok(RateLimitPermit::in_memory(
            Arc::clone(&self.state),
            key.clone(),
        ))
    }
}

pub fn build(config: &RateLimitConfig) -> Box<dyn RateLimiter> {
    match config.backend {
        crate::config::RateLimitBackend::None => Box::new(NoLimit),
        crate::config::RateLimitBackend::InMemory => Box::new(InMemoryRateLimiter::new(
            config.max_in_flight_per_subject,
            config.max_subjects,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(namespace: &str, subject: &str) -> RateLimitKey {
        RateLimitKey {
            namespace: namespace.to_owned(),
            subject: subject.to_owned(),
        }
    }

    #[tokio::test]
    async fn no_limit_always_admits() {
        let limiter = NoLimit;
        for _ in 0..100 {
            let permit = limiter.acquire(&key("n", "s")).await.expect("admit");
            drop(permit);
        }
    }

    #[tokio::test]
    async fn same_key_saturates_and_drop_releases_once() {
        let limiter = InMemoryRateLimiter::new(1, 10);
        let first = limiter.acquire(&key("n", "s")).await.expect("admit");
        assert!(matches!(
            limiter.acquire(&key("n", "s")).await,
            Err(RateLimitError::Exceeded)
        ));
        drop(first);
        let second = limiter.acquire(&key("n", "s")).await.expect("released");
        drop(second);
    }

    #[tokio::test]
    async fn namespace_is_part_of_key() {
        let limiter = InMemoryRateLimiter::new(1, 10);
        let first = limiter.acquire(&key("one", "same")).await.expect("admit");
        let second = limiter.acquire(&key("two", "same")).await.expect("admit");
        drop((first, second));
    }

    #[tokio::test]
    async fn concurrent_acquisition_is_bounded() {
        let limiter = Arc::new(InMemoryRateLimiter::new(2, 10));
        let mut tasks = Vec::new();
        for _ in 0..3 {
            let limiter = Arc::clone(&limiter);
            tasks.push(tokio::spawn(async move {
                limiter.acquire(&key("n", "s")).await
            }));
        }
        let mut admitted = 0;
        let mut denied = 0;
        for task in tasks {
            match task.await.expect("task") {
                Ok(permit) => {
                    admitted += 1;
                    drop(permit);
                }
                Err(RateLimitError::Exceeded) => denied += 1,
                Err(RateLimitError::SubjectCapacityExceeded) => unreachable!(),
            }
        }
        assert_eq!((admitted, denied), (2, 1));
    }
}
