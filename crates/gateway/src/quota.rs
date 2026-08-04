//! Quota enforcement — the read path.
//!
//! Deliberately a *separate* trait from [`crate::usage::UsageSink`] (§5.2):
//! quota is on the request path (must be fast and fresh), records are off it
//! (can be slow and batched). A Tinybird sink is fine; a Tinybird quota store
//! is not, because ingestion lag makes caps meaningless under burst.
//!
//! Token counts are unknown until a response completes, so enforcement is
//! **reserve-then-reconcile**: `reserve` a conservative estimate pre-dispatch,
//! `commit` the actual receipt after. Concurrent in-flight requests can
//! overshoot, so caps are *soft* unless a backend implements hard reservation
//! — stated honestly rather than implying precision.
//!
//! Default backend is in-memory (no datastore). Redis/Postgres backends for
//! cross-replica exact accounting are opt-in follow-ups.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

/// The dimension a quota is scoped to. Neutral vocabulary, like usage records.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QuotaKey {
    pub namespace: String,
    pub model: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    Allowed,
    Denied,
}

#[async_trait]
pub trait QuotaStore: Send + Sync {
    #[allow(dead_code)] // surfaced in logs/metrics once telemetry lands
    fn name(&self) -> &'static str;
    /// Pre-dispatch check + conservative reservation.
    async fn reserve(&self, key: &QuotaKey, estimated_tokens: u64) -> Admission;
    /// Post-response reconciliation with the actual token count.
    async fn commit(&self, key: &QuotaKey, actual_tokens: u64);
}

/// Always-allow. The default posture when no quota is configured.
pub struct NoQuota;

#[async_trait]
impl QuotaStore for NoQuota {
    fn name(&self) -> &'static str {
        "none"
    }
    async fn reserve(&self, _key: &QuotaKey, _estimated_tokens: u64) -> Admission {
        Admission::Allowed
    }
    async fn commit(&self, _key: &QuotaKey, _actual_tokens: u64) {}
}

/// Per-replica in-memory token counter. No datastore; not shared across
/// replicas (a fleet enforces per-replica ceilings — documented, not hidden).
/// Not yet wired into `main` (config-selected backend is a follow-up).
#[allow(dead_code)]
pub struct InMemoryQuota {
    limit_tokens: u64,
    used: Mutex<HashMap<QuotaKey, u64>>,
}

#[allow(dead_code)]
impl InMemoryQuota {
    pub fn new(limit_tokens: u64) -> Self {
        Self {
            limit_tokens,
            used: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl QuotaStore for InMemoryQuota {
    fn name(&self) -> &'static str {
        "in_memory"
    }

    async fn reserve(&self, key: &QuotaKey, estimated_tokens: u64) -> Admission {
        let mut used = self.used.lock().unwrap_or_else(|e| e.into_inner());
        let current = *used.get(key).unwrap_or(&0);
        if current.saturating_add(estimated_tokens) > self.limit_tokens {
            return Admission::Denied;
        }
        used.insert(key.clone(), current.saturating_add(estimated_tokens));
        Admission::Allowed
    }

    async fn commit(&self, key: &QuotaKey, actual_tokens: u64) {
        let mut used = self.used.lock().unwrap_or_else(|e| e.into_inner());
        let entry = used.entry(key.clone()).or_insert(0);
        *entry = actual_tokens.max(*entry);
    }
}
