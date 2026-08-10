//! Exact cross-replica in-flight concurrency leases in Redis.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use redis::Script;
use redis::aio::ConnectionManager;

use super::{PermitRelease, RateLimitError, RateLimitKey, RateLimitPermit, RateLimiter};
use crate::config::StoreUnavailable;
use crate::telemetry::metrics;

const ACQUIRE: &str = r#"
local now = tonumber(ARGV[1])
local ttl_ms = tonumber(ARGV[2])
local max = tonumber(ARGV[3])
local lease_id = ARGV[4]
local leases = redis.call('HGETALL', KEYS[1])
local live = 0
for i = 1, #leases, 2 do
  local expires_at = tonumber(leases[i + 1])
  if expires_at <= now then
    redis.call('HDEL', KEYS[1], leases[i])
  else
    live = live + 1
  end
end
if live >= max then
  return 0
end
redis.call('HSET', KEYS[1], lease_id, now + ttl_ms)
redis.call('PEXPIRE', KEYS[1], ttl_ms * 2)
return 1
"#;

const RELEASE: &str = "redis.call('HDEL', KEYS[1], ARGV[1]); return 1";

pub(crate) struct RedisRelease {
    connection: ConnectionManager,
    script: Script,
    key: String,
    lease_id: String,
    timeout: Duration,
}

impl RedisRelease {
    pub(crate) fn spawn(self) {
        let Some(handle) = tokio::runtime::Handle::try_current().ok() else {
            tracing::warn!("rate-limit permit dropped without a Tokio runtime; lease will expire");
            return;
        };
        handle.spawn(async move {
            let mut connection = self.connection;
            let result = tokio::time::timeout(self.timeout, async {
                self.script
                    .prepare_invoke()
                    .key(self.key)
                    .arg(self.lease_id)
                    .invoke_async::<i64>(&mut connection)
                    .await
            })
            .await;
            match result {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => tracing::debug!(%error, "rate-limit lease release failed"),
                Err(_) => tracing::warn!("rate-limit lease release timed out; lease will expire"),
            }
        });
    }
}

pub struct RedisRateLimiter {
    key_prefix: String,
    max_in_flight: usize,
    lease_ttl: Duration,
    timeout: Duration,
    on_unavailable: StoreUnavailable,
    connection: ConnectionManager,
    acquire: Script,
    release: Script,
}

impl RedisRateLimiter {
    pub async fn connect(
        url: &str,
        key_prefix: String,
        max_in_flight: usize,
        lease_ttl: Duration,
        timeout: Duration,
        on_unavailable: StoreUnavailable,
    ) -> Result<Self, redis::RedisError> {
        let client = redis::Client::open(url)?;
        let mut connection = ConnectionManager::new(client).await?;
        redis::cmd("PING")
            .query_async::<String>(&mut connection)
            .await?;
        Ok(Self {
            key_prefix,
            max_in_flight,
            lease_ttl,
            timeout,
            on_unavailable,
            connection,
            acquire: Script::new(ACQUIRE),
            release: Script::new(RELEASE),
        })
    }

    fn key(&self, key: &RateLimitKey) -> String {
        format!(
            "{}:{{{}|{}}}:leases",
            self.key_prefix, key.namespace, key.subject
        )
    }

    fn unavailable(
        &self,
        error: impl std::fmt::Display,
    ) -> Result<RateLimitPermit, RateLimitError> {
        unavailable(self.on_unavailable, error)
    }
}

fn unavailable(
    policy: StoreUnavailable,
    error: impl std::fmt::Display,
) -> Result<RateLimitPermit, RateLimitError> {
    match policy {
        StoreUnavailable::Deny => {
            tracing::error!(error = %error, "rate-limit store unavailable; denying request");
            metrics::record_rate_limit_unavailable_denial();
            Err(RateLimitError::StoreUnavailable)
        }
        StoreUnavailable::Allow => {
            tracing::warn!(error = %error, "rate-limit store unavailable; serving unenforced");
            Ok(RateLimitPermit::no_limit())
        }
    }
}

#[async_trait]
impl RateLimiter for RedisRateLimiter {
    fn name(&self) -> &'static str {
        "redis"
    }

    async fn acquire(&self, key: &RateLimitKey) -> Result<RateLimitPermit, RateLimitError> {
        let lease_id = next_id();
        let result = tokio::time::timeout(self.timeout, async {
            let mut connection = self.connection.clone();
            self.acquire
                .prepare_invoke()
                .key(self.key(key))
                .arg(now_ms())
                .arg(self.lease_ttl.as_millis() as u64)
                .arg(self.max_in_flight)
                .arg(&lease_id)
                .invoke_async::<i64>(&mut connection)
                .await
        })
        .await;
        match result {
            Ok(Ok(1)) => Ok(RateLimitPermit {
                release: Some(PermitRelease::Redis(RedisRelease {
                    connection: self.connection.clone(),
                    script: self.release.clone(),
                    key: self.key(key),
                    lease_id,
                    timeout: self.timeout,
                })),
            }),
            Ok(Ok(_)) => {
                metrics::record_rate_limit_denial();
                Err(RateLimitError::Exceeded)
            }
            Ok(Err(error)) => self.unavailable(error),
            Err(_) => self.unavailable("operation timed out"),
        }
    }
}

fn next_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    format!(
        "{:x}-{:x}",
        now_ms(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StoreUnavailable;

    fn key() -> RateLimitKey {
        RateLimitKey {
            namespace: "acme".into(),
            subject: "subject".into(),
        }
    }

    async fn limiter(url: &str, prefix: String, ttl: Duration) -> RedisRateLimiter {
        RedisRateLimiter::connect(
            url,
            prefix,
            1,
            ttl,
            Duration::from_millis(250),
            StoreUnavailable::Deny,
        )
        .await
        .expect("connect")
    }

    #[test]
    fn lease_script_reclaims_expired_holds_and_counts_live_leases() {
        assert!(ACQUIRE.contains("HDEL"));
        assert!(ACQUIRE.contains("expires_at <= now"));
        assert!(ACQUIRE.contains("live >= max"));
        assert!(ACQUIRE.contains("PEXPIRE"));
    }

    #[test]
    fn lease_key_uses_one_hash_tag() {
        let key = format!("axond:rate_limit:{{{}|{}}}:leases", "acme", "subject");
        let start = key.find('{').unwrap() + 1;
        let end = key.find('}').unwrap();
        assert_eq!(&key[start..end], "acme|subject");
    }

    #[test]
    fn unavailable_policy_maps_to_distinct_deny_or_unenforced_admission() {
        assert!(matches!(
            unavailable(StoreUnavailable::Deny, "offline"),
            Err(RateLimitError::StoreUnavailable)
        ));
        assert!(unavailable(StoreUnavailable::Allow, "offline").is_ok());
    }

    #[tokio::test]
    async fn two_limiters_sharing_one_redis_enforce_one_limit() {
        let Ok(url) = std::env::var("AXOND_TEST_REDIS_URL") else {
            return;
        };
        let prefix = format!("axond:test:{}", next_id());
        let first = limiter(&url, prefix.clone(), Duration::from_secs(300)).await;
        let second = limiter(&url, prefix, Duration::from_secs(300)).await;
        let held = first.acquire(&key()).await.expect("first admitted");
        assert!(matches!(
            second.acquire(&key()).await,
            Err(RateLimitError::Exceeded)
        ));
        drop(held);
        tokio::time::sleep(Duration::from_millis(25)).await;
        let released = second.acquire(&key()).await.expect("drop released");
        drop(released);
    }

    #[tokio::test]
    async fn an_abandoned_lease_is_reclaimed_after_ttl() {
        let Ok(url) = std::env::var("AXOND_TEST_REDIS_URL") else {
            return;
        };
        let limiter = limiter(
            &url,
            format!("axond:test:{}", next_id()),
            Duration::from_millis(40),
        )
        .await;
        let held = limiter.acquire(&key()).await.expect("admitted");
        std::mem::forget(held);
        assert!(matches!(
            limiter.acquire(&key()).await,
            Err(RateLimitError::Exceeded)
        ));
        tokio::time::sleep(Duration::from_millis(60)).await;
        limiter
            .acquire(&key())
            .await
            .expect("expired lease reclaimed");
    }

    #[tokio::test]
    async fn unreachable_server_fails_at_boot() {
        let result = RedisRateLimiter::connect(
            "redis://127.0.0.1:1/",
            "axond:test".into(),
            1,
            Duration::from_secs(1),
            Duration::from_millis(10),
            StoreUnavailable::Deny,
        )
        .await;
        assert!(result.is_err());
    }
}
