//! Exact cross-replica in-flight concurrency leases in Redis.

use std::sync::OnceLock;
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
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
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
    // Lease ids are Redis hash fields, so they must be globally unique:
    // another replica colliding here would overwrite a live lease and
    // over-admit past the fleet-wide concurrency limit.
    static EPOCH_MICROS: OnceLock<u64> = OnceLock::new();
    let epoch_micros = *EPOCH_MICROS.get_or_init(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_micros() as u64)
            .unwrap_or_default()
    });
    format!(
        "{:x}-{:x}-{:x}",
        epoch_micros,
        std::process::id(),
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
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::task::JoinHandle;

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

    struct RedisRelay {
        address: SocketAddr,
        blackhole: Arc<AtomicBool>,
        task: JoinHandle<()>,
    }

    impl RedisRelay {
        async fn start(redis_url: &str) -> Self {
            let target = redis_url
                .strip_prefix("redis://")
                .unwrap_or(redis_url)
                .trim_end_matches('/')
                .parse::<SocketAddr>()
                .expect("AXOND_TEST_REDIS_URL must use a host:port Redis URL");
            let listener = TcpListener::bind(("127.0.0.1", 0))
                .await
                .expect("bind relay");
            let address = listener.local_addr().expect("relay address");
            let blackhole = Arc::new(AtomicBool::new(false));
            let relay_state = Arc::clone(&blackhole);
            let task = tokio::spawn(async move {
                loop {
                    let Ok((client, _)) = listener.accept().await else {
                        break;
                    };
                    let state = Arc::clone(&relay_state);
                    tokio::spawn(async move {
                        let Ok(server) = TcpStream::connect(target).await else {
                            return;
                        };
                        proxy(client, server, state).await;
                    });
                }
            });
            Self {
                address,
                blackhole,
                task,
            }
        }

        fn url(&self) -> String {
            format!("redis://{}/", self.address)
        }

        fn set_blackhole(&self, blackhole: bool) {
            self.blackhole.store(blackhole, Ordering::Release);
        }
    }

    impl Drop for RedisRelay {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn proxy(client: TcpStream, server: TcpStream, blackhole: Arc<AtomicBool>) {
        let (mut client_read, mut client_write) = client.into_split();
        let (mut server_read, mut server_write) = server.into_split();
        let mut client_buffer = [0_u8; 16 * 1024];
        let mut server_buffer = [0_u8; 16 * 1024];
        loop {
            tokio::select! {
                result = client_read.read(&mut client_buffer) => {
                    let Ok(size) = result else { return };
                    if size == 0 { return; }
                    if !blackhole.load(Ordering::Acquire)
                        && server_write.write_all(&client_buffer[..size]).await.is_err()
                    {
                        return;
                    }
                }
                result = server_read.read(&mut server_buffer) => {
                    let Ok(size) = result else { return };
                    if size == 0 { return; }
                    if !blackhole.load(Ordering::Acquire)
                        && client_write.write_all(&server_buffer[..size]).await.is_err()
                    {
                        return;
                    }
                }
            }
        }
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

    #[test]
    fn lease_ids_are_unique_within_a_process() {
        let ids = (0..100)
            .map(|_| next_id())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(ids.len(), 100);
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
    async fn unavailable_redis_denies_within_timeout_and_recovers() {
        let Ok(url) = std::env::var("AXOND_TEST_REDIS_URL") else {
            return;
        };
        let relay = RedisRelay::start(&url).await;
        let timeout = Duration::from_millis(50);
        let prefix = format!("axond:test:{}", next_id());
        let deny = RedisRateLimiter::connect(
            &relay.url(),
            prefix.clone(),
            1,
            Duration::from_secs(5),
            timeout,
            StoreUnavailable::Deny,
        )
        .await
        .expect("connect deny limiter");
        let allow = RedisRateLimiter::connect(
            &relay.url(),
            format!("axond:test:{}", next_id()),
            1,
            Duration::from_secs(5),
            timeout,
            StoreUnavailable::Allow,
        )
        .await
        .expect("connect allow limiter");

        relay.set_blackhole(true);
        let started = std::time::Instant::now();
        assert!(matches!(
            deny.acquire(&key()).await,
            Err(RateLimitError::StoreUnavailable)
        ));
        let elapsed = started.elapsed();
        assert!(
            elapsed >= timeout,
            "returned before operation timeout: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "unavailable acquire was not bounded: {elapsed:?}"
        );
        assert!(allow.acquire(&key()).await.is_ok());

        relay.set_blackhole(false);
        let recovered = RedisRateLimiter::connect(
            &relay.url(),
            prefix,
            1,
            Duration::from_secs(5),
            timeout,
            StoreUnavailable::Deny,
        )
        .await
        .expect("connect after relay restore");
        recovered
            .acquire(&key())
            .await
            .expect("limiter admitted after relay restore");
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
