//! Exact cross-replica in-flight concurrency leases in Redis.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use redis::aio::ConnectionLike;
use redis::aio::ConnectionManager;
use redis::{Client, Script};
use ring::rand::{SecureRandom, SystemRandom};
use thiserror::Error;
use tokio::sync::{Semaphore, SemaphorePermit};

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
const RELEASE_MAX_ATTEMPTS: usize = 8;
const RELEASE_RETRY_CONCURRENCY: usize = 16;
const RELEASE_RETRY_WINDOW_MULTIPLIER: u32 = 10;
const SHARED_CONNECTION_REPLACEMENT_COOLDOWN: Duration = Duration::from_millis(250);

// Keep fresh-connection retries below a small fixed concurrency cap; the
// shared manager's first attempt remains available to every release.
static RELEASE_RETRY_SEMAPHORE: OnceLock<Semaphore> = OnceLock::new();

#[derive(Clone)]
struct SharedConnection {
    manager: ConnectionManager,
    generation: u64,
}

pub(crate) struct RedisRelease {
    connection: SharedConnection,
    client: Client,
    script: Script,
    key: String,
    lease_id: String,
    timeout: Duration,
    lease_ttl: Duration,
    retry_semaphore: &'static Semaphore,
    current_connection: Arc<ArcSwap<SharedConnection>>,
    suspect_generation: Arc<AtomicU64>,
}

impl RedisRelease {
    pub(crate) fn spawn(self) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::warn!("rate-limit permit dropped without a Tokio runtime; lease will expire");
            return;
        };
        handle.spawn(async move {
            // This fixed margin covers each attempt's bounded connect and invoke
            // plus capped exponential backoff; the TTL cap makes later retries pointless.
            let retry_window = self
                .timeout
                .saturating_mul(
                    RELEASE_RETRY_WINDOW_MULTIPLIER.saturating_mul(RELEASE_MAX_ATTEMPTS as u32),
                )
                .min(self.lease_ttl);
            let deadline = tokio::time::Instant::now() + retry_window;
            let current = self.current_connection.load_full();
            let suspect_generation = self.suspect_generation.load(Ordering::Acquire);
            let shared = if current.generation == self.connection.generation
                && suspect_generation < self.connection.generation
            {
                Some(self.connection.clone())
            } else if suspect_generation < current.generation {
                Some(current.as_ref().clone())
            } else {
                None
            };
            if let Some(shared) = shared {
                let shared_generation = shared.generation;
                let mut connection = shared.manager;
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if !remaining.is_zero() {
                    let result = tokio::time::timeout(
                        self.timeout.min(remaining),
                        Self::invoke(&self.script, &self.key, &self.lease_id, &mut connection),
                    )
                    .await;
                    if result.is_err() {
                        // Cancelling a multiplexed response can desynchronize
                        // reply alignment. Escalate only this snapshot's
                        // generation; the next acquire owns replacement.
                        self.suspect_generation
                            .fetch_max(shared_generation, Ordering::AcqRel);
                    }
                    if matches!(result, Ok(Ok(_))) {
                        let current = self.current_connection.load_full();
                        if current.generation == shared_generation
                            && self.suspect_generation.load(Ordering::Acquire) < shared_generation
                        {
                            return;
                        }
                    }
                    tracing::debug!(
                        "rate-limit lease release failed; retrying on a fresh connection"
                    );
                }
            }

            for attempt in 2..=RELEASE_MAX_ATTEMPTS {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let backoff = self
                    .timeout
                    .min(Duration::from_millis(25))
                    .saturating_mul(2_u32.pow((attempt - 2) as u32))
                    .min(Duration::from_millis(200));
                tokio::time::sleep(backoff.min(remaining)).await;

                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let Some(_retry_permit) = try_release_retry_permit(self.retry_semaphore) else {
                    tracing::debug!(
                        attempt,
                        "rate-limit lease release retry unavailable; skipping attempt"
                    );
                    continue;
                };
                let connection = tokio::time::timeout(
                    self.timeout.min(remaining),
                    self.client.get_multiplexed_async_connection(),
                )
                .await;
                let Ok(Ok(mut connection)) = connection else {
                    tracing::debug!(
                        attempt,
                        "fresh Redis connection for lease release failed; retrying"
                    );
                    continue;
                };
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let result = tokio::time::timeout(
                    self.timeout.min(remaining),
                    Self::invoke(&self.script, &self.key, &self.lease_id, &mut connection),
                )
                .await;
                if matches!(result, Ok(Ok(_))) {
                    return;
                }
                tracing::debug!(attempt, "rate-limit lease release failed; retrying");
            }

            tracing::warn!("rate-limit lease release gave up; lease will expire");
        });
    }

    async fn invoke<C: ConnectionLike>(
        script: &Script,
        key: &str,
        lease_id: &str,
        connection: &mut C,
    ) -> redis::RedisResult<i64> {
        script
            .prepare_invoke()
            .key(key)
            .arg(lease_id)
            .invoke_async(connection)
            .await
    }
}

fn release_retry_semaphore() -> &'static Semaphore {
    RELEASE_RETRY_SEMAPHORE.get_or_init(|| Semaphore::new(RELEASE_RETRY_CONCURRENCY))
}

fn try_release_retry_permit(semaphore: &Semaphore) -> Option<SemaphorePermit<'_>> {
    semaphore.try_acquire().ok()
}

pub struct RedisRateLimiter {
    key_prefix: String,
    max_in_flight: usize,
    lease_ttl: Duration,
    timeout: Duration,
    on_unavailable: StoreUnavailable,
    connection: Arc<ArcSwap<SharedConnection>>,
    client: Client,
    connect_timeout: Duration,
    acquire: Script,
    release: Script,
    retry_semaphore: &'static Semaphore,
    suspect_generation: Arc<AtomicU64>,
    replacement_in_flight: Arc<AtomicBool>,
    last_replacement_ms: Arc<AtomicU64>,
}

#[derive(Debug, Error)]
pub enum RedisConnectError {
    #[error("Redis connection failed: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("Redis connection timed out after {timeout:?}")]
    Timeout { timeout: Duration },
}

impl RedisRateLimiter {
    pub async fn connect(
        url: &str,
        key_prefix: String,
        max_in_flight: usize,
        lease_ttl: Duration,
        timeout: Duration,
        connect_timeout: Duration,
        on_unavailable: StoreUnavailable,
    ) -> Result<Self, RedisConnectError> {
        let client = redis::Client::open(url)?;
        let release_client = client.clone();
        let connection = tokio::time::timeout(connect_timeout, async {
            let mut connection = ConnectionManager::new(client).await?;
            redis::cmd("PING")
                .query_async::<String>(&mut connection)
                .await?;
            Ok::<_, redis::RedisError>(connection)
        })
        .await
        .map_err(|_| RedisConnectError::Timeout {
            timeout: connect_timeout,
        })??;
        Ok(Self {
            key_prefix,
            max_in_flight,
            lease_ttl,
            timeout,
            on_unavailable,
            connection: Arc::new(ArcSwap::from_pointee(SharedConnection {
                manager: connection,
                generation: 1,
            })),
            client: release_client,
            connect_timeout,
            acquire: Script::new(ACQUIRE),
            release: Script::new(RELEASE),
            retry_semaphore: release_retry_semaphore(),
            suspect_generation: Arc::new(AtomicU64::new(0)),
            replacement_in_flight: Arc::new(AtomicBool::new(false)),
            last_replacement_ms: Arc::new(AtomicU64::new(0)),
        })
    }

    fn key(&self, key: &RateLimitKey) -> String {
        lease_key(&self.key_prefix, key)
    }

    fn release(&self, key: String, lease_id: String) -> RedisRelease {
        RedisRelease {
            connection: self.connection.load_full().as_ref().clone(),
            client: self.client.clone(),
            script: self.release.clone(),
            key,
            lease_id,
            timeout: self.timeout,
            lease_ttl: self.lease_ttl,
            retry_semaphore: self.retry_semaphore,
            current_connection: self.connection.clone(),
            suspect_generation: self.suspect_generation.clone(),
        }
    }

    fn mark_connection_suspect(&self, generation: u64) {
        self.suspect_generation
            .fetch_max(generation, Ordering::AcqRel);
        let current = self.connection.load_full();
        if current.generation != generation
            || self.suspect_generation.load(Ordering::Acquire) != generation
        {
            return;
        }
        if self
            .replacement_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let previous = self.last_replacement_ms.load(Ordering::Relaxed);

        let connection = self.connection.clone();
        let client = self.client.clone();
        let connect_timeout = self.connect_timeout;
        let suspect_generation = self.suspect_generation.clone();
        let replacement_in_flight = self.replacement_in_flight.clone();
        let last_replacement_ms = self.last_replacement_ms.clone();
        tokio::spawn(async move {
            // Keep the replacement marked in flight while honoring cooldown;
            // otherwise a suspect manager could be left refusing requests with
            // no replacement scheduled to repair it.
            if previous != 0 {
                let elapsed = now_ms().saturating_sub(previous);
                if elapsed < SHARED_CONNECTION_REPLACEMENT_COOLDOWN.as_millis() as u64 {
                    tokio::time::sleep(
                        SHARED_CONNECTION_REPLACEMENT_COOLDOWN
                            .saturating_sub(Duration::from_millis(elapsed)),
                    )
                    .await;
                }
            }
            let current = connection.load_full();
            if current.generation != generation
                || suspect_generation.load(Ordering::Acquire) != generation
            {
                replacement_in_flight.store(false, Ordering::Release);
                return;
            }
            // Record the actual dial attempt, not merely the scheduling time.
            // This makes cooldown cover the whole replacement attempt.
            // (The task remains in flight during any preceding cooldown wait.)
            last_replacement_ms.store(now_ms(), Ordering::Relaxed);
            let replacement = tokio::time::timeout(connect_timeout, async {
                let mut connection = ConnectionManager::new(client).await?;
                redis::cmd("PING")
                    .query_async::<String>(&mut connection)
                    .await?;
                Ok::<_, redis::RedisError>(connection)
            })
            .await;
            if let Ok(Ok(manager)) = replacement {
                let current = connection.load_full();
                if current.generation == generation
                    && suspect_generation.load(Ordering::Acquire) == generation
                {
                    connection.store(Arc::new(SharedConnection {
                        manager,
                        generation: generation + 1,
                    }));
                }
            }
            replacement_in_flight.store(false, Ordering::Release);
        });
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

fn lease_key(prefix: &str, key: &RateLimitKey) -> String {
    format!(
        "{prefix}:{{{}|{}}}:leases",
        encode_component(&key.namespace),
        encode_component(&key.subject)
    )
}

fn encode_component(component: &str) -> String {
    let mut encoded = String::with_capacity(component.len());
    for character in component.chars() {
        match character {
            '%' => encoded.push_str("%25"),
            '|' => encoded.push_str("%7C"),
            '{' => encoded.push_str("%7B"),
            '}' => encoded.push_str("%7D"),
            _ => encoded.push(character),
        }
    }
    encoded
}

#[async_trait]
impl RateLimiter for RedisRateLimiter {
    fn name(&self) -> &'static str {
        "redis"
    }

    async fn acquire(&self, key: &RateLimitKey) -> Result<RateLimitPermit, RateLimitError> {
        let snapshot = self.connection.load_full();
        if self.suspect_generation.load(Ordering::Acquire) >= snapshot.generation {
            self.mark_connection_suspect(snapshot.generation);
            return self.unavailable("shared Redis connection is recovering");
        }
        let lease_id = next_id();
        let lease_key = self.key(key);
        let connection_generation = snapshot.generation;
        let result = tokio::time::timeout(self.timeout, async {
            // Swapping the manager only affects future requests. This request
            // keeps its snapshot; the generation check below discards any
            // successful result if a replacement happened while it was in flight.
            let mut connection = snapshot.manager.clone();
            self.acquire
                .prepare_invoke()
                .key(&lease_key)
                .arg(now_ms())
                .arg(self.lease_ttl.as_millis() as u64)
                .arg(self.max_in_flight)
                .arg(&lease_id)
                .invoke_async::<i64>(&mut connection)
                .await
        })
        .await;
        let current_generation = self.connection.load_full().generation;
        let result_untrusted = connection_generation != current_generation
            || self.suspect_generation.load(Ordering::Acquire) >= connection_generation;
        if result_untrusted && matches!(result, Ok(Ok(_))) {
            self.release(lease_key, lease_id).spawn();
            return self.unavailable("shared Redis connection was replaced during acquire");
        }
        match result {
            Ok(Ok(1)) => Ok(RateLimitPermit {
                release: Some(PermitRelease::Redis(Box::new(
                    self.release(lease_key, lease_id),
                ))),
            }),
            Ok(Ok(_)) => {
                metrics::record_rate_limit_denial();
                Err(RateLimitError::Exceeded)
            }
            Ok(Err(error)) => {
                self.release(lease_key, lease_id).spawn();
                self.unavailable(error)
            }
            Err(_) => {
                self.mark_connection_suspect(connection_generation);
                self.release(lease_key, lease_id).spawn();
                self.unavailable("operation timed out")
            }
        }
    }
}

fn next_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    static RANDOM: OnceLock<u128> = OnceLock::new();
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
    let random = *RANDOM.get_or_init(|| {
        let mut bytes = [0_u8; 16];
        SystemRandom::new()
            .fill(&mut bytes)
            .expect("OS randomness unavailable");
        u128::from_be_bytes(bytes)
    });
    format!(
        "{:x}-{:x}-{:x}-{:x}",
        epoch_micros,
        std::process::id(),
        random,
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
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicI64, AtomicUsize, Ordering},
    };
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::watch;
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
            Duration::from_millis(250),
            StoreUnavailable::Deny,
        )
        .await
        .expect("connect")
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum RelayMode {
        Forward,
        Blackhole,
        StallResponses,
        Cut,
    }

    struct RedisRelay {
        address: SocketAddr,
        mode: watch::Sender<RelayMode>,
        task: JoinHandle<()>,
    }

    impl RedisRelay {
        async fn start(redis_url: &str) -> Self {
            assert!(
                redis_url.starts_with("redis://"),
                "AXOND_TEST_REDIS_URL must use a plain redis:// URL; TLS rediss:// is not supported by the test relay"
            );
            let target = redis_url
                .strip_prefix("redis://")
                .expect("checked redis:// prefix")
                .split('/')
                .next()
                .expect("Redis URL must include a host and port")
                .parse::<SocketAddr>()
                .expect("AXOND_TEST_REDIS_URL must use redis://host:port[/db]");
            let listener = TcpListener::bind(("127.0.0.1", 0))
                .await
                .expect("bind relay");
            let address = listener.local_addr().expect("relay address");
            let (mode, relay_mode) = watch::channel(RelayMode::Forward);
            let task = tokio::spawn(async move {
                loop {
                    let Ok((client, _)) = listener.accept().await else {
                        break;
                    };
                    let mut state = relay_mode.clone();
                    tokio::spawn(async move {
                        if *state.borrow() == RelayMode::Cut {
                            return;
                        }
                        let Ok(server) = TcpStream::connect(target).await else {
                            return;
                        };
                        proxy(client, server, &mut state).await;
                    });
                }
            });
            Self {
                address,
                mode,
                task,
            }
        }

        fn url(&self) -> String {
            format!("redis://{}/", self.address)
        }

        fn set_mode(&self, mode: RelayMode) {
            self.mode.send_replace(mode);
        }
    }

    impl Drop for RedisRelay {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    struct RedisStub {
        address: SocketAddr,
        task: JoinHandle<()>,
    }

    impl RedisStub {
        async fn start(respond_to_ping: bool, stall_after_ping: bool) -> Self {
            let listener = TcpListener::bind(("127.0.0.1", 0))
                .await
                .expect("bind Redis stub");
            let address = listener.local_addr().expect("stub address");
            let task = tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        break;
                    };
                    tokio::spawn(handle_stub_connection(
                        stream,
                        respond_to_ping,
                        stall_after_ping,
                    ));
                }
            });
            Self { address, task }
        }

        fn url(&self) -> String {
            format!("redis://{}/", self.address)
        }
    }

    async fn handle_stub_connection(
        stream: TcpStream,
        respond_to_ping: bool,
        stall_after_ping: bool,
    ) {
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        loop {
            let Some(command) = read_resp_command(&mut reader).await else {
                return;
            };
            let is_ping = command
                .first()
                .is_some_and(|argument| argument.eq_ignore_ascii_case(b"PING"));
            if is_ping {
                if !respond_to_ping {
                    std::future::pending::<()>().await;
                }
                if write_half.write_all(b"+PONG\r\n").await.is_err() {
                    return;
                }
                if stall_after_ping {
                    std::future::pending::<()>().await;
                }
            } else if write_half.write_all(b"+OK\r\n").await.is_err() {
                return;
            }
        }
    }

    impl Drop for RedisStub {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    struct RedisRetryStub {
        address: SocketAddr,
        connections: Arc<AtomicUsize>,
        stalled_lease: Arc<Mutex<Option<Vec<u8>>>>,
        released_lease: Arc<Mutex<Option<Vec<u8>>>>,
        release_connection: Arc<AtomicUsize>,
        shared_release_commands: Arc<AtomicUsize>,
        task: JoinHandle<()>,
    }

    impl RedisRetryStub {
        async fn start() -> Self {
            let listener = TcpListener::bind(("127.0.0.1", 0))
                .await
                .expect("bind Redis retry stub");
            let address = listener.local_addr().expect("retry stub address");
            let connections = Arc::new(AtomicUsize::new(0));
            let stalled_lease = Arc::new(Mutex::new(None));
            let released_lease = Arc::new(Mutex::new(None));
            let release_connection = Arc::new(AtomicUsize::new(0));
            let shared_release_commands = Arc::new(AtomicUsize::new(0));
            let task = {
                let connections = connections.clone();
                let stalled_lease = stalled_lease.clone();
                let released_lease = released_lease.clone();
                let release_connection = release_connection.clone();
                let shared_release_commands = shared_release_commands.clone();
                tokio::spawn(async move {
                    loop {
                        let Ok((stream, _)) = listener.accept().await else {
                            break;
                        };
                        let connection = connections.fetch_add(1, Ordering::Relaxed) + 1;
                        tokio::spawn(handle_retry_connection(
                            stream,
                            connection,
                            connection == 1,
                            stalled_lease.clone(),
                            released_lease.clone(),
                            release_connection.clone(),
                            shared_release_commands.clone(),
                        ));
                    }
                })
            };
            Self {
                address,
                connections,
                stalled_lease,
                released_lease,
                release_connection,
                shared_release_commands,
                task,
            }
        }

        fn url(&self) -> String {
            format!("redis://{}/", self.address)
        }
    }

    impl Drop for RedisRetryStub {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    struct RedisLiveStub {
        address: SocketAddr,
        connections: Arc<AtomicUsize>,
        acquires: Arc<AtomicUsize>,
        releases: Arc<AtomicUsize>,
        acquire_result: Arc<AtomicI64>,
        acquire_delay_ms: Arc<AtomicU64>,
        release_delay_ms: Arc<AtomicU64>,
        task: JoinHandle<()>,
    }

    impl RedisLiveStub {
        async fn start() -> Self {
            let listener = TcpListener::bind(("127.0.0.1", 0))
                .await
                .expect("bind live Redis stub");
            let address = listener.local_addr().expect("live stub address");
            let connections = Arc::new(AtomicUsize::new(0));
            let acquires = Arc::new(AtomicUsize::new(0));
            let releases = Arc::new(AtomicUsize::new(0));
            let acquire_result = Arc::new(AtomicI64::new(1));
            let acquire_delay_ms = Arc::new(AtomicU64::new(0));
            let release_delay_ms = Arc::new(AtomicU64::new(0));
            let task = {
                let connections = connections.clone();
                let acquires = acquires.clone();
                let releases = releases.clone();
                let acquire_result = acquire_result.clone();
                let acquire_delay_ms = acquire_delay_ms.clone();
                let release_delay_ms = release_delay_ms.clone();
                tokio::spawn(async move {
                    loop {
                        let Ok((stream, _)) = listener.accept().await else {
                            break;
                        };
                        connections.fetch_add(1, Ordering::Relaxed);
                        tokio::spawn(handle_live_connection(
                            stream,
                            acquires.clone(),
                            releases.clone(),
                            acquire_result.clone(),
                            acquire_delay_ms.clone(),
                            release_delay_ms.clone(),
                        ));
                    }
                })
            };
            Self {
                address,
                connections,
                acquires,
                releases,
                acquire_result,
                acquire_delay_ms,
                release_delay_ms,
                task,
            }
        }

        fn url(&self) -> String {
            format!("redis://{}/", self.address)
        }

        fn set_acquire_result(&self, result: i64) {
            self.acquire_result.store(result, Ordering::Relaxed);
        }

        fn set_acquire_delay(&self, delay: Duration) {
            self.acquire_delay_ms
                .store(delay.as_millis() as u64, Ordering::Relaxed);
        }

        fn set_release_delay(&self, delay: Duration) {
            self.release_delay_ms
                .store(delay.as_millis() as u64, Ordering::Relaxed);
        }
    }

    impl Drop for RedisLiveStub {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn handle_live_connection(
        stream: TcpStream,
        acquires: Arc<AtomicUsize>,
        releases: Arc<AtomicUsize>,
        acquire_result: Arc<AtomicI64>,
        acquire_delay_ms: Arc<AtomicU64>,
        release_delay_ms: Arc<AtomicU64>,
    ) {
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        loop {
            let Some(command) = read_resp_command(&mut reader).await else {
                return;
            };
            let name = command.first().map(Vec::as_slice).unwrap_or_default();
            let response = if name.eq_ignore_ascii_case(b"PING") {
                b"+PONG\r\n".to_vec()
            } else if name.eq_ignore_ascii_case(b"CLIENT") {
                b"+OK\r\n".to_vec()
            } else if name.eq_ignore_ascii_case(b"SCRIPT") {
                let script = command.get(2).map(Vec::as_slice).unwrap_or_default();
                let hash = ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, script)
                    .as_ref()
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>();
                format!("${}\r\n{}\r\n", hash.len(), hash).into_bytes()
            } else if name.eq_ignore_ascii_case(b"EVALSHA") && command.len() >= 8 {
                acquires.fetch_add(1, Ordering::Relaxed);
                let delay = acquire_delay_ms.load(Ordering::Relaxed);
                if delay != 0 {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                format!(":{}\r\n", acquire_result.load(Ordering::Relaxed)).into_bytes()
            } else if name.eq_ignore_ascii_case(b"EVALSHA") {
                releases.fetch_add(1, Ordering::Relaxed);
                let delay = release_delay_ms.load(Ordering::Relaxed);
                if delay != 0 {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                b":1\r\n".to_vec()
            } else {
                b"+OK\r\n".to_vec()
            };
            if write_half.write_all(&response).await.is_err() {
                return;
            }
        }
    }

    async fn handle_retry_connection(
        stream: TcpStream,
        connection: usize,
        stall_acquire: bool,
        stalled_lease: Arc<Mutex<Option<Vec<u8>>>>,
        released_lease: Arc<Mutex<Option<Vec<u8>>>>,
        release_connection: Arc<AtomicUsize>,
        shared_release_commands: Arc<AtomicUsize>,
    ) {
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        let mut loaded_hash = None;
        let mut loaded_release = false;
        loop {
            let Some(command) = read_resp_command(&mut reader).await else {
                return;
            };
            let name = command.first().map(Vec::as_slice).unwrap_or_default();
            if name.eq_ignore_ascii_case(b"PING") {
                if write_half.write_all(b"+PONG\r\n").await.is_err() {
                    return;
                }
            } else if stall_acquire && name.eq_ignore_ascii_case(b"EVALSHA") && command.len() >= 8 {
                if let Some(lease_id) = command.get(7) {
                    *stalled_lease.lock().unwrap() = Some(lease_id.clone());
                }
                continue;
            } else if name.eq_ignore_ascii_case(b"EVALSHA") {
                if stall_acquire && command.len() == 5 {
                    shared_release_commands.fetch_add(1, Ordering::Relaxed);
                }
                if loaded_release
                    && loaded_hash.as_deref().map(str::as_bytes)
                        == command.get(1).map(Vec::as_slice)
                {
                    if let Some(lease_id) = command.get(4) {
                        *released_lease.lock().unwrap() = Some(lease_id.clone());
                    }
                    release_connection.store(connection, Ordering::Relaxed);
                    if write_half.write_all(b":1\r\n").await.is_err() {
                        return;
                    }
                    continue;
                }
                if write_half
                    .write_all(b"-NOSCRIPT No matching script\r\n")
                    .await
                    .is_err()
                {
                    return;
                }
            } else if name.eq_ignore_ascii_case(b"SCRIPT")
                && command
                    .get(1)
                    .is_some_and(|argument| argument.eq_ignore_ascii_case(b"LOAD"))
            {
                let Some(script) = command.get(2) else {
                    return;
                };
                let hash = ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, script)
                    .as_ref()
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>();
                loaded_hash = Some(hash.clone());
                loaded_release = script.windows(4).any(|window| window == b"HDEL");
                if write_half
                    .write_all(format!("${}\r\n{}\r\n", hash.len(), hash).as_bytes())
                    .await
                    .is_err()
                {
                    return;
                }
            } else if write_half.write_all(b"+OK\r\n").await.is_err() {
                return;
            }
        }
    }

    async fn read_resp_command(
        reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
    ) -> Option<Vec<Vec<u8>>> {
        let mut line = Vec::new();
        reader.read_until(b'\n', &mut line).await.ok()?;
        let count = std::str::from_utf8(line.strip_prefix(b"*")?.strip_suffix(b"\r\n")?)
            .ok()?
            .parse::<usize>()
            .ok()?;
        let mut command = Vec::with_capacity(count);
        for _ in 0..count {
            line.clear();
            reader.read_until(b'\n', &mut line).await.ok()?;
            let length = std::str::from_utf8(line.strip_prefix(b"$")?.strip_suffix(b"\r\n")?)
                .ok()?
                .parse::<usize>()
                .ok()?;
            let mut argument = vec![0; length + 2];
            reader.read_exact(&mut argument).await.ok()?;
            if !argument.ends_with(b"\r\n") {
                return None;
            }
            argument.truncate(length);
            command.push(argument);
        }
        Some(command)
    }

    async fn proxy(client: TcpStream, server: TcpStream, mode: &mut watch::Receiver<RelayMode>) {
        let (mut client_read, mut client_write) = client.into_split();
        let (mut server_read, mut server_write) = server.into_split();
        let mut client_buffer = [0_u8; 16 * 1024];
        let mut server_buffer = [0_u8; 16 * 1024];
        loop {
            tokio::select! {
                changed = mode.changed() => {
                    if changed.is_err() || *mode.borrow() == RelayMode::Cut {
                        return;
                    }
                }
                result = client_read.read(&mut client_buffer) => {
                    let Ok(size) = result else { return };
                    if size == 0 { return; }
                    if matches!(
                        *mode.borrow(),
                        RelayMode::Forward | RelayMode::StallResponses
                    )
                        && server_write.write_all(&client_buffer[..size]).await.is_err()
                    {
                        return;
                    }
                }
                result = server_read.read(&mut server_buffer) => {
                    let Ok(size) = result else { return };
                    if size == 0 { return; }
                    if *mode.borrow() == RelayMode::Forward
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
        let key = lease_key("axond:rate_limit", &key());
        let start = key.find('{').unwrap() + 1;
        let end = key.find('}').unwrap();
        assert_eq!(&key[start..end], "acme|subject");
        assert_eq!(key, "axond:rate_limit:{acme|subject}:leases");
    }

    #[test]
    fn lease_key_encoding_is_injective_and_preserves_one_hash_tag() {
        let first = lease_key(
            "axond:rate_limit",
            &RateLimitKey {
                namespace: "a|b".into(),
                subject: "c".into(),
            },
        );
        let second = lease_key(
            "axond:rate_limit",
            &RateLimitKey {
                namespace: "a".into(),
                subject: "b|c".into(),
            },
        );
        assert_ne!(first, second);

        let braces = lease_key(
            "axond:rate_limit",
            &RateLimitKey {
                namespace: "a{b".into(),
                subject: "c}d".into(),
            },
        );
        assert_eq!(braces.matches('{').count(), 1);
        assert_eq!(braces.matches('}').count(), 1);
        assert!(braces.contains("{a%7Bb|c%7Dd}"));
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
            timeout,
            StoreUnavailable::Allow,
        )
        .await
        .expect("connect allow limiter");

        relay.set_mode(RelayMode::Blackhole);
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

        relay.set_mode(RelayMode::Forward);
        assert!(matches!(
            deny.acquire(&key()).await,
            Err(RateLimitError::StoreUnavailable)
        ));

        relay.set_mode(RelayMode::Forward);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let mut recovered = false;
        while tokio::time::Instant::now() < deadline {
            if deny.acquire(&key()).await.is_ok() {
                recovered = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            recovered,
            "same limiter did not recover after relay forwarding was restored"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropped_response_retires_shared_manager_before_next_acquire() {
        let stub = RedisLiveStub::start().await;
        let relay = RedisRelay::start(&stub.url()).await;
        let limiter = RedisRateLimiter::connect(
            &relay.url(),
            format!("axond:test:{}", next_id()),
            1,
            Duration::from_secs(5),
            Duration::from_millis(50),
            Duration::from_millis(50),
            StoreUnavailable::Deny,
        )
        .await
        .expect("connect limiter");

        relay.set_mode(RelayMode::StallResponses);
        assert!(matches!(
            limiter.acquire(&key()).await,
            Err(RateLimitError::StoreUnavailable)
        ));
        relay.set_mode(RelayMode::Cut);
        assert_eq!(stub.acquires.load(Ordering::Relaxed), 1);

        let started = std::time::Instant::now();
        assert!(matches!(
            limiter.acquire(&key()).await,
            Err(RateLimitError::StoreUnavailable)
        ));
        assert!(
            started.elapsed() < Duration::from_millis(25),
            "suspect connection queued or waited instead of refusing"
        );
        assert_eq!(
            stub.acquires.load(Ordering::Relaxed),
            1,
            "acquire used a poisoned shared connection"
        );

        relay.set_mode(RelayMode::Forward);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let mut recovered = false;
        while tokio::time::Instant::now() < deadline {
            if limiter.acquire(&key()).await.is_ok() {
                recovered = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            recovered,
            "replacement did not restore admission within the bounded recovery window"
        );
        assert!(
            stub.acquires.load(Ordering::Relaxed) >= 2,
            "replacement admission did not reach the live stub"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    /// An unattributable acquire result is unknown, so `allow` may fail open
    /// rather than turning an untrusted denial into a hard 429.
    async fn unattributable_denial_during_replacement_fails_open_when_allowed() {
        let stub = RedisLiveStub::start().await;
        let limiter = Arc::new(
            RedisRateLimiter::connect(
                &stub.url(),
                format!("axond:test:{}", next_id()),
                1,
                Duration::from_secs(5),
                Duration::from_millis(50),
                Duration::from_millis(50),
                StoreUnavailable::Allow,
            )
            .await
            .expect("connect limiter"),
        );
        stub.set_acquire_result(0);
        stub.set_acquire_delay(Duration::from_millis(25));

        let acquire = tokio::spawn({
            let limiter = limiter.clone();
            let key = key();
            async move { limiter.acquire(&key).await }
        });
        let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
        while stub.acquires.load(Ordering::Relaxed) == 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "delayed denial did not reach the live stub"
            );
            tokio::task::yield_now().await;
        }
        let generation = limiter.connection.load_full().generation;
        limiter.mark_connection_suspect(generation);

        assert!(
            acquire.await.expect("acquire task").is_ok(),
            "an unattributable result did not follow the unavailable allow policy"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn release_uses_the_current_connection_after_replacement() {
        let stub = RedisLiveStub::start().await;
        let limiter = RedisRateLimiter::connect(
            &stub.url(),
            format!("axond:test:{}", next_id()),
            1,
            Duration::from_secs(5),
            Duration::from_millis(50),
            Duration::from_millis(50),
            StoreUnavailable::Deny,
        )
        .await
        .expect("connect limiter");

        let permit = limiter.acquire(&key()).await.expect("acquire permit");
        let old_generation = limiter.connection.load_full().generation;
        limiter.mark_connection_suspect(old_generation);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while limiter.connection.load_full().generation == old_generation {
            assert!(
                tokio::time::Instant::now() < deadline,
                "shared connection replacement did not complete"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let connections_after_swap = stub.connections.load(Ordering::Relaxed);
        drop(permit);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while stub.releases.load(Ordering::Relaxed) == 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "release did not reach the live stub"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            stub.connections.load(Ordering::Relaxed),
            connections_after_swap,
            "release fell through to a fresh connection despite a healthy current manager"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_shared_release_marks_its_generation_suspect() {
        let stub = RedisLiveStub::start().await;
        let limiter = RedisRateLimiter::connect(
            &stub.url(),
            format!("axond:test:{}", next_id()),
            1,
            Duration::from_secs(5),
            Duration::from_millis(25),
            Duration::from_millis(25),
            StoreUnavailable::Deny,
        )
        .await
        .expect("connect limiter");
        let permit = limiter.acquire(&key()).await.expect("acquire permit");
        let generation = limiter.connection.load_full().generation;
        stub.set_release_delay(Duration::from_millis(100));
        drop(permit);

        let deadline = tokio::time::Instant::now() + Duration::from_millis(250);
        while limiter.suspect_generation.load(Ordering::Acquire) < generation {
            assert!(
                tokio::time::Instant::now() < deadline,
                "cancelled shared release did not retire its generation"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            limiter.suspect_generation.load(Ordering::Acquire),
            generation
        );
    }

    #[tokio::test]
    async fn unavailable_policy_is_exercised_through_hermetic_acquire() {
        let stub = RedisStub::start(true, true).await;
        let timeout = Duration::from_millis(40);
        let deny = RedisRateLimiter::connect(
            &stub.url(),
            format!("axond:test:{}", next_id()),
            1,
            Duration::from_secs(5),
            timeout,
            timeout,
            StoreUnavailable::Deny,
        )
        .await
        .expect("connect deny limiter");
        let allow = RedisRateLimiter::connect(
            &stub.url(),
            format!("axond:test:{}", next_id()),
            1,
            Duration::from_secs(5),
            timeout,
            timeout,
            StoreUnavailable::Allow,
        )
        .await
        .expect("connect allow limiter");

        assert!(matches!(
            deny.acquire(&key()).await,
            Err(RateLimitError::StoreUnavailable)
        ));
        assert!(allow.acquire(&key()).await.is_ok());
    }

    #[tokio::test]
    async fn ambiguous_release_retries_on_a_fresh_connection() {
        let stub = RedisRetryStub::start().await;
        let timeout = Duration::from_millis(40);
        let limiter = RedisRateLimiter::connect(
            &stub.url(),
            format!("axond:test:{}", next_id()),
            1,
            Duration::from_secs(5),
            timeout,
            timeout,
            StoreUnavailable::Deny,
        )
        .await
        .expect("connect limiter");

        assert!(matches!(
            limiter.acquire(&key()).await,
            Err(RateLimitError::StoreUnavailable)
        ));

        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while tokio::time::Instant::now() < deadline {
            let stalled_lease = stub.stalled_lease.lock().unwrap().clone();
            let released_lease = stub.released_lease.lock().unwrap().clone();
            if let (Some(stalled_lease), Some(released_lease)) = (stalled_lease, released_lease) {
                assert!(stub.connections.load(Ordering::Relaxed) >= 2);
                assert!(stub.release_connection.load(Ordering::Relaxed) >= 2);
                assert_eq!(
                    stub.shared_release_commands.load(Ordering::Relaxed),
                    0,
                    "suspect shared connection received a release command"
                );
                assert_eq!(released_lease, stalled_lease);
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("compensating release was not observed on a fresh connection");
    }

    #[tokio::test]
    async fn ambiguous_release_skips_contended_retry_attempts() {
        static NO_RETRY_SEMAPHORE: Semaphore = Semaphore::const_new(0);

        let stub = RedisRetryStub::start().await;
        let timeout = Duration::from_millis(40);
        let mut limiter = RedisRateLimiter::connect(
            &stub.url(),
            format!("axond:test:{}", next_id()),
            1,
            Duration::from_secs(5),
            timeout,
            timeout,
            StoreUnavailable::Deny,
        )
        .await
        .expect("connect limiter");
        limiter.retry_semaphore = &NO_RETRY_SEMAPHORE;

        assert!(matches!(
            limiter.acquire(&key()).await,
            Err(RateLimitError::StoreUnavailable)
        ));

        // The capped backoff across attempts 2..=8 is
        // 25 + 50 + 100 + 200 + 200 + 200 + 200 = 975 ms; this window
        // leaves headroom for all contended release attempts. The one extra
        // connection is the shared-manager replacement, not a release retry.
        tokio::time::sleep(Duration::from_millis(1_300)).await;
        assert_eq!(stub.connections.load(Ordering::Relaxed), 2);
        assert!(stub.released_lease.lock().unwrap().is_none());
    }

    #[test]
    fn release_retry_semaphore_refuses_exhaustion_without_queueing() {
        let semaphore = Semaphore::new(RELEASE_RETRY_CONCURRENCY);
        let permits = (0..RELEASE_RETRY_CONCURRENCY)
            .map(|_| try_release_retry_permit(&semaphore).expect("reserve retry permit"))
            .collect::<Vec<_>>();

        assert!(try_release_retry_permit(&semaphore).is_none());
        drop(permits);
        assert!(try_release_retry_permit(&semaphore).is_some());
    }

    #[tokio::test]
    async fn connect_timeout_is_bounded_when_ping_never_answers() {
        let stub = RedisStub::start(false, false).await;
        let timeout = Duration::from_millis(40);
        let started = std::time::Instant::now();
        let result = RedisRateLimiter::connect(
            &stub.url(),
            "axond:test".into(),
            1,
            Duration::from_secs(1),
            Duration::from_millis(250),
            timeout,
            StoreUnavailable::Deny,
        )
        .await;

        assert!(matches!(result, Err(RedisConnectError::Timeout { .. })));
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "connect timeout was not bounded: {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn timed_out_acquire_releases_an_ambiguous_redis_lease() {
        let Ok(url) = std::env::var("AXOND_TEST_REDIS_URL") else {
            return;
        };
        let relay = RedisRelay::start(&url).await;
        let limiter = RedisRateLimiter::connect(
            &relay.url(),
            format!("axond:test:{}", next_id()),
            1,
            Duration::from_secs(5),
            Duration::from_millis(50),
            Duration::from_millis(50),
            StoreUnavailable::Deny,
        )
        .await
        .expect("connect limiter");

        relay.set_mode(RelayMode::StallResponses);
        assert!(matches!(
            limiter.acquire(&key()).await,
            Err(RateLimitError::StoreUnavailable)
        ));
        relay.set_mode(RelayMode::Forward);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while tokio::time::Instant::now() < deadline {
            if limiter.acquire(&key()).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("timed-out acquire left a lease consuming the subject slot");
    }

    #[tokio::test]
    async fn unreachable_server_fails_at_boot() {
        let result = RedisRateLimiter::connect(
            "redis://127.0.0.1:1/",
            "axond:test".into(),
            1,
            Duration::from_secs(1),
            Duration::from_millis(10),
            Duration::from_millis(10),
            StoreUnavailable::Deny,
        )
        .await;
        assert!(result.is_err());
    }
}
