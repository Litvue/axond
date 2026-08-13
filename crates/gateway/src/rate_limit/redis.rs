//! Exact cross-replica in-flight concurrency leases in Redis.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use redis::aio::ConnectionLike;
use redis::aio::ConnectionManager;
use redis::{Client, Script};
use ring::rand::{SecureRandom, SystemRandom};
use thiserror::Error;
use tokio::sync::{Semaphore, SemaphorePermit, oneshot};

use super::{PermitRelease, RateLimitError, RateLimitKey, RateLimitPermit, RateLimiter};
use crate::config::StoreUnavailable;
use crate::desired_state::policy::PolicyGeneration;
use crate::policy::{ActivePolicy, Ceilings, ConcurrencyCaps, PolicyHold, Unenforceable, denied};
use crate::redis_support::{RedisConnection as SharedConnection, RedisRecovery as SharedRecovery};
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
  return {0, lease_id}
end
redis.call('HSET', KEYS[1], lease_id, now + ttl_ms)
redis.call('PEXPIRE', KEYS[1], ttl_ms * 2)
return {1, lease_id}
"#;

const RELEASE: &str = "redis.call('HDEL', KEYS[1], ARGV[1]); return 1";
const RELEASE_MAX_ATTEMPTS: usize = 8;
const RELEASE_RETRY_CONCURRENCY: usize = 16;
const RELEASE_RETRY_WINDOW_MULTIPLIER: u32 = 10;
const REDIS_OPERATION_TIMEOUT_MULTIPLIER: u32 = 4;
const RELEASE_TIMEOUT_FLOOR: Duration = Duration::from_secs(1);
#[cfg(test)]
const SHARED_INVOKE_CONCURRENCY: usize = crate::redis_support::INVOKE_CONCURRENCY;

// Keep fresh-connection retries below a small fixed concurrency cap; the
// shared manager's first attempt remains available to every release.
static RELEASE_RETRY_SEMAPHORE: OnceLock<Semaphore> = OnceLock::new();

struct SharedInvokeGuard {
    generation: u64,
    recovery: Arc<SharedRecovery>,
    completed: bool,
}

// The caller owns only the result wait. Its timeout is a latency budget; the
// owned invoke has a longer liveness deadline. If that deadline expires, this
// guard deliberately drops the Redis future and retires the generation before
// it can be reused. Ordinary caller cancellation never reaches this guard
// because the invoke task remains alive until its own deadline.
impl SharedInvokeGuard {
    fn new(generation: u64, recovery: Arc<SharedRecovery>) -> Self {
        Self {
            generation,
            recovery,
            completed: false,
        }
    }

    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for SharedInvokeGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.recovery
                .suspect_generation
                .fetch_max(self.generation, Ordering::AcqRel);
            self.recovery.request_replacement();
        }
    }
}

fn result_is_untrusted(recovery: &SharedRecovery, generation: u64) -> bool {
    let current_generation = recovery.connection.load_full().generation;
    generation != current_generation
        || recovery.suspect_generation.load(Ordering::Acquire) >= generation
}

fn compensate_abandoned_result(result: &OwnedAcquireResult, result_untrusted: bool) -> bool {
    matches!(result, Ok(Ok((1, _)))) || (result_untrusted && matches!(result, Ok(Ok(_))))
}

fn result_has_mismatched_lease_id(result: &OwnedAcquireResult, lease_id: &str) -> bool {
    matches!(result, Ok(Ok((_, echoed_lease_id))) if echoed_lease_id != lease_id)
}

fn mark_mismatched_result(
    recovery: &SharedRecovery,
    generation: u64,
    result: &OwnedAcquireResult,
    lease_id: &str,
) -> bool {
    if !result_has_mismatched_lease_id(result, lease_id) {
        return false;
    }
    recovery
        .suspect_generation
        .fetch_max(generation, Ordering::AcqRel);
    recovery.request_replacement();
    true
}

fn should_compensate_abandoned_send(result: &OwnedAcquireResult, result_untrusted: bool) -> bool {
    compensate_abandoned_result(result, result_untrusted)
}

#[derive(Clone)]
pub(crate) struct RedisRelease {
    connection: SharedConnection,
    client: Client,
    script: Script,
    key: String,
    lease_id: String,
    timeout: Duration,
    lease_ttl: Duration,
    retry_semaphore: &'static Semaphore,
    recovery: Arc<SharedRecovery>,
    ceilings: Ceilings,
    generation: Option<PolicyGeneration>,
}

impl RedisRelease {
    /// A lease outlives the caller that asked for it: an acquire that overran
    /// its caller's wait may have written the key, and the key is only gone once
    /// this compensation lands. So the release counts a hold of its own against
    /// the admitting generation — taken here rather than in the spawned task, so
    /// there is no window between the caller's hold going away and this one
    /// starting — and an operator watching the drain list sees the generation
    /// stay busy until nothing it admitted is left in the store.
    pub(crate) fn spawn(self) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::warn!("rate-limit permit dropped without a Tokio runtime; lease will expire");
            return;
        };
        let hold = PolicyHold::take(&self.ceilings, self.generation);
        handle.spawn(async move {
            let _hold = hold;
            let release_budget = release_timeout(self.timeout);
            // This fixed margin covers each attempt's bounded connect and invoke
            // plus capped exponential backoff; the TTL cap makes later retries pointless.
            let retry_window = release_budget
                .saturating_mul(
                    RELEASE_RETRY_WINDOW_MULTIPLIER.saturating_mul(RELEASE_MAX_ATTEMPTS as u32),
                )
                .min(self.lease_ttl);
            let deadline = tokio::time::Instant::now() + retry_window;
            let current = self.recovery.connection.load_full();
            let suspect_generation = self.recovery.suspect_generation.load(Ordering::Acquire);
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
                    if let Ok(_invoke_permit) =
                        self.recovery.invoke_semaphore.clone().try_acquire_owned()
                    {
                        let mut invoke_guard =
                            SharedInvokeGuard::new(shared_generation, self.recovery.clone());
                        let result = tokio::time::timeout(
                            release_budget.min(remaining),
                            Self::invoke(&self.script, &self.key, &self.lease_id, &mut connection),
                        )
                        .await;
                        if result.is_ok() {
                            invoke_guard.complete();
                        }
                        drop(invoke_guard);
                        if matches!(result, Ok(Ok(_))) {
                            let current = self.recovery.connection.load_full();
                            if current.generation == shared_generation
                                && self.recovery.suspect_generation.load(Ordering::Acquire)
                                    < shared_generation
                            {
                                return;
                            }
                        }
                        tracing::debug!(
                            "rate-limit lease release failed; retrying on a fresh connection"
                        );
                    } else {
                        tracing::debug!(
                            "rate-limit shared release skipped because invoke cap was exhausted"
                        );
                    }
                }
            }

            for attempt in 2..=RELEASE_MAX_ATTEMPTS {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let backoff = release_budget
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
                    release_budget.min(remaining),
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
                    release_budget.min(remaining),
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

fn release_timeout(admission_timeout: Duration) -> Duration {
    // Permit drops have no caller waiting on them, so tolerate four times the
    // configured admission budget, with a one-second floor for ordinary Redis
    // latency. This budget bounds shared and fresh attempts; retry deadlines
    // and lease TTL still bound total release effort.
    admission_timeout
        .saturating_mul(REDIS_OPERATION_TIMEOUT_MULTIPLIER)
        .max(RELEASE_TIMEOUT_FLOOR)
}

fn invoke_timeout(admission_timeout: Duration) -> Duration {
    crate::redis_support::operation_liveness_timeout(admission_timeout)
}

type OwnedAcquireResult = Result<redis::RedisResult<(i64, String)>, tokio::time::error::Elapsed>;

struct TimedOutAcquire {
    result: Option<OwnedAcquireResult>,
    compensate: bool,
}

fn reclaim_timed_out_acquire(
    receiver: &mut oneshot::Receiver<OwnedAcquireResult>,
    // The production caller passes a no-op; the hook deterministically models
    // a send landing between the first probe and receiver close in tests.
    before_close: impl FnOnce(),
) -> TimedOutAcquire {
    match receiver.try_recv() {
        Ok(result) => TimedOutAcquire {
            compensate: matches!(result, Ok(Ok((1, _)))),
            result: Some(result),
        },
        Err(oneshot::error::TryRecvError::Empty) => {
            before_close();
            receiver.close();
            match receiver.try_recv() {
                Ok(result) => TimedOutAcquire {
                    compensate: matches!(result, Ok(Ok((1, _)))),
                    result: Some(result),
                },
                Err(oneshot::error::TryRecvError::Empty)
                | Err(oneshot::error::TryRecvError::Closed) => TimedOutAcquire {
                    result: None,
                    compensate: false,
                },
            }
        }
        Err(oneshot::error::TryRecvError::Closed) => TimedOutAcquire {
            result: None,
            compensate: true,
        },
    }
}

pub struct RedisRateLimiter {
    key_prefix: String,
    /// Where the enforced limit and lease TTL are read, once per acquisition.
    /// Bootstrap values until a control plane publishes over them (#150).
    ceilings: Ceilings,
    timeout: Duration,
    on_unavailable: StoreUnavailable,
    connection: Arc<ArcSwap<SharedConnection>>,
    client: Client,
    acquire: Script,
    release: Script,
    retry_semaphore: &'static Semaphore,
    suspect_generation: Arc<AtomicU64>,
    recovery: Arc<SharedRecovery>,
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
            let mut connection = ConnectionManager::new_with_config(
                client,
                crate::redis_support::connection_manager_config(),
            )
            .await?;
            redis::cmd("PING")
                .query_async::<String>(&mut connection)
                .await?;
            Ok::<_, redis::RedisError>(connection)
        })
        .await
        .map_err(|_| RedisConnectError::Timeout {
            timeout: connect_timeout,
        })??;
        let connection = Arc::new(ArcSwap::from_pointee(SharedConnection {
            manager: connection,
            generation: 1,
        }));
        let recovery =
            SharedRecovery::new(connection.clone(), release_client.clone(), connect_timeout);
        let suspect_generation = recovery.suspect_generation.clone();
        Ok(Self {
            key_prefix,
            ceilings: Ceilings::fixed(ActivePolicy {
                budget: None,
                concurrency: Some(ConcurrencyCaps {
                    max_in_flight_per_subject: max_in_flight as u64,
                    lease_ttl,
                }),
                generation: None,
            }),
            timeout,
            on_unavailable,
            connection,
            client: release_client,
            acquire: Script::new(ACQUIRE),
            release: Script::new(RELEASE),
            retry_semaphore: release_retry_semaphore(),
            suspect_generation,
            recovery,
        })
    }

    /// Read the published limits instead of the bootstrap file's.
    #[must_use]
    pub fn reading(mut self, ceilings: Ceilings) -> Self {
        self.ceilings = ceilings;
        self
    }

    fn key(&self, key: &RateLimitKey) -> String {
        lease_key(&self.key_prefix, key)
    }

    /// The compensating release for a lease, carrying the TTL that lease was
    /// granted under: the retry window is capped by it, and a publication that
    /// shortens the TTL must not shorten the window of a lease already held.
    fn release(
        &self,
        key: String,
        lease_id: String,
        lease_ttl: Duration,
        generation: Option<PolicyGeneration>,
    ) -> RedisRelease {
        RedisRelease {
            connection: self.connection.load_full().as_ref().clone(),
            client: self.client.clone(),
            script: self.release.clone(),
            key,
            lease_id,
            timeout: self.timeout,
            lease_ttl,
            retry_semaphore: self.retry_semaphore,
            recovery: self.recovery.clone(),
            ceilings: self.ceilings.clone(),
            generation,
        }
    }

    fn schedule_replacement(&self, generation: u64) {
        self.recovery.schedule_replacement(generation);
    }

    fn mark_connection_suspect(&self, generation: u64) {
        self.suspect_generation
            .fetch_max(generation, Ordering::AcqRel);
        self.schedule_replacement(generation);
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
        // The limits governing this acquisition, read once: a lease admitted here
        // runs to completion on these terms even if a publication lands while it
        // is held, and it is counted against the generation that stated them.
        let active = self.ceilings.active(&key.namespace);
        let Some(caps) = active.concurrency else {
            // Sampled, not per request: the namespace is ungoverned until a
            // publication governs it, so the log would otherwise grow with the
            // traffic being denied rather than with the condition.
            // Named for the responsibility, not for `self.name()`: the spend
            // store is usually Redis too, and a namespace missing a cap and a
            // ceiling is two problems, reported and counted apart.
            if denied(
                Unenforceable::Ungoverned,
                crate::policy::ungoverned::RATE_LIMIT_REDIS,
                &key.namespace,
            ) {
                tracing::warn!(
                    store = crate::policy::ungoverned::RATE_LIMIT_REDIS,
                    namespace = %key.namespace,
                    "no policy governs this namespace, so its concurrency limit cannot be \
                     enforced; denying every request for it until one is published"
                );
            }
            return Err(RateLimitError::StoreUnavailable);
        };
        let snapshot = self.connection.load_full();
        if self.suspect_generation.load(Ordering::Acquire) >= snapshot.generation {
            self.mark_connection_suspect(snapshot.generation);
            return self.unavailable("shared Redis connection is recovering");
        }
        let Some(invoke_permit) = self
            .recovery
            .invoke_semaphore
            .clone()
            .try_acquire_owned()
            .ok()
        else {
            return self.unavailable("shared Redis invoke cap is exhausted");
        };
        // Counted before the round-trip, and moved into the permit if one is
        // granted: an operator waits on the drain list before a stop-the-fleet
        // migration, so it may over-report a lease about to be denied but never
        // miss one admitted while a publication landed. Every other path drops
        // the guard.
        let hold = PolicyHold::take(&self.ceilings, active.generation);
        let lease_id = next_id();
        let lease_key = self.key(key);
        let connection_generation = snapshot.generation;
        let release = self.release(
            lease_key.clone(),
            lease_id.clone(),
            caps.lease_ttl,
            active.generation,
        );
        let abandoned_release = release.clone();
        // The spawned invoke may write the key after its caller has given up, so
        // the generation stays counted for as long as that invoke can still
        // create a lease; the compensating release then takes over the count.
        let invoke_hold = PolicyHold::take(&self.ceilings, active.generation);
        let (sender, mut receiver) = oneshot::channel();
        let acquire = self.acquire.clone();
        let ttl = caps.lease_ttl;
        let max_in_flight = usize::try_from(caps.max_in_flight_per_subject).unwrap_or(usize::MAX);
        let invoke_lease_id = lease_id.clone();
        // The caller's timeout is a latency budget; the owned invoke gets the
        // longer liveness budget so ordinary slow Redis responses do not
        // retire a healthy shared generation.
        let invoke_timeout = invoke_timeout(self.timeout);
        let recovery = self.recovery.clone();
        tokio::spawn(async move {
            let _invoke_hold = invoke_hold;
            // Swapping the manager only affects future requests. This task
            // keeps its snapshot and consumes the response even if its caller
            // stops waiting.
            let mut connection = snapshot.manager.clone();
            let mut invoke_guard = SharedInvokeGuard::new(connection_generation, recovery.clone());
            let result: OwnedAcquireResult = tokio::time::timeout(
                invoke_timeout,
                acquire
                    .prepare_invoke()
                    .key(&lease_key)
                    .arg(now_ms())
                    .arg(ttl.as_millis() as u64)
                    .arg(max_in_flight)
                    .arg(&invoke_lease_id)
                    .invoke_async::<(i64, String)>(&mut connection),
            )
            .await;
            if result.is_ok() {
                invoke_guard.complete();
            }
            drop(invoke_guard);
            let definite_denial = matches!(result, Ok(Ok((0, _))));
            let definitely_created = matches!(result, Ok(Ok((1, _))));
            let ambiguous = !definite_denial && !definitely_created;
            let result_untrusted = result_is_untrusted(&recovery, connection_generation);
            let mismatched_lease_id =
                mark_mismatched_result(&recovery, connection_generation, &result, &invoke_lease_id);
            if ambiguous {
                abandoned_release.clone().spawn();
            }
            let compensate_on_send_failure =
                mismatched_lease_id || should_compensate_abandoned_send(&result, result_untrusted);
            if sender.send(result).is_err() && compensate_on_send_failure {
                abandoned_release.spawn();
            }
            drop(invoke_permit);
        });
        let result: Result<_, ()> = match tokio::time::timeout(self.timeout, &mut receiver).await {
            Ok(result) => Ok(result),
            Err(_) => {
                let reclaimed = reclaim_timed_out_acquire(&mut receiver, || {});
                if let Some(result) = reclaimed.result {
                    let mismatched_lease_id = mark_mismatched_result(
                        &self.recovery,
                        connection_generation,
                        &result,
                        &lease_id,
                    );
                    if mismatched_lease_id {
                        release.spawn();
                        return self
                            .unavailable("shared Redis response echoed a different lease id");
                    }
                    let result_untrusted =
                        result_is_untrusted(&self.recovery, connection_generation);
                    let compensate = reclaimed.compensate
                        || compensate_abandoned_result(&result, result_untrusted);
                    if compensate {
                        release.spawn();
                    }
                    if result_untrusted {
                        return self
                            .unavailable("shared Redis connection was replaced during acquire");
                    }
                    return match result {
                        Ok(Ok((1, _))) => {
                            self.unavailable("acquire completed after its caller wait expired")
                        }
                        Ok(Ok(_)) => {
                            metrics::record_rate_limit_denial();
                            Err(RateLimitError::Exceeded)
                        }
                        Ok(Err(error)) => self.unavailable(error),
                        Err(_) => self.unavailable("operation timed out"),
                    };
                }
                if reclaimed.compensate {
                    release.spawn();
                }
                return self.unavailable("operation timed out");
            }
        };
        let result_untrusted = result_is_untrusted(&self.recovery, connection_generation);
        let mismatched_lease_id = if let Ok(Ok(owned_result)) = &result {
            mark_mismatched_result(
                &self.recovery,
                connection_generation,
                owned_result,
                &lease_id,
            )
        } else {
            false
        };
        if mismatched_lease_id {
            release.spawn();
            return self.unavailable("shared Redis response echoed a different lease id");
        }
        if result_untrusted && matches!(result, Ok(Ok(Ok(Ok(_))))) {
            release.spawn();
            return self.unavailable("shared Redis connection was replaced during acquire");
        }
        match result {
            Ok(Ok(Ok(Ok((1, _))))) => Ok(RateLimitPermit {
                release: Some(PermitRelease::Redis(Box::new(release))),
                hold: Some(hold),
            }),
            Ok(Ok(Ok(Ok(_)))) => {
                metrics::record_rate_limit_denial();
                Err(RateLimitError::Exceeded)
            }
            Ok(Ok(Ok(Err(error)))) => self.unavailable(error),
            Ok(Ok(Err(_))) | Err(_) => self.unavailable("operation timed out"),
            Ok(Err(_)) => {
                // A dropped sender means the task is gone; its invoke may
                // already have executed HSET, so no other path can clean up.
                release.spawn();
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
        atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering},
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

    #[derive(Clone)]
    struct RetryReleaseState {
        released_lease: Arc<Mutex<Option<Vec<u8>>>>,
        release_connection: Arc<AtomicUsize>,
        shared_release_commands: Arc<AtomicUsize>,
        block_replacement: Arc<AtomicBool>,
        stall_shared_release: Arc<AtomicBool>,
    }

    struct RedisRetryStub {
        address: SocketAddr,
        connections: Arc<AtomicUsize>,
        released_lease: Arc<Mutex<Option<Vec<u8>>>>,
        release_connection: Arc<AtomicUsize>,
        shared_release_commands: Arc<AtomicUsize>,
        stall_shared_release: Arc<AtomicBool>,
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
            let block_replacement = Arc::new(AtomicBool::new(false));
            let stall_shared_release = Arc::new(AtomicBool::new(false));
            let release_state = RetryReleaseState {
                released_lease: released_lease.clone(),
                release_connection: release_connection.clone(),
                shared_release_commands: shared_release_commands.clone(),
                block_replacement: block_replacement.clone(),
                stall_shared_release: stall_shared_release.clone(),
            };
            let task = {
                let connections = connections.clone();
                let stalled_lease = stalled_lease.clone();
                let release_state = release_state.clone();
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
                            release_state.clone(),
                        ));
                    }
                })
            };
            Self {
                address,
                connections,
                released_lease,
                release_connection,
                shared_release_commands,
                stall_shared_release,
                task,
            }
        }

        fn url(&self) -> String {
            format!("redis://{}/", self.address)
        }

        fn stall_shared_release(&self) {
            self.stall_shared_release.store(true, Ordering::Relaxed);
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

    struct RedisAlignmentStub {
        address: SocketAddr,
        commands: Arc<AtomicUsize>,
        task: JoinHandle<()>,
    }

    impl RedisAlignmentStub {
        async fn start() -> Self {
            let listener = TcpListener::bind(("127.0.0.1", 0))
                .await
                .expect("bind alignment stub");
            let address = listener.local_addr().expect("alignment stub address");
            let commands = Arc::new(AtomicUsize::new(0));
            let task = {
                let commands = commands.clone();
                tokio::spawn(async move {
                    loop {
                        let Ok((stream, _)) = listener.accept().await else {
                            break;
                        };
                        tokio::spawn(handle_alignment_connection(stream, commands.clone()));
                    }
                })
            };
            Self {
                address,
                commands,
                task,
            }
        }

        fn url(&self) -> String {
            format!("redis://{}/", self.address)
        }
    }

    impl Drop for RedisAlignmentStub {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn handle_alignment_connection(stream: TcpStream, commands: Arc<AtomicUsize>) {
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        loop {
            let Some(command) = read_resp_command(&mut reader).await else {
                return;
            };
            let name = command.first().map(Vec::as_slice).unwrap_or_default();
            if name.eq_ignore_ascii_case(b"PING") || name.eq_ignore_ascii_case(b"CLIENT") {
                if write_half.write_all(b"+PONG\r\n").await.is_err() {
                    return;
                }
                continue;
            }
            let ordinal = commands.fetch_add(1, Ordering::Relaxed) + 1;
            let response = match ordinal {
                // Model a response dropped by the transport: keep reading
                // commands but never put this response on the wire.
                1 => continue,
                2 => b":111\r\n".as_slice(),
                3 => b":222\r\n".as_slice(),
                _ => b":333\r\n".as_slice(),
            };
            if write_half.write_all(response).await.is_err() {
                return;
            }
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
                let result = acquire_result.load(Ordering::Relaxed);
                let delay = acquire_delay_ms.load(Ordering::Relaxed);
                if delay != 0 {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                let echoed_lease_id = command.get(7).cloned().unwrap_or_default();
                format!(
                    "*2\r\n:{result}\r\n${}\r\n{}\r\n",
                    echoed_lease_id.len(),
                    String::from_utf8_lossy(&echoed_lease_id)
                )
                .into_bytes()
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
        release_state: RetryReleaseState,
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
                if connection > 1 && release_state.block_replacement.load(Ordering::Relaxed) {
                    std::future::pending::<()>().await;
                }
                if write_half.write_all(b"+PONG\r\n").await.is_err() {
                    return;
                }
            } else if stall_acquire && name.eq_ignore_ascii_case(b"EVALSHA") && command.len() >= 8 {
                if let Some(lease_id) = command.get(7) {
                    *stalled_lease.lock().unwrap() = Some(lease_id.clone());
                }
                continue;
            } else if name.eq_ignore_ascii_case(b"EVALSHA") {
                if command.len() == 5 {
                    release_state
                        .shared_release_commands
                        .fetch_add(1, Ordering::Relaxed);
                    if connection == 1 && release_state.stall_shared_release.load(Ordering::Relaxed)
                    {
                        continue;
                    }
                }
                if loaded_release
                    && loaded_hash.as_deref().map(str::as_bytes)
                        == command.get(1).map(Vec::as_slice)
                {
                    if let Some(lease_id) = command.get(4) {
                        *release_state.released_lease.lock().unwrap() = Some(lease_id.clone());
                    }
                    release_state
                        .release_connection
                        .store(connection, Ordering::Relaxed);
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
        let Some(url) = crate::test_services::redis_url() else {
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
        let Some(url) = crate::test_services::redis_url() else {
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
        let Some(url) = crate::test_services::redis_url() else {
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

        relay.set_mode(RelayMode::Cut);
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
    async fn dropped_response_leaves_shared_manager_usable_for_next_acquire() {
        let stub = RedisLiveStub::start().await;
        let limiter = Arc::new(
            RedisRateLimiter::connect(
                &stub.url(),
                format!("axond:test:{}", next_id()),
                1,
                Duration::from_secs(5),
                Duration::from_millis(50),
                Duration::from_millis(50),
                StoreUnavailable::Deny,
            )
            .await
            .expect("connect limiter"),
        );
        let acquire = tokio::spawn({
            let limiter = limiter.clone();
            async move { limiter.acquire(&key()).await }
        });
        while stub.acquires.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
        acquire.abort();
        let _ = acquire.await;
        assert!(limiter.acquire(&key()).await.is_ok());
        assert!(
            stub.acquires.load(Ordering::Relaxed) >= 2,
            "next admission did not use the still-usable shared manager"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn caller_timeout_does_not_retire_a_slow_but_usable_invoke() {
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
        let generation = limiter.connection.load_full().generation;
        let connections = stub.connections.load(Ordering::Relaxed);
        stub.set_acquire_delay(Duration::from_millis(100));

        assert!(matches!(
            limiter.acquire(&key()).await,
            Err(RateLimitError::StoreUnavailable)
        ));
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            limiter.connection.load_full().generation,
            generation,
            "caller timeout retired a generation whose invoke was still within its liveness budget"
        );
        assert_eq!(
            stub.connections.load(Ordering::Relaxed),
            connections,
            "caller timeout triggered an unnecessary replacement"
        );

        stub.set_acquire_delay(Duration::ZERO);
        assert!(
            limiter.acquire(&key()).await.is_ok(),
            "shared connection was not usable after a slow caller timeout"
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
    async fn stalled_shared_release_retires_after_release_budget() {
        let stub = RedisLiveStub::start().await;
        let limiter = RedisRateLimiter::connect(
            &stub.url(),
            format!("axond:test:{}", next_id()),
            1,
            Duration::from_secs(5),
            Duration::from_millis(250),
            Duration::from_millis(25),
            StoreUnavailable::Deny,
        )
        .await
        .expect("connect limiter");
        let permit = limiter.acquire(&key()).await.expect("acquire permit");
        let generation = limiter.connection.load_full().generation;
        let timeout = Duration::from_millis(250);
        let release_budget = release_timeout(timeout);
        assert!(release_budget > timeout);
        stub.set_release_delay(release_budget.saturating_mul(2));
        drop(permit);

        let deadline = tokio::time::Instant::now() + release_budget.saturating_mul(2);
        let release_deadline = tokio::time::Instant::now() + Duration::from_millis(250);
        while stub.releases.load(Ordering::Relaxed) == 0 {
            assert!(
                tokio::time::Instant::now() < release_deadline,
                "shared release did not reach the live stub"
            );
            tokio::task::yield_now().await;
        }
        while limiter.connection.load_full().generation == generation
            && limiter.suspect_generation.load(Ordering::Acquire) < generation
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "stalled shared release was not bounded by its release budget"
            );
            tokio::task::yield_now().await;
        }
        assert!(
            limiter.connection.load_full().generation > generation
                || limiter.suspect_generation.load(Ordering::Acquire) >= generation,
            "stalled shared release did not retire its generation"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropped_shared_acquire_future_keeps_generation_usable() {
        let stub = RedisLiveStub::start().await;
        let limiter = Arc::new(
            RedisRateLimiter::connect(
                &stub.url(),
                format!("axond:test:{}", next_id()),
                1,
                Duration::from_secs(5),
                Duration::from_secs(1),
                Duration::from_millis(50),
                StoreUnavailable::Deny,
            )
            .await
            .expect("connect limiter"),
        );
        let generation = limiter.connection.load_full().generation;
        stub.set_acquire_delay(Duration::from_millis(100));
        let acquire = tokio::spawn({
            let limiter = limiter.clone();
            async move { limiter.acquire(&key()).await }
        });

        let deadline = tokio::time::Instant::now() + Duration::from_millis(250);
        while stub.acquires.load(Ordering::Relaxed) == 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "cancelled acquire did not reach the live stub"
            );
            tokio::task::yield_now().await;
        }
        acquire.abort();
        let _ = acquire.await;

        assert!(
            limiter.suspect_generation.load(Ordering::Acquire) < generation,
            "abandoning the wait retired a still-running generation"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(limiter.acquire(&key()).await.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abandoned_acquire_compensates_a_lease_created_later() {
        let stub = RedisLiveStub::start().await;
        let limiter = Arc::new(
            RedisRateLimiter::connect(
                &stub.url(),
                format!("axond:test:{}", next_id()),
                1,
                Duration::from_secs(5),
                Duration::from_millis(25),
                Duration::from_millis(50),
                StoreUnavailable::Deny,
            )
            .await
            .expect("connect limiter"),
        );
        stub.set_acquire_delay(Duration::from_millis(100));
        let acquire = tokio::spawn({
            let limiter = limiter.clone();
            async move { limiter.acquire(&key()).await }
        });
        while stub.acquires.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
        acquire.abort();
        let _ = acquire.await;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while stub.releases.load(Ordering::Relaxed) == 0 {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shared_invoke_cap_refusal_leaves_connection_usable() {
        let stub = RedisLiveStub::start().await;
        let limiter = RedisRateLimiter::connect(
            &stub.url(),
            format!("axond:test:{}", next_id()),
            1,
            Duration::from_secs(5),
            Duration::from_secs(1),
            Duration::from_millis(50),
            StoreUnavailable::Deny,
        )
        .await
        .expect("connect limiter");
        let mut permits = Vec::with_capacity(SHARED_INVOKE_CONCURRENCY);
        for _ in 0..SHARED_INVOKE_CONCURRENCY {
            permits.push(
                limiter
                    .recovery
                    .invoke_semaphore
                    .clone()
                    .try_acquire_owned()
                    .expect("invoke cap permit"),
            );
        }
        let result = tokio::time::timeout(Duration::from_millis(100), limiter.acquire(&key()))
            .await
            .expect("cap-exhausted acquire queued behind in-flight work");
        assert!(matches!(result, Err(RateLimitError::StoreUnavailable)));
        drop(permits);
        assert_eq!(
            limiter.suspect_generation.load(Ordering::Acquire),
            0,
            "healthy connection was retired by invoke-cap saturation"
        );
        assert!(
            limiter.acquire(&key()).await.is_ok(),
            "connection did not remain usable after cap refusal"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retired_invoke_reclaims_its_shared_permit() {
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
        let mut held_permits = Vec::with_capacity(SHARED_INVOKE_CONCURRENCY - 1);
        for _ in 0..SHARED_INVOKE_CONCURRENCY - 1 {
            held_permits.push(
                limiter
                    .recovery
                    .invoke_semaphore
                    .clone()
                    .try_acquire_owned()
                    .expect("invoke cap permit"),
            );
        }
        stub.set_acquire_delay(Duration::from_secs(5));
        let generation = limiter.connection.load_full().generation;
        assert!(matches!(
            limiter.acquire(&key()).await,
            Err(RateLimitError::StoreUnavailable)
        ));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let reclaimed = loop {
            if let Ok(permit) = limiter
                .recovery
                .invoke_semaphore
                .clone()
                .try_acquire_owned()
            {
                break Some(permit);
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "retired invoke did not return its semaphore permit"
            );
            tokio::task::yield_now().await;
        };
        assert!(reclaimed.is_some());
        assert!(
            limiter.suspect_generation.load(Ordering::Acquire) >= generation
                || limiter.connection.load_full().generation > generation,
            "invoke deadline did not retire or replace the stalled generation"
        );
        drop(held_permits);
    }

    /// An acquire can outrun the caller that asked for it, so the lease it wrote
    /// is only gone once the compensating release lands. Until then the
    /// generation that admitted it is still counted: an operator draining before
    /// a stop-the-fleet migration is asking whether anything it admitted is
    /// left, not merely whether anyone is still waiting.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_compensating_release_keeps_its_generation_counted_until_the_lease_is_gone() {
        use crate::desired_state::fixtures::tenant_id;
        use crate::desired_state::policy::PolicyScope;
        use crate::policy::PolicyRuntime;
        use crate::policy::fixtures::{body, generation as policy_generation};
        use crate::policy::view::tests::stateless_config;

        let stub = RedisRetryStub::start().await;
        let runtime = Arc::new(PolicyRuntime::bootstrap(&stateless_config()));
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
        .expect("connect limiter")
        .reading(Ceilings::published(&runtime));
        let admitted = policy_generation(&body(PolicyScope::Tenant(tenant_id(1)), 1, 10), 1);

        stub.stall_shared_release();
        let release = limiter.release(
            "rate-limit-key".into(),
            "lease-id".into(),
            Duration::from_secs(60),
            Some(admitted),
        );
        release.spawn();
        assert_eq!(
            runtime.outstanding(admitted),
            1,
            "the hold is taken as the release is spawned, not once it is polled"
        );

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while runtime.outstanding(admitted) > 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the generation stayed counted after its lease was removed"
            );
            tokio::task::yield_now().await;
        }
        assert!(
            stub.release_connection.load(Ordering::Acquire) > 0,
            "the count was dropped before the lease was released"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stalled_shared_release_falls_through_to_fresh_retry() {
        let stub = RedisRetryStub::start().await;
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
        stub.stall_shared_release();
        let release = limiter.release(
            "rate-limit-key".into(),
            "lease-id".into(),
            Duration::from_secs(60),
            None,
        );
        let started = tokio::time::Instant::now();
        release.spawn();
        let deadline = started + Duration::from_millis(1500);
        while stub.connections.load(Ordering::Relaxed) < 2 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "bounded shared release did not reach a fresh retry connection"
            );
            tokio::task::yield_now().await;
        }
        assert_eq!(
            stub.shared_release_commands.load(Ordering::Relaxed),
            1,
            "shared release was not attempted exactly once before fresh retry"
        );
        while stub.release_connection.load(Ordering::Acquire) == 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "fresh retry did not release the lease"
            );
            tokio::task::yield_now().await;
        }
        assert!(
            stub.release_connection.load(Ordering::Relaxed) >= 2,
            "lease was not released by a fresh connection"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slow_shared_release_stays_within_release_budget() {
        let stub = RedisLiveStub::start().await;
        let limiter = RedisRateLimiter::connect(
            &stub.url(),
            format!("axond:test:{}", next_id()),
            1,
            Duration::from_secs(5),
            Duration::from_millis(250),
            Duration::from_millis(50),
            StoreUnavailable::Deny,
        )
        .await
        .expect("connect limiter");
        let permit = limiter.acquire(&key()).await.expect("acquire permit");
        let generation = limiter.connection.load_full().generation;
        let timeout = Duration::from_millis(250);
        assert!(release_timeout(timeout) > timeout);
        stub.set_release_delay(timeout.saturating_mul(2));
        drop(permit);

        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        while stub.releases.load(Ordering::Relaxed) == 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "slow release did not reach the live stub"
            );
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(timeout.saturating_mul(3)).await;
        assert!(
            limiter.suspect_generation.load(Ordering::Acquire) < generation,
            "ordinary slow release retired a healthy generation"
        );
        assert_eq!(
            limiter.connection.load_full().generation,
            generation,
            "ordinary slow release triggered an unnecessary replacement"
        );
        assert!(limiter.acquire(&key()).await.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropped_waiter_misaligns_following_shared_response() {
        let stub = RedisAlignmentStub::start().await;
        let client = redis::Client::open(stub.url()).expect("open client");
        let mut connection = ConnectionManager::new(client)
            .await
            .expect("connect manager");
        redis::cmd("PING")
            .query_async::<String>(&mut connection)
            .await
            .expect("initial ping");

        let first = tokio::time::timeout(
            Duration::from_millis(10),
            redis::cmd("INCR")
                .arg("first")
                .query_async::<i64>(&mut connection),
        )
        .await;
        assert!(first.is_err(), "first waiter unexpectedly completed");

        let second = tokio::spawn({
            let mut connection = connection.clone();
            async move {
                redis::cmd("INCR")
                    .arg("second")
                    .query_async::<i64>(&mut connection)
                    .await
            }
        });
        let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
        while stub.commands.load(Ordering::Relaxed) < 2 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "second command did not reach alignment stub"
            );
            tokio::task::yield_now().await;
        }
        let third = tokio::time::timeout(
            Duration::from_millis(100),
            redis::cmd("INCR")
                .arg("third")
                .query_async::<i64>(&mut connection),
        )
        .await;
        assert!(third.is_err(), "third waiter unexpectedly completed");
        let second = second
            .await
            .expect("second request task")
            .expect("second request response");
        assert_eq!(
            second, 222,
            "second waiter did not receive the third request's reply"
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shared_invoke_cap_refusal_does_not_poison_or_retry_release() {
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
        let mut invoke_permits = Vec::with_capacity(SHARED_INVOKE_CONCURRENCY);
        for _ in 0..SHARED_INVOKE_CONCURRENCY {
            invoke_permits.push(
                limiter
                    .recovery
                    .invoke_semaphore
                    .clone()
                    .try_acquire_owned()
                    .expect("invoke cap permit"),
            );
        }
        assert!(matches!(
            limiter.acquire(&key()).await,
            Err(RateLimitError::StoreUnavailable)
        ));
        let retry_deadline = tokio::time::Instant::now() + Duration::from_millis(300);
        while tokio::time::Instant::now() < retry_deadline {
            assert_eq!(
                stub.connections.load(Ordering::Relaxed),
                1,
                "cap saturation poisoned the healthy manager"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            limiter.suspect_generation.load(Ordering::Acquire),
            0,
            "cap saturation retired a healthy generation"
        );
        assert_eq!(stub.shared_release_commands.load(Ordering::Relaxed), 0);
        assert_eq!(stub.release_connection.load(Ordering::Relaxed), 0);
        assert!(stub.released_lease.lock().unwrap().is_none());
        drop(invoke_permits);
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
    async fn abandoned_zero_and_disconnected_results_follow_compensation_rule() {
        let stub = RedisStub::start(true, false).await;
        let limiter = RedisRateLimiter::connect(
            &stub.url(),
            "test".to_owned(),
            1,
            Duration::from_secs(5),
            Duration::from_millis(50),
            Duration::from_millis(50),
            StoreUnavailable::Deny,
        )
        .await
        .expect("connect limiter");
        let (sender, mut receiver) = oneshot::channel::<OwnedAcquireResult>();
        receiver.close();
        let denial = Ok(Ok((0, "test".to_owned())));
        assert!(sender.send(Ok(Ok((0, "test".to_owned())))).is_err());
        let result_untrusted = result_is_untrusted(&limiter.recovery, 1);
        assert!(
            !result_untrusted,
            "a healthy current generation must remain trusted"
        );
        assert!(
            !should_compensate_abandoned_send(&denial, result_untrusted),
            "a trusted definite denial proves no lease was created"
        );
        assert!(
            compensate_abandoned_result(&denial, true),
            "a denial from a retired generation is unattributable and must compensate"
        );
        assert!(
            !should_compensate_abandoned_send(&denial, false),
            "a trusted denial sent to an abandoned wait must not compensate"
        );
        let mismatch = Ok(Ok((1, "someone-else".to_owned())));
        assert!(mark_mismatched_result(
            &limiter.recovery,
            1,
            &mismatch,
            "ours"
        ));
        assert!(
            limiter.suspect_generation.load(Ordering::Acquire) >= 1,
            "a mismatched echo must retire the reply generation"
        );
        assert!(
            compensate_abandoned_result(&mismatch, true),
            "a mismatched grant must be compensated"
        );
    }

    #[test]
    fn timeout_handoff_distinguishes_inflight_dead_and_window_grants() {
        let (sender, mut receiver) = oneshot::channel::<OwnedAcquireResult>();
        let reclaimed = reclaim_timed_out_acquire(&mut receiver, || {});
        assert!(!reclaimed.compensate);
        assert!(reclaimed.result.is_none());
        assert!(
            sender.send(Ok(Ok((1, "test".to_owned())))).is_err(),
            "the in-flight sender must be stopped after the handoff closes the receiver"
        );

        let (sender, mut receiver) = oneshot::channel::<OwnedAcquireResult>();
        drop(sender);
        let reclaimed = reclaim_timed_out_acquire(&mut receiver, || {});
        assert!(reclaimed.compensate);
        assert!(reclaimed.result.is_none());

        let (sender, mut receiver) = oneshot::channel::<OwnedAcquireResult>();
        let reclaimed = reclaim_timed_out_acquire(&mut receiver, || {
            sender
                .send(Ok(Ok((1, "test".to_owned()))))
                .expect("sender is alive between the two probes");
        });
        assert!(reclaimed.compensate);
        assert!(matches!(&reclaimed.result, Some(Ok(Ok((1, _))))));
        assert!(
            reclaimed.result.is_some(),
            "a grant sent in the probe/close window must be reclaimed"
        );
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
        let Some(url) = crate::test_services::redis_url() else {
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
