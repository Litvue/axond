use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use redis::Client;
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use tokio::sync::{Notify, Semaphore};

const REPLACEMENT_COOLDOWN: Duration = Duration::from_millis(250);
const OPERATION_LIVENESS_MULTIPLIER: u32 = 4;
const OPERATION_LIVENESS_FLOOR: Duration = Duration::from_millis(500);
// Each RedisRecovery owns this cap per store. On the revocation path it bounds
// stalled owned lookups and writes to 1,024 pending operations before only the
// current request is shed. It is a safety ceiling, not a throughput promise,
// and saturation is not evidence that the socket failed.
pub(crate) const INVOKE_CONCURRENCY: usize = 1024;

pub(crate) fn operation_liveness_timeout(admission_timeout: Duration) -> Duration {
    admission_timeout
        .saturating_mul(OPERATION_LIVENESS_MULTIPLIER)
        .max(OPERATION_LIVENESS_FLOOR)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_millis().min(u64::MAX as u128) as u64
        })
}

#[derive(Clone)]
pub(crate) struct RedisConnection {
    pub(crate) manager: ConnectionManager,
    pub(crate) generation: u64,
}

pub(crate) struct RedisRecovery {
    pub(crate) connection: Arc<ArcSwap<RedisConnection>>,
    client: Client,
    connect_timeout: Duration,
    pub(crate) suspect_generation: Arc<AtomicU64>,
    pub(crate) replacement_in_flight: Arc<AtomicBool>,
    last_replacement_ms: Arc<AtomicU64>,
    pub(crate) invoke_semaphore: Arc<Semaphore>,
    replacement_notify: Arc<Notify>,
}

impl Drop for RedisRecovery {
    fn drop(&mut self) {
        self.replacement_notify.notify_one();
    }
}

impl RedisRecovery {
    pub(crate) fn new(
        connection: Arc<ArcSwap<RedisConnection>>,
        client: Client,
        connect_timeout: Duration,
    ) -> Arc<Self> {
        let recovery = Arc::new(Self {
            connection,
            client,
            connect_timeout,
            suspect_generation: Arc::new(AtomicU64::new(0)),
            replacement_in_flight: Arc::new(AtomicBool::new(false)),
            last_replacement_ms: Arc::new(AtomicU64::new(0)),
            invoke_semaphore: Arc::new(Semaphore::new(INVOKE_CONCURRENCY)),
            replacement_notify: Arc::new(Notify::new()),
        });
        tokio::spawn(Self::replacement_worker(Arc::downgrade(&recovery)));
        recovery
    }

    pub(crate) fn request_replacement(&self) {
        self.replacement_notify.notify_one();
    }

    pub(crate) fn mark_suspect(&self, generation: u64) {
        self.suspect_generation
            .fetch_max(generation, Ordering::AcqRel);
        self.request_replacement();
    }

    pub(crate) fn schedule_replacement(&self, generation: u64) {
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
            if previous != 0 {
                let elapsed = now_ms().saturating_sub(previous);
                if elapsed < REPLACEMENT_COOLDOWN.as_millis() as u64 {
                    tokio::time::sleep(
                        REPLACEMENT_COOLDOWN.saturating_sub(Duration::from_millis(elapsed)),
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
            last_replacement_ms.store(now_ms(), Ordering::Relaxed);
            let replacement = tokio::time::timeout(connect_timeout, async {
                let mut manager =
                    ConnectionManager::new_with_config(client, connection_manager_config()).await?;
                redis::cmd("PING")
                    .query_async::<String>(&mut manager)
                    .await?;
                Ok::<_, redis::RedisError>(manager)
            })
            .await;
            if let Ok(Ok(manager)) = replacement {
                let current = connection.load_full();
                if current.generation == generation
                    && suspect_generation.load(Ordering::Acquire) == generation
                {
                    connection.store(Arc::new(RedisConnection {
                        manager,
                        generation: generation + 1,
                    }));
                    suspect_generation.store(0, Ordering::Release);
                }
            }
            replacement_in_flight.store(false, Ordering::Release);
        });
    }

    async fn replacement_worker(worker: Weak<Self>) {
        loop {
            let notify = match worker.upgrade() {
                Some(recovery) => recovery.replacement_notify.clone(),
                None => return,
            };
            notify.notified().await;
            let Some(recovery) = worker.upgrade() else {
                return;
            };
            let generation = recovery.connection.load_full().generation;
            recovery.schedule_replacement(generation);
        }
    }
}

pub(crate) fn connection_manager_config() -> ConnectionManagerConfig {
    // redis-rs 1.4.1 defaults response_timeout to Some(500 ms)
    // (src/client.rs::DEFAULT_RESPONSE_TIMEOUT). Its internal cancellation
    // can drop a multiplexed waiter and misalign later replies, so callers
    // own the wait and liveness deadlines and keep the manager's response
    // timeout off.
    ConnectionManagerConfig::new().set_response_timeout(None)
}

/// Reachability of a Redis-backed request-path store, on the store's own
/// multiplexed connection.
///
/// Redis multiplexes, so a `PING` shares the socket every request-path command
/// uses without queueing in front of one. That is the property that makes this
/// the honest diagnostic rather than a second opinion: it observes the
/// connection whose loss is what an operator is trying to confirm. The
/// connection is *loaded* per check rather than captured once, so a check after
/// a recovery replacement uses the live one.
pub(crate) struct RedisHealth {
    bound: Duration,
    /// The current manager. A closure because the three stores hold their
    /// connection differently — one owns a self-reconnecting manager, two swap
    /// a shared cell on replacement — and the diagnostic must not fork that.
    connection: Box<dyn Fn() -> ConnectionManager + Send + Sync>,
}

impl RedisHealth {
    pub(crate) fn new(
        bound: Duration,
        connection: impl Fn() -> ConnectionManager + Send + Sync + 'static,
    ) -> Self {
        Self {
            bound,
            connection: Box::new(connection),
        }
    }
}

#[async_trait::async_trait]
impl crate::backends::health::BackendHealth for RedisHealth {
    fn backend(&self) -> &'static str {
        "redis"
    }

    fn bound(&self) -> Duration {
        self.bound
    }

    async fn check(&self) -> Result<(), crate::backends::health::HealthFailure> {
        let mut connection = (self.connection)();
        redis::cmd("PING")
            .query_async::<String>(&mut connection)
            .await
            .map(|_| ())
            .map_err(|error| {
                // A `NOAUTH`/`WRONGPASS` refusal is not an outage: the server
                // answered. Separated so it reaches whoever owns the credential
                // instead of whoever owns Redis.
                let category = if error.kind() == redis::ErrorKind::AuthenticationFailed {
                    crate::backends::FailureCategory::Denied
                } else {
                    crate::backends::FailureCategory::Unavailable
                };
                crate::backends::health::HealthFailure::new(category, error.to_string())
            })
    }
}

#[cfg(test)]
mod tests {
    use super::connection_manager_config;

    #[test]
    fn manager_does_not_add_a_library_response_timeout() {
        assert_eq!(connection_manager_config().response_timeout(), None);
    }
}
