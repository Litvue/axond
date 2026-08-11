use std::time::{Duration, SystemTime};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use redis::aio::ConnectionManager;
use std::sync::Arc;

use super::{RevocationError, RevocationStore, expiry_ms, unavailable, validate_expiry};
use crate::config::StoreUnavailable;
use crate::redis_support::{RedisConnection, RedisRecovery, operation_liveness_timeout};

struct RevocationInvokeGuard {
    generation: u64,
    recovery: Arc<RedisRecovery>,
    completed: bool,
}

impl RevocationInvokeGuard {
    fn new(generation: u64, recovery: Arc<RedisRecovery>) -> Self {
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

impl Drop for RevocationInvokeGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.recovery.mark_suspect(self.generation);
        }
    }
}

pub struct RedisRevocation {
    connection: Arc<ArcSwap<RedisConnection>>,
    recovery: Arc<RedisRecovery>,
    key_prefix: String,
    timeout: Duration,
    on_unavailable: StoreUnavailable,
}

impl RedisRevocation {
    pub async fn connect(
        url: &str,
        key_prefix: &str,
        timeout: Duration,
        connect_timeout: Duration,
        on_unavailable: StoreUnavailable,
    ) -> Result<Self, RevocationError> {
        let client = redis::Client::open(url)
            .map_err(|e| RevocationError::Invalid(format!("unusable URL: {e}")))?;
        let recovery_client = client.clone();
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
        .map_err(|_| RevocationError::Invalid("Redis connection timed out".to_owned()))?
        .map_err(|e| RevocationError::Invalid(format!("Redis connection failed: {e}")))?;
        let connection = Arc::new(ArcSwap::from_pointee(RedisConnection {
            manager: connection,
            generation: 1,
        }));
        let recovery = RedisRecovery::new(connection.clone(), recovery_client, connect_timeout);
        Ok(Self {
            connection,
            recovery,
            key_prefix: key_prefix.to_owned(),
            timeout,
            on_unavailable,
        })
    }

    fn key(&self, jti: &str) -> String {
        format!("{}:{{{jti}}}", self.key_prefix)
    }
}

#[async_trait]
impl RevocationStore for RedisRevocation {
    fn name(&self) -> &'static str {
        "redis"
    }

    async fn is_revoked(&self, jti: &str) -> Result<bool, RevocationError> {
        let snapshot = self.connection.load_full();
        if self
            .recovery
            .suspect_generation
            .load(std::sync::atomic::Ordering::Acquire)
            >= snapshot.generation
        {
            self.recovery.mark_suspect(snapshot.generation);
            return unavailable(
                self.on_unavailable,
                "redis",
                "shared Redis connection is recovering",
            );
        }
        let key = self.key(jti);
        let timeout = self.timeout;
        let liveness_timeout = operation_liveness_timeout(timeout);
        let recovery = self.recovery.clone();
        let task_recovery = recovery.clone();
        let generation = snapshot.generation;
        let connection = snapshot.manager.clone();
        let Some(invoke_permit) = self
            .recovery
            .invoke_semaphore
            .clone()
            .try_acquire_owned()
            .ok()
        else {
            return unavailable(
                self.on_unavailable,
                "redis",
                "redis invoke cap is exhausted",
            );
        };
        // Keep the Redis future in an owned task to remove caller-side
        // cancellation. The request deadline only ends the caller's wait;
        // the longer liveness deadline owns future cancellation, and the
        // shared recovery retires that generation if it fires.
        let task = tokio::spawn(async move {
            let mut connection = connection;
            let mut invoke_guard = RevocationInvokeGuard::new(generation, task_recovery);
            let result = tokio::time::timeout(liveness_timeout, async move {
                redis::cmd("EXISTS")
                    .arg(key)
                    .query_async::<bool>(&mut connection)
                    .await
            })
            .await;
            if result.is_ok() {
                invoke_guard.complete();
            }
            drop(invoke_permit);
            result
        });
        let result = tokio::time::timeout(timeout, task).await;
        match result {
            Ok(Ok(Ok(Ok(value)))) => {
                if recovery
                    .suspect_generation
                    .load(std::sync::atomic::Ordering::Acquire)
                    >= generation
                    || recovery.connection.load_full().generation != generation
                {
                    return unavailable(
                        self.on_unavailable,
                        "redis",
                        "shared Redis response became unattributable",
                    );
                }
                Ok(value)
            }
            Ok(Ok(Ok(Err(error)))) => unavailable(self.on_unavailable, "redis", error),
            Ok(Ok(Err(_))) | Err(_) => {
                unavailable(self.on_unavailable, "redis", "operation timed out")
            }
            Ok(Err(_)) => unavailable(self.on_unavailable, "redis", "operation failed"),
        }
    }

    async fn revoke(&self, jti: &str, expires_at: SystemTime) -> Result<(), RevocationError> {
        validate_expiry(expires_at)?;
        let expiry_ms = expiry_ms(expires_at)?;
        let snapshot = self.connection.load_full();
        if self
            .recovery
            .suspect_generation
            .load(std::sync::atomic::Ordering::Acquire)
            >= snapshot.generation
        {
            self.recovery.mark_suspect(snapshot.generation);
            return Err(RevocationError::Unavailable {
                backend: "redis",
                message: "shared Redis connection is recovering".to_owned(),
            });
        }
        let key = self.key(jti);
        let timeout = self.timeout;
        let liveness_timeout = operation_liveness_timeout(timeout);
        let recovery = self.recovery.clone();
        let task_recovery = recovery.clone();
        let generation = snapshot.generation;
        let connection = snapshot.manager.clone();
        let Some(invoke_permit) = self
            .recovery
            .invoke_semaphore
            .clone()
            .try_acquire_owned()
            .ok()
        else {
            return Err(RevocationError::Unavailable {
                backend: "redis",
                message: "redis invoke cap is exhausted".to_owned(),
            });
        };
        let task = tokio::spawn(async move {
            let mut connection = connection;
            let mut invoke_guard = RevocationInvokeGuard::new(generation, task_recovery);
            let result = tokio::time::timeout(liveness_timeout, async move {
                redis::cmd("SET")
                    .arg(key)
                    .arg("")
                    .arg("PXAT")
                    .arg(expiry_ms)
                    .query_async::<()>(&mut connection)
                    .await
            })
            .await;
            if result.is_ok() {
                invoke_guard.complete();
            }
            drop(invoke_permit);
            result
        });
        let result = tokio::time::timeout(timeout, task).await;
        match result {
            Ok(Ok(Ok(Ok(())))) => {
                if recovery
                    .suspect_generation
                    .load(std::sync::atomic::Ordering::Acquire)
                    >= generation
                    || recovery.connection.load_full().generation != generation
                {
                    return Err(RevocationError::Unavailable {
                        backend: "redis",
                        message: "shared Redis response became unattributable".to_owned(),
                    });
                }
                Ok(())
            }
            Ok(Ok(Ok(Err(error)))) => Err(RevocationError::Unavailable {
                backend: "redis",
                message: error.to_string(),
            }),
            Ok(Ok(Err(_))) | Err(_) => Err(RevocationError::Unavailable {
                backend: "redis",
                message: "operation timed out".to_owned(),
            }),
            Ok(Err(_)) => Err(RevocationError::Unavailable {
                backend: "redis",
                message: "operation failed".to_owned(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::task::JoinHandle;

    struct RevocationTimeoutStub {
        address: std::net::SocketAddr,
        connections: std::sync::Arc<AtomicUsize>,
        drop_first: std::sync::Arc<AtomicBool>,
        ping_failures: std::sync::Arc<AtomicUsize>,
        response_delay_ms: std::sync::Arc<AtomicU64>,
        task: JoinHandle<()>,
    }

    impl RevocationTimeoutStub {
        async fn start() -> Self {
            let listener = TcpListener::bind(("127.0.0.1", 0))
                .await
                .expect("bind stub");
            let address = listener.local_addr().expect("stub address");
            let connections = std::sync::Arc::new(AtomicUsize::new(0));
            let drop_first = std::sync::Arc::new(AtomicBool::new(true));
            let ping_failures = std::sync::Arc::new(AtomicUsize::new(0));
            let response_delay_ms = std::sync::Arc::new(AtomicU64::new(0));
            let task = {
                let connections = connections.clone();
                let drop_first = drop_first.clone();
                let ping_failures = ping_failures.clone();
                let response_delay_ms = response_delay_ms.clone();
                tokio::spawn(async move {
                    loop {
                        let Ok((stream, _)) = listener.accept().await else {
                            return;
                        };
                        let connection = connections.fetch_add(1, Ordering::Relaxed);
                        tokio::spawn(handle_timeout_connection(
                            stream,
                            connection,
                            drop_first.clone(),
                            ping_failures.clone(),
                            response_delay_ms.clone(),
                        ));
                    }
                })
            };
            Self {
                address,
                connections,
                drop_first,
                ping_failures,
                response_delay_ms,
                task,
            }
        }

        fn url(&self) -> String {
            format!("redis://{}/", self.address)
        }
    }

    impl Drop for RevocationTimeoutStub {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn read_resp_command(
        reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
    ) -> Option<Vec<Vec<u8>>> {
        let mut line = String::new();
        reader.read_line(&mut line).await.ok()?;
        let count = line.strip_prefix('*')?.trim().parse::<usize>().ok()?;
        let mut command = Vec::with_capacity(count);
        for _ in 0..count {
            line.clear();
            reader.read_line(&mut line).await.ok()?;
            let length = line.strip_prefix('$')?.trim().parse::<usize>().ok()?;
            let mut value = vec![0; length];
            reader.read_exact(&mut value).await.ok()?;
            let mut crlf = [0; 2];
            reader.read_exact(&mut crlf).await.ok()?;
            command.push(value);
        }
        Some(command)
    }

    async fn handle_timeout_connection(
        stream: TcpStream,
        connection: usize,
        drop_first: std::sync::Arc<AtomicBool>,
        ping_failures: std::sync::Arc<AtomicUsize>,
        response_delay_ms: std::sync::Arc<AtomicU64>,
    ) {
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        loop {
            let Some(command) = read_resp_command(&mut reader).await else {
                return;
            };
            let name = command.first().map(Vec::as_slice).unwrap_or_default();
            if name.eq_ignore_ascii_case(b"PING") {
                if connection != 0
                    && ping_failures
                        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                            if count > 0 { Some(count - 1) } else { None }
                        })
                        .is_ok()
                {
                    return;
                }
                if write_half.write_all(b"+PONG\r\n").await.is_err() {
                    return;
                }
            } else if name.eq_ignore_ascii_case(b"CLIENT") {
                if write_half.write_all(b"+OK\r\n").await.is_err() {
                    return;
                }
            } else if name.eq_ignore_ascii_case(b"EXISTS") {
                if connection == 0 && drop_first.load(Ordering::Relaxed) {
                    continue;
                }
                if command.iter().any(|part| {
                    part.windows(b"redis-error".len())
                        .any(|window| window == b"redis-error")
                }) {
                    if write_half
                        .write_all(b"-ERR alignment test error\r\n")
                        .await
                        .is_err()
                    {
                        return;
                    }
                    continue;
                }
                let delay = response_delay_ms.load(Ordering::Relaxed);
                if delay != 0 {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                if write_half.write_all(b":1\r\n").await.is_err() {
                    return;
                }
            } else if name.eq_ignore_ascii_case(b"SET") {
                if connection == 0 && drop_first.load(Ordering::Relaxed) {
                    continue;
                }
                let delay = response_delay_ms.load(Ordering::Relaxed);
                if delay != 0 {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                if write_half.write_all(b"+OK\r\n").await.is_err() {
                    return;
                }
            } else if write_half.write_all(b"+OK\r\n").await.is_err() {
                return;
            }
        }
    }

    #[tokio::test]
    async fn two_connections_share_revocations_and_expiry_is_set() {
        let Some(url) = crate::test_services::redis_url() else {
            return;
        };
        let prefix = format!(
            "axond:test:revocation:{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        );
        let first = RedisRevocation::connect(
            &url,
            &prefix,
            Duration::from_millis(250),
            Duration::from_secs(5),
            StoreUnavailable::Deny,
        )
        .await
        .expect("connect");
        let second = RedisRevocation::connect(
            &url,
            &prefix,
            Duration::from_millis(250),
            Duration::from_secs(5),
            StoreUnavailable::Deny,
        )
        .await
        .expect("connect");
        let expiry = SystemTime::now() + Duration::from_secs(30);
        first.revoke("replica-jti", expiry).await.expect("revoke");
        assert!(second.is_revoked("replica-jti").await.expect("read"));
        let mut connection = first.connection.load_full().manager.clone();
        let ttl: i64 = redis::cmd("PTTL")
            .arg(format!("{prefix}:{{replica-jti}}"))
            .query_async(&mut connection)
            .await
            .expect("ttl");
        assert!(ttl > 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn timed_out_operations_retire_generation_before_next_operation() {
        let stub = RevocationTimeoutStub::start().await;
        let store = RedisRevocation::connect(
            &stub.url(),
            "axond:test:revocation-timeout",
            Duration::from_millis(25),
            Duration::from_secs(1),
            StoreUnavailable::Deny,
        )
        .await
        .expect("connect");

        assert!(matches!(
            store.is_revoked("timed-out-jti").await,
            Err(RevocationError::Unavailable { .. })
        ));
        match store.is_revoked("during-replacement").await {
            Ok(value) => assert!(value, "a revoked JTI must not be reported as active"),
            Err(RevocationError::Unavailable { .. }) => {}
            Err(error) => panic!("unexpected replacement error: {error:?}"),
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while stub.connections.load(Ordering::Relaxed) < 2
            || store.connection.load_full().generation == 1
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "replacement did not connect"
            );
            tokio::task::yield_now().await;
        }
        assert!(
            store
                .is_revoked("revoked-jti")
                .await
                .expect("replacement lookup"),
            "a revoked JTI must not be reported as active after timeout recovery"
        );

        let expiry = SystemTime::now() + Duration::from_secs(30);
        let write_stub = RevocationTimeoutStub::start().await;
        let write_store = RedisRevocation::connect(
            &write_stub.url(),
            "axond:test:revocation-write-timeout",
            Duration::from_millis(25),
            Duration::from_secs(1),
            StoreUnavailable::Deny,
        )
        .await
        .expect("connect write store");
        assert!(matches!(
            write_store.revoke("timed-out-write", expiry).await,
            Err(RevocationError::Unavailable { .. })
        ));
        match write_store.revoke("during-write-replacement", expiry).await {
            Ok(()) => assert!(
                write_store.connection.load_full().generation > 1,
                "a write must not succeed from the retired generation"
            ),
            Err(RevocationError::Unavailable { .. }) => {}
            Err(error) => panic!("unexpected write replacement error: {error:?}"),
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while write_stub.connections.load(Ordering::Relaxed) < 2
            || write_store.connection.load_full().generation == 1
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "write replacement did not connect"
            );
            tokio::task::yield_now().await;
        }
        write_store
            .revoke("write-after-replacement", expiry)
            .await
            .expect("replacement write");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn caller_timeout_does_not_retire_a_healthy_generation() {
        let stub = RevocationTimeoutStub::start().await;
        let store = RedisRevocation::connect(
            &stub.url(),
            "axond:test:revocation-slow",
            Duration::from_millis(25),
            Duration::from_secs(1),
            StoreUnavailable::Deny,
        )
        .await
        .expect("connect");
        let generation = store.connection.load_full().generation;
        stub.drop_first.store(false, Ordering::Relaxed);
        stub.response_delay_ms.store(100, Ordering::Relaxed);

        assert!(matches!(
            store.is_revoked("slow-jti").await,
            Err(RevocationError::Unavailable { .. })
        ));
        tokio::time::sleep(Duration::from_millis(125)).await;
        stub.response_delay_ms.store(0, Ordering::Relaxed);
        assert!(store.is_revoked("healthy-jti").await.expect("lookup"));
        assert_eq!(
            store.connection.load_full().generation,
            generation,
            "a caller timeout must not retire a healthy generation"
        );
    }

    #[tokio::test]
    async fn redis_command_errors_preserve_the_backend_message() {
        let stub = RevocationTimeoutStub::start().await;
        let store = RedisRevocation::connect(
            &stub.url(),
            "axond:test:revocation-error",
            Duration::from_millis(25),
            Duration::from_secs(1),
            StoreUnavailable::Deny,
        )
        .await
        .expect("connect");
        stub.drop_first.store(false, Ordering::Relaxed);
        let error = store
            .is_revoked("redis-error")
            .await
            .expect_err("Redis error unexpectedly succeeded");
        assert!(
            error.to_string().contains("alignment test error"),
            "backend error was lost: {error}"
        );
    }

    #[tokio::test]
    async fn panicking_lookup_task_retires_its_generation() {
        let stub = RevocationTimeoutStub::start().await;
        let store = RedisRevocation::connect(
            &stub.url(),
            "axond:test:revocation-panic",
            Duration::from_millis(25),
            Duration::from_secs(1),
            StoreUnavailable::Deny,
        )
        .await
        .expect("connect");
        let generation = store.connection.load_full().generation;
        let recovery = store.recovery.clone();
        let task = tokio::spawn(async move {
            let _guard = RevocationInvokeGuard::new(generation, recovery);
            panic!("alignment test panic");
        });
        assert!(task.await.is_err(), "lookup task unexpectedly succeeded");
        assert_eq!(
            store
                .recovery
                .suspect_generation
                .load(std::sync::atomic::Ordering::Acquire),
            generation
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_replacement_is_retried_by_a_later_lookup() {
        let stub = RevocationTimeoutStub::start().await;
        let store = RedisRevocation::connect(
            &stub.url(),
            "axond:test:revocation-retry",
            Duration::from_millis(25),
            Duration::from_secs(1),
            StoreUnavailable::Deny,
        )
        .await
        .expect("connect");
        assert!(matches!(
            store.is_revoked("timeout-jti").await,
            Err(RevocationError::Unavailable { .. })
        ));
        stub.ping_failures.store(1, Ordering::Relaxed);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while stub.connections.load(Ordering::Relaxed) < 2 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "replacement attempt did not connect"
            );
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(store.connection.load_full().generation, 1);
        stub.ping_failures.store(0, Ordering::Relaxed);
        assert!(matches!(
            store.is_revoked("retry-jti").await,
            Err(RevocationError::Unavailable { .. })
        ));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while store.connection.load_full().generation == 1 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "later lookup did not re-arm replacement"
            );
            tokio::task::yield_now().await;
        }
        assert!(store.is_revoked("recovered-jti").await.expect("lookup"));
    }
}
