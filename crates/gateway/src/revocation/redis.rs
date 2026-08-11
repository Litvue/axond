use std::time::{Duration, SystemTime};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use redis::aio::ConnectionManager;
use std::sync::Arc;

use super::{RevocationError, RevocationStore, expiry_ms, unavailable, validate_expiry};
use crate::config::StoreUnavailable;
use crate::redis_support::{RedisConnection, RedisRecovery};

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
            return unavailable(
                self.on_unavailable,
                "redis",
                "shared Redis connection is recovering",
            );
        }
        let key = self.key(jti);
        let timeout = self.timeout;
        let recovery = self.recovery.clone();
        let task_recovery = recovery.clone();
        let generation = snapshot.generation;
        let connection = snapshot.manager.clone();
        // Keep the Redis future in an owned task to remove caller-side
        // cancellation. Its deadline can still drop the multiplexed future,
        // so the shared recovery retires that generation before reuse.
        let result = tokio::spawn(async move {
            let mut connection = connection;
            let result = tokio::time::timeout(timeout, async move {
                redis::cmd("EXISTS")
                    .arg(key)
                    .query_async::<bool>(&mut connection)
                    .await
            })
            .await;
            if result.is_err() {
                task_recovery.mark_suspect(generation);
            }
            result
        })
        .await;
        match result {
            Ok(Ok(Ok(value))) => {
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
            Ok(Ok(Err(error))) => unavailable(self.on_unavailable, "redis", error),
            Ok(Err(_)) | Err(_) => unavailable(self.on_unavailable, "redis", "operation timed out"),
        }
    }

    async fn revoke(&self, jti: &str, expires_at: SystemTime) -> Result<(), RevocationError> {
        validate_expiry(expires_at)?;
        let expiry_ms = expiry_ms(expires_at)?;
        let key = self.key(jti);
        let timeout = self.timeout;
        let recovery = self.recovery.clone();
        let generation = self.connection.load_full().generation;
        let connection = self.connection.load_full().manager.clone();
        let result = tokio::spawn(async move {
            let mut connection = connection;
            let result = tokio::time::timeout(timeout, async move {
                redis::cmd("SET")
                    .arg(key)
                    .arg("")
                    .arg("PXAT")
                    .arg(expiry_ms)
                    .query_async::<()>(&mut connection)
                    .await
            })
            .await;
            if result.is_err() {
                recovery.mark_suspect(generation);
            }
            result
        })
        .await;
        match result {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(error))) => Err(RevocationError::Unavailable {
                backend: "redis",
                message: error.to_string(),
            }),
            Ok(Err(_)) | Err(_) => Err(RevocationError::Unavailable {
                backend: "redis",
                message: "operation timed out".to_owned(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::task::JoinHandle;

    struct RevocationTimeoutStub {
        address: std::net::SocketAddr,
        connections: std::sync::Arc<AtomicUsize>,
        task: JoinHandle<()>,
    }

    impl RevocationTimeoutStub {
        async fn start() -> Self {
            let listener = TcpListener::bind(("127.0.0.1", 0))
                .await
                .expect("bind stub");
            let address = listener.local_addr().expect("stub address");
            let connections = std::sync::Arc::new(AtomicUsize::new(0));
            let task = {
                let connections = connections.clone();
                tokio::spawn(async move {
                    loop {
                        let Ok((stream, _)) = listener.accept().await else {
                            return;
                        };
                        let connection = connections.fetch_add(1, Ordering::Relaxed);
                        tokio::spawn(handle_timeout_connection(stream, connection));
                    }
                })
            };
            Self {
                address,
                connections,
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

    async fn handle_timeout_connection(stream: TcpStream, connection: usize) {
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        loop {
            let Some(command) = read_resp_command(&mut reader).await else {
                return;
            };
            let name = command.first().map(Vec::as_slice).unwrap_or_default();
            if name.eq_ignore_ascii_case(b"PING") {
                if write_half.write_all(b"+PONG\r\n").await.is_err() {
                    return;
                }
            } else if name.eq_ignore_ascii_case(b"CLIENT") {
                if write_half.write_all(b"+OK\r\n").await.is_err() {
                    return;
                }
            } else if name.eq_ignore_ascii_case(b"EXISTS") {
                if connection == 0 {
                    continue;
                }
                if write_half.write_all(b":1\r\n").await.is_err() {
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
    async fn timed_out_lookup_retires_generation_before_next_lookup() {
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
        assert!(matches!(
            store.is_revoked("during-replacement").await,
            Err(RevocationError::Unavailable { .. })
        ));

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
    }
}
