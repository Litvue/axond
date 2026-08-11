use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use redis::aio::ConnectionManager;

use super::{RevocationError, RevocationStore, expiry_ms, unavailable};
use crate::config::StoreUnavailable;

pub struct RedisRevocation {
    connection: ConnectionManager,
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
        let connection = tokio::time::timeout(connect_timeout, async {
            let mut connection = ConnectionManager::new(client).await?;
            redis::cmd("PING")
                .query_async::<String>(&mut connection)
                .await?;
            Ok::<_, redis::RedisError>(connection)
        })
        .await
        .map_err(|_| RevocationError::Invalid("Redis connection timed out".to_owned()))?
        .map_err(|e| RevocationError::Invalid(format!("Redis connection failed: {e}")))?;
        Ok(Self {
            connection,
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
        let result = tokio::time::timeout(self.timeout, async {
            redis::cmd("EXISTS")
                .arg(self.key(jti))
                .query_async::<bool>(&mut self.connection.clone())
                .await
        })
        .await;
        match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => unavailable(self.on_unavailable, "redis", error),
            Err(_) => unavailable(self.on_unavailable, "redis", "operation timed out"),
        }
    }

    async fn revoke(&self, jti: &str, expires_at: SystemTime) -> Result<(), RevocationError> {
        if expires_at <= SystemTime::now() {
            let result = tokio::time::timeout(self.timeout, async {
                redis::cmd("DEL")
                    .arg(self.key(jti))
                    .query_async::<i64>(&mut self.connection.clone())
                    .await
            })
            .await;
            return match result {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(error)) => unavailable(self.on_unavailable, "redis", error).map(|_| ()),
                Err(_) => {
                    unavailable(self.on_unavailable, "redis", "operation timed out").map(|_| ())
                }
            };
        }
        let result = tokio::time::timeout(self.timeout, async {
            redis::cmd("SET")
                .arg(self.key(jti))
                .arg("")
                .arg("PXAT")
                .arg(expiry_ms(expires_at))
                .query_async::<()>(&mut self.connection.clone())
                .await
        })
        .await;
        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => unavailable(self.on_unavailable, "redis", error).map(|_| ()),
            Err(_) => unavailable(self.on_unavailable, "redis", "operation timed out").map(|_| ()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::UNIX_EPOCH;

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
        let ttl: i64 = redis::cmd("PTTL")
            .arg(format!("{prefix}:{{replica-jti}}"))
            .query_async(&mut first.connection.clone())
            .await
            .expect("ttl");
        assert!(ttl > 0);
        first
            .revoke("expired-jti", UNIX_EPOCH)
            .await
            .expect("expired write");
        assert!(
            !second
                .is_revoked("expired-jti")
                .await
                .expect("expired read")
        );
    }
}
