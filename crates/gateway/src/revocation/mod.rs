//! Optional precise revocation for minted-token JTIs.

mod postgres;
mod redis;

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use crate::config::{RevocationBackend, RevocationConfig, StoreUnavailable};

pub use postgres::PostgresRevocation;
pub use redis::RedisRevocation;

#[derive(Debug, thiserror::Error)]
pub enum RevocationError {
    #[error("revocation backend `{backend}` unavailable: {message}")]
    Unavailable {
        backend: &'static str,
        message: String,
    },
    #[error("invalid revocation backend configuration: {0}")]
    Invalid(String),
}

#[async_trait]
pub trait RevocationStore: Send + Sync {
    fn name(&self) -> &'static str;
    async fn is_revoked(&self, jti: &str) -> Result<bool, RevocationError>;
    async fn revoke(&self, jti: &str, expires_at: SystemTime) -> Result<(), RevocationError>;
}

pub struct NoDenylist;

#[async_trait]
impl RevocationStore for NoDenylist {
    fn name(&self) -> &'static str {
        "none"
    }

    async fn is_revoked(&self, _jti: &str) -> Result<bool, RevocationError> {
        Ok(false)
    }

    async fn revoke(&self, _jti: &str, _expires_at: SystemTime) -> Result<(), RevocationError> {
        Err(RevocationError::Invalid(
            "no revocation denylist is configured".to_owned(),
        ))
    }
}

fn unavailable(
    policy: StoreUnavailable,
    backend: &'static str,
    error: impl std::fmt::Display,
) -> Result<bool, RevocationError> {
    match policy {
        StoreUnavailable::Deny => Err(RevocationError::Unavailable {
            backend,
            message: error.to_string(),
        }),
        StoreUnavailable::Allow => {
            tracing::warn!(backend, error = %error, "revocation store unavailable; admitting token");
            Ok(false)
        }
    }
}

pub(crate) fn expiry_ms(expires_at: SystemTime) -> u64 {
    expires_at
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

pub async fn build(
    config: &RevocationConfig,
    budget: &crate::config::BudgetConfig,
    env: &HashMap<String, String>,
) -> Result<Box<dyn RevocationStore>, RevocationError> {
    let dsn_env = config
        .dsn_env
        .as_deref()
        .filter(|name| !name.trim().is_empty())
        .or_else(|| {
            if budget.backend == crate::config::BudgetBackend::Redis {
                budget.dsn_env.as_deref()
            } else {
                None
            }
        });
    match config.backend {
        RevocationBackend::None => Ok(Box::new(NoDenylist)),
        RevocationBackend::Redis => {
            let name = dsn_env.ok_or_else(|| {
                RevocationError::Invalid(
                    "revocation `redis`: `dsn_env` must name the env var holding the connection string"
                        .to_owned(),
                )
            })?;
            let url = env
                .get(name)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| RevocationError::Invalid(format!("`{name}` is unset or empty")))?;
            Ok(Box::new(
                RedisRevocation::connect(
                    url,
                    config.key_prefix.as_deref().unwrap_or("axond:revocation"),
                    Duration::from_millis(config.timeout_ms),
                    Duration::from_millis(config.connect_timeout_ms),
                    config.on_unavailable,
                )
                .await?,
            ))
        }
        RevocationBackend::Postgres => {
            let name = dsn_env.ok_or_else(|| {
                RevocationError::Invalid(
                    "revocation `postgres`: `dsn_env` must name the env var holding the connection string"
                        .to_owned(),
                )
            })?;
            let dsn = env
                .get(name)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| RevocationError::Invalid(format!("`{name}` is unset or empty")))?;
            Ok(Box::new(
                PostgresRevocation::connect(
                    dsn,
                    config.table.as_deref().unwrap_or("axond_revocation"),
                    config.create_table,
                    Duration::from_millis(config.timeout_ms),
                    Duration::from_millis(config.connect_timeout_ms),
                    config.on_unavailable,
                )
                .await?,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn no_denylist_never_consults_state() {
        let store = NoDenylist;
        assert!(!store.is_revoked("anything").await.unwrap());
    }

    #[tokio::test]
    async fn no_denylist_rejects_operator_writes() {
        let error = NoDenylist
            .revoke("anything", SystemTime::now())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("no revocation denylist"));
    }
}
