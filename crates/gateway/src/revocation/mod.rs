//! Optional precise revocation for minted-token JTIs.
//!
//! One of the seven responsibility-specific backends catalogued in
//! [`crate::backends`], with its own error type and `on_unavailable` policy: a
//! revocation check runs on the request path, so its stance when the store is
//! unreachable is a request-admission decision, unrelated to how the control
//! plane behaves during an outage.

mod postgres;
mod redis;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use crate::backends::health::BackendHealth;
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
    #[error("revocation backend `{backend}` failed during startup: {message}")]
    Startup {
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

    /// This store's reachability, for the status refresher only.
    ///
    /// Deliberately not [`Self::is_revoked`] with a made-up `jti`: a probe must
    /// not perform a tenant lookup, and a denylist miss is indistinguishable
    /// from a store that answers everything as unrevoked. `None` for `none`,
    /// whose component reports `disabled`.
    fn health(&self) -> Option<Arc<dyn BackendHealth>> {
        None
    }
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

pub(crate) fn expiry_ms(expires_at: SystemTime) -> Result<u64, RevocationError> {
    let millis = expires_at
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis();
    u64::try_from(millis)
        .map_err(|_| RevocationError::Invalid("--expires-at is too far in the future".to_owned()))
}

pub(crate) fn validate_expiry(expires_at: SystemTime) -> Result<(), RevocationError> {
    if expires_at <= SystemTime::now() {
        return Err(RevocationError::Invalid(
            "revocation expiry must be in the future".to_owned(),
        ));
    }
    Ok(())
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
                    &config.key_prefix(),
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

    #[test]
    fn expiry_millis_rejects_values_that_do_not_fit_redis() {
        let expiry = UNIX_EPOCH
            .checked_add(Duration::from_secs(i64::MAX as u64))
            .expect("SystemTime supports a wide future range");
        let error = expiry_ms(expiry).expect_err("millisecond conversion must be bounded");
        assert!(
            error
                .to_string()
                .contains("--expires-at is too far in the future")
        );
    }
}
