//! The one durable store (ADR 0063).
//!
//! SQLite WAL is the single-replica implementation; Postgres is HA. Boot
//! requires a reachable backend. Namespace rows are loaded on demand — never
//! preloaded at process start.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::{StorageBackend, StorageConfig};
use crate::namespace::NamespaceId;

#[allow(dead_code)]
mod postgres;
mod sqlite;

pub use postgres::PostgresStore;
pub use sqlite::SqliteStore;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NamespaceRecord {
    pub id: String,
    #[serde(default)]
    pub attrs: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocklist: Option<Vec<String>>,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("namespace `{0}` already exists")]
    Duplicate(String),
    #[error("store unavailable: {0}")]
    Unavailable(String),
    #[error("{0}")]
    Invalid(String),
}

#[async_trait]
pub trait Store: Send + Sync {
    async fn put_namespace(&self, ns: NamespaceRecord) -> Result<(), StoreError>;
    async fn get_namespace(&self, id: &str) -> Result<Option<NamespaceRecord>, StoreError>;
    async fn list_namespaces(
        &self,
        cursor: Option<String>,
        limit: u32,
    ) -> Result<(Vec<NamespaceRecord>, Option<String>), StoreError>;
    async fn update_namespace(
        &self,
        id: &str,
        attrs: Value,
        blocklist: Option<Vec<String>>,
    ) -> Result<Option<NamespaceRecord>, StoreError>;
    #[allow(dead_code)]
    async fn delete_namespace(&self, id: &str) -> Result<bool, StoreError>;
}

#[allow(dead_code)]
pub async fn open(
    config: &StorageConfig,
    env: &std::collections::HashMap<String, String>,
) -> Result<Arc<dyn Store>, StoreError> {
    match config.backend {
        StorageBackend::Sqlite => {
            let path = config
                .path
                .as_deref()
                .ok_or_else(|| StoreError::Invalid("`[storage]` sqlite requires `path`".into()))?;
            Ok(Arc::new(SqliteStore::open(path)?))
        }
        StorageBackend::Postgres => {
            let dsn_env = config.dsn_env.as_deref().ok_or_else(|| {
                StoreError::Invalid("`[storage]` postgres requires `dsn_env`".into())
            })?;
            let dsn = env.get(dsn_env).ok_or_else(|| {
                StoreError::Unavailable(format!("env `{dsn_env}` is unset or empty"))
            })?;
            if dsn.is_empty() {
                return Err(StoreError::Unavailable(format!(
                    "env `{dsn_env}` is unset or empty"
                )));
            }
            Ok(Arc::new(PostgresStore::connect(dsn).await?))
        }
    }
}

pub fn validate_namespace_id(id: &str) -> Result<(), StoreError> {
    NamespaceId::parse(id)
        .map(|_| ())
        .map_err(|err| StoreError::Invalid(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sqlite_round_trip_and_duplicate() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        let rec = NamespaceRecord {
            id: "wsp_x".into(),
            attrs: serde_json::json!({"org": "acme"}),
            blocklist: Some(vec!["*-preview".into()]),
        };
        store.put_namespace(rec.clone()).await.expect("insert");
        let got = store
            .get_namespace("wsp_x")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(got.id, "wsp_x");
        assert_eq!(got.attrs["org"], "acme");
        let err = store.put_namespace(rec).await.expect_err("duplicate");
        assert!(matches!(err, StoreError::Duplicate(_)));
    }
}
