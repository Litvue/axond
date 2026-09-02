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

mod postgres;
mod sqlite;

pub use postgres::PostgresStore;
pub use sqlite::SqliteStore;

/// ADR 0063: opaque namespace `attrs` are capped at 4 KiB (serialized JSON).
pub const MAX_ATTRS_BYTES: usize = 4 * 1024;

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
            Ok(Arc::new(
                PostgresStore::connect(dsn, config.create_table).await?,
            ))
        }
    }
}

/// Insert TOML `[[namespace]]` rows so black-box fixtures keep working.
///
/// `put_namespace` is insert-only: a restart against an existing file correctly
/// hits [`StoreError::Duplicate`], which is ignored. Any other failure fails boot.
pub async fn seed_config_namespaces(
    store: &dyn Store,
    namespaces: &[crate::config::Namespace],
) -> Result<(), StoreError> {
    for namespace in namespaces {
        validate_namespace_id(&namespace.id)?;
    }
    for namespace in namespaces {
        let record = NamespaceRecord {
            id: namespace.id.clone(),
            attrs: serde_json::json!({}),
            blocklist: None,
        };
        match store.put_namespace(record).await {
            Ok(()) | Err(StoreError::Duplicate(_)) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

pub fn validate_namespace_id(id: &str) -> Result<(), StoreError> {
    NamespaceId::parse(id)
        .map(|_| ())
        .map_err(|err| StoreError::Invalid(err.to_string()))
}

/// Refuse attrs whose serialized JSON exceeds [`MAX_ATTRS_BYTES`].
pub fn validate_attrs(attrs: &Value) -> Result<(), StoreError> {
    let len = serde_json::to_vec(attrs)
        .map(|bytes| bytes.len())
        .unwrap_or(usize::MAX);
    if len > MAX_ATTRS_BYTES {
        return Err(StoreError::Invalid(format!(
            "attrs exceeds {MAX_ATTRS_BYTES} byte limit"
        )));
    }
    Ok(())
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

    #[tokio::test]
    async fn sqlite_file_survives_reopen() {
        let path = std::env::temp_dir().join(format!(
            "axond-store-persist-{}-{}.sqlite",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let rec = NamespaceRecord {
            id: "wsp_x".into(),
            attrs: serde_json::json!({"org": "acme"}),
            blocklist: None,
        };
        {
            let store = SqliteStore::open(path.to_str().expect("utf8")).expect("open");
            store.put_namespace(rec.clone()).await.expect("insert");
        }
        let store = SqliteStore::open(path.to_str().expect("utf8")).expect("reopen");
        let got = store
            .get_namespace("wsp_x")
            .await
            .expect("get")
            .expect("row");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        assert_eq!(got, rec);
    }

    #[tokio::test]
    async fn sqlite_list_cursor_only_when_another_row_exists() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        for id in ["a", "b", "c"] {
            store
                .put_namespace(NamespaceRecord {
                    id: id.into(),
                    attrs: serde_json::json!({}),
                    blocklist: None,
                })
                .await
                .expect("insert");
        }
        let (page, next) = store.list_namespaces(None, 3).await.expect("full page");
        assert_eq!(page.len(), 3);
        assert_eq!(next, None);
        let (page, next) = store.list_namespaces(None, 2).await.expect("partial");
        assert_eq!(
            page.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert_eq!(next.as_deref(), Some("b"));
        let (page, next) = store.list_namespaces(next, 2).await.expect("remainder");
        assert_eq!(
            page.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["c"]
        );
        assert_eq!(next, None);
    }
}
