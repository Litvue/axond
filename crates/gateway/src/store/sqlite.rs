use std::sync::Mutex;

use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;

use super::{NamespaceRecord, Store, StoreError};

pub struct SqliteStore {
    conn: Mutex<Connection>,
}

impl SqliteStore {
    pub fn open(path: &str) -> Result<Self, StoreError> {
        let conn = Connection::open(path).map_err(unavailable)?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(unavailable)?;
        conn.pragma_update(None, "busy_timeout", 5000)
            .map_err(unavailable)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS axond_namespace (
                id TEXT PRIMARY KEY NOT NULL,
                attrs TEXT NOT NULL DEFAULT '{}',
                blocklist TEXT
            );",
        )
        .map_err(unavailable)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

fn unavailable(err: rusqlite::Error) -> StoreError {
    StoreError::Unavailable(err.to_string())
}

fn row_to_record(
    id: String,
    attrs: String,
    blocklist: Option<String>,
) -> Result<NamespaceRecord, StoreError> {
    let attrs: Value = serde_json::from_str(&attrs)
        .map_err(|error| StoreError::Invalid(format!("namespace `{id}` attrs: {error}")))?;
    let blocklist = match blocklist {
        None => None,
        Some(raw) => Some(serde_json::from_str(&raw).map_err(|error| {
            StoreError::Invalid(format!("namespace `{id}` blocklist: {error}"))
        })?),
    };
    Ok(NamespaceRecord {
        id,
        attrs,
        blocklist,
    })
}

#[async_trait]
impl Store for SqliteStore {
    async fn put_namespace(&self, ns: NamespaceRecord) -> Result<(), StoreError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        let attrs = ns.attrs.to_string();
        let blocklist = ns
            .blocklist
            .as_ref()
            .map(|list| serde_json::to_string(list).unwrap_or_else(|_| "[]".into()));
        match conn.execute(
            "INSERT INTO axond_namespace (id, attrs, blocklist) VALUES (?1, ?2, ?3)",
            params![ns.id, attrs, blocklist],
        ) {
            Ok(_) => Ok(()),
            Err(rusqlite::Error::SqliteFailure(err, _))
                if err.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                Err(StoreError::Duplicate(ns.id))
            }
            Err(err) => Err(unavailable(err)),
        }
    }

    async fn get_namespace(&self, id: &str) -> Result<Option<NamespaceRecord>, StoreError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        conn.query_row(
            "SELECT id, attrs, blocklist FROM axond_namespace WHERE id = ?1",
            params![id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .optional()
        .map_err(unavailable)?
        .map(|(id, attrs, blocklist)| row_to_record(id, attrs, blocklist))
        .transpose()
    }

    async fn list_namespaces(
        &self,
        cursor: Option<String>,
        limit: u32,
    ) -> Result<(Vec<NamespaceRecord>, Option<String>), StoreError> {
        let limit = limit.clamp(1, 1000);
        let conn = self
            .conn
            .lock()
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        let mut stmt = conn
            .prepare(
                "SELECT id, attrs, blocklist FROM axond_namespace
                 WHERE (?1 IS NULL OR id > ?1)
                 ORDER BY id
                 LIMIT ?2",
            )
            .map_err(unavailable)?;
        let rows = stmt
            .query_map(params![cursor, limit], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })
            .map_err(unavailable)?;
        let mut out = Vec::new();
        for row in rows {
            let (id, attrs, blocklist) = row.map_err(unavailable)?;
            out.push(row_to_record(id, attrs, blocklist)?);
        }
        let next = if out.len() == limit as usize {
            out.last().map(|row| row.id.clone())
        } else {
            None
        };
        Ok((out, next))
    }

    async fn update_namespace(
        &self,
        id: &str,
        attrs: Value,
        blocklist: Option<Vec<String>>,
    ) -> Result<Option<NamespaceRecord>, StoreError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        let blocklist_json = blocklist
            .as_ref()
            .map(|list| serde_json::to_string(list).unwrap_or_else(|_| "[]".into()));
        conn.query_row(
            "UPDATE axond_namespace SET attrs = ?1, blocklist = ?2 WHERE id = ?3
             RETURNING id, attrs, blocklist",
            params![attrs.to_string(), blocklist_json, id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .optional()
        .map_err(unavailable)?
        .map(|(id, attrs, blocklist)| row_to_record(id, attrs, blocklist))
        .transpose()
    }

    async fn delete_namespace(&self, id: &str) -> Result<bool, StoreError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        let n = conn
            .execute("DELETE FROM axond_namespace WHERE id = ?1", params![id])
            .map_err(unavailable)?;
        Ok(n > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn corrupt_attrs_are_invalid() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        {
            let conn = store.conn.lock().expect("lock");
            conn.execute(
                "INSERT INTO axond_namespace (id, attrs, blocklist) VALUES ('bad', 'not-json', NULL)",
                [],
            )
            .expect("insert corrupt row");
        }
        let err = store.get_namespace("bad").await.expect_err("invalid");
        assert!(matches!(err, StoreError::Invalid(_)), "{err:?}");
    }

    #[tokio::test]
    async fn update_returns_the_written_row() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        store
            .put_namespace(NamespaceRecord {
                id: "wsp_x".into(),
                attrs: serde_json::json!({}),
                blocklist: None,
            })
            .await
            .expect("insert");
        let got = store
            .update_namespace("wsp_x", serde_json::json!({"org": "acme"}), None)
            .await
            .expect("update")
            .expect("row");
        assert_eq!(got.attrs["org"], "acme");
    }
}
