use async_trait::async_trait;
use serde_json::Value;
use tokio_postgres::Client;

use super::{NamespaceRecord, Store, StoreError};

pub struct PostgresStore {
    client: Client,
}

impl PostgresStore {
    pub async fn connect(dsn: &str, create_table: bool) -> Result<Self, StoreError> {
        let (client, connection) = tokio_postgres::connect(dsn, crate::usage::tls_connector())
            .await
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        if create_table {
            client
                .batch_execute(
                    "CREATE TABLE IF NOT EXISTS axond_namespace (
                        id TEXT PRIMARY KEY NOT NULL,
                        attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
                        blocklist JSONB
                    );",
                )
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        }
        Ok(Self { client })
    }
}

fn record_from(
    id: String,
    attrs: Value,
    blocklist: Option<Value>,
) -> Result<NamespaceRecord, StoreError> {
    let blocklist = match blocklist {
        None | Some(Value::Null) => None,
        Some(value) => Some(serde_json::from_value(value).map_err(|error| {
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
impl Store for PostgresStore {
    async fn put_namespace(&self, ns: NamespaceRecord) -> Result<(), StoreError> {
        let client = &self.client;
        let blocklist = ns
            .blocklist
            .as_ref()
            .map(|list| serde_json::to_value(list).unwrap_or(Value::Null));
        match client
            .execute(
                "INSERT INTO axond_namespace (id, attrs, blocklist) VALUES ($1, $2, $3)",
                &[&ns.id, &ns.attrs, &blocklist],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(err) if err.code().is_some_and(|c| c.code() == "23505") => {
                Err(StoreError::Duplicate(ns.id))
            }
            Err(err) => Err(StoreError::Unavailable(err.to_string())),
        }
    }

    async fn get_namespace(&self, id: &str) -> Result<Option<NamespaceRecord>, StoreError> {
        let client = &self.client;
        let row = client
            .query_opt(
                "SELECT id, attrs, blocklist FROM axond_namespace WHERE id = $1",
                &[&id],
            )
            .await
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        row.map(|row| record_from(row.get(0), row.get(1), row.get(2)))
            .transpose()
    }

    async fn list_namespaces(
        &self,
        cursor: Option<String>,
        limit: u32,
    ) -> Result<(Vec<NamespaceRecord>, Option<String>), StoreError> {
        let limit = i64::from(limit.clamp(1, 1000));
        let client = &self.client;
        let rows = client
            .query(
                "SELECT id, attrs, blocklist FROM axond_namespace
                 WHERE ($1::text IS NULL OR id > $1)
                 ORDER BY id
                 LIMIT $2",
                &[&cursor, &limit],
            )
            .await
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(record_from(row.get(0), row.get(1), row.get(2))?);
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
        let blocklist = blocklist.map(|list| serde_json::to_value(list).unwrap_or(Value::Null));
        let client = &self.client;
        let row = client
            .query_opt(
                "UPDATE axond_namespace SET attrs = $1, blocklist = $2 WHERE id = $3
                 RETURNING id, attrs, blocklist",
                &[&attrs, &blocklist, &id],
            )
            .await
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        row.map(|row| record_from(row.get(0), row.get(1), row.get(2)))
            .transpose()
    }

    async fn delete_namespace(&self, id: &str) -> Result<bool, StoreError> {
        let client = &self.client;
        let n = client
            .execute("DELETE FROM axond_namespace WHERE id = $1", &[&id])
            .await
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        Ok(n > 0)
    }
}
