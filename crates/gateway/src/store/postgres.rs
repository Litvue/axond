use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tokio_postgres::{Client, Config};

use super::{NamespaceRecord, Store, StoreError};

const SEED_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const SEED_DEADLINE: Duration = Duration::from_secs(15);

pub struct PostgresStore {
    client: Client,
    /// DSN for short-lived seed connections. The request-path `client` is bound
    /// to the process Tokio runtime and must not be driven with `block_on`.
    dsn: String,
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
        client
            .batch_execute("SELECT id, attrs, blocklist FROM axond_namespace LIMIT 0")
            .await
            .map_err(|e| {
                StoreError::Unavailable(format!(
                    "axond_namespace schema missing or incompatible: {e}"
                ))
            })?;
        Ok(Self {
            client,
            dsn: dsn.to_owned(),
        })
    }
}

/// Insert-only seed of addressable namespace ids.
///
/// Publish is rare (reload / convergence). This path is `ON CONFLICT DO NOTHING`,
/// so reseeding existing ids is a no-op rather than a rewrite. A dedicated
/// runtime is used because the request-path client is bound to the process
/// Tokio runtime and must not be driven with `block_on`.
fn seed_on_dedicated_runtime(dsn: &str, ids: &[&str]) -> Result<(), StoreError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| StoreError::Unavailable(error.to_string()))?;
    runtime.block_on(async {
        match tokio::time::timeout(SEED_DEADLINE, seed_namespaces(dsn, ids)).await {
            Ok(result) => result,
            Err(_) => Err(StoreError::Unavailable("namespace seed timed out".into())),
        }
    })
}

async fn seed_namespaces(dsn: &str, ids: &[&str]) -> Result<(), StoreError> {
    let mut config = dsn
        .parse::<Config>()
        .map_err(|error| StoreError::Unavailable(error.to_string()))?;
    config.connect_timeout(SEED_CONNECT_TIMEOUT);
    let (client, connection) = config
        .connect(crate::usage::tls_connector())
        .await
        .map_err(|error| StoreError::Unavailable(error.to_string()))?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("SET lock_timeout = '2s'; SET statement_timeout = '5s'")
        .await
        .map_err(|error| StoreError::Unavailable(error.to_string()))?;
    let ids: Vec<String> = ids.iter().map(|id| (*id).to_owned()).collect();
    client
        .execute(
            "INSERT INTO axond_namespace (id, attrs, blocklist) \
             SELECT u.id, '{}'::jsonb, NULL \
             FROM UNNEST($1::text[]) AS u(id) \
             ON CONFLICT (id) DO NOTHING",
            &[&ids],
        )
        .await
        .map_err(|error| StoreError::Unavailable(error.to_string()))?;
    Ok(())
}

fn record_from(
    id: String,
    attrs: Value,
    blocklist: Option<Value>,
) -> Result<NamespaceRecord, StoreError> {
    let blocklist = match blocklist {
        None | Some(Value::Null) => None,
        // Corrupt stored JSON is an operational store failure (503), not a
        // client bad_request: the caller did not supply this payload.
        Some(value) => Some(serde_json::from_value(value).map_err(|error| {
            StoreError::Unavailable(format!("namespace `{id}` blocklist: {error}"))
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
        let fetch = limit + 1;
        let client = &self.client;
        let rows = client
            .query(
                "SELECT id, attrs, blocklist FROM axond_namespace
                 WHERE ($1::text IS NULL OR id > $1)
                 ORDER BY id
                 LIMIT $2",
                &[&cursor, &fetch],
            )
            .await
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        let has_more = rows.len() as i64 > limit;
        let rows: Vec<_> = if has_more {
            rows.into_iter().take(limit as usize).collect()
        } else {
            rows
        };
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(record_from(row.get(0), row.get(1), row.get(2))?);
        }
        let next = if has_more {
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

    fn seed_namespaces_blocking(
        &self,
        namespaces: &[crate::config::Namespace],
    ) -> Result<(), StoreError> {
        let ids: Vec<&str> = namespaces
            .iter()
            .filter(|namespace| super::validate_namespace_id(&namespace.id).is_ok())
            .map(|namespace| namespace.id.as_str())
            .collect();
        // Reload/convergence is rare and seed is insert-only. Skip the seed
        // connection when the filtered list is empty so publish cannot hang
        // on Postgres for a no-op.
        if ids.is_empty() {
            return Ok(());
        }
        std::thread::scope(|scope| {
            match scope
                .spawn(|| seed_on_dedicated_runtime(&self.dsn, &ids))
                .join()
            {
                Ok(result) => result,
                Err(_) => Err(StoreError::Unavailable(
                    "namespace seed thread panicked".into(),
                )),
            }
        })
    }
}
