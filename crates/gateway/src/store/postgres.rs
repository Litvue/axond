use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio_postgres::{Client, Config, GenericClient, Transaction};

use super::{
    BudgetRecord, BudgetReserve, NamespaceRecord, Store, StoreError, from_sql_amount, sql_amount,
};

const BUDGET_DDL: &str = include_str!("../../sql/store_budget_v1.sql");

const SEED_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const SEED_DEADLINE: Duration = Duration::from_secs(15);

pub struct PostgresStore {
    client: Mutex<Client>,
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
            client
                .batch_execute(BUDGET_DDL)
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
        client
            .batch_execute(
                "SELECT namespace, period, limit_microdollars, spent_microdollars
                 FROM axond_budget LIMIT 0",
            )
            .await
            .map_err(|e| {
                StoreError::Unavailable(format!("axond_budget schema missing or incompatible: {e}"))
            })?;
        Ok(Self {
            client: Mutex::new(client),
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

async fn read_budget(
    client: &impl GenericClient,
    namespace: &str,
    period: &str,
) -> Result<Option<BudgetRecord>, StoreError> {
    let row = client
        .query_opt(
            "SELECT limit_microdollars, spent_microdollars FROM axond_budget
             WHERE namespace = $1 AND period = $2",
            &[&namespace, &period],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    let Some(row) = row else {
        return Ok(None);
    };
    let limit: i64 = row.get(0);
    let spent: i64 = row.get(1);
    let reserved: i64 = client
        .query_one(
            "SELECT COALESCE(SUM(amount_microdollars), 0)::bigint FROM axond_budget_reservation
             WHERE namespace = $1 AND period = $2 AND expires_at > now()",
            &[&namespace, &period],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?
        .get(0);
    let active = client
        .query_opt(
            "SELECT period FROM axond_budget_active WHERE namespace = $1",
            &[&namespace],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?
        .is_some_and(|row| row.get::<_, String>(0) == period);
    Ok(Some(BudgetRecord::new(
        namespace,
        period,
        from_sql_amount(limit),
        from_sql_amount(spent),
        from_sql_amount(reserved),
        active,
    )))
}

#[async_trait]
impl Store for PostgresStore {
    async fn put_namespace(&self, ns: NamespaceRecord) -> Result<(), StoreError> {
        let client = self.client.lock().await;
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
        let client = self.client.lock().await;
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
        let client = self.client.lock().await;
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
        let client = self.client.lock().await;
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
        let client = self.client.lock().await;
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

    async fn put_budget(
        &self,
        namespace: &str,
        period: &str,
        limit_microdollars: u64,
    ) -> Result<BudgetRecord, StoreError> {
        let limit = sql_amount(limit_microdollars)?;
        let mut client = self.client.lock().await;
        let tx = client
            .transaction()
            .await
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        let exists = tx
            .query_opt("SELECT 1 FROM axond_namespace WHERE id = $1", &[&namespace])
            .await
            .map_err(|e| StoreError::Unavailable(e.to_string()))?
            .is_some();
        if !exists {
            return Err(StoreError::NotFound(namespace.to_owned()));
        }
        tx.execute(
            "INSERT INTO axond_budget (namespace, period, limit_microdollars, spent_microdollars)
             VALUES ($1, $2, $3, 0)
             ON CONFLICT (namespace, period) DO UPDATE SET
                limit_microdollars = excluded.limit_microdollars",
            &[&namespace, &period, &limit],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        tx.execute(
            "INSERT INTO axond_budget_active (namespace, period) VALUES ($1, $2)
             ON CONFLICT (namespace) DO UPDATE SET period = excluded.period",
            &[&namespace, &period],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        let rec = read_budget(&tx, namespace, period)
            .await?
            .ok_or_else(|| StoreError::Unavailable("budget row missing after put".into()))?;
        tx.commit()
            .await
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        Ok(rec)
    }

    async fn get_budget(
        &self,
        namespace: &str,
        period: &str,
    ) -> Result<Option<BudgetRecord>, StoreError> {
        let client = self.client.lock().await;
        read_budget(&*client, namespace, period).await
    }

    async fn reserve_budget(
        &self,
        namespace: &str,
        estimate_microdollars: u64,
        reservation_ttl: Duration,
        reservation_id: &str,
    ) -> Result<BudgetReserve, StoreError> {
        let estimate = sql_amount(estimate_microdollars)?;
        let ttl_ms = sql_amount(reservation_ttl.as_millis().min(i64::MAX as u128) as u64)?;
        let mut client = self.client.lock().await;
        let tx = client
            .transaction()
            .await
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        let outcome = hold(&tx, namespace, estimate, ttl_ms, reservation_id).await?;
        match &outcome {
            BudgetReserve::Allowed { .. } => tx
                .commit()
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?,
            BudgetReserve::Exceeded => tx
                .rollback()
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?,
        }
        Ok(outcome)
    }

    async fn settle_budget(
        &self,
        namespace: &str,
        period: &str,
        reservation_id: &str,
        actual_microdollars: u64,
    ) -> Result<(), StoreError> {
        let actual = sql_amount(actual_microdollars)?;
        let mut client = self.client.lock().await;
        let tx = client
            .transaction()
            .await
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        // Active row first, then spend row: the same order `hold` takes them.
        let _ = tx
            .query_opt(
                "SELECT period FROM axond_budget_active WHERE namespace = $1 FOR UPDATE",
                &[&namespace],
            )
            .await
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        let _ = tx
            .query_opt(
                "SELECT limit_microdollars FROM axond_budget
                 WHERE namespace = $1 AND period = $2 FOR UPDATE",
                &[&namespace, &period],
            )
            .await
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        tx.execute(
            "DELETE FROM axond_budget_reservation WHERE id = $1",
            &[&reservation_id],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        tx.execute(
            "UPDATE axond_budget
             SET spent_microdollars = spent_microdollars + $1
             WHERE namespace = $2 AND period = $3",
            &[&actual, &namespace, &period],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        tx.commit()
            .await
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        Ok(())
    }
}

async fn hold(
    tx: &Transaction<'_>,
    namespace: &str,
    estimate: i64,
    ttl_ms: i64,
    reservation_id: &str,
) -> Result<BudgetReserve, StoreError> {
    let Some(active) = tx
        .query_opt(
            "SELECT period FROM axond_budget_active WHERE namespace = $1 FOR UPDATE",
            &[&namespace],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?
    else {
        return Ok(BudgetReserve::Exceeded);
    };
    let period: String = active.get(0);
    tx.execute(
        "DELETE FROM axond_budget_reservation
         WHERE namespace = $1 AND period = $2 AND expires_at <= now()",
        &[&namespace, &period],
    )
    .await
    .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    let Some(row) = tx
        .query_opt(
            "SELECT limit_microdollars, spent_microdollars FROM axond_budget
             WHERE namespace = $1 AND period = $2 FOR UPDATE",
            &[&namespace, &period],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?
    else {
        return Ok(BudgetReserve::Exceeded);
    };
    let limit: i64 = row.get(0);
    let spent: i64 = row.get(1);
    let reserved: i64 = tx
        .query_one(
            "SELECT COALESCE(SUM(amount_microdollars), 0)::bigint FROM axond_budget_reservation
             WHERE namespace = $1 AND period = $2 AND expires_at > now()",
            &[&namespace, &period],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?
        .get(0);
    if spent.saturating_add(reserved).saturating_add(estimate) > limit {
        return Ok(BudgetReserve::Exceeded);
    }
    tx.execute(
        "INSERT INTO axond_budget_reservation
            (id, namespace, period, amount_microdollars, expires_at)
         VALUES ($1, $2, $3, $4, now() + ($5::bigint * interval '1 millisecond'))",
        &[&reservation_id, &namespace, &period, &estimate, &ttl_ms],
    )
    .await
    .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    Ok(BudgetReserve::Allowed { period })
}
