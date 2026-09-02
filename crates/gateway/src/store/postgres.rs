use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio_postgres::{Client, GenericClient, Transaction};

use super::{
    BudgetRecord, BudgetReserve, NamespaceRecord, Store, StoreError, from_sql_amount, sql_amount,
};
use crate::backends::health::{BackendHealth, PostgresHealth};

const BUDGET_DDL: &str = include_str!("../../sql/store_budget_v1.sql");
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const SEED_DEADLINE: Duration = Duration::from_secs(15);
const POOL_SIZE: usize = 32;
const PROBE_BOUND: Duration = Duration::from_secs(CONNECT_TIMEOUT.as_secs() + 5);

pub struct PostgresStore {
    config: tokio_postgres::Config,
    /// Idle sessions. The lock is only held while taking or returning a client,
    /// never across a query, so namespace reads and admissions can run together.
    idle: Mutex<Vec<Client>>,
    /// Caps live + idle sessions. Waiters queue here instead of opening more.
    slots: Arc<Semaphore>,
    health: Arc<PostgresHealth>,
}

impl PostgresStore {
    pub async fn connect(dsn: &str, create_table: bool) -> Result<Self, StoreError> {
        let mut config: tokio_postgres::Config = dsn
            .parse()
            .map_err(|e| StoreError::Invalid(format!("unparsable DSN: {e}")))?;
        config.connect_timeout(CONNECT_TIMEOUT);
        config.application_name(crate::telemetry::SERVICE_NAME);
        let store = Self {
            health: Arc::new(PostgresHealth::new("store", config.clone(), PROBE_BOUND)),
            config,
            idle: Mutex::new(Vec::new()),
            slots: Arc::new(Semaphore::new(POOL_SIZE)),
        };
        let mut client = store.connect_client().await?;
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
        ensure_budget_schema(&mut client, create_table).await?;
        probe_schema(&client).await?;
        store.checkin(client).await;
        Ok(store)
    }

    async fn connect_client(&self) -> Result<Client, StoreError> {
        let (client, connection) = self
            .config
            .connect(crate::usage::tls_connector())
            .await
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!(error = %e, "postgres store connection closed");
            }
        });
        client
            .batch_execute("SET lock_timeout = '2s'; SET statement_timeout = '5s'")
            .await
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        Ok(client)
    }

    async fn checkout(&self) -> Result<(Client, OwnedSemaphorePermit), StoreError> {
        let permit = self
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| StoreError::Unavailable("postgres store pool is closed".into()))?;
        {
            let mut idle = self.idle.lock().await;
            while let Some(client) = idle.pop() {
                if !client.is_closed() {
                    return Ok((client, permit));
                }
            }
        }
        match self.connect_client().await {
            Ok(client) => Ok((client, permit)),
            Err(error) => Err(error),
        }
    }

    async fn checkin(&self, client: Client) {
        if client.is_closed() {
            return;
        }
        self.idle.lock().await.push(client);
    }

    fn keep_session(error: &StoreError) -> bool {
        matches!(
            error,
            StoreError::Duplicate(_) | StoreError::NotFound(_) | StoreError::Invalid(_)
        )
    }

    async fn with_client<T>(
        &self,
        operation: impl AsyncFnOnce(&mut Client) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        let (mut client, permit) = self.checkout().await?;
        let result = operation(&mut client).await;
        let reuse = match &result {
            Ok(_) => !client.is_closed(),
            Err(error) => Self::keep_session(error) && !client.is_closed(),
        };
        if reuse {
            self.checkin(client).await;
        }
        drop(permit);
        result
    }
}

async fn probe_schema(client: &Client) -> Result<(), StoreError> {
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
             FROM axond_store_budget LIMIT 0;
             SELECT namespace, period FROM axond_store_budget_active LIMIT 0;
             SELECT id, namespace, period, amount_microdollars, expires_at
             FROM axond_store_budget_reservation LIMIT 0",
        )
        .await
        .map_err(|e| {
            StoreError::Unavailable(format!("budget schema missing or incompatible: {e}"))
        })?;
    Ok(())
}

/// Names the Store ledger uses. Distinct from the withdrawn `[budget]`
/// Postgres backend (`axond_budget` / `axond_budget_reservation`).
const STORE_BUDGET: &str = "axond_store_budget";
const STORE_BUDGET_ACTIVE: &str = "axond_store_budget_active";
const STORE_BUDGET_RESERVATION: &str = "axond_store_budget_reservation";
const STORE_BUDGET_RESERVATION_IDX: &str = "axond_store_budget_reservation_scope_idx";
const DRAFT_STORE_BUDGET: &str = "axond_budget";
const DRAFT_STORE_BUDGET_ACTIVE: &str = "axond_budget_active";
const DRAFT_STORE_BUDGET_RESERVATION: &str = "axond_budget_reservation";
const DRAFT_STORE_BUDGET_RESERVATION_IDX: &str = "axond_budget_reservation_scope_idx";

/// Create or rename Store budget tables so [`probe_schema`] can succeed.
///
/// 1. New `axond_store_budget*` with `period` + `limit_microdollars` — use them.
/// 2. Else `axond_budget` with a `period` column (this file's previous draft) —
///    rename to `axond_store_budget*`.
/// 3. Else `create_table` applies [`BUDGET_DDL`].
/// 4. Leftover `axond_budget` with a `subject` column (budget_v1.sql) is left
///    untouched; spend is not migrated (subject vs period).
async fn ensure_budget_schema(client: &mut Client, create_table: bool) -> Result<(), StoreError> {
    if !store_budget_ready(client).await? && draft_store_budget_present(client).await? {
        rename_draft_store_budget(client).await?;
    }
    if create_table {
        client
            .batch_execute(BUDGET_DDL)
            .await
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    }
    Ok(())
}

async fn store_budget_ready(client: &impl GenericClient) -> Result<bool, StoreError> {
    Ok(has_column(client, STORE_BUDGET, "period").await?
        && has_column(client, STORE_BUDGET, "limit_microdollars").await?
        && has_column(client, STORE_BUDGET_ACTIVE, "period").await?
        && has_column(client, STORE_BUDGET_RESERVATION, "period").await?)
}

async fn draft_store_budget_present(client: &impl GenericClient) -> Result<bool, StoreError> {
    has_column(client, DRAFT_STORE_BUDGET, "period").await
}

async fn rename_draft_store_budget(client: &mut Client) -> Result<(), StoreError> {
    let tx = client
        .transaction()
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    rename_relation(&tx, "TABLE", DRAFT_STORE_BUDGET, STORE_BUDGET).await?;
    rename_relation(&tx, "TABLE", DRAFT_STORE_BUDGET_ACTIVE, STORE_BUDGET_ACTIVE).await?;
    rename_relation(
        &tx,
        "TABLE",
        DRAFT_STORE_BUDGET_RESERVATION,
        STORE_BUDGET_RESERVATION,
    )
    .await?;
    rename_relation(
        &tx,
        "INDEX",
        DRAFT_STORE_BUDGET_RESERVATION_IDX,
        STORE_BUDGET_RESERVATION_IDX,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    Ok(())
}

async fn rename_relation(
    client: &impl GenericClient,
    kind: &str,
    from: &str,
    to: &str,
) -> Result<(), StoreError> {
    if relation_exists(client, from).await? && !relation_exists(client, to).await? {
        client
            .execute(&format!("ALTER {kind} {from} RENAME TO {to}"), &[])
            .await
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    }
    Ok(())
}

async fn relation_exists(client: &impl GenericClient, name: &str) -> Result<bool, StoreError> {
    let exists: bool = client
        .query_one("SELECT to_regclass($1) IS NOT NULL", &[&name])
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?
        .get(0);
    Ok(exists)
}

async fn has_column(
    client: &impl GenericClient,
    table: &str,
    column: &str,
) -> Result<bool, StoreError> {
    Ok(client
        .query_opt(
            "SELECT 1
             FROM pg_attribute
             WHERE attrelid = to_regclass($1)
               AND attname = $2
               AND attnum > 0
               AND NOT attisdropped",
            &[&table, &column],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?
        .is_some())
}

/// Insert-only seed of addressable namespace ids.
///
/// Publish is rare (reload / convergence). This path is `ON CONFLICT DO NOTHING`,
/// so reseeding existing ids is a no-op rather than a rewrite. A dedicated
/// runtime is used because the request-path pool is bound to the process
/// Tokio runtime and must not be driven with `block_on`.
fn seed_on_dedicated_runtime(
    config: tokio_postgres::Config,
    ids: &[&str],
) -> Result<(), StoreError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| StoreError::Unavailable(error.to_string()))?;
    runtime.block_on(async {
        match tokio::time::timeout(SEED_DEADLINE, seed_namespaces(config, ids)).await {
            Ok(result) => result,
            Err(_) => Err(StoreError::Unavailable("namespace seed timed out".into())),
        }
    })
}

async fn seed_namespaces(
    config: tokio_postgres::Config,
    ids: &[&str],
) -> Result<(), StoreError> {
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
            "SELECT
                 b.limit_microdollars,
                 b.spent_microdollars,
                 COALESCE((
                     SELECT SUM(r.amount_microdollars)
                     FROM axond_store_budget_reservation r
                     WHERE r.namespace = b.namespace
                       AND r.period = b.period
                       AND r.expires_at > now()
                 ), 0)::bigint,
                 EXISTS (
                     SELECT 1 FROM axond_store_budget_active a
                     WHERE a.namespace = b.namespace AND a.period = b.period
                 )
             FROM axond_store_budget b
             WHERE b.namespace = $1 AND b.period = $2",
            &[&namespace, &period],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    let Some(row) = row else {
        return Ok(None);
    };
    Ok(Some(BudgetRecord::new(
        namespace,
        period,
        from_sql_amount(row.get(0)),
        from_sql_amount(row.get(1)),
        from_sql_amount(row.get(2)),
        row.get(3),
    )))
}

#[async_trait]
impl Store for PostgresStore {
    fn health(&self) -> Option<Arc<dyn BackendHealth>> {
        Some(Arc::clone(&self.health) as Arc<dyn BackendHealth>)
    }

    async fn put_namespace(&self, ns: NamespaceRecord) -> Result<(), StoreError> {
        self.with_client(async move |client| {
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
        })
        .await
    }

    async fn get_namespace(&self, id: &str) -> Result<Option<NamespaceRecord>, StoreError> {
        let id = id.to_owned();
        self.with_client(async move |client| {
            let row = client
                .query_opt(
                    "SELECT id, attrs, blocklist FROM axond_namespace WHERE id = $1",
                    &[&id],
                )
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
            row.map(|row| record_from(row.get(0), row.get(1), row.get(2)))
                .transpose()
        })
        .await
    }

    async fn list_namespaces(
        &self,
        cursor: Option<String>,
        limit: u32,
    ) -> Result<(Vec<NamespaceRecord>, Option<String>), StoreError> {
        let limit = i64::from(limit.clamp(1, 1000));
        let fetch = limit + 1;
        self.with_client(async move |client| {
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
        })
        .await
    }

    async fn update_namespace(
        &self,
        id: &str,
        attrs: Value,
        blocklist: Option<Vec<String>>,
    ) -> Result<Option<NamespaceRecord>, StoreError> {
        let id = id.to_owned();
        self.with_client(async move |client| {
            let blocklist = blocklist.map(|list| serde_json::to_value(list).unwrap_or(Value::Null));
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
        })
        .await
    }

    async fn delete_namespace(&self, id: &str) -> Result<bool, StoreError> {
        let id = id.to_owned();
        self.with_client(async move |client| {
            let n = client
                .execute("DELETE FROM axond_namespace WHERE id = $1", &[&id])
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
            Ok(n > 0)
        })
        .await
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
        let config = self.config.clone();
        std::thread::scope(|scope| {
            match scope
                .spawn(|| seed_on_dedicated_runtime(config, &ids))
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
        let namespace = namespace.to_owned();
        let period = period.to_owned();
        let limit = sql_amount(limit_microdollars)?;
        self.with_client(async move |client| {
            let tx = client
                .transaction()
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
            let rec = put_budget_tx(&tx, &namespace, &period, limit).await?;
            tx.commit()
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
            Ok(rec)
        })
        .await
    }

    async fn get_budget(
        &self,
        namespace: &str,
        period: &str,
    ) -> Result<Option<BudgetRecord>, StoreError> {
        let namespace = namespace.to_owned();
        let period = period.to_owned();
        self.with_client(async move |client| read_budget(client, &namespace, &period).await)
            .await
    }

    async fn reserve_budget(
        &self,
        namespace: &str,
        estimate_microdollars: u64,
        reservation_ttl: Duration,
        reservation_id: &str,
    ) -> Result<BudgetReserve, StoreError> {
        let namespace = namespace.to_owned();
        let reservation_id = reservation_id.to_owned();
        let estimate = sql_amount(estimate_microdollars)?;
        let ttl_ms = sql_amount(reservation_ttl.as_millis().min(i64::MAX as u128) as u64)?;
        self.with_client(async move |client| {
            let tx = client
                .transaction()
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
            let outcome = hold(&tx, &namespace, estimate, ttl_ms, &reservation_id).await?;
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
        })
        .await
    }

    async fn settle_budget(
        &self,
        namespace: &str,
        period: &str,
        reservation_id: &str,
        actual_microdollars: u64,
    ) -> Result<(), StoreError> {
        let namespace = namespace.to_owned();
        let period = period.to_owned();
        let reservation_id = reservation_id.to_owned();
        let actual = sql_amount(actual_microdollars)?;
        self.with_client(async move |client| {
            let tx = client
                .transaction()
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
            settle_tx(&tx, &namespace, &period, &reservation_id, actual).await?;
            tx.commit()
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
            Ok(())
        })
        .await
    }
}

/// Active row first, then the spend row: the same order as reserve and settle.
async fn put_budget_tx(
    tx: &Transaction<'_>,
    namespace: &str,
    period: &str,
    limit: i64,
) -> Result<BudgetRecord, StoreError> {
    let exists = tx
        .query_opt("SELECT 1 FROM axond_namespace WHERE id = $1", &[&namespace])
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?
        .is_some();
    if !exists {
        return Err(StoreError::NotFound(namespace.to_owned()));
    }
    tx.execute(
        "INSERT INTO axond_store_budget_active (namespace, period) VALUES ($1, $2)
         ON CONFLICT (namespace) DO UPDATE SET period = excluded.period",
        &[&namespace, &period],
    )
    .await
    .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    tx.execute(
        "INSERT INTO axond_store_budget (namespace, period, limit_microdollars, spent_microdollars)
         VALUES ($1, $2, $3, 0)
         ON CONFLICT (namespace, period) DO UPDATE SET
            limit_microdollars = excluded.limit_microdollars",
        &[&namespace, &period, &limit],
    )
    .await
    .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    read_budget(tx, namespace, period)
        .await?
        .ok_or_else(|| StoreError::Unavailable("budget row missing after put".into()))
}

async fn settle_tx(
    tx: &Transaction<'_>,
    namespace: &str,
    period: &str,
    reservation_id: &str,
    actual: i64,
) -> Result<(), StoreError> {
    let _ = tx
        .query_opt(
            "SELECT period FROM axond_store_budget_active WHERE namespace = $1 FOR UPDATE",
            &[&namespace],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    let _ = tx
        .query_opt(
            "SELECT limit_microdollars FROM axond_store_budget
             WHERE namespace = $1 AND period = $2 FOR UPDATE",
            &[&namespace, &period],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    tx.execute(
        "DELETE FROM axond_store_budget_reservation WHERE id = $1",
        &[&reservation_id],
    )
    .await
    .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    tx.execute(
        "UPDATE axond_store_budget
         SET spent_microdollars = spent_microdollars + $1
         WHERE namespace = $2 AND period = $3",
        &[&actual, &namespace, &period],
    )
    .await
    .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    Ok(())
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
            "SELECT period FROM axond_store_budget_active WHERE namespace = $1 FOR UPDATE",
            &[&namespace],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?
    else {
        return Ok(BudgetReserve::Exceeded);
    };
    let period: String = active.get(0);
    tx.execute(
        "DELETE FROM axond_store_budget_reservation
         WHERE namespace = $1 AND expires_at <= now()",
        &[&namespace],
    )
    .await
    .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    let Some(row) = tx
        .query_opt(
            "SELECT limit_microdollars, spent_microdollars FROM axond_store_budget
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
            "SELECT COALESCE(SUM(amount_microdollars), 0)::bigint FROM axond_store_budget_reservation
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
        "INSERT INTO axond_store_budget_reservation
            (id, namespace, period, amount_microdollars, expires_at)
         VALUES ($1, $2, $3, $4, now() + ($5::bigint * interval '1 millisecond'))",
        &[&reservation_id, &namespace, &period, &estimate, &ttl_ms],
    )
    .await
    .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    Ok(BudgetReserve::Allowed { period })
}

#[cfg(test)]
impl PostgresStore {
    /// Kill the next idle session so the following Store call must reconnect.
    pub(super) async fn drop_idle_connection(&self) -> Result<(), StoreError> {
        let (client, permit) = self.checkout().await?;
        let _ = client
            .execute("SELECT pg_terminate_backend(pg_backend_pid())", &[])
            .await;
        self.checkin(client).await;
        drop(permit);
        Ok(())
    }

    pub(super) async fn reservation_count(&self, namespace: &str) -> Result<i64, StoreError> {
        let namespace = namespace.to_owned();
        self.with_client(async move |client| {
            let count: i64 = client
                .query_one(
                    "SELECT count(*)::bigint FROM axond_store_budget_reservation WHERE namespace = $1",
                    &[&namespace],
                )
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?
                .get(0);
            Ok(count)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Coexist with leftover `budget_v1.sql` is exercised when
    /// `AXOND_TEST_POSTGRES_DSN` is set (`postgres_legacy_budget_v1_coexists_with_store_budget`).
    #[test]
    fn store_budget_ddl_uses_store_prefixed_names() {
        assert!(BUDGET_DDL.contains("CREATE TABLE IF NOT EXISTS axond_store_budget ("));
        assert!(BUDGET_DDL.contains("CREATE TABLE IF NOT EXISTS axond_store_budget_active ("));
        assert!(BUDGET_DDL.contains("CREATE TABLE IF NOT EXISTS axond_store_budget_reservation ("));
        assert!(
            BUDGET_DDL
                .contains("CREATE INDEX IF NOT EXISTS axond_store_budget_reservation_scope_idx")
        );
        assert!(!BUDGET_DDL.contains("CREATE TABLE IF NOT EXISTS axond_budget ("));
        assert!(!BUDGET_DDL.contains("CREATE TABLE IF NOT EXISTS axond_budget_active ("));
        assert!(!BUDGET_DDL.contains("CREATE TABLE IF NOT EXISTS axond_budget_reservation ("));
    }
}
