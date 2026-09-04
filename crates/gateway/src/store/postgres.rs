use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio_postgres::{Client, GenericClient, Transaction};

use super::{
    BudgetAdmit, BudgetRecord, NamespaceRecord, NamespaceResolve, ProviderModels, Store,
    StoreError, UsageAppend, UsageSummaryRow, admit_from_ledger, from_sql_amount, sql_amount,
    sql_amount_saturating,
};
use crate::backends::health::{BackendHealth, PostgresHealth};

const BUDGET_DDL: &str = include_str!("../../sql/store_budget_v1.sql");
const INCARNATION_DDL: &str = include_str!("../../sql/store_namespace_incarnation_v1.sql");
const USAGE_DDL: &str = include_str!("../../sql/store_usage_v1.sql");
const MODELS_DDL: &str = include_str!("../../sql/store_provider_models_v1.sql");
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const SEED_DEADLINE: Duration = Duration::from_secs(15);
const POOL_SIZE: usize = 32;
const IDLE_CAP: usize = 8;
/// Same order as `lock_timeout` / `statement_timeout`: fail 503 rather than hang.
#[cfg(not(test))]
const POOL_WAIT: Duration = Duration::from_secs(2);
#[cfg(test)]
const POOL_WAIT: Duration = Duration::from_millis(50);
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
        if create_table {
            client
                .batch_execute(INCARNATION_DDL)
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
            client
                .batch_execute(USAGE_DDL)
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
            client
                .batch_execute(MODELS_DDL)
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        }
        probe_schema(&client).await?;
        sweep_expired_holds(&client).await?;
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
        let permit = match tokio::time::timeout(POOL_WAIT, self.slots.clone().acquire_owned()).await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => {
                return Err(StoreError::Unavailable(
                    "postgres store pool is closed".into(),
                ));
            }
            Err(_) => {
                return Err(StoreError::Unavailable(
                    "postgres store pool saturated".into(),
                ));
            }
        };
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
        let mut idle = self.idle.lock().await;
        if idle.len() < IDLE_CAP {
            idle.push(client);
        }
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
        .batch_execute(
            "SELECT id, attrs, blocklist FROM axond_namespace LIMIT 0;
             SELECT id, n FROM axond_namespace_incarnation LIMIT 0;
             SELECT id, incarnation, expires_at FROM axond_store_budget_reservation_tombstone LIMIT 0",
        )
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
             SELECT id, namespace, period, amount_microdollars, expires_at, incarnation
             FROM axond_store_budget_reservation LIMIT 0",
        )
        .await
        .map_err(|e| {
            StoreError::Unavailable(format!("budget schema missing or incompatible: {e}"))
        })?;
    client
        .batch_execute(
            "SELECT request_id, namespace, period, model, status, cost_microdollars,
                    recorded_at
             FROM axond_store_usage LIMIT 0",
        )
        .await
        .map_err(|e| {
            StoreError::Unavailable(format!(
                "axond_store_usage schema missing or incompatible: {e}"
            ))
        })?;
    client
        .batch_execute(
            "SELECT provider, fetched_at, stale, models, source
             FROM axond_store_provider_models LIMIT 0",
        )
        .await
        .map_err(|e| {
            StoreError::Unavailable(format!(
                "axond_store_provider_models schema missing or incompatible: {e}"
            ))
        })?;
    Ok(())
}

/// Expired holds left by replicas that predate ADR 0064; nothing on the live path removes them.
async fn sweep_expired_holds(client: &impl GenericClient) -> Result<(), StoreError> {
    client
        .execute(
            "DELETE FROM axond_store_budget_reservation WHERE expires_at <= now()",
            &[],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    client
        .execute(
            "DELETE FROM axond_store_budget_reservation_tombstone WHERE expires_at < now()",
            &[],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?;
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
/// 1. New `axond_store_budget*` complete with spend rows — use them (already
///    migrated). Complete and empty with draft rows — drop empty dest, rename.
/// 2. Draft `axond_budget*` with a `period` column and dest missing or empty —
///    drop empty dest relations, then rename. Incomplete dest with any rows is
///    a boot error (do not mix dest spend with renamed draft tables).
///    Needs table-rename privilege; migration-only roles should run this out
///    of band before boot.
/// 3. Else `create_table` applies [`BUDGET_DDL`].
/// 4. Leftover `axond_budget` with a `subject` column (budget_v1.sql) is left
///    untouched; spend is not migrated (subject vs period).
///
/// Then `ADD COLUMN IF NOT EXISTS incarnation` on the reservation table so a
/// draft rename cannot drop it (`create_table = false` included).
async fn ensure_budget_schema(client: &mut Client, create_table: bool) -> Result<(), StoreError> {
    let renamed =
        if draft_store_budget_present(client).await? && should_rename_draft(client).await? {
            rename_draft_store_budget(client).await?;
            true
        } else {
            false
        };
    if create_table {
        client
            .batch_execute(BUDGET_DDL)
            .await
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    }
    // Additive ALTER only when we created tables or just renamed draft
    // relations that predate incarnation. create_table=false otherwise
    // only probes.
    if create_table || renamed {
        ensure_reservation_incarnation(client).await?;
    }
    Ok(())
}

/// Draft `axond_budget_reservation` predates incarnation. Additive ALTER;
/// no-op if the column already exists. Not CREATE TABLE.
async fn ensure_reservation_incarnation(client: &impl GenericClient) -> Result<(), StoreError> {
    if !relation_exists(client, STORE_BUDGET_RESERVATION).await? {
        return Ok(());
    }
    client
        .execute(
            "ALTER TABLE axond_store_budget_reservation
             ADD COLUMN IF NOT EXISTS incarnation bigint NOT NULL DEFAULT 1",
            &[],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    Ok(())
}

async fn should_rename_draft(client: &impl GenericClient) -> Result<bool, StoreError> {
    let dest_complete = store_budget_ready(client).await?;
    let dest_has_rows = ledger_has_rows(
        client,
        STORE_BUDGET,
        STORE_BUDGET_ACTIVE,
        STORE_BUDGET_RESERVATION,
    )
    .await?;
    if !dest_complete {
        if dest_has_rows {
            return Err(partial_dest_store_budget_error(client).await?);
        }
        return Ok(true);
    }
    Ok(ledger_has_rows(
        client,
        DRAFT_STORE_BUDGET,
        DRAFT_STORE_BUDGET_ACTIVE,
        DRAFT_STORE_BUDGET_RESERVATION,
    )
    .await?
        && !dest_has_rows)
}

async fn partial_dest_store_budget_error(
    client: &impl GenericClient,
) -> Result<StoreError, StoreError> {
    let mut present = Vec::new();
    for name in [STORE_BUDGET, STORE_BUDGET_ACTIVE, STORE_BUDGET_RESERVATION] {
        if relation_exists(client, name).await? {
            present.push(name);
        }
    }
    Ok(StoreError::Unavailable(format!(
        "partial {} schema has rows; finish creating the missing axond_store_budget* tables or drop them before renaming draft axond_budget* spend",
        present.join(", ")
    )))
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
    drop_empty_store_budget(&tx).await?;
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

/// Drop `axond_store_budget*` only when every relation is missing or empty.
/// Never drops a new table that already has spend.
async fn drop_empty_store_budget(client: &impl GenericClient) -> Result<(), StoreError> {
    if ledger_has_rows(
        client,
        STORE_BUDGET,
        STORE_BUDGET_ACTIVE,
        STORE_BUDGET_RESERVATION,
    )
    .await?
    {
        return Ok(());
    }
    for (kind, name) in [
        ("TABLE", STORE_BUDGET_RESERVATION),
        ("TABLE", STORE_BUDGET_ACTIVE),
        ("TABLE", STORE_BUDGET),
        ("INDEX", STORE_BUDGET_RESERVATION_IDX),
    ] {
        if relation_exists(client, name).await? {
            client
                .execute(&format!("DROP {kind} IF EXISTS {name}"), &[])
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        }
    }
    Ok(())
}

async fn ledger_has_rows(
    client: &impl GenericClient,
    budget: &str,
    active: &str,
    reservation: &str,
) -> Result<bool, StoreError> {
    Ok(relation_has_rows(client, budget).await?
        || relation_has_rows(client, active).await?
        || relation_has_rows(client, reservation).await?)
}

async fn relation_has_rows(client: &impl GenericClient, table: &str) -> Result<bool, StoreError> {
    if !relation_exists(client, table).await? {
        return Ok(false);
    }
    let has_rows: bool = client
        .query_one(&format!("SELECT EXISTS (SELECT 1 FROM {table})"), &[])
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?
        .get(0);
    Ok(has_rows)
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

async fn seed_namespaces(config: tokio_postgres::Config, ids: &[&str]) -> Result<(), StoreError> {
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
        row.get(2),
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
            let tx = client
                .transaction()
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
            lock_namespace_id(&tx, &ns.id).await?;
            match tx
                .execute(
                    "INSERT INTO axond_namespace (id, attrs, blocklist) VALUES ($1, $2, $3)",
                    &[&ns.id, &ns.attrs, &blocklist],
                )
                .await
            {
                Ok(_) => {
                    // Keep n if this id was deleted earlier. Advisory lock is
                    // the lifecycle mutex; this row is the settle generation.
                    tx.execute(
                        "INSERT INTO axond_namespace_incarnation (id, n) VALUES ($1, 1)
                         ON CONFLICT (id) DO NOTHING",
                        &[&ns.id],
                    )
                    .await
                    .map_err(|e| StoreError::Unavailable(e.to_string()))?;
                    tx.commit()
                        .await
                        .map_err(|e| StoreError::Unavailable(e.to_string()))?;
                    Ok(())
                }
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

    async fn resolve_namespace(&self, id: &str) -> Result<Option<NamespaceResolve>, StoreError> {
        let id = id.to_owned();
        self.with_client(async move |client| resolve_namespace_on(client, &id).await)
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
            let tx = client
                .transaction()
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
            let deleted = delete_namespace_tx(&tx, &id).await?;
            tx.commit()
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
            Ok(deleted)
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

    async fn admit_budget(&self, namespace: &str) -> Result<BudgetAdmit, StoreError> {
        let namespace = namespace.to_owned();
        self.with_client(async move |client| admit_budget_on(client, &namespace).await)
            .await
    }

    async fn charge_budget(
        &self,
        namespace: &str,
        period: &str,
        incarnation: i64,
        actual_microdollars: u64,
    ) -> Result<(), StoreError> {
        let namespace = namespace.to_owned();
        let period = period.to_owned();
        let actual = sql_amount_saturating(actual_microdollars);
        self.with_client(async move |client| {
            charge_budget_on(client, &namespace, &period, incarnation, actual).await
        })
        .await
    }

    async fn append_usage(&self, event: UsageAppend) -> Result<(), StoreError> {
        let cost = event.cost_microdollars.map(sql_amount_saturating);
        self.with_client(async move |client| {
            client
                .execute(
                    "INSERT INTO axond_store_usage
                        (request_id, namespace, period, model, status, cost_microdollars, recorded_at)
                     VALUES ($1, $2, $3, $4, $5, $6, now())
                     ON CONFLICT (request_id) DO NOTHING",
                    &[
                        &event.request_id,
                        &event.namespace,
                        &event.period,
                        &event.model,
                        &event.status,
                        &cost,
                    ],
                )
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
            Ok(())
        })
        .await
    }

    async fn summarize_usage(
        &self,
        namespace: &str,
        period: &str,
    ) -> Result<Vec<UsageSummaryRow>, StoreError> {
        let namespace = namespace.to_owned();
        let period = period.to_owned();
        self.with_client(async move |client| {
            // SUM(bigint) is numeric; clamp before ::bigint so overflow saturates.
            let rows = client
                .query(
                    "SELECT model, status, COUNT(*)::bigint,
                            LEAST(COALESCE(SUM(COALESCE(cost_microdollars, 0)), 0), 9223372036854775807)::bigint
                     FROM axond_store_usage
                     WHERE namespace = $1 AND period = $2
                     GROUP BY model, status
                     ORDER BY model, status",
                    &[&namespace, &period],
                )
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
            Ok(rows
                .into_iter()
                .map(|row| UsageSummaryRow {
                    model: row.get(0),
                    status: row.get(1),
                    count: from_sql_amount(row.get(2)),
                    cost_microdollars: from_sql_amount(row.get(3)),
                })
                .collect())
        })
        .await
    }

    async fn get_provider_models(
        &self,
        provider: &str,
    ) -> Result<Option<ProviderModels>, StoreError> {
        let provider = provider.to_owned();
        self.with_client(async move |client| {
            let row = client
                .query_opt(
                    "SELECT provider, fetched_at, stale, models, source
                     FROM axond_store_provider_models WHERE provider = $1",
                    &[&provider],
                )
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
            row.map(|row| {
                postgres_provider_models(row.get(0), row.get(1), row.get(2), row.get(3), row.get(4))
            })
            .transpose()
        })
        .await
    }

    async fn list_provider_models(&self) -> Result<Vec<ProviderModels>, StoreError> {
        self.with_client(async move |client| {
            let rows = client
                .query(
                    "SELECT provider, fetched_at, stale, models, source
                     FROM axond_store_provider_models ORDER BY provider",
                    &[],
                )
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
            rows.into_iter()
                .map(|row| {
                    postgres_provider_models(
                        row.get(0),
                        row.get(1),
                        row.get(2),
                        row.get(3),
                        row.get(4),
                    )
                })
                .collect()
        })
        .await
    }

    async fn put_provider_models(&self, row: ProviderModels) -> Result<(), StoreError> {
        self.with_client(async move |client| {
            let models = Value::Array(row.data);
            client
                .execute(
                    "INSERT INTO axond_store_provider_models (provider, fetched_at, stale, models, source)
                     VALUES ($1, $2, $3, $4, $5)
                     ON CONFLICT (provider) DO UPDATE SET
                        fetched_at = excluded.fetched_at,
                        stale = excluded.stale,
                        models = excluded.models,
                        source = excluded.source
                     WHERE axond_store_provider_models.source IS NOT DISTINCT FROM excluded.source
                        OR axond_store_provider_models.stale",
                    &[
                        &row.provider,
                        &row.fetched_at,
                        &row.stale,
                        &models,
                        &row.source,
                    ],
                )
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
            Ok(())
        })
        .await
    }

    async fn mark_provider_models_stale_unless_source(
        &self,
        provider: &str,
        source: &str,
    ) -> Result<(), StoreError> {
        let provider = provider.to_owned();
        let source = source.to_owned();
        self.with_client(async move |client| {
            client
                .execute(
                    "UPDATE axond_store_provider_models SET stale = TRUE
                     WHERE provider = $1 AND source IS DISTINCT FROM $2",
                    &[&provider, &source],
                )
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
            Ok(())
        })
        .await
    }

    async fn mark_provider_models_stale_if_source(
        &self,
        provider: &str,
        source: &str,
    ) -> Result<(), StoreError> {
        let provider = provider.to_owned();
        let source = source.to_owned();
        self.with_client(async move |client| {
            client
                .execute(
                    "UPDATE axond_store_provider_models SET stale = TRUE
                     WHERE provider = $1 AND source IS NOT DISTINCT FROM $2",
                    &[&provider, &source],
                )
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
            Ok(())
        })
        .await
    }
}

fn postgres_provider_models(
    provider: String,
    fetched_at: Option<String>,
    stale: bool,
    models: Value,
    source: Option<String>,
) -> Result<ProviderModels, StoreError> {
    let data = match models {
        Value::Array(items) => items,
        other => {
            return Err(StoreError::Unavailable(format!(
                "provider `{provider}` models: expected array, got {other}"
            )));
        }
    };
    Ok(ProviderModels {
        provider,
        fetched_at,
        stale,
        data,
        source,
    })
}

/// Transaction-scoped advisory lock on the namespace id. Shared by CREATE,
/// DELETE, and PUT budget so those paths cannot deadlock or orphan ledgers.
/// Charge is a single UPDATE and does not take this lock. Does not insert an
/// incarnation row for a missing id.
async fn lock_namespace_id(tx: &Transaction<'_>, id: &str) -> Result<(), StoreError> {
    tx.query("SELECT pg_advisory_xact_lock(hashtext($1))", &[&id])
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    Ok(())
}

async fn delete_namespace_tx(tx: &Transaction<'_>, id: &str) -> Result<bool, StoreError> {
    lock_namespace_id(tx, id).await?;
    let _ = tx
        .query_opt(
            "SELECT period FROM axond_store_budget_active WHERE namespace = $1 FOR UPDATE",
            &[&id],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    let _ = tx
        .query(
            "SELECT limit_microdollars FROM axond_store_budget
             WHERE namespace = $1 FOR UPDATE",
            &[&id],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    tx.execute(
        "DELETE FROM axond_store_budget WHERE namespace = $1",
        &[&id],
    )
    .await
    .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    tx.execute(
        "DELETE FROM axond_store_budget_active WHERE namespace = $1",
        &[&id],
    )
    .await
    .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    let n = tx
        .execute("DELETE FROM axond_namespace WHERE id = $1", &[&id])
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    if n > 0 {
        // Missing companion row (upgraded DB, or create that skipped the
        // insert): start at 2 so leftover incarnation=1 holds cannot match
        // a later recreate that inserts n=1 ON CONFLICT DO NOTHING.
        tx.execute(
            "INSERT INTO axond_namespace_incarnation (id, n) VALUES ($1, 2)
             ON CONFLICT (id) DO UPDATE SET n = axond_namespace_incarnation.n + 1",
            &[&id],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    }
    Ok(n > 0)
}

/// Active row first, then the spend row: the same order as reserve and settle.
async fn put_budget_tx(
    tx: &Transaction<'_>,
    namespace: &str,
    period: &str,
    limit: i64,
) -> Result<BudgetRecord, StoreError> {
    lock_namespace_id(tx, namespace).await?;
    let exists = tx
        .query_opt(
            "SELECT 1 FROM axond_namespace WHERE id = $1 FOR UPDATE",
            &[&namespace],
        )
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

async fn charge_budget_on(
    client: &impl GenericClient,
    namespace: &str,
    period: &str,
    incarnation: i64,
    actual: i64,
) -> Result<(), StoreError> {
    // One statement: READ COMMITTED re-evaluates `spent + actual` after waiting
    // on the row, so concurrent charges both apply. Incarnation and namespace
    // existence are the same snapshot as the increment (ADR 0064).
    client
        .execute(
            "UPDATE axond_store_budget
             SET spent_microdollars = CASE
                 WHEN spent_microdollars >= 9223372036854775807 - $1 THEN 9223372036854775807
                 ELSE spent_microdollars + $1
             END
             WHERE namespace = $2 AND period = $3
               AND EXISTS (SELECT 1 FROM axond_namespace WHERE id = $2)
               AND COALESCE(
                     (SELECT n FROM axond_namespace_incarnation WHERE id = $2),
                     1
                   ) = $4",
            &[&actual, &namespace, &period, &incarnation],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    Ok(())
}

async fn admit_budget_on(
    client: &impl GenericClient,
    namespace: &str,
) -> Result<BudgetAdmit, StoreError> {
    let Some(row) = client
        .query_opt(
            "SELECT a.period, b.limit_microdollars, b.spent_microdollars,
                    COALESCE(i.n, 1)
             FROM axond_store_budget_active a
             JOIN axond_store_budget b
               ON b.namespace = a.namespace AND b.period = a.period
             LEFT JOIN axond_namespace_incarnation i ON i.id = a.namespace
             WHERE a.namespace = $1",
            &[&namespace],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?
    else {
        return Ok(BudgetAdmit::Exceeded);
    };
    Ok(admit_from_ledger(
        Some(row.get(0)),
        Some(row.get(1)),
        Some(row.get(2)),
        row.get(3),
    ))
}

async fn resolve_namespace_on(
    client: &impl GenericClient,
    id: &str,
) -> Result<Option<NamespaceResolve>, StoreError> {
    let Some(row) = client
        .query_opt(
            "SELECT n.id, n.attrs, n.blocklist,
                    a.period, b.limit_microdollars, b.spent_microdollars,
                    COALESCE(i.n, 1)
             FROM axond_namespace n
             LEFT JOIN axond_store_budget_active a ON a.namespace = n.id
             LEFT JOIN axond_store_budget b
               ON b.namespace = a.namespace AND b.period = a.period
             LEFT JOIN axond_namespace_incarnation i ON i.id = n.id
             WHERE n.id = $1",
            &[&id],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?
    else {
        return Ok(None);
    };
    Ok(Some(NamespaceResolve {
        record: record_from(row.get(0), row.get(1), row.get(2))?,
        admit: admit_from_ledger(row.get(3), row.get(4), row.get(5), row.get(6)),
    }))
}

#[cfg(test)]
impl PostgresStore {
    fn test_pool(slots: Arc<Semaphore>) -> Self {
        let config = tokio_postgres::Config::new();
        Self {
            health: Arc::new(PostgresHealth::new("store", config.clone(), PROBE_BOUND)),
            config,
            idle: Mutex::new(Vec::new()),
            slots,
        }
    }

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

    pub(super) async fn insert_reservation(
        &self,
        id: &str,
        namespace: &str,
        expires_in_ms: i64,
    ) -> Result<(), StoreError> {
        let id = id.to_owned();
        let namespace = namespace.to_owned();
        self.with_client(async move |client| {
            client
                .execute(
                    "INSERT INTO axond_store_budget_reservation
                        (id, namespace, period, amount_microdollars, expires_at, incarnation)
                     VALUES ($1, $2, 'p', 1,
                             now() + ($3 * interval '1 millisecond'), 1)",
                    &[&id, &namespace, &expires_in_ms],
                )
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
            Ok(())
        })
        .await
    }

    pub(super) async fn insert_expired_reservation_tombstone(
        &self,
        id: &str,
        incarnation: i64,
    ) -> Result<(), StoreError> {
        let id = id.to_owned();
        self.with_client(async move |client| {
            client
                .execute(
                    "INSERT INTO axond_store_budget_reservation_tombstone
                        (id, incarnation, expires_at)
                     VALUES ($1, $2, now() - interval '1 second')",
                    &[&id, &incarnation],
                )
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
            Ok(())
        })
        .await
    }

    pub(super) async fn tombstone_exists(&self, id: &str) -> Result<bool, StoreError> {
        let id = id.to_owned();
        self.with_client(async move |client| {
            let exists: bool = client
                .query_one(
                    "SELECT EXISTS(
                         SELECT 1
                         FROM axond_store_budget_reservation_tombstone
                         WHERE id = $1
                     )",
                    &[&id],
                )
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?
                .get(0);
            Ok(exists)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn checkout_times_out_when_pool_is_saturated() {
        let slots = Arc::new(Semaphore::new(POOL_SIZE));
        let mut held = Vec::with_capacity(POOL_SIZE);
        for _ in 0..POOL_SIZE {
            held.push(slots.clone().acquire_owned().await.expect("permit"));
        }
        let store = PostgresStore::test_pool(slots);
        let err = store.checkout().await.expect_err("saturated");
        assert!(
            matches!(err, StoreError::Unavailable(ref message) if message.contains("pool saturated")),
            "{err:?}"
        );
        drop(held);
    }

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
        assert!(
            !BUDGET_DDL.contains("incarnation"),
            "incarnation is store_namespace_incarnation_v1.sql, not a v1 row-shape edit"
        );
        assert!(
            INCARNATION_DDL.contains("CREATE TABLE IF NOT EXISTS axond_namespace_incarnation (")
        );
        assert!(
            INCARNATION_DDL
                .contains("ADD COLUMN IF NOT EXISTS incarnation bigint NOT NULL DEFAULT 1")
        );
        assert!(
            INCARNATION_DDL
                .contains("CREATE TABLE IF NOT EXISTS axond_store_budget_reservation_tombstone (")
        );
        assert!(INCARNATION_DDL.contains(
            "CREATE INDEX IF NOT EXISTS axond_store_budget_reservation_tombstone_expires_idx"
        ));
    }

    #[test]
    fn store_provider_models_ddl_is_embedded() {
        assert!(MODELS_DDL.contains("CREATE TABLE IF NOT EXISTS axond_store_provider_models ("));
        assert!(MODELS_DDL.contains("source      text"));
    }
}
