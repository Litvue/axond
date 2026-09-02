use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio_postgres::{Client, GenericClient, Transaction};

use super::{
    BudgetRecord, BudgetReserve, NamespaceRecord, Store, StoreError, UsageAppend, UsageSummaryRow,
    from_sql_amount, sql_amount, sql_amount_saturating,
};
use crate::backends::health::{BackendHealth, PostgresHealth};

const BUDGET_DDL: &str = include_str!("../../sql/store_budget_v1.sql");
const INCARNATION_DDL: &str = include_str!("../../sql/store_namespace_incarnation_v1.sql");
const USAGE_DDL: &str = include_str!("../../sql/store_usage_v1.sql");
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
        }
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
                 COALESCE((
                     SELECT SUM(r.amount_microdollars)
                     FROM axond_store_budget_reservation r
                     WHERE r.namespace = b.namespace
                       AND r.period = b.period
                       AND r.expires_at > now()
                       AND r.incarnation = COALESCE((
                           SELECT n FROM axond_namespace_incarnation i WHERE i.id = b.namespace
                       ), 1)
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

    async fn namespace_incarnation(&self, id: &str) -> Result<Option<i64>, StoreError> {
        let id = id.to_owned();
        self.with_client(async move |client| {
            Ok(client
                .query_opt(
                    "SELECT n FROM axond_namespace_incarnation WHERE id = $1",
                    &[&id],
                )
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?
                .map(|row| row.get(0)))
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
            // Commit Exceeded too: hold() may have deleted expired rows.
            tx.commit()
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
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
        let actual = sql_amount_saturating(actual_microdollars);
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
}

/// Transaction-scoped advisory lock on the namespace id. Shared by CREATE,
/// DELETE, PUT budget, and settle so those paths cannot deadlock or orphan
/// ledgers. Does not insert an incarnation row for a missing id.
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

async fn settle_tx(
    tx: &Transaction<'_>,
    namespace: &str,
    period: &str,
    reservation_id: &str,
    actual: i64,
) -> Result<(), StoreError> {
    lock_namespace_id(tx, namespace).await?;
    let ns_exists = tx
        .query_opt(
            "SELECT id FROM axond_namespace WHERE id = $1 FOR UPDATE",
            &[&namespace],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?
        .is_some();
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
    // Claim the hold (or its tombstone) in the same statement that removes it
    // so two settlements cannot both observe the incarnation.
    let held_incarnation: Option<i64> = tx
        .query_opt(
            "DELETE FROM axond_store_budget_reservation WHERE id = $1
             RETURNING incarnation",
            &[&reservation_id],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?
        .map(|row| row.get(0));
    let held_incarnation = match held_incarnation {
        Some(n) => Some(n),
        None => tx
            .query_opt(
                "DELETE FROM axond_store_budget_reservation_tombstone WHERE id = $1
                 RETURNING incarnation",
                &[&reservation_id],
            )
            .await
            .map_err(|e| StoreError::Unavailable(e.to_string()))?
            .map(|row| row.get(0)),
    };
    let current: i64 = tx
        .query_opt(
            "SELECT n FROM axond_namespace_incarnation WHERE id = $1",
            &[&namespace],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?
        .map(|row| row.get(0))
        .unwrap_or(1);
    // Unknown reservation id (no row, no tombstone) is a no-op.
    let charge = match held_incarnation {
        Some(incarnation) => ns_exists && incarnation == current,
        None => false,
    };
    if charge {
        tx.execute(
            "UPDATE axond_store_budget
             SET spent_microdollars = CASE
                 WHEN spent_microdollars >= 9223372036854775807 - $1 THEN 9223372036854775807
                 ELSE spent_microdollars + $1
             END
             WHERE namespace = $2 AND period = $3",
            &[&actual, &namespace, &period],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    }
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
    let incarnation: i64 = tx
        .query_opt(
            "SELECT n FROM axond_namespace_incarnation WHERE id = $1",
            &[&namespace],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?
        .map(|row| row.get(0))
        .unwrap_or(1);
    // Vacuum tombstones whose *retention* has elapsed. Copy newly expired
    // holds with a fresh deadline of now()+ttl so a request that outlived
    // its hold can still settle after later admissions.
    tx.execute(
        "DELETE FROM axond_store_budget_reservation_tombstone
         WHERE expires_at < now()",
        &[],
    )
    .await
    .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    tx.execute(
        "INSERT INTO axond_store_budget_reservation_tombstone
            (id, incarnation, expires_at)
         SELECT id, incarnation, now() + ($2::double precision / 1000.0) * interval '1 second'
         FROM axond_store_budget_reservation
         WHERE namespace = $1 AND expires_at <= now()
         ON CONFLICT (id) DO UPDATE SET
            incarnation = EXCLUDED.incarnation,
            expires_at = EXCLUDED.expires_at",
        &[&namespace, &ttl_ms],
    )
    .await
    .map_err(|e| StoreError::Unavailable(e.to_string()))?;
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
             WHERE namespace = $1 AND period = $2 AND expires_at > now()
               AND incarnation = $3",
            &[&namespace, &period, &incarnation],
        )
        .await
        .map_err(|e| StoreError::Unavailable(e.to_string()))?
        .get(0);
    if spent.saturating_add(reserved).saturating_add(estimate) > limit {
        return Ok(BudgetReserve::Exceeded);
    }
    tx.execute(
        "INSERT INTO axond_store_budget_reservation
            (id, namespace, period, amount_microdollars, expires_at, incarnation)
         VALUES ($1, $2, $3, $4, now() + ($5::bigint * interval '1 millisecond'), $6)",
        &[
            &reservation_id,
            &namespace,
            &period,
            &estimate,
            &ttl_ms,
            &incarnation,
        ],
    )
    .await
    .map_err(|e| StoreError::Unavailable(e.to_string()))?;
    Ok(BudgetReserve::Allowed { period })
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
}
