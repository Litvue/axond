use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::Value;

use super::{
    BudgetAdmit, BudgetCadence, BudgetClock, BudgetPolicy, BudgetRecord, NamespaceRecord,
    NamespaceResolve, ProviderModels, Store, StoreError, UsageAppend, UsageSummaryRow,
    admit_from_ledger, from_sql_amount, monthly_period_key, sql_amount, sql_amount_saturating,
    validate_timezone,
};

pub struct SqliteStore {
    conn: Arc<Mutex<Connection>>,
    clock: Arc<dyn BudgetClock>,
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS axond_namespace (
    id TEXT PRIMARY KEY NOT NULL,
    attrs TEXT NOT NULL DEFAULT '{}',
    blocklist TEXT
);
CREATE TABLE IF NOT EXISTS axond_namespace_incarnation (
    id TEXT PRIMARY KEY NOT NULL,
    n INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS axond_store_budget (
    namespace TEXT NOT NULL,
    period TEXT NOT NULL,
    limit_microdollars INTEGER NOT NULL,
    spent_microdollars INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (namespace, period)
);
CREATE TABLE IF NOT EXISTS axond_store_budget_active (
    namespace TEXT PRIMARY KEY NOT NULL,
    period TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS axond_store_budget_cadence (
    namespace TEXT PRIMARY KEY NOT NULL,
    cadence TEXT NOT NULL,
    limit_microdollars INTEGER NOT NULL,
    timezone TEXT NOT NULL DEFAULT 'UTC'
);
CREATE TABLE IF NOT EXISTS axond_store_budget_reservation (
    id TEXT PRIMARY KEY NOT NULL,
    namespace TEXT NOT NULL,
    period TEXT NOT NULL,
    amount_microdollars INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    incarnation INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS axond_store_budget_reservation_scope_idx
    ON axond_store_budget_reservation (namespace, period, expires_at);
CREATE TABLE IF NOT EXISTS axond_store_budget_reservation_tombstone (
    id TEXT PRIMARY KEY NOT NULL,
    incarnation INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS axond_store_budget_reservation_tombstone_expires_idx
    ON axond_store_budget_reservation_tombstone (expires_at);
CREATE TABLE IF NOT EXISTS axond_store_usage (
    request_id TEXT PRIMARY KEY NOT NULL,
    namespace TEXT NOT NULL,
    period TEXT,
    model TEXT NOT NULL,
    status TEXT NOT NULL,
    cost_microdollars INTEGER,
    recorded_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER))
);
CREATE INDEX IF NOT EXISTS axond_store_usage_ns_period
    ON axond_store_usage (namespace, period);
CREATE TABLE IF NOT EXISTS axond_store_provider_models (
    provider TEXT PRIMARY KEY NOT NULL,
    fetched_at TEXT,
    stale INTEGER NOT NULL,
    models TEXT NOT NULL,
    source TEXT
);
";

impl SqliteStore {
    pub fn open(path: &str) -> Result<Self, StoreError> {
        let conn = Connection::open(path).map_err(unavailable)?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(unavailable)?;
        conn.pragma_update(None, "busy_timeout", 5000)
            .map_err(unavailable)?;
        conn.execute_batch(SCHEMA).map_err(unavailable)?;
        migrate_reservation_incarnation(&conn)?;
        migrate_tombstone_expires_at(&conn)?;
        migrate_provider_models_source(&conn)?;
        sweep_expired_holds(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            clock: Arc::new(super::SystemClock),
        })
    }

    #[cfg(test)]
    pub(crate) fn with_clock(mut self, clock: Arc<dyn BudgetClock>) -> Self {
        self.clock = clock;
        self
    }

    /// Seed TOML `[[namespace]]` rows without `spawn_blocking`.
    ///
    /// `AppState::new` opens SQLite on the caller's thread (including
    /// `#[tokio::test]` and plain `#[test]`). Going through the async `Store`
    /// trait would `block_on` a `spawn_blocking` future, which panics when no
    /// Tokio runtime is on the stack and can stall a worker when one is.
    pub fn seed_config_namespaces_sync(
        &self,
        namespaces: &[crate::config::Namespace],
    ) -> Result<(), StoreError> {
        let namespaces: Vec<_> = namespaces
            .iter()
            .filter(|namespace| super::validate_namespace_id(&namespace.id).is_ok())
            .collect();
        let conn = self
            .conn
            .lock()
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        for namespace in namespaces {
            match conn.execute(
                "INSERT INTO axond_namespace (id, attrs, blocklist) VALUES (?1, '{}', NULL)",
                params![namespace.id],
            ) {
                Ok(_) => {}
                Err(rusqlite::Error::SqliteFailure(err, _))
                    if err.code == rusqlite::ErrorCode::ConstraintViolation => {}
                Err(err) => return Err(unavailable(err)),
            }
        }
        Ok(())
    }

    async fn with_conn<T, F>(&self, f: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, StoreError> + Send + 'static,
    {
        let conn = Arc::clone(&self.conn);
        let run = move || {
            let mut guard = conn
                .lock()
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
            f(&mut guard)
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::spawn_blocking(run)
                .await
                .map_err(|e| StoreError::Unavailable(e.to_string()))?
        } else {
            run()
        }
    }
}

fn unavailable(err: rusqlite::Error) -> StoreError {
    StoreError::Unavailable(err.to_string())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn sweep_expired_holds(conn: &Connection) -> Result<(), StoreError> {
    conn.execute(
        "DELETE FROM axond_store_budget_reservation WHERE expires_at <= ?1",
        params![now_ms()],
    )
    .map_err(unavailable)?;
    conn.execute(
        "DELETE FROM axond_store_budget_reservation_tombstone WHERE expires_at < ?1",
        params![now_ms()],
    )
    .map_err(unavailable)?;
    Ok(())
}

/// `CREATE TABLE IF NOT EXISTS` does not add columns to an existing file.
fn table_has_column(conn: &Connection, table: &str, column: &str) -> Result<bool, StoreError> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .map_err(unavailable)?;
    let mut rows = stmt.query([]).map_err(unavailable)?;
    while let Some(row) = rows.next().map_err(unavailable)? {
        let name: String = row.get(1).map_err(unavailable)?;
        if name == column {
            return Ok(true);
        }
    }
    Ok(false)
}

fn migrate_reservation_incarnation(conn: &Connection) -> Result<(), StoreError> {
    if !table_has_column(conn, "axond_store_budget_reservation", "incarnation")? {
        conn.execute(
            "ALTER TABLE axond_store_budget_reservation
             ADD COLUMN incarnation INTEGER NOT NULL DEFAULT 1",
            [],
        )
        .map_err(unavailable)?;
    }
    Ok(())
}

fn migrate_tombstone_expires_at(conn: &Connection) -> Result<(), StoreError> {
    if !table_has_column(
        conn,
        "axond_store_budget_reservation_tombstone",
        "expires_at",
    )? {
        conn.execute(
            "ALTER TABLE axond_store_budget_reservation_tombstone
             ADD COLUMN expires_at INTEGER NOT NULL DEFAULT 0",
            [],
        )
        .map_err(unavailable)?;
    }
    Ok(())
}

fn migrate_provider_models_source(conn: &Connection) -> Result<(), StoreError> {
    if !table_has_column(conn, "axond_store_provider_models", "source")? {
        conn.execute(
            "ALTER TABLE axond_store_provider_models ADD COLUMN source TEXT",
            [],
        )
        .map_err(unavailable)?;
    }
    Ok(())
}

fn namespace_exists(conn: &Connection, id: &str) -> Result<bool, StoreError> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM axond_namespace WHERE id = ?1",
            params![id],
            |_| Ok(()),
        )
        .optional()
        .map_err(unavailable)?
        .is_some())
}

fn insert_usage(conn: &Connection, event: &UsageAppend) -> Result<(), StoreError> {
    let cost = event.cost_microdollars.map(sql_amount_saturating);
    conn.execute(
        "INSERT OR IGNORE INTO axond_store_usage
            (request_id, namespace, period, model, status, cost_microdollars, recorded_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, CAST(strftime('%s','now') AS INTEGER))",
        params![
            event.request_id,
            event.namespace,
            event.period,
            event.model,
            event.status,
            cost,
        ],
    )
    .map_err(unavailable)?;
    Ok(())
}

fn row_to_record(
    id: String,
    attrs: String,
    blocklist: Option<String>,
) -> Result<NamespaceRecord, StoreError> {
    // Corrupt stored JSON is an operational store failure (503), not a client
    // bad_request: the caller did not supply this payload.
    let attrs: Value = serde_json::from_str(&attrs)
        .map_err(|error| StoreError::Unavailable(format!("namespace `{id}` attrs: {error}")))?;
    let blocklist = match blocklist {
        None => None,
        Some(raw) => Some(serde_json::from_str(&raw).map_err(|error| {
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
impl Store for SqliteStore {
    async fn put_namespace(&self, ns: NamespaceRecord) -> Result<(), StoreError> {
        self.with_conn(move |conn| {
            let attrs = ns.attrs.to_string();
            let blocklist = ns
                .blocklist
                .as_ref()
                .map(|list| serde_json::to_string(list).unwrap_or_else(|_| "[]".into()));
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(unavailable)?;
            match tx.execute(
                "INSERT INTO axond_namespace (id, attrs, blocklist) VALUES (?1, ?2, ?3)",
                params![ns.id, attrs, blocklist],
            ) {
                Ok(_) => {
                    tx.execute(
                        "INSERT INTO axond_namespace_incarnation (id, n) VALUES (?1, 1)
                         ON CONFLICT(id) DO NOTHING",
                        params![ns.id],
                    )
                    .map_err(unavailable)?;
                    tx.commit().map_err(unavailable)?;
                    Ok(())
                }
                Err(rusqlite::Error::SqliteFailure(err, _))
                    if err.code == rusqlite::ErrorCode::ConstraintViolation =>
                {
                    Err(StoreError::Duplicate(ns.id))
                }
                Err(err) => Err(unavailable(err)),
            }
        })
        .await
    }

    async fn get_namespace(&self, id: &str) -> Result<Option<NamespaceRecord>, StoreError> {
        let id = id.to_string();
        self.with_conn(move |conn| {
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
        })
        .await
    }

    async fn resolve_namespace(&self, id: &str) -> Result<Option<NamespaceResolve>, StoreError> {
        let id = id.to_string();
        let clock = Arc::clone(&self.clock);
        self.with_conn(move |conn| resolve_namespace_on(conn, &id, &clock))
            .await
    }

    async fn list_namespaces(
        &self,
        cursor: Option<String>,
        limit: u32,
    ) -> Result<(Vec<NamespaceRecord>, Option<String>), StoreError> {
        let limit = limit.clamp(1, 1000);
        self.with_conn(move |conn| {
            let fetch = i64::from(limit) + 1;
            let mut stmt = conn
                .prepare(
                    "SELECT id, attrs, blocklist FROM axond_namespace
                     WHERE (?1 IS NULL OR id > ?1)
                     ORDER BY id
                     LIMIT ?2",
                )
                .map_err(unavailable)?;
            let rows = stmt
                .query_map(params![cursor, fetch], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                })
                .map_err(unavailable)?;
            let mut raw = Vec::new();
            for row in rows {
                raw.push(row.map_err(unavailable)?);
            }
            let has_more = raw.len() > limit as usize;
            if has_more {
                raw.truncate(limit as usize);
            }
            let mut out = Vec::with_capacity(raw.len());
            for (id, attrs, blocklist) in raw {
                out.push(row_to_record(id, attrs, blocklist)?);
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
        let id = id.to_string();
        self.with_conn(move |conn| {
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
        })
        .await
    }

    async fn delete_namespace(&self, id: &str) -> Result<bool, StoreError> {
        let id = id.to_string();
        self.with_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(unavailable)?;
            let existed = namespace_exists(&tx, &id)?;
            if existed {
                // Keep reservation rows. Implicit n=1 when the companion row
                // is missing (legacy id).
                tx.execute(
                    "INSERT INTO axond_namespace_incarnation (id, n) VALUES (?1, 2)
                     ON CONFLICT(id) DO UPDATE SET n = n + 1",
                    params![id],
                )
                .map_err(unavailable)?;
            }
            tx.execute(
                "DELETE FROM axond_store_budget WHERE namespace = ?1",
                params![id],
            )
            .map_err(unavailable)?;
            tx.execute(
                "DELETE FROM axond_store_budget_active WHERE namespace = ?1",
                params![id],
            )
            .map_err(unavailable)?;
            tx.execute(
                "DELETE FROM axond_store_budget_cadence WHERE namespace = ?1",
                params![id],
            )
            .map_err(unavailable)?;
            let n = tx
                .execute("DELETE FROM axond_namespace WHERE id = ?1", params![id])
                .map_err(unavailable)?;
            tx.commit().map_err(unavailable)?;
            Ok(n > 0)
        })
        .await
    }

    fn seed_namespaces_blocking(
        &self,
        namespaces: &[crate::config::Namespace],
    ) -> Result<(), StoreError> {
        self.seed_config_namespaces_sync(namespaces)
    }

    async fn put_budget(
        &self,
        namespace: &str,
        period: &str,
        limit_microdollars: u64,
    ) -> Result<BudgetRecord, StoreError> {
        let namespace = namespace.to_string();
        let period = period.to_string();
        let limit = sql_amount(limit_microdollars)?;
        let clock = Arc::clone(&self.clock);
        self.with_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(unavailable)?;
            if !namespace_exists(&tx, &namespace)? {
                return Err(StoreError::NotFound(namespace));
            }
            tx.execute(
                "INSERT INTO axond_store_budget_active (namespace, period) VALUES (?1, ?2)
                 ON CONFLICT (namespace) DO UPDATE SET period = excluded.period",
                params![namespace, period],
            )
            .map_err(unavailable)?;
            tx.execute(
                "INSERT INTO axond_store_budget (namespace, period, limit_microdollars, spent_microdollars)
                 VALUES (?1, ?2, ?3, 0)
                 ON CONFLICT (namespace, period) DO UPDATE SET
                    limit_microdollars = excluded.limit_microdollars",
                params![namespace, period, limit],
            )
            .map_err(unavailable)?;
            let rec = read_budget(&tx, &namespace, &period, &clock)?;
            tx.commit().map_err(unavailable)?;
            rec.ok_or_else(|| StoreError::Unavailable("budget row missing after put".into()))
        })
        .await
    }

    async fn get_budget(
        &self,
        namespace: &str,
        period: &str,
    ) -> Result<Option<BudgetRecord>, StoreError> {
        let namespace = namespace.to_string();
        let period = period.to_string();
        let clock = Arc::clone(&self.clock);
        self.with_conn(move |conn| read_budget(conn, &namespace, &period, &clock))
            .await
    }

    async fn put_budget_policy(
        &self,
        namespace: &str,
        cadence: BudgetCadence,
        limit_microdollars: u64,
        timezone: &str,
        fixed_period: Option<&str>,
    ) -> Result<BudgetPolicy, StoreError> {
        let namespace = namespace.to_owned();
        let timezone = timezone.to_owned();
        let fixed_period = fixed_period.map(str::to_owned);
        let limit = sql_amount(limit_microdollars)?;
        let tz = validate_timezone(&timezone)?;
        let clock = Arc::clone(&self.clock);
        self.with_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(unavailable)?;
            if !namespace_exists(&tx, &namespace)? {
                return Err(StoreError::NotFound(namespace));
            }
            let period = match cadence {
                BudgetCadence::Monthly => monthly_period_key(clock.now(), &tz),
                BudgetCadence::Fixed => {
                    if let Some(period) = fixed_period {
                        period
                    } else {
                        tx.query_row(
                            "SELECT period FROM axond_store_budget_active WHERE namespace = ?1",
                            params![namespace],
                            |row| row.get(0),
                        )
                        .optional()
                        .map_err(unavailable)?
                        .ok_or_else(|| {
                            StoreError::Invalid(
                                "fixed cadence needs a period: the namespace has no active period"
                                    .into(),
                            )
                        })?
                    }
                }
            };
            tx.execute(
                "INSERT INTO axond_store_budget_cadence
                    (namespace, cadence, limit_microdollars, timezone)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(namespace) DO UPDATE SET
                    cadence = excluded.cadence,
                    limit_microdollars = excluded.limit_microdollars,
                    timezone = excluded.timezone",
                params![namespace, cadence.as_str(), limit, timezone],
            )
            .map_err(unavailable)?;
            if cadence == BudgetCadence::Fixed {
                tx.execute(
                    "INSERT INTO axond_store_budget_active (namespace, period) VALUES (?1, ?2)
                     ON CONFLICT(namespace) DO UPDATE SET period = excluded.period",
                    params![namespace, period],
                )
                .map_err(unavailable)?;
            }
            tx.execute(
                "INSERT INTO axond_store_budget
                    (namespace, period, limit_microdollars, spent_microdollars)
                 VALUES (?1, ?2, ?3, 0)
                 ON CONFLICT(namespace, period) DO UPDATE SET
                    limit_microdollars = excluded.limit_microdollars",
                params![namespace, period, limit],
            )
            .map_err(unavailable)?;
            let policy = read_budget_policy(&tx, &namespace, &clock)?;
            tx.commit().map_err(unavailable)?;
            policy.ok_or_else(|| StoreError::Unavailable("budget policy missing after put".into()))
        })
        .await
    }

    async fn get_budget_policy(&self, namespace: &str) -> Result<Option<BudgetPolicy>, StoreError> {
        let namespace = namespace.to_owned();
        let clock = Arc::clone(&self.clock);
        self.with_conn(move |conn| {
            if !namespace_exists(conn, &namespace)? {
                return Err(StoreError::NotFound(namespace));
            }
            read_budget_policy(conn, &namespace, &clock)
        })
        .await
    }

    async fn admit_budget(&self, namespace: &str) -> Result<BudgetAdmit, StoreError> {
        let namespace = namespace.to_string();
        let clock = Arc::clone(&self.clock);
        self.with_conn(move |conn| admit_budget_on(conn, &namespace, &clock))
            .await
    }

    async fn charge_budget(
        &self,
        namespace: &str,
        period: &str,
        incarnation: i64,
        actual_microdollars: u64,
    ) -> Result<(), StoreError> {
        let namespace = namespace.to_string();
        let period = period.to_string();
        let actual = sql_amount_saturating(actual_microdollars);
        self.with_conn(move |conn| charge_budget_on(conn, &namespace, &period, incarnation, actual))
            .await
    }

    async fn append_usage(&self, event: UsageAppend) -> Result<(), StoreError> {
        self.with_conn(move |conn| insert_usage(conn, &event)).await
    }

    fn blocking_usage_index(&self) -> bool {
        true
    }

    fn append_usage_sync(&self, event: UsageAppend) -> Result<(), StoreError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        insert_usage(&conn, &event)
    }

    async fn summarize_usage(
        &self,
        namespace: &str,
        period: &str,
    ) -> Result<Vec<UsageSummaryRow>, StoreError> {
        let namespace = namespace.to_string();
        let period = period.to_string();
        self.with_conn(move |conn| {
            // SQLite SUM overflows INTEGER (and then becomes REAL); fold in Rust.
            let mut stmt = conn
                .prepare(
                    "SELECT model, status, COALESCE(cost_microdollars, 0)
                     FROM axond_store_usage
                     WHERE namespace = ?1 AND period = ?2",
                )
                .map_err(unavailable)?;
            let rows = stmt
                .query_map(params![namespace, period], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })
                .map_err(unavailable)?;
            let mut grouped = BTreeMap::<(String, String), (u64, i64)>::new();
            for row in rows {
                let (model, status, cost) = row.map_err(unavailable)?;
                let entry = grouped.entry((model, status)).or_default();
                entry.0 = entry.0.saturating_add(1);
                entry.1 = entry.1.saturating_add(cost);
            }
            Ok(grouped
                .into_iter()
                .map(|((model, status), (count, cost))| UsageSummaryRow {
                    model,
                    status,
                    count,
                    cost_microdollars: from_sql_amount(cost),
                })
                .collect())
        })
        .await
    }

    async fn get_provider_models(
        &self,
        provider: &str,
    ) -> Result<Option<ProviderModels>, StoreError> {
        let provider = provider.to_string();
        self.with_conn(move |conn| {
            conn.query_row(
                "SELECT provider, fetched_at, stale, models, source
                 FROM axond_store_provider_models WHERE provider = ?1",
                params![provider],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                },
            )
            .optional()
            .map_err(unavailable)?
            .map(|(provider, fetched_at, stale, models, source)| {
                sqlite_provider_models(provider, fetched_at, stale, models, source)
            })
            .transpose()
        })
        .await
    }

    async fn list_provider_models(&self) -> Result<Vec<ProviderModels>, StoreError> {
        self.with_conn(move |conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT provider, fetched_at, stale, models, source
                     FROM axond_store_provider_models ORDER BY provider",
                )
                .map_err(unavailable)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                })
                .map_err(unavailable)?;
            let mut out = Vec::new();
            for row in rows {
                let (provider, fetched_at, stale, models, source) = row.map_err(unavailable)?;
                out.push(sqlite_provider_models(
                    provider, fetched_at, stale, models, source,
                )?);
            }
            Ok(out)
        })
        .await
    }

    async fn put_provider_models(&self, row: ProviderModels) -> Result<(), StoreError> {
        self.with_conn(move |conn| {
            let models = serde_json::to_string(&row.data).map_err(|error| {
                StoreError::Unavailable(format!("provider `{}` models: {error}", row.provider))
            })?;
            conn.execute(
                "INSERT INTO axond_store_provider_models (provider, fetched_at, stale, models, source)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT (provider) DO UPDATE SET
                    fetched_at = excluded.fetched_at,
                    stale = excluded.stale,
                    models = excluded.models,
                    source = excluded.source
                 WHERE axond_store_provider_models.source IS NOT DISTINCT FROM excluded.source
                    OR axond_store_provider_models.stale = 1",
                params![
                    row.provider,
                    row.fetched_at,
                    if row.stale { 1 } else { 0 },
                    models,
                    row.source,
                ],
            )
            .map_err(unavailable)?;
            Ok(())
        })
        .await
    }

    async fn mark_provider_models_stale_unless_source(
        &self,
        provider: &str,
        source: &str,
    ) -> Result<(), StoreError> {
        let provider = provider.to_string();
        let source = source.to_string();
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE axond_store_provider_models SET stale = 1
                 WHERE provider = ?1 AND (source IS NULL OR source != ?2)",
                params![provider, source],
            )
            .map_err(unavailable)?;
            Ok(())
        })
        .await
    }

    async fn mark_provider_models_stale_if_source(
        &self,
        provider: &str,
        source: &str,
    ) -> Result<(), StoreError> {
        let provider = provider.to_string();
        let source = source.to_string();
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE axond_store_provider_models SET stale = 1
                 WHERE provider = ?1 AND source IS NOT NULL AND source = ?2",
                params![provider, source],
            )
            .map_err(unavailable)?;
            Ok(())
        })
        .await
    }
}

fn sqlite_provider_models(
    provider: String,
    fetched_at: Option<String>,
    stale: i64,
    models: String,
    source: Option<String>,
) -> Result<ProviderModels, StoreError> {
    let data: Vec<Value> = serde_json::from_str(&models).map_err(|error| {
        StoreError::Unavailable(format!("provider `{provider}` models: {error}"))
    })?;
    Ok(ProviderModels {
        provider,
        fetched_at,
        stale: stale != 0,
        data,
        source,
    })
}

fn admit_monthly(
    conn: &mut Connection,
    namespace: &str,
    cadence_limit: Option<i64>,
    timezone: Option<String>,
    incarnation: i64,
    clock: &Arc<dyn BudgetClock>,
) -> Result<BudgetAdmit, StoreError> {
    let timezone = timezone.unwrap_or_else(|| super::DEFAULT_TIMEZONE.into());
    let tz = validate_timezone(&timezone)?;
    let period = monthly_period_key(clock.now(), &tz);
    if let Some((limit, spent)) = conn
        .query_row(
            "SELECT limit_microdollars, spent_microdollars
             FROM axond_store_budget WHERE namespace = ?1 AND period = ?2",
            params![namespace, period],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(unavailable)?
    {
        return Ok(admit_from_ledger(
            Some(period),
            Some(limit),
            Some(spent),
            incarnation,
        ));
    }

    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(unavailable)?;
    if !namespace_exists(&tx, namespace)? {
        return Ok(BudgetAdmit::Exceeded);
    }
    tx.execute(
        "INSERT OR IGNORE INTO axond_store_budget
            (namespace, period, limit_microdollars, spent_microdollars)
         VALUES (?1, ?2, ?3, 0)",
        params![
            namespace,
            period,
            cadence_limit
                .ok_or_else(|| StoreError::Unavailable("monthly cadence limit missing".into()))?
        ],
    )
    .map_err(unavailable)?;
    let row = tx
        .query_row(
            "SELECT limit_microdollars, spent_microdollars
             FROM axond_store_budget WHERE namespace = ?1 AND period = ?2",
            params![namespace, period],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(unavailable)?;
    let (limit, spent) = row.unwrap_or((None, None));
    let result = admit_from_ledger(Some(period), limit, spent, incarnation);
    tx.commit().map_err(unavailable)?;
    Ok(result)
}

fn admit_budget_on(
    conn: &mut Connection,
    namespace: &str,
    clock: &Arc<dyn BudgetClock>,
) -> Result<BudgetAdmit, StoreError> {
    let row = conn
        .query_row(
            "SELECT c.cadence, c.limit_microdollars, c.timezone,
                    a.period, b.limit_microdollars, b.spent_microdollars,
                    COALESCE(i.n, 1)
             FROM axond_namespace n
             LEFT JOIN axond_store_budget_cadence c ON c.namespace = n.id
             LEFT JOIN axond_store_budget_active a ON a.namespace = n.id
             LEFT JOIN axond_store_budget b
               ON b.namespace = a.namespace AND b.period = a.period
             LEFT JOIN axond_namespace_incarnation i ON i.id = n.id
             WHERE n.id = ?1",
            params![namespace],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            },
        )
        .optional()
        .map_err(unavailable)?;
    let Some((cadence, cadence_limit, timezone, period, limit, spent, incarnation)) = row else {
        return Ok(BudgetAdmit::Exceeded);
    };
    let result = if cadence.as_deref() == Some("monthly") {
        admit_monthly(conn, namespace, cadence_limit, timezone, incarnation, clock)?
    } else {
        admit_from_ledger(period, limit, spent, incarnation)
    };
    Ok(result)
}

fn resolve_namespace_on(
    conn: &mut Connection,
    id: &str,
    clock: &Arc<dyn BudgetClock>,
) -> Result<Option<NamespaceResolve>, StoreError> {
    let row = conn
        .query_row(
            "SELECT n.id, n.attrs, n.blocklist,
                    c.cadence, c.limit_microdollars, c.timezone,
                    a.period, b.limit_microdollars, b.spent_microdollars,
                    COALESCE(i.n, 1)
             FROM axond_namespace n
             LEFT JOIN axond_store_budget_cadence c ON c.namespace = n.id
             LEFT JOIN axond_store_budget_active a ON a.namespace = n.id
             LEFT JOIN axond_store_budget b
               ON b.namespace = a.namespace AND b.period = a.period
             LEFT JOIN axond_namespace_incarnation i ON i.id = n.id
             WHERE n.id = ?1",
            params![id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                    row.get::<_, Option<i64>>(8)?,
                    row.get::<_, i64>(9)?,
                ))
            },
        )
        .optional()
        .map_err(unavailable)?;
    let Some((
        id,
        attrs,
        blocklist,
        cadence,
        cadence_limit,
        timezone,
        period,
        limit,
        spent,
        incarnation,
    )) = row
    else {
        return Ok(None);
    };
    let admit = if cadence.as_deref() == Some("monthly") {
        admit_monthly(conn, &id, cadence_limit, timezone, incarnation, clock)?
    } else {
        admit_from_ledger(period, limit, spent, incarnation)
    };
    Ok(Some(NamespaceResolve {
        record: row_to_record(id, attrs, blocklist)?,
        admit,
    }))
}

fn charge_budget_on(
    conn: &Connection,
    namespace: &str,
    period: &str,
    incarnation: i64,
    actual: i64,
) -> Result<(), StoreError> {
    conn.execute(
        "UPDATE axond_store_budget
         SET spent_microdollars = CASE
             WHEN spent_microdollars >= 9223372036854775807 - ?1 THEN 9223372036854775807
             ELSE spent_microdollars + ?1
         END
         WHERE namespace = ?2 AND period = ?3
           AND EXISTS (SELECT 1 FROM axond_namespace WHERE id = ?2)
           AND COALESCE(
                 (SELECT n FROM axond_namespace_incarnation WHERE id = ?2),
                 1
               ) = ?4",
        params![actual, namespace, period, incarnation],
    )
    .map_err(unavailable)?;
    Ok(())
}

fn read_budget(
    conn: &Connection,
    namespace: &str,
    period: &str,
    clock: &Arc<dyn BudgetClock>,
) -> Result<Option<BudgetRecord>, StoreError> {
    let row = conn
        .query_row(
            "SELECT
             b.limit_microdollars,
             b.spent_microdollars,
             c.cadence,
             c.timezone
         FROM axond_store_budget b
         LEFT JOIN axond_store_budget_cadence c ON c.namespace = b.namespace
         WHERE b.namespace = ?1 AND b.period = ?2",
            params![namespace, period],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            },
        )
        .optional()
        .map_err(unavailable)?;
    let Some((limit, spent, cadence, timezone)) = row else {
        return Ok(None);
    };
    let active = if cadence.as_deref() == Some("monthly") {
        let timezone = timezone.unwrap_or_else(|| super::DEFAULT_TIMEZONE.into());
        let tz = validate_timezone(&timezone)?;
        monthly_period_key(clock.now(), &tz) == period
    } else {
        conn.query_row(
            "SELECT EXISTS (
                 SELECT 1 FROM axond_store_budget_active
                 WHERE namespace = ?1 AND period = ?2
             )",
            params![namespace, period],
            |row| row.get(0),
        )
        .map_err(unavailable)?
    };
    Ok(Some(BudgetRecord::new(
        namespace,
        period,
        from_sql_amount(limit),
        from_sql_amount(spent),
        active,
    )))
}

fn read_budget_policy(
    conn: &Connection,
    namespace: &str,
    clock: &Arc<dyn BudgetClock>,
) -> Result<Option<BudgetPolicy>, StoreError> {
    let cadence = conn
        .query_row(
            "SELECT cadence, limit_microdollars, timezone
             FROM axond_store_budget_cadence WHERE namespace = ?1",
            params![namespace],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()
        .map_err(unavailable)?;
    let Some((cadence, policy_limit, timezone)) = cadence else {
        let Some(period) = conn
            .query_row(
                "SELECT period FROM axond_store_budget_active WHERE namespace = ?1",
                params![namespace],
                |row| row.get(0),
            )
            .optional()
            .map_err(unavailable)?
        else {
            return Ok(None);
        };
        let Some((limit, spent)) = conn
            .query_row(
                "SELECT limit_microdollars, spent_microdollars FROM axond_store_budget
                 WHERE namespace = ?1 AND period = ?2",
                params![namespace, period],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(unavailable)?
        else {
            return Ok(None);
        };
        let limit = from_sql_amount(limit);
        let spent = from_sql_amount(spent);
        return Ok(Some(BudgetPolicy {
            namespace: namespace.into(),
            cadence: BudgetCadence::Fixed,
            limit_microdollars: limit,
            timezone: super::DEFAULT_TIMEZONE.into(),
            period,
            spent_microdollars: spent,
            reserved_microdollars: 0,
            remaining_microdollars: limit.saturating_sub(spent),
            active: true,
        }));
    };
    let cadence = BudgetCadence::parse(&cadence)
        .ok_or_else(|| StoreError::Unavailable("invalid budget cadence".into()))?;
    let tz = validate_timezone(&timezone)?;
    let period = if cadence == BudgetCadence::Monthly {
        monthly_period_key(clock.now(), &tz)
    } else {
        conn.query_row(
            "SELECT period FROM axond_store_budget_active WHERE namespace = ?1",
            params![namespace],
            |row| row.get(0),
        )
        .optional()
        .map_err(unavailable)?
        .unwrap_or_default()
    };
    let row = conn
        .query_row(
            "SELECT limit_microdollars, spent_microdollars FROM axond_store_budget
             WHERE namespace = ?1 AND period = ?2",
            params![namespace, period],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(unavailable)?;
    let (limit, spent) = row
        .map(|(limit, spent)| (from_sql_amount(limit), from_sql_amount(spent)))
        .unwrap_or((from_sql_amount(policy_limit), 0));
    Ok(Some(BudgetPolicy {
        namespace: namespace.into(),
        cadence,
        limit_microdollars: limit,
        timezone,
        period,
        spent_microdollars: spent,
        reserved_microdollars: 0,
        remaining_microdollars: limit.saturating_sub(spent),
        active: true,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cadence_store() -> (SqliteStore, Arc<super::super::FixedClock>) {
        let clock = Arc::new(super::super::FixedClock(std::sync::Mutex::new(
            "2026-09-30T23:30:00Z".parse().expect("timestamp"),
        )));
        (
            SqliteStore::open(":memory:")
                .expect("memory sqlite")
                .with_clock(Arc::clone(&clock) as Arc<dyn BudgetClock>),
            clock,
        )
    }

    async fn add_namespace(store: &SqliteStore, id: &str) {
        store
            .put_namespace(NamespaceRecord {
                id: id.into(),
                attrs: serde_json::json!({}),
                blocklist: None,
            })
            .await
            .expect("namespace");
    }

    #[tokio::test]
    async fn monthly_budget_lazily_creates_and_rolls_over() {
        let (store, clock) = cadence_store();
        add_namespace(&store, "ns").await;
        let policy = store
            .put_budget_policy("ns", BudgetCadence::Monthly, 1_000, "UTC", None)
            .await
            .expect("policy");
        assert_eq!(policy.period, "2026-09");
        let rows: i64 = store
            .conn
            .lock()
            .expect("lock")
            .query_row(
                "SELECT count(*) FROM axond_store_budget WHERE namespace = 'ns'",
                [],
                |row| row.get(0),
            )
            .expect("count");
        assert_eq!(rows, 1, "PUT creates the current monthly row");
        store
            .charge_budget("ns", "2026-09", 1, 600)
            .await
            .expect("charge");
        clock.set("2026-10-01T00:00:00Z".parse().expect("timestamp"));
        assert!(matches!(
            store.admit_budget("ns").await.expect("admit"),
            BudgetAdmit::Allowed { ref period, .. } if period == "2026-10"
        ));
        let old = store
            .get_budget("ns", "2026-09")
            .await
            .expect("old")
            .expect("old row");
        assert_eq!(old.spent_microdollars, 600);
        assert!(!old.active);
        let current = store
            .get_budget("ns", "2026-10")
            .await
            .expect("current")
            .expect("current row");
        assert_eq!(current.limit_microdollars, 1_000);
        assert!(current.active);
    }

    #[tokio::test]
    async fn monthly_cadence_wins_over_legacy_active_and_limit_changes_apply() {
        let (store, clock) = cadence_store();
        add_namespace(&store, "ns").await;
        store
            .put_budget_policy("ns", BudgetCadence::Monthly, 1_000, "UTC", None)
            .await
            .expect("policy");
        store.put_budget("ns", "legacy", 10).await.expect("legacy");
        assert!(matches!(
            store.admit_budget("ns").await.expect("admit"),
            BudgetAdmit::Allowed { ref period, .. } if period == "2026-09"
        ));
        assert!(
            !store
                .get_budget("ns", "legacy")
                .await
                .expect("legacy")
                .expect("row")
                .active
        );
        store
            .charge_budget("ns", "2026-09", 1, 600)
            .await
            .expect("charge");
        store
            .put_budget_policy("ns", BudgetCadence::Monthly, 500, "UTC", None)
            .await
            .expect("lower policy");
        assert!(matches!(
            store.admit_budget("ns").await.expect("admit"),
            BudgetAdmit::Exceeded
        ));
        clock.set("2026-10-01T00:00:00Z".parse().expect("timestamp"));
        let policy = store
            .get_budget_policy("ns")
            .await
            .expect("get policy")
            .expect("policy");
        assert_eq!(policy.limit_microdollars, 500);
        assert_eq!(policy.period, "2026-10");
    }

    #[tokio::test]
    async fn fixed_cadence_and_legacy_policy_views_are_compatible() {
        let (store, _) = cadence_store();
        add_namespace(&store, "ns").await;
        assert!(matches!(
            store
                .put_budget_policy("ns", BudgetCadence::Fixed, 10, "UTC", None)
                .await,
            Err(StoreError::Invalid(_))
        ));
        store.put_budget("ns", "legacy", 10).await.expect("legacy");
        let synthesized = store
            .get_budget_policy("ns")
            .await
            .expect("get")
            .expect("policy");
        assert_eq!(synthesized.cadence, BudgetCadence::Fixed);
        assert_eq!(synthesized.period, "legacy");
        store
            .put_budget_policy("ns", BudgetCadence::Fixed, 20, "UTC", Some("p1"))
            .await
            .expect("fixed");
        assert!(matches!(
            store.admit_budget("ns").await.expect("admit"),
            BudgetAdmit::Allowed { ref period, .. } if period == "p1"
        ));
    }

    #[tokio::test]
    async fn deleting_namespace_deletes_cadence_policy() {
        let (store, _) = cadence_store();
        add_namespace(&store, "ns").await;
        store
            .put_budget_policy("ns", BudgetCadence::Monthly, 10, "UTC", None)
            .await
            .expect("policy");
        assert!(store.delete_namespace("ns").await.expect("delete"));
        let count: i64 = store
            .conn
            .lock()
            .expect("lock")
            .query_row(
                "SELECT count(*) FROM axond_store_budget_cadence WHERE namespace = 'ns'",
                [],
                |row| row.get(0),
            )
            .expect("count");
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn sqlite_first_monthly_admission_racing_delete_leaves_no_rows() {
        let (store, _) = cadence_store();
        let store = Arc::new(store);
        for attempt in 0..5 {
            let ns = format!("monthly_delete_race_{attempt}");
            add_namespace(&store, &ns).await;
            store
                .put_budget_policy(&ns, BudgetCadence::Monthly, 1_000, "UTC", None)
                .await
                .expect("policy");
            {
                let conn = store.conn.lock().expect("lock");
                conn.execute(
                    "DELETE FROM axond_store_budget
                     WHERE namespace = ?1 AND period = ?2",
                    params![ns, "2026-09"],
                )
                .expect("remove current row");
            }
            let admit_store = Arc::clone(&store);
            let delete_store = Arc::clone(&store);
            let admit_ns = ns.clone();
            let delete_ns = ns.clone();
            let (admit, deleted) = tokio::join!(
                tokio::spawn(async move { admit_store.admit_budget(&admit_ns).await }),
                tokio::spawn(async move { delete_store.delete_namespace(&delete_ns).await }),
            );
            let admit = admit.expect("admit task");
            assert!(
                matches!(
                    admit,
                    Ok(BudgetAdmit::Allowed { .. } | BudgetAdmit::Exceeded)
                ),
                "unexpected admit result: {admit:?}"
            );
            assert!(deleted.expect("delete task").expect("delete"));
            let (budget_rows, cadence_rows) = {
                let conn = store.conn.lock().expect("lock");
                let budget_rows: i64 = conn
                    .query_row(
                        "SELECT count(*) FROM axond_store_budget
                         WHERE namespace = ?1 AND period = ?2",
                        params![ns, "2026-09"],
                        |row| row.get(0),
                    )
                    .expect("budget count");
                let cadence_rows: i64 = conn
                    .query_row(
                        "SELECT count(*) FROM axond_store_budget_cadence
                         WHERE namespace = ?1",
                        params![ns],
                        |row| row.get(0),
                    )
                    .expect("cadence count");
                (budget_rows, cadence_rows)
            };
            assert_eq!(budget_rows, 0);
            assert_eq!(cadence_rows, 0);
            assert!(store.get_namespace(&ns).await.expect("namespace").is_none());
        }
    }

    #[tokio::test]
    async fn corrupt_attrs_are_unavailable() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        {
            let conn = store.conn.lock().expect("lock");
            conn.execute(
                "INSERT INTO axond_namespace (id, attrs, blocklist) VALUES ('bad', 'not-json', NULL)",
                [],
            )
            .expect("insert corrupt row");
        }
        let err = store.get_namespace("bad").await.expect_err("unavailable");
        assert!(matches!(err, StoreError::Unavailable(_)), "{err:?}");
    }

    fn namespace(id: &str) -> crate::config::Namespace {
        crate::config::Namespace {
            id: id.to_owned(),
            default: false,
            allow_platform_fallback: false,
            project: None,
            policy: None,
            static_policy: None,
        }
    }

    #[tokio::test]
    async fn seed_namespaces_insert_only_and_ignore_duplicates() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        store
            .seed_namespaces_blocking(&[
                namespace("wsp_x"),
                namespace("wsp_x"),
                namespace("acme/core"),
                namespace(""),
            ])
            .expect("seed");
        let got = store
            .get_namespace("wsp_x")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(got.id, "wsp_x");
        assert_eq!(got.attrs, serde_json::json!({}));
        assert!(
            store
                .get_namespace("acme/core")
                .await
                .expect("slash id skipped")
                .is_none()
        );
        store
            .seed_namespaces_blocking(&[namespace("wsp_x")])
            .expect("duplicate ignored");
        store.seed_namespaces_blocking(&[]).expect("empty seed");
        assert!(store.get_namespace("wsp_x").await.expect("get").is_some());
    }

    #[tokio::test]
    async fn seed_config_namespaces_skips_slash_ids() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        super::super::seed_config_namespaces(
            &store,
            &[namespace("wsp_ok"), namespace("acme/core")],
        )
        .await
        .expect("seed");
        assert!(store.get_namespace("wsp_ok").await.expect("get").is_some());
        assert!(
            store
                .get_namespace("acme/core")
                .await
                .expect("slash")
                .is_none()
        );
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

    #[tokio::test]
    async fn delete_drops_budget_tables_and_keeps_usage_rows() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        store
            .put_namespace(NamespaceRecord {
                id: "wsp_x".into(),
                attrs: serde_json::json!({}),
                blocklist: None,
            })
            .await
            .expect("ns");
        store
            .put_budget("wsp_x", "p", 10_000)
            .await
            .expect("budget");
        store.admit_budget("wsp_x").await.expect("hold");
        store
            .append_usage(UsageAppend {
                request_id: "req_1".into(),
                namespace: "wsp_x".into(),
                period: Some("p".into()),
                model: "openai/gpt-4o".into(),
                status: "ok".into(),
                cost_microdollars: Some(5),
            })
            .await
            .expect("usage");
        assert!(store.delete_namespace("wsp_x").await.expect("delete"));
        let (budget, active, reservations, usage): (i64, i64, i64, i64) = {
            let conn = store.conn.lock().expect("lock");
            let count = |sql: &str| conn.query_row(sql, [], |row| row.get(0)).expect("count");
            (
                count("SELECT count(*) FROM axond_store_budget WHERE namespace = 'wsp_x'"),
                count("SELECT count(*) FROM axond_store_budget_active WHERE namespace = 'wsp_x'"),
                count(
                    "SELECT count(*) FROM axond_store_budget_reservation WHERE namespace = 'wsp_x'",
                ),
                count("SELECT count(*) FROM axond_store_usage WHERE namespace = 'wsp_x'"),
            )
        };
        assert_eq!((budget, active, reservations, usage), (0, 0, 0, 1));
        assert!(store.get_namespace("wsp_x").await.expect("get").is_none());
    }

    #[tokio::test]
    async fn seed_restores_a_toml_listed_id_after_store_delete() {
        let path = std::env::temp_dir().join(format!(
            "axond-seed-{}-{}.sqlite",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let path_str = path.to_str().expect("utf8 path");
        let store = SqliteStore::open(path_str).expect("open");
        store
            .put_namespace(NamespaceRecord {
                id: "wsp_seed".into(),
                attrs: serde_json::json!({}),
                blocklist: None,
            })
            .await
            .expect("put");
        assert!(store.delete_namespace("wsp_seed").await.expect("delete"));
        drop(store);
        let store = SqliteStore::open(path_str).expect("reopen");
        let toml_ns = crate::config::Namespace {
            id: "wsp_seed".into(),
            default: false,
            allow_platform_fallback: false,
            project: None,
            policy: None,
            static_policy: None,
        };
        super::super::seed_config_namespaces(&store, &[toml_ns])
            .await
            .expect("seed");
        assert!(
            store
                .get_namespace("wsp_seed")
                .await
                .expect("get")
                .is_some(),
            "a TOML-listed id is restored on seed; HTTP DELETE of those ids is 409"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn admission_is_read_only_and_leaves_legacy_tombstones_untouched() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        store
            .put_namespace(NamespaceRecord {
                id: "wsp_x".into(),
                attrs: serde_json::json!({}),
                blocklist: None,
            })
            .await
            .expect("ns");
        store
            .put_budget("wsp_x", "p", 10_000)
            .await
            .expect("budget");
        {
            let conn = store.conn.lock().expect("lock");
            conn.execute(
                "INSERT INTO axond_store_budget_reservation_tombstone
                    (id, incarnation, expires_at) VALUES ('stale', 1, 1)",
                [],
            )
            .expect("past tombstone");
            conn.execute_batch("PRAGMA query_only = ON")
                .expect("read only");
        }
        let admit = store.admit_budget("wsp_x").await.expect("read-only admit");
        assert!(matches!(admit, BudgetAdmit::Allowed { .. }));
        let resolved = store
            .resolve_namespace("wsp_x")
            .await
            .expect("read-only resolve")
            .expect("namespace");
        assert_eq!(resolved.admit, admit);
        let leftover: i64 = {
            let conn = store.conn.lock().expect("lock");
            conn.execute_batch("PRAGMA query_only = OFF")
                .expect("writable");
            conn.query_row(
                "SELECT count(*) FROM axond_store_budget_reservation_tombstone
                 WHERE id = 'stale'",
                [],
                |row| row.get(0),
            )
            .expect("count")
        };
        assert_eq!(leftover, 1, "admit does not vacuum tombstones");
        store
            .charge_budget("wsp_x", "p", 1, 99)
            .await
            .expect("charge");
        let got = store
            .get_budget("wsp_x", "p")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(got.spent_microdollars, 99);
        assert_eq!(got.reserved_microdollars, 0);
    }

    #[test]
    fn reopen_sweeps_expired_legacy_holds() {
        let path = std::env::temp_dir().join(format!(
            "axond-store-{}-{}.sqlite",
            std::process::id(),
            now_ms()
        ));
        let path = path.to_str().expect("temp path");
        let store = SqliteStore::open(path).expect("open");
        let live_expires_at = now_ms() + 60_000;
        {
            let conn = store.conn.lock().expect("lock");
            conn.execute(
                "INSERT INTO axond_store_budget_reservation
                    (id, namespace, period, amount_microdollars, expires_at, incarnation)
                 VALUES ('expired', 'ns', 'p', 1, 1, 1)",
                [],
            )
            .expect("expired reservation");
            conn.execute(
                "INSERT INTO axond_store_budget_reservation
                    (id, namespace, period, amount_microdollars, expires_at, incarnation)
                 VALUES ('live', 'ns', 'p', 1, ?1, 1)",
                params![live_expires_at],
            )
            .expect("live reservation");
            conn.execute(
                "INSERT INTO axond_store_budget_reservation_tombstone
                    (id, incarnation, expires_at) VALUES ('expired_tombstone', 1, 1)",
                [],
            )
            .expect("expired tombstone");
            conn.execute(
                "INSERT INTO axond_store_budget_reservation_tombstone
                    (id, incarnation, expires_at)
                 VALUES ('live_tombstone', 1, ?1)",
                params![live_expires_at],
            )
            .expect("live tombstone");
        }
        drop(store);

        let reopened = SqliteStore::open(path).expect("reopen");
        let conn = reopened.conn.lock().expect("lock");
        let reservations: i64 = conn
            .query_row(
                "SELECT count(*) FROM axond_store_budget_reservation",
                [],
                |row| row.get(0),
            )
            .expect("reservation count");
        let tombstones: i64 = conn
            .query_row(
                "SELECT count(*) FROM axond_store_budget_reservation_tombstone",
                [],
                |row| row.get(0),
            )
            .expect("tombstone count");
        assert_eq!(reservations, 1);
        assert_eq!(tombstones, 1);
        drop(conn);
        std::fs::remove_file(path).expect("remove temp database");
    }

    #[test]
    fn budget_tables_use_store_prefix() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        let conn = store.conn.lock().expect("lock");
        let names: Vec<String> = {
            let mut stmt = conn
                .prepare(
                    "SELECT name FROM sqlite_master
                     WHERE type IN ('table', 'index')
                     ORDER BY name",
                )
                .expect("prep");
            stmt.query_map([], |row| row.get(0))
                .expect("query")
                .map(|row| row.expect("name"))
                .collect()
        };
        assert!(names.iter().any(|n| n == "axond_store_budget"));
        assert!(names.iter().any(|n| n == "axond_store_budget_active"));
        assert!(names.iter().any(|n| n == "axond_store_budget_reservation"));
        assert!(
            names
                .iter()
                .any(|n| n == "axond_store_budget_reservation_tombstone")
        );
        assert!(
            names
                .iter()
                .any(|n| n == "axond_store_budget_reservation_scope_idx")
        );
        assert!(names.iter().any(|n| n == "axond_store_provider_models"));
        assert!(!names.iter().any(|n| n == "axond_budget"));
        assert!(!names.iter().any(|n| n == "axond_budget_reservation"));
    }

    #[test]
    fn migrate_reservation_incarnation_adds_column_when_pragma_shows_it_absent() {
        let conn = Connection::open_in_memory().expect("memory");
        conn.execute_batch(
            "CREATE TABLE axond_store_budget_reservation (
                id TEXT PRIMARY KEY NOT NULL,
                namespace TEXT NOT NULL,
                period TEXT NOT NULL,
                amount_microdollars INTEGER NOT NULL,
                expires_at INTEGER NOT NULL
            )",
        )
        .expect("legacy table");
        migrate_reservation_incarnation(&conn).expect("add");
        migrate_reservation_incarnation(&conn).expect("idempotent");
        let names: Vec<String> = {
            let mut stmt = conn
                .prepare("PRAGMA table_info(axond_store_budget_reservation)")
                .expect("pragma");
            stmt.query_map([], |row| row.get::<_, String>(1))
                .expect("query")
                .map(|row| row.expect("name"))
                .collect()
        };
        assert!(names.iter().any(|n| n == "incarnation"));
    }

    #[test]
    fn migrate_tombstone_expires_at_adds_column_when_pragma_shows_it_absent() {
        let conn = Connection::open_in_memory().expect("memory");
        conn.execute_batch(
            "CREATE TABLE axond_store_budget_reservation_tombstone (
                id TEXT PRIMARY KEY NOT NULL,
                incarnation INTEGER NOT NULL
            )",
        )
        .expect("legacy table");
        migrate_tombstone_expires_at(&conn).expect("add");
        migrate_tombstone_expires_at(&conn).expect("idempotent");
        let names: Vec<String> = {
            let mut stmt = conn
                .prepare("PRAGMA table_info(axond_store_budget_reservation_tombstone)")
                .expect("pragma");
            stmt.query_map([], |row| row.get::<_, String>(1))
                .expect("query")
                .map(|row| row.expect("name"))
                .collect()
        };
        assert!(names.iter().any(|n| n == "expires_at"));
    }

    #[tokio::test]
    async fn provider_models_cache_keeps_last_good_when_marked_stale() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        let good = ProviderModels {
            provider: "openai".into(),
            fetched_at: Some("2026-09-02T12:00:00Z".into()),
            stale: false,
            data: vec![serde_json::json!({"id": "gpt-4o", "object": "model"})],
            source: Some("https://api.openai.com/v1".into()),
        };
        store.put_provider_models(good.clone()).await.expect("put");
        let got = store
            .get_provider_models("openai")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(got, good);
        store
            .mark_provider_models_stale_if_source("openai", "https://api.openai.com/v1")
            .await
            .expect("stale");
        let stale = store
            .get_provider_models("openai")
            .await
            .expect("get stale")
            .expect("row");
        assert!(stale.stale);
        assert_eq!(stale.data, good.data);
        assert_eq!(stale.fetched_at, good.fetched_at);
        assert_eq!(stale.source, good.source);
        store
            .mark_provider_models_stale_if_source("missing", "https://api.openai.com/v1")
            .await
            .expect("missing mark");
        assert!(
            store
                .get_provider_models("missing")
                .await
                .expect("missing")
                .is_none()
        );
        let listed = store.list_provider_models().await.expect("list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].provider, "openai");
    }

    #[tokio::test]
    async fn provider_models_source_stale_does_not_replace_matching_source() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        let old = ProviderModels {
            provider: "openai".into(),
            fetched_at: Some("2026-09-02T12:00:00Z".into()),
            stale: false,
            data: vec![serde_json::json!({"id": "old", "object": "model"})],
            source: Some("https://api.openai.com/v1".into()),
        };
        store.put_provider_models(old.clone()).await.expect("put");
        store
            .mark_provider_models_stale_unless_source("openai", "https://example.invalid/v1")
            .await
            .expect("mismatch");
        let stale = store
            .get_provider_models("openai")
            .await
            .expect("get")
            .expect("row");
        assert!(stale.stale);
        assert_eq!(stale.data, old.data);
        assert_eq!(stale.source, old.source);

        let fresh = ProviderModels {
            provider: "openai".into(),
            fetched_at: Some("2026-09-02T12:01:00Z".into()),
            stale: false,
            data: vec![serde_json::json!({"id": "new", "object": "model"})],
            source: Some("https://example.invalid/v1".into()),
        };
        store.put_provider_models(fresh.clone()).await.expect("put");
        store
            .mark_provider_models_stale_unless_source("openai", "https://example.invalid/v1")
            .await
            .expect("match is no-op");
        let got = store
            .get_provider_models("openai")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(got, fresh);
    }

    #[tokio::test]
    async fn provider_models_failed_refresh_does_not_stale_a_newer_source() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        let fresh = ProviderModels {
            provider: "openai".into(),
            fetched_at: Some("2026-09-02T12:01:00Z".into()),
            stale: false,
            data: vec![serde_json::json!({"id": "new", "object": "model"})],
            source: Some("https://example.invalid/v1".into()),
        };
        store.put_provider_models(fresh.clone()).await.expect("put");
        store
            .mark_provider_models_stale_if_source("openai", "https://api.openai.com/v1")
            .await
            .expect("old source");
        let got = store
            .get_provider_models("openai")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(got, fresh);
        store
            .mark_provider_models_stale_if_source("openai", "https://example.invalid/v1")
            .await
            .expect("matching source");
        let stale = store
            .get_provider_models("openai")
            .await
            .expect("get")
            .expect("row");
        assert!(stale.stale);
        assert_eq!(stale.data, fresh.data);
        assert_eq!(stale.source, fresh.source);
    }

    #[tokio::test]
    async fn provider_models_put_does_not_replace_a_newer_source() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        let neu = ProviderModels {
            provider: "openai".into(),
            fetched_at: Some("2026-09-02T12:01:00Z".into()),
            stale: false,
            data: vec![serde_json::json!({"id": "new", "object": "model"})],
            source: Some("https://example.invalid/v1".into()),
        };
        store.put_provider_models(neu.clone()).await.expect("new");
        let old = ProviderModels {
            provider: "openai".into(),
            fetched_at: Some("2026-09-02T12:02:00Z".into()),
            stale: false,
            data: vec![serde_json::json!({"id": "old", "object": "model"})],
            source: Some("https://api.openai.com/v1".into()),
        };
        store.put_provider_models(old).await.expect("old put");
        let got = store
            .get_provider_models("openai")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(got, neu);
    }

    #[tokio::test]
    async fn provider_models_mark_unless_then_put_replaces_fresh_foreign() {
        // Store hole discovery must not hit on later rounds: marking a
        // foreign row stale opens the put CAS for an old URL.
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        let neu = ProviderModels {
            provider: "openai".into(),
            fetched_at: Some("2026-09-02T12:01:00Z".into()),
            stale: false,
            data: vec![serde_json::json!({"id": "new", "object": "model"})],
            source: Some("https://example.invalid/v1".into()),
        };
        store.put_provider_models(neu.clone()).await.expect("new");
        store
            .mark_provider_models_stale_unless_source("openai", "https://api.openai.com/v1")
            .await
            .expect("mark");
        let old = ProviderModels {
            provider: "openai".into(),
            fetched_at: Some("2026-09-02T12:02:00Z".into()),
            stale: false,
            data: vec![serde_json::json!({"id": "old", "object": "model"})],
            source: Some("https://api.openai.com/v1".into()),
        };
        store
            .put_provider_models(old.clone())
            .await
            .expect("old put");
        let got = store
            .get_provider_models("openai")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(got, old);
    }
}
