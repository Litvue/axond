use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::Value;

use super::{
    BudgetRecord, BudgetReserve, NamespaceRecord, ProviderModels, Store, StoreError, UsageAppend,
    UsageSummaryRow, from_sql_amount, sql_amount, sql_amount_saturating,
};

pub struct SqliteStore {
    conn: Arc<Mutex<Connection>>,
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
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
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

fn current_incarnation(conn: &Connection, id: &str) -> Result<i64, StoreError> {
    Ok(conn
        .query_row(
            "SELECT n FROM axond_namespace_incarnation WHERE id = ?1",
            params![id],
            |row| row.get(0),
        )
        .optional()
        .map_err(unavailable)?
        .unwrap_or(1))
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

    async fn namespace_incarnation(&self, id: &str) -> Result<Option<i64>, StoreError> {
        let id = id.to_string();
        self.with_conn(move |conn| {
            conn.query_row(
                "SELECT n FROM axond_namespace_incarnation WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .optional()
            .map_err(unavailable)
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
            let rec = read_budget(&tx, &namespace, &period, now_ms())?;
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
        self.with_conn(move |conn| read_budget(conn, &namespace, &period, now_ms()))
            .await
    }

    async fn reserve_budget(
        &self,
        namespace: &str,
        estimate_microdollars: u64,
        reservation_ttl: Duration,
        reservation_id: &str,
    ) -> Result<BudgetReserve, StoreError> {
        let namespace = namespace.to_string();
        let reservation_id = reservation_id.to_string();
        let estimate = sql_amount(estimate_microdollars)?;
        let ttl_ms = sql_amount(reservation_ttl.as_millis().min(i64::MAX as u128) as u64)?;
        self.with_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(unavailable)?;
            let Some(period) = tx
                .query_row(
                    "SELECT period FROM axond_store_budget_active WHERE namespace = ?1",
                    params![namespace],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(unavailable)?
            else {
                return Ok(BudgetReserve::Exceeded);
            };
            let now = now_ms();
            let incarnation = current_incarnation(&tx, &namespace)?;
            // Vacuum tombstones whose retention has elapsed. Copy newly
            // expired holds with now()+ttl so a late settle after later
            // admissions can still charge this incarnation.
            tx.execute(
                "DELETE FROM axond_store_budget_reservation_tombstone
                 WHERE expires_at < ?1",
                params![now],
            )
            .map_err(unavailable)?;
            let retained_until = now.saturating_add(ttl_ms);
            tx.execute(
                "INSERT OR REPLACE INTO axond_store_budget_reservation_tombstone
                    (id, incarnation, expires_at)
                 SELECT id, incarnation, ?3 FROM axond_store_budget_reservation
                 WHERE namespace = ?1 AND expires_at <= ?2",
                params![namespace, now, retained_until],
            )
            .map_err(unavailable)?;
            tx.execute(
                "DELETE FROM axond_store_budget_reservation
                 WHERE namespace = ?1 AND expires_at <= ?2",
                params![namespace, now],
            )
            .map_err(unavailable)?;
            let (limit, spent) = match tx
                .query_row(
                    "SELECT limit_microdollars, spent_microdollars FROM axond_store_budget
                     WHERE namespace = ?1 AND period = ?2",
                    params![namespace, period],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                )
                .optional()
                .map_err(unavailable)?
            {
                Some(row) => row,
                None => {
                    tx.commit().map_err(unavailable)?;
                    return Ok(BudgetReserve::Exceeded);
                }
            };
            let reserved: i64 = tx
                .query_row(
                    "SELECT COALESCE(SUM(amount_microdollars), 0) FROM axond_store_budget_reservation
                     WHERE namespace = ?1 AND period = ?2 AND expires_at > ?3
                       AND incarnation = ?4",
                    params![namespace, period, now, incarnation],
                    |row| row.get(0),
                )
                .map_err(unavailable)?;
            if spent.saturating_add(reserved).saturating_add(estimate) > limit {
                tx.commit().map_err(unavailable)?;
                return Ok(BudgetReserve::Exceeded);
            }
            let expires_at = now.saturating_add(ttl_ms);
            tx.execute(
                "INSERT INTO axond_store_budget_reservation
                    (id, namespace, period, amount_microdollars, expires_at, incarnation)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    reservation_id,
                    namespace,
                    period,
                    estimate,
                    expires_at,
                    incarnation
                ],
            )
            .map_err(unavailable)?;
            tx.commit().map_err(unavailable)?;
            Ok(BudgetReserve::Allowed { period })
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
        let namespace = namespace.to_string();
        let period = period.to_string();
        let reservation_id = reservation_id.to_string();
        let actual = sql_amount_saturating(actual_microdollars);
        self.with_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(unavailable)?;
            let held_incarnation: Option<i64> = tx
                .query_row(
                    "DELETE FROM axond_store_budget_reservation WHERE id = ?1
                     RETURNING incarnation",
                    params![reservation_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(unavailable)?;
            let held_incarnation = match held_incarnation {
                Some(n) => Some(n),
                None => tx
                    .query_row(
                        "DELETE FROM axond_store_budget_reservation_tombstone WHERE id = ?1
                         RETURNING incarnation",
                        params![reservation_id],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(unavailable)?,
            };
            let ns_exists = namespace_exists(&tx, &namespace)?;
            let current = current_incarnation(&tx, &namespace)?;
            // Unknown reservation id (no row, no tombstone) is a no-op.
            let charge = match held_incarnation {
                Some(incarnation) => ns_exists && incarnation == current,
                None => false,
            };
            if charge {
                tx.execute(
                    "UPDATE axond_store_budget
                     SET spent_microdollars = CASE
                         WHEN spent_microdollars >= 9223372036854775807 - ?1 THEN 9223372036854775807
                         ELSE spent_microdollars + ?1
                     END
                     WHERE namespace = ?2 AND period = ?3",
                    params![actual, namespace, period],
                )
                .map_err(unavailable)?;
            }
            tx.commit().map_err(unavailable)?;
            Ok(())
        })
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
                 WHERE axond_store_provider_models.source IS NULL
                    OR axond_store_provider_models.source = excluded.source",
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

    async fn mark_provider_models_stale(&self, provider: &str) -> Result<(), StoreError> {
        let provider = provider.to_string();
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE axond_store_provider_models SET stale = 1 WHERE provider = ?1",
                params![provider],
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

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn read_budget(
    conn: &Connection,
    namespace: &str,
    period: &str,
    now: i64,
) -> Result<Option<BudgetRecord>, StoreError> {
    conn.query_row(
        "SELECT
             b.limit_microdollars,
             b.spent_microdollars,
             COALESCE((
                 SELECT SUM(r.amount_microdollars)
                 FROM axond_store_budget_reservation r
                 WHERE r.namespace = b.namespace
                   AND r.period = b.period
                   AND r.expires_at > ?3
                   AND r.incarnation = COALESCE((
                       SELECT n FROM axond_namespace_incarnation i WHERE i.id = b.namespace
                   ), 1)
             ), 0),
             EXISTS (
                 SELECT 1 FROM axond_store_budget_active a
                 WHERE a.namespace = b.namespace AND a.period = b.period
             )
         FROM axond_store_budget b
         WHERE b.namespace = ?1 AND b.period = ?2",
        params![namespace, period, now],
        |row| {
            Ok(BudgetRecord::new(
                namespace,
                period,
                from_sql_amount(row.get(0)?),
                from_sql_amount(row.get(1)?),
                from_sql_amount(row.get(2)?),
                row.get(3)?,
            ))
        },
    )
    .optional()
    .map_err(unavailable)
}

#[cfg(test)]
mod tests {
    use super::*;

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
    async fn reserve_expires_old_period_holds_for_the_namespace() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        store
            .put_namespace(NamespaceRecord {
                id: "wsp_x".into(),
                attrs: serde_json::json!({}),
                blocklist: None,
            })
            .await
            .expect("ns");
        store.put_budget("wsp_x", "old", 10_000).await.expect("old");
        store
            .reserve_budget("wsp_x", 10, Duration::from_millis(1), "stale")
            .await
            .expect("stale hold");
        tokio::time::sleep(Duration::from_millis(5)).await;
        store.put_budget("wsp_x", "new", 10_000).await.expect("new");
        store
            .reserve_budget("wsp_x", 1, Duration::from_secs(30), "live")
            .await
            .expect("live");
        let n: i64 = {
            let conn = store.conn.lock().expect("lock");
            conn.query_row(
                "SELECT count(*) FROM axond_store_budget_reservation WHERE namespace = 'wsp_x'",
                [],
                |row| row.get(0),
            )
            .expect("count")
        };
        assert_eq!(n, 1, "expired holds from the old period must be reclaimed");
    }

    #[tokio::test]
    async fn denied_reserve_still_drops_expired_holds() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        store
            .put_namespace(NamespaceRecord {
                id: "wsp_x".into(),
                attrs: serde_json::json!({}),
                blocklist: None,
            })
            .await
            .expect("ns");
        store.put_budget("wsp_x", "p", 100).await.expect("budget");
        store
            .reserve_budget("wsp_x", 10, Duration::from_millis(1), "stale")
            .await
            .expect("stale hold");
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(matches!(
            store
                .reserve_budget("wsp_x", 200, Duration::from_secs(30), "over")
                .await
                .expect("denied"),
            BudgetReserve::Exceeded
        ));
        let n: i64 = {
            let conn = store.conn.lock().expect("lock");
            conn.query_row(
                "SELECT count(*) FROM axond_store_budget_reservation WHERE namespace = 'wsp_x'",
                [],
                |row| row.get(0),
            )
            .expect("count")
        };
        assert_eq!(n, 0, "denied admission must keep the expiry delete");
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
        store
            .reserve_budget("wsp_x", 10, Duration::from_secs(30), "r1")
            .await
            .expect("hold");
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
        assert_eq!((budget, active, reservations, usage), (0, 0, 1, 1));
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
        assert!(
            store
                .namespace_incarnation("wsp_seed")
                .await
                .expect("incarnation")
                .is_some()
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn settle_after_recreate_does_not_charge_new_budget() {
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
        store
            .reserve_budget("wsp_x", 77, Duration::from_secs(30), "r1")
            .await
            .expect("hold");
        assert!(store.delete_namespace("wsp_x").await.expect("delete"));
        store
            .put_namespace(NamespaceRecord {
                id: "wsp_x".into(),
                attrs: serde_json::json!({}),
                blocklist: None,
            })
            .await
            .expect("recreate");
        let rec = store
            .put_budget("wsp_x", "p", 10_000)
            .await
            .expect("new ledger");
        assert_eq!(rec.spent_microdollars, 0);
        assert_eq!(rec.reserved_microdollars, 0);
        store
            .settle_budget("wsp_x", "p", "r1", 77)
            .await
            .expect("late settle");
        let got = store
            .get_budget("wsp_x", "p")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(got.spent_microdollars, 0);
        assert_eq!(got.reserved_microdollars, 0);
        let holds: i64 = {
            let conn = store.conn.lock().expect("lock");
            conn.query_row(
                "SELECT count(*) FROM axond_store_budget_reservation WHERE id = 'r1'",
                [],
                |row| row.get(0),
            )
            .expect("count")
        };
        assert_eq!(holds, 0, "old hold is dropped even when spend is skipped");
    }

    #[tokio::test]
    async fn reserve_reclaims_expired_holds_of_any_incarnation() {
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
        store
            .reserve_budget("wsp_x", 10, Duration::from_millis(1), "old")
            .await
            .expect("hold");
        assert!(store.delete_namespace("wsp_x").await.expect("delete"));
        tokio::time::sleep(Duration::from_millis(5)).await;
        store
            .put_namespace(NamespaceRecord {
                id: "wsp_x".into(),
                attrs: serde_json::json!({}),
                blocklist: None,
            })
            .await
            .expect("recreate");
        store
            .put_budget("wsp_x", "p", 10_000)
            .await
            .expect("new ledger");
        store
            .reserve_budget("wsp_x", 1, Duration::from_secs(30), "new")
            .await
            .expect("expire path");
        let (old, n): (i64, i64) = {
            let conn = store.conn.lock().expect("lock");
            let count = |sql: &str| conn.query_row(sql, [], |row| row.get(0)).expect("count");
            (
                count("SELECT count(*) FROM axond_store_budget_reservation WHERE id = 'old'"),
                count(
                    "SELECT count(*) FROM axond_store_budget_reservation WHERE namespace = 'wsp_x'",
                ),
            )
        };
        assert_eq!(old, 0, "expired prior-incarnation hold must be reclaimed");
        assert_eq!(n, 1);
        store
            .settle_budget("wsp_x", "p", "old", 10)
            .await
            .expect("late settle");
        let got = store
            .get_budget("wsp_x", "p")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(got.spent_microdollars, 0);
        assert_eq!(got.reserved_microdollars, 1);
    }

    #[tokio::test]
    async fn unexpired_prior_incarnation_hold_survives_reserve() {
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
        store
            .reserve_budget("wsp_x", 10, Duration::from_secs(30), "old")
            .await
            .expect("hold");
        assert!(store.delete_namespace("wsp_x").await.expect("delete"));
        store
            .put_namespace(NamespaceRecord {
                id: "wsp_x".into(),
                attrs: serde_json::json!({}),
                blocklist: None,
            })
            .await
            .expect("recreate");
        store
            .put_budget("wsp_x", "p", 10_000)
            .await
            .expect("new ledger");
        store
            .reserve_budget("wsp_x", 1, Duration::from_secs(30), "new")
            .await
            .expect("live hold");
        let n: i64 = {
            let conn = store.conn.lock().expect("lock");
            conn.query_row(
                "SELECT count(*) FROM axond_store_budget_reservation WHERE namespace = 'wsp_x'",
                [],
                |row| row.get(0),
            )
            .expect("count")
        };
        assert_eq!(
            n, 2,
            "unexpired prior-incarnation hold stays until settle or TTL"
        );
        store
            .settle_budget("wsp_x", "p", "old", 10)
            .await
            .expect("late settle");
        let got = store
            .get_budget("wsp_x", "p")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(got.spent_microdollars, 0);
        assert_eq!(got.reserved_microdollars, 1);
    }

    #[tokio::test]
    async fn unknown_reservation_settle_is_a_noop() {
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
        store
            .reserve_budget("wsp_x", 80, Duration::from_secs(30), "r1")
            .await
            .expect("hold");
        {
            let conn = store.conn.lock().expect("lock");
            conn.execute(
                "DELETE FROM axond_store_budget_reservation WHERE id = 'r1'",
                [],
            )
            .expect("drop hold");
        }
        store
            .settle_budget("wsp_x", "p", "r1", 11)
            .await
            .expect("unknown id");
        let got = store
            .get_budget("wsp_x", "p")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(got.spent_microdollars, 0);
        assert_eq!(got.reserved_microdollars, 0);
    }

    #[tokio::test]
    async fn expired_tombstone_is_vacuumed_on_reserve_and_late_settle_is_noop() {
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
        }
        store
            .reserve_budget("wsp_x", 1, Duration::from_secs(30), "live")
            .await
            .expect("reserve vacuums");
        let leftover: i64 = {
            let conn = store.conn.lock().expect("lock");
            conn.query_row(
                "SELECT count(*) FROM axond_store_budget_reservation_tombstone
                 WHERE id = 'stale'",
                [],
                |row| row.get(0),
            )
            .expect("count")
        };
        assert_eq!(leftover, 0, "past-expiry tombstone must be vacuumed");
        store
            .settle_budget("wsp_x", "p", "stale", 99)
            .await
            .expect("late settle");
        let got = store
            .get_budget("wsp_x", "p")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(got.spent_microdollars, 0);
        assert_eq!(got.reserved_microdollars, 1);
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
            .mark_provider_models_stale("openai")
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
            .mark_provider_models_stale("missing")
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
    async fn provider_models_put_does_not_replace_a_different_source() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        let newer = ProviderModels {
            provider: "openai".into(),
            fetched_at: Some("2026-09-02T12:01:00Z".into()),
            stale: false,
            data: vec![serde_json::json!({"id": "new", "object": "model"})],
            source: Some("https://example.invalid/v1".into()),
        };
        store.put_provider_models(newer.clone()).await.expect("new");
        let older = ProviderModels {
            provider: "openai".into(),
            fetched_at: Some("2026-09-02T12:00:00Z".into()),
            stale: false,
            data: vec![serde_json::json!({"id": "old", "object": "model"})],
            source: Some("https://api.openai.com/v1".into()),
        };
        store.put_provider_models(older).await.expect("old");
        let got = store
            .get_provider_models("openai")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(got, newer);
    }
}
