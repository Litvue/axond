//! The one durable store (ADR 0063).
//!
//! SQLite WAL is the single-replica implementation; Postgres is HA. Boot
//! requires a reachable backend. Namespace rows are loaded on demand — never
//! preloaded at process start. The budget ledger (`spent + reserved` per
//! `(namespace, period)`) lives here, not in Redis. Postgres tables are
//! `axond_store_budget*` so they do not collide with leftover `axond_budget`
//! from the withdrawn `[budget]` backend.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

use crate::backends::health::BackendHealth;
use crate::config::{StorageBackend, StorageConfig};
use crate::namespace::NamespaceId;

mod postgres;
mod sqlite;

pub use postgres::PostgresStore;
pub use sqlite::SqliteStore;

/// ADR 0063: opaque namespace `attrs` are capped at 4 KiB (serialized JSON).
pub const MAX_ATTRS_BYTES: usize = 4 * 1024;

/// Opaque billing-period keys share the namespace id charset and bound.
pub const MAX_PERIOD_LEN: usize = 128;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct NamespaceRecord {
    pub id: String,
    #[serde(default)]
    pub attrs: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocklist: Option<Vec<String>>,
}

/// One `(namespace, period)` ledger, as GET returns it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct BudgetRecord {
    pub namespace: String,
    pub period: String,
    pub limit_microdollars: u64,
    pub spent_microdollars: u64,
    pub reserved_microdollars: u64,
    pub remaining_microdollars: u64,
    pub active: bool,
}

impl BudgetRecord {
    fn new(
        namespace: impl Into<String>,
        period: impl Into<String>,
        limit_microdollars: u64,
        spent_microdollars: u64,
        reserved_microdollars: u64,
        active: bool,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            period: period.into(),
            limit_microdollars,
            spent_microdollars,
            reserved_microdollars,
            remaining_microdollars: remaining(
                limit_microdollars,
                spent_microdollars,
                reserved_microdollars,
            ),
            active,
        }
    }
}

pub fn remaining(limit: u64, spent: u64, reserved: u64) -> u64 {
    limit.saturating_sub(spent).saturating_sub(reserved)
}

/// One usage event as the Store indexes it for `GET .../usage`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageAppend {
    pub request_id: String,
    pub namespace: String,
    pub period: Option<String>,
    pub model: String,
    pub status: String,
    pub cost_microdollars: Option<u64>,
}

/// Per-model per-status totals for one namespace and period.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UsageSummaryRow {
    pub model: String,
    pub status: String,
    pub count: u64,
    pub cost_microdollars: u64,
}

/// `GET /api/v1/namespaces/{ns}/usage?period=` body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UsageSummary {
    pub namespace: String,
    pub period: String,
    pub data: Vec<UsageSummaryRow>,
}

/// Cached upstream `GET /models` listing for one configured provider.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ProviderModels {
    pub provider: String,
    /// RFC3339 of the last successful fetch. Absent if none has succeeded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fetched_at: Option<String>,
    /// True when the last upstream fetch failed. Last-good `data` is still
    /// returned; empty + stale if never fetched.
    pub stale: bool,
    pub data: Vec<Value>,
}

impl ProviderModels {
    pub fn empty_stale(provider: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            fetched_at: None,
            stale: true,
            data: Vec::new(),
        }
    }
}

/// Outcome of a pre-dispatch hold against the namespace's active period.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BudgetReserve {
    Allowed { period: String },
    Exceeded,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("namespace `{0}` already exists")]
    Duplicate(String),
    #[error("namespace `{0}` not found")]
    NotFound(String),
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
    /// Remove the namespace and its live budget ledger in one transaction.
    /// Usage rows are retained. Reservation rows are kept so an in-flight
    /// settle can see the incarnation it reserved under. Missing id is
    /// `Ok(false)`.
    async fn delete_namespace(&self, id: &str) -> Result<bool, StoreError>;
    /// Seed addressable namespace ids without `spawn_blocking`.
    fn seed_namespaces_blocking(
        &self,
        namespaces: &[crate::config::Namespace],
    ) -> Result<(), StoreError>;

    /// Set the period's limit and mark it as the namespace's active period.
    /// Never resets spend. A missing namespace is [`StoreError::NotFound`].
    async fn put_budget(
        &self,
        namespace: &str,
        period: &str,
        limit_microdollars: u64,
    ) -> Result<BudgetRecord, StoreError>;

    async fn get_budget(
        &self,
        namespace: &str,
        period: &str,
    ) -> Result<Option<BudgetRecord>, StoreError>;

    /// Hold `estimate` against the namespace's active period. No budget row
    /// is [`BudgetReserve::Exceeded`] (fail closed).
    async fn reserve_budget(
        &self,
        namespace: &str,
        estimate_microdollars: u64,
        reservation_ttl: Duration,
        reservation_id: &str,
    ) -> Result<BudgetReserve, StoreError>;

    /// Charge `actual` and release the hold in one operation. Charge only when
    /// a reservation or expire-tombstone records an incarnation that matches
    /// the live namespace. A prior-incarnation hold (row or tombstone) is
    /// dropped without charging. An unknown reservation id is a no-op.
    async fn settle_budget(
        &self,
        namespace: &str,
        period: &str,
        reservation_id: &str,
        actual_microdollars: u64,
    ) -> Result<(), StoreError>;

    /// Reachability for the status refresher. SQLite has none; Postgres does.
    fn health(&self) -> Option<Arc<dyn BackendHealth>> {
        None
    }

    /// Index one usage event for the management summary. Duplicate
    /// `request_id` is ignored (at-least-once).
    async fn append_usage(&self, event: UsageAppend) -> Result<(), StoreError>;

    /// When true, [`Self::append_usage`] runs blocking I/O inside `spawn_blocking`
    /// and dropping its future cannot cancel the work. The usage-index worker
    /// then calls [`Self::append_usage_sync`] on one OS thread instead.
    fn blocking_usage_index(&self) -> bool {
        false
    }

    /// Insert one usage-index row on the caller's thread. SQLite only; must
    /// not schedule `spawn_blocking`.
    fn append_usage_sync(&self, event: UsageAppend) -> Result<(), StoreError> {
        let _ = event;
        Err(StoreError::Unavailable(
            "this store has no synchronous usage-index path".into(),
        ))
    }

    /// Per-model per-status counts and cost totals for `namespace`+`period`.
    async fn summarize_usage(
        &self,
        namespace: &str,
        period: &str,
    ) -> Result<Vec<UsageSummaryRow>, StoreError>;

    /// Cached upstream listing for one provider, or `None` if never written.
    async fn get_provider_models(
        &self,
        provider: &str,
    ) -> Result<Option<ProviderModels>, StoreError> {
        let _ = provider;
        Ok(None)
    }

    /// Every cached provider listing. Missing configured providers are absent.
    async fn list_provider_models(&self) -> Result<Vec<ProviderModels>, StoreError> {
        Ok(Vec::new())
    }

    /// Insert or replace one provider's cached listing.
    async fn put_provider_models(&self, row: ProviderModels) -> Result<(), StoreError> {
        let _ = row;
        Ok(())
    }
}

#[cfg(test)]
pub struct UnavailableStore;

#[cfg(test)]
#[async_trait]
impl Store for UnavailableStore {
    async fn put_namespace(&self, _: NamespaceRecord) -> Result<(), StoreError> {
        Err(StoreError::Unavailable("down".into()))
    }
    async fn get_namespace(&self, _: &str) -> Result<Option<NamespaceRecord>, StoreError> {
        Err(StoreError::Unavailable("down".into()))
    }
    async fn list_namespaces(
        &self,
        _: Option<String>,
        _: u32,
    ) -> Result<(Vec<NamespaceRecord>, Option<String>), StoreError> {
        Err(StoreError::Unavailable("down".into()))
    }
    async fn update_namespace(
        &self,
        _: &str,
        _: Value,
        _: Option<Vec<String>>,
    ) -> Result<Option<NamespaceRecord>, StoreError> {
        Err(StoreError::Unavailable("down".into()))
    }
    async fn delete_namespace(&self, _: &str) -> Result<bool, StoreError> {
        Err(StoreError::Unavailable("down".into()))
    }
    fn seed_namespaces_blocking(&self, _: &[crate::config::Namespace]) -> Result<(), StoreError> {
        Err(StoreError::Unavailable("down".into()))
    }
    async fn put_budget(&self, _: &str, _: &str, _: u64) -> Result<BudgetRecord, StoreError> {
        Err(StoreError::Unavailable("down".into()))
    }
    async fn get_budget(&self, _: &str, _: &str) -> Result<Option<BudgetRecord>, StoreError> {
        Err(StoreError::Unavailable("down".into()))
    }
    async fn reserve_budget(
        &self,
        _: &str,
        _: u64,
        _: Duration,
        _: &str,
    ) -> Result<BudgetReserve, StoreError> {
        Err(StoreError::Unavailable("down".into()))
    }
    async fn settle_budget(&self, _: &str, _: &str, _: &str, _: u64) -> Result<(), StoreError> {
        Err(StoreError::Unavailable("down".into()))
    }
    async fn append_usage(&self, _: UsageAppend) -> Result<(), StoreError> {
        Err(StoreError::Unavailable("down".into()))
    }
    async fn summarize_usage(&self, _: &str, _: &str) -> Result<Vec<UsageSummaryRow>, StoreError> {
        Err(StoreError::Unavailable("down".into()))
    }
    async fn get_provider_models(&self, _: &str) -> Result<Option<ProviderModels>, StoreError> {
        Err(StoreError::Unavailable("down".into()))
    }
    async fn list_provider_models(&self) -> Result<Vec<ProviderModels>, StoreError> {
        Err(StoreError::Unavailable("down".into()))
    }
    async fn put_provider_models(&self, _: ProviderModels) -> Result<(), StoreError> {
        Err(StoreError::Unavailable("down".into()))
    }
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
        if validate_namespace_id(&namespace.id).is_err() {
            continue;
        }
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

/// Opaque period: 1–128 characters of `[A-Za-z0-9._-]+`.
pub fn validate_period(period: &str) -> Result<(), StoreError> {
    if period.is_empty() || period.len() > MAX_PERIOD_LEN {
        return Err(StoreError::Invalid(format!(
            "period must be 1–{MAX_PERIOD_LEN} characters of [A-Za-z0-9._-]"
        )));
    }
    if !period
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(StoreError::Invalid(
            "period must be 1–128 characters of [A-Za-z0-9._-]".into(),
        ));
    }
    Ok(())
}

/// Fail-closed conversion for reserve (and other pre-dispatch amounts).
/// An estimate above `i64::MAX` is rejected before the provider runs.
pub(crate) fn sql_amount(value: u64) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| {
        StoreError::Invalid("microdollar amount exceeds the store integer range".into())
    })
}

/// Settlement actuals that exceed `i64::MAX` charge the representable cap
/// rather than dropping the write. PUT limits use [`sql_amount`] and reject.
pub(crate) fn sql_amount_saturating(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

pub(crate) fn from_sql_amount(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

/// Fail-closed admit: overflow of `spent + reserved + estimate` is exceeded,
/// matching GET remaining at `i64::MAX`.
pub(crate) fn budget_would_exceed(spent: i64, reserved: i64, estimate: i64, limit: i64) -> bool {
    spent
        .checked_add(reserved)
        .and_then(|total| total.checked_add(estimate))
        .map(|total| total > limit)
        .unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_amount_rejects_unrepresentable_reserve() {
        assert!(sql_amount(u64::MAX).is_err());
        assert!(sql_amount(i64::MAX as u64 + 1).is_err());
        assert_eq!(sql_amount(i64::MAX as u64).expect("max"), i64::MAX);
        assert_eq!(sql_amount(640).expect("in range"), 640);
    }

    #[test]
    fn sql_amount_saturating_charges_the_representable_cap() {
        assert_eq!(sql_amount_saturating(u64::MAX), i64::MAX);
        assert_eq!(sql_amount_saturating(i64::MAX as u64 + 1), i64::MAX);
        assert_eq!(sql_amount_saturating(i64::MAX as u64), i64::MAX);
        assert_eq!(sql_amount_saturating(640), 640);
    }

    #[test]
    fn budget_would_exceed_fail_closes_on_i64_max_limit() {
        assert!(budget_would_exceed(i64::MAX, 0, 1, i64::MAX));
        assert!(budget_would_exceed(i64::MAX - 1, 1, 1, i64::MAX));
        assert!(!budget_would_exceed(i64::MAX - 1, 0, 1, i64::MAX));
        assert!(!budget_would_exceed(0, 0, 1, 10));
        assert!(budget_would_exceed(5, 5, 1, 10));
    }

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

    async fn seeded(store: &dyn Store) {
        store
            .put_namespace(NamespaceRecord {
                id: "wsp_x".into(),
                attrs: serde_json::json!({}),
                blocklist: None,
            })
            .await
            .expect("namespace");
    }

    #[tokio::test]
    async fn sqlite_put_get_budget_and_active_period() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        seeded(&store).await;
        let rec = store
            .put_budget("wsp_x", "2026-09", 1_000)
            .await
            .expect("put");
        assert_eq!(rec.limit_microdollars, 1_000);
        assert_eq!(rec.spent_microdollars, 0);
        assert_eq!(rec.reserved_microdollars, 0);
        assert_eq!(rec.remaining_microdollars, 1_000);
        assert!(rec.active);
        let got = store
            .get_budget("wsp_x", "2026-09")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(got, rec);
        let missing_ns = store
            .put_budget("ghost", "2026-09", 1)
            .await
            .expect_err("unknown ns");
        assert!(matches!(missing_ns, StoreError::NotFound(_)));
    }

    #[tokio::test]
    async fn sqlite_put_does_not_reset_spend_and_accepts_a_lower_limit() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        seeded(&store).await;
        store.put_budget("wsp_x", "p1", 10_000).await.expect("put");
        match store
            .reserve_budget("wsp_x", 100, Duration::from_secs(30), "r1")
            .await
            .expect("reserve")
        {
            BudgetReserve::Allowed { period } => assert_eq!(period, "p1"),
            other => panic!("expected hold, got {other:?}"),
        }
        store
            .settle_budget("wsp_x", "p1", "r1", 40)
            .await
            .expect("settle");
        let lowered = store.put_budget("wsp_x", "p1", 10).await.expect("lower");
        assert_eq!(lowered.spent_microdollars, 40);
        assert_eq!(lowered.limit_microdollars, 10);
        assert_eq!(lowered.remaining_microdollars, 0);
        assert!(matches!(
            store
                .reserve_budget("wsp_x", 1, Duration::from_secs(30), "r2")
                .await
                .expect("over"),
            BudgetReserve::Exceeded
        ));
    }

    #[tokio::test]
    async fn sqlite_new_period_switches_admission_and_keeps_old_spend() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        seeded(&store).await;
        store.put_budget("wsp_x", "old", 10_000).await.expect("old");
        store
            .reserve_budget("wsp_x", 50, Duration::from_secs(30), "r1")
            .await
            .expect("hold");
        store
            .settle_budget("wsp_x", "old", "r1", 50)
            .await
            .expect("settle");
        let neu = store.put_budget("wsp_x", "new", 10_000).await.expect("new");
        assert!(neu.active);
        assert_eq!(neu.spent_microdollars, 0);
        let old = store
            .get_budget("wsp_x", "old")
            .await
            .expect("get old")
            .expect("row");
        assert!(!old.active);
        assert_eq!(old.spent_microdollars, 50);
        match store
            .reserve_budget("wsp_x", 1, Duration::from_secs(30), "r2")
            .await
            .expect("new period")
        {
            BudgetReserve::Allowed { period } => assert_eq!(period, "new"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn sqlite_no_budget_row_is_exceeded() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        seeded(&store).await;
        assert!(matches!(
            store
                .reserve_budget("wsp_x", 1, Duration::from_secs(30), "r")
                .await
                .expect("closed"),
            BudgetReserve::Exceeded
        ));
    }

    #[tokio::test]
    async fn sqlite_delete_namespace_drops_budget_and_keeps_usage() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        seeded(&store).await;
        store.put_budget("wsp_x", "p", 10_000).await.expect("put");
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
        assert!(!store.delete_namespace("wsp_x").await.expect("repeat"));
        assert!(store.get_namespace("wsp_x").await.expect("get").is_none());
        assert!(
            store
                .get_budget("wsp_x", "p")
                .await
                .expect("budget")
                .is_none()
        );
        let usage = store
            .summarize_usage("wsp_x", "p")
            .await
            .expect("summarize");
        assert_eq!(
            usage,
            vec![UsageSummaryRow {
                model: "openai/gpt-4o".into(),
                status: "ok".into(),
                count: 1,
                cost_microdollars: 5,
            }]
        );
        seeded(&store).await;
        assert!(
            store
                .get_budget("wsp_x", "p")
                .await
                .expect("recreate budget")
                .is_none()
        );
        assert!(matches!(
            store
                .reserve_budget("wsp_x", 1, Duration::from_secs(30), "r2")
                .await
                .expect("closed"),
            BudgetReserve::Exceeded
        ));
    }

    #[tokio::test]
    async fn sqlite_settle_after_recreate_does_not_charge_new_budget() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        seeded(&store).await;
        store.put_budget("wsp_x", "p", 10_000).await.expect("put");
        store
            .reserve_budget("wsp_x", 77, Duration::from_secs(30), "r1")
            .await
            .expect("hold");
        assert!(store.delete_namespace("wsp_x").await.expect("delete"));
        seeded(&store).await;
        let rec = store
            .put_budget("wsp_x", "p", 10_000)
            .await
            .expect("recreate");
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
    }

    #[tokio::test]
    async fn sqlite_delete_recreate_expires_any_incarnation_hold() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        seeded(&store).await;
        store.put_budget("wsp_x", "p", 10_000).await.expect("put");
        store
            .reserve_budget("wsp_x", 10, Duration::from_millis(1), "old")
            .await
            .expect("hold");
        assert!(store.delete_namespace("wsp_x").await.expect("delete"));
        tokio::time::sleep(Duration::from_millis(5)).await;
        seeded(&store).await;
        store
            .put_budget("wsp_x", "p", 10_000)
            .await
            .expect("recreate");
        match store
            .reserve_budget("wsp_x", 1, Duration::from_secs(30), "new")
            .await
            .expect("expire path")
        {
            BudgetReserve::Allowed { period } => assert_eq!(period, "p"),
            other => panic!("{other:?}"),
        }
        store
            .settle_budget("wsp_x", "p", "old", 10)
            .await
            .expect("late settle after TTL");
        let got = store
            .get_budget("wsp_x", "p")
            .await
            .expect("get")
            .expect("row");
        // Expire-delete wrote a tombstone for incarnation 1; current is 2.
        assert_eq!(got.spent_microdollars, 0);
        assert_eq!(got.reserved_microdollars, 1);
    }

    #[tokio::test]
    async fn sqlite_late_settle_after_two_reserves_still_charges_this_incarnation() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        seeded(&store).await;
        store.put_budget("wsp_x", "p", 10_000).await.expect("put");
        store
            .reserve_budget("wsp_x", 40, Duration::from_millis(1), "r1")
            .await
            .expect("hold");
        tokio::time::sleep(Duration::from_millis(5)).await;
        match store
            .reserve_budget("wsp_x", 1, Duration::from_secs(30), "r2")
            .await
            .expect("second")
        {
            BudgetReserve::Allowed { period } => assert_eq!(period, "p"),
            other => panic!("{other:?}"),
        }
        match store
            .reserve_budget("wsp_x", 1, Duration::from_secs(30), "r3")
            .await
            .expect("third")
        {
            BudgetReserve::Allowed { period } => assert_eq!(period, "p"),
            other => panic!("{other:?}"),
        }
        store
            .settle_budget("wsp_x", "p", "r1", 40)
            .await
            .expect("late settle");
        let got = store
            .get_budget("wsp_x", "p")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(got.spent_microdollars, 40);
        assert_eq!(got.reserved_microdollars, 2);
    }

    #[tokio::test]
    async fn sqlite_in_flight_hold_counts_and_settle_is_one_operation() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        seeded(&store).await;
        store.put_budget("wsp_x", "p", 100).await.expect("put");
        store
            .reserve_budget("wsp_x", 60, Duration::from_secs(30), "r1")
            .await
            .expect("first");
        let held = store
            .get_budget("wsp_x", "p")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(held.reserved_microdollars, 60);
        assert_eq!(held.remaining_microdollars, 40);
        assert!(matches!(
            store
                .reserve_budget("wsp_x", 50, Duration::from_secs(30), "r2")
                .await
                .expect("second"),
            BudgetReserve::Exceeded
        ));
        store
            .settle_budget("wsp_x", "p", "r1", 25)
            .await
            .expect("settle");
        let after = store
            .get_budget("wsp_x", "p")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(after.spent_microdollars, 25);
        assert_eq!(after.reserved_microdollars, 0);
        assert_eq!(after.remaining_microdollars, 75);
    }

    #[tokio::test]
    async fn sqlite_put_budget_rejects_unrepresentable_limit() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        seeded(&store).await;
        let err = store
            .put_budget("wsp_x", "p", i64::MAX as u64 + 1)
            .await
            .expect_err("limit must match the response");
        assert!(matches!(err, StoreError::Invalid(_)), "{err:?}");
        assert!(store.get_budget("wsp_x", "p").await.expect("get").is_none());
    }

    #[tokio::test]
    async fn sqlite_settle_saturates_oversized_actual_and_releases_the_hold() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        seeded(&store).await;
        store.put_budget("wsp_x", "p", 10_000).await.expect("put");
        match store
            .reserve_budget("wsp_x", 1, Duration::from_secs(30), "r0")
            .await
            .expect("prior")
        {
            BudgetReserve::Allowed { period } => assert_eq!(period, "p"),
            other => panic!("{other:?}"),
        }
        store
            .settle_budget("wsp_x", "p", "r0", 40)
            .await
            .expect("prior spend");
        match store
            .reserve_budget("wsp_x", 1, Duration::from_secs(30), "r1")
            .await
            .expect("reserve")
        {
            BudgetReserve::Allowed { period } => assert_eq!(period, "p"),
            other => panic!("{other:?}"),
        }
        store
            .settle_budget("wsp_x", "p", "r1", i64::MAX as u64 + 1)
            .await
            .expect("settle");
        let got = store
            .get_budget("wsp_x", "p")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(got.spent_microdollars, i64::MAX as u64);
        assert_eq!(got.reserved_microdollars, 0);
    }

    #[tokio::test]
    async fn sqlite_reserve_rejects_unrepresentable_estimate() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        seeded(&store).await;
        store.put_budget("wsp_x", "p", 10_000).await.expect("put");
        let err = store
            .reserve_budget("wsp_x", i64::MAX as u64 + 1, Duration::from_secs(30), "r")
            .await
            .expect_err("fail closed");
        assert!(matches!(err, StoreError::Invalid(_)), "{err:?}");
    }

    #[tokio::test]
    async fn sqlite_expired_hold_does_not_block_and_later_settle_still_charges() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        seeded(&store).await;
        store.put_budget("wsp_x", "p", 100).await.expect("put");
        store
            .reserve_budget("wsp_x", 80, Duration::from_millis(1), "r1")
            .await
            .expect("hold");
        tokio::time::sleep(Duration::from_millis(5)).await;
        match store
            .reserve_budget("wsp_x", 80, Duration::from_secs(30), "r2")
            .await
            .expect("after expiry")
        {
            BudgetReserve::Allowed { period } => assert_eq!(period, "p"),
            other => panic!("{other:?}"),
        }
        store
            .settle_budget("wsp_x", "p", "r1", 11)
            .await
            .expect("late settle");
        let got = store
            .get_budget("wsp_x", "p")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(got.spent_microdollars, 11);
        assert_eq!(got.reserved_microdollars, 80);
    }

    #[tokio::test]
    async fn postgres_two_connections_cannot_both_reserve_the_last_dollar() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let ns = format!(
            "wsp_race_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        );
        let a = PostgresStore::connect(&dsn, true).await.expect("connect a");
        let b = PostgresStore::connect(&dsn, true).await.expect("connect b");
        a.put_namespace(NamespaceRecord {
            id: ns.clone(),
            attrs: serde_json::json!({}),
            blocklist: None,
        })
        .await
        .expect("ns");
        a.put_budget(&ns, "p", 1).await.expect("budget");
        let first = tokio::spawn({
            let ns = ns.clone();
            async move { a.reserve_budget(&ns, 1, Duration::from_secs(30), "a").await }
        });
        let second = tokio::spawn({
            let ns = ns.clone();
            async move { b.reserve_budget(&ns, 1, Duration::from_secs(30), "b").await }
        });
        let results = [first.await.expect("join a"), second.await.expect("join b")];
        let allowed = results
            .iter()
            .filter(|r| matches!(r, Ok(BudgetReserve::Allowed { .. })))
            .count();
        let exceeded = results
            .iter()
            .filter(|r| matches!(r, Ok(BudgetReserve::Exceeded)))
            .count();
        assert_eq!(
            (allowed, exceeded),
            (1, 1),
            "exactly one replica may take the last dollar: {results:?}"
        );
    }

    fn unique_ns(prefix: &str) -> String {
        format!(
            "{prefix}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        )
    }

    async fn postgres_seeded(dsn: &str) -> (PostgresStore, String) {
        let store = PostgresStore::connect(dsn, true).await.expect("connect");
        let ns = unique_ns("wsp");
        store
            .put_namespace(NamespaceRecord {
                id: ns.clone(),
                attrs: serde_json::json!({}),
                blocklist: None,
            })
            .await
            .expect("ns");
        (store, ns)
    }

    #[tokio::test]
    async fn postgres_delete_namespace_drops_budget_and_keeps_usage() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let (store, ns) = postgres_seeded(&dsn).await;
        store.put_budget(&ns, "p", 10_000).await.expect("put");
        store
            .reserve_budget(&ns, 10, Duration::from_secs(30), "r1")
            .await
            .expect("hold");
        store
            .append_usage(UsageAppend {
                request_id: format!("req_{ns}"),
                namespace: ns.clone(),
                period: Some("p".into()),
                model: "openai/gpt-4o".into(),
                status: "ok".into(),
                cost_microdollars: Some(5),
            })
            .await
            .expect("usage");
        assert!(store.delete_namespace(&ns).await.expect("delete"));
        assert!(!store.delete_namespace(&ns).await.expect("repeat"));
        assert!(store.get_namespace(&ns).await.expect("get").is_none());
        assert!(store.get_budget(&ns, "p").await.expect("budget").is_none());
        assert_eq!(store.reservation_count(&ns).await.expect("holds"), 1);
        let usage = store.summarize_usage(&ns, "p").await.expect("summarize");
        assert_eq!(
            usage,
            vec![UsageSummaryRow {
                model: "openai/gpt-4o".into(),
                status: "ok".into(),
                count: 1,
                cost_microdollars: 5,
            }]
        );
        store
            .put_namespace(NamespaceRecord {
                id: ns.clone(),
                attrs: serde_json::json!({}),
                blocklist: None,
            })
            .await
            .expect("recreate");
        assert!(
            store
                .get_budget(&ns, "p")
                .await
                .expect("recreate budget")
                .is_none()
        );
        assert!(matches!(
            store
                .reserve_budget(&ns, 1, Duration::from_secs(30), "r2")
                .await
                .expect("closed"),
            BudgetReserve::Exceeded
        ));
    }

    #[tokio::test]
    async fn postgres_settle_after_recreate_does_not_charge_new_budget() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let (store, ns) = postgres_seeded(&dsn).await;
        store.put_budget(&ns, "p", 10_000).await.expect("put");
        store
            .reserve_budget(&ns, 77, Duration::from_secs(30), "r1")
            .await
            .expect("hold");
        assert!(store.delete_namespace(&ns).await.expect("delete"));
        store
            .put_namespace(NamespaceRecord {
                id: ns.clone(),
                attrs: serde_json::json!({}),
                blocklist: None,
            })
            .await
            .expect("recreate");
        let rec = store
            .put_budget(&ns, "p", 10_000)
            .await
            .expect("new ledger");
        assert_eq!(rec.spent_microdollars, 0);
        assert_eq!(rec.reserved_microdollars, 0);
        store
            .settle_budget(&ns, "p", "r1", 77)
            .await
            .expect("late settle");
        let got = store.get_budget(&ns, "p").await.expect("get").expect("row");
        assert_eq!(got.spent_microdollars, 0);
        assert_eq!(got.reserved_microdollars, 0);
        assert_eq!(store.reservation_count(&ns).await.expect("holds"), 0);
    }

    #[tokio::test]
    async fn postgres_put_namespace_inserts_incarnation_one() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let (scoped, setup) = postgres_isolated(&dsn).await;
        let store = PostgresStore::connect(&scoped, true)
            .await
            .expect("connect");
        let ns = unique_ns("wsp_inc");
        store
            .put_namespace(NamespaceRecord {
                id: ns.clone(),
                attrs: serde_json::json!({}),
                blocklist: None,
            })
            .await
            .expect("put");
        let n: i64 = setup
            .query_one(
                "SELECT n FROM axond_namespace_incarnation WHERE id = $1",
                &[&ns],
            )
            .await
            .expect("row")
            .get(0);
        assert_eq!(n, 1);
    }

    #[tokio::test]
    async fn postgres_delete_missing_incarnation_row_isolates_late_settle() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let (scoped, setup) = postgres_isolated(&dsn).await;
        let store = PostgresStore::connect(&scoped, true)
            .await
            .expect("connect");
        let ns = unique_ns("wsp_legacy_inc");
        store
            .put_namespace(NamespaceRecord {
                id: ns.clone(),
                attrs: serde_json::json!({}),
                blocklist: None,
            })
            .await
            .expect("put");
        store.put_budget(&ns, "p", 10_000).await.expect("budget");
        setup
            .execute(
                "DELETE FROM axond_namespace_incarnation WHERE id = $1",
                &[&ns],
            )
            .await
            .expect("drop companion");
        store
            .reserve_budget(&ns, 77, Duration::from_secs(30), "r1")
            .await
            .expect("hold");
        assert!(store.delete_namespace(&ns).await.expect("delete"));
        let n: i64 = setup
            .query_one(
                "SELECT n FROM axond_namespace_incarnation WHERE id = $1",
                &[&ns],
            )
            .await
            .expect("bumped")
            .get(0);
        assert_eq!(
            n, 2,
            "delete must create n=2 when the companion was missing"
        );
        store
            .put_namespace(NamespaceRecord {
                id: ns.clone(),
                attrs: serde_json::json!({}),
                blocklist: None,
            })
            .await
            .expect("recreate");
        let n: i64 = setup
            .query_one(
                "SELECT n FROM axond_namespace_incarnation WHERE id = $1",
                &[&ns],
            )
            .await
            .expect("kept")
            .get(0);
        assert_eq!(n, 2, "recreate must not reset incarnation");
        let rec = store
            .put_budget(&ns, "p", 10_000)
            .await
            .expect("new ledger");
        assert_eq!(rec.spent_microdollars, 0);
        store
            .settle_budget(&ns, "p", "r1", 77)
            .await
            .expect("late settle");
        let got = store.get_budget(&ns, "p").await.expect("get").expect("row");
        assert_eq!(got.spent_microdollars, 0);
        assert_eq!(got.reserved_microdollars, 0);
    }

    #[tokio::test]
    async fn postgres_delete_recreate_expires_any_incarnation_hold() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let (store, ns) = postgres_seeded(&dsn).await;
        store.put_budget(&ns, "p", 10_000).await.expect("put");
        store
            .reserve_budget(&ns, 10, Duration::from_millis(1), "old")
            .await
            .expect("hold");
        assert!(store.delete_namespace(&ns).await.expect("delete"));
        tokio::time::sleep(Duration::from_millis(5)).await;
        store
            .put_namespace(NamespaceRecord {
                id: ns.clone(),
                attrs: serde_json::json!({}),
                blocklist: None,
            })
            .await
            .expect("recreate");
        store
            .put_budget(&ns, "p", 10_000)
            .await
            .expect("new ledger");
        match store
            .reserve_budget(&ns, 1, Duration::from_secs(30), "new")
            .await
            .expect("expire path")
        {
            BudgetReserve::Allowed { period } => assert_eq!(period, "p"),
            other => panic!("{other:?}"),
        }
        store
            .settle_budget(&ns, "p", "old", 10)
            .await
            .expect("late settle after TTL");
        let got = store.get_budget(&ns, "p").await.expect("get").expect("row");
        assert_eq!(got.spent_microdollars, 0);
        assert_eq!(got.reserved_microdollars, 1);
    }

    #[tokio::test]
    async fn postgres_expired_tombstone_is_vacuumed_on_reserve_and_late_settle_is_noop() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let (store, ns) = postgres_seeded(&dsn).await;
        store.put_budget(&ns, "p", 10_000).await.expect("put");
        let stale = format!("vac_{ns}");
        store
            .insert_expired_reservation_tombstone(&stale, 1)
            .await
            .expect("past tombstone");
        match store
            .reserve_budget(&ns, 1, Duration::from_secs(30), &format!("live_{ns}"))
            .await
            .expect("reserve vacuums")
        {
            BudgetReserve::Allowed { period } => assert_eq!(period, "p"),
            other => panic!("{other:?}"),
        }
        store
            .settle_budget(&ns, "p", &stale, 99)
            .await
            .expect("late settle");
        let got = store.get_budget(&ns, "p").await.expect("get").expect("row");
        assert_eq!(got.spent_microdollars, 0);
        assert_eq!(got.reserved_microdollars, 1);
    }

    #[tokio::test]
    async fn postgres_concurrent_settle_charges_once() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let (a, ns) = postgres_seeded(&dsn).await;
        let b = PostgresStore::connect(&dsn, true)
            .await
            .expect("second client");
        a.put_budget(&ns, "p", 10_000).await.expect("put");
        let rid = format!("once_{ns}");
        match a
            .reserve_budget(&ns, 50, Duration::from_secs(30), &rid)
            .await
            .expect("hold")
        {
            BudgetReserve::Allowed { period } => assert_eq!(period, "p"),
            other => panic!("{other:?}"),
        }
        let (left, right) = tokio::join!(
            a.settle_budget(&ns, "p", &rid, 50),
            b.settle_budget(&ns, "p", &rid, 50),
        );
        left.expect("settle a");
        right.expect("settle b");
        let got = a.get_budget(&ns, "p").await.expect("get").expect("row");
        assert_eq!(got.spent_microdollars, 50);
        assert_eq!(got.reserved_microdollars, 0);
    }

    #[tokio::test]
    async fn postgres_delete_and_create_cannot_orphan_a_budget() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let a = PostgresStore::connect(&dsn, true).await.expect("a");
        let b = PostgresStore::connect(&dsn, true).await.expect("b");
        for _ in 0..8 {
            let ns = unique_ns("race");
            let rec = NamespaceRecord {
                id: ns.clone(),
                attrs: serde_json::json!({}),
                blocklist: None,
            };
            let (deleted, created) = tokio::join!(a.delete_namespace(&ns), async {
                b.put_namespace(rec.clone()).await?;
                b.put_budget(&ns, "p", 10_000).await
            });
            deleted.expect("delete");
            match created {
                Ok(_) | Err(StoreError::NotFound(_)) => {}
                Err(error) => panic!("{error}"),
            }
            let ns_row = a.get_namespace(&ns).await.expect("get ns");
            let budget = a.get_budget(&ns, "p").await.expect("get budget");
            if ns_row.is_none() {
                assert!(budget.is_none(), "orphaned budget for {ns}");
            }
        }
    }

    #[tokio::test]
    async fn postgres_reconnects_after_a_dropped_connection() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let (store, ns) = postgres_seeded(&dsn).await;
        store.put_budget(&ns, "p", 10_000).await.expect("budget");
        store.drop_idle_connection().await.expect("drop");
        match store
            .reserve_budget(&ns, 1, Duration::from_secs(30), "after-drop")
            .await
            .expect("reserve after drop")
        {
            BudgetReserve::Allowed { period } => assert_eq!(period, "p"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn postgres_put_races_reserve_and_settle_without_deadlock() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let (putter, ns) = postgres_seeded(&dsn).await;
        putter.put_budget(&ns, "p", 10_000).await.expect("budget");
        let reserver = PostgresStore::connect(&dsn, true).await.expect("connect b");
        let ns_put = ns.clone();
        let ns_res = ns.clone();
        let raced = tokio::time::timeout(Duration::from_secs(10), async move {
            let puts = tokio::spawn(async move {
                for i in 0..80u64 {
                    putter
                        .put_budget(&ns_put, "p", 10_000 + i)
                        .await
                        .expect("put");
                }
            });
            let holds = tokio::spawn(async move {
                for i in 0..80 {
                    let id = format!("r{i}");
                    match reserver
                        .reserve_budget(&ns_res, 1, Duration::from_secs(30), &id)
                        .await
                        .expect("reserve")
                    {
                        BudgetReserve::Allowed { period } => {
                            assert_eq!(period, "p");
                            reserver
                                .settle_budget(&ns_res, "p", &id, 1)
                                .await
                                .expect("settle");
                        }
                        BudgetReserve::Exceeded => {}
                    }
                }
            });
            puts.await.expect("put join");
            holds.await.expect("hold join");
        })
        .await;
        assert!(raced.is_ok(), "PUT vs reserve/settle deadlocked");
    }

    #[tokio::test]
    async fn postgres_get_does_not_mix_spend_and_reserved_across_a_settlement() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let (store, ns) = postgres_seeded(&dsn).await;
        store.put_budget(&ns, "p", 100).await.expect("budget");
        store
            .reserve_budget(&ns, 60, Duration::from_secs(30), "r1")
            .await
            .expect("hold");
        let reader = PostgresStore::connect(&dsn, true).await.expect("reader");
        let ns_get = ns.clone();
        let getter = tokio::spawn(async move {
            let mut samples = Vec::new();
            for _ in 0..200 {
                samples.push(
                    reader
                        .get_budget(&ns_get, "p")
                        .await
                        .expect("get")
                        .expect("row"),
                );
            }
            samples
        });
        store
            .settle_budget(&ns, "p", "r1", 60)
            .await
            .expect("settle");
        let samples = getter.await.expect("join");
        assert!(!samples.is_empty());
        for rec in samples {
            assert_eq!(
                rec.remaining_microdollars,
                remaining(
                    rec.limit_microdollars,
                    rec.spent_microdollars,
                    rec.reserved_microdollars
                )
            );
            assert_eq!(
                rec.spent_microdollars + rec.reserved_microdollars,
                60,
                "GET mixed pre- and post-settle snapshots: {rec:?}"
            );
        }
    }

    #[tokio::test]
    async fn postgres_boot_probes_active_and_reservation_tables() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let schema = unique_ns("probe").replace('-', "_");
        let (setup, connection) = tokio_postgres::connect(&dsn, crate::usage::tls_connector())
            .await
            .expect("setup");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        setup
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .expect("schema");
        let sep = if dsn.contains('?') { '&' } else { '?' };
        let scoped = format!("{dsn}{sep}options=-csearch_path%3D{schema}");
        let err = match PostgresStore::connect(&scoped, false).await {
            Err(error) => error,
            Ok(_) => panic!("empty schema must fail boot"),
        };
        assert!(
            matches!(err, StoreError::Unavailable(ref message) if message.contains("schema missing")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn postgres_reserve_expires_holds_from_old_periods() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let (store, ns) = postgres_seeded(&dsn).await;
        store.put_budget(&ns, "old", 10_000).await.expect("old");
        store
            .reserve_budget(&ns, 10, Duration::from_millis(1), "stale")
            .await
            .expect("stale");
        tokio::time::sleep(Duration::from_millis(5)).await;
        store.put_budget(&ns, "new", 10_000).await.expect("new");
        store
            .reserve_budget(&ns, 1, Duration::from_secs(30), "live")
            .await
            .expect("live");
        assert_eq!(
            store.reservation_count(&ns).await.expect("count"),
            1,
            "expired holds from the old period must be reclaimed"
        );
    }

    #[tokio::test]
    async fn postgres_denied_reserve_still_drops_expired_holds() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let (store, ns) = postgres_seeded(&dsn).await;
        store.put_budget(&ns, "p", 100).await.expect("budget");
        store
            .reserve_budget(&ns, 10, Duration::from_millis(1), "stale")
            .await
            .expect("stale");
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(matches!(
            store
                .reserve_budget(&ns, 200, Duration::from_secs(30), "over")
                .await
                .expect("denied"),
            BudgetReserve::Exceeded
        ));
        assert_eq!(
            store.reservation_count(&ns).await.expect("count"),
            0,
            "denied admission must keep the expiry delete"
        );
    }

    #[tokio::test]
    async fn postgres_store_exposes_health_sqlite_does_not() {
        let sqlite = SqliteStore::open(":memory:").expect("sqlite");
        assert!(sqlite.health().is_none());
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let store = PostgresStore::connect(&dsn, true).await.expect("connect");
        assert!(store.health().is_some());
    }

    /// Isolated schema on the shared test Postgres, with `search_path` set on
    /// both the setup client and the Store DSN.
    async fn postgres_isolated(dsn: &str) -> (String, tokio_postgres::Client) {
        let schema = unique_ns("store").replace('-', "_");
        let (setup, connection) = tokio_postgres::connect(dsn, crate::usage::tls_connector())
            .await
            .expect("setup");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        setup
            .batch_execute(&format!(
                "CREATE SCHEMA {schema}; SET search_path TO {schema}"
            ))
            .await
            .expect("schema");
        let sep = if dsn.contains('?') { '&' } else { '?' };
        (format!("{dsn}{sep}options=-csearch_path%3D{schema}"), setup)
    }

    /// Leftover `budget_v1.sql` (`axond_budget` PK `(namespace, subject)`) must
    /// not block Store boot. Spend is not migrated.
    #[tokio::test]
    async fn postgres_legacy_budget_v1_coexists_with_store_budget() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let (scoped, setup) = postgres_isolated(&dsn).await;
        setup
            .batch_execute(include_str!("../../sql/budget_v1.sql"))
            .await
            .expect("legacy budget_v1");
        setup
            .batch_execute(
                "INSERT INTO axond_budget (namespace, subject, spent_microdollars)
                 VALUES ('wsp_legacy', 'user-1', 42)",
            )
            .await
            .expect("legacy row");
        let store = PostgresStore::connect(&scoped, true)
            .await
            .expect("boot beside leftover budget_v1");
        store
            .put_namespace(NamespaceRecord {
                id: "wsp_x".into(),
                attrs: serde_json::json!({}),
                blocklist: None,
            })
            .await
            .expect("ns");
        let rec = store
            .put_budget("wsp_x", "2026-09", 1_000)
            .await
            .expect("store budget");
        assert_eq!(rec.limit_microdollars, 1_000);
        let legacy_spent: i64 = setup
            .query_one(
                "SELECT spent_microdollars FROM axond_budget
                 WHERE namespace = 'wsp_legacy' AND subject = 'user-1'",
                &[],
            )
            .await
            .expect("legacy intact")
            .get(0);
        assert_eq!(legacy_spent, 42);
        let has_subject: bool = setup
            .query_one(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_attribute
                     WHERE attrelid = to_regclass('axond_budget')
                       AND attname = 'subject'
                       AND attnum > 0
                       AND NOT attisdropped
                 )",
                &[],
            )
            .await
            .expect("legacy shape")
            .get(0);
        assert!(
            has_subject,
            "legacy axond_budget must keep its subject column"
        );
        let store_limit: i64 = setup
            .query_one(
                "SELECT limit_microdollars FROM axond_store_budget
                 WHERE namespace = 'wsp_x' AND period = '2026-09'",
                &[],
            )
            .await
            .expect("store row")
            .get(0);
        assert_eq!(store_limit, 1_000);
    }

    /// Earlier draft Store DDL reused `axond_budget` with a `period` column.
    /// Connect renames those relations even when `create_table` is false.
    #[tokio::test]
    async fn postgres_renames_draft_store_budget_tables() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let (scoped, setup) = postgres_isolated(&dsn).await;
        setup
            .batch_execute(
                "CREATE TABLE axond_namespace (
                     id TEXT PRIMARY KEY NOT NULL,
                     attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
                     blocklist JSONB
                 );
                 CREATE TABLE axond_budget (
                     namespace           text        NOT NULL,
                     period              text        NOT NULL,
                     limit_microdollars  bigint      NOT NULL,
                     spent_microdollars  bigint      NOT NULL DEFAULT 0,
                     PRIMARY KEY (namespace, period)
                 );
                 CREATE TABLE axond_budget_active (
                     namespace text PRIMARY KEY NOT NULL,
                     period    text NOT NULL
                 );
                 CREATE TABLE axond_budget_reservation (
                     id                  text        PRIMARY KEY,
                     namespace           text        NOT NULL,
                     period              text        NOT NULL,
                     amount_microdollars bigint      NOT NULL,
                     expires_at          timestamptz NOT NULL,
                     incarnation         bigint      NOT NULL DEFAULT 1
                 );
                 CREATE INDEX axond_budget_reservation_scope_idx
                     ON axond_budget_reservation (namespace, period, expires_at);
                 CREATE TABLE axond_namespace_incarnation (
                     id text PRIMARY KEY NOT NULL,
                     n  bigint NOT NULL
                 );
                 CREATE TABLE axond_store_budget_reservation_tombstone (
                     id          text PRIMARY KEY NOT NULL,
                     incarnation bigint NOT NULL,
                     expires_at  timestamptz NOT NULL
                 );
                 INSERT INTO axond_namespace (id, attrs) VALUES ('wsp_x', '{}'::jsonb);
                 INSERT INTO axond_budget
                     (namespace, period, limit_microdollars, spent_microdollars)
                     VALUES ('wsp_x', '2026-09', 1000, 40);
                 INSERT INTO axond_budget_active (namespace, period)
                     VALUES ('wsp_x', '2026-09');
                 CREATE TABLE axond_store_usage (
                     request_id          text        PRIMARY KEY,
                     namespace           text        NOT NULL,
                     period              text,
                     model               text        NOT NULL,
                     status              text        NOT NULL,
                     cost_microdollars   bigint,
                     recorded_at         timestamptz NOT NULL DEFAULT now()
                 );",
            )
            .await
            .expect("draft store tables");
        setup
            .batch_execute(include_str!("../../sql/store_provider_models_v1.sql"))
            .await
            .expect("provider models");
        let store = PostgresStore::connect(&scoped, false)
            .await
            .expect("rename draft tables");
        let rec = store
            .get_budget("wsp_x", "2026-09")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(rec.spent_microdollars, 40);
        assert_eq!(rec.limit_microdollars, 1_000);
        assert!(rec.active);
        let renamed: bool = setup
            .query_one("SELECT to_regclass('axond_store_budget') IS NOT NULL", &[])
            .await
            .expect("renamed")
            .get(0);
        assert!(renamed);
        let old_gone: bool = setup
            .query_one("SELECT to_regclass('axond_budget') IS NULL", &[])
            .await
            .expect("old name")
            .get(0);
        assert!(old_gone, "draft axond_budget must be renamed, not copied");
    }

    /// Hand-applying `store_budget_v1.sql` creates empty new tables. Connect
    /// must still rename draft spend onto those names.
    #[tokio::test]
    async fn postgres_renames_draft_spend_past_empty_store_budget_ddl() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let (scoped, setup) = postgres_isolated(&dsn).await;
        setup
            .batch_execute(
                "CREATE TABLE axond_namespace (
                     id TEXT PRIMARY KEY NOT NULL,
                     attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
                     blocklist JSONB
                 );
                 CREATE TABLE axond_budget (
                     namespace           text        NOT NULL,
                     period              text        NOT NULL,
                     limit_microdollars  bigint      NOT NULL,
                     spent_microdollars  bigint      NOT NULL DEFAULT 0,
                     PRIMARY KEY (namespace, period)
                 );
                 CREATE TABLE axond_budget_active (
                     namespace text PRIMARY KEY NOT NULL,
                     period    text NOT NULL
                 );
                 CREATE TABLE axond_budget_reservation (
                     id                  text        PRIMARY KEY,
                     namespace           text        NOT NULL,
                     period              text        NOT NULL,
                     amount_microdollars bigint      NOT NULL,
                     expires_at          timestamptz NOT NULL,
                     incarnation         bigint      NOT NULL DEFAULT 1
                 );
                 CREATE INDEX axond_budget_reservation_scope_idx
                     ON axond_budget_reservation (namespace, period, expires_at);
                 CREATE TABLE axond_namespace_incarnation (
                     id text PRIMARY KEY NOT NULL,
                     n  bigint NOT NULL
                 );
                 CREATE TABLE axond_store_budget_reservation_tombstone (
                     id          text PRIMARY KEY NOT NULL,
                     incarnation bigint NOT NULL,
                     expires_at  timestamptz NOT NULL
                 );
                 INSERT INTO axond_namespace (id, attrs) VALUES ('wsp_x', '{}'::jsonb);
                 INSERT INTO axond_budget
                     (namespace, period, limit_microdollars, spent_microdollars)
                     VALUES ('wsp_x', '2026-09', 1000, 40);
                 INSERT INTO axond_budget_active (namespace, period)
                     VALUES ('wsp_x', '2026-09');
                 CREATE TABLE axond_store_usage (
                     request_id          text        PRIMARY KEY,
                     namespace           text        NOT NULL,
                     period              text,
                     model               text        NOT NULL,
                     status              text        NOT NULL,
                     cost_microdollars   bigint,
                     recorded_at         timestamptz NOT NULL DEFAULT now()
                 );",
            )
            .await
            .expect("draft store tables");
        setup
            .batch_execute(include_str!("../../sql/store_provider_models_v1.sql"))
            .await
            .expect("provider models");
        setup
            .batch_execute(include_str!("../../sql/store_budget_v1.sql"))
            .await
            .expect("empty store_budget_v1");
        let store = PostgresStore::connect(&scoped, false)
            .await
            .expect("rename past empty new tables");
        let rec = store
            .get_budget("wsp_x", "2026-09")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(rec.spent_microdollars, 40);
        assert_eq!(rec.limit_microdollars, 1_000);
        assert!(rec.active);
        let old_gone: bool = setup
            .query_one("SELECT to_regclass('axond_budget') IS NULL", &[])
            .await
            .expect("old name")
            .get(0);
        assert!(
            old_gone,
            "draft axond_budget must be renamed, not left beside empty new tables"
        );
    }

    /// Draft period tables with spend, then both shipped Store SQLs, then
    /// `connect(false)`: rename drops the incarnation-bearing reservation
    /// table, so connect must ADD COLUMN. Skipped without
    /// `AXOND_TEST_POSTGRES_DSN`.
    #[tokio::test]
    async fn postgres_connect_false_recovers_incarnation_after_draft_rename() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let (scoped, setup) = postgres_isolated(&dsn).await;
        setup
            .batch_execute(
                "CREATE TABLE axond_namespace (
                     id TEXT PRIMARY KEY NOT NULL,
                     attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
                     blocklist JSONB
                 );
                 CREATE TABLE axond_budget (
                     namespace           text        NOT NULL,
                     period              text        NOT NULL,
                     limit_microdollars  bigint      NOT NULL,
                     spent_microdollars  bigint      NOT NULL DEFAULT 0,
                     PRIMARY KEY (namespace, period)
                 );
                 CREATE TABLE axond_budget_active (
                     namespace text PRIMARY KEY NOT NULL,
                     period    text NOT NULL
                 );
                 CREATE TABLE axond_budget_reservation (
                     id                  text        PRIMARY KEY,
                     namespace           text        NOT NULL,
                     period              text        NOT NULL,
                     amount_microdollars bigint      NOT NULL,
                     expires_at          timestamptz NOT NULL
                 );
                 CREATE INDEX axond_budget_reservation_scope_idx
                     ON axond_budget_reservation (namespace, period, expires_at);
                 INSERT INTO axond_namespace (id, attrs) VALUES ('wsp_x', '{}'::jsonb);
                 INSERT INTO axond_budget
                     (namespace, period, limit_microdollars, spent_microdollars)
                     VALUES ('wsp_x', '2026-09', 1000, 40);
                 INSERT INTO axond_budget_active (namespace, period)
                     VALUES ('wsp_x', '2026-09');
                 CREATE TABLE axond_store_usage (
                     request_id          text        PRIMARY KEY,
                     namespace           text        NOT NULL,
                     period              text,
                     model               text        NOT NULL,
                     status              text        NOT NULL,
                     cost_microdollars   bigint,
                     recorded_at         timestamptz NOT NULL DEFAULT now()
                 );",
            )
            .await
            .expect("draft period tables");
        setup
            .batch_execute(include_str!("../../sql/store_budget_v1.sql"))
            .await
            .expect("budget v1");
        setup
            .batch_execute(include_str!("../../sql/store_namespace_incarnation_v1.sql"))
            .await
            .expect("incarnation v1");
        let store = PostgresStore::connect(&scoped, false)
            .await
            .expect("connect false after draft rename");
        let rec = store
            .get_budget("wsp_x", "2026-09")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(rec.spent_microdollars, 40);
        let has_incarnation: bool = setup
            .query_one(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_attribute
                     WHERE attrelid = to_regclass('axond_store_budget_reservation')
                       AND attname = 'incarnation'
                       AND attnum > 0
                       AND NOT attisdropped
                 )",
                &[],
            )
            .await
            .expect("col")
            .get(0);
        assert!(
            has_incarnation,
            "reservation must have incarnation after draft rename"
        );
    }

    /// Non-empty `axond_store_budget*` already hold migrated spend; leave them.
    #[tokio::test]
    async fn postgres_keeps_nonempty_store_budget_beside_draft() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let (scoped, setup) = postgres_isolated(&dsn).await;
        setup
            .batch_execute(
                "CREATE TABLE axond_namespace (
                     id TEXT PRIMARY KEY NOT NULL,
                     attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
                     blocklist JSONB
                 );
                 CREATE TABLE axond_budget (
                     namespace           text        NOT NULL,
                     period              text        NOT NULL,
                     limit_microdollars  bigint      NOT NULL,
                     spent_microdollars  bigint      NOT NULL DEFAULT 0,
                     PRIMARY KEY (namespace, period)
                 );
                 CREATE TABLE axond_budget_active (
                     namespace text PRIMARY KEY NOT NULL,
                     period    text NOT NULL
                 );
                 INSERT INTO axond_namespace (id, attrs) VALUES ('wsp_x', '{}'::jsonb);
                 INSERT INTO axond_budget
                     (namespace, period, limit_microdollars, spent_microdollars)
                     VALUES ('wsp_x', '2026-09', 1000, 40);
                 INSERT INTO axond_budget_active (namespace, period)
                     VALUES ('wsp_x', '2026-09');
                 CREATE TABLE axond_store_usage (
                     request_id          text        PRIMARY KEY,
                     namespace           text        NOT NULL,
                     period              text,
                     model               text        NOT NULL,
                     status              text        NOT NULL,
                     cost_microdollars   bigint,
                     recorded_at         timestamptz NOT NULL DEFAULT now()
                 );",
            )
            .await
            .expect("draft");
        setup
            .batch_execute(include_str!("../../sql/store_provider_models_v1.sql"))
            .await
            .expect("provider models");
        setup
            .batch_execute(include_str!("../../sql/store_budget_v1.sql"))
            .await
            .expect("new ddl");
        setup
            .batch_execute(include_str!("../../sql/store_namespace_incarnation_v1.sql"))
            .await
            .expect("incarnation ddl");
        setup
            .batch_execute(
                "INSERT INTO axond_store_budget
                     (namespace, period, limit_microdollars, spent_microdollars)
                     VALUES ('wsp_x', '2026-09', 1000, 7);
                 INSERT INTO axond_store_budget_active (namespace, period)
                     VALUES ('wsp_x', '2026-09');",
            )
            .await
            .expect("new spend");
        let store = PostgresStore::connect(&scoped, false)
            .await
            .expect("keep new spend");
        let rec = store
            .get_budget("wsp_x", "2026-09")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(rec.spent_microdollars, 7);
        let draft_remains: bool = setup
            .query_one("SELECT to_regclass('axond_budget') IS NOT NULL", &[])
            .await
            .expect("draft")
            .get(0);
        assert!(
            draft_remains,
            "must not drop draft when new tables have rows"
        );
    }

    async fn postgres_connect_with_draft_and_dest(
        dsn: &str,
        dest_sql: &str,
    ) -> Result<PostgresStore, StoreError> {
        let (scoped, setup) = postgres_isolated(dsn).await;
        setup
            .batch_execute(
                "CREATE TABLE axond_namespace (
                     id TEXT PRIMARY KEY NOT NULL,
                     attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
                     blocklist JSONB
                 );
                 CREATE TABLE axond_budget (
                     namespace           text        NOT NULL,
                     period              text        NOT NULL,
                     limit_microdollars  bigint      NOT NULL,
                     spent_microdollars  bigint      NOT NULL DEFAULT 0,
                     PRIMARY KEY (namespace, period)
                 );
                 CREATE TABLE axond_budget_active (
                     namespace text PRIMARY KEY NOT NULL,
                     period    text NOT NULL
                 );
                 CREATE TABLE axond_budget_reservation (
                     id                  text        PRIMARY KEY,
                     namespace           text        NOT NULL,
                     period              text        NOT NULL,
                     amount_microdollars bigint      NOT NULL,
                     expires_at          timestamptz NOT NULL
                 );
                 INSERT INTO axond_namespace (id, attrs) VALUES ('wsp_x', '{}'::jsonb);
                 INSERT INTO axond_budget
                     (namespace, period, limit_microdollars, spent_microdollars)
                     VALUES ('wsp_x', '2026-09', 1000, 40);
                 INSERT INTO axond_budget_active (namespace, period)
                     VALUES ('wsp_x', '2026-09');",
            )
            .await
            .expect("draft");
        setup.batch_execute(dest_sql).await.expect("dest");
        PostgresStore::connect(&scoped, false).await
    }

    fn assert_partial_dest_boot_error(err: StoreError, table: &str) {
        let named = format!("{table} ");
        assert!(
            matches!(
                err,
                StoreError::Unavailable(ref message)
                    if message.contains("partial")
                        && message.contains(&named)
                        && message.contains("drop")
            ),
            "{table}: {err:?}"
        );
    }

    /// Dest budget with rows but missing active/reservation must not mix with
    /// a draft rename.
    #[tokio::test]
    async fn postgres_partial_dest_budget_with_rows_refuses_draft_rename() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let err = match postgres_connect_with_draft_and_dest(
            &dsn,
            "CREATE TABLE axond_store_budget (
                 namespace           text        NOT NULL,
                 period              text        NOT NULL,
                 limit_microdollars  bigint      NOT NULL,
                 spent_microdollars  bigint      NOT NULL DEFAULT 0,
                 PRIMARY KEY (namespace, period)
             );
             INSERT INTO axond_store_budget
                 (namespace, period, limit_microdollars, spent_microdollars)
                 VALUES ('wsp_x', '2026-09', 1000, 7);",
        )
        .await
        {
            Err(error) => error,
            Ok(_) => panic!("partial dest budget with rows must fail boot"),
        };
        assert_partial_dest_boot_error(err, "axond_store_budget");
    }

    /// Dest active with rows but missing budget/reservation must not mix with
    /// a draft rename.
    #[tokio::test]
    async fn postgres_partial_dest_active_with_rows_refuses_draft_rename() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let err = match postgres_connect_with_draft_and_dest(
            &dsn,
            "CREATE TABLE axond_store_budget_active (
                 namespace text PRIMARY KEY NOT NULL,
                 period    text NOT NULL
             );
             INSERT INTO axond_store_budget_active (namespace, period)
                 VALUES ('wsp_x', '2026-09');",
        )
        .await
        {
            Err(error) => error,
            Ok(_) => panic!("partial dest active with rows must fail boot"),
        };
        assert_partial_dest_boot_error(err, "axond_store_budget_active");
    }

    /// Dest reservation with rows but missing budget/active must not mix with
    /// a draft rename.
    #[tokio::test]
    async fn postgres_partial_dest_reservation_with_rows_refuses_draft_rename() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let err = match postgres_connect_with_draft_and_dest(
            &dsn,
            "CREATE TABLE axond_store_budget_reservation (
                 id                  text        PRIMARY KEY,
                 namespace           text        NOT NULL,
                 period              text        NOT NULL,
                 amount_microdollars bigint      NOT NULL,
                 expires_at          timestamptz NOT NULL
             );
             INSERT INTO axond_store_budget_reservation
                 (id, namespace, period, amount_microdollars, expires_at)
                 VALUES ('r1', 'wsp_x', '2026-09', 10, now() + interval '1 hour');",
        )
        .await
        {
            Err(error) => error,
            Ok(_) => panic!("partial dest reservation with rows must fail boot"),
        };
        assert_partial_dest_boot_error(err, "axond_store_budget_reservation");
    }

    /// `create_table = false` probes incarnation objects; it must not CREATE
    /// them or ALTER the reservation table. Additive `incarnation` is only
    /// recovered after a draft rename (or `create_table = true`).
    #[tokio::test]
    async fn postgres_create_table_false_does_not_apply_incarnation_ddl() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let (scoped, setup) = postgres_isolated(&dsn).await;
        setup
            .batch_execute(
                "CREATE TABLE axond_namespace (
                     id TEXT PRIMARY KEY NOT NULL,
                     attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
                     blocklist JSONB
                 );
                 CREATE TABLE axond_store_usage (
                     request_id          text        PRIMARY KEY,
                     namespace           text        NOT NULL,
                     period              text,
                     model               text        NOT NULL,
                     status              text        NOT NULL,
                     cost_microdollars   bigint,
                     recorded_at         timestamptz NOT NULL DEFAULT now()
                 );",
            )
            .await
            .expect("ns and usage");
        setup
            .batch_execute(include_str!("../../sql/store_budget_v1.sql"))
            .await
            .expect("budget v1");
        let err = match PostgresStore::connect(&scoped, false).await {
            Err(error) => error,
            Ok(_) => panic!("missing incarnation must fail closed"),
        };
        assert!(
            matches!(err, StoreError::Unavailable(ref message) if message.contains("schema missing")),
            "{err:?}"
        );
        let table: bool = setup
            .query_one(
                "SELECT to_regclass('axond_namespace_incarnation') IS NOT NULL",
                &[],
            )
            .await
            .expect("regclass")
            .get(0);
        assert!(
            !table,
            "create_table=false must not CREATE axond_namespace_incarnation"
        );
        let column: bool = setup
            .query_one(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_attribute
                     WHERE attrelid = to_regclass('axond_store_budget_reservation')
                       AND attname = 'incarnation'
                       AND attnum > 0
                       AND NOT attisdropped
                 )",
                &[],
            )
            .await
            .expect("col")
            .get(0);
        assert!(
            !column,
            "create_table=false must not ADD COLUMN incarnation without a draft rename"
        );
        let tombstone: bool = setup
            .query_one(
                "SELECT to_regclass('axond_store_budget_reservation_tombstone') IS NOT NULL",
                &[],
            )
            .await
            .expect("tombstone")
            .get(0);
        assert!(
            !tombstone,
            "create_table=false must not CREATE reservation tombstone"
        );
    }

    #[tokio::test]
    async fn postgres_settle_saturates_oversized_actual_and_releases_the_hold() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let (store, ns) = postgres_seeded(&dsn).await;
        store.put_budget(&ns, "p", 10_000).await.expect("put");
        match store
            .reserve_budget(&ns, 1, Duration::from_secs(30), "r0")
            .await
            .expect("prior")
        {
            BudgetReserve::Allowed { period } => assert_eq!(period, "p"),
            other => panic!("{other:?}"),
        }
        store
            .settle_budget(&ns, "p", "r0", 40)
            .await
            .expect("prior spend");
        match store
            .reserve_budget(&ns, 1, Duration::from_secs(30), "r1")
            .await
            .expect("reserve")
        {
            BudgetReserve::Allowed { period } => assert_eq!(period, "p"),
            other => panic!("{other:?}"),
        }
        store
            .settle_budget(&ns, "p", "r1", i64::MAX as u64 + 1)
            .await
            .expect("settle");
        let got = store.get_budget(&ns, "p").await.expect("get").expect("row");
        assert_eq!(got.spent_microdollars, i64::MAX as u64);
        assert_eq!(got.reserved_microdollars, 0);
        assert_eq!(store.reservation_count(&ns).await.expect("count"), 0);
    }

    #[tokio::test]
    async fn postgres_append_usage_saturates_oversized_cost() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let (store, ns) = postgres_seeded(&dsn).await;
        store
            .append_usage(UsageAppend {
                request_id: format!("req_over_{ns}"),
                namespace: ns.clone(),
                period: Some("p".into()),
                model: "openai/gpt-4o".into(),
                status: "ok".into(),
                cost_microdollars: Some(i64::MAX as u64 + 1),
            })
            .await
            .expect("append");
        let rows = store.summarize_usage(&ns, "p").await.expect("summarize");
        assert_eq!(
            rows,
            vec![UsageSummaryRow {
                model: "openai/gpt-4o".into(),
                status: "ok".into(),
                count: 1,
                cost_microdollars: i64::MAX as u64,
            }]
        );
    }

    #[tokio::test]
    async fn sqlite_usage_summary_groups_by_model_and_status() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        seeded(&store).await;
        for (id, period, model, status, cost) in [
            ("req_a", "p", "openai/gpt-4o", "ok", Some(10)),
            ("req_b", "p", "openai/gpt-4o", "ok", Some(15)),
            ("req_c", "p", "openai/gpt-4o", "upstream_error", Some(1)),
            ("req_d", "p", "anthropic/claude", "ok", None),
            ("req_e", "other", "openai/gpt-4o", "ok", Some(99)),
            ("req_a", "p", "openai/gpt-4o", "ok", Some(10)),
        ] {
            store
                .append_usage(UsageAppend {
                    request_id: id.into(),
                    namespace: "wsp_x".into(),
                    period: Some(period.into()),
                    model: model.into(),
                    status: status.into(),
                    cost_microdollars: cost,
                })
                .await
                .expect("append");
        }
        let rows = store
            .summarize_usage("wsp_x", "p")
            .await
            .expect("summarize");
        assert_eq!(
            rows,
            vec![
                UsageSummaryRow {
                    model: "anthropic/claude".into(),
                    status: "ok".into(),
                    count: 1,
                    cost_microdollars: 0,
                },
                UsageSummaryRow {
                    model: "openai/gpt-4o".into(),
                    status: "ok".into(),
                    count: 2,
                    cost_microdollars: 25,
                },
                UsageSummaryRow {
                    model: "openai/gpt-4o".into(),
                    status: "upstream_error".into(),
                    count: 1,
                    cost_microdollars: 1,
                },
            ]
        );
        assert!(
            store
                .summarize_usage("wsp_x", "empty")
                .await
                .expect("empty")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn sqlite_usage_summary_saturates_cost_at_i64_max() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        seeded(&store).await;
        let half_plus_one = (i64::MAX / 2) as u64 + 1;
        for id in ["req_a", "req_b"] {
            store
                .append_usage(UsageAppend {
                    request_id: id.into(),
                    namespace: "wsp_x".into(),
                    period: Some("p".into()),
                    model: "openai/gpt-4o".into(),
                    status: "ok".into(),
                    cost_microdollars: Some(half_plus_one),
                })
                .await
                .expect("append");
        }
        let rows = store
            .summarize_usage("wsp_x", "p")
            .await
            .expect("summarize");
        assert_eq!(
            rows,
            vec![UsageSummaryRow {
                model: "openai/gpt-4o".into(),
                status: "ok".into(),
                count: 2,
                cost_microdollars: i64::MAX as u64,
            }]
        );
    }

    #[tokio::test]
    async fn sqlite_append_usage_saturates_oversized_cost() {
        let store = SqliteStore::open(":memory:").expect("memory sqlite");
        seeded(&store).await;
        store
            .append_usage(UsageAppend {
                request_id: "req_over".into(),
                namespace: "wsp_x".into(),
                period: Some("p".into()),
                model: "openai/gpt-4o".into(),
                status: "ok".into(),
                cost_microdollars: Some(i64::MAX as u64 + 1),
            })
            .await
            .expect("append");
        let rows = store
            .summarize_usage("wsp_x", "p")
            .await
            .expect("summarize");
        assert_eq!(
            rows,
            vec![UsageSummaryRow {
                model: "openai/gpt-4o".into(),
                status: "ok".into(),
                count: 1,
                cost_microdollars: i64::MAX as u64,
            }]
        );
    }
}
