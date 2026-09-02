//! The durable usage sink: batched multi-row inserts into a versioned table.
//!
//! The schema is the expensive part of this sink — it lands in an adopter's own
//! database and is read by their billing queries — so it is treated as an API:
//! it lives in [`ops/postgres/usage_v2.sql`](../../../../ops/postgres/usage_v2.sql),
//! every row carries `schema_version`, and a change to the row shape is a new
//! versioned file rather than an edit (ADR 0009).
//!
//! One connection serves the sink, driven by the single flush task that
//! [`super::BatchedSink`] owns. A failed write drops the connection so the next
//! batch reconnects; the batch itself is retried once and then dropped and
//! counted, because the alternative — unbounded retention — trades the request
//! path for records nobody can read yet.

use std::sync::OnceLock;
use std::time::Duration;

use async_trait::async_trait;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, Config};
use tokio_postgres_rustls::MakeRustlsConnect;

use super::{ObservedRecord, SinkFailure, UsageRecord, UsageSink, UsageSinkError};

/// The DDL for the current schema version, shared with operators who apply it
/// themselves. The table and the serialized record are one schema with one
/// version — [`UsageRecord::SCHEMA_VERSION`] — not two that can drift.
// Embedded from the package-local copy of `ops/postgres/usage_v2.sql`, because
// `ops/` is outside this crate and so outside the published package.
// `tests/shipped_ddl.rs` fails if the two copies differ by a byte.
const SCHEMA_DDL: &str = include_str!("../../sql/usage_v2.sql");

/// Additive migrations for the current schema version, in application order.
/// These are applied after the base DDL for fresh tables; existing
/// installations apply them before deploying a writer that emits the new
/// columns. Nullable columns only, so a writer deployed ahead of one of them
/// still writes rows the earlier shape can read.
const ADDITIVE_DDL: [&str; 3] = [
    include_str!("../../sql/usage_v1_001_add_signer_kid.sql"),
    include_str!("../../sql/usage_v2_001_add_price_identity.sql"),
    include_str!("../../sql/usage_v2_002_nullable_cost.sql"),
];

/// Which migration adds each column the base DDL of the current schema version
/// does not have, so a writer that would bind a column the table lacks can name
/// the file to apply instead of failing every batch at insert time.
///
/// Ordering is the enforcement: a writer binds all of [`COLUMNS`], so it refuses
/// to boot against a table an operator has not migrated yet, rather than
/// discovering it one dropped batch at a time.
const ADDITIVE_COLUMNS: [(&str, &str); 4] = [
    ("signer_kid", "usage_v1_001_add_signer_kid.sql"),
    ("price_book", "usage_v2_001_add_price_identity.sql"),
    ("price_book_checksum", "usage_v2_001_add_price_identity.sql"),
    ("price_catalog", "usage_v2_001_add_price_identity.sql"),
];

/// The table name the shipped DDL uses; substituted when the sink is configured
/// with another one.
const DEFAULT_TABLE: &str = "axond_usage";

/// Stands in for an index-name prefix while the table name is substituted, so
/// the two do not rewrite each other. Not valid SQL, and never left in the DDL.
const INDEX_PREFIX_PLACEHOLDER: &str = "\u{1}index_prefix\u{1}";

/// Columns written per row, in parameter order. `reasoning_tokens` remains
/// reserved for a future schema version; the cache counters are canonical.
const COLUMNS: [&str; 25] = [
    "schema_version",
    "request_id",
    "trace_id",
    "namespace",
    "subject",
    "signer_kid",
    "model",
    "target_provider",
    "target_model",
    "credential_source",
    "credential_id",
    "status",
    "input_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
    "output_tokens",
    "cost_microdollars",
    "catalog_version",
    "price_book",
    "price_book_checksum",
    "price_catalog",
    "latency_ms",
    "attempts",
    "started_at",
    "recorded_at",
];

/// The wire protocol carries a 16-bit parameter count, so one statement can
/// bind at most 65535 values.
const MAX_BIND_PARAMETERS: usize = u16::MAX as usize;

/// Rows one INSERT can carry. Configured batches larger than this are split
/// into sequential statements by `record_batch`.
pub const MAX_ROWS_PER_STATEMENT: usize = MAX_BIND_PARAMETERS / COLUMNS.len();

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct PostgresSinkSettings {
    pub table: String,
    /// Apply the shipped DDL at boot. Off by default: in most deployments the
    /// gateway's role has no DDL rights, and schema changes are the operator's
    /// to sequence.
    pub create_table: bool,
}

pub struct PostgresSink {
    table: String,
    config: Config,
    /// `None` until the first write and after any failure, so a broken
    /// connection is replaced rather than reused.
    client: tokio::sync::Mutex<Option<Client>>,
}

impl PostgresSink {
    /// Connect, validate the table name, and optionally create the table.
    ///
    /// Failing here means the process refuses to boot, which is the point: a
    /// usage sink that cannot write is a silent data-loss bug, and the config
    /// graph is validated at boot rather than at request time.
    pub async fn connect(
        dsn: &str,
        settings: PostgresSinkSettings,
    ) -> Result<Self, UsageSinkError> {
        validate_table_name(&settings.table)
            .map_err(|message| UsageSinkError::invalid("postgres", message))?;
        let mut config: Config = dsn
            .parse()
            .map_err(|e| UsageSinkError::invalid("postgres", format!("unparsable DSN: {e}")))?;
        config.connect_timeout(CONNECT_TIMEOUT);
        config.application_name(crate::telemetry::SERVICE_NAME);

        let sink = Self {
            table: settings.table,
            config,
            client: tokio::sync::Mutex::new(None),
        };
        let client = sink.connect_client().await?;
        if settings.create_table {
            client.batch_execute(&sink.schema_ddl()).await?;
        }
        // Migration before writer: every column this sink binds must already
        // exist, or the boot fails naming the file to apply. An existing
        // installation that has not run a migration would otherwise accept the
        // boot and lose every batch at insert time. This is intentionally
        // fail-closed for every additive migration, including older ones.
        if let Some(gap) = migration_gap(&sink.missing_columns(&client).await?) {
            return Err(UsageSinkError::invalid("postgres", gap));
        }
        *sink.client.lock().await = Some(client);
        Ok(sink)
    }

    /// The columns this writer binds that the configured table does not have.
    ///
    /// An empty answer for an absent table: creating it is the operator's to
    /// sequence (`create_table` is off by default), and refusing a boot for a
    /// table that will be created before the first flush would be a new failure
    /// mode rather than a caught one.
    ///
    /// `to_regclass($1)` is deliberate. It resolves the configured relation on
    /// this connection using its `search_path`, exactly as the unqualified
    /// INSERT does; reconstructing `public` from the configured string would
    /// check a different table for DSNs that set `options=-csearch_path=...`.
    async fn missing_columns(&self, client: &Client) -> Result<Vec<String>, tokio_postgres::Error> {
        let rows = client
            .query(
                "SELECT a.attname \
                 FROM pg_attribute AS a \
                 WHERE a.attrelid = to_regclass($1) \
                   AND a.attnum > 0 \
                   AND NOT a.attisdropped",
                &[&self.table],
            )
            .await?;
        let present: Vec<String> = rows.iter().map(|row| row.get::<_, String>(0)).collect();
        if present.is_empty() {
            return Ok(Vec::new());
        }
        Ok(COLUMNS
            .iter()
            .filter(|column| !present.iter().any(|present| present == *column))
            .map(|column| (*column).to_owned())
            .collect())
    }

    /// The shipped DDL, retargeted at the configured table.
    ///
    /// Table references and index names are substituted separately: an index
    /// lives in its table's schema and its name may not carry a qualifier, so
    /// `billing.axond_usage` yields `ON billing.axond_usage` but
    /// `axond_usage_recorded_at_idx`.
    fn schema_ddl(&self) -> String {
        let index_prefix = self.table.rsplit('.').next().unwrap_or(&self.table);
        let retarget = |ddl: &str| {
            ddl.replace(&format!("{DEFAULT_TABLE}_"), INDEX_PREFIX_PLACEHOLDER)
                .replace(DEFAULT_TABLE, &self.table)
                .replace(INDEX_PREFIX_PLACEHOLDER, &format!("{index_prefix}_"))
        };
        let mut ddl = retarget(SCHEMA_DDL);
        for additive in ADDITIVE_DDL {
            ddl.push('\n');
            ddl.push_str(&retarget(additive));
        }
        ddl
    }

    async fn connect_client(&self) -> Result<Client, tokio_postgres::Error> {
        // `sslmode` in the DSN decides whether the connector is used; supplying
        // one always means managed Postgres works without a second build.
        let (client, connection) = self.config.connect(tls_connector()).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!(error = %e, "postgres usage connection closed");
            }
        });
        Ok(client)
    }

    /// Insert all chunks in one transaction, reconnecting once if the
    /// connection was stale or the transaction failed.
    async fn insert_batch(&self, batch: &[ObservedRecord]) -> Result<(), SinkFailure> {
        let mut guard = self.client.lock().await;
        let mut last_error: Option<tokio_postgres::Error> = None;
        for _ in 0..2 {
            let mut client = match guard.take() {
                Some(client) if !client.is_closed() => client,
                _ => match self.connect_client().await {
                    Ok(client) => client,
                    Err(e) => {
                        last_error = Some(e);
                        continue;
                    }
                },
            };
            let transaction = match client.transaction().await {
                Ok(transaction) => transaction,
                Err(e) => {
                    last_error = Some(e);
                    continue;
                }
            };
            let result = async {
                for chunk in batch.chunks(MAX_ROWS_PER_STATEMENT) {
                    let sql = insert_sql(&self.table, chunk.len());
                    let values: Vec<Vec<Box<dyn ToSql + Sync + Send>>> =
                        chunk.iter().map(row).collect();
                    let params: Vec<&(dyn ToSql + Sync)> = values
                        .iter()
                        .flatten()
                        .map(|value| value.as_ref() as &(dyn ToSql + Sync))
                        .collect();
                    transaction.execute(sql.as_str(), &params).await?;
                }
                transaction.commit().await
            }
            .await;
            match result {
                Ok(()) => {
                    *guard = Some(client);
                    return Ok(());
                }
                // The transaction is rolled back and the connection is
                // discarded either way: a failed write is not safely reusable.
                Err(e) => last_error = Some(e),
            }
        }
        Err(SinkFailure::new(last_error.map_or_else(
            || "insert failed".to_owned(),
            |e| e.to_string(),
        )))
    }
}

#[async_trait]
impl UsageSink for PostgresSink {
    fn name(&self) -> &'static str {
        "postgres"
    }

    async fn record(&self, record: &UsageRecord) {
        let batch = [ObservedRecord::now(record.clone())];
        if let Err(e) = self.record_batch(&batch).await {
            tracing::warn!(error = %e, "usage record not persisted");
        }
    }

    async fn record_batch(&self, batch: &[ObservedRecord]) -> Result<(), SinkFailure> {
        self.insert_batch(batch).await
    }
}

/// Rustls with the Mozilla root bundle, built once. The process-default crypto
/// provider is installed here because `reqwest` configures its own explicitly
/// and so leaves the default unset. Shared with the Postgres budget backend, so
/// both Postgres connections speak TLS the same way.
pub fn tls_connector() -> MakeRustlsConnect {
    static CONNECTOR: OnceLock<MakeRustlsConnect> = OnceLock::new();
    CONNECTOR
        .get_or_init(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
            MakeRustlsConnect::with_webpki_roots()
        })
        .clone()
}

/// Why a boot is refused when the table is behind this writer, naming the
/// migrations to apply and in which order.
///
/// A column with no migration of its own belongs to the base DDL, so the table
/// is not merely unmigrated: it was created from an older schema version, and
/// the answer is that version's file rather than a migration.
fn migration_gap(missing: &[String]) -> Option<String> {
    if missing.is_empty() {
        return None;
    }
    let mut files: Vec<&str> = Vec::new();
    for (column, file) in ADDITIVE_COLUMNS {
        if missing.iter().any(|name| name == column) && !files.contains(&file) {
            files.push(file);
        }
    }
    let remedy = if files.is_empty() {
        format!(
            "recreate it from ops/postgres/usage_v{}.sql",
            UsageRecord::SCHEMA_VERSION
        )
    } else {
        format!(
            "apply ops/postgres/{} before deploying this writer",
            files.join(", then ops/postgres/")
        )
    };
    Some(format!(
        "usage table is missing column(s) {}: {remedy}",
        missing.join(", ")
    ))
}

/// A multi-row `INSERT` with one parameter set per row.
///
/// `ON CONFLICT DO NOTHING` carries no target, so it is inert on the shipped DDL
/// (which indexes `request_id` without a unique constraint) and absorbs the
/// duplicate on a table where an operator has added the unique index
/// `docs/usage-schema.md` describes. That is what makes this sink a legitimate
/// destination for the billing-grade outbox, whose redelivery after a lease
/// expiry is routine rather than exceptional: without it a duplicate either
/// double-counts the spend or fails the batch until the event is quarantined.
fn insert_sql(table: &str, rows: usize) -> String {
    let mut sql = String::with_capacity(64 + rows * COLUMNS.len() * 5);
    sql.push_str("INSERT INTO ");
    sql.push_str(table);
    sql.push_str(" (");
    sql.push_str(&COLUMNS.join(", "));
    sql.push_str(") VALUES ");
    for row in 0..rows {
        if row > 0 {
            sql.push_str(", ");
        }
        sql.push('(');
        for column in 0..COLUMNS.len() {
            if column > 0 {
                sql.push_str(", ");
            }
            sql.push('$');
            sql.push_str(&(row * COLUMNS.len() + column + 1).to_string());
        }
        sql.push(')');
    }
    sql.push_str(" ON CONFLICT DO NOTHING");
    sql
}

/// Bind values for one row, in `COLUMNS` order. Counts are `u64` in the record
/// and `bigint` on the wire, so an implausible value saturates rather than
/// wrapping into a negative row.
fn row(observed: &ObservedRecord) -> Vec<Box<dyn ToSql + Sync + Send>> {
    let record = &observed.record;
    let recorded_at = observed.observed_at;
    let started_at = recorded_at
        .checked_sub(Duration::from_millis(record.latency_ms))
        .unwrap_or(recorded_at);
    vec![
        Box::new(record.schema_version as i32),
        Box::new(record.request_id.clone()),
        Box::new(record.trace_id.clone()),
        Box::new(record.namespace.clone()),
        Box::new(record.subject.clone()),
        Box::new(record.signer_kid.clone()),
        Box::new(record.model.clone()),
        Box::new(record.target_provider.clone()),
        Box::new(record.target_model.clone()),
        Box::new(record.credential_source.to_owned()),
        Box::new(record.credential_id.clone()),
        Box::new(record.status.as_str().to_owned()),
        Box::new(bigint(record.input_tokens)),
        Box::new(bigint(record.cache_read_tokens)),
        Box::new(bigint(record.cache_write_tokens)),
        Box::new(bigint(record.output_tokens)),
        Box::new(record.cost_microdollars.map(bigint)),
        Box::new(bigint(record.catalog_version)),
        Box::new(record.price_book.clone()),
        Box::new(record.price_book_checksum.clone()),
        Box::new(record.price_catalog.clone()),
        Box::new(bigint(record.latency_ms)),
        Box::new(record.attempts as i64),
        Box::new(started_at),
        Box::new(recorded_at),
    ]
}

fn bigint(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// Table names come from config and are interpolated into SQL, so the accepted
/// shape is narrow: an optional schema qualifier, lowercase identifiers only.
pub fn validate_table_name(table: &str) -> Result<(), String> {
    let parts: Vec<&str> = table.split('.').collect();
    if parts.len() > 2 {
        return Err(format!(
            "`{table}` is not a valid table name: at most one schema qualifier"
        ));
    }
    for part in parts {
        let valid = !part.is_empty()
            && part.len() <= 63
            && part.starts_with(|c: char| c.is_ascii_lowercase() || c == '_')
            && part
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
        if !valid {
            return Err(format!(
                "`{table}` is not a valid table name: use lowercase letters, digits, and underscores"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::tests::sample_record;
    use super::*;

    fn observed() -> ObservedRecord {
        ObservedRecord::now(sample_record())
    }

    #[test]
    fn the_row_shape_matches_the_shipped_ddl() {
        for column in COLUMNS {
            assert!(
                SCHEMA_DDL.contains(column)
                    || ADDITIVE_DDL
                        .iter()
                        .any(|additive| additive.contains(column)),
                "column `{column}` is written but not declared in the base or additive DDL"
            );
        }
        assert_eq!(UsageRecord::SCHEMA_VERSION, 2);
        assert!(SCHEMA_DDL.contains("version 2"));
    }

    /// Migration before writer, on the ordering itself: each additive column is
    /// attributed to the file that adds it, in the order the files apply, so the
    /// remedy a refused boot prints is one an operator can run top to bottom.
    #[test]
    fn every_additive_column_is_attributed_to_the_migration_that_adds_it() {
        let files = [
            (
                "usage_v1_001_add_signer_kid.sql",
                include_str!("../../sql/usage_v1_001_add_signer_kid.sql"),
            ),
            (
                "usage_v2_001_add_price_identity.sql",
                include_str!("../../sql/usage_v2_001_add_price_identity.sql"),
            ),
            (
                "usage_v2_002_nullable_cost.sql",
                include_str!("../../sql/usage_v2_002_nullable_cost.sql"),
            ),
        ];
        for (column, file) in ADDITIVE_COLUMNS {
            assert!(COLUMNS.contains(&column), "`{column}` is never bound");
            let (_, ddl) = files
                .iter()
                .find(|(name, _)| *name == file)
                .expect("an additive column is attributed to a shipped migration");
            assert!(
                ddl.contains(column),
                "`{file}` does not add the `{column}` it is credited with"
            );
        }
        // A boot refused for a v1 and a v2 column names them in apply order.
        let gap = migration_gap(&["price_book".to_owned(), "signer_kid".to_owned()])
            .expect("missing columns are a gap");
        let signer = gap
            .find("usage_v1_001_add_signer_kid.sql")
            .expect("the v1 file is named");
        let price = gap
            .find("usage_v2_001_add_price_identity.sql")
            .expect("the v2 file is named");
        assert!(signer < price, "{gap}");
        assert!(gap.contains("before deploying this writer"), "{gap}");
    }

    /// A table that is level with the writer is not a gap, and a table missing a
    /// base column is a schema-version problem rather than a pending migration.
    #[test]
    fn only_a_column_the_writer_binds_and_the_table_lacks_refuses_a_boot() {
        assert_eq!(migration_gap(&[]), None);
        let older = migration_gap(&["cache_read_tokens".to_owned()]).expect("a gap");
        assert!(older.contains("ops/postgres/usage_v2.sql"), "{older}");
    }

    #[test]
    fn every_column_is_bound_once_per_row() {
        let batch = [observed(), observed()];
        let bound: usize = batch.iter().map(|o| row(o).len()).sum();
        assert_eq!(bound, COLUMNS.len() * 2);
        let sql = insert_sql("axond_usage", 2);
        assert!(sql.starts_with("INSERT INTO axond_usage (schema_version, request_id"));
        assert!(sql.contains(&format!("${}", COLUMNS.len() * 2)));
        assert!(!sql.contains(&format!("${}", COLUMNS.len() * 2 + 1)));
    }

    /// A redelivery from the billing-grade outbox is normal, so the statement
    /// this sink writes has to be one a unique `request_id` index can absorb.
    #[test]
    fn the_insert_absorbs_a_duplicate_rather_than_failing_the_batch() {
        assert!(insert_sql("axond_usage", 1).ends_with(" ON CONFLICT DO NOTHING"));
    }

    #[test]
    fn a_batch_never_exceeds_the_parameter_limit() {
        const _: () = assert!(MAX_ROWS_PER_STATEMENT * COLUMNS.len() <= MAX_BIND_PARAMETERS);
        let oversized: Vec<ObservedRecord> = (0..MAX_ROWS_PER_STATEMENT + 7)
            .map(|_| observed())
            .collect();
        let chunks: Vec<usize> = oversized
            .chunks(MAX_ROWS_PER_STATEMENT)
            .map(<[ObservedRecord]>::len)
            .collect();
        assert_eq!(chunks, vec![MAX_ROWS_PER_STATEMENT, 7]);
    }

    #[test]
    fn started_at_precedes_the_recorded_instant_by_the_latency() {
        let mut record = sample_record();
        record.latency_ms = 250;
        let observed = ObservedRecord::now(record);
        let values = row(&observed);
        assert_eq!(values.len(), COLUMNS.len());
        let started = observed
            .observed_at
            .checked_sub(Duration::from_millis(250))
            .expect("in range");
        assert!(started < observed.observed_at);
    }

    fn ddl_for(table: &str) -> String {
        let sink = PostgresSink {
            table: table.to_owned(),
            config: "host=localhost".parse().expect("static dsn"),
            client: tokio::sync::Mutex::new(None),
        };
        sink.schema_ddl()
    }

    #[test]
    fn the_ddl_is_retargeted_at_the_configured_table() {
        let ddl = ddl_for("usage_rows");
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS usage_rows"));
        assert!(ddl.contains("CREATE INDEX IF NOT EXISTS usage_rows_recorded_at_idx"));
        assert!(!ddl.contains(DEFAULT_TABLE));
    }

    /// An index lives in its table's schema and its *name* may not be
    /// qualified, so the qualifier belongs to the table reference only.
    #[test]
    fn a_schema_qualified_table_keeps_its_index_names_unqualified() {
        let ddl = ddl_for("billing.axond_usage");
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS billing.axond_usage"));
        for index in [
            "axond_usage_recorded_at_idx",
            "axond_usage_namespace_recorded_at_idx",
            "axond_usage_request_id_idx",
        ] {
            assert!(
                ddl.contains(&format!(
                    "CREATE INDEX IF NOT EXISTS {index}\n    ON billing.axond_usage"
                )),
                "index `{index}` is not created on the qualified table with an unqualified name"
            );
        }
        assert!(
            !ddl.contains("billing.axond_usage_"),
            "qualified index name"
        );
        assert!(!ddl.contains(INDEX_PREFIX_PLACEHOLDER));
    }

    #[test]
    fn table_names_that_could_carry_sql_are_rejected() {
        assert!(validate_table_name("axond_usage").is_ok());
        assert!(validate_table_name("billing.axond_usage").is_ok());
        for bad in [
            "",
            "Axond_Usage",
            "usage; drop table users",
            "usage\"",
            "a.b.c",
            "9usage",
        ] {
            assert!(validate_table_name(bad).is_err(), "accepted `{bad}`");
        }
    }

    /// Round-trips a batch through a real database when one is offered. Skipped
    /// (not failed) otherwise, so the suite stays runnable with no datastore —
    /// the same posture as the gateway itself.
    #[tokio::test]
    async fn a_batch_lands_in_postgres() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let table = "axond_usage_test";
        let sink = PostgresSink::connect(
            &dsn,
            PostgresSinkSettings {
                table: table.to_owned(),
                create_table: true,
            },
        )
        .await
        .expect("connect");
        {
            let guard = sink.client.lock().await;
            let client = guard.as_ref().expect("connected");
            client
                .execute(&format!("TRUNCATE {table}"), &[])
                .await
                .expect("truncate");
        }

        let batch: Vec<ObservedRecord> = (0..3).map(|_| observed()).collect();
        sink.record_batch(&batch).await.expect("insert");

        let guard = sink.client.lock().await;
        let client = guard.as_ref().expect("connected");
        let rows = client
            .query(
                &format!("SELECT schema_version, namespace, cost_microdollars, latency_ms, recorded_at - started_at FROM {table}"),
                &[],
            )
            .await
            .expect("select");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get::<_, i32>(0), UsageRecord::SCHEMA_VERSION as i32);
        assert_eq!(rows[0].get::<_, &str>(1), "acme");
        assert_eq!(rows[0].get::<_, i64>(2), 640);
        assert_eq!(rows[0].get::<_, i64>(3), 812);
    }

    /// The writer and its boot gate must resolve an unqualified table through
    /// the same connection search path. Looking only in `public` would treat
    /// an unmigrated table in the configured schema as absent and let the
    /// writer boot before dropping every insert.
    #[tokio::test]
    async fn the_schema_gate_follows_the_connection_search_path_and_names_the_gap() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let suffix = std::process::id();
        let schema = format!("axond_usage_search_path_{suffix}");
        let table = format!("axond_usage_{suffix}");
        let setup = PostgresSink {
            table: table.clone(),
            config: dsn.parse().expect("static test dsn"),
            client: tokio::sync::Mutex::new(None),
        };
        let setup_client = setup.connect_client().await.expect("connect");
        setup_client
            .batch_execute(&format!(
                "CREATE SCHEMA IF NOT EXISTS {schema}; \
                 DROP TABLE IF EXISTS {schema}.{table}; \
                 CREATE TABLE {schema}.{table} (schema_version integer)"
            ))
            .await
            .expect("create an intentionally unmigrated table on the search path");

        // Use the DSN's startup option rather than a later SET so this covers
        // the same connection-level search_path an operator configures for
        // the INSERT path.
        let mut config: Config = dsn.parse().expect("static test dsn");
        config.options(format!("-csearch_path={schema}"));
        let sink = PostgresSink {
            table: table.clone(),
            config,
            client: tokio::sync::Mutex::new(None),
        };
        let client = sink
            .connect_client()
            .await
            .expect("connect with search_path");

        let missing = sink
            .missing_columns(&client)
            .await
            .expect("inspect the table resolved through search_path");
        let qualified = format!("{schema}.{table}");
        let resolves_to_expected_relation: bool = client
            .query_one(
                "SELECT to_regclass($1) = to_regclass($2)",
                &[&table, &qualified],
            )
            .await
            .expect("resolve the same relation as INSERT")
            .get(0);
        assert!(
            resolves_to_expected_relation,
            "the unqualified relation must resolve to {qualified} through search_path"
        );
        assert!(missing.iter().any(|column| column == "price_book"));
        assert!(missing.iter().any(|column| column == "signer_kid"));
        let gap = migration_gap(&missing).expect("an unmigrated table is a boot gap");
        assert!(gap.contains("usage_v1_001_add_signer_kid.sql"), "{gap}");
        assert!(gap.contains("usage_v2_001_add_price_identity.sql"), "{gap}");
        assert!(
            gap.contains("before deploying this writer"),
            "the gate is fail-closed with an operator remedy: {gap}"
        );

        client
            .batch_execute(&format!("DROP TABLE {schema}.{table}"))
            .await
            .expect("drop the test table");
    }

    #[tokio::test]
    async fn a_later_chunk_failure_rolls_back_the_whole_batch() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let table = "axond_usage_atomicity_test";
        let sink = PostgresSink::connect(
            &dsn,
            PostgresSinkSettings {
                table: table.to_owned(),
                create_table: true,
            },
        )
        .await
        .expect("connect");
        {
            let guard = sink.client.lock().await;
            let client = guard.as_ref().expect("connected");
            client
                .execute(&format!("TRUNCATE {table}"), &[])
                .await
                .expect("truncate");
            client
                .execute(
                    &format!(
                        "ALTER TABLE {table} DROP CONSTRAINT IF EXISTS atomicity_test_request_id"
                    ),
                    &[],
                )
                .await
                .expect("drop prior failure constraint");
            client
                .execute(
                    &format!(
                        "ALTER TABLE {table} ADD CONSTRAINT atomicity_test_request_id \
                         CHECK (request_id <> 'fail-later')"
                    ),
                    &[],
                )
                .await
                .expect("add failure constraint");
        }

        let mut batch: Vec<ObservedRecord> =
            (0..=MAX_ROWS_PER_STATEMENT).map(|_| observed()).collect();
        batch.last_mut().expect("second chunk").record.request_id = "fail-later".into();
        assert!(
            sink.record_batch(&batch).await.is_err(),
            "the constrained second chunk must fail"
        );

        let client = sink.connect_client().await.expect("reconnect");
        let rows = client
            .query_one(&format!("SELECT count(*) FROM {table}"), &[])
            .await
            .expect("count");
        assert_eq!(rows.get::<_, i64>(0), 0);
    }
}
