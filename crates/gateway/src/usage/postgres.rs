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
const SCHEMA_DDL: &str = include_str!("../../../../ops/postgres/usage_v2.sql");

/// Additive migrations for the current schema version. These are applied after
/// the base DDL for fresh tables; existing installations apply them before
/// deploying a writer that emits the new column.
const ADDITIVE_DDL: &str = include_str!("../../../../ops/postgres/usage_v1_001_add_signer_kid.sql");

/// The table name the shipped DDL uses; substituted when the sink is configured
/// with another one.
const DEFAULT_TABLE: &str = "axond_usage";

/// Stands in for an index-name prefix while the table name is substituted, so
/// the two do not rewrite each other. Not valid SQL, and never left in the DDL.
const INDEX_PREFIX_PLACEHOLDER: &str = "\u{1}index_prefix\u{1}";

/// Columns written per row, in parameter order. `reasoning_tokens` remains
/// reserved for a future schema version; the cache counters are canonical.
const COLUMNS: [&str; 22] = [
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
        *sink.client.lock().await = Some(client);
        Ok(sink)
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
        format!("{}\n{}", retarget(SCHEMA_DDL), retarget(ADDITIVE_DDL))
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

/// A multi-row `INSERT` with one parameter set per row.
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
        Box::new(bigint(record.cost_microdollars)),
        Box::new(bigint(record.catalog_version)),
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
                SCHEMA_DDL.contains(column) || ADDITIVE_DDL.contains(column),
                "column `{column}` is written but not declared in the base or additive DDL"
            );
        }
        assert_eq!(UsageRecord::SCHEMA_VERSION, 2);
        assert!(SCHEMA_DDL.contains("version 2"));
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

        let guard = sink.client.lock().await;
        let client = guard.as_ref().expect("reconnected");
        let rows = client
            .query_one(&format!("SELECT count(*) FROM {table}"), &[])
            .await
            .expect("count");
        assert_eq!(rows.get::<_, i64>(0), 0);
    }
}
