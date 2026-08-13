//! The durable [`CatalogStore`]: imported catalogues in PostgreSQL.
//!
//! The tables are `ops/postgres/catalog_v1.sql`, and their shape carries the
//! contract rather than restating it in Rust:
//!
//! - `axond_catalog_snapshot` is keyed by content identity, so retention is
//!   idempotent by construction. A re-import of unchanged content conflicts on
//!   the primary key and stores nothing; nothing updates a row, so an approved
//!   price book's pinned content stays exactly what it was approved against.
//! - `axond_catalog_active` is one row, enforced by a constant primary key. Two
//!   active catalogues are unrepresentable rather than merely unwritten.
//! - The active row references a snapshot, so an active pointer to bytes nobody
//!   stored cannot be committed — the one state
//!   [`CatalogStoreError::Corrupt`] would have to report is refused by the
//!   database instead.
//!
//! Activation is a transaction: the insert and the pointer move commit together,
//! because a replica that crashed between them would leave the deployment either
//! missing an import it admitted or pointing at a snapshot it never wrote.
//!
//! Failures are classified by who has to act, the way the secret store's boot
//! path is: a `SQLSTATE` only an operator can clear — no table, no grant, no such
//! database — is [`CatalogStoreError::Denied`] and carries a remedy, because
//! retrying it forever changes nothing, while everything else is
//! [`CatalogStoreError::Unavailable`] and costs one refused refresh. Both refuse
//! the *import*, never an inference request: nothing in this module is on the
//! request path.

use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, Config, Row};

use super::{CatalogStore, CatalogStoreError, RetainedCatalog, Retention, StoredCatalogState};
use crate::backends::catalog::{
    CatalogContentId, ETag, HttpDate, RawPayload, RefusalReason, SchemaVersion, SourceSnapshot,
    SourceValidators,
};
use crate::backends::{Capabilities, Capability};
use crate::desired_state::{BlobKind, BlobRef, Checksum};

const BACKEND: &str = "postgres";

/// The shipped DDL this store applies with `create_table = true`.
const SCHEMA_DDL: &str = include_str!("../../../sql/catalog_v1.sql");

/// How the store connects, and what it may do at boot.
#[derive(Debug, Clone)]
pub struct CatalogStoreSettings {
    /// The PostgreSQL schema the tables live in, if not the connection's
    /// default. Validated as an identifier, because it is interpolated into
    /// `SET search_path`.
    pub schema: Option<String>,
    /// Whether boot may apply the shipped DDL. An operator who applies it out of
    /// band leaves this off and gets a refusal instead of a schema change.
    pub create_table: bool,
    pub connect_timeout: Duration,
    /// The ceiling on one catalogue operation. Generous: a snapshot is a
    /// multi-megabyte payload, and nothing here is called with a request in
    /// flight.
    pub operation_timeout: Duration,
}

impl Default for CatalogStoreSettings {
    fn default() -> Self {
        Self {
            schema: None,
            create_table: true,
            connect_timeout: Duration::from_secs(10),
            operation_timeout: Duration::from_secs(30),
        }
    }
}

/// A [`CatalogStore`] holding imported catalogues in `axond_catalog_snapshot`.
pub struct PostgresCatalogStore {
    config: Config,
    settings: CatalogStoreSettings,
    /// Set on every connection, including reconnections: a reconnect that landed
    /// on the default schema would silently read a different table.
    search_path: Option<String>,
    client: tokio::sync::Mutex<Option<Client>>,
}

/// Written by hand: a derived one would print the [`Config`], which carries the
/// password from the DSN.
impl std::fmt::Debug for PostgresCatalogStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresCatalogStore")
            .field("schema", &self.search_path)
            .finish_non_exhaustive()
    }
}

impl PostgresCatalogStore {
    /// Connect, optionally apply the shipped DDL, and prove both tables are
    /// readable.
    pub async fn connect(
        dsn: &str,
        settings: CatalogStoreSettings,
    ) -> Result<Self, CatalogStoreError> {
        let mut config: Config = dsn.parse().map_err(|error| {
            // The DSN itself is never echoed: it carries a password.
            CatalogStoreError::denied(
                BACKEND,
                format!("the catalogue-store DSN could not be parsed: {error}"),
            )
        })?;
        config.connect_timeout(settings.connect_timeout);
        config.application_name(crate::telemetry::SERVICE_NAME);
        let search_path = settings
            .schema
            .as_deref()
            .map(|schema| {
                crate::usage::validate_table_name(schema)
                    .map_err(|error| CatalogStoreError::denied(BACKEND, error))?;
                if schema.contains('.') {
                    return Err(CatalogStoreError::denied(
                        BACKEND,
                        format!("`{schema}` is not a single unqualified schema name"),
                    ));
                }
                Ok(schema.to_owned())
            })
            .transpose()?;

        let store = Self {
            config,
            settings,
            search_path,
            client: tokio::sync::Mutex::new(None),
        };
        let client = tokio::time::timeout(store.settings.connect_timeout, store.connect_client())
            .await
            .map_err(|_| CatalogStoreError::unavailable(BACKEND, "connection timed out"))?
            .map_err(|error| {
                boot_failure("connect to the catalogue store", &error, || {
                    "Check the role, password, and database named by the connection string under \
                     `[catalog]`."
                        .to_owned()
                })
            })?;
        store.prepare_schema(&client).await?;
        *store.client.lock().await = Some(client);
        Ok(store)
    }

    /// Apply the shipped DDL when allowed, and either way establish that the
    /// tables this build writes are present and readable.
    ///
    /// Boot refuses rather than degrades: a missing table with
    /// `create_table = false` is [`CatalogStoreError::Denied`], because a store
    /// that carried on would refuse every refresh with a storage error that
    /// pointed at the wrong thing.
    async fn prepare_schema(&self, client: &Client) -> Result<(), CatalogStoreError> {
        if self.settings.create_table {
            client.batch_execute(SCHEMA_DDL).await.map_err(|error| {
                boot_failure("apply the catalogue-store schema", &error, || {
                    "Grant the connecting role `CREATE` on the schema, or apply \
                     `ops/postgres/catalog_v1.sql` yourself and set `create_table = false` under \
                     `[catalog]`."
                        .to_owned()
                })
            })?;
        }
        for table in ["axond_catalog_snapshot", "axond_catalog_active"] {
            client
                .query_one(&format!("SELECT count(*) FROM {table} WHERE false"), &[])
                .await
                .map_err(|error| {
                    boot_failure(&format!("read the catalogue store's `{table}` table"), &error, || {
                        "Apply `ops/postgres/catalog_v1.sql`, or set `create_table = true` under \
                         `[catalog]` to let boot apply it."
                            .to_owned()
                    })
                })?;
        }
        Ok(())
    }

    async fn connect_client(&self) -> Result<Client, tokio_postgres::Error> {
        let (client, connection) = self.config.connect(crate::usage::tls_connector()).await?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::warn!(%error, "postgres catalogue-store connection closed");
            }
        });
        if let Some(schema) = &self.search_path {
            // Validated as an identifier at construction, so this is not an
            // injection point; there is no parameter form of `SET`.
            client
                .batch_execute(&format!("SET search_path TO {schema}"))
                .await?;
        }
        Ok(client)
    }

    /// Run one operation on a connected client, reconnecting a dead connection
    /// and dropping one an outage broke.
    async fn run<T>(
        &self,
        operation: impl for<'a> FnOnce(
            &'a mut Client,
        ) -> Pin<
            Box<dyn Future<Output = Result<T, CatalogStoreError>> + Send + 'a>,
        >,
    ) -> Result<T, CatalogStoreError> {
        let mut guard = self.client.lock().await;
        if guard.as_ref().is_none_or(Client::is_closed) {
            // Not classified the way boot's connect is: a long-lived replica
            // reconnects for the life of the process, and a rotation halfway
            // through or a pooler answering for a reloading backend produces
            // permanent-looking codes that clear on the next attempt. A refresh
            // is retried; refusing here would strand a deployment over a blip.
            *guard = Some(
                self.connect_client()
                    .await
                    .map_err(|error| outage("reconnect", &error))?,
            );
        }
        let result = tokio::time::timeout(
            self.settings.operation_timeout,
            operation(guard.as_mut().expect("connected")),
        )
        .await
        .map_err(|_| CatalogStoreError::unavailable(BACKEND, "operation timed out"))
        .and_then(|result| result);
        if matches!(result, Err(CatalogStoreError::Unavailable { .. })) {
            *guard = None;
        }
        result
    }
}

/// The snapshot columns, in one place so the statements and the row decoder
/// cannot disagree about their order.
const SNAPSHOT_COLUMNS: &str = "content_id, source_url, schema_version, raw_digest, raw_bytes, \
                                payload, fetched_at, etag, last_modified";

#[async_trait]
impl CatalogStore for PostgresCatalogStore {
    fn name(&self) -> &'static str {
        BACKEND
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::new(&[
            // Activation is one transaction, and retention conflicts on the
            // content identity rather than writing a second copy.
            Capability::TransactionalWrites,
            Capability::IdempotentWrites,
        ])
    }

    async fn load(&self) -> Result<StoredCatalogState, CatalogStoreError> {
        self.run(|client| {
            Box::pin(async move {
                let Some(active) = client
                    .query_opt(
                        "SELECT content_id, etag, last_modified, confirmed_at, \
                         consecutive_refusals, last_refusal FROM axond_catalog_active \
                         WHERE singleton",
                        &[],
                    )
                    .await
                    .map_err(|error| statement_failure("read the active catalogue", &error))?
                else {
                    return Ok(StoredCatalogState::default());
                };
                let consecutive_refusals: i64 = active.get(4);
                let state = StoredCatalogState {
                    active: None,
                    // The column is constrained non-negative, so a value that
                    // does not fit is a database someone else has been writing
                    // to; saturating keeps a report available rather than
                    // failing boot over a counter.
                    consecutive_refusals: u32::try_from(consecutive_refusals).unwrap_or(u32::MAX),
                    last_refusal: active
                        .get::<_, Option<String>>(5)
                        .map(|reason| decode_refusal(&reason)),
                };
                let Some(content_id) = active.get::<_, Option<String>>(0) else {
                    return Ok(state);
                };
                let content_id = decode_content_id(&content_id)?;
                let row = client
                    .query_opt(
                        &format!(
                            "SELECT {SNAPSHOT_COLUMNS} FROM axond_catalog_snapshot \
                             WHERE content_id = $1"
                        ),
                        &[&content_id.checksum().to_string()],
                    )
                    .await
                    .map_err(|error| statement_failure("read the active snapshot", &error))?
                    .ok_or_else(|| {
                        CatalogStoreError::corrupt(
                            BACKEND,
                            format!("active catalogue {content_id} is not retained"),
                        )
                    })?;
                let mut retained = decode_snapshot(&row)?;
                // The active row, not the import row, states what is currently
                // known: a `304` moved provenance without moving content.
                retained.source.validators = SourceValidators {
                    etag: active.get::<_, Option<String>>(1).map(ETag),
                    last_modified: active.get::<_, Option<String>>(2).map(HttpDate),
                };
                retained.source.fetched_at =
                    active.get::<_, Option<SystemTime>>(3).ok_or_else(|| {
                        CatalogStoreError::corrupt(
                            BACKEND,
                            format!("active catalogue {content_id} has no confirmation time"),
                        )
                    })?;
                Ok(StoredCatalogState {
                    active: Some(retained),
                    ..state
                })
            })
        })
        .await
    }

    async fn retained(
        &self,
        content_id: CatalogContentId,
    ) -> Result<Option<RetainedCatalog>, CatalogStoreError> {
        self.run(move |client| {
            Box::pin(async move {
                let row = client
                    .query_opt(
                        &format!(
                            "SELECT {SNAPSHOT_COLUMNS} FROM axond_catalog_snapshot \
                             WHERE content_id = $1"
                        ),
                        &[&content_id.checksum().to_string()],
                    )
                    .await
                    .map_err(|error| statement_failure("read a retained catalogue", &error))?;
                row.as_ref().map(decode_snapshot).transpose()
            })
        })
        .await
    }

    async fn activate(
        &self,
        import: &RetainedCatalog,
        activated_at: SystemTime,
    ) -> Result<Retention, CatalogStoreError> {
        let content_id = import.content_id().checksum().to_string();
        let raw_digest = import.source.raw.digest.to_string();
        let raw_bytes = i64::try_from(import.source.raw.size_bytes).map_err(|_| {
            CatalogStoreError::denied(
                BACKEND,
                format!(
                    "a {}-byte payload is not storable",
                    import.source.raw.size_bytes
                ),
            )
        })?;
        let etag = import
            .source
            .validators
            .etag
            .as_ref()
            .map(|tag| tag.0.clone());
        let last_modified = import
            .source
            .validators
            .last_modified
            .as_ref()
            .map(|date| date.0.clone());
        self.run(move |client| {
            let content_id = content_id.clone();
            let raw_digest = raw_digest.clone();
            let etag = etag.clone();
            let last_modified = last_modified.clone();
            let source_url = import.source.source_url.clone();
            let schema_version = import.source.schema_version.as_str();
            let payload = import.payload.as_bytes().to_vec();
            let fetched_at = import.source.fetched_at;
            Box::pin(async move {
                let transaction = client
                    .transaction()
                    .await
                    .map_err(|error| statement_failure("begin an activation", &error))?;
                // `DO NOTHING` is the idempotence: the same catalogue imported
                // twice is one row, and the second import is told so.
                let inserted = transaction
                    .execute(
                        &format!(
                            "INSERT INTO axond_catalog_snapshot ({SNAPSHOT_COLUMNS}) \
                             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
                             ON CONFLICT (content_id) DO NOTHING"
                        ),
                        &[
                            &content_id,
                            &source_url,
                            &schema_version,
                            &raw_digest,
                            &raw_bytes,
                            &payload,
                            &fetched_at,
                            &etag,
                            &last_modified,
                        ],
                    )
                    .await
                    .map_err(|error| statement_failure("retain a catalogue snapshot", &error))?;
                // Re-activating the content already active carries the stated
                // validators over the held ones, exactly as `confirm` does: a
                // full answer whose bytes are the active content is the `304`
                // case with a body, and an intermediary that stripped the
                // `ETag` must not cost the deployment its conditional request.
                // Genuinely new content replaces them, held value and all.
                transaction
                    .execute(
                        "INSERT INTO axond_catalog_active (singleton, content_id, etag, \
                         last_modified, confirmed_at, consecutive_refusals, last_refusal, \
                         last_refusal_at, updated_at) \
                         VALUES (true, $1, $2, $3, $4, 0, NULL, NULL, now()) \
                         ON CONFLICT (singleton) DO UPDATE SET content_id = EXCLUDED.content_id, \
                         etag = CASE WHEN axond_catalog_active.content_id = EXCLUDED.content_id \
                         THEN COALESCE(EXCLUDED.etag, axond_catalog_active.etag) \
                         ELSE EXCLUDED.etag END, \
                         last_modified = CASE WHEN axond_catalog_active.content_id = \
                         EXCLUDED.content_id THEN COALESCE(EXCLUDED.last_modified, \
                         axond_catalog_active.last_modified) ELSE EXCLUDED.last_modified END, \
                         confirmed_at = EXCLUDED.confirmed_at, consecutive_refusals = 0, \
                         last_refusal = NULL, last_refusal_at = NULL, updated_at = now()",
                        &[&content_id, &etag, &last_modified, &activated_at],
                    )
                    .await
                    .map_err(|error| statement_failure("activate a catalogue snapshot", &error))?;
                transaction
                    .commit()
                    .await
                    .map_err(|error| statement_failure("commit an activation", &error))?;
                Ok(if inserted == 1 {
                    Retention::Retained
                } else {
                    Retention::AlreadyRetained
                })
            })
        })
        .await
    }

    async fn confirm(
        &self,
        content_id: CatalogContentId,
        validators: &SourceValidators,
        confirmed_at: SystemTime,
    ) -> Result<bool, CatalogStoreError> {
        let etag = validators.etag.as_ref().map(|tag| tag.0.clone());
        let last_modified = validators.last_modified.as_ref().map(|date| date.0.clone());
        self.run(move |client| {
            let etag = etag.clone();
            let last_modified = last_modified.clone();
            Box::pin(async move {
                // `COALESCE` is `SourceValidators::carry_over` in SQL: a
                // validator the answer does not state leaves the held one alone,
                // because an intermediary that dropped it has not withdrawn it.
                let updated = client
                    .execute(
                        "UPDATE axond_catalog_active SET etag = COALESCE($2, etag), \
                         last_modified = COALESCE($3, last_modified), confirmed_at = $4, \
                         consecutive_refusals = 0, last_refusal = NULL, last_refusal_at = NULL, \
                         updated_at = now() WHERE singleton AND content_id = $1",
                        &[
                            &content_id.checksum().to_string(),
                            &etag,
                            &last_modified,
                            &confirmed_at,
                        ],
                    )
                    .await
                    .map_err(|error| statement_failure("confirm the active catalogue", &error))?;
                Ok(updated == 1)
            })
        })
        .await
    }

    async fn refuse(
        &self,
        reason: RefusalReason,
        refused_at: SystemTime,
    ) -> Result<(), CatalogStoreError> {
        self.run(move |client| {
            Box::pin(async move {
                // The insert is for the deployment whose very first refresh was
                // refused: there is a refusal run to record and no catalogue yet.
                client
                    .execute(
                        "INSERT INTO axond_catalog_active (singleton, consecutive_refusals, \
                         last_refusal, last_refusal_at, updated_at) \
                         VALUES (true, 1, $1, $2, now()) \
                         ON CONFLICT (singleton) DO UPDATE SET \
                         consecutive_refusals = axond_catalog_active.consecutive_refusals + 1, \
                         last_refusal = EXCLUDED.last_refusal, \
                         last_refusal_at = EXCLUDED.last_refusal_at, updated_at = now()",
                        &[&reason.as_str(), &refused_at],
                    )
                    .await
                    .map_err(|error| statement_failure("record a refused refresh", &error))?;
                Ok(())
            })
        })
        .await
    }
}

fn decode_content_id(text: &str) -> Result<CatalogContentId, CatalogStoreError> {
    Checksum::parse(text)
        .map(CatalogContentId::from_checksum)
        .map_err(|error| {
            CatalogStoreError::corrupt(BACKEND, format!("stored content id is unreadable: {error}"))
        })
}

/// A stored reason resolved back to the vocabulary, exhaustively over
/// [`RefusalReason::ALL`].
///
/// An unrecognised spelling — written by a newer release — becomes
/// [`RefusalReason::Unknown`] rather than a decode failure: a label this build
/// does not know is not a reason to refuse to report the catalogue's state.
fn decode_refusal(text: &str) -> RefusalReason {
    RefusalReason::ALL
        .iter()
        .copied()
        .find(|reason| reason.as_str() == text)
        .unwrap_or(RefusalReason::Unknown)
}

fn decode_snapshot(row: &Row) -> Result<RetainedCatalog, CatalogStoreError> {
    let content_id = decode_content_id(&row.get::<_, String>(0))?;
    let schema_version: String = row.get(2);
    // Resolved against the shapes this build parses rather than trusted: a row
    // written under a document shape this binary does not read must be a named
    // refusal at hydration, not a payload parsed as something it is not.
    let schema_version = [SchemaVersion::MODELS_DEV_CATALOG_V1]
        .into_iter()
        .find(|known| known.as_str() == schema_version)
        .ok_or_else(|| {
            CatalogStoreError::corrupt(
                BACKEND,
                format!(
                    "catalogue {content_id} was imported under unknown schema `{schema_version}`"
                ),
            )
        })?;
    let raw_bytes: i64 = row.get(4);
    let payload: Vec<u8> = row.get(5);
    Ok(RetainedCatalog {
        source: SourceSnapshot {
            source_url: row.get(1),
            schema_version,
            validators: SourceValidators {
                etag: row.get::<_, Option<String>>(7).map(ETag),
                last_modified: row.get::<_, Option<String>>(8).map(HttpDate),
            },
            fetched_at: row.get(6),
            raw: BlobRef {
                kind: BlobKind::CatalogSnapshot,
                digest: Checksum::parse(&row.get::<_, String>(3)).map_err(|error| {
                    CatalogStoreError::corrupt(
                        BACKEND,
                        format!("catalogue {content_id} has an unreadable payload digest: {error}"),
                    )
                })?,
                size_bytes: u64::try_from(raw_bytes).map_err(|_| {
                    CatalogStoreError::corrupt(
                        BACKEND,
                        format!("catalogue {content_id} states a negative payload length"),
                    )
                })?,
            },
            content_id,
        },
        payload: RawPayload::new(payload),
    })
}

/// A `SQLSTATE` an operator has to answer, rather than one a retry clears.
const fn operator_must_act(code: &SqlState) -> bool {
    matches!(
        *code,
        SqlState::INSUFFICIENT_PRIVILEGE
            | SqlState::UNDEFINED_TABLE
            | SqlState::UNDEFINED_COLUMN
            | SqlState::INVALID_SCHEMA_NAME
            | SqlState::INVALID_CATALOG_NAME
            | SqlState::INVALID_PASSWORD
            | SqlState::INVALID_AUTHORIZATION_SPECIFICATION
    )
}

/// A boot-time failure while doing `operation`, split by who has to act.
///
/// Everything without one of those codes is retryable: an error with no
/// `SQLSTATE` never reached a server, and a server can answer with a transient
/// code — starting up, out of connections, racing a sibling replica's
/// `CREATE TABLE IF NOT EXISTS` — that the next attempt clears.
fn boot_failure(
    operation: &str,
    error: &tokio_postgres::Error,
    remedy: impl FnOnce() -> String,
) -> CatalogStoreError {
    match error.code().filter(|code| operator_must_act(code)) {
        Some(code) => CatalogStoreError::denied(
            BACKEND,
            format!(
                "could not {operation} ({}: {error}). {}",
                code.code(),
                remedy()
            ),
        ),
        None => outage(operation, error),
    }
}

/// A statement failure while doing `operation`, split by who has to act.
///
/// Boot proves only that the two tables can be read, so a role granted
/// `SELECT` but not `INSERT`, or a table dropped after boot, first shows up
/// here. Reported as an outage it would be retried until someone read the log:
/// a privilege is not restored by waiting, and the connection would be
/// discarded and reopened on every attempt. The same `SQLSTATE`s that name an
/// operator at boot name one at runtime.
fn statement_failure(operation: &str, error: &tokio_postgres::Error) -> CatalogStoreError {
    match error.code().filter(|code| operator_must_act(code)) {
        Some(code) => CatalogStoreError::denied(
            BACKEND,
            format!(
                "could not {operation} ({}: {error}). Apply ops/postgres/catalog_v1.sql and grant \
                 the gateway role SELECT, INSERT and UPDATE on axond_catalog_snapshot and \
                 axond_catalog_active",
                code.code(),
            ),
        ),
        None => outage(operation, error),
    }
}

fn outage(operation: &str, error: &tokio_postgres::Error) -> CatalogStoreError {
    CatalogStoreError::unavailable(BACKEND, format!("{operation} failed: {error}"))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::super::hydrate;
    use super::*;
    use crate::backends::catalog::CatalogSnapshot;
    use crate::backends::models_dev::{SEED_PAYLOAD, seed_snapshot};
    use crate::test_services::postgres_dsn;

    fn at(seconds: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)
    }

    fn seed_import() -> RetainedCatalog {
        RetainedCatalog {
            source: seed_snapshot().source,
            payload: RawPayload::new(SEED_PAYLOAD.as_bytes()),
        }
    }

    /// A second import, distinct from the seed, without a second parser: the
    /// seed's own provenance over a payload one model shorter.
    fn trimmed_import() -> RetainedCatalog {
        let payload = SEED_PAYLOAD.replacen("\"reasoning\": true", "\"reasoning\": false", 1);
        assert_ne!(payload, SEED_PAYLOAD, "the fixture must actually change");
        let snapshot: CatalogSnapshot = crate::backends::models_dev::ModelsDevAdapter::default()
            .parse(
                payload.as_bytes(),
                SourceValidators::etag("\"trimmed\""),
                at(50),
            )
            .expect("the edited seed parses");
        RetainedCatalog {
            source: snapshot.source,
            payload: RawPayload::new(payload.as_bytes()),
        }
    }

    /// A store on its own schema, so tests are independent and leave nothing
    /// behind for the next run to trip over. `None` when no Postgres is
    /// configured, which skips the test.
    async fn store() -> Option<(PostgresCatalogStore, String)> {
        let dsn = postgres_dsn()?;
        let schema = format!(
            "axond_catalog_test_{}",
            crate::desired_state::Uuid7Generator::new()
                .next()
                .to_string()
                .replace('-', "")
        );
        let (client, connection) = tokio_postgres::connect(&dsn, crate::usage::tls_connector())
            .await
            .expect("connect to the test database");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .expect("create the test schema");
        let store = PostgresCatalogStore::connect(
            &dsn,
            CatalogStoreSettings {
                schema: Some(schema.clone()),
                ..CatalogStoreSettings::default()
            },
        )
        .await
        .expect("the store applies its own schema");
        Some((store, schema))
    }

    async fn drop_schema(schema: &str) {
        let Some(dsn) = postgres_dsn() else {
            return;
        };
        let (client, connection) = tokio_postgres::connect(&dsn, crate::usage::tls_connector())
            .await
            .expect("connect to the test database");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .await
            .expect("drop the test schema");
    }

    /// Boot proves only that the tables can be read, so a table dropped — or a
    /// privilege revoked — after boot first shows up on a write. Reported as an
    /// outage it would be retried until a human read the log; waiting does not
    /// restore a table.
    #[tokio::test]
    async fn a_table_that_disappeared_after_boot_names_the_operator_rather_than_an_outage() {
        let Some((store, schema)) = store().await else {
            return;
        };
        let dsn = postgres_dsn().expect("a store implies a dsn");
        let (client, connection) = tokio_postgres::connect(&dsn, crate::usage::tls_connector())
            .await
            .expect("connect to the test database");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!(
                "DROP TABLE {schema}.axond_catalog_snapshot, {schema}.axond_catalog_active"
            ))
            .await
            .expect("drop the tables out from under the store");

        let error = store
            .activate(&seed_import(), at(100))
            .await
            .expect_err("the table is gone");
        assert!(
            matches!(error, CatalogStoreError::Denied { .. }),
            "a missing table is an operator's to answer: {error}"
        );
        assert!(!crate::backends::BackendFailure::retryable(&error));
        drop_schema(&schema).await;
    }

    #[tokio::test]
    async fn an_import_survives_the_process_that_made_it() {
        let Some((store, schema)) = store().await else {
            return;
        };
        let import = seed_import();
        assert_eq!(
            store.activate(&import, at(100)).await.expect("activate"),
            Retention::Retained
        );

        let state = store.load().await.expect("load");
        let active = state.active.expect("an active catalogue");
        assert_eq!(active, {
            let mut expected = import.clone();
            expected.source.fetched_at = at(100);
            expected
        });
        // The whole point of retaining the bytes: the domain comes back without
        // the upstream.
        assert_eq!(
            hydrate(&active).expect("rehydrate").content,
            seed_snapshot().content
        );
        drop_schema(&schema).await;
    }

    #[tokio::test]
    async fn the_same_catalogue_imported_twice_is_stored_once() {
        let Some((store, schema)) = store().await else {
            return;
        };
        let import = seed_import();
        store.activate(&import, at(100)).await.expect("activate");
        assert_eq!(
            store.activate(&import, at(200)).await.expect("re-activate"),
            Retention::AlreadyRetained
        );

        let state = store.load().await.expect("load");
        assert_eq!(
            state.active.expect("active").source.fetched_at,
            at(200),
            "an unchanged re-import still moves the confirmation time"
        );
        drop_schema(&schema).await;
    }

    /// The `CASE`/`COALESCE` on the active pointer, proved where it runs: a
    /// full answer carrying the active content without an `ETag` keeps the tag
    /// the deployment holds, while genuinely new content replaces it.
    #[tokio::test]
    async fn re_activation_carries_a_validator_over_and_new_content_replaces_it() {
        let Some((store, schema)) = store().await else {
            return;
        };
        let mut import = seed_import();
        import.source.validators = SourceValidators::etag("\"held\"");
        store.activate(&import, at(100)).await.expect("activate");

        let mut stripped = import.clone();
        stripped.source.validators = SourceValidators::default();
        store
            .activate(&stripped, at(200))
            .await
            .expect("re-activate");
        assert_eq!(
            store
                .load()
                .await
                .expect("load")
                .active
                .expect("active")
                .source
                .validators,
            SourceValidators::etag("\"held\""),
            "an intermediary that stripped the tag must not cost the tag"
        );

        let mut newer = trimmed_import();
        newer.source.validators = SourceValidators::default();
        store.activate(&newer, at(300)).await.expect("activate");
        let active = store.load().await.expect("load").active.expect("active");
        assert_eq!(active.content_id(), newer.content_id());
        assert_eq!(
            active.source.validators,
            SourceValidators::default(),
            "a validator describes a document, so new content does not inherit one"
        );
        drop_schema(&schema).await;
    }

    /// A superseded catalogue stays exactly as it was imported, because a price
    /// book approved against it names it by identity.
    #[tokio::test]
    async fn a_superseded_snapshot_stays_retained_and_unchanged() {
        let Some((store, schema)) = store().await else {
            return;
        };
        let first = seed_import();
        let second = trimmed_import();
        assert_ne!(first.content_id(), second.content_id());
        store.activate(&first, at(100)).await.expect("activate");
        store.activate(&second, at(200)).await.expect("activate");

        let state = store.load().await.expect("load");
        assert_eq!(
            state.active.expect("active").content_id(),
            second.content_id()
        );
        let retained = store
            .retained(first.content_id())
            .await
            .expect("read")
            .expect("the superseded import is still there");
        assert_eq!(retained, first);
        assert_eq!(
            hydrate(&retained).expect("rehydrate").content.content_id(),
            first.content_id()
        );
        drop_schema(&schema).await;
    }

    #[tokio::test]
    async fn a_confirmation_moves_provenance_and_not_content() {
        let Some((store, schema)) = store().await else {
            return;
        };
        let import = seed_import();
        store.activate(&import, at(100)).await.expect("activate");

        assert!(
            store
                .confirm(import.content_id(), &SourceValidators::default(), at(700))
                .await
                .expect("confirm"),
            "the active catalogue was confirmed"
        );
        let state = store.load().await.expect("load");
        let active = state.active.expect("active");
        assert_eq!(active.source.fetched_at, at(700));
        assert_eq!(
            active.source.validators, import.source.validators,
            "an answer that states no validator leaves the held one alone"
        );

        store
            .confirm(
                import.content_id(),
                &SourceValidators::etag("\"next\""),
                at(800),
            )
            .await
            .expect("confirm");
        assert_eq!(
            store
                .load()
                .await
                .expect("load")
                .active
                .expect("active")
                .source
                .validators,
            SourceValidators::etag("\"next\"")
        );
        assert_eq!(
            store
                .retained(import.content_id())
                .await
                .expect("read")
                .expect("the import")
                .source
                .validators,
            import.source.validators,
            "the immutable import keeps what it arrived with"
        );
        drop_schema(&schema).await;
    }

    #[tokio::test]
    async fn confirming_content_that_is_not_active_records_nothing() {
        let Some((store, schema)) = store().await else {
            return;
        };
        store
            .activate(&seed_import(), at(100))
            .await
            .expect("activate");
        assert!(
            !store
                .confirm(
                    trimmed_import().content_id(),
                    &SourceValidators::etag("\"other\""),
                    at(200)
                )
                .await
                .expect("confirm"),
            "a 304 about content this deployment does not hold confirms nothing"
        );
        drop_schema(&schema).await;
    }

    #[tokio::test]
    async fn a_refusal_run_outlives_the_process_and_is_cleared_by_an_import() {
        let Some((store, schema)) = store().await else {
            return;
        };
        store
            .refuse(RefusalReason::Unreachable, at(10))
            .await
            .expect("refuse");
        store
            .refuse(RefusalReason::Unreachable, at(20))
            .await
            .expect("refuse");

        let state = store.load().await.expect("load");
        assert_eq!(state.consecutive_refusals, 2);
        assert_eq!(state.last_refusal, Some(RefusalReason::Unreachable));
        assert!(state.active.is_none());

        store
            .activate(&seed_import(), at(30))
            .await
            .expect("activate");
        let state = store.load().await.expect("load");
        assert_eq!(state.consecutive_refusals, 0);
        assert_eq!(state.last_refusal, None);
        assert!(state.active.is_some());
        drop_schema(&schema).await;
    }

    /// The database, not the gateway, is what makes a second active catalogue
    /// impossible.
    #[tokio::test]
    async fn the_active_pointer_is_one_row_by_construction() {
        let Some((store, schema)) = store().await else {
            return;
        };
        store
            .activate(&seed_import(), at(100))
            .await
            .expect("activate");
        store
            .activate(&trimmed_import(), at(200))
            .await
            .expect("activate");
        let rows: i64 = store
            .run(|client| {
                Box::pin(async move {
                    client
                        .query_one("SELECT count(*) FROM axond_catalog_active", &[])
                        .await
                        .map(|row| row.get(0))
                        .map_err(|error| statement_failure("count active rows", &error))
                })
            })
            .await
            .expect("count");
        assert_eq!(rows, 1);
        drop_schema(&schema).await;
    }

    #[test]
    fn a_reason_a_newer_release_wrote_reports_as_unknown_rather_than_failing() {
        assert_eq!(decode_refusal("not_retained"), RefusalReason::NotRetained);
        assert_eq!(
            decode_refusal("a-reason-from-the-future"),
            RefusalReason::Unknown
        );
    }
}
