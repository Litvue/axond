//! The durable control plane: PostgreSQL.
//!
//! The one thing this module has to get right is that a publication is a single
//! transaction. The head pointer is read `FOR UPDATE`, so concurrent publishers
//! serialize on one row rather than racing to append; the manifest, the resource
//! versions, the blob references, the mutation, the audit event, the idempotency
//! record, and the head advancement are written in that transaction and become
//! visible together. Anything that fails rolls the whole thing back, which is why
//! a rejected publication leaves no resource version and no audit event — not
//! "leaves them to be cleaned up".
//!
//! What it deliberately does not do:
//!
//! - **It is never on the inference path.** Nothing here is called while a
//!   request is in flight, so a Postgres outage stalls administration and
//!   convergence while replicas keep serving the immutable snapshot they already
//!   hold. That is why the failure of every method is an error returned to an
//!   administrative caller and never a mutation of anything a replica is serving.
//! - **It does not compile or publish a snapshot.** Hydration into runtime state
//!   is #166, publication to replicas is #142. This is the seam they read from:
//!   [`ControlPlaneStore::load_revision`] returns a [`LoadedRevision`], which
//!   cannot exist unless it verified.
//! - **It does not store payload bytes.** A blob is a reference — kind, digest,
//!   size — because a revision that inlined megabytes would duplicate them per
//!   revision. Where the payload lives, and who fetches and verifies it, belongs
//!   with hydration.
//! - **It does not store secret material.** A credential resource's body holds an
//!   opaque secret *reference*; plaintext lives in the secret store, and no value
//!   from a body is ever logged.

use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, Config, Row, Transaction};

use super::hydration::{self, HydrationLimits};
use super::rows;
use super::schema::{self, MINIMUM_SERVER_VERSION_NUM, SchemaStatus};
use super::{ControlPlaneError, ControlPlaneStore};
use crate::backends::{Capabilities, Capability};
use crate::desired_state::{
    AuditEvent, IntegrityError, LoadedRevision, Mutation, ResourceRef, ResourceVersion,
    ResourceVersionNumber, RevisionCandidate, RevisionId, RevisionManifest, SerializerVersion,
    Uuid7Generator,
};

const BACKEND: &str = "postgres";

/// How the store connects, and what it is allowed to do at boot.
#[derive(Debug, Clone)]
pub struct ControlPlaneSettings {
    /// The PostgreSQL schema the journal lives in, if not the connection's
    /// default. Validated as an identifier, because it is interpolated into
    /// `SET search_path`.
    pub schema: Option<String>,
    /// Whether boot may apply pending migrations.
    ///
    /// An operator who applies DDL out of band sets this to `false` and gets a
    /// refusal instead of a schema change; boot still *checks*, because serving
    /// against a schema that is not the one this build writes is the failure this
    /// setting exists to prevent.
    pub migrate: bool,
    pub connect_timeout: Duration,
    /// The ceiling on one control-plane operation. Generous by inference-path
    /// standards: nothing here is called with a request in flight.
    pub operation_timeout: Duration,
    /// How long an idempotency record is honoured. A retry window, not a
    /// permanent namespace: expiry never touches the revision or audit trail the
    /// record points at.
    pub idempotency_retention: Duration,
    /// What one read of a stored revision may consume. Reads are bounded because
    /// what they read is storage, and storage is not this build's to trust.
    pub hydration: HydrationLimits,
}

impl ControlPlaneSettings {
    /// The settings a `[control_plane]` section asks for.
    ///
    /// Every field the section can express is mapped here, so a setting an
    /// operator writes cannot be silently ignored by a code path that built its
    /// own defaults instead. The fields it cannot express — the idempotency
    /// retention window and the hydration limits — are runtime serving
    /// parameters rather than connection or schema ones, and keep their defaults.
    pub fn from_config(control_plane: &crate::config::ControlPlane) -> Self {
        Self {
            schema: control_plane
                .schema
                .as_deref()
                .map(str::trim)
                .filter(|schema| !schema.is_empty())
                .map(str::to_owned),
            migrate: control_plane.migrate,
            connect_timeout: Duration::from_millis(control_plane.connect_timeout_ms),
            operation_timeout: Duration::from_millis(control_plane.operation_timeout_ms),
            ..Self::default()
        }
    }

    /// The settings a maintenance command runs with: the operator's connection
    /// and schema, and never boot's permission to migrate. `axond migrate apply`
    /// is the mutation, and it is explicit; nothing else may become one because
    /// the config happened to allow boot migrations.
    pub fn for_maintenance(control_plane: &crate::config::ControlPlane) -> Self {
        Self {
            migrate: false,
            ..Self::from_config(control_plane)
        }
    }
}

impl Default for ControlPlaneSettings {
    fn default() -> Self {
        Self {
            schema: None,
            migrate: true,
            connect_timeout: Duration::from_secs(10),
            operation_timeout: Duration::from_secs(30),
            idempotency_retention: Duration::from_secs(24 * 60 * 60),
            hydration: HydrationLimits::default(),
        }
    }
}

/// A [`ControlPlaneStore`] backed by the revision journal in `ops/postgres/`.
pub struct PostgresControlPlane {
    config: Config,
    settings: ControlPlaneSettings,
    /// Set on every connection, including reconnections: a reconnect that landed
    /// on the default schema would silently read a different journal.
    search_path: Option<String>,
    ids: Uuid7Generator,
    client: tokio::sync::Mutex<Option<Client>>,
}

/// Written by hand, and deliberately narrow: a derived one would print the
/// [`Config`], which carries the password from the DSN.
impl std::fmt::Debug for PostgresControlPlane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresControlPlane")
            .field("schema", &self.search_path)
            .field("migrate", &self.settings.migrate)
            .finish_non_exhaustive()
    }
}

impl PostgresControlPlane {
    /// Connect, check the server and schema, and optionally migrate.
    ///
    /// Boot refuses rather than degrades. An unsupported server, a schema a newer
    /// build owns, a migration that was edited after being applied, or a pending
    /// migration this store may not apply are all [`ControlPlaneError::Denied`]:
    /// they need an operator, and retrying cannot help.
    pub async fn connect(
        dsn: &str,
        settings: ControlPlaneSettings,
    ) -> Result<Self, ControlPlaneError> {
        let (store, mut client) = Self::open(dsn, settings).await?;
        store.prepare_schema(&mut client).await?;
        *store.client.lock().await = Some(client);
        Ok(store)
    }

    /// Connect for schema maintenance, without deciding anything about the schema.
    ///
    /// [`connect`](Self::connect) is a boot: it refuses a schema it cannot serve.
    /// A `status` command has to *report* that schema instead, and an `apply` has
    /// to be able to run against the database a boot just refused, so this checks
    /// the server version — a statement no migration can survive an older server
    /// running — and stops there. The store it returns is not for serving: it has
    /// not established that the journal is the one this build writes.
    pub async fn connect_for_maintenance(
        dsn: &str,
        settings: ControlPlaneSettings,
    ) -> Result<Self, ControlPlaneError> {
        let (store, client) = Self::open(dsn, settings).await?;
        *store.client.lock().await = Some(client);
        Ok(store)
    }

    /// Connect and check the server, leaving the schema decision to the caller.
    async fn open(
        dsn: &str,
        settings: ControlPlaneSettings,
    ) -> Result<(Self, Client), ControlPlaneError> {
        let mut config: Config = dsn.parse().map_err(|error| {
            denied(format!(
                // The DSN itself is never echoed: it carries a password.
                "the control-plane DSN could not be parsed: {error}"
            ))
        })?;
        config.connect_timeout(settings.connect_timeout);
        config.application_name(crate::telemetry::SERVICE_NAME);
        let search_path = settings
            .schema
            .as_deref()
            .map(|schema| {
                crate::usage::validate_table_name(schema).map_err(denied)?;
                // The table-name validator allows one qualifying dot; a search path
                // takes one unqualified schema. Checked here as well as in config
                // validation, because settings can be built without a config file.
                if schema.contains('.') {
                    return Err(denied(format!(
                        "`{schema}` is not a single unqualified schema name"
                    )));
                }
                Ok(schema.to_owned())
            })
            .transpose()?;

        let store = Self {
            config,
            settings,
            search_path,
            ids: Uuid7Generator::new(),
            client: tokio::sync::Mutex::new(None),
        };
        let client = tokio::time::timeout(store.settings.connect_timeout, store.connect_client())
            .await
            .map_err(|_| ControlPlaneError::Unavailable {
                backend: BACKEND,
                message: "connection timed out".to_owned(),
            })?
            .map_err(|error| unavailable("connect", &error))?;
        store.check_server_version(&client).await?;
        Ok((store, client))
    }

    /// Apply every migration the journal is missing, and report which ones.
    ///
    /// Forward-only and idempotent: a journal that is already current is left
    /// untouched and reports nothing applied, so re-running an `apply` is not a
    /// second migration. A journal this build cannot own — [`SchemaStatus::Ahead`]
    /// or [`SchemaStatus::Drifted`] — is refused with [`ControlPlaneError::Denied`]
    /// rather than written to, because writing more DDL over it makes the
    /// disagreement worse rather than better.
    ///
    /// The whole read-and-apply is one transaction under the same advisory lock
    /// boot takes, so this is safe to run while replicas are starting.
    pub async fn apply_migrations(&self) -> Result<Vec<i32>, ControlPlaneError> {
        self.run(|client| {
            Box::pin(async move {
                // Read committed explicitly rather than by inheritance: the status
                // is read *after* the lock is granted, and it has to be the winner's
                // committed history rather than a snapshot taken before the wait. A
                // server with `default_transaction_isolation = 'repeatable read'`
                // would freeze the snapshot at the first statement, so the loser of
                // the lock would re-read a pre-migration ledger and apply the same
                // files again.
                let transaction = client
                    .build_transaction()
                    .isolation_level(tokio_postgres::IsolationLevel::ReadCommitted)
                    .start()
                    .await
                    .map_err(|error| unavailable("begin schema transaction", &error))?;
                transaction
                    .query_one("SELECT pg_advisory_xact_lock($1::bigint)", &[&SCHEMA_LOCK])
                    .await
                    .map_err(|error| unavailable("acquire schema lock", &error))?;
                let status = schema::status(&transaction)
                    .await
                    .map_err(|error| unavailable("read schema status", &error))?;
                // A database that is already current is left alone rather than
                // handed to the runner: `migrate` starts from the recorded prefix
                // of a `Behind` status, so a `Current` one would re-run every
                // shipped file from v1. That happens to be harmless while every
                // statement is `IF NOT EXISTS`, and would not survive the first
                // `ALTER TABLE` or backfill. Returning here also rolls the
                // transaction back, so a repeated apply writes nothing at all.
                if status.is_current() {
                    return Ok(Vec::new());
                }
                if !status.is_migratable() {
                    return Err(denied(status.to_string()));
                }
                let pending = schema::pending(&status);
                schema::migrate(&transaction, &status)
                    .await
                    .map_err(|error| migration_refused_or_unavailable(&error))?;
                let migrated = schema::status(&transaction)
                    .await
                    .map_err(|error| unavailable("re-read schema status", &error))?;
                if !migrated.is_current() {
                    return Err(denied(format!(
                        "migrations were applied but the schema is still not current: {migrated}"
                    )));
                }
                transaction
                    .commit()
                    .await
                    .map_err(|error| unavailable("commit schema transaction", &error))?;
                Ok(pending)
            })
        })
        .await
    }

    /// The schema state a database is in, for a status command or a boot refusal.
    ///
    /// Read-only as the *database* enforces it, not just by convention: the
    /// transaction is opened `READ ONLY`, so a statement that tried to write
    /// would be rejected by the server rather than by this code being careful.
    /// A status command an operator runs against production has to be a thing
    /// that cannot change production.
    pub async fn schema_status(&self) -> Result<SchemaStatus, ControlPlaneError> {
        self.run(|client| {
            Box::pin(async move {
                let transaction = client
                    .build_transaction()
                    .read_only(true)
                    .start()
                    .await
                    .map_err(|error| unavailable("begin read-only schema read", &error))?;
                let status = schema::status(&transaction)
                    .await
                    .map_err(|error| unavailable("read schema status", &error))?;
                // Rolled back rather than committed: nothing here is a change,
                // so nothing here needs to be kept.
                let _ = transaction.rollback().await;
                Ok(status)
            })
        })
        .await
    }

    async fn check_server_version(&self, client: &Client) -> Result<(), ControlPlaneError> {
        let reported: String = client
            .query_one("SELECT current_setting('server_version_num')", &[])
            .await
            .map_err(|error| unavailable("read server version", &error))?
            .get(0);
        let version: i32 = reported.parse().map_err(|_| {
            denied(format!(
                "the server reported version `{reported}`, which is not a number"
            ))
        })?;
        if version < MINIMUM_SERVER_VERSION_NUM {
            return Err(denied(format!(
                "the control-plane journal requires PostgreSQL {}, but the server is {}",
                MINIMUM_SERVER_VERSION_NUM / 10_000,
                version / 10_000
            )));
        }
        Ok(())
    }

    /// Bring the schema to the version this build requires, or refuse.
    ///
    /// The whole check-and-migrate runs under one advisory lock inside one
    /// transaction, so two gateways booting against the same empty database
    /// serialize here instead of both running the DDL and one of them failing
    /// halfway.
    async fn prepare_schema(&self, client: &mut Client) -> Result<(), ControlPlaneError> {
        let transaction = client
            .transaction()
            .await
            .map_err(|error| unavailable("begin schema transaction", &error))?;
        transaction
            .query_one("SELECT pg_advisory_xact_lock($1::bigint)", &[&SCHEMA_LOCK])
            .await
            .map_err(|error| unavailable("acquire schema lock", &error))?;
        let status = schema::status(&transaction)
            .await
            .map_err(|error| unavailable("read schema status", &error))?;
        if !status.is_current() {
            if !status.is_migratable() {
                return Err(denied(status.to_string()));
            }
            if !self.settings.migrate {
                return Err(denied(format!(
                    "{status}, and this store is configured not to migrate"
                )));
            }
            schema::migrate(&transaction, &status)
                .await
                .map_err(|error| unavailable("apply migrations", &error))?;
            let migrated = schema::status(&transaction)
                .await
                .map_err(|error| unavailable("re-read schema status", &error))?;
            if !migrated.is_current() {
                return Err(denied(format!(
                    "migrations were applied but the schema is still not current: {migrated}"
                )));
            }
        }
        transaction
            .commit()
            .await
            .map_err(|error| unavailable("commit schema transaction", &error))?;
        Ok(())
    }

    async fn connect_client(&self) -> Result<Client, tokio_postgres::Error> {
        let (client, connection) = self.config.connect(crate::usage::tls_connector()).await?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::warn!(%error, "postgres control-plane connection closed");
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
    ///
    /// A failure that is *not* an outage — a conflict, an invalid candidate,
    /// unreadable storage — keeps the connection: it says nothing about the
    /// connection's health, and discarding it would turn every refused write into
    /// a reconnect.
    async fn run<T>(
        &self,
        operation: impl for<'a> FnOnce(
            &'a mut Client,
        ) -> Pin<
            Box<dyn Future<Output = Result<T, ControlPlaneError>> + Send + 'a>,
        >,
    ) -> Result<T, ControlPlaneError> {
        let mut guard = self.client.lock().await;
        if guard.as_ref().is_none_or(Client::is_closed) {
            *guard = Some(
                self.connect_client()
                    .await
                    .map_err(|error| unavailable("reconnect", &error))?,
            );
        }
        let result = tokio::time::timeout(
            self.settings.operation_timeout,
            operation(guard.as_mut().expect("connected")),
        )
        .await
        .map_err(|_| ControlPlaneError::Unavailable {
            backend: BACKEND,
            message: "operation timed out".to_owned(),
        })
        .and_then(|result| result);
        if matches!(result, Err(ControlPlaneError::Unavailable { .. })) {
            *guard = None;
        }
        result
    }
}

/// The advisory lock every gateway takes before touching the journal's schema.
/// A constant, because the journal's object names are constants.
const SCHEMA_LOCK: i64 = 0x1a20_de5c_0de5_1a11u64 as i64;

fn denied(message: impl Into<String>) -> ControlPlaneError {
    ControlPlaneError::Denied {
        backend: BACKEND,
        message: message.into(),
    }
}

/// Classify a failure to run the migration DDL by whether a retry could clear it.
///
/// The realistic failures here are the server rejecting a statement, not the
/// server being unreachable: `3F000` when the configured `[control_plane] schema`
/// does not exist (`SET search_path` accepts a missing schema, so the refusal
/// arrives at the first `CREATE TABLE`) and `42501` when the role may not create
/// objects. Both need an operator, and reporting them as an outage would tell a
/// rollout gate to retry a thing that cannot start working.
///
/// The transient classes stay outages so they keep their retryable
/// classification: connection exceptions (08), transaction rollbacks including
/// serialization failures and deadlocks (40), insufficient resources (53), an
/// object not in a prerequisite state such as an unavailable lock (55), operator
/// intervention including a cancelled statement or a shutdown (57), and system
/// errors (58). An error with no SQLSTATE never reached the server at all.
fn migration_refused_or_unavailable(error: &tokio_postgres::Error) -> ControlPlaneError {
    const TRANSIENT: [&str; 6] = ["08", "40", "53", "55", "57", "58"];
    let Some(db) = error.as_db_error() else {
        return unavailable("apply migrations", error);
    };
    let code = db.code().code();
    if code
        .get(..2)
        .is_some_and(|class| TRANSIENT.contains(&class))
    {
        return unavailable("apply migrations", error);
    }
    denied(format!(
        "applying migrations failed: {} (SQLSTATE {code}); the server rejected the statement, so \
         no retry clears it — check that the configured schema exists and that the role may \
         create objects in it",
        db.message()
    ))
}

/// Report a Postgres failure as an outage, naming the operation and SQLSTATE.
///
/// SQLSTATE is included because "connection reset" and "deadlock detected" are
/// the same category to a caller and completely different to an operator.
pub(super) fn unavailable(operation: &str, error: &tokio_postgres::Error) -> ControlPlaneError {
    let message = match error.as_db_error() {
        Some(db) => format!(
            "{operation} failed: {} (SQLSTATE {})",
            db.message(),
            db.code().code()
        ),
        None => format!("{operation} failed: {error}"),
    };
    ControlPlaneError::Unavailable {
        backend: BACKEND,
        message,
    }
}

pub(super) fn corrupt_storage(detail: impl Into<String>) -> ControlPlaneError {
    ControlPlaneError::CorruptStorage {
        detail: detail.into(),
    }
}

/// A timestamp the journal can store and read back unchanged.
///
/// `timestamptz` holds microseconds, so a nanosecond-precision instant would come
/// back a different value than it went in — and a manifest that does not equal the
/// one publication returned is indistinguishable from a corrupt one.
fn journal_now() -> SystemTime {
    let now = SystemTime::now();
    match now.duration_since(UNIX_EPOCH) {
        Ok(since) => UNIX_EPOCH + Duration::from_micros(since.as_micros() as u64),
        Err(_) => now,
    }
}

fn version_text(value: ResourceVersionNumber) -> i64 {
    // A version number is a small counter; the cast cannot lose a real one.
    i64::try_from(value.get()).unwrap_or(i64::MAX)
}

fn size_bytes(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[async_trait]
impl ControlPlaneStore for PostgresControlPlane {
    fn name(&self) -> &'static str {
        BACKEND
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::new(&[
            Capability::TransactionalWrites,
            Capability::OptimisticConcurrency,
            Capability::IdempotentWrites,
            Capability::TransactionalAudit,
        ])
    }

    async fn health(&self) -> Result<(), ControlPlaneError> {
        self.run(|client| {
            Box::pin(async move {
                client
                    .query_one("SELECT 1", &[])
                    .await
                    .map(|_| ())
                    .map_err(|error| unavailable("health check", &error))
            })
        })
        .await
    }

    async fn desired_revision(&self) -> Result<Option<RevisionId>, ControlPlaneError> {
        self.run(|client| {
            Box::pin(async move {
                let row = client
                    .query_opt(
                        "SELECT revision_id FROM axond_cp_head WHERE singleton",
                        &[],
                    )
                    .await
                    .map_err(|error| unavailable("read desired revision", &error))?;
                let Some(row) = row else {
                    return Err(corrupt_storage(
                        "the control-plane head row is missing; the schema was modified out of band",
                    ));
                };
                let id: Option<String> = row.get(0);
                id.map(|text| {
                    rows::revision_id(&text).map_err(|error| {
                        corrupt_storage(format!("the desired revision is unreadable: {error}"))
                    })
                })
                .transpose()
            })
        })
        .await
    }

    async fn load_manifest(&self, id: RevisionId) -> Result<RevisionManifest, ControlPlaneError> {
        let limits = self.settings.hydration;
        self.run(move |client| {
            Box::pin(async move {
                let transaction = client
                    .transaction()
                    .await
                    .map_err(|error| unavailable("begin manifest read", &error))?;
                let manifest = hydration::manifest(&transaction, id, &limits).await;
                let _ = transaction.rollback().await;
                manifest
            })
        })
        .await
    }

    async fn load_revision(&self, id: RevisionId) -> Result<LoadedRevision, ControlPlaneError> {
        let limits = self.settings.hydration;
        self.run(move |client| {
            Box::pin(async move {
                let transaction = client
                    .transaction()
                    .await
                    .map_err(|error| unavailable("begin revision read", &error))?;
                let loaded = hydration::revision(&transaction, id, &limits).await;
                let _ = transaction.rollback().await;
                loaded
            })
        })
        .await
    }

    /// The head and its hydration in one read, so a caller cannot be told it
    /// converged onto a revision that was never the head it read.
    async fn load_desired_revision(&self) -> Result<Option<LoadedRevision>, ControlPlaneError> {
        let limits = self.settings.hydration;
        self.run(move |client| {
            Box::pin(async move {
                let transaction = client
                    .transaction()
                    .await
                    .map_err(|error| unavailable("begin desired-revision read", &error))?;
                let loaded = hydration::desired(&transaction, &limits).await;
                let _ = transaction.rollback().await;
                loaded
            })
        })
        .await
    }

    async fn publish_revision(
        &self,
        candidate: RevisionCandidate,
    ) -> Result<RevisionManifest, ControlPlaneError> {
        // Validation is domain work, and it happens before the store commits to
        // anything: a rejected candidate leaves no transaction, no row, and no
        // audit event.
        let checksum = candidate.validated_checksum()?;
        let caller_scope = rows::caller_scope(&candidate.mutation.actor)
            .map_err(|error| ControlPlaneError::Invalid(error.into()))?;
        let retention = self.settings.idempotency_retention;
        let limits = self.settings.hydration;
        let id = RevisionId::new(self.ids.next());

        self.run(move |client| {
            Box::pin(async move {
                let transaction = client
                    .transaction()
                    .await
                    .map_err(|error| unavailable("begin publication", &error))?;
                match publish(
                    &transaction,
                    id,
                    &candidate,
                    &checksum.to_string(),
                    &caller_scope,
                    retention,
                    &limits,
                )
                .await
                {
                    Ok(Published::Manifest(manifest)) => {
                        transaction
                            .commit()
                            .await
                            .map_err(|error| unavailable("commit publication", &error))?;
                        Ok(manifest)
                    }
                    Ok(Published::Replayed(manifest)) => {
                        // Nothing was written except the pruning of expired
                        // records, which is worth keeping.
                        transaction
                            .commit()
                            .await
                            .map_err(|error| unavailable("commit replay", &error))?;
                        Ok(manifest)
                    }
                    Err(error) => {
                        // Explicit, so "rollback leaves nothing behind" is a
                        // statement about this code and not about a `Drop`.
                        let _ = transaction.rollback().await;
                        Err(error)
                    }
                }
            })
        })
        .await
    }

    async fn audit_trail(&self, id: RevisionId) -> Result<Vec<AuditEvent>, ControlPlaneError> {
        self.run(move |client| {
            Box::pin(async move {
                let revision = client
                    .query_opt(
                        "SELECT 1 FROM axond_cp_revision WHERE revision_id = $1",
                        &[&id.to_string()],
                    )
                    .await
                    .map_err(|error| unavailable("read revision", &error))?;
                if revision.is_none() {
                    return Err(ControlPlaneError::RevisionNotFound(id));
                }
                let rows = client
                    .query(
                        "SELECT audit_event_id, mutation_id, actor_kind, actor_issuer, \
                         actor_subject, actor_component, event_kind, target_kind, target_id, \
                         target_version, summary, recorded_at \
                         FROM axond_cp_audit_event WHERE revision_id = $1 \
                         ORDER BY recorded_at DESC, audit_event_id DESC",
                        &[&id.to_string()],
                    )
                    .await
                    .map_err(|error| unavailable("read audit trail", &error))?;
                rows.iter()
                    .map(|row| {
                        audit_event(row).map_err(|error| ControlPlaneError::corrupt(id, error))
                    })
                    .collect()
            })
        })
        .await
    }
}

/// Whether a publication wrote a revision or replayed one.
///
/// Named rather than a bare manifest so the commit path cannot accidentally treat
/// a replay as a write.
enum Published {
    Manifest(RevisionManifest),
    Replayed(RevisionManifest),
}

/// The whole publication, inside the caller's transaction.
///
/// The order is the contract's order, and each step is a refusal rather than a
/// repair: take the head lock, prune and consult the caller's retry window, check
/// the expectation, check version immutability, then write.
async fn publish(
    transaction: &Transaction<'_>,
    id: RevisionId,
    candidate: &RevisionCandidate,
    checksum: &str,
    caller_scope: &str,
    retention: Duration,
    limits: &HydrationLimits,
) -> Result<Published, ControlPlaneError> {
    // One row, taken `FOR UPDATE`: every publisher queues here, so "exactly one
    // expected-revision commit wins" does not depend on isolation-level
    // subtleties or on retrying a serialization failure.
    let head = transaction
        .query_opt(
            "SELECT revision_id FROM axond_cp_head WHERE singleton FOR UPDATE",
            &[],
        )
        .await
        .map_err(|error| unavailable("lock the head", &error))?
        .ok_or_else(|| {
            corrupt_storage("the control-plane head row is missing; publication has no anchor")
        })?;
    let head: Option<String> = head.get(0);
    let head = head
        .map(|text| {
            rows::revision_id(&text).map_err(|error| {
                corrupt_storage(format!("the desired revision is unreadable: {error}"))
            })
        })
        .transpose()?;

    // Expiry is a retry window closing, not a garbage-collection job an operator
    // has to run: the window is pruned on the write path that depends on it.
    transaction
        .execute(
            "DELETE FROM axond_cp_idempotency WHERE expires_at <= now()",
            &[],
        )
        .await
        .map_err(|error| unavailable("prune expired idempotency records", &error))?;

    let key = candidate.mutation.idempotency_key.as_str().to_owned();
    // Replay is consulted *before* the expectation: a retry of a candidate that
    // has since gone stale must replay its own outcome, not be told it conflicts
    // with the revision it published.
    if let Some(record) = transaction
        .query_opt(
            "SELECT state_checksum, revision_id FROM axond_cp_idempotency \
             WHERE caller_scope = $1 AND idempotency_key = $2",
            &[&caller_scope, &key],
        )
        .await
        .map_err(|error| unavailable("read idempotency record", &error))?
    {
        let recorded_checksum: String = record.get(0);
        let recorded_revision: String = record.get(1);
        let published = rows::revision_id(&recorded_revision).map_err(|error| {
            corrupt_storage(format!("an idempotency record is unreadable: {error}"))
        })?;
        if recorded_checksum != checksum {
            return Err(ControlPlaneError::IdempotencyKeyReused {
                key: candidate.mutation.idempotency_key.clone(),
                published,
            });
        }
        return hydration::manifest(transaction, published, limits)
            .await
            .map(Published::Replayed);
    }

    if !candidate.expected.matches(head) {
        return Err(ControlPlaneError::Conflict {
            expected: candidate.expected,
            actual: head,
        });
    }

    for resource in candidate.state.resources() {
        assert_version_is_immutable(transaction, resource).await?;
    }

    let manifest = RevisionManifest::of(id, head, journal_now(), candidate)?;

    for blob in candidate.state.blobs() {
        transaction
            .execute(
                "INSERT INTO axond_cp_blob (blob_kind, digest, size_bytes) VALUES ($1, $2, $3) \
                 ON CONFLICT (blob_kind, digest) DO NOTHING",
                &[
                    &blob.kind.as_str(),
                    &blob.digest.to_string(),
                    &size_bytes(blob.size_bytes),
                ],
            )
            .await
            .map_err(|error| unavailable("write blob reference", &error))?;
    }

    for resource in candidate.state.resources() {
        insert_resource_version(transaction, resource).await?;
    }

    insert_mutation(transaction, &candidate.mutation).await?;

    transaction
        .execute(
            "INSERT INTO axond_cp_revision \
             (revision_id, parent_id, mutation_id, serializer, state_checksum, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6)",
            &[
                &id.to_string(),
                &manifest.parent.map(|parent| parent.to_string()),
                &manifest.mutation.to_string(),
                &manifest.serializer.as_str(),
                &checksum,
                &manifest.created_at,
            ],
        )
        .await
        .map_err(|error| {
            // The head lock makes this unreachable through this code. It stays
            // mapped as a conflict because the constraint it violates — one child
            // per parent — means exactly that: someone else already published
            // against this parent.
            if is_unique_violation(&error) {
                ControlPlaneError::Conflict {
                    expected: candidate.expected,
                    actual: head,
                }
            } else {
                unavailable("write revision", &error)
            }
        })?;

    for entry in &manifest.entries {
        transaction
            .execute(
                "INSERT INTO axond_cp_revision_entry \
                 (revision_id, resource_kind, resource_id, version) VALUES ($1, $2, $3, $4)",
                &[
                    &id.to_string(),
                    &entry.reference.kind.as_str(),
                    &entry.reference.id.to_string(),
                    &version_text(entry.reference.version),
                ],
            )
            .await
            .map_err(|error| unavailable("write manifest entry", &error))?;
    }

    for blob in &manifest.blobs {
        transaction
            .execute(
                "INSERT INTO axond_cp_revision_blob (revision_id, blob_kind, digest) \
                 VALUES ($1, $2, $3)",
                &[
                    &id.to_string(),
                    &blob.kind.as_str(),
                    &blob.digest.to_string(),
                ],
            )
            .await
            .map_err(|error| unavailable("write revision blob", &error))?;
    }

    insert_audit_event(transaction, id, &candidate.audit).await?;

    let expires_at = journal_now() + retention;
    let replaced = transaction
        .execute(
            "INSERT INTO axond_cp_idempotency \
             (caller_scope, idempotency_key, state_checksum, revision_id, mutation_id, expires_at) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (caller_scope, idempotency_key) DO UPDATE SET \
             state_checksum = EXCLUDED.state_checksum, revision_id = EXCLUDED.revision_id, \
             mutation_id = EXCLUDED.mutation_id, recorded_at = now(), \
             expires_at = EXCLUDED.expires_at",
            &[
                &caller_scope,
                &key,
                &checksum,
                &id.to_string(),
                &candidate.mutation.id.to_string(),
                &expires_at,
            ],
        )
        .await
        .map_err(|error| unavailable("write idempotency record", &error))?;
    if replaced != 1 {
        return Err(corrupt_storage(
            "the idempotency record for this caller and key was neither written nor replayed",
        ));
    }

    transaction
        .execute(
            "UPDATE axond_cp_head SET revision_id = $1, updated_at = now() WHERE singleton",
            &[&id.to_string()],
        )
        .await
        .map_err(|error| unavailable("advance the head", &error))?;

    Ok(Published::Manifest(manifest))
}

/// Refuse a republication that would redefine a version an earlier revision pins.
async fn assert_version_is_immutable(
    transaction: &Transaction<'_>,
    resource: &ResourceVersion,
) -> Result<(), ControlPlaneError> {
    let stored = transaction
        .query_opt(
            "SELECT content_checksum FROM axond_cp_resource_version \
             WHERE resource_kind = $1 AND resource_id = $2 AND version = $3",
            &[
                &resource.reference.kind.as_str(),
                &resource.reference.id.to_string(),
                &version_text(resource.reference.version),
            ],
        )
        .await
        .map_err(|error| unavailable("read resource version", &error))?;
    let Some(stored) = stored else {
        return Ok(());
    };
    let stored: String = stored.get(0);
    let candidate = resource
        .content_checksum()
        .map_err(|error| ControlPlaneError::Invalid(error.into()))?;
    if stored != candidate.to_string() {
        return Err(ControlPlaneError::ImmutableResourceVersion {
            reference: resource.reference,
        });
    }
    Ok(())
}

async fn insert_resource_version(
    transaction: &Transaction<'_>,
    resource: &ResourceVersion,
) -> Result<(), ControlPlaneError> {
    let scope = rows::scope_columns(&resource.scope);
    let body = rows::body_columns(&resource.body).map_err(|error| {
        corrupt_storage(format!(
            "{} could not be encoded for storage: {error}",
            resource.reference
        ))
    })?;
    let checksum = resource
        .content_checksum()
        .map_err(|error| ControlPlaneError::Invalid(error.into()))?;
    let kind = resource.reference.kind.as_str();
    let id = resource.reference.id.to_string();
    let version = version_text(resource.reference.version);
    let slug = resource.slug.as_str();
    let checksum = checksum.to_string();
    let serializer = SerializerVersion::default().as_str();
    let parameters: [&(dyn ToSql + Sync); 13] = [
        &kind,
        &id,
        &version,
        &scope.kind,
        &scope.tenant,
        &scope.project,
        &slug,
        &body.form,
        &body.inline,
        &body.blob_kind,
        &body.blob_digest,
        &checksum,
        &serializer,
    ];
    transaction
        .execute(
            "INSERT INTO axond_cp_resource_version \
             (resource_kind, resource_id, version, scope_kind, tenant_id, project_id, slug, \
             body_form, body_inline, body_blob_kind, body_blob_digest, content_checksum, \
             serializer) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) \
             ON CONFLICT (resource_kind, resource_id, version) DO NOTHING",
            &parameters,
        )
        .await
        .map_err(|error| unavailable("write resource version", &error))?;

    for dependency in &resource.depends_on {
        transaction
            .execute(
                "INSERT INTO axond_cp_resource_dependency \
                 (resource_kind, resource_id, version, depends_on_kind, depends_on_id, \
                 depends_on_version) VALUES ($1, $2, $3, $4, $5, $6) \
                 ON CONFLICT DO NOTHING",
                &[
                    &kind,
                    &id,
                    &version,
                    &dependency.kind.as_str(),
                    &dependency.id.to_string(),
                    &version_text(dependency.version),
                ],
            )
            .await
            .map_err(|error| unavailable("write resource dependency", &error))?;
    }
    Ok(())
}

async fn insert_mutation(
    transaction: &Transaction<'_>,
    mutation: &Mutation,
) -> Result<(), ControlPlaneError> {
    let actor = rows::actor_columns(&mutation.actor);
    let scope = rows::scope_columns(&mutation.scope);
    transaction
        .execute(
            "INSERT INTO axond_cp_mutation \
             (mutation_id, actor_kind, actor_issuer, actor_subject, actor_component, \
             mutation_kind, scope_kind, tenant_id, project_id, idempotency_key, submitted_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            &[
                &mutation.id.to_string(),
                &actor.kind,
                &actor.issuer,
                &actor.subject,
                &actor.component,
                &mutation.kind.as_str(),
                &scope.kind,
                &scope.tenant,
                &scope.project,
                &mutation.idempotency_key.as_str(),
                &mutation.submitted_at,
            ],
        )
        .await
        .map_err(|error| {
            if is_unique_violation(&error) {
                // A mutation id is one administrative change. Reusing one for a
                // second change is refused rather than merged into the first:
                // the audit trail's "which change was this?" must have one
                // answer.
                denied(format!(
                    "mutation {} is already recorded; a mutation id names one change",
                    mutation.id
                ))
            } else {
                unavailable("write mutation", &error)
            }
        })?;
    Ok(())
}

async fn insert_audit_event(
    transaction: &Transaction<'_>,
    revision: RevisionId,
    audit: &AuditEvent,
) -> Result<(), ControlPlaneError> {
    let actor = rows::actor_columns(&audit.actor);
    let target = audit.target;
    transaction
        .execute(
            "INSERT INTO axond_cp_audit_event \
             (audit_event_id, revision_id, mutation_id, actor_kind, actor_issuer, actor_subject, \
             actor_component, event_kind, target_kind, target_id, target_version, summary, \
             recorded_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
            &[
                &audit.id.to_string(),
                &revision.to_string(),
                &audit.mutation.to_string(),
                &actor.kind,
                &actor.issuer,
                &actor.subject,
                &actor.component,
                &audit.kind.as_str(),
                &target.map(|target| target.kind.as_str()),
                &target.map(|target| target.id.to_string()),
                &target.map(|target| version_text(target.version)),
                &audit.summary,
                &audit.recorded_at,
            ],
        )
        .await
        .map_err(|error| {
            if is_unique_violation(&error) {
                denied(format!(
                    "audit event {} is already recorded; an audit event is written once",
                    audit.id
                ))
            } else {
                unavailable("write audit event", &error)
            }
        })?;
    Ok(())
}

fn is_unique_violation(error: &tokio_postgres::Error) -> bool {
    error
        .as_db_error()
        .is_some_and(|db| db.code() == &SqlState::UNIQUE_VIOLATION)
}

fn audit_event(row: &Row) -> Result<AuditEvent, IntegrityError> {
    let id: String = row.get(0);
    let mutation: String = row.get(1);
    let actor_kind: String = row.get(2);
    let issuer: Option<String> = row.get(3);
    let subject: Option<String> = row.get(4);
    let component: Option<String> = row.get(5);
    let event_kind: String = row.get(6);
    let target_kind: Option<String> = row.get(7);
    let target_id: Option<String> = row.get(8);
    let target_version: Option<i64> = row.get(9);
    let summary: String = row.get(10);
    let recorded_at: SystemTime = row.get(11);
    let target = match (target_kind, target_id, target_version) {
        (None, None, None) => None,
        (Some(kind), Some(id), Some(version)) => Some(ResourceRef::new(
            rows::resource_kind(&kind)?,
            rows::resource_id(&id)?,
            rows::version_number(version)?,
        )),
        _ => {
            return Err(rows::unreadable(
                "an audit event's target is half a reference",
            ));
        }
    };
    Ok(AuditEvent {
        id: rows::audit_event_id(&id)?,
        mutation: rows::mutation_id(&mutation)?,
        actor: rows::actor(
            &actor_kind,
            issuer.as_deref(),
            subject.as_deref(),
            component.as_deref(),
        )?,
        kind: rows::mutation_kind(&event_kind)?,
        target,
        summary,
        recorded_at,
    })
}

#[cfg(test)]
mod tests {
    use std::time::UNIX_EPOCH;

    use super::super::hydration::HydrationLimit;
    use super::*;
    use crate::backends::{BackendFailure, FailureCategory};
    use crate::desired_state::fixtures::{
        DESIRED_STATE_RESOURCES, candidate, reference, state, state_with_renamed_alias,
        state_with_second_tenant, state_with_two_blobs, tenant,
    };
    use crate::desired_state::{
        Actor, AuditEventId, DesiredState, ExpectedRevision, MutationId, ResourceKind, Uuid7,
        ValidationError, oracle::InMemoryControlPlane,
    };

    /// Each test owns a schema, so the journal's fixed object names do not make
    /// two tests one test.
    async fn journal() -> Option<(PostgresControlPlane, String, String)> {
        let dsn = crate::test_services::postgres_dsn()?;
        let schema = format!(
            "cp_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        );
        let mut config: Config = dsn.parse().expect("test dsn");
        config.connect_timeout(Duration::from_secs(5));
        let (client, connection) = config
            .connect(crate::usage::tls_connector())
            .await
            .expect("connect to create the test schema");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .expect("create the test schema");
        let store = PostgresControlPlane::connect(&dsn, settings(&schema))
            .await
            .expect("boot against a fresh schema");
        Some((store, dsn, schema))
    }

    fn settings(schema: &str) -> ControlPlaneSettings {
        ControlPlaneSettings {
            schema: Some(schema.to_owned()),
            operation_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(5),
            ..ControlPlaneSettings::default()
        }
    }

    /// A second store on the same journal, as a second replica's administrator is.
    async fn second_store(dsn: &str, schema: &str) -> PostgresControlPlane {
        PostgresControlPlane::connect(
            dsn,
            ControlPlaneSettings {
                // The schema is already current; a second booter must not need to
                // be allowed to migrate.
                migrate: false,
                ..settings(schema)
            },
        )
        .await
        .expect("boot against a current schema")
    }

    /// A store on the same journal whose hydration bounds are the caller's.
    ///
    /// The bounds are settings rather than constants precisely so a test can
    /// state one against a five-resource fixture instead of building a revision
    /// large enough to reach a production ceiling.
    async fn bounded_store(
        dsn: &str,
        schema: &str,
        hydration: HydrationLimits,
    ) -> PostgresControlPlane {
        PostgresControlPlane::connect(
            dsn,
            ControlPlaneSettings {
                migrate: false,
                hydration,
                ..settings(schema)
            },
        )
        .await
        .expect("boot against a current schema")
    }

    impl PostgresControlPlane {
        /// Corrupt the journal the way a restored backup or a manual `UPDATE`
        /// would: out of band, behind the store's back. Tests only — this is how
        /// storage the writer would never produce gets produced.
        async fn corrupt_with(&self, statement: &str) {
            let sql = statement.to_owned();
            self.run(move |client| {
                let sql = sql.clone();
                Box::pin(async move {
                    client
                        .batch_execute(&sql)
                        .await
                        .map_err(|error| unavailable("corrupt the journal", &error))
                })
            })
            .await
            .expect("the corrupting statement itself must succeed");
        }

        async fn count(&self, table: &str) -> i64 {
            let sql = format!("SELECT count(*) FROM {table}");
            self.run(move |client| {
                Box::pin(async move {
                    client
                        .query_one(&sql, &[])
                        .await
                        .map(|row| row.get(0))
                        .map_err(|error| unavailable("count", &error))
                })
            })
            .await
            .expect("count")
        }
    }

    fn uuid(seed: u64) -> Uuid7 {
        Uuid7::from_parts(seed, 0, seed).expect("seed in range")
    }

    /// A candidate carrying an explicit mutation identity, for the cases that
    /// reuse a key the fixture derives its ids from: a mutation and an audit
    /// event are each written once, so a second change needs its own.
    fn candidate_with_mutation(
        expected: ExpectedRevision,
        key: &str,
        state: crate::desired_state::DesiredState,
        seed: u64,
    ) -> RevisionCandidate {
        let mut candidate = candidate(expected, key, state);
        let mutation = MutationId::new(uuid(seed));
        candidate.mutation.id = mutation;
        candidate.audit.mutation = mutation;
        candidate.audit.id = AuditEventId::new(uuid(seed + 1));
        candidate
    }

    #[tokio::test]
    async fn boot_migrates_a_fresh_database_and_reports_the_schema_current() {
        let Some((store, dsn, schema)) = journal().await else {
            return;
        };
        assert_eq!(
            store.schema_status().await.expect("status"),
            SchemaStatus::Current {
                version: schema::required_version()
            }
        );
        assert_eq!(store.name(), "postgres");
        assert!(store.health().await.is_ok());
        assert_eq!(store.desired_revision().await.expect("head"), None);

        // A second boot is a no-op, and needs no permission to migrate.
        let second = second_store(&dsn, &schema).await;
        assert!(second.schema_status().await.expect("status").is_current());

        // An unmigrated database with migration withheld is a refusal, not a
        // gateway that serves against a schema it did not write.
        let bare = format!("{schema}_bare");
        store
            .run(move |client| {
                let sql = format!("CREATE SCHEMA {bare}");
                Box::pin(async move {
                    client
                        .batch_execute(&sql)
                        .await
                        .map_err(|error| unavailable("create schema", &error))
                })
            })
            .await
            .expect("create the bare schema");
        let refusal = PostgresControlPlane::connect(
            &dsn,
            ControlPlaneSettings {
                migrate: false,
                ..settings(&format!("{schema}_bare"))
            },
        )
        .await
        .expect_err("an unmigrated schema must not be served");
        assert_eq!(refusal.category(), FailureCategory::Denied);
        assert!(refusal.to_string().contains("not present"), "{refusal}");
    }

    #[tokio::test]
    async fn a_database_a_newer_build_owns_is_refused_rather_than_migrated_backwards() {
        let Some((store, dsn, schema)) = journal().await else {
            return;
        };
        store
            .run(|client| {
                Box::pin(async move {
                    client
                        .execute(
                            "INSERT INTO axond_cp_schema_migration (version, name, checksum) \
                             VALUES (99, 'control_plane_0099_future', $1)",
                            &[&crate::desired_state::Checksum::of(b"future").to_string()],
                        )
                        .await
                        .map(|_| ())
                        .map_err(|error| unavailable("record a future migration", &error))
                })
            })
            .await
            .expect("record a future migration");

        let status = store.schema_status().await.expect("status");
        assert!(
            matches!(status, SchemaStatus::Ahead { applied: 99, .. }),
            "{status:?}"
        );
        let refusal = PostgresControlPlane::connect(&dsn, settings(&schema))
            .await
            .expect_err("a newer schema must not be served");
        assert_eq!(refusal.category(), FailureCategory::Denied);
        assert!(refusal.to_string().contains("newer gateway"), "{refusal}");
    }

    #[tokio::test]
    async fn publication_is_a_chain_whose_history_stays_loadable() {
        let Some((store, _, _)) = journal().await else {
            return;
        };
        let first = store
            .publish_revision(candidate(ExpectedRevision::Empty, "first", state()))
            .await
            .expect("first publication");
        assert_eq!(first.parent, None);
        assert_eq!(
            store.desired_revision().await.expect("head"),
            Some(first.id)
        );

        let second = store
            .publish_revision(candidate(
                ExpectedRevision::Exactly(first.id),
                "second",
                state_with_renamed_alias(),
            ))
            .await
            .expect("second publication");
        assert_eq!(second.parent, Some(first.id));
        assert_ne!(second.checksum, first.checksum);
        assert_eq!(
            store.desired_revision().await.expect("head"),
            Some(second.id)
        );

        // A manifest read back is the manifest publication returned: identity,
        // parentage, entries, blobs, and checksum, not an approximation of them.
        assert_eq!(
            store.load_manifest(first.id).await.expect("manifest"),
            first
        );
        assert_eq!(
            store.load_manifest(second.id).await.expect("manifest"),
            second
        );

        // And the earlier revision still hydrates, unchanged by the later one.
        let loaded = store.load_revision(first.id).await.expect("hydrate");
        assert_eq!(loaded.manifest(), &first);
        assert_eq!(loaded.state().len(), DESIRED_STATE_RESOURCES);
        assert_eq!(loaded.state(), &state());

        let trail = store.audit_trail(first.id).await.expect("audit trail");
        assert_eq!(trail.len(), 1);
        assert_eq!(trail[0].summary, "applied first");

        // Two revisions sharing four of five versions store nine, not ten: a
        // manifest is a reference structure in storage too.
        assert_eq!(store.count("axond_cp_resource_version").await, 6);
        assert_eq!(store.count("axond_cp_blob").await, 1);
        assert_eq!(store.count("axond_cp_revision").await, 2);

        let missing = RevisionId::new(uuid(9_999));
        assert!(matches!(
            store.load_manifest(missing).await,
            Err(ControlPlaneError::RevisionNotFound(_))
        ));
        assert!(matches!(
            store.audit_trail(missing).await,
            Err(ControlPlaneError::RevisionNotFound(_))
        ));
    }

    #[tokio::test]
    async fn concurrent_writers_agree_that_exactly_one_commit_wins() {
        let Some((store, dsn, schema)) = journal().await else {
            return;
        };
        let first = store
            .publish_revision(candidate(ExpectedRevision::Empty, "first", state()))
            .await
            .expect("first publication");
        let other = second_store(&dsn, &schema).await;

        // Two administrators, two connections, one expectation.
        let expected = ExpectedRevision::Exactly(first.id);
        let (left, right) = tokio::join!(
            store.publish_revision(candidate(expected, "race-left", state_with_renamed_alias())),
            other.publish_revision(candidate(expected, "race-right", state()))
        );
        let (winner, loser) = match (left, right) {
            (Ok(winner), Err(loser)) | (Err(loser), Ok(winner)) => (winner, loser),
            (left, right) => panic!(
                "exactly one writer must win an expected-revision race, got {left:?} and {right:?}"
            ),
        };
        assert!(matches!(
            loser,
            ControlPlaneError::Conflict {
                expected: ExpectedRevision::Exactly(_),
                actual: Some(_)
            }
        ));
        assert_eq!(loser.category(), FailureCategory::Conflict);
        assert!(loser.retryable() || !loser.retryable());

        assert_eq!(winner.parent, Some(first.id));
        assert_eq!(
            store.desired_revision().await.expect("head"),
            Some(winner.id)
        );
        // The loser left nothing: two revisions exist, not three, and the loser's
        // mutation and audit event were never recorded.
        assert_eq!(store.count("axond_cp_revision").await, 2);
        assert_eq!(store.count("axond_cp_mutation").await, 2);
        assert_eq!(store.count("axond_cp_audit_event").await, 2);
    }

    #[tokio::test]
    async fn a_failed_publication_rolls_back_every_row_it_had_written() {
        let Some((store, _, _)) = journal().await else {
            return;
        };
        let first = store
            .publish_revision(candidate(ExpectedRevision::Empty, "first", state()))
            .await
            .expect("first publication");
        let recorded = store.audit_trail(first.id).await.expect("audit trail");

        // A candidate that fails *after* its resources and blobs are written: a
        // fresh resource version, but a mutation id that is already recorded.
        let mut doomed_state = state();
        doomed_state
            .insert(tenant(9, "rolled-back"))
            .expect("a second tenant is valid desired state");
        let mut doomed = candidate(
            ExpectedRevision::Exactly(first.id),
            "rollback",
            doomed_state,
        );
        doomed.mutation.id = first.mutation;
        doomed.audit.mutation = first.mutation;
        let error = store
            .publish_revision(doomed)
            .await
            .expect_err("a duplicate mutation id must not be merged into the first change");
        assert_eq!(error.category(), FailureCategory::Denied);

        // Nothing partial survives: not the new resource version, not a revision,
        // not an audit event, and not the head.
        assert_eq!(store.count("axond_cp_revision").await, 1);
        assert_eq!(store.count("axond_cp_audit_event").await, 1);
        assert_eq!(
            store.count("axond_cp_resource_version").await,
            DESIRED_STATE_RESOURCES as i64,
            "the rolled-back publication's resource version must not remain"
        );
        assert_eq!(
            store.desired_revision().await.expect("head"),
            Some(first.id)
        );
        assert_eq!(store.audit_trail(first.id).await.expect("trail"), recorded);
    }

    #[tokio::test]
    async fn an_immutable_version_cannot_be_redefined_and_leaves_nothing_behind() {
        let Some((store, _, _)) = journal().await else {
            return;
        };
        let first = store
            .publish_revision(candidate(ExpectedRevision::Empty, "first", state()))
            .await
            .expect("first publication");

        // The same reference, different content: a caller redefining state an
        // earlier revision still pins.
        let mut redefined = DesiredState::new();
        for resource in state().resources() {
            let resource = if resource.reference.kind == ResourceKind::Tenant {
                let mut renamed = resource.clone();
                renamed.slug = crate::desired_state::Slug::parse("renamed").expect("slug");
                renamed
            } else {
                resource.clone()
            };
            redefined.insert(resource).expect("valid state");
        }
        for blob in state().blobs() {
            redefined.declare_blob(*blob);
        }
        let error = store
            .publish_revision(candidate(
                ExpectedRevision::Exactly(first.id),
                "redefine",
                redefined,
            ))
            .await
            .expect_err("an immutable version must not be redefined");
        assert!(
            matches!(error, ControlPlaneError::ImmutableResourceVersion { .. }),
            "{error:?}"
        );
        assert_eq!(store.count("axond_cp_revision").await, 1);
        assert_eq!(store.count("axond_cp_audit_event").await, 1);
        assert_eq!(
            store.desired_revision().await.expect("head"),
            Some(first.id)
        );
    }

    #[tokio::test]
    async fn a_repeated_key_replays_its_outcome_and_a_reused_one_is_refused() {
        let Some((store, _, _)) = journal().await else {
            return;
        };
        let first = store
            .publish_revision(candidate(ExpectedRevision::Empty, "retry-1", state()))
            .await
            .expect("first publication");

        // The retry arrives with a stale expectation, which is exactly the case a
        // retry is: replay identity is the state checksum, so it replays rather
        // than conflicting.
        let replayed = store
            .publish_revision(candidate(ExpectedRevision::Empty, "retry-1", state()))
            .await
            .expect("a retry of the same state replays");
        assert_eq!(replayed, first);
        assert_eq!(store.count("axond_cp_revision").await, 1);
        assert_eq!(store.count("axond_cp_mutation").await, 1);
        assert_eq!(store.count("axond_cp_audit_event").await, 1);
        assert_eq!(store.audit_trail(first.id).await.expect("trail").len(), 1);

        // The same key describing different state is refused, never replayed as
        // an outcome the caller did not ask for.
        let error = store
            .publish_revision(candidate_with_mutation(
                ExpectedRevision::Exactly(first.id),
                "retry-1",
                state_with_renamed_alias(),
                4_242,
            ))
            .await
            .expect_err("a reused key must be refused");
        let ControlPlaneError::IdempotencyKeyReused { published, .. } = error else {
            panic!("expected a reused-key refusal, got {error:?}");
        };
        assert_eq!(published, first.id);
        assert_eq!(store.count("axond_cp_revision").await, 1);
    }

    #[tokio::test]
    async fn one_callers_key_neither_replays_nor_blocks_anothers() {
        let Some((store, _, _)) = journal().await else {
            return;
        };
        let first = store
            .publish_revision(candidate(ExpectedRevision::Empty, "retry-1", state()))
            .await
            .expect("first publication");

        // A different authenticated caller, the same key, the same state: a
        // second write, because deduplication is scoped per caller.
        let mut other = candidate_with_mutation(
            ExpectedRevision::Exactly(first.id),
            "retry-1",
            state(),
            7_777,
        );
        let system = Actor::System {
            component: "catalog-refresh".to_owned(),
        };
        other.mutation.actor = system.clone();
        other.audit.actor = system;
        let second = store
            .publish_revision(other)
            .await
            .expect("another caller's identical key is another write");
        assert_ne!(second.id, first.id);
        assert_eq!(second.parent, Some(first.id));
        assert_eq!(store.count("axond_cp_idempotency").await, 2);
    }

    #[tokio::test]
    async fn an_expired_retry_window_closes_without_touching_the_revision() {
        let Some((store, dsn, schema)) = journal().await else {
            return;
        };
        // A store whose retry window is closed as soon as it is opened, so the
        // retry below arrives after expiry rather than after a wait.
        let expiring = PostgresControlPlane::connect(
            &dsn,
            ControlPlaneSettings {
                migrate: false,
                idempotency_retention: Duration::ZERO,
                ..settings(&schema)
            },
        )
        .await
        .expect("boot");
        let first = expiring
            .publish_revision(candidate(ExpectedRevision::Empty, "retry-1", state()))
            .await
            .expect("first publication");

        let second = store
            .publish_revision(candidate_with_mutation(
                ExpectedRevision::Exactly(first.id),
                "retry-1",
                state(),
                8_888,
            ))
            .await
            .expect("an expired record is not a replay");
        assert_ne!(second.id, first.id);

        // Expiry is a window closing, not a deletion: the revision the record
        // pointed at, and its audit trail, are untouched.
        assert_eq!(
            store.load_manifest(first.id).await.expect("manifest"),
            first
        );
        assert_eq!(store.audit_trail(first.id).await.expect("trail").len(), 1);
        assert_eq!(store.count("axond_cp_revision").await, 2);
    }

    #[tokio::test]
    async fn an_outage_is_an_outage_and_changes_nothing_a_replica_holds() {
        let Some((store, dsn, schema)) = journal().await else {
            return;
        };
        let first = store
            .publish_revision(candidate(ExpectedRevision::Empty, "first", state()))
            .await
            .expect("first publication");
        // What a replica is serving: an immutable value it already holds.
        let held = store.load_revision(first.id).await.expect("hydrate");

        let stalled = PostgresControlPlane::connect(
            &dsn,
            ControlPlaneSettings {
                migrate: false,
                operation_timeout: Duration::from_nanos(1),
                ..settings(&schema)
            },
        )
        .await
        .expect("boot");
        let error = stalled
            .publish_revision(candidate(
                ExpectedRevision::Exactly(first.id),
                "stalled",
                state_with_renamed_alias(),
            ))
            .await
            .expect_err("a publication that cannot finish must not report success");
        assert_eq!(error.category(), FailureCategory::Unavailable);
        assert!(error.retryable());

        // The outage moved nothing: not the head, not the journal, and not the
        // snapshot the replica holds — which is a value, not a query.
        assert_eq!(
            store.desired_revision().await.expect("head"),
            Some(first.id)
        );
        assert_eq!(store.count("axond_cp_revision").await, 1);
        assert_eq!(store.count("axond_cp_audit_event").await, 1);
        assert_eq!(held.manifest(), &first);
        assert_eq!(held.state(), &state());

        // And the store recovers: an outage discards the connection rather than
        // poisoning the store.
        assert!(store.health().await.is_ok());
    }

    #[tokio::test]
    async fn the_journal_answers_the_contract_the_way_the_oracle_does() {
        let Some((store, _, _)) = journal().await else {
            return;
        };
        let oracle = InMemoryControlPlane::new();
        let stores: [&dyn ControlPlaneStore; 2] = [&store, &oracle];
        let mut heads = Vec::new();
        for store in stores {
            assert_eq!(store.desired_revision().await.expect("head"), None);
            let first = store
                .publish_revision(candidate(ExpectedRevision::Empty, "first", state()))
                .await
                .expect("first publication");
            let stale = store
                .publish_revision(candidate(
                    ExpectedRevision::Empty,
                    "stale",
                    state_with_renamed_alias(),
                ))
                .await
                .expect_err("a stale expectation conflicts");
            assert_eq!(stale.category(), FailureCategory::Conflict);
            let replay = store
                .publish_revision(candidate(ExpectedRevision::Empty, "first", state()))
                .await
                .expect("a retry replays");
            assert_eq!(replay, first);
            let loaded = store.load_revision(first.id).await.expect("hydrate");
            assert_eq!(loaded.state(), &state());
            assert_eq!(
                store.audit_trail(first.id).await.expect("trail").len(),
                1,
                "one mutation is one audit event"
            );
            heads.push(store.desired_revision().await.expect("head").is_some());
        }
        assert_eq!(heads, vec![true, true]);
    }

    #[tokio::test]
    async fn requested_tls_is_not_silently_downgraded() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        // The CI server speaks no TLS. `sslmode=require` must therefore fail to
        // connect rather than continue in the clear.
        let separator = if dsn.contains('?') { "&" } else { "?" };
        let error = PostgresControlPlane::connect(
            &format!("{dsn}{separator}sslmode=require"),
            ControlPlaneSettings {
                migrate: false,
                ..ControlPlaneSettings::default()
            },
        )
        .await
        .expect_err("TLS that cannot be established is a refusal, not a plaintext session");
        assert!(
            matches!(
                error,
                ControlPlaneError::Unavailable { .. } | ControlPlaneError::Denied { .. }
            ),
            "{error:?}"
        );
    }

    /// The three revisions the hydration tests share: the fixture state, a
    /// rename of one alias, and a second tenant's resources alongside the first's.
    ///
    /// Published in order, so the first two are *historical* by the time anything
    /// loads them — which is the case #166 exists to keep answerable.
    async fn three_revisions(
        store: &PostgresControlPlane,
    ) -> (RevisionManifest, RevisionManifest, RevisionManifest) {
        let first = store
            .publish_revision(candidate(ExpectedRevision::Empty, "first", state()))
            .await
            .expect("first publication");
        let second = store
            .publish_revision(candidate(
                ExpectedRevision::Exactly(first.id),
                "second",
                state_with_renamed_alias(),
            ))
            .await
            .expect("second publication");
        let third = store
            .publish_revision(candidate(
                ExpectedRevision::Exactly(second.id),
                "third",
                state_with_second_tenant(),
            ))
            .await
            .expect("third publication");
        (first, second, third)
    }

    #[tokio::test]
    async fn a_historical_revision_hydrates_as_the_state_it_published() {
        let Some((store, _, _)) = journal().await else {
            return;
        };
        let (first, second, third) = three_revisions(&store).await;

        // Two newer commits later, the oldest revision is still the value it was
        // published as — not a diff to be applied, and not a state the newer
        // revisions edited.
        for (manifest, published) in [
            (&first, state()),
            (&second, state_with_renamed_alias()),
            (&third, state_with_second_tenant()),
        ] {
            let loaded = store.load_revision(manifest.id).await.expect("hydrate");
            assert_eq!(loaded.state(), &published);
            assert_eq!(loaded.manifest(), manifest);
            // The checksum invariant, stated as the hydration itself: the state
            // that came back hashes to what the manifest recorded.
            assert_eq!(
                loaded.state().checksum().expect("canonical"),
                manifest.checksum
            );
            assert_eq!(loaded.manifest().entries.len(), loaded.state().len());
        }

        // Hydration is repeatable, because a revision is immutable: the same read
        // twice is the same value, byte for byte.
        let once = store.load_revision(first.id).await.expect("hydrate");
        let twice = store.load_revision(first.id).await.expect("hydrate");
        assert_eq!(once.state(), twice.state());
        assert_eq!(once.manifest(), twice.manifest());
    }

    #[tokio::test]
    async fn two_revisions_share_immutable_resources_without_sharing_a_value() {
        let Some((store, _, _)) = journal().await else {
            return;
        };
        let (first, second, _) = three_revisions(&store).await;

        // The tenant, project, credential, and catalogue are one row each,
        // pinned by every revision that names them; only the alias was rewritten,
        // and the second tenant's three resources are new.
        assert_eq!(
            store.count("axond_cp_resource_version").await,
            i64::try_from(DESIRED_STATE_RESOURCES + 1 + 3).expect("small"),
        );
        // One catalogue payload, three revisions: a blob is referenced, never
        // copied per revision.
        assert_eq!(store.count("axond_cp_blob").await, 1);
        assert_eq!(store.count("axond_cp_revision_blob").await, 3);

        let older = store.load_revision(first.id).await.expect("hydrate");
        let newer = store.load_revision(second.id).await.expect("hydrate");

        // The shared credential hydrates as an equal value in both revisions,
        // and each revision holds its own alias version. Sharing storage is not
        // sharing state: these are two independent values, so neither can be
        // mutated through the other.
        let credential = reference(ResourceKind::ProviderCredential, 3);
        assert_eq!(
            older.state().get(&credential),
            newer.state().get(&credential)
        );
        let alias_v1 = reference(ResourceKind::Alias, 4);
        let alias_v2 = alias_v1.at(ResourceVersionNumber::FIRST.next());
        assert!(older.state().get(&alias_v1).is_some());
        assert!(older.state().get(&alias_v2).is_none());
        assert!(newer.state().get(&alias_v2).is_some());
        assert!(newer.state().get(&alias_v1).is_none());
        assert_ne!(older.state(), newer.state());

        // And the shared blob is the same reference in both, by digest and size.
        let blobs: Vec<_> = older.state().blobs().collect();
        assert_eq!(blobs, newer.state().blobs().collect::<Vec<_>>());
        assert_eq!(blobs.len(), 1);
    }

    #[tokio::test]
    async fn the_desired_revision_and_its_hydration_are_one_read() {
        let Some((store, _, _)) = journal().await else {
            return;
        };
        // No publication is an empty answer, not an error: a cold deployment has
        // nothing desired yet.
        assert!(
            store
                .load_desired_revision()
                .await
                .expect("an unpublished journal is not a failure")
                .is_none()
        );

        let (_, _, third) = three_revisions(&store).await;
        let desired = store
            .load_desired_revision()
            .await
            .expect("hydrate the head")
            .expect("a head exists");
        assert_eq!(desired.manifest(), &third);
        assert_eq!(desired.state(), &state_with_second_tenant());

        // The oracle answers the same question the same way through the default
        // implementation, so #142 reads one seam rather than two.
        let oracle = InMemoryControlPlane::new();
        assert!(
            oracle
                .load_desired_revision()
                .await
                .expect("empty")
                .is_none()
        );
        let published = oracle
            .publish_revision(candidate(ExpectedRevision::Empty, "first", state()))
            .await
            .expect("publication");
        let loaded = oracle
            .load_desired_revision()
            .await
            .expect("hydrate the head")
            .expect("a head exists");
        assert_eq!(loaded.manifest(), &published);
        assert_eq!(loaded.state(), &state());
    }

    #[tokio::test]
    async fn a_manifest_reference_whose_version_is_gone_is_named_not_dropped() {
        let Some((store, _, _)) = journal().await else {
            return;
        };
        let (first, _, _) = three_revisions(&store).await;

        // A restored backup that lost a row, reproduced exactly: the referential
        // integrity the writer relies on is dropped, and then the row is.
        let project = reference(ResourceKind::Project, 2);
        store
            .corrupt_with(
                "DO $$ DECLARE name text; BEGIN \
                 SELECT conname INTO name FROM pg_constraint \
                 WHERE conrelid = 'axond_cp_revision_entry'::regclass AND contype = 'f' \
                 AND conname LIKE '%resource%'; \
                 EXECUTE format('ALTER TABLE axond_cp_revision_entry DROP CONSTRAINT %I', name); \
                 END $$;",
            )
            .await;
        store
            .corrupt_with(&format!(
                "DELETE FROM axond_cp_resource_version WHERE resource_id = '{}'",
                project.id
            ))
            .await;

        // A shorter manifest would hydrate, and then fail its checksum for
        // reasons that name no row. The reference is named instead.
        for error in [
            store
                .load_manifest(first.id)
                .await
                .expect_err("a manifest missing a version is not a manifest"),
            store
                .load_revision(first.id)
                .await
                .expect_err("a revision missing a version is not hydratable"),
        ] {
            assert_eq!(error.category(), FailureCategory::Corrupt);
            let ControlPlaneError::Corrupt { source, .. } = &error else {
                panic!("expected corruption, got {error:?}");
            };
            assert_eq!(
                **source,
                IntegrityError::MissingResource { reference: project },
                "{error:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_declared_blob_whose_record_is_gone_is_named_not_dropped() {
        let Some((store, _, _)) = journal().await else {
            return;
        };
        let (first, _, _) = three_revisions(&store).await;

        // The blob record deleted from under the revisions that declare it. The
        // declarations remain, so this is the blob-side twin of a missing
        // resource version: the manifest must not come back one blob shorter and
        // well-formed.
        store
            .corrupt_with(
                "DO $$ DECLARE name text; BEGIN \
                 SELECT conname INTO name FROM pg_constraint \
                 WHERE conrelid = 'axond_cp_revision_blob'::regclass AND contype = 'f' \
                 AND conname LIKE '%blob_kind%'; \
                 EXECUTE format('ALTER TABLE axond_cp_revision_blob DROP CONSTRAINT %I', name); \
                 END $$;",
            )
            .await;
        store
            .corrupt_with(
                "ALTER TABLE axond_cp_resource_version \
                 DROP CONSTRAINT axond_cp_resource_version_body_blob_kind_body_blob_digest_fkey; \
                 DELETE FROM axond_cp_blob",
            )
            .await;

        for error in [
            store
                .load_manifest(first.id)
                .await
                .expect_err("a manifest declaring a blob that is gone is not a manifest"),
            store
                .load_revision(first.id)
                .await
                .expect_err("a revision declaring a blob that is gone is not hydratable"),
        ] {
            assert_eq!(error.category(), FailureCategory::Corrupt);
            let ControlPlaneError::Corrupt { source, .. } = &error else {
                panic!("expected corruption, got {error:?}");
            };
            let IntegrityError::Unreadable { detail } = &**source else {
                panic!("expected an unreadable row, got {error:?}");
            };
            assert!(detail.contains("no blob record"), "{detail}");
        }
    }

    #[tokio::test]
    async fn a_dependency_edge_that_leaves_the_revision_is_refused() {
        let Some((store, _, _)) = journal().await else {
            return;
        };
        let (first, _, _) = three_revisions(&store).await;

        // An edge from the first revision's alias to the *second* revision's
        // alias version: both rows exist, so referential integrity is satisfied,
        // and the reference still leaves the revision that names it.
        let alias = reference(ResourceKind::Alias, 4);
        store
            .corrupt_with(&format!(
                "INSERT INTO axond_cp_resource_dependency \
                 (resource_kind, resource_id, version, depends_on_kind, depends_on_id, \
                 depends_on_version) VALUES ('alias', '{id}', 1, 'alias', '{id}', 2)",
                id = alias.id
            ))
            .await;

        let error = store
            .load_revision(first.id)
            .await
            .expect_err("a reference that leaves the revision is not hydratable");
        let ControlPlaneError::Corrupt { source, .. } = &error else {
            panic!("expected corruption, got {error:?}");
        };
        assert!(
            matches!(
                **source,
                IntegrityError::Invalid(ValidationError::DanglingResourceReference { from, to })
                    if from == alias && to == alias.at(ResourceVersionNumber::FIRST.next())
            ),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn an_edge_across_a_tenant_boundary_is_refused_by_the_reference_layer() {
        let Some((store, _, _)) = journal().await else {
            return;
        };
        let (_, _, third) = three_revisions(&store).await;

        // One tenant's alias made to depend on another tenant's credential.
        // Both versions are in this revision, so nothing but the tenant
        // comparison distinguishes this from a legitimate edge.
        let alias = reference(ResourceKind::Alias, 14);
        let credential = reference(ResourceKind::ProviderCredential, 3);
        store
            .corrupt_with(&format!(
                "INSERT INTO axond_cp_resource_dependency \
                 (resource_kind, resource_id, version, depends_on_kind, depends_on_id, \
                 depends_on_version) VALUES ('alias', '{alias}', 1, 'provider-credential', \
                 '{credential}', 1)",
                alias = alias.id,
                credential = credential.id
            ))
            .await;

        let error = store
            .load_revision(third.id)
            .await
            .expect_err("one tenant's state must never hydrate into another's");
        assert_eq!(error.category(), FailureCategory::Corrupt);
        let ControlPlaneError::Corrupt { source, .. } = &error else {
            panic!("expected corruption, got {error:?}");
        };
        assert!(
            matches!(
                **source,
                IntegrityError::Invalid(ValidationError::CrossTenantReference { from, to })
                    if from == alias && to == credential
            ),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn a_deployment_scoped_version_may_not_depend_on_one_tenants_state() {
        let Some((store, _, _)) = journal().await else {
            return;
        };
        let (first, _, _) = three_revisions(&store).await;

        // The catalogue is deployment-scoped: shared by every tenant. An edge
        // from it into one tenant's credential would make deployment-wide state
        // depend on one tenant's, which is the isolation failure that scales.
        let catalog = reference(ResourceKind::CatalogModel, 5);
        let credential = reference(ResourceKind::ProviderCredential, 3);
        store
            .corrupt_with(&format!(
                "INSERT INTO axond_cp_resource_dependency \
                 (resource_kind, resource_id, version, depends_on_kind, depends_on_id, \
                 depends_on_version) VALUES ('catalog-model', '{catalog}', 1, \
                 'provider-credential', '{credential}', 1)",
                catalog = catalog.id,
                credential = credential.id
            ))
            .await;

        let error = store
            .load_revision(first.id)
            .await
            .expect_err("deployment-wide state must not depend on one tenant's");
        let ControlPlaneError::Corrupt { source, .. } = &error else {
            panic!("expected corruption, got {error:?}");
        };
        assert!(
            matches!(
                **source,
                IntegrityError::Invalid(ValidationError::TenantScopedDependency { from, to })
                    if from == catalog && to == credential
            ),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn a_state_checksum_that_no_longer_matches_refuses_the_revision() {
        let Some((store, _, _)) = journal().await else {
            return;
        };
        let (first, _, _) = three_revisions(&store).await;

        // A revision row made to claim a different state's checksum: the rows
        // are individually well-formed, and the whole no longer adds up.
        let foreign = state_with_renamed_alias().checksum().expect("canonical");
        store
            .corrupt_with(&format!(
                "UPDATE axond_cp_revision SET state_checksum = '{foreign}' \
                 WHERE revision_id = '{}'",
                first.id
            ))
            .await;

        let error = store
            .load_revision(first.id)
            .await
            .expect_err("a state that hashes to something else is not the state");
        let ControlPlaneError::Corrupt { source, .. } = &error else {
            panic!("expected corruption, got {error:?}");
        };
        assert!(
            matches!(
                **source,
                IntegrityError::ChecksumMismatch { expected, .. } if expected == foreign
            ),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn a_resource_whose_content_checksum_no_longer_matches_is_refused() {
        let Some((store, _, _)) = journal().await else {
            return;
        };
        let (first, _, _) = three_revisions(&store).await;

        // A slug edited in place. Renaming is a new version, so the row's own
        // content checksum is now the checksum of state this row never held.
        let project = reference(ResourceKind::Project, 2);
        store
            .corrupt_with(&format!(
                "UPDATE axond_cp_resource_version SET slug = 'edited' \
                 WHERE resource_id = '{}'",
                project.id
            ))
            .await;

        let error = store
            .load_revision(first.id)
            .await
            .expect_err("a resource that is not its own content checksum is not hydratable");
        let ControlPlaneError::Corrupt { source, .. } = &error else {
            panic!("expected corruption, got {error:?}");
        };
        assert!(
            matches!(
                **source,
                IntegrityError::ContentMismatch { reference, .. } if reference == project
            ),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn a_body_that_cannot_be_decoded_is_corruption_and_not_an_outage() {
        let Some((store, _, _)) = journal().await else {
            return;
        };
        let (first, _, _) = three_revisions(&store).await;

        let project = reference(ResourceKind::Project, 2);
        store
            .corrupt_with(&format!(
                "UPDATE axond_cp_resource_version SET body_inline = '\\xdeadbeef'::bytea \
                 WHERE resource_id = '{}'",
                project.id
            ))
            .await;

        // The manifest does not read bodies, so it still loads: corruption is
        // reported by the read that actually touches the corrupt column, and
        // hydration is that read.
        store
            .load_manifest(first.id)
            .await
            .expect("a manifest hydrates no body");
        let error = store
            .load_revision(first.id)
            .await
            .expect_err("a body this build cannot decode is not hydratable");
        assert_eq!(error.category(), FailureCategory::Corrupt);
        assert!(!error.retryable(), "corruption is not cleared by retrying");
        let ControlPlaneError::Corrupt { source, .. } = &error else {
            panic!("expected corruption, got {error:?}");
        };
        assert!(
            matches!(**source, IntegrityError::Unreadable { .. }),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn a_row_written_by_a_serializer_this_build_does_not_read_is_refused() {
        let Some((store, _, _)) = journal().await else {
            return;
        };
        let (first, _, _) = three_revisions(&store).await;

        store
            .corrupt_with(
                "UPDATE axond_cp_resource_version SET serializer = 'axond.desired-state.v99'",
            )
            .await;

        let error = store
            .load_revision(first.id)
            .await
            .expect_err("a row this build cannot read is not hydratable");
        assert_eq!(error.category(), FailureCategory::Corrupt);
        let ControlPlaneError::Corrupt { source, .. } = &error else {
            panic!("expected corruption, got {error:?}");
        };
        assert!(
            matches!(**source, IntegrityError::Unreadable { .. }),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn a_version_number_the_domain_cannot_hold_is_refused() {
        let Some((store, _, _)) = journal().await else {
            return;
        };
        let (first, _, _) = three_revisions(&store).await;

        // Version numbers start at one, and the DDL says so. This is what a
        // journal looks like after someone removed that guarantee and wrote a
        // zero: a manifest entry naming a version the domain cannot represent.
        let alias = reference(ResourceKind::Alias, 4);
        store
            .corrupt_with(
                "DO $$ DECLARE name text; BEGIN \
                 SELECT conname INTO name FROM pg_constraint \
                 WHERE conrelid = 'axond_cp_revision_entry'::regclass AND contype = 'f' \
                 AND conname LIKE '%resource%'; \
                 EXECUTE format('ALTER TABLE axond_cp_revision_entry DROP CONSTRAINT %I', name); \
                 END $$;",
            )
            .await;
        store
            .corrupt_with(&format!(
                "UPDATE axond_cp_revision_entry SET version = 0 \
                 WHERE resource_id = '{}' AND revision_id = '{revision}'",
                alias.id,
                revision = first.id
            ))
            .await;

        let error = store
            .load_revision(first.id)
            .await
            .expect_err("a version number that is not one is not a version number");
        assert_eq!(error.category(), FailureCategory::Corrupt);
        let ControlPlaneError::Corrupt { source, .. } = &error else {
            panic!("expected corruption, got {error:?}");
        };
        let IntegrityError::Unreadable { detail } = &**source else {
            panic!("expected an unreadable row, got {error:?}");
        };
        assert!(detail.contains("resource version 0"), "{detail}");
    }

    #[tokio::test]
    async fn a_revision_larger_than_the_bound_is_refused_whole() {
        let Some((store, dsn, schema)) = journal().await else {
            return;
        };
        let (first, _, third) = three_revisions(&store).await;

        // Each bound, stated against the same five-resource revision. Every one
        // of them is a refusal: not a truncated manifest, not the resources that
        // fit, and not a candidate missing its blob.
        let cases: [(HydrationLimits, &str); 5] = [
            (
                HydrationLimits {
                    max_entries: 2,
                    ..HydrationLimits::default()
                },
                "resource versions",
            ),
            (
                HydrationLimits {
                    max_blobs: 0,
                    ..HydrationLimits::default()
                },
                "blobs",
            ),
            (
                HydrationLimits {
                    max_blob_bytes: 8,
                    ..HydrationLimits::default()
                },
                "bytes",
            ),
            (
                HydrationLimits {
                    max_dependency_edges: 1,
                    ..HydrationLimits::default()
                },
                "dependency edges",
            ),
            (
                HydrationLimits {
                    max_inline_body_bytes: 4,
                    ..HydrationLimits::default()
                },
                "inline body",
            ),
        ];
        for (limits, expected) in cases {
            let bounded = bounded_store(&dsn, &schema, limits).await;
            let error = bounded
                .load_revision(first.id)
                .await
                .expect_err("a revision past a bound must not hydrate");
            assert_eq!(error.category(), FailureCategory::Denied);
            assert!(!error.retryable(), "a bound is not cleared by retrying");
            assert!(
                matches!(error, ControlPlaneError::TooLarge { revision, .. } if revision == first.id),
                "{error:?}"
            );
            assert!(error.to_string().contains(expected), "{error}");
        }

        // The declared-bytes figure is the revision's real total, not however
        // much of it was counted before the ceiling was crossed: an operator
        // deciding where to put the bound is told the size they have to clear.
        //
        // Stated against a revision declaring *two* blobs, because with one the
        // partial sum and the total are the same number and the assertion would
        // hold under the short-circuiting this pins.
        let two_blobs = state_with_two_blobs();
        let fourth = store
            .publish_revision(candidate(
                ExpectedRevision::Exactly(third.id),
                "fourth",
                two_blobs.clone(),
            ))
            .await
            .expect("fourth publication");
        let sizes: Vec<u64> = two_blobs.blobs().map(|blob| blob.size_bytes).collect();
        assert_eq!(sizes.len(), 2, "the fixture must declare two blobs");
        let total = sizes.iter().sum::<u64>();
        let bounded = bounded_store(
            &dsn,
            &schema,
            HydrationLimits {
                max_blob_bytes: 8,
                ..HydrationLimits::default()
            },
        )
        .await;
        let error = bounded
            .load_manifest(fourth.id)
            .await
            .expect_err("a revision past a bound must not hydrate");
        assert!(error.to_string().contains(&total.to_string()), "{error}");
        // And the figure is not either blob on its own, which is what a sum that
        // stopped at the first row over the ceiling would have reported.
        for size in sizes {
            assert!(
                !error.to_string().contains(&size.to_string()),
                "reported a partial sum: {error}"
            );
        }

        // The candidate bound is the last line: a state that hydrated is still
        // refused if the value itself is larger than this build returns.
        let bounded = bounded_store(
            &dsn,
            &schema,
            HydrationLimits {
                max_state_bytes: 64,
                ..HydrationLimits::default()
            },
        )
        .await;
        let error = bounded
            .load_revision(first.id)
            .await
            .expect_err("a candidate past the bound must not be returned");
        assert!(
            matches!(
                error,
                ControlPlaneError::TooLarge {
                    limit: HydrationLimit::StateBytes { .. },
                    ..
                }
            ),
            "{error:?}"
        );

        // Nothing above changed the journal, and the store whose bounds fit
        // still hydrates the same revision it always did.
        assert_eq!(
            store
                .load_revision(first.id)
                .await
                .expect("hydrate")
                .state(),
            &state()
        );
    }

    #[test]
    fn a_journal_timestamp_survives_a_round_trip_through_the_column_it_is_stored_in() {
        let now = journal_now();
        let since = now.duration_since(UNIX_EPOCH).expect("after the epoch");
        assert_eq!(
            since.subsec_nanos() % 1_000,
            0,
            "a timestamp with sub-microsecond precision cannot be read back as itself"
        );
    }
}
