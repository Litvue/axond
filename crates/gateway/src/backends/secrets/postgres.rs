//! The production [`SecretStore`]: envelope-encrypted rows in PostgreSQL.
//!
//! The selected boundary for #145. Material is sealed in this process under a
//! per-version data-encryption key, that key is sealed under the deployment KEK
//! the bootstrap config *references*, and only the sealed bytes reach the
//! database ([`envelope`](super::envelope)). So the database holds no material an
//! operator, a backup, or a stolen replica can read, and the key is not in the
//! database to be dumped with it.
//!
//! Three storage rules make the domain contract enforceable rather than
//! aspirational:
//!
//! - **A version is a row, and a row is written once.** The primary key is
//!   `(secret_id, version)`, rotation inserts the next version, and nothing
//!   updates sealed bytes. A revision compiled against version 2 therefore keeps
//!   resolving version 2 while version 3 is staged and proven — the overlap a
//!   zero-downtime rotation is made of.
//! - **Ownership is checked on every read.** A statement keys on the reference and
//!   returns the row's own owner columns, which are then matched against the
//!   caller's tenant and project exactly — so the check cannot be skipped by
//!   writing a query that forgets a predicate. A row this owner does not own
//!   answers exactly as an absent one does ([`SecretError::NotFound`] to a caller,
//!   distinguishable only in this process's own logs). The seal is bound to the
//!   owner as well, so a row moved between tenants in the database does not open.
//! - **Tombstoning destroys bytes in the transaction that records it.** The
//!   lifecycle move and the `NULL`ing of the four sealed columns are one
//!   statement, and the shipped DDL's check constraint refuses any other
//!   combination, so "destroyed" cannot mean "relabelled".
//!
//! Nothing here is on the request path: the whole store is
//! [`BackendPath::SnapshotCompilation`](crate::backends::BackendPath::SnapshotCompilation),
//! so an outage stalls administration and convergence while replicas keep serving
//! the snapshot they already hold.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use async_trait::async_trait;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, Config, Row, Transaction};

use super::envelope::{DeploymentKek, EnvelopeError, SealedSecret};
use super::{
    ENVELOPE_CAPABILITIES, KekRef, SecretDescriptor, SecretError, SecretMaterial, SecretResolver,
    SecretStore,
};
use crate::desired_state::secrets::{
    LifecycleTransition, SecretLifecycle, SecretOwner, SecretRef, SecretVersion,
};
use crate::desired_state::{SecretId, Uuid7Generator};

const BACKEND: &str = "encrypted-postgres";

/// The shipped DDL this store applies with `create_table = true`.
const SCHEMA_DDL: &str = include_str!("../../../sql/secret_store_v1.sql");

/// The columns a sealed record is read back from, in one place so `resolve` and
/// the row decoder cannot disagree about their order.
const MATERIAL_COLUMNS: &str = "tenant_id, project_id, lifecycle, scheme, kek_reference, wrapped_dek, dek_nonce, \
     ciphertext, nonce";

/// How the store connects, and what it may do at boot.
#[derive(Debug, Clone)]
pub struct SecretStoreSettings {
    /// The PostgreSQL schema the table lives in, if not the connection's default.
    /// Validated as an identifier, because it is interpolated into
    /// `SET search_path`.
    pub schema: Option<String>,
    /// Whether boot may apply the shipped DDL. An operator who applies it out of
    /// band leaves this off and gets a refusal instead of a schema change.
    pub create_table: bool,
    pub connect_timeout: Duration,
    /// The ceiling on one secret-store operation. Generous by inference-path
    /// standards: nothing here is called with a request in flight.
    pub operation_timeout: Duration,
}

impl Default for SecretStoreSettings {
    fn default() -> Self {
        Self {
            schema: None,
            create_table: true,
            connect_timeout: Duration::from_secs(10),
            operation_timeout: Duration::from_secs(30),
        }
    }
}

impl SecretStoreSettings {
    /// The settings a `[secret_store]` section asks for, with connection bounds
    /// inherited from `[control_plane]`: encrypted Postgres is normally the same
    /// database, and two independent sets of timeouts for one server is a
    /// configuration surface with no decision behind it.
    pub fn from_config(
        secret_store: &crate::config::SecretStore,
        control_plane: &crate::config::ControlPlane,
    ) -> Self {
        Self {
            schema: secret_store
                .schema
                .as_deref()
                .map(str::trim)
                .filter(|schema| !schema.is_empty())
                .map(str::to_owned),
            create_table: secret_store.create_table,
            connect_timeout: Duration::from_millis(control_plane.connect_timeout_ms),
            operation_timeout: Duration::from_millis(control_plane.operation_timeout_ms),
        }
    }
}

/// A [`SecretStore`] holding envelope-encrypted material in `axond_secret`.
pub struct PostgresSecrets {
    config: Config,
    settings: SecretStoreSettings,
    /// Set on every connection, including reconnections: a reconnect that landed
    /// on the default schema would silently read a different table.
    search_path: Option<String>,
    kek: DeploymentKek,
    ids: Uuid7Generator,
    client: tokio::sync::Mutex<Option<Client>>,
}

/// Written by hand, and deliberately narrow: a derived one would print the
/// [`Config`], which carries the password from the DSN.
impl std::fmt::Debug for PostgresSecrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSecrets")
            .field("schema", &self.search_path)
            .field("kek", self.kek.reference())
            .finish_non_exhaustive()
    }
}

impl PostgresSecrets {
    /// Connect, optionally apply the shipped DDL, and prove the table is
    /// readable.
    ///
    /// The KEK is resolved by the caller — it comes from an env var or a file
    /// named in bootstrap config — so this takes the key rather than the
    /// reference: a store that read the material itself would be a second place
    /// key bytes are handled.
    pub async fn connect(
        dsn: &str,
        settings: SecretStoreSettings,
        kek: DeploymentKek,
    ) -> Result<Self, SecretError> {
        let mut config: Config = dsn.parse().map_err(|error| {
            // The DSN itself is never echoed: it carries a password.
            denied(format!("the secret-store DSN could not be parsed: {error}"))
        })?;
        config.connect_timeout(settings.connect_timeout);
        config.application_name(crate::telemetry::SERVICE_NAME);
        let search_path = settings
            .schema
            .as_deref()
            .map(|schema| {
                crate::usage::validate_table_name(schema).map_err(denied)?;
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
            kek,
            ids: Uuid7Generator::new(),
            client: tokio::sync::Mutex::new(None),
        };
        // Only this call site is a boot: a bad password or an absent database is
        // a deployment to fix, and a replica that has not started yet loses
        // nothing by saying so. `connect_client` is also the reconnect path, and
        // there the same codes stay retryable — see `run`.
        let client = tokio::time::timeout(store.settings.connect_timeout, store.connect_client())
            .await
            .map_err(|_| unavailable_message("connection timed out"))?
            .map_err(|error| {
                boot_failure(
                    "connect to the secret store",
                    &error,
                    // Nothing has answered a query yet, so there is no
                    // writability to report against this one.
                    Writability::Unknown,
                    || {
                        "Check the role, password, and database named by the `dsn_env` connection \
                         string under `[secret_store]`."
                            .to_owned()
                    },
                )
            })?;
        let writability = Writability::of(&client).await;
        writability.report();
        store.prepare_schema(&client, writability).await?;
        *store.client.lock().await = Some(client);
        Ok(store)
    }

    /// The KEK reference material is currently sealed under. A name, for the
    /// status endpoint and for logs.
    pub fn kek_reference(&self) -> &KekRef {
        self.kek.reference()
    }

    /// Apply the shipped DDL when allowed, and either way establish that the
    /// table this build writes is present and readable.
    ///
    /// Boot refuses rather than degrades: a missing table with
    /// `create_table = false` is [`SecretError::Denied`], because a store that
    /// carried on would fail every candidate revision at compile time with an
    /// error that pointed at the wrong thing.
    ///
    /// Neither step is blanket-`Unavailable` like the statements below are: the
    /// two failures an operator actually hits here — no privilege to create the
    /// table, and no table to read — are permanent, and telling on-call to wait
    /// for a database that is answering fine would send them the wrong way.
    async fn prepare_schema(
        &self,
        client: &Client,
        writability: Writability,
    ) -> Result<(), SecretError> {
        if self.settings.create_table {
            client.batch_execute(SCHEMA_DDL).await.map_err(|error| {
                boot_failure("apply secret-store schema", &error, writability, || {
                    "Grant the connecting role `CREATE` on the schema and ownership of \
                         `axond_secret`, or apply `ops/postgres/secret_store_v1.sql` yourself and \
                         set `create_table = false` under `[secret_store]`."
                        .to_owned()
                })
            })?;
        }
        client
            .query_one("SELECT count(*) FROM axond_secret WHERE false", &[])
            .await
            .map_err(|error| {
                boot_failure(
                    "read the secret store's `axond_secret` table",
                    &error,
                    writability,
                    || {
                        "Apply `ops/postgres/secret_store_v1.sql`, or set `create_table = true` \
                         under `[secret_store]` to let boot apply it."
                            .to_owned()
                    },
                )
            })?;
        Ok(())
    }

    async fn connect_client(&self) -> Result<Client, tokio_postgres::Error> {
        let (client, connection) = self.config.connect(crate::usage::tls_connector()).await?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::warn!(%error, "postgres secret-store connection closed");
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
    /// and dropping one an outage broke. A refusal keeps the connection: it says
    /// nothing about its health.
    async fn run<T>(
        &self,
        operation: impl for<'a> FnOnce(
            &'a mut Client,
        ) -> Pin<
            Box<dyn Future<Output = Result<T, SecretError>> + Send + 'a>,
        >,
    ) -> Result<T, SecretError> {
        let mut guard = self.client.lock().await;
        if guard.as_ref().is_none_or(Client::is_closed) {
            // Deliberately not classified the way boot's connect is. A serving
            // replica reconnects for the life of the process, and a credential
            // rotation the deployment is halfway through, or a pooler answering
            // for a backend that has not reloaded `pg_hba.conf`, answers with the
            // same permanent-looking codes and clears on the next attempt.
            // Refusing here would strand a replica over a blip; convergence
            // retries an outage.
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
        .map_err(|_| unavailable_message("operation timed out"))
        .and_then(|result| result);
        if matches!(result, Err(SecretError::Unavailable { .. })) {
            *guard = None;
        }
        result
    }

    /// Insert one sealed version. The caller has already established that this is
    /// a version nobody has written.
    async fn insert(
        client: &Transaction<'_>,
        owner: SecretOwner,
        reference: SecretRef,
        sealed: &SealedSecret,
    ) -> Result<u64, SecretError> {
        client
            .execute(
                "INSERT INTO axond_secret (secret_id, version, tenant_id, project_id, lifecycle, \
                 scheme, kek_reference, wrapped_dek, dek_nonce, ciphertext, nonce) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
                 ON CONFLICT (secret_id, version) DO NOTHING",
                &[
                    &reference.secret.to_string(),
                    &version_of(reference),
                    &owner.tenant.to_string(),
                    &owner.project.map(|project| project.to_string()),
                    &SecretLifecycle::Staged.as_str(),
                    &sealed.scheme,
                    &sealed.kek.0,
                    &sealed.wrapped_dek,
                    &sealed.dek_nonce,
                    &sealed.ciphertext,
                    &sealed.nonce,
                ],
            )
            .await
            .map_err(|error| unavailable("insert secret version", &error))
    }

    /// The one place a reference becomes a descriptor: unknown, then not this
    /// owner's, in that order, so neither check can be skipped in one method.
    fn descriptor_of(
        row: &Row,
        owner: SecretOwner,
        reference: &SecretRef,
    ) -> Result<SecretDescriptor, SecretError> {
        let tenant: String = row.get("tenant_id");
        let project: Option<String> = row.get("project_id");
        if tenant != owner.tenant.to_string()
            || project != owner.project.map(|project| project.to_string())
        {
            return Err(SecretError::Ownership {
                reference: *reference,
                owner,
            });
        }
        let stored: String = row.get("lifecycle");
        // A state a newer release wrote is not a state this build may guess at:
        // treating an unknown lifecycle as resolvable would put material in
        // service that an administrator withdrew.
        let lifecycle = SecretLifecycle::parse(&stored).ok_or_else(|| {
            SecretError::Invalid(format!(
                "secret {reference} is stored in state `{stored}`, which this build does not read"
            ))
        })?;
        Ok(SecretDescriptor {
            reference: *reference,
            owner,
            lifecycle,
        })
    }

    async fn locked_descriptor(
        transaction: &Transaction<'_>,
        owner: SecretOwner,
        reference: &SecretRef,
    ) -> Result<SecretDescriptor, SecretError> {
        let row = transaction
            .query_opt(
                "SELECT tenant_id, project_id, lifecycle FROM axond_secret \
                 WHERE secret_id = $1 AND version = $2 FOR UPDATE",
                &[&reference.secret.to_string(), &version_of(*reference)],
            )
            .await
            .map_err(|error| unavailable("read secret version", &error))?
            .ok_or(SecretError::NotFound(*reference))?;
        Self::descriptor_of(&row, owner, reference)
    }
}

#[async_trait]
impl SecretResolver for PostgresSecrets {
    fn name(&self) -> &'static str {
        BACKEND
    }

    fn capabilities(&self) -> crate::backends::Capabilities {
        ENVELOPE_CAPABILITIES
    }

    async fn resolve(
        &self,
        owner: SecretOwner,
        reference: &SecretRef,
    ) -> Result<SecretMaterial, SecretError> {
        let reference = *reference;
        // The read and the unwrap are separate steps on purpose: the connection
        // is released before any plaintext exists, so material is never held
        // across a database round trip, and a slow store cannot lengthen the
        // window a key is in memory for.
        let sealed = self
            .run(|client| {
                Box::pin(async move {
                    let row = client
                        .query_opt(
                            &format!(
                                "SELECT {MATERIAL_COLUMNS} FROM axond_secret \
                                 WHERE secret_id = $1 AND version = $2"
                            ),
                            &[&reference.secret.to_string(), &version_of(reference)],
                        )
                        .await
                        .map_err(|error| unavailable("read secret material", &error))?
                        .ok_or(SecretError::NotFound(reference))?;
                    let descriptor = Self::descriptor_of(&row, owner, &reference)?;
                    if !descriptor.permits_resolution() {
                        return Err(SecretError::Lifecycle {
                            reference,
                            state: descriptor.lifecycle,
                        });
                    }
                    sealed_of(&row, &reference)
                })
            })
            .await?;
        self.kek
            .open(owner, &reference, &sealed)
            .map_err(|error| unwrap_error(error, &reference, sealed.kek))
    }

    async fn exists(&self, owner: SecretOwner, reference: &SecretRef) -> Result<bool, SecretError> {
        match self.describe(owner, reference).await {
            Ok(descriptor) => Ok(descriptor.lifecycle.permits_resolution()),
            // A reference somebody else owns answers as one that is not stored:
            // probing must not enumerate another tenant's material.
            Err(SecretError::NotFound(_) | SecretError::Ownership { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }
}

#[async_trait]
impl SecretStore for PostgresSecrets {
    async fn stage(
        &self,
        owner: SecretOwner,
        material: SecretMaterial,
    ) -> Result<SecretDescriptor, SecretError> {
        if material.is_empty() {
            return Err(SecretError::Invalid("material is empty".to_owned()));
        }
        let reference = SecretRef::first(SecretId::new(self.ids.next()));
        let sealed = self
            .kek
            .seal(owner, &reference, &material)
            .map_err(|error| seal_error(error, &reference))?;
        self.run(|client| {
            Box::pin(async move {
                let transaction = client
                    .transaction()
                    .await
                    .map_err(|error| unavailable("begin stage", &error))?;
                let inserted = Self::insert(&transaction, owner, reference, &sealed).await?;
                if inserted != 1 {
                    // A time-ordered UUIDv7 collision. Reported rather than
                    // overwritten: the row that exists is somebody's material.
                    return Err(SecretError::Invalid(format!("{reference} already exists")));
                }
                transaction
                    .commit()
                    .await
                    .map_err(|error| unavailable("commit stage", &error))?;
                Ok(SecretDescriptor {
                    reference,
                    owner,
                    lifecycle: SecretLifecycle::Staged,
                })
            })
        })
        .await
    }

    async fn rotate(
        &self,
        owner: SecretOwner,
        reference: &SecretRef,
        material: SecretMaterial,
    ) -> Result<SecretDescriptor, SecretError> {
        if material.is_empty() {
            return Err(SecretError::Invalid("material is empty".to_owned()));
        }
        let reference = *reference;
        let rotated = reference.rotated();
        let sealed = self
            .kek
            .seal(owner, &rotated, &material)
            .map_err(|error| seal_error(error, &rotated))?;
        self.run(|client| {
            Box::pin(async move {
                let transaction = client
                    .transaction()
                    .await
                    .map_err(|error| unavailable("begin rotation", &error))?;
                // The base version is locked for the rotation, so two
                // administrators rotating one secret serialize on it instead of
                // both computing the same next version.
                let current = Self::locked_descriptor(&transaction, owner, &reference).await?;
                // Only a tombstone refuses: rotating from a revoked version is
                // how withdrawn material is replaced, and it mints a successor
                // rather than returning the revoked version to service.
                if current.lifecycle.is_terminal() {
                    return Err(SecretError::Lifecycle {
                        reference,
                        state: current.lifecycle,
                    });
                }
                let inserted = Self::insert(&transaction, owner, rotated, &sealed).await?;
                if inserted != 1 {
                    // A version is immutable, so rotating twice from one base
                    // reference is a stale request rather than a second rotation:
                    // overwriting would change what a credential body already
                    // pinning `rotated` resolves to.
                    return Err(SecretError::Invalid(format!("{rotated} already exists")));
                }
                transaction
                    .commit()
                    .await
                    .map_err(|error| unavailable("commit rotation", &error))?;
                Ok(SecretDescriptor {
                    reference: rotated,
                    owner,
                    lifecycle: SecretLifecycle::Staged,
                })
            })
        })
        .await
    }

    async fn transition(
        &self,
        owner: SecretOwner,
        reference: &SecretRef,
        next: SecretLifecycle,
    ) -> Result<LifecycleTransition, SecretError> {
        let reference = *reference;
        self.run(|client| {
            Box::pin(async move {
                let transaction = client
                    .transaction()
                    .await
                    .map_err(|error| unavailable("begin transition", &error))?;
                let current = Self::locked_descriptor(&transaction, owner, &reference).await?;
                let transition = current
                    .lifecycle
                    .transition_to(next)
                    .map_err(|source| SecretError::Transition { reference, source })?;
                if let LifecycleTransition::Moved { to, .. } = transition {
                    // Tombstoning is the destruction, not a label on material
                    // that stays: the bytes are nulled in the statement that
                    // records the state, and the shipped constraint refuses any
                    // other combination.
                    let statement = if to == SecretLifecycle::Tombstoned {
                        "UPDATE axond_secret SET lifecycle = $3, updated_at = now(), \
                         destroyed_at = now(), wrapped_dek = NULL, dek_nonce = NULL, \
                         ciphertext = NULL, nonce = NULL \
                         WHERE secret_id = $1 AND version = $2"
                    } else {
                        "UPDATE axond_secret SET lifecycle = $3, updated_at = now() \
                         WHERE secret_id = $1 AND version = $2"
                    };
                    transaction
                        .execute(
                            statement,
                            &[
                                &reference.secret.to_string(),
                                &version_of(reference),
                                &to.as_str(),
                            ],
                        )
                        .await
                        .map_err(|error| unavailable("record transition", &error))?;
                }
                transaction
                    .commit()
                    .await
                    .map_err(|error| unavailable("commit transition", &error))?;
                Ok(transition)
            })
        })
        .await
    }

    async fn describe(
        &self,
        owner: SecretOwner,
        reference: &SecretRef,
    ) -> Result<SecretDescriptor, SecretError> {
        let reference = *reference;
        self.run(|client| {
            Box::pin(async move {
                let row = client
                    .query_opt(
                        "SELECT tenant_id, project_id, lifecycle FROM axond_secret \
                         WHERE secret_id = $1 AND version = $2",
                        &[&reference.secret.to_string(), &version_of(reference)],
                    )
                    .await
                    .map_err(|error| unavailable("describe secret version", &error))?
                    .ok_or(SecretError::NotFound(reference))?;
                Self::descriptor_of(&row, owner, &reference)
            })
        })
        .await
    }

    async fn versions(
        &self,
        owner: SecretOwner,
        secret: SecretId,
    ) -> Result<Vec<SecretDescriptor>, SecretError> {
        self.run(|client| {
            Box::pin(async move {
                let rows = client
                    .query(
                        "SELECT version, tenant_id, project_id, lifecycle FROM axond_secret \
                         WHERE secret_id = $1 ORDER BY version",
                        &[&secret.to_string()],
                    )
                    .await
                    .map_err(|error| unavailable("list secret versions", &error))?;
                let mut descriptors = Vec::with_capacity(rows.len());
                for row in &rows {
                    let stored: i64 = row.get("version");
                    let version = u64::try_from(stored)
                        .ok()
                        .and_then(SecretVersion::new)
                        .ok_or_else(|| {
                            SecretError::Invalid(format!(
                                "secret {secret} holds version `{stored}`, which is not a version"
                            ))
                        })?;
                    let reference = SecretRef::new(secret, version);
                    match Self::descriptor_of(row, owner, &reference) {
                        Ok(descriptor) => descriptors.push(descriptor),
                        // Another owner's rows answer as absent ones do, so a
                        // listing cannot be used to enumerate foreign material.
                        Err(SecretError::Ownership { .. }) => return Ok(Vec::new()),
                        Err(error) => return Err(error),
                    }
                }
                Ok(descriptors)
            })
        })
        .await
    }
}

/// A version as the column type. `bigint` is signed, and a version past
/// `i64::MAX` is not reachable by rotation, so the conversion is saturating
/// rather than fallible.
fn version_of(reference: SecretRef) -> i64 {
    i64::try_from(reference.version.get()).unwrap_or(i64::MAX)
}

/// The sealed record a row holds, or why it is not one.
fn sealed_of(row: &Row, reference: &SecretRef) -> Result<SealedSecret, SecretError> {
    let wrapped_dek: Option<Vec<u8>> = row.get("wrapped_dek");
    let dek_nonce: Option<Vec<u8>> = row.get("dek_nonce");
    let ciphertext: Option<Vec<u8>> = row.get("ciphertext");
    let nonce: Option<Vec<u8>> = row.get("nonce");
    let kek = KekRef(row.get::<_, String>("kek_reference"));
    match (wrapped_dek, dek_nonce, ciphertext, nonce) {
        (Some(wrapped_dek), Some(dek_nonce), Some(ciphertext), Some(nonce)) => Ok(SealedSecret {
            scheme: row.get("scheme"),
            kek,
            wrapped_dek,
            dek_nonce,
            ciphertext,
            nonce,
        }),
        // A live row with no bytes is storage the DDL's constraint forbids, so it
        // is corruption rather than a lifecycle answer.
        _ => Err(SecretError::Unwrap {
            reference: *reference,
            kek,
        }),
    }
}

/// An envelope failure while opening, as the contract's error.
///
/// Everything except an unimplemented scheme is [`SecretError::Unwrap`], which is
/// `Corrupt`: a wrong or rotated KEK, a tampered row, and a record bound to
/// another reference are one operator question — "which key does this database's
/// material belong to" — and none of them is retryable.
fn unwrap_error(error: EnvelopeError, reference: &SecretRef, kek: KekRef) -> SecretError {
    match error {
        EnvelopeError::UnknownScheme { found } => SecretError::Invalid(format!(
            "secret {reference} is sealed with scheme `{found}`, which this build does not read"
        )),
        EnvelopeError::Random | EnvelopeError::Unopenable | EnvelopeError::Malformed { .. } => {
            SecretError::Unwrap {
                reference: *reference,
                kek,
            }
        }
    }
}

/// An envelope failure while sealing. Only the CSPRNG can fail here, and a
/// process whose CSPRNG is unavailable cannot store material at all.
fn seal_error(error: EnvelopeError, reference: &SecretRef) -> SecretError {
    SecretError::Denied {
        backend: BACKEND,
        message: format!("secret {reference} could not be sealed: {error}"),
    }
}

fn denied(message: impl Into<String>) -> SecretError {
    SecretError::Denied {
        backend: BACKEND,
        message: message.into(),
    }
}

fn unavailable_message(message: impl Into<String>) -> SecretError {
    SecretError::Unavailable {
        backend: BACKEND,
        message: message.into(),
    }
}

/// A boot-time failure while doing `operation`, split by who has to act.
///
/// A `SQLSTATE` the operator has to answer — a missing table, a missing grant, a
/// database that is not there — is [`SecretError::Denied`] and carries `remedy`,
/// because retrying it forever changes nothing. Everything else is
/// [`SecretError::Unavailable`]: an error with no `SQLSTATE` never reached a
/// server, and a server *can* answer with a transient code (it is starting up,
/// out of connections, deadlocked, or racing a sibling replica's
/// `CREATE TABLE IF NOT EXISTS`), which the next attempt clears. Misclassifying
/// those as `Denied` would stop a replica that was about to succeed and hand the
/// operator a remedy for a problem it does not have.
/// A retryable failure carries `writability`'s diagnostic when the server
/// refused the write, so the one case an operator does have to fix — a `dsn_env`
/// left pointing at a standby — names itself in every retried outage instead of
/// hiding behind a generic "the store is down".
fn boot_failure(
    operation: &str,
    error: &tokio_postgres::Error,
    writability: Writability,
    remedy: impl FnOnce() -> String,
) -> SecretError {
    match error.code().filter(|code| operator_must_act(code)) {
        Some(code) => denied(format!(
            "could not {operation} ({}: {error}). {}",
            code.code(),
            remedy()
        )),
        None => match writability.diagnosis(error.code()) {
            Some(diagnosis) => {
                unavailable_message(format!("{operation} failed: {error}. {diagnosis}"))
            }
            None => unavailable(operation, error),
        },
    }
}

/// What the server said about accepting writes when boot asked, before any
/// statement of ours could fail.
///
/// The preflight exists because `25006` alone cannot be acted on: a stable hot
/// standby and a two-second failover window produce the same code. Asking
/// `pg_is_in_recovery()` separates them, and the answer is reported as a
/// diagnostic rather than a refusal — a replica that boots mid-failover must
/// still be allowed to retry into a promoted primary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Writability {
    /// The endpoint accepts writes.
    Writable,
    /// The endpoint is in recovery: a hot standby, or a primary not yet
    /// promoted. A `dsn_env` pointed here on purpose is a misconfiguration.
    Standby,
    /// Not in recovery, but writes are refused: `default_transaction_read_only`
    /// on the role or the database, or a pooler holding the session read-only.
    ReadOnly,
    /// Boot could not ask — the probe itself failed, or nothing had connected
    /// yet.
    Unknown,
}

impl Writability {
    /// Ask the server directly. A probe that fails is [`Self::Unknown`]: the
    /// diagnostic is a courtesy, and losing it must not fail a boot that would
    /// otherwise work.
    async fn of(client: &Client) -> Self {
        let Ok(row) = client
            .query_one(
                "SELECT pg_is_in_recovery(), current_setting('transaction_read_only') = 'on'",
                &[],
            )
            .await
        else {
            return Self::Unknown;
        };
        match (row.get::<_, bool>(0), row.get::<_, bool>(1)) {
            (true, _) => Self::Standby,
            (false, true) => Self::ReadOnly,
            (false, false) => Self::Writable,
        }
    }

    /// Say so at boot, before anything has failed, so the endpoint is named in
    /// the log of a replica that goes on to retry.
    fn report(self) {
        match self {
            Self::Standby => tracing::warn!(
                "the secret store's `dsn_env` endpoint is in recovery: writes are refused while it \
                 is a standby. Repoint it at the primary if this is not a failover in progress."
            ),
            Self::ReadOnly => tracing::warn!(
                "the secret store's `dsn_env` endpoint refuses writes although it is not in \
                 recovery: check `default_transaction_read_only` on the role or the database, and \
                 the pooler's routing."
            ),
            Self::Writable | Self::Unknown => {}
        }
    }

    /// The operator-facing half of a `25006`, for the retryable error's message.
    fn diagnosis(self, code: Option<&SqlState>) -> Option<&'static str> {
        if code != Some(&SqlState::READ_ONLY_SQL_TRANSACTION) {
            return None;
        }
        match self {
            Self::Standby => Some(
                "The endpoint is in recovery (`pg_is_in_recovery()`). This retries in case a \
                 failover is in progress; if it is not, the `dsn_env` under `[secret_store]` names \
                 a standby and has to be repointed at the primary.",
            ),
            Self::ReadOnly => Some(
                "The endpoint is not in recovery but still refuses writes: check \
                 `default_transaction_read_only` on the connecting role and the database, and the \
                 pooler's routing.",
            ),
            // A server that took writes at boot and refuses them now is a
            // demotion in progress, which is exactly what retrying is for.
            Self::Writable | Self::Unknown => None,
        }
    }
}

/// Whether a `SQLSTATE` names a condition only the operator can clear.
///
/// Class `42` (access rule violated, undefined object), `3F` (invalid schema
/// name), `28` (invalid authorization) and `3D` (invalid catalog name) describe a
/// deployment that is configured wrong: the role lacks a grant, the schema or
/// database is absent, the password is not the one the server wants. No amount of
/// waiting fixes any of them. Every other class —
/// notably `08` connection, `53` insufficient resources, `57` operator
/// intervention, `40` rollback, `55` object in use, and the `23505` two
/// concurrently booting replicas race on — clears on its own.
///
/// The duplicate-object codes are the exception inside class `42`:
/// `CREATE TABLE IF NOT EXISTS` is not race-free, and a fleet booting at once
/// can surface the collision as `42P07`/`42710` rather than `23505`. The next
/// attempt finds the object already there, so those are the sibling replica
/// winning, not a deployment to fix.
///
/// `25006` (read-only transaction) is the deliberate near miss. A `dsn_env`
/// pointed at a hot standby answers with it and needs an operator, but so does a
/// primary that is being demoted and a pooler routing to a replica for the length
/// of a failover, and those clear by themselves. Refusing a replica that started
/// mid-failover, with a remedy naming a grant it already has, is the worse of the
/// two mistakes, so the standing misconfiguration is separated out by
/// [`Writability`]'s preflight instead: it names the endpoint in the boot log and
/// in every retried outage, without ever refusing one.
fn operator_must_act(code: &SqlState) -> bool {
    if matches!(
        *code,
        SqlState::DUPLICATE_TABLE | SqlState::DUPLICATE_OBJECT
    ) {
        return false;
    }
    matches!(code.code().get(..2), Some("42" | "3F" | "28" | "3D"))
}

/// A Postgres failure while doing `operation`.
///
/// Everything is `Unavailable`: the statements here are parameterized and the
/// only constraint they can violate is the primary key, which every caller
/// checks by row count before it gets here. So a failing statement is a database
/// this replica cannot use rather than a request it should not have made, and
/// convergence retries it on a backoff.
fn unavailable(operation: &str, error: &tokio_postgres::Error) -> SecretError {
    SecretError::Unavailable {
        backend: BACKEND,
        message: format!("{operation} failed: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;

    use super::*;
    use crate::backends::{BackendFailure, Capability, FailureCategory};
    use crate::desired_state::fixtures::{project_id, tenant_id};
    use crate::desired_state::secrets::SecretVersion;
    use crate::test_services::postgres_dsn;

    const PLAINTEXT: &str = "sk-live-do-not-log";

    #[test]
    fn only_the_sqlstates_an_operator_can_clear_refuse_a_boot() {
        for permanent in [
            SqlState::INSUFFICIENT_PRIVILEGE,
            SqlState::UNDEFINED_TABLE,
            SqlState::INVALID_SCHEMA_NAME,
            SqlState::INVALID_PASSWORD,
            SqlState::INVALID_CATALOG_NAME,
        ] {
            assert!(
                operator_must_act(&permanent),
                "{} needs an operator",
                permanent.code()
            );
        }
        // A booting, saturated, contended, or restarting server answers with a
        // code too, and the next attempt clears every one of these — including
        // the unique violation two replicas racing `CREATE TABLE IF NOT EXISTS`
        // collide on.
        for transient in [
            SqlState::CANNOT_CONNECT_NOW,
            SqlState::TOO_MANY_CONNECTIONS,
            SqlState::ADMIN_SHUTDOWN,
            SqlState::T_R_DEADLOCK_DETECTED,
            SqlState::LOCK_NOT_AVAILABLE,
            SqlState::UNIQUE_VIOLATION,
            SqlState::DUPLICATE_TABLE,
            SqlState::DUPLICATE_OBJECT,
            SqlState::CONNECTION_FAILURE,
            // A demotion or a failover window answers with this one, and the
            // hot-standby DSN it also names stays visible in the retried outage.
            SqlState::READ_ONLY_SQL_TRANSACTION,
        ] {
            assert!(
                !operator_must_act(&transient),
                "{} is worth retrying",
                transient.code()
            );
        }
    }

    /// `25006` is retryable either way; what changes is what on-call is told.
    #[test]
    fn a_read_only_endpoint_is_diagnosed_rather_than_refused() {
        let read_only = Some(&SqlState::READ_ONLY_SQL_TRANSACTION);
        let standby = Writability::Standby
            .diagnosis(read_only)
            .expect("a standby names itself");
        assert!(standby.contains("pg_is_in_recovery"), "{standby}");
        assert!(standby.contains("dsn_env"), "{standby}");
        let misconfigured = Writability::ReadOnly
            .diagnosis(read_only)
            .expect("a read-only session names its setting");
        assert!(
            misconfigured.contains("default_transaction_read_only"),
            "{misconfigured}"
        );
        // A server that accepted writes at boot and refuses one now is being
        // demoted: there is nothing for an operator to do but let it retry.
        assert_eq!(Writability::Writable.diagnosis(read_only), None);
        assert_eq!(Writability::Unknown.diagnosis(read_only), None);
        // And the diagnostic belongs to `25006` alone.
        assert_eq!(
            Writability::Standby.diagnosis(Some(&SqlState::TOO_MANY_CONNECTIONS)),
            None
        );
    }

    /// The same endpoint as `dsn`, reached as another role. The test DSN is
    /// whatever the environment supplies, so the credentials are replaced rather
    /// than assumed; `None` for a key/value DSN this cannot rewrite.
    fn with_role(dsn: &str, role: &str, password: &str) -> Option<String> {
        let (scheme, rest) = dsn.split_once("://")?;
        if !matches!(scheme, "postgres" | "postgresql") {
            return None;
        }
        let endpoint = match rest.split_once('/') {
            // Only an `@` in the authority is credentials; one in the path or
            // the query is not.
            Some((authority, _)) => authority
                .rsplit_once('@')
                .map_or(rest, |(_, host)| &rest[authority.len() - host.len()..]),
            None => rest.rsplit_once('@').map_or(rest, |(_, host)| host),
        };
        Some(format!("{scheme}://{role}:{password}@{endpoint}"))
    }

    #[test]
    fn a_test_dsn_is_rewritten_onto_another_role_whatever_credentials_it_carried() {
        for dsn in [
            "postgres://postgres:secret@127.0.0.1:5432/axond",
            "postgres://127.0.0.1:5432/axond",
            "postgresql://someone@127.0.0.1:5432/axond?sslmode=disable",
        ] {
            let rewritten = with_role(dsn, "reader", "pw").expect("a URL-shaped DSN");
            assert!(
                rewritten.contains("://reader:pw@127.0.0.1:5432/axond"),
                "{rewritten}"
            );
        }
        assert_eq!(
            with_role("host=127.0.0.1 user=postgres", "reader", "pw"),
            None
        );
    }

    /// A live endpoint that refuses writes: the boot is retryable, and the error
    /// says which of the two read-only worlds it is in.
    #[tokio::test]
    async fn a_read_only_boot_is_an_outage_carrying_its_own_diagnosis() {
        let Some(dsn) = postgres_dsn() else {
            return;
        };
        let (client, connection) = tokio_postgres::connect(&dsn, crate::usage::tls_connector())
            .await
            .expect("connect to the test database");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let role = format!(
            "axond_ro_{}",
            Uuid7Generator::new().next().to_string().replace('-', "")
        );
        // The grant matters: without `CREATE` the role would fail the DDL on
        // privilege too, and the test would only be passing because the server
        // happens to check read-only-ness first.
        client
            .batch_execute(&format!(
                "CREATE ROLE {role} LOGIN PASSWORD 'axond'; ALTER ROLE {role} SET \
                 default_transaction_read_only = on; GRANT CREATE, USAGE ON SCHEMA public TO \
                 {role}"
            ))
            .await
            .expect("a role that may create but cannot write");

        // Everything after the role exists runs through `outcome`, so the shared
        // test database does not keep the login role when an assertion fails.
        let outcome = match with_role(&dsn, &role, "axond") {
            Some(read_only_dsn) => Some((
                schema_ddl_sqlstate(&read_only_dsn).await,
                PostgresSecrets::connect(&read_only_dsn, SecretStoreSettings::default(), kek(29))
                    .await
                    .err(),
            )),
            None => None,
        };
        client
            .batch_execute(&format!(
                "REVOKE ALL ON SCHEMA public FROM {role}; DROP ROLE IF EXISTS {role}"
            ))
            .await
            .expect("drop the test role");

        let Some((sqlstate, outcome)) = outcome else {
            // A key/value DSN this cannot rewrite: nothing was exercised.
            return;
        };
        // The fixture is pinned rather than assumed: the role holds `CREATE` on
        // the schema, so the server cannot answer the DDL with `42501` and reach
        // the assertions below by a different route than the one they name.
        assert_eq!(
            sqlstate.as_ref(),
            Some(&SqlState::READ_ONLY_SQL_TRANSACTION),
            "this fixture is only a demotion window if the endpoint refuses the DDL for being \
             read-only; a different SQLSTATE means the role or the pooler, not read-only-ness, \
             is what fails here"
        );

        let error = outcome.expect("a read-only endpoint cannot be prepared");
        assert_eq!(
            error.category(),
            FailureCategory::Unavailable,
            "a demotion window must be retried, not refused: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains("default_transaction_read_only"),
            "the outage carries the preflight's answer for the endpoint that is read-only \
             without being in recovery: {message}"
        );
    }

    /// The SQLSTATE the endpoint actually answers the shipped schema statements
    /// with, so a classification assertion cannot quietly start proving
    /// something else on another server version or behind a pooler.
    async fn schema_ddl_sqlstate(dsn: &str) -> Option<SqlState> {
        let (client, connection) = tokio_postgres::connect(dsn, crate::usage::tls_connector())
            .await
            .ok()?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(SCHEMA_DDL)
            .await
            .err()?
            .code()
            .cloned()
    }

    fn kek(seed: u8) -> DeploymentKek {
        DeploymentKek::parse(
            KekRef("AXOND_TEST_KEK".to_owned()),
            &STANDARD.encode([seed; 32]),
        )
        .expect("a 32-byte key")
    }

    fn owner() -> SecretOwner {
        SecretOwner::tenant(tenant_id(1))
    }

    /// A store on its own schema, so tests are independent and leave nothing
    /// behind for the next run to trip over. `None` when no Postgres is
    /// configured, which skips the test.
    async fn store(seed: u8) -> Option<(PostgresSecrets, String)> {
        let dsn = postgres_dsn()?;
        let schema = format!(
            "axond_secret_test_{}",
            Uuid7Generator::new().next().to_string().replace('-', "")
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
        let store = PostgresSecrets::connect(
            &dsn,
            SecretStoreSettings {
                schema: Some(schema.clone()),
                ..SecretStoreSettings::default()
            },
            kek(seed),
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

    #[test]
    fn the_store_declares_envelope_encryption_and_never_the_request_path() {
        let responsibility =
            crate::backends::responsibility("SecretStore").expect("a declared responsibility");
        assert!(!responsibility.path.on_request_path());
        assert!(ENVELOPE_CAPABILITIES.has(Capability::EnvelopeEncryption));
    }

    /// A DSN that parses but points nowhere is an outage, not a refusal: the
    /// distinction is what makes convergence retry instead of failing a revision.
    #[tokio::test]
    async fn an_unreachable_store_is_unavailable() {
        let error = PostgresSecrets::connect(
            "postgres://axond@127.0.0.1:1/axond?connect_timeout=1",
            SecretStoreSettings {
                connect_timeout: Duration::from_millis(200),
                ..SecretStoreSettings::default()
            },
            kek(1),
        )
        .await
        .expect_err("nothing listens there");
        assert_eq!(error.category(), FailureCategory::Unavailable);
        assert!(!error.to_string().contains("sk-"), "{error}");
    }

    /// A database that is not there is a DSN to fix, and boot says so instead of
    /// telling on-call to wait for a server that is answering fine. The reconnect
    /// path deliberately keeps the same code retryable — see `run`.
    #[tokio::test]
    async fn a_boot_against_an_absent_database_is_refused_not_an_outage() {
        let Some(dsn) = postgres_dsn() else {
            return;
        };
        let config: Config = dsn.parse().expect("the test DSN parses");
        let tokio_postgres::config::Host::Tcp(host) = &config.get_hosts()[0] else {
            panic!("the test DSN names a TCP host");
        };
        let absent = format!(
            "host={host} port={} user={} password={} dbname=axond_absent_database",
            config.get_ports()[0],
            config.get_user().unwrap_or("postgres"),
            String::from_utf8_lossy(config.get_password().unwrap_or_default()),
        );

        let error = PostgresSecrets::connect(&absent, SecretStoreSettings::default(), kek(1))
            .await
            .expect_err("there is no such database");

        assert_eq!(error.category(), FailureCategory::Denied);
        let message = error.to_string();
        assert!(message.contains("dsn_env"), "{message}");
    }

    #[tokio::test]
    async fn a_malformed_dsn_is_refused_without_being_echoed() {
        let error = PostgresSecrets::connect("not a dsn", SecretStoreSettings::default(), kek(1))
            .await
            .expect_err("a DSN that does not parse");
        assert_eq!(error.category(), FailureCategory::Denied);
        assert!(!error.to_string().contains("not a dsn"), "{error}");
    }

    #[tokio::test]
    async fn material_round_trips_and_the_database_holds_only_ciphertext() {
        let Some((store, schema)) = store(11).await else {
            return;
        };
        let staged = store
            .stage(owner(), SecretMaterial::new(PLAINTEXT.to_owned()))
            .await
            .expect("staging material");
        assert_eq!(staged.lifecycle, SecretLifecycle::Staged);
        assert_eq!(staged.reference.version, SecretVersion::FIRST);
        assert_eq!(
            store
                .resolve(owner(), &staged.reference)
                .await
                .expect("staged material resolves")
                .expose(),
            PLAINTEXT
        );
        assert!(store.exists(owner(), &staged.reference).await.unwrap());

        // What a dump of this table would contain.
        let row = store
            .run(|client| {
                Box::pin(async move {
                    client
                        .query_one(
                            "SELECT ciphertext, scheme, kek_reference FROM axond_secret \
                             WHERE secret_id = $1",
                            &[&staged.reference.secret.to_string()],
                        )
                        .await
                        .map_err(|error| unavailable("read the row back", &error))
                })
            })
            .await
            .expect("one row");
        let ciphertext: Vec<u8> = row.get("ciphertext");
        assert!(!ciphertext.windows(6).any(|window| window == b"sk-liv"));
        assert_eq!(
            row.get::<_, String>("scheme"),
            super::super::envelope::SCHEME
        );
        assert_eq!(row.get::<_, String>("kek_reference"), "AXOND_TEST_KEK");

        drop_schema(&schema).await;
    }

    /// Create, rotate, disable, roll back, revoke, and destroy — the operator
    /// sequence, against the real store.
    #[tokio::test]
    async fn the_lifecycle_is_what_the_domain_defines() {
        let Some((store, schema)) = store(12).await else {
            return;
        };
        let first = store
            .stage(owner(), SecretMaterial::new(PLAINTEXT.to_owned()))
            .await
            .expect("staging")
            .reference;
        store
            .transition(owner(), &first, SecretLifecycle::Active)
            .await
            .expect("staged material can be put in service");

        // Rotation stages the next version and leaves the serving one alone, so
        // both resolve: the overlap an uninterrupted rotation needs.
        let second = store
            .rotate(owner(), &first, SecretMaterial::new("sk-live-2".to_owned()))
            .await
            .expect("rotating");
        assert_eq!(second.reference, first.rotated());
        assert_eq!(second.lifecycle, SecretLifecycle::Staged);
        assert_eq!(
            store.resolve(owner(), &first).await.unwrap().expose(),
            PLAINTEXT
        );
        assert_eq!(
            store
                .resolve(owner(), &second.reference)
                .await
                .unwrap()
                .expose(),
            "sk-live-2"
        );
        // Rotating again from the stale base reference does not overwrite the
        // version somebody may already have published against.
        assert!(matches!(
            store
                .rotate(owner(), &first, SecretMaterial::new("sk-live-3".to_owned()))
                .await,
            Err(SecretError::Invalid(_))
        ));
        assert_eq!(
            store
                .resolve(owner(), &second.reference)
                .await
                .unwrap()
                .expose(),
            "sk-live-2"
        );

        // Disabling withholds material reversibly; the rollback is the move back.
        store
            .transition(owner(), &first, SecretLifecycle::Disabled)
            .await
            .expect("disabling");
        assert!(matches!(
            store.resolve(owner(), &first).await,
            Err(SecretError::Lifecycle {
                state: SecretLifecycle::Disabled,
                ..
            })
        ));
        assert!(!store.exists(owner(), &first).await.unwrap());
        assert_eq!(
            store
                .transition(owner(), &first, SecretLifecycle::Disabled)
                .await
                .expect("a retry is not a conflict"),
            LifecycleTransition::Unchanged(SecretLifecycle::Disabled)
        );
        store
            .transition(owner(), &first, SecretLifecycle::Active)
            .await
            .expect("a disabled version rolls back into service");
        assert_eq!(
            store.resolve(owner(), &first).await.unwrap().expose(),
            PLAINTEXT
        );

        // Revocation is irreversible, and tombstoning destroys the bytes.
        store
            .transition(owner(), &first, SecretLifecycle::Revoked)
            .await
            .expect("revoking");
        assert!(matches!(
            store
                .transition(owner(), &first, SecretLifecycle::Active)
                .await,
            Err(SecretError::Transition { .. })
        ));
        assert!(matches!(
            store
                .rotate(owner(), &first, SecretMaterial::new("x".to_owned()))
                .await,
            Err(SecretError::Invalid(_) | SecretError::Lifecycle { .. })
        ));
        store
            .transition(owner(), &first, SecretLifecycle::Tombstoned)
            .await
            .expect("tombstoning");
        assert_eq!(
            store.describe(owner(), &first).await.unwrap().lifecycle,
            SecretLifecycle::Tombstoned
        );
        let held = store
            .run(|client| {
                Box::pin(async move {
                    client
                        .query_one(
                            "SELECT ciphertext IS NOT NULL AS held, destroyed_at IS NOT NULL AS \
                             destroyed FROM axond_secret WHERE secret_id = $1 AND version = $2",
                            &[&first.secret.to_string(), &version_of(first)],
                        )
                        .await
                        .map_err(|error| unavailable("read the tombstoned row", &error))
                })
            })
            .await
            .expect("the row survives its material");
        assert!(!held.get::<_, bool>("held"), "tombstoning destroys bytes");
        assert!(held.get::<_, bool>("destroyed"));
        // The record of the compromise remains; the material does not.
        assert!(matches!(
            store.resolve(owner(), &first).await,
            Err(SecretError::Lifecycle {
                state: SecretLifecycle::Tombstoned,
                ..
            })
        ));
        // A rotation cannot resurrect a tombstoned secret's line.
        assert!(matches!(
            store
                .rotate(owner(), &first, SecretMaterial::new("sk-live-4".to_owned()))
                .await,
            Err(SecretError::Lifecycle { .. })
        ));

        drop_schema(&schema).await;
    }

    /// Another tenant — and another *project* of the same tenant — cannot resolve,
    /// describe, probe, rotate, or move somebody else's material, and cannot tell
    /// a foreign reference from one that was never stored.
    #[tokio::test]
    async fn material_is_isolated_by_owner() {
        let Some((store, schema)) = store(13).await else {
            return;
        };
        let mine = store
            .stage(owner(), SecretMaterial::new(PLAINTEXT.to_owned()))
            .await
            .expect("staging")
            .reference;

        for theirs in [
            SecretOwner::tenant(tenant_id(9)),
            SecretOwner::project(tenant_id(1), project_id(2)),
        ] {
            assert!(matches!(
                store.resolve(theirs, &mine).await,
                Err(SecretError::Ownership { .. })
            ));
            assert!(!store.exists(theirs, &mine).await.unwrap());
            assert!(matches!(
                store.describe(theirs, &mine).await,
                Err(SecretError::Ownership { .. })
            ));
            assert!(matches!(
                store
                    .rotate(theirs, &mine, SecretMaterial::new("sk-live-x".to_owned()))
                    .await,
                Err(SecretError::Ownership { .. })
            ));
            assert!(matches!(
                store
                    .transition(theirs, &mine, SecretLifecycle::Revoked)
                    .await,
                Err(SecretError::Ownership { .. })
            ));
            // Ownership is reported as absence to a caller: the category is what
            // an `/admin/v1` response is built from.
            assert_eq!(
                store.describe(theirs, &mine).await.unwrap_err().category(),
                FailureCategory::NotFound
            );
        }

        // A reference nobody stored answers the same way a foreign one does.
        let absent = SecretRef::first(SecretId::new(Uuid7Generator::new().next()));
        assert!(matches!(
            store.resolve(owner(), &absent).await,
            Err(SecretError::NotFound(_))
        ));
        assert!(!store.exists(owner(), &absent).await.unwrap());
        assert!(store.resolve(owner(), &mine).await.is_ok());

        drop_schema(&schema).await;
    }

    /// Material sealed under one KEK is not readable under another: the failure
    /// is `Corrupt`, so it pages an operator instead of being retried.
    #[tokio::test]
    async fn a_rotated_kek_cannot_unwrap_existing_material() {
        let Some((store, schema)) = store(14).await else {
            return;
        };
        let staged = store
            .stage(owner(), SecretMaterial::new(PLAINTEXT.to_owned()))
            .await
            .expect("staging")
            .reference;

        let dsn = postgres_dsn().expect("a configured store");
        let rotated = PostgresSecrets::connect(
            &dsn,
            SecretStoreSettings {
                schema: Some(schema.clone()),
                create_table: false,
                ..SecretStoreSettings::default()
            },
            kek(15),
        )
        .await
        .expect("a store with a different key still connects");
        let error = rotated
            .resolve(owner(), &staged)
            .await
            .expect_err("material does not open under another key");
        assert!(matches!(error, SecretError::Unwrap { .. }));
        assert_eq!(error.category(), FailureCategory::Corrupt);
        assert!(!error.to_string().contains("sk-"), "{error}");

        drop_schema(&schema).await;
    }

    /// Empty material is refused before anything is sealed or written: an empty
    /// provider key is a credential that fails at the provider, in production,
    /// with no local diagnosis.
    #[tokio::test]
    async fn empty_material_is_refused() {
        let Some((store, schema)) = store(16).await else {
            return;
        };
        assert!(matches!(
            store
                .stage(owner(), SecretMaterial::new(String::new()))
                .await,
            Err(SecretError::Invalid(_))
        ));
        let staged = store
            .stage(owner(), SecretMaterial::new(PLAINTEXT.to_owned()))
            .await
            .expect("staging")
            .reference;
        assert!(matches!(
            store
                .rotate(owner(), &staged, SecretMaterial::new(String::new()))
                .await,
            Err(SecretError::Invalid(_))
        ));

        drop_schema(&schema).await;
    }

    /// A database that refuses to create the table has answered, so the answer is
    /// a refusal with the grant to fix — not an outage the replica should retry
    /// until somebody notices the message names the wrong problem.
    #[tokio::test]
    async fn a_schema_the_table_cannot_be_created_in_is_refused_not_an_outage() {
        let Some(dsn) = postgres_dsn() else {
            return;
        };
        // A search path naming no existing schema: `CREATE TABLE` has nowhere to
        // go and Postgres says so with a `SQLSTATE`, which is the same shape as
        // the privilege failure an operator actually hits.
        let error = PostgresSecrets::connect(
            &dsn,
            SecretStoreSettings {
                schema: Some(format!(
                    "axond_absent_{}",
                    Uuid7Generator::new().next().to_string().replace('-', "")
                )),
                create_table: true,
                ..SecretStoreSettings::default()
            },
            kek(19),
        )
        .await
        .expect_err("the table cannot be created there");

        assert_eq!(error.category(), FailureCategory::Denied);
        let message = error.to_string();
        assert!(message.contains("create_table"), "{message}");
    }

    /// Rotation from a revoked base is permitted, and deliberately: it mints a
    /// fresh version rather than returning the revoked one to service, which is
    /// how an operator replaces material they have just withdrawn. The revoked
    /// version stays revoked and stays unresolvable.
    #[tokio::test]
    async fn rotating_from_a_revoked_version_mints_a_successor_and_leaves_it_revoked() {
        let Some((store, schema)) = store(23).await else {
            return;
        };
        let staged = store
            .stage(owner(), SecretMaterial::new(PLAINTEXT.to_owned()))
            .await
            .expect("a staged version");
        store
            .transition(owner(), &staged.reference, SecretLifecycle::Revoked)
            .await
            .expect("a withdrawal");

        let rotated = store
            .rotate(
                owner(),
                &staged.reference,
                SecretMaterial::new("sk-replacement".to_owned()),
            )
            .await
            .expect("a replacement for withdrawn material");

        assert_eq!(rotated.reference, staged.reference.rotated());
        assert_eq!(rotated.lifecycle, SecretLifecycle::Staged);
        assert_eq!(
            store
                .describe(owner(), &staged.reference)
                .await
                .expect("the base version")
                .lifecycle,
            SecretLifecycle::Revoked,
            "the withdrawn version does not come back"
        );
        assert!(matches!(
            store.resolve(owner(), &staged.reference).await,
            Err(SecretError::Lifecycle { .. })
        ));

        drop_schema(&schema).await;
    }

    /// An operator who applies the DDL out of band gets a refusal rather than a
    /// schema change, and a missing table is named as the thing to fix.
    #[tokio::test]
    async fn a_missing_table_is_refused_rather_than_created() {
        let Some(dsn) = postgres_dsn() else {
            return;
        };
        let schema = format!(
            "axond_secret_test_{}",
            Uuid7Generator::new().next().to_string().replace('-', "")
        );
        let (client, connection) = tokio_postgres::connect(&dsn, crate::usage::tls_connector())
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .expect("create the test schema");

        let error = PostgresSecrets::connect(
            &dsn,
            SecretStoreSettings {
                schema: Some(schema.clone()),
                create_table: false,
                ..SecretStoreSettings::default()
            },
            kek(17),
        )
        .await
        .expect_err("an empty schema with no permission to create the table");
        assert_eq!(error.category(), FailureCategory::Denied);
        assert!(error.to_string().contains("secret_store_v1.sql"), "{error}");

        drop_schema(&schema).await;
    }
}
