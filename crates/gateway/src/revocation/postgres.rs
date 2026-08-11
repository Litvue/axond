use std::time::{Duration, SystemTime};
use std::{future::Future, pin::Pin};

use async_trait::async_trait;
use tokio_postgres::{Client, Config};

use super::{RevocationError, RevocationStore, unavailable, validate_expiry};
use crate::config::StoreUnavailable;
use crate::usage::validate_table_name;

// Embedded from the package-local copy of `ops/postgres/revocation_v1.sql`; see
// `tests/shipped_ddl.rs`, which gates the two copies against drift.
const SCHEMA_DDL: &str = include_str!("../../sql/revocation_v1.sql");

pub struct PostgresRevocation {
    table: String,
    config: Config,
    timeout: Duration,
    on_unavailable: StoreUnavailable,
    client: tokio::sync::Mutex<Option<Client>>,
}

impl PostgresRevocation {
    pub async fn connect(
        dsn: &str,
        table: &str,
        create_table: bool,
        timeout: Duration,
        connect_timeout: Duration,
        on_unavailable: StoreUnavailable,
    ) -> Result<Self, RevocationError> {
        validate_table_name(table).map_err(RevocationError::Invalid)?;
        let mut config: Config = dsn
            .parse()
            .map_err(|e| RevocationError::Invalid(format!("unparsable DSN: {e}")))?;
        config.connect_timeout(connect_timeout);
        config.application_name(crate::telemetry::SERVICE_NAME);
        let store = Self {
            table: table.to_owned(),
            config,
            timeout,
            on_unavailable,
            client: tokio::sync::Mutex::new(None),
        };
        let mut client = tokio::time::timeout(connect_timeout, store.connect_client())
            .await
            .map_err(|_| RevocationError::Startup {
                backend: "postgres",
                message: "connection timed out".to_owned(),
            })?
            .map_err(|e| RevocationError::Startup {
                backend: "postgres",
                message: format!("connection failed: {e}"),
            })?;
        if create_table {
            let transaction = client
                .transaction()
                .await
                .map_err(|e| startup_error("begin schema transaction", e))?;
            let lock_key = advisory_lock_key(table);
            transaction
                .query_one("SELECT pg_advisory_xact_lock($1::bigint)", &[&lock_key])
                .await
                .map_err(|e| startup_error("acquire schema lock", e))?;
            transaction
                .batch_execute(&store.schema_ddl())
                .await
                .map_err(|e| startup_error("create table", e))?;
            transaction
                .commit()
                .await
                .map_err(|e| startup_error("commit schema transaction", e))?;
        }
        *store.client.lock().await = Some(client);
        Ok(store)
    }

    fn schema_ddl(&self) -> String {
        let index_prefix = self.table.rsplit('.').next().unwrap_or(&self.table);
        SCHEMA_DDL
            .replace("axond_revocation_expires_at_idx", "\u{1}")
            .replace("axond_revocation", &self.table)
            .replace('\u{1}', &format!("{index_prefix}_expires_at_idx"))
    }

    async fn connect_client(&self) -> Result<Client, tokio_postgres::Error> {
        let (client, connection) = self.config.connect(crate::usage::tls_connector()).await?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::warn!(%error, "postgres revocation connection closed");
            }
        });
        Ok(client)
    }

    async fn run<T>(
        &self,
        operation: impl for<'a> FnOnce(
            &'a mut Client,
        ) -> Pin<
            Box<dyn Future<Output = Result<T, tokio_postgres::Error>> + Send + 'a>,
        >,
    ) -> Result<T, RevocationError> {
        let mut guard = self.client.lock().await;
        if guard.as_ref().is_none_or(Client::is_closed) {
            *guard =
                Some(
                    self.connect_client()
                        .await
                        .map_err(|e| RevocationError::Unavailable {
                            backend: "postgres",
                            message: e.to_string(),
                        })?,
                );
        }
        let result =
            tokio::time::timeout(self.timeout, operation(guard.as_mut().expect("connected")))
                .await
                .map_err(|_| RevocationError::Unavailable {
                    backend: "postgres",
                    message: "operation timed out".to_owned(),
                })?
                .map_err(|e| RevocationError::Unavailable {
                    backend: "postgres",
                    message: e.to_string(),
                });
        if result.is_err() {
            *guard = None;
        }
        result
    }
}

fn advisory_lock_key(table: &str) -> i64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in table.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash as i64
}

fn startup_error(operation: &str, error: tokio_postgres::Error) -> RevocationError {
    let message = match error.as_db_error() {
        Some(db_error) => format!(
            "{operation} failed: {} (SQLSTATE {})",
            db_error.message(),
            db_error.code().code()
        ),
        None => format!("{operation} failed: {error}"),
    };
    RevocationError::Startup {
        backend: "postgres",
        message,
    }
}

#[async_trait]
impl RevocationStore for PostgresRevocation {
    fn name(&self) -> &'static str {
        "postgres"
    }

    async fn is_revoked(&self, jti: &str) -> Result<bool, RevocationError> {
        let table = self.table.clone();
        let jti = jti.to_owned();
        match self
            .run(|client: &mut Client| {
                Box::pin(async move {
                    let row = client
                        .query_opt(
                            &format!("SELECT 1 FROM {table} WHERE jti = $1 AND expires_at > now()"),
                            &[&jti],
                        )
                        .await?;
                    Ok(row.is_some())
                })
            })
            .await
        {
            Ok(value) => Ok(value),
            Err(error) => unavailable(self.on_unavailable, "postgres", error),
        }
    }

    async fn revoke(&self, jti: &str, expires_at: SystemTime) -> Result<(), RevocationError> {
        validate_expiry(expires_at)?;
        let table = self.table.clone();
        let jti = jti.to_owned();
        self.run(|client: &mut Client| {
            Box::pin(async move {
                client
                    .execute(
                        &format!(
                            "INSERT INTO {table} (jti, expires_at) VALUES ($1::text, $2) \
                             ON CONFLICT (jti) DO UPDATE \
                             SET expires_at = GREATEST({table}.expires_at, EXCLUDED.expires_at)"
                        ),
                        &[&jti, &expires_at],
                    )
                    .await?;
                Ok(())
            })
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::UNIX_EPOCH;

    #[test]
    fn schema_ddl_keeps_index_name_unqualified_for_schema_tables() {
        let store = PostgresRevocation {
            table: "tenant.axond_revocation".to_owned(),
            config: "host=localhost".parse().expect("dsn"),
            timeout: Duration::from_millis(1),
            on_unavailable: StoreUnavailable::Deny,
            client: tokio::sync::Mutex::new(None),
        };
        let ddl = store.schema_ddl();
        assert!(ddl.contains("tenant.axond_revocation"));
        assert!(ddl.contains("axond_revocation_expires_at_idx"));
        assert!(!ddl.contains("tenant.axond_revocation_expires_at_idx"));
    }

    #[tokio::test]
    async fn concurrent_create_table_boots_are_serialized() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let table = format!(
            "axond_revocation_concurrent_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        );
        let first_dsn = dsn.clone();
        let first_table = table.clone();
        let second_dsn = dsn;
        let second_table = table;
        let first = PostgresRevocation::connect(
            &first_dsn,
            &first_table,
            true,
            Duration::from_millis(500),
            Duration::from_secs(5),
            StoreUnavailable::Deny,
        );
        let second = PostgresRevocation::connect(
            &second_dsn,
            &second_table,
            true,
            Duration::from_millis(500),
            Duration::from_secs(5),
            StoreUnavailable::Deny,
        );
        let (first, second) = tokio::join!(first, second);
        first.expect("first concurrent boot");
        second.expect("second concurrent boot");
    }

    #[tokio::test]
    async fn two_connections_share_revocations_and_expiry_is_honored() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let table = format!(
            "axond_revocation_test_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        );
        let first = PostgresRevocation::connect(
            &dsn,
            &table,
            true,
            Duration::from_millis(500),
            Duration::from_secs(5),
            StoreUnavailable::Deny,
        )
        .await
        .expect("connect");
        let second = PostgresRevocation::connect(
            &dsn,
            &table,
            false,
            Duration::from_millis(500),
            Duration::from_secs(5),
            StoreUnavailable::Deny,
        )
        .await
        .expect("connect");
        first
            .revoke("replica-jti", SystemTime::now() + Duration::from_secs(30))
            .await
            .expect("revoke");
        assert!(second.is_revoked("replica-jti").await.expect("read"));
        let client = first.client.lock().await;
        client
            .as_ref()
            .expect("client")
            .execute(
                &format!("INSERT INTO {table} (jti, expires_at) VALUES ($1, $2)"),
                &[&"expired-jti", &UNIX_EPOCH],
            )
            .await
            .expect("expired row");
        assert!(
            !second
                .is_revoked("expired-jti")
                .await
                .expect("expired read")
        );
    }
}
