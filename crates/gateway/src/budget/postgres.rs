//! Shared budget state in Postgres, for deployments that already run one and
//! would rather not add Redis.
//!
//! Both operations run in a transaction that takes a row lock on the budget's
//! spend row (`SELECT ... FOR UPDATE`), so concurrent reservations for one key
//! serialize: the compare and the insert cannot interleave across replicas, and
//! a cap is never double-spent. Reservations live in their own table with a
//! deadline, and a reserve reclaims the expired ones for its key first, so a
//! replica that dies mid-request cannot leak a hold.
//!
//! The schema lands in an adopter's own database, so it is treated as an
//! interface: it ships as [`crates/gateway/sql/budget_v1.sql`](../../sql/budget_v1.sql)
//! and a change to the row shape is a new versioned file rather than an edit.

use std::time::Duration;

use async_trait::async_trait;
use tokio_postgres::{Client, Config};

use super::{Admission, BudgetError, BudgetKey, BudgetStore, Denial, Reservation, SharedSettings};
use crate::usage::validate_table_name;

const BACKEND: &str = "postgres";

/// The DDL for the current schema version, shared with operators who apply it
/// themselves.
const SCHEMA_DDL: &str = include_str!("../../sql/budget_v1.sql");

/// The table name the shipped DDL uses; substituted when another is configured.
const DEFAULT_TABLE: &str = "axond_budget";

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct PostgresBudgetSettings {
    /// Base table name. The reservation table is `<table>_reservation`.
    pub table: String,
    /// Apply the shipped DDL at boot. Off by default: in most deployments the
    /// gateway's role has no DDL rights.
    pub create_table: bool,
    pub shared: SharedSettings,
}

pub struct PostgresBudget {
    table: String,
    settings: SharedSettings,
    config: Config,
    /// Replaced after any failure, so a broken connection is not reused.
    client: tokio::sync::Mutex<Option<Client>>,
}

impl PostgresBudget {
    pub async fn connect(dsn: &str, settings: PostgresBudgetSettings) -> Result<Self, BudgetError> {
        validate_table_name(&settings.table)
            .map_err(|message| BudgetError::invalid(BACKEND, message))?;
        let mut config: Config = dsn
            .parse()
            .map_err(|e| BudgetError::invalid(BACKEND, format!("unparsable DSN: {e}")))?;
        config.connect_timeout(CONNECT_TIMEOUT);
        config.application_name(crate::telemetry::SERVICE_NAME);

        let store = Self {
            table: settings.table,
            settings: settings.shared,
            config,
            client: tokio::sync::Mutex::new(None),
        };
        let client = store.connect_client().await?;
        if settings.create_table {
            client.batch_execute(&store.schema_ddl()).await?;
        }
        *store.client.lock().await = Some(client);
        Ok(store)
    }

    /// The shipped DDL, retargeted at the configured table. Index names carry
    /// the unqualified table name because an index lives in its table's schema
    /// and its name may not be qualified.
    fn schema_ddl(&self) -> String {
        let index_prefix = self.table.rsplit('.').next().unwrap_or(&self.table);
        SCHEMA_DDL
            .replace(&format!("{DEFAULT_TABLE}_reservation_"), "\u{1}\u{1}")
            .replace(&format!("{DEFAULT_TABLE}_reservation"), "\u{2}")
            .replace(DEFAULT_TABLE, &self.table)
            .replace("\u{2}", &self.reservation_table())
            .replace("\u{1}\u{1}", &format!("{index_prefix}_reservation_"))
    }

    fn reservation_table(&self) -> String {
        format!("{}_reservation", self.table)
    }

    async fn connect_client(&self) -> Result<Client, tokio_postgres::Error> {
        let (client, connection) = self.config.connect(crate::usage::tls_connector()).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!(error = %e, "postgres budget connection closed");
            }
        });
        Ok(client)
    }

    /// Runs one operation on the store's single connection, holding the lock for
    /// its whole duration so concurrent requests queue on the connection rather
    /// than each opening one of their own. A failed operation drops the
    /// connection, so the next caller reconnects.
    async fn run<T>(
        &self,
        operation: impl AsyncFnOnce(&mut Client) -> Result<T, tokio_postgres::Error>,
    ) -> Result<T, tokio_postgres::Error> {
        let mut guard = self.client.lock().await;
        if guard.as_ref().is_none_or(Client::is_closed) {
            *guard = Some(self.connect_client().await?);
        }
        let client = guard.as_mut().expect("connected above");
        let result = operation(client).await;
        if result.is_err() {
            *guard = None;
        }
        result
    }

    /// The admission decision, in one transaction. Returns whether the estimate
    /// fits; the reservation row is inserted in the same transaction so the
    /// answer cannot be stale by the time it is used.
    async fn try_hold(
        &self,
        client: &mut Client,
        key: &BudgetKey,
        reservation: &Reservation,
    ) -> Result<bool, tokio_postgres::Error> {
        let table = &self.table;
        let reservations = self.reservation_table();
        let transaction = client.transaction().await?;

        transaction
            .execute(
                &format!(
                    "INSERT INTO {table} (namespace, subject) VALUES ($1, $2)
                     ON CONFLICT (namespace, subject) DO NOTHING"
                ),
                &[&key.namespace, &key.subject],
            )
            .await?;
        // The row lock is the serialization point: every reservation for this
        // key queues behind it, across replicas.
        let spent: i64 = transaction
            .query_one(
                &format!(
                    "SELECT spent_microdollars FROM {table}
                     WHERE namespace = $1 AND subject = $2 FOR UPDATE"
                ),
                &[&key.namespace, &key.subject],
            )
            .await?
            .get(0);
        transaction
            .execute(
                &format!(
                    "DELETE FROM {reservations}
                     WHERE namespace = $1 AND subject = $2 AND expires_at <= now()"
                ),
                &[&key.namespace, &key.subject],
            )
            .await?;
        let held: i64 = transaction
            .query_one(
                &format!(
                    "SELECT COALESCE(SUM(amount_microdollars), 0)::bigint FROM {reservations}
                     WHERE namespace = $1 AND subject = $2"
                ),
                &[&key.namespace, &key.subject],
            )
            .await?
            .get(0);

        let amount = bigint(reservation.estimate_microdollars);
        let limit = bigint(self.settings.limit_microdollars);
        if spent.saturating_add(held).saturating_add(amount) > limit {
            transaction.rollback().await?;
            return Ok(false);
        }
        let ttl_ms = bigint(self.settings.reservation_ttl.as_millis() as u64);
        transaction
            .execute(
                &format!(
                    "INSERT INTO {reservations}
                         (id, namespace, subject, amount_microdollars, expires_at)
                     VALUES ($1, $2, $3, $4, now() + ($5::bigint * interval '1 millisecond'))"
                ),
                &[
                    &reservation.id,
                    &key.namespace,
                    &key.subject,
                    &amount,
                    &ttl_ms,
                ],
            )
            .await?;
        transaction.commit().await?;
        Ok(true)
    }

    /// Release the hold and add the measured spend atomically, so a settlement
    /// can never charge without releasing or release without charging.
    async fn commit_spend(
        &self,
        client: &mut Client,
        key: &BudgetKey,
        reservation: &Reservation,
        actual_microdollars: u64,
    ) -> Result<(), tokio_postgres::Error> {
        let table = &self.table;
        let reservations = self.reservation_table();
        let transaction = client.transaction().await?;
        // The spend row is taken before the reservation row, the same order
        // `try_hold` takes them, so a settlement and a reserve on one key cannot
        // deadlock against each other.
        transaction
            .execute(
                &format!(
                    "INSERT INTO {table} (namespace, subject, spent_microdollars)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (namespace, subject) DO UPDATE
                     SET spent_microdollars = {table}.spent_microdollars + $3,
                         updated_at = now()"
                ),
                &[&key.namespace, &key.subject, &bigint(actual_microdollars)],
            )
            .await?;
        transaction
            .execute(
                &format!("DELETE FROM {reservations} WHERE id = $1"),
                &[&reservation.id],
            )
            .await?;
        transaction.commit().await
    }
}

#[async_trait]
impl BudgetStore for PostgresBudget {
    fn name(&self) -> &'static str {
        "postgres"
    }

    async fn reserve(&self, key: &BudgetKey, estimated_microdollars: u64) -> Admission {
        let reservation = Reservation {
            id: Reservation::next_id(),
            estimate_microdollars: estimated_microdollars,
        };
        match self
            .run(async |client| self.try_hold(client, key, &reservation).await)
            .await
        {
            Ok(true) => Admission::Allowed(reservation),
            Ok(false) => Admission::Denied(Denial::Exceeded),
            Err(e) => self.settings.unavailable.admission(BACKEND, &e),
        }
    }

    /// A settlement that cannot reach Postgres leaves the hold to expire on its
    /// own deadline, rather than blocking the request path on a retry.
    async fn settle(&self, key: &BudgetKey, reservation: &Reservation, actual_microdollars: u64) {
        if reservation.id.is_empty() {
            return;
        }
        match self
            .run(async |client| {
                self.commit_spend(client, key, reservation, actual_microdollars)
                    .await
            })
            .await
        {
            Ok(()) => {}
            Err(e) => tracing::error!(
                error = %e,
                namespace = %key.namespace,
                actual_microdollars,
                "budget settlement was lost; the reservation expires on its own deadline"
            ),
        }
    }
}

/// Micro-dollars are `u64` in the gateway and `bigint` on the wire, so an
/// implausible value saturates rather than wrapping into a negative charge.
fn bigint(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::super::UnavailablePolicy;
    use super::super::tests::key;
    use super::*;

    fn store(table: &str) -> PostgresBudget {
        PostgresBudget {
            table: table.to_owned(),
            settings: settings(1_000),
            config: "host=localhost".parse().expect("static dsn"),
            client: tokio::sync::Mutex::new(None),
        }
    }

    fn settings(limit: u64) -> SharedSettings {
        SharedSettings {
            limit_microdollars: limit,
            reservation_ttl: Duration::from_secs(300),
            unavailable: UnavailablePolicy::Deny,
        }
    }

    #[test]
    fn the_shipped_ddl_declares_every_column_the_store_writes() {
        for column in [
            "namespace",
            "subject",
            "spent_microdollars",
            "amount_microdollars",
            "expires_at",
        ] {
            assert!(
                SCHEMA_DDL.contains(column),
                "column `{column}` is written but not declared in budget_v1.sql"
            );
        }
    }

    #[test]
    fn the_ddl_is_retargeted_at_the_configured_tables() {
        let ddl = store("caps").schema_ddl();
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS caps ("));
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS caps_reservation ("));
        assert!(ddl.contains("caps_reservation_scope_idx"));
        assert!(!ddl.contains(DEFAULT_TABLE));
    }

    /// An index lives in its table's schema and its *name* may not be
    /// qualified, so the qualifier belongs to the table reference only.
    #[test]
    fn a_schema_qualified_table_keeps_its_index_names_unqualified() {
        let ddl = store("billing.axond_budget").schema_ddl();
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS billing.axond_budget ("));
        assert!(ddl.contains("ON billing.axond_budget_reservation"));
        assert!(!ddl.contains("billing.axond_budget_reservation_scope_idx"));
        assert!(ddl.contains("IF NOT EXISTS axond_budget_reservation_scope_idx"));
    }

    #[test]
    fn micro_dollars_saturate_rather_than_wrapping_negative() {
        assert_eq!(bigint(u64::MAX), i64::MAX);
        assert_eq!(bigint(640), 640);
    }

    /// Exercises the real thing when a database is offered. Skipped (not
    /// failed) otherwise, so the suite stays runnable with no datastore.
    #[tokio::test]
    async fn two_stores_sharing_one_database_enforce_a_single_cap() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let table = "axond_budget_test";
        let replica_a = PostgresBudget::connect(
            &dsn,
            PostgresBudgetSettings {
                table: table.to_owned(),
                create_table: true,
                shared: settings(1_000),
            },
        )
        .await
        .expect("connect");
        {
            let guard = replica_a.client.lock().await;
            let client = guard.as_ref().expect("connected");
            for statement in [
                format!("TRUNCATE {table}"),
                format!("TRUNCATE {table}_reservation"),
            ] {
                client.execute(&statement, &[]).await.expect("truncate");
            }
        }
        let replica_b = PostgresBudget::connect(
            &dsn,
            PostgresBudgetSettings {
                table: table.to_owned(),
                create_table: false,
                shared: settings(1_000),
            },
        )
        .await
        .expect("connect");
        let k = key();

        let Admission::Allowed(held) = replica_a.reserve(&k, 700).await else {
            panic!("the first reservation must be admitted");
        };
        // The other replica sees the outstanding hold.
        assert_eq!(
            replica_b.reserve(&k, 700).await,
            Admission::Denied(Denial::Exceeded)
        );

        replica_a.settle(&k, &held, 100).await;
        let Admission::Allowed(second) = replica_b.reserve(&k, 700).await else {
            panic!("releasing the unused estimate must free it");
        };
        replica_b.settle(&k, &second, 700).await;

        // 100 + 700 settled leaves no room for 300.
        assert_eq!(
            replica_a.reserve(&k, 300).await,
            Admission::Denied(Denial::Exceeded)
        );
    }

    #[tokio::test]
    async fn an_unreachable_database_fails_at_boot() {
        let err = PostgresBudget::connect(
            "host=127.0.0.1 port=1 user=axond connect_timeout=1",
            PostgresBudgetSettings {
                table: DEFAULT_TABLE.to_owned(),
                create_table: false,
                shared: settings(1),
            },
        )
        .await
        .err()
        .expect("an unreachable database must fail at boot");
        assert!(matches!(err, BudgetError::Postgres(_)), "{err:?}");
    }

    #[tokio::test]
    async fn a_table_name_that_could_carry_sql_is_rejected() {
        let err = PostgresBudget::connect(
            "host=127.0.0.1",
            PostgresBudgetSettings {
                table: "caps; drop table users".to_owned(),
                create_table: false,
                shared: settings(1),
            },
        )
        .await
        .err()
        .expect("an unsafe table name must be rejected");
        assert!(matches!(err, BudgetError::Invalid { .. }), "{err:?}");
    }
}
