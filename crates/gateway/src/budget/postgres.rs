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
//! interface: it ships as [`ops/postgres/budget_v1.sql`](../../../../ops/postgres/budget_v1.sql)
//! and a change to the row shape is a new versioned file rather than an edit.
//!
//! `namespace_limit_microdollars` adds a second scope, and with it
//! [`ops/postgres/budget_v2.sql`](../../../../ops/postgres/budget_v2.sql): a
//! namespace spend table, an index for namespace-wide reservation cleanup, and a
//! backfill that seeds each namespace total from the subject rows already there,
//! so enabling the cap does not hand every tenant a fresh budget. Both
//! transactions then take the **namespace row first and the subject row second**,
//! so a reserve and a settlement on one namespace cannot deadlock, and one
//! reservation row is still the whole hold: it is inserted and deleted once, and
//! the settlement charges both scopes in the same transaction.

use std::time::Duration;

use async_trait::async_trait;
use tokio_postgres::{Client, Config};

use super::{
    Admission, BudgetError, BudgetKey, BudgetStore, Denial, ExceededScope, Reservation,
    SharedSettings,
};
use crate::telemetry::metrics;
use crate::usage::validate_table_name;

const BACKEND: &str = "postgres";

/// The DDL for the current schema version, shared with operators who apply it
/// themselves.
// Embedded from the package-local copy of `ops/postgres/budget_v1.sql`; see
// `tests/shipped_ddl.rs`, which gates the two copies against drift.
const SCHEMA_DDL: &str = include_str!("../../sql/budget_v1.sql");

/// The additive DDL the namespace cap needs, applied on top of the v1 schema.
const SCHEMA_DDL_V2: &str = include_str!("../../../../ops/postgres/budget_v2.sql");

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
            client.batch_execute(&store.schema_ddl(SCHEMA_DDL)).await?;
            if store.settings.enforces_namespace_cap() {
                client
                    .batch_execute(&store.schema_ddl(SCHEMA_DDL_V2))
                    .await?;
            }
        }
        if store.settings.enforces_namespace_cap() {
            store.require_namespace_schema(&client).await?;
        }
        *store.client.lock().await = Some(client);
        Ok(store)
    }

    /// A shipped DDL file, retargeted at the configured tables. Index names
    /// carry the unqualified table name because an index lives in its table's
    /// schema and its name may not be qualified.
    fn schema_ddl(&self, ddl: &str) -> String {
        let index_prefix = self.table.rsplit('.').next().unwrap_or(&self.table);
        // Longest names first, into placeholders, so a substitution cannot chew
        // into a suffix that has not been replaced yet.
        ddl.replace(&format!("{DEFAULT_TABLE}_reservation_"), "\u{1}")
            .replace(&format!("{DEFAULT_TABLE}_namespace_"), "\u{2}")
            .replace(&format!("{DEFAULT_TABLE}_reservation"), "\u{3}")
            .replace(&format!("{DEFAULT_TABLE}_namespace"), "\u{4}")
            .replace(DEFAULT_TABLE, &self.table)
            .replace("\u{3}", &self.reservation_table())
            .replace("\u{4}", &self.namespace_table())
            .replace("\u{1}", &format!("{index_prefix}_reservation_"))
            .replace("\u{2}", &format!("{index_prefix}_namespace_"))
    }

    fn reservation_table(&self) -> String {
        format!("{}_reservation", self.table)
    }

    fn namespace_table(&self) -> String {
        format!("{}_namespace", self.table)
    }

    /// Prove the v2 schema is there *and* backfilled before serving traffic: a
    /// missing namespace row reads as zero spend, which would silently hand a
    /// tenant a second budget.
    async fn require_namespace_schema(&self, client: &Client) -> Result<(), BudgetError> {
        let namespaces = self.namespace_table();
        let table = &self.table;
        client
            .execute(&format!("SELECT 1 FROM {namespaces} WHERE false"), &[])
            .await
            .map_err(|e| {
                BudgetError::invalid(
                    BACKEND,
                    format!(
                        "`namespace_limit_microdollars` needs the v2 schema, but `{namespaces}` is \
                         unreadable ({e}). Apply `ops/postgres/budget_v2.sql` (or set \
                         `create_table = true`) before enabling the namespace cap."
                    ),
                )
            })?;
        let unbackfilled: i64 = client
            .query_one(
                &format!(
                    "SELECT count(*)::bigint FROM (
                         SELECT namespace FROM {table}
                         EXCEPT SELECT namespace FROM {namespaces}
                     ) AS missing"
                ),
                &[],
            )
            .await?
            .get(0);
        if unbackfilled > 0 {
            return Err(BudgetError::invalid(
                BACKEND,
                format!(
                    "{unbackfilled} namespace(s) have spend in `{table}` but no row in \
                     `{namespaces}`: run the backfill at the bottom of \
                     `ops/postgres/budget_v2.sql` before serving traffic, or the namespace cap \
                     starts from zero."
                ),
            ));
        }
        Ok(())
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

    /// The admission decision, in one transaction. Returns which cap ran out, if
    /// either; the reservation row is inserted in the same transaction so the
    /// answer cannot be stale by the time it is used.
    async fn try_hold(
        &self,
        client: &mut Client,
        key: &BudgetKey,
        reservation: &Reservation,
    ) -> Result<Option<ExceededScope>, tokio_postgres::Error> {
        let table = &self.table;
        let namespaces = self.namespace_table();
        let reservations = self.reservation_table();
        let amount = bigint(reservation.estimate_microdollars);
        let transaction = client.transaction().await?;

        // The namespace row is taken first, and by both operations, so the two
        // scopes always lock in one order and cannot deadlock against each other.
        let namespace_spent = match self.settings.namespace_limit_microdollars {
            Some(_) => {
                transaction
                    .execute(
                        &format!(
                            "INSERT INTO {namespaces} (namespace) VALUES ($1)
                             ON CONFLICT (namespace) DO NOTHING"
                        ),
                        &[&key.namespace],
                    )
                    .await?;
                let spent: i64 = transaction
                    .query_one(
                        &format!(
                            "SELECT spent_microdollars FROM {namespaces}
                             WHERE namespace = $1 FOR UPDATE"
                        ),
                        &[&key.namespace],
                    )
                    .await?
                    .get(0);
                Some(spent)
            }
            None => None,
        };

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

        // With a namespace cap the cleanup and the held sums span every subject
        // in the namespace, which the v2 `(namespace, expires_at)` index serves.
        let (held, namespace_held) = if namespace_spent.is_some() {
            transaction
                .execute(
                    &format!(
                        "DELETE FROM {reservations}
                         WHERE namespace = $1 AND expires_at <= now()"
                    ),
                    &[&key.namespace],
                )
                .await?;
            let row = transaction
                .query_one(
                    &format!(
                        "SELECT
                             COALESCE(SUM(amount_microdollars)
                                 FILTER (WHERE subject = $2), 0)::bigint,
                             COALESCE(SUM(amount_microdollars), 0)::bigint
                         FROM {reservations} WHERE namespace = $1"
                    ),
                    &[&key.namespace, &key.subject],
                )
                .await?;
            (row.get(0), Some(row.get::<_, i64>(1)))
        } else {
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
            (held, None)
        };

        let limit = bigint(self.settings.limit_microdollars);
        if spent.saturating_add(held).saturating_add(amount) > limit {
            transaction.rollback().await?;
            return Ok(Some(ExceededScope::Subject));
        }
        // Nothing is written until both caps have room, so a denial cannot leave
        // one scope holding an estimate the other rejected.
        if let (Some(namespace_spent), Some(namespace_held), Some(namespace_limit)) = (
            namespace_spent,
            namespace_held,
            self.settings.namespace_limit_microdollars,
        ) && namespace_spent
            .saturating_add(namespace_held)
            .saturating_add(amount)
            > bigint(namespace_limit)
        {
            transaction.rollback().await?;
            return Ok(Some(ExceededScope::Namespace));
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
        Ok(None)
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
        let namespaces = self.namespace_table();
        let reservations = self.reservation_table();
        let charge = bigint(actual_microdollars);
        let transaction = client.transaction().await?;
        // Namespace row first, then the subject row, then the reservation row:
        // the same order `try_hold` takes them, so a settlement and a reserve on
        // one namespace cannot deadlock against each other. Both scopes are
        // charged in this one transaction, so neither can be charged alone.
        if self.settings.enforces_namespace_cap() {
            transaction
                .execute(
                    &format!(
                        "INSERT INTO {namespaces} (namespace, spent_microdollars)
                         VALUES ($1, $2)
                         ON CONFLICT (namespace) DO UPDATE
                         SET spent_microdollars = {namespaces}.spent_microdollars + $2,
                             updated_at = now()"
                    ),
                    &[&key.namespace, &charge],
                )
                .await?;
        }
        transaction
            .execute(
                &format!(
                    "INSERT INTO {table} (namespace, subject, spent_microdollars)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (namespace, subject) DO UPDATE
                     SET spent_microdollars = {table}.spent_microdollars + $3,
                         updated_at = now()"
                ),
                &[&key.namespace, &key.subject, &charge],
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
            Ok(None) => Admission::Allowed(reservation),
            Ok(Some(scope)) => {
                if scope == ExceededScope::Namespace {
                    metrics::record_budget_namespace_denial();
                    tracing::info!(
                        namespace = %key.namespace,
                        "namespace spend cap is exhausted; denying"
                    );
                }
                Admission::Denied(Denial::Exceeded)
            }
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
            namespace_limit_microdollars: None,
            reservation_ttl: Duration::from_secs(300),
            unavailable: UnavailablePolicy::Deny,
        }
    }

    fn namespace_settings(limit: u64, namespace_limit: u64) -> SharedSettings {
        SharedSettings {
            namespace_limit_microdollars: Some(namespace_limit),
            ..settings(limit)
        }
    }

    /// A store on the shared test database, with both scopes emptied first so
    /// the tests do not inherit each other's spend.
    async fn namespace_store(dsn: &str, table: &str, shared: SharedSettings) -> PostgresBudget {
        let store = PostgresBudget::connect(
            dsn,
            PostgresBudgetSettings {
                table: table.to_owned(),
                create_table: true,
                shared,
            },
        )
        .await
        .expect("connect");
        {
            let guard = store.client.lock().await;
            let client = guard.as_ref().expect("connected");
            for statement in [
                format!("TRUNCATE {table}"),
                format!("TRUNCATE {table}_reservation"),
                format!("TRUNCATE {table}_namespace"),
            ] {
                client.execute(&statement, &[]).await.expect("truncate");
            }
        }
        store
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
        let ddl = store("caps").schema_ddl(SCHEMA_DDL);
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS caps ("));
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS caps_reservation ("));
        assert!(ddl.contains("caps_reservation_scope_idx"));
        assert!(!ddl.contains(DEFAULT_TABLE));
    }

    /// The namespace DDL follows the same substitution, including the table the
    /// backfill reads and the index it adds.
    #[test]
    fn the_namespace_ddl_is_retargeted_at_the_configured_tables() {
        let ddl = store("caps").schema_ddl(SCHEMA_DDL_V2);
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS caps_namespace ("));
        assert!(ddl.contains("ON caps_reservation (namespace, expires_at)"));
        assert!(ddl.contains("INSERT INTO caps_namespace"));
        assert!(ddl.contains("FROM caps\n"));
        assert!(!ddl.contains(DEFAULT_TABLE));
    }

    /// An index lives in its table's schema and its *name* may not be
    /// qualified, so the qualifier belongs to the table reference only.
    #[test]
    fn a_schema_qualified_table_keeps_its_index_names_unqualified() {
        let ddl = store("billing.axond_budget").schema_ddl(SCHEMA_DDL);
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS billing.axond_budget ("));
        assert!(ddl.contains("ON billing.axond_budget_reservation"));
        assert!(!ddl.contains("billing.axond_budget_reservation_scope_idx"));
        assert!(ddl.contains("IF NOT EXISTS axond_budget_reservation_scope_idx"));

        let v2 = store("billing.axond_budget").schema_ddl(SCHEMA_DDL_V2);
        assert!(v2.contains("CREATE TABLE IF NOT EXISTS billing.axond_budget_namespace ("));
        assert!(v2.contains("IF NOT EXISTS axond_budget_reservation_namespace_expires_idx"));
        assert!(!v2.contains("billing.axond_budget_reservation_namespace_expires_idx"));
    }

    /// The shipped v1 file is an applied interface, so v2 is additive: it must
    /// not redefine what v1 owns.
    #[test]
    fn the_namespace_ddl_never_touches_the_v1_tables() {
        assert!(!SCHEMA_DDL_V2.contains("CREATE TABLE IF NOT EXISTS axond_budget ("));
        assert!(!SCHEMA_DDL_V2.contains("CREATE TABLE IF NOT EXISTS axond_budget_reservation ("));
        assert!(SCHEMA_DDL_V2.contains("ON CONFLICT (namespace) DO NOTHING"));
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
    async fn two_subjects_cannot_collectively_exceed_the_namespace_cap() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let store = namespace_store(
            &dsn,
            "axond_budget_ns_two_subjects_test",
            namespace_settings(1_000, 1_200),
        )
        .await;
        let first = BudgetKey {
            namespace: "acme".into(),
            subject: "first".into(),
        };
        let second = BudgetKey {
            namespace: "acme".into(),
            subject: "second".into(),
        };

        let Admission::Allowed(held) = store.reserve(&first, 800).await else {
            panic!("the first subject fits both caps");
        };
        assert_eq!(
            store.reserve(&second, 800).await,
            Admission::Denied(Denial::Exceeded)
        );
        store.settle(&first, &held, 800).await;
        // Settled spend counts the same as the hold did.
        assert_eq!(
            store.reserve(&second, 401).await,
            Admission::Denied(Denial::Exceeded)
        );
        let Admission::Allowed(fits) = store.reserve(&second, 400).await else {
            panic!("400 still fits under a 1200 namespace cap");
        };
        store.release(&second, &fits).await;
        // A release frees the estimate in both scopes.
        assert!(matches!(
            store.reserve(&second, 400).await,
            Admission::Allowed(_)
        ));
    }

    #[tokio::test]
    async fn namespaces_do_not_share_a_cap_and_the_subject_cap_still_binds() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let store = namespace_store(
            &dsn,
            "axond_budget_ns_isolation_test",
            namespace_settings(500, 1_000),
        )
        .await;
        let acme = BudgetKey {
            namespace: "acme".into(),
            subject: "s".into(),
        };
        let other = BudgetKey {
            namespace: "other".into(),
            subject: "s".into(),
        };

        let Admission::Allowed(held) = store.reserve(&acme, 500).await else {
            panic!("the subject cap has room");
        };
        // The subject's own cap binds first, even with namespace headroom.
        assert_eq!(
            store.reserve(&acme, 1).await,
            Admission::Denied(Denial::Exceeded)
        );
        store.settle(&acme, &held, 500).await;
        assert!(matches!(
            store.reserve(&other, 500).await,
            Admission::Allowed(_)
        ));
    }

    /// Two replicas, one database, one namespace cap — and enough distinct
    /// subjects that only a shared namespace ledger can hold the line.
    #[tokio::test]
    async fn two_replicas_enforce_one_namespace_cap_under_contention() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let table = "axond_budget_ns_replicas_test";
        let replica_a = namespace_store(&dsn, table, namespace_settings(1_000_000, 1_000)).await;
        let replica_b = PostgresBudget::connect(
            &dsn,
            PostgresBudgetSettings {
                table: table.to_owned(),
                create_table: false,
                shared: namespace_settings(1_000_000, 1_000),
            },
        )
        .await
        .expect("connect");

        let mut admitted = 0;
        for index in 0..40 {
            let store = if index % 2 == 0 {
                &replica_a
            } else {
                &replica_b
            };
            let k = BudgetKey {
                namespace: "acme".into(),
                subject: format!("subject-{index}"),
            };
            if let Admission::Allowed(held) = store.reserve(&k, 100).await {
                admitted += 1;
                store.settle(&k, &held, 100).await;
            }
        }
        assert_eq!(admitted, 10);
    }

    #[tokio::test]
    async fn an_expired_hold_frees_the_namespace_cap_too() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let mut expiring = namespace_settings(1_000, 1_000);
        expiring.reservation_ttl = Duration::from_millis(50);
        let store = namespace_store(&dsn, "axond_budget_ns_expiry_test", expiring).await;
        let died = BudgetKey {
            namespace: "acme".into(),
            subject: "died".into(),
        };
        let alive = BudgetKey {
            namespace: "acme".into(),
            subject: "alive".into(),
        };

        assert!(matches!(
            store.reserve(&died, 900).await,
            Admission::Allowed(_)
        ));
        assert_eq!(
            store.reserve(&alive, 900).await,
            Admission::Denied(Denial::Exceeded)
        );
        tokio::time::sleep(Duration::from_millis(80)).await;
        // The dead replica's hold is reclaimed for the namespace, not just for
        // the subject that made it.
        assert!(matches!(
            store.reserve(&alive, 900).await,
            Admission::Allowed(_)
        ));
    }

    /// The migration promise: turning the cap on carries existing subject spend
    /// into the namespace total instead of granting a fresh budget.
    #[tokio::test]
    async fn the_v2_backfill_carries_existing_subject_spend() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let table = "axond_budget_ns_backfill_test";
        // Spend under the v1 schema, as a gateway without the cap would.
        let v1 = namespace_store(&dsn, table, namespace_settings(1_000, 1_000)).await;
        {
            let guard = v1.client.lock().await;
            let client = guard.as_ref().expect("connected");
            client
                .execute(&format!("TRUNCATE {table}_namespace"), &[])
                .await
                .expect("forget the namespace totals");
            for subject in ["first", "second"] {
                client
                    .execute(
                        &format!(
                            "INSERT INTO {table} (namespace, subject, spent_microdollars)
                             VALUES ('acme', $1, 400)"
                        ),
                        &[&subject],
                    )
                    .await
                    .expect("v1 spend");
            }
        }
        drop(v1);

        // Un-backfilled state must not serve traffic: it would read zero.
        let err = PostgresBudget::connect(
            &dsn,
            PostgresBudgetSettings {
                table: table.to_owned(),
                create_table: false,
                shared: namespace_settings(1_000, 1_000),
            },
        )
        .await
        .err()
        .expect("un-backfilled state must fail at boot");
        assert!(format!("{err}").contains("budget_v2.sql"), "{err}");

        // `create_table = true` applies the shipped backfill.
        let store = PostgresBudget::connect(
            &dsn,
            PostgresBudgetSettings {
                table: table.to_owned(),
                create_table: true,
                shared: namespace_settings(1_000, 1_000),
            },
        )
        .await
        .expect("connect");
        let third = BudgetKey {
            namespace: "acme".into(),
            subject: "third".into(),
        };
        // 800 was already spent in the namespace, so only 200 is left.
        assert_eq!(
            store.reserve(&third, 201).await,
            Admission::Denied(Denial::Exceeded)
        );
        assert!(matches!(
            store.reserve(&third, 200).await,
            Admission::Allowed(_)
        ));
    }

    /// Applying it twice must not double-count or reset: the namespace row the
    /// request path maintains wins.
    #[tokio::test]
    async fn the_v2_backfill_is_idempotent() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let table = "axond_budget_ns_reapply_test";
        let store = namespace_store(&dsn, table, namespace_settings(1_000, 1_000)).await;
        let k = BudgetKey {
            namespace: "acme".into(),
            subject: "only".into(),
        };
        let Admission::Allowed(held) = store.reserve(&k, 600).await else {
            panic!("an empty namespace admits");
        };
        store.settle(&k, &held, 600).await;

        let reapplied = PostgresBudget::connect(
            &dsn,
            PostgresBudgetSettings {
                table: table.to_owned(),
                create_table: true,
                shared: namespace_settings(1_000, 1_000),
            },
        )
        .await
        .expect("re-applying the DDL is safe");
        assert_eq!(
            reapplied.reserve(&k, 401).await,
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
