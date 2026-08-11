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
const SCHEMA_DDL_V2: &str = include_str!("../../sql/budget_v2.sql");

/// The table name the shipped DDL uses; substituted when another is configured.
const DEFAULT_TABLE: &str = "axond_budget";

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// What a cap-aware replica tells the database about itself, on every connection.
/// The v2 fence trigger rejects spend and reservation writes from sessions that
/// have not said this, which is what stops a replica configured without the cap
/// from charging a subject while leaving the namespace total behind.
const NAMESPACE_CAP_DECLARATION: &str = "SET axond.budget_namespace_cap = 'on'";
/// The same declaration, scoped to one transaction. Sent inside every transaction
/// that writes, so the fence holds even when the session it was declared on is
/// not the backend the transaction runs on — a pooler in transaction mode can
/// route the two apart.
const NAMESPACE_CAP_DECLARATION_LOCAL: &str = "SET LOCAL axond.budget_namespace_cap = 'on'";

#[derive(Debug, Clone)]
pub struct PostgresBudgetSettings {
    /// Base table name. The reservation table is `<table>_reservation`.
    pub table: String,
    /// Apply the shipped DDL at boot. Off by default: in most deployments the
    /// gateway's role has no DDL rights.
    pub create_table: bool,
    pub shared: SharedSettings,
}

/// Which of the two relations the v2 fence actually covers. Answered per
/// relation because a partial fence is neither state's contract: a cap-enabled
/// replica needs both, and a cap-less one is excluded by either.
#[derive(Debug, Clone, Copy)]
struct FenceTargets {
    spend: bool,
    reservation: bool,
}

impl FenceTargets {
    fn complete(self) -> bool {
        self.spend && self.reservation
    }

    fn any(self) -> bool {
        self.spend || self.reservation
    }

    /// The named relations still missing the fence, for the boot error.
    fn unfenced(self, spend: &str, reservation: &str) -> Vec<String> {
        let mut missing = Vec::new();
        if !self.spend {
            missing.push(spend.to_owned());
        }
        if !self.reservation {
            missing.push(reservation.to_owned());
        }
        missing
    }
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
            // The v2 file is a migration, not idempotent boot DDL: it takes an
            // `EXCLUSIVE` lock on the spend table and ends with a whole-table
            // aggregate, all in one batch. Re-running it on every restart would
            // block every other replica's reserves and settlements for the
            // duration of that aggregate, so it runs only when the schema it
            // installs is not already there.
            if store.settings.enforces_namespace_cap()
                && !store.namespace_schema_ready(&client).await
            {
                client
                    .batch_execute(&store.schema_ddl(SCHEMA_DDL_V2))
                    .await?;
            }
        }
        if store.settings.enforces_namespace_cap() {
            store.require_namespace_schema(&client).await?;
        } else {
            store.require_no_namespace_fence(&client).await?;
        }
        *store.client.lock().await = Some(client);
        Ok(store)
    }

    /// A shipped DDL file, retargeted at the configured tables. Index names
    /// carry the unqualified table name because an index lives in its table's
    /// schema and its name may not be qualified.
    fn schema_ddl(&self, ddl: &str) -> String {
        let index_prefix = self.table.rsplit('.').next().unwrap_or(&self.table);
        // `batch_execute` already runs a multi-statement query as one implicit
        // transaction, so the file's own transaction control is redundant here
        // (and nested `BEGIN` only earns a warning).
        let ddl: String = ddl
            .lines()
            .filter(|line| !matches!(line.trim(), "BEGIN;" | "COMMIT;"))
            .map(|line| format!("{line}\n"))
            .collect();
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
        // The backfill and the fence are installed together, so a missing fence
        // means the applied file predates it — and without it a replica without
        // the cap could still write here, leaving this namespace total short.
        // Both relations are required: an unfenced reservation table lets a
        // cap-less replica hold budget the namespace never sees, and an unfenced
        // spend table lets it charge spend the namespace never sees.
        let fence = self.fence_targets(client).await?;
        if !fence.complete() {
            let unfenced = fence
                .unfenced(&self.table, &self.reservation_table())
                .join("`, `");
            return Err(BudgetError::invalid(
                BACKEND,
                format!(
                    "`{unfenced}` has no `{}` trigger, so nothing stops a replica configured \
                     without `namespace_limit_microdollars` from recording spend or holds that \
                     never reach the namespace total. Re-apply `ops/postgres/budget_v2.sql` (with \
                     the fleet stopped), which installs it on the spend table and the reservation \
                     table.",
                    self.fence_name()
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
        if self.settings.enforces_namespace_cap() {
            // Every connection, not just the first: the fence is per session, and
            // a reconnect after a failure gets a fresh one. Each writing
            // transaction re-declares it locally as well, for a pooler that may
            // not give this session's backend to that transaction.
            client.batch_execute(NAMESPACE_CAP_DECLARATION).await?;
        }
        Ok(client)
    }

    /// The name of the v2 fence trigger, which is also its function's name. The
    /// name is per-table by construction, so it is never the whole answer to
    /// whether a given relation is fenced; see [`Self::fence_targets`].
    fn fence_name(&self) -> String {
        let prefix = self.table.rsplit('.').next().unwrap_or(&self.table);
        format!("{prefix}_namespace_fence")
    }

    /// Whether everything the v2 file installs is already in place *and* in force:
    /// the namespace table, the fence on both relations, and a namespace total for
    /// every namespace with spend. That is exactly what the cap-enabled boot check
    /// demands, so anything it would reject reads as not ready and the file is
    /// applied; anything it would accept needs no lock and no backfill.
    async fn namespace_schema_ready(&self, client: &Client) -> bool {
        self.require_namespace_schema(client).await.is_ok()
    }

    /// Whether the fence is on the spend table and on the reservation table,
    /// answered per relation.
    ///
    /// Trigger names are unique only per table, so counting `pg_trigger` rows by
    /// name alone would accept a same-named trigger on some unrelated table — in
    /// another schema, or a custom-`table` deployment's — as proof that *these*
    /// two relations are fenced. Both are therefore resolved with `to_regclass`,
    /// exactly as configured (schema qualification included), and matched against
    /// `tgrelid`. A relation that does not exist resolves to `NULL`, which matches
    /// nothing and so reads as unfenced.
    async fn fence_targets(&self, client: &Client) -> Result<FenceTargets, tokio_postgres::Error> {
        let row = client
            .query_one(
                "SELECT
                     coalesce(bool_or(tgrelid = to_regclass($2)), false) AS spend,
                     coalesce(bool_or(tgrelid = to_regclass($3)), false) AS reservation
                 FROM pg_trigger
                 WHERE tgname = $1 AND NOT tgisinternal",
                &[&self.fence_name(), &self.table, &self.reservation_table()],
            )
            .await?;
        Ok(FenceTargets {
            spend: row.get("spend"),
            reservation: row.get("reservation"),
        })
    }

    /// The mirror image of the fence: a replica configured *without* the cap must
    /// not serve from a database that enforces one. The fence would reject its
    /// writes anyway; refusing at boot turns a stream of failed settlements into
    /// one legible error, and refuses before any traffic is admitted.
    /// A partial fence counts: whichever relation carries it will reject this
    /// replica's writes, so booting would only defer the failure to traffic.
    async fn require_no_namespace_fence(&self, client: &Client) -> Result<(), BudgetError> {
        if self.fence_targets(client).await?.any() {
            return Err(BudgetError::invalid(
                BACKEND,
                format!(
                    "`{}` enforces a namespace spend cap (the `{}` trigger from \
                     `ops/postgres/budget_v2.sql` is installed), so \
                     `namespace_limit_microdollars` must be set here too: without it this replica \
                     would charge subjects without ever charging the namespace, and the cap would \
                     under-count permanently. Set the same namespace limit as the rest of the \
                     fleet, or drop the fence triggers to go back to per-subject-only \
                     enforcement.",
                    self.table,
                    self.fence_name()
                ),
            ));
        }
        Ok(())
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
        if self.settings.enforces_namespace_cap() {
            transaction
                .batch_execute(NAMESPACE_CAP_DECLARATION_LOCAL)
                .await?;
        }

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
        if self.settings.enforces_namespace_cap() {
            transaction
                .batch_execute(NAMESPACE_CAP_DECLARATION_LOCAL)
                .await?;
        }
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
    use std::sync::Arc;

    use tokio::sync::Barrier;

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

        // Every task waits on the barrier, so the admissions genuinely overlap
        // instead of taking turns: whatever serializes them has to be the
        // namespace row lock, not the test's own sequencing.
        let replica_a = Arc::new(replica_a);
        let replica_b = Arc::new(replica_b);
        let contenders = 40;
        let start = Arc::new(Barrier::new(contenders));
        let mut tasks = Vec::with_capacity(contenders);
        for index in 0..contenders {
            // Distinct subjects, so only the namespace cap can deny any of them.
            let key = BudgetKey {
                namespace: "acme".into(),
                subject: format!("subject-{index}"),
            };
            let store = if index % 2 == 0 {
                Arc::clone(&replica_a)
            } else {
                Arc::clone(&replica_b)
            };
            let start = Arc::clone(&start);
            tasks.push(tokio::spawn(async move {
                start.wait().await;
                match store.reserve(&key, 100).await {
                    Admission::Allowed(held) => {
                        store.settle(&key, &held, 100).await;
                        true
                    }
                    Admission::Denied(_) => false,
                }
            }));
        }

        let mut admitted = 0;
        for task in tasks {
            if task.await.expect("no task panicked") {
                admitted += 1;
            }
        }
        // The cap divided by the estimate, exactly: not one request more, even
        // though forty raced for it across two replicas.
        assert_eq!(admitted, 10);
    }

    /// The fence, from the direction that matters: a replica configured without
    /// the namespace cap must not be able to record spend in a database that
    /// enforces one, or the namespace total would drift down forever.
    #[tokio::test]
    async fn a_replica_without_the_namespace_cap_cannot_write_to_a_fenced_database() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let table = "axond_budget_ns_fence_test";
        let capped = namespace_store(&dsn, table, namespace_settings(1_000, 1_000)).await;

        // Boot direction one: the old configuration refuses to start.
        let refused = PostgresBudget::connect(
            &dsn,
            PostgresBudgetSettings {
                table: table.to_owned(),
                create_table: false,
                shared: settings(1_000),
            },
        )
        .await
        .err()
        .unwrap_or_else(|| panic!("a cap-less replica must not boot against a fenced database"));
        assert!(
            format!("{refused}").contains("namespace_limit_microdollars"),
            "{refused}"
        );

        // Spend from the cap-aware replica, so the fence has an existing row to
        // defend as well as an insert to refuse.
        let key = BudgetKey {
            namespace: "acme".into(),
            subject: "honest".into(),
        };
        let Admission::Allowed(held) = capped.reserve(&key, 600).await else {
            panic!("the declared replica writes as before");
        };
        capped.settle(&key, &held, 600).await;

        // And the database enforces it itself, for a binary that never had a boot
        // check to run: a session that has not declared namespace-cap support
        // cannot write spend or take a hold.
        let (bare, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(connection);
        for statement in [
            format!(
                "INSERT INTO {table} (namespace, subject, spent_microdollars)
                 VALUES ('acme', 'sneaky', 500)"
            ),
            format!(
                "UPDATE {table} SET spent_microdollars = spent_microdollars + 500
                 WHERE namespace = 'acme'"
            ),
            format!(
                "INSERT INTO {table}_reservation
                     (id, namespace, subject, amount_microdollars, expires_at)
                 VALUES ('x', 'acme', 'sneaky', 500, now() + interval '1 minute')"
            ),
        ] {
            let rejected = bare
                .execute(&statement, &[])
                .await
                .expect_err("the fence must reject an undeclared writer");
            let raised = rejected
                .as_db_error()
                .map(|error| error.message().to_owned())
                .unwrap_or_default();
            assert!(
                raised.contains("namespace spend cap"),
                "the fence must be what refused it: {rejected} ({raised})"
            );
        }

        // The namespace total is exactly what the declared replica charged.
        assert_eq!(
            capped.reserve(&key, 401).await,
            Admission::Denied(Denial::Exceeded),
            "the namespace has 600 of its 1000 spent, and nothing slipped past"
        );
    }

    /// The fence's declaration is a session setting, and a pooler in transaction
    /// mode can run it on one backend and the writes on another. Both writing
    /// transactions therefore re-declare it locally, which has to be enough on a
    /// session that never declared anything — and has to stay scoped to that
    /// transaction, or it would be indistinguishable from the session-wide one.
    #[tokio::test]
    async fn a_transaction_carries_the_fence_declaration_itself() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let table = "axond_budget_ns_local_declaration_test";
        let store = namespace_store(&dsn, table, namespace_settings(1_000, 1_000)).await;

        // A session as a transaction pooler would hand it over: nothing declared.
        let (mut undeclared, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(connection);
        let insert = format!(
            "INSERT INTO {table} (namespace, subject, spent_microdollars)
             VALUES ('acme', 'pooled', 10)"
        );

        let declared = undeclared.transaction().await.expect("begin");
        declared
            .batch_execute(NAMESPACE_CAP_DECLARATION_LOCAL)
            .await
            .expect("declare for this transaction");
        declared.execute(&insert, &[]).await.expect(
            "the fence must accept a transaction that declares the cap itself, \
             whatever the session did",
        );
        declared.commit().await.expect("commit");

        // And the declaration did not outlive it, so the fence is still fencing.
        let silent = undeclared.transaction().await.expect("begin");
        let rejected = silent
            .execute(&insert, &[])
            .await
            .expect_err("a transaction that declares nothing is still refused");
        let raised = rejected
            .as_db_error()
            .map(|error| error.message().to_owned())
            .unwrap_or_default();
        assert!(
            raised.contains("namespace spend cap"),
            "the fence must be what refused it: {rejected} ({raised})"
        );
        drop(silent);

        // The store itself keeps working, declaration and all.
        let key = BudgetKey {
            namespace: "acme".into(),
            subject: "honest".into(),
        };
        let Admission::Allowed(held) = store.reserve(&key, 100).await else {
            panic!("a reserve declares the cap inside its own transaction");
        };
        store.settle(&key, &held, 100).await;
        assert_eq!(
            store.reserve(&key, 901).await,
            Admission::Denied(Denial::Exceeded),
            "the settlement landed against the subject and the namespace alike"
        );
    }

    /// `create_table = true` re-applies the shipped DDL on every boot, but the v2
    /// file is a migration: an `EXCLUSIVE` lock plus a whole-table aggregate. A
    /// restart must not take that lock again, or it would stall every other
    /// replica's reserves and settlements for as long as the aggregate runs.
    #[tokio::test]
    async fn a_restart_does_not_re_lock_the_spend_table_to_reapply_the_v2_ddl() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let table = "axond_budget_ns_relock_test";
        let store = namespace_store(&dsn, table, namespace_settings(1_000, 1_000)).await;
        drop(store);

        // Another replica is mid-settlement, holding `ROW EXCLUSIVE` on the spend
        // table — which conflicts with the `EXCLUSIVE` the v2 file takes.
        let (mut busy, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(connection);
        busy.batch_execute(NAMESPACE_CAP_DECLARATION)
            .await
            .expect("declare");
        let holding = busy.transaction().await.expect("begin");
        holding
            .execute(
                &format!(
                    "INSERT INTO {table} (namespace, subject, spent_microdollars)
                     VALUES ('acme', 'busy', 10)"
                ),
                &[],
            )
            .await
            .expect("write as the fleet does");

        let booted = tokio::time::timeout(
            Duration::from_secs(10),
            PostgresBudget::connect(
                &dsn,
                PostgresBudgetSettings {
                    table: table.to_owned(),
                    create_table: true,
                    shared: namespace_settings(1_000, 1_000),
                },
            ),
        )
        .await
        .expect("booting must not queue behind an in-flight settlement");
        booted.expect("connect");
        holding.rollback().await.expect("rollback");

        // But a schema that is *not* already installed is still applied, so the
        // skip cannot leave a replica serving without the fence.
        let (admin, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(connection);
        admin
            .batch_execute(&format!(
                "DROP TRIGGER {table}_namespace_fence ON {table}_reservation"
            ))
            .await
            .expect("drop the reservation fence");
        namespace_store(&dsn, table, namespace_settings(1_000, 1_000)).await;
        let restored: bool = admin
            .query_one(
                "SELECT count(*) = 1 FROM pg_trigger
                 WHERE tgname = $1 AND tgrelid = to_regclass($2) AND NOT tgisinternal",
                &[
                    &format!("{table}_namespace_fence"),
                    &format!("{table}_reservation"),
                ],
            )
            .await
            .expect("query")
            .get(0);
        assert!(restored, "an incomplete v2 schema is still applied at boot");
    }

    /// A trigger name is unique per table, never globally, and the fence has to
    /// cover *both* relations. So neither half of the fence on its own, nor a
    /// same-named trigger on an unrelated table, may read as a fenced database.
    #[tokio::test]
    async fn a_partial_fence_or_a_same_named_decoy_does_not_count_as_fenced() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let table = "axond_budget_ns_partial_fence_test";
        let fence = format!("{table}_namespace_fence");
        let capped = namespace_store(&dsn, table, namespace_settings(1_000, 1_000)).await;
        let (admin, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(connection);

        // One of the two triggers missing: the reservation table would let a
        // cap-less replica take holds the namespace never sees.
        admin
            .batch_execute(&format!("DROP TRIGGER {fence} ON {table}_reservation"))
            .await
            .expect("drop the reservation fence");
        let refused = PostgresBudget::connect(
            &dsn,
            PostgresBudgetSettings {
                table: table.to_owned(),
                create_table: false,
                shared: namespace_settings(1_000, 1_000),
            },
        )
        .await
        .err()
        .unwrap_or_else(|| panic!("a half-fenced database must not accept a cap-enabled replica"));
        assert!(
            format!("{refused}").contains(&format!("{table}_reservation")),
            "the error must name the relation that is missing the fence: {refused}"
        );
        // And the other direction still refuses, because the spend table is fenced
        // and will reject this replica's writes.
        PostgresBudget::connect(
            &dsn,
            PostgresBudgetSettings {
                table: table.to_owned(),
                create_table: false,
                shared: settings(1_000),
            },
        )
        .await
        .err()
        .unwrap_or_else(|| panic!("a partly fenced database still excludes a cap-less replica"));
        drop(capped);

        // A same-named trigger on an unrelated table proves nothing about these
        // two relations, so a name-only check would boot an unfenced database.
        let decoy = "axond_budget_ns_decoy_test";
        let decoy_fence = format!("{decoy}_namespace_fence");
        let store = namespace_store(&dsn, decoy, namespace_settings(1_000, 1_000)).await;
        drop(store);
        admin
            .batch_execute(&format!(
                "DROP TRIGGER {decoy_fence} ON {decoy};
                 DROP TRIGGER {decoy_fence} ON {decoy}_reservation;
                 CREATE TABLE IF NOT EXISTS {decoy}_bystander (namespace text);
                 DROP TRIGGER IF EXISTS {decoy_fence} ON {decoy}_bystander;
                 CREATE TRIGGER {decoy_fence}
                     BEFORE INSERT ON {decoy}_bystander
                     FOR EACH ROW EXECUTE FUNCTION {decoy_fence}()"
            ))
            .await
            .expect("plant the decoy");
        let refused = PostgresBudget::connect(
            &dsn,
            PostgresBudgetSettings {
                table: decoy.to_owned(),
                create_table: false,
                shared: namespace_settings(1_000, 1_000),
            },
        )
        .await
        .err()
        .unwrap_or_else(|| panic!("a decoy trigger elsewhere must not pass for the fence"));
        assert!(
            format!("{refused}").contains(decoy),
            "the error must name the unfenced tables: {refused}"
        );
        // With no fence on either of its own relations, the cap-less replica is the
        // one configuration this database does accept.
        PostgresBudget::connect(
            &dsn,
            PostgresBudgetSettings {
                table: decoy.to_owned(),
                create_table: false,
                shared: settings(1_000),
            },
        )
        .await
        .expect("an unfenced database accepts a cap-less replica");
    }

    #[tokio::test]
    async fn an_expired_hold_frees_the_namespace_cap_too() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let table = "axond_budget_ns_expiry_test";
        // A long TTL, so the denial below cannot race the clock; expiry is then
        // forced by backdating the row rather than by sleeping.
        let mut expiring = namespace_settings(1_000, 1_000);
        expiring.reservation_ttl = Duration::from_secs(600);
        let store = namespace_store(&dsn, table, expiring).await;
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

        // The replica holding it died: its hold is now in the past.
        {
            let guard = store.client.lock().await;
            let client = guard.as_ref().expect("connected");
            let backdated = client
                .execute(
                    &format!(
                        "UPDATE {table}_reservation SET expires_at = now() - interval '1 second' \
                         WHERE namespace = $1 AND subject = $2"
                    ),
                    &[&died.namespace, &died.subject],
                )
                .await
                .expect("backdate the hold");
            assert_eq!(backdated, 1, "exactly one hold is outstanding");
        }

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
