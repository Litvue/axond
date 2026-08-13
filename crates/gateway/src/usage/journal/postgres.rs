//! The Postgres outbox: a [`UsageJournal`] that keeps a billing-grade promise.
//!
//! The shape is the one ADR 0009 anticipated — an `axond_usage_outbox` table for
//! the events, a per-consumer delivery table for the state, `FOR UPDATE SKIP
//! LOCKED` for claims, and a lease column for expiry — and the DDL is shipped as
//! an interface in
//! [`ops/postgres/usage_outbox_v1.sql`](../../../../../ops/postgres/usage_outbox_v1.sql)
//! for operators who apply their own schema.
//!
//! # Why the durability actually holds
//!
//! [`append`](UsageJournal::append) commits before it returns, so an accepted
//! event survives the process that accepted it: that is the whole difference
//! between this and [`PostgresSink`](crate::usage::PostgresSink), which buffers
//! and drops. Everything else follows from where the state lives — leases,
//! attempt counters, and acknowledgements are rows, so a replica that dies
//! mid-delivery leaves a lease that expires rather than a lock nobody can
//! release.
//!
//! # Ordering, and the two statements that enforce it
//!
//! A claim takes, per [`OrderingKey`](super::OrderingKey), only that key's *lowest* unresolved
//! position, and only when nothing else holds a live lease on it. The selection
//! locks its candidate events `FOR UPDATE SKIP LOCKED`, so a concurrent claimant
//! skips a key rather than overtaking it — but the selection alone cannot decide
//! ownership, because the lease it reads lives on the delivery row and its
//! snapshot may predate a lease another claimant has since committed. The
//! ownership decision is therefore the upsert of the delivery row, whose
//! `ON CONFLICT DO UPDATE … WHERE` re-checks the lease against the current row
//! under its lock: the loser updates nothing and leaves the key alone. Two
//! workers claiming concurrently therefore cannot both be delivering the same
//! caller's event.
//!
//! # What this build will not deliver
//!
//! Two row states are neither delivered nor retried:
//!
//! - A record whose `schema_version` is *newer* than [`UsageRecord`] is skipped
//!   untouched, with no attempt spent. During a rolling upgrade the replica that
//!   wrote it can deliver it, and a build that cannot read a row must not condemn
//!   it — the ordering key waits, which is the honest cost of the guarantee.
//! - A record this build cannot decode *at its own version* is corruption, not a
//!   version skew, and retrying it forever would block its ordering key forever.
//!   It is quarantined as [`PoisonReason::Malformed`] on the spot, so an operator
//!   sees it on the poison count with the row still there to inspect.
//!
//! # Connections
//!
//! A small fixed pool, because the request path's append must not queue behind a
//! worker's claim. Each connection reconnects on failure and re-applies its
//! `search_path`, so a reconnect cannot silently land on another schema's outbox.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use tokio_postgres::{Client, Config, Transaction};

use super::{
    Appended, Capacity, CapacityPolicy, Claim, ConsumerId, Delivery, DeliveryId, DeliveryMode,
    JournalError, JournalStats, PoisonReason, UsageEvent, UsageJournal,
};
use crate::usage::{ObservedRecord, Status, UsageRecord};

/// The DDL for the current outbox version, shared with operators who apply it
/// themselves. Embedded from the package-local copy, because `ops/` is outside
/// this crate; `tests/shipped_ddl.rs` fails if the two copies differ by a byte.
const SCHEMA_DDL: &str = include_str!("../../../sql/usage_outbox_v1.sql");

const BACKEND: &str = "postgres";

/// How long a measured count is trusted before the capacity gate takes another
/// one. It is also what bounds how long a refusal can outlive the backlog that
/// caused it, because the measurement a refusal is made on expires.
const COUNT_REFRESH: Duration = Duration::from_secs(1);

/// How the outbox connects, and what it is allowed to do at boot.
#[derive(Debug, Clone)]
pub struct PostgresJournalSettings {
    /// The schema the outbox lives in, if not the connection's default.
    /// Validated as an identifier, because it is interpolated into
    /// `SET search_path`.
    pub schema: Option<String>,
    /// Whether boot may apply the shipped DDL. Off by default, like every other
    /// store here: most deployments give the gateway's role no DDL rights.
    pub create_schema: bool,
    pub capacity: Capacity,
    pub connect_timeout: Duration,
    /// The ceiling on one journal operation, including the append that a request
    /// waits on.
    pub operation_timeout: Duration,
    /// Connections held open. Two is the useful minimum — one for the request
    /// path's appends, one for the delivery worker.
    pub connections: usize,
}

impl Default for PostgresJournalSettings {
    fn default() -> Self {
        Self {
            schema: None,
            create_schema: false,
            capacity: Capacity::BILLING_GRADE,
            connect_timeout: Duration::from_secs(10),
            operation_timeout: Duration::from_secs(10),
            connections: 2,
        }
    }
}

/// A durable usage outbox in PostgreSQL.
pub struct PostgresJournal {
    settings: PostgresJournalSettings,
    pool: Pool,
    /// Shared rather than owned so an operation's closure can hold it without
    /// borrowing the journal: the pool's retry needs a closure it can call twice.
    stored: Arc<CapacityGate>,
}

/// Written by hand and deliberately narrow: a derived one would print the
/// [`Config`], which carries the password from the DSN.
impl std::fmt::Debug for PostgresJournal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresJournal")
            .field("schema", &self.settings.schema)
            .field("capacity", &self.settings.capacity)
            .finish_non_exhaustive()
    }
}

impl PostgresJournal {
    /// Connect, check the schema, and optionally apply the shipped DDL.
    ///
    /// Boot refuses rather than degrades: a billing-grade deployment whose
    /// outbox is missing must not start and then discover, one request in, that
    /// it cannot make an event durable.
    pub async fn connect(
        dsn: &str,
        settings: PostgresJournalSettings,
    ) -> Result<Self, JournalError> {
        let mut config: Config = dsn
            .parse()
            // The DSN itself is never echoed: it carries a password.
            .map_err(|error| {
                backend(format!(
                    "the usage journal DSN could not be parsed: {error}"
                ))
            })?;
        config.connect_timeout(settings.connect_timeout);
        config.application_name(crate::telemetry::SERVICE_NAME);
        let search_path = settings
            .schema
            .as_deref()
            .map(|schema| {
                crate::usage::validate_table_name(schema).map_err(backend)?;
                // The table-name validator allows one qualifying dot; a search
                // path takes one unqualified schema. Checked here as well as in
                // config validation, because settings can be built directly.
                if schema.contains('.') {
                    return Err(backend(format!(
                        "`{schema}` is not a single unqualified schema name"
                    )));
                }
                Ok(schema.to_owned())
            })
            .transpose()?;
        if settings.connections == 0 {
            return Err(backend("a usage journal needs at least one connection"));
        }

        let journal = Self {
            pool: Pool::new(config, search_path, settings.connections),
            settings,
            stored: Arc::new(CapacityGate::new()),
        };
        if journal.settings.create_schema {
            journal
                .run("apply schema", |client| {
                    Box::pin(async move {
                        client.batch_execute(SCHEMA_DDL).await?;
                        Ok(())
                    })
                })
                .await?;
        }
        journal.check_schema().await?;
        Ok(journal)
    }

    /// Refuse to serve against a schema that is not the one this build writes.
    ///
    /// The check is a read of each table this journal uses, because the failure
    /// it prevents — a deployment that boots, accepts events, and cannot append
    /// them — is worse than a refusal an operator can fix with one `psql` run.
    async fn check_schema(&self) -> Result<(), JournalError> {
        self.run("check schema", |client| {
            Box::pin(async move {
                for table in [
                    "axond_usage_outbox",
                    "axond_usage_outbox_consumer",
                    "axond_usage_outbox_delivery",
                    "axond_usage_outbox_loss",
                ] {
                    if let Err(error) = client
                        .query_opt(&format!("SELECT 1 FROM {table} LIMIT 1"), &[])
                        .await
                    {
                        return Err(OpError::Journal(backend(format!(
                            "`{table}` is not readable, so this build cannot own the usage \
                             outbox; apply `ops/postgres/usage_outbox_v1.sql` (or set \
                             `[usage_journal] create_schema = true`): {error}"
                        ))));
                    }
                }
                Ok(())
            })
        })
        .await
    }

    /// Run one operation on a pooled connection, under the operation timeout.
    async fn run<T, F>(&self, what: &'static str, op: F) -> Result<T, JournalError>
    where
        T: Send,
        F: for<'a> Fn(&'a mut Client) -> BoxFuture<'a, Result<T, OpError>> + Send + Sync,
    {
        match tokio::time::timeout(self.settings.operation_timeout, self.pool.run(&op)).await {
            Ok(result) => result,
            Err(_) => Err(backend(format!(
                "`{what}` exceeded its {:?} bound",
                self.settings.operation_timeout
            ))),
        }
    }
}

/// How many events are stored: the gate's recent measurement, or one taken here.
///
/// Two bounds, because this runs inside the append a request is waiting on and an
/// unbounded `count(*)` gets slower exactly as the outbox falls behind, which is
/// when appends can least afford it. A replica measures at most once per
/// [`COUNT_REFRESH`] however full the outbox is, and a measurement stops at
/// `max_events + 1` rows — the largest number the decision can distinguish, since
/// anything above the limit is over it.
async fn stored_events(
    tx: &Transaction<'_>,
    gate: &CapacityGate,
    max_events: u64,
) -> Result<u64, OpError> {
    if let Some(estimate) = gate.estimate() {
        return Ok(estimate);
    }
    let bound = i64::try_from(max_events.saturating_add(1)).unwrap_or(i64::MAX);
    let row = tx
        .query_one(
            "SELECT count(*) FROM (SELECT 1 FROM axond_usage_outbox LIMIT $1) bounded",
            &[&bound],
        )
        .await?;
    let counted = row.get::<_, i64>(0).max(0) as u64;
    gate.measured(counted);
    Ok(counted)
}

#[async_trait]
impl UsageJournal for PostgresJournal {
    fn name(&self) -> &'static str {
        BACKEND
    }

    fn capacity(&self) -> Capacity {
        self.settings.capacity
    }

    /// Durable by construction: the append commits before the request that
    /// produced it is answered.
    fn mode(&self) -> DeliveryMode {
        DeliveryMode::BillingGrade
    }

    async fn append(&self, event: &UsageEvent) -> Result<Appended, JournalError> {
        let idempotency_key = event.idempotency_key().clone();
        let key = idempotency_key.as_str().to_owned();
        let record = serde_json::to_value(event.record()).map_err(|error| {
            backend(format!("the usage record could not be serialized: {error}"))
        })?;
        let capacity = self.settings.capacity;
        let observed_at = event.observed_at();
        let ordering = event.ordering_key().clone();
        let version = i32::try_from(event.record().schema_version).unwrap_or(i32::MAX);

        let gate = Arc::clone(&self.stored);
        self.run("append", move |client| {
            let (key, record, ordering) = (key.clone(), record.clone(), ordering.clone());
            let idempotency_key = idempotency_key.clone();
            let gate = Arc::clone(&gate);
            Box::pin(async move {
                let tx = client.transaction().await?;
                // The idempotency check and the insert are one transaction, so a
                // concurrent append of the same event cannot produce two rows;
                // the UNIQUE constraint is the backstop if it tries.
                if let Some(row) = tx
                    .query_opt(
                        "SELECT position, record = $2::jsonb FROM axond_usage_outbox \
                         WHERE request_id = $1",
                        &[&key, &record],
                    )
                    .await?
                {
                    let position = row.get::<_, i64>(0).max(0) as u64;
                    return if row.get::<_, bool>(1) {
                        Ok(Appended::AlreadyPresent { position })
                    } else {
                        Err(OpError::Journal(JournalError::Conflict {
                            key: idempotency_key,
                        }))
                    };
                }

                let mut dropped = 0;
                let mut stored = stored_events(&tx, &gate, capacity.max_events).await?;
                if stored >= capacity.max_events {
                    // Space that is only owed to a courtesy window goes first:
                    // giving up a delivered event costs a redundant
                    // re-acknowledgement, refusing costs an undelivered event.
                    let wanted = stored - capacity.max_events + 1;
                    let reclaimed = reclaim_delivered(&tx, wanted).await?;
                    stored -= reclaimed;
                    if stored >= capacity.max_events
                        && capacity.policy == CapacityPolicy::DropOldest
                    {
                        dropped = drop_oldest(&tx, stored - capacity.max_events + 1).await?;
                        stored -= dropped;
                    }
                    if reclaimed > 0 || dropped > 0 {
                        gate.invalidate();
                    }
                    if stored >= capacity.max_events {
                        // Everything left is either undelivered or somebody's
                        // quarantined evidence, so there is no room to make. The
                        // measurement is kept: the next append refuses on it
                        // rather than measuring a backlog that is still there,
                        // and it expires on its own inside a second.
                        return Err(OpError::Journal(JournalError::AtCapacity {
                            pending: stored,
                            capacity,
                        }));
                    }
                }

                let row = tx
                    .query_one(
                        "INSERT INTO axond_usage_outbox \
                           (request_id, schema_version, namespace, subject, record, observed_at) \
                         VALUES ($1, $2, $3, $4, $5::jsonb, $6) \
                         RETURNING position",
                        &[
                            &key,
                            &version,
                            &ordering.namespace,
                            &ordering.subject,
                            &record,
                            &observed_at,
                        ],
                    )
                    .await?;
                tx.commit().await?;
                // Counted after the commit, because a drop this transaction
                // rolled back lost nothing, and the loss counter is the one an
                // operator is paged on.
                if dropped > 0 {
                    crate::telemetry::metrics::record_usage_journal_lost(
                        BACKEND,
                        "capacity_drop",
                        dropped,
                    );
                }
                gate.appended();
                Ok(Appended::Accepted {
                    position: row.get::<_, i64>(0).max(0) as u64,
                })
            })
        })
        .await
    }

    async fn claim(
        &self,
        consumer: &ConsumerId,
        claim: Claim,
    ) -> Result<Vec<Delivery>, JournalError> {
        if claim.max_events == 0 {
            return Ok(Vec::new());
        }
        let consumer = consumer.clone();
        let name = consumer.as_str().to_owned();
        let max_attempts =
            i32::try_from(self.settings.capacity.max_delivery_attempts).unwrap_or(i32::MAX);
        let lease_expires_at = claim.now + claim.lease;
        let readable = i32::try_from(UsageRecord::SCHEMA_VERSION).unwrap_or(i32::MAX);

        self.run("claim", move |client| {
            let (name, consumer) = (name.clone(), consumer.clone());
            Box::pin(async move {
                let tx = client.transaction().await?;
                // A consumer exists once it has claimed, and nothing else
                // registers one: retention waits on every registered consumer.
                tx.execute(
                    "INSERT INTO axond_usage_outbox_consumer (consumer) VALUES ($1) \
                     ON CONFLICT (consumer) DO NOTHING",
                    &[&name],
                )
                .await?;
                // The floor the selection starts from, so a claim walks the
                // backlog rather than the retained history behind it. Read here
                // and raised by maintenance: raising it from the claim would take
                // the consumer row's lock and serialize every replica's claims
                // against each other.
                let floor: i64 = tx
                    .query_one(
                        "SELECT resolved_through FROM axond_usage_outbox_consumer \
                         WHERE consumer = $1",
                        &[&name],
                    )
                    .await?
                    .get(0);

                let mut claimed: Vec<Delivery> = Vec::with_capacity(claim.max_events);
                // What this claim would report, held until the commit that makes
                // it true: the pool re-runs this closure once after a database
                // error, and a retried pass must not count its first pass's
                // quarantines twice.
                let mut condemnations: Vec<PoisonReason> = Vec::new();
                let mut undeliverable = Undeliverable::default();
                // A pass that only condemned rows advanced the head of those
                // ordering keys without filling the batch, so the selection runs
                // again: the caller's next event is deliverable now, and a claim
                // spent entirely on poison would otherwise stall delivery for a
                // poll interval per bad row. Bounded by the batch size, because
                // every iteration either claims or condemns at least one row.
                for _ in 0..claim.max_events {
                    let remaining =
                        i64::try_from(claim.max_events - claimed.len()).unwrap_or(i64::MAX);
                    // Per ordering key, that key's lowest unresolved position — and
                    // only when no live lease holds it. `SKIP LOCKED` makes a
                    // concurrent claimant skip the key rather than overtake it.
                    //
                    // Both sides are floored on the consumer's resolved prefix, so
                    // neither walks the acknowledged history the retention window
                    // keeps. On the delivery side the floor is redundant — its
                    // position equals a joined event's — but stating it on the join
                    // is what lets that side be an index range too, because the
                    // planner will not infer it through an outer join.
                    let candidates = tx
                        .query(
                            "WITH open AS (
                             SELECT e.position, e.namespace, e.subject,
                                    COALESCE(d.attempts, 0) AS attempts,
                                    d.lease_expires_at
                             FROM axond_usage_outbox e
                             LEFT JOIN axond_usage_outbox_delivery d
                                 ON d.position = e.position AND d.consumer = $1
                                AND d.position > $4
                             WHERE e.position > $4
                               AND d.acknowledged_at IS NULL AND d.quarantined_at IS NULL
                         ),
                         head AS (
                             SELECT DISTINCT ON (namespace, subject)
                                    position, attempts, lease_expires_at
                             FROM open
                             ORDER BY namespace, subject, position
                         )
                         SELECT h.position, h.attempts,
                                e.request_id, e.schema_version, e.record, e.observed_at
                         FROM head h
                         JOIN axond_usage_outbox e ON e.position = h.position
                         WHERE h.lease_expires_at IS NULL OR h.lease_expires_at <= $2
                         ORDER BY h.position
                         LIMIT $3
                         FOR UPDATE OF e SKIP LOCKED",
                            &[&name, &claim.now, &remaining, &floor],
                        )
                        .await?;
                    if candidates.is_empty() {
                        break;
                    }

                    let mut condemned = 0usize;
                    for row in candidates {
                        let position: i64 = row.get(0);
                        let attempt = row.get::<_, i32>(1).saturating_add(1);
                        let stored_version: i32 = row.get(3);
                        // A row a newer writer produced is left exactly as it is: no
                        // attempt, no verdict, no lease. Its own version's replica
                        // will deliver it.
                        if stored_version > readable {
                            undeliverable.schema_ahead(position);
                            continue;
                        }
                        if attempt > max_attempts {
                            condemn(
                                &tx,
                                position,
                                &name,
                                PoisonReason::AttemptsExhausted,
                                attempt,
                            )
                            .await?;
                            condemnations.push(PoisonReason::AttemptsExhausted);
                            condemned += 1;
                            continue;
                        }
                        let event = match decode(&row) {
                            Ok(event) => event,
                            // Corruption at this build's own version: retrying it
                            // forever would block the ordering key forever, so it
                            // leaves the delivery path with the row still there.
                            Err(reason) => {
                                tracing::error!(
                                    position,
                                    consumer = %name,
                                    reason = %reason,
                                    "usage outbox row could not be decoded; quarantining it"
                                );
                                undeliverable.corrupt();
                                condemn(&tx, position, &name, PoisonReason::Malformed, attempt)
                                    .await?;
                                condemnations.push(PoisonReason::Malformed);
                                condemned += 1;
                                continue;
                            }
                        };
                        // The claim itself, and the only step that decides who
                        // owns the delivery. `ON CONFLICT DO UPDATE` takes the
                        // row lock and re-evaluates its `WHERE` against the
                        // *current* row rather than this transaction's snapshot,
                        // so a claimant that selected the event before a
                        // concurrent claimant committed its lease updates zero
                        // rows and moves on. Locking the event row alone could
                        // not do this: the lease lives on the delivery row, and
                        // the snapshot the `LEFT JOIN` read it through may predate
                        // it.
                        let taken = tx
                            .execute(
                                "INSERT INTO axond_usage_outbox_delivery
                             (position, consumer, attempts, lease_expires_at)
                         VALUES ($1, $2, $3, $4)
                         ON CONFLICT (position, consumer) DO UPDATE
                             SET attempts = $3, lease_expires_at = $4
                             WHERE axond_usage_outbox_delivery.acknowledged_at IS NULL
                               AND axond_usage_outbox_delivery.quarantined_at IS NULL
                               AND (axond_usage_outbox_delivery.lease_expires_at IS NULL
                                    OR axond_usage_outbox_delivery.lease_expires_at <= $5)",
                                &[&position, &name, &attempt, &lease_expires_at, &claim.now],
                            )
                            .await?;
                        if taken == 0 {
                            // Somebody else holds this key's head, or resolved it
                            // while this claim was selecting. Not an error, and
                            // not a condemnation: the next claim sees whatever
                            // they leave behind.
                            continue;
                        }
                        claimed.push(Delivery {
                            id: DeliveryId {
                                consumer: consumer.clone(),
                                event: event.id(),
                                attempt: attempt.max(1) as u32,
                            },
                            event,
                            lease_expires_at,
                        });
                    }
                    // Nothing was condemned, so another pass would select the same
                    // heads: whatever is left is leased, ahead of this build, or the
                    // batch is full.
                    if condemned == 0 || claimed.len() >= claim.max_events {
                        break;
                    }
                }
                tx.commit().await?;
                for reason in condemnations {
                    crate::telemetry::metrics::record_usage_journal_quarantined(
                        BACKEND,
                        &name,
                        reason.as_str(),
                    );
                }
                for reason in undeliverable.reasons {
                    crate::telemetry::metrics::record_usage_journal_undeliverable(BACKEND, reason);
                }
                Ok(claimed)
            })
        })
        .await
    }

    async fn ack(&self, delivery: &DeliveryId) -> Result<(), JournalError> {
        self.verdict(delivery, None).await
    }

    async fn quarantine(
        &self,
        delivery: &DeliveryId,
        reason: PoisonReason,
    ) -> Result<(), JournalError> {
        self.verdict(delivery, Some(reason)).await
    }

    async fn stats(&self, consumer: &ConsumerId) -> Result<JournalStats, JournalError> {
        let name = consumer.as_str().to_owned();
        let now = SystemTime::now();
        let capacity = self.settings.capacity;
        self.run("stats", move |client| {
            let name = name.clone();
            Box::pin(async move {
                let row = client
                    .query_one(
                        "WITH state AS (
                             SELECT e.observed_at, d.acknowledged_at, d.quarantined_at,
                                    d.lease_expires_at,
                                    (d.acknowledged_at IS NULL AND d.quarantined_at IS NULL
                                     AND (d.lease_expires_at IS NULL
                                          OR d.lease_expires_at <= $2)) AS pending
                             FROM axond_usage_outbox e
                             LEFT JOIN axond_usage_outbox_delivery d
                                 ON d.position = e.position AND d.consumer = $1
                         )
                         SELECT
                             count(*) FILTER (WHERE pending),
                             count(*) FILTER (WHERE acknowledged_at IS NULL
                                              AND quarantined_at IS NULL
                                              AND lease_expires_at > $2),
                             count(*) FILTER (WHERE quarantined_at IS NOT NULL),
                             min(observed_at) FILTER (WHERE pending),
                             (SELECT dropped FROM axond_usage_outbox_loss WHERE id)
                         FROM state",
                        &[&name, &now],
                    )
                    .await?;
                let oldest: Option<SystemTime> = row.get(3);
                Ok(JournalStats {
                    pending: row.get::<_, i64>(0).max(0) as u64,
                    in_flight: row.get::<_, i64>(1).max(0) as u64,
                    quarantined: row.get::<_, i64>(2).max(0) as u64,
                    oldest_pending_age: oldest.and_then(|oldest| now.duration_since(oldest).ok()),
                    dropped: row.get::<_, Option<i64>>(4).unwrap_or_default().max(0) as u64,
                    capacity,
                })
            })
        })
        .await
    }

    async fn maintain(&self, now: SystemTime) -> Result<u64, JournalError> {
        let retain = self.settings.capacity.retain_acknowledged;
        let pruned = self
            .run("maintain", move |client| {
                Box::pin(async move {
                    let cutoff = now.checked_sub(retain).unwrap_or(now);
                    let pruned = client
                        .execute(
                            &format!(
                                "DELETE FROM axond_usage_outbox e WHERE e.observed_at <= $1 \
                                      AND {DELIVERED}"
                            ),
                            &[&cutoff],
                        )
                        .await?;
                    client.execute(ADVANCE_RESOLVED_THROUGH, &[]).await?;
                    Ok(pruned)
                })
            })
            .await?;
        if pruned > 0 {
            self.stored.invalidate();
        }
        Ok(pruned)
    }
}

impl PostgresJournal {
    /// The one statement behind [`UsageJournal::ack`] and
    /// [`UsageJournal::quarantine`], because the two differ only in which
    /// terminal column they set and which one they refuse to overwrite.
    async fn verdict(
        &self,
        delivery: &DeliveryId,
        poison: Option<PoisonReason>,
    ) -> Result<(), JournalError> {
        let (name, event) = (delivery.consumer.as_str().to_owned(), delivery.event);
        let key = event.to_string();
        let refused = delivery.clone();
        self.run("verdict", move |client| {
            let (name, key, refused) = (name.clone(), key.clone(), refused.clone());
            Box::pin(async move {
                let tx = client.transaction().await?;
                let state = tx
                    .query_opt(
                        "SELECT d.attempts, d.acknowledged_at IS NOT NULL,
                                d.quarantined_at IS NOT NULL
                         FROM axond_usage_outbox e
                         JOIN axond_usage_outbox_delivery d
                             ON d.position = e.position AND d.consumer = $2
                         WHERE e.request_id = $1
                         FOR UPDATE OF d",
                        &[&key, &name],
                    )
                    .await?;
                // No delivery row means this consumer was never handed the
                // event — or it was pruned, which a consumer must read as
                // "already acknowledged" rather than as an anomaly.
                let Some(state) = state.filter(|row| row.get::<_, i32>(0) > 0) else {
                    return Err(OpError::Journal(JournalError::NotOutstanding {
                        delivery: refused,
                    }));
                };
                let (acknowledged, quarantined) =
                    (state.get::<_, bool>(1), state.get::<_, bool>(2));
                match poison {
                    // Idempotent in both directions, and exclusive: a repeated
                    // verdict is `Ok`, the opposite one is refused.
                    None if acknowledged => return Ok(()),
                    None if quarantined => {
                        return Err(OpError::Journal(JournalError::Quarantined {
                            delivery: refused,
                        }));
                    }
                    Some(_) if quarantined => return Ok(()),
                    Some(_) if acknowledged => {
                        return Err(OpError::Journal(JournalError::AlreadyAcknowledged {
                            delivery: refused,
                        }));
                    }
                    _ => {}
                }
                // Deliberately not gated on the lease or the attempt number: the
                // acknowledgement that matters most is the one a worker repeats
                // after a crash, and it can only repeat the delivery id it has.
                let sql = match poison {
                    None => {
                        "UPDATE axond_usage_outbox_delivery d
                         SET acknowledged_at = now(), lease_expires_at = NULL
                         FROM axond_usage_outbox e
                         WHERE e.position = d.position AND e.request_id = $1 AND d.consumer = $2"
                    }
                    Some(_) => {
                        "UPDATE axond_usage_outbox_delivery d
                         SET quarantined_at = now(), lease_expires_at = NULL, poison_reason = $3
                         FROM axond_usage_outbox e
                         WHERE e.position = d.position AND e.request_id = $1 AND d.consumer = $2"
                    }
                };
                match poison {
                    None => tx.execute(sql, &[&key, &name]).await?,
                    Some(reason) => tx.execute(sql, &[&key, &name, &reason.as_str()]).await?,
                };
                tx.commit().await?;
                Ok(())
            })
        })
        .await
    }
}

/// What one claim found it could not deliver, counted the way an operator reads
/// it: once per row, not once per look.
///
/// A schema-ahead row is left untouched on purpose, so it is still the head of
/// its ordering key on the claim's next selection pass — and a claim makes
/// another pass whenever it condemned something. Reporting it per pass would
/// turn one row a newer replica wrote into a burst on the undeliverable counter,
/// which is exactly the signal a rolling upgrade is being watched on. Corruption
/// needs no such guard: a corrupt row is quarantined in the same pass, so it
/// cannot be selected twice.
#[derive(Default)]
struct Undeliverable {
    reasons: Vec<&'static str>,
    ahead: HashSet<i64>,
}

impl Undeliverable {
    fn schema_ahead(&mut self, position: i64) {
        if self.ahead.insert(position) {
            self.reasons.push("schema_ahead");
        }
    }

    fn corrupt(&mut self) {
        self.reasons.push("corrupt");
    }
}

/// Raise every consumer's resolved prefix to just below its first unresolved
/// event.
///
/// This is what makes a claim's cost the backlog rather than the retained
/// history: acknowledged events stay for `retain_acknowledged`, and a selection
/// with no floor walks all of them on every poll. The scan here is the mirror
/// image — it starts at the old floor, follows `position` in index order, and
/// stops at the first row this consumer has not finished with (`ORDER BY … LIMIT
/// 1`), so the rows it examines are the ones it is about to skip forever. Run
/// once per maintenance tick rather than per claim, because the update takes the
/// consumer row's lock and replicas claim concurrently.
///
/// `GREATEST` makes it monotonic, and the `max(position)` fallback covers a
/// consumer that has finished everything: with no unresolved row there is no
/// position to sit below, and the floor is the end of the outbox.
const ADVANCE_RESOLVED_THROUGH: &str = "UPDATE axond_usage_outbox_consumer c
     SET resolved_through = GREATEST(
         c.resolved_through,
         COALESCE(
             (SELECT e.position - 1
              FROM axond_usage_outbox e
              LEFT JOIN axond_usage_outbox_delivery d
                  ON d.position = e.position AND d.consumer = c.consumer
              WHERE e.position > c.resolved_through
                AND d.acknowledged_at IS NULL AND d.quarantined_at IS NULL
              ORDER BY e.position
              LIMIT 1),
             (SELECT COALESCE(max(position), 0) FROM axond_usage_outbox)))";

/// An event every registered consumer has finished with, and nobody has
/// quarantined. The predicate retention and reclamation share, written once
/// because getting the two out of step is how a journal loses an undelivered
/// event.
const DELIVERED: &str = "EXISTS (SELECT 1 FROM axond_usage_outbox_consumer)
     AND NOT EXISTS (
         SELECT 1 FROM axond_usage_outbox_delivery d
         WHERE d.position = e.position AND d.quarantined_at IS NOT NULL)
     AND NOT EXISTS (
         SELECT 1 FROM axond_usage_outbox_consumer c
         WHERE NOT EXISTS (
             SELECT 1 FROM axond_usage_outbox_delivery d
             WHERE d.position = e.position AND d.consumer = c.consumer
               AND d.acknowledged_at IS NOT NULL))";

/// Give up the retention window on up to `wanted` delivered events, oldest
/// first. Lossless: every consumer already acknowledged them.
async fn reclaim_delivered(tx: &Transaction<'_>, wanted: u64) -> Result<u64, OpError> {
    let limit = i64::try_from(wanted).unwrap_or(i64::MAX);
    Ok(tx
        .execute(
            &format!(
                "DELETE FROM axond_usage_outbox WHERE position IN (
                     SELECT e.position FROM axond_usage_outbox e
                     WHERE {DELIVERED} ORDER BY e.position LIMIT $1)"
            ),
            &[&limit],
        )
        .await?)
}

/// Delete up to `wanted` oldest droppable events, raising the durable loss
/// total. The caller counts the loss in telemetry after the commit, so a
/// rolled-back drop is not reported as lost billing data.
///
/// A quarantined event is never a candidate: it is evidence an operator was
/// asked to look at, so a journal whose whole backlog is poison refuses instead.
async fn drop_oldest(tx: &Transaction<'_>, wanted: u64) -> Result<u64, OpError> {
    let limit = i64::try_from(wanted).unwrap_or(i64::MAX);
    let dropped = tx
        .execute(
            "DELETE FROM axond_usage_outbox WHERE position IN (
                 SELECT e.position FROM axond_usage_outbox e
                 WHERE NOT EXISTS (
                     SELECT 1 FROM axond_usage_outbox_delivery d
                     WHERE d.position = e.position AND d.quarantined_at IS NOT NULL)
                 ORDER BY e.position LIMIT $1)",
            &[&limit],
        )
        .await?;
    if dropped > 0 {
        let lost = i64::try_from(dropped).unwrap_or(i64::MAX);
        tx.execute(
            "UPDATE axond_usage_outbox_loss SET dropped = dropped + $1 WHERE id",
            &[&lost],
        )
        .await?;
    }
    Ok(dropped)
}

/// Take an event out of one consumer's delivery path, registering the attempt it
/// died on so the poison count and the attempt history agree. Telemetry is the
/// caller's, once the transaction that condemned the row has committed.
async fn condemn(
    tx: &Transaction<'_>,
    position: i64,
    consumer: &str,
    reason: PoisonReason,
    attempt: i32,
) -> Result<(), OpError> {
    tx.execute(
        "INSERT INTO axond_usage_outbox_delivery
             (position, consumer, attempts, quarantined_at, poison_reason)
         VALUES ($1, $2, $3, now(), $4)
         ON CONFLICT (position, consumer) DO UPDATE
             SET attempts = $3, quarantined_at = now(), poison_reason = $4,
                 lease_expires_at = NULL",
        &[&position, &consumer, &attempt, &reason.as_str()],
    )
    .await?;
    Ok(())
}

/// The row shape `record` is read back as.
///
/// A shadow of [`UsageRecord`], which cannot deserialize itself: its
/// `credential_source` is a `&'static str` from a closed vocabulary, so reading
/// one back is a checked mapping rather than a borrow. A value outside that
/// vocabulary is corruption, and the mapping is where it is caught.
#[derive(serde::Deserialize)]
struct StoredRecord {
    schema_version: u32,
    request_id: String,
    #[serde(default)]
    trace_id: Option<String>,
    namespace: String,
    subject: String,
    #[serde(default)]
    signer_kid: Option<String>,
    model: String,
    target_provider: String,
    target_model: String,
    credential_source: String,
    credential_id: String,
    status: Status,
    input_tokens: u64,
    cache_read_tokens: u64,
    cache_write_tokens: u64,
    output_tokens: u64,
    cost_microdollars: u64,
    catalog_version: u64,
    latency_ms: u64,
    attempts: u32,
}

impl StoredRecord {
    fn into_record(self) -> Result<UsageRecord, String> {
        let credential_source = match self.credential_source.as_str() {
            "platform" => "platform",
            "byok" => "byok",
            other => return Err(format!("`{other}` is not a credential source")),
        };
        Ok(UsageRecord {
            schema_version: self.schema_version,
            request_id: self.request_id,
            trace_id: self.trace_id,
            namespace: self.namespace,
            subject: self.subject,
            signer_kid: self.signer_kid,
            model: self.model,
            target_provider: self.target_provider,
            target_model: self.target_model,
            credential_source,
            credential_id: self.credential_id,
            status: self.status,
            input_tokens: self.input_tokens,
            cache_read_tokens: self.cache_read_tokens,
            cache_write_tokens: self.cache_write_tokens,
            output_tokens: self.output_tokens,
            cost_microdollars: self.cost_microdollars,
            catalog_version: self.catalog_version,
            latency_ms: self.latency_ms,
            attempts: self.attempts,
        })
    }
}

/// Rebuild the event a claim hands out, or say why the row is unreadable.
///
/// The row's own `request_id` column has to agree with the one inside the
/// record: they are the same identity written twice, so a disagreement means one
/// of them is not the identity a consumer deduplicates on.
fn decode(row: &tokio_postgres::Row) -> Result<UsageEvent, String> {
    let request_id: String = row.get(2);
    let stored: StoredRecord = serde_json::from_value(row.get::<_, serde_json::Value>(4))
        .map_err(|error| format!("the stored record is unreadable: {error}"))?;
    if stored.request_id != request_id {
        return Err(format!(
            "the row's `request_id` and the record's identity disagree: `{request_id}` \
             against `{}`",
            stored.request_id
        ));
    }
    let record = stored.into_record()?;
    UsageEvent::new(ObservedRecord {
        record,
        observed_at: row.get(5),
    })
    .map_err(|error| error.to_string())
}

fn backend(message: impl Into<String>) -> JournalError {
    JournalError::Backend(message.into())
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A journal operation's failure: a database error the pool may retry on a
/// fresh connection, or a contract decision it must not.
enum OpError {
    Db(tokio_postgres::Error),
    Journal(JournalError),
}

impl From<tokio_postgres::Error> for OpError {
    fn from(error: tokio_postgres::Error) -> Self {
        Self::Db(error)
    }
}

/// A fixed set of connections, each replaced on failure.
///
/// Small and hand-rolled rather than a pooling dependency: the outbox needs
/// exactly enough connections that a request's append does not queue behind the
/// delivery worker's claim.
struct Pool {
    config: Config,
    /// Re-applied on every connection, including reconnections: a reconnect that
    /// landed on the default schema would silently write another outbox.
    search_path: Option<String>,
    slots: Vec<tokio::sync::Mutex<Option<Client>>>,
    next: AtomicUsize,
}

impl Pool {
    fn new(config: Config, search_path: Option<String>, connections: usize) -> Self {
        Self {
            config,
            search_path,
            slots: (0..connections.max(1))
                .map(|_| tokio::sync::Mutex::new(None))
                .collect(),
            next: AtomicUsize::new(0),
        }
    }

    async fn run<T, F>(&self, op: &F) -> Result<T, JournalError>
    where
        T: Send,
        F: for<'a> Fn(&'a mut Client) -> BoxFuture<'a, Result<T, OpError>> + Send + Sync,
    {
        let mut guard = self.acquire().await;
        let mut last: Option<tokio_postgres::Error> = None;
        for _ in 0..2 {
            let mut client = match guard.take() {
                Some(client) if !client.is_closed() => client,
                _ => match self.connect().await {
                    Ok(client) => client,
                    Err(error) => {
                        last = Some(error);
                        continue;
                    }
                },
            };
            match op(&mut client).await {
                Ok(value) => {
                    *guard = Some(client);
                    return Ok(value);
                }
                Err(OpError::Journal(error)) => {
                    *guard = Some(client);
                    return Err(error);
                }
                // The connection is discarded: a failed transaction is not
                // safely reusable, and the retry starts from a clean session.
                Err(OpError::Db(error)) => last = Some(error),
            }
        }
        Err(backend(last.map_or_else(
            || "the usage journal operation failed".to_owned(),
            |error| error.to_string(),
        )))
    }

    /// A free connection if there is one, otherwise a fair wait on the next
    /// slot in rotation.
    async fn acquire(&self) -> tokio::sync::MutexGuard<'_, Option<Client>> {
        for slot in &self.slots {
            if let Ok(guard) = slot.try_lock() {
                return guard;
            }
        }
        let index = self.next.fetch_add(1, AtomicOrdering::Relaxed) % self.slots.len();
        self.slots[index].lock().await
    }

    async fn connect(&self) -> Result<Client, tokio_postgres::Error> {
        let (client, connection) = self.config.connect(crate::usage::tls_connector()).await?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::warn!(error = %error, "usage journal connection closed");
            }
        });
        if let Some(schema) = self.search_path.as_deref() {
            client
                .batch_execute(&format!("SET search_path TO {schema}"))
                .await?;
        }
        Ok(client)
    }
}

/// How many events the journal is holding, without counting them on every
/// append.
///
/// An exact `count(*)` per append would make the request path pay for the whole
/// backlog. The gate keeps the last exact count plus the appends since, and
/// insists on a fresh exact one whenever the estimate is stale or close enough
/// to the limit that the answer decides whether an event is refused — so the
/// bound is enforced exactly where it bites and estimated only where it cannot
/// matter.
struct CapacityGate {
    state: std::sync::Mutex<GateState>,
}

#[derive(Default)]
struct GateState {
    measured: u64,
    appended_since: u64,
    at: Option<Instant>,
}

impl CapacityGate {
    fn new() -> Self {
        Self {
            state: std::sync::Mutex::new(GateState::default()),
        }
    }

    /// A measurement no older than [`COUNT_REFRESH`], plus what this process has
    /// appended since, or nothing when there is none that fresh.
    ///
    /// This is what keeps the decision off the table on the request path: a
    /// replica measures at most once per refresh window however full the outbox
    /// is, rather than once per append. Between measurements the number moves
    /// only by this process's own appends, and every deletion this process makes
    /// invalidates it, so the staleness it can carry is another replica's
    /// deletion within the window — which can only make it read *fuller* than
    /// the outbox is. That is the safe direction for a limit whose job is to
    /// refuse, and it lasts at most a window.
    fn estimate(&self) -> Option<u64> {
        let state = self.state.lock().expect("capacity gate");
        (state.at?.elapsed() < COUNT_REFRESH)
            .then(|| state.measured.saturating_add(state.appended_since))
    }

    fn measured(&self, count: u64) {
        let mut state = self.state.lock().expect("capacity gate");
        *state = GateState {
            measured: count,
            appended_since: 0,
            at: Some(Instant::now()),
        };
    }

    fn appended(&self) {
        let mut state = self.state.lock().expect("capacity gate");
        state.appended_since = state.appended_since.saturating_add(1);
    }

    /// Forget the measurement: something deleted rows, so the next append has to
    /// count again rather than refuse on a number that is now too high.
    fn invalidate(&self) {
        self.state.lock().expect("capacity gate").at = None;
    }
}

/// The outbox against a real PostgreSQL, which is the only place the durability
/// claim can actually be tested: the oracle in
/// [`super::oracle`](super::oracle) says what the contract *is*, and these say
/// this implementation of it holds — including the two states no in-memory fake
/// can produce, a row a newer writer wrote and a row that is corrupt.
///
/// Each test owns a schema of its own, because the table names are fixed. Skipped
/// unless `AXOND_TEST_POSTGRES_DSN` is set (`AXOND_TEST_REQUIRE_SERVICES=1` turns
/// a skip into a failure, so CI cannot report a green run for tests that never
/// ran).
#[cfg(test)]
mod tests {
    use super::super::tests::{consumer, event_for};
    use super::*;

    fn capacity(max_events: u64, policy: CapacityPolicy) -> Capacity {
        Capacity {
            max_events,
            max_delivery_attempts: 3,
            retain_acknowledged: Duration::from_secs(3600),
            policy,
        }
    }

    fn settings(schema: &str, create_schema: bool, capacity: Capacity) -> PostgresJournalSettings {
        PostgresJournalSettings {
            schema: Some(schema.to_owned()),
            create_schema,
            capacity,
            ..PostgresJournalSettings::default()
        }
    }

    /// A connection into `schema` for the checks and the corruptions that only
    /// make sense from outside the journal's own API.
    async fn client(dsn: &str, schema: Option<&str>) -> Client {
        let (client, connection) = dsn
            .parse::<Config>()
            .expect("a test DSN")
            .connect(crate::usage::tls_connector())
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        if let Some(schema) = schema {
            client
                .batch_execute(&format!("SET search_path TO {schema}"))
                .await
                .expect("search_path");
        }
        client
    }

    /// A journal in a schema of its own, freshly created. `None` when no test
    /// Postgres is configured, which is the skip.
    async fn outbox(name: &str, capacity: Capacity) -> Option<(String, PostgresJournal)> {
        let dsn = crate::test_services::postgres_dsn()?;
        let schema = format!("axond_outbox_{name}");
        let admin = client(&dsn, None).await;
        admin
            .batch_execute(&format!(
                "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}"
            ))
            .await
            .expect("a schema of its own");
        let journal = PostgresJournal::connect(&dsn, settings(&schema, true, capacity))
            .await
            .expect("connect");
        Some((dsn, journal))
    }

    fn claim_at(max_events: usize, lease: Duration, now: SystemTime) -> Claim {
        Claim {
            max_events,
            lease,
            now,
        }
    }

    #[tokio::test]
    async fn an_outbox_that_is_not_there_refuses_to_boot() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let schema = "axond_outbox_missing";
        let admin = client(&dsn, None).await;
        admin
            .batch_execute(&format!(
                "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}"
            ))
            .await
            .expect("an empty schema");
        // Boot refuses rather than degrades: the alternative is a deployment that
        // starts, accepts a request, and only then finds it cannot make it durable.
        let error = PostgresJournal::connect(
            &dsn,
            settings(schema, false, capacity(16, CapacityPolicy::Refuse)),
        )
        .await
        .expect_err("an outbox that is not there is a boot failure");
        assert!(
            matches!(&error, JournalError::Backend(message) if message.contains("axond_usage_outbox")),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn an_appended_event_survives_the_process_that_appended_it() {
        let Some((dsn, journal)) = outbox("restart", capacity(16, CapacityPolicy::Refuse)).await
        else {
            return;
        };
        let event = event_for("GW_INBOUND_ACME_KEY");
        assert!(journal.append(&event).await.expect("append").is_new());
        // The crash: the process that appended goes away before anything was
        // claimed, and a new one connects to the same rows.
        drop(journal);
        let restarted = PostgresJournal::connect(
            &dsn,
            settings(
                "axond_outbox_restart",
                false,
                capacity(16, CapacityPolicy::Refuse),
            ),
        )
        .await
        .expect("reconnect");
        let billing = consumer("billing");

        let claimed = restarted
            .claim(
                &billing,
                claim_at(8, Duration::from_secs(30), SystemTime::now()),
            )
            .await
            .expect("claim");
        assert_eq!(claimed.len(), 1, "{claimed:?}");
        assert_eq!(claimed[0].event.id(), event.id());
        assert_eq!(claimed[0].event.record(), event.record());
        assert_eq!(claimed[0].id.attempt, 1);
        restarted.ack(&claimed[0].id).await.expect("ack");
        // Acknowledged state is durable too, so a restart resumes rather than
        // redelivering everything.
        assert!(
            restarted
                .claim(
                    &billing,
                    claim_at(8, Duration::from_secs(30), SystemTime::now())
                )
                .await
                .expect("claim")
                .is_empty()
        );
        let stats = restarted.stats(&billing).await.expect("stats");
        assert!(stats.is_drained(), "{stats:?}");
    }

    #[tokio::test]
    async fn appending_the_same_event_twice_journals_it_once() {
        let Some((_, journal)) = outbox("idempotent", capacity(16, CapacityPolicy::Refuse)).await
        else {
            return;
        };
        let event = event_for("GW_INBOUND_ACME_KEY");
        let first = journal.append(&event).await.expect("append");
        // The retry after an unknown outcome: the caller rebuilt the envelope, so
        // the observation time differs and the fact does not.
        let again = UsageEvent::new(ObservedRecord {
            record: event.record().clone(),
            observed_at: event.observed_at() + Duration::from_secs(5),
        })
        .expect("the same fact");
        let second = journal.append(&again).await.expect("append");
        assert!(first.is_new());
        assert!(!second.is_new(), "{second:?}");
        assert_eq!(first.position(), second.position());

        let mut different = event.record().clone();
        different.cost_microdollars += 1;
        let conflicting =
            UsageEvent::new(ObservedRecord::now(different)).expect("a well-formed event");
        let error = journal
            .append(&conflicting)
            .await
            .expect_err("the same identity with different content is a conflict");
        assert!(
            matches!(&error, JournalError::Conflict { key } if key == event.idempotency_key()),
            "{error:?}"
        );
        // And the stored event is the first one, untouched.
        let claimed = journal
            .claim(
                &consumer("billing"),
                claim_at(8, Duration::from_secs(30), SystemTime::now()),
            )
            .await
            .expect("claim");
        assert_eq!(claimed.len(), 1, "{claimed:?}");
        assert_eq!(
            claimed[0].event.record().cost_microdollars,
            event.record().cost_microdollars
        );
    }

    #[tokio::test]
    async fn an_expired_lease_redelivers_the_event_as_a_new_attempt() {
        let Some((_, journal)) = outbox("lease", capacity(16, CapacityPolicy::Refuse)).await else {
            return;
        };
        let event = event_for("GW_INBOUND_ACME_KEY");
        journal.append(&event).await.expect("append");
        let billing = consumer("billing");
        let now = SystemTime::now();

        let first = journal
            .claim(&billing, claim_at(8, Duration::from_secs(30), now))
            .await
            .expect("claim");
        assert_eq!(first.len(), 1);
        // A live lease keeps the event to itself, so two workers cannot deliver it
        // side by side.
        assert!(
            journal
                .claim(&billing, claim_at(8, Duration::from_secs(30), now))
                .await
                .expect("claim")
                .is_empty()
        );
        // Past the lease, the crashed worker's event comes back as a redelivery —
        // the same billable identity, a different delivery.
        let again = journal
            .claim(
                &billing,
                claim_at(8, Duration::from_secs(30), now + Duration::from_secs(31)),
            )
            .await
            .expect("claim");
        assert_eq!(again.len(), 1, "{again:?}");
        assert_eq!(again[0].event.id(), event.id());
        assert_eq!(again[0].id.attempt, 2);
        assert!(again[0].id.is_redelivery());
        // The acknowledgement a crashed worker repeats is honoured, and the one
        // after it is a no-op rather than an error.
        journal.ack(&first[0].id).await.expect("late ack");
        journal.ack(&again[0].id).await.expect("idempotent ack");
    }

    #[tokio::test]
    async fn one_callers_events_are_claimed_in_order_and_one_at_a_time() {
        let Some((_, journal)) = outbox("ordering", capacity(16, CapacityPolicy::Refuse)).await
        else {
            return;
        };
        let first = event_for("GW_INBOUND_ACME_KEY");
        let second = event_for("GW_INBOUND_ACME_KEY");
        let other = event_for("GW_INBOUND_OTHER_KEY");
        for event in [&first, &second, &other] {
            journal.append(event).await.expect("append");
        }
        let billing = consumer("billing");
        let now = SystemTime::now();

        // One event per ordering key, and the earliest of each: a slow caller
        // cannot hold up another caller's events, and its own stay in order.
        let claimed = journal
            .claim(&billing, claim_at(8, Duration::from_secs(30), now))
            .await
            .expect("claim");
        let ids: Vec<_> = claimed.iter().map(|d| d.event.id()).collect();
        assert_eq!(ids, vec![first.id(), other.id()], "{ids:?}");
        journal.ack(&claimed[0].id).await.expect("ack");
        let next = journal
            .claim(&billing, claim_at(8, Duration::from_secs(30), now))
            .await
            .expect("claim");
        let ids: Vec<_> = next.iter().map(|d| d.event.id()).collect();
        assert_eq!(ids, vec![second.id()], "{ids:?}");
    }

    #[tokio::test]
    async fn a_delivery_that_was_never_claimed_cannot_be_acknowledged() {
        let Some((_, journal)) = outbox("stray", capacity(16, CapacityPolicy::Refuse)).await else {
            return;
        };
        let event = event_for("GW_INBOUND_ACME_KEY");
        journal.append(&event).await.expect("append");
        let stray = DeliveryId {
            consumer: consumer("billing"),
            event: event.id(),
            attempt: 1,
        };
        let error = journal
            .ack(&stray)
            .await
            .expect_err("a consumer that never claimed has nothing to acknowledge");
        assert!(
            matches!(error, JournalError::NotOutstanding { .. }),
            "{error:?}"
        );
        // And it did not register a consumer retention would then wait on.
        let stats = journal.stats(&consumer("billing")).await.expect("stats");
        assert_eq!(stats.pending, 1, "{stats:?}");
    }

    #[tokio::test]
    async fn an_event_that_exhausts_its_attempts_is_quarantined_not_retried_forever() {
        let Some((_, journal)) = outbox(
            "attempts",
            Capacity {
                max_delivery_attempts: 2,
                ..capacity(16, CapacityPolicy::Refuse)
            },
        )
        .await
        else {
            return;
        };
        let blocked = event_for("GW_INBOUND_ACME_KEY");
        let behind = event_for("GW_INBOUND_ACME_KEY");
        journal.append(&blocked).await.expect("append");
        journal.append(&behind).await.expect("append");
        let billing = consumer("billing");
        let mut now = SystemTime::now();

        for attempt in 1..=2 {
            let claimed = journal
                .claim(&billing, claim_at(8, Duration::from_secs(1), now))
                .await
                .expect("claim");
            assert_eq!(claimed.len(), 1, "attempt {attempt}: {claimed:?}");
            assert_eq!(claimed[0].event.id(), blocked.id());
            now += Duration::from_secs(2);
        }
        // The third pass condemns it instead of handing it out again, and the
        // event behind it — the same caller's next request — is delivered.
        let claimed = journal
            .claim(&billing, claim_at(8, Duration::from_secs(1), now))
            .await
            .expect("claim");
        assert_eq!(claimed.len(), 1, "{claimed:?}");
        assert_eq!(claimed[0].event.id(), behind.id());
        let stats = journal.stats(&billing).await.expect("stats");
        assert_eq!(stats.quarantined, 1, "{stats:?}");
        // The evidence is still there for an operator, and an acknowledgement
        // cannot quietly release it.
        let error = journal
            .ack(&DeliveryId {
                consumer: billing.clone(),
                event: blocked.id(),
                attempt: 2,
            })
            .await
            .expect_err("quarantine is terminal");
        assert!(
            matches!(error, JournalError::Quarantined { .. }),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn a_full_outbox_refuses_the_append_rather_than_dropping_usage() {
        let Some((_, journal)) = outbox("refuse", capacity(1, CapacityPolicy::Refuse)).await else {
            return;
        };
        journal
            .append(&event_for("GW_INBOUND_ACME_KEY"))
            .await
            .expect("append");
        let error = journal
            .append(&event_for("GW_INBOUND_ACME_KEY"))
            .await
            .expect_err("a full billing-grade outbox refuses");
        assert!(
            matches!(
                error,
                JournalError::AtCapacity { pending, capacity } if pending >= 1 && capacity.max_events == 1
            ),
            "{error:?}"
        );
        let stats = journal.stats(&consumer("billing")).await.expect("stats");
        assert_eq!(stats.dropped, 0, "a refusal is not a loss: {stats:?}");
    }

    #[tokio::test]
    async fn drop_oldest_bounds_storage_and_counts_what_it_lost() {
        let Some((_, journal)) =
            outbox("drop_oldest", capacity(1, CapacityPolicy::DropOldest)).await
        else {
            return;
        };
        journal
            .append(&event_for("GW_INBOUND_ACME_KEY"))
            .await
            .expect("append");
        let kept = event_for("GW_INBOUND_ACME_KEY");
        journal.append(&kept).await.expect("append");
        let billing = consumer("billing");

        let stats = journal.stats(&billing).await.expect("stats");
        assert_eq!(stats.pending, 1, "{stats:?}");
        // The loss is reported rather than inferred, which is the whole point of
        // making the lossy policy explicit.
        assert_eq!(stats.dropped, 1, "{stats:?}");
        let claimed = journal
            .claim(
                &billing,
                claim_at(8, Duration::from_secs(30), SystemTime::now()),
            )
            .await
            .expect("claim");
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].event.id(), kept.id());
    }

    #[tokio::test]
    async fn a_delivered_event_yields_its_retention_window_before_an_append_is_refused() {
        let Some((_, journal)) = outbox("reclaim", capacity(1, CapacityPolicy::Refuse)).await
        else {
            return;
        };
        let delivered = event_for("GW_INBOUND_ACME_KEY");
        journal.append(&delivered).await.expect("append");
        let billing = consumer("billing");
        let claimed = journal
            .claim(
                &billing,
                claim_at(8, Duration::from_secs(30), SystemTime::now()),
            )
            .await
            .expect("claim");
        journal.ack(&claimed[0].id).await.expect("ack");

        // A re-acknowledgement is cheaper than a refusal, so the courtesy window
        // goes first and the new event is accepted.
        let next = event_for("GW_INBOUND_ACME_KEY");
        assert!(journal.append(&next).await.expect("append").is_new());
        let stats = journal.stats(&billing).await.expect("stats");
        assert_eq!(stats.pending, 1, "{stats:?}");
        assert_eq!(stats.dropped, 0, "a reclaim is not a loss: {stats:?}");
    }

    #[tokio::test]
    async fn retention_prunes_only_what_every_consumer_finished_with() {
        let Some((dsn, journal)) = outbox(
            "retention",
            Capacity {
                retain_acknowledged: Duration::ZERO,
                ..capacity(16, CapacityPolicy::Refuse)
            },
        )
        .await
        else {
            return;
        };
        let acknowledged = event_for("GW_INBOUND_ACME_KEY");
        let pending = event_for("GW_INBOUND_OTHER_KEY");
        journal.append(&acknowledged).await.expect("append");
        journal.append(&pending).await.expect("append");
        let billing = consumer("billing");
        let claimed = journal
            .claim(
                &billing,
                claim_at(8, Duration::from_secs(30), SystemTime::now()),
            )
            .await
            .expect("claim");
        let first = claimed
            .iter()
            .find(|delivery| delivery.event.id() == acknowledged.id())
            .expect("the acknowledged event was claimed");
        journal.ack(&first.id).await.expect("ack");

        assert_eq!(journal.maintain(SystemTime::now()).await.expect("prune"), 1);
        let rows: i64 = client(&dsn, Some("axond_outbox_retention"))
            .await
            .query_one("SELECT count(*) FROM axond_usage_outbox", &[])
            .await
            .expect("count")
            .get(0);
        assert_eq!(rows, 1, "only the delivered event was pruned");
        let stats = journal.stats(&billing).await.expect("stats");
        assert_eq!(stats.in_flight + stats.pending, 1, "{stats:?}");
    }

    #[tokio::test]
    async fn a_row_a_newer_build_wrote_is_left_for_that_build_to_deliver() {
        let Some((dsn, journal)) =
            outbox("schema_ahead", capacity(16, CapacityPolicy::Refuse)).await
        else {
            return;
        };
        let event = event_for("GW_INBOUND_ACME_KEY");
        journal.append(&event).await.expect("append");
        let ahead = i32::try_from(UsageRecord::SCHEMA_VERSION).expect("a small version") + 1;
        client(&dsn, Some("axond_outbox_schema_ahead"))
            .await
            .execute(
                "UPDATE axond_usage_outbox SET schema_version = $1",
                &[&ahead],
            )
            .await
            .expect("the row a rolling upgrade's newer replica wrote");
        let billing = consumer("billing");

        // Skipped untouched: no attempt spent, no verdict, no lease. The replica
        // that can read it delivers it.
        assert!(
            journal
                .claim(
                    &billing,
                    claim_at(8, Duration::from_secs(30), SystemTime::now())
                )
                .await
                .expect("claim")
                .is_empty()
        );
        let stats = journal.stats(&billing).await.expect("stats");
        assert_eq!(stats.quarantined, 0, "{stats:?}");
        assert_eq!(stats.pending, 1, "{stats:?}");
    }

    #[tokio::test]
    async fn a_row_this_build_cannot_decode_is_quarantined_rather_than_blocking_its_key() {
        let Some((dsn, journal)) = outbox("corrupt", capacity(16, CapacityPolicy::Refuse)).await
        else {
            return;
        };
        let corrupt = event_for("GW_INBOUND_ACME_KEY");
        let behind = event_for("GW_INBOUND_ACME_KEY");
        journal.append(&corrupt).await.expect("append");
        journal.append(&behind).await.expect("append");
        client(&dsn, Some("axond_outbox_corrupt"))
            .await
            .execute(
                "UPDATE axond_usage_outbox \
                 SET record = jsonb_set(record, '{credential_source}', '\"nonsense\"') \
                 WHERE request_id = $1",
                &[&corrupt.id().to_string()],
            )
            .await
            .expect("corruption at this build's own version");
        let billing = consumer("billing");

        // Corruption is not a version skew: retrying it forever would block the
        // caller's later events, so it leaves the delivery path here.
        let claimed = journal
            .claim(
                &billing,
                claim_at(8, Duration::from_secs(30), SystemTime::now()),
            )
            .await
            .expect("claim");
        assert_eq!(claimed.len(), 1, "{claimed:?}");
        assert_eq!(claimed[0].event.id(), behind.id());
        let stats = journal.stats(&billing).await.expect("stats");
        assert_eq!(stats.quarantined, 1, "{stats:?}");
        // Still on disk: an operator was asked to look at it.
        let rows: i64 = client(&dsn, Some("axond_outbox_corrupt"))
            .await
            .query_one("SELECT count(*) FROM axond_usage_outbox", &[])
            .await
            .expect("count")
            .get(0);
        assert_eq!(rows, 2);
    }

    #[tokio::test]
    async fn consumers_acknowledge_independently() {
        let Some((_, journal)) = outbox("consumers", capacity(16, CapacityPolicy::Refuse)).await
        else {
            return;
        };
        let event = event_for("GW_INBOUND_ACME_KEY");
        journal.append(&event).await.expect("append");
        let (billing, warehouse) = (consumer("billing"), consumer("warehouse"));
        let now = SystemTime::now();

        let claimed = journal
            .claim(&billing, claim_at(8, Duration::from_secs(30), now))
            .await
            .expect("claim");
        journal.ack(&claimed[0].id).await.expect("ack");
        // The second destination has its own delivery state, so the first one's
        // acknowledgement neither delivers nor hides the event.
        let other = journal
            .claim(&warehouse, claim_at(8, Duration::from_secs(30), now))
            .await
            .expect("claim");
        assert_eq!(other.len(), 1, "{other:?}");
        assert_eq!(other[0].event.id(), event.id());
        let stats = journal.stats(&billing).await.expect("stats");
        assert!(stats.is_drained(), "{stats:?}");
        let stats = journal.stats(&warehouse).await.expect("stats");
        assert_eq!(stats.in_flight, 1, "{stats:?}");
    }

    #[tokio::test]
    async fn stats_report_the_backlog_and_the_bound_it_is_measured_against() {
        let Some((_, journal)) = outbox("stats", capacity(16, CapacityPolicy::Refuse)).await else {
            return;
        };
        let mut record = event_for("GW_INBOUND_ACME_KEY").record().clone();
        record.request_id = crate::usage::identity::next_request_id().to_string();
        let old = UsageEvent::new(ObservedRecord {
            record,
            observed_at: SystemTime::now() - Duration::from_secs(120),
        })
        .expect("a well-formed event");
        journal.append(&old).await.expect("append");

        let stats = journal.stats(&consumer("billing")).await.expect("stats");
        assert_eq!(stats.pending, 1, "{stats:?}");
        assert_eq!(stats.capacity, journal.capacity());
        // The age is what says how far behind a bill is; a depth alone does not.
        let age = stats.oldest_pending_age.expect("an age");
        assert!(age >= Duration::from_secs(100), "{age:?}");
    }

    /// One row a newer replica wrote is one undeliverable event, however many
    /// times a claim's selection passes look at it.
    #[test]
    fn a_schema_ahead_row_is_reported_once_per_claim() {
        let mut undeliverable = Undeliverable::default();
        undeliverable.schema_ahead(7);
        undeliverable.schema_ahead(7);
        undeliverable.schema_ahead(9);
        undeliverable.corrupt();
        assert_eq!(
            undeliverable.reasons,
            vec!["schema_ahead", "schema_ahead", "corrupt"],
            "one report per row, and corruption is never re-selected"
        );
    }

    /// The capacity decision near the limit, which is where it is both load-bearing
    /// and expensive: a full outbox must keep refusing without measuring itself
    /// again on every request, and must accept again once there is room.
    #[tokio::test]
    async fn a_full_outbox_refuses_from_a_bounded_measurement_and_recovers() {
        let Some((dsn, journal)) = outbox("near_full", capacity(2, CapacityPolicy::Refuse)).await
        else {
            return;
        };
        journal
            .append(&event_for("GW_INBOUND_ACME_KEY"))
            .await
            .expect("append");
        // The append that fills it: the last one below the limit still succeeds,
        // which is the near-full case the exact measurement exists for.
        journal
            .append(&event_for("GW_INBOUND_ACME_KEY"))
            .await
            .expect("the outbox is full only after this one");
        for _ in 0..3 {
            let error = journal
                .append(&event_for("GW_INBOUND_ACME_KEY"))
                .await
                .expect_err("a full billing-grade outbox refuses");
            assert!(
                matches!(error, JournalError::AtCapacity { pending, .. } if pending >= 2),
                "{error:?}"
            );
        }

        // Room appears without this process making it — another replica's
        // retention, or an operator. The measurement a refusal was made on expires,
        // so the next append measures again and accepts.
        client(&dsn, Some("axond_outbox_near_full"))
            .await
            .execute(
                "DELETE FROM axond_usage_outbox WHERE position = \
                 (SELECT min(position) FROM axond_usage_outbox)",
                &[],
            )
            .await
            .expect("make room");
        tokio::time::sleep(COUNT_REFRESH + Duration::from_millis(50)).await;
        assert!(
            journal
                .append(&event_for("GW_INBOUND_ACME_KEY"))
                .await
                .expect("an outbox with room accepts")
                .is_new()
        );
    }

    /// Two replicas of one consumer, claiming at the same instant. The delivery
    /// lease is the only thing that decides ownership, so the two must partition
    /// the events rather than both deliver one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_replicas_cannot_claim_the_same_event() {
        let Some((dsn, first)) = outbox("concurrent", capacity(64, CapacityPolicy::Refuse)).await
        else {
            return;
        };
        // A second journal on its own connections, which is what a second replica
        // is: same schema, same consumer, no shared state in this process.
        let second = PostgresJournal::connect(
            &dsn,
            settings(
                "axond_outbox_concurrent",
                false,
                capacity(64, CapacityPolicy::Refuse),
            ),
        )
        .await
        .expect("a second replica");
        let billing = consumer("billing");
        let mut in_flight = 0u64;
        // Repeated, because the window this closes is small: the loser has to have
        // selected the event before the winner committed its lease.
        for _ in 0..8 {
            // Distinct ordering keys, so a claim may take both: one key at a time is
            // the ordering guarantee, and with a single key it would hide the race.
            for key in ["GW_INBOUND_ACME_KEY", "GW_INBOUND_OTHER_KEY"] {
                first.append(&event_for(key)).await.expect("append");
            }
            let now = SystemTime::now();
            let (left, right) = tokio::join!(
                first.claim(&billing, claim_at(8, Duration::from_secs(30), now)),
                second.claim(&billing, claim_at(8, Duration::from_secs(30), now)),
            );
            let mut delivered: Vec<_> = left
                .expect("claim")
                .into_iter()
                .chain(right.expect("claim"))
                .map(|delivery| delivery.event.id())
                .collect();
            let claimed = delivered.len();
            delivered.sort();
            delivered.dedup();
            assert_eq!(
                delivered.len(),
                claimed,
                "an event was handed to both replicas at once"
            );
            in_flight += claimed as u64;
            // And what the two of them together took is what the outbox thinks is
            // leased: a lease neither of them owns would show up here.
            let stats = first.stats(&billing).await.expect("stats");
            assert_eq!(stats.in_flight, in_flight, "{stats:?}");
        }
    }

    /// The floor a claim starts from. Acknowledged events stay for the retention
    /// window, and a claim must not walk them again on every poll.
    #[tokio::test]
    async fn maintenance_moves_the_claim_floor_past_the_acknowledged_prefix() {
        let Some((dsn, journal)) = outbox("floor", capacity(64, CapacityPolicy::Refuse)).await
        else {
            return;
        };
        let billing = consumer("billing");
        let now = SystemTime::now();
        // A retained acknowledged prefix: claimed, acknowledged, and still stored,
        // because the retention window has not passed.
        for _ in 0..4 {
            journal
                .append(&event_for("GW_INBOUND_ACME_KEY"))
                .await
                .expect("append");
            let claimed = journal
                .claim(&billing, claim_at(1, Duration::from_secs(30), now))
                .await
                .expect("claim");
            journal.ack(&claimed[0].id).await.expect("ack");
        }
        let pending = event_for("GW_INBOUND_ACME_KEY");
        journal.append(&pending).await.expect("append");

        // Nothing is pruned: every row is inside its retention window.
        assert_eq!(journal.maintain(now).await.expect("maintain"), 0);
        let admin = client(&dsn, Some("axond_outbox_floor")).await;
        let floor: i64 = admin
            .query_one(
                "SELECT resolved_through FROM axond_usage_outbox_consumer WHERE consumer = $1",
                &[&billing.as_str()],
            )
            .await
            .expect("the floor")
            .get(0);
        let first_open: i64 = admin
            .query_one(
                "SELECT min(e.position) FROM axond_usage_outbox e \
                 LEFT JOIN axond_usage_outbox_delivery d \
                     ON d.position = e.position AND d.consumer = $1 \
                 WHERE d.acknowledged_at IS NULL AND d.quarantined_at IS NULL",
                &[&billing.as_str()],
            )
            .await
            .expect("the first unresolved position")
            .get(0);
        assert_eq!(
            floor,
            first_open - 1,
            "the floor sits just below the oldest event still to deliver"
        );
        // And the event past the floor is still delivered, which is the half of
        // this that a floor could break.
        let claimed = journal
            .claim(&billing, claim_at(8, Duration::from_secs(30), now))
            .await
            .expect("claim");
        assert_eq!(claimed.len(), 1, "{claimed:?}");
        assert_eq!(claimed[0].event.id(), pending.id());
    }
}
