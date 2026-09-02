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

/// How long an appended row is left alone before the claim floor may pass it.
///
/// See [`ADVANCE_RESOLVED_THROUGH`]: it only has to exceed the longest an append
/// transaction can hold an allocated `position` open, which
/// [`PostgresJournalSettings::operation_timeout`] bounds. Five minutes is
/// generous against that and cheap, because the only cost of a late floor is a
/// claim scanning a few more rows for one tick.
const FLOOR_SETTLE_MARGIN: Duration = Duration::from_secs(300);

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
    /// Connections held open. One is reserved for the delivery worker, so the
    /// rest are the appends a replica can have in flight at once.
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
            connections: 8,
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
        if settings.connections < 2 {
            return Err(backend(
                "a usage journal needs at least two connections: one is reserved for the \
                 delivery worker so its claims cannot stall the appends requests wait on",
            ));
        }

        let journal = Self {
            pool: Pool::new(config, search_path, settings.connections),
            settings,
            stored: Arc::new(CapacityGate::new()),
        };
        if journal.settings.create_schema {
            journal
                .run("apply schema", Lane::Request, |client| {
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
    ///
    /// It names the columns rather than selecting a constant, so a table an older
    /// copy of the DDL created — `axond_usage_outbox_consumer` without
    /// `resolved_through`, which every claim reads — is a boot refusal rather than
    /// a runtime error on the first claim.
    async fn check_schema(&self) -> Result<(), JournalError> {
        self.run("check schema", Lane::Request, |client| {
            Box::pin(async move {
                for (table, columns) in [
                    (
                        "axond_usage_outbox",
                        "position, request_id, schema_version, namespace, subject, record, \
                         observed_at, appended_at",
                    ),
                    (
                        "axond_usage_outbox_consumer",
                        "consumer, registered_at, resolved_through",
                    ),
                    (
                        "axond_usage_outbox_delivery",
                        "position, consumer, attempts, lease_expires_at, acknowledged_at, \
                         quarantined_at, poison_reason",
                    ),
                    ("axond_usage_outbox_loss", "id, dropped"),
                ] {
                    if let Err(error) = client
                        .query_opt(&format!("SELECT {columns} FROM {table} LIMIT 1"), &[])
                        .await
                    {
                        return Err(OpError::Journal(backend(format!(
                            "`{table}` is not readable with the columns this build needs, so it \
                             cannot own the usage outbox; apply (or re-apply) \
                             `ops/postgres/usage_outbox_v1.sql` (or set \
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
    async fn run<T, F>(&self, what: &'static str, lane: Lane, op: F) -> Result<T, JournalError>
    where
        T: Send,
        F: for<'a> Fn(&'a mut Client) -> BoxFuture<'a, Result<T, OpError>> + Send + Sync,
    {
        match tokio::time::timeout(self.settings.operation_timeout, self.pool.run(&op, lane)).await
        {
            Ok(result) => result,
            Err(_) => Err(backend(format!(
                "`{what}` exceeded its {:?} bound",
                self.settings.operation_timeout
            ))),
        }
    }
}

/// How many events are stored: the position span, less the gaps in it that the
/// gate knows about, or a fresh bounded count when it does not.
///
/// Bounded three ways over, because this runs inside the append a request is
/// waiting on and an unbounded `count(*)` gets slower exactly as the outbox falls
/// behind, which is when appends can least afford it. The span is two index probes
/// on [`SPAN`]; a span below the limit *is* the answer, because the span can only
/// overstate how many rows it covers, so an outbox that is not spanning its limit
/// never counts at all; a replica counts at most once per [`COUNT_REFRESH`]
/// otherwise; and a count stops at `max_events + 1` rows, the largest number the
/// decision can distinguish, since anything above the limit is over it.
async fn stored_events(
    tx: &Transaction<'_>,
    gate: &CapacityGate,
    max_events: u64,
) -> Result<u64, OpError> {
    let span = tx.query_one(SPAN, &[]).await?.get::<_, i64>(0).max(0) as u64;
    if span < max_events {
        // An upper bound that admits, so the cheapest correct answer: the rows
        // stored cannot outnumber the positions they occupy.
        return Ok(span);
    }
    if let Some(estimate) = gate.estimate(span, max_events) {
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
    gate.measured(span, counted, max_events);
    Ok(counted)
}

/// How many positions the outbox currently covers. `max`/`min` over the primary
/// key, so it is two index probes whatever the backlog, and it moves with every
/// replica's appends rather than only this one's.
const SPAN: &str = "SELECT COALESCE(max(position) - min(position) + 1, 0) FROM axond_usage_outbox";

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
        self.run("append", Lane::Request, move |client| {
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
                let stored = stored_events(&tx, &gate, capacity.max_events).await?;
                if stored >= capacity.max_events {
                    if gate.refusing() {
                        // A recent append already established that this backlog
                        // holds nothing anybody may delete. Refusing on that
                        // costs nothing, which is the point: the probe below is
                        // an ordered walk to the limit-th newest position, and a
                        // request that is going to be turned away must not run
                        // it once per attempt while the outbox stays full.
                        return Err(OpError::Journal(JournalError::AtCapacity {
                            pending: stored,
                            capacity,
                        }));
                    }
                    // `stored` is trusted only for *whether* the outbox is at its
                    // limit, never for how far over it is: a count stops at
                    // `max_events + 1` and a cached one lags deletions, so
                    // subtracting it from the limit could free two rows out of a
                    // hundred surplus and admit anyway. How much room to make is
                    // therefore decided by position — everything at or below the
                    // surplus cutoff is beyond the limit, whatever the count says.
                    let mut surplus = surplus_cutoff(&tx, capacity.max_events).await?;
                    // Space that is only owed to a courtesy window goes first:
                    // giving up a delivered event costs a redundant
                    // re-acknowledgement, refusing costs an undelivered event.
                    let mut reclaimed = 0;
                    if let Some(cutoff) = surplus {
                        reclaimed = reclaim_delivered(&tx, cutoff).await?;
                        if reclaimed > 0 {
                            surplus = surplus_cutoff(&tx, capacity.max_events).await?;
                        }
                    }
                    if let Some(cutoff) = surplus
                        && capacity.policy == CapacityPolicy::DropOldest
                    {
                        dropped = drop_oldest(&tx, cutoff).await?;
                        if dropped > 0 {
                            surplus = surplus_cutoff(&tx, capacity.max_events).await?;
                        }
                    }
                    if reclaimed > 0 || dropped > 0 {
                        gate.invalidate();
                    }
                    if surplus.is_some() {
                        // Everything left is either undelivered or somebody's
                        // quarantined evidence, so there is no room to make.
                        if reclaimed > 0 || dropped > 0 {
                            // Whatever room this attempt did make is committed
                            // even though the append itself fails: rolling it
                            // back would make every refused request re-delete
                            // the same rows and write the same WAL for nothing.
                            tx.commit().await?;
                            if dropped > 0 {
                                crate::telemetry::metrics::record_usage_journal_lost(
                                    BACKEND,
                                    "capacity_drop",
                                    dropped,
                                );
                            }
                        } else {
                            // Nothing was freed, so nothing until a deletion can
                            // change the answer: the verdict is remembered and
                            // the next append refuses on it without probing.
                            // It expires on its own inside a second, so a
                            // refusal cannot outlive the backlog that caused it.
                            gate.unreclaimable();
                        }
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

        self.run("claim", Lane::Delivery, move |client| {
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
                            if condemn(
                                &tx,
                                position,
                                &name,
                                PoisonReason::AttemptsExhausted,
                                attempt,
                            )
                            .await?
                            {
                                condemnations.push(PoisonReason::AttemptsExhausted);
                                condemned += 1;
                            }
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
                                if condemn(&tx, position, &name, PoisonReason::Malformed, attempt)
                                    .await?
                                {
                                    condemnations.push(PoisonReason::Malformed);
                                    condemned += 1;
                                }
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

    /// One statement for the whole claim, then [`Self::ack`] for whatever it did
    /// not resolve.
    ///
    /// The bulk `UPDATE` deliberately only touches the unambiguous case — a
    /// delivery this consumer holds that has no verdict yet — because everything
    /// else (the event pruned underneath the claim, a quarantine, a stray
    /// acknowledgement) is a distinct answer the contract owes the caller, and
    /// `verdict` is where those distinctions live. In steady state nothing is
    /// left over, so a claim of 256 costs one round trip instead of a thousand.
    async fn ack_all(&self, deliveries: &[DeliveryId]) -> Vec<Result<(), JournalError>> {
        if deliveries.len() < 2 {
            let mut verdicts = Vec::with_capacity(deliveries.len());
            for delivery in deliveries {
                verdicts.push(self.ack(delivery).await);
            }
            return verdicts;
        }
        // A claim is one consumer's, so the statement is written for that one
        // and anything else in the set falls through to the single-event path.
        let name = deliveries[0].consumer.as_str().to_owned();
        let keys: Vec<String> = deliveries
            .iter()
            .filter(|delivery| delivery.consumer.as_str() == name)
            .map(|delivery| delivery.event.to_string())
            .collect();
        let resolved = self
            .run("ack_all", Lane::Delivery, {
                let (name, keys) = (name.clone(), keys.clone());
                move |client| {
                    let (name, keys) = (name.clone(), keys.clone());
                    Box::pin(async move {
                        // Not gated on the lease or the attempt number, for the
                        // same reason `verdict` is not: the acknowledgement that
                        // matters most is the one repeated after a crash.
                        let rows = client
                            .query(
                                "UPDATE axond_usage_outbox_delivery d
                                 SET acknowledged_at = now(), lease_expires_at = NULL
                                 FROM axond_usage_outbox e
                                 WHERE e.position = d.position
                                   AND d.consumer = $2
                                   AND e.request_id = ANY($1)
                                   AND d.attempts > 0
                                   AND d.acknowledged_at IS NULL
                                   AND d.quarantined_at IS NULL
                                 RETURNING e.request_id",
                                &[&keys, &name],
                            )
                            .await?;
                        Ok(rows
                            .iter()
                            .map(|row| row.get::<_, String>(0))
                            .collect::<HashSet<String>>())
                    })
                }
            })
            .await;
        // A failed statement leaves nothing known to be acknowledged, so every
        // event falls through to the single-event path rather than being
        // reported as an error the worker would warn about.
        let resolved: HashSet<String> = resolved.unwrap_or_default();
        let mut verdicts = Vec::with_capacity(deliveries.len());
        for delivery in deliveries {
            let resolved = delivery.consumer.as_str() == name
                && resolved.contains(&delivery.event.to_string());
            if resolved {
                verdicts.push(Ok(()));
            } else {
                verdicts.push(self.ack(delivery).await);
            }
        }
        verdicts
    }

    async fn quarantine(
        &self,
        delivery: &DeliveryId,
        reason: PoisonReason,
    ) -> Result<(), JournalError> {
        self.verdict(delivery, Some(reason)).await
    }

    async fn relinquish(&self, delivery: &DeliveryId) -> Result<(), JournalError> {
        let (name, key) = (
            delivery.consumer.as_str().to_owned(),
            delivery.event.to_string(),
        );
        let attempt = i32::try_from(delivery.attempt).unwrap_or(i32::MAX);
        let refused = delivery.clone();
        self.run("relinquish", Lane::Delivery, move |client| {
            let (name, key, refused) = (name.clone(), key.clone(), refused.clone());
            Box::pin(async move {
                // Gated on the attempt this delivery spent, so a refund cannot
                // undo an attempt a later claim already made, and on the event
                // being unresolved, so it cannot revive a verdict. Both make it
                // safe to repeat.
                let refunded = client
                    .execute(
                        "UPDATE axond_usage_outbox_delivery d
                         SET attempts = d.attempts - 1
                         FROM axond_usage_outbox e
                         WHERE e.position = d.position AND e.request_id = $1
                           AND d.consumer = $2 AND d.attempts = $3
                           AND d.acknowledged_at IS NULL AND d.quarantined_at IS NULL",
                        &[&key, &name, &attempt],
                    )
                    .await?;
                if refunded == 0 {
                    // Either somebody else moved the delivery on, which is an
                    // ordinary race, or there is no delivery row at all — the
                    // one case a consumer should hear about, and only while the
                    // event itself is still here. An event retention or capacity
                    // took has no attempt left to give back and no later delivery
                    // to protect, so saying so is not an anomaly.
                    let exists = client
                        .query_opt(
                            "SELECT 1 FROM axond_usage_outbox e
                             JOIN axond_usage_outbox_delivery d
                                 ON d.position = e.position AND d.consumer = $2
                             WHERE e.request_id = $1",
                            &[&key, &name],
                        )
                        .await?;
                    if exists.is_none()
                        && client
                            .query_opt(
                                "SELECT 1 FROM axond_usage_outbox WHERE request_id = $1",
                                &[&key],
                            )
                            .await?
                            .is_some()
                    {
                        return Err(OpError::Journal(JournalError::NotOutstanding {
                            delivery: refused,
                        }));
                    }
                }
                Ok(())
            })
        })
        .await
    }

    async fn stats(&self, consumer: &ConsumerId) -> Result<JournalStats, JournalError> {
        let name = consumer.as_str().to_owned();
        let now = SystemTime::now();
        let capacity = self.settings.capacity;
        self.run("stats", Lane::Delivery, move |client| {
            let name = name.clone();
            Box::pin(async move {
                // Floored on the consumer's resolved prefix for the same reason a
                // claim is: the backlog is what these gauges describe, and the
                // retained acknowledged history behind the floor would otherwise
                // make a gauge published every maintenance tick a full scan of the
                // retention window. Quarantined events are counted from the
                // delivery side instead, because they *are* resolved — they sit
                // below the floor — and an operator still has to see them; its
                // partial index holds only poison rows.
                let row = client
                    .query_one(
                        "WITH floor AS (
                             SELECT COALESCE(
                                 (SELECT resolved_through FROM axond_usage_outbox_consumer
                                  WHERE consumer = $1), 0) AS position
                         ),
                         state AS (
                             SELECT e.observed_at, d.acknowledged_at, d.quarantined_at,
                                    d.lease_expires_at,
                                    (d.acknowledged_at IS NULL AND d.quarantined_at IS NULL
                                     AND (d.lease_expires_at IS NULL
                                          OR d.lease_expires_at <= $2)) AS pending
                             FROM axond_usage_outbox e
                             LEFT JOIN axond_usage_outbox_delivery d
                                 ON d.position = e.position AND d.consumer = $1
                                AND d.position > (SELECT position FROM floor)
                             WHERE e.position > (SELECT position FROM floor)
                         )
                         SELECT
                             count(*) FILTER (WHERE pending),
                             count(*) FILTER (WHERE acknowledged_at IS NULL
                                              AND quarantined_at IS NULL
                                              AND lease_expires_at > $2),
                             (SELECT count(*) FROM axond_usage_outbox_delivery
                              WHERE consumer = $1 AND quarantined_at IS NOT NULL),
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

    /// Retention is measured from `observed_at`, not from the acknowledgement:
    /// the window exists to keep an idempotency key for as long as its caller
    /// may retry, and that clock starts when the request was served. An event
    /// delivered after a long outage is therefore prunable sooner than a
    /// promptly delivered one, which is the intent — its caller's retry horizon
    /// has been running the whole time.
    async fn maintain(&self, now: SystemTime) -> Result<u64, JournalError> {
        let retain = self.settings.capacity.retain_acknowledged;
        // Comfortably longer than an append can hold a `position` open, which is
        // what makes raising the floor safe.
        let settled = now
            .checked_sub(FLOOR_SETTLE_MARGIN.max(self.settings.operation_timeout * 6))
            .unwrap_or(now);
        let pruned = self
            .run("maintain", Lane::Delivery, move |client| {
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
                    client
                        .execute(ADVANCE_RESOLVED_THROUGH, &[&settled])
                        .await?;
                    Ok(pruned)
                })
            })
            .await?;
        if pruned > 0 {
            self.stored.invalidate();
        }
        Ok(pruned)
    }

    async fn consumers_besides(&self, mine: &ConsumerId) -> Result<Vec<String>, JournalError> {
        let name = mine.as_str().to_owned();
        self.run("consumers", Lane::Delivery, move |client| {
            let name = name.clone();
            Box::pin(async move {
                // One row per consumer that has ever claimed, so this is a small
                // table however large the backlog it gates.
                let rows = client
                    .query(
                        "SELECT consumer FROM axond_usage_outbox_consumer WHERE consumer <> $1 \
                         ORDER BY consumer",
                        &[&name],
                    )
                    .await?;
                Ok(rows.iter().map(|row| row.get::<_, String>(0)).collect())
            })
        })
        .await
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
        self.run("verdict", Lane::Delivery, move |client| {
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
                let Some(state) = state.filter(|row| row.get::<_, i32>(0) > 0) else {
                    // Two different situations answer differently. The event is
                    // still here and this consumer was never handed it: a stray
                    // verdict, refused, and deliberately without creating the
                    // delivery state that would register the consumer. Or the
                    // event is gone — reclaimed for capacity, dropped, or pruned
                    // while this worker held its claim — and an acknowledgement
                    // of an event the journal no longer has is the contract's
                    // "already acknowledged": there is nothing left to redeliver,
                    // so answering `NotOutstanding` would only produce a warning
                    // about a redelivery that cannot happen and undercount what
                    // this replica actually delivered. A *quarantine* still
                    // refuses, because its whole purpose is a poison count and a
                    // row an operator can look at, and neither survives the row.
                    let vanished = tx
                        .query_opt(
                            "SELECT 1 FROM axond_usage_outbox WHERE request_id = $1",
                            &[&key],
                        )
                        .await?
                        .is_none();
                    if vanished && poison.is_none() {
                        return Ok(());
                    }
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
///
/// # Why the floor also stops at a settled watermark
///
/// `position` is a `bigserial`, so it is allocated before the appending
/// transaction commits and two concurrent appends can commit out of order: a
/// pass that sees position 12 while 11 is still uncommitted would read 12 as the
/// first unresolved position and put the floor on 11 — an event that is durable,
/// never claimed again, therefore never acknowledged, therefore never pruned.
/// Silent non-delivery, in the mode whose whole point is that nothing is lost.
///
/// So the floor may not pass `$1`, the highest position appended before the
/// caller's settle margin. An append's transaction is bounded by
/// `operation_timeout`, and a position below a row appended before the margin was
/// allocated within one such bound of it, so by the time the margin has passed
/// every lower position has committed or been rolled back. What the margin costs
/// is latency in the floor, not correctness: the floor simply catches up on a
/// later tick.
///
/// The watermark is the highest position appended before the margin — written as
/// a descending walk of the primary key rather than `max(position) WHERE …`, so
/// it reads only the rows appended inside the margin. Taking instead the *first*
/// row inside the margin and stepping back from it would not be safe: a lower
/// position could be inside the margin too, and an uncommitted one is invisible
/// to this pass either way.
const ADVANCE_RESOLVED_THROUGH: &str = "UPDATE axond_usage_outbox_consumer c
     SET resolved_through = GREATEST(
         c.resolved_through,
         LEAST(
             COALESCE(
                 (SELECT e.position - 1
                  FROM axond_usage_outbox e
                  LEFT JOIN axond_usage_outbox_delivery d
                      ON d.position = e.position AND d.consumer = c.consumer
                  WHERE e.position > c.resolved_through
                    AND d.acknowledged_at IS NULL AND d.quarantined_at IS NULL
                  ORDER BY e.position
                  LIMIT 1),
                 (SELECT COALESCE(max(position), 0) FROM axond_usage_outbox)),
             COALESCE(
                 (SELECT e.position FROM axond_usage_outbox e
                  WHERE e.appended_at <= $1
                  ORDER BY e.position DESC
                  LIMIT 1),
                 0)))";

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

/// Give up the retention window on the delivered events at or below `cutoff`,
/// which are the ones beyond the limit. Lossless: every consumer already
/// acknowledged them, so only the courtesy window is spent.
async fn reclaim_delivered(tx: &Transaction<'_>, cutoff: i64) -> Result<u64, OpError> {
    Ok(tx
        .execute(
            &format!(
                "DELETE FROM axond_usage_outbox WHERE position IN (
                     SELECT e.position FROM axond_usage_outbox e
                     WHERE e.position <= $1 AND {DELIVERED})"
            ),
            &[&cutoff],
        )
        .await?)
}

/// The newest position that is beyond `max_events`, or `None` when the outbox
/// has room for one more row.
///
/// Exact, and exact without counting: skipping to the `max_events`-th newest
/// position over the primary key answers "are there already `max_events` rows"
/// and, in the same probe, says which positions the surplus occupies. Only the
/// at-limit path pays for it — an outbox spanning fewer positions than its limit
/// never gets here.
async fn surplus_cutoff(tx: &Transaction<'_>, max_events: u64) -> Result<Option<i64>, OpError> {
    let keep = i64::try_from(max_events.saturating_sub(1)).unwrap_or(i64::MAX);
    Ok(tx
        .query_opt(
            "SELECT position FROM axond_usage_outbox ORDER BY position DESC OFFSET $1 LIMIT 1",
            &[&keep],
        )
        .await?
        .map(|row| row.get::<_, i64>(0)))
}

/// Delete the droppable events at or below `cutoff` — the surplus beyond the
/// limit — raising the durable loss total. The caller counts the loss in telemetry after the commit, so a
/// rolled-back drop is not reported as lost billing data.
///
/// A quarantined event is never a candidate: it is evidence an operator was
/// asked to look at, so a journal whose whole backlog is poison refuses instead.
async fn drop_oldest(tx: &Transaction<'_>, cutoff: i64) -> Result<u64, OpError> {
    let dropped = tx
        .execute(
            "DELETE FROM axond_usage_outbox WHERE position IN (
                 SELECT e.position FROM axond_usage_outbox e
                 WHERE e.position <= $1 AND NOT EXISTS (
                     SELECT 1 FROM axond_usage_outbox_delivery d
                     WHERE d.position = e.position AND d.quarantined_at IS NOT NULL))",
            &[&cutoff],
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
///
/// Guarded like the claim's own upsert, and for the same reason: the delivery
/// row may have moved since the selection's snapshot read it. An event the
/// consumer acknowledged meanwhile keeps its acknowledgement — condemning it
/// would trip the one-verdict constraint and fail the whole claim with an opaque
/// backend error over a race that has already resolved correctly — and an
/// already quarantined event keeps the reason it was first condemned for.
/// `false` says the row was left alone, so the caller does not count a
/// quarantine that did not happen.
async fn condemn(
    tx: &Transaction<'_>,
    position: i64,
    consumer: &str,
    reason: PoisonReason,
    attempt: i32,
) -> Result<bool, OpError> {
    let condemned = tx
        .execute(
            "INSERT INTO axond_usage_outbox_delivery
             (position, consumer, attempts, quarantined_at, poison_reason)
         VALUES ($1, $2, $3, now(), $4)
         ON CONFLICT (position, consumer) DO UPDATE
             SET attempts = $3, quarantined_at = now(), poison_reason = $4,
                 lease_expires_at = NULL
             WHERE axond_usage_outbox_delivery.acknowledged_at IS NULL
               AND axond_usage_outbox_delivery.quarantined_at IS NULL",
            &[&position, &consumer, &attempt, &reason.as_str()],
        )
        .await?;
    Ok(condemned == 1)
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
    // Absent in a row a pre-ADR-0063 writer appended; default so claim/replay
    // of older events still decodes.
    #[serde(default)]
    attrs: Option<serde_json::Value>,
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
    #[serde(default)]
    cost_microdollars: Option<u64>,
    catalog_version: u64,
    // Absent in a row a pre-#147 writer appended, which is exactly what a
    // request the file configuration priced also writes: no approved book named
    // the rates, so there is no identity to replay.
    #[serde(default)]
    price_book: Option<String>,
    #[serde(default)]
    price_book_checksum: Option<String>,
    #[serde(default)]
    price_catalog: Option<String>,
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
            attrs: self.attrs,
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
            price_book: self.price_book,
            price_book_checksum: self.price_book_checksum,
            price_catalog: self.price_catalog,
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

/// Which connections an operation may use.
///
/// The lanes do not overlap, because they compete over very different
/// durations: a claim holds its connection for as long as a destination takes to
/// answer, and an append is a request waiting for a `200`. Sharing one pool lets
/// one slow delivery pass sit on a slot every append needs.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Lane {
    /// Appends, and the boot-time schema work that runs before a worker exists.
    Request,
    /// The delivery worker's claims, verdicts, counts, and maintenance.
    Delivery,
}

/// A fixed set of connections, each replaced on failure, split into the two
/// lanes above.
///
/// Small and hand-rolled rather than a pooling dependency: the outbox needs
/// exactly enough connections that a request's append does not queue behind the
/// delivery worker's claim.
struct Pool {
    config: Config,
    /// Re-applied on every connection, including reconnections: a reconnect that
    /// landed on the default schema would silently write another outbox.
    search_path: Option<String>,
    /// The last slot is the delivery lane; the rest serve requests.
    slots: Vec<tokio::sync::Mutex<Option<Client>>>,
    next: AtomicUsize,
}

impl Pool {
    fn new(config: Config, search_path: Option<String>, connections: usize) -> Self {
        Self {
            config,
            search_path,
            slots: (0..connections.max(2))
                .map(|_| tokio::sync::Mutex::new(None))
                .collect(),
            next: AtomicUsize::new(0),
        }
    }

    async fn run<T, F>(&self, op: &F, lane: Lane) -> Result<T, JournalError>
    where
        T: Send,
        F: for<'a> Fn(&'a mut Client) -> BoxFuture<'a, Result<T, OpError>> + Send + Sync,
    {
        let mut guard = self.acquire(lane).await;
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

    /// A free connection in the lane if there is one, otherwise a fair wait on
    /// the next slot in that lane's rotation.
    async fn acquire(&self, lane: Lane) -> tokio::sync::MutexGuard<'_, Option<Client>> {
        let lane = match lane {
            Lane::Delivery => &self.slots[self.slots.len() - 1..],
            Lane::Request => &self.slots[..self.slots.len() - 1],
        };
        for slot in lane {
            if let Ok(guard) = slot.try_lock() {
                return guard;
            }
        }
        let index = self.next.fetch_add(1, AtomicOrdering::Relaxed) % lane.len();
        lane[index].lock().await
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
/// backlog, so the gate caches instead what the cheap part of the answer cannot
/// give it. Every append reads the *position span* — `max(position) -
/// min(position) + 1`, two index probes — which is shared state and therefore
/// counts every replica's appends and prunes, not just this process's. What the
/// span does not know is how many positions inside it are no longer occupied, and
/// that number moves only when rows are deleted: retention, reclamation and
/// drop-oldest, all of which run on the maintenance and worker paths rather than
/// per append. So the gate caches the gaps and the request path subtracts them.
struct CapacityGate {
    state: std::sync::Mutex<GateState>,
}

#[derive(Default)]
struct GateState {
    measured: Option<Measured>,
    at: Option<Instant>,
    /// When an append last found the outbox full with nothing it could give
    /// up. See [`CapacityGate::refusing`].
    unreclaimable: Option<Instant>,
}

/// What a bounded count established about the span it was taken against.
#[derive(Clone, Copy)]
enum Measured {
    /// Positions inside `span` holding no row. The span is kept with them
    /// because a gap count only describes the span it was taken against.
    Gaps { gaps: u64, span: u64 },
    /// The count stopped at its bound, so the outbox is over the limit and how
    /// far over is not worth knowing.
    Over,
}

impl CapacityGate {
    fn new() -> Self {
        Self {
            state: std::sync::Mutex::new(GateState::default()),
        }
    }

    /// How many rows `span` positions hold, from a gap measurement no older than
    /// [`COUNT_REFRESH`] — or nothing when there is none that fresh.
    ///
    /// This is exact for appends however many replicas make them, because they
    /// all take positions from the same sequence and `span` is read fresh from
    /// the outbox on every append. Deletions are what the cached part can lag:
    /// this process invalidates on its own, and another replica's deletion leaves
    /// the gaps understated for at most a window, which reads *fuller* than the
    /// outbox is. That is the safe direction for a limit whose job is to refuse.
    ///
    /// With one exception, which is why the span is remembered: a deletion that
    /// removes the *lowest* row collapses the span past a gap that is still
    /// counted, and `span - gaps` would then read *emptier* than the outbox is —
    /// far enough, if a hand-deleted quarantined event sat below a large hole,
    /// to admit past `max_events`. A span smaller than the one the gaps were
    /// taken against is therefore no estimate at all, and the caller counts.
    fn estimate(&self, span: u64, max_events: u64) -> Option<u64> {
        let state = self.state.lock().expect("capacity gate");
        if state.at?.elapsed() >= COUNT_REFRESH {
            return None;
        }
        Some(match state.measured? {
            Measured::Gaps {
                gaps,
                span: measured,
            } if span >= measured => span.saturating_sub(gaps),
            Measured::Gaps { .. } => return None,
            Measured::Over => max_events.saturating_add(1),
        })
    }

    /// Record what a bounded count of `counted` rows over `span` positions says.
    fn measured(&self, span: u64, counted: u64, max_events: u64) {
        let measured = if counted > max_events {
            Measured::Over
        } else {
            Measured::Gaps {
                gaps: span.saturating_sub(counted),
                span,
            }
        };
        let mut state = self.state.lock().expect("capacity gate");
        state.measured = Some(measured);
        state.at = Some(Instant::now());
    }

    /// Whether an append may refuse without probing the outbox again.
    ///
    /// A full outbox that had nothing to give up stays that way until something
    /// deletes a row, and finding out costs an ordered walk to the limit-th
    /// newest position — which is exactly the work a refused request cannot
    /// afford to do while the database is already the bottleneck. So the verdict
    /// is remembered for a window instead: refusals inside it are free, the
    /// window is the same [`COUNT_REFRESH`] that bounds how long a count is
    /// trusted, and any deletion this replica makes clears it early.
    fn refusing(&self) -> bool {
        self.state
            .lock()
            .expect("capacity gate")
            .unreclaimable
            .is_some_and(|at| at.elapsed() < COUNT_REFRESH)
    }

    /// Record that the outbox is full of events nothing may delete.
    fn unreclaimable(&self) {
        self.state.lock().expect("capacity gate").unreclaimable = Some(Instant::now());
    }

    /// Forget the measurement: something deleted rows, so the next append has to
    /// count again rather than refuse on a number that is now too high.
    fn invalidate(&self) {
        let mut state = self.state.lock().expect("capacity gate");
        state.at = None;
        state.unreclaimable = None;
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

    /// [`StoredRecord`] is a hand-kept mirror of [`UsageRecord`], which cannot
    /// itself be deserialized because `credential_source` is a `&'static str`
    /// from a closed vocabulary. A field added to the record and forgotten here
    /// would not be a warning: every row already written would stop decoding and
    /// be quarantined as [`PoisonReason::Malformed`], so the mirror is asserted
    /// as a round trip rather than trusted.
    #[test]
    fn a_stored_record_decodes_back_to_exactly_the_record_that_was_written() {
        let record = crate::usage::tests::sample_record();
        let stored: StoredRecord = serde_json::from_value(
            serde_json::to_value(&record).expect("a usage record serializes"),
        )
        .expect("the stored mirror reads every field the record writes");
        assert_eq!(
            stored.into_record().expect("a known credential source"),
            record
        );
    }

    /// The worker holds a connection for as long as a destination takes to
    /// answer. If that connection came out of the same set the appends use, a
    /// slow destination would be indistinguishable from a slow gateway, so the
    /// lanes are asserted to be disjoint rather than assumed to be wide enough.
    #[tokio::test]
    async fn a_claim_in_flight_does_not_hold_a_connection_an_append_needs() {
        let pool = Pool::new(
            "host=127.0.0.1 user=nobody".parse().expect("a config"),
            None,
            2,
        );
        let claim = pool.acquire(Lane::Delivery).await;

        let append = tokio::time::timeout(Duration::from_millis(50), pool.acquire(Lane::Request));
        assert!(
            append.await.is_ok(),
            "an append waited on the worker's connection"
        );
        // And the reservation is mutual: the delivery lane is one slot, so a
        // second claimant waits rather than borrowing the request lane.
        let second = tokio::time::timeout(Duration::from_millis(50), pool.acquire(Lane::Delivery));
        assert!(
            second.await.is_err(),
            "a second claim took a connection reserved for requests"
        );
        drop(claim);
    }

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

    /// The schema an earlier copy of the shipped DDL left behind: a consumer
    /// table without `resolved_through`, which every claim reads. `CREATE TABLE
    /// IF NOT EXISTS` would leave it that way, so this asserts the two things
    /// that keep it from becoming a runtime failure on the first claim — boot
    /// refuses, and re-applying the DDL adopts the column.
    #[tokio::test]
    async fn a_consumer_table_from_before_the_claim_floor_is_migrated_not_served() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let schema = "axond_outbox_migration";
        let admin = client(&dsn, None).await;
        admin
            .batch_execute(&format!(
                "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}"
            ))
            .await
            .expect("a schema of its own");
        let old = client(&dsn, Some(schema)).await;
        old.batch_execute(SCHEMA_DDL).await.expect("the schema");
        old.batch_execute("ALTER TABLE axond_usage_outbox_consumer DROP COLUMN resolved_through")
            .await
            .expect("the shape an earlier copy created");

        let error = PostgresJournal::connect(
            &dsn,
            settings(schema, false, capacity(16, CapacityPolicy::Refuse)),
        )
        .await
        .expect_err("a consumer table this build cannot claim from is a boot failure");
        assert!(
            matches!(&error, JournalError::Backend(message)
                if message.contains("axond_usage_outbox_consumer")),
            "{error:?}"
        );

        // Re-applying the shipped DDL is the whole upgrade: the column is added
        // additively rather than skipped along with the table.
        let journal = PostgresJournal::connect(
            &dsn,
            settings(schema, true, capacity(16, CapacityPolicy::Refuse)),
        )
        .await
        .expect("re-applying the DDL adopts the column");
        let billing = consumer("billing");
        let event = event_for("GW_INBOUND_ACME_KEY");
        assert!(journal.append(&event).await.expect("append").is_new());
        let claimed = journal
            .claim(
                &billing,
                claim_at(4, Duration::from_secs(30), SystemTime::now()),
            )
            .await
            .expect("a claim reads the migrated column");
        assert_eq!(claimed.len(), 1);
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
        different.cost_microdollars = Some(different.settle_cost() + 1);
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

    /// The same contract the oracle pins: an event that left the outbox while a
    /// worker was delivering it is acknowledged, not refused as never handed out.
    #[tokio::test]
    async fn acknowledging_an_event_the_outbox_no_longer_holds_is_not_an_error() {
        let Some((_, journal)) =
            outbox("acked_gone", capacity(1, CapacityPolicy::DropOldest)).await
        else {
            return;
        };
        journal
            .append(&event_for("GW_INBOUND_ACME_KEY"))
            .await
            .expect("append");
        let billing = consumer("billing");
        let claimed = journal
            .claim(
                &billing,
                claim_at(1, Duration::from_secs(30), SystemTime::now()),
            )
            .await
            .expect("claim");
        // Dropped underneath the delivery, exactly as it would be if retention or
        // a capacity reclaim had taken it.
        journal
            .append(&event_for("GW_INBOUND_ACME_KEY"))
            .await
            .expect("a lossy policy makes room rather than refusing");

        journal
            .ack(&claimed[0].id)
            .await
            .expect("nothing is left to redeliver, so this is not the worker's problem");
        let error = journal
            .quarantine(&claimed[0].id, PoisonReason::Malformed)
            .await
            .expect_err("a row that is gone cannot be set aside for an operator");
        assert!(
            matches!(error, JournalError::NotOutstanding { .. }),
            "{error:?}"
        );
    }

    /// The batched acknowledgement is a round-trip optimisation, not a second
    /// contract: what it resolves in one statement must be exactly what a
    /// verdict each would have answered, quarantine and stray delivery included.
    #[tokio::test]
    async fn acknowledging_a_claim_in_one_statement_still_answers_for_each_event() {
        let Some((_, journal)) = outbox("ack_all", capacity(16, CapacityPolicy::Refuse)).await
        else {
            return;
        };
        let billing = consumer("billing");
        for subject in ["acme", "globex", "initech", "umbrella"] {
            journal.append(&event_for(subject)).await.expect("append");
        }
        let claimed = journal
            .claim(
                &billing,
                claim_at(8, Duration::from_secs(30), SystemTime::now()),
            )
            .await
            .expect("claim");
        assert_eq!(claimed.len(), 4, "one event per ordering key");
        // One of them is poison, and a batched acknowledgement may not quietly
        // release it; one was never handed out at all.
        journal
            .quarantine(&claimed[3].id, PoisonReason::Malformed)
            .await
            .expect("quarantine");
        let stray = DeliveryId {
            consumer: consumer("audit"),
            event: claimed[0].event.id(),
            attempt: 1,
        };

        let ids: Vec<DeliveryId> = claimed
            .iter()
            .map(|delivery| delivery.id.clone())
            .chain([stray])
            .collect();
        let verdicts = journal.ack_all(&ids).await;

        assert!(verdicts[0].is_ok() && verdicts[1].is_ok() && verdicts[2].is_ok());
        assert!(
            matches!(verdicts[3], Err(JournalError::Quarantined { .. })),
            "{:?}",
            verdicts[3]
        );
        assert!(
            matches!(verdicts[4], Err(JournalError::NotOutstanding { .. })),
            "{:?}",
            verdicts[4]
        );
        // Repeating it is the recovery path, and says the same thing.
        let repeated = journal.ack_all(&ids).await;
        assert!(repeated[0].is_ok() && repeated[1].is_ok() && repeated[2].is_ok());
        assert!(matches!(repeated[3], Err(JournalError::Quarantined { .. })));
        let stats = journal.stats(&billing).await.expect("stats");
        assert_eq!(stats.pending, 0, "{stats:?}");
        assert_eq!(stats.quarantined, 1, "{stats:?}");
    }

    #[tokio::test]
    async fn an_outbox_far_over_a_lowered_limit_is_brought_under_it_by_one_append() {
        // The way an outbox gets more rows than its limit: the limit was lowered
        // under a backlog that had already been admitted.
        let Some((dsn, journal)) = outbox("surplus", capacity(8, CapacityPolicy::DropOldest)).await
        else {
            return;
        };
        for _ in 0..8 {
            journal
                .append(&event_for("GW_INBOUND_ACME_KEY"))
                .await
                .expect("append");
        }
        let lowered = PostgresJournal::connect(
            &dsn,
            settings(
                "axond_outbox_surplus",
                false,
                capacity(2, CapacityPolicy::DropOldest),
            ),
        )
        .await
        .expect("connect");

        lowered
            .append(&event_for("GW_INBOUND_ACME_KEY"))
            .await
            .expect("append");

        // The surplus is decided by position, so the whole of it goes at once.
        // Arithmetic on a count that stops at `max_events + 1` would have freed
        // two rows out of seven and admitted anyway, leaving the outbox over the
        // limit it was configured with.
        let stored = client(&dsn, Some("axond_outbox_surplus"))
            .await
            .query_one("SELECT count(*) FROM axond_usage_outbox", &[])
            .await
            .expect("count")
            .get::<_, i64>(0);
        assert!(stored <= 2, "the outbox is inside its limit: {stored}");
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

    /// A refusal is the common case while the outbox is full, and the probe that
    /// decides how far over the limit it is walks to the limit-th newest
    /// position. Neither that walk nor the reclamation it feeds may be repeated
    /// per refused request: the room this append made is committed even though
    /// it fails, and the verdict is remembered so the next one costs nothing.
    #[test]
    fn a_refusal_that_freed_nothing_is_remembered_until_something_is_deleted() {
        let gate = CapacityGate::new();
        assert!(!gate.refusing(), "an outbox with room refuses nothing");
        gate.unreclaimable();
        assert!(gate.refusing(), "the backlog cannot have changed by itself");
        gate.invalidate();
        assert!(
            !gate.refusing(),
            "a deletion made room, so the next append has to look again"
        );
    }

    /// A gap count describes the span it was taken against and nothing else. If
    /// the lowest row is deleted — the documented way an operator disposes of a
    /// quarantined event — the span collapses past gaps that are still counted,
    /// and reusing them would read the outbox as emptier than it is and admit
    /// past `max_events`.
    #[test]
    fn a_gap_count_is_not_reused_against_a_span_that_has_since_collapsed() {
        let gate = CapacityGate::new();
        // 900 rows in a 1,000-wide span: one ancient row at the bottom, a
        // reclaimed hole above it, and the recent block at the limit.
        gate.measured(1_000, 900, 1_000);
        assert_eq!(
            gate.estimate(1_000, 1_000),
            Some(900),
            "the span it was measured against is exactly what it describes"
        );
        assert_eq!(
            gate.estimate(1_100, 1_000),
            Some(1_000),
            "appends extend the span, and every one of them stored a row"
        );
        assert_eq!(
            gate.estimate(899, 1_000),
            None,
            "the span collapsed below the measurement, so the gaps say nothing \
             and the append has to count"
        );
    }

    #[tokio::test]
    async fn the_room_a_refused_append_made_is_kept_rather_than_rolled_back() {
        let Some((dsn, journal)) =
            outbox("refused_reclaim", capacity(8, CapacityPolicy::Refuse)).await
        else {
            return;
        };
        for _ in 0..8 {
            journal
                .append(&event_for("GW_INBOUND_ACME_KEY"))
                .await
                .expect("append");
        }
        let billing = consumer("billing");
        let claimed = journal
            .claim(
                &billing,
                claim_at(1, Duration::from_secs(30), SystemTime::now()),
            )
            .await
            .expect("claim");
        journal.ack(&claimed[0].id).await.expect("ack");

        // Lowering the limit puts six rows beyond it, of which only the
        // acknowledged one may be given up: the append still has to refuse.
        let lowered = PostgresJournal::connect(
            &dsn,
            settings(
                "axond_outbox_refused_reclaim",
                false,
                capacity(2, CapacityPolicy::Refuse),
            ),
        )
        .await
        .expect("connect");
        let error = lowered
            .append(&event_for("GW_INBOUND_ACME_KEY"))
            .await
            .expect_err("an outbox of undelivered events refuses");
        assert!(
            matches!(error, JournalError::AtCapacity { .. }),
            "{error:?}"
        );

        let stored = client(&dsn, Some("axond_outbox_refused_reclaim"))
            .await
            .query_one("SELECT count(*) FROM axond_usage_outbox", &[])
            .await
            .expect("count")
            .get::<_, i64>(0);
        assert_eq!(
            stored, 7,
            "the reclaim rode out on the refusal instead of being rolled back \
             for the next request to redo"
        );
    }

    /// Retention waits on every registered consumer, so a retired name stalls it
    /// forever. Deleting the row is the operator's call — a second fleet's
    /// consumer looks exactly the same — so the journal's job is to name it.
    #[tokio::test]
    async fn a_consumer_this_deployment_is_not_running_is_reported_not_deleted() {
        let Some((dsn, journal)) = outbox("others", capacity(16, CapacityPolicy::Refuse)).await
        else {
            return;
        };
        let mine = consumer("billing");
        assert!(
            journal
                .consumers_besides(&mine)
                .await
                .expect("consumers")
                .is_empty(),
            "nothing has claimed yet"
        );
        journal
            .append(&event_for("GW_INBOUND_ACME_KEY"))
            .await
            .expect("append");
        let now = SystemTime::now();
        for name in ["billing", "retired"] {
            journal
                .claim(&consumer(name), claim_at(8, Duration::from_secs(30), now))
                .await
                .expect("claim");
        }

        assert_eq!(
            journal.consumers_besides(&mine).await.expect("consumers"),
            vec!["retired".to_owned()],
            "the name retention is also waiting on"
        );
        // Reported, never reaped: the row may belong to a live consumer.
        let registered: i64 = client(&dsn, Some("axond_outbox_others"))
            .await
            .query_one("SELECT count(*) FROM axond_usage_outbox_consumer", &[])
            .await
            .expect("count")
            .get(0);
        assert_eq!(registered, 2);
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

        // Nothing is pruned: every row is inside its retention window. Run past
        // the settle margin, because a floor may not pass a position that could
        // still be in flight — which every row this test just wrote could be.
        let settled = now + FLOOR_SETTLE_MARGIN + Duration::from_secs(1);
        assert_eq!(journal.maintain(settled).await.expect("maintain"), 0);
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

    /// `max_events` bounds the outbox, not one replica's share of it: the
    /// admission decision reads the position span on every append, so it counts
    /// what other replicas have appended and cannot be talked past by a cache
    /// that only knows about this process.
    #[tokio::test]
    async fn the_event_limit_holds_across_replicas() {
        const LIMIT: u64 = 6;
        let Some((dsn, first)) =
            outbox("replica_capacity", capacity(LIMIT, CapacityPolicy::Refuse)).await
        else {
            return;
        };
        let second = PostgresJournal::connect(
            &dsn,
            settings(
                "axond_outbox_replica_capacity",
                false,
                capacity(LIMIT, CapacityPolicy::Refuse),
            ),
        )
        .await
        .expect("a second replica");

        // Alternating, and within one refresh window, so each replica decides
        // mostly from a cache the other replica's appends never touched.
        let mut accepted = 0u64;
        let mut refused = 0u64;
        for turn in 0..LIMIT * 2 {
            let journal: &PostgresJournal = if turn % 2 == 0 { &first } else { &second };
            match journal.append(&event_for("GW_INBOUND_ACME_KEY")).await {
                Ok(_) => accepted += 1,
                Err(JournalError::AtCapacity { .. }) => refused += 1,
                Err(error) => panic!("unexpected append failure: {error}"),
            }
        }
        assert_eq!(accepted, LIMIT, "the two replicas together overfilled it");
        assert_eq!(refused, LIMIT, "the rest were refused, not lost");

        let admin = client(&dsn, Some("axond_outbox_replica_capacity")).await;
        let stored: i64 = admin
            .query_one("SELECT count(*) FROM axond_usage_outbox", &[])
            .await
            .expect("the stored count")
            .get(0);
        assert_eq!(stored as u64, LIMIT, "the outbox holds more than its bound");
    }

    /// `position` is allocated before an append commits, so a later position can
    /// become visible while an earlier one is still in flight. A floor that took
    /// the first *visible* unresolved position as its bound would step over the
    /// earlier event, and because the floor only rises that event would never be
    /// claimed, acknowledged, or pruned — durable and undelivered forever.
    #[tokio::test]
    async fn the_claim_floor_never_passes_an_append_that_has_not_committed() {
        let Some((dsn, journal)) = outbox("floor_race", capacity(64, CapacityPolicy::Refuse)).await
        else {
            return;
        };
        let billing = consumer("billing");
        let now = SystemTime::now();

        // An append that has taken its position and not committed, held open the
        // way a slow request would hold it.
        let mut inflight = client(&dsn, Some("axond_outbox_floor_race")).await;
        let held = inflight.transaction().await.expect("a held append");
        let early = event_for("GW_INBOUND_ACME_KEY");
        let record = serde_json::to_value(early.record()).expect("a record");
        let position: i64 = held
            .query_one(
                "INSERT INTO axond_usage_outbox \
                   (request_id, schema_version, namespace, subject, record, observed_at) \
                 VALUES ($1, $2, $3, $4, $5::jsonb, $6) RETURNING position",
                &[
                    &early.idempotency_key().as_str(),
                    &i32::try_from(early.record().schema_version).expect("a version"),
                    &early.ordering_key().namespace,
                    &early.ordering_key().subject,
                    &record,
                    &early.observed_at(),
                ],
            )
            .await
            .expect("the held append takes a position")
            .get(0);

        // A later append that commits first, and is delivered.
        let later = event_for("GW_INBOUND_OTHER_KEY");
        journal.append(&later).await.expect("append");
        let claimed = journal
            .claim(&billing, claim_at(8, Duration::from_secs(30), now))
            .await
            .expect("claim");
        assert_eq!(claimed.len(), 1, "{claimed:?}");
        journal.ack(&claimed[0].id).await.expect("ack");

        // Maintenance now sees only the later, resolved event, so the unfloored
        // answer would be "everything through it is finished".
        journal.maintain(now).await.expect("maintain");
        let admin = client(&dsn, Some("axond_outbox_floor_race")).await;
        let floor: i64 = admin
            .query_one(
                "SELECT resolved_through FROM axond_usage_outbox_consumer WHERE consumer = $1",
                &[&billing.as_str()],
            )
            .await
            .expect("the floor")
            .get(0);
        assert!(
            floor < position,
            "the floor ({floor}) passed a position ({position}) that had not committed"
        );

        // And the event delivers once it commits, which is the property the floor
        // could have destroyed.
        held.commit().await.expect("the held append commits");
        let claimed = journal
            .claim(&billing, claim_at(8, Duration::from_secs(30), now))
            .await
            .expect("claim");
        assert_eq!(
            claimed.iter().map(|d| d.event.id()).collect::<Vec<_>>(),
            vec![early.id()],
            "the late-committing event is still claimable"
        );
    }
}
