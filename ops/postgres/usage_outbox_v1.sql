-- Axond billing-grade usage outbox, version 1 (docs/usage-schema.md).
--
-- Apply before enabling `[usage_journal] backend = "postgres"`, which is the opt-in
-- billing-grade delivery mode: the gateway appends every settled usage event
-- here before the request is answered, and a delivery worker replays each event
-- into the configured sinks until they acknowledge it.
--
--     psql "$AXOND_USAGE_JOURNAL_DSN" -f ops/postgres/usage_outbox_v1.sql
--
-- Apply it into a dedicated schema by setting the search path first, and name
-- the same schema in `[usage_journal] schema`:
--
--     psql "$AXOND_USAGE_JOURNAL_DSN" \
--       -c 'CREATE SCHEMA IF NOT EXISTS billing' \
--       -c 'SET search_path TO billing' \
--       -f ops/postgres/usage_outbox_v1.sql
--
-- The table names are fixed. Unlike the `axond_usage` row table, the outbox is
-- the gateway's own delivery state rather than an interface a billing query
-- reads, so it is not retargetable; a second outbox on one database is a second
-- schema.

-- One appended event. `request_id` is the globally unique event identity
-- (`req_` + UUIDv7) and the idempotency key a consumer deduplicates on, so it is
-- UNIQUE here: a re-append of the same event collides rather than becoming a
-- second billable fact.
--
-- `record` is the canonical usage record as JSON, and `schema_version` is the
-- version it was written at. A row whose version is newer than the reading
-- build is left alone rather than delivered or condemned, so a rolling upgrade
-- with replicas on both versions delivers every event exactly once its own
-- writer's version is deployed.
CREATE TABLE IF NOT EXISTS axond_usage_outbox (
    position       bigserial PRIMARY KEY,
    request_id     text        NOT NULL UNIQUE,
    schema_version integer     NOT NULL,
    namespace      text        NOT NULL,
    subject        text        NOT NULL,
    record         jsonb       NOT NULL,
    observed_at    timestamptz NOT NULL,
    appended_at    timestamptz NOT NULL DEFAULT now()
);

-- Claims walk one ordering key's events in append order, so the index is on the
-- ordering key and the position rather than on the position alone.
CREATE INDEX IF NOT EXISTS axond_usage_outbox_ordering_idx
    ON axond_usage_outbox (namespace, subject, position);

-- Retention prunes on the event's own observation time.
CREATE INDEX IF NOT EXISTS axond_usage_outbox_observed_at_idx
    ON axond_usage_outbox (observed_at);

-- A consumer exists once it has claimed. Nothing else registers one, because
-- retention waits on every registered consumer: a row created by a stray
-- acknowledgement would hold the outbox open forever.
CREATE TABLE IF NOT EXISTS axond_usage_outbox_consumer (
    consumer      text PRIMARY KEY,
    registered_at timestamptz NOT NULL DEFAULT now()
);

-- Per-consumer delivery state. `lease_expires_at` is what makes recovery
-- automatic: a worker that dies mid-delivery stops renewing, the lease expires,
-- and the event is claimable again with the next attempt number. Acknowledgement
-- and quarantine are exclusive terminal states.
CREATE TABLE IF NOT EXISTS axond_usage_outbox_delivery (
    position         bigint      NOT NULL
        REFERENCES axond_usage_outbox (position) ON DELETE CASCADE,
    consumer         text        NOT NULL
        REFERENCES axond_usage_outbox_consumer (consumer) ON DELETE CASCADE,
    attempts         integer     NOT NULL DEFAULT 0,
    lease_expires_at timestamptz,
    acknowledged_at  timestamptz,
    quarantined_at   timestamptz,
    poison_reason    text,
    PRIMARY KEY (position, consumer),
    CONSTRAINT axond_usage_outbox_delivery_one_verdict
        CHECK (acknowledged_at IS NULL OR quarantined_at IS NULL)
);

-- The claim predicate: unresolved deliveries for one consumer, in append order.
CREATE INDEX IF NOT EXISTS axond_usage_outbox_delivery_open_idx
    ON axond_usage_outbox_delivery (consumer, position)
    WHERE acknowledged_at IS NULL AND quarantined_at IS NULL;

-- Events lost to `capacity_policy = "drop-oldest"`, which is the only way this
-- outbox can lose an accepted event. Durable rather than a process counter, so
-- the number an operator alerts on survives the replica that dropped them.
CREATE TABLE IF NOT EXISTS axond_usage_outbox_loss (
    id      boolean PRIMARY KEY DEFAULT true CHECK (id),
    dropped bigint  NOT NULL DEFAULT 0
);

INSERT INTO axond_usage_outbox_loss (id, dropped)
    VALUES (true, 0)
    ON CONFLICT (id) DO NOTHING;
