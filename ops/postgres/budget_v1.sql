-- Axond budget schema, version 1 (docs/adr/0010-shared-budget-backends.md).
--
-- Apply once per database before enabling `backend = "postgres"` under
-- `[budget]`:
--
--     psql "$AXOND_BUDGET_POSTGRES_DSN" -f ops/postgres/budget_v1.sql
--
-- The gateway can apply this itself with `create_table = true`, which runs the
-- same statements (with the table names substituted). Never edit this file in
-- place once it has been applied: a change to the row shape is a new
-- `budget_v<N>.sql`.
--
-- Two tables, because settled spend and outstanding reservations have different
-- lifetimes: spend is cumulative per `(namespace, subject)`, while a reservation
-- is a short-lived hold that either becomes spend or expires. The spend row is
-- also the lock the gateway serializes admissions on (`SELECT ... FOR UPDATE`),
-- which is what keeps a shared cap from being double-spent across replicas.

CREATE TABLE IF NOT EXISTS axond_budget (
    namespace          text        NOT NULL,
    subject            text        NOT NULL,
    -- Cumulative measured spend, in micro-dollars. Integer, so no float drift.
    spent_microdollars bigint      NOT NULL DEFAULT 0,
    updated_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace, subject)
);

CREATE TABLE IF NOT EXISTS axond_budget_reservation (
    -- Gateway-generated hold id; a settlement releases exactly its own hold.
    id                  text        PRIMARY KEY,
    namespace           text        NOT NULL,
    subject             text        NOT NULL,
    -- The estimate being held, in micro-dollars.
    amount_microdollars bigint      NOT NULL,
    -- A hold left behind by a replica that died stops counting against the cap
    -- here; the next reservation for the same key reclaims it.
    expires_at          timestamptz NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS axond_budget_reservation_scope_idx
    ON axond_budget_reservation (namespace, subject);

CREATE INDEX IF NOT EXISTS axond_budget_reservation_expires_at_idx
    ON axond_budget_reservation (expires_at);
