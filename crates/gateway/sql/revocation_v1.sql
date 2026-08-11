-- Axond precise minted-token revocation schema, version 1.
--
-- Apply once per database before enabling `backend = "postgres"` under
-- `[revocation]`:
--
--     psql "$AXOND_REVOCATION_POSTGRES_DSN" -f crates/gateway/sql/revocation_v1.sql
--
-- The gateway can apply this itself with `create_table = true`, which runs the
-- same statements (with the table names substituted). Never edit this file in
-- place once it has been applied: a change to the row shape is a new
-- `revocation_v<N>.sql`.
--
-- Expired rows are harmless leftovers; operators may delete them as maintenance.
CREATE TABLE IF NOT EXISTS axond_revocation (
    jti         text        PRIMARY KEY,
    expires_at  timestamptz NOT NULL,
    revoked_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS axond_revocation_expires_at_idx
    ON axond_revocation (expires_at);
