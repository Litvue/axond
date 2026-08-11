-- Axond precise minted-token revocation schema, version 1.
-- Expired rows are harmless leftovers; operators may delete them as maintenance.
CREATE TABLE IF NOT EXISTS axond_revocation (
    jti         text        PRIMARY KEY,
    expires_at  timestamptz NOT NULL,
    revoked_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS axond_revocation_expires_at_idx
    ON axond_revocation (expires_at);
