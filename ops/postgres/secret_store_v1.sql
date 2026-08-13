-- Axond secret store schema, version 1 — envelope-encrypted provider credential
-- material (docs/adr/0038-envelope-encrypted-secret-store-and-snapshot-time-resolution.md).
--
-- One row per *version* of a secret. A version is immutable: rotation inserts
-- the next version, it never updates this row's bytes, so a revision compiled
-- against version 2 keeps resolving version 2 after version 3 is staged. Apply
-- once per database before booting a stateful gateway:
--
--     psql "$AXOND_SECRET_POSTGRES_DSN" -f ops/postgres/secret_store_v1.sql
--
-- The gateway can apply it itself with `create_table = true` under
-- `[secret_store]`, which runs exactly these statements. Never edit this file in
-- place once it has been applied: a change to the row shape is a new
-- `secret_store_v<N>.sql`.
--
-- Nothing here is readable without the deployment's key-encryption key. The KEK
-- is *not* in this database: it is referenced from bootstrap configuration
-- (`kek_env` or `kek_file`), so a database dump — or a stolen replica — is
-- ciphertext, and `kek_reference` records only the name the key was resolved
-- from, never the key.

BEGIN;

CREATE TABLE IF NOT EXISTS axond_secret (
    -- The opaque secret id in its prefixed text form (`sct_…`), and the
    -- one-based version. Together they are the `SecretRef` a credential body
    -- pins, and the primary key: a version exists once and is never rewritten.
    secret_id     text        NOT NULL,
    version       bigint      NOT NULL,
    -- The owner, exactly as the domain models it: a tenant, and optionally one
    -- of its projects. Not a hierarchy — a project's credential does not resolve
    -- its tenant's material — so both columns are matched exactly on every read.
    tenant_id     text        NOT NULL,
    project_id    text        NULL,
    -- staged | active | disabled | revoked | tombstoned. Text rather than an
    -- enum type so a newer release can write a state an older replica reports as
    -- unreadable instead of failing to insert.
    lifecycle     text        NOT NULL,
    -- The sealing scheme, so a record written by a scheme this build does not
    -- implement is a described refusal rather than a decryption failure.
    scheme        text        NOT NULL,
    -- The *name* the KEK was resolved from. A reference, never material.
    kek_reference text        NOT NULL,
    -- The per-version data-encryption key, sealed under the KEK, and the
    -- material, sealed under that DEK. Both AAD-bound to the owner and the exact
    -- reference above, so a row copied to another tenant or renumbered to
    -- another version stops opening.
    wrapped_dek   bytea       NULL,
    dek_nonce     bytea       NULL,
    ciphertext    bytea       NULL,
    nonce         bytea       NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    -- When the bytes were destroyed. Set with the tombstone, in the same
    -- statement that nulls them.
    destroyed_at  timestamptz NULL,
    PRIMARY KEY (secret_id, version),
    CONSTRAINT axond_secret_version_positive CHECK (version >= 1),
    -- Tombstoning *is* the destruction of material, so the database refuses a
    -- tombstoned row that still holds bytes and a live row that holds none.
    -- Enforced here as well as in the gateway, because a store that kept the
    -- bytes of a destroyed secret would be indistinguishable from one that did
    -- not, until an audit.
    CONSTRAINT axond_secret_material_matches_lifecycle CHECK (
        (lifecycle = 'tombstoned'
            AND wrapped_dek IS NULL AND dek_nonce IS NULL
            AND ciphertext IS NULL AND nonce IS NULL
            AND destroyed_at IS NOT NULL)
        OR
        (lifecycle <> 'tombstoned'
            AND wrapped_dek IS NOT NULL AND dek_nonce IS NOT NULL
            AND ciphertext IS NOT NULL AND nonce IS NOT NULL
            AND destroyed_at IS NULL)
    )
);

-- No index on the owner columns: every statement here keys on the reference
-- through the primary key and checks the owner columns it read, so an owner index
-- would be unused weight on the write path. An owner-scoped listing, where the
-- reference is not known up front, arrives with the index it needs in a later
-- `secret_store_v<N>.sql`.

COMMIT;
