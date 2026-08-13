-- Axond catalogue store schema, version 1 — imported models.dev snapshots and
-- the pointer to the one that is active
-- (docs/adr/0049-durable-catalogue-snapshots-and-refresh-orchestration.md).
--
-- Two tables, and the split is the point. `axond_catalog_snapshot` is history:
-- one row per distinct imported catalogue, keyed by the identity of its
-- normalized content, holding the exact bytes that were accepted. Nothing ever
-- updates a row here — re-importing an unchanged catalogue conflicts on the
-- primary key and stores nothing — so a price book that was approved against a
-- content id keeps resolving the content it was approved against, and a rollback
-- is a pointer move rather than a fetch from an upstream that has moved on.
--
-- `axond_catalog_active` is the present: a single row saying which catalogue is
-- active, what the source last confirmed about it, and how many refreshes in a
-- row have been refused. Apply once per database before booting a gateway that
-- imports catalogues:
--
--     psql "$AXOND_CONTROL_PLANE_DSN" -f ops/postgres/catalog_v1.sql
--
-- The gateway can apply it itself with `create_table = true` under `[catalog]`,
-- which runs exactly these statements. Never edit this file in place once it has
-- been applied: a change to the row shape is a new `catalog_v<N>.sql`.
--
-- Nothing in here is a secret. A catalogue is a public document about public
-- models, and the payload column holds it verbatim precisely so a deployment can
-- prove later what it imported.

BEGIN;

CREATE TABLE IF NOT EXISTS axond_catalog_snapshot (
    -- The SHA-256 of the canonical normalized content, in its text form
    -- (`sha256:…`). The primary key, so one catalogue is stored once however
    -- many times it is imported, and however differently the payload was
    -- formatted each time.
    content_id     text        NOT NULL PRIMARY KEY,
    -- Provenance, exactly as recorded at import. `schema_version` is the shape
    -- the payload was parsed as, so a row written by a build that reads a
    -- document this one does not is a described refusal rather than a
    -- misinterpretation.
    source_url     text        NOT NULL,
    schema_version text        NOT NULL,
    -- The digest and length of `payload`, as the import recorded them. Kept
    -- beside the bytes rather than derived from them: a hydrating gateway checks
    -- the payload against the reference it stored, and a reference computed from
    -- the same possibly-damaged bytes would agree with anything.
    raw_digest     text        NOT NULL,
    raw_bytes      bigint      NOT NULL,
    -- The bytes that were accepted. A catalogue is rehydrated by re-parsing
    -- these through the same parser that admitted them; there is deliberately no
    -- stored form of the normalized domain, which would be a second definition
    -- of a catalogue, free to drift from the parser's.
    payload        bytea       NOT NULL,
    -- The validators and retrieval time this import arrived with. Historical:
    -- what the source says *now* about the active catalogue lives on the active
    -- row, because a `304` moves provenance without moving content.
    fetched_at     timestamptz NOT NULL,
    etag           text        NULL,
    last_modified  text        NULL,
    imported_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT axond_catalog_snapshot_raw_bytes_match_payload
        CHECK (raw_bytes = octet_length(payload)),
    CONSTRAINT axond_catalog_snapshot_raw_bytes_non_negative
        CHECK (raw_bytes >= 0)
);

CREATE TABLE IF NOT EXISTS axond_catalog_active (
    -- One row, enforced by the database. A second active catalogue is not a
    -- state this deployment has an answer for, and a unique-by-constant primary
    -- key makes it unrepresentable rather than merely unwritten.
    singleton            boolean     NOT NULL PRIMARY KEY DEFAULT true,
    -- The active import, or NULL before the first one succeeds. Nullable because
    -- refusals are counted from the very first refresh: a deployment whose
    -- upstream has never answered has a refusal run to report and no catalogue.
    -- The reference is what makes an active pointer to an absent snapshot
    -- impossible.
    content_id           text        NULL
        REFERENCES axond_catalog_snapshot (content_id),
    -- What the source last stated about the active content, and when it last
    -- confirmed it. This is what an active catalogue's age is measured from, so
    -- "confirmed unchanged a minute ago" does not read as "imported a week ago".
    etag                 text        NULL,
    last_modified        text        NULL,
    confirmed_at         timestamptz NULL,
    -- The run of refused refreshes since the last admitted or confirmed one, and
    -- the bounded reason for the most recent. Durable so that a restart cannot
    -- present a stuck deployment as a healthy one that has merely forgotten.
    consecutive_refusals bigint      NOT NULL DEFAULT 0,
    last_refusal         text        NULL,
    last_refusal_at      timestamptz NULL,
    updated_at           timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT axond_catalog_active_is_singleton CHECK (singleton),
    CONSTRAINT axond_catalog_active_refusals_non_negative
        CHECK (consecutive_refusals >= 0),
    -- An active catalogue is always a catalogue that was confirmed at some
    -- point, and a confirmation is always of some content.
    CONSTRAINT axond_catalog_active_confirmed_with_content CHECK (
        (content_id IS NULL AND confirmed_at IS NULL)
        OR (content_id IS NOT NULL AND confirmed_at IS NOT NULL)
    )
);

-- No index beyond the two primary keys: every statement reads the singleton row
-- or one snapshot by identity, and history is listed by an operator rarely
-- enough that a scan is the right cost.

COMMIT;
