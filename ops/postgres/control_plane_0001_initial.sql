-- Axond control-plane revision journal, migration 0001.
--
-- Durable desired state is a chain of immutable revisions: publishing a change
-- writes new resource versions and a new manifest referencing them, and nothing
-- is edited in place. Apply this file once per database, before pointing a
-- gateway at it:
--
--     psql "$AXOND_CONTROL_PLANE_DSN" -f ops/postgres/control_plane_0001_initial.sql
--
-- Migrations are forward-only and versioned: never edit an applied file in
-- place, and never renumber one. A schema change is a new
-- `control_plane_<NNNN>_<name>.sql`, and `axond_cp_schema_migration` records the
-- version, the file's name, and a checksum of its text so an edited migration is
-- reported as drift instead of applied twice.
--
-- Requires PostgreSQL 14 or newer (identity columns, `ON CONFLICT` on partial
-- unique indexes). Every object lives in the current schema, so an operator who
-- wants the journal beside other tables can `SET search_path` before applying.
--
-- What the constraints here do and do not express: they constrain *structure* —
-- id and checksum shapes, scope ownership, actor attribution, body exclusivity,
-- referential integrity, and the linearity of the revision chain. They do not
-- enumerate the domain's resource or blob vocabularies, because a new
-- `ResourceKind` must not require a migration; an unreadable kind is refused
-- when a revision is loaded, as corruption rather than as an outage.

CREATE TABLE IF NOT EXISTS axond_cp_schema_migration (
    version     integer     PRIMARY KEY,
    name        text        NOT NULL,
    -- Checksum of the migration's text, so an in-place edit is detectable.
    checksum    text        NOT NULL CHECK (checksum ~ '^sha256:[0-9a-f]{64}$'),
    applied_at  timestamptz NOT NULL DEFAULT now()
);

-- Content-addressed payloads a revision references. References only: the digest
-- is the address, so a catalogue snapshot pinned by a hundred revisions is one
-- row rather than a hundred copies.
CREATE TABLE IF NOT EXISTS axond_cp_blob (
    blob_kind   text        NOT NULL,
    digest      text        NOT NULL CHECK (digest ~ '^sha256:[0-9a-f]{64}$'),
    size_bytes  bigint      NOT NULL CHECK (size_bytes >= 0),
    declared_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (blob_kind, digest)
);

-- One immutable version of one resource, shared by every revision that pins it.
-- `(resource_kind, resource_id, version)` names bytes that never change: a
-- publication that would redefine an existing row is refused before it is
-- attempted, and the primary key is the backstop.
CREATE TABLE IF NOT EXISTS axond_cp_resource_version (
    resource_kind     text        NOT NULL,
    resource_id       text        NOT NULL CHECK (resource_id ~ '^res_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'),
    version           bigint      NOT NULL CHECK (version > 0),
    scope_kind        text        NOT NULL CHECK (scope_kind IN ('deployment', 'tenant', 'project')),
    tenant_id         text        NULL CHECK (tenant_id ~ '^ten_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'),
    project_id        text        NULL CHECK (project_id ~ '^prj_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'),
    slug              text        NOT NULL CHECK (length(slug) BETWEEN 1 AND 63),
    body_form         text        NOT NULL CHECK (body_form IN ('inline', 'blob')),
    -- Canonical bytes of an inline body, under `serializer`. Opaque here: the
    -- domain owns the encoding, and this column is never interpreted by SQL.
    body_inline       bytea       NULL,
    body_blob_kind    text        NULL,
    body_blob_digest  text        NULL,
    -- The version's own checksum: reference, scope, slug, body, and dependency
    -- edges, which is what a manifest entry records and what immutability is
    -- decided by. Comparing one value means a republication that changes a slug
    -- or an edge is refused as surely as one that changes a body.
    content_checksum  text        NOT NULL CHECK (content_checksum ~ '^sha256:[0-9a-f]{64}$'),
    serializer        text        NOT NULL,
    created_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (resource_kind, resource_id, version),
    -- Tenancy is composite ownership, not a loose column: a tenant-scoped row
    -- without a tenant, or a deployment-scoped row carrying one, is unwritable.
    CONSTRAINT axond_cp_resource_version_scope_ownership CHECK (
        (scope_kind = 'deployment' AND tenant_id IS NULL AND project_id IS NULL)
        OR (scope_kind = 'tenant' AND tenant_id IS NOT NULL AND project_id IS NULL)
        OR (scope_kind = 'project' AND tenant_id IS NOT NULL AND project_id IS NOT NULL)
    ),
    -- A body is inline or content-addressed, never both and never neither.
    CONSTRAINT axond_cp_resource_version_body_form CHECK (
        (body_form = 'inline' AND body_inline IS NOT NULL AND body_blob_kind IS NULL AND body_blob_digest IS NULL)
        OR (body_form = 'blob' AND body_inline IS NULL AND body_blob_kind IS NOT NULL AND body_blob_digest IS NOT NULL)
    ),
    FOREIGN KEY (body_blob_kind, body_blob_digest) REFERENCES axond_cp_blob (blob_kind, digest)
);

CREATE INDEX IF NOT EXISTS axond_cp_resource_version_tenant_idx
    ON axond_cp_resource_version (tenant_id, resource_kind, slug);

-- The dependency edges the domain validates: an alias on its credential, a
-- credential on its provider. Both ends are foreign keys, so a revision cannot
-- reference a version that was never written.
CREATE TABLE IF NOT EXISTS axond_cp_resource_dependency (
    resource_kind       text   NOT NULL,
    resource_id         text   NOT NULL,
    version             bigint NOT NULL,
    depends_on_kind     text   NOT NULL,
    depends_on_id       text   NOT NULL,
    depends_on_version  bigint NOT NULL,
    PRIMARY KEY (resource_kind, resource_id, version, depends_on_kind, depends_on_id, depends_on_version),
    FOREIGN KEY (resource_kind, resource_id, version)
        REFERENCES axond_cp_resource_version (resource_kind, resource_id, version),
    FOREIGN KEY (depends_on_kind, depends_on_id, depends_on_version)
        REFERENCES axond_cp_resource_version (resource_kind, resource_id, version)
);

-- One administrative change, with its attribution. Retained forever: it is what
-- an audit event points at, so it is never expiry-pruned.
CREATE TABLE IF NOT EXISTS axond_cp_mutation (
    mutation_id      text        PRIMARY KEY CHECK (mutation_id ~ '^mut_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'),
    actor_kind       text        NOT NULL CHECK (actor_kind IN ('human', 'breakglass', 'system')),
    actor_issuer     text        NULL,
    actor_subject    text        NULL,
    actor_component  text        NULL,
    mutation_kind    text        NOT NULL,
    scope_kind       text        NOT NULL CHECK (scope_kind IN ('deployment', 'tenant', 'project')),
    tenant_id        text        NULL,
    project_id       text        NULL,
    idempotency_key  text        NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 200),
    submitted_at     timestamptz NOT NULL,
    -- Every mutation has an actor, and each kind of actor is attributed by
    -- exactly the columns it has: a subject is meaningless without its issuer,
    -- and breakglass is not a component that happens to be named breakglass.
    CONSTRAINT axond_cp_mutation_actor_attribution CHECK (
        (actor_kind = 'human' AND actor_issuer IS NOT NULL AND actor_subject IS NOT NULL AND actor_component IS NULL)
        OR (actor_kind = 'breakglass' AND actor_issuer IS NULL AND actor_subject IS NULL AND actor_component IS NULL)
        OR (actor_kind = 'system' AND actor_component IS NOT NULL AND actor_issuer IS NULL AND actor_subject IS NULL)
    ),
    CONSTRAINT axond_cp_mutation_scope_ownership CHECK (
        (scope_kind = 'deployment' AND tenant_id IS NULL AND project_id IS NULL)
        OR (scope_kind = 'tenant' AND tenant_id IS NOT NULL AND project_id IS NULL)
        OR (scope_kind = 'project' AND tenant_id IS NOT NULL AND project_id IS NOT NULL)
    )
);

-- A published revision. `parent_id` is the revision it was published against,
-- and the two unique constraints below are what make the chain linear at the
-- storage layer: at most one root, and at most one child per parent. So two
-- writers that both believe they hold the head cannot both commit even if the
-- head lock were ever bypassed.
CREATE TABLE IF NOT EXISTS axond_cp_revision (
    revision_id     text        PRIMARY KEY CHECK (revision_id ~ '^rev_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'),
    parent_id       text        NULL UNIQUE REFERENCES axond_cp_revision (revision_id),
    mutation_id     text        NOT NULL UNIQUE REFERENCES axond_cp_mutation (mutation_id),
    serializer      text        NOT NULL,
    state_checksum  text        NOT NULL CHECK (state_checksum ~ '^sha256:[0-9a-f]{64}$'),
    created_at      timestamptz NOT NULL,
    -- Publication order, which is the authority on "which revision is newest".
    -- Ids are time-ordered per generator, so they are a convenience, not the
    -- authority: across a restart or a second replica they degrade to
    -- wall-clock agreement.
    seq             bigint      NOT NULL GENERATED ALWAYS AS IDENTITY UNIQUE,
    CONSTRAINT axond_cp_revision_is_not_its_own_parent CHECK (parent_id IS NULL OR parent_id <> revision_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS axond_cp_revision_single_root_idx
    ON axond_cp_revision ((parent_id IS NULL))
    WHERE parent_id IS NULL;

-- One manifest line per resource version. Scope, slug, and content checksum are
-- not repeated here: they belong to the version, and duplicating them is how a
-- manifest and its rows come to disagree.
CREATE TABLE IF NOT EXISTS axond_cp_revision_entry (
    revision_id    text   NOT NULL REFERENCES axond_cp_revision (revision_id),
    resource_kind  text   NOT NULL,
    resource_id    text   NOT NULL,
    version        bigint NOT NULL,
    PRIMARY KEY (revision_id, resource_kind, resource_id, version),
    -- One version of a resource per revision: "which version of this alias is
    -- live" must have exactly one answer.
    UNIQUE (revision_id, resource_kind, resource_id),
    FOREIGN KEY (resource_kind, resource_id, version)
        REFERENCES axond_cp_resource_version (resource_kind, resource_id, version)
);

CREATE TABLE IF NOT EXISTS axond_cp_revision_blob (
    revision_id  text NOT NULL REFERENCES axond_cp_revision (revision_id),
    blob_kind    text NOT NULL,
    digest       text NOT NULL,
    PRIMARY KEY (revision_id, blob_kind, digest),
    FOREIGN KEY (blob_kind, digest) REFERENCES axond_cp_blob (blob_kind, digest)
);

-- The audit event a mutation carries, written in the mutation's own
-- transaction. An audit trail that can be half-written is not an audit trail.
CREATE TABLE IF NOT EXISTS axond_cp_audit_event (
    audit_event_id   text        PRIMARY KEY CHECK (audit_event_id ~ '^aud_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'),
    revision_id      text        NOT NULL REFERENCES axond_cp_revision (revision_id),
    mutation_id      text        NOT NULL REFERENCES axond_cp_mutation (mutation_id),
    actor_kind       text        NOT NULL CHECK (actor_kind IN ('human', 'breakglass', 'system')),
    actor_issuer     text        NULL,
    actor_subject    text        NULL,
    actor_component  text        NULL,
    event_kind       text        NOT NULL,
    target_kind      text        NULL,
    target_id        text        NULL,
    target_version   bigint      NULL,
    summary          text        NOT NULL,
    recorded_at      timestamptz NOT NULL,
    CONSTRAINT axond_cp_audit_event_actor_attribution CHECK (
        (actor_kind = 'human' AND actor_issuer IS NOT NULL AND actor_subject IS NOT NULL AND actor_component IS NULL)
        OR (actor_kind = 'breakglass' AND actor_issuer IS NULL AND actor_subject IS NULL AND actor_component IS NULL)
        OR (actor_kind = 'system' AND actor_component IS NOT NULL AND actor_issuer IS NULL AND actor_subject IS NULL)
    ),
    -- A target is a whole reference or absent: a deletion has no new version to
    -- point at, and half a reference points at nothing.
    CONSTRAINT axond_cp_audit_event_target_is_whole CHECK (
        (target_kind IS NULL AND target_id IS NULL AND target_version IS NULL)
        OR (target_kind IS NOT NULL AND target_id IS NOT NULL AND target_version IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS axond_cp_audit_event_revision_idx
    ON axond_cp_audit_event (revision_id, recorded_at DESC);

-- The idempotency record: what a caller's key published, and the checksum of the
-- state it published. Replay identity is the state checksum and nothing else, so
-- a retry that describes the same desired state replays its own outcome even if
-- the caller rebuilt the request. Scoped per caller — `caller_scope` is a digest
-- of the authenticated caller's identity, never a credential — so one
-- administrator's `retry-1` can neither replay nor block another's. Rows expire:
-- deduplication is a retry window, not a permanent namespace, and expiry never
-- touches the revision or the audit trail it points at.
CREATE TABLE IF NOT EXISTS axond_cp_idempotency (
    caller_scope     text        NOT NULL CHECK (caller_scope ~ '^sha256:[0-9a-f]{64}$'),
    idempotency_key  text        NOT NULL,
    state_checksum   text        NOT NULL CHECK (state_checksum ~ '^sha256:[0-9a-f]{64}$'),
    revision_id      text        NOT NULL REFERENCES axond_cp_revision (revision_id),
    mutation_id      text        NOT NULL REFERENCES axond_cp_mutation (mutation_id),
    recorded_at      timestamptz NOT NULL DEFAULT now(),
    expires_at       timestamptz NOT NULL,
    PRIMARY KEY (caller_scope, idempotency_key)
);

CREATE INDEX IF NOT EXISTS axond_cp_idempotency_expires_at_idx
    ON axond_cp_idempotency (expires_at);

-- Which revision is desired, as one row. Publication takes `FOR UPDATE` on it,
-- so concurrent publishers serialize on the head rather than racing to append,
-- and a reader asking "what is desired?" performs one primary-key lookup.
CREATE TABLE IF NOT EXISTS axond_cp_head (
    singleton    boolean     PRIMARY KEY DEFAULT true CHECK (singleton),
    revision_id  text        NULL REFERENCES axond_cp_revision (revision_id),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

INSERT INTO axond_cp_head (singleton, revision_id)
    VALUES (true, NULL)
    ON CONFLICT (singleton) DO NOTHING;
