-- Axond control-plane tenancy and access boundaries, migration 0002.
--
-- Migration 0001 stores revisions: immutable versions, manifests, mutations, and
-- an audit trail. It is deliberately body-agnostic — it never interprets a
-- resource body — which means the tenancy the domain validates (#191) and the
-- identity directory it resolves (#144) exist only inside canonical bytes. That
-- is enough for a single writer that always validates first. It is not enough for
-- two properties an operator has to be able to state about the database itself:
--
--   * a durable ownership or identity row cannot belong to a tenant that does
--     not exist, and cannot name another tenant's project;
--   * a session pinned to one tenant cannot read another's rows even if the
--     service layer above it has a bug.
--
-- So this migration projects the two ownership tables (`axond_cp_tenant`,
-- `axond_cp_project`) and the directory (`axond_cp_principal`,
-- `axond_cp_principal_role`) out of the revision they were published in, points
-- composite foreign keys at them, and enables row-level security keyed on one
-- session setting. The projection is written in the publishing transaction, so
-- "projected" and "published" cannot come apart.
--
-- Service-layer authorization remains authoritative. Nothing here decides whether
-- a principal may do something: roles are stored so a grant is a row that can be
-- audited, revoked, and constrained, and RLS is a second wall behind the first,
-- not the first wall.
--
-- Forward-only: 0001 is not edited, and no foreign key is added to
-- `axond_cp_resource_version` or `axond_cp_mutation`. Those tables hold history,
-- and the domain deliberately admits a revision an older build published whose
-- tenant-scoped rows have no projected owner: unroutable state, not a
-- contradiction. A foreign key there — even `NOT VALID`, which still checks every
-- future insert — would refuse to *republish* that history, turning a
-- compatibility exemption the domain grants into a write failure the operator
-- cannot clear. Ownership is therefore enforced where every row is new: the
-- projection tables below. Promoting the journal tables needs a backfill of
-- pre-0002 owners first, and is tracked as follow-up work rather than smuggled in
-- here.

-- Tenants, as rows. The lifecycle column is why this is a table and not a view
-- over resource versions: disabling or deleting a tenant is a transition that
-- billing, audit retention, and admission all read, and "is this tenant served?"
-- must be answerable without decoding a body.
--
-- Deletion is a lifecycle state, not a `DELETE`. A deleted tenant keeps its row,
-- its projects, its mutations, and its audit trail: physical erasure is a
-- separate compliance job with its own retention argument, and a foreign key to
-- a tenant that was erased would either fail or orphan the history that proves
-- what was billed.
CREATE TABLE IF NOT EXISTS axond_cp_tenant (
    tenant_id     text        PRIMARY KEY CHECK (tenant_id ~ '^ten_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'),
    slug          text        NOT NULL CHECK (length(slug) BETWEEN 1 AND 63),
    lifecycle     text        NOT NULL CHECK (lifecycle IN ('active', 'disabled', 'deleted')),
    revision_id   text        NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- A tenant's slug is unique among the tenants that still exist as names. A
-- deleted tenant releases its slug: keeping it reserved forever turns a deletion
-- into a permanent namespace claim, and the id — not the slug — is what history
-- points at.
CREATE UNIQUE INDEX IF NOT EXISTS axond_cp_tenant_slug_idx
    ON axond_cp_tenant (slug)
    WHERE lifecycle <> 'deleted';

-- Projects: the routing and accounting boundary beneath a tenant, and what a
-- stateless deployment calls a namespace. `UNIQUE (tenant_id, project_id)` is
-- redundant as a uniqueness claim — `project_id` is already the key — and is
-- declared anyway because it is the target a composite foreign key needs: it is
-- what makes "this row's project belongs to this row's tenant" a database fact
-- rather than a service-layer convention.
CREATE TABLE IF NOT EXISTS axond_cp_project (
    project_id   text        PRIMARY KEY CHECK (project_id ~ '^prj_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'),
    tenant_id    text        NOT NULL REFERENCES axond_cp_tenant (tenant_id),
    slug         text        NOT NULL CHECK (length(slug) BETWEEN 1 AND 63),
    revision_id  text        NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, project_id),
    UNIQUE (tenant_id, slug)
);

-- The identity directory: who may administer this deployment, and where.
--
-- Two kinds of principal, attributed by exactly the columns they have. A human
-- is named by an issuer-scoped OIDC subject, because a bare subject from an
-- unnamed issuer is not an identity — two identity providers can both call
-- someone `admin`. A workload is Axond-owned and named by nothing but its id.
--
-- `key_digest` is a verification digest, never key material: a minted workload
-- key is displayed once at creation and stored only as `sha256:...`, so a
-- database read — a backup, a replica, a support session — cannot recover a
-- credential. Provider credentials are a different problem with a different
-- owner (#198); this column exists so authenticating a workload against the
-- admin API does not require one.
CREATE TABLE IF NOT EXISTS axond_cp_principal (
    principal_id   text        PRIMARY KEY CHECK (principal_id ~ '^prn_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'),
    resource_id    text        NOT NULL,
    identity_kind  text        NOT NULL CHECK (identity_kind IN ('human', 'workload')),
    scope_kind     text        NOT NULL CHECK (scope_kind IN ('deployment', 'tenant', 'project')),
    tenant_id      text        NULL,
    project_id     text        NULL,
    slug           text        NOT NULL CHECK (length(slug) BETWEEN 1 AND 63),
    display_name   text        NOT NULL,
    issuer         text        NULL,
    subject        text        NULL,
    key_digest     text        NULL CHECK (key_digest ~ '^sha256:[0-9a-f]{64}$'),
    revision_id    text        NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT axond_cp_principal_scope_ownership CHECK (
        (scope_kind = 'deployment' AND tenant_id IS NULL AND project_id IS NULL)
        OR (scope_kind = 'tenant' AND tenant_id IS NOT NULL AND project_id IS NULL)
        OR (scope_kind = 'project' AND tenant_id IS NOT NULL AND project_id IS NOT NULL)
    ),
    -- A human has an issuer and a subject and no key; a workload has a key digest
    -- at most, and belongs to a tenant. A deployment-scoped workload would be a
    -- service account with authority over every tenant and no tenant to hold it
    -- accountable, which is refused here as well as in the domain.
    CONSTRAINT axond_cp_principal_identity_attribution CHECK (
        (identity_kind = 'human' AND issuer IS NOT NULL AND subject IS NOT NULL AND key_digest IS NULL)
        OR (identity_kind = 'workload' AND issuer IS NULL AND subject IS NULL AND tenant_id IS NOT NULL)
    ),
    FOREIGN KEY (tenant_id) REFERENCES axond_cp_tenant (tenant_id),
    FOREIGN KEY (tenant_id, project_id) REFERENCES axond_cp_project (tenant_id, project_id)
);

-- One sign-in resolves to one principal. Without this, two rows could claim the
-- same `(issuer, subject)` and "who is this?" would depend on which row a query
-- happened to return first.
CREATE UNIQUE INDEX IF NOT EXISTS axond_cp_principal_oidc_idx
    ON axond_cp_principal (issuer, subject)
    WHERE identity_kind = 'human';

-- A minted key authenticates at most one workload, for the same reason.
CREATE UNIQUE INDEX IF NOT EXISTS axond_cp_principal_key_digest_idx
    ON axond_cp_principal (key_digest)
    WHERE key_digest IS NOT NULL;

CREATE INDEX IF NOT EXISTS axond_cp_principal_tenant_idx
    ON axond_cp_principal (tenant_id, identity_kind);

-- Grants, one row per role. A row rather than an array so a grant can be
-- revoked, indexed, and counted, and so "who is a tenant admin here?" is a
-- query rather than a scan of decoded bodies.
CREATE TABLE IF NOT EXISTS axond_cp_principal_role (
    principal_id  text NOT NULL REFERENCES axond_cp_principal (principal_id) ON DELETE CASCADE,
    role          text NOT NULL CHECK (role IN ('platform-admin', 'tenant-admin', 'operator', 'billing-viewer', 'developer')),
    PRIMARY KEY (principal_id, role)
);

-- Refused administrative actions.
--
-- A separate table because a denial has no revision: nothing was published, so
-- there is no manifest for `axond_cp_audit_event` to hang off, and inventing an
-- empty revision to hold a refusal would put a row in the revision chain that
-- describes no state. The columns that matter are who asked, what they asked for,
-- where, and the reason — which is recorded here in full even though the caller
-- is told only `forbidden`: an administrator reading their own tenant's trail
-- should see why, and a stranger probing for tenant ids should learn nothing.
CREATE TABLE IF NOT EXISTS axond_cp_access_denial (
    denial_id          text        PRIMARY KEY CHECK (denial_id ~ '^aud_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'),
    actor_kind         text        NOT NULL CHECK (actor_kind IN ('human', 'breakglass', 'workload', 'system')),
    actor_issuer       text        NULL,
    actor_subject      text        NULL,
    actor_component    text        NULL,
    actor_tenant_id    text        NULL,
    actor_principal_id text        NULL,
    surface            text        NOT NULL,
    action             text        NOT NULL,
    scope_kind         text        NOT NULL CHECK (scope_kind IN ('deployment', 'tenant', 'project')),
    tenant_id          text        NULL,
    project_id         text        NULL,
    reason             text        NOT NULL,
    recorded_at        timestamptz NOT NULL,
    CONSTRAINT axond_cp_access_denial_scope_ownership CHECK (
        (scope_kind = 'deployment' AND tenant_id IS NULL AND project_id IS NULL)
        OR (scope_kind = 'tenant' AND tenant_id IS NOT NULL AND project_id IS NULL)
        OR (scope_kind = 'project' AND tenant_id IS NOT NULL AND project_id IS NOT NULL)
    ),
    CONSTRAINT axond_cp_access_denial_actor_attribution CHECK (
        (actor_kind = 'human' AND actor_issuer IS NOT NULL AND actor_subject IS NOT NULL AND actor_component IS NULL AND actor_tenant_id IS NULL AND actor_principal_id IS NULL)
        OR (actor_kind = 'breakglass' AND actor_issuer IS NULL AND actor_subject IS NULL AND actor_component IS NULL AND actor_tenant_id IS NULL AND actor_principal_id IS NULL)
        OR (actor_kind = 'workload' AND actor_issuer IS NULL AND actor_subject IS NULL AND actor_component IS NULL AND actor_tenant_id IS NOT NULL AND actor_principal_id IS NOT NULL)
        OR (actor_kind = 'system' AND actor_component IS NOT NULL AND actor_issuer IS NULL AND actor_subject IS NULL AND actor_tenant_id IS NULL AND actor_principal_id IS NULL)
    )
);

-- A denial is *not* referentially tied to the tenant it named. It usually names a
-- tenant the caller has no business knowing exists, and half of them name a
-- tenant that does not: a foreign key here would refuse to record exactly the
-- attempts most worth recording.
CREATE INDEX IF NOT EXISTS axond_cp_access_denial_tenant_idx
    ON axond_cp_access_denial (tenant_id, recorded_at DESC);

CREATE INDEX IF NOT EXISTS axond_cp_access_denial_recorded_at_idx
    ON axond_cp_access_denial (recorded_at DESC);

-- Workload attribution on the two attribution tables. A workload's changes were
-- previously unrecordable: `actor_kind` admitted three values, and attributing a
-- service account as a human would have made "everything this person did" return
-- a CI job's changes.
ALTER TABLE axond_cp_mutation
    ADD COLUMN IF NOT EXISTS actor_tenant_id text NULL,
    ADD COLUMN IF NOT EXISTS actor_principal_id text NULL;

ALTER TABLE axond_cp_audit_event
    ADD COLUMN IF NOT EXISTS actor_tenant_id text NULL,
    ADD COLUMN IF NOT EXISTS actor_principal_id text NULL;

ALTER TABLE axond_cp_mutation
    DROP CONSTRAINT IF EXISTS axond_cp_mutation_actor_kind_check;
ALTER TABLE axond_cp_mutation
    DROP CONSTRAINT IF EXISTS axond_cp_mutation_actor_attribution;
ALTER TABLE axond_cp_mutation
    ADD CONSTRAINT axond_cp_mutation_actor_attribution CHECK (
        (actor_kind = 'human' AND actor_issuer IS NOT NULL AND actor_subject IS NOT NULL AND actor_component IS NULL AND actor_tenant_id IS NULL AND actor_principal_id IS NULL)
        OR (actor_kind = 'breakglass' AND actor_issuer IS NULL AND actor_subject IS NULL AND actor_component IS NULL AND actor_tenant_id IS NULL AND actor_principal_id IS NULL)
        OR (actor_kind = 'workload' AND actor_issuer IS NULL AND actor_subject IS NULL AND actor_component IS NULL AND actor_tenant_id IS NOT NULL AND actor_principal_id IS NOT NULL)
        OR (actor_kind = 'system' AND actor_component IS NOT NULL AND actor_issuer IS NULL AND actor_subject IS NULL AND actor_tenant_id IS NULL AND actor_principal_id IS NULL)
    );

ALTER TABLE axond_cp_audit_event
    DROP CONSTRAINT IF EXISTS axond_cp_audit_event_actor_kind_check;
ALTER TABLE axond_cp_audit_event
    DROP CONSTRAINT IF EXISTS axond_cp_audit_event_actor_attribution;
ALTER TABLE axond_cp_audit_event
    ADD CONSTRAINT axond_cp_audit_event_actor_attribution CHECK (
        (actor_kind = 'human' AND actor_issuer IS NOT NULL AND actor_subject IS NOT NULL AND actor_component IS NULL AND actor_tenant_id IS NULL AND actor_principal_id IS NULL)
        OR (actor_kind = 'breakglass' AND actor_issuer IS NULL AND actor_subject IS NULL AND actor_component IS NULL AND actor_tenant_id IS NULL AND actor_principal_id IS NULL)
        OR (actor_kind = 'workload' AND actor_issuer IS NULL AND actor_subject IS NULL AND actor_component IS NULL AND actor_tenant_id IS NOT NULL AND actor_principal_id IS NOT NULL)
        OR (actor_kind = 'system' AND actor_component IS NOT NULL AND actor_issuer IS NULL AND actor_subject IS NULL AND actor_tenant_id IS NULL AND actor_principal_id IS NULL)
    );

-- Row-level security: the second wall.
--
-- Every policy has the same shape. `axond.tenant_id` is a session setting, and
-- when it is unset or empty the policy admits everything — the publisher writes
-- deployment-wide state and must see all of it, and a migration that silently
-- hid rows from the existing store would be a data-loss bug rather than a
-- hardening step. When a session *does* set it, that session sees deployment
-- rows and its own tenant's rows and nothing else, whatever query the service
-- layer above it issues.
--
-- `FORCE` so the policies apply to the table owner too. Without it, a deployment
-- whose application role owns its tables — the common single-role install — would
-- enable RLS and get no enforcement at all, which is worse than not claiming it.
ALTER TABLE axond_cp_resource_version ENABLE ROW LEVEL SECURITY;
ALTER TABLE axond_cp_resource_version FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS axond_cp_resource_version_tenant_isolation ON axond_cp_resource_version;
CREATE POLICY axond_cp_resource_version_tenant_isolation ON axond_cp_resource_version
    USING (
        coalesce(current_setting('axond.tenant_id', true), '') = ''
        OR tenant_id IS NULL
        OR tenant_id = current_setting('axond.tenant_id', true)
    )
    WITH CHECK (
        coalesce(current_setting('axond.tenant_id', true), '') = ''
        OR tenant_id IS NULL
        OR tenant_id = current_setting('axond.tenant_id', true)
    );

ALTER TABLE axond_cp_tenant ENABLE ROW LEVEL SECURITY;
ALTER TABLE axond_cp_tenant FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS axond_cp_tenant_isolation ON axond_cp_tenant;
CREATE POLICY axond_cp_tenant_isolation ON axond_cp_tenant
    USING (
        coalesce(current_setting('axond.tenant_id', true), '') = ''
        OR tenant_id = current_setting('axond.tenant_id', true)
    )
    WITH CHECK (
        coalesce(current_setting('axond.tenant_id', true), '') = ''
        OR tenant_id = current_setting('axond.tenant_id', true)
    );

ALTER TABLE axond_cp_project ENABLE ROW LEVEL SECURITY;
ALTER TABLE axond_cp_project FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS axond_cp_project_isolation ON axond_cp_project;
CREATE POLICY axond_cp_project_isolation ON axond_cp_project
    USING (
        coalesce(current_setting('axond.tenant_id', true), '') = ''
        OR tenant_id = current_setting('axond.tenant_id', true)
    )
    WITH CHECK (
        coalesce(current_setting('axond.tenant_id', true), '') = ''
        OR tenant_id = current_setting('axond.tenant_id', true)
    );

ALTER TABLE axond_cp_principal ENABLE ROW LEVEL SECURITY;
ALTER TABLE axond_cp_principal FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS axond_cp_principal_isolation ON axond_cp_principal;
CREATE POLICY axond_cp_principal_isolation ON axond_cp_principal
    USING (
        coalesce(current_setting('axond.tenant_id', true), '') = ''
        OR tenant_id IS NULL
        OR tenant_id = current_setting('axond.tenant_id', true)
    )
    WITH CHECK (
        coalesce(current_setting('axond.tenant_id', true), '') = ''
        OR tenant_id IS NULL
        OR tenant_id = current_setting('axond.tenant_id', true)
    );

ALTER TABLE axond_cp_access_denial ENABLE ROW LEVEL SECURITY;
ALTER TABLE axond_cp_access_denial FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS axond_cp_access_denial_isolation ON axond_cp_access_denial;
CREATE POLICY axond_cp_access_denial_isolation ON axond_cp_access_denial
    USING (
        coalesce(current_setting('axond.tenant_id', true), '') = ''
        OR tenant_id IS NULL
        OR tenant_id = current_setting('axond.tenant_id', true)
    )
    WITH CHECK (
        coalesce(current_setting('axond.tenant_id', true), '') = ''
        OR tenant_id IS NULL
        OR tenant_id = current_setting('axond.tenant_id', true)
    );

-- The administrative journal. A pinned session that could read every mutation
-- would learn which tenants exist and what was changed in them, which is the
-- enumeration the opaque `forbidden` refusal and the per-tenant denial read
-- exist to prevent; a journal outside the wall makes the wall decorative.
ALTER TABLE axond_cp_mutation ENABLE ROW LEVEL SECURITY;
ALTER TABLE axond_cp_mutation FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS axond_cp_mutation_isolation ON axond_cp_mutation;
CREATE POLICY axond_cp_mutation_isolation ON axond_cp_mutation
    USING (
        coalesce(current_setting('axond.tenant_id', true), '') = ''
        OR tenant_id IS NULL
        OR tenant_id = current_setting('axond.tenant_id', true)
    )
    WITH CHECK (
        coalesce(current_setting('axond.tenant_id', true), '') = ''
        OR tenant_id IS NULL
        OR tenant_id = current_setting('axond.tenant_id', true)
    );

-- The two tables with no tenant column of their own are filtered through the row
-- that owns them: a grant through its principal, an audit event through its
-- mutation. The subquery is itself subject to that table's policy, so the
-- ownership question is answered by the same wall rather than by a second copy
-- of it — and a grant whose principal a pinned session cannot see is a grant it
-- cannot see either.
ALTER TABLE axond_cp_principal_role ENABLE ROW LEVEL SECURITY;
ALTER TABLE axond_cp_principal_role FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS axond_cp_principal_role_isolation ON axond_cp_principal_role;
CREATE POLICY axond_cp_principal_role_isolation ON axond_cp_principal_role
    USING (
        coalesce(current_setting('axond.tenant_id', true), '') = ''
        OR EXISTS (
            SELECT 1 FROM axond_cp_principal AS owner
            WHERE owner.principal_id = axond_cp_principal_role.principal_id
        )
    )
    WITH CHECK (
        coalesce(current_setting('axond.tenant_id', true), '') = ''
        OR EXISTS (
            SELECT 1 FROM axond_cp_principal AS owner
            WHERE owner.principal_id = axond_cp_principal_role.principal_id
        )
    );

ALTER TABLE axond_cp_audit_event ENABLE ROW LEVEL SECURITY;
ALTER TABLE axond_cp_audit_event FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS axond_cp_audit_event_isolation ON axond_cp_audit_event;
CREATE POLICY axond_cp_audit_event_isolation ON axond_cp_audit_event
    USING (
        coalesce(current_setting('axond.tenant_id', true), '') = ''
        OR EXISTS (
            SELECT 1 FROM axond_cp_mutation AS carried
            WHERE carried.mutation_id = axond_cp_audit_event.mutation_id
        )
    )
    WITH CHECK (
        coalesce(current_setting('axond.tenant_id', true), '') = ''
        OR EXISTS (
            SELECT 1 FROM axond_cp_mutation AS carried
            WHERE carried.mutation_id = axond_cp_audit_event.mutation_id
        )
    );

-- Idempotency records name no tenant, but each one points at the mutation it
-- deduplicates, and that mutation is a tenant's change. Filtered through it, a
-- pinned session sees the keys of its own retries and learns nothing about how
-- often another tenant publishes — the cadence signal the opaque `forbidden`
-- refusal exists to suppress would otherwise be readable straight off this
-- table.
ALTER TABLE axond_cp_idempotency ENABLE ROW LEVEL SECURITY;
ALTER TABLE axond_cp_idempotency FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS axond_cp_idempotency_isolation ON axond_cp_idempotency;
CREATE POLICY axond_cp_idempotency_isolation ON axond_cp_idempotency
    USING (
        coalesce(current_setting('axond.tenant_id', true), '') = ''
        OR EXISTS (
            SELECT 1 FROM axond_cp_mutation AS recorded
            WHERE recorded.mutation_id = axond_cp_idempotency.mutation_id
        )
    )
    WITH CHECK (
        coalesce(current_setting('axond.tenant_id', true), '') = ''
        OR EXISTS (
            SELECT 1 FROM axond_cp_mutation AS recorded
            WHERE recorded.mutation_id = axond_cp_idempotency.mutation_id
        )
    );

-- The publication chain itself — the head, the revisions, what each revision
-- carried, and the content it carried it as — is deployment-wide by
-- construction: one revision is *every* tenant's desired state at one instant,
-- so there is no tenant column to key a policy on and no per-tenant subset of a
-- revision to expose. A tenant's view of published state is its resource
-- versions, which are already walled; the chain that published them is platform
-- state, and a session pinned to a tenant reads none of it. An unpinned session
-- — the service, a migration, an operator's `psql` — is unaffected, which is the
-- same shape every policy above has.
DO $$
DECLARE
    chained text;
BEGIN
    FOREACH chained IN ARRAY ARRAY[
        'axond_cp_head',
        'axond_cp_revision',
        'axond_cp_revision_entry',
        'axond_cp_revision_blob',
        'axond_cp_blob',
        'axond_cp_resource_dependency'
    ] LOOP
        EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', chained);
        EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', chained);
        EXECUTE format('DROP POLICY IF EXISTS %I ON %I', chained || '_isolation', chained);
        EXECUTE format(
            'CREATE POLICY %I ON %I USING (%s) WITH CHECK (%s)',
            chained || '_isolation',
            chained,
            'coalesce(current_setting(''axond.tenant_id'', true), '''') = ''''',
            'coalesce(current_setting(''axond.tenant_id'', true), '''') = '''''
        );
    END LOOP;
END
$$;
