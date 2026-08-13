-- Control-plane schema 0003: deferred tenancy constraints, workload attribution,
-- and the attribution half of the denial and mutation walls (#144).
--
-- Forward-only, and that is the whole reason this file exists. Everything here
-- could have been written into 0002, which introduced the tables it alters — but
-- 0002 is applied and recorded, and the ledger compares a stored checksum against
-- the shipped text (ADR 0009, ADR 0032). Editing it would report every migrated
-- database as drifted rather than migrating it. So the rules 0002 got wrong are
-- replaced here, by name, idempotently.
--
-- Three things change.
--
-- 1. Uniqueness and ownership become *deferrable*. Uniqueness is a property of a
--    revision, not of the order the projection happens to write its rows in: two
--    tenants that trade names, two projects that trade slugs, two administrators
--    that trade sign-ins, or a revision that moves a project — and the principals
--    scoped into it — to another tenant, all pass through an intermediate row set
--    that violates an immediately-checked rule. An index cannot be deferred, so
--    the partial unique indexes 0002 created are dropped and replaced by
--    `EXCLUDE` constraints, and the foreign keys are re-declared deferrable. The
--    projection settles them with `SET CONSTRAINTS ALL IMMEDIATE` once every row
--    is written, so a state that really is contradictory is still refused inside
--    the projection, where it can be attributed, rather than at commit.
--
-- 2. `actor_kind = 'workload'` is admitted by the journal's attribution checks.
--    0001 wrote its vocabulary as an inline column check, so the constraint
--    holding the old list is named by PostgreSQL rather than by us; a guessed name
--    dropped with `IF EXISTS` succeeds silently against a database that named it
--    differently and then rejects the first workload-attributed mutation. The old
--    checks are therefore found by what they constrain.
--
-- 3. The denial and mutation policies filter the *actor's* tenant as well as the
--    row's scope. A row that names no tenant is shared state; the tenant and
--    principal id of the workload that attempted it are not.

-- A tenant's name, deferrable. `DROP INDEX` rather than leaving the index beside
-- the constraint: two rules for one property means the undeferrable one decides,
-- and a deployment that applied 0002 would keep refusing name swaps forever.
DROP INDEX IF EXISTS axond_cp_tenant_slug_idx;

DO $$
BEGIN
    IF NOT EXISTS (
        -- By the table, not by the name alone: a constraint name is unique per
        -- table, not per database, and a deployment that shares its database with
        -- another schema would otherwise skip creating this one.
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'axond_cp_tenant'::regclass
          AND conname = 'axond_cp_tenant_slug_unique'
    ) THEN
        ALTER TABLE axond_cp_tenant
            ADD CONSTRAINT axond_cp_tenant_slug_unique
            EXCLUDE (slug WITH =) WHERE (lifecycle <> 'deleted')
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END
$$;

-- A project's name and its owner. 0002 declared both inline, so both are named by
-- PostgreSQL: they are found by their definition and replaced by named deferrable
-- constraints, which is also what lets a later migration reason about them.
--
-- The name constraint is unconditional, where a tenant's is partial on lifecycle,
-- because a project has no lifecycle to publish: a project a revision stops
-- declaring keeps its row and its name, and the name is handed back by renaming
-- that project in the revision that drops it. Giving a project the lifecycle a
-- tenant has changes the tenancy contract (#191) every downstream slice reads, so
-- it is follow-up work; the runbook states the release path this schema supports.
DO $$
DECLARE
    stale record;
BEGIN
    FOR stale IN
        SELECT conname FROM pg_constraint
        WHERE conrelid = 'axond_cp_project'::regclass
          AND contype = 'u'
          AND pg_get_constraintdef(oid) = 'UNIQUE (tenant_id, slug)'
          AND conname <> 'axond_cp_project_slug_unique'
    LOOP
        EXECUTE format('ALTER TABLE axond_cp_project DROP CONSTRAINT %I', stale.conname);
    END LOOP;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'axond_cp_project'::regclass
          AND conname = 'axond_cp_project_slug_unique'
    ) THEN
        ALTER TABLE axond_cp_project
            ADD CONSTRAINT axond_cp_project_slug_unique UNIQUE (tenant_id, slug)
            DEFERRABLE INITIALLY DEFERRED;
    END IF;

    FOR stale IN
        SELECT conname FROM pg_constraint
        WHERE conrelid = 'axond_cp_project'::regclass
          AND contype = 'f'
          AND NOT condeferrable
          AND pg_get_constraintdef(oid) LIKE 'FOREIGN KEY (tenant_id) REFERENCES axond_cp_tenant%'
    LOOP
        EXECUTE format('ALTER TABLE axond_cp_project DROP CONSTRAINT %I', stale.conname);
    END LOOP;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'axond_cp_project'::regclass
          AND conname = 'axond_cp_project_tenant_fkey'
    ) THEN
        ALTER TABLE axond_cp_project
            ADD CONSTRAINT axond_cp_project_tenant_fkey
            FOREIGN KEY (tenant_id) REFERENCES axond_cp_tenant (tenant_id)
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END
$$;

-- One sign-in resolves to one principal, and a minted key authenticates one
-- workload — as exclusion constraints, so that reassigning two administrators'
-- sign-ins, or handing one workload's key to another, is a revision the directory
-- accepts rather than a race on which id sorted first.
DROP INDEX IF EXISTS axond_cp_principal_oidc_idx;
DROP INDEX IF EXISTS axond_cp_principal_key_digest_idx;

DO $$
DECLARE
    stale record;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'axond_cp_principal'::regclass
          AND conname = 'axond_cp_principal_oidc_unique'
    ) THEN
        ALTER TABLE axond_cp_principal
            ADD CONSTRAINT axond_cp_principal_oidc_unique
            EXCLUDE (issuer WITH =, subject WITH =) WHERE (identity_kind = 'human')
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'axond_cp_principal'::regclass
          AND conname = 'axond_cp_principal_key_digest_unique'
    ) THEN
        ALTER TABLE axond_cp_principal
            ADD CONSTRAINT axond_cp_principal_key_digest_unique
            EXCLUDE (key_digest WITH =) WHERE (key_digest IS NOT NULL)
            DEFERRABLE INITIALLY DEFERRED;
    END IF;

    -- A principal's owner, deferred for the reason the names are: a revision that
    -- moves a project to another tenant carries the principals scoped into it, and
    -- the composite key has no `ON UPDATE` action to cascade instead.
    FOR stale IN
        SELECT conname FROM pg_constraint
        WHERE conrelid = 'axond_cp_principal'::regclass
          AND contype = 'f'
          AND NOT condeferrable
    LOOP
        EXECUTE format('ALTER TABLE axond_cp_principal DROP CONSTRAINT %I', stale.conname);
    END LOOP;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'axond_cp_principal'::regclass
          AND conname = 'axond_cp_principal_tenant_fkey'
    ) THEN
        ALTER TABLE axond_cp_principal
            ADD CONSTRAINT axond_cp_principal_tenant_fkey
            FOREIGN KEY (tenant_id) REFERENCES axond_cp_tenant (tenant_id)
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'axond_cp_principal'::regclass
          AND conname = 'axond_cp_principal_project_fkey'
    ) THEN
        ALTER TABLE axond_cp_principal
            ADD CONSTRAINT axond_cp_principal_project_fkey
            FOREIGN KEY (tenant_id, project_id)
            REFERENCES axond_cp_project (tenant_id, project_id)
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END
$$;

-- The journal's attribution vocabulary, found by what it constrains rather than
-- by a name PostgreSQL chose.
DO $$
DECLARE
    stale record;
BEGIN
    FOR stale IN
        SELECT conrelid::regclass AS table_name, conname
        FROM pg_constraint
        WHERE conrelid IN ('axond_cp_mutation'::regclass, 'axond_cp_audit_event'::regclass)
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%actor_kind%'
    LOOP
        EXECUTE format(
            'ALTER TABLE %s DROP CONSTRAINT %I',
            stale.table_name,
            stale.conname
        );
    END LOOP;
END
$$;

ALTER TABLE axond_cp_mutation
    ADD CONSTRAINT axond_cp_mutation_actor_attribution CHECK (
        (actor_kind = 'human' AND actor_issuer IS NOT NULL AND actor_subject IS NOT NULL AND actor_component IS NULL AND actor_tenant_id IS NULL AND actor_principal_id IS NULL)
        OR (actor_kind = 'breakglass' AND actor_issuer IS NULL AND actor_subject IS NULL AND actor_component IS NULL AND actor_tenant_id IS NULL AND actor_principal_id IS NULL)
        OR (actor_kind = 'workload' AND actor_tenant_id IS NOT NULL AND actor_principal_id IS NOT NULL AND actor_issuer IS NULL AND actor_subject IS NULL AND actor_component IS NULL)
        OR (actor_kind = 'system' AND actor_component IS NOT NULL AND actor_issuer IS NULL AND actor_subject IS NULL AND actor_tenant_id IS NULL AND actor_principal_id IS NULL)
    );

ALTER TABLE axond_cp_audit_event
    ADD CONSTRAINT axond_cp_audit_event_actor_attribution CHECK (
        (actor_kind = 'human' AND actor_issuer IS NOT NULL AND actor_subject IS NOT NULL AND actor_component IS NULL AND actor_tenant_id IS NULL AND actor_principal_id IS NULL)
        OR (actor_kind = 'breakglass' AND actor_issuer IS NULL AND actor_subject IS NULL AND actor_component IS NULL AND actor_tenant_id IS NULL AND actor_principal_id IS NULL)
        OR (actor_kind = 'workload' AND actor_tenant_id IS NOT NULL AND actor_principal_id IS NOT NULL AND actor_issuer IS NULL AND actor_subject IS NULL AND actor_component IS NULL)
        OR (actor_kind = 'system' AND actor_component IS NOT NULL AND actor_issuer IS NULL AND actor_subject IS NULL AND actor_tenant_id IS NULL AND actor_principal_id IS NULL)
    );

-- A refusal this tenant was the target of is this tenant's row, whoever attempted
-- it: it is the event the trail exists for, and withholding it would record a
-- cross-tenant probe that the tenant it was aimed at can never read. The
-- attribution filter therefore applies to the one class of row a pinned session
-- reads that is not its own — a deployment-scoped refusal names no tenant, so the
-- row is shared state, but when a workload made the attempt it carries that
-- workload's tenant and principal id, and reading those would tell one tenant
-- which service accounts of every other tenant tried to administer the deployment
-- and what they tried. The unpinned publisher sees every row.
DROP POLICY IF EXISTS axond_cp_access_denial_isolation ON axond_cp_access_denial;
CREATE POLICY axond_cp_access_denial_isolation ON axond_cp_access_denial
    USING (
        coalesce(current_setting('axond.tenant_id', true), '') = ''
        OR tenant_id = current_setting('axond.tenant_id', true)
        OR (
            tenant_id IS NULL
            AND (actor_tenant_id IS NULL OR actor_tenant_id = current_setting('axond.tenant_id', true))
        )
    )
    WITH CHECK (
        coalesce(current_setting('axond.tenant_id', true), '') = ''
        OR tenant_id = current_setting('axond.tenant_id', true)
        OR (
            tenant_id IS NULL
            AND (actor_tenant_id IS NULL OR actor_tenant_id = current_setting('axond.tenant_id', true))
        )
    );

-- The journal, by the same rule and therefore in the same shape: a change that
-- names this tenant is this tenant's, whoever made it — hiding it would hide a
-- tenant's own change history, and its audit events and idempotency records with
-- it, since those are walled through this table. Only the deployment-scoped rows,
-- which a pinned session reads without being their subject, withhold the actor.
DROP POLICY IF EXISTS axond_cp_mutation_isolation ON axond_cp_mutation;
CREATE POLICY axond_cp_mutation_isolation ON axond_cp_mutation
    USING (
        coalesce(current_setting('axond.tenant_id', true), '') = ''
        OR tenant_id = current_setting('axond.tenant_id', true)
        OR (
            tenant_id IS NULL
            AND (actor_tenant_id IS NULL OR actor_tenant_id = current_setting('axond.tenant_id', true))
        )
    )
    WITH CHECK (
        coalesce(current_setting('axond.tenant_id', true), '') = ''
        OR tenant_id = current_setting('axond.tenant_id', true)
        OR (
            tenant_id IS NULL
            AND (actor_tenant_id IS NULL OR actor_tenant_id = current_setting('axond.tenant_id', true))
        )
    );
