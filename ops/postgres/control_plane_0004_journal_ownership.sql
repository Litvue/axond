-- Control-plane schema 0004: journal ownership, as a key the database holds (#144).
--
-- 0002 projected tenants and projects out of the revision they were published in
-- so that ownership could be a foreign key rather than a service-layer
-- convention, and deliberately stopped short of the two journal tables:
-- `axond_cp_resource_version` and `axond_cp_mutation` hold history, and the
-- domain admits a revision an older build published whose tenant-scoped rows
-- have no projected owner. Unroutable state, not a contradiction. A key there
-- would have refused to *republish* that history — a rollback to a pre-tenancy
-- revision is the ordinary case — so 0002 recorded the gap instead of closing it
-- badly.
--
-- The gap is closed here in the only way that keeps both properties. Two pieces:
--
-- 1. The keys are `NOT VALID`. Rows already stored keep the exemption 0002
--    granted, because rewriting them means inventing an owner for a tenant no
--    revision ever declared, and an invented row is indistinguishable afterwards
--    from a published one. `NOT VALID` still checks every row written from now
--    on, which is the property an operator wants stated about the database: a
--    journal row cannot name a tenant this deployment has no row for.
--
-- 2. The publishing projection makes that satisfiable for history too. Before it
--    writes journal rows it records an owner for every tenant the state's
--    resources name and the revision does not declare, at `lifecycle = 'deleted'`
--    — the vocabulary's own word for a tenant that is neither served nor
--    administrable, which is exactly what an undeclared tenant is. So
--    republishing a pre-tenancy revision still succeeds, and what it leaves
--    behind is a tenant row that says "referenced by history, declared by
--    nothing" rather than a live tenant nobody granted. Resources only: a
--    mutation's scope and an actor's tenant describe a change being made now, so
--    one naming a tenant this deployment has no row for is refused by these keys
--    rather than handed a row that would satisfy the principal key afterwards.
--
-- No `VALIDATE CONSTRAINT` step follows, and none is pending: validation would
-- scan for exactly the historical rows point 1 exempts, so it would fail on any
-- database that has them, and reporting a validation an operator can never
-- complete is worse than stating the boundary. The runbook says which rows are
-- covered.
--
-- Ownership stops at the tenant. A composite key to `axond_cp_project` would need
-- an owner row for every project pre-0002 history names, and a project has no
-- lifecycle to publish (#191): a synthesized project row would be
-- indistinguishable from a declared one, which is the failure point 1 avoids for
-- tenants. The tenant is the isolation boundary — it is what row-level security
-- is keyed on and what a cross-tenant reference is measured against — so it is
-- what the database enforces here; a journal row's project remains checked in the
-- domain, where the revision it belongs to is available. Giving projects a
-- lifecycle is the tenancy-contract change that would let this be tightened.
--
-- `axond_cp_access_denial` stays outside this migration for the reason 0002 gives
-- it: a refusal's whole subject may be a tenant that does not exist, and a key
-- there would refuse to record exactly the attempt the trail exists for.
--
-- `actor_principal_id` deliberately gets no key either: a principal row is
-- deleted when a revision stops declaring it, which is the point of that table,
-- and a key from the journal to it would either resurrect revoked administrators
-- or refuse to record what they did. Attribution is copied onto the mutation,
-- never joined from the directory.

DO $$
BEGIN
    -- By table and name, for the reason 0003 states: a constraint name is unique
    -- per table, not per database.
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'axond_cp_resource_version'::regclass
          AND conname = 'axond_cp_resource_version_tenant_fkey'
    ) THEN
        -- Deferrable, like every other ownership key this schema declares: a
        -- revision that moves ownership passes through an intermediate row set,
        -- and the projection settles the constraints once every row is written so
        -- that a state which really is contradictory is refused where it can be
        -- attributed.
        ALTER TABLE axond_cp_resource_version
            ADD CONSTRAINT axond_cp_resource_version_tenant_fkey
            FOREIGN KEY (tenant_id) REFERENCES axond_cp_tenant (tenant_id)
            DEFERRABLE INITIALLY DEFERRED NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'axond_cp_mutation'::regclass
          AND conname = 'axond_cp_mutation_tenant_fkey'
    ) THEN
        ALTER TABLE axond_cp_mutation
            ADD CONSTRAINT axond_cp_mutation_tenant_fkey
            FOREIGN KEY (tenant_id) REFERENCES axond_cp_tenant (tenant_id)
            DEFERRABLE INITIALLY DEFERRED NOT VALID;
    END IF;

    -- The tenant of the actor, which is a second ownership claim on the same row:
    -- a workload that administers something belongs to a tenant, and a mutation
    -- attributed to a tenant the deployment has no row for is attribution nobody
    -- can follow up on. No apostrophe appears in these guards on purpose: the
    -- adoption reader treats one as the start of a literal, and a guard it cannot
    -- read is a migration it cannot confirm.
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'axond_cp_mutation'::regclass
          AND conname = 'axond_cp_mutation_actor_tenant_fkey'
    ) THEN
        ALTER TABLE axond_cp_mutation
            ADD CONSTRAINT axond_cp_mutation_actor_tenant_fkey
            FOREIGN KEY (actor_tenant_id) REFERENCES axond_cp_tenant (tenant_id)
            DEFERRABLE INITIALLY DEFERRED NOT VALID;
    END IF;

    -- The audit trail carries its own copy of the attribution, and a copy that can
    -- name a tenant the copy on the mutation cannot is a copy that disagrees.
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'axond_cp_audit_event'::regclass
          AND conname = 'axond_cp_audit_event_actor_tenant_fkey'
    ) THEN
        ALTER TABLE axond_cp_audit_event
            ADD CONSTRAINT axond_cp_audit_event_actor_tenant_fkey
            FOREIGN KEY (actor_tenant_id) REFERENCES axond_cp_tenant (tenant_id)
            DEFERRABLE INITIALLY DEFERRED NOT VALID;
    END IF;
END
$$;
