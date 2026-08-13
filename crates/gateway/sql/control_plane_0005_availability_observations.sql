-- Axond durable discovery observations, migration 0003.
--
-- Availability is derived, never published: a revision states what a tenant
-- enabled and whom it may authenticate as, and nothing in it can state that a
-- particular account can actually call a particular model. That last fact is
-- learned — asynchronously, off the request path — and this table is where the
-- learning is kept so a replica that restarts does not start again from "I have
-- never looked".
--
-- What it is not:
--
--   * It is not desired state. No revision reads it, no publication writes it,
--     and losing the whole table costs a deployment its freshness and nothing
--     else: every target it described falls back to `unknown`, which is the
--     honest answer for a replica that has not looked. Fail-closed by
--     construction rather than by convention.
--   * It is not a request-path table. Availability is evaluated against the
--     in-memory index a replica compiled; this is read once per projection and
--     written by whatever takes the looks.
--
-- Two rows per (scope, target) at most, and the reason is the semantics rather
-- than an optimisation: an availability record holds the look it is deciding on
-- and the last look that found the target, so a discovery outage degrades to
-- last-known-good instead of to a refusal. `slot` is which of those a row is,
-- and the primary key is what makes "one current look, one fallback" a database
-- fact rather than a hope.
--
-- No provider detail is stored. A failed probe's `detail` can carry an upstream
-- error body — a URL with a key in the query string, an account name, a quota
-- message naming another tenant — and a column that holds it is a column that
-- ends up in a backup, a support session, and a screenshot. The bounded
-- `result`/`completeness` pair is what a verdict may state, so it is all that is
-- durable; operator detail lives for the lifetime of the process that observed
-- it and no longer.
CREATE TABLE IF NOT EXISTS axond_cp_availability_observation (
    tenant_id     text        NOT NULL REFERENCES axond_cp_tenant (tenant_id),
    project_id    text        NULL,
    provider      text        NOT NULL CHECK (length(provider) BETWEEN 1 AND 128),
    model         text        NOT NULL CHECK (length(model) BETWEEN 1 AND 128),
    -- `current` is the look being decided on; `last_known_good` is the newest
    -- look that found the target, kept for the outage that follows it.
    slot          text        NOT NULL CHECK (slot IN ('current', 'last_known_good')),
    result        text        NOT NULL CHECK (result IN ('present', 'absent', 'indeterminate')),
    completeness  text        NOT NULL CHECK (completeness IN ('complete', 'partial', 'unsupported', 'unreliable')),
    source        text        NOT NULL CHECK (source IN ('provider_listing', 'provider_probe', 'catalogue_record', 'operator_assertion')),
    observed_at   timestamptz NOT NULL,
    -- Null is "does not expire", which is not the same as "expired": an operator
    -- assertion holds until it is replaced, and a listing's TTL is the provider's
    -- to state.
    expires_at    timestamptz NULL,
    -- The newest conclusive answer this record has ever held, whether or not the
    -- look that carried it is still held. Without it, a stored positive that
    -- arrives after a complete listing dropped the target could resurrect it.
    definitive_at timestamptz NULL,
    recorded_at   timestamptz NOT NULL DEFAULT now(),
    -- A project row belongs to the tenant that owns it: the composite key exists
    -- on `axond_cp_project` for exactly this.
    FOREIGN KEY (tenant_id, project_id) REFERENCES axond_cp_project (tenant_id, project_id)
);

-- Uniqueness is two partial indexes rather than a primary key, because a
-- tenant-wide observation has no project and a primary key column cannot be null.
-- One slot per (scope, target) either way: a tenant-wide row and a project's row
-- about the same target are different scopes and both may exist.
CREATE UNIQUE INDEX IF NOT EXISTS axond_cp_availability_observation_tenant_slot_idx
    ON axond_cp_availability_observation (tenant_id, provider, model, slot)
    WHERE project_id IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS axond_cp_availability_observation_project_slot_idx
    ON axond_cp_availability_observation (tenant_id, project_id, provider, model, slot)
    WHERE project_id IS NOT NULL;

-- The read a projection issues: everything one scope knows, ordered so a reload
-- is deterministic.
CREATE INDEX IF NOT EXISTS axond_cp_availability_observation_scope_idx
    ON axond_cp_availability_observation (tenant_id, project_id, provider, model);

-- The same wall every tenant-owned table is behind, keyed on the same session
-- setting: a session pinned to one tenant reads that tenant's looks and no
-- others'. Availability leaks which models a tenant enabled and which of them
-- its credentials can reach, which is exactly the enumeration the rest of 0002's
-- policies exist to prevent.
ALTER TABLE axond_cp_availability_observation ENABLE ROW LEVEL SECURITY;
ALTER TABLE axond_cp_availability_observation FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS axond_cp_availability_observation_isolation ON axond_cp_availability_observation;
CREATE POLICY axond_cp_availability_observation_isolation ON axond_cp_availability_observation
    USING (
        coalesce(current_setting('axond.tenant_id', true), '') = ''
        OR tenant_id = current_setting('axond.tenant_id', true)
    )
    WITH CHECK (
        coalesce(current_setting('axond.tenant_id', true), '') = ''
        OR tenant_id = current_setting('axond.tenant_id', true)
    );
