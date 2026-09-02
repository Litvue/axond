-- Additive usage schema migration for the v2 row shape.
--
-- Fresh installations apply usage_v2.sql first, then every usage_v2_<sequence>
-- file in filename order. Existing installations apply this file before
-- deploying a gateway that records the active budget period at admission.
-- Replace axond_usage below when the usage sink uses a custom table.
--
-- `period` is the namespace's active budget period at admission (ADR 0063).
-- It is nullable and does not change UsageRecord::SCHEMA_VERSION.
ALTER TABLE axond_usage
    ADD COLUMN IF NOT EXISTS period text;
