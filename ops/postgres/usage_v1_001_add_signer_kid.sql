-- Additive usage schema migration for the v1 row shape.
--
-- Fresh installations apply usage_v1.sql first, then this file and any later
-- usage_v1_<sequence>_<name>.sql files in filename order. Existing
-- installations apply this file before deploying a gateway that writes
-- signer_kid. Replace axond_usage below when the usage sink uses a custom table.
--
-- This column is nullable and does not change UsageRecord::SCHEMA_VERSION.
ALTER TABLE axond_usage
    ADD COLUMN IF NOT EXISTS signer_kid text;
