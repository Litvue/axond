-- Additive usage schema migration for the v2 row shape.
--
-- Fresh installations apply usage_v2.sql first, then every usage_v2_<sequence>
-- file in filename order. Existing installations apply this file before
-- deploying a gateway that records unpriced-allow traffic with a NULL cost.
-- Replace axond_usage below when the usage sink uses a custom table.
--
-- Unpriced models admitted with `unpriced_models = allow` record
-- cost_microdollars as NULL. Priced rows still write an integer.
ALTER TABLE axond_usage
    ALTER COLUMN cost_microdollars DROP NOT NULL;
