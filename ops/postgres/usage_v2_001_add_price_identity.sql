-- Additive usage schema migration for the v2 row shape.
--
-- Fresh installations apply usage_v2.sql first, then this file and any later
-- usage_v2_<sequence>_<name>.sql files in filename order. Existing
-- installations apply this file before deploying a gateway that writes the
-- price-book identity. Replace axond_usage below when the usage sink uses a
-- custom table.
--
-- These columns name the immutable pricing a row was charged against: the
-- approved price-book resource version, the checksum of its body, and the
-- catalogue content it was approved against (docs/adr/0056-request-path-pricing.md).
-- They are NULL for a request the file configuration priced, and for every row
-- written before they existed.
--
-- The columns are nullable and do not change UsageRecord::SCHEMA_VERSION.
ALTER TABLE axond_usage
    ADD COLUMN IF NOT EXISTS price_book text,
    ADD COLUMN IF NOT EXISTS price_book_checksum text,
    ADD COLUMN IF NOT EXISTS price_catalog text;
