-- Migration 002: add zip to geocoded_records
--
-- zip is not PII in the same way as a full street address; it is retained
-- indefinitely alongside coordinates and block_geoid for use in ZIP-level
-- breakdown reports. Copied from raw_addresses at geocoding time.

ALTER TABLE geocoded_records ADD COLUMN zip TEXT;

INSERT INTO schema_migrations (version, applied_at)
VALUES (2, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'));
