-- Migration 001: initial schema
-- Pipeline: raw_addresses (staging, PII) → geocoded_records → district_assignments
-- BEF data: bef_versions + bef_blocks (versioned, never deleted)

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- ── PII staging table ────────────────────────────────────────────────────────
-- Raw addresses live ONLY here. Purged on retention_purge_after (default 90 days
-- post-geocoding, configurable). All other tables reference address_hash only.
CREATE TABLE IF NOT EXISTS raw_addresses (
    id                  TEXT PRIMARY KEY,          -- from source CSV
    address_hash        TEXT NOT NULL UNIQUE,      -- SHA-256(normalized address); join key
    street              TEXT NOT NULL,
    city                TEXT NOT NULL,
    state               TEXT NOT NULL,
    zip                 TEXT,
    source_file         TEXT NOT NULL,             -- originating CSV filename (not path)
    uploaded_at         TEXT NOT NULL,             -- ISO-8601 UTC
    retention_purge_after TEXT NOT NULL            -- ISO-8601 UTC; compute at insert time
);

-- ── Geocoded records ──────────────────────────────────────────────────────────
-- Retained indefinitely. No raw address fields. Enables re-assignment after
-- redistricting without re-geocoding.
CREATE TABLE IF NOT EXISTS geocoded_records (
    address_hash        TEXT PRIMARY KEY,
    lat                 REAL,
    lng                 REAL,
    block_geoid         TEXT,                      -- 15-digit Census block GEOID
    geocoder_source     TEXT NOT NULL DEFAULT 'census',
    geocoder_benchmark  TEXT,
    geocoder_vintage    TEXT,
    match_score         TEXT,                      -- Census returns a string quality indicator
    match_type          TEXT,                      -- 'Exact', 'Non_Exact', 'Tie', 'No_Match'
    geocoded_at         TEXT NOT NULL              -- ISO-8601 UTC
);

CREATE INDEX IF NOT EXISTS idx_geocoded_geoid ON geocoded_records (block_geoid);

-- ── BEF version registry ──────────────────────────────────────────────────────
-- One row per downloaded BEF file. Never deleted. expiration_date NULL = current.
CREATE TABLE IF NOT EXISTS bef_versions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    bef_source_id       TEXT NOT NULL,             -- matches id in config/bef_sources.yaml
    district_type       TEXT NOT NULL,             -- CD, SD, AD, BOE
    label               TEXT NOT NULL,
    effective_date      TEXT NOT NULL,             -- ISO-8601 date
    expiration_date     TEXT,                      -- NULL = currently active
    source_url          TEXT NOT NULL,
    local_filename      TEXT NOT NULL,
    file_hash           TEXT NOT NULL,             -- SHA-256 of downloaded file
    downloaded_at       TEXT NOT NULL,             -- ISO-8601 UTC
    approved_by         TEXT,                      -- user who approved this version load
    approved_at         TEXT,
    notes               TEXT
);

CREATE INDEX IF NOT EXISTS idx_bef_versions_type_date
    ON bef_versions (district_type, effective_date);

-- ── BEF block-to-district mapping ─────────────────────────────────────────────
-- Populated from BEF CSV files. Keyed on (geoid, bef_version_id) for point-in-time
-- lookups. Never updated in place — new BEF version = new rows with new bef_version_id.
CREATE TABLE IF NOT EXISTS bef_blocks (
    geoid               TEXT NOT NULL,             -- 15-digit Census block GEOID
    district_type       TEXT NOT NULL,             -- CD, SD, AD, BOE
    district_number     TEXT NOT NULL,
    bef_version_id      INTEGER NOT NULL REFERENCES bef_versions (id),
    PRIMARY KEY (geoid, district_type, bef_version_id)
);

CREATE INDEX IF NOT EXISTS idx_bef_blocks_geoid ON bef_blocks (geoid, district_type);

-- ── District assignments ───────────────────────────────────────────────────────
-- One row per (address, district_type). No raw address. References bef_version used.
CREATE TABLE IF NOT EXISTS district_assignments (
    address_hash        TEXT NOT NULL,
    district_type       TEXT NOT NULL,             -- CD, SD, AD, BOE
    district_number     TEXT NOT NULL,
    bef_version_id      INTEGER NOT NULL REFERENCES bef_versions (id),
    assigned_at         TEXT NOT NULL,             -- ISO-8601 UTC
    PRIMARY KEY (address_hash, district_type, bef_version_id)
);

CREATE INDEX IF NOT EXISTS idx_assignments_district
    ON district_assignments (district_type, district_number);

-- ── Geocoding misses ──────────────────────────────────────────────────────────
-- Logged for audit and potential retry. No raw address — hash only.
CREATE TABLE IF NOT EXISTS geocode_misses (
    address_hash        TEXT PRIMARY KEY,
    reason              TEXT,                      -- Census match_type or error description
    attempted_at        TEXT NOT NULL,
    retry_eligible      INTEGER NOT NULL DEFAULT 1 -- boolean; set 0 after manual review
);

-- ── Schema version ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS schema_migrations (
    version             INTEGER PRIMARY KEY,
    applied_at          TEXT NOT NULL
);

INSERT INTO schema_migrations (version, applied_at)
VALUES (1, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'));
