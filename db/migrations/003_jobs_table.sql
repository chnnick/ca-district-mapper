-- Migration 003: job tracking table for async pipeline runs
--
-- status lifecycle: geocoding → matching → done | failed
-- ingest_summary, geocode_summary, match_summary are JSON blobs.
-- Jobs are serialized — only one may be in geocoding/matching at a time.

CREATE TABLE IF NOT EXISTS jobs (
    id              TEXT PRIMARY KEY,
    status          TEXT NOT NULL,   -- geocoding | matching | done | failed
    source_file     TEXT NOT NULL,
    ingest_summary  TEXT,
    geocode_summary TEXT,
    match_summary   TEXT,
    error           TEXT,
    created_at      TEXT NOT NULL,
    started_at      TEXT,
    finished_at     TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status);

INSERT INTO schema_migrations (version, applied_at)
VALUES (3, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'));
