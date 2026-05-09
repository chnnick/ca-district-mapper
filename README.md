# California District Mapper

Batch pipeline that maps California constituent addresses to state and federal legislative districts (CD, SD, AD, BOE) for use in constituent reporting to elected officials.

Upload a CSV of addresses → the pipeline geocodes them via the U.S. Census Geocoder → joins to California Statewide Database Block Equivalency Files (BEF) → produces aggregate constituent counts per district, exportable as CSV reports.

---

## Quick start

1. **Install [Docker Desktop](https://www.docker.com/products/docker-desktop/)** (one-time setup). Open it and wait for the whale icon in your menu bar / taskbar to stop animating.
2. **Download the latest release zip** from the [Releases page](../../releases/latest) and unzip it.
3. **Launch the app:**
   - **macOS:** Double-click `Launch California District Mapper.command`
   - **Windows:** Double-click `Launch California District Mapper.bat`

Your browser opens automatically at `http://localhost:8000` once the app is ready. The first launch downloads district data and may take a few minutes — this is normal.

To stop the app, double-click `Stop California District Mapper.command` (macOS) or `Stop California District Mapper.bat` (Windows).

Your uploaded CSVs, generated reports, and the database are stored in the `data/`, `logs/`, and `reports/` folders next to the launcher. They persist between restarts.

---

## How it works

```
CSV upload → ingest + normalize → Census Geocoder (batch) → block GEOID → BEF join → district assignments → reports / map
```

1. **Ingest** — CSV rows are validated, normalized (whitespace/case), SHA-256 hashed, and loaded into `raw_addresses`. Malformed rows are rejected with specific errors, never silently dropped.

> [!WARNING]
> The currently accepted row headers are: `street`, `city`, `state`, `zip`, `country`, `id`.

> [!WARNING]
> Remove all empty rows at bottom prior to submitting data

2. **Geocode** — Addresses are batched and sent to the [U.S. Census Geocoder](https://geocoding.geo.census.gov/geocoder/geographies/addressbatch). Matched records receive a 15-digit Census block GEOID. Misses are logged by hash only.
3. **Match** — Block GEOIDs are joined against the active BEF version to produce `district_assignments`. No raw addresses leave the `raw_addresses` table.
4. **Reports** — Aggregate constituent counts per district, with optional ZIP-level breakdown. Every report includes a methodology footer citing the geocoder and BEF version used.

---

## District scope

| Type | Name |
|------|------|
| CD   | U.S. Congressional Districts |
| SD   | California State Senate Districts |
| AD   | California State Assembly Districts |
| BOE  | CA Board of Equalization Districts |

County and municipal districts are out of scope for v1.

---

## Running with Docker (recommended)

Docker is the simplest way to run the app — no Python or Node.js install required.

**Prerequisites:** [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Mac or Windows)

### macOS

```bash
./scripts/launch.sh    # build image, start container, open browser
./scripts/stop.sh      # stop the container
./scripts/reset.sh     # wipe all data and stop (see Resetting below)
```

### Windows

Double-click the scripts or run them from Command Prompt:

```bat
launch.bat
stop.bat
reset.bat
```

The app runs at `http://localhost:8000`. Data is stored on your host machine under `data/`, `logs/`, and `reports/` — not inside the container — so it persists across restarts.

> [!WARNING]
> **The district analytics panel will show "No data" until BEF files are loaded.** The map displays geocoded points regardless, but constituent counts and district assignments require the BEF step below.

After first launch, load the BEF district data (required one-time setup — downloads ~4 ZIPs from CSDB, ~30 MB total):

```bash
docker compose exec app python scripts/load_bef.py --approved-by "Your Name"
```

This populates `data/bef/` and loads all active district lookup tables (CD, SD, AD, BOE). `--approved-by` is required — rows with NULL `approved_by` are silently ignored by the matcher. Re-run after a database reset to reload from already-downloaded files without re-downloading.

---

## Setup (development)

**Requirements:** Python 3.11+, Node.js 18+

```bash
# Python environment
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt   # for tests

# Frontend dependencies
cd frontend && npm install
```

---

## Running

**1. Backend** (applies migrations automatically on startup):

```bash
source .venv/bin/activate
uvicorn src.api.app:create_app --factory --reload --host 0.0.0.0 --port 8000
```

**2. Load BEF data** (one-time setup — downloads ~4 ZIPs from CSDB and populates district lookup tables):

```bash
python scripts/load_bef.py --approved-by "Your Name"
```

This downloads CD, SD, AD, and BOE Block Equivalency Files to `data/bef/` and loads them into the database. `--approved-by` is required — rows with NULL `approved_by` are silently ignored by the matcher. Already-downloaded ZIPs are reused on subsequent runs; already-loaded versions (same hash) are skipped. Pass `--include-superseded` to also load the historical 2021 CD BEF for point-in-time queries. Pass `--dry-run` to preview without writing anything. Under Docker, this also runs automatically in the background on first startup.

**3. Frontend dev server** (proxies `/api` to `localhost:8000`):

```bash
cd frontend && npm run dev
# open http://localhost:5173
```

**Production build** (FastAPI serves the built frontend at `/`):

```bash
cd frontend && npm run build
# Then run uvicorn — it picks up frontend/dist/ automatically
```

---

## API reference

All routes are under `/api`.

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/uploads` | Upload a CSV; returns `job_id` to poll |
| GET | `/api/jobs/{id}` | Job status and pipeline summaries |
| GET | `/api/jobs` | List recent jobs |
| GET | `/api/reports/rollup` | CSV: constituent counts per district |
| GET | `/api/reports/legislators` | JSON: all districts with counts |
| GET | `/api/reports/legislators/{type}/{number}` | CSV: ZIP breakdown for one legislator |
| GET | `/api/reports/legislators/{type}/{number}/stats` | JSON: same, for UI display |
| GET | `/api/map/points` | JSON: `[{lat, lng}]` for all geocoded records (or filtered by district) |

**Upload flow:**

```
POST /api/uploads          → 202 { job_id, ingest: { loaded, rejected, ... } }
GET  /api/jobs/{job_id}    → poll until status = "done" or "failed"
```

Jobs are serialized — only one may geocode/match at a time. A 409 is returned if a job is already running.

**Report query parameters:**

- `district_types` — comma-separated filter: `CD`, `SD`, `AD`, `BOE`
- `as_of_date` — ISO date (YYYY-MM-DD) for point-in-time BEF lookups

---

## BEF management

Block Equivalency Files define which Census blocks belong to each district. They change after redistricting.

- BEF source URLs and version metadata live in `config/bef_sources.yaml`. URLs are never hardcoded in `src/`.
- Versions are **never deleted**. New versions are appended with an `effective_date`; superseded versions get an `expiration_date`.
- District lookups are point-in-time — pass `as_of_date` to query against a historical BEF.
- Loading a new BEF requires a hash comparison and `approved_by` field — no silent auto-replacement.
- Every downloaded BEF ZIP is saved to `data/bef/` and hashed (SHA-256) before loading.

Current BEF versions:

| ID | District type | Effective | Status |
|----|--------------|-----------|--------|
| `ab604_cd` | CD | 2026-01-01 | Active |
| `2021_cd` | CD | 2021-01-01 | Superseded 2025-12-31 |
| `2021_sd` | SD | 2021-01-01 | Active |
| `2021_ad` | AD | 2021-01-01 | Active |
| `2021_boe` | BOE | 2021-01-01 | Active |

---

## CSV format

Required columns (order-independent, case-insensitive):

```
id, street, city, state, zip
```

- Extra columns are ignored.
- Rows with missing `id`, `street`, or `state` are rejected with a specific error.
- Missing `zip` is accepted; those records may not geocode.
- Apartment numbers, mixed case, and extra whitespace are handled automatically.

---

## Resetting

To wipe all data and start fresh (clears the database, uploaded CSVs, logs, and reports):

**Docker (macOS):**
```bash
./scripts/reset.sh
```

**Docker (Windows):**
```bat
reset.bat
```

**Without Docker:**
```bash
# Stop the backend server first, then:
rm -f data/district_mapper.db data/district_mapper.db-shm data/district_mapper.db-wal
find data/raw data/processed -type f ! -name '.gitkeep' -delete
```

In all cases, BEF files in `data/bef/` are preserved — re-run the loader to repopulate the BEF tables without re-downloading:

```bash
# Docker
docker compose exec app python scripts/load_bef.py --approved-by "Your Name"

# Without Docker
python scripts/load_bef.py --approved-by "Your Name"
```

**Stuck job (409 on upload):** If the server was killed mid-job, a job row may be left in `geocoding` or `matching` status. The quickest fix is a full reset above. To clear only the stuck job without losing other data:

```bash
sqlite3 data/district_mapper.db \
  "UPDATE jobs SET status='failed', error='interrupted', finished_at=datetime('now') \
   WHERE status IN ('geocoding','matching');"
```

---

## Tests

```bash
pytest tests/
```

139 tests covering ingest, geocoder parsing, BEF loading, district assignment, report generation, and all API endpoints.

---

## Stack

- **Backend** — Python, FastAPI, SQLite (`data/district_mapper.db`)
- **Frontend** — React 18, TypeScript (strict), Vite, react-leaflet
- **Geocoder** — U.S. Census Geocoder only (no commercial APIs)
- **District data** — California Statewide Database BEF (Block Equivalency Files)
- **No ORM** — raw `sqlite3` with parameterized queries throughout

---

## Project layout

```
cal-district-mapper/
├── src/
│   ├── api/               # FastAPI app, routes, deps
│   │   └── routes/        # uploads, jobs, reports, map
│   ├── ingest/            # CSV validation, normalization, loader
│   ├── geocode/           # Census API client, response parser, runner
│   ├── match/             # BEF config, loader, district assigner
│   ├── reports/           # Query functions and CSV writers
│   ├── guards/            # PII enforcement (blocks address data in output)
│   └── db.py              # Connection factory + migration runner
├── frontend/
│   └── src/
│       ├── api/client.ts  # Typed fetch wrappers
│       ├── components/    # UploadPanel, DistrictList, StatsPanel, MapView
│       ├── types.ts        # Shared TypeScript types
│       └── App.tsx
├── db/
│   └── migrations/        # Versioned SQL migration scripts
├── config/
│   ├── bef_sources.yaml   # BEF source URLs and version metadata
│   └── settings.yaml      # Retention period, batch sizes (optional)
├── data/
│   ├── raw/               # Uploaded CSVs (gitignored)
│   └── bef/               # Downloaded BEF ZIP archives (gitignored)
├── tests/
└── reports/               # Generated CSV reports (gitignored)
```

---

## Database schema

| Table | Contents |
|-------|----------|
| `raw_addresses` | Raw PII — street, city, state, zip. Purged 90 days after geocoding. |
| `geocoded_records` | lat, lng, block_geoid, zip. Retained indefinitely. Join key: `address_hash`. |
| `bef_versions` | BEF version registry. Append-only; versions are never deleted. |
| `bef_blocks` | GEOID → district number mapping, versioned by `bef_version_id`. |
| `district_assignments` | Final mapping of `address_hash` to district. No raw addresses. |
| `geocode_misses` | Hashed-address audit log for Census non-matches. |
| `jobs` | Pipeline job tracking with JSON summaries per stage. |

Schema is managed via numbered migration scripts in `db/migrations/`. New migrations are applied automatically on startup.

---

## PII controls

Raw addresses are treated as PII and are structurally isolated:

- **One table only** — `raw_addresses` is the only place raw address fields (street, city, state, zip) are stored.
- **Hash-based joins** — every other table references `address_hash` (SHA-256 of normalized `street|city|state|zip`).
- **No addresses in output** — `src/guards/pii_guard.py` raises an error if CSV output to `reports/`, `logs/`, or `docs/` contains address-like column names.
- **90-day purge** — `retention_purge_after` is set on each raw address row; a purge job removes raw address fields after geocoding is complete.
- **Hashed logs** — geocode misses and audit events are logged by hash only.
- **Encryption at rest** — handled at the OS/volume level (FileVault on macOS, LUKS/KMS in production). The application does not manage encryption keys.
