# cal-district-mapper

Batch pipeline: CSV of ~10k California addresses → legislative district assignments
for constituent reporting to CA legislators (CD, SD, AD, BOE).

## Hard constraints — read before touching any code

### PII — structural controls (primary)
- Raw addresses live in exactly ONE database table (`raw_addresses`). Everywhere else: `address_hash` or geocoded coordinates.
- Raw addresses MUST NOT appear in /reports/, /docs/, /logs/, or any committed file.
- Logs use hashed addresses only.
- Reports contain aggregate counts only (+ optional ZIP-level breakdown). Never individual records.
- `data/raw/` is gitignored. Only manifests and processed (de-PII'd) files are version-controlled.
- Raw addresses are purged 90 days after geocoding (configurable via `config/settings.yaml`). Geocoded records retained indefinitely.
- The `src/guards/` module enforces the no-raw-address-in-output rule — don't bypass it.

### PII — encryption at rest
- Encryption is handled at the OS/cloud level (FileVault on macOS, LUKS or KMS-backed volume in prod).
- The application does NOT manage encryption keys. It trusts the volume it writes to.
- Schema keeps raw address fields isolated so adding SQLCipher later is a one-table change, not a rewrite.

### Geocoding
- Only the U.S. Census Geocoder (census.gov) is approved. No commercial APIs (Google, Mapbox, Geocodio, etc.) without explicit approval.
- Batch endpoint: https://geocoding.geo.census.gov/geocoder/geographies/addressbatch
- `benchmark=Public_AR_Current`, `vintage=Current_Current`
- The batch endpoint returns block GEOIDs. District assignment comes from BEF join, not the geocoder.
- v1: Census only. ~10% miss rate acceptable. Log misses (hashed address only). No paid fallback.

### BEF (Block Equivalency Files)
- BEF source URLs live in `config/bef_sources.yaml` — never hardcode them anywhere in src/.
- Never delete a BEF version. Append new versions with `effective_date`; mark superseded versions with `expiration_date`. Keep both indefinitely.
- District lookups are point-in-time queries against the versioned BEF table.
- BEF refresh = hash-comparator with manual approval step — NOT a blind auto-downloader. Auto-replacing a BEF silently is a worse failure mode than missing an update.
- Verify URL reachability before each refresh attempt.
- Always save a local copy of every BEF version downloaded to `data/bef/`.

### CSV ingestion
- Required columns: `id`, `street`, `city`, `state`, `zip`.
- Reject malformed rows with a clear, specific error — never silently drop them.
- Expect messy real-world data: apartment numbers embedded in `street`, missing ZIPs, mixed case, extra whitespace.

## District scope (v1)
CD, SD, AD, BOE only. County-level districts (supervisorial, city council) are out of scope.

## Reports — required methodology footer
Every report must include:
> Addresses geocoded via U.S. Census Geocoder; district assignments from California Statewide Database Block Equivalency Files ([BEF name] effective [date]).

## BEF versions
See `config/bef_sources.yaml` for the canonical list with effective/expiration dates.
AB 604 congressional BEF is current (effective Jan 2026). The 2021 congressional BEF is superseded — store it, mark it, but do not use it for current-cycle reports.

## Stack
- Language: Python
- Database: SQLite (single-file, `data/district_mapper.db`, gitignored)
- No ORM — use `sqlite3` directly with parameterized queries
- Schema managed via versioned migration scripts in `db/migrations/`
