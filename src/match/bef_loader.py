import csv
import hashlib
import io
import sqlite3
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests

from .bef_config import BefSource

_CHUNK_SIZE = 8_192
_BATCH_SIZE = 10_000


def verify_url_reachable(url: str, timeout: int = 10) -> bool:
    """HEAD request to confirm URL is reachable. Returns False on any error."""
    try:
        resp = requests.head(url, timeout=timeout, allow_redirects=True)
        return resp.status_code < 400
    except requests.exceptions.RequestException:
        return False


def download_bef(url: str, local_path: Path, timeout: int = 120) -> str:
    """
    Stream-download a BEF ZIP to local_path. Returns SHA-256 hex digest.
    Raises on HTTP errors or connection failures.
    """
    local_path.parent.mkdir(parents=True, exist_ok=True)
    h = hashlib.sha256()
    resp = requests.get(url, stream=True, timeout=timeout)
    resp.raise_for_status()
    with open(local_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=_CHUNK_SIZE):
            f.write(chunk)
            h.update(chunk)
    return h.hexdigest()


def hash_file(path: Path) -> str:
    """SHA-256 of a local file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(_CHUNK_SIZE), b""):
            h.update(chunk)
    return h.hexdigest()


def get_current_file_hash(conn: sqlite3.Connection, bef_source_id: str) -> str | None:
    """Return the file_hash of the most recently loaded version for this source, or None."""
    row = conn.execute(
        """
        SELECT file_hash FROM bef_versions
        WHERE bef_source_id = ?
        ORDER BY downloaded_at DESC LIMIT 1
        """,
        (bef_source_id,),
    ).fetchone()
    return row["file_hash"] if row else None


def load_bef_version(
    conn: sqlite3.Connection,
    source: BefSource,
    file_path: Path,
    file_hash: str,
    approved_by: str,
) -> tuple[int, int]:
    """
    Extract BEF CSV from ZIP, bulk-load block rows into bef_blocks, register the
    version in bef_versions. Returns (bef_version_id, block_count).

    Raises ValueError if:
    - This file_hash is already loaded for this source (duplicate guard).
    - The expected geoid_column or district_column are not found in any CSV in the ZIP.
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    existing = conn.execute(
        "SELECT id FROM bef_versions WHERE bef_source_id = ? AND file_hash = ?",
        (source.id, file_hash),
    ).fetchone()
    if existing:
        raise ValueError(
            f"BEF {source.id!r} with hash {file_hash[:12]}... already loaded "
            f"(version id={existing['id']})"
        )

    cur = conn.execute(
        """
        INSERT INTO bef_versions
            (bef_source_id, district_type, label, effective_date, expiration_date,
             source_url, local_filename, file_hash, downloaded_at,
             approved_by, approved_at, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source.id,
            source.district_type,
            source.label,
            source.effective_date,
            source.expiration_date,
            source.url,
            source.local_filename,
            file_hash,
            now,
            approved_by,
            now,
            source.notes,
        ),
    )
    version_id = cur.lastrowid
    block_count = _load_blocks_from_zip(conn, file_path, source, version_id)
    return version_id, block_count


def _load_blocks_from_zip(
    conn: sqlite3.Connection,
    zip_path: Path,
    source: BefSource,
    version_id: int,
) -> int:
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise ValueError(f"No CSV files found in {zip_path.name}")

        if source.has_header:
            target = _find_target_csv_with_header(zf, csv_names, source.geoid_column, source.district_column)
        else:
            target = _find_single_csv(zf, csv_names, zip_path.name)

        with zf.open(target) as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8-sig")
            batch: list[tuple] = []
            total = 0

            if source.has_header:
                reader = csv.DictReader(text)
                headers = reader.fieldnames or []
                for col in (source.geoid_column, source.district_column):
                    if col not in headers:
                        raise ValueError(
                            f"Column {col!r} not found in {target}. "
                            f"Available columns: {headers}"
                        )
                rows = ((r[source.geoid_column].strip(), r[source.district_column].strip()) for r in reader)
            else:
                # Headerless: col 0 = GEOID, col 1 = district number.
                rows = ((r[0].strip(), r[1].strip()) for r in csv.reader(text) if len(r) >= 2)

            for geoid, district in rows:
                if not geoid or not district:
                    continue
                batch.append((geoid, source.district_type, district, version_id))
                if len(batch) >= _BATCH_SIZE:
                    conn.executemany(
                        "INSERT OR IGNORE INTO bef_blocks "
                        "(geoid, district_type, district_number, bef_version_id) "
                        "VALUES (?, ?, ?, ?)",
                        batch,
                    )
                    total += len(batch)
                    batch = []

            if batch:
                conn.executemany(
                    "INSERT OR IGNORE INTO bef_blocks "
                    "(geoid, district_type, district_number, bef_version_id) "
                    "VALUES (?, ?, ?, ?)",
                    batch,
                )
                total += len(batch)

    return total


def _find_single_csv(zf: zipfile.ZipFile, csv_names: list[str], zip_name: str) -> str:
    """Return the sole CSV in the ZIP, or raise if there are multiple."""
    if len(csv_names) == 1:
        return csv_names[0]
    raise ValueError(
        f"Multiple CSVs in {zip_name} and has_header=false — cannot auto-select: {csv_names}. "
        f"Inspect the ZIP and update bef_sources.yaml."
    )


def _find_target_csv_with_header(
    zf: zipfile.ZipFile,
    csv_names: list[str],
    geoid_column: str,
    district_column: str,
) -> str:
    """Return the CSV in the ZIP that contains both required header columns."""
    candidates = []
    for name in csv_names:
        with zf.open(name) as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8-sig")
            reader = csv.reader(text)
            try:
                headers = [h.strip() for h in next(reader)]
            except StopIteration:
                continue
            if geoid_column in headers and district_column in headers:
                candidates.append(name)

    if len(candidates) == 1:
        return candidates[0]
    if not candidates:
        raise ValueError(
            f"No CSV in ZIP contains both {geoid_column!r} and {district_column!r}. "
            f"CSV files present: {csv_names}. "
            f"Check geoid_column and district_column in config/bef_sources.yaml."
        )
    raise ValueError(
        f"Multiple CSVs contain both required columns; cannot auto-select: {candidates}. "
        f"Inspect the ZIP and update bef_sources.yaml."
    )
