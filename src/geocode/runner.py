import sqlite3
from datetime import datetime, timezone

from .client import BatchRecord, geocode_batch
from .parser import parse_response

DEFAULT_BATCH_SIZE = 1_000


def run_geocoding(
    conn: sqlite3.Connection,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict:
    """
    Fetch ungeocoded addresses from raw_addresses, send to Census Geocoder in
    batches, and write results to geocoded_records and geocode_misses.

    Addresses already in geocoded_records are skipped. Addresses in
    geocode_misses with retry_eligible=0 are skipped.

    Returns a summary dict: {batches, geocoded, misses, errors}.
    """
    summary = {"batches": 0, "geocoded": 0, "misses": 0, "errors": 0}

    pending = _fetch_pending(conn)
    if not pending:
        return summary

    for offset in range(0, len(pending), batch_size):
        chunk = pending[offset : offset + batch_size]
        _process_batch(conn, chunk, summary)
        summary["batches"] += 1

    return summary


def _process_batch(conn: sqlite3.Connection, chunk: list, summary: dict) -> None:
    # Use sequential string IDs for Census; keep mapping back to (address_hash, zip)
    id_to_row = {str(i): row for i, row in enumerate(chunk)}

    records = [
        BatchRecord(
            census_id=str(i),
            street=row["street"],
            city=row["city"],
            state=row["state"],
            zip=row["zip"] or "",
        )
        for i, row in enumerate(chunk)
    ]

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        response_text = geocode_batch(records)
    except Exception as exc:
        # Entire batch failed — log all as misses (hash only, no raw address)
        _insert_misses(conn, [r["address_hash"] for r in chunk], f"batch_error: {exc}", now)
        summary["errors"] += len(chunk)
        return

    results = parse_response(response_text)
    responded_ids: set[str] = set()

    for result in results:
        source_row = id_to_row.get(result.census_id)
        if source_row is None:
            continue
        responded_ids.add(result.census_id)

        if result.is_match:
            _insert_geocoded(conn, source_row["address_hash"], source_row["zip"], result, now)
            summary["geocoded"] += 1
        else:
            reason = result.match + (f" ({result.match_type})" if result.match_type else "")
            _insert_miss(conn, source_row["address_hash"], reason, now)
            summary["misses"] += 1

    # Log any addresses Census silently dropped (no response row at all)
    silent_drops = [
        id_to_row[cid]["address_hash"] for cid in id_to_row if cid not in responded_ids
    ]
    if silent_drops:
        _insert_misses(conn, silent_drops, "No_Response", now)
        summary["misses"] += len(silent_drops)


def _fetch_pending(conn: sqlite3.Connection) -> list:
    return conn.execute(
        """
        SELECT ra.address_hash, ra.street, ra.city, ra.state, ra.zip
        FROM raw_addresses ra
        WHERE ra.address_hash NOT IN (SELECT address_hash FROM geocoded_records)
          AND ra.address_hash NOT IN (
              SELECT address_hash FROM geocode_misses WHERE retry_eligible = 0
          )
        GROUP BY ra.address_hash
        ORDER BY ra.address_hash
        """
    ).fetchall()


def _insert_geocoded(
    conn: sqlite3.Connection,
    address_hash: str,
    zip_code: str | None,
    result,
    geocoded_at: str,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO geocoded_records
            (address_hash, lat, lng, block_geoid, zip,
             geocoder_source, geocoder_benchmark, geocoder_vintage,
             match_score, match_type, geocoded_at)
        VALUES (?, ?, ?, ?, ?, 'census', 'Public_AR_Current', 'Current_Current', ?, ?, ?)
        """,
        (
            address_hash,
            result.lat,
            result.lng,
            result.block_geoid,
            zip_code or None,
            result.match,
            result.match_type,
            geocoded_at,
        ),
    )


def _insert_miss(
    conn: sqlite3.Connection,
    address_hash: str,
    reason: str,
    attempted_at: str,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO geocode_misses
            (address_hash, reason, attempted_at, retry_eligible)
        VALUES (?, ?, ?, 1)
        """,
        (address_hash, reason, attempted_at),
    )


def _insert_misses(
    conn: sqlite3.Connection,
    address_hashes: list[str],
    reason: str,
    attempted_at: str,
) -> None:
    conn.executemany(
        """
        INSERT OR REPLACE INTO geocode_misses
            (address_hash, reason, attempted_at, retry_eligible)
        VALUES (?, ?, ?, 1)
        """,
        [(h, reason, attempted_at) for h in address_hashes],
    )
