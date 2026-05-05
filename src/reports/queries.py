import sqlite3
from datetime import datetime, timezone

from src.match.assigner import get_active_bef_version_id


def get_district_rollup(
    conn: sqlite3.Connection,
    district_types: list[str] | None = None,
    as_of_date: str | None = None,
) -> list[dict]:
    """
    Return constituent counts per district for each active BEF version.
    Rows: {district_type, district_number, constituent_count}.
    Ordered by district_type then district_number (numeric sort).
    """
    if district_types is None:
        district_types = ["CD", "SD", "AD", "BOE"]
    if as_of_date is None:
        as_of_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    version_ids = {
        dt: get_active_bef_version_id(conn, dt, as_of_date)
        for dt in district_types
    }
    active = {dt: vid for dt, vid in version_ids.items() if vid is not None}
    if not active:
        return []

    placeholders = ",".join("?" * len(active))
    rows = conn.execute(
        f"""
        SELECT
            da.district_type,
            da.district_number,
            COUNT(DISTINCT da.address_hash) AS constituent_count
        FROM district_assignments da
        WHERE da.bef_version_id IN ({placeholders})
        GROUP BY da.district_type, da.district_number
        ORDER BY da.district_type,
                 CAST(da.district_number AS INTEGER)
        """,
        list(active.values()),
    ).fetchall()

    return [dict(r) for r in rows]


def get_legislator_zip_breakdown(
    conn: sqlite3.Connection,
    district_type: str,
    district_number: str,
    bef_version_id: int,
) -> list[dict]:
    """
    Return ZIP-level constituent counts for one legislator's district.
    Rows: {zip, constituent_count}, ordered by constituent_count DESC.
    ZIPs absent from geocoded_records are grouped as 'unknown'.
    """
    rows = conn.execute(
        """
        SELECT
            COALESCE(NULLIF(gr.zip, ''), 'unknown') AS zip,
            COUNT(DISTINCT da.address_hash)          AS constituent_count
        FROM district_assignments da
        JOIN geocoded_records gr ON gr.address_hash = da.address_hash
        WHERE da.district_type   = ?
          AND da.district_number = ?
          AND da.bef_version_id  = ?
        GROUP BY COALESCE(NULLIF(gr.zip, ''), 'unknown')
        ORDER BY constituent_count DESC, zip
        """,
        (district_type, district_number, bef_version_id),
    ).fetchall()

    return [dict(r) for r in rows]


def get_methodology_lines(
    conn: sqlite3.Connection,
    district_types: list[str] | None = None,
    as_of_date: str | None = None,
) -> list[str]:
    """
    Return methodology footer lines suitable for appending to any report.
    One line per active district type describing the BEF source used.
    """
    if district_types is None:
        district_types = ["CD", "SD", "AD", "BOE"]
    if as_of_date is None:
        as_of_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    lines = ["Addresses geocoded via U.S. Census Geocoder (census.gov)."]

    for dt in district_types:
        vid = get_active_bef_version_id(conn, dt, as_of_date)
        if vid is None:
            continue
        row = conn.execute(
            "SELECT label, effective_date FROM bef_versions WHERE id = ?", (vid,)
        ).fetchone()
        if row:
            lines.append(
                f"District assignments ({dt}): California Statewide Database "
                f"Block Equivalency Files — {row['label']} (effective {row['effective_date']})."
            )

    return lines
