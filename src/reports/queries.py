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


def get_districts_for_person(
    conn: sqlite3.Connection,
    person_id: str,
    as_of_date: str | None = None,
) -> dict | None:
    """
    Look up the four district assignments (CD, SD, AD, BOE) for a single person
    by their source-CSV id.

    Returns:
      None                                          — id not found in raw_addresses
      {id, status: "not_geocoded"}                  — id known but no geocode yet
      {id, lat, lng, status: "ok",       districts} — all four districts resolved
      {id, lat, lng, status: "partial",  districts} — some district types missing
        (e.g. only some BEFs are loaded; districts contains only the types we have)

    Never returns raw address fields (street/city/state/zip).
    """
    if as_of_date is None:
        as_of_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    row = conn.execute(
        """
        SELECT ra.id, ra.address_hash, gr.lat, gr.lng, gr.block_geoid
        FROM raw_addresses ra
        LEFT JOIN geocoded_records gr ON gr.address_hash = ra.address_hash
        WHERE ra.id = ?
        """,
        (person_id,),
    ).fetchone()
    if row is None:
        return None
    if row["block_geoid"] is None or row["lat"] is None:
        return {"id": row["id"], "status": "not_geocoded"}

    types = ["CD", "SD", "AD", "BOE"]
    active = {
        dt: vid
        for dt in types
        if (vid := get_active_bef_version_id(conn, dt, as_of_date)) is not None
    }
    districts: dict[str, str] = {}
    if active:
        placeholders = ",".join("?" * len(active))
        assign_rows = conn.execute(
            f"""
            SELECT district_type, district_number
            FROM district_assignments
            WHERE address_hash = ?
              AND bef_version_id IN ({placeholders})
            """,
            [row["address_hash"], *active.values()],
        ).fetchall()
        districts = {r["district_type"]: r["district_number"] for r in assign_rows}

    status = "ok" if len(districts) == 4 else "partial"
    return {
        "id": row["id"],
        "lat": row["lat"],
        "lng": row["lng"],
        "status": status,
        "districts": districts,
    }


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
