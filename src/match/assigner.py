import sqlite3
from datetime import datetime, timezone


def get_active_bef_version_id(
    conn: sqlite3.Connection,
    district_type: str,
    as_of_date: str | None = None,
) -> int | None:
    """
    Return the bef_version_id of the approved BEF version active on as_of_date.
    as_of_date: ISO-8601 date string (YYYY-MM-DD); defaults to today (UTC).
    Returns None if no version is loaded for this district type.
    """
    if as_of_date is None:
        as_of_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    row = conn.execute(
        """
        SELECT id FROM bef_versions
        WHERE district_type = ?
          AND effective_date <= ?
          AND (expiration_date IS NULL OR expiration_date >= ?)
          AND approved_by IS NOT NULL
        ORDER BY effective_date DESC
        LIMIT 1
        """,
        (district_type, as_of_date, as_of_date),
    ).fetchone()

    return row["id"] if row else None


def run_assignment(
    conn: sqlite3.Connection,
    district_types: list[str] | None = None,
    as_of_date: str | None = None,
) -> dict:
    """
    Bulk-join geocoded_records against bef_blocks for each district type and
    write new rows to district_assignments.

    Existing assignments are not overwritten (INSERT OR IGNORE).
    Returns a per-district-type summary dict.
    """
    if district_types is None:
        district_types = ["CD", "SD", "AD", "BOE"]

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    no_block_geoid = conn.execute(
        "SELECT COUNT(*) FROM geocoded_records WHERE block_geoid IS NULL"
    ).fetchone()[0]

    summary: dict = {}

    for district_type in district_types:
        version_id = get_active_bef_version_id(conn, district_type, as_of_date)
        if version_id is None:
            summary[district_type] = {"assigned": 0, "no_active_bef": True}
            continue

        cur = conn.execute(
            """
            INSERT OR IGNORE INTO district_assignments
                (address_hash, district_type, district_number, bef_version_id, assigned_at)
            SELECT gr.address_hash, ?, bb.district_number, ?, ?
            FROM geocoded_records gr
            JOIN bef_blocks bb
                ON  bb.geoid          = gr.block_geoid
                AND bb.district_type  = ?
                AND bb.bef_version_id = ?
            WHERE gr.block_geoid IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM district_assignments da
                  WHERE da.address_hash   = gr.address_hash
                    AND da.district_type  = ?
                    AND da.bef_version_id = ?
              )
            """,
            (district_type, version_id, now,
             district_type, version_id,
             district_type, version_id),
        )

        no_bef_match = conn.execute(
            """
            SELECT COUNT(DISTINCT gr.address_hash)
            FROM geocoded_records gr
            WHERE gr.block_geoid IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM bef_blocks bb
                  WHERE bb.geoid          = gr.block_geoid
                    AND bb.district_type  = ?
                    AND bb.bef_version_id = ?
              )
            """,
            (district_type, version_id),
        ).fetchone()[0]

        summary[district_type] = {
            "bef_version_id": version_id,
            "assigned": cur.rowcount,
            "no_bef_match": no_bef_match,
            "no_block_geoid": no_block_geoid,
        }

    return summary
