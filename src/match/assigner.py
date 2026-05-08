import logging
import sqlite3
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


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

    Returns a summary dict:
        {
            "assigned":  int,   # distinct address_hashes with ≥1 assignment after this run
            "by_type":   { "<district_type>": { "assigned": int, ... }, ... },
        }
    """
    if district_types is None:
        district_types = ["CD", "SD", "AD", "BOE"]

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    no_block_geoid = conn.execute(
        "SELECT COUNT(*) FROM geocoded_records WHERE block_geoid IS NULL"
    ).fetchone()[0]

    by_type: dict = {}

    for district_type in district_types:
        version_id = get_active_bef_version_id(conn, district_type, as_of_date)
        if version_id is None:
            logger.warning("run_assignment: no active BEF for %s (as_of=%s) — skipping", district_type, as_of_date)
            by_type[district_type] = {"assigned": 0, "no_active_bef": True}
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

        logger.info(
            "run_assignment: %s  version_id=%d  assigned=%d  no_bef_match=%d  no_block_geoid=%d",
            district_type, version_id, cur.rowcount, no_bef_match, no_block_geoid,
        )
        if cur.rowcount == 0:
            logger.warning(
                "run_assignment: 0 rows assigned for %s (version_id=%d) — "
                "check that geocoded_records has block_geoids and bef_blocks has matching geoids",
                district_type, version_id,
            )
        by_type[district_type] = {
            "bef_version_id": version_id,
            "assigned": cur.rowcount,
            "no_bef_match": no_bef_match,
            "no_block_geoid": no_block_geoid,
        }

    # Count unique addresses with any assignment. An address can have up to 4 rows
    # (one per district type), so DISTINCT collapses them so each address counts once.
    # fetchone() grabs the single result row; [0] pulls the count out of it.
    total_assigned = conn.execute(
        "SELECT COUNT(DISTINCT address_hash) FROM district_assignments"
    ).fetchone()[0]

    return {"assigned": total_assigned, "by_type": by_type}
