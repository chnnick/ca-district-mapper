import sqlite3

from fastapi import APIRouter, Depends

from src.api.deps import get_db
from src.match.assigner import get_active_bef_version_id

router = APIRouter()


@router.get("/map/points")
def get_map_points(
    district_type: str | None = None,
    district_number: str | None = None,
    conn: sqlite3.Connection = Depends(get_db),
) -> list[dict]:
    """
    Return geocoded lat/lng points.

    No params: all geocoded points (unfiltered, for initial map load).
    district_type + district_number: only constituents assigned to that district
    under the current active BEF version.
    """
    if district_type and district_number:
        district_type = district_type.upper()
        version_id = get_active_bef_version_id(conn, district_type)
        if version_id is None:
            return []
        rows = conn.execute(
            """
            SELECT DISTINCT gr.lat, gr.lng
            FROM geocoded_records gr
            JOIN district_assignments da ON da.address_hash = gr.address_hash
            WHERE gr.lat  IS NOT NULL
              AND gr.lng  IS NOT NULL
              AND da.district_type   = ?
              AND da.district_number = ?
              AND da.bef_version_id  = ?
            """,
            (district_type, district_number, version_id),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT lat, lng FROM geocoded_records WHERE lat IS NOT NULL AND lng IS NOT NULL"
        ).fetchall()

    return [{"lat": row["lat"], "lng": row["lng"]} for row in rows]
