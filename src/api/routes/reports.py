import csv
import io
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from src.api.deps import get_db
from src.match.assigner import get_active_bef_version_id
from src.reports.queries import (
    get_district_rollup,
    get_legislator_zip_breakdown,
    get_methodology_lines,
)

router = APIRouter()

_VALID_DISTRICT_TYPES = frozenset({"CD", "SD", "AD", "BOE"})


@router.get("/reports/rollup")
def get_rollup(
    district_types: str | None = None,
    as_of_date: str | None = None,
    conn=Depends(get_db),
):
    """District rollup CSV: one row per district with constituent count."""
    types = _parse_district_types(district_types)
    rows = get_district_rollup(conn, types, as_of_date)
    footer = get_methodology_lines(conn, types, as_of_date)
    filename = f"district_rollup_{_today()}.csv"
    return _csv_response(
        filename=filename,
        fieldnames=["district_type", "district_number", "constituent_count"],
        rows=rows,
        footer=footer,
    )


@router.get("/reports/legislators")
def list_legislators(
    district_types: str | None = None,
    as_of_date: str | None = None,
    conn=Depends(get_db),
):
    """JSON list of available legislator reports with constituent counts."""
    types = _parse_district_types(district_types)
    return get_district_rollup(conn, types, as_of_date)


@router.get("/reports/legislators/{district_type}/{district_number}")
def get_legislator_report(
    district_type: str,
    district_number: str,
    as_of_date: str | None = None,
    conn=Depends(get_db),
):
    """Per-legislator CSV: constituent count + ZIP breakdown."""
    district_type = district_type.upper()
    if district_type not in _VALID_DISTRICT_TYPES:
        raise HTTPException(400, detail=f"Invalid district_type. Must be one of: {sorted(_VALID_DISTRICT_TYPES)}")

    version_id = get_active_bef_version_id(conn, district_type, as_of_date)
    if version_id is None:
        raise HTTPException(404, detail=f"No active BEF loaded for district type {district_type}")

    zip_rows = get_legislator_zip_breakdown(conn, district_type, district_number, version_id)
    if not zip_rows:
        raise HTTPException(404, detail=f"No data for {district_type} {district_number}")

    total = sum(r["constituent_count"] for r in zip_rows)
    footer = get_methodology_lines(conn, [district_type], as_of_date)
    footer.insert(0, f"Total constituents: {total}")

    rows = [
        {"district_type": district_type, "district_number": district_number, **r}
        for r in zip_rows
    ]
    filename = f"{district_type}_{district_number}_{_today()}.csv"
    return _csv_response(
        filename=filename,
        fieldnames=["district_type", "district_number", "zip", "constituent_count"],
        rows=rows,
        footer=footer,
    )


@router.get("/reports/legislators/{district_type}/{district_number}/stats")
def get_legislator_stats(
    district_type: str,
    district_number: str,
    as_of_date: str | None = None,
    conn=Depends(get_db),
):
    """JSON stats for a single legislator: total count + ZIP breakdown."""
    district_type = district_type.upper()
    if district_type not in _VALID_DISTRICT_TYPES:
        raise HTTPException(400, detail=f"Invalid district_type. Must be one of: {sorted(_VALID_DISTRICT_TYPES)}")

    version_id = get_active_bef_version_id(conn, district_type, as_of_date)
    if version_id is None:
        raise HTTPException(404, detail=f"No active BEF loaded for district type {district_type}")

    zip_rows = get_legislator_zip_breakdown(conn, district_type, district_number, version_id)
    if not zip_rows:
        raise HTTPException(404, detail=f"No data for {district_type} {district_number}")

    total = sum(r["constituent_count"] for r in zip_rows)
    return {
        "district_type": district_type,
        "district_number": district_number,
        "total": total,
        "zip_breakdown": [{"zip": r["zip"], "constituent_count": r["constituent_count"]} for r in zip_rows],
    }


def _csv_response(
    filename: str,
    fieldnames: list[str],
    rows: list[dict],
    footer: list[str],
) -> StreamingResponse:
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    buf.write("\n")
    buf.write(f"# Generated: {generated}\n")
    for line in footer:
        buf.write(f"# {line}\n")

    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _parse_district_types(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    types = [t.strip().upper() for t in raw.split(",") if t.strip()]
    invalid = set(types) - _VALID_DISTRICT_TYPES
    if invalid:
        raise HTTPException(400, detail=f"Invalid district_type(s): {sorted(invalid)}")
    return types or None


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")
