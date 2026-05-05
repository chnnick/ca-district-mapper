import csv
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from src.guards.pii_guard import check_csv_columns
from src.match.assigner import get_active_bef_version_id
from src.reports.queries import (
    get_district_rollup,
    get_legislator_zip_breakdown,
    get_methodology_lines,
)

_ROLLUP_COLUMNS = ["district_type", "district_number", "constituent_count"]
_LEGISLATOR_COLUMNS = ["district_type", "district_number", "zip", "constituent_count"]


def write_rollup_report(
    conn: sqlite3.Connection,
    output_path: Path,
    district_types: list[str] | None = None,
    as_of_date: str | None = None,
) -> Path:
    """
    Write a district rollup CSV (all district types, aggregate counts only).
    Appends methodology footer after the data rows.
    Returns the output path.
    """
    output_path = Path(output_path)
    check_csv_columns(_ROLLUP_COLUMNS, output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rollup = get_district_rollup(conn, district_types, as_of_date)
    methodology = get_methodology_lines(conn, district_types, as_of_date)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_ROLLUP_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rollup)
        _write_methodology_footer(f, methodology)

    return output_path


def write_legislator_report(
    conn: sqlite3.Connection,
    district_type: str,
    district_number: str,
    output_path: Path,
    as_of_date: str | None = None,
) -> Path:
    """
    Write a per-legislator CSV: total constituent count + ZIP breakdown.
    Appends methodology footer. Returns the output path.
    """
    output_path = Path(output_path)
    check_csv_columns(_LEGISLATOR_COLUMNS, output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    bef_version_id = get_active_bef_version_id(conn, district_type, as_of_date)
    zip_rows = (
        get_legislator_zip_breakdown(conn, district_type, district_number, bef_version_id)
        if bef_version_id
        else []
    )
    total = sum(r["constituent_count"] for r in zip_rows)
    methodology = get_methodology_lines(conn, [district_type], as_of_date)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_LEGISLATOR_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in zip_rows:
            writer.writerow({
                "district_type":     district_type,
                "district_number":   district_number,
                "zip":               row["zip"],
                "constituent_count": row["constituent_count"],
            })
        _write_methodology_footer(f, methodology, total=total)

    return output_path


def write_all_legislator_reports(
    conn: sqlite3.Connection,
    output_dir: Path,
    district_types: list[str] | None = None,
    as_of_date: str | None = None,
) -> list[Path]:
    """
    Write one CSV per legislator per district type. Returns list of paths written.
    """
    if district_types is None:
        district_types = ["CD", "SD", "AD", "BOE"]

    output_dir = Path(output_dir)
    rollup = get_district_rollup(conn, district_types, as_of_date)
    written: list[Path] = []

    for row in rollup:
        dt = row["district_type"]
        dn = row["district_number"]
        path = output_dir / f"{dt}_{dn}.csv"
        write_legislator_report(conn, dt, dn, path, as_of_date)
        written.append(path)

    return written


def _write_methodology_footer(
    f,
    methodology_lines: list[str],
    total: int | None = None,
) -> None:
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    f.write("\n")
    if total is not None:
        f.write(f"# Total constituents: {total}\n")
    f.write(f"# Generated: {generated}\n")
    for line in methodology_lines:
        f.write(f"# {line}\n")
