import csv
from pathlib import Path

import pytest

from src.guards.pii_guard import check_csv_columns
from src.reports.queries import get_district_rollup, get_legislator_zip_breakdown, get_methodology_lines
from src.reports.writer import write_all_legislator_reports, write_legislator_report, write_rollup_report


# ── pii_guard ─────────────────────────────────────────────────────────────────

def test_guard_allows_aggregate_columns_in_reports(tmp_path):
    path = tmp_path / "reports" / "out.csv"
    check_csv_columns(["district_type", "district_number", "constituent_count"], path)


def test_guard_blocks_street_column_in_reports(tmp_path):
    path = tmp_path / "reports" / "out.csv"
    with pytest.raises(ValueError, match="street"):
        check_csv_columns(["id", "street", "constituent_count"], path)


def test_guard_blocks_name_column_in_logs(tmp_path):
    path = tmp_path / "logs" / "audit.csv"
    with pytest.raises(ValueError, match="name"):
        check_csv_columns(["address_hash", "name", "district"], path)


def test_guard_allows_pii_columns_outside_protected_dirs(tmp_path):
    path = tmp_path / "data" / "staging.csv"
    check_csv_columns(["id", "street", "name"], path)


def test_guard_is_case_insensitive_on_columns(tmp_path):
    path = tmp_path / "reports" / "out.csv"
    with pytest.raises(ValueError):
        check_csv_columns(["STREET", "City"], path)


# ── queries ───────────────────────────────────────────────────────────────────

def test_get_district_rollup_returns_rows(db_with_assignments):
    rollup = get_district_rollup(db_with_assignments, district_types=["CD"])
    assert len(rollup) == 3  # CD 11, 13, 34


def test_get_district_rollup_correct_counts(db_with_assignments):
    rollup = get_district_rollup(db_with_assignments, district_types=["CD"])
    by_district = {r["district_number"]: r["constituent_count"] for r in rollup}
    assert by_district["13"] == 2   # hash_aaa and hash_ddd both in CD 13
    assert by_district["11"] == 1
    assert by_district["34"] == 1


def test_get_district_rollup_empty_when_no_bef(db_with_assignments):
    rollup = get_district_rollup(db_with_assignments, district_types=["SD"])
    assert rollup == []


def test_get_legislator_zip_breakdown(db_with_assignments):
    from src.match.assigner import get_active_bef_version_id
    vid = get_active_bef_version_id(db_with_assignments, "CD")
    rows = get_legislator_zip_breakdown(db_with_assignments, "CD", "13", vid)

    assert len(rows) == 1         # both CD-13 addresses have zip 94601
    assert rows[0]["zip"] == "94601"
    assert rows[0]["constituent_count"] == 2


def test_get_legislator_zip_breakdown_groups_unknown_zip(db_with_assignments):
    from src.match.assigner import get_active_bef_version_id
    # Insert a geocoded record with no zip for CD 13
    db_with_assignments.execute(
        "INSERT INTO geocoded_records (address_hash, lat, lng, block_geoid, zip, geocoder_source, geocoder_benchmark, geocoder_vintage, match_score, match_type, geocoded_at) VALUES ('hash_zzz', 37.80, -122.27, '060014001001003', NULL, 'census', 'Public_AR_Current', 'Current_Current', 'Match', 'Exact', '2026-04-30T00:00:00Z')"
    )
    db_with_assignments.execute(
        "INSERT INTO bef_blocks (geoid, district_type, district_number, bef_version_id) VALUES ('060014001001003', 'CD', '13', 1)"
    )
    vid = get_active_bef_version_id(db_with_assignments, "CD")
    db_with_assignments.execute(
        "INSERT INTO district_assignments (address_hash, district_type, district_number, bef_version_id, assigned_at) VALUES ('hash_zzz', 'CD', '13', ?, '2026-04-30T00:00:00Z')",
        (vid,),
    )
    db_with_assignments.commit()

    rows = get_legislator_zip_breakdown(db_with_assignments, "CD", "13", vid)
    zips = {r["zip"] for r in rows}
    assert "unknown" in zips


def test_get_methodology_lines_contains_geocoder(db_with_assignments):
    lines = get_methodology_lines(db_with_assignments, ["CD"])
    assert any("Census Geocoder" in l for l in lines)


def test_get_methodology_lines_contains_bef_label(db_with_assignments):
    lines = get_methodology_lines(db_with_assignments, ["CD"])
    assert any("Test CD" in l for l in lines)


def test_get_methodology_lines_omits_missing_district_types(db_with_assignments):
    lines = get_methodology_lines(db_with_assignments, ["SD"])  # no SD BEF loaded
    assert not any("SD" in l for l in lines[1:])  # first line is geocoder


# ── writer ────────────────────────────────────────────────────────────────────

def test_write_rollup_report_creates_file(tmp_path, db_with_assignments):
    path = tmp_path / "reports" / "rollup.csv"
    result = write_rollup_report(db_with_assignments, path, district_types=["CD"])
    assert result == path
    assert path.exists()


def test_write_rollup_report_correct_data(tmp_path, db_with_assignments):
    path = tmp_path / "reports" / "rollup.csv"
    write_rollup_report(db_with_assignments, path, district_types=["CD"])

    rows = list(csv.DictReader(open(path)))
    districts = {r["district_number"] for r in rows if not r["district_type"].startswith("#")}
    assert "11" in districts
    assert "13" in districts
    assert "34" in districts


def test_write_rollup_report_has_methodology_footer(tmp_path, db_with_assignments):
    path = tmp_path / "reports" / "rollup.csv"
    write_rollup_report(db_with_assignments, path, district_types=["CD"])
    content = path.read_text()
    assert "Census Geocoder" in content
    assert "Generated:" in content


def test_write_rollup_report_no_pii_in_output(tmp_path, db_with_assignments):
    path = tmp_path / "reports" / "rollup.csv"
    write_rollup_report(db_with_assignments, path, district_types=["CD"])
    content = path.read_text().lower()
    # Raw address fields must not appear
    for field in ("123 main", "456 oak", "789 elm", "321 pine"):
        assert field not in content


def test_write_legislator_report_creates_file(tmp_path, db_with_assignments):
    path = tmp_path / "reports" / "CD_13.csv"
    result = write_legislator_report(db_with_assignments, "CD", "13", path)
    assert result == path
    assert path.exists()


def test_write_legislator_report_zip_breakdown(tmp_path, db_with_assignments):
    path = tmp_path / "reports" / "CD_13.csv"
    write_legislator_report(db_with_assignments, "CD", "13", path)

    rows = [r for r in csv.DictReader(open(path)) if not r["district_type"].startswith("#")]
    assert len(rows) == 1
    assert rows[0]["zip"] == "94601"
    assert rows[0]["constituent_count"] == "2"


def test_write_legislator_report_total_in_footer(tmp_path, db_with_assignments):
    path = tmp_path / "reports" / "CD_13.csv"
    write_legislator_report(db_with_assignments, "CD", "13", path)
    content = path.read_text()
    assert "Total constituents: 2" in content


def test_write_all_legislator_reports(tmp_path, db_with_assignments):
    out_dir = tmp_path / "reports"
    paths = write_all_legislator_reports(db_with_assignments, out_dir, district_types=["CD"])
    assert len(paths) == 3  # CD 11, 13, 34
    for p in paths:
        assert p.exists()


def test_write_rollup_blocks_pii_columns(tmp_path, db_with_assignments):
    """Guard should fire if a caller somehow passes PII column names."""
    from src.guards.pii_guard import check_csv_columns
    path = tmp_path / "reports" / "bad.csv"
    with pytest.raises(ValueError, match="street"):
        check_csv_columns(["district_type", "street", "constituent_count"], path)
