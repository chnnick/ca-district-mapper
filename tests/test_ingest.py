import textwrap
from pathlib import Path

import pytest

from src.ingest import IngestResult, load_csv
from src.ingest.normalize import address_hash, normalize_row
from src.ingest.validate import validate_schema


def write_csv(tmp_path, name: str, content: str) -> Path:
    p = tmp_path / name
    p.write_text(textwrap.dedent(content).lstrip())
    return p


# ── validate_schema ───────────────────────────────────────────────────────────

def test_validate_schema_passes_with_all_columns():
    validate_schema(["id", "street", "city", "state", "zip"])


def test_validate_schema_passes_with_extra_columns():
    validate_schema(["id", "street", "city", "state", "zip", "email", "name"])


def test_validate_schema_raises_on_missing_column():
    with pytest.raises(ValueError, match="missing required columns"):
        validate_schema(["id", "street", "city", "state"])  # no zip


def test_validate_schema_raises_listing_all_missing():
    with pytest.raises(ValueError) as exc_info:
        validate_schema(["name", "email"])
    msg = str(exc_info.value)
    assert "city" in msg
    assert "id" in msg
    assert "state" in msg
    assert "street" in msg
    assert "zip" in msg


def test_validate_schema_is_case_insensitive():
    validate_schema(["ID", "Street", "CITY", "State", "ZIP"])


# ── normalize_row ─────────────────────────────────────────────────────────────

def test_normalize_strips_and_uppercases():
    row = {"id": "1", "street": "  123 main st  ", "city": "los angeles", "state": "ca", "zip": "90001"}
    n = normalize_row(row)
    assert n["street"] == "123 MAIN ST"
    assert n["city"] == "LOS ANGELES"
    assert n["state"] == "CA"
    assert n["zip"] == "90001"


def test_normalize_collapses_internal_whitespace():
    row = {"id": "1", "street": "123  Main   St  Apt  4B", "city": "San Francisco", "state": "CA", "zip": "94102"}
    n = normalize_row(row)
    assert n["street"] == "123 MAIN ST APT 4B"


def test_normalize_handles_missing_zip():
    row = {"id": "1", "street": "123 Main St", "city": "Oakland", "state": "CA", "zip": None}
    n = normalize_row(row)
    assert n["zip"] == ""


def test_normalize_handles_blank_zip():
    row = {"id": "1", "street": "123 Main St", "city": "Oakland", "state": "CA", "zip": "  "}
    n = normalize_row(row)
    assert n["zip"] == ""


# ── address_hash ──────────────────────────────────────────────────────────────

def test_address_hash_is_deterministic():
    row = {"id": "1", "street": "123 MAIN ST", "city": "OAKLAND", "state": "CA", "zip": "94601"}
    assert address_hash(row) == address_hash(row)


def test_address_hash_differs_for_different_addresses():
    a = {"id": "1", "street": "123 MAIN ST", "city": "OAKLAND", "state": "CA", "zip": "94601"}
    b = {"id": "2", "street": "456 OAK AVE", "city": "OAKLAND", "state": "CA", "zip": "94601"}
    assert address_hash(a) != address_hash(b)


def test_address_hash_excludes_id():
    """Hash must not include id — same address with different source ids should match."""
    a = {"id": "1", "street": "123 MAIN ST", "city": "OAKLAND", "state": "CA", "zip": "94601"}
    b = {"id": "999", "street": "123 MAIN ST", "city": "OAKLAND", "state": "CA", "zip": "94601"}
    assert address_hash(a) == address_hash(b)


# ── load_csv ──────────────────────────────────────────────────────────────────

VALID_CSV = """\
    id,street,city,state,zip
    1,123 Main St,Oakland,CA,94601
    2,456 Oak Ave,San Francisco,CA,94102
    3,789 Elm Blvd,Los Angeles,CA,90001
"""


def test_load_csv_happy_path(tmp_path, db):
    path = write_csv(tmp_path, "test.csv", VALID_CSV)
    result = load_csv(path, db)

    assert result.total_rows == 3
    assert result.loaded == 3
    assert result.rejected == 0
    assert result.duplicates_skipped == 0
    assert result.errors == []

    rows = db.execute("SELECT COUNT(*) FROM raw_addresses").fetchone()[0]
    assert rows == 3


def test_load_csv_records_source_file(tmp_path, db):
    path = write_csv(tmp_path, "contacts_export.csv", VALID_CSV)
    load_csv(path, db)
    source = db.execute("SELECT DISTINCT source_file FROM raw_addresses").fetchone()[0]
    assert source == "contacts_export.csv"


def test_load_csv_sets_retention_purge_after(tmp_path, db):
    path = write_csv(tmp_path, "test.csv", VALID_CSV)
    load_csv(path, db, retention_days=30)
    row = db.execute("SELECT uploaded_at, retention_purge_after FROM raw_addresses LIMIT 1").fetchone()
    assert row["retention_purge_after"] > row["uploaded_at"]


def test_load_csv_rejects_missing_required_field(tmp_path, db):
    csv_content = """\
        id,street,city,state,zip
        1,,Oakland,CA,94601
        2,456 Oak Ave,San Francisco,CA,94102
    """
    path = write_csv(tmp_path, "test.csv", csv_content)
    result = load_csv(path, db)

    assert result.rejected == 1
    assert result.loaded == 1
    assert len(result.errors) == 1
    assert "Row 2" in result.errors[0]
    assert "'street'" in result.errors[0]


def test_load_csv_missing_id_rejected(tmp_path, db):
    csv_content = """\
        id,street,city,state,zip
        ,123 Main St,Oakland,CA,94601
    """
    path = write_csv(tmp_path, "test.csv", csv_content)
    result = load_csv(path, db)
    assert result.rejected == 1
    assert "'id'" in result.errors[0]


def test_load_csv_allows_missing_zip(tmp_path, db):
    csv_content = """\
        id,street,city,state,zip
        1,123 Main St,Oakland,CA,
    """
    path = write_csv(tmp_path, "test.csv", csv_content)
    result = load_csv(path, db)
    assert result.loaded == 1
    assert result.rejected == 0


def test_load_csv_deduplicates_same_address(tmp_path, db):
    csv_content = """\
        id,street,city,state,zip
        1,123 Main St,Oakland,CA,94601
        2,123 Main St,Oakland,CA,94601
    """
    path = write_csv(tmp_path, "test.csv", csv_content)
    result = load_csv(path, db)

    assert result.loaded == 1
    assert result.duplicates_skipped == 1
    assert db.execute("SELECT COUNT(*) FROM raw_addresses").fetchone()[0] == 1


def test_load_csv_deduplicates_case_insensitive(tmp_path, db):
    csv_content = """\
        id,street,city,state,zip
        1,123 main st,Oakland,CA,94601
        2,123 MAIN ST,OAKLAND,CA,94601
    """
    path = write_csv(tmp_path, "test.csv", csv_content)
    result = load_csv(path, db)

    assert result.loaded == 1
    assert result.duplicates_skipped == 1


def test_load_csv_raises_on_bad_schema(tmp_path, db):
    csv_content = """\
        name,address,city,state,zip
        Jane,123 Main St,Oakland,CA,94601
    """
    path = write_csv(tmp_path, "test.csv", csv_content)
    with pytest.raises(ValueError, match="missing required columns"):
        load_csv(path, db)


def test_load_csv_handles_messy_whitespace(tmp_path, db):
    csv_content = """\
        id,street,city,state,zip
        1,"  123   Main  St  Apt 4B  ","  Oakland  ","  CA  ",94601
    """
    path = write_csv(tmp_path, "test.csv", csv_content)
    result = load_csv(path, db)

    assert result.loaded == 1
    row = db.execute("SELECT street, city, state FROM raw_addresses").fetchone()
    assert row["street"] == "123 MAIN ST APT 4B"
    assert row["city"] == "OAKLAND"
    assert row["state"] == "CA"
