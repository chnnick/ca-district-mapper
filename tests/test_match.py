import csv
import hashlib
import io
import zipfile
from pathlib import Path

import pytest

from src.match.assigner import get_active_bef_version_id, run_assignment
from src.match.bef_config import BefSource, load_bef_sources
from src.match.bef_loader import get_current_file_hash, hash_file, load_bef_version


# ── helpers ───────────────────────────────────────────────────────────────────

def make_bef_zip(
    tmp_path: Path,
    filename: str,
    rows: list[dict],
    geoid_col: str = "GEOID20",
    district_col: str = "CD",
) -> tuple[Path, str]:
    """Build a ZIP containing one BEF CSV; return (path, sha256)."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=[geoid_col, district_col])
    writer.writeheader()
    writer.writerows(rows)
    csv_bytes = buf.getvalue().encode("utf-8")

    zip_path = tmp_path / filename
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("blocks.csv", csv_bytes)

    return zip_path, hash_file(zip_path)


def make_source(
    district_type: str = "CD",
    effective_date: str = "2026-01-01",
    expiration_date: str | None = None,
    geoid_column: str = "GEOID20",
    district_column: str = "CD",
) -> BefSource:
    return BefSource(
        id=f"test_{district_type.lower()}",
        label=f"Test {district_type}",
        district_type=district_type,
        effective_date=effective_date,
        expiration_date=expiration_date,
        supersedes=None,
        url="https://example.com/test.zip",
        local_filename="test.zip",
        geoid_column=geoid_column,
        district_column=district_column,
    )


SAMPLE_BLOCKS = [
    {"GEOID20": "060014001001001", "CD": "13"},
    {"GEOID20": "060014001001002", "CD": "13"},
    {"GEOID20": "060750123002001", "CD": "11"},
]


# ── bef_config ────────────────────────────────────────────────────────────────

def test_load_bef_sources_returns_all_entries():
    config = Path(__file__).parent.parent / "config" / "bef_sources.yaml"
    sources = load_bef_sources(config)
    assert len(sources) == 5


def test_load_bef_sources_current_cd_is_ab604():
    config = Path(__file__).parent.parent / "config" / "bef_sources.yaml"
    sources = load_bef_sources(config)
    current_cd = next(s for s in sources if s.district_type == "CD" and s.expiration_date is None)
    assert current_cd.id == "ab604_cd"


def test_load_bef_sources_superseded_cd_has_expiration():
    config = Path(__file__).parent.parent / "config" / "bef_sources.yaml"
    sources = load_bef_sources(config)
    old_cd = next(s for s in sources if s.id == "2021_cd")
    assert old_cd.expiration_date == "2025-12-31"


def test_load_bef_sources_has_column_config():
    config = Path(__file__).parent.parent / "config" / "bef_sources.yaml"
    sources = load_bef_sources(config)
    for s in sources:
        assert s.geoid_column, f"{s.id} missing geoid_column"
        assert s.district_column, f"{s.id} missing district_column"


# ── bef_loader ────────────────────────────────────────────────────────────────

def test_load_bef_version_inserts_version_row(tmp_path, db):
    source = make_source()
    zip_path, file_hash = make_bef_zip(tmp_path, "test.zip", SAMPLE_BLOCKS)

    version_id, block_count = load_bef_version(db, source, zip_path, file_hash, "testuser")

    row = db.execute("SELECT * FROM bef_versions WHERE id = ?", (version_id,)).fetchone()
    assert row["bef_source_id"] == "test_cd"
    assert row["district_type"] == "CD"
    assert row["file_hash"] == file_hash
    assert row["approved_by"] == "testuser"


def test_load_bef_version_returns_block_count(tmp_path, db):
    source = make_source()
    zip_path, file_hash = make_bef_zip(tmp_path, "test.zip", SAMPLE_BLOCKS)

    _, block_count = load_bef_version(db, source, zip_path, file_hash, "testuser")

    assert block_count == len(SAMPLE_BLOCKS)
    db_count = db.execute("SELECT COUNT(*) FROM bef_blocks").fetchone()[0]
    assert db_count == len(SAMPLE_BLOCKS)


def test_load_bef_version_stores_correct_geoid_and_district(tmp_path, db):
    source = make_source()
    zip_path, file_hash = make_bef_zip(tmp_path, "test.zip", SAMPLE_BLOCKS)
    load_bef_version(db, source, zip_path, file_hash, "testuser")

    row = db.execute(
        "SELECT district_number FROM bef_blocks WHERE geoid = '060014001001001'"
    ).fetchone()
    assert row["district_number"] == "13"


def test_load_bef_version_rejects_duplicate_hash(tmp_path, db):
    source = make_source()
    zip_path, file_hash = make_bef_zip(tmp_path, "test.zip", SAMPLE_BLOCKS)
    load_bef_version(db, source, zip_path, file_hash, "testuser")
    db.commit()

    with pytest.raises(ValueError, match="already loaded"):
        load_bef_version(db, source, zip_path, file_hash, "testuser")


def test_load_bef_version_raises_on_missing_geoid_column(tmp_path, db):
    source = make_source(geoid_column="WRONG_COL")
    zip_path, file_hash = make_bef_zip(tmp_path, "test.zip", SAMPLE_BLOCKS)

    with pytest.raises(ValueError, match="WRONG_COL"):
        load_bef_version(db, source, zip_path, file_hash, "testuser")


def test_load_bef_version_raises_on_missing_district_column(tmp_path, db):
    source = make_source(district_column="WRONG_DIST")
    zip_path, file_hash = make_bef_zip(tmp_path, "test.zip", SAMPLE_BLOCKS)

    with pytest.raises(ValueError, match="WRONG_DIST"):
        load_bef_version(db, source, zip_path, file_hash, "testuser")


def test_load_bef_version_skips_blank_rows(tmp_path, db):
    rows = SAMPLE_BLOCKS + [{"GEOID20": "", "CD": "13"}, {"GEOID20": "060014001001003", "CD": ""}]
    source = make_source()
    zip_path, file_hash = make_bef_zip(tmp_path, "test.zip", rows)
    _, block_count = load_bef_version(db, source, zip_path, file_hash, "testuser")
    assert block_count == len(SAMPLE_BLOCKS)


def test_get_current_file_hash_returns_none_when_empty(db):
    assert get_current_file_hash(db, "ab604_cd") is None


def test_get_current_file_hash_returns_latest(tmp_path, db):
    source = make_source()
    zip_path, file_hash = make_bef_zip(tmp_path, "test.zip", SAMPLE_BLOCKS)
    load_bef_version(db, source, zip_path, file_hash, "testuser")
    db.commit()

    assert get_current_file_hash(db, "test_cd") == file_hash


# ── assigner ──────────────────────────────────────────────────────────────────

@pytest.fixture
def db_with_bef(tmp_path, db):
    """DB with geocoded records and a loaded CD BEF covering their GEOIDs."""
    now = "2026-04-30T00:00:00Z"
    purge = "2026-07-29T00:00:00Z"

    db.executemany(
        "INSERT INTO raw_addresses (id, address_hash, street, city, state, zip, source_file, uploaded_at, retention_purge_after) VALUES (?, ?, ?, ?, ?, ?, 'test.csv', ?, ?)",
        [
            ("1", "hash_aaa", "123 MAIN ST", "OAKLAND", "CA", "94601", now, purge),
            ("2", "hash_bbb", "456 OAK AVE", "SAN FRANCISCO", "CA", "94102", now, purge),
            ("3", "hash_ccc", "789 ELM BLVD", "LOS ANGELES", "CA", "90001", now, purge),
        ],
    )
    db.executemany(
        "INSERT INTO geocoded_records (address_hash, lat, lng, block_geoid, geocoder_source, geocoder_benchmark, geocoder_vintage, match_score, match_type, geocoded_at) VALUES (?, ?, ?, ?, 'census', 'Public_AR_Current', 'Current_Current', 'Match', 'Exact', ?)",
        [
            ("hash_aaa", 37.80, -122.27, "060014001001001", now),
            ("hash_bbb", 37.77, -122.43, "060750123002001", now),
            ("hash_ccc", 34.05, -118.24, None, now),  # no block_geoid
        ],
    )

    source = make_source(effective_date="2026-01-01")
    zip_path, file_hash = make_bef_zip(
        tmp_path, "test_cd.zip",
        [
            {"GEOID20": "060014001001001", "CD": "13"},
            {"GEOID20": "060750123002001", "CD": "11"},
        ],
    )
    load_bef_version(db, source, zip_path, file_hash, "testuser")
    db.commit()
    return db


def test_get_active_bef_version_id_returns_current(db_with_bef):
    vid = get_active_bef_version_id(db_with_bef, "CD")
    assert vid is not None


def test_get_active_bef_version_id_returns_none_when_no_bef(db_with_bef):
    assert get_active_bef_version_id(db_with_bef, "SD") is None


def test_get_active_bef_version_id_point_in_time_before_effective(db_with_bef):
    assert get_active_bef_version_id(db_with_bef, "CD", as_of_date="2025-12-31") is None


def test_get_active_bef_version_id_point_in_time_on_effective(db_with_bef):
    assert get_active_bef_version_id(db_with_bef, "CD", as_of_date="2026-01-01") is not None


def test_run_assignment_assigns_known_geoids(db_with_bef):
    summary = run_assignment(db_with_bef, district_types=["CD"])

    assert summary["CD"]["assigned"] == 2  # hash_aaa and hash_bbb have GEOIDs in BEF
    rows = db_with_bef.execute("SELECT COUNT(*) FROM district_assignments").fetchone()[0]
    assert rows == 2


def test_run_assignment_correct_district_numbers(db_with_bef):
    run_assignment(db_with_bef, district_types=["CD"])

    row = db_with_bef.execute(
        "SELECT district_number FROM district_assignments WHERE address_hash = 'hash_aaa'"
    ).fetchone()
    assert row["district_number"] == "13"

    row = db_with_bef.execute(
        "SELECT district_number FROM district_assignments WHERE address_hash = 'hash_bbb'"
    ).fetchone()
    assert row["district_number"] == "11"


def test_run_assignment_skips_null_block_geoid(db_with_bef):
    run_assignment(db_with_bef, district_types=["CD"])
    row = db_with_bef.execute(
        "SELECT * FROM district_assignments WHERE address_hash = 'hash_ccc'"
    ).fetchone()
    assert row is None


def test_run_assignment_is_idempotent(db_with_bef):
    run_assignment(db_with_bef, district_types=["CD"])
    db_with_bef.commit()
    run_assignment(db_with_bef, district_types=["CD"])

    count = db_with_bef.execute("SELECT COUNT(*) FROM district_assignments").fetchone()[0]
    assert count == 2


def test_run_assignment_no_active_bef_skips_gracefully(db_with_bef):
    summary = run_assignment(db_with_bef, district_types=["SD"])
    assert summary["SD"]["no_active_bef"] is True
    assert summary["SD"]["assigned"] == 0


def test_run_assignment_reports_no_bef_match_count(db_with_bef):
    # Add a geocoded record whose GEOID is not in the BEF
    db_with_bef.execute(
        "INSERT INTO geocoded_records (address_hash, lat, lng, block_geoid, geocoder_source, geocoder_benchmark, geocoder_vintage, match_score, match_type, geocoded_at) VALUES ('hash_zzz', 36.0, -119.0, '060990000009999', 'census', 'Public_AR_Current', 'Current_Current', 'Match', 'Exact', '2026-04-30T00:00:00Z')"
    )
    db_with_bef.commit()

    summary = run_assignment(db_with_bef, district_types=["CD"])
    assert summary["CD"]["no_bef_match"] == 1
