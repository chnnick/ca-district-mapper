import csv
import io
import sqlite3
import zipfile
from pathlib import Path

import pytest


@pytest.fixture
def db(tmp_path):
    migrations = Path(__file__).parent.parent / "db" / "migrations"
    conn = sqlite3.connect(tmp_path / "test.db")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    for script in sorted(migrations.glob("*.sql")):
        conn.executescript(script.read_text())
    conn.commit()
    return conn


@pytest.fixture
def db_with_addresses(db):
    """DB pre-loaded with three normalized addresses for geocode/match tests."""
    now = "2026-04-30T00:00:00Z"
    purge = "2026-07-29T00:00:00Z"
    db.executemany(
        """
        INSERT INTO raw_addresses
            (id, address_hash, street, city, state, zip,
             source_file, uploaded_at, retention_purge_after)
        VALUES (?, ?, ?, ?, ?, ?, 'test.csv', ?, ?)
        """,
        [
            ("1", "hash_aaa", "123 MAIN ST", "OAKLAND", "CA", "94601", now, purge),
            ("2", "hash_bbb", "456 OAK AVE", "SAN FRANCISCO", "CA", "94102", now, purge),
            ("3", "hash_ccc", "789 ELM BLVD", "LOS ANGELES", "CA", "90001", now, purge),
        ],
    )
    db.commit()
    return db


@pytest.fixture
def db_with_assignments(tmp_path, db):
    """
    DB with raw_addresses, geocoded_records (with zip), a loaded CD BEF,
    and district_assignments — ready for report tests.
    """
    from src.match.bef_loader import load_bef_version
    from src.match.assigner import run_assignment
    from src.match.bef_config import BefSource

    now = "2026-04-30T00:00:00Z"
    purge = "2026-07-29T00:00:00Z"

    db.executemany(
        "INSERT INTO raw_addresses (id, address_hash, street, city, state, zip, source_file, uploaded_at, retention_purge_after) VALUES (?, ?, ?, ?, ?, ?, 'test.csv', ?, ?)",
        [
            ("1", "hash_aaa", "123 MAIN ST",  "OAKLAND",       "CA", "94601", now, purge),
            ("2", "hash_bbb", "456 OAK AVE",  "SAN FRANCISCO", "CA", "94102", now, purge),
            ("3", "hash_ccc", "789 ELM BLVD", "LOS ANGELES",   "CA", "90001", now, purge),
            ("4", "hash_ddd", "321 PINE ST",  "OAKLAND",       "CA", "94601", now, purge),
        ],
    )
    db.executemany(
        "INSERT INTO geocoded_records (address_hash, lat, lng, block_geoid, zip, geocoder_source, geocoder_benchmark, geocoder_vintage, match_score, match_type, geocoded_at) VALUES (?, ?, ?, ?, ?, 'census', 'Public_AR_Current', 'Current_Current', 'Match', 'Exact', ?)",
        [
            ("hash_aaa", 37.80, -122.27, "060014001001001", "94601", now),
            ("hash_bbb", 37.77, -122.43, "060750123002001", "94102", now),
            ("hash_ccc", 34.05, -118.24, "060374001003001", "90001", now),
            ("hash_ddd", 37.80, -122.27, "060014001001002", "94601", now),
        ],
    )

    # Build a BEF ZIP covering all four GEOIDs
    bef_rows = [
        {"GEOID20": "060014001001001", "CD": "13"},
        {"GEOID20": "060014001001002", "CD": "13"},
        {"GEOID20": "060750123002001", "CD": "11"},
        {"GEOID20": "060374001003001", "CD": "34"},
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["GEOID20", "CD"])
    writer.writeheader()
    writer.writerows(bef_rows)

    zip_path = tmp_path / "test_cd.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("blocks.csv", buf.getvalue().encode("utf-8"))

    from src.match.bef_loader import hash_file
    file_hash = hash_file(zip_path)

    source = BefSource(
        id="test_cd", label="Test CD", district_type="CD",
        effective_date="2026-01-01", expiration_date=None, supersedes=None,
        url="https://example.com/test.zip", local_filename="test_cd.zip",
        geoid_column="GEOID20", district_column="CD",
    )
    load_bef_version(db, source, zip_path, file_hash, "testuser")
    run_assignment(db, district_types=["CD"])
    db.commit()
    return db
