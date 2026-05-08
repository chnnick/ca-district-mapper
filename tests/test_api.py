import csv
import io
import json
import zipfile
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from src.api.app import create_app
from src.db import apply_migrations


# ── fixtures ──────────────────────────────────────────────────────────────────

MIGRATIONS = Path(__file__).parent.parent / "db" / "migrations"


def _make_app(tmp_path):
    db_path = tmp_path / "test.db"
    apply_migrations(db_path, MIGRATIONS)
    return create_app(db_path=db_path, raw_dir=tmp_path / "raw")


@pytest.fixture
def client(tmp_path):
    return TestClient(_make_app(tmp_path))


@pytest.fixture
def client_with_data(tmp_path):
    """TestClient backed by a DB that already has assignments ready to report on."""
    import sqlite3
    from src.match.bef_config import BefSource
    from src.match.bef_loader import hash_file, load_bef_version
    from src.match.assigner import run_assignment

    db_path = tmp_path / "test.db"
    apply_migrations(db_path, MIGRATIONS)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")

    now, purge = "2026-04-30T00:00:00Z", "2026-07-29T00:00:00Z"
    conn.executemany(
        "INSERT INTO raw_addresses (id, address_hash, street, city, state, zip, source_file, uploaded_at, retention_purge_after) VALUES (?, ?, ?, ?, ?, ?, 'test.csv', ?, ?)",
        [
            ("1", "hash_aaa", "123 MAIN ST",  "OAKLAND",       "CA", "94601", now, purge),
            ("2", "hash_bbb", "456 OAK AVE",  "SAN FRANCISCO", "CA", "94102", now, purge),
            ("3", "hash_ddd", "321 PINE ST",  "OAKLAND",       "CA", "94601", now, purge),
        ],
    )
    conn.executemany(
        "INSERT INTO geocoded_records (address_hash, lat, lng, block_geoid, zip, geocoder_source, geocoder_benchmark, geocoder_vintage, match_score, match_type, geocoded_at) VALUES (?, ?, ?, ?, ?, 'census', 'Public_AR_Current', 'Current_Current', 'Match', 'Exact', ?)",
        [
            ("hash_aaa", 37.80, -122.27, "060014001001001", "94601", now),
            ("hash_bbb", 37.77, -122.43, "060750123002001", "94102", now),
            ("hash_ddd", 37.80, -122.27, "060014001001002", "94601", now),
        ],
    )

    bef_rows = [
        {"GEOID20": "060014001001001", "CD": "13"},
        {"GEOID20": "060014001001002", "CD": "13"},
        {"GEOID20": "060750123002001", "CD": "11"},
    ]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=["GEOID20", "CD"])
    w.writeheader()
    w.writerows(bef_rows)
    zip_path = tmp_path / "test_cd.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("blocks.csv", buf.getvalue().encode("utf-8"))

    source = BefSource(
        id="test_cd", label="Test CD", district_type="CD",
        effective_date="2026-01-01", expiration_date=None, supersedes=None,
        url="https://example.com/test.zip", local_filename="test_cd.zip",
        geoid_column="GEOID20", district_column="CD", has_header=True,
    )
    load_bef_version(conn, source, zip_path, hash_file(zip_path), "testuser")
    run_assignment(conn, district_types=["CD"])
    conn.commit()
    conn.close()

    return TestClient(create_app(db_path=db_path, raw_dir=tmp_path / "raw"))


def _csv_upload(content: bytes, filename: str = "upload.csv"):
    return {"file": (filename, content, "text/csv")}


VALID_CSV = b"id,street,city,state,zip\n1,123 Main St,Oakland,CA,94601\n2,456 Oak Ave,San Francisco,CA,94102\n"
BAD_SCHEMA_CSV = b"name,address\nJane,123 Main St\n"
EMPTY_CSV = b"id,street,city,state,zip\n"


# ── POST /uploads ─────────────────────────────────────────────────────────────

def test_upload_returns_202_and_job_id(client):
    with patch("src.api.routes.uploads._run_pipeline"):
        resp = client.post("/api/uploads", files=_csv_upload(VALID_CSV))
    assert resp.status_code == 202
    data = resp.json()
    assert "job_id" in data
    assert data["ingest"]["loaded"] == 2


def test_upload_rejects_non_csv(client):
    resp = client.post("/api/uploads", files={"file": ("data.xlsx", b"...", "application/octet-stream")})
    assert resp.status_code == 400


def test_upload_rejects_bad_schema(client):
    with patch("src.api.routes.uploads._run_pipeline"):
        resp = client.post("/api/uploads", files=_csv_upload(BAD_SCHEMA_CSV))
    assert resp.status_code == 422


def test_upload_422_when_all_rows_rejected(client):
    all_bad = b"id,street,city,state,zip\n,123 Main St,,CA,94601\n"
    with patch("src.api.routes.uploads._run_pipeline"):
        resp = client.post("/api/uploads", files=_csv_upload(all_bad))
    assert resp.status_code == 422


def test_upload_409_when_job_running(client):
    with patch("src.api.routes.uploads._run_pipeline"):
        first = client.post("/api/uploads", files=_csv_upload(VALID_CSV))
    job_id = first.json()["job_id"]

    # Manually set the job to geocoding so the next upload sees it as running
    import sqlite3
    db_path = client.app.state.db_path
    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE jobs SET status='geocoding' WHERE id=?", (job_id,))
    conn.commit()
    conn.close()

    with patch("src.api.routes.uploads._run_pipeline"):
        resp = client.post("/api/uploads", files=_csv_upload(VALID_CSV))
    assert resp.status_code == 409
    assert job_id in resp.json()["detail"]


def test_upload_saves_file_to_raw_dir(tmp_path):
    app = _make_app(tmp_path)
    c = TestClient(app)
    with patch("src.api.routes.uploads._run_pipeline"):
        c.post("/api/uploads", files=_csv_upload(VALID_CSV, "myfile.csv"))
    assert (tmp_path / "raw" / "myfile.csv").exists()


def test_upload_ingest_summary_includes_errors(client):
    mixed = b"id,street,city,state,zip\n1,123 Main St,Oakland,CA,94601\n,456 Oak Ave,SF,CA,94102\n"
    with patch("src.api.routes.uploads._run_pipeline"):
        resp = client.post("/api/uploads", files=_csv_upload(mixed))
    assert resp.status_code == 202
    data = resp.json()
    assert data["ingest"]["loaded"] == 1
    assert data["ingest"]["rejected"] == 1
    assert len(data["ingest"]["errors"]) == 1


# ── GET /jobs ─────────────────────────────────────────────────────────────────

def test_get_job_returns_status(client):
    with patch("src.api.routes.uploads._run_pipeline"):
        job_id = client.post("/api/uploads", files=_csv_upload(VALID_CSV)).json()["job_id"]
    resp = client.get(f"/api/jobs/{job_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == job_id
    assert data["status"] in ("geocoding", "matching", "done", "failed")
    assert data["source_file"] == "upload.csv"


def test_get_job_404_for_unknown(client):
    assert client.get("/api/jobs/nonexistent-id").status_code == 404


def test_get_job_deserializes_ingest_summary(client):
    with patch("src.api.routes.uploads._run_pipeline"):
        job_id = client.post("/api/uploads", files=_csv_upload(VALID_CSV)).json()["job_id"]
    data = client.get(f"/api/jobs/{job_id}").json()
    assert isinstance(data["ingest_summary"], dict)
    assert "loaded" in data["ingest_summary"]


def test_list_jobs(client):
    with patch("src.api.routes.uploads._run_pipeline"):
        client.post("/api/uploads", files=_csv_upload(VALID_CSV))
    resp = client.get("/api/jobs")
    assert resp.status_code == 200
    assert len(resp.json()) >= 1


# ── GET /reports/legislators (JSON) ──────────────────────────────────────────

def test_list_legislators_returns_json(client_with_data):
    resp = client_with_data.get("/api/reports/legislators")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert any(r["district_type"] == "CD" for r in data)


def test_list_legislators_correct_counts(client_with_data):
    resp = client_with_data.get("/api/reports/legislators?district_types=CD")
    by_district = {r["district_number"]: r["constituent_count"] for r in resp.json()}
    assert by_district["13"] == 2
    assert by_district["11"] == 1


def test_list_legislators_rejects_invalid_type(client_with_data):
    assert client_with_data.get("/api/reports/legislators?district_types=XX").status_code == 400


# ── GET /reports/rollup (CSV) ─────────────────────────────────────────────────

def test_rollup_returns_csv(client_with_data):
    resp = client_with_data.get("/api/reports/rollup?district_types=CD")
    assert resp.status_code == 200
    assert "text/csv" in resp.headers["content-type"]
    assert "attachment" in resp.headers["content-disposition"]


def test_rollup_csv_has_correct_columns(client_with_data):
    resp = client_with_data.get("/api/reports/rollup?district_types=CD")
    reader = csv.DictReader(io.StringIO(resp.text))
    rows = [r for r in reader if not r["district_type"].startswith("#")]
    assert set(rows[0].keys()) >= {"district_type", "district_number", "constituent_count"}


def test_rollup_csv_has_methodology_footer(client_with_data):
    resp = client_with_data.get("/api/reports/rollup?district_types=CD")
    assert "Census Geocoder" in resp.text
    assert "Generated:" in resp.text


def test_rollup_no_raw_addresses_in_output(client_with_data):
    resp = client_with_data.get("/api/reports/rollup?district_types=CD")
    content = resp.text.lower()
    for addr in ("123 main", "456 oak", "321 pine"):
        assert addr not in content


# ── GET /reports/legislators/{type}/{number} (CSV) ────────────────────────────

def test_legislator_report_returns_csv(client_with_data):
    resp = client_with_data.get("/api/reports/legislators/CD/13")
    assert resp.status_code == 200
    assert "text/csv" in resp.headers["content-type"]


def test_legislator_report_has_zip_breakdown(client_with_data):
    resp = client_with_data.get("/api/reports/legislators/CD/13")
    rows = [r for r in csv.DictReader(io.StringIO(resp.text)) if not r["district_type"].startswith("#")]
    assert len(rows) == 1
    assert rows[0]["zip"] == "94601"
    assert rows[0]["constituent_count"] == "2"


def test_legislator_report_footer_has_total(client_with_data):
    resp = client_with_data.get("/api/reports/legislators/CD/13")
    assert "Total constituents: 2" in resp.text


def test_legislator_report_404_unknown_district(client_with_data):
    assert client_with_data.get("/api/reports/legislators/CD/99").status_code == 404


def test_legislator_report_400_invalid_type(client_with_data):
    assert client_with_data.get("/api/reports/legislators/XX/13").status_code == 400


def test_legislator_report_case_insensitive_type(client_with_data):
    resp = client_with_data.get("/api/reports/legislators/cd/13")
    assert resp.status_code == 200


# ── GET /reports/legislators/{type}/{number}/stats (JSON) ────────────────────

def test_legislator_stats_returns_json(client_with_data):
    resp = client_with_data.get("/api/reports/legislators/CD/13/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert data["district_type"] == "CD"
    assert data["district_number"] == "13"
    assert data["total"] == 2
    assert isinstance(data["zip_breakdown"], list)
    assert data["zip_breakdown"][0]["zip"] == "94601"
    assert data["zip_breakdown"][0]["constituent_count"] == 2


def test_legislator_stats_404_unknown_district(client_with_data):
    assert client_with_data.get("/api/reports/legislators/CD/99/stats").status_code == 404


def test_legislator_stats_400_invalid_type(client_with_data):
    assert client_with_data.get("/api/reports/legislators/XX/13/stats").status_code == 400


# ── GET /map/points ───────────────────────────────────────────────────────────

def test_map_points_all(client_with_data):
    resp = client_with_data.get("/api/map/points")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) >= 1
    assert all("lat" in p and "lng" in p for p in data)


def test_map_points_filtered(client_with_data):
    resp = client_with_data.get("/api/map/points?district_type=CD&district_number=13")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["lat"] == pytest.approx(37.80)
    assert data[0]["lng"] == pytest.approx(-122.27)


def test_map_points_filtered_empty(client_with_data):
    resp = client_with_data.get("/api/map/points?district_type=CD&district_number=99")
    assert resp.status_code == 200
    assert resp.json() == []


# ── GET /people/{id}/districts ────────────────────────────────────────────────

_PII_FIELDS = ("street", "city", "state", "zip")


def test_person_lookup_404_for_unknown_id(client_with_data):
    assert client_with_data.get("/api/people/nope/districts").status_code == 404


def test_person_lookup_returns_districts_and_coords(client_with_data):
    resp = client_with_data.get("/api/people/1/districts")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == "1"
    assert data["status"] in ("ok", "partial")
    assert data["districts"]["CD"] == "13"
    assert data["lat"] == pytest.approx(37.80)
    assert data["lng"] == pytest.approx(-122.27)


def test_person_lookup_response_excludes_pii(client_with_data):
    resp = client_with_data.get("/api/people/1/districts")
    body = resp.text.lower()
    for field in _PII_FIELDS:
        assert f'"{field}"' not in body
    for addr in ("123 main", "oakland"):
        assert addr not in body


def test_person_lookup_not_geocoded_when_no_geocode_record(client_with_data, tmp_path):
    import sqlite3
    db_path = client_with_data.app.state.db_path
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO raw_addresses (id, address_hash, street, city, state, zip,"
        " source_file, uploaded_at, retention_purge_after) VALUES"
        " ('99', 'hash_no_geo', '999 NEW ST', 'OAKLAND', 'CA', '94601', 'test.csv',"
        " '2026-04-30T00:00:00Z', '2026-07-29T00:00:00Z')"
    )
    conn.commit()
    conn.close()

    resp = client_with_data.get("/api/people/99/districts")
    assert resp.status_code == 200
    assert resp.json() == {"id": "99", "status": "not_geocoded"}


