from unittest.mock import patch

import pytest

from src.geocode.client import BatchRecord, _build_request_csv
from src.geocode.parser import GeocodedResult, parse_response
from src.geocode.runner import run_geocoding


# ── _build_request_csv ────────────────────────────────────────────────────────

def test_build_request_csv_format():
    records = [
        BatchRecord("0", "123 MAIN ST", "OAKLAND", "CA", "94601"),
        BatchRecord("1", "456 OAK AVE", "SAN FRANCISCO", "CA", "94102"),
    ]
    csv_text = _build_request_csv(records)
    lines = [l for l in csv_text.strip().splitlines() if l]
    assert len(lines) == 2
    assert lines[0] == "0,123 MAIN ST,OAKLAND,CA,94601"
    assert lines[1] == "1,456 OAK AVE,SAN FRANCISCO,CA,94102"


def test_build_request_csv_empty_zip():
    records = [BatchRecord("0", "123 MAIN ST", "OAKLAND", "CA", "")]
    csv_text = _build_request_csv(records)
    assert csv_text.strip() == "0,123 MAIN ST,OAKLAND,CA,"


# ── parse_response ────────────────────────────────────────────────────────────

MATCH_ROW = (
    '0,"123 Main St, Oakland, CA, 94601",Match,Exact,'
    '"123 Main St, Oakland, CA 94601",'
    '"-122.271356,37.804363",'  # coordinates must be quoted — comma inside the field
    "1102535,L,06,001,400100,1001"
)

NO_MATCH_ROW = '1,"456 Nowhere Blvd, Oakland, CA, 99999",No_Match,,,,,,,,'

TIE_ROW = '2,"789 Elm Blvd, Los Angeles, CA, 90001",Tie,,,,,,,,'


def test_parse_response_match():
    results = parse_response(MATCH_ROW)
    assert len(results) == 1
    r = results[0]
    assert r.census_id == "0"
    assert r.match == "Match"
    assert r.match_type == "Exact"
    assert r.is_match is True
    assert r.lat == pytest.approx(37.804363)
    assert r.lng == pytest.approx(-122.271356)
    assert r.block_geoid == "060014001001001"


def test_parse_response_no_match():
    results = parse_response(NO_MATCH_ROW)
    r = results[0]
    assert r.match == "No_Match"
    assert r.is_match is False
    assert r.lat is None
    assert r.block_geoid is None


def test_parse_response_tie():
    results = parse_response(TIE_ROW)
    r = results[0]
    assert r.match == "Tie"
    assert r.is_match is False


def test_parse_response_multiple_rows():
    text = "\n".join([MATCH_ROW, NO_MATCH_ROW, TIE_ROW])
    results = parse_response(text)
    assert len(results) == 3
    assert results[0].is_match is True
    assert results[1].is_match is False
    assert results[2].is_match is False


def test_parse_response_empty_input():
    assert parse_response("") == []
    assert parse_response("   \n  ") == []


def test_parse_geoid_zero_pads_components():
    # State=6, County=1, Tract=400100, Block=1 — should zero-pad to 15 digits
    # state=06(2) + county=001(3) + tract=400100(6) + block=0001(4) = 15
    row = '0,"addr",Match,Exact,"matched","-122,37","tiger","L",6,1,400100,1'
    r = parse_response(row)[0]
    assert len(r.block_geoid) == 15
    assert r.block_geoid == "060014001000001"


# ── run_geocoding ─────────────────────────────────────────────────────────────

CENSUS_MATCH_RESPONSE = """\
0,"123 MAIN ST, OAKLAND, CA, 94601",Match,Exact,"123 Main St, Oakland, CA 94601","-122.271356,37.804363",1102535,L,06,001,400100,1001
1,"456 OAK AVE, SAN FRANCISCO, CA, 94102",No_Match,,,,,,,,
"""

CENSUS_ALL_MATCH_RESPONSE = """\
0,"123 MAIN ST, OAKLAND, CA, 94601",Match,Exact,"123 Main St, Oakland, CA 94601","-122.271356,37.804363",1102535,L,06,001,400100,1001
1,"456 OAK AVE, SAN FRANCISCO, CA, 94102",Match,Non_Exact,"456 Oak Ave, San Francisco, CA 94102","-122.431297,37.773972",1103000,L,06,075,012300,2001
2,"789 ELM BLVD, LOS ANGELES, CA, 90001",Match,Exact,"789 Elm Blvd, Los Angeles, CA 90001","-118.243683,34.052235",1104000,L,06,037,207400,3001
"""


def test_run_geocoding_writes_matches_and_misses(db_with_addresses):
    with patch("src.geocode.runner.geocode_batch") as mock_geocode:
        mock_geocode.return_value = CENSUS_MATCH_RESPONSE
        summary = run_geocoding(db_with_addresses)

    # Two addresses sent in one batch (hash_ccc won't appear in mock response
    # since mock returns only 2 rows for 3 submitted — but wait, the mock
    # returns rows 0 and 1 for all 3 addresses: hash_ccc will be a silent drop)
    assert summary["batches"] == 1
    assert summary["geocoded"] == 1
    assert summary["misses"] == 2  # 1 No_Match + 1 silent drop


def test_run_geocoding_match_written_to_geocoded_records(db_with_addresses):
    with patch("src.geocode.runner.geocode_batch") as mock_geocode:
        mock_geocode.return_value = CENSUS_ALL_MATCH_RESPONSE
        run_geocoding(db_with_addresses)

    rows = db_with_addresses.execute("SELECT COUNT(*) FROM geocoded_records").fetchone()[0]
    assert rows == 3


def test_run_geocoding_geoid_stored_correctly(db_with_addresses):
    with patch("src.geocode.runner.geocode_batch") as mock_geocode:
        mock_geocode.return_value = CENSUS_ALL_MATCH_RESPONSE
        run_geocoding(db_with_addresses)

    row = db_with_addresses.execute(
        "SELECT block_geoid FROM geocoded_records WHERE address_hash = 'hash_aaa'"
    ).fetchone()
    assert row["block_geoid"] == "060014001001001"


def test_run_geocoding_miss_written_to_geocode_misses(db_with_addresses):
    with patch("src.geocode.runner.geocode_batch") as mock_geocode:
        mock_geocode.return_value = CENSUS_MATCH_RESPONSE
        run_geocoding(db_with_addresses)

    miss = db_with_addresses.execute(
        "SELECT reason FROM geocode_misses WHERE address_hash = 'hash_bbb'"
    ).fetchone()
    assert miss is not None
    assert "No_Match" in miss["reason"]


def test_run_geocoding_skips_already_geocoded(db_with_addresses):
    db_with_addresses.execute(
        """
        INSERT INTO geocoded_records
            (address_hash, lat, lng, block_geoid,
             geocoder_source, geocoder_benchmark, geocoder_vintage,
             match_score, match_type, geocoded_at)
        VALUES ('hash_aaa', 37.8, -122.2, '060014001001001',
                'census', 'Public_AR_Current', 'Current_Current',
                'Match', 'Exact', '2026-01-01T00:00:00Z')
        """
    )
    db_with_addresses.commit()

    with patch("src.geocode.runner.geocode_batch") as mock_geocode:
        mock_geocode.return_value = CENSUS_ALL_MATCH_RESPONSE
        run_geocoding(db_with_addresses)

    # hash_aaa already geocoded — only 2 should have been submitted
    call_args = mock_geocode.call_args[0][0]
    submitted_ids = {r.census_id for r in call_args}
    assert len(submitted_ids) == 2


def test_run_geocoding_no_pending_returns_early(db):
    with patch("src.geocode.runner.geocode_batch") as mock_geocode:
        summary = run_geocoding(db)

    mock_geocode.assert_not_called()
    assert summary == {"batches": 0, "geocoded": 0, "misses": 0, "errors": 0}


def test_run_geocoding_batch_error_logs_misses(db_with_addresses):
    with patch("src.geocode.runner.geocode_batch", side_effect=Exception("timeout")):
        summary = run_geocoding(db_with_addresses)

    assert summary["errors"] == 3
    misses = db_with_addresses.execute("SELECT COUNT(*) FROM geocode_misses").fetchone()[0]
    assert misses == 3
    reason = db_with_addresses.execute(
        "SELECT reason FROM geocode_misses LIMIT 1"
    ).fetchone()["reason"]
    assert "batch_error" in reason
    assert "timeout" in reason


def test_run_geocoding_respects_batch_size(db_with_addresses):
    responses = [
        '0,"addr",Match,Exact,"matched","-122.27,37.80",tid,L,06,001,400100,1001\n',
        '0,"addr",Match,Exact,"matched","-118.24,34.05",tid,L,06,037,207400,3001\n',
        '0,"addr",Match,Exact,"matched","-122.43,37.77",tid,L,06,075,012300,2001\n',
    ]
    with patch("src.geocode.runner.geocode_batch") as mock_geocode:
        mock_geocode.side_effect = responses
        run_geocoding(db_with_addresses, batch_size=1)

    assert mock_geocode.call_count == 3



