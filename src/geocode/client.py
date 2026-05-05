import csv
import io
import time
from typing import NamedTuple

import requests

CENSUS_BATCH_URL = "https://geocoding.geo.census.gov/geocoder/geographies/addressbatch"
BENCHMARK = "Public_AR_Current"
VINTAGE = "Current_Current"

_MAX_RETRIES = 3
_BACKOFF_BASE = 2  # seconds


class BatchRecord(NamedTuple):
    census_id: str
    street: str
    city: str
    state: str
    zip: str


def geocode_batch(records: list[BatchRecord], timeout: int = 300) -> str:
    """
    POST a batch of addresses to the Census Geocoder batch endpoint.
    Returns raw response text (CSV).

    Retries up to _MAX_RETRIES times on 5xx or connection errors with
    exponential backoff. Raises on non-retryable errors or after all retries.

    Census API docs: https://geocoding.geo.census.gov/geocoder/Geocoding_Services_API.pdf
    """
    payload = _build_request_csv(records)

    last_exc: Exception | None = None
    for attempt in range(_MAX_RETRIES):
        try:
            resp = requests.post(
                CENSUS_BATCH_URL,
                files={"addressFile": ("addresses.csv", payload, "text/plain")},
                data={"benchmark": BENCHMARK, "vintage": VINTAGE, "returntype": "geographies"},
                timeout=timeout,
            )
            resp.raise_for_status()
            return resp.text
        except requests.exceptions.HTTPError as exc:
            last_exc = exc
            if exc.response.status_code < 500:
                raise  # 4xx — not retryable
        except requests.exceptions.RequestException as exc:
            last_exc = exc

        if attempt < _MAX_RETRIES - 1:
            time.sleep(_BACKOFF_BASE**attempt)

    raise last_exc  # type: ignore[misc]


def _build_request_csv(records: list[BatchRecord]) -> str:
    """Build the no-header CSV the Census batch API expects."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    for r in records:
        writer.writerow([r.census_id, r.street, r.city, r.state, r.zip])
    return buf.getvalue()
