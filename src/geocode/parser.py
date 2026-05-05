import csv
import io
from dataclasses import dataclass


@dataclass
class GeocodedResult:
    census_id: str
    match: str        # Census field 3: "Match", "No_Match", "Tie"
    match_type: str   # Census field 4: "Exact", "Non_Exact", or "" when no match
    lat: float | None
    lng: float | None
    block_geoid: str | None  # 15-digit GEOID: state(2)+county(3)+tract(6)+block(4)

    @property
    def is_match(self) -> bool:
        return self.match == "Match" and self.block_geoid is not None


def parse_response(response_text: str) -> list[GeocodedResult]:
    """
    Parse the Census batch geocoder CSV response.

    The Census API returns one row per submitted address, but may omit rows
    entirely for addresses it cannot parse. Callers should detect and handle
    any address_hashes that receive no response row.
    """
    results = []
    reader = csv.reader(io.StringIO(response_text.strip()))
    for row in reader:
        if not row:
            continue
        results.append(_parse_row(row))
    return results


def _parse_row(row: list[str]) -> GeocodedResult:
    # Pad to 12 fields — Census omits trailing empty fields on no-match rows
    row = (row + [""] * 12)[:12]

    census_id   = row[0].strip()
    match       = row[2].strip()   # "Match", "No_Match", "Tie"
    match_type  = row[3].strip()   # "Exact", "Non_Exact", or ""
    coords      = row[5].strip()   # "longitude,latitude"
    state_fips  = row[8].strip()
    county_fips = row[9].strip()
    tract       = row[10].strip()
    block       = row[11].strip()

    lat, lng = _parse_coords(coords)
    geoid = _build_geoid(state_fips, county_fips, tract, block)

    return GeocodedResult(
        census_id=census_id,
        match=match,
        match_type=match_type,
        lat=lat,
        lng=lng,
        block_geoid=geoid,
    )


def _parse_coords(coords: str) -> tuple[float | None, float | None]:
    if not coords:
        return None, None
    parts = coords.split(",")
    if len(parts) != 2:
        return None, None
    try:
        return float(parts[1]), float(parts[0])  # response is "lng,lat"; return lat,lng
    except ValueError:
        return None, None


def _build_geoid(state: str, county: str, tract: str, block: str) -> str | None:
    if not all([state, county, tract, block]):
        return None
    # Census Geocoder returns tract without decimal; zero-pad each component
    return state.zfill(2) + county.zfill(3) + tract.zfill(6) + block.zfill(4)
