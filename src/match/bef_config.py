from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass
class BefSource:
    id: str
    label: str
    district_type: str
    effective_date: str
    expiration_date: str | None
    supersedes: str | None
    url: str
    local_filename: str
    has_header: bool
    geoid_column: str
    district_column: str
    notes: str = ""


def load_bef_sources(config_path: str | Path) -> list[BefSource]:
    with open(config_path) as f:
        data = yaml.safe_load(f)
    return [
        BefSource(
            id=entry["id"],
            label=entry["label"],
            district_type=entry["district_type"],
            effective_date=str(entry["effective_date"]),
            expiration_date=str(entry["expiration_date"]) if entry.get("expiration_date") else None,
            supersedes=entry.get("supersedes"),
            url=entry["url"],
            local_filename=entry["local_filename"],
            has_header=entry.get("has_header", True),
            geoid_column=entry.get("geoid_column", "GEOID20"),
            district_column=entry.get("district_column", entry["district_type"]),
            notes=entry.get("notes", ""),
        )
        for entry in data["bef_sources"]
    ]
