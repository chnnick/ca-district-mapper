"""Resolve filesystem locations for data, raw uploads, and BEF downloads.

When APP_DATA_DIR is set (e.g. the Tauri sidecar passes the platform
app-data directory), all writable state is rooted there. Otherwise paths
fall back to the in-repo `data/` directory used by Docker and local dev.
"""

import os
from pathlib import Path

_PROJECT_ROOT = Path(__file__).parent.parent


def data_root() -> Path:
    override = os.environ.get("APP_DATA_DIR")
    return Path(override) if override else _PROJECT_ROOT / "data"


def db_path() -> Path:
    return data_root() / "district_mapper.db"


def raw_dir() -> Path:
    return data_root() / "raw"


def bef_dir() -> Path:
    return data_root() / "bef"
