"""Resolve filesystem locations for data, raw uploads, and BEF downloads.

When APP_DATA_DIR is set (e.g. the Tauri sidecar passes the platform
app-data directory), all writable state is rooted there. Otherwise paths
fall back to the in-repo `data/` directory used by Docker and local dev.
"""

import os
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).parent.parent


def bundle_root() -> Path:
    """Root for read-only bundled assets (migrations, config).

    In a PyInstaller-frozen build, asset files are extracted to sys._MEIPASS;
    in source checkouts, they live under the repo root.
    """
    if getattr(sys, "frozen", False):
        return Path(sys._MEIPASS)  # type: ignore[attr-defined]
    return _PROJECT_ROOT


def data_root() -> Path:
    override = os.environ.get("APP_DATA_DIR")
    return Path(override) if override else _PROJECT_ROOT / "data"


def db_path() -> Path:
    return data_root() / "district_mapper.db"


def raw_dir() -> Path:
    return data_root() / "raw"


def bef_dir() -> Path:
    return data_root() / "bef"


def migrations_dir() -> Path:
    return bundle_root() / "db" / "migrations"


def bef_config_path() -> Path:
    return bundle_root() / "config" / "bef_sources.yaml"
