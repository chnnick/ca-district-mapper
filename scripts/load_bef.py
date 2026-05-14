"""
Load Block Equivalency Files (BEF) into the district_mapper database.

Downloads each BEF ZIP listed in config/bef_sources.yaml, verifies the
SHA-256 hash, and bulk-inserts block rows into bef_blocks. Already-loaded
versions (same hash) are skipped. Superseded versions are skipped by default.

Usage:
    python scripts/load_bef.py --approved-by "Your Name"
    python scripts/load_bef.py --approved-by "Your Name" --include-superseded
    python scripts/load_bef.py --approved-by "Your Name" --dry-run
"""

import argparse
import sys
from pathlib import Path

# Allow running from the project root without installing the package.
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db import apply_migrations, get_connection
from src.match import (
    download_bef,
    get_current_file_hash,
    load_bef_version,
    verify_url_reachable,
)
from src.match.bef_config import load_bef_sources
from src.paths import bef_dir as _resolve_bef_dir
from src.paths import db_path as _resolve_db_path

PROJECT_ROOT = Path(__file__).parent.parent
DEFAULT_DB = _resolve_db_path()
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "bef_sources.yaml"
DEFAULT_BEF_DIR = _resolve_bef_dir()
MIGRATIONS_DIR = PROJECT_ROOT / "db" / "migrations"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--approved-by", required=True, metavar="NAME",
                   help="Name to store in bef_versions.approved_by. Required: "
                        "rows with NULL approved_by are silently ignored by the matcher.")
    p.add_argument("--db", default=str(DEFAULT_DB), metavar="PATH",
                   help=f"Path to SQLite database (default: {DEFAULT_DB})")
    p.add_argument("--config", default=str(DEFAULT_CONFIG), metavar="PATH",
                   help=f"Path to bef_sources.yaml (default: {DEFAULT_CONFIG})")
    p.add_argument("--bef-dir", default=str(DEFAULT_BEF_DIR), metavar="PATH",
                   help=f"Directory to store downloaded BEF ZIPs (default: {DEFAULT_BEF_DIR})")
    p.add_argument("--include-superseded", action="store_true",
                   help="Also load BEF versions that have an expiration_date (superseded versions)")
    p.add_argument("--dry-run", action="store_true",
                   help="Print what would happen without downloading or writing anything")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    db_path = Path(args.db)
    bef_dir = Path(args.bef_dir)
    config_path = Path(args.config)

    print(f"DB:     {db_path}")
    print(f"Config: {config_path}")
    print(f"BEFdir: {bef_dir}")
    print(f"Approved by: {args.approved_by}")
    if args.dry_run:
        print("[DRY RUN — no files will be written]\n")
    else:
        print()

    sources = load_bef_sources(config_path)

    active = [s for s in sources if s.expiration_date is None]
    superseded = [s for s in sources if s.expiration_date is not None]

    to_load = active + (superseded if args.include_superseded else [])

    if superseded and not args.include_superseded:
        ids = ", ".join(s.id for s in superseded)
        print(f"Skipping {len(superseded)} superseded version(s): {ids}")
        print("  Pass --include-superseded to load them for historical point-in-time queries.\n")

    if not args.dry_run:
        apply_migrations(db_path, MIGRATIONS_DIR)

    results = {"loaded": 0, "skipped": 0, "failed": 0}

    from src.match.bef_loader import hash_file

    for source in to_load:
        print(f"── {source.id}  ({source.district_type}, effective {source.effective_date})")

        local_path = bef_dir / source.local_filename

        # Use the local file if it already exists; download otherwise.
        if local_path.exists():
            print(f"   Local file found: {local_path.name}")
            if args.dry_run:
                print("   [dry run] Would hash local file and check against DB\n")
                continue
            file_hash = hash_file(local_path)
        else:
            print(f"   Checking URL: {source.url}")
            if not verify_url_reachable(source.url):
                print(f"   ERROR: URL not reachable — skipping.\n")
                results["failed"] += 1
                continue

            if args.dry_run:
                print(f"   [dry run] Would download to {local_path}\n")
                continue

            print(f"   Downloading...")
            try:
                file_hash = download_bef(source.url, local_path)
            except Exception as exc:
                print(f"   ERROR: Download failed — {exc}\n")
                results["failed"] += 1
                continue
            print(f"   Saved to {local_path.name}  sha256={file_hash[:16]}...")

        # Each source gets its own connection so a failure rolls back only that source,
        # not every source loaded so far.
        with get_connection(db_path) as conn:
            current_hash = get_current_file_hash(conn, source.id)
            if current_hash == file_hash:
                print(f"   Already loaded (hash matches) — skipping.\n")
                results["skipped"] += 1
                continue

            print(f"   Loading blocks into DB...")
            try:
                version_id, block_count = load_bef_version(
                    conn, source, local_path, file_hash, args.approved_by
                )
            except ValueError as exc:
                print(f"   ERROR: {exc}\n")
                results["failed"] += 1
                continue

        print(f"   Loaded {block_count:,} blocks  (bef_version_id={version_id})\n")
        results["loaded"] += 1

    print("─" * 50)
    print(f"Done.  loaded={results['loaded']}  skipped={results['skipped']}  failed={results['failed']}")
    if results["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
