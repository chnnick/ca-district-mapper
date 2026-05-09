import logging
import threading
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from src.api.routes import jobs, map, people, reports, uploads
from src.db import apply_migrations, get_connection

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).parent.parent.parent


def _has_active_bef(db_path: str) -> bool:
    """True if at least one currently-effective, approved BEF version exists.

    The approved_by gate matches what get_active_bef_version_id requires, so an
    "active but unapproved" row will not falsely satisfy this check and skip
    the in-process auto-load — that mismatch is what produces silent 0-row
    assignment runs.
    """
    with get_connection(db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM bef_versions "
            "WHERE expiration_date IS NULL AND approved_by IS NOT NULL LIMIT 1"
        ).fetchone()
    return row is not None


def _log_bef_status(db_path: str) -> None:
    """Log a diagnostic summary of loaded BEF versions and block counts."""
    with get_connection(db_path) as conn:
        versions = conn.execute(
            """
            SELECT id, bef_source_id, district_type, effective_date, expiration_date,
                   approved_by, downloaded_at
            FROM bef_versions
            ORDER BY district_type, effective_date
            """
        ).fetchall()

        if not versions:
            logger.warning("BEF status: no versions in bef_versions table — reports will return empty")
            return

        logger.info("BEF status: %d version(s) in database:", len(versions))
        for v in versions:
            block_count = conn.execute(
                "SELECT COUNT(*) FROM bef_blocks WHERE bef_version_id = ?", (v["id"],)
            ).fetchone()[0]
            status = "ACTIVE" if v["expiration_date"] is None else f"superseded (expires {v['expiration_date']})"
            approved = v["approved_by"] or "NULL (will be ignored by reports!)"
            logger.info(
                "  [%s] id=%d source=%s district_type=%s effective=%s blocks=%d approved_by=%s",
                status, v["id"], v["bef_source_id"], v["district_type"],
                v["effective_date"], block_count, approved,
            )

        unapproved = [v for v in versions if v["approved_by"] is None]
        if unapproved:
            logger.warning(
                "BEF status: %d version(s) have approved_by=NULL — "
                "get_active_bef_version_id requires approved_by IS NOT NULL; "
                "run scripts/load_bef.py --approved-by 'Your Name' to fix",
                len(unapproved),
            )

        assignment_count = conn.execute(
            "SELECT COUNT(*) FROM district_assignments"
        ).fetchone()[0]
        geocoded_count = conn.execute(
            "SELECT COUNT(*) FROM geocoded_records"
        ).fetchone()[0]
        logger.info(
            "BEF status: geocoded_records=%d  district_assignments=%d",
            geocoded_count, assignment_count,
        )
        if geocoded_count > 0 and assignment_count == 0:
            logger.warning(
                "BEF status: addresses are geocoded but district_assignments is empty — "
                "run assignment step to populate reports"
            )


def _auto_load_bef(db_path: str) -> None:
    from src.match.bef_config import load_bef_sources
    from src.match.bef_loader import (
        download_bef,
        get_current_file_hash,
        hash_file,
        load_bef_version,
        verify_url_reachable,
    )

    config_path = _PROJECT_ROOT / "config" / "bef_sources.yaml"
    bef_dir = _PROJECT_ROOT / "data" / "bef"
    bef_dir.mkdir(parents=True, exist_ok=True)

    try:
        sources = [s for s in load_bef_sources(config_path) if s.expiration_date is None]
    except Exception as exc:
        logger.error("BEF auto-load: failed to read config: %s", exc)
        return

    for source in sources:
        local_path = bef_dir / source.local_filename
        try:
            if local_path.exists():
                file_hash = hash_file(local_path)
            else:
                logger.info("BEF auto-load: downloading %s", source.id)
                if not verify_url_reachable(source.url):
                    logger.warning("BEF auto-load: URL unreachable for %s, skipping", source.id)
                    continue
                file_hash = download_bef(source.url, local_path)

            with get_connection(db_path) as conn:
                if get_current_file_hash(conn, source.id) == file_hash:
                    continue
                version_id, block_count = load_bef_version(conn, source, local_path, file_hash, approved_by="auto-load")
            logger.info("BEF auto-load: loaded %s (%d blocks, version_id=%d)", source.id, block_count, version_id)
        except Exception as exc:
            logger.error("BEF auto-load: failed for %s: %s", source.id, exc)


def create_app(
    db_path: str | Path = "data/district_mapper.db",
    raw_dir: str | Path = "data/raw",
) -> FastAPI:
    db_path = str(db_path)
    raw_dir = str(raw_dir)
    migrations_dir = _PROJECT_ROOT / "db" / "migrations"

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        apply_migrations(db_path, migrations_dir)
        if not _has_active_bef(db_path):
            logger.info("No active BEF found — starting background load")
            threading.Thread(target=_auto_load_bef, args=(db_path,), daemon=True).start()
        else:
            _log_bef_status(db_path)
        yield

    app = FastAPI(title="cal-district-mapper", lifespan=lifespan)
    app.state.db_path = db_path
    app.state.raw_dir = raw_dir

    app.include_router(uploads.router, prefix="/api")
    app.include_router(jobs.router, prefix="/api")
    app.include_router(reports.router, prefix="/api")
    app.include_router(map.router, prefix="/api")
    app.include_router(people.router, prefix="/api")

    frontend_dist = Path(__file__).parent.parent.parent / "frontend" / "dist"
    if frontend_dist.exists():
        app.mount("/", StaticFiles(directory=str(frontend_dist), html=True), name="static")

    return app
