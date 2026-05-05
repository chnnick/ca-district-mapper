from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from src.api.routes import jobs, map, reports, uploads
from src.db import apply_migrations


def create_app(
    db_path: str | Path = "data/district_mapper.db",
    raw_dir: str | Path = "data/raw",
) -> FastAPI:
    db_path = str(db_path)
    raw_dir = str(raw_dir)
    migrations_dir = Path(__file__).parent.parent.parent / "db" / "migrations"

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        apply_migrations(db_path, migrations_dir)
        yield

    app = FastAPI(title="cal-district-mapper", lifespan=lifespan)
    app.state.db_path = db_path
    app.state.raw_dir = raw_dir

    app.include_router(uploads.router, prefix="/api")
    app.include_router(jobs.router, prefix="/api")
    app.include_router(reports.router, prefix="/api")
    app.include_router(map.router, prefix="/api")

    frontend_dist = Path(__file__).parent.parent.parent / "frontend" / "dist"
    if frontend_dist.exists():
        app.mount("/", StaticFiles(directory=str(frontend_dist), html=True), name="static")

    return app
