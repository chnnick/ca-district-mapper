import json

from fastapi import APIRouter, Depends, HTTPException

from src.api.deps import get_db

router = APIRouter()


@router.get("/jobs/{job_id}")
def get_job(job_id: str, conn=Depends(get_db)):
    row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if row is None:
        raise HTTPException(404, detail="Job not found")
    return _serialize(row)


@router.get("/jobs")
def list_jobs(limit: int = 20, conn=Depends(get_db)):
    rows = conn.execute(
        "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    return [_serialize(r) for r in rows]


def _serialize(row) -> dict:
    d = dict(row)
    for field in ("ingest_summary", "geocode_summary", "match_summary"):
        raw = d.get(field)
        if raw:
            try:
                d[field] = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                pass
    return d
