import json
from datetime import datetime, timezone

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


@router.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: str, conn=Depends(get_db)):
    """
    Mark an in-progress job as failed. The background pipeline guards its own
    writes against terminal status, so any in-flight geocode/match step that
    finishes after this point will not overwrite the cancellation.
    """
    row = conn.execute(
        "SELECT id, status FROM jobs WHERE id = ?", (job_id,)
    ).fetchone()
    if row is None:
        raise HTTPException(404, detail="Job not found")
    if row["status"] not in ("geocoding", "matching"):
        raise HTTPException(
            409,
            detail=f"Job {job_id} is not running (status={row['status']}).",
        )

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn.execute(
        "UPDATE jobs SET status='failed', error=?, finished_at=? WHERE id=?",
        ("Cancelled by client (timeout)", now, job_id),
    )
    return {"job_id": job_id, "status": "failed"}


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
