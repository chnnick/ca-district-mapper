import csv
import io
import json
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, UploadFile
from fastapi.responses import StreamingResponse

from src.api.deps import get_db
from src.db import get_connection
from src.geocode import run_geocoding
from src.ingest import load_csv
from src.match import run_assignment

router = APIRouter()

# Module-level lock prevents two uploads from both passing the "no running job"
# check before either has committed its job row.
_upload_lock = threading.Lock()


@router.post("/uploads", status_code=202)
def upload_csv(
    request: Request,
    file: UploadFile,
    background_tasks: BackgroundTasks,
    conn=Depends(get_db),
):
    """
    Accept a CSV upload, run ingest synchronously, then queue geocode+match
    as a background job. Returns immediately with job_id to poll via GET /jobs/{id}.

    409 if a job is already running (jobs are serialized).
    """
    if not (file.filename or "").lower().endswith(".csv"):
        raise HTTPException(400, detail="File must be a .csv")

    with _upload_lock:
        running = conn.execute(
            "SELECT id FROM jobs WHERE status IN ('geocoding', 'matching')"
        ).fetchone()
        if running:
            raise HTTPException(
                409,
                detail=f"Job {running['id']} is already running. Poll GET /jobs/{running['id']} for status.",
            )

        raw_dir = Path(request.app.state.raw_dir)
        raw_dir.mkdir(parents=True, exist_ok=True)
        dest = raw_dir / file.filename

        with open(dest, "wb") as f:
            f.write(file.file.read())

        try:
            ingest_result = load_csv(dest, conn)
        except ValueError as exc:
            raise HTTPException(422, detail=str(exc))

        # Fail only when rows were explicitly rejected by validation and nothing
        # was loaded or already present. If all rows are duplicates, proceed —
        # the geocode/match steps are idempotent and will process existing data.
        if ingest_result.loaded == 0 and ingest_result.duplicates_skipped == 0 and ingest_result.total_rows > 0:
            raise HTTPException(
                422,
                detail={
                    "message": "No valid rows loaded — all rows were rejected.",
                    "errors": ingest_result.errors,
                },
            )

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        job_id = str(uuid.uuid4())
        ingest_summary = {
            "loaded": ingest_result.loaded,
            "rejected": ingest_result.rejected,
            "duplicates_skipped": ingest_result.duplicates_skipped,
            "errors": ingest_result.errors,
        }
        conn.execute(
            """
            INSERT INTO jobs (id, status, source_file, ingest_summary, created_at)
            VALUES (?, 'geocoding', ?, ?, ?)
            """,
            (job_id, file.filename, json.dumps(ingest_summary), now),
        )
        # Commit now so the background task can see the job row. FastAPI runs
        # background tasks before Depends(get_db) commits, so without this the
        # INSERT is invisible when _run_pipeline tries to UPDATE it.
        conn.commit()

    db_path = request.app.state.db_path
    background_tasks.add_task(_run_pipeline, db_path, job_id)

    return {
        "job_id": job_id,
        "ingest": ingest_summary,
    }


def _run_pipeline(db_path: str, job_id: str) -> None:
    """Background task: geocode → match. Updates job status at each stage."""
    now = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def set_status(status: str, **fields):
        with get_connection(db_path) as c:
            if fields:
                cols = ", ".join(f"{k}=?" for k in fields)
                c.execute(
                    f"UPDATE jobs SET status=?, {cols} WHERE id=?",
                    [status, *fields.values(), job_id],
                )
            else:
                c.execute("UPDATE jobs SET status=? WHERE id=?", [status, job_id])

    try:
        set_status("geocoding", started_at=now())

        with get_connection(db_path) as conn:
            geocode_summary = run_geocoding(conn)

        set_status("matching", geocode_summary=json.dumps(geocode_summary))

        with get_connection(db_path) as conn:
            match_summary = run_assignment(conn)

        set_status("done", match_summary=json.dumps(match_summary), finished_at=now())

    except Exception as exc:
        try:
            set_status("failed", error=str(exc), finished_at=now())
        except Exception:
            pass


# ── Upload history: list, download, delete ────────────────────────────────────


@router.get("/uploads")
def list_uploads(conn=Depends(get_db)):
    """
    Aggregate uploads by source_file across the jobs and raw_addresses tables.
    Returns one entry per filename with the latest upload time, current row
    count, and whether raw data is still on hand (false after retention purge).
    """
    rows = conn.execute(
        """
        SELECT
            j.source_file                               AS source_file,
            MAX(j.created_at)                           AS last_uploaded_at,
            COUNT(DISTINCT r.address_hash)              AS row_count,
            MIN(r.retention_purge_after)                AS retention_purge_after
        FROM jobs j
        LEFT JOIN raw_addresses r ON r.source_file = j.source_file
        GROUP BY j.source_file
        ORDER BY last_uploaded_at DESC
        """
    ).fetchall()

    return [
        {
            "source_file": r["source_file"],
            "last_uploaded_at": r["last_uploaded_at"],
            "row_count": r["row_count"],
            "has_raw_data": r["row_count"] > 0,
            "retention_purge_after": r["retention_purge_after"],
        }
        for r in rows
    ]


@router.get("/uploads/{source_file}/download")
def download_upload(source_file: str, conn=Depends(get_db)):
    """
    Reconstruct the original CSV from raw_addresses for the given source_file.
    Returns 410 if no rows remain (purged per retention policy).
    """
    rows = conn.execute(
        "SELECT id, street, city, state, zip FROM raw_addresses "
        "WHERE source_file = ? ORDER BY id",
        (source_file,),
    ).fetchall()
    if not rows:
        raise HTTPException(410, detail="Raw data purged per retention policy")

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["id", "street", "city", "state", "zip"])
    writer.writeheader()
    for r in rows:
        writer.writerow({k: r[k] for k in ("id", "street", "city", "state", "zip")})

    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{source_file}"'},
    )


@router.delete("/uploads/{source_file}")
def delete_upload(source_file: str, request: Request, conn=Depends(get_db)):
    """
    Hard-delete all data for an uploaded CSV: raw_addresses, geocoded_records,
    district_assignments, geocode_misses, jobs row, and the staged raw file.
    Refuses while a job for that source_file is still running.
    """
    with _upload_lock:
        running = conn.execute(
            "SELECT id FROM jobs "
            "WHERE source_file = ? AND status IN ('geocoding', 'matching')",
            (source_file,),
        ).fetchone()
        if running:
            raise HTTPException(
                409,
                detail=f"Job {running['id']} is still running for {source_file}.",
            )

        hashes = [
            r["address_hash"]
            for r in conn.execute(
                "SELECT address_hash FROM raw_addresses WHERE source_file = ?",
                (source_file,),
            )
        ]

        # Confirm there's something to delete (either rows or a jobs entry)
        job_exists = conn.execute(
            "SELECT 1 FROM jobs WHERE source_file = ? LIMIT 1", (source_file,)
        ).fetchone()
        if not hashes and not job_exists:
            raise HTTPException(404, detail=f"No upload found for {source_file}")

        if hashes:
            placeholders = ",".join("?" * len(hashes))
            conn.execute(
                f"DELETE FROM district_assignments WHERE address_hash IN ({placeholders})",
                hashes,
            )
            conn.execute(
                f"DELETE FROM geocoded_records   WHERE address_hash IN ({placeholders})",
                hashes,
            )
            conn.execute(
                f"DELETE FROM geocode_misses     WHERE address_hash IN ({placeholders})",
                hashes,
            )
        conn.execute("DELETE FROM raw_addresses WHERE source_file = ?", (source_file,))
        conn.execute("DELETE FROM jobs          WHERE source_file = ?", (source_file,))

    raw_file = Path(request.app.state.raw_dir) / source_file
    try:
        raw_file.unlink(missing_ok=True)
    except OSError:
        pass

    return {"deleted": {"source_file": source_file, "rows": len(hashes)}}
