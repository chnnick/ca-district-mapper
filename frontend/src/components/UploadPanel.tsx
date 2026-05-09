import { useEffect, useRef, useState } from "react";
import { cancelJob, fetchJob, uploadCSV } from "../api/client";
import type { Job } from "../types";

interface Props {
  onUploadDone: () => void;
}

const UPLOAD_TIMEOUT_MS = 5 * 60 * 1000;

export default function UploadPanel({ onUploadDone }: Props) {
  const [file, setFile] = useState<File | null>(null);
  const [job, setJob] = useState<Job | null>(null);
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const stopPolling = () => {
    if (pollRef.current !== null) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
    if (timeoutRef.current !== null) {
      clearTimeout(timeoutRef.current);
      timeoutRef.current = null;
    }
  };

  useEffect(() => () => stopPolling(), []);

  const handleTimeout = async (jobId: string) => {
    stopPolling();
    try {
      await cancelJob(jobId);
    } catch {
      // Cancel may 404/409 if the job already finished — fall through.
    }
    setJob(null);
    setError(
      `Upload timed out after ${UPLOAD_TIMEOUT_MS / 60000} minutes — job cancelled.`,
    );
  };

  const startPolling = (jobId: string) => {
    stopPolling();
    timeoutRef.current = setTimeout(
      () => void handleTimeout(jobId),
      UPLOAD_TIMEOUT_MS,
    );
    pollRef.current = setInterval(async () => {
      try {
        const j = await fetchJob(jobId);
        setJob(j);
        if (j.status === "done" || j.status === "failed") {
          stopPolling();
          if (j.status === "done") onUploadDone();
        }
      } catch {
        stopPolling();
      }
    }, 2000);
  };

  const handleUpload = async () => {
    if (!file) return;
    setError(null);
    setJob(null);
    setUploading(true);
    try {
      const res = await uploadCSV(file);
      const initial: Job = {
        id: res.job_id,
        status: "geocoding",
        source_file: file.name,
        ingest_summary: res.ingest,
        geocode_summary: null,
        match_summary: null,
        error: null,
        created_at: new Date().toISOString(),
        started_at: null,
        finished_at: null,
      };
      setJob(initial);
      startPolling(res.job_id);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Upload failed");
    } finally {
      setUploading(false);
    }
  };

  const statusLabel = (j: Job) => {
    if (j.status === "done") {
      const g = j.geocode_summary;
      const m = j.match_summary;
      return `Done — geocoded: ${g?.geocoded ?? "?"}, assigned: ${m?.assigned ?? "?"}`;
    }
    if (j.status === "failed") return `Failed: ${j.error ?? "unknown error"}`;
    if (j.status === "matching") return "Matching districts…";
    return "Geocoding addresses…";
  };

  return (
    <div className="upload-panel">
      <h3>Upload CSV</h3>
      <input
        type="file"
        accept=".csv"
        onChange={(e) => setFile(e.target.files?.[0] ?? null)}
      />
      <button onClick={() => void handleUpload()} disabled={!file || uploading}>
        {uploading ? "Uploading…" : "Upload"}
      </button>
      {error && <div className="job-status failed">{error}</div>}
      {job && (
        <div className={`job-status ${job.status}`}>{statusLabel(job)}</div>
      )}
    </div>
  );
}
