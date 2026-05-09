import { useEffect, useState } from "react";
import { deleteUpload, fetchUploads, uploadDownloadUrl } from "../api/client";
import type { UploadHistoryEntry } from "../types";

interface Props {
  refreshKey: number;
  onChange: () => void;
}

export default function UploadHistory({ refreshKey, onChange }: Props) {
  const [entries, setEntries] = useState<UploadHistoryEntry[] | null>(null);
  const [pendingDelete, setPendingDelete] = useState<UploadHistoryEntry | null>(
    null,
  );
  const [deleting, setDeleting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    fetchUploads()
      .then((data) => {
        if (!cancelled) setEntries(data);
      })
      .catch(() => {
        if (!cancelled) setEntries([]);
      });
    return () => {
      cancelled = true;
    };
  }, [refreshKey]);

  const handleConfirmDelete = async () => {
    if (!pendingDelete) return;
    setDeleting(true);
    setError(null);
    try {
      await deleteUpload(pendingDelete.source_file);
      setPendingDelete(null);
      onChange();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Delete failed");
    } finally {
      setDeleting(false);
    }
  };

  return (
    <div className="upload-history">
      <h3>Uploaded CSVs</h3>
      {entries === null && <div className="empty-state">Loading…</div>}
      {entries !== null && entries.length === 0 && (
        <div className="empty-state">No uploads yet.</div>
      )}
      {entries !== null && entries.length > 0 && (
        <ul className="upload-history-list">
          {entries.map((entry) => (
            <UploadRow
              key={entry.source_file}
              entry={entry}
              onRequestDelete={() => setPendingDelete(entry)}
            />
          ))}
        </ul>
      )}

      {pendingDelete && (
        <div className="confirm-dialog">
          <div className="confirm-text">
            Delete <strong>{pendingDelete.source_file}</strong>? This will
            remove {pendingDelete.row_count.toLocaleString()} address
            {pendingDelete.row_count === 1 ? "" : "es"} and their pins
            permanently.
          </div>
          {error && <div className="job-status failed">{error}</div>}
          <div className="confirm-buttons">
            <button
              className="btn-secondary"
              onClick={() => {
                setPendingDelete(null);
                setError(null);
              }}
              disabled={deleting}
            >
              Cancel
            </button>
            <button
              className="btn-danger"
              onClick={() => void handleConfirmDelete()}
              disabled={deleting}
            >
              {deleting ? "Deleting…" : "Delete"}
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

interface RowProps {
  entry: UploadHistoryEntry;
  onRequestDelete: () => void;
}

function UploadRow({ entry, onRequestDelete }: RowProps) {
  const downloadDisabled = !entry.has_raw_data;
  const purgeDate =
    entry.retention_purge_after ??
    estimatePurgeDate(entry.last_uploaded_at);
  const downloadTitle = downloadDisabled
    ? `Raw data purged${purgeDate ? ` on ${formatDate(purgeDate)}` : ""} per retention policy.`
    : `Download ${entry.source_file}`;

  return (
    <li className="upload-history-row">
      <div className="upload-history-meta">
        <div className="upload-history-name" title={entry.source_file}>
          {entry.source_file}
        </div>
        <div className="upload-history-sub">
          {formatRelative(entry.last_uploaded_at)} ·{" "}
          {entry.row_count.toLocaleString()} addr
        </div>
      </div>
      <div className="upload-history-actions">
        {downloadDisabled ? (
          <span
            className="upload-history-btn disabled"
            title={downloadTitle}
            aria-label="Download disabled"
          >
            ↓
          </span>
        ) : (
          <a
            className="upload-history-btn"
            href={uploadDownloadUrl(entry.source_file)}
            download={entry.source_file}
            title={downloadTitle}
            aria-label={`Download ${entry.source_file}`}
          >
            ↓
          </a>
        )}
        <button
          className="upload-history-btn"
          onClick={onRequestDelete}
          title={`Delete ${entry.source_file}`}
          aria-label={`Delete ${entry.source_file}`}
        >
          ✕
        </button>
      </div>
    </li>
  );
}

function formatRelative(iso: string): string {
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return iso;
  const diffSec = (Date.now() - then) / 1000;
  if (diffSec < 60) return "just now";
  if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m ago`;
  if (diffSec < 86400) return `${Math.floor(diffSec / 3600)}h ago`;
  const days = Math.floor(diffSec / 86400);
  if (days < 30) return `${days}d ago`;
  return new Date(iso).toLocaleDateString();
}

function formatDate(iso: string): string {
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleDateString();
}

function estimatePurgeDate(uploadedAt: string): string | null {
  const t = new Date(uploadedAt).getTime();
  if (Number.isNaN(t)) return null;
  return new Date(t + 90 * 24 * 60 * 60 * 1000).toISOString();
}
