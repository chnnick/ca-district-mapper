import type {
  DistrictRow,
  DistrictStats,
  DistrictType,
  Job,
  MapPoint,
  PersonDistricts,
  UploadHistoryEntry,
  UploadResponse,
} from "../types";

// In the Tauri shell, the Rust side injects window.__API_BASE__ pointing
// at the loopback sidecar (e.g. "http://127.0.0.1:53421"). Under Docker /
// `uvicorn`, FastAPI serves the SPA same-origin, so the empty base string
// keeps requests at "/api/...".
declare global {
  interface Window {
    __API_BASE__?: string;
  }
}
const API_BASE = window.__API_BASE__ ?? "";

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}/api${path}`, init);
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`${res.status} ${res.statusText}: ${body}`);
  }
  return res.json() as Promise<T>;
}

export function fetchDistricts(
  districtType?: DistrictType,
): Promise<DistrictRow[]> {
  const qs = districtType ? `?district_types=${districtType}` : "";
  return apiFetch<DistrictRow[]>(`/reports/legislators${qs}`);
}

export function fetchMapPoints(
  districtType?: DistrictType,
  districtNumber?: string,
): Promise<MapPoint[]> {
  if (districtType && districtNumber) {
    return apiFetch<MapPoint[]>(
      `/map/points?district_type=${districtType}&district_number=${districtNumber}`,
    );
  }
  return apiFetch<MapPoint[]>("/map/points");
}

export function fetchDistrictStats(
  districtType: DistrictType,
  districtNumber: string,
): Promise<DistrictStats> {
  return apiFetch<DistrictStats>(
    `/reports/legislators/${districtType}/${districtNumber}/stats`,
  );
}

export async function uploadCSV(file: File): Promise<UploadResponse> {
  const form = new FormData();
  form.append("file", file);
  const res = await fetch(`${API_BASE}/api/uploads`, {
    method: "POST",
    body: form,
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`${res.status} ${res.statusText}: ${body}`);
  }
  return res.json() as Promise<UploadResponse>;
}

export function fetchJob(jobId: string): Promise<Job> {
  return apiFetch<Job>(`/jobs/${jobId}`);
}

export function cancelJob(
  jobId: string,
): Promise<{ job_id: string; status: string }> {
  return apiFetch(`/jobs/${jobId}/cancel`, { method: "POST" });
}

export function fetchPersonDistricts(id: string): Promise<PersonDistricts> {
  return apiFetch<PersonDistricts>(
    `/people/${encodeURIComponent(id)}/districts`,
  );
}

export function fetchUploads(): Promise<UploadHistoryEntry[]> {
  return apiFetch<UploadHistoryEntry[]>("/uploads");
}

export function deleteUpload(
  sourceFile: string,
): Promise<{ deleted: { source_file: string; rows: number } }> {
  return apiFetch(`/uploads/${encodeURIComponent(sourceFile)}`, {
    method: "DELETE",
  });
}

export function uploadDownloadUrl(sourceFile: string): string {
  return `${API_BASE}/api/uploads/${encodeURIComponent(sourceFile)}/download`;
}
