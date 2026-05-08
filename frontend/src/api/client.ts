import type {
  DistrictRow,
  DistrictStats,
  DistrictType,
  Job,
  MapPoint,
  PersonDistricts,
  UploadResponse,
} from "../types";

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`/api${path}`, init);
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
  const res = await fetch("/api/uploads", { method: "POST", body: form });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`${res.status} ${res.statusText}: ${body}`);
  }
  return res.json() as Promise<UploadResponse>;
}

export function fetchJob(jobId: string): Promise<Job> {
  return apiFetch<Job>(`/jobs/${jobId}`);
}

export function fetchPersonDistricts(id: string): Promise<PersonDistricts> {
  return apiFetch<PersonDistricts>(
    `/people/${encodeURIComponent(id)}/districts`,
  );
}
