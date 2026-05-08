export type DistrictType = "CD" | "SD" | "AD" | "BOE";

export interface DistrictRow {
  district_type: DistrictType;
  district_number: string;
  constituent_count: number;
}

export interface MapPoint {
  lat: number;
  lng: number;
}

export interface IngestSummary {
  loaded: number;
  rejected: number;
  duplicates_skipped: number;
  errors: string[];
}

export interface GeocodeSummary {
  geocoded: number;
  missed: number;
}

export interface MatchSummary {
  assigned: number;
}

export interface Job {
  id: string;
  status: "geocoding" | "matching" | "done" | "failed";
  source_file: string;
  ingest_summary: IngestSummary | null;
  geocode_summary: GeocodeSummary | null;
  match_summary: MatchSummary | null;
  error: string | null;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
}

export interface UploadResponse {
  job_id: string;
  ingest: IngestSummary;
}

export interface ZipRow {
  zip: string;
  constituent_count: number;
}

export interface DistrictStats {
  district_type: DistrictType;
  district_number: string;
  total: number;
  zip_breakdown: ZipRow[];
}

export interface PersonDistricts {
  id: string;
  lat?: number;
  lng?: number;
  status: "ok" | "partial" | "not_geocoded";
  districts?: Partial<Record<DistrictType, string>>;
}
