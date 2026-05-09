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

export interface MatchByType {
  assigned: number;
  bef_version_id?: number;
  no_bef_match?: number;
  no_block_geoid?: number;
  no_active_bef?: boolean;
}

export interface MatchSummary {
  assigned: number;
  by_type: Partial<Record<DistrictType, MatchByType>>;
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

export interface UploadHistoryEntry {
  source_file: string;
  last_uploaded_at: string;
  row_count: number;
  has_raw_data: boolean;
  retention_purge_after: string | null;
}
