import { useState } from "react";
import { fetchPersonDistricts } from "../api/client";
import type { DistrictType, MapPoint, PersonDistricts } from "../types";

const DISTRICT_ORDER: DistrictType[] = ["CD", "SD", "AD", "BOE"];

interface Props {
  onPersonLookup?: (point: MapPoint | null) => void;
}

export default function PersonLookup({ onPersonLookup }: Props) {
  const [id, setId] = useState("");
  const [result, setResult] = useState<PersonDistricts | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [lookedUpId, setLookedUpId] = useState<string | null>(null);

  const handleLookup = async () => {
    const trimmed = id.trim();
    if (!trimmed) return;
    setError(null);
    setResult(null);
    setLookedUpId(trimmed);
    setLoading(true);
    try {
      const r = await fetchPersonDistricts(trimmed);
      setResult(r);
      if (r.lat !== undefined && r.lng !== undefined) {
        onPersonLookup?.({ lat: r.lat, lng: r.lng });
      } else {
        onPersonLookup?.(null);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Lookup failed");
      onPersonLookup?.(null);
    } finally {
      setLoading(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter") void handleLookup();
  };

  return (
    <div className="upload-panel">
      <h3>Look Up By ID</h3>
      <input
        type="text"
        placeholder="Enter person's id to query for their districts"
        value={id}
        onChange={(e) => setId(e.target.value)}
        onKeyDown={handleKeyDown}
      />
      <button
        onClick={() => void handleLookup()}
        disabled={!id.trim() || loading}
      >
        {loading ? "Looking up…" : "Look up"}
      </button>

      {error && (
        <div className="job-status failed">
          {error.includes("404")
            ? `No person found with id "${lookedUpId}"`
            : error}
        </div>
      )}

      {result?.status === "not_geocoded" && (
        <div className="job-status">Address found but not yet geocoded.</div>
      )}

      {result && (result.status === "ok" || result.status === "partial") && (
        <div className="person-districts">
          {result.status === "partial" && (
            <div className="job-status">Some districts unavailable.</div>
          )}
          <ul>
            {DISTRICT_ORDER.map((dt) => (
              <li key={dt}>
                <span className="district-label">{dt}</span>
                <span className="district-value">
                  {result.districts?.[dt] ?? "—"}
                </span>
              </li>
            ))}
          </ul>
          {result.lat !== undefined && result.lng !== undefined && (
            <div className="person-coords">
              {result.lat.toFixed(5)}, {result.lng.toFixed(5)}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
