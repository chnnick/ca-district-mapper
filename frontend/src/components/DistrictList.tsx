import { useEffect, useState } from "react";
import { fetchDistricts } from "../api/client";
import type { DistrictRow, DistrictType } from "../types";

const DISTRICT_TYPES: Record<DistrictType, string> = {
  CD: "Congressional District",
  SD: "State District",
  AD: "Assembly District",
  BOE: "Board of Education District",
};

interface Props {
  selectedType: DistrictType;
  selectedDistrict: string | null;
  onSelectType: (type: DistrictType) => void;
  onSelectDistrict: (districtNumber: string) => void;
  refreshKey: number;
}

export default function DistrictList({
  selectedType,
  selectedDistrict,
  onSelectType,
  onSelectDistrict,
  refreshKey,
}: Props) {
  const [rows, setRows] = useState<DistrictRow[]>([]);

  useEffect(() => {
    fetchDistricts(selectedType)
      .then(setRows)
      .catch(() => setRows([]));
  }, [selectedType, refreshKey]);

  const filtered = rows.filter((r) => r.district_type === selectedType);

  return (
    <div className="district-tabs">
      <div className="tab-bar">
        {(Object.keys(DISTRICT_TYPES) as DistrictType[]).map((t) => (
          <button
            key={t}
            className={t === selectedType ? "active" : ""}
            title={DISTRICT_TYPES[t]}
            onClick={() => onSelectType(t)}
          >
            {t}
          </button>
        ))}
      </div>
      <div className="district-list">
        {filtered.length === 0 ? (
          <div className="empty-state">No data</div>
        ) : (
          filtered.map((row) => (
            <div
              key={row.district_number}
              className={`district-row${selectedDistrict === row.district_number ? " selected" : ""}`}
              onClick={() => onSelectDistrict(row.district_number)}
            >
              <span className="label">
                {row.district_type} {row.district_number}
              </span>
              <span className="count">
                {row.constituent_count.toLocaleString()}
              </span>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
