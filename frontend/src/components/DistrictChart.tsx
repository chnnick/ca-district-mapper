import { useEffect, useState } from "react";
import {
  Bar,
  BarChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { fetchDistricts } from "../api/client";
import type { DistrictRow, DistrictType } from "../types";

interface Props {
  districtType: DistrictType;
  refreshKey: number;
}

export default function DistrictChart({ districtType, refreshKey }: Props) {
  const [data, setData] = useState<DistrictRow[] | null>(null);
  const [error, setError] = useState(false);

  useEffect(() => {
    setData(null);
    setError(false);
    fetchDistricts(districtType)
      .then(setData)
      .catch(() => setError(true));
  }, [districtType, refreshKey]);

  if (error) return null;
  if (!data) return <div className="empty-state">Loading…</div>;
  if (data.length === 0) return null;

  return (
    <div className="district-chart">
      <h3>{districtType} Distribution</h3>
      <ResponsiveContainer width="100%" height={180}>
        <BarChart data={data} margin={{ top: 4, right: 8, bottom: 4, left: 0 }}>
          <XAxis dataKey="district_number" tick={{ fontSize: 11 }} />
          <YAxis tick={{ fontSize: 11 }} width={30} />
          <Tooltip
            formatter={(v: number) => [v.toLocaleString(), "Constituents"]}
            labelFormatter={(l: string) => `District ${l}`}
          />
          <Bar dataKey="constituent_count" fill="#1a73e8" radius={[2, 2, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
