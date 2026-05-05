import { useEffect, useState } from "react";
import { fetchDistrictStats } from "../api/client";
import type { DistrictStats, DistrictType } from "../types";

interface Props {
  districtType: DistrictType;
  districtNumber: string;
}

export default function StatsPanel({ districtType, districtNumber }: Props) {
  const [stats, setStats] = useState<DistrictStats | null>(null);
  const [error, setError] = useState(false);

  useEffect(() => {
    setStats(null);
    setError(false);
    fetchDistrictStats(districtType, districtNumber)
      .then(setStats)
      .catch(() => setError(true));
  }, [districtType, districtNumber]);

  if (error) return null;
  if (!stats) return <div className="empty-state">Loading…</div>;

  return (
    <div className="stats-panel">
      <h3>
        {districtType} {districtNumber}
      </h3>
      <div className="stats-total">
        {stats.total.toLocaleString()}{" "}
        <span>constituents</span>
      </div>
      <table className="zip-table">
        <thead>
          <tr>
            <th>ZIP</th>
            <th>Count</th>
          </tr>
        </thead>
        <tbody>
          {stats.zip_breakdown.map((row) => (
            <tr key={row.zip}>
              <td>{row.zip}</td>
              <td>{row.constituent_count.toLocaleString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
