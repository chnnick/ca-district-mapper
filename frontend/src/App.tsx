import { useCallback, useEffect, useState } from "react";
import { fetchMapPoints } from "./api/client";
import DistrictChart from "./components/DistrictChart";
import DistrictList from "./components/DistrictList";
import MapView from "./components/MapView";
import Sidebar from "./components/Sidebar";
import StatsPanel from "./components/StatsPanel";
import UploadPanel from "./components/UploadPanel";
import type { DistrictType, MapPoint } from "./types";

export default function App() {
  const [selectedType, setSelectedType] = useState<DistrictType>("CD");
  const [selectedDistrict, setSelectedDistrict] = useState<string | null>(null);
  const [mapPoints, setMapPoints] = useState<MapPoint[]>([]);
  const [refreshKey, setRefreshKey] = useState(0);

  const loadPoints = useCallback(async () => {
    try {
      if (selectedDistrict) {
        const pts = await fetchMapPoints(selectedType, selectedDistrict);
        setMapPoints(pts);
      } else {
        const pts = await fetchMapPoints();
        setMapPoints(pts);
      }
    } catch {
      setMapPoints([]);
    }
  }, [selectedType, selectedDistrict]);

  useEffect(() => {
    void loadPoints();
  }, [loadPoints, refreshKey]);

  const handleUploadDone = () => setRefreshKey((k) => k + 1);

  const handleSelectDistrict = (districtNumber: string) => {
    setSelectedDistrict((prev) =>
      prev === districtNumber ? null : districtNumber,
    );
  };

  const handleSelectType = (type: DistrictType) => {
    setSelectedType(type);
    setSelectedDistrict(null);
  };

  return (
    <div className="app">
      <Sidebar>
        <UploadPanel onUploadDone={handleUploadDone} />
        <DistrictList
          selectedType={selectedType}
          selectedDistrict={selectedDistrict}
          onSelectType={handleSelectType}
          onSelectDistrict={handleSelectDistrict}
          refreshKey={refreshKey}
        />
        {selectedDistrict && (
          <StatsPanel
            districtType={selectedType}
            districtNumber={selectedDistrict}
          />
        )}
        <DistrictChart districtType={selectedType} refreshKey={refreshKey} />
      </Sidebar>
      <div className="map-container">
        <MapView points={mapPoints} />
      </div>
    </div>
  );
}
