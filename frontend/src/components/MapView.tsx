import "leaflet/dist/leaflet.css";
import { useEffect } from "react";
import { CircleMarker, MapContainer, TileLayer, useMap } from "react-leaflet";
import type { MapPoint } from "../types";
import type { LatLngBoundsExpression } from "leaflet";

interface FitBoundsProps {
  points: MapPoint[];
}

function FitBounds({ points }: FitBoundsProps) {
  const map = useMap();
  useEffect(() => {
    if (points.length === 0) return;
    const bounds: LatLngBoundsExpression = points.map((p) => [p.lat, p.lng]);
    map.fitBounds(bounds, { padding: [40, 40], maxZoom: 13 });
  }, [map, points]);
  return null;
}

interface Props {
  points: MapPoint[];
}

export default function MapView({ points }: Props) {
  return (
    <MapContainer
      center={[37.5, -119.5]}
      zoom={6}
      style={{ height: "100%", width: "100%" }}
    >
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />
      {points.map((p, i) => (
        <CircleMarker
          key={i}
          center={[p.lat, p.lng]}
          radius={5}
          pathOptions={{ color: "#1a73e8", fillColor: "#1a73e8", fillOpacity: 0.7, weight: 1 }}
        />
      ))}
      <FitBounds points={points} />
    </MapContainer>
  );
}
