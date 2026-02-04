import { MapContainer, TileLayer } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import { useEffect } from 'react';
import clsx from 'clsx';

// Placeholder GeoJSON or fetch from API
// In real app, fetch from /api/geo/lgas
// Here we might mock or use a simple box for demo if actual GeoJSON is missing from frontend assets

interface MapCanvasProps {
    year: number;
    highlightMetric: string; // e.g., 'gini', 'emissions'
    onLgaSelect: (lgaId: number) => void;
    className?: string;
}

const MapCanvas = ({ year, highlightMetric, onLgaSelect, className }: MapCanvasProps) => {
    // Placeholder for GeoJSON data
    // const [geoData, setGeoData] = useState<any>(null);

    // Mock usage to satisfy linter until real GeoJSON is implemented
    useEffect(() => {
        // In real app: fetch data
    }, []);

    const handleMockClick = () => {
        onLgaSelect(1); // Simulating selection of LGA ID 1
    };

    return (
        <div className={clsx("relative w-full h-full rounded-xl overflow-hidden glass-panel border border-gray-700", className)} onClick={handleMockClick}>
            <MapContainer
                center={[-37.8136, 144.9631]} // Melbourne
                zoom={8}
                className="w-full h-full"
                zoomControl={false}
            >
                <TileLayer
                    attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
                    url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                />
                {/* GeoJSON layer would go here with style function based on year/metric */}
            </MapContainer>

            <div className="absolute top-4 right-4 z-[1000] bg-cyber-slate/90 p-2 rounded text-xs text-neon-teal">
                Layer: {highlightMetric} ({year})
            </div>
        </div>
    );
};

export default MapCanvas;
