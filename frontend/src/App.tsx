import { useState, useEffect } from 'react';
import MapCanvas from './components/MapCanvas';
import TimelineSlider from './components/TimelineSlider';
import MetricCard from './components/MetricCard';
import ScenarioPanel from './components/ScenarioPanel';
import { fetchStateStats } from './api/client';

function App() {
  const [year, setYear] = useState(2026);
  const [isPlaying, setIsPlaying] = useState(false);
  const [stats, setStats] = useState<any>(null);
  const [simParams, setSimParams] = useState({
    inflation: 2.5,
    migration: 50,
    housing_supply: 30,
    infra_investment: 60
  });

  useEffect(() => {
    // Load data when year changes
    fetchStateStats(year).then(setStats);
  }, [year]);

  useEffect(() => {
    let interval: any;
    if (isPlaying) {
      interval = setInterval(() => {
        setYear(y => (y >= 2026 ? 1976 : y + 1));
      }, 500);
    }
    return () => clearInterval(interval);
  }, [isPlaying]);

  const handleLgaSelect = (id: number) => {
    console.log("Selected LGA", id);
  };

  return (
    <div className="w-screen h-screen bg-void-black text-gray-200 flex overflow-hidden">
      {/* Sidebar */}
      <div className="shrink-0 h-full p-4 z-10">
        <ScenarioPanel
          params={simParams}
          onParamChange={(k, v) => setSimParams(p => ({ ...p, [k]: v }))}
        />
      </div>

      {/* Main Content */}
      <div className="flex-1 flex flex-col h-full p-4 gap-4 relative">
        {/* Header / Metrics */}
        <div className="grid grid-cols-4 gap-4 shrink-0 z-10">
          <MetricCard
            label="Total GSP (Billions)"
            value={`$${stats?.gdp?.toFixed(1) || '---'}`}
            color="teal"
            trend={2.4}
          />
          <MetricCard
            label="Population (Millions)"
            value={`${stats?.population?.toFixed(2) || '---'}`}
            color="blue"
          />
          <MetricCard
            label="State Debt"
            value={`$${stats?.debt?.toFixed(1) || '---'}B`}
            color="amber"
            trend={-1.2}
          />
          <MetricCard
            label="Sim Year"
            value={year}
            color="teal"
          />
        </div>

        {/* Map Area */}
        <div className="flex-1 relative min-h-0">
          <MapCanvas
            year={year}
            highlightMetric="gdp"
            onLgaSelect={handleLgaSelect}
            className="absolute inset-0"
          />
        </div>

        {/* Timeline */}
        <div className="shrink-0 z-10">
          <TimelineSlider
            year={year}
            isPlaying={isPlaying}
            onTogglePlay={() => setIsPlaying(!isPlaying)}
            onChange={setYear}
          />
        </div>
      </div>
    </div>
  );
}

export default App;
