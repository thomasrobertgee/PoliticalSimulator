interface ScenarioPanelProps {
    onParamChange: (param: string, val: number) => void;
    params: { [key: string]: number };
}

const ScenarioPanel = ({ onParamChange, params }: ScenarioPanelProps) => {
    return (
        <div className="glass-panel w-64 p-4 rounded-xl flex flex-col gap-6 h-full">
            <h2 className="text-sm font-bold text-neon-teal uppercase tracking-widest border-b border-gray-700 pb-2">
                Simulation Variables
            </h2>

            <div className="flex flex-col gap-4 overflow-y-auto">
                {Object.entries(params).map(([key, val]) => (
                    <div key={key} className="flex flex-col gap-1">
                        <div className="flex justify-between text-xs text-gray-300">
                            <span className="capitalize">{key.replace('_', ' ')}</span>
                            <span className="font-mono text-neon-teal">{val}</span>
                        </div>
                        <input
                            type="range"
                            min={0}
                            max={100}
                            value={val}
                            onChange={(e) => onParamChange(key, parseInt(e.target.value))}
                            className="w-full h-1 bg-gray-800 rounded-lg appearance-none cursor-pointer accent-stress-amber"
                        />
                    </div>
                ))}
            </div>

            <div className="mt-auto">
                <button className="w-full py-2 bg-neon-teal/10 hover:bg-neon-teal/20 text-neon-teal text-xs uppercase font-bold rounded border border-neon-teal/50 transition-all">
                    Run Projection
                </button>
            </div>
        </div>
    );
};

export default ScenarioPanel;
