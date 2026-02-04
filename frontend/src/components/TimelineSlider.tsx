import { Play, Pause } from 'lucide-react';

interface TimelineSliderProps {
    year: number;
    min?: number;
    max?: number;
    onChange: (year: number) => void;
    isPlaying?: boolean;
    onTogglePlay?: () => void;
}

const TimelineSlider = ({ year, min = 1976, max = 2026, onChange, isPlaying, onTogglePlay }: TimelineSliderProps) => {
    return (
        <div className="flex items-center gap-4 w-full glass-panel p-4 rounded-xl">
            <button
                onClick={onTogglePlay}
                className="p-2 rounded-full bg-neon-teal/20 hover:bg-neon-teal/40 text-neon-teal transition-colors"
            >
                {isPlaying ? <Pause size={20} /> : <Play size={20} />}
            </button>

            <div className="flex-1 flex flex-col gap-1">
                <div className="flex justify-between text-xs text-gray-400 font-mono">
                    <span>{min}</span>
                    <span className="text-neon-teal text-lg font-bold">{year}</span>
                    <span>{max}</span>
                </div>
                <input
                    type="range"
                    min={min}
                    max={max}
                    value={year}
                    onChange={(e) => onChange(parseInt(e.target.value))}
                    className="w-full h-2 bg-gray-700 rounded-lg appearance-none cursor-pointer accent-neon-teal hover:accent-teal-300"
                />
            </div>
        </div>
    );
};

export default TimelineSlider;
