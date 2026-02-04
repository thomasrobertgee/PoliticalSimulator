import clsx from 'clsx';
import { ArrowUpRight, ArrowDownRight } from 'lucide-react';

interface MetricCardProps {
    label: string;
    value: string | number;
    trend?: number; // percentage change
    trendLabel?: string;
    color?: 'teal' | 'amber' | 'blue';
}

const MetricCard = ({ label, value, trend, trendLabel, color = 'teal' }: MetricCardProps) => {
    const isPositive = trend && trend > 0;

    const colorStyles = {
        teal: 'text-neon-teal border-neon-teal/30',
        amber: 'text-stress-amber border-stress-amber/30',
        blue: 'text-blue-400 border-blue-400/30'
    };

    return (
        <div className={clsx("glass-panel p-4 rounded-xl flex flex-col gap-1 border-l-4", colorStyles[color].replace('text-', 'border-'))}>
            <span className="text-xs text-gray-400 uppercase tracking-wider">{label}</span>
            <div className="flex items-end justify-between">
                <span className={clsx("text-2xl font-bold font-mono", colorStyles[color].split(' ')[0])}>
                    {value}
                </span>
                {trend !== undefined && (
                    <div className={clsx("flex items-center text-xs", isPositive ? "text-green-400" : "text-red-400")}>
                        {isPositive ? <ArrowUpRight size={14} /> : <ArrowDownRight size={14} />}
                        <span>{Math.abs(trend)}% {trendLabel}</span>
                    </div>
                )}
            </div>
        </div>
    );
};

export default MetricCard;
