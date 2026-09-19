import { Bar, BarChart, CartesianGrid, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import type { DashboardStats } from "../../api/types";
import { PROVIDER_DISPLAY_NAMES } from "../../lib/chains";
import { EmptyState } from "../EmptyState";

// Shades of `brand` (tailwind.config.js) rather than an arbitrary
// multi-color palette -- this chart has no pre-existing app-wide
// provider->color mapping to reuse (unlike StatusDistributionChart, which
// reuses StatusBadge's statusColor), so it stays within the app's one
// accent family instead of introducing new colors. Cycles if there are ever
// more providers than shades.
const PROVIDER_COLORS = ["#0bb6a3", "#08746d", "#57eed2", "#0d4c49"];

export function ProviderSelectionChart({ stats }: { stats: DashboardStats }) {
  const entries = Object.entries(stats.provider_usage);

  if (entries.length === 0) {
    return <EmptyState message="No provider selection data yet" />;
  }

  const data = entries
    .map(([provider, count], i) => ({
      provider,
      label: PROVIDER_DISPLAY_NAMES[provider] ?? provider,
      count,
      color: PROVIDER_COLORS[i % PROVIDER_COLORS.length],
    }))
    .sort((a, b) => b.count - a.count);

  return (
    <div>
      <h3 className="text-sm font-medium text-slate-700 mb-2">Provider Selection</h3>
      <ResponsiveContainer width="100%" height={240}>
        <BarChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
          <XAxis dataKey="label" fontSize={12} />
          <YAxis allowDecimals={false} fontSize={12} label={{ value: "Payments", angle: -90, position: "insideLeft" }} />
          <Tooltip formatter={(value) => [value, "Payments"]} />
          <Bar dataKey="count" isAnimationActive={false}>
            {data.map((entry) => (
              <Cell key={entry.provider} fill={entry.color} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
