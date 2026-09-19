import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import type { DashboardStats } from "../../api/types";
import { CHAIN_DISPLAY_NAMES } from "../../lib/chains";
import { EmptyState } from "../EmptyState";

// Same brand-500 accent as PaymentVolumeChart/RoutingCostChart -- a single
// series (corridor frequency), no per-bar color distinction needed.
const ACCENT_COLOR = "#0bb6a3";

// With 5 chains there can be up to 20 corridors, each with a long
// "Ethereum → Base"-style label -- illegible on a single bar chart
// without a cap. Only the top TOP_N by count are ever rendered.
const TOP_N = 8;

// Backed by DashboardStats.network_usage -- a true SQL aggregate
// (postgres.Store.GetDashboardStats's GROUP BY source_chain, destination_chain
// query, added in this task) rather than a client-side approximation over a
// paginated /payments response, per the plan's N+1/performance constraint.
export function NetworkUsageChart({ stats }: { stats: DashboardStats }) {
  if (stats.network_usage.length === 0) {
    return <EmptyState message="No network usage data yet" />;
  }

  const data = [...stats.network_usage]
    .sort((a, b) => b.count - a.count)
    .slice(0, TOP_N)
    .map((entry) => ({
      corridor: `${CHAIN_DISPLAY_NAMES[entry.source_chain] ?? entry.source_chain} → ${
        CHAIN_DISPLAY_NAMES[entry.destination_chain] ?? entry.destination_chain
      }`,
      count: entry.count,
    }));

  return (
    <div>
      <h3 className="text-sm font-medium text-slate-700 mb-2">Network Usage</h3>
      <ResponsiveContainer width="100%" height={240}>
        <BarChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
          <XAxis dataKey="corridor" fontSize={12} />
          <YAxis allowDecimals={false} fontSize={12} label={{ value: "Payments", angle: -90, position: "insideLeft" }} />
          <Tooltip formatter={(value) => [value, "Payments"]} />
          <Bar dataKey="count" fill={ACCENT_COLOR} isAnimationActive={false} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
