import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";

import type { TimeseriesPoint } from "../../api/types";
import { formatTimestamp } from "../../lib/format";
import { EmptyState } from "../EmptyState";

// #0bb6a3 is `brand-500` from tailwind.config.js -- the single accent color
// this app reserves for primary interactive/highlight use, reused here for
// the one chart whose whole subject (payment volume) is the page's central
// metric.
const ACCENT_COLOR = "#0bb6a3";

export function PaymentVolumeChart({ points }: { points: TimeseriesPoint[] }) {
  if (points.length === 0) {
    return <EmptyState message="No payment volume data yet" />;
  }
  const data = points.map((p) => ({ bucket: p.bucket, count: p.count }));
  return (
    <div>
      <h3 className="text-sm font-medium text-slate-700 mb-2">Payment Volume</h3>
      <ResponsiveContainer width="100%" height={240}>
        <AreaChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
          <XAxis dataKey="bucket" tickFormatter={(v) => formatTimestamp(v).split(",")[0]} fontSize={12} />
          <YAxis allowDecimals={false} fontSize={12} label={{ value: "Payments", angle: -90, position: "insideLeft" }} />
          <Tooltip labelFormatter={(v) => formatTimestamp(v as string)} formatter={(value) => [value, "Payments"]} />
          <Area type="monotone" dataKey="count" stroke={ACCENT_COLOR} fill={ACCENT_COLOR} fillOpacity={0.2} />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
