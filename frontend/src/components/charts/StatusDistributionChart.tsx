import { Bar, BarChart, CartesianGrid, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import type { DashboardStats, PaymentStatus } from "../../api/types";
import { statusColor } from "../StatusBadge";
import { EmptyState } from "../EmptyState";

// DashboardStats has no single "status" field to derive a distribution
// from -- it's three separate counters (completed_payments,
// processing_payments, failed_payments), where processing_payments already
// aggregates the ROUTED/PROCESSING/SUBMITTED in-flight family server-side
// (see postgres.Store.GetDashboardStats's COUNT(*) FILTER clause). One
// representative PaymentStatus per bucket is enough to key statusColor,
// since ROUTED/PROCESSING/SUBMITTED share one color family (StatusBadge.tsx).
const BUCKETS: { key: keyof Pick<DashboardStats, "completed_payments" | "processing_payments" | "failed_payments">; label: string; status: PaymentStatus }[] = [
  { key: "completed_payments", label: "Completed", status: "COMPLETED" },
  { key: "processing_payments", label: "In flight", status: "PROCESSING" },
  { key: "failed_payments", label: "Failed", status: "FAILED" },
];

export function StatusDistributionChart({ stats }: { stats: DashboardStats }) {
  const data = BUCKETS.map((bucket) => ({
    label: bucket.label,
    count: stats[bucket.key],
    color: statusColor(bucket.status),
  }));

  if (data.every((d) => d.count === 0)) {
    return <EmptyState message="No payment status data yet" />;
  }

  return (
    <div>
      <h3 className="text-sm font-medium text-slate-700 mb-2">Status Distribution</h3>
      <ResponsiveContainer width="100%" height={240}>
        <BarChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
          <XAxis dataKey="label" fontSize={12} />
          <YAxis allowDecimals={false} fontSize={12} label={{ value: "Payments", angle: -90, position: "insideLeft" }} />
          <Tooltip formatter={(value) => [value, "Payments"]} />
          <Bar dataKey="count" isAnimationActive={false}>
            {data.map((entry) => (
              <Cell key={entry.label} fill={entry.color} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
