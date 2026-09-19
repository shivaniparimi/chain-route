import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";

import type { TimeseriesPoint } from "../../api/types";
import { formatTimestamp } from "../../lib/format";
import { EmptyState } from "../EmptyState";

// Same brand-500 accent as PaymentVolumeChart -- see that file's comment.
const ACCENT_COLOR = "#0bb6a3";

// The y-axis is labeled "fee units", not a currency symbol like "$" or a
// token symbol like "ETH". total_fee (source of this chart's `value`, via
// GET /dashboard/timeseries?metric=routing_cost -> COALESCE(AVG(total_fee), 0))
// comes from resp.GetTotalFee() in go-api/internal/handler/payments.go --
// the C++ routing simulator's own `double` fee output (see routingv1.CandidateEdge.Fee
// and FindRouteResponse's hop fees), which is a fee-simulation unit for
// simulated-mode payments, not a real currency amount. Even in testnet
// mode, the winning hop's fee is still populated from the same simulator
// response field (candidate.TotalFee = resp.GetTotalFee()), not recomputed
// from the bridge quote's actual base-units FeeAmount -- so there is no
// place in this code path where total_fee is confirmed to be a specific
// token's base-unit amount across both modes. Labeling this axis "USD" or
// any token symbol would fabricate a unit this data doesn't actually carry;
// "fee units" is the honest label.
export function RoutingCostChart({ points }: { points: TimeseriesPoint[] }) {
  if (points.length === 0) {
    return <EmptyState message="No routing cost data yet" />;
  }
  const data = points.map((p) => ({ bucket: p.bucket, value: p.value }));
  return (
    <div>
      <h3 className="text-sm font-medium text-slate-700 mb-2">Routing Cost</h3>
      <ResponsiveContainer width="100%" height={240}>
        <AreaChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
          <XAxis dataKey="bucket" tickFormatter={(v) => formatTimestamp(v).split(",")[0]} fontSize={12} />
          <YAxis
            fontSize={12}
            label={{ value: "Routing cost (fee units)", angle: -90, position: "insideLeft" }}
          />
          <Tooltip
            labelFormatter={(v) => formatTimestamp(v as string)}
            formatter={(value) => [value, "Avg. fee units"]}
          />
          <Area type="monotone" dataKey="value" stroke={ACCENT_COLOR} fill={ACCENT_COLOR} fillOpacity={0.2} />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
