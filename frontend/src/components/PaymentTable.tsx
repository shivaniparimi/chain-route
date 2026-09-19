import { Link } from "react-router-dom";

import type { PaymentListItem } from "../api/types";
import { CHAIN_DISPLAY_NAMES, PROVIDER_DISPLAY_NAMES } from "../lib/chains";
import { formatRelativeTime, formatTimestamp } from "../lib/format";
import { StatusBadge } from "./StatusBadge";

interface PaymentTableProps {
  payments: PaymentListItem[];
}

// Payment IDs are UUIDs -- too long to show in full in a table cell. Show
// a short prefix, with the full ID always available via the `title`
// tooltip and via the row's link target.
function truncateId(id: string): string {
  return id.length > 8 ? `${id.slice(0, 8)}…` : id;
}

// total_fee (and Payment.Amount, rendered below) are plain human-scale
// decimal numbers/strings from the backend (see go-api/internal/payment
// /payment.go's comment distinguishing Payment.Amount, "0.001", from the
// Quote type's base-units integer strings) -- NOT base-units integers, so
// lib/format.ts's `formatBaseUnits` does not apply here and would produce
// wrong values if used. Render the fee with a bounded number of decimals
// instead.
function formatRoutingCost(fee: number): string {
  return fee.toLocaleString(undefined, { maximumFractionDigits: 6 });
}

// Renders PaymentListItem[] as a table. Deliberately has no empty-state
// opinion of its own -- when `payments` is empty it renders nothing, and
// the parent page (which knows about active filters) decides which empty
// message to show.
export function PaymentTable({ payments }: PaymentTableProps) {
  if (payments.length === 0) return null;

  return (
    <table className="w-full text-left text-sm">
      <thead className="border-b border-slate-200 text-xs font-medium uppercase tracking-wide text-slate-500">
        <tr>
          <th scope="col" className="px-4 py-2">
            Payment ID
          </th>
          <th scope="col" className="px-4 py-2">
            Route
          </th>
          <th scope="col" className="px-4 py-2">
            Asset
          </th>
          <th scope="col" className="px-4 py-2">
            Amount
          </th>
          <th scope="col" className="px-4 py-2">
            Provider
          </th>
          <th scope="col" className="px-4 py-2">
            Routing cost
          </th>
          <th scope="col" className="px-4 py-2">
            Status
          </th>
          <th scope="col" className="px-4 py-2">
            Execution mode
          </th>
          <th scope="col" className="px-4 py-2">
            Created
          </th>
        </tr>
      </thead>
      <tbody className="divide-y divide-slate-100">
        {payments.map((payment) => (
          <tr key={payment.id} className="hover:bg-slate-50">
            <td className="px-4 py-2 font-mono text-xs text-slate-700" title={payment.id}>
              <Link to={`/payments/${payment.id}`} className="text-brand-700 hover:underline">
                {truncateId(payment.id)}
              </Link>
            </td>
            <td className="px-4 py-2 text-slate-700">
              {CHAIN_DISPLAY_NAMES[payment.source_chain] ?? payment.source_chain}
              {" → "}
              {CHAIN_DISPLAY_NAMES[payment.destination_chain] ?? payment.destination_chain}
            </td>
            <td className="px-4 py-2 text-slate-700">{payment.asset}</td>
            <td className="px-4 py-2 text-slate-700">{payment.amount}</td>
            <td className="px-4 py-2 text-slate-700">
              {payment.bridge_provider
                ? PROVIDER_DISPLAY_NAMES[payment.bridge_provider] ?? payment.bridge_provider
                : "—"}
            </td>
            <td className="px-4 py-2 text-slate-700">{formatRoutingCost(payment.total_fee)}</td>
            <td className="px-4 py-2">
              <StatusBadge status={payment.status} />
            </td>
            <td className="px-4 py-2 capitalize text-slate-700">{payment.execution_mode}</td>
            <td className="px-4 py-2 text-slate-700" title={formatTimestamp(payment.created_at)}>
              {formatRelativeTime(payment.created_at)}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
