import type { PaymentStatus } from "../api/types";

// Human-readable label per status. Exported so FilterBar (and anywhere
// else that lists statuses) can render the same wording instead of the
// raw SCREAMING_CASE enum value.
export const STATUS_LABELS: Record<PaymentStatus, string> = {
  ROUTED: "Routed",
  PROCESSING: "Processing",
  SUBMITTED: "Submitted",
  COMPLETED: "Completed",
  FAILED: "Failed",
};

// Hex values mirror the `status.*` tokens in tailwind.config.js exactly.
// ROUTED/PROCESSING/SUBMITTED share the "in-flight" amber family (the
// design calls these out as one active stage, distinguished from each
// other by their exact label rather than by a separate color per
// sub-state -- see the tailwind.config.js comment for the same
// reasoning). COMPLETED is green, FAILED is red.
const STATUS_COLORS: Record<PaymentStatus, string> = {
  ROUTED: "#d97706",
  PROCESSING: "#d97706",
  SUBMITTED: "#d97706",
  COMPLETED: "#16a34a",
  FAILED: "#dc2626",
};

// Plain function (not just Tailwind classes) so non-DOM consumers --
// e.g. Task 8/9's route/timeline visualizations, which need real CSS
// color values for Recharts stroke/fill props -- can key off the same
// status -> color mapping instead of re-deriving their own.
export function statusColor(status: PaymentStatus): string {
  return STATUS_COLORS[status];
}

const BADGE_CLASSES: Record<PaymentStatus, string> = {
  ROUTED: "bg-status-processing-bg text-status-processing",
  PROCESSING: "bg-status-processing-bg text-status-processing",
  SUBMITTED: "bg-status-processing-bg text-status-processing",
  COMPLETED: "bg-status-completed-bg text-status-completed",
  FAILED: "bg-status-failed-bg text-status-failed",
};

export function StatusBadge({ status }: { status: PaymentStatus }) {
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-xs font-medium ${BADGE_CLASSES[status]}`}
    >
      <span
        aria-hidden="true"
        className="h-1.5 w-1.5 rounded-full"
        style={{ backgroundColor: statusColor(status) }}
      />
      {STATUS_LABELS[status]}
    </span>
  );
}
