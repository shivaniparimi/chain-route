import type { ReactNode } from "react";

export interface StatCardTrend {
  direction: "up" | "down" | "flat";
  label: string;
}

interface StatCardProps {
  label: string;
  value: ReactNode;
  trend?: StatCardTrend;
}

const TREND_CLASSES: Record<StatCardTrend["direction"], string> = {
  up: "text-status-completed",
  down: "text-status-failed",
  flat: "text-slate-500",
};

// Small and restrained by design -- a label, a value, and an optional
// trend note, nothing else. These support the Overview Dashboard's charts
// (context: total/completed/processing/failed counts above the fold), they
// are deliberately not styled as the page's centerpiece: no gradients, no
// oversized numerals, no icon well, no shadow-heavy card chrome.
export function StatCard({ label, value, trend }: StatCardProps) {
  return (
    <div className="rounded-lg border border-slate-200 bg-white p-4">
      <dt className="text-xs font-medium uppercase tracking-wide text-slate-500">{label}</dt>
      <dd className="mt-1 text-2xl font-semibold text-slate-900">{value}</dd>
      {trend ? <p className={`mt-1 text-xs ${TREND_CLASSES[trend.direction]}`}>{trend.label}</p> : null}
    </div>
  );
}
