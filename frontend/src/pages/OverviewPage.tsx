import type { ReactNode } from "react";

import { NetworkUsageChart } from "../components/charts/NetworkUsageChart";
import { PaymentVolumeChart } from "../components/charts/PaymentVolumeChart";
import { ProviderSelectionChart } from "../components/charts/ProviderSelectionChart";
import { RoutingCostChart } from "../components/charts/RoutingCostChart";
import { StatusDistributionChart } from "../components/charts/StatusDistributionChart";
import { ErrorState } from "../components/ErrorState";
import { LoadingState } from "../components/LoadingState";
import { StatCard } from "../components/StatCard";
import { useDashboardStats } from "../hooks/useDashboardStats";
import { useTimeseries } from "../hooks/useTimeseries";

function ChartCard({ children }: { children: ReactNode }) {
  return <div className="rounded-lg border border-slate-200 bg-white p-4 shadow-sm">{children}</div>;
}

// Renders one query's loading/error/content lifecycle inside a ChartCard,
// so every chart section below reads the same three-line shape regardless
// of which hook backs it. Kept local to this page (not promoted to a shared
// component) since its signature -- a bare TanStack Query result plus a
// content renderer -- is specific to how this page wires charts to queries.
function ChartSection<T>({
  query,
  loadingLabel,
  render,
}: {
  query: { isLoading: boolean; isError: boolean; error: unknown; data: T | undefined; refetch: () => void };
  loadingLabel: string;
  render: (data: T) => ReactNode;
}) {
  return (
    <ChartCard>
      {query.isLoading ? (
        <LoadingState label={loadingLabel} />
      ) : query.isError ? (
        <ErrorState error={query.error} onRetry={() => query.refetch()} />
      ) : (
        render(query.data as T)
      )}
    </ChartCard>
  );
}

// The Overview Dashboard: a brief summary, a restrained row of stat cards
// (support, not centerpiece -- per the design's "avoid generic KPI cards as
// the centerpiece" constraint), then the 5 charts in a responsive grid.
//
// Each chart section is driven by its own hook call/query key
// (useDashboardStats for the three stats-derived charts, and one
// independent useTimeseries call per timeseries metric) so a slow
// routing_cost query never blocks the volume chart -- their query keys
// differ, so TanStack Query fetches, loads, and errors them independently.
// The three stats-derived charts (status/provider/network) necessarily
// share one query, since GET /dashboard/stats is the one endpoint backing
// all three -- there is no separate endpoint to split them across.
export function OverviewPage() {
  const statsQuery = useDashboardStats();
  const volumeQuery = useTimeseries({ metric: "volume" });
  const costQuery = useTimeseries({ metric: "routing_cost" });

  return (
    <div className="p-6">
      <h1 className="text-2xl font-semibold text-slate-900">Overview</h1>
      <p className="mt-2 text-slate-600">
        Aggregate payment volume, status, provider selection, routing cost, and network usage
        across every supported corridor.
      </p>

      <div className="mt-6">
        {statsQuery.isLoading ? (
          <LoadingState label="Loading dashboard stats…" />
        ) : statsQuery.isError ? (
          <ErrorState error={statsQuery.error} onRetry={() => statsQuery.refetch()} />
        ) : statsQuery.data ? (
          <dl className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            <StatCard label="Total payments" value={statsQuery.data.total_payments} />
            <StatCard label="Completed" value={statsQuery.data.completed_payments} />
            <StatCard label="In flight" value={statsQuery.data.processing_payments} />
            <StatCard label="Failed" value={statsQuery.data.failed_payments} />
          </dl>
        ) : null}
      </div>

      <div className="mt-6 grid gap-4 lg:grid-cols-2">
        <ChartSection
          query={volumeQuery}
          loadingLabel="Loading payment volume…"
          render={(data) => <PaymentVolumeChart points={data.points} />}
        />
        <ChartSection
          query={statsQuery}
          loadingLabel="Loading status distribution…"
          render={(data) => <StatusDistributionChart stats={data} />}
        />
        <ChartSection
          query={statsQuery}
          loadingLabel="Loading provider selection…"
          render={(data) => <ProviderSelectionChart stats={data} />}
        />
        <ChartSection
          query={costQuery}
          loadingLabel="Loading routing cost…"
          render={(data) => <RoutingCostChart points={data.points} />}
        />
        <div className="lg:col-span-2">
          <ChartSection
            query={statsQuery}
            loadingLabel="Loading network usage…"
            render={(data) => <NetworkUsageChart stats={data} />}
          />
        </div>
      </div>
    </div>
  );
}
