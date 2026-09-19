import { useQuery } from "@tanstack/react-query";

import { apiGet } from "../api/client";
import type { TimeseriesResponse } from "../api/types";

export interface UseTimeseriesOptions {
  metric: "volume" | "routing_cost";
  interval?: "hour" | "day";
  days?: number;
}

// Fetches GET /dashboard/timeseries for one metric at a time -- each chart
// that needs a timeseries (PaymentVolumeChart, RoutingCostChart) calls this
// with its own `metric`, giving each an independent query key/cache entry
// and an independent loading/error lifecycle, so a slow routing_cost query
// never blocks the volume chart from rendering. Polls every 10s, matching
// useDashboardStats's cadence.
export function useTimeseries({ metric, interval = "day", days = 30 }: UseTimeseriesOptions) {
  return useQuery({
    queryKey: ["dashboard-timeseries", metric, interval, days],
    queryFn: () =>
      apiGet<TimeseriesResponse>("/dashboard/timeseries", { metric, interval, days }),
    refetchInterval: 10_000,
  });
}
