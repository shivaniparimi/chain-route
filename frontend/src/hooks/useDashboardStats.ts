import { useQuery } from "@tanstack/react-query";

import { apiGet } from "../api/client";
import type { DashboardStats } from "../api/types";

// Fetches Task 3/12's GET /dashboard/stats -- the aggregate counters
// backing the Overview Dashboard's stat cards, status/provider/network
// breakdown charts. Polls every 10s (same cadence as usePaymentList) so the
// dashboard stays current without a manual refresh, since payments keep
// arriving/completing while the page is open.
export function useDashboardStats() {
  return useQuery({
    queryKey: ["dashboard-stats"],
    queryFn: () => apiGet<DashboardStats>("/dashboard/stats"),
    refetchInterval: 10_000,
  });
}
