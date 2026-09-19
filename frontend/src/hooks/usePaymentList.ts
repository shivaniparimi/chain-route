import { useQuery } from "@tanstack/react-query";

import { apiGet } from "../api/client";
import type { PaymentListFilters, PaymentListResponse } from "../api/types";

// Polls GET /payments (the only endpoint the explorer is allowed to hit --
// there is no unbounded "list all payments" route) every 10s so the table
// stays fresh without a manual refresh.
//
// The query key includes the full `filters` object, which the caller is
// expected to fold the current pagination cursor into (PaymentListFilters
// already has an optional `cursor` field for exactly this). TanStack Query
// hashes query keys structurally, so any change to a filter value or to
// the cursor produces a distinct cache entry -- the old page's data is not
// reused, and the new key starts its own loading/refetch lifecycle.
export function usePaymentList(filters: PaymentListFilters) {
  return useQuery({
    queryKey: ["payments", filters],
    queryFn: () =>
      apiGet<PaymentListResponse>("/payments", filters as Record<string, string | number | undefined>),
    refetchInterval: 10_000,
  });
}
