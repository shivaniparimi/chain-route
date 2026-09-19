import { useQuery } from "@tanstack/react-query";

import { apiGet } from "../api/client";
import type { Payment, PaymentStatus } from "../api/types";

// Statuses a payment can still move on from. Once a payment reaches
// COMPLETED or FAILED it is terminal (see LifecycleTimeline.tsx's
// STATUS_ORDER comment) -- there is nothing left to poll for, so polling
// forever would just be wasted requests against a row that can never
// change again.
const NON_TERMINAL_STATUSES: PaymentStatus[] = ["ROUTED", "PROCESSING", "SUBMITTED"];

// Fetches GET /payments/{id}. Polls every 5s while the payment is still
// in flight so the lifecycle/route/status on the detail page stay live
// without a manual refresh, and stops polling entirely once the payment
// reaches a terminal status -- refetchInterval is a function of the
// latest query data (TanStack Query re-evaluates it after every fetch),
// not a fixed value decided once at mount time.
export function usePayment(id: string | undefined) {
  return useQuery({
    queryKey: ["payment", id],
    queryFn: () => apiGet<Payment>(`/payments/${id}`),
    enabled: Boolean(id),
    refetchInterval: (query) => {
      const status = query.state.data?.status;
      if (!status) return 5_000;
      return NON_TERMINAL_STATUSES.includes(status) ? 5_000 : false;
    },
  });
}
