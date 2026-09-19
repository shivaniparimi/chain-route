import { useQuery } from "@tanstack/react-query";

import { apiGet } from "../api/client";
import type { PaymentQuotesResponse } from "../api/types";

// Fetches Task 3's GET /payments/{id}/quotes. Unlike usePaymentList (which
// polls every 10s because payment status changes over time), quotes are
// written once at routing time and never mutated afterward -- there is
// nothing to poll for, so this hook has no `refetchInterval`.
export function usePaymentQuotes(paymentId: string) {
  return useQuery({
    queryKey: ["payment-quotes", paymentId],
    queryFn: () => apiGet<PaymentQuotesResponse>(`/payments/${paymentId}/quotes`),
  });
}
