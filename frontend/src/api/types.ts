// Mirrors the JSON shapes actually serialized by go-api/internal/handler
// (dashboard.go, payments.go), verified field-by-field against that
// package's committed json-tagged response structs as of this task --
// NOT transcribed blind from the phase plan's draft sketch. Every field
// below matched the Go source with zero drift at the time of writing;
// if the backend's response shape ever changes, this file (and nothing
// else) is what needs to change on the frontend.

export type PaymentStatus = "ROUTED" | "PROCESSING" | "SUBMITTED" | "COMPLETED" | "FAILED";
export type ExecutionMode = "simulated" | "testnet";

// handler.hopResponse (payments.go)
export interface Hop {
  hop_index: number;
  from_chain: string;
  to_chain: string;
  bridge_name: string;
  fee: number;
  latency_ms: number;
  liquidity: number;
  reliability: number;
}

// handler.paymentResponse -- the GET /payments/{id} response shape
// (payments.go, pre-existing endpoint predating this phase).
//
// provider_reference_id/external_status/raw_external_status: added by
// Task 11 (additive-only extension to payments.go's toPaymentResponse --
// every pre-existing field above is untouched). null whenever there's no
// execution row yet (simulated-mode payments, or a testnet-mode payment
// that hasn't started executing) -- same nil convention as
// external_tx_hash/submitted_at just above.
export interface Payment {
  id: string;
  source_chain: string;
  destination_chain: string;
  asset: string;
  amount: string;
  status: PaymentStatus;
  total_fee: number;
  hops: Hop[];
  execution_mode: ExecutionMode;
  bridge_provider: string | null;
  failure_reason: string | null;
  external_tx_hash: string | null;
  submitted_at: string | null;
  created_at: string;
  updated_at: string;
  completed_at: string | null;
  provider_reference_id: string | null;
  external_status: string | null;
  raw_external_status: string | null;
}

// handler.paymentListItem (dashboard.go) -- the row shape for GET
// /payments. Deliberately narrower than Payment: no hops/failure_reason/
// external_tx_hash/submitted_at/updated_at/completed_at.
export interface PaymentListItem {
  id: string;
  source_chain: string;
  destination_chain: string;
  asset: string;
  amount: string;
  status: PaymentStatus;
  execution_mode: ExecutionMode;
  bridge_provider: string | null;
  total_fee: number;
  created_at: string;
}

// handler.paymentListResponse (dashboard.go)
export interface PaymentListResponse {
  payments: PaymentListItem[];
  next_cursor: string | null;
}

// Query-param filters accepted by GET /payments (ListPayments handler).
export interface PaymentListFilters {
  status?: PaymentStatus;
  provider?: string;
  source_chain?: string;
  destination_chain?: string;
  execution_mode?: ExecutionMode;
  cursor?: string;
  limit?: number;
}

// handler.quoteItem (dashboard.go)
export interface Quote {
  provider: string;
  input_amount: string;
  output_amount: string;
  fee_amount: string;
  estimated_fill_time_sec: number;
  selected: boolean;
  quoted_at: string;
}

// handler.paymentQuotesResponse (dashboard.go) -- GET /payments/{id}/quotes
export interface PaymentQuotesResponse {
  quotes: Quote[];
}

// handler.networkUsageEntry (dashboard.go) -- one element of
// DashboardStats.network_usage.
export interface NetworkUsageEntry {
  source_chain: string;
  destination_chain: string;
  count: number;
}

// handler.dashboardStats (dashboard.go) -- GET /dashboard/stats
//
// network_usage: added by Task 12 (phase 12, additive-only extension to
// dashboard.go's toDashboardStatsResponse -- every pre-existing field above
// is untouched) so the Overview Dashboard's NetworkUsageChart has a true,
// indexed SQL aggregate (postgres.Store.GetDashboardStats's new
// GROUP BY source_chain, destination_chain query) to render instead of a
// client-side approximation over a paginated /payments response. Always an
// array (never null): an empty payments table serializes as "[]", matching
// provider_usage's existing nil-map-renders-as-{} convention.
export interface DashboardStats {
  total_payments: number;
  completed_payments: number;
  processing_payments: number;
  failed_payments: number;
  provider_usage: Record<string, number>;
  average_routing_cost: number;
  network_usage: NetworkUsageEntry[];
}

// handler.timeseriesPoint (dashboard.go)
export interface TimeseriesPoint {
  bucket: string;
  value: number;
  count: number;
}

// handler.timeseriesResponse (dashboard.go) -- GET /dashboard/timeseries
export interface TimeseriesResponse {
  metric: "volume" | "routing_cost";
  points: TimeseriesPoint[];
}
