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

// handler.dashboardStats (dashboard.go) -- GET /dashboard/stats
export interface DashboardStats {
  total_payments: number;
  completed_payments: number;
  processing_payments: number;
  failed_payments: number;
  provider_usage: Record<string, number>;
  average_routing_cost: number;
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
