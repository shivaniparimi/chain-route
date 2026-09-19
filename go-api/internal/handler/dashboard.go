package handler

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"chainroute/go-api/internal/payment"
)

// DashboardStore is the subset of *postgres.Store the read-only
// dashboard endpoints need -- kept narrow and separate from PaymentStore
// (which backs the write-adjacent POST /payments and GET /payments/{id}
// paths) per this package's existing per-handler-interface convention.
type DashboardStore interface {
	ListPayments(ctx context.Context, filter payment.ListFilter) ([]payment.Payment, string, error)
	GetQuotesByPaymentID(ctx context.Context, paymentID string) ([]payment.Quote, error)
	GetDashboardStats(ctx context.Context) (payment.DashboardStats, error)
	GetTimeseries(ctx context.Context, metric, interval string, days int) ([]payment.TimeseriesPoint, error)
}

type paymentListItem struct {
	ID               string  `json:"id"`
	SourceChain      string  `json:"source_chain"`
	DestinationChain string  `json:"destination_chain"`
	Asset            string  `json:"asset"`
	Amount           string  `json:"amount"`
	Status           string  `json:"status"`
	ExecutionMode    string  `json:"execution_mode"`
	BridgeProvider   *string `json:"bridge_provider"`
	TotalFee         float64 `json:"total_fee"`
	CreatedAt        string  `json:"created_at"`
}

type paymentListResponse struct {
	Payments   []paymentListItem `json:"payments"`
	NextCursor *string           `json:"next_cursor"`
}

const (
	defaultListLimit = 25
	maxListLimit     = 100
)

func (h *Handler) ListPayments(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	limit := defaultListLimit
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		limit = n
		if limit > maxListLimit {
			limit = maxListLimit
		}
	}

	filter := payment.ListFilter{Limit: limit, Cursor: q.Get("cursor")}

	if s := q.Get("status"); s != "" {
		if !isValidStatus(s) {
			writeError(w, http.StatusBadRequest, "invalid status: "+s)
			return
		}
		filter.Status = &s
	}
	if p := q.Get("provider"); p != "" {
		filter.Provider = &p
	}
	if sc := q.Get("source_chain"); sc != "" {
		filter.SourceChain = &sc
	}
	if dc := q.Get("destination_chain"); dc != "" {
		filter.DestinationChain = &dc
	}
	if em := q.Get("execution_mode"); em != "" {
		if em != string(payment.ExecutionModeSimulated) && em != string(payment.ExecutionModeTestnet) {
			writeError(w, http.StatusBadRequest, "execution_mode must be \"simulated\" or \"testnet\"")
			return
		}
		filter.ExecutionMode = &em
	}

	payments, nextCursor, err := h.DashboardStore.ListPayments(r.Context(), filter)
	if err != nil {
		if errors.Is(err, payment.ErrInvalidCursor) {
			writeError(w, http.StatusBadRequest, "invalid cursor")
			return
		}
		h.logger().ErrorContext(r.Context(), "failed to list payments", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	items := make([]paymentListItem, 0, len(payments))
	for _, p := range payments {
		items = append(items, paymentListItem{
			ID: p.ID, SourceChain: p.SourceChain, DestinationChain: p.DestinationChain,
			Asset: p.Asset, Amount: p.Amount, Status: string(p.Status),
			ExecutionMode: string(p.ExecutionMode), BridgeProvider: p.BridgeProvider,
			TotalFee: p.TotalFee, CreatedAt: p.CreatedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	resp := paymentListResponse{Payments: items}
	if nextCursor != "" {
		resp.NextCursor = &nextCursor
	}
	writeJSON(w, http.StatusOK, resp)
}

func isValidStatus(s string) bool {
	switch payment.Status(s) {
	case payment.StatusRouted, payment.StatusProcessing, payment.StatusSubmitted, payment.StatusCompleted, payment.StatusFailed:
		return true
	}
	return false
}

type quoteItem struct {
	Provider             string `json:"provider"`
	InputAmount          string `json:"input_amount"`
	OutputAmount         string `json:"output_amount"`
	FeeAmount            string `json:"fee_amount"`
	EstimatedFillTimeSec int64  `json:"estimated_fill_time_sec"`
	Selected             bool   `json:"selected"`
	QuotedAt             string `json:"quoted_at"`
}

type paymentQuotesResponse struct {
	Quotes []quoteItem `json:"quotes"`
}

// GetPaymentQuotes returns every quote fetched for a payment (winning and
// losing), selected-first -- read-only, backed by Task 1's
// GetQuotesByPaymentID. 404s if the payment itself doesn't exist; an
// existing payment with no quotes (simulated-mode, or predating migration
// 0007) renders as "quotes": [] rather than null.
func (h *Handler) GetPaymentQuotes(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	_, found, err := h.Store.GetPayment(r.Context(), id)
	if err != nil {
		h.logger().ErrorContext(r.Context(), "failed to read payment for quotes lookup", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "payment not found")
		return
	}
	quotes, err := h.DashboardStore.GetQuotesByPaymentID(r.Context(), id)
	if err != nil {
		h.logger().ErrorContext(r.Context(), "failed to read payment quotes", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	items := make([]quoteItem, 0, len(quotes))
	for _, q := range quotes {
		items = append(items, quoteItem{
			Provider: q.Provider, InputAmount: q.InputAmount, OutputAmount: q.OutputAmount,
			FeeAmount: q.FeeAmount, EstimatedFillTimeSec: q.EstimatedFillTimeSec,
			Selected: q.Selected, QuotedAt: q.QuotedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	writeJSON(w, http.StatusOK, paymentQuotesResponse{Quotes: items})
}

// dashboardStats is the JSON response shape for GET /dashboard/stats --
// kept as its own thin, json-tagged struct (converted from
// payment.DashboardStats by toDashboardStatsResponse) rather than adding
// json tags to the domain type itself, matching this package's existing
// convention for Payment/paymentResponse and Payment/paymentListItem.
type dashboardStats struct {
	TotalPayments      int64            `json:"total_payments"`
	CompletedPayments  int64            `json:"completed_payments"`
	ProcessingPayments int64            `json:"processing_payments"`
	FailedPayments     int64            `json:"failed_payments"`
	ProviderUsage      map[string]int64 `json:"provider_usage"`
	AverageRoutingCost float64          `json:"average_routing_cost"`
}

func toDashboardStatsResponse(s payment.DashboardStats) dashboardStats {
	providerUsage := s.ProviderUsage
	if providerUsage == nil {
		providerUsage = map[string]int64{}
	}
	return dashboardStats{
		TotalPayments: s.TotalPayments, CompletedPayments: s.CompletedPayments,
		ProcessingPayments: s.ProcessingPayments, FailedPayments: s.FailedPayments,
		ProviderUsage: providerUsage, AverageRoutingCost: s.AverageRoutingCost,
	}
}

// GetDashboardStats returns the read-only aggregate counters backing the
// dashboard overview: payment counts by status, per-provider usage, and
// average routing cost -- computed entirely in SQL by
// postgres.Store.GetDashboardStats (a small fixed number of aggregation
// queries, never fetch-all-then-aggregate-in-Go).
func (h *Handler) GetDashboardStats(w http.ResponseWriter, r *http.Request) {
	stats, err := h.DashboardStore.GetDashboardStats(r.Context())
	if err != nil {
		h.logger().ErrorContext(r.Context(), "failed to read dashboard stats", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, toDashboardStatsResponse(stats))
}

type timeseriesPoint struct {
	Bucket string  `json:"bucket"`
	Value  float64 `json:"value"`
	Count  int64   `json:"count"`
}

type timeseriesResponse struct {
	Metric string            `json:"metric"`
	Points []timeseriesPoint `json:"points"`
}

var validTimeseriesMetrics = map[string]bool{"volume": true, "routing_cost": true}
var validTimeseriesIntervals = map[string]bool{"hour": true, "day": true}

const (
	defaultTimeseriesDays = 30
	maxTimeseriesDays     = 90
)

// GetDashboardTimeseries returns bucketed payment volume or average routing
// cost over time, for the dashboard's trend chart -- read-only, computed
// entirely in SQL by postgres.Store.GetTimeseries (one aggregation query,
// never fetch-all-then-aggregate-in-Go). metric and interval are validated
// against a fixed allow-list before ever reaching the store, since
// GetTimeseries builds its SQL aggregate expression from metric via
// fmt.Sprintf (see that method's doc comment) -- this validation is what
// keeps that safe.
func (h *Handler) GetDashboardTimeseries(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	metric := q.Get("metric")
	if metric == "" {
		metric = "volume"
	}
	if !validTimeseriesMetrics[metric] {
		writeError(w, http.StatusBadRequest, "metric must be \"volume\" or \"routing_cost\"")
		return
	}

	interval := q.Get("interval")
	if interval == "" {
		interval = "day"
	}
	if !validTimeseriesIntervals[interval] {
		writeError(w, http.StatusBadRequest, "interval must be \"hour\" or \"day\"")
		return
	}

	days := defaultTimeseriesDays
	if raw := q.Get("days"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "days must be a positive integer")
			return
		}
		days = n
		if days > maxTimeseriesDays {
			days = maxTimeseriesDays
		}
	}

	points, err := h.DashboardStore.GetTimeseries(r.Context(), metric, interval, days)
	if err != nil {
		h.logger().ErrorContext(r.Context(), "failed to read dashboard timeseries", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	items := make([]timeseriesPoint, 0, len(points))
	for _, p := range points {
		items = append(items, timeseriesPoint{
			Bucket: p.Bucket.UTC().Format(time.RFC3339),
			Value:  p.Value,
			Count:  p.Count,
		})
	}
	writeJSON(w, http.StatusOK, timeseriesResponse{Metric: metric, Points: items})
}
