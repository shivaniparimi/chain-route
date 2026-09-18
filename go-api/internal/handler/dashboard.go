package handler

import (
	"context"
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
