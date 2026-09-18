package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"chainroute/go-api/internal/payment"
)

type fakeDashboardStore struct {
	listResult []payment.Payment
	listCursor string
	listErr    error

	lastFilter payment.ListFilter
}

func (f *fakeDashboardStore) ListPayments(_ context.Context, filter payment.ListFilter) ([]payment.Payment, string, error) {
	f.lastFilter = filter
	return f.listResult, f.listCursor, f.listErr
}

func doListPaymentsRequest(h *Handler, target string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("GET", target, nil)
	rec := httptest.NewRecorder()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /payments", h.ListPayments)
	mux.ServeHTTP(rec, req)
	return rec
}

func samplePayments(n int) []payment.Payment {
	out := make([]payment.Payment, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, payment.Payment{
			ID: "id-" + string(rune('a'+i)), SourceChain: "ethereum", DestinationChain: "base",
			Asset: "USDC", Amount: "10.00", Status: payment.StatusRouted,
			ExecutionMode: payment.ExecutionModeSimulated, TotalFee: 1.5,
			CreatedAt: time.Date(2026, 1, 1, 0, 0, i, 0, time.UTC),
		})
	}
	return out
}

func TestListPayments_DefaultLimitApplied(t *testing.T) {
	store := &fakeDashboardStore{listResult: samplePayments(3)}
	h := &Handler{DashboardStore: store}

	rec := doListPaymentsRequest(h, "/payments")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if store.lastFilter.Limit != defaultListLimit {
		t.Fatalf("expected default limit %d, got %d", defaultListLimit, store.lastFilter.Limit)
	}
}

func TestListPayments_LimitClampedToMax(t *testing.T) {
	store := &fakeDashboardStore{listResult: samplePayments(1)}
	h := &Handler{DashboardStore: store}

	rec := doListPaymentsRequest(h, "/payments?limit=500")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if store.lastFilter.Limit != maxListLimit {
		t.Fatalf("expected limit clamped to %d, got %d", maxListLimit, store.lastFilter.Limit)
	}
}

func TestListPayments_InvalidLimitReturns400(t *testing.T) {
	h := &Handler{DashboardStore: &fakeDashboardStore{}}

	for _, raw := range []string{"0", "-1", "abc"} {
		rec := doListPaymentsRequest(h, "/payments?limit="+raw)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("limit=%q: expected 400, got %d", raw, rec.Code)
		}
	}
}

func TestListPayments_InvalidStatusReturns400(t *testing.T) {
	h := &Handler{DashboardStore: &fakeDashboardStore{}}
	rec := doListPaymentsRequest(h, "/payments?status=bogus")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestListPayments_InvalidExecutionModeReturns400(t *testing.T) {
	h := &Handler{DashboardStore: &fakeDashboardStore{}}
	rec := doListPaymentsRequest(h, "/payments?execution_mode=bogus")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestListPayments_ValidRequestReturnsExpectedShape(t *testing.T) {
	provider := "across"
	payments := []payment.Payment{
		{
			ID: "pay-1", SourceChain: "ethereum", DestinationChain: "base",
			Asset: "USDC", Amount: "10.00", Status: payment.StatusCompleted,
			ExecutionMode: payment.ExecutionModeTestnet, BridgeProvider: &provider,
			TotalFee:  0.25,
			CreatedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		},
	}
	store := &fakeDashboardStore{listResult: payments}
	h := &Handler{DashboardStore: store}

	rec := doListPaymentsRequest(h, "/payments?status=COMPLETED&provider=across&source_chain=ethereum&destination_chain=base&execution_mode=testnet")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	if store.lastFilter.Status == nil || *store.lastFilter.Status != "COMPLETED" {
		t.Fatalf("expected status filter COMPLETED, got %+v", store.lastFilter.Status)
	}
	if store.lastFilter.Provider == nil || *store.lastFilter.Provider != "across" {
		t.Fatalf("expected provider filter across, got %+v", store.lastFilter.Provider)
	}
	if store.lastFilter.SourceChain == nil || *store.lastFilter.SourceChain != "ethereum" {
		t.Fatalf("expected source_chain filter ethereum, got %+v", store.lastFilter.SourceChain)
	}
	if store.lastFilter.DestinationChain == nil || *store.lastFilter.DestinationChain != "base" {
		t.Fatalf("expected destination_chain filter base, got %+v", store.lastFilter.DestinationChain)
	}
	if store.lastFilter.ExecutionMode == nil || *store.lastFilter.ExecutionMode != "testnet" {
		t.Fatalf("expected execution_mode filter testnet, got %+v", store.lastFilter.ExecutionMode)
	}

	var resp paymentListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if len(resp.Payments) != 1 {
		t.Fatalf("expected 1 payment, got %d", len(resp.Payments))
	}
	item := resp.Payments[0]
	if item.ID != "pay-1" || item.SourceChain != "ethereum" || item.DestinationChain != "base" ||
		item.Asset != "USDC" || item.Amount != "10.00" || item.Status != "COMPLETED" ||
		item.ExecutionMode != "testnet" || item.BridgeProvider == nil || *item.BridgeProvider != "across" ||
		item.TotalFee != 0.25 || item.CreatedAt != "2026-01-02T03:04:05Z" {
		t.Fatalf("unexpected item shape: %+v", item)
	}
	if resp.NextCursor != nil {
		t.Fatalf("expected nil next_cursor, got %v", *resp.NextCursor)
	}
}

func TestListPayments_NextCursorNilWhenFewerThanLimitPlusOne(t *testing.T) {
	store := &fakeDashboardStore{listResult: samplePayments(2), listCursor: ""}
	h := &Handler{DashboardStore: store}

	rec := doListPaymentsRequest(h, "/payments?limit=25")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp paymentListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.NextCursor != nil {
		t.Fatalf("expected nil next_cursor, got %v", *resp.NextCursor)
	}
}

func TestListPayments_NextCursorPresentWhenMoreRowsExist(t *testing.T) {
	store := &fakeDashboardStore{listResult: samplePayments(2), listCursor: "some-cursor"}
	h := &Handler{DashboardStore: store}

	rec := doListPaymentsRequest(h, "/payments?limit=2")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp paymentListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.NextCursor == nil || *resp.NextCursor != "some-cursor" {
		t.Fatalf("expected next_cursor 'some-cursor', got %v", resp.NextCursor)
	}
}

func TestListPayments_StoreErrorReturns500(t *testing.T) {
	store := &fakeDashboardStore{listErr: context.DeadlineExceeded}
	h := &Handler{DashboardStore: store}

	rec := doListPaymentsRequest(h, "/payments")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
}
