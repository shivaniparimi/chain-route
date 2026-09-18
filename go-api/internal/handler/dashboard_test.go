package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"chainroute/go-api/internal/payment"
)

type fakeDashboardStore struct {
	listResult []payment.Payment
	listCursor string
	listErr    error

	lastFilter payment.ListFilter

	quotesResult []payment.Quote
	quotesErr    error
	lastQuotesID string

	statsResult payment.DashboardStats
	statsErr    error
}

func (f *fakeDashboardStore) ListPayments(_ context.Context, filter payment.ListFilter) ([]payment.Payment, string, error) {
	f.lastFilter = filter
	return f.listResult, f.listCursor, f.listErr
}

func (f *fakeDashboardStore) GetQuotesByPaymentID(_ context.Context, paymentID string) ([]payment.Quote, error) {
	f.lastQuotesID = paymentID
	return f.quotesResult, f.quotesErr
}

func (f *fakeDashboardStore) GetDashboardStats(_ context.Context) (payment.DashboardStats, error) {
	return f.statsResult, f.statsErr
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

func TestListPayments_MalformedCursorReturns400(t *testing.T) {
	// A cursor-decode failure is a client input error, not a server fault:
	// it must surface as 400 (like the endpoint's other invalid-input
	// cases: limit, status, execution_mode), never as the 500 a genuine
	// store/database error gets.
	store := &fakeDashboardStore{listErr: fmt.Errorf("wrapped: %w", payment.ErrInvalidCursor)}
	h := &Handler{DashboardStore: store}

	rec := doListPaymentsRequest(h, "/payments?cursor=not-valid-base64!!!")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for a malformed cursor, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(strings.ToLower(rec.Body.String()), "cursor") {
		t.Fatalf("expected error message to mention 'cursor', got %s", rec.Body.String())
	}
}

func TestListPayments_NonCursorStoreErrorStillReturns500(t *testing.T) {
	// Guard against over-broadly treating every store error as a 400:
	// only ErrInvalidCursor should map to 400; everything else stays 500.
	store := &fakeDashboardStore{listErr: context.DeadlineExceeded}
	h := &Handler{DashboardStore: store}

	rec := doListPaymentsRequest(h, "/payments?cursor=some-cursor-value")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 for a non-cursor store error, got %d", rec.Code)
	}
}

func doPaymentQuotesRequest(h *Handler, target string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("GET", target, nil)
	rec := httptest.NewRecorder()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /payments/{id}/quotes", h.GetPaymentQuotes)
	mux.ServeHTTP(rec, req)
	return rec
}

func doDashboardStatsRequest(h *Handler, target string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("GET", target, nil)
	rec := httptest.NewRecorder()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /dashboard/stats", h.GetDashboardStats)
	mux.ServeHTTP(rec, req)
	return rec
}

func TestGetPaymentQuotes_NonexistentPaymentReturns404(t *testing.T) {
	h := &Handler{
		Store:          &fakePaymentStore{getFound: false},
		DashboardStore: &fakeDashboardStore{},
	}

	rec := doPaymentQuotesRequest(h, "/payments/does-not-exist/quotes")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestGetPaymentQuotes_NoQuotesReturnsEmptyArrayNotNull(t *testing.T) {
	h := &Handler{
		Store:          &fakePaymentStore{getFound: true, getResult: payment.Payment{ID: "pay-1"}},
		DashboardStore: &fakeDashboardStore{quotesResult: nil},
	}

	rec := doPaymentQuotesRequest(h, "/payments/pay-1/quotes")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"quotes":[]`) {
		t.Fatalf("expected quotes to render as an empty array, not null: %s", rec.Body.String())
	}
}

func TestGetPaymentQuotes_ReturnsExpectedShapeWithSelectedFlag(t *testing.T) {
	quotedAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	quotes := []payment.Quote{
		{
			Provider: "across", InputAmount: "1000000000000000000", OutputAmount: "999000000000000000",
			FeeAmount: "1000000000000000", EstimatedFillTimeSec: 30, Selected: true, QuotedAt: quotedAt,
		},
		{
			Provider: "relay", InputAmount: "1000000000000000000", OutputAmount: "998000000000000000",
			FeeAmount: "2000000000000000", EstimatedFillTimeSec: 60, Selected: false, QuotedAt: quotedAt,
		},
	}
	h := &Handler{
		Store:          &fakePaymentStore{getFound: true, getResult: payment.Payment{ID: "pay-1"}},
		DashboardStore: &fakeDashboardStore{quotesResult: quotes},
	}

	rec := doPaymentQuotesRequest(h, "/payments/pay-1/quotes")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp paymentQuotesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if len(resp.Quotes) != 2 {
		t.Fatalf("expected 2 quotes, got %d", len(resp.Quotes))
	}
	if resp.Quotes[0].Provider != "across" || !resp.Quotes[0].Selected ||
		resp.Quotes[0].InputAmount != "1000000000000000000" || resp.Quotes[0].OutputAmount != "999000000000000000" ||
		resp.Quotes[0].FeeAmount != "1000000000000000" || resp.Quotes[0].EstimatedFillTimeSec != 30 ||
		resp.Quotes[0].QuotedAt != "2026-01-02T03:04:05Z" {
		t.Fatalf("unexpected first quote shape: %+v", resp.Quotes[0])
	}
	if resp.Quotes[1].Provider != "relay" || resp.Quotes[1].Selected {
		t.Fatalf("expected second quote to be unselected relay quote: %+v", resp.Quotes[1])
	}
}

func TestGetPaymentQuotes_PaymentLookupErrorReturns500(t *testing.T) {
	h := &Handler{
		Store:          &fakePaymentStore{getErr: context.DeadlineExceeded},
		DashboardStore: &fakeDashboardStore{},
	}

	rec := doPaymentQuotesRequest(h, "/payments/pay-1/quotes")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
}

func TestGetPaymentQuotes_StoreErrorReturns500(t *testing.T) {
	h := &Handler{
		Store:          &fakePaymentStore{getFound: true, getResult: payment.Payment{ID: "pay-1"}},
		DashboardStore: &fakeDashboardStore{quotesErr: context.DeadlineExceeded},
	}

	rec := doPaymentQuotesRequest(h, "/payments/pay-1/quotes")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
}

func TestGetDashboardStats_ReturnsExpectedShape(t *testing.T) {
	stats := payment.DashboardStats{
		TotalPayments: 10, CompletedPayments: 6, ProcessingPayments: 3, FailedPayments: 1,
		ProviderUsage:      map[string]int64{"across": 4, "relay": 2},
		AverageRoutingCost: 1.25,
	}
	h := &Handler{DashboardStore: &fakeDashboardStore{statsResult: stats}}

	rec := doDashboardStatsRequest(h, "/dashboard/stats")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp dashboardStats
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.TotalPayments != 10 || resp.CompletedPayments != 6 || resp.ProcessingPayments != 3 ||
		resp.FailedPayments != 1 || resp.AverageRoutingCost != 1.25 {
		t.Fatalf("unexpected stats shape: %+v", resp)
	}
	if resp.ProviderUsage["across"] != 4 || resp.ProviderUsage["relay"] != 2 || len(resp.ProviderUsage) != 2 {
		t.Fatalf("unexpected provider usage: %+v", resp.ProviderUsage)
	}
}

func TestGetDashboardStats_StoreErrorReturns500(t *testing.T) {
	h := &Handler{DashboardStore: &fakeDashboardStore{statsErr: context.DeadlineExceeded}}

	rec := doDashboardStatsRequest(h, "/dashboard/stats")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
}
