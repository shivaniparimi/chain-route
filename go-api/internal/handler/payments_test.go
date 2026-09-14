package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	routingv1 "chainroute/go-api/internal/gen/chainroute/v1"
	"chainroute/go-api/internal/payment"
)

type fakePaymentStore struct {
	createResult  payment.Payment
	createOutcome payment.CreateResult
	createErr     error
	getResult     payment.Payment
	getFound      bool
	getErr        error
	lastCreate    payment.Payment

	lookupResult  payment.Payment
	lookupOutcome payment.CreateResult
	lookupFound   bool
	lookupErr     error
}

func (f *fakePaymentStore) CreateOrGetPayment(_ context.Context, p payment.Payment) (payment.Payment, payment.CreateResult, error) {
	f.lastCreate = p
	return f.createResult, f.createOutcome, f.createErr
}

func (f *fakePaymentStore) GetPayment(_ context.Context, id string) (payment.Payment, bool, error) {
	return f.getResult, f.getFound, f.getErr
}

func (f *fakePaymentStore) LookupByIdempotencyKey(_ context.Context, _ payment.Payment) (payment.Payment, payment.CreateResult, bool, error) {
	return f.lookupResult, f.lookupOutcome, f.lookupFound, f.lookupErr
}

func doPaymentRequest(h *Handler, method, path, idempotencyKey, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	rec := httptest.NewRecorder()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /payments", h.PostPayments)
	mux.HandleFunc("GET /payments/{id}", h.GetPayment)
	mux.ServeHTTP(rec, req)
	return rec
}

func validPaymentBody() string {
	return `{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":"1000.00"}`
}

func TestPostPayments_MissingIdempotencyKey(t *testing.T) {
	h := &Handler{Client: &fakeClient{}, Store: &fakePaymentStore{}}
	rec := doPaymentRequest(h, "POST", "/payments", "", validPaymentBody())
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestPostPayments_TooLongIdempotencyKey(t *testing.T) {
	h := &Handler{Client: &fakeClient{}, Store: &fakePaymentStore{}}
	longKey := make([]byte, 256)
	for i := range longKey {
		longKey[i] = 'a'
	}
	rec := doPaymentRequest(h, "POST", "/payments", string(longKey), validPaymentBody())
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestPostPayments_InvalidJSON(t *testing.T) {
	h := &Handler{Client: &fakeClient{}, Store: &fakePaymentStore{}}
	rec := doPaymentRequest(h, "POST", "/payments", "key-1", "{not json")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestPostPayments_InvalidChain(t *testing.T) {
	h := &Handler{Client: &fakeClient{}, Store: &fakePaymentStore{}}
	rec := doPaymentRequest(h, "POST", "/payments", "key-1",
		`{"source_chain":"mars","destination_chain":"base","asset":"USDC","amount":"1000.00"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestPostPayments_SameChain(t *testing.T) {
	h := &Handler{Client: &fakeClient{}, Store: &fakePaymentStore{}}
	rec := doPaymentRequest(h, "POST", "/payments", "key-1",
		`{"source_chain":"ethereum","destination_chain":"ethereum","asset":"USDC","amount":"1000.00"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestPostPayments_AmountValidation(t *testing.T) {
	cases := map[string]string{
		"too many integer digits":    `{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":"123456789012345678901"}`,
		"too many fractional digits": `{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":"1.1234567890123456789"}`,
		"zero":                       `{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":"0.00"}`,
		"negative sign":              `{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":"-100.00"}`,
		"exponent notation":          `{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":"1e10"}`,
		"leading dot":                `{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":".50"}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			h := &Handler{Client: &fakeClient{}, Store: &fakePaymentStore{}}
			rec := doPaymentRequest(h, "POST", "/payments", "key-1", body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 for %s, got %d: %s", name, rec.Code, rec.Body.String())
			}
		})
	}
}

func TestPostPayments_NoRouteFound(t *testing.T) {
	fake := &fakeClient{response: &routingv1.FindRouteResponse{RouteFound: false}}
	h := &Handler{Client: fake, Store: &fakePaymentStore{}}
	rec := doPaymentRequest(h, "POST", "/payments", "key-1", validPaymentBody())
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d", rec.Code)
	}
}

func TestPostPayments_SuccessfulCreation(t *testing.T) {
	fakeRoute := &fakeClient{response: &routingv1.FindRouteResponse{
		RouteFound: true,
		TotalFee:   2.5,
		Hops: []*routingv1.RouteHop{
			{FromChain: routingv1.Chain_CHAIN_ETHEREUM, ToChain: routingv1.Chain_CHAIN_BASE,
				BridgeName: "Hop#1", Fee: 2.5, LatencyMs: 500, Liquidity: 1000000, Reliability: 0.99},
		},
	}}
	store := &fakePaymentStore{
		createOutcome: payment.Created,
		createResult: payment.Payment{
			ID: "test-id-1", SourceChain: "ethereum", DestinationChain: "base",
			Asset: "USDC", Amount: "1000.00", Status: payment.StatusRouted, TotalFee: 2.5,
			Hops:      []payment.Hop{{HopIndex: 0, FromChain: "ethereum", ToChain: "base", BridgeName: "Hop#1", Fee: 2.5}},
			CreatedAt: time.Now(), UpdatedAt: time.Now(),
		},
	}
	h := &Handler{Client: fakeRoute, Store: store}
	rec := doPaymentRequest(h, "POST", "/payments", "key-1", validPaymentBody())

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	if loc := rec.Header().Get("Location"); loc != "/payments/test-id-1" {
		t.Fatalf("unexpected Location header: %q", loc)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["id"] != "test-id-1" || body["amount"] != "1000.00" || body["status"] != "ROUTED" {
		t.Fatalf("unexpected body: %+v", body)
	}
	if store.lastCreate.IdempotencyKey != "key-1" {
		t.Fatalf("expected idempotency key to be passed through, got %q", store.lastCreate.IdempotencyKey)
	}
}

func TestPostPayments_Replayed(t *testing.T) {
	fakeRoute := &fakeClient{response: &routingv1.FindRouteResponse{RouteFound: true, TotalFee: 2.5}}
	store := &fakePaymentStore{
		createOutcome: payment.Replayed,
		createResult:  payment.Payment{ID: "existing-id", Status: payment.StatusRouted, CreatedAt: time.Now(), UpdatedAt: time.Now()},
	}
	h := &Handler{Client: fakeRoute, Store: store}
	rec := doPaymentRequest(h, "POST", "/payments", "key-1", validPaymentBody())
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestPostPayments_Conflict(t *testing.T) {
	fakeRoute := &fakeClient{response: &routingv1.FindRouteResponse{RouteFound: true, TotalFee: 2.5}}
	store := &fakePaymentStore{createOutcome: payment.Conflict}
	h := &Handler{Client: fakeRoute, Store: store}
	rec := doPaymentRequest(h, "POST", "/payments", "key-1", validPaymentBody())
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rec.Code)
	}
}

func TestPostPayments_EarlyLookupReplayed_SkipsRouting(t *testing.T) {
	fakeRoute := &fakeClient{response: &routingv1.FindRouteResponse{RouteFound: true, TotalFee: 2.5}}
	store := &fakePaymentStore{
		lookupFound:   true,
		lookupOutcome: payment.Replayed,
		lookupResult: payment.Payment{
			ID: "existing-id", Status: payment.StatusRouted, CreatedAt: time.Now(), UpdatedAt: time.Now(),
		},
	}
	h := &Handler{Client: fakeRoute, Store: store}
	rec := doPaymentRequest(h, "POST", "/payments", "key-1", validPaymentBody())
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if loc := rec.Header().Get("Location"); loc != "/payments/existing-id" {
		t.Fatalf("unexpected Location header: %q", loc)
	}
	if fakeRoute.callCount != 0 {
		t.Fatalf("expected FindRoute not to be called, got %d calls", fakeRoute.callCount)
	}
}

func TestPostPayments_EarlyLookupConflict_SkipsRouting(t *testing.T) {
	fakeRoute := &fakeClient{response: &routingv1.FindRouteResponse{RouteFound: true, TotalFee: 2.5}}
	store := &fakePaymentStore{
		lookupFound:   true,
		lookupOutcome: payment.Conflict,
	}
	h := &Handler{Client: fakeRoute, Store: store}
	rec := doPaymentRequest(h, "POST", "/payments", "key-1", validPaymentBody())
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", rec.Code, rec.Body.String())
	}
	if fakeRoute.callCount != 0 {
		t.Fatalf("expected FindRoute not to be called, got %d calls", fakeRoute.callCount)
	}
}

func TestGetPayment_Found(t *testing.T) {
	store := &fakePaymentStore{
		getFound: true,
		getResult: payment.Payment{
			ID: "test-id-1", SourceChain: "ethereum", DestinationChain: "base",
			Asset: "USDC", Amount: "1000.00", Status: payment.StatusRouted,
			CreatedAt: time.Now(), UpdatedAt: time.Now(),
		},
	}
	h := &Handler{Client: &fakeClient{}, Store: store}
	rec := doPaymentRequest(h, "GET", "/payments/test-id-1", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestGetPayment_NotFound(t *testing.T) {
	store := &fakePaymentStore{getFound: false}
	h := &Handler{Client: &fakeClient{}, Store: store}
	rec := doPaymentRequest(h, "GET", "/payments/nonexistent", "", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestToPaymentResponse_CompletedAtNullWhenNotSet(t *testing.T) {
	store := &fakePaymentStore{
		createOutcome: payment.Created,
		createResult: payment.Payment{
			ID: "test-id-completed-at-null", Status: payment.StatusRouted,
			CreatedAt: time.Now(), UpdatedAt: time.Now(), CompletedAt: nil,
		},
	}
	h := &Handler{Client: &fakeClient{response: &routingv1.FindRouteResponse{RouteFound: true, TotalFee: 2.5}}, Store: store}
	rec := doPaymentRequest(h, "POST", "/payments", "key-1", validPaymentBody())

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	v, ok := body["completed_at"]
	if !ok {
		t.Fatal("expected completed_at key to be present in the response")
	}
	if v != nil {
		t.Fatalf("expected completed_at to be null, got %v", v)
	}
}

func TestToPaymentResponse_CompletedAtSetWhenTerminal(t *testing.T) {
	completedAt := time.Date(2026, 9, 12, 0, 0, 0, 0, time.UTC)
	store := &fakePaymentStore{
		getFound: true,
		getResult: payment.Payment{
			ID: "test-id-completed-at-set", Status: payment.StatusCompleted,
			CreatedAt: time.Now(), UpdatedAt: time.Now(), CompletedAt: &completedAt,
		},
	}
	h := &Handler{Client: &fakeClient{}, Store: store}
	rec := doPaymentRequest(h, "GET", "/payments/test-id-completed-at-set", "", "")

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	got, ok := body["completed_at"].(string)
	if !ok {
		t.Fatalf("expected completed_at to be a string, got %v", body["completed_at"])
	}
	if got != completedAt.Format(time.RFC3339Nano) {
		t.Fatalf("expected %q, got %q", completedAt.Format(time.RFC3339Nano), got)
	}
}
