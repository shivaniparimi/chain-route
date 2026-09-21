package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"chainroute/go-api/internal/bridge/quote"
	routingv1 "chainroute/go-api/internal/gen/chainroute/v1"
	"chainroute/go-api/internal/observability"
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

	execResult payment.Execution
	execFound  bool
	execErr    error
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

func (f *fakePaymentStore) GetExecutionByPaymentID(_ context.Context, _ string) (payment.Execution, bool, error) {
	return f.execResult, f.execFound, f.execErr
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

// TestToPaymentResponse_FailureReasonSetWhenFailed guards the fix for the
// final-review finding that payment.FailureReason was persisted and read
// internally but never exposed via GET /payments/{id} -- the whole
// justification for the failure_reason column is that a caller can learn
// WHY a payment failed. Mirrors
// TestToPaymentResponse_CompletedAtSetWhenTerminal's null-vs-set pattern.
func TestToPaymentResponse_FailureReasonSetWhenFailed(t *testing.T) {
	reason := "fee_slippage_exceeded"
	store := &fakePaymentStore{
		getFound: true,
		getResult: payment.Payment{
			ID: "test-id-failure-reason-set", Status: payment.StatusFailed, FailureReason: &reason,
			CreatedAt: time.Now(), UpdatedAt: time.Now(),
		},
	}
	h := &Handler{Client: &fakeClient{}, Store: store}
	rec := doPaymentRequest(h, "GET", "/payments/test-id-failure-reason-set", "", "")

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	got, ok := body["failure_reason"].(string)
	if !ok {
		t.Fatalf("expected failure_reason to be a string, got %v", body["failure_reason"])
	}
	if got != reason {
		t.Fatalf("expected failure_reason = %q, got %q", reason, got)
	}
}

// TestToPaymentResponse_FailureReasonNullWhenNotSet is the null-key
// counterpart -- a payment with no failure reason must show
// "failure_reason": null (present, not absent), matching the existing
// completed_at convention this file already tests.
func TestToPaymentResponse_FailureReasonNullWhenNotSet(t *testing.T) {
	store := &fakePaymentStore{
		getFound: true,
		getResult: payment.Payment{
			ID: "test-id-failure-reason-null", Status: payment.StatusRouted, FailureReason: nil,
			CreatedAt: time.Now(), UpdatedAt: time.Now(),
		},
	}
	h := &Handler{Client: &fakeClient{}, Store: store}
	rec := doPaymentRequest(h, "GET", "/payments/test-id-failure-reason-null", "", "")

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	v, ok := body["failure_reason"]
	if !ok {
		t.Fatal("expected failure_reason key to be present in the response")
	}
	if v != nil {
		t.Fatalf("expected failure_reason to be null, got %v", v)
	}
}

// TestToPaymentResponse_ProviderReferenceAndExternalStatusSetWhenExecutionFound
// guards the Task 11 additive extension to GET /payments/{id}: the handler
// already loaded payment.Execution (for external_tx_hash/submitted_at)
// but silently dropped ProviderReferenceID/ExternalStatus/RawExternalStatus.
// The Payment Detail page needs these for its "provider status/reference"
// field, so they're now surfaced too.
func TestToPaymentResponse_ProviderReferenceAndExternalStatusSetWhenExecutionFound(t *testing.T) {
	refID := "relay-request-123"
	rawStatus := "success"
	store := &fakePaymentStore{
		getFound: true,
		getResult: payment.Payment{
			ID: "test-id-exec-found", Status: payment.StatusSubmitted,
			CreatedAt: time.Now(), UpdatedAt: time.Now(),
		},
		execFound: true,
		execResult: payment.Execution{
			ProviderReferenceID: &refID,
			ExternalStatus:      payment.ExternalStatusFilled,
			RawExternalStatus:   &rawStatus,
		},
	}
	h := &Handler{Client: &fakeClient{}, Store: store}
	rec := doPaymentRequest(h, "GET", "/payments/test-id-exec-found", "", "")

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got := body["provider_reference_id"]; got != refID {
		t.Fatalf("expected provider_reference_id = %q, got %v", refID, got)
	}
	if got := body["external_status"]; got != "filled" {
		t.Fatalf("expected external_status = %q, got %v", "filled", got)
	}
	if got := body["raw_external_status"]; got != rawStatus {
		t.Fatalf("expected raw_external_status = %q, got %v", rawStatus, got)
	}
}

// TestToPaymentResponse_ProviderReferenceNullWhenNoExecution is the
// null-key counterpart -- a simulated-mode payment (or a testnet-mode
// payment whose execution hasn't started yet) has no execution row at
// all, so these three fields must be present-but-null, matching the
// existing external_tx_hash/submitted_at convention.
func TestToPaymentResponse_ProviderReferenceNullWhenNoExecution(t *testing.T) {
	store := &fakePaymentStore{
		getFound: true,
		getResult: payment.Payment{
			ID: "test-id-no-exec", Status: payment.StatusRouted,
			CreatedAt: time.Now(), UpdatedAt: time.Now(),
		},
		execFound: false,
	}
	h := &Handler{Client: &fakeClient{}, Store: store}
	rec := doPaymentRequest(h, "GET", "/payments/test-id-no-exec", "", "")

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	for _, key := range []string{"provider_reference_id", "external_status", "raw_external_status"} {
		v, ok := body[key]
		if !ok {
			t.Fatalf("expected %q key to be present in the response", key)
		}
		if v != nil {
			t.Fatalf("expected %q to be null, got %v", key, v)
		}
	}
}

func TestPostPayments_TestnetModeRejectedWhenServerNotConfigured(t *testing.T) {
	// h.BlockchainEnv left at its zero value "" -- testnet mode is disabled.
	h := &Handler{Client: &fakeClient{}, Store: &fakePaymentStore{}}
	rec := doPaymentRequest(h, "POST", "/payments", "testnet-gate-key",
		`{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when the server isn't configured for testnet execution, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestPostPayments_TestnetModeRejectsUnsupportedRoute(t *testing.T) {
	h := &Handler{Client: &fakeClient{}, Store: &fakePaymentStore{}, BlockchainEnv: "testnet"}
	rec := doPaymentRequest(h, "POST", "/payments", "testnet-route-key",
		`{"source_chain":"arbitrum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for an unsupported testnet route, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestPostPayments_TestnetModeRejectsAmountOverCeiling(t *testing.T) {
	// A registered provider for the route is required so the request
	// actually reaches the MaxTestnetAmountWei ceiling check instead of
	// being rejected earlier at the registry-lookup step (an empty/nil
	// registry also yields 400, but via the "unsupported route" branch,
	// which would make this test a false positive for the ceiling logic
	// it's named after).
	registry := quote.NewRegistry()
	registry.Register(testnetChainKey, &fakeQuoteProvider{name: "across", quote: quote.Quote{ProviderName: "across", Available: true}})

	h := &Handler{
		Client: &fakeClient{}, Store: &fakePaymentStore{},
		BlockchainEnv: "testnet", MaxTestnetAmountWei: big.NewInt(1), // absurdly low, guarantees rejection
		QuoteRegistry: registry,
	}
	rec := doPaymentRequest(h, "POST", "/payments", "testnet-ceiling-key",
		`{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for an amount over the configured ceiling, got %d: %s", rec.Code, rec.Body.String())
	}
}

type fakeQuoteProvider struct {
	name  string
	quote quote.Quote
	err   error
	delay time.Duration
}

func (f *fakeQuoteProvider) Name() string { return f.name }
func (f *fakeQuoteProvider) GetQuote(_ context.Context, _ quote.Request) (quote.Quote, error) {
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	return f.quote, f.err
}

// testnetChainKey mirrors the ethereum->base, WETH route the handler
// resolves for source_chain=ethereum, destination_chain=base, asset=eth
// in testnet mode (see testnetChainIDByChain/bridgedAssetSymbol in
// routes.go).
var testnetChainKey = quote.RouteKey{SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH"}

func TestPostPayments_TestnetMode_UnregisteredRouteReturns400(t *testing.T) {
	h := &Handler{
		Client: &fakeClient{}, Store: &fakePaymentStore{}, BlockchainEnv: "testnet",
		QuoteRegistry: quote.NewRegistry(), // nothing registered
	}
	rec := doPaymentRequest(h, "POST", "/payments", "test-unregistered-route",
		`{"source_chain":"arbitrum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %s", rec.Code, rec.Body.String())
	}
}

// Unlike TestPostPayments_TestnetMode_UnregisteredRouteReturns400 (which
// uses an unsupported chain -- arbitrum -- so it's rejected by the
// chain/asset map lookup before ever consulting the registry), this uses
// a chain/asset combination the maps DO resolve (ethereum/base/eth) but
// for which the registry itself has nothing registered, exercising the
// len(providers) == 0 branch on its own.
func TestPostPayments_TestnetMode_ValidRouteButNoProviderRegisteredReturns400(t *testing.T) {
	h := &Handler{
		Client: &fakeClient{}, Store: &fakePaymentStore{}, BlockchainEnv: "testnet",
		QuoteRegistry: quote.NewRegistry(), // nothing registered for testnetChainKey
	}
	rec := doPaymentRequest(h, "POST", "/payments", "test-valid-route-no-provider",
		`{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %s", rec.Code, rec.Body.String())
	}
}

func TestPostPayments_TestnetMode_ProviderReportsUnavailableReturns422(t *testing.T) {
	registry := quote.NewRegistry()
	registry.Register(testnetChainKey, &fakeQuoteProvider{name: "across", quote: quote.Quote{ProviderName: "across", Available: false}})

	h := &Handler{
		Client: &fakeClient{}, Store: &fakePaymentStore{}, BlockchainEnv: "testnet",
		QuoteRegistry: registry,
	}
	rec := doPaymentRequest(h, "POST", "/payments", "test-unavailable",
		`{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body = %s", rec.Code, rec.Body.String())
	}
}

func TestPostPayments_TestnetMode_ProviderErrorReturns503(t *testing.T) {
	registry := quote.NewRegistry()
	registry.Register(testnetChainKey, &fakeQuoteProvider{name: "across", err: errors.New("connection refused")})

	h := &Handler{
		Client: &fakeClient{}, Store: &fakePaymentStore{}, BlockchainEnv: "testnet",
		QuoteRegistry: registry,
	}
	rec := doPaymentRequest(h, "POST", "/payments", "test-provider-error",
		`{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body = %s", rec.Code, rec.Body.String())
	}
}

func TestPostPayments_TestnetMode_CandidateEdgeBuiltFromLiveQuote(t *testing.T) {
	registry := quote.NewRegistry()
	registry.Register(testnetChainKey, &fakeQuoteProvider{name: "across", quote: quote.Quote{
		ProviderName: "across", Available: true,
		InputAmountBaseUnits: big.NewInt(1_000_000_000_000_000), OutputAmountBaseUnits: big.NewInt(999_900_000_000_000),
		FeeBaseUnits: big.NewInt(100_000_000_000), EstimatedFillTimeSec: 60,
		QuotedAt: time.Now(), ExpiresAt: time.Now().Add(time.Minute),
		RawProviderPayload: json.RawMessage(`{"spokePoolAddress":"0xabc"}`),
	}})

	fakeRoute := &fakeClient{
		response: &routingv1.FindRouteResponse{
			RouteFound: true, TotalFee: 0.0001,
			Hops: []*routingv1.RouteHop{{FromChain: routingv1.Chain_CHAIN_ETHEREUM, ToChain: routingv1.Chain_CHAIN_BASE, BridgeName: "across", Fee: 0.0001, LatencyMs: 60000, Liquidity: 0.001, Reliability: 1.0}},
		},
	}
	store := &fakePaymentStore{}
	h := &Handler{Client: fakeRoute, Store: store, BlockchainEnv: "testnet", QuoteRegistry: registry}

	rec := doPaymentRequest(h, "POST", "/payments", "test-candidate-edge",
		`{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body = %s", rec.Code, rec.Body.String())
	}
	if len(fakeRoute.lastReq.GetCandidateEdges()) != 1 {
		t.Fatalf("expected exactly 1 candidate edge sent to FindRoute, got %d", len(fakeRoute.lastReq.GetCandidateEdges()))
	}
	edge := fakeRoute.lastReq.GetCandidateEdges()[0]
	if edge.GetBridgeName() != "across" {
		t.Errorf("bridge_name = %q, want across", edge.GetBridgeName())
	}
	if edge.GetLiquidity() != 0.001 {
		t.Errorf("liquidity = %v, want 0.001 (the request amount, since Available=true)", edge.GetLiquidity())
	}
	if len(store.lastCreate.Quotes) == 0 {
		t.Fatal("expected the created payment to carry Quotes to persist")
	}
	var selected *payment.Quote
	for i := range store.lastCreate.Quotes {
		if store.lastCreate.Quotes[i].Selected {
			selected = &store.lastCreate.Quotes[i]
		}
	}
	if selected == nil {
		t.Fatal("expected exactly one Quotes entry with Selected=true")
	}
	if selected.Provider != "across" {
		t.Errorf("selected Quote.Provider = %q, want across", selected.Provider)
	}
}

func TestPostPayments_TestnetMode_BothProvidersHealthy_RoutesOverBoth(t *testing.T) {
	registry := quote.NewRegistry()
	registry.Register(testnetChainKey, &fakeQuoteProvider{name: "across", quote: quote.Quote{ProviderName: "across", Available: true, FeeBaseUnits: big.NewInt(200), OutputAmountBaseUnits: big.NewInt(999_999_999_999_800), RawProviderPayload: json.RawMessage(`{}`)}})
	registry.Register(testnetChainKey, &fakeQuoteProvider{name: "relay", quote: quote.Quote{ProviderName: "relay", Available: true, FeeBaseUnits: big.NewInt(100), OutputAmountBaseUnits: big.NewInt(999_999_999_999_900), RawProviderPayload: json.RawMessage(`{}`)}})

	fakeRoute := &fakeClient{response: &routingv1.FindRouteResponse{RouteFound: true, Hops: []*routingv1.RouteHop{{FromChain: routingv1.Chain_CHAIN_ETHEREUM, ToChain: routingv1.Chain_CHAIN_BASE, BridgeName: "relay", Fee: 0.0001}}}}
	h := &Handler{Client: fakeRoute, Store: &fakePaymentStore{}, BlockchainEnv: "testnet", QuoteRegistry: registry}

	rec := doPaymentRequest(h, "POST", "/payments", "both-healthy",
		`{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(fakeRoute.lastReq.GetCandidateEdges()) != 2 {
		t.Fatalf("expected 2 candidate edges, got %d", len(fakeRoute.lastReq.GetCandidateEdges()))
	}
	// Registration order (§9's tie-break basis): across registered first, so its edge must appear first.
	if fakeRoute.lastReq.GetCandidateEdges()[0].GetBridgeName() != "across" {
		t.Errorf("edge[0].bridge_name = %q, want across (registration order)", fakeRoute.lastReq.GetCandidateEdges()[0].GetBridgeName())
	}
	if fakeRoute.lastReq.GetCandidateEdges()[1].GetBridgeName() != "relay" {
		t.Errorf("edge[1].bridge_name = %q, want relay (registration order)", fakeRoute.lastReq.GetCandidateEdges()[1].GetBridgeName())
	}
}

func TestPostPayments_TestnetMode_OneProviderErrors_RoutesOverTheOther(t *testing.T) {
	registry := quote.NewRegistry()
	registry.Register(testnetChainKey, &fakeQuoteProvider{name: "across", err: errors.New("connection refused")})
	registry.Register(testnetChainKey, &fakeQuoteProvider{name: "relay", quote: quote.Quote{ProviderName: "relay", Available: true, FeeBaseUnits: big.NewInt(100), OutputAmountBaseUnits: big.NewInt(999_999_999_999_900), RawProviderPayload: json.RawMessage(`{}`)}})

	fakeRoute := &fakeClient{response: &routingv1.FindRouteResponse{RouteFound: true, Hops: []*routingv1.RouteHop{{FromChain: routingv1.Chain_CHAIN_ETHEREUM, ToChain: routingv1.Chain_CHAIN_BASE, BridgeName: "relay", Fee: 0.0001}}}}
	h := &Handler{Client: fakeRoute, Store: &fakePaymentStore{}, BlockchainEnv: "testnet", QuoteRegistry: registry}

	rec := doPaymentRequest(h, "POST", "/payments", "one-errors",
		`{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 (routing over the healthy provider), got %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestPostPayments_TestnetMode_BothProvidersError_Returns503(t *testing.T) {
	registry := quote.NewRegistry()
	registry.Register(testnetChainKey, &fakeQuoteProvider{name: "across", err: errors.New("connection refused")})
	registry.Register(testnetChainKey, &fakeQuoteProvider{name: "relay", err: errors.New("timeout")})

	h := &Handler{Client: &fakeClient{}, Store: &fakePaymentStore{}, BlockchainEnv: "testnet", QuoteRegistry: registry}
	rec := doPaymentRequest(h, "POST", "/payments", "both-error",
		`{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body = %s", rec.Code, rec.Body.String())
	}
}

func TestPostPayments_TestnetMode_BothUnavailable_Returns422(t *testing.T) {
	registry := quote.NewRegistry()
	registry.Register(testnetChainKey, &fakeQuoteProvider{name: "across", quote: quote.Quote{ProviderName: "across", Available: false}})
	registry.Register(testnetChainKey, &fakeQuoteProvider{name: "relay", quote: quote.Quote{ProviderName: "relay", Available: false}})

	h := &Handler{Client: &fakeClient{}, Store: &fakePaymentStore{}, BlockchainEnv: "testnet", QuoteRegistry: registry}
	rec := doPaymentRequest(h, "POST", "/payments", "both-unavailable",
		`{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body = %s", rec.Code, rec.Body.String())
	}
}

// TestPostPayments_TestnetMode_OrderingIndependentOfCompletionTime proves
// the concurrent quote-fetch aggregation orders candidate edges by
// registration order, never completion order: across (registered first)
// is the slow provider here, yet must still land at index 0 even though
// relay (registered second, with no delay) finishes first. This is the
// property the sequential loop's replacement must preserve so C++
// Dijkstra's tie-break behavior stays reproducible regardless of network
// timing.
func TestPostPayments_TestnetMode_OrderingIndependentOfCompletionTime(t *testing.T) {
	registry := quote.NewRegistry()
	registry.Register(testnetChainKey, &fakeQuoteProvider{
		name: "across", delay: 50 * time.Millisecond,
		quote: quote.Quote{ProviderName: "across", Available: true, FeeBaseUnits: big.NewInt(200), OutputAmountBaseUnits: big.NewInt(999_999_999_999_800), RawProviderPayload: json.RawMessage(`{}`)},
	})
	registry.Register(testnetChainKey, &fakeQuoteProvider{
		name: "relay", delay: 0,
		quote: quote.Quote{ProviderName: "relay", Available: true, FeeBaseUnits: big.NewInt(100), OutputAmountBaseUnits: big.NewInt(999_999_999_999_900), RawProviderPayload: json.RawMessage(`{}`)},
	})

	fakeRoute := &fakeClient{response: &routingv1.FindRouteResponse{RouteFound: true, Hops: []*routingv1.RouteHop{{FromChain: routingv1.Chain_CHAIN_ETHEREUM, ToChain: routingv1.Chain_CHAIN_BASE, BridgeName: "relay", Fee: 0.0001}}}}
	h := &Handler{Client: fakeRoute, Store: &fakePaymentStore{}, BlockchainEnv: "testnet", QuoteRegistry: registry}

	rec := doPaymentRequest(h, "POST", "/payments", "ordering-independent",
		`{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(fakeRoute.lastReq.GetCandidateEdges()) != 2 {
		t.Fatalf("expected 2 candidate edges, got %d", len(fakeRoute.lastReq.GetCandidateEdges()))
	}
	if fakeRoute.lastReq.GetCandidateEdges()[0].GetBridgeName() != "across" {
		t.Errorf("edge[0].bridge_name = %q, want across (registration order, despite being the slower provider)", fakeRoute.lastReq.GetCandidateEdges()[0].GetBridgeName())
	}
	if fakeRoute.lastReq.GetCandidateEdges()[1].GetBridgeName() != "relay" {
		t.Errorf("edge[1].bridge_name = %q, want relay (registration order)", fakeRoute.lastReq.GetCandidateEdges()[1].GetBridgeName())
	}
}

func TestPostPayments_DefaultExecutionModeIsSimulated(t *testing.T) {
	fakeRoute := &fakeClient{response: &routingv1.FindRouteResponse{RouteFound: true, TotalFee: 1.0}}
	store := &fakePaymentStore{
		createOutcome: payment.Created,
		createResult: payment.Payment{
			ID: "default-mode-id", Status: payment.StatusRouted, ExecutionMode: payment.ExecutionModeSimulated,
			CreatedAt: time.Now(), UpdatedAt: time.Now(),
		},
	}
	h := &Handler{Client: fakeRoute, Store: store}
	rec := doPaymentRequest(h, "POST", "/payments", "default-mode-key",
		`{"source_chain":"ethereum","destination_chain":"base","asset":"usdc","amount":"100"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	if store.lastCreate.ExecutionMode != payment.ExecutionModeSimulated {
		t.Fatalf("expected execution_mode to default to simulated when omitted from the request, got %q", store.lastCreate.ExecutionMode)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["execution_mode"] != "simulated" {
		t.Fatalf("expected execution_mode=simulated by default, got %v", body["execution_mode"])
	}
}

// newTestHandlerWithMetrics builds a Handler wired to a fresh, isolated
// *observability.Metrics (never the shared DefaultMetrics singleton, so
// assertions in one test can't observe increments from another), backed
// by a simple successful routing client/store -- enough for
// simulated-mode payment creation.
func newTestHandlerWithMetrics(t *testing.T) (*Handler, *observability.Metrics) {
	t.Helper()
	metrics := observability.NewMetrics()
	fakeRoute := &fakeClient{response: &routingv1.FindRouteResponse{
		RouteFound: true, TotalFee: 1.0,
		Hops: []*routingv1.RouteHop{{FromChain: routingv1.Chain_CHAIN_ETHEREUM, ToChain: routingv1.Chain_CHAIN_BASE, BridgeName: "Hop#1", Fee: 1.0}},
	}}
	store := &fakePaymentStore{
		createOutcome: payment.Created,
		createResult: payment.Payment{
			ID: "metrics-test-id", Status: payment.StatusRouted, ExecutionMode: payment.ExecutionModeSimulated,
			CreatedAt: time.Now(), UpdatedAt: time.Now(),
		},
	}
	h := &Handler{Client: fakeRoute, Store: store, Metrics: metrics}
	return h, metrics
}

// newTestHandlerWithMetricsAndProviders builds a testnet-mode-capable
// Handler wired to a fresh *observability.Metrics, registering each given
// provider under testnetChainKey (the ethereum->base/WETH route every
// testnet-mode test in this file already targets).
func newTestHandlerWithMetricsAndProviders(t *testing.T, providers map[string]quote.Provider) (*Handler, *observability.Metrics) {
	t.Helper()
	metrics := observability.NewMetrics()
	registry := quote.NewRegistry()
	for _, p := range providers {
		registry.Register(testnetChainKey, p)
	}
	fakeRoute := &fakeClient{response: &routingv1.FindRouteResponse{
		RouteFound: true, TotalFee: 0.0001,
		Hops: []*routingv1.RouteHop{{FromChain: routingv1.Chain_CHAIN_ETHEREUM, ToChain: routingv1.Chain_CHAIN_BASE, BridgeName: "relay", Fee: 0.0001}},
	}}
	store := &fakePaymentStore{
		createOutcome: payment.Created,
		createResult: payment.Payment{
			ID: "metrics-test-id-testnet", Status: payment.StatusRouted, ExecutionMode: payment.ExecutionModeTestnet,
			CreatedAt: time.Now(), UpdatedAt: time.Now(),
		},
	}
	h := &Handler{
		Client: fakeRoute, Store: store, Metrics: metrics,
		BlockchainEnv: "testnet", QuoteRegistry: registry,
	}
	return h, metrics
}

func TestPostPayments_SimulatedMode_IncrementsPaymentsCreated(t *testing.T) {
	h, metrics := newTestHandlerWithMetrics(t)
	req := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(
		`{"source_chain":"ethereum","destination_chain":"base","asset":"usdc","amount":"100"}`))
	req.Header.Set("Idempotency-Key", t.Name())
	w := httptest.NewRecorder()
	h.PostPayments(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201, body: %s", w.Code, w.Body.String())
	}
	if got := testutil.ToFloat64(metrics.PaymentsCreated.WithLabelValues("simulated")); got != 1 {
		t.Errorf("PaymentsCreated{simulated} = %v, want 1", got)
	}
}

func TestPostPayments_TestnetMode_RecordsQuoteMetricsWithBoundedProviderLabels(t *testing.T) {
	h, metrics := newTestHandlerWithMetricsAndProviders(t,
		map[string]quote.Provider{
			"across": &fakeQuoteProvider{name: "across", quote: quote.Quote{ProviderName: "across", Available: true, FeeBaseUnits: big.NewInt(100), OutputAmountBaseUnits: big.NewInt(900), InputAmountBaseUnits: big.NewInt(1000)}},
			"relay":  &fakeQuoteProvider{name: "relay", quote: quote.Quote{ProviderName: "relay", Available: true, FeeBaseUnits: big.NewInt(50), OutputAmountBaseUnits: big.NewInt(950), InputAmountBaseUnits: big.NewInt(1000)}},
		})
	req := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(
		`{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`))
	req.Header.Set("Idempotency-Key", t.Name())
	w := httptest.NewRecorder()
	h.PostPayments(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201, body: %s", w.Code, w.Body.String())
	}
	for _, provider := range []string{"across", "relay"} {
		if got := testutil.ToFloat64(metrics.QuoteRequests.WithLabelValues(provider)); got != 1 {
			t.Errorf("QuoteRequests{%s} = %v, want 1", provider, got)
		}
	}
	// relay's fee (50) beats across's (100), so relay must be the one
	// credited with the win -- proves the metric reflects the ACTUAL
	// C++-selected winner, not just "whichever provider happened first."
	if got := testutil.ToFloat64(metrics.RoutingSelectedProvider.WithLabelValues("relay")); got != 1 {
		t.Errorf("RoutingSelectedProvider{relay} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.RoutingSelectedProvider.WithLabelValues("across")); got != 0 {
		t.Errorf("RoutingSelectedProvider{across} = %v, want 0 (across did not win)", got)
	}
}

// TestPostPayments_TestnetMode_PersistsBothWinningAndLosingQuotes proves
// the handler now builds candidate.Quotes from every fetched provider
// response (Task 1 of the payment-analytics-dashboard plan), not just the
// C++-router-selected winner -- required so a later dashboard view can
// honestly compare Across vs. Relay, including the quote that lost.
func TestPostPayments_TestnetMode_PersistsBothWinningAndLosingQuotes(t *testing.T) {
	registry := quote.NewRegistry()
	registry.Register(testnetChainKey, &fakeQuoteProvider{name: "across", quote: quote.Quote{
		ProviderName: "across", Available: true,
		FeeBaseUnits: big.NewInt(100), OutputAmountBaseUnits: big.NewInt(900), InputAmountBaseUnits: big.NewInt(1000),
		RawProviderPayload: json.RawMessage(`{}`),
	}})
	registry.Register(testnetChainKey, &fakeQuoteProvider{name: "relay", quote: quote.Quote{
		ProviderName: "relay", Available: true,
		FeeBaseUnits: big.NewInt(50), OutputAmountBaseUnits: big.NewInt(950), InputAmountBaseUnits: big.NewInt(1000),
		RawProviderPayload: json.RawMessage(`{}`),
	}})

	fakeRoute := &fakeClient{response: &routingv1.FindRouteResponse{
		RouteFound: true, TotalFee: 0.0001,
		Hops: []*routingv1.RouteHop{{FromChain: routingv1.Chain_CHAIN_ETHEREUM, ToChain: routingv1.Chain_CHAIN_BASE, BridgeName: "relay", Fee: 0.0001}},
	}}
	store := &fakePaymentStore{}
	h := &Handler{Client: fakeRoute, Store: store, BlockchainEnv: "testnet", QuoteRegistry: registry}

	rec := doPaymentRequest(h, "POST", "/payments", "both-quotes-persisted",
		`{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body = %s", rec.Code, rec.Body.String())
	}
	if len(store.lastCreate.Quotes) != 2 {
		t.Fatalf("expected 2 persisted quotes (winning and losing), got %d: %+v", len(store.lastCreate.Quotes), store.lastCreate.Quotes)
	}
	var selectedCount int
	var selectedProvider string
	for _, q := range store.lastCreate.Quotes {
		if q.Selected {
			selectedCount++
			selectedProvider = q.Provider
		}
	}
	if selectedCount != 1 {
		t.Fatalf("expected exactly 1 Quotes entry with Selected=true, got %d", selectedCount)
	}
	// Verify against the actual C++-router-selected winner (hops[0].BridgeName),
	// not an assumption baked into the test (e.g. "the cheaper one" or "the first one").
	wantWinner := fakeRoute.response.Hops[0].GetBridgeName()
	if selectedProvider != wantWinner {
		t.Errorf("selected provider = %q, want %q (hops[0].BridgeName)", selectedProvider, wantWinner)
	}
}

func TestPostPayments_QuoteProviderFailure_RecordsFailureMetricWithBoundedReason(t *testing.T) {
	h, metrics := newTestHandlerWithMetricsAndProviders(t,
		map[string]quote.Provider{
			"across": &fakeQuoteProvider{name: "across", err: errors.New("connection refused")},
		})
	req := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(
		`{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`))
	req.Header.Set("Idempotency-Key", t.Name())
	w := httptest.NewRecorder()
	h.PostPayments(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
	if got := testutil.ToFloat64(metrics.QuoteFailures.WithLabelValues("across", "http_error")); got != 1 {
		t.Errorf("QuoteFailures{across,http_error} = %v, want 1", got)
	}
}

// TestPostPayments_MetricsAndLoggerLeftNil_DoesNotPanic proves the
// nil-safe accessor pattern (established before Task 6) actually holds: a
// Handler literal that leaves Metrics/Logger unset -- exactly like every
// pre-Phase-10 test in this file -- must not panic.
//
// Unlike the task brief's literal snippet (which used a bare &fakeClient{},
// whose nil response makes the handler correctly return 422 "no route
// found" -- not a bug, just a fixture mismatch), this uses the same
// route-found fixture as TestPostPayments_SuccessfulCreation so the
// request actually reaches the Metrics/Logger-touching code paths that
// are the point of this test.
func TestPostPayments_MetricsAndLoggerLeftNil_DoesNotPanic(t *testing.T) {
	fakeRoute := &fakeClient{response: &routingv1.FindRouteResponse{
		RouteFound: true, TotalFee: 1.0,
		Hops: []*routingv1.RouteHop{{FromChain: routingv1.Chain_CHAIN_ETHEREUM, ToChain: routingv1.Chain_CHAIN_BASE, BridgeName: "Hop#1", Fee: 1.0}},
	}}
	store := &fakePaymentStore{
		createOutcome: payment.Created,
		createResult: payment.Payment{
			ID: "nil-metrics-logger-id", Status: payment.StatusRouted, ExecutionMode: payment.ExecutionModeSimulated,
			CreatedAt: time.Now(), UpdatedAt: time.Now(),
		},
	}
	h := &Handler{Client: fakeRoute, Store: store}
	req := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(
		`{"source_chain":"ethereum","destination_chain":"base","asset":"usdc","amount":"100"}`))
	req.Header.Set("Idempotency-Key", t.Name())
	w := httptest.NewRecorder()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("PostPayments panicked with Metrics/Logger left nil: %v", r)
		}
	}()
	h.PostPayments(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201, body: %s", w.Code, w.Body.String())
	}
}

// TestPostPayments_SimulatedMode_CompletesSuccessfullyWithUnreachableTracingBackend
// proves tracing failure cannot block or fail payment creation --
// engineering constraint: "instrumentation must not become a correctness
// dependency."
func TestPostPayments_SimulatedMode_CompletesSuccessfullyWithUnreachableTracingBackend(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "10.255.255.1:4317")
	shutdown, err := observability.InitTracing(context.Background(), "test")
	if err != nil {
		t.Fatalf("InitTracing: %v", err)
	}
	defer shutdown(context.Background())

	h, _ := newTestHandlerWithMetrics(t)
	req := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(
		`{"source_chain":"ethereum","destination_chain":"base","asset":"usdc","amount":"100"}`))
	req.Header.Set("Idempotency-Key", t.Name())
	w := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		h.PostPayments(w, req)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("PostPayments did not return within 5s -- tracing to an unreachable endpoint may be blocking the request")
	}
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201, body: %s", w.Code, w.Body.String())
	}
}
