package across

import (
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"chainroute/go-api/internal/bridge/quote"
)

func TestProvider_GetQuote_HappyPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(realSuggestedFeesFixture))
	}))
	defer srv.Close()

	p := NewProvider(NewClient(srv.URL), 2*time.Minute)
	req := quote.Request{
		SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
		AmountBaseUnits: big.NewInt(1_000_000_000_000_000),
	}
	before := time.Now()
	q, err := p.GetQuote(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q.ProviderName != "across" {
		t.Errorf("ProviderName = %q, want across", q.ProviderName)
	}
	if !q.Available {
		t.Error("expected Available=true for a successful quote")
	}
	if q.OutputAmountBaseUnits.String() != "997592172330233" {
		t.Errorf("OutputAmountBaseUnits = %s, want 997592172330233", q.OutputAmountBaseUnits)
	}
	wantFee := new(big.Int).Sub(req.AmountBaseUnits, q.OutputAmountBaseUnits)
	if q.FeeBaseUnits.Cmp(wantFee) != 0 {
		t.Errorf("FeeBaseUnits = %s, want %s", q.FeeBaseUnits, wantFee)
	}
	if q.EstimatedFillTimeSec != 10 {
		t.Errorf("EstimatedFillTimeSec = %d, want 10", q.EstimatedFillTimeSec)
	}
	if q.QuotedAt.Before(before) {
		t.Error("QuotedAt should be set at call time, not zero/earlier")
	}
	if !q.ExpiresAt.After(q.QuotedAt) {
		t.Error("ExpiresAt must be after QuotedAt")
	}

	var payload QuotePayload
	if err := json.Unmarshal(q.RawProviderPayload, &payload); err != nil {
		t.Fatalf("RawProviderPayload did not unmarshal into QuotePayload: %v", err)
	}
	if payload.SpokePoolAddress != "0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662" {
		t.Errorf("payload.SpokePoolAddress = %q", payload.SpokePoolAddress)
	}
}

func TestProvider_GetQuote_AmountTooLowReturnsUnavailableNotError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"isAmountTooLow":true,"outputAmount":"0","fillDeadline":"0","exclusivityDeadline":0,"exclusiveRelayer":"0x0","timestamp":"0","spokePoolAddress":"0x0"}`))
	}))
	defer srv.Close()

	p := NewProvider(NewClient(srv.URL), time.Minute)
	q, err := p.GetQuote(context.Background(), quote.Request{
		SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
		AmountBaseUnits: big.NewInt(1),
	})
	if err != nil {
		t.Fatalf("expected nil error for an unavailable-but-successfully-answered quote, got %v", err)
	}
	if q.Available {
		t.Error("expected Available=false")
	}
}

func TestProvider_GetQuote_UnsupportedRouteIsAnError(t *testing.T) {
	p := NewProvider(NewClient("http://unused"), time.Minute)
	_, err := p.GetQuote(context.Background(), quote.Request{
		SourceChainID: 1, DestinationChainID: 2, Asset: "USDC",
		AmountBaseUnits: big.NewInt(1),
	})
	if err == nil {
		t.Fatal("expected an error for a route this provider does not support")
	}
}

func TestProvider_GetQuote_PropagatesTransientAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	p := NewProvider(NewClient(srv.URL), time.Minute)
	_, err := p.GetQuote(context.Background(), quote.Request{
		SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
		AmountBaseUnits: big.NewInt(1_000_000_000_000_000),
	})
	if err == nil {
		t.Fatal("expected a non-2xx API response to propagate as an error, not be swallowed into Available=false")
	}
}
