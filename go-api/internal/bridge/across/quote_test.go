package across

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// realSuggestedFeesFixture is the exact response this plan's own live
// verification against https://testnet.across.to/api/suggested-fees
// captured for the Sepolia WETH -> Base Sepolia WETH route (amount =
// 0.001 WETH in wei). Using a captured real response, not a hand-written
// approximation, means this test would have caught a shape mismatch
// against the actual API.
const realSuggestedFeesFixture = `{"estimatedFillTimeSec":10,"capitalFeePct":"99958333334000","capitalFeeTotal":"99958333334","relayGasFeePct":"981018513630000","relayGasFeeTotal":"981018513630","relayFeePct":"2407827669767028","relayFeeTotal":"2407827669767","lpFeePct":"0","timestamp":"1789340112","isAmountTooLow":false,"quoteBlock":"11698950","exclusiveRelayer":"0x0000000000000000000000000000000000000000","exclusivityDeadline":0,"spokePoolAddress":"0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662","destinationSpokePoolAddress":"0x82B564983aE7274c86695917BBf8C99ECb6F0F8F","totalRelayFee":{"pct":"2407827669767028","total":"2407827669767"},"relayerCapitalFee":{"pct":"99958333334000","total":"99958333334"},"relayerGasFee":{"pct":"981018513630000","total":"981018513630"},"lpFee":{"pct":"1326850822803028","total":"1326850822803"},"internalizedSwapFee":{"pct":"0","total":"0"},"limits":{"minDeposit":"3925643657709","maxDeposit":"3231735644024318","maxDepositInstant":"3231735644024318","maxDepositShortDelay":"3231735644024318","recommendedDepositInstant":"3231735644024318"},"fillDeadline":"1789347312","outputAmount":"997592172330233","inputToken":{"address":"0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14","symbol":"WETH","decimals":18,"chainId":11155111},"outputToken":{"address":"0x4200000000000000000000000000000000000006","symbol":"WETH","decimals":18,"chainId":84532},"id":"6dr22-1789340515991-1eb23d940902"}`

func TestSuggestedFees_ParsesRealCapturedResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/suggested-fees" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(realSuggestedFeesFixture))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	resp, err := c.SuggestedFees(context.Background(), 11155111, 84532,
		"0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14", "0x4200000000000000000000000000000000000006", "1000000000000000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.OutputAmount != "997592172330233" {
		t.Fatalf("unexpected OutputAmount: %s", resp.OutputAmount)
	}
	if resp.SpokePoolAddress != "0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662" {
		t.Fatalf("unexpected SpokePoolAddress: %s", resp.SpokePoolAddress)
	}
	if resp.ExclusiveRelayer != "0x0000000000000000000000000000000000000000" {
		t.Fatalf("unexpected ExclusiveRelayer: %s", resp.ExclusiveRelayer)
	}
	if resp.IsAmountTooLow {
		t.Fatal("expected IsAmountTooLow=false")
	}
}

func TestSuggestedFees_RejectsAmountTooLow(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"isAmountTooLow":true,"outputAmount":"0","fillDeadline":"0","exclusivityDeadline":0,"exclusiveRelayer":"0x0","timestamp":"0","spokePoolAddress":"0x0"}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	_, err := c.SuggestedFees(context.Background(), 11155111, 84532, "0xin", "0xout", "1")
	if err == nil {
		t.Fatal("expected an error when the API reports isAmountTooLow")
	}
}

func TestSuggestedFees_AmountTooLowIsErrAmountTooLow(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"isAmountTooLow":true,"outputAmount":"0","fillDeadline":"0","exclusivityDeadline":0,"exclusiveRelayer":"0x0","timestamp":"0","spokePoolAddress":"0x0"}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	_, err := c.SuggestedFees(context.Background(), 11155111, 84532, "0xin", "0xout", "1")
	if !errors.Is(err, ErrAmountTooLow) {
		t.Fatalf("expected errors.Is(err, ErrAmountTooLow), got %v", err)
	}
}

func TestSuggestedFees_ParsesEstimatedFillTimeSec(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(realSuggestedFeesFixture))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	resp, err := c.SuggestedFees(context.Background(), 11155111, 84532,
		"0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14", "0x4200000000000000000000000000000000000006", "1000000000000000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.EstimatedFillTimeSec != 10 {
		t.Fatalf("EstimatedFillTimeSec = %d, want 10 (per realSuggestedFeesFixture)", resp.EstimatedFillTimeSec)
	}
}

func TestSuggestedFees_PropagatesAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"InvalidParamError","message":"bad input"}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	_, err := c.SuggestedFees(context.Background(), 1, 2, "a", "b", "1")
	if err == nil {
		t.Fatal("expected an error for a non-2xx response")
	}
}
