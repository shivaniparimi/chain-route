package across

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"chainroute/go-api/internal/bridge/quote"
)

// rawAcrossPayloadFixture matches the JSON shape across.QuotePayload
// marshals -- the same fixed values internal/worker/executor_test.go's
// rawAcrossPayload() helper uses, kept in sync deliberately since both
// exercise the same depositV3 signing inputs.
const rawAcrossPayloadFixture = `{"exclusiveRelayer":"0x0000000000000000000000000000000000000000","quoteTimestamp":"1789340112","fillDeadline":"1789347312","exclusivityDeadline":0,"spokePoolAddress":"0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662"}`

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

func TestProvider_BuildTransaction_ProducesExpectedEnvelope(t *testing.T) {
	p := &Provider{
		WalletAddress:    common.HexToAddress("0xAbC0000000000000000000000000000000000001"),
		SpokePoolAddress: common.HexToAddress("0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662"),
		WETHOrigin:       common.HexToAddress("0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14"),
		WETHDestination:  common.HexToAddress("0x4200000000000000000000000000000000000006"),
	}
	fresh := quote.Quote{
		ProviderName: "across", SourceChainID: 11155111, DestinationChainID: 84532,
		InputAmountBaseUnits: big.NewInt(1_000_000_000_000_000), OutputAmountBaseUnits: big.NewInt(997_592_172_330_233),
		RawProviderPayload: json.RawMessage(rawAcrossPayloadFixture),
	}

	env, err := p.BuildTransaction(context.Background(), fresh)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if env.To != p.SpokePoolAddress {
		t.Errorf("To = %s, want %s", env.To.Hex(), p.SpokePoolAddress.Hex())
	}
	if env.Value.Cmp(fresh.InputAmountBaseUnits) != 0 {
		t.Errorf("Value = %s, want %s", env.Value, fresh.InputAmountBaseUnits)
	}
	if env.ChainID != 11155111 {
		t.Errorf("ChainID = %d, want 11155111", env.ChainID)
	}
	if len(env.Data) == 0 {
		t.Error("Data must not be empty")
	}
}

// rawAcrossPayloadMismatchedSpokePoolFixture is identical to
// rawAcrossPayloadFixture except its spokePoolAddress is a different
// contract address than the one this package's tests configure on
// Provider.SpokePoolAddress -- used to prove BuildTransaction refuses to
// sign against a live quote that echoes a different SpokePool than what
// is configured.
const rawAcrossPayloadMismatchedSpokePoolFixture = `{"exclusiveRelayer":"0x0000000000000000000000000000000000000000","quoteTimestamp":"1789340112","fillDeadline":"1789347312","exclusivityDeadline":0,"spokePoolAddress":"0x1234567890123456789012345678901234567890"}`

// TestProvider_BuildTransaction_RejectsSpokePoolMismatch is the extraction
// of signAndBroadcastFresh's pre-refactor SpokePool echo-validation check
// (design spec §21) -- BuildTransaction must refuse to sign when the live
// quote's own echoed spokePoolAddress does not match p.SpokePoolAddress,
// the same defense-in-depth GetQuote already applies to echoed chain
// IDs/token addresses. This must happen BEFORE calldata is packed, so a
// passing spokePoolABI.Pack call is never reached with a mismatched
// SpokePool.
func TestProvider_BuildTransaction_RejectsSpokePoolMismatch(t *testing.T) {
	p := &Provider{
		WalletAddress:    common.HexToAddress("0xAbC0000000000000000000000000000000000001"),
		SpokePoolAddress: common.HexToAddress("0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662"),
		WETHOrigin:       common.HexToAddress("0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14"),
		WETHDestination:  common.HexToAddress("0x4200000000000000000000000000000000000006"),
	}
	fresh := quote.Quote{
		ProviderName: "across", SourceChainID: 11155111, DestinationChainID: 84532,
		InputAmountBaseUnits: big.NewInt(1_000_000_000_000_000), OutputAmountBaseUnits: big.NewInt(997_592_172_330_233),
		RawProviderPayload: json.RawMessage(rawAcrossPayloadMismatchedSpokePoolFixture),
	}

	env, err := p.BuildTransaction(context.Background(), fresh)
	if err == nil {
		t.Fatal("expected an error when the quoted SpokePool address does not match Provider.SpokePoolAddress, got nil")
	}
	if env.Data != nil {
		t.Errorf("expected a zero-value envelope on error, got Data = %x", env.Data)
	}
}

// TestProvider_CheckStatus_MapsAcrossStatusesToSharedStates also asserts
// that Provider.OriginChainID (not a hardcoded placeholder) is the value
// sent as the originChainId query parameter -- the fix called for in this
// task's brief Step 5 self-correction.
func TestProvider_CheckStatus_MapsAcrossStatusesToSharedStates(t *testing.T) {
	cases := []struct {
		acrossStatus string
		want         quote.ExternalState
	}{
		{"filled", quote.StateFilled},
		{"expired", quote.StateRefunded},
		{"refunded", quote.StateRefunded},
		{"pending", quote.StatePending},
		{"slowFillRequested", quote.StatePending},
		{"some-future-unknown-status", quote.StatePending},
	}
	for _, c := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if got := r.URL.Query().Get("originChainId"); got != "11155111" {
				t.Errorf("status %q: originChainId query param = %q, want 11155111", c.acrossStatus, got)
			}
			w.Write([]byte(fmt.Sprintf(`{"status":%q}`, c.acrossStatus)))
		}))
		defer srv.Close()
		p := &Provider{Client: NewClient(srv.URL), OriginChainID: 11155111}
		result, err := p.CheckStatus(context.Background(), quote.StatusRequest{OriginTxHash: "0xabc"})
		if err != nil {
			t.Fatalf("status %q: unexpected error: %v", c.acrossStatus, err)
		}
		if result.State != c.want {
			t.Errorf("status %q: State = %q, want %q", c.acrossStatus, result.State, c.want)
		}
		if result.RawStatus != c.acrossStatus {
			t.Errorf("status %q: RawStatus = %q, want %q", c.acrossStatus, result.RawStatus, c.acrossStatus)
		}
	}
}

// TestProvider_CheckStatus_DepositNotFoundIsNonTerminal uses a different
// OriginChainID (84532) than the mapping test above so a passing query-
// param assertion in both tests cannot be a coincidence of a single
// hardcoded value shared by test and implementation.
func TestProvider_CheckStatus_DepositNotFoundIsNonTerminal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("originChainId"); got != "84532" {
			t.Errorf("originChainId query param = %q, want 84532", got)
		}
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error":"DepositNotFoundException"}`))
	}))
	defer srv.Close()
	p := &Provider{Client: NewClient(srv.URL), OriginChainID: 84532}
	_, err := p.CheckStatus(context.Background(), quote.StatusRequest{OriginTxHash: "0xabc"})
	if !errors.Is(err, ErrDepositNotFound) {
		t.Fatalf("expected ErrDepositNotFound, got %v", err)
	}
}
