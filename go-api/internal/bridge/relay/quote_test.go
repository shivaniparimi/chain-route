package relay

import (
	"context"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"chainroute/go-api/internal/bridge/quote"
)

const realQuoteFixture = `{"requestId":"0x1789582942e8e6519cfd826bc708bcf799f4705ca98482b0159b34b4297604ae","steps":[{"id":"deposit","action":"Confirm transaction in your wallet","description":"Depositing funds to the relayer to execute the swap for WETH","kind":"transaction","items":[{"status":"incomplete","data":{"from":"0x000000000000000000000000000000000000dEaD","to":"0x5feab8db4534f9f7e2669bb260c57a01ad1c12e3","data":"0xdeadbeef","value":"1000000000000000","chainId":11155111,"gas":"32432","maxFeePerGas":"1481105048","maxPriorityFeePerGas":"180155790"},"check":{"endpoint":"/intents/status?requestId=0x1789582942e8e6519cfd826bc708bcf799f4705ca98482b0159b34b4297604ae","method":"GET"}}],"requestId":"0x1789582942e8e6519cfd826bc708bcf799f4705ca98482b0159b34b4297604ae","depositAddress":""}],"fees":{},"details":{"operation":"swap","sender":"0x000000000000000000000000000000000000dEaD","recipient":"0x000000000000000000000000000000000000dEaD","currencyIn":{"currency":{"chainId":11155111,"address":"0x0000000000000000000000000000000000000000","symbol":"ETH","decimals":18},"amount":"1000000000000000"},"currencyOut":{"currency":{"chainId":84532,"address":"0x4200000000000000000000000000000000000006","symbol":"WETH","decimals":18},"amount":"991476614845915","minimumAmount":"960839987447177"},"timeEstimate":4},"protocol":{"v2":{"orderData":{"deadline":1790187742}}}}`

func TestGetQuote_HappyPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/quote" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.Write([]byte(realQuoteFixture))
	}))
	defer srv.Close()

	p := NewProvider(NewClient(srv.URL), common.HexToAddress("0x000000000000000000000000000000000000dEaD"), 0)
	q, err := p.GetQuote(context.Background(), quote.Request{
		SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
		AmountBaseUnits: big.NewInt(1_000_000_000_000_000),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q.ProviderName != "relay" {
		t.Errorf("ProviderName = %q, want relay", q.ProviderName)
	}
	if !q.Available {
		t.Error("expected Available=true")
	}
	// OutputAmountBaseUnits MUST be minimumAmount (960839987447177), not
	// expectedAmount (991476614845915) -- this is the economic-comparability
	// decision from design doc §7.
	if q.OutputAmountBaseUnits.String() != "960839987447177" {
		t.Errorf("OutputAmountBaseUnits = %s, want the minimumAmount 960839987447177, not the optimistic expectedAmount", q.OutputAmountBaseUnits)
	}
	wantFee := new(big.Int).Sub(big.NewInt(1_000_000_000_000_000), q.OutputAmountBaseUnits)
	if q.FeeBaseUnits.Cmp(wantFee) != 0 {
		t.Errorf("FeeBaseUnits = %s, want %s", q.FeeBaseUnits, wantFee)
	}
	if q.EstimatedFillTimeSec != 4 {
		t.Errorf("EstimatedFillTimeSec = %d, want 4", q.EstimatedFillTimeSec)
	}
}

func TestGetQuote_NoRoutesFoundReturnsUnavailableNotError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"message":"no routes found","errorCode":"NO_SWAP_ROUTES_FOUND","requestId":"0xabc"}`))
	}))
	defer srv.Close()

	p := NewProvider(NewClient(srv.URL), common.HexToAddress("0x000000000000000000000000000000000000dEaD"), 0)
	q, err := p.GetQuote(context.Background(), quote.Request{
		SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH", AmountBaseUnits: big.NewInt(1),
	})
	if err != nil {
		t.Fatalf("expected nil error for a no-route response, got %v", err)
	}
	if q.Available {
		t.Error("expected Available=false")
	}
}

func TestGetQuote_MultiStepResponseIsHardError(t *testing.T) {
	twoSteps := `{"requestId":"0xabc","steps":[{"id":"approve","kind":"transaction","items":[{"data":{"to":"0x1","data":"0x","value":"0","chainId":11155111}}]},{"id":"deposit","kind":"transaction","items":[{"data":{"to":"0x2","data":"0x","value":"1000000000000000","chainId":11155111}}]}],"details":{"currencyIn":{"currency":{"chainId":11155111,"address":"0x0000000000000000000000000000000000000000"},"amount":"1000000000000000"},"currencyOut":{"currency":{"chainId":84532,"address":"0x4200000000000000000000000000000000000006"},"minimumAmount":"1","amount":"1"},"timeEstimate":1}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(twoSteps))
	}))
	defer srv.Close()

	p := NewProvider(NewClient(srv.URL), common.HexToAddress("0x000000000000000000000000000000000000dEaD"), 0)
	_, err := p.GetQuote(context.Background(), quote.Request{
		SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH", AmountBaseUnits: big.NewInt(1_000_000_000_000_000),
	})
	if err == nil {
		t.Fatal("expected an error for a multi-step response -- Phase 9 only supports exactly one transaction step")
	}
}

func TestGetQuote_WrongEchoedChainOrAddressIsHardError(t *testing.T) {
	wrongChain := `{"requestId":"0xabc","steps":[{"id":"deposit","kind":"transaction","items":[{"data":{"to":"0x1","data":"0x","value":"1000000000000000","chainId":11155111}}]}],"details":{"currencyIn":{"currency":{"chainId":999999,"address":"0x0000000000000000000000000000000000000000"},"amount":"1000000000000000"},"currencyOut":{"currency":{"chainId":84532,"address":"0x4200000000000000000000000000000000000006"},"minimumAmount":"1","amount":"1"},"timeEstimate":1}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(wrongChain))
	}))
	defer srv.Close()

	p := NewProvider(NewClient(srv.URL), common.HexToAddress("0x000000000000000000000000000000000000dEaD"), 0)
	_, err := p.GetQuote(context.Background(), quote.Request{
		SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH", AmountBaseUnits: big.NewInt(1_000_000_000_000_000),
	})
	if err == nil {
		t.Fatal("expected an error when the echoed currencyIn.chainId doesn't match the request")
	}
}

func TestGetQuote_EmptyRequestIDIsHardError(t *testing.T) {
	emptyRequestID := `{"requestId":"","steps":[{"id":"deposit","kind":"transaction","items":[{"data":{"to":"0x1","data":"0x","value":"1000000000000000","chainId":11155111}}]}],"details":{"currencyIn":{"currency":{"chainId":11155111,"address":"0x0000000000000000000000000000000000000000"},"amount":"1000000000000000"},"currencyOut":{"currency":{"chainId":84532,"address":"0x4200000000000000000000000000000000000006"},"minimumAmount":"1","amount":"1"},"timeEstimate":1}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(emptyRequestID))
	}))
	defer srv.Close()

	p := NewProvider(NewClient(srv.URL), common.HexToAddress("0x000000000000000000000000000000000000dEaD"), 0)
	_, err := p.GetQuote(context.Background(), quote.Request{
		SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH", AmountBaseUnits: big.NewInt(1_000_000_000_000_000),
	})
	if err == nil {
		t.Fatal("expected an error when the response's requestId is empty -- we cannot reference this quote for reconciliation later")
	}
}

func TestGetQuote_UnsupportedAssetIsError(t *testing.T) {
	p := NewProvider(NewClient("http://unused"), common.HexToAddress("0x0"), 0)
	_, err := p.GetQuote(context.Background(), quote.Request{
		SourceChainID: 11155111, DestinationChainID: 84532, Asset: "USDC", AmountBaseUnits: big.NewInt(1),
	})
	if err == nil {
		t.Fatal("expected an error for an unsupported asset")
	}
}

// TestGetQuote_RequestBodyHasNativeInWETHOutAsymmetry pins the one piece of
// package-invisible business logic this whole task exists to encode: the
// outbound /quote request must ask for ETH-in/WETH-out (design doc §7), not
// WETH-in/WETH-out or any other combination. A regression that swapped
// originCurrency/destinationCurrency or hardcoded the wrong constant would
// not be caught by any response-handling test above.
func TestGetQuote_RequestBodyHasNativeInWETHOutAsymmetry(t *testing.T) {
	var captured quoteRequestBody
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}
		if err := json.Unmarshal(body, &captured); err != nil {
			t.Fatalf("unmarshal request body: %v", err)
		}
		w.Write([]byte(realQuoteFixture))
	}))
	defer srv.Close()

	p := NewProvider(NewClient(srv.URL), common.HexToAddress("0x000000000000000000000000000000000000dEaD"), 0)
	_, err := p.GetQuote(context.Background(), quote.Request{
		SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
		AmountBaseUnits: big.NewInt(1_000_000_000_000_000),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if captured.OriginCurrency != nativeAddress {
		t.Errorf("OriginCurrency = %q, want the native address %q", captured.OriginCurrency, nativeAddress)
	}
	if captured.DestinationCurrency != "0x4200000000000000000000000000000000000006" {
		t.Errorf("DestinationCurrency = %q, want Base Sepolia WETH 0x4200000000000000000000000000000000000006", captured.DestinationCurrency)
	}
}

// TestGetQuote_ImplausibleOutputAmountIsError guards against a malformed or
// adversarial provider response where minimumAmount >= the input amount,
// which would otherwise silently produce a negative (or zero) FeeBaseUnits
// that flows into downstream cost comparison and signing logic.
func TestGetQuote_ImplausibleOutputAmountIsError(t *testing.T) {
	fixture := `{"requestId":"0xabc","steps":[{"id":"deposit","kind":"transaction","items":[{"data":{"to":"0x1","data":"0x","value":"1000000000000000","chainId":11155111}}]}],"details":{"currencyIn":{"currency":{"chainId":11155111,"address":"0x0000000000000000000000000000000000000000"},"amount":"1000000000000000"},"currencyOut":{"currency":{"chainId":84532,"address":"0x4200000000000000000000000000000000000006"},"minimumAmount":"1000000000000000","amount":"1000000000000000"},"timeEstimate":1}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(fixture))
	}))
	defer srv.Close()

	p := NewProvider(NewClient(srv.URL), common.HexToAddress("0x000000000000000000000000000000000000dEaD"), 0)
	_, err := p.GetQuote(context.Background(), quote.Request{
		SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH", AmountBaseUnits: big.NewInt(1_000_000_000_000_000),
	})
	if err == nil {
		t.Fatal("expected an error when minimumAmount >= the input amount -- an implausible quote")
	}
}
