package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"chainroute/go-api/internal/bridge/quote"
)

// wethAddressByChainID mirrors across.wethAddressByChainID -- confirmed
// live (Task 1) to be the same testnet addresses Across already uses.
var wethAddressByChainID = map[int64]string{
	11155111: "0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14",
	84532:    "0x4200000000000000000000000000000000000006",
}

const nativeAddress = "0x0000000000000000000000000000000000000000"

type quoteRequestBody struct {
	User                string `json:"user"`
	OriginChainID       int64  `json:"originChainId"`
	DestinationChainID  int64  `json:"destinationChainId"`
	OriginCurrency      string `json:"originCurrency"`
	DestinationCurrency string `json:"destinationCurrency"`
	Amount              string `json:"amount"`
	TradeType           string `json:"tradeType"`
}

type currencyInfo struct {
	Currency struct {
		ChainID int64  `json:"chainId"`
		Address string `json:"address"`
	} `json:"currency"`
	Amount        string `json:"amount"`
	MinimumAmount string `json:"minimumAmount"`
}

// quoteStepItemData is the transaction envelope Relay's response embeds --
// named (not anonymous) so it can be referenced from both quoteResponseBody
// and the helper below without repeating the field list twice.
type quoteStepItemData struct {
	To                   string `json:"to"`
	Data                 string `json:"data"`
	Value                string `json:"value"`
	ChainID              int64  `json:"chainId"`
	Gas                  string `json:"gas"`
	MaxFeePerGas         string `json:"maxFeePerGas"`
	MaxPriorityFeePerGas string `json:"maxPriorityFeePerGas"`
}

type quoteStepItem struct {
	Data quoteStepItemData `json:"data"`
}

type quoteStep struct {
	Kind  string          `json:"kind"`
	Items []quoteStepItem `json:"items"`
}

type quoteResponseBody struct {
	RequestID string      `json:"requestId"`
	Steps     []quoteStep `json:"steps"`
	Details   struct {
		CurrencyIn   currencyInfo `json:"currencyIn"`
		CurrencyOut  currencyInfo `json:"currencyOut"`
		TimeEstimate int64        `json:"timeEstimate"`
		Recipient    string       `json:"recipient"`
	} `json:"details"`
	Protocol struct {
		V2 struct {
			OrderData struct {
				Deadline int64 `json:"deadline"`
			} `json:"orderData"`
		} `json:"v2"`
	} `json:"protocol"`
}

// errorResponseBody is what a non-2xx /quote response's body actually
// contains (verified live, Task 1) -- distinct from quoteResponseBody.
type errorResponseBody struct {
	Message   string `json:"message"`
	ErrorCode string `json:"errorCode"`
	RequestID string `json:"requestId"`
}

// QuotePayload is exactly what Provider.GetQuote marshals into
// quote.Quote.RawProviderPayload, and what Provider.BuildTransaction
// (Task 7) unmarshals back.
type QuotePayload struct {
	To        string `json:"to"`
	Data      string `json:"data"`
	Value     string `json:"value"`
	ChainID   int64  `json:"chainId"`
	RequestID string `json:"requestId"`
	Deadline  int64  `json:"deadline"`
	Recipient string `json:"recipient"`
}

// Provider implements quote.Provider, quote.Signer, and quote.StatusChecker.
type Provider struct {
	Client        *Client
	WalletAddress common.Address
	QuoteTTL      time.Duration
}

func NewProvider(client *Client, walletAddress common.Address, quoteTTL time.Duration) *Provider {
	return &Provider{Client: client, WalletAddress: walletAddress, QuoteTTL: quoteTTL}
}

func (p *Provider) Name() string { return "relay" }

func (p *Provider) GetQuote(ctx context.Context, req quote.Request) (quote.Quote, error) {
	if req.Asset != "WETH" {
		return quote.Quote{}, fmt.Errorf("relay provider: unsupported asset %q (only WETH is supported)", req.Asset)
	}
	destAddr, ok := wethAddressByChainID[req.DestinationChainID]
	if !ok {
		return quote.Quote{}, fmt.Errorf("relay provider: unsupported destination chain %d", req.DestinationChainID)
	}
	if _, ok := wethAddressByChainID[req.SourceChainID]; !ok {
		return quote.Quote{}, fmt.Errorf("relay provider: unsupported source chain %d", req.SourceChainID)
	}

	now := time.Now().UTC()
	var resp quoteResponseBody
	err := p.Client.post(ctx, "/quote", quoteRequestBody{
		User: p.WalletAddress.Hex(), OriginChainID: req.SourceChainID, DestinationChainID: req.DestinationChainID,
		OriginCurrency: nativeAddress, DestinationCurrency: destAddr, // ETH-in/WETH-out -- design doc §7
		Amount: req.AmountBaseUnits.String(), TradeType: "EXACT_INPUT",
	}, &resp)
	if err != nil {
		if isNoRoutesFound(err) {
			return quote.Quote{ProviderName: p.Name(), SourceChainID: req.SourceChainID, DestinationChainID: req.DestinationChainID,
				Asset: req.Asset, Available: false, QuotedAt: now, ExpiresAt: now.Add(p.QuoteTTL)}, nil
		}
		return quote.Quote{}, fmt.Errorf("relay provider: get quote: %w", err)
	}

	if len(resp.Steps) != 1 || resp.Steps[0].Kind != "transaction" || len(resp.Steps[0].Items) != 1 {
		kind := ""
		if len(resp.Steps) > 0 {
			kind = resp.Steps[0].Kind
		}
		return quote.Quote{}, fmt.Errorf("relay provider: expected exactly one transaction step, got %d steps (kind[0]=%q) -- refusing an unsupported multi-step execution model", len(resp.Steps), kind)
	}
	item := resp.Steps[0].Items[0].Data

	if resp.Details.CurrencyIn.Currency.ChainID != req.SourceChainID || resp.Details.CurrencyIn.Currency.Address != nativeAddress {
		return quote.Quote{}, fmt.Errorf("relay provider: response currencyIn %+v does not match request", resp.Details.CurrencyIn)
	}
	if resp.Details.CurrencyOut.Currency.ChainID != req.DestinationChainID || resp.Details.CurrencyOut.Currency.Address != destAddr {
		return quote.Quote{}, fmt.Errorf("relay provider: response currencyOut %+v does not match request", resp.Details.CurrencyOut)
	}
	if item.ChainID != req.SourceChainID {
		return quote.Quote{}, fmt.Errorf("relay provider: transaction step chainId %d does not match origin chain %d", item.ChainID, req.SourceChainID)
	}

	outputAmount, ok := new(big.Int).SetString(resp.Details.CurrencyOut.MinimumAmount, 10)
	if !ok {
		return quote.Quote{}, fmt.Errorf("relay provider: minimumAmount %q is not a valid integer", resp.Details.CurrencyOut.MinimumAmount)
	}
	if outputAmount.Cmp(req.AmountBaseUnits) >= 0 {
		return quote.Quote{}, fmt.Errorf("relay provider: quoted output amount %s is not less than the input amount %s -- refusing an implausible quote", outputAmount, req.AmountBaseUnits)
	}
	feeAmount := new(big.Int).Sub(req.AmountBaseUnits, outputAmount)

	payload := QuotePayload{To: item.To, Data: item.Data, Value: item.Value, ChainID: item.ChainID,
		RequestID: resp.RequestID, Deadline: resp.Protocol.V2.OrderData.Deadline, Recipient: resp.Details.Recipient}
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return quote.Quote{}, fmt.Errorf("relay provider: marshal raw payload: %w", err)
	}

	return quote.Quote{
		ProviderName: p.Name(), SourceChainID: req.SourceChainID, DestinationChainID: req.DestinationChainID,
		Asset: req.Asset, InputAmountBaseUnits: req.AmountBaseUnits, OutputAmountBaseUnits: outputAmount,
		FeeBaseUnits: feeAmount, EstimatedFillTimeSec: resp.Details.TimeEstimate,
		Available: true, QuotedAt: now, ExpiresAt: now.Add(p.QuoteTTL), RawProviderPayload: rawPayload,
	}, nil
}

// isNoRoutesFound reports whether err is a Relay APIError whose body
// carries errorCode "NO_SWAP_ROUTES_FOUND" -- the signal that a route
// genuinely doesn't exist right now, distinct from a transport/API failure.
func isNoRoutesFound(err error) bool {
	apiErr, ok := err.(*APIError)
	if !ok {
		return false
	}
	var body errorResponseBody
	if jsonErr := json.Unmarshal([]byte(apiErr.Body), &body); jsonErr != nil {
		return false
	}
	return body.ErrorCode == "NO_SWAP_ROUTES_FOUND"
}
