package across

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"time"

	"chainroute/go-api/internal/bridge/quote"
)

// wethAddressByChainID is the same small, fixed testnet-asset map Phase 7
// already hardcodes in cmd/worker/main.go -- centralized here since
// Provider is now the one place that must resolve (chainID, asset) into
// an actual token address for a quote request.
var wethAddressByChainID = map[int64]string{
	11155111: "0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14", // Sepolia WETH
	84532:    "0x4200000000000000000000000000000000000006", // Base Sepolia WETH
}

// QuotePayload is exactly what Provider.GetQuote marshals into
// quote.Quote.RawProviderPayload, and exactly what DecodeQuotePayload
// unmarshals back -- the Across-specific fields BuildAndSignDepositV3Tx
// needs beyond the normalized Quote's own OutputAmountBaseUnits.
type QuotePayload struct {
	ExclusiveRelayer    string `json:"exclusiveRelayer"`
	QuoteTimestamp      string `json:"quoteTimestamp"`
	FillDeadline        string `json:"fillDeadline"`
	ExclusivityDeadline int64  `json:"exclusivityDeadline"`
	SpokePoolAddress    string `json:"spokePoolAddress"`
}

// DecodeQuotePayload is the inverse of the marshal Provider.GetQuote
// performs -- called by the worker (Task 11) to recover Across-specific
// signing inputs from a freshly-fetched quote.Quote's opaque payload.
func DecodeQuotePayload(raw json.RawMessage) (QuotePayload, error) {
	var p QuotePayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return QuotePayload{}, fmt.Errorf("decode across quote payload: %w", err)
	}
	return p, nil
}

// Provider implements quote.Provider by wrapping the existing,
// live-verified Client.SuggestedFees -- it introduces no second Across
// HTTP integration.
type Provider struct {
	Client   *Client
	QuoteTTL time.Duration
}

func NewProvider(client *Client, quoteTTL time.Duration) *Provider {
	return &Provider{Client: client, QuoteTTL: quoteTTL}
}

func (p *Provider) Name() string { return "across" }

func (p *Provider) GetQuote(ctx context.Context, req quote.Request) (quote.Quote, error) {
	if req.Asset != "WETH" {
		return quote.Quote{}, fmt.Errorf("across provider: unsupported asset %q (only WETH is supported)", req.Asset)
	}
	inputAddr, ok := wethAddressByChainID[req.SourceChainID]
	if !ok {
		return quote.Quote{}, fmt.Errorf("across provider: unsupported source chain %d", req.SourceChainID)
	}
	outputAddr, ok := wethAddressByChainID[req.DestinationChainID]
	if !ok {
		return quote.Quote{}, fmt.Errorf("across provider: unsupported destination chain %d", req.DestinationChainID)
	}

	now := time.Now().UTC()
	resp, err := p.Client.SuggestedFees(ctx, req.SourceChainID, req.DestinationChainID, inputAddr, outputAddr, req.AmountBaseUnits.String())
	if err != nil {
		if errors.Is(err, ErrAmountTooLow) {
			return quote.Quote{
				ProviderName: p.Name(), SourceChainID: req.SourceChainID, DestinationChainID: req.DestinationChainID,
				Asset: req.Asset, Available: false, QuotedAt: now, ExpiresAt: now.Add(p.QuoteTTL),
			}, nil
		}
		return quote.Quote{}, fmt.Errorf("across provider: get quote: %w", err)
	}

	// Validate the response's echoed chain IDs/addresses match what was
	// requested BEFORE trusting any of its numbers -- the same defense
	// Executor.validateQuote already applies at signing time (design §15),
	// applied here at quote-normalization time too.
	if resp.InputToken.ChainID != req.SourceChainID || resp.InputToken.Address != inputAddr {
		return quote.Quote{}, fmt.Errorf("across provider: response inputToken %+v does not match request", resp.InputToken)
	}
	if resp.OutputToken.ChainID != req.DestinationChainID || resp.OutputToken.Address != outputAddr {
		return quote.Quote{}, fmt.Errorf("across provider: response outputToken %+v does not match request", resp.OutputToken)
	}

	outputAmount, ok := new(big.Int).SetString(resp.OutputAmount, 10)
	if !ok {
		return quote.Quote{}, fmt.Errorf("across provider: outputAmount %q is not a valid integer", resp.OutputAmount)
	}
	feeAmount := new(big.Int).Sub(req.AmountBaseUnits, outputAmount)

	// quoteTimestamp is the same value the response calls "timestamp";
	// stored under the payload's own field name for BuildAndSignDepositV3Tx.
	payload := QuotePayload{
		ExclusiveRelayer: resp.ExclusiveRelayer, QuoteTimestamp: resp.Timestamp,
		FillDeadline: resp.FillDeadline, ExclusivityDeadline: resp.ExclusivityDeadline,
		SpokePoolAddress: resp.SpokePoolAddress,
	}
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return quote.Quote{}, fmt.Errorf("across provider: marshal raw payload: %w", err)
	}

	return quote.Quote{
		ProviderName: p.Name(), SourceChainID: req.SourceChainID, DestinationChainID: req.DestinationChainID,
		Asset: req.Asset, InputAmountBaseUnits: req.AmountBaseUnits, OutputAmountBaseUnits: outputAmount,
		FeeBaseUnits: feeAmount, EstimatedFillTimeSec: resp.EstimatedFillTimeSec,
		Available: true, QuotedAt: now, ExpiresAt: now.Add(p.QuoteTTL), RawProviderPayload: rawPayload,
	}, nil
}
