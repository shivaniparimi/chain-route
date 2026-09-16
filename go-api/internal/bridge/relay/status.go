package relay

import (
	"context"
	"fmt"

	"chainroute/go-api/internal/bridge/quote"
)

type intentStatusResponse struct {
	Status     string   `json:"status"`
	TxHashes   []string `json:"txHashes"`
	InTxHashes []string `json:"inTxHashes"`
}

// relayStatusToState maps Relay's eight-value vocabulary (verified live,
// design doc §2) onto the shared ExternalState enum.
func relayStatusToState(raw string) quote.ExternalState {
	switch raw {
	case "success":
		return quote.StateFilled
	case "refund":
		return quote.StateRefunded
	case "failure":
		return quote.StateFillFailed
	default: // waiting, depositing, pending, submitted, delayed, unknown, or anything unrecognized
		return quote.StatePending
	}
}

func (p *Provider) CheckStatus(ctx context.Context, req quote.StatusRequest) (quote.StatusResult, error) {
	if req.ProviderReferenceID == "" {
		return quote.StatusResult{}, fmt.Errorf("relay: CheckStatus requires a ProviderReferenceID (requestId)")
	}
	var resp intentStatusResponse
	if err := p.Client.get(ctx, "/intents/status?requestId="+req.ProviderReferenceID, &resp); err != nil {
		return quote.StatusResult{}, err
	}
	result := quote.StatusResult{State: relayStatusToState(resp.Status), RawStatus: resp.Status}
	if len(resp.TxHashes) > 0 {
		result.DestinationTxHash = &resp.TxHashes[0]
	}
	return result, nil
}
