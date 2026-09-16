package relay

import (
	"context"
	"fmt"
	"log"
	"net/url"

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
	case "waiting", "depositing", "pending", "submitted", "delayed":
		return quote.StatePending
	default:
		// A genuinely unrecognized status (not one of Relay's documented
		// eight values, design doc §2) -- treated the same as pending
		// (never a silent crash, per the reconciler's must-never-fail
		// contract), but logged loudly (M8) so a future Relay API change
		// this client has never seen doesn't go unnoticed indefinitely.
		log.Printf("WARNING: relay: unrecognized status %q -- treating as pending", raw)
		return quote.StatePending
	}
}

func (p *Provider) CheckStatus(ctx context.Context, req quote.StatusRequest) (quote.StatusResult, error) {
	if req.ProviderReferenceID == "" {
		return quote.StatusResult{}, fmt.Errorf("relay: CheckStatus requires a ProviderReferenceID (requestId)")
	}
	q := url.Values{}
	q.Set("requestId", req.ProviderReferenceID)
	var resp intentStatusResponse
	if err := p.Client.get(ctx, "/intents/status", q, &resp); err != nil {
		return quote.StatusResult{}, err
	}
	result := quote.StatusResult{State: relayStatusToState(resp.Status), RawStatus: resp.Status}
	if len(resp.TxHashes) > 0 {
		result.DestinationTxHash = &resp.TxHashes[0]
	}
	return result, nil
}
