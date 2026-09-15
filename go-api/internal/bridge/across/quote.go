package across

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
)

// ErrAmountTooLow is returned by SuggestedFees when Across reports
// isAmountTooLow=true. Exposed as a sentinel (not just an error string)
// so callers -- specifically Provider.GetQuote in provider.go -- can
// distinguish "this route has no viable quote for this amount" from a
// genuine request/network failure via errors.Is, rather than string
// matching.
var ErrAmountTooLow = errors.New("across reports this amount is too low for the requested route")

// SuggestedFeesResponse is the subset of GET /suggested-fees's response
// this system actually uses to construct a depositV3 call. Field names
// and JSON tags are taken directly from a live captured response (see
// quote_test.go's realSuggestedFeesFixture), not guessed from docs alone.
type SuggestedFeesResponse struct {
	OutputAmount         string         `json:"outputAmount"`
	FillDeadline         string         `json:"fillDeadline"`
	ExclusivityDeadline  int64          `json:"exclusivityDeadline"`
	ExclusiveRelayer     string         `json:"exclusiveRelayer"`
	Timestamp            string         `json:"timestamp"`
	SpokePoolAddress     string         `json:"spokePoolAddress"`
	IsAmountTooLow       bool           `json:"isAmountTooLow"`
	EstimatedFillTimeSec int64          `json:"estimatedFillTimeSec"`
	InputToken           QuoteTokenInfo `json:"inputToken"`
	OutputToken          QuoteTokenInfo `json:"outputToken"`
}

// QuoteTokenInfo echoes the token/chain the quote was actually computed
// for -- used to validate the response matches what was requested before
// any of it flows into a signing call (design spec §21).
type QuoteTokenInfo struct {
	Address  string `json:"address"`
	ChainID  int64  `json:"chainId"`
	Decimals int    `json:"decimals"`
}

// SuggestedFees calls GET /suggested-fees. Per the Across docs (confirmed
// during Phase 7 planning), this response must NEVER be cached -- fees are
// market/gas/utilization-dependent and can change between calls, so every
// execution attempt must call this fresh, immediately before constructing
// the depositV3 transaction.
func (c *Client) SuggestedFees(ctx context.Context, originChainID, destinationChainID int64, inputToken, outputToken, amount string) (SuggestedFeesResponse, error) {
	q := url.Values{}
	q.Set("originChainId", strconv.FormatInt(originChainID, 10))
	q.Set("destinationChainId", strconv.FormatInt(destinationChainID, 10))
	q.Set("inputToken", inputToken)
	q.Set("outputToken", outputToken)
	q.Set("amount", amount)

	var out SuggestedFeesResponse
	if err := c.get(ctx, "/suggested-fees", q, &out); err != nil {
		return SuggestedFeesResponse{}, err
	}
	if out.IsAmountTooLow {
		return SuggestedFeesResponse{}, fmt.Errorf("%w: origin=%d destination=%d amount=%s", ErrAmountTooLow, originChainID, destinationChainID, amount)
	}
	return out, nil
}
