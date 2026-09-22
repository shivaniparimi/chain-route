package across

import (
	"context"
	"errors"
	"net/url"
	"strconv"
	"strings"

	"chainroute/go-api/internal/bridge/quote"
)

// DepositStatusResponse is GET /deposit/status's response. Status is left
// as a raw string (not an enum) deliberately: an unrecognized value here
// must never crash the reconciler (design spec §13 -- only a definitive,
// recognized terminal signal may ever produce FAILED).
//
// Across's own generated TypeScript interface for this endpoint (captured
// live during Phase 7 planning/verification)
// documents 11 possible status values, not the 4 originally
// assumed: "filled", "pending", "expired", "refunded",
// "slowFillRequested", "slowFilled", "deposit-pending", "deposit-failed",
// "auto-refund-pending", "manual-refund-required", "refund-failed". The
// last 7 belong to Across's deposit-address/PDA and HyperCore
// gasless-withdrawal products, which this system's direct depositV3 flow
// does not use, but the live API can still return them, and the surface
// may grow further. No real "filled" response body has been observed live
// (see findings doc), so this client cannot assume every documented field
// is always present.
//
// The reconciler (Task 13) is responsible for interpreting Status:
// "filled" is the only success/terminal-success case; "expired" and
// "refunded" are terminal-non-success; every other recognized value
// ("pending", "slowFillRequested", "slowFilled", "deposit-pending") is
// non-terminal/keep-polling; and -- critically -- any UNRECOGNIZED status
// string (a future value this client has never seen) must be treated the
// same as "still pending, retry later," with a warning logged including
// the raw string, and must never cause a parse failure or crash.
type DepositStatusResponse struct {
	Status string `json:"status"`
}

// ErrDepositNotFound is returned when Across reports
// DepositNotFoundException -- expected immediately after broadcast, before
// Across's indexer has observed the deposit yet, not itself a failure
// signal (design spec §13).
var ErrDepositNotFound = errors.New("across: deposit not found")

// DepositStatusByTxHash calls GET /deposit/status?originChainId=...&depositTxHash=...
// -- using our own persisted signed_tx_hash directly, confirmed as a valid
// query parameter during Phase 7 planning's live verification, avoiding
// any need to parse a FundsDeposited event out of the origin receipt to
// extract a numeric depositId.
func (c *Client) DepositStatusByTxHash(ctx context.Context, originChainID int64, depositTxHash string) (DepositStatusResponse, error) {
	q := url.Values{}
	q.Set("originChainId", strconv.FormatInt(originChainID, 10))
	q.Set("depositTxHash", depositTxHash)

	var out DepositStatusResponse
	err := c.get(ctx, "/deposit/status", q, &out)
	if err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && strings.Contains(apiErr.Body, "DepositNotFoundException") {
			return DepositStatusResponse{}, ErrDepositNotFound
		}
		return DepositStatusResponse{}, err
	}
	return out, nil
}

// acrossStatusToState maps Across's own status vocabulary onto the shared
// ExternalState enum -- a direct extraction of the mapping
// worker.Reconciler.checkAndUpdateOutcome already applied inline before
// this task, not new logic.
func acrossStatusToState(raw string) quote.ExternalState {
	switch raw {
	case "filled":
		return quote.StateFilled
	case "expired", "refunded":
		return quote.StateRefunded
	default:
		return quote.StatePending
	}
}

// CheckStatus implements quote.StatusChecker. p.OriginChainID must be set
// by the caller (cmd/worker/main.go) to the chain the deposit originated
// on -- DepositStatusByTxHash sends it as the live originChainId query
// parameter Across's API requires to disambiguate req.OriginTxHash.
func (p *Provider) CheckStatus(ctx context.Context, req quote.StatusRequest) (quote.StatusResult, error) {
	resp, err := p.Client.DepositStatusByTxHash(ctx, p.OriginChainID, req.OriginTxHash)
	if err != nil {
		return quote.StatusResult{}, err
	}
	return quote.StatusResult{State: acrossStatusToState(resp.Status), RawStatus: resp.Status}, nil
}
