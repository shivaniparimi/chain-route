package quote

import (
	"context"
	"encoding/json"
	"math/big"
	"time"
)

// Provider is one bridge's live quote source. Implementations (Task 6's
// across.Provider) wrap an existing provider-specific HTTP client -- this
// package never makes an HTTP call itself.
type Provider interface {
	Name() string
	GetQuote(ctx context.Context, req Request) (Quote, error)
}

// Request is what the router needs quoted: a specific route, asset, and
// exact amount.
type Request struct {
	SourceChainID      int64
	DestinationChainID int64
	Asset              string // normalized symbol, e.g. "WETH"
	AmountBaseUnits    *big.Int
}

// Quote is the normalized result of asking one provider for one route's
// current terms. Provider-specific transaction-construction inputs
// (Across's exclusiveRelayer/fillDeadline/etc.) are NOT typed fields here
// -- they travel opaquely in RawProviderPayload, decoded only by that
// provider's own execution code (design doc §4).
type Quote struct {
	ProviderName          string
	SourceChainID         int64
	DestinationChainID    int64
	Asset                 string
	InputAmountBaseUnits  *big.Int
	OutputAmountBaseUnits *big.Int
	FeeBaseUnits          *big.Int // InputAmountBaseUnits - OutputAmountBaseUnits
	EstimatedFillTimeSec  int64
	Available             bool // false: no viable route/liquidity for this amount right now
	QuotedAt              time.Time
	ExpiresAt             time.Time // ChainRoute policy TTL, not a protocol guarantee
	RawProviderPayload    json.RawMessage
}
