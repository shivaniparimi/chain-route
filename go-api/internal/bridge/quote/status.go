package quote

import "context"

// ExternalState is the shared, provider-agnostic vocabulary Reconciler
// operates on. Each provider's own richer status vocabulary is mapped down
// to one of these by that provider's own CheckStatus (design doc §14).
type ExternalState string

const (
	StatePending    ExternalState = "pending"
	StateFilled     ExternalState = "filled"
	StateRefunded   ExternalState = "refunded"
	StateReverted   ExternalState = "reverted"
	StateFillFailed ExternalState = "fill_failed"
)

// StatusRequest carries whichever identifier(s) a provider's own status API
// actually needs. Across uses OriginTxHash; Relay uses
// ProviderReferenceID. A provider ignores whichever field it doesn't need.
type StatusRequest struct {
	ProviderReferenceID string
	OriginTxHash        string
}

// StatusResult is the normalized outcome of one status check. RawStatus
// preserves the provider's own literal status word for observability, even
// though State collapses many raw values into one shared bucket.
type StatusResult struct {
	State             ExternalState
	RawStatus         string
	DestinationTxHash *string
}

// StatusChecker is implemented by any provider Reconciler can poll for a
// destination-side outcome.
type StatusChecker interface {
	Provider
	CheckStatus(ctx context.Context, req StatusRequest) (StatusResult, error)
}
