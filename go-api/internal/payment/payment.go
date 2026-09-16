package payment

import (
	"encoding/json"
	"time"
)

type Status string

const (
	StatusRouted     Status = "ROUTED"
	StatusProcessing Status = "PROCESSING"
	StatusSubmitted  Status = "SUBMITTED"
	StatusCompleted  Status = "COMPLETED"
	StatusFailed     Status = "FAILED"
)

// ExecutionMode selects how a payment's routing decision is carried out:
// ExecutionModeSimulated (default, Phase 1-6 behavior, unchanged) or
// ExecutionModeTestnet (Phase 7: a real signed Sepolia -> Base Sepolia
// transaction via Across).
type ExecutionMode string

const (
	ExecutionModeSimulated ExecutionMode = "simulated"
	ExecutionModeTestnet   ExecutionMode = "testnet"
)

// ExternalStatus is payment_executions' own finer-grained state machine,
// living entirely within the payments.status = SUBMITTED window.
type ExternalStatus string

const (
	ExternalStatusPending    ExternalStatus = "pending"
	ExternalStatusFilled     ExternalStatus = "filled"
	ExternalStatusExpired    ExternalStatus = "expired"
	ExternalStatusRefunded   ExternalStatus = "refunded"
	ExternalStatusReverted   ExternalStatus = "reverted"
	ExternalStatusFillFailed ExternalStatus = "fill_failed" // Relay's "failure" (unsuccessful fill), distinct from reverted/refunded/expired
)

// Quote is the normalized bridge quote that produced a testnet-mode
// payment's winning route -- persisted once, atomically with the payment
// (design doc §7). nil for simulated-mode payments.
type Quote struct {
	ID                 string
	PaymentID          string
	Provider           string
	OriginChainID      int64
	DestinationChainID int64
	Asset              string
	// InputAmount/OutputAmount/FeeAmount are BASE-UNITS INTEGER decimal
	// strings (e.g. "1000000000000000"), NOT human-decimal amounts like
	// Payment.Amount ("0.001") -- deliberately different from Payment.Amount's
	// convention. This is required, not stylistic: the NUMERIC(38,0) columns
	// in migration 0005 (Task 7) have zero decimal places, and Task 11's
	// exceedsSlippageTolerance parses FeeAmount directly via
	// new(big.Int).SetString(feeAmount, 10), which fails on a string
	// containing a ".". Populate these with the *big.Int fields' own
	// .String() method (see Task 10 Step 4), never money.BaseUnitsToDecimal
	// (that conversion is for the CandidateEdge proto double fields only).
	InputAmount          string
	OutputAmount         string
	FeeAmount            string
	EstimatedFillTimeSec int64
	QuotedAt             time.Time
	ExpiresAt            time.Time
	RawProviderPayload   json.RawMessage
	CreatedAt            time.Time
}

type Payment struct {
	ID               string
	IdempotencyKey   string
	SourceChain      string
	DestinationChain string
	Asset            string
	Amount           string
	Status           Status
	TotalFee         float64
	Hops             []Hop
	ExecutionMode    ExecutionMode
	BridgeProvider   *string
	FailureReason    *string // populated only for the Phase 8 reasons: routing_quote_expired, fee_slippage_exceeded, route_unavailable, amount_exceeds_guardrail
	Quote            *Quote  // set by the caller before CreateOrGetPayment for testnet-mode; nil for simulated
	CreatedAt        time.Time
	UpdatedAt        time.Time
	CompletedAt      *time.Time
}

type Hop struct {
	HopIndex    int
	FromChain   string
	ToChain     string
	BridgeName  string
	Fee         float64
	LatencyMs   float64
	Liquidity   float64
	Reliability float64
}

// Execution is one row of payment_executions: everything about the real
// external side effect for one testnet-mode payment. UNIQUE(payment_id) in
// the schema guarantees at most one Execution ever exists per payment.
type Execution struct {
	ID                  string
	PaymentID           string
	BridgeProvider      string
	OriginChainID       int64
	DestinationChainID  int64
	WalletAddress       string
	Nonce               int64
	SignedTxHash        *string
	RawSignedTx         []byte
	BroadcastAt         *time.Time
	ProviderReferenceID *string // provider-agnostic external reference (Relay's requestId; unused/nil for Across)
	ExternalStatus      ExternalStatus
	RawExternalStatus   *string // the provider's own literal status string, for observability (design doc §16)
	ConfirmedAt         *time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// CreateResult reports what Store.CreateOrGetPayment actually did.
type CreateResult int

const (
	Created  CreateResult = iota // a brand-new payment was inserted
	Replayed                     // an existing payment, same logical request, was returned
	Conflict                     // the idempotency key was already used with a different request
)
