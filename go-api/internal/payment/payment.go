package payment

import "time"

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
	ExternalStatusPending  ExternalStatus = "pending"
	ExternalStatusFilled   ExternalStatus = "filled"
	ExternalStatusExpired  ExternalStatus = "expired"
	ExternalStatusRefunded ExternalStatus = "refunded"
	ExternalStatusReverted ExternalStatus = "reverted"
)

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
	ID                 string
	PaymentID          string
	BridgeProvider     string
	OriginChainID      int64
	DestinationChainID int64
	WalletAddress      string
	Nonce              int64
	SignedTxHash       *string
	RawSignedTx        []byte
	BroadcastAt        *time.Time
	AcrossDepositID    *string
	ExternalStatus     ExternalStatus
	ConfirmedAt        *time.Time
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// CreateResult reports what Store.CreateOrGetPayment actually did.
type CreateResult int

const (
	Created  CreateResult = iota // a brand-new payment was inserted
	Replayed                     // an existing payment, same logical request, was returned
	Conflict                     // the idempotency key was already used with a different request
)
