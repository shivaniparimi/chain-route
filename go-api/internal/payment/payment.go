package payment

import "time"

type Status string

const StatusRouted Status = "ROUTED"

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
	CreatedAt        time.Time
	UpdatedAt        time.Time
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

// CreateResult reports what Store.CreateOrGetPayment actually did.
type CreateResult int

const (
	Created  CreateResult = iota // a brand-new payment was inserted
	Replayed                     // an existing payment, same logical request, was returned
	Conflict                     // the idempotency key was already used with a different request
)
