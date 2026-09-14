package worker

import (
	"context"
	"fmt"
	"time"

	"chainroute/go-api/internal/events"
	"chainroute/go-api/internal/execution"
	"chainroute/go-api/internal/payment"
)

// defaultTestnetHandleTimeout is used when Processor.TestnetTimeout is left
// at its zero value (e.g. in tests that don't care about timing). Sized
// comfortably larger than across.Client's own 15-second HTTP timeout plus
// the handful of additional EVM RPC round trips DriveExecutionForward makes
// (review Finding 4).
const defaultTestnetHandleTimeout = 30 * time.Second

// PaymentStore is the subset of *postgres.Store the processor needs.
type PaymentStore interface {
	ClaimPayment(ctx context.Context, paymentID string) (bool, payment.ExecutionMode, error)
	CompletePayment(ctx context.Context, paymentID string, terminal payment.Status) (bool, error)
}

// TestnetExecutor is the subset of *Executor Processor needs -- kept as an
// interface so processor_test.go can fake it without constructing a real
// wallet/RPC/Across client.
type TestnetExecutor interface {
	ExecuteTestnetPayment(ctx context.Context, paymentID string) error
}

// Processor handles one PAYMENT_ROUTED event at a time. Executor is nil
// when BLOCKCHAIN_ENV != testnet (cmd/worker only wires it when testnet
// execution is actually enabled) -- HandleRoutedPayment must never reach
// the testnet branch in that configuration, because the API layer (Task
// 15) refuses to create execution_mode="testnet" payments unless the
// server itself is configured for testnet, so no ROUTED testnet-mode
// payment can exist for Kafka to ever deliver in the first place.
type Processor struct {
	Store    PaymentStore
	Executor TestnetExecutor

	// TestnetTimeout bounds testnet-mode execution (ExecuteTestnetPayment)
	// with its own, independent context, distinct from whatever context the
	// caller passed into HandleRoutedPayment (cmd/worker's runConsumeLoop
	// uses a fixed 10-second handleCtx sized for Phase 6's simulated path,
	// which is too small for testnet execution's several sequential network
	// round trips -- review Finding 4). Zero value falls back to
	// defaultTestnetHandleTimeout. Simulated-mode handling is intentionally
	// untouched by this field and keeps using the caller's own context.
	TestnetTimeout time.Duration
}

// HandleRoutedPayment claims the payment and, only if the claim succeeds,
// runs execution and persists the terminal outcome. If the claim is a
// no-op (the payment is already PROCESSING or terminal), it returns
// immediately WITHOUT calling execution.Execute -- this is what keeps a
// tight duplicate-delivery loop (the same event handled twice back to
// back) from ever invoking Execute more than once. A genuinely stalled
// worker racing the recovery sweep is a different, accepted case (see the
// Phase 6 design spec §7, §11) that this function does not need to
// special-case: whichever of the two wins CompletePayment's guard is the
// one that persists.
func (p *Processor) HandleRoutedPayment(ctx context.Context, evt events.RoutedPayment) error {
	claimed, mode, err := p.Store.ClaimPayment(ctx, evt.PaymentID)
	if err != nil {
		return fmt.Errorf("claim payment %s: %w", evt.PaymentID, err)
	}
	if !claimed {
		return nil
	}

	if mode == payment.ExecutionModeTestnet {
		if p.Executor == nil {
			return fmt.Errorf("payment %s is execution_mode=testnet but this worker has no Executor configured (BLOCKCHAIN_ENV != testnet) -- this should be unreachable if the API layer's testnet gate is working", evt.PaymentID)
		}
		timeout := p.TestnetTimeout
		if timeout <= 0 {
			timeout = defaultTestnetHandleTimeout
		}
		// Deliberately a fresh, independent context here, not a child of
		// ctx: the caller's ctx (cmd/worker's handleCtx) is sized for the
		// Phase 6 simulated path and may already have little budget left by
		// the time ClaimPayment returns -- testnet execution needs its own
		// larger, independent timeout (review Finding 4), not a truncation
		// of whatever remains of the caller's.
		testnetCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return p.Executor.ExecuteTestnetPayment(testnetCtx, evt.PaymentID)
	}

	result := execution.Execute(evt.PaymentID)
	terminal := payment.StatusCompleted
	if !result.Success {
		terminal = payment.StatusFailed
	}

	if _, err := p.Store.CompletePayment(ctx, evt.PaymentID, terminal); err != nil {
		return fmt.Errorf("complete payment %s: %w", evt.PaymentID, err)
	}
	return nil
}
