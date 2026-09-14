package worker

import (
	"context"
	"fmt"

	"chainroute/go-api/internal/events"
	"chainroute/go-api/internal/execution"
	"chainroute/go-api/internal/payment"
)

// PaymentStore is the subset of *postgres.Store the processor needs.
type PaymentStore interface {
	ClaimPayment(ctx context.Context, paymentID string) (bool, payment.ExecutionMode, error)
	CompletePayment(ctx context.Context, paymentID string, terminal payment.Status) (bool, error)
}

// Processor handles one PAYMENT_ROUTED event at a time.
type Processor struct {
	Store PaymentStore
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
	claimed, _, err := p.Store.ClaimPayment(ctx, evt.PaymentID)
	if err != nil {
		return fmt.Errorf("claim payment %s: %w", evt.PaymentID, err)
	}
	if !claimed {
		return nil
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
