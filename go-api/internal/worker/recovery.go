package worker

import (
	"context"
	"log"
	"time"

	"chainroute/go-api/internal/execution"
	"chainroute/go-api/internal/payment"
)

// RecoveryStore is the subset of *postgres.Store the recovery sweep needs.
type RecoveryStore interface {
	StalePaymentIDs(ctx context.Context, staleness time.Duration) ([]string, error)
	CompletePayment(ctx context.Context, paymentID string, terminal payment.Status) (bool, time.Time, error)
}

// Recovery periodically re-completes payments stuck in PROCESSING past a
// staleness threshold, directly (never routing back through ROUTED). The
// staleness threshold is a heuristic, not a certainty: a merely-stalled
// (not crashed) original worker can race this sweep and independently
// call execution.Execute for the same payment. That is safe because
// Execute is deterministic and CompletePayment's guard (WHERE
// status = 'PROCESSING') ensures only one of the two ever persists a
// terminal transition -- see the Phase 6 design spec §7, §11.
type Recovery struct {
	Store     RecoveryStore
	Staleness time.Duration
}

// SweepOnce runs one recovery pass, returning the number of payments it
// actually completed (payments already completed by another actor between
// the stale-ID lookup and this sweep's own attempt are not counted).
func (r *Recovery) SweepOnce(ctx context.Context) (int, error) {
	ids, err := r.Store.StalePaymentIDs(ctx, r.Staleness)
	if err != nil {
		return 0, err
	}
	completed := 0
	for _, id := range ids {
		result := execution.Execute(id)
		terminal := payment.StatusCompleted
		if !result.Success {
			terminal = payment.StatusFailed
		}
		didComplete, _, err := r.Store.CompletePayment(ctx, id, terminal)
		if err != nil {
			log.Printf("ERROR: recovery sweep failed to complete payment %s: %v", id, err)
			continue
		}
		if didComplete {
			completed++
		}
	}
	return completed, nil
}

// Run loops SweepOnce on the given interval until ctx is done.
func (r *Recovery) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := r.SweepOnce(ctx); err != nil {
				log.Printf("ERROR: recovery sweep failed: %v", err)
			}
		}
	}
}
