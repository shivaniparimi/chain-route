package worker

import (
	"context"
	"log/slog"
	"time"

	"chainroute/go-api/internal/execution"
	"chainroute/go-api/internal/observability"
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
	Metrics   *observability.Metrics
	Logger    *slog.Logger
}

// metrics returns r.Metrics, or a shared safe-to-record-into default when
// it is nil -- e.g. for an existing test's struct literal that predates
// this phase and never sets the field. Every instrumentation call in this
// file must go through this accessor, never through r.Metrics directly,
// so that a nil Metrics field can never nil-pointer-panic.
func (r *Recovery) metrics() *observability.Metrics {
	if r.Metrics != nil {
		return r.Metrics
	}
	return observability.DefaultMetrics()
}

// logger mirrors metrics: it returns r.Logger, or a shared default when
// nil. Every log call in this file must go through this accessor, never
// through r.Logger directly.
func (r *Recovery) logger() *slog.Logger {
	if r.Logger != nil {
		return r.Logger
	}
	return observability.DefaultLogger()
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
		outcome := "completed"
		if !result.Success {
			terminal = payment.StatusFailed
			outcome = "failed"
		}
		didComplete, createdAt, err := r.Store.CompletePayment(ctx, id, terminal)
		if err != nil {
			r.logger().ErrorContext(ctx, "recovery sweep failed to complete payment", "payment_id", id, "error", err)
			continue
		}
		if didComplete {
			completed++
			r.metrics().StaleRecoveries.WithLabelValues("recovery").Inc()
			r.metrics().PaymentDuration.WithLabelValues(string(payment.ExecutionModeSimulated), outcome).Observe(time.Since(createdAt).Seconds())
			if terminal == payment.StatusCompleted {
				r.metrics().PaymentsCompleted.WithLabelValues(string(payment.ExecutionModeSimulated)).Inc()
			} else {
				r.metrics().PaymentsFailed.WithLabelValues(string(payment.ExecutionModeSimulated), "execution").Inc()
			}
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
				r.logger().ErrorContext(ctx, "recovery sweep failed", "error", err)
			}
		}
	}
}
