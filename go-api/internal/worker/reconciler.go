package worker

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"go.opentelemetry.io/otel/attribute"

	"chainroute/go-api/internal/bridge/quote"
	"chainroute/go-api/internal/observability"
	"chainroute/go-api/internal/payment"
)

// ReconcilerStore is the subset of *postgres.Store the reconciler needs,
// beyond what it reaches indirectly through Executor.
type ReconcilerStore interface {
	ExecutorStore
	GetExecutionByPaymentID(ctx context.Context, paymentID string) (payment.Execution, bool, error)
	StaleTestnetProcessingWithoutExecutionIDs(ctx context.Context, staleness time.Duration) ([]string, error)
	ReconciliationCandidates(ctx context.Context, staleness time.Duration) ([]payment.Execution, error)
	UpdateExecutionExternalStatus(ctx context.Context, executionID string, status payment.ExternalStatus, rawStatus string, confirmedAt *sql.NullTime) error
	CompleteSubmittedPayment(ctx context.Context, paymentID string, terminal payment.Status) (bool, time.Time, error)
	LowestUnconfirmedNonce(ctx context.Context, walletAddress string) (int64, bool, error)
}

// ReconcilerEthClient extends ExecutorEthClient with the read-only calls
// reconciliation itself needs (never a write beyond what Executor already
// performs via DriveExecutionForward).
type ReconcilerEthClient interface {
	ExecutorEthClient
	TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error)
	PendingNonceAt(ctx context.Context, account common.Address) (uint64, error)
}

// Reconciler is the fourth cmd/worker goroutine (design spec §15),
// distinct from Recovery (Phase 6, simulated-mode only): it answers "what
// actually happened externally," using chain RPC and the Across status
// API, not just Postgres.
type Reconciler struct {
	Store          ReconcilerStore
	Executor       *Executor
	OriginClient   ReconcilerEthClient
	StatusCheckers map[string]quote.StatusChecker // keyed by provider name, e.g. "across", "relay"
	WalletAddress  common.Address
	OriginChainID  int64
	Staleness      time.Duration
	Metrics        *observability.Metrics
	Logger         *slog.Logger
}

// metrics returns r.Metrics, or a shared safe-to-record-into default when
// it is nil -- e.g. for an existing test's struct literal that predates
// this phase and never sets the field. Every instrumentation call in this
// file must go through this accessor, never through r.Metrics directly,
// so that a nil Metrics field can never nil-pointer-panic.
func (r *Reconciler) metrics() *observability.Metrics {
	if r.Metrics != nil {
		return r.Metrics
	}
	return observability.DefaultMetrics()
}

// logger mirrors metrics: it returns r.Logger, or a shared default when
// nil. Every log call in this file must go through this accessor, never
// through r.Logger directly.
func (r *Reconciler) logger() *slog.Logger {
	if r.Logger != nil {
		return r.Logger
	}
	return observability.DefaultLogger()
}

// SweepOnce runs one full reconciliation pass MINUS the divergence check,
// which Run schedules on its own, separately configurable interval (see
// Run below) -- recovering stale crash-point-A payments, driving stale
// not-yet-broadcast executions forward, and checking already-broadcast
// executions' real outcome (lowest-nonce-first per wallet). Each phase
// logs and continues past a single item's error rather than aborting the
// whole sweep, mirroring Recovery.SweepOnce's own style.
func (r *Reconciler) SweepOnce(ctx context.Context) {
	r.recoverStaleProcessingWithoutExecution(ctx)
	r.driveStaleNotYetBroadcast(ctx)
	r.checkBroadcastOutcomes(ctx)
}

// recoverStaleProcessingWithoutExecution handles crash point A (design
// spec §12): a testnet-mode payment stuck PROCESSING with no execution
// row at all. Executor.ExecuteTestnetPayment already implements exactly
// the race-safe claim-then-drive-forward sequence this needs -- reused
// directly, not reimplemented.
func (r *Reconciler) recoverStaleProcessingWithoutExecution(ctx context.Context) {
	ids, err := r.Store.StaleTestnetProcessingWithoutExecutionIDs(ctx, r.Staleness)
	if err != nil {
		r.logger().ErrorContext(ctx, "reconciler: query stale testnet processing without execution", "error", err)
		return
	}
	for _, id := range ids {
		if err := r.Executor.ExecuteTestnetPayment(ctx, id); err != nil {
			r.logger().ErrorContext(ctx, "reconciler: recover stale processing payment", "payment_id", id, "error", err)
			continue
		}
		r.metrics().StaleRecoveries.WithLabelValues("reconciler").Inc()
	}
}

// driveStaleNotYetBroadcast handles crash points B/C: an execution row
// exists (with its nonce already fixed) but has not been broadcast, and
// has sat without progress past staleness. Re-fetches the row immediately
// before acting, since the original executor may have finished between
// ReconciliationCandidates' query and now (design spec §15) -- acting on
// a stale in-memory copy here could otherwise attempt to re-drive a row
// that's already moved on.
func (r *Reconciler) driveStaleNotYetBroadcast(ctx context.Context) {
	candidates, err := r.Store.ReconciliationCandidates(ctx, r.Staleness)
	if err != nil {
		r.logger().ErrorContext(ctx, "reconciler: query reconciliation candidates", "error", err)
		return
	}
	for _, c := range candidates {
		if c.BroadcastAt != nil {
			continue // handled by checkBroadcastOutcomes
		}
		fresh, found, err := r.Store.GetExecutionByPaymentID(ctx, c.PaymentID)
		if err != nil || !found {
			r.logger().ErrorContext(ctx, "reconciler: re-check execution before driving forward", "payment_id", c.PaymentID, "error", err)
			continue
		}
		if fresh.BroadcastAt != nil {
			continue // the original executor finished in the meantime
		}
		if err := r.Executor.DriveExecutionForward(ctx, fresh); err != nil {
			r.logger().ErrorContext(ctx, "reconciler: drive execution forward", "payment_id", c.PaymentID, "error", err)
		}
	}
}

// checkBroadcastOutcomes handles already-broadcast, unconfirmed
// executions, restricted to the LOWEST unconfirmed nonce per wallet
// (design spec §15): checking or rebroadcasting a higher nonce cannot
// possibly progress it while a lower one is unresolved, by ordinary EVM
// nonce-ordering rules, so this never spends RPC/API budget on nonces that
// structurally cannot mine yet.
func (r *Reconciler) checkBroadcastOutcomes(ctx context.Context) {
	candidates, err := r.Store.ReconciliationCandidates(ctx, r.Staleness)
	if err != nil {
		r.logger().ErrorContext(ctx, "reconciler: query reconciliation candidates", "error", err)
		return
	}

	lowestByWallet := map[string]payment.Execution{}
	for _, c := range candidates {
		if c.BroadcastAt == nil {
			continue
		}
		current, ok := lowestByWallet[c.WalletAddress]
		if !ok || c.Nonce < current.Nonce {
			lowestByWallet[c.WalletAddress] = c
		}
	}

	for _, exec := range lowestByWallet {
		if err := r.checkAndUpdateOutcome(ctx, exec); err != nil {
			r.logger().ErrorContext(ctx, "reconciler: check outcome", "execution_id", exec.ID, "payment_id", exec.PaymentID, "error", err)
		}
	}
}

func (r *Reconciler) checkAndUpdateOutcome(ctx context.Context, exec payment.Execution) error {
	ctx, span := observability.Tracer("reconcile").Start(ctx, "reconcile.check")
	defer span.End()
	providerLabel := observability.SanitizeProviderLabel(exec.BridgeProvider)
	span.SetAttributes(attribute.String("payment.provider", providerLabel), attribute.String("payment.id", exec.PaymentID))
	start := time.Now()

	if exec.SignedTxHash == nil {
		return fmt.Errorf("execution %s is marked broadcast but has no signed_tx_hash", exec.ID)
	}

	// Crash-point repair (review Finding 1): DriveExecutionForward performs
	// MarkExecutionBroadcast then MarkSubmitted as two separate statements.
	// If the process died in between, this execution row is exactly what we
	// see here -- broadcast_at set -- while the payment itself is still
	// durably stuck PROCESSING, and driveStaleNotYetBroadcast deliberately
	// skips any candidate with BroadcastAt set (it's handled here instead).
	// Repair it before doing anything else: MarkSubmitted's own
	// WHERE status = 'PROCESSING' guard makes this a safe no-op if the
	// payment is already SUBMITTED.
	if _, err := r.Store.MarkSubmitted(ctx, exec.PaymentID); err != nil {
		return fmt.Errorf("repair submitted status for payment %s: %w", exec.PaymentID, err)
	}

	hash := common.HexToHash(*exec.SignedTxHash)

	receipt, err := r.OriginClient.TransactionReceipt(ctx, hash)
	if err != nil {
		// Not yet mined, or a transient RPC error -- neither is a
		// definitive failure signal (design spec §13). Leave state as-is;
		// the next sweep retries.
		return nil
	}
	if receipt.Status == 0 {
		return r.markTerminal(ctx, exec, quote.StatusResult{State: quote.StateReverted, RawStatus: "origin_reverted"}, payment.StatusFailed)
	}

	checker, ok := r.StatusCheckers[exec.BridgeProvider]
	if !ok {
		return fmt.Errorf("execution %s uses provider %q, which this reconciler has no configured status checker for", exec.ID, exec.BridgeProvider)
	}
	result, err := checker.CheckStatus(ctx, quote.StatusRequest{
		ProviderReferenceID: derefOrEmpty(exec.ProviderReferenceID),
		OriginTxHash:        *exec.SignedTxHash,
	})
	if err != nil {
		return nil // transient API error -- not a failure signal (design spec §13), retry next sweep
	}
	// Counted here, not on the transient-error early return above: a
	// reconciliation is "we got a real, definitive answer from the status
	// checker" (chainroute_reconciliations_total's own Help text: "Total
	// reconciliation status checks per provider"), whether that answer
	// turns out to be terminal (StateFilled/Refunded/Reverted/FillFailed)
	// or still StatePending -- distinct from ExecutionsCompleted/
	// ExecutionsFailed below, which only fire on the terminal branches.
	r.metrics().Reconciliations.WithLabelValues(providerLabel).Inc()
	r.metrics().ReconciliationDuration.WithLabelValues(providerLabel).Observe(time.Since(start).Seconds())

	switch result.State {
	case quote.StateFilled:
		return r.markTerminal(ctx, exec, result, payment.StatusCompleted)
	case quote.StateRefunded, quote.StateReverted, quote.StateFillFailed:
		return r.markTerminal(ctx, exec, result, payment.StatusFailed)
	default: // quote.StatePending
		return nil
	}
}

// derefOrEmpty returns the empty string for a nil pointer rather than
// panicking -- Across-provider executions never populate
// exec.ProviderReferenceID (it's Relay-only, design doc §16), and this
// dispatch path must work for either provider.
func derefOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// externalStateToStatus maps the shared, provider-agnostic
// quote.ExternalState back onto payment.ExternalStatus for persistence.
// The two enums are intentionally distinct types (design doc §6's
// rationale for two small interfaces applies to their result types too):
// quote.ExternalState is Reconciler's own polling vocabulary, while
// payment.ExternalStatus is payment_executions' durable column.
func externalStateToStatus(s quote.ExternalState) payment.ExternalStatus {
	switch s {
	case quote.StateFilled:
		return payment.ExternalStatusFilled
	case quote.StateRefunded:
		return payment.ExternalStatusRefunded
	case quote.StateReverted:
		return payment.ExternalStatusReverted
	case quote.StateFillFailed:
		return payment.ExternalStatusFillFailed
	default:
		return payment.ExternalStatusPending
	}
}

func (r *Reconciler) markTerminal(ctx context.Context, exec payment.Execution, result quote.StatusResult, terminal payment.Status) error {
	providerLabel := observability.SanitizeProviderLabel(exec.BridgeProvider)
	external := externalStateToStatus(result.State)
	confirmedAt := &sql.NullTime{Time: time.Now().UTC(), Valid: true}
	if err := r.Store.UpdateExecutionExternalStatus(ctx, exec.ID, external, result.RawStatus, confirmedAt); err != nil {
		return fmt.Errorf("record %s: %w", external, err)
	}
	completed, createdAt, err := r.Store.CompleteSubmittedPayment(ctx, exec.PaymentID, terminal)
	if err != nil {
		return fmt.Errorf("complete payment as %s: %w", terminal, err)
	}
	if !completed {
		// completed=false means CompleteSubmittedPayment's own
		// WHERE status = 'SUBMITTED' guard affected zero rows even though
		// we just made a definitive terminal observation on-chain/via
		// Across. That should be unreachable -- checkAndUpdateOutcome
		// always repairs PROCESSING -> SUBMITTED via MarkSubmitted first --
		// so silently discarding this (review Finding 1) would hide a
		// genuine state-machine bug or double-completion race. Log loudly
		// rather than error: the terminal observation itself was already
		// recorded above, and erroring here would just cause the sweep to
		// retry an update that will never succeed.
		//
		// Deliberately return here BEFORE any of the completion metrics
		// below: completed=false means no real, new payment transition
		// happened (guard-suppressed no-op), so recording
		// ExecutionsCompleted/ExecutionsFailed/PaymentDuration/
		// PaymentsCompleted/PaymentsFailed here would
		// double-count against whatever actor already completed this
		// payment -- the same class of bug Task 9's review caught and
		// fixed for Executor's Broadcasts metric (gate on the operation's
		// real effect, not merely a nil error).
		r.logger().ErrorContext(ctx, "reconciler: CompleteSubmittedPayment affected no rows despite a definitive terminal observation -- the payment was not in SUBMITTED status; this indicates an unexpected state transition and needs investigation",
			"payment_id", exec.PaymentID, "execution_id", exec.ID, "terminal", string(terminal), "external_status", string(external))
		return nil
	}
	outcome := "completed"
	if terminal == payment.StatusFailed {
		outcome = "failed"
		r.metrics().ExecutionsFailed.WithLabelValues(providerLabel, "reconciled_failed").Inc()
	} else {
		r.metrics().ExecutionsCompleted.WithLabelValues(providerLabel).Inc()
	}
	r.metrics().PaymentDuration.WithLabelValues(string(payment.ExecutionModeTestnet), outcome).Observe(time.Since(createdAt).Seconds())
	if terminal == payment.StatusCompleted {
		r.metrics().PaymentsCompleted.WithLabelValues(string(payment.ExecutionModeTestnet)).Inc()
	} else {
		r.metrics().PaymentsFailed.WithLabelValues(string(payment.ExecutionModeTestnet), "reconciliation").Inc()
	}
	return nil
}

// checkNonceDivergence is a read-only diagnostic (design spec §15): it
// NEVER writes to wallet_nonces. It only logs a warning if the chain's own
// pending nonce count falls behind what ChainRoute's own durable state
// expects, which would indicate a nonce got stuck or the dedicated-wallet
// invariant (§18) was violated by out-of-band wallet use.
func (r *Reconciler) checkNonceDivergence(ctx context.Context) {
	chainPending, err := r.OriginClient.PendingNonceAt(ctx, r.WalletAddress)
	if err != nil {
		r.logger().WarnContext(ctx, "reconciler: divergence check: query chain pending nonce", "error", err)
		return
	}
	lowest, found, err := r.Store.LowestUnconfirmedNonce(ctx, r.WalletAddress.Hex())
	if err != nil {
		r.logger().WarnContext(ctx, "reconciler: divergence check: query lowest unconfirmed nonce", "error", err)
		return
	}
	if !found {
		return
	}
	if int64(chainPending) < lowest {
		r.logger().WarnContext(ctx, "reconciler: nonce divergence -- diagnostic only, wallet_nonces is never adjusted automatically",
			"wallet_address", r.WalletAddress.Hex(), "chain_pending_nonce", chainPending, "lowest_unconfirmed_nonce", lowest)
	}
}

// Run loops SweepOnce on sweepInterval and the read-only divergence check
// on its own, separately configurable divergenceInterval (design spec §31
// deliberately names NONCE_DIVERGENCE_CHECK_INTERVAL_SECONDS as a distinct
// knob from RECONCILE_SWEEP_INTERVAL_SECONDS, since the divergence check
// is a cheap diagnostic that doesn't need to run as often as the
// correctness-critical sweep). Both tickers share one shutdown path,
// mirroring Recovery.Run and Publisher.Run's existing pattern.
func (r *Reconciler) Run(ctx context.Context, sweepInterval, divergenceInterval time.Duration) {
	sweepTicker := time.NewTicker(sweepInterval)
	defer sweepTicker.Stop()
	divergenceTicker := time.NewTicker(divergenceInterval)
	defer divergenceTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-sweepTicker.C:
			r.SweepOnce(ctx)
		case <-divergenceTicker.C:
			r.checkNonceDivergence(ctx)
		}
	}
}
