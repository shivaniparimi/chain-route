package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"chainroute/go-api/internal/events"
	"chainroute/go-api/internal/execution"
	"chainroute/go-api/internal/observability"
	"chainroute/go-api/internal/payment"
)

type fakeStore struct {
	claimResult       bool
	claimMode         payment.ExecutionMode
	claimErr          error
	completeResult    bool
	completeErr       error
	completeCreatedAt time.Time
	claimCalls        int
	completeCalls     int
	lastTerminal      payment.Status
}

func (f *fakeStore) ClaimPayment(ctx context.Context, paymentID string) (bool, payment.ExecutionMode, error) {
	f.claimCalls++
	mode := f.claimMode
	if mode == "" {
		mode = payment.ExecutionModeSimulated
	}
	return f.claimResult, mode, f.claimErr
}

func (f *fakeStore) CompletePayment(ctx context.Context, paymentID string, terminal payment.Status) (bool, time.Time, error) {
	f.completeCalls++
	f.lastTerminal = terminal
	createdAt := f.completeCreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	return f.completeResult, createdAt, f.completeErr
}

// findIDWithOutcome brute-forces a payment ID string for which the pure,
// deterministic execution.Execute produces the desired outcome, so tests
// can exercise both the success and failure paths deterministically.
func findIDWithOutcome(t *testing.T, wantSuccess bool) string {
	t.Helper()
	for i := 0; i < 10000; i++ {
		id := fmt.Sprintf("test-processor-id-%d", i)
		if execution.Execute(id).Success == wantSuccess {
			return id
		}
	}
	t.Fatalf("could not find a payment ID with Execute(...).Success == %v within 10000 tries", wantSuccess)
	return ""
}

func TestHandleRoutedPayment_SuccessPath(t *testing.T) {
	id := findIDWithOutcome(t, true)
	store := &fakeStore{claimResult: true, completeResult: true}
	proc := &Processor{Store: store}
	if err := proc.HandleRoutedPayment(context.Background(), events.RoutedPayment{PaymentID: id}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store.claimCalls != 1 {
		t.Fatalf("expected 1 claim call, got %d", store.claimCalls)
	}
	if store.completeCalls != 1 {
		t.Fatalf("expected 1 complete call, got %d", store.completeCalls)
	}
	if store.lastTerminal != payment.StatusCompleted {
		t.Fatalf("expected COMPLETED, got %v", store.lastTerminal)
	}
}

func TestHandleRoutedPayment_FailurePath(t *testing.T) {
	id := findIDWithOutcome(t, false)
	store := &fakeStore{claimResult: true, completeResult: true}
	proc := &Processor{Store: store}
	if err := proc.HandleRoutedPayment(context.Background(), events.RoutedPayment{PaymentID: id}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store.lastTerminal != payment.StatusFailed {
		t.Fatalf("expected FAILED, got %v", store.lastTerminal)
	}
}

func TestHandleRoutedPayment_AlreadyProcessingIsNoOp(t *testing.T) {
	store := &fakeStore{claimResult: false}
	proc := &Processor{Store: store}
	if err := proc.HandleRoutedPayment(context.Background(), events.RoutedPayment{PaymentID: "any-id"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store.completeCalls != 0 {
		t.Fatalf("expected CompletePayment to never be called when claim is a no-op, got %d calls", store.completeCalls)
	}
}

func TestHandleRoutedPayment_DuplicateDeliveryExecutesOnlyOnce(t *testing.T) {
	store := &fakeStore{claimResult: true, completeResult: true}
	proc := &Processor{Store: store}
	evt := events.RoutedPayment{PaymentID: "dup-delivery-id"}
	if err := proc.HandleRoutedPayment(context.Background(), evt); err != nil {
		t.Fatalf("first call: %v", err)
	}
	store.claimResult = false // simulate: already PROCESSING after the first call
	if err := proc.HandleRoutedPayment(context.Background(), evt); err != nil {
		t.Fatalf("second call: %v", err)
	}
	if store.completeCalls != 1 {
		t.Fatalf("expected exactly 1 CompletePayment call across both deliveries, got %d", store.completeCalls)
	}
}

func TestHandleRoutedPayment_ClaimErrorPropagates(t *testing.T) {
	store := &fakeStore{claimErr: fmt.Errorf("boom")}
	proc := &Processor{Store: store}
	err := proc.HandleRoutedPayment(context.Background(), events.RoutedPayment{PaymentID: "any-id"})
	if err == nil {
		t.Fatal("expected an error to propagate from a failing claim")
	}
	if store.completeCalls != 0 {
		t.Fatalf("expected CompletePayment not to be called after a claim error, got %d calls", store.completeCalls)
	}
}

type fakeTestnetExecutor struct {
	called    bool
	calledID  string
	returnErr error
}

func (f *fakeTestnetExecutor) ExecuteTestnetPayment(ctx context.Context, paymentID string) error {
	f.called = true
	f.calledID = paymentID
	return f.returnErr
}

func TestHandleRoutedPayment_TestnetModeDispatchesToExecutor(t *testing.T) {
	store := &fakeStore{claimResult: true, claimMode: payment.ExecutionModeTestnet}
	executor := &fakeTestnetExecutor{}
	proc := &Processor{Store: store, Executor: executor}

	evt := events.RoutedPayment{PaymentID: "testnet-pay-1"}
	if err := proc.HandleRoutedPayment(context.Background(), evt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !executor.called || executor.calledID != "testnet-pay-1" {
		t.Fatalf("expected Executor.ExecuteTestnetPayment to be called with the payment id, got called=%v id=%q", executor.called, executor.calledID)
	}
	if store.completeCalls != 0 {
		t.Fatal("testnet-mode dispatch must never call CompletePayment directly -- the executor/reconciler own that transition via MarkSubmitted/CompleteSubmittedPayment")
	}
}

func TestHandleRoutedPayment_SimulatedModeNeverCallsExecutor(t *testing.T) {
	id := findIDWithOutcome(t, true)
	store := &fakeStore{claimResult: true, claimMode: payment.ExecutionModeSimulated, completeResult: true}
	executor := &fakeTestnetExecutor{}
	proc := &Processor{Store: store, Executor: executor}

	if err := proc.HandleRoutedPayment(context.Background(), events.RoutedPayment{PaymentID: id}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if executor.called {
		t.Fatal("simulated-mode payments must never reach the testnet executor")
	}
}

func TestHandleRoutedPayment_TestnetModeWithNilExecutorErrors(t *testing.T) {
	store := &fakeStore{claimResult: true, claimMode: payment.ExecutionModeTestnet}
	proc := &Processor{Store: store, Executor: nil}

	err := proc.HandleRoutedPayment(context.Background(), events.RoutedPayment{PaymentID: "any"})
	if err == nil {
		t.Fatal("expected an error when a testnet-mode payment reaches a Processor with no Executor configured")
	}
}

func TestHandleRoutedPayment_SimulatedMode_RecordsProcessingMetricsAndPaymentDuration(t *testing.T) {
	id := findIDWithOutcome(t, true)
	metrics := observability.NewMetrics()
	store := &fakeStore{claimResult: true, claimMode: payment.ExecutionModeSimulated, completeResult: true, completeCreatedAt: time.Now().Add(-2 * time.Second)}
	proc := &Processor{Store: store, Metrics: metrics, Logger: observability.NewLogger("test")}

	if err := proc.HandleRoutedPayment(context.Background(), events.RoutedPayment{PaymentID: id}); err != nil {
		t.Fatalf("HandleRoutedPayment: %v", err)
	}
	if got := testutil.ToFloat64(metrics.EventsConsumed); got != 1 {
		t.Errorf("EventsConsumed = %v, want 1", got)
	}
	if got := testutil.CollectAndCount(metrics.PaymentDuration); got == 0 {
		t.Error("expected at least one PaymentDuration observation")
	}
	if got := testutil.CollectAndCount(metrics.ProcessingDuration); got == 0 {
		t.Error("expected at least one ProcessingDuration observation")
	}
}

func TestHandleRoutedPayment_ClaimFailure_RecordsProcessingFailure(t *testing.T) {
	metrics := observability.NewMetrics()
	store := &fakeStore{claimErr: errors.New("db down")}
	proc := &Processor{Store: store, Metrics: metrics, Logger: observability.NewLogger("test")}

	if err := proc.HandleRoutedPayment(context.Background(), events.RoutedPayment{PaymentID: "pay-1"}); err == nil {
		t.Fatal("expected an error")
	}
	if got := testutil.ToFloat64(metrics.ProcessingFailures.WithLabelValues("unknown", "claim_conflict")); got != 1 {
		t.Errorf("ProcessingFailures{unknown,claim_conflict} = %v, want 1", got)
	}
}
