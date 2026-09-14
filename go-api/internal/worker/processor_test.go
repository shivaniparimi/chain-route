package worker

import (
	"context"
	"fmt"
	"testing"

	"chainroute/go-api/internal/events"
	"chainroute/go-api/internal/execution"
	"chainroute/go-api/internal/payment"
)

type fakeStore struct {
	claimResult    bool
	claimErr       error
	completeResult bool
	completeErr    error
	claimCalls     int
	completeCalls  int
	lastTerminal   payment.Status
}

func (f *fakeStore) ClaimPayment(ctx context.Context, paymentID string) (bool, error) {
	f.claimCalls++
	return f.claimResult, f.claimErr
}

func (f *fakeStore) CompletePayment(ctx context.Context, paymentID string, terminal payment.Status) (bool, error) {
	f.completeCalls++
	f.lastTerminal = terminal
	return f.completeResult, f.completeErr
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
