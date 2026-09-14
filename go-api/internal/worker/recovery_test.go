package worker

import (
	"context"
	"testing"
	"time"

	"chainroute/go-api/internal/payment"
)

type fakeRecoveryStore struct {
	staleIDs          []string
	staleErr          error
	completeResult    bool
	completeErr       error
	completeCalls     []string
	completeTerminals map[string]payment.Status
}

func (f *fakeRecoveryStore) StalePaymentIDs(ctx context.Context, staleness time.Duration) ([]string, error) {
	return f.staleIDs, f.staleErr
}

func (f *fakeRecoveryStore) CompletePayment(ctx context.Context, paymentID string, terminal payment.Status) (bool, error) {
	if f.completeTerminals == nil {
		f.completeTerminals = map[string]payment.Status{}
	}
	f.completeTerminals[paymentID] = terminal
	f.completeCalls = append(f.completeCalls, paymentID)
	return f.completeResult, f.completeErr
}

func TestSweepOnce_CompletesStalePayments(t *testing.T) {
	id := findIDWithOutcome(t, true)
	store := &fakeRecoveryStore{staleIDs: []string{id}, completeResult: true}
	r := &Recovery{Store: store, Staleness: 2 * time.Minute}
	n, err := r.SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 completed, got %d", n)
	}
	if store.completeTerminals[id] != payment.StatusCompleted {
		t.Fatalf("expected COMPLETED, got %v", store.completeTerminals[id])
	}
}

func TestSweepOnce_SkipsPaymentsAlreadyCompletedByAnotherActor(t *testing.T) {
	id := "already-completed-elsewhere"
	store := &fakeRecoveryStore{staleIDs: []string{id}, completeResult: false}
	r := &Recovery{Store: store, Staleness: 2 * time.Minute}
	n, err := r.SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 completed (CompletePayment reported a no-op), got %d", n)
	}
}

func TestSweepOnce_PropagatesStaleLookupError(t *testing.T) {
	store := &fakeRecoveryStore{staleErr: context.DeadlineExceeded}
	r := &Recovery{Store: store, Staleness: 2 * time.Minute}
	if _, err := r.SweepOnce(context.Background()); err == nil {
		t.Fatal("expected an error when StalePaymentIDs fails")
	}
}
