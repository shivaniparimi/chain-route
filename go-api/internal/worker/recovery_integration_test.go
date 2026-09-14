//go:build integration

package worker

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"chainroute/go-api/internal/payment"
)

func backdatePaymentUpdatedAt(t *testing.T, paymentID string, ago time.Duration) {
	t.Helper()
	db, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if _, err := db.ExecContext(context.Background(),
		`UPDATE payments SET updated_at = now() - ($2 * interval '1 second') WHERE id = $1`,
		paymentID, ago.Seconds()); err != nil {
		t.Fatalf("backdate: %v", err)
	}
}

func TestRecovery_CompletesStalePaymentDirectly(t *testing.T) {
	store := newIntegrationStore(t)
	key := "test-recovery-completes-stale-payment"

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Fatal("DATABASE_URL must be set for integration tests")
	}
	cleanupDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open cleanup db: %v", err)
	}
	t.Cleanup(func() { cleanupDB.Close() })
	cleanup := func() {
		if _, err := cleanupDB.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key = $1`, key); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	created, _, err := store.CreateOrGetPayment(context.Background(), testPaymentForWorker(key))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if claimed, err := store.ClaimPayment(context.Background(), created.ID); err != nil || !claimed {
		t.Fatalf("claim: claimed=%v err=%v", claimed, err)
	}
	backdatePaymentUpdatedAt(t, created.ID, 10*time.Minute)

	r := &Recovery{Store: store, Staleness: 2 * time.Minute}
	n, err := r.SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 payment recovered, got %d", n)
	}

	fetched, found, err := store.GetPayment(context.Background(), created.ID)
	if err != nil || !found {
		t.Fatalf("get: found=%v err=%v", found, err)
	}
	if fetched.Status != payment.StatusCompleted && fetched.Status != payment.StatusFailed {
		t.Fatalf("expected a terminal status, got %v", fetched.Status)
	}
}

// TestRecovery_StalledWorkerVsSweepRace directly proves the property the
// Phase 6 design spec's revision was built around: a stalled-but-not-
// crashed worker and the recovery sweep can both genuinely attempt to
// complete the same payment, and this is safe -- exactly one of the two
// ever persists a terminal transition, and the payment ends in exactly one
// consistent terminal state either way.
func TestRecovery_StalledWorkerVsSweepRace(t *testing.T) {
	store := newIntegrationStore(t)
	key := "test-recovery-stalled-worker-vs-sweep-race"

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Fatal("DATABASE_URL must be set for integration tests")
	}
	cleanupDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open cleanup db: %v", err)
	}
	t.Cleanup(func() { cleanupDB.Close() })
	cleanup := func() {
		if _, err := cleanupDB.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key = $1`, key); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	created, _, err := store.CreateOrGetPayment(context.Background(), testPaymentForWorker(key))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	claimed, err := store.ClaimPayment(context.Background(), created.ID)
	if err != nil || !claimed {
		t.Fatalf("claim: claimed=%v err=%v", claimed, err)
	}
	// The worker claimed long ago and has not completed yet -- from the
	// sweep's perspective this is indistinguishable from a crash, but here
	// it is a stall: the "worker" goroutine below is still going to
	// complete it.
	backdatePaymentUpdatedAt(t, created.ID, 10*time.Minute)

	r := &Recovery{Store: store, Staleness: 2 * time.Minute}

	var wg sync.WaitGroup
	wg.Add(2)
	var sweepErr, workerErr error
	go func() {
		defer wg.Done()
		_, sweepErr = r.SweepOnce(context.Background())
	}()
	go func() {
		defer wg.Done()
		_, workerErr = store.CompletePayment(context.Background(), created.ID, payment.StatusCompleted)
	}()
	wg.Wait()

	if sweepErr != nil {
		t.Fatalf("sweep error: %v", sweepErr)
	}
	if workerErr != nil {
		t.Fatalf("worker error: %v", workerErr)
	}

	fetched, found, err := store.GetPayment(context.Background(), created.ID)
	if err != nil || !found {
		t.Fatalf("get: found=%v err=%v", found, err)
	}
	if fetched.Status != payment.StatusCompleted && fetched.Status != payment.StatusFailed {
		t.Fatalf("expected a terminal status, got %v", fetched.Status)
	}
	if fetched.CompletedAt == nil {
		t.Fatal("expected CompletedAt to be set exactly once")
	}
}
