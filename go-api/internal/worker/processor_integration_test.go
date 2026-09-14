//go:build integration

package worker

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"chainroute/go-api/internal/events"
	"chainroute/go-api/internal/payment"
	"chainroute/go-api/internal/postgres"
)

func newIntegrationStore(t *testing.T) *postgres.Store {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Fatal("DATABASE_URL must be set for integration tests")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("ping db: %v", err)
	}
	return postgres.New(db)
}

func testPaymentForWorker(idempotencyKey string) payment.Payment {
	return payment.Payment{
		IdempotencyKey:   idempotencyKey,
		SourceChain:      "ethereum",
		DestinationChain: "base",
		Asset:            "USDC",
		Amount:           "1000.00",
		TotalFee:         1.5,
		Hops: []payment.Hop{
			{HopIndex: 0, FromChain: "ethereum", ToChain: "base", BridgeName: "TestBridge#1",
				Fee: 1.5, LatencyMs: 500, Liquidity: 1000000, Reliability: 0.99},
		},
	}
}

func TestHandleRoutedPayment_ConcurrentDuplicateConsumers(t *testing.T) {
	store := newIntegrationStore(t)
	key := "test-worker-concurrent-duplicate-consumers"

	// This test uses a fixed idempotency key, so clean up before and after
	// so repeated invocations against the persistent local database don't
	// collide with their own leftovers.
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

	proc := &Processor{Store: store}
	evt := events.RoutedPayment{PaymentID: created.ID}

	const n = 10
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			if err := proc.HandleRoutedPayment(context.Background(), evt); err != nil {
				t.Errorf("HandleRoutedPayment: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	fetched, found, err := store.GetPayment(context.Background(), created.ID)
	if err != nil || !found {
		t.Fatalf("get after processing: found=%v err=%v", found, err)
	}
	if fetched.Status != payment.StatusCompleted && fetched.Status != payment.StatusFailed {
		t.Fatalf("expected a terminal status, got %v", fetched.Status)
	}
	if fetched.CompletedAt == nil {
		t.Fatal("expected CompletedAt to be set")
	}
}
