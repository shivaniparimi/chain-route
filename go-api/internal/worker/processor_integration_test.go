//go:build integration

package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

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

// countingTestnetExecutor is a TestnetExecutor fake that counts how many
// times it was invoked. It is only ever actually called concurrently by
// TestHandleRoutedPayment_ConcurrentDuplicateConsumers's testnet subtests
// in the buggy case (ClaimPayment's real ROUTED-only guard failing to
// serialize duplicate deliveries) -- the mutex exists so that a real bug
// would surface as a wrong count rather than a data race masking it.
type countingTestnetExecutor struct {
	mu        sync.Mutex
	calls     int
	lastID    string
	returnErr error
}

func (f *countingTestnetExecutor) ExecuteTestnetPayment(ctx context.Context, paymentID string) error {
	f.mu.Lock()
	f.calls++
	f.lastID = paymentID
	f.mu.Unlock()
	return f.returnErr
}

func (f *countingTestnetExecutor) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// testnetQuoteFixture builds the Quote/Hop combination CreateOrGetPayment
// requires for a testnet-mode payment won by provider (design doc §7):
// same shape as the postgres package's own quote_store_integration_test.go
// fixtures, just inlined here since worker package tests can't import
// postgres's unexported test helpers.
func testnetQuoteFixture(provider string) (*payment.Quote, []payment.Hop) {
	now := time.Now().UTC()
	hops := []payment.Hop{{HopIndex: 0, FromChain: "ethereum", ToChain: "base", BridgeName: provider,
		Fee: 0.0001, LatencyMs: 4000, Liquidity: 0.001, Reliability: 1.0}}
	quote := &payment.Quote{
		Provider: provider, OriginChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
		InputAmount: "1000000000000000", OutputAmount: "999900000000000", FeeAmount: "100000000000",
		EstimatedFillTimeSec: 4, QuotedAt: now, ExpiresAt: now.Add(time.Minute),
		RawProviderPayload: json.RawMessage(`{"requestId":"0xabc"}`),
	}
	return quote, hops
}

// TestHandleRoutedPayment_ConcurrentDuplicateConsumers proves, against the
// real Postgres store (not a fake), that ClaimPayment's ROUTED-only guard
// makes duplicate Kafka delivery of the same PAYMENT_ROUTED event a safe
// no-op past the first successful claim -- for every bridge provider a
// routed payment can carry, not just the original simulated-mode-only
// case. It is table-driven/subtest (t.Run) per provider rather than three
// copies of the same body, since only the payment fixture and the
// post-condition assertion (terminal status vs. exactly-one-executor-call)
// differ.
func TestHandleRoutedPayment_ConcurrentDuplicateConsumers(t *testing.T) {
	cases := []struct {
		name          string
		executionMode payment.ExecutionMode
		provider      string // "" for simulated
	}{
		{name: "simulated", executionMode: payment.ExecutionModeSimulated},
		{name: "across", executionMode: payment.ExecutionModeTestnet, provider: "across"},
		{name: "relay", executionMode: payment.ExecutionModeTestnet, provider: "relay"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			store := newIntegrationStore(t)
			key := "test-worker-concurrent-duplicate-consumers-" + tc.name

			// This test uses a fixed idempotency key, so clean up before and
			// after so repeated invocations against the persistent local
			// database don't collide with their own leftovers.
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

			p := testPaymentForWorker(key)
			p.ExecutionMode = tc.executionMode
			proc := &Processor{Store: store}
			var executor *countingTestnetExecutor
			if tc.provider != "" {
				p.BridgeProvider = &tc.provider
				quote, hops := testnetQuoteFixture(tc.provider)
				p.Quote = quote
				p.Hops = hops
				executor = &countingTestnetExecutor{}
				proc.Executor = executor
			}

			created, _, err := store.CreateOrGetPayment(context.Background(), p)
			if err != nil {
				t.Fatalf("create: %v", err)
			}

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

			if tc.provider == "" {
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
				return
			}

			// Testnet-mode: the fake Executor never calls MarkSubmitted, so
			// the payment stays PROCESSING -- what proves duplicate delivery
			// was a safe no-op here is that ClaimPayment's guard let exactly
			// one of the n concurrent deliveries reach the executor at all,
			// regardless of which provider (across/relay) it carries.
			if calls := executor.callCount(); calls != 1 {
				t.Fatalf("provider %s: expected exactly 1 Executor.ExecuteTestnetPayment call across %d concurrent duplicate deliveries, got %d", tc.provider, n, calls)
			}
			fetched, found, err := store.GetPayment(context.Background(), created.ID)
			if err != nil || !found {
				t.Fatalf("get after processing: found=%v err=%v", found, err)
			}
			if fetched.Status != payment.StatusProcessing {
				t.Fatalf("provider %s: expected the testnet-mode payment to remain PROCESSING (the fake executor does not transition it), got %v", tc.provider, fetched.Status)
			}
			if fetched.BridgeProvider == nil || *fetched.BridgeProvider != tc.provider {
				t.Fatalf("provider %s: expected BridgeProvider to round-trip, got %v", tc.provider, fetched.BridgeProvider)
			}
		})
	}
}
