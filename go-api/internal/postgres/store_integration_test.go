//go:build integration

package postgres

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

func newTestStore(t *testing.T) *Store {
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
	// Each test uses a fixed idempotency key and cleans up its own row via
	// t.Cleanup (or upfront deletion for tests with multiple assertions).
	// This is safe for sequential runs of this package against a shared
	// database, but NOT safe for concurrent execution -- running two
	// `go test` processes against the same DATABASE_URL at the same time,
	// or adding t.Parallel() to these tests, will cause spurious failures
	// from key collisions between tests, not from any bug in the code
	// under test.
	return New(db)
}

func TestGetPayment_NotFound(t *testing.T) {
	s := newTestStore(t)
	_, found, err := s.GetPayment(context.Background(), "00000000-0000-0000-0000-000000000000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Fatal("expected not found")
	}
}

func TestGetPayment_MalformedID(t *testing.T) {
	s := newTestStore(t)
	_, found, err := s.GetPayment(context.Background(), "not-a-uuid")
	if err != nil {
		t.Fatalf("expected malformed id to be treated as not-found, got error: %v", err)
	}
	if found {
		t.Fatal("expected not found")
	}
}

func testPayment(idempotencyKey string) payment.Payment {
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

func TestCreateOrGetPayment_NormalCreation(t *testing.T) {
	s := newTestStore(t)
	key := "test-normal-creation-key"
	p := testPayment(key)

	// Clean up before and after so repeated invocations against the
	// persistent local database don't collide with their own leftovers.
	cleanup := func() {
		if _, err := s.db.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key = $1`, key); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	result, outcome, err := s.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome != payment.Created {
		t.Fatalf("expected Created, got %v", outcome)
	}
	if result.ID == "" {
		t.Fatal("expected a generated ID")
	}
	if len(result.Hops) != 1 || result.Hops[0].BridgeName != "TestBridge#1" {
		t.Fatalf("unexpected hops: %+v", result.Hops)
	}

	fetched, found, err := s.GetPayment(context.Background(), result.ID)
	if err != nil || !found {
		t.Fatalf("expected to find the created payment: found=%v err=%v", found, err)
	}
	if fetched.Amount != "1000.00" && fetched.Amount != "1000.000000000000000000" {
		t.Fatalf("unexpected amount round-trip: %q", fetched.Amount)
	}
}

func TestCreateOrGetPayment_SequentialIdenticalRetry(t *testing.T) {
	s := newTestStore(t)
	key := "test-sequential-retry-key"
	p := testPayment(key)

	// Clean up before and after so repeated invocations against the
	// persistent local database don't collide with their own leftovers.
	cleanup := func() {
		if _, err := s.db.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key = $1`, key); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	first, outcome1, err := s.CreateOrGetPayment(context.Background(), p)
	if err != nil || outcome1 != payment.Created {
		t.Fatalf("first call: outcome=%v err=%v", outcome1, err)
	}

	second, outcome2, err := s.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("second call error: %v", err)
	}
	if outcome2 != payment.Replayed {
		t.Fatalf("expected Replayed, got %v", outcome2)
	}
	if second.ID != first.ID {
		t.Fatalf("expected same payment id, got %s vs %s", first.ID, second.ID)
	}

	var count int
	row := s.db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM payments WHERE idempotency_key = $1`, key)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 row, got %d", count)
	}
}

func TestCreateOrGetPayment_SameKeyDifferentRequest(t *testing.T) {
	s := newTestStore(t)
	key := "test-same-key-different-request"
	original := testPayment(key)

	// Clean up before and after so repeated invocations against the
	// persistent local database don't collide with their own leftovers.
	cleanup := func() {
		if _, err := s.db.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key = $1`, key); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	_, outcome1, err := s.CreateOrGetPayment(context.Background(), original)
	if err != nil || outcome1 != payment.Created {
		t.Fatalf("first call: outcome=%v err=%v", outcome1, err)
	}

	changed := original
	changed.Amount = "2000.00"
	_, outcome2, err := s.CreateOrGetPayment(context.Background(), changed)
	if err != nil {
		t.Fatalf("second call error: %v", err)
	}
	if outcome2 != payment.Conflict {
		t.Fatalf("expected Conflict, got %v", outcome2)
	}

	var storedAmount string
	row := s.db.QueryRowContext(context.Background(),
		`SELECT amount::text FROM payments WHERE idempotency_key = $1`, key)
	if err := row.Scan(&storedAmount); err != nil {
		t.Fatalf("query stored amount: %v", err)
	}
	if storedAmount != "1000.000000000000000000" {
		t.Fatalf("original row was modified: stored amount is %q", storedAmount)
	}
}

func TestCreateOrGetPayment_CommitSucceedsResponseLostThenRetry(t *testing.T) {
	s := newTestStore(t)
	key := "test-commit-lost-response-retry"
	p := testPayment(key)

	// Clean up before and after so repeated invocations against the
	// persistent local database don't collide with their own leftovers.
	cleanup := func() {
		if _, err := s.db.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key = $1`, key); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	// Simulate: the DB transaction committed, but the caller never
	// observed the result (e.g. the process crashed before responding).
	direct, outcome1, err := s.CreateOrGetPayment(context.Background(), p)
	if err != nil || outcome1 != payment.Created {
		t.Fatalf("direct call: outcome=%v err=%v", outcome1, err)
	}

	// The "retry": a fresh call with the same key and body, as a client
	// would send after not receiving a response.
	retried, outcome2, err := s.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("retry error: %v", err)
	}
	if outcome2 != payment.Replayed {
		t.Fatalf("expected Replayed on retry, got %v", outcome2)
	}
	if retried.ID != direct.ID {
		t.Fatalf("retry returned a different payment: %s vs %s", retried.ID, direct.ID)
	}
}

func TestCreateOrGetPayment_ConcurrentSameKeyCreation(t *testing.T) {
	s := newTestStore(t)
	key := "test-concurrent-same-key"
	p := testPayment(key)

	// This test asserts exactly one row is created from n concurrent calls,
	// which only holds if no row for this key survived a previous run of
	// this test against the same (persistent) database. Clean up before and
	// after so repeated invocations don't collide with their own leftovers.
	cleanup := func() {
		if _, err := s.db.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key = $1`, key); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	const n = 10
	type outcome struct {
		id     string
		result payment.CreateResult
		err    error
	}
	results := make([]outcome, n)

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start // released simultaneously, to maximize actual overlap
			pay, res, err := s.CreateOrGetPayment(context.Background(), p)
			results[i] = outcome{id: pay.ID, result: res, err: err}
		}(i)
	}
	close(start)
	wg.Wait()

	createdCount := 0
	replayedCount := 0
	var firstID string
	for i, o := range results {
		if o.err != nil {
			t.Fatalf("goroutine %d: unexpected error: %v", i, o.err)
		}
		switch o.result {
		case payment.Created:
			createdCount++
		case payment.Replayed:
			replayedCount++
		default:
			t.Fatalf("goroutine %d: unexpected result %v", i, o.result)
		}
		if firstID == "" {
			firstID = o.id
		} else if o.id != firstID {
			t.Fatalf("goroutine %d: got a different payment id (%s) than the rest (%s)", i, o.id, firstID)
		}
	}

	if createdCount != 1 {
		t.Fatalf("expected exactly 1 Created result among %d concurrent calls, got %d", n, createdCount)
	}
	if replayedCount != n-1 {
		t.Fatalf("expected %d Replayed results, got %d", n-1, replayedCount)
	}

	var count int
	row := s.db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM payments WHERE idempotency_key = $1`, key)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 row in the database, got %d", count)
	}
}

func TestCreateOrGetPayment_InsertsOutboxEventAtomically(t *testing.T) {
	s := newTestStore(t)
	key := "test-outbox-atomic-insert"
	p := testPayment(key)

	cleanup := func() {
		if _, err := s.db.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key = $1`, key); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	result, outcome, err := s.CreateOrGetPayment(context.Background(), p)
	if err != nil || outcome != payment.Created {
		t.Fatalf("create: outcome=%v err=%v", outcome, err)
	}

	var count int
	var eventType string
	var publishedAt sql.NullTime
	row := s.db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM outbox_events WHERE payment_id = $1`, result.ID)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 outbox row, got %d", count)
	}

	row = s.db.QueryRowContext(context.Background(),
		`SELECT event_type, published_at FROM outbox_events WHERE payment_id = $1`, result.ID)
	if err := row.Scan(&eventType, &publishedAt); err != nil {
		t.Fatalf("select query: %v", err)
	}
	if eventType != "PAYMENT_ROUTED" {
		t.Fatalf("expected event_type PAYMENT_ROUTED, got %q", eventType)
	}
	if publishedAt.Valid {
		t.Fatalf("expected published_at to be NULL for a freshly inserted event")
	}
}

func TestCreateOrGetPayment_ReplayDoesNotInsertAnotherOutboxEvent(t *testing.T) {
	s := newTestStore(t)
	key := "test-outbox-no-duplicate-on-replay"
	p := testPayment(key)

	cleanup := func() {
		if _, err := s.db.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key = $1`, key); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	first, outcome1, err := s.CreateOrGetPayment(context.Background(), p)
	if err != nil || outcome1 != payment.Created {
		t.Fatalf("first call: outcome=%v err=%v", outcome1, err)
	}

	_, outcome2, err := s.CreateOrGetPayment(context.Background(), p)
	if err != nil || outcome2 != payment.Replayed {
		t.Fatalf("second call: outcome=%v err=%v", outcome2, err)
	}

	var count int
	row := s.db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM outbox_events WHERE payment_id = $1`, first.ID)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 outbox row after a replay, got %d", count)
	}
}

func TestClaimPayment_TransitionsRoutedToProcessing(t *testing.T) {
	s := newTestStore(t)
	key := "test-claim-routed-to-processing"
	p := testPayment(key)

	cleanup := func() {
		if _, err := s.db.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key = $1`, key); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	created, outcome, err := s.CreateOrGetPayment(context.Background(), p)
	if err != nil || outcome != payment.Created {
		t.Fatalf("create: outcome=%v err=%v", outcome, err)
	}

	claimed, err := s.ClaimPayment(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if !claimed {
		t.Fatal("expected claim to succeed on a ROUTED payment")
	}

	fetched, found, err := s.GetPayment(context.Background(), created.ID)
	if err != nil || !found {
		t.Fatalf("get after claim: found=%v err=%v", found, err)
	}
	if fetched.Status != payment.StatusProcessing {
		t.Fatalf("expected PROCESSING, got %v", fetched.Status)
	}
}

func TestClaimPayment_NoOpIfNotRouted(t *testing.T) {
	s := newTestStore(t)
	key := "test-claim-noop-if-not-routed"
	p := testPayment(key)

	cleanup := func() {
		if _, err := s.db.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key = $1`, key); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	created, _, err := s.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.ClaimPayment(context.Background(), created.ID); err != nil {
		t.Fatalf("first claim: %v", err)
	}

	claimedAgain, err := s.ClaimPayment(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if claimedAgain {
		t.Fatal("expected the second claim on an already-PROCESSING payment to be a no-op")
	}
}

func TestClaimPayment_ConcurrentClaimsSucceedExactlyOnce(t *testing.T) {
	s := newTestStore(t)
	key := "test-claim-concurrent-race"
	p := testPayment(key)

	cleanup := func() {
		if _, err := s.db.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key = $1`, key); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	created, _, err := s.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	const n = 10
	results := make([]bool, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			claimed, err := s.ClaimPayment(context.Background(), created.ID)
			if err != nil {
				t.Errorf("goroutine %d: %v", i, err)
				return
			}
			results[i] = claimed
		}(i)
	}
	close(start)
	wg.Wait()

	claimedCount := 0
	for _, claimed := range results {
		if claimed {
			claimedCount++
		}
	}
	if claimedCount != 1 {
		t.Fatalf("expected exactly 1 successful claim among %d concurrent attempts, got %d", n, claimedCount)
	}
}

func TestCompletePayment_TransitionsProcessingToTerminal(t *testing.T) {
	s := newTestStore(t)
	key := "test-complete-processing-to-terminal"
	p := testPayment(key)

	cleanup := func() {
		if _, err := s.db.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key = $1`, key); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	created, _, err := s.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.ClaimPayment(context.Background(), created.ID); err != nil {
		t.Fatalf("claim: %v", err)
	}

	completed, err := s.CompletePayment(context.Background(), created.ID, payment.StatusCompleted)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if !completed {
		t.Fatal("expected completion to succeed on a PROCESSING payment")
	}

	fetched, found, err := s.GetPayment(context.Background(), created.ID)
	if err != nil || !found {
		t.Fatalf("get after complete: found=%v err=%v", found, err)
	}
	if fetched.Status != payment.StatusCompleted {
		t.Fatalf("expected COMPLETED, got %v", fetched.Status)
	}
	if fetched.CompletedAt == nil {
		t.Fatal("expected CompletedAt to be set")
	}
}

func TestCompletePayment_NoOpIfNotProcessing(t *testing.T) {
	s := newTestStore(t)
	key := "test-complete-noop-if-not-processing"
	p := testPayment(key)

	cleanup := func() {
		if _, err := s.db.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key = $1`, key); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	created, _, err := s.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// Payment is ROUTED, not PROCESSING -- completion must be a no-op.
	completed, err := s.CompletePayment(context.Background(), created.ID, payment.StatusCompleted)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if completed {
		t.Fatal("expected completion on a ROUTED (not PROCESSING) payment to be a no-op")
	}
}

func TestCompletePayment_ConcurrentCompletionsSucceedExactlyOnce(t *testing.T) {
	s := newTestStore(t)
	key := "test-complete-concurrent-race"
	p := testPayment(key)

	cleanup := func() {
		if _, err := s.db.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key = $1`, key); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	created, _, err := s.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.ClaimPayment(context.Background(), created.ID); err != nil {
		t.Fatalf("claim: %v", err)
	}

	const n = 10
	results := make([]bool, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			terminal := payment.StatusCompleted
			if i%2 == 0 {
				terminal = payment.StatusFailed
			}
			completed, err := s.CompletePayment(context.Background(), created.ID, terminal)
			if err != nil {
				t.Errorf("goroutine %d: %v", i, err)
				return
			}
			results[i] = completed
		}(i)
	}
	close(start)
	wg.Wait()

	completedCount := 0
	for _, completed := range results {
		if completed {
			completedCount++
		}
	}
	if completedCount != 1 {
		t.Fatalf("expected exactly 1 successful completion among %d concurrent attempts, got %d", n, completedCount)
	}
}

func TestStalePaymentIDs_FindsOnlyPaymentsPastStaleness(t *testing.T) {
	s := newTestStore(t)
	staleKey := "test-stale-payment-ids-stale"
	freshKey := "test-stale-payment-ids-fresh"

	cleanup := func() {
		if _, err := s.db.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key IN ($1, $2)`, staleKey, freshKey); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	stale := testPayment(staleKey)
	fresh := testPayment(freshKey)
	staleCreated, _, err := s.CreateOrGetPayment(context.Background(), stale)
	if err != nil {
		t.Fatalf("create stale: %v", err)
	}
	freshCreated, _, err := s.CreateOrGetPayment(context.Background(), fresh)
	if err != nil {
		t.Fatalf("create fresh: %v", err)
	}
	if _, err := s.ClaimPayment(context.Background(), staleCreated.ID); err != nil {
		t.Fatalf("claim stale: %v", err)
	}
	if _, err := s.ClaimPayment(context.Background(), freshCreated.ID); err != nil {
		t.Fatalf("claim fresh: %v", err)
	}
	// Backdate only the "stale" payment's updated_at.
	if _, err := s.db.ExecContext(context.Background(),
		`UPDATE payments SET updated_at = now() - interval '10 minutes' WHERE id = $1`,
		staleCreated.ID); err != nil {
		t.Fatalf("backdate: %v", err)
	}

	staleIDs, err := s.StalePaymentIDs(context.Background(), 2*time.Minute)
	if err != nil {
		t.Fatalf("StalePaymentIDs: %v", err)
	}
	found := false
	for _, id := range staleIDs {
		if id == staleCreated.ID {
			found = true
		}
		if id == freshCreated.ID {
			t.Fatalf("fresh payment %s should not be reported as stale", freshCreated.ID)
		}
	}
	if !found {
		t.Fatalf("expected stale payment %s to be reported", staleCreated.ID)
	}
}
