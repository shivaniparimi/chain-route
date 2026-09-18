//go:build integration

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"chainroute/go-api/internal/payment"
)

func strPtr(s string) *string { return &s }

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

	claimed, _, err := s.ClaimPayment(context.Background(), created.ID)
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
	if _, _, err := s.ClaimPayment(context.Background(), created.ID); err != nil {
		t.Fatalf("first claim: %v", err)
	}

	claimedAgain, _, err := s.ClaimPayment(context.Background(), created.ID)
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
			claimed, _, err := s.ClaimPayment(context.Background(), created.ID)
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
	if _, _, err := s.ClaimPayment(context.Background(), created.ID); err != nil {
		t.Fatalf("claim: %v", err)
	}

	completed, createdAt, err := s.CompletePayment(context.Background(), created.ID, payment.StatusCompleted)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if !completed {
		t.Fatal("expected completion to succeed on a PROCESSING payment")
	}
	if createdAt.IsZero() {
		t.Fatal("expected CompletePayment to return a non-zero created_at")
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
	completed, createdAt, err := s.CompletePayment(context.Background(), created.ID, payment.StatusCompleted)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if completed {
		t.Fatal("expected completion on a ROUTED (not PROCESSING) payment to be a no-op")
	}
	if !createdAt.IsZero() {
		t.Fatalf("expected zero-value created_at on a no-op completion, got %v", createdAt)
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
	if _, _, err := s.ClaimPayment(context.Background(), created.ID); err != nil {
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
			completed, _, err := s.CompletePayment(context.Background(), created.ID, terminal)
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
	if _, _, err := s.ClaimPayment(context.Background(), staleCreated.ID); err != nil {
		t.Fatalf("claim stale: %v", err)
	}
	if _, _, err := s.ClaimPayment(context.Background(), freshCreated.ID); err != nil {
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

func TestPublishNextOutboxEvent_PublishesAndMarksRow(t *testing.T) {
	s := newTestStore(t)
	key := "test-outbox-publish-success"
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

	var publishedEvt OutboxEvent
	for i := 0; i < 200; i++ {
		var gotEvt OutboxEvent
		didPublish, err := s.PublishNextOutboxEvent(context.Background(), func(evt OutboxEvent) error {
			gotEvt = evt
			return nil
		})
		if err != nil {
			t.Fatalf("publish: %v", err)
		}
		if !didPublish {
			t.Fatal("queue drained before finding our target row -- test isolation bug")
		}
		if gotEvt.PaymentID == created.ID {
			publishedEvt = gotEvt
			break
		}
	}
	if publishedEvt.PaymentID != created.ID {
		t.Fatal("never observed our target payment's outbox event")
	}
	if publishedEvt.EventType != "PAYMENT_ROUTED" {
		t.Fatalf("expected event_type PAYMENT_ROUTED, got %q", publishedEvt.EventType)
	}

	var publishedAt sql.NullTime
	row := s.db.QueryRowContext(context.Background(),
		`SELECT published_at FROM outbox_events WHERE id = $1`, publishedEvt.ID)
	if err := row.Scan(&publishedAt); err != nil {
		t.Fatalf("select: %v", err)
	}
	if !publishedAt.Valid {
		t.Fatal("expected published_at to be set after a successful publish")
	}
}

func TestPublishNextOutboxEvent_NoRowsReturnsFalseNoError(t *testing.T) {
	s := newTestStore(t)
	// Drain whatever is currently pending (from this or earlier tests).
	for {
		didPublish, err := s.PublishNextOutboxEvent(context.Background(), func(OutboxEvent) error { return nil })
		if err != nil {
			t.Fatalf("drain: %v", err)
		}
		if !didPublish {
			break
		}
	}
	// The queue is now empty; one more call must be a clean no-op.
	didPublish, err := s.PublishNextOutboxEvent(context.Background(), func(OutboxEvent) error {
		t.Fatal("publish should not be called when there is no unpublished row")
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error on an empty queue, got %v", err)
	}
	if didPublish {
		t.Fatal("expected published=false on an empty queue")
	}
}

func TestPublishNextOutboxEvent_FailedPublishLeavesRowUnpublishedForRetry(t *testing.T) {
	s := newTestStore(t)
	key := "test-outbox-publish-fails-then-retries"
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

	publishErr := errors.New("simulated publish failure")
	failedOnce := false
	sawFailureForTarget := false
	sawSuccessForTarget := false

	for i := 0; i < 200 && !sawSuccessForTarget; i++ {
		didPublish, err := s.PublishNextOutboxEvent(context.Background(), func(evt OutboxEvent) error {
			if evt.PaymentID == created.ID && !failedOnce {
				failedOnce = true
				return publishErr
			}
			if evt.PaymentID == created.ID {
				sawSuccessForTarget = true
			}
			return nil
		})
		if err != nil {
			if !errors.Is(err, publishErr) {
				t.Fatalf("unexpected error: %v", err)
			}
			sawFailureForTarget = true
			continue
		}
		if !didPublish {
			break
		}
	}
	if !sawFailureForTarget {
		t.Fatal("expected to observe the simulated publish failure for the target row")
	}
	if !sawSuccessForTarget {
		t.Fatal("expected the target row to be successfully published on a later pass")
	}
}

func TestPublishNextOutboxEvent_ConcurrentPublishersClaimDistinctRows(t *testing.T) {
	s := newTestStore(t)
	const n = 8
	paymentIDs := make([]string, n)
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("test-outbox-concurrent-%d-%d", i, time.Now().UnixNano())
		keys[i] = key
		created, _, err := s.CreateOrGetPayment(context.Background(), testPayment(key))
		if err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
		paymentIDs[i] = created.ID
	}
	t.Cleanup(func() {
		for _, key := range keys {
			s.db.ExecContext(context.Background(), `DELETE FROM payments WHERE idempotency_key = $1`, key)
		}
	})

	var mu sync.Mutex
	published := map[string]int{}

	const workers = 4
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for {
				didPublish, err := s.PublishNextOutboxEvent(context.Background(), func(evt OutboxEvent) error {
					mu.Lock()
					published[evt.PaymentID]++
					mu.Unlock()
					return nil
				})
				if err != nil {
					t.Errorf("publish: %v", err)
					return
				}
				if !didPublish {
					return
				}
			}
		}()
	}
	wg.Wait()

	for _, id := range paymentIDs {
		if published[id] != 1 {
			t.Fatalf("expected payment %s to be published exactly once, got %d", id, published[id])
		}
	}
}

func TestCreateOrGetPayment_DefaultsExecutionModeToSimulated(t *testing.T) {
	s := newTestStore(t)
	key := "test-default-mode-key"
	cleanup := func() {
		s.db.ExecContext(context.Background(), `DELETE FROM payments WHERE idempotency_key = $1`, key)
	}
	cleanup()
	t.Cleanup(cleanup)

	p := testPayment(key) // does not set ExecutionMode
	result, outcome, err := s.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome != payment.Created {
		t.Fatalf("expected Created, got %v", outcome)
	}
	if result.ExecutionMode != payment.ExecutionModeSimulated {
		t.Fatalf("expected simulated, got %q", result.ExecutionMode)
	}
}

func TestClaimPayment_ReturnsExecutionMode(t *testing.T) {
	s := newTestStore(t)
	key := "test-claim-mode-key"
	cleanup := func() {
		s.db.ExecContext(context.Background(), `DELETE FROM payments WHERE idempotency_key = $1`, key)
	}
	cleanup()
	t.Cleanup(cleanup)

	p := testPayment(key)
	created, _, err := s.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	claimed, mode, err := s.ClaimPayment(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if !claimed {
		t.Fatal("expected claim to succeed")
	}
	if mode != payment.ExecutionModeSimulated {
		t.Fatalf("expected simulated, got %q", mode)
	}
}

// chainPaymentFor builds a payment.Payment on caller-supplied source/dest
// chains (rather than the fixed ethereum/base of testPayment) so
// ListPayments tests can scope their query to a SourceChain/DestinationChain
// filter that no other test or pre-existing row in the shared database will
// ever match, instead of relying on absolute timestamps to isolate rows.
func chainPaymentFor(idempotencyKey, sourceChain, destChain string) payment.Payment {
	p := testPayment(idempotencyKey)
	p.SourceChain = sourceChain
	p.DestinationChain = destChain
	return p
}

func deleteByIdempotencyKeys(t *testing.T, s *Store, keys ...string) {
	t.Helper()
	for _, key := range keys {
		if _, err := s.db.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key = $1`, key); err != nil {
			t.Fatalf("cleanup %s: %v", key, err)
		}
	}
}

func TestListPayments_PaginatesWithoutOverlapOrGap(t *testing.T) {
	s := newTestStore(t)
	const n = 5
	src, dst := "zz-test-list-page-src", "zz-test-list-page-dst"
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("test-list-page-%d", i)
	}
	deleteByIdempotencyKeys(t, s, keys...)
	t.Cleanup(func() { deleteByIdempotencyKeys(t, s, keys...) })

	ids := make([]string, n)
	base := time.Now().UTC()
	for i := 0; i < n; i++ {
		created, outcome, err := s.CreateOrGetPayment(context.Background(), chainPaymentFor(keys[i], src, dst))
		if err != nil || outcome != payment.Created {
			t.Fatalf("create %d: outcome=%v err=%v", i, outcome, err)
		}
		ids[i] = created.ID
		// Backdate created_at to a distinct, deterministic value (ascending
		// with i) so ORDER BY created_at DESC, id DESC gives a known order:
		// ids[n-1] first, ids[0] last.
		if _, err := s.db.ExecContext(context.Background(),
			`UPDATE payments SET created_at = $2 WHERE id = $1`,
			created.ID, base.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("backdate %d: %v", i, err)
		}
	}

	wantOrder := []string{ids[4], ids[3], ids[2], ids[1], ids[0]}

	var gotOrder []string
	filter := payment.ListFilter{Limit: 2, SourceChain: &src, DestinationChain: &dst}
	for page := 0; page < 10; page++ {
		results, nextCursor, err := s.ListPayments(context.Background(), filter)
		if err != nil {
			t.Fatalf("page %d: ListPayments: %v", page, err)
		}
		for _, p := range results {
			gotOrder = append(gotOrder, p.ID)
		}
		if nextCursor == "" {
			break
		}
		filter.Cursor = nextCursor
	}

	if len(gotOrder) != n {
		t.Fatalf("expected %d total rows across pages (no overlap/gap), got %d: %v", n, len(gotOrder), gotOrder)
	}
	for i := range wantOrder {
		if gotOrder[i] != wantOrder[i] {
			t.Fatalf("order mismatch at position %d: want %v, got %v", i, wantOrder, gotOrder)
		}
	}
}

func TestListPayments_FiltersByStatus(t *testing.T) {
	s := newTestStore(t)
	src, dst := "zz-test-list-status-src", "zz-test-list-status-dst"
	routedKey, completedKey := "test-list-status-routed", "test-list-status-completed"
	deleteByIdempotencyKeys(t, s, routedKey, completedKey)
	t.Cleanup(func() { deleteByIdempotencyKeys(t, s, routedKey, completedKey) })

	routed, outcome, err := s.CreateOrGetPayment(context.Background(), chainPaymentFor(routedKey, src, dst))
	if err != nil || outcome != payment.Created {
		t.Fatalf("create routed: outcome=%v err=%v", outcome, err)
	}
	completed, outcome, err := s.CreateOrGetPayment(context.Background(), chainPaymentFor(completedKey, src, dst))
	if err != nil || outcome != payment.Created {
		t.Fatalf("create completed: outcome=%v err=%v", outcome, err)
	}
	if _, _, err := s.ClaimPayment(context.Background(), completed.ID); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, _, err := s.CompletePayment(context.Background(), completed.ID, payment.StatusCompleted); err != nil {
		t.Fatalf("complete: %v", err)
	}

	status := string(payment.StatusCompleted)
	results, _, err := s.ListPayments(context.Background(), payment.ListFilter{
		Limit: 10, SourceChain: &src, DestinationChain: &dst, Status: &status,
	})
	if err != nil {
		t.Fatalf("ListPayments: %v", err)
	}
	if len(results) != 1 || results[0].ID != completed.ID {
		t.Fatalf("expected exactly the completed payment %s, got %+v", completed.ID, results)
	}
	_ = routed
}

func TestListPayments_FiltersByProvider(t *testing.T) {
	s := newTestStore(t)
	src, dst := "zz-test-list-provider-src", "zz-test-list-provider-dst"
	withProviderKey, withoutProviderKey := "test-list-provider-with", "test-list-provider-without"
	deleteByIdempotencyKeys(t, s, withProviderKey, withoutProviderKey)
	t.Cleanup(func() { deleteByIdempotencyKeys(t, s, withProviderKey, withoutProviderKey) })

	withProvider := chainPaymentFor(withProviderKey, src, dst)
	withProvider.ExecutionMode = payment.ExecutionModeTestnet
	withProvider.BridgeProvider = strPtr("zz-test-provider")
	created, outcome, err := s.CreateOrGetPayment(context.Background(), withProvider)
	if err != nil || outcome != payment.Created {
		t.Fatalf("create with provider: outcome=%v err=%v", outcome, err)
	}

	without := chainPaymentFor(withoutProviderKey, src, dst)
	if _, outcome, err := s.CreateOrGetPayment(context.Background(), without); err != nil || outcome != payment.Created {
		t.Fatalf("create without provider: outcome=%v err=%v", outcome, err)
	}

	provider := "zz-test-provider"
	results, _, err := s.ListPayments(context.Background(), payment.ListFilter{
		Limit: 10, SourceChain: &src, DestinationChain: &dst, Provider: &provider,
	})
	if err != nil {
		t.Fatalf("ListPayments: %v", err)
	}
	if len(results) != 1 || results[0].ID != created.ID {
		t.Fatalf("expected exactly the payment with provider %s, got %+v", created.ID, results)
	}
}

func TestListPayments_FiltersBySourceAndDestinationChain(t *testing.T) {
	s := newTestStore(t)
	keyA, keyB := "test-list-chain-a", "test-list-chain-b"
	srcA, dstA := "zz-test-list-chain-a-src", "zz-test-list-chain-a-dst"
	srcB, dstB := "zz-test-list-chain-b-src", "zz-test-list-chain-b-dst"
	deleteByIdempotencyKeys(t, s, keyA, keyB)
	t.Cleanup(func() { deleteByIdempotencyKeys(t, s, keyA, keyB) })

	pA, outcome, err := s.CreateOrGetPayment(context.Background(), chainPaymentFor(keyA, srcA, dstA))
	if err != nil || outcome != payment.Created {
		t.Fatalf("create A: outcome=%v err=%v", outcome, err)
	}
	if _, outcome, err := s.CreateOrGetPayment(context.Background(), chainPaymentFor(keyB, srcB, dstB)); err != nil || outcome != payment.Created {
		t.Fatalf("create B: outcome=%v err=%v", outcome, err)
	}

	results, _, err := s.ListPayments(context.Background(), payment.ListFilter{
		Limit: 10, SourceChain: &srcA, DestinationChain: &dstA,
	})
	if err != nil {
		t.Fatalf("ListPayments: %v", err)
	}
	if len(results) != 1 || results[0].ID != pA.ID {
		t.Fatalf("expected exactly payment A %s, got %+v", pA.ID, results)
	}
}

func TestListPayments_FiltersByExecutionMode(t *testing.T) {
	s := newTestStore(t)
	src, dst := "zz-test-list-mode-src", "zz-test-list-mode-dst"
	simulatedKey, testnetKey := "test-list-mode-simulated", "test-list-mode-testnet"
	deleteByIdempotencyKeys(t, s, simulatedKey, testnetKey)
	t.Cleanup(func() { deleteByIdempotencyKeys(t, s, simulatedKey, testnetKey) })

	if _, outcome, err := s.CreateOrGetPayment(context.Background(), chainPaymentFor(simulatedKey, src, dst)); err != nil || outcome != payment.Created {
		t.Fatalf("create simulated: outcome=%v err=%v", outcome, err)
	}
	testnetPayment := chainPaymentFor(testnetKey, src, dst)
	testnetPayment.ExecutionMode = payment.ExecutionModeTestnet
	testnetCreated, outcome, err := s.CreateOrGetPayment(context.Background(), testnetPayment)
	if err != nil || outcome != payment.Created {
		t.Fatalf("create testnet: outcome=%v err=%v", outcome, err)
	}

	mode := string(payment.ExecutionModeTestnet)
	results, _, err := s.ListPayments(context.Background(), payment.ListFilter{
		Limit: 10, SourceChain: &src, DestinationChain: &dst, ExecutionMode: &mode,
	})
	if err != nil {
		t.Fatalf("ListPayments: %v", err)
	}
	if len(results) != 1 || results[0].ID != testnetCreated.ID {
		t.Fatalf("expected exactly the testnet payment %s, got %+v", testnetCreated.ID, results)
	}
}

func TestStalePaymentIDs_ExcludesTestnetMode(t *testing.T) {
	s := newTestStore(t)
	key := "test-stale-excludes-testnet-key"
	cleanup := func() {
		s.db.ExecContext(context.Background(), `DELETE FROM payments WHERE idempotency_key = $1`, key)
	}
	cleanup()
	t.Cleanup(cleanup)

	p := testPayment(key)
	p.ExecutionMode = payment.ExecutionModeTestnet
	created, _, err := s.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, _, err := s.ClaimPayment(context.Background(), created.ID); err != nil {
		t.Fatalf("claim: %v", err)
	}
	// Force it stale.
	if _, err := s.db.ExecContext(context.Background(),
		`UPDATE payments SET updated_at = now() - interval '1 hour' WHERE id = $1`, created.ID); err != nil {
		t.Fatalf("backdate: %v", err)
	}

	ids, err := s.StalePaymentIDs(context.Background(), time.Minute)
	if err != nil {
		t.Fatalf("stale ids: %v", err)
	}
	for _, id := range ids {
		if id == created.ID {
			t.Fatal("StalePaymentIDs must never return a testnet-mode payment -- Recovery's execution.Execute has no meaning for a real transaction")
		}
	}
}

// TestGetDashboardStats_CountsAndProviderUsageAreExact seeds a mix of
// payments spanning every status bucket and two distinct bridge providers,
// then asserts GetDashboardStats's counts and provider-usage map move by
// exactly the seeded amounts. It compares before/after deltas rather than
// absolute values because GetDashboardStats aggregates over the entire
// payments table with no scoping filter (unlike ListPayments, which every
// other integration test in this file can scope by a unique
// source_chain/destination_chain pair) -- this is the only way to make the
// assertion exact against a shared, persistent database that may already
// hold rows from other runs, while still catching a subtly wrong
// FILTER/GROUP BY clause (which would move the deltas, not just the
// absolute totals).
func TestGetDashboardStats_CountsAndProviderUsageAreExact(t *testing.T) {
	s := newTestStore(t)
	providerA := "zz-test-stats-provider-a"
	providerB := "zz-test-stats-provider-b"
	keys := []string{
		"test-stats-routed-no-provider",
		"test-stats-routed-provider-a",
		"test-stats-completed-provider-a",
		"test-stats-failed-provider-b",
		"test-stats-processing-no-provider",
	}
	deleteByIdempotencyKeys(t, s, keys...)
	t.Cleanup(func() { deleteByIdempotencyKeys(t, s, keys...) })

	before, err := s.GetDashboardStats(context.Background())
	if err != nil {
		t.Fatalf("baseline GetDashboardStats: %v", err)
	}

	// 1: simulated, left ROUTED, no provider -- counts toward "processing".
	if _, outcome, err := s.CreateOrGetPayment(context.Background(), testPayment(keys[0])); err != nil || outcome != payment.Created {
		t.Fatalf("create %s: outcome=%v err=%v", keys[0], outcome, err)
	}

	// 2: testnet, left ROUTED, provider A -- counts toward "processing" and provider A.
	p2 := testPayment(keys[1])
	p2.ExecutionMode = payment.ExecutionModeTestnet
	p2.BridgeProvider = strPtr(providerA)
	if _, outcome, err := s.CreateOrGetPayment(context.Background(), p2); err != nil || outcome != payment.Created {
		t.Fatalf("create %s: outcome=%v err=%v", keys[1], outcome, err)
	}

	// 3: testnet, driven to COMPLETED, provider A -- counts toward "completed" and provider A.
	p3 := testPayment(keys[2])
	p3.ExecutionMode = payment.ExecutionModeTestnet
	p3.BridgeProvider = strPtr(providerA)
	created3, outcome, err := s.CreateOrGetPayment(context.Background(), p3)
	if err != nil || outcome != payment.Created {
		t.Fatalf("create %s: outcome=%v err=%v", keys[2], outcome, err)
	}
	if _, _, err := s.ClaimPayment(context.Background(), created3.ID); err != nil {
		t.Fatalf("claim %s: %v", keys[2], err)
	}
	if _, _, err := s.CompletePayment(context.Background(), created3.ID, payment.StatusCompleted); err != nil {
		t.Fatalf("complete %s: %v", keys[2], err)
	}

	// 4: testnet, driven to FAILED, provider B -- counts toward "failed" and provider B.
	p4 := testPayment(keys[3])
	p4.ExecutionMode = payment.ExecutionModeTestnet
	p4.BridgeProvider = strPtr(providerB)
	created4, outcome, err := s.CreateOrGetPayment(context.Background(), p4)
	if err != nil || outcome != payment.Created {
		t.Fatalf("create %s: outcome=%v err=%v", keys[3], outcome, err)
	}
	if _, _, err := s.ClaimPayment(context.Background(), created4.ID); err != nil {
		t.Fatalf("claim %s: %v", keys[3], err)
	}
	if ok, err := s.MarkProcessingFailed(context.Background(), created4.ID, "test-failure"); err != nil || !ok {
		t.Fatalf("mark failed %s: ok=%v err=%v", keys[3], ok, err)
	}

	// 5: simulated, driven to PROCESSING, no provider -- counts toward "processing".
	created5, outcome, err := s.CreateOrGetPayment(context.Background(), testPayment(keys[4]))
	if err != nil || outcome != payment.Created {
		t.Fatalf("create %s: outcome=%v err=%v", keys[4], outcome, err)
	}
	if _, _, err := s.ClaimPayment(context.Background(), created5.ID); err != nil {
		t.Fatalf("claim %s: %v", keys[4], err)
	}

	after, err := s.GetDashboardStats(context.Background())
	if err != nil {
		t.Fatalf("GetDashboardStats: %v", err)
	}

	if got := after.TotalPayments - before.TotalPayments; got != 5 {
		t.Fatalf("expected total_payments to increase by 5, got %d", got)
	}
	if got := after.CompletedPayments - before.CompletedPayments; got != 1 {
		t.Fatalf("expected completed_payments to increase by 1, got %d", got)
	}
	if got := after.ProcessingPayments - before.ProcessingPayments; got != 3 {
		t.Fatalf("expected processing_payments (ROUTED/PROCESSING/SUBMITTED) to increase by 3, got %d", got)
	}
	if got := after.FailedPayments - before.FailedPayments; got != 1 {
		t.Fatalf("expected failed_payments to increase by 1, got %d", got)
	}

	if got := after.ProviderUsage[providerA] - before.ProviderUsage[providerA]; got != 2 {
		t.Fatalf("expected provider %s usage to increase by 2, got %d (before=%d after=%d)",
			providerA, got, before.ProviderUsage[providerA], after.ProviderUsage[providerA])
	}
	if got := after.ProviderUsage[providerB] - before.ProviderUsage[providerB]; got != 1 {
		t.Fatalf("expected provider %s usage to increase by 1, got %d (before=%d after=%d)",
			providerB, got, before.ProviderUsage[providerB], after.ProviderUsage[providerB])
	}

	// AverageRoutingCost is a table-wide AVG(total_fee), so it can't be
	// delta-checked directly -- but AVG * COUNT recovers the pre-existing
	// fee sum, letting us predict the exact post-seed average in closed
	// form: every testPayment() seeded above has a fixed TotalFee of 1.5,
	// so the new sum is simply the recovered pre-existing sum plus 5*1.5.
	// This verifies the actual COALESCE(AVG(total_fee), 0) SQL against
	// real seeded rows, rather than only checking counts around it.
	const seededFee = 1.5
	const seededCount = 5
	beforeSum := before.AverageRoutingCost * float64(before.TotalPayments)
	wantAvg := (beforeSum + seededCount*seededFee) / float64(before.TotalPayments+seededCount)
	if diff := after.AverageRoutingCost - wantAvg; diff < -1e-9 || diff > 1e-9 {
		t.Fatalf("expected average_routing_cost to be %v (recovered pre-existing sum %v + %d*%v over %d rows), got %v",
			wantAvg, beforeSum, seededCount, seededFee, before.TotalPayments+seededCount, after.AverageRoutingCost)
	}
}
