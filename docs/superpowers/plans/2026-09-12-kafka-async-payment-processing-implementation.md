# Phase 6 Asynchronous Payment Processing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add asynchronous payment execution to ChainRoute: a worker consumes Kafka events for `ROUTED` payments, simulates execution, and drives the payment to `PROCESSING` then `COMPLETED`/`FAILED`, using a transactional outbox for reliable publication and PostgreSQL conditional updates for idempotent, crash-safe state transitions.

**Architecture:** A new `cmd/worker` Go binary runs three goroutines (outbox publisher, Kafka consumer/processor, stale-`PROCESSING` recovery sweep) sharing one DB pool and one Kafka client. `POST /payments` (existing) additionally inserts an outbox row in its existing transaction. PostgreSQL remains the sole source of truth; Kafka is a delivery mechanism only.

**Tech Stack:** Go (existing), PostgreSQL (existing), `github.com/segmentio/kafka-go` (new), Redpanda (new — local Kafka-compatible broker for dev/test).

## Global Constraints

- Kafka is the sole new infrastructure dependency. No Redis. No distributed locks beyond PostgreSQL's own row-level locking. No Kafka transactions, no inbox/processed-events table, no generalized execution ledger.
- PostgreSQL remains the source of truth for all durable state; Kafka events carry only `payment_id`, `event_type`, `occurred_at` — never the full payment or route.
- State machine: `ROUTED -> PROCESSING -> COMPLETED` or `PROCESSING -> FAILED`. No other transitions. Enforced via conditional `UPDATE ... WHERE status = '<expected>'`, never an application-level check-then-write.
- `execution.Execute(paymentID string) Result` is pure, deterministic, side-effect-free: `fnv.New64a` hash of the ID, `sum % 10 == 0` fails, else succeeds. No I/O, no sleep, no external calls.
- Outbox: `outbox_events` row inserted in the SAME transaction as the payment+hops insert (extends Phase 5's `CreateOrGetPayment`). Claiming via `SELECT ... FOR UPDATE SKIP LOCKED`, one row per iteration, with the Kafka publish call made WHILE the transaction is held open (accepted tradeoff — do not add `claimed_at`/lease columns).
- Recovery sweep re-completes stale `PROCESSING` rows directly (never routes back through `ROUTED`), re-running `execution.Execute` and applying `UPDATE ... WHERE status = 'PROCESSING' AND updated_at < now() - staleness`.
- Kafka offset commits are MANUAL, only after a definitive per-message outcome (success or recognized no-op) — never auto-commit.
- Message key = `payment_id`. Topic = `chainroute.payments.routed`. Consumer group = `chainroute-payment-worker`.
- Duplicate execution ATTEMPTS (not claims, not persisted transitions) are possible and accepted — safe only because `Execute` is pure. Do not weaken any test that exercises this; do not "fix" it with a lock/lease/inbox table.
- The worker binary (`cmd/worker`) is separate from `cmd/server`; both share `internal/payment`, `internal/postgres`. Kafka producer/consumer live in `internal/kafka`. Worker-specific goroutine logic lives in `internal/worker`.
- API (`cmd/server`) startup/availability is independent of Kafka's availability — it never talks to Kafka. Worker startup: PostgreSQL is blocking-ping-or-die; Kafka is not (log a warning, let `kafka-go`'s own retry take over).
- No committed credentials. Config via env vars: `DATABASE_URL` (reused), `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC` (default `chainroute.payments.routed`), `KAFKA_CONSUMER_GROUP` (default `chainroute-payment-worker`), `KAFKA_POLL_TIMEOUT_MS`, `OUTBOX_POLL_INTERVAL_MS` (default `500`), `WORKER_RECOVERY_SWEEP_INTERVAL_SECONDS` (default `30`), `WORKER_RECOVERY_STALENESS_SECONDS` (default `120`).
- Do not modify `router/`, `cpp-routing-service/`, or `proto/`. Preserve all Phase 1-5 behavior and tests, including `POST /payments` client-facing idempotency (unchanged — Phase 6 only adds one more `INSERT` to the existing transaction).
- `go vet ./...` clean, no new project-code warnings.

## Environment note

This environment has PostgreSQL already installed and running (from Phase 5) but no Kafka-compatible broker. Task 1 installs Redpanda via Homebrew, mirroring the Postgres install pattern from Phase 5's Task 1.

---

### Task 1: Environment setup — Redpanda, migration, kafka-go dependency

**Files:**
- Create: `go-api/migrations/0002_payment_processing.sql`

**Interfaces:**
- Produces: a running local Redpanda broker with topic `chainroute.payments.routed` created; `payments` table extended with `PROCESSING`/`COMPLETED`/`FAILED` statuses and a `completed_at` column; a new `outbox_events` table; `github.com/segmentio/kafka-go` available as a Go dependency.

- [ ] **Step 1: Install and start Redpanda**

```bash
brew install redpanda-data/tap/redpanda
rpk redpanda start --overprovisioned --smp 1 --memory 1G --reserve-memory 0M \
    --node-id 0 --check=false &
sleep 3
rpk cluster info
```

If `brew install redpanda-data/tap/redpanda` is unavailable in this environment, fall back to `brew install redpanda` (the formula name has varied across Homebrew tap versions) — try both, use whichever succeeds, and note which one worked in your report.

- [ ] **Step 2: Create the topic**

```bash
rpk topic create chainroute.payments.routed --partitions 3 --replicas 1
rpk topic list
```

Expected: `chainroute.payments.routed` appears in the topic list.

- [ ] **Step 3: Write the migration**

`go-api/migrations/0002_payment_processing.sql`:

```sql
ALTER TABLE payments
    DROP CONSTRAINT payments_status_check,
    ADD CONSTRAINT payments_status_check
        CHECK (status IN ('ROUTED', 'PROCESSING', 'COMPLETED', 'FAILED')),
    ADD COLUMN completed_at TIMESTAMPTZ NULL;

CREATE TABLE outbox_events (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id   UUID NOT NULL REFERENCES payments(id),
    event_type   TEXT NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ NULL
);

CREATE INDEX outbox_events_unpublished_idx
    ON outbox_events (created_at) WHERE published_at IS NULL;
```

- [ ] **Step 4: Apply the migration**

```bash
DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable"
/opt/homebrew/opt/postgresql@16/bin/psql "$DATABASE_URL" -f go-api/migrations/0002_payment_processing.sql
/opt/homebrew/opt/postgresql@16/bin/psql "$DATABASE_URL" -c '\d payments'
/opt/homebrew/opt/postgresql@16/bin/psql "$DATABASE_URL" -c '\d outbox_events'
```

Expected: `payments.status` CHECK now lists all four values; `payments` has a nullable `completed_at`; `outbox_events` exists with the columns above.

- [ ] **Step 5: Add the kafka-go dependency**

```bash
cd go-api
go get github.com/segmentio/kafka-go
go mod tidy
go build ./...
cd ..
```

Expected: `go-api/go.mod` requires `github.com/segmentio/kafka-go`; build succeeds (nothing imports it yet).

- [ ] **Step 6: Commit**

```bash
git add go-api/migrations/0002_payment_processing.sql go-api/go.mod go-api/go.sum
git commit -m "feat(go-api): add Phase 6 migration and kafka-go dependency"
```

---

### Task 2: Domain model extension, event type, and deterministic execution

**Files:**
- Modify: `go-api/internal/payment/payment.go`
- Create: `go-api/internal/events/routed_payment.go`
- Create: `go-api/internal/execution/execution.go`
- Create: `go-api/internal/execution/execution_test.go`

**Interfaces:**
- Produces:
```go
package payment
const (
    StatusRouted     Status = "ROUTED"
    StatusProcessing Status = "PROCESSING"
    StatusCompleted  Status = "COMPLETED"
    StatusFailed     Status = "FAILED"
)
type Payment struct {
    // ...existing fields unchanged...
    CompletedAt *time.Time // nil while ROUTED or PROCESSING
}
```
```go
package events
type RoutedPayment struct {
    PaymentID  string    `json:"payment_id"`
    EventType  string    `json:"event_type"`
    OccurredAt time.Time `json:"occurred_at"`
}
const RoutedPaymentEventType = "PAYMENT_ROUTED"
```
```go
package execution
type Result struct { Success bool; Reason string }
func Execute(paymentID string) Result
```

- [ ] **Step 1: Extend the payment domain model**

The current `go-api/internal/payment/payment.go` has `const StatusRouted Status = "ROUTED"` and a `Payment` struct with fields `ID, IdempotencyKey, SourceChain, DestinationChain, Asset, Amount, Status, TotalFee, Hops, CreatedAt, UpdatedAt` (in that order) plus a `Hop` struct and `CreateResult` enum — leave all of that as-is. Change only the `Status` constant block and add one field to `Payment`:

```go
const (
	StatusRouted     Status = "ROUTED"
	StatusProcessing Status = "PROCESSING"
	StatusCompleted  Status = "COMPLETED"
	StatusFailed     Status = "FAILED"
)
```

Add `CompletedAt *time.Time` as the last field of the `Payment` struct (after `UpdatedAt`). It is `nil` while the payment is `ROUTED` or `PROCESSING`, and set once the payment reaches `COMPLETED` or `FAILED`.

- [ ] **Step 2: Write the event type**

`go-api/internal/events/routed_payment.go`:

```go
package events

import "time"

// RoutedPaymentEventType is the only Kafka event type Phase 6 produces.
const RoutedPaymentEventType = "PAYMENT_ROUTED"

// RoutedPayment is the thin Kafka payload for a routed payment. It carries
// only payment_id plus metadata -- PostgreSQL remains the source of truth,
// and the consumer always re-reads current state before acting.
type RoutedPayment struct {
	PaymentID  string    `json:"payment_id"`
	EventType  string    `json:"event_type"`
	OccurredAt time.Time `json:"occurred_at"`
}
```

- [ ] **Step 3: Write the failing execution tests**

`go-api/internal/execution/execution_test.go`:

```go
package execution

import "testing"

func TestExecute_Deterministic(t *testing.T) {
	ids := []string{
		"c869364e-8d1c-4e54-bda8-0465dd935abc",
		"00000000-0000-0000-0000-000000000000",
		"ffffffff-ffff-ffff-ffff-ffffffffffff",
		"11111111-1111-1111-1111-111111111111",
	}
	for _, id := range ids {
		first := Execute(id)
		second := Execute(id)
		if first != second {
			t.Fatalf("Execute(%q) is not deterministic: first=%+v second=%+v", id, first, second)
		}
	}
}

func TestExecute_BothOutcomesReachable(t *testing.T) {
	sawSuccess := false
	sawFailure := false
	for i := 0; i < 1000 && !(sawSuccess && sawFailure); i++ {
		id := randomLikeID(i)
		result := Execute(id)
		if result.Success {
			sawSuccess = true
		} else {
			sawFailure = true
			if result.Reason == "" {
				t.Fatalf("expected a non-empty Reason on failure for id %q", id)
			}
		}
	}
	if !sawSuccess {
		t.Fatal("expected at least one success outcome across 1000 ids")
	}
	if !sawFailure {
		t.Fatal("expected at least one failure outcome across 1000 ids")
	}
}

func randomLikeID(i int) string {
	return "test-execution-id-" + string(rune('a'+i%26)) + string(rune('0'+i%10)) + "-" + string(rune(i))
}
```

- [ ] **Step 4: Run to verify these fail (package doesn't exist yet)**

```bash
cd go-api
go test ./internal/execution/... 2>&1 | head -10
cd ..
```

Expected: build failure, `Execute` undefined.

- [ ] **Step 5: Implement Execute**

`go-api/internal/execution/execution.go`:

```go
package execution

import "hash/fnv"

// Result is the outcome of a simulated payment execution.
type Result struct {
	Success bool
	Reason  string
}

// Execute is a pure, deterministic stand-in for a real payment executor.
// It has no I/O, no external calls, and no randomness: the same paymentID
// always produces the same Result. This determinism is load-bearing -- it
// is what lets the recovery sweep safely recompute a crashed or stalled
// execution and be certain it reproduces the exact outcome the original
// attempt would have persisted.
func Execute(paymentID string) Result {
	h := fnv.New64a()
	h.Write([]byte(paymentID))
	if h.Sum64()%10 == 0 {
		return Result{Success: false, Reason: "simulated execution failure"}
	}
	return Result{Success: true}
}
```

- [ ] **Step 6: Run tests, verify they pass**

```bash
cd go-api
go build ./...
go vet ./...
go test ./internal/execution/... -v
cd ..
```

Expected: both tests pass. If `TestExecute_BothOutcomesReachable` fails to observe a failure outcome within 1000 tries, that means the `%10` modulus in `Execute` isn't producing failures at the expected ~10% rate for the `randomLikeID` generator's output distribution -- do not weaken the test's iteration count or its requirement to see both outcomes; instead verify `randomLikeID` actually produces 1000 distinct strings (add a debug print if needed) and fix the generator, not the test's assertions.

- [ ] **Step 7: Commit**

```bash
git add go-api/internal/payment/payment.go go-api/internal/events/ go-api/internal/execution/
git commit -m "feat(go-api): add PROCESSING/COMPLETED/FAILED statuses, routed-payment event type, and deterministic execution"
```

---

### Task 3: Outbox event insertion in the existing payment transaction

**Files:**
- Modify: `go-api/internal/postgres/store.go`
- Modify: `go-api/internal/postgres/store_integration_test.go`

**Interfaces:**
- Consumes: `events.RoutedPayment`, `events.RoutedPaymentEventType` (Task 2).
- Produces: `CreateOrGetPayment` now also inserts one `outbox_events` row per newly created payment, in the same transaction. No new exported method yet (claiming is Task 5).

The current `go-api/internal/postgres/store.go` ends its `CreateOrGetPayment` winning path with a loop inserting `payment_route_hops` rows, then `tx.Commit()`. This task inserts the outbox row between the hop-insert loop and the commit.

- [ ] **Step 1: Write the failing integration test**

Add to `go-api/internal/postgres/store_integration_test.go` (same package, same `testPayment` helper already defined there):

```go
func TestCreateOrGetPayment_InsertsOutboxEventAtomically(t *testing.T) {
	s := newTestStore(t)
	key := "test-outbox-atomic-insert"
	p := testPayment(key)

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
```

Add `"database/sql"` to this file's import block if not already present (it is already imported by `newTestStore`).

- [ ] **Step 2: Run to verify these fail**

```bash
cd go-api
DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable" \
    go test -tags=integration -run TestCreateOrGetPayment_InsertsOutboxEventAtomically -v ./internal/postgres/...
cd ..
```

Expected: FAIL — no outbox row exists yet (count is 0).

- [ ] **Step 3: Insert the outbox row in the existing transaction**

In `go-api/internal/postgres/store.go`, add to the import block:

```go
	"encoding/json"
	"time"

	"chainroute/go-api/internal/events"
```

In `CreateOrGetPayment`, insert this block immediately after the hop-insert `for` loop and before `if err := tx.Commit(); err != nil {`:

```go
	outboxPayload, err := json.Marshal(events.RoutedPayment{
		PaymentID:  created.ID,
		EventType:  events.RoutedPaymentEventType,
		OccurredAt: time.Now().UTC(),
	})
	if err != nil {
		return payment.Payment{}, 0, fmt.Errorf("marshal outbox payload: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO outbox_events (payment_id, event_type, payload)
		VALUES ($1, $2, $3)
	`, created.ID, events.RoutedPaymentEventType, outboxPayload); err != nil {
		return payment.Payment{}, 0, fmt.Errorf("insert outbox event: %w", err)
	}
```

- [ ] **Step 4: Run tests, verify they pass**

```bash
cd go-api
go build ./...
go vet ./...
DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable" \
    go test -tags=integration -v ./internal/postgres/...
cd ..
```

Expected: all integration tests pass (9 total: the 7 from Phase 5 plus these 2 new ones).

- [ ] **Step 5: Commit**

```bash
git add go-api/internal/postgres/
git commit -m "feat(go-api): insert outbox event atomically with payment creation"
```

---

### Task 4: Payment state-transition methods and concurrent claim safety

**Files:**
- Modify: `go-api/internal/postgres/store.go`
- Modify: `go-api/internal/postgres/store_integration_test.go`

**Interfaces:**
- Produces:
```go
func (s *Store) ClaimPayment(ctx context.Context, paymentID string) (claimed bool, err error)
func (s *Store) CompletePayment(ctx context.Context, paymentID string, terminal payment.Status) (completed bool, err error)
func (s *Store) StalePaymentIDs(ctx context.Context, staleness time.Duration) ([]string, error)
```
`GetPayment` and `LookupByIdempotencyKey` now also populate `payment.Payment.CompletedAt` (`*time.Time`, `nil` unless the payment is terminal).
- Consumes: `payment.StatusProcessing`, `payment.StatusCompleted`, `payment.StatusFailed` (Task 2).

This is the safety-critical task: `ClaimPayment` and `CompletePayment` are what make duplicate claims and duplicate persisted transitions impossible, and what make duplicate execution *attempts* (a different, accepted property — see the design spec §7) safe. Do not weaken the concurrent test below.

- [ ] **Step 1: Add `completed_at` to the existing read queries**

In `go-api/internal/postgres/store.go`, `findByIdempotencyKey`'s query and scan:

Change:
```go
	row := s.db.QueryRowContext(ctx, `
		SELECT id, source_chain, destination_chain, asset, amount::text,
		       total_fee, status, created_at, updated_at,
		       (source_chain = $2 AND destination_chain = $3
		        AND asset = $4 AND amount = $5::NUMERIC) AS request_matches
		FROM payments
		WHERE idempotency_key = $1
	`, p.IdempotencyKey, p.SourceChain, p.DestinationChain, p.Asset, p.Amount)

	var status string
	err = row.Scan(&existing.ID, &existing.SourceChain, &existing.DestinationChain,
		&existing.Asset, &existing.Amount, &existing.TotalFee, &status,
		&existing.CreatedAt, &existing.UpdatedAt, &matches)
```
to:
```go
	row := s.db.QueryRowContext(ctx, `
		SELECT id, source_chain, destination_chain, asset, amount::text,
		       total_fee, status, completed_at, created_at, updated_at,
		       (source_chain = $2 AND destination_chain = $3
		        AND asset = $4 AND amount = $5::NUMERIC) AS request_matches
		FROM payments
		WHERE idempotency_key = $1
	`, p.IdempotencyKey, p.SourceChain, p.DestinationChain, p.Asset, p.Amount)

	var status string
	var completedAt sql.NullTime
	err = row.Scan(&existing.ID, &existing.SourceChain, &existing.DestinationChain,
		&existing.Asset, &existing.Amount, &existing.TotalFee, &status, &completedAt,
		&existing.CreatedAt, &existing.UpdatedAt, &matches)
```
and immediately after `existing.Status = payment.Status(status)`, add:
```go
	if completedAt.Valid {
		existing.CompletedAt = &completedAt.Time
	}
```

Apply the equivalent change to `GetPayment`: add `completed_at` to its `SELECT` column list (after `status`), add a `var completedAt sql.NullTime` alongside the existing `var status string`, add `&completedAt` to the `Scan` call (after `&status`), and after `p.Status = payment.Status(status)` add:
```go
	if completedAt.Valid {
		p.CompletedAt = &completedAt.Time
	}
```

- [ ] **Step 2: Write the failing tests**

Add to `go-api/internal/postgres/store_integration_test.go`:

```go
func TestClaimPayment_TransitionsRoutedToProcessing(t *testing.T) {
	s := newTestStore(t)
	key := "test-claim-routed-to-processing"
	p := testPayment(key)
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
```

Add `"sync"` and `"time"` to this file's import block if not already present (`"sync"` was added in Phase 5's Task 4; `"time"` is new).

- [ ] **Step 3: Run to verify these fail**

```bash
cd go-api
DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable" \
    go test -tags=integration ./internal/postgres/... 2>&1 | head -20
cd ..
```

Expected: compile errors — `ClaimPayment`, `CompletePayment`, `StalePaymentIDs` undefined.

- [ ] **Step 4: Implement the three methods**

Add to `go-api/internal/postgres/store.go`, after `CreateOrGetPayment`:

```go
// ClaimPayment atomically transitions a payment from ROUTED to PROCESSING.
// claimed=false means the payment was not ROUTED (already claimed by
// another delivery, or in some other state) -- a safe no-op, not an error.
// This is the sole mechanism preventing duplicate claims (see the Phase 6
// design spec, §7 case 2).
func (s *Store) ClaimPayment(ctx context.Context, paymentID string) (claimed bool, err error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE payments SET status = $2, updated_at = now()
		WHERE id = $1 AND status = $3
	`, paymentID, payment.StatusProcessing, payment.StatusRouted)
	if err != nil {
		return false, fmt.Errorf("claim payment: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("claim payment rows affected: %w", err)
	}
	return rows == 1, nil
}

// CompletePayment atomically transitions a payment from PROCESSING to the
// given terminal status. completed=false means the payment was not
// PROCESSING -- a safe no-op. This is the sole mechanism guaranteeing
// exactly one terminal outcome is ever persisted per payment (Phase 6
// design spec, §7 case 4), regardless of how many times the caller's
// execution logic itself ran.
func (s *Store) CompletePayment(ctx context.Context, paymentID string, terminal payment.Status) (completed bool, err error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE payments SET status = $2, completed_at = now(), updated_at = now()
		WHERE id = $1 AND status = $3
	`, paymentID, terminal, payment.StatusProcessing)
	if err != nil {
		return false, fmt.Errorf("complete payment: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("complete payment rows affected: %w", err)
	}
	return rows == 1, nil
}

// StalePaymentIDs returns the IDs of payments that have been PROCESSING for
// longer than staleness. The caller (the worker's recovery sweep) is
// responsible for re-running execution and calling CompletePayment for
// each -- this method only identifies candidates.
func (s *Store) StalePaymentIDs(ctx context.Context, staleness time.Duration) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id FROM payments
		WHERE status = $1
		  AND updated_at < now() - make_interval(secs => $2)
	`, payment.StatusProcessing, staleness.Seconds())
	if err != nil {
		return nil, fmt.Errorf("query stale payments: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan stale payment id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
```

- [ ] **Step 5: Run tests, verify they pass**

```bash
cd go-api
go build ./...
go vet ./...
DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable" \
    go test -tags=integration -v ./internal/postgres/...
cd ..
```

Expected: all pass (16 total: 9 from before plus 7 new).

- [ ] **Step 6: Run the concurrent tests repeatedly to rule out a lucky pass**

```bash
cd go-api
for i in 1 2 3 4 5; do
    DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable" \
        go test -tags=integration -run 'TestClaimPayment_ConcurrentClaimsSucceedExactlyOnce|TestCompletePayment_ConcurrentCompletionsSucceedExactlyOnce' -count=1 -v ./internal/postgres/...
done
cd ..
```

Expected: passes cleanly all 5 times. A failure here is a real bug in the guard clause, not something to retry past.

- [ ] **Step 7: Commit**

```bash
git add go-api/internal/postgres/
git commit -m "feat(go-api): add ClaimPayment, CompletePayment, StalePaymentIDs with concurrency-safe transitions"
```

---

### Task 5: Outbox claiming and publishing (`PublishNextOutboxEvent`)

**Files:**
- Modify: `go-api/internal/postgres/store.go`
- Modify: `go-api/internal/postgres/store_integration_test.go`

**Interfaces:**
- Produces:
```go
type OutboxEvent struct {
    ID        string
    PaymentID string
    EventType string
    Payload   []byte
}
func (s *Store) PublishNextOutboxEvent(ctx context.Context, publish func(OutboxEvent) error) (published bool, err error)
```
- Consumes: nothing new from earlier tasks beyond `outbox_events` (Task 1).

`PublishNextOutboxEvent` claims the oldest unpublished row via
`SELECT ... FOR UPDATE SKIP LOCKED`, invokes the caller-supplied `publish`
function *while the claiming transaction is held open*, and marks the row
published only if `publish` succeeds. This is the accepted tradeoff from the
design spec §5: the transaction spans the publish call. `published=false,
err=nil` means there was nothing to claim. Task 9 wires a real Kafka
producer as the `publish` function; this task tests it with fakes only.

- [ ] **Step 1: Write the failing tests**

Add to `go-api/internal/postgres/store_integration_test.go`:

```go
func TestPublishNextOutboxEvent_PublishesAndMarksRow(t *testing.T) {
	s := newTestStore(t)
	key := "test-outbox-publish-success"
	p := testPayment(key)
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
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("test-outbox-concurrent-%d-%d", i, time.Now().UnixNano())
		created, _, err := s.CreateOrGetPayment(context.Background(), testPayment(key))
		if err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
		paymentIDs[i] = created.ID
	}

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
```

Add `"errors"` and `"fmt"` to this file's import block if not already present.

- [ ] **Step 2: Run to verify these fail**

```bash
cd go-api
DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable" \
    go test -tags=integration ./internal/postgres/... 2>&1 | head -20
cd ..
```

Expected: compile errors — `OutboxEvent`, `PublishNextOutboxEvent` undefined.

- [ ] **Step 3: Implement `PublishNextOutboxEvent`**

Add to `go-api/internal/postgres/store.go`, after `StalePaymentIDs`:

```go
// OutboxEvent is one row from outbox_events, as seen by a publisher.
type OutboxEvent struct {
	ID        string
	PaymentID string
	EventType string
	Payload   []byte
}

// PublishNextOutboxEvent claims the oldest unpublished outbox row via
// SELECT ... FOR UPDATE SKIP LOCKED, invokes publish with it while the
// claiming transaction is held open, and marks the row published only if
// publish succeeds. published=false, err=nil means there was nothing to
// claim. If publish returns an error, the transaction rolls back, the row
// stays unpublished, and a later call will retry it -- this is the
// accepted at-least-once mechanism (Phase 6 design spec §5, §6): a
// duplicate publish is possible and is why consumers must be idempotent.
//
// The transaction deliberately spans the publish call (an accepted
// tradeoff, not an oversight -- see the design spec §5): a slow or
// unavailable Kafka broker will hold one row lock and one pooled
// connection for the duration of that call. No claimed_at/lease columns
// are added to decouple this, since SKIP LOCKED already provides mutual
// exclusion and automatically releases the lock if this process crashes
// mid-transaction.
func (s *Store) PublishNextOutboxEvent(ctx context.Context, publish func(OutboxEvent) error) (published bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var evt OutboxEvent
	row := tx.QueryRowContext(ctx, `
		SELECT id, payment_id, event_type, payload
		FROM outbox_events
		WHERE published_at IS NULL
		ORDER BY created_at
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`)
	if err := row.Scan(&evt.ID, &evt.PaymentID, &evt.EventType, &evt.Payload); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("claim outbox event: %w", err)
	}

	if err := publish(evt); err != nil {
		return false, fmt.Errorf("publish outbox event: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `UPDATE outbox_events SET published_at = now() WHERE id = $1`, evt.ID); err != nil {
		return false, fmt.Errorf("mark outbox event published: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit outbox publish: %w", err)
	}

	return true, nil
}
```

- [ ] **Step 4: Run tests, verify they pass**

```bash
cd go-api
go build ./...
go vet ./...
DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable" \
    go test -tags=integration -v ./internal/postgres/...
cd ..
```

Expected: all pass (20 total: 16 from before plus 4 new).

- [ ] **Step 5: Run the concurrent test repeatedly**

```bash
cd go-api
for i in 1 2 3 4 5; do
    DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable" \
        go test -tags=integration -run TestPublishNextOutboxEvent_ConcurrentPublishersClaimDistinctRows -count=1 -v ./internal/postgres/...
done
cd ..
```

Expected: passes cleanly all 5 times.

- [ ] **Step 6: Commit**

```bash
git add go-api/internal/postgres/
git commit -m "feat(go-api): add PublishNextOutboxEvent with SKIP LOCKED claiming"
```

---

### Task 6: Kafka producer/consumer wrappers

**Files:**
- Create: `go-api/internal/kafka/producer.go`
- Create: `go-api/internal/kafka/consumer.go`
- Create: `go-api/internal/kafka/kafka_integration_test.go`

**Interfaces:**
- Produces:
```go
package kafka
func NewProducer(brokers []string, topic string) *Producer
func (p *Producer) Publish(ctx context.Context, key string, value []byte) error
func (p *Producer) Close() error

type ConsumerConfig struct { Brokers []string; Topic string; GroupID string }
func NewConsumer(cfg ConsumerConfig) *Consumer
func (c *Consumer) FetchMessage(ctx context.Context) (kafkago.Message, error)
func (c *Consumer) CommitMessages(ctx context.Context, msgs ...kafkago.Message) error
func (c *Consumer) Close() error
```

These are thin wrappers with no business logic — claim/execute/transition
logic lives in `internal/worker` (Tasks 7-8), which is what makes that logic
unit-testable against fakes without a real broker.

- [ ] **Step 1: Write the producer**

`go-api/internal/kafka/producer.go`:

```go
package kafka

import (
	"context"

	kafkago "github.com/segmentio/kafka-go"
)

// Producer publishes messages to a single Kafka topic, keyed by payment ID
// so that any future event type for the same payment lands on the same
// partition (no ordering requirement exists today -- see the Phase 6
// design spec §14 -- this costs nothing and avoids a harder migration
// later).
type Producer struct {
	writer *kafkago.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafkago.Writer{
			Addr:         kafkago.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafkago.Hash{},
			RequiredAcks: kafkago.RequireOne,
		},
	}
}

// Publish sends value to the topic with the given key.
func (p *Producer) Publish(ctx context.Context, key string, value []byte) error {
	return p.writer.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(key),
		Value: value,
	})
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
```

- [ ] **Step 2: Write the consumer**

`go-api/internal/kafka/consumer.go`:

```go
package kafka

import (
	"context"

	kafkago "github.com/segmentio/kafka-go"
)

type ConsumerConfig struct {
	Brokers []string
	Topic   string
	GroupID string
}

// Consumer reads messages from a topic within a consumer group. Offsets
// are committed ONLY via an explicit call to CommitMessages -- the reader's
// CommitInterval is deliberately left at its zero value, which makes
// kafka-go commit synchronously on CommitMessages rather than on a
// background timer (see the Phase 6 design spec §13: manual commit,
// issued only after a message reaches a definitive outcome).
type Consumer struct {
	reader *kafkago.Reader
}

func NewConsumer(cfg ConsumerConfig) *Consumer {
	return &Consumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: cfg.Brokers,
			Topic:   cfg.Topic,
			GroupID: cfg.GroupID,
		}),
	}
}

// FetchMessage blocks until a message is available or ctx is done. It does
// NOT commit the offset -- call CommitMessages only after the message has
// been fully, durably handled.
func (c *Consumer) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	return c.reader.FetchMessage(ctx)
}

// CommitMessages commits the offsets for the given messages.
func (c *Consumer) CommitMessages(ctx context.Context, msgs ...kafkago.Message) error {
	return c.reader.CommitMessages(ctx, msgs...)
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
```

- [ ] **Step 3: Build**

```bash
cd go-api
go build ./...
go vet ./...
cd ..
```

Expected: builds clean (no tests run yet — these need a real broker).

- [ ] **Step 4: Write the Kafka integration tests**

`go-api/internal/kafka/kafka_integration_test.go`:

```go
//go:build kafka_integration

package kafka

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

var testBrokers = []string{"localhost:9092"}

func TestProducerConsumer_RoundTrip(t *testing.T) {
	topic := fmt.Sprintf("chainroute.payments.routed.test-roundtrip-%d", time.Now().UnixNano())

	producer := NewProducer(testBrokers, topic)
	defer producer.Close()
	consumer := NewConsumer(ConsumerConfig{Brokers: testBrokers, Topic: topic, GroupID: fmt.Sprintf("test-roundtrip-%d", time.Now().UnixNano())})
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := producer.Publish(ctx, "payment-123", []byte(`{"payment_id":"payment-123"}`)); err != nil {
		t.Fatalf("publish: %v", err)
	}

	msg, err := consumer.FetchMessage(ctx)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if string(msg.Key) != "payment-123" {
		t.Fatalf("expected key payment-123, got %q", msg.Key)
	}
	if string(msg.Value) != `{"payment_id":"payment-123"}` {
		t.Fatalf("unexpected value: %s", msg.Value)
	}
	if err := consumer.CommitMessages(ctx, msg); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func TestConsumerGroup_SplitsMessagesAcrossPartitions(t *testing.T) {
	topic := fmt.Sprintf("chainroute.payments.routed.test-group-%d", time.Now().UnixNano())
	group := fmt.Sprintf("test-group-%d", time.Now().UnixNano())

	conn, err := kafkago.Dial("tcp", testBrokers[0])
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 2, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	producer := NewProducer(testBrokers, topic)
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const n = 20
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		if err := producer.Publish(ctx, key, []byte(key)); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	consumerA := NewConsumer(ConsumerConfig{Brokers: testBrokers, Topic: topic, GroupID: group})
	defer consumerA.Close()
	consumerB := NewConsumer(ConsumerConfig{Brokers: testBrokers, Topic: topic, GroupID: group})
	defer consumerB.Close()

	var mu sync.Mutex
	seenByA, seenByB := 0, 0
	var wg sync.WaitGroup
	wg.Add(2)
	drain := func(c *Consumer, count *int) {
		defer wg.Done()
		for {
			fctx, fcancel := context.WithTimeout(ctx, 3*time.Second)
			msg, err := c.FetchMessage(fctx)
			fcancel()
			if err != nil {
				return
			}
			mu.Lock()
			*count++
			mu.Unlock()
			_ = c.CommitMessages(ctx, msg)
		}
	}
	go drain(consumerA, &seenByA)
	go drain(consumerB, &seenByB)
	wg.Wait()

	if seenByA+seenByB != n {
		t.Fatalf("expected %d total messages consumed, got %d (A=%d, B=%d)", n, seenByA+seenByB, seenByA, seenByB)
	}
	if seenByA == 0 || seenByB == 0 {
		t.Fatalf("expected both consumers to receive at least one message, got A=%d B=%d", seenByA, seenByB)
	}
}
```

- [ ] **Step 5: Run against local Redpanda (from Task 1)**

```bash
cd go-api
rpk cluster info >/dev/null 2>&1 || echo "WARNING: Redpanda does not appear to be running -- start it per Task 1 before running this step"
go test -tags=kafka_integration -v ./internal/kafka/...
cd ..
```

Expected: both tests pass.

- [ ] **Step 6: Commit**

```bash
git add go-api/internal/kafka/
git commit -m "feat(go-api): add Kafka producer/consumer wrappers with manual offset commit"
```

---

### Task 7: Worker processor — the idempotent Kafka message handler

**Files:**
- Create: `go-api/internal/worker/processor.go`
- Create: `go-api/internal/worker/processor_test.go`
- Create: `go-api/internal/worker/processor_integration_test.go`

**Interfaces:**
- Consumes: `payment.StatusCompleted`/`StatusFailed` (Task 2), `execution.Execute` (Task 2), `events.RoutedPayment` (Task 2), `postgres.Store.ClaimPayment`/`CompletePayment` (Task 4, satisfying the `PaymentStore` interface below by having matching method signatures — no explicit implements declaration needed in Go).
- Produces:
```go
package worker
type PaymentStore interface {
    ClaimPayment(ctx context.Context, paymentID string) (bool, error)
    CompletePayment(ctx context.Context, paymentID string, terminal payment.Status) (bool, error)
}
type Processor struct{ Store PaymentStore }
func (p *Processor) HandleRoutedPayment(ctx context.Context, evt events.RoutedPayment) error
```

This is the second safety-critical task: `HandleRoutedPayment` is what makes
duplicate Kafka delivery safe. Do not weaken the duplicate-delivery or
concurrent-consumer tests below.

- [ ] **Step 1: Write the failing unit tests**

`go-api/internal/worker/processor_test.go`:

```go
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
```

- [ ] **Step 2: Run to verify these fail**

```bash
cd go-api
go test ./internal/worker/... 2>&1 | head -10
cd ..
```

Expected: build failure, `Processor`/`PaymentStore`/`HandleRoutedPayment` undefined.

- [ ] **Step 3: Implement the processor**

`go-api/internal/worker/processor.go`:

```go
package worker

import (
	"context"
	"fmt"

	"chainroute/go-api/internal/events"
	"chainroute/go-api/internal/execution"
	"chainroute/go-api/internal/payment"
)

// PaymentStore is the subset of *postgres.Store the processor needs.
type PaymentStore interface {
	ClaimPayment(ctx context.Context, paymentID string) (bool, error)
	CompletePayment(ctx context.Context, paymentID string, terminal payment.Status) (bool, error)
}

// Processor handles one PAYMENT_ROUTED event at a time.
type Processor struct {
	Store PaymentStore
}

// HandleRoutedPayment claims the payment and, only if the claim succeeds,
// runs execution and persists the terminal outcome. If the claim is a
// no-op (the payment is already PROCESSING or terminal), it returns
// immediately WITHOUT calling execution.Execute -- this is what keeps a
// tight duplicate-delivery loop (the same event handled twice back to
// back) from ever invoking Execute more than once. A genuinely stalled
// worker racing the recovery sweep is a different, accepted case (see the
// Phase 6 design spec §7, §11) that this function does not need to
// special-case: whichever of the two wins CompletePayment's guard is the
// one that persists.
func (p *Processor) HandleRoutedPayment(ctx context.Context, evt events.RoutedPayment) error {
	claimed, err := p.Store.ClaimPayment(ctx, evt.PaymentID)
	if err != nil {
		return fmt.Errorf("claim payment %s: %w", evt.PaymentID, err)
	}
	if !claimed {
		return nil
	}

	result := execution.Execute(evt.PaymentID)
	terminal := payment.StatusCompleted
	if !result.Success {
		terminal = payment.StatusFailed
	}

	if _, err := p.Store.CompletePayment(ctx, evt.PaymentID, terminal); err != nil {
		return fmt.Errorf("complete payment %s: %w", evt.PaymentID, err)
	}
	return nil
}
```

- [ ] **Step 4: Run unit tests, verify they pass**

```bash
cd go-api
go build ./...
go vet ./...
go test ./internal/worker/... -v
cd ..
```

Expected: all 5 tests pass.

- [ ] **Step 5: Write the failing concurrent-duplicate-consumers integration test**

`go-api/internal/worker/processor_integration_test.go`:

```go
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
```

- [ ] **Step 6: Run, verify it passes, then repeat 5x**

```bash
cd go-api
DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable" \
    go test -tags=integration -run TestHandleRoutedPayment_ConcurrentDuplicateConsumers -v ./internal/worker/...
for i in 1 2 3 4 5; do
    DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable" \
        go test -tags=integration -run TestHandleRoutedPayment_ConcurrentDuplicateConsumers -count=1 -v ./internal/worker/...
done
cd ..
```

Expected: passes cleanly every time.

- [ ] **Step 7: Commit**

```bash
git add go-api/internal/worker/
git commit -m "feat(go-api): add worker processor with idempotent claim-execute-transition handling"
```

---

### Task 8: Recovery sweep, and the stalled-worker-vs-sweep race test

**Files:**
- Create: `go-api/internal/worker/recovery.go`
- Create: `go-api/internal/worker/recovery_test.go`
- Create: `go-api/internal/worker/recovery_integration_test.go`

**Interfaces:**
- Consumes: `postgres.Store.StalePaymentIDs`/`CompletePayment` (Task 4), `execution.Execute` (Task 2), `findIDWithOutcome`/`newIntegrationStore`/`testPaymentForWorker` (Task 7, same package).
- Produces:
```go
package worker
type RecoveryStore interface {
    StalePaymentIDs(ctx context.Context, staleness time.Duration) ([]string, error)
    CompletePayment(ctx context.Context, paymentID string, terminal payment.Status) (bool, error)
}
type Recovery struct { Store RecoveryStore; Staleness time.Duration }
func (r *Recovery) SweepOnce(ctx context.Context) (completed int, err error)
func (r *Recovery) Run(ctx context.Context, interval time.Duration)
```

This task directly exercises the property the design spec's revision was
built around: a stalled (not crashed) worker and the recovery sweep can
both genuinely invoke `execution.Execute` for the same payment, and that is
safe — but exactly one of the two ever persists a terminal transition. Do
not weaken the race test in Step 7; it exists specifically to prove this.

- [ ] **Step 1: Write the failing unit tests**

`go-api/internal/worker/recovery_test.go`:

```go
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
```

- [ ] **Step 2: Run to verify these fail**

```bash
cd go-api
go test ./internal/worker/... 2>&1 | head -10
cd ..
```

Expected: build failure, `Recovery`/`RecoveryStore`/`SweepOnce` undefined.

- [ ] **Step 3: Implement the recovery sweep**

`go-api/internal/worker/recovery.go`:

```go
package worker

import (
	"context"
	"log"
	"time"

	"chainroute/go-api/internal/execution"
	"chainroute/go-api/internal/payment"
)

// RecoveryStore is the subset of *postgres.Store the recovery sweep needs.
type RecoveryStore interface {
	StalePaymentIDs(ctx context.Context, staleness time.Duration) ([]string, error)
	CompletePayment(ctx context.Context, paymentID string, terminal payment.Status) (bool, error)
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
		if !result.Success {
			terminal = payment.StatusFailed
		}
		didComplete, err := r.Store.CompletePayment(ctx, id, terminal)
		if err != nil {
			log.Printf("ERROR: recovery sweep failed to complete payment %s: %v", id, err)
			continue
		}
		if didComplete {
			completed++
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
				log.Printf("ERROR: recovery sweep failed: %v", err)
			}
		}
	}
}
```

- [ ] **Step 4: Run unit tests, verify they pass**

```bash
cd go-api
go build ./...
go vet ./...
go test ./internal/worker/... -v
cd ..
```

Expected: all unit tests pass (5 from Task 7, 3 new here).

- [ ] **Step 5: Write the failing integration tests**

`go-api/internal/worker/recovery_integration_test.go`:

```go
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
```

- [ ] **Step 6: Run, verify they pass**

```bash
cd go-api
DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable" \
    go test -tags=integration -v ./internal/worker/...
cd ..
```

Expected: all pass (2 new integration tests here, plus the 1 from Task 7 = 3 total in this package).

- [ ] **Step 7: Run the race test repeatedly to rule out a lucky pass**

```bash
cd go-api
for i in 1 2 3 4 5; do
    DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable" \
        go test -tags=integration -run TestRecovery_StalledWorkerVsSweepRace -count=1 -v ./internal/worker/...
done
cd ..
```

Expected: passes cleanly all 5 times. A failure here (e.g. `CompletedAt` observed inconsistent, or an unexpected error from either goroutine) is a real bug in `CompletePayment`'s guard, not something to retry past.

- [ ] **Step 8: Commit**

```bash
git add go-api/internal/worker/
git commit -m "feat(go-api): add recovery sweep with stalled-worker-vs-sweep race safety"
```

---

### Task 9: Outbox publisher goroutine and `cmd/worker/main.go` wiring

**Files:**
- Create: `go-api/internal/worker/publisher.go`
- Create: `go-api/internal/worker/publisher_test.go`
- Create: `go-api/cmd/worker/main.go`

**Interfaces:**
- Consumes: `postgres.Store.PublishNextOutboxEvent`/`postgres.OutboxEvent` (Task 5), `kafka.Producer`/`Consumer` (Task 6), `worker.Processor`/`Recovery` (Tasks 7-8), `events.RoutedPayment` (Task 2).
- Produces:
```go
package worker
type OutboxStore interface {
    PublishNextOutboxEvent(ctx context.Context, publish func(postgres.OutboxEvent) error) (bool, error)
}
type Publisher struct {
    Store   OutboxStore
    Publish func(ctx context.Context, key string, value []byte) error
}
func (p *Publisher) PollOnce(ctx context.Context) (bool, error)
func (p *Publisher) Run(ctx context.Context, pollInterval time.Duration)
```

- [ ] **Step 1: Write the failing unit tests**

`go-api/internal/worker/publisher_test.go`:

```go
package worker

import (
	"context"
	"testing"

	"chainroute/go-api/internal/postgres"
)

type fakeOutboxStore struct {
	publishResult bool
	publishErr    error
	calls         int
}

func (f *fakeOutboxStore) PublishNextOutboxEvent(ctx context.Context, publish func(postgres.OutboxEvent) error) (bool, error) {
	f.calls++
	if f.publishErr != nil {
		return false, f.publishErr
	}
	if f.publishResult {
		if err := publish(postgres.OutboxEvent{
			ID: "evt-1", PaymentID: "pay-1", EventType: "PAYMENT_ROUTED", Payload: []byte(`{}`),
		}); err != nil {
			return false, err
		}
	}
	return f.publishResult, nil
}

func TestPollOnce_CallsPublishWithEventFields(t *testing.T) {
	var gotKey string
	var gotValue []byte
	store := &fakeOutboxStore{publishResult: true}
	p := &Publisher{
		Store: store,
		Publish: func(ctx context.Context, key string, value []byte) error {
			gotKey = key
			gotValue = value
			return nil
		},
	}
	published, err := p.PollOnce(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if !published {
		t.Fatal("expected published=true")
	}
	if gotKey != "pay-1" {
		t.Fatalf("expected key pay-1, got %q", gotKey)
	}
	if string(gotValue) != "{}" {
		t.Fatalf("unexpected value: %s", gotValue)
	}
}

func TestPollOnce_NoRowsReturnsFalse(t *testing.T) {
	store := &fakeOutboxStore{publishResult: false}
	p := &Publisher{Store: store, Publish: func(context.Context, string, []byte) error { return nil }}
	published, err := p.PollOnce(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if published {
		t.Fatal("expected published=false")
	}
}

func TestPollOnce_PublishErrorPropagates(t *testing.T) {
	store := &fakeOutboxStore{publishResult: true}
	p := &Publisher{
		Store:   store,
		Publish: func(context.Context, string, []byte) error { return context.DeadlineExceeded },
	}
	if _, err := p.PollOnce(context.Background()); err == nil {
		t.Fatal("expected an error to propagate from a failing publish")
	}
}
```

- [ ] **Step 2: Run to verify these fail**

```bash
cd go-api
go test ./internal/worker/... 2>&1 | head -10
cd ..
```

Expected: build failure, `Publisher`/`OutboxStore` undefined.

- [ ] **Step 3: Implement the publisher**

`go-api/internal/worker/publisher.go`:

```go
package worker

import (
	"context"
	"log"
	"time"

	"chainroute/go-api/internal/postgres"
)

// OutboxStore is the subset of *postgres.Store the publisher needs.
type OutboxStore interface {
	PublishNextOutboxEvent(ctx context.Context, publish func(postgres.OutboxEvent) error) (bool, error)
}

// Publisher polls the outbox and publishes unpublished events to Kafka,
// keyed by payment ID so the underlying claim transaction (see the Phase 6
// design spec §5) can mark the row published only after Publish succeeds.
type Publisher struct {
	Store   OutboxStore
	Publish func(ctx context.Context, key string, value []byte) error
}

// PollOnce attempts to publish the next unpublished outbox event, if any.
func (p *Publisher) PollOnce(ctx context.Context) (bool, error) {
	return p.Store.PublishNextOutboxEvent(ctx, func(evt postgres.OutboxEvent) error {
		return p.Publish(ctx, evt.PaymentID, evt.Payload)
	})
}

// Run polls on the given interval until ctx is done.
func (p *Publisher) Run(ctx context.Context, pollInterval time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		published, err := p.PollOnce(ctx)
		if err != nil {
			log.Printf("ERROR: outbox publish failed: %v", err)
		}
		if err != nil || !published {
			select {
			case <-ctx.Done():
				return
			case <-time.After(pollInterval):
			}
		}
	}
}
```

- [ ] **Step 4: Run tests, verify they pass**

```bash
cd go-api
go build ./...
go vet ./...
go test ./internal/worker/... -v
cd ..
```

Expected: all unit tests pass (8 from Tasks 7-8, 3 new here = 11 total unit tests in `internal/worker`).

- [ ] **Step 5: Write `cmd/worker/main.go`**

`go-api/cmd/worker/main.go`:

```go
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"chainroute/go-api/internal/events"
	"chainroute/go-api/internal/kafka"
	"chainroute/go-api/internal/postgres"
	"chainroute/go-api/internal/worker"
)

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL environment variable is required")
	}
	bootstrapServers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	if bootstrapServers == "" {
		log.Fatal("KAFKA_BOOTSTRAP_SERVERS environment variable is required")
	}
	brokers := strings.Split(bootstrapServers, ",")

	topic := envOrDefault("KAFKA_TOPIC", "chainroute.payments.routed")
	consumerGroup := envOrDefault("KAFKA_CONSUMER_GROUP", "chainroute-payment-worker")
	outboxPollInterval := envDuration("OUTBOX_POLL_INTERVAL_MS", 500*time.Millisecond, time.Millisecond)
	recoverySweepInterval := envDuration("WORKER_RECOVERY_SWEEP_INTERVAL_SECONDS", 30*time.Second, time.Second)
	recoveryStaleness := envDuration("WORKER_RECOVERY_STALENESS_SECONDS", 120*time.Second, time.Second)

	// PostgreSQL is blocking-ping-or-die at startup, matching cmd/server:
	// nothing in this binary can do anything useful without the database.
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(10)

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := db.PingContext(pingCtx); err != nil {
		pingCancel()
		log.Fatalf("failed to connect to database: %v", err)
	}
	pingCancel()

	store := postgres.New(db)

	// Kafka is deliberately NOT blocking-or-die here: a transiently
	// unreachable broker at startup is tolerated, since kafka-go's writer
	// and reader dial lazily and retry internally rather than failing
	// fast. The recovery sweep goroutine below depends only on Postgres
	// and keeps working regardless of Kafka's availability.
	producer := kafka.NewProducer(brokers, topic)
	defer producer.Close()
	consumer := kafka.NewConsumer(kafka.ConsumerConfig{Brokers: brokers, Topic: topic, GroupID: consumerGroup})
	defer consumer.Close()

	processor := &worker.Processor{Store: store}
	publisher := &worker.Publisher{
		Store: store,
		Publish: func(ctx context.Context, key string, value []byte) error {
			return producer.Publish(ctx, key, value)
		},
	}
	recovery := &worker.Recovery{Store: store, Staleness: recoveryStaleness}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); publisher.Run(ctx, outboxPollInterval) }()
	go func() { defer wg.Done(); recovery.Run(ctx, recoverySweepInterval) }()
	go func() { defer wg.Done(); runConsumeLoop(ctx, consumer, processor) }()

	log.Printf("worker started: topic=%s group=%s brokers=%v", topic, consumerGroup, brokers)
	<-ctx.Done()
	log.Println("shutting down...")
	wg.Wait()
	log.Println("worker shut down")
}

// runConsumeLoop stops requesting new messages once ctx is done (FetchMessage
// returns an error on a cancelled context), but each in-flight message is
// handled with its own bounded context independent of the shutdown signal,
// so a message already in progress gets a grace period to finish rather
// than being aborted mid-cycle.
func runConsumeLoop(ctx context.Context, consumer *kafka.Consumer, processor *worker.Processor) {
	for {
		msg, err := consumer.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("WARNING: fetch message failed, retrying: %v", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		var evt events.RoutedPayment
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			log.Printf("ERROR: failed to decode event, committing offset to skip it: %v", err)
			_ = consumer.CommitMessages(context.Background(), msg)
			continue
		}

		handleCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = processor.HandleRoutedPayment(handleCtx, evt)
		cancel()
		if err != nil {
			log.Printf("ERROR: failed to handle routed payment %s, NOT committing offset: %v", evt.PaymentID, err)
			continue
		}

		if err := consumer.CommitMessages(context.Background(), msg); err != nil {
			log.Printf("ERROR: failed to commit offset for payment %s: %v", evt.PaymentID, err)
		}
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration, unit time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("invalid %s: %v", key, err)
	}
	return time.Duration(n) * unit
}
```

- [ ] **Step 6: Build**

```bash
cd go-api
go build -o /tmp/chainroute-worker ./cmd/worker
go vet ./...
cd ..
```

Expected: builds cleanly.

- [ ] **Step 7: Manual smoke test against real Postgres and Redpanda**

```bash
rpk cluster info >/dev/null 2>&1 || echo "WARNING: start Redpanda per Task 1 first"

DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable" \
KAFKA_BOOTSTRAP_SERVERS="localhost:9092" \
/tmp/chainroute-worker &
WORKER_PID=$!
sleep 2
kill -0 "$WORKER_PID" && echo "worker is running"

kill -TERM "$WORKER_PID"
wait "$WORKER_PID" 2>/dev/null
echo "worker exited cleanly"
```

Expected: `worker started: topic=chainroute.payments.routed group=chainroute-payment-worker ...` is logged, the process stays alive, and SIGTERM produces `shutting down...` / `worker shut down` with a clean exit.

- [ ] **Step 8: Commit**

```bash
git add go-api/internal/worker/publisher.go go-api/internal/worker/publisher_test.go go-api/cmd/worker/
git commit -m "feat(go-api): add outbox publisher and wire the worker binary's three goroutines"
```

---

### Task 10: Expose `completed_at` in the HTTP payment response

**Files:**
- Modify: `go-api/internal/handler/payments.go`
- Modify: `go-api/internal/handler/payments_test.go`

**Interfaces:**
- Consumes: `payment.Payment.CompletedAt` (Task 2), `payment.StatusProcessing`/`StatusCompleted`/`StatusFailed` (Task 2).
- Produces: `paymentResponse.CompletedAt *string` (nullable, RFC3339Nano, `null` while `ROUTED`/`PROCESSING`).

The current `go-api/internal/handler/payments.go` has a `paymentResponse` struct with fields `ID, SourceChain, DestinationChain, Asset, Amount, Status, TotalFee, Hops, CreatedAt, UpdatedAt` (in that order) and a `toPaymentResponse(p payment.Payment) paymentResponse` function. This task adds one field and one conditional formatting line — nothing else in this file changes; `PostPayments`, `GetPayment`, and all existing validation/routing logic are untouched.

- [ ] **Step 1: Write the failing test**

Add to `go-api/internal/handler/payments_test.go`:

```go
func TestToPaymentResponse_CompletedAtNullWhenNotSet(t *testing.T) {
	store := &fakePaymentStore{
		createOutcome: payment.Created,
		createResult: payment.Payment{
			ID: "test-id-completed-at-null", Status: payment.StatusRouted,
			CreatedAt: time.Now(), UpdatedAt: time.Now(), CompletedAt: nil,
		},
	}
	h := &Handler{Client: &fakeClient{response: &routingv1.FindRouteResponse{RouteFound: true, TotalFee: 2.5}}, Store: store}
	rec := doPaymentRequest(h, "POST", "/payments", "key-1", validPaymentBody())

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if v, ok := body["completed_at"]; ok && v != nil {
		t.Fatalf("expected completed_at to be null, got %v", v)
	}
}

func TestToPaymentResponse_CompletedAtSetWhenTerminal(t *testing.T) {
	completedAt := time.Date(2026, 9, 12, 0, 0, 0, 0, time.UTC)
	store := &fakePaymentStore{
		getFound: true,
		getResult: payment.Payment{
			ID: "test-id-completed-at-set", Status: payment.StatusCompleted,
			CreatedAt: time.Now(), UpdatedAt: time.Now(), CompletedAt: &completedAt,
		},
	}
	h := &Handler{Client: &fakeClient{}, Store: store}
	rec := doPaymentRequest(h, "GET", "/payments/test-id-completed-at-set", "", "")

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	got, ok := body["completed_at"].(string)
	if !ok {
		t.Fatalf("expected completed_at to be a string, got %v", body["completed_at"])
	}
	if got != completedAt.Format(time.RFC3339Nano) {
		t.Fatalf("expected %q, got %q", completedAt.Format(time.RFC3339Nano), got)
	}
}
```

Add `"time"` to this file's import block if not already present (it likely already is, from the existing `CreatedAt`/`UpdatedAt` fields used elsewhere in the file).

- [ ] **Step 2: Run to verify these fail**

```bash
cd go-api
go test ./internal/handler/... 2>&1 | head -10
cd ..
```

Expected: compile error or test failure — `CompletedAt` doesn't exist on `paymentResponse` / is never populated.

- [ ] **Step 3: Add the field**

In `go-api/internal/handler/payments.go`, change:

```go
type paymentResponse struct {
	ID               string        `json:"id"`
	SourceChain      string        `json:"source_chain"`
	DestinationChain string        `json:"destination_chain"`
	Asset            string        `json:"asset"`
	Amount           string        `json:"amount"`
	Status           string        `json:"status"`
	TotalFee         float64       `json:"total_fee"`
	Hops             []hopResponse `json:"hops"`
	CreatedAt        string        `json:"created_at"`
	UpdatedAt        string        `json:"updated_at"`
}
```
to:
```go
type paymentResponse struct {
	ID               string        `json:"id"`
	SourceChain      string        `json:"source_chain"`
	DestinationChain string        `json:"destination_chain"`
	Asset            string        `json:"asset"`
	Amount           string        `json:"amount"`
	Status           string        `json:"status"`
	TotalFee         float64       `json:"total_fee"`
	Hops             []hopResponse `json:"hops"`
	CreatedAt        string        `json:"created_at"`
	UpdatedAt        string        `json:"updated_at"`
	CompletedAt      *string       `json:"completed_at"`
}
```

Change `toPaymentResponse`:

```go
func toPaymentResponse(p payment.Payment) paymentResponse {
	hops := make([]hopResponse, 0, len(p.Hops))
	for _, h := range p.Hops {
		hops = append(hops, hopResponse{
			HopIndex: h.HopIndex, FromChain: h.FromChain, ToChain: h.ToChain,
			BridgeName: h.BridgeName, Fee: h.Fee, LatencyMs: h.LatencyMs,
			Liquidity: h.Liquidity, Reliability: h.Reliability,
		})
	}
	var completedAt *string
	if p.CompletedAt != nil {
		formatted := p.CompletedAt.UTC().Format(time.RFC3339Nano)
		completedAt = &formatted
	}
	return paymentResponse{
		ID: p.ID, SourceChain: p.SourceChain, DestinationChain: p.DestinationChain,
		Asset: p.Asset, Amount: p.Amount, Status: string(p.Status), TotalFee: p.TotalFee,
		Hops: hops, CreatedAt: p.CreatedAt.UTC().Format(time.RFC3339Nano),
		UpdatedAt: p.UpdatedAt.UTC().Format(time.RFC3339Nano),
		CompletedAt: completedAt,
	}
}
```

- [ ] **Step 4: Run tests, verify they pass**

```bash
cd go-api
go build ./...
go vet ./...
go test ./internal/handler/... -v
cd ..
```

Expected: all pass (28 from Phase 5 plus 2 new = 30).

- [ ] **Step 5: Commit**

```bash
git add go-api/internal/handler/
git commit -m "feat(go-api): expose completed_at in payment API responses"
```

---

### Task 11: E2E extension — the full async pipeline

**Files:**
- Modify: `scripts/e2e_test.sh`

**Interfaces:**
- Consumes: `cmd/worker` (Task 9), migration `0002` (Task 1), the `POST /payments`/`GET /payments/{id}` endpoints (existing + Task 10).

The current `scripts/e2e_test.sh` runs 9 numbered tests: routing (1-3), payment creation/idempotency (4-8), and restart verification (9). It already defines `ROOT_DIR`, `CPP_PORT`, `HTTP_PORT`, `SEED`, `PAYMENT_IDEMPOTENCY_KEY`, `DATABASE_URL`, a `cleanup()` trapped on EXIT, and builds/starts the C++ service then the Go API server. This task adds: a new `ASYNC_IDEMPOTENCY_KEY`, building and starting the worker binary, applying migration `0002`, verifying Redpanda is reachable, and a new Test 10 that posts a payment, starts the worker, and polls until the payment reaches a terminal state.

- [ ] **Step 1: Add the async idempotency key variable**

Change:
```bash
PAYMENT_IDEMPOTENCY_KEY="e2e-test-key-$$-$(date +%s)"
```
to:
```bash
PAYMENT_IDEMPOTENCY_KEY="e2e-test-key-$$-$(date +%s)"
ASYNC_IDEMPOTENCY_KEY="e2e-async-test-key-$$-$(date +%s)"
```

- [ ] **Step 2: Extend cleanup() to stop the worker and delete both test rows**

Change:
```bash
cleanup() {
    [[ -n "${GO_PID:-}" ]] && kill "$GO_PID" 2>/dev/null || true
    [[ -n "${CPP_PID:-}" ]] && kill "$CPP_PID" 2>/dev/null || true
    if [[ -n "${DATABASE_URL:-}" ]]; then
        /opt/homebrew/opt/postgresql@16/bin/psql "$DATABASE_URL" -tAc \
            "DELETE FROM payments WHERE idempotency_key = '$PAYMENT_IDEMPOTENCY_KEY'" >/dev/null 2>&1 || true
    fi
}
```
to:
```bash
cleanup() {
    [[ -n "${WORKER_PID:-}" ]] && kill "$WORKER_PID" 2>/dev/null || true
    [[ -n "${GO_PID:-}" ]] && kill "$GO_PID" 2>/dev/null || true
    [[ -n "${CPP_PID:-}" ]] && kill "$CPP_PID" 2>/dev/null || true
    if [[ -n "${DATABASE_URL:-}" ]]; then
        /opt/homebrew/opt/postgresql@16/bin/psql "$DATABASE_URL" -tAc \
            "DELETE FROM payments WHERE idempotency_key IN ('$PAYMENT_IDEMPOTENCY_KEY', '$ASYNC_IDEMPOTENCY_KEY')" >/dev/null 2>&1 || true
    fi
}
```

- [ ] **Step 3: Build the worker binary alongside the Go API server**

Change:
```bash
echo "Building Go service..."
(cd "$ROOT_DIR/go-api" && go build -o "$ROOT_DIR/go-api/server" ./cmd/server)
```
to:
```bash
echo "Building Go service..."
(cd "$ROOT_DIR/go-api" && go build -o "$ROOT_DIR/go-api/server" ./cmd/server)

echo "Building Go worker..."
(cd "$ROOT_DIR/go-api" && go build -o "$ROOT_DIR/go-api/worker" ./cmd/worker)
```

- [ ] **Step 4: Apply migration 0002 and verify Redpanda, alongside the existing migration-0001 check**

Change:
```bash
echo "Ensuring database schema is up to date..."
/opt/homebrew/opt/postgresql@16/bin/psql "$DATABASE_URL" -tAc \
    "SELECT 1 FROM information_schema.tables WHERE table_name='payments'" | grep -q 1 || \
    /opt/homebrew/opt/postgresql@16/bin/psql "$DATABASE_URL" -f "$ROOT_DIR/go-api/migrations/0001_create_payments.sql"
```
to:
```bash
echo "Ensuring database schema is up to date..."
/opt/homebrew/opt/postgresql@16/bin/psql "$DATABASE_URL" -tAc \
    "SELECT 1 FROM information_schema.tables WHERE table_name='payments'" | grep -q 1 || \
    /opt/homebrew/opt/postgresql@16/bin/psql "$DATABASE_URL" -f "$ROOT_DIR/go-api/migrations/0001_create_payments.sql"
/opt/homebrew/opt/postgresql@16/bin/psql "$DATABASE_URL" -tAc \
    "SELECT 1 FROM information_schema.tables WHERE table_name='outbox_events'" | grep -q 1 || \
    /opt/homebrew/opt/postgresql@16/bin/psql "$DATABASE_URL" -f "$ROOT_DIR/go-api/migrations/0002_payment_processing.sql"

echo "Checking Redpanda is reachable..."
if ! rpk cluster info >/dev/null 2>&1; then
    echo "FAIL: Redpanda does not appear to be running -- start it per the Phase 6 plan's Task 1 before running this script" >&2
    exit 1
fi
rpk topic create chainroute.payments.routed --partitions 3 --replicas 1 >/dev/null 2>&1 || true
```

- [ ] **Step 5: Add Test 10 after Test 9, before the final success line**

Change:
```bash
RESTART_RESPONSE=$(curl -s "http://127.0.0.1:$HTTP_PORT/payments/$PAYMENT_ID")
echo "$RESTART_RESPONSE" | grep -q "\"id\":\"$PAYMENT_ID\"" || { echo "FAIL: payment not found after restart"; exit 1; }
echo "OK: payment $PAYMENT_ID still readable after Go process restart"

echo "All E2E checks passed."
```
to:
```bash
RESTART_RESPONSE=$(curl -s "http://127.0.0.1:$HTTP_PORT/payments/$PAYMENT_ID")
echo "$RESTART_RESPONSE" | grep -q "\"id\":\"$PAYMENT_ID\"" || { echo "FAIL: payment not found after restart"; exit 1; }
echo "OK: payment $PAYMENT_ID still readable after Go process restart"

echo "Test 10: async execution reaches a terminal state via Kafka"
KAFKA_BOOTSTRAP_SERVERS="localhost:9092" \
OUTBOX_POLL_INTERVAL_MS=200 \
WORKER_RECOVERY_SWEEP_INTERVAL_SECONDS=5 \
WORKER_RECOVERY_STALENESS_SECONDS=30 \
"$ROOT_DIR/go-api/worker" &
WORKER_PID=$!
sleep 1
kill -0 "$WORKER_PID" || { echo "FAIL: worker exited before becoming ready" >&2; exit 1; }

ASYNC_CREATE_STATUS=$(curl -s -o /tmp/e2e_async_create_body.json -w "%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/payments" \
    -H "Content-Type: application/json" -H "Idempotency-Key: $ASYNC_IDEMPOTENCY_KEY" \
    -d '{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":"500.00"}')
ASYNC_PAYMENT_RESPONSE=$(cat /tmp/e2e_async_create_body.json)
rm -f /tmp/e2e_async_create_body.json
[[ "$ASYNC_CREATE_STATUS" == "201" ]] || { echo "FAIL: expected 201, got $ASYNC_CREATE_STATUS"; exit 1; }
ASYNC_PAYMENT_ID=$(echo "$ASYNC_PAYMENT_RESPONSE" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
[[ -n "$ASYNC_PAYMENT_ID" ]] || { echo "FAIL: no payment id in async response"; exit 1; }

TERMINAL_STATUS=""
for i in $(seq 1 50); do
    ASYNC_GET_RESPONSE=$(curl -s "http://127.0.0.1:$HTTP_PORT/payments/$ASYNC_PAYMENT_ID")
    if echo "$ASYNC_GET_RESPONSE" | grep -q '"status":"COMPLETED"'; then
        TERMINAL_STATUS="COMPLETED"
        break
    fi
    if echo "$ASYNC_GET_RESPONSE" | grep -q '"status":"FAILED"'; then
        TERMINAL_STATUS="FAILED"
        break
    fi
    sleep 0.1
done
[[ -n "$TERMINAL_STATUS" ]] || { echo "FAIL: payment $ASYNC_PAYMENT_ID did not reach a terminal state in time: $ASYNC_GET_RESPONSE"; exit 1; }
echo "$ASYNC_GET_RESPONSE" | grep -q '"completed_at":"[^"]' || { echo "FAIL: expected a non-null completed_at, got: $ASYNC_GET_RESPONSE"; exit 1; }
echo "OK: payment $ASYNC_PAYMENT_ID reached $TERMINAL_STATUS with completed_at set"

echo "All E2E checks passed."
```

- [ ] **Step 6: Run the full script**

```bash
./scripts/e2e_test.sh
```

Expected: all 10 numbered tests print `OK`, ending with `All E2E checks passed.`, and no leftover processes afterward. If Test 10 times out, check the worker's stderr/stdout (it was backgrounded — consider temporarily redirecting it to a file while debugging) for Kafka connectivity errors before assuming a logic bug.

- [ ] **Step 7: Run it a second time to confirm re-runnability**

```bash
./scripts/e2e_test.sh
```

Expected: passes again cleanly, with a fresh `ASYNC_IDEMPOTENCY_KEY` and `PAYMENT_IDEMPOTENCY_KEY` each run (both are PID+timestamp based) and the `cleanup()` trap removing both rows every time.

- [ ] **Step 8: Commit**

```bash
git add scripts/e2e_test.sh
git commit -m "test(e2e): extend E2E script with the full async payment-processing pipeline"
```

---

### Task 12: Full-phase verification and evidence-gathering

**Files:**
- None created. Modify only if verification uncovers a genuine failure.

**Interfaces:**
- Consumes: everything from Tasks 1-11.
- Produces: confirmation of every guarantee in the design spec's §26, and the evidence needed for the phase-completion report.

- [ ] **Step 1: Confirm router/ and cpp-routing-service/ are untouched**

```bash
MERGE_BASE=$(git merge-base main HEAD)
git diff --stat "$MERGE_BASE"..HEAD -- router/ cpp-routing-service/ proto/
```

Expected: empty output. If this shows any changes, STOP and report BLOCKED.
Use the same `$MERGE_BASE` value in Step 6 below.

- [ ] **Step 2: Rebuild and test router/ and cpp-routing-service/**

```bash
rm -rf router/build cpp-routing-service/build
cmake -S router -B router/build && cmake --build router/build
ctest --test-dir router/build 2>&1 | tail -5
cmake -S cpp-routing-service -B cpp-routing-service/build && cmake --build cpp-routing-service/build
ctest --test-dir cpp-routing-service/build 2>&1 | tail -5
```

Expected: 49/49 and 62/62, unchanged from Phase 5.

- [ ] **Step 3: Run all Go test suites with counts**

```bash
cd go-api
go vet ./...
go test ./... -v 2>&1 | tee /tmp/go-unit-output.txt | tail -5
grep -c '^--- PASS' /tmp/go-unit-output.txt
grep -c '^--- FAIL' /tmp/go-unit-output.txt

DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable" \
    go vet -tags=integration ./...
DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable" \
    go test -tags=integration ./... -v 2>&1 | tee /tmp/go-integration-output.txt | tail -5
grep -c '^--- PASS' /tmp/go-integration-output.txt
grep -c '^--- FAIL' /tmp/go-integration-output.txt

go test -tags=kafka_integration ./internal/kafka/... -v
cd ..
```

Expected: `go vet` clean in both modes; 0 failures in both suites; the Kafka integration tests pass. Record the exact pass counts for the report.

- [ ] **Step 4: Run the full E2E script twice**

```bash
./scripts/e2e_test.sh
./scripts/e2e_test.sh
```

Expected: both runs print `All E2E checks passed.` with 10/10 numbered tests OK, and no leftover processes or rows afterward.

- [ ] **Step 5: Manually verify Kafka-unavailable startup tolerance**

```bash
rpk redpanda stop 2>/dev/null || pkill -f redpanda || true
sleep 1

DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable" \
KAFKA_BOOTSTRAP_SERVERS="localhost:9092" \
/tmp/chainroute-worker > /tmp/worker-no-kafka.log 2>&1 &
WORKER_PID=$!
sleep 3
kill -0 "$WORKER_PID" && echo "OK: worker did not crash with Kafka unavailable"
cat /tmp/worker-no-kafka.log

echo "Restarting Redpanda..."
brew services start redpanda-data/tap/redpanda 2>/dev/null || rpk redpanda start --overprovisioned --smp 1 --memory 1G --reserve-memory 0M --node-id 0 --check=false &
sleep 5
rpk cluster info

sleep 5
kill -0 "$WORKER_PID" && echo "OK: worker is still running after Redpanda recovered"

kill -TERM "$WORKER_PID"
wait "$WORKER_PID" 2>/dev/null
```

Expected: the worker process does not exit/crash-loop while Kafka is down (only Postgres is blocking-or-die at startup), and remains running once Redpanda comes back. Record this observation for the report — this is the one Phase 6 guarantee that is best verified live rather than via an automated test, since it concerns process-level crash-loop behavior against a real broker's actual availability transitions.

- [ ] **Step 6: Capture the final repository structure and files-changed listing**

```bash
find go-api scripts -type f \
    -not -path "*/build/*" -not -path "*/gen/*" -not -path "*/.git/*" | sort
git diff --stat "$MERGE_BASE"..HEAD -- go-api/ scripts/
```

- [ ] **Step 7: Capture the final PostgreSQL schema**

```bash
DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable"
/opt/homebrew/opt/postgresql@16/bin/psql "$DATABASE_URL" -c '\d payments'
/opt/homebrew/opt/postgresql@16/bin/psql "$DATABASE_URL" -c '\d outbox_events'
```

- [ ] **Step 8: Capture one live example of the full async pipeline**

Using a fresh live stack (C++ service, Go API, worker, all built and started as in Task 11), `POST /payments` with a new Idempotency-Key, then poll `GET /payments/{id}` and capture the exact request/response pair showing the transition from the creation response (`status: ROUTED`) to the final polled response (`status: COMPLETED` or `FAILED`, `completed_at` populated). Tear down cleanly afterward.

- [ ] **Step 9: Note test-coverage equivalences for the report**

Two items from the design spec's failure-injection list (§24) are covered by tests already written for other reasons rather than by a dedicated separate test — note this explicitly in the report rather than writing a redundant test:
- "Worker DB commit succeeded, offset commit skipped" is exercised by Task 7's `TestHandleRoutedPayment_DuplicateDeliveryExecutesOnlyOnce`: calling the handler twice for the identical event is exactly what happens when an offset commit is skipped and Kafka redelivers the same message.
- "Publish succeeded, outbox mark failed" and "publish itself failed" both roll back to the same unpublished, retryable state in `PublishNextOutboxEvent`'s implementation (any failure between claiming the row and committing the transaction has an identical effect) — Task 5's `TestPublishNextOutboxEvent_FailedPublishLeavesRowUnpublishedForRetry` exercises this shared code path.

- [ ] **Step 10: Commit (only if Step 2 or Step 3 required fixes)**

```bash
git add -A
git commit -m "fix(go-api): resolve issues found during Phase 6 full verification"
```

If no changes were needed, skip this commit.

- [ ] **Step 11: Report back**

Report to the controller: pass/fail confirmation for every step above, the exact test counts by component, the Step 6 repository structure and files-changed listing, the Step 7 schema output, and the Step 8 live example captured verbatim. The controller composes the final phase-completion report to the user from this material.
