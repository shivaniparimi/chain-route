# ChainRoute Phase 5 Durable Payments Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `POST /payments` and `GET /payments/{id}` per
`docs/superpowers/specs/2026-09-11-postgres-payments-design.md`: durable,
atomically-persisted payments with a PostgreSQL-enforced idempotency key,
on top of the already-merged Phase 4 Go API and C++ routing service,
unmodified.

**Architecture:** A new `internal/payment` domain package, a new
`internal/postgres` repository (raw SQL via `database/sql` + the `pgx`
stdlib driver, one `Store` type), extensions to the existing
`internal/handler.Handler` (a new `Store` field, two new HTTP methods),
and `cmd/server/main.go` wiring for the DB pool. Idempotency correctness
comes entirely from a PostgreSQL `UNIQUE` constraint plus
`INSERT ... ON CONFLICT DO NOTHING RETURNING` — no application-level
locking anywhere.

**Tech Stack:** Go 1.22+ (existing), PostgreSQL 13+, `github.com/jackc/pgx/v5`
(via its `stdlib` shim), existing gRPC/protobuf stack unchanged.

## Global Constraints

- Do NOT modify `router/` or any C++ file under `cpp-routing-service/`, or
  `proto/chainroute/v1/routing.proto`. This plan only touches `go-api/`
  and `scripts/e2e_test.sh`.
- `internal/handler/routes.go` (`POST /routes`) keeps its exact existing
  behavior — the only change to that file is adding one new field
  (`Store PaymentStore`) to the existing `Handler` struct, which is
  additive and does not alter `PostRoutes`.
- The authoritative payment `amount` is a Go `string` everywhere in the
  persistence path — request JSON, the `payment.Payment` struct, the SQL
  parameter bound to the `NUMERIC(38,18)` column, the value read back, the
  response JSON. It is never parsed into a Go numeric type for
  persistence or idempotency comparison. (The **one** narrow exception,
  explained in Task 5, is a transient `float64` conversion solely to
  populate the pre-existing, unmodified `FindRouteRequest.amount double`
  gRPC field — that field has always been a liquidity-filter threshold
  for the C++ simulator, not authoritative money, since Phase 2; Phase 5
  does not change its type or meaning.)
- Simulated route metrics (`total_fee`, per-hop `fee`/`latency_ms`/
  `liquidity`/`reliability`) stay `float64` in Go and `DOUBLE PRECISION`
  in Postgres, unchanged in kind from Phase 4.
- Idempotency correctness must never depend on an application-level
  check-then-insert sequence. The only correctness-bearing operation is
  `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING RETURNING ...`
  backed by `UNIQUE (idempotency_key)`. Any earlier lookup is explicitly
  an optimization only, and the code/tests must demonstrate this (the
  concurrent test in Task 4 is what proves it).
- Payment + all its hops persist in exactly one transaction. A losing
  `ON CONFLICT` attempt never inserts hops.
- Exactly one externally visible status: `ROUTED`.
- `POST /routes` remains public and unchanged.
- Amount validation regex is exactly `^\d{1,20}(\.\d{1,18})?$`, plus a
  reject-if-all-digits-are-zero check, run before the gRPC call.
- No Kafka, Redis, background workers, payment execution, blockchain
  calls, ORM, or migration framework.
- Build with `go vet ./...` clean and no new project-code warnings
  anywhere.

## Environment note

This environment has PostgreSQL client tools (`psql`, `pg_ctl` from
`libpq`) but no PostgreSQL **server**. Task 1 installs and starts one via
Homebrew, mirroring how Phase 4 installed the missing Go/gRPC toolchain.

---

### Task 1: Environment setup, migration, and pgx dependency

**Files:**
- Create: `go-api/migrations/0001_create_payments.sql`
- Create: `go-api/.env.example`
- Modify: `go-api/go.mod`, `go-api/go.sum` (add `github.com/jackc/pgx/v5`)

**Interfaces:**
- Produces: a running local PostgreSQL instance with the `payments` and
  `payment_route_hops` tables created, and `pgx` available as a Go
  dependency.

- [ ] **Step 1: Install and start PostgreSQL**

```bash
brew install postgresql@16
brew services start postgresql@16
```

Wait a few seconds for it to come up, then confirm it's listening:

```bash
/opt/homebrew/opt/postgresql@16/bin/pg_isready
```

Create a database for this project:

```bash
/opt/homebrew/opt/postgresql@16/bin/createdb chainroute
```

**If `createdb`/`psql` on PATH resolve to the `libpq`-only versions
(which lack `createdb`) instead of the full `postgresql@16` client
tools**, use the full paths under `/opt/homebrew/opt/postgresql@16/bin/`
explicitly, or `brew link postgresql@16 --force` to prefer them on PATH
(check for conflicts with the existing `libpq` symlinks first — resolve
by using explicit paths if linking conflicts).

- [ ] **Step 2: Write the migration**

`go-api/migrations/0001_create_payments.sql`:

```sql
CREATE TABLE payments (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key    TEXT NOT NULL,
    source_chain       TEXT NOT NULL,
    destination_chain  TEXT NOT NULL,
    asset              TEXT NOT NULL,
    amount             NUMERIC(38, 18) NOT NULL CHECK (amount > 0),
    status             TEXT NOT NULL DEFAULT 'ROUTED' CHECK (status IN ('ROUTED')),
    total_fee          DOUBLE PRECISION NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (source_chain <> destination_chain),
    UNIQUE (idempotency_key)
);

CREATE TABLE payment_route_hops (
    payment_id   UUID NOT NULL REFERENCES payments(id) ON DELETE CASCADE,
    hop_index    INTEGER NOT NULL,
    from_chain   TEXT NOT NULL,
    to_chain     TEXT NOT NULL,
    bridge_name  TEXT NOT NULL,
    fee          DOUBLE PRECISION NOT NULL,
    latency_ms   DOUBLE PRECISION NOT NULL,
    liquidity    DOUBLE PRECISION NOT NULL,
    reliability  DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (payment_id, hop_index)
);
```

- [ ] **Step 3: Apply the migration**

```bash
DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable"
/opt/homebrew/opt/postgresql@16/bin/psql "$DATABASE_URL" -f go-api/migrations/0001_create_payments.sql
/opt/homebrew/opt/postgresql@16/bin/psql "$DATABASE_URL" -c '\dt'
```

Expected: `\dt` lists `payments` and `payment_route_hops`.

- [ ] **Step 4: Write the .env.example**

`go-api/.env.example`:

```
DATABASE_URL=postgres://user:password@localhost:5432/chainroute?sslmode=disable
```

- [ ] **Step 5: Add the pgx dependency**

```bash
cd go-api
go get github.com/jackc/pgx/v5
go mod tidy
cd ..
```

Expected: `go-api/go.mod` now requires `github.com/jackc/pgx/v5`;
`go build ./...` still succeeds (nothing imports it yet, but the module
resolves cleanly).

- [ ] **Step 6: Commit**

```bash
git add go-api/migrations/ go-api/.env.example go-api/go.mod go-api/go.sum
git commit -m "feat(go-api): add payments migration, .env.example, and pgx dependency"
```

---

### Task 2: Payment domain model and Store.GetPayment

**Files:**
- Create: `go-api/internal/payment/payment.go`
- Create: `go-api/internal/postgres/store.go`
- Create: `go-api/internal/postgres/store_integration_test.go`

**Interfaces:**
- Produces:
```go
package payment
type Status string
const StatusRouted Status = "ROUTED"
type Payment struct { ID, IdempotencyKey, SourceChain, DestinationChain, Asset, Amount string; Status Status; TotalFee float64; Hops []Hop; CreatedAt, UpdatedAt time.Time }
type Hop struct { HopIndex int; FromChain, ToChain, BridgeName string; Fee, LatencyMs, Liquidity, Reliability float64 }
type CreateResult int
const ( Created CreateResult = iota; Replayed; Conflict )
```
```go
package postgres
type Store struct{ /* unexported db *sql.DB */ }
func New(db *sql.DB) *Store
func (s *Store) GetPayment(ctx context.Context, id string) (payment.Payment, bool, error)
```
(`CreateOrGetPayment` is added in Task 3 — this task's `store.go` compiles
with only `GetPayment` and its shared helper implemented.)

- [ ] **Step 1: Write the domain model**

`go-api/internal/payment/payment.go`:

```go
package payment

import "time"

type Status string

const StatusRouted Status = "ROUTED"

type Payment struct {
	ID                string
	IdempotencyKey    string
	SourceChain       string
	DestinationChain  string
	Asset             string
	Amount            string
	Status            Status
	TotalFee          float64
	Hops              []Hop
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type Hop struct {
	HopIndex    int
	FromChain   string
	ToChain     string
	BridgeName  string
	Fee         float64
	LatencyMs   float64
	Liquidity   float64
	Reliability float64
}

// CreateResult reports what Store.CreateOrGetPayment actually did.
type CreateResult int

const (
	Created  CreateResult = iota // a brand-new payment was inserted
	Replayed                      // an existing payment, same logical request, was returned
	Conflict                      // the idempotency key was already used with a different request
)
```

- [ ] **Step 2: Write the Store skeleton with GetPayment and shared helpers**

`go-api/internal/postgres/store.go`:

```go
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"

	"chainroute/go-api/internal/payment"
)

type Store struct {
	db *sql.DB
}

func New(db *sql.DB) *Store {
	return &Store{db: db}
}

// findByIdempotencyKey looks up an existing payment by idempotency key,
// reporting via Postgres's own NUMERIC equality whether it matches the
// given candidate's request fields. found=false means no row exists.
func (s *Store) findByIdempotencyKey(ctx context.Context, p payment.Payment) (existing payment.Payment, matches bool, found bool, err error) {
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
	if errors.Is(err, sql.ErrNoRows) {
		return payment.Payment{}, false, false, nil
	}
	if err != nil {
		return payment.Payment{}, false, false, fmt.Errorf("find by idempotency key: %w", err)
	}
	existing.Status = payment.Status(status)
	existing.IdempotencyKey = p.IdempotencyKey
	return existing, matches, true, nil
}

func (s *Store) hopsForPayment(ctx context.Context, paymentID string) ([]payment.Hop, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT hop_index, from_chain, to_chain, bridge_name, fee, latency_ms, liquidity, reliability
		FROM payment_route_hops
		WHERE payment_id = $1
		ORDER BY hop_index
	`, paymentID)
	if err != nil {
		return nil, fmt.Errorf("query hops: %w", err)
	}
	defer rows.Close()

	var hops []payment.Hop
	for rows.Next() {
		var h payment.Hop
		if err := rows.Scan(&h.HopIndex, &h.FromChain, &h.ToChain, &h.BridgeName,
			&h.Fee, &h.LatencyMs, &h.Liquidity, &h.Reliability); err != nil {
			return nil, fmt.Errorf("scan hop: %w", err)
		}
		hops = append(hops, h)
	}
	return hops, rows.Err()
}

func (s *Store) GetPayment(ctx context.Context, id string) (payment.Payment, bool, error) {
	var p payment.Payment
	var status string
	row := s.db.QueryRowContext(ctx, `
		SELECT id, idempotency_key, source_chain, destination_chain, asset, amount::text,
		       total_fee, status, created_at, updated_at
		FROM payments
		WHERE id = $1
	`, id)
	err := row.Scan(&p.ID, &p.IdempotencyKey, &p.SourceChain, &p.DestinationChain, &p.Asset,
		&p.Amount, &p.TotalFee, &status, &p.CreatedAt, &p.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return payment.Payment{}, false, nil
	}
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "22P02" { // invalid_text_representation: malformed UUID
			return payment.Payment{}, false, nil
		}
		return payment.Payment{}, false, fmt.Errorf("get payment: %w", err)
	}
	p.Status = payment.Status(status)

	hops, err := s.hopsForPayment(ctx, p.ID)
	if err != nil {
		return payment.Payment{}, false, err
	}
	p.Hops = hops

	return p, true, nil
}
```

Note the `pgconn.PgError` / SQLSTATE `22P02` check: a malformed (non-UUID)
`id` string causes Postgres itself to reject the query with an
`invalid_text_representation` error, which is deliberately treated the
same as not-found (per the spec: "a malformed id is also just 404").
Any *other* error (a real connectivity failure, etc.) is NOT swallowed
this way — it propagates as a genuine error, which the handler (Task 5)
maps to a 500. Do not broaden this to "any error means not found."

- [ ] **Step 3: Write the integration test**

`go-api/internal/postgres/store_integration_test.go`:

```go
//go:build integration

package postgres

import (
	"context"
	"database/sql"
	"os"
	"testing"

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
	// Isolate each test's rows with a unique idempotency-key prefix rather
	// than truncating shared tables, so tests can run in parallel safely.
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
```

(More `GetPayment` coverage — the found-with-hops case — comes for free
in Task 3's tests, since `CreateOrGetPayment` is the only way to insert a
row and its own tests already verify a subsequent `GetPayment` round-trip
matches. No need to duplicate that here.)

- [ ] **Step 4: Build and run**

```bash
cd go-api
go build ./...
go vet ./...
DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable" go test -tags=integration ./internal/postgres/...
cd ..
```

Expected: builds clean, `go vet` clean, both tests pass.

- [ ] **Step 5: Commit**

```bash
git add go-api/internal/payment/ go-api/internal/postgres/
git commit -m "feat(go-api): add payment domain model and Store.GetPayment"
```

---

### Task 3: Store.CreateOrGetPayment (creation + idempotency core)

**Files:**
- Modify: `go-api/internal/postgres/store.go` (add `CreateOrGetPayment`)
- Modify: `go-api/internal/postgres/store_integration_test.go` (add 4 tests)

**Interfaces:**
- Produces: `func (s *Store) CreateOrGetPayment(ctx context.Context, p payment.Payment) (payment.Payment, payment.CreateResult, error)`

- [ ] **Step 1: Write the failing tests**

Add to `go-api/internal/postgres/store_integration_test.go`, inside the
same package, using a helper to build test payments:

```go
func testPayment(idempotencyKey string) payment.Payment {
	return payment.Payment{
		IdempotencyKey:    idempotencyKey,
		SourceChain:       "ethereum",
		DestinationChain:  "base",
		Asset:             "USDC",
		Amount:            "1000.00",
		TotalFee:          1.5,
		Hops: []payment.Hop{
			{HopIndex: 0, FromChain: "ethereum", ToChain: "base", BridgeName: "TestBridge#1",
				Fee: 1.5, LatencyMs: 500, Liquidity: 1000000, Reliability: 0.99},
		},
	}
}

func TestCreateOrGetPayment_NormalCreation(t *testing.T) {
	s := newTestStore(t)
	p := testPayment("test-normal-creation-key")

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
```

- [ ] **Step 2: Run to verify these fail (CreateOrGetPayment doesn't exist yet)**

```bash
cd go-api
DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable" go test -tags=integration ./internal/postgres/... 2>&1 | head -20
cd ..
```

Expected: compile error, `CreateOrGetPayment` undefined.

- [ ] **Step 3: Implement CreateOrGetPayment**

Add to `go-api/internal/postgres/store.go`, after `GetPayment`:

```go
func (s *Store) CreateOrGetPayment(ctx context.Context, p payment.Payment) (payment.Payment, payment.CreateResult, error) {
	// Optimization only, not correctness-critical: skip the routing RPC's
	// result entirely for the common retry case. If this races with a
	// concurrent insert and misses it, nothing breaks -- the INSERT below
	// is what actually enforces correctness.
	if existing, matches, found, err := s.findByIdempotencyKey(ctx, p); err != nil {
		return payment.Payment{}, 0, err
	} else if found {
		if !matches {
			return payment.Payment{}, payment.Conflict, nil
		}
		hops, err := s.hopsForPayment(ctx, existing.ID)
		if err != nil {
			return payment.Payment{}, 0, err
		}
		existing.Hops = hops
		return existing, payment.Replayed, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return payment.Payment{}, 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var created payment.Payment
	row := tx.QueryRowContext(ctx, `
		INSERT INTO payments (idempotency_key, source_chain, destination_chain, asset, amount, total_fee)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id, created_at, updated_at
	`, p.IdempotencyKey, p.SourceChain, p.DestinationChain, p.Asset, p.Amount, p.TotalFee)

	err = row.Scan(&created.ID, &created.CreatedAt, &created.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		// Lost the race: a concurrent request already created this key.
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			return payment.Payment{}, 0, fmt.Errorf("rollback after lost race: %w", rbErr)
		}
		existing, matches, found, ferr := s.findByIdempotencyKey(ctx, p)
		if ferr != nil {
			return payment.Payment{}, 0, ferr
		}
		if !found {
			return payment.Payment{}, 0, errors.New("idempotency key conflicted but no row found on re-read")
		}
		if !matches {
			return payment.Payment{}, payment.Conflict, nil
		}
		hops, herr := s.hopsForPayment(ctx, existing.ID)
		if herr != nil {
			return payment.Payment{}, 0, herr
		}
		existing.Hops = hops
		return existing, payment.Replayed, nil
	}
	if err != nil {
		return payment.Payment{}, 0, fmt.Errorf("insert payment: %w", err)
	}

	for _, h := range p.Hops {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO payment_route_hops
				(payment_id, hop_index, from_chain, to_chain, bridge_name, fee, latency_ms, liquidity, reliability)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		`, created.ID, h.HopIndex, h.FromChain, h.ToChain, h.BridgeName, h.Fee, h.LatencyMs, h.Liquidity, h.Reliability); err != nil {
			return payment.Payment{}, 0, fmt.Errorf("insert hop %d: %w", h.HopIndex, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return payment.Payment{}, 0, fmt.Errorf("commit: %w", err)
	}

	created.IdempotencyKey = p.IdempotencyKey
	created.SourceChain = p.SourceChain
	created.DestinationChain = p.DestinationChain
	created.Asset = p.Asset
	created.Amount = p.Amount
	created.Status = payment.StatusRouted
	created.TotalFee = p.TotalFee
	created.Hops = p.Hops

	return created, payment.Created, nil
}
```

- [ ] **Step 4: Build and run, verify all pass**

```bash
cd go-api
go build ./...
go vet ./...
DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable" go test -tags=integration -v ./internal/postgres/...
cd ..
```

Expected: all tests pass (2 from Task 2 + 4 new = 6).

- [ ] **Step 5: Commit**

```bash
git add go-api/internal/postgres/
git commit -m "feat(go-api): add Store.CreateOrGetPayment with atomic idempotency"
```

---

### Task 4: Concurrent same-key creation test

**Files:**
- Modify: `go-api/internal/postgres/store_integration_test.go` (add 1 test)

**Interfaces:**
- Consumes: `Store.CreateOrGetPayment` (Task 3), unchanged.

This is its own task, not folded into Task 3, because it is the single
most important test in this plan for proving the "no application-level
check-then-insert race" constraint actually holds — it deserves focused
review on its own.

- [ ] **Step 1: Write the test**

Add to `go-api/internal/postgres/store_integration_test.go`:

```go
func TestCreateOrGetPayment_ConcurrentSameKeyCreation(t *testing.T) {
	s := newTestStore(t)
	key := "test-concurrent-same-key"
	p := testPayment(key)

	const n = 10
	type outcome struct {
		id      string
		result  payment.CreateResult
		err     error
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
```

Add `"sync"` to the file's imports.

- [ ] **Step 2: Run repeatedly to verify it's not flaky**

Concurrency tests can pass by luck once. Run it several times:

```bash
cd go-api
for i in 1 2 3 4 5; do
    DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable" \
        go test -tags=integration -run TestCreateOrGetPayment_ConcurrentSameKeyCreation -count=1 -v ./internal/postgres/...
done
cd ..
```

Expected: passes cleanly all 5 times. If it ever fails, that is a real
bug in `CreateOrGetPayment` (most likely the `ON CONFLICT` clause or the
uniqueness constraint), not a flaky test to retry past — stop and
investigate rather than re-running until it happens to pass.

- [ ] **Step 3: Run the full package once more**

```bash
cd go-api
DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable" go test -tags=integration ./internal/postgres/...
cd ..
```

Expected: all 7 tests pass.

- [ ] **Step 4: Commit**

```bash
git add go-api/internal/postgres/store_integration_test.go
git commit -m "test(go-api): add concurrent same-key idempotency test"
```

---

### Task 5: HTTP handler (POST /payments, GET /payments/{id})

**Files:**
- Modify: `go-api/internal/handler/routes.go` (add `Store PaymentStore` field to `Handler`)
- Create: `go-api/internal/handler/payments.go`
- Create: `go-api/internal/handler/payments_test.go`

**Interfaces:**
- Consumes: `payment.Payment`/`Hop`/`CreateResult` (Task 2), `RoutingClient`
  (existing, from `routes.go`), `chainByName`/`assetByName`/
  `chainNameByValue` (existing, from `routes.go`).
- Produces:
```go
type PaymentStore interface {
    CreateOrGetPayment(ctx context.Context, p payment.Payment) (payment.Payment, payment.CreateResult, error)
    GetPayment(ctx context.Context, id string) (payment.Payment, bool, error)
}
func (h *Handler) PostPayments(w http.ResponseWriter, r *http.Request)
func (h *Handler) GetPayment(w http.ResponseWriter, r *http.Request)
```

- [ ] **Step 1: Extend the existing Handler struct**

In `go-api/internal/handler/routes.go`, change:

```go
type Handler struct {
	Client RoutingClient
}
```

to:

```go
type Handler struct {
	Client RoutingClient
	Store  PaymentStore
}
```

This is the only change to `routes.go` — `PostRoutes`'s body and
behavior are completely unchanged.

- [ ] **Step 2: Write the failing tests**

`go-api/internal/handler/payments_test.go`:

```go
package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	routingv1 "chainroute/go-api/internal/gen/chainroute/v1"
	"chainroute/go-api/internal/payment"
)

type fakePaymentStore struct {
	createResult payment.Payment
	createOutcome payment.CreateResult
	createErr    error
	getResult    payment.Payment
	getFound     bool
	getErr       error
	lastCreate   payment.Payment
}

func (f *fakePaymentStore) CreateOrGetPayment(_ context.Context, p payment.Payment) (payment.Payment, payment.CreateResult, error) {
	f.lastCreate = p
	return f.createResult, f.createOutcome, f.createErr
}

func (f *fakePaymentStore) GetPayment(_ context.Context, id string) (payment.Payment, bool, error) {
	return f.getResult, f.getFound, f.getErr
}

func doPaymentRequest(h *Handler, method, path, idempotencyKey, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	rec := httptest.NewRecorder()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /payments", h.PostPayments)
	mux.HandleFunc("GET /payments/{id}", h.GetPayment)
	mux.ServeHTTP(rec, req)
	return rec
}

func validPaymentBody() string {
	return `{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":"1000.00"}`
}

func TestPostPayments_MissingIdempotencyKey(t *testing.T) {
	h := &Handler{Client: &fakeClient{}, Store: &fakePaymentStore{}}
	rec := doPaymentRequest(h, "POST", "/payments", "", validPaymentBody())
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestPostPayments_TooLongIdempotencyKey(t *testing.T) {
	h := &Handler{Client: &fakeClient{}, Store: &fakePaymentStore{}}
	longKey := make([]byte, 256)
	for i := range longKey {
		longKey[i] = 'a'
	}
	rec := doPaymentRequest(h, "POST", "/payments", string(longKey), validPaymentBody())
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestPostPayments_InvalidJSON(t *testing.T) {
	h := &Handler{Client: &fakeClient{}, Store: &fakePaymentStore{}}
	rec := doPaymentRequest(h, "POST", "/payments", "key-1", "{not json")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestPostPayments_InvalidChain(t *testing.T) {
	h := &Handler{Client: &fakeClient{}, Store: &fakePaymentStore{}}
	rec := doPaymentRequest(h, "POST", "/payments", "key-1",
		`{"source_chain":"mars","destination_chain":"base","asset":"USDC","amount":"1000.00"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestPostPayments_SameChain(t *testing.T) {
	h := &Handler{Client: &fakeClient{}, Store: &fakePaymentStore{}}
	rec := doPaymentRequest(h, "POST", "/payments", "key-1",
		`{"source_chain":"ethereum","destination_chain":"ethereum","asset":"USDC","amount":"1000.00"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestPostPayments_AmountValidation(t *testing.T) {
	cases := map[string]string{
		"too many integer digits":    `{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":"123456789012345678901"}`,
		"too many fractional digits": `{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":"1.1234567890123456789"}`,
		"zero":                       `{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":"0.00"}`,
		"negative sign":              `{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":"-100.00"}`,
		"exponent notation":          `{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":"1e10"}`,
		"leading dot":                `{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":".50"}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			h := &Handler{Client: &fakeClient{}, Store: &fakePaymentStore{}}
			rec := doPaymentRequest(h, "POST", "/payments", "key-1", body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 for %s, got %d: %s", name, rec.Code, rec.Body.String())
			}
		})
	}
}

func TestPostPayments_NoRouteFound(t *testing.T) {
	fake := &fakeClient{response: &routingv1.FindRouteResponse{RouteFound: false}}
	h := &Handler{Client: fake, Store: &fakePaymentStore{}}
	rec := doPaymentRequest(h, "POST", "/payments", "key-1", validPaymentBody())
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d", rec.Code)
	}
}

func TestPostPayments_SuccessfulCreation(t *testing.T) {
	fakeRoute := &fakeClient{response: &routingv1.FindRouteResponse{
		RouteFound: true,
		TotalFee:   2.5,
		Hops: []*routingv1.RouteHop{
			{FromChain: routingv1.Chain_CHAIN_ETHEREUM, ToChain: routingv1.Chain_CHAIN_BASE,
				BridgeName: "Hop#1", Fee: 2.5, LatencyMs: 500, Liquidity: 1000000, Reliability: 0.99},
		},
	}}
	store := &fakePaymentStore{
		createOutcome: payment.Created,
		createResult: payment.Payment{
			ID: "test-id-1", SourceChain: "ethereum", DestinationChain: "base",
			Asset: "USDC", Amount: "1000.00", Status: payment.StatusRouted, TotalFee: 2.5,
			Hops:      []payment.Hop{{HopIndex: 0, FromChain: "ethereum", ToChain: "base", BridgeName: "Hop#1", Fee: 2.5}},
			CreatedAt: time.Now(), UpdatedAt: time.Now(),
		},
	}
	h := &Handler{Client: fakeRoute, Store: store}
	rec := doPaymentRequest(h, "POST", "/payments", "key-1", validPaymentBody())

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	if loc := rec.Header().Get("Location"); loc != "/payments/test-id-1" {
		t.Fatalf("unexpected Location header: %q", loc)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["id"] != "test-id-1" || body["amount"] != "1000.00" || body["status"] != "ROUTED" {
		t.Fatalf("unexpected body: %+v", body)
	}
	if store.lastCreate.IdempotencyKey != "key-1" {
		t.Fatalf("expected idempotency key to be passed through, got %q", store.lastCreate.IdempotencyKey)
	}
}

func TestPostPayments_Replayed(t *testing.T) {
	fakeRoute := &fakeClient{response: &routingv1.FindRouteResponse{RouteFound: true, TotalFee: 2.5}}
	store := &fakePaymentStore{
		createOutcome: payment.Replayed,
		createResult:  payment.Payment{ID: "existing-id", Status: payment.StatusRouted, CreatedAt: time.Now(), UpdatedAt: time.Now()},
	}
	h := &Handler{Client: fakeRoute, Store: store}
	rec := doPaymentRequest(h, "POST", "/payments", "key-1", validPaymentBody())
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestPostPayments_Conflict(t *testing.T) {
	fakeRoute := &fakeClient{response: &routingv1.FindRouteResponse{RouteFound: true, TotalFee: 2.5}}
	store := &fakePaymentStore{createOutcome: payment.Conflict}
	h := &Handler{Client: fakeRoute, Store: store}
	rec := doPaymentRequest(h, "POST", "/payments", "key-1", validPaymentBody())
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rec.Code)
	}
}

func TestGetPayment_Found(t *testing.T) {
	store := &fakePaymentStore{
		getFound: true,
		getResult: payment.Payment{
			ID: "test-id-1", SourceChain: "ethereum", DestinationChain: "base",
			Asset: "USDC", Amount: "1000.00", Status: payment.StatusRouted,
			CreatedAt: time.Now(), UpdatedAt: time.Now(),
		},
	}
	h := &Handler{Client: &fakeClient{}, Store: store}
	rec := doPaymentRequest(h, "GET", "/payments/test-id-1", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestGetPayment_NotFound(t *testing.T) {
	store := &fakePaymentStore{getFound: false}
	h := &Handler{Client: &fakeClient{}, Store: store}
	rec := doPaymentRequest(h, "GET", "/payments/nonexistent", "", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}
```

- [ ] **Step 3: Verify these fail to compile**

```bash
cd go-api
go build ./... 2>&1 | head -20
cd ..
```

Expected: compile errors — `PaymentStore`, `PostPayments`, `GetPayment`
undefined on `Handler`.

- [ ] **Step 4: Implement payments.go**

`go-api/internal/handler/payments.go`:

```go
package handler

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	routingv1 "chainroute/go-api/internal/gen/chainroute/v1"
	"chainroute/go-api/internal/payment"
)

var amountPattern = regexp.MustCompile(`^\d{1,20}(\.\d{1,18})?$`)

const maxIdempotencyKeyLength = 255

type PaymentStore interface {
	CreateOrGetPayment(ctx context.Context, p payment.Payment) (payment.Payment, payment.CreateResult, error)
	GetPayment(ctx context.Context, id string) (payment.Payment, bool, error)
}

var assetNameByValue = map[routingv1.Asset]string{
	routingv1.Asset_ASSET_USDC: "USDC",
	routingv1.Asset_ASSET_ETH:  "ETH",
}

type createPaymentRequest struct {
	SourceChain      string `json:"source_chain"`
	DestinationChain string `json:"destination_chain"`
	Asset            string `json:"asset"`
	Amount           string `json:"amount"`
}

type hopResponse struct {
	HopIndex    int     `json:"hop_index"`
	FromChain   string  `json:"from_chain"`
	ToChain     string  `json:"to_chain"`
	BridgeName  string  `json:"bridge_name"`
	Fee         float64 `json:"fee"`
	LatencyMs   float64 `json:"latency_ms"`
	Liquidity   float64 `json:"liquidity"`
	Reliability float64 `json:"reliability"`
}

type paymentResponse struct {
	ID                string        `json:"id"`
	SourceChain       string        `json:"source_chain"`
	DestinationChain  string        `json:"destination_chain"`
	Asset             string        `json:"asset"`
	Amount            string        `json:"amount"`
	Status            string        `json:"status"`
	TotalFee          float64       `json:"total_fee"`
	Hops              []hopResponse `json:"hops"`
	CreatedAt         string        `json:"created_at"`
	UpdatedAt         string        `json:"updated_at"`
}

func toPaymentResponse(p payment.Payment) paymentResponse {
	hops := make([]hopResponse, 0, len(p.Hops))
	for _, h := range p.Hops {
		hops = append(hops, hopResponse{
			HopIndex: h.HopIndex, FromChain: h.FromChain, ToChain: h.ToChain,
			BridgeName: h.BridgeName, Fee: h.Fee, LatencyMs: h.LatencyMs,
			Liquidity: h.Liquidity, Reliability: h.Reliability,
		})
	}
	return paymentResponse{
		ID: p.ID, SourceChain: p.SourceChain, DestinationChain: p.DestinationChain,
		Asset: p.Asset, Amount: p.Amount, Status: string(p.Status), TotalFee: p.TotalFee,
		Hops: hops, CreatedAt: p.CreatedAt.UTC().Format(time.RFC3339Nano),
		UpdatedAt: p.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
}

func isZeroAmount(amount string) bool {
	for _, r := range amount {
		if r != '0' && r != '.' {
			return false
		}
	}
	return true
}

func (h *Handler) PostPayments(w http.ResponseWriter, r *http.Request) {
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if idempotencyKey == "" {
		writeError(w, http.StatusBadRequest, "Idempotency-Key header is required")
		return
	}
	if len(idempotencyKey) > maxIdempotencyKeyLength {
		writeError(w, http.StatusBadRequest, "Idempotency-Key exceeds maximum length")
		return
	}

	var req createPaymentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	sourceChain, ok := chainByName[strings.ToLower(req.SourceChain)]
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid chain: "+req.SourceChain)
		return
	}
	destChain, ok := chainByName[strings.ToLower(req.DestinationChain)]
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid chain: "+req.DestinationChain)
		return
	}
	asset, ok := assetByName[strings.ToLower(req.Asset)]
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid asset: "+req.Asset)
		return
	}
	if !amountPattern.MatchString(req.Amount) || isZeroAmount(req.Amount) {
		writeError(w, http.StatusBadRequest,
			"amount must be a positive decimal with at most 20 integer digits and 18 fractional digits")
		return
	}
	if sourceChain == destChain {
		writeError(w, http.StatusBadRequest, "source and destination must differ")
		return
	}

	// This float64 conversion feeds only the pre-existing (Phase 2-4,
	// unmodified) FindRouteRequest.amount `double` field, which has
	// always been a liquidity-filter threshold for the C++ simulator --
	// never the authoritative amount. req.Amount (the exact string) is
	// what gets persisted below, untouched by this conversion.
	amountForRouting, err := strconv.ParseFloat(req.Amount, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid amount")
		return
	}

	grpcReq := &routingv1.FindRouteRequest{
		SourceChain: sourceChain, DestinationChain: destChain,
		Asset: asset, Amount: amountForRouting,
	}

	resp, err := h.Client.FindRoute(r.Context(), grpcReq)
	if err != nil {
		st, _ := status.FromError(err)
		switch st.Code() {
		case codes.InvalidArgument:
			log.Printf("WARNING: routing service rejected a request that passed Go validation (possible validation drift): %v", st.Message())
			writeError(w, http.StatusBadRequest, st.Message())
		case codes.Unavailable:
			writeError(w, http.StatusServiceUnavailable, "routing service unavailable")
		case codes.DeadlineExceeded:
			writeError(w, http.StatusGatewayTimeout, "routing service timed out")
		default:
			log.Printf("ERROR: routing service call failed: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}

	if !resp.GetRouteFound() {
		writeError(w, http.StatusUnprocessableEntity, "no route available for the requested payment")
		return
	}

	hops := make([]payment.Hop, 0, len(resp.GetHops()))
	for i, hop := range resp.GetHops() {
		hops = append(hops, payment.Hop{
			HopIndex: i, FromChain: chainNameByValue[hop.GetFromChain()], ToChain: chainNameByValue[hop.GetToChain()],
			BridgeName: hop.GetBridgeName(), Fee: hop.GetFee(), LatencyMs: hop.GetLatencyMs(),
			Liquidity: hop.GetLiquidity(), Reliability: hop.GetReliability(),
		})
	}

	candidate := payment.Payment{
		IdempotencyKey: idempotencyKey, SourceChain: chainNameByValue[sourceChain],
		DestinationChain: chainNameByValue[destChain], Asset: assetNameByValue[asset],
		Amount: req.Amount, TotalFee: resp.GetTotalFee(), Hops: hops,
	}

	result, outcome, err := h.Store.CreateOrGetPayment(r.Context(), candidate)
	if err != nil {
		log.Printf("ERROR: failed to persist payment: %v", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	switch outcome {
	case payment.Created:
		w.Header().Set("Location", "/payments/"+result.ID)
		writeJSON(w, http.StatusCreated, toPaymentResponse(result))
	case payment.Replayed:
		w.Header().Set("Location", "/payments/"+result.ID)
		writeJSON(w, http.StatusOK, toPaymentResponse(result))
	case payment.Conflict:
		writeError(w, http.StatusConflict, "Idempotency-Key already used with a different request")
	}
}

func (h *Handler) GetPayment(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	p, found, err := h.Store.GetPayment(r.Context(), id)
	if err != nil {
		log.Printf("ERROR: failed to read payment: %v", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "payment not found")
		return
	}
	writeJSON(w, http.StatusOK, toPaymentResponse(p))
}
```

- [ ] **Step 5: Build and run, verify all tests pass**

```bash
cd go-api
go build ./...
go vet ./...
go test ./internal/handler/...
cd ..
```

Expected: builds clean, `go vet` clean, all tests pass (13 pre-existing +
13 new = 26).

- [ ] **Step 6: Commit**

```bash
git add go-api/internal/handler/
git commit -m "feat(go-api): add POST /payments and GET /payments/{id} handlers"
```

---

### Task 6: main.go wiring

**Files:**
- Modify: `go-api/cmd/server/main.go`

**Interfaces:**
- Consumes: `postgres.New`, `postgres.Store` (Tasks 1-3), `handler.Handler`'s
  new `Store` field (Task 5).

- [ ] **Step 1: Rewrite main.go**

`go-api/cmd/server/main.go`:

```go
package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"chainroute/go-api/internal/grpcclient"
	"chainroute/go-api/internal/handler"
	"chainroute/go-api/internal/postgres"
)

func main() {
	httpAddr := flag.String("http-addr", ":8080", "HTTP listen address")
	grpcAddr := flag.String("grpc-addr", "127.0.0.1:50051", "gRPC routing service address")
	flag.Parse()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL environment variable is required")
	}

	client, err := grpcclient.Dial(*grpcAddr)
	if err != nil {
		log.Fatalf("failed to dial routing service at %s: %v", *grpcAddr, err)
	}
	defer client.Close()

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

	h := &handler.Handler{Client: client, Store: store}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /routes", h.PostRoutes)
	mux.HandleFunc("POST /payments", h.PostPayments)
	mux.HandleFunc("GET /payments/{id}", h.GetPayment)

	server := &http.Server{Addr: *httpAddr, Handler: mux}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("go-api listening on %s, routing service at %s, database connected", *httpAddr, *grpcAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown error: %v", err)
	}
	log.Println("go-api shut down")
}
```

- [ ] **Step 2: Build**

```bash
cd go-api
go build -o /tmp/go-api-server ./cmd/server
cd ..
```

Expected: builds successfully.

- [ ] **Step 3: Manual smoke test against the real running stack**

```bash
DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable"

cmake --build cpp-routing-service/build >/dev/null 2>&1 || \
    (cmake -S cpp-routing-service -B cpp-routing-service/build && cmake --build cpp-routing-service/build)
./cpp-routing-service/build/chainroute_service_server --seed=1001 --listen-address=127.0.0.1:50096 &
CPP_PID=$!
sleep 1

DATABASE_URL="$DATABASE_URL" /tmp/go-api-server --http-addr=:8097 --grpc-addr=127.0.0.1:50096 &
GO_PID=$!
sleep 1

curl -s -X POST http://127.0.0.1:8097/payments \
    -H "Content-Type: application/json" -H "Idempotency-Key: smoke-test-1" \
    -d '{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":"1000.00"}'
echo ""

kill -TERM "$GO_PID" "$CPP_PID"
wait "$GO_PID" "$CPP_PID" 2>/dev/null || true
```

Expected: a `201` response with a real payment body (JSON printed by
`curl -s` without `-i` won't show the status line — add `-i` if you want
to see it explicitly), then both processes shut down cleanly.

- [ ] **Step 4: Commit**

```bash
git add go-api/cmd/server/main.go
git commit -m "feat(go-api): wire PostgreSQL connection pool and payments routes into main"
```

---

### Task 7: E2E script extension

**Files:**
- Modify: `scripts/e2e_test.sh`

**Interfaces:**
- Consumes: everything from Tasks 1-6.

- [ ] **Step 1: Add Postgres setup and new test cases**

In `scripts/e2e_test.sh`, after the existing variable declarations near
the top, add:

```bash
DATABASE_URL="${DATABASE_URL:-postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable}"
export DATABASE_URL
```

Before starting the Go service (find the existing `"$ROOT_DIR/go-api/server" ...`
launch line), ensure the migration is applied (idempotent — `CREATE TABLE`
would error if already applied, so guard it):

```bash
echo "Ensuring database schema is up to date..."
/opt/homebrew/opt/postgresql@16/bin/psql "$DATABASE_URL" -tAc \
    "SELECT 1 FROM information_schema.tables WHERE table_name='payments'" | grep -q 1 || \
    /opt/homebrew/opt/postgresql@16/bin/psql "$DATABASE_URL" -f "$ROOT_DIR/go-api/migrations/0001_create_payments.sql"
```

After the existing three `POST /routes`-based tests (real route, invalid
chain, same chain), append:

```bash
echo "Test 4: POST /payments creates a payment"
PAYMENT_RESPONSE=$(curl -s -X POST "http://127.0.0.1:$HTTP_PORT/payments" \
    -H "Content-Type: application/json" -H "Idempotency-Key: e2e-test-key-1" \
    -d '{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":"1000.00"}')
echo "$PAYMENT_RESPONSE"
echo "$PAYMENT_RESPONSE" | grep -q '"status":"ROUTED"' || { echo "FAIL: expected status ROUTED"; exit 1; }
PAYMENT_ID=$(echo "$PAYMENT_RESPONSE" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
[[ -n "$PAYMENT_ID" ]] || { echo "FAIL: no payment id in response"; exit 1; }
echo "OK: created payment $PAYMENT_ID"

echo "Test 5: GET /payments/{id} reads it back"
GET_RESPONSE=$(curl -s "http://127.0.0.1:$HTTP_PORT/payments/$PAYMENT_ID")
echo "$GET_RESPONSE" | grep -q "\"id\":\"$PAYMENT_ID\"" || { echo "FAIL: GET did not return the same payment"; exit 1; }
echo "OK"

echo "Test 6: replaying the same Idempotency-Key returns 200 with the same payment"
REPLAY_STATUS=$(curl -s -o /tmp/e2e_replay_body.json -w "%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/payments" \
    -H "Content-Type: application/json" -H "Idempotency-Key: e2e-test-key-1" \
    -d '{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":"1000.00"}')
[[ "$REPLAY_STATUS" == "200" ]] || { echo "FAIL: expected 200 on replay, got $REPLAY_STATUS"; exit 1; }
grep -q "\"id\":\"$PAYMENT_ID\"" /tmp/e2e_replay_body.json || { echo "FAIL: replay returned a different payment"; exit 1; }
rm -f /tmp/e2e_replay_body.json
echo "OK: replay returned the same payment with 200"

echo "Test 7: same Idempotency-Key with a different body returns 409"
CONFLICT_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/payments" \
    -H "Content-Type: application/json" -H "Idempotency-Key: e2e-test-key-1" \
    -d '{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":"2000.00"}')
[[ "$CONFLICT_STATUS" == "409" ]] || { echo "FAIL: expected 409, got $CONFLICT_STATUS"; exit 1; }
echo "OK: got 409"

echo "Test 8: missing Idempotency-Key returns 400"
NOKEY_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/payments" \
    -H "Content-Type: application/json" \
    -d '{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":"1000.00"}')
[[ "$NOKEY_STATUS" == "400" ]] || { echo "FAIL: expected 400, got $NOKEY_STATUS"; exit 1; }
echo "OK: got 400"
```

Before the final "All E2E checks passed." line and the existing
teardown, add the restart-verification step:

```bash
echo "Test 9: payment survives a Go process restart"
kill -TERM "$GO_PID"
wait "$GO_PID" 2>/dev/null || true

"$ROOT_DIR/go-api/server" \
    --http-addr=":$HTTP_PORT" --grpc-addr="127.0.0.1:$CPP_PORT" &
GO_PID=$!
sleep 1

RESTART_RESPONSE=$(curl -s "http://127.0.0.1:$HTTP_PORT/payments/$PAYMENT_ID")
echo "$RESTART_RESPONSE" | grep -q "\"id\":\"$PAYMENT_ID\"" || { echo "FAIL: payment not found after restart"; exit 1; }
echo "OK: payment $PAYMENT_ID still readable after Go process restart"
```

(`GO_PID` is reassigned here so the script's existing `cleanup` trap still
tears down the *new* process correctly.)

- [ ] **Step 2: Run the full script**

```bash
./scripts/e2e_test.sh
```

Expected: all 9 tests print `OK`, ending with `All E2E checks passed.`,
and no leftover processes afterward.

- [ ] **Step 3: Commit**

```bash
git add scripts/e2e_test.sh
git commit -m "test(e2e): extend E2E script with payments, idempotency, and restart checks"
```

---

### Task 8: Full Phase 5 verification and reporting

**Files:**
- None created. Potentially modify any file if a warning or failure
  surfaces.

**Interfaces:**
- Consumes: everything from Tasks 1-7.
- Produces: full confirmation of every item in the user's
  post-implementation request list, for the controller to compose the
  final response (items 9-10 of that list are the controller's job, not
  this task's — see note at the end).

- [ ] **Step 1: Clean-build and test router/ and cpp-routing-service/ (unaffected, confirm they still are)**

```bash
rm -rf router/build cpp-routing-service/build
cmake -S router -B router/build && cmake --build router/build
ctest --test-dir router/build --output-on-failure 2>&1 | tail -5
cmake -S cpp-routing-service -B cpp-routing-service/build && cmake --build cpp-routing-service/build
ctest --test-dir cpp-routing-service/build --output-on-failure 2>&1 | tail -5
```

Expected: 49/49 and 62/62 respectively, unchanged from before this plan.

- [ ] **Step 2: Run all Go tests — unit and integration — with vet**

```bash
cd go-api
go vet ./...
go test ./...
DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable" go test -tags=integration ./internal/postgres/...
cd ..
```

Expected: `go vet` clean; unit tests pass (26 handler + 1 grpcclient = 27,
plus 0 in `payment`/`postgres` packages under the default non-integration
build); integration tests pass (7 in `postgres`). If any warning or
failure surfaces, fix the ROOT CAUSE (never suppress, never weaken a
test) and re-run until clean.

- [ ] **Step 3: Run the full E2E script**

```bash
./scripts/e2e_test.sh
```

Expected: `All E2E checks passed.`

- [ ] **Step 4: Verify router/ was not modified by this plan**

```bash
git diff --stat e415d71..HEAD -- router/
```

(If `e415d71` is not the right pre-Phase-5 commit in this checkout, use
`git log --oneline -- router/` to find the last commit that touched
`router/` and confirm it predates this plan's commits.) Expected: empty
output. If this shows any changes, STOP and report BLOCKED rather than
silently reverting.

- [ ] **Step 5: Capture the final repository structure**

```bash
find router proto cpp-routing-service go-api scripts -type f \
    -not -path "*/build/*" -not -path "*/gen/*" -not -path "*/.git/*" | sort
find go-api/internal/gen -type f | sort
```

- [ ] **Step 6: Capture the files-changed listing**

```bash
git diff --stat e415d71..HEAD -- go-api/ scripts/
```

- [ ] **Step 7: Capture the final PostgreSQL schema**

```bash
/opt/homebrew/opt/postgresql@16/bin/psql "postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable" -c '\d payments'
/opt/homebrew/opt/postgresql@16/bin/psql "postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable" -c '\d payment_route_hops'
```

- [ ] **Step 8: Capture the four concrete request examples**

Using a fresh live stack (build+start both services and the Go server
against `DATABASE_URL`, as in Task 6 Step 3), capture and save each of
these exactly:

1. A real `POST /payments` with a fresh `Idempotency-Key` — full request
   (headers + body) and full response (status + body).
2. The **same** request replayed with the **same** `Idempotency-Key` —
   request and response, showing `200` and the identical payment.
3. The same `Idempotency-Key` with a **different** body (e.g. a different
   `amount`) — request and response, showing `409`.
4. `GET /payments/{id}` for the payment from #1, called **after**
   restarting the Go process (SIGTERM, then start a fresh process against
   the same `DATABASE_URL`) — request and response, showing the same data
   read back from a process that never created it in memory.

Tear down cleanly afterward.

- [ ] **Step 9: Commit (only if Step 2 or Step 3 required code changes)**

```bash
git add -A
git commit -m "fix(go-api): resolve issues found during Phase 5 full verification"
```

If no changes were needed, skip this commit.

- [ ] **Step 10: Report back**

Report to the controller: the Step 5 repository structure, the Step 6
files-changed listing, the Step 7 schema output, the Step 8 four request/
response captures verbatim, and pass/fail confirmation for Steps 1-4. The
controller (not this task) composes the final step-by-step request
lifecycle explanation and the Phase 6-not-started confirmation — those
require synthesizing across the whole phase and are written directly for
the user, not as an implementation step.
