# Phase 12: React/TypeScript Payment Analytics Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A read-only React+TypeScript dashboard visualizing real ChainRoute payment data (lifecycle, routing, provider comparison), backed by a small justified schema extension (persist every fetched quote, not just the winner) and four new read-only backend endpoints — without touching routing, execution, or recovery logic.

**Architecture:** See `docs/superpowers/specs/2026-09-18-payment-analytics-dashboard-design.md` for full rationale. Key decisions locked in: keyset (not offset) pagination; a partial unique index (`WHERE selected`) enforcing exactly one winning quote per payment at the database level; polling via TanStack Query (not SSE/WebSockets); a hand-rolled SVG route visualization (not a graph library); S3+CloudFront for the frontend in AWS (no container needed for static assets); Tailwind with a restrained custom palette (not a component-library look).

## Global Constraints

- Do not modify `router/src/route.cpp`, `router/include/chainroute/route.hpp`, the C++ router's proto contract, or any file under `go-api/internal/{worker,bridge,execution}/` — this phase adds read paths and one additive persistence change, nothing about how a payment is routed, executed, or recovered changes.
- Every new HTTP endpoint is read-only. None may execute an `INSERT`/`UPDATE`/`DELETE` against `payments`, `payment_executions`, `payment_route_hops`, or `outbox_events`.
- `postgres.Store.GetQuoteByPaymentID` (singular) must continue returning exactly the selected/winning quote after the migration, with a task-level test proving this explicitly against a payment with multiple quote rows.
- No unbounded list endpoint — `GET /payments` always paginates, default 25/max 100 results.
- No N+1 query patterns — every new endpoint is a single query (or a small fixed number of queries) using SQL aggregation, never fetch-all-then-aggregate-in-Go.
- Never fabricate quote comparison or analytics data. An empty result renders an honest empty state in the frontend, never invented numbers.
- Never expose a private key, raw signed transaction byte, RPC URL, or API key in any HTTP response, log line, or frontend bundle.
- CORS allows only an explicit, configured origin list — never `*`.
- Terraform is `fmt`/`validate`/dummy-credential-`plan`-checked but never applied — no AWS credentials, no authorization in this environment.
- Docker/Terraform additions never expose Postgres, Redpanda, the C++ router, or the observability stack directly to the browser.
- Demo data is created only via real `POST /payments` calls in simulated mode — never a direct SQL insert, never hardcoded into the frontend.
- Every task that modifies an existing file must read that file's CURRENT content in full first — this plan's code reflects the repo as surveyed at plan-writing time.

---

### Task 1: Migration 0007 + quote-persistence backend changes

**Files:**
- Create: `go-api/migrations/0007_dashboard_payment_analytics.sql`
- Modify: `go-api/internal/payment/payment.go`
- Modify: `go-api/internal/postgres/quote_store.go`
- Modify: `go-api/internal/postgres/store.go`
- Modify: `go-api/internal/handler/payments.go`
- Test: `go-api/internal/postgres/quote_store_integration_test.go` (extend)
- Test: `go-api/internal/handler/payments_test.go` (extend)

**Interfaces:**
- Produces: `payment.Quote.Selected bool`; `payment.Payment.Quotes []Quote` (replacing the singular `Quote *Quote` field); `postgres.Store.GetQuotesByPaymentID(ctx, paymentID) ([]payment.Quote, error)` — consumed by Task 3's dashboard handler.

- [ ] **Step 1: `go-api/migrations/0007_dashboard_payment_analytics.sql`**

```sql
ALTER TABLE payment_quotes DROP CONSTRAINT payment_quotes_payment_id_key;
ALTER TABLE payment_quotes ADD COLUMN selected BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE payment_quotes ADD CONSTRAINT payment_quotes_payment_id_provider_key UNIQUE (payment_id, provider);

CREATE UNIQUE INDEX payment_quotes_one_selected_per_payment
    ON payment_quotes (payment_id) WHERE selected;

CREATE INDEX payments_created_at_idx ON payments (created_at DESC);
CREATE INDEX payments_status_idx ON payments (status);
CREATE INDEX payments_execution_mode_idx ON payments (execution_mode);
CREATE INDEX payments_bridge_provider_idx ON payments (bridge_provider) WHERE bridge_provider IS NOT NULL;
CREATE INDEX payments_source_dest_idx ON payments (source_chain, destination_chain);
```

Read `go-api/migrations/0005_realtime_bridge_routing.sql` first to confirm the exact constraint name Postgres auto-generated for `payment_quotes`'s original `UNIQUE (payment_id)` (it should be `payment_quotes_payment_id_key`, Postgres's standard auto-naming convention for a single-column unique constraint, but confirm by reading the migration rather than assuming — if migration 0005 named it explicitly with a different name, use that name in the `DROP CONSTRAINT` line instead).

- [ ] **Step 2: `go-api/internal/payment/payment.go` — add `Selected`, replace `Quote *Quote` with `Quotes []Quote`**

Read the current file in full (132 lines) first. Add `Selected bool` to the `Quote` struct (after `FeeAmount`, before `EstimatedFillTimeSec` is a reasonable position, or wherever reads naturally — exact position doesn't matter, just add the field). Change `Payment.Quote *Quote` to `Payment.Quotes []Quote`, updating its doc comment: `// Quotes holds every quote fetched for a testnet-mode payment (winning and losing), exactly one of which has Selected=true -- the C++ router's winning hop, persisted atomically with the payment. Empty for simulated-mode payments.`

- [ ] **Step 3: `go-api/internal/postgres/quote_store.go` — `insertPaymentQuotes` (plural), updated `GetQuoteByPaymentID`, new `GetQuotesByPaymentID`**

Read the current file in full (shown above — 78 lines) before editing.

```go
func insertPaymentQuotes(ctx context.Context, tx *sql.Tx, paymentID string, quotes []payment.Quote) error {
	for _, q := range quotes {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO payment_quotes
				(payment_id, provider, origin_chain_id, destination_chain_id, asset,
				 input_amount, output_amount, fee_amount, estimated_fill_time_sec,
				 quoted_at, expires_at, raw_provider_payload, selected)
			VALUES ($1, $2, $3, $4, $5, $6::NUMERIC, $7::NUMERIC, $8::NUMERIC, $9, $10, $11, $12::JSONB, $13)
		`, paymentID, q.Provider, q.OriginChainID, q.DestinationChainID, q.Asset,
			q.InputAmount, q.OutputAmount, q.FeeAmount, q.EstimatedFillTimeSec,
			q.QuotedAt, q.ExpiresAt, string(q.RawProviderPayload), q.Selected); err != nil {
			return fmt.Errorf("insert payment_quotes (provider=%s): %w", q.Provider, err)
		}
	}
	return nil
}

// GetQuoteByPaymentID returns the single SELECTED quote row for paymentID
// -- the payment_quotes_one_selected_per_payment partial unique index
// (migration 0007) guarantees there is at most one such row, so this
// keeps returning exactly what it always returned pre-migration: the
// winning quote the Executor validates against and executes. found=false
// means this is a simulated-mode payment, or a testnet-mode payment
// somehow created without one (should be unreachable).
func (s *Store) GetQuoteByPaymentID(ctx context.Context, paymentID string) (payment.Quote, bool, error) {
	var q payment.Quote
	var rawPayload []byte
	row := s.db.QueryRowContext(ctx, `
		SELECT id, payment_id, provider, origin_chain_id, destination_chain_id, asset,
		       input_amount::text, output_amount::text, fee_amount::text, estimated_fill_time_sec,
		       quoted_at, expires_at, raw_provider_payload, created_at, selected
		FROM payment_quotes
		WHERE payment_id = $1 AND selected = true
	`, paymentID)
	err := row.Scan(&q.ID, &q.PaymentID, &q.Provider, &q.OriginChainID, &q.DestinationChainID, &q.Asset,
		&q.InputAmount, &q.OutputAmount, &q.FeeAmount, &q.EstimatedFillTimeSec,
		&q.QuotedAt, &q.ExpiresAt, &rawPayload, &q.CreatedAt, &q.Selected)
	if errors.Is(err, sql.ErrNoRows) {
		return payment.Quote{}, false, nil
	}
	if err != nil {
		return payment.Quote{}, false, fmt.Errorf("get quote by payment id: %w", err)
	}
	q.RawProviderPayload = rawPayload
	return q, true, nil
}

// GetQuotesByPaymentID returns every quote row for paymentID (winning and
// losing), selected-first -- consumed only by the dashboard read path
// (Task 3), never by the execution path. Returns an empty (non-nil)
// slice, not an error, when the payment exists but has no quotes
// (simulated mode, or predates migration 0007).
func (s *Store) GetQuotesByPaymentID(ctx context.Context, paymentID string) ([]payment.Quote, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, payment_id, provider, origin_chain_id, destination_chain_id, asset,
		       input_amount::text, output_amount::text, fee_amount::text, estimated_fill_time_sec,
		       quoted_at, expires_at, raw_provider_payload, created_at, selected
		FROM payment_quotes
		WHERE payment_id = $1
		ORDER BY selected DESC, provider
	`, paymentID)
	if err != nil {
		return nil, fmt.Errorf("get quotes by payment id: %w", err)
	}
	defer rows.Close()

	quotes := []payment.Quote{}
	for rows.Next() {
		var q payment.Quote
		var rawPayload []byte
		if err := rows.Scan(&q.ID, &q.PaymentID, &q.Provider, &q.OriginChainID, &q.DestinationChainID, &q.Asset,
			&q.InputAmount, &q.OutputAmount, &q.FeeAmount, &q.EstimatedFillTimeSec,
			&q.QuotedAt, &q.ExpiresAt, &rawPayload, &q.CreatedAt, &q.Selected); err != nil {
			return nil, fmt.Errorf("scan payment_quotes row: %w", err)
		}
		q.RawProviderPayload = rawPayload
		quotes = append(quotes, q)
	}
	return quotes, rows.Err()
}
```

- [ ] **Step 4: `go-api/internal/postgres/store.go` — update the `CreateOrGetPayment` call site**

Read the current file around line 256 (`if p.Quote != nil { if err := insertPaymentQuote(ctx, tx, created.ID, p.Quote); err != nil { ... } }`) in full context first. Change to:

```go
	if len(p.Quotes) > 0 {
		if err := insertPaymentQuotes(ctx, tx, created.ID, p.Quotes); err != nil {
			return payment.Payment{}, 0, err
		}
	}
```

This stays inside the exact same transaction `CreateOrGetPayment` already runs — no new transaction boundary, no change to what the caller observes on success/failure.

- [ ] **Step 5: `go-api/internal/handler/payments.go` — build `candidate.Quotes` from every fetched provider response**

Read the current file's testnet-mode quote-fetch section and the winning-quote-attachment section (around lines 225-329 and 378-413, per the survey — confirm exact current line numbers before editing) in full. The handler already builds `quotesByBridgeName map[string]quote.Quote` from every provider's successful+available response (this loop is unchanged). Find the block that currently does:
```go
	if mode == payment.ExecutionModeTestnet && len(hops) > 0 {
		winningQuote := quotesByBridgeName[hops[0].BridgeName]
		candidate.Quote = &payment.Quote{
			Provider: winningQuote.ProviderName, ...
		}
	}
```
Replace it with a loop building `candidate.Quotes []payment.Quote` from every entry in `quotesByBridgeName`, not just the winner:
```go
	if mode == payment.ExecutionModeTestnet && len(hops) > 0 {
		winningBridgeName := hops[0].BridgeName
		for bridgeName, q := range quotesByBridgeName {
			candidate.Quotes = append(candidate.Quotes, payment.Quote{
				Provider: q.ProviderName, OriginChainID: q.SourceChainID,
				DestinationChainID: q.DestinationChainID, Asset: q.Asset,
				InputAmount:          q.InputAmountBaseUnits.String(),
				OutputAmount:         q.OutputAmountBaseUnits.String(),
				FeeAmount:            q.FeeBaseUnits.String(),
				EstimatedFillTimeSec: q.EstimatedFillTimeSec,
				QuotedAt:             q.QuotedAt, ExpiresAt: q.ExpiresAt,
				RawProviderPayload: q.RawProviderPayload,
				Selected:            bridgeName == winningBridgeName,
			})
		}
	}
```
Read the exact current field names on the local `winningQuote`/`quote.Quote` type (`quote.Quote`'s fields were confirmed in an earlier phase survey as `ProviderName, SourceChainID, DestinationChainID, Asset, InputAmountBaseUnits, OutputAmountBaseUnits, FeeBaseUnits, EstimatedFillTimeSec, Available, QuotedAt, ExpiresAt, RawProviderPayload` — confirm these match the actual current `go-api/internal/bridge/quote/quote.go` before finalizing this loop). **The C++ router's `FindRoute` call and the `hops[0].BridgeName` determination of the winner are completely unchanged above this block** — this only changes what happens to already-computed, already-in-memory data after the routing decision is made. Map iteration order in Go is randomized, which is fine here since `candidate.Quotes`' order has no semantic meaning (the dashboard sorts by `selected DESC` at read time, per Step 3's `GetQuotesByPaymentID` query) — but if you want deterministic test assertions, note this in your test rather than relying on slice order.

- [ ] **Step 6: Write the failing test proving `GetQuoteByPaymentID`'s post-migration behavior is unchanged**

In `go-api/internal/postgres/quote_store_integration_test.go` (read the existing file first for its current test patterns and fixtures), add:
```go
func TestGetQuoteByPaymentID_ReturnsOnlyTheSelectedQuoteAmongMultiple(t *testing.T) {
	// ... use the existing integration-test fixture pattern to create a
	// payment, then directly insert two payment_quotes rows for it (one
	// across, selected=false; one relay, selected=true) via the store's
	// own insertPaymentQuotes (or a direct test-only SQL insert if that's
	// the file's existing convention -- check first) ...
	got, found, err := store.GetQuoteByPaymentID(ctx, paymentID)
	if err != nil || !found {
		t.Fatalf("GetQuoteByPaymentID: found=%v err=%v", found, err)
	}
	if got.Provider != "relay" || !got.Selected {
		t.Errorf("expected the selected=true relay quote, got provider=%s selected=%v", got.Provider, got.Selected)
	}
}

func TestGetQuotesByPaymentID_ReturnsAllQuotesSelectedFirst(t *testing.T) {
	// same fixture as above
	got, err := store.GetQuotesByPaymentID(ctx, paymentID)
	if err != nil {
		t.Fatalf("GetQuotesByPaymentID: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 quotes, got %d", len(got))
	}
	if !got[0].Selected {
		t.Errorf("expected the selected quote first, got %+v", got[0])
	}
}

func TestGetQuotesByPaymentID_EmptyForSimulatedModePayment(t *testing.T) {
	// create a simulated-mode payment (no quotes) via the existing fixture pattern
	got, err := store.GetQuotesByPaymentID(ctx, simulatedPaymentID)
	if err != nil {
		t.Fatalf("GetQuotesByPaymentID: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected zero quotes for a simulated-mode payment, got %d", len(got))
	}
}
```

- [ ] **Step 7: Update `payments_test.go`'s existing tests referencing the old `candidate.Quote`/`payment.Quote` singular field**

Grep `go-api/internal/handler/payments_test.go` for `.Quote` (singular field access) and update every reference to the new `.Quotes` plural slice, adjusting assertions accordingly (e.g. a test asserting `result.Quote.Provider == "across"` becomes an assertion over `result.Quotes`, finding the entry with `Selected: true`). Add a new test: `TestPostPayments_TestnetMode_PersistsBothWinningAndLosingQuotes` — using the existing two-provider fake-quote-provider setup (from Phase 9's `TestPostPayments_TestnetMode_RecordsQuoteMetricsWithBoundedProviderLabels`-style tests, if that fixture pattern still exists — check `payments_test.go`), assert the persisted payment's `Quotes` slice has exactly 2 entries, exactly one with `Selected: true`, and that the `Selected: true` entry's provider matches the actual C++-router-selected winner (not just "the first one" or "the cheaper one" assumed by construction — verify against `hops[0].BridgeName`).

- [ ] **Step 8: Run the full regression**

```bash
cd go-api && go build ./... && go vet ./... && go test ./...
export DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable"
go test -tags=integration ./...
```
Every existing test must still pass, plus the new ones. Fix any compile error from the `Quote *Quote` → `Quotes []Quote` field rename that ripples into other files (grep the whole `go-api/` tree for `.Quote\b` singular-field usage outside `payment.go`/`quote_store.go`/`payments.go`/`store.go` to catch anything this plan's own survey didn't anticipate — e.g. check `go-api/internal/worker/executor.go`'s own usage of `payment.Quote` the TYPE (not the field) is unaffected, since `Executor` calls `GetQuoteByPaymentID` directly, which still returns a single `payment.Quote` value, not through `Payment.Quotes`).

- [ ] **Step 9: Commit**

```bash
git add go-api/migrations/0007_dashboard_payment_analytics.sql go-api/internal/payment/payment.go go-api/internal/postgres/quote_store.go go-api/internal/postgres/store.go go-api/internal/handler/payments.go go-api/internal/postgres/quote_store_integration_test.go go-api/internal/handler/payments_test.go
git commit -m "feat(go-api): persist every fetched bridge quote, not just the winner, for dashboard comparison"
```

---

### Task 2: CORS middleware + `GET /payments` paginated/filtered list endpoint

**Files:**
- Create: `go-api/internal/handler/cors.go`
- Create: `go-api/internal/handler/dashboard.go`
- Modify: `go-api/internal/postgres/store.go` (new `ListPayments` method)
- Modify: `go-api/cmd/server/main.go` (register the new route + CORS wrapping)
- Test: `go-api/internal/handler/dashboard_test.go`
- Test: `go-api/internal/postgres/store_integration_test.go` (extend)

**Interfaces:**
- Consumes: nothing from Task 1 directly (this task's list endpoint doesn't need quotes).
- Produces: `DashboardHandler` type (or extend the existing `Handler` — see Step 2) with `ListPayments` — consumed by Task 3's remaining endpoints, which should live on the same handler type for a single, cohesive dashboard API surface.

- [ ] **Step 1: `go-api/internal/handler/cors.go`**

```go
package handler

import (
	"net/http"
	"strings"
)

// CORS is a minimal, explicit middleware -- this API has no cookies/
// credentials anywhere, so the need is narrow: allow GET/POST from a
// configured origin list, never "*", never a blanket credentialed
// reflect-any-origin configuration.
func CORS(allowedOrigins []string, next http.Handler) http.Handler {
	allowed := make(map[string]bool, len(allowedOrigins))
	for _, o := range allowedOrigins {
		allowed[strings.TrimSpace(o)] = true
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && allowed[origin] {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Idempotency-Key")
			w.Header().Set("Vary", "Origin")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
```

- [ ] **Step 2: Add a `DashboardStore` interface and `paymentListItem`/`paymentListResponse` types to `go-api/internal/handler/dashboard.go`**

Read `go-api/internal/handler/routes.go`'s `Handler` struct definition first (this task adds to the SAME `Handler` struct, following the file's existing pattern of composing narrow interfaces as separate fields — do not create a second, parallel `DashboardHandler` type, since `main.go` already constructs one `Handler` wired with everything):

```go
package handler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"chainroute/go-api/internal/payment"
)

// DashboardStore is the subset of *postgres.Store the read-only
// dashboard endpoints need -- kept narrow and separate from PaymentStore
// (which backs the write-adjacent POST /payments and GET /payments/{id}
// paths) per this package's existing per-handler-interface convention.
type DashboardStore interface {
	ListPayments(ctx context.Context, filter payment.ListFilter) ([]payment.Payment, string, error)
}

type paymentListItem struct {
	ID               string  `json:"id"`
	SourceChain      string  `json:"source_chain"`
	DestinationChain string  `json:"destination_chain"`
	Asset            string  `json:"asset"`
	Amount           string  `json:"amount"`
	Status           string  `json:"status"`
	ExecutionMode    string  `json:"execution_mode"`
	BridgeProvider   *string `json:"bridge_provider"`
	TotalFee         float64 `json:"total_fee"`
	CreatedAt        string  `json:"created_at"`
}

type paymentListResponse struct {
	Payments   []paymentListItem `json:"payments"`
	NextCursor *string           `json:"next_cursor"`
}

const (
	defaultListLimit = 25
	maxListLimit     = 100
)

func (h *Handler) ListPayments(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	limit := defaultListLimit
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		limit = n
		if limit > maxListLimit {
			limit = maxListLimit
		}
	}

	filter := payment.ListFilter{Limit: limit, Cursor: q.Get("cursor")}

	if s := q.Get("status"); s != "" {
		if !isValidStatus(s) {
			writeError(w, http.StatusBadRequest, "invalid status: "+s)
			return
		}
		filter.Status = &s
	}
	if p := q.Get("provider"); p != "" {
		filter.Provider = &p
	}
	if sc := q.Get("source_chain"); sc != "" {
		filter.SourceChain = &sc
	}
	if dc := q.Get("destination_chain"); dc != "" {
		filter.DestinationChain = &dc
	}
	if em := q.Get("execution_mode"); em != "" {
		if em != string(payment.ExecutionModeSimulated) && em != string(payment.ExecutionModeTestnet) {
			writeError(w, http.StatusBadRequest, "execution_mode must be \"simulated\" or \"testnet\"")
			return
		}
		filter.ExecutionMode = &em
	}

	payments, nextCursor, err := h.dashboardStore().ListPayments(r.Context(), filter)
	if err != nil {
		h.logger().ErrorContext(r.Context(), "failed to list payments", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	items := make([]paymentListItem, 0, len(payments))
	for _, p := range payments {
		items = append(items, paymentListItem{
			ID: p.ID, SourceChain: p.SourceChain, DestinationChain: p.DestinationChain,
			Asset: p.Asset, Amount: p.Amount, Status: string(p.Status),
			ExecutionMode: string(p.ExecutionMode), BridgeProvider: p.BridgeProvider,
			TotalFee: p.TotalFee, CreatedAt: p.CreatedAt.UTC().Format(timeFormat),
		})
	}
	resp := paymentListResponse{Payments: items}
	if nextCursor != "" {
		resp.NextCursor = &nextCursor
	}
	writeJSON(w, http.StatusOK, resp)
}

func isValidStatus(s string) bool {
	switch payment.Status(s) {
	case payment.StatusRouted, payment.StatusProcessing, payment.StatusSubmitted, payment.StatusCompleted, payment.StatusFailed:
		return true
	}
	return false
}

// encodeCursor/decodeCursor implement opaque keyset-pagination cursors
// (base64 of "createdAtRFC3339Nano,id") -- read by ListPayments' SQL
// WHERE clause, never interpreted by the client.
func encodeCursor(createdAt, id string) string {
	return base64.URLEncoding.EncodeToString([]byte(createdAt + "," + id))
}

func decodeCursor(cursor string) (createdAt, id string, err error) {
	raw, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return "", "", fmt.Errorf("invalid cursor: %w", err)
	}
	parts := strings.SplitN(string(raw), ",", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid cursor format")
	}
	return parts[0], parts[1], nil
}
```

Read the CURRENT `go-api/internal/handler/payments.go` for its exact `writeError`/`writeJSON`/`timeFormat`-or-equivalent helpers (the survey confirmed `CreatedAt` is formatted via `time.RFC3339Nano` inline — check whether a shared `const timeFormat = time.RFC3339Nano` already exists somewhere, or whether you need to reference `time.RFC3339Nano` directly instead of a `timeFormat` constant that doesn't actually exist yet in this codebase — adapt the sketch above to match reality rather than inventing a constant name that doesn't exist).

Add `Metrics`/`Logger`/`DashboardStore` field(s) to the `Handler` struct in `routes.go` if `DashboardStore` isn't already composed in — add a new `DashboardStore DashboardStore` field alongside the existing `Client`/`Store`/`Metrics`/`Logger` fields, and a `h.dashboardStore()` nil-safe accessor is NOT needed the way `metrics()`/`logger()` are (unlike those, a nil `DashboardStore` would be a genuine wiring bug in `main.go`, not something with a safe fallback — a nil-pointer panic on a mis-wired server is the correct, loud failure mode here, not a silent no-op).

- [ ] **Step 3: `payment.ListFilter` type**

Add to `go-api/internal/payment/payment.go`:
```go
// ListFilter narrows a ListPayments query. All fields are optional
// (nil/zero means "no filter on this dimension"); Limit and Cursor
// govern keyset pagination.
type ListFilter struct {
	Limit            int
	Cursor           string
	Status           *string
	Provider         *string
	SourceChain      *string
	DestinationChain *string
	ExecutionMode    *string
}
```

- [ ] **Step 4: `postgres.Store.ListPayments`**

Add to `go-api/internal/postgres/store.go` (read the file's existing query-building conventions first — e.g. how `hopsForPayment` or other multi-row-scan methods are structured, to match style):

```go
func (s *Store) ListPayments(ctx context.Context, filter payment.ListFilter) ([]payment.Payment, string, error) {
	ctx, span := observability.Tracer("db").Start(ctx, "db.ListPayments")
	defer span.End()

	query := `
		SELECT id, source_chain, destination_chain, asset, amount, status,
		       total_fee, execution_mode, bridge_provider, created_at
		FROM payments
		WHERE 1=1
	`
	args := []any{}
	argN := 0
	nextArg := func(v any) string {
		argN++
		args = append(args, v)
		return fmt.Sprintf("$%d", argN)
	}

	if filter.Cursor != "" {
		createdAt, id, err := decodeCursorForQuery(filter.Cursor)
		if err != nil {
			return nil, "", err
		}
		query += fmt.Sprintf(" AND (created_at, id) < (%s, %s)", nextArg(createdAt), nextArg(id))
	}
	if filter.Status != nil {
		query += fmt.Sprintf(" AND status = %s", nextArg(*filter.Status))
	}
	if filter.Provider != nil {
		query += fmt.Sprintf(" AND bridge_provider = %s", nextArg(*filter.Provider))
	}
	if filter.SourceChain != nil {
		query += fmt.Sprintf(" AND source_chain = %s", nextArg(*filter.SourceChain))
	}
	if filter.DestinationChain != nil {
		query += fmt.Sprintf(" AND destination_chain = %s", nextArg(*filter.DestinationChain))
	}
	if filter.ExecutionMode != nil {
		query += fmt.Sprintf(" AND execution_mode = %s", nextArg(*filter.ExecutionMode))
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = 25
	}
	query += fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT %s", nextArg(limit+1))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("list payments: %w", err)
	}
	defer rows.Close()

	var results []payment.Payment
	for rows.Next() {
		var p payment.Payment
		var bridgeProvider sql.NullString
		if err := rows.Scan(&p.ID, &p.SourceChain, &p.DestinationChain, &p.Asset, &p.Amount,
			&p.Status, &p.TotalFee, &p.ExecutionMode, &bridgeProvider, &p.CreatedAt); err != nil {
			return nil, "", fmt.Errorf("scan payment row: %w", err)
		}
		if bridgeProvider.Valid {
			p.BridgeProvider = &bridgeProvider.String
		}
		results = append(results, p)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	nextCursor := ""
	if len(results) > limit {
		last := results[limit-1]
		nextCursor = encodeCursor(last.CreatedAt.UTC().Format(time.RFC3339Nano), last.ID)
		results = results[:limit]
	}
	return results, nextCursor, nil
}
```

**Note on the `%s`-built query with parameterized VALUES**: every `nextArg(...)` call still returns a `$N` PLACEHOLDER string interpolated into the query text, with the actual value appended to `args` and passed through `QueryContext(ctx, query, args...)` — this is standard, safe parameterized-query construction (the query TEXT is built dynamically for the optional WHERE clauses, but every actual VALUE flows through `$N` placeholders, never string-concatenated into the SQL itself). Confirm this pattern is genuinely injection-safe by reading it carefully before shipping — `filter.Status`/`filter.Provider`/etc. are never concatenated as literal strings into `query`, only their `$N` placeholder position is.

Add a package-level `decodeCursorForQuery` in `store.go` (or reuse `handler.decodeCursor` if you prefer keeping cursor encoding/decoding in one place — since `postgres` shouldn't import `handler`, duplicate the small decode function in `postgres/store.go` with its own name, or better, move `encodeCursor`/`decodeCursor` into the shared `payment` package if both `handler` and `postgres` need them — read both files' existing import graphs before deciding; the `payment` package is already imported by both, making it the natural shared home).

Add `"time"` and `"database/sql"` imports if not already present in `store.go`.

- [ ] **Step 5: Register the route and wrap with CORS in `go-api/cmd/server/main.go`**

Read the current file's `mux := http.NewServeMux()` block in full. Add:
```go
mux.HandleFunc("GET /payments", h.ListPayments)
```
And wrap the whole mux (before the existing `otelhttp.NewHandler` wrapping, so CORS headers apply to every route including `/metrics`) with the new CORS middleware:
```go
corsOrigins := strings.Split(envOrDefaultServer("CHAINROUTE_CORS_ALLOWED_ORIGINS", "http://localhost:5173"), ",")
corsHandler := handler.CORS(corsOrigins, mux)
instrumentedMux := otelhttp.NewHandler(corsHandler, "http.server", otelhttp.WithSpanNameFormatter(spanNameFormatter))
```
Add `"strings"` to imports if not already present. Thread `DashboardStore: store` into the `Handler{}` literal (the concrete `*postgres.Store` satisfies the new narrow `DashboardStore` interface structurally, same as it already does for `PaymentStore`).

- [ ] **Step 6: Tests**

`go-api/internal/handler/dashboard_test.go`: a `fakeDashboardStore` (mirroring `fakePaymentStore`'s canned-response-plus-capture-field convention) with tests for: default limit applied when `limit` is omitted; `limit` clamped to `maxListLimit` when a larger value is requested; an invalid `status` filter value returns 400; a valid request returns the expected JSON shape; `next_cursor` is `null`/omitted when fewer than `limit+1` rows exist, present when more do.

`go-api/internal/postgres/store_integration_test.go`: seed several real payments with distinct `created_at`/`status`/`execution_mode` values via the existing integration-test fixture pattern, then test `ListPayments` genuinely paginates correctly across two calls (first call's `nextCursor` fed into a second call returns the next page with no overlap/gap), and that each filter dimension genuinely narrows results.

- [ ] **Step 7: Run the full regression**

```bash
cd go-api && go build ./... && go vet ./... && go test ./...
export DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable"
go test -tags=integration ./...
```

- [ ] **Step 8: Commit**

```bash
git add go-api/internal/handler/cors.go go-api/internal/handler/dashboard.go go-api/internal/handler/dashboard_test.go go-api/internal/handler/routes.go go-api/internal/payment/payment.go go-api/internal/postgres/store.go go-api/internal/postgres/store_integration_test.go go-api/cmd/server/main.go
git commit -m "feat(go-api): add CORS middleware and paginated/filtered GET /payments"
```

---

### Task 3: `GET /payments/{id}/quotes` and `GET /dashboard/stats`

**Files:**
- Modify: `go-api/internal/handler/dashboard.go`
- Modify: `go-api/internal/postgres/store.go` (new `GetDashboardStats` method)
- Modify: `go-api/cmd/server/main.go` (register 2 routes)
- Test: `go-api/internal/handler/dashboard_test.go` (extend)
- Test: `go-api/internal/postgres/store_integration_test.go` (extend)

**Interfaces:**
- Consumes: Task 1's `GetQuotesByPaymentID`.
- Produces: nothing further consumed by later tasks within this backend track (Task 4 is independent).

- [ ] **Step 1: `GET /payments/{id}/quotes` handler**

Add to `dashboard.go`:
```go
type quoteItem struct {
	Provider             string `json:"provider"`
	InputAmount          string `json:"input_amount"`
	OutputAmount         string `json:"output_amount"`
	FeeAmount            string `json:"fee_amount"`
	EstimatedFillTimeSec int64  `json:"estimated_fill_time_sec"`
	Selected             bool   `json:"selected"`
	QuotedAt             string `json:"quoted_at"`
}
type paymentQuotesResponse struct {
	Quotes []quoteItem `json:"quotes"`
}

func (h *Handler) GetPaymentQuotes(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	_, found, err := h.Store.GetPayment(r.Context(), id)
	if err != nil {
		h.logger().ErrorContext(r.Context(), "failed to read payment for quotes lookup", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "payment not found")
		return
	}
	quotes, err := h.dashboardStore().GetQuotesByPaymentID(r.Context(), id)
	if err != nil {
		h.logger().ErrorContext(r.Context(), "failed to read payment quotes", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	items := make([]quoteItem, 0, len(quotes))
	for _, q := range quotes {
		items = append(items, quoteItem{
			Provider: q.Provider, InputAmount: q.InputAmount, OutputAmount: q.OutputAmount,
			FeeAmount: q.FeeAmount, EstimatedFillTimeSec: q.EstimatedFillTimeSec,
			Selected: q.Selected, QuotedAt: q.QuotedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	writeJSON(w, http.StatusOK, paymentQuotesResponse{Quotes: items})
}
```
Extend the `DashboardStore` interface (added in Task 2) with `GetQuotesByPaymentID(ctx context.Context, paymentID string) ([]payment.Quote, error)`.

- [ ] **Step 2: `GET /dashboard/stats` handler**

```go
type dashboardStats struct {
	TotalPayments      int64            `json:"total_payments"`
	CompletedPayments  int64            `json:"completed_payments"`
	ProcessingPayments int64            `json:"processing_payments"`
	FailedPayments     int64            `json:"failed_payments"`
	ProviderUsage      map[string]int64 `json:"provider_usage"`
	AverageRoutingCost float64          `json:"average_routing_cost"`
}

func (h *Handler) GetDashboardStats(w http.ResponseWriter, r *http.Request) {
	stats, err := h.dashboardStore().GetDashboardStats(r.Context())
	if err != nil {
		h.logger().ErrorContext(r.Context(), "failed to read dashboard stats", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, stats)
}
```
Extend `DashboardStore` with `GetDashboardStats(ctx context.Context) (payment.DashboardStats, error)` (define `payment.DashboardStats` in `payment.go` with the same shape as `dashboardStats` above, following the existing convention where the `payment` package holds domain types and `handler` has its own thin `json`-tagged response-shape structs — check whether other handlers convert a domain type to a response type this way, or whether the domain type itself carries `json` tags already, and match whichever convention `payments.go` already established).

- [ ] **Step 3: `postgres.Store.GetDashboardStats`**

```go
func (s *Store) GetDashboardStats(ctx context.Context) (payment.DashboardStats, error) {
	var stats payment.DashboardStats
	stats.ProviderUsage = map[string]int64{}

	row := s.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*) AS total,
			COUNT(*) FILTER (WHERE status = 'COMPLETED') AS completed,
			COUNT(*) FILTER (WHERE status IN ('ROUTED', 'PROCESSING', 'SUBMITTED')) AS processing,
			COUNT(*) FILTER (WHERE status = 'FAILED') AS failed,
			COALESCE(AVG(total_fee), 0) AS avg_fee
		FROM payments
	`)
	if err := row.Scan(&stats.TotalPayments, &stats.CompletedPayments, &stats.ProcessingPayments, &stats.FailedPayments, &stats.AverageRoutingCost); err != nil {
		return payment.DashboardStats{}, fmt.Errorf("get dashboard stats: %w", err)
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT bridge_provider, COUNT(*)
		FROM payments
		WHERE bridge_provider IS NOT NULL
		GROUP BY bridge_provider
	`)
	if err != nil {
		return payment.DashboardStats{}, fmt.Errorf("get provider usage: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var provider string
		var count int64
		if err := rows.Scan(&provider, &count); err != nil {
			return payment.DashboardStats{}, fmt.Errorf("scan provider usage row: %w", err)
		}
		stats.ProviderUsage[provider] = count
	}
	return stats, rows.Err()
}
```
Exactly two queries, both aggregating in SQL — no per-row fetch-then-aggregate-in-Go.

- [ ] **Step 4: Register both routes in `cmd/server/main.go`**

```go
mux.HandleFunc("GET /payments/{id}/quotes", h.GetPaymentQuotes)
mux.HandleFunc("GET /dashboard/stats", h.GetDashboardStats)
```

- [ ] **Step 5: Tests**

`dashboard_test.go`: `GetPaymentQuotes` returns 404 for a nonexistent payment ID; returns `quotes: []` (not `null`) for a payment with no quotes; returns the expected shape with `selected` correctly reflected for a payment with quotes. `GetDashboardStats` returns the expected shape from a canned fake store response.

`store_integration_test.go`: seed a real mix of payments (varying status/execution_mode/bridge_provider) and confirm `GetDashboardStats`'s counts and provider-usage map are exactly correct against what was seeded — this is the one place a subtly wrong `FILTER`/`GROUP BY` clause would be caught.

- [ ] **Step 6: Run the full regression** (same commands as Task 2 Step 7).

- [ ] **Step 7: Commit**

```bash
git add go-api/internal/handler/dashboard.go go-api/internal/handler/dashboard_test.go go-api/internal/payment/payment.go go-api/internal/postgres/store.go go-api/internal/postgres/store_integration_test.go go-api/cmd/server/main.go
git commit -m "feat(go-api): add GET /payments/{id}/quotes and GET /dashboard/stats"
```

---

### Task 4: `GET /dashboard/timeseries`

**Files:**
- Modify: `go-api/internal/handler/dashboard.go`
- Modify: `go-api/internal/postgres/store.go` (new `GetTimeseries` method)
- Modify: `go-api/cmd/server/main.go` (register 1 route)
- Test: `go-api/internal/handler/dashboard_test.go` (extend)
- Test: `go-api/internal/postgres/store_integration_test.go` (extend)

**Interfaces:** none beyond what Tasks 2-3 already established.

- [ ] **Step 1: Handler**

```go
type timeseriesPoint struct {
	Bucket string  `json:"bucket"`
	Value  float64 `json:"value"`
	Count  int64   `json:"count"`
}
type timeseriesResponse struct {
	Metric string             `json:"metric"`
	Points []timeseriesPoint  `json:"points"`
}

var validTimeseriesMetrics = map[string]bool{"volume": true, "routing_cost": true}
var validTimeseriesIntervals = map[string]bool{"hour": true, "day": true}

func (h *Handler) GetDashboardTimeseries(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	metric := q.Get("metric")
	if metric == "" {
		metric = "volume"
	}
	if !validTimeseriesMetrics[metric] {
		writeError(w, http.StatusBadRequest, "metric must be \"volume\" or \"routing_cost\"")
		return
	}
	interval := q.Get("interval")
	if interval == "" {
		interval = "day"
	}
	if !validTimeseriesIntervals[interval] {
		writeError(w, http.StatusBadRequest, "interval must be \"hour\" or \"day\"")
		return
	}
	days := 30
	if raw := q.Get("days"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "days must be a positive integer")
			return
		}
		days = n
		if days > 90 {
			days = 90
		}
	}

	points, err := h.dashboardStore().GetTimeseries(r.Context(), metric, interval, days)
	if err != nil {
		h.logger().ErrorContext(r.Context(), "failed to read dashboard timeseries", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	items := make([]timeseriesPoint, 0, len(points))
	for _, p := range points {
		items = append(items, timeseriesPoint{Bucket: p.Bucket.UTC().Format(time.RFC3339), Value: p.Value, Count: p.Count})
	}
	writeJSON(w, http.StatusOK, timeseriesResponse{Metric: metric, Points: items})
}
```
Extend `DashboardStore` with `GetTimeseries(ctx context.Context, metric, interval string, days int) ([]payment.TimeseriesPoint, error)`; define `payment.TimeseriesPoint{Bucket time.Time; Value float64; Count int64}` in `payment.go`.

- [ ] **Step 2: `postgres.Store.GetTimeseries`**

```go
func (s *Store) GetTimeseries(ctx context.Context, metric, interval string, days int) ([]payment.TimeseriesPoint, error) {
	aggregateExpr := "COUNT(*)"
	if metric == "routing_cost" {
		aggregateExpr = "COALESCE(AVG(total_fee), 0)"
	}
	query := fmt.Sprintf(`
		SELECT date_trunc($1, created_at) AS bucket, COUNT(*), %s
		FROM payments
		WHERE created_at > now() - ($2 || ' days')::interval
		GROUP BY bucket
		ORDER BY bucket
	`, aggregateExpr)

	rows, err := s.db.QueryContext(ctx, query, interval, days)
	if err != nil {
		return nil, fmt.Errorf("get timeseries: %w", err)
	}
	defer rows.Close()

	points := []payment.TimeseriesPoint{}
	for rows.Next() {
		var p payment.TimeseriesPoint
		if err := rows.Scan(&p.Bucket, &p.Count, &p.Value); err != nil {
			return nil, fmt.Errorf("scan timeseries row: %w", err)
		}
		points = append(points, p)
	}
	return points, rows.Err()
}
```
**Note**: `aggregateExpr` is interpolated into the query TEXT via `fmt.Sprintf`, but it is chosen from a fixed, code-controlled 2-value set (`"COUNT(*)"` or `"COALESCE(AVG(total_fee), 0)"`) based on the ALREADY-VALIDATED `metric` string (the handler rejects anything outside `{"volume","routing_cost"}` before this function is ever called) — never derived from unvalidated user input directly. `interval`/`days` (the actual user-influenced values) flow through `$1`/`$2` placeholders, not string interpolation. This is safe, but read it carefully and confirm the handler's validation genuinely runs before this is reachable (it does, per Step 1 above) rather than assuming.

- [ ] **Step 3: Register the route**

```go
mux.HandleFunc("GET /dashboard/timeseries", h.GetDashboardTimeseries)
```

- [ ] **Step 4: Tests**

`dashboard_test.go`: invalid `metric`/`interval`/`days` values return 400; defaults apply when omitted; `days` clamped to 90.

`store_integration_test.go`: seed payments with distinct `created_at` values spanning multiple days, confirm `GetTimeseries("volume", "day", 30)` buckets counts correctly, and `GetTimeseries("routing_cost", "day", 30)` computes the correct average per bucket against known seeded `total_fee` values.

- [ ] **Step 5: Run the full regression** (same commands as before).

- [ ] **Step 6: Commit**

```bash
git add go-api/internal/handler/dashboard.go go-api/internal/handler/dashboard_test.go go-api/internal/payment/payment.go go-api/internal/postgres/store.go go-api/internal/postgres/store_integration_test.go go-api/cmd/server/main.go
git commit -m "feat(go-api): add GET /dashboard/timeseries"
```

---

### Task 5: Frontend scaffold

**Files:**
- Create: `frontend/package.json`, `vite.config.ts`, `tsconfig.json`, `tsconfig.node.json`, `tailwind.config.js`, `postcss.config.js`, `index.html`, `.eslintrc.cjs`, `.gitignore`
- Create: `frontend/src/main.tsx`, `App.tsx`, `router.tsx`
- Create: `frontend/src/api/client.ts`, `frontend/src/api/types.ts`
- Create: `frontend/src/lib/chains.ts`, `frontend/src/lib/format.ts`
- Create: `frontend/public/env-config.js`

**Interfaces:**
- Produces: `apiGet<T>(path: string, params?: Record<string, string>): Promise<T>` (the typed fetch wrapper every later frontend task uses), the full TypeScript type set matching the Go JSON shapes from Tasks 1-4, `CHAIN_DISPLAY_NAMES`/`PROVIDER_DISPLAY_NAMES` lookup maps, `formatBaseUnits`/`formatTimestamp` helpers — consumed by every subsequent frontend task.

- [ ] **Step 1: Scaffold via Vite's own generator, then adapt**

```bash
cd /path/to/repo/root
npm create vite@latest frontend -- --template react-ts
cd frontend
npm install
npm install -D tailwindcss postcss autoprefixer @tanstack/react-query recharts react-router-dom
npm install -D vitest @testing-library/react @testing-library/jest-dom jsdom
npx tailwindcss init -p
```
(Exact package versions: let `npm install` resolve latest-compatible; do not pin to specific old versions unless a real compatibility problem is hit — this is a brand-new project with no existing lockfile to match.)

- [ ] **Step 2: `frontend/src/api/types.ts`** — every type matching the Go JSON shapes byte-for-byte (field names, optionality) from Tasks 1-4's actual shipped code (re-read those files' final committed shapes before transcribing, since this plan's own sketches may have drifted slightly during implementation):

```typescript
export type PaymentStatus = "ROUTED" | "PROCESSING" | "SUBMITTED" | "COMPLETED" | "FAILED";
export type ExecutionMode = "simulated" | "testnet";

export interface Hop {
  hop_index: number;
  from_chain: string;
  to_chain: string;
  bridge_name: string;
  fee: number;
  latency_ms: number;
  liquidity: number;
  reliability: number;
}

export interface Payment {
  id: string;
  source_chain: string;
  destination_chain: string;
  asset: string;
  amount: string;
  status: PaymentStatus;
  total_fee: number;
  hops: Hop[];
  execution_mode: ExecutionMode;
  bridge_provider: string | null;
  failure_reason: string | null;
  external_tx_hash: string | null;
  submitted_at: string | null;
  created_at: string;
  updated_at: string;
  completed_at: string | null;
}

export interface PaymentListItem {
  id: string;
  source_chain: string;
  destination_chain: string;
  asset: string;
  amount: string;
  status: PaymentStatus;
  execution_mode: ExecutionMode;
  bridge_provider: string | null;
  total_fee: number;
  created_at: string;
}

export interface PaymentListResponse {
  payments: PaymentListItem[];
  next_cursor: string | null;
}

export interface PaymentListFilters {
  status?: PaymentStatus;
  provider?: string;
  source_chain?: string;
  destination_chain?: string;
  execution_mode?: ExecutionMode;
  cursor?: string;
  limit?: number;
}

export interface Quote {
  provider: string;
  input_amount: string;
  output_amount: string;
  fee_amount: string;
  estimated_fill_time_sec: number;
  selected: boolean;
  quoted_at: string;
}

export interface PaymentQuotesResponse {
  quotes: Quote[];
}

export interface DashboardStats {
  total_payments: number;
  completed_payments: number;
  processing_payments: number;
  failed_payments: number;
  provider_usage: Record<string, number>;
  average_routing_cost: number;
}

export interface TimeseriesPoint {
  bucket: string;
  value: number;
  count: number;
}

export interface TimeseriesResponse {
  metric: "volume" | "routing_cost";
  points: TimeseriesPoint[];
}
```

- [ ] **Step 3: `frontend/src/api/client.ts`**

```typescript
declare global {
  interface Window {
    __CHAINROUTE_API_BASE_URL__?: string;
  }
}

function apiBaseUrl(): string {
  if (typeof window !== "undefined" && window.__CHAINROUTE_API_BASE_URL__) {
    return window.__CHAINROUTE_API_BASE_URL__;
  }
  return import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8080";
}

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
    this.name = "ApiError";
  }
}

export async function apiGet<T>(path: string, params?: Record<string, string | number | undefined>): Promise<T> {
  const url = new URL(apiBaseUrl() + path);
  if (params) {
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined) url.searchParams.set(key, String(value));
    }
  }
  const res = await fetch(url.toString());
  if (!res.ok) {
    let message = res.statusText;
    try {
      const body = await res.json();
      if (body?.error) message = body.error;
    } catch {
      // ignore -- fall back to statusText
    }
    throw new ApiError(res.status, message);
  }
  return res.json() as Promise<T>;
}
```

- [ ] **Step 4: `frontend/src/lib/chains.ts`**

```typescript
export const SIMULATED_GRAPH_CHAINS = ["ethereum", "base", "arbitrum", "optimism", "polygon"] as const;

export const CHAIN_DISPLAY_NAMES: Record<string, string> = {
  ethereum: "Ethereum",
  base: "Base",
  arbitrum: "Arbitrum",
  optimism: "Optimism",
  polygon: "Polygon",
};

export const PROVIDER_DISPLAY_NAMES: Record<string, string> = {
  across: "Across",
  relay: "Relay",
};

// The one real corridor with signed testnet execution behind it, per the
// backend's own testnetChainIDByChain map (handler/routes.go) -- WETH
// only, Ethereum Sepolia <-> Base Sepolia. Kept in exactly one place so a
// future backend change to supported corridors has exactly one frontend
// constant to update, and so this is never re-derived from chain-name
// pattern matching anywhere else in the UI.
export function hasRealTestnetExecution(sourceChain: string, destChain: string, asset: string, executionMode: string): boolean {
  if (executionMode !== "testnet") return false;
  const pair = [sourceChain.toLowerCase(), destChain.toLowerCase()].sort().join("-");
  return pair === "base-ethereum" && asset.toLowerCase() === "eth";
}
```

- [ ] **Step 5: `frontend/src/lib/format.ts`**

```typescript
export function formatTimestamp(iso: string): string {
  return new Date(iso).toLocaleString(undefined, {
    dateStyle: "medium",
    timeStyle: "medium",
  });
}

export function formatRelativeTime(iso: string): string {
  const diffMs = Date.now() - new Date(iso).getTime();
  const diffSec = Math.floor(diffMs / 1000);
  if (diffSec < 60) return `${diffSec}s ago`;
  if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m ago`;
  if (diffSec < 86400) return `${Math.floor(diffSec / 3600)}h ago`;
  return `${Math.floor(diffSec / 86400)}d ago`;
}

// Quote amounts are base-units integer decimal strings (e.g. WETH has 18
// decimals) -- this is a DISPLAY approximation for the dashboard only,
// never used for anything that needs exact precision (that stays
// server-side). decimals defaults to 18 (WETH/ETH), the only asset with
// real testnet quote data today per the backend survey.
export function formatBaseUnits(baseUnits: string, decimals = 18): string {
  const value = BigInt(baseUnits);
  const divisor = BigInt(10) ** BigInt(decimals);
  const whole = value / divisor;
  const fraction = value % divisor;
  const fractionStr = fraction.toString().padStart(decimals, "0").slice(0, 6).replace(/0+$/, "");
  return fractionStr ? `${whole}.${fractionStr}` : whole.toString();
}
```

- [ ] **Step 6: Tailwind setup, `frontend/tailwind.config.js`**

A restrained, ChainRoute-specific palette (not a default indigo/purple SaaS template) — e.g. a slate/neutral base with one accent color used sparingly for the selected-route/selected-provider highlight, and semantic status colors (green/amber/red/blue matching COMPLETED/PROCESSING-family/FAILED/informational) reused consistently everywhere a status appears. Configure `content: ["./index.html", "./src/**/*.{ts,tsx}"]`.

- [ ] **Step 7: `frontend/public/env-config.js`** (committed as a placeholder/default; overwritten at container start by Task 13's Docker entrypoint)

```javascript
window.__CHAINROUTE_API_BASE_URL__ = "http://localhost:8080";
```
Referenced in `index.html` via a `<script src="/env-config.js"></script>` tag BEFORE the main Vite bundle script tag.

- [ ] **Step 8: React Query provider + router skeleton in `App.tsx`/`main.tsx`/`router.tsx`**

Set up `QueryClientProvider` (default `refetchIntervalInBackground: false`, per the design's real-time-strategy decision), `BrowserRouter` with 3 placeholder routes (`/` → a stub `OverviewPage`, `/payments` → a stub `PaymentExplorerPage`, `/payments/:id` → a stub `PaymentDetailPage`) that later tasks fill in — this task's own scope is the scaffold compiling and rendering an empty shell, not the pages' real content.

- [ ] **Step 9: Verify the scaffold**

```bash
cd frontend
npm run build
npx tsc --noEmit
npx eslint src --ext .ts,.tsx
```
All three must succeed cleanly on the empty/stub pages before any later task adds real content on top.

- [ ] **Step 10: Commit**

```bash
git add frontend
git commit -m "feat(frontend): scaffold Vite + React + TypeScript + Tailwind + React Query + Recharts"
```

---

### Task 6: Shared UI primitives

**Files:**
- Create: `frontend/src/components/StatusBadge.tsx`
- Create: `frontend/src/components/LoadingState.tsx`, `EmptyState.tsx`, `ErrorState.tsx`
- Create: `frontend/src/components/Pagination.tsx`
- Create: `frontend/src/components/FilterBar.tsx`
- Test: one `.test.tsx` per component

**Interfaces:**
- Produces: `<StatusBadge status={...} />`, `<LoadingState />`, `<EmptyState message={...} />`, `<ErrorState error={...} />`, `<Pagination hasNext={...} onNext={...} onPrevious={...} />`, `<FilterBar filters={...} onChange={...} />` — consumed by every later page/component task.

- [ ] **Step 1: `StatusBadge.tsx`**

Maps each `PaymentStatus` to a color + label using the palette Task 5 established: `ROUTED`/`PROCESSING`/`SUBMITTED` → an "active" family (e.g. blue/amber, distinguishing in-flight sub-states with a subtitle or icon rather than identical treatment — the spec asks the UI to "clearly distinguish completed / active / pending / failed stages"), `COMPLETED` → green, `FAILED` → red. Export a small pure function `statusColor(status: PaymentStatus): string` separately from the component so Task 8/9's route/timeline visualizations can reuse the same color mapping rather than re-deriving it.

- [ ] **Step 2: `LoadingState.tsx`/`EmptyState.tsx`/`ErrorState.tsx`**

Small, reusable, no business logic — `LoadingState` (a simple, non-distracting spinner/skeleton, not a flashy animation per the "avoid excessive animations" constraint), `EmptyState({ message, icon? })`, `ErrorState({ error })` rendering `ApiError`'s message with a retry affordance if a `onRetry` callback is passed.

- [ ] **Step 3: `Pagination.tsx`**

Simple Previous/Next controls driven by the keyset-cursor pattern (no page-number display, since keyset pagination doesn't support "jump to page N" — a Previous/Next-only control is the correct, honest UI for this pagination style, not a limitation to work around).

- [ ] **Step 4: `FilterBar.tsx`**

Dropdowns/selects for `status`, `provider`, `source_chain`, `destination_chain`, `execution_mode`, each populated from the fixed known-value sets (`payment.Status` values, `across`/`relay`, the 5 `SIMULATED_GRAPH_CHAINS`, `simulated`/`testnet`) rather than free-text input — every filter value the user can pick is guaranteed valid against the backend's own validation from Task 2, so a filtered request never gets a 400.

- [ ] **Step 5: Tests** — Vitest + React Testing Library, one test file per component, covering: `StatusBadge` renders the right label/color per status; `EmptyState`/`ErrorState` render their message; `Pagination`'s Next button is disabled when `hasNext` is false; `FilterBar`'s `onChange` fires with the right filter key/value on selection.

- [ ] **Step 6: Verify**

```bash
cd frontend && npx tsc --noEmit && npx eslint src --ext .ts,.tsx && npx vitest run && npm run build
```

- [ ] **Step 7: Commit**

```bash
git add frontend/src/components
git commit -m "feat(frontend): add shared UI primitives (status badge, loading/empty/error states, pagination, filters)"
```

---

### Task 7: Payment Explorer page

**Files:**
- Create: `frontend/src/pages/PaymentExplorerPage.tsx`
- Create: `frontend/src/components/PaymentTable.tsx`
- Create: `frontend/src/hooks/usePaymentList.ts`
- Test: `PaymentTable.test.tsx`

**Interfaces:**
- Consumes: Task 5's `apiGet`/types, Task 6's `StatusBadge`/`Pagination`/`FilterBar`/`LoadingState`/`EmptyState`/`ErrorState`.

- [ ] **Step 1: `usePaymentList.ts`** — a TanStack Query hook wrapping `apiGet<PaymentListResponse>("/payments", filters)`, `refetchInterval: 10_000`, keyed on the current filter state + cursor so changing a filter or paging resets/refetches correctly.

- [ ] **Step 2: `PaymentTable.tsx`** — renders `PaymentListItem[]` as a table with columns matching §3's exact field list (payment ID truncated with a full-ID tooltip, source→destination chain, asset, amount, provider, routing cost, status via `StatusBadge`, execution mode, created time via `formatRelativeTime` with a full-timestamp tooltip), each row linking to `/payments/:id`.

- [ ] **Step 3: `PaymentExplorerPage.tsx`** — wires `FilterBar` + `PaymentTable` + `Pagination` + `usePaymentList`, handling loading/empty/error states via Task 6's components (empty state distinguishes "no payments exist yet" from "no payments match these filters," using the active filter count to choose the message).

- [ ] **Step 4: Test `PaymentTable.test.tsx`** — renders a fixture list, confirms every column's expected value appears, confirms row links point at the right `/payments/:id` path, confirms an empty list renders nothing (parent page owns the empty state, not the table itself).

- [ ] **Step 5: Wire the real route** in `router.tsx`, replacing the Task 5 stub.

- [ ] **Step 6: Verify** (same 4 commands as Task 6 Step 6).

- [ ] **Step 7: Commit**

```bash
git add frontend/src/pages/PaymentExplorerPage.tsx frontend/src/components/PaymentTable.tsx frontend/src/hooks/usePaymentList.ts frontend/src/router.tsx
git commit -m "feat(frontend): add payment explorer page with filtering, pagination, and polling"
```

---

### Task 8: Route visualization component

**Files:**
- Create: `frontend/src/components/RouteVisualization.tsx`
- Test: `RouteVisualization.test.tsx`

**Interfaces:**
- Produces: `<RouteVisualization hops={payment.hops} sourceChain={...} destinationChain={...} executionMode={...} asset={...} />` — consumed by Task 11's Payment Detail page.

- [ ] **Step 1: Implement the hand-rolled SVG network diagram**

Fixed layout for the 5 `SIMULATED_GRAPH_CHAINS` nodes (e.g. a simple pentagon or horizontal-row arrangement — a pentagon reads more like "a network" and avoids implying a linear pipeline). For each node, render muted/gray by default; for nodes/edges that are part of THIS payment's actual `hops` array, render in the accent color with a solid line; every other potential edge between the 5 nodes stays a faint dashed line (representing "structurally routable in the simulated graph, not part of this payment"). Render `hops[].bridge_name` as a label on the traversed edge(s). Render a badge (using Task 5's `hasRealTestnetExecution` helper) reading either "Simulated route" (muted) or "Live testnet execution" (accent, with a small distinguishing icon) — this badge's logic MUST call `hasRealTestnetExecution(sourceChain, destinationChain, asset, executionMode)`, never infer from chain names directly inside this component (keeping the single source of truth in Task 5's `lib/chains.ts`).

Where `hops.length > 1` (an intermediate hop exists — check whether this is actually reachable given the current 5-node/direct-edge routing graph, or whether every real response is always a single hop; read the C++ router's actual behavior/test fixtures if uncertain, and render the component correctly for BOTH cases regardless, per the design's explicit "where intermediate hops exist, show them clearly" requirement), render each hop as a distinct labeled segment in sequence, not collapsed into a single source→destination arrow.

- [ ] **Step 2: Test**

`RouteVisualization.test.tsx`: a simulated-mode single-hop payment renders the "Simulated route" badge and highlights exactly the source/destination nodes and one edge; a testnet-mode Ethereum↔Base WETH payment renders the "Live testnet execution" badge; a testnet-mode payment on a chain pair `hasRealTestnetExecution` returns false for (if such test data can exist — check whether the backend could ever actually produce this combination, per the design doc's note that this should be unreachable by construction, but the component's OWN logic should still be independently tested against the helper function directly, not just against "realistic" fixtures) correctly does NOT render the live-execution badge.

- [ ] **Step 3: Verify + commit**

```bash
cd frontend && npx tsc --noEmit && npx eslint src --ext .ts,.tsx && npx vitest run && npm run build
git add frontend/src/components/RouteVisualization.tsx frontend/src/components/RouteVisualization.test.tsx
git commit -m "feat(frontend): add reusable SVG route visualization distinguishing simulated graph from live testnet execution"
```

---

### Task 9: Lifecycle timeline component

**Files:**
- Create: `frontend/src/components/LifecycleTimeline.tsx`
- Test: `LifecycleTimeline.test.tsx`

**Interfaces:**
- Produces: `<LifecycleTimeline payment={payment} />` — consumed by Task 11.

- [ ] **Step 1: Derive lifecycle stages from real fields only**

The 7 conceptual stages from §4 (`Payment Created → Quotes Retrieved → Route Selected → Persisted → Kafka Processing → Blockchain Execution → Destination Confirmation`) do not each have a distinct backend timestamp (per the design doc's §1 finding — only `created_at`/`updated_at`/`completed_at` on `payments` and `broadcast_at`/`confirmed_at` on the execution exist). Implement this component to derive stage completion/active/pending/failed state from what's ACTUALLY available, being explicit in the UI about which stages are inferred-from-current-status vs. have a real timestamp:
- "Payment Created" — always complete, timestamp = `created_at`.
- "Quotes Retrieved" + "Route Selected" — complete (no distinct timestamp; both are known to have happened by the time `created_at`'s row exists, since routing happens synchronously before persistence) for testnet-mode payments with quotes; for simulated-mode payments, render these two stages as "N/A — simulated mode" rather than claiming a false timestamp.
- "Persisted" — complete, same timestamp as "Payment Created" (persistence is what created the row).
- "Kafka Processing" — active/complete based on `status` (`ROUTED` = pending, `PROCESSING` or later = complete), no distinct timestamp available — label it as inferred, not timestamped.
- "Blockchain Execution" — for testnet-mode: pending until `submitted_at` (maps to `broadcast_at`) is set, then complete with that timestamp; for simulated-mode, render as "N/A — simulated mode" (no real execution happens).
- "Destination Confirmation" — complete with `completed_at` timestamp when `status` is `COMPLETED`; shown as failed (not pending) with `failure_reason` displayed when `status` is `FAILED`.

Never invent a timestamp for a stage that doesn't have one — show "—" or "not tracked" rather than guessing.

- [ ] **Step 2: Visual treatment**

Each stage rendered as a step in a horizontal or vertical timeline, with 4 distinct visual states (complete/active/pending/failed) using `StatusBadge`'s color mapping (Task 6) for consistency — e.g. complete = filled circle + accent/green, active = pulsing-but-restrained indicator, pending = outline/muted circle, failed = red with the `failure_reason` text shown inline at that stage, not just in a separate field elsewhere on the page.

- [ ] **Step 3: Test**

`LifecycleTimeline.test.tsx`: a `COMPLETED` testnet-mode payment with all timestamps renders every stage as complete with the right timestamps; a `FAILED` payment renders the failure at the correct stage with `failure_reason` visible; a `ROUTED` simulated-mode payment renders "Quotes Retrieved"/"Route Selected"/"Blockchain Execution" as N/A, not as pending or complete.

- [ ] **Step 4: Verify + commit**

```bash
cd frontend && npx tsc --noEmit && npx eslint src --ext .ts,.tsx && npx vitest run && npm run build
git add frontend/src/components/LifecycleTimeline.tsx frontend/src/components/LifecycleTimeline.test.tsx
git commit -m "feat(frontend): add payment lifecycle timeline derived from real backend timestamps only"
```

---

### Task 10: Provider comparison component

**Files:**
- Create: `frontend/src/components/ProviderComparison.tsx`
- Create: `frontend/src/hooks/usePaymentQuotes.ts`
- Test: `ProviderComparison.test.tsx`

**Interfaces:**
- Consumes: Task 3's `GET /payments/{id}/quotes`.
- Produces: `<ProviderComparison paymentId={id} />` — consumed by Task 11.

- [ ] **Step 1: `usePaymentQuotes.ts`** — `apiGet<PaymentQuotesResponse>(\`/payments/${paymentId}/quotes\`)`, no polling needed (quotes are immutable once a payment is created, unlike status).

- [ ] **Step 2: `ProviderComparison.tsx`**

If `quotes.length === 0`: render Task 6's `EmptyState` with the message "No provider comparison data available for this payment" (covers both simulated-mode payments and payments that predate migration 0007 — the component does not need to distinguish these two cases, since the honest message is the same either way: no data exists).

If `quotes.length >= 1`: render each quote as a card (provider name via `PROVIDER_DISPLAY_NAMES`, quoted output via `formatBaseUnits`, fee via `formatBaseUnits`, estimated fill time), with the `selected: true` entry visually distinguished (border/background per Task 5's accent color) and labeled "Selected by ChainRoute." If exactly one quote exists, add a small note ("Only one provider responded for this payment") rather than presenting a single card as if it "won" a comparison that didn't happen.

- [ ] **Step 3: Test**

`ProviderComparison.test.tsx`: empty array renders the empty state; two quotes render both cards with the selected one visually marked (test via a data attribute or class name, not just visual inspection); a single-quote array renders the "only one provider responded" note.

- [ ] **Step 4: Verify + commit**

```bash
cd frontend && npx tsc --noEmit && npx eslint src --ext .ts,.tsx && npx vitest run && npm run build
git add frontend/src/components/ProviderComparison.tsx frontend/src/hooks/usePaymentQuotes.ts
git commit -m "feat(frontend): add Across vs Relay provider comparison, honest about missing data"
```

---

### Task 11: Payment Detail page

**Files:**
- Create: `frontend/src/pages/PaymentDetailPage.tsx`
- Create: `frontend/src/hooks/usePayment.ts`
- Create: `frontend/src/lib/explorer.ts`
- Test: `PaymentDetailPage.test.tsx`

**Interfaces:**
- Consumes: Tasks 5, 6, 8, 9, 10 in full.

- [ ] **Step 1: `usePayment.ts`** — `apiGet<Payment>(\`/payments/${id}\`)`, `refetchInterval: 5_000` while `status` is non-terminal (`ROUTED`/`PROCESSING`/`SUBMITTED`), `refetchInterval: false` once `COMPLETED`/`FAILED` (no point polling a payment that can no longer change — an explicit, small efficiency/correctness detail worth getting right).

- [ ] **Step 2: `frontend/src/lib/explorer.ts`**

```typescript
const EXPLORER_BASE_URLS: Record<string, string> = {
  "ethereum-sepolia": "https://sepolia.etherscan.io/tx/",
  "base-sepolia": "https://sepolia.basescan.org/tx/",
};

// Only ever links out for a testnet-mode payment on the one real
// corridor -- never guesses a mainnet explorer URL, never links for
// simulated-mode payments (their tx hash, if any, is not a real
// on-chain transaction).
export function explorerUrl(payment: { execution_mode: string; source_chain: string; destination_chain: string; asset: string; external_tx_hash: string | null }): string | null {
  if (!payment.external_tx_hash) return null;
  if (payment.execution_mode !== "testnet") return null;
  // The tx is broadcast on the ORIGIN chain -- confirm against the
  // backend's own convention (executor.go broadcasts via OriginClient on
  // OriginChainID) which chain "source_chain" maps to before finalizing
  // this lookup key.
  const key = `${payment.source_chain.toLowerCase()}-sepolia`;
  const base = EXPLORER_BASE_URLS[key];
  return base ? base + payment.external_tx_hash : null;
}
```
Read `go-api/internal/worker/executor.go`'s broadcast logic to confirm the transaction is genuinely broadcast on the ORIGIN chain (not the destination) before finalizing which of `source_chain`/`destination_chain` this function keys off — do not assume, verify against the actual code.

- [ ] **Step 3: `PaymentDetailPage.tsx`**

Layout, per the spec's explicit instruction that this is "the strongest part of the application" and "the visual centerpiece should be the payment route/lifecycle, not generic KPI cards": `LifecycleTimeline` and `RouteVisualization` prominent near the top, `ProviderComparison` below, then a details panel with every field from §4's list (source/destination, amount, asset, provider, routing cost, execution mode, tx hash as a link via `explorerUrl` when non-null else plain monospace text, provider status/reference — note: `ProviderReferenceID`/`ExternalStatus`/`RawExternalStatus` are NOT currently in the `GET /payments/{id}` response per the survey; if this data is needed for §4's "provider status/reference" requirement, either extend `GET /payments/{id}`'s existing response — carefully, preserving every existing field, additive only — or accept this is not currently surfaced and note it as a limitation; make this decision explicitly and document it in the task's commit message rather than silently omitting the requirement).

- [ ] **Step 4: Test**

`PaymentDetailPage.test.tsx`: mocks `apiGet` to return a fixture `Payment`, confirms `LifecycleTimeline`/`RouteVisualization`/`ProviderComparison` all receive the right props, confirms a `FAILED` payment's `failure_reason` is visible on the page, confirms loading/error states render correctly when the fetch is pending/fails.

- [ ] **Step 5: Wire the real route**, replacing the Task 5 stub.

- [ ] **Step 6: Verify + commit**

```bash
cd frontend && npx tsc --noEmit && npx eslint src --ext .ts,.tsx && npx vitest run && npm run build
git add frontend/src/pages/PaymentDetailPage.tsx frontend/src/hooks/usePayment.ts frontend/src/lib/explorer.ts frontend/src/router.tsx
git commit -m "feat(frontend): add payment detail page with lifecycle, route, and provider comparison"
```

---

### Task 12: Overview Dashboard page

**Files:**
- Create: `frontend/src/pages/OverviewPage.tsx`
- Create: `frontend/src/components/charts/PaymentVolumeChart.tsx`, `StatusDistributionChart.tsx`, `ProviderSelectionChart.tsx`, `RoutingCostChart.tsx`, `NetworkUsageChart.tsx`
- Create: `frontend/src/components/StatCard.tsx`
- Create: `frontend/src/hooks/useDashboardStats.ts`, `useTimeseries.ts`
- Modify (conditionally, only if `NetworkUsageChart` extends the stats endpoint per Step 4): `frontend/src/api/types.ts`, `go-api/internal/handler/dashboard.go`, `go-api/internal/postgres/store.go`, `go-api/internal/payment/payment.go`
- Test: `PaymentVolumeChart.test.tsx` (representative — see Step 3)

**Interfaces:**
- Consumes: `GET /dashboard/stats`, `GET /dashboard/timeseries`.

- [ ] **Step 1: `useDashboardStats.ts`/`useTimeseries.ts`** — `apiGet<DashboardStats>("/dashboard/stats")` (`refetchInterval: 10_000`); `apiGet<TimeseriesResponse>("/dashboard/timeseries", {metric, interval, days})` per-metric.

- [ ] **Step 2: `StatCard.tsx`** — small, restrained (per "avoid generic KPI cards as the centerpiece" — these support the page, they are not its focus), label + value + optional trend indicator.

- [ ] **Step 3: `PaymentVolumeChart.tsx`** — the one chart given FULL code here; the remaining four follow this exact pattern with different data/labels:

```typescript
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";
import type { TimeseriesPoint } from "../../api/types";
import { formatTimestamp } from "../../lib/format";

export function PaymentVolumeChart({ points }: { points: TimeseriesPoint[] }) {
  if (points.length === 0) {
    return <EmptyState message="No payment volume data yet" />;
  }
  const data = points.map((p) => ({ bucket: p.bucket, count: p.count }));
  return (
    <div>
      <h3 className="text-sm font-medium text-slate-700 mb-2">Payment Volume</h3>
      <ResponsiveContainer width="100%" height={240}>
        <AreaChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
          <XAxis dataKey="bucket" tickFormatter={(v) => formatTimestamp(v).split(",")[0]} fontSize={12} />
          <YAxis allowDecimals={false} fontSize={12} label={{ value: "Payments", angle: -90, position: "insideLeft" }} />
          <Tooltip labelFormatter={(v) => formatTimestamp(v as string)} formatter={(value: number) => [value, "Payments"]} />
          <Area type="monotone" dataKey="count" stroke="#..." fill="#..." fillOpacity={0.2} />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
```
(Fill in the actual accent color hex from Task 5's Tailwind config rather than the `#...` placeholder — read `tailwind.config.js` for the real value.) Import `EmptyState` from `../EmptyState`.

- [ ] **Step 4: The remaining four charts, following `PaymentVolumeChart`'s exact structure**

`StatusDistributionChart.tsx` — a `PieChart`/`BarChart` from `DashboardStats`'s four counts (total/completed/processing/failed — derive the distribution client-side from the already-fetched stats object, no new endpoint needed), using `StatusBadge`'s color mapping (Task 6) for each slice/bar so status colors are consistent across the whole app, not redefined here.

`ProviderSelectionChart.tsx` — a bar/pie chart from `DashboardStats.provider_usage` (across vs relay counts), using `PROVIDER_DISPLAY_NAMES`.

`RoutingCostChart.tsx` — identical structure to `PaymentVolumeChart` but sourced from `GET /dashboard/timeseries?metric=routing_cost`, using `p.value` (the average fee) instead of `p.count`, y-axis labeled in the appropriate unit (note `total_fee` in the existing `payments` table is a `DOUBLE PRECISION` from the C++ router's simulated-fee-units convention, not a real currency amount for simulated-mode payments — label the axis honestly, e.g. "Routing cost (fee units)" rather than implying USD, unless you've confirmed testnet-mode `total_fee` really is in a specific token's base units worth converting — check `go-api/internal/handler/payments.go`'s `resp.GetTotalFee()` usage and the C++ proto's `total_fee` field semantics before choosing a label; if genuinely ambiguous across simulated vs. testnet modes, the honest label is a generic unit, not a fabricated currency symbol).

`NetworkUsageChart.tsx` — a bar chart of source→destination chain-pair frequency. **This requires data `GET /dashboard/stats` doesn't currently provide** (chain-pair counts, not just status/provider counts) — either extend `dashboardStats`'s Go response with a `network_usage: [{source_chain, destination_chain, count}]` field (requires revisiting Task 3's endpoint — acceptable, since Task 3's response shape isn't yet "shipped" to any external consumer beyond this same plan) or compute an approximation client-side from a large-enough `GET /payments` page (worse — re-introduces pagination limits into what should be a true aggregate). Prefer extending `GET /dashboard/stats` (go back and add one more `GROUP BY source_chain, destination_chain` query to `postgres.Store.GetDashboardStats`, following Task 3's exact established pattern for the provider-usage query) over a client-side approximation — this keeps every chart backed by a true, indexed SQL aggregate per the plan's own N+1/performance constraint. If you extend the Go response this way, also add the matching `network_usage: { source_chain: string; destination_chain: string; count: number }[]` field to `DashboardStats` in `frontend/src/api/types.ts` (Task 5 shipped that interface without this field, since the field didn't exist yet at that point in the plan) — the Go struct and the TS interface must agree on the exact field names.

- [ ] **Step 5: `OverviewPage.tsx`**

Stat cards row (total/completed/processing/failed, from `DashboardStats`) below a brief page-level summary, then the 5 charts in a responsive grid, each independently loading/erroring (a slow `routing_cost` timeseries query must not block the `volume` chart from rendering) — fetch each chart's data via its own hook call, not one monolithic "overview data" fetch, so Task 6's per-chart loading/empty/error states are meaningful.

- [ ] **Step 6: Test** — `PaymentVolumeChart.test.tsx` as the representative pattern (empty points renders `EmptyState`; populated points renders the expected number of data points); apply the same shape of test to at least one more chart (`StatusDistributionChart`) to prove the pattern generalizes, rather than writing all five exhaustively if time is genuinely constrained — but do not skip testing entirely.

- [ ] **Step 7: Wire the real route**, replacing the Task 5 stub.

- [ ] **Step 8: Verify + commit**

```bash
cd frontend && npx tsc --noEmit && npx eslint src --ext .ts,.tsx && npx vitest run && npm run build
git add frontend/src/pages/OverviewPage.tsx frontend/src/components/charts frontend/src/components/StatCard.tsx frontend/src/hooks/useDashboardStats.ts frontend/src/hooks/useTimeseries.ts frontend/src/router.tsx
# if Step 4's NetworkUsageChart required extending GET /dashboard/stats:
git add go-api/internal/handler/dashboard.go go-api/internal/postgres/store.go go-api/internal/payment/payment.go
git commit -m "feat: add overview dashboard page with payment volume, status, provider, cost, and network-usage charts"
```

---

### Task 13: Docker integration

**Files:**
- Create: `frontend/Dockerfile`
- Create: `frontend/docker-entrypoint.sh`
- Modify: `docker-compose.yml`
- Modify: `.github/workflows/ci.yml` (add a `frontend` job)

**Interfaces:** none beyond what Tasks 5-12 established.

- [ ] **Step 1: `frontend/Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1
FROM node:20-bookworm AS builder
WORKDIR /src
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginx:1.27-alpine AS runtime
COPY --from=builder /src/dist /usr/share/nginx/html
COPY docker-entrypoint.sh /docker-entrypoint.d/40-chainroute-env-config.sh
RUN chmod +x /docker-entrypoint.d/40-chainroute-env-config.sh
EXPOSE 80
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --spider -q http://localhost:80/ || exit 1
```
(`nginx:1.27-alpine`'s official image already runs any executable script placed in `/docker-entrypoint.d/` before starting nginx, in filename-sort order — the `40-` prefix is a convention, not load-bearing, just keeps it readable alongside nginx's own numbered default scripts. This image has a working shell and `wget`, so a real `HEALTHCHECK` — unlike the Go server's distroless image — is genuinely achievable here; confirm `wget` is actually present in `nginx:1.27-alpine` before finalizing, since Alpine's minimal image set sometimes omits tools a Debian-based image would have — if `wget` isn't present, use `CMD curl -f http://localhost/ || exit 1` instead, whichever is actually installed.)

- [ ] **Step 2: `frontend/docker-entrypoint.sh`**

```bash
#!/bin/sh
set -eu
echo "window.__CHAINROUTE_API_BASE_URL__ = \"${API_BASE_URL:-http://localhost:8080}\";" > /usr/share/nginx/html/env-config.js
```

- [ ] **Step 3: Add the `frontend` service to `docker-compose.yml`**

Read the current file in full first (Phase 11's version). Add:
```yaml
  frontend:
    build:
      context: ./frontend
      dockerfile: Dockerfile
    environment:
      API_BASE_URL: http://localhost:8080
    ports:
      - "5173:80"
    networks: [chainroute-net]
    restart: unless-stopped
```
No `depends_on` on `go-server` — per the design doc, the frontend's own loading/error states handle a not-yet-ready API, matching the established "nothing gates on a soft dependency" pattern this project already uses for observability. Confirm `go-server`'s CORS default (`http://localhost:5173`, from Task 2) already matches this port with zero additional config.

- [ ] **Step 4: CI — add a `frontend` job to `.github/workflows/ci.yml`**

Read the current file in full first. Add, following the existing `go` job's exact structural pattern (checkout, setup action, lint/typecheck/test/build steps, `working-directory: frontend`):
```yaml
  frontend:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: "20"
          cache: "npm"
          cache-dependency-path: frontend/package-lock.json
      - name: install
        working-directory: frontend
        run: npm ci
      - name: typecheck
        working-directory: frontend
        run: npx tsc --noEmit
      - name: lint
        working-directory: frontend
        run: npx eslint src --ext .ts,.tsx
      - name: test
        working-directory: frontend
        run: npx vitest run
      - name: build
        working-directory: frontend
        run: npm run build
```
Also extend the existing `docker-build` job with one more `docker/build-push-action@v6` step (`context: frontend`, `push: false`) and one more `hadolint-action` step for `frontend/Dockerfile`, following the exact pattern the three existing Dockerfile build/lint step-pairs already use.

- [ ] **Step 5: Validate**

```bash
docker compose -f docker-compose.yml config
python3 -c "import yaml, sys; yaml.safe_load(open('.github/workflows/ci.yml'))"
```
If a Docker daemon is reachable (`timeout 8 docker info`), attempt `docker build -t chainroute-frontend:dev -f frontend/Dockerfile frontend` for real and report honestly either way — this project's session history has had no reachable Docker daemon in every prior phase; if that's still true, state so plainly rather than fabricating a build result.

- [ ] **Step 6: Commit**

```bash
git add frontend/Dockerfile frontend/docker-entrypoint.sh docker-compose.yml .github/workflows/ci.yml
git commit -m "feat: integrate frontend into Docker Compose and CI"
```

---

### Task 14: Terraform integration

**Files:**
- Create: `infra/terraform/modules/frontend/main.tf`, `variables.tf`, `outputs.tf`
- Modify: `infra/terraform/environments/dev/main.tf`, `variables.tf`, `outputs.tf`

**Interfaces:** none beyond Phase 11's existing module tree.

- [ ] **Step 1: `infra/terraform/modules/frontend/main.tf`**

```hcl
resource "aws_s3_bucket" "frontend" {
  bucket_prefix = "${var.environment}-chainroute-frontend-"
}

resource "aws_s3_bucket_public_access_block" "frontend" {
  bucket                  = aws_s3_bucket.frontend.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_cloudfront_origin_access_control" "frontend" {
  name                              = "${var.environment}-chainroute-frontend-oac"
  origin_access_control_origin_type = "s3"
  signing_behavior                   = "always"
  signing_protocol                   = "sigv4"
}

resource "aws_s3_bucket_policy" "frontend" {
  bucket = aws_s3_bucket.frontend.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "cloudfront.amazonaws.com" }
      Action    = "s3:GetObject"
      Resource  = "${aws_s3_bucket.frontend.arn}/*"
      Condition = {
        StringEquals = { "AWS:SourceArn" = aws_cloudfront_distribution.frontend.arn }
      }
    }]
  })
}

resource "aws_cloudfront_distribution" "frontend" {
  enabled             = true
  default_root_object = "index.html"

  origin {
    domain_name              = aws_s3_bucket.frontend.bucket_regional_domain_name
    origin_id                = "s3-frontend"
    origin_access_control_id = aws_cloudfront_origin_access_control.frontend.id
  }

  default_cache_behavior {
    target_origin_id       = "s3-frontend"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods         = ["GET", "HEAD"]
    cached_methods           = ["GET", "HEAD"]
    forwarded_values {
      query_string = false
      cookies { forward = "none" }
    }
  }

  # SPA client-side routing: any path CloudFront can't find in the S3
  # bucket (i.e. anything but the built asset filenames) should serve
  # index.html so React Router can handle it client-side, not a raw
  # CloudFront/S3 404 or 403 page.
  custom_error_response {
    error_code         = 403
    response_code      = 200
    response_page_path = "/index.html"
  }
  custom_error_response {
    error_code         = 404
    response_code      = 200
    response_page_path = "/index.html"
  }

  restrictions {
    geo_restriction { restriction_type = "none" }
  }

  viewer_certificate {
    cloudfront_default_certificate = var.acm_certificate_arn == null
    acm_certificate_arn            = var.acm_certificate_arn
    ssl_support_method             = var.acm_certificate_arn == null ? null : "sni-only"
  }
}
```

- [ ] **Step 2: `infra/terraform/modules/frontend/variables.tf`**

```hcl
variable "environment" {
  type = string
}

variable "acm_certificate_arn" {
  description = "Optional ACM certificate ARN for a custom domain -- MUST be in us-east-1 regardless of the deployment's own aws_region, since that is a hard CloudFront requirement, not a ChainRoute choice. Null uses CloudFront's own default *.cloudfront.net certificate (works out of the box, no custom domain)."
  type        = string
  default     = null
}
```

- [ ] **Step 3: `infra/terraform/modules/frontend/outputs.tf`**

```hcl
output "cloudfront_domain_name" {
  value = aws_cloudfront_distribution.frontend.domain_name
}

output "s3_bucket_name" {
  value = aws_s3_bucket.frontend.id
}
```

- [ ] **Step 4: Wire into `infra/terraform/environments/dev/main.tf`**

Read the current file in full first. Add:
```hcl
module "frontend" {
  count  = var.enable_frontend ? 1 : 0
  source = "../../modules/frontend"

  environment = var.environment
}
```
Add `enable_frontend` (bool, default `true`) to `environments/dev/variables.tf`, mirroring `enable_observability_stack`'s exact pattern. Update `go_server_service`'s `environment_variables` to add `CHAINROUTE_CORS_ALLOWED_ORIGINS`, computed from the frontend module's CloudFront domain when enabled:
```hcl
CHAINROUTE_CORS_ALLOWED_ORIGINS = var.enable_frontend ? "https://${module.frontend[0].cloudfront_domain_name}" : "http://localhost:5173"
```
Add `cloudfront_domain_name` to `environments/dev/outputs.tf` (conditional on `enable_frontend`, following Terraform's standard `count`-indexed-module optional-output pattern: `value = var.enable_frontend ? module.frontend[0].cloudfront_domain_name : null`).

- [ ] **Step 5: Verify**

```bash
export PATH="$HOME/.local/bin:$PATH"
cd infra/terraform
terraform fmt -recursive -check -diff
cd environments/dev
terraform init -backend=false
terraform validate
export AWS_ACCESS_KEY_ID=dummy AWS_SECRET_ACCESS_KEY=dummy AWS_REGION=us-east-1
cp terraform.tfvars.example terraform.tfvars
terraform plan   # expect it to walk the full graph and fail only at STS credential validation, same as every prior Phase 11 check
rm terraform.tfvars
```
Also re-sweep every new `description` field this task adds against AWS's security-group/CloudFront description character constraints (this task doesn't add any security groups, but if any resource here has a `description` argument with API-level character restrictions, check it the same way Phase 11's final fix round did).

Do **not** run `terraform apply`.

- [ ] **Step 6: Commit**

```bash
git add infra/terraform/modules/frontend infra/terraform/environments/dev
git commit -m "feat(terraform): add S3+CloudFront static hosting for the frontend"
```

---

### Task 15: Demo data seed script

**Files:**
- Create: `scripts/seed_demo_data.sh`

**Interfaces:** none.

- [ ] **Step 1: Write the script**

```bash
#!/usr/bin/env bash
# Populates a running ChainRoute server with realistic-looking demo
# payments via the REAL POST /payments API, simulated mode only -- never
# a direct SQL insert, never run against a deployed environment by
# default. Local development / demo convenience only.
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
COUNT="${COUNT:-40}"

CHAINS=(ethereum base arbitrum optimism polygon)
ASSETS=(usdc eth)

for i in $(seq 1 "$COUNT"); do
	src_idx=$((RANDOM % ${#CHAINS[@]}))
	dst_idx=$((RANDOM % ${#CHAINS[@]}))
	while [ "$dst_idx" -eq "$src_idx" ]; do
		dst_idx=$((RANDOM % ${#CHAINS[@]}))
	done
	asset_idx=$((RANDOM % ${#ASSETS[@]}))
	amount=$(( (RANDOM % 500) + 1 ))

	curl -sf -X POST "$BASE_URL/payments" \
		-H "Content-Type: application/json" \
		-H "Idempotency-Key: demo-seed-$(date +%s)-$i-$RANDOM" \
		-d "{\"source_chain\":\"${CHAINS[$src_idx]}\",\"destination_chain\":\"${CHAINS[$dst_idx]}\",\"asset\":\"${ASSETS[$asset_idx]}\",\"amount\":\"${amount}\"}" \
		> /dev/null
	echo "seeded payment $i/$COUNT"
done

echo "Done. Demo data uses simulated mode only -- no real blockchain activity, no fabricated data outside the app's own real code path."
```
`chmod +x scripts/seed_demo_data.sh`.

- [ ] **Step 2: Test manually against a locally-reachable server, if one can be started**

If a native `go-api` server can be started in this environment (per the established pattern from every prior phase — build+run natively, matching `scripts/e2e_test.sh`'s own approach), run the script against it and confirm real payments appear via `GET /payments`. If no server can be reasonably started standalone in this task's scope, at minimum run `bash -n scripts/seed_demo_data.sh` for syntax validation and note in the report that live execution wasn't attempted here (it will be exercised as part of the plan's own post-implementation full-stack verification, if Docker/native services are available then).

- [ ] **Step 3: Commit**

```bash
git add scripts/seed_demo_data.sh
git commit -m "feat: add demo data seed script using the real POST /payments API"
```

---

### Task 16: Documentation

**Files:**
- Modify: `README.md`

**Interfaces:** none (final task).

- [ ] **Step 1: Update `README.md`**

Read the current file in full (already substantial from Phases 10-11). Add sections covering: frontend architecture (the `frontend/` structure, React+TypeScript+Vite+Tailwind+TanStack Query+Recharts, why each was chosen), major pages/components (Overview, Payment Explorer, Payment Detail, the shared component set), the 4 new backend endpoints (exact paths/params/response shapes, cross-referenced against what actually shipped in Tasks 1-4, not this plan's draft sketches), the `payment_quotes` schema change and why it was necessary (only the winning quote was persisted before; §6 requires honest comparison data), the provider-comparison behavior (including the explicit empty-state handling for payments without data), the real-time update strategy (polling, with the same reasoning the design doc states), local development (`cd frontend && npm install && npm run dev`, pointing `VITE_API_BASE_URL` at a locally-running server), Docker startup (`docker compose up -d`, now including the frontend on `:5173`), Terraform (`modules/frontend`, the `enable_frontend` toggle, S3+CloudFront rationale), the demo-data workflow (`scripts/seed_demo_data.sh`), and an explicit "React Dashboard vs. Grafana" section stating which panels/views live where and why (cross-referencing the design doc's own non-overlap reasoning).

**Do not fabricate screenshots.** If a running instance of the app can genuinely be started and screenshotted in this environment, do so and embed real images; if not (consistent with this project's established, disclosed pattern of Docker/browser-based verification being unavailable in this session's environment), state plainly that no screenshots are included and why, rather than describing imagined ones.

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: document the React dashboard, new endpoints, schema change, and demo workflow"
```

---

## Post-implementation (controller responsibility, not a numbered task)

After Task 16, run the full regression suite: Go build/vet/test, Postgres integration tests (including every new Task 1-4 test), Kafka-tagged tests (compile-only if no broker, as established throughout this project), C++ `router`/`cpp-routing-service` `ctest` (confirming zero behavior change — this phase should not have touched any C++ file at all), frontend `tsc`/`eslint`/`vitest`/`vite build`, `docker compose config` validation, Terraform `fmt`/`validate`/dummy-credential-`plan`. If a Docker daemon and/or native Postgres+Kafka+Go+C++ stack can actually be started, attempt the real end-to-end flow the user's request names explicitly (create payment → route → persist → Kafka → process → execute simulated → update status → dashboard reflects the new state) and report the ACTUAL result; if it cannot be run, say so plainly rather than claiming it. Then dispatch the final whole-branch review (most capable model), covering in addition to the standard checklist: every frontend data field traced back to a real backend field (no fabricated/hallucinated data anywhere in a chart or table); no N+1 query pattern in any new endpoint; no secret/internal-service endpoint ever reachable from the frontend bundle or Docker/Terraform config; simulated-vs-real-testnet execution represented accurately in the route visualization and lifecycle timeline; the Across/Relay comparison never presents unavailable data as if it were real; every chart backed by a real persisted-data query; zero changes to `router/`, zero changes to any file under `go-api/internal/{worker,bridge,execution}/`. Produce the final report in the exact format the user's original request specifies. Do not merge or push — stop after implementation, testing, and final review, per explicit user instruction.
