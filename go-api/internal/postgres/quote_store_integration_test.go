//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"chainroute/go-api/internal/payment"
)

func TestCreateOrGetPayment_PersistsQuoteAtomicallyWithPayment(t *testing.T) {
	store := newTestStore(t) // reuse the existing integration test helper -- confirm its exact name in store_integration_test.go
	key := "quote-atomic-" + t.Name()
	cleanup := func() {
		if _, err := store.db.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key = $1`, key); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	now := time.Now().UTC()
	p := payment.Payment{
		IdempotencyKey: key, SourceChain: "ethereum", DestinationChain: "base",
		Asset: "eth", Amount: "0.001", ExecutionMode: payment.ExecutionModeTestnet,
		Hops: []payment.Hop{{HopIndex: 0, FromChain: "ethereum", ToChain: "base", BridgeName: "across", Fee: 0.0001, LatencyMs: 60000, Liquidity: 0.001, Reliability: 1.0}},
		Quotes: []payment.Quote{{
			Provider: "across", OriginChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
			InputAmount: "1000000000000000", OutputAmount: "999900000000000", FeeAmount: "100000000000",
			EstimatedFillTimeSec: 60, QuotedAt: now, ExpiresAt: now.Add(2 * time.Minute),
			RawProviderPayload: json.RawMessage(`{"spokePoolAddress":"0xabc"}`),
			Selected:           true,
		}},
	}

	created, outcome, err := store.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("CreateOrGetPayment: %v", err)
	}
	if outcome != payment.Created {
		t.Fatalf("expected Created, got %v", outcome)
	}

	q, found, err := store.GetQuoteByPaymentID(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("GetQuoteByPaymentID: %v", err)
	}
	if !found {
		t.Fatal("expected a payment_quotes row to exist")
	}
	if q.Provider != "across" || q.FeeAmount != "100000000000" {
		t.Errorf("unexpected quote: %+v", q)
	}
}

func TestCreateOrGetPayment_SimulatedModeHasNoQuoteRow(t *testing.T) {
	store := newTestStore(t)
	key := "no-quote-" + t.Name()
	cleanup := func() {
		if _, err := store.db.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key = $1`, key); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	p := payment.Payment{
		IdempotencyKey: key, SourceChain: "ethereum", DestinationChain: "base",
		Asset: "usdc", Amount: "100", ExecutionMode: payment.ExecutionModeSimulated,
	}
	created, _, err := store.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("CreateOrGetPayment: %v", err)
	}
	_, found, err := store.GetQuoteByPaymentID(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("GetQuoteByPaymentID: %v", err)
	}
	if found {
		t.Fatal("simulated-mode payment must not have a payment_quotes row")
	}
}

// TestCreateOrGetPayment_QuoteInsertFailureRollsBackWholeTransaction proves
// the design doc's §7 claim that payments/payment_quotes atomicity is
// "database-enforced," not merely a property inferred from reading
// CreateOrGetPayment's transaction code. It forces a real failure inside
// the transaction, strictly after the payments row (and its hop rows) have
// already been written via tx.ExecContext, but before commit: a Quote with
// a nil RawProviderPayload makes insertPaymentQuotes' own
// `$12::JSONB` cast run `”::JSONB`, which Postgres rejects with
// "invalid input syntax for type json" (confirmed independently via
// `psql -c "SELECT ”::JSONB;"`). No test-only seam or production code
// change was needed to force this -- it's a real constraint violation
// reachable through the normal CreateOrGetPayment call path. If the
// payments insert and the quote insert were not atomic (e.g. two separate
// transactions, or the outer commit happening before the quote insert is
// attempted), the payments row created here would survive this failure;
// asserting count == 0 afterward is the actual proof of rollback, not an
// inference from the surrounding Go code.
func TestCreateOrGetPayment_QuoteInsertFailureRollsBackWholeTransaction(t *testing.T) {
	store := newTestStore(t)
	key := "quote-rollback-" + t.Name()
	cleanup := func() {
		if _, err := store.db.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key = $1`, key); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	now := time.Now().UTC()
	p := payment.Payment{
		IdempotencyKey: key, SourceChain: "ethereum", DestinationChain: "base",
		Asset: "eth", Amount: "0.001", ExecutionMode: payment.ExecutionModeTestnet,
		Hops: []payment.Hop{{HopIndex: 0, FromChain: "ethereum", ToChain: "base", BridgeName: "across", Fee: 0.0001, LatencyMs: 60000, Liquidity: 0.001, Reliability: 1.0}},
		Quotes: []payment.Quote{{
			Provider: "across", OriginChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
			InputAmount: "1000000000000000", OutputAmount: "999900000000000", FeeAmount: "100000000000",
			EstimatedFillTimeSec: 60, QuotedAt: now, ExpiresAt: now.Add(2 * time.Minute),
			Selected: true,
			// RawProviderPayload deliberately left as its nil zero value:
			// insertPaymentQuotes passes string(nil) == "" into `$12::JSONB`,
			// which Postgres rejects at the database level.
		}},
	}

	_, _, err := store.CreateOrGetPayment(context.Background(), p)
	if err == nil {
		t.Fatal("expected CreateOrGetPayment to fail when the quote insert violates the JSONB cast")
	}

	var count int
	row := store.db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM payments WHERE idempotency_key = $1`, key)
	if scanErr := row.Scan(&count); scanErr != nil {
		t.Fatalf("count query: %v", scanErr)
	}
	if count != 0 {
		t.Fatalf("expected the payments row to be rolled back when the quote insert failed, but found %d row(s) -- payments/payment_quotes atomicity is broken", count)
	}

	var hopCount int
	row = store.db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM payment_route_hops h JOIN payments p ON p.id = h.payment_id WHERE p.idempotency_key = $1`, key)
	if scanErr := row.Scan(&hopCount); scanErr != nil {
		t.Fatalf("hop count query: %v", scanErr)
	}
	if hopCount != 0 {
		t.Fatalf("expected the hop row to be rolled back too, but found %d row(s)", hopCount)
	}
}

// TestCreateOrGetPayment_RelayWinnerProducesConsistentProviderAcrossTables
// proves design doc §10/§24's cross-table consistency claim for a
// Relay-selected route: payments.bridge_provider, the winning
// payment_route_hops row, and payment_quotes.provider must all agree on
// "relay" -- not just individually correct, but consistent with each other,
// since Task 5-12 touch these three tables independently.
func TestCreateOrGetPayment_RelayWinnerProducesConsistentProviderAcrossTables(t *testing.T) {
	store := newTestStore(t)
	key := "relay-consistency-" + t.Name()
	cleanup := func() {
		if _, err := store.db.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key = $1`, key); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	p := payment.Payment{
		IdempotencyKey: key, SourceChain: "ethereum", DestinationChain: "base",
		Asset: "eth", Amount: "0.001", ExecutionMode: payment.ExecutionModeTestnet,
		Hops:           []payment.Hop{{HopIndex: 0, FromChain: "ethereum", ToChain: "base", BridgeName: "relay", Fee: 0.0001, LatencyMs: 4000, Liquidity: 0.001, Reliability: 1.0}},
		BridgeProvider: strPtr("relay"),
		Quotes: []payment.Quote{{Provider: "relay", OriginChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
			InputAmount: "1000000000000000", OutputAmount: "999900000000000", FeeAmount: "100000000000",
			EstimatedFillTimeSec: 4, QuotedAt: time.Now(), ExpiresAt: time.Now().Add(time.Minute),
			RawProviderPayload: json.RawMessage(`{"requestId":"0xabc"}`), Selected: true}},
	}
	created, _, err := store.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("CreateOrGetPayment: %v", err)
	}

	got, found, err := store.GetPayment(context.Background(), created.ID)
	if err != nil || !found {
		t.Fatalf("GetPayment: found=%v err=%v", found, err)
	}
	if got.BridgeProvider == nil || *got.BridgeProvider != "relay" {
		t.Errorf("payments.bridge_provider = %v, want relay", got.BridgeProvider)
	}
	if len(got.Hops) == 0 || got.Hops[0].BridgeName != "relay" {
		t.Errorf("payment_route_hops.bridge_name = %+v, want relay", got.Hops)
	}
	q, found, err := store.GetQuoteByPaymentID(context.Background(), created.ID)
	if err != nil || !found {
		t.Fatalf("GetQuoteByPaymentID: found=%v err=%v", found, err)
	}
	if q.Provider != "relay" {
		t.Errorf("payment_quotes.provider = %q, want relay", q.Provider)
	}
}

// TestCreateOrGetPayment_LosingAcrossQuoteIsNeverPersisted asserts, for the
// same Relay-winning payment shape as the test above, that there is no
// trace anywhere in the database of the losing "across" candidate: no
// payment_route_hops row and no payment_quotes row naming "across" for
// this payment_id. Since this payment carries only a single (relay) quote
// -- this test predates Task 1's every-fetched-quote persistence and is
// kept as a direct guard that a payment whose candidate.Quotes never
// contained an "across" entry has no such row, independent of how many
// quotes are actually persisted per payment -- confirming the row(s) that
// exist say "relay", never "across".
func TestCreateOrGetPayment_LosingAcrossQuoteIsNeverPersisted(t *testing.T) {
	store := newTestStore(t)
	key := "no-losing-across-" + t.Name()
	cleanup := func() {
		if _, err := store.db.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key = $1`, key); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	p := payment.Payment{
		IdempotencyKey: key, SourceChain: "ethereum", DestinationChain: "base",
		Asset: "eth", Amount: "0.001", ExecutionMode: payment.ExecutionModeTestnet,
		Hops:           []payment.Hop{{HopIndex: 0, FromChain: "ethereum", ToChain: "base", BridgeName: "relay", Fee: 0.0001, LatencyMs: 4000, Liquidity: 0.001, Reliability: 1.0}},
		BridgeProvider: strPtr("relay"),
		Quotes: []payment.Quote{{Provider: "relay", OriginChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
			InputAmount: "1000000000000000", OutputAmount: "999900000000000", FeeAmount: "100000000000",
			EstimatedFillTimeSec: 4, QuotedAt: time.Now(), ExpiresAt: time.Now().Add(time.Minute),
			RawProviderPayload: json.RawMessage(`{"requestId":"0xabc"}`), Selected: true}},
	}
	created, _, err := store.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("CreateOrGetPayment: %v", err)
	}

	var hopCount int
	if err := store.db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM payment_route_hops WHERE payment_id = $1 AND bridge_name = 'across'`, created.ID,
	).Scan(&hopCount); err != nil {
		t.Fatalf("hop count query: %v", err)
	}
	if hopCount != 0 {
		t.Fatalf("expected no payment_route_hops row naming across for a relay-won payment, got %d", hopCount)
	}

	var quoteCount int
	if err := store.db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM payment_quotes WHERE payment_id = $1 AND provider = 'across'`, created.ID,
	).Scan(&quoteCount); err != nil {
		t.Fatalf("quote count query: %v", err)
	}
	if quoteCount != 0 {
		t.Fatalf("expected no payment_quotes row naming across for a relay-won payment, got %d", quoteCount)
	}

	// Also assert via GetQuoteByPaymentID (the selected-quote read path
	// the Executor depends on) that the winning row is "relay", directly,
	// for clarity of intent.
	q, found, err := store.GetQuoteByPaymentID(context.Background(), created.ID)
	if err != nil || !found {
		t.Fatalf("GetQuoteByPaymentID: found=%v err=%v", found, err)
	}
	if q.Provider != "relay" {
		t.Fatalf("payment_quotes.provider = %q, want relay (proves the single UNIQUE(payment_id) row is not across)", q.Provider)
	}
}

// TestGetQuoteByPaymentID_ReturnsOnlyTheSelectedQuoteAmongMultiple proves
// the load-bearing post-migration-0007 contract: with two payment_quotes
// rows for the same payment (one losing, one winning), GetQuoteByPaymentID
// must keep returning exactly the winning/selected one -- the Executor
// calls this method directly during payment execution and must never see
// a losing quote.
func TestGetQuoteByPaymentID_ReturnsOnlyTheSelectedQuoteAmongMultiple(t *testing.T) {
	store := newTestStore(t)
	key := "selected-quote-among-multiple-" + t.Name()
	cleanup := func() {
		if _, err := store.db.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key = $1`, key); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	now := time.Now().UTC()
	p := payment.Payment{
		IdempotencyKey: key, SourceChain: "ethereum", DestinationChain: "base",
		Asset: "eth", Amount: "0.001", ExecutionMode: payment.ExecutionModeTestnet,
		Hops:           []payment.Hop{{HopIndex: 0, FromChain: "ethereum", ToChain: "base", BridgeName: "relay", Fee: 0.0001, LatencyMs: 4000, Liquidity: 0.001, Reliability: 1.0}},
		BridgeProvider: strPtr("relay"),
		Quotes: []payment.Quote{
			{Provider: "across", OriginChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
				InputAmount: "1000000000000000", OutputAmount: "999800000000000", FeeAmount: "200000000000",
				EstimatedFillTimeSec: 60, QuotedAt: now, ExpiresAt: now.Add(2 * time.Minute),
				RawProviderPayload: json.RawMessage(`{"spokePoolAddress":"0xabc"}`), Selected: false},
			{Provider: "relay", OriginChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
				InputAmount: "1000000000000000", OutputAmount: "999900000000000", FeeAmount: "100000000000",
				EstimatedFillTimeSec: 4, QuotedAt: now, ExpiresAt: now.Add(time.Minute),
				RawProviderPayload: json.RawMessage(`{"requestId":"0xabc"}`), Selected: true},
		},
	}
	created, _, err := store.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("CreateOrGetPayment: %v", err)
	}

	got, found, err := store.GetQuoteByPaymentID(context.Background(), created.ID)
	if err != nil || !found {
		t.Fatalf("GetQuoteByPaymentID: found=%v err=%v", found, err)
	}
	if got.Provider != "relay" || !got.Selected {
		t.Errorf("expected the selected=true relay quote, got provider=%s selected=%v", got.Provider, got.Selected)
	}
}

// TestGetQuotesByPaymentID_ReturnsAllQuotesSelectedFirst proves the
// dashboard-facing read path (Task 3) returns every persisted quote for a
// payment, winning and losing, ordered selected-first.
func TestGetQuotesByPaymentID_ReturnsAllQuotesSelectedFirst(t *testing.T) {
	store := newTestStore(t)
	key := "all-quotes-selected-first-" + t.Name()
	cleanup := func() {
		if _, err := store.db.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key = $1`, key); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	now := time.Now().UTC()
	p := payment.Payment{
		IdempotencyKey: key, SourceChain: "ethereum", DestinationChain: "base",
		Asset: "eth", Amount: "0.001", ExecutionMode: payment.ExecutionModeTestnet,
		Hops:           []payment.Hop{{HopIndex: 0, FromChain: "ethereum", ToChain: "base", BridgeName: "relay", Fee: 0.0001, LatencyMs: 4000, Liquidity: 0.001, Reliability: 1.0}},
		BridgeProvider: strPtr("relay"),
		Quotes: []payment.Quote{
			{Provider: "across", OriginChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
				InputAmount: "1000000000000000", OutputAmount: "999800000000000", FeeAmount: "200000000000",
				EstimatedFillTimeSec: 60, QuotedAt: now, ExpiresAt: now.Add(2 * time.Minute),
				RawProviderPayload: json.RawMessage(`{"spokePoolAddress":"0xabc"}`), Selected: false},
			{Provider: "relay", OriginChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
				InputAmount: "1000000000000000", OutputAmount: "999900000000000", FeeAmount: "100000000000",
				EstimatedFillTimeSec: 4, QuotedAt: now, ExpiresAt: now.Add(time.Minute),
				RawProviderPayload: json.RawMessage(`{"requestId":"0xabc"}`), Selected: true},
		},
	}
	created, _, err := store.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("CreateOrGetPayment: %v", err)
	}

	got, err := store.GetQuotesByPaymentID(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("GetQuotesByPaymentID: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 quotes, got %d", len(got))
	}
	if !got[0].Selected {
		t.Errorf("expected the selected quote first, got %+v", got[0])
	}
	if got[1].Selected {
		t.Errorf("expected only one selected quote, but got[1] is also selected: %+v", got[1])
	}
}

// TestGetQuotesByPaymentID_EmptyForSimulatedModePayment proves
// GetQuotesByPaymentID returns an empty, non-nil slice (not an error) for
// a simulated-mode payment, which never carries any payment_quotes rows.
func TestGetQuotesByPaymentID_EmptyForSimulatedModePayment(t *testing.T) {
	store := newTestStore(t)
	key := "no-quotes-simulated-" + t.Name()
	cleanup := func() {
		if _, err := store.db.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key = $1`, key); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	p := payment.Payment{
		IdempotencyKey: key, SourceChain: "ethereum", DestinationChain: "base",
		Asset: "usdc", Amount: "100", ExecutionMode: payment.ExecutionModeSimulated,
	}
	created, _, err := store.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("CreateOrGetPayment: %v", err)
	}

	got, err := store.GetQuotesByPaymentID(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("GetQuotesByPaymentID: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected zero quotes for a simulated-mode payment, got %d", len(got))
	}
}

func TestMarkProcessingFailed_TransitionsFromProcessingOnly(t *testing.T) {
	store := newTestStore(t)
	key := "mark-failed-" + t.Name()
	cleanup := func() {
		if _, err := store.db.ExecContext(context.Background(),
			`DELETE FROM payments WHERE idempotency_key = $1`, key); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	p := payment.Payment{
		IdempotencyKey: key, SourceChain: "ethereum", DestinationChain: "base",
		Asset: "eth", Amount: "0.001", ExecutionMode: payment.ExecutionModeTestnet,
	}
	created, _, err := store.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("CreateOrGetPayment: %v", err)
	}
	claimed, _, err := store.ClaimPayment(context.Background(), created.ID)
	if err != nil || !claimed {
		t.Fatalf("ClaimPayment: claimed=%v err=%v", claimed, err)
	}

	ok, err := store.MarkProcessingFailed(context.Background(), created.ID, "fee_slippage_exceeded")
	if err != nil {
		t.Fatalf("MarkProcessingFailed: %v", err)
	}
	if !ok {
		t.Fatal("expected MarkProcessingFailed to succeed from PROCESSING")
	}

	got, found, err := store.GetPayment(context.Background(), created.ID)
	if err != nil || !found {
		t.Fatalf("GetPayment: found=%v err=%v", found, err)
	}
	if got.Status != payment.StatusFailed {
		t.Errorf("Status = %v, want FAILED", got.Status)
	}
	if got.FailureReason == nil || *got.FailureReason != "fee_slippage_exceeded" {
		t.Errorf("FailureReason = %v, want fee_slippage_exceeded", got.FailureReason)
	}

	// Second call must be a safe no-op (guarded by WHERE status = 'PROCESSING').
	ok2, err := store.MarkProcessingFailed(context.Background(), created.ID, "routing_quote_expired")
	if err != nil {
		t.Fatalf("MarkProcessingFailed (second call): %v", err)
	}
	if ok2 {
		t.Fatal("expected the second MarkProcessingFailed call to report false -- payment is no longer PROCESSING")
	}
}
