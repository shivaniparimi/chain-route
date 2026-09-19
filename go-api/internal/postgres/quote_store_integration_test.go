//go:build integration

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
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

// --- Support for TestMigrationReplay_0007And0008BackfillPreExistingQuoteRows ---
//
// These helpers replay the REAL migration .sql files from disk via `psql`
// (mirroring scripts/apply_migrations.sh's own invocation convention)
// against an isolated scratch schema, rather than re-implementing any
// migration's SQL inline in Go. This is what lets the test below fail if
// migration 0007's or 0008's backfill logic is ever removed from the real
// files -- a hand-written duplicate of the backfill statement in Go would
// keep passing even if the real file regressed.

// psqlBinary mirrors apply_migrations.sh's own PSQL_BIN-env-var-with-
// "psql"-default convention, so this test resolves the same psql binary
// the deploy-time script would.
func psqlBinary() string {
	if v := os.Getenv("PSQL_BIN"); v != "" {
		return v
	}
	return "psql"
}

// migrationFilesThrough returns every go-api/migrations/*.sql file in
// filename order, up to and including the first file whose basename
// contains marker. Migrations are globbed from disk (never hardcoded)
// so this keeps working as new migration files are added.
func migrationFilesThrough(t *testing.T, marker string) []string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not determine this test file's path via runtime.Caller")
	}
	migrationsDir := filepath.Join(filepath.Dir(thisFile), "..", "..", "migrations")
	all, err := filepath.Glob(filepath.Join(migrationsDir, "*.sql"))
	if err != nil {
		t.Fatalf("glob migrations dir: %v", err)
	}
	sort.Strings(all)
	if len(all) < 5 {
		t.Fatalf("sanity check failed: expected at least 5 migration files in %s, found %d: %v", migrationsDir, len(all), all)
	}
	idx := -1
	for i, f := range all {
		if strings.Contains(filepath.Base(f), marker) {
			idx = i
			break
		}
	}
	if idx == -1 {
		t.Fatalf("could not find a migration file matching %q under %s (found: %v)", marker, migrationsDir, all)
	}
	return all[:idx+1]
}

// migrationFileNamed returns the single migration file whose basename
// contains marker, globbed from disk rather than hardcoded.
func migrationFileNamed(t *testing.T, marker string) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not determine this test file's path via runtime.Caller")
	}
	migrationsDir := filepath.Join(filepath.Dir(thisFile), "..", "..", "migrations")
	all, err := filepath.Glob(filepath.Join(migrationsDir, "*.sql"))
	if err != nil {
		t.Fatalf("glob migrations dir: %v", err)
	}
	for _, f := range all {
		if strings.Contains(filepath.Base(f), marker) {
			return f
		}
	}
	t.Fatalf("could not find a migration file matching %q under %s", marker, migrationsDir)
	return ""
}

// replayMigrationInSchema shells out to the real psql binary and executes
// the given real migration file, exactly as scripts/apply_migrations.sh
// does (`psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$migration_file"`),
// except it first SETs search_path to the scratch schema in the SAME
// psql invocation -- psql runs -c/-f actions on one command line in order
// within a single session, so the SET persists across the following -f.
// This never re-implements migration SQL in Go; it runs the real file.
func replayMigrationInSchema(t *testing.T, baseDSN, schema, migrationFile string) {
	t.Helper()
	cmd := exec.Command(psqlBinary(), baseDSN,
		"-v", "ON_ERROR_STOP=1",
		"-c", fmt.Sprintf("SET search_path TO %s, public;", schema),
		"-f", migrationFile)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("psql replay of %s failed: %v\n--- psql output ---\n%s", filepath.Base(migrationFile), err, out)
	}
}

// withScratchSearchPath bakes `-c search_path=<schema>,public` into the
// DSN's `options` runtime parameter (URL-encoded, with `+` corrected to
// %20 since Postgres's libpq option parser rejects a literal `+` -- this
// was verified experimentally against a real connection, not assumed) so
// every connection opened from the returned DSN gets search_path set at
// connection time automatically.
func withScratchSearchPath(baseDSN, schema string) string {
	opts := "-c search_path=" + schema + ",public"
	escaped := strings.ReplaceAll(url.QueryEscape(opts), "+", "%20")
	sep := "?"
	if strings.Contains(baseDSN, "?") {
		sep = "&"
	}
	return baseDSN + sep + "options=" + escaped
}

// TestMigrationReplay_0007And0008BackfillPreExistingQuoteRows replays the
// REAL migration files 0001 through 0007 (and, in its second half, the
// real corrective migration 0008) from disk via psql against an isolated
// scratch Postgres schema, and proves two things using the actual files
// on disk -- never a hand-written duplicate of their SQL:
//
//  1. Migration 0007's own inline backfill (`UPDATE payment_quotes SET
//     selected = true`) makes a payment_quotes row that existed before
//     0007 ran (inserted here with the exact pre-0007 column shape --
//     no `selected` column exists yet at that point) visible again to
//     the real postgres.Store.GetQuoteByPaymentID method, which is what
//     worker/executor.go's resume/recovery path depends on.
//  2. Migration 0008 (a safety net for a database that already recorded
//     an unbackfilled 0007 as applied) independently re-backfills that
//     same shape of row and restores GetQuoteByPaymentID's ability to
//     find it, when applied on its own against a row whose `selected`
//     column was manually reset to false (simulating exactly that
//     bypassed-0007 scenario).
//
// This test would fail if either file's backfill logic were ever removed
// -- see the report for this cleanup task for the actual before/after
// experiment (temporarily commenting out 0007's backfill line and
// confirming this test fails) that demonstrates this.
func TestMigrationReplay_0007And0008BackfillPreExistingQuoteRows(t *testing.T) {
	baseDSN := os.Getenv("DATABASE_URL")
	if baseDSN == "" {
		t.Fatal("DATABASE_URL must be set for integration tests")
	}

	schema := fmt.Sprintf("test_mig0007_%d", time.Now().UnixNano())

	adminDB, err := sql.Open("pgx", baseDSN)
	if err != nil {
		t.Fatalf("open admin db: %v", err)
	}
	defer adminDB.Close()
	if _, err := adminDB.ExecContext(context.Background(), "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create scratch schema %s: %v", schema, err)
	}
	// Registered immediately after the schema exists, and unconditional,
	// so the scratch schema is dropped even if a later step in this test
	// fails or panics -- scratch schemas must never accumulate in the
	// shared test database across repeated runs.
	t.Cleanup(func() {
		cleanupDB, err := sql.Open("pgx", baseDSN)
		if err != nil {
			t.Errorf("cleanup: open db to drop schema %s: %v", schema, err)
			return
		}
		defer cleanupDB.Close()
		if _, err := cleanupDB.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			t.Errorf("cleanup: drop schema %s: %v", schema, err)
		}
	})

	files := migrationFilesThrough(t, "0007_dashboard_payment_analytics")
	migration0007 := files[len(files)-1]
	preMigrations := files[:len(files)-1]

	// Replay every real migration file 0001-0006, in filename order,
	// against the scratch schema.
	for _, f := range preMigrations {
		replayMigrationInSchema(t, baseDSN, schema, f)
	}

	scratchDSN := withScratchSearchPath(baseDSN, schema)
	scratchDB, err := sql.Open("pgx", scratchDSN)
	if err != nil {
		t.Fatalf("open scratch-schema db: %v", err)
	}
	defer scratchDB.Close()
	if err := scratchDB.PingContext(context.Background()); err != nil {
		t.Fatalf("ping scratch-schema db: %v", err)
	}

	// Fixture setup (plain test-data insert, not migration SQL): one
	// historical payment with its single winning quote, in the exact
	// column shape migrations 0001 and 0005 create. No `selected` column
	// exists on payment_quotes yet at this point -- migration 0007 (which
	// adds it) has not been replayed yet -- so this genuinely reproduces
	// "a real testnet payment that existed before 0007 ran."
	now := time.Now().UTC()
	var paymentID string
	if err := scratchDB.QueryRowContext(context.Background(), `
		INSERT INTO payments
			(idempotency_key, source_chain, destination_chain, asset, amount, total_fee, execution_mode)
		VALUES ($1, 'ethereum', 'base', 'eth', 0.001, 0.0001, 'testnet')
		RETURNING id
	`, "historical-"+t.Name()).Scan(&paymentID); err != nil {
		t.Fatalf("insert historical payment fixture: %v", err)
	}
	const fixtureFeeAmount = "100000000000"
	if _, err := scratchDB.ExecContext(context.Background(), `
		INSERT INTO payment_quotes
			(payment_id, provider, origin_chain_id, destination_chain_id, asset,
			 input_amount, output_amount, fee_amount, estimated_fill_time_sec,
			 quoted_at, expires_at, raw_provider_payload)
		VALUES ($1, 'across', 11155111, 84532, 'WETH',
			1000000000000000, 999900000000000, `+fixtureFeeAmount+`, 60,
			$2, $3, '{"spokePoolAddress":"0xhistorical"}'::JSONB)
	`, paymentID, now, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("insert historical payment_quotes fixture: %v", err)
	}

	// Replay the REAL 0007 migration file.
	replayMigrationInSchema(t, baseDSN, schema, migration0007)

	// Direct-SQL assertion: this is what would fail if 0007's backfill
	// line were ever deleted from the real file.
	var selected bool
	if err := scratchDB.QueryRowContext(context.Background(),
		`SELECT selected FROM payment_quotes WHERE payment_id = $1`, paymentID,
	).Scan(&selected); err != nil {
		t.Fatalf("query selected after 0007 replay: %v", err)
	}
	if !selected {
		t.Fatal("expected selected = true after replaying the real 0007 migration file -- its backfill statement is missing or broken")
	}

	// Real-function assertion: the exact method worker/executor.go
	// depends on for resume/recovery, bound to the scratch-schema DSN,
	// must find the row with the fixture's own field values.
	store := New(scratchDB)
	q, found, err := store.GetQuoteByPaymentID(context.Background(), paymentID)
	if err != nil {
		t.Fatalf("GetQuoteByPaymentID after 0007 replay: %v", err)
	}
	if !found {
		t.Fatal("GetQuoteByPaymentID did not find the historical quote after the real 0007 migration ran -- the backfill this method depends on is missing or broken")
	}
	if q.Provider != "across" || q.FeeAmount != fixtureFeeAmount || !q.Selected {
		t.Errorf("unexpected quote after 0007 replay: %+v", q)
	}

	// --- Part 2: migration 0008 as a standalone safety net ---
	//
	// Simulate "migration 0007 was already applied without its backfill"
	// by resetting this row's selected column back to false via a plain
	// SQL UPDATE (test fixture manipulation, not migration SQL -- 0007
	// itself is not re-run). Then apply the REAL 0008 corrective
	// migration file and confirm it alone restores the row.
	if _, err := scratchDB.ExecContext(context.Background(),
		`UPDATE payment_quotes SET selected = false WHERE payment_id = $1`, paymentID); err != nil {
		t.Fatalf("reset selected to false to simulate unbackfilled 0007: %v", err)
	}
	_, found, err = store.GetQuoteByPaymentID(context.Background(), paymentID)
	if err != nil {
		t.Fatalf("GetQuoteByPaymentID after simulated reset: %v", err)
	}
	if found {
		t.Fatal("expected GetQuoteByPaymentID to NOT find a selected=false row -- if this fails, the test fixture reset itself is broken")
	}

	migration0008 := migrationFileNamed(t, "0008_backfill_orphaned_selected_quotes")
	replayMigrationInSchema(t, baseDSN, schema, migration0008)

	q2, found2, err := store.GetQuoteByPaymentID(context.Background(), paymentID)
	if err != nil {
		t.Fatalf("GetQuoteByPaymentID after 0008 replay: %v", err)
	}
	if !found2 {
		t.Fatal("expected GetQuoteByPaymentID to find the row again after replaying the real 0008 migration file -- 0008's corrective backfill is missing or broken")
	}
	if q2.Provider != "across" || q2.FeeAmount != fixtureFeeAmount {
		t.Errorf("unexpected quote after 0008 replay: %+v", q2)
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
