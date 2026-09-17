//go:build integration

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"chainroute/go-api/internal/payment"
)

func insertRawTestnetPayment(t *testing.T, s *Store, idempotencyKey string) string {
	t.Helper()
	var id string
	err := s.db.QueryRowContext(context.Background(), `
		INSERT INTO payments (idempotency_key, source_chain, destination_chain, asset, amount, total_fee, execution_mode, status)
		VALUES ($1, 'ethereum', 'base', 'ETH', '0.001', 0, 'testnet', 'PROCESSING')
		RETURNING id
	`, idempotencyKey).Scan(&id)
	if err != nil {
		t.Fatalf("insert raw testnet payment: %v", err)
	}
	return id
}

func TestTryCreateExecution_ExecutorReconcilerRaceIsSafe(t *testing.T) {
	s := newTestStore(t)
	key := "test-race-key"
	wallet := "0xRaceTestWallet00000000000000000000001"

	cleanup := func() {
		s.db.ExecContext(context.Background(), `DELETE FROM payments WHERE idempotency_key = $1`, key)
		s.db.ExecContext(context.Background(), `DELETE FROM wallet_nonces WHERE wallet_address = $1`, wallet)
	}
	cleanup()
	t.Cleanup(cleanup)

	paymentID := insertRawTestnetPayment(t, s, key)
	if err := s.SeedWalletNonce(context.Background(), wallet, 5); err != nil {
		t.Fatalf("seed nonce: %v", err)
	}

	params := CreateExecutionParams{
		PaymentID: paymentID, WalletAddress: wallet, BridgeProvider: "across",
		OriginChainID: 11155111, DestinationChainID: 84532,
	}

	const attempts = 2
	var wg sync.WaitGroup
	created := make([]bool, attempts)
	errs := make([]error, attempts)
	wg.Add(attempts)
	for i := 0; i < attempts; i++ {
		i := i
		go func() {
			defer wg.Done()
			_, c, err := s.TryCreateExecution(context.Background(), params)
			created[i] = c
			errs[i] = err
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("attempt %d: unexpected error: %v", i, err)
		}
	}
	winners := 0
	for _, c := range created {
		if c {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("expected exactly 1 winner among %d concurrent attempts, got %d", attempts, winners)
	}

	var rowCount int
	if err := s.db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM payment_executions WHERE payment_id = $1`, paymentID).Scan(&rowCount); err != nil {
		t.Fatalf("count executions: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("expected exactly 1 payment_executions row, got %d", rowCount)
	}

	var nextNonce int64
	if err := s.db.QueryRowContext(context.Background(),
		`SELECT next_nonce FROM wallet_nonces WHERE wallet_address = $1`, wallet).Scan(&nextNonce); err != nil {
		t.Fatalf("read next_nonce: %v", err)
	}
	if nextNonce != 6 {
		t.Fatalf("expected next_nonce to have advanced by exactly 1 (seeded at 5, expected 6), got %d -- "+
			"a lost race must roll back its nonce allocation, not merely leave it unused", nextNonce)
	}
}

func TestTryCreateExecution_SecondCallForSamePaymentIsSafeNoOp(t *testing.T) {
	s := newTestStore(t)
	key := "test-second-call-key"
	wallet := "0xSecondCallWallet0000000000000000000002"
	cleanup := func() {
		s.db.ExecContext(context.Background(), `DELETE FROM payments WHERE idempotency_key = $1`, key)
		s.db.ExecContext(context.Background(), `DELETE FROM wallet_nonces WHERE wallet_address = $1`, wallet)
	}
	cleanup()
	t.Cleanup(cleanup)

	paymentID := insertRawTestnetPayment(t, s, key)
	if err := s.SeedWalletNonce(context.Background(), wallet, 0); err != nil {
		t.Fatalf("seed: %v", err)
	}
	params := CreateExecutionParams{PaymentID: paymentID, WalletAddress: wallet, BridgeProvider: "across", OriginChainID: 11155111, DestinationChainID: 84532}

	first, created1, err := s.TryCreateExecution(context.Background(), params)
	if err != nil || !created1 {
		t.Fatalf("first call: created=%v err=%v", created1, err)
	}
	second, created2, err := s.TryCreateExecution(context.Background(), params)
	if err != nil {
		t.Fatalf("second call: unexpected error: %v", err)
	}
	if created2 {
		t.Fatal("second call for the same payment must report created=false, not create a second row")
	}
	_ = second
	if first.Nonce != 0 {
		t.Fatalf("expected first execution to hold nonce 0, got %d", first.Nonce)
	}
}

func TestPersistSignedExecution_AndMarkBroadcast(t *testing.T) {
	s := newTestStore(t)
	key, wallet := "test-persist-signed-key", "0xPersistWallet000000000000000000000003"
	cleanup := func() {
		s.db.ExecContext(context.Background(), `DELETE FROM payments WHERE idempotency_key = $1`, key)
		s.db.ExecContext(context.Background(), `DELETE FROM wallet_nonces WHERE wallet_address = $1`, wallet)
	}
	cleanup()
	t.Cleanup(cleanup)

	paymentID := insertRawTestnetPayment(t, s, key)
	s.SeedWalletNonce(context.Background(), wallet, 0)
	exec, created, err := s.TryCreateExecution(context.Background(), CreateExecutionParams{
		PaymentID: paymentID, WalletAddress: wallet, BridgeProvider: "across", OriginChainID: 11155111, DestinationChainID: 84532,
	})
	if err != nil || !created {
		t.Fatalf("setup: created=%v err=%v", created, err)
	}

	if err := s.PersistSignedExecution(context.Background(), exec.ID, []byte{0x01, 0x02}, "0xdeadbeef", nil); err != nil {
		t.Fatalf("persist signed: %v", err)
	}
	got, found, err := s.GetExecutionByPaymentID(context.Background(), paymentID)
	if err != nil || !found {
		t.Fatalf("get: found=%v err=%v", found, err)
	}
	if got.SignedTxHash == nil || *got.SignedTxHash != "0xdeadbeef" {
		t.Fatalf("expected signed_tx_hash to be persisted, got %v", got.SignedTxHash)
	}
	if got.BroadcastAt != nil {
		t.Fatal("expected broadcast_at to still be NULL before MarkExecutionBroadcast")
	}

	if err := s.MarkExecutionBroadcast(context.Background(), exec.ID); err != nil {
		t.Fatalf("mark broadcast: %v", err)
	}
	got, _, _ = s.GetExecutionByPaymentID(context.Background(), paymentID)
	if got.BroadcastAt == nil {
		t.Fatal("expected broadcast_at to be set after MarkExecutionBroadcast")
	}
}

func TestReconciliationCandidates_TwoClauseSelection(t *testing.T) {
	s := newTestStore(t)
	keyPending, keyStale, wallet := "test-recon-pending-key", "test-recon-stale-key", "0xReconWallet00000000000000000000000004"
	cleanup := func() {
		s.db.ExecContext(context.Background(), `DELETE FROM payments WHERE idempotency_key IN ($1, $2)`, keyPending, keyStale)
		s.db.ExecContext(context.Background(), `DELETE FROM wallet_nonces WHERE wallet_address = $1`, wallet)
	}
	cleanup()
	t.Cleanup(cleanup)
	s.SeedWalletNonce(context.Background(), wallet, 0)

	// Row A: broadcast, unconfirmed -- must ALWAYS be a candidate.
	idA := insertRawTestnetPayment(t, s, keyPending)
	execA, _, _ := s.TryCreateExecution(context.Background(), CreateExecutionParams{PaymentID: idA, WalletAddress: wallet, BridgeProvider: "across", OriginChainID: 11155111, DestinationChainID: 84532})
	s.PersistSignedExecution(context.Background(), execA.ID, []byte{0x01}, "0xaaa", nil)
	s.MarkExecutionBroadcast(context.Background(), execA.ID)

	// Row B: not yet broadcast, freshly created -- must NOT be a candidate yet.
	idB := insertRawTestnetPayment(t, s, keyStale)
	execB, _, _ := s.TryCreateExecution(context.Background(), CreateExecutionParams{PaymentID: idB, WalletAddress: wallet, BridgeProvider: "across", OriginChainID: 11155111, DestinationChainID: 84532})

	candidates, err := s.ReconciliationCandidates(context.Background(), time.Hour)
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	var sawA, sawB bool
	for _, c := range candidates {
		if c.ID == execA.ID {
			sawA = true
		}
		if c.ID == execB.ID {
			sawB = true
		}
	}
	if !sawA {
		t.Fatal("expected the broadcast-unconfirmed row to be a candidate on every tick")
	}
	if sawB {
		t.Fatal("expected the freshly-created, not-yet-broadcast row to NOT be a candidate before it goes stale")
	}

	// Now backdate row B past staleness and confirm it becomes a candidate.
	s.db.ExecContext(context.Background(), `UPDATE payment_executions SET updated_at = now() - interval '1 hour' WHERE id = $1`, execB.ID)
	candidates, err = s.ReconciliationCandidates(context.Background(), time.Minute)
	if err != nil {
		t.Fatalf("candidates after backdate: %v", err)
	}
	sawB = false
	for _, c := range candidates {
		if c.ID == execB.ID {
			sawB = true
		}
	}
	if !sawB {
		t.Fatal("expected the stale not-yet-broadcast row to become a candidate")
	}
}

func TestStaleTestnetProcessingWithoutExecutionIDs(t *testing.T) {
	s := newTestStore(t)
	key := "test-stale-no-exec-key"
	cleanup := func() { s.db.ExecContext(context.Background(), `DELETE FROM payments WHERE idempotency_key = $1`, key) }
	cleanup()
	t.Cleanup(cleanup)

	id := insertRawTestnetPayment(t, s, key)
	s.db.ExecContext(context.Background(), `UPDATE payments SET updated_at = now() - interval '1 hour' WHERE id = $1`, id)

	ids, err := s.StaleTestnetProcessingWithoutExecutionIDs(context.Background(), time.Minute)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	found := false
	for _, got := range ids {
		if got == id {
			found = true
		}
	}
	if !found {
		t.Fatal("expected the stale, execution-less testnet payment to be returned")
	}
}

func TestMarkSubmitted_AndCompleteSubmittedPayment(t *testing.T) {
	s := newTestStore(t)
	key := "test-mark-submitted-key"
	cleanup := func() { s.db.ExecContext(context.Background(), `DELETE FROM payments WHERE idempotency_key = $1`, key) }
	cleanup()
	t.Cleanup(cleanup)

	id := insertRawTestnetPayment(t, s, key)

	submitted, err := s.MarkSubmitted(context.Background(), id)
	if err != nil {
		t.Fatalf("mark submitted: %v", err)
	}
	if !submitted {
		t.Fatal("expected MarkSubmitted to succeed for a PROCESSING payment")
	}

	// A second call must be a safe no-op: the payment is no longer PROCESSING.
	submittedAgain, err := s.MarkSubmitted(context.Background(), id)
	if err != nil {
		t.Fatalf("mark submitted again: %v", err)
	}
	if submittedAgain {
		t.Fatal("expected second MarkSubmitted call to report submitted=false")
	}

	completed, createdAt, err := s.CompleteSubmittedPayment(context.Background(), id, payment.StatusCompleted)
	if err != nil {
		t.Fatalf("complete submitted payment: %v", err)
	}
	if !completed {
		t.Fatal("expected CompleteSubmittedPayment to succeed for a SUBMITTED payment")
	}
	if createdAt.IsZero() {
		t.Fatal("expected CompleteSubmittedPayment to return a non-zero created_at")
	}

	var status string
	if err := s.db.QueryRowContext(context.Background(), `SELECT status FROM payments WHERE id = $1`, id).Scan(&status); err != nil {
		t.Fatalf("read status: %v", err)
	}
	if status != string(payment.StatusCompleted) {
		t.Fatalf("expected status COMPLETED, got %s", status)
	}

	// A second call must be a safe no-op: the payment is no longer SUBMITTED.
	completedAgain, _, err := s.CompleteSubmittedPayment(context.Background(), id, payment.StatusCompleted)
	if err != nil {
		t.Fatalf("complete submitted payment again: %v", err)
	}
	if completedAgain {
		t.Fatal("expected second CompleteSubmittedPayment call to report completed=false")
	}
}

func TestUpdateExecutionExternalStatus(t *testing.T) {
	s := newTestStore(t)
	key, wallet := "test-update-status-key", "0xUpdateStatusWallet0000000000000000005"
	cleanup := func() {
		s.db.ExecContext(context.Background(), `DELETE FROM payments WHERE idempotency_key = $1`, key)
		s.db.ExecContext(context.Background(), `DELETE FROM wallet_nonces WHERE wallet_address = $1`, wallet)
	}
	cleanup()
	t.Cleanup(cleanup)

	paymentID := insertRawTestnetPayment(t, s, key)
	s.SeedWalletNonce(context.Background(), wallet, 0)
	exec, created, err := s.TryCreateExecution(context.Background(), CreateExecutionParams{
		PaymentID: paymentID, WalletAddress: wallet, BridgeProvider: "across", OriginChainID: 11155111, DestinationChainID: 84532,
	})
	if err != nil || !created {
		t.Fatalf("setup: created=%v err=%v", created, err)
	}

	// nil confirmedAt: still pending, no confirmed_at set.
	if err := s.UpdateExecutionExternalStatus(context.Background(), exec.ID, payment.ExternalStatusPending, "", nil); err != nil {
		t.Fatalf("update status (pending): %v", err)
	}
	got, _, err := s.GetExecutionByPaymentID(context.Background(), paymentID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ExternalStatus != payment.ExternalStatusPending {
		t.Fatalf("expected pending, got %s", got.ExternalStatus)
	}
	if got.ConfirmedAt != nil {
		t.Fatal("expected confirmed_at to remain NULL when nil is passed")
	}

	// A real time: terminal, confirmed_at set.
	now := time.Now().UTC().Truncate(time.Second)
	if err := s.UpdateExecutionExternalStatus(context.Background(), exec.ID, payment.ExternalStatusFilled, "", &sql.NullTime{Time: now, Valid: true}); err != nil {
		t.Fatalf("update status (filled): %v", err)
	}
	got, _, err = s.GetExecutionByPaymentID(context.Background(), paymentID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ExternalStatus != payment.ExternalStatusFilled {
		t.Fatalf("expected filled, got %s", got.ExternalStatus)
	}
	if got.ConfirmedAt == nil {
		t.Fatal("expected confirmed_at to be set after passing a real time")
	}
}

func TestLowestUnconfirmedNonce(t *testing.T) {
	s := newTestStore(t)
	keyA, keyB, wallet := "test-lowest-nonce-a-key", "test-lowest-nonce-b-key", "0xLowestNonceWallet0000000000000000006"
	cleanup := func() {
		s.db.ExecContext(context.Background(), `DELETE FROM payments WHERE idempotency_key IN ($1, $2)`, keyA, keyB)
		s.db.ExecContext(context.Background(), `DELETE FROM wallet_nonces WHERE wallet_address = $1`, wallet)
	}
	cleanup()
	t.Cleanup(cleanup)

	// No executions yet for this wallet.
	_, found, err := s.LowestUnconfirmedNonce(context.Background(), wallet)
	if err != nil {
		t.Fatalf("lowest nonce (empty): %v", err)
	}
	if found {
		t.Fatal("expected found=false when the wallet has no executions")
	}

	s.SeedWalletNonce(context.Background(), wallet, 0)
	idA := insertRawTestnetPayment(t, s, keyA)
	idB := insertRawTestnetPayment(t, s, keyB)
	execA, _, _ := s.TryCreateExecution(context.Background(), CreateExecutionParams{PaymentID: idA, WalletAddress: wallet, BridgeProvider: "across", OriginChainID: 11155111, DestinationChainID: 84532})
	_, _, _ = s.TryCreateExecution(context.Background(), CreateExecutionParams{PaymentID: idB, WalletAddress: wallet, BridgeProvider: "across", OriginChainID: 11155111, DestinationChainID: 84532})

	nonce, found, err := s.LowestUnconfirmedNonce(context.Background(), wallet)
	if err != nil {
		t.Fatalf("lowest nonce: %v", err)
	}
	if !found || nonce != execA.Nonce {
		t.Fatalf("expected lowest unconfirmed nonce %d, got %d (found=%v)", execA.Nonce, nonce, found)
	}

	// Confirm the lowest-nonce execution; the lowest unconfirmed nonce must advance.
	if err := s.UpdateExecutionExternalStatus(context.Background(), execA.ID, payment.ExternalStatusFilled, "", &sql.NullTime{Time: time.Now(), Valid: true}); err != nil {
		t.Fatalf("confirm execA: %v", err)
	}
	nonce, found, err = s.LowestUnconfirmedNonce(context.Background(), wallet)
	if err != nil {
		t.Fatalf("lowest nonce after confirm: %v", err)
	}
	if !found || nonce != execA.Nonce+1 {
		t.Fatalf("expected lowest unconfirmed nonce to advance to %d, got %d (found=%v)", execA.Nonce+1, nonce, found)
	}
}

// TestTryCreateExecution_NonceUniqueAcrossAcrossAndRelayExecutions proves
// wallet_nonces allocates a strictly-unique sequence across concurrent
// executions regardless of which provider each execution belongs to
// (design doc §15/§24: nonce allocation is provider-agnostic -- both
// Across and Relay executions draw from the same per-wallet sequence).
//
// payment_executions.payment_id has REFERENCES payments(id) ON DELETE
// CASCADE (migration 0004, confirmed by reading the schema directly), so
// unlike the brief's original sketch, each goroutine below first inserts
// a minimal real payments row via insertRawTestnetPayment -- a synthetic,
// non-existent payment ID would be rejected by the foreign key before
// TryCreateExecution's nonce/race behavior is even exercised.
func TestTryCreateExecution_NonceUniqueAcrossAcrossAndRelayExecutions(t *testing.T) {
	s := newTestStore(t)
	wallet := "0xCrossProviderNonceTest" + t.Name()

	const n = 10
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("test-cross-provider-nonce-%s-%d", t.Name(), i)
	}
	cleanup := func() {
		for _, key := range keys {
			s.db.ExecContext(context.Background(), `DELETE FROM payments WHERE idempotency_key = $1`, key)
		}
		s.db.ExecContext(context.Background(), `DELETE FROM wallet_nonces WHERE wallet_address = $1`, wallet)
	}
	cleanup()
	t.Cleanup(cleanup)

	if err := s.SeedWalletNonce(context.Background(), wallet, 0); err != nil {
		t.Fatalf("seed: %v", err)
	}

	paymentIDs := make([]string, n)
	for i := 0; i < n; i++ {
		paymentIDs[i] = insertRawTestnetPayment(t, s, keys[i])
	}

	var wg sync.WaitGroup
	nonces := make(chan int64, n)
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		provider := "across"
		if i%2 == 0 {
			provider = "relay"
		}
		go func(paymentID, provider string) {
			defer wg.Done()
			exec, created, err := s.TryCreateExecution(context.Background(), CreateExecutionParams{
				PaymentID: paymentID, WalletAddress: wallet, BridgeProvider: provider, OriginChainID: 11155111, DestinationChainID: 84532,
			})
			if err != nil {
				errs <- err
				return
			}
			if !created {
				errs <- fmt.Errorf("payment %s: expected created=true", paymentID)
				return
			}
			nonces <- exec.Nonce
		}(paymentIDs[i], provider)
	}
	wg.Wait()
	close(nonces)
	close(errs)
	for err := range errs {
		t.Fatalf("unexpected error: %v", err)
	}
	seen := map[int64]bool{}
	for nonce := range nonces {
		if seen[nonce] {
			t.Fatalf("nonce %d allocated twice", nonce)
		}
		seen[nonce] = true
	}
	if len(seen) != n {
		t.Fatalf("expected %d unique nonces, got %d", n, len(seen))
	}
}

// TestPersistSignedExecution_PersistsProviderReferenceIDAtomicallyWithSignedBytes
// proves PersistSignedExecution's single UPDATE lands SignedTxHash and
// ProviderReferenceID together -- there is no intermediate durable state
// where one is set without the other (relevant to Relay executions, whose
// provider_reference_id carries Relay's requestId; Across executions pass
// nil here and are covered by the pre-existing
// TestPersistSignedExecution_AndMarkBroadcast above).
func TestPersistSignedExecution_PersistsProviderReferenceIDAtomicallyWithSignedBytes(t *testing.T) {
	s := newTestStore(t)
	key, wallet := "test-persist-provider-ref-key", "0xPersistProviderRefWallet00000000000007"
	cleanup := func() {
		s.db.ExecContext(context.Background(), `DELETE FROM payments WHERE idempotency_key = $1`, key)
		s.db.ExecContext(context.Background(), `DELETE FROM wallet_nonces WHERE wallet_address = $1`, wallet)
	}
	cleanup()
	t.Cleanup(cleanup)

	paymentID := insertRawTestnetPayment(t, s, key)
	if err := s.SeedWalletNonce(context.Background(), wallet, 0); err != nil {
		t.Fatalf("seed: %v", err)
	}
	exec, created, err := s.TryCreateExecution(context.Background(), CreateExecutionParams{
		PaymentID: paymentID, WalletAddress: wallet, BridgeProvider: "relay", OriginChainID: 11155111, DestinationChainID: 84532,
	})
	if err != nil || !created {
		t.Fatalf("setup: created=%v err=%v", created, err)
	}

	if err := s.PersistSignedExecution(context.Background(), exec.ID, []byte{0x01, 0x02, 0x03}, "0xrelaysignedtx", strPtr("0xrelayrequestid123")); err != nil {
		t.Fatalf("persist signed: %v", err)
	}

	got, found, err := s.GetExecutionByPaymentID(context.Background(), paymentID)
	if err != nil || !found {
		t.Fatalf("get: found=%v err=%v", found, err)
	}
	if got.SignedTxHash == nil || *got.SignedTxHash != "0xrelaysignedtx" {
		t.Fatalf("expected SignedTxHash to be persisted, got %v", got.SignedTxHash)
	}
	if got.ProviderReferenceID == nil || *got.ProviderReferenceID != "0xrelayrequestid123" {
		t.Fatalf("expected ProviderReferenceID to be persisted atomically with SignedTxHash, got %v", got.ProviderReferenceID)
	}
}
