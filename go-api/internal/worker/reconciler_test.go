package worker

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"chainroute/go-api/internal/payment"
)

type fakeReconcilerStore struct {
	fakeExecutorStore
	staleNoExecIDs    []string
	candidates        []payment.Execution
	getExecByPayment  map[string]payment.Execution
	updateStatusCalls []payment.ExternalStatus
	completeCalls     []payment.Status
	lowestNonce       int64
	lowestNonceFound  bool
}

func (f *fakeReconcilerStore) GetExecutionByPaymentID(ctx context.Context, paymentID string) (payment.Execution, bool, error) {
	e, ok := f.getExecByPayment[paymentID]
	return e, ok, nil
}
func (f *fakeReconcilerStore) StaleTestnetProcessingWithoutExecutionIDs(ctx context.Context, staleness time.Duration) ([]string, error) {
	return f.staleNoExecIDs, nil
}
func (f *fakeReconcilerStore) ReconciliationCandidates(ctx context.Context, staleness time.Duration) ([]payment.Execution, error) {
	return f.candidates, nil
}
func (f *fakeReconcilerStore) UpdateExecutionExternalStatus(ctx context.Context, executionID string, status payment.ExternalStatus, confirmedAt *sql.NullTime) error {
	f.updateStatusCalls = append(f.updateStatusCalls, status)
	return nil
}
func (f *fakeReconcilerStore) CompleteSubmittedPayment(ctx context.Context, paymentID string, terminal payment.Status) (bool, error) {
	f.completeCalls = append(f.completeCalls, terminal)
	return true, nil
}
func (f *fakeReconcilerStore) LowestUnconfirmedNonce(ctx context.Context, walletAddress string) (int64, bool, error) {
	return f.lowestNonce, f.lowestNonceFound, nil
}

type fakeReconcilerEthClient struct {
	fakeExecutorEthClient
	receipts     map[common.Hash]*types.Receipt
	pendingNonce uint64

	// queriedHashes records every hash TransactionReceipt was called with,
	// in call order -- strengthens TestSweepOnce_LowestNonceFirstOnly by
	// letting it assert exactly which execution's hash was (and wasn't)
	// looked up, rather than merely that nothing panicked.
	queriedHashes []common.Hash
}

func (f *fakeReconcilerEthClient) TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	f.queriedHashes = append(f.queriedHashes, txHash)
	r, ok := f.receipts[txHash]
	if !ok {
		return nil, sql.ErrNoRows // stand-in "not found yet" error
	}
	return r, nil
}
func (f *fakeReconcilerEthClient) PendingNonceAt(ctx context.Context, account common.Address) (uint64, error) {
	return f.pendingNonce, nil
}

func TestSweepOnce_LowestNonceFirstOnly(t *testing.T) {
	wallet := "0xLowestNonceWallet00000000000000000005"
	hashLow := "0x1111111111111111111111111111111111111111111111111111111111111111"
	hashHigh := "0x2222222222222222222222222222222222222222222222222222222222222222"
	store := &fakeReconcilerStore{
		candidates: []payment.Execution{
			{ID: "exec-low", PaymentID: "pay-low", WalletAddress: wallet, Nonce: 5, SignedTxHash: strPtr(hashLow), BroadcastAt: timePtr()},
			{ID: "exec-high", PaymentID: "pay-high", WalletAddress: wallet, Nonce: 6, SignedTxHash: strPtr(hashHigh), BroadcastAt: timePtr()},
		},
	}
	ethClient := &fakeReconcilerEthClient{receipts: map[common.Hash]*types.Receipt{}}
	r := &Reconciler{Store: store, OriginClient: ethClient, Across: newTestAcrossServer(t), OriginChainID: 11155111, Staleness: time.Hour}

	r.checkBroadcastOutcomes(context.Background())

	// Strengthened per the task brief: assert exactly which hash was
	// queried, not just that nothing panicked. Only the lowest nonce (5)
	// may ever reach TransactionReceipt -- checking or rebroadcasting a
	// higher nonce cannot progress it while a lower one is unresolved.
	if len(ethClient.queriedHashes) != 1 {
		t.Fatalf("expected exactly one TransactionReceipt query, got %d: %v", len(ethClient.queriedHashes), ethClient.queriedHashes)
	}
	if ethClient.queriedHashes[0] != common.HexToHash(hashLow) {
		t.Fatalf("expected the lowest-nonce execution's hash (%s) to be queried, got %s", hashLow, ethClient.queriedHashes[0].Hex())
	}
	for _, h := range ethClient.queriedHashes {
		if h == common.HexToHash(hashHigh) {
			t.Fatalf("the higher-nonce execution's hash must never be queried while the lower nonce is unresolved")
		}
	}
}

func TestCheckAndUpdateOutcome_RevertedTransactionFailsPayment(t *testing.T) {
	hash := "0x3333333333333333333333333333333333333333333333333333333333333333"
	store := &fakeReconcilerStore{}
	ethClient := &fakeReconcilerEthClient{receipts: map[common.Hash]*types.Receipt{
		common.HexToHash(hash): {Status: 0},
	}}
	r := &Reconciler{Store: store, OriginClient: ethClient, Across: newTestAcrossServer(t), OriginChainID: 11155111, Staleness: time.Hour}

	exec := payment.Execution{ID: "exec-1", PaymentID: "pay-1", SignedTxHash: &hash}
	if err := r.checkAndUpdateOutcome(context.Background(), exec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(store.updateStatusCalls) != 1 || store.updateStatusCalls[0] != payment.ExternalStatusReverted {
		t.Fatalf("expected a single ExternalStatusReverted update, got %v", store.updateStatusCalls)
	}
	if len(store.completeCalls) != 1 || store.completeCalls[0] != payment.StatusFailed {
		t.Fatalf("expected a single StatusFailed completion, got %v", store.completeCalls)
	}
}

func TestCheckAndUpdateOutcome_TransientPollFailureDoesNotFailPayment(t *testing.T) {
	hash := "0x4444444444444444444444444444444444444444444444444444444444444444"
	store := &fakeReconcilerStore{}
	ethClient := &fakeReconcilerEthClient{receipts: map[common.Hash]*types.Receipt{}} // no receipt yet -- "not mined"
	r := &Reconciler{Store: store, OriginClient: ethClient, Across: newTestAcrossServer(t), OriginChainID: 11155111, Staleness: time.Hour}

	exec := payment.Execution{ID: "exec-2", PaymentID: "pay-2", SignedTxHash: &hash}
	if err := r.checkAndUpdateOutcome(context.Background(), exec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(store.completeCalls) != 0 {
		t.Fatal("a transient/not-yet-mined poll must never produce a terminal transition")
	}
}

// TestCheckAndUpdateOutcome_AcrossStatusSwitch closes the coverage gap the
// Step-4 mutation check exposed: no test anywhere previously supplied a
// mined/successful receipt (Status: 1), so execution never reached the
// Across-status switch at all -- "filled"/"expired"/"refunded"/"pending"/
// default were all completely unguarded. Each case stubs a mined receipt
// (so the function proceeds past the receipt-status gate) plus a specific
// Across /deposit/status response body via the existing
// newTestAcrossServerWithBody fixture (executor_test.go), and asserts the
// exact sequence of store calls -- not just "no error returned" -- since a
// wrong terminal write with a nil error is exactly the failure mode this
// task exists to prevent (design spec §13).
func TestCheckAndUpdateOutcome_AcrossStatusSwitch(t *testing.T) {
	tests := []struct {
		name               string
		acrossStatusBody   string
		wantUpdateStatuses []payment.ExternalStatus
		wantCompleteCalls  []payment.Status
	}{
		{
			name:             "pending status produces no terminal write",
			acrossStatusBody: `{"status":"pending"}`,
		},
		{
			name:               "filled status completes the payment",
			acrossStatusBody:   `{"status":"filled"}`,
			wantUpdateStatuses: []payment.ExternalStatus{payment.ExternalStatusFilled},
			wantCompleteCalls:  []payment.Status{payment.StatusCompleted},
		},
		{
			name:               "expired status fails the payment",
			acrossStatusBody:   `{"status":"expired"}`,
			wantUpdateStatuses: []payment.ExternalStatus{payment.ExternalStatusExpired},
			wantCompleteCalls:  []payment.Status{payment.StatusFailed},
		},
		{
			name:               "refunded status fails the payment",
			acrossStatusBody:   `{"status":"refunded"}`,
			wantUpdateStatuses: []payment.ExternalStatus{payment.ExternalStatusRefunded},
			wantCompleteCalls:  []payment.Status{payment.StatusFailed},
		},
		{
			name:             "unrecognized status produces no terminal write",
			acrossStatusBody: `{"status":"slowFillRequested"}`,
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// A distinct, valid 32-byte hash per subtest -- decimal digits
			// are valid hex digits, so this is a legitimate common.Hash
			// once run through common.HexToHash.
			hash := fmt.Sprintf("0x%064d", i+10)
			store := &fakeReconcilerStore{}
			ethClient := &fakeReconcilerEthClient{receipts: map[common.Hash]*types.Receipt{
				common.HexToHash(hash): {Status: 1}, // mined AND successful -- required to reach the Across switch
			}}
			r := &Reconciler{
				Store: store, OriginClient: ethClient,
				Across:        newTestAcrossServerWithBody(t, tc.acrossStatusBody),
				OriginChainID: 11155111, Staleness: time.Hour,
			}

			exec := payment.Execution{ID: "exec-switch", PaymentID: "pay-switch", SignedTxHash: &hash}
			if err := r.checkAndUpdateOutcome(context.Background(), exec); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !reflect.DeepEqual(store.updateStatusCalls, tc.wantUpdateStatuses) {
				t.Fatalf("UpdateExecutionExternalStatus calls: got %v, want %v", store.updateStatusCalls, tc.wantUpdateStatuses)
			}
			if !reflect.DeepEqual(store.completeCalls, tc.wantCompleteCalls) {
				t.Fatalf("CompleteSubmittedPayment calls: got %v, want %v", store.completeCalls, tc.wantCompleteCalls)
			}
		})
	}
}

func TestNonceDivergence_LogsButNeverWrites(t *testing.T) {
	store := &fakeReconcilerStore{lowestNonce: 10, lowestNonceFound: true}
	ethClient := &fakeReconcilerEthClient{pendingNonce: 5} // behind -- divergence
	r := &Reconciler{Store: store, OriginClient: ethClient, WalletAddress: common.HexToAddress("0xabc")}

	r.checkNonceDivergence(context.Background()) // must not panic; ReconcilerStore has no write method this could call
}

func strPtr(s string) *string { return &s }
func timePtr() *time.Time     { t := time.Now(); return &t }
