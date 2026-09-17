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
	"github.com/prometheus/client_golang/prometheus/testutil"

	"chainroute/go-api/internal/bridge/quote"
	"chainroute/go-api/internal/observability"
	"chainroute/go-api/internal/payment"
)

type fakeReconcilerStore struct {
	fakeExecutorStore
	staleNoExecIDs       []string
	candidates           []payment.Execution
	getExecByPayment     map[string]payment.Execution
	updateStatusCalls    []payment.ExternalStatus
	updateRawStatusCalls []string
	completeCalls        []payment.Status
	lowestNonce          int64
	lowestNonceFound     bool

	// completeSubmittedFails, when true, makes CompleteSubmittedPayment
	// report completed=false (as if its own WHERE status = 'SUBMITTED'
	// guard affected no rows) instead of the default success. Named so its
	// Go zero value (false) preserves every existing test's behavior
	// unmodified -- the fake still returns (true, nil) by default.
	completeSubmittedFails bool
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
func (f *fakeReconcilerStore) UpdateExecutionExternalStatus(ctx context.Context, executionID string, status payment.ExternalStatus, rawStatus string, confirmedAt *sql.NullTime) error {
	f.updateStatusCalls = append(f.updateStatusCalls, status)
	f.updateRawStatusCalls = append(f.updateRawStatusCalls, rawStatus)
	return nil
}
func (f *fakeReconcilerStore) CompleteSubmittedPayment(ctx context.Context, paymentID string, terminal payment.Status) (bool, time.Time, error) {
	f.completeCalls = append(f.completeCalls, terminal)
	if f.completeSubmittedFails {
		return false, time.Time{}, nil
	}
	return true, time.Now(), nil
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

// fakeStatusChecker is a configurable quote.StatusChecker double: CheckStatus
// returns a preset result/error and records that it was called (and with
// what request), so tests can assert exactly which provider's checker was
// invoked -- never GetQuote, which the reconciler has no reason to call and
// which panics here if it ever is.
type fakeStatusChecker struct {
	name        string
	checkResult quote.StatusResult
	checkErr    error
	checkCalled bool
	checkedReq  quote.StatusRequest
}

func (f *fakeStatusChecker) Name() string { return f.name }

func (f *fakeStatusChecker) GetQuote(ctx context.Context, req quote.Request) (quote.Quote, error) {
	panic("fakeStatusChecker.GetQuote should never be called by the reconciler")
}

func (f *fakeStatusChecker) CheckStatus(ctx context.Context, req quote.StatusRequest) (quote.StatusResult, error) {
	f.checkCalled = true
	f.checkedReq = req
	return f.checkResult, f.checkErr
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
	r := &Reconciler{Store: store, OriginClient: ethClient, OriginChainID: 11155111, Staleness: time.Hour}

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
	r := &Reconciler{Store: store, OriginClient: ethClient, OriginChainID: 11155111, Staleness: time.Hour}

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
	r := &Reconciler{Store: store, OriginClient: ethClient, OriginChainID: 11155111, Staleness: time.Hour}

	exec := payment.Execution{ID: "exec-2", PaymentID: "pay-2", SignedTxHash: &hash}
	if err := r.checkAndUpdateOutcome(context.Background(), exec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(store.completeCalls) != 0 {
		t.Fatal("a transient/not-yet-mined poll must never produce a terminal transition")
	}
}

// TestCheckAndUpdateOutcome_RepairsBroadcastButNotYetSubmittedPayment is the
// regression test for review Finding 1: a payment_executions row that has
// broadcast_at set (DriveExecutionForward's MarkExecutionBroadcast already
// ran) but whose payment is still durably PROCESSING (the process died
// before DriveExecutionForward's subsequent MarkSubmitted call) must be
// repaired to SUBMITTED by checkAndUpdateOutcome -- driveStaleNotYetBroadcast
// deliberately skips exactly this case (BroadcastAt != nil), so this is the
// only remaining place that can ever call MarkSubmitted for it. This is
// asserted here regardless of what the receipt lookup finds (no receipt
// yet, in this test), since the repair must happen before -- not
// conditioned on -- the rest of the terminal-status logic.
func TestCheckAndUpdateOutcome_RepairsBroadcastButNotYetSubmittedPayment(t *testing.T) {
	hash := "0x5555555555555555555555555555555555555555555555555555555555555555"
	store := &fakeReconcilerStore{}
	ethClient := &fakeReconcilerEthClient{receipts: map[common.Hash]*types.Receipt{}} // not yet mined
	r := &Reconciler{Store: store, OriginClient: ethClient, OriginChainID: 11155111, Staleness: time.Hour}

	exec := payment.Execution{ID: "exec-repair", PaymentID: "pay-repair", SignedTxHash: &hash, BroadcastAt: timePtr()}
	if err := r.checkAndUpdateOutcome(context.Background(), exec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !store.submittedCalled {
		t.Fatal("expected checkAndUpdateOutcome to call MarkSubmitted to repair a broadcast-but-still-PROCESSING payment, even when the receipt isn't mined yet")
	}
}

// TestMarkTerminal_LogsRatherThanErrorsWhenCompleteSubmittedPaymentReturnsFalse
// covers the other half of review Finding 1's fix: markTerminal must no
// longer silently discard CompleteSubmittedPayment's returned bool. A
// completed=false result (the payment wasn't SUBMITTED when a definitive
// terminal observation was made) should be logged loudly, not swallowed --
// but also must not itself be treated as a hard error, since the terminal
// observation (UpdateExecutionExternalStatus) was already durably recorded
// and there is nothing to retry.
func TestMarkTerminal_LogsRatherThanErrorsWhenCompleteSubmittedPaymentReturnsFalse(t *testing.T) {
	hash := "0x6666666666666666666666666666666666666666666666666666666666666666"
	store := &fakeReconcilerStore{completeSubmittedFails: true}
	ethClient := &fakeReconcilerEthClient{receipts: map[common.Hash]*types.Receipt{
		common.HexToHash(hash): {Status: 0}, // reverted -- reaches markTerminal directly
	}}
	r := &Reconciler{Store: store, OriginClient: ethClient, OriginChainID: 11155111, Staleness: time.Hour}

	exec := payment.Execution{ID: "exec-mismatch", PaymentID: "pay-mismatch", SignedTxHash: &hash}
	if err := r.checkAndUpdateOutcome(context.Background(), exec); err != nil {
		t.Fatalf("a false return from CompleteSubmittedPayment must be logged, not surfaced as an error: %v", err)
	}
	if len(store.updateStatusCalls) != 1 || store.updateStatusCalls[0] != payment.ExternalStatusReverted {
		t.Fatalf("expected the terminal observation to still be recorded, got %v", store.updateStatusCalls)
	}
	if len(store.completeCalls) != 1 || store.completeCalls[0] != payment.StatusFailed {
		t.Fatalf("expected CompleteSubmittedPayment to still be called exactly once, got %v", store.completeCalls)
	}
}

// TestCheckAndUpdateOutcome_DispatchesByPersistedProvider asserts that an
// execution's own persisted BridgeProvider ("relay") selects which
// StatusChecker is consulted, never a different one that happens to also
// be configured -- design doc §12's "no silent substitution" rule applies
// to reconciliation exactly as it does to signing (Task 9).
func TestCheckAndUpdateOutcome_DispatchesByPersistedProvider(t *testing.T) {
	hash := "0x1010101010101010101010101010101010101010101010101010101010101010"
	store := &fakeReconcilerStore{}
	ethClient := &fakeReconcilerEthClient{receipts: map[common.Hash]*types.Receipt{
		common.HexToHash(hash): {Status: 1}, // mined AND successful -- required to reach checker dispatch
	}}
	acrossChecker := &fakeStatusChecker{name: "across", checkResult: quote.StatusResult{State: quote.StateFilled, RawStatus: "filled"}}
	relayChecker := &fakeStatusChecker{name: "relay", checkResult: quote.StatusResult{State: quote.StateFilled, RawStatus: "success"}}
	r := &Reconciler{
		Store: store, OriginClient: ethClient,
		StatusCheckers: map[string]quote.StatusChecker{"across": acrossChecker, "relay": relayChecker},
		OriginChainID:  11155111, Staleness: time.Hour,
	}

	exec := payment.Execution{ID: "exec-dispatch", PaymentID: "pay-dispatch", SignedTxHash: &hash, BridgeProvider: "relay"}
	if err := r.checkAndUpdateOutcome(context.Background(), exec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !relayChecker.checkCalled {
		t.Fatal("expected the \"relay\" StatusChecker to be invoked")
	}
	if acrossChecker.checkCalled {
		t.Fatal("the \"across\" StatusChecker must never be invoked for an execution persisted as \"relay\"")
	}
}

// TestCheckAndUpdateOutcome_UnknownProviderIsHardError asserts that an
// execution whose persisted BridgeProvider has no configured StatusChecker
// returns a hard error rather than silently substituting a different
// provider or writing a terminal state (design doc §12/§21).
func TestCheckAndUpdateOutcome_UnknownProviderIsHardError(t *testing.T) {
	hash := "0x2020202020202020202020202020202020202020202020202020202020202020"
	store := &fakeReconcilerStore{}
	ethClient := &fakeReconcilerEthClient{receipts: map[common.Hash]*types.Receipt{
		common.HexToHash(hash): {Status: 1},
	}}
	acrossChecker := &fakeStatusChecker{name: "across", checkResult: quote.StatusResult{State: quote.StateFilled, RawStatus: "filled"}}
	r := &Reconciler{
		Store: store, OriginClient: ethClient,
		StatusCheckers: map[string]quote.StatusChecker{"across": acrossChecker},
		OriginChainID:  11155111, Staleness: time.Hour,
	}

	exec := payment.Execution{ID: "exec-unknown", PaymentID: "pay-unknown", SignedTxHash: &hash, BridgeProvider: "relay"}
	if err := r.checkAndUpdateOutcome(context.Background(), exec); err == nil {
		t.Fatal("expected an error for a BridgeProvider with no configured StatusChecker")
	}
	if len(store.updateStatusCalls) != 0 {
		t.Fatalf("expected no terminal status write, got %v", store.updateStatusCalls)
	}
	if len(store.completeCalls) != 0 {
		t.Fatalf("expected no payment completion, got %v", store.completeCalls)
	}
}

// TestCheckAndUpdateOutcome_RelaySuccessCompletesPayment asserts that a
// Relay-provider execution whose StatusChecker reports StateFilled
// completes the payment and persists Relay's own raw status string
// ("success") for observability (design doc §16).
func TestCheckAndUpdateOutcome_RelaySuccessCompletesPayment(t *testing.T) {
	hash := "0x3030303030303030303030303030303030303030303030303030303030303030"
	store := &fakeReconcilerStore{}
	ethClient := &fakeReconcilerEthClient{receipts: map[common.Hash]*types.Receipt{
		common.HexToHash(hash): {Status: 1},
	}}
	relayChecker := &fakeStatusChecker{name: "relay", checkResult: quote.StatusResult{State: quote.StateFilled, RawStatus: "success"}}
	r := &Reconciler{
		Store: store, OriginClient: ethClient,
		StatusCheckers: map[string]quote.StatusChecker{"relay": relayChecker},
		OriginChainID:  11155111, Staleness: time.Hour,
	}

	exec := payment.Execution{ID: "exec-relay-success", PaymentID: "pay-relay-success", SignedTxHash: &hash, BridgeProvider: "relay"}
	if err := r.checkAndUpdateOutcome(context.Background(), exec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(store.updateStatusCalls, []payment.ExternalStatus{payment.ExternalStatusFilled}) {
		t.Fatalf("UpdateExecutionExternalStatus calls: got %v", store.updateStatusCalls)
	}
	if !reflect.DeepEqual(store.updateRawStatusCalls, []string{"success"}) {
		t.Fatalf("expected RawExternalStatus \"success\" to be persisted, got %v", store.updateRawStatusCalls)
	}
	if !reflect.DeepEqual(store.completeCalls, []payment.Status{payment.StatusCompleted}) {
		t.Fatalf("CompleteSubmittedPayment calls: got %v", store.completeCalls)
	}
}

// TestCheckAndUpdateOutcome_RelayFailureFailsPayment asserts that a
// Relay-provider execution whose StatusChecker reports StateFillFailed
// (Relay's "failure" -- an unsuccessful fill, distinct from
// reverted/refunded/expired, design doc §14) fails the payment with
// external_status='fill_failed'.
func TestCheckAndUpdateOutcome_RelayFailureFailsPayment(t *testing.T) {
	hash := "0x4040404040404040404040404040404040404040404040404040404040404040"
	store := &fakeReconcilerStore{}
	ethClient := &fakeReconcilerEthClient{receipts: map[common.Hash]*types.Receipt{
		common.HexToHash(hash): {Status: 1},
	}}
	relayChecker := &fakeStatusChecker{name: "relay", checkResult: quote.StatusResult{State: quote.StateFillFailed, RawStatus: "failure"}}
	r := &Reconciler{
		Store: store, OriginClient: ethClient,
		StatusCheckers: map[string]quote.StatusChecker{"relay": relayChecker},
		OriginChainID:  11155111, Staleness: time.Hour,
	}

	exec := payment.Execution{ID: "exec-relay-failure", PaymentID: "pay-relay-failure", SignedTxHash: &hash, BridgeProvider: "relay"}
	if err := r.checkAndUpdateOutcome(context.Background(), exec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(store.updateStatusCalls, []payment.ExternalStatus{payment.ExternalStatusFillFailed}) {
		t.Fatalf("UpdateExecutionExternalStatus calls: got %v", store.updateStatusCalls)
	}
	if !reflect.DeepEqual(store.completeCalls, []payment.Status{payment.StatusFailed}) {
		t.Fatalf("CompleteSubmittedPayment calls: got %v", store.completeCalls)
	}
}

// TestCheckAndUpdateOutcome_RefundedFailsPayment asserts that a
// StatusChecker reporting quote.StateRefunded (raw status "refunded" here --
// Across's "expired"/"refunded" raw statuses and Relay's "refund" raw status
// both map to this same shared state) drives the Reconciler's own terminal
// switch (reconciler.go's `case quote.StateRefunded, quote.StateReverted,
// quote.StateFillFailed:`) end-to-end: markTerminal is called with
// payment.StatusFailed, the persisted external status is the
// externalStateToStatus(quote.StateRefunded) mapping
// (payment.ExternalStatusRefunded), and the configured raw status string is
// what UpdateExecutionExternalStatus persists as rawStatus. Prior to this
// test, no test anywhere in the package drove a fakeStatusChecker to return
// quote.StateRefunded and asserted the Reconciler's resulting behavior --
// across/status_test.go and relay/status_test.go cover only the raw-string
// -> quote.ExternalState mapping at the provider layer, not this switch.
func TestCheckAndUpdateOutcome_RefundedFailsPayment(t *testing.T) {
	hash := "0x6060606060606060606060606060606060606060606060606060606060606060"
	store := &fakeReconcilerStore{}
	ethClient := &fakeReconcilerEthClient{receipts: map[common.Hash]*types.Receipt{
		common.HexToHash(hash): {Status: 1},
	}}
	acrossChecker := &fakeStatusChecker{name: "across", checkResult: quote.StatusResult{State: quote.StateRefunded, RawStatus: "refunded"}}
	r := &Reconciler{
		Store: store, OriginClient: ethClient,
		StatusCheckers: map[string]quote.StatusChecker{"across": acrossChecker},
		OriginChainID:  11155111, Staleness: time.Hour,
	}

	exec := payment.Execution{ID: "exec-refunded", PaymentID: "pay-refunded", SignedTxHash: &hash, BridgeProvider: "across"}
	if err := r.checkAndUpdateOutcome(context.Background(), exec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(store.updateStatusCalls, []payment.ExternalStatus{payment.ExternalStatusRefunded}) {
		t.Fatalf("UpdateExecutionExternalStatus calls: got %v", store.updateStatusCalls)
	}
	if !reflect.DeepEqual(store.updateRawStatusCalls, []string{"refunded"}) {
		t.Fatalf("expected raw status \"refunded\" to be persisted, got %v", store.updateRawStatusCalls)
	}
	if !reflect.DeepEqual(store.completeCalls, []payment.Status{payment.StatusFailed}) {
		t.Fatalf("CompleteSubmittedPayment calls: got %v", store.completeCalls)
	}
}

// TestCheckAndUpdateOutcome_TransientStatusCheckerErrorIsNonTerminal
// mirrors the existing Across transient-error test (now generalized): any
// error from CheckStatus -- a transient API failure, a not-yet-indexed
// deposit, etc. -- must never produce a terminal write (design spec §13).
func TestCheckAndUpdateOutcome_TransientStatusCheckerErrorIsNonTerminal(t *testing.T) {
	hash := "0x5050505050505050505050505050505050505050505050505050505050505050"
	store := &fakeReconcilerStore{}
	ethClient := &fakeReconcilerEthClient{receipts: map[common.Hash]*types.Receipt{
		common.HexToHash(hash): {Status: 1},
	}}
	relayChecker := &fakeStatusChecker{name: "relay", checkErr: fmt.Errorf("transient relay API error")}
	r := &Reconciler{
		Store: store, OriginClient: ethClient,
		StatusCheckers: map[string]quote.StatusChecker{"relay": relayChecker},
		OriginChainID:  11155111, Staleness: time.Hour,
	}

	exec := payment.Execution{ID: "exec-relay-transient", PaymentID: "pay-relay-transient", SignedTxHash: &hash, BridgeProvider: "relay"}
	if err := r.checkAndUpdateOutcome(context.Background(), exec); err != nil {
		t.Fatalf("a transient StatusChecker error must not be surfaced as an error: %v", err)
	}
	if len(store.updateStatusCalls) != 0 {
		t.Fatalf("expected no terminal status write, got %v", store.updateStatusCalls)
	}
	if len(store.completeCalls) != 0 {
		t.Fatalf("expected no payment completion, got %v", store.completeCalls)
	}
}

// TestCheckAndUpdateOutcome_RelaySuccessCompletesPayment_RecordsMetricsAndDuration
// is Task 10's metrics regression test (mirroring the sibling functional
// test above, TestCheckAndUpdateOutcome_RelaySuccessCompletesPayment): a
// Relay StateFilled outcome must record a Reconciliations{relay}
// observation, an ExecutionsCompleted{relay} observation, a
// ReconciliationDuration sample, and -- since markTerminal now threads the
// real created_at CompleteSubmittedPayment returns (Task 7) through to
// PaymentDuration -- a PaymentDuration sample too.
func TestCheckAndUpdateOutcome_RelaySuccessCompletesPayment_RecordsMetricsAndDuration(t *testing.T) {
	hash := "0x7070707070707070707070707070707070707070707070707070707070707070"
	store := &fakeReconcilerStore{}
	ethClient := &fakeReconcilerEthClient{receipts: map[common.Hash]*types.Receipt{
		common.HexToHash(hash): {Status: 1},
	}}
	relayChecker := &fakeStatusChecker{name: "relay", checkResult: quote.StatusResult{State: quote.StateFilled, RawStatus: "success"}}
	metrics := observability.NewMetrics()
	r := &Reconciler{
		Store: store, OriginClient: ethClient,
		StatusCheckers: map[string]quote.StatusChecker{"relay": relayChecker},
		OriginChainID:  11155111, Staleness: time.Hour,
		Metrics: metrics,
	}

	exec := payment.Execution{ID: "exec-relay-metrics", PaymentID: "pay-relay-metrics", SignedTxHash: &hash, BridgeProvider: "relay"}
	if err := r.checkAndUpdateOutcome(context.Background(), exec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := testutil.ToFloat64(metrics.Reconciliations.WithLabelValues("relay")); got != 1 {
		t.Errorf("Reconciliations{relay} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.ExecutionsCompleted.WithLabelValues("relay")); got != 1 {
		t.Errorf("ExecutionsCompleted{relay} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.PaymentsCompleted.WithLabelValues(string(payment.ExecutionModeTestnet))); got != 1 {
		t.Errorf("PaymentsCompleted{testnet} = %v, want 1", got)
	}
	if got := testutil.CollectAndCount(metrics.ReconciliationDuration); got == 0 {
		t.Error("expected a ReconciliationDuration observation")
	}
	if got := testutil.CollectAndCount(metrics.PaymentDuration); got == 0 {
		t.Error("expected a PaymentDuration observation (created_at now flows through markTerminal)")
	}
}

// TestCheckAndUpdateOutcome_TransientPollFailure_DoesNotRecordReconciliations
// asserts that a not-yet-mined/transient receipt lookup -- which never
// reaches the StatusChecker at all -- must not count as a reconciliation
// attempt (Reconciliations is meant to measure real, completed status
// checks, not skipped ones).
func TestCheckAndUpdateOutcome_TransientPollFailure_DoesNotRecordReconciliations(t *testing.T) {
	hash := "0x8080808080808080808080808080808080808080808080808080808080808080"
	store := &fakeReconcilerStore{}
	ethClient := &fakeReconcilerEthClient{receipts: map[common.Hash]*types.Receipt{}} // not yet mined
	metrics := observability.NewMetrics()
	r := &Reconciler{Store: store, OriginClient: ethClient, OriginChainID: 11155111, Staleness: time.Hour, Metrics: metrics}

	exec := payment.Execution{ID: "exec-transient-metrics", PaymentID: "pay-transient-metrics", SignedTxHash: &hash, BridgeProvider: "relay"}
	if err := r.checkAndUpdateOutcome(context.Background(), exec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := testutil.ToFloat64(metrics.Reconciliations.WithLabelValues("relay")); got != 0 {
		t.Errorf("Reconciliations{relay} = %v, want 0 for a transient/not-yet-mined poll", got)
	}
}

// TestCheckAndUpdateOutcome_StillPendingRecordsReconciliationButNotCompletion
// asserts the brief's deliberate distinction: a StatusChecker call that
// succeeds but reports quote.StatePending IS counted as a reconciliation
// (it got a real, definitive non-terminal answer), but must never record
// ExecutionsCompleted/ExecutionsFailed/PaymentsCompleted/PaymentsFailed,
// since the payment has not reached a terminal outcome.
func TestCheckAndUpdateOutcome_StillPendingRecordsReconciliationButNotCompletion(t *testing.T) {
	hash := "0x9090909090909090909090909090909090909090909090909090909090909090"
	store := &fakeReconcilerStore{}
	ethClient := &fakeReconcilerEthClient{receipts: map[common.Hash]*types.Receipt{
		common.HexToHash(hash): {Status: 1},
	}}
	relayChecker := &fakeStatusChecker{name: "relay", checkResult: quote.StatusResult{State: quote.StatePending, RawStatus: "pending"}}
	metrics := observability.NewMetrics()
	r := &Reconciler{
		Store: store, OriginClient: ethClient,
		StatusCheckers: map[string]quote.StatusChecker{"relay": relayChecker},
		OriginChainID:  11155111, Staleness: time.Hour,
		Metrics: metrics,
	}

	exec := payment.Execution{ID: "exec-pending-metrics", PaymentID: "pay-pending-metrics", SignedTxHash: &hash, BridgeProvider: "relay"}
	if err := r.checkAndUpdateOutcome(context.Background(), exec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := testutil.ToFloat64(metrics.Reconciliations.WithLabelValues("relay")); got != 1 {
		t.Errorf("Reconciliations{relay} = %v, want 1 for a real but still-pending status check", got)
	}
	if got := testutil.ToFloat64(metrics.ExecutionsCompleted.WithLabelValues("relay")); got != 0 {
		t.Errorf("ExecutionsCompleted{relay} = %v, want 0 for a non-terminal outcome", got)
	}
	if got := testutil.ToFloat64(metrics.PaymentsCompleted.WithLabelValues(string(payment.ExecutionModeTestnet))); got != 0 {
		t.Errorf("PaymentsCompleted{testnet} = %v, want 0 for a non-terminal outcome", got)
	}
	if len(store.completeCalls) != 0 {
		t.Fatalf("expected no payment completion for a still-pending outcome, got %v", store.completeCalls)
	}
}

// TestMarkTerminal_CompleteSubmittedPaymentGuardFalse_DoesNotRecordCompletionMetrics
// is this task's double-counting regression test, directly analogous to
// Task 9's fix: when CompleteSubmittedPayment's own guard affects zero
// rows (completed=false -- e.g. a race where the payment was already
// completed by a previous sweep), markTerminal must NOT increment
// ExecutionsCompleted/PaymentsCompleted/PaymentDuration/PaymentsProcessing,
// since no real, new completion happened.
func TestMarkTerminal_CompleteSubmittedPaymentGuardFalse_DoesNotRecordCompletionMetrics(t *testing.T) {
	hash := "0xa0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0"
	store := &fakeReconcilerStore{completeSubmittedFails: true}
	ethClient := &fakeReconcilerEthClient{receipts: map[common.Hash]*types.Receipt{
		common.HexToHash(hash): {Status: 1},
	}}
	relayChecker := &fakeStatusChecker{name: "relay", checkResult: quote.StatusResult{State: quote.StateFilled, RawStatus: "success"}}
	metrics := observability.NewMetrics()
	r := &Reconciler{
		Store: store, OriginClient: ethClient,
		StatusCheckers: map[string]quote.StatusChecker{"relay": relayChecker},
		OriginChainID:  11155111, Staleness: time.Hour,
		Metrics: metrics,
	}

	exec := payment.Execution{ID: "exec-guard-false", PaymentID: "pay-guard-false", SignedTxHash: &hash, BridgeProvider: "relay"}
	if err := r.checkAndUpdateOutcome(context.Background(), exec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Reconciliations still fires -- a real status check happened.
	if got := testutil.ToFloat64(metrics.Reconciliations.WithLabelValues("relay")); got != 1 {
		t.Errorf("Reconciliations{relay} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.ExecutionsCompleted.WithLabelValues("relay")); got != 0 {
		t.Errorf("ExecutionsCompleted{relay} = %v, want 0 when CompleteSubmittedPayment's guard affected no rows", got)
	}
	if got := testutil.ToFloat64(metrics.PaymentsCompleted.WithLabelValues(string(payment.ExecutionModeTestnet))); got != 0 {
		t.Errorf("PaymentsCompleted{testnet} = %v, want 0 when CompleteSubmittedPayment's guard affected no rows", got)
	}
	if got := testutil.CollectAndCount(metrics.PaymentDuration); got != 0 {
		t.Errorf("PaymentDuration observations = %v, want 0 when CompleteSubmittedPayment's guard affected no rows", got)
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
