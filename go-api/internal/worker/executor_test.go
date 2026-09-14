package worker

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	gethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"chainroute/go-api/internal/bridge/across"
	"chainroute/go-api/internal/evm"
	"chainroute/go-api/internal/payment"
	"chainroute/go-api/internal/postgres"
)

// testExecutorKey is the corrected go-ethereum test key (64 hex chars),
// consistent with internal/evm/wallet_test.go and
// internal/bridge/across/execute_test.go. The brief's literal key ended in
// "...dbcda3f25" (63 hex chars, malformed); the correct suffix is "f291".
const testExecutorKey = "b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291"

type fakeExecutorStore struct {
	tryCreateExec    payment.Execution
	tryCreateCreated bool
	tryCreateErr     error
	pmt              payment.Payment
	pmtFound         bool
	persistedRawTx   []byte
	persistedHash    string
	broadcastCalled  bool
	submittedCalled  bool
}

func (f *fakeExecutorStore) TryCreateExecution(ctx context.Context, p postgres.CreateExecutionParams) (payment.Execution, bool, error) {
	return f.tryCreateExec, f.tryCreateCreated, f.tryCreateErr
}
func (f *fakeExecutorStore) GetPayment(ctx context.Context, id string) (payment.Payment, bool, error) {
	return f.pmt, f.pmtFound, nil
}
func (f *fakeExecutorStore) PersistSignedExecution(ctx context.Context, executionID string, rawTx []byte, txHash string) error {
	f.persistedRawTx, f.persistedHash = rawTx, txHash
	return nil
}
func (f *fakeExecutorStore) MarkExecutionBroadcast(ctx context.Context, executionID string) error {
	f.broadcastCalled = true
	return nil
}
func (f *fakeExecutorStore) MarkSubmitted(ctx context.Context, paymentID string) (bool, error) {
	f.submittedCalled = true
	return true, nil
}

type fakeExecutorEthClient struct {
	sendCalled    bool
	sendErr       error
	txByHashFound bool
	txByHashErr   error
}

func (f *fakeExecutorEthClient) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	return big.NewInt(1_000_000_000), nil
}
func (f *fakeExecutorEthClient) EstimateGas(ctx context.Context, msg gethereum.CallMsg) (uint64, error) {
	return 300_000, nil
}
func (f *fakeExecutorEthClient) SendTransaction(ctx context.Context, tx *types.Transaction) error {
	f.sendCalled = true
	return f.sendErr
}
func (f *fakeExecutorEthClient) TransactionByHash(ctx context.Context, hash common.Hash) (*types.Transaction, bool, error) {
	if f.txByHashFound {
		return &types.Transaction{}, true, nil
	}
	return nil, false, f.txByHashErr
}

// validAcrossQuoteBody is the exact fixture from the brief: chain IDs and
// token addresses that match newTestExecutor's OriginChainID/DestChainID/
// WETHOrigin/WETHDestination, so validateQuote accepts it.
const validAcrossQuoteBody = `{"outputAmount":"997592172330233","fillDeadline":"1789347312","exclusivityDeadline":0,"exclusiveRelayer":"0x0000000000000000000000000000000000000000","timestamp":"1789340112","spokePoolAddress":"0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662","isAmountTooLow":false,"inputToken":{"address":"0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14","chainId":11155111,"decimals":18},"outputToken":{"address":"0x4200000000000000000000000000000000000006","chainId":84532,"decimals":18}}`

// acrossQuoteBody builds a /suggested-fees response body with the given
// inputToken/outputToken chain IDs, token addresses and outputAmount --
// used to drive validateQuote's individual rejection branches by making
// exactly one field diverge from validAcrossQuoteBody's otherwise-matching
// values.
func acrossQuoteBody(inputChainID, outputChainID int64, inputAddr, outputAddr, outputAmount string) string {
	return fmt.Sprintf(`{"outputAmount":"%s","fillDeadline":"1789347312","exclusivityDeadline":0,"exclusiveRelayer":"0x0000000000000000000000000000000000000000","timestamp":"1789340112","spokePoolAddress":"0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662","isAmountTooLow":false,"inputToken":{"address":"%s","chainId":%d,"decimals":18},"outputToken":{"address":"%s","chainId":%d,"decimals":18}}`,
		outputAmount, inputAddr, inputChainID, outputAddr, outputChainID)
}

func newTestAcrossServer(t *testing.T) *across.Client {
	t.Helper()
	return newTestAcrossServerWithBody(t, validAcrossQuoteBody)
}

func newTestAcrossServerWithBody(t *testing.T, body string) *across.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return across.NewClient(srv.URL)
}

func newTestExecutor(t *testing.T, store *fakeExecutorStore, ethClient *fakeExecutorEthClient) *Executor {
	t.Helper()
	return newTestExecutorWithAcrossBody(t, store, ethClient, validAcrossQuoteBody)
}

func newTestExecutorWithAcrossBody(t *testing.T, store *fakeExecutorStore, ethClient *fakeExecutorEthClient, body string) *Executor {
	t.Helper()
	wallet, err := evm.LoadWallet(testExecutorKey)
	if err != nil {
		t.Fatalf("load wallet: %v", err)
	}
	return &Executor{
		Store: store, Wallet: wallet, OriginClient: ethClient, Across: newTestAcrossServerWithBody(t, body),
		BridgeProvider: "across", OriginChainID: 11155111, DestChainID: 84532,
		SpokePoolAddress: common.HexToAddress("0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662"),
		WETHOrigin:       common.HexToAddress("0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14"),
		WETHDestination:  common.HexToAddress("0x4200000000000000000000000000000000000006"),
	}
}

func TestExecuteTestnetPayment_HappyPath(t *testing.T) {
	store := &fakeExecutorStore{
		tryCreateCreated: true,
		tryCreateExec:    payment.Execution{ID: "exec-1", PaymentID: "pay-1", Nonce: 3},
		pmt:              payment.Payment{ID: "pay-1", Amount: "0.001"},
		pmtFound:         true,
	}
	ethClient := &fakeExecutorEthClient{txByHashFound: false, txByHashErr: gethereum.NotFound}
	e := newTestExecutor(t, store, ethClient)

	if err := e.ExecuteTestnetPayment(context.Background(), "pay-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store.persistedHash == "" {
		t.Fatal("expected the signed tx hash to be persisted before broadcast")
	}
	if !ethClient.sendCalled {
		t.Fatal("expected SendTransaction to be called")
	}
	if !store.broadcastCalled {
		t.Fatal("expected MarkExecutionBroadcast to be called")
	}
	if !store.submittedCalled {
		t.Fatal("expected MarkSubmitted to be called")
	}
}

func TestExecuteTestnetPayment_LostRaceIsSafeNoOp(t *testing.T) {
	store := &fakeExecutorStore{tryCreateCreated: false}
	ethClient := &fakeExecutorEthClient{}
	e := newTestExecutor(t, store, ethClient)

	if err := e.ExecuteTestnetPayment(context.Background(), "pay-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ethClient.sendCalled {
		t.Fatal("must never sign/broadcast when TryCreateExecution reports it lost the race")
	}
}

func TestDriveExecutionForward_AmbiguousBroadcastFoundOnChainNeverResends(t *testing.T) {
	hash := "0xabcd000000000000000000000000000000000000000000000000000000000000"[:66]
	store := &fakeExecutorStore{}
	ethClient := &fakeExecutorEthClient{txByHashFound: true} // already known to the chain
	e := newTestExecutor(t, store, ethClient)

	exec := payment.Execution{
		ID: "exec-2", PaymentID: "pay-2", Nonce: 1,
		SignedTxHash: &hash, RawSignedTx: []byte{0xde, 0xad},
	}
	if err := e.DriveExecutionForward(context.Background(), exec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ethClient.sendCalled {
		t.Fatal("must never rebroadcast when the hash is already found on-chain")
	}
	if !store.broadcastCalled {
		t.Fatal("expected MarkExecutionBroadcast even on the found-on-chain path -- it is now confirmed broadcast, whoever sent it")
	}
}

// TestDriveExecutionForward_TransientLookupErrorPropagatesWithoutResend
// guards the fix for the code-review finding that broadcastWithRecovery
// was treating ANY TransactionByHash error as "not found." Only
// ethereum.NotFound means the chain has genuinely never seen this tx; a
// transient RPC/timeout error must propagate as a real error rather than
// being silently treated as license to rebroadcast against a possibly
// flaky endpoint.
func TestDriveExecutionForward_TransientLookupErrorPropagatesWithoutResend(t *testing.T) {
	hash := "0xabcd000000000000000000000000000000000000000000000000000000000000"[:66]
	store := &fakeExecutorStore{}
	ethClient := &fakeExecutorEthClient{txByHashFound: false, txByHashErr: errors.New("i/o timeout")}
	e := newTestExecutor(t, store, ethClient)

	exec := payment.Execution{
		ID: "exec-transient", PaymentID: "pay-transient", Nonce: 1,
		SignedTxHash: &hash, RawSignedTx: []byte{0xde, 0xad},
	}
	err := e.DriveExecutionForward(context.Background(), exec)
	if err == nil {
		t.Fatal("expected a transient TransactionByHash error to propagate as an error, not be treated as not-found")
	}
	if ethClient.sendCalled {
		t.Fatal("must never rebroadcast on an ambiguous/transient lookup error -- only a genuine ethereum.NotFound permits resend")
	}
	if store.broadcastCalled {
		t.Fatal("must not mark broadcast when the on-chain lookup itself failed")
	}
}

// TestSignAndPersist_PersistsBeforeBroadcast guards the crash-safety
// ordering design spec §8 step 2 requires: the signed transaction must be
// durably persisted BEFORE any broadcast is attempted, so a crash between
// the two never loses a signed tx that was already sent. This uses a store
// fake that records call order into a shared slice with the eth client, so
// a mutation that reorders signAndPersist/broadcastWithRecovery is caught
// even though neither individual fake's own boolean flags would notice.
type callOrderStore struct {
	fakeExecutorStore
	calls *[]string
}

func (f *callOrderStore) PersistSignedExecution(ctx context.Context, executionID string, rawTx []byte, txHash string) error {
	*f.calls = append(*f.calls, "persist")
	return f.fakeExecutorStore.PersistSignedExecution(ctx, executionID, rawTx, txHash)
}

type callOrderEthClient struct {
	fakeExecutorEthClient
	calls *[]string
}

func (f *callOrderEthClient) SendTransaction(ctx context.Context, tx *types.Transaction) error {
	*f.calls = append(*f.calls, "send")
	return f.fakeExecutorEthClient.SendTransaction(ctx, tx)
}

func TestExecuteTestnetPayment_PersistsSignedTxBeforeBroadcasting(t *testing.T) {
	var calls []string
	store := &callOrderStore{
		fakeExecutorStore: fakeExecutorStore{
			tryCreateCreated: true,
			tryCreateExec:    payment.Execution{ID: "exec-3", PaymentID: "pay-3", Nonce: 5},
			pmt:              payment.Payment{ID: "pay-3", Amount: "0.001"},
			pmtFound:         true,
		},
		calls: &calls,
	}
	ethClient := &callOrderEthClient{
		fakeExecutorEthClient: fakeExecutorEthClient{txByHashFound: false, txByHashErr: gethereum.NotFound},
		calls:                 &calls,
	}
	e := newTestExecutor(t, &store.fakeExecutorStore, &ethClient.fakeExecutorEthClient)
	e.Store = store
	e.OriginClient = ethClient

	if err := e.ExecuteTestnetPayment(context.Background(), "pay-3"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(calls) != 2 {
		t.Fatalf("expected exactly 2 recorded calls (persist, send), got %v", calls)
	}
	if calls[0] != "persist" || calls[1] != "send" {
		t.Fatalf("expected persist to happen before send, got order %v", calls)
	}
}

// The tests below guard validateQuote's §21 safety gate -- a quote whose
// echoed chain IDs, token addresses, or outputAmount don't match what was
// requested must never reach the signing call, and MaxAmountWei must be
// enforced before a quote is even fetched. Each test drives the full
// ExecuteTestnetPayment path (not just validateQuote in isolation) and
// asserts both that an error is returned AND that SendTransaction is never
// called -- no broadcast may ever be attempted with a rejected quote or an
// over-ceiling amount.

func newRejectedQuoteTestSetup(t *testing.T, paymentID string) (*fakeExecutorStore, *fakeExecutorEthClient) {
	t.Helper()
	store := &fakeExecutorStore{
		tryCreateCreated: true,
		tryCreateExec:    payment.Execution{ID: "exec-" + paymentID, PaymentID: paymentID, Nonce: 1},
		pmt:              payment.Payment{ID: paymentID, Amount: "0.001"},
		pmtFound:         true,
	}
	ethClient := &fakeExecutorEthClient{}
	return store, ethClient
}

func assertQuoteRejected(t *testing.T, e *Executor, store *fakeExecutorStore, ethClient *fakeExecutorEthClient, paymentID string) {
	t.Helper()
	err := e.ExecuteTestnetPayment(context.Background(), paymentID)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if ethClient.sendCalled {
		t.Fatal("must never call SendTransaction when the quote is rejected or the amount exceeds the guardrail")
	}
	if store.persistedHash != "" {
		t.Fatal("must never persist a signed tx built from a rejected quote")
	}
	if store.broadcastCalled || store.submittedCalled {
		t.Fatal("must never mark broadcast/submitted when the quote is rejected or the amount exceeds the guardrail")
	}
}

func TestSignAndPersist_RejectsWrongInputTokenChainID(t *testing.T) {
	store, ethClient := newRejectedQuoteTestSetup(t, "pay-reject-input-chain")
	body := acrossQuoteBody(999999, 84532,
		"0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14", "0x4200000000000000000000000000000000000006", "997592172330233")
	e := newTestExecutorWithAcrossBody(t, store, ethClient, body)
	assertQuoteRejected(t, e, store, ethClient, "pay-reject-input-chain")
}

func TestSignAndPersist_RejectsWrongOutputTokenChainID(t *testing.T) {
	store, ethClient := newRejectedQuoteTestSetup(t, "pay-reject-output-chain")
	body := acrossQuoteBody(11155111, 999999,
		"0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14", "0x4200000000000000000000000000000000000006", "997592172330233")
	e := newTestExecutorWithAcrossBody(t, store, ethClient, body)
	assertQuoteRejected(t, e, store, ethClient, "pay-reject-output-chain")
}

func TestSignAndPersist_RejectsWrongInputTokenAddress(t *testing.T) {
	store, ethClient := newRejectedQuoteTestSetup(t, "pay-reject-input-addr")
	body := acrossQuoteBody(11155111, 84532,
		"0x0000000000000000000000000000000000dEaD", "0x4200000000000000000000000000000000000006", "997592172330233")
	e := newTestExecutorWithAcrossBody(t, store, ethClient, body)
	assertQuoteRejected(t, e, store, ethClient, "pay-reject-input-addr")
}

func TestSignAndPersist_RejectsWrongOutputTokenAddress(t *testing.T) {
	store, ethClient := newRejectedQuoteTestSetup(t, "pay-reject-output-addr")
	body := acrossQuoteBody(11155111, 84532,
		"0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14", "0x0000000000000000000000000000000000dEaD", "997592172330233")
	e := newTestExecutorWithAcrossBody(t, store, ethClient, body)
	assertQuoteRejected(t, e, store, ethClient, "pay-reject-output-addr")
}

func TestSignAndPersist_RejectsNonIntegerOutputAmount(t *testing.T) {
	store, ethClient := newRejectedQuoteTestSetup(t, "pay-reject-amount")
	body := acrossQuoteBody(11155111, 84532,
		"0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14", "0x4200000000000000000000000000000000000006", "not-an-integer")
	e := newTestExecutorWithAcrossBody(t, store, ethClient, body)
	assertQuoteRejected(t, e, store, ethClient, "pay-reject-amount")
}

func TestSignAndPersist_RejectsAmountExceedingMaxAmountWei(t *testing.T) {
	store, ethClient := newRejectedQuoteTestSetup(t, "pay-reject-guardrail")
	e := newTestExecutor(t, store, ethClient)
	// 0.001 WETH = 10^15 wei, far above this ceiling.
	e.MaxAmountWei = big.NewInt(1)
	assertQuoteRejected(t, e, store, ethClient, "pay-reject-guardrail")
}
