package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	gethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"chainroute/go-api/internal/bridge/across"
	"chainroute/go-api/internal/bridge/quote"
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
	tryCreateCalled  bool
	pmt              payment.Payment
	pmtFound         bool
	persistedRawTx   []byte
	persistedHash    string
	broadcastCalled  bool
	submittedCalled  bool
	quoteRow         payment.Quote
	quoteFound       bool
	quoteErr         error
	markFailedReason string
}

func (f *fakeExecutorStore) TryCreateExecution(ctx context.Context, p postgres.CreateExecutionParams) (payment.Execution, bool, error) {
	f.tryCreateCalled = true
	return f.tryCreateExec, f.tryCreateCreated, f.tryCreateErr
}
func (f *fakeExecutorStore) GetPayment(ctx context.Context, id string) (payment.Payment, bool, error) {
	return f.pmt, f.pmtFound, nil
}
func (f *fakeExecutorStore) GetQuoteByPaymentID(ctx context.Context, paymentID string) (payment.Quote, bool, error) {
	return f.quoteRow, f.quoteFound, f.quoteErr
}
func (f *fakeExecutorStore) MarkProcessingFailed(ctx context.Context, paymentID, reason string) (bool, error) {
	f.markFailedReason = reason
	return true, nil
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

// defaultQuoteRowFeeAmount is the exact fee validAcrossQuoteBody yields for
// the "0.001" WETH amount every pre-existing test uses (0.001 WETH =
// 1_000_000_000_000_000 wei; outputAmount 997592172330233; fee =
// 1_000_000_000_000_000 - 997592172330233 = 2407827669767). Setting the
// default payment_quotes baseline fee to exactly this value means the real
// across.Provider wired below (which recomputes this same fee from the same
// mock response) never trips the new slippage check for tests that predate
// Task 11 and never touch quote fields themselves.
const defaultQuoteRowFeeAmount = "2407827669767"

func newTestExecutorWithAcrossBody(t *testing.T, store *fakeExecutorStore, ethClient *fakeExecutorEthClient, body string) *Executor {
	t.Helper()
	wallet, err := evm.LoadWallet(testExecutorKey)
	if err != nil {
		t.Fatalf("load wallet: %v", err)
	}
	acrossClient := newTestAcrossServerWithBody(t, body)

	// Every pre-existing (pre-Task-11) test constructs a bare
	// fakeExecutorStore with no quote_row/payment fields at all -- default
	// both here so ExecuteTestnetPayment's new payment_quotes-gated
	// pre-checks don't reject them. A test that sets its own
	// quoteRow.Provider (every Task 11 test does) is left untouched.
	if store.quoteRow.Provider == "" {
		store.quoteFound = true
		store.quoteRow = payment.Quote{
			Provider: "across", OriginChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
			FeeAmount: defaultQuoteRowFeeAmount, ExpiresAt: time.Now().Add(time.Hour),
		}
	}
	if store.pmt.ID == "" {
		store.pmtFound = true
		store.pmt = payment.Payment{ID: "default-test-payment", Amount: "0.001"}
	}

	return &Executor{
		Store: store, Wallet: wallet, OriginClient: ethClient, Across: acrossClient,
		QuoteProviders: map[string]quote.Provider{"across": across.NewProvider(acrossClient, time.Hour)},
		BridgeProvider: "across", OriginChainID: 11155111, DestChainID: 84532,
		SpokePoolAddress: common.HexToAddress("0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662"),
		WETHOrigin:       common.HexToAddress("0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14"),
		WETHDestination:  common.HexToAddress("0x4200000000000000000000000000000000000006"),
	}
}

// rawAcrossPayload is a valid across.QuotePayload JSON encoding, used to
// build fakeExecutorQuoteProvider fixtures whose freshQuote can be signed
// by signAndBroadcastFresh via across.DecodeQuotePayload.
func rawAcrossPayload() json.RawMessage {
	return json.RawMessage(`{"exclusiveRelayer":"0x0000000000000000000000000000000000000000","quoteTimestamp":"1789340112","fillDeadline":"1789347312","exclusivityDeadline":0,"spokePoolAddress":"0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662"}`)
}

// fakeExecutorQuoteProvider is a local stand-in for quote.Provider, used by
// tests that need to control the FRESH quote returned mid-execution
// (independent of the Across HTTP fixture newTestExecutorWithAcrossBody
// wires by default) -- e.g. to simulate a fee that has moved since routing
// time.
type fakeExecutorQuoteProvider struct {
	quote quote.Quote
	err   error
}

func (f *fakeExecutorQuoteProvider) Name() string { return "across" }
func (f *fakeExecutorQuoteProvider) GetQuote(ctx context.Context, req quote.Request) (quote.Quote, error) {
	return f.quote, f.err
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

// TestDriveExecutionForward_AlreadySignedIgnoresExpiredRoutingQuote guards
// the fix for the finding that DriveExecutionForward was checking
// exec.SignedTxHash != nil too late -- after the routing-time quote expiry
// check. An already-signed row (crash point C/D/E/F) is typically resumed
// later, by which point the routing-time quote has very plausibly expired;
// checking expiry first would wrongly call MarkProcessingFailed and mark
// the payment FAILED even though its transaction may already be sitting
// on-chain, mined or pending. This pairs a signed exec with an EXPIRED
// quoteRow and asserts broadcastWithRecovery still runs and
// MarkProcessingFailed is never called.
func TestDriveExecutionForward_AlreadySignedIgnoresExpiredRoutingQuote(t *testing.T) {
	hash := "0xabcd000000000000000000000000000000000000000000000000000000000000"[:66]
	store := &fakeExecutorStore{
		quoteFound: true,
		quoteRow: payment.Quote{
			Provider: "across", OriginChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
			FeeAmount: "100000000000", ExpiresAt: time.Now().Add(-time.Minute), // already expired
		},
	}
	ethClient := &fakeExecutorEthClient{txByHashFound: false, txByHashErr: gethereum.NotFound}
	e := newTestExecutor(t, store, ethClient)

	// broadcastWithRecovery's not-found path unmarshals RawSignedTx as a
	// real RLP-encoded transaction before rebroadcasting it -- unlike the
	// found-on-chain tests above (which never reach that unmarshal), this
	// test needs genuine RLP bytes here, not an arbitrary byte slice.
	rawTx, err := types.NewTx(&types.LegacyTx{
		Nonce: 1, GasPrice: big.NewInt(1), Gas: 21000, To: &common.Address{}, Value: big.NewInt(0),
	}).MarshalBinary()
	if err != nil {
		t.Fatalf("build fixture raw tx: %v", err)
	}

	exec := payment.Execution{
		ID: "exec-already-signed", PaymentID: "pay-already-signed", Nonce: 1,
		SignedTxHash: &hash, RawSignedTx: rawTx,
	}
	if err := e.DriveExecutionForward(context.Background(), exec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ethClient.sendCalled {
		t.Fatal("expected broadcastWithRecovery to still run (and rebroadcast) for an already-signed row, even with an expired routing-time quote")
	}
	if !store.broadcastCalled {
		t.Fatal("expected MarkExecutionBroadcast to be called")
	}
	if !store.submittedCalled {
		t.Fatal("expected MarkSubmitted to be called")
	}
	if store.markFailedReason != "" {
		t.Fatalf("must never call MarkProcessingFailed for an already-signed row, even with an expired routing-time quote; got reason %q", store.markFailedReason)
	}
}

// TestDriveExecutionForward_UnsignedResumedExecRechecksSlippageBeforeSigning
// covers the resume path (crash point B) for an UNSIGNED execution row:
// unlike the already-signed path above, this one must still re-check
// expiry/availability/slippage against a freshly-fetched quote before ever
// signing, exactly as ExecuteTestnetPayment does on a first attempt.
func TestDriveExecutionForward_UnsignedResumedExecRechecksSlippageBeforeSigning(t *testing.T) {
	store := &fakeExecutorStore{
		quoteFound: true,
		quoteRow: payment.Quote{
			Provider: "across", OriginChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
			FeeAmount: "100000000000", ExpiresAt: time.Now().Add(time.Hour),
		},
		pmt: payment.Payment{ID: "pay-resume-slippage", Amount: "0.001"}, pmtFound: true,
	}
	ethClient := &fakeExecutorEthClient{}
	e := newTestExecutor(t, store, ethClient)
	e.MaxFeeSlippageBps = 500 // 5%
	// Fresh fee is double the routing-time fee -- far beyond 5% tolerance.
	e.QuoteProviders = map[string]quote.Provider{"across": &fakeExecutorQuoteProvider{
		quote: quote.Quote{
			ProviderName: "across", Available: true,
			FeeBaseUnits: big.NewInt(200_000_000_000), OutputAmountBaseUnits: big.NewInt(800_000_000_000_000),
			RawProviderPayload: rawAcrossPayload(),
		},
	}}

	// An unsigned execution row -- the nonce is already allocated (crash
	// point B), but signing hasn't happened yet.
	exec := payment.Execution{ID: "exec-resume-slippage", PaymentID: "pay-resume-slippage", Nonce: 1}
	if err := e.DriveExecutionForward(context.Background(), exec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ethClient.sendCalled {
		t.Fatal("must never sign/broadcast an unsigned resumed execution whose fresh fee exceeds slippage tolerance")
	}
	if store.persistedHash != "" {
		t.Fatal("must never persist a signed tx built from a quote that exceeds slippage tolerance")
	}
	if store.broadcastCalled || store.submittedCalled {
		t.Fatal("must never mark broadcast/submitted when the fresh quote exceeds slippage tolerance")
	}
}

// TestExceedsSlippageTolerance_UnparseableBaselineDoesNotPanic guards the
// fix for the finding that an unparseable baselineFeeDecimal left baseline
// nil (SetString's ok=false case) but the old code still fell through to
// freshFee.Cmp(baseline), panicking on the nil *big.Int. Not reachable in
// production today (every write path populates FeeAmount via a real
// *big.Int's .String()), but a safety-critical helper like this must never
// panic on malformed input.
func TestExceedsSlippageTolerance_UnparseableBaselineDoesNotPanic(t *testing.T) {
	if !exceedsSlippageTolerance("not-a-number", big.NewInt(1), 500) {
		t.Error("expected a positive fresh fee against an unparseable baseline to be treated as exceeding tolerance")
	}
	if exceedsSlippageTolerance("not-a-number", big.NewInt(0), 500) {
		t.Error("expected a zero fresh fee against an unparseable baseline to not exceed tolerance")
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

// TestSignAndPersist_RejectsAmountExceedingMaxAmountWei guards the
// MAX_TESTNET_AMOUNT_WEI guardrail. Since Task 11, this check runs before
// nonce allocation, alongside expiry/slippage, and so is now a "handled
// outcome" (MarkProcessingFailed + nil error) rather than a hard Go error
// -- consistent with the other pre-nonce-allocation checks -- so this no
// longer uses assertQuoteRejected (which expects a returned error).
func TestSignAndPersist_RejectsAmountExceedingMaxAmountWei(t *testing.T) {
	store, ethClient := newRejectedQuoteTestSetup(t, "pay-reject-guardrail")
	e := newTestExecutor(t, store, ethClient)
	// 0.001 WETH = 10^15 wei, far above this ceiling.
	e.MaxAmountWei = big.NewInt(1)

	if err := e.ExecuteTestnetPayment(context.Background(), "pay-reject-guardrail"); err != nil {
		t.Fatalf("unexpected error (an over-guardrail amount is a handled outcome, not a Go error): %v", err)
	}
	if store.markFailedReason != "amount_exceeds_guardrail" {
		t.Errorf("markFailedReason = %q, want amount_exceeds_guardrail", store.markFailedReason)
	}
	if store.tryCreateCalled {
		t.Fatal("must never allocate a nonce when the amount exceeds the guardrail")
	}
	if ethClient.sendCalled {
		t.Fatal("must never call SendTransaction when the amount exceeds the guardrail")
	}
	if store.persistedHash != "" {
		t.Fatal("must never persist a signed tx built from an over-guardrail amount")
	}
	if store.broadcastCalled || store.submittedCalled {
		t.Fatal("must never mark broadcast/submitted when the amount exceeds the guardrail")
	}
}

// The tests below guard Task 11's core safety property: expiry and
// fee-slippage checks (plus provider lookup) must run BEFORE any nonce
// allocation (TryCreateExecution), since EVM nonces are strictly
// sequential per wallet and a routinely-failing check must never strand
// one.

func TestExecuteTestnetPayment_ExpiredQuoteFailsBeforeNonceAllocation(t *testing.T) {
	store := &fakeExecutorStore{
		quoteFound: true,
		quoteRow: payment.Quote{
			Provider: "across", OriginChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
			FeeAmount: "100000000000", ExpiresAt: time.Now().Add(-time.Minute), // already expired
		},
		pmt: payment.Payment{ID: "pay-expired", Amount: "0.001"}, pmtFound: true,
	}
	ethClient := &fakeExecutorEthClient{}
	e := newTestExecutor(t, store, ethClient)
	e.QuoteProviders = map[string]quote.Provider{"across": &fakeExecutorQuoteProvider{}}

	if err := e.ExecuteTestnetPayment(context.Background(), "pay-expired"); err != nil {
		t.Fatalf("unexpected error (an expiry rejection is a handled outcome, not a Go error): %v", err)
	}
	if store.markFailedReason != "routing_quote_expired" {
		t.Errorf("markFailedReason = %q, want routing_quote_expired", store.markFailedReason)
	}
	if store.tryCreateCalled {
		t.Fatal("must never allocate a nonce (call TryCreateExecution) when the routing quote has already expired")
	}
	if ethClient.sendCalled {
		t.Fatal("must never broadcast when the routing quote has already expired")
	}
}

func TestExecuteTestnetPayment_ExcessiveSlippageFailsBeforeNonceAllocation(t *testing.T) {
	store := &fakeExecutorStore{
		quoteFound: true,
		quoteRow: payment.Quote{
			Provider: "across", OriginChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
			FeeAmount: "100000000000", ExpiresAt: time.Now().Add(time.Hour),
		},
		pmt: payment.Payment{ID: "pay-slippage", Amount: "0.001"}, pmtFound: true,
	}
	ethClient := &fakeExecutorEthClient{}
	e := newTestExecutor(t, store, ethClient)
	e.MaxFeeSlippageBps = 500 // 5%
	// Fresh fee is double the routing-time fee -- far beyond 5% tolerance.
	e.QuoteProviders = map[string]quote.Provider{"across": &fakeExecutorQuoteProvider{
		quote: quote.Quote{
			ProviderName: "across", Available: true,
			FeeBaseUnits: big.NewInt(200_000_000_000), OutputAmountBaseUnits: big.NewInt(800_000_000_000_000),
			RawProviderPayload: rawAcrossPayload(),
		},
	}}

	if err := e.ExecuteTestnetPayment(context.Background(), "pay-slippage"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store.markFailedReason != "fee_slippage_exceeded" {
		t.Errorf("markFailedReason = %q, want fee_slippage_exceeded", store.markFailedReason)
	}
	if store.tryCreateCalled {
		t.Fatal("must never allocate a nonce when fresh fee exceeds slippage tolerance")
	}
	if ethClient.sendCalled {
		t.Fatal("must never broadcast when fresh fee exceeds slippage tolerance")
	}
}

func TestExecuteTestnetPayment_FeeDecreaseAlwaysPasses(t *testing.T) {
	store := &fakeExecutorStore{
		quoteFound: true,
		quoteRow: payment.Quote{
			Provider: "across", OriginChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
			FeeAmount: "200000000000", ExpiresAt: time.Now().Add(time.Hour),
		},
		tryCreateCreated: true,
		tryCreateExec:    payment.Execution{ID: "exec-cheaper", PaymentID: "pay-cheaper", Nonce: 1},
		pmt:              payment.Payment{ID: "pay-cheaper", Amount: "0.001"}, pmtFound: true,
	}
	ethClient := &fakeExecutorEthClient{txByHashFound: false, txByHashErr: gethereum.NotFound}
	e := newTestExecutor(t, store, ethClient)
	e.MaxFeeSlippageBps = 500
	// Fresh fee is LOWER than the routing-time fee -- must always pass,
	// regardless of tolerance (one-directional check, design doc §9).
	e.QuoteProviders = map[string]quote.Provider{"across": &fakeExecutorQuoteProvider{
		quote: quote.Quote{
			ProviderName: "across", Available: true,
			// InputAmountBaseUnits set (unlike the ExcessiveSlippage fixture
			// above, which never reaches signing) because this test proceeds
			// all the way through signAndBroadcastFresh, which builds the
			// real DepositV3 tx from freshQuote.InputAmountBaseUnits -- a nil
			// *big.Int there would panic inside the ABI packer. Consistent
			// with FeeBaseUnits: 1_000_000_000_000_000 (0.001 WETH, this
			// test's payment amount) - 950_000_000_000_000 = 50_000_000_000.
			InputAmountBaseUnits: big.NewInt(1_000_000_000_000_000), OutputAmountBaseUnits: big.NewInt(950_000_000_000_000),
			FeeBaseUnits:       big.NewInt(50_000_000_000),
			RawProviderPayload: rawAcrossPayload(),
		},
	}}

	if err := e.ExecuteTestnetPayment(context.Background(), "pay-cheaper"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store.markFailedReason != "" {
		t.Fatalf("expected no failure for a fee decrease, got reason %q", store.markFailedReason)
	}
	if !ethClient.sendCalled {
		t.Fatal("expected broadcast to proceed when the fresh fee is lower than the routing-time quote")
	}
}

// TestExecuteTestnetPayment_QuoteRouteMismatchIsHardError guards the fix for
// the final-review finding that nothing tied the persisted quote's route
// (quoteRow.OriginChainID/DestinationChainID) to what signAndBroadcastFresh
// actually signs against (e.OriginChainID/e.DestChainID, the Executor's own
// fixed constants). This pairs a quoteRow naming a DIFFERENT destination
// chain than the executor's configured DestChainID with an otherwise
// perfectly valid, available, non-slippage-tripping fresh quote, and asserts
// signAndBroadcastFresh's new guard rejects it before ever calling
// SendTransaction -- same "should be unreachable" hard-error category as
// TestExecuteTestnetPayment_UnknownPersistedProviderIsHardError below.
func TestExecuteTestnetPayment_QuoteRouteMismatchIsHardError(t *testing.T) {
	store, ethClient := newRejectedQuoteTestSetup(t, "pay-route-mismatch")
	store.quoteFound = true
	store.quoteRow = payment.Quote{
		Provider: "across", OriginChainID: 11155111, DestinationChainID: 999999, Asset: "WETH", // mismatched destination chain
		FeeAmount: "100000000000", ExpiresAt: time.Now().Add(time.Hour),
	}
	e := newTestExecutor(t, store, ethClient)
	e.QuoteProviders = map[string]quote.Provider{"across": &fakeExecutorQuoteProvider{
		quote: quote.Quote{
			ProviderName: "across", Available: true,
			FeeBaseUnits:          big.NewInt(100_000_000_000), // equal to baseline fee -- never trips slippage
			InputAmountBaseUnits:  big.NewInt(1_000_000_000_000_000),
			OutputAmountBaseUnits: big.NewInt(900_000_000_000_000),
			RawProviderPayload:    rawAcrossPayload(),
		},
	}}
	assertQuoteRejected(t, e, store, ethClient, "pay-route-mismatch")
}

// TestExecuteTestnetPayment_QuoteSpokePoolMismatchIsHardError is the
// SpokePool-address half of the same guard: a quoteRow whose origin/dest
// chain IDs match the executor, but whose decoded quote payload names a
// different SpokePool address than e.SpokePoolAddress, must also be
// rejected before signing.
func TestExecuteTestnetPayment_QuoteSpokePoolMismatchIsHardError(t *testing.T) {
	store, ethClient := newRejectedQuoteTestSetup(t, "pay-spokepool-mismatch")
	store.quoteFound = true
	store.quoteRow = payment.Quote{
		Provider: "across", OriginChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
		FeeAmount: "100000000000", ExpiresAt: time.Now().Add(time.Hour),
	}
	e := newTestExecutor(t, store, ethClient)
	mismatchedPayload := json.RawMessage(`{"exclusiveRelayer":"0x0000000000000000000000000000000000000000","quoteTimestamp":"1789340112","fillDeadline":"1789347312","exclusivityDeadline":0,"spokePoolAddress":"0x000000000000000000000000000000000000dEaD"}`)
	e.QuoteProviders = map[string]quote.Provider{"across": &fakeExecutorQuoteProvider{
		quote: quote.Quote{
			ProviderName: "across", Available: true,
			FeeBaseUnits:          big.NewInt(100_000_000_000),
			InputAmountBaseUnits:  big.NewInt(1_000_000_000_000_000),
			OutputAmountBaseUnits: big.NewInt(900_000_000_000_000),
			RawProviderPayload:    mismatchedPayload,
		},
	}}
	assertQuoteRejected(t, e, store, ethClient, "pay-spokepool-mismatch")
}

// TestDriveExecutionForward_UnsignedResumedExecRejectsAmountExceedingMaxAmountWei
// guards the fix for the final-review finding that MAX_TESTNET_AMOUNT_WEI was
// enforced on the first-attempt path (ExecuteTestnetPayment) but not on
// DriveExecutionForward's unsigned-row resume branch (crash point B). An
// operator lowering the ceiling and restarting the worker must still stop an
// in-flight, not-yet-signed payment from broadcasting.
func TestDriveExecutionForward_UnsignedResumedExecRejectsAmountExceedingMaxAmountWei(t *testing.T) {
	store := &fakeExecutorStore{
		quoteFound: true,
		quoteRow: payment.Quote{
			Provider: "across", OriginChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
			FeeAmount: "100000000000", ExpiresAt: time.Now().Add(time.Hour),
		},
		pmt: payment.Payment{ID: "pay-resume-ceiling", Amount: "0.001"}, pmtFound: true,
	}
	ethClient := &fakeExecutorEthClient{}
	e := newTestExecutor(t, store, ethClient)
	e.MaxAmountWei = big.NewInt(1) // 0.001 WETH = 10^15 wei, far above this ceiling
	e.QuoteProviders = map[string]quote.Provider{"across": &fakeExecutorQuoteProvider{
		quote: quote.Quote{
			ProviderName: "across", Available: true,
			FeeBaseUnits:          big.NewInt(100_000_000_000), // equal to baseline -- never trips slippage
			InputAmountBaseUnits:  big.NewInt(1_000_000_000_000_000),
			OutputAmountBaseUnits: big.NewInt(900_000_000_000_000),
			RawProviderPayload:    rawAcrossPayload(),
		},
	}}

	// An unsigned execution row -- the nonce is already allocated (crash
	// point B), but signing hasn't happened yet.
	exec := payment.Execution{ID: "exec-resume-ceiling", PaymentID: "pay-resume-ceiling", Nonce: 1}
	if err := e.DriveExecutionForward(context.Background(), exec); err != nil {
		t.Fatalf("unexpected error (an over-guardrail amount is a handled outcome, not a Go error): %v", err)
	}
	if store.markFailedReason != "amount_exceeds_guardrail" {
		t.Errorf("markFailedReason = %q, want amount_exceeds_guardrail", store.markFailedReason)
	}
	if ethClient.sendCalled {
		t.Fatal("must never sign/broadcast a resumed unsigned execution whose amount exceeds MAX_TESTNET_AMOUNT_WEI")
	}
	if store.persistedHash != "" {
		t.Fatal("must never persist a signed tx built from an over-guardrail amount on resume")
	}
	if store.broadcastCalled || store.submittedCalled {
		t.Fatal("must never mark broadcast/submitted when the amount exceeds the guardrail on resume")
	}
}

func TestExecuteTestnetPayment_UnknownPersistedProviderIsHardError(t *testing.T) {
	store := &fakeExecutorStore{
		quoteFound: true,
		quoteRow:   payment.Quote{Provider: "some-future-provider", ExpiresAt: time.Now().Add(time.Hour)},
		pmt:        payment.Payment{ID: "pay-unknown-provider", Amount: "0.001"}, pmtFound: true,
	}
	ethClient := &fakeExecutorEthClient{}
	e := newTestExecutor(t, store, ethClient)
	e.QuoteProviders = map[string]quote.Provider{"across": &fakeExecutorQuoteProvider{}} // no entry for "some-future-provider"

	err := e.ExecuteTestnetPayment(context.Background(), "pay-unknown-provider")
	if err == nil {
		t.Fatal("expected a hard error when the persisted provider has no configured client -- never a silent substitution")
	}
	if store.tryCreateCalled {
		t.Fatal("must never allocate a nonce for a provider this worker cannot execute")
	}
}
