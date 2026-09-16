package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/big"
	"strconv"
	"time"

	gethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"chainroute/go-api/internal/bridge/across"
	"chainroute/go-api/internal/bridge/quote"
	"chainroute/go-api/internal/evm"
	"chainroute/go-api/internal/money"
	"chainroute/go-api/internal/payment"
	"chainroute/go-api/internal/postgres"
)

const wethDecimals = 18

// ExecutorStore is the subset of *postgres.Store the executor needs.
type ExecutorStore interface {
	TryCreateExecution(ctx context.Context, p postgres.CreateExecutionParams) (payment.Execution, bool, error)
	GetPayment(ctx context.Context, id string) (payment.Payment, bool, error)
	GetQuoteByPaymentID(ctx context.Context, paymentID string) (payment.Quote, bool, error)
	MarkProcessingFailed(ctx context.Context, paymentID, reason string) (bool, error)
	PersistSignedExecution(ctx context.Context, executionID string, rawTx []byte, txHash string, providerReferenceID *string) error
	MarkExecutionBroadcast(ctx context.Context, executionID string) error
	MarkSubmitted(ctx context.Context, paymentID string) (bool, error)
}

// ExecutorEthClient is the minimal ethclient.Client surface the executor
// needs against the ORIGIN chain (Sepolia) -- across.EthClient's
// SuggestGasPrice/EstimateGas plus the broadcast/lookup calls this file
// itself needs. *ethclient.Client satisfies both interfaces structurally.
type ExecutorEthClient interface {
	across.EthClient
	SendTransaction(ctx context.Context, tx *types.Transaction) error
	TransactionByHash(ctx context.Context, hash common.Hash) (tx *types.Transaction, isPending bool, err error)
}

// Executor drives one testnet-mode payment from a claimed PROCESSING state
// through to a broadcast, SUBMITTED transaction. It is deliberately the
// same code path whether starting fresh (ExecuteTestnetPayment) or
// resuming an existing execution row after a crash or via the reconciler
// (DriveExecutionForward directly) -- design spec §15.
type Executor struct {
	Store             ExecutorStore
	Wallet            *evm.Wallet
	OriginClient      ExecutorEthClient
	Across            *across.Client
	QuoteProviders    map[string]quote.Provider // keyed by provider name, e.g. "across"
	MaxFeeSlippageBps int64
	BridgeProvider    string
	OriginChainID     int64
	DestChainID       int64
	SpokePoolAddress  common.Address
	WETHOrigin        common.Address
	WETHDestination   common.Address
	MaxAmountWei      *big.Int
}

// ExecuteTestnetPayment runs the expiry/provider-lookup/fresh-quote/
// slippage checks BEFORE ever allocating a wallet nonce (design spec §9,
// §21): EVM nonces are strictly sequential per wallet, so allocating one
// for a payment that then fails a check that can legitimately and
// routinely fail (market fees move constantly) would permanently block
// every higher nonce on this wallet from ever mining. Only once every
// check has passed does this call TryCreateExecution (design spec §15's
// UNIQUE(payment_id)-arbitrated race) and, only if it actually won that
// race, sign and broadcast. A lost race (created=false) is a safe no-op:
// some other actor -- the reconciler, on a concurrent tick -- already owns
// this payment's execution.
func (e *Executor) ExecuteTestnetPayment(ctx context.Context, paymentID string) error {
	quoteRow, found, err := e.Store.GetQuoteByPaymentID(ctx, paymentID)
	if err != nil {
		return fmt.Errorf("get quote for payment %s: %w", paymentID, err)
	}
	if !found {
		return fmt.Errorf("payment %s has no payment_quotes row -- cannot execute without a selected route", paymentID)
	}

	if time.Now().After(quoteRow.ExpiresAt) {
		if _, err := e.Store.MarkProcessingFailed(ctx, paymentID, "routing_quote_expired"); err != nil {
			return fmt.Errorf("mark payment %s failed (routing_quote_expired): %w", paymentID, err)
		}
		return nil
	}

	provider, ok := e.QuoteProviders[quoteRow.Provider]
	if !ok {
		return fmt.Errorf("payment %s uses provider %q, which this worker has no configured client for -- refusing to substitute a different provider", paymentID, quoteRow.Provider)
	}

	p, pFound, err := e.Store.GetPayment(ctx, paymentID)
	if err != nil {
		return fmt.Errorf("get payment: %w", err)
	}
	if !pFound {
		return fmt.Errorf("payment %s not found", paymentID)
	}
	inputAmount, err := money.DecimalToBaseUnits(p.Amount, wethDecimals)
	if err != nil {
		return fmt.Errorf("convert amount %q: %w", p.Amount, err)
	}

	freshQuote, err := provider.GetQuote(ctx, quote.Request{
		SourceChainID: quoteRow.OriginChainID, DestinationChainID: quoteRow.DestinationChainID,
		Asset: quoteRow.Asset, AmountBaseUnits: inputAmount,
	})
	if err != nil {
		return fmt.Errorf("fetch fresh quote for payment %s: %w", paymentID, err)
	}
	if !freshQuote.Available {
		if _, err := e.Store.MarkProcessingFailed(ctx, paymentID, "route_unavailable"); err != nil {
			return fmt.Errorf("mark payment %s failed (route no longer available): %w", paymentID, err)
		}
		return nil
	}

	if exceedsSlippageTolerance(quoteRow.FeeAmount, freshQuote.FeeBaseUnits, e.MaxFeeSlippageBps) {
		if _, err := e.Store.MarkProcessingFailed(ctx, paymentID, "fee_slippage_exceeded"); err != nil {
			return fmt.Errorf("mark payment %s failed (fee_slippage_exceeded): %w", paymentID, err)
		}
		return nil
	}

	if e.MaxAmountWei != nil && inputAmount.Cmp(e.MaxAmountWei) > 0 {
		if _, err := e.Store.MarkProcessingFailed(ctx, paymentID, "amount_exceeds_guardrail"); err != nil {
			return fmt.Errorf("mark payment %s failed (amount_exceeds_guardrail): %w", paymentID, err)
		}
		return nil
	}

	exec, created, err := e.Store.TryCreateExecution(ctx, postgres.CreateExecutionParams{
		PaymentID: paymentID, WalletAddress: e.Wallet.Address.Hex(), BridgeProvider: e.BridgeProvider,
		OriginChainID: e.OriginChainID, DestinationChainID: e.DestChainID,
	})
	if err != nil {
		return fmt.Errorf("try create execution for payment %s: %w", paymentID, err)
	}
	if !created {
		return nil
	}
	return e.signAndBroadcastFresh(ctx, exec, quoteRow, freshQuote)
}

// exceedsSlippageTolerance reports whether freshFee exceeds baselineFee
// (a decimal base-units string, e.g. payment_quotes.fee_amount) by more
// than toleranceBps basis points. The check is one-directional (design
// doc §9): a fresh fee equal to or lower than the baseline never exceeds
// tolerance, regardless of toleranceBps.
func exceedsSlippageTolerance(baselineFeeDecimal string, freshFee *big.Int, toleranceBps int64) bool {
	baseline, ok := new(big.Int).SetString(baselineFeeDecimal, 10)
	if !ok {
		// An unparseable baseline is never a valid *big.Int -- return
		// directly rather than falling through to Cmp(nil), which would
		// panic. Any positive fresh fee is treated as exceeding tolerance
		// against a baseline we can't make sense of.
		return freshFee.Sign() > 0
	}
	if baseline.Sign() == 0 {
		// A zero baseline can't be exceeded by a positive tolerance-relative
		// amount in a well-defined way; treat any positive fresh fee
		// increase over a zero baseline as exceeding tolerance rather than
		// dividing by zero.
		return freshFee.Cmp(baseline) > 0
	}
	if freshFee.Cmp(baseline) <= 0 {
		return false
	}
	increase := new(big.Int).Sub(freshFee, baseline)
	// increase/baseline > toleranceBps/10000  <=>  increase*10000 > baseline*toleranceBps
	lhs := new(big.Int).Mul(increase, big.NewInt(10000))
	rhs := new(big.Int).Mul(baseline, big.NewInt(toleranceBps))
	return lhs.Cmp(rhs) > 0
}

// DriveExecutionForward is the resume entry point used by BOTH
// crash-recovery and the reconciler. For an already-signed row (crash
// point C/D/E/F: exec.SignedTxHash != nil), it goes straight to broadcast
// recovery using the persisted bytes -- checked FIRST, before touching
// payment_quotes or the provider map at all. This must not be reordered:
// an already-signed row is typically resumed later (by the reconciler, or
// on worker restart), by which point the routing-time quote has very
// plausibly expired -- if the expiry check ran first, it would call
// MarkProcessingFailed and mark the payment FAILED even though its
// transaction may already be sitting on-chain, mined or pending, which is
// exactly the "never re-derive/re-decide for an already-signed row"
// guarantee this task exists to protect. The already-signed path therefore
// has zero new preconditions in front of it, unchanged from Phase 7: it
// never re-quotes or re-checks slippage or expiry, since re-deriving
// anything at that point risks constructing a second, distinct transaction
// for the same payment. Only the unsigned-row path (crash point B) needs
// quoteRow/expiry/provider/fresh-quote/slippage -- that logic runs only in
// the else branch below, mirroring ExecuteTestnetPayment's own sequence,
// since the nonce is already allocated by the time this is reached but the
// payment must still never be signed and broadcast against a stale or
// now-unfavorable quote.
func (e *Executor) DriveExecutionForward(ctx context.Context, exec payment.Execution) error {
	if exec.SignedTxHash != nil {
		// Already signed (crash point C/D/E/F) -- go straight to broadcast
		// recovery using the persisted bytes, never re-quote or re-check
		// expiry/slippage for an already-signed row (design doc §8 concern
		// 2's protocol-freshness argument only applies BEFORE signing; once
		// signed, re-deriving anything risks a second distinct transaction).
		if err := e.broadcastWithRecovery(ctx, exec); err != nil {
			return fmt.Errorf("broadcast execution %s: %w", exec.ID, err)
		}
		if err := e.Store.MarkExecutionBroadcast(ctx, exec.ID); err != nil {
			return fmt.Errorf("mark execution %s broadcast: %w", exec.ID, err)
		}
		_, err := e.Store.MarkSubmitted(ctx, exec.PaymentID)
		return err
	}

	quoteRow, found, err := e.Store.GetQuoteByPaymentID(ctx, exec.PaymentID)
	if err != nil {
		return fmt.Errorf("get quote for payment %s: %w", exec.PaymentID, err)
	}
	if !found {
		return fmt.Errorf("payment %s has no payment_quotes row -- cannot resume execution without a selected route", exec.PaymentID)
	}
	if time.Now().After(quoteRow.ExpiresAt) {
		if _, err := e.Store.MarkProcessingFailed(ctx, exec.PaymentID, "routing_quote_expired"); err != nil {
			return fmt.Errorf("mark payment %s failed (routing_quote_expired): %w", exec.PaymentID, err)
		}
		return nil
	}
	provider, ok := e.QuoteProviders[quoteRow.Provider]
	if !ok {
		return fmt.Errorf("payment %s uses provider %q, which this worker has no configured client for", exec.PaymentID, quoteRow.Provider)
	}

	p, pFound, err := e.Store.GetPayment(ctx, exec.PaymentID)
	if err != nil {
		return fmt.Errorf("get payment: %w", err)
	}
	if !pFound {
		return fmt.Errorf("payment %s not found", exec.PaymentID)
	}
	inputAmount, err := money.DecimalToBaseUnits(p.Amount, wethDecimals)
	if err != nil {
		return fmt.Errorf("convert amount %q: %w", p.Amount, err)
	}
	freshQuote, err := provider.GetQuote(ctx, quote.Request{
		SourceChainID: quoteRow.OriginChainID, DestinationChainID: quoteRow.DestinationChainID,
		Asset: quoteRow.Asset, AmountBaseUnits: inputAmount,
	})
	if err != nil {
		return fmt.Errorf("fetch fresh quote for payment %s: %w", exec.PaymentID, err)
	}
	if !freshQuote.Available || exceedsSlippageTolerance(quoteRow.FeeAmount, freshQuote.FeeBaseUnits, e.MaxFeeSlippageBps) {
		// Disclosed limitation (design doc §9, §13 point Q): this nonce is
		// already allocated and stays allocated -- MarkProcessingFailed is
		// NOT called here, because the payment is not stuck PROCESSING
		// without an execution row (crash point A's case); it is stuck
		// WITH an allocated nonce, which MarkProcessingFailed's guard
		// (WHERE status = 'PROCESSING') would still technically satisfy,
		// but doing so would misrepresent a stuck-nonce situation as a
		// cleanly-failed one with no lingering state. Log loudly instead;
		// resolving the underlying nonce is a manual operator action.
		log.Printf("WARNING: execution %s (payment %s, nonce %d) resumed with an already-allocated nonce but the fresh quote is unavailable or exceeds slippage tolerance -- this nonce cannot proceed and will keep blocking higher nonces on wallet %s until an operator intervenes", exec.ID, exec.PaymentID, exec.Nonce, exec.WalletAddress)
		return nil
	}

	if e.MaxAmountWei != nil && inputAmount.Cmp(e.MaxAmountWei) > 0 {
		if _, err := e.Store.MarkProcessingFailed(ctx, exec.PaymentID, "amount_exceeds_guardrail"); err != nil {
			return fmt.Errorf("mark payment %s failed (amount_exceeds_guardrail): %w", exec.PaymentID, err)
		}
		return nil
	}

	return e.signAndBroadcastFresh(ctx, exec, quoteRow, freshQuote)
}

// signAndBroadcastFresh builds DepositV3Params from freshQuote (via
// across.DecodeQuotePayload) instead of calling e.Across.SuggestedFees or
// re-validating anything itself -- the validation across.Provider.GetQuote
// already performed (Task 6) is what makes this safe: signAndBroadcastFresh
// trusts freshQuote came from a provider call, not raw user input.
//
// It also re-asserts, right before building the transaction, that the
// persisted routing-time quote (quoteRow) actually agrees with this
// Executor's own fixed chain/SpokePool constants. Nothing else in this call
// chain ties "the route that was quoted and persisted" to "the route this
// Executor is hardwired to sign for" -- across.Provider.GetQuote only checks
// its own request against its own response, which says nothing about
// whether quoteRow.OriginChainID/DestinationChainID (the persisted,
// routing-time selection) match e.OriginChainID/e.DestChainID. If a second
// route were ever registered for the same provider name but a different
// destination, the provider-name lookup upstream would still succeed and
// this would otherwise silently sign against the WRONG chain/SpokePool. Not
// reachable today (exactly one route is registered), but this is the exact
// class of bug design spec §21 exists to prevent, so it is a hard,
// should-be-unreachable error, never a silent substitution.
func (e *Executor) signAndBroadcastFresh(ctx context.Context, exec payment.Execution, quoteRow payment.Quote, freshQuote quote.Quote) error {
	if quoteRow.OriginChainID != e.OriginChainID || quoteRow.DestinationChainID != e.DestChainID {
		return fmt.Errorf("execution %s: persisted quote route (origin=%d, dest=%d) does not match this executor's configured route (origin=%d, dest=%d) -- refusing to sign against a mismatched route",
			exec.ID, quoteRow.OriginChainID, quoteRow.DestinationChainID, e.OriginChainID, e.DestChainID)
	}

	payload, err := across.DecodeQuotePayload(freshQuote.RawProviderPayload)
	if err != nil {
		return fmt.Errorf("decode across quote payload for execution %s: %w", exec.ID, err)
	}

	if quotedSpokePool := common.HexToAddress(payload.SpokePoolAddress); quotedSpokePool != e.SpokePoolAddress {
		return fmt.Errorf("execution %s: quoted SpokePool address %s does not match this executor's configured SpokePool %s -- refusing to sign against a mismatched route",
			exec.ID, quotedSpokePool.Hex(), e.SpokePoolAddress.Hex())
	}

	quoteTimestamp, err := strconv.ParseUint(payload.QuoteTimestamp, 10, 32)
	if err != nil {
		return fmt.Errorf("parse quote timestamp %q: %w", payload.QuoteTimestamp, err)
	}
	fillDeadline, err := strconv.ParseUint(payload.FillDeadline, 10, 32)
	if err != nil {
		return fmt.Errorf("parse fill deadline %q: %w", payload.FillDeadline, err)
	}

	signedTx, err := across.BuildAndSignDepositV3Tx(ctx, e.OriginClient, e.Wallet, e.OriginChainID, e.SpokePoolAddress, uint64(exec.Nonce), across.DepositV3Params{
		Recipient: e.Wallet.Address, InputToken: e.WETHOrigin, OutputToken: e.WETHDestination,
		InputAmount: freshQuote.InputAmountBaseUnits, OutputAmount: freshQuote.OutputAmountBaseUnits,
		DestinationChainID: big.NewInt(e.DestChainID), ExclusiveRelayer: common.HexToAddress(payload.ExclusiveRelayer),
		QuoteTimestamp: uint32(quoteTimestamp), FillDeadline: uint32(fillDeadline),
		ExclusivityDeadline: uint32(payload.ExclusivityDeadline),
	})
	if err != nil {
		return fmt.Errorf("build/sign tx: %w", err)
	}

	rawTx, err := signedTx.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal signed tx: %w", err)
	}
	hash := signedTx.Hash().Hex()
	if err := e.Store.PersistSignedExecution(ctx, exec.ID, rawTx, hash, nil); err != nil {
		return fmt.Errorf("persist signed execution: %w", err)
	}
	exec.SignedTxHash = &hash
	exec.RawSignedTx = rawTx

	if err := e.broadcastWithRecovery(ctx, exec); err != nil {
		return fmt.Errorf("broadcast execution %s: %w", exec.ID, err)
	}
	if err := e.Store.MarkExecutionBroadcast(ctx, exec.ID); err != nil {
		return fmt.Errorf("mark execution %s broadcast: %w", exec.ID, err)
	}
	if _, err := e.Store.MarkSubmitted(ctx, exec.PaymentID); err != nil {
		return fmt.Errorf("mark payment %s submitted: %w", exec.PaymentID, err)
	}
	return nil
}

// broadcastWithRecovery implements design spec §8 step 4 / §14: on ANY
// attempt (fresh or resumed), the FIRST action is always a read -- check
// the chain for the precomputed signed_tx_hash. Only if that read
// confirms the transaction does not exist does this rebroadcast, and even
// then it rebroadcasts the exact persisted bytes, never a re-signed one.
func (e *Executor) broadcastWithRecovery(ctx context.Context, exec payment.Execution) error {
	if exec.SignedTxHash == nil || exec.RawSignedTx == nil {
		return fmt.Errorf("execution %s has no signed transaction to broadcast", exec.ID)
	}
	hash := common.HexToHash(*exec.SignedTxHash)

	if _, _, err := e.OriginClient.TransactionByHash(ctx, hash); err == nil {
		// Already known to the chain, pending or mined -- never
		// rebroadcast or re-sign.
		return nil
	} else if !errors.Is(err, gethereum.NotFound) {
		// A genuine lookup failure (RPC timeout, connection error, etc.)
		// is NOT the same as a confirmed "the chain has never seen this
		// tx" -- only ethereum.NotFound means that. Anything else must
		// propagate as an error rather than being silently treated as
		// license to rebroadcast against a possibly-flaky endpoint.
		return fmt.Errorf("check transaction %s on chain: %w", hash.Hex(), err)
	}

	var tx types.Transaction
	if err := tx.UnmarshalBinary(exec.RawSignedTx); err != nil {
		return fmt.Errorf("unmarshal persisted signed tx: %w", err)
	}
	if err := e.OriginClient.SendTransaction(ctx, &tx); err != nil {
		// This error is itself ambiguous -- the node may have accepted
		// the tx before the error surfaced. Do NOT treat this as
		// terminal; leave broadcast_at unset so the next attempt
		// re-runs this exact hash-check-then-broadcast sequence.
		return fmt.Errorf("send transaction: %w", err)
	}
	return nil
}
