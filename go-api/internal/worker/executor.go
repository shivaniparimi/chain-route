package worker

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strconv"

	gethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"chainroute/go-api/internal/bridge/across"
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
	PersistSignedExecution(ctx context.Context, executionID string, rawTx []byte, txHash string) error
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
	Store            ExecutorStore
	Wallet           *evm.Wallet
	OriginClient     ExecutorEthClient
	Across           *across.Client
	BridgeProvider   string
	OriginChainID    int64
	DestChainID      int64
	SpokePoolAddress common.Address
	WETHOrigin       common.Address
	WETHDestination  common.Address
	MaxAmountWei     *big.Int
}

// ExecuteTestnetPayment claims an execution identity for paymentID (via
// TryCreateExecution's UNIQUE(payment_id)-arbitrated race, design spec
// §15) and, only if this call actually won that race, drives it forward.
// A lost race (created=false) is a safe no-op: some other actor -- the
// reconciler, on a concurrent tick -- already owns this payment's
// execution.
func (e *Executor) ExecuteTestnetPayment(ctx context.Context, paymentID string) error {
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
	return e.DriveExecutionForward(ctx, exec)
}

// DriveExecutionForward advances exec to broadcast + SUBMITTED, picking up
// from wherever it currently is: signs if unsigned (crash point B),
// broadcasts with ambiguous-outcome recovery if unbroadcast (crash point
// C/D/E/F), then marks the payment SUBMITTED. This is exactly what makes
// it safe for the reconciler to call this same method on a resumed row
// (design spec §15) -- there is no separate "resume" code path to drift
// out of sync with the fresh-execution path.
func (e *Executor) DriveExecutionForward(ctx context.Context, exec payment.Execution) error {
	if exec.SignedTxHash == nil {
		if err := e.signAndPersist(ctx, &exec); err != nil {
			return fmt.Errorf("sign execution %s: %w", exec.ID, err)
		}
	}

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

func (e *Executor) signAndPersist(ctx context.Context, exec *payment.Execution) error {
	p, found, err := e.Store.GetPayment(ctx, exec.PaymentID)
	if err != nil {
		return fmt.Errorf("get payment: %w", err)
	}
	if !found {
		return fmt.Errorf("payment %s not found", exec.PaymentID)
	}

	inputAmount, err := money.DecimalToBaseUnits(p.Amount, wethDecimals)
	if err != nil {
		return fmt.Errorf("convert amount %q: %w", p.Amount, err)
	}
	if e.MaxAmountWei != nil && inputAmount.Cmp(e.MaxAmountWei) > 0 {
		return fmt.Errorf("amount exceeds MAX_TESTNET_AMOUNT_WEI guardrail")
	}

	quote, err := e.Across.SuggestedFees(ctx, e.OriginChainID, e.DestChainID,
		e.WETHOrigin.Hex(), e.WETHDestination.Hex(), inputAmount.String())
	if err != nil {
		return fmt.Errorf("get quote: %w", err)
	}
	if err := e.validateQuote(quote); err != nil {
		return fmt.Errorf("reject quote: %w", err)
	}

	outputAmount, ok := new(big.Int).SetString(quote.OutputAmount, 10)
	if !ok {
		return fmt.Errorf("across returned a non-integer outputAmount %q", quote.OutputAmount)
	}
	quoteTimestamp, err := strconv.ParseUint(quote.Timestamp, 10, 32)
	if err != nil {
		return fmt.Errorf("parse quote timestamp %q: %w", quote.Timestamp, err)
	}
	fillDeadline, err := strconv.ParseUint(quote.FillDeadline, 10, 32)
	if err != nil {
		return fmt.Errorf("parse fill deadline %q: %w", quote.FillDeadline, err)
	}

	signedTx, err := across.BuildAndSignDepositV3Tx(ctx, e.OriginClient, e.Wallet, e.OriginChainID, e.SpokePoolAddress, uint64(exec.Nonce), across.DepositV3Params{
		Recipient: e.Wallet.Address, InputToken: e.WETHOrigin, OutputToken: e.WETHDestination,
		InputAmount: inputAmount, OutputAmount: outputAmount, DestinationChainID: big.NewInt(e.DestChainID),
		ExclusiveRelayer:    common.HexToAddress(quote.ExclusiveRelayer),
		QuoteTimestamp:      uint32(quoteTimestamp),
		FillDeadline:        uint32(fillDeadline),
		ExclusivityDeadline: uint32(quote.ExclusivityDeadline),
	})
	if err != nil {
		return fmt.Errorf("build/sign tx: %w", err)
	}

	rawTx, err := signedTx.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal signed tx: %w", err)
	}
	hash := signedTx.Hash().Hex()
	if err := e.Store.PersistSignedExecution(ctx, exec.ID, rawTx, hash); err != nil {
		return fmt.Errorf("persist signed execution: %w", err)
	}
	exec.SignedTxHash = &hash
	exec.RawSignedTx = rawTx
	return nil
}

// validateQuote rejects an Across response before it can ever reach a
// signing call (design spec §21): the quote's own echoed chain IDs and
// token addresses must match what was requested.
func (e *Executor) validateQuote(q across.SuggestedFeesResponse) error {
	if q.InputToken.ChainID != e.OriginChainID {
		return fmt.Errorf("quote inputToken.chainId %d does not match origin chain %d", q.InputToken.ChainID, e.OriginChainID)
	}
	if q.OutputToken.ChainID != e.DestChainID {
		return fmt.Errorf("quote outputToken.chainId %d does not match destination chain %d", q.OutputToken.ChainID, e.DestChainID)
	}
	if common.HexToAddress(q.InputToken.Address) != e.WETHOrigin {
		return fmt.Errorf("quote inputToken.address %s does not match expected origin WETH %s", q.InputToken.Address, e.WETHOrigin.Hex())
	}
	if common.HexToAddress(q.OutputToken.Address) != e.WETHDestination {
		return fmt.Errorf("quote outputToken.address %s does not match expected destination WETH %s", q.OutputToken.Address, e.WETHDestination.Hex())
	}
	if _, ok := new(big.Int).SetString(q.OutputAmount, 10); !ok {
		return fmt.Errorf("quote outputAmount %q is not a valid integer", q.OutputAmount)
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
