package across

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"chainroute/go-api/internal/bridge/quote"
	"chainroute/go-api/internal/evm"
)

// ethereumCallMsg is a local alias so this file's public EthClient
// interface doesn't leak the go-ethereum import path into every caller's
// type signature verbatim -- purely a readability convenience.
type ethereumCallMsg = ethereum.CallMsg

// EthClient is the minimal ethclient.Client surface this file needs,
// small enough to fake in tests without a real RPC endpoint.
type EthClient interface {
	SuggestGasPrice(ctx context.Context) (*big.Int, error)
	EstimateGas(ctx context.Context, msg ethereumCallMsg) (uint64, error)
}

// spokePoolDepositV3ABI is the exact depositV3 function fragment pulled
// directly from across-protocol/contracts' deployed ABI (verified live
// during Phase 7 planning -- see this plan's "Verified Across testnet
// facts" section, and Task 1's findings doc for any drift since).
const spokePoolDepositV3ABI = `[{"inputs":[{"internalType":"address","name":"depositor","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"address","name":"inputToken","type":"address"},{"internalType":"address","name":"outputToken","type":"address"},{"internalType":"uint256","name":"inputAmount","type":"uint256"},{"internalType":"uint256","name":"outputAmount","type":"uint256"},{"internalType":"uint256","name":"destinationChainId","type":"uint256"},{"internalType":"address","name":"exclusiveRelayer","type":"address"},{"internalType":"uint32","name":"quoteTimestamp","type":"uint32"},{"internalType":"uint32","name":"fillDeadline","type":"uint32"},{"internalType":"uint32","name":"exclusivityDeadline","type":"uint32"},{"internalType":"bytes","name":"message","type":"bytes"}],"name":"depositV3","outputs":[],"stateMutability":"payable","type":"function"}]`

var spokePoolABI = func() abi.ABI {
	parsed, err := abi.JSON(strings.NewReader(spokePoolDepositV3ABI))
	if err != nil {
		panic(fmt.Sprintf("across: invalid embedded SpokePool ABI: %v", err))
	}
	return parsed
}()

// DepositV3Params is everything BuildAndSignDepositV3Tx needs beyond the
// signer's own address (used as both depositor and recipient -- Phase 7
// does not implement separate recipient management) and the SpokePool
// address. InputAmount MUST come from money.DecimalToBaseUnits, never a
// float conversion. OutputAmount/ExclusiveRelayer/QuoteTimestamp/
// FillDeadline/ExclusivityDeadline come directly from a freshly-fetched
// SuggestedFeesResponse (design spec §21: an unvalidated external
// response never flows directly into a signing call -- the caller is
// responsible for the chain-ID/token-address validation described there
// before populating this struct).
type DepositV3Params struct {
	Recipient           common.Address
	InputToken          common.Address
	OutputToken         common.Address
	InputAmount         *big.Int
	OutputAmount        *big.Int
	DestinationChainID  *big.Int
	ExclusiveRelayer    common.Address
	QuoteTimestamp      uint32
	FillDeadline        uint32
	ExclusivityDeadline uint32
}

// BuildAndSignDepositV3Tx constructs and signs a depositV3 call as a
// native-ETH deposit: msg.value = params.InputAmount, params.InputToken
// set to the WETH address, and NO separate approve() transaction --
// depositV3 auto-wraps native ETH when InputToken is the chain's
// wrapped-native address (confirmed live during Phase 7 planning; see
// this plan's "Verified Across testnet facts"). This deliberately avoids
// a two-step approve()+depositV3 flow, which would consume two nonces per
// payment and break the one-execution-row-one-nonce design.
//
// nonce MUST be the value already durably allocated and persisted in the
// payment's payment_executions row (design spec §8) -- this function
// never allocates a nonce itself. The returned transaction is signed but
// NOT broadcast; the caller (Task 12/13) is responsible for persisting
// its raw bytes and hash BEFORE attempting to broadcast it.
func BuildAndSignDepositV3Tx(ctx context.Context, client EthClient, wallet *evm.Wallet, originChainID int64, spokePool common.Address, nonce uint64, params DepositV3Params) (*types.Transaction, error) {
	calldata, err := spokePoolABI.Pack("depositV3",
		wallet.Address, params.Recipient, params.InputToken, params.OutputToken,
		params.InputAmount, params.OutputAmount, params.DestinationChainID,
		params.ExclusiveRelayer, params.QuoteTimestamp, params.FillDeadline, params.ExclusivityDeadline,
		[]byte{},
	)
	if err != nil {
		return nil, fmt.Errorf("pack depositV3 calldata: %w", err)
	}

	gasPrice, err := client.SuggestGasPrice(ctx)
	if err != nil {
		return nil, fmt.Errorf("suggest gas price: %w", err)
	}

	// A conservative fallback gas limit if estimation fails (e.g. the RPC
	// endpoint doesn't support eth_estimateGas against a not-yet-mined
	// state). SpokePool deposit calls typically cost well under this on
	// Sepolia/Base Sepolia; this is a safety ceiling, not a tuned value.
	const fallbackGasLimit = 500_000
	gasLimit, err := client.EstimateGas(ctx, ethereum.CallMsg{
		From:  wallet.Address,
		To:    &spokePool,
		Value: params.InputAmount,
		Data:  calldata,
	})
	if err != nil {
		gasLimit = fallbackGasLimit
	}

	unsignedTx := types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		To:       &spokePool,
		Value:    params.InputAmount,
		Gas:      gasLimit,
		GasPrice: gasPrice,
		Data:     calldata,
	})

	signed, err := wallet.SignTx(unsignedTx, big.NewInt(originChainID))
	if err != nil {
		return nil, fmt.Errorf("sign depositV3 tx: %w", err)
	}
	return signed, nil
}

// BuildTransaction implements quote.Signer. It is the same depositV3
// native-ETH-deposit calldata BuildAndSignDepositV3Tx packs (msg.value =
// freshQuote.InputAmountBaseUnits, WalletAddress as both depositor and
// recipient, no separate approve() transaction), relocated behind the
// shared Signer interface so Executor no longer needs an Across-specific
// code path to build a transaction to sign. Unlike
// BuildAndSignDepositV3Tx, it does not estimate gas, suggest a gas price,
// or sign -- Executor (design doc §11) is the sole owner of those steps
// for every provider.
func (p *Provider) BuildTransaction(ctx context.Context, freshQuote quote.Quote) (quote.TxEnvelope, error) {
	payload, err := DecodeQuotePayload(freshQuote.RawProviderPayload)
	if err != nil {
		return quote.TxEnvelope{}, fmt.Errorf("across: decode quote payload: %w", err)
	}

	// Re-validate the live quote's own echoed SpokePool address against
	// this Provider's configured one before signing anything -- the same
	// defense-in-depth principle GetQuote already applies to the echoed
	// chain IDs/token addresses (design spec §21: an unvalidated external
	// response never flows directly into a signing call). This is the
	// pre-refactor signAndBroadcastFresh check, relocated here since this
	// is where the decoded payload is actually available; it catches
	// Across's live API ever returning a different SpokePool contract
	// than what ChainRoute has hardcoded (e.g. a contract migration or
	// config drift) -- something a later, purely-configured comparison in
	// Executor cannot catch.
	if quotedSpokePool := common.HexToAddress(payload.SpokePoolAddress); quotedSpokePool != p.SpokePoolAddress {
		return quote.TxEnvelope{}, fmt.Errorf("across: quoted SpokePool address %s does not match configured SpokePool %s",
			quotedSpokePool.Hex(), p.SpokePoolAddress.Hex())
	}

	quoteTimestamp, err := strconv.ParseUint(payload.QuoteTimestamp, 10, 32)
	if err != nil {
		return quote.TxEnvelope{}, fmt.Errorf("across: parse quote timestamp %q: %w", payload.QuoteTimestamp, err)
	}
	fillDeadline, err := strconv.ParseUint(payload.FillDeadline, 10, 32)
	if err != nil {
		return quote.TxEnvelope{}, fmt.Errorf("across: parse fill deadline %q: %w", payload.FillDeadline, err)
	}

	calldata, err := spokePoolABI.Pack("depositV3",
		p.WalletAddress, p.WalletAddress, p.WETHOrigin, p.WETHDestination,
		freshQuote.InputAmountBaseUnits, freshQuote.OutputAmountBaseUnits, big.NewInt(freshQuote.DestinationChainID),
		common.HexToAddress(payload.ExclusiveRelayer), uint32(quoteTimestamp), uint32(fillDeadline), uint32(payload.ExclusivityDeadline),
		[]byte{},
	)
	if err != nil {
		return quote.TxEnvelope{}, fmt.Errorf("across: pack depositV3 calldata: %w", err)
	}

	return quote.TxEnvelope{
		To: p.SpokePoolAddress, Value: freshQuote.InputAmountBaseUnits, Data: calldata, ChainID: freshQuote.SourceChainID,
	}, nil
}
