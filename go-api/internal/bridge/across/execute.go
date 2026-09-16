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

	"chainroute/go-api/internal/bridge/quote"
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

// BuildTransaction implements quote.Signer. It builds the same depositV3
// native-ETH-deposit calldata (msg.value = freshQuote.InputAmountBaseUnits,
// WalletAddress as both depositor and recipient, no separate approve()
// transaction) behind the shared Signer interface, so Executor needs no
// Across-specific code path to build a transaction to sign. It does not
// estimate gas, suggest a gas price, or sign -- Executor (design doc §11)
// is the sole owner of those steps for every provider.
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
