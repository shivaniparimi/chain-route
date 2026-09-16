package quote

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// TxEnvelope is an unsigned transaction description a Signer hands back to
// Executor. Producing this does NOT sign anything -- Executor is the sole
// caller of wallet.SignTx, for every provider (design doc §11).
type TxEnvelope struct {
	To      common.Address
	Value   *big.Int
	Data    []byte
	ChainID int64
}

// Signer is implemented by any provider whose fresh quotes Executor can
// turn into a signed transaction. BuildTransaction must be built entirely
// from freshQuote and must never call any signing function itself.
type Signer interface {
	Provider
	BuildTransaction(ctx context.Context, freshQuote Quote) (TxEnvelope, error)
}
