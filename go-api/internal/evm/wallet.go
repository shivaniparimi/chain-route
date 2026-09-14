package evm

import (
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

// Wallet holds the dedicated Phase 7 testnet signing key. Its PrivateKey
// field must never be logged, serialized, or included in any error message
// -- callers only ever expose Address.
type Wallet struct {
	PrivateKey *ecdsa.PrivateKey
	Address    common.Address
}

// LoadWallet parses a hex-encoded private key (with or without a leading
// "0x") and derives its address.
func LoadWallet(hexKey string) (*Wallet, error) {
	hexKey = strings.TrimPrefix(hexKey, "0x")
	key, err := crypto.HexToECDSA(hexKey)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}
	return &Wallet{PrivateKey: key, Address: crypto.PubkeyToAddress(key.PublicKey)}, nil
}

// SignTx signs tx for the given chain ID using the latest applicable
// signer. Never re-signs an already-signed transaction with different
// content -- callers must construct a fresh unsigned tx.Transaction per
// signing attempt and never mutate a previously-signed one.
func (w *Wallet) SignTx(tx *types.Transaction, chainID *big.Int) (*types.Transaction, error) {
	signer := types.LatestSignerForChainID(chainID)
	signed, err := types.SignTx(tx, signer, w.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("sign tx: %w", err)
	}
	return signed, nil
}
