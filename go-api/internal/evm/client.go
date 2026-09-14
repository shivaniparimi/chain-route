package evm

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/ethclient"
)

// Dial connects to rpcURL and validates its reported chain ID matches
// expectedChainID exactly, refusing to return a client on any mismatch.
// This is the actual guardrail against accidental mainnet use (Phase 7
// design spec §5): an RPC URL's hostname proves nothing, but the chain
// itself cannot lie about its own chain ID over JSON-RPC.
func Dial(ctx context.Context, rpcURL string, expectedChainID int64) (*ethclient.Client, error) {
	client, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return nil, fmt.Errorf("dial rpc: %w", err)
	}
	gotChainID, err := client.ChainID(ctx)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("query chain id: %w", err)
	}
	if gotChainID.Cmp(big.NewInt(expectedChainID)) != 0 {
		client.Close()
		return nil, fmt.Errorf("chain id mismatch: expected %d, got %s -- refusing to proceed", expectedChainID, gotChainID.String())
	}
	return client, nil
}
