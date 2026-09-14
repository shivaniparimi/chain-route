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
		// Deliberately NOT %w-wrapping the underlying error here: for HTTP
		// transports go-ethereum's dial failure surfaces as a *url.Error
		// whose Error() string embeds the full request URL, and provider
		// RPC URLs (Alchemy, Infura, etc.) commonly embed an API key in
		// that URL's path. Propagating it verbatim would let a down or
		// misconfigured endpoint leak the credential into logs on one of
		// the most common startup failure modes (review Finding 2).
		return nil, fmt.Errorf("dial rpc: connection failed (see network diagnostics separately, url omitted for credential safety)")
	}
	gotChainID, err := client.ChainID(ctx)
	if err != nil {
		client.Close()
		// Same reasoning as above: for HTTP transports the dial itself is
		// lazy, so a down/misconfigured endpoint's failure actually
		// surfaces here, at the first real request -- again as a
		// *url.Error carrying the full URL.
		return nil, fmt.Errorf("query chain id: request failed (url omitted for credential safety)")
	}
	if gotChainID.Cmp(big.NewInt(expectedChainID)) != 0 {
		client.Close()
		return nil, fmt.Errorf("chain id mismatch: expected %d, got %s -- refusing to proceed", expectedChainID, gotChainID.String())
	}
	return client, nil
}
