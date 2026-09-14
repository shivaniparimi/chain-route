//go:build testnet_integration

package worker

import (
	"context"
	"os"
	"testing"
	"time"

	"chainroute/go-api/internal/bridge/across"
	"chainroute/go-api/internal/evm"
)

// TestRealTestnetDepositCycle exercises one real quote -> sign -> broadcast
// -> reconcile cycle against the actual Sepolia/Base Sepolia testnets and
// the real Across testnet API. It NEVER runs as part of `go test ./...`
// or `go test -tags=integration ./...` -- both the testnet_integration
// build tag AND RUN_TESTNET_TESTS=1 are required (design spec §24).
func TestRealTestnetDepositCycle(t *testing.T) {
	if os.Getenv("RUN_TESTNET_TESTS") != "1" {
		t.Skip("RUN_TESTNET_TESTS=1 not set")
	}
	key := os.Getenv("TESTNET_WALLET_PRIVATE_KEY")
	sepoliaRPC := os.Getenv("ETHEREUM_SEPOLIA_RPC_URL")
	baseSepoliaRPC := os.Getenv("BASE_SEPOLIA_RPC_URL")
	if key == "" || sepoliaRPC == "" || baseSepoliaRPC == "" {
		t.Fatal("TESTNET_WALLET_PRIVATE_KEY, ETHEREUM_SEPOLIA_RPC_URL, and BASE_SEPOLIA_RPC_URL are required when RUN_TESTNET_TESTS=1")
	}

	wallet, err := evm.LoadWallet(key)
	if err != nil {
		t.Fatalf("load wallet: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	sepoliaClient, err := evm.Dial(ctx, sepoliaRPC, 11155111)
	if err != nil {
		t.Fatalf("dial sepolia: %v", err)
	}

	acrossClient := across.NewClient("https://testnet.across.to/api")
	quote, err := acrossClient.SuggestedFees(ctx, 11155111, 84532,
		"0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14", "0x4200000000000000000000000000000000000006", "1000000000000000")
	if err != nil {
		t.Fatalf("get real quote: %v", err)
	}
	if quote.OutputAmount == "" {
		t.Fatal("expected a non-empty real outputAmount")
	}

	nonce, err := sepoliaClient.PendingNonceAt(ctx, wallet.Address)
	if err != nil {
		t.Fatalf("query real pending nonce: %v", err)
	}
	t.Logf("real testnet check: wallet=%s pending_nonce=%d quote_output_amount=%s spoke_pool=%s",
		wallet.Address.Hex(), nonce, quote.OutputAmount, quote.SpokePoolAddress)

	// This deliberately stops at "a real quote was obtained and the chain
	// is reachable" rather than actually broadcasting a transaction here
	// -- the full broadcast -> reconcile -> COMPLETED cycle is exercised
	// by scripts/e2e_testnet_test.sh (design spec §25), which runs the
	// real worker binary end-to-end rather than duplicating that flow
	// inline in a Go test. This test's job is a fast, direct-package
	// confirmation that the real API/RPC integration points are alive.
}
