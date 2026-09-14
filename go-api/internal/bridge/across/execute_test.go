package across

import (
	"context"
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"chainroute/go-api/internal/evm"
)

// expectedDepositV3Calldata is the depositV3 calldata for exactly the
// wallet/params used by TestBuildAndSignDepositV3Tx_ProducesExpectedCalldataAndValue,
// computed once via spokePoolABI.Pack against those same fixed inputs and
// hardcoded here. Pinning the full byte string (not just checking
// non-empty data) catches regressions a length-only check would miss --
// e.g. two same-type parameters (fillDeadline/exclusivityDeadline, both
// uint32) silently swapped during a future edit would still produce a
// non-empty, plausible-looking blob but a different byte string. The
// leading 4 bytes (7b939232) are the depositV3 function selector,
// independently verified against the live upstream ABI during code
// review.
const expectedDepositV3Calldata = "7b93923200000000000000000000000071562b71999873db5b286df957af199ec94617f700000000000000000000000071562b71999873db5b286df957af199ec94617f7000000000000000000000000fff9976782d46cc05630d1f6ebab18b2324d6b14000000000000000000000000420000000000000000000000000000000000000600000000000000000000000000000000000000000000000000038d7ea4c6800000000000000000000000000000000000000000000000000000038b4e070ffcf90000000000000000000000000000000000000000000000000000000000014a340000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006aa729d0000000000000000000000000000000000000000000000000000000006aa745f0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001800000000000000000000000000000000000000000000000000000000000000000"

// fakeEthClient implements the minimal EthClient surface BuildAndSignDepositV3Tx
// needs, returning fixed values so the test is fully deterministic -- no
// real network call.
type fakeEthClient struct {
	gasPrice *big.Int
	gasLimit uint64
}

func (f *fakeEthClient) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	return f.gasPrice, nil
}

func (f *fakeEthClient) EstimateGas(ctx context.Context, msg ethereumCallMsg) (uint64, error) {
	return f.gasLimit, nil
}

func TestBuildAndSignDepositV3Tx_ProducesExpectedCalldataAndValue(t *testing.T) {
	// This is the corrected, real go-ethereum test key (64 hex chars) --
	// Task 8 found the brief's original literal here was a malformed
	// 63-char string and fixed it to this value, verified live via
	// crypto.HexToECDSA/PubkeyToAddress. See internal/evm/wallet_test.go
	// for the same key/address pair used consistently across the codebase.
	const testKey = "b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291"
	wallet, err := evm.LoadWallet(testKey)
	if err != nil {
		t.Fatalf("load wallet: %v", err)
	}

	client := &fakeEthClient{gasPrice: big.NewInt(1_000_000_000), gasLimit: 300_000}
	spokePool := common.HexToAddress("0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662")
	weth := common.HexToAddress("0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14")

	inputAmount, _ := new(big.Int).SetString("1000000000000000", 10) // 0.001 WETH

	params := DepositV3Params{
		Recipient:           wallet.Address,
		InputToken:          weth,
		OutputToken:         common.HexToAddress("0x4200000000000000000000000000000000000006"),
		InputAmount:         inputAmount,
		OutputAmount:        mustBigInt(t, "997592172330233"),
		DestinationChainID:  big.NewInt(84532),
		ExclusiveRelayer:    common.HexToAddress("0x0000000000000000000000000000000000000000"),
		QuoteTimestamp:      1789340112,
		FillDeadline:        1789347312,
		ExclusivityDeadline: 0,
	}

	tx, err := BuildAndSignDepositV3Tx(context.Background(), client, wallet, 11155111, spokePool, 5, params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if tx.To() == nil || *tx.To() != spokePool {
		t.Fatalf("expected tx.To() to be the SpokePool address, got %v", tx.To())
	}
	if tx.Value().Cmp(inputAmount) != 0 {
		t.Fatalf("expected tx.Value() to equal inputAmount (native-ETH deposit path), got %s", tx.Value())
	}
	if tx.Nonce() != 5 {
		t.Fatalf("expected nonce 5, got %d", tx.Nonce())
	}
	if len(tx.Data()) == 0 {
		t.Fatal("expected non-empty calldata")
	}
	// The first 4 bytes are the depositV3 function selector -- deterministic
	// given the fixed ABI, independent of parameter values.
	if len(tx.Data()) < 4 {
		t.Fatal("calldata too short to contain a function selector")
	}

	gotHex := hex.EncodeToString(tx.Data())
	if gotHex[:8] != "7b939232" {
		t.Fatalf("expected depositV3 selector 7b939232, got %s", gotHex[:8])
	}
	if gotHex != expectedDepositV3Calldata {
		t.Fatalf("calldata mismatch:\n got: %s\nwant: %s", gotHex, expectedDepositV3Calldata)
	}
}

func mustBigInt(t *testing.T, s string) *big.Int {
	t.Helper()
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		t.Fatalf("invalid big.Int literal: %s", s)
	}
	return n
}
