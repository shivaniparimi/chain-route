#!/usr/bin/env bash
# Real Sepolia -> Base Sepolia testnet smoke test. NEVER part of the
# default local/CI suite -- requires explicit opt-in via environment
# variables AND a funded test wallet. Spends real (worthless) testnet
# ETH/WETH and takes real network time (testnet fills average ~1 minute).
set -euo pipefail

if [[ "${RUN_TESTNET_TESTS:-}" != "1" ]]; then
    echo "RUN_TESTNET_TESTS=1 not set; skipping real testnet smoke test." >&2
    exit 0
fi

: "${BLOCKCHAIN_ENV:?BLOCKCHAIN_ENV=testnet is required}"
: "${TESTNET_WALLET_PRIVATE_KEY:?TESTNET_WALLET_PRIVATE_KEY is required}"
: "${ETHEREUM_SEPOLIA_RPC_URL:?ETHEREUM_SEPOLIA_RPC_URL is required}"
: "${BASE_SEPOLIA_RPC_URL:?BASE_SEPOLIA_RPC_URL is required}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HTTP_PORT="${HTTP_PORT:-8099}"
IDEMPOTENCY_KEY="testnet-smoke-$$-$(date +%s)"

echo "Starting go-api and worker with BLOCKCHAIN_ENV=testnet..."
(cd "$ROOT_DIR/go-api" && go build -o "$ROOT_DIR/go-api/server" ./cmd/server && go build -o "$ROOT_DIR/go-api/worker" ./cmd/worker)

"$ROOT_DIR/go-api/server" -http-addr=":$HTTP_PORT" &
SERVER_PID=$!
"$ROOT_DIR/go-api/worker" &
WORKER_PID=$!
trap 'kill "$SERVER_PID" "$WORKER_PID" 2>/dev/null || true' EXIT

sleep 2

AMOUNT_ETH="0.001"
echo "Submitting a real testnet payment ($AMOUNT_ETH WETH, Sepolia -> Base Sepolia)..."
RESPONSE=$(curl -s -X POST "http://127.0.0.1:$HTTP_PORT/payments" \
    -H "Idempotency-Key: $IDEMPOTENCY_KEY" \
    -H "Content-Type: application/json" \
    -d "{\"source_chain\":\"ethereum\",\"destination_chain\":\"base\",\"asset\":\"eth\",\"amount\":\"$AMOUNT_ETH\",\"execution_mode\":\"testnet\"}")
PAYMENT_ID=$(echo "$RESPONSE" | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")
echo "Payment created: $PAYMENT_ID"

echo "Verifying the route was sourced from a live bridge quote..."
HOP0_FEE=$(echo "$RESPONSE" | python3 -c "import json,sys; print(json.load(sys.stdin)['hops'][0]['fee'])")
python3 -c "import sys; sys.exit(0 if float('$HOP0_FEE') > 0 else 1)" || { echo "FAIL: expected hops[0].fee to be a positive number, got: $HOP0_FEE" >&2; exit 1; }

QUOTE_CHECK_RESPONSE=$(curl -s "http://127.0.0.1:$HTTP_PORT/payments/$PAYMENT_ID")
BRIDGE_PROVIDER=$(echo "$QUOTE_CHECK_RESPONSE" | python3 -c "import json,sys; print(json.load(sys.stdin).get('bridge_provider'))")
[[ "$BRIDGE_PROVIDER" == "across" || "$BRIDGE_PROVIDER" == "relay" ]] || { echo "FAIL: expected bridge_provider to be 'across' or 'relay', got: $BRIDGE_PROVIDER. Full response: $QUOTE_CHECK_RESPONSE" >&2; exit 1; }
echo "OK: hops[0].fee=$HOP0_FEE (positive), bridge_provider=$BRIDGE_PROVIDER"

# Dual-provider fee-minimum check (design doc §25): independently fetch a
# fresh live quote from BOTH Across and Relay (via a script-local Go
# program that reuses the real quote.Provider implementations, so the
# comparison is against the same fee math the server itself uses, just a
# separately-issued live call) and assert hops[0].fee equals
# min(acrossFee, relayFee) -- deliberately never asserting which provider
# name wins, per the design doc and explicit instruction. Whichever
# provider actually won, the polling loop below already reads
# bridge_provider generically and needs no branching.
echo "Independently fetching live Across and Relay quotes to verify the winning fee is the true minimum..."
AMOUNT_WEI=$(python3 -c "print(int(round($AMOUNT_ETH * 10**18)))")
FEE_CHECK_DIR="$(mktemp -d "$ROOT_DIR/go-api/.testnet_feecheck.XXXXXX")"
cat > "$FEE_CHECK_DIR/fee_check.go" <<'GOEOF'
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"time"

	"chainroute/go-api/internal/bridge/across"
	"chainroute/go-api/internal/bridge/quote"
	"chainroute/go-api/internal/bridge/relay"
	"chainroute/go-api/internal/evm"
	"chainroute/go-api/internal/money"
)

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	wallet, err := evm.LoadWallet(os.Getenv("TESTNET_WALLET_PRIVATE_KEY"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "load wallet:", err)
		os.Exit(1)
	}

	amountWei, ok := new(big.Int).SetString(os.Args[1], 10)
	if !ok {
		fmt.Fprintln(os.Stderr, "invalid amount:", os.Args[1])
		os.Exit(1)
	}
	req := quote.Request{SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH", AmountBaseUnits: amountWei}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	acrossClient := across.NewClient(envOrDefault("ACROSS_TESTNET_API_URL", "https://testnet.across.to/api"))
	acrossClient.APIKey = os.Getenv("ACROSS_API_KEY")
	acrossClient.IntegratorID = os.Getenv("ACROSS_INTEGRATOR_ID")
	acrossQuote, err := across.NewProvider(acrossClient, time.Minute).GetQuote(ctx, req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "across quote:", err)
		os.Exit(1)
	}
	if !acrossQuote.Available {
		fmt.Fprintln(os.Stderr, "across quote unavailable for this amount")
		os.Exit(1)
	}

	relayClient := relay.NewClient(envOrDefault("RELAY_TESTNET_API_URL", "https://api.testnets.relay.link"))
	relayClient.APIKey = os.Getenv("RELAY_API_KEY")
	relayQuote, err := relay.NewProvider(relayClient, wallet.Address, time.Minute).GetQuote(ctx, req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "relay quote:", err)
		os.Exit(1)
	}
	if !relayQuote.Available {
		fmt.Fprintln(os.Stderr, "relay quote unavailable for this amount")
		os.Exit(1)
	}

	minFee := acrossQuote.FeeBaseUnits
	if relayQuote.FeeBaseUnits.Cmp(minFee) < 0 {
		minFee = relayQuote.FeeBaseUnits
	}

	out := map[string]string{
		"across_fee_wei":  acrossQuote.FeeBaseUnits.String(),
		"relay_fee_wei":   relayQuote.FeeBaseUnits.String(),
		"min_fee_decimal": money.BaseUnitsToDecimal(minFee, 18),
	}
	enc, err := json.Marshal(out)
	if err != nil {
		fmt.Fprintln(os.Stderr, "marshal output:", err)
		os.Exit(1)
	}
	fmt.Println(string(enc))
}
GOEOF

FEE_CHECK_JSON=$(cd "$ROOT_DIR/go-api" && go run "$FEE_CHECK_DIR/fee_check.go" "$AMOUNT_WEI")
rm -rf "$FEE_CHECK_DIR"
ACROSS_FEE_WEI=$(echo "$FEE_CHECK_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin)['across_fee_wei'])")
RELAY_FEE_WEI=$(echo "$FEE_CHECK_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin)['relay_fee_wei'])")
MIN_FEE_DECIMAL=$(echo "$FEE_CHECK_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin)['min_fee_decimal'])")
python3 -c "import sys; sys.exit(0 if float('$HOP0_FEE') == float('$MIN_FEE_DECIMAL') else 1)" || { echo "FAIL: expected hops[0].fee ($HOP0_FEE) to equal the independently-fetched min(acrossFee=$ACROSS_FEE_WEI wei, relayFee=$RELAY_FEE_WEI wei) = $MIN_FEE_DECIMAL. Winning provider was $BRIDGE_PROVIDER." >&2; exit 1; }
echo "OK: hops[0].fee=$HOP0_FEE matches independently-verified min(across=$ACROSS_FEE_WEI wei, relay=$RELAY_FEE_WEI wei)=$MIN_FEE_DECIMAL (winning provider: $BRIDGE_PROVIDER)"

echo "Polling for COMPLETED (this can take a few minutes on testnet)..."
for i in $(seq 1 120); do
    STATUS_RESPONSE=$(curl -s "http://127.0.0.1:$HTTP_PORT/payments/$PAYMENT_ID")
    STATUS=$(echo "$STATUS_RESPONSE" | python3 -c "import json,sys; print(json.load(sys.stdin)['status'])")
    echo "  [$i] status=$STATUS"
    if [[ "$STATUS" == "COMPLETED" ]]; then
        TX_HASH=$(echo "$STATUS_RESPONSE" | python3 -c "import json,sys; print(json.load(sys.stdin).get('external_tx_hash'))")
        echo "SUCCESS: payment $PAYMENT_ID completed. external_tx_hash=$TX_HASH"
        echo "View on Sepolia: https://sepolia.etherscan.io/tx/$TX_HASH"
        exit 0
    fi
    if [[ "$STATUS" == "FAILED" ]]; then
        echo "FAILED: payment $PAYMENT_ID reached FAILED. Full response: $STATUS_RESPONSE" >&2
        exit 1
    fi
    sleep 5
done

echo "TIMEOUT: payment $PAYMENT_ID did not reach a terminal state within the poll budget. Last response: $STATUS_RESPONSE" >&2
exit 1
