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

echo "Submitting a real testnet payment (0.001 WETH, Sepolia -> Base Sepolia)..."
RESPONSE=$(curl -s -X POST "http://127.0.0.1:$HTTP_PORT/payments" \
    -H "Idempotency-Key: $IDEMPOTENCY_KEY" \
    -H "Content-Type: application/json" \
    -d '{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}')
PAYMENT_ID=$(echo "$RESPONSE" | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")
echo "Payment created: $PAYMENT_ID"

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
