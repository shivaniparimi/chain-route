#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CPP_PORT=50098
HTTP_PORT=8099
SEED=1001
PAYMENT_IDEMPOTENCY_KEY="e2e-test-key-$$-$(date +%s)"

DATABASE_URL="${DATABASE_URL:-postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable}"
export DATABASE_URL

cleanup() {
    [[ -n "${GO_PID:-}" ]] && kill "$GO_PID" 2>/dev/null || true
    [[ -n "${CPP_PID:-}" ]] && kill "$CPP_PID" 2>/dev/null || true
    if [[ -n "${DATABASE_URL:-}" ]]; then
        /opt/homebrew/opt/postgresql@16/bin/psql "$DATABASE_URL" -tAc \
            "DELETE FROM payments WHERE idempotency_key = '$PAYMENT_IDEMPOTENCY_KEY'" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

echo "Building C++ service..."
cmake -S "$ROOT_DIR/cpp-routing-service" -B "$ROOT_DIR/cpp-routing-service/build" >/dev/null
cmake --build "$ROOT_DIR/cpp-routing-service/build" >/dev/null

echo "Building Go service..."
(cd "$ROOT_DIR/go-api" && go build -o "$ROOT_DIR/go-api/server" ./cmd/server)

echo "Starting C++ service on 127.0.0.1:$CPP_PORT (seed=$SEED)..."
"$ROOT_DIR/cpp-routing-service/build/chainroute_service_server" \
    --seed="$SEED" --listen-address="127.0.0.1:$CPP_PORT" &
CPP_PID=$!

# Give the child a brief grace period to hit a fast-path failure (e.g. the
# listen address is already in use) and exit before we start polling. Without
# this, a bind failure that surfaces in tens of milliseconds can race the
# first kill -0 check below: the dying process still looks alive at the
# instant it's checked, and if a leftover process from an earlier run is
# squatting on the same port, the very next health probe in that same loop
# iteration can succeed against that leftover process -- reporting readiness
# via the wrong server before the dead-child check ever gets a chance to
# catch it on a later iteration.
sleep 0.3

# Resolve a grpc health-probe binary. The upstream project
# (github.com/grpc-ecosystem/grpc-health-probe) ships a binary literally
# named `grpc-health-probe` (hyphen) whether installed via `go install` or
# Homebrew's `grpc_health_probe` formula (which also installs a
# hyphenated binary) -- there is no standard `grpc_health_probe`
# (underscore) binary name. Resolve it defensively: prefer whatever is
# already on PATH, fall back to the default `go install` location, then
# try installing it (brew, then `go install`), and finally fall back to a
# plain TCP-connect check per this script's documented fallback policy.
GOPATH_BIN="$(go env GOPATH 2>/dev/null || echo "$HOME/go")/bin"
HEALTH_PROBE=""
for candidate in grpc-health-probe grpc_health_probe "$GOPATH_BIN/grpc-health-probe"; do
    if command -v "$candidate" >/dev/null 2>&1; then
        HEALTH_PROBE="$candidate"
        break
    fi
done
if [[ -z "$HEALTH_PROBE" ]]; then
    echo "grpc-health-probe not found; attempting to install..."
    if command -v brew >/dev/null 2>&1 && brew install grpc-health-probe >/dev/null 2>&1; then
        command -v grpc-health-probe >/dev/null 2>&1 && HEALTH_PROBE="grpc-health-probe"
    fi
fi
if [[ -z "$HEALTH_PROBE" ]] && command -v go >/dev/null 2>&1; then
    if go install github.com/grpc-ecosystem/grpc-health-probe@latest >/dev/null 2>&1; then
        [[ -x "$GOPATH_BIN/grpc-health-probe" ]] && HEALTH_PROBE="$GOPATH_BIN/grpc-health-probe"
    fi
fi

if [[ -n "$HEALTH_PROBE" ]]; then
    echo "Waiting for C++ service readiness via grpc.health.v1 (using $HEALTH_PROBE)..."
else
    echo "grpc-health-probe unavailable; falling back to a plain TCP-connect readiness check (weaker: confirms the port is open, not that gRPC is actually serving)."
fi

READY=0
for i in $(seq 1 30); do
    kill -0 "$CPP_PID" 2>/dev/null || { echo "C++ service exited before becoming ready" >&2; exit 1; }
    if [[ -n "$HEALTH_PROBE" ]]; then
        # IMPORTANT: no -service= flag. gRPC's default health check service
        # only reports SERVING for the overall/empty service name, not for
        # "chainroute.v1.RoutingService" by name (which returns NOT_FOUND).
        if "$HEALTH_PROBE" -addr="127.0.0.1:$CPP_PORT" >/dev/null 2>&1; then
            echo "C++ service is ready."
            READY=1
            break
        fi
    else
        if nc -z 127.0.0.1 "$CPP_PORT" >/dev/null 2>&1; then
            echo "C++ service port is open (TCP-only readiness check)."
            READY=1
            break
        fi
    fi
    sleep 0.5
done
if [[ "$READY" -ne 1 ]]; then
    echo "C++ service did not become ready in time" >&2
    exit 1
fi

kill -0 "$CPP_PID" 2>/dev/null || { echo "C++ service exited before becoming ready" >&2; exit 1; }

echo "Ensuring database schema is up to date..."
/opt/homebrew/opt/postgresql@16/bin/psql "$DATABASE_URL" -tAc \
    "SELECT 1 FROM information_schema.tables WHERE table_name='payments'" | grep -q 1 || \
    /opt/homebrew/opt/postgresql@16/bin/psql "$DATABASE_URL" -f "$ROOT_DIR/go-api/migrations/0001_create_payments.sql"

echo "Starting Go service on :$HTTP_PORT..."
"$ROOT_DIR/go-api/server" \
    --http-addr=":$HTTP_PORT" --grpc-addr="127.0.0.1:$CPP_PORT" &
GO_PID=$!
sleep 1

echo "Test 1: a real multi-hop route"
RESPONSE=$(curl -s -X POST "http://127.0.0.1:$HTTP_PORT/routes" \
    -H "Content-Type: application/json" \
    -d '{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":1000}')
echo "$RESPONSE"
echo "$RESPONSE" | grep -q '"route_found":true' || { echo "FAIL: expected route_found:true, got: $RESPONSE"; exit 1; }
echo "$RESPONSE" | grep -q '"bridge_name":"Stargate#1"' || { echo "FAIL: expected hop via Stargate#1, got: $RESPONSE"; exit 1; }
echo "$RESPONSE" | grep -q '"bridge_name":"Synapse#2"' || { echo "FAIL: expected hop via Synapse#2, got: $RESPONSE"; exit 1; }
echo "$RESPONSE" | grep -q '"total_fee":1.958142' || { echo "FAIL: expected total_fee close to 1.9581423176459705, got: $RESPONSE"; exit 1; }
echo "OK"

echo "Test 2: invalid chain -> 400"
STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/routes" \
    -H "Content-Type: application/json" \
    -d '{"source_chain":"mars","destination_chain":"base","asset":"USDC","amount":1000}')
[[ "$STATUS" == "400" ]] || { echo "FAIL: expected 400, got $STATUS"; exit 1; }
echo "OK: got 400"

echo "Test 3: same chain -> 400"
STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/routes" \
    -H "Content-Type: application/json" \
    -d '{"source_chain":"ethereum","destination_chain":"ethereum","asset":"USDC","amount":1000}')
[[ "$STATUS" == "400" ]] || { echo "FAIL: expected 400, got $STATUS"; exit 1; }
echo "OK: got 400"

echo "Test 4: POST /payments creates a payment"
CREATE_STATUS=$(curl -s -o /tmp/e2e_create_body.json -w "%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/payments" \
    -H "Content-Type: application/json" -H "Idempotency-Key: $PAYMENT_IDEMPOTENCY_KEY" \
    -d '{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":"1000.00"}')
PAYMENT_RESPONSE=$(cat /tmp/e2e_create_body.json)
rm -f /tmp/e2e_create_body.json
echo "$PAYMENT_RESPONSE"
[[ "$CREATE_STATUS" == "201" ]] || { echo "FAIL: expected 201, got $CREATE_STATUS"; exit 1; }
echo "$PAYMENT_RESPONSE" | grep -q '"status":"ROUTED"' || { echo "FAIL: expected status ROUTED"; exit 1; }
PAYMENT_ID=$(echo "$PAYMENT_RESPONSE" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
[[ -n "$PAYMENT_ID" ]] || { echo "FAIL: no payment id in response"; exit 1; }
echo "OK: created payment $PAYMENT_ID with 201"

echo "Test 5: GET /payments/{id} reads it back"
GET_RESPONSE=$(curl -s "http://127.0.0.1:$HTTP_PORT/payments/$PAYMENT_ID")
echo "$GET_RESPONSE" | grep -q "\"id\":\"$PAYMENT_ID\"" || { echo "FAIL: GET did not return the same payment"; exit 1; }
echo "OK"

echo "Test 6: replaying the same Idempotency-Key returns 200 with the same payment"
REPLAY_STATUS=$(curl -s -o /tmp/e2e_replay_body.json -w "%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/payments" \
    -H "Content-Type: application/json" -H "Idempotency-Key: $PAYMENT_IDEMPOTENCY_KEY" \
    -d '{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":"1000.00"}')
[[ "$REPLAY_STATUS" == "200" ]] || { echo "FAIL: expected 200 on replay, got $REPLAY_STATUS"; exit 1; }
grep -q "\"id\":\"$PAYMENT_ID\"" /tmp/e2e_replay_body.json || { echo "FAIL: replay returned a different payment"; exit 1; }
rm -f /tmp/e2e_replay_body.json
echo "OK: replay returned the same payment with 200"

echo "Test 7: same Idempotency-Key with a different body returns 409"
CONFLICT_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/payments" \
    -H "Content-Type: application/json" -H "Idempotency-Key: $PAYMENT_IDEMPOTENCY_KEY" \
    -d '{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":"2000.00"}')
[[ "$CONFLICT_STATUS" == "409" ]] || { echo "FAIL: expected 409, got $CONFLICT_STATUS"; exit 1; }
echo "OK: got 409"

echo "Test 8: missing Idempotency-Key returns 400"
NOKEY_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://127.0.0.1:$HTTP_PORT/payments" \
    -H "Content-Type: application/json" \
    -d '{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":"1000.00"}')
[[ "$NOKEY_STATUS" == "400" ]] || { echo "FAIL: expected 400, got $NOKEY_STATUS"; exit 1; }
echo "OK: got 400"

echo "Test 9: payment survives a Go process restart"
kill -TERM "$GO_PID"
wait "$GO_PID" 2>/dev/null || true

"$ROOT_DIR/go-api/server" \
    --http-addr=":$HTTP_PORT" --grpc-addr="127.0.0.1:$CPP_PORT" &
GO_PID=$!
sleep 1

RESTART_RESPONSE=$(curl -s "http://127.0.0.1:$HTTP_PORT/payments/$PAYMENT_ID")
echo "$RESTART_RESPONSE" | grep -q "\"id\":\"$PAYMENT_ID\"" || { echo "FAIL: payment not found after restart"; exit 1; }
echo "OK: payment $PAYMENT_ID still readable after Go process restart"

echo "All E2E checks passed."
