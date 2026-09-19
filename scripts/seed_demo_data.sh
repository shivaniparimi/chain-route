#!/usr/bin/env bash
# Populates a running ChainRoute server with realistic-looking demo
# payments via the REAL POST /payments API, simulated mode only -- never
# a direct SQL insert, never run against a deployed environment by
# default. Local development / demo convenience only.
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
COUNT="${COUNT:-40}"

CHAINS=(ethereum base arbitrum optimism polygon)
ASSETS=(usdc eth)

for i in $(seq 1 "$COUNT"); do
	src_idx=$((RANDOM % ${#CHAINS[@]}))
	dst_idx=$((RANDOM % ${#CHAINS[@]}))
	while [ "$dst_idx" -eq "$src_idx" ]; do
		dst_idx=$((RANDOM % ${#CHAINS[@]}))
	done
	asset_idx=$((RANDOM % ${#ASSETS[@]}))
	amount=$(( (RANDOM % 500) + 1 ))

	curl -sf -X POST "$BASE_URL/payments" \
		-H "Content-Type: application/json" \
		-H "Idempotency-Key: demo-seed-$(date +%s)-$i-$RANDOM" \
		-d "{\"source_chain\":\"${CHAINS[$src_idx]}\",\"destination_chain\":\"${CHAINS[$dst_idx]}\",\"asset\":\"${ASSETS[$asset_idx]}\",\"amount\":\"${amount}\"}" \
		> /dev/null
	echo "seeded payment $i/$COUNT"
done

echo "Done. Demo data uses simulated mode only -- no real blockchain activity, no fabricated data outside the app's own real code path."
