# Phase 7: Real Testnet Bridge Execution via Across Protocol — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Phase 6's pure-function simulated execution, for one opt-in `execution_mode`, with a real signed Ethereum Sepolia transaction broadcast through Across Protocol's testnet deployment, bridged to Base Sepolia, and reconciled to a real observed terminal outcome — while leaving simulated-mode execution byte-for-byte unchanged.

**Architecture:** Four new internal packages (`money`, `evm`, `bridge/across`) plus two new worker components (`executor.go`, a fourth `reconciler.go` goroutine) sit alongside the unmodified Phase 6 pipeline. A new `payment_executions` table (one row per payment, `UNIQUE(payment_id)`) and `wallet_nonces` table (Postgres-only nonce allocation) are the correctness boundary — no in-memory mutex, no Redis, no distributed lock anywhere.

**Tech Stack:** Go 1.27, `github.com/ethereum/go-ethereum` (new dependency, `crypto`/`core/types`/`ethclient`/`accounts/abi` only — not a full node), PostgreSQL, the existing `kafka-go`/Redpanda stack, unmodified.

## Global Constraints

- Simulated-mode execution (`execution.Execute`, `Processor`, `Recovery`, `Publisher`) must remain **byte-for-byte behaviorally unchanged** for `execution_mode="simulated"` payments — this is the default and must stay so.
- All Phase 1-6 tests must continue to pass **unmodified**. Where a shared method's signature must change (only `ClaimPayment`, see Task 5), update every existing call site — but never rewrite an existing Phase 1-6 test's assertions or expected values, only the mechanical fallout of the signature change.
- `router/`, `cpp-routing-service/`, and `proto/chainroute/v1/routing.proto` are **not modified** anywhere in this plan.
- Only migration `go-api/migrations/0004_across_testnet_execution.sql` is added. Migrations `0001`-`0003` are never edited.
- No `float64`/`big.Float` anywhere in amount or nonce handling. `big.Int` only.
- No in-memory mutex, no Redis, no distributed lock, no Kafka transactions — PostgreSQL transactions and conditional `UPDATE`/`INSERT ... ON CONFLICT`/unique constraints are the sole correctness mechanism, exactly as Phase 5/6 established.
- Never log: the private key, RPC URLs (may embed API keys), or raw signed transaction bytes — under any log level.
- `BLOCKCHAIN_ENV=testnet` gates every real-execution code path; its absence must make the worker behave exactly as Phase 6 (simulated-only), and the HTTP API must reject any `execution_mode="testnet"` request when the server itself isn't configured for testnet.
- Module: `chainroute/go-api`. Existing packages this plan touches or depends on: `internal/payment`, `internal/postgres`, `internal/worker`, `internal/handler`, `internal/kafka`, `internal/events`, `internal/execution`, `cmd/server`, `cmd/worker`.

## Verified Across testnet facts (live-checked 2026-09-13; DO NOT re-guess, but Task 1 re-confirms before use since bridge state can drift)

These were obtained by fetching the *raw* deployment JSON from `github.com/across-protocol/contracts` (not a summarized/truncated page render) and by making live, unauthenticated HTTP calls against `https://testnet.across.to/api`. Two independent sources agree on both SpokePool addresses (the raw deployment file and the live `/suggested-fees` response's own `spokePoolAddress`/`destinationSpokePoolAddress` fields).

- **Sepolia (chain id `11155111`) SpokePool**: `0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662`
- **Base Sepolia (chain id `84532`) SpokePool**: `0x82B564983aE7274c86695917BBf8C99ECb6F0F8F`
- **Sepolia WETH**: `0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14`
- **Base Sepolia WETH**: `0x4200000000000000000000000000000000000006` (the standard OP-stack predeploy address — Base Sepolia is OP-stack)
- **Route confirmed live** via `GET https://testnet.across.to/api/available-routes?originChainId=11155111&destinationChainId=84532`: Sepolia WETH → Base Sepolia WETH is present and currently active, both as an ERC-20 WETH route (`"isNative": false`) and as a **native-ETH route sharing the same underlying WETH token address** (`"isNative": true`).
- **`depositV3` open item RESOLVED**: the `isNative: true` route entry, combined with `depositV3`'s `payable` modifier, confirms the SpokePool accepts native ETH directly (auto-wrapped internally) when `inputToken` is set to the WETH address and `msg.value` equals `inputAmount`. **This plan uses the native-ETH path** (`msg.value = inputAmount`, no separate `approve()` call) — deliberately, because a two-step ERC-20 `approve()`+`depositV3` flow would consume **two** nonces per payment, breaking the one-execution-row-one-nonce invariant central to the approved design. This is a design decision made during planning, not left to the implementer to rediscover.
- **`depositV3` exact ABI** (pulled directly from the raw ABI JSON in `deployments/sepolia/Ethereum_SpokePool.json`, confirmed identical on Base Sepolia's `Base_SpokePool.json`):
  ```json
  {
    "inputs": [
      {"internalType": "address", "name": "depositor", "type": "address"},
      {"internalType": "address", "name": "recipient", "type": "address"},
      {"internalType": "address", "name": "inputToken", "type": "address"},
      {"internalType": "address", "name": "outputToken", "type": "address"},
      {"internalType": "uint256", "name": "inputAmount", "type": "uint256"},
      {"internalType": "uint256", "name": "outputAmount", "type": "uint256"},
      {"internalType": "uint256", "name": "destinationChainId", "type": "uint256"},
      {"internalType": "address", "name": "exclusiveRelayer", "type": "address"},
      {"internalType": "uint32", "name": "quoteTimestamp", "type": "uint32"},
      {"internalType": "uint32", "name": "fillDeadline", "type": "uint32"},
      {"internalType": "uint32", "name": "exclusivityDeadline", "type": "uint32"},
      {"internalType": "bytes", "name": "message", "type": "bytes"}
    ],
    "name": "depositV3",
    "outputs": [],
    "stateMutability": "payable",
    "type": "function"
  }
  ```
- **`/suggested-fees` — confirmed live request/response** (real captured response, amount=0.001 WETH in wei):
  - Request: `GET /suggested-fees?originChainId=11155111&destinationChainId=84532&inputToken=0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14&outputToken=0x4200000000000000000000000000000000000006&amount=1000000000000000`
  - Response (HTTP 200, no auth header sent or required):
    ```json
    {"estimatedFillTimeSec":10,"capitalFeePct":"99958333334000","capitalFeeTotal":"99958333334","relayGasFeePct":"981018513630000","relayGasFeeTotal":"981018513630","relayFeePct":"2407827669767028","relayFeeTotal":"2407827669767","lpFeePct":"0","timestamp":"1789340112","isAmountTooLow":false,"quoteBlock":"11698950","exclusiveRelayer":"0x0000000000000000000000000000000000000000","exclusivityDeadline":0,"spokePoolAddress":"0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662","destinationSpokePoolAddress":"0x82B564983aE7274c86695917BBf8C99ECb6F0F8F","totalRelayFee":{"pct":"2407827669767028","total":"2407827669767"},"relayerCapitalFee":{"pct":"99958333334000","total":"99958333334"},"relayerGasFee":{"pct":"981018513630000","total":"981018513630"},"lpFee":{"pct":"1326850822803028","total":"1326850822803"},"internalizedSwapFee":{"pct":"0","total":"0"},"limits":{"minDeposit":"3925643657709","maxDeposit":"3231735644024318","maxDepositInstant":"3231735644024318","maxDepositShortDelay":"3231735644024318","recommendedDepositInstant":"3231735644024318"},"fillDeadline":"1789347312","outputAmount":"997592172330233","inputToken":{"address":"0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14","symbol":"WETH","decimals":18,"chainId":11155111},"outputToken":{"address":"0x4200000000000000000000000000000000000006","symbol":"WETH","decimals":18,"chainId":84532},"id":"6dr22-1789340515991-1eb23d940902"}
    ```
- **`/deposit/status` — confirmed live parameter acceptance and error shape**:
  - `GET /deposit/status` with no params → HTTP 400, body `{"error":"IncorrectQueryParamsException","message":"Incorrect query params provided"}` (proves the endpoint validates params, so the next two results are meaningful, not "anything goes").
  - `GET /deposit/status?originChainId=11155111&depositId=<n>` → HTTP 404, body `{"error":"DepositNotFoundException","message":"Deposit not found given the provided constraints"}` (params accepted, no matching deposit — expected for a made-up id).
  - `GET /deposit/status?originChainId=11155111&depositTxHash=<hash>` → same 404/accepted shape. **`depositTxHash` is a valid, accepted query parameter** — this plan uses `depositTxHash` (our own `signed_tx_hash`) rather than `depositId`, avoiding any need to parse `FundsDeposited` event logs out of the origin receipt just to extract a numeric deposit id.
  - The exact **success**-case response shape (the `status` field's precise enum spelling — `"filled"` etc.) was **not** observed live (no real deposit was made during planning). Task 10 confirms this against docs.across.to and/or a real deposit before finalizing `status.go`'s response struct.
- **Auth**: both endpoints above returned real data/errors with **no** `Authorization` header and **no** `integratorId` query param sent. Testnet does not currently enforce the mainnet-documented Bearer-key/integrator-id requirement. `ACROSS_API_KEY`/`ACROSS_INTEGRATOR_ID` remain wired as optional (harmless if ever required later), per the spec.

---

### Task 1: Across verification gate + go-ethereum dependency

**Files:**
- Create: `docs/superpowers/reports/2026-09-13-across-verification-findings.md`
- Modify: `go-api/go.mod`, `go-api/go.sum`

**Interfaces:**
- Produces: a committed findings doc other tasks' code comments may cite; the `github.com/ethereum/go-ethereum` dependency available to Tasks 8, 11.

- [ ] **Step 1: Re-verify the facts above are still current**

Re-run the same checks this plan's own research used, and note any drift:

```bash
curl -s "https://testnet.across.to/api/available-routes?originChainId=11155111&destinationChainId=84532"
curl -s "https://testnet.across.to/api/suggested-fees?originChainId=11155111&destinationChainId=84532&inputToken=0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14&outputToken=0x4200000000000000000000000000000000000006&amount=1000000000000000"
curl -sL "https://raw.githubusercontent.com/across-protocol/contracts/master/deployments/sepolia/Ethereum_SpokePool.json" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['address'])"
curl -sL "https://raw.githubusercontent.com/across-protocol/contracts/master/deployments/base-sepolia/Base_SpokePool.json" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['address'])"
```

If any address, the route's presence, or the `depositV3` ABI shape has changed from the "Verified Across testnet facts" section above this task, **STOP** and report the conflict rather than silently proceeding — this plan's later tasks (8, 11) hardcode the values recorded there.

- [ ] **Step 2: Confirm the `/deposit/status` success response shape**

Look up the current `docs.across.to` API reference for `GET /deposit/status` (search for "Across API reference deposit status" if the direct URL 404s, as it did during planning). Record the exact `status` field enum spelling and any other fields present on a `filled`/`pending`/`expired`/`refunded` response. If no real deposit can be observed and docs are unclear, note this explicitly as a residual unknown for Task 10 to defend against with tolerant parsing (unknown status strings must not crash the reconciler — treat as `pending`, log a warning).

- [ ] **Step 3: Write the findings doc**

Write `docs/superpowers/reports/2026-09-13-across-verification-findings.md` containing: the confirmed (or updated) addresses, ABI, route status, auth findings, and the `/deposit/status` response shape from Step 2. Copy forward the "Verified Across testnet facts" section from this plan file as the base, editing only what Step 1/2 found to have changed.

- [ ] **Step 4: Add the go-ethereum dependency**

```bash
cd go-api
go get github.com/ethereum/go-ethereum@v1.14.11
go mod tidy
```

Use the latest `v1.14.x` tag available at implementation time if `v1.14.11` no longer resolves — this is a large dependency tree; `go mod tidy` will pull in its transitive requirements. Do not vendor.

- [ ] **Step 5: Verify the build still succeeds with the new dependency present but unused**

```bash
go build ./...
```

Expected: succeeds (no code references go-ethereum yet, but the module resolves and compiles cleanly).

- [ ] **Step 6: Commit**

```bash
git add docs/superpowers/reports/2026-09-13-across-verification-findings.md go-api/go.mod go-api/go.sum
git commit -m "Add Across testnet verification findings and go-ethereum dependency"
```

---

### Task 2: Exact decimal → base-units conversion (`internal/money`)

**Files:**
- Create: `go-api/internal/money/convert.go`
- Test: `go-api/internal/money/convert_test.go`

**Interfaces:**
- Produces: `money.DecimalToBaseUnits(amount string, decimals uint8) (*big.Int, error)`, consumed by Task 12 (`worker/executor.go`, which converts the payment amount before passing it into Task 11's `DepositV3Params.InputAmount` — Task 11's `BuildAndSignDepositV3Tx` itself takes an already-converted `*big.Int` and never calls this directly) and Task 15 (handler's `MAX_TESTNET_AMOUNT_WEI` check).

- [ ] **Step 1: Write the failing tests**

```go
package money

import (
	"math/big"
	"testing"
)

func TestDecimalToBaseUnits_ExactFit(t *testing.T) {
	got, err := DecimalToBaseUnits("1.123456", 6)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := big.NewInt(1123456)
	if got.Cmp(want) != 0 {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestDecimalToBaseUnits_RejectsExcessPrecision(t *testing.T) {
	_, err := DecimalToBaseUnits("1.1234567", 6)
	if err == nil {
		t.Fatal("expected an error for 7 fractional digits against a 6-decimal token, got none")
	}
}

func TestDecimalToBaseUnits_RightPadsShortFractional(t *testing.T) {
	got, err := DecimalToBaseUnits("1.5", 18)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want, _ := new(big.Int).SetString("1500000000000000000", 10)
	if got.Cmp(want) != 0 {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestDecimalToBaseUnits_NoFractionalPart(t *testing.T) {
	got, err := DecimalToBaseUnits("42", 18)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want, _ := new(big.Int).SetString("42000000000000000000", 10)
	if got.Cmp(want) != 0 {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestDecimalToBaseUnits_MaxDigitsWETH(t *testing.T) {
	// 20 integer digits, 18 fractional digits -- the maximum Phase 5's
	// amount pattern (^\d{1,20}(\.\d{1,18})?$) allows.
	got, err := DecimalToBaseUnits("99999999999999999999.999999999999999999", 18)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want, _ := new(big.Int).SetString("99999999999999999999999999999999999999", 10)
	if got.Cmp(want) != 0 {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestDecimalToBaseUnits_TrailingZerosExactFit(t *testing.T) {
	got, err := DecimalToBaseUnits("2.500000", 6)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := big.NewInt(2500000)
	if got.Cmp(want) != 0 {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestDecimalToBaseUnits_RejectionProducesNoValue(t *testing.T) {
	got, err := DecimalToBaseUnits("1.1234567", 6)
	if err == nil {
		t.Fatal("expected error")
	}
	if got != nil {
		t.Fatalf("expected a nil result alongside the rejection error, got %v", got)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd go-api && go test ./internal/money/... -v`
Expected: FAIL with "undefined: DecimalToBaseUnits" (the package doesn't exist yet).

- [ ] **Step 3: Implement**

```go
package money

import (
	"fmt"
	"math/big"
	"strings"
)

// DecimalToBaseUnits converts an exact decimal amount string (already
// validated by the caller against a pattern like Phase 5's
// ^\d{1,20}(\.\d{1,18})?$) into the token's integer base units, using
// big.Int exclusively -- never float64 or big.Float.
//
// If amount's fractional part is shorter than decimals, it is right-padded
// with zeros (always exact). If it is LONGER than decimals, the amount
// cannot be represented exactly at that precision: this returns an error
// rather than truncating the excess digits, since silently dropping a
// fractional digit would move real value without the caller's knowledge.
func DecimalToBaseUnits(amount string, decimals uint8) (*big.Int, error) {
	intPart, fracPart, hasFrac := amount, "", false
	if idx := strings.IndexByte(amount, '.'); idx >= 0 {
		intPart, fracPart, hasFrac = amount[:idx], amount[idx+1:], true
	}
	_ = hasFrac

	if len(fracPart) > int(decimals) {
		return nil, fmt.Errorf("amount %q has %d fractional digits, exceeding the token's %d-decimal precision -- refusing to truncate", amount, len(fracPart), decimals)
	}

	padded := fracPart + strings.Repeat("0", int(decimals)-len(fracPart))
	digits := intPart + padded

	result, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return nil, fmt.Errorf("amount %q could not be parsed as an integer after base-unit conversion", amount)
	}
	return result, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd go-api && go test ./internal/money/... -v`
Expected: PASS, all 7 tests.

- [ ] **Step 5: Commit**

```bash
git add go-api/internal/money
git commit -m "Add exact decimal-to-base-units conversion with rejection, not truncation"
```

---

### Task 3: Extend `internal/payment` with testnet types

**Files:**
- Modify: `go-api/internal/payment/payment.go`

**Interfaces:**
- Consumes: nothing new.
- Produces: `payment.StatusSubmitted`, `payment.ExecutionMode` (`ExecutionModeSimulated = "simulated"`, `ExecutionModeTestnet = "testnet"`), `payment.Payment.ExecutionMode`/`BridgeProvider` fields, `payment.ExternalStatus` (`ExternalStatusPending|Filled|Expired|Refunded|Reverted`), and the new `payment.Execution` struct — all consumed by Tasks 5-16.

- [ ] **Step 1: Edit `payment.go`**

Replace the file's content with:

```go
package payment

import "time"

type Status string

const (
	StatusRouted     Status = "ROUTED"
	StatusProcessing Status = "PROCESSING"
	StatusSubmitted  Status = "SUBMITTED"
	StatusCompleted  Status = "COMPLETED"
	StatusFailed     Status = "FAILED"
)

// ExecutionMode selects how a payment's routing decision is carried out:
// ExecutionModeSimulated (default, Phase 1-6 behavior, unchanged) or
// ExecutionModeTestnet (Phase 7: a real signed Sepolia -> Base Sepolia
// transaction via Across).
type ExecutionMode string

const (
	ExecutionModeSimulated ExecutionMode = "simulated"
	ExecutionModeTestnet   ExecutionMode = "testnet"
)

// ExternalStatus is payment_executions' own finer-grained state machine,
// living entirely within the payments.status = SUBMITTED window.
type ExternalStatus string

const (
	ExternalStatusPending  ExternalStatus = "pending"
	ExternalStatusFilled   ExternalStatus = "filled"
	ExternalStatusExpired  ExternalStatus = "expired"
	ExternalStatusRefunded ExternalStatus = "refunded"
	ExternalStatusReverted ExternalStatus = "reverted"
)

type Payment struct {
	ID               string
	IdempotencyKey   string
	SourceChain      string
	DestinationChain string
	Asset            string
	Amount           string
	Status           Status
	TotalFee         float64
	Hops             []Hop
	ExecutionMode    ExecutionMode
	BridgeProvider   *string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	CompletedAt      *time.Time
}

type Hop struct {
	HopIndex    int
	FromChain   string
	ToChain     string
	BridgeName  string
	Fee         float64
	LatencyMs   float64
	Liquidity   float64
	Reliability float64
}

// Execution is one row of payment_executions: everything about the real
// external side effect for one testnet-mode payment. UNIQUE(payment_id) in
// the schema guarantees at most one Execution ever exists per payment.
type Execution struct {
	ID                 string
	PaymentID          string
	BridgeProvider     string
	OriginChainID      int64
	DestinationChainID int64
	WalletAddress      string
	Nonce              int64
	SignedTxHash       *string
	RawSignedTx        []byte
	BroadcastAt        *time.Time
	AcrossDepositID    *string
	ExternalStatus     ExternalStatus
	ConfirmedAt        *time.Time
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// CreateResult reports what Store.CreateOrGetPayment actually did.
type CreateResult int

const (
	Created  CreateResult = iota // a brand-new payment was inserted
	Replayed                     // an existing payment, same logical request, was returned
	Conflict                     // the idempotency key was already used with a different request
)
```

- [ ] **Step 2: Build to confirm no syntax errors and find broken call sites**

Run: `cd go-api && go build ./...`
Expected: fails in `internal/postgres` and `internal/handler` (they reference `payment.Payment{...}` positionally or via existing field names, which are additive here — a genuine failure here means a downstream package used a field name this step removed; there are none, so this should actually succeed). If it fails, it's because a Task-1-installed dependency isn't referenced yet, unrelated to this change — re-run `go vet ./internal/payment/...` alone to confirm this package compiles standalone.

- [ ] **Step 3: Commit**

```bash
git add go-api/internal/payment/payment.go
git commit -m "Add testnet execution types to internal/payment"
```

---

### Task 4: Migration 0004

**Files:**
- Create: `go-api/migrations/0004_across_testnet_execution.sql`

**Interfaces:**
- Produces: the `payments.execution_mode`/`bridge_provider` columns, the extended `payments_status_check`, `payment_executions`, and `wallet_nonces` tables that every remaining task depends on.

- [ ] **Step 1: Write the migration**

```sql
ALTER TABLE payments
    ADD COLUMN execution_mode TEXT NOT NULL DEFAULT 'simulated'
        CHECK (execution_mode IN ('simulated', 'testnet')),
    ADD COLUMN bridge_provider TEXT NULL;

ALTER TABLE payments
    DROP CONSTRAINT payments_status_check,
    ADD CONSTRAINT payments_status_check
        CHECK (status IN ('ROUTED', 'PROCESSING', 'SUBMITTED', 'COMPLETED', 'FAILED'));

CREATE TABLE payment_executions (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id           UUID NOT NULL REFERENCES payments(id) ON DELETE CASCADE,
    bridge_provider      TEXT NOT NULL,
    origin_chain_id      BIGINT NOT NULL,
    destination_chain_id BIGINT NOT NULL,
    wallet_address       TEXT NOT NULL,
    nonce                BIGINT NOT NULL,
    unsigned_tx_params   JSONB NULL,
    signed_tx_hash       TEXT NULL,
    raw_signed_tx        BYTEA NULL,
    broadcast_at         TIMESTAMPTZ NULL,
    across_deposit_id    TEXT NULL,
    external_status      TEXT NOT NULL DEFAULT 'pending'
        CHECK (external_status IN ('pending', 'filled', 'expired', 'refunded', 'reverted')),
    confirmed_at         TIMESTAMPTZ NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (wallet_address, nonce),
    UNIQUE (payment_id)
);

CREATE INDEX payment_executions_pending_idx
    ON payment_executions (updated_at)
    WHERE external_status = 'pending';

CREATE TABLE wallet_nonces (
    wallet_address TEXT PRIMARY KEY,
    next_nonce     BIGINT NOT NULL
);
```

- [ ] **Step 2: Apply it to the local database and verify**

```bash
DATABASE_URL="${DATABASE_URL:-postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable}"
/opt/homebrew/opt/postgresql@16/bin/psql "$DATABASE_URL" -f go-api/migrations/0004_across_testnet_execution.sql
/opt/homebrew/opt/postgresql@16/bin/psql "$DATABASE_URL" -tAc "SELECT 1 FROM information_schema.tables WHERE table_name='payment_executions'"
/opt/homebrew/opt/postgresql@16/bin/psql "$DATABASE_URL" -tAc "SELECT 1 FROM pg_constraint WHERE conname = 'payment_executions_payment_id_key'"
```

Expected: both `psql -tAc` checks print `1`. The second check confirms Postgres's default auto-generated constraint name for an inline `UNIQUE (payment_id)` column constraint — Task 6's race-handling code depends on this exact name.

- [ ] **Step 3: Confirm existing Phase 1-6 tests still pass against the migrated schema**

```bash
cd go-api && DATABASE_URL="$DATABASE_URL" go test -tags=integration ./internal/postgres/... ./internal/worker/... -v
```

Expected: PASS — the new columns have safe defaults (`execution_mode` defaults to `'simulated'`), so no existing INSERT/SELECT should break.

- [ ] **Step 4: Commit**

```bash
git add go-api/migrations/0004_across_testnet_execution.sql
git commit -m "Add migration 0004: execution_mode, payment_executions, wallet_nonces"
```

---

### Task 5: Extend `postgres.Store` for `payments` columns + fix `Recovery` mode leak

**Files:**
- Modify: `go-api/internal/postgres/store.go`
- Modify: `go-api/internal/worker/processor.go` (call-site fallout only, see Step 4)
- Modify: `go-api/internal/worker/processor_test.go` (call-site fallout only)
- Test: `go-api/internal/postgres/store_integration_test.go` (add cases; do not remove/alter existing ones)

**Interfaces:**
- Consumes: `payment.ExecutionMode`, `payment.ExecutionModeSimulated` (Task 3).
- Produces: `Store.ClaimPayment(ctx, paymentID) (claimed bool, mode payment.ExecutionMode, err error)` (**signature change** — was `(bool, error)`), consumed by Task 14. `Store.GetPayment`/`CreateOrGetPayment`/`LookupByIdempotencyKey` now populate/accept `Payment.ExecutionMode`/`BridgeProvider`.

- [ ] **Step 1: Why `ClaimPayment`'s signature must change, and why this is safe**

`Processor.HandleRoutedPayment` (Task 14) must know a payment's `execution_mode` immediately after claiming it, to decide whether to run simulated execution or dispatch to the testnet executor. Fetching it via a second query would be an unnecessary round trip and a TOCTOU gap (the mode can't actually change after creation, so this is about efficiency and code clarity, not a race). `RETURNING execution_mode` on the existing conditional `UPDATE` gets it for free in the same statement. `Recovery` never calls `ClaimPayment` (it uses `StalePaymentIDs`/`CompletePayment` directly), so this change's blast radius is exactly: `Store.ClaimPayment`'s two callers (`Processor`, and `Processor`'s own tests) and `postgres`'s own integration tests for `ClaimPayment`.

- [ ] **Step 2: Add a normalization helper and update `findByIdempotencyKey`, `CreateOrGetPayment`, `GetPayment`, `LookupByIdempotencyKey`**

**Critical correctness note:** every existing Phase 1-6 test and call site constructs `payment.Payment{...}` **without** setting `ExecutionMode` (it didn't exist before this plan), so `p.ExecutionMode` will be `""` (Go's zero value) for all of them. The `payments.execution_mode` column is `NOT NULL CHECK (IN ('simulated','testnet'))` — inserting `""` directly would violate that constraint and break every unmodified Phase 5/6 test that creates a payment. Normalize `""` to `ExecutionModeSimulated` **in Go**, before it ever reaches SQL, in every function that reads or writes `p.ExecutionMode`. Add this helper near the top of `store.go`:

```go
func normalizeExecutionMode(m payment.ExecutionMode) payment.ExecutionMode {
	if m == "" {
		return payment.ExecutionModeSimulated
	}
	return m
}
```

Edit `findByIdempotencyKey` — add `execution_mode`, `bridge_provider` to the `SELECT`, include `execution_mode` in the `request_matches` computation (a retry that changes `execution_mode` for the same idempotency key is a genuine conflict, not a replay), and normalize before comparing:

```go
func (s *Store) findByIdempotencyKey(ctx context.Context, p payment.Payment) (existing payment.Payment, matches bool, found bool, err error) {
	mode := normalizeExecutionMode(p.ExecutionMode)
	row := s.db.QueryRowContext(ctx, `
		SELECT id, source_chain, destination_chain, asset, amount::text,
		       total_fee, status, execution_mode, bridge_provider, completed_at, created_at, updated_at,
		       (source_chain = $2 AND destination_chain = $3
		        AND asset = $4 AND amount = $5::NUMERIC AND execution_mode = $6) AS request_matches
		FROM payments
		WHERE idempotency_key = $1
	`, p.IdempotencyKey, p.SourceChain, p.DestinationChain, p.Asset, p.Amount, string(mode))

	var status, execMode string
	var bridgeProvider sql.NullString
	var completedAt sql.NullTime
	err = row.Scan(&existing.ID, &existing.SourceChain, &existing.DestinationChain,
		&existing.Asset, &existing.Amount, &existing.TotalFee, &status, &execMode, &bridgeProvider,
		&completedAt, &existing.CreatedAt, &existing.UpdatedAt, &matches)
	if errors.Is(err, sql.ErrNoRows) {
		return payment.Payment{}, false, false, nil
	}
	if err != nil {
		return payment.Payment{}, false, false, fmt.Errorf("find by idempotency key: %w", err)
	}
	existing.Status = payment.Status(status)
	existing.ExecutionMode = payment.ExecutionMode(execMode)
	if bridgeProvider.Valid {
		existing.BridgeProvider = &bridgeProvider.String
	}
	if completedAt.Valid {
		existing.CompletedAt = &completedAt.Time
	}
	existing.IdempotencyKey = p.IdempotencyKey
	return existing, matches, true, nil
}
```

Edit `GetPayment`'s `SELECT`/`Scan` the same way (add `execution_mode, bridge_provider` columns, scan into the same pattern as above).

Edit `CreateOrGetPayment`: normalize `p.ExecutionMode` once at the top (before the `findByIdempotencyKey` pre-check, so the pre-check compares normalized values too), pass it and `p.BridgeProvider` into the `INSERT`:

```go
func (s *Store) CreateOrGetPayment(ctx context.Context, p payment.Payment) (payment.Payment, payment.CreateResult, error) {
	p.ExecutionMode = normalizeExecutionMode(p.ExecutionMode)
	// ... existing pre-check via findByIdempotencyKey(ctx, p), unchanged ...

	// ... existing tx begin, unchanged ...

	var created payment.Payment
	row := tx.QueryRowContext(ctx, `
		INSERT INTO payments (idempotency_key, source_chain, destination_chain, asset, amount, total_fee, execution_mode, bridge_provider)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id, amount::text, created_at, updated_at
	`, p.IdempotencyKey, p.SourceChain, p.DestinationChain, p.Asset, p.Amount, p.TotalFee, string(p.ExecutionMode), p.BridgeProvider)

	// ... existing Scan/lost-race handling, unchanged ...

	// ... existing hops loop, outbox insert, commit, unchanged ...

	created.IdempotencyKey = p.IdempotencyKey
	created.SourceChain = p.SourceChain
	created.DestinationChain = p.DestinationChain
	created.Asset = p.Asset
	created.Status = payment.StatusRouted
	created.TotalFee = p.TotalFee
	created.Hops = p.Hops
	created.ExecutionMode = p.ExecutionMode
	created.BridgeProvider = p.BridgeProvider

	return created, payment.Created, nil
}
```

`p.BridgeProvider` is a `*string`; passing a nil `*string` to `database/sql` as a query parameter correctly binds `NULL` — no extra handling needed. `LookupByIdempotencyKey` needs no changes beyond `findByIdempotencyKey` already normalizing and returning the new fields — verify by reading it, but do not add logic there.

- [ ] **Step 3: Change `ClaimPayment`'s signature**

```go
// ClaimPayment atomically transitions a payment from ROUTED to PROCESSING,
// returning its execution_mode in the same round trip. claimed=false means
// the payment was not ROUTED (already claimed by another delivery, or in
// some other state) -- a safe no-op, not an error; mode is the zero value
// in that case and must not be used.
func (s *Store) ClaimPayment(ctx context.Context, paymentID string) (claimed bool, mode payment.ExecutionMode, err error) {
	var modeStr string
	row := s.db.QueryRowContext(ctx, `
		UPDATE payments SET status = $2, updated_at = now()
		WHERE id = $1 AND status = $3
		RETURNING execution_mode
	`, paymentID, payment.StatusProcessing, payment.StatusRouted)
	err = row.Scan(&modeStr)
	if errors.Is(err, sql.ErrNoRows) {
		return false, "", nil
	}
	if err != nil {
		return false, "", fmt.Errorf("claim payment: %w", err)
	}
	return true, payment.ExecutionMode(modeStr), nil
}
```

- [ ] **Step 4: Fix every broken call site**

`go build ./...` will now fail everywhere `ClaimPayment`'s old 2-value return was used. Fix each:

`go-api/internal/worker/processor.go` — `p.Store.ClaimPayment(ctx, evt.PaymentID)` now returns 3 values. For this task only, discard the mode with `_` (Task 14 wires the real dispatch logic):

```go
claimed, _, err := p.Store.ClaimPayment(ctx, evt.PaymentID)
```

Also update the `PaymentStore` interface in the same file:

```go
type PaymentStore interface {
	ClaimPayment(ctx context.Context, paymentID string) (bool, payment.ExecutionMode, error)
	CompletePayment(ctx context.Context, paymentID string, terminal payment.Status) (bool, error)
}
```

`go-api/internal/worker/processor_test.go` — `fakeStore.ClaimPayment` must match the new interface:

```go
type fakeStore struct {
	claimResult    bool
	claimMode      payment.ExecutionMode
	claimErr       error
	completeResult bool
	completeErr    error
	claimCalls     int
	completeCalls  int
	lastTerminal   payment.Status
}

func (f *fakeStore) ClaimPayment(ctx context.Context, paymentID string) (bool, payment.ExecutionMode, error) {
	f.claimCalls++
	mode := f.claimMode
	if mode == "" {
		mode = payment.ExecutionModeSimulated
	}
	return f.claimResult, mode, f.claimErr
}
```

Every existing test in this file constructs `&fakeStore{claimResult: true, completeResult: true}` etc. without setting `claimMode` — the `if mode == ""` default above keeps every existing test passing unmodified with simulated-mode behavior, satisfying the Global Constraint that Phase 1-6 tests never need their assertions changed.

- [ ] **Step 5: Fix `StalePaymentIDs` to stay simulated-mode-only**

This is required, not optional: `Recovery` (per the approved Phase 7 spec §15) must never touch testnet-mode payments — it calls the pure, deterministic `execution.Execute`, which has no meaning for a real external transaction. Before this plan, `StalePaymentIDs` had no `execution_mode` column to filter on, so it implicitly swept everything; now that the column exists, it must explicitly exclude testnet-mode payments or Phase 7 introduces a real correctness bug into unmodified Phase 6 code:

```go
func (s *Store) StalePaymentIDs(ctx context.Context, staleness time.Duration) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id FROM payments
		WHERE status = $1
		  AND execution_mode = $3
		  AND updated_at < now() - make_interval(secs => $2)
	`, payment.StatusProcessing, staleness.Seconds(), string(payment.ExecutionModeSimulated))
	// ... rest unchanged ...
}
```

- [ ] **Step 6: Add integration test cases (do not touch existing ones)**

Add to `store_integration_test.go`:

```go
func TestCreateOrGetPayment_DefaultsExecutionModeToSimulated(t *testing.T) {
	s := newTestStore(t)
	key := "test-default-mode-key"
	cleanup := func() {
		s.db.ExecContext(context.Background(), `DELETE FROM payments WHERE idempotency_key = $1`, key)
	}
	cleanup()
	t.Cleanup(cleanup)

	p := testPayment(key) // does not set ExecutionMode
	result, outcome, err := s.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome != payment.Created {
		t.Fatalf("expected Created, got %v", outcome)
	}
	if result.ExecutionMode != payment.ExecutionModeSimulated {
		t.Fatalf("expected simulated, got %q", result.ExecutionMode)
	}
}

func TestClaimPayment_ReturnsExecutionMode(t *testing.T) {
	s := newTestStore(t)
	key := "test-claim-mode-key"
	cleanup := func() {
		s.db.ExecContext(context.Background(), `DELETE FROM payments WHERE idempotency_key = $1`, key)
	}
	cleanup()
	t.Cleanup(cleanup)

	p := testPayment(key)
	created, _, err := s.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	claimed, mode, err := s.ClaimPayment(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if !claimed {
		t.Fatal("expected claim to succeed")
	}
	if mode != payment.ExecutionModeSimulated {
		t.Fatalf("expected simulated, got %q", mode)
	}
}

func TestStalePaymentIDs_ExcludesTestnetMode(t *testing.T) {
	s := newTestStore(t)
	key := "test-stale-excludes-testnet-key"
	cleanup := func() {
		s.db.ExecContext(context.Background(), `DELETE FROM payments WHERE idempotency_key = $1`, key)
	}
	cleanup()
	t.Cleanup(cleanup)

	p := testPayment(key)
	p.ExecutionMode = payment.ExecutionModeTestnet
	created, _, err := s.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, _, err := s.ClaimPayment(context.Background(), created.ID); err != nil {
		t.Fatalf("claim: %v", err)
	}
	// Force it stale.
	if _, err := s.db.ExecContext(context.Background(),
		`UPDATE payments SET updated_at = now() - interval '1 hour' WHERE id = $1`, created.ID); err != nil {
		t.Fatalf("backdate: %v", err)
	}

	ids, err := s.StalePaymentIDs(context.Background(), time.Minute)
	if err != nil {
		t.Fatalf("stale ids: %v", err)
	}
	for _, id := range ids {
		if id == created.ID {
			t.Fatal("StalePaymentIDs must never return a testnet-mode payment -- Recovery's execution.Execute has no meaning for a real transaction")
		}
	}
}
```

- [ ] **Step 7: Run tests**

```bash
cd go-api
go build ./...
go test ./internal/worker/... -v
DATABASE_URL="${DATABASE_URL:-postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable}" go test -tags=integration ./internal/postgres/... ./internal/worker/... -v
```

Expected: all PASS, including every pre-existing test in `processor_test.go`, `store_integration_test.go`, `recovery_integration_test.go` unmodified.

- [ ] **Step 8: Commit**

```bash
git add go-api/internal/postgres/store.go go-api/internal/postgres/store_integration_test.go go-api/internal/worker/processor.go go-api/internal/worker/processor_test.go
git commit -m "Extend Store for execution_mode; keep Recovery simulated-mode-only"
```

---

### Task 6: `TryCreateExecution` — the race-critical atomic claim

**Files:**
- Create: `go-api/internal/postgres/execution_store.go`
- Test: `go-api/internal/postgres/execution_store_integration_test.go`

**Interfaces:**
- Consumes: `payment.Execution`, `payment.ExecutionModeTestnet` (Task 3).
- Produces: `Store.SeedWalletNonce(ctx, walletAddress string, initialNonce int64) error`, `Store.TryCreateExecution(ctx, p CreateExecutionParams) (exec payment.Execution, created bool, err error)`, `CreateExecutionParams{PaymentID, WalletAddress, BridgeProvider string; OriginChainID, DestinationChainID int64}` — all consumed by Task 12 (executor) and Task 13 (reconciler).

- [ ] **Step 1: Write the failing concurrency test first**

This is the test the user's requirements explicitly call for: proving the executor-vs-reconciler race resolves to exactly one execution row, exactly one nonce consumed, and the loser's nonce allocation rolls back rather than merely going unused.

```go
//go:build integration

package postgres

import (
	"context"
	"sync"
	"testing"

	"chainroute/go-api/internal/payment"
)

func insertRawTestnetPayment(t *testing.T, s *Store, idempotencyKey string) string {
	t.Helper()
	var id string
	err := s.db.QueryRowContext(context.Background(), `
		INSERT INTO payments (idempotency_key, source_chain, destination_chain, asset, amount, total_fee, execution_mode, status)
		VALUES ($1, 'ethereum', 'base', 'ETH', '0.001', 0, 'testnet', 'PROCESSING')
		RETURNING id
	`, idempotencyKey).Scan(&id)
	if err != nil {
		t.Fatalf("insert raw testnet payment: %v", err)
	}
	return id
}

func TestTryCreateExecution_ExecutorReconcilerRaceIsSafe(t *testing.T) {
	s := newTestStore(t)
	key := "test-race-key"
	wallet := "0xRaceTestWallet00000000000000000000001"

	cleanup := func() {
		s.db.ExecContext(context.Background(), `DELETE FROM payments WHERE idempotency_key = $1`, key)
		s.db.ExecContext(context.Background(), `DELETE FROM wallet_nonces WHERE wallet_address = $1`, wallet)
	}
	cleanup()
	t.Cleanup(cleanup)

	paymentID := insertRawTestnetPayment(t, s, key)
	if err := s.SeedWalletNonce(context.Background(), wallet, 5); err != nil {
		t.Fatalf("seed nonce: %v", err)
	}

	params := CreateExecutionParams{
		PaymentID: paymentID, WalletAddress: wallet, BridgeProvider: "across",
		OriginChainID: 11155111, DestinationChainID: 84532,
	}

	const attempts = 2
	var wg sync.WaitGroup
	created := make([]bool, attempts)
	errs := make([]error, attempts)
	wg.Add(attempts)
	for i := 0; i < attempts; i++ {
		i := i
		go func() {
			defer wg.Done()
			_, c, err := s.TryCreateExecution(context.Background(), params)
			created[i] = c
			errs[i] = err
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("attempt %d: unexpected error: %v", i, err)
		}
	}
	winners := 0
	for _, c := range created {
		if c {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("expected exactly 1 winner among %d concurrent attempts, got %d", attempts, winners)
	}

	var rowCount int
	if err := s.db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM payment_executions WHERE payment_id = $1`, paymentID).Scan(&rowCount); err != nil {
		t.Fatalf("count executions: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("expected exactly 1 payment_executions row, got %d", rowCount)
	}

	var nextNonce int64
	if err := s.db.QueryRowContext(context.Background(),
		`SELECT next_nonce FROM wallet_nonces WHERE wallet_address = $1`, wallet).Scan(&nextNonce); err != nil {
		t.Fatalf("read next_nonce: %v", err)
	}
	if nextNonce != 6 {
		t.Fatalf("expected next_nonce to have advanced by exactly 1 (seeded at 5, expected 6), got %d -- "+
			"a lost race must roll back its nonce allocation, not merely leave it unused", nextNonce)
	}
}

func TestTryCreateExecution_SecondCallForSamePaymentIsSafeNoOp(t *testing.T) {
	s := newTestStore(t)
	key := "test-second-call-key"
	wallet := "0xSecondCallWallet0000000000000000000002"
	cleanup := func() {
		s.db.ExecContext(context.Background(), `DELETE FROM payments WHERE idempotency_key = $1`, key)
		s.db.ExecContext(context.Background(), `DELETE FROM wallet_nonces WHERE wallet_address = $1`, wallet)
	}
	cleanup()
	t.Cleanup(cleanup)

	paymentID := insertRawTestnetPayment(t, s, key)
	if err := s.SeedWalletNonce(context.Background(), wallet, 0); err != nil {
		t.Fatalf("seed: %v", err)
	}
	params := CreateExecutionParams{PaymentID: paymentID, WalletAddress: wallet, BridgeProvider: "across", OriginChainID: 11155111, DestinationChainID: 84532}

	first, created1, err := s.TryCreateExecution(context.Background(), params)
	if err != nil || !created1 {
		t.Fatalf("first call: created=%v err=%v", created1, err)
	}
	second, created2, err := s.TryCreateExecution(context.Background(), params)
	if err != nil {
		t.Fatalf("second call: unexpected error: %v", err)
	}
	if created2 {
		t.Fatal("second call for the same payment must report created=false, not create a second row")
	}
	_ = second
	if first.Nonce != 0 {
		t.Fatalf("expected first execution to hold nonce 0, got %d", first.Nonce)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd go-api && DATABASE_URL="..." go test -tags=integration ./internal/postgres/... -run TestTryCreateExecution -v`
Expected: FAIL with "undefined: CreateExecutionParams" / "undefined: SeedWalletNonce" / "undefined: TryCreateExecution".

- [ ] **Step 3: Implement `execution_store.go`**

```go
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"

	"chainroute/go-api/internal/payment"
)

// SeedWalletNonce inserts the wallet's starting nonce exactly once. A
// second call for the same wallet is a safe no-op -- this is intended to
// be called at every worker startup with the chain's own
// eth_getTransactionCount(wallet, "pending") value; only the very first
// call across the wallet's lifetime actually takes effect (Phase 7 design
// spec §7): wallet_nonces.next_nonce is never overwritten from a chain
// read after that.
func (s *Store) SeedWalletNonce(ctx context.Context, walletAddress string, initialNonce int64) error {
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO wallet_nonces (wallet_address, next_nonce)
		VALUES ($1, $2)
		ON CONFLICT (wallet_address) DO NOTHING
	`, walletAddress, initialNonce); err != nil {
		return fmt.Errorf("seed wallet nonce: %w", err)
	}
	return nil
}

// CreateExecutionParams is everything TryCreateExecution needs to allocate
// a nonce and create a payment's execution row. Phase 7 has exactly one
// wallet/bridge/route, so these are typically the same constants on every
// call; they are still parameters (not hardcoded) so tests can use
// isolated wallet addresses without colliding.
type CreateExecutionParams struct {
	PaymentID           string
	WalletAddress       string
	BridgeProvider      string
	OriginChainID       int64
	DestinationChainID  int64
}

// executionExistsConstraint is Postgres's default auto-generated name for
// an inline UNIQUE(payment_id) column constraint on payment_executions
// (verified against the actual migrated schema in Task 4, Step 2).
const executionExistsConstraint = "payment_executions_payment_id_key"

// TryCreateExecution atomically allocates the next nonce for
// p.WalletAddress and creates p.PaymentID's payment_executions row, in one
// Postgres transaction (Phase 7 design spec §8, §15): the row is only ever
// inserted already carrying its allocated nonce, so there is no durable
// intermediate state where a nonce is allocated but no execution row
// exists for it.
//
// created=false, err=nil means UNIQUE(payment_id) rejected this attempt
// because another actor (the original executor, or a concurrent
// reconciler tick) already created the execution row for this payment
// first -- this is the expected, safe outcome of the executor/reconciler
// race described in the design spec §15, not an error. Because the nonce
// UPDATE and the row INSERT are one transaction, losing this race rolls
// back the nonce allocation too: a lost race never burns a nonce.
func (s *Store) TryCreateExecution(ctx context.Context, p CreateExecutionParams) (exec payment.Execution, created bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return payment.Execution{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var nonce int64
	nonceRow := tx.QueryRowContext(ctx, `
		UPDATE wallet_nonces SET next_nonce = next_nonce + 1
		WHERE wallet_address = $1
		RETURNING next_nonce - 1
	`, p.WalletAddress)
	if err := nonceRow.Scan(&nonce); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return payment.Execution{}, false, fmt.Errorf("wallet %s has no seeded wallet_nonces row -- SeedWalletNonce must run at worker startup before any execution is attempted", p.WalletAddress)
		}
		return payment.Execution{}, false, fmt.Errorf("allocate nonce: %w", err)
	}

	var externalStatus string
	insertRow := tx.QueryRowContext(ctx, `
		INSERT INTO payment_executions
			(payment_id, bridge_provider, origin_chain_id, destination_chain_id, wallet_address, nonce)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, external_status, created_at, updated_at
	`, p.PaymentID, p.BridgeProvider, p.OriginChainID, p.DestinationChainID, p.WalletAddress, nonce)
	if err := insertRow.Scan(&exec.ID, &externalStatus, &exec.CreatedAt, &exec.UpdatedAt); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == executionExistsConstraint {
			return payment.Execution{}, false, nil
		}
		return payment.Execution{}, false, fmt.Errorf("insert execution: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return payment.Execution{}, false, fmt.Errorf("commit: %w", err)
	}

	exec.PaymentID = p.PaymentID
	exec.BridgeProvider = p.BridgeProvider
	exec.OriginChainID = p.OriginChainID
	exec.DestinationChainID = p.DestinationChainID
	exec.WalletAddress = p.WalletAddress
	exec.Nonce = nonce
	exec.ExternalStatus = payment.ExternalStatus(externalStatus)
	return exec, true, nil
}
```

- [ ] **Step 4: Run to verify both tests pass**

Run: `cd go-api && DATABASE_URL="..." go test -tags=integration ./internal/postgres/... -run TestTryCreateExecution -v -race`
Expected: PASS. Running with `-race` is deliberate here — this is exactly the kind of concurrent-goroutine test the race detector is for, and it must be clean.

- [ ] **Step 5: Mutation check (do this now, not deferred to final review)**

Temporarily remove `UNIQUE (payment_id)` from a **scratch copy** of the schema (a throwaway test database or a manually-run `ALTER TABLE payment_executions DROP CONSTRAINT payment_executions_payment_id_key;` against a disposable local database — never the shared dev database other tasks depend on), rerun `TestTryCreateExecution_ExecutorReconcilerRaceIsSafe` several times, and confirm it now fails (more than one row / winner, or `next_nonce` advances by 2). This proves the test actually exercises the constraint rather than passing vacuously because the race never really overlaps. Restore the constraint (or just discard the scratch database) before continuing. Record the before/after result in the task report.

- [ ] **Step 6: Commit**

```bash
git add go-api/internal/postgres/execution_store.go go-api/internal/postgres/execution_store_integration_test.go
git commit -m "Add TryCreateExecution: race-safe nonce allocation + execution row creation"
```

---

### Task 7: Remaining `payment_executions`/reconciliation store methods

**Files:**
- Modify: `go-api/internal/postgres/execution_store.go`
- Test: `go-api/internal/postgres/execution_store_integration_test.go`

**Interfaces:**
- Consumes: `payment.Execution`, `payment.ExternalStatus`, `payment.Status` (Task 3); `Store.TryCreateExecution` (Task 6, for test setup).
- Produces: `Store.GetExecutionByPaymentID`, `Store.PersistSignedExecution`, `Store.MarkExecutionBroadcast`, `Store.UpdateExecutionExternalStatus`, `Store.MarkSubmitted`, `Store.CompleteSubmittedPayment`, `Store.StaleTestnetProcessingWithoutExecutionIDs`, `Store.ReconciliationCandidates`, `Store.LowestUnconfirmedNonce` — consumed by Task 12 (executor), Task 13 (reconciler), Task 15 (handler response fields).

- [ ] **Step 1: Append these methods to `execution_store.go`**

```go
// GetExecutionByPaymentID returns the (at most one, per UNIQUE(payment_id))
// execution row for paymentID. found=false means testnet execution has not
// started for this payment yet, or it is a simulated-mode payment.
func (s *Store) GetExecutionByPaymentID(ctx context.Context, paymentID string) (payment.Execution, bool, error) {
	var e payment.Execution
	var signedTxHash, acrossDepositID sql.NullString
	var broadcastAt, confirmedAt sql.NullTime
	var externalStatus string
	row := s.db.QueryRowContext(ctx, `
		SELECT id, payment_id, bridge_provider, origin_chain_id, destination_chain_id,
		       wallet_address, nonce, signed_tx_hash, raw_signed_tx, broadcast_at,
		       across_deposit_id, external_status, confirmed_at, created_at, updated_at
		FROM payment_executions
		WHERE payment_id = $1
	`, paymentID)
	err := row.Scan(&e.ID, &e.PaymentID, &e.BridgeProvider, &e.OriginChainID, &e.DestinationChainID,
		&e.WalletAddress, &e.Nonce, &signedTxHash, &e.RawSignedTx, &broadcastAt,
		&acrossDepositID, &externalStatus, &confirmedAt, &e.CreatedAt, &e.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return payment.Execution{}, false, nil
	}
	if err != nil {
		return payment.Execution{}, false, fmt.Errorf("get execution by payment id: %w", err)
	}
	if signedTxHash.Valid {
		e.SignedTxHash = &signedTxHash.String
	}
	if acrossDepositID.Valid {
		e.AcrossDepositID = &acrossDepositID.String
	}
	if broadcastAt.Valid {
		e.BroadcastAt = &broadcastAt.Time
	}
	if confirmedAt.Valid {
		e.ConfirmedAt = &confirmedAt.Time
	}
	e.ExternalStatus = payment.ExternalStatus(externalStatus)
	return e, true, nil
}

// PersistSignedExecution durably persists the signed transaction bytes and
// its deterministic hash BEFORE any broadcast attempt -- this ordering is
// the core of Phase 7's crash-safety (design spec §8 step 2).
func (s *Store) PersistSignedExecution(ctx context.Context, executionID string, rawTx []byte, txHash string) error {
	if _, err := s.db.ExecContext(ctx, `
		UPDATE payment_executions SET raw_signed_tx = $2, signed_tx_hash = $3, updated_at = now()
		WHERE id = $1
	`, executionID, rawTx, txHash); err != nil {
		return fmt.Errorf("persist signed execution: %w", err)
	}
	return nil
}

// MarkExecutionBroadcast records that eth_sendRawTransaction was actually
// attempted for this execution -- called only after PersistSignedExecution
// has already committed.
func (s *Store) MarkExecutionBroadcast(ctx context.Context, executionID string) error {
	if _, err := s.db.ExecContext(ctx, `
		UPDATE payment_executions SET broadcast_at = now(), updated_at = now()
		WHERE id = $1
	`, executionID); err != nil {
		return fmt.Errorf("mark execution broadcast: %w", err)
	}
	return nil
}

// UpdateExecutionExternalStatus records the reconciler's observed terminal
// (or still-pending) outcome for one execution.
func (s *Store) UpdateExecutionExternalStatus(ctx context.Context, executionID string, status payment.ExternalStatus, confirmedAt *sql.NullTime) error {
	if _, err := s.db.ExecContext(ctx, `
		UPDATE payment_executions SET external_status = $2, confirmed_at = $3, updated_at = now()
		WHERE id = $1
	`, executionID, string(status), confirmedAt); err != nil {
		return fmt.Errorf("update execution external status: %w", err)
	}
	return nil
}

// MarkSubmitted transitions a testnet-mode payment from PROCESSING to
// SUBMITTED once its transaction has actually been broadcast (design spec
// §7, §10). submitted=false means the payment was not PROCESSING -- a safe
// no-op, mirroring ClaimPayment/CompletePayment's own guard pattern.
func (s *Store) MarkSubmitted(ctx context.Context, paymentID string) (submitted bool, err error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE payments SET status = $2, updated_at = now()
		WHERE id = $1 AND status = $3
	`, paymentID, payment.StatusSubmitted, payment.StatusProcessing)
	if err != nil {
		return false, fmt.Errorf("mark submitted: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("mark submitted rows affected: %w", err)
	}
	return rows == 1, nil
}

// CompleteSubmittedPayment atomically transitions a testnet-mode payment
// from SUBMITTED to a terminal status, mirroring CompletePayment's
// PROCESSING->terminal guard exactly, but for the SUBMITTED->terminal edge
// that only testnet-mode payments ever traverse.
func (s *Store) CompleteSubmittedPayment(ctx context.Context, paymentID string, terminal payment.Status) (completed bool, err error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE payments SET status = $2, completed_at = now(), updated_at = now()
		WHERE id = $1 AND status = $3
	`, paymentID, terminal, payment.StatusSubmitted)
	if err != nil {
		return false, fmt.Errorf("complete submitted payment: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("complete submitted payment rows affected: %w", err)
	}
	return rows == 1, nil
}

// StaleTestnetProcessingWithoutExecutionIDs returns testnet-mode payments
// stuck at crash point A (design spec §12): PROCESSING, stale, with no
// payment_executions row at all. Nothing else in the system revisits such
// a payment (Kafka redelivery no-ops per ClaimPayment's ROUTED-only guard;
// Recovery is simulated-mode-only per Task 5) -- this is the sole recovery
// path for it (design spec §15).
func (s *Store) StaleTestnetProcessingWithoutExecutionIDs(ctx context.Context, staleness time.Duration) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id FROM payments
		WHERE execution_mode = $1
		  AND status = $2
		  AND updated_at < now() - make_interval(secs => $3)
		  AND NOT EXISTS (
		      SELECT 1 FROM payment_executions WHERE payment_executions.payment_id = payments.id
		  )
	`, string(payment.ExecutionModeTestnet), string(payment.StatusProcessing), staleness.Seconds())
	if err != nil {
		return nil, fmt.Errorf("query stale testnet processing: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan stale testnet processing id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// ReconciliationCandidates returns every payment_executions row the
// reconciler should consider: already-broadcast-but-unconfirmed rows
// (checked every tick, no staleness needed), and not-yet-broadcast rows
// that have sat without progress longer than staleness (design spec §15).
func (s *Store) ReconciliationCandidates(ctx context.Context, staleness time.Duration) ([]payment.Execution, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, payment_id, bridge_provider, origin_chain_id, destination_chain_id,
		       wallet_address, nonce, signed_tx_hash, raw_signed_tx, broadcast_at,
		       across_deposit_id, external_status, confirmed_at, created_at, updated_at
		FROM payment_executions
		WHERE (broadcast_at IS NOT NULL AND confirmed_at IS NULL)
		   OR (broadcast_at IS NULL AND updated_at < now() - make_interval(secs => $1))
	`, staleness.Seconds())
	if err != nil {
		return nil, fmt.Errorf("query reconciliation candidates: %w", err)
	}
	defer rows.Close()

	var out []payment.Execution
	for rows.Next() {
		var e payment.Execution
		var signedTxHash, acrossDepositID sql.NullString
		var broadcastAt, confirmedAt sql.NullTime
		var externalStatus string
		if err := rows.Scan(&e.ID, &e.PaymentID, &e.BridgeProvider, &e.OriginChainID, &e.DestinationChainID,
			&e.WalletAddress, &e.Nonce, &signedTxHash, &e.RawSignedTx, &broadcastAt,
			&acrossDepositID, &externalStatus, &confirmedAt, &e.CreatedAt, &e.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan reconciliation candidate: %w", err)
		}
		if signedTxHash.Valid {
			e.SignedTxHash = &signedTxHash.String
		}
		if acrossDepositID.Valid {
			e.AcrossDepositID = &acrossDepositID.String
		}
		if broadcastAt.Valid {
			e.BroadcastAt = &broadcastAt.Time
		}
		if confirmedAt.Valid {
			e.ConfirmedAt = &confirmedAt.Time
		}
		e.ExternalStatus = payment.ExternalStatus(externalStatus)
		out = append(out, e)
	}
	return out, rows.Err()
}

// LowestUnconfirmedNonce returns the lowest nonce among walletAddress's
// unconfirmed executions (confirmed_at IS NULL, regardless of broadcast
// state) -- used both for the reconciler's lowest-nonce-first
// prioritization and its read-only chain-divergence check (design spec
// §15). found=false means the wallet has no unconfirmed executions.
func (s *Store) LowestUnconfirmedNonce(ctx context.Context, walletAddress string) (nonce int64, found bool, err error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT MIN(nonce) FROM payment_executions
		WHERE wallet_address = $1 AND confirmed_at IS NULL
	`, walletAddress)
	var n sql.NullInt64
	if err := row.Scan(&n); err != nil {
		return 0, false, fmt.Errorf("lowest unconfirmed nonce: %w", err)
	}
	if !n.Valid {
		return 0, false, nil
	}
	return n.Int64, true, nil
}
```

Add `"time"` to this file's imports (used by `StaleTestnetProcessingWithoutExecutionIDs`/`ReconciliationCandidates`).

**Note on `UpdateExecutionExternalStatus`'s `confirmedAt` parameter:** it takes `*sql.NullTime` rather than `*time.Time` deliberately, so a caller can express "leave `confirmed_at` unchanged, still pending" (`nil`) vs "explicitly set it to NULL" vs "set it to a real time" without three different method signatures. Task 13 always passes either `nil` (still pending) or `&sql.NullTime{Time: t, Valid: true}` (now confirmed) — it never needs the "explicitly NULL" case, but the type keeps the method honest about what's representable at the schema level.

- [ ] **Step 2: Write tests for each new method**

Add to `execution_store_integration_test.go` (one focused test per method — lifecycle, staleness filtering, and the two-clause candidate selection are the ones worth real assertions; trivial wrapper methods like `MarkExecutionBroadcast` need only a smoke test):

```go
func TestPersistSignedExecution_AndMarkBroadcast(t *testing.T) {
	s := newTestStore(t)
	key, wallet := "test-persist-signed-key", "0xPersistWallet000000000000000000000003"
	cleanup := func() {
		s.db.ExecContext(context.Background(), `DELETE FROM payments WHERE idempotency_key = $1`, key)
		s.db.ExecContext(context.Background(), `DELETE FROM wallet_nonces WHERE wallet_address = $1`, wallet)
	}
	cleanup()
	t.Cleanup(cleanup)

	paymentID := insertRawTestnetPayment(t, s, key)
	s.SeedWalletNonce(context.Background(), wallet, 0)
	exec, created, err := s.TryCreateExecution(context.Background(), CreateExecutionParams{
		PaymentID: paymentID, WalletAddress: wallet, BridgeProvider: "across", OriginChainID: 11155111, DestinationChainID: 84532,
	})
	if err != nil || !created {
		t.Fatalf("setup: created=%v err=%v", created, err)
	}

	if err := s.PersistSignedExecution(context.Background(), exec.ID, []byte{0x01, 0x02}, "0xdeadbeef"); err != nil {
		t.Fatalf("persist signed: %v", err)
	}
	got, found, err := s.GetExecutionByPaymentID(context.Background(), paymentID)
	if err != nil || !found {
		t.Fatalf("get: found=%v err=%v", found, err)
	}
	if got.SignedTxHash == nil || *got.SignedTxHash != "0xdeadbeef" {
		t.Fatalf("expected signed_tx_hash to be persisted, got %v", got.SignedTxHash)
	}
	if got.BroadcastAt != nil {
		t.Fatal("expected broadcast_at to still be NULL before MarkExecutionBroadcast")
	}

	if err := s.MarkExecutionBroadcast(context.Background(), exec.ID); err != nil {
		t.Fatalf("mark broadcast: %v", err)
	}
	got, _, _ = s.GetExecutionByPaymentID(context.Background(), paymentID)
	if got.BroadcastAt == nil {
		t.Fatal("expected broadcast_at to be set after MarkExecutionBroadcast")
	}
}

func TestReconciliationCandidates_TwoClauseSelection(t *testing.T) {
	s := newTestStore(t)
	keyPending, keyStale, wallet := "test-recon-pending-key", "test-recon-stale-key", "0xReconWallet00000000000000000000000004"
	cleanup := func() {
		s.db.ExecContext(context.Background(), `DELETE FROM payments WHERE idempotency_key IN ($1, $2)`, keyPending, keyStale)
		s.db.ExecContext(context.Background(), `DELETE FROM wallet_nonces WHERE wallet_address = $1`, wallet)
	}
	cleanup()
	t.Cleanup(cleanup)
	s.SeedWalletNonce(context.Background(), wallet, 0)

	// Row A: broadcast, unconfirmed -- must ALWAYS be a candidate.
	idA := insertRawTestnetPayment(t, s, keyPending)
	execA, _, _ := s.TryCreateExecution(context.Background(), CreateExecutionParams{PaymentID: idA, WalletAddress: wallet, BridgeProvider: "across", OriginChainID: 11155111, DestinationChainID: 84532})
	s.PersistSignedExecution(context.Background(), execA.ID, []byte{0x01}, "0xaaa")
	s.MarkExecutionBroadcast(context.Background(), execA.ID)

	// Row B: not yet broadcast, freshly created -- must NOT be a candidate yet.
	idB := insertRawTestnetPayment(t, s, keyStale)
	execB, _, _ := s.TryCreateExecution(context.Background(), CreateExecutionParams{PaymentID: idB, WalletAddress: wallet, BridgeProvider: "across", OriginChainID: 11155111, DestinationChainID: 84532})

	candidates, err := s.ReconciliationCandidates(context.Background(), time.Hour)
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	var sawA, sawB bool
	for _, c := range candidates {
		if c.ID == execA.ID {
			sawA = true
		}
		if c.ID == execB.ID {
			sawB = true
		}
	}
	if !sawA {
		t.Fatal("expected the broadcast-unconfirmed row to be a candidate on every tick")
	}
	if sawB {
		t.Fatal("expected the freshly-created, not-yet-broadcast row to NOT be a candidate before it goes stale")
	}

	// Now backdate row B past staleness and confirm it becomes a candidate.
	s.db.ExecContext(context.Background(), `UPDATE payment_executions SET updated_at = now() - interval '1 hour' WHERE id = $1`, execB.ID)
	candidates, err = s.ReconciliationCandidates(context.Background(), time.Minute)
	if err != nil {
		t.Fatalf("candidates after backdate: %v", err)
	}
	sawB = false
	for _, c := range candidates {
		if c.ID == execB.ID {
			sawB = true
		}
	}
	if !sawB {
		t.Fatal("expected the stale not-yet-broadcast row to become a candidate")
	}
}

func TestStaleTestnetProcessingWithoutExecutionIDs(t *testing.T) {
	s := newTestStore(t)
	key := "test-stale-no-exec-key"
	cleanup := func() { s.db.ExecContext(context.Background(), `DELETE FROM payments WHERE idempotency_key = $1`, key) }
	cleanup()
	t.Cleanup(cleanup)

	id := insertRawTestnetPayment(t, s, key)
	s.db.ExecContext(context.Background(), `UPDATE payments SET updated_at = now() - interval '1 hour' WHERE id = $1`, id)

	ids, err := s.StaleTestnetProcessingWithoutExecutionIDs(context.Background(), time.Minute)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	found := false
	for _, got := range ids {
		if got == id {
			found = true
		}
	}
	if !found {
		t.Fatal("expected the stale, execution-less testnet payment to be returned")
	}
}
```

- [ ] **Step 3: Run tests**

```bash
cd go-api && DATABASE_URL="..." go test -tags=integration ./internal/postgres/... -v
```

Expected: PASS, all new and pre-existing tests in this package.

- [ ] **Step 4: Commit**

```bash
git add go-api/internal/postgres/execution_store.go go-api/internal/postgres/execution_store_integration_test.go
git commit -m "Add remaining payment_executions store methods and reconciliation queries"
```

---

### Task 8: `internal/evm` — wallet and RPC client

**Files:**
- Create: `go-api/internal/evm/wallet.go`
- Create: `go-api/internal/evm/client.go`
- Test: `go-api/internal/evm/wallet_test.go`

**Interfaces:**
- Consumes: `github.com/ethereum/go-ethereum` (Task 1).
- Produces: `evm.Wallet{PrivateKey, Address}`, `evm.LoadWallet(hexKey string) (*Wallet, error)`, `(*Wallet).SignTx(tx *types.Transaction, chainID *big.Int) (*types.Transaction, error)`, `evm.Dial(ctx, rpcURL string, expectedChainID int64) (*ethclient.Client, error)` — consumed by Task 11 (execute.go) and Task 16 (cmd/worker wiring).

- [ ] **Step 1: Write the failing test**

```go
package evm

import "testing"

func TestLoadWallet_ValidKey(t *testing.T) {
	// A well-known, publicly-documented go-ethereum test private key --
	// never a real funded key. Address is its deterministic derivation.
	const testKey = "b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f25"
	w, err := LoadWallet(testKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.Address.Hex() != "0x2e988A386a799F506693793c6A5AF6B54dfAaBfB" {
		t.Fatalf("unexpected derived address: %s", w.Address.Hex())
	}
}

func TestLoadWallet_AcceptsHexPrefix(t *testing.T) {
	const testKey = "0xb71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f25"
	w, err := LoadWallet(testKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.Address.Hex() != "0x2e988A386a799F506693793c6A5AF6B54dfAaBfB" {
		t.Fatalf("unexpected derived address: %s", w.Address.Hex())
	}
}

func TestLoadWallet_RejectsInvalidKey(t *testing.T) {
	if _, err := LoadWallet("not-hex"); err == nil {
		t.Fatal("expected an error for a malformed key")
	}
}
```

**Note:** verify the expected address for the sample key above by actually running `crypto.PubkeyToAddress` once during implementation (e.g. a throwaway `go run` snippet) rather than trusting the literal string here — it is illustrative, not independently re-derived during planning; correct it in the test if it doesn't match.

- [ ] **Step 2: Run to verify failure**

Run: `cd go-api && go test ./internal/evm/... -v`
Expected: FAIL, package doesn't exist.

- [ ] **Step 3: Implement `wallet.go`**

```go
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
```

- [ ] **Step 4: Implement `client.go`**

```go
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
```

Note: this deliberately does not print `rpcURL` in any error message beyond what `ethclient.DialContext`'s own wrapped error might already include (out of this function's control) — do not add an explicit `%s` interpolation of `rpcURL` anywhere in this file, since it may embed a provider API key (design spec §21).

- [ ] **Step 5: Run tests**

```bash
cd go-api && go test ./internal/evm/... -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add go-api/internal/evm
git commit -m "Add internal/evm: wallet loading/signing and chain-ID-validated RPC dial"
```

---

### Task 9: `internal/bridge/across` — HTTP client + quote endpoint

**Files:**
- Create: `go-api/internal/bridge/across/client.go`
- Create: `go-api/internal/bridge/across/quote.go`
- Test: `go-api/internal/bridge/across/quote_test.go`

**Interfaces:**
- Produces: `across.Client`, `across.NewClient(baseURL string) *Client`, `(*Client).SuggestedFees(ctx, originChainID, destinationChainID int64, inputToken, outputToken, amount string) (SuggestedFeesResponse, error)` — consumed by Task 11 (execute.go).

- [ ] **Step 1: Implement `client.go`**

```go
package across

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client is a thin HTTP client for the Across testnet API. No auth is sent
// by default -- live testing during Phase 7 planning confirmed the
// testnet API requires neither a Bearer key nor an integratorId, unlike
// the documented mainnet requirement. APIKey/IntegratorID remain wired as
// optional, harmless if the testnet API ever starts requiring them.
type Client struct {
	baseURL    string
	httpClient *http.Client
	APIKey     string
	IntegratorID string
}

func NewClient(baseURL string) *Client {
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{Timeout: 15 * time.Second},
	}
}

func (c *Client) get(ctx context.Context, path string, query url.Values, out any) error {
	if c.IntegratorID != "" {
		query.Set("integratorId", c.IntegratorID)
	}
	u := c.baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return fmt.Errorf("build request for %s: %w", path, err)
	}
	if c.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request %s: %w", path, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response body from %s: %w", path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &APIError{Path: path, StatusCode: resp.StatusCode, Body: string(body)}
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode response from %s: %w", path, err)
	}
	return nil
}

// APIError is a non-2xx response from the Across API, carrying the raw
// body so callers can pattern-match on known error shapes (e.g. status.go's
// DepositNotFoundException handling) without this package hardcoding every
// possible error type.
type APIError struct {
	Path       string
	StatusCode int
	Body       string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("across api %s returned %d: %s", e.Path, e.StatusCode, e.Body)
}
```

- [ ] **Step 2: Write the failing quote test using the REAL captured response as a fixture**

```go
package across

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// realSuggestedFeesFixture is the exact response this plan's own live
// verification against https://testnet.across.to/api/suggested-fees
// captured for the Sepolia WETH -> Base Sepolia WETH route (amount =
// 0.001 WETH in wei). Using a captured real response, not a hand-written
// approximation, means this test would have caught a shape mismatch
// against the actual API.
const realSuggestedFeesFixture = `{"estimatedFillTimeSec":10,"capitalFeePct":"99958333334000","capitalFeeTotal":"99958333334","relayGasFeePct":"981018513630000","relayGasFeeTotal":"981018513630","relayFeePct":"2407827669767028","relayFeeTotal":"2407827669767","lpFeePct":"0","timestamp":"1789340112","isAmountTooLow":false,"quoteBlock":"11698950","exclusiveRelayer":"0x0000000000000000000000000000000000000000","exclusivityDeadline":0,"spokePoolAddress":"0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662","destinationSpokePoolAddress":"0x82B564983aE7274c86695917BBf8C99ECb6F0F8F","totalRelayFee":{"pct":"2407827669767028","total":"2407827669767"},"relayerCapitalFee":{"pct":"99958333334000","total":"99958333334"},"relayerGasFee":{"pct":"981018513630000","total":"981018513630"},"lpFee":{"pct":"1326850822803028","total":"1326850822803"},"internalizedSwapFee":{"pct":"0","total":"0"},"limits":{"minDeposit":"3925643657709","maxDeposit":"3231735644024318","maxDepositInstant":"3231735644024318","maxDepositShortDelay":"3231735644024318","recommendedDepositInstant":"3231735644024318"},"fillDeadline":"1789347312","outputAmount":"997592172330233","inputToken":{"address":"0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14","symbol":"WETH","decimals":18,"chainId":11155111},"outputToken":{"address":"0x4200000000000000000000000000000000000006","symbol":"WETH","decimals":18,"chainId":84532},"id":"6dr22-1789340515991-1eb23d940902"}`

func TestSuggestedFees_ParsesRealCapturedResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/suggested-fees" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(realSuggestedFeesFixture))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	resp, err := c.SuggestedFees(context.Background(), 11155111, 84532,
		"0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14", "0x4200000000000000000000000000000000000006", "1000000000000000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.OutputAmount != "997592172330233" {
		t.Fatalf("unexpected OutputAmount: %s", resp.OutputAmount)
	}
	if resp.SpokePoolAddress != "0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662" {
		t.Fatalf("unexpected SpokePoolAddress: %s", resp.SpokePoolAddress)
	}
	if resp.ExclusiveRelayer != "0x0000000000000000000000000000000000000000" {
		t.Fatalf("unexpected ExclusiveRelayer: %s", resp.ExclusiveRelayer)
	}
	if resp.IsAmountTooLow {
		t.Fatal("expected IsAmountTooLow=false")
	}
}

func TestSuggestedFees_RejectsAmountTooLow(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"isAmountTooLow":true,"outputAmount":"0","fillDeadline":"0","exclusivityDeadline":0,"exclusiveRelayer":"0x0","timestamp":"0","spokePoolAddress":"0x0"}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	_, err := c.SuggestedFees(context.Background(), 11155111, 84532, "0xin", "0xout", "1")
	if err == nil {
		t.Fatal("expected an error when the API reports isAmountTooLow")
	}
}

func TestSuggestedFees_PropagatesAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"InvalidParamError","message":"bad input"}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	_, err := c.SuggestedFees(context.Background(), 1, 2, "a", "b", "1")
	if err == nil {
		t.Fatal("expected an error for a non-2xx response")
	}
}
```

- [ ] **Step 3: Run to verify failure, then implement `quote.go`**

```go
package across

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
)

// SuggestedFeesResponse is the subset of GET /suggested-fees's response
// this system actually uses to construct a depositV3 call. Field names
// and JSON tags are taken directly from a live captured response (see
// quote_test.go's realSuggestedFeesFixture), not guessed from docs alone.
type SuggestedFeesResponse struct {
	OutputAmount        string `json:"outputAmount"`
	FillDeadline        string `json:"fillDeadline"`
	ExclusivityDeadline int64  `json:"exclusivityDeadline"`
	ExclusiveRelayer    string `json:"exclusiveRelayer"`
	Timestamp           string `json:"timestamp"`
	SpokePoolAddress    string `json:"spokePoolAddress"`
	IsAmountTooLow      bool   `json:"isAmountTooLow"`
}

// SuggestedFees calls GET /suggested-fees. Per the Across docs (confirmed
// during Phase 7 planning), this response must NEVER be cached -- fees are
// market/gas/utilization-dependent and can change between calls, so every
// execution attempt must call this fresh, immediately before constructing
// the depositV3 transaction.
func (c *Client) SuggestedFees(ctx context.Context, originChainID, destinationChainID int64, inputToken, outputToken, amount string) (SuggestedFeesResponse, error) {
	q := url.Values{}
	q.Set("originChainId", strconv.FormatInt(originChainID, 10))
	q.Set("destinationChainId", strconv.FormatInt(destinationChainID, 10))
	q.Set("inputToken", inputToken)
	q.Set("outputToken", outputToken)
	q.Set("amount", amount)

	var out SuggestedFeesResponse
	if err := c.get(ctx, "/suggested-fees", q, &out); err != nil {
		return SuggestedFeesResponse{}, err
	}
	if out.IsAmountTooLow {
		return SuggestedFeesResponse{}, fmt.Errorf("across reports this amount is too low for the Sepolia -> Base Sepolia WETH route")
	}
	return out, nil
}
```

- [ ] **Step 4: Run tests**

```bash
cd go-api && go test ./internal/bridge/across/... -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add go-api/internal/bridge/across/client.go go-api/internal/bridge/across/quote.go go-api/internal/bridge/across/quote_test.go
git commit -m "Add across.Client and GET /suggested-fees, tested against a real captured response"
```

---

### Task 10: `internal/bridge/across` — deposit status endpoint

**Files:**
- Create: `go-api/internal/bridge/across/status.go`
- Test: `go-api/internal/bridge/across/status_test.go`

**Interfaces:**
- Consumes: `across.Client` (Task 9).
- Produces: `across.DepositStatusResponse`, `across.ErrDepositNotFound`, `(*Client).DepositStatusByTxHash(ctx, originChainID int64, depositTxHash string) (DepositStatusResponse, error)` — consumed by Task 13 (reconciler).

- [ ] **Step 1: Confirm the success-response shape**

Before writing this file, check Task 1's findings doc (`docs/superpowers/reports/2026-09-13-across-verification-findings.md`) for the `/deposit/status` success-response shape it recorded. If it found a confirmed `status` enum spelling (e.g. exactly `"filled"`, `"pending"`, `"expired"`, `"refunded"`), use it. If it remains unconfirmed, implement tolerant parsing per Step 3 below (unrecognized status strings must not crash the reconciler).

- [ ] **Step 2: Write the failing tests, using the REAL captured error shapes as fixtures**

```go
package across

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// realDepositNotFoundFixture and realIncorrectParamsFixture are the exact
// error bodies this plan's live verification captured from
// https://testnet.across.to/api/deposit/status.
const realDepositNotFoundFixture = `{"error":"DepositNotFoundException","message":"Deposit not found given the provided constraints"}`

func TestDepositStatusByTxHash_NotFoundMapsToSentinelError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("depositTxHash"); got != "0xabc" {
			t.Fatalf("expected depositTxHash=0xabc, got %q", got)
		}
		if got := r.URL.Query().Get("originChainId"); got != "11155111" {
			t.Fatalf("expected originChainId=11155111, got %q", got)
		}
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(realDepositNotFoundFixture))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	_, err := c.DepositStatusByTxHash(context.Background(), 11155111, "0xabc")
	if !errors.Is(err, ErrDepositNotFound) {
		t.Fatalf("expected ErrDepositNotFound, got %v", err)
	}
}

func TestDepositStatusByTxHash_ParsesFilledResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"filled"}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	resp, err := c.DepositStatusByTxHash(context.Background(), 11155111, "0xabc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != "filled" {
		t.Fatalf("expected status=filled, got %q", resp.Status)
	}
}

func TestDepositStatusByTxHash_UnrecognizedStatusDoesNotError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"some-future-status-this-client-has-never-seen"}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	resp, err := c.DepositStatusByTxHash(context.Background(), 11155111, "0xabc")
	if err != nil {
		t.Fatalf("an unrecognized status string must parse successfully (the reconciler decides what to do with it), got error: %v", err)
	}
	if resp.Status != "some-future-status-this-client-has-never-seen" {
		t.Fatalf("expected the raw status string to be preserved, got %q", resp.Status)
	}
}
```

- [ ] **Step 3: Implement `status.go`**

```go
package across

import (
	"context"
	"errors"
	"net/url"
	"strconv"
	"strings"
)

// DepositStatusResponse is GET /deposit/status's response. Status is left
// as a raw string (not an enum) deliberately: an unrecognized value here
// must never crash the reconciler (design spec §13 -- only a definitive,
// recognized terminal signal may ever produce FAILED). The reconciler
// (Task 13) is responsible for interpreting Status against the known set
// {pending, filled, expired, refunded} and treating anything else as
// "still uncertain, retry later," logging a warning.
type DepositStatusResponse struct {
	Status string `json:"status"`
}

// ErrDepositNotFound is returned when Across reports
// DepositNotFoundException -- expected immediately after broadcast, before
// Across's indexer has observed the deposit yet, not itself a failure
// signal (design spec §13).
var ErrDepositNotFound = errors.New("across: deposit not found")

// DepositStatusByTxHash calls GET /deposit/status?originChainId=...&depositTxHash=...
// -- using our own persisted signed_tx_hash directly, confirmed as a valid
// query parameter during Phase 7 planning's live verification, avoiding
// any need to parse a FundsDeposited event out of the origin receipt to
// extract a numeric depositId.
func (c *Client) DepositStatusByTxHash(ctx context.Context, originChainID int64, depositTxHash string) (DepositStatusResponse, error) {
	q := url.Values{}
	q.Set("originChainId", strconv.FormatInt(originChainID, 10))
	q.Set("depositTxHash", depositTxHash)

	var out DepositStatusResponse
	err := c.get(ctx, "/deposit/status", q, &out)
	if err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && strings.Contains(apiErr.Body, "DepositNotFoundException") {
			return DepositStatusResponse{}, ErrDepositNotFound
		}
		return DepositStatusResponse{}, err
	}
	return out, nil
}
```

- [ ] **Step 4: Run tests**

```bash
cd go-api && go test ./internal/bridge/across/... -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add go-api/internal/bridge/across/status.go go-api/internal/bridge/across/status_test.go
git commit -m "Add across deposit-status polling by tx hash, tested against real captured error shapes"
```

---

### Task 11: `internal/bridge/across` — `depositV3` construction and signing

**Files:**
- Create: `go-api/internal/bridge/across/execute.go`
- Test: `go-api/internal/bridge/across/execute_test.go`

**Interfaces:**
- Consumes: `money.DecimalToBaseUnits` (Task 2), `evm.Wallet` (Task 8), `across.SuggestedFeesResponse` (Task 9).
- Produces: `across.DepositV3Params`, `across.BuildAndSignDepositV3Tx(ctx, client EthClient, wallet *evm.Wallet, originChainID int64, spokePool, wethAddress common.Address, nonce uint64, params DepositV3Params) (*types.Transaction, error)` — consumed by Task 12 (executor).

- [ ] **Step 1: Write the failing test — deterministic calldata against a fixed fixture**

```go
package across

import (
	"context"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"chainroute/go-api/internal/evm"
)

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
	const testKey = "b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f25"
	wallet, err := evm.LoadWallet(testKey)
	if err != nil {
		t.Fatalf("load wallet: %v", err)
	}

	client := &fakeEthClient{gasPrice: big.NewInt(1_000_000_000), gasLimit: 300_000}
	spokePool := common.HexToAddress("0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662")
	weth := common.HexToAddress("0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14")

	inputAmount, _ := new(big.Int).SetString("1000000000000000", 10) // 0.001 WETH

	params := DepositV3Params{
		Recipient:            wallet.Address,
		InputToken:           weth,
		OutputToken:          common.HexToAddress("0x4200000000000000000000000000000000000006"),
		InputAmount:          inputAmount,
		OutputAmount:         mustBigInt(t, "997592172330233"),
		DestinationChainID:   big.NewInt(84532),
		ExclusiveRelayer:     common.HexToAddress("0x0000000000000000000000000000000000000000"),
		QuoteTimestamp:       1789340112,
		FillDeadline:         1789347312,
		ExclusivityDeadline:  0,
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
}

func mustBigInt(t *testing.T, s string) *big.Int {
	t.Helper()
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		t.Fatalf("invalid big.Int literal: %s", s)
	}
	return n
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd go-api && go test ./internal/bridge/across/... -run TestBuildAndSignDepositV3Tx -v`
Expected: FAIL, `BuildAndSignDepositV3Tx`/`DepositV3Params`/`ethereumCallMsg` undefined.

- [ ] **Step 3: Implement `execute.go`**

```go
package across

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"chainroute/go-api/internal/evm"
)

// ethereumCallMsg is a local alias so this file's public EthClient
// interface doesn't leak the go-ethereum import path into every caller's
// type signature verbatim -- purely a readability convenience.
type ethereumCallMsg = ethereum.CallMsg

// EthClient is the minimal ethclient.Client surface this file needs,
// small enough to fake in tests without a real RPC endpoint.
type EthClient interface {
	SuggestGasPrice(ctx context.Context) (*big.Int, error)
	EstimateGas(ctx context.Context, msg ethereumCallMsg) (uint64, error)
}

// spokePoolDepositV3ABI is the exact depositV3 function fragment pulled
// directly from across-protocol/contracts' deployed ABI (verified live
// during Phase 7 planning -- see this plan's "Verified Across testnet
// facts" section, and Task 1's findings doc for any drift since).
const spokePoolDepositV3ABI = `[{"inputs":[{"internalType":"address","name":"depositor","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"address","name":"inputToken","type":"address"},{"internalType":"address","name":"outputToken","type":"address"},{"internalType":"uint256","name":"inputAmount","type":"uint256"},{"internalType":"uint256","name":"outputAmount","type":"uint256"},{"internalType":"uint256","name":"destinationChainId","type":"uint256"},{"internalType":"address","name":"exclusiveRelayer","type":"address"},{"internalType":"uint32","name":"quoteTimestamp","type":"uint32"},{"internalType":"uint32","name":"fillDeadline","type":"uint32"},{"internalType":"uint32","name":"exclusivityDeadline","type":"uint32"},{"internalType":"bytes","name":"message","type":"bytes"}],"name":"depositV3","outputs":[],"stateMutability":"payable","type":"function"}]`

var spokePoolABI = func() abi.ABI {
	parsed, err := abi.JSON(strings.NewReader(spokePoolDepositV3ABI))
	if err != nil {
		panic(fmt.Sprintf("across: invalid embedded SpokePool ABI: %v", err))
	}
	return parsed
}()

// DepositV3Params is everything BuildAndSignDepositV3Tx needs beyond the
// signer's own address (used as both depositor and recipient -- Phase 7
// does not implement separate recipient management) and the SpokePool
// address. InputAmount MUST come from money.DecimalToBaseUnits, never a
// float conversion. OutputAmount/ExclusiveRelayer/QuoteTimestamp/
// FillDeadline/ExclusivityDeadline come directly from a freshly-fetched
// SuggestedFeesResponse (design spec §21: an unvalidated external
// response never flows directly into a signing call -- the caller is
// responsible for the chain-ID/token-address validation described there
// before populating this struct).
type DepositV3Params struct {
	Recipient            common.Address
	InputToken           common.Address
	OutputToken          common.Address
	InputAmount          *big.Int
	OutputAmount         *big.Int
	DestinationChainID   *big.Int
	ExclusiveRelayer     common.Address
	QuoteTimestamp       uint32
	FillDeadline         uint32
	ExclusivityDeadline  uint32
}

// BuildAndSignDepositV3Tx constructs and signs a depositV3 call as a
// native-ETH deposit: msg.value = params.InputAmount, params.InputToken
// set to the WETH address, and NO separate approve() transaction --
// depositV3 auto-wraps native ETH when InputToken is the chain's
// wrapped-native address (confirmed live during Phase 7 planning; see
// this plan's "Verified Across testnet facts"). This deliberately avoids
// a two-step approve()+depositV3 flow, which would consume two nonces per
// payment and break the one-execution-row-one-nonce design.
//
// nonce MUST be the value already durably allocated and persisted in the
// payment's payment_executions row (design spec §8) -- this function
// never allocates a nonce itself. The returned transaction is signed but
// NOT broadcast; the caller (Task 12/13) is responsible for persisting
// its raw bytes and hash BEFORE attempting to broadcast it.
func BuildAndSignDepositV3Tx(ctx context.Context, client EthClient, wallet *evm.Wallet, originChainID int64, spokePool common.Address, nonce uint64, params DepositV3Params) (*types.Transaction, error) {
	calldata, err := spokePoolABI.Pack("depositV3",
		wallet.Address, params.Recipient, params.InputToken, params.OutputToken,
		params.InputAmount, params.OutputAmount, params.DestinationChainID,
		params.ExclusiveRelayer, params.QuoteTimestamp, params.FillDeadline, params.ExclusivityDeadline,
		[]byte{},
	)
	if err != nil {
		return nil, fmt.Errorf("pack depositV3 calldata: %w", err)
	}

	gasPrice, err := client.SuggestGasPrice(ctx)
	if err != nil {
		return nil, fmt.Errorf("suggest gas price: %w", err)
	}

	// A conservative fallback gas limit if estimation fails (e.g. the RPC
	// endpoint doesn't support eth_estimateGas against a not-yet-mined
	// state). SpokePool deposit calls typically cost well under this on
	// Sepolia/Base Sepolia; this is a safety ceiling, not a tuned value.
	const fallbackGasLimit = 500_000
	gasLimit, err := client.EstimateGas(ctx, ethereum.CallMsg{
		From:  wallet.Address,
		To:    &spokePool,
		Value: params.InputAmount,
		Data:  calldata,
	})
	if err != nil {
		gasLimit = fallbackGasLimit
	}

	unsignedTx := types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		To:       &spokePool,
		Value:    params.InputAmount,
		Gas:      gasLimit,
		GasPrice: gasPrice,
		Data:     calldata,
	})

	signed, err := wallet.SignTx(unsignedTx, big.NewInt(originChainID))
	if err != nil {
		return nil, fmt.Errorf("sign depositV3 tx: %w", err)
	}
	return signed, nil
}

var _ = strconv.Itoa // placeholder import guard removed once unused import is resolved during implementation
```

Remove the trailing `var _ = strconv.Itoa` line and the now-unused `strconv` import once the implementer confirms the file compiles without it — it is included here only as a marker that this file should NOT end up with an unused-import build failure; delete both before running Step 4.

- [ ] **Step 4: Run tests**

```bash
cd go-api && go test ./internal/bridge/across/... -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add go-api/internal/bridge/across/execute.go go-api/internal/bridge/across/execute_test.go
git commit -m "Add depositV3 construction and signing via the native-ETH auto-wrap path"
```

---

### Task 12: `worker/executor.go` — the execution lifecycle

**Files:**
- Modify: `go-api/internal/bridge/across/quote.go` (extend `SuggestedFeesResponse` — see Step 1)
- Create: `go-api/internal/worker/executor.go`
- Test: `go-api/internal/worker/executor_test.go`

**Interfaces:**
- Consumes: `postgres.CreateExecutionParams`, `Store.TryCreateExecution/GetPayment/PersistSignedExecution/MarkExecutionBroadcast/MarkSubmitted` (Tasks 5-7); `evm.Wallet` (Task 8); `across.Client/SuggestedFees/BuildAndSignDepositV3Tx` (Tasks 9, 11); `money.DecimalToBaseUnits` (Task 2).
- Produces: `worker.Executor`, `(*Executor).ExecuteTestnetPayment(ctx, paymentID string) error`, `(*Executor).DriveExecutionForward(ctx, exec payment.Execution) error` — the second is consumed directly by Task 13 (reconciler), which resumes stale rows through the identical code path the executor itself uses.

- [ ] **Step 1: Extend `SuggestedFeesResponse` (from Task 9) with token-echo fields for validation**

The design spec §21 requires validating an Across quote (chain IDs match the request, token addresses match the expected WETH addresses) before it ever flows into a signing call. Task 9's `SuggestedFeesResponse` didn't need these fields yet; add them now in `go-api/internal/bridge/across/quote.go`:

```go
type SuggestedFeesResponse struct {
	OutputAmount        string          `json:"outputAmount"`
	FillDeadline        string          `json:"fillDeadline"`
	ExclusivityDeadline int64           `json:"exclusivityDeadline"`
	ExclusiveRelayer    string          `json:"exclusiveRelayer"`
	Timestamp           string          `json:"timestamp"`
	SpokePoolAddress    string          `json:"spokePoolAddress"`
	IsAmountTooLow      bool            `json:"isAmountTooLow"`
	InputToken          QuoteTokenInfo  `json:"inputToken"`
	OutputToken         QuoteTokenInfo  `json:"outputToken"`
}

// QuoteTokenInfo echoes the token/chain the quote was actually computed
// for -- used to validate the response matches what was requested before
// any of it flows into a signing call (design spec §21).
type QuoteTokenInfo struct {
	Address string `json:"address"`
	ChainID int64  `json:"chainId"`
	Decimals int   `json:"decimals"`
}
```

Re-run `go test ./internal/bridge/across/... -v` — Task 9's existing test asserts on specific fields only (not full struct equality via `reflect.DeepEqual`), so it must still pass unmodified; if it doesn't, the test was written more strictly than intended and should be fixed to match Task 9's own stated assertion style, not worked around.

- [ ] **Step 2: Write the failing executor tests**

```go
package worker

import (
	"context"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	gethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"chainroute/go-api/internal/bridge/across"
	"chainroute/go-api/internal/evm"
	"chainroute/go-api/internal/payment"
	"chainroute/go-api/internal/postgres"
)

const testExecutorKey = "b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f25"

type fakeExecutorStore struct {
	tryCreateExec    payment.Execution
	tryCreateCreated bool
	tryCreateErr     error
	pmt              payment.Payment
	pmtFound         bool
	persistedRawTx   []byte
	persistedHash    string
	broadcastCalled  bool
	submittedCalled  bool
}

func (f *fakeExecutorStore) TryCreateExecution(ctx context.Context, p postgres.CreateExecutionParams) (payment.Execution, bool, error) {
	return f.tryCreateExec, f.tryCreateCreated, f.tryCreateErr
}
func (f *fakeExecutorStore) GetPayment(ctx context.Context, id string) (payment.Payment, bool, error) {
	return f.pmt, f.pmtFound, nil
}
func (f *fakeExecutorStore) PersistSignedExecution(ctx context.Context, executionID string, rawTx []byte, txHash string) error {
	f.persistedRawTx, f.persistedHash = rawTx, txHash
	return nil
}
func (f *fakeExecutorStore) MarkExecutionBroadcast(ctx context.Context, executionID string) error {
	f.broadcastCalled = true
	return nil
}
func (f *fakeExecutorStore) MarkSubmitted(ctx context.Context, paymentID string) (bool, error) {
	f.submittedCalled = true
	return true, nil
}

type fakeExecutorEthClient struct {
	sendCalled       bool
	sendErr          error
	txByHashFound    bool
	txByHashErr      error
}

func (f *fakeExecutorEthClient) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	return big.NewInt(1_000_000_000), nil
}
func (f *fakeExecutorEthClient) EstimateGas(ctx context.Context, msg gethereum.CallMsg) (uint64, error) {
	return 300_000, nil
}
func (f *fakeExecutorEthClient) SendTransaction(ctx context.Context, tx *types.Transaction) error {
	f.sendCalled = true
	return f.sendErr
}
func (f *fakeExecutorEthClient) TransactionByHash(ctx context.Context, hash common.Hash) (*types.Transaction, bool, error) {
	if f.txByHashFound {
		return &types.Transaction{}, true, nil
	}
	return nil, false, f.txByHashErr
}

func newTestAcrossServer(t *testing.T) *across.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"outputAmount":"997592172330233","fillDeadline":"1789347312","exclusivityDeadline":0,"exclusiveRelayer":"0x0000000000000000000000000000000000000000","timestamp":"1789340112","spokePoolAddress":"0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662","isAmountTooLow":false,"inputToken":{"address":"0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14","chainId":11155111,"decimals":18},"outputToken":{"address":"0x4200000000000000000000000000000000000006","chainId":84532,"decimals":18}}`))
	}))
	t.Cleanup(srv.Close)
	return across.NewClient(srv.URL)
}

func newTestExecutor(t *testing.T, store *fakeExecutorStore, ethClient *fakeExecutorEthClient) *Executor {
	t.Helper()
	wallet, err := evm.LoadWallet(testExecutorKey)
	if err != nil {
		t.Fatalf("load wallet: %v", err)
	}
	return &Executor{
		Store: store, Wallet: wallet, OriginClient: ethClient, Across: newTestAcrossServer(t),
		BridgeProvider: "across", OriginChainID: 11155111, DestChainID: 84532,
		SpokePoolAddress: common.HexToAddress("0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662"),
		WETHOrigin:       common.HexToAddress("0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14"),
		WETHDestination:  common.HexToAddress("0x4200000000000000000000000000000000000006"),
	}
}

func TestExecuteTestnetPayment_HappyPath(t *testing.T) {
	store := &fakeExecutorStore{
		tryCreateCreated: true,
		tryCreateExec:    payment.Execution{ID: "exec-1", PaymentID: "pay-1", Nonce: 3},
		pmt:              payment.Payment{ID: "pay-1", Amount: "0.001"},
		pmtFound:         true,
	}
	ethClient := &fakeExecutorEthClient{txByHashFound: false, txByHashErr: errors.New("not found")}
	e := newTestExecutor(t, store, ethClient)

	if err := e.ExecuteTestnetPayment(context.Background(), "pay-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store.persistedHash == "" {
		t.Fatal("expected the signed tx hash to be persisted before broadcast")
	}
	if !ethClient.sendCalled {
		t.Fatal("expected SendTransaction to be called")
	}
	if !store.broadcastCalled {
		t.Fatal("expected MarkExecutionBroadcast to be called")
	}
	if !store.submittedCalled {
		t.Fatal("expected MarkSubmitted to be called")
	}
}

func TestExecuteTestnetPayment_LostRaceIsSafeNoOp(t *testing.T) {
	store := &fakeExecutorStore{tryCreateCreated: false}
	ethClient := &fakeExecutorEthClient{}
	e := newTestExecutor(t, store, ethClient)

	if err := e.ExecuteTestnetPayment(context.Background(), "pay-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ethClient.sendCalled {
		t.Fatal("must never sign/broadcast when TryCreateExecution reports it lost the race")
	}
}

func TestDriveExecutionForward_AmbiguousBroadcastFoundOnChainNeverResends(t *testing.T) {
	hash := "0x" + "ab" + "00"[:0] + "cd0000000000000000000000000000000000000000000000000000000000"
	store := &fakeExecutorStore{}
	ethClient := &fakeExecutorEthClient{txByHashFound: true} // already known to the chain
	e := newTestExecutor(t, store, ethClient)

	exec := payment.Execution{
		ID: "exec-2", PaymentID: "pay-2", Nonce: 1,
		SignedTxHash: &hash, RawSignedTx: []byte{0xde, 0xad},
	}
	if err := e.DriveExecutionForward(context.Background(), exec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ethClient.sendCalled {
		t.Fatal("must never rebroadcast when the hash is already found on-chain")
	}
	if !store.broadcastCalled {
		t.Fatal("expected MarkExecutionBroadcast even on the found-on-chain path -- it is now confirmed broadcast, whoever sent it")
	}
}
```

**Note on the `hash` construction above**: write a real, syntactically valid 32-byte hex string literal directly (e.g. `"0xabcd000000000000000000000000000000000000000000000000000000000000"` trimmed to exactly 66 characters) rather than the placeholder concatenation shown — fix this during implementation; the exact hash value is immaterial to the test, only its validity as a hex string matters for `common.HexToHash` not to panic.

- [ ] **Step 3: Run to verify failure**

Run: `cd go-api && go test ./internal/worker/... -run 'TestExecuteTestnetPayment|TestDriveExecutionForward' -v`
Expected: FAIL, `Executor` undefined.

- [ ] **Step 4: Implement `executor.go`**

```go
package worker

import (
	"context"
	"fmt"
	"math/big"
	"strconv"

	gethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"chainroute/go-api/internal/bridge/across"
	"chainroute/go-api/internal/evm"
	"chainroute/go-api/internal/money"
	"chainroute/go-api/internal/payment"
	"chainroute/go-api/internal/postgres"
)

const wethDecimals = 18

// ExecutorStore is the subset of *postgres.Store the executor needs.
type ExecutorStore interface {
	TryCreateExecution(ctx context.Context, p postgres.CreateExecutionParams) (payment.Execution, bool, error)
	GetPayment(ctx context.Context, id string) (payment.Payment, bool, error)
	PersistSignedExecution(ctx context.Context, executionID string, rawTx []byte, txHash string) error
	MarkExecutionBroadcast(ctx context.Context, executionID string) error
	MarkSubmitted(ctx context.Context, paymentID string) (bool, error)
}

// ExecutorEthClient is the minimal ethclient.Client surface the executor
// needs against the ORIGIN chain (Sepolia) -- across.EthClient's
// SuggestGasPrice/EstimateGas plus the broadcast/lookup calls this file
// itself needs. *ethclient.Client satisfies both interfaces structurally.
type ExecutorEthClient interface {
	across.EthClient
	SendTransaction(ctx context.Context, tx *types.Transaction) error
	TransactionByHash(ctx context.Context, hash common.Hash) (tx *types.Transaction, isPending bool, err error)
}

// Executor drives one testnet-mode payment from a claimed PROCESSING state
// through to a broadcast, SUBMITTED transaction. It is deliberately the
// same code path whether starting fresh (ExecuteTestnetPayment) or
// resuming an existing execution row after a crash or via the reconciler
// (DriveExecutionForward directly) -- design spec §15.
type Executor struct {
	Store            ExecutorStore
	Wallet           *evm.Wallet
	OriginClient     ExecutorEthClient
	Across           *across.Client
	BridgeProvider   string
	OriginChainID    int64
	DestChainID      int64
	SpokePoolAddress common.Address
	WETHOrigin       common.Address
	WETHDestination  common.Address
	MaxAmountWei     *big.Int
}

// ExecuteTestnetPayment claims an execution identity for paymentID (via
// TryCreateExecution's UNIQUE(payment_id)-arbitrated race, design spec
// §15) and, only if this call actually won that race, drives it forward.
// A lost race (created=false) is a safe no-op: some other actor -- the
// reconciler, on a concurrent tick -- already owns this payment's
// execution.
func (e *Executor) ExecuteTestnetPayment(ctx context.Context, paymentID string) error {
	exec, created, err := e.Store.TryCreateExecution(ctx, postgres.CreateExecutionParams{
		PaymentID: paymentID, WalletAddress: e.Wallet.Address.Hex(), BridgeProvider: e.BridgeProvider,
		OriginChainID: e.OriginChainID, DestinationChainID: e.DestChainID,
	})
	if err != nil {
		return fmt.Errorf("try create execution for payment %s: %w", paymentID, err)
	}
	if !created {
		return nil
	}
	return e.DriveExecutionForward(ctx, exec)
}

// DriveExecutionForward advances exec to broadcast + SUBMITTED, picking up
// from wherever it currently is: signs if unsigned (crash point B),
// broadcasts with ambiguous-outcome recovery if unbroadcast (crash point
// C/D/E/F), then marks the payment SUBMITTED. This is exactly what makes
// it safe for the reconciler to call this same method on a resumed row
// (design spec §15) -- there is no separate "resume" code path to drift
// out of sync with the fresh-execution path.
func (e *Executor) DriveExecutionForward(ctx context.Context, exec payment.Execution) error {
	if exec.SignedTxHash == nil {
		if err := e.signAndPersist(ctx, &exec); err != nil {
			return fmt.Errorf("sign execution %s: %w", exec.ID, err)
		}
	}

	if err := e.broadcastWithRecovery(ctx, exec); err != nil {
		return fmt.Errorf("broadcast execution %s: %w", exec.ID, err)
	}
	if err := e.Store.MarkExecutionBroadcast(ctx, exec.ID); err != nil {
		return fmt.Errorf("mark execution %s broadcast: %w", exec.ID, err)
	}
	if _, err := e.Store.MarkSubmitted(ctx, exec.PaymentID); err != nil {
		return fmt.Errorf("mark payment %s submitted: %w", exec.PaymentID, err)
	}
	return nil
}

func (e *Executor) signAndPersist(ctx context.Context, exec *payment.Execution) error {
	p, found, err := e.Store.GetPayment(ctx, exec.PaymentID)
	if err != nil {
		return fmt.Errorf("get payment: %w", err)
	}
	if !found {
		return fmt.Errorf("payment %s not found", exec.PaymentID)
	}

	inputAmount, err := money.DecimalToBaseUnits(p.Amount, wethDecimals)
	if err != nil {
		return fmt.Errorf("convert amount %q: %w", p.Amount, err)
	}
	if e.MaxAmountWei != nil && inputAmount.Cmp(e.MaxAmountWei) > 0 {
		return fmt.Errorf("amount exceeds MAX_TESTNET_AMOUNT_WEI guardrail")
	}

	quote, err := e.Across.SuggestedFees(ctx, e.OriginChainID, e.DestChainID,
		e.WETHOrigin.Hex(), e.WETHDestination.Hex(), inputAmount.String())
	if err != nil {
		return fmt.Errorf("get quote: %w", err)
	}
	if err := e.validateQuote(quote); err != nil {
		return fmt.Errorf("reject quote: %w", err)
	}

	outputAmount, ok := new(big.Int).SetString(quote.OutputAmount, 10)
	if !ok {
		return fmt.Errorf("across returned a non-integer outputAmount %q", quote.OutputAmount)
	}
	quoteTimestamp, err := strconv.ParseUint(quote.Timestamp, 10, 32)
	if err != nil {
		return fmt.Errorf("parse quote timestamp %q: %w", quote.Timestamp, err)
	}
	fillDeadline, err := strconv.ParseUint(quote.FillDeadline, 10, 32)
	if err != nil {
		return fmt.Errorf("parse fill deadline %q: %w", quote.FillDeadline, err)
	}

	signedTx, err := across.BuildAndSignDepositV3Tx(ctx, e.OriginClient, e.Wallet, e.OriginChainID, e.SpokePoolAddress, uint64(exec.Nonce), across.DepositV3Params{
		Recipient: e.Wallet.Address, InputToken: e.WETHOrigin, OutputToken: e.WETHDestination,
		InputAmount: inputAmount, OutputAmount: outputAmount, DestinationChainID: big.NewInt(e.DestChainID),
		ExclusiveRelayer:    common.HexToAddress(quote.ExclusiveRelayer),
		QuoteTimestamp:      uint32(quoteTimestamp),
		FillDeadline:        uint32(fillDeadline),
		ExclusivityDeadline: uint32(quote.ExclusivityDeadline),
	})
	if err != nil {
		return fmt.Errorf("build/sign tx: %w", err)
	}

	rawTx, err := signedTx.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal signed tx: %w", err)
	}
	hash := signedTx.Hash().Hex()
	if err := e.Store.PersistSignedExecution(ctx, exec.ID, rawTx, hash); err != nil {
		return fmt.Errorf("persist signed execution: %w", err)
	}
	exec.SignedTxHash = &hash
	exec.RawSignedTx = rawTx
	return nil
}

// validateQuote rejects an Across response before it can ever reach a
// signing call (design spec §21): the quote's own echoed chain IDs and
// token addresses must match what was requested.
func (e *Executor) validateQuote(q across.SuggestedFeesResponse) error {
	if q.InputToken.ChainID != e.OriginChainID {
		return fmt.Errorf("quote inputToken.chainId %d does not match origin chain %d", q.InputToken.ChainID, e.OriginChainID)
	}
	if q.OutputToken.ChainID != e.DestChainID {
		return fmt.Errorf("quote outputToken.chainId %d does not match destination chain %d", q.OutputToken.ChainID, e.DestChainID)
	}
	if common.HexToAddress(q.InputToken.Address) != e.WETHOrigin {
		return fmt.Errorf("quote inputToken.address %s does not match expected origin WETH %s", q.InputToken.Address, e.WETHOrigin.Hex())
	}
	if common.HexToAddress(q.OutputToken.Address) != e.WETHDestination {
		return fmt.Errorf("quote outputToken.address %s does not match expected destination WETH %s", q.OutputToken.Address, e.WETHDestination.Hex())
	}
	if _, ok := new(big.Int).SetString(q.OutputAmount, 10); !ok {
		return fmt.Errorf("quote outputAmount %q is not a valid integer", q.OutputAmount)
	}
	return nil
}

// broadcastWithRecovery implements design spec §8 step 4 / §14: on ANY
// attempt (fresh or resumed), the FIRST action is always a read -- check
// the chain for the precomputed signed_tx_hash. Only if that read
// confirms the transaction does not exist does this rebroadcast, and even
// then it rebroadcasts the exact persisted bytes, never a re-signed one.
func (e *Executor) broadcastWithRecovery(ctx context.Context, exec payment.Execution) error {
	if exec.SignedTxHash == nil || exec.RawSignedTx == nil {
		return fmt.Errorf("execution %s has no signed transaction to broadcast", exec.ID)
	}
	hash := common.HexToHash(*exec.SignedTxHash)

	if _, _, err := e.OriginClient.TransactionByHash(ctx, hash); err == nil {
		// Already known to the chain, pending or mined -- never
		// rebroadcast or re-sign.
		return nil
	}

	var tx types.Transaction
	if err := tx.UnmarshalBinary(exec.RawSignedTx); err != nil {
		return fmt.Errorf("unmarshal persisted signed tx: %w", err)
	}
	if err := e.OriginClient.SendTransaction(ctx, &tx); err != nil {
		// This error is itself ambiguous -- the node may have accepted
		// the tx before the error surfaced. Do NOT treat this as
		// terminal; leave broadcast_at unset so the next attempt
		// re-runs this exact hash-check-then-broadcast sequence.
		return fmt.Errorf("send transaction: %w", err)
	}
	return nil
}

var _ = gethereum.CallMsg{} // ensures the gethereum import stays referenced if EstimateGas's signature is inlined differently; remove if unused after implementation.
```

Remove the trailing `var _ = gethereum.CallMsg{}` guard line if `go vet`/the compiler reports it as an unused import once the file is complete — it is a placeholder reminder for the implementer, not intended to ship.

- [ ] **Step 5: Run tests**

```bash
cd go-api && go build ./... && go test ./internal/worker/... -run 'TestExecuteTestnetPayment|TestDriveExecutionForward' -v
```

Expected: PASS.

- [ ] **Step 6: Mutation check — signed-before-broadcast ordering and no-new-tx-after-ambiguous-broadcast**

In a scratch copy of `executor.go` (never the real working tree), temporarily swap the order of `broadcastWithRecovery` and the hash-check inside it (e.g. call `SendTransaction` unconditionally, skipping `TransactionByHash` first), rerun `TestDriveExecutionForward_AmbiguousBroadcastFoundOnChainNeverResends`, and confirm it now fails (`ethClient.sendCalled` becomes true when it shouldn't). Separately, mutate `signAndPersist` to call `e.Store.PersistSignedExecution` AFTER a (temporarily added) broadcast call instead of before, and confirm some test in this file's suite would need updating to catch it — if none does, add one that asserts persistence happens before any `SendTransaction` call (e.g. a store fake that records call order into a shared slice and a test asserting `"persist"` precedes `"send"`). Restore the real file before continuing and rerun the full suite clean.

- [ ] **Step 7: Commit**

```bash
git add go-api/internal/bridge/across/quote.go go-api/internal/worker/executor.go go-api/internal/worker/executor_test.go
git commit -m "Add worker.Executor: claim, quote, sign, persist-before-broadcast, ambiguous-broadcast recovery"
```

---

### Task 13: `worker/reconciler.go` — the fourth goroutine

**Files:**
- Create: `go-api/internal/worker/reconciler.go`
- Test: `go-api/internal/worker/reconciler_test.go`

**Interfaces:**
- Consumes: `Executor`, `Executor.ExecuteTestnetPayment`/`DriveExecutionForward` (Task 12); `Store.StaleTestnetProcessingWithoutExecutionIDs`/`ReconciliationCandidates`/`GetExecutionByPaymentID`/`UpdateExecutionExternalStatus`/`CompleteSubmittedPayment`/`LowestUnconfirmedNonce` (Task 7); `across.Client.DepositStatusByTxHash`/`ErrDepositNotFound` (Task 10).
- Produces: `worker.Reconciler`, `(*Reconciler).SweepOnce(ctx)`, `(*Reconciler).Run(ctx, interval)` — consumed by Task 16 (cmd/worker wiring, the 4th goroutine).

- [ ] **Step 1: Implement `reconciler.go`**

```go
package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"chainroute/go-api/internal/bridge/across"
	"chainroute/go-api/internal/payment"
)

// ReconcilerStore is the subset of *postgres.Store the reconciler needs,
// beyond what it reaches indirectly through Executor.
type ReconcilerStore interface {
	ExecutorStore
	GetExecutionByPaymentID(ctx context.Context, paymentID string) (payment.Execution, bool, error)
	StaleTestnetProcessingWithoutExecutionIDs(ctx context.Context, staleness time.Duration) ([]string, error)
	ReconciliationCandidates(ctx context.Context, staleness time.Duration) ([]payment.Execution, error)
	UpdateExecutionExternalStatus(ctx context.Context, executionID string, status payment.ExternalStatus, confirmedAt *sql.NullTime) error
	CompleteSubmittedPayment(ctx context.Context, paymentID string, terminal payment.Status) (bool, error)
	LowestUnconfirmedNonce(ctx context.Context, walletAddress string) (int64, bool, error)
}

// ReconcilerEthClient extends ExecutorEthClient with the read-only calls
// reconciliation itself needs (never a write beyond what Executor already
// performs via DriveExecutionForward).
type ReconcilerEthClient interface {
	ExecutorEthClient
	TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error)
	PendingNonceAt(ctx context.Context, account common.Address) (uint64, error)
}

// Reconciler is the fourth cmd/worker goroutine (design spec §15),
// distinct from Recovery (Phase 6, simulated-mode only): it answers "what
// actually happened externally," using chain RPC and the Across status
// API, not just Postgres.
type Reconciler struct {
	Store         ReconcilerStore
	Executor      *Executor
	OriginClient  ReconcilerEthClient
	Across        *across.Client
	WalletAddress common.Address
	OriginChainID int64
	Staleness     time.Duration
}

// SweepOnce runs one full reconciliation pass MINUS the divergence check,
// which Run schedules on its own, separately configurable interval (see
// Run below) -- recovering stale crash-point-A payments, driving stale
// not-yet-broadcast executions forward, and checking already-broadcast
// executions' real outcome (lowest-nonce-first per wallet). Each phase
// logs and continues past a single item's error rather than aborting the
// whole sweep, mirroring Recovery.SweepOnce's own style.
func (r *Reconciler) SweepOnce(ctx context.Context) {
	r.recoverStaleProcessingWithoutExecution(ctx)
	r.driveStaleNotYetBroadcast(ctx)
	r.checkBroadcastOutcomes(ctx)
}

// recoverStaleProcessingWithoutExecution handles crash point A (design
// spec §12): a testnet-mode payment stuck PROCESSING with no execution
// row at all. Executor.ExecuteTestnetPayment already implements exactly
// the race-safe claim-then-drive-forward sequence this needs -- reused
// directly, not reimplemented.
func (r *Reconciler) recoverStaleProcessingWithoutExecution(ctx context.Context) {
	ids, err := r.Store.StaleTestnetProcessingWithoutExecutionIDs(ctx, r.Staleness)
	if err != nil {
		log.Printf("ERROR: reconciler: query stale testnet processing without execution: %v", err)
		return
	}
	for _, id := range ids {
		if err := r.Executor.ExecuteTestnetPayment(ctx, id); err != nil {
			log.Printf("ERROR: reconciler: recover stale processing payment %s: %v", id, err)
		}
	}
}

// driveStaleNotYetBroadcast handles crash points B/C: an execution row
// exists (with its nonce already fixed) but has not been broadcast, and
// has sat without progress past staleness. Re-fetches the row immediately
// before acting, since the original executor may have finished between
// ReconciliationCandidates' query and now (design spec §15) -- acting on
// a stale in-memory copy here could otherwise attempt to re-drive a row
// that's already moved on.
func (r *Reconciler) driveStaleNotYetBroadcast(ctx context.Context) {
	candidates, err := r.Store.ReconciliationCandidates(ctx, r.Staleness)
	if err != nil {
		log.Printf("ERROR: reconciler: query reconciliation candidates: %v", err)
		return
	}
	for _, c := range candidates {
		if c.BroadcastAt != nil {
			continue // handled by checkBroadcastOutcomes
		}
		fresh, found, err := r.Store.GetExecutionByPaymentID(ctx, c.PaymentID)
		if err != nil || !found {
			log.Printf("ERROR: reconciler: re-check execution for payment %s before driving forward: %v", c.PaymentID, err)
			continue
		}
		if fresh.BroadcastAt != nil {
			continue // the original executor finished in the meantime
		}
		if err := r.Executor.DriveExecutionForward(ctx, fresh); err != nil {
			log.Printf("ERROR: reconciler: drive execution forward for payment %s: %v", c.PaymentID, err)
		}
	}
}

// checkBroadcastOutcomes handles already-broadcast, unconfirmed
// executions, restricted to the LOWEST unconfirmed nonce per wallet
// (design spec §15): checking or rebroadcasting a higher nonce cannot
// possibly progress it while a lower one is unresolved, by ordinary EVM
// nonce-ordering rules, so this never spends RPC/API budget on nonces that
// structurally cannot mine yet.
func (r *Reconciler) checkBroadcastOutcomes(ctx context.Context) {
	candidates, err := r.Store.ReconciliationCandidates(ctx, r.Staleness)
	if err != nil {
		log.Printf("ERROR: reconciler: query reconciliation candidates: %v", err)
		return
	}

	lowestByWallet := map[string]payment.Execution{}
	for _, c := range candidates {
		if c.BroadcastAt == nil {
			continue
		}
		current, ok := lowestByWallet[c.WalletAddress]
		if !ok || c.Nonce < current.Nonce {
			lowestByWallet[c.WalletAddress] = c
		}
	}

	for _, exec := range lowestByWallet {
		if err := r.checkAndUpdateOutcome(ctx, exec); err != nil {
			log.Printf("ERROR: reconciler: check outcome for execution %s (payment %s): %v", exec.ID, exec.PaymentID, err)
		}
	}
}

func (r *Reconciler) checkAndUpdateOutcome(ctx context.Context, exec payment.Execution) error {
	if exec.SignedTxHash == nil {
		return fmt.Errorf("execution %s is marked broadcast but has no signed_tx_hash", exec.ID)
	}
	hash := common.HexToHash(*exec.SignedTxHash)

	receipt, err := r.OriginClient.TransactionReceipt(ctx, hash)
	if err != nil {
		// Not yet mined, or a transient RPC error -- neither is a
		// definitive failure signal (design spec §13). Leave state as-is;
		// the next sweep retries.
		return nil
	}
	if receipt.Status == 0 {
		return r.markTerminal(ctx, exec, payment.ExternalStatusReverted, payment.StatusFailed)
	}

	status, err := r.Across.DepositStatusByTxHash(ctx, r.OriginChainID, *exec.SignedTxHash)
	if err != nil {
		if errors.Is(err, across.ErrDepositNotFound) {
			return nil // Across's indexer hasn't observed it yet -- not a failure.
		}
		return nil // transient API error -- not a failure signal, retry next sweep.
	}

	switch status.Status {
	case "filled":
		return r.markTerminal(ctx, exec, payment.ExternalStatusFilled, payment.StatusCompleted)
	case "expired":
		return r.markTerminal(ctx, exec, payment.ExternalStatusExpired, payment.StatusFailed)
	case "refunded":
		return r.markTerminal(ctx, exec, payment.ExternalStatusRefunded, payment.StatusFailed)
	case "pending":
		return nil
	default:
		log.Printf("WARNING: reconciler: unrecognized across status %q for execution %s -- treating as still pending", status.Status, exec.ID)
		return nil
	}
}

func (r *Reconciler) markTerminal(ctx context.Context, exec payment.Execution, external payment.ExternalStatus, terminal payment.Status) error {
	confirmedAt := &sql.NullTime{Time: time.Now().UTC(), Valid: true}
	if err := r.Store.UpdateExecutionExternalStatus(ctx, exec.ID, external, confirmedAt); err != nil {
		return fmt.Errorf("record %s: %w", external, err)
	}
	if _, err := r.Store.CompleteSubmittedPayment(ctx, exec.PaymentID, terminal); err != nil {
		return fmt.Errorf("complete payment as %s: %w", terminal, err)
	}
	return nil
}

// checkNonceDivergence is a read-only diagnostic (design spec §15): it
// NEVER writes to wallet_nonces. It only logs a warning if the chain's own
// pending nonce count falls behind what ChainRoute's own durable state
// expects, which would indicate a nonce got stuck or the dedicated-wallet
// invariant (§18) was violated by out-of-band wallet use.
func (r *Reconciler) checkNonceDivergence(ctx context.Context) {
	chainPending, err := r.OriginClient.PendingNonceAt(ctx, r.WalletAddress)
	if err != nil {
		log.Printf("WARNING: reconciler: divergence check: query chain pending nonce: %v", err)
		return
	}
	lowest, found, err := r.Store.LowestUnconfirmedNonce(ctx, r.WalletAddress.Hex())
	if err != nil {
		log.Printf("WARNING: reconciler: divergence check: query lowest unconfirmed nonce: %v", err)
		return
	}
	if !found {
		return
	}
	if int64(chainPending) < lowest {
		log.Printf("WARNING: reconciler: nonce divergence for wallet %s: chain pending nonce %d is behind ChainRoute's lowest unconfirmed nonce %d -- diagnostic only, wallet_nonces is never adjusted automatically", r.WalletAddress.Hex(), chainPending, lowest)
	}
}

// Run loops SweepOnce on sweepInterval and the read-only divergence check
// on its own, separately configurable divergenceInterval (design spec §31
// deliberately names NONCE_DIVERGENCE_CHECK_INTERVAL_SECONDS as a distinct
// knob from RECONCILE_SWEEP_INTERVAL_SECONDS, since the divergence check
// is a cheap diagnostic that doesn't need to run as often as the
// correctness-critical sweep). Both tickers share one shutdown path,
// mirroring Recovery.Run and Publisher.Run's existing pattern.
func (r *Reconciler) Run(ctx context.Context, sweepInterval, divergenceInterval time.Duration) {
	sweepTicker := time.NewTicker(sweepInterval)
	defer sweepTicker.Stop()
	divergenceTicker := time.NewTicker(divergenceInterval)
	defer divergenceTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-sweepTicker.C:
			r.SweepOnce(ctx)
		case <-divergenceTicker.C:
			r.checkNonceDivergence(ctx)
		}
	}
}
```

- [ ] **Step 2: Write tests**

```go
package worker

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"chainroute/go-api/internal/bridge/across"
	"chainroute/go-api/internal/evm"
	"chainroute/go-api/internal/payment"
	"chainroute/go-api/internal/postgres"
)

type fakeReconcilerStore struct {
	fakeExecutorStore
	staleNoExecIDs      []string
	candidates          []payment.Execution
	getExecByPayment    map[string]payment.Execution
	updateStatusCalls   []payment.ExternalStatus
	completeCalls       []payment.Status
	lowestNonce         int64
	lowestNonceFound    bool
}

func (f *fakeReconcilerStore) GetExecutionByPaymentID(ctx context.Context, paymentID string) (payment.Execution, bool, error) {
	e, ok := f.getExecByPayment[paymentID]
	return e, ok, nil
}
func (f *fakeReconcilerStore) StaleTestnetProcessingWithoutExecutionIDs(ctx context.Context, staleness time.Duration) ([]string, error) {
	return f.staleNoExecIDs, nil
}
func (f *fakeReconcilerStore) ReconciliationCandidates(ctx context.Context, staleness time.Duration) ([]payment.Execution, error) {
	return f.candidates, nil
}
func (f *fakeReconcilerStore) UpdateExecutionExternalStatus(ctx context.Context, executionID string, status payment.ExternalStatus, confirmedAt *sql.NullTime) error {
	f.updateStatusCalls = append(f.updateStatusCalls, status)
	return nil
}
func (f *fakeReconcilerStore) CompleteSubmittedPayment(ctx context.Context, paymentID string, terminal payment.Status) (bool, error) {
	f.completeCalls = append(f.completeCalls, terminal)
	return true, nil
}
func (f *fakeReconcilerStore) LowestUnconfirmedNonce(ctx context.Context, walletAddress string) (int64, bool, error) {
	return f.lowestNonce, f.lowestNonceFound, nil
}

type fakeReconcilerEthClient struct {
	fakeExecutorEthClient
	receipts       map[common.Hash]*types.Receipt
	pendingNonce   uint64
}

func (f *fakeReconcilerEthClient) TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	r, ok := f.receipts[txHash]
	if !ok {
		return nil, sql.ErrNoRows // stand-in "not found yet" error
	}
	return r, nil
}
func (f *fakeReconcilerEthClient) PendingNonceAt(ctx context.Context, account common.Address) (uint64, error) {
	return f.pendingNonce, nil
}

func TestSweepOnce_LowestNonceFirstOnly(t *testing.T) {
	wallet := "0xLowestNonceWallet00000000000000000005"
	hashLow, hashHigh := "0x"+string(make([]byte, 0))+"1111111111111111111111111111111111111111111111111111111111111", "0x2222222222222222222222222222222222222222222222222222222222222222"
	_ = hashLow
	store := &fakeReconcilerStore{
		candidates: []payment.Execution{
			{ID: "exec-low", PaymentID: "pay-low", WalletAddress: wallet, Nonce: 5, SignedTxHash: strPtr("0x1111111111111111111111111111111111111111111111111111111111111111"), BroadcastAt: timePtr()},
			{ID: "exec-high", PaymentID: "pay-high", WalletAddress: wallet, Nonce: 6, SignedTxHash: strPtr("0x2222222222222222222222222222222222222222222222222222222222222222"), BroadcastAt: timePtr()},
		},
	}
	ethClient := &fakeReconcilerEthClient{receipts: map[common.Hash]*types.Receipt{}}
	r := &Reconciler{Store: store, OriginClient: ethClient, Across: newTestAcrossServer(t), OriginChainID: 11155111, Staleness: time.Hour}

	r.checkBroadcastOutcomes(context.Background())

	// Only the lowest nonce (5) should have been checked -- since neither
	// receipt is populated in the fake, both would "not error out" the
	// same way, so instead assert via which one WOULD be queried: extend
	// the fake to record calls if a stronger assertion is needed. Minimal
	// assertion here: no panic, no call for a nonce that was never in the
	// candidate set.
	_ = hashHigh
}

func TestCheckAndUpdateOutcome_RevertedTransactionFailsPayment(t *testing.T) {
	hash := "0x3333333333333333333333333333333333333333333333333333333333333333"
	store := &fakeReconcilerStore{}
	ethClient := &fakeReconcilerEthClient{receipts: map[common.Hash]*types.Receipt{
		common.HexToHash(hash): {Status: 0},
	}}
	r := &Reconciler{Store: store, OriginClient: ethClient, Across: newTestAcrossServer(t), OriginChainID: 11155111, Staleness: time.Hour}

	exec := payment.Execution{ID: "exec-1", PaymentID: "pay-1", SignedTxHash: &hash}
	if err := r.checkAndUpdateOutcome(context.Background(), exec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(store.updateStatusCalls) != 1 || store.updateStatusCalls[0] != payment.ExternalStatusReverted {
		t.Fatalf("expected a single ExternalStatusReverted update, got %v", store.updateStatusCalls)
	}
	if len(store.completeCalls) != 1 || store.completeCalls[0] != payment.StatusFailed {
		t.Fatalf("expected a single StatusFailed completion, got %v", store.completeCalls)
	}
}

func TestCheckAndUpdateOutcome_TransientPollFailureDoesNotFailPayment(t *testing.T) {
	hash := "0x4444444444444444444444444444444444444444444444444444444444444444"
	store := &fakeReconcilerStore{}
	ethClient := &fakeReconcilerEthClient{receipts: map[common.Hash]*types.Receipt{}} // no receipt yet -- "not mined"
	r := &Reconciler{Store: store, OriginClient: ethClient, Across: newTestAcrossServer(t), OriginChainID: 11155111, Staleness: time.Hour}

	exec := payment.Execution{ID: "exec-2", PaymentID: "pay-2", SignedTxHash: &hash}
	if err := r.checkAndUpdateOutcome(context.Background(), exec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(store.completeCalls) != 0 {
		t.Fatal("a transient/not-yet-mined poll must never produce a terminal transition")
	}
}

func TestNonceDivergence_LogsButNeverWrites(t *testing.T) {
	store := &fakeReconcilerStore{lowestNonce: 10, lowestNonceFound: true}
	ethClient := &fakeReconcilerEthClient{pendingNonce: 5} // behind -- divergence
	r := &Reconciler{Store: store, OriginClient: ethClient, WalletAddress: common.HexToAddress("0xabc")}

	r.checkNonceDivergence(context.Background()) // must not panic; ReconcilerStore has no write method this could call
}

func strPtr(s string) *string { return &s }
func timePtr() *time.Time     { t := time.Now(); return &t }
```

**Note:** `TestSweepOnce_LowestNonceFirstOnly`'s assertion is intentionally weak as drafted — strengthen it during implementation by having `fakeReconcilerEthClient.TransactionReceipt` append every queried hash to a `queriedHashes []common.Hash` field, then assert that list contains exactly the lowest-nonce execution's hash and not the higher one. Do not leave the weak version in the committed test; it is a placeholder to be completed, not a placeholder to be shipped (per this plan's own "No Placeholders" discipline) — strengthen it as part of Step 2, before Step 3.

- [ ] **Step 3: Run tests**

```bash
cd go-api && go build ./... && go test ./internal/worker/... -run 'TestSweepOnce|TestCheckAndUpdateOutcome|TestNonceDivergence' -v
```

Expected: PASS.

- [ ] **Step 4: Mutation check — terminal-state transition guards**

In a scratch copy, change `checkAndUpdateOutcome`'s `"pending"` case to fall through to `markTerminal(..., payment.StatusFailed)` instead of `return nil`. Rerun `TestCheckAndUpdateOutcome_TransientPollFailureDoesNotFailPayment` and confirm it now fails. This proves the test actually guards against manufacturing `FAILED` out of a non-terminal signal (design spec §13), not merely happening to pass. Restore the real file before continuing.

- [ ] **Step 5: Commit**

```bash
git add go-api/internal/worker/reconciler.go go-api/internal/worker/reconciler_test.go
git commit -m "Add worker.Reconciler: stale-PROCESSING recovery, resume, lowest-nonce-first outcome checks, divergence monitor"
```

---

### Task 14: Wire `Processor` to dispatch by `execution_mode`

**Files:**
- Modify: `go-api/internal/worker/processor.go`
- Modify: `go-api/internal/worker/processor_test.go`

**Interfaces:**
- Consumes: `Store.ClaimPayment` returning `(bool, payment.ExecutionMode, error)` (Task 5); `Executor.ExecuteTestnetPayment` (Task 12).
- Produces: `Processor.Executor *Executor` field, `HandleRoutedPayment` dispatching simulated vs testnet — consumed by Task 16 (cmd/worker wiring).

- [ ] **Step 1: Update `PaymentStore` interface and `Processor` struct**

```go
type PaymentStore interface {
	ClaimPayment(ctx context.Context, paymentID string) (bool, payment.ExecutionMode, error)
	CompletePayment(ctx context.Context, paymentID string, terminal payment.Status) (bool, error)
}

// TestnetExecutor is the subset of *Executor Processor needs -- kept as an
// interface so processor_test.go can fake it without constructing a real
// wallet/RPC/Across client.
type TestnetExecutor interface {
	ExecuteTestnetPayment(ctx context.Context, paymentID string) error
}

// Processor handles one PAYMENT_ROUTED event at a time. Executor is nil
// when BLOCKCHAIN_ENV != testnet (cmd/worker only wires it when testnet
// execution is actually enabled) -- HandleRoutedPayment must never reach
// the testnet branch in that configuration, because the API layer (Task
// 15) refuses to create execution_mode="testnet" payments unless the
// server itself is configured for testnet, so no ROUTED testnet-mode
// payment can exist for Kafka to ever deliver in the first place.
type Processor struct {
	Store    PaymentStore
	Executor TestnetExecutor
}
```

- [ ] **Step 2: Update `HandleRoutedPayment` to dispatch**

```go
func (p *Processor) HandleRoutedPayment(ctx context.Context, evt events.RoutedPayment) error {
	claimed, mode, err := p.Store.ClaimPayment(ctx, evt.PaymentID)
	if err != nil {
		return fmt.Errorf("claim payment %s: %w", evt.PaymentID, err)
	}
	if !claimed {
		return nil
	}

	if mode == payment.ExecutionModeTestnet {
		if p.Executor == nil {
			return fmt.Errorf("payment %s is execution_mode=testnet but this worker has no Executor configured (BLOCKCHAIN_ENV != testnet) -- this should be unreachable if the API layer's testnet gate is working", evt.PaymentID)
		}
		return p.Executor.ExecuteTestnetPayment(ctx, evt.PaymentID)
	}

	result := execution.Execute(evt.PaymentID)
	terminal := payment.StatusCompleted
	if !result.Success {
		terminal = payment.StatusFailed
	}
	if _, err := p.Store.CompletePayment(ctx, evt.PaymentID, terminal); err != nil {
		return fmt.Errorf("complete payment %s: %w", evt.PaymentID, err)
	}
	return nil
}
```

- [ ] **Step 3: Update `processor_test.go`'s `fakeStore` (from Task 5) and add dispatch tests**

`fakeStore.ClaimPayment` already returns `(bool, payment.ExecutionMode, error)` with a `claimMode` field defaulting to simulated per Task 5, Step 4 — every existing test in this file keeps passing unmodified. Add:

```go
type fakeTestnetExecutor struct {
	called    bool
	calledID  string
	returnErr error
}

func (f *fakeTestnetExecutor) ExecuteTestnetPayment(ctx context.Context, paymentID string) error {
	f.called = true
	f.calledID = paymentID
	return f.returnErr
}

func TestHandleRoutedPayment_TestnetModeDispatchesToExecutor(t *testing.T) {
	store := &fakeStore{claimResult: true, claimMode: payment.ExecutionModeTestnet}
	executor := &fakeTestnetExecutor{}
	proc := &Processor{Store: store, Executor: executor}

	evt := events.RoutedPayment{PaymentID: "testnet-pay-1"}
	if err := proc.HandleRoutedPayment(context.Background(), evt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !executor.called || executor.calledID != "testnet-pay-1" {
		t.Fatalf("expected Executor.ExecuteTestnetPayment to be called with the payment id, got called=%v id=%q", executor.called, executor.calledID)
	}
	if store.completeCalls != 0 {
		t.Fatal("testnet-mode dispatch must never call CompletePayment directly -- the executor/reconciler own that transition via MarkSubmitted/CompleteSubmittedPayment")
	}
}

func TestHandleRoutedPayment_SimulatedModeNeverCallsExecutor(t *testing.T) {
	id := findIDWithOutcome(t, true)
	store := &fakeStore{claimResult: true, claimMode: payment.ExecutionModeSimulated, completeResult: true}
	executor := &fakeTestnetExecutor{}
	proc := &Processor{Store: store, Executor: executor}

	if err := proc.HandleRoutedPayment(context.Background(), events.RoutedPayment{PaymentID: id}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if executor.called {
		t.Fatal("simulated-mode payments must never reach the testnet executor")
	}
}

func TestHandleRoutedPayment_TestnetModeWithNilExecutorErrors(t *testing.T) {
	store := &fakeStore{claimResult: true, claimMode: payment.ExecutionModeTestnet}
	proc := &Processor{Store: store, Executor: nil}

	err := proc.HandleRoutedPayment(context.Background(), events.RoutedPayment{PaymentID: "any"})
	if err == nil {
		t.Fatal("expected an error when a testnet-mode payment reaches a Processor with no Executor configured")
	}
}
```

- [ ] **Step 4: Run tests**

```bash
cd go-api && go build ./... && go test ./internal/worker/... -v
```

Expected: PASS, including every pre-existing test in `processor_test.go` unmodified.

- [ ] **Step 5: Commit**

```bash
git add go-api/internal/worker/processor.go go-api/internal/worker/processor_test.go
git commit -m "Dispatch Processor by execution_mode: simulated (unchanged) vs testnet (Executor)"
```

---

### Task 15: HTTP API — `execution_mode` request/response fields and testnet gating

**Files:**
- Modify: `go-api/internal/handler/payments.go`
- Modify: `go-api/internal/handler/routes.go`
- Modify: `go-api/internal/handler/payments_test.go`

**Interfaces:**
- Consumes: `payment.ExecutionMode`/`ExecutionModeSimulated`/`ExecutionModeTestnet` (Task 3); `money.DecimalToBaseUnits` (Task 2); `Store.GetExecutionByPaymentID` (Task 7).
- Produces: `Handler.BlockchainEnv string`, `Handler.MaxTestnetAmountWei *big.Int` fields — consumed by Task 16 (cmd/server wiring).

- [ ] **Step 1: Extend `PaymentStore` interface and `Handler` struct**

In `routes.go`, extend `Handler`:

```go
type Handler struct {
	Client             RoutingClient
	Store              PaymentStore
	BlockchainEnv      string   // "testnet" enables execution_mode="testnet" requests; anything else (including "") rejects them
	MaxTestnetAmountWei *big.Int // nil means no ceiling is enforced (only safe when BlockchainEnv != "testnet")
}
```

Add `"math/big"` to `routes.go`'s imports.

In `payments.go`, extend `PaymentStore`:

```go
type PaymentStore interface {
	CreateOrGetPayment(ctx context.Context, p payment.Payment) (payment.Payment, payment.CreateResult, error)
	GetPayment(ctx context.Context, id string) (payment.Payment, bool, error)
	LookupByIdempotencyKey(ctx context.Context, p payment.Payment) (payment.Payment, payment.CreateResult, bool, error)
	GetExecutionByPaymentID(ctx context.Context, paymentID string) (payment.Execution, bool, error)
}
```

- [ ] **Step 2: Extend request/response types**

```go
type createPaymentRequest struct {
	SourceChain      string `json:"source_chain"`
	DestinationChain string `json:"destination_chain"`
	Asset            string `json:"asset"`
	Amount           string `json:"amount"`
	ExecutionMode    string `json:"execution_mode"`
}

type paymentResponse struct {
	ID               string        `json:"id"`
	SourceChain      string        `json:"source_chain"`
	DestinationChain string        `json:"destination_chain"`
	Asset            string        `json:"asset"`
	Amount           string        `json:"amount"`
	Status           string        `json:"status"`
	TotalFee         float64       `json:"total_fee"`
	Hops             []hopResponse `json:"hops"`
	ExecutionMode    string        `json:"execution_mode"`
	BridgeProvider   *string       `json:"bridge_provider"`
	ExternalTxHash   *string       `json:"external_tx_hash"`
	SubmittedAt      *string       `json:"submitted_at"`
	CreatedAt        string        `json:"created_at"`
	UpdatedAt        string        `json:"updated_at"`
	CompletedAt      *string       `json:"completed_at"`
}
```

- [ ] **Step 3: Update `toPaymentResponse` to take the optional execution row**

```go
func toPaymentResponse(p payment.Payment, exec payment.Execution, execFound bool) paymentResponse {
	hops := make([]hopResponse, 0, len(p.Hops))
	for _, h := range p.Hops {
		hops = append(hops, hopResponse{
			HopIndex: h.HopIndex, FromChain: h.FromChain, ToChain: h.ToChain,
			BridgeName: h.BridgeName, Fee: h.Fee, LatencyMs: h.LatencyMs,
			Liquidity: h.Liquidity, Reliability: h.Reliability,
		})
	}
	var completedAt *string
	if p.CompletedAt != nil {
		formatted := p.CompletedAt.UTC().Format(time.RFC3339Nano)
		completedAt = &formatted
	}
	var externalTxHash, submittedAt *string
	if execFound {
		if exec.SignedTxHash != nil {
			externalTxHash = exec.SignedTxHash
		}
		if exec.BroadcastAt != nil {
			formatted := exec.BroadcastAt.UTC().Format(time.RFC3339Nano)
			submittedAt = &formatted
		}
	}
	return paymentResponse{
		ID: p.ID, SourceChain: p.SourceChain, DestinationChain: p.DestinationChain,
		Asset: p.Asset, Amount: p.Amount, Status: string(p.Status), TotalFee: p.TotalFee,
		Hops: hops, ExecutionMode: string(p.ExecutionMode), BridgeProvider: p.BridgeProvider,
		ExternalTxHash: externalTxHash, SubmittedAt: submittedAt,
		CreatedAt:   p.CreatedAt.UTC().Format(time.RFC3339Nano),
		UpdatedAt:   p.UpdatedAt.UTC().Format(time.RFC3339Nano),
		CompletedAt: completedAt,
	}
}
```

Every existing call site of `toPaymentResponse(p)` in `payments.go` becomes `toPaymentResponse(p, payment.Execution{}, false)` for a simulated-mode payment (no execution row exists) — update each call site in `PostPayments` accordingly (the `Replayed`/`Created` branches). `GetPayment` (Step 5 below) is the only call site that passes a real, looked-up execution.

- [ ] **Step 4: Add testnet-mode validation to `PostPayments`**

Insert this block in `PostPayments`, after the existing amount-pattern/zero-amount check and before the idempotency-key lookup (so an invalid `execution_mode` request fails fast, consistent with the existing validation-before-lookup ordering):

```go
mode := payment.ExecutionModeSimulated
if req.ExecutionMode != "" {
	mode = payment.ExecutionMode(req.ExecutionMode)
}
if mode != payment.ExecutionModeSimulated && mode != payment.ExecutionModeTestnet {
	writeError(w, http.StatusBadRequest, "execution_mode must be \"simulated\" or \"testnet\"")
	return
}
var bridgeProvider *string
if mode == payment.ExecutionModeTestnet {
	if h.BlockchainEnv != "testnet" {
		writeError(w, http.StatusBadRequest, "execution_mode=testnet is not enabled on this server")
		return
	}
	// Phase 7 supports exactly one hardcoded route/asset (design spec
	// §19): Sepolia -> Base Sepolia, ETH (interpreted as WETH for testnet
	// execution -- see internal/handler's asset naming, which reuses the
	// existing "eth" asset value rather than introducing a new protobuf
	// Asset variant purely for this one testnet path).
	if strings.ToLower(req.SourceChain) != "ethereum" || strings.ToLower(req.DestinationChain) != "base" || strings.ToLower(req.Asset) != "eth" {
		writeError(w, http.StatusBadRequest, "execution_mode=testnet only supports source_chain=ethereum, destination_chain=base, asset=eth (bridged as WETH)")
		return
	}
	amountWei, err := money.DecimalToBaseUnits(req.Amount, 18)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid amount for testnet execution: "+err.Error())
		return
	}
	if h.MaxTestnetAmountWei != nil && amountWei.Cmp(h.MaxTestnetAmountWei) > 0 {
		writeError(w, http.StatusBadRequest, "amount exceeds the configured maximum testnet execution amount")
		return
	}
	provider := "across"
	bridgeProvider = &provider
}
```

Add `"chainroute/go-api/internal/money"` and `"math/big"` (if not already present via the `Handler` struct change) to `payments.go`'s imports.

Thread `mode`/`bridgeProvider` into the `lookupCandidate` and `candidate` payment.Payment literals later in the function:

```go
lookupCandidate := payment.Payment{
	IdempotencyKey: idempotencyKey, SourceChain: chainNameByValue[sourceChain],
	DestinationChain: chainNameByValue[destChain], Asset: assetNameByValue[asset],
	Amount: req.Amount, ExecutionMode: mode,
}
```

```go
candidate := payment.Payment{
	IdempotencyKey: idempotencyKey, SourceChain: chainNameByValue[sourceChain],
	DestinationChain: chainNameByValue[destChain], Asset: assetNameByValue[asset],
	Amount: req.Amount, TotalFee: resp.GetTotalFee(), Hops: hops,
	ExecutionMode: mode, BridgeProvider: bridgeProvider,
}
```

- [ ] **Step 5: Update `GetPayment` to look up and include the execution row**

```go
func (h *Handler) GetPayment(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	p, found, err := h.Store.GetPayment(r.Context(), id)
	if err != nil {
		log.Printf("ERROR: failed to read payment: %v", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "payment not found")
		return
	}
	exec, execFound, err := h.Store.GetExecutionByPaymentID(r.Context(), id)
	if err != nil {
		log.Printf("ERROR: failed to read payment execution: %v", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, toPaymentResponse(p, exec, execFound))
}
```

- [ ] **Step 6: Update `payments_test.go`'s fake store and existing calls**

The fake `PaymentStore` implementation in `payments_test.go` must gain a `GetExecutionByPaymentID` method to satisfy the extended interface — add it returning `(payment.Execution{}, false, nil)` unconditionally, which keeps every existing simulated-mode test passing unmodified (no execution row exists for a simulated payment). Find the fake store type in this file (likely named something like `fakePaymentStore` or similar — read the file to find its exact name) and add:

```go
func (f *fakePaymentStore) GetExecutionByPaymentID(ctx context.Context, paymentID string) (payment.Execution, bool, error) {
	return payment.Execution{}, false, nil
}
```

- [ ] **Step 7: Add new tests for testnet-mode validation**

```go
func TestPostPayments_TestnetModeRejectedWhenServerNotConfigured(t *testing.T) {
	h := newTestHandler(t) // reuse this file's existing handler-construction helper; h.BlockchainEnv left at its zero value ""
	body := `{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`
	req := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(body))
	req.Header.Set("Idempotency-Key", "testnet-gate-key")
	rec := httptest.NewRecorder()
	h.PostPayments(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when the server isn't configured for testnet execution, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestPostPayments_TestnetModeRejectsUnsupportedRoute(t *testing.T) {
	h := newTestHandler(t)
	h.BlockchainEnv = "testnet"
	body := `{"source_chain":"arbitrum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`
	req := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(body))
	req.Header.Set("Idempotency-Key", "testnet-route-key")
	rec := httptest.NewRecorder()
	h.PostPayments(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for an unsupported testnet route, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestPostPayments_TestnetModeRejectsAmountOverCeiling(t *testing.T) {
	h := newTestHandler(t)
	h.BlockchainEnv = "testnet"
	h.MaxTestnetAmountWei = big.NewInt(1) // absurdly low, guarantees rejection
	body := `{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`
	req := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(body))
	req.Header.Set("Idempotency-Key", "testnet-ceiling-key")
	rec := httptest.NewRecorder()
	h.PostPayments(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for an amount over the configured ceiling, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestPostPayments_DefaultExecutionModeIsSimulated(t *testing.T) {
	h := newTestHandler(t) // existing helper presumably wires a stub RoutingClient that returns a valid route
	body := `{"source_chain":"ethereum","destination_chain":"base","asset":"usdc","amount":"100"}`
	req := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(body))
	req.Header.Set("Idempotency-Key", "default-mode-key")
	rec := httptest.NewRecorder()
	h.PostPayments(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	var body2 map[string]any
	json.Unmarshal(rec.Body.Bytes(), &body2)
	if body2["execution_mode"] != "simulated" {
		t.Fatalf("expected execution_mode=simulated by default, got %v", body2["execution_mode"])
	}
}
```

**Note:** `newTestHandler` is written as if this file already has a shared handler-construction helper — read `payments_test.go` and `routes_test.go` during implementation to find the actual existing pattern (it may construct `&Handler{...}` inline per test instead). Adapt these four tests to match whatever pattern the file already uses; do not introduce a new helper if an equivalent one already exists, and do not change the existing tests' construction style.

- [ ] **Step 8: Run tests**

```bash
cd go-api && go build ./... && go test ./internal/handler/... -v
```

Expected: PASS, including every pre-existing test in this package unmodified (aside from the mechanical `GetExecutionByPaymentID` fake addition and `toPaymentResponse` call-site signature updates, neither of which changes any test's assertions).

- [ ] **Step 9: Commit**

```bash
git add go-api/internal/handler/payments.go go-api/internal/handler/routes.go go-api/internal/handler/payments_test.go
git commit -m "Add execution_mode API surface: request validation, testnet gating, response fields"
```

---

### Task 16: Wire `cmd/worker` and `cmd/server`

**Files:**
- Modify: `go-api/cmd/worker/main.go`
- Modify: `go-api/cmd/server/main.go`
- Create: `go-api/.env.example` (only if it doesn't already exist — check first; if it exists, extend it, don't overwrite)

**Interfaces:**
- Consumes: everything from Tasks 3, 5-15.
- Produces: the fully wired binaries — nothing downstream depends on this task; it is the integration point.

- [ ] **Step 1: Extend `cmd/worker/main.go`**

Add new env var parsing (alongside the existing `envOrDefault`/`envDuration` helpers), guarded so a non-testnet worker starts exactly as Phase 6 did:

```go
blockchainEnv := os.Getenv("BLOCKCHAIN_ENV")

reconcileStaleness := envDuration("RECONCILE_STALENESS_SECONDS", 120*time.Second, time.Second)
reconcileSweepInterval := envDuration("RECONCILE_SWEEP_INTERVAL_SECONDS", 30*time.Second, time.Second)
nonceDivergenceCheckInterval := envDuration("NONCE_DIVERGENCE_CHECK_INTERVAL_SECONDS", 60*time.Second, time.Second)
```

After `store := postgres.New(db)` and before constructing `processor`, add:

```go
var executor *worker.Executor
var reconciler *worker.Reconciler

if blockchainEnv == "testnet" {
	testnetWalletKey := os.Getenv("TESTNET_WALLET_PRIVATE_KEY")
	if testnetWalletKey == "" {
		log.Fatal("TESTNET_WALLET_PRIVATE_KEY is required when BLOCKCHAIN_ENV=testnet")
	}
	sepoliaRPC := os.Getenv("ETHEREUM_SEPOLIA_RPC_URL")
	baseSepoliaRPC := os.Getenv("BASE_SEPOLIA_RPC_URL")
	if sepoliaRPC == "" || baseSepoliaRPC == "" {
		log.Fatal("ETHEREUM_SEPOLIA_RPC_URL and BASE_SEPOLIA_RPC_URL are required when BLOCKCHAIN_ENV=testnet")
	}
	acrossBaseURL := envOrDefault("ACROSS_TESTNET_API_URL", "https://testnet.across.to/api")
	maxTestnetAmountWei := envBigInt("MAX_TESTNET_AMOUNT_WEI", big.NewInt(10_000_000_000_000_000)) // 0.01 WETH default ceiling

	wallet, err := evm.LoadWallet(testnetWalletKey)
	if err != nil {
		log.Fatalf("failed to load testnet wallet: %v", err)
	}
	log.Printf("testnet execution enabled: wallet address %s", wallet.Address.Hex())

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 10*time.Second)
	sepoliaClient, err := evm.Dial(dialCtx, sepoliaRPC, 11155111)
	dialCancel()
	if err != nil {
		log.Fatalf("failed to dial Sepolia RPC: %v", err)
	}
	dialCtx2, dialCancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	_, err = evm.Dial(dialCtx2, baseSepoliaRPC, 84532)
	dialCancel2()
	if err != nil {
		log.Fatalf("failed to dial Base Sepolia RPC: %v", err)
	}

	seedCtx, seedCancel := context.WithTimeout(context.Background(), 10*time.Second)
	pendingNonce, err := sepoliaClient.PendingNonceAt(seedCtx, wallet.Address)
	seedCancel()
	if err != nil {
		log.Fatalf("failed to query starting nonce: %v", err)
	}
	seedCtx2, seedCancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	err = store.SeedWalletNonce(seedCtx2, wallet.Address.Hex(), int64(pendingNonce))
	seedCancel2()
	if err != nil {
		log.Fatalf("failed to seed wallet nonce: %v", err)
	}

	acrossClient := across.NewClient(acrossBaseURL)
	acrossClient.APIKey = os.Getenv("ACROSS_API_KEY")
	acrossClient.IntegratorID = os.Getenv("ACROSS_INTEGRATOR_ID")

	executor = &worker.Executor{
		Store: store, Wallet: wallet, OriginClient: sepoliaClient, Across: acrossClient,
		BridgeProvider: "across", OriginChainID: 11155111, DestChainID: 84532,
		SpokePoolAddress: common.HexToAddress("0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662"),
		WETHOrigin:       common.HexToAddress("0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14"),
		WETHDestination:  common.HexToAddress("0x4200000000000000000000000000000000000006"),
		MaxAmountWei:     maxTestnetAmountWei,
	}
	reconciler = &worker.Reconciler{
		Store: store, Executor: executor, OriginClient: sepoliaClient, Across: acrossClient,
		WalletAddress: wallet.Address, OriginChainID: 11155111, Staleness: reconcileStaleness,
	}
}

processor := &worker.Processor{Store: store, Executor: executor}
```

Update the goroutine wait group to conditionally add the reconciler:

```go
var wg sync.WaitGroup
goroutines := 3
if reconciler != nil {
	goroutines = 4
}
wg.Add(goroutines)
go func() { defer wg.Done(); publisher.Run(ctx, outboxPollInterval) }()
go func() { defer wg.Done(); recovery.Run(ctx, recoverySweepInterval) }()
go func() { defer wg.Done(); runConsumeLoop(ctx, consumer, processor) }()
if reconciler != nil {
	go func() { defer wg.Done(); reconciler.Run(ctx, reconcileSweepInterval, nonceDivergenceCheckInterval) }()
}
```

Add the new imports: `"math/big"`, `"github.com/ethereum/go-ethereum/common"`, `"chainroute/go-api/internal/bridge/across"`, `"chainroute/go-api/internal/evm"`.

Add an `envBigInt` helper alongside the existing `envOrDefault`/`envDuration`:

```go
func envBigInt(key string, def *big.Int) *big.Int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, ok := new(big.Int).SetString(v, 10)
	if !ok {
		log.Fatalf("invalid %s: not a valid base-10 integer", key)
	}
	return n
}
```

Note: `Processor{Store: store, Executor: executor}` type-checks even when `executor` is `nil` — a nil `*worker.Executor` satisfies the `TestnetExecutor` interface as a typed nil, and `Processor.Executor == nil` in `HandleRoutedPayment`'s check compares correctly against that typed nil in this case since the field itself is declared as the concrete `*worker.Executor` pointer being assigned into an interface-typed struct field — **verify this compiles and behaves as expected during implementation**: if Go's typed-nil-in-interface subtlety causes `p.Executor == nil` to evaluate `false` even when `executor` is a nil `*Executor`, declare `Processor.Executor` assignment explicitly guarded (`var pe worker.TestnetExecutor; if executor != nil { pe = executor }; processor := &worker.Processor{Store: store, Executor: pe}`) instead. Write a quick throwaway test of this exact pattern if unsure before relying on it.

- [ ] **Step 2: Extend `cmd/server/main.go`**

```go
blockchainEnv := os.Getenv("BLOCKCHAIN_ENV")
maxTestnetAmountWei := envBigIntServer("MAX_TESTNET_AMOUNT_WEI", big.NewInt(10_000_000_000_000_000))
```

(`cmd/server` doesn't currently have an `envBigInt`-style helper — add a small local one, or extract Task 16 Step 1's `envBigInt` into a tiny shared internal package if duplicating a 10-line function across two `main` packages feels wrong; either is acceptable, prefer duplication here since these are two independent `main` packages and Phase 6 never introduced a shared `cmd`-internal package for this pattern.)

```go
h := &handler.Handler{Client: client, Store: store, BlockchainEnv: blockchainEnv, MaxTestnetAmountWei: maxTestnetAmountWei}
```

Add `"math/big"` to `cmd/server/main.go`'s imports.

- [ ] **Step 3: `.env.example`**

Check if `go-api/.env.example` (or a repo-root `.env.example`) already exists. If it does, append (don't overwrite) a Phase 7 section:

```
# Phase 7: real testnet execution (all optional; omit BLOCKCHAIN_ENV entirely for Phase 1-6 simulated-only behavior)
BLOCKCHAIN_ENV=
TESTNET_WALLET_PRIVATE_KEY=
ETHEREUM_SEPOLIA_RPC_URL=
BASE_SEPOLIA_RPC_URL=
ACROSS_TESTNET_API_URL=https://testnet.across.to/api
ACROSS_API_KEY=
ACROSS_INTEGRATOR_ID=
MAX_TESTNET_AMOUNT_WEI=
RECONCILE_STALENESS_SECONDS=
RECONCILE_SWEEP_INTERVAL_SECONDS=
NONCE_DIVERGENCE_CHECK_INTERVAL_SECONDS=
```

If no such file exists anywhere in the repo, create `go-api/.env.example` with just this content plus a one-line header comment. Never put a real key or URL in this file.

- [ ] **Step 4: Build and run the full existing test suite**

```bash
cd go-api
go build ./...
go vet ./...
go test ./...
DATABASE_URL="${DATABASE_URL:-postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable}" go test -tags=integration ./...
```

Expected: everything builds and passes. `go build ./cmd/worker` and `go build ./cmd/server` must both succeed with `BLOCKCHAIN_ENV` unset (default Phase 1-6 behavior) — manually run each briefly (`BLOCKCHAIN_ENV= ./worker` with valid `DATABASE_URL`/`KAFKA_BOOTSTRAP_SERVERS`, Ctrl-C after confirming the "worker started" log line, no testnet-related fatal errors) to confirm the testnet block is fully skipped, not just that it compiles.

- [ ] **Step 5: Commit**

```bash
git add go-api/cmd/worker/main.go go-api/cmd/server/main.go
git add go-api/.env.example # if created or modified
git commit -m "Wire testnet execution into cmd/worker (4th goroutine) and cmd/server (API gate)"
```

---

### Task 17: E2E script migration check + gated real-testnet Go tests + smoke script

**Files:**
- Modify: `scripts/e2e_test.sh`
- Create: `go-api/internal/worker/testnet_integration_test.go`
- Create: `scripts/e2e_testnet_test.sh`

**Interfaces:**
- Consumes: everything from Tasks 1-16.
- Produces: nothing further downstream depends on this task.

This task covers both of the design spec's two distinct real-testnet-gated deliverables: §24 (a `testnet_integration`-tagged **Go** test suite exercising one real quote→sign→broadcast→reconcile cycle, run via `go test`) and §25 (a **separate**, standalone bash smoke script exercising the same cycle end-to-end through the HTTP API and both binaries). They are gated the same way but serve different purposes — §24 is a fast, direct-package-level check; §25 is a full-system smoke test. Neither is ever part of the default `go test ./...` or `scripts/e2e_test.sh` run.

- [ ] **Step 1: Add the migration 0004 check to `scripts/e2e_test.sh`**

Mirror the existing pattern exactly (each prior migration checked via a `psql -tAc` existence probe before applying). Insert after the existing `0003` check:

```bash
/opt/homebrew/opt/postgresql@16/bin/psql "$DATABASE_URL" -tAc \
    "SELECT 1 FROM information_schema.tables WHERE table_name='payment_executions'" | grep -q 1 || \
    /opt/homebrew/opt/postgresql@16/bin/psql "$DATABASE_URL" -f "$ROOT_DIR/go-api/migrations/0004_across_testnet_execution.sql"
```

Do not modify any other part of `e2e_test.sh` — the existing deterministic local suite must remain fully local and network-free, exactly as the design spec §25 requires. Read the file first to find the exact insertion point (immediately after the existing `0003` check block, before the Redpanda-resolution section) and confirm nothing else in the file references schema state this migration changes in a way that needs updating.

- [ ] **Step 2: Run the existing E2E suite to confirm it still passes unmodified**

```bash
./scripts/e2e_test.sh
```

Expected: PASS, identical to its Phase 6 behavior (this script never sets `BLOCKCHAIN_ENV`, so the worker binary it builds and runs takes the simulated-only path).

- [ ] **Step 3: Write the `testnet_integration`-tagged Go test suite (design spec §24)**

```go
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
```

Run: `cd go-api && go build -tags=testnet_integration ./...` to confirm it compiles under the build tag (it will not run without `RUN_TESTNET_TESTS=1`, which is expected — confirm the `t.Skip` path works with `go test -tags=testnet_integration ./internal/worker/... -run TestRealTestnetDepositCycle -v` and no testnet env vars set). Confirm plain `go test ./...` (no tags) does not even compile this file (expected — the build tag excludes it entirely, which is the point).

- [ ] **Step 4: Write the gated real-testnet smoke script**

```bash
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
```

- [ ] **Step 5: Make it executable**

```bash
chmod +x scripts/e2e_testnet_test.sh
```

- [ ] **Step 6: Do NOT run either gated real-testnet check as part of normal plan execution**

Both the Step 3 Go test and this script require a funded Sepolia test wallet and real network access — both are explicitly gated (`RUN_TESTNET_TESTS=1`, plus for the script `BLOCKCHAIN_ENV=testnet` and real RPC URLs/a funded key) and neither runs automatically as part of this plan, `go test ./...`, `go test -tags=integration ./...`, or `scripts/e2e_test.sh`. Confirm this by re-running `scripts/e2e_test.sh` once more and confirming it makes no reference to `RUN_TESTNET_TESTS`, the new build tag, or either gated artifact.

- [ ] **Step 7: Commit**

```bash
git add scripts/e2e_test.sh scripts/e2e_testnet_test.sh go-api/internal/worker/testnet_integration_test.go
git commit -m "Add migration 0004 check to e2e_test.sh; add gated real-testnet Go tests and smoke script"
```

---

## Final verification (part of the subagent-driven-development final whole-branch review, not a numbered task)

Before this branch is considered complete, run and report on all of:

- `cd router && <the project's existing C++ router test command>`
- `cd cpp-routing-service && <the project's existing C++ routing-service test command>`
- `cd go-api && go build ./... && go vet ./... && go test ./...`
- `cd go-api && DATABASE_URL=... go test -tags=integration ./...`
- `cd go-api && DATABASE_URL=... KAFKA_BOOTSTRAP_SERVERS=... go test -tags=integration ./internal/kafka/...`
- `./scripts/e2e_test.sh`
- `go test -tags=testnet_integration ./internal/worker/...` and `./scripts/e2e_testnet_test.sh` **only** if `RUN_TESTNET_TESTS=1` and a real funded Sepolia wallet/RPC access are actually available — otherwise report explicitly that both were skipped and why, never fabricate a result.

Also perform, at the controller level (not delegated to an implementer subagent, per this session's established Phase 5/6 precedent):

- Mutation testing on: the `UNIQUE(payment_id)` race (Task 6, already specified as a per-task mutation check — re-verify once more on the final integrated branch); nonce-rollback-on-lost-race (same); signed-before-broadcast ordering (Task 12); no-new-transaction-after-ambiguous-broadcast (Task 12); terminal-state transition guards (Task 13, `CompleteSubmittedPayment`'s `WHERE status = 'SUBMITTED'` guard and the `"pending"`-must-never-become-`FAILED` case).
- A direct read of `go-api/migrations/0004_across_testnet_execution.sql` against the live schema (`\d payment_executions`, `\d wallet_nonces` via `psql`) to confirm the constraints actually match what was intended, not just what the migration file says.

The closing report to the user must include, per the user's own requirement list: files changed; final repository structure; verified Across details actually used (cross-checked against Task 1's findings doc); final schema; final execution state machine; the nonce strategy actually implemented; the request/execution lifecycle; crash/recovery behavior; test counts/results by component; mutation-testing results; a real testnet transaction hash and destination-fill evidence **only if** the opt-in real test actually ran (never fabricated); anything that could not be tested and why; guarantees provided; guarantees explicitly not provided (most importantly: no exactly-once network submission claim — only one durable external execution identity per payment, with safely recoverable rebroadcast/reconciliation of that same signed transaction identity); and explicit confirmation that `router/`, `cpp-routing-service/`, and `proto/` were not modified.
