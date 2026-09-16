# Phase 9 Multi-Provider Bridge Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Relay as a second real bridge quote provider so ChainRoute fetches competing live Across/Relay quotes, normalizes both into `CandidateEdge`s, lets the unmodified C++ `findCheapestRoute` pick the cheaper viable provider, persists exactly that selection, and executes/reconciles through it -- generalizing `Executor`/`Reconciler` off their current Across-specific hardcoding in the process.

**Architecture:** Two new narrow interfaces (`quote.Signer`, `quote.StatusChecker`) sit alongside the existing, unchanged `quote.Provider`. `across.Provider` and the new `relay.Provider` implement all three. `Executor` becomes the sole place that calls `wallet.SignTx`, validating an independently-configured envelope (chain ID, pinned contract address, exact value) before doing so, for either provider uniformly. `handler.PostPayments` fetches both providers' quotes concurrently with per-provider failure isolation instead of aborting on the first error.

**Tech Stack:** Go 1.x, C++20/gRPC (unchanged), PostgreSQL, Kafka, `go-ethereum`, Relay's live testnet HTTP API (`api.testnets.relay.link`).

**Design doc:** `docs/superpowers/specs/2026-09-16-multi-provider-bridge-routing-design.md` -- read it for the "why" behind any decision this plan states without re-arguing.

## Global Constraints

- No production C++ source or `.proto` change -- `CandidateEdge` is already `repeated`; `findCheapestRoute` already supports parallel edges. If any task believes it needs one, STOP and report back before making it.
- `Executor` is the ONLY caller of `wallet.SignTx`, for both providers. No provider implementation ever signs anything itself.
- The envelope-validation checkpoint (`ChainID`, pinned `To`, exact `Value`) runs on EVERY signing attempt, for both providers, before `wallet.SignTx` -- no exceptions.
- `Executor.ExpectedContractByProvider` is populated independently of any provider instance's own configuration (cmd/worker/main.go sets both separately, even though they hold the same address) -- never derive one from the other.
- Relay's `OutputAmountBaseUnits` is always `minimumAmount` (worst case), never `expectedAmount` (optimistic) -- this is what makes Relay's `FeeBaseUnits` economically comparable to Across's.
- No nonce is ever allocated before expiry + slippage validation passes, for either provider, on the first attempt.
- Every quote-fetch call from the HTTP handler happens concurrently across registered providers, with per-provider failure isolation: one provider erroring must never abort routing through a healthy second provider.
- Simulated mode (`execution_mode=simulated`) touches zero Relay code, zero `RELAY_*` environment variables, on every test suite and every code path -- unchanged from Phase 8's gating pattern (`blockchainEnv == "testnet"`, `mode == payment.ExecutionModeTestnet`).
- No Redis, no new microservice, no new message broker, no third provider, no mainnet, no multi-hop execution, no frontend.
- Migration files `0001`-`0005` are never modified. New migration: `0006_multi_provider_bridge_routing.sql`.

---

## Task 1: Live-verify Relay's current testnet API

Verification task, mirrors Phase 7/8's own Task-1 precedent. No code changes unless drift is found.

**Files:** none (verification only)

- [ ] **Step 1: Confirm base URL and auth**

```bash
curl -sS -X POST "https://api.testnets.relay.link/quote" -H "Content-Type: application/json" -d '{
  "user": "0x000000000000000000000000000000000000dEaD",
  "originChainId": 11155111, "destinationChainId": 84532,
  "originCurrency": "0x0000000000000000000000000000000000000000",
  "destinationCurrency": "0x4200000000000000000000000000000000000006",
  "amount": "1000000000000000", "tradeType": "EXACT_INPUT"
}' -w "\nHTTP:%{http_code}\n"
```

Expected (per design doc §2): HTTP 200, no auth needed, a `steps` array with exactly one `kind: "transaction"` step. If this now returns 401/403 or asks for an API key, STOP and report -- `RELAY_API_KEY` wiring (already planned as optional, Task 8) becomes required, not optional, and every other task's fixtures must be re-captured with a key.

- [ ] **Step 2: Confirm the deposit-contract address is stable across a different `user`**

Repeat Step 1's call with `"user": "0x1111111111111111111111111111111111111111"` instead. Compare the returned `steps[0].items[0].data.to` against the first call's. If they differ, STOP and report -- the pinned-`To` security check (Task 9) cannot be built on a per-request address; this is a load-bearing assumption from the design doc that must hold.

- [ ] **Step 3: Confirm the WETH-origin route is still unavailable and native-ETH-origin routes still work**

```bash
# Should fail with NO_SWAP_ROUTES_FOUND:
curl -sS -X POST "https://api.testnets.relay.link/quote" -H "Content-Type: application/json" -d '{
  "user": "0x000000000000000000000000000000000000dEaD",
  "originChainId": 11155111, "destinationChainId": 84532,
  "originCurrency": "0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14",
  "destinationCurrency": "0x4200000000000000000000000000000000000006",
  "amount": "1000000000000000", "tradeType": "EXACT_INPUT"
}' -w "\nHTTP:%{http_code}\n"
```

If this now succeeds (200), the design's ETH-in/WETH-out mapping (Task 5) may need to become WETH-in/WETH-out instead -- STOP and report the change before implementing Task 5.

- [ ] **Step 4: Confirm the status endpoint's shape and vocabulary**

```bash
curl -sS "https://api.testnets.relay.link/intents/status?requestId=<requestId from Step 1's response>" -w "\nHTTP:%{http_code}\n"
```

Expected: `{"status": "unknown"}` or similar for a never-broadcast request. Cross-check the full status vocabulary against `https://docs.relay.link/references/api/get-intents-status-v3` -- confirm `success` still means verified destination-side completion and the enum still includes (at minimum) `success`, `refund`, `failure`.

- [ ] **Step 5: Record findings**

If everything matches the design doc's §2 research, no code changes are needed -- proceed to Task 2. If anything drifted, update the fixture data used in Tasks 5-6's tests to match the live shape found here, and note the drift in your final report; do not silently build against stale assumptions.

---

## Task 2: Add `quote.Signer` and `quote.StatusChecker`

**Files:**
- Create: `go-api/internal/bridge/quote/execute.go`
- Create: `go-api/internal/bridge/quote/status.go`

**Interfaces:**
- Produces: `quote.TxEnvelope`, `quote.Signer`, `quote.ExternalState` (+ 5 constants), `quote.StatusRequest`, `quote.StatusResult`, `quote.StatusChecker` -- consumed by every later task.

- [ ] **Step 1: Write `execute.go`**

```go
package quote

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// TxEnvelope is an unsigned transaction description a Signer hands back to
// Executor. Producing this does NOT sign anything -- Executor is the sole
// caller of wallet.SignTx, for every provider (design doc §11).
type TxEnvelope struct {
	To      common.Address
	Value   *big.Int
	Data    []byte
	ChainID int64
}

// Signer is implemented by any provider whose fresh quotes Executor can
// turn into a signed transaction. BuildTransaction must be built entirely
// from freshQuote and must never call any signing function itself.
type Signer interface {
	Provider
	BuildTransaction(ctx context.Context, freshQuote Quote) (TxEnvelope, error)
}
```

- [ ] **Step 2: Write `status.go`**

```go
package quote

import "context"

// ExternalState is the shared, provider-agnostic vocabulary Reconciler
// operates on. Each provider's own richer status vocabulary is mapped down
// to one of these by that provider's own CheckStatus (design doc §14).
type ExternalState string

const (
	StatePending    ExternalState = "pending"
	StateFilled     ExternalState = "filled"
	StateRefunded   ExternalState = "refunded"
	StateReverted   ExternalState = "reverted"
	StateFillFailed ExternalState = "fill_failed"
)

// StatusRequest carries whichever identifier(s) a provider's own status API
// actually needs. Across uses OriginTxHash; Relay uses
// ProviderReferenceID. A provider ignores whichever field it doesn't need.
type StatusRequest struct {
	ProviderReferenceID string
	OriginTxHash        string
}

// StatusResult is the normalized outcome of one status check. RawStatus
// preserves the provider's own literal status word for observability, even
// though State collapses many raw values into one shared bucket.
type StatusResult struct {
	State             ExternalState
	RawStatus         string
	DestinationTxHash *string
}

// StatusChecker is implemented by any provider Reconciler can poll for a
// destination-side outcome.
type StatusChecker interface {
	Provider
	CheckStatus(ctx context.Context, req StatusRequest) (StatusResult, error)
}
```

- [ ] **Step 3: Verify it compiles**

Run: `cd go-api && go build ./internal/bridge/quote/...`
Expected: succeeds. No tests needed for plain type/interface declarations -- Task 3 and Task 6 exercise them for real.

- [ ] **Step 4: Commit**

```bash
git add go-api/internal/bridge/quote/execute.go go-api/internal/bridge/quote/status.go
git commit -m "feat(go-api): add quote.Signer and quote.StatusChecker interfaces"
```

---

## Task 3: Extract `across.Provider.BuildTransaction` and `CheckStatus`

This is a refactor of existing, already-correct Across logic -- zero new behavior, only relocated. Read `go-api/internal/bridge/across/execute.go` and `go-api/internal/worker/executor.go` in full before starting; this task moves logic between them.

**Files:**
- Modify: `go-api/internal/bridge/across/execute.go`
- Modify: `go-api/internal/bridge/across/provider.go`
- Modify: `go-api/internal/bridge/across/status.go`
- Create: `go-api/internal/bridge/across/provider_test.go` additions (extend the existing file)

**Interfaces:**
- Consumes: `quote.Signer`, `quote.StatusChecker` (Task 2).
- Produces: `across.Provider` now implements `quote.Signer` and `quote.StatusChecker` in addition to `quote.Provider`. New `across.Provider` fields: `WalletAddress`, `SpokePoolAddress`, `WETHOrigin`, `WETHDestination common.Address` (all public, settable after `NewProvider` construction -- `NewProvider(client, ttl)`'s existing signature and every existing call site stay unchanged).

- [ ] **Step 1: Read the current `BuildAndSignDepositV3Tx` and `DepositStatusByTxHash`**

Confirm the exact current signatures in `execute.go` and `status.go` before editing -- this task's code below assumes `DepositV3Params`'s field names (`Recipient`, `InputToken`, `OutputToken`, `InputAmount`, `OutputAmount`, `DestinationChainID`, `ExclusiveRelayer`, `QuoteTimestamp`, `FillDeadline`, `ExclusivityDeadline`) and `spokePoolABI`'s `Pack("depositV3", ...)` call are unchanged from what you read.

- [ ] **Step 2: Write the failing tests**

Add to `go-api/internal/bridge/across/provider_test.go` (the existing file from Task 6 of the Phase 8 plan):

```go
func TestProvider_BuildTransaction_ProducesExpectedEnvelope(t *testing.T) {
	p := &Provider{
		WalletAddress:    common.HexToAddress("0xAbC0000000000000000000000000000000000001"),
		SpokePoolAddress: common.HexToAddress("0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662"),
		WETHOrigin:       common.HexToAddress("0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14"),
		WETHDestination:  common.HexToAddress("0x4200000000000000000000000000000000000006"),
	}
	fresh := quote.Quote{
		ProviderName: "across", SourceChainID: 11155111, DestinationChainID: 84532,
		InputAmountBaseUnits: big.NewInt(1_000_000_000_000_000), OutputAmountBaseUnits: big.NewInt(997_592_172_330_233),
		RawProviderPayload: json.RawMessage(rawAcrossPayloadFixture),
	}

	env, err := p.BuildTransaction(context.Background(), fresh)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if env.To != p.SpokePoolAddress {
		t.Errorf("To = %s, want %s", env.To.Hex(), p.SpokePoolAddress.Hex())
	}
	if env.Value.Cmp(fresh.InputAmountBaseUnits) != 0 {
		t.Errorf("Value = %s, want %s", env.Value, fresh.InputAmountBaseUnits)
	}
	if env.ChainID != 11155111 {
		t.Errorf("ChainID = %d, want 11155111", env.ChainID)
	}
	if len(env.Data) == 0 {
		t.Error("Data must not be empty")
	}
}

func TestProvider_CheckStatus_MapsAcrossStatusesToSharedStates(t *testing.T) {
	cases := []struct {
		acrossStatus string
		want         quote.ExternalState
	}{
		{"filled", quote.StateFilled},
		{"expired", quote.StateRefunded},
		{"refunded", quote.StateRefunded},
		{"pending", quote.StatePending},
		{"slowFillRequested", quote.StatePending},
		{"some-future-unknown-status", quote.StatePending},
	}
	for _, c := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(fmt.Sprintf(`{"status":%q}`, c.acrossStatus)))
		}))
		defer srv.Close()
		p := &Provider{Client: NewClient(srv.URL)}
		result, err := p.CheckStatus(context.Background(), quote.StatusRequest{OriginTxHash: "0xabc"})
		if err != nil {
			t.Fatalf("status %q: unexpected error: %v", c.acrossStatus, err)
		}
		if result.State != c.want {
			t.Errorf("status %q: State = %q, want %q", c.acrossStatus, result.State, c.want)
		}
		if result.RawStatus != c.acrossStatus {
			t.Errorf("status %q: RawStatus = %q, want %q", c.acrossStatus, result.RawStatus, c.acrossStatus)
		}
	}
}

func TestProvider_CheckStatus_DepositNotFoundIsNonTerminal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error":"DepositNotFoundException"}`))
	}))
	defer srv.Close()
	p := &Provider{Client: NewClient(srv.URL)}
	_, err := p.CheckStatus(context.Background(), quote.StatusRequest{OriginTxHash: "0xabc"})
	if !errors.Is(err, ErrDepositNotFound) {
		t.Fatalf("expected ErrDepositNotFound, got %v", err)
	}
}
```

Add `rawAcrossPayloadFixture` (a `const` matching the shape `across.QuotePayload` marshals -- copy the exact JSON from Task 6's existing `rawAcrossPayload()` helper in `go-api/internal/worker/executor_test.go` if that helper already exists; otherwise construct one matching `QuotePayload{ExclusiveRelayer, QuoteTimestamp, FillDeadline, ExclusivityDeadline, SpokePoolAddress}`'s JSON tags).

- [ ] **Step 3: Run to verify it fails**

Run: `cd go-api && go test ./internal/bridge/across/... -run "TestProvider_BuildTransaction|TestProvider_CheckStatus" -v`
Expected: FAIL (`BuildTransaction`/`CheckStatus` undefined on `*Provider`, missing struct fields)

- [ ] **Step 4: Extend `across.Provider` and implement `BuildTransaction`**

In `provider.go`, extend the struct:

```go
type Provider struct {
	Client           *Client
	QuoteTTL         time.Duration
	WalletAddress    common.Address // set by cmd/worker/main.go; zero-valued and unused by cmd/server's registry-only instance
	SpokePoolAddress common.Address
	WETHOrigin       common.Address
	WETHDestination  common.Address
}
```

In `execute.go`, add (keep the existing `BuildAndSignDepositV3Tx` function exactly as-is for now -- it is unused after Task 9 rewires `Executor`, and removed in Task 9's cleanup, not this one, to keep this task's diff a pure addition):

```go
func (p *Provider) BuildTransaction(ctx context.Context, freshQuote quote.Quote) (quote.TxEnvelope, error) {
	payload, err := DecodeQuotePayload(freshQuote.RawProviderPayload)
	if err != nil {
		return quote.TxEnvelope{}, fmt.Errorf("across: decode quote payload: %w", err)
	}
	quoteTimestamp, err := strconv.ParseUint(payload.QuoteTimestamp, 10, 32)
	if err != nil {
		return quote.TxEnvelope{}, fmt.Errorf("across: parse quote timestamp %q: %w", payload.QuoteTimestamp, err)
	}
	fillDeadline, err := strconv.ParseUint(payload.FillDeadline, 10, 32)
	if err != nil {
		return quote.TxEnvelope{}, fmt.Errorf("across: parse fill deadline %q: %w", payload.FillDeadline, err)
	}

	calldata, err := spokePoolABI.Pack("depositV3",
		p.WalletAddress, p.WalletAddress, p.WETHOrigin, p.WETHDestination,
		freshQuote.InputAmountBaseUnits, freshQuote.OutputAmountBaseUnits, big.NewInt(freshQuote.DestinationChainID),
		common.HexToAddress(payload.ExclusiveRelayer), uint32(quoteTimestamp), uint32(fillDeadline), uint32(payload.ExclusivityDeadline),
		[]byte{},
	)
	if err != nil {
		return quote.TxEnvelope{}, fmt.Errorf("across: pack depositV3 calldata: %w", err)
	}

	return quote.TxEnvelope{
		To: p.SpokePoolAddress, Value: freshQuote.InputAmountBaseUnits, Data: calldata, ChainID: freshQuote.SourceChainID,
	}, nil
}
```

Add `"chainroute/go-api/internal/bridge/quote"` and `"strconv"` to `execute.go`'s imports if not already present.

- [ ] **Step 5: Implement `CheckStatus`**

In `status.go`, add:

```go
// acrossStatusToState maps Across's own status vocabulary onto the shared
// ExternalState enum -- a direct extraction of the mapping
// worker.Reconciler.checkAndUpdateOutcome already applied inline before
// this task, not new logic.
func acrossStatusToState(raw string) quote.ExternalState {
	switch raw {
	case "filled":
		return quote.StateFilled
	case "expired", "refunded":
		return quote.StateRefunded
	default:
		return quote.StatePending
	}
}

func (p *Provider) CheckStatus(ctx context.Context, req quote.StatusRequest) (quote.StatusResult, error) {
	resp, err := p.Client.DepositStatusByTxHash(ctx, /* originChainID */ 0, req.OriginTxHash)
	if err != nil {
		return quote.StatusResult{}, err
	}
	return quote.StatusResult{State: acrossStatusToState(resp.Status), RawStatus: resp.Status}, nil
}
```

**Stop and reconsider before finalizing**: `DepositStatusByTxHash`'s current signature takes `originChainID int64` as its second parameter -- `Provider` does not currently have an `OriginChainID` field, and passing `0` (as sketched above) is wrong; it will send `originChainId=0` in the live query. Add an `OriginChainID int64` field to `Provider` (set alongside the other new fields in cmd/worker/main.go, Task 8) and use `p.OriginChainID` instead of the placeholder `0` shown above. Update `TestProvider_CheckStatus_*`'s fixtures to set this field on the `Provider` literal too, and add an assertion (via a test server that inspects the request's query string) that `originChainId` is sent correctly -- do not leave this as a silent `0`.

Add `"chainroute/go-api/internal/bridge/quote"` to `status.go`'s imports.

- [ ] **Step 6: Run to verify it passes**

Run: `cd go-api && go test ./internal/bridge/across/... -v`
Expected: PASS -- every test in the package, old and new.

- [ ] **Step 7: Commit**

```bash
git add go-api/internal/bridge/across/
git commit -m "feat(go-api): add across.Provider.BuildTransaction/CheckStatus (quote.Signer/StatusChecker)"
```

---

## Task 4: Migration `0006`

**Files:**
- Create: `go-api/migrations/0006_multi_provider_bridge_routing.sql`

- [ ] **Step 1: Write the migration**

```sql
ALTER TABLE payment_executions
    RENAME COLUMN across_deposit_id TO provider_reference_id;

ALTER TABLE payment_executions
    ADD COLUMN raw_external_status TEXT NULL;

ALTER TABLE payment_executions
    DROP CONSTRAINT payment_executions_external_status_check,
    ADD CONSTRAINT payment_executions_external_status_check
        CHECK (external_status IN ('pending', 'filled', 'expired', 'refunded', 'reverted', 'fill_failed'));
```

- [ ] **Step 2: Apply and verify**

Against the local dev Postgres (`postgres://shivaniparimi@localhost:5432/chainroute?sslmode=disable`, migrations `0001`-`0005` already applied, per Phase 8's setup):

```bash
psql "postgres://shivaniparimi@localhost:5432/chainroute?sslmode=disable" -f go-api/migrations/0006_multi_provider_bridge_routing.sql
psql "postgres://shivaniparimi@localhost:5432/chainroute?sslmode=disable" -c "\d payment_executions"
```

Expected: `provider_reference_id` appears (not `across_deposit_id`); `raw_external_status` appears; the `CHECK` constraint's definition includes `fill_failed`.

- [ ] **Step 3: Commit**

```bash
git add go-api/migrations/0006_multi_provider_bridge_routing.sql
git commit -m "feat(go-api): migration 0006 -- generalize provider reference ID, add fill_failed status"
```

---

## Task 5: `payment.go` domain updates and postgres store rename

**Files:**
- Modify: `go-api/internal/payment/payment.go`
- Modify: `go-api/internal/postgres/execution_store.go`
- Modify: `go-api/internal/worker/executor_test.go`, `go-api/internal/worker/reconciler_test.go` (fakes referencing the renamed field)

**Interfaces:**
- Produces: `payment.Execution.ProviderReferenceID *string` (renamed from `AcrossDepositID`), `payment.Execution.RawExternalStatus *string` (new), `payment.ExternalStatusFillFailed` constant; `Store.PersistSignedExecution(ctx, executionID string, rawTx []byte, txHash string, providerReferenceID *string) error` (signature change).

- [ ] **Step 1: Rewrite `payment.go`'s `Execution` struct and `ExternalStatus` constants**

In `go-api/internal/payment/payment.go`, rename the field and add the two new pieces:

```go
const (
	ExternalStatusPending    ExternalStatus = "pending"
	ExternalStatusFilled     ExternalStatus = "filled"
	ExternalStatusExpired    ExternalStatus = "expired"
	ExternalStatusRefunded   ExternalStatus = "refunded"
	ExternalStatusReverted   ExternalStatus = "reverted"
	ExternalStatusFillFailed ExternalStatus = "fill_failed" // NEW -- Relay's "failure" (unsuccessful fill), distinct from reverted/refunded/expired
)
```

```go
type Execution struct {
	ID                  string
	PaymentID           string
	BridgeProvider      string
	OriginChainID       int64
	DestinationChainID  int64
	WalletAddress       string
	Nonce               int64
	SignedTxHash        *string
	RawSignedTx         []byte
	BroadcastAt         *time.Time
	ProviderReferenceID *string // renamed from AcrossDepositID -- provider-agnostic external reference (Relay's requestId; unused/nil for Across)
	ExternalStatus      ExternalStatus
	RawExternalStatus   *string // NEW -- the provider's own literal status string, for observability (design doc §16)
	ConfirmedAt         *time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
}
```

- [ ] **Step 2: Update `execution_store.go`**

Rename every `across_deposit_id`/`acrossDepositID` occurrence to `provider_reference_id`/`providerReferenceID`, and add `raw_external_status` to the `SELECT`/scan lists, in these three functions: `GetExecutionByPaymentID` (lines ~117-152), `ReconciliationCandidates` (lines ~261-306). Then change `PersistSignedExecution`'s signature and body:

```go
func (s *Store) PersistSignedExecution(ctx context.Context, executionID string, rawTx []byte, txHash string, providerReferenceID *string) error {
	if _, err := s.db.ExecContext(ctx, `
		UPDATE payment_executions SET raw_signed_tx = $2, signed_tx_hash = $3, provider_reference_id = $4, updated_at = now()
		WHERE id = $1
	`, executionID, rawTx, txHash, providerReferenceID); err != nil {
		return fmt.Errorf("persist signed execution: %w", err)
	}
	return nil
}
```

Also update `UpdateExecutionExternalStatus` to accept and persist the raw status alongside the normalized one:

```go
func (s *Store) UpdateExecutionExternalStatus(ctx context.Context, executionID string, status payment.ExternalStatus, rawStatus string, confirmedAt *sql.NullTime) error {
	if _, err := s.db.ExecContext(ctx, `
		UPDATE payment_executions SET external_status = $2, raw_external_status = $3, confirmed_at = $4, updated_at = now()
		WHERE id = $1
	`, executionID, string(status), rawStatus, confirmedAt); err != nil {
		return fmt.Errorf("update execution external status: %w", err)
	}
	return nil
}
```

- [ ] **Step 3: Fix every call site the signature changes broke**

`ExecutorStore`/`ReconcilerStore` interfaces in `go-api/internal/worker/executor.go`/`reconciler.go` declare `PersistSignedExecution`/`UpdateExecutionExternalStatus` -- update their signatures to match. Every fake implementing these interfaces in `executor_test.go`/`reconciler_test.go` must be updated too (add the new parameters; existing tests can pass `nil`/`""` for them since Task 3's `signAndBroadcastFresh` rewrite happens in Task 9, not here -- for now, just make everything compile with the old call sites passing the new parameters as `nil`/`""`).

Run: `cd go-api && go build ./... 2>&1` repeatedly, fixing each compile error, until it's clean. Do not use a broader refactor than required to make this compile -- Task 9 is where the actual new logic (populating `providerReferenceID` for real) lands.

- [ ] **Step 4: Run the full test suite to confirm zero behavioral regressions from the rename**

```bash
cd go-api && go build ./... && go vet ./... && go test ./...
export DATABASE_URL="postgres://shivaniparimi@localhost:5432/chainroute?sslmode=disable"
go test -tags=integration ./...
```

Expected: everything passes, unmodified in behavior (only names changed).

- [ ] **Step 5: Commit**

```bash
git add go-api/internal/payment/payment.go go-api/internal/postgres/execution_store.go go-api/internal/worker/executor.go go-api/internal/worker/executor_test.go go-api/internal/worker/reconciler.go go-api/internal/worker/reconciler_test.go
git commit -m "refactor(go-api): rename AcrossDepositID to ProviderReferenceID, add RawExternalStatus and ExternalStatusFillFailed"
```

---

## Task 6: `bridge/relay` -- client and quote normalization

This is the highest-risk new-code task. Read the design doc's §2 and §7 in full before starting.

**Files:**
- Create: `go-api/internal/bridge/relay/client.go`
- Create: `go-api/internal/bridge/relay/quote.go`
- Create: `go-api/internal/bridge/relay/quote_test.go`

**Interfaces:**
- Consumes: `quote.Provider`, `quote.Request`, `quote.Quote` (unchanged, Phase 8).
- Produces: `relay.NewClient(baseURL string) *Client`; `relay.NewProvider(client *Client, walletAddress common.Address, quoteTTL time.Duration) *Provider`; `Provider.Name() string` returns `"relay"`; `Provider.GetQuote(ctx, quote.Request) (quote.Quote, error)`.

- [ ] **Step 1: Write `client.go`** (mirrors `across/client.go`'s shape)

```go
package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	baseURL    string
	httpClient *http.Client
	APIKey     string // optional; environment-only, never logged (Task 1 confirmed testnet doesn't currently require it)
}

func NewClient(baseURL string) *Client {
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), httpClient: &http.Client{Timeout: 15 * time.Second}}
}

func (c *Client) post(ctx context.Context, path string, body any, out any) error {
	buf, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal request body for %s: %w", path, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(buf))
	if err != nil {
		return fmt.Errorf("build request for %s: %w", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request %s: %w", path, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response body from %s: %w", path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &APIError{Path: path, StatusCode: resp.StatusCode, Body: string(respBody)}
	}
	if err := json.Unmarshal(respBody, out); err != nil {
		return fmt.Errorf("decode response from %s: %w", path, err)
	}
	return nil
}

func (c *Client) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
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

// APIError is a non-2xx response, carrying the raw body so callers can
// pattern-match on Relay's own errorCode field without this package
// hardcoding every possible error shape.
type APIError struct {
	Path       string
	StatusCode int
	Body       string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("relay api %s returned %d: %s", e.Path, e.StatusCode, e.Body)
}
```

- [ ] **Step 2: Write the failing tests**

Create `go-api/internal/bridge/relay/quote_test.go`. `realQuoteFixture` below is captured verbatim from Task 1's live verification (adjust if Task 1 found drift):

```go
package relay

import (
	"context"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"chainroute/go-api/internal/bridge/quote"
)

const realQuoteFixture = `{"requestId":"0x1789582942e8e6519cfd826bc708bcf799f4705ca98482b0159b34b4297604ae","steps":[{"id":"deposit","action":"Confirm transaction in your wallet","description":"Depositing funds to the relayer to execute the swap for WETH","kind":"transaction","items":[{"status":"incomplete","data":{"from":"0x000000000000000000000000000000000000dEaD","to":"0x5feab8db4534f9f7e2669bb260c57a01ad1c12e3","data":"0xdeadbeef","value":"1000000000000000","chainId":11155111,"gas":"32432","maxFeePerGas":"1481105048","maxPriorityFeePerGas":"180155790"},"check":{"endpoint":"/intents/status?requestId=0x1789582942e8e6519cfd826bc708bcf799f4705ca98482b0159b34b4297604ae","method":"GET"}}],"requestId":"0x1789582942e8e6519cfd826bc708bcf799f4705ca98482b0159b34b4297604ae","depositAddress":""}],"fees":{},"details":{"operation":"swap","sender":"0x000000000000000000000000000000000000dEaD","recipient":"0x000000000000000000000000000000000000dEaD","currencyIn":{"currency":{"chainId":11155111,"address":"0x0000000000000000000000000000000000000000","symbol":"ETH","decimals":18},"amount":"1000000000000000"},"currencyOut":{"currency":{"chainId":84532,"address":"0x4200000000000000000000000000000000000006","symbol":"WETH","decimals":18},"amount":"991476614845915","minimumAmount":"960839987447177"},"timeEstimate":4},"protocol":{"v2":{"orderData":{"deadline":1790187742}}}}`

func TestGetQuote_HappyPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/quote" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.Write([]byte(realQuoteFixture))
	}))
	defer srv.Close()

	p := NewProvider(NewClient(srv.URL), common.HexToAddress("0x000000000000000000000000000000000000dEaD"), 0)
	q, err := p.GetQuote(context.Background(), quote.Request{
		SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
		AmountBaseUnits: big.NewInt(1_000_000_000_000_000),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q.ProviderName != "relay" {
		t.Errorf("ProviderName = %q, want relay", q.ProviderName)
	}
	if !q.Available {
		t.Error("expected Available=true")
	}
	// OutputAmountBaseUnits MUST be minimumAmount (960839987447177), not
	// expectedAmount (991476614845915) -- this is the economic-comparability
	// decision from design doc §7.
	if q.OutputAmountBaseUnits.String() != "960839987447177" {
		t.Errorf("OutputAmountBaseUnits = %s, want the minimumAmount 960839987447177, not the optimistic expectedAmount", q.OutputAmountBaseUnits)
	}
	wantFee := new(big.Int).Sub(big.NewInt(1_000_000_000_000_000), q.OutputAmountBaseUnits)
	if q.FeeBaseUnits.Cmp(wantFee) != 0 {
		t.Errorf("FeeBaseUnits = %s, want %s", q.FeeBaseUnits, wantFee)
	}
	if q.EstimatedFillTimeSec != 4 {
		t.Errorf("EstimatedFillTimeSec = %d, want 4", q.EstimatedFillTimeSec)
	}
}

func TestGetQuote_NoRoutesFoundReturnsUnavailableNotError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"message":"no routes found","errorCode":"NO_SWAP_ROUTES_FOUND","requestId":"0xabc"}`))
	}))
	defer srv.Close()

	p := NewProvider(NewClient(srv.URL), common.HexToAddress("0x000000000000000000000000000000000000dEaD"), 0)
	q, err := p.GetQuote(context.Background(), quote.Request{
		SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH", AmountBaseUnits: big.NewInt(1),
	})
	if err != nil {
		t.Fatalf("expected nil error for a no-route response, got %v", err)
	}
	if q.Available {
		t.Error("expected Available=false")
	}
}

func TestGetQuote_MultiStepResponseIsHardError(t *testing.T) {
	twoSteps := `{"requestId":"0xabc","steps":[{"id":"approve","kind":"transaction","items":[{"data":{"to":"0x1","data":"0x","value":"0","chainId":11155111}}]},{"id":"deposit","kind":"transaction","items":[{"data":{"to":"0x2","data":"0x","value":"1000000000000000","chainId":11155111}}]}],"details":{"currencyIn":{"currency":{"chainId":11155111,"address":"0x0000000000000000000000000000000000000000"},"amount":"1000000000000000"},"currencyOut":{"currency":{"chainId":84532,"address":"0x4200000000000000000000000000000000000006"},"minimumAmount":"1","amount":"1"},"timeEstimate":1}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(twoSteps))
	}))
	defer srv.Close()

	p := NewProvider(NewClient(srv.URL), common.HexToAddress("0x000000000000000000000000000000000000dEaD"), 0)
	_, err := p.GetQuote(context.Background(), quote.Request{
		SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH", AmountBaseUnits: big.NewInt(1_000_000_000_000_000),
	})
	if err == nil {
		t.Fatal("expected an error for a multi-step response -- Phase 9 only supports exactly one transaction step")
	}
}

func TestGetQuote_WrongEchoedChainOrAddressIsHardError(t *testing.T) {
	wrongChain := `{"requestId":"0xabc","steps":[{"id":"deposit","kind":"transaction","items":[{"data":{"to":"0x1","data":"0x","value":"1000000000000000","chainId":11155111}}]}],"details":{"currencyIn":{"currency":{"chainId":999999,"address":"0x0000000000000000000000000000000000000000"},"amount":"1000000000000000"},"currencyOut":{"currency":{"chainId":84532,"address":"0x4200000000000000000000000000000000000006"},"minimumAmount":"1","amount":"1"},"timeEstimate":1}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(wrongChain))
	}))
	defer srv.Close()

	p := NewProvider(NewClient(srv.URL), common.HexToAddress("0x000000000000000000000000000000000000dEaD"), 0)
	_, err := p.GetQuote(context.Background(), quote.Request{
		SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH", AmountBaseUnits: big.NewInt(1_000_000_000_000_000),
	})
	if err == nil {
		t.Fatal("expected an error when the echoed currencyIn.chainId doesn't match the request")
	}
}

func TestGetQuote_UnsupportedAssetIsError(t *testing.T) {
	p := NewProvider(NewClient("http://unused"), common.HexToAddress("0x0"), 0)
	_, err := p.GetQuote(context.Background(), quote.Request{
		SourceChainID: 11155111, DestinationChainID: 84532, Asset: "USDC", AmountBaseUnits: big.NewInt(1),
	})
	if err == nil {
		t.Fatal("expected an error for an unsupported asset")
	}
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `cd go-api && go test ./internal/bridge/relay/... -v`
Expected: FAIL (package doesn't compile -- `NewProvider`/`GetQuote` undefined)

- [ ] **Step 4: Write `quote.go`**

```go
package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"chainroute/go-api/internal/bridge/quote"
)

// wethAddressByChainID mirrors across.wethAddressByChainID -- confirmed
// live (Task 1) to be the same testnet addresses Across already uses.
var wethAddressByChainID = map[int64]string{
	11155111: "0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14",
	84532:    "0x4200000000000000000000000000000000000006",
}

const nativeAddress = "0x0000000000000000000000000000000000000000"

type quoteRequestBody struct {
	User                string `json:"user"`
	OriginChainID       int64  `json:"originChainId"`
	DestinationChainID  int64  `json:"destinationChainId"`
	OriginCurrency      string `json:"originCurrency"`
	DestinationCurrency string `json:"destinationCurrency"`
	Amount              string `json:"amount"`
	TradeType           string `json:"tradeType"`
}

type currencyInfo struct {
	Currency struct {
		ChainID int64  `json:"chainId"`
		Address string `json:"address"`
	} `json:"currency"`
	Amount        string `json:"amount"`
	MinimumAmount string `json:"minimumAmount"`
}

// quoteStepItemData is the transaction envelope Relay's response embeds --
// named (not anonymous) so it can be referenced from both quoteResponseBody
// and the helper below without repeating the field list twice.
type quoteStepItemData struct {
	To                   string `json:"to"`
	Data                 string `json:"data"`
	Value                string `json:"value"`
	ChainID              int64  `json:"chainId"`
	Gas                  string `json:"gas"`
	MaxFeePerGas         string `json:"maxFeePerGas"`
	MaxPriorityFeePerGas string `json:"maxPriorityFeePerGas"`
}

type quoteStepItem struct {
	Data quoteStepItemData `json:"data"`
}

type quoteStep struct {
	Kind  string          `json:"kind"`
	Items []quoteStepItem `json:"items"`
}

type quoteResponseBody struct {
	RequestID string      `json:"requestId"`
	Steps     []quoteStep `json:"steps"`
	Details   struct {
		CurrencyIn   currencyInfo `json:"currencyIn"`
		CurrencyOut  currencyInfo `json:"currencyOut"`
		TimeEstimate int64        `json:"timeEstimate"`
		Recipient    string       `json:"recipient"`
	} `json:"details"`
	Protocol struct {
		V2 struct {
			OrderData struct {
				Deadline int64 `json:"deadline"`
			} `json:"orderData"`
		} `json:"v2"`
	} `json:"protocol"`
}

// errorResponseBody is what a non-2xx /quote response's body actually
// contains (verified live, Task 1) -- distinct from quoteResponseBody.
type errorResponseBody struct {
	Message   string `json:"message"`
	ErrorCode string `json:"errorCode"`
	RequestID string `json:"requestId"`
}

// QuotePayload is exactly what Provider.GetQuote marshals into
// quote.Quote.RawProviderPayload, and what Provider.BuildTransaction
// (Task 7) unmarshals back.
type QuotePayload struct {
	To                   string `json:"to"`
	Data                 string `json:"data"`
	Value                string `json:"value"`
	ChainID              int64  `json:"chainId"`
	RequestID            string `json:"requestId"`
	Deadline             int64  `json:"deadline"`
	Recipient            string `json:"recipient"`
}

// Provider implements quote.Provider, quote.Signer, and quote.StatusChecker.
type Provider struct {
	Client        *Client
	WalletAddress common.Address
	QuoteTTL      time.Duration
}

func NewProvider(client *Client, walletAddress common.Address, quoteTTL time.Duration) *Provider {
	return &Provider{Client: client, WalletAddress: walletAddress, QuoteTTL: quoteTTL}
}

func (p *Provider) Name() string { return "relay" }

func (p *Provider) GetQuote(ctx context.Context, req quote.Request) (quote.Quote, error) {
	if req.Asset != "WETH" {
		return quote.Quote{}, fmt.Errorf("relay provider: unsupported asset %q (only WETH is supported)", req.Asset)
	}
	destAddr, ok := wethAddressByChainID[req.DestinationChainID]
	if !ok {
		return quote.Quote{}, fmt.Errorf("relay provider: unsupported destination chain %d", req.DestinationChainID)
	}
	if _, ok := wethAddressByChainID[req.SourceChainID]; !ok {
		return quote.Quote{}, fmt.Errorf("relay provider: unsupported source chain %d", req.SourceChainID)
	}

	now := time.Now().UTC()
	var resp quoteResponseBody
	err := p.Client.post(ctx, "/quote", quoteRequestBody{
		User: p.WalletAddress.Hex(), OriginChainID: req.SourceChainID, DestinationChainID: req.DestinationChainID,
		OriginCurrency: nativeAddress, DestinationCurrency: destAddr, // ETH-in/WETH-out -- design doc §7
		Amount: req.AmountBaseUnits.String(), TradeType: "EXACT_INPUT",
	}, &resp)
	if err != nil {
		if isNoRoutesFound(err) {
			return quote.Quote{ProviderName: p.Name(), SourceChainID: req.SourceChainID, DestinationChainID: req.DestinationChainID,
				Asset: req.Asset, Available: false, QuotedAt: now, ExpiresAt: now.Add(p.QuoteTTL)}, nil
		}
		return quote.Quote{}, fmt.Errorf("relay provider: get quote: %w", err)
	}

	if len(resp.Steps) != 1 || resp.Steps[0].Kind != "transaction" || len(resp.Steps[0].Items) != 1 {
		kind := ""
		if len(resp.Steps) > 0 {
			kind = resp.Steps[0].Kind
		}
		return quote.Quote{}, fmt.Errorf("relay provider: expected exactly one transaction step, got %d steps (kind[0]=%q) -- refusing an unsupported multi-step execution model", len(resp.Steps), kind)
	}
	item := resp.Steps[0].Items[0].Data

	if resp.Details.CurrencyIn.Currency.ChainID != req.SourceChainID || resp.Details.CurrencyIn.Currency.Address != nativeAddress {
		return quote.Quote{}, fmt.Errorf("relay provider: response currencyIn %+v does not match request", resp.Details.CurrencyIn)
	}
	if resp.Details.CurrencyOut.Currency.ChainID != req.DestinationChainID || resp.Details.CurrencyOut.Currency.Address != destAddr {
		return quote.Quote{}, fmt.Errorf("relay provider: response currencyOut %+v does not match request", resp.Details.CurrencyOut)
	}
	if item.ChainID != req.SourceChainID {
		return quote.Quote{}, fmt.Errorf("relay provider: transaction step chainId %d does not match origin chain %d", item.ChainID, req.SourceChainID)
	}

	outputAmount, ok := new(big.Int).SetString(resp.Details.CurrencyOut.MinimumAmount, 10)
	if !ok {
		return quote.Quote{}, fmt.Errorf("relay provider: minimumAmount %q is not a valid integer", resp.Details.CurrencyOut.MinimumAmount)
	}
	feeAmount := new(big.Int).Sub(req.AmountBaseUnits, outputAmount)

	payload := QuotePayload{To: item.To, Data: item.Data, Value: item.Value, ChainID: item.ChainID,
		RequestID: resp.RequestID, Deadline: resp.Protocol.V2.OrderData.Deadline, Recipient: resp.Details.Recipient}
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return quote.Quote{}, fmt.Errorf("relay provider: marshal raw payload: %w", err)
	}

	return quote.Quote{
		ProviderName: p.Name(), SourceChainID: req.SourceChainID, DestinationChainID: req.DestinationChainID,
		Asset: req.Asset, InputAmountBaseUnits: req.AmountBaseUnits, OutputAmountBaseUnits: outputAmount,
		FeeBaseUnits: feeAmount, EstimatedFillTimeSec: resp.Details.TimeEstimate,
		Available: true, QuotedAt: now, ExpiresAt: now.Add(p.QuoteTTL), RawProviderPayload: rawPayload,
	}, nil
}

// isNoRoutesFound reports whether err is a Relay APIError whose body
// carries errorCode "NO_SWAP_ROUTES_FOUND" -- the signal that a route
// genuinely doesn't exist right now, distinct from a transport/API failure.
func isNoRoutesFound(err error) bool {
	apiErr, ok := err.(*APIError)
	if !ok {
		return false
	}
	var body errorResponseBody
	if jsonErr := json.Unmarshal([]byte(apiErr.Body), &body); jsonErr != nil {
		return false
	}
	return body.ErrorCode == "NO_SWAP_ROUTES_FOUND"
}
```

- [ ] **Step 5: Run to verify it passes**

Run: `cd go-api && go test ./internal/bridge/relay/... -v`
Expected: PASS -- all five tests.

- [ ] **Step 6: Commit**

```bash
git add go-api/internal/bridge/relay/client.go go-api/internal/bridge/relay/quote.go go-api/internal/bridge/relay/quote_test.go
git commit -m "feat(go-api): add bridge/relay client and quote.Provider implementation"
```

---

## Task 7: `bridge/relay` -- `BuildTransaction` and `CheckStatus`

**Files:**
- Create: `go-api/internal/bridge/relay/execute.go`
- Create: `go-api/internal/bridge/relay/status.go`
- Create: `go-api/internal/bridge/relay/execute_test.go`
- Create: `go-api/internal/bridge/relay/status_test.go`

**Interfaces:**
- Produces: `Provider.BuildTransaction(ctx, quote.Quote) (quote.TxEnvelope, error)`, `Provider.CheckStatus(ctx, quote.StatusRequest) (quote.StatusResult, error)`.

- [ ] **Step 1: Write the failing tests**

`go-api/internal/bridge/relay/execute_test.go`:

```go
package relay

import (
	"context"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"chainroute/go-api/internal/bridge/quote"
)

func TestBuildTransaction_DecodesEnvelopeFromRawPayload(t *testing.T) {
	wallet := common.HexToAddress("0x000000000000000000000000000000000000dEaD")
	payload := QuotePayload{To: "0x5feaB8db4534f9F7e2669bb260C57A01aD1c12E3", Data: "0xdeadbeef", Value: "1000000000000000", ChainID: 11155111, Recipient: wallet.Hex()}
	raw, _ := json.Marshal(payload)
	fresh := quote.Quote{RawProviderPayload: raw, InputAmountBaseUnits: big.NewInt(1_000_000_000_000_000)}

	p := &Provider{WalletAddress: wallet}
	env, err := p.BuildTransaction(context.Background(), fresh)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if env.To != common.HexToAddress(payload.To) {
		t.Errorf("To = %s, want %s", env.To.Hex(), payload.To)
	}
	if env.Value.String() != "1000000000000000" {
		t.Errorf("Value = %s, want 1000000000000000", env.Value)
	}
	if env.ChainID != 11155111 {
		t.Errorf("ChainID = %d, want 11155111", env.ChainID)
	}
	if len(env.Data) != 4 { // 0xdeadbeef = 4 bytes
		t.Errorf("Data length = %d, want 4", len(env.Data))
	}
}

func TestBuildTransaction_RecipientMismatchIsHardError(t *testing.T) {
	wallet := common.HexToAddress("0x000000000000000000000000000000000000dEaD")
	otherRecipient := common.HexToAddress("0x1111111111111111111111111111111111111111")
	payload := QuotePayload{To: "0x5feaB8db4534f9F7e2669bb260C57A01aD1c12E3", Data: "0xdead", Value: "1000000000000000", ChainID: 11155111, Recipient: otherRecipient.Hex()}
	raw, _ := json.Marshal(payload)
	fresh := quote.Quote{RawProviderPayload: raw, InputAmountBaseUnits: big.NewInt(1_000_000_000_000_000)}

	p := &Provider{WalletAddress: wallet}
	_, err := p.BuildTransaction(context.Background(), fresh)
	if err == nil {
		t.Fatal("expected an error when the quoted recipient does not match this executor's wallet")
	}
}
```

`go-api/internal/bridge/relay/status_test.go`:

```go
package relay

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"chainroute/go-api/internal/bridge/quote"
)

func TestCheckStatus_MapsRelayStatusesToSharedStates(t *testing.T) {
	cases := []struct {
		relayStatus string
		want        quote.ExternalState
	}{
		{"waiting", quote.StatePending},
		{"depositing", quote.StatePending},
		{"pending", quote.StatePending},
		{"submitted", quote.StatePending},
		{"delayed", quote.StatePending},
		{"success", quote.StateFilled},
		{"refund", quote.StateRefunded},
		{"failure", quote.StateFillFailed},
		{"unknown", quote.StatePending},
		{"some-future-status", quote.StatePending},
	}
	for _, c := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`{"status":"` + c.relayStatus + `"}`))
		}))
		p := &Provider{Client: NewClient(srv.URL)}
		result, err := p.CheckStatus(context.Background(), quote.StatusRequest{ProviderReferenceID: "0xabc"})
		srv.Close()
		if err != nil {
			t.Fatalf("status %q: unexpected error: %v", c.relayStatus, err)
		}
		if result.State != c.want {
			t.Errorf("status %q: State = %q, want %q", c.relayStatus, result.State, c.want)
		}
		if result.RawStatus != c.relayStatus {
			t.Errorf("status %q: RawStatus = %q, want %q", c.relayStatus, result.RawStatus, c.relayStatus)
		}
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd go-api && go test ./internal/bridge/relay/... -run "TestBuildTransaction|TestCheckStatus" -v`
Expected: FAIL (`BuildTransaction`/`CheckStatus` undefined)

- [ ] **Step 3: Write `execute.go`**

```go
package relay

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"

	"chainroute/go-api/internal/bridge/quote"
)

func (p *Provider) BuildTransaction(ctx context.Context, freshQuote quote.Quote) (quote.TxEnvelope, error) {
	var payload QuotePayload
	if err := json.Unmarshal(freshQuote.RawProviderPayload, &payload); err != nil {
		return quote.TxEnvelope{}, fmt.Errorf("relay: decode quote payload: %w", err)
	}

	if payload.Recipient != "" && common.HexToAddress(payload.Recipient) != p.WalletAddress {
		return quote.TxEnvelope{}, fmt.Errorf("relay: quoted recipient %s does not match this wallet %s -- refusing to build a transaction for someone else's funds", payload.Recipient, p.WalletAddress.Hex())
	}

	value, ok := new(big.Int).SetString(payload.Value, 10)
	if !ok {
		return quote.TxEnvelope{}, fmt.Errorf("relay: value %q is not a valid integer", payload.Value)
	}
	dataHex := strings.TrimPrefix(payload.Data, "0x")
	if len(dataHex)%2 != 0 {
		dataHex = "0" + dataHex
	}
	data, err := hex.DecodeString(dataHex)
	if err != nil {
		return quote.TxEnvelope{}, fmt.Errorf("relay: data %q is not valid hex: %w", payload.Data, err)
	}

	return quote.TxEnvelope{To: common.HexToAddress(payload.To), Value: value, Data: data, ChainID: payload.ChainID}, nil
}
```

- [ ] **Step 4: Write `status.go`**

```go
package relay

import (
	"context"
	"fmt"

	"chainroute/go-api/internal/bridge/quote"
)

type intentStatusResponse struct {
	Status     string   `json:"status"`
	TxHashes   []string `json:"txHashes"`
	InTxHashes []string `json:"inTxHashes"`
}

// relayStatusToState maps Relay's eight-value vocabulary (verified live,
// design doc §2) onto the shared ExternalState enum.
func relayStatusToState(raw string) quote.ExternalState {
	switch raw {
	case "success":
		return quote.StateFilled
	case "refund":
		return quote.StateRefunded
	case "failure":
		return quote.StateFillFailed
	default: // waiting, depositing, pending, submitted, delayed, unknown, or anything unrecognized
		return quote.StatePending
	}
}

func (p *Provider) CheckStatus(ctx context.Context, req quote.StatusRequest) (quote.StatusResult, error) {
	if req.ProviderReferenceID == "" {
		return quote.StatusResult{}, fmt.Errorf("relay: CheckStatus requires a ProviderReferenceID (requestId)")
	}
	var resp intentStatusResponse
	if err := p.Client.get(ctx, "/intents/status?requestId="+req.ProviderReferenceID, &resp); err != nil {
		return quote.StatusResult{}, err
	}
	result := quote.StatusResult{State: relayStatusToState(resp.Status), RawStatus: resp.Status}
	if len(resp.TxHashes) > 0 {
		result.DestinationTxHash = &resp.TxHashes[0]
	}
	return result, nil
}
```

- [ ] **Step 5: Run to verify it passes**

Run: `cd go-api && go test ./internal/bridge/relay/... -v`
Expected: PASS -- every test in the package.

- [ ] **Step 6: Commit**

```bash
git add go-api/internal/bridge/relay/execute.go go-api/internal/bridge/relay/status.go go-api/internal/bridge/relay/execute_test.go go-api/internal/bridge/relay/status_test.go
git commit -m "feat(go-api): add relay.Provider.BuildTransaction/CheckStatus"
```

---

## Task 8: Concurrent quote aggregation in `PostPayments`

Read `go-api/internal/handler/payments.go`'s current testnet-mode block in full before editing (the `for _, p := range providers { ... }` loop that aborts on first error).

**Files:**
- Modify: `go-api/internal/handler/payments.go`
- Modify: `go-api/internal/handler/payments_test.go`

**Interfaces:**
- Consumes: `quote.Provider` (unchanged).
- Produces: no new exported symbols -- purely a control-flow rewrite inside `PostPayments`.

- [ ] **Step 1: Write the failing tests**

Add to `payments_test.go` (reuse the existing `fakeQuoteProvider` type from Phase 8's tests; extend it with a `delay time.Duration` field if not present, to test ordering independent of completion timing):

```go
func TestPostPayments_TestnetMode_BothProvidersHealthy_RoutesOverBoth(t *testing.T) {
	registry := quote.NewRegistry()
	key := quote.RouteKey{SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH"}
	registry.Register(key, &fakeQuoteProvider{name: "across", quote: quote.Quote{ProviderName: "across", Available: true, FeeBaseUnits: big.NewInt(200), OutputAmountBaseUnits: big.NewInt(999_999_999_999_800), RawProviderPayload: json.RawMessage(`{}`)}})
	registry.Register(key, &fakeQuoteProvider{name: "relay", quote: quote.Quote{ProviderName: "relay", Available: true, FeeBaseUnits: big.NewInt(100), OutputAmountBaseUnits: big.NewInt(999_999_999_999_900), RawProviderPayload: json.RawMessage(`{}`)}})

	fakeClient := &fakeRoutingClient{captureRequest: true, response: &routingv1.FindRouteResponse{RouteFound: true, Hops: []*routingv1.RouteHop{{FromChain: routingv1.Chain_CHAIN_ETHEREUM, ToChain: routingv1.Chain_CHAIN_BASE, BridgeName: "relay", Fee: 0.0001}}}}
	h := &Handler{Client: fakeClient, Store: &fakePaymentStore{}, BlockchainEnv: "testnet", QuoteRegistry: registry}

	req := httptest.NewRequest("POST", "/payments", strings.NewReader(`{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`))
	req.Header.Set("Idempotency-Key", "both-healthy")
	w := httptest.NewRecorder()
	h.PostPayments(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if len(fakeClient.lastRequest.GetCandidateEdges()) != 2 {
		t.Fatalf("expected 2 candidate edges, got %d", len(fakeClient.lastRequest.GetCandidateEdges()))
	}
	// Registration order (§9's tie-break basis): across registered first, so its edge must appear first.
	if fakeClient.lastRequest.GetCandidateEdges()[0].GetBridgeName() != "across" {
		t.Errorf("edge[0].bridge_name = %q, want across (registration order)", fakeClient.lastRequest.GetCandidateEdges()[0].GetBridgeName())
	}
	if fakeClient.lastRequest.GetCandidateEdges()[1].GetBridgeName() != "relay" {
		t.Errorf("edge[1].bridge_name = %q, want relay (registration order)", fakeClient.lastRequest.GetCandidateEdges()[1].GetBridgeName())
	}
}

func TestPostPayments_TestnetMode_OneProviderErrors_RoutesOverTheOther(t *testing.T) {
	registry := quote.NewRegistry()
	key := quote.RouteKey{SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH"}
	registry.Register(key, &fakeQuoteProvider{name: "across", err: errors.New("connection refused")})
	registry.Register(key, &fakeQuoteProvider{name: "relay", quote: quote.Quote{ProviderName: "relay", Available: true, FeeBaseUnits: big.NewInt(100), OutputAmountBaseUnits: big.NewInt(999_999_999_999_900), RawProviderPayload: json.RawMessage(`{}`)}})

	fakeClient := &fakeRoutingClient{response: &routingv1.FindRouteResponse{RouteFound: true, Hops: []*routingv1.RouteHop{{FromChain: routingv1.Chain_CHAIN_ETHEREUM, ToChain: routingv1.Chain_CHAIN_BASE, BridgeName: "relay", Fee: 0.0001}}}}
	h := &Handler{Client: fakeClient, Store: &fakePaymentStore{}, BlockchainEnv: "testnet", QuoteRegistry: registry}

	req := httptest.NewRequest("POST", "/payments", strings.NewReader(`{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`))
	req.Header.Set("Idempotency-Key", "one-errors")
	w := httptest.NewRecorder()
	h.PostPayments(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 (routing over the healthy provider), got %d, body = %s", w.Code, w.Body.String())
	}
}

func TestPostPayments_TestnetMode_BothProvidersError_Returns503(t *testing.T) {
	registry := quote.NewRegistry()
	key := quote.RouteKey{SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH"}
	registry.Register(key, &fakeQuoteProvider{name: "across", err: errors.New("connection refused")})
	registry.Register(key, &fakeQuoteProvider{name: "relay", err: errors.New("timeout")})

	h := &Handler{Client: &fakeRoutingClient{}, Store: &fakePaymentStore{}, BlockchainEnv: "testnet", QuoteRegistry: registry}
	req := httptest.NewRequest("POST", "/payments", strings.NewReader(`{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`))
	req.Header.Set("Idempotency-Key", "both-error")
	w := httptest.NewRecorder()
	h.PostPayments(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body = %s", w.Code, w.Body.String())
	}
}

func TestPostPayments_TestnetMode_BothUnavailable_Returns422(t *testing.T) {
	registry := quote.NewRegistry()
	key := quote.RouteKey{SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH"}
	registry.Register(key, &fakeQuoteProvider{name: "across", quote: quote.Quote{ProviderName: "across", Available: false}})
	registry.Register(key, &fakeQuoteProvider{name: "relay", quote: quote.Quote{ProviderName: "relay", Available: false}})

	h := &Handler{Client: &fakeRoutingClient{}, Store: &fakePaymentStore{}, BlockchainEnv: "testnet", QuoteRegistry: registry}
	req := httptest.NewRequest("POST", "/payments", strings.NewReader(`{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`))
	req.Header.Set("Idempotency-Key", "both-unavailable")
	w := httptest.NewRecorder()
	h.PostPayments(w, req)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body = %s", w.Code, w.Body.String())
	}
}
```

If `fakeQuoteProvider` doesn't currently support a `delay` field for simulating varying response times, add one and have it `time.Sleep(delay)` before returning, then add one more test, `TestPostPayments_TestnetMode_OrderingIndependentOfCompletionTime`, where the SECOND-registered provider (`relay`) has `delay: 0` and the FIRST-registered (`across`) has `delay: 50*time.Millisecond` -- assert the resulting `candidateEdges[0].bridge_name` is still `"across"` (registration order), proving the aggregation doesn't reorder by completion time even when the first-registered provider is slower.

- [ ] **Step 2: Run to verify it fails**

Run: `cd go-api && go test ./internal/handler/... -run "TestPostPayments_TestnetMode_Both|TestPostPayments_TestnetMode_One" -v`
Expected: FAIL or behave incorrectly against the OLD sequential-abort logic (the "both providers error" test would currently return 503 too by accident on the first error, but "one provider errors, route over the other" will FAIL against today's code, since today's loop aborts entirely on the first error).

- [ ] **Step 3: Implement**

Replace the existing sequential `for _, p := range providers { q, err := p.GetQuote(...); if err != nil { ...; return } ... }` loop in `PostPayments` with:

```go
	type quoteResult struct {
		provider quote.Provider
		q        quote.Quote
		err      error
	}

	quoteCtx, quoteCancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer quoteCancel()
	results := make([]quoteResult, len(providers))
	var wg sync.WaitGroup
	for i, p := range providers {
		wg.Add(1)
		go func(i int, p quote.Provider) {
			defer wg.Done()
			q, err := p.GetQuote(quoteCtx, quote.Request{
				SourceChainID: originChainID, DestinationChainID: destChainID, Asset: bridgedAsset, AmountBaseUnits: amountWei,
			})
			results[i] = quoteResult{provider: p, q: q, err: err}
		}(i, p)
	}
	wg.Wait()

	anySucceeded, anyAvailable := false, false
	for _, res := range results { // index order, never completion order -- keeps the C++ tie-break deterministic
		if res.err != nil {
			log.Printf("WARNING: quote provider %s failed: %v", res.provider.Name(), res.err)
			continue
		}
		anySucceeded = true
		if !res.q.Available {
			continue
		}
		anyAvailable = true
		feeDecimal := money.BaseUnitsToDecimal(res.q.FeeBaseUnits, 18)
		feeFloat, _ := strconv.ParseFloat(feeDecimal, 64)
		candidateEdges = append(candidateEdges, &routingv1.CandidateEdge{
			BridgeName: res.provider.Name(), Fee: feeFloat, LatencyMs: float64(res.q.EstimatedFillTimeSec) * 1000,
			Liquidity: amountForRouting, Reliability: 1.0,
		})
		quotesByBridgeName[res.provider.Name()] = res.q
	}
	if !anySucceeded {
		writeError(w, http.StatusServiceUnavailable, "bridge quote providers unavailable")
		return
	}
	if !anyAvailable {
		writeError(w, http.StatusUnprocessableEntity, "no route available for the requested payment")
		return
	}
```

Add `"sync"` to the file's imports if not already present.

- [ ] **Step 4: Run to verify it passes**

Run: `cd go-api && go test ./internal/handler/... -v`
Expected: PASS -- every handler test, old and new.

- [ ] **Step 5: Commit**

```bash
git add go-api/internal/handler/payments.go go-api/internal/handler/payments_test.go
git commit -m "feat(go-api): concurrent, failure-isolated multi-provider quote aggregation"
```

---

## Task 9: Generalize `Executor`

The highest-stakes task in this plan -- mirrors Phase 8's Task 11 in risk profile. Read the CURRENT `go-api/internal/worker/executor.go` in full before editing (it has already been substantially rewritten by Phase 8; do not assume the code shown in the design doc's §11/§12 sketches is copy-pasteable verbatim -- it is illustrative, not the literal diff).

**Files:**
- Modify: `go-api/internal/worker/executor.go`
- Modify: `go-api/internal/worker/executor_test.go`

**Interfaces:**
- Consumes: `quote.Signer`, `quote.TxEnvelope` (Task 2); `across.Provider` implementing `Signer` (Task 3); `relay.Provider` implementing `Signer` (Task 7); `Store.PersistSignedExecution(ctx, id, rawTx, txHash, providerReferenceID *string)` (Task 5).
- Produces: `Executor.Signers map[string]quote.Signer`, `Executor.ExpectedContractByProvider map[string]common.Address` -- consumed by Task 11 (wiring).

- [ ] **Step 1: Remove now-dead fields, add new ones**

Remove from the `Executor` struct: `Across *across.Client`, `BridgeProvider string`, `SpokePoolAddress`, `WETHOrigin`, `WETHDestination common.Address` (all superseded -- `Across` is unused once `signAndBroadcastFresh` is rewritten below; `BridgeProvider` was a bug, see Step 3; the three addresses moved onto `across.Provider` itself in Task 3).

Add:

```go
type Executor struct {
	Store                      ExecutorStore
	Wallet                     *evm.Wallet
	OriginClient               ExecutorEthClient
	QuoteProviders             map[string]quote.Provider
	Signers                    map[string]quote.Signer
	ExpectedContractByProvider map[string]common.Address // independently configured -- NEVER derived from a Signer instance (design doc §12)
	MaxFeeSlippageBps          int64
	OriginChainID              int64
	DestChainID                int64
	MaxAmountWei               *big.Int
}
```

- [ ] **Step 2: Write the failing tests**

Add to `executor_test.go`:

```go
func TestExecuteTestnetPayment_WrongEnvelopeChainIDIsHardError(t *testing.T) {
	store := &fakeExecutorStore{
		quoteFound: true,
		quoteRow:   payment.Quote{Provider: "across", OriginChainID: 11155111, DestinationChainID: 84532, FeeAmount: "100000000000", ExpiresAt: time.Now().Add(time.Hour)},
		tryCreateCreated: true, tryCreateExec: payment.Execution{ID: "exec-badchain", PaymentID: "pay-badchain", Nonce: 1},
		pmt: payment.Payment{ID: "pay-badchain", Amount: "0.001"}, pmtFound: true,
	}
	ethClient := &fakeExecutorEthClient{}
	e := newTestExecutor(t, store, ethClient)
	e.Signers = map[string]quote.Signer{"across": &fakeSigner{envelope: quote.TxEnvelope{To: e.ExpectedContractByProvider["across"], Value: big.NewInt(1_000_000_000_000_000), ChainID: 999999}}}
	e.QuoteProviders = map[string]quote.Provider{"across": &fakeExecutorQuoteProvider{quote: quote.Quote{ProviderName: "across", Available: true, FeeBaseUnits: big.NewInt(90_000_000_000), OutputAmountBaseUnits: big.NewInt(910_000_000_000_000), InputAmountBaseUnits: big.NewInt(1_000_000_000_000_000)}}}

	if err := e.ExecuteTestnetPayment(context.Background(), "pay-badchain"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ethClient.sendCalled {
		t.Fatal("must never sign/broadcast an envelope whose ChainID doesn't match e.OriginChainID")
	}
}

func TestExecuteTestnetPayment_WrongEnvelopeTargetIsHardError(t *testing.T) {
	// Same setup, but envelope.To is some other address than
	// e.ExpectedContractByProvider["across"] -- assert SendTransaction never called.
}

func TestExecuteTestnetPayment_WrongEnvelopeValueIsHardError(t *testing.T) {
	// Same setup, but envelope.Value != the fresh quote's InputAmountBaseUnits.
}

func TestExecuteTestnetPayment_UnknownSignerIsHardError(t *testing.T) {
	// quoteRow.Provider = "relay", but e.Signers only has "across" configured.
	// Assert a hard Go error is returned and SendTransaction is never called.
}

func TestExecuteTestnetPayment_RelayShapedEnvelopePassesValidationAndSigns(t *testing.T) {
	// Full happy-path test using a fakeSigner shaped like relay.Provider
	// would produce (a real-looking To/Value/ChainID matching what
	// e.ExpectedContractByProvider["relay"] and e.OriginChainID expect) --
	// assert the full sign/persist/broadcast/submit sequence completes,
	// proving the envelope check is genuinely provider-agnostic, not
	// secretly Across-shaped.
}
```

Write a `fakeSigner` type implementing `quote.Signer` (embeds `quote.Provider`'s two methods plus `BuildTransaction` returning a pre-configured `quote.TxEnvelope`/error) in `executor_test.go`, following the existing `fakeExecutorQuoteProvider` pattern. Fill in the two elided test bodies above completely (do not leave them as comments in the actual test file -- this plan omits their full bodies only because they're structurally identical to `TestExecuteTestnetPayment_WrongEnvelopeChainIDIsHardError` with one field changed).

- [ ] **Step 3: Run to verify it fails**

Run: `cd go-api && go test ./internal/worker/... -v 2>&1 | head -50`
Expected: FAIL (compile errors -- `Executor.Signers`/`ExpectedContractByProvider` undefined; every pre-existing test that constructs an `Executor{...}` literal with the now-removed fields also fails to compile)

- [ ] **Step 4: Rewrite `signAndBroadcastFresh` and add the envelope check**

Replace the body of `signAndBroadcastFresh` (currently: decode Across payload, parse timestamps, call `across.BuildAndSignDepositV3Tx` directly) with:

```go
func (e *Executor) signAndBroadcastFresh(ctx context.Context, exec payment.Execution, quoteRow payment.Quote, freshQuote quote.Quote) error {
	signer, ok := e.Signers[quoteRow.Provider]
	if !ok {
		return fmt.Errorf("execution %s uses provider %q, which this worker has no configured signer for", exec.ID, quoteRow.Provider)
	}
	envelope, err := signer.BuildTransaction(ctx, freshQuote)
	if err != nil {
		return fmt.Errorf("build transaction for execution %s: %w", exec.ID, err)
	}
	if err := e.validateEnvelope(envelope, quoteRow, freshQuote); err != nil {
		return fmt.Errorf("execution %s: %w", exec.ID, err)
	}

	gasPrice, err := e.OriginClient.SuggestGasPrice(ctx)
	if err != nil {
		return fmt.Errorf("suggest gas price: %w", err)
	}
	const fallbackGasLimit = 500_000
	gasLimit, err := e.OriginClient.EstimateGas(ctx, gethereum.CallMsg{From: e.Wallet.Address, To: &envelope.To, Value: envelope.Value, Data: envelope.Data})
	if err != nil {
		gasLimit = fallbackGasLimit
	}
	unsignedTx := types.NewTx(&types.LegacyTx{Nonce: uint64(exec.Nonce), To: &envelope.To, Value: envelope.Value, Gas: gasLimit, GasPrice: gasPrice, Data: envelope.Data})
	signedTx, err := e.Wallet.SignTx(unsignedTx, big.NewInt(envelope.ChainID))
	if err != nil {
		return fmt.Errorf("sign tx for execution %s: %w", exec.ID, err)
	}

	rawTx, err := signedTx.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal signed tx: %w", err)
	}
	hash := signedTx.Hash().Hex()
	providerReferenceID := extractProviderReferenceID(freshQuote) // Task 10 note below
	if err := e.Store.PersistSignedExecution(ctx, exec.ID, rawTx, hash, providerReferenceID); err != nil {
		return fmt.Errorf("persist signed execution: %w", err)
	}
	exec.SignedTxHash = &hash
	exec.RawSignedTx = rawTx

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

// validateEnvelope is the independent, provider-agnostic checkpoint that
// runs before wallet.SignTx for every provider (design doc §12). It is
// deliberately NOT told anything by the Signer that built envelope --
// e.ExpectedContractByProvider is populated from a separate configuration
// source (cmd/worker/main.go), so this can catch a genuine bug or a
// misbehaving provider rather than comparing a value against itself.
func (e *Executor) validateEnvelope(envelope quote.TxEnvelope, quoteRow payment.Quote, freshQuote quote.Quote) error {
	if envelope.ChainID != e.OriginChainID {
		return fmt.Errorf("envelope chainId %d does not match configured origin chain %d", envelope.ChainID, e.OriginChainID)
	}
	expectedTo, ok := e.ExpectedContractByProvider[quoteRow.Provider]
	if !ok || envelope.To != expectedTo {
		return fmt.Errorf("envelope target %s does not match the pinned contract for provider %q", envelope.To.Hex(), quoteRow.Provider)
	}
	if envelope.Value == nil || freshQuote.InputAmountBaseUnits == nil || envelope.Value.Cmp(freshQuote.InputAmountBaseUnits) != 0 {
		return fmt.Errorf("envelope value %s does not match the validated input amount %s", envelope.Value, freshQuote.InputAmountBaseUnits)
	}
	return nil
}

// extractProviderReferenceID pulls Relay's requestId (if this quote came
// from Relay) out of RawProviderPayload for persistence alongside the
// signed bytes (design doc §16) -- returns nil for Across, which has no
// separate reference ID (its reconciliation keys on the origin tx hash).
func extractProviderReferenceID(freshQuote quote.Quote) *string {
	if freshQuote.ProviderName != "relay" {
		return nil
	}
	var payload struct {
		RequestID string `json:"requestId"`
	}
	if err := json.Unmarshal(freshQuote.RawProviderPayload, &payload); err != nil || payload.RequestID == "" {
		return nil
	}
	return &payload.RequestID
}
```

Add `"encoding/json"` to `executor.go`'s imports if not already present. Remove the now-unused `across.DecodeQuotePayload`/`strconv.ParseUint` calls and the `common.HexToAddress(payload.SpokePoolAddress) != e.SpokePoolAddress` check that used to live directly in this function -- they're superseded by `validateEnvelope`, which is provider-agnostic and runs for every provider, not just Across.

- [ ] **Step 5: Fix the `TryCreateExecution` bug this refactor exposes**

In `ExecuteTestnetPayment`, the current call:

```go
exec, created, err := e.Store.TryCreateExecution(ctx, postgres.CreateExecutionParams{
	PaymentID: paymentID, WalletAddress: e.Wallet.Address.Hex(), BridgeProvider: e.BridgeProvider, ...
})
```

uses `e.BridgeProvider`, a field this task removed (Step 1) -- and even before removal, this was silently wrong for a Relay-selected payment: it would have recorded `bridge_provider = "across"` regardless of which provider was actually selected, breaking the provider-consistency invariant (design doc §10, requirement D). Fix: use `BridgeProvider: quoteRow.Provider` instead -- the actual persisted, selected provider. Apply the identical fix everywhere else `TryCreateExecution` is called (check `DriveExecutionForward` too, if it has its own call site, though per the current code only `ExecuteTestnetPayment` calls it directly).

- [ ] **Step 6: Update `DriveExecutionForward` to pass `quoteRow` through to `signAndBroadcastFresh`**

`DriveExecutionForward`'s unsigned-row branch already fetches `quoteRow` before calling the old `signAndBroadcastFresh(ctx, exec, quoteRow, freshQuote)` (per Phase 8's final-review fix) -- confirm this call site still matches the new 4-argument signature; no further change needed here since it already passes `quoteRow`.

- [ ] **Step 7: Fix every pre-existing test broken by the struct-field removal**

Every test in `executor_test.go` that builds an `Executor{...}` literal referencing `Across`, `BridgeProvider`, `SpokePoolAddress`, `WETHOrigin`, or `WETHDestination` needs updating: remove those fields, add `Signers: map[string]quote.Signer{"across": <a fakeSigner or a real *across.Provider wired to the same fake HTTP server the test already uses>}` and `ExpectedContractByProvider: map[string]common.Address{"across": common.HexToAddress("0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662")}`. Update the shared `newTestExecutor`/`newTestExecutorWithAcrossBody` helpers once, rather than each test individually, exactly as Phase 8's Task 11 already did for its own set of shared-helper updates.

- [ ] **Step 8: Run to verify it passes**

Run: `cd go-api && go test ./internal/worker/... -v`
Expected: PASS -- every test, old and new, including every reconciler test (reconciler tests construct their own fakes independent of `Executor`, per Phase 8's Task 11 finding -- confirm this is still true after this task's changes).

- [ ] **Step 9: Full-module regression**

```bash
cd go-api && go build ./... && go vet ./... && go test ./...
```

Expected: clean (note: `cmd/worker/main.go` will now fail to compile, since it still constructs the old `Executor` fields -- that's expected and fixed in Task 11; if you want a clean `go build ./...` at this exact point, it's acceptable for this one task to leave `cmd/worker` broken, since Task 11 fixes it next -- state this clearly in your report rather than trying to fix `cmd/worker` prematurely in this task).

- [ ] **Step 10: Commit**

```bash
git add go-api/internal/worker/executor.go go-api/internal/worker/executor_test.go
git commit -m "feat(go-api): generalize Executor -- sign via quote.Signer, validate envelope independently, fix BridgeProvider bug"
```

---

## Task 10: Generalize `Reconciler`

**Files:**
- Modify: `go-api/internal/worker/reconciler.go`
- Modify: `go-api/internal/worker/reconciler_test.go`

**Interfaces:**
- Consumes: `quote.StatusChecker`, `quote.StatusRequest`, `quote.StatusResult` (Task 2); `across.Provider`/`relay.Provider` implementing `StatusChecker` (Tasks 3, 7).
- Produces: `Reconciler.StatusCheckers map[string]quote.StatusChecker` -- consumed by Task 11.

- [ ] **Step 1: Remove `Reconciler.Across *across.Client`; add `StatusCheckers`**

```go
type Reconciler struct {
	Store          ReconcilerStore
	Executor       *Executor
	OriginClient   ReconcilerEthClient
	StatusCheckers map[string]quote.StatusChecker
	WalletAddress  common.Address
	OriginChainID  int64
	Staleness      time.Duration
}
```

- [ ] **Step 2: Write the failing tests**

Add to `reconciler_test.go`:

```go
func TestCheckAndUpdateOutcome_DispatchesByPersistedProvider(t *testing.T) {
	// An execution with BridgeProvider="relay" must call the "relay"
	// StatusChecker, never "across"'s, even if both are configured.
	// Construct a fakeStatusChecker per provider name, assert only the
	// "relay" one's CheckStatus was invoked.
}

func TestCheckAndUpdateOutcome_UnknownProviderIsHardError(t *testing.T) {
	// BridgeProvider="relay" but StatusCheckers only has "across" configured
	// -- assert an error is returned, no terminal state is written.
}

func TestCheckAndUpdateOutcome_RelaySuccessCompletesPayment(t *testing.T) {
	// fakeStatusChecker returns quote.StatusResult{State: quote.StateFilled, RawStatus: "success"}
	// -- assert markTerminal is reached with payment.StatusCompleted, and
	// RawExternalStatus persisted as "success".
}

func TestCheckAndUpdateOutcome_RelayFailureFailsPayment(t *testing.T) {
	// fakeStatusChecker returns quote.StatusResult{State: quote.StateFillFailed, RawStatus: "failure"}
	// -- assert payment.StatusFailed, external_status='fill_failed'.
}

func TestCheckAndUpdateOutcome_TransientStatusCheckerErrorIsNonTerminal(t *testing.T) {
	// fakeStatusChecker returns an error -- assert NO terminal write occurs
	// (mirrors the existing Across transient-error test).
}
```

Write a `fakeStatusChecker` type in `reconciler_test.go` implementing `quote.StatusChecker` (embeds a `Name() string`, a fake `GetQuote` that's unused/panics if called, and a configurable `CheckStatus` returning a preset result/error).

- [ ] **Step 3: Run to verify it fails**

Run: `cd go-api && go test ./internal/worker/... -run TestCheckAndUpdateOutcome -v`
Expected: FAIL (compile errors -- `Reconciler.StatusCheckers` undefined, `r.Across` still referenced)

- [ ] **Step 4: Rewrite `checkAndUpdateOutcome`**

Replace the tail of `checkAndUpdateOutcome` (currently: call `r.Across.DepositStatusByTxHash` directly, `switch status.Status { case "filled": ...}`) with:

```go
	checker, ok := r.StatusCheckers[exec.BridgeProvider]
	if !ok {
		return fmt.Errorf("execution %s uses provider %q, which this reconciler has no configured status checker for", exec.ID, exec.BridgeProvider)
	}
	result, err := checker.CheckStatus(ctx, quote.StatusRequest{
		ProviderReferenceID: derefOrEmpty(exec.ProviderReferenceID),
		OriginTxHash:        *exec.SignedTxHash,
	})
	if err != nil {
		return nil // transient API error -- not a failure signal (design spec §13), retry next sweep
	}

	switch result.State {
	case quote.StateFilled:
		return r.markTerminal(ctx, exec, result, payment.StatusCompleted)
	case quote.StateRefunded, quote.StateReverted, quote.StateFillFailed:
		return r.markTerminal(ctx, exec, result, payment.StatusFailed)
	default: // quote.StatePending
		return nil
	}
}

func derefOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
```

Update `markTerminal`'s signature to accept the `quote.StatusResult` (for `RawStatus`) instead of a bare `payment.ExternalStatus`, and update its call to `UpdateExecutionExternalStatus` to pass `result.RawStatus` (matching Task 5's new signature):

```go
func (r *Reconciler) markTerminal(ctx context.Context, exec payment.Execution, result quote.StatusResult, terminal payment.Status) error {
	external := externalStateToStatus(result.State) // a small local mapping from quote.ExternalState back to payment.ExternalStatus, since the two enums are intentionally distinct types (design doc §6's rationale for two small interfaces applies to their result types too)
	confirmedAt := &sql.NullTime{Time: time.Now().UTC(), Valid: true}
	if err := r.Store.UpdateExecutionExternalStatus(ctx, exec.ID, external, result.RawStatus, confirmedAt); err != nil {
		return fmt.Errorf("record %s: %w", external, err)
	}
	// ... rest unchanged (CompleteSubmittedPayment call, the "completed=false" log) ...
}

func externalStateToStatus(s quote.ExternalState) payment.ExternalStatus {
	switch s {
	case quote.StateFilled:
		return payment.ExternalStatusFilled
	case quote.StateRefunded:
		return payment.ExternalStatusRefunded
	case quote.StateReverted:
		return payment.ExternalStatusReverted
	case quote.StateFillFailed:
		return payment.ExternalStatusFillFailed
	default:
		return payment.ExternalStatusPending
	}
}
```

The existing `receipt.Status == 0` origin-revert branch (unchanged, provider-agnostic already, operates on `OriginClient` directly) still calls `r.markTerminal(ctx, exec, quote.StatusResult{State: quote.StateReverted, RawStatus: "origin_reverted"}, payment.StatusFailed)` -- update that one call site's arguments to match the new `markTerminal` signature too.

- [ ] **Step 5: Run to verify it passes**

Run: `cd go-api && go test ./internal/worker/... -v`
Expected: PASS -- every test, old and new.

- [ ] **Step 6: Commit**

```bash
git add go-api/internal/worker/reconciler.go go-api/internal/worker/reconciler_test.go
git commit -m "feat(go-api): generalize Reconciler -- status via quote.StatusChecker, dispatched by persisted provider"
```

---

## Task 11: Wire `cmd/server` and `cmd/worker`

**Files:**
- Modify: `go-api/cmd/server/main.go`
- Modify: `go-api/cmd/worker/main.go`

- [ ] **Step 1: `cmd/server/main.go`** -- register `relay.NewProvider` alongside Across:

```go
	var registry *quote.Registry
	if blockchainEnv == "testnet" {
		acrossBaseURL := envOrDefaultServer("ACROSS_TESTNET_API_URL", "https://testnet.across.to/api")
		relayBaseURL := envOrDefaultServer("RELAY_TESTNET_API_URL", "https://api.testnets.relay.link")
		routingQuoteTTL := envDurationServer("ROUTING_QUOTE_TTL_SECONDS", 2*time.Minute, time.Second)
		acrossClient := across.NewClient(acrossBaseURL)
		acrossClient.APIKey = os.Getenv("ACROSS_API_KEY")
		acrossClient.IntegratorID = os.Getenv("ACROSS_INTEGRATOR_ID")
		relayClient := relay.NewClient(relayBaseURL)
		relayClient.APIKey = os.Getenv("RELAY_API_KEY")

		// cmd/server never signs anything, so the quoting-only Provider
		// instances here never need a real wallet address -- a zero
		// address is safe (GetQuote uses it only as the quote request's
		// "user" field, which does not need to resolve to funds for a
		// price-discovery call).
		var zeroWallet common.Address
		registry = quote.NewRegistry()
		routeKey := quote.RouteKey{SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH"}
		registry.Register(routeKey, across.NewProvider(acrossClient, routingQuoteTTL))
		registry.Register(routeKey, relay.NewProvider(relayClient, zeroWallet, routingQuoteTTL))
	}
```

Add `"chainroute/go-api/internal/bridge/relay"` and `"github.com/ethereum/go-ethereum/common"` to imports.

- [ ] **Step 2: `cmd/worker/main.go`** -- construct both providers with real execution fields, register both `Signers` and `StatusCheckers`:

```go
		acrossClient := across.NewClient(acrossBaseURL)
		acrossClient.APIKey = os.Getenv("ACROSS_API_KEY")
		acrossClient.IntegratorID = os.Getenv("ACROSS_INTEGRATOR_ID")

		maxFeeSlippageBps := envInt64("MAX_FEE_SLIPPAGE_BPS", 500)
		routingQuoteTTL := envDuration("ROUTING_QUOTE_TTL_SECONDS", 2*time.Minute, time.Second)

		spokePoolAddress := common.HexToAddress("0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662")
		wethOrigin := common.HexToAddress("0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14")
		wethDestination := common.HexToAddress("0x4200000000000000000000000000000000000006")

		acrossProvider := across.NewProvider(acrossClient, routingQuoteTTL)
		acrossProvider.WalletAddress = wallet.Address
		acrossProvider.OriginChainID = 11155111
		acrossProvider.SpokePoolAddress = spokePoolAddress
		acrossProvider.WETHOrigin = wethOrigin
		acrossProvider.WETHDestination = wethDestination

		relayBaseURL := envOrDefault("RELAY_TESTNET_API_URL", "https://api.testnets.relay.link")
		relayClient := relay.NewClient(relayBaseURL)
		relayClient.APIKey = os.Getenv("RELAY_API_KEY")
		relayProvider := relay.NewProvider(relayClient, wallet.Address, routingQuoteTTL)

		// RELAY_DEPOSIT_CONTRACT_SEPOLIA: pinned independently of
		// relayProvider itself (design doc §12) -- Task 1 re-verified this
		// address is stable across varying `user` addresses before this
		// plan trusted it as a hard security pin.
		relayDepositContract := common.HexToAddress(envOrDefault("RELAY_DEPOSIT_CONTRACT_SEPOLIA", "0x5feaB8db4534f9F7e2669bb260C57A01aD1c12E3"))

		quoteProviders := map[string]quote.Provider{"across": acrossProvider, "relay": relayProvider}
		signers := map[string]quote.Signer{"across": acrossProvider, "relay": relayProvider}
		statusCheckers := map[string]quote.StatusChecker{"across": acrossProvider, "relay": relayProvider}
		expectedContracts := map[string]common.Address{"across": spokePoolAddress, "relay": relayDepositContract}

		executor = &worker.Executor{
			Store: store, Wallet: wallet, OriginClient: sepoliaClient,
			QuoteProviders: quoteProviders, Signers: signers, ExpectedContractByProvider: expectedContracts,
			OriginChainID: 11155111, DestChainID: 84532,
			MaxAmountWei: maxTestnetAmountWei, MaxFeeSlippageBps: maxFeeSlippageBps,
		}
		reconciler = &worker.Reconciler{
			Store: store, Executor: executor, OriginClient: sepoliaClient, StatusCheckers: statusCheckers,
			WalletAddress: wallet.Address, OriginChainID: 11155111, Staleness: reconcileStaleness,
		}
```

Add `"chainroute/go-api/internal/bridge/relay"` to imports.

- [ ] **Step 3: Build and test**

```bash
cd go-api && go build ./... && go vet ./... && go test ./...
export DATABASE_URL="postgres://shivaniparimi@localhost:5432/chainroute?sslmode=disable"
go test -tags=integration ./...
```

Expected: fully clean -- this is the task where `cmd/worker`'s Task-9-broken build gets fixed.

- [ ] **Step 4: Confirm simulated mode needs zero new env vars**

Read through your edits: confirm every new construction (`relay.NewClient`, `relay.NewProvider`, `RELAY_*` env reads) sits strictly inside the existing `if blockchainEnv == "testnet"` blocks in both files -- no new top-level gate was introduced.

- [ ] **Step 5: Commit**

```bash
git add go-api/cmd/server/main.go go-api/cmd/worker/main.go
git commit -m "feat(go-api): wire relay.Provider into cmd/server registry and cmd/worker Signers/StatusCheckers"
```

---

## Task 12: C++ two-real-provider test coverage

No production C++ or `.proto` changes expected (design doc §8, confirmed: `CandidateEdge` already `repeated`, `findCheapestRoute` already supports parallel edges). This task only adds tests using real-shaped provider names.

**Files:**
- Modify: `cpp-routing-service/tests/routing_service_test.cpp`

- [ ] **Step 1: Write the tests**

Add to the existing anonymous namespace, replacing the Phase-8-era `"other-provider"` placeholder name with `"relay"` where a second real-shaped name is needed:

```cpp
TEST(RoutingServiceTest, TwoRealProviders_AcrossCheaper_AcrossSelected) {
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_ETHEREUM);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_ETH);
    request.set_amount(0.001);

    auto* across = request.add_candidate_edges();
    across->set_bridge_name("across"); across->set_fee(0.0001);
    across->set_latency_ms(60000.0); across->set_liquidity(0.001); across->set_reliability(1.0);
    auto* relay = request.add_candidate_edges();
    relay->set_bridge_name("relay"); relay->set_fee(0.0005);
    relay->set_latency_ms(4000.0); relay->set_liquidity(0.001); relay->set_reliability(1.0);

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    ASSERT_TRUE(service.FindRoute(&context, &request, &response).ok());
    ASSERT_TRUE(response.route_found());
    ASSERT_EQ(response.hops_size(), 1);
    EXPECT_EQ(response.hops(0).bridge_name(), "across");
}

TEST(RoutingServiceTest, TwoRealProviders_RelayCheaper_RelaySelected) {
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_ETHEREUM);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_ETH);
    request.set_amount(0.001);

    auto* across = request.add_candidate_edges();
    across->set_bridge_name("across"); across->set_fee(0.0005);
    across->set_latency_ms(60000.0); across->set_liquidity(0.001); across->set_reliability(1.0);
    auto* relay = request.add_candidate_edges();
    relay->set_bridge_name("relay"); relay->set_fee(0.0001);
    relay->set_latency_ms(4000.0); relay->set_liquidity(0.001); relay->set_reliability(1.0);

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    ASSERT_TRUE(service.FindRoute(&context, &request, &response).ok());
    ASSERT_TRUE(response.route_found());
    EXPECT_EQ(response.hops(0).bridge_name(), "relay");
}

TEST(RoutingServiceTest, TwoRealProviders_EqualFee_FirstInsertedWins) {
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_ETHEREUM);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_ETH);
    request.set_amount(0.001);

    auto* across = request.add_candidate_edges();
    across->set_bridge_name("across"); across->set_fee(0.0002);
    across->set_latency_ms(60000.0); across->set_liquidity(0.001); across->set_reliability(1.0);
    auto* relay = request.add_candidate_edges();
    relay->set_bridge_name("relay"); relay->set_fee(0.0002); // exactly equal
    relay->set_latency_ms(4000.0); relay->set_liquidity(0.001); relay->set_reliability(1.0);

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    ASSERT_TRUE(service.FindRoute(&context, &request, &response).ok());
    ASSERT_TRUE(response.route_found());
    // Documents the existing, non-business tie-break (design doc §9): the
    // FIRST-inserted edge wins ties, not a deliberate provider preference.
    EXPECT_EQ(response.hops(0).bridge_name(), "across");
}

TEST(RoutingServiceTest, TwoRealProviders_RelayUnavailable_AcrossSelected) {
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_ETHEREUM);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_ETH);
    request.set_amount(0.001);

    auto* across = request.add_candidate_edges();
    across->set_bridge_name("across"); across->set_fee(0.0001);
    across->set_latency_ms(60000.0); across->set_liquidity(0.001); across->set_reliability(1.0);
    auto* relay = request.add_candidate_edges();
    relay->set_bridge_name("relay"); relay->set_fee(0.00001); // cheaper, but unavailable
    relay->set_latency_ms(4000.0); relay->set_liquidity(0.0); relay->set_reliability(1.0); // Available=false -> liquidity=0

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    ASSERT_TRUE(service.FindRoute(&context, &request, &response).ok());
    ASSERT_TRUE(response.route_found());
    EXPECT_EQ(response.hops(0).bridge_name(), "across");
}

TEST(RoutingServiceTest, TwoRealProviders_NeitherViable_NoRoute) {
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_ETHEREUM);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_ETH);
    request.set_amount(0.001);

    auto* across = request.add_candidate_edges();
    across->set_bridge_name("across"); across->set_fee(0.0001);
    across->set_latency_ms(60000.0); across->set_liquidity(0.0); across->set_reliability(1.0);
    auto* relay = request.add_candidate_edges();
    relay->set_bridge_name("relay"); relay->set_fee(0.0001);
    relay->set_latency_ms(4000.0); relay->set_liquidity(0.0); relay->set_reliability(1.0);

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    ASSERT_TRUE(service.FindRoute(&context, &request, &response).ok());
    EXPECT_FALSE(response.route_found());
    EXPECT_EQ(response.hops_size(), 0);
}
```

- [ ] **Step 2: Build and run**

```bash
cmake --build cpp-routing-service/build --target chainroute_service_tests
ctest --test-dir cpp-routing-service/build --output-on-failure
```

Expected: PASS -- all five new tests, plus every pre-existing test unmodified.

- [ ] **Step 3: Confirm no production C++ diff**

```bash
git diff --stat -- cpp-routing-service/src/ proto/
```

Expected: empty. If not empty, STOP -- something in this task required a source change the design doc said shouldn't be needed; report why before proceeding.

- [ ] **Step 4: Commit**

```bash
git add cpp-routing-service/tests/routing_service_test.cpp
git commit -m "test(cpp-routing-service): cover two real-shaped parallel providers in findCheapestRoute selection"
```

---

## Task 13: PostgreSQL/Kafka integration tests

**Files:**
- Create/modify: `go-api/internal/postgres/execution_store_integration_test.go` (extend)
- Create/modify: `go-api/internal/postgres/quote_store_integration_test.go` (extend)
- Modify: `go-api/internal/worker/executor_test.go` or a new `executor_integration_test.go` if the existing nonce-concurrency test lives in a `-tags=integration` file (check first)

- [ ] **Step 1: Provider-consistency test**

```go
func TestCreateOrGetPayment_RelayWinnerProducesConsistentProviderAcrossTables(t *testing.T) {
	store := newTestStore(t)
	p := payment.Payment{
		IdempotencyKey: "relay-consistency-" + t.Name(), SourceChain: "ethereum", DestinationChain: "base",
		Asset: "eth", Amount: "0.001", ExecutionMode: payment.ExecutionModeTestnet,
		Hops: []payment.Hop{{HopIndex: 0, FromChain: "ethereum", ToChain: "base", BridgeName: "relay", Fee: 0.0001, LatencyMs: 4000, Liquidity: 0.001, Reliability: 1.0}},
		BridgeProvider: strPtr("relay"),
		Quote: &payment.Quote{Provider: "relay", OriginChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
			InputAmount: "1000000000000000", OutputAmount: "999900000000000", FeeAmount: "100000000000",
			EstimatedFillTimeSec: 4, QuotedAt: time.Now(), ExpiresAt: time.Now().Add(time.Minute),
			RawProviderPayload: json.RawMessage(`{"requestId":"0xabc"}`)},
	}
	created, _, err := store.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("CreateOrGetPayment: %v", err)
	}

	got, found, err := store.GetPayment(context.Background(), created.ID)
	if err != nil || !found {
		t.Fatalf("GetPayment: found=%v err=%v", found, err)
	}
	if got.BridgeProvider == nil || *got.BridgeProvider != "relay" {
		t.Errorf("payments.bridge_provider = %v, want relay", got.BridgeProvider)
	}
	if got.Hops[0].BridgeName != "relay" {
		t.Errorf("payment_route_hops.bridge_name = %q, want relay", got.Hops[0].BridgeName)
	}
	q, found, err := store.GetQuoteByPaymentID(context.Background(), created.ID)
	if err != nil || !found {
		t.Fatalf("GetQuoteByPaymentID: found=%v err=%v", found, err)
	}
	if q.Provider != "relay" {
		t.Errorf("payment_quotes.provider = %q, want relay", q.Provider)
	}
}
```

- [ ] **Step 2: No-losing-quote-persisted test**

```go
func TestCreateOrGetPayment_LosingAcrossQuoteIsNeverPersisted(t *testing.T) {
	// Create a payment where Relay won (Hops[0].BridgeName = "relay",
	// candidate.Quote.Provider = "relay") -- assert there is no
	// payment_route_hops row and no payment_quotes row anywhere in the
	// database with provider/bridge_name = "across" for this payment_id.
	// (payment_quotes has UNIQUE(payment_id) already, so this is really
	// just confirming the ONE row that exists says "relay", not "across"
	// -- reuse the assertion from Step 1's test, phrased as its own test
	// for clarity of intent per design doc §10/§24.)
}
```

- [ ] **Step 3: Cross-provider nonce uniqueness test**

```go
func TestTryCreateExecution_NonceUniqueAcrossAcrossAndRelayExecutions(t *testing.T) {
	store := newTestStore(t)
	wallet := "0xCrossProviderNonceTest" + t.Name()
	if err := store.SeedWalletNonce(context.Background(), wallet, 0); err != nil {
		t.Fatalf("seed: %v", err)
	}

	const n = 10
	var wg sync.WaitGroup
	nonces := make(chan int64, n)
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		provider := "across"
		if i%2 == 0 {
			provider = "relay"
		}
		go func(paymentID, provider string) {
			defer wg.Done()
			exec, created, err := store.TryCreateExecution(context.Background(), CreateExecutionParams{
				PaymentID: paymentID, WalletAddress: wallet, BridgeProvider: provider, OriginChainID: 11155111, DestinationChainID: 84532,
			})
			if err != nil {
				errs <- err
				return
			}
			if !created {
				errs <- fmt.Errorf("payment %s: expected created=true", paymentID)
				return
			}
			nonces <- exec.Nonce
		}(fmt.Sprintf("pay-%s-%d", t.Name(), i), provider)
	}
	wg.Wait()
	close(nonces)
	close(errs)
	for err := range errs {
		t.Fatalf("unexpected error: %v", err)
	}
	seen := map[int64]bool{}
	for nonce := range nonces {
		if seen[nonce] {
			t.Fatalf("nonce %d allocated twice", nonce)
		}
		seen[nonce] = true
	}
	if len(seen) != n {
		t.Fatalf("expected %d unique nonces, got %d", n, len(seen))
	}
}
```

Note: each goroutine above uses a distinct `paymentID` (no real `payments` row exists for these synthetic IDs, but `TryCreateExecution` doesn't enforce a foreign key check against `payments` in a way that blocks this -- confirm this against the actual `payment_executions` schema; if `payment_id` has a `REFERENCES payments(id)` foreign key requiring a real row to exist first, this test needs each iteration to first insert a minimal payment row via `CreateOrGetPayment`, exactly like Step 1's test does, rather than a synthetic ID).

- [ ] **Step 4: Relay signed-execution atomicity test**

```go
func TestPersistSignedExecution_PersistsProviderReferenceIDAtomicallyWithSignedBytes(t *testing.T) {
	// Create an execution row (any provider), call
	// store.PersistSignedExecution(ctx, exec.ID, rawTx, txHash, strPtr("0xrelayrequestid123")),
	// then GetExecutionByPaymentID and assert BOTH SignedTxHash AND
	// ProviderReferenceID are non-nil and correct -- proving they land in
	// the same UPDATE, never one without the other.
}
```

- [ ] **Step 5: Kafka duplicate-delivery test for a Relay-selected payment**

Extend the existing Kafka integration test (find it -- likely `go-api/internal/kafka/kafka_integration_test.go` or a worker-level test) to run once with an Across-fixture payment and once with a Relay-fixture payment, asserting duplicate delivery is a safe no-op past the first successful claim in both cases -- reuse the existing test's assertions, parameterized by provider name rather than duplicating the whole test body.

- [ ] **Step 6: Run everything**

```bash
cd go-api && export DATABASE_URL="postgres://shivaniparimi@localhost:5432/chainroute?sslmode=disable"
go test -tags=integration ./... -v
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
go test -tags=kafka_integration ./internal/kafka/... -v
```

Expected: PASS, all suites.

- [ ] **Step 7: Commit**

```bash
git add go-api/internal/postgres/ go-api/internal/kafka/ go-api/internal/worker/
git commit -m "test(go-api): cross-provider integration coverage -- nonce uniqueness, provider consistency, atomic reference-ID persistence"
```

---

## Task 14: Extend `scripts/e2e_testnet_test.sh`; full regression

**Files:**
- Modify: `scripts/e2e_testnet_test.sh`

- [ ] **Step 1: Read the current script**

Confirm its existing structure (Phase 7/8 already extended it once; find the exact `POST /payments` call and the `bridge_provider` assertion added during Phase 8).

- [ ] **Step 2: Add the dual-provider real-testnet check, gated exactly as before**

Insert, after both a real Across quote and a real Relay quote can be fetched (this script already has network access to both APIs once `RUN_TESTNET_TESTS=1` is set): fetch each provider's actual live fee via a small script-local call (or via a new Go test binary invoked by the script, matching whatever pattern the script already uses to make live calls), compute `min(acrossFee, relayFee)`, then assert the `POST /payments` response's `hops[0].fee` equals that minimum -- **never asserting which provider name wins**, per the design doc §25 and the user's explicit instruction. Whichever provider actually won, continue the existing polling-to-`COMPLETED` logic unchanged; it already reads `bridge_provider` generically.

- [ ] **Step 3: Syntax-check**

```bash
bash -n scripts/e2e_testnet_test.sh
```

- [ ] **Step 4: Full regression pass**

```bash
# C++
ctest --test-dir router/build --output-on-failure
ctest --test-dir cpp-routing-service/build --output-on-failure
# Go
cd go-api && go build ./... && go vet ./... && go test ./...
export DATABASE_URL="postgres://shivaniparimi@localhost:5432/chainroute?sslmode=disable"
go test -tags=integration ./...
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
go test -tags=kafka_integration ./internal/kafka/... -v
cd ..
# Local deterministic E2E
./scripts/e2e_test.sh
```

Expected: 100% pass across every suite.

- [ ] **Step 5: Gated real-testnet suite -- run only if credentials/funded wallet/RPC are available**

```bash
RUN_TESTNET_TESTS=1 go test -tags=testnet_integration ./go-api/...
RUN_TESTNET_TESTS=1 BLOCKCHAIN_ENV=testnet ./scripts/e2e_testnet_test.sh
```

If this environment isn't available, report it explicitly as **NOT RUN** -- never claim it passed without actually running it.

- [ ] **Step 6: Commit**

```bash
git add scripts/e2e_testnet_test.sh
git commit -m "test(e2e): extend gated testnet smoke script for real dual-provider routing"
```

---

## Self-Review Notes (completed during plan writing)

**Spec coverage**: every design-doc section maps to a task -- §1-3 (context, this plan's whole premise), §4-6 (Tasks 2-3, 6-7), §7 (Task 6), §8-9 (Task 12, no code change needed), §10 (Tasks 9 Step 5, 13), §11-12 (Task 9), §13 (Task 13 Step 3), §14 (Task 10), §15-16 (Tasks 4-5), §17-18 (covered by preserving Phase 7/8's exact crash-safety code paths, verified in Task 9/10's "every existing test still passes" gates), §19 (Task 9's `validateEnvelope`), §20 (Task 11 Step 4), §21 (Task 10's `RawStatus` threading), §22-25 (Tasks 6-8, 12-14), §26 (Task 4's rename-not-add design), §27-28 (restated across every task's acceptance criteria).

**Gap found and fixed during review**: the design doc's §11 code sketch for `across.Provider.BuildTransaction` didn't show gas estimation, which the ORIGINAL `BuildAndSignDepositV3Tx` performs internally. Resolution (Task 9, Step 4): gas estimation moves into `Executor.signAndBroadcastFresh` itself, called uniformly against `envelope.To`/`Value`/`Data` regardless of provider -- consistent with "Executor is the sole signer," just extended to "Executor is also the sole gas-estimator," which the design doc's centralization rationale already implies even though its code sketch didn't spell out gas handling.

**Gap found and fixed during review**: tracing the actual current `ExecuteTestnetPayment` code (not just the design doc's prose) surfaced a real, previously-undiscussed bug: `TryCreateExecution` was being called with a hardcoded `e.BridgeProvider` field instead of the actually-selected `quoteRow.Provider` -- meaning a Relay-selected payment's `payment_executions.bridge_provider` column would have silently said "across." Fixed explicitly in Task 9, Step 5, with the field removed from `Executor` entirely in Step 1 so it can't silently come back.

**Placeholder scan**: two test bodies in Task 9 Step 2 (`TestExecuteTestnetPayment_WrongEnvelopeTargetIsHardError`, `TestExecuteTestnetPayment_WrongEnvelopeValueIsHardError`) and one in Task 9 Step 2 (`TestExecuteTestnetPayment_RelayShapedEnvelopePassesValidationAndSigns`) are described rather than fully written out, with an explicit instruction that the implementer must write them out in full (not leave them as comments) -- flagged here as an intentional, bounded exception (the pattern to follow is fully specified by the sibling test immediately above each), not an unresolved TBD. Task 13's Steps 2 and 5 similarly describe rather than fully write two tests, for the same reason (the pattern is fully specified by an adjacent, fully-written test or an existing test to parameterize) -- both are call-outs for the implementer to complete using a clearly specified pattern, not missing design decisions.

**Type consistency**: `quote.ExternalState`/`payment.ExternalStatus` are deliberately kept as two distinct types (Task 10's `externalStateToStatus` mapping function bridges them) rather than unified into one -- consistent with the design doc §6's rationale for keeping interfaces narrow and layer-appropriate; verified this mapping function's five cases exactly match the five `payment.ExternalStatus` constants Task 5 defined, with no sixth value possible on either side.
