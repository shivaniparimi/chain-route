# Phase 8 Real-Time Bridge Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Connect real Across testnet quotes to the existing C++ Dijkstra router so it selects the route the Kafka worker actually executes, replacing Phase 7's separately-hardcoded routing and execution paths.

**Architecture:** `POST /payments` fetches live Across quotes in Go, normalizes them into `CandidateEdge` protobuf values, and passes them to the unchanged C++ `findCheapestRoute`; the winning quote is persisted atomically with the payment; the worker re-quotes fresh and checks expiry/slippage *before* allocating a nonce, then executes using the provider named in the persisted quote — never one it picks itself.

**Tech Stack:** C++20/gRPC/Protobuf (routing), Go 1.x (API/worker), PostgreSQL (`jackc/pgx`), Kafka (`kafka-go`), `go-ethereum` (signing), Across testnet HTTP API.

**Design doc:** `docs/superpowers/specs/2026-09-14-realtime-bridge-routing-design.md` — read it if a task's rationale (the "why") isn't obvious from context here; this plan only repeats rationale where a *decision made during planning* (not already in the design doc) needs recording.

## Global Constraints

- `router/include/chainroute/{graph,edge,node,route}.hpp` and `route.cpp` are **not modified** — Dijkstra itself is untouched.
- Simulated-mode (`execution_mode=simulated`, the default) behavior is **byte-identical** to pre-Phase-8 `main` — every existing test in `router/`, `cpp-routing-service/`, and Go simulated-mode tests must pass **unmodified**.
- Exactly one provider is implemented: Across. Do not implement Relay or any other provider.
- Real execution stays single-hop: source chain directly to destination chain, one bridge, one transaction.
- Every decimal/base-unit amount conversion uses `math/big` exclusively — never `float64`/`big.Float` — except the final proto-boundary `double` fields (`CandidateEdge.fee`/`liquidity`), which have never carried an exactness contract in this codebase (the existing `amountForRouting` conversion in `handler/payments.go` already documents this).
- Every new Postgres status transition uses the existing conditional-`UPDATE ... WHERE status = $expected` pattern (matching `ClaimPayment`/`CompletePayment`/`MarkSubmitted`) — never an unconditional `UPDATE`.
- Migrations `0001`-`0004` are never modified. New migration: `0005_realtime_bridge_routing.sql`.
- Normal unit/integration tests (`go test ./...`, `go test -tags=integration ./...`, the C++ test binaries, `scripts/e2e_test.sh`) never touch the network. Real-network tests require both the `testnet_integration` build tag **and** `RUN_TESTNET_TESTS=1`, exactly extending Phase 7's existing gate.
- No mainnet support anywhere. No new microservice, cache, or message broker.
- Never log a private key or raw signed transaction bytes, at any log level — unchanged Phase 7 rule.

---

## Task 1: Live-verify the current Across testnet API shape

This is a verification task, not a feature — its job is to catch drift between the design doc's research (§ research section) and actual current behavior *before* Task 6 writes code against an assumed shape.

**Files:**
- Create: none (temporary verification only)
- Reference: `go-api/internal/bridge/across/quote.go`, `go-api/internal/bridge/across/quote_test.go`

- [ ] **Step 1: Determine the current `ACROSS_TESTNET_API_URL`**

The design doc's research section flagged that `docs.across.to` may have consolidated testnet under `app.across.to` or changed auth requirements. Check what's currently configured/assumed:

```bash
grep -rn "ACROSS_TESTNET_API_URL\|testnet.across.to" go-api/cmd/worker/main.go go-api/internal/bridge/across/
```

Confirm it still reads `envOrDefault("ACROSS_TESTNET_API_URL", "https://testnet.across.to/api")` in `cmd/worker/main.go`.

- [ ] **Step 2: Make one live call against the real endpoint**

```bash
curl -sS "https://testnet.across.to/api/suggested-fees?originChainId=11155111&destinationChainId=84532&inputToken=0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14&outputToken=0x4200000000000000000000000000000000000006&amount=1000000000000000" | tee /tmp/across_live_response.json
```

- [ ] **Step 3: Compare the live response's field names against `SuggestedFeesResponse`**

Read `go-api/internal/bridge/across/quote.go`'s struct and `quote_test.go`'s `realSuggestedFeesFixture` constant. Diff the live JSON's top-level keys against the fixture's keys, specifically checking:
- Is the response still flat (no nested `fees` object)?
- Are `outputAmount`, `fillDeadline`, `exclusivityDeadline`, `exclusiveRelayer`, `timestamp`, `spokePoolAddress`, `isAmountTooLow`, `inputToken`, `outputToken` all still present at the top level?
- Is `estimatedFillTimeSec` present? (It's in the existing fixture's raw JSON but not currently parsed by `SuggestedFeesResponse` — Task 6 adds it regardless of what you find here, since it's needed either way.)
- Did the request succeed without any `Authorization` header or `integratorId` query param (confirming Phase 7's "no auth required on testnet" comment still holds)?

- [ ] **Step 4: Record the outcome**

If the live shape matches (fields present, no auth required, flat structure): proceed to Task 2 — no changes needed to `quote.go` beyond what Task 6 already plans (adding `estimatedFillTimeSec`).

If the live shape has drifted (new required field, auth now enforced, nested restructure): **stop and report the exact difference before continuing** — per the design doc's explicit instruction, this must be resolved by updating `across/quote.go`'s parsing (and, if auth is now required, wiring `ACROSS_API_KEY`/`ACROSS_INTEGRATOR_ID` through, which are already-wired-but-unused optional fields on `across.Client`) before Task 6 proceeds, and the normalized `Quote` type in Task 5 is unaffected either way (normalization sits one layer above the provider-specific parser).

---

## Task 2: Add `money.BaseUnitsToDecimal`

The design doc's normalized `Quote` carries fees in base units (`*big.Int`); building a `CandidateEdge.fee` (a proto `double`, in the request's *decimal* asset units) needs the inverse of the existing `money.DecimalToBaseUnits`. This inverse function does not exist yet — verified by reading `go-api/internal/money/convert.go` in full; it has only `DecimalToBaseUnits`.

**Files:**
- Modify: `go-api/internal/money/convert.go`
- Modify: `go-api/internal/money/convert_test.go`

**Interfaces:**
- Produces: `func BaseUnitsToDecimal(amount *big.Int, decimals uint8) string` — used by Task 10 (handler) and Task 6 (across provider, for `FeeBaseUnits` display/logging).

- [ ] **Step 1: Write the failing tests**

Add to `go-api/internal/money/convert_test.go`:

```go
func TestBaseUnitsToDecimal_ExactRoundTrip(t *testing.T) {
	cases := []struct {
		amount   string
		decimals uint8
		want     string
	}{
		{"1000000000000000", 18, "0.001"},
		{"1000000000000000000", 18, "1"},
		{"1", 18, "0.000000000000000001"},
		{"0", 18, "0"},
		{"123456", 6, "0.123456"},
		{"1000000", 6, "1"},
		{"100", 0, "100"},
	}
	for _, c := range cases {
		n, ok := new(big.Int).SetString(c.amount, 10)
		if !ok {
			t.Fatalf("bad test fixture amount %q", c.amount)
		}
		got := BaseUnitsToDecimal(n, c.decimals)
		if got != c.want {
			t.Errorf("BaseUnitsToDecimal(%s, %d) = %q, want %q", c.amount, c.decimals, got, c.want)
		}
	}
}

func TestBaseUnitsToDecimal_InverseOfDecimalToBaseUnits(t *testing.T) {
	// Round-tripping through both conversions must be lossless for every
	// amount DecimalToBaseUnits already accepts.
	decimalsIn := []string{"1.5", "0.001", "1000.123456789012345678", "0"}
	for _, d := range decimalsIn {
		baseUnits, err := DecimalToBaseUnits(d, 18)
		if err != nil {
			t.Fatalf("DecimalToBaseUnits(%q): %v", d, err)
		}
		back := BaseUnitsToDecimal(baseUnits, 18)
		reparsed, err := DecimalToBaseUnits(back, 18)
		if err != nil {
			t.Fatalf("DecimalToBaseUnits(%q) on round-trip: %v", back, err)
		}
		if reparsed.Cmp(baseUnits) != 0 {
			t.Errorf("round-trip mismatch for %q: got base units %s back as %s", d, baseUnits, back)
		}
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd go-api && go test ./internal/money/... -run TestBaseUnitsToDecimal -v`
Expected: FAIL (`BaseUnitsToDecimal` undefined)

- [ ] **Step 3: Implement**

Add to `go-api/internal/money/convert.go`:

```go
// BaseUnitsToDecimal converts an integer base-units amount back into an
// exact decimal string at the given token precision -- the inverse of
// DecimalToBaseUnits, using big.Int exclusively. Trailing fractional
// zeros are trimmed (e.g. 18-decimal "1000000000000000000" becomes "1",
// not "1.000000000000000000"); a decimals of 0 produces the integer
// string unchanged.
func BaseUnitsToDecimal(amount *big.Int, decimals uint8) string {
	neg := amount.Sign() < 0
	s := new(big.Int).Abs(amount).String()
	for len(s) <= int(decimals) {
		s = "0" + s
	}
	splitAt := len(s) - int(decimals)
	intPart, fracPart := s[:splitAt], strings.TrimRight(s[splitAt:], "0")

	result := intPart
	if fracPart != "" {
		result += "." + fracPart
	}
	if neg && result != "0" {
		result = "-" + result
	}
	return result
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd go-api && go test ./internal/money/... -v`
Expected: PASS (all money package tests, including the two new ones and every pre-existing one)

- [ ] **Step 5: Commit**

```bash
git add go-api/internal/money/convert.go go-api/internal/money/convert_test.go
git commit -m "feat(go-api): add BaseUnitsToDecimal, the inverse of DecimalToBaseUnits"
```

---

## Task 3: Proto — add `CandidateEdge` and regenerate stubs

**Files:**
- Modify: `proto/chainroute/v1/routing.proto`
- Regenerate (do not hand-edit): `go-api/internal/gen/chainroute/v1/routing.pb.go`, `go-api/internal/gen/chainroute/v1/routing_grpc.pb.go`
- Regenerate (build-time, via CMake, no committed output): the C++ `routing.pb.h/.cc`, `routing.grpc.pb.h/.cc` in `cpp-routing-service/build/generated/`

**Interfaces:**
- Produces: `routingv1.CandidateEdge{BridgeName string, Fee float64, LatencyMs float64, Liquidity float64, Reliability float64}` and `FindRouteRequest.CandidateEdges []*CandidateEdge` (Go); `chainroute::v1::CandidateEdge` and `FindRouteRequest::candidate_edges()` (C++) — used by Task 4 (C++) and Task 10 (Go handler).

- [ ] **Step 1: Edit the proto file**

In `proto/chainroute/v1/routing.proto`, add before `message FindRouteRequest`:

```proto
message CandidateEdge {
  string bridge_name = 1;
  double fee = 2;
  double latency_ms = 3;
  double liquidity = 4;
  double reliability = 5;
}
```

Then add one field to `FindRouteRequest`:

```proto
message FindRouteRequest {
  Chain source_chain = 1;
  Chain destination_chain = 2;
  Asset asset = 3;
  double amount = 4;
  repeated CandidateEdge candidate_edges = 5;
}
```

- [ ] **Step 2: Regenerate the Go stubs**

Find the exact `protoc` invocation already used for Go generation (check for a `Makefile`, `go:generate` directive, or script):

```bash
grep -rn "protoc-gen-go\|protoc --go" go-api/ scripts/ Makefile 2>/dev/null
```

Run the same command that originally generated `go-api/internal/gen/chainroute/v1/routing.pb.go` (it will be a `protoc` invocation with `--go_out`/`--go-grpc_out` targeting `proto/chainroute/v1/routing.proto`). If no script exists, run directly:

```bash
protoc -I proto --go_out=go-api/internal/gen --go_opt=paths=source_relative \
  --go-grpc_out=go-api/internal/gen --go-grpc_opt=paths=source_relative \
  proto/chainroute/v1/routing.proto
```

- [ ] **Step 3: Verify the Go module still builds**

Run: `cd go-api && go build ./...`
Expected: succeeds; `routingv1.CandidateEdge` and `FindRouteRequest.CandidateEdges`/`GetCandidateEdges()` now exist in `go-api/internal/gen/chainroute/v1/routing.pb.go`.

- [ ] **Step 4: Verify the C++ side regenerates via its existing CMake custom command**

The C++ build already regenerates protobuf/gRPC code from the same `.proto` file at build time (`cpp-routing-service/CMakeLists.txt`'s `add_custom_command` targeting `${PROTO_SRCS}`), so no manual C++ regeneration step is needed — just rebuild:

```bash
cmake -S cpp-routing-service -B cpp-routing-service/build && cmake --build cpp-routing-service/build
```

Expected: succeeds; `chainroute::v1::CandidateEdge` is now available in the generated headers under `cpp-routing-service/build/generated/`.

- [ ] **Step 5: Run every existing test to confirm zero regressions from the proto change alone**

```bash
cd go-api && go test ./... 2>&1 | tail -30
ctest --test-dir cpp-routing-service/build --output-on-failure
ctest --test-dir router/build --output-on-failure
```

Expected: all PASS — a purely additive proto field must not break anything that doesn't reference it yet.

- [ ] **Step 6: Commit**

```bash
git add proto/chainroute/v1/routing.proto go-api/internal/gen/chainroute/v1/
git commit -m "feat(proto): add CandidateEdge message and FindRouteRequest.candidate_edges field"
```

---

## Task 4: C++ — build the routing graph from candidate edges

**Files:**
- Modify: `cpp-routing-service/src/routing_service.cpp`
- Modify: `cpp-routing-service/include/chainroute_service/routing_service.hpp` (only if a new free function needs a forward declaration — check the header first; if `buildGraphFromCandidates` can stay file-local/static in the `.cpp`, no header change is needed)
- Test: `cpp-routing-service/tests/routing_service_test.cpp`
- Modify: `cpp-routing-service/tests/CMakeLists.txt` (only if a new test file is added instead of extending the existing one — plan below extends the existing file, so no CMake change needed)

**Interfaces:**
- Consumes: `chainroute::Graph`, `chainroute::Node`, `chainroute::Edge`, `chainroute::findCheapestRoute` (all existing, unchanged — from `router/include/chainroute/{graph,edge,node,route}.hpp`)
- Produces: `RoutingServiceImpl::FindRoute` now honors `request->candidate_edges()` when non-empty — no new public interface beyond the existing gRPC method.

- [ ] **Step 1: Write the failing tests**

Read `cpp-routing-service/src/routing_service.cpp` and `cpp-routing-service/include/chainroute_service/routing_service.hpp` first to confirm the current `FindRoute` signature and the `RoutingServiceImpl` constructor (`RoutingServiceImpl(chainroute::sim::NetworkSimulator&)`) before writing tests against it.

Append to `cpp-routing-service/tests/routing_service_test.cpp` (inside the existing anonymous namespace):

```cpp
TEST(RoutingServiceTest, UsesCandidateEdgesInsteadOfSimulatorWhenPresent) {
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_ETHEREUM);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_ETH);
    request.set_amount(0.001);

    auto* cheap = request.add_candidate_edges();
    cheap->set_bridge_name("across");
    cheap->set_fee(0.0001);
    cheap->set_latency_ms(60000.0);
    cheap->set_liquidity(0.001);
    cheap->set_reliability(1.0);

    auto* expensive = request.add_candidate_edges();
    expensive->set_bridge_name("other-provider");
    expensive->set_fee(0.01);
    expensive->set_latency_ms(60000.0);
    expensive->set_liquidity(0.001);
    expensive->set_reliability(1.0);

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    const grpc::Status status = service.FindRoute(&context, &request, &response);

    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(response.route_found());
    ASSERT_EQ(response.hops_size(), 1);
    EXPECT_EQ(response.hops(0).bridge_name(), "across");
    EXPECT_DOUBLE_EQ(response.hops(0).fee(), 0.0001);
    EXPECT_EQ(response.hops(0).from_chain(), chainroute::v1::CHAIN_ETHEREUM);
    EXPECT_EQ(response.hops(0).to_chain(), chainroute::v1::CHAIN_BASE);
    EXPECT_DOUBLE_EQ(response.total_fee(), 0.0001);
}

TEST(RoutingServiceTest, CandidateEdgeBelowLiquidityIsFilteredOut) {
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_ETHEREUM);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_ETH);
    request.set_amount(0.001);

    auto* tooSmall = request.add_candidate_edges();
    tooSmall->set_bridge_name("across");
    tooSmall->set_fee(0.0001);
    tooSmall->set_latency_ms(60000.0);
    tooSmall->set_liquidity(0.0);  // Available=false maps to liquidity=0 (design §4)
    tooSmall->set_reliability(1.0);

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    const grpc::Status status = service.FindRoute(&context, &request, &response);

    ASSERT_TRUE(status.ok());
    EXPECT_FALSE(response.route_found());
    EXPECT_EQ(response.hops_size(), 0);
}

TEST(RoutingServiceTest, EmptyCandidateEdgesFallsBackToSimulator) {
    // No candidate_edges set at all -- must take the exact same path as
    // every pre-Phase-8 test above, proving backward compatibility.
    chainroute::sim::NetworkSimulator simulator(1001);
    RoutingServiceImpl service(simulator);

    chainroute::v1::FindRouteRequest request;
    request.set_source_chain(chainroute::v1::CHAIN_ETHEREUM);
    request.set_destination_chain(chainroute::v1::CHAIN_BASE);
    request.set_asset(chainroute::v1::ASSET_USDC);
    request.set_amount(1000.0);

    chainroute::v1::FindRouteResponse response;
    grpc::ServerContext context;
    const grpc::Status status = service.FindRoute(&context, &request, &response);

    ASSERT_TRUE(status.ok());
    // Same assertion shape as the pre-existing ReturnsARouteForAValidRequest
    // test -- simulator-driven, so route_found depends on the seed's
    // topology, not asserted true/false here, only that no crash/error occurs.
}
```

- [ ] **Step 2: Run to verify the new tests fail to compile or fail**

```bash
cmake --build cpp-routing-service/build --target chainroute_service_tests
```

Expected: compiles (since `candidate_edges` already exists from Task 3) but `UsesCandidateEdgesInsteadOfSimulatorWhenPresent` and `CandidateEdgeBelowLiquidityIsFilteredOut` FAIL at runtime (still using the simulator snapshot, ignoring `candidate_edges`).

- [ ] **Step 3: Implement**

In `cpp-routing-service/src/routing_service.cpp`, add a file-local helper above `RoutingServiceImpl::FindRoute` and branch on it inside `FindRoute`:

```cpp
namespace {

chainroute::Graph buildGraphFromCandidates(
    chainroute::ChainId source, chainroute::ChainId dest, chainroute::AssetId asset,
    const google::protobuf::RepeatedPtrField<chainroute::v1::CandidateEdge>& candidates) {
    chainroute::Graph graph;
    const chainroute::NodeIndex sourceNode = graph.addNode(chainroute::Node{source, asset});
    const chainroute::NodeIndex destNode = graph.addNode(chainroute::Node{dest, asset});
    for (const auto& c : candidates) {
        graph.addEdge(sourceNode, chainroute::Edge{
            destNode, c.bridge_name(), c.fee(), c.latency_ms(), c.liquidity(), c.reliability()});
    }
    return graph;
}

}  // namespace
```

Then change the body of `FindRoute` (currently `const chainroute::Graph graph = simulator_.snapshot();`) to:

```cpp
const chainroute::Graph graph = request->candidate_edges_size() > 0
    ? buildGraphFromCandidates(*sourceChain, *destChain, *asset, request->candidate_edges())
    : simulator_.snapshot();
```

This must come *after* the existing `sourceChain`/`destChain`/`asset` validation (they're `std::optional`s already checked via `.has_value()` above this point — dereference only after those checks, matching the existing code's own ordering).

- [ ] **Step 4: Run to verify it passes**

```bash
cmake --build cpp-routing-service/build --target chainroute_service_tests
ctest --test-dir cpp-routing-service/build --output-on-failure
```

Expected: PASS — all new tests, and every pre-existing `RoutingServiceTest` case (simulator path untouched).

- [ ] **Step 5: Run the full existing router + cpp-routing-service suites to confirm no regression**

```bash
ctest --test-dir router/build --output-on-failure
```

Expected: PASS, unmodified.

- [ ] **Step 6: Commit**

```bash
git add cpp-routing-service/src/routing_service.cpp cpp-routing-service/tests/routing_service_test.cpp
git commit -m "feat(cpp-routing-service): build the routing graph from request candidate_edges when present"
```

---

## Task 5: Go — `quote` package (provider interface, normalized model, registry)

**Files:**
- Create: `go-api/internal/bridge/quote/quote.go`
- Create: `go-api/internal/bridge/quote/registry.go`
- Create: `go-api/internal/bridge/quote/quote_test.go`
- Create: `go-api/internal/bridge/quote/registry_test.go`

**Interfaces:**
- Produces: `quote.Provider` (interface), `quote.Request`, `quote.Quote`, `quote.RouteKey`, `quote.Registry` — consumed by Task 6 (`across.Provider` implements `quote.Provider`), Task 10 (handler builds `quote.Request`, reads `quote.Registry`), Task 11 (executor holds `map[string]quote.Provider`).

- [ ] **Step 1: Write `quote.go` (no test-first cycle for plain type declarations — these are exercised by Task 6's tests via the real `across.Provider`)**

```go
package quote

import (
	"context"
	"encoding/json"
	"math/big"
	"time"
)

// Provider is one bridge's live quote source. Implementations (Task 6's
// across.Provider) wrap an existing provider-specific HTTP client -- this
// package never makes an HTTP call itself.
type Provider interface {
	Name() string
	GetQuote(ctx context.Context, req Request) (Quote, error)
}

// Request is what the router needs quoted: a specific route, asset, and
// exact amount.
type Request struct {
	SourceChainID      int64
	DestinationChainID int64
	Asset              string // normalized symbol, e.g. "WETH"
	AmountBaseUnits    *big.Int
}

// Quote is the normalized result of asking one provider for one route's
// current terms. Provider-specific transaction-construction inputs
// (Across's exclusiveRelayer/fillDeadline/etc.) are NOT typed fields here
// -- they travel opaquely in RawProviderPayload, decoded only by that
// provider's own execution code (design doc §4).
type Quote struct {
	ProviderName          string
	SourceChainID         int64
	DestinationChainID    int64
	Asset                 string
	InputAmountBaseUnits  *big.Int
	OutputAmountBaseUnits *big.Int
	FeeBaseUnits          *big.Int // InputAmountBaseUnits - OutputAmountBaseUnits
	EstimatedFillTimeSec  int64
	Available             bool // false: no viable route/liquidity for this amount right now
	QuotedAt              time.Time
	ExpiresAt             time.Time // ChainRoute policy TTL, not a protocol guarantee
	RawProviderPayload    json.RawMessage
}
```

- [ ] **Step 2: Write `registry.go`**

```go
package quote

// RouteKey identifies one (source chain, destination chain, asset)
// combination ChainRoute may support in testnet-execution mode.
type RouteKey struct {
	SourceChainID      int64
	DestinationChainID int64
	Asset              string
}

// Registry is a small, static, in-process map from a supported route to
// the provider(s) that can quote it -- built once at server startup
// (design doc §3). It is not a service, not a cache, not backed by a
// database table.
type Registry struct {
	providers map[RouteKey][]Provider
}

func NewRegistry() *Registry {
	return &Registry{providers: make(map[RouteKey][]Provider)}
}

// Register adds p as a quoter for key. Multiple providers may be
// registered for the same key (not exercised in Phase 8, which registers
// exactly one).
func (r *Registry) Register(key RouteKey, p Provider) {
	r.providers[key] = append(r.providers[key], p)
}

// ProvidersFor returns every provider registered for key, or an empty
// (nil) slice if key is not supported -- callers must treat an empty
// result as "this route is not supported for live execution," never as
// an error to retry.
func (r *Registry) ProvidersFor(key RouteKey) []Provider {
	return r.providers[key]
}
```

- [ ] **Step 3: Write the failing tests**

`go-api/internal/bridge/quote/registry_test.go`:

```go
package quote

import (
	"context"
	"testing"
)

type fakeProvider struct{ name string }

func (f *fakeProvider) Name() string { return f.name }
func (f *fakeProvider) GetQuote(ctx context.Context, req Request) (Quote, error) {
	return Quote{ProviderName: f.name}, nil
}

func TestRegistry_ProvidersFor_RegisteredRouteReturnsProvider(t *testing.T) {
	r := NewRegistry()
	key := RouteKey{SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH"}
	p := &fakeProvider{name: "across"}
	r.Register(key, p)

	got := r.ProvidersFor(key)
	if len(got) != 1 || got[0].Name() != "across" {
		t.Fatalf("expected exactly [across], got %v", got)
	}
}

func TestRegistry_ProvidersFor_UnregisteredRouteReturnsEmpty(t *testing.T) {
	r := NewRegistry()
	got := r.ProvidersFor(RouteKey{SourceChainID: 1, DestinationChainID: 2, Asset: "USDC"})
	if len(got) != 0 {
		t.Fatalf("expected no providers for an unregistered route, got %v", got)
	}
}

func TestRegistry_ProvidersFor_DifferentAssetSameChainsIsUnregistered(t *testing.T) {
	r := NewRegistry()
	key := RouteKey{SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH"}
	r.Register(key, &fakeProvider{name: "across"})

	got := r.ProvidersFor(RouteKey{SourceChainID: 11155111, DestinationChainID: 84532, Asset: "USDC"})
	if len(got) != 0 {
		t.Fatalf("expected registering WETH not to also register USDC for the same chain pair, got %v", got)
	}
}
```

`go-api/internal/bridge/quote/quote_test.go` (a placeholder-free smoke test for the plain struct, since there's no logic in `quote.go` itself to unit test beyond construction — real normalization logic is tested in Task 6 against `across.Provider`):

```go
package quote

import (
	"math/big"
	"testing"
	"time"
)

func TestQuote_FeeIsInputMinusOutput_ByConvention(t *testing.T) {
	// This test documents the convention (design doc §4) that callers
	// constructing a Quote must uphold: FeeBaseUnits = input - output.
	// It is a convention enforced by provider implementations (Task 6),
	// not by this struct -- this test exists so a reader of quote_test.go
	// sees the invariant spelled out even though the struct can't enforce
	// it itself.
	input := big.NewInt(1_000_000_000_000_000)
	output := big.NewInt(997_592_172_330_233)
	q := Quote{
		InputAmountBaseUnits:  input,
		OutputAmountBaseUnits: output,
		FeeBaseUnits:          new(big.Int).Sub(input, output),
		QuotedAt:              time.Now(),
	}
	want := new(big.Int).Sub(input, output)
	if q.FeeBaseUnits.Cmp(want) != 0 {
		t.Fatalf("FeeBaseUnits = %s, want %s", q.FeeBaseUnits, want)
	}
}
```

- [ ] **Step 4: Run to verify the tests fail before implementation exists, then pass after**

Run: `cd go-api && go test ./internal/bridge/quote/... -v`
Expected (before Step 1/2's files exist): FAIL (package doesn't compile). After Steps 1-2: PASS.

- [ ] **Step 5: Commit**

```bash
git add go-api/internal/bridge/quote/
git commit -m "feat(go-api): add quote package -- Provider interface, normalized Quote model, Registry"
```

---

## Task 6: Go — `across.Provider` (wraps the existing Across client)

**Files:**
- Modify: `go-api/internal/bridge/across/quote.go` (add `estimatedFillTimeSec` field, add `ErrAmountTooLow` sentinel)
- Modify: `go-api/internal/bridge/across/quote_test.go` (extend for the new field/sentinel)
- Create: `go-api/internal/bridge/across/provider.go`
- Create: `go-api/internal/bridge/across/provider_test.go`

**Interfaces:**
- Consumes: `quote.Provider`, `quote.Request`, `quote.Quote` (Task 5); `across.Client.SuggestedFees` (existing, Phase 7).
- Produces: `across.NewProvider(client *Client, ttl time.Duration) *Provider` implementing `quote.Provider`; `across.DecodeQuotePayload(raw json.RawMessage) (QuotePayload, error)` — consumed by Task 11 (executor, to recover Across-specific fields for `BuildAndSignDepositV3Tx` from a fresh `quote.Quote`'s `RawProviderPayload`).

- [ ] **Step 1: Write the failing test for the sentinel + new field**

Add to `go-api/internal/bridge/across/quote_test.go`:

```go
func TestSuggestedFees_AmountTooLowIsErrAmountTooLow(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"isAmountTooLow":true,"outputAmount":"0","fillDeadline":"0","exclusivityDeadline":0,"exclusiveRelayer":"0x0","timestamp":"0","spokePoolAddress":"0x0"}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	_, err := c.SuggestedFees(context.Background(), 11155111, 84532, "0xin", "0xout", "1")
	if !errors.Is(err, ErrAmountTooLow) {
		t.Fatalf("expected errors.Is(err, ErrAmountTooLow), got %v", err)
	}
}

func TestSuggestedFees_ParsesEstimatedFillTimeSec(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(realSuggestedFeesFixture))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	resp, err := c.SuggestedFees(context.Background(), 11155111, 84532,
		"0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14", "0x4200000000000000000000000000000000000006", "1000000000000000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.EstimatedFillTimeSec != 10 {
		t.Fatalf("EstimatedFillTimeSec = %d, want 10 (per realSuggestedFeesFixture)", resp.EstimatedFillTimeSec)
	}
}
```

Add `"errors"` to the test file's imports if not already present.

- [ ] **Step 2: Run to verify it fails**

Run: `cd go-api && go test ./internal/bridge/across/... -run TestSuggestedFees_AmountTooLow -v`
Expected: FAIL (`ErrAmountTooLow` undefined; existing error is a bare `fmt.Errorf`)

- [ ] **Step 3: Implement in `quote.go`**

```go
package across

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
)

// ErrAmountTooLow is returned by SuggestedFees when Across reports
// isAmountTooLow=true. Exposed as a sentinel (not just an error string)
// so callers -- specifically Provider.GetQuote in provider.go -- can
// distinguish "this route has no viable quote for this amount" from a
// genuine request/network failure via errors.Is, rather than string
// matching.
var ErrAmountTooLow = errors.New("across reports this amount is too low for the requested route")

type SuggestedFeesResponse struct {
	OutputAmount          string         `json:"outputAmount"`
	FillDeadline          string         `json:"fillDeadline"`
	ExclusivityDeadline   int64          `json:"exclusivityDeadline"`
	ExclusiveRelayer      string         `json:"exclusiveRelayer"`
	Timestamp             string         `json:"timestamp"`
	SpokePoolAddress      string         `json:"spokePoolAddress"`
	IsAmountTooLow        bool           `json:"isAmountTooLow"`
	EstimatedFillTimeSec  int64          `json:"estimatedFillTimeSec"`
	InputToken            QuoteTokenInfo `json:"inputToken"`
	OutputToken           QuoteTokenInfo `json:"outputToken"`
}

type QuoteTokenInfo struct {
	Address  string `json:"address"`
	ChainID  int64  `json:"chainId"`
	Decimals int    `json:"decimals"`
}

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
		return SuggestedFeesResponse{}, fmt.Errorf("%w: origin=%d destination=%d amount=%s", ErrAmountTooLow, originChainID, destinationChainID, amount)
	}
	return out, nil
}
```

(This changes the isAmountTooLow error from an unwrapped `fmt.Errorf` to one wrapping `ErrAmountTooLow` via `%w` — every existing caller of `SuggestedFees` that only checked `err != nil` is unaffected; `Executor.signAndPersist`'s existing call site does exactly that.)

- [ ] **Step 4: Run to verify it passes**

Run: `cd go-api && go test ./internal/bridge/across/... -v`
Expected: PASS — all across-package tests, including every pre-existing one (verify `TestSuggestedFees_RejectsAmountTooLow` in the existing `quote_test.go` still passes; it only checks `err == nil`, unaffected by the wrap).

- [ ] **Step 5: Write the failing tests for `Provider`**

Create `go-api/internal/bridge/across/provider_test.go`:

```go
package across

import (
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"chainroute/go-api/internal/bridge/quote"
)

func TestProvider_GetQuote_HappyPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(realSuggestedFeesFixture))
	}))
	defer srv.Close()

	p := NewProvider(NewClient(srv.URL), 2*time.Minute)
	req := quote.Request{
		SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
		AmountBaseUnits: big.NewInt(1_000_000_000_000_000),
	}
	before := time.Now()
	q, err := p.GetQuote(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q.ProviderName != "across" {
		t.Errorf("ProviderName = %q, want across", q.ProviderName)
	}
	if !q.Available {
		t.Error("expected Available=true for a successful quote")
	}
	if q.OutputAmountBaseUnits.String() != "997592172330233" {
		t.Errorf("OutputAmountBaseUnits = %s, want 997592172330233", q.OutputAmountBaseUnits)
	}
	wantFee := new(big.Int).Sub(req.AmountBaseUnits, q.OutputAmountBaseUnits)
	if q.FeeBaseUnits.Cmp(wantFee) != 0 {
		t.Errorf("FeeBaseUnits = %s, want %s", q.FeeBaseUnits, wantFee)
	}
	if q.EstimatedFillTimeSec != 10 {
		t.Errorf("EstimatedFillTimeSec = %d, want 10", q.EstimatedFillTimeSec)
	}
	if q.QuotedAt.Before(before) {
		t.Error("QuotedAt should be set at call time, not zero/earlier")
	}
	if !q.ExpiresAt.After(q.QuotedAt) {
		t.Error("ExpiresAt must be after QuotedAt")
	}

	var payload QuotePayload
	if err := json.Unmarshal(q.RawProviderPayload, &payload); err != nil {
		t.Fatalf("RawProviderPayload did not unmarshal into QuotePayload: %v", err)
	}
	if payload.SpokePoolAddress != "0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662" {
		t.Errorf("payload.SpokePoolAddress = %q", payload.SpokePoolAddress)
	}
}

func TestProvider_GetQuote_AmountTooLowReturnsUnavailableNotError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"isAmountTooLow":true,"outputAmount":"0","fillDeadline":"0","exclusivityDeadline":0,"exclusiveRelayer":"0x0","timestamp":"0","spokePoolAddress":"0x0"}`))
	}))
	defer srv.Close()

	p := NewProvider(NewClient(srv.URL), time.Minute)
	q, err := p.GetQuote(context.Background(), quote.Request{
		SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
		AmountBaseUnits: big.NewInt(1),
	})
	if err != nil {
		t.Fatalf("expected nil error for an unavailable-but-successfully-answered quote, got %v", err)
	}
	if q.Available {
		t.Error("expected Available=false")
	}
}

func TestProvider_GetQuote_UnsupportedRouteIsAnError(t *testing.T) {
	p := NewProvider(NewClient("http://unused"), time.Minute)
	_, err := p.GetQuote(context.Background(), quote.Request{
		SourceChainID: 1, DestinationChainID: 2, Asset: "USDC",
		AmountBaseUnits: big.NewInt(1),
	})
	if err == nil {
		t.Fatal("expected an error for a route this provider does not support")
	}
}

func TestProvider_GetQuote_PropagatesTransientAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	p := NewProvider(NewClient(srv.URL), time.Minute)
	_, err := p.GetQuote(context.Background(), quote.Request{
		SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
		AmountBaseUnits: big.NewInt(1_000_000_000_000_000),
	})
	if err == nil {
		t.Fatal("expected a non-2xx API response to propagate as an error, not be swallowed into Available=false")
	}
}
```

- [ ] **Step 6: Run to verify it fails**

Run: `cd go-api && go test ./internal/bridge/across/... -run TestProvider -v`
Expected: FAIL (`Provider`/`NewProvider`/`QuotePayload` undefined)

- [ ] **Step 7: Implement `provider.go`**

```go
package across

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"chainroute/go-api/internal/bridge/quote"
)

// wethAddressByChainID is the same small, fixed testnet-asset map Phase 7
// already hardcodes in cmd/worker/main.go -- centralized here since
// Provider is now the one place that must resolve (chainID, asset) into
// an actual token address for a quote request.
var wethAddressByChainID = map[int64]string{
	11155111: "0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14", // Sepolia WETH
	84532:    "0x4200000000000000000000000000000000000006", // Base Sepolia WETH
}

// QuotePayload is exactly what Provider.GetQuote marshals into
// quote.Quote.RawProviderPayload, and exactly what DecodeQuotePayload
// unmarshals back -- the Across-specific fields BuildAndSignDepositV3Tx
// needs beyond the normalized Quote's own OutputAmountBaseUnits.
type QuotePayload struct {
	ExclusiveRelayer    string `json:"exclusiveRelayer"`
	QuoteTimestamp      string `json:"quoteTimestamp"`
	FillDeadline        string `json:"fillDeadline"`
	ExclusivityDeadline int64  `json:"exclusivityDeadline"`
	SpokePoolAddress    string `json:"spokePoolAddress"`
}

// DecodeQuotePayload is the inverse of the marshal Provider.GetQuote
// performs -- called by the worker (Task 11) to recover Across-specific
// signing inputs from a freshly-fetched quote.Quote's opaque payload.
func DecodeQuotePayload(raw json.RawMessage) (QuotePayload, error) {
	var p QuotePayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return QuotePayload{}, fmt.Errorf("decode across quote payload: %w", err)
	}
	return p, nil
}

// Provider implements quote.Provider by wrapping the existing,
// live-verified Client.SuggestedFees -- it introduces no second Across
// HTTP integration.
type Provider struct {
	Client   *Client
	QuoteTTL time.Duration
}

func NewProvider(client *Client, quoteTTL time.Duration) *Provider {
	return &Provider{Client: client, QuoteTTL: quoteTTL}
}

func (p *Provider) Name() string { return "across" }

func (p *Provider) GetQuote(ctx context.Context, req quote.Request) (quote.Quote, error) {
	if req.Asset != "WETH" {
		return quote.Quote{}, fmt.Errorf("across provider: unsupported asset %q (only WETH is supported)", req.Asset)
	}
	inputAddr, ok := wethAddressByChainID[req.SourceChainID]
	if !ok {
		return quote.Quote{}, fmt.Errorf("across provider: unsupported source chain %d", req.SourceChainID)
	}
	outputAddr, ok := wethAddressByChainID[req.DestinationChainID]
	if !ok {
		return quote.Quote{}, fmt.Errorf("across provider: unsupported destination chain %d", req.DestinationChainID)
	}

	now := time.Now().UTC()
	resp, err := p.Client.SuggestedFees(ctx, req.SourceChainID, req.DestinationChainID, inputAddr, outputAddr, req.AmountBaseUnits.String())
	if err != nil {
		if errors.Is(err, ErrAmountTooLow) {
			return quote.Quote{
				ProviderName: p.Name(), SourceChainID: req.SourceChainID, DestinationChainID: req.DestinationChainID,
				Asset: req.Asset, Available: false, QuotedAt: now, ExpiresAt: now.Add(p.QuoteTTL),
			}, nil
		}
		return quote.Quote{}, fmt.Errorf("across provider: get quote: %w", err)
	}

	// Validate the response's echoed chain IDs/addresses match what was
	// requested BEFORE trusting any of its numbers -- the same defense
	// Executor.validateQuote already applies at signing time (design §15),
	// applied here at quote-normalization time too.
	if resp.InputToken.ChainID != req.SourceChainID || resp.InputToken.Address != inputAddr {
		return quote.Quote{}, fmt.Errorf("across provider: response inputToken %+v does not match request", resp.InputToken)
	}
	if resp.OutputToken.ChainID != req.DestinationChainID || resp.OutputToken.Address != outputAddr {
		return quote.Quote{}, fmt.Errorf("across provider: response outputToken %+v does not match request", resp.OutputToken)
	}

	outputAmount, ok := new(big.Int).SetString(resp.OutputAmount, 10)
	if !ok {
		return quote.Quote{}, fmt.Errorf("across provider: outputAmount %q is not a valid integer", resp.OutputAmount)
	}
	feeAmount := new(big.Int).Sub(req.AmountBaseUnits, outputAmount)

	// quoteTimestamp is the same value the response calls "timestamp";
	// stored under the payload's own field name for BuildAndSignDepositV3Tx.
	payload := QuotePayload{
		ExclusiveRelayer: resp.ExclusiveRelayer, QuoteTimestamp: resp.Timestamp,
		FillDeadline: resp.FillDeadline, ExclusivityDeadline: resp.ExclusivityDeadline,
		SpokePoolAddress: resp.SpokePoolAddress,
	}
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return quote.Quote{}, fmt.Errorf("across provider: marshal raw payload: %w", err)
	}

	return quote.Quote{
		ProviderName: p.Name(), SourceChainID: req.SourceChainID, DestinationChainID: req.DestinationChainID,
		Asset: req.Asset, InputAmountBaseUnits: req.AmountBaseUnits, OutputAmountBaseUnits: outputAmount,
		FeeBaseUnits: feeAmount, EstimatedFillTimeSec: resp.EstimatedFillTimeSec,
		Available: true, QuotedAt: now, ExpiresAt: now.Add(p.QuoteTTL), RawProviderPayload: rawPayload,
	}, nil
}

var _ = strconv.Itoa // silence unused import if strconv ends up unused after edits; remove if not needed
```

(Drop the trailing `var _ = strconv.Itoa` line if `strconv` isn't actually imported/needed once written — it's a placeholder against a common goimports mistake, not something to leave in real code. Run `goimports -w` or `go vet` and delete unused imports before committing.)

- [ ] **Step 8: Run to verify it passes**

Run: `cd go-api && go test ./internal/bridge/across/... -v`
Expected: PASS — every test in the package, old and new.

- [ ] **Step 9: Commit**

```bash
git add go-api/internal/bridge/across/
git commit -m "feat(go-api): add across.Provider implementing quote.Provider, plus ErrAmountTooLow sentinel and estimatedFillTimeSec parsing"
```

---

## Task 7: Migration `0005` — `payment_quotes` and `payments.failure_reason`

**Files:**
- Create: `go-api/migrations/0005_realtime_bridge_routing.sql`

- [ ] **Step 1: Write the migration**

```sql
CREATE TABLE payment_quotes (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id              UUID NOT NULL REFERENCES payments(id) ON DELETE CASCADE,
    provider                TEXT NOT NULL,
    origin_chain_id         BIGINT NOT NULL,
    destination_chain_id    BIGINT NOT NULL,
    asset                   TEXT NOT NULL,
    input_amount            NUMERIC(38,0) NOT NULL,
    output_amount           NUMERIC(38,0) NOT NULL,
    fee_amount              NUMERIC(38,0) NOT NULL,
    estimated_fill_time_sec INTEGER NOT NULL,
    quoted_at               TIMESTAMPTZ NOT NULL,
    expires_at              TIMESTAMPTZ NOT NULL,
    raw_provider_payload    JSONB NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (payment_id)
);

ALTER TABLE payments
    ADD COLUMN failure_reason TEXT NULL;
```

- [ ] **Step 2: Apply it against a local Postgres and verify it's idempotent-safe alongside existing migrations**

Find how existing migrations are applied (check `scripts/e2e_test.sh` or the server/worker startup for a migration runner, or a `migrate` CLI invocation):

```bash
grep -rn "migrate\|0004_across" scripts/e2e_test.sh go-api/cmd/
```

Apply migrations `0001` through `0005` in order against a scratch database and confirm no errors:

```bash
psql "$DATABASE_URL" -f go-api/migrations/0001_create_payments.sql
psql "$DATABASE_URL" -f go-api/migrations/0002_payment_processing.sql
psql "$DATABASE_URL" -f go-api/migrations/0003_outbox_events_cascade_delete.sql
psql "$DATABASE_URL" -f go-api/migrations/0004_across_testnet_execution.sql
psql "$DATABASE_URL" -f go-api/migrations/0005_realtime_bridge_routing.sql
psql "$DATABASE_URL" -c "\d payment_quotes"
psql "$DATABASE_URL" -c "\d payments" | grep failure_reason
```

Expected: all five apply cleanly in sequence; `payment_quotes` exists with the columns above; `payments.failure_reason` exists.

- [ ] **Step 3: Commit**

```bash
git add go-api/migrations/0005_realtime_bridge_routing.sql
git commit -m "feat(go-api): add migration 0005 -- payment_quotes table and payments.failure_reason"
```

---

## Task 8: Go — domain model additions (`payment.Quote`, `FailureReason`)

**Files:**
- Modify: `go-api/internal/payment/payment.go`

**Interfaces:**
- Produces: `payment.Quote` struct; `payment.Payment.FailureReason *string`; `payment.Payment.Quote *Quote` (input-only field, set by the handler before calling `CreateOrGetPayment`, nil for simulated mode) — consumed by Task 9 (postgres), Task 10 (handler).

- [ ] **Step 1: Implement**

Add to `go-api/internal/payment/payment.go`:

```go
// Quote is the normalized bridge quote that produced a testnet-mode
// payment's winning route -- persisted once, atomically with the payment
// (design doc §7). nil for simulated-mode payments.
type Quote struct {
	ID                   string
	PaymentID            string
	Provider             string
	OriginChainID        int64
	DestinationChainID   int64
	Asset                string
	InputAmount          string // decimal string, same convention as Payment.Amount
	OutputAmount         string
	FeeAmount            string
	EstimatedFillTimeSec int64
	QuotedAt             time.Time
	ExpiresAt            time.Time
	RawProviderPayload   json.RawMessage
	CreatedAt            time.Time
}
```

Add `"encoding/json"` to the file's imports.

Extend the existing `Payment` struct with two fields (add after `BridgeProvider *string`):

```go
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
	FailureReason    *string // populated only for the two Phase 8 reasons: routing_quote_expired, fee_slippage_exceeded
	Quote            *Quote  // set by the caller before CreateOrGetPayment for testnet-mode; nil for simulated
	CreatedAt        time.Time
	UpdatedAt        time.Time
	CompletedAt      *time.Time
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cd go-api && go build ./...`
Expected: succeeds (no test to write for plain struct additions — Task 9's integration tests exercise these fields for real).

- [ ] **Step 3: Commit**

```bash
git add go-api/internal/payment/payment.go
git commit -m "feat(go-api): add payment.Quote domain type and Payment.FailureReason/Quote fields"
```

---

## Task 9: Postgres — `payment_quotes` persistence, `CreateOrGetPayment` extension, `MarkProcessingFailed`

This task also fixes a latent Phase 7 gap discovered during planning: **no store method exists today to transition a testnet-mode payment from `PROCESSING` to `FAILED` before broadcast.** Phase 7's `Executor.signAndPersist` already has one pre-broadcast rejection path (`MaxAmountWei` exceeded) that today returns a bare Go error with no `FAILED` transition — the payment is left stuck `PROCESSING` forever (Kafka redelivery no-ops per the existing `ClaimPayment` guard; the reconciler's stale-`PROCESSING` recovery just retries the same doomed call indefinitely). Phase 8's expiry/slippage checks need exactly this "mark FAILED before a nonce is ever allocated" capability, so this task adds the missing method and Task 11 uses it for all three pre-nonce-allocation rejection reasons (the two new ones, plus fixing the pre-existing `MaxAmountWei` case as a direct, minimal consequence of now having the mechanism).

**Files:**
- Create: `go-api/internal/postgres/quote_store.go`
- Create: `go-api/internal/postgres/quote_store_integration_test.go`
- Modify: `go-api/internal/postgres/store.go` (`CreateOrGetPayment`, `GetPayment`, `findByIdempotencyKey` — add `failure_reason` column read/write; `CreateOrGetPayment` writes `payment_quotes` when `p.Quote != nil`)
- Modify: `go-api/internal/postgres/store_integration_test.go` (extend for the new column/behavior)

**Interfaces:**
- Produces: `Store.GetQuoteByPaymentID(ctx, paymentID string) (payment.Quote, bool, error)`; `Store.MarkProcessingFailed(ctx, paymentID, reason string) (bool, error)` — consumed by Task 11.
- Consumes: `payment.Quote`, `payment.Payment.Quote`/`.FailureReason` (Task 8).

- [ ] **Step 1: Write the failing integration tests**

Create `go-api/internal/postgres/quote_store_integration_test.go` (mirror the existing `//go:build integration` tag and `setupTestDB`-style helper used by `store_integration_test.go` — read that file's first 40 lines first to copy the exact setup pattern before writing this):

```go
//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"chainroute/go-api/internal/payment"
)

func TestCreateOrGetPayment_PersistsQuoteAtomicallyWithPayment(t *testing.T) {
	store := newTestStore(t) // reuse the existing integration test helper -- confirm its exact name in store_integration_test.go
	now := time.Now().UTC()
	p := payment.Payment{
		IdempotencyKey: "quote-atomic-" + t.Name(), SourceChain: "ethereum", DestinationChain: "base",
		Asset: "eth", Amount: "0.001", ExecutionMode: payment.ExecutionModeTestnet,
		Hops: []payment.Hop{{HopIndex: 0, FromChain: "ethereum", ToChain: "base", BridgeName: "across", Fee: 0.0001, LatencyMs: 60000, Liquidity: 0.001, Reliability: 1.0}},
		Quote: &payment.Quote{
			Provider: "across", OriginChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
			InputAmount: "1000000000000000", OutputAmount: "999900000000000", FeeAmount: "100000000000",
			EstimatedFillTimeSec: 60, QuotedAt: now, ExpiresAt: now.Add(2 * time.Minute),
			RawProviderPayload: json.RawMessage(`{"spokePoolAddress":"0xabc"}`),
		},
	}

	created, outcome, err := store.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("CreateOrGetPayment: %v", err)
	}
	if outcome != payment.Created {
		t.Fatalf("expected Created, got %v", outcome)
	}

	q, found, err := store.GetQuoteByPaymentID(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("GetQuoteByPaymentID: %v", err)
	}
	if !found {
		t.Fatal("expected a payment_quotes row to exist")
	}
	if q.Provider != "across" || q.FeeAmount != "100000000000" {
		t.Errorf("unexpected quote: %+v", q)
	}
}

func TestCreateOrGetPayment_SimulatedModeHasNoQuoteRow(t *testing.T) {
	store := newTestStore(t)
	p := payment.Payment{
		IdempotencyKey: "no-quote-" + t.Name(), SourceChain: "ethereum", DestinationChain: "base",
		Asset: "usdc", Amount: "100", ExecutionMode: payment.ExecutionModeSimulated,
	}
	created, _, err := store.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("CreateOrGetPayment: %v", err)
	}
	_, found, err := store.GetQuoteByPaymentID(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("GetQuoteByPaymentID: %v", err)
	}
	if found {
		t.Fatal("simulated-mode payment must not have a payment_quotes row")
	}
}

func TestMarkProcessingFailed_TransitionsFromProcessingOnly(t *testing.T) {
	store := newTestStore(t)
	p := payment.Payment{
		IdempotencyKey: "mark-failed-" + t.Name(), SourceChain: "ethereum", DestinationChain: "base",
		Asset: "eth", Amount: "0.001", ExecutionMode: payment.ExecutionModeTestnet,
	}
	created, _, err := store.CreateOrGetPayment(context.Background(), p)
	if err != nil {
		t.Fatalf("CreateOrGetPayment: %v", err)
	}
	claimed, _, err := store.ClaimPayment(context.Background(), created.ID)
	if err != nil || !claimed {
		t.Fatalf("ClaimPayment: claimed=%v err=%v", claimed, err)
	}

	ok, err := store.MarkProcessingFailed(context.Background(), created.ID, "fee_slippage_exceeded")
	if err != nil {
		t.Fatalf("MarkProcessingFailed: %v", err)
	}
	if !ok {
		t.Fatal("expected MarkProcessingFailed to succeed from PROCESSING")
	}

	got, found, err := store.GetPayment(context.Background(), created.ID)
	if err != nil || !found {
		t.Fatalf("GetPayment: found=%v err=%v", found, err)
	}
	if got.Status != payment.StatusFailed {
		t.Errorf("Status = %v, want FAILED", got.Status)
	}
	if got.FailureReason == nil || *got.FailureReason != "fee_slippage_exceeded" {
		t.Errorf("FailureReason = %v, want fee_slippage_exceeded", got.FailureReason)
	}

	// Second call must be a safe no-op (guarded by WHERE status = 'PROCESSING').
	ok2, err := store.MarkProcessingFailed(context.Background(), created.ID, "routing_quote_expired")
	if err != nil {
		t.Fatalf("MarkProcessingFailed (second call): %v", err)
	}
	if ok2 {
		t.Fatal("expected the second MarkProcessingFailed call to report false -- payment is no longer PROCESSING")
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd go-api && go test -tags=integration ./internal/postgres/... -run "TestCreateOrGetPayment_PersistsQuote|TestCreateOrGetPayment_SimulatedModeHasNoQuoteRow|TestMarkProcessingFailed" -v`
Expected: FAIL (`GetQuoteByPaymentID`/`MarkProcessingFailed` undefined; migration `0005` must already be applied to the test DB per Task 7)

- [ ] **Step 3: Implement `quote_store.go`**

```go
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"chainroute/go-api/internal/payment"
)

func insertPaymentQuote(ctx context.Context, tx *sql.Tx, paymentID string, q *payment.Quote) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO payment_quotes
			(payment_id, provider, origin_chain_id, destination_chain_id, asset,
			 input_amount, output_amount, fee_amount, estimated_fill_time_sec,
			 quoted_at, expires_at, raw_provider_payload)
		VALUES ($1, $2, $3, $4, $5, $6::NUMERIC, $7::NUMERIC, $8::NUMERIC, $9, $10, $11, $12::JSONB)
	`, paymentID, q.Provider, q.OriginChainID, q.DestinationChainID, q.Asset,
		q.InputAmount, q.OutputAmount, q.FeeAmount, q.EstimatedFillTimeSec,
		q.QuotedAt, q.ExpiresAt, string(q.RawProviderPayload))
	if err != nil {
		return fmt.Errorf("insert payment_quotes: %w", err)
	}
	return nil
}

// GetQuoteByPaymentID returns the (at most one, per UNIQUE(payment_id))
// quote row for paymentID. found=false means this is a simulated-mode
// payment, or a testnet-mode payment somehow created without one (should
// be unreachable once Task 10 is wired -- CreateOrGetPayment always
// writes both together for testnet mode).
func (s *Store) GetQuoteByPaymentID(ctx context.Context, paymentID string) (payment.Quote, bool, error) {
	var q payment.Quote
	var rawPayload []byte
	row := s.db.QueryRowContext(ctx, `
		SELECT id, payment_id, provider, origin_chain_id, destination_chain_id, asset,
		       input_amount::text, output_amount::text, fee_amount::text, estimated_fill_time_sec,
		       quoted_at, expires_at, raw_provider_payload, created_at
		FROM payment_quotes
		WHERE payment_id = $1
	`, paymentID)
	err := row.Scan(&q.ID, &q.PaymentID, &q.Provider, &q.OriginChainID, &q.DestinationChainID, &q.Asset,
		&q.InputAmount, &q.OutputAmount, &q.FeeAmount, &q.EstimatedFillTimeSec,
		&q.QuotedAt, &q.ExpiresAt, &rawPayload, &q.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return payment.Quote{}, false, nil
	}
	if err != nil {
		return payment.Quote{}, false, fmt.Errorf("get quote by payment id: %w", err)
	}
	q.RawProviderPayload = rawPayload
	return q, true, nil
}

// MarkProcessingFailed transitions paymentID from PROCESSING to FAILED
// with a recorded reason, guarded by the same conditional-UPDATE pattern
// every other status transition in this package uses. ok=false means the
// payment was not PROCESSING (already terminal, or still ROUTED) -- a
// safe no-op the caller must not treat as an error, mirroring
// CompletePayment/MarkSubmitted's own convention.
func (s *Store) MarkProcessingFailed(ctx context.Context, paymentID, reason string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE payments
		SET status = 'FAILED', failure_reason = $2, updated_at = now()
		WHERE id = $1 AND status = 'PROCESSING'
	`, paymentID, reason)
	if err != nil {
		return false, fmt.Errorf("mark processing failed: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("mark processing failed: rows affected: %w", err)
	}
	return n == 1, nil
}
```

- [ ] **Step 4: Wire `CreateOrGetPayment`, `GetPayment`, `findByIdempotencyKey` for `failure_reason` and the quote insert**

In `go-api/internal/postgres/store.go`:

In `findByIdempotencyKey`'s `SELECT`, add `failure_reason` to the column list and scan target (a new `sql.NullString`), setting `existing.FailureReason` when valid — mirror exactly how `bridgeProvider`/`completedAt` are already handled in that function.

In `GetPayment`, make the identical addition (same column, same `sql.NullString` pattern, same place in the `SELECT`/`Scan` calls).

In `CreateOrGetPayment`, after the existing hop-insertion loop (`for _, h := range p.Hops { ... }`) and before the outbox-payload marshal, add:

```go
	if p.Quote != nil {
		if err := insertPaymentQuote(ctx, tx, created.ID, p.Quote); err != nil {
			return payment.Payment{}, 0, err
		}
	}
```

This runs inside the same `tx` the payment/hops/outbox-event insert already uses — no new transaction boundary.

- [ ] **Step 5: Run to verify it passes**

Run: `cd go-api && go test -tags=integration ./internal/postgres/... -v`
Expected: PASS — every postgres integration test, old and new. (Requires a local Postgres with migrations `0001`-`0005` applied, per the existing integration-test setup Task 7's Step 2 already exercised.)

- [ ] **Step 6: Commit**

```bash
git add go-api/internal/postgres/
git commit -m "feat(go-api): persist payment_quotes atomically with CreateOrGetPayment; add MarkProcessingFailed"
```

---

## Task 10: Go — wire live quote ingestion into `PostPayments`

**Files:**
- Modify: `go-api/internal/handler/payments.go`
- Modify: `go-api/internal/handler/payments_test.go`

**Interfaces:**
- Consumes: `quote.Registry`, `quote.RouteKey`, `quote.Request`, `quote.Quote` (Task 5); `money.BaseUnitsToDecimal` (Task 2); `routingv1.CandidateEdge` (Task 3); `payment.Quote` (Task 8).
- Produces: `Handler.QuoteRegistry *quote.Registry` field; `Handler.RoutingQuoteTTL time.Duration` field — consumed by Task 12 (`cmd/server/main.go` wiring).

- [ ] **Step 1: Read the current `PostPayments` in full** (already read during design; re-read now to get exact line anchors before editing) to identify the exact block to replace: the `if mode == payment.ExecutionModeTestnet { ... }` block that currently hardcodes the ethereum/base/eth string check and sets `bridgeProvider := "across"`.

- [ ] **Step 2: Write the failing tests**

Add to `go-api/internal/handler/payments_test.go` (match the existing file's fake-`RoutingClient`/fake-`PaymentStore` pattern — read its current fakes before writing these):

```go
type fakeQuoteProvider struct {
	name  string
	quote quote.Quote
	err   error
}

func (f *fakeQuoteProvider) Name() string { return f.name }
func (f *fakeQuoteProvider) GetQuote(ctx context.Context, req quote.Request) (quote.Quote, error) {
	return f.quote, f.err
}

func TestPostPayments_TestnetMode_UnregisteredRouteReturns400(t *testing.T) {
	h := &Handler{
		Client: &fakeRoutingClient{}, Store: &fakePaymentStore{}, BlockchainEnv: "testnet",
		QuoteRegistry: quote.NewRegistry(), // nothing registered
	}
	req := httptest.NewRequest("POST", "/payments", strings.NewReader(
		`{"source_chain":"arbitrum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`))
	req.Header.Set("Idempotency-Key", "test-unregistered-route")
	w := httptest.NewRecorder()

	h.PostPayments(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %s", w.Code, w.Body.String())
	}
}

func TestPostPayments_TestnetMode_ProviderReportsUnavailableReturns422(t *testing.T) {
	registry := quote.NewRegistry()
	key := quote.RouteKey{SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH"}
	registry.Register(key, &fakeQuoteProvider{name: "across", quote: quote.Quote{ProviderName: "across", Available: false}})

	h := &Handler{
		Client: &fakeRoutingClient{}, Store: &fakePaymentStore{}, BlockchainEnv: "testnet",
		QuoteRegistry: registry,
	}
	req := httptest.NewRequest("POST", "/payments", strings.NewReader(
		`{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`))
	req.Header.Set("Idempotency-Key", "test-unavailable")
	w := httptest.NewRecorder()

	h.PostPayments(w, req)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body = %s", w.Code, w.Body.String())
	}
}

func TestPostPayments_TestnetMode_ProviderErrorReturns503(t *testing.T) {
	registry := quote.NewRegistry()
	key := quote.RouteKey{SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH"}
	registry.Register(key, &fakeQuoteProvider{name: "across", err: errors.New("connection refused")})

	h := &Handler{
		Client: &fakeRoutingClient{}, Store: &fakePaymentStore{}, BlockchainEnv: "testnet",
		QuoteRegistry: registry,
	}
	req := httptest.NewRequest("POST", "/payments", strings.NewReader(
		`{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`))
	req.Header.Set("Idempotency-Key", "test-provider-error")
	w := httptest.NewRecorder()

	h.PostPayments(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body = %s", w.Code, w.Body.String())
	}
}

func TestPostPayments_TestnetMode_CandidateEdgeBuiltFromLiveQuote(t *testing.T) {
	registry := quote.NewRegistry()
	key := quote.RouteKey{SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH"}
	registry.Register(key, &fakeQuoteProvider{name: "across", quote: quote.Quote{
		ProviderName: "across", Available: true,
		InputAmountBaseUnits: big.NewInt(1_000_000_000_000_000), OutputAmountBaseUnits: big.NewInt(999_900_000_000_000),
		FeeBaseUnits: big.NewInt(100_000_000_000), EstimatedFillTimeSec: 60,
		QuotedAt: time.Now(), ExpiresAt: time.Now().Add(time.Minute),
		RawProviderPayload: json.RawMessage(`{"spokePoolAddress":"0xabc"}`),
	}})

	fakeClient := &fakeRoutingClient{
		captureRequest: true,
		response: &routingv1.FindRouteResponse{
			RouteFound: true, TotalFee: 0.0001,
			Hops: []*routingv1.RouteHop{{FromChain: routingv1.Chain_CHAIN_ETHEREUM, ToChain: routingv1.Chain_CHAIN_BASE, BridgeName: "across", Fee: 0.0001, LatencyMs: 60000, Liquidity: 0.001, Reliability: 1.0}},
		},
	}
	store := &fakePaymentStore{}
	h := &Handler{Client: fakeClient, Store: store, BlockchainEnv: "testnet", QuoteRegistry: registry}

	req := httptest.NewRequest("POST", "/payments", strings.NewReader(
		`{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`))
	req.Header.Set("Idempotency-Key", "test-candidate-edge")
	w := httptest.NewRecorder()

	h.PostPayments(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body = %s", w.Code, w.Body.String())
	}
	if len(fakeClient.lastRequest.GetCandidateEdges()) != 1 {
		t.Fatalf("expected exactly 1 candidate edge sent to FindRoute, got %d", len(fakeClient.lastRequest.GetCandidateEdges()))
	}
	edge := fakeClient.lastRequest.GetCandidateEdges()[0]
	if edge.GetBridgeName() != "across" {
		t.Errorf("bridge_name = %q, want across", edge.GetBridgeName())
	}
	if edge.GetLiquidity() != 0.001 {
		t.Errorf("liquidity = %v, want 0.001 (the request amount, since Available=true)", edge.GetLiquidity())
	}
	if store.lastCreated.Quote == nil {
		t.Fatal("expected the created payment to carry a Quote to persist")
	}
	if store.lastCreated.Quote.Provider != "across" {
		t.Errorf("persisted Quote.Provider = %q, want across", store.lastCreated.Quote.Provider)
	}
}
```

Note: this task's tests assume `fakeRoutingClient` gains a `captureRequest`/`lastRequest` capability and `fakePaymentStore` gains a `lastCreated` capture — read the existing fakes in `payments_test.go` first; extend them minimally (add the two fields and set them in the existing fake methods) rather than replacing them, so every pre-existing test in the file keeps passing unmodified.

- [ ] **Step 3: Run to verify it fails**

Run: `cd go-api && go test ./internal/handler/... -run TestPostPayments_TestnetMode -v`
Expected: FAIL (compile error: `Handler.QuoteRegistry` undefined, etc.)

- [ ] **Step 4: Implement**

In `go-api/internal/handler/payments.go`:

Add to `Handler`:
```go
type Handler struct {
	Client              RoutingClient
	Store               PaymentStore
	BlockchainEnv       string
	MaxTestnetAmountWei *big.Int
	QuoteRegistry       *quote.Registry // nil when BlockchainEnv != "testnet"; never consulted otherwise
}
```

Add a small chain-ID/asset resolution map near the existing `chainByName`/`assetByName` maps:

```go
// testnetChainIDByChain maps the routing proto's chain-family enum to the
// specific testnet chain ID Phase 7/8 execution actually uses. This is a
// deliberately separate concept from routingv1.Chain (which represents a
// chain family, not a specific network) -- exactly why Phase 7 already
// kept this mapping local to testnet-mode code rather than extending the
// proto enum.
var testnetChainIDByChain = map[routingv1.Chain]int64{
	routingv1.Chain_CHAIN_ETHEREUM: 11155111,
	routingv1.Chain_CHAIN_BASE:     84532,
}

// bridgedAssetSymbol maps the API's asset name to the actual on-chain
// asset a testnet-mode payment bridges as -- "eth" is requested but
// bridged as WETH, matching Phase 7's existing handler comment/behavior.
var bridgedAssetSymbol = map[string]string{
	"eth": "WETH",
}
```

Replace the existing testnet-mode block (the `if mode == payment.ExecutionModeTestnet { ... }` that currently does the hardcoded chain-name string comparison and sets `bridgeProvider := "across"`) with:

```go
	var bridgeProvider *string
	var candidateEdges []*routingv1.CandidateEdge
	quotesByBridgeName := map[string]quote.Quote{}

	if mode == payment.ExecutionModeTestnet {
		if h.BlockchainEnv != "testnet" {
			writeError(w, http.StatusBadRequest, "execution_mode=testnet is not enabled on this server")
			return
		}

		originChainID, ok1 := testnetChainIDByChain[sourceChain]
		destChainID, ok2 := testnetChainIDByChain[destChain]
		bridgedAsset, ok3 := bridgedAssetSymbol[strings.ToLower(req.Asset)]
		if !ok1 || !ok2 || !ok3 {
			writeError(w, http.StatusBadRequest, "execution_mode=testnet does not support this source_chain/destination_chain/asset combination")
			return
		}

		routeKey := quote.RouteKey{SourceChainID: originChainID, DestinationChainID: destChainID, Asset: bridgedAsset}
		providers := h.QuoteRegistry.ProvidersFor(routeKey)
		if len(providers) == 0 {
			writeError(w, http.StatusBadRequest, "execution_mode=testnet does not support this source_chain/destination_chain/asset combination")
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

		anyAvailable := false
		for _, p := range providers {
			q, err := p.GetQuote(r.Context(), quote.Request{
				SourceChainID: originChainID, DestinationChainID: destChainID, Asset: bridgedAsset, AmountBaseUnits: amountWei,
			})
			if err != nil {
				log.Printf("ERROR: quote provider %s failed: %v", p.Name(), err)
				writeError(w, http.StatusServiceUnavailable, "bridge quote provider unavailable")
				return
			}
			if !q.Available {
				continue
			}
			anyAvailable = true
			feeDecimal := money.BaseUnitsToDecimal(q.FeeBaseUnits, 18)
			feeFloat, _ := strconv.ParseFloat(feeDecimal, 64)
			candidateEdges = append(candidateEdges, &routingv1.CandidateEdge{
				BridgeName: p.Name(), Fee: feeFloat, LatencyMs: float64(q.EstimatedFillTimeSec) * 1000,
				Liquidity: req.Amount_, Reliability: 1.0,
			})
			quotesByBridgeName[p.Name()] = q
		}
		if !anyAvailable {
			writeError(w, http.StatusUnprocessableEntity, "no route available for the requested payment")
			return
		}

		provider := "across" // overwritten below once the winning hop is known; placeholder to keep bridgeProvider non-nil until then
		bridgeProvider = &provider
	}
```

Note: `req.Amount_` above is a placeholder name to flag a real ambiguity you must resolve while implementing: the handler already has a `float64` amount (currently computed later, in the existing code, as `amountForRouting, err := strconv.ParseFloat(req.Amount, 64)`, *after* this block today). Move that `strconv.ParseFloat(req.Amount, 64)` conversion to happen *before* this testnet block (it's needed here for `Liquidity`, and it's still needed afterward for `grpcReq.Amount` regardless of mode) — do not duplicate the parse. Rename appropriately once moved; there is no `req.Amount_` field.

After the existing `resp, err := h.Client.FindRoute(...)` call and the existing `!resp.GetRouteFound()` check, and after the existing `hops := make([]payment.Hop, 0, ...)` loop that builds `hops` from `resp.GetHops()`, add (testnet mode only) the logic that determines which quote actually won and finalizes `bridgeProvider`:

```go
	if mode == payment.ExecutionModeTestnet && len(hops) > 0 {
		winningQuote, ok := quotesByBridgeName[hops[0].BridgeName]
		if !ok {
			log.Printf("ERROR: winning hop bridge_name %q has no matching fetched quote -- this should be unreachable", hops[0].BridgeName)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		provider := winningQuote.ProviderName
		bridgeProvider = &provider
	}
```

Finally, when building `candidate` (the `payment.Payment{...}` passed to `CreateOrGetPayment`), attach the quote for testnet mode:

```go
	candidate := payment.Payment{
		IdempotencyKey: idempotencyKey, SourceChain: chainNameByValue[sourceChain],
		DestinationChain: chainNameByValue[destChain], Asset: assetNameByValue[asset],
		Amount: req.Amount, TotalFee: resp.GetTotalFee(), Hops: hops,
		ExecutionMode: mode, BridgeProvider: bridgeProvider,
	}
	if mode == payment.ExecutionModeTestnet && len(hops) > 0 {
		winningQuote := quotesByBridgeName[hops[0].BridgeName]
		candidate.Quote = &payment.Quote{
			Provider: winningQuote.ProviderName, OriginChainID: winningQuote.SourceChainID,
			DestinationChainID: winningQuote.DestinationChainID, Asset: winningQuote.Asset,
			InputAmount: money.BaseUnitsToDecimal(winningQuote.InputAmountBaseUnits, 18),
			OutputAmount: money.BaseUnitsToDecimal(winningQuote.OutputAmountBaseUnits, 18),
			FeeAmount: money.BaseUnitsToDecimal(winningQuote.FeeBaseUnits, 18),
			EstimatedFillTimeSec: winningQuote.EstimatedFillTimeSec,
			QuotedAt: winningQuote.QuotedAt, ExpiresAt: winningQuote.ExpiresAt,
			RawProviderPayload: winningQuote.RawProviderPayload,
		}
	}
```

`candidate.Quote`'s amount fields (`InputAmount`/etc.) are decimal strings, matching `payment.Quote`'s convention from Task 8 — `money.BaseUnitsToDecimal` (Task 2) is what produces them; `NUMERIC(38,0)` in the schema (Task 7) stores base-units integers, so double-check at implementation time whether storing the *base-units integer as a decimal string* (e.g. `"1000000000000000"`) or the *human decimal* (`"0.001"`) is correct against the `NUMERIC(38,0)` column type — `NUMERIC(38,0)` has zero decimal places, so it must receive the base-units integer string, not the human-decimal string. Fix `payment.Quote`'s field semantics to be explicit about this before writing Task 9's SQL: **`payment.Quote.InputAmount`/`OutputAmount`/`FeeAmount` are base-units integer decimal strings (e.g. `"1000000000000000"`), not human-decimal amounts** — so the conversion above should be `winningQuote.InputAmountBaseUnits.String()`, not `money.BaseUnitsToDecimal(...)`. Use `.String()` on the `*big.Int` fields directly when building `candidate.Quote`; reserve `money.BaseUnitsToDecimal` for the `CandidateEdge.Fee`/`Liquidity` proto `double` conversion only, where a true decimal (not an integer string) is actually needed.

Add `"chainroute/go-api/internal/bridge/quote"` to the file's imports.

- [ ] **Step 5: Run to verify it passes**

Run: `cd go-api && go test ./internal/handler/... -v`
Expected: PASS — every handler test, old and new. Pay particular attention to any pre-existing test that exercised the old hardcoded ethereum/base/eth check with a *different* chain (e.g. arbitrum) expecting 400 — it should still get 400, now via the registry-empty path instead of the string-comparison path.

- [ ] **Step 6: Commit**

```bash
git add go-api/internal/handler/payments.go go-api/internal/handler/payments_test.go
git commit -m "feat(go-api): fetch live bridge quotes and build candidate_edges in PostPayments, replacing the hardcoded route check"
```

---

## Task 11: Go — restructure `Executor` for pre-nonce-allocation expiry/slippage checks

**Files:**
- Modify: `go-api/internal/worker/executor.go`
- Modify: `go-api/internal/worker/executor_test.go`

**Interfaces:**
- Consumes: `Store.GetQuoteByPaymentID`, `Store.MarkProcessingFailed` (Task 9); `quote.Provider`, `quote.Request` (Task 5); `across.DecodeQuotePayload` (Task 6).
- Produces: `Executor.QuoteProviders map[string]quote.Provider`; `Executor.MaxFeeSlippageBps int64` — consumed by Task 12 (`cmd/worker/main.go` wiring).

- [ ] **Step 1: Write the failing tests**

Add to `go-api/internal/worker/executor_test.go` (reuse `fakeExecutorStore` and `fakeExecutorEthClient`; extend `fakeExecutorStore` with the two new methods it must satisfy for the new `ExecutorStore` interface, and add a `fakeQuoteProvider` local to this package matching the one from Task 10's handler tests but importing `quote` directly):

```go
type fakeExecutorQuoteProvider struct {
	quote quote.Quote
	err   error
}

func (f *fakeExecutorQuoteProvider) Name() string { return "across" }
func (f *fakeExecutorQuoteProvider) GetQuote(ctx context.Context, req quote.Request) (quote.Quote, error) {
	return f.quote, f.err
}

// Extend fakeExecutorStore with the two new methods (add these to the
// existing struct/methods in executor_test.go rather than duplicating the
// type):
//
// func (f *fakeExecutorStore) GetQuoteByPaymentID(ctx context.Context, paymentID string) (payment.Quote, bool, error) {
// 	return f.quoteRow, f.quoteFound, f.quoteErr
// }
// func (f *fakeExecutorStore) MarkProcessingFailed(ctx context.Context, paymentID, reason string) (bool, error) {
// 	f.markFailedReason = reason
// 	return true, nil
// }
//
// Add fields quoteRow payment.Quote, quoteFound bool, quoteErr error,
// markFailedReason string to the struct definition.

func rawAcrossPayload() json.RawMessage {
	return json.RawMessage(`{"exclusiveRelayer":"0x0000000000000000000000000000000000000000","quoteTimestamp":"1789340112","fillDeadline":"1789347312","exclusivityDeadline":0,"spokePoolAddress":"0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662"}`)
}

func TestExecuteTestnetPayment_ExpiredQuoteFailsBeforeNonceAllocation(t *testing.T) {
	store := &fakeExecutorStore{
		quoteFound: true,
		quoteRow: payment.Quote{
			Provider: "across", OriginChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
			FeeAmount: "100000000000", ExpiresAt: time.Now().Add(-time.Minute), // already expired
		},
		pmt: payment.Payment{ID: "pay-expired", Amount: "0.001"}, pmtFound: true,
	}
	ethClient := &fakeExecutorEthClient{}
	e := newTestExecutor(t, store, ethClient)
	e.QuoteProviders = map[string]quote.Provider{"across": &fakeExecutorQuoteProvider{}}

	if err := e.ExecuteTestnetPayment(context.Background(), "pay-expired"); err != nil {
		t.Fatalf("unexpected error (an expiry rejection is a handled outcome, not a Go error): %v", err)
	}
	if store.markFailedReason != "routing_quote_expired" {
		t.Errorf("markFailedReason = %q, want routing_quote_expired", store.markFailedReason)
	}
	if store.tryCreateCalled {
		t.Fatal("must never allocate a nonce (call TryCreateExecution) when the routing quote has already expired")
	}
	if ethClient.sendCalled {
		t.Fatal("must never broadcast when the routing quote has already expired")
	}
}

func TestExecuteTestnetPayment_ExcessiveSlippageFailsBeforeNonceAllocation(t *testing.T) {
	store := &fakeExecutorStore{
		quoteFound: true,
		quoteRow: payment.Quote{
			Provider: "across", OriginChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
			FeeAmount: "100000000000", ExpiresAt: time.Now().Add(time.Hour),
		},
		pmt: payment.Payment{ID: "pay-slippage", Amount: "0.001"}, pmtFound: true,
	}
	ethClient := &fakeExecutorEthClient{}
	e := newTestExecutor(t, store, ethClient)
	e.MaxFeeSlippageBps = 500 // 5%
	// Fresh fee is double the routing-time fee -- far beyond 5% tolerance.
	e.QuoteProviders = map[string]quote.Provider{"across": &fakeExecutorQuoteProvider{
		quote: quote.Quote{
			ProviderName: "across", Available: true,
			FeeBaseUnits: big.NewInt(200_000_000_000), OutputAmountBaseUnits: big.NewInt(800_000_000_000_000),
			RawProviderPayload: rawAcrossPayload(),
		},
	}}

	if err := e.ExecuteTestnetPayment(context.Background(), "pay-slippage"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store.markFailedReason != "fee_slippage_exceeded" {
		t.Errorf("markFailedReason = %q, want fee_slippage_exceeded", store.markFailedReason)
	}
	if store.tryCreateCalled {
		t.Fatal("must never allocate a nonce when fresh fee exceeds slippage tolerance")
	}
	if ethClient.sendCalled {
		t.Fatal("must never broadcast when fresh fee exceeds slippage tolerance")
	}
}

func TestExecuteTestnetPayment_FeeDecreaseAlwaysPasses(t *testing.T) {
	store := &fakeExecutorStore{
		quoteFound: true,
		quoteRow: payment.Quote{
			Provider: "across", OriginChainID: 11155111, DestinationChainID: 84532, Asset: "WETH",
			FeeAmount: "200000000000", ExpiresAt: time.Now().Add(time.Hour),
		},
		tryCreateCreated: true,
		tryCreateExec:    payment.Execution{ID: "exec-cheaper", PaymentID: "pay-cheaper", Nonce: 1},
		pmt:              payment.Payment{ID: "pay-cheaper", Amount: "0.001"}, pmtFound: true,
	}
	ethClient := &fakeExecutorEthClient{txByHashFound: false, txByHashErr: gethereum.NotFound}
	e := newTestExecutor(t, store, ethClient)
	e.MaxFeeSlippageBps = 500
	// Fresh fee is LOWER than the routing-time fee -- must always pass,
	// regardless of tolerance (one-directional check, design doc §9).
	e.QuoteProviders = map[string]quote.Provider{"across": &fakeExecutorQuoteProvider{
		quote: quote.Quote{
			ProviderName: "across", Available: true,
			FeeBaseUnits: big.NewInt(50_000_000_000), OutputAmountBaseUnits: big.NewInt(950_000_000_000_000),
			RawProviderPayload: rawAcrossPayload(),
		},
	}}

	if err := e.ExecuteTestnetPayment(context.Background(), "pay-cheaper"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store.markFailedReason != "" {
		t.Fatalf("expected no failure for a fee decrease, got reason %q", store.markFailedReason)
	}
	if !ethClient.sendCalled {
		t.Fatal("expected broadcast to proceed when the fresh fee is lower than the routing-time quote")
	}
}

func TestExecuteTestnetPayment_UnknownPersistedProviderIsHardError(t *testing.T) {
	store := &fakeExecutorStore{
		quoteFound: true,
		quoteRow:   payment.Quote{Provider: "some-future-provider", ExpiresAt: time.Now().Add(time.Hour)},
		pmt:        payment.Payment{ID: "pay-unknown-provider", Amount: "0.001"}, pmtFound: true,
	}
	ethClient := &fakeExecutorEthClient{}
	e := newTestExecutor(t, store, ethClient)
	e.QuoteProviders = map[string]quote.Provider{"across": &fakeExecutorQuoteProvider{}} // no entry for "some-future-provider"

	err := e.ExecuteTestnetPayment(context.Background(), "pay-unknown-provider")
	if err == nil {
		t.Fatal("expected a hard error when the persisted provider has no configured client -- never a silent substitution")
	}
	if store.tryCreateCalled {
		t.Fatal("must never allocate a nonce for a provider this worker cannot execute")
	}
}
```

Note: `fakeExecutorStore` needs a `tryCreateCalled bool` flag set inside its existing `TryCreateExecution` method (add `f.tryCreateCalled = true` as its first line) so these new tests can assert "no nonce allocation attempted."

- [ ] **Step 2: Run to verify it fails**

Run: `cd go-api && go test ./internal/worker/... -run "TestExecuteTestnetPayment_Expired|TestExecuteTestnetPayment_Excessive|TestExecuteTestnetPayment_FeeDecrease|TestExecuteTestnetPayment_UnknownPersistedProvider" -v`
Expected: FAIL (compile error: `Executor.QuoteProviders`/`MaxFeeSlippageBps` undefined; `ExecutorStore` missing methods)

- [ ] **Step 3: Implement**

In `go-api/internal/worker/executor.go`:

Extend `ExecutorStore`:
```go
type ExecutorStore interface {
	TryCreateExecution(ctx context.Context, p postgres.CreateExecutionParams) (payment.Execution, bool, error)
	GetPayment(ctx context.Context, id string) (payment.Payment, bool, error)
	GetQuoteByPaymentID(ctx context.Context, paymentID string) (payment.Quote, bool, error)
	MarkProcessingFailed(ctx context.Context, paymentID, reason string) (bool, error)
	PersistSignedExecution(ctx context.Context, executionID string, rawTx []byte, txHash string) error
	MarkExecutionBroadcast(ctx context.Context, executionID string) error
	MarkSubmitted(ctx context.Context, paymentID string) (bool, error)
}
```

Extend `Executor`:
```go
type Executor struct {
	Store             ExecutorStore
	Wallet            *evm.Wallet
	OriginClient      ExecutorEthClient
	Across            *across.Client
	QuoteProviders    map[string]quote.Provider // keyed by provider name, e.g. "across"
	MaxFeeSlippageBps int64
	BridgeProvider    string
	OriginChainID     int64
	DestChainID       int64
	SpokePoolAddress  common.Address
	WETHOrigin        common.Address
	WETHDestination   common.Address
	MaxAmountWei      *big.Int
}
```

(`BridgeProvider`/`OriginChainID`/`DestChainID`/`SpokePoolAddress`/`WETHOrigin`/`WETHDestination` stay on `Executor` for now, still used inside `broadcastWithRecovery` and `DepositV3Params` construction — Phase 8 reads *which provider* dynamically per-payment via the new check below, but Phase 8 has exactly one provider/one route, so these remain the concrete Across/Sepolia/Base-Sepolia constants `cmd/worker/main.go` wires; the new dynamic check is what makes it a hard error, not a silent substitution, on the day a payment's persisted provider ever diverges from these constants.)

Rewrite `ExecuteTestnetPayment`:
```go
func (e *Executor) ExecuteTestnetPayment(ctx context.Context, paymentID string) error {
	quoteRow, found, err := e.Store.GetQuoteByPaymentID(ctx, paymentID)
	if err != nil {
		return fmt.Errorf("get quote for payment %s: %w", paymentID, err)
	}
	if !found {
		return fmt.Errorf("payment %s has no payment_quotes row -- cannot execute without a selected route", paymentID)
	}

	if time.Now().After(quoteRow.ExpiresAt) {
		if _, err := e.Store.MarkProcessingFailed(ctx, paymentID, "routing_quote_expired"); err != nil {
			return fmt.Errorf("mark payment %s failed (routing_quote_expired): %w", paymentID, err)
		}
		return nil
	}

	provider, ok := e.QuoteProviders[quoteRow.Provider]
	if !ok {
		return fmt.Errorf("payment %s uses provider %q, which this worker has no configured client for -- refusing to substitute a different provider", paymentID, quoteRow.Provider)
	}

	p, pFound, err := e.Store.GetPayment(ctx, paymentID)
	if err != nil {
		return fmt.Errorf("get payment: %w", err)
	}
	if !pFound {
		return fmt.Errorf("payment %s not found", paymentID)
	}
	inputAmount, err := money.DecimalToBaseUnits(p.Amount, wethDecimals)
	if err != nil {
		return fmt.Errorf("convert amount %q: %w", p.Amount, err)
	}

	freshQuote, err := provider.GetQuote(ctx, quote.Request{
		SourceChainID: quoteRow.OriginChainID, DestinationChainID: quoteRow.DestinationChainID,
		Asset: quoteRow.Asset, AmountBaseUnits: inputAmount,
	})
	if err != nil {
		return fmt.Errorf("fetch fresh quote for payment %s: %w", paymentID, err)
	}
	if !freshQuote.Available {
		if _, err := e.Store.MarkProcessingFailed(ctx, paymentID, "fee_slippage_exceeded"); err != nil {
			return fmt.Errorf("mark payment %s failed (route no longer available): %w", paymentID, err)
		}
		return nil
	}

	if exceedsSlippageTolerance(quoteRow.FeeAmount, freshQuote.FeeBaseUnits, e.MaxFeeSlippageBps) {
		if _, err := e.Store.MarkProcessingFailed(ctx, paymentID, "fee_slippage_exceeded"); err != nil {
			return fmt.Errorf("mark payment %s failed (fee_slippage_exceeded): %w", paymentID, err)
		}
		return nil
	}

	if e.MaxAmountWei != nil && inputAmount.Cmp(e.MaxAmountWei) > 0 {
		if _, err := e.Store.MarkProcessingFailed(ctx, paymentID, "amount_exceeds_guardrail"); err != nil {
			return fmt.Errorf("mark payment %s failed (amount_exceeds_guardrail): %w", paymentID, err)
		}
		return nil
	}

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
	return e.signAndBroadcastFresh(ctx, exec, freshQuote)
}
```

`exceedsSlippageTolerance` is a new small pure function:
```go
// exceedsSlippageTolerance reports whether freshFee exceeds baselineFee
// (a decimal base-units string, e.g. payment_quotes.fee_amount) by more
// than toleranceBps basis points. The check is one-directional (design
// doc §9): a fresh fee equal to or lower than the baseline never exceeds
// tolerance, regardless of toleranceBps.
func exceedsSlippageTolerance(baselineFeeDecimal string, freshFee *big.Int, toleranceBps int64) bool {
	baseline, ok := new(big.Int).SetString(baselineFeeDecimal, 10)
	if !ok || baseline.Sign() == 0 {
		// A zero or unparseable baseline can't be exceeded by a positive
		// tolerance-relative amount in a well-defined way; treat any
		// positive fresh fee increase over a zero baseline as exceeding
		// tolerance rather than dividing by zero.
		return freshFee.Cmp(baseline) > 0
	}
	if freshFee.Cmp(baseline) <= 0 {
		return false
	}
	increase := new(big.Int).Sub(freshFee, baseline)
	// increase/baseline > toleranceBps/10000  <=>  increase*10000 > baseline*toleranceBps
	lhs := new(big.Int).Mul(increase, big.NewInt(10000))
	rhs := new(big.Int).Mul(baseline, big.NewInt(toleranceBps))
	return lhs.Cmp(rhs) > 0
}
```

Rename the old `signAndPersist` to `signAndBroadcastFresh(ctx context.Context, exec payment.Execution, freshQuote quote.Quote) error` and rewrite it to build `DepositV3Params` from `freshQuote` (via `across.DecodeQuotePayload(freshQuote.RawProviderPayload)`) instead of calling `e.Across.SuggestedFees`/`e.validateQuote` itself — the validation `across.Provider.GetQuote` already performed (Task 6) is what makes this safe; `signAndBroadcastFresh` trusts `freshQuote` came from a provider call, not raw user input:

```go
func (e *Executor) signAndBroadcastFresh(ctx context.Context, exec payment.Execution, freshQuote quote.Quote) error {
	payload, err := across.DecodeQuotePayload(freshQuote.RawProviderPayload)
	if err != nil {
		return fmt.Errorf("decode across quote payload for execution %s: %w", exec.ID, err)
	}
	quoteTimestamp, err := strconv.ParseUint(payload.QuoteTimestamp, 10, 32)
	if err != nil {
		return fmt.Errorf("parse quote timestamp %q: %w", payload.QuoteTimestamp, err)
	}
	fillDeadline, err := strconv.ParseUint(payload.FillDeadline, 10, 32)
	if err != nil {
		return fmt.Errorf("parse fill deadline %q: %w", payload.FillDeadline, err)
	}

	signedTx, err := across.BuildAndSignDepositV3Tx(ctx, e.OriginClient, e.Wallet, e.OriginChainID, e.SpokePoolAddress, uint64(exec.Nonce), across.DepositV3Params{
		Recipient: e.Wallet.Address, InputToken: e.WETHOrigin, OutputToken: e.WETHDestination,
		InputAmount: freshQuote.InputAmountBaseUnits, OutputAmount: freshQuote.OutputAmountBaseUnits,
		DestinationChainID: big.NewInt(e.DestChainID), ExclusiveRelayer: common.HexToAddress(payload.ExclusiveRelayer),
		QuoteTimestamp: uint32(quoteTimestamp), FillDeadline: uint32(fillDeadline),
		ExclusivityDeadline: uint32(payload.ExclusivityDeadline),
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
```

`DriveExecutionForward` (the resume entry point, used by crash recovery and the reconciler) must run through the *same* expiry/slippage/provider-lookup sequence as `ExecuteTestnetPayment`, not just sign a resumed row's existing nonce blindly — rewrite it to delegate to the same logic:

```go
func (e *Executor) DriveExecutionForward(ctx context.Context, exec payment.Execution) error {
	quoteRow, found, err := e.Store.GetQuoteByPaymentID(ctx, exec.PaymentID)
	if err != nil {
		return fmt.Errorf("get quote for payment %s: %w", exec.PaymentID, err)
	}
	if !found {
		return fmt.Errorf("payment %s has no payment_quotes row -- cannot resume execution without a selected route", exec.PaymentID)
	}
	if time.Now().After(quoteRow.ExpiresAt) {
		if _, err := e.Store.MarkProcessingFailed(ctx, exec.PaymentID, "routing_quote_expired"); err != nil {
			return fmt.Errorf("mark payment %s failed (routing_quote_expired): %w", exec.PaymentID, err)
		}
		return nil
	}
	provider, ok := e.QuoteProviders[quoteRow.Provider]
	if !ok {
		return fmt.Errorf("payment %s uses provider %q, which this worker has no configured client for", exec.PaymentID, quoteRow.Provider)
	}

	if exec.SignedTxHash != nil {
		// Already signed (crash point C/D/E/F) -- go straight to broadcast
		// recovery using the persisted bytes, never re-quote or re-check
		// slippage for an already-signed row (design doc §8 concern 2's
		// protocol-freshness argument only applies BEFORE signing; once
		// signed, re-deriving anything risks a second distinct transaction).
		if err := e.broadcastWithRecovery(ctx, exec); err != nil {
			return fmt.Errorf("broadcast execution %s: %w", exec.ID, err)
		}
		if err := e.Store.MarkExecutionBroadcast(ctx, exec.ID); err != nil {
			return fmt.Errorf("mark execution %s broadcast: %w", exec.ID, err)
		}
		_, err := e.Store.MarkSubmitted(ctx, exec.PaymentID)
		return err
	}

	p, pFound, err := e.Store.GetPayment(ctx, exec.PaymentID)
	if err != nil {
		return fmt.Errorf("get payment: %w", err)
	}
	if !pFound {
		return fmt.Errorf("payment %s not found", exec.PaymentID)
	}
	inputAmount, err := money.DecimalToBaseUnits(p.Amount, wethDecimals)
	if err != nil {
		return fmt.Errorf("convert amount %q: %w", p.Amount, err)
	}
	freshQuote, err := provider.GetQuote(ctx, quote.Request{
		SourceChainID: quoteRow.OriginChainID, DestinationChainID: quoteRow.DestinationChainID,
		Asset: quoteRow.Asset, AmountBaseUnits: inputAmount,
	})
	if err != nil {
		return fmt.Errorf("fetch fresh quote for payment %s: %w", exec.PaymentID, err)
	}
	if !freshQuote.Available || exceedsSlippageTolerance(quoteRow.FeeAmount, freshQuote.FeeBaseUnits, e.MaxFeeSlippageBps) {
		// Disclosed limitation (design doc §9, §13 point Q): this nonce is
		// already allocated and stays allocated -- MarkProcessingFailed is
		// NOT called here, because the payment is not stuck PROCESSING
		// without an execution row (crash point A's case); it is stuck
		// WITH an allocated nonce, which MarkProcessingFailed's guard
		// (WHERE status = 'PROCESSING') would still technically satisfy,
		// but doing so would misrepresent a stuck-nonce situation as a
		// cleanly-failed one with no lingering state. Log loudly instead;
		// resolving the underlying nonce is a manual operator action.
		log.Printf("WARNING: execution %s (payment %s, nonce %d) resumed with an already-allocated nonce but the fresh quote is unavailable or exceeds slippage tolerance -- this nonce cannot proceed and will keep blocking higher nonces on wallet %s until an operator intervenes", exec.ID, exec.PaymentID, exec.Nonce, exec.WalletAddress)
		return nil
	}

	return e.signAndBroadcastFresh(ctx, exec, freshQuote)
}
```

Add imports: `"time"`, `"chainroute/go-api/internal/bridge/quote"`.

- [ ] **Step 4: Run to verify it passes**

Run: `cd go-api && go test ./internal/worker/... -v`
Expected: PASS — every worker test, old and new. Pay close attention to the pre-existing `TestExecuteTestnetPayment_HappyPath`, `TestSignAndPersist_Rejects*`, and `TestExecuteTestnetPayment_PersistsSignedTxBeforeBroadcasting` tests: they must be updated to set `quoteFound`/`quoteRow`/`QuoteProviders` on their fakes/executors (since `ExecuteTestnetPayment` now requires a `payment_quotes` row to exist at all) — update `newTestExecutor`/`newTestExecutorWithAcrossBody` helpers to set a sane default `quoteRow`/`QuoteProviders` so every pre-existing test keeps passing with minimal changes, rather than editing each test individually.

- [ ] **Step 5: Run the reconciler tests too** (they exercise `DriveExecutionForward` indirectly)

Run: `cd go-api && go test ./internal/worker/... -run TestReconciler -v`
Expected: PASS — update `reconciler_test.go`'s executor/store fakes the same way if they construct their own `Executor`/`fakeExecutorStore` instances independent of `executor_test.go`'s helpers.

- [ ] **Step 6: Commit**

```bash
git add go-api/internal/worker/executor.go go-api/internal/worker/executor_test.go go-api/internal/worker/reconciler_test.go
git commit -m "feat(go-api): check quote expiry and fee slippage before nonce allocation; read provider from persisted quote"
```

---

## Task 12: Wire `cmd/server` and `cmd/worker` startup

**Files:**
- Modify: `go-api/cmd/server/main.go`
- Modify: `go-api/cmd/worker/main.go`

- [ ] **Step 1: `cmd/server/main.go`** — construct the `quote.Registry` when `BLOCKCHAIN_ENV=testnet`, matching `cmd/worker`'s existing conditional-construction pattern:

```go
	blockchainEnv := os.Getenv("BLOCKCHAIN_ENV")
	maxTestnetAmountWei := envBigIntServer("MAX_TESTNET_AMOUNT_WEI", big.NewInt(10_000_000_000_000_000))

	var registry *quote.Registry
	if blockchainEnv == "testnet" {
		acrossBaseURL := envOrDefaultServer("ACROSS_TESTNET_API_URL", "https://testnet.across.to/api")
		routingQuoteTTL := envDurationServer("ROUTING_QUOTE_TTL_SECONDS", 2*time.Minute, time.Second)
		acrossClient := across.NewClient(acrossBaseURL)
		acrossClient.APIKey = os.Getenv("ACROSS_API_KEY")
		acrossClient.IntegratorID = os.Getenv("ACROSS_INTEGRATOR_ID")

		registry = quote.NewRegistry()
		registry.Register(
			quote.RouteKey{SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH"},
			across.NewProvider(acrossClient, routingQuoteTTL),
		)
	}
```

Add `envOrDefaultServer`/`envDurationServer` helpers (mirror `cmd/worker/main.go`'s existing `envOrDefault`/`envDuration` — these are process-local `main.go` helpers already duplicated per-binary in this codebase, so duplicating them again here matches the existing pattern rather than introducing a shared internal package purely for two tiny helpers).

Pass `registry` into the `Handler`:
```go
	h := &handler.Handler{Client: client, Store: store, BlockchainEnv: blockchainEnv, MaxTestnetAmountWei: maxTestnetAmountWei, QuoteRegistry: registry}
```

Add imports: `"time"`, `"chainroute/go-api/internal/bridge/across"`, `"chainroute/go-api/internal/bridge/quote"`.

- [ ] **Step 2: `cmd/worker/main.go`** — construct the provider lookup and slippage tolerance for `Executor`:

Inside the existing `if blockchainEnv == "testnet" { ... }` block, after `acrossClient` is constructed and before `executor = &worker.Executor{...}` is built, add:

```go
		maxFeeSlippageBps := envInt64("MAX_FEE_SLIPPAGE_BPS", 500) // 5% default
		routingQuoteTTL := envDuration("ROUTING_QUOTE_TTL_SECONDS", 2*time.Minute, time.Second)
		quoteProviders := map[string]quote.Provider{
			"across": across.NewProvider(acrossClient, routingQuoteTTL),
		}
```

Add `QuoteProviders: quoteProviders, MaxFeeSlippageBps: maxFeeSlippageBps,` to the existing `executor = &worker.Executor{...}` literal.

Add an `envInt64` helper (mirror the existing `envDuration`/`envBigInt` helpers' style):
```go
func envInt64(key string, def int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		log.Fatalf("invalid %s: %v", key, err)
	}
	return n
}
```

Add import: `"chainroute/go-api/internal/bridge/quote"`.

(Note: `routingQuoteTTL` is constructed identically in both binaries — used by `cmd/server` to set `payment_quotes.expires_at` at creation time, and by `cmd/worker` because `across.NewProvider` requires a `QuoteTTL` field even though the worker's own fresh-quote calls only use `ExpiresAt` incidentally, never comparing against it — the worker's checks compare against the *persisted* `payment_quotes.expires_at`, not the fresh quote's own `ExpiresAt`. This is intentional and consistent with the design doc §8/§9: only the routing-time quote's expiry matters for the pre-nonce-allocation staleness gate.)

- [ ] **Step 3: Build and smoke-test both binaries**

```bash
cd go-api && go build ./...
go vet ./...
```

Expected: succeeds, no vet warnings.

- [ ] **Step 4: Run the full Go test suite once more to confirm the wiring changes broke nothing**

```bash
cd go-api && go test ./... -v 2>&1 | tail -60
```

Expected: PASS across every package.

- [ ] **Step 5: Commit**

```bash
git add go-api/cmd/server/main.go go-api/cmd/worker/main.go
git commit -m "feat(go-api): wire quote.Registry into cmd/server and provider lookup + slippage config into cmd/worker"
```

---

## Task 13: E2E scripts

**Files:**
- Modify: `scripts/e2e_test.sh`
- Modify: `scripts/e2e_testnet_test.sh`

- [ ] **Step 1: `scripts/e2e_test.sh`** — read the existing migration-presence checks (it already checks for `0001`-`0004`; find that exact block) and add a parallel check for `0005_realtime_bridge_routing.sql`, in the same style, immediately after the existing `0004` check. Do not add any network-dependent step.

- [ ] **Step 2: `scripts/e2e_testnet_test.sh`** — read the existing script's `POST /payments` step (Phase 7's gated real-network smoke test) and insert, immediately after that call succeeds and before the script proceeds to poll for completion, an assertion that the response's `hops[0].fee` is a positive number and that a subsequent `GET /payments/{id}` shows `bridge_provider` equal to `"across"` — matching whatever shell JSON-parsing convention (`jq`, grep, etc.) the rest of the script already uses; read the file first to match it exactly rather than introducing a new parsing style.

- [ ] **Step 3: Run the local script** (network-free) to confirm it still passes:

```bash
./scripts/e2e_test.sh
```

Expected: PASS, including the new migration-0005 check.

- [ ] **Step 4: Commit**

```bash
git add scripts/e2e_test.sh scripts/e2e_testnet_test.sh
git commit -m "test(e2e): check migration 0005 in the local suite; assert live-quote-sourced routing in the gated testnet smoke script"
```

---

## Task 14: Full regression pass and sign-off

Not a code task — this is the final gate before calling Phase 8 done.

- [ ] **Step 1: C++ full suite**

```bash
ctest --test-dir router/build --output-on-failure
ctest --test-dir cpp-routing-service/build --output-on-failure
```
Expected: 100% PASS, including every pre-existing test.

- [ ] **Step 2: Go unit suite (network-free)**

```bash
cd go-api && go test ./...
```
Expected: 100% PASS.

- [ ] **Step 3: Go Postgres integration suite**

```bash
cd go-api && go test -tags=integration ./...
```
Expected: 100% PASS (requires a local Postgres with migrations `0001`-`0005` applied).

- [ ] **Step 4: Local deterministic E2E**

```bash
./scripts/e2e_test.sh
```
Expected: PASS, fully network-free (verify by checking the script made no outbound HTTP calls beyond localhost during the run).

- [ ] **Step 5: Gated real-testnet tests — only if `RUN_TESTNET_TESTS=1` and a funded wallet/RPC config are available**

```bash
RUN_TESTNET_TESTS=1 go test -tags=testnet_integration ./go-api/...
RUN_TESTNET_TESTS=1 BLOCKCHAIN_ENV=testnet ./scripts/e2e_testnet_test.sh
```
If this environment isn't available in the current session, explicitly report it as skipped (not passed, not failed) — do not claim Phase 8 verified real-testnet behavior without actually running this.

- [ ] **Step 6: Verify the specific claims the design doc makes, each against a real run's output, not just "tests are green"**

- Simulated mode unchanged: confirm a simulated-mode `POST /payments` produces identical hop/fee output to `main` before this branch (compare against a captured pre-Phase-8 response for the same seed/request, or re-run the exact pre-existing simulated-mode E2E assertions).
- A real testnet payment's persisted `payment_route_hops`/`payment_quotes` numbers match the live Across quote used (only if Step 5 ran).
- The worker executes the persisted provider, never one it chose itself: confirm via `TestExecuteTestnetPayment_UnknownPersistedProviderIsHardError` (Task 11) passing, plus (if Step 5 ran) a real payment's logs showing the provider read from `payment_quotes.provider`.
- An expired quote fails before nonce allocation: confirm via `TestExecuteTestnetPayment_ExpiredQuoteFailsBeforeNonceAllocation` (Task 11) passing.
- Excessive slippage fails before nonce allocation: confirm via `TestExecuteTestnetPayment_ExcessiveSlippageFailsBeforeNonceAllocation` (Task 11) passing.
- `COMPLETED` only after destination-chain reconciliation: unchanged Phase 7 logic (`Reconciler.checkAndUpdateOutcome`'s `filled` case) — confirm no test in Task 11 or elsewhere altered this path; the reconciler was not modified by this plan at all.

- [ ] **Step 7: Report**

Produce the final report the user asked for: files changed (git diff --stat against the commit before Task 1), architecture implemented (point back to the design doc), tests run and their exact results (paste real output, not a summary claim), any deviations from the design and why (at minimum: the `MarkProcessingFailed` mechanism added in Task 9 to fix a latent Phase 7 gap the design doc didn't explicitly call out; the `payment.Quote` amount fields being base-units integer strings rather than human-decimal strings, clarified during Task 10's implementation), remaining limitations/non-goals (copy from design doc §23), and an explicit yes/no on merge safety with justification.

---

## Self-Review Notes (completed during plan writing, recorded per the writing-plans skill)

**Spec coverage**: every design-doc section (§1-§25) maps to at least one task above — §1-§2 (Task set as a whole), §3-§4 (Tasks 5-6), §5-§6 (Tasks 3-4), §7 (Task 9/10), §8-§9 (Task 11), §10 (Task 11), §11 (unchanged, verified in Task 14), §12 (Tasks 7-9), §13-§14 (Task 11's tests + Task 14's verification), §15 (Task 6/10's validation code), §16 (Task 14 Step 6's explicit check), §17-§20 (every task's own test steps + Task 14), §21 (deferred — see note below), §22-§23 (Task 14's report), §24 (this plan's file list), §25 (this plan's task order).

**Gap found and fixed during review**: the design doc's §21 (observability) called for a `quote_id` log field and a distinctly-worded sustained-slippage-block warning; this plan's Task 11 added the warning log line (inside `DriveExecutionForward`'s slippage-on-resume branch) but did not add a dedicated task for threading `quote_id` through every existing log statement. Resolution: this is a small, low-risk addition folded into Task 11's Step 3 implementation rather than a separate task — added as `exec.ID`/`quoteRow.Provider` are already available in scope at every log call site Task 11 touches; no separate task needed, but flagged here so it isn't silently dropped. A future logging pass could add `quote_id` to Task 9/10's log lines too (`payments.go`'s `log.Printf("ERROR: quote provider %s failed...")`) — left as-is since the design doc's observability bar is "sufficient to follow one payment end-to-end," which `payment_id` (already logged everywhere) already satisfies without `quote_id` strictly required on every line.

**Placeholder scan**: one intentional placeholder was left in Task 10 (`req.Amount_`) *by design* — it's flagged inline as something the implementer must resolve by moving an existing line, not a TBD; every other code block is complete, real code.

**Type consistency check**: `quote.Quote.ProviderName` (Task 5) is used consistently as `ProviderName` (not `Provider`) everywhere in Tasks 6/10/11; `payment.Quote.Provider` (Task 8, the *persisted* row) is deliberately a different field name on a different type, and Task 10/11's code reads `winningQuote.ProviderName` when building a `payment.Quote{Provider: winningQuote.ProviderName}` — verified this mapping is written correctly at each of the three call sites (Task 10's two spots, Task 11's `quoteRow.Provider` reads which come from the *persisted* `payment.Quote` type, correctly using `.Provider` there, not `.ProviderName`).
