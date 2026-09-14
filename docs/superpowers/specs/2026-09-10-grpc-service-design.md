# ChainRoute Phase 4: gRPC Routing Service + Go API Design

**Status:** Approved
**Date:** 2026-09-10

## Context

Phases 1-3 (merged) built a complete, in-process C++ routing engine: `Graph`
(directed index-based multigraph), `Node`/`Edge` (chain+asset pairs and
bridges with fee/latency/liquidity/reliability), `findCheapestRoute`
(fee-only Dijkstra, liquidity as a hard constraint, exact bridge
reconstruction), and `NetworkSimulator` (deterministic, seeded, tick-based
simulated network conditions producing `Graph` snapshots). 49 tests pass.
Everything so far runs as a single C++ binary/test suite with no network
surface.

**Phase 4 scope is exposing that engine as a network service**: a new C++
gRPC server (`cpp-routing-service/`) that owns a `NetworkSimulator` and
answers routing queries, and a new, initially-thin Go HTTP API
(`go-api/`) that validates external JSON requests and forwards them over
gRPC. Architecture:

```
Client -> HTTP/JSON -> Go API service -> gRPC -> C++ routing service
                                                    -> NetworkSimulator.snapshot()
                                                    -> findCheapestRoute()
```

## Goals

- A shared protobuf contract (`proto/chainroute/v1/routing.proto`) defining
  exactly one RPC, `RoutingService.FindRoute`, sufficient for the example
  request/response in the brief.
- The C++ service owns a single `NetworkSimulator` instance and the
  existing Phase 1-3 routing logic, unmodified.
- The Go service is thin: HTTP endpoint, request validation/parsing, gRPC
  call, JSON translation — no routing logic of its own.
- `NodeIndex` never crosses the service boundary in any form.
- Explicit, tested conversion between protobuf `Chain`/`Asset` enums and
  the existing `ChainId`/`AssetId` types.
- A complete, well-defined mapping of every listed edge case (invalid
  chain/asset, non-positive amount, same-chain request, no route, internal
  failure) through gRPC status codes to HTTP status codes and JSON bodies.
- A documented, scriptable way to run the full Go → gRPC → C++ stack
  locally for end-to-end verification.

## Non-goals

- Modifying Phase 1-3 routing behavior, `Graph`/`Node`/`Edge` APIs, or
  `NetworkSimulator`'s public surface, merely to make RPC easier.
- Kafka, PostgreSQL, Redis, Kubernetes, or any cloud deployment tooling.
- Connecting to real blockchain APIs.
- Multi-criteria routing (latency/reliability stay data-only, never part
  of the cost function — unchanged from Phase 2).
- Payment execution or any state machine beyond answering a route query.
- Retry logic (see "Retries" below).
- A background/auto-advancing tick model, or any client-facing control
  over simulated time (see "Simulated time" below).
- Any abstraction (interfaces, factories, pluggable transports) not
  justified by a concrete need stated in this document.

## Repository structure

```
chainroute/
  router/                          # Phase 1-3 C++ library (unchanged)
  proto/
    chainroute/v1/routing.proto    # shared gRPC contract, single source of truth
  cpp-routing-service/             # new: C++ gRPC server
    CMakeLists.txt                 # depends on router/ (add_subdirectory) + system gRPC/protobuf
    include/chainroute_service/
      chain_asset_convert.hpp
      routing_service.hpp
    src/
      main.cpp
      chain_asset_convert.cpp
      routing_service.cpp
    tests/
      chain_asset_convert_test.cpp
      routing_service_test.cpp
  go-api/                          # new: Go HTTP service
    go.mod
    cmd/server/main.go
    internal/
      handler/
        routes.go
        routes_test.go
      grpcclient/
        client.go
        client_test.go
      gen/                         # generated Go protobuf/grpc stubs (committed)
  scripts/
    e2e_test.sh                    # local Go -> gRPC -> C++ smoke test
```

`cpp-routing-service` is a **separate** CMake project from `router/`
(pulled in via `add_subdirectory` with a relative path), not nested inside
it: `router/` stays a lean, dependency-free library exactly as it is
today; the gRPC service is a distinct architectural layer that pulls in
gRPC/protobuf without adding that weight to the core library's build.

**gRPC/protobuf build tooling:** system-installed, located via CMake's
`find_package(Protobuf)` / `find_package(gRPC)` — not `FetchContent`.
Building gRPC's full C++ stack from source is commonly a 10-20+ minute
clean build (it pulls in protobuf, abseil, c-ares, re2, zlib, and a TLS
library as its own dependencies); relying on a pre-installed gRPC/protobuf
(e.g. via Homebrew/apt/vcpkg) matches how most real-world C++ gRPC
services are actually built, at the cost of one documented environment
setup step, unlike Phase 1-3's zero-setup `FetchContent` pattern for
GoogleTest.

**Generated code:** C++ protobuf/gRPC stubs are generated at CMake build
time (not committed) — matches the system-installed-protoc assumption. Go
stubs are generated via a documented command (`go generate` or a Makefile
target) and **committed** to `go-api/internal/gen/` — standard Go
practice, since `go build` has no CMake-like configure-time codegen step,
and committing avoids requiring `protoc` on every Go developer's machine
just to build.

## Protobuf/gRPC API

One package, one service, one RPC:

```protobuf
syntax = "proto3";
package chainroute.v1;

enum Chain {
  CHAIN_UNSPECIFIED = 0;   // proto3 convention: 0 is never a valid real value
  CHAIN_ETHEREUM = 1;
  CHAIN_BASE = 2;
  CHAIN_ARBITRUM = 3;
  CHAIN_OPTIMISM = 4;
  CHAIN_POLYGON = 5;
}

enum Asset {
  ASSET_UNSPECIFIED = 0;
  ASSET_USDC = 1;
  ASSET_ETH = 2;
}

message FindRouteRequest {
  Chain source_chain = 1;
  Chain destination_chain = 2;
  Asset asset = 3;
  double amount = 4;
}

message RouteHop {
  Chain from_chain = 1;
  Chain to_chain = 2;
  string bridge_name = 3;
  double fee = 4;
  double latency_ms = 5;
  double liquidity = 6;
  double reliability = 7;
}

message FindRouteResponse {
  bool route_found = 1;
  repeated RouteHop hops = 2;
  double total_fee = 3;
}

service RoutingService {
  rpc FindRoute(FindRouteRequest) returns (FindRouteResponse);
}
```

"No route" is a normal, successful response (`OK` + `route_found = false`
+ empty `hops`), never a gRPC error status — it's a legitimate business
outcome (per Phase 2's `std::nullopt` design), not a failure.

## Protobuf contract vs. internal C++ types

**In the contract:** `Chain`, `Asset`, `FindRouteRequest`, `RouteHop`,
`FindRouteResponse` — exactly what's needed to request and
display/execute a route.

**Never in the contract:**
- `NodeIndex` (see below).
- The internal `Node`/`Edge`/`Route` structs — mapped field-by-field into
  `RouteHop`/`FindRouteResponse`, never re-exposed as-is.
- `NetworkSimulator`'s internal `BridgeTemplate`/topology state.
- Seed and tick — no client-facing way to query or control simulated time
  in this phase (see "Simulated time").

## Why NodeIndex must not cross the boundary

`NodeIndex` is a `std::size_t` array position, meaningful only relative to
one specific `Graph` instance. It is stable today only because
`NetworkSimulator::snapshot()` happens to generate the same fixed node
universe in the same fixed order on every call — an internal invariant
Phase 3 documents as *current, intentional behavior*, not a promised
external contract. Exposing it on the wire would couple the protocol to
that implementation detail: a future topology change (a 6th chain,
reordered generation) would silently redefine what an index means,
breaking any client that had cached one.

The wire contract identifies nodes only by the stable, meaningful
identity the domain already has: `(Chain, Asset)`. The C++ service
resolves `(chain, asset) → NodeIndex` via `Graph::findNode()` purely
internally to call `findCheapestRoute`, then converts the result's edges
back to `(chain, asset)` pairs via `Graph::nodeAt()` when building the
response. `NodeIndex` is created, used, and discarded entirely within one
request-handling function — it is never serialized, logged in a
client-visible way, or returned.

## Chain/Asset conversion

```cpp
// cpp-routing-service/include/chainroute_service/chain_asset_convert.hpp
namespace chainroute_service {

std::optional<chainroute::ChainId> toChainId(chainroute::v1::Chain proto);
std::optional<chainroute::AssetId> toAssetId(chainroute::v1::Asset proto);

chainroute::v1::Chain toProtoChain(chainroute::ChainId chain);  // total
chainroute::v1::Asset toProtoAsset(chainroute::AssetId asset);  // total

}  // namespace chainroute_service
```

`toChainId`/`toAssetId` return `std::optional` — an out-of-range or
`UNSPECIFIED` value arriving over the wire is expected, validate-able
input, not a programmer error, so it is never thrown. The reverse
direction (`toProtoChain`/`toProtoAsset`) is total: every `ChainId`/
`AssetId` value the simulator's fixed universe ever produces has a proto
counterpart by construction. Both are implemented as exhaustive `switch`
statements (no `default:` case) so `-Wswitch` forces a compile error here
if `ChainId`/`AssetId` ever grows without this mapping being updated —
the same defensive pattern Phase 3 already uses for `assetIndexOf`.

Go has the mirror-image mapping, from lowercase JSON strings (e.g.
`"ethereum"`, `"usdc"`) to the proto enum values, with an explicit
"unrecognized" branch that becomes a 400 response. Go never constructs or
sends `CHAIN_UNSPECIFIED`/`ASSET_UNSPECIFIED` on a request that has passed
its own validation.

## NetworkSimulator ownership

The C++ service constructs exactly one `chainroute::sim::NetworkSimulator`
at process startup, seeded via a `--seed` command-line flag (with a
documented default), owned for the service's entire process lifetime
(e.g. as a member of the `RoutingService` implementation object).

## Simulated time (ticks)

`tick()` is **never called automatically** in this phase — the simulator
stays at whichever tick it was constructed with (tick 0) for the whole
process lifetime; every request sees the same snapshot. This is a
deliberate, explicitly-scoped decision, not a gap: nothing in Phase 4's
stated requirements needs conditions to evolve live, and a background
ticker would require a mutex around `snapshot()`/`tick()` access for no
concrete benefit yet. One pleasant consequence: since `snapshot()` is
`const` and reads only state that is never mutated after construction in
this phase, it is safe to call concurrently from every simultaneous gRPC
request with zero additional synchronization. A later phase can add a
`--tick-interval` flag or an admin RPC to advance time; nothing here
forecloses that.

## Go HTTP API

`POST /routes`, via stdlib `net/http` using Go 1.22+'s `ServeMux` pattern
routing (`mux.HandleFunc("POST /routes", handler)`) — no external router
library, since there is exactly one endpoint.

Request:

```json
{
  "source_chain": "ethereum",
  "destination_chain": "base",
  "asset": "USDC",
  "amount": 1000
}
```

Validation, in order, each failing fast with HTTP 400 before any gRPC
call is made:

1. JSON decodes successfully.
2. `source_chain` is a recognized chain string.
3. `destination_chain` is a recognized chain string.
4. `asset` is a recognized asset string.
5. `amount > 0`.
6. `source_chain != destination_chain` (rejected at this layer).

**Why same-chain is rejected, precisely:** this is a consequence of the
current Phase 4 request shape, not a permanent domain rule. The contract
is `(source_chain, destination_chain, asset, amount)` — a single `asset`
field shared by both ends, meaning source and destination assets are
always identical under this API. When `source_chain == destination_chain`
too, source and destination resolve to the *same* `Node{chain, asset}` —
`findCheapestRoute` would return its algorithmically-valid trivial
zero-hop route (per Phase 2), which is real but never a meaningful routing
request: nothing needs to move. Rejecting it here is specific to *this*
request shape having no independent `source_asset`/`destination_asset`.
A future API revision with distinct source/destination assets could
legitimately support `source_chain == destination_chain` (an on-chain
swap) as a real request; that is explicitly not implemented in Phase 4 —
this section documents why today's rule exists, not a claim that it is
permanent.

Only once all six pass does Go build a `FindRouteRequest` and call the
gRPC client with a bounded deadline (see "Deadlines").

Success response:

```json
{
  "route_found": true,
  "hops": [
    {
      "from_chain": "ethereum",
      "to_chain": "arbitrum",
      "bridge_name": "Hop#1",
      "fee": 1.0,
      "latency_ms": 500.0,
      "liquidity": 1000000.0,
      "reliability": 0.99
    }
  ],
  "total_fee": 1.0
}
```

`route_found: false` (with `hops: []`, `total_fee: 0`) is a 200, not an
error — see the error table below.

## Error propagation

| Case | Caught by | gRPC status | HTTP | Body |
|---|---|---|---|---|
| Invalid `source_chain`/`destination_chain` | Go, pre-RPC | — | 400 | `{"error": "invalid chain: ..."}` |
| Invalid `asset` | Go, pre-RPC | — | 400 | `{"error": "invalid asset: ..."}` |
| `amount <= 0` | Go, pre-RPC | — | 400 | `{"error": "amount must be positive"}` |
| `source_chain == destination_chain` | Go, pre-RPC | — | 400 | `{"error": "source and destination must differ"}` |
| (defense-in-depth) any of the above reaches C++ anyway | C++ | `INVALID_ARGUMENT` | 400 | `{"error": "..."}` — see note below |
| No route available | C++, normal outcome | `OK`, `route_found=false` | 200 | `{"route_found": false, "hops": [], "total_fee": 0}` |
| C++ service unreachable | transport | `UNAVAILABLE` | 503 | generic |
| Unexpected internal error (C++ exception, Go panic recovery, etc.) | C++ or Go | `INTERNAL` | 500 | generic, no internal details leaked |
| gRPC deadline exceeded | transport | `DEADLINE_EXCEEDED` | 504 | generic |

C++ validates defensively even though Go is the primary gate (never trust
a network peer blindly, even an internal one). The Go-side gRPC client maps
status codes uniformly: `INVALID_ARGUMENT → 400`, `UNAVAILABLE → 503`,
`DEADLINE_EXCEEDED → 504`, `INTERNAL → 500`. An `INVALID_ARGUMENT` from C++
is still externally a 400 — the request itself is, in fact, invalid — but
because Go's own validation should have already caught it, Go additionally
logs an internal warning/error noting that a request reached C++ despite
passing Go-side validation. This is not expected to happen in normal
operation; if it does, the log line is the signal that Go's and C++'s
validation rules have drifted apart and need reconciling. The HTTP-facing
semantics for the client stay correct either way — 400 means "your request
was invalid" regardless of which layer detected it.

## Deadlines

Go sets a context deadline (e.g. 2 seconds) before every gRPC call. This
is generous relative to the actual computation — fee-only Dijkstra over a
~10-node graph plus building a snapshot is a sub-millisecond operation per
Phase 1-3's own complexity analysis — so the budget covers connection and
serialization overhead, not algorithm time. No separate C++-side timeout
is needed: nothing in the request path performs unbounded work, blocking
I/O, or anything that could hang independent of the deadline the caller
already sets.

## Retries

**None, at this phase.** `FindRoute` is read-only and side-effect-free
(no state mutation, no payment execution), so retries would be *safe*
from a correctness standpoint — but there is no observed or plausible
failure mode yet that retries would meaningfully help with: this phase
runs two co-located processes with no load balancer, no multi-instance
deployment, and no modeled network partition. Retry logic (backoff,
retry budgets, classifying which gRPC codes are retryable) is real
complexity with no concrete Phase 4 need. If a request fails, the external
HTTP caller can already retry the whole request itself. This is a natural
candidate for a later phase once real multi-instance deployment makes
transient failures an actual, observed concern.

## Graceful startup/shutdown

**C++ service:** parse `--seed` and `--port` (or `--listen-address`)
flags, construct the `NetworkSimulator`, build and start the gRPC server.
On `SIGINT`/`SIGTERM`, call `grpc::Server::Shutdown()` — gRPC's built-in
graceful shutdown stops accepting new RPCs and lets in-flight ones
complete before the process exits.

**Go service:** parse `--http-addr` and `--grpc-addr` flags (or
equivalent env vars), dial the gRPC connection, start `http.Server` in a
goroutine. On signal, call `http.Server.Shutdown(ctx)` — stdlib's built-in
graceful drain (stop accepting new connections, let in-flight handlers
finish, bounded by the passed context) — before closing the gRPC client
connection and exiting.

Both sides use plain OS signal handling; no supervisor or orchestration
framework is introduced (Kubernetes is explicitly out of scope).

## Unit and integration testing strategy

**C++:** `router/`'s existing 49 tests are untouched. New:
- `chain_asset_convert_test.cpp` — exhaustive round-trip coverage for
  every `ChainId`/`AssetId` ↔ proto enum pair, plus invalid-value handling
  (`UNSPECIFIED`, out-of-range values).
- `routing_service_test.cpp` — tests the `RoutingService` implementation
  as a plain C++ object, in-process (gRPC's generated service base class
  is just a virtual-method interface; no real network socket is needed to
  test it). Covers: a successful multi-hop route, no-route-found,
  invalid chain/asset, `amount <= 0` (defense-in-depth, even though Go
  should already catch this).

**Go:** table-driven tests in `routes_test.go` using
`net/http/httptest`, covering every validation case (malformed JSON,
invalid chain, invalid asset, `amount <= 0`, same-chain) and asserting
exact response bodies/status codes — using a **fake** gRPC client (a
small Go interface matching the generated client's method signature) so
no real C++ process is required for unit tests. `client_test.go` covers
the thin gRPC-status-to-typed-error mapping.

## Local end-to-end testing (Go → gRPC → C++)

`scripts/e2e_test.sh`, a plain bash script (no Docker, no new CI
infrastructure beyond "run a script"):

1. Build both binaries (`cmake --build cpp-routing-service/build`,
   `go build ./...`).
2. Start the C++ service in the background on a fixed local port, with a
   **fixed `--seed`** for reproducible expected values.
3. Wait for readiness by polling the standard `grpc.health.v1.Health`
   service (a well-known, tiny, standard protobuf service designed
   exactly for this) rather than inventing a custom readiness signal.
4. Start the Go service, pointed at that gRPC address, on a fixed local
   HTTP port.
5. Run a handful of `curl` requests against `POST /routes`: a real
   multi-hop route (asserting the exact expected JSON, reproducible
   because the seed is fixed), an invalid-chain request (asserting 400),
   a same-chain request (asserting 400).
6. Tear down both processes (PID capture + `trap ... EXIT` + `kill`).

This script is both a manual dev tool and something CI can run directly;
it also serves as the final smoke test after implementation, independent
of the unit-test suites (which use fakes on both sides and never actually
exercise the wire format end-to-end).
