# ChainRoute Phase 4b: Go API Service + End-to-End Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the Go HTTP API (`go-api/`) per
`docs/superpowers/specs/2026-09-10-grpc-service-design.md`, and a real
local end-to-end test proving `HTTP → Go → gRPC → C++` works, completing
Phase 4.

**Prerequisite:** Plan A (`docs/superpowers/plans/2026-09-10-grpc-service-plan-a-cpp.md`)
must be complete — this plan generates Go stubs from the same
`proto/chainroute/v1/routing.proto` Plan A wrote, and its E2E test runs
Plan A's `chainroute_service_server` binary as a subprocess.

**Architecture:** A thin Go service: `internal/handler` validates and
translates HTTP JSON, `internal/grpcclient` wraps the generated gRPC
client with a deadline, `cmd/server/main.go` wires them together with
graceful shutdown. No routing logic in Go.

**Tech Stack:** Go 1.22+, stdlib `net/http`, `google.golang.org/grpc`,
`google.golang.org/protobuf`.

## Global Constraints

- Do NOT modify any file under `router/` or `cpp-routing-service/`
  (Plan A's output). This plan only adds files under `go-api/` and
  `scripts/`.
- The Go service is thin: `internal/handler` does validation and JSON
  translation only — no routing logic, no direct knowledge of `ChainId`/
  `AssetId`/`Graph`, only the proto types.
- Validation order in the HTTP handler, each failing fast with 400 before
  any gRPC call: JSON decodes → `source_chain` recognized →
  `destination_chain` recognized → `asset` recognized → `amount > 0` →
  `source_chain != destination_chain`.
- gRPC status → HTTP status mapping: `INVALID_ARGUMENT → 400`,
  `UNAVAILABLE → 503`, `DEADLINE_EXCEEDED → 504`, everything else
  (including `INTERNAL`) `→ 500`. An `INVALID_ARGUMENT` reaching Go after
  passing Go-side validation is additionally logged as a warning (it
  indicates validation drift between Go and C++), but the HTTP response
  stays 400.
- Go sets a bounded context deadline (2 seconds) on every gRPC call.
- No retries anywhere in the Go service.
- `RoutingClient` in `internal/handler` is a small interface (matching the
  generated client's `FindRoute` method signature) specifically so unit
  tests can inject a fake — this is the one interface introduced in this
  plan, justified by a concrete, stated testing need, not spec creep.
- Graceful shutdown: on `SIGINT`/`SIGTERM`, stop accepting new HTTP
  connections and let in-flight requests finish via `http.Server.Shutdown`,
  before closing the gRPC client connection.
- Generated Go protobuf/gRPC stubs are committed to
  `go-api/internal/gen/`.
- The E2E test uses the standard `grpc.health.v1.Health` service (already
  wired in Plan A) for readiness polling — no custom health check.
- No Docker, no Kubernetes, no retries, no Kafka/PostgreSQL/Redis.

---

### Task 1: Go module setup and generated protobuf/gRPC stubs

**Files:**
- Create: `go-api/go.mod`
- Create: `go-api/internal/gen/chainroute/v1/routing.pb.go` (generated)
- Create: `go-api/internal/gen/chainroute/v1/routing_grpc.pb.go` (generated)

**Interfaces:**
- Produces: a Go module that builds, with generated types
  `routingv1.Chain`, `routingv1.Asset`, `routingv1.FindRouteRequest`,
  `routingv1.RouteHop`, `routingv1.FindRouteResponse`,
  `routingv1.RoutingServiceClient`.

- [ ] **Step 1: Install the Go protoc plugins**

Run:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
export PATH="$PATH:$(go env GOPATH)/bin"
```

Confirm both are now on `PATH`:

```bash
which protoc-gen-go protoc-gen-go-grpc
```

- [ ] **Step 2: Initialize the Go module**

```bash
mkdir -p go-api
cd go-api
go mod init chainroute/go-api
cd ..
```

- [ ] **Step 3: Generate the Go stubs**

From the repository root:

```bash
mkdir -p go-api/internal/gen
protoc \
    --go_out=go-api/internal/gen --go_opt=paths=source_relative \
    --go-grpc_out=go-api/internal/gen --go-grpc_opt=paths=source_relative \
    -I proto \
    proto/chainroute/v1/routing.proto
```

Expected: `go-api/internal/gen/chainroute/v1/routing.pb.go` and
`routing_grpc.pb.go` are created (paths determined by the `go_package`
option in `routing.proto`, which Plan A set to
`"chainroute/go-api/internal/gen/chainroute/v1;routingv1"`).

**If this fails because `routing.proto` doesn't yet have a `go_package`
option:** Plan A's Task 1 Step 2 was written to include
`option go_package = "chainroute/go-api/internal/gen/chainroute/v1;routingv1";`
— if Plan A was implemented from an earlier version of that file without
it, add that line to `proto/chainroute/v1/routing.proto` now (this is the
one legitimate case where this plan touches a Plan A file: a missing proto
option, not routing logic) and re-run.

- [ ] **Step 4: Add gRPC/protobuf dependencies and verify the module builds**

```bash
cd go-api
go get google.golang.org/grpc
go get google.golang.org/protobuf
go build ./...
cd ..
```

Expected: builds successfully with no errors. This confirms the generated
stubs are well-formed and the module's dependencies resolve.

- [ ] **Step 5: Commit**

```bash
git add go-api/
git commit -m "feat(go-api): initialize Go module and generate protobuf/gRPC stubs"
```

---

### Task 2: gRPC client wrapper

**Files:**
- Create: `go-api/internal/grpcclient/client.go`
- Create: `go-api/internal/grpcclient/client_test.go`

**Interfaces:**
- Consumes: `routingv1.RoutingServiceClient` (Task 1's generated stubs).
- Produces:
```go
package grpcclient

type Client struct { /* ... */ }

func Dial(address string) (*Client, error)
func (c *Client) Close() error
func (c *Client) FindRoute(ctx context.Context, req *routingv1.FindRouteRequest) (*routingv1.FindRouteResponse, error)
```

- [ ] **Step 1: Write client.go**

`go-api/internal/grpcclient/client.go`:

```go
package grpcclient

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	routingv1 "chainroute/go-api/internal/gen/chainroute/v1"
)

const requestTimeout = 2 * time.Second

type Client struct {
	conn   *grpc.ClientConn
	client routingv1.RoutingServiceClient
}

func Dial(address string) (*Client, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, client: routingv1.NewRoutingServiceClient(conn)}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) FindRoute(ctx context.Context, req *routingv1.FindRouteRequest) (*routingv1.FindRouteResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	return c.client.FindRoute(ctx, req)
}
```

**If `grpc.NewClient` is not available** (it was added in a specific
grpc-go release; an older resolved version might not have it): use
`grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))`
instead — functionally equivalent for this plan's purposes, just an older
API name. Check `go doc google.golang.org/grpc` if uncertain which is
available in the resolved dependency version.

- [ ] **Step 2: Write a basic test**

`go-api/internal/grpcclient/client_test.go`:

```go
package grpcclient

import "testing"

func TestDialAndClose(t *testing.T) {
	client, err := Dial("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer client.Close()

	if client.client == nil {
		t.Fatal("expected a non-nil generated client")
	}
}
```

(`grpc.NewClient`/`grpc.Dial` with insecure credentials does not eagerly
connect, so `Dial` against an address with nothing listening should still
succeed here — connection attempts happen lazily on the first RPC.)

- [ ] **Step 3: Build and test**

```bash
cd go-api
go build ./...
go test ./internal/grpcclient/...
cd ..
```

Expected: builds and passes.

- [ ] **Step 4: Commit**

```bash
git add go-api/internal/grpcclient/
git commit -m "feat(go-api): add gRPC client wrapper with request deadline"
```

---

### Task 3: HTTP handler (validation, gRPC call, JSON translation)

**Files:**
- Create: `go-api/internal/handler/routes.go`
- Create: `go-api/internal/handler/routes_test.go`

**Interfaces:**
- Consumes: `routingv1.*` (Task 1), the `RoutingClient` interface (defined
  in this task, satisfied by `grpcclient.Client` from Task 2).
- Produces:
```go
package handler

type RoutingClient interface {
    FindRoute(ctx context.Context, req *routingv1.FindRouteRequest) (*routingv1.FindRouteResponse, error)
}

type Handler struct {
    Client RoutingClient
}

func (h *Handler) PostRoutes(w http.ResponseWriter, r *http.Request)
```

- [ ] **Step 1: Write routes.go**

`go-api/internal/handler/routes.go`:

```go
package handler

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	routingv1 "chainroute/go-api/internal/gen/chainroute/v1"
)

type RoutingClient interface {
	FindRoute(ctx context.Context, req *routingv1.FindRouteRequest) (*routingv1.FindRouteResponse, error)
}

type Handler struct {
	Client RoutingClient
}

type findRouteRequest struct {
	SourceChain      string  `json:"source_chain"`
	DestinationChain string  `json:"destination_chain"`
	Asset            string  `json:"asset"`
	Amount           float64 `json:"amount"`
}

type routeHop struct {
	FromChain   string  `json:"from_chain"`
	ToChain     string  `json:"to_chain"`
	BridgeName  string  `json:"bridge_name"`
	Fee         float64 `json:"fee"`
	LatencyMs   float64 `json:"latency_ms"`
	Liquidity   float64 `json:"liquidity"`
	Reliability float64 `json:"reliability"`
}

type findRouteResponse struct {
	RouteFound bool       `json:"route_found"`
	Hops       []routeHop `json:"hops"`
	TotalFee   float64    `json:"total_fee"`
}

type errorResponse struct {
	Error string `json:"error"`
}

var chainByName = map[string]routingv1.Chain{
	"ethereum": routingv1.Chain_CHAIN_ETHEREUM,
	"base":     routingv1.Chain_CHAIN_BASE,
	"arbitrum": routingv1.Chain_CHAIN_ARBITRUM,
	"optimism": routingv1.Chain_CHAIN_OPTIMISM,
	"polygon":  routingv1.Chain_CHAIN_POLYGON,
}

var chainNameByValue = map[routingv1.Chain]string{
	routingv1.Chain_CHAIN_ETHEREUM: "ethereum",
	routingv1.Chain_CHAIN_BASE:     "base",
	routingv1.Chain_CHAIN_ARBITRUM: "arbitrum",
	routingv1.Chain_CHAIN_OPTIMISM: "optimism",
	routingv1.Chain_CHAIN_POLYGON:  "polygon",
}

var assetByName = map[string]routingv1.Asset{
	"usdc": routingv1.Asset_ASSET_USDC,
	"eth":  routingv1.Asset_ASSET_ETH,
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}

func (h *Handler) PostRoutes(w http.ResponseWriter, r *http.Request) {
	var req findRouteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	sourceChain, ok := chainByName[strings.ToLower(req.SourceChain)]
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid chain: "+req.SourceChain)
		return
	}
	destChain, ok := chainByName[strings.ToLower(req.DestinationChain)]
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid chain: "+req.DestinationChain)
		return
	}
	asset, ok := assetByName[strings.ToLower(req.Asset)]
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid asset: "+req.Asset)
		return
	}
	if req.Amount <= 0 {
		writeError(w, http.StatusBadRequest, "amount must be positive")
		return
	}
	if sourceChain == destChain {
		writeError(w, http.StatusBadRequest, "source and destination must differ")
		return
	}

	grpcReq := &routingv1.FindRouteRequest{
		SourceChain:      sourceChain,
		DestinationChain: destChain,
		Asset:            asset,
		Amount:           req.Amount,
	}

	resp, err := h.Client.FindRoute(r.Context(), grpcReq)
	if err != nil {
		st, _ := status.FromError(err)
		switch st.Code() {
		case codes.InvalidArgument:
			log.Printf("WARNING: routing service rejected a request that passed Go validation (possible validation drift): %v", st.Message())
			writeError(w, http.StatusBadRequest, st.Message())
		case codes.Unavailable:
			writeError(w, http.StatusServiceUnavailable, "routing service unavailable")
		case codes.DeadlineExceeded:
			writeError(w, http.StatusGatewayTimeout, "routing service timed out")
		default:
			log.Printf("ERROR: routing service call failed: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}

	hops := make([]routeHop, 0, len(resp.GetHops()))
	for _, hop := range resp.GetHops() {
		hops = append(hops, routeHop{
			FromChain:   chainNameByValue[hop.GetFromChain()],
			ToChain:     chainNameByValue[hop.GetToChain()],
			BridgeName:  hop.GetBridgeName(),
			Fee:         hop.GetFee(),
			LatencyMs:   hop.GetLatencyMs(),
			Liquidity:   hop.GetLiquidity(),
			Reliability: hop.GetReliability(),
		})
	}

	writeJSON(w, http.StatusOK, findRouteResponse{
		RouteFound: resp.GetRouteFound(),
		Hops:       hops,
		TotalFee:   resp.GetTotalFee(),
	})
}
```

- [ ] **Step 2: Write the tests**

`go-api/internal/handler/routes_test.go`:

```go
package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	routingv1 "chainroute/go-api/internal/gen/chainroute/v1"
)

type fakeClient struct {
	response *routingv1.FindRouteResponse
	err      error
	lastReq  *routingv1.FindRouteRequest
}

func (f *fakeClient) FindRoute(_ context.Context, req *routingv1.FindRouteRequest) (*routingv1.FindRouteResponse, error) {
	f.lastReq = req
	return f.response, f.err
}

func doRequest(h *Handler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/routes", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	h.PostRoutes(rec, req)
	return rec
}

func TestPostRoutes_InvalidJSON(t *testing.T) {
	h := &Handler{Client: &fakeClient{}}
	rec := doRequest(h, "{not json")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestPostRoutes_InvalidSourceChain(t *testing.T) {
	h := &Handler{Client: &fakeClient{}}
	rec := doRequest(h, `{"source_chain":"mars","destination_chain":"base","asset":"USDC","amount":1000}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestPostRoutes_InvalidDestinationChain(t *testing.T) {
	h := &Handler{Client: &fakeClient{}}
	rec := doRequest(h, `{"source_chain":"ethereum","destination_chain":"mars","asset":"USDC","amount":1000}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestPostRoutes_InvalidAsset(t *testing.T) {
	h := &Handler{Client: &fakeClient{}}
	rec := doRequest(h, `{"source_chain":"ethereum","destination_chain":"base","asset":"DOGE","amount":1000}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestPostRoutes_NonPositiveAmount(t *testing.T) {
	h := &Handler{Client: &fakeClient{}}
	rec := doRequest(h, `{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":0}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestPostRoutes_NegativeAmount(t *testing.T) {
	h := &Handler{Client: &fakeClient{}}
	rec := doRequest(h, `{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":-5}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestPostRoutes_SameChain(t *testing.T) {
	h := &Handler{Client: &fakeClient{}}
	rec := doRequest(h, `{"source_chain":"ethereum","destination_chain":"ethereum","asset":"USDC","amount":1000}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestPostRoutes_SuccessfulRoute(t *testing.T) {
	fake := &fakeClient{response: &routingv1.FindRouteResponse{
		RouteFound: true,
		TotalFee:   2.5,
		Hops: []*routingv1.RouteHop{
			{FromChain: routingv1.Chain_CHAIN_ETHEREUM, ToChain: routingv1.Chain_CHAIN_ARBITRUM, BridgeName: "Hop#1", Fee: 1.0, LatencyMs: 500, Liquidity: 1000000, Reliability: 0.99},
			{FromChain: routingv1.Chain_CHAIN_ARBITRUM, ToChain: routingv1.Chain_CHAIN_BASE, BridgeName: "Hop#2", Fee: 1.5, LatencyMs: 500, Liquidity: 1000000, Reliability: 0.98},
		},
	}}
	h := &Handler{Client: fake}
	rec := doRequest(h, `{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":1000}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp findRouteResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if !resp.RouteFound || len(resp.Hops) != 2 || resp.TotalFee != 2.5 {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if resp.Hops[0].FromChain != "ethereum" || resp.Hops[1].ToChain != "base" {
		t.Fatalf("unexpected hop chain names: %+v", resp.Hops)
	}
	if fake.lastReq.GetSourceChain() != routingv1.Chain_CHAIN_ETHEREUM {
		t.Fatalf("expected source chain translated to ETHEREUM, got %v", fake.lastReq.GetSourceChain())
	}
}

func TestPostRoutes_NoRouteFound(t *testing.T) {
	fake := &fakeClient{response: &routingv1.FindRouteResponse{RouteFound: false}}
	h := &Handler{Client: fake}
	rec := doRequest(h, `{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":1000}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var resp findRouteResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.RouteFound {
		t.Fatalf("expected route_found=false")
	}
}

func TestPostRoutes_GRPCInvalidArgumentMapsTo400(t *testing.T) {
	fake := &fakeClient{err: status.Error(codes.InvalidArgument, "bad request")}
	h := &Handler{Client: fake}
	rec := doRequest(h, `{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":1000}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestPostRoutes_GRPCUnavailableMapsTo503(t *testing.T) {
	fake := &fakeClient{err: status.Error(codes.Unavailable, "down")}
	h := &Handler{Client: fake}
	rec := doRequest(h, `{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":1000}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

func TestPostRoutes_GRPCDeadlineExceededMapsTo504(t *testing.T) {
	fake := &fakeClient{err: status.Error(codes.DeadlineExceeded, "timeout")}
	h := &Handler{Client: fake}
	rec := doRequest(h, `{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":1000}`)
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("expected 504, got %d", rec.Code)
	}
}

func TestPostRoutes_GRPCInternalMapsTo500(t *testing.T) {
	fake := &fakeClient{err: status.Error(codes.Internal, "boom")}
	h := &Handler{Client: fake}
	rec := doRequest(h, `{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":1000}`)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
}
```

- [ ] **Step 3: Build and test**

```bash
cd go-api
go build ./...
go test ./internal/handler/...
cd ..
```

Expected: builds and all 13 tests pass.

- [ ] **Step 4: Commit**

```bash
git add go-api/internal/handler/
git commit -m "feat(go-api): add POST /routes handler with validation and error mapping"
```

---

### Task 4: Server entry point with graceful shutdown

**Files:**
- Create: `go-api/cmd/server/main.go`

**Interfaces:**
- Consumes: `grpcclient.Client` (Task 2), `handler.Handler` (Task 3).
- Produces: a runnable `go-api` HTTP server binary.

- [ ] **Step 1: Write main.go**

`go-api/cmd/server/main.go`:

```go
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"chainroute/go-api/internal/grpcclient"
	"chainroute/go-api/internal/handler"
)

func main() {
	httpAddr := flag.String("http-addr", ":8080", "HTTP listen address")
	grpcAddr := flag.String("grpc-addr", "127.0.0.1:50051", "gRPC routing service address")
	flag.Parse()

	client, err := grpcclient.Dial(*grpcAddr)
	if err != nil {
		log.Fatalf("failed to dial routing service at %s: %v", *grpcAddr, err)
	}
	defer client.Close()

	h := &handler.Handler{Client: client}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /routes", h.PostRoutes)

	server := &http.Server{Addr: *httpAddr, Handler: mux}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("go-api listening on %s, routing service at %s", *httpAddr, *grpcAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown error: %v", err)
	}
	log.Println("go-api shut down")
}
```

- [ ] **Step 2: Build**

```bash
cd go-api
go build -o /tmp/go-api-server ./cmd/server
cd ..
```

Expected: builds successfully.

- [ ] **Step 3: Manual smoke test of graceful shutdown (no C++ service needed yet)**

```bash
/tmp/go-api-server --http-addr=:8099 --grpc-addr=127.0.0.1:1 &
SERVER_PID=$!
sleep 1
kill -TERM "$SERVER_PID"
wait "$SERVER_PID"
```

Expected: prints `shutting down...` then `go-api shut down`, exits cleanly
(the `--grpc-addr=127.0.0.1:1` is deliberately a nowhere-listening address
— the server should still start and shut down gracefully since
`grpcclient.Dial` doesn't eagerly connect).

- [ ] **Step 4: Commit**

```bash
git add go-api/cmd/
git commit -m "feat(go-api): add server entry point with graceful shutdown"
```

---

### Task 5: Local end-to-end test script

**Files:**
- Create: `scripts/e2e_test.sh`

**Interfaces:**
- Consumes: `cpp-routing-service/build/chainroute_service_server` (Plan A),
  `go-api`'s built binary (Tasks 1-4).

- [ ] **Step 1: Write the script**

`scripts/e2e_test.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CPP_PORT=50098
HTTP_PORT=8099
SEED=1001

cleanup() {
    [[ -n "${GO_PID:-}" ]] && kill "$GO_PID" 2>/dev/null || true
    [[ -n "${CPP_PID:-}" ]] && kill "$CPP_PID" 2>/dev/null || true
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

echo "Waiting for C++ service readiness via grpc.health.v1..."
READY=0
for i in $(seq 1 30); do
    if grpc_health_probe -addr="127.0.0.1:$CPP_PORT" >/dev/null 2>&1; then
        echo "C++ service is ready."
        READY=1
        break
    fi
    sleep 0.5
done
if [[ "$READY" -ne 1 ]]; then
    echo "C++ service did not become ready in time" >&2
    exit 1
fi

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
echo "$RESPONSE" | grep -q '"route_found"' || { echo "FAIL: missing route_found field"; exit 1; }
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

echo "All E2E checks passed."
```

- [ ] **Step 2: Make it executable and run it**

```bash
chmod +x scripts/e2e_test.sh
./scripts/e2e_test.sh
```

Expected: all three tests print `OK`, ending with `All E2E checks
passed.`, and both background processes are cleaned up (verify with
`jobs` or `ps` that nothing from this run is still listening on
`$CPP_PORT`/`$HTTP_PORT` afterward).

**If `grpc_health_probe` isn't installed:** install it
(`brew install grpc-health-probe`) — Plan A's Task 4 already recommended
this. If it genuinely cannot be installed in this environment, replace
the readiness loop with a plain TCP-connect check
(`nc -z 127.0.0.1 "$CPP_PORT"`) as a fallback — weaker (doesn't confirm
the gRPC layer is actually serving, just that the port is open) but keeps
the script working; note which approach was used in the commit message.

- [ ] **Step 3: Commit**

```bash
git add scripts/e2e_test.sh
git commit -m "test(e2e): add local Go -> gRPC -> C++ smoke test script"
```

---

### Task 6: Full Phase 4 verification and reporting

**Files:**
- None created. Potentially modify any file if a warning or failure
  surfaces.

**Interfaces:**
- Consumes: everything from Plan A and this plan.
- Produces: full confirmation of all items 1-9 from the user's
  post-implementation request list, ready for the controller to compose
  the final response (items 10-11 are the controller's job, not this
  task's — see note at the end).

- [ ] **Step 1: Clean-build router/ and verify all 49 tests still pass**

```bash
rm -rf router/build
cmake -S router -B router/build
cmake --build router/build
ctest --test-dir router/build --output-on-failure
```

Expected: 49/49 pass.

- [ ] **Step 2: Clean-build and test the C++ routing service, with warnings enabled**

```bash
rm -rf cpp-routing-service/build
cmake -S cpp-routing-service -B cpp-routing-service/build
cmake --build cpp-routing-service/build 2>&1 | tee /tmp/chainroute_service_build.log
grep -E "cpp-routing-service/(src|include)/" /tmp/chainroute_service_build.log || echo "No project-code warnings"
ctest --test-dir cpp-routing-service/build --output-on-failure
```

Expected: no project-code warnings, all `chainroute_service_tests` pass.
Fix any real warning/failure at its root cause before continuing.

- [ ] **Step 3: Run all Go tests, with vet enabled**

```bash
cd go-api
go vet ./...
go test ./...
cd ..
```

Expected: `go vet` reports nothing (Go's equivalent of a warnings check);
all tests pass. Fix any real issue at its root cause before continuing.

- [ ] **Step 4: Run the real end-to-end test**

```bash
./scripts/e2e_test.sh
```

Expected: `All E2E checks passed.`

- [ ] **Step 5: Verify Phase 1-3 files were not modified by either plan**

```bash
git diff --stat 4a898b5..HEAD -- router/
```

(If `4a898b5` is not the right pre-Phase-4 commit in this checkout, use
`git log --oneline -- router/` to find the last commit that touched
`router/` and confirm it predates Plan A's commits.) Expected: empty
output. If this shows any changes, that is a plan violation — investigate
and revert unless a demonstrated correctness issue justifies it, in which
case stop and report to the human partner rather than proceeding.

- [ ] **Step 6: Capture the final repository structure**

```bash
find router proto cpp-routing-service go-api scripts \
    -type f -not -path "*/build/*" -not -path "*/.git/*" | sort
```

- [ ] **Step 7: Capture the files-changed listing for the whole phase**

```bash
git diff --stat 4a898b5..HEAD -- proto/ cpp-routing-service/ go-api/ scripts/
```

- [ ] **Step 8: Capture one real curl request/response from the running stack**

```bash
cmake --build cpp-routing-service/build >/dev/null
./cpp-routing-service/build/chainroute_service_server --seed=1001 --listen-address=127.0.0.1:50097 &
CPP_PID=$!
sleep 1
(cd go-api && go build -o /tmp/go-api-demo ./cmd/server)
/tmp/go-api-demo --http-addr=:8098 --grpc-addr=127.0.0.1:50097 &
GO_PID=$!
sleep 1

curl -s -X POST http://127.0.0.1:8098/routes \
    -H "Content-Type: application/json" \
    -d '{"source_chain":"ethereum","destination_chain":"base","asset":"USDC","amount":1000}' | tee /tmp/curl_demo_response.json

kill -TERM "$GO_PID" "$CPP_PID"
wait "$GO_PID" "$CPP_PID" 2>/dev/null || true
```

Save the exact request and the exact printed response — this is needed
verbatim for the final report to the user (item 9 of their request).

- [ ] **Step 9: Commit (only if Step 2 or Step 3 required code changes)**

```bash
git add -A
git commit -m "fix: resolve issues found during Phase 4 full verification"
```

If no changes were needed, skip this commit.

- [ ] **Step 10: Report back**

Report to the controller: the Step 6 repository structure, the Step 7
files-changed listing, the exact Step 8 curl command and response, and
confirmation of Steps 1-5's pass/fail results. The controller (not this
task) is responsible for composing the final explanation of the complete
request lifecycle (item 10 of the user's request) and the closing summary
— that requires synthesizing across both plans and is written directly
for the user, not as an implementation step.
