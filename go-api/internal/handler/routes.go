package handler

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/big"
	"net/http"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"chainroute/go-api/internal/bridge/quote"
	routingv1 "chainroute/go-api/internal/gen/chainroute/v1"
	"chainroute/go-api/internal/observability"
)

type RoutingClient interface {
	FindRoute(ctx context.Context, req *routingv1.FindRouteRequest) (*routingv1.FindRouteResponse, error)
}

type Handler struct {
	Client              RoutingClient
	Store               PaymentStore
	BlockchainEnv       string          // "testnet" enables execution_mode="testnet" requests; anything else (including "") rejects them
	MaxTestnetAmountWei *big.Int        // nil means no ceiling is enforced (only safe when BlockchainEnv != "testnet")
	QuoteRegistry       *quote.Registry // nil when BlockchainEnv != "testnet"; never consulted otherwise
	Metrics             *observability.Metrics
	Logger              *slog.Logger
}

// metrics returns h.Metrics, or a shared safe-to-record-into default when
// it is nil -- e.g. for an existing test's struct literal that predates
// this phase and never sets the field. Every instrumentation call in this
// package must go through this accessor, never through h.Metrics
// directly, so that a nil Metrics field can never nil-pointer-panic.
func (h *Handler) metrics() *observability.Metrics {
	if h.Metrics != nil {
		return h.Metrics
	}
	return observability.DefaultMetrics()
}

// logger mirrors metrics: it returns h.Logger, or a shared default when
// nil. Every log call in this package must go through this accessor,
// never through h.Logger directly.
func (h *Handler) logger() *slog.Logger {
	if h.Logger != nil {
		return h.Logger
	}
	return observability.DefaultLogger()
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
			h.logger().WarnContext(r.Context(), "routing service rejected a request that passed Go validation (possible validation drift)", "error", st.Message())
			writeError(w, http.StatusBadRequest, st.Message())
		case codes.Unavailable:
			writeError(w, http.StatusServiceUnavailable, "routing service unavailable")
		case codes.DeadlineExceeded:
			writeError(w, http.StatusGatewayTimeout, "routing service timed out")
		default:
			h.logger().ErrorContext(r.Context(), "routing service call failed", "error", err)
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
