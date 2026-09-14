package handler

import (
	"context"
	"encoding/json"
	"log"
	"math/big"
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
	Client              RoutingClient
	Store               PaymentStore
	BlockchainEnv       string   // "testnet" enables execution_mode="testnet" requests; anything else (including "") rejects them
	MaxTestnetAmountWei *big.Int // nil means no ceiling is enforced (only safe when BlockchainEnv != "testnet")
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
