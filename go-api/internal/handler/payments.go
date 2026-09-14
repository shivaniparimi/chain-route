package handler

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	routingv1 "chainroute/go-api/internal/gen/chainroute/v1"
	"chainroute/go-api/internal/money"
	"chainroute/go-api/internal/payment"
)

var amountPattern = regexp.MustCompile(`^\d{1,20}(\.\d{1,18})?$`)

const maxIdempotencyKeyLength = 255

type PaymentStore interface {
	CreateOrGetPayment(ctx context.Context, p payment.Payment) (payment.Payment, payment.CreateResult, error)
	GetPayment(ctx context.Context, id string) (payment.Payment, bool, error)
	LookupByIdempotencyKey(ctx context.Context, p payment.Payment) (payment.Payment, payment.CreateResult, bool, error)
	GetExecutionByPaymentID(ctx context.Context, paymentID string) (payment.Execution, bool, error)
}

var assetNameByValue = map[routingv1.Asset]string{
	routingv1.Asset_ASSET_USDC: "USDC",
	routingv1.Asset_ASSET_ETH:  "ETH",
}

type createPaymentRequest struct {
	SourceChain      string `json:"source_chain"`
	DestinationChain string `json:"destination_chain"`
	Asset            string `json:"asset"`
	Amount           string `json:"amount"`
	ExecutionMode    string `json:"execution_mode"`
}

type hopResponse struct {
	HopIndex    int     `json:"hop_index"`
	FromChain   string  `json:"from_chain"`
	ToChain     string  `json:"to_chain"`
	BridgeName  string  `json:"bridge_name"`
	Fee         float64 `json:"fee"`
	LatencyMs   float64 `json:"latency_ms"`
	Liquidity   float64 `json:"liquidity"`
	Reliability float64 `json:"reliability"`
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

func isZeroAmount(amount string) bool {
	for _, r := range amount {
		if r != '0' && r != '.' {
			return false
		}
	}
	return true
}

func (h *Handler) PostPayments(w http.ResponseWriter, r *http.Request) {
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if idempotencyKey == "" {
		writeError(w, http.StatusBadRequest, "Idempotency-Key header is required")
		return
	}
	if len(idempotencyKey) > maxIdempotencyKeyLength {
		writeError(w, http.StatusBadRequest, "Idempotency-Key exceeds maximum length")
		return
	}

	var req createPaymentRequest
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
	if !amountPattern.MatchString(req.Amount) || isZeroAmount(req.Amount) {
		writeError(w, http.StatusBadRequest,
			"amount must be a positive decimal with at most 20 integer digits and 18 fractional digits")
		return
	}

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

	if sourceChain == destChain {
		writeError(w, http.StatusBadRequest, "source and destination must differ")
		return
	}

	// Check for an existing payment under this idempotency key BEFORE
	// paying for a routing RPC. This lets a retry of an already-committed
	// payment resolve to its guaranteed outcome (200 Replayed, or 409
	// Conflict) even if the routing service happens to be down --
	// CreateOrGetPayment's own internal pre-check runs too late to help
	// here, since it only happens after routing already succeeded.
	lookupCandidate := payment.Payment{
		IdempotencyKey: idempotencyKey, SourceChain: chainNameByValue[sourceChain],
		DestinationChain: chainNameByValue[destChain], Asset: assetNameByValue[asset],
		Amount: req.Amount, ExecutionMode: mode,
	}
	if existing, outcome, found, err := h.Store.LookupByIdempotencyKey(r.Context(), lookupCandidate); err != nil {
		log.Printf("ERROR: failed to look up payment by idempotency key: %v", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	} else if found {
		switch outcome {
		case payment.Replayed:
			w.Header().Set("Location", "/payments/"+existing.ID)
			writeJSON(w, http.StatusOK, toPaymentResponse(existing, payment.Execution{}, false))
		case payment.Conflict:
			writeError(w, http.StatusConflict, "Idempotency-Key already used with a different request")
		}
		return
	}

	// This float64 conversion feeds only the pre-existing (Phase 2-4,
	// unmodified) FindRouteRequest.amount `double` field, which has
	// always been a liquidity-filter threshold for the C++ simulator --
	// never the authoritative amount. req.Amount (the exact string) is
	// what gets persisted below, untouched by this conversion.
	amountForRouting, err := strconv.ParseFloat(req.Amount, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid amount")
		return
	}

	grpcReq := &routingv1.FindRouteRequest{
		SourceChain: sourceChain, DestinationChain: destChain,
		Asset: asset, Amount: amountForRouting,
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

	if !resp.GetRouteFound() {
		writeError(w, http.StatusUnprocessableEntity, "no route available for the requested payment")
		return
	}

	hops := make([]payment.Hop, 0, len(resp.GetHops()))
	for i, hop := range resp.GetHops() {
		hops = append(hops, payment.Hop{
			HopIndex: i, FromChain: chainNameByValue[hop.GetFromChain()], ToChain: chainNameByValue[hop.GetToChain()],
			BridgeName: hop.GetBridgeName(), Fee: hop.GetFee(), LatencyMs: hop.GetLatencyMs(),
			Liquidity: hop.GetLiquidity(), Reliability: hop.GetReliability(),
		})
	}

	candidate := payment.Payment{
		IdempotencyKey: idempotencyKey, SourceChain: chainNameByValue[sourceChain],
		DestinationChain: chainNameByValue[destChain], Asset: assetNameByValue[asset],
		Amount: req.Amount, TotalFee: resp.GetTotalFee(), Hops: hops,
		ExecutionMode: mode, BridgeProvider: bridgeProvider,
	}

	result, outcome, err := h.Store.CreateOrGetPayment(r.Context(), candidate)
	if err != nil {
		log.Printf("ERROR: failed to persist payment: %v", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	switch outcome {
	case payment.Created:
		w.Header().Set("Location", "/payments/"+result.ID)
		writeJSON(w, http.StatusCreated, toPaymentResponse(result, payment.Execution{}, false))
	case payment.Replayed:
		w.Header().Set("Location", "/payments/"+result.ID)
		writeJSON(w, http.StatusOK, toPaymentResponse(result, payment.Execution{}, false))
	case payment.Conflict:
		writeError(w, http.StatusConflict, "Idempotency-Key already used with a different request")
	}
}

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
