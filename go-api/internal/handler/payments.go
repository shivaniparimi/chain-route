package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"chainroute/go-api/internal/bridge/quote"
	routingv1 "chainroute/go-api/internal/gen/chainroute/v1"
	"chainroute/go-api/internal/money"
	"chainroute/go-api/internal/observability"
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
	FailureReason    *string       `json:"failure_reason"`
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
		FailureReason:  p.FailureReason,
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
	ctx, span := observability.Tracer("payment").Start(r.Context(), "payment.create")
	defer span.End()
	r = r.WithContext(ctx)

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
	if sourceChain == destChain {
		writeError(w, http.StatusBadRequest, "source and destination must differ")
		return
	}

	span.SetAttributes(
		attribute.String("payment.source_chain", req.SourceChain),
		attribute.String("payment.destination_chain", req.DestinationChain),
		attribute.String("payment.asset", req.Asset),
		attribute.String("payment.execution_mode", string(mode)),
	)

	// This float64 conversion feeds only the pre-existing (Phase 2-4,
	// unmodified) FindRouteRequest.amount `double` field, which has
	// always been a liquidity-filter threshold for the C++ simulator --
	// never the authoritative amount. req.Amount (the exact string) is
	// what gets persisted below, untouched by this conversion. Computed
	// here (rather than immediately before grpcReq, as in Phase 2-7)
	// because the testnet-mode block below also needs it, for
	// CandidateEdge.Liquidity.
	amountForRouting, err := strconv.ParseFloat(req.Amount, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid amount")
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
		h.logger().ErrorContext(r.Context(), "failed to look up payment by idempotency key", "error", err)
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
		var providers []quote.Provider
		if h.QuoteRegistry != nil {
			providers = h.QuoteRegistry.ProvidersFor(routeKey)
		}
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

		type quoteResult struct {
			provider quote.Provider
			q        quote.Quote
			err      error
		}

		quoteCtx, quoteCancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer quoteCancel()
		aggCtx, aggSpan := observability.Tracer("quote").Start(quoteCtx, "quote.aggregate")
		results := make([]quoteResult, len(providers))
		var wg sync.WaitGroup
		for i, p := range providers {
			wg.Add(1)
			go func(i int, p quote.Provider) {
				defer wg.Done()
				providerLabel := observability.SanitizeProviderLabel(p.Name())
				spanCtx, qSpan := observability.Tracer("quote").Start(aggCtx, "quote."+providerLabel+".get")
				defer qSpan.End()
				h.metrics().QuoteRequests.WithLabelValues(providerLabel).Inc()
				start := time.Now()
				q, err := p.GetQuote(spanCtx, quote.Request{
					SourceChainID: originChainID, DestinationChainID: destChainID, Asset: bridgedAsset, AmountBaseUnits: amountWei,
				})
				h.metrics().QuoteDuration.WithLabelValues(providerLabel).Observe(time.Since(start).Seconds())
				if err != nil {
					h.metrics().QuoteFailures.WithLabelValues(providerLabel, "http_error").Inc()
				} else {
					available := "false"
					if q.Available {
						available = "true"
					}
					h.metrics().QuoteAvailable.WithLabelValues(providerLabel, available).Inc()
				}
				results[i] = quoteResult{provider: p, q: q, err: err}
			}(i, p)
		}
		wg.Wait()
		aggSpan.End()

		anySucceeded, anyAvailable := false, false
		for _, res := range results { // index order, never completion order -- keeps the C++ tie-break deterministic
			if res.err != nil {
				h.logger().WarnContext(r.Context(), "quote provider failed", "provider", res.provider.Name(), "error", res.err)
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
	}

	grpcReq := &routingv1.FindRouteRequest{
		SourceChain: sourceChain, DestinationChain: destChain,
		Asset: asset, Amount: amountForRouting, CandidateEdges: candidateEdges,
	}

	routingCtx, routingSpan := observability.Tracer("routing").Start(r.Context(), "routing.find")
	h.metrics().RoutingRequests.WithLabelValues(string(mode)).Inc()
	routingStart := time.Now()
	resp, err := h.Client.FindRoute(routingCtx, grpcReq)
	h.metrics().RoutingDuration.WithLabelValues(string(mode)).Observe(time.Since(routingStart).Seconds())
	routingSpan.End()
	if err != nil {
		st, _ := status.FromError(err)
		switch st.Code() {
		case codes.InvalidArgument:
			h.metrics().RoutingFailures.WithLabelValues("invalid_request").Inc()
			h.logger().WarnContext(r.Context(), "routing service rejected a request that passed Go validation (possible validation drift)", "error", st.Message())
			writeError(w, http.StatusBadRequest, st.Message())
		case codes.Unavailable:
			h.metrics().RoutingFailures.WithLabelValues("grpc_error").Inc()
			writeError(w, http.StatusServiceUnavailable, "routing service unavailable")
		case codes.DeadlineExceeded:
			h.metrics().RoutingFailures.WithLabelValues("grpc_error").Inc()
			writeError(w, http.StatusGatewayTimeout, "routing service timed out")
		default:
			h.metrics().RoutingFailures.WithLabelValues("grpc_error").Inc()
			h.logger().ErrorContext(r.Context(), "routing service call failed", "error", err)
			writeError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}

	if !resp.GetRouteFound() {
		h.metrics().RoutingFailures.WithLabelValues("no_route").Inc()
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

	if mode == payment.ExecutionModeTestnet && len(hops) > 0 {
		winningQuote, ok := quotesByBridgeName[hops[0].BridgeName]
		if !ok {
			h.logger().ErrorContext(r.Context(), "winning hop bridge_name has no matching fetched quote -- this should be unreachable", "bridge_name", hops[0].BridgeName)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		provider := winningQuote.ProviderName
		bridgeProvider = &provider

		providerLabel := observability.SanitizeProviderLabel(provider)
		h.metrics().RoutingSelectedProvider.WithLabelValues(providerLabel).Inc()
		h.metrics().QuoteSelected.WithLabelValues(providerLabel).Inc()
		h.metrics().RoutingSelectedFee.WithLabelValues(providerLabel).Observe(float64(winningQuote.FeeBaseUnits.Int64()))
		span.SetAttributes(attribute.String("payment.provider", providerLabel))
	}

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
			InputAmount:          winningQuote.InputAmountBaseUnits.String(),
			OutputAmount:         winningQuote.OutputAmountBaseUnits.String(),
			FeeAmount:            winningQuote.FeeBaseUnits.String(),
			EstimatedFillTimeSec: winningQuote.EstimatedFillTimeSec,
			QuotedAt:             winningQuote.QuotedAt, ExpiresAt: winningQuote.ExpiresAt,
			RawProviderPayload: winningQuote.RawProviderPayload,
		}
	}

	result, outcome, err := h.Store.CreateOrGetPayment(r.Context(), candidate)
	if err != nil {
		h.logger().ErrorContext(r.Context(), "failed to persist payment", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	switch outcome {
	case payment.Created:
		h.metrics().PaymentsCreated.WithLabelValues(string(mode)).Inc()
		h.metrics().PaymentsProcessing.WithLabelValues(string(mode)).Inc()
		span.SetAttributes(attribute.String("payment.id", result.ID), attribute.String("payment.status", string(result.Status)))
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
		h.logger().ErrorContext(r.Context(), "failed to read payment", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "payment not found")
		return
	}
	exec, execFound, err := h.Store.GetExecutionByPaymentID(r.Context(), id)
	if err != nil {
		h.logger().ErrorContext(r.Context(), "failed to read payment execution", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, toPaymentResponse(p, exec, execFound))
}
