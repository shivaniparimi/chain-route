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
