package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSpanNameFormatter_UsesBoundedRoutePatternNotRawPath(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/payments/7f3a9c21-0b4e-4c2d-9a11-abc123456789", nil)
	req.Pattern = "GET /payments/{id}"

	got := spanNameFormatter("http.server", req)

	if got != "GET /payments/{id}" {
		t.Errorf("spanNameFormatter = %q, want %q", got, "GET /payments/{id}")
	}
}

func TestSpanNameFormatter_TwoDifferentPaymentIDsProduceTheSameSpanName(t *testing.T) {
	req1 := httptest.NewRequest(http.MethodGet, "/payments/11111111-2222-3333-4444-555555555555", nil)
	req1.Pattern = "GET /payments/{id}"
	req2 := httptest.NewRequest(http.MethodGet, "/payments/99999999-8888-7777-6666-000000000000", nil)
	req2.Pattern = "GET /payments/{id}"

	name1 := spanNameFormatter("http.server", req1)
	name2 := spanNameFormatter("http.server", req2)

	if name1 != name2 {
		t.Errorf("span names differ across payment IDs (%q vs %q) -- unbounded per-request cardinality, exactly the bug this fix closes", name1, name2)
	}
}

func TestSpanNameFormatter_FallsBackToMethodAndPathWhenPatternUnset(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/routes", nil)
	req.Pattern = ""

	got := spanNameFormatter("http.server", req)

	if got != "POST /routes" {
		t.Errorf("spanNameFormatter = %q, want %q", got, "POST /routes")
	}
}
