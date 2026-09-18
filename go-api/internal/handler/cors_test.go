package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCORS_AllowedOriginGetsHeaders(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := CORS([]string{"http://localhost:5173"}, inner)

	req := httptest.NewRequest("GET", "/payments", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:5173" {
		t.Fatalf("expected Access-Control-Allow-Origin to be echoed, got %q", got)
	}
	if got := rec.Header().Get("Vary"); got != "Origin" {
		t.Fatalf("expected Vary: Origin, got %q", got)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected request to reach inner handler, got %d", rec.Code)
	}
}

func TestCORS_DisallowedOriginGetsNoHeaders(t *testing.T) {
	called := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	h := CORS([]string{"http://localhost:5173"}, inner)

	req := httptest.NewRequest("GET", "/payments", nil)
	req.Header.Set("Origin", "http://evil.example.com")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("expected no Access-Control-Allow-Origin header for a disallowed origin, got %q", got)
	}
	if !called {
		t.Fatal("expected the request to still reach the inner handler (CORS only gates headers, not access)")
	}
}

func TestCORS_NeverReflectsWildcard(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	h := CORS([]string{"http://localhost:5173"}, inner)

	req := httptest.NewRequest("GET", "/payments", nil)
	req.Header.Set("Origin", "*")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got == "*" {
		t.Fatal("must never echo/allow a wildcard origin")
	}
}

func TestCORS_OptionsPreflightReturnsNoContent(t *testing.T) {
	called := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	h := CORS([]string{"http://localhost:5173"}, inner)

	req := httptest.NewRequest("OPTIONS", "/payments", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204 No Content for OPTIONS preflight, got %d", rec.Code)
	}
	if called {
		t.Fatal("preflight OPTIONS must not reach the inner handler")
	}
	if got := rec.Header().Get("Access-Control-Allow-Methods"); got != "GET, POST, OPTIONS" {
		t.Fatalf("unexpected Access-Control-Allow-Methods: %q", got)
	}
}

func TestCORS_NoOriginHeaderPassesThroughWithoutCORSHeaders(t *testing.T) {
	called := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	h := CORS([]string{"http://localhost:5173"}, inner)

	req := httptest.NewRequest("GET", "/payments", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if !called {
		t.Fatal("expected a same-origin/no-Origin request to reach the inner handler")
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("expected no CORS headers when no Origin header is present, got %q", got)
	}
}
