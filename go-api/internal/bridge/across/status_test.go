package across

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// realDepositNotFoundFixture is the exact error body this plan's live
// verification captured from https://testnet.across.to/api/deposit/status
// (re-confirmed by Task 1, see
// docs/superpowers/reports/2026-09-13-across-verification-findings.md).
const realDepositNotFoundFixture = `{"error":"DepositNotFoundException","message":"Deposit not found given the provided constraints"}`

func TestDepositStatusByTxHash_NotFoundMapsToSentinelError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("depositTxHash"); got != "0xabc" {
			t.Fatalf("expected depositTxHash=0xabc, got %q", got)
		}
		if got := r.URL.Query().Get("originChainId"); got != "11155111" {
			t.Fatalf("expected originChainId=11155111, got %q", got)
		}
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(realDepositNotFoundFixture))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	_, err := c.DepositStatusByTxHash(context.Background(), 11155111, "0xabc")
	if !errors.Is(err, ErrDepositNotFound) {
		t.Fatalf("expected ErrDepositNotFound, got %v", err)
	}
}

func TestDepositStatusByTxHash_ParsesFilledResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"filled"}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	resp, err := c.DepositStatusByTxHash(context.Background(), 11155111, "0xabc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != "filled" {
		t.Fatalf("expected status=filled, got %q", resp.Status)
	}
}

func TestDepositStatusByTxHash_UnrecognizedStatusDoesNotError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"some-future-status-this-client-has-never-seen"}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	resp, err := c.DepositStatusByTxHash(context.Background(), 11155111, "0xabc")
	if err != nil {
		t.Fatalf("an unrecognized status string must parse successfully (the reconciler decides what to do with it), got error: %v", err)
	}
	if resp.Status != "some-future-status-this-client-has-never-seen" {
		t.Fatalf("expected the raw status string to be preserved, got %q", resp.Status)
	}
}

// TestDepositStatusByTxHash_ParsesOtherDocumentedStatuses locks in that
// every one of the 11 status values Across's own generated TypeScript
// interface documents (see docs/superpowers/reports/2026-09-13-across-
// verification-findings.md, Step 2) round-trips through this client
// without error -- not just "filled" and an arbitrary unknown string.
func TestDepositStatusByTxHash_ParsesOtherDocumentedStatuses(t *testing.T) {
	documented := []string{
		"pending", "expired", "refunded",
		"slowFillRequested", "slowFilled",
		"deposit-pending", "deposit-failed",
		"auto-refund-pending", "manual-refund-required", "refund-failed",
	}

	for _, status := range documented {
		status := status
		t.Run(status, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(`{"status":"` + status + `"}`))
			}))
			defer srv.Close()

			c := NewClient(srv.URL)
			resp, err := c.DepositStatusByTxHash(context.Background(), 11155111, "0xabc")
			if err != nil {
				t.Fatalf("unexpected error for documented status %q: %v", status, err)
			}
			if resp.Status != status {
				t.Fatalf("expected status=%q, got %q", status, resp.Status)
			}
		})
	}
}
