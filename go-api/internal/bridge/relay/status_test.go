package relay

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"chainroute/go-api/internal/bridge/quote"
)

// TestCheckStatus_EscapesProviderReferenceIDInQueryString guards M5: the
// requestId must be sent through url.Values (properly percent-encoded),
// not raw string concatenation, which would corrupt a reference ID
// containing a reserved query-string character like "&" or "=" into a
// different (or additional) query parameter entirely.
func TestCheckStatus_EscapesProviderReferenceIDInQueryString(t *testing.T) {
	const trickyRequestID = "0xabc&evil=1"
	var gotRequestID string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRequestID = r.URL.Query().Get("requestId")
		w.Write([]byte(`{"status":"pending"}`))
	}))
	defer srv.Close()

	p := &Provider{Client: NewClient(srv.URL)}
	if _, err := p.CheckStatus(context.Background(), quote.StatusRequest{ProviderReferenceID: trickyRequestID}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotRequestID != trickyRequestID {
		t.Errorf("server received requestId %q, want %q -- raw concatenation would have split this into a separate query parameter", gotRequestID, trickyRequestID)
	}
}

func TestCheckStatus_MapsRelayStatusesToSharedStates(t *testing.T) {
	cases := []struct {
		relayStatus string
		want        quote.ExternalState
	}{
		{"waiting", quote.StatePending},
		{"depositing", quote.StatePending},
		{"pending", quote.StatePending},
		{"submitted", quote.StatePending},
		{"delayed", quote.StatePending},
		{"success", quote.StateFilled},
		{"refund", quote.StateRefunded},
		{"failure", quote.StateFillFailed},
		{"unknown", quote.StatePending},
		{"some-future-status", quote.StatePending},
	}
	for _, c := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`{"status":"` + c.relayStatus + `"}`))
		}))
		p := &Provider{Client: NewClient(srv.URL)}
		result, err := p.CheckStatus(context.Background(), quote.StatusRequest{ProviderReferenceID: "0xabc"})
		srv.Close()
		if err != nil {
			t.Fatalf("status %q: unexpected error: %v", c.relayStatus, err)
		}
		if result.State != c.want {
			t.Errorf("status %q: State = %q, want %q", c.relayStatus, result.State, c.want)
		}
		if result.RawStatus != c.relayStatus {
			t.Errorf("status %q: RawStatus = %q, want %q", c.relayStatus, result.RawStatus, c.relayStatus)
		}
	}
}
