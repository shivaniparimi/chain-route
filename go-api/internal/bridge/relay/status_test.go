package relay

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"chainroute/go-api/internal/bridge/quote"
)

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
