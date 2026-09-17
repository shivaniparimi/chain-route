package observability

import (
	"bytes"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetrics_PaymentCountersIncrement(t *testing.T) {
	m := NewMetrics()
	m.PaymentsCreated.WithLabelValues("simulated").Inc()
	m.PaymentsCreated.WithLabelValues("simulated").Inc()
	m.PaymentsCompleted.WithLabelValues("testnet").Inc()

	if got := testutil.ToFloat64(m.PaymentsCreated.WithLabelValues("simulated")); got != 2 {
		t.Errorf("PaymentsCreated{simulated} = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.PaymentsCompleted.WithLabelValues("testnet")); got != 1 {
		t.Errorf("PaymentsCompleted{testnet} = %v, want 1", got)
	}
}

func TestMetrics_DurationHistogramRecordsObservations(t *testing.T) {
	m := NewMetrics()
	m.PaymentDuration.WithLabelValues("simulated", "completed").Observe(0.5)

	count := testutil.CollectAndCount(m.PaymentDuration)
	if count != 1 {
		t.Errorf("expected 1 histogram series registered, got %d", count)
	}
}

func TestSanitizeProviderLabel_KnownProvidersPassThrough(t *testing.T) {
	if got := SanitizeProviderLabel("across"); got != "across" {
		t.Errorf("SanitizeProviderLabel(across) = %q, want across", got)
	}
	if got := SanitizeProviderLabel("relay"); got != "relay" {
		t.Errorf("SanitizeProviderLabel(relay) = %q, want relay", got)
	}
}

func TestSanitizeProviderLabel_UnknownProviderBoundedToUnknown(t *testing.T) {
	// A future third provider, a bug, or adversarial input must never
	// pass through to a Prometheus label unbounded -- this is the
	// cardinality guard the design doc requires.
	if got := SanitizeProviderLabel("some-new-bridge-nobody-registered"); got != "unknown" {
		t.Errorf("SanitizeProviderLabel(unregistered) = %q, want unknown", got)
	}
	if got := SanitizeProviderLabel(""); got != "unknown" {
		t.Errorf("SanitizeProviderLabel(empty) = %q, want unknown", got)
	}
}

func TestDefaultMetrics_ReturnsSameSharedInstanceAcrossCalls(t *testing.T) {
	// Callers whose struct literal leaves Metrics unset must fall back to
	// one shared, safe-to-record-into instance -- not nil, and not a
	// fresh, wasteful registry per call.
	a := DefaultMetrics()
	b := DefaultMetrics()
	if a != b {
		t.Error("DefaultMetrics() must return the same instance on repeated calls")
	}
	a.PaymentsCreated.WithLabelValues("simulated").Inc() // must not panic
}

func TestMetrics_RegistryExposesPrometheusTextFormat(t *testing.T) {
	m := NewMetrics()
	m.PaymentsCreated.WithLabelValues("simulated").Inc()

	if err := testutil.GatherAndCompare(m.Registry, bytes.NewReader([]byte{})); err == nil {
		// GatherAndCompare with no expected input just gathers; if it
		// errors, gathering itself is broken (registration conflict).
	}
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatalf("Gather failed: %v", err)
	}
	if len(families) == 0 {
		t.Fatal("expected at least one registered metric family")
	}
}
