package observability

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// fakeInFlightCounter lets tests control exactly what CountInFlightPayments
// returns, without a real database -- the collector's own behavior (bounded
// label set, zero-when-absent, error handling) is what's under test here,
// not the SQL (that's covered by the real Postgres integration test,
// TestCountInFlightPayments_ReflectsRealNonTerminalPaymentsByMode).
type fakeInFlightCounter struct {
	counts map[string]int64
	err    error
}

func (f *fakeInFlightCounter) CountInFlightPayments(context.Context) (map[string]int64, error) {
	return f.counts, f.err
}

// gaugeValue gathers reg and returns the value of
// chainroute_payments_processing{execution_mode=mode}, or fails the test if
// no such series exists.
func gaugeValue(t *testing.T, reg *prometheus.Registry, mode string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != "chainroute_payments_processing" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "execution_mode" && lp.GetValue() == mode {
					return m.GetGauge().GetValue()
				}
			}
		}
	}
	t.Fatalf("no chainroute_payments_processing series found for execution_mode=%s", mode)
	return 0
}

// seriesCount returns how many chainroute_payments_processing series
// Gather actually produced.
func seriesCount(t *testing.T, reg *prometheus.Registry) int {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() == "chainroute_payments_processing" {
			return len(f.GetMetric())
		}
	}
	return 0
}

func TestPaymentsInFlightCollector_NoActivePayments_ReportsZeroForBothModes(t *testing.T) {
	fake := &fakeInFlightCounter{counts: map[string]int64{}}
	c := NewPaymentsInFlightCollector(fake, nil)
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)

	if got := gaugeValue(t, reg, "simulated"); got != 0 {
		t.Errorf("chainroute_payments_processing{execution_mode=simulated} = %v, want 0", got)
	}
	if got := gaugeValue(t, reg, "testnet"); got != 0 {
		t.Errorf("chainroute_payments_processing{execution_mode=testnet} = %v, want 0", got)
	}
}

func TestPaymentsInFlightCollector_ReflectsWhateverTheStoreReturns(t *testing.T) {
	fake := &fakeInFlightCounter{counts: map[string]int64{"simulated": 7, "testnet": 3}}
	c := NewPaymentsInFlightCollector(fake, nil)
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)

	if got := gaugeValue(t, reg, "simulated"); got != 7 {
		t.Errorf("chainroute_payments_processing{execution_mode=simulated} = %v, want 7", got)
	}
	if got := gaugeValue(t, reg, "testnet"); got != 3 {
		t.Errorf("chainroute_payments_processing{execution_mode=testnet} = %v, want 3", got)
	}
}

func TestPaymentsInFlightCollector_UnknownModeInStoreResultIsIgnored(t *testing.T) {
	// The label set is closed to knownExecutionModes -- a store result
	// keyed by anything else (a future third mode, a data bug) must never
	// create a new, unbounded label series.
	fake := &fakeInFlightCounter{counts: map[string]int64{"simulated": 1, "some-future-mode": 999}}
	c := NewPaymentsInFlightCollector(fake, nil)
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)

	if got := seriesCount(t, reg); got != 2 {
		t.Errorf("expected exactly 2 series (simulated, testnet), got %d", got)
	}
}

func TestPaymentsInFlightCollector_StoreErrorEmitsNothingAndDoesNotPanic(t *testing.T) {
	fake := &fakeInFlightCounter{err: errors.New("db unreachable")}
	c := NewPaymentsInFlightCollector(fake, nil)
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)

	if got := seriesCount(t, reg); got != 0 {
		t.Errorf("expected no series emitted after a store error, got %d", got)
	}
}

func TestPaymentsInFlightCollector_ValueCanNeverBeNegative(t *testing.T) {
	// counts is a map[string]int64; a missing key (e.g. a mode with zero
	// in-flight payments) reads as the Go zero value, 0 -- never negative.
	// This is the structural property that makes the old Inc()/Dec()
	// gauge's negative-drift bug unreachable here.
	fake := &fakeInFlightCounter{counts: map[string]int64{}}
	c := NewPaymentsInFlightCollector(fake, nil)
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)

	for _, mode := range []string{"simulated", "testnet"} {
		if got := gaugeValue(t, reg, mode); got < 0 {
			t.Errorf("chainroute_payments_processing{execution_mode=%s} = %v, must never be negative", mode, got)
		}
	}
}

func TestPaymentsInFlightCollector_NilLoggerFallsBackToDefault(t *testing.T) {
	fake := &fakeInFlightCounter{err: errors.New("db unreachable")}
	c := NewPaymentsInFlightCollector(fake, nil) // must not panic on the error path with a nil Logger
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)

	if _, err := reg.Gather(); err != nil {
		t.Fatalf("Gather: %v", err)
	}
}
