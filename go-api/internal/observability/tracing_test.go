package observability

import (
	"context"
	"testing"
	"time"
)

func TestInitTracing_ReturnsWorkingShutdownEvenWithNoCollector(t *testing.T) {
	// No OTEL_EXPORTER_OTLP_ENDPOINT is set in the test environment, so
	// this must not error, not block, and not call log.Fatal-equivalent --
	// tracing initialization failure must degrade gracefully (design doc
	// §2's failure-isolation requirement).
	shutdown, err := InitTracing(context.Background(), "test-service")
	if err != nil {
		t.Fatalf("InitTracing returned an error: %v", err)
	}
	if shutdown == nil {
		t.Fatal("expected a non-nil shutdown function")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Errorf("shutdown returned an error: %v", err)
	}
}

// TestStripEndpointScheme guards M-1: OTEL_EXPORTER_OTLP_ENDPOINT is
// spec'd as a full URL including scheme (e.g. "http://localhost:4317"),
// but otlptracegrpc.WithEndpoint expects bare "host:port". Both the
// spec-conformant form and the bare form must end up with the same
// effective endpoint passed to the gRPC client.
func TestStripEndpointScheme(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"bare host:port unchanged", "localhost:4317", "localhost:4317"},
		{"http scheme stripped", "http://localhost:4317", "localhost:4317"},
		{"https scheme stripped", "https://localhost:4317", "localhost:4317"},
		{"http scheme with different host/port", "http://otel-collector:4317", "otel-collector:4317"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := stripEndpointScheme(tc.input); got != tc.want {
				t.Errorf("stripEndpointScheme(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestTracer_StartEndNeverPanics(t *testing.T) {
	shutdown, err := InitTracing(context.Background(), "test-service")
	if err != nil {
		t.Fatalf("InitTracing: %v", err)
	}
	defer shutdown(context.Background())

	tracer := Tracer("payment")
	_, span := tracer.Start(context.Background(), "payment.create")
	span.SetAttributes()
	span.End()
	// No panic, no error return to check -- this is the entire contract.
}
