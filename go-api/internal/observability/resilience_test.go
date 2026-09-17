package observability

import (
	"context"
	"testing"
	"time"
)

// TestInitTracing_UnreachableEndpointStillReturnsWorkingNoOpBehavior proves
// InitTracing's non-blocking exporter configuration (otlptracegrpc's
// WithTimeout/WithInsecure plus BatchSpanProcessor -- see tracing.go) holds
// even when OTEL_EXPORTER_OTLP_ENDPOINT points at an address that will
// never answer: construction must not error, span creation/End must not
// block or panic, and shutdown must return within its own timeout. This is
// the design doc §2 guarantee ("instrumentation must not become a
// correctness dependency") for the specific case of a genuinely unreachable
// (not merely absent) Collector.
func TestInitTracing_UnreachableEndpointStillReturnsWorkingNoOpBehavior(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "10.255.255.1:4317") // non-routable, per RFC 5737-adjacent test range
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	shutdown, err := InitTracing(ctx, "test-service")
	if err != nil {
		t.Fatalf("InitTracing must not error even with an unreachable endpoint, got: %v", err)
	}

	tracer := Tracer("test")
	spanCtx, span := tracer.Start(context.Background(), "test-span")
	span.End()
	_ = spanCtx // creating and ending a span against an unreachable exporter must not block or panic

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()
	if err := shutdown(shutdownCtx); err != nil {
		t.Logf("shutdown against an unreachable endpoint returned %v (acceptable -- flush simply had nothing reachable to flush to)", err)
	}
}
