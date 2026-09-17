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
