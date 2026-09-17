package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestNewLogger_EmitsJSONWithServiceField(t *testing.T) {
	var buf bytes.Buffer
	logger := newLoggerWithWriter("go-api", &buf)
	logger.Info("payment created", "payment_id", "pay-123", "provider", "across")

	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, buf.String())
	}
	if got["service"] != "go-api" {
		t.Errorf("service = %v, want go-api", got["service"])
	}
	if got["payment_id"] != "pay-123" {
		t.Errorf("payment_id = %v, want pay-123", got["payment_id"])
	}
	if got["msg"] != "payment created" {
		t.Errorf("msg = %v, want %q", got["msg"], "payment created")
	}
	if _, ok := got["time"]; !ok {
		t.Error("expected a time field")
	}
	if _, ok := got["level"]; !ok {
		t.Error("expected a level field")
	}
}

func TestNewLogger_InjectsTraceIDWhenSpanActive(t *testing.T) {
	var buf bytes.Buffer
	logger := newLoggerWithWriter("worker", &buf)

	sr := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(sr))
	ctx, span := tp.Tracer("test").Start(context.Background(), "test-span")

	logger.InfoContext(ctx, "processing payment")
	span.End()

	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	traceID, ok := got["trace_id"].(string)
	if !ok || traceID == "" {
		t.Fatalf("expected a non-empty trace_id field, got %v", got["trace_id"])
	}
}

func TestNewLogger_NoTraceIDWhenNoSpanActive(t *testing.T) {
	var buf bytes.Buffer
	logger := newLoggerWithWriter("worker", &buf)
	logger.InfoContext(context.Background(), "no span here")

	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	if _, ok := got["trace_id"]; ok {
		t.Errorf("expected no trace_id field when no span is active, got %v", got["trace_id"])
	}
}
