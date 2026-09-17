// Package observability provides ChainRoute's logging, metrics, and
// tracing primitives. Every function here is safe to call whether or not
// the underlying backend (an OTel Collector, a Prometheus scraper) is
// actually running -- observability must never become a correctness
// dependency for payment processing (Phase 10 design doc, engineering
// constraints).
package observability

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"

	"go.opentelemetry.io/otel/trace"
)

// traceContextHandler wraps a slog.Handler, adding trace_id/span_id
// fields from the active OTel span in ctx (if any) to every record. This
// is the single place trace/log correlation happens, so call sites never
// repeat trace.SpanContextFromContext boilerplate.
type traceContextHandler struct {
	slog.Handler
}

func (h traceContextHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, r)
}

func (h traceContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return traceContextHandler{h.Handler.WithAttrs(attrs)}
}

func (h traceContextHandler) WithGroup(name string) slog.Handler {
	return traceContextHandler{h.Handler.WithGroup(name)}
}

// NewLogger returns a JSON-structured logger pre-bound with a "service"
// field (e.g. "go-api", "worker"), writing to stdout. Call sites should
// prefer the *Context variants (InfoContext/ErrorContext/WarnContext) so
// trace_id is attached automatically when a span is active.
func NewLogger(service string) *slog.Logger {
	return newLoggerWithWriter(service, os.Stdout)
}

var defaultLogger = sync.OnceValue(func() *slog.Logger { return NewLogger("chainroute") })

// DefaultLogger returns a shared logger for callers that were not
// explicitly configured with one -- e.g. an existing test's struct
// literal that predates this phase and leaves Logger unset. Every
// consuming struct's private logger() accessor (see the note before
// Task 6) falls back to this rather than leaving a nil *slog.Logger
// field that would panic on first use.
func DefaultLogger() *slog.Logger {
	return defaultLogger()
}

func newLoggerWithWriter(service string, w io.Writer) *slog.Logger {
	base := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo})
	handler := traceContextHandler{base}.WithAttrs([]slog.Attr{slog.String("service", service)})
	return slog.New(handler)
}
