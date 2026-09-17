package observability

import (
	"context"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// otlpEndpointEnv matches the OTel SDK's own conventional variable name,
// so no ChainRoute-specific env var is needed; unset defaults to
// "localhost:4317" (the OTel Collector's default OTLP/gRPC port).
const otlpEndpointEnv = "OTEL_EXPORTER_OTLP_ENDPOINT"

// stripEndpointScheme strips a leading "http://" or "https://" from an
// OTLP endpoint value. The OTel spec defines OTEL_EXPORTER_OTLP_ENDPOINT
// as a full URL including scheme (e.g. "http://localhost:4317"), but
// otlptracegrpc.WithEndpoint expects bare "host:port" -- passing a
// scheme-prefixed value through unmodified causes silent, permanent
// export failure (the gRPC dial target is malformed) for anyone who sets
// the env var the spec-conformant way. Any other value (including one
// with no scheme at all, the bare host:port form) passes through
// unchanged.
func stripEndpointScheme(endpoint string) string {
	endpoint = strings.TrimPrefix(endpoint, "https://")
	endpoint = strings.TrimPrefix(endpoint, "http://")
	return endpoint
}

// InitTracing configures the global OTel TracerProvider to export via
// OTLP/gRPC, with a short, non-blocking dial and export timeout so an
// absent Collector degrades to "traces silently dropped," never to a
// blocked or failed payment (design doc §2). It never returns a non-nil
// error for a reachable-but-idle exporter -- gRPC's own lazy-connect
// semantics mean Dial itself doesn't block on connectivity; failures
// surface only as dropped export attempts, which BatchSpanProcessor
// already tolerates.
func InitTracing(ctx context.Context, serviceName string) (shutdown func(context.Context) error, err error) {
	endpoint := os.Getenv(otlpEndpointEnv)
	if endpoint == "" {
		endpoint = "localhost:4317"
	}
	endpoint = stripEndpointScheme(endpoint)

	client := otlptracegrpc.NewClient(
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithTimeout(2*time.Second),
	)
	exporter, err := otlptrace.New(ctx, client)
	if err != nil {
		// Construction itself failing (malformed endpoint, etc.) still must
		// not be fatal to the caller: fall back to a no-op provider rather
		// than propagating an error that a strict caller might treat as
		// startup-fatal.
		otel.SetTracerProvider(trace.NewNoopTracerProvider())
		return func(context.Context) error { return nil }, nil
	}

	res, _ := resource.Merge(resource.Default(), resource.NewSchemaless(
		semconv.ServiceNameKey.String(serviceName),
	))

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return func(shutdownCtx context.Context) error {
		return tp.Shutdown(shutdownCtx)
	}, nil
}

// Tracer returns a tracer scoped to one ChainRoute component (e.g.
// "payment", "quote", "execution"). Safe to call whether or not
// InitTracing has run -- the global provider defaults to a no-op.
func Tracer(component string) trace.Tracer {
	return otel.Tracer("chainroute/" + component)
}
