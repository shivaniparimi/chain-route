# Phase 10: Production Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Instrument ChainRoute's full payment lifecycle with OpenTelemetry traces, Prometheus metrics, and structured JSON logs; add a local Docker observability stack (Prometheus + OTel Collector + Jaeger + Grafana) with an auto-provisioned dashboard; add a reproducible simulated-mode throughput benchmark — all without changing routing, payment, persistence, execution, or reconciliation behavior.

**Architecture:** A new `internal/observability` package provides three independent, side-effect-isolated primitives (`Logger`, `Metrics`/`Registry`, `InitTracing`/`Tracer`) that every other package calls into but never depends on being configured. Metrics are scraped directly from `/metrics` on the Go server and a new worker metrics listener (no OTel Collector hop). Traces export via OTLP/gRPC to an OTel Collector, forwarded to Jaeger. Trace context crosses the outbox→Kafka boundary via a new `TraceCarrier` field on the existing JSON event payload — no Kafka header changes needed, since the payload already round-trips through the DB and back. The C++ router gets hand-rolled atomic counters exposed via a minimal raw-socket responder, no new C++ dependency.

**Tech Stack:** `go.opentelemetry.io/otel` + `otel/sdk/trace` + `otel/exporters/otlp/otlptrace/otlptracegrpc` + `go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp` + `go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc`; `github.com/prometheus/client_golang/prometheus` + `promhttp` + `prometheus/testutil`; stdlib `log/slog`; Docker Compose; POSIX sockets (C++, no new library).

## Global Constraints

- Do not modify `router/src/route.cpp` or the `findCheapestRoute` header signature in `router/include/chainroute/route.hpp`.
- Do not change `payment.Status`/`ExternalStatus`/`ExecutionMode` values, DB schema (no new migration), nonce allocation, idempotency, or any of the four `BLOCKCHAIN_ENV=="testnet"` gates (`cmd/server`, `cmd/worker`, `handler.PostPayments`, `worker.Processor.HandleRoutedPayment`).
- Every span-creation call site follows `ctx, span := tracer.Start(ctx, name); defer span.End()` with no branching on success — the OTel API always returns a valid (no-op if unconfigured) span/tracer, so there is nothing to check.
- The OTel exporter must be configured non-blocking (`BatchSpanProcessor`, short 2s export timeout, `otlptracegrpc.WithInsecure()`) so an absent Collector never blocks or fails a payment.
- No payment IDs, tx hashes, provider reference IDs, or wallet addresses as **Prometheus label values** (fine as span attributes / log fields). Every label value comes from a closed, fixed Go/C++ constant set.
- Never log or attach private keys, raw signed tx bytes, RPC URLs with embedded credentials, or provider API keys in logs, traces, or metrics.
- No new database migration. The benchmark's machine-readable output is a JSON file, not a DB row.
- `go-api/go.mod`'s `go 1.27.1` stays; dependencies are additive only.
- Every task that modifies an existing file must be preceded by reading that file's CURRENT content in full — the code shown in this plan reflects the repo as surveyed at plan-writing time; if it has drifted, adapt to the real current code rather than pasting this plan's snippet verbatim over a mismatch.

---

### Task 1: Structured JSON logging foundation

**Files:**
- Create: `go-api/internal/observability/logging.go`
- Test: `go-api/internal/observability/logging_test.go`

**Interfaces:**
- Produces: `observability.NewLogger(service string) *slog.Logger` — consumed by every later task that replaces a `log.Printf` call site.

- [ ] **Step 1: Write the failing test**

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd go-api && go test ./internal/observability/... -run TestNewLogger -v`
Expected: FAIL — `newLoggerWithWriter` undefined (package doesn't exist yet).

- [ ] **Step 3: Implement**

```go
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd go-api && go test ./internal/observability/... -run TestNewLogger -v`
Expected: PASS (all 3 tests).

- [ ] **Step 5: Add dependency**

```bash
cd go-api && go get go.opentelemetry.io/otel@v1.32.0 go.opentelemetry.io/otel/sdk@v1.32.0
go mod tidy
```

- [ ] **Step 6: Commit**

```bash
git add go-api/internal/observability/logging.go go-api/internal/observability/logging_test.go go-api/go.mod go-api/go.sum
git commit -m "feat(go-api): add structured JSON logging with trace correlation"
```

---

### Task 2: Prometheus metrics foundation

**Files:**
- Create: `go-api/internal/observability/metrics.go`
- Test: `go-api/internal/observability/metrics_test.go`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `observability.NewMetrics() *Metrics` and the `*Metrics` struct's exported fields (below) — consumed by Tasks 4-10.

- [ ] **Step 1: Write the failing test**

```go
package observability

import (
	"strings"
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

	var sb strings.Builder
	if err := testutil.GatherAndCompare(m.Registry, &sb); err == nil {
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd go-api && go test ./internal/observability/... -run TestMetrics -v`
Expected: FAIL — `NewMetrics`/`SanitizeProviderLabel` undefined.

- [ ] **Step 3: Implement**

```go
package observability

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// knownProviders is the closed set of bridge provider names ChainRoute
// registers today (Phase 9). SanitizeProviderLabel bounds every provider
// name that could reach a Prometheus label to this set, so an
// unregistered/future/malformed name can never create an unbounded
// number of label series (design doc §3's cardinality constraint).
var knownProviders = map[string]bool{"across": true, "relay": true}

// SanitizeProviderLabel maps any provider name not in the known set to
// "unknown". Always call this before using a provider name as a
// Prometheus label value -- never pass a raw provider string through.
func SanitizeProviderLabel(name string) string {
	if knownProviders[name] {
		return name
	}
	return "unknown"
}

// Metrics holds every ChainRoute Prometheus metric, registered against
// its own isolated Registry (never the global default registerer) so
// tests can construct independent instances without collisions and so
// production code has one explicit object to pass around rather than
// reaching for package-level globals.
type Metrics struct {
	Registry *prometheus.Registry

	// Payments
	PaymentsCreated   *prometheus.CounterVec   // labels: execution_mode
	PaymentsCompleted *prometheus.CounterVec   // labels: execution_mode
	PaymentsFailed    *prometheus.CounterVec   // labels: execution_mode, failure_reason_class
	PaymentsProcessing *prometheus.GaugeVec    // labels: execution_mode
	PaymentDuration   *prometheus.HistogramVec // labels: execution_mode, outcome

	// Routing
	RoutingRequests        *prometheus.CounterVec   // labels: execution_mode
	RoutingDuration        *prometheus.HistogramVec // labels: execution_mode
	RoutingFailures        *prometheus.CounterVec   // labels: reason
	RoutingSelectedProvider *prometheus.CounterVec  // labels: provider
	RoutingSelectedFee     *prometheus.HistogramVec // labels: provider

	// Bridge providers (Across + Relay, differentiated only by the
	// "provider" label -- never a per-provider metric family)
	QuoteRequests  *prometheus.CounterVec   // labels: provider
	QuoteFailures  *prometheus.CounterVec   // labels: provider, reason
	QuoteDuration  *prometheus.HistogramVec // labels: provider
	QuoteAvailable *prometheus.CounterVec   // labels: provider, available
	QuoteSelected  *prometheus.CounterVec   // labels: provider

	// Kafka / worker
	EventsPublished     prometheus.Counter
	EventsConsumed      prometheus.Counter
	ProcessingDuration  *prometheus.HistogramVec // labels: execution_mode
	ProcessingFailures  *prometheus.CounterVec   // labels: execution_mode, reason
	StaleRecoveries     *prometheus.CounterVec   // labels: source

	// Blockchain execution
	ExecutionAttempts       *prometheus.CounterVec   // labels: provider
	Broadcasts               *prometheus.CounterVec   // labels: provider
	Reconciliations          *prometheus.CounterVec   // labels: provider
	ExecutionsCompleted      *prometheus.CounterVec   // labels: provider
	ExecutionsFailed         *prometheus.CounterVec   // labels: provider, reason
	ExecutionDuration        *prometheus.HistogramVec // labels: provider
	ReconciliationDuration   *prometheus.HistogramVec // labels: provider
}

// NewMetrics constructs and registers every ChainRoute metric against a
// fresh, isolated *prometheus.Registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		Registry: reg,

		PaymentsCreated: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chainroute_payments_created_total", Help: "Total payments created.",
		}, []string{"execution_mode"}),
		PaymentsCompleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chainroute_payments_completed_total", Help: "Total payments reaching COMPLETED.",
		}, []string{"execution_mode"}),
		PaymentsFailed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chainroute_payments_failed_total", Help: "Total payments reaching FAILED.",
		}, []string{"execution_mode", "failure_reason_class"}),
		PaymentsProcessing: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "chainroute_payments_processing", Help: "Payments currently in PROCESSING.",
		}, []string{"execution_mode"}),
		PaymentDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "chainroute_payment_duration_seconds", Help: "Payment created-to-terminal wall time.",
			Buckets: prometheus.ExponentialBuckets(0.05, 2, 12),
		}, []string{"execution_mode", "outcome"}),

		RoutingRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chainroute_routing_requests_total", Help: "Total gRPC FindRoute calls.",
		}, []string{"execution_mode"}),
		RoutingDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "chainroute_routing_duration_seconds", Help: "Client-observed FindRoute latency.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
		}, []string{"execution_mode"}),
		RoutingFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chainroute_routing_failures_total", Help: "Total FindRoute failures.",
		}, []string{"reason"}),
		RoutingSelectedProvider: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chainroute_routing_selected_provider_total", Help: "Winning hop's bridge provider.",
		}, []string{"provider"}),
		RoutingSelectedFee: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "chainroute_routing_selected_fee_base_units", Help: "Winning hop's fee, in base units.",
			Buckets: prometheus.ExponentialBuckets(1e9, 4, 16),
		}, []string{"provider"}),

		QuoteRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chainroute_quote_requests_total", Help: "Total quote requests per provider.",
		}, []string{"provider"}),
		QuoteFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chainroute_quote_failures_total", Help: "Total quote request failures per provider.",
		}, []string{"provider", "reason"}),
		QuoteDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "chainroute_quote_duration_seconds", Help: "Quote request latency per provider.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 12),
		}, []string{"provider"}),
		QuoteAvailable: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chainroute_quote_available_total", Help: "Quote availability outcomes per provider.",
		}, []string{"provider", "available"}),
		QuoteSelected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chainroute_quote_selected_total", Help: "Times a provider's quote won route selection.",
		}, []string{"provider"}),

		EventsPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chainroute_events_published_total", Help: "Total outbox events published to Kafka.",
		}),
		EventsConsumed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chainroute_events_consumed_total", Help: "Total Kafka events consumed by the worker.",
		}),
		ProcessingDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "chainroute_processing_duration_seconds", Help: "HandleRoutedPayment wall time.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 14),
		}, []string{"execution_mode"}),
		ProcessingFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chainroute_processing_failures_total", Help: "Total HandleRoutedPayment failures.",
		}, []string{"execution_mode", "reason"}),
		StaleRecoveries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chainroute_stale_recoveries_total", Help: "Total stale-payment recoveries.",
		}, []string{"source"}),

		ExecutionAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chainroute_execution_attempts_total", Help: "Total testnet execution attempts per provider.",
		}, []string{"provider"}),
		Broadcasts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chainroute_broadcasts_total", Help: "Total transactions broadcast per provider.",
		}, []string{"provider"}),
		Reconciliations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chainroute_reconciliations_total", Help: "Total reconciliation status checks per provider.",
		}, []string{"provider"}),
		ExecutionsCompleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chainroute_executions_completed_total", Help: "Total executions reaching a completed outcome per provider.",
		}, []string{"provider"}),
		ExecutionsFailed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chainroute_executions_failed_total", Help: "Total execution failures per provider.",
		}, []string{"provider", "reason"}),
		ExecutionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "chainroute_execution_duration_seconds", Help: "Sign-through-broadcast wall time per provider.",
			Buckets: prometheus.ExponentialBuckets(0.05, 2, 12),
		}, []string{"provider"}),
		ReconciliationDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "chainroute_reconciliation_duration_seconds", Help: "checkAndUpdateOutcome wall time per provider.",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
		}, []string{"provider"}),
	}

	reg.MustRegister(
		m.PaymentsCreated, m.PaymentsCompleted, m.PaymentsFailed, m.PaymentsProcessing, m.PaymentDuration,
		m.RoutingRequests, m.RoutingDuration, m.RoutingFailures, m.RoutingSelectedProvider, m.RoutingSelectedFee,
		m.QuoteRequests, m.QuoteFailures, m.QuoteDuration, m.QuoteAvailable, m.QuoteSelected,
		m.EventsPublished, m.EventsConsumed, m.ProcessingDuration, m.ProcessingFailures, m.StaleRecoveries,
		m.ExecutionAttempts, m.Broadcasts, m.Reconciliations, m.ExecutionsCompleted, m.ExecutionsFailed,
		m.ExecutionDuration, m.ReconciliationDuration,
	)
	return m
}

var defaultMetrics = sync.OnceValue(NewMetrics)

// DefaultMetrics returns a shared Metrics instance for callers that were
// not explicitly configured with one -- e.g. an existing test's struct
// literal that predates this phase and leaves Metrics unset. Recording
// into it is always safe (it is a real, valid Registry); nothing scrapes
// it in production, since real callers (cmd/server, cmd/worker) always
// construct and thread through their own instance explicitly.
func DefaultMetrics() *Metrics {
	return defaultMetrics()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd go-api && go test ./internal/observability/... -run 'TestMetrics|TestSanitizeProviderLabel|TestDefaultMetrics' -v`
Expected: PASS (6 tests).

- [ ] **Step 5: Add dependency**

```bash
cd go-api && go get github.com/prometheus/client_golang@v1.20.5
go mod tidy
```

- [ ] **Step 6: Commit**

```bash
git add go-api/internal/observability/metrics.go go-api/internal/observability/metrics_test.go go-api/go.mod go-api/go.sum
git commit -m "feat(go-api): add Prometheus metrics catalogue with bounded provider labels"
```

---

### Task 3: OpenTelemetry tracing foundation

**Files:**
- Create: `go-api/internal/observability/tracing.go`
- Test: `go-api/internal/observability/tracing_test.go`

**Interfaces:**
- Produces: `observability.InitTracing(ctx context.Context, serviceName string) (shutdown func(context.Context) error, err error)`, `observability.Tracer(component string) trace.Tracer` — consumed by every later tracing task.

- [ ] **Step 1: Write the failing test**

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd go-api && go test ./internal/observability/... -run 'TestInitTracing|TestTracer' -v`
Expected: FAIL — `InitTracing`/`Tracer` undefined.

- [ ] **Step 3: Implement**

```go
package observability

import (
	"context"
	"os"
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd go-api && go test ./internal/observability/... -run 'TestInitTracing|TestTracer' -v`
Expected: PASS.

- [ ] **Step 5: Add dependencies**

```bash
cd go-api && go get \
  go.opentelemetry.io/otel/exporters/otlp/otlptrace@v1.32.0 \
  go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc@v1.32.0 \
  go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp@v0.57.0 \
  go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc@v0.57.0
go mod tidy
```

- [ ] **Step 6: Commit**

```bash
git add go-api/internal/observability/tracing.go go-api/internal/observability/tracing_test.go go-api/go.mod go-api/go.sum
git commit -m "feat(go-api): add OpenTelemetry tracing with non-blocking OTLP export and no-op fallback"
```

---

### Task 4: Wire `cmd/server` — HTTP tracing, `/metrics`, structured logging

**Files:**
- Modify: `go-api/cmd/server/main.go`
- Modify: `go-api/internal/grpcclient/client.go`
- Test: `go-api/internal/grpcclient/client_test.go` (extend)

**Interfaces:**
- Consumes: `observability.NewLogger`, `observability.NewMetrics`, `observability.InitTracing` (Tasks 1-3).
- Produces: `h.Metrics *observability.Metrics` field on `handler.Handler` (read the current `handler.Handler` struct in `internal/handler/routes.go` before this task — Task 6 is the one that actually populates and reads most of its fields, but this task is what threads the `*observability.Metrics` instance from `main()` into the `Handler{}` literal).

- [ ] **Step 1: Add the otelgrpc client dial option**

Read `go-api/internal/grpcclient/client.go` in full first (11 lines shown in the design survey; confirm it still matches). Modify `Dial`:

```go
package grpcclient

import (
	"context"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	routingv1 "chainroute/go-api/internal/gen/chainroute/v1"
)

const requestTimeout = 2 * time.Second

type Client struct {
	conn   *grpc.ClientConn
	client routingv1.RoutingServiceClient
}

func Dial(address string) (*Client, error) {
	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, client: routingv1.NewRoutingServiceClient(conn)}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) FindRoute(ctx context.Context, req *routingv1.FindRouteRequest) (*routingv1.FindRouteResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	return c.client.FindRoute(ctx, req)
}
```

Add a test asserting `Dial` still succeeds and produces a working client (the existing `TestDialAndClose` in `client_test.go` already exercises this — confirm it still passes unmodified; no new test is needed purely for the stats handler, since `otelgrpc.NewClientHandler()` returning a valid non-nil handler is exercised transitively by `Dial` succeeding at all).

- [ ] **Step 2: Wire tracing/metrics/logging into `cmd/server/main.go`**

Read the current file in full (shown in Task setup survey above) before editing. Replace the body with the same structure, adding: `observability.InitTracing` at the top (with deferred shutdown), `observability.NewLogger("go-api")` replacing every `log.Printf`/`log.Fatalf` in this file, `observability.NewMetrics()` threaded into `handler.Handler{Metrics: metrics}`, `otelhttp.NewHandler` wrapping `mux`, and a `GET /metrics` route:

```go
package main

import (
	"context"
	"database/sql"
	"flag"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"chainroute/go-api/internal/bridge/across"
	"chainroute/go-api/internal/bridge/quote"
	"chainroute/go-api/internal/bridge/relay"
	"chainroute/go-api/internal/grpcclient"
	"chainroute/go-api/internal/handler"
	"chainroute/go-api/internal/observability"
	"chainroute/go-api/internal/postgres"
)

func main() {
	httpAddr := flag.String("http-addr", ":8080", "HTTP listen address")
	grpcAddr := flag.String("grpc-addr", "127.0.0.1:50051", "gRPC routing service address")
	flag.Parse()

	logger := observability.NewLogger("go-api")

	shutdownTracing, err := observability.InitTracing(context.Background(), "go-api")
	if err != nil {
		logger.Warn("tracing initialization failed, continuing without traces", "error", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := shutdownTracing(shutdownCtx); err != nil {
			logger.Warn("tracing shutdown error", "error", err)
		}
	}()

	metrics := observability.NewMetrics()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		logger.Error("DATABASE_URL environment variable is required")
		os.Exit(1)
	}

	blockchainEnv := os.Getenv("BLOCKCHAIN_ENV")
	maxTestnetAmountWei := envBigIntServer("MAX_TESTNET_AMOUNT_WEI", big.NewInt(10_000_000_000_000_000), logger)

	var registry *quote.Registry
	if blockchainEnv == "testnet" {
		acrossBaseURL := envOrDefaultServer("ACROSS_TESTNET_API_URL", "https://testnet.across.to/api")
		relayBaseURL := envOrDefaultServer("RELAY_TESTNET_API_URL", "https://api.testnets.relay.link")
		routingQuoteTTL := envDurationServer("ROUTING_QUOTE_TTL_SECONDS", 2*time.Minute, time.Second, logger)
		acrossClient := across.NewClient(acrossBaseURL)
		acrossClient.APIKey = os.Getenv("ACROSS_API_KEY")
		acrossClient.IntegratorID = os.Getenv("ACROSS_INTEGRATOR_ID")
		relayClient := relay.NewClient(relayBaseURL)
		relayClient.APIKey = os.Getenv("RELAY_API_KEY")

		var zeroWallet common.Address
		registry = quote.NewRegistry()
		routeKey := quote.RouteKey{SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH"}
		registry.Register(routeKey, across.NewProvider(acrossClient, routingQuoteTTL))
		registry.Register(routeKey, relay.NewProvider(relayClient, zeroWallet, routingQuoteTTL))
	}

	client, err := grpcclient.Dial(*grpcAddr)
	if err != nil {
		logger.Error("failed to dial routing service", "grpc_addr", *grpcAddr, "error", err)
		os.Exit(1)
	}
	defer client.Close()

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		logger.Error("failed to open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	db.SetMaxOpenConns(10)

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := db.PingContext(pingCtx); err != nil {
		pingCancel()
		logger.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	pingCancel()

	store := postgres.New(db)

	h := &handler.Handler{
		Client: client, Store: store, BlockchainEnv: blockchainEnv,
		MaxTestnetAmountWei: maxTestnetAmountWei, QuoteRegistry: registry,
		Metrics: metrics, Logger: logger,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /routes", h.PostRoutes)
	mux.HandleFunc("POST /payments", h.PostPayments)
	mux.HandleFunc("GET /payments/{id}", h.GetPayment)
	mux.Handle("GET /metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))

	instrumentedMux := otelhttp.NewHandler(mux, "http.server")
	server := &http.Server{Addr: *httpAddr, Handler: instrumentedMux}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("go-api listening", "http_addr", *httpAddr, "grpc_addr", *grpcAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Warn("graceful shutdown error", "error", err)
	}
	logger.Info("go-api shut down")
}

func envBigIntServer(key string, def *big.Int, logger *slog.Logger) *big.Int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, ok := new(big.Int).SetString(v, 10)
	if !ok {
		logger.Error("invalid environment variable: not a valid base-10 integer", "key", key)
		os.Exit(1)
	}
	return n
}

func envOrDefaultServer(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDurationServer(key string, def time.Duration, unit time.Duration, logger *slog.Logger) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		logger.Error("invalid environment variable", "key", key, "error", err)
		os.Exit(1)
	}
	return time.Duration(n) * unit
}
```

Add `"log/slog"` to the imports. Note `handler.Metrics`/`handler.Logger` fields don't exist on `Handler` yet — Task 6 adds them; this task's own build will not compile in isolation on that point alone, which is acceptable **only if Task 6 is applied in the same PR/commit sequence before running the full build** — to keep this task independently buildable, add the two fields to `Handler` (in `internal/handler/routes.go`, alongside the existing `Client`/`Store`/etc. fields) as an empty-bodied stub addition in THIS task:

```go
// Added fields on the existing Handler struct in internal/handler/routes.go:
Metrics *observability.Metrics
Logger  *slog.Logger
```

(Task 6 is the one that actually reads and uses `h.Metrics`/`h.Logger` inside `PostPayments`/`GetPayment`/`PostRoutes`; this task only adds the fields and threads a real value into them so `cmd/server` compiles end-to-end on its own.)

- [ ] **Step 3: Build and test**

```bash
cd go-api && go build ./... && go vet ./... && go test ./internal/grpcclient/... ./internal/observability/... -v
```

Expected: clean build (note: `handler.Handler{Metrics:, Logger:}` fields now exist but are unused inside `handler` package itself until Task 6 — `go vet` does not flag unused struct fields, only unused local variables, so this is not an error).

- [ ] **Step 4: Commit**

```bash
git add go-api/cmd/server/main.go go-api/internal/grpcclient/client.go go-api/internal/handler/routes.go
git commit -m "feat(go-api): wire tracing/metrics/structured-logging into cmd/server"
```

---

### Task 5: Wire `cmd/worker` — metrics endpoint, tracing, structured logging

**Files:**
- Modify: `go-api/cmd/worker/main.go`

**Interfaces:**
- Consumes: `observability.NewLogger`, `observability.NewMetrics`, `observability.InitTracing` (Tasks 1-3).
- Produces: a `*observability.Metrics` instance and a `*slog.Logger` instance, threaded into `worker.Publisher`, `worker.Processor`, `worker.Executor`, `worker.Reconciler`, `worker.Recovery` structs (added as new optional fields by Tasks 8-10; this task only starts the metrics HTTP server and constructs the shared `metrics`/`logger` values `cmd/worker` will pass to them).

- [ ] **Step 1: Read the current file in full**

Read `go-api/cmd/worker/main.go` (328 lines, per the design survey) before making any change — this task must not guess at its exact goroutine-startup sequence, env var handling, or the typed-nil-interface guard at its `Executor`/`Reconciler` construction.

- [ ] **Step 2: Add the metrics HTTP server and observability init**

Near the top of `main()`, after parsing env vars but before constructing `Publisher`/`Recovery`/etc., add:

```go
logger := observability.NewLogger("worker")

shutdownTracing, err := observability.InitTracing(context.Background(), "worker")
if err != nil {
	logger.Warn("tracing initialization failed, continuing without traces", "error", err)
}
defer func() {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := shutdownTracing(shutdownCtx); err != nil {
		logger.Warn("tracing shutdown error", "error", err)
	}
}()

metrics := observability.NewMetrics()

metricsAddr := envOrDefault("METRICS_ADDR", ":9091")
metricsMux := http.NewServeMux()
metricsMux.Handle("GET /metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))
metricsServer := &http.Server{Addr: metricsAddr, Handler: metricsMux}
```

Then add one more goroutine to the existing `sync.WaitGroup` (find the exact `wg.Add`/`go func()` block that starts `publisher.Run`/`recovery.Run`/the consume loop, and add a sibling entry following the identical pattern):

```go
wg.Add(1)
go func() {
	defer wg.Done()
	logger.Info("metrics endpoint listening", "addr", metricsAddr)
	if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("metrics HTTP server error", "error", err)
	}
}()
```

And in the existing shutdown sequence (wherever the file currently handles `ctx.Done()`/signal shutdown before `wg.Wait()`), add a call to shut the metrics server down alongside whatever else is already stopped there:

```go
shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := metricsServer.Shutdown(shutdownCtx); err != nil {
	logger.Warn("metrics server shutdown error", "error", err)
}
```

Add `"net/http"`, `"github.com/prometheus/client_golang/prometheus/promhttp"`, and `"chainroute/go-api/internal/observability"` to imports; add a local `envOrDefault(key, def string) string` helper identical in shape to `cmd/server`'s (or reuse if one already exists in this file under a different name — check first).

- [ ] **Step 3: Replace this file's own `log.Printf`/`log.Fatalf` calls with `logger`**

Every remaining `log.Printf`/`log.Fatalf`/`log.Println` call site in `cmd/worker/main.go` itself (not the `worker` package's own files — those are Tasks 8-10) becomes `logger.Info(...)`/`logger.Error(...)` + `os.Exit(1)` where the original was `Fatal`. Follow the exact same conversion pattern as Task 4's `cmd/server/main.go` edit.

- [ ] **Step 4: Build and test**

```bash
cd go-api && go build ./... && go vet ./...
```

Expected: clean build.

- [ ] **Step 5: Commit**

```bash
git add go-api/cmd/worker/main.go
git commit -m "feat(go-api): add worker metrics endpoint and structured logging/tracing init"
```

---

### Convention required from Task 6 onward: nil-safe `metrics()`/`logger()` accessors

`handler.Handler`, `worker.Publisher`, `worker.Processor`, `worker.Recovery`, and `worker.Reconciler` are each constructed as **inline struct literals in dozens of existing tests** (45 in the handler package alone, verified at plan-writing time), not through one shared test helper — unlike `worker.Executor`, which already funnels through `newTestExecutor`. If `Metrics *observability.Metrics`/`Logger *slog.Logger` fields are added and then dereferenced directly (`h.Metrics.PaymentsCreated...`, `h.Logger.InfoContext(...)`), every one of those pre-existing literals that doesn't set the new fields will nil-panic the instant its handler/method runs — silently breaking dozens of Phase 1-9 tests that have nothing to do with this phase.

**Every task from here on (6, 8, 9, 10) must therefore add a small private accessor method on each instrumented struct, and call the accessor, never the raw field, from within that struct's own methods:**

```go
// Example for Handler (add identically-shaped metrics()/logger() methods
// on Publisher, Processor, Recovery, Reconciler in Tasks 8-10):
func (h *Handler) metrics() *observability.Metrics {
	if h.Metrics != nil {
		return h.Metrics
	}
	return observability.DefaultMetrics()
}

func (h *Handler) logger() *slog.Logger {
	if h.Logger != nil {
		return h.Logger
	}
	return observability.DefaultLogger()
}
```

Wherever a code block below this point writes `h.Metrics.X`/`h.Logger.Y` (or `e.Metrics...`, `r.Metrics...`, `p.Metrics...`, etc.), read it as `h.metrics().X`/`h.logger().Y` — the accessor form is the actual requirement; the field-access form in the code blocks below is written that way only for readability. This is not optional and is not a style preference: it is what makes "observability being unavailable does not break payment processing" (a required test in Task 11) true by construction, for every existing AND future caller, rather than true only for callers that remembered to set the fields.

---

### Task 6: Instrument `handler.PostPayments`/`GetPayment`/`PostRoutes`

**Files:**
- Modify: `go-api/internal/handler/payments.go`
- Modify: `go-api/internal/handler/routes.go`
- Test: `go-api/internal/handler/payments_test.go` (extend)

**Interfaces:**
- Consumes: `h.Metrics *observability.Metrics`, `h.Logger *slog.Logger` (Task 4's field additions); `observability.Tracer`, `observability.SanitizeProviderLabel`.
- Produces: nothing new consumed by later tasks (this is a leaf instrumentation task) except the general pattern Tasks 9-10 mirror.

- [ ] **Step 1: Write the failing tests**

Add to `payments_test.go` (follow the existing fake-store/fake-provider patterns already in this file):

```go
func TestPostPayments_SimulatedMode_IncrementsPaymentsCreatedAndProcessing(t *testing.T) {
	h, metrics := newTestHandlerWithMetrics(t) // helper added below
	req := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(
		`{"source_chain":"ethereum","destination_chain":"base","asset":"usdc","amount":"100"}`))
	req.Header.Set("Idempotency-Key", t.Name())
	w := httptest.NewRecorder()
	h.PostPayments(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201, body: %s", w.Code, w.Body.String())
	}
	if got := testutil.ToFloat64(metrics.PaymentsCreated.WithLabelValues("simulated")); got != 1 {
		t.Errorf("PaymentsCreated{simulated} = %v, want 1", got)
	}
}

func TestPostPayments_TestnetMode_RecordsQuoteMetricsWithBoundedProviderLabels(t *testing.T) {
	h, metrics := newTestHandlerWithMetricsAndProviders(t,
		map[string]quote.Provider{
			"across": &fakeQuoteProvider{name: "across", q: quote.Quote{ProviderName: "across", Available: true, FeeBaseUnits: big.NewInt(100), OutputAmountBaseUnits: big.NewInt(900), InputAmountBaseUnits: big.NewInt(1000)}},
			"relay":  &fakeQuoteProvider{name: "relay", q: quote.Quote{ProviderName: "relay", Available: true, FeeBaseUnits: big.NewInt(50), OutputAmountBaseUnits: big.NewInt(950), InputAmountBaseUnits: big.NewInt(1000)}},
		})
	req := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(
		`{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`))
	req.Header.Set("Idempotency-Key", t.Name())
	w := httptest.NewRecorder()
	h.PostPayments(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201, body: %s", w.Code, w.Body.String())
	}
	for _, provider := range []string{"across", "relay"} {
		if got := testutil.ToFloat64(metrics.QuoteRequests.WithLabelValues(provider)); got != 1 {
			t.Errorf("QuoteRequests{%s} = %v, want 1", provider, got)
		}
	}
	// relay's fee (50) beats across's (100), so relay must be the one
	// credited with the win -- proves the metric reflects the ACTUAL
	// C++-selected winner, not just "whichever provider happened first."
	if got := testutil.ToFloat64(metrics.RoutingSelectedProvider.WithLabelValues("relay")); got != 1 {
		t.Errorf("RoutingSelectedProvider{relay} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.RoutingSelectedProvider.WithLabelValues("across")); got != 0 {
		t.Errorf("RoutingSelectedProvider{across} = %v, want 0 (across did not win)", got)
	}
}

func TestPostPayments_QuoteProviderFailure_RecordsFailureMetricWithBoundedReason(t *testing.T) {
	h, metrics := newTestHandlerWithMetricsAndProviders(t,
		map[string]quote.Provider{
			"across": &fakeQuoteProvider{name: "across", err: errors.New("connection refused")},
		})
	req := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(
		`{"source_chain":"ethereum","destination_chain":"base","asset":"eth","amount":"0.001","execution_mode":"testnet"}`))
	req.Header.Set("Idempotency-Key", t.Name())
	w := httptest.NewRecorder()
	h.PostPayments(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
	if got := testutil.ToFloat64(metrics.QuoteFailures.WithLabelValues("across", "http_error")); got != 1 {
		t.Errorf("QuoteFailures{across,http_error} = %v, want 1", got)
	}
}
```

Add the two test helpers (`newTestHandlerWithMetrics`, `newTestHandlerWithMetricsAndProviders`) near the file's other existing `newTestHandler`-style helpers, following their exact construction pattern but additionally setting `Metrics: observability.NewMetrics()` and returning that `*observability.Metrics` alongside the `*Handler`. Import `"github.com/prometheus/client_golang/prometheus/testutil"`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd go-api && go test ./internal/handler/... -run 'TestPostPayments_.*Metrics|TestPostPayments_QuoteProviderFailure' -v`
Expected: FAIL — `h.Metrics` unused/nil, no increments happen yet.

- [ ] **Step 3: Add the nil-safe accessors**

In `routes.go` (where the `Handler` struct itself is defined), add:

```go
func (h *Handler) metrics() *observability.Metrics {
	if h.Metrics != nil {
		return h.Metrics
	}
	return observability.DefaultMetrics()
}

func (h *Handler) logger() *slog.Logger {
	if h.Logger != nil {
		return h.Logger
	}
	return observability.DefaultLogger()
}
```

Every instrumentation call added in Steps 4-5 below uses `h.metrics()`/`h.logger()`, never `h.Metrics`/`h.Logger` directly.

- [ ] **Step 4: Instrument `payments.go`**

Read the current file in full (409 lines, shown above) before editing. Apply these additions in place (do not restructure the existing control flow — every early `return` after `writeError` stays exactly where it is; instrumentation is added alongside, never replacing, a control-flow branch):

At the top of `PostPayments`, wrap the whole body in a span and start the payment-processing gauge:

```go
func (h *Handler) PostPayments(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.Tracer("payment").Start(r.Context(), "payment.create")
	defer span.End()
	r = r.WithContext(ctx)

	// ... existing idempotency-key / JSON decode / chain-asset-amount
	// validation exactly as before, unchanged ...
```

Just before the `mode := payment.ExecutionModeSimulated` line, no change needed there. After `mode` is finalized and validated (right after the existing `if mode != payment.ExecutionModeSimulated && mode != payment.ExecutionModeTestnet` block), add:

```go
	span.SetAttributes(
		attribute.String("payment.source_chain", req.SourceChain),
		attribute.String("payment.destination_chain", req.DestinationChain),
		attribute.String("payment.asset", req.Asset),
		attribute.String("payment.execution_mode", string(mode)),
	)
```

Inside the `if mode == payment.ExecutionModeTestnet { ... }` block, wrap the quote fan-out (the existing `sync.WaitGroup` section) in an aggregation span, and wrap each provider's goroutine body in its own quote span + metrics:

```go
		quoteCtx, quoteCancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer quoteCancel()
		aggCtx, aggSpan := observability.Tracer("quote").Start(quoteCtx, "quote.aggregate")
		results := make([]quoteResult, len(providers))
		var wg sync.WaitGroup
		for i, p := range providers {
			wg.Add(1)
			go func(i int, p quote.Provider) {
				defer wg.Done()
				providerLabel := observability.SanitizeProviderLabel(p.Name())
				spanCtx, qSpan := observability.Tracer("quote").Start(aggCtx, "quote."+providerLabel+".get")
				defer qSpan.End()
				h.Metrics.QuoteRequests.WithLabelValues(providerLabel).Inc()
				start := time.Now()
				q, err := p.GetQuote(spanCtx, quote.Request{
					SourceChainID: originChainID, DestinationChainID: destChainID, Asset: bridgedAsset, AmountBaseUnits: amountWei,
				})
				h.Metrics.QuoteDuration.WithLabelValues(providerLabel).Observe(time.Since(start).Seconds())
				if err != nil {
					h.Metrics.QuoteFailures.WithLabelValues(providerLabel, "http_error").Inc()
				} else {
					available := "false"
					if q.Available {
						available = "true"
					}
					h.Metrics.QuoteAvailable.WithLabelValues(providerLabel, available).Inc()
				}
				results[i] = quoteResult{provider: p, q: q, err: err}
			}(i, p)
		}
		wg.Wait()
		aggSpan.End()
```

Replace the existing `log.Printf("WARNING: quote provider %s failed: %v", ...)` inside the aggregation loop with `h.Logger.WarnContext(r.Context(), "quote provider failed", "provider", res.provider.Name(), "error", res.err)`.

After the `grpcReq`/`h.Client.FindRoute` call, wrap it with a routing span + metrics (replacing the plain call, not restructuring the surrounding error-handling switch):

```go
	routingCtx, routingSpan := observability.Tracer("routing").Start(r.Context(), "routing.find")
	h.Metrics.RoutingRequests.WithLabelValues(string(mode)).Inc()
	routingStart := time.Now()
	resp, err := h.Client.FindRoute(routingCtx, grpcReq)
	h.Metrics.RoutingDuration.WithLabelValues(string(mode)).Observe(time.Since(routingStart).Seconds())
	routingSpan.End()
	if err != nil {
		st, _ := status.FromError(err)
		switch st.Code() {
		case codes.InvalidArgument:
			h.Metrics.RoutingFailures.WithLabelValues("invalid_request").Inc()
			h.Logger.WarnContext(r.Context(), "routing service rejected a request that passed Go validation", "error", st.Message())
			writeError(w, http.StatusBadRequest, st.Message())
		case codes.Unavailable:
			h.Metrics.RoutingFailures.WithLabelValues("grpc_error").Inc()
			writeError(w, http.StatusServiceUnavailable, "routing service unavailable")
		case codes.DeadlineExceeded:
			h.Metrics.RoutingFailures.WithLabelValues("grpc_error").Inc()
			writeError(w, http.StatusGatewayTimeout, "routing service timed out")
		default:
			h.Metrics.RoutingFailures.WithLabelValues("grpc_error").Inc()
			h.Logger.ErrorContext(r.Context(), "routing service call failed", "error", err)
			writeError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}
	if !resp.GetRouteFound() {
		h.Metrics.RoutingFailures.WithLabelValues("no_route").Inc()
		writeError(w, http.StatusUnprocessableEntity, "no route available for the requested payment")
		return
	}
```

At the winning-hop determination block (`if mode == payment.ExecutionModeTestnet && len(hops) > 0 { winningQuote, ok := ... }`), after `provider := winningQuote.ProviderName; bridgeProvider = &provider`, add:

```go
		providerLabel := observability.SanitizeProviderLabel(provider)
		h.Metrics.RoutingSelectedProvider.WithLabelValues(providerLabel).Inc()
		h.Metrics.QuoteSelected.WithLabelValues(providerLabel).Inc()
		h.Metrics.RoutingSelectedFee.WithLabelValues(providerLabel).Observe(float64(winningQuote.FeeBaseUnits.Int64()))
		span.SetAttributes(attribute.String("payment.provider", providerLabel))
```

Just before the final `h.Store.CreateOrGetPayment` call, and after it succeeds with outcome `payment.Created`, add the created counter and the processing gauge increment (a payment starts "processing" the instant it's durably ROUTED, since that's what makes it eligible for the worker to claim):

```go
	result, outcome, err := h.Store.CreateOrGetPayment(r.Context(), candidate)
	if err != nil {
		h.Logger.ErrorContext(r.Context(), "failed to persist payment", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if outcome == payment.Created {
		h.Metrics.PaymentsCreated.WithLabelValues(string(mode)).Inc()
		h.Metrics.PaymentsProcessing.WithLabelValues(string(mode)).Inc()
		span.SetAttributes(attribute.String("payment.id", result.ID), attribute.String("payment.status", string(result.Status)))
	}
```

Replace the remaining `log.Printf("ERROR: ...")` calls in this file (idempotency lookup failure, persist failure) with `h.Logger.ErrorContext(r.Context(), ...)` equivalents. Replace `GetPayment`'s two `log.Printf` calls similarly using `h.Logger.ErrorContext(r.Context(), ...)`.

Add imports: `"go.opentelemetry.io/otel/attribute"`, `"chainroute/go-api/internal/observability"`. Remove the now-unused `"log"` import if nothing else in the file still uses it (check `routes.go` separately — it has its own `log.Printf` calls, converted next).

- [ ] **Step 5: Instrument `routes.go`**

Read the current file (181 lines) in full. Replace its two `log.Printf` calls with `h.logger().WarnContext(r.Context(), ...)`/`h.logger().ErrorContext(r.Context(), ...)`. `PostRoutes` does not need a payment-lifecycle span (it's a pure routing preview endpoint, not part of the payment lifecycle this phase instruments) — leave its control flow otherwise untouched; `otelhttp.NewHandler` at the mux level (Task 4) already gives it an automatic `http.server` span.

- [ ] **Step 6: Run tests to verify they pass**

Run: `cd go-api && go test ./internal/handler/... -v`
Expected: PASS — every existing test plus the 3 new ones.

- [ ] **Step 7: Full package regression**

```bash
cd go-api && go build ./... && go vet ./... && go test ./...
```

Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add go-api/internal/handler/payments.go go-api/internal/handler/routes.go go-api/internal/handler/payments_test.go
git commit -m "feat(go-api): instrument PostPayments with quote/routing spans and Prometheus metrics"
```

---

### Task 7: Postgres persistence spans + outbox trace-context propagation + payment-duration wiring

**Files:**
- Modify: `go-api/internal/postgres/store.go`
- Modify: `go-api/internal/postgres/execution_store.go`
- Modify: `go-api/internal/events/routed_payment.go`
- Modify: `go-api/internal/worker/processor.go`
- Modify: `go-api/internal/worker/recovery.go`
- Modify: `go-api/internal/worker/reconciler.go`
- Test: `go-api/internal/postgres/store_test.go` / `execution_store_integration_test.go` (extend)
- Test: `go-api/internal/worker/processor_test.go`, `recovery_test.go` (extend for the new signatures)

**Interfaces:**
- Consumes: `observability.Tracer` (Task 3).
- Produces: `events.RoutedPayment.TraceCarrier map[string]string`; `Store.CompletePayment`/`Store.CompleteSubmittedPayment` new signature `(completed bool, createdAt time.Time, err error)` — consumed by Tasks 8, 9, 10.

This task combines two things deliberately: it is the ONE place the `payments` table's `created_at` needs to be read back out for the payment-duration histogram, and every completion call site needs updating together or the codebase won't compile in between — splitting it further would leave a broken intermediate state.

- [ ] **Step 1: Add `TraceCarrier` to the event payload**

```go
// go-api/internal/events/routed_payment.go
package events

import "time"

const RoutedPaymentEventType = "PAYMENT_ROUTED"

// RoutedPayment is the thin Kafka payload for a routed payment. It carries
// only payment_id plus metadata -- PostgreSQL remains the source of truth,
// and the consumer always re-reads current state before acting.
//
// TraceCarrier holds the OTel trace context active when the outbox row
// was created (propagation.MapCarrier's serialized form), so the worker's
// consume loop can extract it and continue the SAME trace across the
// Kafka boundary, rather than starting an unrelated one. It is empty when
// tracing is unconfigured -- Extract on an empty map is always safe.
type RoutedPayment struct {
	PaymentID    string            `json:"payment_id"`
	EventType    string            `json:"event_type"`
	OccurredAt   time.Time         `json:"occurred_at"`
	TraceCarrier map[string]string `json:"trace_carrier,omitempty"`
}
```

- [ ] **Step 2: Inject the trace context at outbox-insert time**

Read `go-api/internal/postgres/store.go` lines ~200-270 in full first (the exact surrounding transaction code, shown above). Change only the `events.RoutedPayment{...}` construction:

```go
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	outboxPayload, err := json.Marshal(events.RoutedPayment{
		PaymentID:    created.ID,
		EventType:    events.RoutedPaymentEventType,
		OccurredAt:   time.Now().UTC(),
		TraceCarrier: carrier,
	})
```

Add `"go.opentelemetry.io/otel"` and `"go.opentelemetry.io/otel/propagation"` to imports.

- [ ] **Step 3: Add `db.<method>` spans around the store's own DB calls**

Wrap the bodies of `CreateOrGetPayment`, `GetPayment`, `TryCreateExecution`, `PersistSignedExecution` (the four the design doc names) with a span at each function's top:

```go
func (s *Store) CreateOrGetPayment(ctx context.Context, p payment.Payment) (payment.Payment, payment.CreateResult, error) {
	ctx, span := observability.Tracer("db").Start(ctx, "db.CreateOrGetPayment")
	defer span.End()
	// ... existing body, unchanged, using this ctx ...
```

Apply the identical one-line-at-top-plus-defer pattern to `GetPayment` (store.go) and to `TryCreateExecution`/`PersistSignedExecution` (execution_store.go) — read each function's current signature first; the pattern is mechanical and must not touch anything past the first two lines of each function body. Add `"chainroute/go-api/internal/observability"` to both files' imports.

- [ ] **Step 4: Extend `CompletePayment`/`CompleteSubmittedPayment` to return `created_at`**

Read the current `CompletePayment` (store.go) and `CompleteSubmittedPayment` (execution_store.go) in full (shown above) before editing. Change both from `ExecContext` to `QueryRowContext` with `RETURNING created_at`, and widen the signature:

```go
// store.go
func (s *Store) CompletePayment(ctx context.Context, paymentID string, terminal payment.Status) (completed bool, createdAt time.Time, err error) {
	row := s.db.QueryRowContext(ctx, `
		UPDATE payments SET status = $2, completed_at = now(), updated_at = now()
		WHERE id = $1 AND status = $3
		RETURNING created_at
	`, paymentID, terminal, payment.StatusProcessing)
	if err := row.Scan(&createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, time.Time{}, nil
		}
		return false, time.Time{}, fmt.Errorf("complete payment: %w", err)
	}
	return true, createdAt, nil
}
```

```go
// execution_store.go
func (s *Store) CompleteSubmittedPayment(ctx context.Context, paymentID string, terminal payment.Status) (completed bool, createdAt time.Time, err error) {
	row := s.db.QueryRowContext(ctx, `
		UPDATE payments SET status = $2, completed_at = now(), updated_at = now()
		WHERE id = $1 AND status = $3
		RETURNING created_at
	`, paymentID, terminal, payment.StatusSubmitted)
	if err := row.Scan(&createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, time.Time{}, nil
		}
		return false, time.Time{}, fmt.Errorf("complete submitted payment: %w", err)
	}
	return true, createdAt, nil
}
```

(`sql.ErrNoRows` from `QueryRowContext`/`Scan` is the direct replacement for the old `RowsAffected() == 0` check — a `WHERE` clause matching zero rows makes `RETURNING` produce zero rows, which surfaces as `sql.ErrNoRows` on `Scan`, not as a scan error, so this preserves the exact "no matching row = completed=false, not an error" semantics.)

- [ ] **Step 5: Update every caller and every fake/mock implementing the old signature**

Update, in order, checking each compiles before moving to the next:
1. `worker/processor.go`'s `PaymentStore` interface: `CompletePayment(ctx context.Context, paymentID string, terminal payment.Status) (bool, time.Time, error)`.
2. `worker/processor.go`'s `HandleRoutedPayment`: `if _, createdAt, err := p.Store.CompletePayment(ctx, evt.PaymentID, terminal); err != nil { ... }` — capture `createdAt` (Task 8 uses it; this task just threads the plumbing so nothing breaks).
3. `worker/recovery.go`'s `RecoveryStore` interface and `SweepOnce`'s call site: same signature widening, same capture-and-ignore-for-now pattern.
4. `worker/reconciler.go`'s `ReconcilerStore` interface (`CompleteSubmittedPayment` entry) and `markTerminal`'s call site: same pattern.
5. Every test file with a fake implementing `PaymentStore`/`RecoveryStore`/`ReconcilerStore` (`processor_test.go`, `recovery_test.go`, `reconciler_test.go`, and any integration test file) — find every fake's `CompletePayment`/`CompleteSubmittedPayment` method and widen its signature and return statement to match (e.g. `return true, time.Now(), nil` where it previously returned `true, nil`, or thread a configurable `createdAt` field on the fake if a test specifically needs to control the duration measured — check each test's existing assertions to judge which is needed).

- [ ] **Step 6: Run tests to verify everything still passes**

```bash
cd go-api && go build ./... && go vet ./... && go test ./...
export DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable"
go test -tags=integration ./...
```

Expected: clean (no behavior change yet from `createdAt` — Task 8 is what actually observes it into a histogram).

- [ ] **Step 7: Commit**

```bash
git add go-api/internal/postgres/store.go go-api/internal/postgres/execution_store.go go-api/internal/events/routed_payment.go go-api/internal/worker/processor.go go-api/internal/worker/recovery.go go-api/internal/worker/reconciler.go go-api/internal/postgres/*_test.go go-api/internal/worker/*_test.go
git commit -m "feat(go-api): propagate trace context through the outbox, add db spans, extend completion signatures to return created_at"
```

---

### Task 8: Kafka/worker processing spans + metrics + payment completion metrics

**Files:**
- Modify: `go-api/internal/worker/publisher.go`
- Modify: `go-api/internal/worker/processor.go`
- Modify: `go-api/internal/worker/recovery.go`
- Modify: `go-api/cmd/worker/main.go`
- Test: `go-api/internal/worker/publisher_test.go`, `processor_test.go`, `recovery_test.go` (extend)

**Interfaces:**
- Consumes: `Store.CompletePayment` new signature, `events.RoutedPayment.TraceCarrier` (Task 7); `observability.Tracer`/`Metrics`/`SanitizeProviderLabel` (Tasks 1-3).
- Produces: `Publisher.Metrics`, `Processor.Metrics`, `Processor.Logger`, `Recovery.Metrics` fields — consumed by Task 5's wiring (already done structurally; this task is what those fields actually get used for).

- [ ] **Step 1: Write the failing tests**

```go
// publisher_test.go addition
func TestPublisher_PollOnce_IncrementsEventsPublished(t *testing.T) {
	metrics := observability.NewMetrics()
	store := &fakeOutboxStore{hasEvent: true, evt: postgres.OutboxEvent{PaymentID: "pay-1", Payload: []byte(`{}`)}}
	p := &Publisher{Store: store, Publish: func(context.Context, string, []byte) error { return nil }, Metrics: metrics}

	if _, err := p.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}
	if got := testutil.ToFloat64(metrics.EventsPublished); got != 1 {
		t.Errorf("EventsPublished = %v, want 1", got)
	}
}
```

```go
// processor_test.go addition
func TestHandleRoutedPayment_SimulatedMode_RecordsProcessingMetricsAndPaymentDuration(t *testing.T) {
	metrics := observability.NewMetrics()
	store := &fakeProcessorStore{claimed: true, mode: payment.ExecutionModeSimulated, completeCreatedAt: time.Now().Add(-2 * time.Second)}
	proc := &Processor{Store: store, Metrics: metrics, Logger: observability.NewLogger("test")}

	if err := proc.HandleRoutedPayment(context.Background(), events.RoutedPayment{PaymentID: "pay-1"}); err != nil {
		t.Fatalf("HandleRoutedPayment: %v", err)
	}
	if got := testutil.ToFloat64(metrics.EventsConsumed); got != 1 {
		t.Errorf("EventsConsumed = %v, want 1", got)
	}
	if got := testutil.CollectAndCount(metrics.PaymentDuration); got == 0 {
		t.Error("expected at least one PaymentDuration observation")
	}
	if got := testutil.CollectAndCount(metrics.ProcessingDuration); got == 0 {
		t.Error("expected at least one ProcessingDuration observation")
	}
}

func TestHandleRoutedPayment_ClaimFailure_RecordsProcessingFailure(t *testing.T) {
	metrics := observability.NewMetrics()
	store := &fakeProcessorStore{claimErr: errors.New("db down")}
	proc := &Processor{Store: store, Metrics: metrics, Logger: observability.NewLogger("test")}

	if err := proc.HandleRoutedPayment(context.Background(), events.RoutedPayment{PaymentID: "pay-1"}); err == nil {
		t.Fatal("expected an error")
	}
	if got := testutil.ToFloat64(metrics.ProcessingFailures.WithLabelValues("simulated", "claim_conflict")); got != 1 {
		t.Errorf("ProcessingFailures{simulated,claim_conflict} = %v, want 1", got)
	}
}
```

Extend `fakeProcessorStore` (in `processor_test.go`) with a `completeCreatedAt time.Time` field, returned from its `CompletePayment` fake implementation.

- [ ] **Step 2: Run to verify failure**

Run: `cd go-api && go test ./internal/worker/... -run 'TestPublisher_PollOnce_Increments|TestHandleRoutedPayment_.*Metrics|TestHandleRoutedPayment_ClaimFailure' -v`
Expected: FAIL — `Metrics`/`Logger` fields don't exist yet on `Publisher`/`Processor`.

- [ ] **Step 3: Instrument `Publisher`**

```go
// publisher.go
type Publisher struct {
	Store   OutboxStore
	Publish func(ctx context.Context, key string, value []byte) error
	Metrics *observability.Metrics
}

func (p *Publisher) PollOnce(ctx context.Context) (bool, error) {
	ctx, span := observability.Tracer("outbox").Start(ctx, "outbox.publish")
	defer span.End()
	published, err := p.Store.PublishNextOutboxEvent(ctx, func(evt postgres.OutboxEvent) error {
		return p.Publish(ctx, evt.PaymentID, evt.Payload)
	})
	if published && err == nil {
		p.Metrics.EventsPublished.Inc()
	}
	return published, err
}
```

Add `"chainroute/go-api/internal/observability"` to imports. Add a `Logger *slog.Logger` field to `Publisher` alongside `Metrics`, and in `Run`, replace `log.Printf("ERROR: outbox publish failed: %v", err)` with `p.Logger.ErrorContext(ctx, "outbox publish failed", "error", err)`. Thread `Logger: logger` into the `Publisher{...}` literal in `cmd/worker/main.go` (Task 5's construction point) alongside `Metrics: metrics`.

- [ ] **Step 4: Instrument `Processor`**

```go
// processor.go — struct additions
type Processor struct {
	Store          PaymentStore
	Executor       TestnetExecutor
	TestnetTimeout time.Duration
	Metrics        *observability.Metrics
	Logger         *slog.Logger
}

func (p *Processor) HandleRoutedPayment(ctx context.Context, evt events.RoutedPayment) error {
	carrier := propagation.MapCarrier(evt.TraceCarrier)
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)
	ctx, span := observability.Tracer("kafka").Start(ctx, "kafka.process")
	defer span.End()
	span.SetAttributes(attribute.String("payment.id", evt.PaymentID))

	start := time.Now()
	p.Metrics.EventsConsumed.Inc()

	claimed, mode, err := p.Store.ClaimPayment(ctx, evt.PaymentID)
	if err != nil {
		p.Metrics.ProcessingFailures.WithLabelValues("unknown", "claim_conflict").Inc()
		return fmt.Errorf("claim payment %s: %w", evt.PaymentID, err)
	}
	if !claimed {
		return nil
	}
	span.SetAttributes(attribute.String("payment.execution_mode", string(mode)))

	if mode == payment.ExecutionModeTestnet {
		if p.Executor == nil {
			p.Metrics.ProcessingFailures.WithLabelValues(string(mode), "execution_error").Inc()
			return fmt.Errorf("payment %s is execution_mode=testnet but this worker has no Executor configured (BLOCKCHAIN_ENV != testnet) -- this should be unreachable if the API layer's testnet gate is working", evt.PaymentID)
		}
		timeout := p.TestnetTimeout
		if timeout <= 0 {
			timeout = defaultTestnetHandleTimeout
		}
		// Independent deadline (unchanged, Finding 4), but the trace SPAN
		// CONTEXT must still carry forward so the child execution.run span
		// nests under this kafka.process trace rather than starting a new,
		// disconnected one.
		testnetCtx := trace.ContextWithSpanContext(context.Background(), trace.SpanContextFromContext(ctx))
		testnetCtx, cancel := context.WithTimeout(testnetCtx, timeout)
		defer cancel()
		err := p.Executor.ExecuteTestnetPayment(testnetCtx, evt.PaymentID)
		p.Metrics.ProcessingDuration.WithLabelValues(string(mode)).Observe(time.Since(start).Seconds())
		if err != nil {
			p.Metrics.ProcessingFailures.WithLabelValues(string(mode), "execution_error").Inc()
		}
		return err
	}

	result := execution.Execute(evt.PaymentID)
	terminal := payment.StatusCompleted
	outcome := "completed"
	if !result.Success {
		terminal = payment.StatusFailed
		outcome = "failed"
	}

	completed, createdAt, err := p.Store.CompletePayment(ctx, evt.PaymentID, terminal)
	p.Metrics.ProcessingDuration.WithLabelValues(string(mode)).Observe(time.Since(start).Seconds())
	if err != nil {
		p.Metrics.ProcessingFailures.WithLabelValues(string(mode), "execution_error").Inc()
		return fmt.Errorf("complete payment %s: %w", evt.PaymentID, err)
	}
	if completed {
		p.Metrics.PaymentDuration.WithLabelValues(string(mode), outcome).Observe(time.Since(createdAt).Seconds())
		p.Metrics.PaymentsProcessing.WithLabelValues(string(mode)).Dec()
		if terminal == payment.StatusCompleted {
			p.Metrics.PaymentsCompleted.WithLabelValues(string(mode)).Inc()
		} else {
			p.Metrics.PaymentsFailed.WithLabelValues(string(mode), "execution").Inc()
		}
	}
	return nil
}
```

Add imports: `"log/slog"`, `"go.opentelemetry.io/otel"`, `"go.opentelemetry.io/otel/attribute"`, `"go.opentelemetry.io/otel/propagation"`, `"go.opentelemetry.io/otel/trace"`, `"chainroute/go-api/internal/observability"`.

Note the `"unknown"` provider label used at the claim-failure point above: at that point in the code, no provider has been determined yet (claim failure happens before any provider lookup), so `ProcessingFailures` there is scoped by `execution_mode`/`reason` only, never by provider — this matches the metric's own label set (`execution_mode`, `reason`), which never includes `provider`; re-read the metric catalogue in Task 2 to confirm — `ProcessingFailures` has no `provider` label at all, so remove any stray reference to one if you find it doesn't compile.

- [ ] **Step 5: Instrument `Recovery`**

```go
// recovery.go
type Recovery struct {
	Store     RecoveryStore
	Staleness time.Duration
	Metrics   *observability.Metrics
	Logger    *slog.Logger
}

func (r *Recovery) SweepOnce(ctx context.Context) (int, error) {
	ids, err := r.Store.StalePaymentIDs(ctx, r.Staleness)
	if err != nil {
		return 0, err
	}
	completed := 0
	for _, id := range ids {
		result := execution.Execute(id)
		terminal := payment.StatusCompleted
		outcome := "completed"
		if !result.Success {
			terminal = payment.StatusFailed
			outcome = "failed"
		}
		didComplete, createdAt, err := r.Store.CompletePayment(ctx, id, terminal)
		if err != nil {
			r.Logger.ErrorContext(ctx, "recovery sweep failed to complete payment", "payment_id", id, "error", err)
			continue
		}
		if didComplete {
			completed++
			r.Metrics.StaleRecoveries.WithLabelValues("recovery").Inc()
			r.Metrics.PaymentDuration.WithLabelValues(string(payment.ExecutionModeSimulated), outcome).Observe(time.Since(createdAt).Seconds())
			r.Metrics.PaymentsProcessing.WithLabelValues(string(payment.ExecutionModeSimulated)).Dec()
			if terminal == payment.StatusCompleted {
				r.Metrics.PaymentsCompleted.WithLabelValues(string(payment.ExecutionModeSimulated)).Inc()
			} else {
				r.Metrics.PaymentsFailed.WithLabelValues(string(payment.ExecutionModeSimulated), "execution").Inc()
			}
		}
	}
	return completed, nil
}
```

In `Run`, replace `log.Printf("ERROR: recovery sweep failed: %v", err)` with `r.Logger.ErrorContext(ctx, "recovery sweep failed", "error", err)`.

- [ ] **Step 6: Thread `Metrics`/`Logger` from `cmd/worker/main.go`**

Update the `Publisher{...}`, `Processor{...}`, `Recovery{...}` literals in `cmd/worker/main.go` (constructed in Task 5's edit) to include `Metrics: metrics, Logger: logger`.

- [ ] **Step 7: Run tests**

```bash
cd go-api && go build ./... && go vet ./... && go test ./...
```

Expected: PASS, all packages.

- [ ] **Step 8: Commit**

```bash
git add go-api/internal/worker/publisher.go go-api/internal/worker/processor.go go-api/internal/worker/recovery.go go-api/cmd/worker/main.go go-api/internal/worker/*_test.go
git commit -m "feat(go-api): instrument outbox/Kafka/worker processing and payment completion metrics"
```

---

### Task 9: Worker execution spans + metrics (Executor)

**Files:**
- Modify: `go-api/internal/worker/executor.go`
- Modify: `go-api/cmd/worker/main.go`
- Test: `go-api/internal/worker/executor_test.go` (extend)

**Interfaces:**
- Consumes: `observability.Tracer`/`Metrics`/`SanitizeProviderLabel` (Tasks 1-3).
- Produces: `Executor.Metrics`/`Executor.Logger` fields.

- [ ] **Step 1: Write the failing tests**

```go
func TestExecuteTestnetPayment_HappyPath_RecordsExecutionMetrics(t *testing.T) {
	// Reuse the existing happy-path test setup (fakeExecutorStore, fake
	// signer/provider) exactly as already constructed elsewhere in this
	// file, additionally wiring Metrics: observability.NewMetrics().
	metrics := observability.NewMetrics()
	e := newTestExecutor(t, /* ...existing fakes... */)
	e.Metrics = metrics

	if err := e.ExecuteTestnetPayment(context.Background(), "pay-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := testutil.ToFloat64(metrics.ExecutionAttempts.WithLabelValues("across")); got != 1 {
		t.Errorf("ExecutionAttempts{across} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.Broadcasts.WithLabelValues("across")); got != 1 {
		t.Errorf("Broadcasts{across} = %v, want 1", got)
	}
	if got := testutil.CollectAndCount(metrics.ExecutionDuration); got == 0 {
		t.Error("expected an ExecutionDuration observation")
	}
}

func TestExecuteTestnetPayment_EnvelopeValidationFailure_RecordsFailureMetricNotAttempt(t *testing.T) {
	// Mirror one of the existing WrongEnvelope*IsHardError tests, adding
	// Metrics, and assert ExecutionAttempts is NOT incremented (the
	// attempt only counts once TryCreateExecution actually wins the race
	// -- a rejected envelope never reaches that point) while
	// ExecutionsFailed IS incremented with reason "envelope_invalid".
	metrics := observability.NewMetrics()
	e := /* ...existing bad-envelope test setup... */
	e.Metrics = metrics

	_ = e.ExecuteTestnetPayment(context.Background(), "pay-badchain")

	if got := testutil.ToFloat64(metrics.ExecutionAttempts.WithLabelValues("across")); got != 0 {
		t.Errorf("ExecutionAttempts{across} = %v, want 0 (nonce never allocated)", got)
	}
	if got := testutil.ToFloat64(metrics.ExecutionsFailed.WithLabelValues("across", "envelope_invalid")); got != 1 {
		t.Errorf("ExecutionsFailed{across,envelope_invalid} = %v, want 1", got)
	}
}
```

Adapt these to the exact existing fake/helper names already present in `executor_test.go` (read the file first — do not invent new fake types when `newTestExecutor`/`fakeExecutorStore`/`fakeSigner` etc. already exist per Phase 9's implementation).

- [ ] **Step 2: Run to verify failure**

Run: `cd go-api && go test ./internal/worker/... -run TestExecuteTestnetPayment_.*Metrics -v`
Expected: FAIL — `Metrics` field doesn't exist.

- [ ] **Step 3: Instrument `Executor`**

Read the full current `executor.go` (452 lines) before editing — this file was substantially rewritten in Phase 9 and this plan's earlier code excerpts (Task setup survey, above) are illustrative, not a literal diff. Add fields:

```go
type Executor struct {
	Store                      ExecutorStore
	Wallet                     *evm.Wallet
	OriginClient               ExecutorEthClient
	QuoteProviders             map[string]quote.Provider
	Signers                    map[string]quote.Signer
	ExpectedContractByProvider map[string]common.Address
	MaxFeeSlippageBps          int64
	OriginChainID              int64
	DestChainID                int64
	MaxAmountWei               *big.Int
	Metrics                    *observability.Metrics
	Logger                     *slog.Logger
}
```

Wrap `ExecuteTestnetPayment` and `DriveExecutionForward`'s bodies with an `execution.run` span (span at the top, `defer span.End()`, exactly like every prior instrumentation task's pattern) and a provider-labeled duration timer. Since the provider isn't known until `quoteRow` is fetched inside the function, start timing immediately but only label/record metrics once `quoteRow.Provider` is available:

```go
func (e *Executor) ExecuteTestnetPayment(ctx context.Context, paymentID string) error {
	ctx, span := observability.Tracer("execution").Start(ctx, "execution.run")
	defer span.End()
	span.SetAttributes(attribute.String("payment.id", paymentID))
	start := time.Now()

	quoteRow, found, err := e.Store.GetQuoteByPaymentID(ctx, paymentID)
	if err != nil {
		return fmt.Errorf("get quote for payment %s: %w", paymentID, err)
	}
	if !found {
		return fmt.Errorf("payment %s has no payment_quotes row -- cannot execute without a selected route", paymentID)
	}
	providerLabel := observability.SanitizeProviderLabel(quoteRow.Provider)
	span.SetAttributes(attribute.String("payment.provider", providerLabel))

	// ... existing expiry/provider-lookup/GetPayment/GetQuote/slippage/
	// amount-guardrail checks, UNCHANGED, except: every existing
	// `return fmt.Errorf(...)`/`return nil` after a MarkProcessingFailed
	// call in this function gets one line added immediately before it:
	//     e.Metrics.ExecutionsFailed.WithLabelValues(providerLabel, "<reason>").Inc()
	// where <reason> matches the MarkProcessingFailed reason string at
	// that call site ("routing_quote_expired" -> reason "quote_expired",
	// "route_unavailable" -> reason "route_unavailable",
	// "fee_slippage_exceeded" -> reason "slippage_exceeded",
	// "amount_exceeds_guardrail" -> reason "amount_exceeds_guardrail") ...

	envelope, err := e.buildValidatedEnvelope(ctx, paymentID, quoteRow, freshQuote)
	if err != nil {
		e.Metrics.ExecutionsFailed.WithLabelValues(providerLabel, "envelope_invalid").Inc()
		return err
	}

	exec, created, err := e.Store.TryCreateExecution(ctx, postgres.CreateExecutionParams{
		PaymentID: paymentID, WalletAddress: e.Wallet.Address.Hex(), BridgeProvider: quoteRow.Provider,
		OriginChainID: e.OriginChainID, DestinationChainID: e.DestChainID,
	})
	if err != nil {
		return fmt.Errorf("try create execution for payment %s: %w", paymentID, err)
	}
	if !created {
		return nil
	}
	e.Metrics.ExecutionAttempts.WithLabelValues(providerLabel).Inc()
	err = e.signAndBroadcastFresh(ctx, exec, envelope, freshQuote)
	e.Metrics.ExecutionDuration.WithLabelValues(providerLabel).Observe(time.Since(start).Seconds())
	if err != nil {
		e.Metrics.ExecutionsFailed.WithLabelValues(providerLabel, "broadcast_error").Inc()
	}
	return err
}
```

Inside `signAndBroadcastFresh`, wrap the sign step and the broadcast step each in their own child span (find the exact current line where `e.Wallet.SignTx` is called, and the exact line where `e.broadcastWithRecovery` is called):

```go
	_, signSpan := observability.Tracer("execution").Start(ctx, "execution.sign")
	signedTx, err := e.Wallet.SignTx(unsignedTx, big.NewInt(envelope.ChainID))
	signSpan.End()
	if err != nil {
		return fmt.Errorf("sign tx for execution %s: %w", exec.ID, err)
	}

	// ... existing marshal/persist unchanged ...

	broadcastCtx, broadcastSpan := observability.Tracer("execution").Start(ctx, "execution.broadcast")
	err = e.broadcastWithRecovery(broadcastCtx, exec)
	broadcastSpan.End()
	if err != nil {
		return fmt.Errorf("broadcast execution %s: %w", exec.ID, err)
	}
	e.Metrics.Broadcasts.WithLabelValues(observability.SanitizeProviderLabel(exec.BridgeProvider)).Inc()
```

Add `"time"` (if not already imported), `"log/slog"`, `"go.opentelemetry.io/otel/attribute"`, `"chainroute/go-api/internal/observability"` to imports. Replace this file's one existing `log.Printf("WARNING: ...")` (line 282 per the survey) with `e.Logger.WarnContext(ctx, ...)`.

- [ ] **Step 4: Thread `Metrics`/`Logger` from `cmd/worker/main.go`**

Update the `Executor{...}` literal (inside the `if blockchainEnv == "testnet"` block) to include `Metrics: metrics, Logger: logger`.

- [ ] **Step 5: Run tests**

```bash
cd go-api && go build ./... && go vet ./... && go test ./internal/worker/... -v
```

Expected: PASS, every existing `executor_test.go` test plus the 2 new ones — note EVERY existing test in this file that constructs an `Executor{...}` literal directly (not via a shared helper) now additionally needs `Metrics: observability.NewMetrics()` set, or it will nil-panic the first time `e.Metrics.X.Inc()` runs. Check whether a shared `newTestExecutor` helper already sets every field (per Phase 9's Task 9 pattern of updating shared helpers once) — if so, add `Metrics`/`Logger` defaults there instead of touching every individual test.

- [ ] **Step 6: Commit**

```bash
git add go-api/internal/worker/executor.go go-api/cmd/worker/main.go go-api/internal/worker/executor_test.go
git commit -m "feat(go-api): instrument Executor with sign/broadcast spans and execution metrics"
```

---

### Task 10: Reconciliation spans + metrics (Reconciler)

**Files:**
- Modify: `go-api/internal/worker/reconciler.go`
- Modify: `go-api/cmd/worker/main.go`
- Test: `go-api/internal/worker/reconciler_test.go` (extend)

**Interfaces:**
- Consumes: `observability.Tracer`/`Metrics`/`SanitizeProviderLabel` (Tasks 1-3); `Store.CompleteSubmittedPayment` new signature (Task 7).
- Produces: `Reconciler.Metrics`/`Reconciler.Logger` fields.

- [ ] **Step 1: Write the failing tests**

```go
func TestCheckAndUpdateOutcome_RelaySuccessCompletesPayment_RecordsMetricsAndDuration(t *testing.T) {
	metrics := observability.NewMetrics()
	r := /* ...existing relay-success test setup from Phase 9's Task 10... */
	r.Metrics = metrics

	if err := r.checkAndUpdateOutcome(context.Background(), exec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := testutil.ToFloat64(metrics.Reconciliations.WithLabelValues("relay")); got != 1 {
		t.Errorf("Reconciliations{relay} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.ExecutionsCompleted.WithLabelValues("relay")); got != 1 {
		t.Errorf("ExecutionsCompleted{relay} = %v, want 1", got)
	}
	if got := testutil.CollectAndCount(metrics.ReconciliationDuration); got == 0 {
		t.Error("expected a ReconciliationDuration observation")
	}
	if got := testutil.CollectAndCount(metrics.PaymentDuration); got == 0 {
		t.Error("expected a PaymentDuration observation (created_at now flows through markTerminal)")
	}
}
```

Adapt to the exact existing fake `StatusChecker`/store setup already in this file (Phase 9's Task 10 added the sibling `RelaySuccessCompletesPayment` test this mirrors).

- [ ] **Step 2: Run to verify failure**

Run: `cd go-api && go test ./internal/worker/... -run TestCheckAndUpdateOutcome_RelaySuccess.*Metrics -v`
Expected: FAIL.

- [ ] **Step 3: Instrument `Reconciler`**

Read the full current `reconciler.go` (shown in full above) before editing. Add fields:

```go
type Reconciler struct {
	Store          ReconcilerStore
	Executor       *Executor
	OriginClient   ReconcilerEthClient
	StatusCheckers map[string]quote.StatusChecker
	WalletAddress  common.Address
	OriginChainID  int64
	Staleness      time.Duration
	Metrics        *observability.Metrics
	Logger         *slog.Logger
}
```

Wrap `checkAndUpdateOutcome` with a span and a duration timer, and increment `Reconciliations` right after a status checker call actually completes (success or definitive-terminal, not on a transient-error early return, since a transient error isn't really "a reconciliation," it's a skipped attempt):

```go
func (r *Reconciler) checkAndUpdateOutcome(ctx context.Context, exec payment.Execution) error {
	ctx, span := observability.Tracer("reconcile").Start(ctx, "reconcile.check")
	defer span.End()
	providerLabel := observability.SanitizeProviderLabel(exec.BridgeProvider)
	span.SetAttributes(attribute.String("payment.provider", providerLabel), attribute.String("payment.id", exec.PaymentID))
	start := time.Now()

	// ... existing SignedTxHash-nil check, MarkSubmitted repair, hash
	// decode, TransactionReceipt call, receipt.Status==0 branch --
	// UNCHANGED ...

	checker, ok := r.StatusCheckers[exec.BridgeProvider]
	if !ok {
		return fmt.Errorf("execution %s uses provider %q, which this reconciler has no configured status checker for", exec.ID, exec.BridgeProvider)
	}
	result, err := checker.CheckStatus(ctx, quote.StatusRequest{
		ProviderReferenceID: derefOrEmpty(exec.ProviderReferenceID),
		OriginTxHash:        *exec.SignedTxHash,
	})
	if err != nil {
		return nil // transient -- not counted as a reconciliation attempt
	}
	r.Metrics.Reconciliations.WithLabelValues(providerLabel).Inc()
	r.Metrics.ReconciliationDuration.WithLabelValues(providerLabel).Observe(time.Since(start).Seconds())

	switch result.State {
	case quote.StateFilled:
		return r.markTerminal(ctx, exec, result, payment.StatusCompleted)
	case quote.StateRefunded, quote.StateReverted, quote.StateFillFailed:
		return r.markTerminal(ctx, exec, result, payment.StatusFailed)
	default:
		return nil
	}
}
```

Update `markTerminal` to capture `createdAt` from the widened `CompleteSubmittedPayment` (Task 7) and record the completion metrics/duration:

```go
func (r *Reconciler) markTerminal(ctx context.Context, exec payment.Execution, result quote.StatusResult, terminal payment.Status) error {
	external := externalStateToStatus(result.State)
	confirmedAt := &sql.NullTime{Time: time.Now().UTC(), Valid: true}
	if err := r.Store.UpdateExecutionExternalStatus(ctx, exec.ID, external, result.RawStatus, confirmedAt); err != nil {
		return fmt.Errorf("record %s: %w", external, err)
	}
	completed, createdAt, err := r.Store.CompleteSubmittedPayment(ctx, exec.PaymentID, terminal)
	if err != nil {
		return fmt.Errorf("complete payment as %s: %w", terminal, err)
	}
	providerLabel := observability.SanitizeProviderLabel(exec.BridgeProvider)
	if !completed {
		r.Logger.ErrorContext(ctx, "CompleteSubmittedPayment affected no rows despite a definitive terminal observation",
			"payment_id", exec.PaymentID, "execution_id", exec.ID, "terminal", string(terminal), "external_status", string(external))
		return nil
	}
	outcome := "completed"
	if terminal == payment.StatusFailed {
		outcome = "failed"
		r.Metrics.ExecutionsFailed.WithLabelValues(providerLabel, "reconciled_failed").Inc()
	} else {
		r.Metrics.ExecutionsCompleted.WithLabelValues(providerLabel).Inc()
	}
	r.Metrics.PaymentDuration.WithLabelValues(string(payment.ExecutionModeTestnet), outcome).Observe(time.Since(createdAt).Seconds())
	r.Metrics.PaymentsProcessing.WithLabelValues(string(payment.ExecutionModeTestnet)).Dec()
	if terminal == payment.StatusCompleted {
		r.Metrics.PaymentsCompleted.WithLabelValues(string(payment.ExecutionModeTestnet)).Inc()
	} else {
		r.Metrics.PaymentsFailed.WithLabelValues(string(payment.ExecutionModeTestnet), "reconciliation").Inc()
	}
	return nil
}
```

Replace this file's remaining `log.Printf` calls (`recoverStaleProcessingWithoutExecution`, `driveStaleNotYetBroadcast`, `checkBroadcastOutcomes`, `checkNonceDivergence`) with `r.Logger.ErrorContext(ctx, ...)`/`r.Logger.WarnContext(ctx, ...)` equivalents, and add `r.Metrics.StaleRecoveries.WithLabelValues("reconciler").Inc()` inside `recoverStaleProcessingWithoutExecution`'s loop, once per ID successfully recovered (mirroring `Recovery`'s own `"recovery"`-labeled counter from Task 8 — these are the two independent sources named in the metric catalogue).

Add imports: `"time"` (if not present), `"log/slog"`, `"go.opentelemetry.io/otel/attribute"`, `"chainroute/go-api/internal/observability"`.

- [ ] **Step 4: Thread `Metrics`/`Logger` from `cmd/worker/main.go`**

Update the `Reconciler{...}` literal to include `Metrics: metrics, Logger: logger`.

- [ ] **Step 5: Run tests**

```bash
cd go-api && go build ./... && go vet ./... && go test ./internal/worker/... -v
```

Expected: PASS — check the shared reconciler test helper for the same "every literal needs Metrics/Logger" concern as Task 9.

- [ ] **Step 6: Commit**

```bash
git add go-api/internal/worker/reconciler.go go-api/cmd/worker/main.go go-api/internal/worker/reconciler_test.go
git commit -m "feat(go-api): instrument Reconciler with reconciliation spans, metrics, and payment-duration wiring"
```

---

### Task 11: Resilience tests — observability failure never blocks payment processing

**Files:**
- Test: `go-api/internal/observability/resilience_test.go` (new)
- Test: `go-api/internal/handler/payments_test.go` (extend)

**Interfaces:**
- Consumes: everything from Tasks 1-10 (this is a cross-cutting proof task, no new production code).

- [ ] **Step 1: Write the tests**

```go
// payments_test.go addition — proves the nil-safe accessor pattern
// (established before Task 6) actually holds: a Handler literal that
// leaves Metrics/Logger unset (exactly like every pre-Phase-10 test in
// this file) must not panic.
func TestPostPayments_MetricsAndLoggerLeftNil_DoesNotPanic(t *testing.T) {
	h := &Handler{Client: &fakeClient{}, Store: &fakePaymentStore{}}
	req := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(
		`{"source_chain":"ethereum","destination_chain":"base","asset":"usdc","amount":"100"}`))
	req.Header.Set("Idempotency-Key", t.Name())
	w := httptest.NewRecorder()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("PostPayments panicked with Metrics/Logger left nil: %v", r)
		}
	}()
	h.PostPayments(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201, body: %s", w.Code, w.Body.String())
	}
}
```

```go
package observability

import (
	"context"
	"testing"
	"time"
)

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
```

```go
// payments_test.go addition
func TestPostPayments_SimulatedMode_CompletesSuccessfullyWithUnreachableTracingBackend(t *testing.T) {
	// Proves tracing failure cannot block or fail payment creation --
	// engineering constraint: "instrumentation must not become a
	// correctness dependency."
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "10.255.255.1:4317")
	shutdown, err := observability.InitTracing(context.Background(), "test")
	if err != nil {
		t.Fatalf("InitTracing: %v", err)
	}
	defer shutdown(context.Background())

	h, _ := newTestHandlerWithMetrics(t)
	req := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(
		`{"source_chain":"ethereum","destination_chain":"base","asset":"usdc","amount":"100"}`))
	req.Header.Set("Idempotency-Key", t.Name())
	w := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		h.PostPayments(w, req)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("PostPayments did not return within 5s -- tracing to an unreachable endpoint may be blocking the request")
	}
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201, body: %s", w.Code, w.Body.String())
	}
}
```

- [ ] **Step 2: Run to verify current behavior**

```bash
cd go-api && go test ./internal/observability/... ./internal/handler/... -run 'TestInitTracing_Unreachable|TestPostPayments_.*Unreachable' -v -timeout 30s
```

Expected: PASS if Tasks 1-6's non-blocking configuration (`otlptracegrpc.WithTimeout(2*time.Second)`, `WithInsecure`, `BatchSpanProcessor`) is correct. If this FAILS or times out, that is a genuine defect in Task 3's exporter configuration — fix `InitTracing` (not these tests) before proceeding; do not weaken the timeout assertion to make a slow test "pass."

- [ ] **Step 3: Commit**

```bash
git add go-api/internal/observability/resilience_test.go go-api/internal/handler/payments_test.go
git commit -m "test(go-api): prove tracing failure never blocks payment processing"
```

---

### Task 12: C++ router metrics

**Files:**
- Create: `cpp-routing-service/src/metrics.hpp`
- Create: `cpp-routing-service/src/metrics.cpp`
- Modify: `cpp-routing-service/src/routing_service.cpp`
- Modify: `cpp-routing-service/src/main.cpp`
- Modify: `cpp-routing-service/CMakeLists.txt`
- Test: `cpp-routing-service/tests/metrics_test.cpp`
- Modify: `cpp-routing-service/tests/CMakeLists.txt`

**Interfaces:**
- Produces: `chainroute::RouteMetrics` class, `chainroute::RouteMetrics::PrometheusText() const -> std::string`, `chainroute::MetricsScope` RAII helper — used only within this task's own files.

- [ ] **Step 1: Read current files**

Read `cpp-routing-service/src/routing_service.cpp`, `cpp-routing-service/src/main.cpp`, and both `CMakeLists.txt` files in full before editing (their exact current shape was surveyed at plan-writing time but must be re-confirmed).

- [ ] **Step 2: Write the failing test**

```cpp
// cpp-routing-service/tests/metrics_test.cpp
#include "metrics.hpp"

#include <gtest/gtest.h>

TEST(RouteMetricsTest, StartsAtZero) {
    chainroute::RouteMetrics m;
    std::string text = m.PrometheusText();
    EXPECT_NE(text.find("chainroute_router_requests_total 0"), std::string::npos);
    EXPECT_NE(text.find("chainroute_router_successful_routes_total 0"), std::string::npos);
    EXPECT_NE(text.find("chainroute_router_no_route_total 0"), std::string::npos);
    EXPECT_NE(text.find("chainroute_router_errors_total 0"), std::string::npos);
}

TEST(RouteMetricsTest, RecordSuccessIncrementsRequestsAndSuccessfulRoutes) {
    chainroute::RouteMetrics m;
    m.RecordRequest();
    m.RecordOutcome(chainroute::RouteOutcome::kSuccess, 5, 0.002);

    std::string text = m.PrometheusText();
    EXPECT_NE(text.find("chainroute_router_requests_total 1"), std::string::npos);
    EXPECT_NE(text.find("chainroute_router_successful_routes_total 1"), std::string::npos);
    EXPECT_NE(text.find("chainroute_router_no_route_total 0"), std::string::npos);
}

TEST(RouteMetricsTest, RecordNoRouteIncrementsNoRouteCounter) {
    chainroute::RouteMetrics m;
    m.RecordRequest();
    m.RecordOutcome(chainroute::RouteOutcome::kNoRoute, 3, 0.001);

    std::string text = m.PrometheusText();
    EXPECT_NE(text.find("chainroute_router_no_route_total 1"), std::string::npos);
    EXPECT_NE(text.find("chainroute_router_successful_routes_total 0"), std::string::npos);
}

TEST(RouteMetricsTest, RecordErrorIncrementsErrorCounter) {
    chainroute::RouteMetrics m;
    m.RecordRequest();
    m.RecordOutcome(chainroute::RouteOutcome::kError, 0, 0.0005);

    std::string text = m.PrometheusText();
    EXPECT_NE(text.find("chainroute_router_errors_total 1"), std::string::npos);
}

TEST(RouteMetricsTest, PrometheusTextIsWellFormedExpositionFormat) {
    chainroute::RouteMetrics m;
    m.RecordRequest();
    m.RecordOutcome(chainroute::RouteOutcome::kSuccess, 4, 0.01);
    std::string text = m.PrometheusText();

    // Every metric line has a "# HELP" and "# TYPE" line before its
    // value line -- the minimum Prometheus text-format contract scrapers
    // rely on to know each series' type.
    EXPECT_NE(text.find("# HELP chainroute_router_requests_total"), std::string::npos);
    EXPECT_NE(text.find("# TYPE chainroute_router_requests_total counter"), std::string::npos);
    EXPECT_NE(text.find("# TYPE chainroute_router_duration_seconds histogram"), std::string::npos);
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `cmake -S cpp-routing-service -B cpp-routing-service/build && cmake --build cpp-routing-service/build --target chainroute_service_tests`
Expected: FAIL — `metrics.hpp` does not exist.

- [ ] **Step 4: Implement `metrics.hpp`/`metrics.cpp`**

```cpp
// cpp-routing-service/src/metrics.hpp
#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <string>

namespace chainroute {

enum class RouteOutcome { kSuccess, kNoRoute, kError };

// RouteMetrics holds hand-rolled, lock-free counters and a fixed-bucket
// latency histogram for the routing service. No routing/Dijkstra logic
// lives here -- this is purely observational bookkeeping the service
// layer updates around its existing FindRoute call, per Phase 10 design
// doc §4 ("no large dependency for a trivial metric").
class RouteMetrics {
public:
    static constexpr int kLatencyBucketCount = 8;
    // Upper bounds in seconds: 1ms,2ms,5ms,10ms,20ms,50ms,100ms,+Inf(implicit last bucket)
    static constexpr std::array<double, kLatencyBucketCount> kLatencyBucketsSeconds = {
        0.001, 0.002, 0.005, 0.010, 0.020, 0.050, 0.100, 1.000};
    static constexpr int kEdgeBucketCount = 5;
    // Upper bounds for candidate-edge-count histogram: 0,1,2,5,10(+Inf implicit)
    static constexpr std::array<int, kEdgeBucketCount> kEdgeBuckets = {0, 1, 2, 5, 10};

    void RecordRequest();
    void RecordOutcome(RouteOutcome outcome, int candidateEdgeCount, double durationSeconds);

    // PrometheusText renders a snapshot of every metric in Prometheus
    // text exposition format (https://prometheus.io/docs/instrumenting/exposition_formats/).
    std::string PrometheusText() const;

private:
    std::atomic<uint64_t> requests_total_{0};
    std::atomic<uint64_t> successful_routes_total_{0};
    std::atomic<uint64_t> no_route_total_{0};
    std::atomic<uint64_t> errors_total_{0};

    std::array<std::atomic<uint64_t>, kLatencyBucketCount> latency_bucket_counts_{};
    std::atomic<uint64_t> latency_count_{0};
    std::atomic<double> latency_sum_{0.0};

    std::array<std::atomic<uint64_t>, kEdgeBucketCount> edge_bucket_counts_{};
    std::atomic<uint64_t> edge_count_{0};
    std::atomic<double> edge_sum_{0.0};
};

// MetricsScope is an RAII helper: construction records a request,
// destruction records the outcome/duration/candidate-edge-count based on
// whatever the caller set via SetOutcome/SetCandidateEdgeCount before the
// scope ends. This lets FindRoute bracket its ENTIRE existing body
// (including every early-return validation branch) with a single
// declaration at the top, with zero changes to the Dijkstra call itself.
class MetricsScope {
public:
    MetricsScope(RouteMetrics& metrics, int candidateEdgeCount);
    ~MetricsScope();

    void SetOutcome(RouteOutcome outcome);

private:
    RouteMetrics& metrics_;
    int candidate_edge_count_;
    RouteOutcome outcome_ = RouteOutcome::kError; // default: an unset scope (e.g. an early throw) counts as an error, never as a silent success
    std::chrono::steady_clock::time_point start_;
};

}  // namespace chainroute
```

```cpp
// cpp-routing-service/src/metrics.cpp
#include "metrics.hpp"

#include <chrono>
#include <sstream>

namespace chainroute {

void RouteMetrics::RecordRequest() { requests_total_.fetch_add(1, std::memory_order_relaxed); }

void RouteMetrics::RecordOutcome(RouteOutcome outcome, int candidateEdgeCount, double durationSeconds) {
    switch (outcome) {
        case RouteOutcome::kSuccess:
            successful_routes_total_.fetch_add(1, std::memory_order_relaxed);
            break;
        case RouteOutcome::kNoRoute:
            no_route_total_.fetch_add(1, std::memory_order_relaxed);
            break;
        case RouteOutcome::kError:
            errors_total_.fetch_add(1, std::memory_order_relaxed);
            break;
    }

    latency_count_.fetch_add(1, std::memory_order_relaxed);
    double expectedSum = latency_sum_.load(std::memory_order_relaxed);
    while (!latency_sum_.compare_exchange_weak(expectedSum, expectedSum + durationSeconds, std::memory_order_relaxed)) {
    }
    for (int i = 0; i < kLatencyBucketCount; ++i) {
        if (durationSeconds <= kLatencyBucketsSeconds[i]) {
            latency_bucket_counts_[i].fetch_add(1, std::memory_order_relaxed);
        }
    }

    edge_count_.fetch_add(1, std::memory_order_relaxed);
    double expectedEdgeSum = edge_sum_.load(std::memory_order_relaxed);
    while (!edge_sum_.compare_exchange_weak(expectedEdgeSum, expectedEdgeSum + candidateEdgeCount, std::memory_order_relaxed)) {
    }
    for (int i = 0; i < kEdgeBucketCount; ++i) {
        if (candidateEdgeCount <= kEdgeBuckets[i]) {
            edge_bucket_counts_[i].fetch_add(1, std::memory_order_relaxed);
        }
    }
}

std::string RouteMetrics::PrometheusText() const {
    std::ostringstream out;
    out << "# HELP chainroute_router_requests_total Total FindRoute requests.\n";
    out << "# TYPE chainroute_router_requests_total counter\n";
    out << "chainroute_router_requests_total " << requests_total_.load() << "\n";

    out << "# HELP chainroute_router_successful_routes_total Total requests that found a route.\n";
    out << "# TYPE chainroute_router_successful_routes_total counter\n";
    out << "chainroute_router_successful_routes_total " << successful_routes_total_.load() << "\n";

    out << "# HELP chainroute_router_no_route_total Total requests with no viable route.\n";
    out << "# TYPE chainroute_router_no_route_total counter\n";
    out << "chainroute_router_no_route_total " << no_route_total_.load() << "\n";

    out << "# HELP chainroute_router_errors_total Total requests that errored.\n";
    out << "# TYPE chainroute_router_errors_total counter\n";
    out << "chainroute_router_errors_total " << errors_total_.load() << "\n";

    out << "# HELP chainroute_router_duration_seconds FindRoute latency.\n";
    out << "# TYPE chainroute_router_duration_seconds histogram\n";
    uint64_t cumulative = 0;
    for (int i = 0; i < kLatencyBucketCount; ++i) {
        cumulative += latency_bucket_counts_[i].load();
        out << "chainroute_router_duration_seconds_bucket{le=\"" << kLatencyBucketsSeconds[i] << "\"} " << cumulative << "\n";
    }
    out << "chainroute_router_duration_seconds_bucket{le=\"+Inf\"} " << latency_count_.load() << "\n";
    out << "chainroute_router_duration_seconds_sum " << latency_sum_.load() << "\n";
    out << "chainroute_router_duration_seconds_count " << latency_count_.load() << "\n";

    out << "# HELP chainroute_router_candidate_edges Candidate edge count per request.\n";
    out << "# TYPE chainroute_router_candidate_edges histogram\n";
    uint64_t edgeCumulative = 0;
    for (int i = 0; i < kEdgeBucketCount; ++i) {
        edgeCumulative += edge_bucket_counts_[i].load();
        out << "chainroute_router_candidate_edges_bucket{le=\"" << kEdgeBuckets[i] << "\"} " << edgeCumulative << "\n";
    }
    out << "chainroute_router_candidate_edges_bucket{le=\"+Inf\"} " << edge_count_.load() << "\n";
    out << "chainroute_router_candidate_edges_sum " << edge_sum_.load() << "\n";
    out << "chainroute_router_candidate_edges_count " << edge_count_.load() << "\n";

    return out.str();
}

MetricsScope::MetricsScope(RouteMetrics& metrics, int candidateEdgeCount)
    : metrics_(metrics), candidate_edge_count_(candidateEdgeCount), start_(std::chrono::steady_clock::now()) {
    metrics_.RecordRequest();
}

MetricsScope::~MetricsScope() {
    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start_).count();
    metrics_.RecordOutcome(outcome_, candidate_edge_count_, elapsed);
}

void MetricsScope::SetOutcome(RouteOutcome outcome) { outcome_ = outcome; }

}  // namespace chainroute
```

Add `<chrono>` to `metrics.hpp`'s includes (used by `MetricsScope`'s `start_` member type).

- [ ] **Step 5: Instrument `routing_service.cpp`**

Read the current `RoutingServiceImpl::FindRoute` (lines 28-84 per the survey) in full. Add a `RouteMetrics& metrics_` member to `RoutingServiceImpl` (constructor parameter, alongside the existing `NetworkSimulator&`), and bracket `FindRoute`'s body with a `MetricsScope`, setting the outcome at each existing early-return/final-return point — **no other line in this function changes**:

```cpp
grpc::Status RoutingServiceImpl::FindRoute(grpc::ServerContext* context,
                                            const chainroute::v1::FindRouteRequest* request,
                                            chainroute::v1::FindRouteResponse* response) {
    chainroute::MetricsScope scope(metrics_, request->candidate_edges_size());
    // ... existing enum/amount validation; on each existing early
    // `return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, ...)`,
    // add `scope.SetOutcome(chainroute::RouteOutcome::kError);`
    // immediately before the return ...

    // ... existing graph construction + findCheapestRoute call, UNCHANGED ...

    if (!route.has_value()) {
        scope.SetOutcome(chainroute::RouteOutcome::kNoRoute);
        response->set_route_found(false);
        return grpc::Status::OK;
    }

    // ... existing response serialization ...
    scope.SetOutcome(chainroute::RouteOutcome::kSuccess);
    return grpc::Status::OK;
}
```

Add `#include "metrics.hpp"` to `routing_service.cpp`'s includes and to its header (`routing_service.hpp`, adding the `RouteMetrics&` constructor parameter and member declaration there).

- [ ] **Step 6: Add the raw-socket `/metrics` listener to `main.cpp`**

Read `main.cpp` in full first (confirmed to already call `grpc::EnableDefaultHealthCheckService(true)` at line 81, per the survey). Add a metrics listener thread, started alongside the gRPC server and joined at shutdown:

```cpp
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cstring>
#include <thread>

// ... existing includes/main() setup ...

namespace {

void RunMetricsListener(const chainroute::RouteMetrics& metrics, int port, std::atomic<bool>& shouldStop) {
    int serverFd = socket(AF_INET, SOCK_STREAM, 0);
    if (serverFd < 0) {
        return;
    }
    int opt = 1;
    setsockopt(serverFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct timeval acceptTimeout{.tv_sec = 1, .tv_usec = 0};
    setsockopt(serverFd, SOL_SOCKET, SO_RCVTIMEO, &acceptTimeout, sizeof(acceptTimeout));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (bind(serverFd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(serverFd);
        return;
    }
    listen(serverFd, 16);

    while (!shouldStop.load()) {
        sockaddr_in clientAddr{};
        socklen_t clientLen = sizeof(clientAddr);
        int clientFd = accept(serverFd, reinterpret_cast<sockaddr*>(&clientAddr), &clientLen);
        if (clientFd < 0) {
            continue;  // accept timeout or transient error -- loop and re-check shouldStop
        }
        char buf[512];
        recv(clientFd, buf, sizeof(buf), 0);  // discard the request line -- this endpoint serves exactly one fixed body

        std::string body = metrics.PrometheusText();
        std::ostringstream response;
        response << "HTTP/1.1 200 OK\r\n"
                  << "Content-Type: text/plain; version=0.0.4\r\n"
                  << "Content-Length: " << body.size() << "\r\n"
                  << "Connection: close\r\n\r\n"
                  << body;
        std::string responseStr = response.str();
        send(clientFd, responseStr.data(), responseStr.size(), 0);
        close(clientFd);
    }
    close(serverFd);
}

}  // namespace

// Inside main(), after constructing the RouteMetrics and RoutingServiceImpl
// (passing metrics into the latter's constructor per Step 5) and before
// server->Wait():
int metricsPort = 9102;
if (const char* envPort = std::getenv("METRICS_PORT")) {
    metricsPort = std::atoi(envPort);
}
std::atomic<bool> stopMetricsListener{false};
std::thread metricsThread(RunMetricsListener, std::cref(metrics), metricsPort, std::ref(stopMetricsListener));

// ... existing server->Wait() or equivalent blocking call ...

// At shutdown (wherever the existing code handles server shutdown):
stopMetricsListener.store(true);
metricsThread.join();
```

Add `<sstream>` if not already included. This listener never parses HTTP method/path — it accepts any connection, discards the request bytes, and always serves the same fixed body, per the design doc's "not a general-purpose HTTP server" decision.

- [ ] **Step 7: Update CMake**

`cpp-routing-service/CMakeLists.txt`: add `src/metrics.cpp` to `chainroute_service_lib`'s source list (alongside the existing `chain_asset_convert.cpp`, `routing_service.cpp`). No new `find_package`/`FetchContent` entry — this task introduces zero new external dependencies, only a new source file using existing POSIX headers.

`cpp-routing-service/tests/CMakeLists.txt`: add `metrics_test.cpp` to the test executable's source list, following the exact pattern the existing `routing_service_test.cpp`/`chain_asset_convert_test.cpp` entries already use.

- [ ] **Step 8: Build and run**

```bash
cmake -S cpp-routing-service -B cpp-routing-service/build
cmake --build cpp-routing-service/build --target chainroute_service_tests
ctest --test-dir cpp-routing-service/build --output-on-failure
```

Expected: PASS, all tests including the 5 new `RouteMetricsTest` cases plus every pre-existing test.

- [ ] **Step 9: Confirm deterministic routing unaffected**

```bash
ctest --test-dir router/build --output-on-failure
```

Expected: PASS, 49/49, unchanged (this task never touches `router/`).

- [ ] **Step 10: Commit**

```bash
git add cpp-routing-service/src/metrics.hpp cpp-routing-service/src/metrics.cpp cpp-routing-service/src/routing_service.cpp cpp-routing-service/src/routing_service.hpp cpp-routing-service/src/main.cpp cpp-routing-service/CMakeLists.txt cpp-routing-service/tests/metrics_test.cpp cpp-routing-service/tests/CMakeLists.txt
git commit -m "feat(cpp-routing-service): add hand-rolled Prometheus metrics with no new dependency"
```

---

### Task 13: Docker observability stack (Prometheus + OTel Collector + Jaeger + Grafana)

**Files:**
- Create: `docker-compose.observability.yml`
- Create: `observability/prometheus/prometheus.yml`
- Create: `observability/otel-collector/config.yml`
- Create: `observability/grafana/provisioning/datasources/datasource.yml`
- Create: `observability/grafana/provisioning/dashboards/dashboard.yml`
- Create: `observability/grafana/dashboards/chainroute-overview.json`
- Modify: `.gitignore`

- [ ] **Step 1: Prometheus scrape config**

```yaml
# observability/prometheus/prometheus.yml
global:
  scrape_interval: 5s

scrape_configs:
  - job_name: chainroute-go-api
    static_configs:
      - targets: ["host.docker.internal:8080"]
        labels:
          service: go-api
  - job_name: chainroute-worker
    static_configs:
      - targets: ["host.docker.internal:9091"]
        labels:
          service: worker
  - job_name: chainroute-router
    static_configs:
      - targets: ["host.docker.internal:9102"]
        labels:
          service: router
```

- [ ] **Step 2: OTel Collector config**

```yaml
# observability/otel-collector/config.yml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317

processors:
  batch:
    timeout: 2s

exporters:
  otlp/jaeger:
    endpoint: jaeger:4317
    tls:
      insecure: true

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlp/jaeger]
```

- [ ] **Step 3: Grafana provisioning**

```yaml
# observability/grafana/provisioning/datasources/datasource.yml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: false
```

```yaml
# observability/grafana/provisioning/dashboards/dashboard.yml
apiVersion: 1
providers:
  - name: chainroute
    orgId: 1
    folder: ChainRoute
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    options:
      path: /etc/grafana/dashboards
```

- [ ] **Step 4: Dashboard JSON**

Write `observability/grafana/dashboards/chainroute-overview.json` — a hand-authored Grafana dashboard (schema version 39, one row per design doc §7 panel), with 10 panels, each a `timeseries` panel type with a single Prometheus `targets[0].expr`:

```json
{
  "title": "ChainRoute Overview",
  "uid": "chainroute-overview",
  "schemaVersion": 39,
  "version": 1,
  "timezone": "browser",
  "time": {"from": "now-1h", "to": "now"},
  "refresh": "10s",
  "panels": [
    {
      "id": 1, "title": "Payment Throughput", "type": "timeseries",
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
      "datasource": {"type": "prometheus", "uid": "Prometheus"},
      "targets": [{"expr": "sum by (execution_mode) (rate(chainroute_payments_created_total[1m]))", "legendFormat": "{{execution_mode}}"}]
    },
    {
      "id": 2, "title": "Payment Success vs Failure", "type": "timeseries",
      "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0},
      "datasource": {"type": "prometheus", "uid": "Prometheus"},
      "targets": [
        {"expr": "sum(rate(chainroute_payments_completed_total[5m]))", "legendFormat": "completed"},
        {"expr": "sum(rate(chainroute_payments_failed_total[5m]))", "legendFormat": "failed"}
      ]
    },
    {
      "id": 3, "title": "End-to-End Payment Latency (p50/p95/p99)", "type": "timeseries",
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 8},
      "datasource": {"type": "prometheus", "uid": "Prometheus"},
      "targets": [
        {"expr": "histogram_quantile(0.5, sum(rate(chainroute_payment_duration_seconds_bucket[5m])) by (le))", "legendFormat": "p50"},
        {"expr": "histogram_quantile(0.95, sum(rate(chainroute_payment_duration_seconds_bucket[5m])) by (le))", "legendFormat": "p95"},
        {"expr": "histogram_quantile(0.99, sum(rate(chainroute_payment_duration_seconds_bucket[5m])) by (le))", "legendFormat": "p99"}
      ]
    },
    {
      "id": 4, "title": "Routing Latency (p50/p95/p99)", "type": "timeseries",
      "gridPos": {"h": 8, "w": 12, "x": 12, "y": 8},
      "datasource": {"type": "prometheus", "uid": "Prometheus"},
      "targets": [
        {"expr": "histogram_quantile(0.5, sum(rate(chainroute_routing_duration_seconds_bucket[5m])) by (le))", "legendFormat": "p50"},
        {"expr": "histogram_quantile(0.95, sum(rate(chainroute_routing_duration_seconds_bucket[5m])) by (le))", "legendFormat": "p95"},
        {"expr": "histogram_quantile(0.99, sum(rate(chainroute_routing_duration_seconds_bucket[5m])) by (le))", "legendFormat": "p99"}
      ]
    },
    {
      "id": 5, "title": "Across vs Relay Selection", "type": "timeseries",
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 16},
      "datasource": {"type": "prometheus", "uid": "Prometheus"},
      "targets": [{"expr": "sum by (provider) (rate(chainroute_routing_selected_provider_total[5m]))", "legendFormat": "{{provider}}"}]
    },
    {
      "id": 6, "title": "Provider Quote Latency (p95)", "type": "timeseries",
      "gridPos": {"h": 8, "w": 12, "x": 12, "y": 16},
      "datasource": {"type": "prometheus", "uid": "Prometheus"},
      "targets": [{"expr": "histogram_quantile(0.95, sum(rate(chainroute_quote_duration_seconds_bucket[5m])) by (le, provider))", "legendFormat": "{{provider}}"}]
    },
    {
      "id": 7, "title": "Provider Quote Failures", "type": "timeseries",
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 24},
      "datasource": {"type": "prometheus", "uid": "Prometheus"},
      "targets": [{"expr": "sum by (provider, reason) (rate(chainroute_quote_failures_total[5m]))", "legendFormat": "{{provider}}/{{reason}}"}]
    },
    {
      "id": 8, "title": "Kafka/Worker Processing", "type": "timeseries",
      "gridPos": {"h": 8, "w": 12, "x": 12, "y": 24},
      "datasource": {"type": "prometheus", "uid": "Prometheus"},
      "targets": [
        {"expr": "sum(rate(chainroute_events_consumed_total[1m]))", "legendFormat": "events consumed/s"},
        {"expr": "histogram_quantile(0.95, sum(rate(chainroute_processing_duration_seconds_bucket[5m])) by (le))", "legendFormat": "p95 processing latency"}
      ]
    },
    {
      "id": 9, "title": "Blockchain Execution Outcomes", "type": "timeseries",
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 32},
      "datasource": {"type": "prometheus", "uid": "Prometheus"},
      "targets": [
        {"expr": "sum by (provider) (rate(chainroute_executions_completed_total[5m]))", "legendFormat": "{{provider}} completed"},
        {"expr": "sum by (provider) (rate(chainroute_executions_failed_total[5m]))", "legendFormat": "{{provider}} failed"}
      ]
    },
    {
      "id": 10, "title": "Payments Currently Processing", "type": "timeseries",
      "gridPos": {"h": 8, "w": 12, "x": 12, "y": 32},
      "datasource": {"type": "prometheus", "uid": "Prometheus"},
      "targets": [{"expr": "chainroute_payments_processing", "legendFormat": "{{execution_mode}}"}]
    }
  ]
}
```

- [ ] **Step 5: Docker Compose**

```yaml
# docker-compose.observability.yml
# Optional local observability stack -- ChainRoute's own server/worker/
# routing-service binaries run natively (see scripts/e2e_test.sh) and are
# fully functional without this stack running at all. Start with:
#   docker compose -f docker-compose.observability.yml up -d
services:
  prometheus:
    image: prom/prometheus:v2.55.1
    ports: ["9090:9090"]
    volumes:
      - ./observability/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    extra_hosts:
      - "host.docker.internal:host-gateway"

  otel-collector:
    image: otel/opentelemetry-collector-contrib:0.113.0
    command: ["--config=/etc/otel-collector-config.yml"]
    ports: ["4317:4317"]
    volumes:
      - ./observability/otel-collector/config.yml:/etc/otel-collector-config.yml:ro
    depends_on: [jaeger]

  jaeger:
    image: jaegertracing/all-in-one:1.63.0
    environment:
      - COLLECTOR_OTLP_ENABLED=true
    ports:
      - "16686:16686"  # Jaeger UI
      - "4318:4318"    # OTLP/HTTP (unused here, but exposed for manual debugging)

  grafana:
    image: grafana/grafana:11.3.1
    ports: ["3000:3000"]
    environment:
      - GF_AUTH_ANONYMOUS_ENABLED=true
      - GF_AUTH_ANONYMOUS_ORG_ROLE=Admin
    volumes:
      - ./observability/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./observability/grafana/dashboards:/etc/grafana/dashboards:ro
    depends_on: [prometheus]
```

Note: the OTel Collector config in Step 2 exports to `jaeger:4317` (OTLP/gRPC) — confirm this against the actual `jaegertracing/all-in-one:1.63.0` image's documented OTLP-gRPC port before running (recent Jaeger versions default to `4317`/`4318` identically to the OTel convention; if this specific tag differs, adjust the collector config's `endpoint` accordingly and note the correction in the task report — do not silently ship a config that can't actually connect).

- [ ] **Step 6: gitignore any local state**

Add to `.gitignore` (check the existing file first for a suitable place):

```
# Phase 10 observability stack local state
observability/*.local.yml
```

(Prometheus/Grafana/Jaeger in this compose file use ephemeral/in-memory or container-internal storage by default — no bind-mounted data directory is created, so no additional ignore entry is needed unless a later change adds one.)

- [ ] **Step 7: Verify the stack starts (manual check, no automated test)**

```bash
docker compose -f docker-compose.observability.yml up -d
sleep 5
curl -sf http://localhost:9090/-/healthy
curl -sf http://localhost:3000/api/health
curl -sf http://localhost:16686/
docker compose -f docker-compose.observability.yml down
```

Expected: all three curls succeed (HTTP 200-range). If Docker is not available/running in this environment, report this step as NOT RUN rather than skipping it silently, and note the config files were reviewed for correctness by inspection instead.

- [ ] **Step 8: Commit**

```bash
git add docker-compose.observability.yml observability/ .gitignore
git commit -m "feat: add local Docker observability stack (Prometheus, OTel Collector, Jaeger, Grafana)"
```

---

### Task 14: Benchmark tool for simulated-mode throughput

**Files:**
- Create: `go-api/cmd/benchmark/main.go`
- Test: `go-api/cmd/benchmark/main_test.go`

**Interfaces:**
- Consumes: the public HTTP contract of `POST /payments`/`GET /payments/{id}` (no internal package imports — this is a black-box client, deliberately, so it measures the same thing a real caller would).

- [ ] **Step 1: Write a unit test for the percentile-computation helper**

```go
package main

import (
	"testing"
	"time"
)

func TestPercentile_ComputesExpectedValues(t *testing.T) {
	durations := []time.Duration{
		10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond,
		40 * time.Millisecond, 50 * time.Millisecond, 60 * time.Millisecond,
		70 * time.Millisecond, 80 * time.Millisecond, 90 * time.Millisecond,
		100 * time.Millisecond,
	}
	p50 := percentile(durations, 0.50)
	p95 := percentile(durations, 0.95)
	p99 := percentile(durations, 0.99)

	if p50 != 50*time.Millisecond {
		t.Errorf("p50 = %v, want 50ms", p50)
	}
	if p95 != 90*time.Millisecond && p95 != 100*time.Millisecond {
		// nearest-rank percentile on a 10-element set for p95 lands on
		// index 9 (0-indexed) with ceil(0.95*10)=10th rank -> the 10th
		// element (index 9) = 100ms under a standard nearest-rank
		// definition; accept either of the two commonly-used
		// conventions rather than pin one arbitrary rounding rule.
		t.Errorf("p95 = %v, want 90ms or 100ms", p95)
	}
	if p99 != 100*time.Millisecond {
		t.Errorf("p99 = %v, want 100ms", p99)
	}
}

func TestPercentile_EmptyInputReturnsZero(t *testing.T) {
	if got := percentile(nil, 0.5); got != 0 {
		t.Errorf("percentile(nil) = %v, want 0", got)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd go-api && go test ./cmd/benchmark/... -v`
Expected: FAIL — `percentile` undefined (package doesn't exist).

- [ ] **Step 3: Implement**

```go
// go-api/cmd/benchmark/main.go
//
// Reproducible, network-free-execution load-test for ChainRoute's
// payment pipeline. Defines "processed" as reaching a terminal payment
// status (COMPLETED or FAILED) observed via GET /payments/{id} -- the
// full HTTP -> DB -> outbox -> Kafka -> worker -> terminal-state pipeline,
// not merely HTTP-accept. Reports both accept-latency and end-to-end
// latency so the distinction is never hidden (Phase 10 design doc §8).
//
// This benchmark ONLY exercises execution_mode=simulated (never testnet)
// -- it must never spend real testnet funds.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type result struct {
	acceptLatency time.Duration
	e2eLatency    time.Duration
	success       bool
}

type benchmarkOutput struct {
	Timestamp        time.Time     `json:"timestamp"`
	DurationSeconds  float64       `json:"duration_seconds"`
	Concurrency      int           `json:"concurrency"`
	BaseURL          string        `json:"base_url"`
	Hardware         hardwareInfo  `json:"hardware"`
	TotalRequests    int64         `json:"total_requests"`
	Successful       int64         `json:"successful"`
	Failed           int64         `json:"failed"`
	ThroughputAccept float64       `json:"throughput_accept_per_sec"`
	ThroughputProcessed float64    `json:"throughput_processed_per_sec"`
	AcceptLatencyMs  latencyReport `json:"accept_latency_ms"`
	E2ELatencyMs     latencyReport `json:"e2e_latency_ms"`
}

type hardwareInfo struct {
	NumCPU int    `json:"num_cpu"`
	GOOS   string `json:"goos"`
	GOARCH string `json:"goarch"`
}

type latencyReport struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

func percentile(durations []time.Duration, p float64) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted))*p + 0.9999) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func main() {
	duration := flag.Duration("duration", 30*time.Second, "benchmark duration")
	concurrency := flag.Int("concurrency", runtime.GOMAXPROCS(0), "number of concurrent workers")
	baseURL := flag.String("base-url", "http://localhost:8099", "ChainRoute server base URL")
	outPath := flag.String("out", "", "path to write machine-readable JSON result (optional)")
	flag.Parse()

	fmt.Printf("ChainRoute payment pipeline benchmark\n")
	fmt.Printf("  duration=%s concurrency=%d base_url=%s\n", *duration, *concurrency, *baseURL)
	fmt.Printf("  mode: simulated (network-free execution; no real blockchain spending)\n\n")

	client := &http.Client{Timeout: 15 * time.Second}
	stop := time.Now().Add(*duration)

	var total, successful, failed int64
	resultsCh := make(chan result, 10000)
	var wg sync.WaitGroup
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			seq := 0
			for time.Now().Before(stop) {
				seq++
				idempotencyKey := fmt.Sprintf("bench-%d-%d-%d", workerID, seq, time.Now().UnixNano())
				r, ok := runOnePayment(client, *baseURL, idempotencyKey)
				atomic.AddInt64(&total, 1)
				if ok {
					atomic.AddInt64(&successful, 1)
				} else {
					atomic.AddInt64(&failed, 1)
				}
				resultsCh <- r
			}
		}(w)
	}
	wg.Wait()
	close(resultsCh)

	var acceptLatencies, e2eLatencies []time.Duration
	for r := range resultsCh {
		acceptLatencies = append(acceptLatencies, r.acceptLatency)
		if r.success {
			e2eLatencies = append(e2eLatencies, r.e2eLatency)
		}
	}

	elapsed := time.Since(stop.Add(-*duration)).Seconds()
	out := benchmarkOutput{
		Timestamp: time.Now().UTC(), DurationSeconds: elapsed, Concurrency: *concurrency, BaseURL: *baseURL,
		Hardware:      hardwareInfo{NumCPU: runtime.NumCPU(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH},
		TotalRequests: total, Successful: successful, Failed: failed,
		ThroughputAccept:    float64(total) / elapsed,
		ThroughputProcessed: float64(successful) / elapsed,
		AcceptLatencyMs: latencyReport{
			P50: float64(percentile(acceptLatencies, 0.50).Microseconds()) / 1000,
			P95: float64(percentile(acceptLatencies, 0.95).Microseconds()) / 1000,
			P99: float64(percentile(acceptLatencies, 0.99).Microseconds()) / 1000,
		},
		E2ELatencyMs: latencyReport{
			P50: float64(percentile(e2eLatencies, 0.50).Microseconds()) / 1000,
			P95: float64(percentile(e2eLatencies, 0.95).Microseconds()) / 1000,
			P99: float64(percentile(e2eLatencies, 0.99).Microseconds()) / 1000,
		},
	}

	fmt.Printf("Total requests:        %d\n", out.TotalRequests)
	fmt.Printf("Successful (processed):%d\n", out.Successful)
	fmt.Printf("Failed:                %d\n", out.Failed)
	fmt.Printf("Throughput (accept):   %.2f req/sec\n", out.ThroughputAccept)
	fmt.Printf("Throughput (processed):%.2f payments/sec\n", out.ThroughputProcessed)
	fmt.Printf("Accept latency:  p50=%.1fms p95=%.1fms p99=%.1fms\n", out.AcceptLatencyMs.P50, out.AcceptLatencyMs.P95, out.AcceptLatencyMs.P99)
	fmt.Printf("E2E latency:     p50=%.1fms p95=%.1fms p99=%.1fms\n", out.E2ELatencyMs.P50, out.E2ELatencyMs.P95, out.E2ELatencyMs.P99)

	if *outPath != "" {
		data, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to marshal result: %v\n", err)
			os.Exit(1)
		}
		if err := os.WriteFile(*outPath, data, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write result to %s: %v\n", *outPath, err)
			os.Exit(1)
		}
		fmt.Printf("\nMachine-readable result written to %s\n", *outPath)
	}
}

// runOnePayment posts one simulated-mode payment and polls until terminal
// (COMPLETED/FAILED) or a bounded number of retries elapses. success=true
// only if the payment reached COMPLETED -- reaching FAILED is a "failed"
// outcome for the benchmark's own success/failure counters, distinct
// from an HTTP/transport error, both of which count as failed=true here.
func runOnePayment(client *http.Client, baseURL, idempotencyKey string) (result, bool) {
	body := []byte(`{"source_chain":"ethereum","destination_chain":"base","asset":"usdc","amount":"100"}`)
	acceptStart := time.Now()
	req, _ := http.NewRequest(http.MethodPost, baseURL+"/payments", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idempotencyKey)
	resp, err := client.Do(req)
	acceptLatency := time.Since(acceptStart)
	if err != nil || (resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK) {
		if resp != nil {
			resp.Body.Close()
		}
		return result{acceptLatency: acceptLatency, success: false}, false
	}
	var created struct {
		ID string `json:"id"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&created)
	resp.Body.Close()

	e2eStart := time.Now()
	for i := 0; i < 100; i++ {
		time.Sleep(50 * time.Millisecond)
		getResp, err := client.Get(baseURL + "/payments/" + created.ID)
		if err != nil {
			continue
		}
		var payment struct {
			Status string `json:"status"`
		}
		_ = json.NewDecoder(getResp.Body).Decode(&payment)
		getResp.Body.Close()
		if payment.Status == "COMPLETED" {
			return result{acceptLatency: acceptLatency, e2eLatency: time.Since(e2eStart), success: true}, true
		}
		if payment.Status == "FAILED" {
			return result{acceptLatency: acceptLatency, e2eLatency: time.Since(e2eStart), success: false}, false
		}
	}
	return result{acceptLatency: acceptLatency, success: false}, false
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd go-api && go test ./cmd/benchmark/... -v`
Expected: PASS.

- [ ] **Step 5: Build the binary**

```bash
cd go-api && go build -o /tmp/chainroute-benchmark ./cmd/benchmark
```

Expected: builds cleanly. Running it for real against a live server is done in the plan's final regression pass (after Task 15), not as part of this task's own commit step — this task only needs the tool to exist, compile, and have its pure logic unit-tested.

- [ ] **Step 6: Commit**

```bash
git add go-api/cmd/benchmark/main.go go-api/cmd/benchmark/main_test.go
git commit -m "feat(go-api): add reproducible simulated-mode payment pipeline benchmark"
```

---

### Task 15: Documentation

**Files:**
- Modify: `README.md`

**Interfaces:** none (final task).

- [ ] **Step 1: Write `README.md`**

`README.md` is currently empty. Write a complete top-level README covering (this is the one task in this plan where the "step" is prose, not code — every subsection below must be written out in full, not left as a heading):

1. **What ChainRoute is** — one paragraph: a cross-chain payment router comparing live Across/Relay bridge quotes, routed by a C++ Dijkstra service, executed and reconciled by a Go worker, with Postgres persistence and Kafka-based async processing.
2. **Architecture** — a short prose flow: `HTTP (Go server) -> quote providers (Across/Relay) -> gRPC -> C++ router -> Postgres (+ outbox) -> Kafka -> worker -> blockchain execution -> reconciliation`.
3. **Running locally** — how to build/run the Go server, worker, and C++ service natively (point at `scripts/e2e_test.sh` as the canonical example, don't duplicate its logic in prose).
4. **Observability** (the new section this task exists to add):
   - Architecture: a text diagram showing where metrics/traces/logs are generated —
     ```
     Go server/worker  --/metrics-->  Prometheus (:9090, scrapes :8080, :9091)
     C++ router        --/metrics-->  Prometheus (scrapes :9102)
     Go server/worker  --OTLP/gRPC--> OTel Collector (:4317) --> Jaeger (:16686)
     Go server/worker  --stdout-->    structured JSON logs (log/slog)
     ```
   - How to start the stack: `docker compose -f docker-compose.observability.yml up -d`
   - Prometheus endpoints: go-api `:8080/metrics`, worker `:9091/metrics` (configurable via `METRICS_ADDR`), C++ router `:9102/metrics` (configurable via `METRICS_PORT`); Prometheus UI at `:9090`.
   - Grafana: `http://localhost:3000` (anonymous admin access in this local-only compose file — explicitly note this is NOT a production configuration), dashboard auto-provisioned as "ChainRoute Overview."
   - Tracing: set `OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317` (the default) to export to the local Collector; view traces at the Jaeger UI, `http://localhost:16686`. Explain the Jaeger-over-Tempo choice in one sentence (no object-storage backend needed for local dev).
   - Important metrics: list the ~10 metric names from Task 2/12's catalogue in one table, one line each: name, type, labels, meaning.
   - How to run the benchmark: `go run ./go-api/cmd/benchmark -duration=30s -concurrency=8 -out=/tmp/result.json`, with the server (and, if testing the full pipeline, the worker + Kafka) already running in simulated mode.
   - How to interpret benchmark output: explain the accept-vs-processed distinction explicitly (a payment is "processed" only once it reaches COMPLETED/FAILED via polling `GET /payments/{id}` — not merely accepted by `POST /payments`), and that the reported number is whatever this specific run measured, on whatever hardware `hardware` in the JSON output names — never a claimed universal number.
5. **Environment variables** — a table covering everything currently in `go-api/.env.example` PLUS the new ones this phase adds (`OTEL_EXPORTER_OTLP_ENDPOINT`, `METRICS_ADDR`, `METRICS_PORT`) plus the ones the survey found undocumented (`KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_CONSUMER_GROUP`, `OUTBOX_POLL_INTERVAL_MS`, `WORKER_RECOVERY_SWEEP_INTERVAL_SECONDS`, `WORKER_RECOVERY_STALENESS_SECONDS`, `TESTNET_HANDLE_TIMEOUT_SECONDS`, `RELAY_TESTNET_API_URL`, `RELAY_API_KEY`, `RELAY_DEPOSIT_CONTRACT_SEPOLIA`) — opportunistic gap-fill the design doc flagged, not scope creep, since it costs nothing extra once this README is being written anyway.

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add README covering architecture, observability stack, and benchmark usage"
```

---

## Known, disclosed scope boundary

One `log.Printf` call site (`go-api/internal/bridge/relay/status.go`, the "unrecognized status" warning added during Phase 9) is deliberately NOT converted to structured logging by this plan: `relay.Provider` is a leaf quote/execution/status implementation with no `*slog.Logger` field and no natural place to receive one without threading a logger parameter into every `quote.Provider`/`quote.Signer`/`quote.StatusChecker` constructor across both `across` and `relay` packages — a disproportionate change for one log line. This is a conscious, disclosed scope boundary, not an oversight; note it in the final report's "remaining limitations" rather than silently leaving it unmentioned.

## Post-implementation (controller responsibility, not a numbered task)

After Task 15, run the full regression suite exactly as the user's own "Implementation process" §5 specifies (formatting, builds, unit tests, PostgreSQL integration tests, Kafka/Redpanda tests, existing E2E tests), then run the Task 14 benchmark for real against a running simulated-mode server+worker+Postgres+Kafka stack and report the ACTUAL measured numbers (never fabricated), then dispatch the final whole-branch review per this session's established subagent-driven-development pattern, covering (in addition to the standard checklist): no secrets in any log/metric/trace attribute; no unbounded Prometheus label; simulated mode still fully network-free with the observability stack absent; every span/metric call site is unconditional and cannot itself cause a payment-path error; the C++ metrics listener doesn't introduce a routing behavior change; the benchmark's methodology matches what the README claims. Produce the final summary in the exact format requested (architecture added, files changed, metrics added, traces added, dashboard panels, tests executed/results, benchmark methodology/results, remaining limitations). Do not merge or push — stop after implementation, testing, and final review, per explicit user instruction.
