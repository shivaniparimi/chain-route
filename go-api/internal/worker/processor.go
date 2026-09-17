package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"chainroute/go-api/internal/events"
	"chainroute/go-api/internal/execution"
	"chainroute/go-api/internal/observability"
	"chainroute/go-api/internal/payment"
)

// defaultTestnetHandleTimeout is used when Processor.TestnetTimeout is left
// at its zero value (e.g. in tests that don't care about timing). Sized
// comfortably larger than across.Client's own 15-second HTTP timeout plus
// the handful of additional EVM RPC round trips DriveExecutionForward makes
// (review Finding 4).
const defaultTestnetHandleTimeout = 30 * time.Second

// PaymentStore is the subset of *postgres.Store the processor needs.
type PaymentStore interface {
	ClaimPayment(ctx context.Context, paymentID string) (bool, payment.ExecutionMode, error)
	CompletePayment(ctx context.Context, paymentID string, terminal payment.Status) (bool, time.Time, error)
}

// TestnetExecutor is the subset of *Executor Processor needs -- kept as an
// interface so processor_test.go can fake it without constructing a real
// wallet/RPC/Across client.
type TestnetExecutor interface {
	ExecuteTestnetPayment(ctx context.Context, paymentID string) error
}

// Processor handles one PAYMENT_ROUTED event at a time. Executor is nil
// when BLOCKCHAIN_ENV != testnet (cmd/worker only wires it when testnet
// execution is actually enabled) -- HandleRoutedPayment must never reach
// the testnet branch in that configuration, because the API layer (Task
// 15) refuses to create execution_mode="testnet" payments unless the
// server itself is configured for testnet, so no ROUTED testnet-mode
// payment can exist for Kafka to ever deliver in the first place.
type Processor struct {
	Store    PaymentStore
	Executor TestnetExecutor

	// TestnetTimeout bounds testnet-mode execution (ExecuteTestnetPayment)
	// with its own, independent context, distinct from whatever context the
	// caller passed into HandleRoutedPayment (cmd/worker's runConsumeLoop
	// uses a fixed 10-second handleCtx sized for Phase 6's simulated path,
	// which is too small for testnet execution's several sequential network
	// round trips -- review Finding 4). Zero value falls back to
	// defaultTestnetHandleTimeout. Simulated-mode handling is intentionally
	// untouched by this field and keeps using the caller's own context.
	TestnetTimeout time.Duration

	Metrics *observability.Metrics
	Logger  *slog.Logger
}

// metrics returns p.Metrics, or a shared safe-to-record-into default when
// it is nil -- e.g. for an existing test's struct literal that predates
// this phase and never sets the field. Every instrumentation call in this
// file must go through this accessor, never through p.Metrics directly,
// so that a nil Metrics field can never nil-pointer-panic.
func (p *Processor) metrics() *observability.Metrics {
	if p.Metrics != nil {
		return p.Metrics
	}
	return observability.DefaultMetrics()
}

// logger mirrors metrics: it returns p.Logger, or a shared default when
// nil. Every log call in this file must go through this accessor, never
// through p.Logger directly.
func (p *Processor) logger() *slog.Logger {
	if p.Logger != nil {
		return p.Logger
	}
	return observability.DefaultLogger()
}

// HandleRoutedPayment claims the payment and, only if the claim succeeds,
// runs execution and persists the terminal outcome. If the claim is a
// no-op (the payment is already PROCESSING or terminal), it returns
// immediately WITHOUT calling execution.Execute -- this is what keeps a
// tight duplicate-delivery loop (the same event handled twice back to
// back) from ever invoking Execute more than once. A genuinely stalled
// worker racing the recovery sweep is a different, accepted case (see the
// Phase 6 design spec §7, §11) that this function does not need to
// special-case: whichever of the two wins CompletePayment's guard is the
// one that persists.
func (p *Processor) HandleRoutedPayment(ctx context.Context, evt events.RoutedPayment) error {
	carrier := propagation.MapCarrier(evt.TraceCarrier)
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)
	ctx, span := observability.Tracer("kafka").Start(ctx, "kafka.process")
	defer span.End()
	span.SetAttributes(attribute.String("payment.id", evt.PaymentID))

	start := time.Now()
	p.metrics().EventsConsumed.Inc()

	claimed, mode, err := p.Store.ClaimPayment(ctx, evt.PaymentID)
	if err != nil {
		p.metrics().ProcessingFailures.WithLabelValues("unknown", "claim_conflict").Inc()
		return fmt.Errorf("claim payment %s: %w", evt.PaymentID, err)
	}
	if !claimed {
		return nil
	}
	span.SetAttributes(attribute.String("payment.execution_mode", string(mode)))

	if mode == payment.ExecutionModeTestnet {
		if p.Executor == nil {
			p.metrics().ProcessingFailures.WithLabelValues(string(mode), "execution_error").Inc()
			return fmt.Errorf("payment %s is execution_mode=testnet but this worker has no Executor configured (BLOCKCHAIN_ENV != testnet) -- this should be unreachable if the API layer's testnet gate is working", evt.PaymentID)
		}
		timeout := p.TestnetTimeout
		if timeout <= 0 {
			timeout = defaultTestnetHandleTimeout
		}
		// Deliberately a fresh, independent context here, not a child of
		// ctx: the caller's ctx (cmd/worker's handleCtx) is sized for the
		// Phase 6 simulated path and may already have little budget left by
		// the time ClaimPayment returns -- testnet execution needs its own
		// larger, independent timeout (review Finding 4), not a truncation
		// of whatever remains of the caller's. The trace SPAN CONTEXT is
		// still carried forward via trace.ContextWithSpanContext so the
		// child execution.run span nests under this kafka.process trace
		// rather than starting a new, disconnected one -- this is
		// orthogonal to the deadline, which stays independent.
		testnetCtx := trace.ContextWithSpanContext(context.Background(), trace.SpanContextFromContext(ctx))
		testnetCtx, cancel := context.WithTimeout(testnetCtx, timeout)
		defer cancel()
		err := p.Executor.ExecuteTestnetPayment(testnetCtx, evt.PaymentID)
		p.metrics().ProcessingDuration.WithLabelValues(string(mode)).Observe(time.Since(start).Seconds())
		if err != nil {
			p.metrics().ProcessingFailures.WithLabelValues(string(mode), "execution_error").Inc()
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
	p.metrics().ProcessingDuration.WithLabelValues(string(mode)).Observe(time.Since(start).Seconds())
	if err != nil {
		p.metrics().ProcessingFailures.WithLabelValues(string(mode), "execution_error").Inc()
		return fmt.Errorf("complete payment %s: %w", evt.PaymentID, err)
	}
	if completed {
		p.metrics().PaymentDuration.WithLabelValues(string(mode), outcome).Observe(time.Since(createdAt).Seconds())
		p.metrics().PaymentsProcessing.WithLabelValues(string(mode)).Dec()
		if terminal == payment.StatusCompleted {
			p.metrics().PaymentsCompleted.WithLabelValues(string(mode)).Inc()
		} else {
			p.metrics().PaymentsFailed.WithLabelValues(string(mode), "execution").Inc()
		}
	}
	return nil
}
