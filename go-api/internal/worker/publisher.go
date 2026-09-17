package worker

import (
	"context"
	"log/slog"
	"time"

	"chainroute/go-api/internal/observability"
	"chainroute/go-api/internal/postgres"
)

// OutboxStore is the subset of *postgres.Store the publisher needs.
type OutboxStore interface {
	PublishNextOutboxEvent(ctx context.Context, publish func(postgres.OutboxEvent) error) (bool, error)
}

// Publisher polls the outbox and publishes unpublished events to Kafka,
// keyed by payment ID so the underlying claim transaction (see the Phase 6
// design spec §5) can mark the row published only after Publish succeeds.
type Publisher struct {
	Store   OutboxStore
	Publish func(ctx context.Context, key string, value []byte) error
	Metrics *observability.Metrics
	Logger  *slog.Logger
}

// metrics returns p.Metrics, or a shared safe-to-record-into default when
// it is nil -- e.g. for an existing test's struct literal that predates
// this phase and never sets the field. Every instrumentation call in this
// file must go through this accessor, never through p.Metrics directly,
// so that a nil Metrics field can never nil-pointer-panic.
func (p *Publisher) metrics() *observability.Metrics {
	if p.Metrics != nil {
		return p.Metrics
	}
	return observability.DefaultMetrics()
}

// logger mirrors metrics: it returns p.Logger, or a shared default when
// nil. Every log call in this file must go through this accessor, never
// through p.Logger directly.
func (p *Publisher) logger() *slog.Logger {
	if p.Logger != nil {
		return p.Logger
	}
	return observability.DefaultLogger()
}

// PollOnce attempts to publish the next unpublished outbox event, if any.
func (p *Publisher) PollOnce(ctx context.Context) (bool, error) {
	ctx, span := observability.Tracer("outbox").Start(ctx, "outbox.publish")
	defer span.End()
	published, err := p.Store.PublishNextOutboxEvent(ctx, func(evt postgres.OutboxEvent) error {
		return p.Publish(ctx, evt.PaymentID, evt.Payload)
	})
	if published && err == nil {
		p.metrics().EventsPublished.Inc()
	}
	return published, err
}

// Run polls on the given interval until ctx is done.
func (p *Publisher) Run(ctx context.Context, pollInterval time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		published, err := p.PollOnce(ctx)
		if err != nil {
			p.logger().ErrorContext(ctx, "outbox publish failed", "error", err)
		}
		if err != nil || !published {
			select {
			case <-ctx.Done():
				return
			case <-time.After(pollInterval):
			}
		}
	}
}
