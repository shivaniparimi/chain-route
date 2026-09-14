package worker

import (
	"context"
	"log"
	"time"

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
}

// PollOnce attempts to publish the next unpublished outbox event, if any.
func (p *Publisher) PollOnce(ctx context.Context) (bool, error) {
	return p.Store.PublishNextOutboxEvent(ctx, func(evt postgres.OutboxEvent) error {
		return p.Publish(ctx, evt.PaymentID, evt.Payload)
	})
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
			log.Printf("ERROR: outbox publish failed: %v", err)
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
