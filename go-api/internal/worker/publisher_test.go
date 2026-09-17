package worker

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"chainroute/go-api/internal/observability"
	"chainroute/go-api/internal/postgres"
)

type fakeOutboxStore struct {
	publishResult bool
	publishErr    error
	calls         int
}

func (f *fakeOutboxStore) PublishNextOutboxEvent(ctx context.Context, publish func(postgres.OutboxEvent) error) (bool, error) {
	f.calls++
	if f.publishErr != nil {
		return false, f.publishErr
	}
	if f.publishResult {
		if err := publish(postgres.OutboxEvent{
			ID: "evt-1", PaymentID: "pay-1", EventType: "PAYMENT_ROUTED", Payload: []byte(`{}`),
		}); err != nil {
			return false, err
		}
	}
	return f.publishResult, nil
}

func TestPollOnce_CallsPublishWithEventFields(t *testing.T) {
	var gotKey string
	var gotValue []byte
	store := &fakeOutboxStore{publishResult: true}
	p := &Publisher{
		Store: store,
		Publish: func(ctx context.Context, key string, value []byte) error {
			gotKey = key
			gotValue = value
			return nil
		},
	}
	published, err := p.PollOnce(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if !published {
		t.Fatal("expected published=true")
	}
	if gotKey != "pay-1" {
		t.Fatalf("expected key pay-1, got %q", gotKey)
	}
	if string(gotValue) != "{}" {
		t.Fatalf("unexpected value: %s", gotValue)
	}
}

func TestPollOnce_NoRowsReturnsFalse(t *testing.T) {
	store := &fakeOutboxStore{publishResult: false}
	p := &Publisher{Store: store, Publish: func(context.Context, string, []byte) error { return nil }}
	published, err := p.PollOnce(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if published {
		t.Fatal("expected published=false")
	}
}

func TestPollOnce_PublishErrorPropagates(t *testing.T) {
	store := &fakeOutboxStore{publishResult: true}
	p := &Publisher{
		Store:   store,
		Publish: func(context.Context, string, []byte) error { return context.DeadlineExceeded },
	}
	if _, err := p.PollOnce(context.Background()); err == nil {
		t.Fatal("expected an error to propagate from a failing publish")
	}
}

func TestPublisher_PollOnce_IncrementsEventsPublished(t *testing.T) {
	metrics := observability.NewMetrics()
	store := &fakeOutboxStore{publishResult: true}
	p := &Publisher{Store: store, Publish: func(context.Context, string, []byte) error { return nil }, Metrics: metrics}

	if _, err := p.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}
	if got := testutil.ToFloat64(metrics.EventsPublished); got != 1 {
		t.Errorf("EventsPublished = %v, want 1", got)
	}
}
