package worker

import (
	"context"
	"testing"

	"encoding/json"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"chainroute/go-api/internal/events"
	"chainroute/go-api/internal/observability"
	"chainroute/go-api/internal/postgres"
)

type fakeOutboxStore struct {
	publishResult bool
	publishErr    error
	calls         int
	// payload is the OutboxEvent.Payload handed to the publish closure when
	// publishResult is true. Defaults to an empty JSON object when unset.
	payload []byte
}

func (f *fakeOutboxStore) PublishNextOutboxEvent(ctx context.Context, publish func(postgres.OutboxEvent) error) (bool, error) {
	f.calls++
	if f.publishErr != nil {
		return false, f.publishErr
	}
	if f.publishResult {
		payload := f.payload
		if payload == nil {
			payload = []byte(`{}`)
		}
		if err := publish(postgres.OutboxEvent{
			ID: "evt-1", PaymentID: "pay-1", EventType: "PAYMENT_ROUTED", Payload: payload,
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

// TestPollOnce_NoSpanWhenNothingToPublish guards against the outbox.publish
// span being started unconditionally on every poll tick -- at the default
// poll interval, most ticks find nothing to publish, and starting a span
// on every one of them produces a large volume of disconnected noise
// root-spans with no real work behind them.
func TestPollOnce_NoSpanWhenNothingToPublish(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prevTP := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prevTP)

	store := &fakeOutboxStore{publishResult: false}
	p := &Publisher{Store: store, Publish: func(context.Context, string, []byte) error { return nil }}

	published, err := p.PollOnce(context.Background())
	if err != nil {
		t.Fatalf("PollOnce: %v", err)
	}
	if published {
		t.Fatal("expected published=false")
	}
	if got := len(sr.Ended()); got != 0 {
		t.Fatalf("expected no spans recorded when there was nothing to publish, got %d", got)
	}
}

// TestPollOnce_CreatesChildSpanWhenPublishing confirms that when there IS
// something to publish, PollOnce starts exactly one outbox.publish span,
// and that it is a child of the trace context extracted from the outbox
// event's own TraceCarrier (the same field store.go populates at payment
// creation and Processor.HandleRoutedPayment extracts on the consume
// side) -- not a new, disconnected root trace.
func TestPollOnce_CreatesChildSpanWhenPublishing(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	}()

	// Build a parent trace (using a separate tracer name so it's easy to
	// tell apart from the outbox.publish span below) and inject it into a
	// carrier the same way store.go does when it creates the outbox row
	// alongside the payment. This parent span is itself recorded by sr,
	// same as outbox.publish will be -- the assertions below pick out
	// outbox.publish by name rather than assuming it's the only span
	// recorded.
	parentCtx, parentSpan := tp.Tracer("test").Start(context.Background(), "payment.create")
	wantTraceID := parentSpan.SpanContext().TraceID()
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(parentCtx, carrier)
	parentSpan.End()

	payload, err := json.Marshal(events.RoutedPayment{
		PaymentID:    "pay-1",
		EventType:    events.RoutedPaymentEventType,
		TraceCarrier: carrier,
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	store := &fakeOutboxStore{publishResult: true, payload: payload}
	p := &Publisher{Store: store, Publish: func(context.Context, string, []byte) error { return nil }}

	if _, err := p.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	var publishSpans int
	var got sdktrace.ReadOnlySpan
	for _, s := range sr.Ended() {
		if s.Name() == "outbox.publish" {
			publishSpans++
			got = s
		}
	}
	if publishSpans != 1 {
		t.Fatalf("expected exactly one outbox.publish span recorded, got %d", publishSpans)
	}
	if got.Parent().TraceID() != wantTraceID {
		t.Fatalf("expected span to be a child of the extracted trace %s, got parent trace ID %s", wantTraceID, got.Parent().TraceID())
	}
}
