package events

import "time"

// RoutedPaymentEventType is the only Kafka event type Phase 6 produces.
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
