package events

import "time"

// RoutedPaymentEventType is the only Kafka event type Phase 6 produces.
const RoutedPaymentEventType = "PAYMENT_ROUTED"

// RoutedPayment is the thin Kafka payload for a routed payment. It carries
// only payment_id plus metadata -- PostgreSQL remains the source of truth,
// and the consumer always re-reads current state before acting.
type RoutedPayment struct {
	PaymentID  string    `json:"payment_id"`
	EventType  string    `json:"event_type"`
	OccurredAt time.Time `json:"occurred_at"`
}
