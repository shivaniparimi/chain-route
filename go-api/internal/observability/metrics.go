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
	PaymentsCreated    *prometheus.CounterVec   // labels: execution_mode
	PaymentsCompleted  *prometheus.CounterVec   // labels: execution_mode
	PaymentsFailed     *prometheus.CounterVec   // labels: execution_mode, failure_reason_class
	PaymentsProcessing *prometheus.GaugeVec     // labels: execution_mode
	PaymentDuration    *prometheus.HistogramVec // labels: execution_mode, outcome

	// Routing
	RoutingRequests         *prometheus.CounterVec   // labels: execution_mode
	RoutingDuration         *prometheus.HistogramVec // labels: execution_mode
	RoutingFailures         *prometheus.CounterVec   // labels: reason
	RoutingSelectedProvider *prometheus.CounterVec   // labels: provider
	RoutingSelectedFee      *prometheus.HistogramVec // labels: provider

	// Bridge providers (Across + Relay, differentiated only by the
	// "provider" label -- never a per-provider metric family)
	QuoteRequests  *prometheus.CounterVec   // labels: provider
	QuoteFailures  *prometheus.CounterVec   // labels: provider, reason
	QuoteDuration  *prometheus.HistogramVec // labels: provider
	QuoteAvailable *prometheus.CounterVec   // labels: provider, available
	QuoteSelected  *prometheus.CounterVec   // labels: provider

	// Kafka / worker
	EventsPublished    prometheus.Counter
	EventsConsumed     prometheus.Counter
	ProcessingDuration *prometheus.HistogramVec // labels: execution_mode
	ProcessingFailures *prometheus.CounterVec   // labels: execution_mode, reason
	StaleRecoveries    *prometheus.CounterVec   // labels: source

	// Blockchain execution
	ExecutionAttempts      *prometheus.CounterVec   // labels: provider
	Broadcasts             *prometheus.CounterVec   // labels: provider
	Reconciliations        *prometheus.CounterVec   // labels: provider
	ExecutionsCompleted    *prometheus.CounterVec   // labels: provider
	ExecutionsFailed       *prometheus.CounterVec   // labels: provider, reason
	ExecutionDuration      *prometheus.HistogramVec // labels: provider
	ReconciliationDuration *prometheus.HistogramVec // labels: provider
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
