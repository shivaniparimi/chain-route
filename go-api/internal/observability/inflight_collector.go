package observability

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// InFlightCounter is the narrow interface PaymentsInFlightCollector needs:
// one query against the authoritative PostgreSQL payment-state table,
// grouped by execution_mode. Implemented by *postgres.Store.
type InFlightCounter interface {
	CountInFlightPayments(ctx context.Context) (map[string]int64, error)
}

// knownExecutionModes is the closed, bounded label-value set
// PaymentsInFlightCollector ever emits -- always both, even when a mode's
// count is zero, so "no active payments" reports a real 0 series rather
// than an absent one. Mirrors payment.ExecutionModeSimulated/Testnet.
var knownExecutionModes = []string{"simulated", "testnet"}

// collectTimeout bounds the PostgreSQL query Collect issues on every
// Prometheus scrape, so a slow/unreachable database delays one scrape
// rather than hanging it indefinitely.
const collectTimeout = 5 * time.Second

// PaymentsInFlightCollector reports chainroute_payments_processing by
// querying PostgreSQL directly on every Prometheus scrape, instead of
// maintaining an in-memory gauge mutated by Inc()/Dec() calls scattered
// across two separate OS processes (the Go API server and the worker).
// That older design was fundamentally broken: each process holds its own
// private Prometheus registry, so the server's own series only ever grew
// (Inc() on creation) and the worker's own series only ever fell (Dec()
// on completion) -- neither series, nor even summing them across scrape
// targets, reflects the true in-flight count once either process
// restarts and its in-memory counter resets to zero while the other's
// does not.
//
// Deriving the value fresh from PostgreSQL on every scrape has none of
// these problems: it is correct immediately after either process
// restarts (there is no per-process state to reset), it can never go
// negative (COUNT(*) cannot), and registering it in exactly one process
// (the API server, which already holds the Store this needs) makes it
// the single authoritative source instead of two racing ones. Multiple
// API server replicas would each report the identical, correct value
// (see the Grafana panel's `max by` query, not `sum by` -- summing
// identical values across replicas would multiply the count).
type PaymentsInFlightCollector struct {
	Store  InFlightCounter
	Logger *slog.Logger

	desc *prometheus.Desc
}

// NewPaymentsInFlightCollector constructs a collector for the given
// store. logger may be nil, in which case DefaultLogger() is used.
func NewPaymentsInFlightCollector(store InFlightCounter, logger *slog.Logger) *PaymentsInFlightCollector {
	if logger == nil {
		logger = DefaultLogger()
	}
	return &PaymentsInFlightCollector{
		Store:  store,
		Logger: logger,
		desc: prometheus.NewDesc(
			"chainroute_payments_processing",
			"Payments currently in a non-terminal status (ROUTED, PROCESSING, or SUBMITTED), queried live from PostgreSQL on every scrape.",
			[]string{"execution_mode"}, nil,
		),
	}
}

func (c *PaymentsInFlightCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect queries PostgreSQL for the current in-flight count per
// execution_mode and emits one gauge sample per known mode (0 when a mode
// has no in-flight payments). A query failure is logged and Collect
// simply emits nothing for this scrape -- it must never panic or block
// the rest of the /metrics response.
func (c *PaymentsInFlightCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), collectTimeout)
	defer cancel()

	counts, err := c.Store.CountInFlightPayments(ctx)
	if err != nil {
		c.Logger.Error("failed to query in-flight payment counts", "error", err)
		return
	}
	for _, mode := range knownExecutionModes {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(counts[mode]), mode)
	}
}
