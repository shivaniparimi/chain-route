# ChainRoute full-system integration test and throughput benchmark

**Date:** 2026-09-19
**Scope:** Phases 1-12, run against `main` after merging the Phase 12 dashboard branch.

## Environment

Docker Desktop's daemon was unreachable for most of this session (a single
image-build attempt filled the host disk and left the daemon wedged after
restart) and was not pursued further to avoid risking other, unrelated
Docker state on this machine. The full stack was instead run **natively**:

- PostgreSQL 16 (Homebrew service, already running; migrations 0001-0008
  confirmed applied via `scripts/apply_migrations.sh`)
- Apache Kafka 4.3.1 in KRaft mode (Homebrew `kafka` formula, no Docker/
  Zookeeper needed) — a real Kafka broker, not Redpanda, but wire-compatible
  with `go-api`'s `segmentio/kafka-go` client the same way Redpanda is
  in the Docker Compose setup
- Prometheus 3.14.0 and Grafana 13.2.2 (Homebrew formulas), provisioned with
  this repo's own `observability/prometheus/prometheus.yml` and
  `observability/grafana/provisioning/*` (paths substituted from
  `host.docker.internal` to `localhost` for native execution)
- `cpp-routing-service`, `go-api/cmd/server`, `go-api/cmd/worker` built
  natively from `main` and run as plain OS processes
- `frontend` built for production (`npm run build`) and served via
  `vite preview`

**Not verified in this environment:** OpenTelemetry Collector / Jaeger
(no Homebrew formula, Docker unavailable) — trace EXPORT from the Go
services was confirmed to fail open (retries every few seconds, logs a
warning, never blocks a request), but no trace was ever actually observed
landing in a tracing backend. A real Docker image build was never
attempted successfully for any of this phase's images.

## Bottleneck found and fixed

The existing Phase 10 benchmark tool (`go-api/cmd/benchmark`) already
measures true end-to-end **completed** payments (submit, then poll
`GET /payments/{id}` until `COMPLETED`/`FAILED`), not HTTP accept —
no changes were needed to what it measures.

Initial sweep (concurrency 1/4/8/16/32/64, 30s each) showed throughput
plateau at **~75-78 payments/sec** from concurrency 8 onward, while E2E
latency kept climbing (p50 101ms at c=8 → p50 718ms at c=64) and Kafka
consumer lag stayed at 0 throughout, and worker process CPU stayed under
1%. A flat throughput ceiling with idle CPU and zero broker lag rules out
Kafka/consumer capacity as the constraint.

Reading `internal/worker/publisher.go` found the actual constraint:
`Publisher.PollOnce` claims and publishes **exactly one** outbox row per
call (`PublishNextOutboxEvent`'s `FOR UPDATE SKIP LOCKED LIMIT 1`), one
full DB-claim + synchronous Kafka-produce round trip at a time, on a single
goroutine. This is the pipeline's real serialization point — not the
consumer, which was already keeping up easily.

`PublishNextOutboxEvent`'s `FOR UPDATE SKIP LOCKED` claim is exactly
Postgres's standard idiom for safe concurrent claimants: two publisher
goroutines can never claim the same row. This, plus `Processor`/`Publisher`
holding no shared mutable state beyond a connection-pooled `*sql.DB` and
concurrency-safe `Metrics`/`Logger`, makes running multiple publisher (and
consumer) goroutines safe.

**Fix:** added two new, additive, default-preserving env vars —
`WORKER_PUBLISHER_CONCURRENCY` and `WORKER_CONSUMER_CONCURRENCY`, both
defaulting to `1` (today's exact prior behavior for any deployment that
doesn't set them) — that launch that many identical, independent instances
of the existing publish/consume loop bodies against the same shared
`Store`/`Consumer`/`Processor`/`Publisher`. No change to any transaction
boundary, commit-after-success ordering, Kafka delivery guarantee, or
payment-state guard — only how many of each loop run concurrently.

## Before / after (same benchmark tool, same machine, back-to-back)

| concurrency | before: processed/sec | after (publisher=4, consumer=3): processed/sec |
|---|---|---|
| 8  | 75.34  | (warm-up run, see note below) |
| 16 | 75.45  | 107.56 |
| 32 | 77.38  | 314.19 / 315.57 (repeat run) |
| 64 | 77.43  | 311.02 |
| 96 | not run | 278.09 (degrading — see below) |

Consumer-concurrency alone (3 goroutines, publisher still at 1) made no
measurable difference (74-78/sec, matching the "before" row) — confirming
the consumer was never the constraint. Publisher concurrency is what moved
the ceiling.

At concurrency 96, throughput dropped to 278/sec, E2E p50 latency rose to
258ms (vs ~101ms at 32/64), and go-server CPU jumped to ~58% (vs low
single digits at 32/64) — a real degradation signal, not sustained
throughput. 32 and 64 are the reproducible, healthy plateau (two
independent runs at 32: 314.19 and 315.57/sec).

## Regression after the change

`go build`, `go vet`, `go test ./...`, and `go test -tags=integration ./...`
(against real local Postgres) all pass with no changes required anywhere
outside `cmd/worker/main.go`. `router` (49/49) and `cpp-routing-service`
(78/78) ctest suites unaffected (no C++ file touched). Frontend
`tsc`/`eslint`/`vitest`/`build` unaffected (no frontend file touched).
