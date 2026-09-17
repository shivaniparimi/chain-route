# ChainRoute

ChainRoute is a cross-chain payment router. It compares live bridge quotes
from multiple providers (Across and Relay), asks a C++ routing service to
pick the cheapest viable path across a simulated multi-hop bridge topology,
persists the resulting payment to PostgreSQL with a transactional outbox,
executes it asynchronously through Kafka (either as a network-free
simulation or, in testnet mode, as a real signed transaction on Ethereum
Sepolia / Base Sepolia), and reconciles its on-chain outcome back into
payment state. The system is split into a Go HTTP API/worker (`go-api/`)
and a C++ gRPC routing service (`cpp-routing-service/`, built on the
`router/` Dijkstra graph library), talking to each other over gRPC using
the schema in `proto/chainroute/`.

## Architecture

A payment's request flows through the system as follows:

```
HTTP request (Go server, go-api/cmd/server)
  -> quote providers (Across, Relay) -- only in BLOCKCHAIN_ENV=testnet, for live fee comparison
  -> gRPC FindRoute call -> C++ router (cpp-routing-service, Dijkstra over a simulated bridge graph)
  -> PostgreSQL: payment row written + an outbox_events row in the same transaction
  -> Kafka ("chainroute.payments.routed" topic), published from the outbox by a background poller
  -> Go worker (go-api/cmd/worker) consumes the event
  -> blockchain execution (simulated network-free execution, or a real signed tx in testnet mode)
  -> reconciliation: the worker polls the bridge provider's status API and the destination chain
     until the payment reaches a terminal state (COMPLETED or FAILED)
```

`POST /routes` only exercises the quoting + routing steps (no persistence).
`POST /payments` runs the full pipeline: it creates the payment and its
outbox event transactionally, returns immediately with the payment in
`ROUTED` state, and the worker moves it to `PROCESSING` and then to a
terminal state as execution and reconciliation complete. `GET
/payments/{id}` is how a caller observes that progress.

The outbox pattern (a `payments` write and an `outbox_events` write in one
DB transaction, with a separate poller publishing to Kafka) exists so that
"the payment was accepted" and "the payment was durably queued for
processing" can never disagree, even across a crash between the two steps.
The worker additionally runs a periodic recovery sweep (stale outbox rows)
and, in testnet mode, a reconciler that re-checks payments stuck in
`PROCESSING` and a nonce-divergence check against the wallet's on-chain
nonce.

## Running locally

You need a local PostgreSQL instance (with the schema migrations in
`go-api/migrations/` applied) and, for the full async pipeline, a running
Kafka-compatible broker (Redpanda in development) reachable on
`localhost:9092` with the `chainroute.payments.routed` topic created.

The canonical way to build and run everything — the C++ routing service,
the Go server, and the Go worker, wired together and smoke-tested end to
end — is `scripts/e2e_test.sh`. It builds the C++ service with CMake,
builds the Go server and worker with `go build`, starts the C++ service
on `127.0.0.1:50098`, starts the Go server on `:8099` pointed at it, runs a
battery of HTTP checks (including a payment surviving a server restart),
then starts the worker against Redpanda and confirms an async payment
reaches a terminal state via Kafka. Read that script for the exact
sequence, flags, and health-check strategy rather than duplicating it
here; it is also the fastest way to confirm a change works end to end.

To run the pieces individually rather than through that script:

```bash
# C++ routing service (gRPC, listens on 0.0.0.0:50051 by default)
cmake -S cpp-routing-service -B cpp-routing-service/build
cmake --build cpp-routing-service/build
./cpp-routing-service/build/chainroute_service_server --seed=42 --listen-address=127.0.0.1:50051

# Go server (HTTP, listens on :8080 by default)
export DATABASE_URL=postgres://user:password@localhost:5432/chainroute?sslmode=disable
(cd go-api && go run ./cmd/server --http-addr=:8080 --grpc-addr=127.0.0.1:50051)

# Go worker (consumes Kafka, requires KAFKA_BOOTSTRAP_SERVERS)
export DATABASE_URL=postgres://user:password@localhost:5432/chainroute?sslmode=disable
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
(cd go-api && go run ./cmd/worker)
```

By default everything runs in **simulated mode**: no `BLOCKCHAIN_ENV`
environment variable means quotes and execution are entirely network-free
(the C++ service's own deterministic simulator stands in for real bridge
behavior), which is what `scripts/e2e_test.sh` and the benchmark
(`go-api/cmd/benchmark`) both rely on. Setting `BLOCKCHAIN_ENV=testnet`
switches the server to real Across/Relay quote calls and the worker to
real signed Sepolia/Base Sepolia transactions — see the environment
variable table below for what that mode additionally requires.

## Observability

Phase 10 adds a metrics, tracing, and structured-logging layer on top of
the pipeline above. None of it is a correctness dependency: tracing
initialization failure is logged and swallowed (the service falls back to
a no-op tracer), and the local observability stack itself is entirely
optional — the Go and C++ binaries run and pass `scripts/e2e_test.sh`
with it stopped or never started.

```
Go server/worker  --/metrics-->  Prometheus (:9090, scrapes :8080 and :9091)
C++ router        --/metrics-->  Prometheus (:9090, scrapes :9102)
Go server/worker  --OTLP/gRPC--> OTel Collector (:4317) --> Jaeger (:16686)
Go server/worker  --stdout-->    structured JSON logs (log/slog, one line per record,
                                  with trace_id/span_id attached when a span is active)
```

The C++ router does not export traces or structured logs — Phase 10 gave
it only a `/metrics` endpoint (see "How it's implemented" below).

### Starting the stack

```bash
docker compose -f docker-compose.observability.yml up -d
```

This starts four containers: Prometheus, an OpenTelemetry Collector,
Jaeger, and Grafana. It does **not** start ChainRoute's own binaries —
those still run natively as described above. The compose file reaches the
natively-running server/worker/router via `host.docker.internal`, which
Prometheus's scrape config (`observability/prometheus/prometheus.yml`) and
`extra_hosts: host.docker.internal:host-gateway` entry both depend on.

### Prometheus

- `go-api` server: `http://localhost:8080/metrics`
- `go-api` worker: `http://localhost:9091/metrics` (configurable via the
  `METRICS_ADDR` environment variable, e.g. `METRICS_ADDR=:9092`)
- C++ router: `http://localhost:9102/metrics` (configurable via the
  `METRICS_PORT` environment variable)
- Prometheus itself (UI + query API): `http://localhost:9090`

`observability/prometheus/prometheus.yml` scrapes all three targets every
5 seconds under the job names `chainroute-go-api`, `chainroute-worker`,
and `chainroute-router`.

### Grafana

`http://localhost:3000`. This compose file enables anonymous access with
the `Admin` org role (`GF_AUTH_ANONYMOUS_ENABLED=true`,
`GF_AUTH_ANONYMOUS_ORG_ROLE=Admin`) so the dashboard is reachable with no
login for local development. **This is not a production configuration** —
it must not be reused for any deployment reachable outside a local
machine. Grafana auto-provisions the Prometheus datasource
(`observability/grafana/provisioning/datasources/datasource.yml`) and a
single dashboard, "ChainRoute Overview"
(`observability/grafana/dashboards/chainroute-overview.json`, provisioned
into the "ChainRoute" folder), on startup — no manual dashboard import is
needed.

### Tracing

Both the Go server and the Go worker export traces via OTLP/gRPC. The
target endpoint is controlled by `OTEL_EXPORTER_OTLP_ENDPOINT`, which
defaults to `localhost:4317` — the OTel Collector's default OTLP/gRPC
port and exactly the port the observability compose file publishes, so no
configuration is needed for local development. The Collector
(`observability/otel-collector/config.yml`) batches spans and forwards
them to Jaeger over OTLP/gRPC as well (`jaeger:4317` inside the compose
network). View traces at the Jaeger UI: `http://localhost:16686`.

The stack uses Jaeger's all-in-one image as the trace backend rather than
Grafana Tempo, because Jaeger's all-in-one mode needs no separate
object-storage backend for local development — it's a single container
with in-memory storage, which keeps this optional local stack simple.

### Metrics catalogue

All Go-side metrics live in `go-api/internal/observability/metrics.go`,
registered against an isolated `prometheus.Registry` (never the global
default registerer). Every `provider` label value is sanitized to the
closed set `{across, relay, unknown}` before being used
(`SanitizeProviderLabel`), so an unexpected provider name can never create
unbounded label cardinality.

**Payments**

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `chainroute_payments_created_total` | Counter | `execution_mode` | Total payments created via `POST /payments`. |
| `chainroute_payments_completed_total` | Counter | `execution_mode` | Total payments that reached `COMPLETED`. |
| `chainroute_payments_failed_total` | Counter | `execution_mode`, `failure_reason_class` | Total payments that reached `FAILED`. |
| `chainroute_payments_processing` | Gauge | `execution_mode` | Payments currently in `PROCESSING`. |
| `chainroute_payment_duration_seconds` | Histogram | `execution_mode`, `outcome` | Wall time from payment creation to a terminal state. |

**Routing (Go server -> C++ router)**

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `chainroute_routing_requests_total` | Counter | `execution_mode` | Total gRPC `FindRoute` calls issued. |
| `chainroute_routing_duration_seconds` | Histogram | `execution_mode` | Client-observed `FindRoute` latency. |
| `chainroute_routing_failures_total` | Counter | `reason` | Total `FindRoute` failures. |
| `chainroute_routing_selected_provider_total` | Counter | `provider` | Which bridge provider's quote won the selected route/hop. |
| `chainroute_routing_selected_fee_base_units` | Histogram | `provider` | Fee (in base units) of the winning hop, by provider. |

**Bridge quote providers (Across, Relay)**

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `chainroute_quote_requests_total` | Counter | `provider` | Total quote requests issued per provider. |
| `chainroute_quote_failures_total` | Counter | `provider`, `reason` | Total quote request failures per provider. |
| `chainroute_quote_duration_seconds` | Histogram | `provider` | Quote request latency per provider. |
| `chainroute_quote_available_total` | Counter | `provider`, `available` | Whether a provider returned a usable quote. |
| `chainroute_quote_selected_total` | Counter | `provider` | Times a provider's quote won route selection. |

**Kafka / worker pipeline**

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `chainroute_events_published_total` | Counter | — | Total outbox events published to Kafka. |
| `chainroute_events_consumed_total` | Counter | — | Total Kafka events consumed by the worker. |
| `chainroute_processing_duration_seconds` | Histogram | `execution_mode` | `HandleRoutedPayment` wall time. |
| `chainroute_processing_failures_total` | Counter | `execution_mode`, `reason` | Total `HandleRoutedPayment` failures. |
| `chainroute_stale_recoveries_total` | Counter | `source` | Total stale-payment recoveries (outbox sweep / reconciler). |

**Blockchain execution (testnet mode)**

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `chainroute_execution_attempts_total` | Counter | `provider` | Total testnet execution attempts per provider. |
| `chainroute_broadcasts_total` | Counter | `provider` | Total transactions broadcast per provider. |
| `chainroute_reconciliations_total` | Counter | `provider` | Total reconciliation status checks per provider. |
| `chainroute_executions_completed_total` | Counter | `provider` | Total executions reaching a completed outcome per provider. |
| `chainroute_executions_failed_total` | Counter | `provider`, `reason` | Total execution failures per provider. |
| `chainroute_execution_duration_seconds` | Histogram | `provider` | Sign-through-broadcast wall time per provider. |
| `chainroute_reconciliation_duration_seconds` | Histogram | `provider` | Per-check reconciliation wall time per provider. |

**C++ router** (`cpp-routing-service/src/metrics.cpp`, hand-rolled
lock-free counters and a fixed-bucket histogram — no Prometheus client
library dependency, exposed by a minimal raw-socket `/metrics` listener)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `chainroute_router_requests_total` | Counter | — | Total `FindRoute` requests received. |
| `chainroute_router_successful_routes_total` | Counter | — | Total requests that found a viable route. |
| `chainroute_router_no_route_total` | Counter | — | Total requests with no viable route. |
| `chainroute_router_errors_total` | Counter | — | Total requests that errored. |
| `chainroute_router_duration_seconds` | Histogram | — | `FindRoute` latency (fixed buckets: 1/2/5/10/20/50/100/1000ms). |
| `chainroute_router_candidate_edges` | Histogram | — | Candidate edge count considered per request (fixed buckets: 0/1/2/5/10). |

### Running the benchmark

`go-api/cmd/benchmark` is a reproducible, network-free load test for the
full payment pipeline. It only ever exercises `execution_mode=simulated`
(it never spends real testnet funds). Start the server (and, if you want
to measure the full async pipeline rather than just HTTP accept latency,
the worker plus Kafka/Redpanda) in simulated mode first, then run:

```bash
go run ./go-api/cmd/benchmark -duration=30s -concurrency=8 -base-url=http://localhost:8099 -out=/tmp/result.json
```

Flags: `-duration` (how long to send load, default `30s`), `-concurrency`
(number of concurrent worker goroutines, default `GOMAXPROCS`),
`-base-url` (the running server's base URL, default
`http://localhost:8099` — matching the port `scripts/e2e_test.sh` starts
the server on), `-out` (optional path to write a machine-readable JSON
result; if omitted, only the human-readable summary is printed).

The JSON written to `-out` has this shape:

```json
{
  "timestamp": "2026-09-17T00:00:00Z",
  "duration_seconds": 30.01,
  "concurrency": 8,
  "base_url": "http://localhost:8099",
  "hardware": { "num_cpu": 10, "goos": "darwin", "goarch": "arm64" },
  "total_requests": 1234,
  "successful": 1200,
  "failed": 34,
  "throughput_accept_per_sec": 41.1,
  "throughput_processed_per_sec": 40.0,
  "accept_latency_ms": { "p50": 3.2, "p95": 6.1, "p99": 9.8 },
  "e2e_latency_ms": { "p50": 210.4, "p95": 480.2, "p99": 720.9 }
}
```

### Interpreting benchmark output

The benchmark distinguishes two different things, and the report is
useless if they're conflated:

- **Accept** is `POST /payments` returning `201`/`200`. `accept_latency_ms`
  and `throughput_accept_per_sec` describe only this — the HTTP round
  trip plus the DB write of the payment and its outbox row. A payment
  counted here has *not* necessarily executed yet.
- **Processed** means the benchmark polled `GET /payments/{id}` until the
  payment reached a terminal status. Only `COMPLETED` counts as
  `successful`; both `FAILED` and any payment that never reached a
  terminal state within the poll budget count as `failed`.
  `e2e_latency_ms` and `throughput_processed_per_sec` describe only the
  subset that reached `COMPLETED`, measured from just after accept to
  observed completion — i.e. it captures outbox-publish + Kafka +
  worker-processing + simulated-execution latency, not just the HTTP
  call.

The `hardware` object records the exact machine the run measured
(`num_cpu`, `goos`, `goarch`). Any number this benchmark reports is a
measurement of that one run on that one machine under that one
concurrency setting — never a claimed universal throughput or latency
figure for ChainRoute in general. Re-run it on the hardware you actually
care about before drawing conclusions from it.

## Environment variables

All variables are read via `os.Getenv`; those with a documented default
below fall back to it when unset or empty. `go-api/.env.example` has a
starter file for local development (extend it with any variables you
need beyond `DATABASE_URL`).

**Core (server and worker)**

| Variable | Default | Used by | Purpose |
|---|---|---|---|
| `DATABASE_URL` | *(required)* | server, worker | PostgreSQL connection string. |
| `BLOCKCHAIN_ENV` | `""` (simulated) | server, worker | Set to `testnet` to enable live Across/Relay quoting and real signed Sepolia/Base Sepolia execution. Anything other than `testnet` (including unset) keeps everything network-free. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | server, worker | OTLP/gRPC endpoint traces are exported to (the OTel Collector). |

**Go server (`go-api/cmd/server`)**

| Variable / flag | Default | Purpose |
|---|---|---|
| `--http-addr` (flag) | `:8080` | HTTP listen address. |
| `--grpc-addr` (flag) | `127.0.0.1:50051` | Address of the C++ routing service. |
| `MAX_TESTNET_AMOUNT_WEI` | `10000000000000000` (0.01 WETH) | Ceiling on testnet-mode payment amounts. |
| `ACROSS_TESTNET_API_URL` | `https://testnet.across.to/api` | Across quote API base URL (testnet mode only). |
| `ACROSS_API_KEY` | `""` | Across API key (optional). |
| `ACROSS_INTEGRATOR_ID` | `""` | Across integrator ID (optional). |
| `RELAY_TESTNET_API_URL` | `https://api.testnets.relay.link` | Relay quote API base URL (testnet mode only). |
| `RELAY_API_KEY` | `""` | Relay API key (optional). |
| `ROUTING_QUOTE_TTL_SECONDS` | `120` | How long a fetched quote stays valid before it's considered stale. |

**Go worker (`go-api/cmd/worker`)**

| Variable | Default | Purpose |
|---|---|---|
| `METRICS_ADDR` | `:9091` | Listen address for the worker's own `/metrics` endpoint. |
| `KAFKA_BOOTSTRAP_SERVERS` | *(required)* | Comma-separated Kafka/Redpanda broker addresses. |
| `KAFKA_TOPIC` | `chainroute.payments.routed` | Topic the worker consumes routed-payment events from. |
| `KAFKA_CONSUMER_GROUP` | `chainroute-payment-worker` | Kafka consumer group ID. |
| `OUTBOX_POLL_INTERVAL_MS` | `500` | How often the outbox publisher polls Postgres for unpublished events. |
| `WORKER_RECOVERY_SWEEP_INTERVAL_SECONDS` | `30` | How often the recovery sweep runs. |
| `WORKER_RECOVERY_STALENESS_SECONDS` | `120` | Age (in `ROUTED`/unpublished state) before an outbox row is considered stale and recovered. |
| `TESTNET_HANDLE_TIMEOUT_SECONDS` | `30` | Per-message context timeout for testnet-mode `HandleRoutedPayment` calls (simulated-mode handling keeps a fixed, separate 10-second budget). |
| `RECONCILE_STALENESS_SECONDS` | `120` | Age (in `PROCESSING`) before the reconciler re-checks a testnet-mode payment. |
| `RECONCILE_SWEEP_INTERVAL_SECONDS` | `30` | How often the reconciler sweep runs (testnet mode only). |
| `NONCE_DIVERGENCE_CHECK_INTERVAL_SECONDS` | `60` | How often the reconciler checks the wallet's on-chain nonce against the DB's recorded nonce (testnet mode only). |
| `MAX_FEE_SLIPPAGE_BPS` | `500` (5%) | Maximum allowed slippage between quoted and executed fee (testnet mode only). |
| `TESTNET_WALLET_PRIVATE_KEY` | *(required if `BLOCKCHAIN_ENV=testnet`)* | Private key of the wallet that signs and broadcasts testnet transactions. |
| `ETHEREUM_SEPOLIA_RPC_URL` | *(required if `BLOCKCHAIN_ENV=testnet`)* | Ethereum Sepolia RPC endpoint. |
| `BASE_SEPOLIA_RPC_URL` | *(required if `BLOCKCHAIN_ENV=testnet`)* | Base Sepolia RPC endpoint. |
| `RELAY_DEPOSIT_CONTRACT_SEPOLIA` | `0x5feaB8db4534f9F7e2669bb260C57A01aD1c12E3` | Relay's deposit contract address on Sepolia, pinned as a security check against the contract address returned in a Relay quote. |
| `ACROSS_TESTNET_API_URL`, `ACROSS_API_KEY`, `ACROSS_INTEGRATOR_ID`, `RELAY_TESTNET_API_URL`, `RELAY_API_KEY`, `MAX_TESTNET_AMOUNT_WEI`, `ROUTING_QUOTE_TTL_SECONDS` | *(same as server, above)* | Shared with the server; the worker needs them to build its own Across/Relay provider instances for execution. |

**C++ routing service (`cpp-routing-service`)**

| Variable / flag | Default | Purpose |
|---|---|---|
| `--seed` (flag) | `42` | Seed for the deterministic network simulator. |
| `--listen-address` (flag) | `0.0.0.0:50051` | gRPC listen address. |
| `METRICS_PORT` | `9102` | Port for the raw-socket `/metrics` listener. |

`go-api/.env.example` currently documents `DATABASE_URL` and the Phase
7 testnet-execution variables (`BLOCKCHAIN_ENV`,
`TESTNET_WALLET_PRIVATE_KEY`, `ETHEREUM_SEPOLIA_RPC_URL`,
`BASE_SEPOLIA_RPC_URL`, `ACROSS_TESTNET_API_URL`, `ACROSS_API_KEY`,
`ACROSS_INTEGRATOR_ID`, `MAX_TESTNET_AMOUNT_WEI`,
`RECONCILE_STALENESS_SECONDS`, `RECONCILE_SWEEP_INTERVAL_SECONDS`,
`NONCE_DIVERGENCE_CHECK_INTERVAL_SECONDS`); the tables above are the
complete, current set including the Kafka/outbox/recovery, Relay, and
Phase 10 observability variables that file doesn't yet list.
