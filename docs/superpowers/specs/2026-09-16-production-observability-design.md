# Phase 10: Production Observability — Design

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Instrument ChainRoute's full payment lifecycle (HTTP → quotes → C++ routing → Postgres → Kafka outbox → worker → blockchain execution → reconciliation → terminal state) with OpenTelemetry traces, Prometheus metrics, and structured JSON logs, plus a local observability stack and a reproducible throughput benchmark — without changing any existing routing, payment, persistence, or execution behavior.

**Architecture:** Metrics use `prometheus/client_golang` directly, scraped by Prometheus from `/metrics` endpoints the Go server and worker expose natively (no OTel Collector hop for metrics — keeps the pipeline short and matches "expose a `/metrics` endpoint" literally). Traces use the OpenTelemetry Go SDK, exported via OTLP/gRPC to an OTel Collector, which forwards to Jaeger (all-in-one, OTLP-native) for visualization. Logging switches from `log.Printf` to stdlib `log/slog` with a JSON handler — zero new dependency. The C++ routing service gets hand-rolled atomic counters/histograms exposed as Prometheus text format over a minimal raw-socket HTTP responder — no new C++ library, since the metric surface is trivial (six counters/histograms, one fixed response body) and pulling in a client library would violate the "no large dependency for a trivial metric" constraint. The whole observability stack (Prometheus, OTel Collector, Jaeger, Grafana) is Dockerized and optional; ChainRoute's own services remain native binaries, run exactly as `scripts/e2e_test.sh` runs them today, and must work with the stack absent.

**Tech Stack:** `go.opentelemetry.io/otel` + `otel/sdk` + `otel/exporters/otlp/otlptrace/otlptracegrpc` + `otel/exporters/otlp/otlptrace` propagators + `go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp` + `google.golang.org/grpc/otelgrpc` (client interceptor); `github.com/prometheus/client_golang/prometheus` + `promhttp`; stdlib `log/slog`; Docker Compose for the stack; a hand-rolled C++ metrics responder (POSIX sockets, no new dependency).

## Global Constraints

- Do not modify `router/src/route.cpp` (Dijkstra) or its header signature (`findCheapestRoute(graph, source, destination, amount) -> optional<Route>`).
- Do not change any `payment.Status`/`ExternalStatus`/`ExecutionMode` semantics, DB schema for payment/execution tables, nonce allocation, idempotency, or the `BLOCKCHAIN_ENV=="testnet"` gating already in place at all four points (`cmd/server`, `cmd/worker`, `handler.PostPayments`, `worker.Processor.HandleRoutedPayment`).
- Simulated mode must remain 100% network-free: the OTel Go SDK's own exporter goroutines must be configured to fail silently/non-blocking (batch span processor, short flush timeouts, `otlptracegrpc.WithInsecure()` + a dial timeout) so an absent Collector never blocks or errors out a payment request.
- No payment IDs, tx hashes, request IDs, wallet addresses, or other unbounded values as **Prometheus label values**. They are fine as **span attributes** and **log fields** (both are per-event, not aggregated by label cardinality).
- Never log/attach private keys, raw signed tx bytes, RPC URLs with embedded credentials, or provider API keys, in logs, traces, or metrics.
- No new migration — nothing here is persisted to Postgres. The benchmark's machine-readable output is a JSON file on disk, not a DB row.
- `go-api/go.mod`'s `go 1.27.1` and all five existing direct dependencies stay; only additive dependencies.

---

## 1. Current architecture relevant to observability (confirmed against the live repo, not assumed)

- **`cmd/server/main.go`**: `net/http.ServeMux` (Go 1.22+ pattern routing), no middleware chain today, routes `POST /routes`, `POST /payments`, `GET /payments/{id}`. No `/metrics`/`/health`. Builds a `quote.Registry` (Across + Relay providers) only when `BLOCKCHAIN_ENV=="testnet"`.
- **`cmd/worker/main.go`**: no HTTP surface at all. Three-to-four goroutines: `Publisher.Run` (outbox→Kafka), `Recovery.Run` (stale simulated-payment sweep), the Kafka consume loop (`Processor.HandleRoutedPayment`), and conditionally `Reconciler.Run` (testnet-only).
- **`handler.PostPayments`** (`internal/handler/payments.go`): idempotency lookup → (testnet mode only) parallel `quote.Provider.GetQuote` fan-out with a 10s timeout → gRPC `Client.FindRoute` to the C++ service → `Store.CreateOrGetPayment` (one DB transaction that also atomically inserts the outbox row).
- **Outbox → Kafka**: `postgres.Store.PublishNextOutboxEvent` (invoked by `worker.Publisher.PollOnce`) reads the next unpublished row, calls the injected `Publish` closure (→ `kafka.Producer.Publish`), commits "published" only on success.
- **Worker execution**: `worker.Processor.HandleRoutedPayment` → `ClaimPayment` (idempotent claim) → simulated: `execution.Execute` + `CompletePayment`; testnet: `Executor.ExecuteTestnetPayment` (quote re-validation → `buildValidatedEnvelope` → `Signers[provider].BuildTransaction` → `validateEnvelope` → `TryCreateExecution` (nonce) → sign → persist → broadcast → mark submitted).
- **Reconciliation**: `Reconciler.SweepOnce` (three phases: recover-stale-no-execution, drive-stale-not-broadcast, check-broadcast-outcomes via `StatusCheckers[provider].CheckStatus`).
- **Logging today**: 60 call sites, all stdlib `log.Printf`/`log.Fatalf`, unstructured text, hand-rolled `"ERROR: "`/`"WARNING: "` prefixes. No structured logger exists.
- **C++ service** (`cpp-routing-service/src/routing_service.cpp`): `RoutingServiceImpl::FindRoute` builds a `Graph` from either live `candidate_edges` (testnet) or the network simulator's snapshot (simulated default), calls `findCheapestRoute`, serializes the response. Already has a gRPC health check (`grpc::EnableDefaultHealthCheckService`) but no metrics. CMake uses `find_package` for Protobuf/gRPC (system-installed) and `FetchContent` for gtest (source-fetched) — two precedents for how a new dependency could be added, neither of which this design needs (see §3).
- **No Docker anywhere in the repo.** All local dev is native binaries (`scripts/e2e_test.sh`), against a natively-running Homebrew Postgres and either a native `rpk`/Redpanda or a container literally named `redpanda` that the script tolerates but doesn't itself define.
- **No benchmark/load-test code exists.**
- **`README.md` and `CLAUDE.md` are both empty.** `docs/superpowers/{specs,plans}/` is the established documentation home for phase design/plan docs.

## 2. Distributed tracing

### SDK and export path
Go services initialize an OTel `TracerProvider` with `otlptracegrpc` exporting to `OTEL_EXPORTER_OTLP_ENDPOINT` (default `localhost:4317`), wrapped in a `BatchSpanProcessor` with a short (2s) export timeout and non-blocking behavior on connection failure — so an absent Collector degrades to "traces silently dropped," never to a blocked or failed payment. A single shared `internal/observability` package provides:
- `observability.InitTracing(ctx, serviceName string) (shutdown func(context.Context) error, err error)` — called once from `cmd/server`/`cmd/worker`'s `main()`. If OTel initialization itself fails (e.g. malformed endpoint), it logs a warning and returns a **no-op tracer provider** rather than `log.Fatal`ing — tracing must never be a startup dependency.
- `observability.Tracer(component string) trace.Tracer` — thin wrapper so call sites don't repeat `otel.Tracer("chainroute/...")`.

### Spans (one per bullet in the user's list, mapped to real code)
| Lifecycle stage | Where | Span name |
|---|---|---|
| Incoming HTTP request | `otelhttp.NewHandler` wrapping `mux` in `cmd/server` | `http.server` (auto, method+route) |
| Payment creation | `handler.PostPayments`, wraps the whole handler body | `payment.create` |
| Across quote retrieval | inside the per-provider goroutine in `PostPayments`, and inside `across.Provider.GetQuote` | `quote.across.get` |
| Relay quote retrieval | same shape | `quote.relay.get` |
| Quote aggregation | around the `sync.WaitGroup` fan-out/fan-in in `PostPayments` | `quote.aggregate` |
| gRPC call to C++ router | `otelgrpc.NewClientHandler()` as a `grpc.WithStatsHandler` dial option on the client used by `handler.Handler.Client` | `grpc.client` (auto) |
| Postgres persistence | wraps `Store.CreateOrGetPayment`, `Store.GetPayment`, `Store.TryCreateExecution`, `Store.PersistSignedExecution` | `db.<method>` |
| Outbox publication | wraps `Store.PublishNextOutboxEvent`'s call into the publish closure | `outbox.publish` |
| Kafka message processing | wraps `Processor.HandleRoutedPayment` | `kafka.process` |
| Worker execution | wraps `Executor.ExecuteTestnetPayment` / `DriveExecutionForward` | `execution.run` |
| Tx signing/broadcast | wraps `signAndBroadcastFresh`'s sign+broadcast steps as child spans | `execution.sign`, `execution.broadcast` |
| Provider status reconciliation | wraps `Reconciler.checkAndUpdateOutcome` | `reconcile.check` |

Attributes attached where applicable (never as metric labels, only as span attributes): `payment.id`, `payment.provider`, `payment.source_chain`, `payment.destination_chain`, `payment.asset`, `payment.status`, `payment.execution_mode`.

### Context propagation across boundaries
- **HTTP → Go API → gRPC**: automatic — `otelhttp` extracts the incoming trace context from request headers (if a caller sends any; none do today, so this mainly matters for future callers/load-test correlation) and starts the server span; `otelgrpc`'s client stats handler injects it into the outgoing gRPC metadata automatically once both are wired to the same global `TracerProvider`/propagator.
- **Go API/outbox → Kafka → worker**: **manual**, since `kafka-go` has no built-in OTel integration. `worker.Publisher.PollOnce` injects the current trace context (from the span active when the outbox row was created, restored via a `trace_id`/`span_id` pair stored alongside the outbox payload — see below) into the outgoing `kafkago.Message.Headers` using `otel.GetTextMapPropagator().Inject`. The worker's consume loop extracts it via `.Extract` before starting the `kafka.process` span, so the trace genuinely spans "API accepted payment" → "worker executed it," even with Kafka in between.
  - Concretely: `postgres.OutboxEvent` already carries a JSON `Payload` (the `events.RoutedPayment` struct); add one field, `TraceCarrier map[string]string`, populated by `handler.PostPayments` at outbox-insert time via `propagation.MapCarrier` + `Inject`, and read back by `worker.Publisher.PollOnce` to set the Kafka message headers. This survives the outbox's crash/replay semantics without adding a new column (it's just JSON, like the rest of the payload) — **this is a plan-time decision worth flagging: it is a small, additive JSON field, not a schema change,** consistent with the "no migration" constraint.

### Failure isolation
Every span-creation call site follows the pattern `ctx, span := tracer.Start(ctx, name); defer span.End()` with **no branching on tracer/span validity** — the OTel API guarantees a no-op span if the provider isn't configured, so this can never itself error. Nothing in the payment path checks `err` from a span operation, because there is none to check.

## 3. Prometheus metrics

### Library and endpoints
`prometheus/client_golang`. A shared `observability.Registry` (a `prometheus.Registry`, not the global default, to keep test isolation clean) with metric constructors in `internal/observability/metrics.go`. `cmd/server` adds `mux.Handle("GET /metrics", promhttp.HandlerFor(reg, ...))`. `cmd/worker` gets a **new** minimal `http.Server` (a 6-line addition — one more goroutine in the existing `WaitGroup`) listening on `METRICS_ADDR` (default `:9091`) serving only `/metrics`.

### Metric catalogue (names final, labels bounded)

**Payments**
- `chainroute_payments_created_total` (counter; labels: `execution_mode`)
- `chainroute_payments_completed_total` (counter; labels: `execution_mode`)
- `chainroute_payments_failed_total` (counter; labels: `execution_mode`, `failure_reason_class` — a small fixed enum like `routing`,`quote`,`execution`,`persistence`,`other`, never the raw free-text failure reason)
- `chainroute_payments_processing` (gauge; labels: `execution_mode`) — set via `Inc`/`Dec` around the processing window
- `chainroute_payment_duration_seconds` (histogram; labels: `execution_mode`, `outcome`) — created→terminal wall time

**Routing**
- `chainroute_routing_requests_total` (counter; labels: `execution_mode`)
- `chainroute_routing_duration_seconds` (histogram; labels: `execution_mode`) — measured Go-side around the gRPC call (client-observed latency, includes network)
- `chainroute_routing_failures_total` (counter; labels: `reason` — `grpc_error`,`no_route`,`invalid_request`)
- `chainroute_routing_selected_provider_total` (counter; labels: `provider` — bounded to the known set `across`,`relay`; **this is the "compare how often Across vs Relay is selected" metric**)
- `chainroute_routing_selected_fee_base_units` (histogram; labels: `provider`) — the winning hop's fee, so fee distributions are comparable per provider without ever using a payment ID

**Bridge providers** (Across and Relay instrumented identically, differentiated only by the `provider` label — never a per-provider metric family)
- `chainroute_quote_requests_total` (counter; labels: `provider`)
- `chainroute_quote_failures_total` (counter; labels: `provider`, `reason` — `http_error`,`timeout`,`invalid_response`)
- `chainroute_quote_duration_seconds` (histogram; labels: `provider`)
- `chainroute_quote_available_total` (counter; labels: `provider`, `available` — `"true"`/`"false"`)
- `chainroute_quote_selected_total` (counter; labels: `provider`) — same underlying event as `chainroute_routing_selected_provider_total` but recorded at the quote layer for cross-checking; kept because the two call sites are genuinely different (aggregation-time vs. C++-response-time) and cheap to keep both

**Kafka / worker**
- `chainroute_events_published_total` (counter)
- `chainroute_events_consumed_total` (counter)
- `chainroute_processing_duration_seconds` (histogram; labels: `execution_mode`) — `HandleRoutedPayment` wall time
- `chainroute_processing_failures_total` (counter; labels: `execution_mode`, `reason` — `claim_conflict`,`execution_error`,`other`)
- `chainroute_stale_recoveries_total` (counter; labels: `source` — `recovery`,`reconciler`) — the two independent stale-payment sweepers

**Blockchain execution**
- `chainroute_execution_attempts_total` (counter; labels: `provider`)
- `chainroute_broadcasts_total` (counter; labels: `provider`)
- `chainroute_reconciliations_total` (counter; labels: `provider`)
- `chainroute_executions_completed_total` (counter; labels: `provider`)
- `chainroute_executions_failed_total` (counter; labels: `provider`, `reason` — `signing_error`,`broadcast_error`,`envelope_invalid`,`reconciled_failed`)
- `chainroute_execution_duration_seconds` (histogram; labels: `provider`) — sign-through-broadcast wall time
- `chainroute_reconciliation_duration_seconds` (histogram; labels: `provider`) — one `checkAndUpdateOutcome` call

All label value sets are closed enums fixed in code (`execution_mode` ∈ {simulated,testnet}, `provider` ∈ {across,relay}, `reason`/`failure_reason_class`/`outcome` each a small fixed Go `const` set) — never derived from free-text error strings, payment IDs, or addresses. This is enforced by a helper, `observability.SanitizeProviderLabel(name string) string`, that maps any unrecognized provider name to the literal string `"unknown"` rather than passing it through, so a future third provider or a bug can't blow up cardinality.

## 4. C++ router observability

### Approach: hand-rolled counters, no new library
Six primitives in a new `cpp-routing-service/src/metrics.hpp`/`.cpp` (~120 lines): a `RouteMetrics` struct holding `std::atomic<uint64_t>` counters (`requests_total`, `successful_routes_total`, `no_route_total`, `errors_total`) and a small fixed-bucket latency histogram (a `std::array<std::atomic<uint64_t>, N>` bucket-count array + a running sum — the same approach Prometheus text format expects, computed by hand). `RoutingServiceImpl::FindRoute` is wrapped with a scope-timer at its existing entry/exit points (a `MetricsScope` RAII helper that increments `requests_total` on construction and records latency + the appropriate outcome counter on destruction) — **zero lines inside the Dijkstra call itself change**; the instrumentation brackets the outer function, and `request->candidate_edges_size()` (already read at line 49 for other reasons) is recorded as an additional labeled counter (`candidate_edge_count` histogram) at the same bracket point.

### Exposition
A second, tiny listener thread in `main.cpp` (started alongside the existing gRPC server, joined at shutdown) accepts raw TCP connections on `METRICS_PORT` (default `9102`), reads the request line (discarding everything else — this endpoint serves exactly one fixed body, so no request parsing/routing logic is needed beyond "a connection arrived"), and writes back a valid minimal `HTTP/1.1 200 OK` response whose body is hand-formatted Prometheus text exposition built from the `RouteMetrics` snapshot. This uses only `<sys/socket.h>`/`<netinet/in.h>` (already implicitly available via gRPC's own transport deps) — no FetchContent, no find_package, no new build-system entry. This is deliberately not a general-purpose HTTP server; it is the simplest thing that produces a scrapeable `/metrics` response, matching "do not introduce a large dependency solely for a trivial metric."

### Metrics exposed
- `chainroute_router_requests_total` (counter)
- `chainroute_router_duration_seconds` (histogram, fixed buckets e.g. 1ms..1s)
- `chainroute_router_candidate_edges` (histogram, small integer buckets 0..20)
- `chainroute_router_successful_routes_total` (counter)
- `chainroute_router_no_route_total` (counter)
- `chainroute_router_errors_total` (counter)

No labels beyond the metric name itself — the C++ service has no per-request identifying dimension worth exposing (no provider/payment concept exists at this layer), so there is no cardinality risk to design around here.

## 5. Structured logging

Replace all 60 `log.Printf`/`log.Fatalf` call sites with `log/slog` (stdlib, Go 1.21+, zero new dependency) configured with `slog.NewJSONHandler(os.Stdout, ...)`. A shared `observability.Logger(service string) *slog.Logger` returns a logger pre-bound with a `service` attribute (`"go-api"` or `"worker"`). Call sites use `logger.InfoContext(ctx, "message", "payment_id", id, "provider", p, ...)` — `InfoContext`/`ErrorContext` variants so a `slog.Handler` wrapper (added once, centrally) can pull `trace_id`/`span_id` out of the active OTel span in `ctx` and inject them as fields automatically, so **every** log call gets trace correlation for free without repeating `trace.SpanContextFromContext(ctx).TraceID()` at every call site. Fields used where available: `timestamp` (automatic), `severity` (slog level), `service`, `operation` (a short string per call site, e.g. `"payment.create"`), `payment_id`, `provider`, `status`, `trace_id`, `error`. Log volume is deliberately NOT 1:1 with metrics — the existing sparse `WARNING`/`ERROR` call sites stay sparse; this phase does not add a log line for every metric increment.

## 6. Local observability stack (Docker)

`docker-compose.observability.yml` at repo root, four services, none of which run ChainRoute's own code (server/worker/C++ service keep running natively, exactly as `scripts/e2e_test.sh` starts them today):

- **`prometheus`** (`prom/prometheus`): mounts `observability/prometheus/prometheus.yml`, scrapes `host.docker.internal:8080/metrics` (go-api), `host.docker.internal:9091/metrics` (worker), `host.docker.internal:9102/metrics` (C++ router) — three static targets, no service discovery needed for a local dev stack. Port `9090` exposed.
- **`otel-collector`** (`otel/opentelemetry-collector-contrib`, pinned tag): mounts `observability/otel-collector/config.yml` — OTLP gRPC receiver on `4317`, batch processor, `otlp` exporter forwarding to Jaeger's OTLP endpoint. (Contrib image chosen only because the base collector image occasionally lacks the plain `otlp` exporter in some distros; documented in the compose file comment either way.)
- **`jaeger`** (`jaegertracing/all-in-one`): single container, in-memory storage (acceptable for local dev, explicitly not for production), OTLP receiver enabled via `COLLECTOR_OTLP_ENABLED=true`, UI on `16686`.
- **`grafana`** (`grafana/grafana`): mounts `observability/grafana/provisioning/datasources/` (auto-provisions the Prometheus datasource pointing at `http://prometheus:9090`) and `observability/grafana/provisioning/dashboards/` (a dashboard provider pointing at `observability/grafana/dashboards/`, where the committed dashboard JSON lives) and `observability/grafana/dashboards/`. UI on `3000`.

**Why Jaeger over Tempo**: Tempo needs an object-storage backend (even MinIO locally) to be configured before it accepts writes usefully; Jaeger all-in-one accepts OTLP directly and needs zero backing storage config for local dev. Given the explicit ask for "the simplest architecture that integrates cleanly," Jaeger all-in-one is the smaller footprint for a single-command local stack. This is documented in the README, not just here.

`docker-compose up -d` (or a `make observability-up`/`scripts/observability_up.sh` convenience wrapper) starts the four containers; `docker-compose down` or the app simply not being started leaves ChainRoute's own services fully functional — Prometheus/OTel Collector/Jaeger/Grafana are pure consumers of telemetry the app already tolerates being ignored.

## 7. Grafana dashboard

One provisioned dashboard, `ChainRoute Overview`, panels (each a direct PromQL query against the metrics in §3):
1. Payment throughput — `rate(chainroute_payments_created_total[1m])` by `execution_mode`
2. Payment success/failure — `rate(chainroute_payments_completed_total[5m])` vs `rate(chainroute_payments_failed_total[5m])`
3. End-to-end payment latency (p50/p95/p99) — `histogram_quantile(0.5|0.95|0.99, rate(chainroute_payment_duration_seconds_bucket[5m]))`
4. Routing latency (p50/p95/p99) — same pattern on `chainroute_routing_duration_seconds_bucket`
5. Across vs Relay selection — `sum by (provider) (rate(chainroute_routing_selected_provider_total[5m]))`
6. Provider quote latency — `histogram_quantile(0.95, rate(chainroute_quote_duration_seconds_bucket[5m]))` by `provider`
7. Provider quote failures — `rate(chainroute_quote_failures_total[5m])` by `provider`, `reason`
8. Kafka/worker processing — `rate(chainroute_events_consumed_total[1m])`, `histogram_quantile(0.95, rate(chainroute_processing_duration_seconds_bucket[5m]))`
9. Blockchain execution outcomes — `rate(chainroute_executions_completed_total[5m])` vs `rate(chainroute_executions_failed_total[5m])` by `provider`
10. Currently-processing gauge — `chainroute_payments_processing`

Dashboard JSON is hand-authored (not exported from a live Grafana instance) so it's reviewable as plain text in the diff, and provisioned via the standard Grafana file-provisioning convention (`provisioning/dashboards/dashboard.yml` pointing at a folder), which loads automatically on container start — no manual "import dashboard" click required.

## 8. Benchmark

**Definition of "processed"**: a payment is processed when `GET /payments/{id}` reports a terminal status (`COMPLETED` or `FAILED`) — i.e., it has gone through the full HTTP→DB→outbox→Kafka→worker→terminal-state pipeline, not merely been accepted by `POST /payments`. This is the honest end-to-end number the phase asks for, at the cost of being lower than pure HTTP-accept throughput; the benchmark reports **both** numbers so the distinction is never hidden.

**Tool**: a new `go-api/cmd/benchmark/main.go` (not a shell script wrapping `ab`/`hey`, so it can poll for completion using the same JSON contract the real client uses, and so it's trivially reproducible with `go run`). Flags: `-duration` (default 30s), `-concurrency` (default matches `GOMAXPROCS`, overridable), `-base-url` (default `http://localhost:8099`, matching `e2e_test.sh`'s port), `-mode` (`simulated` only — testnet mode is explicitly excluded per "avoid real blockchain spending"), `-out` (JSON result path).

**Method**: `concurrency` worker goroutines each loop for `duration`: build a unique idempotency key, `POST /payments` (simulated mode, deterministic fixed payload), record accept latency; then poll `GET /payments/{id}` (bounded retries with backoff, generous timeout) until terminal, recording end-to-end latency. Both latencies recorded into a lock-free-ish per-goroutine slice, merged after the run for percentile computation (no shared mutable histogram in the hot path, to avoid the benchmark's own lock contention skewing results).

**Output**: human-readable stdout summary (total requests, successful, failed, throughput req/s and processed/s, p50/p95/p99 for both accept and end-to-end latency) plus a machine-readable JSON file with the same fields, timestamped, including the exact flags used and a `"hardware"` string (`runtime.NumCPU()`, `runtime.GOOS`/`GOARCH`) so the result is self-documenting about the environment it was measured on. **No number is hard-coded anywhere in code or docs** — the design doc and README will describe the methodology only; the actual measured figure is reported at implementation/verification time from a real run.

**Non-goals for the benchmark**: it does not exercise testnet/real-blockchain execution (excluded by design, per the phase's own constraint), and it does not attempt to benchmark the C++ router in isolation (that's a different, narrower benchmark that could be added later but isn't asked for here).

## 9. Tests

- **Go metrics**: unit tests asserting each counter/histogram increments on the expected code path (using `prometheus/testutil` for exact value assertions against the isolated `observability.Registry`, not the global default registry — keeps tests parallel-safe), that `provider`/`execution_mode`/`reason` labels only ever take values from the closed set even when fed adversarial input (e.g. an unregistered provider name → `"unknown"`), and that a failed operation increments the corresponding `*_failed_total`/`*_failures_total` counter.
- **Tracing**: using the OTel SDK's `sdktrace.NewTracerProvider` with an in-memory `tracetest.SpanRecorder` exporter, assert (a) a span is created at each of the listed lifecycle points in at least one representative test, (b) the outbox→Kafka trace-carrier round-trips (inject at publish, extract at consume, same trace ID on both ends), (c) tracing being completely unconfigured (no-op provider) does not alter `PostPayments`' HTTP status code or response body versus today's behavior — a before/after-equivalence test.
- **C++ metrics**: a gtest case constructing a `RouteMetrics`, driving a few `FindRoute` calls (reusing the existing test harness) through success/no-route/error paths, and asserting the exposed text-format output contains the expected counter values — verifies the exposition format is valid Prometheus text (parseable) without needing a real network listener in the test (the listener and the metrics struct are separable; the struct's snapshot-to-text function is what's tested directly).
- **Simulated-mode network isolation**: an explicit test (extending the existing simulated-mode test coverage) asserting that with `OTEL_EXPORTER_OTLP_ENDPOINT` pointed at an unreachable address, a full simulated-mode payment still completes successfully within a bounded time — proving tracing failure cannot block or fail payment processing.
- **Observability-unavailable resilience**: a test that `observability.InitTracing` against a bad/unreachable endpoint returns a working no-op provider and no error that would propagate to `log.Fatal` in `main()`.
- Full existing suites re-run unmodified at the end: Go unit + `-tags=integration` (Postgres) + worker integration (Kafka-shape via the existing Postgres-backed duplicate-delivery test) + C++ `ctest` (both `router/build` and `cpp-routing-service/build`) + `scripts/e2e_test.sh`.

## 10. Documentation

`README.md` (currently empty) gets its first real content: a short architecture overview, an "Observability" section (stack diagram in prose: app → Prometheus direct-scrape for metrics; app → OTel Collector → Jaeger for traces; app → stdout JSON for logs), exact commands to start the stack, the three `/metrics` endpoints and their ports, Grafana URL, how to run the benchmark and how to read its output, and a one-paragraph explanation of why Jaeger was chosen over Tempo.

## Non-goals (explicit, matching the phase's own constraints)

No frontend/dashboard UI beyond Grafana's own. No changes to Dijkstra, payment semantics, idempotency, nonce allocation, or provider selection/fallback logic. No production deployment config (this is a local dev stack). No alerting rules (Prometheus Alertmanager is out of scope — not requested). No benchmark of testnet/real execution. No third bridge provider. No auto-failover.
