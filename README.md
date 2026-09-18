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

Phase 11 adds a deployment layer on top of this pipeline without changing
any of it: each of the three runtime components (`go-server`, `go-worker`,
`cpp-router`) is packaged as its own Docker image and composed together
locally via Docker Compose, and the same three services plus Postgres and
Redpanda can be provisioned in AWS (ECS Fargate, behind a public
Application Load Balancer) via the Terraform under `infra/terraform/`. See
the **Docker**, **Terraform**, and **Networking and deployment data flow**
sections below.

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

Note the port mismatch this implies with the "Running locally" section
above: the committed `prometheus.yml` scrapes go-api on its bare default
port `:8080`, but `scripts/e2e_test.sh` (and the benchmark command below)
start the server on `:8099` instead, specifically to avoid colliding with
a real port 8080. The two are independent conventions and don't need to
agree, but if you follow the e2e script's port, the `chainroute-go-api`
scrape target will show as `down` in Prometheus with no explanation. To
fix it, either start a second/direct server instance on `:8080` for the
observability stack to scrape, or edit `prometheus.yml`'s
`chainroute-go-api` target to `host.docker.internal:8099` to match.

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
| `chainroute_payments_processing` | Gauge | `execution_mode` | Payments created but not yet in a terminal state. **Split across two processes** — see note below. |
| `chainroute_payment_duration_seconds` | Histogram | `execution_mode`, `outcome` | Wall time from payment creation to a terminal state. |

> `chainroute_payments_processing` is `Inc()`'d in the `go-api` server
> process (on payment creation) and `Dec()`'d only in the `go-api` worker
> process (on reaching a terminal state). Those are two separate binaries
> with two separate, isolated Prometheus registries, so neither process's
> own scraped series is meaningful on its own — the server's series only
> ever climbs and the worker's series only ever falls into negative
> numbers. Query it with `sum by (execution_mode)
> (chainroute_payments_processing)` (as the Grafana dashboard's "Payments
> Currently Processing" panel already does) to recover the true in-flight
> count; querying either scrape target's value directly will look wrong
> by design.

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
  "timestamp": "<RFC3339 timestamp of the run>",
  "duration_seconds": 0,
  "concurrency": 0,
  "base_url": "http://localhost:8099",
  "hardware": { "num_cpu": 0, "goos": "<GOOS>", "goarch": "<GOARCH>" },
  "total_requests": 0,
  "successful": 0,
  "failed": 0,
  "throughput_accept_per_sec": 0,
  "throughput_processed_per_sec": 0,
  "accept_latency_ms": { "p50": 0, "p95": 0, "p99": 0 },
  "e2e_latency_ms": { "p50": 0, "p95": 0, "p99": 0 }
}
```

(All-zero/placeholder values above — this block shows the JSON's shape
only, not an actual measurement. Do not quote any number from this block
as a real result; run the benchmark yourself to get one.)

### Interpreting benchmark output

The benchmark distinguishes two different things, and the report is
useless if they're conflated:

- **Accept** is `POST /payments` returning `201`/`200`. `accept_latency_ms`
  measures only this — the HTTP round trip plus the DB write of the
  payment and its outbox row. A payment counted here has *not*
  necessarily executed yet.
- **Processed** means the benchmark polled `GET /payments/{id}` until the
  payment reached a terminal status. Only `COMPLETED` counts as
  `successful`; both `FAILED` and any payment that never reached a
  terminal state within the poll budget count as `failed`.
  `e2e_latency_ms` and `throughput_processed_per_sec` describe only the
  subset that reached `COMPLETED`, measured from just after accept to
  observed completion — i.e. it captures outbox-publish + Kafka +
  worker-processing + simulated-execution latency, not just the HTTP
  call.

`throughput_accept_per_sec` is **not** an HTTP-accept-only rate, despite
its name — it does not describe only the accept phase the way
`accept_latency_ms` does. It is `total_requests / duration_seconds`,
where `total_requests` counts fully-completed benchmark iterations —
each one accept *plus* its own poll-to-terminal-or-timeout loop (see
`runOnePayment` in `go-api/cmd/benchmark/main.go`: the counter increments
only after that whole function returns, gated by up to ~5s of polling per
iteration, not right after the accept call completes). At
`-concurrency=N`, at most `N` iterations are ever in flight at once, so
this number is fundamentally bounded by how long each iteration's polling
takes, not by how fast the server can accept HTTP requests alone — it can
be one to two orders of magnitude lower than the server's actual HTTP
accept rate. If you want a number that isolates HTTP accept throughput,
compute it yourself from `accept_latency_ms` and the concurrency setting,
or measure accept latency under load with a separate tool; don't read
`throughput_accept_per_sec` as that number.

The `hardware` object records the exact machine the run measured
(`num_cpu`, `goos`, `goarch`). Any number this benchmark reports is a
measurement of that one run on that one machine under that one
concurrency setting — never a claimed universal throughput or latency
figure for ChainRoute in general. Re-run it on the hardware you actually
care about before drawing conclusions from it.

### Phase 11: benchmarking against the full Docker stack

Phase 11 packages the full pipeline as a Docker Compose stack (see
**Docker** below) instead of the natively-run processes the sections
above assume. The intent, per this phase's implementation plan, was to
run `docker compose up -d`, wait for every service to report healthy,
and then run `go run ./go-api/cmd/benchmark` against the containerized
stack's published `:8080` port, to get throughput/latency numbers that
reflect the container and bridge-network overhead (distroless runtimes,
Docker's own network stack, the outbox poller and worker each running in
their own container) rather than same-host native processes talking over
`localhost`.

That run was never possible in this environment: no Docker daemon has
been reachable at any point during this phase, consistent with every
earlier phase of this project. Confirmed again immediately before writing
this section — `docker info` reports the Docker CLI is installed (v28.3.2,
via Docker Desktop's `desktop-linux` context) but `Cannot connect to the
Docker daemon at unix:///Users/shivaniparimi/.docker/run/docker.sock`.
What actually happened instead, and what it does and does not prove: every
Dockerfile was reviewed line-by-line against its build's real
requirements (see the Docker section's honesty note below), and
`docker compose config` — which validates a compose file's YAML,
variable interpolation, and service references without needing a daemon
— was run against all three compose files (`docker-compose.yml`, the
`docker-compose.yml` + `docker-compose.testnet.yml` override with its
required testnet variables supplied, and `docker-compose.observability.yml`)
and passed cleanly every time. That confirms the stack's configuration is
internally consistent; it does not confirm that any image actually
builds, that any container actually starts, or any throughput/latency
number for the containerized stack. No such number exists anywhere in
this repository, and none should be assumed or quoted until a real Docker
daemon is available and the run described above is actually performed.

## Docker

Phase 11 packages all three ChainRoute runtime components as containers.
Each has its own Dockerfile, and `docker-compose.yml` wires all three
together with Postgres and Redpanda into a single local stack.

**Images:**

| Service | Dockerfile | Build context | Base image (build → runtime) | Listens on |
|---|---|---|---|---|
| `go-server` | `go-api/cmd/server/Dockerfile` | `go-api/` | `golang:1.27-bookworm` → `gcr.io/distroless/static-debian12:nonroot` | `:8080` (HTTP) |
| `go-worker` | `go-api/cmd/worker/Dockerfile` | `go-api/` | `golang:1.27-bookworm` → `gcr.io/distroless/static-debian12:nonroot` | `:9091` (`/metrics`) |
| `cpp-router` | `Dockerfile.cpp-router` | `.` (repo root) | `debian:bookworm-slim` → `debian:bookworm-slim` | `:50051` (gRPC), `:9102` (`/metrics`) |

The C++ router's build context is the repo root (not `cpp-routing-service/`
alone) because its CMake build also needs `router/` (the Dijkstra graph
library it links against) and `proto/` (the shared gRPC schema) — the
Dockerfile `COPY`s all three directories in explicitly rather than relying
on a wider context.

**Building each image individually:**

```bash
# Go server (multi-stage: CGO_ENABLED=0 static build, then a distroless nonroot runtime)
docker build -t chainroute/go-server -f go-api/cmd/server/Dockerfile go-api

# Go worker (same base images as the server)
docker build -t chainroute/go-worker -f go-api/cmd/worker/Dockerfile go-api

# C++ router (builds router/ + cpp-routing-service/ + proto/ via CMake Release,
# with test targets skipped via -DCHAINROUTE_BUILD_TESTS=OFF)
docker build -t chainroute/cpp-router -f Dockerfile.cpp-router .
```

**Running the full local stack:**

```bash
docker compose up -d
```

This starts, on a single `chainroute-net` bridge network: `postgres`
(`postgres:16-bookworm`, with a healthcheck via `pg_isready`), a one-shot
`migrate` service (also `postgres:16-bookworm`, running
`scripts/apply_migrations.sh` against the compose-internal Postgres —
the same portable, idempotent migration script used natively and in CI),
`redpanda` (`docker.redpanda.com/redpandadata/redpanda:v24.2.7`, single
broker), `cpp-router`, `go-server`, `go-worker`, and the bundled
observability stack (`prometheus`, `otel-collector`, `jaeger`, `grafana`
— using a Docker-native Prometheus scrape config,
`observability/prometheus/prometheus.docker.yml`, that targets the
compose service names directly rather than `host.docker.internal`).
`go-server` and `go-worker` don't start until `postgres` is healthy and
`migrate` has completed successfully; `go-server` also waits on
`cpp-router` being healthy, and `go-worker` waits on `redpanda` being
healthy. Everything runs in simulated mode by default — no
`BLOCKCHAIN_ENV` is set anywhere in `docker-compose.yml`. Published host
ports: `8080` (go-server HTTP), `9091` (go-worker metrics), `9090`
(Prometheus), `16686` (Jaeger UI), `3000` (Grafana); `redpanda` and
`cpp-router` are reachable only from other containers on `chainroute-net`.
The Postgres password defaults to `chainroute_local_dev`
(`POSTGRES_PASSWORD:-chainroute_local_dev` in the compose file) and can be
overridden by exporting `POSTGRES_PASSWORD` or setting it in a `.env`
file — see `go-api/.env.example`.

`docker-compose.observability.yml` (Phase 10) is a separate, independent
file for the native-binary development workflow described in the
**Observability** section above (it reaches natively-running binaries via
`host.docker.internal`). It is not meant to be run together with the full
stack above — each is self-contained and provides its own
Prometheus/OTel-Collector/Jaeger/Grafana containers.

**Testnet mode:**

```bash
docker compose -f docker-compose.yml -f docker-compose.testnet.yml up -d
```

`docker-compose.testnet.yml` overrides `go-server` and `go-worker` to set
`BLOCKCHAIN_ENV=testnet` and injects the real Across/Relay/Sepolia
credentials from a `.env` file (git-ignored — never hardcode these values
in a committed file). `TESTNET_WALLET_PRIVATE_KEY`,
`ETHEREUM_SEPOLIA_RPC_URL`, and `BASE_SEPOLIA_RPC_URL` are required (the
override file uses Compose's `${VAR:?message}` syntax, so `docker compose`
itself refuses to start with a clear error if any is missing);
`ACROSS_API_KEY`, `ACROSS_INTEGRATOR_ID`, and `RELAY_API_KEY` are optional
and default to empty.

**The `go-server`/`go-worker` health-check gap:** neither image defines a
container-level `HEALTHCHECK`, and `docker-compose.yml` doesn't add one
via `healthcheck:` either. This isn't an oversight — both run on
`gcr.io/distroless/static-debian12:nonroot`, which ships no shell and no
HTTP client, so there is nothing an in-container `HEALTHCHECK CMD` could
actually execute, and neither service has a dedicated `/healthz` endpoint
yet to check even from outside. The practical substitutes are external:
`docker compose ps` (shows exit/restart status if a container has
crashed) and `curl http://localhost:8080/metrics` from the host (a `200`
response is a reasonable proxy for "the HTTP server is up"; there is no
equivalent for `go-worker` since it has no HTTP surface to accept
requests, only to be scraped on `:9091/metrics`). This is a direct
contrast with the **`cpp-router`** image, which *does* ship a working
`HEALTHCHECK` (`grpc-health-probe -addr=localhost:50051`, every 10s) —
possible only because that image is built on `debian:bookworm-slim`, a
normal shell-capable base, specifically so `grpc-health-probe` (a small
static binary fetched in the builder stage) has an environment to run in.
`go-server` depending on `cpp-router: condition: service_healthy` in
`docker-compose.yml` relies on exactly this working healthcheck.

**What's been verified and what hasn't:** every Dockerfile was written and
statically reviewed against its build's actual requirements — package
names and versions cross-checked against the real Debian Bookworm package
index (see `Dockerfile.cpp-router`'s inline comment and Task 3's report
for that trail), multi-stage build/runtime splits checked for correctness
by inspection. A real `docker build` or `docker run` has never been
possible in this development environment: no Docker daemon has been
reachable at any point across this entire phase (the Docker CLI itself is
installed — `docker info` reports client v28.3.2 — but its `Server:`
block reports `Cannot connect to the Docker daemon`). `docker compose
config` (which parses and validates a compose file's YAML and variable
interpolation without needing a daemon) was run repeatedly against all
three compose files and passed, including the testnet override once its
required variables were supplied. The full stack has never actually been
started.

## Terraform

`infra/terraform/` provisions ChainRoute onto AWS ECS Fargate: the Go
server, Go worker, and C++ router as Fargate services, Postgres as RDS,
a single-node Redpanda broker as its own Fargate service, an optional
observability stack (also Fargate), and a public Application Load
Balancer in front of `go-server`. **`infra/terraform/README.md` is the
canonical module reference** — module-by-module inputs/outputs, the full
design rationale, and the remote-state and Redpanda-vs-MSK tradeoffs in
more depth than here. This section is a summary and a pointer, not a
duplicate.

Module tree (see `infra/terraform/README.md` for the full version):

```
infra/terraform/
├── versions.tf          shared provider pins (aws ~> 5.0, random ~> 3.6)
├── modules/
│   ├── networking/       VPC, public/private subnets, NAT, security groups, Cloud Map namespace
│   ├── database/         RDS Postgres + Secrets Manager credentials
│   ├── messaging/        single-node Redpanda, run via ecs-service
│   ├── observability/    OTel Collector, Jaeger, Prometheus, Grafana, each run via ecs-service
│   ├── ecs-service/       reusable Fargate task/service module -- used by every app and infra service
│   └── alb/               public ALB (the only 0.0.0.0/0 ingress point in the whole design) + go-server target group
└── environments/
    └── dev/               root module: provider, ECS cluster, IAM roles, ECR repos, wires the modules above together
```

`environments/dev` is the only environment defined today; a future
`environments/prod` would reuse the same modules with its own tfvars and
state.

**Validating locally:**

```bash
cd infra/terraform
terraform fmt -recursive -check -diff   # drop -check -diff to fix formatting in place

cd environments/dev
terraform init -backend=false           # downloads providers; no AWS credentials needed
terraform validate                      # checks HCL syntax/references; no AWS credentials needed
```

These three commands were genuinely, successfully run in this development
environment — `terraform fmt -recursive -check -diff`,
`terraform init -backend=false`, and `terraform validate` all passed
cleanly (once a disk-space issue was resolved and Terraform itself was
installed). This is real, verified signal, not a limitation to caveat.

`terraform plan` additionally requires real AWS credentials (it makes
read-only API calls to reconcile against live state), and
`terraform apply` will create real, billable AWS resources. **Neither was
ever run in this project.** Never run `terraform apply` — or `plan` for
real — without a real AWS account, real credentials configured for it,
and explicit authorization from whoever owns that account and its bill.
No AWS credentials are configured anywhere in this repository or in the
environment this was built in, by design.

**Redpanda, not MSK:** `modules/messaging` runs a single-node Redpanda
broker as one Fargate task rather than Amazon MSK or MSK Serverless. This
mirrors `docker-compose.yml`'s local setup exactly and costs a single
small Fargate task, at the cost of no replication or high availability —
a task restart loses any in-flight, uncommitted data, exactly the way a
local dev restart would. This is a disclosed, deliberate portfolio-scale
tradeoff, not an oversight; the production upgrade path is MSK Serverless
or a multi-broker Redpanda cluster spread across availability zones. See
`infra/terraform/README.md` and the phase's design doc §4.1 for the full
reasoning.

**Remote state:** Terraform uses local state by default, so `init` and
`validate` don't require any pre-existing AWS resources.
`environments/dev/backend.tf` documents (commented out) how to point this
environment at an S3-plus-DynamoDB remote backend once that bucket and
lock table exist — `infra/terraform/README.md` has the exact steps.

## Networking and deployment data flow

```
Internet
  |
  v  (public ingress, ports 80/443 only -- the ONLY 0.0.0.0/0 entry point
  |   in the whole Terraform design)
Application Load Balancer (public subnets, alb security group)
  |  HTTP :8080, target group health-checked via GET /metrics
  v
go-server (private subnets, app security group)
  |
  |-- gRPC :50051 -->  cpp-router      (private subnets, internal security group)
  `-- SQL   :5432 -->  RDS Postgres     (private subnets, data security group)
                          ^
                          | outbox poller reads the payment + outbox_events row
                          | written above, in the same DB transaction
                        go-worker      (private subnets, internal security group)
                          |
                          |-- SQL   :5432 -->  RDS Postgres (same instance as above; reconciliation writes)
                          `-- Kafka :9092 <->  Redpanda      (private subnets, internal security group;
                                                              go-worker's own outbox-poller goroutine
                                                              publishes the routed-payment event here, and
                                                              its own consume-loop goroutine -- same
                                                              process -- consumes it back)

go-server / go-worker / cpp-router
  |
  `-- OTLP/gRPC :4317 / scraped /metrics -->  observability stack
                                                (private subnets, internal security group;
                                                 OTel Collector -> Jaeger, Prometheus <- scrapes
                                                 the three services above, Grafana <- Prometheus)
```

This is the same request/data flow as the local **Architecture** diagram
above, redrawn with the AWS trust boundaries Terraform actually
provisions: everything except the ALB sits in private subnets behind
security groups with no path from the public internet, and the
observability stack is both private and non-blocking — exactly as in the
local Docker Compose stack, OTLP export failures are logged and swallowed
rather than affecting payment processing, and the whole stack can be
disabled via the `enable_observability_stack` Terraform variable
(`environments/dev/variables.tf`) without touching any app service.

## CI/CD

**`.github/workflows/ci.yml`** runs on every pull request and every push
to `main`. It has seven jobs, all independent:

| Job | What it validates |
|---|---|
| `go` | `gofmt -l` (clean), `go vet ./...`, `go build ./...`, `go test ./...` in `go-api` (Go 1.27.1) |
| `go-integration` | Spins up a `postgres:16-bookworm` service container, applies migrations with `scripts/apply_migrations.sh`, runs `go test -tags=integration ./...` |
| `kafka-integration` | Spins up a `docker.redpanda.com/redpandadata/redpanda:v24.2.7` service container, runs `go test -tags=kafka_integration ./internal/kafka/...` |
| `cpp` | Builds and `ctest`s both `router/` and `cpp-routing-service/` via CMake (Release) |
| `docker-build` | Builds all three images (`go-server`, `go-worker`, `cpp-router`) with `docker/build-push-action` (`push: false`), then lints all three Dockerfiles with `hadolint` |
| `terraform` | `terraform fmt -recursive -check -diff`, `terraform init -backend=false`, `terraform validate` against `environments/dev` |
| `security-scan` | `gitleaks` secret scan across the full git history |

**`.github/workflows/deploy.yml`** is manual-only
(`workflow_dispatch`) and requires, to actually deploy anything:

- A human triggering it explicitly, choosing an `environment` input
  (a directory name under `infra/terraform/environments/`, default `dev`)
  and typing `confirm_apply: yes` exactly — any other value runs
  `terraform plan` only and stops there.
- The `production` GitHub Environment's approval gate, which the workflow
  requests (`environment: production`) but which has no approvers or
  branch protections configured in this repository yet — that
  configuration is a one-time, out-of-band GitHub repository setting, not
  something this workflow file can create for itself.
- A real AWS IAM role for OIDC federation (`aws-actions/configure-aws-credentials`
  assumes `secrets.AWS_DEPLOY_ROLE_ARN` — no long-lived AWS credentials
  are ever stored as a repository secret). Neither that IAM role, its
  trust policy for this repository's OIDC provider, nor the
  `AWS_DEPLOY_ROLE_ARN` secret itself exist yet; they must be created
  out-of-band against a real AWS account before this workflow can
  succeed.

**CI never auto-deploys.** `ci.yml` contains no deploy step of any kind,
and `deploy.yml` only ever runs on a human explicitly dispatching it —
never on a push or a pull request. Even when dispatched, it only reaches
`terraform apply` if the `confirm_apply` input is the literal string
`yes`; every other input value, including the default, stops after
`terraform plan`.

**What's been verified and what hasn't:** the workflow YAML for both
files is `actionlint`-clean (zero errors or warnings). Every command
`ci.yml` runs was independently exercised manually on this development
machine and passed: `go build`/`go vet`/`go test` in `go-api`, `cmake`
configure/build/`ctest` for both `router/` and `cpp-routing-service/`,
and `terraform fmt`/`init -backend=false`/`validate`. Neither workflow
has ever actually executed as a real GitHub Actions run — that requires
pushing this branch, which has not happened as part of this phase. In
particular, the CMake Protobuf CONFIG/MODULE-mode fallback added to
`cpp-routing-service/CMakeLists.txt` and `router/CMakeLists.txt` (so the
Docker build and a real Ubuntu CI runner, whose `libprotobuf-dev` package
ships no CMake CONFIG-mode files, can still configure successfully) was
verified only against this macOS/Homebrew machine, the one platform
reachable in this environment. Task 10's own full clean rebuild of
`cpp-routing-service` passed `ctest` at 78/78, identical to its pre-change
baseline, confirming no regression on that platform for the CONFIG-mode
path; Task 10's verification of `router` itself was configure-time only
(grepping the configure log for FetchContent/network activity, not an
actual `ctest` run), so it produced no comparable pass count. Re-running
`router`'s own test suite live while finalizing this documentation task
(`cmake --build router/build -j && ctest --test-dir router/build`)
confirmed 49/49 passing today, matching the project's established
baseline. Whether the Module-mode fallback actually succeeds against
Debian/Ubuntu's real `libprotobuf-dev` package remains unverified until
this workflow genuinely runs on a real Ubuntu GitHub Actions runner.

## Environment variables

All variables are read via `os.Getenv`; those with a documented default
below fall back to it when unset or empty. `go-api/.env.example` has a
starter file for local development covering every variable in the tables
below.

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

`go-api/.env.example` lists every variable from the tables above (Core /
Server / Worker / Testnet-only), plus the Phase 11 Docker-Compose-only
`POSTGRES_PASSWORD` (read by `docker-compose.yml` to build the
`postgres`/`migrate`/`go-server`/`go-worker` services' `DATABASE_URL`,
not by either Go binary directly) — copy it to `.env` and fill in the
values you need for the mode you're running in.
