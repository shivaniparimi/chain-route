# ChainRoute

A cross-chain crypto payment system. You submit a payment (source chain, destination chain, asset, amount), and it finds the cheapest way to route it across bridge providers, moves it through an async pipeline, and tracks it to completion — recovering automatically if a worker crashes mid-payment.

Built to explore a real question: the same cross-chain transfer can cost very different amounts depending on which bridge you route it through. ChainRoute turns that into a shortest-path routing problem and solves it with Dijkstra's algorithm over live fee/latency/liquidity data.

## Architecture

```
                     ┌─────────────────┐
  POST /payments ──▶ │   Go API (8080)  │
                     └────────┬─────────┘
                              │ gRPC: "what's the cheapest route?"
                              ▼
                     ┌─────────────────────┐
                     │  C++ router (50051)  │  Dijkstra over bridge
                     │                       │  fee / latency / liquidity
                     └───────────────────────┘
                              │ route chosen
                              ▼
                     ┌─────────────────┐
                     │   PostgreSQL     │  payment saved + an outbox
                     │                  │  event, in one transaction
                     └────────┬─────────┘
                              │ outbox publisher polls, publishes
                              ▼
                     ┌─────────────────┐
                     │  Kafka / Redpanda │  chainroute.payments.routed
                     └────────┬─────────┘
                              │ consumed
                              ▼
                     ┌─────────────────┐
                     │   Go worker      │  executes the payment,
                     │                  │  recovers stuck/crashed ones
                     └────────┬─────────┘
                              │
                              ▼
                     payment marked COMPLETED / FAILED

                     ┌─────────────────┐
                     │  React dashboard  │  reads the same API
                     └─────────────────┘

         Prometheus scrapes all three services → Grafana dashboard
```

**Why it's split this way:** the API accepts a payment and returns fast — it doesn't wait around for the payment to actually finish. The database write and the Kafka publish happen together (the [transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html)), so a payment can never be "in Kafka" without also being safely in the database, or vice versa. A separate worker does the actual execution asynchronously, and if it dies partway through, a recovery sweep picks the payment back up and finishes it — no payment gets silently lost or double-processed.

## Components

| Component | Language | What it does |
|---|---|---|
| `router/` + `cpp-routing-service/` | C++ | Builds a graph of chains/bridges and runs Dijkstra to find the cheapest route. Served over gRPC. |
| `go-api/cmd/server` | Go | HTTP API: accepts payments, calls the router, writes to Postgres. |
| `go-api/cmd/worker` | Go | Consumes Kafka, executes payments, retries/recovers failures. |
| `frontend/` | React + TypeScript | Dashboard for browsing payments and routing stats. |
| `proto/` | Protobuf | Shared gRPC contract between the Go API and the C++ router. |

**Stack:** C++ · Go · PostgreSQL · Kafka · gRPC · React · TypeScript · Docker · Terraform · Prometheus/Grafana

## Running it locally

```bash
docker compose up -d
```

This starts Postgres, Kafka (Redpanda), the C++ router, the Go API, the Go worker, and the frontend. The API is on `localhost:8080`, the dashboard on `localhost:5173`.

`docker compose -f docker-compose.yml -f docker-compose.observability.yml up -d` also brings up Prometheus, an OTel Collector, Jaeger, and Grafana.

### API

```
POST /payments                    submit a payment
GET  /payments                    list payments (paginated, filterable)
GET  /payments/{id}               a single payment's status and route
GET  /payments/{id}/quotes        every bridge quote that was compared
GET  /dashboard/stats             aggregate counts and cost stats
GET  /dashboard/timeseries        payment volume over time
```

## Performance

Measured on a native (non-Docker) run of the actual pipeline — a real payment going through the API, Postgres, Kafka, and the worker, counted only once it reaches a terminal `COMPLETED`/`FAILED` state, not just once the HTTP request is accepted:

- **300+ completed payments/sec**, sustained, at 32–64 concurrent in-flight payments.
- The C++ router alone (in isolation, not the full pipeline) resolves a route in low single-digit microseconds.
- The original bottleneck was a single-threaded step that published one Kafka message at a time — fixing that took throughput from ~75/sec to 300+/sec. `go-api/cmd/worker` has the concurrency knobs (`WORKER_PUBLISHER_CONCURRENCY`, `WORKER_CONSUMER_CONCURRENCY`) that fix it.

All of this runs in **simulated execution mode** by default — no real blockchain transactions or funds. There's also a `testnet` mode that signs and broadcasts real (testnet) transactions via the Across and Relay bridge APIs, gated behind an explicit environment variable.

## What hasn't been verified

Being direct about this rather than implying more than is true: a real Docker build/deploy of the full stack, and an actual deployment to AWS via the included Terraform, have not been run end-to-end. The Terraform is written and passes `validate`/`plan` against dummy credentials, but `apply` has never been run against a real AWS account.

## Project layout

```
router/                 C++ Dijkstra routing library + its own graph/simulator tests
cpp-routing-service/    gRPC server wrapping the router
proto/                  shared .proto contracts
go-api/                 Go API server + worker (cmd/) and everything they share (internal/)
frontend/               React dashboard
infra/terraform/        AWS ECS Fargate infrastructure-as-code
observability/          Prometheus, Grafana, and OTel Collector configs
scripts/                local dev helpers (migrations, demo data, e2e tests)
```
