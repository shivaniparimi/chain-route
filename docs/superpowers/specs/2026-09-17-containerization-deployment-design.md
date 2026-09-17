# Phase 11: Containerization, Terraform, and Cloud Deployment — Design

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make ChainRoute reproducibly deployable via Docker (production images + a full local Compose stack) and Terraform (a practical, cost-bounded AWS architecture), plus CI that validates every PR without auto-deploying — without touching routing, payment, Kafka, Postgres, Across/Relay, execution, recovery, or observability semantics.

**Architecture:** Three new production Dockerfiles (Go server, Go worker, C++ router — all multi-stage, non-root, pinned base images). A new root `docker-compose.yml` runs the full containerized backend (app services + Postgres + Redpanda + the existing Phase 10 observability stack, reused unmodified as a second compose file); the existing `docker-compose.observability.yml` stays exactly as-is for the native-binary workflow. A portable migration-application script replaces the macOS-hardcoded `psql` path currently embedded in `scripts/e2e_test.sh`, reused by both the new Compose migration step and the existing script. Terraform provisions a practical AWS architecture (VPC, ECS Fargate for every service including a single-node Redpanda broker — explicitly not MSK, with the tradeoff documented — RDS Postgres, ALB for the one public ingress point, Secrets Manager, CloudWatch), structured into small reusable modules, `fmt`/`validate`-clean, never applied (no AWS credentials in this environment). GitHub Actions validates every PR (Go, C++, Docker builds, Terraform fmt/validate, lightweight security scans) and a separate, manually-triggered workflow handles deployment.

**Tech Stack:** Docker + Docker Compose v2 (`include`-free, two independent compose files); Terraform ≥1.5 targeting AWS (VPC, ECS Fargate, ECR, RDS, ALB, Cloud Map, Secrets Manager, CloudWatch); GitHub Actions.

## Global Constraints

- Do not modify `router/src/route.cpp`/`router/include/chainroute/route.hpp` (Dijkstra) or any payment/execution/reconciliation/recovery logic. Containerization changes only how existing binaries are packaged and started, never what they do.
- Do not weaken the `BLOCKCHAIN_ENV=="testnet"` gating, provider-selection semantics, or introduce any cross-provider fallback. Docker/Terraform configuration surfaces this exact same env var, unchanged.
- Default local Docker environment is simulated-mode, fully network-free (no `BLOCKCHAIN_ENV` set); a separate, clearly-labeled optional Compose override provides testnet configuration.
- No secret (private key, RPC URL with embedded credentials, API key, DB credential) is ever baked into an image, committed, placed in a Terraform output, or logged. `.env.example`/`terraform.tfvars.example` contain placeholders only.
- Terraform is written, `fmt`-clean, and `validate`-clean, but **never applied** in this environment — no AWS credentials exist here, and applying would incur real charges without authorization.
- Docker images can be **built** if a Docker daemon is reachable in the execution environment, but as of this design's writing no daemon has been reachable in any prior phase of this project's session history — every claim about live container behavior must be honestly qualified as "built and run" vs. "written and statically reviewed" depending on what the implementation phase actually achieves.
- CI validates PRs; it never deploys automatically. A deploy workflow exists but requires manual/`workflow_dispatch` invocation, gated behind a GitHub Environment.
- Only the Go HTTP API gets public ingress. Postgres, the C++ router, the worker, Kafka/Redpanda, and the observability stack stay on private networking in the Terraform design.
- Observability (Prometheus/Grafana/Jaeger/OTel Collector) remains strictly non-critical-path — a Docker Compose `depends_on` for these services must never use `condition: service_healthy` in a way that blocks app-service startup, and their Terraform module is independently toggleable.
- Keep the architecture proportionate to a portfolio project — no unnecessarily expensive or sophisticated infrastructure added merely to widen the technology list (explicit user constraint).

---

## 1. Current state (confirmed against the live repo, not assumed)

- **No Dockerfile exists anywhere.** `docker-compose.observability.yml` (root, 4 services: Prometheus, OTel Collector, Jaeger, Grafana) is the only Docker artifact in the repo — it has no application container; Prometheus reaches the natively-running Go binaries via `host.docker.internal`.
- **No CI exists** (`.github/` doesn't exist). **No Terraform exists.**
- **Go server** (`go-api/cmd/server`, `go 1.27.1`): flags `--http-addr` (`:8080`), `--grpc-addr` (`127.0.0.1:50051`); requires `DATABASE_URL`; all HTTP routes (`POST /routes`, `POST /payments`, `GET /payments/{id}`, `GET /metrics`) share one listener, **no dedicated health endpoint** exists. Graceful SIGTERM shutdown already implemented (`signal.NotifyContext` + `server.Shutdown`).
- **Go worker** (`go-api/cmd/worker`): requires `DATABASE_URL` + `KAFKA_BOOTSTRAP_SERVERS`; a separate metrics-only HTTP server on `METRICS_ADDR` (`:9091`); Kafka producer/consumer construction is non-blocking (lazy dial), so a transiently-unreachable broker at startup does not crash it — Postgres connectivity, however, **is** a blocking-or-die startup check.
- **C++ router** (`cpp-routing-service`, binary name `chainroute_service_server`): built via CMake with `find_package(Protobuf CONFIG REQUIRED)`/`find_package(gRPC CONFIG REQUIRED)` — **not vendored**, must be installed via a package manager in any build image. Build also requires the sibling `router/` and `proto/` directories in the build context. Flags `--seed`, `--listen-address` (`0.0.0.0:50051`); env `METRICS_PORT` (`9102`); already has `grpc::EnableDefaultHealthCheckService(true)` and a carefully-designed signal-handling sequence (blocks signals before spawning gRPC's completion-queue threads, handles SIGTERM on a dedicated thread outside async-signal-handler context) specifically built to work correctly as a container's PID 1 — this must not be disturbed by wrapping the binary in a shell script for `CMD`/`ENTRYPOINT`.
- **Migrations** (`go-api/migrations/`, 6 files) are applied today only by `scripts/e2e_test.sh`'s hardcoded, macOS-Homebrew-specific `psql` path with per-file `information_schema`-based existence guards — not portable to a container, no version tracking. This needs a portable equivalent for Phase 11.
- **`README.md`'s environment-variable tables (post-Phase-10) are the authoritative variable reference** — `go-api/.env.example` is stale (missing Kafka/Relay/observability vars) and should be brought up to date as part of this phase's documentation work, not treated as ground truth.
- Local dev/test canonical flow is `scripts/e2e_test.sh` (native binaries, ports `50098`/`8099`, gRPC readiness via `grpc-health-probe` with **no** `-service=` flag against the default/unnamed health service, Kafka readiness via `rpk cluster info` or a TCP fallback, no explicit Postgres-readiness wait).

## 2. Production Docker images

### 2.1 Go server / worker (two Dockerfiles, near-identical shape)

Multi-stage: `golang:1.27-bookworm` builder (`CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server`), runtime stage `gcr.io/distroless/static-debian12:nonroot` (already runs as a non-root `nonroot` user by convention, no shell — a deliberate choice: distroless's absence of a shell means `ENTRYPOINT`/`CMD` must be exec-form JSON array, which is exactly what's required to preserve Go's own signal handling anyway). Copy only the compiled binary. `EXPOSE 8080` (server) / `EXPOSE 9091` (worker metrics). A Docker `HEALTHCHECK` needs a tool to make an HTTP call from inside a shell-less distroless image — since distroless has no `curl`/`wget`, either (a) skip the Docker-level `HEALTHCHECK` directive and rely on Compose/ECS's own HTTP health check hitting `GET /metrics` from outside the container (the practical choice, since `/metrics` is the only endpoint that exists today and is cheap to hit), or (b) add a tiny Go-based healthcheck binary. Given "do not over-engineer," choose (a): document `GET /metrics` as the container-level health signal for both Compose and the Terraform ALB/ECS target group, explicitly noting no purpose-built `/healthz` exists yet (a reasonable, disclosed limitation, not a defect this phase needs to invent new server code to fix — the design brief's own "add health checks where appropriate" is satisfied by using the cheapest available always-registered endpoint honestly, not by inventing new API surface out of scope).

If `CGO_ENABLED=0` fails to build cleanly (go-ethereum's secp256k1 path has historically had a cgo-dependent implementation on some platforms) — this must be verified empirically once a Docker daemon is available; if it fails, fall back to `CGO_ENABLED=1` with a `debian:bookworm-slim` runtime stage instead of distroless (still non-root via a created `appuser`), which tolerates dynamically-linked cgo output. Document this decision point explicitly in the implementation task rather than assuming success.

### 2.2 C++ router

Multi-stage: builder stage `debian:bookworm-slim` + `apt-get install -y build-essential cmake protobuf-compiler-grpc libprotobuf-dev libgrpc++-dev libssl-dev pkg-config` (Debian Bookworm's packaged Protobuf/gRPC versions export CMake config files, satisfying `find_package(... CONFIG REQUIRED)` without needing Homebrew-specific versions — the C++ code has no version-specific API dependency beyond stable, long-established proto3/gRPC-C++ generation). Build context must include `cpp-routing-service/`, `router/`, and `proto/` (three sibling directories) — the Dockerfile therefore lives at the **repo root**, not inside `cpp-routing-service/`, with a `.dockerignore` excluding `go-api/`, `.git/`, `docs/`, build directories, etc. Runtime stage: `debian:bookworm-slim` + only the runtime (non-`-dev`) shared-library packages (`libprotobuf32` / `libgrpc++1.51` or whatever exact runtime package names Bookworm ships — verify exact names once a Docker build is actually run; the `-dev` build stage's `apt list --installed` output can confirm the runtime counterparts), plus a created non-root user. `EXPOSE 50051 9102`. `ENTRYPOINT ["/app/chainroute_service_server"]` in exec form (critical — the binary's own signal-blocking-before-thread-spawn design assumes it is PID 1 receiving SIGTERM directly, not a shell's child). Docker `HEALTHCHECK`: bundle `grpc-health-probe` (a small static Go binary, downloadable via a pinned-version `curl`/`wget` in the builder stage and copied into the runtime stage) invoked exactly as `scripts/e2e_test.sh` already does — `grpc-health-probe -addr=localhost:50051` with **no** `-service=` flag, matching the existing, already-correct local convention exactly rather than inventing a different check.

### 2.3 Version pinning

Every base image pinned to an exact tag (`golang:1.27-bookworm`, not `golang:latest`; `debian:bookworm-slim` with a specific date-stamped digest recorded in the Dockerfile comment if practical; `gcr.io/distroless/static-debian12:nonroot`). Every `apt-get install` uses Bookworm's stable package versions (no `apt-get upgrade`, no third-party PPAs) — Bookworm itself is the pin.

## 3. Local Docker Compose environment

**Two independent compose files, not merged via `include:`** (a deliberate simplicity choice — Compose's cross-file service-merge semantics for lists like `volumes` are easy to get subtly wrong without a live environment to test against, and this project's constraint is "do not over-engineer," so two clearly-scoped files beat one clever one):

- **`docker-compose.observability.yml`** (existing, **completely untouched** — Phase 10's already-reviewed, already-correct native-binary + observability workflow keeps working exactly as before).
- **`docker-compose.yml`** (new, root) — the full containerized backend:
  - `postgres` (`postgres:16-bookworm` — matching the version already used locally — `POSTGRES_DB=chainroute`, named volume `postgres-data:/var/lib/postgresql/data`, healthcheck `pg_isready -U chainroute`).
  - `migrate` (a one-shot init service, same `postgres:16-bookworm` image reused as a `psql` client, mounting `go-api/migrations/` read-only, running the new portable `scripts/apply_migrations.sh`, `depends_on: postgres: condition: service_healthy`, `restart: "no"`).
  - `redpanda` (`docker.redpanda.com/redpandadata/redpanda`, pinned tag, single-node `--overprovisioned --smp 1 --memory 512M`, healthcheck via `rpk cluster health` inside the container).
  - `cpp-router` (built from the new root Dockerfile), `EXPOSE 50051 9102`, healthcheck via the bundled `grpc-health-probe`.
  - `go-server` (built from the new Go Dockerfile), env `DATABASE_URL=postgres://chainroute:chainroute@postgres:5432/chainroute?sslmode=disable`, `--grpc-addr=cpp-router:50051`, `depends_on: [postgres (healthy), migrate (completed_successfully), cpp-router (healthy)]`, ports `8080:8080` published to the host (the one service a developer needs from outside), healthcheck via `GET /metrics`.
  - `go-worker` (same image, different `command`/entrypoint arg selecting the worker binary — or a second Dockerfile; see Task-level decision), env `KAFKA_BOOTSTRAP_SERVERS=redpanda:9092`, `depends_on: [postgres (healthy), migrate (completed_successfully), redpanda (healthy)]`, port `9091` published for local Prometheus scraping convenience, healthcheck via `GET /metrics` on its own metrics port.
  - All app/infra services on one explicit user-defined bridge network (`chainroute-net`) — isolated from the default bridge, addressable by service name.
  - `restart: unless-stopped` (bounded — not `always`, which would also restart after an explicit `docker compose stop`) on every long-running service; `migrate` uses `restart: "no"` (a one-shot job that either succeeds or needs investigation, not blind retry).
  - **No `BLOCKCHAIN_ENV` set anywhere in this file** — default compose run is simulated-mode, network-free, by omission (matching the app's own existing default-to-simulated behavior).
  - A second file, **`docker-compose.testnet.yml`**, is a Compose *override* (used via `docker compose -f docker-compose.yml -f docker-compose.testnet.yml up`) that adds `BLOCKCHAIN_ENV=testnet` and the testnet-only env vars to `go-server`/`go-worker`, sourced from a `.env` file (git-ignored) rather than hardcoded — so testnet credentials never enter any committed file, image, or compose YAML literal.
  - A **separate Prometheus config**, `observability/prometheus/prometheus.docker.yml`, scrapes by Compose service name (`go-server:8080`, `go-worker:9091`, `cpp-router:9102`) instead of `host.docker.internal` — used only by a Prometheus service defined inside the new root `docker-compose.yml` (a second, independent Prometheus container from the one in `docker-compose.observability.yml`; running both compose files together is not the intended workflow — each is self-contained). The new `docker-compose.yml` therefore defines its OWN `prometheus`/`otel-collector`/`jaeger`/`grafana` services (same pinned images as Phase 10's, same Grafana provisioning/dashboard files reused by bind-mount, no duplication of *config content*, only of the compose service *definitions* needed to avoid fragile cross-file merging).

**"A developer should be able to start the backend with a documented small number of commands"**: `docker compose up -d` (from repo root) — one command, full stack, simulated mode, migrations applied automatically by the `migrate` service before `go-server`/`go-worker` start.

## 4. Terraform infrastructure (AWS, written and validated, never applied)

`infra/terraform/`:
```
infra/terraform/
├── modules/
│   ├── networking/       # VPC, public+private subnets (2 AZs), 1 NAT gateway, security groups
│   ├── database/         # RDS Postgres (single instance, private subnet, Secrets Manager-managed password)
│   ├── messaging/        # single-node Redpanda on ECS Fargate (see §4.1 tradeoff)
│   ├── ecs-service/       # reusable module: one Fargate service + task def + (optional) Cloud Map registration + (optional) ALB target group attachment
│   └── observability/    # otel-collector + prometheus + grafana + jaeger, each via the ecs-service module, toggleable as a whole
├── environments/
│   └── dev/
│       ├── main.tf        # wires modules together for the dev/demo environment
│       ├── variables.tf
│       ├── terraform.tfvars.example   # placeholders only, never real values
│       └── backend.tf     # remote-state guidance (S3 + DynamoDB lock, commented-out concrete config -- see §4.4)
├── versions.tf             # provider version constraints (root)
└── README.md               # this module tree's own usage doc
```

### 4.1 Kafka tradeoff (explicit, as required)

**Decision: a single-node Redpanda container on ECS Fargate, not MSK or MSK Serverless.** Reasoning: MSK's cheapest realistic configuration still commits to always-on broker cost regardless of traffic; MSK Serverless removes broker-sizing but still carries a non-trivial per-partition-hour + throughput cost floor unsuited to a portfolio project's demo/low-traffic profile. A single Redpanda task mirrors the local Docker Compose setup exactly (same image, same behavior), costs only Fargate's per-vCPU/memory rate for one small task, and is explicitly documented here as **not production-grade** (no replication, no HA — a task restart loses in-flight uncommitted data the same way a local dev restart would). The upgrade path (MSK Serverless, or a 3-node Redpanda cluster across AZs) is named as the production alternative, deliberately not built, because building it would violate the phase's own "do not provision an unnecessarily expensive production-scale Kafka cluster just to make the architecture look sophisticated" instruction.

### 4.2 Networking

VPC with 2 public subnets (ALB only) and 2 private subnets (everything else: ECS tasks, RDS, Redpanda). One NAT gateway (in one public subnet) for private-subnet egress (pulling container images, calling Across/Relay APIs in testnet mode) — documented tradeoff: a single NAT gateway is a single point of failure across AZs for outbound traffic; a second NAT gateway (one per AZ) would double this specific cost for redundancy this portfolio project's scope doesn't need. Security groups: ALB SG allows inbound 443/80 from `0.0.0.0/0`; go-server SG allows inbound 8080 from ALB SG only; cpp-router SG allows inbound 50051/9102 from go-server/go-worker SGs only; RDS SG allows inbound 5432 from go-server/go-worker SGs only; Redpanda SG allows inbound 9092 from go-worker SG only. **No security group allows public ingress to anything except the ALB.**

### 4.3 Compute, data, secrets, observability

- ECS Fargate cluster; each of go-server/go-worker/cpp-router/redpanda/otel-collector/prometheus/grafana/jaeger is one Fargate service via the reusable `ecs-service` module, registered in AWS Cloud Map (`chainroute.local` private DNS namespace) for service-to-service discovery (go-server resolves `cpp-router.chainroute.local:50051`, etc.) — no public DNS for anything but the ALB.
- ECR repository per image (go-server, go-worker, cpp-router), lifecycle policy retaining a bounded number of recent images (cost/hygiene, not unlimited retention).
- RDS Postgres 16, `db.t4g.micro` (smallest practical burstable instance — explicitly sized for a demo/portfolio workload, documented as the first thing to resize for real traffic), automated backups enabled, private subnet only, password generated and stored in Secrets Manager, referenced by ECS task definitions via the `secrets` block (never a plaintext `environment` entry).
- Secrets Manager holds: RDS credentials (auto-rotated reference), `TESTNET_WALLET_PRIVATE_KEY` (only populated if the testnet-enabled variable is set — the base `dev` environment leaves this secret's value unset/placeholder, since the default deployed mode is simulated, matching the local Compose default), `ACROSS_API_KEY`/`RELAY_API_KEY`/RPC URLs. Terraform *creates* the secret resources and references their ARNs in task definitions; it never sets literal secret *values* in `.tfvars` beyond a placeholder that must be replaced out-of-band (e.g. via `aws secretsmanager put-secret-value` run manually, documented in the README) — this is what keeps real secrets out of any committed file or Terraform state diff shown to a reviewer.
- ALB (public subnets) → target group → go-server ECS service only. HTTPS listener (ACM cert — variable-driven, a placeholder/self-signed-friendly default for a demo domain) with an HTTP→HTTPS redirect.
- CloudWatch: ECS `awslogs` log driver for every service's container logs (this is the "logging integration" the brief asks for — free/included with ECS, no separate infra needed); CloudWatch Container Insights optionally enabled per environment variable for basic metrics (CPU/memory) — deliberately not a replacement for the Prometheus/Grafana stack, which remains the actual application-metrics story per Phase 10's design and is deployed (optionally, toggleable) via the `observability` module.
- `enable_observability_stack` (bool variable, default `true` for `dev` but easy to flip `false`) toggles whether the four observability Fargate services are created at all — the explicit, cheap "don't force this cost if you just want the app running" escape hatch the constraints call for.

### 4.4 Remote state

`backend.tf` documents (in comments, not live/applied) the standard S3-backend + DynamoDB-lock-table pattern for team-safe remote state, with a placeholder bucket/table name and the exact `terraform init -backend-config=...` invocation — provided as guidance since this environment has no AWS account to actually create the backend bucket in; local `terraform.tfstate` (git-ignored) is what `terraform validate`/`fmt` run against here.

### 4.5 Variables, outputs, validation

`variables.tf` per module with `type`, `description`, and sensible defaults; root `environments/dev/variables.tf` aggregates the environment-level knobs (region, environment name, `enable_observability_stack`, RDS instance class, desired task counts). `outputs.tf` per module surfaces only non-sensitive values (ALB DNS name, ECR repo URLs, Cloud Map namespace) — **never** a secret ARN's *value*, only its ARN reference (an ARN is not a secret; the underlying secret value is not readable from Terraform output by design once the resource type is `aws_secretsmanager_secret_version` with a placeholder). `terraform.tfvars.example` at the `dev` environment root contains every variable with a placeholder/dummy value and a comment, never a real credential.

## 5. Migration portability

New `scripts/apply_migrations.sh`: portable (uses `psql` from `PATH`, or a `PSQL_BIN` env var override — no hardcoded Homebrew path), applies `go-api/migrations/*.sql` in filename order, reusing the exact same idempotency-guard predicates `scripts/e2e_test.sh` already established per file (table-existence checks, a `pg_constraint`-based check, a column-existence check) rather than inventing new ones. `scripts/e2e_test.sh` is updated to call this shared script instead of its own inline copy — a small, explicitly-justified refactor (removing duplicated, macOS-hardcoded logic in favor of one portable, shared implementation used by both the test script and the new Compose `migrate` service), not a change to the test script's actual test *behavior*.

## 6. CI/CD

`.github/workflows/ci.yml` (triggers: `pull_request`, `push` to `main`) — independent jobs, all validate-only:
- `go`: checkout, `actions/setup-go@v5` pinned to `1.27.1`, `gofmt -l .` (fails on any diff), `go vet ./...`, `go build ./...`, `go test ./...`.
- `go-integration`: same setup plus a `postgres:16` **service container** (GitHub Actions' built-in service-container feature — no separate Docker-in-Docker needed), `go test -tags=integration ./...` against it.
- `kafka-integration`: a `redpanda` **service container** (pinned tag, same image as Compose), `go test -tags=kafka_integration ./internal/kafka/...` against it — this is new CI coverage that doesn't exist as a runnable check anywhere today (every prior phase's Kafka-tagged tests only ever compiled, never ran, for lack of a local broker).
- `cpp`: checkout, `apt-get install` the same Bookworm packages as the Docker builder stage, `cmake`+`ctest` for both `router/` and `cpp-routing-service/`.
- `docker-build`: `docker/build-push-action@v6` with `push: false` for all three Dockerfiles (build-only, proves the Dockerfiles work, no registry credentials needed) — plus `hadolint` (a lightweight, single-binary Dockerfile linter) on each Dockerfile.
- `terraform`: `hashicorp/setup-terraform@v3`, `terraform fmt -check -recursive`, `terraform init -backend=false`, `terraform validate`, for `infra/terraform/environments/dev`.
- `security-scan`: `gitleaks` (secret-scanning, single lightweight action, catches accidentally-committed keys before they ever need a human reviewer to notice) over the diff.

`.github/workflows/deploy.yml` (trigger: `workflow_dispatch` only, `environment: production` with GitHub's required-reviewers protection) — `terraform plan` always; `terraform apply` only if a `confirm_apply` input is explicitly set to `"yes"` in the manual dispatch, using OIDC federation to assume an AWS role (documented, not exercisable without a real AWS account attached to this repo). No secret is ever echoed to a log step.

## 7. Documentation

`README.md` gains: an updated architecture section reflecting Docker/Terraform, a "Docker" section (building images, `docker compose up`, testnet override), a "Terraform" section (module structure, `fmt`/`validate`/`plan` commands, explicit "never `apply` without authorization" note, remote-state guidance), an updated environment-variables section covering the new Docker/Terraform-specific vars (`POSTGRES_PASSWORD` for Compose, `enable_observability_stack` for Terraform, etc.), a networking/data-flow diagram (ASCII, consistent with Phase 10's existing diagram style), a CI/CD section, and an updated benchmark section reporting Phase 11's actual Docker-based benchmark run (see §8) with the same non-fabrication discipline Phase 10 established. `go-api/.env.example` is brought up to date to match README's tables (closing the gap Phase 10's own survey flagged).

## 8. Benchmark against the full Docker stack

If a Docker daemon is reachable when this phase is implemented: start `docker compose up -d`, wait for all healthchecks, run `go run ./go-api/cmd/benchmark -duration=<fixed> -concurrency=<fixed> -base-url=http://localhost:8080 -out=<path>` against the real containerized stack (Kafka/Redpanda genuinely in the loop this time, unlike Phase 10's run), and report the real, measured `throughput_processed_per_sec`/p50/p95/p99 with exact hardware/environment context — never inventing or extrapolating a number. If no Docker daemon is reachable (as has been true in every prior phase of this session), state that plainly and do not claim a throughput figure; report what WAS statically validated instead (Dockerfile correctness by inspection, Compose config validity via `docker compose config`, Terraform `fmt`/`validate`).

## Non-goals (explicit, matching the phase's own constraints)

No React/TypeScript frontend (Phase 12). No production-scale managed Kafka. No multi-region deployment. No Kubernetes (ECS Fargate is the chosen practical compute layer). No automatic CI-to-prod deployment. No changes to routing/payment/execution/recovery/provider-selection semantics. No `terraform apply` against real AWS infrastructure in this environment.
