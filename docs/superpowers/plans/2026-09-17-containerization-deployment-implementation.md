# Phase 11: Containerization, Terraform, and Cloud Deployment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Production Dockerfiles for all three ChainRoute services, a full local Docker Compose backend, a practical AWS Terraform architecture (written/validated, never applied), GitHub Actions CI (validate-only) plus a manual-trigger deploy workflow, and documentation — without touching routing/payment/execution/recovery/observability semantics.

**Architecture:** See `docs/superpowers/specs/2026-09-17-containerization-deployment-design.md` for full rationale. Key decisions locked in: two independent Compose files (no cross-file `include:`); separate Dockerfiles for the Go server and Go worker (no shared image with a switchable entrypoint); single-node Redpanda on ECS Fargate, not MSK; `GET /metrics` as the pragmatic container health signal; a portable `scripts/apply_migrations.sh` shared by the new Compose `migrate` service and the existing `scripts/e2e_test.sh`.

## Global Constraints

- Do not modify `router/src/route.cpp`, `router/include/chainroute/route.hpp`, or any payment/execution/reconciliation/recovery/provider-selection logic anywhere in `go-api/internal/`.
- Default Docker Compose stack is simulated-mode (no `BLOCKCHAIN_ENV` set) and fully network-free. Testnet configuration lives only in the separate `docker-compose.testnet.yml` override plus a git-ignored `.env` file — never hardcoded credentials in any committed YAML.
- No secret ever appears in a Dockerfile, a committed `.env`/`.tfvars` file, a Terraform output, or a CI log. `.env.example`/`terraform.tfvars.example` contain placeholders only.
- Terraform must be `fmt`-clean and `validate`-clean but is **never applied** — no AWS credentials exist in this environment, and applying would incur unauthorized cloud charges.
- Every base image is pinned to an exact tag, never `:latest`.
- CI (`ci.yml`) validates every PR; it never deploys. The deploy workflow (`deploy.yml`) requires manual `workflow_dispatch` and a GitHub Environment approval gate.
- Only the Go server gets public ingress anywhere in the Terraform design. Postgres, Redpanda, the C++ router, the worker, and the observability stack are private-subnet/internal-only.
- Observability containers (Prometheus/OTel Collector/Jaeger/Grafana) must never be a `depends_on: condition: service_healthy` dependency of any app service, in either Compose file or the Terraform ECS service definitions — their unavailability must never block app startup or payment processing.
- Every task that modifies an existing file must read that file's CURRENT content in full first — this plan's code reflects the repo as surveyed at plan-writing time.

---

### Task 1: Portable migration script

**Files:**
- Create: `scripts/apply_migrations.sh`
- Modify: `scripts/e2e_test.sh`

**Interfaces:**
- Produces: `scripts/apply_migrations.sh <DATABASE_URL>` — a standalone, portable script consumed by Task 5's Compose `migrate` service and by this task's own update to `e2e_test.sh`.

- [ ] **Step 1: Read the current migration-application block in `scripts/e2e_test.sh` in full**

Locate the exact section applying all 6 migrations via hardcoded `/opt/homebrew/opt/postgresql@16/bin/psql` calls with their per-file existence guards (table-existence checks for 0001/0002/0004/0005, a `pg_constraint`-based check for 0003, a column-existence check for 0006). Copy every guard predicate exactly — do not re-derive or simplify them; they must remain byte-identical in behavior to what `e2e_test.sh` already correctly does.

- [ ] **Step 2: Write `scripts/apply_migrations.sh`**

```bash
#!/usr/bin/env bash
# Applies every go-api/migrations/*.sql file against DATABASE_URL,
# idempotently -- each migration is skipped if its guard predicate shows
# it was already applied. Portable: uses `psql` from PATH by default, or
# the path given in PSQL_BIN, rather than a hardcoded Homebrew location --
# this is what lets the exact same script run inside a container (where
# `psql` is simply on PATH via the postgres:16 image) and natively on a
# developer's machine (where it must already be resolvable, e.g. via
# `brew link postgresql@16` or PSQL_BIN=/opt/homebrew/opt/postgresql@16/bin/psql).
set -euo pipefail

DATABASE_URL="${1:?usage: apply_migrations.sh <DATABASE_URL>}"
PSQL_BIN="${PSQL_BIN:-psql}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MIGRATIONS_DIR="${MIGRATIONS_DIR:-$SCRIPT_DIR/../go-api/migrations}"

apply_if_missing() {
	local guard_sql="$1" migration_file="$2"
	if "$PSQL_BIN" "$DATABASE_URL" -tAc "$guard_sql" | grep -q 1; then
		echo "apply_migrations: $(basename "$migration_file") already applied, skipping"
		return 0
	fi
	echo "apply_migrations: applying $(basename "$migration_file")"
	"$PSQL_BIN" "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$migration_file"
}

apply_if_missing \
	"SELECT 1 FROM information_schema.tables WHERE table_name='payments'" \
	"$MIGRATIONS_DIR/0001_create_payments.sql"

apply_if_missing \
	"SELECT 1 FROM information_schema.tables WHERE table_name='outbox_events'" \
	"$MIGRATIONS_DIR/0002_payment_processing.sql"

apply_if_missing \
	"SELECT 1 FROM pg_constraint WHERE conname='outbox_events_payment_id_fkey' AND confdeltype='c'" \
	"$MIGRATIONS_DIR/0003_outbox_events_cascade_delete.sql"

apply_if_missing \
	"SELECT 1 FROM information_schema.tables WHERE table_name='payment_executions'" \
	"$MIGRATIONS_DIR/0004_across_testnet_execution.sql"

apply_if_missing \
	"SELECT 1 FROM information_schema.tables WHERE table_name='payment_quotes'" \
	"$MIGRATIONS_DIR/0005_realtime_bridge_routing.sql"

apply_if_missing \
	"SELECT 1 FROM information_schema.columns WHERE table_name='payment_executions' AND column_name='provider_reference_id'" \
	"$MIGRATIONS_DIR/0006_multi_provider_bridge_routing.sql"

echo "apply_migrations: all migrations up to date"
```

Read the CURRENT `e2e_test.sh`'s exact guard SQL text before finalizing this — the sketch above is illustrative of the exact predicates the survey found; confirm each one matches character-for-character against the live script (in particular the exact `pg_constraint`/`information_schema` column names) before shipping, since a mismatched guard would either skip a needed migration or re-run one non-idempotently.

`chmod +x scripts/apply_migrations.sh`.

- [ ] **Step 3: Update `scripts/e2e_test.sh` to call the shared script**

Replace the entire inline hardcoded-`psql`-path migration-application block with a single call:

```bash
PSQL_BIN="/opt/homebrew/opt/postgresql@16/bin/psql" "$ROOT_DIR/scripts/apply_migrations.sh" "$DATABASE_URL"
```

(Preserve the existing hardcoded Homebrew path here specifically — `e2e_test.sh` is a macOS-local dev script and this doesn't change its own environment assumptions, only removes ~6 duplicated inline guard blocks in favor of the shared, portable implementation.) Read the exact surrounding lines in the current file to confirm nothing else in that section (e.g. a variable used later) gets orphaned by this replacement.

- [ ] **Step 4: Test manually against local Postgres**

```bash
export DATABASE_URL="postgres://$(whoami)@localhost:5432/chainroute?sslmode=disable"
PSQL_BIN="/opt/homebrew/opt/postgresql@16/bin/psql" scripts/apply_migrations.sh "$DATABASE_URL"
```

Expected: every migration reports "already applied, skipping" (since this database already has the full Phase 1-10 schema) — this is is the test that the guards correctly detect the current, fully-migrated state without erroring or re-running anything. Then run the full existing `scripts/e2e_test.sh` (or as much of it as the environment supports — Redpanda/Docker availability is a pre-existing, separate constraint unrelated to this task) and confirm it still reaches the same point it did before this change, with the new one-line migration call substituting cleanly for the old inline block.

- [ ] **Step 5: Commit**

```bash
git add scripts/apply_migrations.sh scripts/e2e_test.sh
git commit -m "refactor: extract portable migration-application script from e2e_test.sh"
```

---

### Task 2: Go server and worker Dockerfiles

**Files:**
- Create: `go-api/cmd/server/Dockerfile`
- Create: `go-api/cmd/worker/Dockerfile`
- Create: `go-api/.dockerignore`

**Interfaces:**
- Produces: two buildable images, consumed by Task 5's `docker-compose.yml` (`build: context: ./go-api, dockerfile: cmd/server/Dockerfile` and the worker equivalent) and Task 10's CI `docker-build` job.

- [ ] **Step 1: `go-api/.dockerignore`**

```
.git
**/*_test.go
**/build
**/.env
```

- [ ] **Step 2: `go-api/cmd/server/Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1
FROM golang:1.27-bookworm AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot AS runtime
COPY --from=builder /out/server /app/server
EXPOSE 8080
ENTRYPOINT ["/app/server"]
```

**Build context is `go-api/`** (not the repo root) — the Compose service (Task 5) and any manual build must run `docker build -f cmd/server/Dockerfile .` from inside `go-api/`, or `docker build -f go-api/cmd/server/Dockerfile go-api` from the repo root.

- [ ] **Step 3: `go-api/cmd/worker/Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1
FROM golang:1.27-bookworm AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

FROM gcr.io/distroless/static-debian12:nonroot AS runtime
COPY --from=builder /out/worker /app/worker
EXPOSE 9091
ENTRYPOINT ["/app/worker"]
```

- [ ] **Step 4: Build both images, if a Docker daemon is reachable**

```bash
docker --version >/dev/null 2>&1 && timeout 8 docker info >/dev/null 2>&1 && echo DOCKER_OK || echo DOCKER_UNAVAILABLE
```

If `DOCKER_OK`: `docker build -t chainroute-server:dev -f cmd/server/Dockerfile go-api` (run from repo root) and the worker equivalent, confirm both build to completion, then `docker run --rm chainroute-server:dev` briefly (it will fail fast on missing `DATABASE_URL`, which is EXPECTED and fine — the point is confirming the binary starts and the distroless base actually runs a `CGO_ENABLED=0` Go binary without a missing-libc error; if you see a libc/dynamic-linking error instead of the expected "DATABASE_URL environment variable is required" log line, `CGO_ENABLED=0` did not produce a truly static binary and you must fall back to the design doc's documented alternative: change the runtime stage to `debian:bookworm-slim`, create a non-root user explicitly (`RUN useradd -u 10001 appuser` in that stage, `USER appuser`), and re-test).

If `DOCKER_UNAVAILABLE`: report this explicitly rather than fabricating a successful build — confirm only that `go build ./cmd/server` and `go build ./cmd/worker` succeed natively (already covered by existing CI/regression) and that the Dockerfile syntax is valid via `docker build --help`-independent means is not possible without a daemon; state plainly in your report that the Dockerfile could only be reviewed statically in this environment.

- [ ] **Step 5: Commit**

```bash
git add go-api/cmd/server/Dockerfile go-api/cmd/worker/Dockerfile go-api/.dockerignore
git commit -m "feat: add production Dockerfiles for the Go server and worker"
```

---

### Task 3: C++ router Dockerfile

**Files:**
- Create: `Dockerfile.cpp-router` (repo root — the build context must include `cpp-routing-service/`, `router/`, and `proto/`, three sibling directories, so this Dockerfile cannot live inside any one of them)
- Create: `.dockerignore` (repo root)

**Interfaces:**
- Produces: a buildable image, consumed by Task 5's `docker-compose.yml` (`build: context: ., dockerfile: Dockerfile.cpp-router`) and Task 10's CI `docker-build` job.

- [ ] **Step 1: Root `.dockerignore`**

```
.git
.claude
docs
go-api
router/build
cpp-routing-service/build
*.md
```

- [ ] **Step 2: `Dockerfile.cpp-router`**

```dockerfile
# syntax=docker/dockerfile:1
FROM debian:bookworm-slim AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential cmake protobuf-compiler-grpc libprotobuf-dev \
        libgrpc++-dev libssl-dev pkg-config ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY router/ router/
COPY cpp-routing-service/ cpp-routing-service/
COPY proto/ proto/

RUN cmake -S cpp-routing-service -B /build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build /build -j"$(nproc)" --target chainroute_service_server

# grpc-health-probe: a small static Go binary, pinned version, used by the
# HEALTHCHECK below exactly the way scripts/e2e_test.sh already checks
# this service locally -- no -service= flag, matching the default/unnamed
# health service main.cpp registers via grpc::EnableDefaultHealthCheckService.
ARG GRPC_HEALTH_PROBE_VERSION=v0.4.28
RUN curl -fsSL -o /usr/local/bin/grpc-health-probe \
        "https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc-health-probe-linux-$(dpkg --print-architecture)" \
    && chmod +x /usr/local/bin/grpc-health-probe

FROM debian:bookworm-slim AS runtime
# Runtime-only counterparts of the builder's -dev packages -- exact
# package names must be confirmed once a real build runs (apt-cache
# search/apt list --installed in the builder stage); libprotobuf32 and
# libgrpc++1.51 are Bookworm's expected names as of this plan's writing
# but MUST be verified, not assumed, per Task 3 Step 4 below.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libprotobuf32 libgrpc++1.51 libssl3 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --create-home --shell /usr/sbin/nologin routerapp

COPY --from=builder /build/chainroute_service_server /app/chainroute_service_server
COPY --from=builder /usr/local/bin/grpc-health-probe /usr/local/bin/grpc-health-probe

USER routerapp
EXPOSE 50051 9102
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/grpc-health-probe", "-addr=localhost:50051"]
ENTRYPOINT ["/app/chainroute_service_server"]
```

Note the `ENTRYPOINT` is exec-form with no shell — this is load-bearing, not stylistic: `main.cpp`'s signal-handling design (blocking SIGINT/SIGTERM before spawning gRPC's completion-queue threads, then handling shutdown on a dedicated thread outside async-signal-handler context) is built assuming the binary itself is PID 1 receiving signals directly. A shell-form `CMD`/`ENTRYPOINT` (or any `sh -c "..."` wrapper) would insert a shell as PID 1 that must itself forward SIGTERM correctly — Docker does not guarantee this for shell-form commands. Do not change this to shell form for convenience.

`--listen-address` defaults to `0.0.0.0:50051` in the binary itself already (confirmed in the design survey) — no `CMD` args are needed to bind on all interfaces inside the container.

- [ ] **Step 3: Build, if a Docker daemon is reachable**

```bash
docker build -t chainroute-cpp-router:dev -f Dockerfile.cpp-router .
```

(Run from the repo ROOT — the build context must include all three sibling directories.)

- [ ] **Step 4: Verify the runtime package names, if the build succeeds**

Inside the builder stage (temporarily, for verification only — not part of the shipped Dockerfile), run `dpkg -l | grep -E 'libprotobuf|libgrpc'` to find the EXACT installed runtime package names/versions Bookworm actually provides for the `-dev` packages installed. If they differ from `libprotobuf32`/`libgrpc++1.51` (the plan's best-effort guess at plan-writing time, without a live Debian Bookworm environment to check against), correct the runtime stage's `apt-get install` line to the real names and rebuild to confirm the final image actually starts and serves gRPC (`grpc-health-probe -addr=localhost:50051` against a running container, or at minimum `docker run --rm chainroute-cpp-router:dev` observed not to crash with a missing-shared-library error).

If a Docker daemon is NOT reachable in this environment: report this explicitly. State clearly that the Dockerfile's package names are a best-effort match to Debian Bookworm's published package index (verifiable via `apt-cache` metadata research if internet access is available, even without a Docker daemon) but were not confirmed by an actual build — this is a known, disclosed limitation, not a claimed success.

- [ ] **Step 5: Commit**

```bash
git add Dockerfile.cpp-router .dockerignore
git commit -m "feat: add production Dockerfile for the C++ routing service"
```

---

### Task 4: Docker Compose — app services (Postgres, migrate, Redpanda, C++ router, Go server, Go worker)

**Files:**
- Create: `docker-compose.yml` (repo root)
- Create: `observability/prometheus/prometheus.docker.yml`

**Interfaces:**
- Consumes: Task 1's `scripts/apply_migrations.sh`, Task 2's two Go Dockerfiles, Task 3's C++ Dockerfile.
- Produces: the `chainroute-net` network and the `go-server`/`go-worker`/`cpp-router` service names — consumed by Task 5 (observability services added to this same file).

- [ ] **Step 1: Write `docker-compose.yml`**

```yaml
# Full containerized ChainRoute backend -- simulated mode by default (no
# BLOCKCHAIN_ENV set anywhere below). Start with: docker compose up -d
# For a testnet-enabled run, see docker-compose.testnet.yml.
# This file is independent of docker-compose.observability.yml (which
# remains for the native-binary + observability-only workflow); running
# both together is not the intended usage -- each is self-contained.
name: chainroute

networks:
  chainroute-net:
    driver: bridge

volumes:
  postgres-data:

services:
  postgres:
    image: postgres:16-bookworm
    environment:
      POSTGRES_DB: chainroute
      POSTGRES_USER: chainroute
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-chainroute_local_dev}
    volumes:
      - postgres-data:/var/lib/postgresql/data
    networks: [chainroute-net]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U chainroute"]
      interval: 5s
      timeout: 5s
      retries: 10
    restart: unless-stopped

  migrate:
    image: postgres:16-bookworm
    depends_on:
      postgres:
        condition: service_healthy
    volumes:
      - ./go-api/migrations:/migrations:ro
      - ./scripts/apply_migrations.sh:/apply_migrations.sh:ro
    entrypoint: ["/bin/bash", "/apply_migrations.sh"]
    command: ["postgres://chainroute:${POSTGRES_PASSWORD:-chainroute_local_dev}@postgres:5432/chainroute?sslmode=disable"]
    environment:
      MIGRATIONS_DIR: /migrations
    networks: [chainroute-net]
    restart: "no"

  redpanda:
    image: docker.redpanda.com/redpandadata/redpanda:v24.2.7
    command:
      - redpanda
      - start
      - --smp=1
      - --memory=512M
      - --overprovisioned
      - --node-id=0
      - --check=false
      - --kafka-addr=PLAINTEXT://0.0.0.0:9092
      - --advertise-kafka-addr=PLAINTEXT://redpanda:9092
    networks: [chainroute-net]
    healthcheck:
      test: ["CMD", "rpk", "cluster", "health", "--exit-when-healthy"]
      interval: 5s
      timeout: 5s
      retries: 15
    restart: unless-stopped

  cpp-router:
    build:
      context: .
      dockerfile: Dockerfile.cpp-router
    networks: [chainroute-net]
    restart: unless-stopped
    # HEALTHCHECK is baked into the image (Task 3); no override needed here.

  go-server:
    build:
      context: ./go-api
      dockerfile: cmd/server/Dockerfile
    depends_on:
      postgres:
        condition: service_healthy
      migrate:
        condition: service_completed_successfully
      cpp-router:
        condition: service_healthy
    environment:
      DATABASE_URL: postgres://chainroute:${POSTGRES_PASSWORD:-chainroute_local_dev}@postgres:5432/chainroute?sslmode=disable
      OTEL_EXPORTER_OTLP_ENDPOINT: otel-collector:4317
    command: ["--http-addr=:8080", "--grpc-addr=cpp-router:50051"]
    ports:
      - "8080:8080"
    networks: [chainroute-net]
    # No container-level HEALTHCHECK: distroless has no shell/HTTP client
    # to run one from inside the container, and the app has no dedicated
    # health endpoint yet. External checks (docker compose ps for exit
    # status, or curl http://localhost:8080/metrics from the host) are
    # the practical substitute -- see README's Docker section. The C++
    # router's HEALTHCHECK (Task 3) works because that image is
    # debian:bookworm-slim-based with a normal shell; this is a direct
    # consequence of the two runtime bases differing, not an oversight.
    restart: unless-stopped

  go-worker:
    build:
      context: ./go-api
      dockerfile: cmd/worker/Dockerfile
    depends_on:
      postgres:
        condition: service_healthy
      migrate:
        condition: service_completed_successfully
      redpanda:
        condition: service_healthy
    environment:
      DATABASE_URL: postgres://chainroute:${POSTGRES_PASSWORD:-chainroute_local_dev}@postgres:5432/chainroute?sslmode=disable
      KAFKA_BOOTSTRAP_SERVERS: redpanda:9092
      METRICS_ADDR: ":9091"
      OTEL_EXPORTER_OTLP_ENDPOINT: otel-collector:4317
    ports:
      - "9091:9091"
    networks: [chainroute-net]
    restart: unless-stopped
```

- [ ] **Step 2: `observability/prometheus/prometheus.docker.yml`**

```yaml
global:
  scrape_interval: 5s

scrape_configs:
  - job_name: chainroute-go-api
    static_configs:
      - targets: ["go-server:8080"]
        labels:
          service: go-api
  - job_name: chainroute-worker
    static_configs:
      - targets: ["go-worker:9091"]
        labels:
          service: worker
  - job_name: chainroute-router
    static_configs:
      - targets: ["cpp-router:9102"]
        labels:
          service: router
```

(Mirrors `observability/prometheus/prometheus.yml`'s existing job structure exactly, substituting Compose service names for `host.docker.internal`.)

- [ ] **Step 3: Validate the compose file's syntax**

```bash
docker compose -f docker-compose.yml config
```

This needs only the Docker CLI's compose plugin, not a running daemon (confirmed in a prior phase of this project: `docker compose config` works even when `docker info` itself hangs/fails). Confirm it parses cleanly and every service/network/volume renders as expected, with correct variable substitution for `POSTGRES_PASSWORD`.

- [ ] **Step 4: If a Docker daemon is reachable, start the app-service subset and confirm basic reachability**

```bash
docker compose up -d postgres migrate redpanda cpp-router go-server go-worker
sleep 15
docker compose ps
curl -sf http://localhost:8080/metrics | head -5
docker compose logs go-server --tail 30
docker compose down
```

Report exactly what you observed (all healthy / a specific service failing and why) — do not claim success without this actually having been run, and do not silently skip this step without stating clearly that Docker was unavailable if that's the reason.

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml observability/prometheus/prometheus.docker.yml
git commit -m "feat: add full containerized backend via docker-compose.yml"
```

---

### Task 5: Docker Compose — observability services + testnet override

**Files:**
- Modify: `docker-compose.yml` (add prometheus/otel-collector/jaeger/grafana services)
- Create: `docker-compose.testnet.yml`

**Interfaces:**
- Consumes: Task 4's `docker-compose.yml`, Task 4 Step 3's `prometheus.docker.yml`, the EXISTING (Phase 10, unmodified) `observability/otel-collector/config.yml` and `observability/grafana/provisioning/`+`dashboards/`.

- [ ] **Step 1: Add observability services to `docker-compose.yml`**

Append to the `services:` block (same pinned image tags as the existing `docker-compose.observability.yml`, reused deliberately for consistency):

```yaml
  prometheus:
    image: prom/prometheus:v2.55.1
    volumes:
      - ./observability/prometheus/prometheus.docker.yml:/etc/prometheus/prometheus.yml:ro
    networks: [chainroute-net]
    ports:
      - "9090:9090"
    restart: unless-stopped

  otel-collector:
    image: otel/opentelemetry-collector-contrib:0.113.0
    command: ["--config=/etc/otel-collector-config.yml"]
    volumes:
      - ./observability/otel-collector/config.yml:/etc/otel-collector-config.yml:ro
    networks: [chainroute-net]
    depends_on: [jaeger]
    restart: unless-stopped

  jaeger:
    image: jaegertracing/all-in-one:1.63.0
    environment:
      - COLLECTOR_OTLP_ENABLED=true
    networks: [chainroute-net]
    ports:
      - "16686:16686"
    restart: unless-stopped

  grafana:
    image: grafana/grafana:11.3.1
    environment:
      - GF_AUTH_ANONYMOUS_ENABLED=true
      - GF_AUTH_ANONYMOUS_ORG_ROLE=Admin
    volumes:
      - ./observability/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./observability/grafana/dashboards:/etc/grafana/dashboards:ro
    networks: [chainroute-net]
    ports:
      - "3000:3000"
    depends_on: [prometheus]
    restart: unless-stopped
```

**Critical constraint check**: confirm `go-server`/`go-worker`/`cpp-router`'s `depends_on` blocks (Task 4) do NOT list `otel-collector`/`prometheus`/`grafana`/`jaeger` at all, in either direction — observability containers are independent and their absence/slowness must never block or delay app-service startup (Global Constraints). `go-server`/`go-worker` reference `OTEL_EXPORTER_OTLP_ENDPOINT: otel-collector:4317` as a plain env var (Task 4), which is fine — that's the app optimistically trying to export traces, non-blockingly, exactly as Phase 10's `InitTracing` already guarantees (a non-blocking `BatchSpanProcessor`, confirmed in Phase 10's own final review); it is NOT a startup dependency.

- [ ] **Step 2: `docker-compose.testnet.yml`**

```yaml
# Testnet override -- use with: docker compose -f docker-compose.yml -f docker-compose.testnet.yml up -d
# Requires a .env file (git-ignored) providing TESTNET_WALLET_PRIVATE_KEY,
# ETHEREUM_SEPOLIA_RPC_URL, BASE_SEPOLIA_RPC_URL, and optionally
# ACROSS_API_KEY/ACROSS_INTEGRATOR_ID/RELAY_API_KEY -- never hardcode any
# of these values in this file or any other committed file.
services:
  go-server:
    environment:
      BLOCKCHAIN_ENV: testnet
      ACROSS_API_KEY: ${ACROSS_API_KEY:-}
      ACROSS_INTEGRATOR_ID: ${ACROSS_INTEGRATOR_ID:-}
      RELAY_API_KEY: ${RELAY_API_KEY:-}

  go-worker:
    environment:
      BLOCKCHAIN_ENV: testnet
      TESTNET_WALLET_PRIVATE_KEY: ${TESTNET_WALLET_PRIVATE_KEY:?TESTNET_WALLET_PRIVATE_KEY must be set in .env for testnet mode}
      ETHEREUM_SEPOLIA_RPC_URL: ${ETHEREUM_SEPOLIA_RPC_URL:?ETHEREUM_SEPOLIA_RPC_URL must be set in .env for testnet mode}
      BASE_SEPOLIA_RPC_URL: ${BASE_SEPOLIA_RPC_URL:?BASE_SEPOLIA_RPC_URL must be set in .env for testnet mode}
      ACROSS_API_KEY: ${ACROSS_API_KEY:-}
      ACROSS_INTEGRATOR_ID: ${ACROSS_INTEGRATOR_ID:-}
      RELAY_API_KEY: ${RELAY_API_KEY:-}
```

- [ ] **Step 3: Ensure `.env` is git-ignored**

Check the repo root `.gitignore` for an existing `.env`/`.env.*` rule (the design survey noted secret-exclusion rules already exist at the top of `.gitignore`) — confirm it covers a plain root-level `.env` file (the one Compose auto-loads); add one if missing.

- [ ] **Step 4: Validate**

```bash
docker compose -f docker-compose.yml -f docker-compose.testnet.yml config
```

Confirm this fails cleanly with the `:?` error messages if no `.env` is present (proving the required-variable guards work), and succeeds when a scratch `.env` with dummy values is present (delete the scratch file immediately after testing — never commit it).

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml docker-compose.testnet.yml .gitignore
git commit -m "feat: add observability services to the full stack and a testnet override"
```

---

### Task 6: Terraform — networking and database modules

**Files:**
- Create: `infra/terraform/modules/networking/{main.tf,variables.tf,outputs.tf}`
- Create: `infra/terraform/modules/database/{main.tf,variables.tf,outputs.tf}`

**Interfaces:**
- Produces: `module.networking.vpc_id`, `.public_subnet_ids`, `.private_subnet_ids`, `.alb_security_group_id`, `.app_security_group_id`, `.data_security_group_id` — consumed by every later Terraform task. `module.database.endpoint`, `.secret_arn` — consumed by Task 9's root wiring.

- [ ] **Step 1: `modules/networking/main.tf`**

```hcl
data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags = { Name = "${var.environment}-chainroute-vpc" }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "${var.environment}-chainroute-igw" }
}

resource "aws_subnet" "public" {
  count                   = 2
  vpc_id                  = aws_vpc.main.id
  cidr_block              = cidrsubnet(var.vpc_cidr, 4, count.index)
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true
  tags                    = { Name = "${var.environment}-chainroute-public-${count.index}" }
}

resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = aws_vpc.main.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 4, count.index + 2)
  availability_zone = data.aws_availability_zones.available.names[count.index]
  tags              = { Name = "${var.environment}-chainroute-private-${count.index}" }
}

# Single NAT gateway (in the first public subnet) for private-subnet
# egress -- documented cost/availability tradeoff (design doc §4.2): a
# single NAT is a single point of failure across AZs for outbound
# traffic; a per-AZ NAT gateway would double this specific cost for
# redundancy this portfolio-scale deployment doesn't need.
resource "aws_eip" "nat" {
  domain = "vpc"
  tags   = { Name = "${var.environment}-chainroute-nat-eip" }
}

resource "aws_nat_gateway" "main" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id
  tags          = { Name = "${var.environment}-chainroute-nat" }
  depends_on    = [aws_internet_gateway.main]
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }
  tags = { Name = "${var.environment}-chainroute-public-rt" }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.main.id
  }
  tags = { Name = "${var.environment}-chainroute-private-rt" }
}

resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "private" {
  count          = 2
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# Security groups -- only the ALB accepts public ingress anywhere in this
# design (design doc §4.2). Every other SG's ingress is scoped to another
# SG, never a CIDR block.
resource "aws_security_group" "alb" {
  name_prefix = "${var.environment}-chainroute-alb-"
  vpc_id      = aws_vpc.main.id
  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${var.environment}-chainroute-alb-sg" }
}

resource "aws_security_group" "app" {
  name_prefix = "${var.environment}-chainroute-app-"
  vpc_id      = aws_vpc.main.id
  ingress {
    description     = "go-server HTTP from ALB only"
    from_port       = 8080
    to_port         = 8080
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${var.environment}-chainroute-app-sg" }
}

resource "aws_security_group" "internal" {
  name_prefix = "${var.environment}-chainroute-internal-"
  vpc_id      = aws_vpc.main.id
  description = "Shared SG for cpp-router, redpanda, observability, and any service-to-service traffic -- ingress rules are added per-consumer by later modules via aws_security_group_rule, not baked in here, to avoid a monolithic SG with rules for every port up front."
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${var.environment}-chainroute-internal-sg" }
}

resource "aws_security_group" "data" {
  name_prefix = "${var.environment}-chainroute-data-"
  vpc_id      = aws_vpc.main.id
  ingress {
    description     = "Postgres from app/worker services only"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.app.id, aws_security_group.internal.id]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${var.environment}-chainroute-data-sg" }
}

resource "aws_service_discovery_private_dns_namespace" "internal" {
  name = "chainroute.local"
  vpc  = aws_vpc.main.id
}
```

- [ ] **Step 2: `modules/networking/variables.tf`**

```hcl
variable "environment" {
  description = "Deployment environment name (e.g. dev, prod), used in resource name prefixes."
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.20.0.0/16"
}
```

- [ ] **Step 3: `modules/networking/outputs.tf`**

```hcl
output "vpc_id" {
  value = aws_vpc.main.id
}

output "public_subnet_ids" {
  value = aws_subnet.public[*].id
}

output "private_subnet_ids" {
  value = aws_subnet.private[*].id
}

output "alb_security_group_id" {
  value = aws_security_group.alb.id
}

output "app_security_group_id" {
  value = aws_security_group.app.id
}

output "internal_security_group_id" {
  value = aws_security_group.internal.id
}

output "data_security_group_id" {
  value = aws_security_group.data.id
}

output "service_discovery_namespace_id" {
  value = aws_service_discovery_private_dns_namespace.internal.id
}
```

- [ ] **Step 4: `modules/database/main.tf`**

```hcl
resource "random_password" "db" {
  length  = 32
  special = false
}

resource "aws_secretsmanager_secret" "db" {
  name_prefix = "${var.environment}-chainroute-db-"
}

resource "aws_secretsmanager_secret_version" "db" {
  secret_id = aws_secretsmanager_secret.db.id
  secret_string = jsonencode({
    username = "chainroute"
    password = random_password.db.result
  })
}

resource "aws_db_subnet_group" "main" {
  name_prefix = "${var.environment}-chainroute-"
  subnet_ids  = var.private_subnet_ids
}

resource "aws_db_instance" "main" {
  identifier_prefix      = "${var.environment}-chainroute-"
  engine                 = "postgres"
  engine_version         = "16"
  instance_class         = var.instance_class
  allocated_storage      = 20
  db_name                = "chainroute"
  username               = "chainroute"
  password               = random_password.db.result
  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [var.data_security_group_id]
  publicly_accessible    = false
  backup_retention_period = 7
  skip_final_snapshot    = var.environment != "prod"
  tags                   = { Name = "${var.environment}-chainroute-postgres" }
}
```

`instance_class` defaults to `db.t4g.micro` (design doc §4.3, explicitly the smallest practical burstable size, sized for a demo/portfolio workload — resize first if real traffic materializes).

- [ ] **Step 5: `modules/database/variables.tf`**

```hcl
variable "environment" {
  type = string
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "data_security_group_id" {
  type = string
}

variable "instance_class" {
  type    = string
  default = "db.t4g.micro"
}
```

- [ ] **Step 6: `modules/database/outputs.tf`**

```hcl
output "endpoint" {
  value = aws_db_instance.main.endpoint
}

output "secret_arn" {
  value = aws_secretsmanager_secret.db.arn
}
```

(No output ever exposes `random_password.db.result` or the secret's decoded value — only the ARN, per the Global Constraints. `terraform output` for this module can never print a real credential.)

- [ ] **Step 7: Commit**

```bash
git add infra/terraform/modules/networking infra/terraform/modules/database
git commit -m "feat(terraform): add networking and database modules"
```

---

### Task 7: Terraform — reusable ECS service module and Redpanda messaging module

**Files:**
- Create: `infra/terraform/modules/ecs-service/{main.tf,variables.tf,outputs.tf}`
- Create: `infra/terraform/modules/messaging/{main.tf,variables.tf,outputs.tf}`

**Interfaces:**
- Produces: `module.ecs-service` (reusable, instantiated once per Fargate service by Tasks 8-9 and the root environment) and `module.messaging.redpanda_endpoint` — consumed by Task 9's root wiring.

- [ ] **Step 1: `modules/ecs-service/variables.tf`** (the reusable module's exact contract — every later task's instantiation must match this)

```hcl
variable "environment" {
  type = string
}

variable "service_name" {
  description = "Short name, e.g. \"go-server\", used in resource names and the Cloud Map service name."
  type        = string
}

variable "cluster_id" {
  type = string
}

variable "image" {
  description = "Full ECR image URI including tag."
  type        = string
}

variable "container_port" {
  type = number
}

variable "cpu" {
  type    = number
  default = 256
}

variable "memory" {
  type    = number
  default = 512
}

variable "desired_count" {
  type    = number
  default = 1
}

variable "subnet_ids" {
  type = list(string)
}

variable "security_group_ids" {
  type = list(string)
}

variable "environment_variables" {
  description = "Plain (non-secret) container env vars."
  type        = map(string)
  default     = {}
}

variable "secrets" {
  description = "Map of container env var name -> Secrets Manager ARN (with an optional ::jsonkey suffix), injected via the ECS task definition's `secrets` block, never a literal value."
  type        = map(string)
  default     = {}
}

variable "log_group_name" {
  type = string
}

variable "execution_role_arn" {
  type = string
}

variable "task_role_arn" {
  type = string
}

variable "service_discovery_namespace_id" {
  description = "If set, registers this service in Cloud Map under this namespace for internal DNS resolution (e.g. cpp-router.chainroute.local)."
  type        = string
  default     = null
}

variable "target_group_arn" {
  description = "If set, attaches this service to an ALB target group (only go-server should ever set this)."
  type        = string
  default     = null
}
```

- [ ] **Step 2: `modules/ecs-service/main.tf`**

```hcl
resource "aws_ecs_task_definition" "this" {
  family                   = "${var.environment}-chainroute-${var.service_name}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.cpu
  memory                   = var.memory
  execution_role_arn       = var.execution_role_arn
  task_role_arn            = var.task_role_arn

  container_definitions = jsonencode([{
    name      = var.service_name
    image     = var.image
    essential = true
    portMappings = [{
      containerPort = var.container_port
      protocol      = "tcp"
    }]
    environment = [for k, v in var.environment_variables : { name = k, value = v }]
    secrets     = [for k, arn in var.secrets : { name = k, valueFrom = arn }]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = var.log_group_name
        "awslogs-region"        = data.aws_region.current.name
        "awslogs-stream-prefix" = var.service_name
      }
    }
  }])
}

data "aws_region" "current" {}

resource "aws_service_discovery_service" "this" {
  count = var.service_discovery_namespace_id == null ? 0 : 1
  name  = var.service_name
  dns_config {
    namespace_id = var.service_discovery_namespace_id
    dns_records {
      ttl  = 10
      type = "A"
    }
  }
}

resource "aws_ecs_service" "this" {
  name            = "${var.environment}-chainroute-${var.service_name}"
  cluster         = var.cluster_id
  task_definition = aws_ecs_task_definition.this.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = var.subnet_ids
    security_groups = var.security_group_ids
  }

  dynamic "service_registries" {
    for_each = var.service_discovery_namespace_id == null ? [] : [1]
    content {
      registry_arn = aws_service_discovery_service.this[0].arn
    }
  }

  dynamic "load_balancer" {
    for_each = var.target_group_arn == null ? [] : [1]
    content {
      target_group_arn = var.target_group_arn
      container_name    = var.service_name
      container_port    = var.container_port
    }
  }
}
```

- [ ] **Step 3: `modules/ecs-service/outputs.tf`**

```hcl
output "service_name" {
  value = aws_ecs_service.this.name
}

output "task_definition_arn" {
  value = aws_ecs_task_definition.this.arn
}
```

- [ ] **Step 4: `modules/messaging/main.tf`** — single-node Redpanda on Fargate (design doc §4.1's explicit, documented tradeoff — not MSK)

```hcl
# Single-node Redpanda on ECS Fargate -- deliberately NOT MSK/MSK
# Serverless (design doc §4.1): mirrors the local Docker Compose setup
# exactly, costs only one small Fargate task, and is explicitly NOT
# production-grade (no replication, no HA -- a task restart loses
# in-flight uncommitted data the same way a local dev restart would).
# The production upgrade path is MSK Serverless or a multi-broker
# Redpanda cluster across AZs; deliberately not built here, since doing
# so would be exactly the "unnecessarily expensive production-scale
# Kafka cluster to look sophisticated" this phase was told not to add.
module "redpanda_service" {
  source = "../ecs-service"

  environment            = var.environment
  service_name           = "redpanda"
  cluster_id              = var.cluster_id
  image                   = "docker.redpanda.com/redpandadata/redpanda:v24.2.7"
  container_port          = 9092
  cpu                     = 512
  memory                  = 1024
  subnet_ids              = var.private_subnet_ids
  security_group_ids      = [var.internal_security_group_id]
  log_group_name          = var.log_group_name
  execution_role_arn      = var.execution_role_arn
  task_role_arn           = var.task_role_arn
  service_discovery_namespace_id = var.service_discovery_namespace_id
}
```

- [ ] **Step 5: `modules/messaging/variables.tf`** and **`outputs.tf`**

```hcl
# variables.tf
variable "environment" { type = string }
variable "cluster_id" { type = string }
variable "private_subnet_ids" { type = list(string) }
variable "internal_security_group_id" { type = string }
variable "log_group_name" { type = string }
variable "execution_role_arn" { type = string }
variable "task_role_arn" { type = string }
variable "service_discovery_namespace_id" { type = string }
```

```hcl
# outputs.tf
output "bootstrap_endpoint" {
  value = "redpanda.chainroute.local:9092"
}
```

- [ ] **Step 6: Commit**

```bash
git add infra/terraform/modules/ecs-service infra/terraform/modules/messaging
git commit -m "feat(terraform): add reusable ECS service module and single-node Redpanda messaging module"
```

---

### Task 8: Terraform — observability module and ALB

**Files:**
- Create: `infra/terraform/modules/observability/{main.tf,variables.tf,outputs.tf}`
- Create: `infra/terraform/modules/alb/{main.tf,variables.tf,outputs.tf}`

**Interfaces:**
- Consumes: `modules/ecs-service` (Task 7).
- Produces: `module.observability` (toggleable via its own `enabled` variable, checked at the ROOT level per design doc §4.3 — this module itself always declares its resources, but the root wiring in Task 9 only instantiates it when `enable_observability_stack = true`, via a root-level `count`/conditional module call), `module.alb.dns_name`, `.target_group_arn`.

- [ ] **Step 1: `modules/observability/main.tf`** — four Fargate services via the reusable module, each independent, none blocking app startup (this is a Terraform-level structural fact, not something to re-derive: nothing in `modules/ecs-service` or the root wiring ever makes an app service's `depends_on` reference one of these)

```hcl
module "otel_collector" {
  source = "../ecs-service"

  environment        = var.environment
  service_name       = "otel-collector"
  cluster_id         = var.cluster_id
  image              = "otel/opentelemetry-collector-contrib:0.113.0"
  container_port     = 4317
  cpu                = 256
  memory             = 512
  subnet_ids         = var.private_subnet_ids
  security_group_ids = [var.internal_security_group_id]
  log_group_name     = var.log_group_name
  execution_role_arn = var.execution_role_arn
  task_role_arn      = var.task_role_arn
  service_discovery_namespace_id = var.service_discovery_namespace_id
}

module "jaeger" {
  source = "../ecs-service"

  environment        = var.environment
  service_name       = "jaeger"
  cluster_id         = var.cluster_id
  image              = "jaegertracing/all-in-one:1.63.0"
  container_port     = 16686
  cpu                = 256
  memory             = 512
  environment_variables = { COLLECTOR_OTLP_ENABLED = "true" }
  subnet_ids         = var.private_subnet_ids
  security_group_ids = [var.internal_security_group_id]
  log_group_name     = var.log_group_name
  execution_role_arn = var.execution_role_arn
  task_role_arn      = var.task_role_arn
  service_discovery_namespace_id = var.service_discovery_namespace_id
}

module "prometheus" {
  source = "../ecs-service"

  environment        = var.environment
  service_name       = "prometheus"
  cluster_id         = var.cluster_id
  image              = "prom/prometheus:v2.55.1"
  container_port     = 9090
  cpu                = 256
  memory             = 512
  subnet_ids         = var.private_subnet_ids
  security_group_ids = [var.internal_security_group_id]
  log_group_name     = var.log_group_name
  execution_role_arn = var.execution_role_arn
  task_role_arn      = var.task_role_arn
  service_discovery_namespace_id = var.service_discovery_namespace_id
}

module "grafana" {
  source = "../ecs-service"

  environment        = var.environment
  service_name       = "grafana"
  cluster_id         = var.cluster_id
  image              = "grafana/grafana:11.3.1"
  container_port     = 3000
  cpu                = 256
  memory             = 512
  environment_variables = {
    GF_AUTH_ANONYMOUS_ENABLED  = "true"
    GF_AUTH_ANONYMOUS_ORG_ROLE = "Admin"
  }
  subnet_ids         = var.private_subnet_ids
  security_group_ids = [var.internal_security_group_id]
  log_group_name     = var.log_group_name
  execution_role_arn = var.execution_role_arn
  task_role_arn      = var.task_role_arn
  service_discovery_namespace_id = var.service_discovery_namespace_id
}
```

Note: unlike the local Compose setup, Grafana/Prometheus's config-file bind-mounts don't have a direct ECS equivalent without an EFS volume or baking config into a custom image layered on top of the upstream `prom/prometheus`/`grafana/grafana` images. For THIS phase, document (in the module's own README section, not hidden) that provisioning the actual Prometheus scrape-config/Grafana dashboard content into these cloud containers is a follow-up (e.g. a small custom image per service with the config baked in via `COPY`, or an EFS-backed volume) — do not silently pretend this module ships fully-configured Prometheus/Grafana; it provisions the compute/networking shell correctly, and the config-delivery mechanism is explicitly named as unfinished rather than glossed over. This is consistent with "do not over-engineer" — building a full EFS+custom-image config pipeline for four optional observability containers in a portfolio project's Terraform is disproportionate; the honest, disclosed gap is preferable to invisible complexity.

- [ ] **Step 2: `modules/observability/variables.tf`/`outputs.tf`** — same variable shape as `modules/messaging` (environment, cluster_id, private_subnet_ids, internal_security_group_id, log_group_name, execution_role_arn, task_role_arn, service_discovery_namespace_id); no outputs needed beyond what Terraform surfaces automatically, since nothing else in the root wiring reads from this module.

- [ ] **Step 3: `modules/alb/main.tf`**

```hcl
resource "aws_lb" "main" {
  name_prefix        = "cr${substr(var.environment, 0, 3)}"
  load_balancer_type = "application"
  security_groups    = [var.alb_security_group_id]
  subnets            = var.public_subnet_ids
}

resource "aws_lb_target_group" "go_server" {
  name_prefix = "crgs-"
  port        = 8080
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"
  health_check {
    path                = "/metrics"
    healthy_threshold   = 2
    unhealthy_threshold = 3
    interval            = 15
    timeout             = 5
  }
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.main.arn
  port              = 80
  protocol          = "HTTP"
  default_action {
    type = "redirect"
    redirect {
      port        = "443"
      protocol    = "HTTPS"
      status_code = "HTTP_301"
    }
  }
}

resource "aws_lb_listener" "https" {
  load_balancer_arn = aws_lb.main.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-2016-08"
  certificate_arn   = var.acm_certificate_arn
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.go_server.arn
  }
}
```

`GET /metrics` as the target-group health check (design doc §2.1's explicit, disclosed pragmatic choice — no dedicated `/healthz` exists yet).

- [ ] **Step 4: `modules/alb/variables.tf`/`outputs.tf`** — `vpc_id`, `public_subnet_ids`, `alb_security_group_id`, `acm_certificate_arn` (string, no default — the root `tfvars.example` documents this as a placeholder ARN the deployer must supply for their own domain); outputs `dns_name` (`aws_lb.main.dns_name`) and `target_group_arn`.

- [ ] **Step 5: Commit**

```bash
git add infra/terraform/modules/observability infra/terraform/modules/alb
git commit -m "feat(terraform): add observability module (compute/networking shell) and public ALB module"
```

---

### Task 9: Terraform — root environment wiring, versions, backend guidance, tfvars example

**Files:**
- Create: `infra/terraform/versions.tf`
- Create: `infra/terraform/environments/dev/{main.tf,variables.tf,outputs.tf,backend.tf,terraform.tfvars.example}`
- Create: `infra/terraform/README.md`

**Interfaces:**
- Consumes: every module from Tasks 6-8.
- Produces: nothing further consumed — this is the leaf/root of the module tree.

- [ ] **Step 1: `infra/terraform/versions.tf`**

```hcl
terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}
```

- [ ] **Step 2: `environments/dev/main.tf`**

```hcl
provider "aws" {
  region = var.aws_region
}

module "networking" {
  source      = "../../modules/networking"
  environment = var.environment
}

resource "aws_cloudwatch_log_group" "chainroute" {
  name              = "/ecs/${var.environment}-chainroute"
  retention_in_days = 14
}

resource "aws_ecs_cluster" "main" {
  name = "${var.environment}-chainroute"
}

resource "aws_iam_role" "ecs_execution" {
  name_prefix = "${var.environment}-chainroute-exec-"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_execution" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "ecs_task" {
  name_prefix = "${var.environment}-chainroute-task-"
  assume_role_policy = aws_iam_role.ecs_execution.assume_role_policy
}

module "database" {
  source                  = "../../modules/database"
  environment             = var.environment
  private_subnet_ids      = module.networking.private_subnet_ids
  data_security_group_id  = module.networking.data_security_group_id
  instance_class          = var.db_instance_class
}

module "messaging" {
  source                          = "../../modules/messaging"
  environment                     = var.environment
  cluster_id                      = aws_ecs_cluster.main.id
  private_subnet_ids              = module.networking.private_subnet_ids
  internal_security_group_id      = module.networking.internal_security_group_id
  log_group_name                  = aws_cloudwatch_log_group.chainroute.name
  execution_role_arn               = aws_iam_role.ecs_execution.arn
  task_role_arn                    = aws_iam_role.ecs_task.arn
  service_discovery_namespace_id   = module.networking.service_discovery_namespace_id
}

module "observability" {
  count  = var.enable_observability_stack ? 1 : 0
  source = "../../modules/observability"

  environment                     = var.environment
  cluster_id                      = aws_ecs_cluster.main.id
  private_subnet_ids              = module.networking.private_subnet_ids
  internal_security_group_id      = module.networking.internal_security_group_id
  log_group_name                  = aws_cloudwatch_log_group.chainroute.name
  execution_role_arn               = aws_iam_role.ecs_execution.arn
  task_role_arn                    = aws_iam_role.ecs_task.arn
  service_discovery_namespace_id   = module.networking.service_discovery_namespace_id
}

module "alb" {
  source                 = "../../modules/alb"
  vpc_id                 = module.networking.vpc_id
  public_subnet_ids      = module.networking.public_subnet_ids
  alb_security_group_id  = module.networking.alb_security_group_id
  acm_certificate_arn    = var.acm_certificate_arn
}

resource "aws_ecr_repository" "go_server" {
  name = "${var.environment}-chainroute-go-server"
}

resource "aws_ecr_repository" "go_worker" {
  name = "${var.environment}-chainroute-go-worker"
}

resource "aws_ecr_repository" "cpp_router" {
  name = "${var.environment}-chainroute-cpp-router"
}

resource "aws_ecr_lifecycle_policy" "bounded_retention" {
  for_each   = { server = aws_ecr_repository.go_server.name, worker = aws_ecr_repository.go_worker.name, router = aws_ecr_repository.cpp_router.name }
  repository = each.value
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep only the last 10 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 10
      }
      action = { type = "expire" }
    }]
  })
}

module "go_server_service" {
  source = "../../modules/ecs-service"

  environment        = var.environment
  service_name       = "go-server"
  cluster_id         = aws_ecs_cluster.main.id
  image              = "${aws_ecr_repository.go_server.repository_url}:${var.image_tag}"
  container_port     = 8080
  subnet_ids         = module.networking.private_subnet_ids
  security_group_ids = [module.networking.app_security_group_id]
  environment_variables = {
    DATABASE_URL              = "postgres://chainroute@${module.database.endpoint}/chainroute?sslmode=require"
    OTEL_EXPORTER_OTLP_ENDPOINT = "otel-collector.chainroute.local:4317"
  }
  secrets             = { DATABASE_PASSWORD = module.database.secret_arn }
  log_group_name      = aws_cloudwatch_log_group.chainroute.name
  execution_role_arn  = aws_iam_role.ecs_execution.arn
  task_role_arn       = aws_iam_role.ecs_task.arn
  target_group_arn    = module.alb.target_group_arn
}

module "go_worker_service" {
  source = "../../modules/ecs-service"

  environment        = var.environment
  service_name       = "go-worker"
  cluster_id         = aws_ecs_cluster.main.id
  image              = "${aws_ecr_repository.go_worker.repository_url}:${var.image_tag}"
  container_port     = 9091
  subnet_ids         = module.networking.private_subnet_ids
  security_group_ids = [module.networking.internal_security_group_id]
  environment_variables = {
    DATABASE_URL             = "postgres://chainroute@${module.database.endpoint}/chainroute?sslmode=require"
    KAFKA_BOOTSTRAP_SERVERS  = module.messaging.bootstrap_endpoint
    OTEL_EXPORTER_OTLP_ENDPOINT = "otel-collector.chainroute.local:4317"
  }
  secrets            = { DATABASE_PASSWORD = module.database.secret_arn }
  log_group_name     = aws_cloudwatch_log_group.chainroute.name
  execution_role_arn = aws_iam_role.ecs_execution.arn
  task_role_arn      = aws_iam_role.ecs_task.arn
}

module "cpp_router_service" {
  source = "../../modules/ecs-service"

  environment        = var.environment
  service_name       = "cpp-router"
  cluster_id         = aws_ecs_cluster.main.id
  image              = "${aws_ecr_repository.cpp_router.repository_url}:${var.image_tag}"
  container_port     = 50051
  subnet_ids         = module.networking.private_subnet_ids
  security_group_ids = [module.networking.internal_security_group_id]
  log_group_name     = aws_cloudwatch_log_group.chainroute.name
  execution_role_arn = aws_iam_role.ecs_execution.arn
  task_role_arn      = aws_iam_role.ecs_task.arn
  service_discovery_namespace_id = module.networking.service_discovery_namespace_id
}
```

- [ ] **Step 3: `environments/dev/variables.tf`**

```hcl
variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "db_instance_class" {
  type    = string
  default = "db.t4g.micro"
}

variable "enable_observability_stack" {
  description = "Toggle the Prometheus/OTel-Collector/Jaeger/Grafana ECS services entirely -- the explicit cost/complexity escape hatch (design doc §4.3)."
  type        = bool
  default     = true
}

variable "acm_certificate_arn" {
  description = "ACM certificate ARN for the ALB's HTTPS listener -- placeholder in terraform.tfvars.example, must be supplied per-deployment for a real domain."
  type        = string
}

variable "image_tag" {
  description = "Image tag to deploy for all three app services (typically a git SHA from CI)."
  type        = string
  default     = "latest"
}
```

- [ ] **Step 4: `environments/dev/outputs.tf`**

```hcl
output "alb_dns_name" {
  value = module.alb.dns_name
}

output "ecr_go_server_repository_url" {
  value = aws_ecr_repository.go_server.repository_url
}

output "ecr_go_worker_repository_url" {
  value = aws_ecr_repository.go_worker.repository_url
}

output "ecr_cpp_router_repository_url" {
  value = aws_ecr_repository.cpp_router.repository_url
}

output "database_secret_arn" {
  value = module.database.secret_arn
}
```

(Every output is a URL, DNS name, or ARN — never a secret value, per Global Constraints.)

- [ ] **Step 5: `environments/dev/backend.tf`** — remote-state guidance, commented out (no real S3 bucket/DynamoDB table exists to point at in this environment)

```hcl
# Remote state (recommended for any team/shared use -- NOT configured by
# default so `terraform init`/`validate` work with only local state in
# this repo, without requiring a pre-existing S3 bucket/DynamoDB table).
#
# To enable: create the bucket + lock table once (e.g. via a separate,
# one-time bootstrap `terraform apply` or the AWS CLI), then uncomment
# and fill in the real names below, and run:
#   terraform init -migrate-state
#
# terraform {
#   backend "s3" {
#     bucket         = "REPLACE-ME-chainroute-tfstate"
#     key            = "dev/terraform.tfstate"
#     region         = "us-east-1"
#     dynamodb_table = "REPLACE-ME-chainroute-tflock"
#     encrypt        = true
#   }
# }
```

- [ ] **Step 6: `environments/dev/terraform.tfvars.example`**

```hcl
aws_region                 = "us-east-1"
environment                 = "dev"
db_instance_class           = "db.t4g.micro"
enable_observability_stack  = true
acm_certificate_arn         = "arn:aws:acm:us-east-1:123456789012:certificate/REPLACE-ME"
image_tag                   = "latest"
```

Every value here is a placeholder or a genuinely safe default (no real account ID, no real certificate). Copy to `terraform.tfvars` (git-ignored) before running `plan`/`apply` for real.

- [ ] **Step 7: Add Terraform state/vars to `.gitignore`**

Confirm `.gitignore` covers `*.tfstate`, `*.tfstate.*`, `.terraform/`, `terraform.tfvars` (the real, filled-in file — NOT `.tfvars.example`, which must stay tracked) — add any missing entries.

- [ ] **Step 8: `infra/terraform/README.md`** — module tree overview, how to run `fmt`/`validate`/`plan` locally, explicit "never `apply` without real AWS credentials and authorization" note, and a one-paragraph summary of the Redpanda/MSK tradeoff (cross-referencing the design doc, not duplicating its full reasoning).

- [ ] **Step 9: Run formatting and validation**

```bash
cd infra/terraform
terraform fmt -recursive -check -diff
# if the above reports diffs:
terraform fmt -recursive
cd environments/dev
cp terraform.tvars.example terraform.tfvars   # scratch copy for validation only, delete after -- do NOT commit
terraform init -backend=false
terraform validate
rm terraform.tfvars
```

Fix every `fmt`/`validate` error until both are clean. Do **not** run `terraform plan` or `terraform apply` — no AWS credentials exist in this environment, and `plan` against a real provider requires them (the Terraform docs note `validate` alone does not need provider credentials, only `-backend=false init` and syntactically-valid provider blocks — confirm this holds for `aws ~> 5.0` before assuming `validate` genuinely doesn't reach out to AWS; if it does need credentials for some reason, report that clearly rather than working around it with fabricated ones).

- [ ] **Step 10: Commit**

```bash
git add infra/terraform/versions.tf infra/terraform/environments infra/terraform/README.md .gitignore
git commit -m "feat(terraform): wire root dev environment, add backend guidance and tfvars example"
```

---

### Task 10: GitHub Actions — CI workflow

**Files:**
- Create: `.github/workflows/ci.yml`

**Interfaces:** none (leaf task, consumes only existing repo structure).

- [ ] **Step 1: Write `.github/workflows/ci.yml`**

```yaml
name: CI

on:
  pull_request:
  push:
    branches: [main]

jobs:
  go:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.27.1"
          cache-dependency-path: go-api/go.sum
      - name: gofmt check
        working-directory: go-api
        run: test -z "$(gofmt -l .)"
      - name: go vet
        working-directory: go-api
        run: go vet ./...
      - name: go build
        working-directory: go-api
        run: go build ./...
      - name: go test
        working-directory: go-api
        run: go test ./...

  go-integration:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16-bookworm
        env:
          POSTGRES_DB: chainroute
          POSTGRES_USER: chainroute
          POSTGRES_PASSWORD: chainroute_ci
        ports: ["5432:5432"]
        options: >-
          --health-cmd "pg_isready -U chainroute"
          --health-interval 5s
          --health-timeout 5s
          --health-retries 10
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.27.1"
          cache-dependency-path: go-api/go.sum
      - name: apply migrations
        run: PSQL_BIN=psql scripts/apply_migrations.sh "postgres://chainroute:chainroute_ci@localhost:5432/chainroute?sslmode=disable"
      - name: go test -tags=integration
        working-directory: go-api
        env:
          DATABASE_URL: postgres://chainroute:chainroute_ci@localhost:5432/chainroute?sslmode=disable
        run: go test -tags=integration ./...

  kafka-integration:
    runs-on: ubuntu-latest
    services:
      redpanda:
        image: docker.redpanda.com/redpandadata/redpanda:v24.2.7
        ports: ["9092:9092"]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.27.1"
          cache-dependency-path: go-api/go.sum
      - name: go test -tags=kafka_integration
        working-directory: go-api
        env:
          KAFKA_BOOTSTRAP_SERVERS: localhost:9092
        run: go test -tags=kafka_integration ./internal/kafka/...

  cpp:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: install build dependencies
        run: |
          sudo apt-get update
          sudo apt-get install -y build-essential cmake protobuf-compiler-grpc libprotobuf-dev libgrpc++-dev libssl-dev pkg-config
      - name: build and test router
        run: |
          cmake -S router -B router/build -DCMAKE_BUILD_TYPE=Release
          cmake --build router/build -j
          ctest --test-dir router/build --output-on-failure
      - name: build and test cpp-routing-service
        run: |
          cmake -S cpp-routing-service -B cpp-routing-service/build -DCMAKE_BUILD_TYPE=Release
          cmake --build cpp-routing-service/build -j
          ctest --test-dir cpp-routing-service/build --output-on-failure

  docker-build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - name: build go-server image
        uses: docker/build-push-action@v6
        with:
          context: go-api
          file: go-api/cmd/server/Dockerfile
          push: false
      - name: build go-worker image
        uses: docker/build-push-action@v6
        with:
          context: go-api
          file: go-api/cmd/worker/Dockerfile
          push: false
      - name: build cpp-router image
        uses: docker/build-push-action@v6
        with:
          context: .
          file: Dockerfile.cpp-router
          push: false
      - name: hadolint (cpp-router)
        uses: hadolint/hadolint-action@v3.1.0
        with:
          dockerfile: Dockerfile.cpp-router
      - name: hadolint (go-server)
        uses: hadolint/hadolint-action@v3.1.0
        with:
          dockerfile: go-api/cmd/server/Dockerfile
      - name: hadolint (go-worker)
        uses: hadolint/hadolint-action@v3.1.0
        with:
          dockerfile: go-api/cmd/worker/Dockerfile

  terraform:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: hashicorp/setup-terraform@v3
        with:
          terraform_version: "1.9.8"
      - name: terraform fmt check
        working-directory: infra/terraform
        run: terraform fmt -recursive -check -diff
      - name: terraform init (no backend)
        working-directory: infra/terraform/environments/dev
        run: terraform init -backend=false
      - name: terraform validate
        working-directory: infra/terraform/environments/dev
        run: terraform validate

  security-scan:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
      - name: gitleaks
        uses: gitleaks/gitleaks-action@v2
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

- [ ] **Step 2: Validate the workflow YAML syntax**

```bash
python3 -c "import yaml, sys; yaml.safe_load(open('.github/workflows/ci.yml'))" && echo "YAML valid"
```

(A GitHub-Actions-specific schema validator would be better if available in this environment — e.g. `actionlint` — use it if installable; otherwise plain YAML parsing at least catches syntax errors.)

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "feat(ci): add validate-only GitHub Actions workflow for Go, C++, Docker, and Terraform"
```

---

### Task 11: GitHub Actions — manual deploy workflow

**Files:**
- Create: `.github/workflows/deploy.yml`

**Interfaces:** none.

- [ ] **Step 1: Write `.github/workflows/deploy.yml`**

```yaml
name: Deploy

on:
  workflow_dispatch:
    inputs:
      environment:
        description: "Target environment (must match a directory under infra/terraform/environments/)"
        required: true
        default: dev
      confirm_apply:
        description: "Type exactly 'yes' to actually apply -- any other value runs plan only"
        required: true
        default: "no"

jobs:
  plan-and-maybe-apply:
    runs-on: ubuntu-latest
    environment: production
    permissions:
      id-token: write
      contents: read
    steps:
      - uses: actions/checkout@v4
      - uses: hashicorp/setup-terraform@v3
        with:
          terraform_version: "1.9.8"

      # OIDC federation -- no long-lived AWS credentials stored as
      # repository secrets. Requires an AWS IAM role trust policy for
      # this repo's OIDC provider to be created out-of-band (not by this
      # workflow) before this step can succeed against a real account.
      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.AWS_DEPLOY_ROLE_ARN }}
          aws-region: us-east-1

      - name: terraform init
        working-directory: infra/terraform/environments/${{ inputs.environment }}
        run: terraform init

      - name: terraform plan
        working-directory: infra/terraform/environments/${{ inputs.environment }}
        run: terraform plan -out=tfplan

      - name: terraform apply (gated)
        if: inputs.confirm_apply == 'yes'
        working-directory: infra/terraform/environments/${{ inputs.environment }}
        run: terraform apply -auto-approve tfplan
```

`environment: production` requires the repository to have a GitHub Environment named `production` configured with required reviewers for this job to actually run when invoked — this is the "protected workflow" gate the design calls for, on top of `workflow_dispatch` already meaning it never fires on a normal push/PR. `secrets.AWS_DEPLOY_ROLE_ARN` and the OIDC trust relationship do not exist in this environment (no AWS account is connected to this repo) — this workflow is correct, complete, and inert here; it becomes exercisable only once a real deployment target and its IAM trust policy are configured out-of-band, which is out of scope for this plan to perform.

- [ ] **Step 2: Validate YAML syntax** (same method as Task 10 Step 2).

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/deploy.yml
git commit -m "feat(ci): add manual, environment-gated deploy workflow"
```

---

### Task 12: Documentation and `.env.example` update

**Files:**
- Modify: `README.md`
- Modify: `go-api/.env.example`

**Interfaces:** none (final task).

- [ ] **Step 1: Update `go-api/.env.example`**

Read the current file (14 lines, stale per the design survey) and the current README's full environment-variable tables (post-Phase-10). Rewrite `.env.example` to list every variable from those tables — core, server, worker, and the new Phase 11 Compose-only vars (`POSTGRES_PASSWORD`) — with the same placeholder/empty-value convention the file already uses, organized under comment headers matching README's own groupings (Core / Server / Worker / Testnet-only). This closes the staleness gap Phase 10's own survey flagged and this phase's survey re-confirmed.

- [ ] **Step 2: Update `README.md`**

Add, in this order (matching the existing document's section flow — read the current file in full first to find the right insertion points relative to Phase 10's existing sections):

1. **Architecture** section: extend the existing prose/diagram to mention Docker/Terraform as the deployment layer (one or two sentences, not a rewrite).
2. **Docker** section: how to build each image individually, `docker compose up -d` for the full local stack, the testnet override (`docker compose -f docker-compose.yml -f docker-compose.testnet.yml up -d` with a `.env` file), and an explicit note on the `go-server`/`go-worker` health-check gap (distroless has no shell/HTTP client for an in-container `HEALTHCHECK`; external checks — `docker compose ps`, `curl http://localhost:8080/metrics` — are the practical substitute) versus the C++ router's working `HEALTHCHECK` (different, shell-capable base image).
3. **Terraform** section: module tree diagram (prose, mirroring `infra/terraform/README.md`'s content at a higher level, not duplicating it verbatim), `fmt`/`validate`/`plan` commands, the explicit "never `apply` without real credentials and authorization" note, the Redpanda-not-MSK tradeoff in one paragraph, remote-state guidance pointer.
4. **Networking/data-flow diagram**: an ASCII diagram (matching Phase 10's existing diagram style in the same document) showing: `Internet → ALB (public) → go-server (private) → {Postgres, cpp-router, Kafka/Redpanda} (all private) → observability stack (private, non-blocking)`.
5. **CI/CD** section: what `ci.yml` validates on every PR, what `deploy.yml` requires to actually deploy (manual dispatch + environment approval + real AWS OIDC role — none of which exist in this repo yet), and the explicit "CI never auto-deploys" statement.
6. **Benchmark** section update: extend Phase 10's existing benchmark section with a "Phase 11: benchmarking against the full Docker stack" subsection reporting whatever Task-13 (below, the controller's post-implementation step) actually measured — or, if Docker was unavailable when this phase was implemented, stating that plainly rather than inventing a number, exactly as Phase 10's own benchmark section already models.

- [ ] **Step 3: Commit**

```bash
git add README.md go-api/.env.example
git commit -m "docs: document Docker, Terraform, CI/CD, and update .env.example for Phase 11"
```

---

## Post-implementation (controller responsibility, not a numbered task)

After Task 12, run the full regression suite (Go unit + integration + Kafka-tagged tests against a real Redpanda if Docker is available, C++ `ctest` for both `router/` and `cpp-routing-service/`, `terraform fmt`/`validate`, the existing Phase 1-10 E2E script). Then, if a Docker daemon is genuinely reachable: `docker compose up -d`, wait for health, run `go run ./go-api/cmd/benchmark` against the full containerized stack for a fixed duration/concurrency, and report the REAL measured `throughput_processed_per_sec`/p50/p95/p99 with exact hardware/environment context. If Docker is unavailable (as in every prior phase of this project to date), state that explicitly and do not claim a throughput number — report exactly what WAS statically validated (Dockerfile review, `docker compose config`, Terraform `fmt`/`validate`) versus what could not be run, matching Phase 10's own established honesty discipline. Then dispatch the final whole-branch review (most capable model available) covering, in addition to the standard checklist: no secret in any Dockerfile/Compose file/Terraform file/CI log; only the ALB has public ingress in the Terraform design; observability services never block app-service startup in either Compose file or the ECS wiring; the C++ container's exec-form `ENTRYPOINT` and signal-handling assumptions are preserved; `router/src/route.cpp` has zero diff; no `terraform apply` was ever run; Docker/Terraform claims in the final report are honestly qualified by what was actually executable in this environment. Produce the final report in the format the user's original request specifies (what was run vs. only statically validated, benchmark results or their explicit absence, architecture summary, remaining limitations). Do not merge or push — stop after implementation, testing, benchmark execution where possible, and final review, per explicit user instruction.
