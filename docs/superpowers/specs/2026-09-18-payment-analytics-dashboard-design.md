# Phase 12: React/TypeScript Payment Analytics Dashboard — Design

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** A product-facing React+TypeScript dashboard (separate from Grafana) that makes a payment's journey through ChainRoute visually understandable — creation, quoting, routing, persistence, Kafka processing, execution, and terminal state — backed only by data the system actually persists, with a small, justified backend extension so real (not fabricated) Across-vs-Relay quote comparison is possible.

**Architecture:** New read-only dashboard endpoints on the existing Go server (`GET /payments` paginated+filtered list, `GET /payments/{id}/quotes`, `GET /dashboard/stats`, `GET /dashboard/timeseries`), backed by a new migration that stores every fetched quote per payment (not just the winner) with a `selected` flag — the execution path continues reading only the selected quote, unchanged. A new `frontend/` Vite+React+TypeScript app (Tailwind, TanStack Query, Recharts, React Router) polls these endpoints. CORS middleware on the Go server lets the browser call the API cross-origin in both local Docker Compose and AWS (S3+CloudFront for static hosting, avoiding a container just to serve files). No routing, execution, or recovery logic changes anywhere.

**Tech Stack:** Vite + React 18 + TypeScript; Tailwind CSS (restrained custom palette, not a component-library look); TanStack Query (polling-based real-time, not SSE/WebSockets — justified in §8); Recharts (lightweight, composable, React-native charting); React Router; Vitest + React Testing Library; a hand-rolled SVG route-visualization component (not a graph library — the graph is 5 nodes, at most 2 hops, a library would be disproportionate).

## Global Constraints

- Do not modify `router/src/route.cpp`/`router/include/chainroute/route.hpp` (Dijkstra), the C++ router's request/response shape, or any Go payment/execution/reconciliation/recovery/provider-selection logic. This phase adds read paths and one additive persistence change; it changes nothing about how a payment is routed, executed, or recovered.
- The dashboard is strictly read-only against payment state. No dashboard endpoint may write to `payments`, `payment_executions`, `payment_route_hops`, or `outbox_events`, or influence which provider wins a route. The one write this phase adds (persisting every fetched quote, not just the winner) happens inside the EXISTING `CreateOrGetPayment` transaction, at the exact point that transaction already runs — it is not a new decision point, and the C++ router's selection is computed and returned to the handler before this write happens, unchanged.
- `GetQuoteByPaymentID` (singular — the method `Executor.ExecuteTestnetPayment`/`DriveExecutionForward` depend on to know which quote to re-validate and execute against) must continue to return exactly the one selected/winning quote after this migration, with identical semantics to today. This is verified explicitly by task-level tests, not assumed.
- Never fabricate quote comparison data. Where a payment predates this phase's migration (no `selected` column data, or was simulated-mode and never had quotes at all), the dashboard shows an honest empty/unavailable state, never invented numbers.
- Never expose private keys, raw signed transaction bytes, RPC URLs, or provider API keys anywhere in an HTTP response, a log, or the frontend bundle.
- No unbounded/paginate-less list endpoint. `GET /payments` always paginates (default + max page size enforced server-side).
- No N+1 query patterns for the list/aggregate endpoints — a single indexed query (or a small fixed number of queries) per request, using SQL aggregation (`COUNT`, `GROUP BY`, `date_trunc`) rather than fetching all rows and aggregating in Go.
- CORS is permissive enough for local dev and the deployed frontend's actual origin, never a blanket reflect-any-origin credential-bearing configuration (no cookies/credentials are used by this API, so this is a lower-stakes surface than a session-authenticated app, but the allowed-origin list is still explicit and configurable, not `*` combined with credentials).
- Docker/Terraform additions never expose Postgres, Redpanda, the C++ gRPC router, or Prometheus/OTel/Jaeger/Grafana directly to the browser. Only the Go server's HTTP API and the frontend's static assets are reachable from outside their respective private networks.
- Terraform is written and `fmt`/`validate`/statically-`plan`-checked (same dummy-credential method established in Phase 11) but **never applied** — no AWS credentials, no authorization in this environment.
- Demo data is created only through the existing `POST /payments` API in simulated mode (network-free, using the app's own real code path) — never a direct SQL `INSERT` bypassing application logic, and never hard-coded into the frontend.

---

## 1. Current state (confirmed against the live repo, not assumed)

- **Exactly 4 HTTP endpoints exist**: `POST /routes`, `POST /payments`, `GET /payments/{id}`, `GET /metrics`. No list endpoint, no pagination, no aggregation, no CORS middleware anywhere.
- **`payment_quotes` has `UNIQUE(payment_id)`** — schema-enforced, at most one quote row per payment. Confirmed via three independent sources (the constraint itself, the Go struct's doc comment, and `PostPayments`'s actual control flow): during testnet-mode `POST /payments`, quotes are fetched from **every** registered provider concurrently and held in an in-memory map, but only the C++ router's winning hop's quote is ever passed to `CreateOrGetPayment` and persisted — every losing quote is discarded when the request completes. **There is no real losing-quote data anywhere in the database today.**
- **No lifecycle history table.** `payments.created_at`/`updated_at`/`completed_at` and `payment_executions.broadcast_at`/`confirmed_at` are the only timestamps that exist — no per-transition audit log. The dashboard's lifecycle timeline is built from these snapshot timestamps plus the current `status`/`external_status` values, not a full event history.
- **Provider names**: `"across"`, `"relay"` (exact strings, already bounded in Prometheus via `SanitizeProviderLabel`).
- **Simulated routing graph (5 chains, `POST /routes` and simulated-mode `POST /payments`)**: Ethereum, Base, Arbitrum, Optimism, Polygon (`proto/chainroute/v1/routing.proto`'s `Chain` enum). **Real testnet execution (2 chains, 1 asset)**: only Ethereum Sepolia ↔ Base Sepolia, WETH only (`handler/routes.go`'s `testnetChainIDByChain` map has no entries for Arbitrum/Optimism/Polygon; the quote registry has no route key for USDC). This gap is exactly what §5's route visualization must represent honestly.
- **Phase 10's Grafana dashboard** has 10 panels, all Prometheus-metric-derived rate/latency/count time series (throughput, latency percentiles, Kafka/worker processing, execution outcomes). None of them show an individual payment's record, a filterable payment list, or a genuine per-payment quote comparison — Prometheus's label-cardinality bounds structurally prevent per-payment-ID metrics. This phase's dashboard is payment-record-level; Grafana stays metric-level. No overlap risk as designed.
- **Docker Compose** (Phase 11): `go-server` is the only app-tier service published to the host (`8080:8080`); Postgres/Redpanda/cpp-router are internal-only on the `chainroute-net` bridge network. **Terraform**: the ALB is the sole public-ingress point, forwarding only to `go-server`'s target group on 8080; no frontend infrastructure exists anywhere in the module tree.
- **No frontend/Node tooling exists anywhere in the repo** — confirmed clean slate.

## 2. Persistence extension (§6 — justified, scoped, additive)

### Migration `0007_dashboard_payment_analytics.sql`

```sql
-- Drop the single-quote-per-payment constraint; a payment can now have
-- one quote row per provider that was fetched (winning and losing),
-- never more than one row per (payment, provider) pair.
ALTER TABLE payment_quotes DROP CONSTRAINT payment_quotes_payment_id_key;
ALTER TABLE payment_quotes ADD COLUMN selected BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE payment_quotes ADD CONSTRAINT payment_quotes_payment_id_provider_key UNIQUE (payment_id, provider);

-- Exactly one selected=true row per payment (never zero once any quote
-- exists, never more than one) -- enforced by the database, not just
-- application code, so a future bug can't silently select two winners
-- or none. A partial unique index is the standard Postgres idiom for
-- "at most one true per group."
CREATE UNIQUE INDEX payment_quotes_one_selected_per_payment
    ON payment_quotes (payment_id) WHERE selected;

-- Indexes for the new dashboard read paths (list/filter/aggregate) --
-- avoids sequential scans on payments as data grows.
CREATE INDEX payments_created_at_idx ON payments (created_at DESC);
CREATE INDEX payments_status_idx ON payments (status);
CREATE INDEX payments_execution_mode_idx ON payments (execution_mode);
CREATE INDEX payments_bridge_provider_idx ON payments (bridge_provider) WHERE bridge_provider IS NOT NULL;
CREATE INDEX payments_source_dest_idx ON payments (source_chain, destination_chain);
```

### Go/application changes required (all additive, no behavior change to the execution path)

- `payment.Quote` gains a `Selected bool` field.
- `postgres.insertPaymentQuote` (currently inserts one `*payment.Quote`) becomes `insertPaymentQuotes` accepting `[]payment.Quote` (each with its own `Selected` value), inserted in the same existing transaction `CreateOrGetPayment` already runs — no new transaction, no new atomicity boundary, no change to what `CreateOrGetPayment`'s caller observes.
- `handler.PostPayments`'s testnet-mode branch changes from building one `candidate.Quote *payment.Quote` to building `candidate.Quotes []payment.Quote` — looping over every entry already collected in `quotesByBridgeName` (every successful+available provider response, which the handler already computes today), marking exactly the winning provider's entry `Selected: true`. **The C++ router's winning-hop determination itself is completely untouched** — this only changes what happens to data the handler already has in memory after that decision was made.
- `postgres.Store.GetQuoteByPaymentID` (singular — depended on by `Executor`) is updated to `SELECT ... WHERE payment_id = $1 AND selected = true` — returns exactly the one row it always returned, by construction of the partial unique index above. A task-level test proves this explicitly: seed a payment with two quote rows (one selected, one not) and confirm `GetQuoteByPaymentID` returns only the selected one, with identical fields to what it would have returned pre-migration.
- New `postgres.Store.GetQuotesByPaymentID` (plural) — returns every quote row for a payment, ordered so the selected one is first (`ORDER BY selected DESC, provider`), consumed only by the new dashboard endpoint, never by the execution path.
- **What does NOT change**: `TryCreateExecution`'s nonce allocation, `buildValidatedEnvelope`'s envelope validation, the C++ router's request/response, provider-selection determinism (still entirely the C++ router's Dijkstra result), the existing `UNIQUE(wallet_address, nonce)` guarantee, idempotency-key handling, the outbox/Kafka pipeline.

## 3. Backend dashboard API (§9)

Four new endpoints, one new narrow `DashboardStore` interface (following the existing per-handler narrow-interface convention — not widening `PaymentStore`), registered alongside the existing four on the same `mux`:

### `GET /payments` — paginated, filtered list
Query params: `limit` (default 25, max 100, clamped not rejected), `cursor` (opaque, base64 of `created_at,id` for keyset pagination — not offset pagination, which degrades under filtering + concurrent inserts), `status`, `provider`, `source_chain`, `destination_chain`, `execution_mode` (each optional, validated against the same enums the write path already validates against — an invalid filter value is a 400, not a silently-empty result).

```go
type paymentListItem struct {
	ID               string  `json:"id"`
	SourceChain      string  `json:"source_chain"`
	DestinationChain string  `json:"destination_chain"`
	Asset            string  `json:"asset"`
	Amount           string  `json:"amount"`
	Status           string  `json:"status"`
	ExecutionMode    string  `json:"execution_mode"`
	BridgeProvider   *string `json:"bridge_provider"`
	TotalFee         float64 `json:"total_fee"`
	CreatedAt        string  `json:"created_at"`
}
type paymentListResponse struct {
	Payments   []paymentListItem `json:"payments"`
	NextCursor *string           `json:"next_cursor"` // nil when no more pages
}
```
One indexed query using keyset pagination (`WHERE created_at < $cursor_created_at OR (created_at = $cursor_created_at AND id < $cursor_id) ORDER BY created_at DESC, id DESC LIMIT $limit+1`) plus the optional filter predicates as additional `AND` clauses against the new indexes above — a single round trip, no N+1.

### `GET /payments/{id}/quotes` — all quotes for one payment
```go
type quoteItem struct {
	Provider             string `json:"provider"`
	InputAmount          string `json:"input_amount"`  // base-units integer string, same convention as existing fields
	OutputAmount         string `json:"output_amount"`
	FeeAmount            string `json:"fee_amount"`
	EstimatedFillTimeSec int64  `json:"estimated_fill_time_sec"`
	Selected             bool   `json:"selected"`
	QuotedAt             string `json:"quoted_at"`
}
type paymentQuotesResponse struct {
	Quotes []quoteItem `json:"quotes"` // empty array (not null) for simulated-mode payments or pre-migration payments with no quote data
}
```
404 if the payment itself doesn't exist; empty `quotes: []` (200) if the payment exists but has no quotes (simulated mode, or predates this migration) — the frontend must render this as an honest "no comparison data available" state, never fabricate.

### `GET /dashboard/stats` — aggregate counters
```go
type dashboardStats struct {
	TotalPayments      int64            `json:"total_payments"`
	CompletedPayments  int64            `json:"completed_payments"`
	ProcessingPayments int64            `json:"processing_payments"` // ROUTED+PROCESSING+SUBMITTED, i.e. non-terminal
	FailedPayments     int64            `json:"failed_payments"`
	ProviderUsage      map[string]int64 `json:"provider_usage"`      // {"across": N, "relay": M} -- only providers that have ever won a route
	AverageRoutingCost float64          `json:"average_routing_cost"`
}
```
One `SELECT COUNT(*) FILTER (WHERE status = 'COMPLETED'), COUNT(*) FILTER (WHERE status IN ('ROUTED','PROCESSING','SUBMITTED')), COUNT(*) FILTER (WHERE status = 'FAILED'), COUNT(*), AVG(total_fee) FROM payments` (one query, no per-row Go-side aggregation) plus one `SELECT bridge_provider, COUNT(*) FROM payments WHERE bridge_provider IS NOT NULL GROUP BY bridge_provider` (one query, using the new `payments_bridge_provider_idx`).

### `GET /dashboard/timeseries` — bucketed time series for charts
Query param `metric` (`volume` | `routing_cost`), `interval` (`hour` | `day`, default `day`), `days` (lookback window, default 30, max 90 — bounded to prevent an unbounded scan).
```go
type timeseriesPoint struct {
	Bucket string  `json:"bucket"` // RFC3339 bucket start
	Value  float64 `json:"value"`
	Count  int64   `json:"count"`
}
type timeseriesResponse struct {
	Metric string             `json:"metric"`
	Points []timeseriesPoint  `json:"points"`
}
```
One `SELECT date_trunc($interval, created_at) AS bucket, COUNT(*), <SUM or AVG depending on metric> FROM payments WHERE created_at > now() - ($days || ' days')::interval GROUP BY bucket ORDER BY bucket` — a single aggregating query using the existing `payments_created_at_idx`, never a per-row fetch-then-aggregate-in-Go pattern.

### CORS

A small, explicit middleware (not a general-purpose library — the need is narrow: allow `GET`/`POST` from a configured origin list, no credentials/cookies involved anywhere in this API). `CHAINROUTE_CORS_ALLOWED_ORIGINS` env var (comma-separated, e.g. `http://localhost:5173,https://dashboard.example.com`), defaulting to `http://localhost:5173` (the Vite dev server's default port) for a zero-config local dev experience — this default is explicitly documented as dev-only; a real deployment sets the env var to the actual CloudFront domain.

## 4. Frontend architecture

### Structure
```
frontend/
├── src/
│   ├── api/           # typed fetch client, one function per endpoint, types matching the Go JSON shapes exactly
│   ├── components/     # StatusBadge, RouteVisualization, LifecycleTimeline, ProviderComparison,
│   │                    # PaymentTable, FilterBar, Pagination, Loading/Empty/Error states, charts/
│   ├── pages/           # OverviewPage, PaymentExplorerPage, PaymentDetailPage
│   ├── hooks/            # useApiPolling wrapper around TanStack Query with the phase's chosen interval
│   ├── lib/               # chain/provider display-name maps, explorer-URL builder, formatting helpers
│   └── App.tsx, main.tsx, router.tsx
├── public/env-config.js  # runtime-substituted at container start (Docker) -- see §10
├── index.html, vite.config.ts, tailwind.config.js, tsconfig.json, package.json
```

### Real-time strategy (§8) — polling, not SSE/WebSockets, with reasoning stated explicitly

**Decision: TanStack Query with a fixed `refetchInterval` (5s on the payment detail page while non-terminal, 10s on the list/overview pages).** Reasoning, to be stated in the README as the design doc requires: the backend has no existing pub/sub or event-stream primitive a browser could safely consume — Kafka carries internal worker-processing events, not something designed for external/browser consumption, and building a new SSE endpoint would mean the Go server itself polling Postgres for changes and re-broadcasting to connected browsers, which is strictly more moving parts than letting the browser poll the already-built list/detail endpoints directly, for a data-change frequency (a payment moves through at most 5-6 states over seconds-to-low-tens-of-seconds) that doesn't need sub-second latency. Polling is the simplest mechanism that is actually justified by the system's real event frequency and existing infrastructure — WebSockets would add a persistent-connection lifecycle, reconnection handling, and a new server-side fan-out mechanism for a UX benefit (shaving a few seconds off state-change visibility) the phase doesn't call for. TanStack Query's `refetchInterval` also naturally pauses when the browser tab isn't focused (`refetchIntervalInBackground: false`), avoiding wasted requests.

### Route visualization (§5)

A hand-rolled SVG component, not a graph library (5 fixed nodes, at most 2 hops — react-flow/vis-network would be materially heavier than the problem warrants). Nodes for all 5 simulated-graph chains are always rendered in a fixed layout; the payment's actual hop path is highlighted (source/destination/intermediate nodes in a distinct color, the traversed edge(s) drawn solid); every other node/edge is rendered muted/dashed to represent "in the simulated graph but not part of this payment's route." A persistent, explicit legend distinguishes two independent facts so neither is ever implied by the other: (a) which chains are in the **simulated routing graph** (all 5, always shown) vs. (b) whether **this specific payment** had real signed testnet execution behind it — determined from `execution_mode === "testnet"` AND the source/destination pair being the one real corridor (Ethereum Sepolia ↔ Base Sepolia) — shown as a small badge ("Simulated route" vs. "Live testnet execution"), never inferred from the chain names alone (a testnet-mode payment on an unsupported corridor cannot exist per the backend's own validation, but the component still derives the badge from actual response fields, not chain-name pattern-matching, so it stays correct if the backend's supported-corridor set ever changes).

### Provider comparison (§6, frontend side)

Reads `GET /payments/{id}/quotes`. If the array is empty, renders an explicit "No comparison data available for this payment" empty state (simulated-mode payments, or payments created before this phase's migration) — never a fabricated Across/Relay comparison. If populated, renders both providers' quoted output/fee/estimated-fill-time side by side with the `selected: true` entry visually highlighted and labeled "Selected by ChainRoute," making the actual causal claim the spec asks for ("ChainRoute received these options and selected this route") without overclaiming when only one provider responded (a single-entry array still renders, labeled as the only quote received, not a "comparison").

## 5. Docker integration (§10)

`frontend/Dockerfile`: multi-stage — `node:20-bookworm` builder (`npm ci && npm run build`), `nginx:1.27-alpine` runtime serving `dist/`. Since Vite bakes env vars in at build time but the same image must be deployable with a different API URL without rebuilding, the runtime stage's entrypoint script generates `dist/env-config.js` from the container's `API_BASE_URL` env var at container start (a well-established static-SPA pattern: `window.__CHAINROUTE_API_BASE_URL__ = "<value>"`, loaded by `index.html` before the main bundle, read by the API client with a build-time `import.meta.env.VITE_API_BASE_URL` fallback for `npm run dev`). `docker-compose.yml` gains a `frontend` service (`ports: ["5173:80"]`, `API_BASE_URL=http://localhost:8080`, `restart: unless-stopped`, no `depends_on` requiring the backend to be healthy first — the frontend's own loading/error states handle a not-yet-ready API, matching the "observability never gates startup" spirit applied to the frontend too). `go-server`'s CORS default (`http://localhost:5173`) matches this port exactly, zero extra config needed for `docker compose up`.

## 6. Terraform integration (§11)

New `infra/terraform/modules/frontend`: private S3 bucket (no public bucket policy — access only via CloudFront Origin Access Control) + CloudFront distribution (default cache behavior → S3 origin, custom error responses mapping 403/404 → `/index.html` 200 for SPA client-side routing) + optional ACM cert var (CloudFront requires `us-east-1` certs specifically — documented as a constraint in the module, not silently assumed). No compute (no ECS service, no container) for the frontend — static assets don't need a running task, which is the more cost-conscious choice than a 4th Fargate service. Wired into `environments/dev/main.tf` behind a new `enable_frontend` toggle (default `true`, mirroring `enable_observability_stack`'s pattern) — the cost-conscious escape hatch this phase's own constraints call for. The Go server's `CHAINROUTE_CORS_ALLOWED_ORIGINS` gets the CloudFront distribution's domain added via a Terraform-computed value passed as an ECS task environment variable, keeping the deployed frontend's actual origin authoritative rather than hardcoded.

## 7. Demo data (§13)

A new `scripts/seed_demo_data.sh` (or a small Go program under `go-api/cmd/seed`) that issues real `POST /payments` requests (simulated mode only, distinct idempotency keys, a spread of chain pairs/assets/amounts) against a running server, using the exact same code path a real user would — never a direct SQL insert. Explicitly documented as a local-development-only convenience, never run in CI or against a deployed environment by default.

## 8. Testing (§12)

Backend: task-level Go unit + integration tests for every new store method and handler (following `fakePaymentStore`'s existing narrow-fake-per-interface convention), plus a dedicated test proving `GetQuoteByPaymentID`'s post-migration behavior is unchanged for the execution path. Frontend: `tsc --noEmit` (type check), ESLint, Vitest + React Testing Library component tests for the components with real logic (route visualization's highlight logic, lifecycle timeline's stage-derivation logic, provider comparison's empty-vs-populated rendering), a production `vite build`. Full existing Go/C++ regression suites re-run to confirm zero behavior change outside the new read paths.

## Non-goals (explicit, matching the phase's own constraints)

No WebSockets/SSE. No new distributed-system component (no message queue, no cache layer, no separate analytics database) — Postgres aggregation queries are sufficient at this data scale. No authentication/authorization system (out of scope, not requested — the dashboard is read-only and the API has none today; not introduced here either). No changes to Dijkstra, execution, recovery, or provider-selection logic anywhere. No fabricated quote/analytics data for payments that don't have it.
