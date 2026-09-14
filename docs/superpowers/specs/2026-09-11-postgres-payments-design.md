# ChainRoute Phase 5: Durable Payment Creation with PostgreSQL

**Status:** Approved (revised)
**Date:** 2026-09-11

## Context

Phases 1-4 (merged, pushed to GitHub) built: a C++ routing engine (`router/`
— directed multigraph, fee-only Dijkstra, deterministic `NetworkSimulator`),
a shared protobuf contract (`proto/chainroute/v1/routing.proto`), a C++
gRPC routing service (`cpp-routing-service/`) wrapping the engine, and a
thin Go HTTP API (`go-api/`) exposing `POST /routes` — validates a JSON
request, calls the C++ service over gRPC, translates the response back to
JSON. Nothing is persisted anywhere; every request is stateless.

**Phase 5 scope is durable payment creation and state management**: a new
`POST /payments` endpoint that validates a request, requests a route from
the existing C++ service (unchanged), persists the payment and its exact
selected route in PostgreSQL, and returns the durable representation; and
`GET /payments/{id}` to read it back. PostgreSQL is the only new
infrastructure dependency. No payment execution, no asynchronous
processing, no blockchain transactions.

**This revision** adds a required `Idempotency-Key` on `POST /payments`,
enforced atomically at the PostgreSQL layer, and tightens amount
validation to match `NUMERIC(38,18)` exactly rather than accepting any
syntactically-valid decimal.

## Goals

- `POST /payments` creates a durably-persisted payment record with its
  selected route, only after routing succeeds.
- `GET /payments/{id}` reads the persisted payment back from PostgreSQL.
- The authoritative payment amount is never represented as a float64
  anywhere in the pipeline, and Go-side validation rejects any value the
  database could not store before any network call is made.
- A payment can never exist in a partially-persisted state (payment row
  without its complete hop list, or vice versa).
- A retry of `POST /payments` with the same `Idempotency-Key` and a
  logically identical request never creates a duplicate payment — this
  holds under concurrent retries, enforced by PostgreSQL, not application
  logic.
- Existing `POST /routes` behavior is unchanged and remains public.
- Phase 1-3 router code (`router/`) is unchanged.

## Non-goals

- Payment execution or any blockchain transaction.
- Asynchronous processing, background workers, Kafka, Redis.
- Multiple externally-visible payment states beyond `ROUTED` (see
  "State model" below for why).
- A generalized idempotency framework reusable by future endpoints — only
  what `POST /payments` concretely needs.
- Retries of any kind beyond the idempotency-key mechanism itself, unless
  concretely required for Phase 5 correctness (none else were found to
  be).
- A heavyweight ORM, a migration framework, Kubernetes, or cloud
  deployment tooling.
- Authentication, authorization, listing/pagination, update, or delete
  endpoints for payments.

## File structure

```
go-api/
  cmd/server/main.go              # extended: DB pool, startup ping, payments wiring
  internal/
    gen/...                       # unchanged (Phase 4)
    grpcclient/...                # unchanged (Phase 4)
    handler/
      routes.go, routes_test.go   # unchanged (Phase 4, POST /routes)
      payments.go                 # new: POST /payments, GET /payments/{id}
      payments_test.go
    payment/
      payment.go                  # domain model (Payment, Hop, Status) — pure Go
    postgres/
      store.go                    # Postgres-backed repository
      store_integration_test.go   # real-DB tests, build-tagged `integration`
  migrations/
    0001_create_payments.sql
  .env.example                    # committed template, no real credentials
```

`internal/handler/payments.go` defines a small local `PaymentStore`
interface (`CreateOrGetPayment`, `GetPayment`), satisfied by
`*postgres.Store` — this mirrors the existing `RoutingClient` interface
pattern from Phase 4 exactly, and is the only new interface this phase
introduces, justified by the same testability need (injecting a fake in
unit tests) as its predecessor.

## Payment domain model

```go
package payment

type Status string
const StatusRouted Status = "ROUTED"

type Payment struct {
    ID                string
    IdempotencyKey    string
    SourceChain       string
    DestinationChain  string
    Asset             string
    Amount            string    // exact decimal string — never float64
    Status            Status
    TotalFee          float64   // simulated metric, not authoritative money
    Hops              []Hop
    CreatedAt         time.Time
    UpdatedAt         time.Time
}

type Hop struct {
    HopIndex    int
    FromChain   string
    ToChain     string
    BridgeName  string
    Fee         float64
    LatencyMs   float64
    Liquidity   float64
    Reliability float64
}
```

## State model: exactly one status, and why

Routing happens before any database row exists — the flow is validate →
route → persist → respond. There is no moment where a payment record
exists without a complete route: either the whole pipeline succeeds and a
fully-routed row is committed, or it fails at some earlier stage and no
row exists at all. A client can never observe an intermediate state via
`GET /payments/{id}`, because nothing is persisted until routing has
already succeeded.

Given that, states like `PENDING`, `CREATED`, or `FAILED` would model
states that can never actually be observed: `PENDING` never exists as a
queryable row (nothing is written before routing succeeds), and `FAILED`
payments are never persisted either (no route means no row, since Phase 5
has no execution to fail during). So Phase 5 needs exactly **one** status:
`ROUTED`. This is not a decision to skip state modeling — it reflects that
the current synchronous, execution-free flow genuinely only produces one
observable outcome. A future phase adding asynchronous execution
(`ROUTED → EXECUTING → COMPLETED/FAILED`) will need more states *then*,
when there is an actual transition to model. Adding them now would model
states that can never be observed, which is speculative complexity, not
realism.

The `status` column still exists as real schema (`TEXT NOT NULL DEFAULT
'ROUTED' CHECK (status IN ('ROUTED'))`), not a Postgres `ENUM` type —
`ALTER TABLE ... DROP/ADD CONSTRAINT` is simpler to extend later than
`ALTER TYPE ... ADD VALUE`, and extending the legal value set is exactly
what a future phase will need to do.

## POST /payments: semantics

`POST /payments` requires a client-supplied `Idempotency-Key` HTTP header
(any non-empty opaque string up to 255 characters — see "Idempotency" for
the full design). It is successful if and only if: the header is present,
request validation passes, a route is found (`route_found = true` from
the C++ service), and the payment plus all its hops are committed to
PostgreSQL. Only then is the HTTP response sent. Depending on whether this
specific request caused a new row to be created or surfaced an existing
one, the response is **201 Created** or **200 OK** — see "Idempotency."

If routing succeeds as an RPC but finds no route, no payment is created —
a route-less "payment" is not a meaningful resource to persist. This is
**422 Unprocessable Entity**, deliberately distinct from `/routes`'s
`200 + route_found: false`: `/routes`'s entire job is "tell me whether a
route exists," so a negative answer is itself the successful result;
`/payments`'s job is "create a durable payment," so "no route" means the
requested creation cannot happen.

## Amount validation

The authoritative amount is stored as `NUMERIC(38, 18)`. Go-side
validation must reject, before any network call, exactly what that column
cannot hold — not merely "some decimal-looking string." Rule:

```
^\d{1,20}(\.\d{1,18})?$
```

- **1-20 integer digits** — `NUMERIC(38,18)` allows 38 total significant
  digits, 18 of which are reserved for the fractional part, leaving 20
  for the integer part.
- **An optional decimal point followed by 1-18 fractional digits** — never
  more than 18; never a bare trailing `.` with no digits after it; a
  leading digit is required (`"0.50"`, not `".50"`).
- **No sign of any kind** — not even a leading `+`. Combined with the
  next rule, this is what rejects negative values at the syntax level
  rather than needing a separate numeric-sign check.
- **No exponent notation** (`e`/`E`) and no other characters.
- **Not identically zero** — after the regex match confirms the string is
  a syntactically valid non-negative decimal, reject it if every digit
  (ignoring the decimal point) is `'0'` (e.g. `"0"`, `"0.00"`). This is a
  simple string check, not a numeric parse: it never needs to convert the
  value to any numeric type, float64 included, to determine positivity.

This validation runs immediately after the chain/asset checks, **before**
the gRPC call to the routing service — a value that cannot possibly be
persisted becomes a 400 immediately, rather than successfully consuming a
routing RPC and only then failing as a database error (or, worse, a
`NUMERIC` overflow error surfacing from Postgres, which is a real
possibility here: without this check, an oversized value would pass
Go-side "is this a decimal" validation, succeed at routing, and only then
fail inside the transaction — turning a client mistake into a 500).

## Failure behavior

| Case | Caught by | HTTP | Persisted? |
|---|---|---|---|
| Missing/empty/too-long `Idempotency-Key` header | Go, pre-routing | 400 | No |
| Invalid chain/asset format, or same chain | Go, pre-routing | 400 | No |
| Amount fails the `NUMERIC(38,18)` syntax/positivity rule | Go, pre-routing | 400 | No |
| No route exists | Go, after gRPC call | 422 | No |
| C++ routing service unavailable | gRPC `UNAVAILABLE` | 503 | No |
| Routing times out | gRPC `DEADLINE_EXCEEDED` | 504 | No |
| Routing succeeds, PostgreSQL unreachable/errors | Go, after gRPC call | 500 | No |
| Transaction fails or the process crashes mid-transaction | Postgres/Go | 500 or no response | **No** — Postgres's atomicity guarantees zero rows, never partial rows |
| Same `Idempotency-Key`, logically identical request | Postgres-backed comparison | **200** (not 201) | Row already existed — returned, not re-created |
| Same `Idempotency-Key`, logically different request | Postgres-backed comparison | **409** | No — the original row is untouched |

## PostgreSQL schema

```sql
CREATE TABLE payments (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key    TEXT NOT NULL,
    source_chain       TEXT NOT NULL,
    destination_chain  TEXT NOT NULL,
    asset              TEXT NOT NULL,
    amount             NUMERIC(38, 18) NOT NULL CHECK (amount > 0),
    status             TEXT NOT NULL DEFAULT 'ROUTED' CHECK (status IN ('ROUTED')),
    total_fee          DOUBLE PRECISION NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (source_chain <> destination_chain),
    UNIQUE (idempotency_key)
);

CREATE TABLE payment_route_hops (
    payment_id   UUID NOT NULL REFERENCES payments(id) ON DELETE CASCADE,
    hop_index    INTEGER NOT NULL,
    from_chain   TEXT NOT NULL,
    to_chain     TEXT NOT NULL,
    bridge_name  TEXT NOT NULL,
    fee          DOUBLE PRECISION NOT NULL,
    latency_ms   DOUBLE PRECISION NOT NULL,
    liquidity    DOUBLE PRECISION NOT NULL,
    reliability  DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (payment_id, hop_index)
);
```

**Primary keys:** `payments.id` (UUID); `payment_route_hops`'s composite
`(payment_id, hop_index)` — also serves as the index needed for "all hops
of payment X, in order," so no separate index on `payment_id` alone is
added.

**Uniqueness:** `UNIQUE (idempotency_key)` on `payments` — this is not
just a data-integrity nicety, it is the mechanism the idempotency design
below depends on for its correctness guarantee (see "Idempotency").

**Foreign key:** `payment_route_hops.payment_id → payments.id`, `ON DELETE
CASCADE` — no delete endpoint exists yet, but this is a defensive
integrity default.

**Constraints:** `amount > 0` and `source_chain <> destination_chain` are
enforced at the database layer too, as defense-in-depth behind Go's own
validation.

Requires PostgreSQL 13+ (`gen_random_uuid()` built into core, no
extension needed).

## Monetary representation

| Option | Verdict |
|---|---|
| PostgreSQL `NUMERIC`/`DECIMAL` | **Chosen** for the `amount` column — exact, arbitrary-precision, no rounding error |
| Integer minor units | Also exact, but requires a per-asset scale factor (USDC uses 6 decimals on-chain, not 2 like cents) that has no concrete need yet |
| Go decimal library (e.g. `shopspring/decimal`) | Unnecessary — a new dependency for a value Go never needs to do arithmetic on |

**Chosen:** `amount` is a plain Go `string`, end to end, always. The
conversion boundary is: JSON string (`"1000.00"`) → validated against the
`NUMERIC(38,18)`-matching rule above (never parsed into a numeric Go
type) → bound directly as a `string` query parameter to `NUMERIC` columns
(Postgres's wire protocol accepts the exact text representation) → read
back from Postgres as a `string` → written directly into the JSON
response. **Float64 never appears anywhere in this path**, including
during idempotency comparison (see below) — not by discipline, but
because no code path exists that could introduce it.

**Distinguishing authoritative amount from simulated metrics:** `amount`
is `NUMERIC`, string-typed in Go, exact. `total_fee` and each hop's
`fee`/`latency_ms`/`liquidity`/`reliability` originate as `float64` from
the C++ `NetworkSimulator` (unchanged since Phase 4) and are stored as
`DOUBLE PRECISION` — explicitly not `NUMERIC`, because converting them to
an exact type would falsely imply a precision they never had.

## SQL transaction boundary

One `sql.Tx` for the creation path, opened only after `route_found =
true`:

1. `INSERT INTO payments (...) VALUES (...) ON CONFLICT (idempotency_key) DO NOTHING RETURNING id, created_at, updated_at`
2. If a row came back (this request won): one multi-row `INSERT INTO
   payment_route_hops (...) VALUES (...), (...), ...` for all hops, then
   `COMMIT`.
3. If no row came back (this request's key already existed): `ROLLBACK`
   (nothing was written) and fall into the comparison path described
   under "Idempotency" — no hops are ever inserted for a losing request.

Any failure at any point — a constraint violation, a lost connection, the
process crashing before commit — leaves **zero** rows for that attempt,
via Postgres's own atomicity guarantees. This is what makes "a payment
must never exist with only part of its selected route" true by
construction. Go-side pattern: `BeginTx` → `defer tx.Rollback()` (a no-op
after a successful `Commit()`) → the two inserts above → `Commit()`.

## Idempotency

**The mechanism, and why it needs no application-level check-then-insert:**
correctness comes entirely from the `UNIQUE (idempotency_key)` constraint
combined with `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING
RETURNING ...`. When two requests race to insert the same key, Postgres
itself serializes them at the row level: the second transaction's insert
attempt waits on the first (whether the first commits or rolls back)
before evaluating the conflict, so it is architecturally impossible for
two rows to ever exist for the same key, no matter how tightly concurrent
the requests are. This is a well-established Postgres idiom for exactly
this problem, and it is the sole correctness mechanism — no Go-side
"SELECT then decide" sequence is ever relied on for correctness.

**The repository method:**

```go
type CreateResult int
const (
    Created  CreateResult = iota // brand-new row inserted -> 201
    Replayed                      // existing row, same logical request -> 200
    Conflict                      // existing row, different logical request -> 409
)

func (s *Store) CreateOrGetPayment(ctx context.Context, p payment.Payment) (payment.Payment, CreateResult, error)
```

**Algorithm:**

1. *(Optimization, not correctness-critical)* First, a plain read-only
   query checks whether `idempotency_key` already has a row, comparing
   its stored fields against the incoming request in the same query:
   ```sql
   SELECT id, source_chain, destination_chain, asset, amount::text,
          total_fee, status, created_at, updated_at,
          (source_chain = $2 AND destination_chain = $3
           AND asset = $4 AND amount = $5::NUMERIC) AS request_matches
   FROM payments
   WHERE idempotency_key = $1
   ```
   If a row is found: `request_matches = true` → fetch its hops and
   return `(existing, Replayed, nil)`, skipping the routing RPC entirely
   for the common retry case. `request_matches = false` → return
   `(Payment{}, Conflict, nil)`, also skipping routing. This step exists
   purely so that a retry — the expected common case for an idempotent
   endpoint — doesn't pay for a redundant routing RPC. If this check
   races with a concurrent insert and misses it, nothing breaks: the next
   step's `ON CONFLICT` still catches it.
2. If no existing row was found, the caller proceeds to call the routing
   service, then invokes the transactional insert from "SQL transaction
   boundary" above.
3. If that insert wins (`RETURNING` produced a row): insert hops, commit,
   return `(created, Created, nil)`.
4. If that insert loses (`RETURNING` produced no row — a concurrent
   request won first): roll back, then run the **same** comparison query
   as step 1 against the now-committed winning row, returning `Replayed`
   or `Conflict` exactly as in step 1.

Steps 1 and 4 share one internal helper, so the comparison logic exists
in exactly one place.

**The amount comparison is never done via Go string equality or
float64.** `amount = $5::NUMERIC` is evaluated by Postgres using
`NUMERIC`'s own exact equality — critical because Postgres normalizes a
stored `NUMERIC(38,18)` to its full 18 fractional digits on read (e.g.
`"1000.00"` round-trips as `"1000.000000000000000000"`), so a naive Go
string comparison between the original request and a re-read value would
incorrectly report a mismatch. Delegating the comparison to Postgres's
own type system sidesteps this entirely, with zero floating-point
involvement at any point.

**What counts as "logically identical":** exactly the four fields that
define the request's *input* — `source_chain`, `destination_chain`,
`asset`, `amount`. Not `total_fee` or `hops`: those are *outputs* of
routing, not part of what the client asked for, and comparing them would
conflate "did the client send the same request" with "did routing produce
the same answer" — two different questions.

**Why replay is 200, not 201:** `201 Created` asserts "this request just
created a resource." On a replay, this specific request created nothing
— an existing resource was found and returned. Returning `200` is honest
about what happened on *this* request, and matches the general REST
convention that `200` means "here is the resource" versus `201` meaning
"I just made this." Both responses carry the identical payment body and a
`Location: /payments/{id}` header; only the status code differs.

**Idempotency-Key header:** required; any non-empty string up to 255
characters (no format mandated — clients may use their own request IDs,
UUIDs, or anything else opaque). Missing, empty, or over-length is a 400,
checked immediately after JSON decoding, before any other validation.

## Payment ID generation

Postgres-generated `UUID` via `gen_random_uuid()`, retrieved via
`RETURNING id`. UUID rather than a sequential integer specifically
because the ID is exposed in a public URL — sequential IDs would let a
client enumerate other payments.

## Go repository/data-access layer

`github.com/jackc/pgx/v5`, used via its `pgx/v5/stdlib` compatibility shim
so the codebase works entirely through plain `database/sql`. Raw
parameterized SQL (`$1, $2, ...`), no query builder, no ORM.

```go
package postgres

type Store struct{ db *sql.DB }

func (s *Store) CreateOrGetPayment(ctx context.Context, p payment.Payment) (payment.Payment, CreateResult, error)
func (s *Store) GetPayment(ctx context.Context, id string) (payment.Payment, bool, error)
```

`GetPayment` is unchanged from the original design: `(Payment{}, false,
nil)` for not-found, `(Payment{}, false, err)` for a real error, `(p,
true, nil)` for found.

## Migrations

Plain numbered SQL files under `go-api/migrations/` —
`0001_create_payments.sql` contains both `CREATE TABLE` statements
(including `idempotency_key` and its `UNIQUE` constraint from the start,
since nothing has been deployed yet). Applied manually: `psql
"$DATABASE_URL" -f go-api/migrations/0001_create_payments.sql`. No
migration-framework dependency in this phase — revisit once a second
migration is actually needed.

## PostgreSQL configuration

- **Connection string:** a single `DATABASE_URL` environment variable. A
  committed `.env.example` documents the shape with a placeholder, never
  a real credential.
- **Connection pool:** stdlib `database/sql`'s built-in pool
  (`SetMaxOpenConns` with a small sensible default).
- **Startup connectivity check:** `db.PingContext(ctx)` with a bounded
  timeout, run before the HTTP server starts accepting traffic.
- **Graceful shutdown:** `db.Close()` added to the existing shutdown
  sequence, alongside the gRPC connection close and the HTTP server's
  `Shutdown(ctx)`.

## API design

### POST /payments

Request headers: `Idempotency-Key: <opaque client-chosen string>`
(required).

Request body:
```json
{
  "source_chain": "ethereum",
  "destination_chain": "base",
  "asset": "USDC",
  "amount": "1000.00"
}
```

Success — **201 Created** (new payment) or **200 OK** (exact replay of an
existing one), header `Location: /payments/{id}` on both:
```json
{
  "id": "a1b2c3d4-...",
  "source_chain": "ethereum",
  "destination_chain": "base",
  "asset": "USDC",
  "amount": "1000.00",
  "status": "ROUTED",
  "total_fee": 1.9581423176459705,
  "hops": [
    {
      "hop_index": 0,
      "from_chain": "ethereum",
      "to_chain": "optimism",
      "bridge_name": "Stargate#1",
      "fee": 0.8300611962648615,
      "latency_ms": 2639.9631974376093,
      "liquidity": 441096.81211234996,
      "reliability": 0.8601490009039938
    }
  ],
  "created_at": "2026-09-11T12:00:00Z",
  "updated_at": "2026-09-11T12:00:00Z"
}
```

**409 Conflict** (same key, different request):
```json
{"error": "Idempotency-Key already used with a different request"}
```

Other error bodies: `{"error": "..."}`, status per the "Failure behavior"
table.

### GET /payments/{id}

Unchanged: `200 OK` with the same body shape, or `404 {"error": "payment
not found"}` (a malformed id is also just 404). Registered as `"GET
/payments/{id}"` via Go 1.22+ `ServeMux` pattern matching and
`r.PathValue("id")`.

## POST /routes stays public

No compelling reason to remove it. It is stateless and read-only, and
remains independently useful — e.g. a client checking route
feasibility/pricing before committing to creating a payment.

## The crash window, and how idempotency resolves it

The scenario: the database transaction commits → the process crashes
before the HTTP response is written → the client sees a connection
failure, not the success response → the client retries.

**Before this revision**, a retry with no deduplication mechanism would
create a second, independent payment row. **With the `Idempotency-Key`
mechanism above**, a retry that reuses the same key (which any correctly
implemented client naturally does when retrying the same logical
operation) is now safe: the retry's `INSERT ... ON CONFLICT DO NOTHING`
finds the already-committed row, the comparison confirms the request is
identical, and the client receives `200` with the original payment — no
duplicate is created, and this holds even if the retry races against
another concurrent retry of the same failed request.

This does **not** eliminate every possible client-side mistake — a client
that generates a *new* idempotency key on every retry (defeating the
purpose) will still get duplicate rows, but that is a client
implementation error, not a gap in this design; the server-side guarantee
is complete for any client that reuses its key on retry, which is the
documented contract of the header.

## Crash-point durability analysis

| Crash point | Durable state after | Retry behavior |
|---|---|---|
| Before or during routing | None | Safe: routing simply reruns |
| After routing succeeds, before the DB transaction begins | None | Safe: routing reruns, no duplicate risk |
| During the transaction, before `COMMIT` | None — Postgres guarantees an uncommitted transaction is invisible | Safe: the retry's insert proceeds normally |
| After `COMMIT`, before the HTTP response is sent | A complete, valid payment exists | **Safe** — a same-key retry finds and returns the existing payment (200), rather than duplicating it |

## Concurrency behavior

Two `POST /payments` requests with **different** idempotency keys are
fully independent, exactly as before: separate rows, no shared mutable
state, Postgres's MVCC handles concurrent inserts natively.

Two requests with the **same** key are no longer independent by design —
they now race for the same logical row, and that race is resolved
entirely by the `UNIQUE (idempotency_key)` constraint plus `ON CONFLICT
DO NOTHING`: exactly one wins and inserts, every other concurrent request
(however many, however tightly timed) observes the conflict and returns
the winner's payment. No application-level mutex, `SELECT ... FOR
UPDATE`, or advisory lock is used or needed — the unique index itself is
the synchronization primitive.

The connection pool (`SetMaxOpenConns`) remains the only bound on
concurrent database access; a burst of requests queues for a free
connection rather than failing.

## Testing strategy

**Unit** (`internal/handler/payments_test.go`): `httptest`-based, with
fakes for both `RoutingClient` and `PaymentStore`, covering every row of
the "Failure behavior" table (including the missing-header and
too-long-key cases, and the tightened amount-format rejections: too many
integer digits, too many fractional digits, zero, negative sign,
exponent notation) plus successful create (201) and a faked replay (200)
and faked conflict (409) response shape.

**Postgres integration tests** (`internal/postgres/store_integration_test.go`,
`//go:build integration`, so default `go test ./...` needs no live
database) — the five cases requested, each against a real PostgreSQL
instance:

1. **Normal creation** — a fresh key creates a payment; `Created` result;
   row exists with the key stored.
2. **Sequential identical retry** — same key, same body, called twice:
   first call returns `Created`; second returns `Replayed` with the
   identical payment `id`; exactly one row exists in `payments`
   afterward.
3. **Same key, different request** — same key, then a second call with a
   different `amount`: second call returns `Conflict`; still exactly one
   row, unchanged, holding the *original* amount.
4. **Concurrent same-key creation** — N goroutines (e.g. 10), released
   simultaneously via a shared start barrier, all calling
   `CreateOrGetPayment` with the identical key and body: afterward,
   exactly one row exists for that key; exactly one goroutine's result is
   `Created`, all others are `Replayed`; every result references the
   same payment `id`. This is the test that actually exercises Postgres's
   row-level serialization, not just single-threaded logic.
5. **Commit succeeds, response lost, then retry** — call
   `CreateOrGetPayment` directly against the store (simulating "the
   database transaction committed") without going through the HTTP
   layer, discard its result (simulating "the process crashed before the
   client saw the response"), then issue a real HTTP `POST /payments`
   with the same key and body: assert `200`, with the same payment `id`
   the direct store call produced — the literal scenario from "The crash
   window," proven safe.

**End-to-end**: extend `scripts/e2e_test.sh` with a required
`Idempotency-Key` header on its existing payments test case, plus a new
case sending the same key twice and asserting `201` then `200` with
matching bodies.

**Restart verification**: unchanged from the original design — `SIGTERM`
the Go process after creating a payment, start a fresh process against
the same `DATABASE_URL`, `GET /payments/{id}` again.

## Guarantees Phase 5 provides

- A payment visible via `GET` is always complete — payment and all hops,
  atomically.
- The authoritative payment amount is exact decimal, end to end, never
  float64, anywhere in the pipeline, including idempotency comparison.
- A `201`/`200` response means the payment was already durably committed
  to PostgreSQL before the response was sent.
- Retrying `POST /payments` with the same `Idempotency-Key` and a
  logically identical request never creates more than one payment, even
  under concurrent retries — enforced by a PostgreSQL `UNIQUE` constraint,
  not application logic.
- Reusing an `Idempotency-Key` with a different request is rejected
  (`409`), never silently accepted or silently overwritten.
- Data survives Go process restarts.
- Validation is defense-in-depth at both the Go and PostgreSQL layers,
  and amount validation specifically matches the database's own storage
  limits, so a client error surfaces as a 400, never a database error.

## What Phase 5 does NOT provide yet

- No payment execution of any kind — `ROUTED` represents a routing
  decision, not a fund movement; nothing moves on any blockchain.
- No update, cancellation, or deletion endpoint for payments.
- No asynchronous processing, background workers, Kafka, or Redis.
- No authentication or authorization on any endpoint.
- No listing or pagination endpoint — only lookup by a known ID.
- No protection against a client that generates a *new* idempotency key
  on every retry — the guarantee is complete only for clients that reuse
  their key, which is the documented contract of the mechanism.
