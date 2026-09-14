# Phase 6 Design: Asynchronous Payment Processing via Kafka

## Purpose

Phase 5 gave ChainRoute durable payment creation with exactly one externally
visible status, `ROUTED`. Phase 6 introduces asynchronous execution of routed
payments through a worker that consumes Kafka events and simulates execution,
carrying the lifecycle to `PROCESSING` and then `COMPLETED` or `FAILED`. The
purpose of this phase is not blockchain realism — it is to correctly implement
the distributed-systems failure modes that appear once PostgreSQL, Kafka, and
an asynchronous worker are combined: the gap between a database commit and a
message publish, at-least-once delivery, duplicate consumption, and crash
recovery.

Kafka is the sole new infrastructure dependency. PostgreSQL remains the source
of truth for all durable state. No Redis, no distributed locks beyond what
PostgreSQL itself provides, no real blockchain interaction, no actual fund
movement.

## 1. Repository structure

```
go-api/
  cmd/
    server/main.go              (existing HTTP API — additive changes only)
    worker/main.go              (NEW — separate binary)
  internal/
    events/
      routed_payment.go         (NEW — Kafka payload shape)
    execution/
      execution.go              (NEW — deterministic simulated execution)
    kafka/
      producer.go               (NEW — thin producer wrapper)
      consumer.go                (NEW — thin consumer-group wrapper)
    payment/
      payment.go                 (extended — PROCESSING/COMPLETED/FAILED)
    postgres/
      store.go                   (extended — outbox insert, claim/transition
                                   queries, recovery sweep query)
      store_integration_test.go  (extended)
    worker/
      processor.go               (NEW — Kafka event handler: claim -> execute
                                   -> transition, used by the consumer loop)
      publisher.go                (NEW — outbox claim/publish loop)
      recovery.go                 (NEW — stale-PROCESSING recovery sweep)
    handler/, grpcclient/        (unchanged)
  migrations/
    0002_payment_processing.sql  (NEW)
scripts/
  e2e_test.sh                    (extended — same canonical script)
```

**The worker is a separate Go binary from the HTTP API.** Its runtime shape
is fundamentally different: an indefinite Kafka consume loop rather than a
request/response server. Coupling them would mean an API deploy interrupts
in-flight payment processing, and a worker crash would take down payment
routing. This mirrors why `cpp-routing-service` and `go-api` are already
separate processes — different concerns, independent lifecycles.

**One worker binary runs three goroutines**: the outbox publisher, the Kafka
consumer, and the recovery sweep (§11). They are not split into separate
binaries — all three share the same DB pool and Kafka client, and nothing in
Phase 6's scope requires scaling them independently. Splitting them further
would be exactly the kind of unjustified service proliferation the project
avoids; it remains a small change later if a concrete need for independent
scaling appears.

**Kafka client library:** `github.com/segmentio/kafka-go` — pure Go, no
cgo/librdkafka dependency, consistent with this project's consistent
preference for simple, dependency-light drivers (the same reasoning that led
to choosing `pgx` via `database/sql` over a heavier option in Phase 5).

## 2. Payment state machine

States: `ROUTED`, `PROCESSING`, `COMPLETED`, `FAILED`.

Legal transitions:
- `ROUTED -> PROCESSING` (a worker claims the payment)
- `PROCESSING -> COMPLETED` (simulated execution succeeds)
- `PROCESSING -> FAILED` (simulated execution fails)

No other transitions exist. `COMPLETED` and `FAILED` are terminal. There is no
direct `ROUTED -> COMPLETED`/`FAILED`, and no `PROCESSING -> PROCESSING`.

PostgreSQL cannot natively express "valid state machine transitions," so
transition legality is enforced by a conditional `UPDATE` whose `WHERE`
clause encodes the precondition:

```sql
UPDATE payments SET status = 'PROCESSING', updated_at = now()
WHERE id = $1 AND status = 'ROUTED';
```

The number of rows affected tells the caller whether the transition actually
happened. This single mechanism serves as the transition-legality guard, the
concurrency-correctness boundary for concurrent claims (§8), and the
guarantee that exactly one terminal outcome is ever persisted per payment.
No separate machinery is needed for any of these. **It does not, by itself,
guarantee that `execution.Execute` is only ever invoked once per payment** —
§7 draws this distinction precisely, because the recovery sweep (§11)
introduces a case where the same payment's execution can genuinely be
attempted more than once, even though only one outcome is ever committed.

Schema changes (`payments` table, via migration `0002`):
- `status` keeps its `TEXT ... CHECK (status IN (...))` shape from Phase 5,
  extended to `('ROUTED', 'PROCESSING', 'COMPLETED', 'FAILED')`. Still not a
  Postgres `ENUM` type, for the same reason as Phase 5: easier to extend via
  a constraint swap than `ALTER TYPE`.
- New column: `completed_at TIMESTAMPTZ NULL`. Set when the payment leaves
  `PROCESSING` for either terminal state. Named for "when the PROCESSING
  phase ended," not "when it succeeded" — it is set on both `COMPLETED` and
  `FAILED`.

## 3. Kafka topic and event design

One topic, one thin event type. The event body:

```json
{
  "payment_id": "c869364e-8d1c-4e54-bda8-0465dd935abc",
  "event_type": "PAYMENT_ROUTED",
  "occurred_at": "2026-09-12T00:44:44.243881Z"
}
```

The event does **not** carry the full payment or its route hops. PostgreSQL
is the source of truth: the consumer always re-reads current state from
`payments` before acting on it, so shipping a snapshot into Kafka would only
create a second, staleness-prone copy of data the consumer must fetch fresh
anyway in order to safely perform its conditional transition. `payment_id` is
the only field required for correctness; `event_type` costs nothing today and
allows future event types on the same topic without a topic-per-type
proliferation; `occurred_at` supports latency observability.

## 4. Why naive retry fails and the outbox pattern is required

The failure under analysis:

```
BEGIN
INSERT payment
INSERT route hops
COMMIT

kafka.Publish(payment)
-> FAILS
```

The payment is durably `ROUTED` in PostgreSQL; no Kafka event exists anywhere
to eventually process it.

The reason a naive in-process retry does not fix this: `kafka.Publish` and
the Postgres `COMMIT` are two independent operations with no atomic bridge
between them, and no two-phase commit spans them. An inline retry loop only
shrinks the failure window — it does not close it. A process crash *during*
the retry loop (not just the original publish) still leaves the payment
durably `ROUTED` with no event anywhere and, critically, no record that one
was ever owed. The retry attempt's own memory of "I still need to publish
this" was never itself durable.

**Phase 6 implements the transactional outbox pattern.** Moving "an event
must eventually be published" into the same Postgres transaction as the
payment insert converts an unrecoverable race ("publish now, or lose the
obligation forever") into a durably recorded fact that a separate, crash-safe
process can retry indefinitely — because the record of the obligation
survives independently of whichever process last tried to act on it. This
requires no distributed transaction across Postgres and Kafka. The gap it
closes is exactly the one demonstrated above; this is not an option applied
for its own sake but the smallest correct closure of a concretely
demonstrated failure.

## 5. Outbox schema and claiming

```sql
CREATE TABLE outbox_events (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id   UUID NOT NULL REFERENCES payments(id),
    event_type   TEXT NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ NULL
);

CREATE INDEX outbox_events_unpublished_idx
    ON outbox_events (created_at) WHERE published_at IS NULL;
```

- **Event ID**: `id`, a fresh `gen_random_uuid()` per row. Used for
  observability/tracing; it is not the correctness mechanism for consumer
  idempotency (that is §7).
- **Event type**: `event_type` text column, e.g. `'PAYMENT_ROUTED'` — not a
  Postgres `ENUM`, matching `payments.status`'s reasoning.
- **Aggregate/payment ID**: `payment_id`, a plain foreign key to
  `payments(id)`. Named concretely rather than a generic `aggregate_id`,
  since only one aggregate type (`Payment`) exists in this system — a
  generic name would be speculative generality with no current referent.
- **Payload**: `JSONB`, the exact event body shown in §3.
- **`created_at`**: insertion time, used for FIFO-ish ordering of the
  publisher's poll (not a correctness requirement, just a reasonable
  default order).
- **`published_at`**: `NULL` means unpublished. Set exactly once, by the
  publisher, after a successful Kafka publish.

**Insertion**: the outbox row is inserted in the *same transaction* as the
payment and hop rows — an extension of Phase 5's existing
`CreateOrGetPayment` transaction, not a new transaction boundary.

**Claiming**: `SELECT ... FOR UPDATE SKIP LOCKED`, one row per publisher
iteration:

```sql
BEGIN;

SELECT id, payment_id, event_type, payload
FROM outbox_events
WHERE published_at IS NULL
ORDER BY created_at
FOR UPDATE SKIP LOCKED
LIMIT 1;

-- publish to Kafka: key = payment_id, value = payload

UPDATE outbox_events SET published_at = now() WHERE id = $1;

COMMIT;
```

If no unpublished row exists, the publisher sleeps for
`OUTBOX_POLL_INTERVAL_MS` and retries.

**Concurrent publisher behavior**: multiple publisher instances (or, within
Phase 6's single worker binary, a single publisher goroutine — but the
mechanism is designed to be correct under N) naturally skip whatever row
another instance currently holds locked. No `claimed_at`/`claimed_by`
bookkeeping is needed, and none is added: `SKIP LOCKED` already provides
mutual exclusion, and if a publisher crashes mid-transaction, Postgres
releases the row lock the instant the connection drops — the row becomes
claimable again with no manual staleness/expiry logic required. Adding claim
timestamp columns here would duplicate what Postgres's native locking already
guarantees.

**Accepted tradeoff: the transaction spans the Kafka publish call.** The
claim transaction above stays open for the full duration of the Kafka
`publish` network call, not just the local database statements. If the
broker is slow or briefly unreachable, this holds one Postgres row lock and
occupies one pooled connection for however long that call takes. At Phase
6's scale — a background, non-request-path responsibility, not a hot path —
this is an accepted, explicit scalability tradeoff, not an oversight: it
keeps the claiming protocol to a single transaction with no separate
lease/expiry bookkeeping. A `claimed_at`-with-timeout ("lease") scheme would
decouple the lock's lifetime from the Kafka call's latency, but it would
reintroduce exactly the claim-timestamp infrastructure this design already
chose to avoid above, in order to solve a scaling problem Phase 6 has not
demonstrated it has. This is revisited only if a concrete throughput or
connection-pool-exhaustion problem actually appears in practice — not
preemptively.

**Publish succeeds, but marking published fails**: the row's `UPDATE` and the
surrounding `COMMIT` never complete, so the transaction rolls back and
`published_at` stays `NULL`. The row becomes claimable again and is published
a second time on a later pass. This is an expected, correct outcome under
at-least-once delivery — not a bug to be prevented, which is exactly why
consumers must be idempotent (§7).

## 6. At-least-once, not exactly-once

The outbox pattern guarantees an event is published **at least once**,
never exactly once, precisely because of the scenario in §5's last
paragraph:

```
Kafka publish succeeds
      |
process crashes (before the published_at UPDATE commits)
      |
outbox row still appears unpublished
      |
event gets published again
```

Phase 6 treats this as a first-class, named property of the system, not an
implementation detail to be hidden: **Kafka consumers must tolerate
duplicate events.** §7 designs the mechanism that makes this safe.

## 7. Idempotent consumer — five distinct duplicate-safety guarantees, no inbox table

The original draft of this design claimed the claim `UPDATE` made duplicate
execution impossible. That claim was too strong once the recovery sweep
(§11) is considered, and it collapsed five genuinely different things that
can each happen more than once into one category. They must be kept
separate, because each has a different guarantee:

1. **Duplicate Kafka delivery** — CAN happen. At-least-once delivery (§6)
   means the same message may reach a consumer more than once.
2. **Duplicate handler claim** — CANNOT happen. The claim `UPDATE`,
   ```sql
   UPDATE payments SET status = 'PROCESSING', updated_at = now()
   WHERE id = $1 AND status = 'ROUTED';
   ```
   is guarded by PostgreSQL row-level locking: exactly one delivery, ever,
   transitions a given payment from `ROUTED` to `PROCESSING`. Every other
   delivery — concurrent (§8) or later — sees `0` rows affected and never
   proceeds to execute via this path.
3. **Duplicate execution *attempt*** — CAN happen. This is the corrected
   part of the design. `execution.Execute(paymentID)` can genuinely be
   invoked more than once for the same payment: if the worker that claimed
   a payment stalls — is still alive, just slow, not crashed — for longer
   than the recovery sweep's staleness threshold, the sweep will
   independently call `Execute` for that same payment while the original
   worker's own call may still be in flight or about to persist. Both calls
   genuinely execute. This is distinct from a duplicate *claim*: the sweep
   does not re-enter via `ROUTED -> PROCESSING` (case 2, which remains
   exactly-once) — it acts directly on stale `PROCESSING` rows (§11).
4. **Duplicate persisted terminal transition** — CANNOT happen. Whichever
   actor — the original worker or the recovery sweep — issues its
   `PROCESSING -> COMPLETED`/`FAILED` `UPDATE` first wins; the guard
   `WHERE status = 'PROCESSING'` means the second attempt, regardless of
   what its own `Execute` call computed, affects `0` rows and is discarded.
   Exactly one terminal value is ever committed per payment.
5. **Duplicate external side effect** — not applicable within Phase 6 (see
   below), but a real, named risk for any future system built on this
   pattern.

Because Phase 6's `execution.Execute` is pure and side-effect-free (§10), a
duplicate execution *attempt* (case 3) is harmless: both invocations compute
the identical deterministic result, and only one of them is ever actually
persisted (case 4). **This is not incidental — it is why `Execute` was
designed to be pure in the first place.** The safety of tolerating duplicate
execution attempts depends entirely on execution having no side effects; it
is not a general property of "conditional state transitions," and it does
not extend automatically to a hypothetical future executor that performs a
real external effect (a real bridge transfer, a real API call):

```
PROCESSING
   -> external transfer succeeds
   -> worker crashes before the terminal DB update
   -> recovery cannot determine, from PostgreSQL alone, whether the
      external effect already occurred
   -> blindly re-executing could duplicate the transfer
```

PostgreSQL's conditional state transition only ever reports what has or
hasn't been *persisted in PostgreSQL* — it has no visibility into whether a
separate, external system already performed a real-world effect. Phase 6
deliberately does not attempt to solve this: there is no real external
effect to protect in this phase, so building machinery for it now — a
distributed lock, an inbox/processed-events table, Kafka transactions, or a
generalized execution ledger — would be speculative infrastructure hiding a
problem this phase does not have, rather than serving a concrete Phase 6
need. A future phase that introduces a real external side effect would need
its own mechanism at that specific boundary — most plausibly an idempotent
external operation keyed by `payment_id` or a dedicated `execution_id` (the
same idempotency-key pattern Phase 5 already uses at the HTTP boundary,
applied at whatever external system a future executor integrates with), or a
reconciliation step that queries the external system's own record before
retrying. That is out of scope for Phase 6; this document names it as a
boundary Phase 6 does not cross, rather than an unnamed gap.

**No inbox/processed-events table is added in Phase 6.** What Phase 6
actually needs — exactly-once entry into `ROUTED -> PROCESSING` (case 2) and
exactly-once persistence of the terminal outcome (case 4) — is fully solved
by the conditional `UPDATE` alone. An inbox table would only become
necessary if Phase 6 needed to guarantee exactly-once *execution attempts*
rather than exactly-once *persisted outcomes*, and, per the analysis above,
it doesn't: redundant invocations of a pure function are free.

## 8. Concurrent duplicate workers

Two workers racing to claim the same payment issue the same conditional
`UPDATE` concurrently. Postgres's row-level locking serializes the two
statements: exactly one commits with `1` row affected; the other,
deterministically, gets `0`, regardless of timing or which process reaches
the database first. This is the same pattern Phase 5 used for
`POST /payments` idempotency (`ON CONFLICT DO NOTHING`) — applied here to a
state transition instead of a row insertion. No in-memory mutex, no
distributed lock, and no additional coordination primitive is introduced;
PostgreSQL alone provides the correctness boundary.

This is a different scenario from the stalled-worker/recovery-sweep race
discussed in §7 and §11. Here, both actors attempt the *same* transition
(`ROUTED -> PROCESSING`) at effectively the same instant, and Postgres's row
lock trivially serializes them — this is duplicate *claim* prevention (§7,
case 2), which remains unconditionally exactly-once. §7/§11 address a
different case: one actor has already succeeded at this transition, and a
second actor (the recovery sweep) later acts on the same payment while it is
still `PROCESSING` — that is a duplicate *execution attempt* (§7, case 3), a
weaker guarantee this design accepts because execution is pure.

## 9. Worker transaction boundaries

Two separate transactions per payment, not one spanning the full cycle:

1. **Claim**: `UPDATE payments SET status='PROCESSING', updated_at=now()
   WHERE id=$1 AND status='ROUTED'`, committed *before* simulated execution
   begins.
2. **Completion**: `UPDATE payments SET status=$2, completed_at=now(),
   updated_at=now() WHERE id=$1 AND status='PROCESSING'`, committed *after*
   simulated execution finishes, where `$2` is `'COMPLETED'` or `'FAILED'`.

Keeping these as two separate transactions, rather than wrapping execution
inside one long transaction, is deliberate: it means a crash during execution
leaves the payment durably `PROCESSING`, not silently rolled back to
`ROUTED`. This is the honest failure mode a real executor — one making real
external calls that cannot be wrapped in a database transaction — would
actually have. Collapsing claim and completion into a single transaction
would hide this failure mode entirely and leave §11's recovery mechanism
untested by anything real.

## 10. Simulated execution

A pure, deterministic function with no I/O, no sleeping, and no external
calls of any kind:

```go
package execution

type Result struct {
    Success bool
    Reason  string
}

func Execute(paymentID string) Result {
    h := fnv.New64a()
    h.Write([]byte(paymentID))
    if h.Sum64()%10 == 0 {
        return Result{Success: false, Reason: "simulated execution failure"}
    }
    return Result{Success: true}
}
```

"Execution" here is a stand-in for whatever a real payment processor would do
to move funds along the routed path — the phase explicitly does not attempt
blockchain realism. `Execute` takes only the payment ID and is fully
deterministic: the same ID always produces the same result. This determinism
is load-bearing, not incidental — it is what allows §11's recovery sweep to
safely recompute a crashed execution and be certain it reproduces the exact
outcome the original attempt would have persisted, with no risk of a retry
silently flipping a payment from would-have-succeeded to would-have-failed
or vice versa. The ~10% failure rate (`sum % 10 == 0`) gives `FAILED` a real,
reproducible, testable trigger.

## 11. PROCESSING durability and recovery

`PROCESSING` is a committed row and therefore durable by construction — it
survives a worker crash unconditionally (§9). The open question is what
un-sticks a payment if the worker that claimed it dies before completing it.

**A recovery sweep**, running as a third goroutine inside the worker binary,
periodically re-completes stale `PROCESSING` rows directly:

```sql
UPDATE payments
SET status = $2, completed_at = now(), updated_at = now()
WHERE id = $1
  AND status = 'PROCESSING'
  AND updated_at < now() - interval '2 minutes';
```

The sweep selects candidate rows (`status='PROCESSING' AND updated_at <
now() - staleness threshold`), re-runs `execution.Execute(paymentID)` for
each, and applies the update above with `$2` set to whatever `Execute`
deterministically produced.

This deliberately does **not** transition `PROCESSING` back to `ROUTED`.
Reintroducing that backward edge would complicate the state machine (§2) and
require routing the payment through Kafka a second time. Because execution is
a pure function of the payment ID, the sweep can safely complete the payment
directly — it is guaranteed to compute the same result the original,
crashed attempt would have. This is the mechanism that prevents a payment
from ever being permanently stranded in `PROCESSING`.

**The staleness threshold is a heuristic, not a certainty.** It identifies
"this worker has probably died"; it cannot actually distinguish that from
"this worker is still alive and simply slow." This means the recovery sweep
and a merely-stalled — not actually crashed — original worker can race: both
may independently call `execution.Execute` for the same payment around the
same time, and both may attempt their own completion `UPDATE`. §7 names this
precisely as a duplicate *execution attempt*, distinct from a duplicate
*claim* or a duplicate *persisted transition*, and explains why it is safe
here: execution is pure, so a redundant attempt is wasted computation, never
incorrect computation, and the `WHERE status = 'PROCESSING'` guard on both
actors' completion `UPDATE` ensures only one of them ever actually commits.
Phase 6 does not add a heartbeat/lease mechanism to more precisely
distinguish "dead" from "slow" workers — that would only matter if execution
had side effects worth protecting more tightly, and it does not; adding one
now would be speculative infrastructure for a problem this phase does not
have.

Sweep interval and staleness threshold are configurable (§16) —
`WORKER_RECOVERY_SWEEP_INTERVAL_SECONDS` (default 30s) and
`WORKER_RECOVERY_STALENESS_SECONDS` (default 120s).

## 12. Exhaustive worker crash-point analysis

The table distinguishes the three guarantees from §7 that actually vary by
crash point (duplicate claim, duplicate execution attempt, duplicate
persisted terminal transition — duplicate Kafka delivery is implicit in
"redelivered" appearing at all, and duplicate external side effect does not
apply anywhere in this table, since Phase 6's `Execute` has none).

| # | Crash point | Durable Postgres state | Kafka state | On restart / redelivery | Duplicate claim? | Duplicate execution attempt? | Duplicate persisted transition? |
|---|---|---|---|---|---|---|---|
| 1 | Before claiming work | Unchanged (e.g. `ROUTED`) | Offset not committed | Event redelivered; processing starts fresh | No | No — `Execute` was never called before the crash; the redelivery is the only invocation | No |
| 2 | After consuming the Kafka event, before any DB work | Unchanged | Offset not committed | Same as #1 | No | No — same reasoning | No |
| 3 | After claiming (`ROUTED`\-\>`PROCESSING` committed), before `Execute` is called | `PROCESSING` | Offset not committed | Redelivered event's claim attempt: `0` rows affected, skips `Execute` entirely. Recovery sweep (§11) eventually calls `Execute` once and completes it. | No | No — `Execute` is invoked exactly once, by the sweep | No |
| 4 | During `Execute` (the call began but the process died before or without returning) | `PROCESSING` (unchanged since claim) | Offset not committed | Redelivery skips `Execute` as in #3. Recovery sweep later calls `Execute` again. | No | **Yes** — `Execute` was invoked once by the crashed attempt and once more by the sweep | No — only the sweep's transition is ever persisted |
| 5 | After `Execute` returns, before the completion `UPDATE`/commit | `PROCESSING` (result computed but never persisted) | Offset not committed | Same as #4 | No | **Yes** — same reasoning | No |
| 6 | After the completion commit, before the Kafka offset commit | `COMPLETED`/`FAILED` (terminal, durable) | Offset not committed — Kafka **will** redeliver | Redelivered handler's claim check sees a terminal status, `0` rows affected, skips without calling `Execute`; offset committed on this pass | No | No — the terminal persist already happened before any redelivery could race with it | No |
| — | Worker **stalls** (does not crash) past the staleness threshold, but is still alive and eventually completes its own attempt | `PROCESSING` throughout | N/A — no crash, no redelivery involved | Recovery sweep calls `Execute` independently while the stalled worker's own call may still be in flight or about to persist | No — the sweep does not re-enter via `ROUTED`\-\>`PROCESSING` | **Yes** — both the stalled worker and the sweep may genuinely invoke `Execute` for the same payment | No — whichever of the two issues its `WHERE status = 'PROCESSING'` `UPDATE` first wins; the other affects `0` rows |

Two rows (4 and 5) show a duplicate execution attempt is possible from a
genuine crash alone, with no "stall" required: if the original worker had
already called `Execute` before dying, the sweep's later, independent call
is a second invocation. The bottom row shows the same property arising from
a stall rather than a crash. In every row, the persisted terminal transition
remains exactly-once regardless — that guarantee, unlike "no duplicate
execution," does hold uniformly across every case, because it rests on the
conditional `UPDATE`'s `WHERE status = 'PROCESSING'` clause rather than on
how many times `Execute` happened to run beforehand.

## 13. Kafka offset commit strategy

**Manual commit**, issued only after the full claim -> execute -> transition
cycle reaches a definitive outcome (a successful transition, or a recognized
no-op because the payment was already `PROCESSING`/terminal).

Auto-commit is explicitly rejected: it commits offsets on a background timer
independent of whether a given message was actually, fully handled. Under
auto-commit, a crash between offset-commit-interval boundaries could commit
an offset for a message whose processing crashed mid-cycle (crash points 3-5
in §12) — which would mean Kafka never redelivers it, leaving the payment's
only path to recovery solely dependent on the sweep in §11 rather than on
Kafka's own redelivery plus the sweep as a backstop. The chosen strategy
deliberately biases toward over-redelivery (safe, since the transition guard
absorbs it for free) over under-delivery (which would erode the guarantee in
§12 that recovery is always possible).

## 14. Ordering requirements

**No ordering guarantee is required, per-payment or globally.** Each payment
produces exactly one event in its entire Phase 6 lifecycle — there is
nothing to order relative to itself — and payments are independent of one
another; processing payment A before or after payment B has no correctness
implication anywhere in this design.

**Message key is still `payment_id`.** This costs nothing today and ensures
that if a future phase adds a second event type for the same payment (e.g. a
cancellation event), it lands on the same Kafka partition as the original —
free insurance against a harder migration later, chosen even though nothing
in Phase 6 itself currently requires it.

## 15. Topic and consumer group naming

- Topic: `chainroute.payments.routed`
- Consumer group: `chainroute-payment-worker`

Additional worker replicas simply join this same consumer group — standard
Kafka horizontal scaling, requiring no naming or topology change.

## 16. Configuration via environment variables

| Variable | Purpose | Default |
|---|---|---|
| `DATABASE_URL` | Postgres connection (reused from Phase 5) | none, required |
| `KAFKA_BOOTSTRAP_SERVERS` | Comma-separated broker addresses | none, required |
| `KAFKA_TOPIC` | Topic name | `chainroute.payments.routed` |
| `KAFKA_CONSUMER_GROUP` | Consumer group ID | `chainroute-payment-worker` |
| `KAFKA_POLL_TIMEOUT_MS` | Consumer fetch/poll timeout | implementation default, overridable |
| `OUTBOX_POLL_INTERVAL_MS` | Outbox publisher poll interval | `500` |
| `WORKER_RECOVERY_SWEEP_INTERVAL_SECONDS` | Recovery sweep cadence | `30` |
| `WORKER_RECOVERY_STALENESS_SECONDS` | Staleness threshold for the sweep | `120` |

No credentials are committed. The local Redpanda instance used for
development and integration testing runs without authentication, matching
Phase 5's `.env.example` pattern of a committed template with placeholder
values only.

## 17. Startup behavior when Kafka is unavailable

API and worker are treated separately, deliberately:

- **`cmd/server` (HTTP API)**: entirely unaffected by Kafka's availability.
  `POST /payments` only ever writes to PostgreSQL (payment, hops, and the
  outbox row, all in one transaction) — it never talks to Kafka directly.
  The API's startup and availability have zero dependency on Kafka being
  reachable.
- **`cmd/worker`**: PostgreSQL is still blocking-ping-or-die at startup,
  matching the API's existing philosophy that a dead database is a fatal
  condition. Kafka is **not** blocking-or-die: a transiently unreachable
  broker at worker startup logs a warning and lets `kafka-go`'s own
  reconnect/retry behavior inside the consumer and producer loops take over,
  rather than crash-looping the entire process. The worker's recovery-sweep
  goroutine (which depends only on Postgres) continues to function
  independent of Kafka's availability.

## 18. Graceful shutdown

The worker uses the same `signal.NotifyContext` pattern as the existing
`cmd/server`:

1. Stop the Kafka consumer's poll loop — do not request the next batch of
   messages.
2. Allow any in-flight claim -> execute -> transition cycle to finish,
   bounded by a shutdown grace period.
3. Commit the Kafka offset for that in-flight message only if it reached a
   definitive outcome within the grace period; otherwise leave the offset
   uncommitted and let it redeliver on restart (safe, per §7/§12).
4. Stop the outbox publisher similarly: finish the current claim
   transaction (commit or roll back cleanly), do not begin a new one.
5. Stop the recovery-sweep ticker.
6. Close the Kafka consumer group and writer clients, triggering a clean
   rebalance rather than waiting out a session timeout.
7. Close the Postgres connection pool last, after every goroutine above has
   stopped.

## 19. Exposing asynchronous state via GET /payments/{id}

No mechanism change. `GetPayment` already issues a fresh `SELECT` against
`payments` on every call, with no caching layer anywhere in the system, so it
naturally reflects whichever status — `ROUTED`, `PROCESSING`, `COMPLETED`, or
`FAILED` — is currently committed. The JSON response gains a nullable
`completed_at` field. No push notification, webhook, or streaming mechanism
is introduced; clients are expected to poll `GET`, which is sufficient for
Phase 6's scope.

## 20. Two distinct idempotency problems

Phase 6 does not touch Phase 5's idempotency logic, but the two mechanisms
are easy to conflate as one concept, so the distinction is stated explicitly:

- **Client/API idempotency** (Phase 5, unchanged): protects against a client
  retrying `POST /payments` with the same `Idempotency-Key`, e.g. after a
  lost HTTP response. It answers "did the client already submit this?" and
  is enforced by the `UNIQUE(idempotency_key)` constraint plus
  `INSERT ... ON CONFLICT DO NOTHING RETURNING`. Phase 6 only adds one more
  `INSERT` (the outbox row) to this same, already-existing transaction; it
  does not alter the idempotency-key logic in any way.
- **Kafka consumer idempotency** (Phase 6, new): protects against the same
  Kafka event being delivered more than once to the worker. It answers "did
  the worker already act on this existing payment row?" and is enforced by
  the state-transition-guarded `UPDATE` (§7).

Different questions, different layers (HTTP API vs. asynchronous worker),
different mechanisms (a header plus a uniqueness constraint vs. a
state-machine-guarded conditional update).

## 21. Unit tests

- `execution.Execute`: a fixed table of known payment IDs mapped to expected
  results, asserting reproducibility (calling it twice for the same ID
  yields the same `Result`).
- The worker's per-event handling logic, against a fake `PaymentStore`
  (no real Kafka or Postgres): normal success path, normal failure path,
  "already `PROCESSING`" no-op path, "already terminal" no-op path.
- Handler-level tests for the new `completed_at` field's JSON
  serialization (including the `null` case).

## 22. PostgreSQL integration tests

Extending `store_integration_test.go` (or a sibling file in the same
package):

- Outbox row inserted atomically with the payment and its hops — verified by
  direct query within the same test.
- Concurrent `SELECT ... FOR UPDATE SKIP LOCKED` claiming: N goroutines
  racing over a small set of unpublished outbox rows (using a fake in-process
  publish function to record calls), asserting each row is published exactly
  once with no double-claim.
- Concurrent claim-`UPDATE` race for the same payment: N goroutines
  attempting `ROUTED -> PROCESSING` simultaneously (mirroring Phase 5's
  concurrent idempotency test), asserting exactly one succeeds (`1` row
  affected) and the rest see `0`.
- Recovery sweep: a payment manually set to `PROCESSING` with a backdated
  `updated_at`, then the sweep run directly, asserting it reaches a terminal
  state matching `execution.Execute`'s deterministic output for that ID.

## 23. Kafka integration tests

New build tag (e.g. `kafka_integration`), requiring a real local Redpanda
instance (installed via Homebrew, documented setup mirroring Phase 5's
Postgres installation task):

- Produce/consume round trip through the real `kafka` package wrapper:
  publish a `PAYMENT_ROUTED` event, consume it, assert the payload decodes
  correctly.
- Consumer group partition assignment: two consumer instances in the same
  group correctly split partitions (a smoke test confirming the wrapper's
  configuration is correct, not a deep Kafka behavior test).

## 24. Failure-injection tests

- **Duplicate Kafka delivery**: call the worker's handler function twice in
  a row with the identical event (no real redelivery needed — this only
  requires invoking the handler twice in-process), asserting exactly one
  execution and one persisted transition.
- **Concurrent duplicate consumers**: N goroutines calling the claim-handler
  for the same `payment_id` simultaneously, asserting exactly one executes
  (same pattern as §22's concurrent claim test, exercised at the handler
  level).
- **Publish succeeded, outbox mark failed**: force the mark-published
  transaction to fail after a successful fake publish (e.g. cancel the
  context between the publish call and the `UPDATE`), asserting the row
  remains unpublished and is republished on the next poll — an explicit,
  expected duplicate, not a failure of the test.
- **Worker DB commit succeeded, offset commit skipped**: commit the
  `PROCESSING -> COMPLETED`/`FAILED` transaction, then deliberately skip the
  offset-commit step (simulating crash point 6 in §12), then redeliver the
  identical event to a fresh handler call — asserting the redelivered call's
  claim attempt affects `0` rows, `Execute` is not re-invoked, and the
  offset is committed on this second pass.
- **Worker crash/restart**: kill the worker process mid-`PROCESSING`
  (integration/E2E level, mirroring Phase 5's SIGTERM-restart pattern),
  asserting the recovery sweep — with a shortened test-only interval —
  eventually completes the payment.
- **Stalled-worker vs. recovery-sweep race**: simulate a worker that has
  successfully claimed a payment (`ROUTED -> PROCESSING` committed) and is
  artificially delayed — via a test hook, not a real crash — past the
  staleness threshold before it attempts its own completion `UPDATE`; run
  the recovery sweep concurrently against the same payment. Assert: both
  the stalled worker's own `Execute` call and the sweep's `Execute` call are
  allowed to occur (this test exists specifically to exercise §7 case 3, not
  to prevent it), exactly one of the two completion `UPDATE`s actually
  succeeds (`1` row affected), the other affects `0` rows, and the payment
  ends in exactly one, consistent terminal state with `completed_at` set
  once. This is the test that directly proves the duplicate-execution-
  attempt-but-not-duplicate-persistence property this design accepts.
- **Kafka temporarily unavailable**: start the worker before Redpanda is
  reachable (or stop Redpanda mid-run), asserting the worker does not
  crash-loop, logs and retries, and resumes processing once Redpanda becomes
  available again.

## 25. E2E extension

Extends the existing `scripts/e2e_test.sh` — one canonical E2E entrypoint,
not a second script — to also start Redpanda and the new worker binary
alongside the C++ routing service and the Go API:

```
POST /payments
   -> routed through the C++ service (existing)
   -> payment + hops + outbox event persisted atomically (Phase 5 + this phase)
   -> outbox publisher publishes the Kafka event
   -> worker consumes it
   -> simulated execution runs
   -> payment transitions to COMPLETED or FAILED
   -> GET /payments/{id} observes the terminal state
```

The test polls `GET /payments/{id}` with a short retry loop (e.g. up to 5
seconds, checked every 100ms) and asserts the status is **either**
`COMPLETED` or `FAILED` — not a hard-coded outcome. Because `execution.Execute`
is a function of a server-generated UUID the test cannot pre-select, asserting
"reached a terminal state" is the honest, non-flaky claim available at the
E2E level; pinning an exact outcome for a known ID is the unit test's job
(§21), not this one's.

## 26. Guarantees provided

- Payment, route hops, and the outbox event are inserted atomically in one
  Postgres transaction (extends Phase 5's atomicity guarantee).
- Every committed outbox row is eventually published to Kafka at least once,
  provided the publisher process eventually runs again after any crash — no
  event is silently and permanently lost while its outbox row still exists.
- Kafka delivers each message to the consumer group at least once, under
  manual offset commit.
- Every payment reaches exactly one persisted terminal outcome —
  `COMPLETED` or `FAILED` — and it is never overwritten once set. This holds
  regardless of how many times `execution.Execute` was actually invoked for
  that payment (§7, §12); only one terminal `UPDATE` per payment ever
  succeeds.
- `execution.Execute` may genuinely be invoked more than once for the same
  payment — a duplicate *execution attempt* (§7 case 3), distinct from a
  duplicate claim or a duplicate persisted transition — if the payment's
  worker stalls or crashes mid-attempt and the recovery sweep independently
  re-executes it (§11, §12). This is safe in Phase 6 specifically because
  `Execute` is pure and side-effect-free: redundant invocations always
  compute the identical result, and only one is ever committed.
- A payment can never become permanently stuck in `PROCESSING` — the
  recovery sweep guarantees eventual forward progress once the staleness
  threshold elapses.
- Phase 5's `POST /payments` client-facing idempotency is fully preserved,
  unchanged.

## 27. What Phase 6 does not guarantee

- Exactly-once Kafka delivery, or exactly-once outbox publication — both are
  explicitly at-least-once, made safe by an idempotent consumer instead.
- Exactly-once invocation of `execution.Execute` — a payment's execution may
  genuinely be attempted more than once (§7 case 3, §11, §12). Phase 6
  guarantees exactly-once *persistence* of the terminal outcome, not
  exactly-once *computation* of it.
- Exactly-once external side effects for any future executor that performs
  a real external operation. PostgreSQL's conditional state transitions
  guarantee exactly-once persistence of PostgreSQL state — they say nothing
  about whether an external system was called once or twice. A future phase
  that replaces `execution.Execute` with a real external call (an actual
  bridge transfer, a real payment-provider API) would need its own
  idempotency mechanism at that specific boundary — most plausibly an
  idempotent external operation keyed by `payment_id`/`execution_id`, or a
  reconciliation step against the external system's own record — which
  Phase 6 deliberately does not build now, since no real external effect
  exists yet to protect (§7).
- Any ordering guarantee across different payments.
- Real fund movement or any real blockchain interaction of any kind —
  execution is entirely simulated.
- Bounded end-to-end processing latency — no SLA on how long
  `ROUTED -> COMPLETED`/`FAILED` takes; the worst case depends on the outbox
  poll interval, Kafka delivery timing, and, if a worker crash occurs, the
  recovery sweep's interval and staleness threshold.
- Automatic detection or alerting if both PostgreSQL and every worker
  instance are unavailable simultaneously for long enough that an outbox row
  goes unpublished indefinitely. Phase 6 adds no dead-letter queue and no
  alerting infrastructure — there is no concrete requirement for either yet,
  and adding them without one would be exactly the kind of unjustified
  abstraction this project avoids.

## Constraints preserved from prior phases

- The C++ routing algorithm and `router/`/`cpp-routing-service/` are
  untouched.
- `POST /routes` and `POST /payments`/`GET /payments/{id}` keep their
  Phase 4/5 behavior; Phase 6's only change to the payments write path is
  the additional outbox-row insert within the existing transaction, plus the
  wider set of legal `status` values and the new `completed_at` field.
- PostgreSQL remains the sole source of truth; Kafka is a delivery
  mechanism, never a store of record.
- No Redis, no distributed locks beyond PostgreSQL's own row-level locking,
  no microservice added without a concrete, stated reason.
