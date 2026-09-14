# Phase 7 Design: Real Testnet Bridge Execution via Across Protocol

## Purpose

Phases 1-6 built a complete simulated payment pipeline: deterministic routing,
durable persistence, and asynchronous execution — but "execution" has always
been a pure, repeatable, side-effect-free function. Phase 7 introduces the
first real external side effect: a signed Ethereum Sepolia transaction,
broadcast through Across Protocol's testnet deployment, bridged to Base
Sepolia, and reconciled to a real observed terminal outcome.

The purpose of this phase is not blockchain realism at scale — it is to prove
that ChainRoute can correctly reason about a **non-repeatable** external
operation: one that cannot be safely retried by just calling the same
function again, the way Phase 6's `execution.Execute` could. This is a
categorically different correctness problem than anything solved so far, and
Phase 7 exists specifically to solve it, narrowly, once, before any future
phase considers generalizing it.

No mainnet interaction, no real funds beyond Sepolia/Base Sepolia testnet
ETH/WETH, and no multi-chain/multi-provider generality are in scope.

## Across Protocol research (verified against docs.across.to; do not treat
## as current without re-checking at implementation time — bridge protocol
## APIs evolve)

- **Testnet API base URL**: `https://testnet.across.to/api`, distinct from
  the mainnet `https://app.across.to/api`.
- **Testnet chains**: 13 deployments are documented, including
  **Sepolia (chain ID `11155111`)** and **Base Sepolia (chain ID `84532`)**,
  both with deployed SpokePool contracts. The requested route is real and
  currently supported.
- **Testnet asset**: WETH is the consistently-documented testnet asset
  across the Sepolia and Base Sepolia contract-address reference pages. No
  authoritative testnet-wide token list was found beyond this. The worker
  must not hardcode blind trust in this — Task 1 of implementation verifies
  the actual supported testnet routes/tokens against the live API
  (`/available-routes` or equivalent) before any transaction is constructed.
- **Quote endpoint**: `GET /suggested-fees` — parameters `originChainId`,
  `destinationChainId`, `inputToken`, `outputToken`, `amount`. Response
  includes `totalRelayFee.total`, `lpFee.pct`, `relayerGasFee.total`,
  `expectedFillTimeSec`, `isAmountTooLow`. The docs explicitly state
  responses must **not be cached** — fees are market-, gas-, and
  utilization-dependent and can change between calls.
- **Status endpoint**: `GET /deposit/status` — returns one of
  `pending | filled | expired | refunded`. `filled` specifically means a
  `FilledRelay` event fired **on the destination chain** — this is the
  precise signal Phase 7 uses to define `COMPLETED` (see §15 below), not
  merely "origin transaction confirmed."
- **Authentication**: the mainnet API documents a required Bearer API key
  plus `integratorId` query parameter. Testnet's actual enforcement is not
  stated in the docs found. The client supports both as optional
  configuration; Task 1 confirms the real requirement against a live call
  before implementation proceeds further.
- **On-chain execution mechanism**: origination is a direct call to the
  SpokePool contract's `depositV3`-family function — not calldata returned
  by the quote API (that mechanism exists only for the separate `/swap`
  embedded-actions flow, which does not apply to a plain same-asset
  bridge). The exact current function signature and parameter order must be
  pulled from the audited ABI / `across-protocol/contracts` repository at
  implementation time, not guessed here.
- **Open item, honestly flagged**: whether `depositV3` accepts native ETH
  directly (with an internal auto-wrap step) or strictly requires
  pre-wrapped WETH as an ERC-20 input token was not conclusively resolved by
  the documentation fetched during design. This is deliberately isolated to
  one function in `execute.go` (§4) — it does not affect the schema,
  crash-safety design, or reconciliation logic, all of which are agnostic to
  this detail. It is confirmed and documented as part of Task 1.
- **Expected fill time**: testnet fills typically complete in about one
  minute (versus ~2 seconds on mainnet, due to the absence of relayer
  competition/incentives on testnet) — this directly informs the
  reconciler's polling interval and staleness expectations (§10, §16).
- **Testnet guidance**: developers are asked to test with small amounts
  (docs suggest amounts on the order of $1-equivalent).

**Implementation-time verification gate.** The research above is a
design-time snapshot, not a source of truth for implementation. Before any
implementation task proceeds past the Across integration bootstrap task
(Task 1 of the eventual implementation plan), the following must be
freshly re-verified against current, live Across testnet
behavior — never guessed, and never silently carried forward from this
document if it has changed: Sepolia -> Base Sepolia route availability;
current WETH addresses on both chains; current SpokePool contract
addresses on both chains; the exact `depositV3` (or successor) ABI and
function signature/parameter order in current use; whether native ETH or
strictly pre-wrapped WETH is required as `depositV3`'s input (the open
item above); the current `/suggested-fees` and `/deposit/status` request
parameters and response shape; and whether testnet actually enforces the
Bearer API key / `integratorId` requirements documented for mainnet. This
gate is unchanged by every other revision in this document — the fixes
above (§6, §7, §12, §15, §29) are all agnostic to which of these
implementation-time facts turn out to be true, by the same isolation
argument as the native-ETH-vs-WETH open item.

## 1. Phase objective — what changes from Phase 6

| | Phase 6 | Phase 7 |
|---|---|---|
| Network | Simulated (`NetworkSimulator`) | Real Sepolia + Base Sepolia testnets |
| Execution | Pure function, `execution.Execute(paymentID)` | Real signed transaction via Across testnet |
| Repeatable? | Yes — deterministic, side-effect-free | No — a broadcast transaction cannot be safely re-sent as a "new" attempt |
| Outcome source | In-process computation | Real on-chain state + Across's own status API |
| Funds | None | Real Sepolia/Base Sepolia testnet ETH/WETH (no monetary value) |

Phase 7 does **not** claim real mainnet fund movement anywhere, at any level
of this document.

## 2. Scope

Phase 7 is deliberately narrow:

- **One bridge provider**: Across.
- **One origin testnet**: Ethereum Sepolia (`11155111`).
- **One destination testnet**: Base Sepolia (`84532`).
- **One asset**: WETH.
- **One real execution path**: no provider abstraction, no chain
  abstraction beyond what's needed to keep signing/RPC code generically
  named (see §4) for future reuse — not to actually support a second chain
  now.

This narrowness is sufficient because Phase 7's hard problem —
nonce/signing/rebroadcast/reconciliation correctness around a
non-repeatable external side effect — does not get easier or harder by
adding a second chain, a second asset, or a second provider. Generalizing
now would spend effort proving something Phase 7 isn't about (bridge
diversity) instead of the thing it is about (crash-safety around a real
side effect). A future phase can generalize once this narrow path is
proven correct.

## 3. Architecture

```
go-api/internal/
  evm/
    wallet.go        # loads private key from env, exposes address, signs txs
    client.go         # ethclient wrapper; startup chain-ID validation
  bridge/
    across/
      client.go        # thin HTTP client for testnet.across.to/api
      quote.go          # GET /suggested-fees, typed response
      execute.go        # depositV3 construction + signing + broadcast
      status.go         # GET /deposit/status, receipt polling
  worker/
    executor.go        # NEW: claim -> nonce -> sign -> persist -> broadcast
    reconciler.go       # NEW: 4th goroutine, polls SUBMITTED executions
    processor.go        # existing (Phase 6) -- extended to dispatch by execution_mode
    recovery.go          # existing (Phase 6) -- unchanged, simulated-mode only
    publisher.go         # existing (Phase 6) -- unchanged
```

`evm/` is deliberately separate from `bridge/across/`: signing and RPC
chain-ID validation are not Across-specific concerns. Keeping them apart
means a hypothetical second provider would not need to duplicate
wallet/signing code — though no second provider is being built now, this
costs nothing today and avoids conflating two different responsibilities in
one file.

The worker remains the sole owner of execution. `POST /payments` persists
intent only (payment + hops + outbox event, exactly as Phase 5/6) — it never
calls Across, signs anything, or touches a chain. Nothing changes about the
HTTP request path's transaction boundary or latency characteristics.

## 4. Wallet / signing model

- **Wallet**: one dedicated test wallet for Phase 7. Its private key comes
  from the environment variable `TESTNET_WALLET_PRIVATE_KEY` (hex-encoded,
  no `0x` prefix required either way — the loader accepts both). It is
  never committed, never logged. The wallet's **address** (derived from the
  key at startup) is logged and is safe to log.
- **Signing library**: `github.com/ethereum/go-ethereum`'s `crypto` and
  `core/types` packages, accessed via `ethclient` for RPC — not the full
  go-ethereum node. This is the de facto standard for EVM transaction
  construction and signing in Go. "Smallest reasonable dependency" here
  means avoiding an unnecessary heavy dependency, not reinventing
  secp256k1 signing and RLP encoding by hand for a security-sensitive
  operation — the same reasoning that chose `kafka-go` over hand-rolling
  the Kafka wire protocol in Phase 6.
- **Testnet-only guardrail**: `BLOCKCHAIN_ENV` must be set to exactly
  `testnet` for any real-execution code path to be reachable at all. Its
  absence (or any other value) means the worker runs Phase 6
  simulated-only behavior unconditionally — real execution is opt-in at
  the process level, not just the request level.

## 5. RPC provider

- `ETHEREUM_SEPOLIA_RPC_URL` and `BASE_SEPOLIA_RPC_URL` — required
  environment variables, no default, no committed value. `ethclient.Dial`
  against each at worker startup.
- **Startup chain-ID validation**: immediately after dialing each RPC, the
  worker calls `eth_chainId` and compares the result against the expected
  constant (`11155111` for the Sepolia client, `84532` for the Base Sepolia
  client). Any mismatch is a fatal startup error. This is the actual
  guardrail against accidental mainnet use — an RPC URL's hostname proves
  nothing (a URL can be renamed, proxied, or simply wrong), but the chain
  itself cannot lie about its own chain ID over JSON-RPC.

## 6. Real amount handling

The existing `NUMERIC(38,18)` string amount (Phase 5) is reused unchanged
as the authoritative payment amount — no new representation is introduced
for the payment record itself.

Conversion to the token's integer base units for the on-chain call uses
`big.Int` exclusively:

1. Split the validated decimal string on `.` into integer and fractional
   parts (the string has already passed Phase 5's
   `^\d{1,20}(\.\d{1,18})?$` validation, so this split is always safe).
2. If the fractional part is **shorter** than the token's decimal count,
   right-pad it with zeros to exactly that length — this is always exact
   (padding with zeros never changes the represented value).
3. If the fractional part is **longer** than the token's decimal count,
   the amount cannot be represented exactly at that token's precision:
   **return a validation error immediately, before any concatenation or
   parsing.** The helper never truncates a fractional digit. For example,
   against a hypothetical 6-decimal token, `1.123456` is valid (exactly 6
   fractional digits) and `1.1234567` (7 digits) is rejected — silently
   dropping that seventh digit would move real value without the caller's
   knowledge, which this design treats as a correctness bug, not a
   rounding convenience.
4. Otherwise (fractional part already exactly the token's decimal count,
   including zero decimals with no `.` present), concatenate
   integer+fractional digits and parse as a single `big.Int` via
   `(*big.Int).SetString(..., 10)`.

No `float64` or `big.Float` appears anywhere in this path. Token decimals
come from a small static Go map (`map[string]uint8{"WETH": 18}`) rather
than an on-chain `decimals()` call — for one known, fixed testnet asset, a
static constant is simpler and avoids an extra RPC round-trip per
transaction; this is revisited if a second asset is ever added. WETH's 18
decimals match Phase 5's own `\.\d{1,18}` amount format exactly, so this
rejection path is unreachable on the one asset Phase 7 actually uses — the
helper's generic contract is still specified precisely because it is
written to be correct for any token decimal count, not just this one.

## 7. Schema

### `payments` (extended)

```sql
ALTER TABLE payments
    ADD COLUMN execution_mode TEXT NOT NULL DEFAULT 'simulated'
        CHECK (execution_mode IN ('simulated', 'testnet')),
    ADD COLUMN bridge_provider TEXT NULL;

ALTER TABLE payments
    DROP CONSTRAINT payments_status_check,
    ADD CONSTRAINT payments_status_check
        CHECK (status IN ('ROUTED', 'PROCESSING', 'SUBMITTED', 'COMPLETED', 'FAILED'));
```

`SUBMITTED` is a new, real status: "a transaction has been broadcast but not
yet confirmed as destination-filled." Simulated-mode payments never enter
`SUBMITTED` — their lifecycle stays `ROUTED -> PROCESSING ->
COMPLETED/FAILED` exactly as Phase 6. Testnet-mode payments follow
`ROUTED -> PROCESSING -> SUBMITTED -> COMPLETED/FAILED`. This directly
satisfies the requirement that a broadcast-but-unconfirmed transaction be
distinguishable from one that was never submitted — overloading
`PROCESSING` to mean both would hide exactly the boundary this phase exists
to make visible.

### `payment_executions` (new table, not more `payments` columns)

```sql
CREATE TABLE payment_executions (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id          UUID NOT NULL REFERENCES payments(id) ON DELETE CASCADE,
    bridge_provider     TEXT NOT NULL,
    origin_chain_id     BIGINT NOT NULL,
    destination_chain_id BIGINT NOT NULL,
    wallet_address      TEXT NOT NULL,
    nonce               BIGINT NOT NULL,
    unsigned_tx_params  JSONB NULL,
    signed_tx_hash      TEXT NULL,
    raw_signed_tx       BYTEA NULL,
    broadcast_at        TIMESTAMPTZ NULL,
    across_deposit_id   TEXT NULL,
    external_status     TEXT NOT NULL DEFAULT 'pending'
        CHECK (external_status IN ('pending', 'filled', 'expired', 'refunded', 'reverted')),
    confirmed_at        TIMESTAMPTZ NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (wallet_address, nonce),
    UNIQUE (payment_id)
);

CREATE INDEX payment_executions_pending_idx
    ON payment_executions (updated_at)
    WHERE external_status = 'pending';
```

**Why a separate table, not more `payments` columns**: `payment_executions`
represents everything about the *external side effect itself* — nonce,
signed bytes, broadcast timing, provider-specific deposit tracking — which
is conceptually distinct from the payment's own record and is exactly what
the reconciler (§15) needs to query in isolation
(`WHERE external_status = 'pending'`). Modeling it as `payments` columns
would mean a dozen nullable fields that only apply to one `execution_mode`,
and would conflate "the payment" with "one attempt to execute it
externally" — a distinction that matters the moment retries or multi-step
reconciliation are involved. The `UNIQUE (wallet_address, nonce)`
constraint makes a nonce collision a hard database-level impossibility,
mirroring the `UNIQUE(idempotency_key)` pattern from Phase 5.

**`UNIQUE (payment_id)` makes "at most one execution row per payment" a
database-enforced invariant, not just a design intention.** This is not
redundant with `UNIQUE (wallet_address, nonce)` — that constraint prevents
two payments from ever sharing a nonce, but says nothing about whether the
*same* payment could end up with two different execution attempts (e.g.
two different nonces) if some future code path tried to create a second
row for it. Phase 7 never intends that, and this constraint is what turns
"never intends to" into "cannot, even under a race" — it is the exact
mechanism §12 (crash point A) and §15 (reconciliation) rely on to arbitrate
a race between a still-alive-but-slow executor and the reconciler both
attempting to create the execution record for the same payment: whichever
`INSERT` commits first wins, and the other fails on this constraint and
backs off as a safe no-op, never producing a second execution identity for
one payment.

### `wallet_nonces` (new table)

```sql
CREATE TABLE wallet_nonces (
    wallet_address TEXT PRIMARY KEY,
    next_nonce     BIGINT NOT NULL
);
```

Atomic allocation via a single conditional `UPDATE`:

```sql
UPDATE wallet_nonces
SET next_nonce = next_nonce + 1
WHERE wallet_address = $1
RETURNING next_nonce - 1;
```

This is the same row-lock-as-coordination pattern as Phase 6's
`ClaimPayment` — no in-memory mutex, correct under multiple concurrently
running worker instances, because Postgres serializes concurrent `UPDATE`s
on the same row natively. The row is seeded exactly once, at first
worker startup for a given wallet, via
`INSERT ... ON CONFLICT (wallet_address) DO NOTHING` using the chain's own
`eth_getTransactionCount(wallet, "pending")` as the initial value.

**Postgres remains the sole authority for *allocating* nonce values, for
the life of the wallet — `wallet_nonces.next_nonce` is never overwritten
from a chain read, at startup or otherwise.** This is stronger than "never
re-queried": the chain **is** read periodically after the initial seed
(§8/§15 describe exactly when and why), but only to detect divergence and
decide *what to do about already-allocated nonces* — never to reset or
advance the counter itself. Overwriting the counter from
`eth_getTransactionCount` on every restart was considered and rejected:
the chain's "pending" nonce and Postgres's "next to allocate" counter
answer different questions (one reflects what the chain has already seen
broadcast; the other reflects what ChainRoute has already committed to
allocating, including nonces allocated but not yet broadcast), and
collapsing them would silently reallocate a nonce still owned by an
in-flight `payment_executions` row, producing exactly the double-spend
`UNIQUE (wallet_address, nonce)` exists to prevent. §8 (recovery of an
already-allocated nonce) and §15 (reconciliation, including nonce-gap
handling) describe the actual reconciliation rule in full.

## 8. The central correctness problem, and what guarantee is actually
## possible

Kafka's at-least-once delivery and the `ROUTED`-guarded claim from Phase 6
solve "did we invoke the executor twice" — they do **not** solve "did we
broadcast a transaction twice." Those are different problems, and this
phase does not pretend otherwise.

The actual, achievable guarantee: **once a nonce is durably allocated and a
transaction is signed and persisted, that exact transaction — never a new
one — is what gets (re)broadcast on any restart.** Concretely:

1. Nonce allocation (§7's `wallet_nonces` `UPDATE`) and the creation of the
   `payment_executions` row **happen together, in one Postgres
   transaction**: the row is only ever inserted already carrying its
   allocated `nonce` (hence `nonce` is `NOT NULL` in the schema) — there is
   no durable intermediate state where a `payment_executions` row exists
   without a nonce, and (because `UNIQUE (payment_id)` lives in the same
   table as `nonce`) no way for this transaction to partially succeed: if
   the `INSERT` fails, the `wallet_nonces` `UPDATE` in the same transaction
   rolls back with it, so a lost race never burns an allocated nonce. This
   is what makes the crash-point table in §12 exact: a crash before this
   transaction commits leaves no execution row at all (point A, whose
   recovery mechanism is specified in §15); any crash after it leaves a
   row whose `nonce` is already fixed and never re-allocated (point B
   onward). §15 also specifies how a nonce that *was* durably allocated but
   never resolves on-chain is handled — that is a distinct problem from
   crash recovery and is not solved by this transactional guarantee alone.
2. The transaction is constructed and signed; the raw signed bytes and
   their hash (computable deterministically from the bytes themselves,
   before any network call) are persisted to `raw_signed_tx` and
   `signed_tx_hash` **before** any broadcast attempt.
3. Only after that commit does the worker attempt `eth_sendRawTransaction`.
4. On **any** failure, crash, or ambiguous outcome at or after step 3, the
   recovery path is: query the chain directly for `signed_tx_hash` via
   `eth_getTransactionByHash`. If found, the transaction already exists —
   proceed to reconciliation (§15), no rebroadcast needed. If not found,
   rebroadcast the **identical persisted bytes** — never construct or sign
   a new transaction for this execution row. Rebroadcasting an
   already-mined transaction is a safe, node-level no-op; nodes reject it
   without side effects.

What is explicitly **not** guaranteed: exactly-once *submission* to the
network in the face of a truly ambiguous RPC response (the transaction may
have been accepted by the node before the connection dropped, with no way
to know without checking the chain). What Phase 7 guarantees instead is
that this ambiguity can always be resolved safely after the fact, without
ever risking a second, distinct transaction for the same payment.

## 9. Transaction identity / deduplication

Fully covered by §7 (schema) and §8 (algorithm) above. Concurrency across
multiple worker instances submitting from the same wallet is handled
entirely by `wallet_nonces`' atomic conditional `UPDATE` — no in-memory
mutex is the correctness boundary anywhere in this design, matching the
same principle Phase 6 established for payment state transitions.

## 10. Execution state machine

`ROUTED -> PROCESSING -> SUBMITTED -> COMPLETED` or
`SUBMITTED -> FAILED` (testnet mode). Simulated mode is unchanged:
`ROUTED -> PROCESSING -> COMPLETED/FAILED`, never touching `SUBMITTED`.
`payment_executions.external_status` is a second, finer-grained state
machine living entirely within the `SUBMITTED` window:
`pending -> filled | expired | refunded | reverted`, driving the
`payments.status` transition to `COMPLETED` (on `filled`) or `FAILED` (on
`expired`/`refunded`/`reverted`) via the reconciler (§15).

## 11. Kafka interaction

The transactional outbox, at-least-once Kafka delivery, and manual offset
commits from Phase 6 are unchanged. A redelivered `PAYMENT_ROUTED` event's
behavior depends on the payment's current status, exactly via the existing
`ClaimPayment` guard (`WHERE status = 'ROUTED'`):

| Current status | Claim result | Behavior |
|---|---|---|
| `ROUTED` | succeeds | Normal processing begins (first or a genuinely-not-yet-started delivery) |
| `PROCESSING`, execution not yet submitted | fails (status ≠ ROUTED) | Safe no-op, offset commits. Kafka redelivery never progresses a `PROCESSING` payment (by design — see §11 rationale below); the in-flight executor, or failing that the reconciler's stale-`PROCESSING` recovery (§15), is what progresses this payment |
| `SUBMITTED` | fails | Safe no-op, offset commits. Only the reconciler acts on `SUBMITTED` payments |
| `COMPLETED` / `FAILED` | fails | Safe no-op, offset commits |

No new Kafka-level idempotency mechanism is needed — the existing guard,
now covering one more intermediate status, remains sufficient for
distinguishing "should this delivery start a new attempt" (no, once past
`ROUTED`). It deliberately does **not** by itself guarantee that some
attempt is always still in flight — a redelivery is a safe no-op precisely
because it assumes *something else* is progressing the payment, which is
true only as long as the original executor is actually alive. §15 closes
this gap for the case where that assumption turns out to be false (the
executor crashed and nothing else picks the payment back up on its own).

## 12. Submission crash-point analysis (A-K)

| Point | Durable PostgreSQL state | Blockchain state | Kafka state | Restart behavior | New tx risk |
|---|---|---|---|---|---|
| A. Before nonce allocation | `PROCESSING`, no execution row | none | offset uncommitted | **Not** self-resolved by Kafka redelivery (§11 — a redelivery sees `PROCESSING` and no-ops) or by the original executor if it does not restart. Recovered exclusively by the reconciler's stale-`PROCESSING`-with-no-execution-row sweep (§15): once `payments.updated_at` exceeds the staleness threshold, the reconciler attempts the same atomic nonce-allocation + row-creation transaction the executor would have run; `UNIQUE (payment_id)` (§7) arbitrates a race against a still-alive-but-slow executor, so exactly one of them creates the row and the loser no-ops. The winner then proceeds exactly as point B | No (§15: the race is transactional, so a lost race rolls back its nonce allocation too) |
| B. After nonce allocated, before tx construction | execution row with `nonce` set, no signed bytes | none | offset uncommitted | Reconstruct and sign using the **already-allocated** nonce (never re-allocate) | No |
| C. Signed tx persisted, before broadcast | `raw_signed_tx`, `signed_tx_hash` set | none (never sent) | offset uncommitted | Broadcast the persisted bytes | No |
| D. Crash during RPC broadcast call | signed tx persisted | ambiguous (may or may not have been received) | offset uncommitted | Query chain for `signed_tx_hash`; found → reconcile; not found → rebroadcast identical bytes | No |
| E. Broadcast succeeds, response lost | signed tx persisted | tx exists on-chain | offset uncommitted | Same as D — hash lookup finds it | No |
| F. Broadcast succeeds, crash before any DB update | signed tx persisted (from step C), `broadcast_at` possibly unset | tx exists | offset uncommitted | Same as D | No |
| G. `signed_tx_hash`/status persisted, offset not committed | `SUBMITTED`, execution row complete | tx exists (pending or mined) | Kafka **will** redeliver | Redelivered claim fails (§11 table) — safe no-op, offset commits on this pass | No |
| H. Transaction pending a long time | `SUBMITTED`, `external_status='pending'` | pending | n/a | Reconciler keeps polling on its interval; no forced action taken | No |
| I. Transaction reverts | `SUBMITTED` until reconciler observes it | reverted (`receipt.Status == 0`) | n/a | Reconciler sets `external_status='reverted'`, `payments.status='FAILED'` — a definitive terminal signal | No |
| J. Destination fill delayed | `SUBMITTED`, `external_status='pending'` | origin confirmed, destination not yet filled | n/a | Reconciler keeps polling `/deposit/status`; testnet fills average ~1 minute, so this is expected, not exceptional | No |
| K. Origin confirms, destination ultimately fails/expires | `SUBMITTED` until reconciler observes it | origin confirmed, destination never fills | n/a | Across reports `expired`/`refunded` → `external_status` set accordingly → `payments.status='FAILED'`. A bare polling timeout or transient API failure alone does **not** produce `FAILED` (see §13) | No |

Every row concludes "no new transaction risk" — this holds uniformly
because every restart path re-derives from durably persisted nonce/signed
bytes rather than re-signing, the same way Phase 6's uniform "no duplicate
persisted transition" guarantee held across every crash point there.

**This table describes recovery of ChainRoute's own crashes, which is
distinct from a nonce that never resolves on-chain at all** (e.g. a
transaction dropped from the mempool, or a nonce consumed by activity
outside ChainRoute despite the dedicated-wallet invariant in §18). Every
row above still applies — the durable nonce/signed-bytes are always
correctly recovered — but "recovered" only means "ChainRoute keeps
rebroadcasting the correct, identical bytes for that nonce"; it does not
mean the transaction is guaranteed to ever mine. §15 documents the
resulting cross-payment impact (a stuck low nonce blocks every higher
nonce from the same wallet, by ordinary EVM nonce-ordering rules) and the
reconciliation priority rule Phase 7 applies to it.

## 13. FAILED semantics

`FAILED` is reserved for **definitive, terminal** signals only:

- The origin transaction's receipt shows `status == 0` (an actual on-chain
  revert).
- Across's `/deposit/status` returns `expired` or `refunded` — the
  protocol's own terminal-failure vocabulary.

`FAILED` is explicitly **not** triggered by:

- An RPC call timing out.
- A broadcast response being lost (ambiguous, not failed — see §8).
- A single failed poll against `/deposit/status` or the RPC endpoint.
- `/deposit/status` remaining `pending` for longer than expected (testnet
  fills can reasonably take a few minutes; this is monitored, not treated
  as failure).

Any of the "not failed" cases above simply leave the execution in its
current durable state; the reconciler retries on its next interval. This
distinction — uncertain state is not the same as failure — is the direct
answer to a system that must not manufacture false negatives out of
infrastructure flakiness.

## 14. Recovery of an uncertain broadcast

Already specified precisely in §8, step 4: on any ambiguous outcome, the
worker's **first** action is always a read (`eth_getTransactionByHash` for
the precomputed `signed_tx_hash`) — never a write (never construct or sign
a fresh transaction). Only if that read confirms the transaction does not
exist does the worker rebroadcast, and even then it rebroadcasts the
**exact same persisted signed bytes**, not a newly-signed one. This
ordering — check before you might duplicate — is the core discipline that
makes Phase 7 safe despite operating on a non-repeatable side effect.

## 15. Reconciliation

A **fourth goroutine in the existing `cmd/worker` binary** — not the
existing `Recovery` type, not a new service. `Recovery` (Phase 6) sweeps
stale `PROCESSING` payments using only Postgres and the pure
`execution.Execute` function; reconciling a real external transaction
requires a genuinely different data source (chain RPC + the Across status
API) and answers a different question ("what actually happened
externally," not "did anyone finish this yet"). Mixing the two
responsibilities into one type would overload `Recovery` with two
different data-access patterns and two different failure modes. A new
`Reconciler` type, running as a fourth goroutine in the same binary
(avoiding a new microservice, matching the existing three-goroutine
pattern from Phase 6), is the smallest coherent design:

```sql
SELECT * FROM payment_executions
WHERE (broadcast_at IS NOT NULL AND confirmed_at IS NULL)
   OR (broadcast_at IS NULL AND updated_at < now() - make_interval(secs => $1));
```

The two clauses answer two different questions, and only one of them needs
a staleness guard. **Already broadcast, not yet confirmed**
(`broadcast_at IS NOT NULL AND confirmed_at IS NULL`) is checked on every
tick with no staleness condition — querying the chain or Across's API for
a transaction that was broadcast moments ago by a still-live executor is
always safe and informative (worst case, it learns "still pending," which
it already knew). **Not yet broadcast** (`broadcast_at IS NULL`) is the
case that needs a staleness threshold (`$1`, the same
`RECONCILE_STALENESS_SECONDS`-style parameter as `Recovery`'s own
staleness config): `external_status` defaults to `'pending'` the instant a
`payment_executions` row is inserted — including rows a live executor
created a fraction of a second ago and is still actively working through
nonce allocation, signing, and broadcast. Without the `updated_at`
staleness guard on this clause, the reconciler would immediately treat
every brand-new, actively-in-progress execution as a candidate, exactly
the "still alive, just slow" scenario Phase 6's `Recovery` already had to
account for with its own staleness threshold. The fix is the same one
Phase 6 used: only treat a not-yet-broadcast row as a reconciliation
candidate once it has sat without progress (`updated_at` unchanged) longer
than a configured threshold.

For each **already-broadcast, not-yet-confirmed** candidate (`broadcast_at
IS NOT NULL AND confirmed_at IS NULL`): check the origin chain receipt
(mined? reverted?) and Across's `/deposit/status` (filled? expired?
refunded?), and update `payment_executions`/`payments` accordingly.

For each **not-yet-broadcast, stale** candidate (`broadcast_at IS NULL AND
updated_at < staleness`): the reconciler must first re-check
`broadcast_at` and `signed_tx_hash`, since its own `UPDATE` may simply not
have landed yet when the reconciler looked (the original executor is still
alive, just slow) — if those are now populated, treat it as
already-broadcast and follow the same on-chain-hash-lookup path as §8/§14
rather than assuming nothing was ever signed. Otherwise, drive the row
forward using the already-allocated `nonce` (never re-allocated): if
`signed_tx_hash` is still unset, construct and sign the transaction and
persist `raw_signed_tx`/`signed_tx_hash` (crash point B's recovery); then
broadcast the persisted bytes (crash point C's recovery, or a continuation
of the same action for a row that just got signed). Any ambiguous outcome
from that broadcast attempt is handled exactly per §8 step 4/§14. This is
what makes confirmation not depend on the original worker process instance
staying alive until the bridge finishes — any worker instance's reconciler
goroutine, including one from a freshly restarted process, will pick up
exactly where the system left off, purely from durable state.

### Recovering a stale `PROCESSING` payment with no execution row

The two clauses above both query `payment_executions`, so neither can see
a payment stuck at crash point A (§12): `payments.status = 'PROCESSING'`
with no `payment_executions` row at all. Nothing else in this design
revisits such a payment — Kafka redelivery no-ops once status leaves
`ROUTED` (§11), and Phase 6's `Recovery` sweep is simulated-mode only — so
without an explicit mechanism this payment is permanently stranded. The
reconciler runs a third, separate candidate query for exactly this case:

```sql
SELECT id, execution_mode, bridge_provider /* + route/asset fields needed
       to construct the execution row and the depositV3 call */
FROM payments
WHERE execution_mode = 'testnet'
  AND status = 'PROCESSING'
  AND updated_at < now() - make_interval(secs => $1)
  AND NOT EXISTS (
      SELECT 1 FROM payment_executions
      WHERE payment_executions.payment_id = payments.id
  );
```

This reuses the same `$1` (`RECONCILE_STALENESS_SECONDS`) staleness
threshold as the not-yet-broadcast clause above, for the same reason: a
payment that entered `PROCESSING` a fraction of a second ago is
indistinguishable from one whose executor crashed before allocating a
nonce, so a staleness guard is required to avoid racing a live, merely-slow
executor. Introducing a second threshold for what is conceptually the same
"how long can a testnet-mode payment sit with no forward progress before
we treat it as abandoned" question would add a knob without adding safety.

For each candidate, the reconciler attempts **the same atomic transaction
the executor itself would run** (§8 step 1): allocate the next nonce from
`wallet_nonces` and insert the `payment_executions` row, in one Postgres
transaction:

```sql
BEGIN;
UPDATE wallet_nonces
SET next_nonce = next_nonce + 1
WHERE wallet_address = $1
RETURNING next_nonce - 1;

INSERT INTO payment_executions
    (payment_id, bridge_provider, origin_chain_id, destination_chain_id,
     wallet_address, nonce)
VALUES ($2, $3, $4, $5, $1, $6);
COMMIT;
```

**Race analysis.** Two actors can reach this transaction for the same
payment: the original executor, still alive but slow (has not yet crashed,
just hasn't committed this transaction yet), and the reconciler, having
independently decided the payment looks stale. Both attempt the same
`INSERT`. `UNIQUE (payment_id)` (§7) guarantees at most one commits: the
loser's `INSERT` fails with a unique-violation, and because the nonce
`UPDATE` and the `INSERT` are in the same transaction, the loser's entire
transaction rolls back — **the allocated nonce is never persisted for the
loser**, so this specific race never burns a nonce or creates a second
execution identity. The loser treats the unique-violation as an expected,
safe outcome (not an error to retry or alert on) and moves to its next
candidate; the payment now has exactly one `payment_executions` row,
created by whichever actor actually won, and that row is indistinguishable
from a normal crash-point-B row (nonce set, no signed bytes yet) — the
winner (or, if the winner was the executor, the *next* reconciler tick,
since the row now exists and will surface under the not-yet-broadcast
clause once/if it goes stale) drives it forward exactly as described
above.

### Nonce reconciliation: what happens when an allocated nonce doesn't resolve

Everything above guarantees that ChainRoute never *abandons* an allocated
nonce — every crash path eventually gets its already-allocated nonce
signed and (re)broadcast. It does not guarantee that nonce ever mines.
EVM nonces are strictly sequential per account: if the transaction at
nonce 10 never mines (dropped from the mempool, underpriced, or a nonce
consumed by activity outside ChainRoute despite the dedicated-wallet
invariant in §18), nonce 11 and every higher nonce from the same wallet
are blocked from mining regardless of their own validity — no amount of
retrying nonce 11 changes that.

Phase 7's strategy is deliberately the smallest one that stays correct
without new infrastructure:

1. **`wallet_nonces.next_nonce` is never overwritten from a chain read, at
   any point after the initial seed** (§7). A chain re-query cannot safely
   tell ChainRoute "the next nonce to allocate is N," because the chain's
   pending-nonce count reflects only what it has seen *broadcast* — it
   knows nothing about a nonce ChainRoute has already durably allocated in
   `wallet_nonces`/`payment_executions` but not yet broadcast (crash point
   B). Trusting the chain here would let it hand out an already-allocated
   nonce a second time, which is exactly the double-spend
   `UNIQUE (wallet_address, nonce)` exists to prevent. This directly
   answers "do not simply overwrite the DB counter with the chain pending
   nonce": Phase 7 doesn't, ever.
2. **Reconciliation effort for already-broadcast, unconfirmed transactions
   is strictly prioritized by nonce, lowest first, per wallet.** Among the
   "already-broadcast, not-yet-confirmed" candidates (the first clause
   above) for a given `wallet_address`, the reconciler only actively
   re-checks the **lowest** unconfirmed nonce's chain receipt / Across
   status on a given tick; higher nonces for that wallet remain tracked
   (they still appear in the candidate set) but are not separately
   investigated until the lowest one resolves. This is not a correctness
   requirement — broadcasting or checking nonce 11 before nonce 10
   resolves cannot corrupt any state, since the chain itself simply won't
   mine 11 first — it is a resource-focusing rule: it guarantees
   ChainRoute's own polling budget (RPC calls, Across API calls) is never
   spent chasing transactions that structurally cannot progress yet, and
   that the moment the true blocker (the lowest nonce) resolves, the very
   next tick re-evaluates every nonce above it. This scoping matters: it
   applies only to already-broadcast candidates, not to the not-yet-
   broadcast clause or the stale-`PROCESSING` clause above, since signing
   or broadcasting a higher nonce before a lower one is broadcast is not
   incorrect (the chain will simply queue it) and gating it would add
   complexity with no safety benefit.
3. **A read-only divergence check, run once per reconciler tick per active
   wallet, never a write.** The reconciler compares
   `eth_getTransactionCount(wallet, "pending")` against the lowest
   unconfirmed nonce known to `payment_executions` for that wallet. A
   match or a chain value *ahead* of ChainRoute's lowest unconfirmed nonce
   is expected and unremarkable (the chain mining ChainRoute's own
   transactions naturally advances its pending count). A chain value
   *behind* what ChainRoute expects, or one that has stopped advancing
   despite ChainRoute repeatedly rebroadcasting, is logged as a distinct,
   alertable condition — this is diagnostic only; per point 1, it never
   feeds back into `wallet_nonces`.
4. **A stuck lowest nonce that is genuinely unrecoverable (permanently
   invalid, or consumed by activity outside ChainRoute) is a disclosed
   Phase 7 limitation, not a silently-hidden one.** Because ChainRoute
   always rebroadcasts the identical persisted bytes for a nonce that
   hasn't confirmed (§8/§14), a transaction merely dropped from a node's
   mempool typically self-heals via ordinary rebroadcast without any new
   mechanism. A transaction that can genuinely never mine (e.g. the nonce
   was consumed by an out-of-band use of the wallet, violating the
   dedicated-wallet invariant) has no automated resolution in this
   design — no fee-bumping, no cancellation transaction, no nonce-skipping.
   This is added to §30 as an explicit non-goal: resolving it is an
   operator action (send a manual replacement/cancellation transaction at
   the stuck nonce), consistent with Phase 7 already excluding
   production-grade key/wallet management.

This directly addresses the six scenarios raised: a worker crash after
allocation, an allocated-but-unsigned execution, and a signed-but-
unbroadcast execution are all durable-recovery cases already covered
above (points B/C); external wallet use and dropped transactions are
covered by points 3 and 4; and the chain's pending nonce moving
independently of the DB counter is expected behavior under point 1, not a
divergence to correct.

No new goroutine, service, distributed lock, or Redis dependency is
introduced by any of this — it is entirely additional query and ordering
logic inside the existing reconciler goroutine, operating only through
Postgres transactions and read-only chain queries.

## 16. Across status — precise definition of COMPLETED

`COMPLETED` is set **only** when Across's `/deposit/status` returns
`filled` — meaning a `FilledRelay` event has actually fired on the
**destination** chain. This is deliberately stricter than "origin
transaction confirmed" (which only proves the deposit was initiated, not
that funds arrived) and stricter than "Across recognized the deposit"
(an intermediate indexing state, not completion). Choosing the strictest
available, protocol-native signal is what makes `COMPLETED` mean what a
user would actually expect it to mean: the bridge transfer is done.

## 17. Testnet guardrails

- `BLOCKCHAIN_ENV=testnet` required for any real-execution code path
  (§4).
- Startup chain-ID validation against `eth_chainId` for both configured
  RPCs, refusing to start on any mismatch (§5) — not an RPC-hostname
  check, which proves nothing.
- A configurable `MAX_TESTNET_AMOUNT_WEI` ceiling, rejecting any
  `execution_mode="testnet"` payment request above it — defense in depth
  against a configuration mistake moving more testnet value than
  intended, even though testnet value is not real money.
- No arbitrary calldata is ever accepted from an HTTP client — the
  worker constructs the `depositV3` call entirely server-side from
  validated internal inputs.

## 18. Funding / developer setup

To run Phase 7 locally, a developer needs:

- A dedicated test wallet (a fresh keypair generated for this purpose,
  never reused for anything of value) — its private key set via
  `TESTNET_WALLET_PRIVATE_KEY`.
- Sepolia ETH for gas, obtained from a public Sepolia faucet.
- WETH on Sepolia (wrap Sepolia ETH via the WETH contract's `deposit()`
  function) if the confirmed `depositV3` behavior (§ Across research,
  open item) requires pre-wrapped input.
- `ETHEREUM_SEPOLIA_RPC_URL` / `BASE_SEPOLIA_RPC_URL` — any RPC provider
  of the developer's choosing (a committed `.env.example` holds only
  placeholders, never a real key/URL).
- Local Postgres, Redpanda/Kafka, and the C++ routing service — all
  exactly as required by Phases 1-6, unchanged.

No faucet keys or wallet secrets are ever committed to the repository.

## 19. C++ router relationship

The simulator is not modified, and Phase 7 does not claim the simulated
multi-bridge graph "selected" a real Across route — that would be
dishonest, since the graph's bridge names and fee/liquidity figures are
simulator output, not real Across data. Of the three options considered,
Phase 7 uses **Option C: Across is a separate real-execution mode; the
simulator continues to serve simulated-mode routing unchanged.** For
`execution_mode="testnet"`, the C++ router is still called (preserving the
existing hops/fee metadata shape for the payment record, kept purely as
informational context) — but the API layer additionally validates the
request against the one real, hardcoded route (Sepolia WETH -> Base
Sepolia WETH) and rejects any other combination with 400 when
`execution_mode="testnet"` is requested. No real Across quote data is
ingested into the C++ Dijkstra graph in this phase (Option: real quote
ingestion, considered and explicitly declined) — routing simulation and
real testnet execution remain two separate, unintegrated systems. This is
the honest tradeoff: Phase 7 proves execution correctness, not routing
intelligence: a future phase could ingest real Across quotes into the
router's edge data as a distinct piece of work, but doing so now would
conflate two unrelated efforts and risk under-designing both.

## 20. API changes

`POST /payments` gains one new optional field, `execution_mode`, defaulting
to `"simulated"` — every existing caller's behavior is entirely unchanged.
`execution_mode: "testnet"` is only accepted for the one hardcoded
route/asset (§19); any other combination with `execution_mode: "testnet"`
returns 400. `GET /payments/{id}` gains `execution_mode`, `bridge_provider`
(both read directly from the `payments` columns added in §7),
`external_tx_hash`, and `submitted_at` — the latter two are read from the
associated `payment_executions` row (`signed_tx_hash` and `broadcast_at`
respectively; the API field names describe the concept from a client's
perspective, the schema column names describe the mechanism from the
worker's perspective — the mapping is 1:1). All four are nullable JSON
fields, populated only when they actually exist for that payment (a
simulated-mode payment, or a testnet-mode payment that hasn't reached
`SUBMITTED` yet, shows `null` for `external_tx_hash`/`submitted_at`).

## 21. Security

- Private key: environment-only, never logged, never included in any error
  message or panic output.
- RPC URLs / any provider credentials embedded in them: environment-only,
  never logged.
- No mainnet support anywhere in Phase 7's code paths.
- No arbitrary calldata accepted from the HTTP client (§17).
- The Across quote response is validated (chain IDs match the request,
  token addresses match the expected WETH addresses, fee figures are
  within a sane bound) before being used to construct any transaction —
  an unvalidated external response never flows directly into a signing
  call.
- Chain ID validated both at worker startup and per-transaction
  construction (defense in depth, not just a one-time check).
- `MAX_TESTNET_AMOUNT_WEI` guardrail (§17).

## 22. Unit tests (no network access required)

- Exact decimal string -> `big.Int` base-units conversion, including edge
  cases (max digits, single fractional digit, trailing zeros, and a
  fractional part exactly matching the token's decimal count).
- Amount-conversion **rejection**: a decimal amount with more fractional
  digits than the token's configured decimal precision returns a
  validation error rather than a truncated value — e.g., against a
  hypothetical 6-decimal token, `1.123456` succeeds and `1.1234567` (7
  fractional digits) is rejected. This proves rejection, not truncation:
  a test asserting only that the function returns *some* error is
  insufficient — it must assert no `big.Int` value is produced and that a
  same-magnitude, exactly-representable amount (`1.123456`) succeeds,
  ruling out an overly broad validator that rejects everything.
- Across `/suggested-fees` and `/deposit/status` response parsing against
  fixed, hand-written JSON fixtures.
- `payments`/`payment_executions` state-transition legality (valid and
  invalid transitions).
- `depositV3` transaction construction from known inputs, against a fixed
  expected calldata/parameter fixture.
- Chain-ID validation logic (accepts the two expected testnet IDs, rejects
  everything else including real mainnet chain IDs).
- Deterministic `signed_tx_hash` computation from fixed signed transaction
  bytes.
- Kafka redelivery/no-op behavior for each payment status (§11 table),
  against fakes.
- Ambiguous-broadcast decision logic (found on-chain -> reconcile; not
  found -> rebroadcast identical bytes), against a fake chain client.

## 23. PostgreSQL integration tests (existing `integration` build tag)

- `payment_executions` row lifecycle: created with a nonce, updated with
  signed-tx fields, updated with broadcast/confirmation fields.
- Nonce allocation under concurrency: N goroutines racing
  `wallet_nonces`' conditional `UPDATE` for the same wallet, asserting
  every allocated nonce is unique and sequential (mirroring Phase 6's
  `ClaimPayment` concurrent test pattern).
- Legal `payments.status` transitions for testnet mode
  (`ROUTED->PROCESSING->SUBMITTED->COMPLETED/FAILED`), and that
  `SUBMITTED` is unreachable for simulated-mode payments.
- Crash/restart recovery: an execution row with signed bytes but no
  `broadcast_at` is correctly identified as needing a rebroadcast check
  on restart.
- Reconciliation candidate selection: the exact `SELECT`s from §15 return
  the right rows under a mix of `pending`/`filled`/`expired` executions,
  and under a mix of testnet-mode payments with and without an existing
  `payment_executions` row.
- Stale-`PROCESSING`-with-no-execution-row race: two concurrent goroutines
  (modeling the original executor and the reconciler) both attempt the
  nonce-allocation + `payment_executions`-creation transaction for the
  same payment; assert exactly one `payment_executions` row exists
  afterward, exactly one nonce was consumed from `wallet_nonces` (the
  loser's nonce allocation rolled back, not merely unused), and the
  losing side's unique-violation is handled as a safe no-op rather than
  a returned error.
- Nonce-priority ordering: given multiple unconfirmed
  (`broadcast_at IS NOT NULL AND confirmed_at IS NULL`) executions for the
  same wallet at different nonces, assert the reconciler's candidate
  selection for active re-checking surfaces only the lowest nonce, and
  that once the lowest nonce's row is confirmed, the next-lowest becomes
  the active candidate.

## 24. Real testnet integration tests (separately gated)

A new build tag, `testnet_integration`, **and** an explicit
`RUN_TESTNET_TESTS=1` environment variable are **both** required before
any test in this suite runs — plain `go test ./...`, and even
`go test -tags=integration ./...`, never touch the network or spend
testnet funds. This suite covers one real quote -> sign -> broadcast ->
reconcile cycle against the actual Sepolia/Base Sepolia testnets and the
real Across testnet API, using a funded test wallet the developer
provides via the same environment variables as production use.

## 25. E2E test

The existing deterministic local E2E suite (`scripts/e2e_test.sh`) is
**not** modified to depend on any real network — it remains fully local
and deterministic, exactly as Phase 6 left it. A **separate**,
explicitly-invoked real-testnet smoke script is added
(`scripts/e2e_testnet_test.sh` or similar), gated the same way as §24, that
exercises: `POST /payments` with `execution_mode="testnet"` -> a real
Across quote -> payment/outbox persisted -> Kafka -> the worker
constructs/signs/sends a real transaction -> `signed_tx_hash` persisted ->
the reconciler observes real destination completion ->
`GET /payments/{id}` reports `COMPLETED` with a real `external_tx_hash`.
This script is never part of the default CI/local test run.

## 26. Observability

Structured logging sufficient to follow one payment end-to-end includes:
`payment_id`, the Kafka event's identity, `execution_id`
(`payment_executions.id`), `bridge_provider`, `tx_hash`, and every state
transition with its old/new status. The raw signed transaction bytes and
the private key are never logged, under any log level, by default — there
is no configuration flag in this design that enables logging raw signed
transaction bytes, since no concrete debugging need justifies that risk
today.

## 27. Migration strategy

One new migration file, `go-api/migrations/0004_across_testnet_execution.sql`,
containing the `payments` `ALTER TABLE`, the new `payment_executions` and
`wallet_nonces` tables (§7). Migrations `0001`-`0003` are not modified.

## 28. Preserving prior phases

All Phase 1-6 tests (router, cpp-routing-service, Go unit/integration/
Kafka-integration, E2E) must continue to pass unmodified. `router/` and
`cpp-routing-service/` are not touched — nothing in this design requires a
routing-algorithm or C++ change (§19). The protobuf contract
(`proto/chainroute/v1/routing.proto`) is not touched — Phase 7's new
surface area is entirely in `go-api/`. Simulated mode remains the default
and fully functional execution path for all existing deterministic local
tests.

## 29. Final guarantees

Phase 7 makes no single "exactly-once" claim — the eight properties below
are independently true, independently limited, and must not be collapsed
into one another. In particular: **multiple broadcasts of the identical
signed transaction bytes are allowed and expected under ambiguous
failures. The desired guarantee is that recovery never intentionally
creates a second distinct transaction identity for the same payment.**
Exactly-once *network submission* is explicitly **not** claimed anywhere
below.

- **Payment creation.** Exactly-once durable creation per idempotency key,
  unchanged from Phase 5: `CreateOrGetPayment`'s `ON CONFLICT` behavior is
  untouched by Phase 7.
- **Kafka delivery.** At-least-once, unchanged from Phase 6. A
  `PAYMENT_ROUTED` event may be redelivered any number of times; every
  redelivery past the first successful `ClaimPayment` is a safe no-op
  (§11).
- **External execution identity.** At most one `payment_executions` row
  ever exists per payment, enforced by `UNIQUE (payment_id)` (§7) — not
  merely intended, but database-enforced even under a race between the
  original executor and the reconciler (§12 point A, §15). This is the
  guarantee that "recovery never intentionally creates a second distinct
  transaction identity" cashes out to concretely: one `payment_executions`
  row means one nonce, one set of signed bytes, one transaction identity,
  for the life of that payment.
- **Nonce allocation.** Exactly-once allocation of each nonce value to at
  most one `payment_executions` row (`UNIQUE (wallet_address, nonce)`,
  §7), and every allocated nonce is eventually retried until signed and
  broadcast — no allocated nonce is ever silently abandoned by ChainRoute
  itself (§8, §15). This does **not** guarantee the resulting transaction
  ever mines (§12's post-table note, §15's nonce-reconciliation strategy).
- **Transaction broadcast attempts.** At-least-once, by design — any
  ambiguous outcome (crash, timeout, lost response) is resolved by
  checking the chain first and, only if not found, rebroadcasting (§8
  step 4, §14). A broadcast may therefore be attempted multiple times for
  the same payment; this is expected, not an error condition.
- **Distinct signed transactions per payment.** Exactly one: a
  `payment_executions` row is signed once (`raw_signed_tx`/
  `signed_tx_hash` persisted before any broadcast), and every subsequent
  broadcast attempt for that row reuses those identical persisted bytes —
  never a re-sign (§8 step 4, §14, §15).
- **Terminal state persistence.** `payments.status` becomes
  `COMPLETED`/`FAILED` at most once per payment (the existing conditional
  `UPDATE ... WHERE status = $expected` pattern from Phase 5/6 applies
  unchanged), and only in response to a definitive, observed signal, never
  an assumption or a timeout (§13, §16).
- **Destination fill observation.** `COMPLETED` reflects an actually
  observed `filled` result from Across's `/deposit/status` — a real
  `FilledRelay` event on the destination chain — not an inference from
  origin-chain confirmation alone (§16). Fill latency itself is not
  contractually bounded (testnet fills are typically ~1 minute).

**Also not claimed, unchanged in substance from the prior draft:**
Mainnet or production fund-movement safety; real-money production
readiness of any kind; automated recovery from a genuinely unrecoverable
stuck nonce (§15, §30).

## 30. Non-goals

Mainnet; real user funds; multi-wallet custody; a wallet UI; generalized
multi-bridge/multi-provider aggregation; smart-contract development beyond
what Across's existing contracts require ChainRoute to call; Redis; Kafka
transactions; distributed locks beyond PostgreSQL's own row-level locking;
a frontend; production-grade key management (HSM/KMS) of any kind;
automated recovery from a nonce that is genuinely unrecoverable on-chain
(gas-price bumping / replace-by-fee, cancellation transactions, or
nonce-skipping) — §15 documents this as a disclosed limitation requiring
manual operator intervention, not a silent gap.

## 31. Configuration variables (consolidated)

Referenced individually in §4, §5, and §17 above; collected here for a
single reference point.

| Variable | Purpose | Default |
|---|---|---|
| `BLOCKCHAIN_ENV` | Must equal `testnet` to enable any real-execution code path | none, required for real execution |
| `TESTNET_WALLET_PRIVATE_KEY` | Signing key for the dedicated test wallet | none, required for real execution |
| `ETHEREUM_SEPOLIA_RPC_URL` | RPC endpoint for Sepolia | none, required for real execution |
| `BASE_SEPOLIA_RPC_URL` | RPC endpoint for Base Sepolia | none, required for real execution |
| `ACROSS_TESTNET_API_URL` | Across testnet API base | `https://testnet.across.to/api` |
| `ACROSS_API_KEY` | Optional Bearer token, if testnet enforces it (confirmed in Task 1) | none, optional |
| `ACROSS_INTEGRATOR_ID` | Optional integrator identifier, if testnet requires it (confirmed in Task 1) | none, optional |
| `MAX_TESTNET_AMOUNT_WEI` | Guardrail ceiling on any single testnet-mode payment amount | a small, conservative default |
| `RECONCILE_STALENESS_SECONDS` | How long a not-yet-broadcast execution, or a `PROCESSING` payment with no execution row at all, sits without progress before the reconciler treats it as abandoned (§15 — one threshold, reused for both cases, since both ask the same "how long can this sit with no forward progress" question) | mirrors `WORKER_RECOVERY_STALENESS_SECONDS`'s order of magnitude |
| `RECONCILE_SWEEP_INTERVAL_SECONDS` | How often the reconciler goroutine runs | mirrors `WORKER_RECOVERY_SWEEP_INTERVAL_SECONDS` |
| `NONCE_DIVERGENCE_CHECK_INTERVAL_SECONDS` | How often the reconciler runs its read-only `eth_getTransactionCount` divergence check per active wallet (§15) — diagnostic only, never writes to `wallet_nonces` | mirrors `RECONCILE_SWEEP_INTERVAL_SECONDS`'s order of magnitude |

All Phase 6 environment variables (`DATABASE_URL`,
`KAFKA_BOOTSTRAP_SERVERS`, etc.) are unchanged and still required.
