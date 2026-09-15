# Phase 8 Design: Real-Time Bridge Routing

## Repository state note (resolved before this design)

At the start of this design session, local `main` (tip `3a577ce`) contained
only the Phase 1-7 *design/plan* documents -- Phase 7's actual
implementation (executor, reconciler, `bridge/across/*`, migration `0004`,
etc.) lived on a separate branch (`phase7-cleaned`, byte-identical in
content to `phase-all-cleaned` and `worktree-phase7-across-testnet-execution`)
that had never been merged into local `main`. Separately, `origin/main`
already had this content, under a rewritten/squashed commit history
(evidently prepared from another device). Local `main` has been reset to
`origin/main` (`git reset --hard origin/main`, tip now `671522a`) so that
"Phases 1-7 merged into main" is actually true going forward. This document
is written against that real, current `main`, verified by reading the
actual source files (not just the Phase 7 design doc) -- see §1-2.

## Across/Relay research (verified 2026-09-14; re-verify at implementation
## time -- bridge APIs change, and this section documents what was found,
## not a promise that it is still true when Task 1 below actually runs)

- **Across's own Phase 7 code already did live verification, not
  guesswork.** `go-api/internal/bridge/across/quote.go`'s
  `SuggestedFeesResponse` struct and `client.go`'s no-auth behavior are
  documented in-code as taken from "a live captured response" and "live
  testing during Phase 7 planning" against the actual
  `https://testnet.across.to/api` endpoint -- this is stronger evidence
  than documentation prose, and Phase 8 reuses this exact, already-proven
  client rather than re-deriving the shape from docs.
- **Fresh research this session found signal that Across's *documentation*
  has since drifted** in ways that do not necessarily contradict the
  live-verified testnet client above, but must be re-checked before Phase 8
  code ships: `docs.across.to`'s current API reference page describes
  `/suggested-fees` responses now referencing a **nested `fees` object**
  and states **all requests require a Bearer token + `integratorId`** --
  both different from what Phase 7's own live-captured testnet fixture
  shows (flat fields, no required auth). This is plausibly a mainnet-only
  documentation change that hasn't propagated to testnet, or plausibly a
  sign the testnet API itself will change. **Task 1 of the implementation
  plan (§24) must re-run Phase 7's own verification step (a live call
  against the configured `ACROSS_TESTNET_API_URL`) before any Phase 8 code
  is written against an assumed shape.** If the live shape has changed,
  `across/quote.go` is updated as part of Task 1, and every downstream
  Phase 8 type (the normalized `Quote`) is unaffected, because normalization
  happens one layer above the provider-specific parser (§4).
- **Across's contract-addresses reference page** (`docs.across.to/reference/
  contract-addresses/sepolia-testnet`) independently confirms Sepolia
  (`11155111`) and Base Sepolia (`84532`) both have deployed SpokePools
  today, consistent with Phase 7's hardcoded addresses in `cmd/worker/main.go`.
- **A confusingly-named second Across "testnet" exists and is NOT the one
  Phase 7 uses**: `docs.across.to/guides/migration/non-evm/testnet`
  describes a separate environment for testing non-EVM-migration event
  indexing, running two SpokePools both on Sepolia chain IDs, where "no
  bundles will be run" and "all WETH you deposit will be permanently
  locked" -- i.e. deposits there never actually fill. This is a distinct
  deployment from the real, functional Sepolia<->Base Sepolia testnet
  Phase 7's code integrates with. Flagged here so nobody re-discovers this
  confusion mid-implementation and second-guesses working code.
- **A real, live-verified second provider candidate exists: Relay.**
  Relay publishes a dedicated testnet API host,
  `https://api.testnets.relay.link`, distinct from its mainnet host. A
  direct live query against `https://api.testnets.relay.link/chains`
  (this session, not documentation) confirmed both Ethereum Sepolia
  (`11155111`) and Base Sepolia (`84532`) are currently listed, each with
  native ETH and WETH (`0xfFf9...d6B14` on Sepolia, `0x4200...0006` on Base
  Sepolia -- the same WETH addresses Phase 7 already hardcodes) marked as
  bridgeable. Relay's quote flow (`POST /quote` per its integration guides:
  `originChainId`/`destinationChainId`/`currency`/`amount`/`user` in,
  fee/time/execution-steps out, with a `requestId` for status tracking) is
  structurally analogous to Across's quote-then-status-poll shape, which is
  why the provider interface in §4 is shaped the way it is: proven against
  one real provider, but not accidentally Across-specific. **Per your
  explicit choice, Phase 8 does not implement Relay** -- it's documented
  here as the verified next candidate, not invented or assumed.
- **Other candidates were not pursued**: deBridge and Stargate are
  frequently named as "best Base bridge" in 2026 commentary, but no
  dedicated, documented testnet API host for either was found in this
  session's research (unlike Relay's clearly-separate `api.testnets.
  relay.link`) -- rather than guess they support Sepolia/Base Sepolia
  testnet quoting, they are left unverified and unlisted as candidates.

## 1. Phase objective and exact problem being solved

Phase 7 proved ChainRoute can safely execute one real, non-repeatable
external side effect (a signed Sepolia->Base Sepolia transaction via
Across) with full crash-safety. It did this by deliberately **not**
connecting that execution to routing: `cpp-routing-service/src/
routing_service.cpp`'s `FindRoute` always calls `simulator_.snapshot()`
for its graph, regardless of `execution_mode`, and `handler/payments.go`
separately hardcodes "only `ethereum`->`base`, asset `eth`" as the one
allowed testnet combination. The router's Dijkstra run and the worker's
real execution are two parallel, unconnected systems that happen to agree
on one hardcoded route by construction, not by any actual data flow
between them.

**The exact problem Phase 8 solves**: make the C++ router's Dijkstra run
select a route using the *same* real Across quote data the worker will
execute, and make the worker execute *exactly* the route the router
selected -- never a hardcoded route, never a route the worker
independently re-derives or re-picks. Concretely: `payment_route_hops` and
a new `payment_quotes` row become the single source of truth connecting
"what the router chose" to "what got signed and broadcast," with an
explicit, honest answer for what happens when the time between those two
events makes the original quote unreliable (§8, §9).

**What Phase 8 is explicitly not**: routing intelligence beyond one real
provider (Across), multi-hop real execution, or a second live provider
(§23) -- narrow scope, same discipline Phase 7 used, for the same reason:
proving the quote-to-execution data flow correctly does not get easier or
harder by adding a second provider or a second hop, so this phase doesn't
spend effort there.

## 2. Current architecture and what changes

| Component | Current (verified from source on `main`) | Phase 8 |
|---|---|---|
| `cpp-routing-service/src/routing_service.cpp` | `FindRoute` always builds its `Graph` from `simulator_.snapshot()` | Builds the `Graph` from request-supplied `candidate_edges` when present; simulator snapshot otherwise -- unchanged for that case |
| `router/include/chainroute/{graph,edge,node,route}.hpp` | Fully generic; simulator-agnostic already | **Untouched.** No change needed -- confirmed by reading the headers directly |
| `proto/chainroute/v1/routing.proto` | `FindRouteRequest` has no way to carry real edge data | Adds `CandidateEdge` message + `repeated CandidateEdge candidate_edges` field (§5) |
| `go-api/internal/handler/payments.go` | Hardcodes "testnet mode only supports ethereum->base, asset eth" via string comparison; no quote fetch before routing | Replaces the hardcoded check with "is there a registered quote provider for this route" (§3); fetches quotes and builds `candidate_edges` before calling `FindRoute` |
| `go-api/internal/worker/executor.go` | `signAndPersist` fetches its own fresh Across quote, validates it against the request's own chain/token constants, signs | Adds a pre-nonce-allocation fresh-quote-vs-persisted-quote tolerance check (§9); the provider/addresses used come from the persisted `payment_quotes`/`payment_route_hops` row, not `cmd/worker/main.go` constants (§10) |
| `payment_route_hops` | Already generic (`bridge_name, fee, latency_ms, liquidity, reliability`) | **No schema change.** Now populated from real quote data instead of simulator output, for testnet-mode payments |
| Persistence of "which quote was selected" | Does not exist | New `payment_quotes` table (§12) |
| `payment_executions`, `wallet_nonces`, the reconciler, `broadcastWithRecovery` | Phase 7, fully working | **Unchanged** (§11) |

## 3. Live quote ingestion architecture

```
POST /payments (execution_mode=testnet)
  -> handler.PostPayments
     -> quote.Registry.ProvidersFor(sourceChain, destChain, asset)  // NEW
        -> for each registered provider: provider.GetQuote(ctx, req)
     -> build []CandidateEdge from returned Quotes (Available == true only)
     -> grpcReq.CandidateEdges = candidateEdges
     -> h.Client.FindRoute(ctx, grpcReq)          // C++ Dijkstra, unchanged algorithm
     -> store.CreateOrGetPayment(...)              // payment + hops + NEW payment_quotes row, one transaction
     -> outbox event, Kafka, exactly as Phase 5/6/7
```

Quote fetching happens **in the HTTP request path**, synchronously, before
the routing RPC -- not in a background job, not cached, not pre-fetched.
This mirrors exactly how the routing RPC itself already works (a
synchronous call inside `PostPayments`) and is required by Across's own
"do not cache, fees are market-dependent" guidance (§ research above):
there is no correct way to serve a quote that's already stale before it's
even used to route.

**`quote.Registry`** is a small, static, in-process map built once at
`cmd/server` startup: `map[routeKey][]quote.Provider`, where `routeKey` is
`(sourceChainID, destChainID, asset)`. For Phase 8 this map has exactly one
entry (Sepolia->Base Sepolia, WETH) mapping to exactly one provider
(Across). This is not a service, not a cache, not a database table --
it's a compile-time-shaped Go value, because the set of (route, provider)
combinations ChainRoute actually supports changes only when someone adds
code, not at runtime. `ProvidersFor` returning an empty slice is exactly
how "this route isn't supported in testnet mode" is now expressed --
replacing the old hardcoded string comparison with a lookup that a second
provider extends without touching the shape of the check itself.

**Why quote fetching happens in Go, not by asking the C++ service to also
know about "no providers configured for this route"**: the "is this route
even supported for live execution" question is a Go/API-layer concern (it
gates whether *any* candidate edges exist at all, and therefore whether
`execution_mode=testnet` is accepted in the first place) -- the C++ router
never needs to know why a `candidate_edges` list is empty vs. absent; it
only ever needs to know "run Dijkstra over this graph" (simulator's or the
caller-supplied one).

## 4. Bridge provider interface and normalized quote model

```go
package quote // go-api/internal/bridge/quote

type Provider interface {
    Name() string
    GetQuote(ctx context.Context, req Request) (Quote, error)
}

type Request struct {
    SourceChainID, DestinationChainID int64
    Asset                              string   // normalized symbol, e.g. "WETH"
    AmountBaseUnits                    *big.Int
}

type Quote struct {
    ProviderName          string          // e.g. "across" -- distinct name from the Provider interface above, deliberately, to avoid the two colliding in reader's or IDE's eyes
    SourceChainID, DestinationChainID int64
    Asset                 string
    InputAmountBaseUnits  *big.Int
    OutputAmountBaseUnits *big.Int
    FeeBaseUnits          *big.Int        // InputAmountBaseUnits - OutputAmountBaseUnits
    EstimatedFillTimeSec  int64
    Available             bool            // false: no viable route/liquidity for this amount right now
    QuotedAt, ExpiresAt   time.Time       // ExpiresAt is ChainRoute policy (§8), not a protocol guarantee
    RawProviderPayload    json.RawMessage // opaque, provider-specific execution inputs (Across: exclusiveRelayer, fillDeadline, exclusivityDeadline, timestamp)
}
```

`across.Provider` (new, thin) wraps the **existing**
`across.Client.SuggestedFees` (Phase 7's already-built, already-live-
verified client) -- it does not duplicate the Across HTTP integration.
`GetQuote` calls `SuggestedFees`, sets `Available = !resp.IsAmountTooLow`,
computes `FeeBaseUnits = InputAmountBaseUnits - OutputAmountBaseUnits`, and
marshals `{exclusiveRelayer, fillDeadline, exclusivityDeadline, timestamp,
spokePoolAddress}` into `RawProviderPayload`. Routing-time and
execution-time quotes therefore go through byte-identical parsing code --
there is exactly one place in the whole codebase that understands Across's
JSON shape, so routing and execution can never silently disagree about
what a field means.

**Why `RawProviderPayload` is opaque JSON instead of typed fields for
everything**: the normalized `Quote` carries exactly what the *router*
needs -- fee, availability, estimated time, chain/asset/provider identity
(your explicit requirement). Across's `depositV3`-specific fields are real
and necessary, but forcing them into the common model would leak one
provider's transaction-construction shape into a type every future
provider must also satisfy. They're threaded through opaquely and decoded
only by `across`'s own execution code (`BuildAndSignDepositV3Tx`) --
exactly mirroring how `payment_executions.unsigned_tx_params JSONB` already
exists in the Phase 7 schema for the same reason.

**Mapping into `CandidateEdge`/`Edge` fields** (§5, §6) -- documented
honestly rather than glossed over:
- `fee`: `FeeBaseUnits` converted to the request's own decimal asset units
  (via the existing, Phase-7-proven `money` package's inverse of
  `DecimalToBaseUnits`). This is a **deliberate deviation** from
  `edge.hpp`'s own comment (`fee; // absolute cost, USD`) -- Phase 8 does
  not have a price oracle and does not invent one. Dijkstra's correctness
  does not depend on the unit being USD, only on every edge in one graph
  sharing the same unit, which holds trivially here since one request is
  always one asset.
- `liquidity`: `route.cpp`'s `findCheapestRoute` uses `liquidity` as a hard
  eligibility filter (`if (edge.liquidity < amount) continue;`, confirmed
  by reading `router/src/route.cpp` directly), compared against the
  request's own `amount` (display-unit `double`, e.g. `0.001`). Across's
  `/suggested-fees` does not report a liquidity depth figure at all -- it
  either returns a quote for the exact requested amount or reports
  `isAmountTooLow`. The honest mapping is binary, not invented depth data:
  `liquidity = amount` (exactly satisfies the filter) when `Available`,
  `liquidity = 0` (fails the filter) when not. This makes an unavailable
  quote structurally invisible to Dijkstra rather than a candidate with a
  fabricated liquidity number.
- `reliability`: fixed at `1.0`. Across publishes no success-probability
  figure; inventing one would be exactly the "do not invent support" the
  requirements warn against, at the level of a fabricated number instead
  of a fabricated route.
- `latency_ms`: `EstimatedFillTimeSec * 1000`.

## 5. Go <-> C++ gRPC/protobuf changes

```proto
message CandidateEdge {
  string bridge_name = 1;
  double fee = 2;
  double latency_ms = 3;
  double liquidity = 4;
  double reliability = 5;
}

message FindRouteRequest {
  Chain source_chain = 1;
  Chain destination_chain = 2;
  Asset asset = 3;
  double amount = 4;
  repeated CandidateEdge candidate_edges = 5;  // NEW
}
```

No changes to `FindRouteResponse`, `RouteHop`, `Chain`, or `Asset` --
`Ethereum`/`Base`/`ETH` already exist and are sufficient for Across-only
Phase 8. `RouteHop` already carries `bridge_name`/`fee`/`latency_ms`/
`liquidity`/`reliability` per hop, unchanged, since a `CandidateEdge` that
wins Dijkstra becomes exactly one `RouteHop` in the response -- the same
struct shape flows in as a candidate and out as the selected hop.

`FindRoute`'s wire contract is backward compatible by construction: an
absent/empty `candidate_edges` (every existing caller, and every
simulated-mode call after Phase 8 ships) produces byte-identical behavior
to today.

## 6. Changes to the C++ graph/routing model

**None to `graph.hpp`/`edge.hpp`/`node.hpp`/`route.hpp`.** Confirmed by
reading all four headers and `route.cpp` directly: `Graph` already
supports parallel edges between one node pair (exactly how
`NetworkSimulator` itself models multiple bridges today), and
`findCheapestRoute` is already graph-source-agnostic.

The only change is in `cpp-routing-service/src/routing_service.cpp`:

```cpp
chainroute::Graph buildGraphFromCandidates(
    chainroute::ChainId source, chainroute::ChainId dest, chainroute::AssetId asset,
    const google::protobuf::RepeatedPtrField<chainroute::v1::CandidateEdge>& candidates) {
    chainroute::Graph graph;
    auto sourceNode = graph.addNode(chainroute::Node{source, asset});
    auto destNode = graph.addNode(chainroute::Node{dest, asset});
    for (const auto& c : candidates) {
        graph.addEdge(sourceNode, chainroute::Edge{
            destNode, c.bridge_name(), c.fee(), c.latency_ms(), c.liquidity(), c.reliability()});
    }
    return graph;
}
```

`FindRoute` picks this path when `request->candidate_edges_size() > 0`,
else keeps calling `simulator_.snapshot()` exactly as today -- one `if`,
no change to the simulator, no change to `main.cpp`'s wiring.

**Why not extend the simulator itself to accept injected edges**: the
simulator's whole purpose is deterministic, seeded, reproducible synthetic
data for local/CI testing (§16) -- teaching it about real quotes would
conflate two unrelated concerns (deterministic test fixtures vs. real
external data) in one class, for no benefit, since `routing_service.cpp`
can build the two-node real-quote graph itself in a few lines without the
simulator's involvement at all.

## 7. Route selection and persistence

Selection is Dijkstra's existing, untouched output (§6) -- a `Route` with
one edge (single-hop only, §1) chosen as cheapest by `fee` among whatever
candidates passed the `liquidity >= amount` filter.

Persistence happens in the **same Postgres transaction** as
`CreateOrGetPayment`'s existing `payments` + `payment_route_hops` +
outbox-event insert (`store.go`'s `CreateOrGetPayment`, unchanged
structure, one more table written inside the same `tx`):

1. `payments` row (unchanged columns).
2. `payment_route_hops` row(s) -- for testnet mode, exactly one row, whose
   `bridge_name`/`fee`/`latency_ms`/`liquidity`/`reliability` are the
   winning `RouteHop` from the real quote, not simulator output.
3. **New**: `payment_quotes` row (§12) -- the full normalized `Quote` that
   produced that winning edge, including `RawProviderPayload`.
4. Outbox event, exactly as Phase 5/6/7.

**Why one transaction, not "persist the payment, then separately record
the quote"**: this is the exact mechanism that makes "the exact
provider/route selected is persisted" true as a database invariant rather
than an application-level intention -- if the transaction fails partway,
there is no payment row with no matching quote row (which would be
unexecutable: the worker would have a route to execute but no quote to
validate freshness against, §9) and no quote row with no payment (an
orphan). `UNIQUE(payment_id)` on `payment_quotes` (§12) makes "at most one
selected quote per payment" enforced the same way Phase 7's
`UNIQUE(payment_id)` on `payment_executions` already enforces "at most one
execution per payment."

## 8. Quote freshness/expiration model

Two genuinely different kinds of staleness exist here, and conflating them
was the main trap in this design (worked through in detail during
brainstorming, restated precisely here):

1. **Fee/market staleness** -- a ChainRoute policy question: "is the fee
   this payment was routed on still close enough to reality?" Governed by
   `payment_quotes.expires_at = quoted_at + ROUTING_QUOTE_TTL_SECONDS`
   (config, default a conservative small number of minutes) **and**, more
   importantly, by the tolerance check in §9 -- the TTL alone is a coarse
   backstop, not the actual mechanism that decides pass/fail, because a
   quote can be technically "unexpired" by TTL and still have drifted
   fee-wise, and vice versa. Across's own documentation gives no formal
   quote-validity duration -- `ROUTING_QUOTE_TTL_SECONDS` is disclosed
   explicitly as **ChainRoute's own conservative bound, not a protocol
   guarantee**, exactly the same honesty Phase 7 already applied to its
   own open items rather than inventing a number and presenting it as
   authoritative.
2. **Protocol-level quote validity** -- Across's own constraint:
   `quoteTimestamp`/`fillDeadline`/`exclusivityDeadline` are time-sensitive
   on-chain parameters that Phase 7's executor already re-fetches fresh
   immediately before every signing attempt, including on crash-resume.
   **This behavior is unchanged and must stay unchanged** -- reusing a
   routing-time quote's raw fields at execution time (rejected during
   design) risks an on-chain revert against a deadline that's gone stale by
   the time a resumed/reconciler-driven attempt actually broadcasts, which
   could happen minutes or hours after routing.

`payment_quotes.expires_at` exists so that a request to `GET /payments/
{id}` (or an operator) can answer "was this payment ever routable at all,
or did we route it against something already gone stale by the time
execution even began" -- e.g. the worker was down for an extended period
before picking up a Kafka-redelivered `PAYMENT_ROUTED` event. If
`now() > expires_at` at the point execution begins (checked once, before
the fresh re-quote in §9), the payment is marked `FAILED` with reason
`routing_quote_expired` **without even attempting a fresh quote** -- this
is a distinct, cheaper, more specific failure than a slippage rejection,
and matters operationally: it tells you "nothing was even attempted"
rather than "we tried and the market moved."

## 9. Fee/slippage protection

**The restructured executor sequence** (a real, deliberate change to
Phase 7's current ordering in `executor.go`, not a gratuitous refactor):

Today (`main`, Phase 7): `ExecuteTestnetPayment` calls `TryCreateExecution`
(allocates a nonce) **first**; the Across quote fetch and validation
happen **inside** `signAndPersist`, which runs after.

Phase 8, on the **first** attempt for a payment:

1. Check `payment_quotes.expires_at` (§8) -- if already expired, `FAILED`
   as `routing_quote_expired`, no nonce allocated, done.
2. Fetch a **fresh** quote from the same provider recorded in
   `payment_quotes.provider` (via the `quote.Provider` interface, §4).
3. Compare `freshQuote.FeeBaseUnits` against `payment_quotes.fee_amount`
   (the routing-time quote). The check is one-directional: only a fee
   *increase* beyond `MAX_FEE_SLIPPAGE_BPS` (config, default a few hundred
   basis points) triggers `FAILED` as `fee_slippage_exceeded`, with **no
   nonce allocated**. A fresh fee equal to or *lower* than the routing-time
   quote always passes -- the payer is never worse off than what they were
   quoted, so there is nothing to protect against in that direction.
4. Only if within tolerance: proceed to `TryCreateExecution` (nonce
   allocation), then sign using the **fresh** quote's numbers (never the
   stale routing-time ones) -- exactly Phase 7's existing sign/persist/
   broadcast path, untouched from here on.

**Why check-before-allocate, specifically**: a nonce allocated for a check
that can legitimately and routinely fail (market fees move constantly,
unlike Phase 7's `MaxAmountWei` ceiling check, which the API layer already
makes structurally unreachable in normal operation) would strand that
nonce -- and because EVM nonces are strictly sequential per wallet, a stuck
low nonce head-of-line-blocks every higher nonce, i.e. every other payment
queued behind it on the same wallet. Checking tolerance before allocating
means a routine slippage rejection on the *first* attempt costs nothing:
no nonce burned, no other payment blocked.

**The honest remaining gap -- resume/retry.** The fresh-quote-and-
tolerance check must still run on *every* signing attempt, including a
reconciler-driven resume of an already-nonce-allocated row (crash point B,
§13), because concern 2 above (protocol-level freshness) demands a fresh
quote every time, unconditionally. So a row whose nonce is **already**
allocated can still hit a slippage rejection on resume, if a fee move is
large and sustained across retries. **Phase 8 does not pretend to
auto-heal this.** It is disclosed as the same category of limitation
Phase 7 already accepts for a nonce that never resolves on-chain (Phase 7
design spec §15 point 4): no fee-bumping, no cancellation transaction, no
nonce-skipping -- manual operator intervention (a replacement/cancellation
transaction at that nonce) is the documented remedy. Two things keep this
rare rather than routine: a deliberately generous default tolerance (a
single-tick slippage failure just delays that one payment by one
reconciler interval and self-resolves, since nothing has broadcast yet --
only a *sustained* fee move across many retries is actually stuck), and a
dedicated, distinctly-worded log line (§21) so an operator notices a
sustained block quickly rather than discovering a silently backed-up
per-wallet queue.

**Where a slippage/expiry rejection is NOT recoverable by re-routing
automatically, and why**: per your requirement ("do not allow the worker
to independently choose a different bridge after routing"), a rejected
payment simply becomes `FAILED`. It does not trigger a fresh Dijkstra run,
a new quote fetch for a *different* provider, or any other form of
self-directed re-routing by the worker. The caller can submit a new
`POST /payments` (a fresh idempotency key, or after inspecting the
`FAILED` reason) to get a brand-new routing decision from scratch. This
keeps "select a route" (API layer + C++, happens once) and "execute a
route" (worker, never re-decides) a bright line, exactly as required.

## 10. Worker execution changes

- `Executor` gains `QuoteProviders map[string]quote.Provider`, keyed by
  provider name -- a lookup, not a single default, since a future second
  provider means the executor must be able to re-quote via *whichever*
  provider a given payment actually used, not necessarily the one
  `cmd/worker` happens to default to. For Phase 8 this map has exactly one
  entry (`"across"`), mirroring §3's `quote.Registry` in shape (a static,
  in-process value, not infrastructure).
- `Executor.BridgeProvider`/`SpokePoolAddress`/`WETHOrigin`/
  `WETHDestination` are read from the payment's own `payment_quotes`/
  `payment_route_hops` rows at the start of `ExecuteTestnetPayment`,
  **not** from hardcoded constants in `cmd/worker/main.go` as they are on
  current `main`. If `payment_quotes.provider` names a provider the worker
  has no configured client for, that is a hard, loud error producing
  `FAILED` -- never a silent substitution of a different provider. This is
  the concrete mechanism (not just an assertion) behind "the worker never
  independently chooses a different bridge": it reads *which* provider was
  selected, it never decides.
- `DriveExecutionForward` (the single resume entry point used by both
  crash-recovery and the reconciler, unchanged from Phase 7) is where the
  new §9 check actually runs -- so fresh and resumed attempts share one
  code path, matching Phase 7's own stated principle that a resume path
  must never drift out of sync with the fresh path.
- `signAndPersist`'s existing `validateQuote` (chain IDs/token addresses
  echoed by the quote must match what was requested) is unchanged and
  still runs, now against whichever provider's fresh quote was fetched.

## 11. Relationship to Phase 7 execution/reconciliation

Every Phase 7 guarantee is preserved unchanged: durable nonce allocation
(`wallet_nonces`' atomic `UPDATE`), signed-bytes-persisted-before-broadcast,
identical-byte rebroadcast on ambiguous outcomes, chain-lookup-first
recovery, and destination-fill reconciliation via Across's
`/deposit/status`. Phase 8 adds exactly one new precondition in front of
the *first* nonce allocation (§9) and does not modify
`broadcastWithRecovery`, `checkAndUpdateOutcome`, the reconciler's
lowest-nonce-first prioritization, or the nonce-divergence diagnostic at
all -- none of those need to know anything changed upstream of them.

`Reconciler.recoverStaleProcessingWithoutExecution` (crash point A
recovery) calls `Executor.ExecuteTestnetPayment` exactly as today; that
call now runs the §9 checks as its first action, which is exactly the
right place for them to run on this recovery path too -- a payment stuck
`PROCESSING` with no execution row for a long time (long enough to trip
`RECONCILE_STALENESS_SECONDS`) is *more* likely to also have a stale
routing quote, not less, so the checks firing here is a feature, not friction.

## 12. PostgreSQL schema changes

New migration `go-api/migrations/0005_realtime_bridge_routing.sql`:

```sql
CREATE TABLE payment_quotes (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id              UUID NOT NULL REFERENCES payments(id) ON DELETE CASCADE,
    provider                TEXT NOT NULL,
    origin_chain_id         BIGINT NOT NULL,
    destination_chain_id    BIGINT NOT NULL,
    asset                   TEXT NOT NULL,
    input_amount            NUMERIC(38,0) NOT NULL,
    output_amount           NUMERIC(38,0) NOT NULL,
    fee_amount              NUMERIC(38,0) NOT NULL,
    estimated_fill_time_sec INTEGER NOT NULL,
    quoted_at               TIMESTAMPTZ NOT NULL,
    expires_at              TIMESTAMPTZ NOT NULL,
    raw_provider_payload    JSONB NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (payment_id)
);

ALTER TABLE payments
    ADD COLUMN failure_reason TEXT NULL;
    -- Populated ONLY for the two new Phase 8 reasons introduced in §8/§9:
    -- 'routing_quote_expired', 'fee_slippage_exceeded'. Every pre-existing
    -- Phase 1-7 failure path (simulated-mode execution failure, an
    -- on-chain revert, an expired/refunded Across deposit) leaves this
    -- NULL, deliberately -- Phase 8 does not touch simulated-mode logic
    -- (§16) or Phase 7's existing FAILED paths (§11), so it does not
    -- retrofit reason-tracking onto code paths it isn't otherwise
    -- changing. NULL therefore means "a Phase 1-7 failure path," not
    -- "unknown."
```

The `payments.status` enum itself (`ROUTED`/`PROCESSING`/`SUBMITTED`/
`COMPLETED`/`FAILED`) is unchanged -- `FAILED` already covers every
failure case Phase 8 introduces; `failure_reason` adds *why*, not a new
terminal state.

**`payments.bridge_provider` vs. `payment_quotes.provider`**: not
duplication -- `payments.bridge_provider` is the existing (Phase 7),
already-API-exposed summary field (`GET /payments/{id}`), the same role
`payments.total_fee` already plays alongside the detail rows in
`payment_route_hops`. Phase 8 changes *how* it's populated (dynamically
from the winning quote's provider, at `CreateOrGetPayment` time, instead
of the hardcoded `"across"` string literal in today's handler) but keeps
it as the field API clients already read. `payment_quotes.provider` is
the detailed backing row the worker actually keys its provider lookup on
(§10) -- the same summary/detail relationship, not two sources of truth
for the same fact.

**Why `failure_reason` as a new `payments` column, not a `payment_quotes`
column**: a slippage/expiry rejection can happen even for a payment whose
`payment_quotes` row is perfectly fine (the rejection is about the *fresh*
quote at execution time, which is never persisted as its own row -- only
compared, then discarded) -- the reason belongs on the payment's own
terminal-state record, the same place `status` itself lives, not on a
row describing a specific historical quote.

**Why not fold `payment_quotes` into `payment_route_hops`**: `payment_
route_hops` is keyed by `(payment_id, hop_index)` and its columns are
exactly `RouteHop`'s wire shape -- extending it with quote-freshness
metadata (`quoted_at`, `expires_at`, `raw_provider_payload`) would
conflate "what the API response looked like" with "what we need to
re-validate later," the same reasoning Phase 7 used for keeping
`payment_executions` separate from `payments`.

## 13. Failure and crash analysis

New crash points, extending Phase 7's table (§12 of the Phase 7 spec is
unchanged and still applies to everything from nonce allocation onward):

| Point | Durable state | Restart/retry behavior | New tx risk |
|---|---|---|---|
| L. Quote provider unreachable at `POST /payments` time | No payment row created | `503` returned to caller; caller retries the whole request (idempotency key ensures a retry after the provider recovers creates exactly one payment) | No |
| M. Provider reachable, reports no viable route (e.g. `isAmountTooLow`) | No payment row created | `422`, same status code Phase 1-7 simulated mode already uses for "no route found" -- not a new error shape for callers to learn | No |
| N. Quote persisted, `payments` row created, worker never reaches execution before `expires_at` | `payments.status = PROCESSING` or `ROUTED`, `payment_quotes` row present, no `payment_executions` row | On pickup (fresh delivery or reconciler recovery), §9 step 1 fires first: `FAILED('routing_quote_expired')`, no nonce ever touched | No |
| O. Fresh quote fetched, outside tolerance | Same as N | `FAILED('fee_slippage_exceeded')`, no nonce allocated (this attempt) | No |
| P. Fresh quote fetched, within tolerance, crash before `TryCreateExecution` commits | Same as N (nothing durable changed) | Next attempt re-runs step 1-3 from scratch (harmless -- both are pure reads plus one idempotent-in-effect HTTP call) | No |
| Q. Nonce allocated (`payment_executions` row exists), then a *later* resume's §9 check fails | `payment_executions` row with `nonce` set, no signed bytes | **Disclosed limitation** (§9, §23): this specific nonce cannot self-heal from a sustained fee move; every higher nonce on the same wallet is blocked until an operator intervenes | No -- but nonce is stuck, per §9 |

Every row above still holds the Phase 7 invariant "no restart path ever
constructs or signs a *second* transaction for the same execution row" --
Phase 8 only adds new ways to reach `FAILED` *before* a nonce is spent, and
one disclosed way a nonce that *was* already allocated can become stuck
for a reason (fee slippage) that Phase 7 didn't have to consider.

## 14. Concurrency/race-condition analysis

- **Two concurrent `POST /payments`, different idempotency keys, same
  route**: each performs its own independent `GetQuote` call and gets its
  own `payment_quotes` row inside its own transaction. No shared mutable
  quote state exists anywhere -- there is nothing to lock beyond what
  `CreateOrGetPayment`'s existing transaction already provides.
- **Same idempotency key retried while the first request's quote fetch is
  still in flight**: unchanged from Phase 7's existing pattern --
  `LookupByIdempotencyKey` runs *before* the (now quote-fetch-plus-)routing
  RPC is even attempted, so a retry either finds the already-committed
  payment (200/409, per existing logic) or proceeds as an independent
  attempt. No new race: quote-fetching is pure read/compute happening
  entirely before any durable write, structurally identical in shape to
  the routing RPC call it sits next to.
- **Reconciler and a live executor both evaluating §9's tolerance check for
  the same payment**: not a race requiring arbitration. Both would fetch
  their own fresh quote (Across's docs explicitly says these responses are
  never identical between calls, so this is not even a read of shared
  state) and independently reach the same tolerance decision from the same
  persisted `payment_quotes.fee_amount` baseline. The actual race that
  *does* need arbitration -- who gets to allocate the nonce -- is
  unchanged, still resolved by `payment_executions`' existing
  `UNIQUE(payment_id)`, exactly as Phase 7 already proved (design spec §15
  race analysis, untouched).
- **A payment's `payment_quotes` row read concurrently with nothing ever
  writing to it after creation**: `payment_quotes` is write-once
  (`INSERT` inside `CreateOrGetPayment`'s transaction, never `UPDATE`d
  afterward) -- there is no update race to analyze here at all, which is
  itself a deliberate simplification: recording "what was selected" as an
  immutable fact, rather than a row something might later mutate, removes
  an entire category of race by construction.

## 15. Security and validation

- Every quote response is validated before it can influence a graph or a
  signing call: `across.Provider.GetQuote` rejects (does not return
  `Available: true` for) a response whose echoed chain IDs/token addresses
  don't match the request -- reusing exactly the validation Phase 7's
  `Executor.validateQuote` already does, now applied at quote-normalization
  time as well as at signing time (two independent checks, not one shared
  one, since routing-time and execution-time quotes are different HTTP
  responses).
- `CandidateEdge` fields arriving over gRPC from Go into C++ are `double`s
  computed server-side from validated quote data -- never raw
  provider-supplied strings, and never client-supplied (no HTTP client can
  set `candidate_edges` directly; it's built entirely inside
  `handler.PostPayments` from the server's own quote fetch).
- `quote.Registry`'s route-to-provider map is the *only* thing that
  decides whether `execution_mode=testnet` is accepted for a given
  chain/asset combination -- replacing Phase 7's hardcoded string
  comparison with a lookup does not weaken this gate; it's still a
  fail-closed allowlist (an unregistered route yields zero providers,
  which yields zero candidate edges, which yields `422`/`400`, never a
  fallback to "try anyway").
- No new secrets: Phase 8 introduces no new provider API keys beyond what
  Phase 7 already wires (`ACROSS_API_KEY`, optional and currently unused
  in practice per the live-verified testnet behavior, §research). If
  Task 1's re-verification (§ research, §24) finds testnet now requires
  auth, that credential follows the exact same environment-only,
  never-logged handling Phase 7 already established for
  `TESTNET_WALLET_PRIVATE_KEY`.
- `MAX_TESTNET_AMOUNT_WEI` (Phase 7) and the new `MAX_FEE_SLIPPAGE_BPS`
  (§9) are both defense-in-depth ceilings enforced server-side, never
  client-configurable per request.

## 16. Simulated-mode compatibility

`execution_mode=simulated` (the default, and every existing test/caller)
is untouched at every layer:
- `handler.PostPayments`: the new quote-fetch step is gated on
  `mode == payment.ExecutionModeTestnet`; simulated-mode requests never
  call `quote.Registry` at all.
- `FindRouteRequest.candidate_edges` is left empty for every simulated-mode
  call, so `routing_service.cpp` takes the unchanged `simulator_.snapshot()`
  path -- byte-identical output to pre-Phase-8 `main` for the same seed,
  which is exactly what keeps `router/tests` and `cpp-routing-service`'s
  existing fixtures valid with zero changes.
- No new required environment variables for simulated mode: everything in
  §3/§9/§12 (`quote.Registry`, `MAX_FEE_SLIPPAGE_BPS`,
  `ROUTING_QUOTE_TTL_SECONDS`) is only constructed/consulted on the
  testnet-mode code path, mirroring exactly how Phase 7's `BLOCKCHAIN_ENV
  != testnet` already leaves `Executor`/`Reconciler` nil and unused.
- `NetworkSimulator` itself: zero changes (§6).

## 17. Unit tests (no network access required)

- `quote.Quote` normalization from a fixed, hand-written Across
  `SuggestedFeesResponse` fixture (reusing/extending Phase 7's own
  `quote_test.go` fixture) -- asserts `FeeBaseUnits`, `Available`,
  `RawProviderPayload` round-trip correctly, including the
  `isAmountTooLow == true` -> `Available == false` -> zero-liquidity
  mapping (§4).
- `CandidateEdge` construction from a `Quote`: fee/liquidity unit
  conversion (base units -> decimal, matching the request's `amount`
  units), including the deliberate liquidity-is-binary mapping.
- `quote.Registry.ProvidersFor`: registered route returns the expected
  provider(s); unregistered route returns empty, never panics or falls
  back.
- §9's tolerance decision as a pure function: given a persisted fee and a
  fresh fee, assert the exact boundary behavior (at tolerance passes, one
  unit over tolerance fails), independent of any HTTP call, against a fake
  `quote.Provider`.
- §8's expiry check as a pure function: `now` before/at/after `expires_at`.
- C++: `routing_service_test`-level coverage (new, alongside existing
  `router`/`cpp-routing-service` tests) that `FindRoute` with a non-empty
  `candidate_edges` list ignores the simulator entirely and picks the
  cheapest candidate by `fee` subject to the `liquidity >= amount` filter
  -- including a case where every candidate is filtered out (liquidity too
  low) and `route_found = false` is returned, exactly like today's
  no-route case.
- Handler-level: `execution_mode=testnet` for an unregistered route returns
  `400` (replacing the old hardcoded-string-comparison test with a
  registry-lookup one); a provider returning `Available: false` for every
  candidate produces `422` "no route available," matching the existing
  simulated-mode wording/status code.

## 18. PostgreSQL/integration tests (existing `integration` build tag)

- `payment_quotes` row lifecycle: created atomically with `payments` +
  `payment_route_hops` inside `CreateOrGetPayment`'s transaction; a forced
  failure after the `payments` insert but before the `payment_quotes`
  insert (matching Phase 7's own transaction-rollback test style) leaves
  **no** payment row at all -- proving the one-transaction claim in §7, not
  just asserting it.
- `UNIQUE(payment_id)` on `payment_quotes`: a second attempted insert for
  the same `payment_id` fails with the expected constraint violation.
- §9's full sequence against a real (test) Postgres: a payment whose fresh
  re-quote is engineered (via a fake provider) to exceed tolerance ends up
  `FAILED('fee_slippage_exceeded')` with **zero** rows in
  `payment_executions` and **zero** nonces consumed from `wallet_nonces`
  (directly asserting the "no nonce burned" claim from §9, the same way
  Phase 7's own integration tests directly assert nonce-uniqueness under
  concurrency rather than trusting the design doc's prose).
- §8's expiry path: a payment whose `payment_quotes.expires_at` is
  artificially set in the past reaches `FAILED('routing_quote_expired')`
  without any HTTP call to the (fake) provider being made at all --
  asserting step 1 of §9 short-circuits before step 2, not just that the
  end state is correct.
- Crash/resume: an execution row created (nonce allocated) by one
  goroutine, then a second goroutine's `DriveExecutionForward` call
  (modeling the reconciler) hits a slippage rejection on resume -- assert
  the row is left exactly as §13 point Q describes (nonce still allocated,
  no signed bytes, payment `FAILED`), and that this is logged as the
  distinct, alertable condition from §21, not silently swallowed.

## 19. Real testnet integration tests (separately gated, extending Phase
## 7's existing `testnet_integration` build tag + `RUN_TESTNET_TESTS=1`)

- One real `quote.Registry.ProvidersFor` -> real `across.Provider.
  GetQuote` -> real C++ `FindRoute` call with real `candidate_edges` ->
  assert the response's single hop matches the real quote's fee/output
  amount (within the conversion tolerance from unit tests) -- proving the
  whole live-ingestion pipeline against the actual Across testnet API, not
  a fixture.
- One real end-to-end slippage scenario is **not** attempted here (there's
  no reliable way to force a real market fee move on demand) -- covered
  instead by the fake-provider integration tests in §18, which is the
  correct place for a scenario that must be deterministically triggerable.

## 20. End-to-end test plan

`scripts/e2e_test.sh` (local, deterministic, network-free) gains one more
check alongside its existing migration-presence checks: migration `0005`
applied. It is **not** modified to depend on any network, exactly
preserving Phase 7's own rule for this script.

`scripts/e2e_testnet_test.sh` (Phase 7's existing, separately-gated real-
network smoke script) gains one new step inserted before its existing
`POST /payments` call: assert the response's `hops[0].fee` is plausible
(non-zero, within a sane bound) and that a `GET /payments/{id}` immediately
after creation shows `bridge_provider: "across"` sourced from the real
quote -- i.e. this script now proves the *routing* decision came from a
real quote, not just that *execution* did (which it already proved in
Phase 7).

## 21. Observability/logging

Extends Phase 7's existing structured-logging fields (`payment_id`,
`execution_id`, `bridge_provider`, `tx_hash`, state transitions) with:
`quote_id` (`payment_quotes.id`), the routing-time and fresh execution-time
fee (both, whenever a tolerance check runs, so an operator can see the
actual delta that triggered a pass or a rejection), and a distinctly-
worded log line specifically for a **sustained** slippage block (§9,
§13 point Q) -- e.g. logged once per reconciler tick a given execution row
remains stuck on slippage, at `WARNING` or louder, naming the wallet
address and nonce, since that's the actionable information an operator
needs to decide whether to intervene manually. `RawProviderPayload` is
logged only at a size/field-count summary level, never verbatim -- it can
contain provider-specific values not meant for routine logs, and nothing
in this design needs the raw JSON in a log line to debug it (it's always
available by reading the `payment_quotes` row directly).

## 22. Explicit guarantees

- **Selection persistence.** At most one `payment_quotes` row per payment
  (`UNIQUE(payment_id)`), created atomically with the payment and its
  route hops -- not merely intended, database-enforced (§7, §18).
- **No independent re-selection.** The worker never fetches a quote from,
  or constructs a transaction for, any provider other than the one named
  in the payment's own persisted `payment_quotes.provider` (§10) -- a
  provider the worker has no client for is a hard error, never a silent
  substitution.
- **No silent execution under materially different terms.** A fresh
  execution-time quote whose fee exceeds the configured tolerance relative
  to the routing-time quote always produces `FAILED`, never a signed
  transaction (§9) -- and this holds on every attempt, fresh or resumed,
  not just the first.
- **No nonce burned by a routine rejection.** A slippage or expiry
  rejection on the *first* attempt for a payment never allocates a nonce
  (§9, §13) -- directly, integration-tested (§18), not just argued.
- **Every Phase 7 guarantee, unchanged.** Nonce uniqueness, signed-bytes-
  before-broadcast, identical-byte rebroadcast, chain-lookup-before-
  rebroadcast, and destination-fill-defined `COMPLETED` all continue to
  hold exactly as Phase 7 specified and tested them (§11).
- **Simulated mode, byte-identical.** Every existing simulated-mode test
  and fixture continues to pass unmodified (§16).

## 23. Explicit non-goals and limitations

- **A second live provider is not implemented.** Relay is documented
  (§ research) as a real, currently-verified candidate with a compatible
  quote/status shape, specifically so that adding it later is an
  incremental `quote.Provider` implementation plus one `quote.Registry`
  entry, not a redesign -- but implementing it is out of scope for this
  phase, per your explicit choice.
- **Multi-hop real execution is not supported.** The real-quote graph is
  always exactly two nodes (source, destination) with parallel direct
  edges (§1, §6); Dijkstra's multi-hop capability is exercised only by
  simulated mode. Chaining two real bridges in one payment (partial-
  failure semantics across two independent, non-repeatable side effects)
  is a materially larger problem than this phase's scope.
- **A stuck nonce from a sustained slippage block has no automated
  remedy** (§9, §13 point Q) -- disclosed explicitly, in the same category
  as Phase 7's already-disclosed "genuinely unrecoverable nonce" limitation
  (no fee-bumping, no replace-by-fee, no nonce-skipping). Manual operator
  intervention is the documented remedy.
- **No price oracle, no USD-denominated fee comparison across different
  assets.** `fee`/`liquidity` for real-quote edges are in the request's own
  asset units (§4, §6) -- correct for Dijkstra within one request, but not
  literally comparable to the simulator's `fee`-as-USD convention, and this
  is not "fixed" by inventing a conversion.
- **`ROUTING_QUOTE_TTL_SECONDS` is a ChainRoute policy choice, not a
  protocol-verified bound** (§8) -- Across's documentation gives no formal
  quote-validity duration to build this on top of.
- **No mainnet support anywhere**, exactly as Phase 7. No production key
  management beyond what Phase 7 already established. No new
  microservice, cache, or message broker -- `quote.Registry` is an
  in-process value, not infrastructure.

## 24. File-by-file implementation plan

| File | Change |
|---|---|
| `proto/chainroute/v1/routing.proto` | Add `CandidateEdge` message, `candidate_edges` field on `FindRouteRequest` |
| `cpp-routing-service/src/routing_service.cpp` | Add `buildGraphFromCandidates`; branch on `candidate_edges_size()` |
| `cpp-routing-service/tests/` | New test file for the candidate-edge path |
| `go-api/internal/bridge/quote/quote.go` (new) | `Provider` interface, `Request`/`Quote` types |
| `go-api/internal/bridge/quote/registry.go` (new) | `Registry`, `ProvidersFor` |
| `go-api/internal/bridge/quote/quote_test.go` (new) | Unit tests, §17 |
| `go-api/internal/bridge/across/provider.go` (new) | `across.Provider` wrapping the existing `across.Client.SuggestedFees` |
| `go-api/internal/bridge/across/provider_test.go` (new) | Unit tests against the existing live-captured fixture |
| `go-api/internal/handler/payments.go` | Replace hardcoded route check with `quote.Registry` lookup; fetch quotes; build `candidate_edges`; handle `Available=false`/provider-error as `422`/`503` |
| `go-api/internal/handler/payments_test.go` | Extend existing tests for the new gating logic |
| `go-api/internal/postgres/store.go` | `CreateOrGetPayment` gains the `payment_quotes` insert inside its existing transaction |
| `go-api/internal/postgres/quote_store.go` (new) | `GetQuoteByPaymentID` and any query the executor needs |
| `go-api/internal/postgres/*_integration_test.go` | Extend per §18 |
| `go-api/internal/worker/executor.go` | Restructure `ExecuteTestnetPayment`/`signAndPersist` per §9's sequence; read provider/addresses from persisted quote/hop data per §10 |
| `go-api/internal/worker/executor_test.go` | Extend per §17/§18 |
| `go-api/internal/payment/payment.go` | Add `Quote` domain type, `FailureReason` type, extend `Payment` with `FailureReason *string` |
| `go-api/migrations/0005_realtime_bridge_routing.sql` (new) | Per §12 |
| `go-api/cmd/server/main.go` | Construct `quote.Registry` at startup, wire into `Handler` |
| `go-api/cmd/worker/main.go` | Wire the provider lookup into `Executor`/`Reconciler` |
| `scripts/e2e_test.sh` | Migration `0005` check |
| `scripts/e2e_testnet_test.sh` | New assertion per §20 |

## 25. Ordered implementation tasks with acceptance criteria

1. **Re-verify Across's current live quote/status shape** against
   `ACROSS_TESTNET_API_URL` (§ research). *Accept*: a passing test proves
   the current live response matches (or an updated) `SuggestedFeesResponse`
   -- this must happen before any other task touches quote parsing.
2. **Add `CandidateEdge` to the proto; regenerate stubs.** *Accept*:
   existing generated-code-consuming code compiles unchanged; the new
   field round-trips through a Go<->C++ smoke test.
3. **Implement the C++ candidate-edge graph path.** *Accept*: new
   C++ unit tests (§17) pass; all existing `router`/`cpp-routing-service`
   tests still pass unmodified.
4. **Implement `quote.Provider`/`quote.Registry` and `across.Provider`.**
   *Accept*: unit tests (§17) pass against both a fake provider and the
   real live-captured fixture from Task 1.
5. **Migration `0005`: `payment_quotes` + `failure_reason`.** *Accept*:
   migration applies cleanly on top of `0004`; existing Phase 1-7
   integration tests still pass unmodified.
6. **Wire live quote ingestion into `PostPayments`; replace the hardcoded
   route check.** *Accept*: handler tests (§17) pass; a real testnet-mode
   request against a running stack produces a `payment_quotes` row and a
   `payment_route_hops` row whose numbers match.
7. **Restructure `Executor` per §9/§10.** *Accept*: the new unit +
   integration tests (§17/§18) proving "no nonce burned on a routine
   rejection" and "provider read from persisted data, not constants" both
   pass; every existing Phase 7 executor/reconciler test still passes
   unmodified.
8. **Wire `cmd/server`/`cmd/worker` startup.** *Accept*: `MAX_FEE_
   SLIPPAGE_BPS`/`ROUTING_QUOTE_TTL_SECONDS` are configurable with sane
   defaults; simulated mode requires zero new environment variables
   (§16), verified by running the existing local E2E script unmodified.
9. **Extend `scripts/e2e_test.sh` and `scripts/e2e_testnet_test.sh`.**
   *Accept*: both scripts pass; the local one remains fully network-free.
10. **Full regression pass**: every Phase 1-7 test (router,
    cpp-routing-service, Go unit/integration/Kafka-integration, both E2E
    scripts) plus every new Phase 8 test, all green in one run.
