# Phase 9 Design: Multi-Provider Bridge Routing — Across + Relay

## 1. Repository/current-state verification

Verified by reading the actual code on `main` (tip `24fb1f8`, Phase 8 merged), not
prior design docs:

- `go-api/internal/bridge/quote/quote.go`, `registry.go`: `Provider` interface
  (`Name() string`, `GetQuote(ctx, Request) (Quote, error)`), `Quote`
  (`ProviderName`, `FeeBaseUnits`, `Available`, `RawProviderPayload`, etc.),
  `Registry` (`map[RouteKey][]Provider`). Exactly as built, unchanged shape --
  this is the one abstraction Phase 9 reuses without modification on the
  quote side.
- `go-api/internal/bridge/across/provider.go`: `across.Provider` wraps the
  existing `Client.SuggestedFees`. `go-api/internal/bridge/across/execute.go`:
  `BuildAndSignDepositV3Tx` builds *and signs* a `depositV3` call as a
  **native-ETH-value transaction** (`Value: params.InputAmount`), with
  `InputToken` set to the WETH address purely as a `depositV3` parameter for
  the SpokePool's own auto-wrap -- confirmed directly from the code comment
  and the `types.LegacyTx{Value: params.InputAmount, ...}` construction. No
  `approve()` call anywhere. This is the single fact that makes Phase 9's
  Relay integration architecturally tractable (§4, §11).
- `go-api/internal/handler/payments.go`: the testnet-mode quote loop
  (`for _, p := range providers { q, err := p.GetQuote(...); if err != nil {
  ...; return } }`) **aborts the whole request on the first provider
  error** -- correct with exactly one provider, wrong with two. This is the
  one place Phase 8 left genuinely single-provider-shaped, not just
  single-provider-populated.
- `go-api/internal/worker/executor.go`: `Executor.signAndBroadcastFresh`
  calls `across.DecodeQuotePayload`/`across.BuildAndSignDepositV3Tx`
  directly by name. **There is no execution-side provider interface today**
  -- only the quote-side one. This is the real gap Phase 9 closes.
- `go-api/internal/worker/reconciler.go`: `Reconciler.Across *across.Client`;
  `checkAndUpdateOutcome` calls `r.Across.DepositStatusByTxHash` directly and
  switches on Across's literal status strings (`"filled"`, `"expired"`,
  `"refunded"`). Equally single-provider-hardcoded, same gap as Executor.
- `go-api/migrations/0004_across_testnet_execution.sql`:
  `payment_executions.external_status` `CHECK` is Across's own vocabulary
  (`pending|filled|expired|refunded|reverted`); `across_deposit_id TEXT NULL`
  is an Across-named column, confirmed unused by any current write path
  (Across's reconciliation keys entirely on the origin transaction hash, not
  a separate deposit ID) -- available to repurpose.
- `wallet_nonces` (keyed by `wallet_address` only) and
  `payment_executions.(wallet_address, nonce)` `UNIQUE` are **already fully
  provider-agnostic** -- nonce allocation never depended on provider
  identity. This needs a test proving it, not a design change (§9, §12).
- `proto/chainroute/v1/routing.proto`'s `CandidateEdge` is already
  `repeated` on `FindRouteRequest` and `router/`'s `Graph`/`Edge`/
  `findCheapestRoute` already support parallel edges natively (confirmed by
  reading `router/src/route.cpp` and the Phase 8 test
  `UsesCandidateEdgesInsteadOfSimulatorWhenPresent`, which already used two
  differently-named fake edges). **Zero C++ or proto changes are needed for
  basic two-provider routing** -- Phase 8 built this specifically for reuse
  here.
- `payments.bridge_provider`, `payment_route_hops.bridge_name`,
  `payment_quotes.provider` are already all set from the **same**
  `winningQuote.ProviderName` value inside **one** transaction in
  `CreateOrGetPayment` (confirmed in `handler/payments.go`) -- the
  three-way-consistency invariant your requirement #5 asks about already
  holds structurally today, by construction, not by convention. Phase 9
  doesn't need a new mechanism here, only test coverage proving it still
  discriminates correctly between two *real* providers.

## 2. Current Relay research (verified live, 2026-09-16)

All of the following were obtained by direct live calls against
`https://api.testnets.relay.link`, not from stale documentation, per your
instruction. Re-verify at implementation time (Task 1, mirroring Phase 7/8's
own precedent) -- this is a snapshot, not a promise.

- **Base URL**: `https://api.testnets.relay.link` (distinct host from
  mainnet). No `Authorization` header or API key was required for any call
  made during this research -- confirmed by successful 200/400 responses
  with zero auth headers sent.
- **Chains** (`GET /chains`): Sepolia (`11155111`) and Base Sepolia
  (`84532`) both currently listed, each with native ETH
  (`0x0000000000000000000000000000000000000000`) and WETH as a "featured"
  token at **the same addresses Across already uses**:
  `0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14` (Sepolia) and
  `0x4200000000000000000000000000000000000006` (Base Sepolia).
- **Route availability is asymmetric by origin currency, verified by four
  live quote calls**:

  | Origin | Destination | Result |
  |---|---|---|
  | native ETH | native ETH | ✅ 200, real quote |
  | native ETH | WETH | ✅ 200, real quote |
  | WETH | native ETH | ❌ 400 `NO_SWAP_ROUTES_FOUND` |
  | WETH | WETH | ❌ 400 `NO_SWAP_ROUTES_FOUND` |

  Relay's Sepolia→Base Sepolia route **only accepts native ETH as the
  origin currency** on this pair right now. This is the "verified
  incompatibility" your instructions asked me to surface rather than
  paper over -- but as resolved in brainstorming (§4), it is not
  blocking: Relay can still deliver WETH on the destination, and Across's
  own real transaction is *also* a native-value call, so both providers
  end up executing the identical on-chain shape for what ChainRoute treats
  as "the WETH route."
- **Quote endpoint**: `POST /quote`. Request:
  `{user, originChainId, destinationChainId, originCurrency,
  destinationCurrency, amount, tradeType}` (`tradeType: "EXACT_INPUT"`
  used throughout this research). A note found in Relay's own docs repo
  (GitHub PR #462, not yet independently confirmable against the live
  testnet host) flags an upcoming mandatory API-key requirement for
  `/quote/v2` with an **October 2, 2026 enforcement date** -- 16 days from
  today. Scope (testnet vs. mainnet-only) could not be confirmed from
  available documentation. **This is a live, dated risk**: Task 1 of the
  implementation plan must re-check auth enforcement, exactly mirroring
  Phase 7's own "verify live before implementation" gate, and
  `RELAY_API_KEY` is wired as an optional, environment-only config value
  from day one specifically so this can be satisfied without a code
  change if/when it becomes mandatory.
- **Quote response** (real, captured verbatim from a native-ETH→native-ETH
  call): top-level `requestId` (a bytes32-style hex string, generated by
  the quote call itself, **before any transaction is signed** -- this is
  the tracking identifier for the entire intent lifecycle, structurally
  different from Across's origin-tx-hash-based tracking); `steps` (array;
  for this route, exactly **one** step, `{id: "deposit", kind:
  "transaction", items: [{data: {from, to, data, value, chainId, gas,
  maxFeePerGas, maxPriorityFeePerGas}, check: {endpoint:
  "/intents/status?requestId=...", method: "GET"}}]}` -- no approval step,
  confirmed because the origin currency is native ETH, not an ERC-20);
  `fees` (multiple buckets: `gas`, `relayer`, `relayerGas`,
  `relayerService`, `app`, `subsidized`, each with `amount`/`amountUsd`);
  `details.currencyOut.amount` (`expectedAmount`, an optimistic estimate)
  and `details.currencyOut.minimumAmount` (the actual on-chain-enforced
  floor, echoed again inside `protocol.v2.orderData.output.payments[].
  minimumAmount`); `details.timeEstimate` (seconds); `protocol.v2.orderData.
  deadline` (a Unix timestamp, analogous to Across's `fillDeadline`).
- **The `to` address is stable across repeated identical calls**:
  `0x5feab8db4534f9f7e2669bb260c57a01ad1c12e3` on Sepolia, confirmed by two
  separate quote calls. Treated as the pinned, expected Relay deposit
  contract for Sepolia (§4) -- re-verify with a varying `user` address at
  implementation time, since this research only varied nothing else.
- **Status/reconciliation** (`GET /intents/status?requestId=...`, verified
  live against a real, never-broadcast request -- returned `{"status":
  "unknown"}`): the full enum, from Relay's own docs
  (`docs.relay.link/references/api/get-intents-status-v3`): `waiting`,
  `depositing`, `pending`, `submitted`, `delayed` (all non-terminal --
  origin seen, destination fill not yet verified), `success` (**the
  precise, verified destination-side completion signal** -- "the Relay
  Solver successfully executed the Fill Tx, and the funds have reached the
  recipient"), `refund` ("successfully refunded" -- terminal failure),
  `failure` ("unsuccessful fill" -- terminal failure, distinct from
  `refund`). Response also carries `txHashes` (destination-chain
  transaction hash(es), once available) and `inTxHashes` (origin).
- **No approval transaction is needed for the one route this phase
  targets** -- confirmed directly from the single-step response, because
  the origin asset is native ETH. This is stated as a precondition Phase 9
  enforces (§4), not an assumption: `relay.Provider.GetQuote` rejects any
  response with more than one step or a non-`"transaction"` step kind,
  before the quote is even normalized.

Sources consulted: `https://api.testnets.relay.link/chains`,
`https://api.testnets.relay.link/quote` (four live calls, verbatim
responses captured above), `https://api.testnets.relay.link/intents/status`
(one live call), `https://docs.relay.link/references/api/get-intents-status-v3`,
`https://github.com/relayprotocol/relay-docs/pull/462` (API-key callout,
date unconfirmed against the live host).

## 3. Phase objective

Phase 8 built the machinery for real multi-provider competition (parallel
`CandidateEdge`s over gRPC, `quote.Registry`, `quote.Provider`) but
populated it with exactly one provider, so Dijkstra never actually had a
choice to make. Phase 9's objective: implement `bridge/relay` as a second
real, fully-executable provider, fix the one place Phase 8 left genuinely
single-provider-shaped (failure isolation in the handler's quote loop), and
generalize `Executor`/`Reconciler` -- which are currently Across-specific by
direct reference, not merely by configuration -- into real
dispatch-by-persisted-provider and status-check-by-persisted-provider
models. The result: for a supported testnet payment, ChainRoute fetches
live competing quotes from both providers, Dijkstra genuinely picks the
cheaper viable one (by worst-case-guaranteed fee, §8), and the worker
executes and reconciles through whichever provider was actually selected --
never re-deciding, never falling back.

## 4. Architecture changes

```
go-api/internal/bridge/
  quote/
    quote.go        # Provider (unchanged), Request/Quote (unchanged)
    execute.go       # NEW: TxEnvelope, Signer interface
    status.go        # NEW: StatusRequest/StatusResult, StatusChecker interface
    registry.go      # unchanged
  across/
    provider.go      # unchanged (already implements Provider)
    execute.go       # existing BuildAndSignDepositV3Tx split: signing extracted
                       # to Executor; across.Provider gains BuildTransaction
    status.go        # existing DepositStatusByTxHash wrapped by new
                       # across.Provider.CheckStatus
  relay/             # NEW package
    client.go         # thin HTTP client for testnet.relay.link, mirrors
                       # across/client.go's shape
    quote.go           # GET .../quote request/response types + GetQuote
    execute.go         # envelope extraction + BuildTransaction
    status.go          # GET .../intents/status + CheckStatus
worker/
  executor.go        # ExecuteTestnetPayment/DriveExecutionForward: sign via
                       # quote.Signer, not across.* by name; envelope validated
                       # before signing (provider-agnostic checkpoint)
  reconciler.go       # checkAndUpdateOutcome: status via quote.StatusChecker,
                       # not across.Client by name
```

**Why a thin `execute.go`/`status.go` split inside `quote/` rather than one
bigger interface**: `Executor` only ever needs signing (`Signer`);
`Reconciler` only ever needs status (`StatusChecker`). Keeping them as two
small interfaces (each provider struct satisfies both structurally, at zero
cost in Go) means each caller's dependency is exactly what it uses --
matching your instruction not to redesign the abstraction just because
Relay looks different, and keeping `Provider` itself (the quote-fetch
contract every caller already depends on) completely untouched.

## 5. Multi-provider quote aggregation

Rewritten section of `handler.PostPayments`'s testnet-mode block:

```go
type quoteResult struct {
    provider quote.Provider
    q        quote.Quote
    err      error
}

results := make([]quoteResult, len(providers))
var wg sync.WaitGroup
ctx, cancel := context.WithTimeout(r.Context(), quoteAggregationTimeout)
defer cancel()
for i, p := range providers {
    wg.Add(1)
    go func(i int, p quote.Provider) {
        defer wg.Done()
        q, err := p.GetQuote(ctx, quote.Request{...})
        results[i] = quoteResult{provider: p, q: q, err: err}
    }(i, p)
}
wg.Wait()

anySucceeded, anyAvailable := false, false
for _, r := range results {          // index order, never completion order
    if r.err != nil {
        log.Printf("WARNING: quote provider %s failed: %v", r.provider.Name(), r.err)
        continue
    }
    anySucceeded = true
    if !r.q.Available {
        continue
    }
    anyAvailable = true
    candidateEdges = append(candidateEdges, ...)   // built from r.q
    quotesByBridgeName[r.provider.Name()] = r.q
}
if !anySucceeded {
    writeError(w, http.StatusServiceUnavailable, "bridge quote providers unavailable")
    return
}
if !anyAvailable {
    writeError(w, http.StatusUnprocessableEntity, "no route available for the requested payment")
    return
}
```

**Why a pre-sized, index-written result slice instead of a channel or a
mutex-guarded append**: each goroutine owns a disjoint index (`results[i]`),
so there is no shared mutable state and no lock needed, *and* the
subsequent `for _, r := range results` loop iterates in **registration
order**, never completion order -- this is what keeps the tie-break (§9)
deterministic regardless of which HTTP call happens to return first. A
channel-race or naive-append design would make routing outcomes depend on
network timing, which would make tests flaky and production behavior
non-reproducible.

**Why `anySucceeded` vs. `anyAvailable` are two different booleans, not
one**: they answer two different questions your requirement #2 explicitly
asked to be answered precisely -- "did we get real answers" (503 if no)
vs. "were any of the real answers usable" (422 if no). Collapsing them
would make a total provider outage indistinguishable from a genuinely
unroutable payment, which is exactly the operational confusion your
instructions warned against ("do not allow one provider's transient
failure to unnecessarily eliminate a valid quote from another provider" --
the inverse failure mode, a total outage silently reading as "no route,"
is just as misleading).

## 6. Provider abstraction changes

**None to `quote.Provider`/`quote.Request`/`quote.Quote`.** Two new,
narrow interfaces added alongside them:

```go
package quote

type TxEnvelope struct {
    To      common.Address
    Value   *big.Int
    Data    []byte
    ChainID int64
}

type Signer interface {
    Provider
    BuildTransaction(ctx context.Context, freshQuote Quote) (TxEnvelope, error)
}

type ExternalState string

const (
    StatePending    ExternalState = "pending"
    StateFilled     ExternalState = "filled"
    StateRefunded   ExternalState = "refunded"
    StateReverted   ExternalState = "reverted"
    StateFillFailed ExternalState = "fill_failed" // NEW -- see §16
)

type StatusRequest struct {
    ProviderReferenceID string // Relay's requestId; empty/unused for Across
    OriginTxHash        string // used by Across; Relay may cross-check via txHashes
}

type StatusResult struct {
    State             ExternalState
    RawStatus         string  // provider's own literal status string, for observability
    DestinationTxHash *string
}

type StatusChecker interface {
    Provider
    CheckStatus(ctx context.Context, req StatusRequest) (StatusResult, error)
}
```

`across.Provider` and `relay.Provider` both implement `Signer` and
`StatusChecker` in addition to `Provider` -- Go's structural typing means
this costs nothing extra per provider and forces no shared base type.

## 7. Relay normalized quote mapping

`relay.Provider.GetQuote` (in `bridge/relay/quote.go`):

1. Resolves `req.Asset == "WETH"` to `originCurrency =
   0x0000...0000` (native), `destinationCurrency = <destination WETH
   address>` -- the ETH-in/WETH-out mapping confirmed in brainstorming
   (§2), entirely internal to this package, invisible to `quote.Request`.
2. Calls `POST /quote` with `tradeType: "EXACT_INPUT"`.
3. On a `NO_SWAP_ROUTES_FOUND` (or any `errorCode`-carrying 4xx) response:
   returns `Quote{Available: false}, nil` -- not an error, mirroring
   `across.Provider`'s `ErrAmountTooLow` handling exactly (a provider
   answering "no viable route" is a normal outcome, not a failure).
4. **Rejects (returns an error for) any response with `len(steps) != 1` or
   `steps[0].kind != "transaction"`** -- before doing anything else. This
   is the enforced precondition from §4/§11: if Relay's route ever starts
   requiring an approval step, ChainRoute refuses the quote outright
   rather than silently attempting an unplanned second transaction.
5. Validates the echoed response against the request -- `details.
   currencyIn.currency.chainId`/`address` match `req.SourceChainID`/the
   resolved native address; `details.currencyOut.currency.chainId`/
   `address` match `req.DestinationChainID`/the resolved WETH address --
   the same defense-in-depth Across's provider already applies, now
   applied to Relay's differently-shaped response.
6. `InputAmountBaseUnits = req.AmountBaseUnits`; **`OutputAmountBaseUnits =
   details.currencyOut.minimumAmount`** (the worst-case, on-chain-enforced
   floor -- not `expectedAmount`, per the brainstorming decision in §2:
   using the optimistic figure would let a genuinely riskier Relay quote
   look artificially cheaper than Across in Dijkstra's comparison, since
   Across's own `outputAmount` is also a firm commitment, not an
   estimate). `FeeBaseUnits = InputAmountBaseUnits - OutputAmountBaseUnits`,
   via `math/big`, matching `across.Provider`'s convention exactly.
7. `EstimatedFillTimeSec = details.timeEstimate`.
8. `RawProviderPayload` marshals exactly what `relay.Provider.
   BuildTransaction` (§8) will need back: `{to, value, data, chainId, gas,
   maxFeePerGas, maxPriorityFeePerGas, requestId, deadline, recipient}` --
   the single step's transaction fields plus the two identifiers
   (`requestId` for status tracking, `deadline` for the freshness
   discipline in §11) and the quoted recipient (for the consistency check
   in §8).

## 8. Go ↔ C++ routing flow

Unchanged from Phase 8, confirmed by reading `router/` and
`cpp-routing-service/` directly (§1) -- `CandidateEdge` is already
`repeated`, `Graph`/`findCheapestRoute` already support parallel edges.
The handler now simply appends up to two `CandidateEdge`s (one per
provider that returned `Available: true`) instead of at most one. No proto
change, no C++ change.

## 9. Dijkstra/provider selection behavior

Unchanged algorithm; two behavioral facts confirmed by reading
`route.cpp` directly (not assumed):

- **Selection**: `findCheapestRoute` relaxes edges with strict `<`, so the
  lowest-`fee` `CandidateEdge` wins, full stop -- no provider name is ever
  special-cased in C++, satisfying "no provider priority may override
  Dijkstra's result" as a direct code-reading fact, not a promise.
- **Tie-break on exactly equal fees**: resolves to whichever edge is
  earlier in `edgesFrom(u)`, which is insertion order, which is the order
  Go appended `candidateEdges`, which (per §5's index-ordered aggregation)
  is **provider registration order in `cmd/server/main.go`**. This is a
  mechanical artifact of existing iteration order, already fully
  deterministic, and Phase 9 adds nothing on top of it -- per your explicit
  instruction not to introduce a business preference disguised as a
  tie-break.

## 10. Persistence and invariants

**No schema change to `payment_quotes`** -- `UNIQUE(payment_id)` already
enforces "exactly one selected quote per payment" as a database invariant
(migration 0005, unchanged). **No new persistence mechanism for the
three-way provider-consistency invariant** (§1) -- it already holds by
construction (one transaction, one `winningQuote.ProviderName` value
feeding all three columns). Phase 9's job here is exactly one new
integration test: create a payment where Relay wins, assert
`payments.bridge_provider`, `payment_route_hops.bridge_name`, and
`payment_quotes.provider` are all `"relay"` and none is `"across"` --
proving the existing mechanism actually discriminates between two *real*
providers, not just a fake-named placeholder (the only case Phase 8 could
test).

**Unselected quotes are never persisted**, confirmed as already true
today (`payment_quotes` only ever receives the winning quote, per §1) --
no design change needed, only stated as an explicit guarantee (§29).

## 11. Relay execution model

Covered in full detail during brainstorming (§3); summarized precisely
here as the design-of-record:

- `relay.Provider.BuildTransaction(ctx, freshQuote)` decodes
  `freshQuote.RawProviderPayload` and returns
  `quote.TxEnvelope{To: <decoded to>, Value: <decoded value>, Data:
  <decoded data (hex-decoded)>, ChainID: <decoded chainId>}` -- an
  *unsigned* envelope. It does not sign anything.
- `across.Provider.BuildTransaction(ctx, freshQuote)` is `execute.go`'s
  existing calldata-packing logic (unchanged: `spokePoolABI.Pack(...)`),
  with the signing step removed -- returns
  `quote.TxEnvelope{To: e.SpokePoolAddress, Value: freshQuote.
  InputAmountBaseUnits, Data: calldata, ChainID: e.OriginChainID}`.
- **`Executor` is the sole caller of `wallet.SignTx`**, for both
  providers, uniformly. Before calling it, `Executor` validates the
  envelope (§12) -- this is the actual trust-boundary enforcement
  mechanism, not documentation.
- **Freshness discipline is unchanged and requires no special-casing**:
  Relay's quote (like Across's) changes every call and carries its own
  `deadline` (`orderData.deadline`, decoded from `RawProviderPayload`).
  Because `Executor`, not either provider, decides *when* to call
  `GetQuote` (always immediately before signing, on every attempt
  including resume -- unchanged from Phase 8), this generalizes
  automatically.

**Why signing stays centralized in `Executor` rather than delegated to each
provider's own signer**: this is what makes "never blindly sign
arbitrary provider-supplied calldata" an enforced architectural property
instead of a per-provider promise. If each provider signed its own
transaction, the validation checkpoint would need to be duplicated
correctly in every provider implementation, and a future provider could
simply skip it. Centralizing means there is exactly one place in the
entire codebase where a private key touches a transaction, and exactly one
place where envelope validation must be (and is) correct.

## 12. Execution-time validation / slippage, and the trust boundary

**The envelope-validation checkpoint** (new code in `Executor`, runs
between `BuildTransaction` and `wallet.SignTx`, for both providers
uniformly):

```go
func validateEnvelope(env quote.TxEnvelope, e *Executor, quoteRow payment.Quote) error {
    if env.ChainID != e.OriginChainID {
        return fmt.Errorf("envelope chainId %d does not match configured origin chain %d", env.ChainID, e.OriginChainID)
    }
    expectedTo, ok := e.ExpectedContractByProvider[quoteRow.Provider]
    if !ok || env.To != expectedTo {
        return fmt.Errorf("envelope target %s does not match pinned contract for provider %q", env.To.Hex(), quoteRow.Provider)
    }
    expectedValue, _ := new(big.Int).SetString(freshQuote.InputAmountBaseUnits.String(), 10) // the SAME value already passed the slippage check above
    if env.Value.Cmp(expectedValue) != 0 {
        return fmt.Errorf("envelope value %s does not match validated input amount", env.Value)
    }
    return nil
}
```

**Why `Executor.ExpectedContractByProvider` must be independently configured, not derived from the provider that built the envelope**: this map is populated in `cmd/worker/main.go` from the same environment constants each provider *also* uses internally (`SpokePoolAddress` for Across, `RELAY_DEPOSIT_CONTRACT_SEPOLIA` for Relay) -- but it is a **separate** copy, read from a separate source, not looked up on the `quote.Signer` instance that just returned the envelope. If `Executor` instead asked the same provider "what address did you expect?", the check would compare a value against itself and could never catch a bug or a compromised/misbehaving provider implementation. Independence is what makes this a real check rather than a tautology -- the duplication (the same address configured twice) is deliberate, not an oversight to DRY away.

**Per-provider trust characterization, stated plainly, not hedged:**

- **Across**: no real trust question. Every byte of `Data` is
  self-constructed by ChainRoute from a public, audited ABI using only
  values ChainRoute already validated. The envelope check for Across is a
  should-be-unreachable consistency assertion (this generalizes the
  existing route/SpokePool check already added during Phase 8's final
  review, §1) -- not new risk mitigation, just a uniform code path.
- **Relay**: genuinely different. `Data` is Relay's own opaque
  intent-encoding -- not independently re-derivable from a public ABI.
  The envelope check's three components (`ChainID`, pinned `To`, exact
  `Value`) bound the **blast radius** of trusting that opaque `Data`: even
  if `Data` encoded something semantically wrong, it can only ever move
  exactly `Value` (already slippage-validated) to a contract ChainRoute
  independently confirmed is Relay's real, expected deposit contract, on
  the chain ChainRoute expects. **`Data`'s semantic correctness is not,
  and cannot be, independently verified** -- this is the accepted residual
  trust boundary, stated explicitly rather than implied away by a shared
  interface name. It is why Relay integration is scoped to a
  well-audited, narrow route (§23) rather than presented as
  general-purpose arbitrary-provider execution.
- **Recipient consistency**: `relay.Provider.GetQuote` requests the quote
  with `user = e.Wallet.Address` (self-bridging, matching Across's
  `Recipient: e.Wallet.Address` pattern exactly), and
  `relay.Provider.BuildTransaction` additionally cross-checks any
  recipient/refund-recipient fields visible in the decoded payload against
  `e.Wallet.Address` before returning the envelope -- a second,
  independent check on top of the `To`/`Value`/`ChainID` bounds, specific
  to "am I sending my own funds to my own eventual recipient."

**Slippage mapping for Relay**: no code change to
`exceedsSlippageTolerance` -- it already operates on `*big.Int` fee
values regardless of source. The only change is *what* `relay.Provider.
GetQuote` puts into `FeeBaseUnits` (§7: derived from `minimumAmount`, the
worst-case-guaranteed figure) so that the comparison at both routing time
and execution time is "worst-case-guaranteed fee vs. worst-case-guaranteed
fee" for both providers -- economically comparable, not just
type-comparable.

## 13. Nonce/signing model

**Unchanged, and already correct across providers by construction** --
confirmed, not redesigned. `wallet_nonces` is keyed by `wallet_address`
only; `TryCreateExecution`'s atomic nonce-allocation-plus-row-creation
transaction never inspects provider identity. Phase 9 adds exactly one new
integration test (§25) proving two interleaved `ExecuteTestnetPayment`
calls -- one resolving to Across, one to Relay, same wallet -- still
allocate strictly sequential, unique nonces regardless of interleaving.
Every other Phase 7/8 guarantee (signed-bytes-before-broadcast,
hash-lookup-before-rebroadcast, identical-bytes rebroadcast, no second
distinct transaction for one execution identity) is provider-agnostic
today because `broadcastWithRecovery` operates purely on
`exec.SignedTxHash`/`exec.RawSignedTx` -- bytes are bytes, regardless of
which provider's envelope produced them.

## 14. Provider-specific reconciliation

`Reconciler.checkAndUpdateOutcome`, rewritten to use `quote.StatusChecker`
instead of `r.Across` directly:

```go
func (r *Reconciler) checkAndUpdateOutcome(ctx context.Context, exec payment.Execution) error {
    // ... existing MarkSubmitted repair, existing origin-receipt check
    // (unchanged -- provider-agnostic already, operates on OriginClient) ...

    checker, ok := r.StatusCheckers[exec.BridgeProvider]
    if !ok {
        return fmt.Errorf("execution %s uses provider %q with no configured status checker", exec.ID, exec.BridgeProvider)
    }
    result, err := checker.CheckStatus(ctx, quote.StatusRequest{
        ProviderReferenceID: derefOrEmpty(exec.ProviderReferenceID),
        OriginTxHash:        *exec.SignedTxHash,
    })
    if err != nil {
        return nil // transient API error -- not a failure signal (§16), retry next sweep
    }

    switch result.State {
    case quote.StateFilled:
        return r.markTerminal(ctx, exec, result, payment.StatusCompleted)
    case quote.StateRefunded, quote.StateReverted, quote.StateFillFailed:
        return r.markTerminal(ctx, exec, result, payment.StatusFailed)
    default: // quote.StatePending
        return nil
    }
}
```

- `across.Provider.CheckStatus` wraps the existing `DepositStatusByTxHash`
  call (using `req.OriginTxHash`, ignoring `ProviderReferenceID`) and maps
  Across's four status strings onto the shared `ExternalState` enum
  exactly as `checkAndUpdateOutcome`'s existing `switch` already does
  (`filled→Filled`, `expired|refunded→Refunded`, `pending→Pending`) --
  this is a direct, mechanical extraction of logic that already exists,
  not new logic.
- `relay.Provider.CheckStatus` calls `GET /intents/status?requestId=
  <ProviderReferenceID>` and maps Relay's eight status strings (§2) onto
  the same shared enum: `{waiting, depositing, pending, submitted,
  delayed}→Pending`, `success→Filled`, `refund→Refunded`,
  `failure→StateFillFailed` (§16 explains why this needs a value Across
  never did).
- **Why `StatusResult.RawStatus` carries the provider's own literal
  string, not just the normalized enum**: observability requirement #15
  asks "what terminal destination-side status was observed" -- collapsing
  Relay's eight-value vocabulary into five shared buckets for routing
  logic must not also destroy the detail an operator needs when debugging
  ("`delayed`" vs. plain "`pending`" is operationally meaningful even
  though both map to `StatePending`). `RawStatus` is persisted (§16) and
  logged (§21) specifically to preserve this.

**COMPLETED semantics, restated precisely for Relay**: `payments.status`
becomes `COMPLETED` only when `relay.Provider.CheckStatus` observes
Relay's own `"success"` -- per Relay's documentation, this specifically
means "the Relay Solver successfully executed the Fill Tx, and the funds
have reached the recipient" on the **destination** chain. This is
deliberately the same strictness level as Across's `"filled"` (a real
`FilledRelay` event on the destination chain) -- neither "quote accepted,"
nor "origin transaction broadcast," nor "origin transaction mined," nor
"Relay's API accepted the intent" is ever sufficient for either provider.

## 15. Payment/execution state machines

**Unchanged.** `payments.status`
(`ROUTED→PROCESSING→SUBMITTED→COMPLETED/FAILED`) and
`payment_executions.external_status`'s role within the `SUBMITTED` window
are exactly Phase 7/8's state machines -- Phase 9 adds one new
`external_status` value (`fill_failed`, §16) and one new `payments.
failure_reason` value is *not* needed (the reasons already added in Phase 8 --
`routing_quote_expired`, `fee_slippage_exceeded`, `route_unavailable`,
`amount_exceeds_guardrail` -- are all pre-nonce-allocation reasons,
provider-agnostic already; no Relay-specific pre-nonce reason is needed
since §7's rejection paths reuse the same reasons uniformly).

## 16. PostgreSQL schema changes

New migration `0006_multi_provider_bridge_routing.sql`:

```sql
ALTER TABLE payment_executions
    RENAME COLUMN across_deposit_id TO provider_reference_id;

ALTER TABLE payment_executions
    ADD COLUMN raw_external_status TEXT NULL;

ALTER TABLE payment_executions
    DROP CONSTRAINT payment_executions_external_status_check,
    ADD CONSTRAINT payment_executions_external_status_check
        CHECK (external_status IN ('pending', 'filled', 'expired', 'refunded', 'reverted', 'fill_failed'));
```

**Why rename `across_deposit_id` rather than add a new column**: confirmed
unused by any current write path (§1) -- Across's reconciliation has
always keyed on the origin transaction hash, never populated this column.
Renaming it (rather than leaving it dead and adding
`relay_request_id` alongside) keeps exactly one nullable "provider's own
external reference ID" column with one clear, generic meaning, and
Across simply continues to leave it `NULL` -- no data migration needed for
existing rows, no application code ever read the old column, confirmed by
grep.

**Why `raw_external_status` as a new column, not reused JSONB**: this is
one short string (Relay's or Across's own literal status word), read
purely for observability (§21) -- a new nullable `TEXT` column is simpler
than threading it through `unsigned_tx_params`'s existing JSONB blob,
which has a different, execution-construction-time purpose.

**Why exactly one new `CHECK` value (`fill_failed`), not a full vocabulary
rewrite**: Relay's `"failure"` ("unsuccessful fill") is a genuinely new
failure *category* -- distinct from `reverted` (an on-chain origin
revert, `receipt.Status == 0`, which Across's flow already produces
independently of any provider status call) and distinct from `refunded`/
`expired` (Across's own specific terminal-failure vocabulary, which
keeps its exact existing meaning for Across rows and is simply not
produced by Relay's mapping). Adding one value preserves every existing
row's meaning unchanged (backward compatible, §26) while giving Relay's
one genuinely new failure mode its own accurate name instead of
overloading an existing one.

**`Store.PersistSignedExecution` signature change** (interface, not
schema): `PersistSignedExecution(ctx, executionID, rawTx, txHash string,
providerReferenceID *string)` -- the `provider_reference_id` is written in
the **same** `UPDATE` as `raw_signed_tx`/`signed_tx_hash`, not a separate
call. This is a deliberate crash-point elimination (§17): Relay's
`requestId` is known from the moment the fresh quote was fetched (before
signing even happens), so there is no reason to ever have a durable state
where signed bytes exist but the reference ID needed to reconcile them
does not. Across's call sites simply pass `nil`.

## 17. Failure and crash-point analysis

Extending Phase 7/8's table with the points your instructions specifically
asked about. Every row's "second transaction possible?" answer is "no,"
for the same structural reason as Phase 7/8: nothing here changes
*when* nonces are allocated or *when* signed bytes are persisted relative
to broadcast, only *which provider's* envelope produced those bytes.

| Point | Durable DB state | External state | Restart behavior | 2nd tx possible? |
|---|---|---|---|---|
| After Relay quote, before routing | none (quote was never persisted -- Dijkstra hadn't run yet) | none | Caller retries the whole `POST /payments`; a fresh quote is fetched | No |
| After routing, before persistence | Either fully committed or fully rolled back -- `CreateOrGetPayment`'s existing one-transaction guarantee (Task 9's fault-injection test, provider-agnostic already) | none | Retry sees no payment row (rolled back) or the committed one (idempotency key) | No |
| After persistence, before Kafka publication | `payments`/`payment_quotes`/`payment_route_hops` committed, outbox row unconsumed | none | Unchanged Phase 6 outbox-publisher retry | No |
| After execution-time validation (slippage/expiry passed), before nonce allocation | `PROCESSING`, no execution row | none | Crash here loses nothing durable; a redelivery or reconciler sweep re-runs validation from scratch | No |
| After nonce allocation | `payment_executions` row with `nonce`, `bridge_provider` set, no signed bytes | none | Resume re-fetches a fresh quote for the same provider and signs (§11); crash point B, unchanged mechanism | No |
| After signing, before broadcast | `raw_signed_tx`, `signed_tx_hash`, **and `provider_reference_id`** all persisted together (§16) | none (never sent) | Broadcast the persisted bytes; the reference ID is already durable, so reconciliation is never blocked on it | No |
| During ambiguous broadcast | signed tx persisted | ambiguous | Hash-lookup-first recovery (unchanged) -- works identically for either provider's transaction, since it operates on the origin chain, not the provider's API | No |
| After origin confirmation, waiting for destination completion | `SUBMITTED`, `external_status='pending'` | origin confirmed, destination pending | Reconciler polls `StatusChecker.CheckStatus` using `provider_reference_id` (Relay) or `signed_tx_hash` (Across) -- unchanged polling cadence, generalized dispatch | No |
| Worker restart during Relay execution | Whatever `payment_executions` row state existed at restart | whatever it was | `DriveExecutionForward`'s existing `SignedTxHash != nil` branch (§1, unchanged) already handles this uniformly -- Relay's execution is, from `Executor`'s point of view, just "another envelope," fully inside the existing state machine | No |
| Provider API outage during reconciliation | `SUBMITTED`, unchanged | unknown (API down) | `CheckStatus` error is non-terminal (§14's `return nil`) -- retried next sweep, exactly Across's existing "transient API error" handling, now enforced identically for both providers via the shared interface | No |

## 18. Concurrency/race analysis

- **Concurrent provider quote requests**: covered in full in §5 --
  index-ordered result slice, no shared mutable state, no lock needed, no
  goroutine outlives `wg.Wait()`.
- **Duplicate `POST /payments`**: unchanged Phase 5 idempotency-key
  behavior -- the pre-routing `LookupByIdempotencyKey` check runs before
  any provider is even queried, exactly as it already does for the
  existing single routing RPC call.
- **Kafka redelivery**: unchanged -- `ClaimPayment`'s `WHERE status =
  'ROUTED'` guard is provider-agnostic; a redelivered event for a
  Relay-selected payment no-ops identically to an Across-selected one.
- **Multiple workers**: unchanged -- `TryCreateExecution`'s
  `UNIQUE(payment_id)` race arbitration never depended on provider
  identity.
- **Reconciler/executor races**: unchanged mechanism (§17's crash-point
  table); the only new race surface is two different `StatusChecker`
  implementations being called from different goroutines for different
  executions concurrently -- each is a pure, independent HTTP read with no
  shared state, so no new synchronization is needed.
- **Two payments executing through different providers from the same
  wallet**: **new test, no code change** (§13) -- proves
  `wallet_nonces`' atomic `UPDATE` already serializes correctly regardless
  of which provider's execution path is racing which.
- **The wallet nonce allocator remains correct regardless of provider**:
  restated as an explicit guarantee (§29) because it's the one property
  your instructions asked to be re-confirmed, not re-designed.

## 19. Security and trust boundaries

Threat-modeled specifically for Relay, per your instruction #14:

| Threat | Mitigation |
|---|---|
| Malicious/malformed quote response | `relay.Provider.GetQuote` validates echoed chain IDs/addresses (§7) before normalizing anything; malformed JSON is a decode error, isolated per-provider (§5) |
| Malicious transaction calldata | Not independently verifiable (disclosed, §12) -- bounded by the envelope check's `To`/`Value`/`ChainID` pins, not eliminated |
| Incorrect transaction target | `To` pinned against a configured, live-verified expected contract address per chain; mismatch is a hard error, never signed (§12) |
| Wrong chain ID | `ChainID` checked against `e.OriginChainID` exactly; mismatch is a hard error (§12) |
| Wrong token | `relay.Provider.GetQuote`'s echo validation (§7) catches this before a `Quote` is even constructed |
| Excessive transaction value | `Value` checked against the already-slippage-validated `freshQuote.InputAmountBaseUnits` exactly (§12) |
| Unexpected approval transaction | `len(steps) != 1` or wrong `kind` is rejected at quote-normalization time (§7), before any envelope is ever built |
| Stale quote | Unchanged freshness discipline (§11) -- always re-fetched immediately before signing, every attempt |
| Fee manipulation | Slippage check (§12) uses the worst-case-guaranteed `minimumAmount`-derived fee, both at routing time and execution time |
| Redirect/host configuration mistakes | `RELAY_TESTNET_API_URL` has a hardcoded, non-empty default matching the live-verified host, mirroring `ACROSS_TESTNET_API_URL`'s existing pattern -- no silent fallback to an unexpected host |
| Leaking API credentials | `RELAY_API_KEY` (if/when required, §2) follows the exact environment-only, never-logged handling already established for `ACROSS_API_KEY`/`TESTNET_WALLET_PRIVATE_KEY` |
| Signing arbitrary provider-generated transactions | This is the core question §12 answers: ChainRoute does sign Relay-generated `Data`, but only after independently bounding `To`/`Value`/`ChainID` -- stated as an accepted, scoped residual trust, not resolved away |

## 20. Simulated-mode compatibility

Unchanged guarantee, extended to cover the new provider: no code path
reachable when `execution_mode=simulated` touches `quote.Registry`,
`relay.Provider`, or any `RELAY_*` environment variable -- confirmed by
the same gating pattern Phase 8 already established
(`mode == payment.ExecutionModeTestnet`, `blockchainEnv == "testnet"`).
`relay.Provider`'s construction in `cmd/server`/`cmd/worker` sits inside
the exact same `if blockchainEnv == "testnet"` blocks `across.Provider`
already lives in -- no new top-level gate is introduced, the existing one
just registers one more provider.

## 21. Observability

Extends Phase 8's logging fields (`payment_id`, `execution_id`,
`bridge_provider`, `tx_hash`) with exactly what your instructions listed:
which providers were queried and which returned viable quotes (logged per
`quoteResult`, §5); each provider's returned fee (logged alongside the
quote); which provider Dijkstra selected and why another was excluded
(the C++ response's winning `bridge_name`, cross-referenced against
`quotesByBridgeName`'s other entries, logged together); which provider was
actually executed (`exec.BridgeProvider`, already a column); routing-time
vs. execution-time fee (both already available at the slippage-check
call site, §12, logged together whenever that check runs); whether
slippage validation passed (already logged today, unchanged); the
external transaction/request identifier
(`signed_tx_hash`/`provider_reference_id`, both now persisted, §16); the
observed terminal destination-side status (`RawStatus`, §14, logged at
`markTerminal`). Never logs secrets or raw signed transaction bytes --
unchanged rule, unchanged enforcement (no logging call anywhere touches
`RawSignedTx` or any private key material).

## 22. Unit tests

- `relay.Provider.GetQuote`: happy path against a captured live fixture
  (§2's actual response, hand-fixed for determinism); `NO_SWAP_ROUTES_
  FOUND` → `Available: false`, not an error; multi-step or wrong-`kind`
  response → hard error; echoed-chain/address mismatch → hard error;
  malformed JSON → decode error.
- `relay.Provider.BuildTransaction`: decodes a fixture `RawProviderPayload`
  into the exact expected `TxEnvelope`; hex-decoding of `data`/`value`
  edge cases (leading zeros, odd-length hex).
- `relay.Provider.CheckStatus`: all eight Relay status strings map to the
  correct shared `ExternalState`; unrecognized status string treated as
  `Pending` with a logged warning (mirroring Across's existing
  `default:` case).
- `quote.Registry` with two providers registered for one route:
  `ProvidersFor` returns both, in registration order.
- Concurrent quote aggregation (§5): a table-driven test with fake
  providers returning success/error/unavailable in every combination
  from your failure-isolation matrix (§ brainstorming §2), asserting the
  exact resulting HTTP status and `candidateEdges` contents for each.
- Fee normalization: `relay.Provider.GetQuote`'s `minimumAmount`-based
  `FeeBaseUnits` computation against a fixture with a real slippage band.
- `exceedsSlippageTolerance`: unchanged, no new tests needed (it already
  operates on opaque `*big.Int`s).
- `Executor`'s envelope-validation checkpoint: wrong `ChainID`, wrong
  `To`, wrong `Value` each independently rejected before `wallet.SignTx`
  is ever called (assert the fake eth client's send method is never
  invoked) -- for both a fake Across-shaped and a fake Relay-shaped
  envelope, proving the check is provider-agnostic.

## 23. C++ tests

No C++ source changes (§8), but new test coverage in
`cpp-routing-service/tests/routing_service_test.cpp` proving the *specific
scenarios* your instructions listed, using two real-shaped candidate
names (`"across"`, `"relay"`) instead of Phase 8's placeholder
`"other-provider"`:

- Two parallel edges, Across cheaper → selected hop is `"across"`.
- Two parallel edges, Relay cheaper → selected hop is `"relay"`.
- Equal fee → selected hop is whichever was appended first (asserting the
  documented, non-business tie-break from §9, not a new behavior).
- One candidate has `liquidity < amount` (the `Available: false → 0`
  mapping from Phase 8) → the other is selected, no route-found=false.
- Both candidates have `liquidity < amount` → `route_found = false`,
  `hops_size() == 0`.

## 24. PostgreSQL/Kafka integration tests

**PostgreSQL:**
- Selected-quote persistence: a payment routed with both providers
  available, Relay cheaper, persists exactly one `payment_quotes` row
  with `provider = 'relay'`.
- Provider consistency: the same payment's `payments.bridge_provider`,
  `payment_route_hops.bridge_name`, and `payment_quotes.provider` are all
  `'relay'` (§10's new test).
- No unselected quote becomes execution state: the Across quote that
  *lost* is never written anywhere (`payment_quotes`, `payment_executions`
  both have zero Across-provider rows for this payment).
- Slippage before nonce allocation: unchanged test pattern from Phase 8,
  re-run against a Relay-selected payment -- zero `payment_executions`
  rows, zero nonces consumed, on a forced slippage rejection.
- **Nonce uniqueness across Across + Relay executions** (§13, §18): N
  goroutines racing `TryCreateExecution` for a mix of Across-selected and
  Relay-selected payments on the same wallet -- assert every allocated
  nonce is unique and strictly sequential, regardless of provider mix.
- Crash/recovery: a signed-but-unbroadcast Relay execution row (with
  `provider_reference_id` already persisted per §16's atomic-write
  guarantee) is correctly identified as needing a rebroadcast check on
  restart, identically to an Across row.

**Kafka:** duplicate delivery of a `PAYMENT_ROUTED` event for a
Relay-selected payment is a safe no-op past the first successful claim --
identical assertion to Phase 6's existing test, re-run with
`execution_mode=testnet` and a Relay-winning fixture instead of Across.

## 25. Real testnet integration/E2E plan

Extending Phase 7/8's `testnet_integration` build tag + `RUN_TESTNET_
TESTS=1` gate (unchanged gating mechanism):

- One real `relay.Provider.GetQuote` call against the actual live testnet
  API, asserting a real, structurally-valid response (not a fixture).
- One real dual-provider routing call: both Across and Relay quoted live,
  both passed to the real C++ service, asserting `route_found = true` and
  the winning hop's fee is the lower of the two *actual* returned fees --
  **not asserting which provider wins**, per your explicit instruction
  ("do not make the real test depend on one provider always being
  cheaper"). The assertion is comparative (`selected.fee ==
  min(acrossFee, relayFee)`), not fixed to a name.
- Whichever provider the real run selects, execute and reconcile through
  it for real: if Across wins, this is exactly Phase 7/8's existing gated
  smoke test; if Relay wins, the same smoke script now also exercises a
  real Relay deposit → real `intents/status` poll → `COMPLETED` only on a
  real observed `"success"`.
- `scripts/e2e_testnet_test.sh` extended with a branch on the actual
  `GET /payments/{id}` response's `bridge_provider` field, asserting the
  execution-side fields present are consistent with whichever provider
  won (e.g. a Relay-won payment's `external_tx_hash` is the origin
  deposit transaction, not a Relay-internal identifier).

## 26. Backward compatibility

- Across-only execution continues to work unchanged: every Phase 7/8 test
  that never registers a second provider exercises exactly the same code
  paths as before, since `Executor`/`Reconciler` still dispatch by
  persisted provider name -- with one provider registered, that's always
  Across.
- Existing simulated-mode tests: unchanged, unaffected (§20).
- Existing Phase 7/8 persisted payments: fully readable after migration
  `0006` -- the `across_deposit_id → provider_reference_id` rename
  preserves every existing row's data (it was always `NULL` for existing
  rows, §16, so the rename changes nothing observable); the widened
  `external_status` `CHECK` constraint is strictly additive (every value
  a Phase 8 row could already hold remains valid).

## 27. Explicit guarantees

- **Real multi-provider quote comparison.** For a supported route, both
  Across and Relay are queried live, concurrently, and independently
  (§5) -- not sequentially, not with one hardcoded as a fallback.
- **C++ remains the sole routing authority.** Confirmed by reading
  `route.cpp` directly (§9) -- no provider name is special-cased anywhere
  in the algorithm; Go never re-implements cost comparison.
- **The persisted route determines the execution provider, always.**
  `Executor` reads `payment_quotes.provider` and looks it up in a
  provider map; an unconfigured provider name is a hard error, never a
  substitution (unchanged mechanism from Phase 8, now meaningfully
  exercised with two real options).
- **The worker never independently re-routes.** No code path in
  `Executor`/`Reconciler` calls a second provider's quote for comparison
  purposes, or falls back to a different provider after a failure --
  confirmed by the envelope-validation design (§12), which only ever
  validates against the *one* provider named in the persisted quote.
- **Provider failure isolation.** One provider's outage never prevents
  routing through a healthy second provider (§5's failure matrix) --
  and, symmetrically, a total outage of all providers is never confused
  with "no route exists" (503 vs. 422, §5).
- **No routine fee rejection burns a nonce.** Unchanged Phase 8 guarantee,
  provider-agnostic already (§13) -- re-verified, not re-designed.
- **Crash-safe transaction identity, for either provider.** Signed bytes
  persisted before broadcast, ambiguous broadcasts resolved by hash
  lookup first, no recovery path ever constructs a second distinct
  transaction for one execution identity (§17) -- unchanged mechanism,
  now generalized to whichever provider's envelope produced the bytes.
- **Destination-side completion semantics, for either provider.**
  `COMPLETED` means a verified destination-chain fill (`"filled"` for
  Across, `"success"` for Relay) -- never an origin-side or API-acceptance
  signal, for either provider (§14).
- **Simulated-mode determinism, unaffected.** Zero network calls, zero
  new environment variables, for any simulated-mode or standard test
  suite (§20).

## 28. Explicit non-goals and limitations

- **Relay's opaque calldata is not independently verified**, only bounded
  (`To`/`Value`/`ChainID`) -- disclosed explicitly (§12, §19), not solved.
- **Multi-step Relay execution (e.g. an approval transaction) is
  explicitly rejected, not supported.** If Relay's route ever requires
  more than one origin-chain transaction, `relay.Provider.GetQuote`
  refuses the quote (§7) rather than ChainRoute attempting a two-tx flow.
  The one-execution-row-per-payment model is **not** redesigned in this
  phase because verified research shows it doesn't need to be for the
  one route Phase 9 targets -- this is stated as a scoping decision made
  with evidence, not an assumption papered over.
- **A third provider is out of scope.** The abstraction (§6) is proven
  by two real implementations, not designed for an arbitrary N without
  further validation.
- **Multi-hop real execution, multi-asset routing, a price oracle, USD
  normalization, Redis, a new microservice, a new message broker, a
  frontend, wallet/key-management redesign, automatic provider failover
  after routing, and automatic re-routing after payment creation** are
  all out of scope, unchanged from Phase 8's own non-goals and not
  reopened here.
- **No mainnet support anywhere**, unchanged.
- **The `RELAY_API_KEY` requirement's exact scope and enforcement date
  are unconfirmed** (§2) -- Task 1 of the implementation plan re-verifies
  this live, exactly as Phase 7 re-verified Across's auth requirements
  before implementation proceeded.
- **The pinned Relay deposit-contract address was only verified stable
  across two identical calls with one fixed `user` address** -- Task 1
  re-verifies with a varying `user` to rule out a per-request address
  before this is trusted as a hard security pin.

## 29. File-by-file implementation plan

| File | Change |
|---|---|
| `go-api/internal/bridge/quote/execute.go` (new) | `TxEnvelope`, `Signer` interface |
| `go-api/internal/bridge/quote/status.go` (new) | `ExternalState`, `StatusRequest`, `StatusResult`, `StatusChecker` interface |
| `go-api/internal/bridge/across/execute.go` | Split signing out of `BuildAndSignDepositV3Tx`; add `Provider.BuildTransaction` |
| `go-api/internal/bridge/across/status.go` | Add `Provider.CheckStatus` wrapping existing `DepositStatusByTxHash` |
| `go-api/internal/bridge/relay/client.go` (new) | Thin HTTP client for `api.testnets.relay.link`, mirrors `across/client.go` |
| `go-api/internal/bridge/relay/quote.go` (new) | Request/response types, `Provider.GetQuote` |
| `go-api/internal/bridge/relay/execute.go` (new) | `Provider.BuildTransaction` |
| `go-api/internal/bridge/relay/status.go` (new) | `Provider.CheckStatus` |
| `go-api/internal/handler/payments.go` | Concurrent quote aggregation (§5), replacing the sequential abort-on-first-error loop |
| `go-api/internal/worker/executor.go` | `signAndBroadcastFresh` → build via `quote.Signer`, validate envelope (§12), sign centrally; add `Executor.ExpectedContractByProvider map[string]common.Address`, populated independently of any provider instance (§12) |
| `go-api/internal/worker/reconciler.go` | `checkAndUpdateOutcome` → status via `quote.StatusChecker` map, not `r.Across` directly |
| `go-api/internal/postgres/store.go` / `execution_store.go` | `PersistSignedExecution` signature adds `providerReferenceID *string`; column rename support |
| `go-api/internal/payment/payment.go` | `Execution.ProviderReferenceID *string`, `Execution.RawExternalStatus *string`; `ExternalStatusFillFailed` |
| `go-api/migrations/0006_multi_provider_bridge_routing.sql` (new) | Per §16 |
| `go-api/cmd/server/main.go` | Construct `relay.Provider`, register in `quote.Registry` alongside Across |
| `go-api/cmd/worker/main.go` | Construct `relay.Provider`, add to `Executor.QuoteProviders`/new `Signer`/`StatusChecker` maps, pin `RELAY_DEPOSIT_CONTRACT_SEPOLIA` |
| `scripts/e2e_testnet_test.sh` | Extend per §25 |

## 30. Ordered implementation tasks with acceptance criteria

1. **Re-verify Relay's live API** (auth enforcement, `to` address
   stability across varying `user`, exact current response schema).
   *Accept*: a passing test proves the current live response matches (or
   an updated) fixture, and documents whether `RELAY_API_KEY` is required.
2. **Add `quote.Signer`/`quote.StatusChecker`; extract `across.Provider.
   BuildTransaction`/`CheckStatus` from existing code.** *Accept*: unit
   tests pass; every existing Phase 7/8 test still passes unmodified
   (this is a refactor of existing Across logic, not new behavior).
3. **Implement `bridge/relay` (quote, execute, status).** *Accept*: unit
   tests (§22) pass against both a fake HTTP server and the real
   live-verified fixture from Task 1.
4. **Migration `0006`.** *Accept*: applies cleanly on top of `0005`;
   existing Phase 1-8 integration tests still pass unmodified.
5. **Concurrent quote aggregation in `PostPayments`.** *Accept*: the
   failure-isolation matrix's every case (§ brainstorming §2) is covered
   by a passing test; existing single-provider tests still pass
   unmodified.
6. **C++ test coverage for two real-shaped parallel edges.** *Accept*:
   §23's five scenarios pass; no C++ source change required, confirmed by
   an empty diff outside `tests/`.
7. **`Executor`/`Reconciler` generalization.** *Accept*: envelope
   validation tests (§22) pass for both provider shapes; every existing
   Phase 7/8 executor/reconciler test still passes unmodified.
8. **`cmd/server`/`cmd/worker` wiring.** *Accept*: simulated mode requires
   zero new environment variables (§20), verified by running the existing
   local E2E script unmodified.
9. **PostgreSQL/Kafka integration tests** (§24), including the
   cross-provider nonce-uniqueness test.
10. **Extend `scripts/e2e_testnet_test.sh`** (§25).
11. **Full regression pass**: every Phase 1-8 test plus every new Phase 9
    test, all green in one run.
