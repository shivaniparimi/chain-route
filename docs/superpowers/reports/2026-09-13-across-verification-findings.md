# Across testnet verification findings (Task 1)

**Re-verified:** 2026-09-14T04:24 UTC (live-checked at plan time 2026-09-13; this is a same-day re-check per Task 1's brief)

**Verdict: everything the plan recorded in "Verified Across testnet facts" is still current. No drift found on any address, ABI, route, or auth behavior.** The one genuinely new information gathered here is the `/deposit/status` success-response shape (Step 2), which the plan explicitly left as an open item for this task to chase down — that item is now resolved with a primary-source citation, not a guess.

## Step 1: Re-verification of addresses, route, ABI, auth

All four checks from the brief were re-run live against `testnet.across.to` and the raw GitHub deployment JSON (not summarized page renders — fetched via `curl` and parsed with `python3 -c "json.load(...)"`, per the plan's own caution that summarized renders can truncate/mangle hex addresses).

- **Sepolia (chain id `11155111`) SpokePool**: `0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662` — **matches**, confirmed both from `raw.githubusercontent.com/across-protocol/contracts/master/deployments/sepolia/Ethereum_SpokePool.json` (`.address` field) and from the live `/suggested-fees` response's `spokePoolAddress` field.
- **Base Sepolia (chain id `84532`) SpokePool**: `0x82B564983aE7274c86695917BBf8C99ECb6F0F8F` — **matches**, confirmed both from `.../deployments/base-sepolia/Base_SpokePool.json` (`.address` field) and from `/suggested-fees`'s `destinationSpokePoolAddress` field.
- **Sepolia WETH**: `0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14` — **matches**, present as `inputToken.address` in the live `/suggested-fees` response and as `originToken` in `/available-routes`.
- **Base Sepolia WETH**: `0x4200000000000000000000000000000000000006` — **matches**, present as `outputToken.address` in `/suggested-fees` and as `destinationToken` in `/available-routes`.
- **Route Sepolia WETH → Base Sepolia WETH**: **still live and active**, confirmed via `GET /available-routes?originChainId=11155111&destinationChainId=84532`. Both variants the plan recorded are still present:
  - ERC-20 WETH route, `"isNative": false`
  - Native-ETH route sharing the same underlying WETH token address, `"isNative": true`
  - (The live route list today also shows three additional USDC/USDT routes on this chain pair that the plan didn't need and this task doesn't use — noted only for completeness, not acted on.)
- **`depositV3` ABI**: pulled fresh from the raw ABI JSON in both `Ethereum_SpokePool.json` (Sepolia) and `Base_SpokePool.json` (Base Sepolia). **Byte-for-byte identical** to the ABI the plan recorded — same 12 inputs in the same order and types (`depositor`, `recipient`, `inputToken`, `outputToken`, `inputAmount`, `outputAmount`, `destinationChainId`, `exclusiveRelayer`, `quoteTimestamp`, `fillDeadline`, `exclusivityDeadline`, `message`), no outputs, `stateMutability: "payable"`. The native-ETH-via-`payable` design decision the plan recorded (native ETH auto-wrapped when `inputToken` = WETH address and `msg.value = inputAmount`, avoiding a second `approve()` nonce) remains valid — nothing in the ABI or route data contradicts it.
- **`/suggested-fees`**: fresh live response shape is structurally identical to the one the plan captured (same top-level keys: `estimatedFillTimeSec`, `capitalFeePct`/`capitalFeeTotal`, `relayGasFeePct`/`relayGasFeeTotal`, `relayFeePct`/`relayFeeTotal`, `lpFeePct`, `timestamp`, `isAmountTooLow`, `quoteBlock`, `exclusiveRelayer`, `exclusivityDeadline`, `spokePoolAddress`, `destinationSpokePoolAddress`, the `total*Fee` nested objects, `limits`, `fillDeadline`, `outputAmount`, `inputToken`/`outputToken` objects, `id`). Only the numeric fee/timestamp/quoteBlock values differ, as expected for live market data captured at a different moment — no schema drift.
- **`/deposit/status` error-path behavior**: re-confirmed identical to the plan's findings:
  - No params → `HTTP 400`, `{"error":"IncorrectQueryParamsException","message":"Incorrect query params provided"}`
  - `originChainId=11155111&depositId=999999999` (nonexistent) → `HTTP 404`, `{"error":"DepositNotFoundException","message":"Deposit not found given the provided constraints"}`
  - `originChainId=11155111&depositTxHash=<fake hash>` → same `HTTP 404`/`DepositNotFoundException` shape. **`depositTxHash` is still a valid, accepted query parameter** — the plan's choice to key off our own `signed_tx_hash` via `depositTxHash` (instead of parsing `FundsDeposited` event logs for a numeric `depositId`) remains sound.
- **Auth**: re-confirmed no `Authorization` header or `integratorId` param is required — all of the above requests succeeded (or returned their expected validation/not-found errors, as opposed to an auth error) with no credentials sent. `ACROSS_API_KEY`/`ACROSS_INTEGRATOR_ID` should remain wired as optional, exactly as the plan specified.

**No STOP condition was triggered.** Nothing needed updating in the "Verified Across testnet facts" section; it is reproduced above with re-confirmation notes rather than corrections.

## Step 2: `/deposit/status` success-response shape (previously unresolved)

The plan's own research had left this open, noting the direct docs URL 404'd during planning and no real deposit had been observed. Re-running the same investigation:

- `https://docs.across.to/reference/get-deposit-status` still **404s**, confirming the plan's note that this exact path is wrong/stale.
- The correct current path is `https://docs.across.to/api-reference/deposit/status/get` (HTTP 200). This page is a Next.js-rendered docs page; rather than trust a summarized render of it (per the plan's stated concern about summarization dropping/mangling detail), the raw HTML was fetched with `curl` and grepped directly. The page embeds the endpoint's actual generated TypeScript response-type declaration as a literal string (a `Response` interface with full JSDoc), which is a primary-source artifact, not a paraphrase. The relevant excerpt, reproduced verbatim from that embedded declaration:

  ```ts
  export interface Response {
    /**
    * The status of the deposit.
    *  * filled: Deposits with this status have been filled on the destination chain and the recipient should have received funds. A FilledRelay event was emitted on the destination chain SpokePool.
    *  * pending: Deposit has not been filled yet.
    *  * expired: Deposit has expired and will not be filled. Expired deposits will be refunded to the `depositor` on the `originChainId` in the next batch of repayments.
    *  * refunded: Deposit has expired and the depositor has been successfully refunded on the originChain. For deposit-address flows, funds were returned to the refund address.
    *  * slowFillRequested: Deposit has been made but no relayer has filled the intent, therefore Across fills the user's intent without requiring a relayer to front the capital.
    *  * slowFilled: Deposit was completed via the protocol's slow fill path after a slow fill request.
    *  * deposit-pending: Funds have entered the system — e.g. arrived at a persistent deposit address but not been swept yet, or (HyperCore gasless withdrawals) the HyperCore → HyperEVM lift is in progress — but the bridge deposit has not executed yet.
    *  * deposit-failed: (gasless flows) execution failed before an onchain deposit was created; funds were not moved. Terminal.
    *  * auto-refund-pending: (deposit-address flows) the transfer cannot be bridged and is queued for automatic refund to the refund address.
    *  * manual-refund-required: (deposit-address flows) the transfer cannot be bridged and no automatic refund path exists; contact Across support to recover funds.
    *  * refund-failed: (deposit-address flows) automatic refund attempts failed. Terminal only after bounded retries are exhausted.
    */
    status?:
      'filled' | 'pending' | 'expired' | 'refunded' |
      'slowFillRequested' | 'slowFilled' |
      'deposit-pending' | 'deposit-failed' |
      'auto-refund-pending' | 'manual-refund-required' | 'refund-failed';

    fillTxnRef?: string | null;      // fill tx hash on destination chain; null until filled
    fillTx?: string | null;          // DEPRECATED alias of fillTxnRef
    destinationChainId?: number;
    originChainId?: number;
    depositId?: string | null;
    depositTxnRef?: string;          // origin deposit tx hash (or PDA funding tx for deposit-address flows)
    depositTxHash?: string;          // DEPRECATED alias of depositTxnRef
    depositRefundTxnRef?: string | null;
    depositRefundTxHash?: string | null; // DEPRECATED alias of depositRefundTxnRef
    sweepTxnRef?: string | null;
    actionsSucceeded?: boolean | null;
    pagination?: { currentIndex: number; maxIndex: number };
    actionsTargetChainId?: number | null;
    actionsTargetRecipient?: string | null;
    actionsTargetToken?: string | null;
    actionsTargetAmount?: string | null;
    actionsTargetTxnRef?: string | null;
    actionsTargetBlockTimestamp?: string | null; // date-time
  }
  ```

- **This is now resolved, not a residual unknown**: the exact enum spelling is `'filled' | 'pending' | 'expired' | 'refunded' | 'slowFillRequested' | 'slowFilled' | 'deposit-pending' | 'deposit-failed' | 'auto-refund-pending' | 'manual-refund-required' | 'refund-failed'` — 11 values total, considerably more than the 4 (`filled`/`pending`/`expired`/`refunded`) the plan anticipated. The extra 7 values are largely for flows this system doesn't use (Across's newer "deposit-address"/PDA and HyperCore gasless-withdrawal products, not the direct `depositV3` SpokePool call this plan uses), but they are real, documented values the live API can legitimately return for *other* deposit types, and in principle the API surface could evolve further.
- **What is still genuinely unverified**: a real, successful `filled` response body was **not** captured live in either this pass or the original planning pass — no actual funded testnet deposit was made in either case (this task's scope is verification-of-facts and dependency setup only, not executing a real bridge transaction). So while the *field names and enum spelling* above come from a primary-source (the generated TS interface embedded in the docs page), the *exact runtime values* of fields like `depositId`'s format or whether every field is always present have not been observed on a live response.
- **Actionable guidance for Task 10 (`status.go`)**: given (a) an enum with more members than originally assumed, and (b) no live-observed success body, `status.go` must do genuinely tolerant parsing:
  - Parse `status` into a Go string/enum type that does **not** fail decoding on an unrecognized value — treat any `status` string outside a known terminal/success set as equivalent to `pending` (i.e., "still in flight, keep polling"), and log a warning including the raw string when this happens.
  - Only treat `"filled"` as the success/terminal-success case. Treat `"expired"` and `"refunded"` as terminal-non-success. Treat everything else (`"pending"`, `"slowFillRequested"`, `"slowFilled"`, `"deposit-pending"`, and any unrecognized future string) as non-terminal/keep-polling — this system doesn't use deposit-address or HyperCore flows, so `deposit-failed`/`auto-refund-pending`/`manual-refund-required`/`refund-failed` should not occur for our `depositV3` calls in practice, but should still be handled defensively as non-crashing (log and treat conservatively as non-terminal-but-flagged, or terminal-non-success — Task 10's call) rather than causing a panic or decode error.
  - Prefer the non-deprecated field names (`fillTxnRef`, `depositTxnRef`, `depositRefundTxnRef`) over their deprecated aliases (`fillTx`, `depositTxHash`, `depositRefundTxHash`) when both may be present, but tolerate either being the only one populated, since the docs mark the short names deprecated without giving a removal date.

## Verified Across testnet facts (carried forward, unchanged)

- **Sepolia (chain id `11155111`) SpokePool**: `0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662`
- **Base Sepolia (chain id `84532`) SpokePool**: `0x82B564983aE7274c86695917BBf8C99ECb6F0F8F`
- **Sepolia WETH**: `0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14`
- **Base Sepolia WETH**: `0x4200000000000000000000000000000000000006` (standard OP-stack predeploy address)
- **Route**: Sepolia WETH → Base Sepolia WETH is live and active, both as ERC-20 (`isNative: false`) and native-ETH (`isNative: true`, same underlying WETH token address).
- **Design decision (unchanged)**: this plan uses the native-ETH path — `msg.value = inputAmount`, no `approve()` call — to keep one execution row to one nonce.
- **`depositV3` ABI** — 12 inputs (`depositor`, `recipient`, `inputToken`, `outputToken`, `inputAmount`, `outputAmount`, `destinationChainId`, `exclusiveRelayer`, `quoteTimestamp`, `fillDeadline`, `exclusivityDeadline`, `message`), no outputs, `payable`. Identical on both Sepolia and Base Sepolia deployments.
- **`/suggested-fees`**: no auth required; response includes `spokePoolAddress`, `destinationSpokePoolAddress`, fee breakdowns, `limits`, `fillDeadline`, `outputAmount`, token metadata, and a quote `id`.
- **`/deposit/status`**: `depositTxHash` is a valid, accepted query param (used instead of parsing `FundsDeposited` logs for a numeric `depositId`). No-params → 400 `IncorrectQueryParamsException`; unmatched deposit → 404 `DepositNotFoundException`. Success-response shape now documented above under Step 2.
- **Auth**: neither endpoint requires an `Authorization` header or `integratorId` on testnet today. `ACROSS_API_KEY`/`ACROSS_INTEGRATOR_ID` remain wired as optional.

## Sources

- `https://testnet.across.to/api/available-routes?originChainId=11155111&destinationChainId=84532` (live, unauthenticated)
- `https://testnet.across.to/api/suggested-fees?originChainId=11155111&destinationChainId=84532&inputToken=0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14&outputToken=0x4200000000000000000000000000000000000006&amount=1000000000000000` (live, unauthenticated)
- `https://raw.githubusercontent.com/across-protocol/contracts/master/deployments/sepolia/Ethereum_SpokePool.json` (raw JSON, parsed with `python3`/`json`)
- `https://raw.githubusercontent.com/across-protocol/contracts/master/deployments/base-sepolia/Base_SpokePool.json` (raw JSON, parsed with `python3`/`json`)
- `https://testnet.across.to/api/deposit/status` (live, error-path behavior only — no real deposit observed)
- `https://docs.across.to/api-reference/deposit/status/get` (raw HTML fetched with `curl`, embedded `Response` TypeScript interface extracted directly — not a summarized render)
