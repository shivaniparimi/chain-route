// Maps a testnet-mode payment's source chain to the block explorer that can
// show its real broadcast transaction. Deliberately hardcoded to the two
// chains the backend actually executes on today (see the verification note
// below) -- never derived from a generic "-sepolia" suffix pattern applied
// to arbitrary chain names, since that would silently produce a URL for a
// chain the backend never really broadcasts to.
const EXPLORER_BASE_URLS: Record<string, string> = {
  "ethereum-sepolia": "https://sepolia.etherscan.io/tx/",
  "base-sepolia": "https://sepolia.basescan.org/tx/",
};

export interface ExplorerLinkablePayment {
  execution_mode: string;
  source_chain: string;
  destination_chain: string;
  asset: string;
  external_tx_hash: string | null;
}

// Only ever links out for a testnet-mode payment on the one real corridor
// -- never guesses a mainnet explorer URL, never links for simulated-mode
// payments (their tx hash, if any, is not a real on-chain transaction).
//
// Keys off `source_chain`, not `destination_chain` -- verified against
// go-api/internal/worker/executor.go, not assumed:
//   - signAndBroadcastFresh (executor.go) calls e.OriginClient.SendTransaction
//     (the only SendTransaction call site in the executor) to actually put
//     the signed transaction on-chain. There is no "destination client"
//     that ever broadcasts anything -- DestChainID exists only as an int64
//     used for quote routing bookkeeping, never for broadcasting.
//   - Before that, it asserts `envelope.ChainID != e.OriginChainID` and
//     errors out otherwise (executor.go), so the broadcast is provably
//     scoped to the origin chain's ID.
//   - go-api/cmd/worker/main.go wires OriginClient to the Ethereum Sepolia
//     RPC client and sets OriginChainID: 11155111 (Ethereum Sepolia's chain
//     ID) / DestChainID: 84532 (Base Sepolia) -- i.e. "origin" in the
//     executor is genuinely the payment's source chain for the one real
//     corridor (see lib/chains.ts's hasRealTestnetExecution), not a
//     separately-tracked concept that happens to share a name.
// So `source_chain` is the correct key: the tx this function links to was
// broadcast there, never on `destination_chain`.
export function explorerUrl(payment: ExplorerLinkablePayment): string | null {
  if (!payment.external_tx_hash) return null;
  if (payment.execution_mode !== "testnet") return null;
  const key = `${payment.source_chain.toLowerCase()}-sepolia`;
  const base = EXPLORER_BASE_URLS[key];
  return base ? base + payment.external_tx_hash : null;
}
