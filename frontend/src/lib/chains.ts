export const SIMULATED_GRAPH_CHAINS = ["ethereum", "base", "arbitrum", "optimism", "polygon"] as const;

export const CHAIN_DISPLAY_NAMES: Record<string, string> = {
  ethereum: "Ethereum",
  base: "Base",
  arbitrum: "Arbitrum",
  optimism: "Optimism",
  polygon: "Polygon",
};

export const PROVIDER_DISPLAY_NAMES: Record<string, string> = {
  across: "Across",
  relay: "Relay",
};

// The one real corridor with signed testnet execution behind it, per the
// backend's own testnetChainIDByChain map (handler/routes.go) --
// Ethereum Sepolia <-> Base Sepolia only. The check below compares
// against "eth", the API-level `asset` value this function actually
// receives (payment.asset, lowercased) -- NOT "weth". "WETH" is only the
// on-chain bridged token symbol used once funds are wrapped for the
// bridge; it is never the value this function is called with. Do not
// "fix" the comparison to check for "weth" -- that would silently break
// this corridor-detection badge. Kept in exactly one place so a future
// backend change to supported corridors has exactly one frontend
// constant to update, and so this is never re-derived from chain-name
// pattern matching anywhere else in the UI.
export function hasRealTestnetExecution(
  sourceChain: string,
  destChain: string,
  asset: string,
  executionMode: string,
): boolean {
  if (executionMode !== "testnet") return false;
  const pair = [sourceChain.toLowerCase(), destChain.toLowerCase()].sort().join("-");
  return pair === "base-ethereum" && asset.toLowerCase() === "eth";
}
