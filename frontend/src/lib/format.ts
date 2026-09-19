export function formatTimestamp(iso: string): string {
  return new Date(iso).toLocaleString(undefined, {
    dateStyle: "medium",
    timeStyle: "medium",
  });
}

export function formatRelativeTime(iso: string): string {
  const diffMs = Date.now() - new Date(iso).getTime();
  const diffSec = Math.floor(diffMs / 1000);
  if (diffSec < 60) return `${diffSec}s ago`;
  if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m ago`;
  if (diffSec < 86400) return `${Math.floor(diffSec / 3600)}h ago`;
  return `${Math.floor(diffSec / 86400)}d ago`;
}

// Quote amounts are base-units integer decimal strings (e.g. WETH has 18
// decimals) -- this is a DISPLAY approximation for the dashboard only,
// never used for anything that needs exact precision (that stays
// server-side). decimals defaults to 18 (WETH/ETH), the only asset with
// real testnet quote data today per the backend survey.
export function formatBaseUnits(baseUnits: string, decimals = 18): string {
  const value = BigInt(baseUnits);
  const divisor = BigInt(10) ** BigInt(decimals);
  const whole = value / divisor;
  const fraction = value % divisor;
  const fractionStr = fraction.toString().padStart(decimals, "0").slice(0, 6).replace(/0+$/, "");
  return fractionStr ? `${whole}.${fractionStr}` : whole.toString();
}
