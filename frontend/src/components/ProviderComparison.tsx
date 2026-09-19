import type { Quote } from "../api/types";
import { usePaymentQuotes } from "../hooks/usePaymentQuotes";
import { PROVIDER_DISPLAY_NAMES } from "../lib/chains";
import { formatBaseUnits, formatRelativeTime, formatTimestamp } from "../lib/format";
import { EmptyState } from "./EmptyState";
import { ErrorState } from "./ErrorState";
import { LoadingState } from "./LoadingState";

export interface ProviderComparisonProps {
  paymentId: string;
}

function providerLabel(provider: string): string {
  return PROVIDER_DISPLAY_NAMES[provider] ?? provider;
}

// estimated_fill_time_sec has no existing formatter in lib/format.ts (that
// file's durations are all "time since X", not "length of time"), so this
// is a small local helper rather than a reuse of formatRelativeTime.
function formatFillTime(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const remainderSec = seconds % 60;
  return remainderSec === 0 ? `${minutes}m` : `${minutes}m ${remainderSec}s`;
}

// output_amount/fee_amount are genuinely base-units integer decimal
// strings -- confirmed against both the payment.Quote domain type's own
// doc comment and lib/format.ts's formatBaseUnits comment -- unlike
// PaymentListItem.amount/total_fee (human-scale decimals; see
// PaymentTable.tsx's formatRoutingCost comment for that distinction).
// formatBaseUnits is the correct formatter here and would be wrong there.
function QuoteCard({ quote }: { quote: Quote }) {
  return (
    <div
      data-testid="quote-card"
      data-provider={quote.provider}
      data-selected={quote.selected}
      className={`rounded-lg border p-4 ${
        quote.selected ? "border-brand-500 bg-brand-50" : "border-slate-200 bg-white"
      }`}
    >
      <div className="flex items-center justify-between gap-2">
        <h3 className="text-sm font-semibold text-slate-900">{providerLabel(quote.provider)}</h3>
        {quote.selected ? (
          <span className="inline-flex items-center rounded-full bg-brand-100 px-2.5 py-0.5 text-xs font-medium text-brand-800">
            Selected by ChainRoute
          </span>
        ) : null}
      </div>

      <dl className="mt-3 space-y-1 text-sm text-slate-700">
        <div className="flex items-baseline justify-between gap-4">
          <dt className="text-slate-500">Quoted output</dt>
          <dd>{formatBaseUnits(quote.output_amount)}</dd>
        </div>
        <div className="flex items-baseline justify-between gap-4">
          <dt className="text-slate-500">Fee</dt>
          <dd>{formatBaseUnits(quote.fee_amount)}</dd>
        </div>
        <div className="flex items-baseline justify-between gap-4">
          <dt className="text-slate-500">Estimated fill time</dt>
          <dd>{formatFillTime(quote.estimated_fill_time_sec)}</dd>
        </div>
      </dl>

      <p className="mt-3 text-xs text-slate-400" title={formatTimestamp(quote.quoted_at)}>
        Quoted {formatRelativeTime(quote.quoted_at)}
      </p>
    </div>
  );
}

// Renders Task 3's per-provider quote comparison for a payment. This is
// the one place in the dashboard whose whole purpose is an honest
// comparison, so it never fabricates one: a payment with zero persisted
// quotes (simulated-mode payments, and any real payment that predates
// migration 0007's quote persistence -- the two cases are indistinguishable
// from this endpoint's response and don't need to be, since the honest
// message is identical either way) renders Task 6's EmptyState, and a
// payment with exactly one quote gets an explicit note rather than letting
// that single card look like it "won" a comparison that never happened.
export function ProviderComparison({ paymentId }: ProviderComparisonProps) {
  const { data, isLoading, isError, error, refetch } = usePaymentQuotes(paymentId);

  if (isLoading) return <LoadingState label="Loading quotes…" />;
  if (isError) return <ErrorState error={error} onRetry={() => refetch()} />;

  const quotes = data?.quotes ?? [];

  if (quotes.length === 0) {
    return <EmptyState message="No provider comparison data available for this payment" />;
  }

  return (
    <div>
      <h2 className="text-sm font-medium text-slate-700">Provider comparison</h2>

      {quotes.length === 1 ? (
        <p className="mt-1 text-xs text-slate-500">Only one provider responded for this payment</p>
      ) : null}

      <div className="mt-3 grid gap-3 sm:grid-cols-2">
        {quotes.map((quote) => (
          <QuoteCard key={quote.provider} quote={quote} />
        ))}
      </div>
    </div>
  );
}
