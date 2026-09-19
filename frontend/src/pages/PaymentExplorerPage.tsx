import { useState } from "react";

import type { FilterKey } from "../components/FilterBar";
import { FilterBar } from "../components/FilterBar";
import { EmptyState } from "../components/EmptyState";
import { ErrorState } from "../components/ErrorState";
import { LoadingState } from "../components/LoadingState";
import { Pagination } from "../components/Pagination";
import { PaymentTable } from "../components/PaymentTable";
import { usePaymentList } from "../hooks/usePaymentList";
import type { PaymentListFilters } from "../api/types";

// The subset of PaymentListFilters that FilterBar's dropdowns drive --
// used both to build the query and to count "active filters" for the
// empty-state message below. Deliberately excludes `cursor`/`limit`,
// which are pagination state, not user-chosen filters.
const FILTER_KEYS: FilterKey[] = [
  "status",
  "provider",
  "source_chain",
  "destination_chain",
  "execution_mode",
];

export function PaymentExplorerPage() {
  const [filters, setFilters] = useState<PaymentListFilters>({});
  // Cursor-stack pagination: `cursor` is the cursor for the page
  // currently shown; `cursorStack` holds every cursor value that led here
  // (starting from `undefined` for page 1), so "Previous" can pop back to
  // exactly the prior page. The backend (Task 2) is keyset/cursor-based
  // with no total count, so there is no page-number arithmetic here.
  const [cursor, setCursor] = useState<string | undefined>(undefined);
  const [cursorStack, setCursorStack] = useState<(string | undefined)[]>([]);

  const { data, isLoading, isError, error, refetch } = usePaymentList({ ...filters, cursor });

  function handleFilterChange(key: FilterKey, value: string | undefined) {
    setFilters((prev) => ({ ...prev, [key]: value }));
    // Any filter change invalidates the current page position -- always
    // go back to a fresh first page rather than re-paginating with a
    // cursor that was minted under the old filter set.
    setCursor(undefined);
    setCursorStack([]);
  }

  function handleNext() {
    if (!data?.next_cursor) return;
    setCursorStack((stack) => [...stack, cursor]);
    setCursor(data.next_cursor ?? undefined);
  }

  function handlePrevious() {
    if (cursorStack.length === 0) return;
    const previousCursor = cursorStack[cursorStack.length - 1];
    setCursorStack((stack) => stack.slice(0, -1));
    setCursor(previousCursor);
  }

  const activeFilterCount = FILTER_KEYS.filter((key) => filters[key] !== undefined).length;
  const emptyMessage =
    activeFilterCount > 0
      ? "No payments match these filters. Try adjusting or clearing them."
      : "No payments exist yet.";

  return (
    <div className="p-6">
      <h1 className="text-2xl font-semibold text-slate-900">Payments</h1>
      <p className="mt-2 text-slate-600">
        Browse routed, processing, and completed payments across every supported corridor.
      </p>

      <div className="mt-6 overflow-hidden rounded-lg border border-slate-200 bg-white shadow-sm">
        <FilterBar filters={filters} onChange={handleFilterChange} />

        {isLoading ? (
          <LoadingState label="Loading payments…" />
        ) : isError ? (
          <ErrorState error={error} onRetry={() => refetch()} />
        ) : !data || data.payments.length === 0 ? (
          <EmptyState message={emptyMessage} />
        ) : (
          <>
            <div className="overflow-x-auto">
              <PaymentTable payments={data.payments} />
            </div>
            <Pagination
              hasNext={Boolean(data.next_cursor)}
              hasPrevious={cursorStack.length > 0}
              onNext={handleNext}
              onPrevious={handlePrevious}
            />
          </>
        )}
      </div>
    </div>
  );
}
