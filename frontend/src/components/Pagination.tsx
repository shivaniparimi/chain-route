interface PaginationProps {
  hasNext: boolean;
  onNext: () => void;
  onPrevious: () => void;
  // Optional: callers managing a cursor stack can disable "Previous" on
  // the first page. Defaults to enabled so the required prop set from
  // the brief (hasNext/onNext/onPrevious) works unchanged.
  hasPrevious?: boolean;
}

// Previous/Next-only control. The backend (Task 2) uses keyset/cursor
// pagination, which has no concept of "page 3 of 12" -- there is no
// total count and no way to jump to an arbitrary page, so this
// deliberately does not render page numbers.
export function Pagination({ hasNext, onNext, onPrevious, hasPrevious = true }: PaginationProps) {
  const buttonClass =
    "rounded-md border border-slate-300 px-3 py-1.5 text-sm font-medium text-slate-700 hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-40 disabled:hover:bg-transparent focus:outline-none focus:ring-2 focus:ring-brand-500";

  return (
    <div className="flex items-center justify-between gap-3 border-t border-slate-200 px-4 py-3">
      <button type="button" onClick={onPrevious} disabled={!hasPrevious} className={buttonClass}>
        Previous
      </button>
      <button type="button" onClick={onNext} disabled={!hasNext} className={buttonClass}>
        Next
      </button>
    </div>
  );
}
