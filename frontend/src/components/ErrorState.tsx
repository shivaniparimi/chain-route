import { ApiError } from "../api/client";

interface ErrorStateProps {
  error: unknown;
  onRetry?: () => void;
}

// Renders ApiError's message (falling back to a generic message for any
// other thrown value) with an optional retry affordance, shown only when
// the caller passes an `onRetry` callback.
export function ErrorState({ error, onRetry }: ErrorStateProps) {
  const message =
    error instanceof ApiError
      ? error.message
      : error instanceof Error
        ? error.message
        : "Something went wrong. Please try again.";

  return (
    <div className="flex flex-col items-center justify-center gap-3 py-12 text-center text-sm">
      <p className="text-status-failed">{message}</p>
      {onRetry ? (
        <button
          type="button"
          onClick={onRetry}
          className="rounded-md border border-slate-300 px-3 py-1.5 text-sm font-medium text-slate-700 hover:bg-slate-100 focus:outline-none focus:ring-2 focus:ring-brand-500"
        >
          Retry
        </button>
      ) : null}
    </div>
  );
}
