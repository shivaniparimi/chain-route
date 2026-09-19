import type { ReactNode } from "react";

interface EmptyStateProps {
  message: string;
  icon?: ReactNode;
}

// Small, reusable, no business logic -- rendered wherever a list/table
// query resolves with zero rows.
export function EmptyState({ message, icon }: EmptyStateProps) {
  return (
    <div className="flex flex-col items-center justify-center gap-2 py-12 text-center text-sm text-slate-500">
      {icon}
      <p>{message}</p>
    </div>
  );
}
