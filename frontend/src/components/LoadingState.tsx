// A restrained loading indicator: one small spinning ring, no flashy
// animation, gradients, or skeleton shimmer -- per the design's "avoid
// excessive animations" constraint.
export function LoadingState({ label = "Loading…" }: { label?: string }) {
  return (
    <div
      role="status"
      className="flex flex-col items-center justify-center gap-3 py-12 text-sm text-slate-500"
    >
      <span
        aria-hidden="true"
        className="h-6 w-6 animate-spin rounded-full border-2 border-slate-200 border-t-brand-500"
      />
      <span>{label}</span>
    </div>
  );
}
