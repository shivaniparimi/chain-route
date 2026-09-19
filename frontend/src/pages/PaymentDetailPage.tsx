import { useParams } from "react-router-dom";

// Stub for later tasks: a single payment's detail view (hops, quotes,
// execution status) backed by GET /payments/{id} and
// GET /payments/{id}/quotes. This task's scope is only the routing/build
// scaffold, not real content.
export function PaymentDetailPage() {
  const { id } = useParams<{ id: string }>();
  return (
    <div className="p-6">
      <h1 className="text-2xl font-semibold text-slate-900">Payment detail</h1>
      <p className="mt-2 text-slate-600">Payment {id} details will render here.</p>
    </div>
  );
}
