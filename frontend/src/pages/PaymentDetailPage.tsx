import type { ReactNode } from "react";
import { Link, useParams } from "react-router-dom";

import { LifecycleTimeline } from "../components/LifecycleTimeline";
import { RouteVisualization } from "../components/RouteVisualization";
import { ProviderComparison } from "../components/ProviderComparison";
import { StatusBadge } from "../components/StatusBadge";
import { EmptyState } from "../components/EmptyState";
import { ErrorState } from "../components/ErrorState";
import { LoadingState } from "../components/LoadingState";
import { usePayment } from "../hooks/usePayment";
import { explorerUrl } from "../lib/explorer";
import { CHAIN_DISPLAY_NAMES, PROVIDER_DISPLAY_NAMES } from "../lib/chains";
import { formatTimestamp } from "../lib/format";

function chainLabel(chain: string): string {
  return CHAIN_DISPLAY_NAMES[chain] ?? chain;
}

function providerLabel(provider: string): string {
  return PROVIDER_DISPLAY_NAMES[provider] ?? provider;
}

// Human-readable label for payment_executions' finer-grained
// ExternalStatus enum (go-api/internal/payment/payment.go) -- distinct
// from StatusBadge's PaymentStatus labels, since this is a different
// state machine (the provider's own fill/refund/revert lifecycle within
// the SUBMITTED window), not the payment's overall status.
const EXTERNAL_STATUS_LABELS: Record<string, string> = {
  pending: "Pending",
  filled: "Filled",
  expired: "Expired",
  refunded: "Refunded",
  reverted: "Reverted",
  fill_failed: "Fill failed",
};

function externalStatusLabel(status: string): string {
  return EXTERNAL_STATUS_LABELS[status] ?? status;
}

interface DetailRowProps {
  label: string;
  children: ReactNode;
}

function DetailRow({ label, children }: DetailRowProps) {
  return (
    <div className="flex items-baseline justify-between gap-4 py-2">
      <dt className="text-sm text-slate-500">{label}</dt>
      <dd className="text-sm text-slate-900">{children}</dd>
    </div>
  );
}

// The "strongest part of the application" per the design spec: brings
// together every visualization built in Tasks 8-10 for a single payment.
// The route/lifecycle is the visual centerpiece (rendered first, full
// width) -- deliberately not a row of generic KPI cards, per the design's
// explicit instruction.
export function PaymentDetailPage() {
  const { id } = useParams<{ id: string }>();
  const { data: payment, isLoading, isError, error, refetch } = usePayment(id);

  if (!id) {
    return <EmptyState message="No payment ID provided." />;
  }

  if (isLoading) {
    return <LoadingState label="Loading payment…" />;
  }

  if (isError) {
    return (
      <div className="p-6">
        <ErrorState error={error} onRetry={() => refetch()} />
      </div>
    );
  }

  if (!payment) {
    return <EmptyState message="Payment not found." />;
  }

  const txExplorerUrl = explorerUrl(payment);

  return (
    <div className="p-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <Link to="/payments" className="text-sm text-brand-700 hover:underline">
            {"← Back to payments"}
          </Link>
          <h1 className="mt-1 font-mono text-xl font-semibold text-slate-900" title={payment.id}>
            {payment.id}
          </h1>
        </div>
        <StatusBadge status={payment.status} />
      </div>

      {/* Visual centerpiece: lifecycle + route, prominent near the top. */}
      <section className="mt-6 rounded-lg border border-slate-200 bg-white p-5 shadow-sm">
        <h2 className="text-sm font-medium text-slate-700">Lifecycle</h2>
        <div className="mt-4">
          <LifecycleTimeline payment={payment} />
        </div>
      </section>

      <section className="mt-6 rounded-lg border border-slate-200 bg-white p-5 shadow-sm">
        <RouteVisualization
          hops={payment.hops}
          sourceChain={payment.source_chain}
          destinationChain={payment.destination_chain}
          executionMode={payment.execution_mode}
          asset={payment.asset}
        />
      </section>

      <section className="mt-6 rounded-lg border border-slate-200 bg-white p-5 shadow-sm">
        <ProviderComparison paymentId={payment.id} />
      </section>

      {/* Details panel: every field from the design doc's §4 list. */}
      <section className="mt-6 rounded-lg border border-slate-200 bg-white p-5 shadow-sm">
        <h2 className="text-sm font-medium text-slate-700">Details</h2>
        <dl className="mt-2 divide-y divide-slate-100">
          <DetailRow label="Route">
            {chainLabel(payment.source_chain)} {"→"} {chainLabel(payment.destination_chain)}
          </DetailRow>
          <DetailRow label="Amount">
            {payment.amount} {payment.asset}
          </DetailRow>
          <DetailRow label="Asset">{payment.asset}</DetailRow>
          <DetailRow label="Provider">
            {payment.bridge_provider ? providerLabel(payment.bridge_provider) : "—"}
          </DetailRow>
          <DetailRow label="Routing cost">
            {payment.total_fee.toLocaleString(undefined, { maximumFractionDigits: 6 })}
          </DetailRow>
          <DetailRow label="Execution mode">
            <span className="capitalize">{payment.execution_mode}</span>
          </DetailRow>
          <DetailRow label="Transaction hash">
            {payment.external_tx_hash ? (
              txExplorerUrl ? (
                <a
                  href={txExplorerUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="font-mono text-xs text-brand-700 hover:underline"
                >
                  {payment.external_tx_hash}
                </a>
              ) : (
                <span className="font-mono text-xs">{payment.external_tx_hash}</span>
              )
            ) : (
              "—"
            )}
          </DetailRow>
          <DetailRow label="Provider status">
            {payment.external_status ? (
              <span title={payment.raw_external_status ?? undefined}>
                {externalStatusLabel(payment.external_status)}
              </span>
            ) : (
              "—"
            )}
          </DetailRow>
          <DetailRow label="Provider reference">
            {payment.provider_reference_id ? (
              <span className="font-mono text-xs">{payment.provider_reference_id}</span>
            ) : (
              "—"
            )}
          </DetailRow>
          <DetailRow label="Created">{formatTimestamp(payment.created_at)}</DetailRow>
          {payment.status === "FAILED" ? (
            <DetailRow label="Failure reason">
              <span className="text-status-failed">
                {payment.failure_reason ?? "Failed (no reason recorded)"}
              </span>
            </DetailRow>
          ) : null}
        </dl>
      </section>
    </div>
  );
}
