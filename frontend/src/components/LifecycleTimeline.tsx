import type { Payment, PaymentStatus } from "../api/types";
import { formatTimestamp } from "../lib/format";
import { statusColor } from "./StatusBadge";

export type StageState = "complete" | "active" | "pending" | "failed" | "not_applicable";

export interface LifecycleStage {
  id: string;
  label: string;
  state: StageState;
  // Real backend timestamp for this stage, or null when none exists.
  // Rendering code must show "—"/"not tracked" for null, never guess one.
  timestamp: string | null;
  // Short explanatory text: why there's no timestamp, what "N/A" means
  // here, or (for the failed stage) the real failure_reason.
  note: string;
}

interface LifecycleTimelineProps {
  payment: Payment;
}

// Happy-path ordering of PaymentStatus. FAILED is terminal and reachable
// from any point after ROUTED, so it has no rank here -- callers check
// `status === "FAILED"` explicitly instead of comparing ranks against it.
const STATUS_ORDER: PaymentStatus[] = ["ROUTED", "PROCESSING", "SUBMITTED", "COMPLETED"];

function hasReachedOrPassed(status: PaymentStatus, target: PaymentStatus): boolean {
  if (status === "FAILED") return true; // FAILED implies Kafka processing had already started.
  return STATUS_ORDER.indexOf(status) >= STATUS_ORDER.indexOf(target);
}

// Derives the 7 conceptual lifecycle stages (Payment Created -> Quotes
// Retrieved -> Route Selected -> Persisted -> Kafka Processing ->
// Blockchain Execution -> Destination Confirmation, per the design doc's
// §4) from ONLY the fields the backend actually persists on `Payment`:
// created_at, submitted_at, completed_at, status, execution_mode,
// failure_reason. There is no per-stage timestamp table (design doc §1
// finding) -- the phase plan's conceptual execution-table fields
// `broadcast_at`/`confirmed_at` map onto this API type as `submitted_at`
// (broadcast) and `completed_at` (confirmed); there is no field beyond
// those two, so a stage with no real backend signal gets `timestamp:
// null` and an explanatory `note` instead of an invented time.
export function deriveLifecycleStages(payment: Payment): LifecycleStage[] {
  const isTestnet = payment.execution_mode === "testnet";
  const stages: LifecycleStage[] = [];

  // 1. Payment Created -- always complete, real created_at timestamp.
  stages.push({
    id: "created",
    label: "Payment Created",
    state: "complete",
    timestamp: payment.created_at,
    note: "",
  });

  // 2 & 3. Quotes Retrieved / Route Selected -- routing happens
  // synchronously before the payment row is persisted, so neither has its
  // own timestamp; per the brief, both are treated as complete-but-
  // untimestamped for testnet-mode payments (known to have happened by the
  // time the created_at row exists) and as not applicable for
  // simulated-mode payments, rather than implying a real quote/route
  // event occurred where none is tracked.
  const quoteRouteNote = isTestnet
    ? "No distinct timestamp — inferred complete by the time Payment Created's row exists"
    : "N/A — simulated mode";
  const quoteRouteState: StageState = isTestnet ? "complete" : "not_applicable";
  stages.push({
    id: "quotes_retrieved",
    label: "Quotes Retrieved",
    state: quoteRouteState,
    timestamp: null,
    note: quoteRouteNote,
  });
  stages.push({
    id: "route_selected",
    label: "Route Selected",
    state: quoteRouteState,
    timestamp: null,
    note: quoteRouteNote,
  });

  // 4. Persisted -- complete, same timestamp as Payment Created (the row's
  // existence IS the persistence event; there is no separate field).
  stages.push({
    id: "persisted",
    label: "Persisted",
    state: "complete",
    timestamp: payment.created_at,
    note: "",
  });

  // 5. Kafka Processing -- no distinct timestamp; derived purely from
  // whether `status` has moved past ROUTED. Deliberately never renders
  // "active" here: there's no field that reports Kafka processing as
  // *currently* underway, only "hasn't started" (still ROUTED) vs. "has
  // moved on" (PROCESSING or later) -- collapsing to pending/complete
  // avoids claiming an observation we don't have.
  const kafkaComplete = hasReachedOrPassed(payment.status, "PROCESSING");
  stages.push({
    id: "kafka_processing",
    label: "Kafka Processing",
    state: kafkaComplete ? "complete" : "pending",
    timestamp: null,
    note: "Inferred from status — not individually timestamped",
  });

  // 6. Blockchain Execution -- for testnet-mode, pending until
  // `submitted_at` (the design doc's conceptual `broadcast_at`) is set,
  // then complete with that real timestamp. Simulated-mode payments never
  // really execute on-chain, so this is N/A, not pending -- pending would
  // wrongly imply it's still going to happen. Likewise, a testnet payment
  // that reached FAILED status while `submitted_at` is still null failed
  // during Kafka Processing (quote-expiry, route-unavailable, fee-slippage,
  // or guardrail-exceeded -- see go-api/internal/worker/executor.go) and so
  // never reached broadcast at all; rendering "pending" there would falsely
  // imply the broadcast might still happen even though Destination
  // Confirmation already shows the payment as failed. Note: if
  // `submitted_at` IS set on a FAILED payment, the broadcast genuinely
  // happened and only the later confirmation failed, so that case still
  // takes the `complete` branch below.
  if (!isTestnet) {
    stages.push({
      id: "blockchain_execution",
      label: "Blockchain Execution",
      state: "not_applicable",
      timestamp: null,
      note: "N/A — simulated mode",
    });
  } else if (payment.submitted_at) {
    stages.push({
      id: "blockchain_execution",
      label: "Blockchain Execution",
      state: "complete",
      timestamp: payment.submitted_at,
      note: "",
    });
  } else if (payment.status === "FAILED") {
    stages.push({
      id: "blockchain_execution",
      label: "Blockchain Execution",
      state: "not_applicable",
      timestamp: null,
      note: "Never broadcast — payment failed before broadcast",
    });
  } else {
    stages.push({
      id: "blockchain_execution",
      label: "Blockchain Execution",
      state: "pending",
      timestamp: null,
      note: "Not yet broadcast",
    });
  }

  // 7. Destination Confirmation -- complete with `completed_at` (the
  // design doc's conceptual `confirmed_at`) when COMPLETED; failed (not
  // pending) with the real `failure_reason` shown inline when FAILED.
  // While SUBMITTED, the payment has genuinely broadcast and is right now
  // waiting on confirmation -- a real, observable in-between state (unlike
  // Kafka Processing/Blockchain Execution above, which have no such
  // signal) -- so this is the one stage that renders "active" rather than
  // plain "pending".
  if (payment.status === "COMPLETED") {
    stages.push({
      id: "destination_confirmation",
      label: "Destination Confirmation",
      state: "complete",
      timestamp: payment.completed_at,
      note: "",
    });
  } else if (payment.status === "FAILED") {
    stages.push({
      id: "destination_confirmation",
      label: "Destination Confirmation",
      state: "failed",
      timestamp: null,
      note: payment.failure_reason ?? "Failed (no reason recorded)",
    });
  } else if (payment.status === "SUBMITTED") {
    stages.push({
      id: "destination_confirmation",
      label: "Destination Confirmation",
      state: "active",
      timestamp: null,
      note: "Awaiting confirmation",
    });
  } else {
    stages.push({
      id: "destination_confirmation",
      label: "Destination Confirmation",
      state: "pending",
      timestamp: null,
      note: "Not tracked yet",
    });
  }

  return stages;
}

// Reuses StatusBadge's status -> color mapping (Task 6) so "complete"/
// "active"/"failed" here read as the same green/amber/red used everywhere
// else in the app, instead of a separately invented palette.
const COMPLETE_COLOR = statusColor("COMPLETED");
const ACTIVE_COLOR = statusColor("PROCESSING");
const FAILED_COLOR = statusColor("FAILED");
const PENDING_COLOR = "#94a3b8"; // slate-400 -- no PaymentStatus means "pending", so not from statusColor.
const NOT_APPLICABLE_COLOR = "#cbd5e1"; // slate-300 -- fainter than pending; explicitly not "will happen later".

function markerStyle(state: StageState): { border: string; fill: string } {
  switch (state) {
    case "complete":
      return { border: COMPLETE_COLOR, fill: COMPLETE_COLOR };
    case "active":
      return { border: ACTIVE_COLOR, fill: ACTIVE_COLOR };
    case "failed":
      return { border: FAILED_COLOR, fill: FAILED_COLOR };
    case "not_applicable":
      return { border: NOT_APPLICABLE_COLOR, fill: "transparent" };
    case "pending":
    default:
      return { border: PENDING_COLOR, fill: "transparent" };
  }
}

function stageNoteClass(state: StageState): string {
  if (state === "failed") return "text-status-failed";
  if (state === "not_applicable") return "text-slate-400";
  return "text-slate-500";
}

function StageMarker({ state }: { state: StageState }) {
  const { border, fill } = markerStyle(state);
  return (
    <span
      aria-hidden="true"
      className={`relative flex h-4 w-4 shrink-0 items-center justify-center rounded-full border-2 ${
        state === "active" ? "animate-pulse" : ""
      }`}
      style={{ borderColor: border, backgroundColor: fill }}
    />
  );
}

function StageTimestamp({ stage }: { stage: LifecycleStage }) {
  if (stage.timestamp) {
    return <p className="text-xs text-slate-500">{formatTimestamp(stage.timestamp)}</p>;
  }
  // No real timestamp for this stage -- show "—" plus the explanatory
  // note, never a guessed time.
  return <p className={`text-xs ${stageNoteClass(stage.state)}`}>— {stage.note}</p>;
}

// Renders the 7-stage payment lifecycle as a horizontal timeline. Every
// stage's state/timestamp comes from `deriveLifecycleStages`, which only
// reads real `Payment` fields -- this component is purely presentational.
export function LifecycleTimeline({ payment }: LifecycleTimelineProps) {
  const stages = deriveLifecycleStages(payment);

  return (
    <ol className="flex flex-col gap-4 sm:flex-row sm:items-start sm:gap-2">
      {stages.map((stage, index) => (
        <li key={stage.id} className="flex flex-1 items-start gap-3 sm:flex-col sm:items-center sm:gap-2 sm:text-center">
          <div className="flex items-center gap-2 sm:w-full sm:justify-center">
            <StageMarker state={stage.state} />
            {index < stages.length - 1 && (
              <span
                aria-hidden="true"
                className="hidden h-px flex-1 bg-slate-200 sm:block"
              />
            )}
          </div>
          <div className="sm:max-w-[9rem]">
            <p
              className={`text-sm font-medium ${
                stage.state === "not_applicable" ? "text-slate-400" : "text-slate-700"
              }`}
            >
              {stage.label}
            </p>
            <StageTimestamp stage={stage} />
          </div>
        </li>
      ))}
    </ol>
  );
}
