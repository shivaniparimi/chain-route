import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type { Payment } from "../api/types";
import { deriveLifecycleStages, LifecycleTimeline } from "./LifecycleTimeline";

function makePayment(overrides: Partial<Payment>): Payment {
  return {
    id: "11111111-1111-1111-1111-111111111111",
    source_chain: "ethereum",
    destination_chain: "base",
    asset: "ETH",
    amount: "0.5",
    status: "ROUTED",
    total_fee: 0.001,
    hops: [],
    execution_mode: "testnet",
    bridge_provider: "Across",
    failure_reason: null,
    external_tx_hash: null,
    submitted_at: null,
    created_at: "2026-09-18T10:00:00Z",
    updated_at: "2026-09-18T10:00:00Z",
    completed_at: null,
    ...overrides,
  };
}

describe("deriveLifecycleStages", () => {
  it("never fabricates a timestamp for a stage the backend doesn't track (Quotes Retrieved/Route Selected/Kafka Processing all have timestamp: null)", () => {
    const stages = deriveLifecycleStages(makePayment({ status: "ROUTED", execution_mode: "testnet" }));
    const byId = Object.fromEntries(stages.map((s) => [s.id, s]));
    expect(byId.quotes_retrieved.timestamp).toBeNull();
    expect(byId.route_selected.timestamp).toBeNull();
    expect(byId.kafka_processing.timestamp).toBeNull();
  });
});

describe("LifecycleTimeline", () => {
  it("renders every stage as complete with the real timestamps for a COMPLETED testnet-mode payment", () => {
    const payment = makePayment({
      status: "COMPLETED",
      execution_mode: "testnet",
      created_at: "2026-09-18T10:00:00.000Z",
      submitted_at: "2026-09-18T10:00:05.000Z",
      completed_at: "2026-09-18T10:00:20.000Z",
    });

    const { container } = render(<LifecycleTimeline payment={payment} />);

    // 7 stages rendered, none left as pending/not_applicable/failed.
    const items = container.querySelectorAll("ol > li");
    expect(items).toHaveLength(7);

    expect(screen.getByText("Payment Created")).toBeInTheDocument();
    expect(screen.getByText("Quotes Retrieved")).toBeInTheDocument();
    expect(screen.getByText("Route Selected")).toBeInTheDocument();
    expect(screen.getByText("Persisted")).toBeInTheDocument();
    expect(screen.getByText("Kafka Processing")).toBeInTheDocument();
    expect(screen.getByText("Blockchain Execution")).toBeInTheDocument();
    expect(screen.getByText("Destination Confirmation")).toBeInTheDocument();

    // Stages with a real backend timestamp render the formatted time, not "—".
    const stages = deriveLifecycleStages(payment);
    const created = stages.find((s) => s.id === "created")!;
    const persisted = stages.find((s) => s.id === "persisted")!;
    const execution = stages.find((s) => s.id === "blockchain_execution")!;
    const confirmation = stages.find((s) => s.id === "destination_confirmation")!;
    expect(created.state).toBe("complete");
    expect(created.timestamp).toBe(payment.created_at);
    expect(persisted.state).toBe("complete");
    expect(persisted.timestamp).toBe(payment.created_at);
    expect(execution.state).toBe("complete");
    expect(execution.timestamp).toBe(payment.submitted_at);
    expect(confirmation.state).toBe("complete");
    expect(confirmation.timestamp).toBe(payment.completed_at);

    // Quotes Retrieved / Route Selected / Kafka Processing are complete but
    // genuinely have no distinct timestamp -- rendered as "—" plus a note,
    // never a guessed time.
    const quotes = stages.find((s) => s.id === "quotes_retrieved")!;
    const route = stages.find((s) => s.id === "route_selected")!;
    const kafka = stages.find((s) => s.id === "kafka_processing")!;
    expect(quotes.state).toBe("complete");
    expect(quotes.timestamp).toBeNull();
    expect(route.state).toBe("complete");
    expect(route.timestamp).toBeNull();
    expect(kafka.state).toBe("complete");
    expect(kafka.timestamp).toBeNull();

    // No stage is not_applicable or failed for a completed testnet payment.
    expect(stages.every((s) => s.state !== "not_applicable" && s.state !== "failed")).toBe(true);
  });

  it("renders the failure at the Destination Confirmation stage with failure_reason visible inline", () => {
    const payment = makePayment({
      status: "FAILED",
      execution_mode: "testnet",
      submitted_at: "2026-09-18T10:00:05.000Z",
      failure_reason: "destination chain reverted: insufficient liquidity",
    });

    render(<LifecycleTimeline payment={payment} />);

    expect(screen.getByText("Destination Confirmation")).toBeInTheDocument();
    expect(
      screen.getByText((text) => text.includes("destination chain reverted: insufficient liquidity")),
    ).toBeInTheDocument();

    const stages = deriveLifecycleStages(payment);
    const confirmation = stages.find((s) => s.id === "destination_confirmation")!;
    expect(confirmation.state).toBe("failed");
    expect(confirmation.note).toBe("destination chain reverted: insufficient liquidity");
    // Failed stage never gets a fabricated completed_at-style timestamp.
    expect(confirmation.timestamp).toBeNull();
  });

  it("renders Quotes Retrieved/Route Selected/Blockchain Execution as N/A (not pending or complete) for a ROUTED simulated-mode payment", () => {
    const payment = makePayment({ status: "ROUTED", execution_mode: "simulated" });

    render(<LifecycleTimeline payment={payment} />);

    const stages = deriveLifecycleStages(payment);
    const quotes = stages.find((s) => s.id === "quotes_retrieved")!;
    const route = stages.find((s) => s.id === "route_selected")!;
    const execution = stages.find((s) => s.id === "blockchain_execution")!;

    expect(quotes.state).toBe("not_applicable");
    expect(route.state).toBe("not_applicable");
    expect(execution.state).toBe("not_applicable");

    // Rendered text says N/A / simulated mode, distinct from both "pending"
    // and any real timestamp.
    expect(screen.getAllByText((text) => text.includes("N/A — simulated mode")).length).toBeGreaterThan(0);

    // Kafka Processing and Payment Created/Persisted are unaffected by
    // execution_mode -- still pending/complete respectively, not N/A.
    const kafka = stages.find((s) => s.id === "kafka_processing")!;
    const created = stages.find((s) => s.id === "created")!;
    expect(kafka.state).toBe("pending");
    expect(created.state).toBe("complete");
  });

  it("renders Blockchain Execution as pending (not N/A) for a testnet-mode payment that hasn't broadcast yet", () => {
    const payment = makePayment({ status: "ROUTED", execution_mode: "testnet", submitted_at: null });

    const stages = deriveLifecycleStages(payment);
    const execution = stages.find((s) => s.id === "blockchain_execution")!;
    expect(execution.state).toBe("pending");
    expect(execution.timestamp).toBeNull();
  });

  it("renders Destination Confirmation as active (awaiting confirmation) once a testnet payment has been submitted but not yet completed", () => {
    const payment = makePayment({
      status: "SUBMITTED",
      execution_mode: "testnet",
      submitted_at: "2026-09-18T10:00:05.000Z",
    });

    const stages = deriveLifecycleStages(payment);
    const confirmation = stages.find((s) => s.id === "destination_confirmation")!;
    expect(confirmation.state).toBe("active");
    expect(confirmation.timestamp).toBeNull();
  });

  it("renders Blockchain Execution as not_applicable (never 'pending'/'Not yet broadcast') for a testnet payment that FAILED before ever broadcasting", () => {
    // Mirrors a real backend path (go-api/internal/worker/executor.go):
    // quote-expiry, route-unavailable, fee-slippage, and guardrail-exceeded
    // failures all reach status FAILED while submitted_at is still null,
    // because the payment never made it to broadcast. Rendering "pending —
    // Not yet broadcast" here would wrongly imply the broadcast might still
    // happen, directly contradicting Destination Confirmation's "failed"
    // state for the same payment.
    const payment = makePayment({
      status: "FAILED",
      execution_mode: "testnet",
      submitted_at: null,
      failure_reason: "quote expired before route could be confirmed",
    });

    render(<LifecycleTimeline payment={payment} />);

    const stages = deriveLifecycleStages(payment);
    const execution = stages.find((s) => s.id === "blockchain_execution")!;

    expect(execution.state).not.toBe("pending");
    expect(execution.note).not.toContain("Not yet broadcast");
    expect(execution.state).toBe("not_applicable");
    expect(execution.timestamp).toBeNull();
    expect(execution.note).toContain("Never broadcast");

    // Destination Confirmation still correctly shows the payment as failed,
    // so the two stages no longer contradict each other.
    const confirmation = stages.find((s) => s.id === "destination_confirmation")!;
    expect(confirmation.state).toBe("failed");
  });
});
