import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type { PaymentStatus } from "../api/types";
import { statusColor, StatusBadge } from "./StatusBadge";

describe("StatusBadge", () => {
  it.each<[PaymentStatus, string]>([
    ["ROUTED", "Routed"],
    ["PROCESSING", "Processing"],
    ["SUBMITTED", "Submitted"],
    ["COMPLETED", "Completed"],
    ["FAILED", "Failed"],
  ])("renders the %s label as %s", (status, label) => {
    render(<StatusBadge status={status} />);
    expect(screen.getByText(label)).toBeInTheDocument();
  });

  it("gives the completed / in-flight / failed families distinct colors", () => {
    expect(statusColor("COMPLETED")).toBe("#16a34a");
    expect(statusColor("FAILED")).toBe("#dc2626");
    const activeColor = statusColor("ROUTED");
    expect(statusColor("PROCESSING")).toBe(activeColor);
    expect(statusColor("SUBMITTED")).toBe(activeColor);
    expect(activeColor).not.toBe(statusColor("COMPLETED"));
    expect(activeColor).not.toBe(statusColor("FAILED"));
  });

  it("still shows the exact status label for each in-flight sub-state, distinguishing them from each other", () => {
    render(<StatusBadge status="ROUTED" />);
    expect(screen.getByText("Routed")).toBeInTheDocument();
    expect(screen.queryByText("Processing")).not.toBeInTheDocument();
  });
});
