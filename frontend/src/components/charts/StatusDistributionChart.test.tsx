import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type { DashboardStats } from "../../api/types";
import { StatusDistributionChart } from "./StatusDistributionChart";

function makeStats(overrides: Partial<DashboardStats> = {}): DashboardStats {
  return {
    total_payments: 0,
    completed_payments: 0,
    processing_payments: 0,
    failed_payments: 0,
    provider_usage: {},
    average_routing_cost: 0,
    network_usage: [],
    ...overrides,
  };
}

describe("StatusDistributionChart", () => {
  it("renders EmptyState when every status bucket is zero", () => {
    render(<StatusDistributionChart stats={makeStats()} />);
    expect(screen.getByText("No payment status data yet")).toBeInTheDocument();
  });

  it("renders the expected number of data points (one bar per status bucket) when populated", () => {
    const stats = makeStats({ completed_payments: 6, processing_payments: 3, failed_payments: 1 });
    const { container } = render(<StatusDistributionChart stats={stats} />);

    expect(screen.queryByText("No payment status data yet")).not.toBeInTheDocument();

    // One <Cell> (a distinctly-colored bar rectangle) per status bucket --
    // completed/in-flight/failed, matching StatusDistributionChart's fixed
    // BUCKETS list, regardless of which buckets are non-zero.
    const bars = container.querySelectorAll(".recharts-bar-rectangle");
    expect(bars.length).toBe(3);
  });

  it("colors each bucket using StatusBadge's statusColor mapping, not its own colors", () => {
    const stats = makeStats({ completed_payments: 1, processing_payments: 1, failed_payments: 1 });
    const { container } = render(<StatusDistributionChart stats={stats} />);

    const fills = Array.from(container.querySelectorAll(".recharts-bar-rectangle path")).map((el) =>
      el.getAttribute("fill"),
    );
    expect(fills).toContain("#16a34a"); // statusColor("COMPLETED")
    expect(fills).toContain("#d97706"); // statusColor("PROCESSING")
    expect(fills).toContain("#dc2626"); // statusColor("FAILED")
  });
});
