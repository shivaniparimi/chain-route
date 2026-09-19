import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type { TimeseriesPoint } from "../../api/types";
import { PaymentVolumeChart } from "./PaymentVolumeChart";

const fixturePoints: TimeseriesPoint[] = [
  { bucket: "2026-09-16T00:00:00Z", value: 0, count: 3 },
  { bucket: "2026-09-17T00:00:00Z", value: 0, count: 5 },
  { bucket: "2026-09-18T00:00:00Z", value: 0, count: 2 },
];

describe("PaymentVolumeChart", () => {
  it("renders EmptyState when there are no points", () => {
    render(<PaymentVolumeChart points={[]} />);
    expect(screen.getByText("No payment volume data yet")).toBeInTheDocument();
  });

  it("renders the expected number of data points for a populated series", () => {
    const { container } = render(<PaymentVolumeChart points={fixturePoints} />);

    // Recharts doesn't render a discrete DOM node per Area data point by
    // default (no `dot` prop, matching the brief's exact pattern code), so
    // "the expected number of data points" is verified via the categorical
    // XAxis, which renders exactly one tick per unique `bucket` value.
    const ticks = container.querySelectorAll(".recharts-xAxis .recharts-cartesian-axis-tick");
    expect(ticks.length).toBe(fixturePoints.length);

    // The chart itself rendered (not the empty state).
    expect(screen.queryByText("No payment volume data yet")).not.toBeInTheDocument();
    expect(container.querySelector(".recharts-area")).not.toBeNull();
  });
});
