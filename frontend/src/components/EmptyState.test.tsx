import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { EmptyState } from "./EmptyState";

describe("EmptyState", () => {
  it("renders the message", () => {
    render(<EmptyState message="No payments found." />);
    expect(screen.getByText("No payments found.")).toBeInTheDocument();
  });

  it("renders an optional icon", () => {
    render(<EmptyState message="Nothing here." icon={<span data-testid="icon">*</span>} />);
    expect(screen.getByTestId("icon")).toBeInTheDocument();
  });
});
