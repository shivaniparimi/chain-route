import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { LoadingState } from "./LoadingState";

describe("LoadingState", () => {
  it("renders a status role with the default label", () => {
    render(<LoadingState />);
    expect(screen.getByRole("status")).toBeInTheDocument();
    expect(screen.getByText("Loading…")).toBeInTheDocument();
  });

  it("renders a custom label when provided", () => {
    render(<LoadingState label="Fetching payments…" />);
    expect(screen.getByText("Fetching payments…")).toBeInTheDocument();
  });
});
