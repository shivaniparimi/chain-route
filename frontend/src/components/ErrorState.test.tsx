import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ApiError } from "../api/client";
import { ErrorState } from "./ErrorState";

describe("ErrorState", () => {
  it("renders an ApiError's message", () => {
    render(<ErrorState error={new ApiError(404, "payment not found")} />);
    expect(screen.getByText("payment not found")).toBeInTheDocument();
  });

  it("falls back to a generic message for a non-Error value", () => {
    render(<ErrorState error="boom" />);
    expect(screen.getByText("Something went wrong. Please try again.")).toBeInTheDocument();
  });

  it("omits the retry button when no onRetry is passed", () => {
    render(<ErrorState error={new ApiError(500, "server error")} />);
    expect(screen.queryByRole("button", { name: "Retry" })).not.toBeInTheDocument();
  });

  it("calls onRetry when the retry button is clicked", () => {
    const onRetry = vi.fn();
    render(<ErrorState error={new ApiError(500, "server error")} onRetry={onRetry} />);
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    expect(onRetry).toHaveBeenCalledTimes(1);
  });
});
