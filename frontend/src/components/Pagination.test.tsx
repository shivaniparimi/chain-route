import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { Pagination } from "./Pagination";

describe("Pagination", () => {
  it("disables Next when hasNext is false", () => {
    render(<Pagination hasNext={false} onNext={vi.fn()} onPrevious={vi.fn()} />);
    expect(screen.getByRole("button", { name: "Next" })).toBeDisabled();
  });

  it("enables Next when hasNext is true and calls onNext when clicked", () => {
    const onNext = vi.fn();
    render(<Pagination hasNext={true} onNext={onNext} onPrevious={vi.fn()} />);
    const nextButton = screen.getByRole("button", { name: "Next" });
    expect(nextButton).toBeEnabled();
    fireEvent.click(nextButton);
    expect(onNext).toHaveBeenCalledTimes(1);
  });

  it("calls onPrevious when Previous is clicked", () => {
    const onPrevious = vi.fn();
    render(<Pagination hasNext={true} onNext={vi.fn()} onPrevious={onPrevious} />);
    fireEvent.click(screen.getByRole("button", { name: "Previous" }));
    expect(onPrevious).toHaveBeenCalledTimes(1);
  });

  it("disables Previous when hasPrevious is explicitly false", () => {
    render(<Pagination hasNext={true} onNext={vi.fn()} onPrevious={vi.fn()} hasPrevious={false} />);
    expect(screen.getByRole("button", { name: "Previous" })).toBeDisabled();
  });

  it("does not render page numbers", () => {
    render(<Pagination hasNext={true} onNext={vi.fn()} onPrevious={vi.fn()} />);
    expect(screen.queryByText(/page \d/i)).not.toBeInTheDocument();
  });
});
