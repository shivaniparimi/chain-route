import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { PaymentListFilters } from "../api/types";
import { FilterBar } from "./FilterBar";

const emptyFilters: PaymentListFilters = {};

describe("FilterBar", () => {
  it("renders a select for each known filter", () => {
    render(<FilterBar filters={emptyFilters} onChange={vi.fn()} />);
    expect(screen.getByLabelText("Status")).toBeInTheDocument();
    expect(screen.getByLabelText("Provider")).toBeInTheDocument();
    expect(screen.getByLabelText("Source chain")).toBeInTheDocument();
    expect(screen.getByLabelText("Destination chain")).toBeInTheDocument();
    expect(screen.getByLabelText("Execution mode")).toBeInTheDocument();
  });

  it("fires onChange with the status key/value on selection", () => {
    const onChange = vi.fn();
    render(<FilterBar filters={emptyFilters} onChange={onChange} />);
    fireEvent.change(screen.getByLabelText("Status"), { target: { value: "COMPLETED" } });
    expect(onChange).toHaveBeenCalledWith("status", "COMPLETED");
  });

  it("fires onChange with the provider key/value on selection", () => {
    const onChange = vi.fn();
    render(<FilterBar filters={emptyFilters} onChange={onChange} />);
    fireEvent.change(screen.getByLabelText("Provider"), { target: { value: "across" } });
    expect(onChange).toHaveBeenCalledWith("provider", "across");
  });

  it("fires onChange with a known chain for source_chain", () => {
    const onChange = vi.fn();
    render(<FilterBar filters={emptyFilters} onChange={onChange} />);
    fireEvent.change(screen.getByLabelText("Source chain"), { target: { value: "base" } });
    expect(onChange).toHaveBeenCalledWith("source_chain", "base");
  });

  it("fires onChange with undefined when 'All' is reselected", () => {
    const onChange = vi.fn();
    render(<FilterBar filters={{ status: "COMPLETED" }} onChange={onChange} />);
    fireEvent.change(screen.getByLabelText("Status"), { target: { value: "" } });
    expect(onChange).toHaveBeenCalledWith("status", undefined);
  });

  it("fires onChange for execution_mode with a known mode", () => {
    const onChange = vi.fn();
    render(<FilterBar filters={emptyFilters} onChange={onChange} />);
    fireEvent.change(screen.getByLabelText("Execution mode"), { target: { value: "testnet" } });
    expect(onChange).toHaveBeenCalledWith("execution_mode", "testnet");
  });
});
