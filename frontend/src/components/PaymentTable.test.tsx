import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";

import type { PaymentListItem } from "../api/types";
import { PaymentTable } from "./PaymentTable";

const fixturePayments: PaymentListItem[] = [
  {
    id: "11111111-2222-3333-4444-555555555555",
    source_chain: "ethereum",
    destination_chain: "base",
    asset: "USDC",
    amount: "1000.00",
    status: "COMPLETED",
    execution_mode: "simulated",
    bridge_provider: "across",
    total_fee: 2.5,
    created_at: "2026-09-18T12:00:00Z",
  },
  {
    id: "66666666-7777-8888-9999-aaaaaaaaaaaa",
    source_chain: "arbitrum",
    destination_chain: "optimism",
    asset: "WETH",
    amount: "0.5",
    status: "PROCESSING",
    execution_mode: "testnet",
    bridge_provider: null,
    total_fee: 0.25,
    created_at: "2026-09-18T11:00:00Z",
  },
];

function renderTable(payments: PaymentListItem[]) {
  return render(
    <MemoryRouter>
      <PaymentTable payments={payments} />
    </MemoryRouter>,
  );
}

describe("PaymentTable", () => {
  it("renders every column's expected value for each row", () => {
    renderTable(fixturePayments);

    // Payment ID: truncated, full ID available via title tooltip.
    expect(screen.getByTitle("11111111-2222-3333-4444-555555555555")).toBeInTheDocument();
    expect(screen.getByText("11111111…")).toBeInTheDocument();

    // Route (source -> destination display names).
    expect(screen.getByText("Ethereum → Base")).toBeInTheDocument();
    expect(screen.getByText("Arbitrum → Optimism")).toBeInTheDocument();

    // Asset, amount.
    expect(screen.getByText("USDC")).toBeInTheDocument();
    expect(screen.getByText("1000.00")).toBeInTheDocument();
    expect(screen.getByText("WETH")).toBeInTheDocument();
    expect(screen.getByText("0.5")).toBeInTheDocument();

    // Provider (display name when present, em dash when null).
    expect(screen.getByText("Across")).toBeInTheDocument();
    expect(screen.getByText("—")).toBeInTheDocument();

    // Routing cost (total_fee).
    expect(screen.getByText("2.5")).toBeInTheDocument();
    expect(screen.getByText("0.25")).toBeInTheDocument();

    // Status via StatusBadge's label text.
    expect(screen.getByText("Completed")).toBeInTheDocument();
    expect(screen.getByText("Processing")).toBeInTheDocument();

    // Execution mode.
    expect(screen.getByText("simulated")).toBeInTheDocument();
    expect(screen.getByText("testnet")).toBeInTheDocument();

    // Created time: relative time text, full timestamp via title tooltip.
    expect(screen.getAllByTitle(/2026/).length).toBeGreaterThanOrEqual(1);
  });

  it("links each row to /payments/:id", () => {
    renderTable(fixturePayments);

    const links = screen.getAllByRole("link");
    const hrefs = links.map((link) => link.getAttribute("href"));
    expect(hrefs).toContain("/payments/11111111-2222-3333-4444-555555555555");
    expect(hrefs).toContain("/payments/66666666-7777-8888-9999-aaaaaaaaaaaa");
  });

  it("renders nothing for an empty list", () => {
    const { container } = renderTable([]);
    expect(container).toBeEmptyDOMElement();
  });
});
