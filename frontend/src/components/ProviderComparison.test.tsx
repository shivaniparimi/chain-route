import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { PaymentQuotesResponse, Quote } from "../api/types";
import { ProviderComparison } from "./ProviderComparison";

// apiGet is the only seam usePaymentQuotes goes through -- mocking it here
// (rather than mocking the hook itself) exercises the real hook and the
// real component together, closer to how this actually runs. Preserves
// every other real export (importOriginal) -- ErrorState imports ApiError
// from this same module, and a full-module mock would blank that out.
vi.mock("../api/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../api/client")>();
  return { ...actual, apiGet: vi.fn() };
});

import { apiGet } from "../api/client";

const mockedApiGet = vi.mocked(apiGet);

function makeQuote(overrides: Partial<Quote>): Quote {
  return {
    provider: "across",
    input_amount: "1000000000000000000",
    output_amount: "998000000000000000",
    fee_amount: "2000000000000000",
    estimated_fill_time_sec: 45,
    selected: false,
    quoted_at: "2026-09-18T12:00:00Z",
    ...overrides,
  };
}

function renderWithClient(paymentId: string) {
  // retry: false -- without it, a rejected queryFn retries with backoff
  // and the "isError" branch never settles within a test's lifetime.
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={queryClient}>
      <ProviderComparison paymentId={paymentId} />
    </QueryClientProvider>,
  );
}

describe("ProviderComparison", () => {
  it("renders the honest empty state for zero quotes, never invented numbers", async () => {
    const response: PaymentQuotesResponse = { quotes: [] };
    mockedApiGet.mockResolvedValueOnce(response);

    renderWithClient("payment-1");

    expect(
      await screen.findByText("No provider comparison data available for this payment"),
    ).toBeInTheDocument();
    expect(screen.queryAllByTestId("quote-card")).toHaveLength(0);
  });

  it("renders both cards for two quotes, marking the selected one via a data attribute", async () => {
    const response: PaymentQuotesResponse = {
      quotes: [
        makeQuote({ provider: "across", selected: true }),
        makeQuote({
          provider: "relay",
          selected: false,
          output_amount: "995000000000000000",
          fee_amount: "5000000000000000",
          estimated_fill_time_sec: 90,
        }),
      ],
    };
    mockedApiGet.mockResolvedValueOnce(response);

    renderWithClient("payment-2");

    const cards = await screen.findAllByTestId("quote-card");
    expect(cards).toHaveLength(2);

    const acrossCard = cards.find((card) => card.getAttribute("data-provider") === "across");
    const relayCard = cards.find((card) => card.getAttribute("data-provider") === "relay");
    expect(acrossCard).toHaveAttribute("data-selected", "true");
    expect(relayCard).toHaveAttribute("data-selected", "false");

    expect(screen.getByText("Across")).toBeInTheDocument();
    expect(screen.getByText("Relay")).toBeInTheDocument();
    expect(screen.getByText("Selected by ChainRoute")).toBeInTheDocument();

    // Quoted output / fee rendered via formatBaseUnits (base-units integer
    // strings converted to a human-scale decimal), not the raw integer.
    expect(screen.getByText("0.998")).toBeInTheDocument();
    expect(screen.getByText("0.002")).toBeInTheDocument();
    expect(screen.getByText("0.995")).toBeInTheDocument();
    expect(screen.getByText("0.005")).toBeInTheDocument();

    // Exactly one comparison, so no "only one provider" note.
    expect(screen.queryByText("Only one provider responded for this payment")).not.toBeInTheDocument();
  });

  it("adds the 'only one provider responded' note for a single-quote array, instead of presenting it as a won comparison", async () => {
    const response: PaymentQuotesResponse = { quotes: [makeQuote({ provider: "across", selected: true })] };
    mockedApiGet.mockResolvedValueOnce(response);

    renderWithClient("payment-3");

    expect(await screen.findAllByTestId("quote-card")).toHaveLength(1);
    expect(screen.getByText("Only one provider responded for this payment")).toBeInTheDocument();
  });

  it("renders an error state (not fabricated data) when the quotes request fails", async () => {
    mockedApiGet.mockRejectedValueOnce(new Error("network down"));

    renderWithClient("payment-4");

    await waitFor(() => expect(screen.getByText("network down")).toBeInTheDocument());
    expect(screen.queryAllByTestId("quote-card")).toHaveLength(0);
  });
});
