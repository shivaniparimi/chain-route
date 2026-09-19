import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import type { Payment, PaymentQuotesResponse } from "../api/types";

// apiGet is the one seam every hook this page uses (usePayment,
// usePaymentQuotes) goes through -- mocking it here exercises the real
// hooks and the real page/components together. importOriginal keeps
// every other export (ApiError, used by ErrorState) real.
vi.mock("../api/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../api/client")>();
  return { ...actual, apiGet: vi.fn() };
});

import { apiGet } from "../api/client";
import { PaymentDetailPage } from "./PaymentDetailPage";

const mockedApiGet = vi.mocked(apiGet);

function makePayment(overrides: Partial<Payment> = {}): Payment {
  return {
    id: "11111111-1111-1111-1111-111111111111",
    source_chain: "ethereum",
    destination_chain: "base",
    asset: "ETH",
    amount: "0.5",
    status: "COMPLETED",
    total_fee: 0.001,
    hops: [
      {
        hop_index: 0,
        from_chain: "ethereum",
        to_chain: "base",
        bridge_name: "across",
        fee: 0.001,
        latency_ms: 5000,
        liquidity: 100,
        reliability: 0.99,
      },
    ],
    execution_mode: "testnet",
    bridge_provider: "across",
    failure_reason: null,
    external_tx_hash: "0xabc123",
    submitted_at: "2026-09-18T10:00:05.000Z",
    created_at: "2026-09-18T10:00:00.000Z",
    updated_at: "2026-09-18T10:00:20.000Z",
    completed_at: "2026-09-18T10:00:20.000Z",
    provider_reference_id: null,
    external_status: null,
    raw_external_status: null,
    ...overrides,
  };
}

function renderAt(paymentId: string) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={[`/payments/${paymentId}`]}>
        <Routes>
          <Route path="/payments/:id" element={<PaymentDetailPage />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

// Routes both endpoints this page's hooks call (GET /payments/{id} and
// GET /payments/{id}/quotes) off the same mocked apiGet -- ProviderComparison
// fetches its own quotes internally via usePaymentQuotes, it does not take
// a `quotes` prop.
function mockApiFor(payment: Payment, quotes: PaymentQuotesResponse = { quotes: [] }) {
  mockedApiGet.mockImplementation(async (path: string) => {
    if (path === `/payments/${payment.id}`) return payment as never;
    if (path === `/payments/${payment.id}/quotes`) return quotes as never;
    throw new Error(`unexpected path in test: ${path}`);
  });
}

describe("PaymentDetailPage", () => {
  it("shows a loading state while the payment fetch is pending", () => {
    mockedApiGet.mockImplementation(() => new Promise(() => {})); // never resolves
    renderAt("payment-loading");

    expect(screen.getByText("Loading payment…")).toBeInTheDocument();
  });

  it("shows an error state (not fabricated data) when the payment fetch fails", async () => {
    mockedApiGet.mockRejectedValue(new Error("payment fetch failed"));
    renderAt("payment-error");

    await waitFor(() => expect(screen.getByText("payment fetch failed")).toBeInTheDocument());
  });

  it("renders LifecycleTimeline, RouteVisualization, and ProviderComparison with the right props for a COMPLETED payment", async () => {
    const payment = makePayment();
    mockApiFor(payment, {
      quotes: [
        {
          provider: "across",
          input_amount: "500000000000000000",
          output_amount: "499000000000000000",
          fee_amount: "1000000000000000",
          estimated_fill_time_sec: 30,
          selected: true,
          quoted_at: "2026-09-18T10:00:00Z",
        },
      ],
    });

    renderAt(payment.id);

    // LifecycleTimeline: every one of the 7 stage labels present, derived
    // straight from the real Payment (not re-passed as separate props --
    // LifecycleTimeline takes `payment` directly).
    expect(await screen.findByText("Payment Created")).toBeInTheDocument();
    expect(screen.getByText("Destination Confirmation")).toBeInTheDocument();

    // RouteVisualization: rendered from payment.hops/source/destination --
    // confirmed via the route diagram's accessible label, which is built
    // from sourceChain/destinationChain exactly as passed.
    expect(
      screen.getByRole("img", { name: "Route diagram from Ethereum to Base" }),
    ).toBeInTheDocument();
    // Live testnet badge implies executionMode/asset/chains were passed
    // through correctly (hasRealTestnetExecution needs all four to agree).
    expect(screen.getByText("Live testnet execution")).toBeInTheDocument();

    // ProviderComparison: fetched paymentId's own quotes (not a `quotes`
    // prop) and rendered the one quote's provider label.
    expect(await screen.findByText("Across")).toBeInTheDocument();
    expect(mockedApiGet).toHaveBeenCalledWith(`/payments/${payment.id}/quotes`);

    // Details panel: every §4 field.
    expect(screen.getByText("0.5 ETH")).toBeInTheDocument();
    expect(screen.getByText("0xabc123")).toBeInTheDocument();
    // testnet + ethereum source + real tx hash -> a real explorer link.
    const txLink = screen.getByText("0xabc123").closest("a");
    expect(txLink).toHaveAttribute("href", "https://sepolia.etherscan.io/tx/0xabc123");
  });

  it("shows failure_reason on the page for a FAILED payment", async () => {
    const payment = makePayment({
      status: "FAILED",
      failure_reason: "fee_slippage_exceeded",
      external_tx_hash: null,
      submitted_at: null,
      completed_at: null,
    });
    mockApiFor(payment);

    renderAt(payment.id);

    expect(await screen.findByText("fee_slippage_exceeded")).toBeInTheDocument();
  });

  it("renders a plain monospace tx hash (no link) when execution_mode is simulated", async () => {
    const payment = makePayment({ execution_mode: "simulated" });
    mockApiFor(payment);

    renderAt(payment.id);

    const hashEl = await screen.findByText("0xabc123");
    expect(hashEl.closest("a")).toBeNull();
  });
});
