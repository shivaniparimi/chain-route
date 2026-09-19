import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type { Hop } from "../api/types";
import { RouteVisualization } from "./RouteVisualization";

function makeHop(overrides: Partial<Hop>): Hop {
  return {
    hop_index: 0,
    from_chain: "ethereum",
    to_chain: "base",
    bridge_name: "Across",
    fee: 0.01,
    latency_ms: 1200,
    liquidity: 500000,
    reliability: 0.99,
    ...overrides,
  };
}

describe("RouteVisualization", () => {
  it("renders the Simulated route badge and highlights exactly the source/destination nodes and one edge for a simulated single-hop payment", () => {
    const hops = [makeHop({ from_chain: "ethereum", to_chain: "arbitrum", bridge_name: "Relay" })];

    const { container } = render(
      <RouteVisualization
        hops={hops}
        sourceChain="ethereum"
        destinationChain="arbitrum"
        executionMode="simulated"
        asset="USDC"
      />,
    );

    expect(screen.getByText("Simulated route")).toBeInTheDocument();
    expect(screen.queryByText("Live testnet execution")).not.toBeInTheDocument();

    // Exactly the 2 traversed nodes (Ethereum, Arbitrum) are accent-filled;
    // the other 3 (Base, Optimism, Polygon) stay muted.
    const accentCircles = container.querySelectorAll('circle[fill="#0bb6a3"]');
    expect(accentCircles).toHaveLength(2);
    const mutedCircles = container.querySelectorAll('circle[fill="#f1f5f9"]');
    expect(mutedCircles).toHaveLength(3);

    // Exactly one solid accent edge (the traversed hop); every other
    // structurally-routable pair among the 5 chains (10 total - 1 = 9)
    // stays a faint dashed line.
    const solidAccentLines = container.querySelectorAll('line[stroke="#0bb6a3"]');
    expect(solidAccentLines).toHaveLength(1);
    const dashedLines = container.querySelectorAll('line[stroke-dasharray]');
    expect(dashedLines).toHaveLength(9);

    expect(screen.getByText("Relay", { selector: "text" })).toBeInTheDocument();
  });

  it("renders the Live testnet execution badge for a testnet-mode Ethereum<->Base ETH payment", () => {
    const hops = [makeHop({ from_chain: "ethereum", to_chain: "base", bridge_name: "Across" })];

    render(
      <RouteVisualization
        hops={hops}
        sourceChain="ethereum"
        destinationChain="base"
        executionMode="testnet"
        asset="ETH"
      />,
    );

    expect(screen.getByText("Live testnet execution")).toBeInTheDocument();
    expect(screen.queryByText("Simulated route")).not.toBeInTheDocument();
  });

  it("does NOT render the live-execution badge for a testnet-mode payment on a chain pair hasRealTestnetExecution returns false for", () => {
    // Per lib/chains.ts's hasRealTestnetExecution, the only real corridor
    // is Ethereum<->Base in ETH. The backend should never actually produce
    // a testnet-mode payment on, say, Ethereum<->Arbitrum (unreachable by
    // construction per the design doc), but this component's own badge
    // logic is tested directly against the helper, independent of
    // whether the backend could ever really emit this combination.
    const hops = [makeHop({ from_chain: "ethereum", to_chain: "arbitrum", bridge_name: "Across" })];

    render(
      <RouteVisualization
        hops={hops}
        sourceChain="ethereum"
        destinationChain="arbitrum"
        executionMode="testnet"
        asset="ETH"
      />,
    );

    expect(screen.queryByText("Live testnet execution")).not.toBeInTheDocument();
    expect(screen.getByText("Simulated route")).toBeInTheDocument();
  });

  it("does NOT render the live-execution badge for the real Ethereum<->Base corridor when the asset isn't ETH", () => {
    const hops = [makeHop({ from_chain: "ethereum", to_chain: "base", bridge_name: "Across" })];

    render(
      <RouteVisualization
        hops={hops}
        sourceChain="ethereum"
        destinationChain="base"
        executionMode="testnet"
        asset="USDC"
      />,
    );

    expect(screen.queryByText("Live testnet execution")).not.toBeInTheDocument();
    expect(screen.getByText("Simulated route")).toBeInTheDocument();
  });

  it("renders a genuine multi-hop route as distinct labeled segments in sequence, not collapsed into one arrow", () => {
    // The C++ router's simulated-mode topology is a random graph over the
    // 5 chains (not complete), and its Dijkstra routing genuinely returns
    // multi-edge paths when no direct edge exists (see
    // router/tests/sim/network_simulator_test.cpp's
    // SomeChainPairHasNoDirectBridgeAndRoutesMultiHopOrNullopt) -- so this
    // component must render hops.length > 1 correctly, not just assume
    // every real response is a single hop.
    const hops = [
      makeHop({ hop_index: 0, from_chain: "ethereum", to_chain: "polygon", bridge_name: "Across" }),
      makeHop({ hop_index: 1, from_chain: "polygon", to_chain: "base", bridge_name: "Relay" }),
    ];

    const { container } = render(
      <RouteVisualization
        hops={hops}
        sourceChain="ethereum"
        destinationChain="base"
        executionMode="simulated"
        asset="USDC"
      />,
    );

    // Both hops show up as their own list entry, in hop_index order.
    const items = container.querySelectorAll("ol li");
    expect(items).toHaveLength(2);
    expect(items[0].textContent).toContain("1.");
    expect(items[0].textContent).toContain("Across");
    expect(items[1].textContent).toContain("2.");
    expect(items[1].textContent).toContain("Relay");

    // Both bridge names appear as separate edge labels in the diagram.
    expect(screen.getAllByText("Across").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Relay").length).toBeGreaterThan(0);

    // 3 distinct nodes traversed (Ethereum, Polygon, Base); 2 distinct
    // traversed edges, so 10 - 2 = 8 dashed edges remain.
    const accentCircles = container.querySelectorAll('circle[fill="#0bb6a3"]');
    expect(accentCircles).toHaveLength(3);
    const solidAccentLines = container.querySelectorAll('line[stroke="#0bb6a3"]');
    expect(solidAccentLines).toHaveLength(2);
    const dashedLines = container.querySelectorAll('line[stroke-dasharray]');
    expect(dashedLines).toHaveLength(8);
  });

  it("never fabricates a hop: with an empty hops array it renders no traversed nodes/edges and no hop list", () => {
    const { container } = render(
      <RouteVisualization hops={[]} sourceChain="ethereum" destinationChain="base" executionMode="simulated" asset="USDC" />,
    );

    expect(container.querySelectorAll('circle[fill="#0bb6a3"]')).toHaveLength(0);
    expect(container.querySelectorAll('line[stroke="#0bb6a3"]')).toHaveLength(0);
    expect(container.querySelectorAll("ol")).toHaveLength(0);
    // All 10 potential pairs among the 5 chains stay dashed.
    expect(container.querySelectorAll('line[stroke-dasharray]')).toHaveLength(10);
  });
});
