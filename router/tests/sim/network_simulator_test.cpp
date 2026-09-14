#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <optional>
#include <unordered_map>

#include "chainroute/route.hpp"
#include "chainroute/sim/network_simulator.hpp"

namespace chainroute::sim {
namespace {

TEST(NetworkSimulatorTest, SameSeedSameTickProducesIdenticalSnapshots) {
    NetworkSimulator a(1001);
    NetworkSimulator b(1001);

    const Graph ga = a.snapshot();
    const Graph gb = b.snapshot();

    ASSERT_EQ(ga.nodeCount(), gb.nodeCount());
    ASSERT_EQ(ga.edgeCount(), gb.edgeCount());
    for (NodeIndex i = 0; i < ga.nodeCount(); ++i) {
        const auto& edgesA = ga.edgesFrom(i);
        const auto& edgesB = gb.edgesFrom(i);
        ASSERT_EQ(edgesA.size(), edgesB.size());
        for (std::size_t j = 0; j < edgesA.size(); ++j) {
            EXPECT_EQ(edgesA[j].to, edgesB[j].to);
            EXPECT_EQ(edgesA[j].bridgeName, edgesB[j].bridgeName);
            EXPECT_DOUBLE_EQ(edgesA[j].fee, edgesB[j].fee);
            EXPECT_DOUBLE_EQ(edgesA[j].latencyMs, edgesB[j].latencyMs);
            EXPECT_DOUBLE_EQ(edgesA[j].liquidity, edgesB[j].liquidity);
            EXPECT_DOUBLE_EQ(edgesA[j].reliability, edgesB[j].reliability);
        }
    }
}

TEST(NetworkSimulatorTest, DifferentSeedsProduceDifferentSnapshots) {
    NetworkSimulator a(1001);
    NetworkSimulator b(2002);

    const Graph ga = a.snapshot();
    const Graph gb = b.snapshot();

    bool anyDifference = ga.edgeCount() != gb.edgeCount();
    if (!anyDifference) {
        for (NodeIndex i = 0; i < ga.nodeCount() && !anyDifference; ++i) {
            const auto& edgesA = ga.edgesFrom(i);
            const auto& edgesB = gb.edgesFrom(i);
            if (edgesA.size() != edgesB.size()) {
                anyDifference = true;
                break;
            }
            for (std::size_t j = 0; j < edgesA.size(); ++j) {
                if (edgesA[j].bridgeName != edgesB[j].bridgeName ||
                    edgesA[j].fee != edgesB[j].fee) {
                    anyDifference = true;
                    break;
                }
            }
        }
    }
    EXPECT_TRUE(anyDifference);
}

TEST(NetworkSimulatorTest, TickChangesMetricsButNotTopology) {
    NetworkSimulator sim(1001);
    const Graph g0 = sim.snapshot();
    sim.tick();
    const Graph g1 = sim.snapshot();

    ASSERT_EQ(g0.nodeCount(), g1.nodeCount());
    ASSERT_EQ(g0.edgeCount(), g1.edgeCount());

    bool anyMetricDifference = false;
    for (NodeIndex i = 0; i < g0.nodeCount(); ++i) {
        const auto& edges0 = g0.edgesFrom(i);
        const auto& edges1 = g1.edgesFrom(i);
        ASSERT_EQ(edges0.size(), edges1.size());
        for (std::size_t j = 0; j < edges0.size(); ++j) {
            EXPECT_EQ(edges0[j].to, edges1[j].to);
            EXPECT_EQ(edges0[j].bridgeName, edges1[j].bridgeName);
            if (edges0[j].fee != edges1[j].fee || edges0[j].latencyMs != edges1[j].latencyMs ||
                edges0[j].liquidity != edges1[j].liquidity ||
                edges0[j].reliability != edges1[j].reliability) {
                anyMetricDifference = true;
            }
        }
    }
    EXPECT_TRUE(anyMetricDifference);
}

TEST(NetworkSimulatorTest, NodeIndexStableAcrossSeedsAndTicks) {
    NetworkSimulator a(1001);
    NetworkSimulator b(2002);
    const Graph ga = a.snapshot();
    a.tick();
    const Graph ga2 = a.snapshot();
    const Graph gb = b.snapshot();

    const Node ethUsdc{ChainId::Ethereum, AssetId::USDC};
    const auto idxA = ga.findNode(ethUsdc);
    const auto idxA2 = ga2.findNode(ethUsdc);
    const auto idxB = gb.findNode(ethUsdc);
    ASSERT_TRUE(idxA.has_value());
    ASSERT_TRUE(idxA2.has_value());
    ASSERT_TRUE(idxB.has_value());
    EXPECT_EQ(*idxA, *idxA2);
    EXPECT_EQ(*idxA, *idxB);
}

TEST(NetworkSimulatorTest, OldSnapshotUnaffectedByLaterTicks) {
    NetworkSimulator sim(1001);
    const Graph g0 = sim.snapshot();

    double originalFee = -1.0;
    bool found = false;
    for (NodeIndex i = 0; i < g0.nodeCount() && !found; ++i) {
        const auto& edges = g0.edgesFrom(i);
        if (!edges.empty()) {
            originalFee = edges[0].fee;
            found = true;
        }
    }
    ASSERT_TRUE(found);

    for (int i = 0; i < 10; ++i) {
        sim.tick();
    }
    sim.snapshot();

    double stillOriginalFee = -1.0;
    for (NodeIndex i = 0; i < g0.nodeCount(); ++i) {
        const auto& edges = g0.edgesFrom(i);
        if (!edges.empty()) {
            stillOriginalFee = edges[0].fee;
            break;
        }
    }
    EXPECT_DOUBLE_EQ(originalFee, stillOriginalFee);
}

TEST(NetworkSimulatorTest, MetricsStayWithinValidBoundsAcrossManyTicks) {
    NetworkSimulator sim(1001);
    for (int t = 0; t < 1000; ++t) {
        const Graph g = sim.snapshot();
        for (NodeIndex i = 0; i < g.nodeCount(); ++i) {
            for (const Edge& edge : g.edgesFrom(i)) {
                EXPECT_GE(edge.fee, 0.0);
                EXPECT_GT(edge.latencyMs, 0.0);
                EXPECT_GE(edge.liquidity, 0.0);
                EXPECT_GE(edge.reliability, 0.0);
                EXPECT_LE(edge.reliability, 1.0);
            }
        }
        sim.tick();
    }
}

TEST(NetworkSimulatorTest, SomeChainPairHasAsymmetricBridgeAvailability) {
    NetworkSimulator sim(1001);
    const Graph g = sim.snapshot();

    static constexpr std::array<ChainId, 5> kChains = {
        ChainId::Ethereum, ChainId::Base, ChainId::Arbitrum, ChainId::Optimism, ChainId::Polygon,
    };

    bool foundAsymmetry = false;
    for (ChainId a : kChains) {
        for (ChainId b : kChains) {
            if (a == b) continue;
            const auto nodeA = g.findNode(Node{a, AssetId::USDC});
            const auto nodeB = g.findNode(Node{b, AssetId::USDC});
            ASSERT_TRUE(nodeA.has_value());
            ASSERT_TRUE(nodeB.has_value());

            bool aToB = false;
            for (const Edge& e : g.edgesFrom(*nodeA)) {
                if (e.to == *nodeB) { aToB = true; break; }
            }
            bool bToA = false;
            for (const Edge& e : g.edgesFrom(*nodeB)) {
                if (e.to == *nodeA) { bToA = true; break; }
            }
            if (aToB != bToA) {
                foundAsymmetry = true;
            }
        }
    }
    EXPECT_TRUE(foundAsymmetry);
}

TEST(NetworkSimulatorTest, SomeChainPairHasParallelBridges) {
    NetworkSimulator sim(1001);
    const Graph g = sim.snapshot();

    bool foundParallel = false;
    for (NodeIndex i = 0; i < g.nodeCount() && !foundParallel; ++i) {
        std::unordered_map<NodeIndex, int> countByTarget;
        for (const Edge& e : g.edgesFrom(i)) {
            if (++countByTarget[e.to] > 1) {
                foundParallel = true;
                break;
            }
        }
    }
    EXPECT_TRUE(foundParallel);
}

TEST(NetworkSimulatorTest, SomeChainPairHasNoDirectBridgeAndRoutesMultiHopOrNullopt) {
    NetworkSimulator sim(1001);
    const Graph g = sim.snapshot();

    static constexpr std::array<ChainId, 5> kChains = {
        ChainId::Ethereum, ChainId::Base, ChainId::Arbitrum, ChainId::Optimism, ChainId::Polygon,
    };

    std::optional<NodeIndex> missingSource;
    std::optional<NodeIndex> missingTarget;
    for (ChainId a : kChains) {
        for (ChainId b : kChains) {
            if (a == b) continue;
            const auto nodeA = g.findNode(Node{a, AssetId::USDC});
            const auto nodeB = g.findNode(Node{b, AssetId::USDC});
            ASSERT_TRUE(nodeA.has_value());
            ASSERT_TRUE(nodeB.has_value());

            bool direct = false;
            for (const Edge& e : g.edgesFrom(*nodeA)) {
                if (e.to == *nodeB) { direct = true; break; }
            }
            if (!direct) {
                missingSource = nodeA;
                missingTarget = nodeB;
                break;
            }
        }
        if (missingSource) break;
    }
    ASSERT_TRUE(missingSource.has_value());
    ASSERT_TRUE(missingTarget.has_value());

    const auto route = findCheapestRoute(g, *missingSource, *missingTarget, 1000.0);
    if (route.has_value()) {
        EXPECT_GT(route->edges.size(), 1u);
    }
    // A nullopt result is also an acceptable, spec-sanctioned outcome here.
}

TEST(NetworkSimulatorTest, CheapestRouteChangesAcrossTicks) {
    // Fixture pinned from a development-time search (Task 4, plan step 2).
    // No seed/tick search happens at test run time. seed=4 was found by
    // scanning seeds 1-2000 for a direct Ethereum-USDC -> Base-USDC route
    // where the cheapest single-hop bridge swaps between consecutive
    // ticks; tick 2 -> 3 is the first clean flip for this seed (both
    // ticks resolve to the same direct edge count, but a different
    // bridge wins on fee).
    NetworkSimulator sim(4);
    const Node sourceNode{ChainId::Ethereum, AssetId::USDC};
    const Node destNode{ChainId::Base, AssetId::USDC};
    const double amount = 1000.0;

    for (int i = 0; i < 2; ++i) {
        sim.tick();
    }
    const Graph gA = sim.snapshot();
    const auto sourceA = gA.findNode(sourceNode);
    const auto destA = gA.findNode(destNode);
    ASSERT_TRUE(sourceA.has_value());
    ASSERT_TRUE(destA.has_value());
    const auto routeA = findCheapestRoute(gA, *sourceA, *destA, amount);
    ASSERT_TRUE(routeA.has_value());
    ASSERT_EQ(routeA->edges.size(), 1u);
    EXPECT_EQ(routeA->edges[0].bridgeName, "Across#1");
    EXPECT_DOUBLE_EQ(routeA->totalFee, 6.6282180207625432);

    for (int i = 2; i < 3; ++i) {
        sim.tick();
    }
    const Graph gB = sim.snapshot();
    const auto sourceB = gB.findNode(sourceNode);
    const auto destB = gB.findNode(destNode);
    ASSERT_TRUE(sourceB.has_value());
    ASSERT_TRUE(destB.has_value());
    const auto routeB = findCheapestRoute(gB, *sourceB, *destB, amount);
    ASSERT_TRUE(routeB.has_value());
    ASSERT_EQ(routeB->edges.size(), 1u);
    EXPECT_EQ(routeB->edges[0].bridgeName, "Synapse#2");
    EXPECT_DOUBLE_EQ(routeB->totalFee, 6.2292751452127142);

    // The cheapest route genuinely flips to a different bridge, not just a
    // fee wobble on the same bridge.
    EXPECT_NE(routeA->edges[0].bridgeName, routeB->edges[0].bridgeName);
    EXPECT_NE(routeA->totalFee, routeB->totalFee);
}

TEST(NetworkSimulatorTest, LiquidityChangeInvalidatesAPreviouslyEligibleRoute) {
    // Fixture pinned from a development-time search (Task 4, plan step 2).
    //
    // amount sits strictly between the Ethereum-USDC -> Optimism-USDC
    // "Hop#2" edge's tick-0 liquidity (~2010510.8658421615) and its tick-1
    // liquidity (~2002813.4593519256), with real margin on both sides
    // (deliberately not pinned to either boundary, since the metric
    // computation pipeline is not guaranteed bit-portable across
    // compilers/optimization levels). At this amount, Hop#2 is the ONLY
    // eligible direct edge between this pair at tick 0 (the cheaper
    // Stargate#1 and Wormhole#3 edges both have lower liquidity than
    // `amount` and are excluded), so Hop#2 -- despite not being the
    // cheapest fee -- is chosen. By tick 1, Hop#2's own liquidity noise has
    // dropped it below `amount`, so it too becomes ineligible; no other
    // direct or multi-hop path exists at this amount, so the route
    // disappears entirely. This was confirmed causally: the same bridge
    // (matched by name and target) is the one, and only one, edge whose
    // liquidity crosses below `amount` between these two ticks.
    NetworkSimulator sim(1001);
    const Node sourceNode{ChainId::Ethereum, AssetId::USDC};
    const Node destNode{ChainId::Optimism, AssetId::USDC};
    const double amount = 2006000.0;

    // tickA = 0: no ticks advanced yet.
    const Graph gA = sim.snapshot();
    const auto sourceA = gA.findNode(sourceNode);
    const auto destA = gA.findNode(destNode);
    ASSERT_TRUE(sourceA.has_value());
    ASSERT_TRUE(destA.has_value());
    const auto routeA = findCheapestRoute(gA, *sourceA, *destA, amount);
    ASSERT_TRUE(routeA.has_value());
    ASSERT_EQ(routeA->edges.size(), 1u);
    EXPECT_EQ(routeA->edges[0].bridgeName, "Hop#2");
    EXPECT_DOUBLE_EQ(routeA->totalFee, 3.1098994060954808);

    for (int i = 0; i < 1; ++i) {
        sim.tick();
    }
    const Graph gB = sim.snapshot();
    const auto sourceB = gB.findNode(sourceNode);
    const auto destB = gB.findNode(destNode);
    ASSERT_TRUE(sourceB.has_value());
    ASSERT_TRUE(destB.has_value());
    const auto routeB = findCheapestRoute(gB, *sourceB, *destB, amount);

    // Pinned expectation from the search: the route that was eligible at
    // tickA is no longer usable at tickB because the only edge that could
    // carry `amount` (Hop#2) had its liquidity drop below `amount`.
    EXPECT_FALSE(routeB.has_value());
}

}  // namespace
}  // namespace chainroute::sim
