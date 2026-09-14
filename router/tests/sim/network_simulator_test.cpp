#include <gtest/gtest.h>

#include <cstdint>

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

}  // namespace
}  // namespace chainroute::sim
