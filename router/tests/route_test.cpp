#include <gtest/gtest.h>

#include <stdexcept>

#include "chainroute/route.hpp"

namespace chainroute {
namespace {

TEST(RouteTest, PrefersCheaperMultiHopOverDirectRoute) {
    Graph graph;
    const NodeIndex ethUsdc = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const NodeIndex baseUsdc = graph.addNode(Node{ChainId::Base, AssetId::USDC});
    const NodeIndex arbUsdc = graph.addNode(Node{ChainId::Arbitrum, AssetId::USDC});

    graph.addEdge(ethUsdc, Edge{baseUsdc, "DirectBridge", 4.0, 1000.0, 1000000.0, 0.99});
    graph.addEdge(ethUsdc, Edge{arbUsdc, "HopBridge1", 1.0, 500.0, 1000000.0, 0.99});
    graph.addEdge(arbUsdc, Edge{baseUsdc, "HopBridge2", 1.5, 500.0, 1000000.0, 0.99});

    const auto route = findCheapestRoute(graph, ethUsdc, baseUsdc, 1000.0);

    ASSERT_TRUE(route.has_value());
    EXPECT_DOUBLE_EQ(route->totalFee, 2.5);
    ASSERT_EQ(route->edges.size(), 2u);
    EXPECT_EQ(route->edges[0].bridgeName, "HopBridge1");
    EXPECT_EQ(route->edges[1].bridgeName, "HopBridge2");
}

}  // namespace
}  // namespace chainroute
