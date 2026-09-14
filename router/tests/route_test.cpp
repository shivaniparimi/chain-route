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

TEST(RouteTest, FallsBackWhenCheapestPathFailsLiquidity) {
    Graph graph;
    const NodeIndex ethUsdc = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const NodeIndex baseUsdc = graph.addNode(Node{ChainId::Base, AssetId::USDC});
    const NodeIndex arbUsdc = graph.addNode(Node{ChainId::Arbitrum, AssetId::USDC});

    graph.addEdge(ethUsdc, Edge{baseUsdc, "DirectBridge", 4.0, 1000.0, 1000000.0, 0.99});
    graph.addEdge(ethUsdc, Edge{arbUsdc, "HopBridge1", 1.0, 500.0, 500.0, 0.99});
    graph.addEdge(arbUsdc, Edge{baseUsdc, "HopBridge2", 1.5, 500.0, 1000000.0, 0.99});

    const auto route = findCheapestRoute(graph, ethUsdc, baseUsdc, 1000.0);

    ASSERT_TRUE(route.has_value());
    EXPECT_DOUBLE_EQ(route->totalFee, 4.0);
    ASSERT_EQ(route->edges.size(), 1u);
    EXPECT_EQ(route->edges[0].bridgeName, "DirectBridge");
}

TEST(RouteTest, ReturnsNulloptWhenLiquidityEliminatesAllPaths) {
    Graph graph;
    const NodeIndex ethUsdc = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const NodeIndex baseUsdc = graph.addNode(Node{ChainId::Base, AssetId::USDC});

    graph.addEdge(ethUsdc, Edge{baseUsdc, "SmallBridge", 4.0, 1000.0, 500.0, 0.99});

    const auto route = findCheapestRoute(graph, ethUsdc, baseUsdc, 1000.0);

    EXPECT_FALSE(route.has_value());
}

TEST(RouteTest, RejectsMultiHopWhenAnIntermediateEdgeLacksLiquidity) {
    Graph graph;
    const NodeIndex ethUsdc = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const NodeIndex arbUsdc = graph.addNode(Node{ChainId::Arbitrum, AssetId::USDC});
    const NodeIndex polyUsdc = graph.addNode(Node{ChainId::Polygon, AssetId::USDC});
    const NodeIndex baseUsdc = graph.addNode(Node{ChainId::Base, AssetId::USDC});

    // Cheapest-looking 3-hop chain: Ethereum -> Arbitrum -> Polygon -> Base.
    // The middle leg (Arbitrum -> Polygon) doesn't have enough liquidity for the payment.
    graph.addEdge(ethUsdc, Edge{arbUsdc, "Hop1", 0.5, 500.0, 1000000.0, 0.99});
    graph.addEdge(arbUsdc, Edge{polyUsdc, "Hop2", 0.5, 500.0, 500.0, 0.99});
    graph.addEdge(polyUsdc, Edge{baseUsdc, "Hop3", 0.5, 500.0, 1000000.0, 0.99});

    graph.addEdge(ethUsdc, Edge{baseUsdc, "DirectBridge", 4.0, 1000.0, 1000000.0, 0.99});

    const auto route = findCheapestRoute(graph, ethUsdc, baseUsdc, 1000.0);

    ASSERT_TRUE(route.has_value());
    EXPECT_DOUBLE_EQ(route->totalFee, 4.0);
    ASSERT_EQ(route->edges.size(), 1u);
    EXPECT_EQ(route->edges[0].bridgeName, "DirectBridge");
}

TEST(RouteTest, ReturnsNulloptWhenGraphIsDisconnected) {
    Graph graph;
    const NodeIndex ethUsdc = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const NodeIndex baseUsdc = graph.addNode(Node{ChainId::Base, AssetId::USDC});

    const auto route = findCheapestRoute(graph, ethUsdc, baseUsdc, 1000.0);

    EXPECT_FALSE(route.has_value());
}

TEST(RouteTest, ReturnsTrivialRouteWhenSourceEqualsDestination) {
    Graph graph;
    const NodeIndex ethUsdc = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});

    const auto route = findCheapestRoute(graph, ethUsdc, ethUsdc, 1000.0);

    ASSERT_TRUE(route.has_value());
    EXPECT_TRUE(route->edges.empty());
    EXPECT_DOUBLE_EQ(route->totalFee, 0.0);
}

TEST(RouteTest, ReturnsNulloptForIsolatedDestination) {
    Graph graph;
    const NodeIndex ethUsdc = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const NodeIndex baseUsdc = graph.addNode(Node{ChainId::Base, AssetId::USDC});
    const NodeIndex isolatedUsdc = graph.addNode(Node{ChainId::Polygon, AssetId::USDC});

    graph.addEdge(ethUsdc, Edge{baseUsdc, "DirectBridge", 4.0, 1000.0, 1000000.0, 0.99});

    const auto route = findCheapestRoute(graph, ethUsdc, isolatedUsdc, 1000.0);

    EXPECT_FALSE(route.has_value());
}

}  // namespace
}  // namespace chainroute
