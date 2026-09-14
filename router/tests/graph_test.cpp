#include <gtest/gtest.h>

#include <stdexcept>

#include "chainroute/graph.hpp"

namespace chainroute {
namespace {

TEST(GraphTest, AddNodeIsIdempotentForSameNode) {
    Graph graph;
    const NodeIndex first = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const NodeIndex second = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    EXPECT_EQ(first, second);
    EXPECT_EQ(graph.nodeCount(), 1u);
}

TEST(GraphTest, AddNodeAssignsDistinctIndicesForDistinctNodes) {
    Graph graph;
    const NodeIndex a = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const NodeIndex b = graph.addNode(Node{ChainId::Base, AssetId::USDC});
    EXPECT_NE(a, b);
    EXPECT_EQ(graph.nodeCount(), 2u);
}

TEST(GraphTest, FindNodeReturnsIndexWhenPresent) {
    Graph graph;
    const NodeIndex index = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const auto found = graph.findNode(Node{ChainId::Ethereum, AssetId::USDC});
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(*found, index);
}

TEST(GraphTest, FindNodeReturnsNulloptWhenAbsent) {
    Graph graph;
    graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const auto found = graph.findNode(Node{ChainId::Base, AssetId::USDC});
    EXPECT_FALSE(found.has_value());
}

TEST(GraphTest, NodeAtReturnsCorrectNode) {
    Graph graph;
    const NodeIndex index = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    EXPECT_EQ(graph.nodeAt(index), (Node{ChainId::Ethereum, AssetId::USDC}));
}

TEST(GraphTest, NodeAtThrowsOnInvalidIndex) {
    Graph graph;
    graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    EXPECT_THROW(graph.nodeAt(42), std::out_of_range);
}

TEST(GraphTest, NodeCountReflectsDistinctNodesAdded) {
    Graph graph;
    graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    graph.addNode(Node{ChainId::Base, AssetId::USDC});
    graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    EXPECT_EQ(graph.nodeCount(), 2u);
}

TEST(GraphTest, AddEdgeCreatesDirectedEdge) {
    Graph graph;
    const NodeIndex ethUsdc = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const NodeIndex baseUsdc = graph.addNode(Node{ChainId::Base, AssetId::USDC});

    graph.addEdge(ethUsdc, Edge{baseUsdc, "Stargate", 1.0, 500.0, 100000.0, 0.99});

    EXPECT_EQ(graph.edgesFrom(ethUsdc).size(), 1u);
    EXPECT_TRUE(graph.edgesFrom(baseUsdc).empty());
}

TEST(GraphTest, AddEdgeSupportsParallelEdgesBetweenSamePair) {
    Graph graph;
    const NodeIndex ethUsdc = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const NodeIndex baseUsdc = graph.addNode(Node{ChainId::Base, AssetId::USDC});

    graph.addEdge(ethUsdc, Edge{baseUsdc, "Stargate", 1.0, 500.0, 100000.0, 0.99});
    graph.addEdge(ethUsdc, Edge{baseUsdc, "Wormhole", 0.5, 1500.0, 50000.0, 0.95});

    const auto& edges = graph.edgesFrom(ethUsdc);
    ASSERT_EQ(edges.size(), 2u);
    EXPECT_EQ(edges[0].bridgeName, "Stargate");
    EXPECT_EQ(edges[1].bridgeName, "Wormhole");
}

TEST(GraphTest, EdgesFromThrowsOnInvalidIndex) {
    Graph graph;
    graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    EXPECT_THROW(graph.edgesFrom(42), std::out_of_range);
}

TEST(GraphTest, AddEdgeThrowsWhenFromIndexInvalid) {
    Graph graph;
    const NodeIndex baseUsdc = graph.addNode(Node{ChainId::Base, AssetId::USDC});
    EXPECT_THROW(
        graph.addEdge(42, Edge{baseUsdc, "Stargate", 1.0, 500.0, 100000.0, 0.99}),
        std::out_of_range);
}

TEST(GraphTest, AddEdgeThrowsWhenToIndexInvalid) {
    Graph graph;
    const NodeIndex ethUsdc = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    EXPECT_THROW(
        graph.addEdge(ethUsdc, Edge{42, "Stargate", 1.0, 500.0, 100000.0, 0.99}),
        std::out_of_range);
}

TEST(GraphTest, EdgeCountReflectsAllEdgesAcrossNodes) {
    Graph graph;
    const NodeIndex ethUsdc = graph.addNode(Node{ChainId::Ethereum, AssetId::USDC});
    const NodeIndex baseUsdc = graph.addNode(Node{ChainId::Base, AssetId::USDC});
    const NodeIndex arbUsdc = graph.addNode(Node{ChainId::Arbitrum, AssetId::USDC});

    graph.addEdge(ethUsdc, Edge{baseUsdc, "Stargate", 1.0, 500.0, 100000.0, 0.99});
    graph.addEdge(ethUsdc, Edge{arbUsdc, "Wormhole", 0.8, 900.0, 80000.0, 0.97});
    graph.addEdge(baseUsdc, Edge{arbUsdc, "Across", 0.3, 300.0, 60000.0, 0.98});

    EXPECT_EQ(graph.edgeCount(), 3u);
}

}  // namespace
}  // namespace chainroute
