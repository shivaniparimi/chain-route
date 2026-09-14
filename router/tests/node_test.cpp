#include <gtest/gtest.h>

#include <unordered_set>

#include "chainroute/asset.hpp"
#include "chainroute/chain.hpp"
#include "chainroute/node.hpp"

namespace chainroute {
namespace {

TEST(ChainIdTest, ToStringKnownValues) {
    EXPECT_EQ(toString(ChainId::Ethereum), "Ethereum");
    EXPECT_EQ(toString(ChainId::Base), "Base");
    EXPECT_EQ(toString(ChainId::Arbitrum), "Arbitrum");
    EXPECT_EQ(toString(ChainId::Optimism), "Optimism");
    EXPECT_EQ(toString(ChainId::Polygon), "Polygon");
}

TEST(AssetIdTest, ToStringKnownValues) {
    EXPECT_EQ(toString(AssetId::USDC), "USDC");
    EXPECT_EQ(toString(AssetId::USDT), "USDT");
    EXPECT_EQ(toString(AssetId::ETH), "ETH");
    EXPECT_EQ(toString(AssetId::WBTC), "WBTC");
    EXPECT_EQ(toString(AssetId::DAI), "DAI");
}

TEST(NodeTest, EqualNodesCompareEqual) {
    const Node a{ChainId::Ethereum, AssetId::USDC};
    const Node b{ChainId::Ethereum, AssetId::USDC};
    EXPECT_EQ(a, b);
}

TEST(NodeTest, DifferentChainCompareNotEqual) {
    const Node a{ChainId::Ethereum, AssetId::USDC};
    const Node b{ChainId::Base, AssetId::USDC};
    EXPECT_NE(a, b);
}

TEST(NodeTest, DifferentAssetCompareNotEqual) {
    const Node a{ChainId::Ethereum, AssetId::USDC};
    const Node b{ChainId::Ethereum, AssetId::USDT};
    EXPECT_NE(a, b);
}

TEST(NodeTest, EqualNodesHashEqual) {
    const Node a{ChainId::Ethereum, AssetId::USDC};
    const Node b{ChainId::Ethereum, AssetId::USDC};
    EXPECT_EQ(NodeHash{}(a), NodeHash{}(b));
}

TEST(NodeTest, HashUsableInUnorderedSet) {
    std::unordered_set<Node, NodeHash> nodes;
    nodes.insert(Node{ChainId::Ethereum, AssetId::USDC});
    nodes.insert(Node{ChainId::Ethereum, AssetId::USDC});
    nodes.insert(Node{ChainId::Base, AssetId::USDC});
    EXPECT_EQ(nodes.size(), 2u);
}

TEST(NodeTest, ToStringFormatsChainDashAsset) {
    const Node node{ChainId::Ethereum, AssetId::USDC};
    EXPECT_EQ(toString(node), "Ethereum-USDC");
}

}  // namespace
}  // namespace chainroute
