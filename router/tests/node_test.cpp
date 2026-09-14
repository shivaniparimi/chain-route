#include <gtest/gtest.h>

#include "chainroute/asset.hpp"
#include "chainroute/chain.hpp"

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

}  // namespace
}  // namespace chainroute
