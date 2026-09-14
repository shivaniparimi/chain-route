#include <gtest/gtest.h>

#include "chainroute_service/chain_asset_convert.hpp"

namespace chainroute_service {
namespace {

TEST(ChainAssetConvertTest, RoundTripsEveryChain) {
    EXPECT_EQ(toChainId(toProtoChain(chainroute::ChainId::Ethereum)), chainroute::ChainId::Ethereum);
    EXPECT_EQ(toChainId(toProtoChain(chainroute::ChainId::Base)), chainroute::ChainId::Base);
    EXPECT_EQ(toChainId(toProtoChain(chainroute::ChainId::Arbitrum)), chainroute::ChainId::Arbitrum);
    EXPECT_EQ(toChainId(toProtoChain(chainroute::ChainId::Optimism)), chainroute::ChainId::Optimism);
    EXPECT_EQ(toChainId(toProtoChain(chainroute::ChainId::Polygon)), chainroute::ChainId::Polygon);
}

TEST(ChainAssetConvertTest, RoundTripsEveryUsedAsset) {
    EXPECT_EQ(toAssetId(toProtoAsset(chainroute::AssetId::USDC)), chainroute::AssetId::USDC);
    EXPECT_EQ(toAssetId(toProtoAsset(chainroute::AssetId::ETH)), chainroute::AssetId::ETH);
}

TEST(ChainAssetConvertTest, UnspecifiedChainMapsToNullopt) {
    EXPECT_FALSE(toChainId(chainroute::v1::CHAIN_UNSPECIFIED).has_value());
}

TEST(ChainAssetConvertTest, UnspecifiedAssetMapsToNullopt) {
    EXPECT_FALSE(toAssetId(chainroute::v1::ASSET_UNSPECIFIED).has_value());
}

TEST(ChainAssetConvertTest, OutOfRangeWireValueMapsToNullopt) {
    EXPECT_FALSE(toChainId(static_cast<chainroute::v1::Chain>(999)).has_value());
    EXPECT_FALSE(toAssetId(static_cast<chainroute::v1::Asset>(999)).has_value());
}

TEST(ChainAssetConvertTest, ToProtoChainProducesExpectedValues) {
    EXPECT_EQ(toProtoChain(chainroute::ChainId::Ethereum), chainroute::v1::CHAIN_ETHEREUM);
    EXPECT_EQ(toProtoChain(chainroute::ChainId::Polygon), chainroute::v1::CHAIN_POLYGON);
}

TEST(ChainAssetConvertTest, ToProtoAssetProducesExpectedValues) {
    EXPECT_EQ(toProtoAsset(chainroute::AssetId::USDC), chainroute::v1::ASSET_USDC);
    EXPECT_EQ(toProtoAsset(chainroute::AssetId::ETH), chainroute::v1::ASSET_ETH);
}

}  // namespace
}  // namespace chainroute_service
