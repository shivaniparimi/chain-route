#include "chainroute_service/chain_asset_convert.hpp"

namespace chainroute_service {

std::optional<chainroute::ChainId> toChainId(chainroute::v1::Chain proto) {
    switch (proto) {
        case chainroute::v1::CHAIN_ETHEREUM: return chainroute::ChainId::Ethereum;
        case chainroute::v1::CHAIN_BASE: return chainroute::ChainId::Base;
        case chainroute::v1::CHAIN_ARBITRUM: return chainroute::ChainId::Arbitrum;
        case chainroute::v1::CHAIN_OPTIMISM: return chainroute::ChainId::Optimism;
        case chainroute::v1::CHAIN_POLYGON: return chainroute::ChainId::Polygon;
        case chainroute::v1::CHAIN_UNSPECIFIED:
        default:
            return std::nullopt;
    }
}

std::optional<chainroute::AssetId> toAssetId(chainroute::v1::Asset proto) {
    switch (proto) {
        case chainroute::v1::ASSET_USDC: return chainroute::AssetId::USDC;
        case chainroute::v1::ASSET_ETH: return chainroute::AssetId::ETH;
        case chainroute::v1::ASSET_UNSPECIFIED:
        default:
            return std::nullopt;
    }
}

chainroute::v1::Chain toProtoChain(chainroute::ChainId chain) {
    switch (chain) {
        case chainroute::ChainId::Ethereum: return chainroute::v1::CHAIN_ETHEREUM;
        case chainroute::ChainId::Base: return chainroute::v1::CHAIN_BASE;
        case chainroute::ChainId::Arbitrum: return chainroute::v1::CHAIN_ARBITRUM;
        case chainroute::ChainId::Optimism: return chainroute::v1::CHAIN_OPTIMISM;
        case chainroute::ChainId::Polygon: return chainroute::v1::CHAIN_POLYGON;
    }
    return chainroute::v1::CHAIN_UNSPECIFIED;
}

chainroute::v1::Asset toProtoAsset(chainroute::AssetId asset) {
    switch (asset) {
        case chainroute::AssetId::USDC: return chainroute::v1::ASSET_USDC;
        case chainroute::AssetId::ETH: return chainroute::v1::ASSET_ETH;
        case chainroute::AssetId::USDT:
        case chainroute::AssetId::WBTC:
        case chainroute::AssetId::DAI:
            return chainroute::v1::ASSET_UNSPECIFIED;
    }
    return chainroute::v1::ASSET_UNSPECIFIED;
}

}  // namespace chainroute_service
