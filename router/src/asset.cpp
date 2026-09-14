#include "chainroute/asset.hpp"

namespace chainroute {

std::string toString(AssetId asset) {
    switch (asset) {
        case AssetId::USDC: return "USDC";
        case AssetId::USDT: return "USDT";
        case AssetId::ETH: return "ETH";
        case AssetId::WBTC: return "WBTC";
        case AssetId::DAI: return "DAI";
    }
    return "Unknown";
}

}  // namespace chainroute
