#pragma once

#include <cstdint>
#include <string>

namespace chainroute {

enum class AssetId : uint8_t {
    USDC,
    USDT,
    ETH,
    WBTC,
    DAI,
};

std::string toString(AssetId asset);

}  // namespace chainroute
