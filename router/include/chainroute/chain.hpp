#pragma once

#include <cstdint>
#include <string>

namespace chainroute {

enum class ChainId : uint8_t {
    Ethereum,
    Base,
    Arbitrum,
    Optimism,
    Polygon,
};

std::string toString(ChainId chain);

}  // namespace chainroute
