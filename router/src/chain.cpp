#include "chainroute/chain.hpp"

namespace chainroute {

std::string toString(ChainId chain) {
    switch (chain) {
        case ChainId::Ethereum: return "Ethereum";
        case ChainId::Base: return "Base";
        case ChainId::Arbitrum: return "Arbitrum";
        case ChainId::Optimism: return "Optimism";
        case ChainId::Polygon: return "Polygon";
    }
    return "Unknown";
}

}  // namespace chainroute
