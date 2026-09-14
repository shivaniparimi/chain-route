#pragma once

#include <cstddef>
#include <string>

namespace chainroute {

using NodeIndex = std::size_t;

struct Edge {
    NodeIndex to;
    std::string bridgeName;
    double fee;
    double latencyMs;
    double liquidity;
    double reliability;
};

}  // namespace chainroute
