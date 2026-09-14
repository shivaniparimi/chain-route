#pragma once

#include <cstddef>
#include <string>

namespace chainroute {

using NodeIndex = std::size_t;

struct Edge {
    NodeIndex to;              // target node index; source is implicit (see Graph)
    std::string bridgeName;    // e.g. "Stargate", "Uniswap-V3" — disambiguates parallel edges
    double fee;                // absolute cost, USD
    double latencyMs;          // expected execution time, milliseconds
    double liquidity;          // available liquidity, USD
    double reliability;        // success probability, [0.0, 1.0]
};

}  // namespace chainroute
