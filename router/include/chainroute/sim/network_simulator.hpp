#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "chainroute/graph.hpp"
#include "chainroute/sim/deterministic_rng.hpp"

namespace chainroute::sim {

// Internal: one bridge's fixed identity and base parameters, generated
// once at NetworkSimulator construction and never mutated. Not part of
// NetworkSimulator's public contract.
struct BridgeTemplate {
    Node source;
    Node target;
    std::string bridgeName;
    double baseFee;
    double baseLatencyMs;
    double baseLiquidity;
    double baseReliability;
    double volatility;
    std::uint64_t bridgeIndex;
};

class NetworkSimulator {
public:
    explicit NetworkSimulator(Seed seed);

    void tick();
    std::uint64_t currentTick() const;

    Graph snapshot() const;

private:
    Seed seed_;
    std::uint64_t tick_ = 0;
    std::vector<BridgeTemplate> topology_;
};

}  // namespace chainroute::sim
