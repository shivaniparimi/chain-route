#include "chainroute/sim/network_simulator.hpp"

#include <array>
#include <limits>

namespace chainroute::sim {

namespace {

// Fixed generation order: asset outer in {USDC, ETH}, then chain in
// ChainId's 5-chain declaration order. Node creation in snapshot() and
// the topology-generation loops below both use this exact order, so
// NodeIndex assignment and edge insertion order are fully deterministic.
constexpr std::array<ChainId, 5> kChains = {
    ChainId::Ethereum, ChainId::Base, ChainId::Arbitrum, ChainId::Optimism, ChainId::Polygon,
};
constexpr std::array<AssetId, 2> kAssets = {
    AssetId::USDC, AssetId::ETH,
};

// snapshot() below builds nodeIndex[assetIdx][chainIdx] by iterating
// *positions* in kChains, but edge lookup reads it back via
// static_cast<std::size_t>(bridge.source.chain) -- i.e. the raw ChainId
// enum value used directly as an index. That is only correct if kChains is
// in ChainId's exact enum-declaration order (kChains[i] == ChainId(i) for
// every i). These static_asserts fail to compile if kChains is ever
// reordered without updating the cast-based lookup to match.
static_assert(kChains[0] == static_cast<ChainId>(0), "kChains must be in ChainId enum-declaration order");
static_assert(kChains[1] == static_cast<ChainId>(1), "kChains must be in ChainId enum-declaration order");
static_assert(kChains[2] == static_cast<ChainId>(2), "kChains must be in ChainId enum-declaration order");
static_assert(kChains[3] == static_cast<ChainId>(3), "kChains must be in ChainId enum-declaration order");
static_assert(kChains[4] == static_cast<ChainId>(4), "kChains must be in ChainId enum-declaration order");
constexpr std::array<const char*, 6> kBridgeNamePool = {
    "Stargate", "Wormhole", "Across", "Hop", "Synapse", "Celer",
};

constexpr double kPairInclusionProbability = 0.6;
constexpr int kMinBridgesPerPair = 1;
constexpr int kMaxBridgesPerPair = 3;

constexpr double kMinBaseFee = 0.1;
constexpr double kMaxBaseFee = 10.0;
constexpr double kMinBaseLatencyMs = 200.0;
constexpr double kMaxBaseLatencyMs = 5000.0;
constexpr double kMinBaseLiquidity = 10000.0;
constexpr double kMaxBaseLiquidity = 2000000.0;
constexpr double kMinBaseReliability = 0.85;
constexpr double kMaxBaseReliability = 0.999;
constexpr double kMinVolatility = 0.05;
constexpr double kMaxVolatility = 0.3;

// AssetId's underlying values are not contiguous over just {USDC, ETH}
// (USDC=0, ETH=2 in the full 5-value enum), so array indexing into the
// compact 2-element kAssets/nodeIndex arrays needs an explicit map --
// unlike ChainId, whose 5 values ARE contiguous 0..4 and can be cast
// directly.
std::size_t assetIndexOf(AssetId asset) {
    switch (asset) {
        case AssetId::USDC: return 0;
        case AssetId::ETH: return 1;
        default: return 0;
    }
}

// Combines an ordered chain pair into one stable identifier. Target < 8
// always (ChainId has 5 values), so this is injective.
std::uint64_t pairKey(ChainId source, ChainId target) {
    return static_cast<std::uint64_t>(source) * 8 + static_cast<std::uint64_t>(target);
}
// pairKey's radix (8, above) must stay strictly greater than the number of
// ChainId values for the packing to remain collision-free. Fails to
// compile the moment the chain set grows past what radix 8 supports.
static_assert(kChains.size() < 8, "pairKey's radix-8 packing requires kChains.size() < 8");

double clamp(double value, double lo, double hi) {
    if (value < lo) return lo;
    if (value > hi) return hi;
    return value;
}

double scaledDraw(Seed seed, std::string_view role, std::uint64_t a, std::uint64_t b,
                   std::uint64_t c, std::uint64_t d, double lo, double hi) {
    return lo + draw(seed, role, a, b, c, d) * (hi - lo);
}

std::vector<BridgeTemplate> generateTopology(Seed seed) {
    std::vector<BridgeTemplate> topology;

    for (const AssetId asset : kAssets) {
        const auto assetArg = static_cast<std::uint64_t>(asset);

        for (const ChainId sourceChain : kChains) {
            for (const ChainId targetChain : kChains) {
                if (sourceChain == targetChain) {
                    continue;
                }
                const std::uint64_t pk = pairKey(sourceChain, targetChain);

                const double includeRoll = draw(seed, "topo.include", pk, assetArg, 0, 0);
                if (includeRoll >= kPairInclusionProbability) {
                    continue;
                }

                const double countRoll = draw(seed, "topo.count", pk, assetArg, 0, 0);
                const int span = kMaxBridgesPerPair - kMinBridgesPerPair + 1;
                const int bridgeCount = kMinBridgesPerPair + static_cast<int>(countRoll * span);

                for (int bridgeIndex = 0; bridgeIndex < bridgeCount; ++bridgeIndex) {
                    const auto idx = static_cast<std::uint64_t>(bridgeIndex);

                    const double nameRoll = draw(seed, "bridge.name", pk, assetArg, idx, 0);
                    const std::size_t nameIdx =
                        static_cast<std::size_t>(nameRoll * kBridgeNamePool.size());
                    std::string bridgeName = std::string(kBridgeNamePool[nameIdx]) + "#" +
                        std::to_string(bridgeIndex + 1);

                    const double baseFee = scaledDraw(
                        seed, "bridge.baseFee", pk, assetArg, idx, 0, kMinBaseFee, kMaxBaseFee);
                    const double baseLatencyMs = scaledDraw(
                        seed, "bridge.baseLatency", pk, assetArg, idx, 0,
                        kMinBaseLatencyMs, kMaxBaseLatencyMs);
                    const double baseLiquidity = scaledDraw(
                        seed, "bridge.baseLiquidity", pk, assetArg, idx, 0,
                        kMinBaseLiquidity, kMaxBaseLiquidity);
                    const double baseReliability = scaledDraw(
                        seed, "bridge.baseReliability", pk, assetArg, idx, 0,
                        kMinBaseReliability, kMaxBaseReliability);
                    const double volatility = scaledDraw(
                        seed, "bridge.volatility", pk, assetArg, idx, 0,
                        kMinVolatility, kMaxVolatility);

                    topology.push_back(BridgeTemplate{
                        Node{sourceChain, asset},
                        Node{targetChain, asset},
                        std::move(bridgeName),
                        baseFee,
                        baseLatencyMs,
                        baseLiquidity,
                        baseReliability,
                        volatility,
                        idx,
                    });
                }
            }
        }
    }

    return topology;
}

double tickMetric(Seed seed, const BridgeTemplate& bridge, std::string_view role,
                   double base, std::uint64_t tick, double lo, double hi) {
    const std::uint64_t pk = pairKey(bridge.source.chain, bridge.target.chain);
    const auto assetArg = static_cast<std::uint64_t>(bridge.source.asset);
    const double noiseRoll = draw(seed, role, pk, assetArg, bridge.bridgeIndex, tick);
    const double noise = noiseRoll * 2.0 - 1.0;  // maps [0,1) to [-1,1)
    return clamp(base + noise * base * bridge.volatility, lo, hi);
}

}  // namespace

NetworkSimulator::NetworkSimulator(Seed seed) : seed_(seed), topology_(generateTopology(seed)) {}

void NetworkSimulator::tick() {
    ++tick_;
}

std::uint64_t NetworkSimulator::currentTick() const {
    return tick_;
}

Graph NetworkSimulator::snapshot() const {
    Graph g;

    std::array<std::array<NodeIndex, kChains.size()>, kAssets.size()> nodeIndex{};
    for (std::size_t assetIdx = 0; assetIdx < kAssets.size(); ++assetIdx) {
        for (std::size_t chainIdx = 0; chainIdx < kChains.size(); ++chainIdx) {
            nodeIndex[assetIdx][chainIdx] = g.addNode(Node{kChains[chainIdx], kAssets[assetIdx]});
        }
    }

    for (const BridgeTemplate& bridge : topology_) {
        const std::size_t assetIdx = assetIndexOf(bridge.source.asset);
        const std::size_t sourceChainIdx = static_cast<std::size_t>(bridge.source.chain);
        const std::size_t targetChainIdx = static_cast<std::size_t>(bridge.target.chain);
        const NodeIndex sourceIndex = nodeIndex[assetIdx][sourceChainIdx];
        const NodeIndex targetIndex = nodeIndex[assetIdx][targetChainIdx];

        constexpr double kMax = std::numeric_limits<double>::max();
        const double fee = tickMetric(seed_, bridge, "tick.fee", bridge.baseFee, tick_, 0.0, kMax);
        const double latencyMs = tickMetric(
            seed_, bridge, "tick.latency", bridge.baseLatencyMs, tick_, 1.0, kMax);
        const double liquidity = tickMetric(
            seed_, bridge, "tick.liquidity", bridge.baseLiquidity, tick_, 0.0, kMax);
        const double reliability = tickMetric(
            seed_, bridge, "tick.reliability", bridge.baseReliability, tick_, 0.0, 1.0);

        g.addEdge(sourceIndex,
                   Edge{targetIndex, bridge.bridgeName, fee, latencyMs, liquidity, reliability});
    }

    return g;
}

}  // namespace chainroute::sim
